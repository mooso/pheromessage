//! Driver for running a local network of gossip nodes as threads that talk to each other.

use std::{
    fmt::Debug,
    sync::mpsc::{self, RecvTimeoutError},
    thread::spawn,
    time::{Duration, Instant},
};

use clap::Parser;
use hdrhistogram::Histogram;
use log::{debug, info, LevelFilter};
use pheromessage::{
    channel::{preferential_local_gossip_set, uniform_local_gossip_set, LocalGossipNode},
    data::{GossipSet, GossipSetAction},
    Gossip, SharedData,
};
use rand::prelude::*;
use simple_logger::SimpleLogger;

/// Simulate a local gossip network maintaining a set where every node is a thread.
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Number of threads/nodes in the gossip network.
    #[arg(short, long, default_value_t = 16)]
    nodes: usize,

    /// Fanout of how many nodes to gossip to when a message is received.
    #[arg(short, long, default_value_t = 4)]
    fanout: usize,

    /// The time in seconds to run the network for.
    #[arg(short, long, default_value_t = 10)]
    time: u64,

    /// If specified as more than 0 (default), then we'll use a preferential gossip algorithm and designate the number of primary nodes.
    #[arg(short, long, default_value_t = 0)]
    primaries: usize,

    /// If more than 0, then we'll declare a message lost if we don't see it in our target node after this many milliseconds.
    #[arg(short, long, default_value_t = 500)]
    lost_time_millis: u64,
}

/// The action that can be taken by each node upon receiving a message.
#[derive(Debug, Clone)]
enum Action {
    /// A gossip message about modifying a set (as sent from another node).
    GossipModifySet(GossipSetAction<u128>),
    /// A primary message about modifying a set (as sent from the main program).
    ModifySet(GossipSetAction<u128>),
    /// Terminate the node.
    Terminate,
    /// Query for the existence of a given element.
    Query {
        /// The element to query about.
        element: u128,
        /// Where to send the answer.
        answer: mpsc::Sender<bool>,
    },
}

/// The messsage that each node can process.
#[derive(Debug, Clone)]
struct Message {
    id: u128,
    action: Action,
}

impl Message {
    pub fn new(action: Action) -> Message {
        Message {
            id: thread_rng().gen(),
            action,
        }
    }
}

impl pheromessage::Message for Message {
    type I = u128;

    fn id(&self) -> Self::I {
        self.id
    }
}

impl SharedData<Message> for GossipSet<u128> {
    fn update(&mut self, message: &Message) {
        if let Action::GossipModifySet(action) = message.action {
            match action {
                GossipSetAction::Add(v) => self.add_item(v),
                GossipSetAction::Remove(v) => self.remove_item(v),
            }
        }
    }
}

/// Thread function for running a gossip node.
fn run_node<G>(mut gossip: G, receiver: mpsc::Receiver<Message>) -> Result<(), G::Error>
where
    G: Gossip<Message, GossipSet<u128>>,
{
    while let Ok(message) = receiver.recv() {
        match message.action {
            Action::GossipModifySet(_) => gossip.receive(&message)?,
            Action::ModifySet(v) => {
                // This is a bit confusing, but when the main program is asking me to modify
                // the set, I should use the `update()` function on the gossip but use a GossipModifySet
                // action so that's the one that's gossipped to the other nodes.
                gossip.update(&Message {
                    id: message.id,
                    action: Action::GossipModifySet(v),
                })?
            }
            Action::Terminate => break,
            Action::Query { element, answer } => {
                answer.send(gossip.data().is_present(&element)).unwrap()
            }
        }
    }
    Ok(())
}

/// An aggregate of latency.
#[derive(Clone)]
struct LatencyAggregate {
    total_latency: Duration,
    num_elements: usize,
    histogram: Histogram<u64>,
}

impl Default for LatencyAggregate {
    fn default() -> Self {
        Self {
            total_latency: Default::default(),
            num_elements: Default::default(),
            histogram: Histogram::new_with_max(1024 * 1024, 2).unwrap(),
        }
    }
}

impl LatencyAggregate {
    pub fn add_point(&mut self, latency: Duration) {
        self.num_elements += 1;
        self.total_latency += latency;
        self.histogram.record(latency.as_micros() as u64).unwrap();
    }

    pub fn mean_micros(&self) -> f64 {
        self.total_latency.as_micros() as f64 / self.num_elements as f64
    }

    pub fn percentiles(&self) -> String {
        if self.num_elements == 0 {
            return String::default();
        }
        format!(
            "p50: {} us, p90: {} us, p99: {} us, p100: {} us",
            self.histogram.value_at_percentile(50.),
            self.histogram.value_at_percentile(90.),
            self.histogram.value_at_percentile(99.),
            self.histogram.value_at_percentile(100.)
        )
    }
}

/// Definition of aggregator for the fates of elements inserted into one node(source) than waiting for
/// them to appear in another (target).
trait Aggregator {
    /// Record that from the time of inserting an element into a node (source) until it appeared in
    /// another (target), the duration was the given latency.
    fn record_latency(&mut self, source_index: usize, target_index: usize, latency: Duration);
    /// Record that after inserting an element into a node (source), we waited for it to appear in
    /// another (target) then gave up after a timeout.
    fn record_loss(&mut self, source_index: usize, target_index: usize);
    /// Log the current aggregate latencies.
    fn log(&self);
}

/// An aggregator for use with uniform gossip.
#[derive(Clone, Default)]
struct UniformGossipAggregator {
    aggregate: LatencyAggregate,
    lost_elements: usize,
}

/// Helper function to calculate the percentage of lost elements.
fn lost_percent(lost_elements: usize, total_elements: usize) -> f64 {
    if total_elements == 0 {
        0.0
    } else {
        (lost_elements as f64 / total_elements as f64) * 100.0
    }
}

impl Aggregator for UniformGossipAggregator {
    fn record_latency(&mut self, _source_index: usize, _target_index: usize, latency: Duration) {
        self.aggregate.add_point(latency);
    }

    fn record_loss(&mut self, _source_index: usize, _target_index: usize) {
        self.lost_elements += 1;
    }

    fn log(&self) {
        info!(
            "Inserted {} elements with an average latency of {:.2} us ({}). {} elements lost ({:.2}%).",
            self.aggregate.num_elements,
            self.aggregate.mean_micros(),
            self.aggregate.percentiles(),
            self.lost_elements,
            lost_percent(self.lost_elements, self.aggregate.num_elements)
        );
    }
}

/// An aggregator for use with preferential gossip.
#[derive(Clone)]
struct PreferentialGossipAggregator {
    primaries_aggregate: LatencyAggregate,
    secondaries_aggregate: LatencyAggregate,
    num_primaries: usize,
    lost_in_primaries: usize,
    lost_in_secondaries: usize,
}

impl PreferentialGossipAggregator {
    pub fn new(num_primaries: usize) -> PreferentialGossipAggregator {
        PreferentialGossipAggregator {
            primaries_aggregate: Default::default(),
            secondaries_aggregate: Default::default(),
            num_primaries,
            lost_in_primaries: 0,
            lost_in_secondaries: 0,
        }
    }
}

impl Aggregator for PreferentialGossipAggregator {
    fn record_latency(&mut self, _source_index: usize, target_index: usize, latency: Duration) {
        if target_index < self.num_primaries {
            self.primaries_aggregate.add_point(latency);
        } else {
            self.secondaries_aggregate.add_point(latency);
        }
    }

    fn record_loss(&mut self, _source_index: usize, target_index: usize) {
        if target_index < self.num_primaries {
            self.lost_in_primaries += 1;
        } else {
            self.lost_in_secondaries += 1;
        }
    }

    fn log(&self) {
        info!(
            "Inserted {} elements. Primaries average latency is {:.2} us ({}). Secondaries average latency is {:.2} us ({}). Elements lost in: primaries {} ({:.2}%), secondaries {} ({:.2}%)",
            self.primaries_aggregate.num_elements + self.secondaries_aggregate.num_elements,
            self.primaries_aggregate.mean_micros(),
            self.primaries_aggregate.percentiles(),
            self.secondaries_aggregate.mean_micros(),
            self.secondaries_aggregate.percentiles(),
            self.lost_in_primaries,
            lost_percent(self.lost_in_primaries, self.primaries_aggregate.num_elements),
            self.lost_in_secondaries,
            lost_percent(self.lost_in_secondaries, self.secondaries_aggregate.num_elements),
        );
    }
}

fn create_aggregator(args: &Args) -> Box<dyn Aggregator> {
    if args.primaries > 0 {
        Box::new(PreferentialGossipAggregator::new(args.primaries))
    } else {
        Box::new(UniformGossipAggregator::default())
    }
}

/// The outcome for waiting for an element to appear in a target node.
#[derive(Debug, Clone, Copy)]
enum WaitForElementOutcome {
    /// The element appeared in the target node after the recorded duration.
    Appeared(Duration),
    /// The element never appeared in the target node and we gave up.
    Lost,
    /// The end time of the program was reached before we got the element.
    EndTimeReached,
}

/// Wait for an element to appear in a target node. We'll use `my_tx` and `my_rx` to communicate with the node.
/// If `end_time` is reached before the element appears, we'll return with `EndTimeReached`.
/// If `loss_timeout` passes before the element appears, we'll return with `Lost`.
fn wait_for_element(
    target_node: &mpsc::Sender<Message>,
    element: u128,
    end_time: Instant,
    loss_timeout: Option<Duration>,
    my_tx: &mpsc::Sender<bool>,
    my_rx: &mpsc::Receiver<bool>,
) -> WaitForElementOutcome {
    let insertion_time = Instant::now();
    let loss_time = loss_timeout.map(|timeout| insertion_time + timeout);
    // Keep checking for the element in the target node until it appears
    loop {
        target_node
            .send(Message::new(Action::Query {
                element,
                answer: my_tx.clone(),
            }))
            .unwrap();
        // Don't wait for the answer beyond our end time
        let now = Instant::now();
        if now >= end_time {
            return WaitForElementOutcome::EndTimeReached;
        }
        let mut timeout = end_time - now;
        let mut timeout_result = WaitForElementOutcome::EndTimeReached;
        if let Some(loss_timeout) = loss_timeout {
            if loss_timeout < timeout {
                timeout = loss_timeout;
                timeout_result = WaitForElementOutcome::Lost;
            }
        }
        let answer = match my_rx.recv_timeout(timeout) {
            Ok(answer) => answer,
            Err(RecvTimeoutError::Timeout) => return timeout_result,
            Err(e) => Err(e).unwrap(),
        };
        if answer {
            // The target node has seen the element we inserted.
            return WaitForElementOutcome::Appeared(Instant::now() - insertion_time);
        } else if let Some(loss_time) = loss_time {
            if Instant::now() >= loss_time {
                return WaitForElementOutcome::Lost;
            }
        }
    }
}

fn run_network<G>(network: Vec<LocalGossipNode<G, GossipSet<u128>, Message>>, args: Args)
where
    G: Gossip<Message, GossipSet<u128>> + Send + 'static,
    G::Error: Send + Debug,
{
    let mut threads = Vec::with_capacity(network.len());
    let mut senders = Vec::with_capacity(network.len());
    for node in network.into_iter() {
        senders.push(node.sender);
        threads.push(spawn(move || run_node(node.gossip, node.receiver)))
    }

    info!("Running");
    let loss_timeout = if args.lost_time_millis == 0 {
        None
    } else {
        Some(Duration::from_millis(args.lost_time_millis))
    };
    let start = Instant::now();
    let (tx, rx) = mpsc::channel(); // For querying nodes.
    let log_period = Duration::from_secs(1); // How long to wait between log messages
    let mut next_log_target = start + log_period;
    let end = start + Duration::from_secs(args.time);
    let mut aggregator = create_aggregator(&args);
    while Instant::now() < end {
        // Generate a random element to insert, and choose a start and target node
        let element: u128 = thread_rng().gen();
        let start_node = thread_rng().gen_range(0..senders.len());
        let target_node = thread_rng().gen_range(0..senders.len());
        // Send the message to add the element
        senders[start_node]
            .send(Message::new(Action::ModifySet(GossipSetAction::Add(
                element,
            ))))
            .unwrap();
        // Wait for the element to appear in the target
        let outcome = wait_for_element(&senders[target_node], element, end, loss_timeout, &tx, &rx);
        match outcome {
            WaitForElementOutcome::Appeared(latency) => {
                aggregator.record_latency(start_node, target_node, latency)
            }
            WaitForElementOutcome::Lost => aggregator.record_loss(start_node, target_node),
            WaitForElementOutcome::EndTimeReached => break,
        }
        let now = Instant::now();
        if now >= next_log_target {
            aggregator.log();
            next_log_target = now + log_period;
        }
    }

    info!("Terminating");
    for sender in senders {
        if let Err(e) = sender.send(Message::new(Action::Terminate)) {
            // There's a race in the end when one node terminates and the other nodes try to gossip to it
            // then those nodes end up failing to send to that node and exit, so I can't send to them...
            // For that I just ignore errors at the end.
            debug!("Error sending terminate signal: {:?}", e);
        }
    }
    for thread in threads {
        if let Err(e) = thread.join().unwrap() {
            // See above why I'm not worried about errors from the threads.
            debug!("Error sending terminate signal: {:?}", e);
        }
    }
}

fn main() {
    let args = Args::parse();
    SimpleLogger::new()
        .with_level(LevelFilter::Info)
        .with_local_timestamps()
        .env()
        .init()
        .unwrap();
    info!("Creating network");
    if args.primaries == 0 {
        run_network(uniform_local_gossip_set(args.nodes, args.fanout), args);
    } else {
        run_network(
            preferential_local_gossip_set(args.nodes, args.primaries, args.fanout),
            args,
        );
    };
}
