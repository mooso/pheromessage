//! Driver for running a local network of gossip nodes as threads that talk to each other.

use std::{
    fmt::Debug,
    sync::mpsc::{self, RecvTimeoutError},
    thread::spawn,
    time::{Duration, Instant},
};

use clap::Parser;
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
}

/// The action that can be taken by each node upon receiving a message.
#[derive(Debug, Clone)]
enum Action {
    /// Modify the set and gossip about it.
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
        if let Action::ModifySet(action) = message.action {
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
            Action::ModifySet(_) => gossip.receive(&message)?,
            Action::Terminate => break,
            Action::Query { element, answer } => {
                answer.send(gossip.data().is_present(&element)).unwrap()
            }
        }
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, Default)]
struct LatencyAggregate {
    total_latency: Duration,
    num_elements: usize,
}

impl LatencyAggregate {
    pub fn add_point(&mut self, latency: Duration) {
        self.num_elements += 1;
        self.total_latency += latency;
    }

    pub fn mean_micros(&self) -> f64 {
        self.total_latency.as_micros() as f64 / self.num_elements as f64
    }
}

trait Aggregator {
    fn record_latency(&mut self, source_index: usize, target_index: usize, latency: Duration);
    fn log(&self);
}

#[derive(Debug, Clone, Copy, Default)]
struct UniformGossipAggregator {
    aggregate: LatencyAggregate,
}

impl Aggregator for UniformGossipAggregator {
    fn record_latency(&mut self, _source_index: usize, _target_index: usize, latency: Duration) {
        self.aggregate.add_point(latency);
    }

    fn log(&self) {
        info!(
            "Inserted {} elements with an average latency of {:.2} us",
            self.aggregate.num_elements,
            self.aggregate.mean_micros()
        );
    }
}

#[derive(Debug, Clone, Copy)]
struct PreferentialGossipAggregator {
    primaries_aggregate: LatencyAggregate,
    secondaries_aggregate: LatencyAggregate,
    num_primaries: usize,
}

impl PreferentialGossipAggregator {
    pub fn new(num_primaries: usize) -> PreferentialGossipAggregator {
        PreferentialGossipAggregator {
            primaries_aggregate: Default::default(),
            secondaries_aggregate: Default::default(),
            num_primaries,
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

    fn log(&self) {
        info!(
            "Inserted {} elements. Primaries average latency is {:.2} us. Secondaries average latency is {:.2} us.",
            self.primaries_aggregate.num_elements + self.secondaries_aggregate.num_elements,
            self.primaries_aggregate.mean_micros(),
            self.secondaries_aggregate.mean_micros(),
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
        let insertion_time = Instant::now();
        // Keep checking for the element in the target node until it appears
        loop {
            senders[target_node]
                .send(Message::new(Action::Query {
                    element,
                    answer: tx.clone(),
                }))
                .unwrap();
            // Don't wait for the answer beyond our end time
            let now = Instant::now();
            if now >= end {
                break;
            }
            let timeout = end - now;
            let answer = match rx.recv_timeout(timeout) {
                Ok(answer) => answer,
                Err(RecvTimeoutError::Timeout) => break,
                Err(e) => Err(e).unwrap(),
            };
            if answer {
                // The target node has seen the element we inserted.
                let now = Instant::now();
                let latency = now - insertion_time;
                aggregator.record_latency(start_node, target_node, latency);
                if now >= next_log_target {
                    aggregator.log();
                    next_log_target = now + log_period;
                }
                // Done with this element, move on.
                break;
            }
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
