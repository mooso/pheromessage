//! Driver for running a local network of gossip nodes as threads that talk to each other.

use std::{
    sync::mpsc::{self, RecvTimeoutError},
    thread::spawn,
    time::{Duration, Instant},
};

use clap::Parser;
use log::{info, LevelFilter};
use pheromessage::{
    channel::uniform_local_gossip_set,
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

fn main() {
    let args = Args::parse();
    SimpleLogger::new()
        .with_level(LevelFilter::Info)
        .with_local_timestamps()
        .env()
        .init()
        .unwrap();
    if args.primaries > 0 {
        todo!("Not implemented yet");
    }

    info!("Creating network");
    let network = uniform_local_gossip_set(args.nodes, args.fanout);
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
    let mut elements_inserted = 0;
    let mut total_latency = Duration::ZERO;
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
                total_latency += latency;
                elements_inserted += 1;
                if now >= next_log_target {
                    info!(
                        "Inserted {} elements with an average latency of {:.2} us",
                        elements_inserted,
                        (total_latency.as_micros() as f64) / (elements_inserted as f64)
                    );
                    next_log_target = now + log_period;
                }
                // Done with this element, move on.
                break;
            }
        }
    }

    info!("Terminating");
    for sender in senders {
        sender.send(Message::new(Action::Terminate)).unwrap();
    }
    for thread in threads {
        thread.join().unwrap().unwrap();
    }
}
