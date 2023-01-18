//! Implementation of Gossip over local `mpsc` channels.

use std::sync::mpsc::{self, SendError};

use crate::{data::GossipSet, Delivery, UniformGossip};

/// An implementation of `Delivery` that delivers to `mpsc` receivers as endpoints.
pub struct Channels();

/// The singleton `Channels`.
pub const CHANNELS: Channels = Channels();

impl<M> Delivery<M, mpsc::Sender<M>> for Channels
where
    M: Clone,
{
    type Error = SendError<M>;

    fn deliver<'a, I>(&self, message: &M, endpoints: I) -> Result<(), SendError<M>>
    where
        I: ExactSizeIterator<Item = &'a mpsc::Sender<M>>,
        M: 'a,
    {
        for endpoint in endpoints {
            endpoint.send(message.clone())?;
        }
        Ok(())
    }
}

/// A representation of a gossip "node" that is a local `mpsc` receiver
/// and the gossip for it.
pub struct LocalUniformGossipNode<S, M, I> {
    pub gossip: UniformGossip<mpsc::Sender<M>, S, Channels, I>,
    pub receiver: mpsc::Receiver<M>,
    pub sender: mpsc::Sender<M>,
}

/// A representation of a local gossip "node" over a gossip set.
pub type LocalUniformGossipSetNode<T, M> = LocalUniformGossipNode<GossipSet<T>, M, u128>;

/// Creates a set of local gossip "nodes" that maintain a gossip set.
/// Each node can be independently maintained in its own thread and will gossip
/// with the other threads.
pub fn uniform_local_gossip_set<T, M>(
    num_nodes: usize,
    fanout: usize,
) -> Vec<LocalUniformGossipSetNode<T, M>> {
    // Create the senders and receivers for the nodes.
    let channels: Vec<_> = (0..num_nodes).map(|_| mpsc::channel()).collect();
    // First create a Vec<> with all the gossips
    let mut gossips = Vec::with_capacity(num_nodes);
    for i in 0..num_nodes {
        // Create an empty set
        let data = GossipSet::default();
        // Create the set of senders (peers) for the node
        let mut peers = Vec::with_capacity(num_nodes - 1);
        for (j, other) in channels.iter().enumerate() {
            // Add every sender except the one for the node
            if i != j {
                peers.push(other.0.clone());
            }
        }
        // Add the node
        gossips.push(UniformGossip::create(peers, fanout, data, CHANNELS));
    }
    // Then add the senders and receivers to create the network
    gossips
        .into_iter()
        .zip(channels.into_iter())
        .map(|(gossip, (sender, receiver))| LocalUniformGossipSetNode {
            gossip,
            receiver,
            sender,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{
            atomic::{AtomicUsize, Ordering},
            mpsc::RecvTimeoutError,
            Arc,
        },
        time::{Duration, Instant},
    };

    use crate::{data::GossipSetMessage, Gossip};

    use super::*;
    use rand::prelude::*;
    use rayon::{prelude::*, ThreadPoolBuilder};

    /// End-to-end test of a local gossip network.
    #[test]
    fn local_network() {
        let num_nodes = 12;
        let fanout = 6;
        // Create a thread pool with a thread per node (regardless of number of cores,
        // this is for testing and the threads will sleep at various points).
        let pool = ThreadPoolBuilder::new()
            .num_threads(num_nodes)
            .build()
            .unwrap();
        let all_sets = pool.install(|| {
            // Create the gossip network.
            let set = uniform_local_gossip_set(num_nodes, fanout);
            // Create an arbitrary set of operations to add the numbers 0..100, but
            // remove the numbers 20..40
            let mut operations: Vec<_> = (0..100)
                .map(|i| GossipSetMessage::add(i))
                .chain((20..40).map(|i| GossipSetMessage::remove(i)))
                .collect();
            // Since the gossip network is resilient to whatever order of operations,
            // shuffle the operations for fun.
            operations.shuffle(&mut thread_rng());
            // Assign each node a subset of operations.
            let ops_per_node = operations.len() / num_nodes;
            let num_finished = Arc::new(AtomicUsize::new(0));
            let mut set_with_work = Vec::with_capacity(set.len());
            for node in set.into_iter() {
                let work: Vec<_> = operations.drain(..ops_per_node).collect();
                set_with_work.push((node, work, num_finished.clone()));
            }
            // Map every node with its assigned work to a thread
            let all_sets: Vec<_> = set_with_work
                .into_par_iter()
                .map(|n| {
                    let mut node = n.0;
                    let mut work = n.1;
                    let num_finished = n.2;
                    // First go through the work one by one.
                    while let Some(to_send) = work.pop() {
                        node.gossip.update(&to_send).unwrap();
                        // After sending it, busy-wait a random time before sending the next op.
                        let mut random_wait =
                            Duration::from_millis(thread_rng().gen_range(10..100));
                        let end_wait = Instant::now() + random_wait;
                        // Process the messages while waiting.
                        while let Ok(message) = node.receiver.recv_timeout(random_wait) {
                            node.gossip.receive(&message).unwrap();
                            let now = Instant::now();
                            if now >= end_wait {
                                break;
                            } else {
                                random_wait = end_wait - now;
                            }
                        }
                    }
                    // All done with my work - mark that.
                    num_finished.fetch_add(1, Ordering::Relaxed);
                    // Keep processing messages until everyone is done, polling the
                    // the flag every millisecond (I'm sure there's a more efficient way
                    // that doesn't rely on polling, but it's a test so I don't care that much).
                    let poll_time = Duration::from_millis(1);
                    loop {
                        match node.receiver.recv_timeout(poll_time) {
                            Ok(message) => node.gossip.receive(&message).unwrap(),
                            Err(RecvTimeoutError::Disconnected) => break,
                            Err(RecvTimeoutError::Timeout) => {
                                if num_finished.load(Ordering::Relaxed) >= num_nodes {
                                    break;
                                }
                            }
                        }
                    }
                    node.gossip.data
                })
                .collect();
            all_sets
        });
        assert_eq!(num_nodes, all_sets.len());
        for set in all_sets {
            for i in 0..100 {
                if i < 20 || i >= 40 {
                    assert!(set.is_present(&i));
                } else {
                    assert!(!set.is_present(&i));
                }
            }
        }
    }
}
