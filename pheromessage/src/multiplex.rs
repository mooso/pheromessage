//! Implementation of Gossip over local `mpsc` channels where groups of gossip nodes can each share a channel.
//! This is meant to scale local simulations to have N nodes per thread.

use rand::{prelude::*, seq::index::sample};
use std::{
    hash::Hash,
    marker::PhantomData,
    sync::mpsc::{self, SendError},
};

use crate::{
    data::GossipSet, Delivery, Gossip, Message, PreferentialGossip, SharedData, UniformGossip,
};

/// An implementation of `Delivery` that delivers to `mpsc` receivers as shared endpoints for a group of nodes.
pub struct Multiplex();

/// The singleton `Multiplex`.
pub const MULTIPLEX: Multiplex = Multiplex();

/// An envelope for a message intended for a node within a node group.
#[derive(Clone)]
pub struct Envelope<M> {
    /// The message.
    pub message: M,
    /// The index of the node within the node group.
    pub node_index: usize,
}

/// An endpoint for a peer within a gossip network composed of nodes within node groups.
#[derive(Clone)]
pub struct MultiplexEndpoint<M> {
    /// The sender for sending envelopes to the node group.
    sender: mpsc::Sender<Envelope<M>>,
    /// The index of the node within the node group.
    node_index: usize,
}

impl<M> Delivery<M, MultiplexEndpoint<M>> for Multiplex
where
    M: Clone,
{
    type Error = SendError<Envelope<M>>;

    fn deliver<'a, I>(&self, message: &M, endpoints: I) -> Result<(), Self::Error>
    where
        I: ExactSizeIterator<Item = &'a MultiplexEndpoint<M>>,
        M: 'a,
    {
        for endpoint in endpoints {
            endpoint.sender.send(Envelope {
                message: message.clone(),
                node_index: endpoint.node_index,
            })?;
        }
        Ok(())
    }
}

/// A representation of a uniform gossip "node group" that is a local `mpsc` receiver
/// and the gossips for it.
pub struct LocalGossipNodeGroup<G, S, M>
where
    G: Gossip<M, S>,
{
    /// The gossips for the nodes in this group.
    pub gossips: Vec<G>,
    /// The receiver for messages intended for this node group.
    pub receiver: mpsc::Receiver<Envelope<M>>,
    /// The sender of messages to this node group.
    pub sender: mpsc::Sender<Envelope<M>>,
    _s: PhantomData<S>,
}

/// A representation of a gossip "node group" that is a local `mpsc` receiver using uniform gossip technique.
pub type LocalUniformGossipSetNodeGroup<T, M, I> = LocalGossipNodeGroup<
    UniformGossip<MultiplexEndpoint<M>, GossipSet<T>, Multiplex, I>,
    GossipSet<T>,
    M,
>;

/// A representation of a gossip "node group" that is a local `mpsc` receiver using preferential gossip technique.
pub type LocalPreferentialGossipSetNodeGroup<T, M, I> = LocalGossipNodeGroup<
    PreferentialGossip<MultiplexEndpoint<M>, GossipSet<T>, Multiplex, I>,
    GossipSet<T>,
    M,
>;

/// Information about which group a node belongs to, and its index within the group.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct NodeGroupInfo {
    /// The index of the group the node belongs in.
    pub group_index: usize,
    /// The index of the node within the group.
    pub node_index: usize,
}

impl NodeGroupInfo {
    /// Gets the group info for a node given the total number of groups and its global index.
    pub fn for_node(num_groups: usize, global_node_index: usize) -> NodeGroupInfo {
        let group_index = global_node_index % num_groups;
        let node_index = global_node_index / num_groups;
        NodeGroupInfo {
            group_index,
            node_index,
        }
    }
}

/// Creates a set of local gossip "node groups" that maintain a gossip set.
/// Each node group can be independently maintained in its own thread.
/// Each node can gossip with any other node in its own or other groups.
/// `peers_per_node` is the number of peers every node knows about - if set to
/// `num_nodes - 1` (the maximum) then every node will know about every other but
/// that can take up a lot of memory in larger networks, so may be set to lower and
/// each node will know of a random subset of other nodes.
/// `T` is the type of element in the set, and `M` is the type of messages exchanged
/// in the gossip.
pub fn uniform_local_gossip_set<T, M>(
    num_nodes: usize,
    num_groups: usize,
    peers_per_node: usize,
    fanout: usize,
) -> Vec<LocalUniformGossipSetNodeGroup<T, M, M::I>>
where
    M: Clone + Message,
    GossipSet<T>: SharedData<M>,
    <M as Message>::I: Hash + Eq,
{
    // Create the senders and receivers for the node groups.
    let channels: Vec<_> = (0..num_groups).map(|_| mpsc::channel()).collect();
    // First create a Vec<> of Vec<>s with all the gossips
    let nodes_per_group_max = (num_nodes / num_groups) + 1;
    let mut gossips: Vec<_> = (0..num_groups)
        .map(|_| Vec::with_capacity(nodes_per_group_max))
        .collect();
    let mut rng = thread_rng();
    for i in 0..num_nodes {
        // Create an empty set
        let data = GossipSet::default();
        // Create the set of peers for the node
        let peers: Vec<_> = sample(&mut rng, num_nodes - 1, peers_per_node)
            .iter()
            .map(|j| if j < i { j } else { j + 1 })
            .map(|j| {
                let group_info = NodeGroupInfo::for_node(num_groups, j);
                MultiplexEndpoint {
                    sender: channels[group_info.group_index].0.clone(),
                    node_index: group_info.node_index,
                }
            })
            .collect();
        // Add the node
        let group_info = NodeGroupInfo::for_node(num_groups, i);
        gossips[group_info.group_index].push(UniformGossip::create(peers, fanout, data, MULTIPLEX));
    }
    // Then add the senders and receivers to create the network
    gossips
        .into_iter()
        .zip(channels.into_iter())
        .map(|(gossips, (sender, receiver))| LocalGossipNodeGroup {
            gossips,
            receiver,
            sender,
            _s: PhantomData,
        })
        .collect()
}

/// Creates a set of local gossip "node groups" that maintain a gossip set.
/// Each node group can be independently maintained in its own thread.
/// Each node can gossip with any other node in its own or other groups.
/// The first `num_primaries` nodes will be the primary nodes that preferentially
/// get first word of any update, with the rest being secondaries.
/// `peers_per_node` is the number of peers every node knows about - if set to
/// `num_nodes - 1` (the maximum) then every node will know about every other but
/// that can take up a lot of memory in larger networks, so may be set to lower and
/// each node will know of a random subset of other nodes.
/// `T` is the type of element in the set, and `M` is the type of messages exchanged
/// in the gossip.
pub fn preferential_local_gossip_set<T, M>(
    num_nodes: usize,
    num_groups: usize,
    peers_per_node: usize,
    num_primaries: usize,
    fanout: usize,
) -> Vec<LocalPreferentialGossipSetNodeGroup<T, M, M::I>>
where
    M: Clone + Message,
    GossipSet<T>: SharedData<M>,
    <M as Message>::I: Hash + Eq,
{
    // Create the senders and receivers for the node groups.
    let channels: Vec<_> = (0..num_nodes).map(|_| mpsc::channel()).collect();
    // First create a Vec<> with all the gossips
    // First create a Vec<> of Vec<>s with all the gossips
    let nodes_per_group_max = (num_nodes / num_groups) + 1;
    let mut gossips: Vec<_> = (0..num_groups)
        .map(|_| Vec::with_capacity(nodes_per_group_max))
        .collect();
    let mut rng = thread_rng();
    for i in 0..num_nodes {
        // Create an empty set
        let data = GossipSet::default();
        // Create the set of peers for the node
        let primary = i < num_primaries;
        let mut primaries = Vec::with_capacity(peers_per_node);
        let mut secondaries = Vec::with_capacity(peers_per_node);
        sample(&mut rng, num_nodes - 1, peers_per_node)
            .iter()
            .map(|j| if j < i { j } else { j + 1 })
            .for_each(|j| {
                let group_info = NodeGroupInfo::for_node(num_groups, j);
                let endpoint = MultiplexEndpoint {
                    sender: channels[group_info.group_index].0.clone(),
                    node_index: group_info.node_index,
                };
                if j < num_primaries {
                    primaries.push(endpoint);
                } else {
                    secondaries.push(endpoint);
                }
            });
        // Add the node
        let group_info = NodeGroupInfo::for_node(num_groups, i);
        gossips[group_info.group_index].push(PreferentialGossip::create(
            primaries,
            secondaries,
            primary,
            fanout,
            data,
            MULTIPLEX,
        ));
    }
    // Then add the senders and receivers to create the network
    gossips
        .into_iter()
        .zip(channels.into_iter())
        .map(|(gossips, (sender, receiver))| LocalGossipNodeGroup {
            gossips,
            receiver,
            sender,
            _s: PhantomData,
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
    use rayon::{prelude::*, ThreadPoolBuilder};

    /// End-to-end test of a local gossip network.
    #[test]
    fn local_network() {
        let num_nodes = 12;
        let num_groups = 5;
        let peers_per_node = 11;
        let fanout = 6;
        // Create a thread pool with a thread per node group (regardless of number of cores,
        // this is for testing and the threads will sleep at various points).
        let pool = ThreadPoolBuilder::new()
            .num_threads(num_nodes)
            .build()
            .unwrap();
        let all_sets: Vec<_> = pool.install(|| {
            // Create the gossip network.
            let set = uniform_local_gossip_set(num_nodes, num_groups, peers_per_node, fanout);
            // Create an arbitrary set of operations to add the numbers 0..100, but
            // remove the numbers 20..40
            let mut operations: Vec<_> = (0..100)
                .map(|i| GossipSetMessage::add(i))
                .chain((20..40).map(|i| GossipSetMessage::remove(i)))
                .collect();
            // Since the gossip network is resilient to whatever order of operations,
            // shuffle the operations for fun.
            operations.shuffle(&mut thread_rng());
            // Assign each group a subset of operations.
            let ops_per_group = operations.len() / num_groups;
            let num_finished = Arc::new(AtomicUsize::new(0));
            let mut group_with_work = Vec::with_capacity(set.len());
            for group in set.into_iter() {
                let work: Vec<_> = operations.drain(..ops_per_group).collect();
                group_with_work.push((group, work, num_finished.clone()));
            }
            // Map every node group with its assigned work to a thread
            let all_sets: Vec<_> = group_with_work
                .into_par_iter()
                .map(|n| {
                    let mut group = n.0;
                    let mut work = n.1;
                    let num_finished = n.2;
                    let mut node_index = 0;
                    // First go through the work one by one.
                    while let Some(to_send) = work.pop() {
                        group.gossips[node_index].update(&to_send).unwrap();
                        node_index = (node_index + 1) % group.gossips.len();
                        // After sending it, busy-wait a random time before sending the next op.
                        let mut random_wait =
                            Duration::from_millis(thread_rng().gen_range(10..100));
                        let end_wait = Instant::now() + random_wait;
                        // Process the messages while waiting.
                        while let Ok(envelope) = group.receiver.recv_timeout(random_wait) {
                            group.gossips[envelope.node_index]
                                .receive(&envelope.message)
                                .unwrap();
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
                        match group.receiver.recv_timeout(poll_time) {
                            Ok(envelope) => group.gossips[envelope.node_index]
                                .receive(&envelope.message)
                                .unwrap(),
                            Err(RecvTimeoutError::Disconnected) => break,
                            Err(RecvTimeoutError::Timeout) => {
                                if num_finished.load(Ordering::Relaxed) >= num_groups {
                                    break;
                                }
                            }
                        }
                    }
                    group.gossips.into_iter().map(|g| g.data)
                })
                .collect();
            all_sets.into_iter().flatten().collect()
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
