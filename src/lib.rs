use rand::prelude::*;
use std::{
    collections::{HashMap, HashSet},
    hash::Hash,
};

pub mod channel;
pub mod data;
pub mod net;
#[cfg(feature = "postcard")]
pub mod postmessage;

/// Delivery mechanism for delivering messages (`M`) to endpoints (`P`).
pub trait Delivery<M, P> {
    type Error;

    /// Deliver the given message to the given endpoints.
    fn deliver<'a, I>(&self, message: &M, endpoints: I) -> Result<(), Self::Error>
    where
        I: ExactSizeIterator<Item = &'a P>,
        P: 'a;
}

/// A gossip mechanism for maintaining shared data and updating it by gossiping with peers.
pub trait Gossip<M, S> {
    type Error;

    /// Indicate that the given message has been received from a peer.
    fn receive(&mut self, message: &M) -> Result<(), Self::Error>;

    /// Update the data by the given message and gossip it.
    fn update(&mut self, message: &M) -> Result<(), Self::Error>;

    /// The underlying data being gossipped about.
    fn data(&self) -> &S;
}

/// A message that can update shared data.
pub trait Message {
    type I;

    /// The unique ID of the message.
    fn id(&self) -> Self::I;
}

/// A shared data structure that can be maintained through gossip.
pub trait SharedData<M> {
    /// Update the data using the data in the given message.
    fn update(&mut self, message: &M);
}

/// A gossip mechanism that treats all peers equally in updating them.
pub struct UniformGossip<P, S, D, I> {
    /// The set of peers.
    pub peers: Vec<P>,
    /// Set of all message IDs seen so far.
    seen_messages: HashSet<I>,
    /// The delivery mechanism to send gossip messages.
    pub delivery: D,
    /// The data being gossipped about.
    pub data: S,
    /// How many peers to reach out to when gossipping.
    pub fanout: usize,
}

impl<P, S, D, I> UniformGossip<P, S, D, I> {
    /// Create a new uniform gossip mechanism that will gossip to the given set of `peers`,
    /// using the given `delivery` mechanism and maintaining the given `data`.
    /// The gossip will be done using the given `fanout` - each message will be delivered
    /// to a random subset of peers of that size.
    pub fn create(peers: Vec<P>, fanout: usize, data: S, delivery: D) -> UniformGossip<P, S, D, I> {
        UniformGossip {
            peers,
            seen_messages: HashSet::new(),
            delivery,
            data,
            fanout,
        }
    }
}

impl<P, S, D, M, I> Gossip<M, S> for UniformGossip<P, S, D, I>
where
    M: Message<I = I>,
    D: Delivery<M, P>,
    I: Eq + Hash,
    S: SharedData<M>,
{
    type Error = D::Error;

    fn receive(&mut self, message: &M) -> Result<(), Self::Error> {
        // Mark the message as seen
        let id = message.id();
        let new = self.seen_messages.insert(id);
        // Only pass the message on if I've never seen it before, otherwise it's a repeat so throw it away.
        if new {
            // This is the first time I see this message, update my data and pass it on.
            self.data.update(message);
            gossip(&self.delivery, message, &self.peers, self.fanout)?;
        }
        Ok(())
    }

    fn update(&mut self, message: &M) -> Result<(), Self::Error> {
        // Update my data.
        self.data.update(message);
        // Mark it as seen.
        self.seen_messages.insert(message.id());
        // Pass it on to my peers.
        gossip(&self.delivery, message, &self.peers, self.fanout)
    }

    fn data(&self) -> &S {
        &self.data
    }
}

/// Indicator for whether I've seen a message only once, twice or more. Primary
/// nodes behave differently based on that in the preferential gossip algorithm.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy)]
enum SeenCount {
    Once,
    Twice,
    MoreThanTwice,
}

impl SeenCount {
    pub fn increment(&mut self) {
        match self {
            SeenCount::Once => *self = SeenCount::Twice,
            SeenCount::Twice => *self = SeenCount::MoreThanTwice,
            SeenCount::MoreThanTwice => (),
        }
    }
}

/// A gossip mechanism that treats a subset of peers as primaries that should get priority
/// in getting updates faster.
pub struct PreferentialGossip<P, S, D, I> {
    /// The endpoints for all the primary peers.
    primaries: Vec<P>,
    /// The endpoints for all the rest of the peers (not primary).
    secondaries: Vec<P>,
    /// Count of how often I've seen each message by ID.
    message_log: HashMap<I, SeenCount>,
    /// Whether I myself am primary or secondary.
    primary: bool,
    /// The delivery mechanism to send gossip messages.
    delivery: D,
    /// The data being gossipped about.
    data: S,
    /// How many peers to reach out to when gossipping.
    fanout: usize,
}

impl<P, S, D, I> PreferentialGossip<P, S, D, I> {
    fn increment_seen(&mut self, message_id: I) -> SeenCount
    where
        I: Eq + Hash,
    {
        *self
            .message_log
            .entry(message_id)
            .and_modify(|count| count.increment())
            .or_insert(SeenCount::Once)
    }
}

impl<P, S, D, M, I> Gossip<M, S> for PreferentialGossip<P, S, D, I>
where
    M: Message<I = I>,
    D: Delivery<M, P>,
    I: Eq + Hash,
    S: SharedData<M>,
{
    type Error = D::Error;

    fn receive(&mut self, message: &M) -> Result<(), Self::Error> {
        // Update the amount of times I've seen this message.
        let count_seen = self.increment_seen(message.id());
        if count_seen == SeenCount::Once {
            // This is the first time I've seen this message - update the data.
            self.data.update(message);
        }
        // Now check who I should send the message to - if any - based on if I'm primary
        // and how many times I've seen this message.
        let targets = if self.primary {
            // If I'm primary - I pass it on to other primaries if it's the first time
            // I've seen this message, otherwise I pass it on to secondary if this is
            // the second time I've seen it.
            match count_seen {
                SeenCount::Once => Some(&self.primaries),
                SeenCount::Twice => Some(&self.secondaries),
                SeenCount::MoreThanTwice => None,
            }
        } else if count_seen == SeenCount::Once {
            // I'm secondary and this is the first time I've seen it, pass it on to
            // other secondaries.
            Some(&self.secondaries)
        } else {
            // I'm secondary and I've seen it before, throw it away.
            None
        };
        if let Some(targets) = targets {
            gossip(&self.delivery, message, targets, self.fanout)?;
        }
        Ok(())
    }

    fn update(&mut self, message: &M) -> Result<(), Self::Error> {
        self.data.update(message);
        self.increment_seen(message.id());
        gossip(&self.delivery, message, &self.primaries, self.fanout)
    }

    fn data(&self) -> &S {
        &self.data
    }
}

/// Gossip the given `message` to a random subset of size `fanout` of `targets`.
fn gossip<P, D, M, I>(
    delivery: &D,
    message: &M,
    targets: &Vec<P>,
    fanout: usize,
) -> Result<(), D::Error>
where
    M: Message<I = I>,
    D: Delivery<M, P>,
    I: Eq + Hash,
{
    let mut rng = rand::thread_rng();
    let chosen = targets.choose_multiple(&mut rng, fanout);
    delivery.deliver(message, chosen)
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;

    use super::*;

    /// A "network" that just keeps track of which endpoints (keys) received which messages (values).
    struct Network(RefCell<HashMap<usize, Vec<usize>>>);

    impl Delivery<usize, usize> for Network {
        type Error = ();

        fn deliver<'a, I>(&self, message: &usize, endpoints: I) -> Result<(), ()>
        where
            I: ExactSizeIterator<Item = &'a usize>,
        {
            for endpoint in endpoints {
                self.0
                    .borrow_mut()
                    .entry(*endpoint)
                    .or_default()
                    .push(*message);
            }
            Ok(())
        }
    }

    /// Implement `Message` for usize for testing purposes.
    impl Message for usize {
        type I = Self;

        fn id(&self) -> Self {
            *self
        }
    }

    /// When gossipping to the entire network, all of them should receive it.
    #[test]
    fn gossip_to_all() {
        let network = Network(RefCell::new(HashMap::new()));
        gossip(&network, &10, &vec![1, 2, 3], 3).unwrap();
        assert_eq!(Some(&vec![10]), network.0.borrow().get(&1));
        assert_eq!(Some(&vec![10]), network.0.borrow().get(&2));
        assert_eq!(Some(&vec![10]), network.0.borrow().get(&3));
    }

    /// When gossipping to a subset of the network, just that subset should receive it.
    #[test]
    fn gossip_to_some() {
        let network = Network(RefCell::new(HashMap::new()));
        gossip(&network, &10, &vec![1, 2, 3, 4, 5], 3).unwrap();
        assert_eq!(3, network.0.borrow().len());
    }
}
