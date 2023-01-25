//! An illustration WASM package for showing how the different gossip algorithms
//! in `pheromessage` work to disseminate changes. It simulates a cult of goose
//! watchers trying to guess the true color of the magic goose by adding/removing
//! a `PrimaryColor` to the shared `GossipSet`.

use std::{cell::RefCell, collections::VecDeque, iter, rc::Rc};

use pheromessage::{
    data::{GossipSet, GossipSetMessage},
    Delivery, Gossip, Message, UniformGossip,
};
use wasm_bindgen::prelude::*;

/// An implementation of `Delivery` that delivers to `RefCell<VecDeque>` receivers as endpoints.
/// Since in this wasm library we'll never be multi-threaded, it's safe to just put messages
/// into a queue for each node in the gossip network.
struct Queues();
/// The singleton `Queues`.
const QUEUES: Queues = Queues();

impl<M> Delivery<M, Rc<RefCell<VecDeque<M>>>> for Queues
where
    M: Clone,
{
    type Error = ();

    fn deliver<'a, I>(&self, message: &M, endpoints: I) -> Result<(), ()>
    where
        I: ExactSizeIterator<Item = &'a Rc<RefCell<VecDeque<M>>>>,
        M: 'a,
    {
        for endpoint in endpoints {
            endpoint.borrow_mut().push_back(message.clone());
        }
        Ok(())
    }
}

/// A primary color that can be added/removed to the shared set for the guess
/// of the true color of the magic goose.
#[wasm_bindgen]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PrimaryColor {
    Red,
    Blue,
    Green,
}

/// An individual in the cult watching for the true color of the goose.
#[derive(Debug)]
struct GooseWatcher<G>
where
    G: Gossip<GossipSetMessage<PrimaryColor>, GossipSet<PrimaryColor>>,
{
    /// The message queue for messages intended for this watcher.
    message_queue: GooseWatcherPeer,
    /// The gossip node for this watcher.
    gossip: G,
}

/// The entire cult of the goose watchers.
struct GooseCult<G>
where
    G: Gossip<GossipSetMessage<PrimaryColor>, GossipSet<PrimaryColor>>,
{
    /// A buffer that holds the current information for each watcher as to the
    /// current color of the magic goose. This is derivable from `watchers`, but
    /// kept here allocated to communicate with the browser/JS.
    watcher_colors: Vec<u32>,
    /// The individual watchers in the cult.
    watchers: Vec<GooseWatcher<G>>,
}

/// The type of peer in the network (a reference to their message queue).
type GooseWatcherPeer = Rc<RefCell<VecDeque<GossipSetMessage<PrimaryColor>>>>;
// The type of message ID
type MesageId = <GossipSetMessage<PrimaryColor> as Message>::I;
/// The type of uniform gossip for this network.
type UniformGooseGossip =
    UniformGossip<GooseWatcherPeer, GossipSet<PrimaryColor>, Queues, MesageId>;

/// Create a new uniform cult with the given number of watchers and the fanout to use
/// for gossipping.
fn new_uniform(num_watchers: usize, fanout: usize) -> GooseCult<UniformGooseGossip> {
    let watcher_colors = iter::repeat(0_u32).take(num_watchers).collect();
    let message_queues: Vec<GooseWatcherPeer> =
        iter::repeat_with(Rc::default).take(num_watchers).collect();
    // First create a Vec<> with all the gossips
    let mut gossips = Vec::with_capacity(num_watchers);
    for i in 0..num_watchers {
        // Create an empty set
        let data = GossipSet::default();
        // Create the set of senders (peers) for the node
        let mut peers = Vec::with_capacity(num_watchers - 1);
        for (j, other) in message_queues.iter().enumerate() {
            // Add every message queue except the one for the node
            if i != j {
                peers.push(other.clone());
            }
        }
        // Add the node
        gossips.push(UniformGossip::create(peers, fanout, data, QUEUES));
    }
    // Then add the message queues to create the network
    let watchers = gossips
        .into_iter()
        .zip(message_queues.into_iter())
        .map(|(gossip, message_queue)| GooseWatcher {
            gossip,
            message_queue,
        })
        .collect();
    GooseCult {
        watcher_colors,
        watchers,
    }
}

/// Convert a set of `PrimaryColor` into a color.
fn color_for_set(color_set: &GossipSet<PrimaryColor>) -> u32 {
    let mut color = 0;
    if color_set.is_present(&PrimaryColor::Red) {
        color |= 0xFF0000;
    }
    if color_set.is_present(&PrimaryColor::Green) {
        color |= 0x00FF00;
    }
    if color_set.is_present(&PrimaryColor::Blue) {
        color |= 0x0000FF;
    }
    color
}

impl<G> GooseCult<G>
where
    G: Gossip<GossipSetMessage<PrimaryColor>, GossipSet<PrimaryColor>>,
    <G as Gossip<GossipSetMessage<PrimaryColor>, GossipSet<PrimaryColor>>>::Error: std::fmt::Debug,
{
    /// Tick forward in the simulation - process one message per watcher.
    /// Returns false if the network is now idle (no messages remaining).
    pub fn tick(&mut self) -> bool {
        let mut some_messages_remaining = false;
        for (watcher, color) in self.watchers.iter_mut().zip(self.watcher_colors.iter_mut()) {
            if let Some(message) = watcher.message_queue.borrow_mut().pop_front() {
                watcher.gossip.receive(&message).unwrap();
                *color = color_for_set(watcher.gossip.data());
                some_messages_remaining = true;
            }
        }
        some_messages_remaining
    }

    /// Add a color to the gossip starting at the given watcher index (`inspired_watcher`).
    pub fn add_color(&mut self, inspired_watcher: usize, color: PrimaryColor) {
        self.watchers[inspired_watcher]
            .gossip
            .update(&GossipSetMessage::add(color))
            .unwrap();
        self.watcher_colors[inspired_watcher] =
            color_for_set(self.watchers[inspired_watcher].gossip.data());
    }

    /// Remove a color from the gossip starting at the given watcher index (`inspired_watcher`).
    pub fn remove_color(&mut self, inspired_watcher: usize, color: PrimaryColor) {
        self.watchers[inspired_watcher]
            .gossip
            .update(&GossipSetMessage::remove(color))
            .unwrap();
        self.watcher_colors[inspired_watcher] =
            color_for_set(self.watchers[inspired_watcher].gossip.data());
    }
}

/// A uniform cult of goose watchers, where all watchers are the same.
#[wasm_bindgen]
pub struct UniformCult {
    cult: GooseCult<UniformGooseGossip>,
}

#[wasm_bindgen]
impl UniformCult {
    /// Create a new uniform cult of the given number of watchers and `fanout` to use
    /// while gossipping (the number of watchers to talk to when spreading gossip).
    pub fn new(num_watchers: usize, fanout: usize) -> UniformCult {
        UniformCult {
            cult: new_uniform(num_watchers, fanout),
        }
    }

    /// The current knowledge of the color of the goose as an array of size `num_watchers`.
    pub fn colors(&self) -> *const u32 {
        self.cult.watcher_colors.as_ptr()
    }

    /// Tick forward in the simulation - process one message per watcher.
    /// Returns false if the network is now idle (no messages remaining).
    pub fn tick(&mut self) -> bool {
        self.cult.tick()
    }

    /// Add a color to the gossip starting at the given watcher index (`inspired_watcher`).
    pub fn add_color(&mut self, inspired_watcher: usize, color: PrimaryColor) {
        self.cult.add_color(inspired_watcher, color);
    }

    /// Remove a color from the gossip starting at the given watcher index (`inspired_watcher`).
    pub fn remove_color(&mut self, inspired_watcher: usize, color: PrimaryColor) {
        self.cult.remove_color(inspired_watcher, color);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn add_blue() {
        let mut cult = UniformCult::new(100, 10);
        cult.add_color(50, PrimaryColor::Blue);
        assert_eq!(255, cult.cult.watcher_colors[50]);
        cult.tick();
        let num_blue = cult
            .cult
            .watcher_colors
            .iter()
            .filter(|c| **c == 255)
            .count();
        assert!(num_blue > 10);
    }

    #[test]
    fn add_red() {
        let mut cult = UniformCult::new(100, 10);
        cult.add_color(50, PrimaryColor::Red);
        assert_eq!(255 << 16, cult.cult.watcher_colors[50]);
    }
}
