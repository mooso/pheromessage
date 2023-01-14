//! Shared data sets that can be updated through gossip.

use rand::prelude::*;
use std::{collections::HashMap, hash::Hash};

use crate::{Message, SharedData};

/// An action to add/remove an item to a gossipped set.
#[derive(Debug, PartialEq, Eq, Hash)]
pub enum GossipSetAction<T> {
    Add(T),
    Remove(T),
}

/// Per-item counts of how many times a given item was added/removed in a set.
#[derive(Debug, Default)]
struct ItemActions {
    added_count: usize,
    removed_count: usize,
}

/// A set of unique items maintained through gossip.
#[derive(Debug, Default)]
pub struct GossipSet<T> {
    items: HashMap<T, ItemActions>,
}

/// A message that can be used to ad/remove items from a gossipped set.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct GossipSetMessage<T> {
    id: u128,
    pub action: GossipSetAction<T>,
}

impl<T> Message for GossipSetMessage<T> {
    type I = u128;

    fn id(&self) -> Self::I {
        self.id
    }
}

fn new_id() -> u128 {
    thread_rng().gen()
}

impl<T> GossipSetMessage<T> {
    /// Create a new message to add the given value to a set.
    pub fn add(value: T) -> GossipSetMessage<T> {
        GossipSetMessage {
            id: new_id(),
            action: GossipSetAction::Add(value),
        }
    }

    /// Create a new message to remove the given value from a set.
    pub fn remove(value: T) -> GossipSetMessage<T> {
        GossipSetMessage {
            id: new_id(),
            action: GossipSetAction::Remove(value),
        }
    }
}

impl<T> GossipSet<T> {
    /// Checks if the given item is present in the set.
    pub fn is_present(&self, item: &T) -> bool
    where
        T: Eq + Hash,
    {
        if let Some(v) = self.items.get(item) {
            v.added_count > v.removed_count
        } else {
            false
        }
    }

    /// Adds the given item to the set. Typically you wouldn't call this directly, but
    /// rather update the gossip with an add message to update the whole network.
    pub fn add_item(&mut self, item: T)
    where
        T: Eq + Hash,
    {
        self.items.entry(item).or_default().added_count += 1
    }

    /// Removes the given item from the set. Typically you wouldn't call this directly, but
    /// rather update the gossip with a remove message to update the whole network.
    pub fn remove_item(&mut self, item: T)
    where
        T: Eq + Hash,
    {
        self.items.entry(item).or_default().removed_count += 1
    }
}

impl<T> SharedData<GossipSetMessage<T>> for GossipSet<T>
where
    T: Eq + Hash + Clone,
{
    fn update(&mut self, message: &GossipSetMessage<T>) {
        match &message.action {
            GossipSetAction::Add(v) => self.add_item(v.clone()),
            GossipSetAction::Remove(v) => self.remove_item(v.clone()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    pub fn simple_set() {
        let mut set = GossipSet::default();
        set.update(&GossipSetMessage::add(5));
        assert!(set.is_present(&5));
        assert!(!set.is_present(&6));
        set.update(&GossipSetMessage::add(6));
        assert!(set.is_present(&5));
        assert!(set.is_present(&6));
        set.update(&GossipSetMessage::remove(6));
        assert!(set.is_present(&5));
        assert!(!set.is_present(&6));
        set.update(&GossipSetMessage::remove(6));
        assert!(set.is_present(&5));
        assert!(!set.is_present(&6));
    }
}
