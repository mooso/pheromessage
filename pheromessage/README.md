# Overview

This library implements some gossip algorithms in a generic manner that can be used in a real gossip network or in local simulations for gossiping between threads.

# Structure of the code

* `lib.rs` defines the basic API and implementations of the main gossip algorithms
* `data.rs` implements some of the data structures that can be used as the underlying data to be gossipped about
* `net.rs` implements gossip over actual networks (in UDP)
* `channel.rs` implements gossip on a single machine using channel communications
* `multiplex.rs` is a more scalable implementation of gossip on a single machine, where many nodes can share a channel/thread
* `postmessage.rs` implements basic message serialization over the network

There's also an example program - `lset.rs` - for basic local simulation and benchmark data.