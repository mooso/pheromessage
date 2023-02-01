# Overview

This repo was made to explore the paper "Differentiated Consistency for Worldwide Gossips" in the [thalassophilia](https://www.thalassophilia.org/) site, specificially the [Gossip with favorites](https://www.thalassophilia.org/gossip.html) post.

It is a Rust workspace with two crates:

* `pheromessage` is a library implementing the gossip algorithms in question in a generic way that can be used over real networks or local simulations.
* `pherogoose` is a toy WASM library that uses the `pheromessage` library to implement the animations/illustrations used in the post.

The `scripts` directory contains a few scripts I used to generate the graphs in the post.

# Contibuting/issues

The code here is presented as is. The library was written in a generic way that could theoretically land it in [crates.io](https://crates.io) if there's demand for it - so please create an issue if that's something you could see a use for. There are some essential features/changes I'd want to add/make before such a move:

1. Library overall documentation
1. Better factory API for the more basic use cases - as it stands creating a basic gossip node is too cumbersome
1. API and thought into how to bootstrap a new gossip node into an existing newtork

So if any of that interests you please feel free to create issues/file pull requests.