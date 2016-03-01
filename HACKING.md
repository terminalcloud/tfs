# TFS Internals Overview

`tfs` is developed as a standard rust project using `cargo`.

Common Tasks:
  - build: `cargo build`
  - test fast: `cargo test`
  - test slow: `cargo test --features slow-test`
  - benchmark: `cargo bench --features nightly` (requires nightly rust)
  - documentation: `cargo doc --open` (requires a browser)

## Environment Requirements

`tfs` is continuously tested on these targets:
  - `i686-unknown-linux-gnu`
  - `i686-apple-darwin`
  - `x86_64-unknown-linux-musl`
  - `x86_64-unknown-linux-gnu`
  - `x86_64-apple-darwin`

Currently, no system packages are required to develop and test.
In the future this is likely to change, especially to require `FUSE` for
actually binding the library to the host filesystem. When that happens
these instructions will be updated to explain installation and configuration
of any non-cargo dependencies for all of the above targets.

## Structural Overview

The main entry point to the library is the `Fs` type, which is a
composition of the `LocalFs` type coupled with a `Storage` and 0 or
more `Cache` implementations.

All external users interact with the library via `Fs`. `Fs` dispatches
reads, writes, and other actions to its constituent types:
  - `LocalFs` is responsible for the management of all local data on disk
  - `Storage` is responsible for durably replicating data "in the cloud"
  - `Cache`s are responsible for providing a faster way to fetch data "from the cloud"
    - `Cache`s are optional. They can be used to increase performance, but are
      necessary for correctness.

### LocalFs

`LocalFs` manages all on-disk data and all local state. It is primarily
made up of four components:
  - A map of all currently open volumes, each containing in-memory state of
    that volume.
  - A least-recently-used map of all currently loaded immutable chunks
  - A pool of "flushing" threads which move data from the local store to a
    backend Storage (initialized by `Fs`)
  - A pool of "syncing" threads which translate in-memory state to disk
  - An IndexedSparseFile abstraction which holds all data on disk (and which
    Chunk objects "point" into via an Index)

Every open volume in a `LocalFs` is mutable, and can receive both read and
write actions to any of its blocks, which eventually translate to state
transitions on the internal Chunks objects. `Volume`s effectively provide a
mapping from external `VolumeId/BlockIndex` to internal `Chunk` object.

#### Chunks

All `Chunk`s are in one of two states: mutable, or immutable, as represented
by the `Chunk` enum. Both states are represented as a `Monitor` over a state
machine, which can be asked to execute state transitions *or* wait for a
transition to a desired state. `LocalFs` code uses these `wait_for`/`complete`
methods to compose actions on `Chunk`s.

For significantly more detail on the precise semantics of the `Chunk` state
machines, see the documentation on `Chunk`, `ImmutableChunk` and
`MutableChunk`.

Flushing and syncing threads also cause state transitions, and often the
`LocalFs` will await transitions originating from the syncing or flushing
threads.

