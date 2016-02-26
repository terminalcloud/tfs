#!/bin/bash

set -ex

# Build and test our library.
cargo build --target $TARGET
cargo test --target $TARGET

# If we are nightly run benchmarks.
if rustc --version | grep nightly; then
    cargo bench --features nightly --target $TARGET
fi

# Build our deployment candidate.
cargo build --target $TARGET --release

