#!/bin/bash

set -ex

# Build and test other crates as well

pushd tfs-file-ext
cargo build --target $TARGET
cargo test --target $TARGET
popd

# Build and test our library.

cargo build --target $TARGET
sudo env PATH=$PATH cargo test --target $TARGET

sudo env PATH=$PATH cargo clean

# If we are nightly run benchmarks.
if rustc --version | grep nightly; then
    cargo bench --features nightly --target $TARGET
fi

# Build our deployment candidate.
cargo build --target $TARGET --release

