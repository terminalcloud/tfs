#!/bin/bash

set -ex

# Build and test our library.
cargo build --target $TARGET
cargo test --target $TARGET

# Build and test tfs-fuse-sys as well
pushd tfs-fuse-sys
cargo build --target $TARGET
cargo test --target $TARGET
popd

# If we are nightly run benchmarks.
if rustc --version | grep nightly; then
    cargo bench --features nightly --target $TARGET
fi

# Build our deployment candidate.
cargo build --target $TARGET --release

