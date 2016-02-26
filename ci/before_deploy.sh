#!/bin/bash

set -ex

if rustc --version | grep '\(beta\|nightly\|dev\)'; then
    echo "On non-stable rust version, no deploy."
    exit 0
fi

# On stable!

# Create a "staging" directory
mkdir staging

# Copy the release binary to staging.
cp target/$TARGET/release/tfs staging

cd staging

# Prepare the release that will be uploaded.
tar czf ../${PROJECT_NAME}-${TRAVIS_TAG}-${TARGET}.tar.gz *

