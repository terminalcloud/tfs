#!/bin/bash

set -ex

# Install FUSE if we are on osx.
if [ "$TRAVIS_OS_NAME" == "osx" ]; then
    brew install Caskroom/cask/osxfuse
fi

# Install libraries for cross-compiling if we are cross-compiling:
# NOTE: If we are cross-compiling we also must be stable rust.
case $TARGET in
  # Install standard libraries needed for cross compilation
  i686-apple-darwin | \
  i686-unknown-linux-gnu | \
  x86_64-unknown-linux-musl)
    version=$(rustc -V | cut -d' ' -f2)
    tarball=rust-std-${version}-${TARGET}

    curl -Os http://static.rust-lang.org/dist/${tarball}.tar.gz

    tar xzf ${tarball}.tar.gz

    ${tarball}/install.sh --prefix=$(rustc --print sysroot)

    rm -r ${tarball}
    rm ${tarball}.tar.gz
    ;;
  # Nothing to do for native builds
  *)
    ;;
esac

