# TFS

## Developing

Can be developed on any unix system providing `pread` and `pwrite` in addition
to being a normal rust target, but only really efficient on linux where it can
use sparse files.

  - Install either stable, beta, or nightly rust and cargo.
  - `cargo build` to build, `cargo test` to test

## Installing FUSE

NOTE: This is currently not required to develop or use tfs, as FUSE integration
is not yet implemented.

Ubuntu: `apt-get install libfuse-dev`
OS X Mavericks: `brew install osxfuse`
OS X Yosemite/El Capitan: `brew tap caskroom/cask && brew cask install osxfuse`

Note that installation of FUSE on OS X Yosemite and El Capitan requires
a restart to take effect.

## CI

`tfs` is continuously tested against rust nightly, beta and stable on linux
(ubuntu) and OS X (mavericks).

