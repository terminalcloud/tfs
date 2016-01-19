# TFS

## Developing

Can be developed on any unix system providing `pread` and `pwrite` in addition
to being a normal rust target, but only really efficient on linux where it can
use sparse files.

  - Install either stable, beta, or nightly rust and cargo.
  - `cargo build` to build, `cargo test` to test

