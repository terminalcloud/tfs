**NOTE**: This is **ALPHA** quality software! It should not yet be used for any
critical components, does not yet support all core features, and is likely
to have bugs! Take care!

# TFS

## Overview

`tfs` is a fast, content-addressed, snapshottable filesystem.

It provides:
  - [x] Userspace, snapshottable block devices
  - [ ] Durable writes to local disk via fsync
  - [ ] Replicated snapshots to a cloud storage provider
  - [ ] Integration with FUSE to mount block devices on the host filesystem
  - [ ] A convenient, high-level CLI for interacting with said block devices

## Documentation

Clone the repo, run `cargo doc --open`. Hosted documentation coming soon.

## Releases

All versioned releases have binaries for all supported targets automatically
built and uploaded to github releases, where they are available for free use.

## Developing

See [HACKING.md](HACKING.md) for instructions.

## License

Licensed under the GPL Version 3.

Full text of the license is available in LICENSE.txt

