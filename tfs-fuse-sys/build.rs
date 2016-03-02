extern crate pkg_config;

use std::env;

fn main() {
    let target = env::var("TARGET").unwrap();
    if target.ends_with("-apple-darwin") {
        // Use libosxfuse on OS X
        pkg_config::find_library("osxfuse").unwrap();
    } else if target.ends_with("-unknown-linux-gnu") || target.ends_with("-unknown-linux-musl") {
        // Use libfuse on Linux
        pkg_config::find_library("fuse").unwrap();
    } else {
        // Not supported
        panic!("{} is not a supported target for tfs-fuse-sys!", target);
    }
}

