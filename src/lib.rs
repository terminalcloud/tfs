//! Lazy, peer-to-peer immutable object store.

extern crate fuse;
extern crate scoped_threadpool;
extern crate rand;
extern crate uuid;
extern crate libc;
extern crate bit_vec;
extern crate terminal_linked_hash_map;
extern crate rwlock2;
extern crate crossbeam;
extern crate threadpool;
extern crate atomic_option;

#[cfg(test)]
extern crate tempfile;

use uuid::Uuid;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::io;

pub use error::{Error, Result};

pub mod fs;
pub mod s3;
pub mod p2p;
pub mod sparse;
pub mod mock;
pub mod error;

mod lru;
mod impls;
mod util;

pub struct File;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Chunk(usize);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct FileDescriptor(Uuid);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ChunkDescriptor {
    file: FileDescriptor,
    chunk: Chunk
}

#[derive(Debug)]
pub struct Version(AtomicUsize);

impl Version {
    fn new(v: usize) -> Version { Version(AtomicUsize::new(v))}
    fn load(&self) -> usize { self.0.load(Ordering::SeqCst) }
    fn increment(&self) -> usize { self.0.fetch_add(1, Ordering::SeqCst) }
}

impl Clone for Version {
    fn clone(&self) -> Self {
        Version::new(self.load())
    }
}

pub trait Cache: Send + Sync {
    fn read(&self, chunk: &ChunkDescriptor, version: Option<Version>,
            buf: &mut [u8]) -> ::Result<()>;
}

pub trait Storage: Cache {
    fn create(&self, chunk: &ChunkDescriptor, version: Option<Version>,
              data: &[u8]) -> ::Result<()>;
    fn promote(&self, chunk: &ChunkDescriptor) -> ::Result<()>;
    fn delete(&self, chunk: &ChunkDescriptor,
              version: Option<Version>) -> ::Result<()>;
}

