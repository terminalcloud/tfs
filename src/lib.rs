//! Lazy, peer-to-peer immutable object store.

extern crate fuse;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::io;

pub mod fs;
pub mod s3;
pub mod p2p;

mod lru;
mod mock;
mod impls;

pub struct File;
pub struct Chunk;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct FileDescriptor;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ChunkDescriptor;

#[derive(Debug)]
pub struct Version(AtomicUsize);

impl Version {
    fn load(&self) -> usize { self.0.load(Ordering::SeqCst) }
    fn increment(&self) -> usize { self.0.fetch_add(1, Ordering::SeqCst) }
}

pub trait Cache: Sync {
    fn read(&self, chunk: &ChunkDescriptor, version: Option<Version>,
            buf: &mut [u8]) -> io::Result<usize>;
}

pub trait Storage: Cache {
    fn create(&self, chunk: &ChunkDescriptor, version: Option<Version>,
              data: &[u8]) -> io::Result<()>;
    fn promote(&self, chunk: &ChunkDescriptor) -> io::Result<()>;
    fn delete(&self, chunk: &ChunkDescriptor,
              version: Option<Version>) -> io::Result<()>;
}

