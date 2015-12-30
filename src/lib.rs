//! Lazy, peer-to-peer immutable object store.

extern crate fuse;

use std::sync::atomic::AtomicUsize;
use std::io;

pub mod fs;
pub mod s3;
pub mod p2p;

mod lru;
mod mock;
mod impls;

pub struct FileDescriptor;
pub struct File;
pub struct Chunk;
pub struct ChunkDescriptor;
pub struct Version(AtomicUsize);

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

