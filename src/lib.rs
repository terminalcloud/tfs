//! Lazy, peer-to-peer immutable object store.

extern crate rand;
extern crate uuid;
extern crate libc;
extern crate bit_vec;
extern crate terminal_linked_hash_map;
extern crate rwlock2;
extern crate crossbeam;
extern crate scoped_pool;
extern crate atomic_option;
extern crate vec_map;
extern crate slab;

#[macro_use]
extern crate log;

#[cfg(test)]
extern crate tempfile;

use uuid::Uuid;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::borrow::Cow;

pub use error::{Error, Result};

pub mod fs;
pub mod s3;
pub mod p2p;
pub mod sparse;
// pub mod mock;
pub mod error;

mod local;
mod impls;
mod util;

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub struct BlockIndex(usize);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct VolumeId(Uuid);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ContentId([u8; 32]);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct VolumeName<'a>(Cow<'a, str>);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct VolumeMetadata {
    size: usize
}

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

#[derive(Debug, Clone)]
pub struct FileMetadata {
    pub size: usize
}

pub trait Cache: Send + Sync {
    fn read(&self, chunk: &ChunkDescriptor, version: Option<Version>,
            buf: &mut [u8]) -> ::Result<()>;
}

pub trait Storage: Cache {
    fn set_metadata(&self, file: &FileDescriptor, metadata: FileMetadata) -> ::Result<()>;
    fn get_metadata(&self, file: &FileDescriptor) -> ::Result<FileMetadata>;
    fn create(&self, chunk: &ChunkDescriptor, version: Option<Version>,
              data: &[u8]) -> ::Result<()>;
    fn promote(&self, chunk: &ChunkDescriptor, version: Version) -> ::Result<()>;
    fn delete(&self, chunk: &ChunkDescriptor,
              version: Option<Version>) -> ::Result<()>;
}

