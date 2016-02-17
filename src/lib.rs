//! Lazy, peer-to-peer immutable object store.

// FIXME: REMOVE THIS!!!!!
#![allow(dead_code, unused_variables)]

extern crate rand;
extern crate uuid;
extern crate libc;
extern crate terminal_linked_hash_map;
extern crate shared_mutex;
extern crate crossbeam;
extern crate scoped_pool;
extern crate slab;
extern crate vec_map;
extern crate variance;
extern crate tiny_keccak as sha;

#[macro_use]
extern crate log;

#[cfg(test)]
extern crate tempfile;

use uuid::Uuid;
use std::sync::atomic::{AtomicUsize, Ordering};

pub use error::{Error, Result};

pub mod fs;
pub mod s3;
pub mod p2p;
pub mod sparse;
pub mod mock;
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

impl ContentId {
    pub fn null() -> Self { ContentId([0; 32]) }

    pub fn hash(data: &[u8]) -> ContentId {
        let mut hasher = sha::Keccak::new_sha3_256();
        hasher.update(&data);

        let mut hash = [0; 32];
        hasher.finalize(&mut hash);

        ContentId(hash)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct VolumeName(String);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct VolumeMetadata {
    size: usize
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
    fn read(&self, id: ContentId, buf: &mut [u8]) -> ::Result<()>;
}

pub trait Storage: Cache {
    fn set_metadata(&self, volume: &VolumeName, metadata: VolumeMetadata) -> ::Result<()>;
    fn get_metadata(&self, volume: &VolumeName) -> ::Result<VolumeMetadata>;

    fn create(&self, id: ContentId, data: &[u8]) -> ::Result<()>;
    fn delete(&self, id: ContentId) -> ::Result<()>;
}

