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
#[cfg(test)]
extern crate tempdir;

use uuid::Uuid;
use std::collections::HashMap;
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

impl VolumeId {
    pub fn new() -> Self { VolumeId(Uuid::new_v4()) }
}

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

#[derive(Debug, Clone, PartialEq)]
pub struct Snapshot {
    pub metadata: VolumeMetadata,
    pub blocks: HashMap<BlockIndex, ContentId>
}

pub trait Cache: Send + Sync {
    fn read(&self, id: ContentId, buf: &mut [u8]) -> ::Result<()>;
}

pub trait Storage: Cache {
    fn snapshot(&self, volume: &VolumeName, snapshot: Snapshot) -> ::Result<()>;

    fn get_snapshot(&self, name: &VolumeName) -> ::Result<Snapshot>;
    fn get_metadata(&self, volume: &VolumeName) -> ::Result<VolumeMetadata>;

    fn create(&self, id: ContentId, data: &[u8]) -> ::Result<()>;
    fn delete(&self, id: ContentId) -> ::Result<()>;
}

#[cfg(test)]
mod test {
    use fs::Fs;
    use local::Options;
    use mock::MockStorage;
    use util::test::gen_random_block;

    use {VolumeName, VolumeMetadata, BlockIndex};

    #[test]
    fn test_create_write_read() {
        let tempdir = ::tempdir::TempDir::new("tfs-test").unwrap();
        let options = Options {
            mount: tempdir.path().into(),
            size: 100,
            flush_threads: 4,
            sync_threads: 4
        };

        Fs::run(12, options, Box::new(MockStorage::new()), Vec::new(), |fs, scope| {
            let name = VolumeName("test-volume".to_string());
            let metadata = VolumeMetadata { size: 10 };
            let vol_id = fs.create(&name, metadata).unwrap();

            for i in 0..10 {
                scope.execute(move || {
                    let data1 = gen_random_block(50).1;
                    fs.write(&vol_id, BlockIndex(i), 20, &data1).unwrap();

                    let data2 = gen_random_block(50).1;
                    fs.write(&vol_id, BlockIndex(i), 100, &data2).unwrap();

                    let mut buf: &mut [u8] = &mut [0u8; 50];
                    fs.read(&vol_id, BlockIndex(i), 20, buf).unwrap();
                    assert_eq!(&*data1, &*buf);

                    fs.read(&vol_id, BlockIndex(i), 100, buf).unwrap();
                    assert_eq!(&*data2, &*buf);
                });
            }
        }).unwrap();
    }

    #[test]
    fn test_multi_volume() {
        let tempdir = ::tempdir::TempDir::new("tfs-test").unwrap();
        let options = Options {
            mount: tempdir.path().into(),
            size: 100,
            flush_threads: 4,
            sync_threads: 4
        };

        Fs::run(12, options, Box::new(MockStorage::new()), Vec::new(), |fs, scope| {
            for name in 0..10 {
                let name = VolumeName(format!("test-volume{}", name));
                let metadata = VolumeMetadata { size: 10 };
                let vol_id = fs.create(&name, metadata).unwrap();

                for i in 0..10 {
                    scope.execute(move || {
                        let data1 = gen_random_block(50).1;
                        fs.write(&vol_id, BlockIndex(i), 20, &data1).unwrap();

                        let data2 = gen_random_block(50).1;
                        fs.write(&vol_id, BlockIndex(i), 100, &data2).unwrap();

                        let mut buf: &mut [u8] = &mut [0u8; 50];
                        fs.read(&vol_id, BlockIndex(i), 20, buf).unwrap();
                        assert_eq!(&*data1, &*buf);

                        fs.read(&vol_id, BlockIndex(i), 100, buf).unwrap();
                        assert_eq!(&*data2, &*buf);
                    });
                }
            }
        }).unwrap();
    }

    #[test]
    fn test_basic_snapshot_fork() {
        let tempdir = ::tempdir::TempDir::new("tfs-test").unwrap();
        let options = Options {
            mount: tempdir.path().into(),
            size: 100,
            flush_threads: 4,
            sync_threads: 4
        };

        Fs::run(12, options, Box::new(MockStorage::new()), Vec::new(), |fs, scope| {
            let original = VolumeName("original".to_string());
            let fork = VolumeName("fork".to_string());
            let metadata = VolumeMetadata { size: 20 };

            // Create a volume, write to it.
            let original_id = fs.create(&original, metadata).unwrap();
            fs.write(&original_id, BlockIndex(5), 10, &[7, 6, 5, 4, 3, 2]).unwrap();

            // Snapshot that volume under the name fork.
            fs.snapshot(&original_id, fork.clone()).unwrap();

            // Open the snapshot we just created.
            let fork_id = fs.fork(&fork).unwrap();

            // Read from the forked volume, check that the data is what
            // we wrote.
            let mut buf: &mut [u8] = &mut [0; 6];
            fs.read(&fork_id, BlockIndex(5), 10, buf).unwrap();
            assert_eq!(&*buf, &[7, 6, 5, 4, 3, 2]);

            // Write to the original, check it doesn't show up in the fork.
            fs.write(&original_id, BlockIndex(5), 10, &[1, 2, 3]).unwrap();

            // Check the fork.
            let mut buf: &mut [u8] = &mut [0; 6];
            fs.read(&fork_id, BlockIndex(5), 10, buf).unwrap();
            assert_eq!(&*buf, &[7, 6, 5, 4, 3, 2]);

            // Check the write went through on the original.
            let mut buf: &mut [u8] = &mut [0; 6];
            fs.read(&original_id, BlockIndex(5), 10, buf).unwrap();
            assert_eq!(&*buf, &[1, 2, 3, 4, 3, 2]);

        }).unwrap();
    }
}

