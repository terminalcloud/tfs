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

#[cfg(test)]
mod test {
    use scoped_pool::Pool;

    use fs::Fs;
    use local::{Options, LocalFs};
    use mock::MockStorage;
    use util::test::gen_random_block;

    use {VolumeName, VolumeMetadata, BlockIndex};

    // NOTE: Since the FlushPool is currently hard-coded to use 12 threads,
    // all our thread pools must have AT LEAST 12 threads or we will get
    // livelock.

    #[test]
    fn test_create_write_read() {
        let pool = Pool::new(16);
        let tempdir = ::tempdir::TempDir::new("tfs-test").unwrap();

        let localfs = LocalFs::new(Options {
            mount: tempdir.path().into(),
            size: 100
        }).unwrap();

        let fs = &Fs::new(Box::new(MockStorage::new()), Vec::new(), localfs);

        pool.scoped(move |scope| {
            fs.local().init(fs, scope).unwrap();

            let name = VolumeName("test-volume".to_string());
            let metadata = VolumeMetadata { size: 10 };
            let vol_id = fs.create(&name, metadata).unwrap();

            scope.zoom(|scope| {
                for i in 0..10 {
                    scope.execute(move || {
                        let data1 = gen_random_block(50).1;
                        fs.write(&vol_id, BlockIndex(i), 20, &data1).unwrap();

                        let data2 = gen_random_block(50).1;
                        fs.write(&vol_id, BlockIndex(i), 100, &data2).unwrap();

                        let mut buf: &mut [u8] = &mut [0u8; 50];
                        fs.read(&vol_id, BlockIndex(i), 20, buf).unwrap();
                        assert_eq!(&*data1, &*buf);

                        let mut buf: &mut [u8] = &mut [0u8; 50];
                        fs.read(&vol_id, BlockIndex(i), 100, buf).unwrap();
                        assert_eq!(&*data2, &*buf);
                    });
                }
            });

            fs.shutdown();
        });

        pool.shutdown();
    }

    #[test]
    fn test_multi_volume() {
        let pool = Pool::new(16);
        let tempdir = ::tempdir::TempDir::new("tfs-test").unwrap();

        let localfs = LocalFs::new(Options {
            mount: tempdir.path().into(),
            size: 100
        }).unwrap();

        let fs = &Fs::new(Box::new(MockStorage::new()), Vec::new(), localfs);

        pool.scoped(move |scope| {
            fs.local().init(fs, scope).unwrap();

            for name in 0..10 {
                let name = format!("test-volume{}", name);
                scope.zoom(move |scope| {
                    let name = VolumeName(name.to_string());
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

                            let mut buf: &mut [u8] = &mut [0u8; 50];
                            fs.read(&vol_id, BlockIndex(i), 100, buf).unwrap();
                            assert_eq!(&*data2, &*buf);
                        });
                    }
                });
            }

            fs.shutdown();
        });

        pool.shutdown();
    }
}

