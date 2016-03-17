// Copyright (C) 2016 Cloudlabs, Inc
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

//! Lazy, peer-to-peer immutable object store.

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

extern crate fuse;
extern crate tfs_file_ext as fext;

#[macro_use]
extern crate scopeguard;

#[macro_use]
extern crate log;

#[cfg(test)]
extern crate tempfile;
#[cfg(test)]
extern crate tempdir;
#[cfg(test)]
extern crate time;

use uuid::Uuid;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::fmt;

pub use error::{Error, Result};

pub mod fs;
pub mod s3;
pub mod p2p;
pub mod sparse;
pub mod mock;
pub mod error;
pub mod local;

mod impls;
mod util;

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub struct BlockIndex(pub usize);

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
pub struct VolumeName(pub String);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct VolumeMetadata {
    pub size: usize
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

pub trait Cache: Send + Sync + fmt::Debug {
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
    use std::collections::HashMap;
    use std::io::Write;

    use fs::Fs;
    use local::Options;
    use mock::MockStorage;
    use util::test::gen_random_block;
    use sparse::BLOCK_SIZE;

    use {VolumeName, VolumeMetadata, BlockIndex, Snapshot, ContentId, Storage};

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

        Fs::run(12, options, Box::new(MockStorage::new()), Vec::new(), |fs, _scope| {
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

    #[test]
    fn test_shared_snapshot() {
        let tempdir1 = ::tempdir::TempDir::new("tfs-test").unwrap();
        let tempdir2 = ::tempdir::TempDir::new("tfs-test").unwrap();

        let options1 = Options {
            mount: tempdir1.path().into(),
            size: 100,
            flush_threads: 4,
            sync_threads: 4
        };

        let options2 = Options {
            mount: tempdir2.path().into(),
            size: 100,
            flush_threads: 4,
            sync_threads: 4
        };

        let storage = MockStorage::new();

        Fs::run(12, options1, Box::new(storage.clone()), Vec::new(), |fs1, _scope1| {
            Fs::run(12, options2, Box::new(storage.clone()), Vec::new(), |fs2, _scope2| {
                let original = VolumeName("original".to_string());
                let fork = VolumeName("fork".to_string());
                let another_fork = VolumeName("fork2".to_string());
                let metadata = VolumeMetadata { size: 20 };

                // Create a volume, write to it.
                let original_id = fs1.create(&original, metadata).unwrap();
                fs1.write(&original_id, BlockIndex(5), 10, &[7, 6, 5, 4, 3, 2]).unwrap();

                // Snapshot that volume under the name fork.
                fs1.snapshot(&original_id, fork.clone()).unwrap();

                // Open the volume on the *other* fs instance.
                let fork_id = fs2.fork(&fork).unwrap();

                // Read from the forked volume, check that the data is what
                // we wrote.
                let mut buf: &mut [u8] = &mut [0; 6];
                fs2.read(&fork_id, BlockIndex(5), 10, buf).unwrap();
                assert_eq!(&*buf, &[7, 6, 5, 4, 3, 2]);

                // Write to the forked volume.
                fs2.write(&fork_id, BlockIndex(5), 5, &[123, 124, 125]).unwrap();

                // Confirm the write.
                let mut buf: &mut [u8] = &mut [0; 3];
                fs2.read(&fork_id, BlockIndex(5), 5, buf).unwrap();
                assert_eq!(&*buf, &[123, 124, 125]);

                // Snapshot the fork.
                fs2.snapshot(&fork_id, another_fork.clone()).unwrap();

                // Re-open the new snapshot on the first fs.
                let fork_id2 = fs1.fork(&another_fork).unwrap();

                // Read the data again.
                let mut buf: &mut [u8] = &mut [0; 11];
                fs1.read(&fork_id2, BlockIndex(5), 5, buf).unwrap();
                assert_eq!(&*buf, &[123, 124, 125, 0, 0, 7, 6, 5, 4, 3, 2]);
            }).unwrap();
        }).unwrap();
    }

    #[test]
    #[should_panic]
    fn test_fs_run_panic() {
        let tempdir = ::tempdir::TempDir::new("tfs-test").unwrap();
        let options = Options {
            mount: tempdir.path().into(),
            size: 100,
            flush_threads: 4,
            sync_threads: 4
        };

        let m = ::std::sync::Arc::new(::std::sync::Mutex::new(()));

        Fs::run(12, options, Box::new(MockStorage::new()), Vec::new(), |fs, scope| {
            // Will unlock when we panic.
            let m1 = m.clone();
            let _l = m1.lock().unwrap();

            scope.execute(move || {
                // We have panicked.
                let _l = m.lock();
                assert!(_l.is_err());

                // Just do some actions to make sure the Fs is still ok.
                let name = VolumeName("x".to_string());
                let metadata = VolumeMetadata { size: 20 };

                let id = fs.create(&name, metadata).unwrap();
                fs.write(&id, BlockIndex(3), 5, &[1, 5, 6, 7, 8, 8, 9]).unwrap();
            });

            panic!("test panic");
        }).unwrap();
    }

    #[test]
    fn test_basic_flush() {
        let tempdir = ::tempdir::TempDir::new("tfs-test").unwrap();
        let options = Options {
            mount: tempdir.path().into(),
            size: 100,
            flush_threads: 4,
            sync_threads: 4
        };

        let storage = MockStorage::new();
        let start_name = VolumeName("test".to_string());
        let target_name = VolumeName("snapshot".to_string());
        let metadata = VolumeMetadata { size: 10 };

        let block = BlockIndex(5);
        let offset = 2;
        let data = &[1, 2, 3];

        let mut block_data = vec![0; BLOCK_SIZE];
        (&mut block_data[offset..]).write(data).unwrap();

        Fs::run(12, options, Box::new(storage.clone()), Vec::new(), |fs, _scope| {
            let vol = fs.create(&start_name, metadata).unwrap();
            fs.write(&vol, block, offset, data).unwrap();
            fs.snapshot(&vol, target_name.clone()).unwrap();
        }).unwrap();

        // Snapshot should be in storage now.
        let expected = Snapshot {
            metadata: metadata,
            blocks: {
                let mut blocks = HashMap::new();
                blocks.insert(block, ContentId::hash(&block_data));
                blocks
            }
        };

        assert_eq!(storage.get_snapshot(&target_name).unwrap(),
                   expected);
    }

    #[test]
    fn test_fuse_basic_run_exit() {
        use fuse::{Filesystem, Session, channel};

        struct Mock;
        impl Filesystem for Mock { }

        let tempdir = ::tempdir::TempDir::new("tfs-fuse-basic-run-exit-test").unwrap();
        let mountpoint = tempdir.path().to_path_buf();

        let pool = ::scoped_pool::Pool::new(1);
        defer!(pool.shutdown());

        let mut se = Session::new(Mock, &mountpoint, &[]);

        pool.scoped(|scope| {
            scope.execute(|| { se.run() });

            ::std::thread::sleep(::std::time::Duration::new(1, 0));
            defer!(channel::unmount(&mountpoint).unwrap());
        });
    }

    #[test]
    fn test_fuse_hello() {
        use scoped_pool::Pool;

        use std::io::Read;
        use std::path::{Path, PathBuf};
        use std::time::Duration;
        use std::fs::File;
        use std::thread;

        use time::Timespec;

        use fuse::channel;
        use fuse::{FileType, Session, FileAttr, Filesystem, Request,
                   ReplyData, ReplyEntry, ReplyAttr, ReplyDirectory};

        const TTL: Timespec = Timespec { sec: 1, nsec: 0 };

        const CREATE_TIME: Timespec = Timespec { sec: 100, nsec: 0 };

        const HELLO_DIR_ATTR: FileAttr = FileAttr {
            ino: 1,
            size: 0,
            blocks: 0,
            atime: CREATE_TIME,
            mtime: CREATE_TIME,
            ctime: CREATE_TIME,
            crtime: CREATE_TIME,
            kind: FileType::Directory,
            perm: 0o755,
            nlink: 2,
            uid: 501,
            gid: 20,
            rdev: 0,
            flags: 0,
        };

        const HELLO_TXT_CONTENT: &'static str = "Hello World!\n";

        const HELLO_TXT_ATTR: FileAttr = FileAttr {
            ino: 2,
            size: 13,
            blocks: 1,
            atime: CREATE_TIME,
            mtime: CREATE_TIME,
            ctime: CREATE_TIME,
            crtime: CREATE_TIME,
            kind: FileType::RegularFile,
            perm: 0o644,
            nlink: 1,
            uid: 501,
            gid: 20,
            rdev: 0,
            flags: 0,
        };

        struct Hello;

        impl Filesystem for Hello{
            fn lookup(&mut self, _req: &Request, parent: u64,
                      name: &Path, reply: ReplyEntry) {
                if parent == 1 && name.to_str() == Some("hello.txt") {
                    reply.entry(&TTL, &HELLO_TXT_ATTR, 0);
                } else {
                    reply.error(::libc::ENOENT);
                }
            }

            fn getattr(&mut self, _req: &Request, ino: u64, reply: ReplyAttr) {
                match ino {
                    1 => reply.attr(&TTL, &HELLO_DIR_ATTR),
                    2 => reply.attr(&TTL, &HELLO_TXT_ATTR),
                    _ => reply.error(::libc::ENOENT),
                }
            }

            fn read(&mut self, _req: &Request, ino: u64, _fh: u64,
                    offset: u64, _size: u32, reply: ReplyData) {
                if ino == 2 {
                    reply.data(&HELLO_TXT_CONTENT.as_bytes()[offset as usize..]);
                } else {
                    reply.error(::libc::ENOENT);
                }
            }

            fn readdir(&mut self, _req: &Request, ino: u64, _fh: u64,
                       offset: u64, mut reply: ReplyDirectory) {
                if ino == 1 {
                    if offset == 0 {
                        reply.add(1, 0, FileType::Directory, ".");
                        reply.add(1, 1, FileType::Directory, "..");
                        reply.add(2, 2, FileType::RegularFile, "hello.txt");
                    }
                    reply.ok();
                } else {
                    reply.error(::libc::ENOENT);
                }
            }
        }

        let tmp = ::tempdir::TempDir::new("tfs-fuse-test-hello").unwrap();
        let mountpoint = tmp.path().to_path_buf();

        let pool = Pool::new(2);
        defer!(pool.shutdown());

        let path = PathBuf::from(mountpoint);
        let mut se = Session::new(Hello, &path, &[]);
        pool.scoped(|scope| {
            scope.execute(|| { se.run() });

            thread::sleep(Duration::new(1, 0));
            defer!(channel::unmount(&path).unwrap());

            let mut buf = vec![0; HELLO_TXT_CONTENT.len()];
            let mut file = File::open(path.join("hello.txt")).unwrap();
            file.read(&mut buf).unwrap();
            assert_eq!(HELLO_TXT_CONTENT.as_bytes(), &*buf);
        });
    }
}

