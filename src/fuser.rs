use std::path::{PathBuf, Path};

use time::Timespec;
use scoped_pool::Scope;
use fuse::{Filesystem, Request, ReplyAttr, FileAttr, FileType, ReplyEntry,
           ReplyData, ReplyWrite, ReplyDirectory};

use sparse::BLOCK_SIZE;
use fs::Fs;
use {VolumeId, BlockIndex};

/// The INode associated with the root directory.
// FUSE hard-codes inode 1 as the inode value of /
const ROOT_INODE: INode = INode(1);

const TTL: Timespec = Timespec { sec: 1, nsec: 0 };

#[derive(Copy, Clone)]
struct FuseFs<'fs: 'scope, 'id: 'fs, 'scope> {
    fs: &'fs Fs<'id>,
    scope: &'scope Scope<'fs>,
    root: FileAttr
}

/// Metadata for a FUSE operation.
#[derive(Debug)]
struct FuseRequest {
    /// The user id of the initiator of the operation.
    uid: u32,

    /// The group id of the initiator of the operation.
    gid: u32,

    /// The process id of the initiating process.
    pid: u32
}

impl<'a, 'b> From<&'a Request<'b>> for FuseRequest {
    fn from(req: &'a Request<'b>) -> Self {
        FuseRequest {
            uid: req.uid(),
            gid: req.gid(),
            pid: req.pid()
        }
    }
}

// Just a struct so we name the fields at the callsites of Fs::new,
// it's too many easy-to-mix up numbers otherwise.
#[derive(Copy, Clone, Debug, PartialEq)]
struct Root {
    gid: u32,
    uid: u32,
    permissions: u16,
    time: Timespec
}

impl<'fs, 'id, 'scope> FuseFs<'fs,'id, 'scope> {
    fn new(fs: &'fs Fs<'id>, scope: &'scope Scope<'fs>, req: Root) -> Self {
        FuseFs::raw_new(fs, scope, FileAttr {
            ino: 1,
            size: 0,
            blocks: 0,
            atime: req.time,
            mtime: req.time,
            ctime: req.time,
            crtime: req.time,
            kind: FileType::Directory,
            perm: req.permissions,
            nlink: 2,
            uid: req.uid,
            gid: req.gid,
            rdev: 0,
            flags: 0,
        })
    }

    fn raw_new(fs: &'fs Fs<'id>, scope: &'scope Scope<'fs>,
               root: FileAttr) -> Self {
        FuseFs {
            fs: fs,
            scope: scope,
            root: root
        }
    }

    fn getattr(self, req: FuseRequest, ino: u64, reply: ReplyAttr) {
        let inode = INode(ino);

        if inode.is_root() {
            reply.attr(&TTL, &self.root)
        } else {
            debug_assert!(inode.is_volume());

            let vol_id: VolumeId = inode.into();
            match self.fs.local().stat(&vol_id) {
                Ok(metadata) => {
                    reply.attr(&TTL, &FileAttr {
                        ino: vol_id.0,
                        size: (metadata.size * BLOCK_SIZE) as u64,
                        blocks: 0,
                        atime: TTL, // TODO: Handle time metadata
                        mtime: TTL,
                        ctime: TTL,
                        crtime: TTL,
                        kind: FileType::RegularFile,
                        perm: metadata.permissions,
                        nlink: 2,
                        uid: metadata.uid,
                        gid: metadata.gid,
                        rdev: 0,
                        flags: 0
                    })
                },
                Err(e) => reply.error(e.as_c_error())
            }
        }
    }

    fn lookup(self, req: FuseRequest, parent: u64, name: PathBuf, reply: ReplyEntry) {
        debug_assert_eq!(parent, 1);

        let name = name.to_string_lossy();

        if name.starts_with("._.") {
            reply.error(::libc::ENOENT);
            return
        }

        let vol_id = match name.parse() {
            Ok(id) => VolumeId(id),
            Err(_) => { reply.error(::libc::EINVAL); return }
        };

        match self.fs.local().stat(&vol_id) {
            Ok(metadata) => {
                reply.entry(&TTL, &FileAttr {
                    ino: vol_id.0,
                    size: (metadata.size * BLOCK_SIZE) as u64,
                    blocks: 0,
                    atime: TTL, // TODO: Handle time metadata
                    mtime: TTL,
                    ctime: TTL,
                    crtime: TTL,
                    kind: FileType::RegularFile,
                    perm: metadata.permissions,
                    nlink: 2,
                    uid: metadata.uid,
                    gid: metadata.gid,
                    rdev: 0,
                    flags: 0
                }, 0)
            },
            Err(e) => reply.error(e.as_c_error())
        }
    }

    fn read(self, req: FuseRequest, ino: u64, offset: u64, size: u32, reply: ReplyData) {
        let inode = INode(ino);
        debug_assert!(inode.is_volume());
        let vol_id: VolumeId = inode.into();

        if size > BLOCK_SIZE as u32 {
            reply.error(::libc::EINVAL);
            return
        }

        let block = BlockIndex((offset / BLOCK_SIZE as u64) as usize);
        let block_offset = (offset % BLOCK_SIZE as u64) as usize;

        // TODO: Optimize this...
        let mut buffer = vec![0; size as usize];
        match self.fs.read(&vol_id, block, block_offset, &mut buffer) {
            Ok(()) => reply.data(&buffer),
            Err(e) => reply.error(e.as_c_error())
        }
    }

    fn write(self, req: FuseRequest, ino: u64, offset: u64, data: Vec<u8>, reply: ReplyWrite) {
        let inode = INode(ino);

        if inode.is_root() || data.len() > BLOCK_SIZE {
            reply.error(::libc::EINVAL);
            return
        }

        let vol_id: VolumeId = inode.into();
        let block = BlockIndex((offset / BLOCK_SIZE as u64) as usize);
        let block_offset = (offset % BLOCK_SIZE as u64) as usize;

        match self.fs.write(&vol_id, block, block_offset, &data) {
            Ok(()) => reply.written(data.len() as u32),
            Err(e) => reply.error(e.as_c_error())
        }
    }

    fn readdir(self, req: FuseRequest, ino: u64, mut reply: ReplyDirectory) {
        let inode = INode(ino);

        if inode.is_volume() {
            reply.error(::libc::EINVAL);
            return
        }

        debug_assert!(inode.is_root());

        // . and .. are the same for /
        reply.add(ino, 0, FileType::Directory, ".");
        reply.add(ino, 1, FileType::Directory, "..");

        for volume in self.fs.local().list_volumes() {
            reply.add(volume.0, 2, FileType::Directory, format!("{}", volume.0));
        }

        reply.ok();
    }
}

impl<'fs, 'id, 'scope> Filesystem for FuseFs<'fs,'id, 'scope> {
    fn getattr(&mut self, req: &Request, ino: u64, reply: ReplyAttr) {
        let (scope, fs, root) = (self.scope, self.fs, self.root);
        let req = req.into();

        debug!("FUSE getattr: {:?} {:?}", ino, req);
        scope.recurse(move |scope| FuseFs::raw_new(fs, scope, root).getattr(req, ino, reply));
    }

    fn lookup(&mut self, req: &Request, parent: u64,
              name: &Path, reply: ReplyEntry) {
        let (scope, fs, root) = (self.scope, self.fs, self.root);
        let req = req.into();
        let name = name.to_path_buf();

        debug!("FUSE lookup: {:?} {:?} {:?}", name, parent, req);
        scope.recurse(move |scope|
            FuseFs::raw_new(fs, scope, root).lookup(req, parent, name, reply));
    }

    fn read(&mut self, req: &Request, ino: u64, fh: u64,
            offset: u64, size: u32, reply: ReplyData) {
        let (scope, fs, root) = (self.scope, self.fs, self.root);
        let req = req.into();

        debug!("FUSE read: ino {:?}, offset {:?}, size {:?}, {:?}", ino, offset, size, req);
        scope.recurse(move |scope|
            FuseFs::raw_new(fs, scope, root).read(req, ino, offset, size, reply))
    }

    fn write(&mut self, req: &Request, ino: u64, fh: u64, offset: u64,
             data: &[u8], flags: u32, reply: ReplyWrite) {
        let (scope, fs, root) = (self.scope, self.fs, self.root);
        let req = req.into();

        debug!("FUSE write: ino {:?}, offset {:?}, size {:?}, {:?}", ino, offset, data.len(), req);
        trace!("FUSE write data: {:?}", data);

        // TODO: Optimize this somehow...
        let data = data.to_owned();
        scope.recurse(move |scope|
            FuseFs::raw_new(fs, scope, root).write(req, ino, offset, data, reply))
    }

    fn readdir(&mut self, req: &Request, ino: u64, _fh: u64, offset: u64,
               reply: ReplyDirectory) {
        let (scope, fs, root) = (self.scope, self.fs, self.root);
        let req = req.into();

        if offset != 0 { reply.ok(); return }

        debug!("FUSE readdir: ino {:?}, offset {:?}, {:?}", ino, offset, req);
        scope.recurse(move |scope|
            FuseFs::raw_new(fs, scope, root).readdir(req, ino, reply))
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
struct INode(u64);

impl From<VolumeId> for INode {
    fn from(vol: VolumeId) -> INode { INode(vol.0) }
}

impl Into<VolumeId> for INode {
    fn into(self) -> VolumeId { VolumeId(self.0) }
}

impl INode {
    fn is_root(&self) -> bool { *self == ROOT_INODE }
    fn is_volume(&self) -> bool { !self.is_root() }
}

#[cfg(test)]
mod test {
    use std::time::Duration;
    use std::path::PathBuf;
    use std::os::unix::fs::MetadataExt;
    use std::fs::OpenOptions;
    use std::thread;

    use fext::FileExt;
    use time::Timespec;
    use fuse::{Session, channel};
    use scoped_pool::Pool;

    use local::Options;
    use mock::MockStorage;
    use fs::Fs;
    use fuser::{FuseFs, Root};
    use {VolumeName, VolumeMetadata, BlockIndex};

    const CREATE_TIME: Timespec = Timespec { sec: 100, nsec: 0 };

    const TEST_UID: u32 = 501;
    const TEST_GID: u32 = 20;
    const TEST_PERMISSIONS: u16 = 0o777;

    #[test]
    fn test_fuser_getattr() {
        let mount_tmpdir = ::tempdir::TempDir::new("tfs-fuse-fs-test-fuser-getattr").unwrap();
        let mountpoint = mount_tmpdir.path().to_path_buf();

        let pool = Pool::new(12);
        defer!(pool.shutdown());

        let fs_tempdir = ::tempdir::TempDir::new("tfs-test").unwrap();
        let options = Options {
            mount: fs_tempdir.path().into(),
            size: 100,
            flush_threads: 2,
            sync_threads: 2
        };

        let root = Root {
            uid: 510,
            gid: 20,
            time: CREATE_TIME,
            permissions: 0o711
        };

        Fs::run(6, options, Box::new(MockStorage::new()), Vec::new(), |fs, scope| {
            let path = PathBuf::from(mountpoint);
            let mut se = Session::new(FuseFs::new(fs, scope, root), &path, &[]);

            scope.zoom(|scope| {
                scope.execute(|| { se.run(); });
                thread::sleep(Duration::new(1, 0));
                defer!(channel::unmount(&path).unwrap());

                let metadata = ::std::fs::metadata(mount_tmpdir.path()).unwrap();
                assert!(metadata.is_dir());
                assert_eq!(metadata.ino(), 1);
                assert_eq!(metadata.uid(), root.uid);
                assert_eq!(metadata.gid(), root.gid);

                let vol_name = VolumeName("name".to_string());

                let vol_metadata = VolumeMetadata {
                    size: 20,
                    uid: TEST_UID,
                    gid: TEST_GID,
                    permissions: TEST_PERMISSIONS
                };

                let vol_id = fs.create(&vol_name, vol_metadata).unwrap();
                let volume_path = mount_tmpdir.path().join(vol_id.0.to_string());
                let vol_attr = ::std::fs::metadata(volume_path).unwrap();

                assert!(!vol_attr.is_dir());
                assert_eq!(vol_attr.ino(), vol_id.0);
                assert_eq!(vol_attr.uid(), vol_metadata.uid);
                assert_eq!(vol_attr.gid(), vol_metadata.gid);
            });
        }).unwrap();
    }

    #[test]
    fn test_basic_ops() {
        let mount_tmpdir = ::tempdir::TempDir::new("tfs-fuse-fs-test-fuser-read-write").unwrap();
        let mountpoint = mount_tmpdir.path().to_path_buf();

        let pool = Pool::new(12);
        defer!(pool.shutdown());

        let fs_tempdir = ::tempdir::TempDir::new("tfs-test").unwrap();
        let options = Options {
            mount: fs_tempdir.path().into(),
            size: 100,
            flush_threads: 2,
            sync_threads: 2
        };

        let root = Root {
            uid: 501,
            gid: 20,
            time: CREATE_TIME,
            permissions: 0o755
        };

        Fs::run(6, options, Box::new(MockStorage::new()), Vec::new(), |fs, scope| {
            let path = PathBuf::from(mountpoint);
            let mut se = Session::new(FuseFs::new(fs, scope, root), &path, &[]);

            scope.zoom(|scope| {
                scope.execute(|| { se.run(); });
                thread::sleep(Duration::new(1, 0));
                defer!(channel::unmount(&path).unwrap());

                let vol_name = VolumeName("name".to_string());
                let vol_metadata = VolumeMetadata {
                    size: 20,
                    uid: TEST_UID,
                    gid: TEST_GID,
                    permissions: 0o666
                };

                let vol_id = fs.create(&vol_name, vol_metadata).expect("creating volume");
                let vol_path = mount_tmpdir.path().join(vol_id.0.to_string());

                // Open the volume as a file.
                let vol_file = OpenOptions::new()
                    .read(true).write(true).open(vol_path).expect("open volume file");

                // Write some data to the volume.
                let data = &[1, 2, 4, 8, 16];
                let offset = 123;
                assert_eq!(data.len(),
                           vol_file.write_at(offset, data).expect("write to volume file"));

                // Read the data.
                let mut buffer = &mut [0u8; 5];
                assert_eq!(buffer.len(),
                           vol_file.read_at(offset, buffer).expect("read of volume file"));
                assert_eq!(data, &*buffer);

                // Create a second volume and read the current volumes.
                let vol2_name = VolumeName("name2".to_string());
                let vol2_id = fs.create(&vol2_name, vol_metadata).expect("creating volume2");

                // Read the current volumes.
                let dir_entries = ::std::fs::read_dir(mount_tmpdir.path()).expect("ls of /")
                    .map(|entry| entry.unwrap()).collect::<Vec<_>>();

                let expected = vec![mount_tmpdir.path().join("2"), mount_tmpdir.path().join("3")];
                let found = dir_entries.iter()
                    .map(|entry| entry.path().into()).collect::<Vec<PathBuf>>();

                assert_eq!(expected.len(), found.len());
                for f in found {
                    assert!(expected.iter().find(|e| e == &&f).is_some());
                }
            });
        }).unwrap();
    }
}

