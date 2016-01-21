use rwlock2::{RwLock, RwLockReadGuard};
use crossbeam::sync::MsQueue;
use atomic_option::AtomicOption;

use std::fs::{File, OpenOptions};
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::sync::atomic::Ordering;
use std::path::PathBuf;

use local::flush::{FlushMessage, FlushPool};

use util::RwLockExt;
use fs::Fs;
use sparse::Blob;
use {Storage, Cache, ChunkDescriptor, FileDescriptor, Version, FileMetadata};

mod flush;

pub struct LocalFs {
    files: RwLock<HashMap<FileDescriptor, RwLock<Blob>>>,
    mount: PathBuf,
    flush: MsQueue<FlushMessage>,
    pool: AtomicOption<FlushPool>
}

pub type BlobGuard<'a> = RwLockReadGuard<'a, RwLock<Blob>>;

impl LocalFs {
    pub fn new(mount: PathBuf) -> LocalFs {
        LocalFs {
            files: RwLock::new(HashMap::new()),
            mount: mount,
            flush: MsQueue::new(),
            pool: AtomicOption::empty()
        }
    }

    /// Insert a handle to a new Read-Write object with the given starting metadata.
    pub fn create(&self, file: FileDescriptor, metadata: FileMetadata) -> ::Result<()> {
        self.new_blob(file, metadata, |f, m| Blob::new(f, m.size))
    }

    /// Insert a handle to an existing Read-Only object with the given metadata.
    pub fn open(&self, file: FileDescriptor, metadata: FileMetadata) -> ::Result<()> {
        self.new_blob(file, metadata, |f, m| Blob::open(f, m.size))
    }

    pub fn version(&self, chunk: &ChunkDescriptor) -> Option<usize> {
        self.files.read().unwrap().get(&chunk.file)
            .and_then(|blob| {
                match *blob.read().unwrap() {
                    Blob::ReadOnly(_) => None,
                    Blob::ReadWrite(ref v) => v.version(&chunk.chunk)
                }
            })
    }

    pub fn init_flush_thread(&self, fs: Fs) -> ::Result<()> {
        // TODO: Make size of pool configurable.
        let flush_pool = FlushPool::new(16, fs);
        flush_pool.run();
        self.pool.swap(Box::new(flush_pool), Ordering::SeqCst);
        Ok(())
    }

    pub fn try_write_immutable_chunk(&self, chunk: &ChunkDescriptor,
                                     version: Option<Version>,
                                     data: &[u8]) -> ::Result<()> {
        // Write the data locally, filling a previously unfilled chunk.
        let blob = try!(self.get_blob(&chunk.file));
        let blob_guard = blob.read().unwrap();
        blob_guard.fill(chunk.chunk, version, data)
    }

    pub fn try_write_mutable_chunk(&self, chunk: &ChunkDescriptor,
                                   data: &[u8]) -> ::Result<()> {
        // Write locally:
        //   - transactionally/atomically:
        //   - bump the version

        // Queue new versioned chunk to be flushed. When done, unpin.
        let blob = try!(self.get_blob(&chunk.file));
        let mut blob_guard = blob.write().unwrap();
        let version = try!(blob_guard.assert_versioned_mut()
            .write(chunk.chunk, data));

        // Queue the new versioned chunk to be flushed.
        self.flush.push(FlushMessage::Flush(chunk.clone(),
                                            Version::new(version)));

        Ok(())
    }

    pub fn freeze(&self, file: &FileDescriptor) -> ::Result<(HashMap<usize, usize>,
                                                             FileMetadata)> {
        let blob = try!(self.get_blob(file));
        Blob::freeze(&*blob)
    }

    pub fn complete_flush(&self, chunk: &ChunkDescriptor,
                          version: Version) -> ::Result<()> {
        let blob = try!(self.get_blob(&chunk.file));
        let blob_guard = blob.read().unwrap();

        if let Some(rw) = blob_guard.as_read_write() {
            rw.complete_flush(chunk.chunk, version)
        } else {
            // The blob has already become read only.
            Ok(())
        }
    }

    fn get_blob(&self, fd: &FileDescriptor) -> ::Result<BlobGuard> {
        let reader = self.files.read().unwrap();

        // Unfortunate double-lookup here.
        //
        // If you see a better way please fix it.
        if reader.contains_key(fd) {
            Ok(reader.map(|map| map.get(fd).unwrap()))
        } else {
            Err(::Error::NotFound)
        }
    }

    /// Add a new Blob to our internal map.
    fn new_blob<F>(&self, file: FileDescriptor, metadata: FileMetadata,
                   constructor: F) -> ::Result<()>
    where F: FnOnce(File, FileMetadata) -> Blob {
        self.files.if_then::<_, _, ::Result<()>>(
            |files| !files.contains_key(&file),
            |files| {
                let fd = file.clone();
                let path = self.mount.join(fd.0.to_string());
                let file = try!(OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create(true)
                    .open(path));
                let blob = constructor(file, metadata);

                files.insert(fd.clone(), RwLock::new(blob));

                Ok(())
            }
        ).unwrap_or(Err(::Error::AlreadyExists))
    }

}

impl Cache for LocalFs {
    fn read(&self, chunk: &ChunkDescriptor, _: Option<Version>,
            buf: &mut [u8]) -> ::Result<()> {
        self.get_blob(&chunk.file)
            .and_then(|blob| blob.read().unwrap().read(chunk.chunk, buf))
    }
}

