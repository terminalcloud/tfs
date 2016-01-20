use rwlock2::{RwLock, RwLockReadGuard};
use crossbeam::sync::MsQueue;
use atomic_option::AtomicOption;

use std::fs::OpenOptions;
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::sync::atomic::Ordering;
use std::path::PathBuf;

use local::flush::{FlushMessage, FlushPool};

use fs::Fs;
use sparse::Blob;
use {Storage, Cache, ChunkDescriptor, FileDescriptor, Version};

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
        let blob = try!(self.get_or_create_blob(&chunk.file));
        let blob_guard = blob.read().unwrap();
        blob_guard.fill(chunk.chunk, version, data)
    }

    pub fn try_write_mutable_chunk(&self, chunk: &ChunkDescriptor,
                                   data: &[u8]) -> ::Result<()> {
        // Write locally:
        //   - transactionally/atomically:
        //   - bump the version

        // Queue new versioned chunk to be flushed. When done, unpin.
        let blob = try!(self.get_or_create_blob(&chunk.file));
        let mut blob_guard = blob.write().unwrap();
        let version = try!(blob_guard.assert_versioned_mut()
            .write(chunk.chunk, data));

        // Queue the new versioned chunk to be flushed.
        self.flush.push(FlushMessage::Flush(chunk.clone(),
                                            Version::new(version)));

        Ok(())
    }

    pub fn freeze(&self, file: &FileDescriptor) -> ::Result<HashMap<usize, usize>> {
        let blob = try!(self.get_or_create_blob(file));
        Blob::freeze(&*blob)
    }

    pub fn complete_flush(&self, chunk: &ChunkDescriptor,
                          version: Version) -> ::Result<()> {
        let blob = try!(self.get_or_create_blob(&chunk.file));
        let blob_guard = blob.read().unwrap();

        if let Some(rw) = blob_guard.as_read_write() {
            rw.complete_flush(chunk.chunk, version)
        } else {
            // The blob has already become read only.
            Ok(())
        }
    }

    /// Efficiently create a Blob if it doesn't exist.
    ///
    /// If the Blob already exists, only a read guard will be acquired.
    fn get_or_create_blob(&self, fd: &FileDescriptor) -> ::Result<BlobGuard> {
        // This code is very ugly to have the most optimal acquisition of locks.
        //
        // It is conceptually just:
        //    self.files.entry(fd).or_insert_with(|| new_blob())

        // The file is not present at the start of the operation.
        if self.files.read().unwrap().get(fd).is_none() {
            // At this stage we only *may* need to insert the file,
            // as it may have been inserted by another thread as soon
            // as we released the read guard, so we need to check again
            // after acquiring the write lock.
            match self.files.write().unwrap().entry(fd.clone()) {
                // Insert the file.
                Entry::Vacant(v) => {
                    let blob = try!(self.create_blob(&fd));
                    v.insert(RwLock::new(blob));
                },
                // Another thread beat us to the punch.
                _ => {}
            }
        }

        // Now the file is certainly present.
        Ok(self.files.read().unwrap().map(|map| map.get(fd).unwrap()))
    }

    fn create_blob(&self, fd: &FileDescriptor) -> ::Result<Blob> {
        let path = self.mount.join(fd.0.to_string());
        let file = try!(OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path));

        // TODO: Resume from existing chunks file.
        // TODO: Use a variable size.
        Ok(Blob::new(file, 1024))
    }
}

impl Cache for LocalFs {
    fn read(&self, chunk: &ChunkDescriptor, _: Option<Version>,
            buf: &mut [u8]) -> ::Result<()> {
        self.get_or_create_blob(&chunk.file)
            .and_then(|blob| blob.read().unwrap().read(chunk.chunk, buf))
    }
}

