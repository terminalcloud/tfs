use terminal_linked_hash_map::LinkedHashMap;
use rwlock2::{RwLock, RwLockReadGuard};

use std::fs::OpenOptions;
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::sync::Mutex;
use std::path::PathBuf;

use fs::Fs;
use sparse::Blob;
use {Storage, Cache, ChunkDescriptor, FileDescriptor, Version};

pub struct LruFs {
    files: RwLock<HashMap<FileDescriptor, RwLock<Blob>>>,
    chunks: Mutex<LinkedHashMap<ChunkDescriptor, Metadata>>,
    mount: PathBuf
}

pub type BlobGuard<'a> = RwLockReadGuard<'a, RwLock<Blob>>;

/// Per-chunk Metadata.
struct Metadata;

impl LruFs {
    pub fn new(mount: PathBuf) -> LruFs {
        LruFs {
            files: RwLock::new(HashMap::new()),
            chunks: Mutex::new(LinkedHashMap::new()),
            mount: mount
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
        // Create a channel between us and the flush thread to submit flushes on.
        //
        // Start flush thread operating over this channel:
        //   - waits for flushes on the channel
        //   - for each flush:
        //     - check if the flush is still relevant
        //     - if so flush it then unpin (if the version is the same)
        Ok(())
    }

    pub fn try_write_immutable_chunk(&self, chunk: &ChunkDescriptor,
                                     version: Option<Version>,
                                     data: &[u8]) -> ::Result<()> {
        // Write locally if not already there, evict if out of space.
        // No versioning or pinning needed.
        try!(self.reserve(chunk));

        let blob = try!(self.get_or_create_blob(&chunk.file));
        let mut blob_guard = blob.write().unwrap();
        blob_guard.fill(chunk.chunk, version, data)
    }

    pub fn try_write_mutable_chunk(&self, chunk: &ChunkDescriptor,
                                   data: &[u8]) -> ::Result<()> {
        // Write locally: (if out of space, flush/delete unpinned data)
        //   - transactionally/atomically:
        //   - bump the version
        //   - pin the new chunk so it cannot be deleted

        // Queue new versioned chunk to be flushed. When done, unpin.
        try!(self.reserve(chunk));
        try!(self.refresh_chunk(chunk));
        Ok(())
    }

    fn refresh_chunk(&self, chunk: &ChunkDescriptor) -> ::Result<()> {
        try!(self.chunks.lock().unwrap().get_refresh(chunk)
            .ok_or_else(|| ::Error::NotFound));
        Ok(())
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

    /// Reserves space for the specified ChunkDescriptor.
    ///
    /// The space is reserved until the chunk is written, at which point
    /// it is available for eviction again by subsequent calls to reserve.
    ///
    /// Should *always* be called before writing a chunk.
    fn reserve(&self, reserve: &ChunkDescriptor) -> ::Result<()> {
        Ok(())
    }
}

impl Cache for LruFs {
    fn read(&self, chunk: &ChunkDescriptor, _: Option<Version>,
            buf: &mut [u8]) -> ::Result<()> {
        try!(self.refresh_chunk(chunk));
        self.files.read().unwrap().get(&chunk.file)
            .ok_or_else(|| ::Error::NotFound)
            .and_then(|blob| blob.read().unwrap().read(chunk.chunk, buf))
    }
}

#[cfg(test)]
mod test {
    #[test]
    fn fuzz_lru_fs_storage() {
        // TODO: Fill in
    }
}

