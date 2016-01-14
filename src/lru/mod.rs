use terminal_linked_hash_map::LinkedHashMap;
use rwlock2::{RwLock, RwLockReadGuard};
use crossbeam::sync::MsQueue;
use atomic_option::AtomicOption;

use std::fs::OpenOptions;
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::sync::Mutex;
use std::sync::atomic::Ordering;
use std::path::PathBuf;

use lru::flush::{FlushMessage, FlushPool};

use fs::Fs;
use sparse::Blob;
use {Storage, Cache, ChunkDescriptor, FileDescriptor, Version};

mod flush;

pub struct LruFs {
    files: RwLock<HashMap<FileDescriptor, RwLock<Blob>>>,
    chunks: Mutex<ChunkMap>,
    mount: PathBuf,
    flush: MsQueue<FlushMessage>,
    pool: AtomicOption<FlushPool>
}

pub type BlobGuard<'a> = RwLockReadGuard<'a, RwLock<Blob>>;

/// Per-chunk Metadata.
struct Metadata;

impl LruFs {
    pub fn new(mount: PathBuf) -> LruFs {
        LruFs {
            files: RwLock::new(HashMap::new()),
            // TODO: Make size configurable
            chunks: Mutex::new(ChunkMap::new(MAX_LRU_ENTRIES)),
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
        // Write locally if not already there, evict if out of space.
        // No versioning or pinning needed.
        self.chunks.lock().unwrap().immutable_write_reserve(chunk);

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
        self.chunks.lock().unwrap().mutable_write_reserve(chunk);

        let blob = try!(self.get_or_create_blob(&chunk.file));
        let mut blob_guard = blob.write().unwrap();
        let version = try!(blob_guard.assert_versioned_mut()
            .write(chunk.chunk, data));

        // Queue the new versioned chunk to be flushed.
        self.flush.push(FlushMessage::Flush(chunk.clone(),
                                            Some(Version::new(version))));

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
        self.chunks.lock().unwrap().read_refresh(chunk);
        self.get_or_create_blob(&chunk.file)
            .and_then(|blob| blob.read().unwrap().read(chunk.chunk, buf))
    }
}

#[derive(Default)]
struct ChunkMap {
    lru: LinkedHashMap<ChunkDescriptor, Metadata>,
    reserved: HashMap<ChunkDescriptor, Metadata>,
    size: usize
}

// TODO: Make configurable.
const MAX_LRU_ENTRIES: usize = 1024;

impl ChunkMap {
    fn new(size: usize) -> ChunkMap {
        ChunkMap {
            lru: LinkedHashMap::with_capacity(size),
            reserved: HashMap::new(),
            size: size
        }
    }

    fn read_refresh(&mut self, chunk: &ChunkDescriptor) {
        let present = {
            let (lru, reserved) = (&mut self.lru, &mut self.reserved);
            lru.get_refresh(chunk).or_else(|| reserved.get_mut(chunk)).is_some()
        };

        // If the chunk is not present, reserve the chunk.
        if !present {
            self.reserved.insert(chunk.clone(), Metadata);
        }
    }

    fn immutable_write_reserve(&mut self, chunk: &ChunkDescriptor) {
        // If the chunk is reserved by a previous read-miss, fill it.
        if let Some(metadata) = self.reserved.remove(chunk) {
            self.insert_and_evict(chunk);
        // If the chunk is not reserved, just refresh it.
        // The write is likely redundant.
        } else {
            // TODO: Investigate when this branch can be taken and if
            // this is the correct thing to do in that case.
            self.lru.get_refresh(chunk);
        }
    }

    fn mutable_write_reserve(&mut self, chunk: &ChunkDescriptor) {
        match self.reserved.entry(chunk.clone()) {
            Entry::Vacant(v) => {
                if let Some(metadata) = self.lru.remove(chunk) {
                    v.insert(metadata);
                } else {
                    v.insert(Metadata);
                }
            },
            // Already reserved
            _ => {}
        }
    }

    fn insert_and_evict(&mut self, chunk: &ChunkDescriptor) {
        if self.lru.len() == self.size {
            self.lru.pop_back();
        }

        self.lru.insert(chunk.clone(), Metadata);
    }
}

#[cfg(test)]
mod test {
    use lru::ChunkMap;

    #[test]
    fn fuzz_lru_fs_storage() {
        // TODO: Fill in
    }
}

