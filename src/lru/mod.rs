use terminal_linked_hash_map::LinkedHashMap;
use std::collections::HashMap;
use std::sync::RwLock;

use fs::Fs;
use sparse::{SparseFile, VersionedSparseFile, Blob};
use {Storage, Cache, ChunkDescriptor, FileDescriptor, Version};

pub struct LruFs {
    files: HashMap<FileDescriptor, RwLock<Blob>>,
    chunks: RwLock<LinkedHashMap<ChunkDescriptor, Metadata>>
}

/// Per-chunk Metadata.
struct Metadata;

impl LruFs {
    pub fn new() -> LruFs {
        LruFs {
            files: HashMap::new(),
            chunks: RwLock::new(LinkedHashMap::new())
        }
    }

    pub fn version(&self, chunk: &ChunkDescriptor) -> Option<usize> {
        self.files.get(&chunk.file)
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
                                     data: &[u8]) -> ::Result<()> {
        // Write locally if not already there, evict if out of space.
        // No versioning or pinning needed.
        Ok(())
    }

    pub fn try_write_mutable_chunk(&self, chunk: &ChunkDescriptor,
                                   data: &[u8]) -> ::Result<()> {
        // Write locally: (if out of space, flush/delete unpinned data)
        //   - transactionally/atomically:
        //   - bump the version
        //   - pin the new chunk so it cannot be deleted

        // Queue new versioned chunk to be flushed. When done, unpin.
        Ok(())
    }
}

impl Storage for LruFs {
    fn create(&self, chunk: &ChunkDescriptor, version: Option<Version>,
              data: &[u8]) -> ::Result<()> {
        // Create new chunks entry.
        // Create new file if needed, then write chunk if needed.
        Ok(())
    }

    fn promote(&self, chunk: &ChunkDescriptor) -> ::Result<()> {
        Ok(())
    }

    fn delete(&self, chunk: &ChunkDescriptor,
              version: Option<Version>) -> ::Result<()> {
        Ok(())
    }
}

impl Cache for LruFs {
    fn read(&self, chunk: &ChunkDescriptor, _: Option<Version>,
            buf: &mut [u8]) -> ::Result<()> {
        try!(self.chunks.write().unwrap().get_refresh(chunk)
             .ok_or_else(|| ::Error::NotFound));
        self.files.get(&chunk.file)
            .ok_or_else(|| ::Error::NotFound)
            .and_then(|blob| blob.read().unwrap().read(chunk.chunk, buf))
    }
}

#[cfg(test)]
mod test {
    use lru::LruFs;
    use mock::StorageFuzzer;

    #[test]
    fn fuzz_lru_fs_storage() {
        // TODO: uncomment
        // StorageFuzzer::new(LruFs::new()).run(1)
    }
}

