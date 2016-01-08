use fs::Fs;
use sparse::{SparseFile, VersionedSparseFile, Blob};
use {Storage, Cache, ChunkDescriptor, FileDescriptor, Version};

pub struct LruFs {
    files: PinnedLruMap<FileDescriptor, Blob>,
}

struct PinnedLruMap<K, V> {
    key: K,
    value: V
}

impl LruFs {
    pub fn init_flush_thread(&self, fs: Fs) -> ::Result<()> {
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
    fn read(&self, chunk: &ChunkDescriptor, version: Option<Version>,
            buf: &mut [u8]) -> ::Result<usize> {
        Ok(buf.len())
    }
}

