use std::io;

use fs::Fs;
use {Storage, Cache, ChunkDescriptor, Version};

pub struct LruFs {
    files: Vec<u8>
}

impl LruFs {
    pub fn init_flush_thread(&self, fs: Fs) -> io::Result<()> {
        Ok(())
    }

    pub fn try_write_immutable_chunk(&self, chunk: &ChunkDescriptor,
                                     data: &[u8]) -> io::Result<()> {
        // Write locally if not already there, evict if out of space.
        // No versioning or pinning needed.
        Ok(())
    }

    pub fn try_write_mutable_chunk(&self, chunk: &ChunkDescriptor,
                                   data: &[u8]) -> io::Result<()> {
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
              data: &[u8]) -> io::Result<()> {
         Ok(())
    }

    fn promote(&self, chunk: &ChunkDescriptor) -> io::Result<()> {
        Ok(())
    }

    fn delete(&self, chunk: &ChunkDescriptor,
              version: Option<Version>) -> io::Result<()> {
        Ok(())
    }
}

impl Cache for LruFs {
    fn read(&self, chunk: &ChunkDescriptor, version: Option<Version>,
            buf: &mut [u8]) -> io::Result<usize> {
        Ok(buf.len())
    }
}

