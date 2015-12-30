use std::io;
use {Storage, Cache, ChunkDescriptor, Version};

pub struct S3Storage {
    blah: usize
}

impl Storage for S3Storage {
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

impl Cache for S3Storage {
    fn read(&self, chunk: &ChunkDescriptor, version: Option<Version>,
            buf: &mut [u8]) -> io::Result<usize> {
        Ok(buf.len())
    }
}

