use std::io;
use {Storage, Cache, ChunkDescriptor, Version};

impl Storage for Box<Storage> {
    fn create(&self, chunk: &ChunkDescriptor, version: Option<Version>,
              data: &[u8]) -> io::Result<()> {
        (**self).create(chunk, version, data)
    }

    fn promote(&self, chunk: &ChunkDescriptor) -> io::Result<()> {
        (**self).promote(chunk)
    }

    fn delete(&self, chunk: &ChunkDescriptor,
              version: Option<Version>) -> io::Result<()> {
        (**self).delete(chunk, version)
    }
}

impl Cache for Box<Storage> {
    fn read(&self, chunk: &ChunkDescriptor, version: Option<Version>,
            buf: &mut [u8]) -> io::Result<usize> {
        (**self).read(chunk, version, buf)
    }
}

