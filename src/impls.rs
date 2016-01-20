use {Storage, Cache, ChunkDescriptor, Version};

impl Storage for Box<Storage> {
    fn create(&self, chunk: &ChunkDescriptor, version: Option<Version>,
              data: &[u8]) -> ::Result<()> {
        (**self).create(chunk, version, data)
    }

    fn promote(&self, chunk: &ChunkDescriptor, version: Version) -> ::Result<()> {
        (**self).promote(chunk, version)
    }

    fn delete(&self, chunk: &ChunkDescriptor,
              version: Option<Version>) -> ::Result<()> {
        (**self).delete(chunk, version)
    }
}

impl Cache for Box<Storage> {
    fn read(&self, chunk: &ChunkDescriptor, version: Option<Version>,
            buf: &mut [u8]) -> ::Result<()> {
        (**self).read(chunk, version, buf)
    }
}

