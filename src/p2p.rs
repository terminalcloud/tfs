use {Cache, ChunkDescriptor, Version};

pub struct P2PCache {
    blah: usize
}

impl Cache for P2PCache {
    fn read(&self, chunk: &ChunkDescriptor, version: Option<Version>,
            buf: &mut [u8]) -> ::Result<usize> {
        Ok(buf.len())
    }
}

