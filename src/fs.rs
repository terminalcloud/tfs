use std::sync::Arc;
use std::{io, iter};

use lru::LruFs;
use {Storage, Cache, ChunkDescriptor, FileDescriptor};

#[derive(Clone)]
pub struct Fs {
    inner: Arc<FsInner>
}

struct FsInner {
    storage: Box<Storage>,
    caches: Vec<Box<Cache>>,
    local: LruFs
}

impl Fs {
    pub fn new() -> Self {
        unimplemented!()
    }

    pub fn init(&self) -> io::Result<()> {
        self.inner.local.init_flush_thread(self.clone())
    }

    pub fn read(&self, chunk: &ChunkDescriptor, buf: &mut [u8]) -> io::Result<usize> {
        // Try all of our caches in order, starting with local storage
        // and ending with cold storage.
        //
        // Then, if the read succeeded, write it back to local storage
        // for later access, evicting as necessary.

        iter::once(&self.inner.local as &Cache)
            .chain(self.inner.caches.iter().map(|c| &**c))
            .chain(iter::once(&self.inner.storage as &Cache))
            .fold(Err(chunk.not_found(None)), |res, cache| {
                res.or_else(|_| cache.read(chunk, None, buf))
            }).and_then(|u| {
                try!(self.inner.local.try_write_immutable_chunk(&chunk, buf));
                Ok(u)
            })
    }

    pub fn write(&self, chunk: &ChunkDescriptor, data: &[u8]) -> io::Result<()> {
        self.inner.local.try_write_mutable_chunk(chunk, data)
    }

    pub fn freeze(&self, file: &FileDescriptor) -> io::Result<()> {
        // For all pinned chunks:
        //   - upload non-versioned chunk to unpin
        // For all unpinned chunks:
        //   - promote
        Ok(())
    }
}

