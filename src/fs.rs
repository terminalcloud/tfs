use std::sync::Arc;
use std::{io, iter};

use lru::LruFs;
use {Storage, Cache, ChunkDescriptor, FileDescriptor, Version};

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

    pub fn init(&self) -> ::Result<()> {
        self.inner.local.init_flush_thread(self.clone())
    }

    pub fn read(&self, chunk: &ChunkDescriptor, buf: &mut [u8]) -> ::Result<()> {
        // Loop until the version at the beginning and end of a read are the same.
        //
        // This "resolves" conflicts between concurrent reads and writes in a best-effort
        // way. It is still possible to read old data after a write has been submitted,
        // but not yet processed. However, this *always* reflects a race in the program
        // manipulating the file.
        //
        // The strategy is similar to the seqlock reader/writer locking mechanism
        // implemented in the linux kernel. (linux/include/linux/seqlock.h)
        loop {
            // Try all of our caches in order, starting with local storage
            // and ending with cold storage.
            //
            // Then, if the read succeeded, write it back to local storage
            // for later access, evicting as necessary.
            let version = &self.inner.local.version(chunk).map(Version::new);

            let res = iter::once(&self.inner.local as &Cache)
                .chain(self.inner.caches.iter().map(|c| &**c))
                .chain(iter::once(&self.inner.storage as &Cache))
                .fold(Err(::Error::NotFound), |res, cache| {
                    res.or_else(|_| cache.read(chunk, version.clone(), buf))
                }).and_then(|_| {
                    try!(self.inner.local.try_write_immutable_chunk(&chunk, buf));
                    Ok(())
                });

            if version.as_ref().map(|v| v.load()) == self.inner.local.version(chunk) {
                return res
            } else {
                continue
            }
        }
    }

    pub fn write(&self, chunk: &ChunkDescriptor, data: &[u8]) -> ::Result<()> {
        self.inner.local.try_write_mutable_chunk(chunk, data)
    }

    pub fn freeze(&self, file: &FileDescriptor) -> ::Result<()> {
        // For all pinned chunks:
        //   - upload non-versioned chunk to unpin
        // For all unpinned chunks:
        //   - promote
        Ok(())
    }
}

