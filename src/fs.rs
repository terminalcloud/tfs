use std::sync::Arc;
use std::{io, iter};

use local::LocalFs;
use {Storage, Cache, ChunkDescriptor, FileDescriptor, Version};

#[derive(Clone)]
pub struct Fs {
    inner: Arc<FsInner>
}

struct FsInner {
    storage: Box<Storage>,
    caches: Vec<Box<Cache>>,
    local: LocalFs
}

impl Fs {
    pub fn new(storage: Box<Storage>,
               caches: Vec<Box<Cache>>,
               local: LocalFs) -> Self {
        Fs {
            inner: Arc::new(FsInner {
                storage: storage,
                caches: caches,
                local: local
            })
        }
    }

    pub fn init(&self) -> ::Result<()> {
        self.inner.local.init_flush_thread(self.clone())
    }

    pub fn read(&self, chunk: &ChunkDescriptor, buf: &mut [u8]) -> ::Result<()> {
        // TODO: Remove NotFound as an error - we find out if its not found by reading metadata.
        // Before trying to do a read, fetch metadata to look for NotFound.
        //
        // LocalFs should change to assume a read of a not-found chunk means it should be reserved:
        //   - then, in try_write_immutable_chunk we transition from reserved -> stable
        //   - any read which sees a reserved state blocks (on a CondVar) until the data
        //     is downloaded
        //
        // This approach removes a potentially undesirable behavior where concurrent reads of a
        // block which is not in the local cache will race to read from the other Caches and
        // Storage, which can cause large amounts of unnecessary network traffic.

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
            // for later access.
            let version = &self.inner.local.version(chunk).map(Version::new);

            let res = iter::once(&self.inner.local as &Cache)
                .chain(self.inner.caches.iter().map(|c| &**c))
                .chain(iter::once(&self.inner.storage as &Cache))
                .fold(Err(::Error::NotFound), |res, cache| {
                    res.or_else(|_| cache.read(chunk, version.clone(), buf))
                }).and_then(|_| {
                    // Write back the data we got to our cache.
                    //
                    // The LocalFs takes care to ensure we can't overwrite
                    // with old data.
                    self.inner.local.try_write_immutable_chunk(&chunk,
                                                               version.clone(),
                                                               buf)
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

    pub fn local(&self) -> &LocalFs { &self.inner.local }
    pub fn storage(&self) -> &Storage { &*self.inner.storage }
}

