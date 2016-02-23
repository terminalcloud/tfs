use scoped_pool::{Pool, Scope};

use std::io::Write;
use std::iter;

use local::{LocalFs, IoResult, Options};
use sparse::BLOCK_SIZE;
use {Storage, Cache, VolumeId, VolumeName, VolumeMetadata, BlockIndex};

pub struct Fs<'id> {
    storage: Box<Storage>,
    caches: Vec<Box<Cache>>,
    local: LocalFs<'id>
}

impl<'id> Fs<'id> {
    pub fn new(storage: Box<Storage>,
               caches: Vec<Box<Cache>>,
               local: LocalFs<'id>) -> Self {
        Fs {
            storage: storage,
            caches: caches,
            local: local
        }
    }

    pub fn run<F, R>(threads: usize, local: Options, storage: Box<Storage>,
                     caches: Vec<Box<Cache>>, fun: F) -> ::Result<R>
    where F: for<'fs> FnOnce(&'fs Fs<'id>, &Scope<'fs>) -> R {
        let pool = Pool::new(threads);
        let localfs = try!(LocalFs::new(local));
        let fs = &Fs::new(storage, caches, localfs);

        // Shut down the pool on exit or panic.
        defer!(pool.shutdown());

        pool.scoped(move |scope| {
            try!(fs.init(scope));
            Ok(scope.zoom(|scope| {
                // If the function panics, we want to shut down the
                // fs so that the flushing and syncing tasks complete.
                //
                // If we don't shutdown the fs, the pool cannot unwind
                // outside the scoped block since the flushing and syncing
                // threads will still be active, causing a deadlock.
                defer!(fs.shutdown());
                fun(fs, scope)
            }))
        })
    }

    pub fn init<'fs>(&'fs self, scope: &Scope<'fs>) -> ::Result<()> {
        self.local().init(self, scope)
    }

    /// Create a new volume.
    ///
    /// It will be addressable via the returned VolumeId; its properties are set to the
    /// given VolumeMetadata.
    ///
    /// Returns the local id of the volume.
    pub fn create(&self, name: &VolumeName, metadata: VolumeMetadata) -> ::Result<VolumeId> {
        self.local.create(name.clone(), metadata)
    }

    /// Open an existing volume for reading and writing.
    ///
    /// Returns the local id of the volume.
    pub fn fork(&self, original: &VolumeName) -> ::Result<VolumeId> {
        let snapshot = try!(self.storage().get_snapshot(original));
        let vol_id = try!(self.local.open(original.clone(), snapshot));

        Ok(vol_id)
    }

    /// Read a block from a volume.
    pub fn read(&self, volume: &VolumeId, block: BlockIndex,
                offset: usize, mut buffer: &mut [u8]) -> ::Result<()> {
        // Try all of our caches in order, starting with local storage
        // and ending with cold storage.
        //
        // Then, if the read succeeded, write it back to local storage
        // for later access.
        match try!(self.local.read(volume, block, offset, buffer)) {
            // The read succeeded locally.
            IoResult::Complete => Ok(()),

            // The read is of an immutable chunk which must be fetched.
            IoResult::Reserved(id) => {
                // TODO: Potential micro-optimization - if offset == 0
                // and buffer.len() >= BLOCK_SIZE, we can just use it
                // instead of allocating our own.
                let read_buffer: &mut [u8] = &mut [0; BLOCK_SIZE];

                self.caches.iter().map(|c| &**c)
                    .chain(iter::once(&self.storage as &Cache))
                    .fold(Err(::Error::NotFound), |res, cache| {
                        res.or_else(|_| cache.read(id, read_buffer))
                    }).and_then(|_| {
                        // Write back the data we got to our local fs.
                        self.local.write_immutable(id, read_buffer)
                    }).and_then(|_| {
                        Ok(try!(buffer.write(&read_buffer[offset..]).map(|_| ())))
                    })
            }
        }
    }

    pub fn write(&self, volume: &VolumeId, block: BlockIndex,
                 offset: usize, data: &[u8]) -> ::Result<()> {
        // Write to the local fs, which will queue flush and sync actions.
        //
        // We may have to fetch data from the network if it is not yet present.
        match try!(self.local.write_mutable(volume, block, offset, data)) {
            // The write succeeded locally.
            IoResult::Complete => Ok(()),

            // The write was to a currently-immutable chunk, and we need to get
            // its data and fill it back in.
            IoResult::Reserved(id) => {
                // TODO: Potential micro-optimization - if offset == 0
                // and buffer.len() >= BLOCK_SIZE, we can just use it
                // instead of allocating our own.
                let read_buffer = &mut [0; BLOCK_SIZE];

                self.caches.iter().map(|c| &**c)
                    .chain(iter::once(&self.storage as &Cache))
                    .fold(Err(::Error::NotFound), |res, cache| {
                        res.or_else(|_| cache.read(id, read_buffer))
                    }).and_then(|_| {
                        // FIXME(#6): If the first write_immutable fails, the mutable
                        // chunk will remain Reserved forever! Probably fix by
                        // not needing an intermediate ImmutableChunk.

                        // Write back the data we got to our local fs.
                        try!(self.local.write_immutable(id, read_buffer));

                        // Finally finish our mutable write.
                        self.local.finish_mutable_write(volume, block,
                                                        read_buffer,
                                                        offset, data)
                    })
            }
        }
    }

    /// Shut down the Fs.
    pub fn shutdown(&self) {
        self.local().shutdown();
    }

    /// Snapshot a local volume under a new name.
    ///
    /// Uploads the new volume under the given name to the backing Storage, making it
    /// available for opening under the new name on this and other nodes connected to
    /// the same Storage.
    pub fn snapshot(&self, id: &VolumeId, name: VolumeName) -> ::Result<()> {
        self.local().snapshot(id)
            .and_then(|snapshot| self.storage.snapshot(&name, snapshot))
    }

    pub fn local(&self) -> &LocalFs<'id> { &self.local }
    pub fn storage(&self) -> &Storage { &self.storage }
}

