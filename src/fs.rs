use std::io::Write;
use std::iter;

use local::{LocalFs, ReadResult};
use sparse::BLOCK_SIZE;
use {Storage, Cache, Version, VolumeId, VolumeName, VolumeMetadata, BlockIndex};

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
        // let metadata = try!(self.storage().get_metadata(&original));
        // try!(self.local.open(original, metadata.clone()));

        // Ok(metadata)
        unimplemented!()
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
            ReadResult::Complete => Ok(()),

            // The read is of an immutable chunk which must be fetched.
            ReadResult::Reserved(id) => {
                // TODO: Potential micro-optimization - if offset == 0
                // and buffer.len() >= BLOCK_SIZE, we can just use it
                // instead of allocating our own.
                let read_buffer = &mut [0; BLOCK_SIZE];

                self.caches.iter().map(|c| &**c)
                    .chain(iter::once(&self.storage as &Cache))
                    .fold(Err(::Error::NotFound), |res, cache| {
                        res.or_else(|_| cache.read(id, read_buffer))
                    }).and_then(|_| {
                        // Write back the data we got to our local fs.
                        self.local.try_write_immutable_chunk(id, read_buffer)
                    }).and_then(|_| {
                        Ok(try!(buffer.write(read_buffer).map(|_| ())))
                    })
            }
        }
    }

    pub fn write(&self, volume: &VolumeId, block: BlockIndex,
                 offset: usize, data: &[u8]) -> ::Result<()> {
        unimplemented!()
    }

    /// Snapshot a local volume under a new name.
    ///
    /// Uploads the new volume under the given name to the backing Storage, making it
    /// available for opening under the new name on this and other nodes connected to
    /// the same Storage.
    pub fn snapshot(&self, id: &VolumeId, name: VolumeName) -> ::Result<()> {
        unimplemented!()
    }

    pub fn local(&self) -> &LocalFs<'id> { &self.local }
    pub fn storage(&self) -> &Storage { &self.storage }
}

