use std::sync::Arc;
use std::iter;

use local::LocalFs;
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
        unimplemented!()
    }

    /// Open an existing volume for reading and writing.
    ///
    /// Returns the local id of the volume.
    pub fn fork(&self, original: &VolumeName) -> ::Result<VolumeId> {
        // let metadata = try!(self.storage().get_metadata(&file));
        // try!(self.local().open(file, metadata.clone()));

        // Ok(metadata)
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

    pub fn read(&self, volume: &VolumeId, block: BlockIndex,
                offset: usize, buffer: &mut [u8]) -> ::Result<()> {
        unimplemented!()
    }

    pub fn write(&self, volume: &VolumeId, block: BlockIndex,
                 offset: usize, data: &[u8]) -> ::Result<()> {
        unimplemented!()
    }

    pub fn local(&self) -> &LocalFs<'id> { &self.local }
    pub fn storage(&self) -> &Storage { &self.storage }
}

