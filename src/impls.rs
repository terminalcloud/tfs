use {Storage, Cache, VolumeName, VolumeMetadata, ContentId};

impl Storage for Box<Storage> {
    fn set_metadata(&self, volume: &VolumeName, metadata: VolumeMetadata) -> ::Result<()> {
        (**self).set_metadata(volume, metadata)
    }

    fn get_metadata(&self, volume: &VolumeName) -> ::Result<VolumeMetadata> {
        (**self).get_metadata(volume)
    }

    fn create(&self, id: ContentId, data: &[u8]) -> ::Result<()> {
        (**self).create(id, data)
    }

    fn delete(&self, id: ContentId) -> ::Result<()> {
        (**self).delete(id)
    }
}

impl Cache for Box<Storage> {
    fn read(&self, id: ContentId, buf: &mut [u8]) -> ::Result<()> {
        (**self).read(id, buf)
    }
}

