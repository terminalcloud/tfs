use {Storage, Cache, VolumeName, ContentId, VolumeMetadata};

pub struct S3Storage {
    blah: usize
}

impl Storage for S3Storage {
    fn set_metadata(&self, volume: &VolumeName, metadata: VolumeMetadata) -> ::Result<()> { unimplemented!() }
    fn get_metadata(&self, volume: &VolumeName) -> ::Result<VolumeMetadata> { unimplemented!() }

    fn create(&self, id: ContentId, data: &[u8]) -> ::Result<()> { unimplemented!() }
    fn delete(&self, id: ContentId) -> ::Result<()> { unimplemented!() }
}

impl Cache for S3Storage {
    fn read(&self, id: ContentId, buf: &mut [u8]) -> ::Result<()> { unimplemented!() }
}

