use {Storage, Cache, VolumeName, ContentId, VolumeMetadata, Snapshot};

pub struct S3Storage {
    blah: usize
}

impl Storage for S3Storage {
    fn snapshot(&self, volume: &VolumeName, snapshot: Snapshot) -> ::Result<()> { unimplemented!() }

    fn get_snapshot(&self, name: &VolumeName) -> ::Result<Snapshot> { unimplemented!() }
    fn get_metadata(&self, volume: &VolumeName) -> ::Result<VolumeMetadata> { unimplemented!() }

    fn create(&self, id: ContentId, data: &[u8]) -> ::Result<()> { unimplemented!() }
    fn delete(&self, id: ContentId) -> ::Result<()> { unimplemented!() }
}

impl Cache for S3Storage {
    fn read(&self, id: ContentId, buf: &mut [u8]) -> ::Result<()> { unimplemented!() }
}

