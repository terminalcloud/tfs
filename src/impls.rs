use {Storage, Cache, VolumeName, VolumeMetadata, ContentId, Snapshot};

impl Storage for Box<Storage> {
    fn snapshot(&self, volume: &VolumeName, snapshot: Snapshot) -> ::Result<()> {
        (**self).snapshot(volume, snapshot)
    }

    fn get_snapshot(&self, name: &VolumeName) -> ::Result<Snapshot> {
        (**self).get_snapshot(name)
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

