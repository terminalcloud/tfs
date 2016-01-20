use {Storage, Cache, ChunkDescriptor, Version, FileMetadata, FileDescriptor};

pub struct S3Storage {
    blah: usize
}

impl Storage for S3Storage {
    fn set_metadata(&self, file: &FileDescriptor, metadata: FileMetadata) -> ::Result<()> {
        Ok(())
    }

    fn get_metadata(&self, file: &FileDescriptor) -> ::Result<FileMetadata> {
        Ok(FileMetadata { size: 0 })
    }

    fn create(&self, chunk: &ChunkDescriptor, version: Option<Version>,
              data: &[u8]) -> ::Result<()> {
         Ok(())
    }

    fn promote(&self, chunk: &ChunkDescriptor, version: Version) -> ::Result<()> {
        Ok(())
    }

    fn delete(&self, chunk: &ChunkDescriptor,
              version: Option<Version>) -> ::Result<()> {
        Ok(())
    }
}

impl Cache for S3Storage {
    fn read(&self, chunk: &ChunkDescriptor, version: Option<Version>,
            buf: &mut [u8]) -> ::Result<()> {
        Ok(())
    }
}

