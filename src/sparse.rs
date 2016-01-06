use std::os::unix::io::AsRawFd;

use std::collections::HashMap;
use std::fs::File;
use std::io;

use {Chunk, Version};

const BLOCK_SIZE: usize = 2048;

/// The representation of a single tfs object.
///
/// All Blobs begin in the ReadWrite state, where they can be modified
/// to their desired state, then frozen into ReadOnly, where they can be
/// replicated.
pub enum Blob {
    ReadOnly(SparseFile),
    ReadWrite(VersionedSparseFile)
}

impl Blob {
    pub fn new(file: File) -> Blob {
        Blob::ReadWrite(VersionedSparseFile::new(SparseFile::new(file)))
    }

    /// Freeze this Blob into a ReadOnly Blob.
    ///
    /// If the blob is already read-only, does nothing.
    pub fn freeze(self) -> Blob {
        match self {
            this @ Blob::ReadOnly(_) => this,
            Blob::ReadWrite(versioned) => Blob::ReadOnly(versioned.into())
        }
    }
}

/// A sparse file, used for representing tfs objects on disk.
///
/// A sparse file is split into chunks of the same size; each chunk
/// is read and written separately. Conceptually, the file behaves
/// more like a `HashMap<Chunk, Vec<u8>>` then a single large buffer.
///
/// Read only objects are represented as sparse files, where each chunk
/// is lazily acquired when requested. Mutable objects are represented
/// using a VersionedSparseFile, which tracks additional metadata.
pub struct SparseFile {
    file: File
}

/// A sparse file annotated with version information for each chunk.
///
/// Used to represent mutable objects as they are initialized. Each write to
/// a chunk bumps that chunks version number. Only the latest version of each
/// chunk is saved and available for reading.
///
/// Can be "frozen" into a SparseFile using the implementation of `Into<SparseFile>`.
pub struct VersionedSparseFile {
    file: SparseFile,
    versions: HashMap<Chunk, Version>
}

impl SparseFile {
    pub fn new(file: File) -> SparseFile {
        SparseFile { file: file }
    }

    pub fn write(&mut self, chunk: Chunk, data: &[u8]) -> io::Result<()> {
        assert_eq!(data.len(), BLOCK_SIZE);
        self.file.pwrite(BLOCK_SIZE * chunk.0, data).map(|_| ())
    }

    pub fn read(&self, chunk: Chunk, buf: &mut [u8]) -> io::Result<()> {
        assert_eq!(buf.len(), BLOCK_SIZE);
        self.file.pread(BLOCK_SIZE * chunk.0, buf).map(|_| ())
    }

    pub fn evict(&mut self, chunk: Chunk) -> io::Result<()> {
        self.file.punch(chunk.0 * BLOCK_SIZE, BLOCK_SIZE)
    }
}

impl VersionedSparseFile {
    pub fn new(file: SparseFile) -> VersionedSparseFile {
        VersionedSparseFile {
            file: file,
            versions: HashMap::new()
        }
    }

    pub fn version(&self, chunk: &Chunk) -> Option<usize> {
        self.versions.get(chunk)
            .map(|v| v.load())
    }

    pub fn read(&self, chunk: Chunk, buf: &mut [u8]) -> io::Result<()> {
        Ok(())
    }

    pub fn write(&mut self, chunk: Chunk, version: Version, data: &[u8]) -> io::Result<()> {
        Ok(())
    }
}

/// Promote a VersionedSparseFile into a SparseFile.
///
/// Used to transition from Blob::ReadWrite to Blob::ReadOnly
impl Into<SparseFile> for VersionedSparseFile {
    fn into(self) -> SparseFile {
        self.file
    }
}

/// Extension methods available on sparse files.
pub trait SparseFileExt {
    /// Punch a hole in the file with the given size at the passed offset.
    fn punch(&mut self, offset: usize, size: usize) -> io::Result<()>;

    /// Read from the file at the given offset.
    fn pread(&self, offset: usize, buf: &mut [u8]) -> io::Result<usize>;

    /// Write to the file at the given offset.
    fn pwrite(&mut self, offset: usize, data: &[u8]) -> io::Result<usize>;
}

// Shim for converting C-style errors to io::Errors.
fn cvt(err: i64) -> io::Result<usize> {
    if err < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(err as usize)
    }
}

impl SparseFileExt for File {
    fn punch(&mut self, offset: usize, size: usize) -> io::Result<()> {
        use libc::{fallocate, FALLOC_FL_PUNCH_HOLE, FALLOC_FL_KEEP_SIZE};

        unsafe {
            cvt(fallocate(self.as_raw_fd(),
                          FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE,
                          offset as i64, size as i64) as i64) }.map(|_| ())
    }

    fn pread(&self, offset: usize, buf: &mut [u8]) -> io::Result<usize> {
        use libc::pread;

        unsafe { cvt(pread(self.as_raw_fd(),
                           buf.as_mut_ptr() as *mut ::libc::c_void,
                           buf.len(),
                           offset as i64) as i64) }
    }

    fn pwrite(&mut self, offset: usize, data: &[u8]) -> io::Result<usize> {
        use libc::pwrite;

        unsafe { cvt(pwrite(self.as_raw_fd(),
                            data.as_ptr() as *const ::libc::c_void,
                            data.len(),
                            offset as i64) as i64) }
    }
}

#[cfg(test)]
mod test {
    use tempfile::tempfile;

    use sparse::{SparseFileExt, SparseFile, BLOCK_SIZE};
    use {Chunk};

    use std::fs::{OpenOptions, File};

    fn gen_random_chunk() -> Vec<u8> {
        vec![::rand::random(); BLOCK_SIZE]
    }

    #[test]
    fn test_sparse_file_ext() {
        let mut file = tempfile().unwrap();
        file.pwrite(50, &[1, 2, 3, 4, 5]).unwrap();
        file.pwrite(100, &[7, 6, 5, 4, 3, 2, 1]).unwrap();

        let mut buf = &mut [0; 5];
        file.pread(50, buf).unwrap();
        assert_eq!(buf, &[1, 2, 3, 4, 5]);

        let mut buf = &mut [0; 7];
        file.pread(100, buf).unwrap();
        assert_eq!(buf, &[7, 6, 5, 4, 3, 2, 1]);

        // Punched data is read as zeroed.
        let mut buf = &mut [1; 5];
        file.punch(50, 5).unwrap();
        file.pread(50, buf).unwrap();
        assert_eq!(buf, &[0; 5]);

        // Data at the later offset still present after punch.
        let mut buf = &mut [0; 7];
        file.pread(100, buf).unwrap();
        assert_eq!(buf, &[7, 6, 5, 4, 3, 2, 1]);
    }

    #[test]
    fn test_sparse_file_write_read_evict() {
        let mut sparse_file = SparseFile::new(tempfile().unwrap());

        // Run the test for several sets of chunks.
        for _ in 0..4 {
            let buffers = vec![gen_random_chunk(); 4];

            // Write all the chunks.
            for (index, buffer) in buffers.iter().enumerate() {
                let chunk = Chunk(index * 20);
                sparse_file.write(chunk, buffer).unwrap();
            }

            // For each chunk:
            //   - Read it and confirm its contents
            //   - Evict it
            //   - Read it again and confirm it is evicted
            for (index, buffer) in buffers.iter().enumerate() {
                let chunk = Chunk(index * 20);
                let write_buf: &mut [u8] = &mut [0; BLOCK_SIZE];
                sparse_file.read(chunk, write_buf).unwrap();
                assert_eq!(&*write_buf, &**buffer);

                sparse_file.evict(chunk).unwrap();
                sparse_file.read(chunk, write_buf).unwrap();
                assert_eq!(&*write_buf, &[0u8; BLOCK_SIZE] as &[u8]);
            }
        }
    }
}

