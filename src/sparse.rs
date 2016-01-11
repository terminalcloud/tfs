use bit_vec::BitVec;

use std::collections::HashMap;
use std::fs::File;
use std::{io, ops};

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
    /// Create a new ReadWrite blob with the given number of chunks.
    pub fn new(file: File, size: usize) -> Blob {
        Blob::ReadWrite(VersionedSparseFile::new(SparseFile::new(file, size)))
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

impl ops::Deref for Blob {
    type Target = SparseFile;

    fn deref(&self) -> &SparseFile {
        match *self {
            Blob::ReadOnly(ref ro) => ro,
            Blob::ReadWrite(ref v) => &v.file
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
    file: File,
    chunks: BitVec
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
    /// Create a new SparseFile using the passed File for storage.
    ///
    /// The SparseFile is limited to storing `size` chunks of equal size.
    ///
    /// The created SparseFile assumes the File has no chunks in it,
    /// to resume from a previous state use `SparseFile::load`.
    ///
    /// Equivalent to `SparseFile::load(file, BitVec::from_elem(size, false))`.
    pub fn new(file: File, size: usize) -> SparseFile {
        SparseFile::load(file, BitVec::from_elem(size, false))
    }

    /// Load a SparseFile from a backing File and a chunks BitVec.
    pub fn load(file: File, chunks: BitVec) -> SparseFile {
        SparseFile {
            file: file,
            chunks: chunks
        }
    }

    pub fn write(&mut self, chunk: Chunk, data: &[u8]) -> ::Result<()> {
        assert_eq!(data.len(), BLOCK_SIZE);
        self.chunks.set(chunk.0, true);
        try!(self.file.pwrite(BLOCK_SIZE * chunk.0, data));
        Ok(())
    }

    pub fn read(&self, chunk: Chunk, buf: &mut [u8]) -> ::Result<()> {
        assert_eq!(buf.len(), BLOCK_SIZE);
        if self.chunks[chunk.0] {
            try!(self.file.pread(BLOCK_SIZE * chunk.0, buf));
            Ok(())
        } else {
            Err(::Error::NotFound)
        }
    }

    pub fn evict(&mut self, chunk: Chunk) -> ::Result<()> {
        self.chunks.set(chunk.0, false);
        try!(self.file.punch(chunk.0 * BLOCK_SIZE, BLOCK_SIZE));
        Ok(())
    }
}

impl VersionedSparseFile {
    pub fn new(file: SparseFile) -> VersionedSparseFile {
        VersionedSparseFile {
            file: file,
            versions: HashMap::new()
        }
    }

    /// Get the current version of the chunk.
    ///
    /// This is done with a single SeqCst atomic load - note that
    /// this value may be out of date and a more recent version
    /// written immediately.
    pub fn version(&self, chunk: &Chunk) -> Option<usize> {
        self.versions.get(chunk)
            .map(|v| v.load())
    }

    /// Read the most up-to-date version of the chunk.
    pub fn read(&self, chunk: Chunk, buf: &mut [u8]) -> ::Result<()> {
        self.file.read(chunk, buf)
    }

    /// Write a new version of the chunk.
    pub fn write(&mut self, chunk: Chunk, data: &[u8]) -> ::Result<()> {
        self.versions.entry(chunk)
            .or_insert(Version::new(0))
            .increment();
        self.file.write(chunk, data)
    }

    /// Do a write *without* incrementing the version, for refilling after eviction.
    ///
    /// In order to remain consistent, the data *must* match the current version
    /// of the chunk.
    pub fn fill(&mut self, chunk: Chunk, data: &[u8]) -> ::Result<()> {
        self.file.write(chunk, data)
    }

    /// Evict the chunk from the file.
    ///
    /// This does not reset the version of the chunk.
    pub fn evict(&mut self, chunk: Chunk) -> ::Result<()> {
        self.file.evict(chunk)
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

#[cfg(target_os = "linux")]
mod _unix {
    use std::io;
    use std::fs::File;
    use std::os::unix::io::AsRawFd;

    use super::{SparseFileExt};

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
}

#[cfg(target_os = "linux")]
mod _other {

}

#[cfg(test)]
mod test {
    use tempfile::tempfile;

    use sparse::{SparseFileExt, SparseFile, BLOCK_SIZE,
                 VersionedSparseFile};
    use {Chunk};

    use std::sync::RwLock;

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
        const RUNS: usize = 4;
        const CHUNKS_PER_RUN: usize = 4;
        const INDEX_MULTIPLIER: usize = 20;
        const MAX_CHUNKS: usize = CHUNKS_PER_RUN * INDEX_MULTIPLIER;

        use util::test::gen_random_chunk;

        let mut sparse_file = SparseFile::new(tempfile().unwrap(),
                                              MAX_CHUNKS);

        // Run the test for several sets of chunks.
        for _ in 0..RUNS {
            let buffers = vec![gen_random_chunk(BLOCK_SIZE); CHUNKS_PER_RUN];

            // Write all the chunks.
            for (index, buffer) in buffers.iter().enumerate() {
                let chunk = Chunk(index * INDEX_MULTIPLIER);
                sparse_file.write(chunk, buffer).unwrap();
            }

            // For each chunk:
            //   - Read it and confirm its contents
            //   - Evict it
            //   - Read it again and confirm it is evicted
            for (index, buffer) in buffers.iter().enumerate() {
                let chunk = Chunk(index * INDEX_MULTIPLIER);
                let write_buf: &mut [u8] = &mut [0; BLOCK_SIZE];
                sparse_file.read(chunk, write_buf).unwrap();
                assert_eq!(&*write_buf, &**buffer);

                sparse_file.evict(chunk).unwrap();
                sparse_file.read(chunk, write_buf).unwrap_err()
                    .assert_not_found();
            }
        }
    }

    #[test]
    fn fuzz_versioned_sparse_file() {
        // Properties:
        //   - Reads always return latest version
        //   - Writes bump the version
        //   - Evict then write doesn't reset version

        const MAGNITUDE: usize = 1;
        const NUM_VERSIONS: usize = 10 * MAGNITUDE;
        const NUM_OBJECTS: usize = 10 * MAGNITUDE;
        const CHUNK_SIZE: usize = BLOCK_SIZE;
        const MAX_CHUNKS: usize = NUM_OBJECTS;

        use scoped_threadpool::Pool;
        use util::test::gen_random_objects;

        let objects = gen_random_objects(NUM_OBJECTS, CHUNK_SIZE, NUM_VERSIONS);
        let sparse_file = &RwLock::new(VersionedSparseFile::new(
            SparseFile::new(tempfile().unwrap(), MAX_CHUNKS)));

        // Use a thread pool to run the tests.
        let mut pool = Pool::new(objects.len() as u32);

        pool.scoped(|scope| {
            for (index, versions) in objects.values().enumerate() {
                let chunk = Chunk(index);
                scope.execute(move || {
                    // Write all the versions of the chunk in order.
                    // After each write, confirm the version, then confirm the contents.
                    for (version, data) in versions.iter().enumerate() {
                        // Write the data.
                        sparse_file.write().unwrap().write(chunk, data).unwrap();

                        // Check the version.
                        assert_eq!(sparse_file.read().unwrap().version(&chunk).unwrap(),
                                   version + 1); // versions start at 1 not 0

                        // Read the data, confirm the contents.
                        let buf: &mut [u8] = &mut [0; CHUNK_SIZE];
                        sparse_file.read().unwrap().read(chunk, buf).unwrap();
                        assert_eq!(&**data, buf);

                        // Evict the chunk.
                        sparse_file.write().unwrap().evict(chunk).unwrap();

                        // Expect NotFound.
                        sparse_file.read().unwrap().read(chunk, buf)
                            .unwrap_err().assert_not_found();

                        // Next write of data should work fine, versions should
                        // not be reset on eviction then rewrite.
                    }
                });
            }
        });
    }
}

