use slab::Slab;
use variance::InvariantLifetime;

use std::sync::Mutex;
use std::fs::File;
use std::io;

pub const BLOCK_SIZE: usize = 2048;

pub struct IndexedSparseFile<'id> {
    file: File,
    allocator: IndexAllocator<'id>
}

/// An Index, representing a capability to control a block in an IndexedSparseFile.
///
/// Indexes are unique, they cannot be cloned or copied. Having a read pointer to an
/// Index (&'a Index) grants you read-only access to that block for the lifetime 'a,
/// and having a write pointer to an Index (&'a mut Index) grants you read and write
/// access to the block.
///
/// By using Indexes as an access control mechanism, IndexedSparseFiles avoid internal
/// synchronization.
#[derive(Debug, Hash, PartialEq, Eq)]
#[must_use]
pub struct Index<'id> {
    id: InvariantLifetime<'id>,
    index: Frozen<usize>
}

impl<'id> Index<'id> {
    pub fn unchecked_new(index: usize) -> Index<'id> {
        Index {
            id: InvariantLifetime::new(),
            index: Frozen::new(index)
        }
    }

    fn index(&self) -> usize {
        *self.index.as_ref()
    }
}

#[derive(Debug, Clone, Copy, Hash, PartialEq, PartialOrd, Default, Eq)]
pub struct Frozen<T>(T);

impl<T> AsRef<T> for Frozen<T> {
    fn as_ref(&self) -> &T { &self.0 }
}

impl<T> Frozen<T> {
    pub fn new(val: T) -> Self { Frozen(val) }
}

impl<'id> Into<IndexAllocator<'id>> for usize {
    fn into(self) -> IndexAllocator<'id> {
        IndexAllocator::new(self)
    }
}

impl<'id> IndexedSparseFile<'id> {
    pub fn new<I: Into<IndexAllocator<'id>>>(file: File, allocator: I) -> Self {
        IndexedSparseFile {
            file: file,
            allocator: allocator.into()
        }
    }

    /// Read the data at the block with the given Index at the given offset into
    /// the given buffer.
    ///
    /// Preconditions:
    ///   - offset < BLOCK_SIZE
    ///   - buffer.len() <= BLOCK_SIZE - offset
    pub fn read(&self, index: &Index<'id>, offset: usize, buffer: &mut [u8]) -> ::Result<()> {
        assert!(offset < BLOCK_SIZE,
                "offset greater than block size: {:?} > {:?}", offset, BLOCK_SIZE);
        assert!(buffer.len() <= BLOCK_SIZE - offset,
                "requested read larger than block size - offset: {:?} > {:?}",
                buffer.len(), BLOCK_SIZE - offset);

        try!(self.file.pread(BLOCK_SIZE * index.index() + offset, buffer));

        Ok(())
    }

    pub fn write(&self, index: &mut Index<'id>, offset: usize, buffer: &[u8]) -> ::Result<()> {
        assert!(offset < BLOCK_SIZE,
                "offset greater than block size: {:?} > {:?}", offset, BLOCK_SIZE);
        assert!(buffer.len() <= BLOCK_SIZE - offset,
                "requested write larger than block size - offset: {:?} > {:?}",
                buffer.len(), BLOCK_SIZE - offset);

        try!(self.file.pwrite(BLOCK_SIZE * index.index() + offset, buffer));

        Ok(())
    }

    pub fn resize(&mut self, target_size: usize) -> ::Result<()> {
        unimplemented!()
    }

    pub fn allocate(&self) -> Index<'id> {
        self.allocator.allocate()
    }

    pub fn deallocate(&self, index: Index<'id>) -> ::Result<()> {
        try!(self.file.punch(index.index() * BLOCK_SIZE, BLOCK_SIZE));
        self.allocator.deallocate(index);
        Ok(())
    }
}

pub struct IndexAllocator<'id> {
    id: InvariantLifetime<'id>,
    inner: Mutex<Slab<(), usize>>
}

impl<'id> IndexAllocator<'id> {
    fn new(size: usize) -> IndexAllocator<'id> {
        IndexAllocator {
            id: InvariantLifetime::new(),
            inner: Mutex::new(Slab::new(size))
        }
    }

    fn allocate(&self) -> Index<'id> {
        let index = self.inner.lock().unwrap().insert(()).unwrap();
        Index::unchecked_new(index)
    }

    fn deallocate(&self, index: Index<'id>) {
        self.inner.lock().unwrap().remove(*index.index.as_ref());
    }
}

/// Extension methods available on sparse files.
pub trait SparseFileExt {
    /// Punch a hole in the file with the given size at the passed offset.
    fn punch(&self, offset: usize, size: usize) -> io::Result<()>;

    /// Read from the file at the given offset.
    fn pread(&self, offset: usize, buf: &mut [u8]) -> io::Result<usize>;

    /// Write to the file at the given offset.
    fn pwrite(&self, offset: usize, data: &[u8]) -> io::Result<usize>;
}

impl SparseFileExt for File {
    #[cfg(target_os = "linux")]
    fn punch(&self, offset: usize, size: usize) -> io::Result<()> {
        use libc::{fallocate, FALLOC_FL_PUNCH_HOLE, FALLOC_FL_KEEP_SIZE};
        use std::os::unix::io::AsRawFd;

        unsafe {
            cvt(fallocate(self.as_raw_fd(),
                          FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE,
                          offset as i64, size as i64) as i64) }.map(|_| ())
    }

    #[cfg(not(target_os = "linux"))]
    /// WARNING: Implemented using zeroing on the system this documentation was generated for.
    fn punch(&self, offset: usize, size: usize) -> io::Result<()> {
        self.pwrite(offset, &vec![0; size]).map(|_| ())
    }

    fn pread(&self, offset: usize, buf: &mut [u8]) -> io::Result<usize> {
        use libc::pread;
        use std::os::unix::io::AsRawFd;

        unsafe { cvt(pread(self.as_raw_fd(),
                           buf.as_mut_ptr() as *mut ::libc::c_void,
                           buf.len(),
                           offset as i64) as i64) }
    }

    fn pwrite(&self, offset: usize, data: &[u8]) -> io::Result<usize> {
        use libc::pwrite;
        use std::os::unix::io::AsRawFd;

        unsafe { cvt(pwrite(self.as_raw_fd(),
                            data.as_ptr() as *const ::libc::c_void,
                            data.len(),
                            offset as i64) as i64) }
    }
}

// Shim for converting C-style errors to io::Errors.
fn cvt(err: i64) -> io::Result<usize> {
    if err < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(err as usize)
    }
}

#[cfg(test)]
mod test {
    use tempfile::tempfile;

    use sparse::{SparseFileExt, IndexedSparseFile, BLOCK_SIZE};

    #[test]
    fn test_sparse_file_ext() {
        let file = tempfile().unwrap();
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
        const BLOCKS_PER_RUN: usize = 4;
        const INDEX_MULTIPLIER: usize = 20;
        const MAX_CHUNKS: usize = BLOCKS_PER_RUN * INDEX_MULTIPLIER;

        use util::test::gen_random_block;

        let sparse_file = IndexedSparseFile::new(tempfile().unwrap(), MAX_CHUNKS);
        let zeroes: &[u8] = &[0; BLOCK_SIZE];

        // Run the test for several sets of blocks.
        for _ in 0..RUNS {
            let buffers = vec![gen_random_block(BLOCK_SIZE); BLOCKS_PER_RUN];

            let mut indexes = buffers.iter()
                .map(|_| sparse_file.allocate()).collect::<Vec<_>>();

            // Write all the blocks.
            for (index, buffer) in indexes.iter_mut().zip(buffers.iter()) {
                sparse_file.write(index, 0, buffer).unwrap();
            }

            // For each block:
            //   - Read it and confirm its contents
            //   - Evict it
            //   - Read it again and confirm it is evicted
            for (index, buffer) in indexes.into_iter().zip(buffers.iter()) {
                let write_buf: &mut [u8] = &mut [0; BLOCK_SIZE];
                sparse_file.read(&index, 0, write_buf).unwrap();
                assert_eq!(&*write_buf, &**buffer);

                // Evict the block.
                sparse_file.deallocate(index).unwrap();
            }
        }
    }
}

