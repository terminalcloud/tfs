use vec_map::VecMap;
use rwlock2::RwLock;

use std::collections::HashMap;
use std::fs::File;
use std::sync::{Mutex, Condvar, MutexGuard};
use std::io;

use std::os::unix::io::{AsRawFd, FromRawFd};

use {Chunk, Version, FileMetadata};

pub const BLOCK_SIZE: usize = 2048;

/// The representation of a single tfs object.
///
/// All Blobs begin in the ReadWrite state, where they can be modified
/// to their desired state, then frozen into ReadOnly, where they can be
/// replicated.
pub enum Blob {
    ReadOnly(ReadOnlySparseFile),
    ReadWrite(VersionedSparseFile)
}

impl Blob {
    /// Create a new ReadWrite blob with the given number of chunks.
    pub fn new(file: File, size: usize) -> Blob {
        Blob::ReadWrite(VersionedSparseFile::new(RawSparseFile::new(file, size)))
    }

    /// Open a new ReadOnly blob with the given number of chunks.
    pub fn open(file: File, size: usize) -> Blob {
        Blob::ReadOnly(ReadOnlySparseFile::new(RawSparseFile::new(file, size)))
    }

    /// Freeze this Blob into a ReadOnly Blob.
    ///
    /// Returns a map of chunk index to version for all chunks that should
    /// be promoted on the backend Storage.
    ///
    /// If the blob is already read-only, does nothing.
    pub fn freeze(this: &RwLock<Self>) -> ::Result<(HashMap<usize, usize>,
                                                    FileMetadata)> {
        loop {
            match *this.read().unwrap() {
                Blob::ReadOnly(_) => return Err(::Error::AlreadyFrozen),
                Blob::ReadWrite(ref rw) => rw.wait_for_freeze()
            };

            let mut blob_guard = this.write().unwrap();

            // Try to freeze
            let (ro, versions) = match *blob_guard {
                Blob::ReadOnly(_) => return Err(::Error::AlreadyFrozen),
                Blob::ReadWrite(ref mut rw) => {
                    if let Ok(res) = rw.optimistic_freeze() {
                        res
                    } else {
                        // If the freeze now fails, retry.
                        continue
                    }
                }
            };

            // Freeze successful.
            let metadata = FileMetadata { size: ro.file.file.size };
            *blob_guard = Blob::ReadOnly(ro);
            return Ok((versions, metadata))
        }
    }

    /// Read the chunk into the buffer.
    ///
    /// May require retrying.
    pub fn read(&self, chunk: Chunk, buf: &mut [u8]) -> ::Result<()> {
        match *self {
            Blob::ReadOnly(ref ro) => ro.read(chunk, buf),
            Blob::ReadWrite(ref rw) => rw.read(chunk, buf)
        }
    }

    /// Fill the cache for this chunk if not already full.
    ///
    /// If the version is outdated no data will be written.
    pub fn fill(&self, chunk: Chunk, version: Option<Version>,
                data: &[u8]) -> ::Result<()> {
        match *self {
            Blob::ReadOnly(ref ro) => ro.fill(chunk, data),
            Blob::ReadWrite(ref rw) => rw.fill(chunk, version.unwrap(), data),
        }
    }

    pub fn as_read_write(&self) -> Option<&VersionedSparseFile> {
        match *self {
            Blob::ReadWrite(ref rw) => Some(rw),
            _ => None
        }
    }

    pub fn as_read_only(&self) -> Option<&ReadOnlySparseFile> {
        match *self {
            Blob::ReadOnly(ref ro) => Some(ro),
            _ => None
        }
    }

    /// Get the VersionedSparseFile in this Blob::ReadWrite
    ///
    /// ## Panics
    ///
    /// Panics if the Blob is ReadOnly.
    pub fn assert_versioned_mut(&mut self) -> &mut VersionedSparseFile {
        match *self {
            Blob::ReadWrite(ref mut v) => v,
            _ => panic!("Asserted ReadOnly blob as ReadWrite!")
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
struct RawSparseFile {
    file: File,
    size: usize
}

/// A Sparse File with context for each chunk.
///
/// An instance of C is stored associated with each chunk.
struct SparseFile<C> {
    file: RawSparseFile,
    context: VecMap<C>
}

/// A Sparse File for representing read-only objects.
///
/// Stores an instance of ReadChunkMetadata for each Chunk,
/// which can be used to queue and wait for reads to resolve.
pub struct ReadOnlySparseFile {
    file: SparseFile<ReadChunkMetadata>
}

/// A sparse file annotated with version information for each chunk.
///
/// Used to represent mutable objects as they are initialized. Each write to
/// a chunk bumps that chunks version number. Only the latest version of each
/// chunk is saved and available for reading.
pub struct VersionedSparseFile {
    file: SparseFile<VersionedChunkMetadata>
}

impl RawSparseFile {
    /// Create a new RawSparseFile using the passed File for storage.
    ///
    /// The RawSparseFile is limited to storing `size` chunks of equal size.
    ///
    /// The created RawSparseFile assumes the File has no chunks in it,
    /// to resume from a previous state use `SparseFile::load`.
    pub fn new(file: File, size: usize) -> RawSparseFile {
        RawSparseFile {
             file: file,
             size: size
        }
    }

    pub fn write(&self, chunk: Chunk, data: &[u8]) -> ::Result<()> {
        assert_eq!(data.len(), BLOCK_SIZE);
        try!(self.file.pwrite(BLOCK_SIZE * chunk.0, data));
        Ok(())
    }

    pub fn read(&self, chunk: Chunk, buf: &mut [u8]) -> ::Result<()> {
        assert_eq!(buf.len(), BLOCK_SIZE);
        try!(self.file.pread(BLOCK_SIZE * chunk.0, buf));
        Ok(())
    }

    pub fn evict(&self, chunk: Chunk) -> ::Result<()> {
        try!(self.file.punch(chunk.0 * BLOCK_SIZE, BLOCK_SIZE));
        Ok(())
    }

    pub fn try_clone(&self) -> ::Result<Self> {
        use libc::{dup, c_int};

        let file = unsafe {
            let duplicated = try!(cvt(dup(self.file.as_raw_fd()) as i64));
            File::from_raw_fd(duplicated as c_int)
        };

        Ok(RawSparseFile {
            file: file,
            size: self.size
        })
    }
}

impl ReadOnlySparseFile {
    fn new(raw: RawSparseFile) -> ReadOnlySparseFile {
        let size = raw.size;

        ReadOnlySparseFile {
            file: SparseFile {
                file: raw,
                context: (0..size)
                    .map(|i| (i, ReadChunkMetadata::new()))
                    .collect()
            }
        }
    }

    pub fn read(&self, chunk: Chunk, buf: &mut [u8]) -> ::Result<()> {
        let _lock = try!(self.file.context.get(&chunk.0).unwrap()
            .wait_for_read());
        self.file.file.read(chunk, buf)
    }

    pub fn fill(&self, chunk: Chunk, buf: &[u8]) -> ::Result<()> {
        let lock = try!(self.file.context.get(&chunk.0).unwrap()
            .wait_for_fill());
        try!(self.file.file.write(chunk, buf));

        // The operation completed successfully.
        lock.complete();
        Ok(())
    }

    pub fn evict(&self, chunk: Chunk) -> ::Result<()> {
        let context = self.file.context.get(&chunk.0).unwrap();
        let _lock = try!(context.wait_for_evict());

        // We don't actually care if this succeeds.
        self.file.file.evict(chunk).ok();

        Ok(())
    }
}

impl VersionedSparseFile {
    fn new(file: RawSparseFile) -> VersionedSparseFile {
        let size = file.size;

        VersionedSparseFile {
            file: SparseFile {
                file: file,
                context: (0..size)
                    .map(|i| (i, VersionedChunkMetadata::new()))
                    .collect()
            }
        }
    }

    /// Get the current version of the chunk.
    ///
    /// This is done with a single SeqCst atomic load - note that
    /// this value may be out of date and a more recent version
    /// written immediately.
    pub fn version(&self, chunk: &Chunk) -> Option<usize> {
        self.file.context.get(&chunk.0)
            .map(|metadata| metadata.version.load())
    }

    /// Read the most up-to-date version of the chunk.
    pub fn read(&self, chunk: Chunk, buf: &mut [u8]) -> ::Result<()> {
        let _lock = try!(self.file.context.get(&chunk.0).unwrap().wait_for_read());
        self.file.file.read(chunk, buf)
    }

    /// Write a new version of the chunk.
    ///
    /// Returns the version written.
    pub fn write(&self, chunk: Chunk, data: &[u8]) -> ::Result<usize> {
        let context = self.file.context.get(&chunk.0).unwrap();
        let lock = try!(context.wait_for_write());
        let version = context.version.increment();

        try!(self.file.file.write(chunk, data));
        lock.complete();

        Ok(version)
    }

    /// Do a write *without* incrementing the version, for refilling after eviction.
    ///
    /// In order to remain consistent, the data *must* match the current version
    /// of the chunk.
    pub fn fill(&self, chunk: Chunk, version: Version, data: &[u8]) -> ::Result<()> {
        let context = self.file.context.get(&chunk.0).unwrap();
        let lock = try!(context.wait_for_fill());

        if version.load() != context.version.load() {
            // Abort the operation, it will be retried.
            return Ok(())
        }

        try!(self.file.file.write(chunk, data));
        lock.complete();
        Ok(())
    }

    /// Evict the chunk from the file.
    ///
    /// This does not reset the version of the chunk.
    pub fn evict(&self, chunk: Chunk) -> ::Result<()> {
        let _lock = try!(self.file.context.get(&chunk.0).unwrap()
            .wait_for_evict());

        // We don't actually care if this succeeds.
        self.file.file.evict(chunk).ok();
        Ok(())
    }

    pub fn complete_flush(&self, chunk: Chunk, version: Version) -> ::Result<()> {
        self.file.context.get(&chunk.0).unwrap().complete_flush(version)

    }

    fn wait_for_freeze(&self) {
        // Wait for every chunk to enter into the stable state.
        //
        // NOTE: we acquire the locks one by one, as opposed to
        // all at once. This may or may not be ideal, to be determined.
        self.file.context.values()
            .map(|state| state.wait_for_freeze())
            .count();
    }

    fn optimistic_freeze(&mut self) -> ::Result<(ReadOnlySparseFile,
                                                 HashMap<usize, usize>)> {
        let mut versions = HashMap::with_capacity(self.file.file.size);
        let read_only_context = try!(self.file.context.iter_mut()
            .enumerate()
            .map(|(index, (_, md))| {
                let read_state = match *md.state.get_mut().unwrap() {
                    VersionedChunkState::Evicted => ReadChunkState::Evicted,
                    VersionedChunkState::Stable => ReadChunkState::Stable,

                    // If we get to an invalid state, indicate we should retry.
                    _ => return Err(::Error::Retry)
                };

                versions.insert(index, md.version.load());

                Ok((index, ReadChunkMetadata {
                    state: Mutex::new(read_state),
                    cond: Condvar::new()
                }))
        }).collect::<::Result<VecMap<ReadChunkMetadata>>>());

        Ok((ReadOnlySparseFile {
            file: SparseFile {
                file: try!(self.file.file.try_clone()),
                context: read_only_context
            }
        }, versions))
    }
}

struct ReadChunkMetadata {
    state: Mutex<ReadChunkState>,
    cond: Condvar
}

struct FillGuard<'a> {
    lock: MutexGuard<'a, ReadChunkState>,
    metadata: &'a ReadChunkMetadata,
    complete: bool
}

impl<'a> FillGuard<'a> {
    fn complete(mut self) {
        self.complete = true;
    }
}

impl<'a> Drop for FillGuard<'a> {
    fn drop(&mut self) {
        // The write succeeded.
        if self.complete {
            // Transition to the stable state.
            *self.lock = ReadChunkState::Stable;

            // Wake pending readers.
            self.metadata.cond.notify_all()
        // The write failed.
        } else {
            // Transition back to the evicted state.
            *self.lock = ReadChunkState::Evicted;

            // Wake exactly one other reader to retry.
            self.metadata.cond.notify_one()
        }
    }
}

impl ReadChunkMetadata {
    /// Initialize a new chunk-level lock.
    ///
    /// The initial state is Evicted.
    fn new() -> ReadChunkMetadata {
        ReadChunkMetadata {
            state: Mutex::new(ReadChunkState::Evicted),
            cond: Condvar::new(),
        }
    }

    /// Acquire a lock used for evicting a chunk.
    ///
    /// The eviction should be completed before the lock is released.
    fn wait_for_evict(&self) -> ::Result<MutexGuard<ReadChunkState>> {
        let mut state_lock = self.state.lock().unwrap();
        *state_lock = ReadChunkState::Evicted;
        Ok(state_lock)
    }

    /// Acquire a lock used for filling a chunk.
    ///
    /// The write should be completed before the lock is released,
    /// and if the write is succesful the FillGuard should be `complete`d
    /// before dropping to prevent a redundant eviction.
    fn wait_for_fill(&self) -> ::Result<FillGuard> {
        let state_lock = self.state.lock().unwrap();
        debug_assert!(*state_lock == ReadChunkState::Reserved,
                      "state_lock == {:?}, expected Reserved",
                      *state_lock);

        Ok(FillGuard {
            lock: state_lock,
            metadata: self,
            complete: false
        })
    }

    /// Wait for the appropriate state transition before doing a read.
    ///
    /// The read should be completed before the lock is released.
    fn wait_for_read(&self) -> ::Result<MutexGuard<ReadChunkState>> {
        let mut state_lock = self.state.lock().unwrap();

        loop {
            match *state_lock {
                ReadChunkState::Reserved => {
                    state_lock = self.cond.wait(state_lock).unwrap()
                }
                ReadChunkState::Evicted => {
                    *state_lock = ReadChunkState::Reserved;
                    return Err(::Error::Reserved)
                },
                ReadChunkState::Stable => return Ok(state_lock)
            }
        }
    }
}

#[derive(Debug, Copy, Clone, PartialEq)]
enum ReadChunkState {
    Reserved,
    Evicted,
    Stable
}

struct VersionedChunkMetadata {
    state: Mutex<VersionedChunkState>,
    cond: Condvar,
    version: Version
}
#[derive(Debug, Copy, Clone, PartialEq)]
enum VersionedChunkState {
    Reserved,
    Evicted,
    Dirty,
    Stable
}

struct VersionedFillGuard<'a> {
    lock: MutexGuard<'a, VersionedChunkState>,
    previous: VersionedChunkState,
    metadata: &'a VersionedChunkMetadata,
    complete: bool
}

impl<'a> VersionedFillGuard<'a> {
    fn complete(mut self) {
        self.complete = true
    }
}

impl<'a> Drop for VersionedFillGuard<'a> {
    fn drop(&mut self) {
        // The write succeeded.
        if self.complete {
            // Transition to the stable state.
            *self.lock = VersionedChunkState::Stable;

            // Wake pending readers.
            self.metadata.cond.notify_all()
        // The write failed.
        } else {
            // Transition back to our previous state.
            *self.lock = self.previous;

            // Wake other threads, allowing one reader or
            // freezer to continue.
            self.metadata.cond.notify_all()
        }
    }
}

struct WriteGuard<'a> {
    lock: MutexGuard<'a, VersionedChunkState>,
    previous: VersionedChunkState,
    metadata: &'a VersionedChunkMetadata,
    complete: bool
}

impl<'a> WriteGuard<'a> {
    fn complete(mut self) {
        self.complete = true
    }
}

impl<'a> Drop for WriteGuard<'a> {
    fn drop(&mut self) {
        // The write failed.
        if !self.complete {
            // Transition back to our previous state.
            *self.lock = self.previous;
        }

        // Wake pending readers/freezers.
        self.metadata.cond.notify_all()
    }
}

impl VersionedChunkMetadata {
    fn new() -> VersionedChunkMetadata {
        VersionedChunkMetadata {
            state: Mutex::new(VersionedChunkState::Evicted),
            cond: Condvar::new(),
            version: Version::new(0)
        }
    }

    fn wait_for_freeze(&self) -> MutexGuard<VersionedChunkState> {
        use self::VersionedChunkState::*;

        let mut state_lock = self.state.lock().unwrap();
        loop {
            match *state_lock {
                Stable | Evicted => return state_lock,
                Dirty | Reserved => state_lock = self.cond.wait(state_lock).unwrap()
            }
        }
    }

    fn wait_for_evict(&self) -> ::Result<MutexGuard<VersionedChunkState>> {
        use self::VersionedChunkState::*;

        let mut state_lock = self.state.lock().unwrap();
        debug_assert!(*state_lock != Dirty,
                      "state_lock is Dirty on eviction!");

        // The only other non-Stable states are already Evicted
        // and Reserved.
        //
        // If we are currently Reserved we must be thrashing our cache
        // very rapidly, so it's unlikely we care about a duplicate
        // network request anyway.
        *state_lock = Evicted;
        Ok(state_lock)
    }

    fn wait_for_fill(&self) -> ::Result<VersionedFillGuard> {
        use self::VersionedChunkState::*;

        let mut state_lock = self.state.lock().unwrap();
        let previous = *state_lock;
        match previous {
            Evicted | Reserved => {
                *state_lock = Stable;
                Ok(VersionedFillGuard {
                    lock: state_lock,
                    metadata: self,
                    previous: previous,
                    complete: false
                })
            },
            Dirty | Stable => Err(::Error::Retry),
        }
    }

    fn wait_for_write(&self) -> ::Result<WriteGuard> {
        let mut state_lock = self.state.lock().unwrap();

        let previous = *state_lock;
        *state_lock = VersionedChunkState::Dirty;

        Ok(WriteGuard {
            lock: state_lock,
            previous: previous,
            metadata: self,
            complete: false
        })
    }

    /// Wait for the appropriate state transitions to take place before
    /// doing a read of this chunk.
    ///
    /// The read should be completed before releasing the lock.
    fn wait_for_read(&self) -> ::Result<MutexGuard<VersionedChunkState>> {
        use self::VersionedChunkState::*;

        let mut state_lock = self.state.lock().unwrap();
        loop {
            match *state_lock {
                Reserved => {
                    state_lock = self.cond.wait(state_lock).unwrap()
                }
                Evicted => {
                    *state_lock = Reserved;
                    return Err(::Error::Reserved)
                },
                Dirty | Stable => return Ok(state_lock)
            }
        }
    }

    fn complete_flush(&self, version: Version) -> ::Result<()> {
        use self::VersionedChunkState::*;

        // Check one last time if we need to acquire the chunk lock.
        if self.version.load() != version.load() { return Ok(()) }

        // Acquire the lock and attempt to transition to Stable.
        let mut state_lock = self.state.lock().unwrap();

        // The version may have changed, check it again.
        if self.version.load() != version.load() { return Ok(()) }

        // Transition from Dirty -> Stable.
        //
        // If the state is anything other than Dirty, we have made a logic
        // error.
        debug_assert!(*state_lock == Dirty,
                      "state_lock == {:?}, expected Dirty!",
                      *state_lock);
        *state_lock = Stable;

        // Wake waiting freezer threads (readers shouldn't be waiting on us).
        self.cond.notify_all();

        Ok(())
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

// Shim for converting C-style errors to io::Errors.
fn cvt(err: i64) -> io::Result<usize> {
    if err < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(err as usize)
    }
}

impl SparseFileExt for File {
    #[cfg(target_os = "linux")]
    fn punch(&self, offset: usize, size: usize) -> io::Result<()> {
        use libc::{fallocate, FALLOC_FL_PUNCH_HOLE, FALLOC_FL_KEEP_SIZE};

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

        unsafe { cvt(pread(self.as_raw_fd(),
                           buf.as_mut_ptr() as *mut ::libc::c_void,
                           buf.len(),
                           offset as i64) as i64) }
    }

    fn pwrite(&self, offset: usize, data: &[u8]) -> io::Result<usize> {
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

    use sparse::{SparseFileExt, RawSparseFile, BLOCK_SIZE,
                 VersionedSparseFile, ReadOnlySparseFile};
    use {Chunk, Version};

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
        const CHUNKS_PER_RUN: usize = 4;
        const INDEX_MULTIPLIER: usize = 20;
        const MAX_CHUNKS: usize = CHUNKS_PER_RUN * INDEX_MULTIPLIER;

        use util::test::gen_random_chunk;

        let sparse_file = RawSparseFile::new(tempfile().unwrap(),
                                             MAX_CHUNKS);
        let zeroes: &[u8] = &[0; BLOCK_SIZE];

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

                // Expect zeroes after eviction.
                sparse_file.read(chunk, write_buf).unwrap();
                assert_eq!(write_buf, zeroes);
            }
        }
    }

    #[test]
    fn test_raw_sparse_file_clone() {
        use util::test::gen_random_chunk;

        let sparse_file = RawSparseFile::new(tempfile().unwrap(), 10);
        let other = sparse_file.try_clone().unwrap();

        let buffer = gen_random_chunk(BLOCK_SIZE);
        sparse_file.write(Chunk(2), &buffer).unwrap();

        let read_buf: &mut [u8] = &mut [0; BLOCK_SIZE];
        other.read(Chunk(2), read_buf).unwrap();

        assert_eq!(&*buffer, read_buf);
    }

    #[test]
    fn test_read_only_sparse_file() {
        const MAX_CHUNKS: usize = 1024;

        use util::test::gen_random_chunk;

        let raw_sparse_file = RawSparseFile::new(tempfile().unwrap(), MAX_CHUNKS);
        let sparse_file = ReadOnlySparseFile::new(raw_sparse_file);
        let buffers = vec![gen_random_chunk(BLOCK_SIZE); MAX_CHUNKS];

        // For each chunk:
        //   - Attempt to read it and assert we end up in the reserved state.
        //   - Complete the read with a write
        //   - Read the data and ensure it is there
        //   - Evict it and confirm it is evicted
        for (index, buffer) in buffers.iter().enumerate() {
            let chunk = Chunk(index);
            let write_buf: &mut [u8] = &mut [0; BLOCK_SIZE];

            // Read and assert reserved.
            sparse_file.read(chunk, write_buf).unwrap_err()
                .assert_reserved();

            // Complete the read with a fill.
            sparse_file.fill(chunk, buffer).unwrap();

            // Read the data and ensure it is there.
            sparse_file.read(chunk, write_buf).unwrap();
            assert_eq!(&**buffer, &*write_buf);

            // Evict it.
            sparse_file.evict(chunk).unwrap();
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
        let sparse_file = &VersionedSparseFile::new(
            RawSparseFile::new(tempfile().unwrap(), MAX_CHUNKS));

        // Use a thread pool to run the tests.
        let mut pool = Pool::new(objects.len() as u32);

        pool.scoped(|scope| {
            for (index, versions) in objects.values().enumerate() {
                let chunk = Chunk(index);
                scope.execute(move || {
                    // Write all the versions of the chunk in order.
                    // After each write, confirm the version, then confirm the contents.
                    for (version, data) in versions.iter().enumerate() {
                        let buf: &mut [u8] = &mut [0; CHUNK_SIZE];

                        // Write the data.
                        sparse_file.write(chunk, data).unwrap();

                        // Check the version.
                        assert_eq!(sparse_file.version(&chunk).unwrap(),
                                   version + 1); // versions start at 1 not 0

                        // Read the data, confirm the contents.
                        sparse_file.read(chunk, buf).unwrap();
                        assert_eq!(&**data, buf);

                        // Fake flush the data.
                        let written_version = Version::new(version + 1);
                        sparse_file.complete_flush(chunk,
                                                   written_version.clone()).unwrap();

                        // Read the data after flushing, confirm the contents.
                        sparse_file.read(chunk, buf).unwrap();
                        assert_eq!(&**data, buf);

                        // Evict the chunk.
                        sparse_file.evict(chunk).unwrap();

                        // Expect the first new read to yield Reserved.
                        sparse_file.read(chunk, buf)
                            .unwrap_err().assert_reserved();

                        // Complete the Reserved read.
                        sparse_file.fill(chunk, written_version, data).unwrap();

                        // Read the data after completed read.
                        sparse_file.read(chunk, buf).unwrap();
                        assert_eq!(&**data, buf);
                    }
                });
            }
        });
    }
}

