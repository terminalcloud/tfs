use shared_mutex::{SharedMutex, SharedMutexReadGuard, SharedMutexWriteGuard,
                   MappedSharedMutexReadGuard,
                   MappedSharedMutexWriteGuard};
use shared_mutex::monitor::{Monitor, MonitorWriteGuard};

use std::mem;

use sparse::Index;
use {ContentId, Version};

/// The in-memory state associated with each (VolumeId, BlockIndex) pair.
///
/// Each block is either immutable and therefore identified by its ContentId,
/// or mutable, in which case its state is directly available.
pub enum Chunk<'id> {
    Immutable(ContentId),
    Mutable(MutableChunk<'id>)
}

impl<'id> Chunk<'id> {
    /// Transition this Chunk into an Immutable Chunk.
    ///
    /// Many threads can race to freeze a chunk. Only one thread will succeed,
    /// and it will be given a lock containg the Index and ContentId of the chunk
    /// so it can create a new ImmutableChunk in the content id map without copying
    /// or hitting the network.
    ///
    /// The lock should only be released after the new ImmutableChunk is in the
    /// content id map in the Stable state.
    pub fn freeze<'chunk>(this: &'chunk Monitor<Self>) -> Option<FreezeGuard<'chunk, 'id>> {
        loop {
            { // Wait for the chunk to enter into a freezable state.
                let read = this.read().unwrap();
                match **read {
                    // Abort, the chunk is already immutable or another thread
                    // is currently
                    Chunk::Immutable(_) => return None,

                    Chunk::Mutable(ref m) => m.wait_for_freeze(),
                };
            } // Since we now release the read lock, we may have to wait again.

            // We now hope, but cannot be sure, that the chunk is ready to be frozen.
            let mut write = this.write().unwrap();

            // Replace the existing chunk with a sentinel so we can move out of it.
            let sentinel = Chunk::Immutable(ContentId::null());
            let current = mem::replace(&mut **write, sentinel);

            match current {
                c @ Chunk::Immutable(_) => {
                    // Write back over the sentinel and complete, another thread
                    // beat us to the punch.
                    **write = c;
                    return None
                },

                Chunk::Mutable(m) => {
                    let m_state: SharedMutex<_> = m.state.into();

                    match m_state.into_inner().unwrap() {
                        // Success! We can now freeze.
                        MutableChunkState::Stable(index, id) => {
                            // Set ourselves to an immutable chunk.
                            **write = Chunk::Immutable(id);

                            // Return a guard over this chunk, which should be held
                            // until the new ImmutableChunk is included in the content id
                            // map.
                            return Some(FreezeGuard {
                                guard: write,
                                index: Some(index),
                                id: id
                            })
                        },

                        // A writing thread interrupted us, so we must retry.
                        state => {
                            **write = Chunk::Mutable(MutableChunk {
                                version: m.version,
                                state: Monitor::new(state)
                            });
                        }
                    }
                }
            }
        }
    }

    pub fn version(&self) -> Option<usize> {
        match *self {
            Chunk::Mutable(ref m) => Some(m.version.load()),
            _ => None
        }
    }
}

/// An exclusive guard over a chunk held while creating a new
/// ImmutableChunk entry for it when freezing.
pub struct FreezeGuard<'chunk, 'id: 'chunk> {
    guard: MonitorWriteGuard<'chunk, Chunk<'id>>,
    index: Option<Index<'id>>,
    id: ContentId
}

impl<'chunk, 'id> FreezeGuard<'chunk, 'id> {
    /// Move the Index out of the FreezeGuard for use.
    ///
    /// WARNING: Panics if called twice on the same FreezeGuard.
    pub fn take_index(&mut self) -> Index<'id> {
        self.index.take().unwrap()
    }

    /// Read the ContentId of the index.
    pub fn id(&self) -> ContentId { self.id }
}

/// The in-memory state associated with a mutable chunk.
///
/// Besides the state specified and documented in MutableChunkState,
/// a MutableChunk also has a Version associated with it. The version is used
/// as a cross-thread synchronizaton mechanism called a sequence number; all
/// write actions increment the version, and all read actions retry if the version
/// number has changed since they began.
///
/// The version is also used to identify a particular write to the chunk to the
/// flushing threads, which can check if the chunk has changed since the flush
/// was requested, and can cancel the flush if it has.
pub struct MutableChunk<'id> {
    version: Version,
    state: Monitor<MutableChunkState<'id>>
}

impl<'id> MutableChunk<'id> {
    /// Create a new MutableChunk metadata in the Reserved state.
    ///
    /// Should be called *before* allocating an Index and acquiring the data
    /// needed for this MutableChunk.
    pub fn new(id: ContentId) -> Self {
        MutableChunk {
            version: Version::new(0),
            state: Monitor::new(MutableChunkState::Reserved(id))
        }
    }

    /// Create a new MutableChunk from a diry index, in the Dirty state.
    pub fn dirty(index: Index<'id>) -> Self {
        MutableChunk {
            version: Version::new(0),
            state: Monitor::new(MutableChunkState::Dirty(index))
        }
    }

    pub fn version(&self) -> &Version { &self.version }

    pub fn fill(&self, index: Index<'id>) -> usize {
        let mut lock = self.state.write().unwrap();

        // Transition from Reserved => Dirty
        **lock = match **lock {
            MutableChunkState::Reserved(_) => MutableChunkState::Dirty(index),
            ref state @ _ => panic!("Logic error! Wrong state on mutable chunk fill: {:?}", state)
        };

        // Notify pending writers who are waiting for a transition out of the
        // Reserved state.
        self.state.notify_all();

        // Increment the version (always from 0 to 1)
        self.version.increment()
    }

    pub fn wait_for_write(&self) -> MappedSharedMutexWriteGuard<Index<'id>> {
        let mut lock: SharedMutexWriteGuard<_> = self.state.write().unwrap().into();

        loop {
            match lock.into_mapped().result_map(|state| {
                match *state {
                    MutableChunkState::Dirty(ref mut index) |
                        MutableChunkState::Stable(ref mut index, _) => Ok(index),
                    MutableChunkState::Reserved(_) => Err(())
                }
            }) {
                Ok(lock) => return lock,
                Err((l, ())) =>
                    lock = l.recover(&self.state.as_ref()).unwrap()
                        .wait_for_write(self.state.cond()).unwrap()
            }
        }
    }

    pub fn complete_write(&self, m: MappedSharedMutexWriteGuard<Index<'id>>) -> usize {
        let mut lock = m.recover(&self.state.as_ref()).unwrap();

        let sentinel = MutableChunkState::Reserved(ContentId::null());
        let state = mem::replace(&mut *lock, sentinel);

        *lock = match state {
            state @ MutableChunkState::Dirty(_) => state,
            MutableChunkState::Stable(index, _) => MutableChunkState::Dirty(index),
            MutableChunkState::Reserved(_) =>
                panic!("Logic error! Reserved state observed during complete_write!")
        };

        self.version().increment()
    }

    pub fn wait_for_read(&self) -> MappedSharedMutexReadGuard<Index<'id>> {
        let mut lock: SharedMutexReadGuard<_> = self.state.read().unwrap().into();

        loop {
            match lock.into_mapped().result_map(|state| {
                match *state {
                    MutableChunkState::Dirty(ref index) |
                        MutableChunkState::Stable(ref index, _) => Ok(index),
                    MutableChunkState::Reserved(_) => Err(())
                }
            }) {
                Ok(lock) => return lock,
                Err((l, ())) =>
                    lock = l.recover(&self.state.as_ref()).unwrap()
                        .wait_for_read(self.state.cond()).unwrap()
            }
        }
    }

    /// Wait for the chunk to become Stable as part of a snapshot.
    pub fn wait_for_freeze(&self) -> MappedSharedMutexReadGuard<MutableChunkState<'id>> {
        let mut lock: SharedMutexReadGuard<_> = self.state.read().unwrap().into();

        loop {
            match lock.into_mapped().result_map(|state| {
                match *state {
                    MutableChunkState::Dirty(_) => Err(()),
                    MutableChunkState::Reserved(_) => Err(()),
                    ref state @ MutableChunkState::Stable(..) => Ok(state)
                }
            }) {
                // Stable
                Ok(lock) => return lock,

                // Dirty
                Err((l, _)) =>
                    lock = l.recover(&self.state.as_ref()).unwrap()
                        .wait_for_read(self.state.cond()).unwrap()
            }
        }
    }
}

/// The states of a MutableChunk
///
/// A MutableChunk begins in the Reserved state, and remains there while the thread
/// creating it writes the initial data in the chunk to a backing IndexedSparseFile,
/// after which it transitions to the Dirty state.
///
/// Threads other than the creating thread observing a Reserved state have two choices:
/// if they are reading threads, they may attempt to look up an ImmutableChunk with the
/// given ContentId and read from that; if they are writing threads, they should wait
/// for a state transition to Dirty or Stable, then execute their write.
///
/// When the flush action associated with the latest write to this chunk completes,
/// the flushing thread will transition the state to Stable, filling in its ContentId.
///
/// Any write to the chunk should transition the state to Dirty and queue a flush action
/// for the chunk, so it can eventually transition to Stable.
///
/// Threads observing the chunk in either the Dirty or Stable states may read and
/// write the chunk.
#[derive(Debug, PartialEq)]
pub enum MutableChunkState<'id> {
    Reserved(ContentId),
    Dirty(Index<'id>),
    Stable(Index<'id>, ContentId)
}

/// The in-memory state associated with an immutable chunk.
pub struct ImmutableChunk<'id> {
    state: Monitor<ImmutableChunkState<'id>>
}

impl<'id> ImmutableChunk<'id> {
    /// Create a new ImmutableChunk, beginning in the Reserved state.
    pub fn new() -> ImmutableChunk<'id> {
        ImmutableChunk {
            state: Monitor::new(ImmutableChunkState::Reserved)
        }
    }

    /// Create a new ImmutableChunk in the Stable state.
    ///
    /// Used to transition an Index from mutable to immutable.
    pub fn from_mutable(index: Index<'id>) -> ImmutableChunk<'id> {
        ImmutableChunk {
            state: Monitor::new(ImmutableChunkState::Stable(index))
        }
    }

    /// Indicate the chunk has been fetched and written at the given index.
    ///
    /// Transitions to the Stable state from the Reserved state.
    ///
    /// May *only* be called by the thread which initialized the chunk in
    /// the Reserved state.
    pub fn complete_fill(&self, index: Index<'id>) {
        use self::ImmutableChunkState::*;

        let mut lock = self.state.write().unwrap();

        match **lock {
            Reserved => **lock = Stable(index),
            Stable(_) => panic!("Logic error - complete_fill called on Stable immutable chunk!"),
            Evicted => panic!("Logic error - complete_fill called on Evicted immutable chunk!")
        };

        // Notify pending readers that the data is here.
        self.state.notify_all();
    }

    /// Wait for the chunk to be readable.
    ///
    /// Returns a lock on the Index associated with this chunk, which allows
    /// reading from the containing IndexedSparseFile.
    pub fn wait_for_read(&self) -> Option<MappedSharedMutexReadGuard<Index<'id>>> {
        let mut lock: SharedMutexReadGuard<_> = self.state.read().unwrap().into();

        loop {
            match lock.into_mapped().result_map(|state| {
                match *state {
                    ImmutableChunkState::Stable(ref index) => Ok(index),
                    ImmutableChunkState::Reserved => Err(true),
                    ImmutableChunkState::Evicted => Err(false)
                }
            }) {
                // Stable
                Ok(lock) => return Some(lock),

                // Evicted
                Err((_, false)) => return None,

                // Reserved
                Err((l, true)) =>
                    lock = l.recover(&self.state.as_ref()).unwrap()
                        .wait_for_read(self.state.cond()).unwrap()
            }
        }
    }

    /// Attempt to evict this chunk.
    ///
    /// Many threads may race to attempt an eviction, only one will succeed
    /// and receive the Index, which it then must deallocate from the
    /// IndexedSparseFile.
    ///
    /// Here is a small table describing how threads should respond to return values:
    ///   - `Some(Some(index))`: Use the index to proceed with the eviction.
    ///   - `Some(None)`: The chunk is being evicted by another thread, proceed.
    ///   - `None`: This chunk should not be evicted, try another.
    pub fn begin_evict(&self) -> Option<Option<Index<'id>>> {
        use self::ImmutableChunkState::*;

        let state: &SharedMutex<_> = self.state.as_ref();
        state.try_write().ok().and_then(|mut state| {
            match mem::replace(&mut *state, Evicted) {
                // We won the race to evict!
                Stable(index) => Some(Some(index)),

                // We should not evict Reserved chunks.
                Reserved => {
                    *state = Reserved;
                    None
                },

                // Already evicted.
                Evicted => Some(None),
            }
        })
    }
}

/// The states of an ImmutableChunk
///
/// An ImmutableChunk begins in the Reserved state and remains there while
/// the thread which created it completes its read from a cache. Threads
/// observing a Reserved state should wait for a transition to occur.
///
/// When the read completes and the data is written to disk, the creating
/// thread will transition the state to Stable and wake other threads. Threads
/// observing a Stable state can complete their read on the chunk by reading
/// the stored Index.
///
/// When a chunk is evicted, its entry will be removed from the immutable chunk
/// table, and its state will be transitioned to Evicted. Threads observing an
/// Evicted state should release their handle to the chunk and retry their read,
/// allocating a new entry in the immutable chunk table and restarting the cycle.
///
/// Only the following state transitions are allowed:
///   - Reserved -> Stable
///   - Stable -> Evicted
///
/// (But any chain of the above transitions may be observed to happen atomically)
#[derive(Debug, PartialEq)]
pub enum ImmutableChunkState<'id> {
    Reserved,
    Evicted,
    Stable(Index<'id>)
}

