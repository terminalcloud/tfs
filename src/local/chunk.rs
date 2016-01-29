use signal::Signal;
use sparse::Index;

pub struct ChunkMap<'id>(&'id ());

pub struct ImmutableChunk<'id> {
    state: Signal<ImmutableChunkState<'id>>
}

impl<'id> ImmutableChunk<'id> {
    pub fn new() -> ImmutableChunk<'id> {
        ImmutableChunk {
            state: Signal::new(ImmutableChunkState::Reserved)
        }
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
/// obsering a Stable state can complete their read on the chunk by reading
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
enum ImmutableChunkState<'id> {
    Reserved,
    Evicted,
    Stable(Index<'id>)
}

