use shared_mutex::monitor::Monitor;
use crossbeam::sync::MsQueue;
use terminal_linked_hash_map::LinkedHashMap;
use scoped_pool::Scope;

use std::fs::OpenOptions;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock, Mutex, MutexGuard};
use std::io::Write;
use std::path::PathBuf;

use local::flush::{FlushMessage, FlushPool};
use local::chunk::{Chunk, MutableChunk, ImmutableChunk};

use util::RwLockExt;
use sparse::{IndexedSparseFile, BLOCK_SIZE};
use fs::Fs;
use {Storage, Cache, VolumeMetadata, VolumeName, VolumeId, Version, ContentId,
     BlockIndex, Snapshot};

mod flush;
mod chunk;

type SharedMap<K, V> = RwLock<HashMap<K, V>>;

struct SyncMessage;

pub struct LocalFs<'id> {
    config: Options,
    file: IndexedSparseFile<'id>,

    names: SharedMap<VolumeName, VolumeId>,
    volumes: SharedMap<VolumeId, RwLock<Volume<'id>>>,
    chunks: Mutex<ImmutableChunkMap<'id>>,

    flush: MsQueue<FlushMessage>,
    sync: MsQueue<SyncMessage>
}

type ImmutableChunkMap<'id> = LinkedHashMap<ContentId, Arc<ImmutableChunk<'id>>>;

#[derive(Debug)]
pub struct Options {
    pub mount: PathBuf,
    pub size: usize,
    pub flush_threads: usize,
    pub sync_threads: usize
}

#[derive(Debug)]
pub enum IoResult {
    Reserved(ContentId),
    Complete
}

impl<'id> LocalFs<'id> {
    pub fn new(config: Options) -> ::Result<Self> {
        debug!("Creating new LocalFs with: config={:?}", config);

        let data_path = config.mount.join("data.tfs");
        let file = try!(OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&data_path));
        let indexed = IndexedSparseFile::new(file, config.size);

        Ok(LocalFs {
            config: config,
            file: indexed,

            volumes: RwLock::new(HashMap::new()),
            names: RwLock::new(HashMap::new()),
            chunks: Mutex::new(LinkedHashMap::new()),

            flush: MsQueue::new(),
            sync: MsQueue::new()
        })
    }

    pub fn create(&self, volume: VolumeName,
                  metadata: VolumeMetadata) -> ::Result<VolumeId> {
        // If there is no id for this volume generate one and keep track of it.
        self.names.if_then(|names| !names.contains_key(&volume),
                           |names| names.insert(volume.clone(),
                                                VolumeId::new()));

        // Load the id for this name.
        let id = *self.names.read().unwrap().get(&volume).unwrap();

        // Just insert, since we "know" all ids are unique.
        self.volumes.write().unwrap()
            .insert(id, RwLock::new(Volume::new(volume, metadata)));

        Ok(id)
    }

    pub fn open(&self, volume: VolumeName, snapshot: Snapshot) -> ::Result<VolumeId> {
        let id = VolumeId::new();

        // Just create a new anonymous volume here.
        self.volumes.write().unwrap()
            .insert(id, RwLock::new(Volume::open(volume, snapshot)));

        Ok(id)
    }

    pub fn version(&self, volume: &VolumeId, block: BlockIndex) -> Option<usize> {
        self.on_chunk(volume, block, |c| c.read().unwrap().version()).and_then(|v| v)
    }

    pub fn init<'fs>(&self, fs: &'fs Fs<'id>, scope: &Scope<'fs>) -> ::Result<()> {
        debug!("Initializing local fs worker threads.");
        // TODO: Initiate the sync pool too.
        FlushPool::new(fs).run(self.config.flush_threads, scope);
        Ok(())
    }

    pub fn shutdown(&self) {
        debug!("Shutting down local fs worker threads.");
        self.flush.push(FlushMessage::Quit);
    }

    pub fn read(&self, volume: &VolumeId, block: BlockIndex, offset: usize, buffer: &mut [u8]) -> ::Result<IoResult> {
        let id = try!(self.on_chunk(volume, block, |chunk| {
            match **chunk.read().unwrap() {
                // If it's an immutable chunk, extract the id for later use.
                Chunk::Immutable(id) => {
                    Ok(Some(id))
                },

                // If it's a mutable chunk, just do the read and we're done!
                Chunk::Mutable(ref m) => {
                    debug!("Found mutable chunk when reading, waiting.");
                    let guard = m.wait_for_read();
                    debug!("Got reading lock on mutable chunk, doing read.");
                    try!(self.file.read(&guard, offset, buffer));
                    Ok(None)
                }
            }
        }).ok_or(::Error::NotFound).and_then(|x| x));

        if let Some(id) = id {
            debug!("Reading immutable chunk with: id={:?}", id);
            self.read_immutable(id, offset, buffer)
        } else {
            // Mutable chunk case.
            debug!("Read of mutable chunk complete.");
            Ok(IoResult::Complete)
        }
    }

    // Read the data associated with this content id.
    //
    // Can return IoResult::Reserved if the data is not present.
    fn read_immutable(&self, id: ContentId, offset: usize, buffer: &mut [u8]) -> ::Result<IoResult> {
        // We may have to retry
        debug!("Reading immutable chunk with: id={:?}, offset={:?}", id, offset);
        loop {
            debug!("Trying immutable read.");
            let chunk = self.chunks.lock().unwrap().get_refresh(&id).map(|c| c.clone());

            if let Some(chunk) = chunk {
                debug!("Found immutable chunk for id={:?}, waiting for read.", id);
                if let Some(index) = chunk.wait_for_read() {
                    debug!("Got a stable immutable chunk we can read from at index={:?}.", index);
                    // Succesfully got a stable chunk with an index we can read from.
                    try!(self.file.read(&index, offset, buffer));
                    debug!("Completed immutable read of id={:?}.", id);
                    return Ok(IoResult::Complete);
                } // Else the chunk was evicted, so retry.
            } else {
                debug!("Chunk with id={:?} not present, reserving.", id);
                // We are the reserving thread!
                let mut chunks = self.chunks.lock().unwrap();
                if chunks.contains_key(&id) {
                    debug!("Another reserving thread beat us to reserve id={:?}", id);
                    // Another thread beat us to reserving! Retry.
                    continue
                } else {
                    debug!("Creating and inserting new reserved immutable chunk.");
                    // We need to create a new chunk.
                    try!(self.evict_if_needed(&mut chunks));
                    chunks.insert(id, Arc::new(ImmutableChunk::new()));
                    return Ok(IoResult::Reserved(id));
                }
            }
        }
    }

    // Attempt to read an immutable chunk. If it's not there, do not reserve.
    //
    // Returns IoResult::Reserved if the chunk is not present, but does not actually reserve.
    fn read_immutable_no_reserve(&self, id: ContentId, offset: usize, buffer: &mut [u8]) -> ::Result<IoResult> {
        loop {
            let chunk = self.chunks.lock().unwrap().get_refresh(&id).map(|c| c.clone());

            if let Some(chunk) = chunk {
                if let Some(index) = chunk.wait_for_read() {
                    try!(self.file.read(&index, offset, buffer));
                    return Ok(IoResult::Complete)
                } else {
                    // Chunk evicted, retry.
                    continue
                }
            } else {
                return Ok(IoResult::Reserved(id))
            }
        }
    }

    pub fn write_immutable(&self, id: ContentId, data: &[u8]) -> ::Result<()> {
        debug!("Completing an earlier reservation on immutable chunk with: id={:?}", id);
        let chunk = self.chunks.lock().unwrap().get_refresh(&id).map(|c| c.clone())
            .expect("Logic error - chunk evicted in the Reserved state!");

        debug!("Found chunk, allocating space and writing to file.");
        let mut index = self.file.allocate();
        try!(self.file.write(&mut index, 0, data));

        debug!("Wrote to index={:?} for id={:?}, completing fill on chunk.", index, id);
        chunk.complete_fill(index);

        Ok(())
    }

    pub fn write_mutable(&self, volume: &VolumeId, block: BlockIndex,
                         offset: usize, data: &[u8]) -> ::Result<IoResult> {
        // Two major cases:
        //   - mutable chunk already present, just write to it and mark Dirty
        //   - block is currently an immutable chunk
        //     - create new reserved MutableChunk
        //     - throw reserved error, resume at finish_mutable_write after
        //       read_immutable_no_reserve

        debug!("Writing to mutable chunk {:?}/{:?} at offset={:?}",
               volume, block, offset);

        let id = try!(self.on_chunk(volume, block, |chunk| {
            let mut chunk_guard = chunk.write().unwrap();

            let id = match **chunk_guard {
                Chunk::Mutable(ref mut m) => {
                    debug!("Found mutable chunk, waiting to write to it.");

                    // Acquire write guard, execute write.
                    let mut guard = m.wait_for_write();
                    debug!("Awaited write to mutable chunk, got index={:?}", &*guard);
                    try!(self.file.write(&mut guard, offset, data));

                    // Transition to Dirty, get write version.
                    debug!("Finished write, marking chunk dirty.");
                    let version = m.complete_write(guard);

                    // TODO: Queue Sync action.
                    let flush = FlushMessage::Flush(volume.clone(), block,
                                                    Version::new(version));
                    debug!("Queueing flush action: {:?}", flush);
                    self.flush.push(flush);

                    None
                },

                Chunk::Immutable(id) => Some(id),
            };

            // Currently immutable case.
            if let Some(id) = id {
                debug!("Found immutable chunk with id={:?}, transitioning", id);
                // If the block is empty, just write and go.
                if id == ContentId::null() {
                    debug!("Empty chunk found. Allocating new chunk and writing.");
                    // Get an empty index and write our portion of the data.
                    let mut index = self.file.allocate();
                    try!(self.file.write(&mut index, offset, data));

                    debug!("Wrote new mutable chunk to index={:?}", index);
                    let mutable = MutableChunk::dirty(index);
                    let version = mutable.version().increment() + 1;
                    **chunk_guard = Chunk::Mutable(mutable);

                    // TODO: Queue Sync action.
                    let flush = FlushMessage::Flush(volume.clone(), block,
                                                    Version::new(version));
                    debug!("Queueing flush action: {:?}", flush);
                    self.flush.push(flush);

                    Ok(None)
                } else {
                    debug!("Non-empty chunk found, reserving new mutable chunk.");
                    // Set the chunk to mutable and reserved
                    **chunk_guard = Chunk::Mutable(MutableChunk::new(id));

                    Ok(Some(id))
                }
            // Already mutable case.
            } else {
                Ok(None)
            }
        }).ok_or(::Error::NotFound).and_then(|x| x));

        // Just set to reserved case.
        if let Some(id) = id {
            debug!("Mutable chunk reserved earlier, trying to fill from local chunk.");
            // Try to read the data locally.
            let mut block_data = vec![0; BLOCK_SIZE];

            match try!(self.read_immutable_no_reserve(id, 0, &mut block_data)) {
                // The data was already available locally.
                IoResult::Complete => {
                    debug!("Found data locally, writing back to mutable chunk.");
                    // We read the data, and can now complete our write.
                    try!(self.finish_mutable_write(volume, block,
                                                   &mut block_data,
                                                   offset, data));
                    debug!("Completed mutable chunk reservation and write.");
                    Ok(IoResult::Complete)
                },

                // The data is not available locally and must be fetched.
                IoResult::Reserved(id) => {
                    debug!("Couldn't find data for id={:?} locally, leaving reserved.", id);
                    Ok(IoResult::Reserved(id))
                }
            }
        // Mutable chunk case, we are done.
        } else {
            Ok(IoResult::Complete)
        }
    }

    pub fn finish_mutable_write(&self, volume: &VolumeId, block: BlockIndex,
                                block_data: &mut [u8], offset: usize, data: &[u8]) -> ::Result<()> {
        assert!(offset < block_data.len(),
                "offset greater than block size: {:?} > {:?}", offset, block_data.len());
        assert!(data.len() <= block_data.len() - offset,
                "requested write larger than block size - offset: {:?} > {:?}",
                data.len(), block_data.len() - offset);

        debug!("Filling mutable reserved chunk {:?}/{:?}", volume, block);

        // Write data into block_data.
        (&mut block_data[offset..]).write(data).unwrap();

        // Ready to be written to the chunk.
        let data = block_data;

        // Write the data to the file.
        let mut index = self.file.allocate();
        debug!("Writing data to file at index={:?}", index);
        try!(self.file.write(&mut index, 0, data));

        self.on_chunk(volume, block, |chunk| {
            match **chunk.write().unwrap() {
                // Impossible, since snapshot waits for Stable and we are Reserved.
                Chunk::Immutable(_) =>
                    panic!("Logic error! Immutable chunk found when finishing mutable write."),

                // It's a mutable chunk, fill it, setting it to Dirty.
                Chunk::Mutable(ref mut m) => {
                    let version = m.fill(index);

                    // TODO: Queue Sync action.
                    let flush = FlushMessage::Flush(volume.clone(), block,
                                                    Version::new(version));
                    debug!("Queueing flush action: {:?}", flush);
                    self.flush.push(flush);

                    Ok(())
                }
            }
        }).unwrap_or(Err(::Error::NotFound))
    }

    pub fn snapshot(&self, vol_id: &VolumeId) -> ::Result<Snapshot> {
        debug!("Snapshotting volume: {:?}", vol_id);
        self.volumes.read().unwrap().get(vol_id)
            .ok_or(::Error::NotFound)
            .and_then(|volume| {
                let volume = volume.read().unwrap();

                // If the snapshotting flag was already set to true.
                if volume.snapshotting.compare_and_swap(false, true, Ordering::SeqCst) {
                    debug!("Concurrent snapshot on volume: {:?}!", vol_id);
                    return Err(::Error::ConcurrentSnapshot)
                }

                let mut blocks = HashMap::new();

                // We are now the sole thread doing a snapshot.
                for (&block, chunk) in volume.blocks.iter() {
                    let id = match Chunk::freeze(&chunk) {
                        // Chunk already frozen.
                        Err(id) => id,

                        Ok(mut freeze_guard) => {
                            let mut chunks = self.chunks.lock().unwrap();
                            let id = freeze_guard.id();

                            try!(self.evict_if_needed(&mut chunks));
                            let new_chunk = ImmutableChunk::from_mutable(freeze_guard.take_index());
                            chunks.insert(id, Arc::new(new_chunk));

                            id
                        }
                    };

                    // Don't record null content ids.
                    if id == ContentId::null() { continue }

                    debug!("Recording {:?} = {:?} in snapshot of {:?}.",
                           block, id, vol_id);
                    blocks.insert(block, id);
                }

                let snapshot = Snapshot {
                    metadata: volume.metadata,
                    blocks: blocks
                };

                // Snapshot complete, allow others to proceed.
                volume.snapshotting.store(false, Ordering::SeqCst);

                trace!("Snapshot of {:?} complete: {:#?}", vol_id, snapshot);
                Ok(snapshot)
            })
    }

    pub fn complete_flush(&self, volume: &VolumeId, block: BlockIndex,
                          id: ContentId, version: Version) -> ::Result<()> {
        self.on_chunk(volume, block, |chunk| {
            if let Chunk::Mutable(ref m) = **chunk.read().unwrap() {
                m.complete_flush(id, version)
            } else {
                Ok(())
            }
        }).unwrap_or(Ok(()))
    }

    fn on_chunk<F, R>(&self, volume: &VolumeId, block: BlockIndex, cb: F) -> Option<R>
    where F: FnOnce(&Monitor<Chunk<'id>>) -> R {
        self.volumes.read().unwrap().get(volume).and_then(|volume| {
            volume.read().unwrap().blocks.get(&block).map(cb)
        })
    }

    fn evict_if_needed(&self, mut chunks: &mut MutexGuard<ImmutableChunkMap<'id>>) -> ::Result<()> {
        if chunks.len() == self.config.size {
            debug!("Chunks full! Evicting.");
            loop {
                // self.config.size must be > 0 so this unwrap cannot fail.
                let (id, candidate) = chunks.pop_front().unwrap();
                debug!("Found eviction candidate with: id={:?}", id);

                match candidate.begin_evict() {
                    // We are the thread evicting the block.
                    Some(Some(index)) => {
                        debug!("Evicting block at index={:?}", index);
                        return self.file.deallocate(index)
                    },

                    // Another thread is evicting the block.
                    Some(None) => return Ok(()),

                    // This chunk is busy, try another.
                    None => {
                        debug!("Eviction candidate {:?} is busy, trying another.", id);
                        chunks.insert(id, candidate);
                    }
                }
            }
        } else {
            trace!("Chunks not full, no eviction needed.");
            Ok(())
        }
    }
}

struct Volume<'id> {
    blocks: HashMap<BlockIndex, Monitor<Chunk<'id>>>,
    metadata: VolumeMetadata,
    name: VolumeName,

    // Is this volume currently being snapshotted?
    snapshotting: AtomicBool
}

impl<'id> Volume<'id> {
    fn new(name: VolumeName, metadata: VolumeMetadata) -> Self {
        let blocks = (0..metadata.size).map(|block_index| {
            (BlockIndex(block_index), Monitor::new(Chunk::Immutable(ContentId::null())))
        }).collect();

        Volume {
            blocks: blocks,
            metadata: metadata,
            name: name,
            snapshotting: AtomicBool::new(false)
        }
    }

    fn open(name: VolumeName, mut snapshot: Snapshot) -> Self {
        let blocks = (0..snapshot.metadata.size).map(|block_index| {
            let block = BlockIndex(block_index);
            let id = snapshot.blocks.remove(&block)
                .unwrap_or_else(|| ContentId::null());
            let chunk = Monitor::new(Chunk::Immutable(id));

            (block, chunk)
        }).collect();

        Volume {
            blocks: blocks,
            metadata: snapshot.metadata,
            name: name,
            snapshotting: AtomicBool::new(false)
        }
    }
}

