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

pub struct Options {
    pub mount: PathBuf,
    pub size: usize,
    pub flush_threads: usize,
    pub sync_threads: usize
}

pub enum IoResult {
    Reserved(ContentId),
    Complete
}

impl<'id> LocalFs<'id> {
    pub fn new(config: Options) -> ::Result<Self> {
        let file = try!(OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(config.mount.join("data.tfs")));
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
        // TODO: Initiate the sync pool too.
        FlushPool::new(fs).run(self.config.flush_threads, scope);
        Ok(())
    }

    pub fn shutdown(&self) {
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
                    let guard = m.wait_for_read();
                    try!(self.file.read(&guard, offset, buffer));
                    Ok(None)
                }
            }
        }).ok_or(::Error::NotFound).and_then(|x| x));

        if let Some(id) = id {
            self.read_immutable(id, offset, buffer)
        } else {
            // Mutable chunk case.
            Ok(IoResult::Complete)
        }
    }

    // Read the data associated with this content id.
    //
    // Can return IoResult::Reserved if the data is not present.
    fn read_immutable(&self, id: ContentId, offset: usize, buffer: &mut [u8]) -> ::Result<IoResult> {
        // We may have to retry
        loop {
            let chunk = self.chunks.lock().unwrap().get_refresh(&id).map(|c| c.clone());

            if let Some(chunk) = chunk {
                if let Some(index) = chunk.wait_for_read() {
                    // Succesfully got a stable chunk with an index we can read from.
                    try!(self.file.read(&index, offset, buffer));
                    return Ok(IoResult::Complete);
                } // Else the chunk was evicted, so retry.
            } else {
                // We are the reserving thread!
                let mut chunks = self.chunks.lock().unwrap();
                if chunks.contains_key(&id) {
                    // Another thread beat us to reserving! Retry.
                    continue
                } else {
                    // We need to create a new chunk.
                    try!(self.evict_if_needed(&mut chunks));
                    chunks.insert(id, Arc::new(ImmutableChunk::new()));
                    return Ok(IoResult::Reserved(id));
                }
            }
        }
    }

    pub fn write_immutable(&self, id: ContentId, data: &[u8]) -> ::Result<()> {
        let chunk = self.chunks.lock().unwrap().get_refresh(&id).map(|c| c.clone())
            .expect("Logic error - chunk evicted in the Reserved state!");

        let mut index = self.file.allocate();
        try!(self.file.write(&mut index, 0, data));

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
        //       read_immutable

        let id = try!(self.on_chunk(volume, block, |chunk| {
            let mut chunk_guard = chunk.write().unwrap();

            let id = match **chunk_guard {
                Chunk::Mutable(ref mut m) => {
                    // Acquire write guard, execute write.
                    let mut guard = m.wait_for_write();
                    try!(self.file.write(&mut guard, offset, data));

                    // Transition to Dirty, get write version.
                    let version = m.complete_write(guard);

                    // TODO: Queue Sync action.
                    self.flush.push(FlushMessage::Flush(volume.clone(), block,
                                                        Version::new(version)));

                    None
                },

                Chunk::Immutable(id) => Some(id),
            };

            // Currently immutable case.
            if let Some(id) = id {
                // If the block is empty, just write and go.
                if id == ContentId::null() {
                    // Get an empty index and write our portion of the data.
                    let mut index = self.file.allocate();
                    try!(self.file.write(&mut index, offset, data));

                    let mutable = MutableChunk::dirty(index);
                    let version = mutable.version().increment() + 1;
                    **chunk_guard = Chunk::Mutable(mutable);

                    // TODO: Queue Sync action.
                    self.flush.push(FlushMessage::Flush(volume.clone(), block,
                                                        Version::new(version)));

                    Ok(None)
                } else {
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
            // Try to read the data locally.
            let mut block_data = vec![0; BLOCK_SIZE];

            match try!(self.read_immutable(id, 0, &mut block_data)) {
                // The data was already available locally.
                IoResult::Complete => {
                    // We read the data, and can now complete our write.
                    try!(self.finish_mutable_write(volume, block,
                                                   &mut block_data,
                                                   offset, data));
                    Ok(IoResult::Complete)
                },

                // The data is not available locally and must be fetched.
                IoResult::Reserved(id) => Ok(IoResult::Reserved(id))
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

        // Write data into block_data.
        (&mut block_data[offset..]).write(data).unwrap();

        // Ready to be written to the chunk.
        let data = block_data;

        // Write the data to the file.
        let mut index = self.file.allocate();
        try!(self.file.write(&mut index, 0, data));

        self.on_chunk(volume, block, |chunk| {
            match **chunk.write().unwrap() {
                // Impossible, since snapshot waits for Stable and we are Reserved.
                Chunk::Immutable(id) =>
                    panic!("Logic error! Immutable chunk found when finishing mutable write."),

                // It's a mutable chunk, fill it, setting it to Dirty.
                Chunk::Mutable(ref mut m) => {
                    let version = m.fill(index);

                    // TODO: Queue Sync action.
                    self.flush.push(FlushMessage::Flush(volume.clone(), block,
                                                        Version::new(version)));

                    Ok(())
                }
            }
        }).unwrap_or(Err(::Error::NotFound))
    }

    pub fn snapshot(&self, volume: &VolumeId) -> ::Result<Snapshot> {
        self.volumes.read().unwrap().get(volume)
            .ok_or(::Error::NotFound)
            .and_then(|volume| {
                let volume = volume.read().unwrap();

                // If the snapshotting flag was already set to true.
                if volume.snapshotting.compare_and_swap(false, true, Ordering::SeqCst) {
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

                    blocks.insert(block, id);
                }

                let snapshot = Snapshot {
                    metadata: volume.metadata,
                    blocks: blocks
                };

                // Snapshot complete, allow others to proceed.
                volume.snapshotting.store(false, Ordering::SeqCst);

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
            loop {
                // self.config.size must be > 0 so this unwrap cannot fail.
                let (id, candidate) = chunks.pop_front().unwrap();

                match candidate.begin_evict() {
                    // We are the thread evicting the block.
                    Some(Some(index)) => {
                        return self.file.deallocate(index)
                    },

                    // Another thread is evicting the block.
                    Some(None) => return Ok(()),

                    // This chunk is busy, try another.
                    None => { chunks.insert(id, candidate); }
                }
            }
        } else { Ok(()) }
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

