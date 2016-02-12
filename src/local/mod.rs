use shared_mutex::monitor::Monitor;
use crossbeam::sync::MsQueue;
use terminal_linked_hash_map::LinkedHashMap;
use scoped_pool::Scope;
use uuid::Uuid;

use std::fs::OpenOptions;
use std::collections::HashMap;
use std::sync::{Arc, RwLock, Mutex, MutexGuard};
use std::path::PathBuf;

use local::flush::{FlushMessage, FlushPool};
use local::chunk::{Chunk, MutableChunk, ImmutableChunk};

use util::RwLockExt;
use sparse::IndexedSparseFile;
use fs::Fs;
use {Storage, Cache, VolumeMetadata, VolumeName, VolumeId, ContentId, BlockIndex};

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
    pub size: usize
}

pub enum ReadResult {
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
        self.names.if_then(|names| names.contains_key(&volume),
                           |names| names.insert(volume.clone(),
                                                VolumeId(Uuid::new_v4())));

        // Load the id for this name.
        let id = *self.names.read().unwrap().get(&volume).unwrap();

        // Just insert, since we "know" all ids are unique.
        self.volumes.write().unwrap()
            .insert(id, RwLock::new(Volume::new(volume, metadata)));

        Ok(id)
    }

    pub fn version(&self, volume: &VolumeId, block: BlockIndex) -> Option<usize> {
        self.on_chunk(volume, block, |c| c.read().unwrap().version()).and_then(|v| v)
    }

    pub fn init<'fs>(&self, fs: &'fs Fs<'id>, scope: &Scope<'fs>) -> ::Result<()> {
        // TODO: Make size of pool configurable.
        // TODO: Initiate the sync pool too.
        let flush_pool = FlushPool::new(fs);
        flush_pool.run(12, scope);
        Ok(())
    }

    pub fn read(&self, volume: &VolumeId, block: BlockIndex, offset: usize, buffer: &mut [u8]) -> ::Result<ReadResult> {
        let id = try!(self.on_chunk(volume, block, |chunk| {
            match **chunk.read().unwrap() {
                // If it's an immutable chunk, extract the id for later use.
                Chunk::Immutable(id) => {
                    Ok(Some(id))
                },

                // If it's a mutable chunk, just do the read and we're done!
                Chunk::Mutable(ref m) => {
                    let guard = m.lock();
                    try!(self.file.read(&guard.0, offset, buffer));
                    Ok(None)
                }
            }
        }).ok_or(::Error::NotFound).and_then(|x| x));

        if let Some(id) = id {
            // Immutable chunk case.
            //
            // We might have to retry our read.
            loop {
                let chunk = self.chunks.lock().unwrap().get_refresh(&id).map(|c| c.clone());

                if let Some(chunk) = chunk {
                    if let Some(index) = chunk.wait_for_read() {
                        // Succesfully got a stable chunk with an index we can read from.
                        try!(self.file.read(&index, offset, buffer));
                        return Ok(ReadResult::Complete);
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
                        return Ok(ReadResult::Reserved(id));
                    }
                }
            }
        } else {
            // Mutable chunk case.
            Ok(ReadResult::Complete)
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
                         offset: usize, data: &[u8]) -> ::Result<()> {
        // Two major cases:
        //   - mutable chunk already present, just write to it and mark Dirty
        //   - block is currently an immutable chunk

        // // Write locally:
        // //   - transactionally/atomically:
        // //   - bump the version

        // // Queue new versioned chunk to be flushed. When done, unpin.
        // let blob = try!(self.get_blob(&chunk.file));
        // let mut blob_guard = blob.write().unwrap();
        // let version = try!(blob_guard.assert_versioned_mut()
        //     .write(chunk.chunk, data));

        // // Queue the new versioned chunk to be flushed.
        // self.flush.push(FlushMessage::Flush(chunk.clone(),
        //                                     Version::new(version)));

        // Ok(())
        unimplemented!()
    }

    pub fn snapshot(&self, volume: &VolumeId) -> ::Result<VolumeMetadata> {
        // let blob = try!(self.get_blob(file));
        // Blob::freeze(&*blob)
        unimplemented!()
    }

    pub fn complete_flush(&self, volume: &VolumeId, block: BlockIndex,
                          id: ContentId) -> ::Result<()> {
        // let blob = try!(self.get_blob(&chunk.file));
        // let blob_guard = blob.read().unwrap();

        // if let Some(rw) = blob_guard.as_read_write() {
        //     rw.complete_flush(chunk.chunk, version)
        // } else {
        //     // The blob has already become read only.
        //     Ok(())
        // }
        unimplemented!()
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
    name: VolumeName
}

impl<'id> Volume<'id> {
    fn new(name: VolumeName, metadata: VolumeMetadata) -> Self {
        Volume {
            blocks: HashMap::new(),
            metadata: metadata,
            name: name
        }
    }
}

