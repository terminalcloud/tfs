use rwlock2::{RwLock, RwLockReadGuard};
use crossbeam::sync::MsQueue;
use terminal_linked_hash_map::LinkedHashMap;
use scoped_pool::Scope;

use std::fs::{File, OpenOptions};
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::path::PathBuf;

use local::flush::{FlushMessage, FlushPool};
use local::chunk::{ChunkMap, ImmutableChunk};

use util::RwLockExt;
use sparse::{IndexedSparseFile, Index};
use fs::Fs;
use {Storage, Cache, VolumeMetadata, VolumeId, ContentId, BlockIndex};

mod flush;
mod chunk;

pub struct LocalFs<'id> {
    mount: PathBuf,
    file: IndexedSparseFile<'id>,

    volumes: RwLock<HashMap<VolumeId, RwLock<ChunkMap<'id>>>>,
    chunks: Mutex<LinkedHashMap<ContentId, Arc<ImmutableChunk<'id>>>>,

    flush: MsQueue<FlushMessage>
}

impl<'id> LocalFs<'id> {
    pub fn new(mount: PathBuf, size: usize) -> ::Result<Self> {
       let file = try!(OpenOptions::new()
           .read(true)
           .write(true)
           .create(true)
           .open(mount.join("data.tfs")));
       // let indexed = IndexedSparseFile::new(file, size);

       //  Ok(LocalFs {
       //      mount: mount,
       //      file: indexed,
       //      flush: MsQueue::new(),
       //      pool: AtomicOption::empty(),
       //  })

        unimplemented!()
    }

    pub fn create(&self, volume: &VolumeId, metadata: VolumeMetadata) -> ::Result<VolumeId> {
        //self.new_blob(file, metadata, |f, m| Blob::new(f, m.size))
        unimplemented!()
    }

    pub fn version(&self, volume: &VolumeId, block: BlockIndex) -> Option<usize> {
        // self.files.read().unwrap().get(&chunk.file)
        //     .and_then(|blob| {
        //         match *blob.read().unwrap() {
        //             Blob::ReadOnly(_) => None,
        //             Blob::ReadWrite(ref v) => v.version(&chunk.chunk)
        //         }
        //     })
        unimplemented!()
    }

    pub fn init<'fs>(&self, fs: &'fs Fs<'id>, scope: &Scope<'fs>) -> ::Result<()> {
        // TODO: Make size of pool configurable.
        let flush_pool = FlushPool::new(fs);
        flush_pool.run(12, scope);
        Ok(())
    }

    pub fn try_write_immutable_chunk(&self, id: ContentId, data: &[u8]) -> ::Result<()> {
        // // Write the data locally, filling a previously unfilled chunk.
        // let blob = try!(self.get_blob(&chunk.file));
        // let blob_guard = blob.read().unwrap();
        // blob_guard.fill(chunk.chunk, version, data)
        unimplemented!()
    }

    pub fn try_write_mutable_chunk(&self, volume: &VolumeId, block: BlockIndex,
                                   data: &[u8]) -> ::Result<()> {
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
}

impl<'id> Cache for LocalFs<'id> {
    fn read(&self, id: ContentId, buf: &mut [u8]) -> ::Result<()> {
        // self.get_blob(&chunk.file)
        //    .and_then(|blob| blob.read().unwrap().read(chunk.chunk, buf))
        unimplemented!()
    }
}

