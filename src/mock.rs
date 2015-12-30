use std::collections::HashMap;
use std::sync::RwLock;
use std::io::{self, Write};
use std::mem;

use {Storage, Cache, ChunkDescriptor, Version};

pub struct MockStorage {
    inner: RwLock<InnerMockStorage>
}

struct InnerMockStorage {
    storage: HashMap<ChunkDescriptor, HashMap<Option<usize>, Vec<u8>>>
}

impl InnerMockStorage {
    fn read(&self, chunk: &ChunkDescriptor, version: Option<Version>,
            mut buf: &mut [u8]) -> io::Result<usize> {
        self.storage
            .get(chunk)
            .and_then(|chunk_map| chunk_map.get(&version.map(|v| v.load())))
            .ok_or_else(|| panic!()) // TODO: Make notfound
            .and_then(|object| {
                // TODO: Maybe be more flexible here, or just
                // always ensure this is the case.
                assert_eq!(object.len(), buf.len());
                buf.write(&object)
            })
    }

    fn create(&mut self, chunk: &ChunkDescriptor, version: Option<Version>,
              data: &[u8]) -> io::Result<()> {
        self.storage
            .entry(chunk.clone())
            .or_insert_with(|| HashMap::new())
            .entry(version.map(|v| v.load()))
            .or_insert_with(|| data.to_owned());
        Ok(())
    }

    fn promote(&mut self, chunk: &ChunkDescriptor) -> io::Result<()> {
        self.storage.get(chunk)
            .unwrap_or(&HashMap::new())
            .keys().cloned().max()
            .and_then(|promotion_target_version| {
                self.storage
                    .get_mut(chunk)
                    .unwrap_or(&mut HashMap::new())
                    .remove(&promotion_target_version)
            }).map(|object| {
                let empty_map = &mut HashMap::new();
                let chunk_map = self.storage
                    .get_mut(chunk)
                    .unwrap_or(empty_map);

                let mut new_map = HashMap::new();
                new_map.insert(None, object);
                mem::replace(chunk_map, new_map);
            });

        Ok(())
    }

    fn delete(&mut self, chunk: &ChunkDescriptor,
              version: Option<Version>) -> io::Result<()> {
        self.storage
            .get_mut(&chunk)
            .unwrap_or(&mut HashMap::new())
            .remove(&version.map(|v| v.load()));
        Ok(())
    }
}

impl Cache for MockStorage {
    fn read(&self, chunk: &ChunkDescriptor, version: Option<Version>,
            mut buf: &mut [u8]) -> io::Result<usize> {
        self.inner.read().unwrap().read(chunk, version, buf)
    }
}

impl Storage for MockStorage {
    fn create(&self, chunk: &ChunkDescriptor, version: Option<Version>,
              data: &[u8]) -> io::Result<()> {
        self.inner.write().unwrap().create(chunk, version, data)
    }

    fn promote(&self, chunk: &ChunkDescriptor) -> io::Result<()> {
        self.inner.write().unwrap().promote(chunk)
    }

    fn delete(&self, chunk: &ChunkDescriptor,
              version: Option<Version>) -> io::Result<()> {
        self.inner.write().unwrap().delete(chunk, version)
    }
}

