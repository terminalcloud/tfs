use scoped_pool::Pool;

use std::collections::HashMap;
use std::sync::RwLock;
use std::io::Write;

use util::test::gen_random_block;
use {Storage, Cache, ContentId, VolumeMetadata, VolumeName, Snapshot, BlockIndex};

pub struct MockStorage {
    inner: RwLock<InnerMockStorage>
}

#[derive(Default, Clone, Debug)]
struct InnerMockStorage {
    volumes: HashMap<VolumeName, Snapshot>,
    chunks: HashMap<ContentId, Vec<u8>>
}

impl MockStorage {
    pub fn new() -> Self {
        MockStorage {
            inner: RwLock::new(InnerMockStorage::default())
        }
    }
}

impl Storage for MockStorage {
    fn snapshot(&self, volume: &VolumeName, snapshot: Snapshot) -> ::Result<()> {
        self.inner.write().unwrap().snapshot(volume, snapshot)
    }

    fn get_snapshot(&self, name: &VolumeName) -> ::Result<Snapshot> {
        self.inner.read().unwrap().get_snapshot(name)
    }

    fn get_metadata(&self, volume: &VolumeName) -> ::Result<VolumeMetadata> {
        self.inner.read().unwrap().get_metadata(volume)
    }

    fn create(&self, id: ContentId, data: &[u8]) -> ::Result<()> {
        self.inner.write().unwrap().create(id, data)
    }

    fn delete(&self, id: ContentId) -> ::Result<()> {
        self.inner.write().unwrap().delete(id)
    }
}

impl Cache for MockStorage {
    fn read(&self, id: ContentId, buf: &mut [u8]) -> ::Result<()> {
        self.inner.read().unwrap().read(id, buf)
    }
}

impl InnerMockStorage {
    fn snapshot(&mut self, volume: &VolumeName, snapshot: Snapshot) -> ::Result<()> {
        self.volumes.entry(volume.clone())
            .or_insert(snapshot);

        Ok(())
    }

    fn get_snapshot(&self, name: &VolumeName) -> ::Result<Snapshot> {
        self.volumes.get(name).cloned().ok_or(::Error::NotFound)
    }

    fn set_metadata(&mut self, volume: &VolumeName, metadata: VolumeMetadata) -> ::Result<()> {
        self.volumes.get_mut(volume).map(|snap| {
            snap.metadata = metadata;
        }).ok_or(::Error::NotFound)
    }

    fn get_metadata(&self, volume: &VolumeName) -> ::Result<VolumeMetadata> {
        self.volumes.get(&volume).map(|snap| &snap.metadata).cloned().ok_or(::Error::NotFound)
    }

    fn read(&self, id: ContentId, mut buf: &mut [u8]) -> ::Result<()> {
        let data = try!(self.chunks.get(&id).ok_or(::Error::NotFound));

        assert_eq!(data.len(), buf.len());
        buf.write(data).unwrap();

        Ok(())
    }

    fn create(&mut self, id: ContentId, data: &[u8]) -> ::Result<()> {
        self.chunks.entry(id).or_insert_with(|| data.into());

        Ok(())
    }

    fn delete(&mut self, id: ContentId) -> ::Result<()> {
        self.chunks.remove(&id)
            .ok_or(::Error::NotFound)
            .map(|_| ())
    }
}

pub struct StorageFuzzer<S> {
    storage: S
}

impl<S: Storage> StorageFuzzer<S> {
    /// Construct a new StorageFuzzer wrapping a given Storage.
    pub fn new(storage: S) -> StorageFuzzer<S> {
        StorageFuzzer {
            storage: storage
        }
    }

    /// Run all fuzz tests on the underlying Storage.
    ///
    /// The magnitude is used to determine the relative size of a
    /// number of internal parameters. A low magnitude will mean a
    /// faster, but less thorough test; a high magnitude will mean a
    /// slower, but more thorough test.
    ///
    /// Discovering the correct magnitude to use with your storage
    /// usually requires some experimentation.
    pub fn run(&self, magnitude: usize) {
        self.check_properties(magnitude);
        self.check_against_mock(magnitude);
    }

    /// Ensure that the Storage has the precise properties expected of it by fs::Fs.
    ///
    /// Runs many large series of randomized actions meant to emulate a specific
    /// pattern of actions taken by the Fs then verifying that the Storage behaves
    /// in the expected way.
    pub fn check_properties(&self, magnitude: usize) {
        // We will create this many worker threads for controlling
        // this many objects. Each object is only manipulated by one thread.
        let num_objects = magnitude * 100;

        // The size of each chunk.
        let chunk_size = magnitude * 100;

        // Map of (content id => object)
        let objects = (0..num_objects)
            .map(|_| gen_random_block(chunk_size))
            .collect::<HashMap<_, _>>();

        // Ensure metada for non-existent volume gives NotFound.
        self.storage.get_metadata(&VolumeName("random".to_string()))
            .unwrap_err().assert_not_found();

        // Ensure snapshots for non-existent volume gives NotFound.
        self.storage.get_snapshot(&VolumeName("random".to_string()))
            .unwrap_err().assert_not_found();

        // Use a thread pool to run the tests.
        let pool = Pool::new(objects.len());

        pool.scoped(|scope| {
             for (id, data) in objects.clone() {
                 // Test for each object
                 scope.execute(move || {
                    let mut buffer = vec![0; chunk_size];

                    self.storage.create(id, &data).unwrap();
                    self.storage.read(id, &mut buffer).unwrap();

                    assert_eq!(data, buffer);
                 })
            }
        });

        pool.shutdown();

        // Create a snapshot referencing only content-ids we previously uploaded.
        let snapshot = Snapshot {
            metadata: VolumeMetadata { size: num_objects },
            blocks: objects.keys().enumerate()
                .map(|(index, &id)| (BlockIndex(index), id)).collect()
        };

        // Upload the snapshot.
        let name = VolumeName("some_volume".to_string());
        self.storage.snapshot(&name, snapshot.clone()).unwrap();

        // Check we can get the metadata.
        let metadata = self.storage.get_metadata(&name).unwrap();
        assert_eq!(metadata, snapshot.metadata);
    }

    /// Ensure that the underlying Storage behaves equivalently to MockStorage
    /// by runnning a large series of randomly generated actions concurrently.
    ///
    /// This test will attempt to find simple consistency and concurrency
    /// errors in the Storage implementation.
    ///
    /// See `run` for an explanation of the `magnitude` parameter.
    pub fn check_against_mock(&self, magnitude: usize) {
        // Generate a bunch of random objects
    }
}

#[cfg(test)]
mod test {
    use mock::{MockStorage, StorageFuzzer};
    use {Storage, Cache, ContentId};

    #[test]
    fn fuzz_mock_storage() {
        let storage = MockStorage::new();
        let fuzzer = StorageFuzzer { storage: storage };
        fuzzer.run(2);
    }

    #[test]
    fn test_mock_storage_create_read() {
        let data = &[1, 2, 3, 4, 5, 6, 7, 8];
        let id = ContentId::hash(data);

        let storage = MockStorage::new();

        let mut buf = vec![0; data.len()];
        storage.create(id, data).unwrap();
        storage.read(id, &mut buf).unwrap();
        assert_eq!(&buf, data);

        let data = &[8, 7, 6, 5, 5, 6, 7, 8];
        let id = ContentId::hash(data);

        let mut buf = vec![0; data.len()];
        storage.create(id, data).unwrap();
        storage.read(id, &mut buf).unwrap();
        assert_eq!(&buf, data);

        // Try to overwrite, ensure it doesn't work.
        storage.create(id, &[5, 5, 5, 5, 5, 5, 5, 5]).unwrap();
        storage.read(id, &mut buf).unwrap();
        assert_eq!(&buf, data);
    }
}

