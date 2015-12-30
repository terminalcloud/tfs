use scoped_threadpool::Pool;

use uuid::Uuid;

use std::collections::HashMap;
use std::sync::RwLock;
use std::io::{self, Write};
use std::mem;

use {Storage, Cache, ChunkDescriptor, Version, Chunk, FileDescriptor};

pub struct MockStorage {
    inner: RwLock<InnerMockStorage>
}

#[derive(Default)]
struct InnerMockStorage {
    storage: HashMap<ChunkDescriptor, HashMap<Option<usize>, Vec<u8>>>
}

impl MockStorage {
    pub fn new() -> Self {
        MockStorage {
            inner: RwLock::new(InnerMockStorage::default())
        }
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

impl InnerMockStorage {
    fn read(&self, chunk: &ChunkDescriptor, version: Option<Version>,
            mut buf: &mut [u8]) -> io::Result<usize> {
        self.storage
            .get(chunk)
            .and_then(|chunk_map| chunk_map.get(&version.as_ref().map(|v| v.load())))
            .ok_or_else(|| chunk.not_found(version.as_ref()))
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

pub struct StorageFuzzer<S> {
    storage: S
}

fn gen_random_chunk(chunk_size: usize) -> Vec<u8> {
    vec![::rand::random::<u8>(); chunk_size]
}

fn gen_random_objects(num_objects: usize,
                      chunk_size: usize,
                      num_versions: usize) -> HashMap<Chunk, Vec<Vec<u8>>> {
    (0..num_objects).map(|_| {
        (Chunk(Uuid::new_v4()),
         vec![gen_random_chunk(chunk_size); num_versions])
    }).collect::<HashMap<Chunk, Vec<Vec<u8>>>>()
}

impl<S: Storage> StorageFuzzer<S> {
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

        // The number of versions for each chunk.
        let num_versions = magnitude * 10;

        // Map of (chunk_id => (version => object))
        // (the version => object map is represented as a Vec)
        let objects = gen_random_objects(num_objects, chunk_size,
                                         num_versions);

        // Use a thread pool to run the tests.
        let mut pool = Pool::new(objects.len() as u32);

        pool.scoped(|scope| {
            let file = Uuid::new_v4();
            for (chunk, versions) in objects {
                let file = file.clone();

                // The test that gets run for each object.
                let check_object = move || {
                    let chunk_descriptor = ChunkDescriptor {
                        file: FileDescriptor(file.clone()),
                        chunk: chunk.clone()
                    };

                    // Write all the versions of the chunk in order.
                    for (version, data) in versions.iter().enumerate() {
                        self.storage.create(&chunk_descriptor,
                                            Some(Version::new(version)),
                                            data).unwrap();
                    }

                    // Make sure that all the versions of the chunk can
                    // be recovered.
                    let mut buffer = vec![0; chunk_size];
                    for (version, data) in versions.iter().enumerate() {
                        self.storage.read(&chunk_descriptor,
                                          Some(Version::new(version)),
                                          &mut buffer).unwrap();
                        assert_eq!(data, &buffer);
                    }

                    // Ensure that promotion will save the last version.
                    self.storage.promote(&chunk_descriptor).unwrap();
                    self.storage.read(&chunk_descriptor, None, &mut buffer).unwrap();
                    assert_eq!(&buffer, versions.last().unwrap())
                };

                // TODO: This unsafe block can go away in a few weeks.
                unsafe { scope.execute(check_object) };
            }
        });
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
    use uuid::Uuid;
    use mock::{MockStorage, StorageFuzzer};
    use {Storage, Cache, ChunkDescriptor, FileDescriptor, Chunk};

    #[test]
    fn fuzz_mock_storage() {
        let storage = MockStorage::new();
        let fuzzer = StorageFuzzer { storage: storage };
        fuzzer.run(2);
    }

    #[test]
    fn test_mock_storage_create_read() {
        let data = &[1, 2, 3, 4, 5, 6, 7, 8];

        let storage = MockStorage::new();

        let chunk = ChunkDescriptor {
            file: FileDescriptor(Uuid::new_v4()),
            chunk: Chunk(Uuid::new_v4())
        };
        let mut buf = vec![0; data.len()];
        storage.create(&chunk, None, data).unwrap();
        storage.read(&chunk, None, &mut buf).unwrap();
        assert_eq!(&buf, data);

        let chunk = ChunkDescriptor {
            file: FileDescriptor(Uuid::new_v4()),
            chunk: Chunk(Uuid::new_v4())
        };
        let mut buf = vec![0; data.len()];
        storage.create(&chunk, None, &data[..5]).unwrap();
        storage.read(&chunk, None, &mut buf[..5]).unwrap();
        assert_eq!(&buf[..5], &data[..5]);

        // Try to overwrite, ensure it doesn't work.
        storage.create(&chunk, None, data).unwrap();
        storage.read(&chunk, None, &mut buf[..5]).unwrap();
        assert_eq!(&buf, &[1, 2, 3, 4, 5, 0, 0, 0]);
    }
}

