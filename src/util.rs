pub mod test {
    use {Chunk};
    use std::collections::HashMap;

    /// Generate a random chunk of data of the given size.
    pub fn gen_random_chunk(chunk_size: usize) -> Vec<u8> {
        vec![::rand::random::<u8>(); chunk_size]
    }

    /// Generate a single random object with a random versioned history.
    pub fn gen_random_object(chunk_size: usize, num_versions: usize) -> Vec<Vec<u8>> {
        vec![gen_random_chunk(chunk_size); num_versions]
    }

    /// Generate a random set of objects with a random versioned history.
    pub fn gen_random_objects(num_objects: usize, chunk_size: usize,
                              num_versions: usize) -> HashMap<Chunk, Vec<Vec<u8>>> {
        (0..num_objects).map(|_| {
            (Chunk(::rand::random()), gen_random_object(chunk_size, num_versions))
        }).collect::<HashMap<Chunk, Vec<Vec<u8>>>>()
    }
}

