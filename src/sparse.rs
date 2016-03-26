// Copyright (C) 2016 Cloudlabs, Inc
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

use fext::{FileExt, SparseFileExt};

use slab::Slab;
use variance::InvariantLifetime;

use std::sync::Mutex;
use std::fs::File;

// Must be larger than the page size.
//
// For efficiency, should be a multiple of the page size.
pub const BLOCK_SIZE: usize = 4096 * 16;

pub struct IndexedSparseFile<'id> {
    file: File,
    allocator: IndexAllocator<'id>
}

/// An Index, representing a capability to control a block in an IndexedSparseFile.
///
/// Indexes are unique, they cannot be cloned or copied. Having a read pointer to an
/// Index (&'a Index) grants you read-only access to that block for the lifetime 'a,
/// and having a write pointer to an Index (&'a mut Index) grants you read and write
/// access to the block.
///
/// By using Indexes as an access control mechanism, IndexedSparseFiles avoid internal
/// synchronization.
#[derive(Debug, Hash, PartialEq, Eq)]
#[must_use]
pub struct Index<'id> {
    _id: InvariantLifetime<'id>,
    index: Frozen<usize>
}

impl<'id> Index<'id> {
    pub fn unchecked_new(index: usize) -> Index<'id> {
        Index {
            _id: InvariantLifetime::new(),
            index: Frozen::new(index)
        }
    }

    fn index(&self) -> usize {
        *self.index.as_ref()
    }
}

#[derive(Debug, Clone, Copy, Hash, PartialEq, PartialOrd, Default, Eq)]
pub struct Frozen<T>(T);

impl<T> AsRef<T> for Frozen<T> {
    fn as_ref(&self) -> &T { &self.0 }
}

impl<T> Frozen<T> {
    pub fn new(val: T) -> Self { Frozen(val) }
}

impl<'id> Into<IndexAllocator<'id>> for usize {
    fn into(self) -> IndexAllocator<'id> {
        IndexAllocator::new(self)
    }
}

impl<'id> IndexedSparseFile<'id> {
    pub fn new<I: Into<IndexAllocator<'id>>>(file: File, allocator: I) -> Self {
        IndexedSparseFile {
            file: file,
            allocator: allocator.into()
        }
    }

    /// Read the data at the block with the given Index at the given offset into
    /// the given buffer.
    ///
    /// Preconditions:
    ///   - offset < BLOCK_SIZE
    ///   - buffer.len() <= BLOCK_SIZE - offset
    pub fn read(&self, index: &Index<'id>, offset: usize, buffer: &mut [u8]) -> ::Result<()> {
        assert!(offset < BLOCK_SIZE,
                "offset greater than block size: {:?} > {:?}", offset, BLOCK_SIZE);
        assert!(buffer.len() <= BLOCK_SIZE - offset,
                "requested read larger than block size - offset: {:?} > {:?}",
                buffer.len(), BLOCK_SIZE - offset);

        let real_offset = BLOCK_SIZE * index.index() + offset;
        try!(self.file.read_at(real_offset as u64, buffer));

        Ok(())
    }

    pub fn write(&self, index: &mut Index<'id>, offset: usize, buffer: &[u8]) -> ::Result<()> {
        assert!(offset < BLOCK_SIZE,
                "offset greater than block size: {:?} > {:?}", offset, BLOCK_SIZE);
        assert!(buffer.len() <= BLOCK_SIZE - offset,
                "requested write larger than block size - offset: {:?} > {:?}",
                buffer.len(), BLOCK_SIZE - offset);

        let real_offset = BLOCK_SIZE * index.index() + offset;
        try!(self.file.write_at(real_offset as u64, buffer));

        Ok(())
    }

    pub fn resize(&mut self, _target_size: usize) -> ::Result<()> {
        unimplemented!()
    }

    pub fn allocate(&self) -> Index<'id> {
        self.allocator.allocate()
    }

    pub fn deallocate(&self, index: Index<'id>) -> ::Result<()> {
        try!(self.file.punch((index.index() * BLOCK_SIZE) as u64, BLOCK_SIZE));
        self.allocator.deallocate(index);
        Ok(())
    }
}

pub struct IndexAllocator<'id> {
    _id: InvariantLifetime<'id>,
    inner: Mutex<Slab<(), usize>>
}

impl<'id> IndexAllocator<'id> {
    fn new(size: usize) -> IndexAllocator<'id> {
        IndexAllocator {
            _id: InvariantLifetime::new(),
            inner: Mutex::new(Slab::new(size))
        }
    }

    fn allocate(&self) -> Index<'id> {
        let index = self.inner.lock().unwrap().insert(()).unwrap();
        Index::unchecked_new(index)
    }

    fn deallocate(&self, index: Index<'id>) {
        self.inner.lock().unwrap().remove(*index.index.as_ref());
    }
}

#[cfg(test)]
mod test {
    use tempfile::tempfile;

    use sparse::{IndexedSparseFile, BLOCK_SIZE};

    #[test]
    fn test_sparse_file_write_read_evict() {
        const RUNS: usize = 4;
        const BLOCKS_PER_RUN: usize = 4;
        const INDEX_MULTIPLIER: usize = 20;
        const MAX_CHUNKS: usize = BLOCKS_PER_RUN * INDEX_MULTIPLIER;

        use util::test::gen_random_block;

        let sparse_file = IndexedSparseFile::new(tempfile().unwrap(), MAX_CHUNKS);

        // Run the test for several sets of blocks.
        for _ in 0..RUNS {
            let buffers = vec![gen_random_block(BLOCK_SIZE).1; BLOCKS_PER_RUN];

            let mut indexes = buffers.iter()
                .map(|_| sparse_file.allocate()).collect::<Vec<_>>();

            // Write all the blocks.
            for (index, buffer) in indexes.iter_mut().zip(buffers.iter()) {
                sparse_file.write(index, 0, buffer).unwrap();
            }

            // For each block:
            //   - Read it and confirm its contents
            //   - Evict it
            //   - Read it again and confirm it is evicted
            for (index, buffer) in indexes.into_iter().zip(buffers.iter()) {
                let write_buf: &mut [u8] = &mut [0; BLOCK_SIZE];
                sparse_file.read(&index, 0, write_buf).unwrap();
                assert_eq!(&*write_buf, &**buffer);

                // Evict the block.
                sparse_file.deallocate(index).unwrap();
            }
        }
    }
}

