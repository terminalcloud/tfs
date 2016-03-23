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

extern crate tfs;
extern crate tempdir;
extern crate rand;

use tfs::fs::Fs;
use tfs::local::Options;
use tfs::mock::MockStorage;
use tfs::{VolumeName, VolumeMetadata, BlockIndex, ContentId};

const TEST_UID: u32 = 88;
const TEST_GID: u32 = 24;
const TEST_PERMISSIONS: u16 = 0o777;

fn gen_random_block(block_size: usize) -> (ContentId, Vec<u8>) {
    let data = (0..block_size).map(|_| ::rand::random::<u8>()).collect::<Vec<_>>();
    let id = ContentId::hash(&data);

    (id, data)
}

fn main() {
    let tempdir = tempdir::TempDir::new("tfs-test").unwrap();
    let options = Options {
        mount: tempdir.path().into(),
        size: 100,
        flush_threads: 4,
        sync_threads: 4
    };

    println!("Running simple Fs test.");
    println!("Storing data in temporary directory: {:?}", tempdir.path());

    Fs::run(12, options, Box::new(MockStorage::new()), Vec::new(), |fs, scope| {
        let name = VolumeName("test-volume".to_string());
        let metadata = VolumeMetadata {
            size: 20,
            uid: TEST_UID,
            gid: TEST_GID,
            permissions: TEST_PERMISSIONS
        };
        let vol_id = fs.create(&name, metadata).unwrap();

        println!("Created volume {:?} with id {:?}", name, vol_id);

        for i in 0..10 {
            scope.execute(move || {
                let data1 = gen_random_block(50).1;
                fs.write(&vol_id, BlockIndex(i), 20, &data1).unwrap();

                let data2 = gen_random_block(50).1;
                fs.write(&vol_id, BlockIndex(i), 100, &data2).unwrap();

                let mut buf: &mut [u8] = &mut [0u8; 50];
                fs.read(&vol_id, BlockIndex(i), 20, buf).unwrap();
                assert_eq!(&*data1, &*buf);

                fs.read(&vol_id, BlockIndex(i), 100, buf).unwrap();
                assert_eq!(&*data2, &*buf);
            });
        }
    }).unwrap();

    println!("Complete!");
}

