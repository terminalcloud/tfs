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

extern crate libc;

use std::fs::File;
use std::io;

pub trait FileExt {
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize>;
    fn write_at(&self, offset: u64, data: &[u8]) -> io::Result<usize>;
    fn close(self) -> io::Result<()>;
}

impl FileExt for File {
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
        use libc::pread;
        use std::os::unix::io::AsRawFd;

        unsafe { cvt(pread(self.as_raw_fd(),
                           buf.as_mut_ptr() as *mut ::libc::c_void,
                           buf.len(),
                           offset as ::libc::off_t) as ::libc::c_int) }
    }

    fn write_at(&self, offset: u64, data: &[u8]) -> io::Result<usize> {
        use libc::pwrite;
        use std::os::unix::io::AsRawFd;

        unsafe { cvt(pwrite(self.as_raw_fd(),
                            data.as_ptr() as *const ::libc::c_void,
                            data.len(),
                            offset as ::libc::off_t) as ::libc::c_int) }
    }

    fn close(self) -> io::Result<()> {
        use libc::close;
        use std::os::unix::io::IntoRawFd;

        unsafe { cvt(close(self.into_raw_fd())).map(|_| ()) }
    }
}

pub trait SparseFileExt {
    fn punch(&self, offset: u64, size: usize) -> io::Result<()>;
}

impl SparseFileExt for File {
    #[cfg(target_os = "linux")]
    fn punch(&self, offset: u64, size: usize) -> io::Result<()> {
        use libc::{fallocate, FALLOC_FL_PUNCH_HOLE, FALLOC_FL_KEEP_SIZE};
        use std::os::unix::io::AsRawFd;

        unsafe {
            cvt(fallocate(self.as_raw_fd(),
                          FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE,
                          offset as ::libc::off_t,
                          size as ::libc::off_t) as ::libc::c_int) }.map(|_| ())
    }

    #[cfg(not(target_os = "linux"))]
    /// WARNING: Implemented using zeroing on the system this documentation was generated for.
    fn punch(&self, offset: u64, size: usize) -> io::Result<()> {
        self.write_at(offset, &vec![0; size]).map(|_| ())
    }
}

// Shim for converting C-style errors to io::Errors.
fn cvt(err: ::libc::c_int) -> io::Result<usize> {
    if err < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(err as usize)
    }
}

#[test]
fn test_file_ext() {
    extern crate tempfile;

    let file = tempfile::tempfile().unwrap();
    file.write_at(50, &[1, 2, 3, 4, 5]).unwrap();
    file.write_at(100, &[7, 6, 5, 4, 3, 2, 1]).unwrap();

    let mut buf = &mut [0; 5];
    file.read_at(50, buf).unwrap();
    assert_eq!(buf, &[1, 2, 3, 4, 5]);

    let mut buf = &mut [0; 7];
    file.read_at(100, buf).unwrap();
    assert_eq!(buf, &[7, 6, 5, 4, 3, 2, 1]);

    // Punched data is read as zeroed.
    let mut buf = &mut [1; 5];
    file.punch(50, 5).unwrap();
    file.read_at(50, buf).unwrap();
    assert_eq!(buf, &[0; 5]);

    // Data at the later offset still present after punch.
    let mut buf = &mut [0; 7];
    file.read_at(100, buf).unwrap();
    assert_eq!(buf, &[7, 6, 5, 4, 3, 2, 1]);

    file.close().unwrap();
}

