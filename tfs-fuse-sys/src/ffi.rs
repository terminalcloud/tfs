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

#![allow(non_camel_case_types)]
use libc::{c_char, c_int, size_t, c_void, stat, c_ulong, uint64_t, c_uint,
           mode_t, dev_t, gid_t, uid_t, off_t, statvfs, flock, timespec, pid_t};

extern "C" {
    pub fn fuse_new(ch: *mut fuse_chan, args: *mut fuse_args, op: *const fuse_operations,
                    op_size: size_t, user_data: *mut c_void) -> *mut fuse;
    pub fn fuse_destroy(fuse: *mut fuse) -> c_void;
    pub fn fuse_loop_mt(fuse: *mut fuse) -> c_int;
    pub fn fuse_exit(fuse: *mut fuse) -> c_void;
    pub fn fuse_mount(mountpoint: *const c_char, args: *mut fuse_args) -> *mut fuse_chan;
    pub fn fuse_unmount(mountpoint: *const c_char, ch: *mut fuse_chan) -> c_void;
    pub fn fuse_daemonize(foreground: c_int) -> c_int;
    pub fn fuse_set_signal_handlers(session: *mut fuse_session) -> c_int;
    pub fn fuse_remove_signal_handlers(session: &mut fuse_session) -> c_void;
    pub fn fuse_getcontex() -> *mut fuse_context;
}

// Opaque types
pub struct fuse(c_void);
pub struct fuse_chan(c_void);
pub struct fuse_session(c_void);

#[repr(C)]
pub struct fuse_context {
    pub fuse: *mut fuse,
    pub uid: uid_t,
    pub gid: gid_t,
    pub pid: pid_t,
    pub private: *mut c_void
}

#[repr(C)]
pub struct fuse_args {
    pub argc: c_int,
    pub argv: *const *const c_char,
    pub allocated: c_int
}

#[repr(C)]
pub struct fuse_operations {
    pub getattr: Option<extern "C" fn(*const c_char, *mut stat) -> c_int>,
    pub readlink: Option<extern "C" fn(*const c_char, size_t) -> c_int>,
    pub getdir: Option<extern "C" fn() -> c_int>, // Deprecated
    pub mknod: Option<extern "C" fn(*const c_char, mode_t, dev_t) -> c_int>,
    pub mkdir: Option<extern "C" fn(*const c_char, mode_t) -> c_int>,
    pub unlink: Option<extern "C" fn(*const c_char) -> c_int>,
    pub rmdir: Option<extern "C" fn(*const c_char) -> c_int>,
    pub symlink: Option<extern "C" fn(*const c_char, *const c_char) -> c_int>,
    pub rename: Option<extern "C" fn(*const c_char, *const c_char) -> c_int>,
    pub link: Option<extern "C" fn(*const c_char, *const c_char) -> c_int>,
    pub chmod: Option<extern "C" fn(*const c_char, mode_t) -> c_int>,
    pub chown: Option<extern "C" fn(*const c_char, uid_t, gid_t) -> c_int>,
    pub truncate: Option<extern "C" fn(*const c_char, off_t) -> c_int>,
    pub utime: Option<extern "C" fn() -> c_int>, // Deprecated
    pub open: Option<extern "C" fn(*const c_char, *mut fuse_file_info) -> c_int>,
    pub read: Option<extern "C" fn(*const c_char, *mut c_char, size_t, off_t, *mut fuse_file_info) -> c_int>,
    pub write: Option<extern "C" fn(*const c_char, *const c_char, size_t, off_t, *mut fuse_file_info) -> c_int>,
    pub statfs: Option<extern "C" fn(*const c_char, *mut statvfs) -> c_int>,
    pub flush: Option<extern "C" fn(*const c_char, *mut fuse_file_info) -> c_int>,
    pub release: Option<extern "C" fn(*const c_char, *mut fuse_file_info) -> c_int>,
    pub fsync: Option<extern "C" fn(*const c_char, *mut fuse_file_info) -> c_int>,
    pub setxattr: Option<extern "C" fn() -> c_int>, // Not used
    pub getxattr: Option<extern "C" fn() -> c_int>, // Not used
    pub listxattr: Option<extern "C" fn() -> c_int>, // Not used
    pub removexattr: Option<extern "C" fn() -> c_int>, // Not used
    pub opendir: Option<extern "C" fn(*const c_char, *mut fuse_file_info) -> c_int>,
    pub readdir: Option<extern "C" fn(*const c_char, *mut c_void, fuse_fill_dir_t, off_t, *mut fuse_file_info) -> c_int>,
    pub releasedir: Option<extern "C" fn(*const c_char, *mut fuse_file_info) -> c_int>,
    pub fsyncdir: Option<extern "C" fn(*const c_char, *mut fuse_file_info) -> c_int>,
    pub init: Option<extern "C" fn(*mut fuse_conn_info) -> *mut c_void>,
    pub destroy: Option<extern "C" fn(*mut c_void) -> c_void>,
    pub access: Option<extern "C" fn(*const c_char, c_int) -> c_int>,
    pub create: Option<extern "C" fn(*const c_char, mode_t, *mut fuse_file_info) -> c_int>,
    pub ftruncate: Option<extern "C" fn(*const c_char, off_t, *mut fuse_file_info) -> c_int>,
    pub fgetattr: Option<extern "C" fn(*const c_char, *mut stat, *mut fuse_file_info) -> c_int>,
    pub lock: Option<extern "C" fn(*const c_char, *mut fuse_file_info, c_int, *mut flock) -> c_int>,
    pub utimens: Option<extern "C" fn(*const c_char, [timespec; 2]) -> c_int>,
    pub bmap: Option<extern "C" fn(*const c_char, size_t, uint64_t) -> c_int>,

    // Linux has 32 bits of flags, then four more functions.
    #[cfg(target_os = "linux")]
    pub padding1: u32,

    #[cfg(target_os = "linux")]
    pub padding2: [Option<extern "C" fn() -> c_void>; 4],

    // OS X has 10 reserved funcions, then 10 more functions.
    #[cfg(target_os = "macos")]
    pub padding2: [Option<extern "C" fn() -> c_void>; 20]
}

pub type fuse_fill_dir_t = Option<extern "C" fn(*mut c_void, *const c_char, *const stat, off_t)>;

#[repr(C)]
pub struct fuse_file_info {
    flags: c_int,
    fh_old: c_ulong,
    writepage: c_int,
    bitfields: [u8; 4],
    fh: uint64_t,
    lock_owner: uint64_t
}

#[repr(C)]
pub struct fuse_conn_info {
    proto_major: c_uint,
    proto_minor: c_uint,
    async_read: c_uint,
    max_write: c_uint,
    max_readahead: c_uint,
    padding: [c_uint; 27]
}

