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

use std::io;

pub type Result<T> = ::std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    NotFound,
    AlreadyExists,
    AlreadyFrozen,
    ConcurrentSnapshot,
    Io(io::Error)
}

impl Error {
    /// Testing helper for asserting this error is Error::Io
    pub fn assert_io(self) -> io::Error {
        match self {
            Error::Io(io) => io,
            e => panic!("Not io error: {:?}!", e)
        }
    }

    /// Testing helper for asserting this error is Error::NotFound
    pub fn assert_not_found(self) {
        match self {
            Error::NotFound => {},
            e => panic!("Not not-found error: {:?}!", e)
        }
    }

    /// Testing helper for asserting this error is Error::AlreadyExists
    pub fn assert_already_exists(self) {
        match self {
            Error::AlreadyExists => {},
            e => panic!("Not already-exists error: {:?}!", e)
        }
    }

    /// Testing helper for asserting this error is Error::AlreadyFrozen
    pub fn assert_already_frozen(self) {
        match self {
            Error::AlreadyFrozen => {},
            e => panic!("Not already-frozen error: {:?}!", e)
        }
    }

    /// Testing helper for asserting this error is Error::ConcurrentSnapshot
    pub fn assert_concurrent_snapshot(self) {
        match self {
            Error::ConcurrentSnapshot => {},
            e => panic!("Not concurrent-snapshot error: {:?}!", e)
        }
    }
}

impl From<io::Error> for Error {
    fn from(io: io::Error) -> Self {
        Error::Io(io)
    }
}

impl ::std::fmt::Display for Error {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::std::fmt::Debug::fmt(self, f)
    }
}

impl ::std::error::Error for Error {
    fn description(&self) -> &str {
        match *self {
            Error::NotFound => "Not Found",
            Error::AlreadyExists => "Already Exists",
            Error::AlreadyFrozen => "Already Frozen",
            Error::ConcurrentSnapshot => "Concurrent Snapshot",
            Error::Io(ref io) => io.description()
        }
    }

    fn cause(&self) -> Option<&::std::error::Error> {
        match *self {
            Error::Io(ref io) => Some(io),
            _ => None
        }
    }
}

