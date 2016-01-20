use std::io;

pub type Result<T> = ::std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    NotFound,
    Retry,
    /// Indicates an operation failed because it must be filled
    /// by a later Cache or Storage before it can succeed.
    Reserved,
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

    /// Testing helper for asserting this error is Error::Reserved
    pub fn assert_reserved(self) {
        match self {
            Error::Reserved => {},
            e => panic!("Not reserved error: {:?}!", e)
        }
    }

    /// Testing helper for asserting this error is Error::NotFound
    pub fn assert_not_found(self) {
        match self {
            Error::NotFound => {},
            e => panic!("Not not-found error: {:?}!", e)
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
            Error::Retry => "Retry",
            Error::Reserved => "Reserved",
            Error::Io(ref io) => io.description()
        }
    }

    fn cause(&self) -> Option<&::std::error::Error> {
        match *self {
            Error::NotFound => None,
            Error::Retry => None,
            Error::Reserved => None,
            Error::Io(ref io) => Some(io)
        }
    }
}

