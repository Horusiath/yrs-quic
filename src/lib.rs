use quinn::{ConnectError, ConnectionError, ReadError, ReadExactError, WriteError};
use std::sync::Arc;

pub mod discovery;
#[cfg(feature = "mdns")]
mod mdns;
pub mod peer;
pub mod stream_handle;

pub type Result<T> = std::result::Result<T, Error>;

pub type DynError = Box<dyn std::error::Error + Send + Sync>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("I/O error occurred: {0}")]
    IO(#[from] std::io::Error),
    #[error("stream has been closed")]
    StreamClosed,
    #[error("failed to write data to QUIC stream: {0}")]
    StreamWriteFailed(#[from] WriteError),
    #[error("failed to read data from QUIC stream: {0}")]
    StreamReadFailed(#[from] ReadError),
    #[error("error while generating self-signed certificate: {0}")]
    Certificate(#[from] rcgen::Error),
    #[error("TLS error: {0}")]
    Tls(#[from] rustls::Error),
    #[error("connection error: {0}")]
    Connection(#[from] ConnectionError),
    #[error("error while connecting: {0}")]
    Connect(#[from] ConnectError),
    #[error("string identifier is too long - max allowed length is 256 bytes")]
    NameTooLong,
    #[error("stream handle for that stream name has been already defined")]
    StreamHandleDefinedAlready,
    #[error("{0}")]
    Other(#[from] DynError),
    #[error("Peer `{0}` is not unique")]
    PeerNotUnique(Arc<str>),
}

impl From<ReadExactError> for Error {
    fn from(e: ReadExactError) -> Self {
        match e {
            ReadExactError::FinishedEarly => Error::StreamClosed,
            ReadExactError::ReadError(e) => Error::StreamReadFailed(e),
        }
    }
}
