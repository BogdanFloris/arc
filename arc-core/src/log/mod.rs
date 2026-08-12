//! The event log: an append-only sequence of framed protobuf events on disk.
//!
//! The log is the single source of truth for durable state (DESIGN.md §3).
//! Nothing here ever seeks backward, truncates, or rewrites bytes.
//!
//! [`format`] owns the on-disk record layout. [`SegmentWriter`] appends records
//! to one segment file; reading and segment rollover live elsewhere.
//!
//! I/O is synchronous `std::fs` on purpose: the log layer stays runtime-agnostic
//! and the caller decides how to schedule blocking writes.

pub mod format;
mod writer;

use std::path::{Path, PathBuf};

pub use writer::SegmentWriter;

/// Everything that can go wrong in the event log.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// An `Event` arrived with no `payload` set. Writing it would put a record
    /// in the log that carries no information but still consumes a sequence
    /// number — corruption by construction, so it is refused at the door.
    #[error("event has no payload; refusing to append")]
    MissingPayload,

    /// The encoded event exceeds what the u32 length prefix can describe.
    #[error("record payload of {len} bytes exceeds the u32 length prefix")]
    RecordTooLarge {
        /// Payload size that was rejected, in bytes.
        len: usize,
    },

    /// A filesystem operation on a segment failed.
    #[error("log segment {path}: {source}")]
    Io {
        /// Segment the operation was against.
        path: PathBuf,
        /// Underlying failure.
        source: std::io::Error,
    },
}

impl Error {
    /// Attaches the segment path to an I/O failure.
    fn io(path: &Path, source: std::io::Error) -> Self {
        Self::Io {
            path: path.to_path_buf(),
            source,
        }
    }
}
