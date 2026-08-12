//! The event log: an append-only sequence of framed protobuf events on disk.
//!
//! The log is the single source of truth for durable state (DESIGN.md §3).
//! Nothing here ever seeks backward, truncates, or rewrites bytes.
//!
//! [`format`] owns the on-disk record layout. [`SegmentWriter`] appends records
//! to one segment file; [`SegmentReader`] reads one back, and [`LogReader`]
//! replays an ordered list of segments. [`Log`] is the layer above all of them:
//! it names segments, discovers them, rolls over by size, and recovers the
//! append point at startup. Daemons want [`Log`]; the rest is its machinery.
//!
//! I/O is synchronous `std::fs` on purpose: the log layer stays runtime-agnostic
//! and the caller decides how to schedule blocking writes.

mod dir;
pub mod format;
mod reader;
mod writer;

use std::path::{Path, PathBuf};

pub use dir::{DEFAULT_MAX_SEGMENT_LEN, Log, discover_segments, segment_first_seq, segment_name};
pub use reader::{LogReader, RecoveryPoint, SegmentReader};
pub use writer::SegmentWriter;

/// Everything that can go wrong in the event log.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// An `Event` arrived with no `payload` set. Writing it would put a record
    /// in the log that carries no information but still consumes a sequence
    /// number — corruption by construction, so it is refused at the door.
    #[error("event has no payload; refusing to append")]
    MissingPayload,

    /// The encoded event exceeds [`format::MAX_RECORD_LEN`]. A record that big
    /// is a bug upstream, not a legitimate event: nothing is written and no
    /// sequence number is consumed.
    #[error(
        "record payload of {len} bytes exceeds the {} byte record cap",
        format::MAX_RECORD_LEN
    )]
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

    /// A record ran past the end of the segment: a partial header, or a
    /// payload the length prefix promises but the file does not hold. The
    /// signature of an append a crash interrupted; what that means for the log
    /// as a whole is the caller's judgment, not this variant's.
    #[error("log segment {path}: torn record at offset {offset}")]
    TornTail {
        /// Segment holding the torn record.
        path: PathBuf,
        /// Byte offset where the torn record starts.
        offset: u64,
    },

    /// A full-length record whose payload does not match its CRC: bit rot or
    /// an overwritten range, never produced by a crashed append.
    #[error("log segment {path}: corrupt record at offset {offset}")]
    Corruption {
        /// Segment holding the corrupt record.
        path: PathBuf,
        /// Byte offset where the corrupt record starts.
        offset: u64,
    },

    /// CRC-valid bytes that do not decode as an `Event` — version skew or a
    /// writer bug rather than disk damage, which the CRC has already ruled
    /// out.
    #[error("log segment {path}: undecodable record at offset {offset}")]
    Decode {
        /// Segment holding the undecodable record.
        path: PathBuf,
        /// Byte offset where the record starts.
        offset: u64,
        /// The protobuf decode failure.
        #[source]
        source: prost::DecodeError,
    },

    /// A record that decodes to an event with no payload. The writer refuses
    /// these at append time, so one on disk means a foreign or buggy writer.
    #[error("log segment {path}: event with no payload at offset {offset}")]
    EmptyEvent {
        /// Segment holding the record.
        path: PathBuf,
        /// Byte offset where the record starts.
        offset: u64,
    },

    /// Sequence numbers must be gapless and monotonic across the whole log.
    /// A jump means missing segments, segments replayed out of order, or a
    /// writer bug.
    #[error("log segment {path}: expected seq {expected}, found {found}")]
    SeqGap {
        /// Segment holding the out-of-sequence record.
        path: PathBuf,
        /// Sequence number a gapless log required here.
        expected: u64,
        /// Sequence number actually read.
        found: u64,
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
