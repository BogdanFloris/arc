mod dir;
pub mod format;
mod reader;
mod writer;

use std::path::{Path, PathBuf};

pub use dir::{DEFAULT_MAX_SEGMENT_LEN, Log, discover_segments, segment_first_seq, segment_name};
pub use reader::{LogReader, RecoveryPoint, SegmentReader};
pub use writer::SegmentWriter;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("event has no payload; refusing to append")]
    MissingPayload,

    #[error(
        "record payload of {len} bytes exceeds the {} byte record cap",
        format::MAX_RECORD_LEN
    )]
    RecordTooLarge { len: usize },

    #[error("log segment {path}: {source}")]
    Io {
        path: PathBuf,
        source: std::io::Error,
    },

    #[error("log segment {path}: torn record at offset {offset}")]
    TornTail { path: PathBuf, offset: u64 },

    #[error("log segment {path}: corrupt record at offset {offset}")]
    Corruption { path: PathBuf, offset: u64 },

    #[error("log segment {path}: undecodable record at offset {offset}")]
    Decode {
        path: PathBuf,
        offset: u64,
        #[source]
        source: prost::DecodeError,
    },

    #[error("log segment {path}: event with no payload at offset {offset}")]
    EmptyEvent { path: PathBuf, offset: u64 },

    #[error("log segment {path}: expected seq {expected}, found {found}")]
    SeqGap {
        path: PathBuf,
        expected: u64,
        found: u64,
    },
}

impl Error {
    fn io(path: &Path, source: std::io::Error) -> Self {
        Self::Io {
            path: path.to_path_buf(),
            source,
        }
    }
}
