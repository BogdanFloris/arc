//! The file side of the Perfetto layer: packets in, `.pftrace` out.

use std::{
    fs::{self, File},
    io::{self, BufWriter, Write as _},
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use arc_proto::perfetto::{Trace, TracePacket};
use prost::Message as _;

/// Appends packets to one trace file.
///
/// A serialized `Trace` concatenated onto another serialized `Trace` is a
/// valid `Trace` — repeated fields merge — so each packet is written as a
/// one-packet `Trace` and the file is simply appended to. That is what lets
/// the UI open a trace from a daemon that is still running.
pub(super) struct PacketWriter {
    file: BufWriter<File>,
}

impl PacketWriter {
    /// Creates `<dir>/arc-<unix-seconds>.pftrace`, making `dir` if needed.
    ///
    /// One file per daemon run: a run is the unit anyone reasons about, and
    /// rotation inside a run would split a session's spans across files.
    pub(super) fn create(dir: &Path) -> io::Result<(Self, PathBuf)> {
        fs::create_dir_all(dir)?;
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let path = dir.join(format!("arc-{stamp}.pftrace"));
        let file = File::create(&path)?;
        Ok((
            Self {
                file: BufWriter::new(file),
            },
            path,
        ))
    }

    /// Writes one packet and flushes it.
    ///
    /// Flushing every packet costs a write syscall per span — nothing next to
    /// an LLM call — and buys a trace that is complete up to the moment you
    /// copy it, including from a daemon that later dies badly.
    pub(super) fn write(&mut self, packet: TracePacket) -> io::Result<()> {
        let trace = Trace {
            packet: vec![packet],
        };
        self.file.write_all(&trace.encode_to_vec())?;
        self.file.flush()
    }
}
