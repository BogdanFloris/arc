use std::{
    fs::{self, File},
    io::{self, BufWriter, Write as _},
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use arc_proto::perfetto::{Trace, TracePacket};
use prost::Message as _;

pub(super) struct PacketWriter {
    file: BufWriter<File>,
}

impl PacketWriter {
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

    pub(super) fn write(&mut self, packet: TracePacket) -> io::Result<()> {
        // concatenated one-packet traces decode as a single trace
        let trace = Trace {
            packet: vec![packet],
        };
        self.file.write_all(&trace.encode_to_vec())?;
        self.file.flush()
    }
}
