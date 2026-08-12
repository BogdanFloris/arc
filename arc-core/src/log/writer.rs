//! Append-only writer for a single log segment file.

use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

use arc_proto::v1::Event;
use prost::Message;

use super::{Error, format};

/// Appends framed events to one segment file.
///
/// The writer owns the sequence numbering: [`append`](SegmentWriter::append)
/// stamps `seq` from an internal counter and ignores whatever the caller left in
/// the event, so a wrong `seq` cannot reach the log. The counter starts at the
/// `next_seq` given to [`open`](SegmentWriter::open); recovering that value from
/// an existing segment needs the reader, so it is a constructor parameter for
/// now.
///
/// Each append is one `write_all` of one contiguous record followed by an
/// `fsync`. Durability beats throughput here; batching is a later optimisation
/// and needs trace data to justify it.
///
/// The file is opened in append mode, so every write lands at the end of the
/// file regardless of any other handle. This type never seeks, truncates, or
/// rewrites.
#[derive(Debug)]
pub struct SegmentWriter {
    path: PathBuf,
    file: File,
    next_seq: u64,
}

impl SegmentWriter {
    /// Opens the segment at `path`, creating it if absent, and positions writes
    /// at the end of whatever is already there.
    ///
    /// `next_seq` is the sequence number the next appended event gets. Nothing
    /// here validates it against the file's existing contents — that needs the
    /// reader, and startup composition lands with it. Passing a value that does
    /// not follow the last record in the file produces a log with a gap or a
    /// duplicate.
    ///
    /// `path` is a parameter rather than a derived name because segment naming
    /// and rollover are the caller's business.
    ///
    /// # Errors
    ///
    /// [`Error::Io`] if the file cannot be opened or created, or if the parent
    /// directory entry cannot be flushed to disk.
    pub fn open(path: impl Into<PathBuf>, next_seq: u64) -> Result<Self, Error> {
        let path = path.into();
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .map_err(|source| Error::io(&path, source))?;

        // Creating the file only dirties the parent directory; without this
        // fsync a crash can lose the directory entry and with it every record
        // we then wrote into the file.
        sync_parent_dir(&path)?;

        Ok(Self {
            path,
            file,
            next_seq,
        })
    }

    /// Stamps `event.seq` from the internal counter, appends it as one framed
    /// record, fsyncs, and returns the sequence number it was written under.
    ///
    /// Any `seq` already set on `event` is overwritten. `ts` is the caller's to
    /// fill; the log does not read the clock.
    ///
    /// # Errors
    ///
    /// - [`Error::MissingPayload`] if `event.payload` is `None`. Nothing is
    ///   written and no sequence number is consumed.
    /// - [`Error::RecordTooLarge`] if the encoded event overflows the length
    ///   prefix. Nothing is written and no sequence number is consumed.
    /// - [`Error::Io`] if the write or the fsync fails. A record whose write
    ///   succeeded has consumed its sequence number even if the fsync then
    ///   failed, so the counter still advances; treat the writer as failed and
    ///   rebuild it from the file rather than retrying the append.
    #[tracing::instrument(
        level = "debug",
        name = "log.append",
        skip_all,
        fields(
            path = %self.path.display(),
            seq = tracing::field::Empty,
            bytes = tracing::field::Empty,
        )
    )]
    pub fn append(&mut self, mut event: Event) -> Result<u64, Error> {
        if event.payload.is_none() {
            return Err(Error::MissingPayload);
        }

        let seq = self.next_seq;
        event.seq = seq;
        let record = format::encode_record(&event.encode_to_vec())?;

        let span = tracing::Span::current();
        span.record("seq", seq);
        span.record("bytes", record.len());

        self.file
            .write_all(&record)
            .map_err(|source| Error::io(&self.path, source))?;
        // The bytes are in the file from here on, so the sequence number is
        // spent whether or not the fsync below reports success.
        self.next_seq += 1;
        self.file
            .sync_all()
            .map_err(|source| Error::io(&self.path, source))?;

        Ok(seq)
    }

    /// Sequence number the next append will stamp.
    #[must_use]
    pub fn next_seq(&self) -> u64 {
        self.next_seq
    }

    /// Segment file being written.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }
}

/// Flushes the directory entry for `path` so a freshly created segment survives
/// a crash.
fn sync_parent_dir(path: &Path) -> Result<(), Error> {
    let dir = match path.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent,
        _ => Path::new("."),
    };
    File::open(dir)
        .and_then(|handle| handle.sync_all())
        .map_err(|source| Error::io(dir, source))
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::{Path, PathBuf};

    use arc_proto::v1::{Event, MessageAppended, Role, SessionEvent, Source, event, session_event};
    use prost::Message;
    use tempfile::TempDir;

    use super::SegmentWriter;
    use crate::log::{Error, format};

    fn segment(dir: &TempDir) -> PathBuf {
        dir.path().join("000001.log")
    }

    /// An event with `seq` deliberately wrong, to prove the writer stamps it.
    fn event(content: &str) -> Event {
        Event {
            seq: 999,
            ts: None,
            source: Source::User as i32,
            payload: Some(event::Payload::Session(SessionEvent {
                event: Some(session_event::Event::MessageAppended(MessageAppended {
                    session_id: "s-01".to_string(),
                    role: Role::User as i32,
                    content: content.to_string(),
                })),
            })),
        }
    }

    /// Parses a segment by hand, straight off the framing spec — no reader
    /// exists yet, and the point is to check the bytes independently.
    fn parse_records(path: &Path) -> Vec<(u32, u32, Vec<u8>)> {
        let bytes = fs::read(path).expect("read segment");
        let mut records = Vec::new();
        let mut at = 0usize;

        while at < bytes.len() {
            assert!(at + 8 <= bytes.len(), "torn header at offset {at}");
            let len = u32::from_le_bytes(bytes[at..at + 4].try_into().unwrap());
            let crc = u32::from_le_bytes(bytes[at + 4..at + 8].try_into().unwrap());
            let start = at + 8;
            let end = start + len as usize;
            assert!(end <= bytes.len(), "torn payload at offset {at}");
            records.push((len, crc, bytes[start..end].to_vec()));
            at = end;
        }

        records
    }

    #[test]
    fn appends_records_matching_the_framing_spec() {
        let dir = TempDir::new().expect("temp dir");
        let path = segment(&dir);
        let mut writer = SegmentWriter::open(&path, 1).expect("open");

        writer.append(event("first")).expect("append");
        writer.append(event("second")).expect("append");

        let records = parse_records(&path);
        assert_eq!(records.len(), 2);

        for (i, (len, crc, payload)) in records.iter().enumerate() {
            assert_eq!(*len as usize, payload.len(), "len counts payload only");
            assert_eq!(*crc, crc32fast::hash(payload), "crc covers payload only");

            let decoded = Event::decode(payload.as_slice()).expect("decode payload");
            let expected = {
                let mut e = event(if i == 0 { "first" } else { "second" });
                e.seq = i as u64 + 1;
                e
            };
            assert_eq!(decoded, expected);
        }

        // The framing accounts for every byte in the file: nothing padded,
        // nothing left over.
        let framed: usize = records
            .iter()
            .map(|(_, _, payload)| format::HEADER_SIZE + payload.len())
            .sum();
        let on_disk = usize::try_from(fs::metadata(&path).expect("metadata").len()).expect("fits");
        assert_eq!(framed, on_disk);
    }

    #[test]
    fn stamps_seq_monotonically_from_the_starting_value() {
        let dir = TempDir::new().expect("temp dir");
        let path = segment(&dir);
        let mut writer = SegmentWriter::open(&path, 42).expect("open");

        let stamped: Vec<u64> = (0..3)
            .map(|i| writer.append(event(&format!("m{i}"))).expect("append"))
            .collect();

        assert_eq!(stamped, vec![42, 43, 44]);
        assert_eq!(writer.next_seq(), 45);

        let on_disk: Vec<u64> = parse_records(&path)
            .iter()
            .map(|(_, _, payload)| Event::decode(payload.as_slice()).expect("decode").seq)
            .collect();
        assert_eq!(on_disk, vec![42, 43, 44]);
    }

    #[test]
    fn refuses_an_event_without_payload_and_leaves_the_file_untouched() {
        let dir = TempDir::new().expect("temp dir");
        let path = segment(&dir);
        let mut writer = SegmentWriter::open(&path, 1).expect("open");
        writer.append(event("first")).expect("append");
        let before = fs::read(&path).expect("read segment");

        let empty = Event {
            seq: 7,
            ts: None,
            source: Source::System as i32,
            payload: None,
        };
        let err = writer
            .append(empty)
            .expect_err("payload: None must be refused");

        assert!(matches!(err, Error::MissingPayload));
        assert_eq!(fs::read(&path).expect("read segment"), before);
        // The rejected event burned no sequence number.
        assert_eq!(writer.next_seq(), 2);
        assert_eq!(writer.append(event("second")).expect("append"), 2);
    }

    #[test]
    fn refuses_an_oversized_event_and_leaves_the_file_untouched() {
        let dir = TempDir::new().expect("temp dir");
        let path = segment(&dir);
        let mut writer = SegmentWriter::open(&path, 1).expect("open");
        writer.append(event("first")).expect("append");
        let before = fs::read(&path).expect("read segment");

        // One byte of content over the cap is enough: the encoded event is
        // larger than its content.
        let huge = event(&"x".repeat(format::MAX_RECORD_LEN as usize + 1));
        let err = writer
            .append(huge)
            .expect_err("an event over the record cap must be refused");

        assert!(matches!(err, Error::RecordTooLarge { .. }), "got: {err:?}");
        assert_eq!(fs::read(&path).expect("read segment"), before);
        // The rejected event burned no sequence number.
        assert_eq!(writer.next_seq(), 2);
        assert_eq!(writer.append(event("second")).expect("append"), 2);
    }

    #[test]
    fn reopening_appends_after_the_existing_records() {
        let dir = TempDir::new().expect("temp dir");
        let path = segment(&dir);

        let mut writer = SegmentWriter::open(&path, 1).expect("open");
        writer.append(event("first")).expect("append");
        writer.append(event("second")).expect("append");
        let before = fs::read(&path).expect("read segment");
        drop(writer);

        let mut writer = SegmentWriter::open(&path, 3).expect("reopen");
        assert_eq!(writer.append(event("third")).expect("append"), 3);

        let after = fs::read(&path).expect("read segment");
        assert!(after.starts_with(&before), "existing bytes were rewritten");

        let records = parse_records(&path);
        assert_eq!(records.len(), 3);
        let seqs: Vec<u64> = records
            .iter()
            .map(|(_, _, payload)| Event::decode(payload.as_slice()).expect("decode").seq)
            .collect();
        assert_eq!(seqs, vec![1, 2, 3]);
    }
}
