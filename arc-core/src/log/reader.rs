//! Readers for log segment files.

use std::fs::File;
use std::io::{BufReader, Read};
use std::path::PathBuf;

use arc_proto::v1::Event;
use prost::Message;

use super::{Error, format};

/// Reads framed events from one segment file.
///
/// [`SegmentReader`] is a mechanical parser: it reports disk reality and holds
/// no policy. What a torn tail means for the log as a whole is the caller's
/// judgment; [`LogReader`] makes it.
///
/// The iterator fuses on any error: after yielding `Some(Err(_))` it only
/// yields `None`, so a caller draining it cannot loop forever on a bad record.
#[derive(Debug)]
pub struct SegmentReader {
    path: PathBuf,
    reader: BufReader<File>,
    /// Reused across records; replay reads the whole log through this one
    /// allocation.
    payload_buffer: Vec<u8>,
    /// Byte offset of the next unread record, i.e. bytes consumed so far.
    offset: u64,
    /// File size at open. Append-only means the file never shrinks; growth
    /// after open is invisible to this reader by design.
    file_len: u64,
    finished: bool,
}

impl SegmentReader {
    /// Opens the segment at `path` for reading from the beginning.
    ///
    /// # Errors
    ///
    /// [`Error::Io`] if the file cannot be opened or its size read.
    pub fn open(path: impl Into<PathBuf>) -> Result<Self, Error> {
        let path = path.into();
        let file = File::open(&path).map_err(|source| Error::io(&path, source))?;
        let file_len = file
            .metadata()
            .map_err(|source| Error::io(&path, source))?
            .len();

        Ok(Self {
            path,
            reader: BufReader::new(file),
            payload_buffer: Vec::new(),
            offset: 0,
            file_len,
            finished: false,
        })
    }

    #[must_use]
    pub fn offset(&self) -> u64 {
        self.offset
    }

    /// Fuses the iterator and yields the error.
    fn fail(&mut self, error: Error) -> Result<Event, Error> {
        self.finished = true;
        Err(error)
    }

    fn torn_tail(&mut self, offset: u64) -> Result<Event, Error> {
        // Rewind the bookkeeping to the record start: a torn payload has
        // already counted its header, and `offset()` promises to report
        // where valid records end.
        self.offset = offset;
        let path = self.path.clone();
        self.fail(Error::TornTail { path, offset })
    }
}

impl Iterator for SegmentReader {
    type Item = Result<Event, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }

        let start_offset = self.offset;
        let remaining = self.file_len - self.offset;
        if remaining == 0 {
            self.finished = true;
            return None;
        }
        if remaining < format::HEADER_SIZE as u64 {
            return Some(self.torn_tail(start_offset));
        }

        let mut header = [0u8; format::HEADER_SIZE];
        // EOF is impossible here (remaining was checked against an append-only
        // file), so any failure is an I/O error.
        if let Err(source) = self.reader.read_exact(&mut header) {
            let error = Error::io(&self.path, source);
            return Some(self.fail(error));
        }
        self.offset += format::HEADER_SIZE as u64;
        let header = format::decode_header(&header);

        // A length past the cap is checked first, before the length is trusted
        // for anything else. A crashed append cannot produce one: a torn header
        // is a *short* header, caught above, so eight header bytes present means
        // the length is exactly what the writer wrote, and the writer refuses
        // anything over the cap. So this is damage, not truncation — and
        // rejecting it here is what keeps a corrupt header from sizing an
        // allocation, however much of the segment follows it.
        if header.len > format::MAX_RECORD_LEN {
            let path = self.path.clone();
            return Some(self.fail(Error::Corruption {
                path,
                offset: start_offset,
            }));
        }

        // Validate the length against what the file actually holds before
        let payload_len = u64::from(header.len);
        if payload_len > self.file_len - self.offset {
            return Some(self.torn_tail(start_offset));
        }

        self.payload_buffer.resize(header.len as usize, 0);
        if let Err(source) = self.reader.read_exact(&mut self.payload_buffer) {
            let error = Error::io(&self.path, source);
            return Some(self.fail(error));
        }
        self.offset += payload_len;

        if !format::verify(&header, &self.payload_buffer) {
            let path = self.path.clone();
            return Some(self.fail(Error::Corruption {
                path,
                offset: start_offset,
            }));
        }

        match Event::decode(self.payload_buffer.as_slice()) {
            Ok(event) if event.payload.is_none() => {
                let path = self.path.clone();
                Some(self.fail(Error::EmptyEvent {
                    path,
                    offset: start_offset,
                }))
            }
            Ok(event) => Some(Ok(event)),
            Err(source) => {
                let path = self.path.clone();
                Some(self.fail(Error::Decode {
                    path,
                    offset: start_offset,
                    source,
                }))
            }
        }
    }
}

/// Where appending resumes after a replay; feeds [`SegmentWriter::open`].
///
/// Only meaningful once [`LogReader`] iteration has returned `None`.
///
/// [`SegmentWriter::open`]: super::SegmentWriter::open
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoveryPoint {
    /// Last segment read; `None` when the log is empty.
    pub path: Option<PathBuf>,
    /// Byte offset where valid records end in that segment.
    pub offset: u64,
    /// Sequence number the next appended event gets: last seen seq + 1, or 0
    /// when the log holds no events — the first event ever written gets seq 0.
    pub next_seq: u64,
}

/// Iterates events across an ordered list of segments, enforcing the policy
/// [`SegmentReader`] deliberately doesn't: seq continuity across segment
/// boundaries, and a torn tail read as the end of one segment's events rather
/// than as damage. Continuity is what makes that second part safe — nothing can
/// go missing behind a torn tail without the next segment's first seq giving it
/// away.
#[derive(Debug)]
pub struct LogReader {
    segments: std::vec::IntoIter<PathBuf>,
    /// Replaced on segment exhaustion, never removed, so [`recovery_point`]
    /// can always interrogate the last reader.
    ///
    /// [`recovery_point`]: LogReader::recovery_point
    current_reader: Option<SegmentReader>,
    last_seq: Option<u64>,
    finished: bool,
}

impl LogReader {
    /// Takes segments in log order; ordering them is the caller's job
    #[must_use]
    pub fn new(segments: Vec<PathBuf>) -> Self {
        Self {
            segments: segments.into_iter(),
            current_reader: None,
            last_seq: None,
            finished: false,
        }
    }

    /// Marks finished and passes the error through.
    fn fail(&mut self, error: Error) -> Result<Event, Error> {
        tracing::error!(%error, "log replay failed");
        self.finished = true;
        Err(error)
    }

    /// Moves to the next segment. `Ok(false)` means there was none and
    /// iteration is over.
    fn open_next(&mut self) -> Result<bool, Error> {
        let Some(path) = self.segments.next() else {
            self.finished = true;
            return Ok(false);
        };
        self.current_reader = Some(SegmentReader::open(path)?);
        Ok(true)
    }

    /// The exact point where appending resumes, once iteration has ended
    /// (cleanly or on a tolerated last-segment torn tail).
    #[must_use]
    pub fn recovery_point(&self) -> RecoveryPoint {
        RecoveryPoint {
            path: self.current_reader.as_ref().map(|r| r.path.clone()),
            offset: self
                .current_reader
                .as_ref()
                .map_or(0, SegmentReader::offset),
            next_seq: self.last_seq.map_or(0, |seq| seq + 1),
        }
    }
}

impl Iterator for LogReader {
    type Item = Result<Event, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }

        loop {
            let reader = if let Some(reader) = self.current_reader.as_mut() {
                reader
            } else {
                // Only reachable on the first call: afterwards the reader is
                // replaced on segment exhaustion, never removed.
                let Some(path) = self.segments.next() else {
                    // The log has no segments at all.
                    self.finished = true;
                    return None;
                };
                match SegmentReader::open(path) {
                    Ok(reader) => self.current_reader.insert(reader),
                    Err(error) => return Some(self.fail(error)),
                }
            };

            match reader.next() {
                Some(Ok(event)) => {
                    if let Some(last) = self.last_seq {
                        let expected = last + 1;
                        if event.seq != expected {
                            let path = reader.path.clone();
                            return Some(self.fail(Error::SeqGap {
                                path,
                                expected,
                                found: event.seq,
                            }));
                        }
                    }
                    self.last_seq = Some(event.seq);
                    return Some(Ok(event));
                }
                // A torn tail is the signature of an append the machine died in
                // the middle of: the record never committed, so this segment's
                // events end here. On the last segment that ends the replay.
                // On an earlier one it means crash recovery sealed the torn
                // segment and started a new one (see [`Log`]), which is why
                // this is tolerated rather than refused — the strictness that
                // matters is not lost, because the seq-continuity check above
                // still has to pass across the segment boundary. A torn tail
                // that really did swallow committed records surfaces there, as
                // a seq gap.
                //
                // [`Log`]: super::Log
                Some(Err(Error::TornTail { offset, .. })) => {
                    tracing::warn!(
                        path = %reader.path.display(),
                        offset,
                        "torn tail; segment replay ends here"
                    );
                }
                Some(Err(error)) => return Some(self.fail(error)),
                None => {
                    tracing::debug!(
                        path = %reader.path.display(),
                        bytes = reader.offset(),
                        "segment replayed"
                    );
                }
            }

            // Both clean ends of a segment — exhausted, or torn — continue
            // into the next one.
            match self.open_next() {
                Ok(true) => {}
                Ok(false) => return None,
                Err(error) => return Some(self.fail(error)),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::fs::{self, OpenOptions};
    use std::path::Path;

    use arc_proto::v1::{Event, MessageAppended, Role, SessionEvent, Source, event, session_event};
    use tempfile::TempDir;

    use super::{LogReader, RecoveryPoint, SegmentReader};
    use crate::log::{Error, SegmentWriter, format};

    fn event(content: &str) -> Event {
        Event {
            seq: 0, // put by the writer
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

    fn write_segment(path: &Path, start_seq: u64, contents: &[&str]) {
        let mut writer = SegmentWriter::open(path, start_seq).expect("open writer");
        for content in contents {
            writer.append(event(content)).expect("append");
        }
    }

    fn file_len(path: &Path) -> u64 {
        fs::metadata(path).expect("metadata").len()
    }

    fn truncate_to(path: &Path, len: u64) {
        OpenOptions::new()
            .write(true)
            .open(path)
            .expect("open for truncate")
            .set_len(len)
            .expect("truncate");
    }

    /// A segment ending in a torn third record: `contents` intact, then `extra`
    /// bytes of a partially written record. Returns where valid records end.
    fn torn_segment(path: &Path, contents: &[&str], extra: u64) -> u64 {
        write_segment(path, 0, contents);
        let valid_end = file_len(path);
        let mut writer =
            SegmentWriter::open(path, contents.len() as u64).expect("reopen for torn record");
        writer.append(event("torn")).expect("append");
        drop(writer);
        truncate_to(path, valid_end + extra);
        valid_end
    }

    fn contents_of(events: &[Event]) -> Vec<String> {
        events
            .iter()
            .map(|e| match &e.payload {
                Some(event::Payload::Session(SessionEvent {
                    event: Some(session_event::Event::MessageAppended(m)),
                })) => m.content.clone(),
                other => panic!("unexpected payload: {other:?}"),
            })
            .collect()
    }

    #[test]
    fn segment_reader_round_trips_written_records() {
        let dir = TempDir::new().expect("temp dir");
        let path = dir.path().join("000001.log");
        write_segment(&path, 0, &["first", "second", "third"]);

        let mut reader = SegmentReader::open(&path).expect("open reader");
        let events: Vec<Event> = reader.by_ref().map(|r| r.expect("event")).collect();

        assert_eq!(contents_of(&events), ["first", "second", "third"]);
        assert_eq!(events.iter().map(|e| e.seq).collect::<Vec<_>>(), [0, 1, 2]);
        assert_eq!(reader.offset(), file_len(&path));
    }

    #[test]
    fn log_reader_replays_across_segments_in_order() {
        let dir = TempDir::new().expect("temp dir");
        let seg1 = dir.path().join("000001.log");
        let seg2 = dir.path().join("000002.log");
        write_segment(&seg1, 0, &["a", "b", "c"]);
        write_segment(&seg2, 3, &["d", "e"]);

        let mut reader = LogReader::new(vec![seg1, seg2.clone()]);
        let events: Vec<Event> = reader.by_ref().map(|r| r.expect("event")).collect();

        assert_eq!(contents_of(&events), ["a", "b", "c", "d", "e"]);
        assert_eq!(
            events.iter().map(|e| e.seq).collect::<Vec<_>>(),
            [0, 1, 2, 3, 4]
        );
        assert_eq!(
            reader.recovery_point(),
            RecoveryPoint {
                path: Some(seg2.clone()),
                offset: file_len(&seg2),
                next_seq: 5,
            }
        );
    }

    #[test]
    fn empty_log_yields_nothing_and_a_zero_recovery_point() {
        let mut reader = LogReader::new(Vec::new());

        assert!(reader.next().is_none());
        assert_eq!(
            reader.recovery_point(),
            RecoveryPoint {
                path: None,
                offset: 0,
                next_seq: 0,
            }
        );
    }

    #[test]
    fn first_seq_is_accepted_as_is() {
        let dir = TempDir::new().expect("temp dir");
        let path = dir.path().join("000007.log");
        write_segment(&path, 42, &["late start"]);

        let mut reader = LogReader::new(vec![path]);
        let events: Vec<Event> = reader.by_ref().map(|r| r.expect("event")).collect();

        assert_eq!(events[0].seq, 42);
        assert_eq!(reader.recovery_point().next_seq, 43);
    }

    #[test]
    fn torn_tail_on_the_last_segment_ends_replay_cleanly() {
        // 5: mid-header. 10: full header whose payload the file doesn't hold.
        for extra in [5, 10] {
            let dir = TempDir::new().expect("temp dir");
            let path = dir.path().join("000001.log");
            let valid_end = torn_segment(&path, &["first", "second"], extra);

            let mut reader = LogReader::new(vec![path.clone()]);
            let events: Vec<Event> = reader.by_ref().map(|r| r.expect("event")).collect();

            assert_eq!(contents_of(&events), ["first", "second"], "extra={extra}");
            assert_eq!(
                reader.recovery_point(),
                RecoveryPoint {
                    path: Some(path),
                    offset: valid_end,
                    next_seq: 2,
                },
                "extra={extra}"
            );
        }
    }

    #[test]
    fn torn_tail_on_an_earlier_segment_continues_into_the_next() {
        // The crash-recovery shape: a segment torn by a crash, sealed as-is,
        // and a new one opened at the seq the torn record never reached.
        let dir = TempDir::new().expect("temp dir");
        let seg1 = dir.path().join("000001.log");
        let seg2 = dir.path().join("000002.log");
        torn_segment(&seg1, &["a", "b"], 4);
        write_segment(&seg2, 2, &["c"]);

        let mut reader = LogReader::new(vec![seg1, seg2.clone()]);
        let events: Vec<Event> = reader.by_ref().map(|r| r.expect("event")).collect();

        assert_eq!(contents_of(&events), ["a", "b", "c"]);
        assert_eq!(events.iter().map(|e| e.seq).collect::<Vec<_>>(), [0, 1, 2]);
        assert_eq!(
            reader.recovery_point(),
            RecoveryPoint {
                path: Some(seg2.clone()),
                offset: file_len(&seg2),
                next_seq: 3,
            }
        );
    }

    #[test]
    fn a_torn_tail_hiding_missing_records_still_surfaces_as_a_gap() {
        // Same shape, except the next segment does not pick up where the torn
        // one stopped: records went missing, and continuity says so.
        let dir = TempDir::new().expect("temp dir");
        let seg1 = dir.path().join("000001.log");
        let seg2 = dir.path().join("000002.log");
        torn_segment(&seg1, &["a", "b"], 4);
        write_segment(&seg2, 9, &["c"]);

        let mut reader = LogReader::new(vec![seg1, seg2]);
        assert!(reader.next().expect("first").is_ok());
        assert!(reader.next().expect("second").is_ok());

        let error = reader.next().expect("third").expect_err("gap must surface");
        assert!(
            matches!(
                error,
                Error::SeqGap {
                    expected: 2,
                    found: 9,
                    ..
                }
            ),
            "got: {error:?}"
        );
        assert!(reader.next().is_none(), "must fuse after an error");
    }

    #[test]
    fn corrupt_record_is_an_error_even_on_the_last_segment() {
        let dir = TempDir::new().expect("temp dir");
        let path = dir.path().join("000001.log");
        write_segment(&path, 0, &["only"]);

        // Flip the first payload byte; the header stays intact, so this is
        // corruption, not truncation.
        let mut bytes = fs::read(&path).expect("read segment");
        bytes[format::HEADER_SIZE] ^= 0xff;
        fs::write(&path, bytes).expect("write corrupted segment");

        let mut reader = LogReader::new(vec![path]);
        let error = reader
            .next()
            .expect("record")
            .expect_err("corruption must surface");
        assert!(
            matches!(error, Error::Corruption { offset: 0, .. }),
            "got: {error:?}"
        );
        assert!(reader.next().is_none(), "must fuse after an error");
    }

    #[test]
    fn a_header_claiming_more_than_the_record_cap_is_corruption() {
        let dir = TempDir::new().expect("temp dir");
        let path = dir.path().join("000001.log");

        // A header promising one byte past the cap, over a file that holds
        // almost none of it. Without the cap check this would read as a torn
        // tail — tolerated, and sized for on the way there.
        let mut forged = Vec::new();
        forged.extend_from_slice(&(format::MAX_RECORD_LEN + 1).to_le_bytes());
        forged.extend_from_slice(&0u32.to_le_bytes());
        forged.extend_from_slice(b"not sixteen mebibytes");
        fs::write(&path, forged).expect("write forged segment");

        let mut reader = LogReader::new(vec![path]);
        let error = reader
            .next()
            .expect("record")
            .expect_err("an absurd length must surface");
        assert!(
            matches!(error, Error::Corruption { offset: 0, .. }),
            "got: {error:?}"
        );
        assert!(reader.next().is_none(), "must fuse after an error");
    }

    #[test]
    fn seq_gap_across_segments_is_an_error() {
        let dir = TempDir::new().expect("temp dir");
        let seg1 = dir.path().join("000001.log");
        let seg2 = dir.path().join("000002.log");
        write_segment(&seg1, 0, &["a", "b"]);
        write_segment(&seg2, 5, &["gap"]);

        let mut reader = LogReader::new(vec![seg1, seg2]);
        assert!(reader.next().expect("first").is_ok());
        assert!(reader.next().expect("second").is_ok());

        let error = reader.next().expect("third").expect_err("gap must surface");
        assert!(
            matches!(
                error,
                Error::SeqGap {
                    expected: 2,
                    found: 5,
                    ..
                }
            ),
            "got: {error:?}"
        );
        assert!(reader.next().is_none(), "must fuse after an error");
    }

    #[test]
    fn empty_event_record_is_a_hard_error() {
        let dir = TempDir::new().expect("temp dir");
        let path = dir.path().join("000001.log");
        // Event::default() encodes to zero bytes: valid framing, no payload.
        fs::write(&path, format::encode_record(&[]).expect("frame")).expect("write");

        let error = SegmentReader::open(&path)
            .expect("open reader")
            .next()
            .expect("record")
            .expect_err("empty event must surface");
        assert!(
            matches!(error, Error::EmptyEvent { offset: 0, .. }),
            "got: {error:?}"
        );
    }

    #[test]
    fn undecodable_record_is_a_decode_error() {
        let dir = TempDir::new().expect("temp dir");
        let path = dir.path().join("000001.log");
        // 0xff starts a varint key that never completes: CRC-valid, not protobuf.
        fs::write(&path, format::encode_record(&[0xff]).expect("frame")).expect("write");

        let error = SegmentReader::open(&path)
            .expect("open reader")
            .next()
            .expect("record")
            .expect_err("decode failure must surface");
        assert!(
            matches!(error, Error::Decode { offset: 0, .. }),
            "got: {error:?}"
        );
    }
}
