//! The log as a directory of segments: naming, discovery, rollover, recovery.
//!
//! [`SegmentWriter`] and [`SegmentReader`] deal in one file each. [`Log`] is the
//! layer that makes a directory of them behave as a single append-only log.
//!
//! # Naming
//!
//! A segment is named by the seq of its first event, zero-padded to 20 digits
//! (the width of `u64::MAX`), with a `.log` extension:
//!
//! ```text
//! 00000000000000000000.log   first segment, holds seq 0 onward
//! 00000000000000004711.log   rolled over at seq 4711
//! ```
//!
//! The padding is the point. Lexicographic order equals numeric order equals log
//! order, so discovery is "list, sort" with no parsing needed to get the order
//! right, and any seq can be located by binary search over the names alone
//! without opening a file. Files that are not segment names are ignored, which
//! leaves room for sidecars (an index, a backup marker) in the same directory.
//!
//! # Rollover
//!
//! Before an append, [`Log`] checks whether the framed record still fits in the
//! current segment. If not, the segment is left alone and a new one is opened,
//! named by the seq of the event that did not fit. The check is made before the
//! write, never after, so a record is never split across segments and a segment
//! never overshoots the cap — except for the one case where it must: a record
//! larger than the whole cap, which lands alone in its own segment. With the
//! default sizes that is impossible ([`format::MAX_RECORD_LEN`] is a quarter of
//! [`DEFAULT_MAX_SEGMENT_LEN`]); with the tiny caps tests use, it is routine.
//!
//! # Crash recovery seals, it never truncates
//!
//! If the last segment ends in a torn record, [`Log::open`] does not truncate it.
//! The log is append-only in the strong sense: bytes that reached the disk are
//! never rewritten, not even garbage bytes. Instead the torn segment is *sealed*
//! — nothing will ever be appended to it again — and the next append opens a
//! fresh segment.
//!
//! Sealing needs no marker file. A segment is sealed exactly when a later
//! segment exists, and the naming rule makes that fact self-describing: the
//! segment opened after a torn one is named with the seq recovery resumed at, so
//! the boundary between the two carries the seq that proves nothing was lost.
//! [`LogReader`] then replays a torn segment mid-list by ending that segment's
//! events at the tear and continuing, and its seq-continuity check across the
//! boundary is what keeps the tolerance honest: a tear that swallowed committed
//! records shows up as a seq gap, which is still a hard error.

use std::ffi::OsStr;
use std::fs;
use std::path::{Path, PathBuf};

use arc_proto::v1::Event;
use prost::Message;

use super::{Error, LogReader, SegmentWriter, format};

/// Default size cap for one segment: 64 MiB.
///
/// Small enough that a backup tool moves whole segments cheaply, large enough
/// that rollover is rare. Configurable through
/// [`Log::open_with_max_segment_len`].
pub const DEFAULT_MAX_SEGMENT_LEN: u64 = 64 * 1024 * 1024;

/// Digits in the seq part of a segment name: the width of `u64::MAX`.
const SEQ_DIGITS: usize = 20;

/// Extension every segment file carries, dot included.
const SEGMENT_EXT: &str = ".log";

/// Framing overhead per record, in the units size accounting uses.
const HEADER_LEN: u64 = format::HEADER_SIZE as u64;

/// The name of the segment whose first event carries `first_seq`.
#[must_use]
pub fn segment_name(first_seq: u64) -> String {
    segment_file_name(first_seq, 0)
}

/// The seq a segment file name promises its first event carries, or `None` if
/// the name is not a segment name.
#[must_use]
pub fn segment_first_seq(path: &Path) -> Option<u64> {
    let name = path.file_name().and_then(OsStr::to_str)?;
    parse_segment_file_name(name).map(|(first_seq, _)| first_seq)
}

/// Every segment in `dir`, in log order. Non-segment files are ignored.
///
/// # Errors
///
/// [`Error::Io`] if the directory cannot be read.
pub fn discover_segments(dir: &Path) -> Result<Vec<PathBuf>, Error> {
    let entries = fs::read_dir(dir).map_err(|source| Error::io(dir, source))?;

    let mut found: Vec<(u64, u64, PathBuf)> = Vec::new();
    for entry in entries {
        let entry = entry.map_err(|source| Error::io(dir, source))?;
        let path = entry.path();
        let Some((first_seq, generation)) = path
            .file_name()
            .and_then(OsStr::to_str)
            .and_then(parse_segment_file_name)
        else {
            continue;
        };
        // A directory that happens to be named like a segment is not one.
        if !entry
            .file_type()
            .map_err(|source| Error::io(&path, source))?
            .is_file()
        {
            continue;
        }
        found.push((first_seq, generation, path));
    }

    // Sorting the parsed key rather than the name costs nothing and does not
    // depend on the generation suffix being zero-padded; for the canonical
    // names that are all a healthy log ever holds, the two orders are identical.
    found.sort_unstable();
    Ok(found.into_iter().map(|(_, _, path)| path).collect())
}

/// One append-only log spread over a directory of segments.
///
/// [`open`](Log::open) discovers the segments, replays them to find where
/// appending resumes, and leaves a writer positioned on a segment it is safe to
/// append to. [`append`](Log::append) rolls over by size when a record no longer
/// fits. See the module docs for naming, rollover, and crash recovery.
///
/// The startup replay is a full scan of the log. That is honest work at this
/// phase — the log is small and the scan is the same code path that has to be
/// correct for projections anyway — and it is where a checkpoint would slot in
/// later.
#[derive(Debug)]
pub struct Log {
    dir: PathBuf,
    writer: SegmentWriter,
    /// Bytes in the current segment. Maintained by [`Log::append`] from the
    /// exact framed size of each record, so the rollover check needs no `stat`.
    current_len: u64,
    max_segment_len: u64,
}

impl Log {
    /// Opens the log directory `dir`, creating it if absent, with the default
    /// segment size cap.
    ///
    /// # Errors
    ///
    /// See [`Log::open_with_max_segment_len`].
    pub fn open(dir: impl Into<PathBuf>) -> Result<Self, Error> {
        Self::open_with_max_segment_len(dir, DEFAULT_MAX_SEGMENT_LEN)
    }

    /// Opens the log directory `dir` with an explicit segment size cap.
    ///
    /// Every existing segment is replayed to establish the append point. A cap
    /// smaller than a single record is legal and puts every record in its own
    /// segment; tests use small caps to reach rollover cheaply.
    ///
    /// # Errors
    ///
    /// - [`Error::Io`] if the directory cannot be created, listed, or opened.
    /// - Any replay error — [`Error::Corruption`], [`Error::SeqGap`],
    ///   [`Error::Decode`], [`Error::EmptyEvent`] — surfaces here. Damage is
    ///   reported, never worked around: the caller decides what to do about a
    ///   log that does not replay.
    /// - [`Error::SeqGap`] with `expected: 0` if the log's first replayed
    ///   event is not seq 0: the head of the log is missing or emptied, which
    ///   the continuity check alone cannot see.
    #[tracing::instrument(
        level = "debug",
        name = "log.recover",
        skip_all,
        fields(
            dir = tracing::field::Empty,
            segments = tracing::field::Empty,
            events = tracing::field::Empty,
            next_seq = tracing::field::Empty,
            sealed = tracing::field::Empty,
        )
    )]
    pub fn open_with_max_segment_len(
        dir: impl Into<PathBuf>,
        max_segment_len: u64,
    ) -> Result<Self, Error> {
        let dir = dir.into();
        let span = tracing::Span::current();
        span.record("dir", tracing::field::display(dir.display()));

        fs::create_dir_all(&dir).map_err(|source| Error::io(&dir, source))?;
        let segments = discover_segments(&dir)?;
        span.record("segments", segments.len());
        let head = segments.first().cloned();

        let mut reader = LogReader::new(segments);
        let mut first_seq = None;
        let mut events: u64 = 0;
        for result in &mut reader {
            let event = result?;
            if first_seq.is_none() {
                first_seq = Some(event.seq);
            }
            events += 1;
        }
        // A full-directory replay must start at the beginning of history.
        // The seq-continuity check cannot enforce this — the first event has
        // nothing earlier to be compared against — so a lost or emptied head
        // (a torn-empty or deleted first segment, taking seqs 0..n with it)
        // is caught here instead.
        if let Some(found) = first_seq.filter(|&seq| seq != 0) {
            return Err(Error::SeqGap {
                path: head.unwrap_or_else(|| dir.join(segment_name(0))),
                expected: 0,
                found,
            });
        }
        let point = reader.recovery_point();
        span.record("events", events);
        span.record("next_seq", point.next_seq);

        // A last segment holding bytes past its last valid record ended in a
        // torn append. Seal it: leave the bytes where they are and start a new
        // segment at the recovered seq.
        let mut sealed = false;
        let (path, current_len) = match &point.path {
            Some(last) => {
                let len = file_len(last)?;
                if len == point.offset {
                    (last.clone(), len)
                } else {
                    tracing::warn!(
                        path = %last.display(),
                        valid_end = point.offset,
                        file_len = len,
                        "torn tail on the last segment; sealing it and starting a new one",
                    );
                    sealed = true;
                    (fresh_segment_path(&dir, point.next_seq)?, 0)
                }
            }
            // No segments at all: the first one is named by the first seq,
            // which for a fresh log is 0.
            None => (fresh_segment_path(&dir, point.next_seq)?, 0),
        };
        span.record("sealed", sealed);

        Ok(Self {
            dir,
            writer: SegmentWriter::open(path, point.next_seq)?,
            current_len,
            max_segment_len,
        })
    }

    /// Appends `event`, rolling over to a new segment first if it no longer
    /// fits in the current one, and returns the seq it was written under.
    ///
    /// As with [`SegmentWriter::append`], `seq` is stamped by the log and
    /// whatever the caller left in the event is overwritten.
    ///
    /// # Errors
    ///
    /// - [`Error::MissingPayload`] if `event.payload` is `None`.
    /// - [`Error::RecordTooLarge`] if the encoded event exceeds
    ///   [`format::MAX_RECORD_LEN`].
    /// - [`Error::Io`] if opening a new segment, writing, or fsyncing fails.
    ///
    /// A refused event consumes no sequence number and does not trigger
    /// rollover — both checks happen before anything touches the disk.
    pub fn append(&mut self, mut event: Event) -> Result<u64, Error> {
        // The writer refuses these too. Checking here as well keeps a doomed
        // event from opening a new segment on its way to being rejected.
        if event.payload.is_none() {
            return Err(Error::MissingPayload);
        }

        // Stamping seq before measuring matters: seq is a varint, so its value
        // changes the encoded size. This is the same seq the writer will stamp,
        // so the measurement is exact rather than approximate.
        event.seq = self.writer.next_seq();
        let payload_len = event.encoded_len();
        let Some(record_len) = u32::try_from(payload_len)
            .ok()
            .filter(|len| *len <= format::MAX_RECORD_LEN)
            .map(|len| HEADER_LEN + u64::from(len))
        else {
            return Err(Error::RecordTooLarge { len: payload_len });
        };

        // An empty segment always takes the record, whatever the cap says:
        // rolling over here would open a fresh segment that cannot hold it
        // either, forever.
        if self.current_len > 0 && self.current_len + record_len > self.max_segment_len {
            self.roll_over(event.seq)?;
        }

        let seq = self.writer.append(event)?;
        self.current_len += record_len;
        Ok(seq)
    }

    /// Seals the current segment and opens a new one for `first_seq`.
    fn roll_over(&mut self, first_seq: u64) -> Result<(), Error> {
        let path = fresh_segment_path(&self.dir, first_seq)?;
        let span = tracing::debug_span!(
            "log.rollover",
            from = %self.writer.path().display(),
            to = %path.display(),
            first_seq,
            sealed_bytes = self.current_len,
        );
        let _entered = span.enter();

        // Every record in the outgoing segment was fsynced as it was written,
        // so dropping its writer only closes a file descriptor.
        self.writer = SegmentWriter::open(path, first_seq)?;
        self.current_len = 0;
        Ok(())
    }

    /// Sequence number the next appended event gets.
    #[must_use]
    pub fn next_seq(&self) -> u64 {
        self.writer.next_seq()
    }

    /// Segment currently being appended to.
    #[must_use]
    pub fn current_segment(&self) -> &Path {
        self.writer.path()
    }

    /// Bytes written to the current segment.
    #[must_use]
    pub fn current_segment_len(&self) -> u64 {
        self.current_len
    }

    /// Directory holding the segments.
    #[must_use]
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// Every segment on disk, in log order.
    ///
    /// # Errors
    ///
    /// [`Error::Io`] if the directory cannot be read.
    pub fn segments(&self) -> Result<Vec<PathBuf>, Error> {
        discover_segments(&self.dir)
    }

    /// A reader that replays the whole log from the beginning.
    ///
    /// The segment list is taken when this is called, so a reader does not see
    /// segments rolled over after it. Records appended to a segment it has
    /// already opened are invisible to it as well ([`SegmentReader`] fixes the
    /// file length at open).
    ///
    /// [`SegmentReader`]: super::SegmentReader
    ///
    /// # Errors
    ///
    /// [`Error::Io`] if the directory cannot be read.
    pub fn reader(&self) -> Result<LogReader, Error> {
        Ok(LogReader::new(self.segments()?))
    }
}

/// The file name for a segment starting at `first_seq`, in its `generation`th
/// incarnation. Generation 0 is the canonical name.
fn segment_file_name(first_seq: u64, generation: u64) -> String {
    let padded = format!("{first_seq:0>SEQ_DIGITS$}");
    if generation == 0 {
        format!("{padded}{SEGMENT_EXT}")
    } else {
        format!("{padded}_{generation}{SEGMENT_EXT}")
    }
}

/// The `(first_seq, generation)` a segment file name encodes, or `None` if the
/// name is not one.
fn parse_segment_file_name(name: &str) -> Option<(u64, u64)> {
    let stem = name.strip_suffix(SEGMENT_EXT)?;
    if !stem.is_ascii() || stem.len() < SEQ_DIGITS {
        return None;
    }
    let (digits, suffix) = stem.split_at(SEQ_DIGITS);
    if !digits.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }

    let first_seq = digits.parse().ok()?;
    let generation = match suffix {
        "" => 0,
        suffix => suffix.strip_prefix('_')?.parse().ok()?,
    };
    Some((first_seq, generation))
}

/// A path for a new segment starting at `first_seq`, guaranteed not to name a
/// file that already exists.
///
/// Normally that is [`segment_name`] and the first candidate is free. The
/// canonical name can be taken in exactly one situation: a segment was opened
/// for this seq and the machine died during its first record, so the file holds
/// no valid events and its name is spent. Sealing never truncates, so the name
/// stays spent — the new segment takes a `_1`, `_2`, ... suffix, which sorts
/// after the sealed file and before the next seq, keeping discovery order
/// intact.
fn fresh_segment_path(dir: &Path, first_seq: u64) -> Result<PathBuf, Error> {
    // Terminates: each turn tries a name no earlier turn did, and a directory
    // cannot hold every one of them.
    for generation in 0.. {
        let path = dir.join(segment_file_name(first_seq, generation));
        if !path
            .try_exists()
            .map_err(|source| Error::io(&path, source))?
        {
            return Ok(path);
        }
        tracing::warn!(
            path = %path.display(),
            "segment name already taken; a previous segment was sealed holding no events",
        );
    }
    unreachable!("the generation range is unbounded")
}

fn file_len(path: &Path) -> Result<u64, Error> {
    fs::metadata(path)
        .map(|metadata| metadata.len())
        .map_err(|source| Error::io(path, source))
}

#[cfg(test)]
mod tests {
    use std::fs::{self, OpenOptions};
    use std::path::{Path, PathBuf};

    use arc_proto::v1::{Event, MessageAppended, Role, SessionEvent, Source, event, session_event};
    use prost::Message;
    use tempfile::TempDir;

    use super::{
        DEFAULT_MAX_SEGMENT_LEN, HEADER_LEN, Log, discover_segments, segment_first_seq,
        segment_name,
    };
    use crate::log::{Error, SegmentWriter, format};

    /// Fixed-width content so every record frames to the same number of bytes,
    /// which is what makes the rollover arithmetic in these tests exact.
    fn event(content: &str) -> Event {
        assert_eq!(content.len(), 3, "test events are fixed width");
        Event {
            seq: 0, // stamped by the log
            ts: None,
            source: Source::User as i32,
            payload: Some(event::Payload::Session(SessionEvent {
                event: Some(session_event::Event::MessageAppended(MessageAppended {
                    session_id: "s-01".to_string(),
                    role: Role::User as i32,
                    content: content.to_string(),
                    partial: false,
                    turn_id: String::new(),
                })),
            })),
        }
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

    /// Framed size of one test record when written under `seq`.
    fn record_len(seq: u64) -> u64 {
        let mut e = event("m00");
        e.seq = seq;
        HEADER_LEN + e.encoded_len() as u64
    }

    /// Replays the whole directory and asserts the seqs run 0..n gaplessly.
    fn replay(dir: &Path) -> Vec<Event> {
        let segments = discover_segments(dir).expect("discover");
        let mut reader = crate::log::LogReader::new(segments);
        let events: Vec<Event> = reader.by_ref().map(|r| r.expect("replay")).collect();
        let seqs: Vec<u64> = events.iter().map(|e| e.seq).collect();
        assert_eq!(seqs, (0..events.len() as u64).collect::<Vec<_>>());
        events
    }

    fn names_in(dir: &Path) -> Vec<String> {
        discover_segments(dir)
            .expect("discover")
            .iter()
            .map(|p| p.file_name().expect("name").to_string_lossy().into_owned())
            .collect()
    }

    #[test]
    fn segment_names_are_padded_so_name_order_is_seq_order() {
        assert_eq!(segment_name(0), "00000000000000000000.log");
        assert_eq!(segment_name(4711), "00000000000000004711.log");
        assert_eq!(segment_name(u64::MAX), "18446744073709551615.log");

        let mut names: Vec<String> = [0, 9, 10, 4711, 1_000_000, u64::MAX]
            .into_iter()
            .map(segment_name)
            .collect();
        let by_seq = names.clone();
        names.sort();
        assert_eq!(names, by_seq, "lexicographic order must equal seq order");

        assert_eq!(
            segment_first_seq(Path::new("/data/log/00000000000000004711.log")),
            Some(4711)
        );
        for not_a_segment in [
            "index.sqlite",
            "4711.log",
            "0000000000000000471x.log",
            ".log",
        ] {
            assert_eq!(segment_first_seq(Path::new(not_a_segment)), None);
        }
    }

    #[test]
    fn discovery_orders_a_scrambled_directory() {
        let dir = TempDir::new().expect("temp dir");
        // Created in an order no log would ever produce, plus files that are
        // not segments and must be ignored.
        for seq in [4711, 0, 1_000_000, 12] {
            fs::write(dir.path().join(segment_name(seq)), []).expect("create segment");
        }
        fs::write(dir.path().join("index.sqlite"), []).expect("create sidecar");
        fs::write(dir.path().join("00000000000000000007.log.bak"), []).expect("create sidecar");
        fs::create_dir(dir.path().join("00000000000000000003.log")).expect("create decoy dir");

        assert_eq!(
            names_in(dir.path()),
            [
                "00000000000000000000.log",
                "00000000000000000012.log",
                "00000000000000004711.log",
                "00000000000001000000.log",
            ]
        );
    }

    #[test]
    fn empty_directory_opens_at_seq_zero() {
        let dir = TempDir::new().expect("temp dir");
        let path = dir.path().join("log");
        let log = Log::open(&path).expect("open");

        assert_eq!(log.next_seq(), 0);
        assert_eq!(log.current_segment(), path.join(segment_name(0)));
        assert_eq!(log.current_segment_len(), 0);
        assert!(path.is_dir(), "the log directory is created on open");
    }

    #[test]
    fn rollover_triggers_at_the_threshold_and_names_by_first_seq() {
        let dir = TempDir::new().expect("temp dir");
        // Room for exactly two records, one byte short of a third.
        let two_records = record_len(0) + record_len(1);
        let max = two_records + record_len(2) - 1;
        let mut log = Log::open_with_max_segment_len(dir.path(), max).expect("open");

        assert_eq!(log.append(event("m00")).expect("append"), 0);
        assert_eq!(log.append(event("m01")).expect("append"), 1);
        assert_eq!(
            log.current_segment(),
            dir.path().join(segment_name(0)),
            "two records still fit"
        );
        assert_eq!(log.current_segment_len(), two_records);

        assert_eq!(log.append(event("m02")).expect("append"), 2);
        assert_eq!(
            log.current_segment(),
            dir.path().join(segment_name(2)),
            "the third record rolls over into a segment named by its own seq"
        );
        assert_eq!(log.current_segment_len(), record_len(2));

        assert_eq!(
            names_in(dir.path()),
            ["00000000000000000000.log", "00000000000000000002.log"]
        );
        // The sealed segment stayed inside the cap and holds whole records only.
        let sealed = fs::metadata(dir.path().join(segment_name(0)))
            .expect("metadata")
            .len();
        assert_eq!(sealed, two_records);
        assert!(sealed <= max);
    }

    #[test]
    fn a_record_larger_than_the_cap_gets_a_segment_to_itself() {
        let dir = TempDir::new().expect("temp dir");
        let mut log = Log::open_with_max_segment_len(dir.path(), 1).expect("open");

        for i in 0..3 {
            log.append(event(&format!("m{i:02}"))).expect("append");
        }

        assert_eq!(
            names_in(dir.path()),
            [
                "00000000000000000000.log",
                "00000000000000000001.log",
                "00000000000000000002.log",
            ],
            "a cap below one record puts every record in its own segment",
        );
        assert_eq!(contents_of(&replay(dir.path())), ["m00", "m01", "m02"]);
    }

    #[test]
    fn write_roll_reopen_replay_keeps_one_gapless_sequence() {
        let dir = TempDir::new().expect("temp dir");
        let max = record_len(0) * 2;

        let mut log = Log::open_with_max_segment_len(dir.path(), max).expect("open");
        for i in 0..7 {
            assert_eq!(log.append(event(&format!("m{i:02}"))).expect("append"), i);
        }
        let names_before = names_in(dir.path());
        let active = log.current_segment().to_path_buf();
        let len_before = log.current_segment_len();
        drop(log);

        // Reopening picks up where the last one left off: same segment, same
        // byte count, next seq.
        let mut log = Log::open_with_max_segment_len(dir.path(), max).expect("reopen");
        assert_eq!(log.next_seq(), 7);
        assert_eq!(log.current_segment(), active);
        assert_eq!(log.current_segment_len(), len_before);
        assert_eq!(
            names_in(dir.path()),
            names_before,
            "reopening adds no segment"
        );

        for i in 7..12 {
            assert_eq!(log.append(event(&format!("m{i:02}"))).expect("append"), i);
        }

        assert!(
            names_in(dir.path()).len() > 3,
            "the test wrote across several segments: {:?}",
            names_in(dir.path())
        );
        let events = replay(dir.path());
        let expected: Vec<String> = (0..12).map(|i| format!("m{i:02}")).collect();
        assert_eq!(contents_of(&events), expected);

        // The reader agrees with the writer about where appending resumes.
        assert_eq!(
            log.reader()
                .expect("reader")
                .last()
                .expect("event")
                .expect("event")
                .seq,
            11
        );
    }

    #[test]
    fn a_torn_last_segment_is_sealed_and_the_next_append_lands_in_a_new_one() {
        let dir = TempDir::new().expect("temp dir");
        let mut log =
            Log::open_with_max_segment_len(dir.path(), DEFAULT_MAX_SEGMENT_LEN).expect("open");
        log.append(event("m00")).expect("append");
        log.append(event("m01")).expect("append");
        let torn = log.current_segment().to_path_buf();
        let valid_end = log.current_segment_len();
        drop(log);

        // Crash in the middle of the third append: the record reached disk
        // half-written.
        tear(&torn, 2, valid_end + 5);
        let torn_bytes = fs::read(&torn).expect("read torn segment");

        let mut log = Log::open(dir.path()).expect("reopen after the crash");
        assert_eq!(log.next_seq(), 2, "the torn record never committed");
        assert_ne!(
            log.current_segment(),
            torn,
            "a sealed segment is never reopened"
        );
        assert_eq!(log.current_segment(), dir.path().join(segment_name(2)));
        assert_eq!(
            fs::read(&torn).expect("read torn segment"),
            torn_bytes,
            "recovery must not rewrite a single byte of the torn segment"
        );

        assert_eq!(log.append(event("m02")).expect("append"), 2);
        assert_eq!(log.append(event("m03")).expect("append"), 3);

        // The torn segment is now mid-list, and the whole directory still
        // replays: valid events only, seqs gapless across the seal.
        assert_eq!(
            names_in(dir.path()),
            ["00000000000000000000.log", "00000000000000000002.log"]
        );
        assert_eq!(
            contents_of(&replay(dir.path())),
            ["m00", "m01", "m02", "m03"]
        );

        // And it survives another restart, torn segment still in place.
        let log = Log::open(dir.path()).expect("reopen again");
        assert_eq!(log.next_seq(), 4);
        assert_eq!(log.current_segment(), dir.path().join(segment_name(2)));
        assert_eq!(
            contents_of(&replay(dir.path())),
            ["m00", "m01", "m02", "m03"]
        );
    }

    #[test]
    fn a_segment_torn_on_its_first_record_does_not_reuse_the_spent_name() {
        let dir = TempDir::new().expect("temp dir");
        // The machine died during the very first record ever written, so
        // 000...000.log exists and holds nothing valid.
        let spent = dir.path().join(segment_name(0));
        tear(&spent, 0, 5);

        let mut log = Log::open(dir.path()).expect("open");
        assert_eq!(log.next_seq(), 0);
        assert_eq!(
            log.current_segment(),
            dir.path().join("00000000000000000000_1.log"),
            "the canonical name is spent; the new segment sorts right after it"
        );

        log.append(event("m00")).expect("append");
        assert_eq!(
            names_in(dir.path()),
            ["00000000000000000000.log", "00000000000000000000_1.log"]
        );
        assert_eq!(contents_of(&replay(dir.path())), ["m00"]);
    }

    #[test]
    fn a_missing_or_emptied_log_head_is_detected_at_open() {
        // Head torn down to zero valid events, a later segment carries on
        // from seq 4: records 0..=3 are gone, and continuity alone cannot
        // notice because the first replayed event has nothing before it.
        let dir = TempDir::new().expect("temp dir");
        tear(&dir.path().join(segment_name(0)), 0, 5);
        let mut writer =
            SegmentWriter::open(dir.path().join(segment_name(4)), 4).expect("open writer");
        writer.append(event("m04")).expect("append");
        drop(writer);

        let err = Log::open(dir.path()).expect_err("a lost log head must not open");
        assert!(
            matches!(
                err,
                Error::SeqGap {
                    expected: 0,
                    found: 4,
                    ..
                }
            ),
            "got: {err:?}"
        );

        // Same with the head segment missing entirely.
        let dir = TempDir::new().expect("temp dir");
        let mut writer =
            SegmentWriter::open(dir.path().join(segment_name(2)), 2).expect("open writer");
        writer.append(event("m02")).expect("append");
        drop(writer);

        let err = Log::open(dir.path()).expect_err("a missing log head must not open");
        assert!(
            matches!(
                err,
                Error::SeqGap {
                    expected: 0,
                    found: 2,
                    ..
                }
            ),
            "got: {err:?}"
        );
    }

    #[test]
    fn an_oversized_event_is_refused_without_rolling_over() {
        let dir = TempDir::new().expect("temp dir");
        let mut log = Log::open(dir.path()).expect("open");
        log.append(event("m00")).expect("append");
        let before = names_in(dir.path());

        let mut huge = event("m01");
        if let Some(event::Payload::Session(SessionEvent {
            event: Some(session_event::Event::MessageAppended(m)),
        })) = &mut huge.payload
        {
            m.content = "x".repeat(format::MAX_RECORD_LEN as usize + 1);
        }

        let err = log
            .append(huge)
            .expect_err("an oversized event must be refused");
        assert!(matches!(err, Error::RecordTooLarge { .. }), "got: {err:?}");
        assert_eq!(log.next_seq(), 1, "a refused event burns no seq");
        assert_eq!(names_in(dir.path()), before, "and opens no segment");
        assert_eq!(contents_of(&replay(dir.path())), ["m00"]);
    }

    #[test]
    fn an_event_without_payload_is_refused_without_rolling_over() {
        let dir = TempDir::new().expect("temp dir");
        // A cap of 1 makes every append roll over, so a rollover caused by a
        // doomed event would be impossible to miss.
        let mut log = Log::open_with_max_segment_len(dir.path(), 1).expect("open");
        log.append(event("m00")).expect("append");
        let before = names_in(dir.path());

        let err = log
            .append(Event {
                seq: 0,
                ts: None,
                source: Source::System as i32,
                payload: None,
            })
            .expect_err("payload: None must be refused");

        assert!(matches!(err, Error::MissingPayload), "got: {err:?}");
        assert_eq!(log.next_seq(), 1);
        assert_eq!(names_in(dir.path()), before);
    }

    /// Writes a record at `seq` into `path` and truncates it mid-record,
    /// leaving `extra` bytes of it behind — a crashed append, byte for byte.
    fn tear(path: &PathBuf, seq: u64, at: u64) {
        let mut writer = SegmentWriter::open(path, seq).expect("open writer");
        writer.append(event("trn")).expect("append");
        drop(writer);
        OpenOptions::new()
            .write(true)
            .open(path)
            .expect("open for truncate")
            .set_len(at)
            .expect("truncate");
    }
}
