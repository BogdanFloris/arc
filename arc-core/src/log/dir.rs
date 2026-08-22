use std::ffi::OsStr;
use std::fs;
use std::path::{Path, PathBuf};

use arc_proto::v1::Event;
use prost::Message;

use super::{Error, LogReader, SegmentWriter, format};

pub const DEFAULT_MAX_SEGMENT_LEN: u64 = 64 * 1024 * 1024;

const SEQ_DIGITS: usize = 20;

const SEGMENT_EXT: &str = ".log";

const HEADER_LEN: u64 = format::HEADER_SIZE as u64;

#[must_use]
pub fn segment_name(first_seq: u64) -> String {
    segment_file_name(first_seq, 0)
}

#[must_use]
pub fn segment_first_seq(path: &Path) -> Option<u64> {
    let name = path.file_name().and_then(OsStr::to_str)?;
    parse_segment_file_name(name).map(|(first_seq, _)| first_seq)
}

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
        if !entry
            .file_type()
            .map_err(|source| Error::io(&path, source))?
            .is_file()
        {
            continue;
        }
        found.push((first_seq, generation, path));
    }

    found.sort_unstable();
    Ok(found.into_iter().map(|(_, _, path)| path).collect())
}

#[derive(Debug)]
pub struct Log {
    dir: PathBuf,
    writer: SegmentWriter,
    current_len: u64,
    max_segment_len: u64,
}

impl Log {
    pub fn open(dir: impl Into<PathBuf>) -> Result<Self, Error> {
        Self::open_with_max_segment_len(dir, DEFAULT_MAX_SEGMENT_LEN)
    }

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

    pub fn append(&mut self, mut event: Event) -> Result<u64, Error> {
        if event.payload.is_none() {
            return Err(Error::MissingPayload);
        }

        event.seq = self.writer.next_seq();
        let payload_len = event.encoded_len();
        let Some(record_len) = u32::try_from(payload_len)
            .ok()
            .filter(|len| *len <= format::MAX_RECORD_LEN)
            .map(|len| HEADER_LEN + u64::from(len))
        else {
            return Err(Error::RecordTooLarge { len: payload_len });
        };

        if self.current_len > 0 && self.current_len + record_len > self.max_segment_len {
            self.roll_over(event.seq)?;
        }

        let seq = self.writer.append(event)?;
        self.current_len += record_len;
        Ok(seq)
    }

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

        self.writer = SegmentWriter::open(path, first_seq)?;
        self.current_len = 0;
        Ok(())
    }

    #[must_use]
    pub fn next_seq(&self) -> u64 {
        self.writer.next_seq()
    }

    #[must_use]
    pub fn current_segment(&self) -> &Path {
        self.writer.path()
    }

    #[must_use]
    pub fn current_segment_len(&self) -> u64 {
        self.current_len
    }

    #[must_use]
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    pub fn segments(&self) -> Result<Vec<PathBuf>, Error> {
        discover_segments(&self.dir)
    }

    pub fn reader(&self) -> Result<LogReader, Error> {
        Ok(LogReader::new(self.segments()?))
    }
}

fn segment_file_name(first_seq: u64, generation: u64) -> String {
    let padded = format!("{first_seq:0>SEQ_DIGITS$}");
    if generation == 0 {
        format!("{padded}{SEGMENT_EXT}")
    } else {
        format!("{padded}_{generation}{SEGMENT_EXT}")
    }
}

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

fn fresh_segment_path(dir: &Path, first_seq: u64) -> Result<PathBuf, Error> {
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

    fn event(content: &str) -> Event {
        assert_eq!(content.len(), 3, "test events are fixed width");
        Event {
            seq: 0,
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

    fn record_len(seq: u64) -> u64 {
        let mut e = event("m00");
        e.seq = seq;
        HEADER_LEN + e.encoded_len() as u64
    }

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

        assert_eq!(
            names_in(dir.path()),
            ["00000000000000000000.log", "00000000000000000002.log"]
        );
        assert_eq!(
            contents_of(&replay(dir.path())),
            ["m00", "m01", "m02", "m03"]
        );

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
