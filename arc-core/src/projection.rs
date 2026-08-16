//! The `SQLite` projection: the archive tier of memory, derived from the log.
//!
//! [`Projection`] turns events into rows in `data/index.db` (DESIGN.md §5.3).
//! It is a projection in the strict sense (DESIGN.md §3, rule 2): the log is the
//! truth, this file is disposable, and deleting it costs a replay and nothing
//! else. Nothing writes these tables except [`Projection::apply`].
//!
//! This module is the storage mechanism, not the replay driver. [`apply`] is
//! deliberately mechanical — it writes the rows one event implies and moves
//! `last_seq`, in one transaction. It does not check that events arrive in
//! order, skip events it has already seen, or decide when a rebuild is needed.
//! Those are the driver's policy, and the driver reads [`Projection::last_seq`]
//! to make them.
//!
//! Enum fields (`role`) are stored as their raw protobuf integers, values this
//! binary does not know included: the projection preserves, clients interpret.
//! Timestamps become microseconds since the Unix epoch, one lossless-enough
//! integer column instead of a seconds/nanos pair.
//!
//! [`apply`]: Projection::apply

use std::path::{Path, PathBuf};

use arc_proto::v1::{Event, MessageAppended, Role, SessionCreated, event, session_event};
use prost_types::Timestamp;
use rusqlite::{Connection, OptionalExtension, Transaction};

use crate::log;

/// Layout version of the tables this module creates.
///
/// Bumped whenever the DDL below changes shape. A database written by a
/// different version is not migrated — it is rejected by
/// [`Projection::open`] so the caller can delete it and re-project the log,
/// which is cheaper and more honest than a migration path for a file that is
/// already reproducible.
pub const SCHEMA_VERSION: u32 = 1;

/// `projection_meta` key holding the seq of the last applied event.
const LAST_SEQ_KEY: &str = "last_seq";

/// `projection_meta` key holding [`SCHEMA_VERSION`].
const SCHEMA_VERSION_KEY: &str = "schema_version";

/// The tables, exactly as DESIGN.md §5.3 describes them.
///
/// `parent_session`, `fork_point` and `project` exist and stay NULL: forking is
/// Phase 3 and projects are Phase 2, but the columns cost nothing now and
/// adding them later would be a schema bump for no reason.
///
/// `messages.seq` is the log's sequence number, so it is globally unique and
/// makes the natural primary key: it ties every row back to the record that
/// produced it and turns a double-applied event into a constraint failure
/// rather than a duplicate row.
///
/// There is no FTS5 index over `content`. Full-text search is Phase 2, and
/// adding it then is a re-projection of the log, not a migration.
const SCHEMA: &str = "\
CREATE TABLE IF NOT EXISTS sessions (
    id             TEXT PRIMARY KEY,
    parent_session TEXT,
    fork_point     INTEGER,
    project        TEXT,
    title          TEXT,
    started_at     INTEGER
);

CREATE TABLE IF NOT EXISTS messages (
    session_id TEXT    NOT NULL,
    seq        INTEGER PRIMARY KEY,
    role       INTEGER NOT NULL,
    content    TEXT    NOT NULL,
    ts         INTEGER
);

CREATE INDEX IF NOT EXISTS messages_by_session ON messages (session_id, seq);

CREATE TABLE IF NOT EXISTS projection_meta (
    key   TEXT PRIMARY KEY,
    value
);
";

/// Everything that can go wrong in the `SQLite` projection.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The index file could not be opened, or its schema could not be created.
    #[error("projection index {path}: {source}")]
    Open {
        /// Index file the operation was against.
        path: PathBuf,
        /// Underlying failure.
        #[source]
        source: rusqlite::Error,
    },

    /// The index was written by a different [`SCHEMA_VERSION`]. The file is
    /// disposable: delete it and replay the log rather than migrating it.
    #[error("projection index {path}: schema version {found}, this build writes {expected}")]
    SchemaVersion {
        /// Index file that was rejected.
        path: PathBuf,
        /// Version recorded in the file.
        found: u32,
        /// Version this build writes.
        expected: u32,
    },

    /// An `Event` arrived with no `payload` set. The log refuses to write these
    /// and its reader treats one on disk as corruption; the projection agrees
    /// rather than quietly projecting nothing.
    #[error("event {seq} has no payload; refusing to project")]
    MissingPayload {
        /// Sequence number of the offending event.
        seq: u64,
    },

    /// A sequence number that does not fit `SQLite`'s signed 64-bit integers, or
    /// one that came back from the index negative. Neither is reachable from a
    /// log this build wrote — 2^63 events is not a number of events — so one
    /// means a hand-edited or foreign index.
    #[error("sequence number {seq} is out of range for the index")]
    SeqOutOfRange {
        /// The offending value, in whichever direction it went out of range.
        seq: i128,
    },

    /// A statement against the index failed. A constraint violation here means
    /// the same event was applied twice — see [`Projection::apply`].
    #[error("projection index: {0}")]
    Sqlite(#[from] rusqlite::Error),
}

/// One row of [`Projection::sessions`]: what a client needs to list a session
/// before opening it.
///
/// `started_at` stays in the projection's own unit — microseconds since the
/// Unix epoch — because that is what the column holds. Callers that speak
/// protobuf convert at their boundary; nothing in this layer does clock
/// formatting.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionSummary {
    /// Session id, as `SessionCreated` set it.
    pub id: String,
    /// Session title. Empty until something names the session.
    pub title: String,
    /// When the session was created, or `None` if its event carried no
    /// timestamp.
    pub started_at: Option<i64>,
    /// The session's first user message, verbatim. Empty if nobody has spoken
    /// in it yet. A label for pickers — not a title, which stays empty until
    /// something generates one.
    pub preview: String,
    /// When the session was last spoken in, either side, or `None` if nothing
    /// has been said in it. What recency means for a session: `started_at` is
    /// when it was opened, which is not when you last used it.
    pub last_at: Option<i64>,
}

/// What a [`replay`] pass did: how many events it applied and how many it
/// skipped as already projected.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReplayStats {
    /// Events applied to the index by this pass.
    pub applied: u64,
    /// Events skipped because the index already held them.
    pub skipped: u64,
}

/// A [`replay`] failure: either side of the composition can fail.
#[derive(Debug, thiserror::Error)]
pub enum ReplayError {
    /// The projection refused or failed to apply an event. The index keeps
    /// everything committed before the failing event; a later replay resumes
    /// from there.
    #[error("projection error: {source}")]
    Projection { source: Error },
    /// The log could not be read — replay stops at the last applied event.
    #[error("log error: {source}")]
    Log { source: log::Error },
}

/// Replay is used to turn a log directory into an up-to-date index
///
/// Resumes from `Projection::last_seq()`
///
/// # Errors
///
/// - [`ReplayError::Projection`] for a projection error
/// - [`ReplayError::Log`] for a log reader error
#[tracing::instrument(
    level = "debug",
    name = "projection.replay",
    skip_all
    fields(
        applied = tracing::field::Empty,
        skipped = tracing::field::Empty,
        last_seq = tracing::field::Empty,
    )
)]
pub fn replay(
    reader: log::LogReader,
    projection: &mut Projection,
) -> Result<ReplayStats, ReplayError> {
    let mut stats = ReplayStats {
        applied: 0,
        skipped: 0,
    };
    let resume_after = projection
        .last_seq()
        .map_err(|source| ReplayError::Projection { source })?;
    let mut last_seq = resume_after;
    for event_res in reader {
        match event_res {
            Ok(event) => {
                if last_seq.is_some_and(|last| event.seq <= last) {
                    stats.skipped += 1;
                    continue;
                }
                projection
                    .apply(&event)
                    .map_err(|source| ReplayError::Projection { source })?;
                last_seq = Some(event.seq);
                stats.applied += 1;
            }
            Err(e) => return Err(ReplayError::Log { source: e }),
        }
    }

    let span = tracing::Span::current();
    span.record("applied", stats.applied);
    span.record("skipped", stats.skipped);
    if let Some(last) = last_seq {
        span.record("last_seq", last);
    }
    Ok(stats)
}

/// The `SQLite` index over the event log.
///
/// Open it with [`open`](Projection::open), feed it events in log order with
/// [`apply`](Projection::apply), and ask [`last_seq`](Projection::last_seq)
/// where the projection got to.
///
/// I/O is synchronous, like the log layer: the caller decides how to schedule
/// blocking work.
#[derive(Debug)]
pub struct Projection {
    conn: Connection,
}

impl Projection {
    /// Opens the index at `path`, creating the file and the schema if either is
    /// absent.
    ///
    /// Creating the schema is idempotent, so opening an existing index is the
    /// same call. `path` goes straight to `SQLite`, so `:memory:` opens a private
    /// in-memory index — what the tests use.
    ///
    /// # Errors
    ///
    /// - [`Error::Open`] if the file cannot be opened or the schema cannot be
    ///   created.
    /// - [`Error::SchemaVersion`] if the index was written by a build with a
    ///   different [`SCHEMA_VERSION`]. Delete the file and re-project.
    #[tracing::instrument(
        level = "debug",
        name = "projection.open",
        skip_all,
        fields(
            path = %path.as_ref().display(),
            schema_version = SCHEMA_VERSION,
        )
    )]
    pub fn open(path: impl AsRef<Path>) -> Result<Self, Error> {
        let path = path.as_ref();
        let opened = |source| Error::Open {
            path: path.to_path_buf(),
            source,
        };

        let conn = Connection::open(path).map_err(opened)?;

        // Durability here is worthless — the log is the truth and a damaged
        // index is deleted and rebuilt, never repaired — while rebuild speed is
        // the whole cost of that policy. So: never fsync, and keep the rollback
        // journal out of the way of the replay.
        //
        // WAL also lets readers (the daemon answering a client while the
        // projection is catching up) run against the index without blocking on
        // the writer. It is a no-op for `:memory:`, which reports back its own
        // journal mode; the pragma is a query, not an assertion.
        conn.query_row("PRAGMA journal_mode = WAL", [], |row| {
            row.get::<_, String>(0)
        })
        .map_err(opened)?;
        conn.pragma_update(None, "synchronous", "OFF")
            .map_err(opened)?;

        conn.execute_batch(SCHEMA).map_err(opened)?;

        let projection = Self { conn };
        projection.check_schema_version(path)?;
        Ok(projection)
    }

    /// Records [`SCHEMA_VERSION`] in a fresh index, or refuses one written by a
    /// different version.
    fn check_schema_version(&self, path: &Path) -> Result<(), Error> {
        let found: Option<u32> = self
            .conn
            .query_row(
                "SELECT value FROM projection_meta WHERE key = ?1",
                (SCHEMA_VERSION_KEY,),
                |row| row.get(0),
            )
            .optional()
            .map_err(|source| Error::Open {
                path: path.to_path_buf(),
                source,
            })?;

        match found {
            Some(SCHEMA_VERSION) => Ok(()),
            Some(found) => Err(Error::SchemaVersion {
                path: path.to_path_buf(),
                found,
                expected: SCHEMA_VERSION,
            }),
            None => {
                self.conn
                    .execute(
                        "INSERT INTO projection_meta (key, value) VALUES (?1, ?2)",
                        (SCHEMA_VERSION_KEY, SCHEMA_VERSION),
                    )
                    .map_err(|source| Error::Open {
                        path: path.to_path_buf(),
                        source,
                    })?;
                Ok(())
            }
        }
    }

    /// Applies one event: writes the rows it implies and sets `last_seq` to its
    /// sequence number, in one transaction. Either both land or neither does.
    ///
    /// Mechanical by design. Events are applied exactly as handed over, in the
    /// order handed over; ordering, resumption and rebuild policy belong to the
    /// replay driver above this type. Two consequences worth knowing:
    ///
    /// - Applying the same event twice is a primary-key violation, surfaced as
    ///   [`Error::Sqlite`]. That is the point of keying `messages` by log seq: a
    ///   driver that lost its place fails loudly instead of silently
    ///   duplicating history.
    /// - `last_seq` follows the last event applied, whatever it was. Feed
    ///   events out of order and it will say so.
    ///
    /// An event whose kind this build does not know — a `SessionEvent` oneof
    /// arm from a newer schema — writes no rows but still advances `last_seq`,
    /// with a warning. An old binary must be able to replay a newer log to its
    /// end; stalling on the first unknown event would be worse than skipping
    /// it, and the log still holds everything that was skipped.
    ///
    /// # Errors
    ///
    /// - [`Error::MissingPayload`] if `event.payload` is `None`. Nothing is
    ///   written and no transaction is opened.
    /// - [`Error::Sqlite`] if a statement fails, or [`Error::SeqOutOfRange`] if
    ///   `event.seq` does not fit a signed 64-bit integer. Either way the
    ///   transaction is rolled back and the index keeps the state it had before
    ///   the call.
    #[tracing::instrument(
        level = "debug",
        name = "projection.apply",
        skip_all,
        fields(
            seq = event.seq,
            kind = tracing::field::Empty,
        )
    )]
    pub fn apply(&mut self, event: &Event) -> Result<(), Error> {
        // Checked before the transaction opens: a rejected event must leave the
        // connection exactly as it found it.
        let Some(payload) = event.payload.as_ref() else {
            return Err(Error::MissingPayload { seq: event.seq });
        };
        let span = tracing::Span::current();

        let tx = self.conn.transaction()?;
        match payload {
            event::Payload::Session(session) => match session.event.as_ref() {
                Some(session_event::Event::SessionCreated(created)) => {
                    span.record("kind", "session_created");
                    insert_session(&tx, event, created)?;
                }
                Some(session_event::Event::MessageAppended(appended)) => {
                    span.record("kind", "message_appended");
                    insert_message(&tx, event, appended)?;
                }
                // Skipped deliberately, not lost: the projection is disposable,
                // and the messages shape that holds tool rows lands with the
                // archive tier, which re-projects.
                Some(session_event::Event::ToolCallIssued(_)) => {
                    span.record("kind", "tool_call_issued");
                }
                Some(session_event::Event::ToolResultRecorded(_)) => {
                    span.record("kind", "tool_result_recorded");
                }
                None => {
                    span.record("kind", "unknown");
                    tracing::warn!(
                        seq = event.seq,
                        "session event of an unknown kind; skipping its rows"
                    );
                }
            },
            // Skipped the same way and for the same reason: record state is a
            // projection of these, and it lands with the distilled tier.
            event::Payload::Memory(_) => {
                span.record("kind", "memory");
            }
        }
        set_last_seq(&tx, event.seq)?;
        tx.commit()?;

        Ok(())
    }

    /// Sequence number of the last event [`apply`](Projection::apply) accepted,
    /// or `None` if nothing has been applied yet.
    ///
    /// This is where a replay resumes from. `None` means an empty index, which
    /// is indistinguishable from a deleted one — that is the intent.
    ///
    /// # Errors
    ///
    /// [`Error::Sqlite`] if the index cannot be read, or
    /// [`Error::SeqOutOfRange`] if the stored value is negative.
    pub fn last_seq(&self) -> Result<Option<u64>, Error> {
        let stored: Option<i64> = self
            .conn
            .query_row(
                "SELECT value FROM projection_meta WHERE key = ?1",
                (LAST_SEQ_KEY,),
                |row| row.get(0),
            )
            .optional()?;
        stored
            .map(|seq| {
                u64::try_from(seq).map_err(|_| Error::SeqOutOfRange {
                    seq: i128::from(seq),
                })
            })
            .transpose()
    }

    /// Every session in the index, oldest first.
    ///
    /// Ordered by `started_at`, then by `id` to break ties — two sessions
    /// created inside the same microsecond still come back in a fixed order,
    /// so the list a client renders does not shuffle between calls. A session
    /// whose event carried no timestamp sorts first, where `SQLite` puts NULL.
    ///
    /// The whole table, unpaginated: Phase 1 has one user and a session count
    /// a person could scroll. Paging is a wire-protocol question (DESIGN.md
    /// §7) and gets answered when the number of sessions makes it one.
    ///
    /// # Errors
    ///
    /// [`Error::Sqlite`] if the index cannot be read.
    pub fn sessions(&self) -> Result<Vec<SessionSummary>, Error> {
        // The subqueries ride along rather than costing a query per session:
        // `messages_by_session` makes each one an index seek.
        let mut stmt = self.conn.prepare(
            "SELECT s.id, coalesce(s.title, ''), s.started_at,
                    coalesce((SELECT m.content FROM messages m
                              WHERE m.session_id = s.id AND m.role = ?1
                              ORDER BY m.seq LIMIT 1), ''),
                    (SELECT MAX(m.ts) FROM messages m WHERE m.session_id = s.id)
             FROM sessions s ORDER BY s.started_at, s.id",
        )?;
        let rows = stmt.query_map([Role::User as i32], |row| {
            Ok(SessionSummary {
                id: row.get(0)?,
                title: row.get(1)?,
                started_at: row.get(2)?,
                preview: row.get(3)?,
                last_at: row.get(4)?,
            })
        })?;
        Ok(rows.collect::<Result<_, _>>()?)
    }

    /// The conversation history of one session: `(role, content)` pairs in
    /// seq order.
    ///
    /// # Errors
    ///
    /// [`Error::Sqlite`] if the index cannot be read.
    pub fn messages(&self, session_id: &str) -> Result<Vec<(i32, String)>, Error> {
        let mut stmt = self
            .conn
            .prepare("SELECT role, content FROM messages WHERE session_id = ?1 ORDER BY seq")?;
        let rows = stmt.query_map([session_id], |row| Ok((row.get(0)?, row.get(1)?)))?;
        Ok(rows.collect::<Result<_, _>>()?)
    }
}

/// Writes the `sessions` row for a `SessionCreated`.
///
/// `provider` and `model` are on the event but have no column: the archive
/// indexes what sessions are searched by, and the log answers the rest.
fn insert_session(
    tx: &Transaction<'_>,
    event: &Event,
    created: &SessionCreated,
) -> Result<(), Error> {
    tx.execute(
        "INSERT INTO sessions (id, parent_session, fork_point, project, title, started_at)
         VALUES (?1, NULL, NULL, NULL, ?2, ?3)",
        (
            &created.session_id,
            &created.title,
            epoch_micros(event.ts.as_ref()),
        ),
    )?;
    Ok(())
}

/// Writes the `messages` row for a `MessageAppended`.
fn insert_message(
    tx: &Transaction<'_>,
    event: &Event,
    appended: &MessageAppended,
) -> Result<(), Error> {
    tx.execute(
        "INSERT INTO messages (session_id, seq, role, content, ts) VALUES (?1, ?2, ?3, ?4, ?5)",
        (
            &appended.session_id,
            seq_param(event.seq)?,
            // Raw enum integer, unknown values included.
            appended.role,
            &appended.content,
            epoch_micros(event.ts.as_ref()),
        ),
    )?;
    Ok(())
}

/// Points `last_seq` at `seq`.
fn set_last_seq(tx: &Transaction<'_>, seq: u64) -> Result<(), Error> {
    tx.execute(
        "INSERT INTO projection_meta (key, value) VALUES (?1, ?2)
         ON CONFLICT (key) DO UPDATE SET value = excluded.value",
        (LAST_SEQ_KEY, seq_param(seq)?),
    )?;
    Ok(())
}

/// Converts a log sequence number to the signed integer `SQLite` stores.
fn seq_param(seq: u64) -> Result<i64, Error> {
    i64::try_from(seq).map_err(|_| Error::SeqOutOfRange {
        seq: i128::from(seq),
    })
}

/// Flattens a protobuf timestamp to microseconds since the Unix epoch.
///
/// Sub-microsecond precision is dropped; nothing in this system times anything
/// that finely, and one integer column sorts and ranges without a compound key.
/// An absent timestamp stays absent (NULL).
///
/// Saturating rather than failing: a timestamp far enough out to overflow (past
/// year 294 247) is already nonsense, and refusing to project a whole log over
/// one bad clock reading would be the worse failure. It clamps, keeping the row
/// and its ordering.
fn epoch_micros(ts: Option<&Timestamp>) -> Option<i64> {
    let ts = ts?;
    Some(
        ts.seconds
            .saturating_mul(1_000_000)
            .saturating_add(i64::from(ts.nanos) / 1_000),
    )
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use arc_proto::v1::{
        Event, MessageAppended, Role, SessionCreated, SessionEvent, Source, event, session_event,
    };
    use prost_types::Timestamp;
    use rusqlite::OptionalExtension;
    use tempfile::TempDir;

    use super::{Error, Projection, ReplayError, ReplayStats, SessionSummary, replay};
    use crate::log::{Log, LogReader, discover_segments};

    /// 2023-11-14T22:13:20.123456789Z, chosen so the nanos truncate visibly.
    const TS_SECONDS: i64 = 1_700_000_000;
    const TS_NANOS: i32 = 123_456_789;
    const TS_MICROS: i64 = 1_700_000_000_123_456;

    fn timestamp() -> Timestamp {
        Timestamp {
            seconds: TS_SECONDS,
            nanos: TS_NANOS,
        }
    }

    fn session_created(seq: u64) -> Event {
        Event {
            seq,
            ts: Some(timestamp()),
            source: Source::User as i32,
            payload: Some(event::Payload::Session(SessionEvent {
                event: Some(session_event::Event::SessionCreated(SessionCreated {
                    session_id: "s-01".to_string(),
                    title: "first light".to_string(),
                    provider: "gemini".to_string(),
                    model: "gemini-3-pro".to_string(),
                })),
            })),
        }
    }

    fn message_appended(seq: u64, content: &str) -> Event {
        Event {
            seq,
            ts: Some(timestamp()),
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

    /// An event this build cannot interpret: the payload is a session event,
    /// but no oneof arm is set — what a newer schema's event kind decodes to
    /// here.
    fn unknown_kind(seq: u64) -> Event {
        Event {
            seq,
            ts: Some(timestamp()),
            source: Source::System as i32,
            payload: Some(event::Payload::Session(SessionEvent { event: None })),
        }
    }

    /// A whole `sessions` row, so an assertion covers every column — including
    /// the ones that must still be NULL.
    #[derive(Debug, PartialEq)]
    struct SessionRow {
        id: String,
        parent_session: Option<String>,
        fork_point: Option<i64>,
        project: Option<String>,
        title: Option<String>,
        started_at: Option<i64>,
    }

    fn table_names(projection: &Projection) -> Vec<String> {
        let mut stmt = projection
            .conn
            .prepare("SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name")
            .expect("prepare");
        stmt.query_map([], |row| row.get::<_, String>(0))
            .expect("query")
            .collect::<Result<Vec<_>, _>>()
            .expect("rows")
    }

    fn row_count(projection: &Projection, table: &str) -> i64 {
        projection
            .conn
            .query_row(&format!("SELECT count(*) FROM {table}"), [], |row| {
                row.get(0)
            })
            .expect("count")
    }

    #[test]
    fn open_creates_the_schema() {
        let projection = Projection::open(":memory:").expect("open");

        assert_eq!(
            table_names(&projection),
            vec![
                "messages".to_string(),
                "projection_meta".to_string(),
                "sessions".to_string(),
            ]
        );
        assert_eq!(projection.last_seq().expect("last_seq"), None);
    }

    #[test]
    fn reopening_keeps_the_schema_and_the_rows() {
        let dir = TempDir::new().expect("temp dir");
        let path = dir.path().join("index.db");

        let mut projection = Projection::open(&path).expect("open");
        projection.apply(&session_created(0)).expect("apply");
        drop(projection);

        let mut projection = Projection::open(&path).expect("reopen");
        assert_eq!(
            table_names(&projection),
            vec![
                "messages".to_string(),
                "projection_meta".to_string(),
                "sessions".to_string(),
            ]
        );
        assert_eq!(row_count(&projection, "sessions"), 1);
        assert_eq!(projection.last_seq().expect("last_seq"), Some(0));

        // The reopened index is still writable, and picks up where it left off.
        projection
            .apply(&message_appended(1, "hello"))
            .expect("apply");
        assert_eq!(projection.last_seq().expect("last_seq"), Some(1));
    }

    #[test]
    fn projects_a_session_and_a_message() {
        let mut projection = Projection::open(":memory:").expect("open");

        projection.apply(&session_created(0)).expect("apply");
        projection
            .apply(&message_appended(1, "hello"))
            .expect("apply");

        let session = projection
            .conn
            .query_row(
                "SELECT id, parent_session, fork_point, project, title, started_at
                 FROM sessions",
                [],
                |row| {
                    Ok(SessionRow {
                        id: row.get(0)?,
                        parent_session: row.get(1)?,
                        fork_point: row.get(2)?,
                        project: row.get(3)?,
                        title: row.get(4)?,
                        started_at: row.get(5)?,
                    })
                },
            )
            .expect("session row");
        assert_eq!(
            session,
            SessionRow {
                id: "s-01".to_string(),
                // Phase 3 columns exist and stay empty.
                parent_session: None,
                fork_point: None,
                project: None,
                title: Some("first light".to_string()),
                // Sub-microsecond precision is dropped, the rest survives.
                started_at: Some(TS_MICROS),
            }
        );

        let message: (String, i64, i64, String, Option<i64>) = projection
            .conn
            .query_row(
                "SELECT session_id, seq, role, content, ts FROM messages",
                [],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                    ))
                },
            )
            .expect("message row");
        assert_eq!(
            message,
            (
                "s-01".to_string(),
                1,
                i64::from(Role::User as i32),
                "hello".to_string(),
                Some(TS_MICROS),
            )
        );
    }

    #[test]
    fn last_seq_follows_the_last_applied_event() {
        let mut projection = Projection::open(":memory:").expect("open");
        assert_eq!(projection.last_seq().expect("last_seq"), None);

        projection.apply(&session_created(7)).expect("apply");
        assert_eq!(projection.last_seq().expect("last_seq"), Some(7));

        projection
            .apply(&message_appended(8, "hello"))
            .expect("apply");
        assert_eq!(projection.last_seq().expect("last_seq"), Some(8));
    }

    #[test]
    fn an_unknown_event_kind_advances_last_seq_without_writing_rows() {
        let mut projection = Projection::open(":memory:").expect("open");
        projection.apply(&session_created(0)).expect("apply");

        projection.apply(&unknown_kind(1)).expect("apply");

        assert_eq!(row_count(&projection, "sessions"), 1);
        assert_eq!(row_count(&projection, "messages"), 0);
        assert_eq!(projection.last_seq().expect("last_seq"), Some(1));
    }

    #[test]
    fn messages_come_back_in_seq_order() {
        let mut projection = Projection::open(":memory:").expect("open");
        projection.apply(&session_created(0)).expect("apply");
        for seq in 1..=10 {
            projection
                .apply(&message_appended(seq, &format!("hello_{seq}")))
                .expect("apply");
        }

        let conv = projection.messages("s-01").expect("messages");

        assert_eq!(conv.len(), 10);
        for (i, (role, content)) in conv.iter().enumerate() {
            assert_eq!(*role, Role::User as i32);
            assert_eq!(content, &format!("hello_{}", i + 1));
        }
    }

    /// A `SessionCreated` with its own id, title, and creation second.
    fn session_created_as(seq: u64, id: &str, title: &str, at: Option<i64>) -> Event {
        Event {
            seq,
            ts: at.map(|seconds| Timestamp { seconds, nanos: 0 }),
            source: Source::User as i32,
            payload: Some(event::Payload::Session(SessionEvent {
                event: Some(session_event::Event::SessionCreated(SessionCreated {
                    session_id: id.to_string(),
                    title: title.to_string(),
                    provider: "gemini".to_string(),
                    model: "gemini-3-pro".to_string(),
                })),
            })),
        }
    }

    #[test]
    fn sessions_are_ordered_by_start_then_id() {
        let mut projection = Projection::open(":memory:").expect("open");
        assert_eq!(projection.sessions().expect("sessions"), []);

        // Applied in neither id order nor time order, and two share a start.
        for (seq, id, title, at) in [
            (0, "s-c", "third", Some(300)),
            (1, "s-b", "second", Some(200)),
            (2, "s-a", "also second", Some(200)),
            (3, "s-none", "no clock", None),
        ] {
            projection
                .apply(&session_created_as(seq, id, title, at))
                .expect("apply");
        }

        let sessions = projection.sessions().expect("sessions");

        let ids: Vec<&str> = sessions.iter().map(|s| s.id.as_str()).collect();
        assert_eq!(
            ids,
            ["s-none", "s-a", "s-b", "s-c"],
            "no timestamp first, then by start, ties broken by id"
        );
        assert_eq!(
            sessions[1],
            SessionSummary {
                id: "s-a".to_string(),
                title: "also second".to_string(),
                started_at: Some(200_000_000),
                preview: String::new(),
                last_at: None,
            }
        );
        assert_eq!(sessions[0].started_at, None);
    }

    /// The picker labels sessions by their opening line, so the preview must
    /// be the *first user* message — not the first message (which can be the
    /// model, healing a torn turn) and not the last.
    #[test]
    fn a_session_previews_its_first_user_message() {
        let mut projection = Projection::open(":memory:").expect("open");
        projection.apply(&session_created(0)).expect("apply");
        assert_eq!(
            projection.sessions().expect("sessions")[0].preview,
            "",
            "a session nobody has spoken in has no preview"
        );

        let mut reply = message_appended(1, "a stray model line");
        if let Some(event::Payload::Session(SessionEvent {
            event: Some(session_event::Event::MessageAppended(appended)),
        })) = &mut reply.payload
        {
            appended.role = Role::Assistant as i32;
        }
        projection.apply(&reply).expect("apply");
        projection
            .apply(&message_appended(2, "what is a walking skeleton?"))
            .expect("apply");
        projection
            .apply(&message_appended(3, "and a second question"))
            .expect("apply");

        assert_eq!(
            projection.sessions().expect("sessions")[0].preview,
            "what is a walking skeleton?"
        );
    }

    /// Recency is the last message, not the first: a session opened days ago
    /// and spoken in a minute ago is a recent session.
    #[test]
    fn a_session_reports_when_it_was_last_spoken_in() {
        let mut projection = Projection::open(":memory:").expect("open");
        projection.apply(&session_created(0)).expect("apply");
        assert_eq!(
            projection.sessions().expect("sessions")[0].last_at,
            None,
            "a session nobody has spoken in has no last message"
        );

        // `message_appended` stamps every event with the same fixed clock, so
        // move the second one forward by hand to make the MAX meaningful.
        projection
            .apply(&message_appended(1, "first"))
            .expect("apply");
        let mut later = message_appended(2, "second");
        later.ts = Some(Timestamp {
            seconds: 1_700_000_600,
            nanos: 0,
        });
        projection.apply(&later).expect("apply");

        let listed = &projection.sessions().expect("sessions")[0];
        assert_eq!(listed.last_at, Some(1_700_000_600_000_000));
        assert_ne!(
            listed.last_at, listed.started_at,
            "when it was opened is not when it was last used"
        );
    }

    #[test]
    fn a_session_with_no_title_lists_with_an_empty_one() {
        let mut projection = Projection::open(":memory:").expect("open");
        projection
            .apply(&session_created_as(0, "s-01", "", Some(1)))
            .expect("apply");

        let sessions = projection.sessions().expect("sessions");

        assert_eq!(sessions.len(), 1);
        assert_eq!(sessions[0].title, "");
    }

    #[test]
    fn an_unknown_session_and_an_empty_session_are_both_just_empty() {
        let mut projection = Projection::open(":memory:").expect("open");
        assert_eq!(projection.messages("s-01").expect("messages"), []);

        projection.apply(&session_created(0)).expect("apply");
        assert_eq!(projection.messages("s-01").expect("messages"), []);
    }

    #[test]
    fn messages_of_other_sessions_stay_out() {
        let mut projection = Projection::open(":memory:").expect("open");
        projection.apply(&session_created(0)).expect("apply");
        // Two sessions interleaved by seq: only s-01's rows may come back.
        for (seq, session, content) in [
            (1, "s-01", "a"),
            (2, "s-02", "x"),
            (3, "s-01", "b"),
            (4, "s-02", "y"),
        ] {
            let mut event = message_appended(seq, content);
            if let Some(event::Payload::Session(SessionEvent {
                event: Some(session_event::Event::MessageAppended(m)),
            })) = &mut event.payload
            {
                m.session_id = session.to_string();
            }
            projection.apply(&event).expect("apply");
        }

        let conv: Vec<String> = projection
            .messages("s-01")
            .expect("messages")
            .into_iter()
            .map(|(_, content)| content)
            .collect();
        assert_eq!(conv, ["a", "b"]);
    }

    #[test]
    fn an_event_without_payload_errors_and_leaves_the_index_usable() {
        let mut projection = Projection::open(":memory:").expect("open");
        projection.apply(&session_created(0)).expect("apply");

        let empty = Event {
            seq: 1,
            ts: Some(timestamp()),
            source: Source::System as i32,
            payload: None,
        };
        let err = projection
            .apply(&empty)
            .expect_err("payload: None must be refused");
        assert!(
            matches!(err, Error::MissingPayload { seq: 1 }),
            "got: {err:?}"
        );

        // No half-open transaction: the next apply commits normally.
        assert_eq!(projection.last_seq().expect("last_seq"), Some(0));
        projection
            .apply(&message_appended(1, "hello"))
            .expect("apply after the rejected event");
        assert_eq!(projection.last_seq().expect("last_seq"), Some(1));
        assert_eq!(row_count(&projection, "messages"), 1);
    }

    #[test]
    fn applying_the_same_event_twice_fails_and_rolls_back() {
        let mut projection = Projection::open(":memory:").expect("open");
        projection.apply(&session_created(0)).expect("apply");
        projection
            .apply(&message_appended(1, "hello"))
            .expect("apply");

        let err = projection
            .apply(&message_appended(1, "hello"))
            .expect_err("a duplicate seq must violate the primary key");
        assert!(matches!(err, Error::Sqlite(_)), "got: {err:?}");

        // Rolled back whole: no second row, and last_seq is untouched.
        assert_eq!(row_count(&projection, "messages"), 1);
        assert_eq!(projection.last_seq().expect("last_seq"), Some(1));
    }

    #[test]
    fn an_index_from_another_schema_version_is_refused() {
        let dir = TempDir::new().expect("temp dir");
        let path = dir.path().join("index.db");

        let projection = Projection::open(&path).expect("open");
        projection
            .conn
            .execute(
                "UPDATE projection_meta SET value = 99 WHERE key = 'schema_version'",
                [],
            )
            .expect("bump version");
        drop(projection);

        let err = Projection::open(&path).expect_err("a foreign schema version must be refused");
        assert!(
            matches!(err, Error::SchemaVersion { found: 99, .. }),
            "got: {err:?}"
        );
    }

    #[test]
    fn a_missing_timestamp_projects_as_null() {
        let mut projection = Projection::open(":memory:").expect("open");
        let mut event = session_created(0);
        event.ts = None;

        projection.apply(&event).expect("apply");

        let started_at: Option<i64> = projection
            .conn
            .query_row("SELECT started_at FROM sessions", [], |row| row.get(0))
            .optional()
            .expect("query")
            .flatten();
        assert_eq!(started_at, None);
    }

    // --- replay driver ---

    /// A log directory with one session and three messages (seqs 0..=3).
    /// The tiny segment cap forces several segments, so replay always crosses
    /// segment boundaries in these tests.
    fn build_log(dir: &Path) -> Log {
        let mut log = Log::open_with_max_segment_len(dir, 64).expect("open log");
        log.append(session_created(0)).expect("append");
        for (i, content) in ["alpha", "beta", "gamma"].iter().enumerate() {
            log.append(message_appended(i as u64 + 1, content))
                .expect("append");
        }
        log
    }

    /// Every user-visible row in the index, in deterministic order. Two
    /// projections are the same state exactly when their dumps are equal.
    fn dump(projection: &Projection) -> Vec<String> {
        let mut rows = Vec::new();
        let mut stmt = projection
            .conn
            .prepare(
                "SELECT 's', id, coalesce(title, ''), coalesce(started_at, -1)
                 FROM sessions ORDER BY id",
            )
            .expect("prepare");
        rows.extend(
            stmt.query_map([], |row| {
                Ok(format!(
                    "{}|{}|{}|{}",
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, i64>(3)?,
                ))
            })
            .expect("query")
            .map(|r| r.expect("row")),
        );
        let mut stmt = projection
            .conn
            .prepare(
                "SELECT 'm', session_id, seq, role, content, coalesce(ts, -1)
                 FROM messages ORDER BY seq",
            )
            .expect("prepare");
        rows.extend(
            stmt.query_map([], |row| {
                Ok(format!(
                    "{}|{}|{}|{}|{}|{}",
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, i64>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, i64>(5)?,
                ))
            })
            .expect("query")
            .map(|r| r.expect("row")),
        );
        rows
    }

    #[test]
    fn replay_is_deterministic_across_fresh_indexes() {
        let dir = TempDir::new().expect("temp dir");
        let log = build_log(dir.path());

        let mut first = Projection::open(":memory:").expect("open");
        let mut second = Projection::open(":memory:").expect("open");
        let stats = replay(log.reader().expect("reader"), &mut first).expect("replay");
        assert_eq!(
            stats,
            ReplayStats {
                applied: 4,
                skipped: 0
            },
            "a fresh index applies everything — seq 0 included"
        );
        replay(log.reader().expect("reader"), &mut second).expect("replay");

        assert_eq!(dump(&first), dump(&second));
        assert_eq!(first.last_seq().expect("last_seq"), Some(3));
    }

    #[test]
    fn a_second_replay_applies_nothing_and_changes_nothing() {
        let dir = TempDir::new().expect("temp dir");
        let log = build_log(dir.path());

        let mut projection = Projection::open(":memory:").expect("open");
        replay(log.reader().expect("reader"), &mut projection).expect("first replay");
        let before = dump(&projection);

        let stats = replay(log.reader().expect("reader"), &mut projection).expect("second replay");

        assert_eq!(
            stats,
            ReplayStats {
                applied: 0,
                skipped: 4
            }
        );
        assert_eq!(dump(&projection), before);
    }

    #[test]
    fn a_partial_replay_resumes_to_the_same_state_as_a_one_shot() {
        let dir = TempDir::new().expect("temp dir");
        let log = build_log(dir.path());

        // Replay only the first segment, as if the daemon crashed mid-rebuild.
        let segments = discover_segments(log.dir()).expect("discover");
        assert!(segments.len() > 1, "the test needs several segments");
        let mut resumed = Projection::open(":memory:").expect("open");
        let partial =
            replay(LogReader::new(segments[..1].to_vec()), &mut resumed).expect("partial replay");
        assert!(partial.applied < 4, "the partial replay must be partial");

        // Resuming over the full log completes it...
        let stats = replay(log.reader().expect("reader"), &mut resumed).expect("resume");
        assert_eq!(stats.applied + stats.skipped, 4);
        assert_eq!(stats.skipped, partial.applied, "resume skips what landed");

        // ...to exactly the state a one-shot replay produces.
        let mut one_shot = Projection::open(":memory:").expect("open");
        replay(log.reader().expect("reader"), &mut one_shot).expect("one-shot replay");
        assert_eq!(dump(&resumed), dump(&one_shot));
    }

    #[test]
    fn a_failed_apply_keeps_the_resume_point_and_a_later_replay_completes() {
        let dir = TempDir::new().expect("temp dir");
        let log = build_log(dir.path());

        // Sabotage: a hand-inserted row already occupies seq 2, so replay
        // applies 0 and 1, then hits a primary-key violation.
        let mut projection = Projection::open(":memory:").expect("open");
        projection
            .conn
            .execute(
                "INSERT INTO messages (session_id, seq, role, content) VALUES ('s-01', 2, 0, 'x')",
                [],
            )
            .expect("sabotage");

        let err = replay(log.reader().expect("reader"), &mut projection)
            .expect_err("the occupied seq must fail the replay");
        assert!(
            matches!(err, ReplayError::Projection { .. }),
            "got: {err:?}"
        );
        assert_eq!(
            projection.last_seq().expect("last_seq"),
            Some(1),
            "everything before the failure stays committed"
        );

        // Clear the sabotage; the next replay resumes past 1 and completes.
        projection
            .conn
            .execute("DELETE FROM messages WHERE seq = 2", [])
            .expect("clear sabotage");
        let stats = replay(log.reader().expect("reader"), &mut projection).expect("resume");
        assert_eq!(
            stats,
            ReplayStats {
                applied: 2,
                skipped: 2
            }
        );

        let mut one_shot = Projection::open(":memory:").expect("open");
        replay(log.reader().expect("reader"), &mut one_shot).expect("one-shot replay");
        assert_eq!(dump(&projection), dump(&one_shot));
    }
}
