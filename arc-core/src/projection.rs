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

use arc_proto::v1::{Event, MessageAppended, SessionCreated, event, session_event};
use prost_types::Timestamp;
use rusqlite::{Connection, OptionalExtension, Transaction};

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
                None => {
                    span.record("kind", "unknown");
                    tracing::warn!(
                        seq = event.seq,
                        "session event of an unknown kind; skipping its rows"
                    );
                }
            },
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
    use arc_proto::v1::{
        Event, MessageAppended, Role, SessionCreated, SessionEvent, Source, event, session_event,
    };
    use prost_types::Timestamp;
    use rusqlite::OptionalExtension;
    use tempfile::TempDir;

    use super::{Error, Projection};

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
}
