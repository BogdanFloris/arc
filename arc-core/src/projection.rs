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

use arc_proto::v1::{
    Event, MemoryRecord, MemoryRecordCreated, MemoryRecordDeleted, MemoryRecordSuperseded,
    MemoryRecordUpdated, MessageAppended, Provenance, ProvenanceEntry, Role, SessionCreated,
    ToolCallIssued, ToolResultRecorded, event, memory_event, memory_record, session_event,
};
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
///
/// 2: tool-call and tool-result rows joined `messages`, with FTS5.
/// 3: `memory_records` joined — the distilled tier's state table (§5.2).
pub const SCHEMA_VERSION: u32 = 3;

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
/// rather than a duplicate row — on every row kind, tool rows included.
///
/// `messages` holds prose, tool calls and tool results in one table, told
/// apart by `kind` with per-kind nullable columns (DESIGN.md §3.1, "what the
/// projection will need"): one seq-ordered query rebuilds a session, and FTS
/// row-tagging is a join, not a union. `content` is the prose of a message,
/// the text of a result, and empty for a call — arguments are not indexed.
///
/// `messages_fts` is an external-content FTS5 table over `content`, rowid tied
/// to `seq`. It is written by explicit statements inside [`Projection::apply`]'s
/// transaction, never by triggers, so replay stays visible and deterministic.
///
/// `memory_records` is the distilled tier's current state (DESIGN.md §5.2):
/// one row per record, keyed by `id` and mutated by replay — records are
/// mutable state, so seq-as-PK does not apply here. `last_event_seq` is what
/// keeps double-apply failing anyway: created is a plain INSERT (a duplicate
/// id violates the PK), and every mutation guards on `last_event_seq <
/// event.seq`, treating zero affected rows as an error. `links` and
/// `provenance` are JSON text — nothing queries inside them yet, and the file
/// is disposable. `provenance` NULL means the field was absent, not empty.
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
    session_id     TEXT    NOT NULL,
    seq            INTEGER PRIMARY KEY,
    kind           INTEGER NOT NULL,
    turn_id        TEXT    NOT NULL,
    role           INTEGER,
    content        TEXT    NOT NULL,
    partial        INTEGER,
    call_id        TEXT,
    call_index     INTEGER,
    name           TEXT,
    arguments_json TEXT,
    outcome        INTEGER,
    truncated      INTEGER,
    ts             INTEGER
);

CREATE INDEX IF NOT EXISTS messages_by_session ON messages (session_id, seq);

CREATE VIRTUAL TABLE IF NOT EXISTS messages_fts USING fts5(
    content,
    content='messages',
    content_rowid='seq'
);

CREATE TABLE IF NOT EXISTS memory_records (
    id             TEXT    PRIMARY KEY,
    kind           INTEGER NOT NULL,
    namespace      TEXT    NOT NULL,
    title          TEXT    NOT NULL,
    summary        TEXT    NOT NULL,
    body           TEXT    NOT NULL,
    links          TEXT    NOT NULL,
    provenance     TEXT,
    status         INTEGER NOT NULL,
    superseded_by  TEXT,
    created_seq    INTEGER NOT NULL,
    last_event_seq INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS projection_meta (
    key   TEXT PRIMARY KEY,
    value
);
";

/// `messages.kind` for a prose row. Crate-visible: the archive query layer
/// filters on it, and a second definition would be a second authority.
pub(crate) const KIND_MESSAGE: i64 = 0;

/// `messages.kind` for a `ToolCallIssued` row.
const KIND_TOOL_CALL: i64 = 1;

/// `messages.kind` for a `ToolResultRecorded` row.
const KIND_TOOL_RESULT: i64 = 2;

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

    /// A memory event's guarded write touched zero rows: its target record is
    /// missing, or already carries this seq or a later one. Either way the
    /// event was fed out of order or twice — the driver's failure, surfaced
    /// loudly like a duplicate `messages` seq.
    #[error("memory event {seq}: record {id} is missing or not older than the event")]
    StaleMemoryEvent {
        /// Sequence number of the refused event.
        seq: u64,
        /// Id of the record the event targeted.
        id: String,
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

/// One row of [`Projection::messages`]: a session's transcript in the shapes
/// of `provider::Message`, not a struct of per-kind options.
///
/// `role` and `outcome` stay raw protobuf integers, unknown values included —
/// the module convention: the projection preserves, readers interpret (and
/// treat an unrecognized outcome as UNKNOWN, per DESIGN.md §3.1).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MessageRow {
    /// Prose one side said.
    Message {
        /// Speaker, as `MessageAppended` carried it.
        role: i32,
        /// The turn's text, verbatim.
        content: String,
        /// The reply was cut before the model finished.
        partial: bool,
        /// The turn this row belongs to. Empty on Phase 1 rows, which reads
        /// as "one message, one turn" (DESIGN.md §3.1).
        turn_id: String,
    },
    /// A tool call the model issued.
    ToolCall {
        /// The call's id, unique within its session.
        call_id: String,
        /// Position within its step, dense from 0.
        call_index: u32,
        /// The tool asked for.
        name: String,
        /// The arguments, one complete JSON object, verbatim.
        arguments_json: String,
        /// The turn this row belongs to.
        turn_id: String,
    },
    /// What the tool answered, addressed to `call_id`.
    ToolResult {
        /// The call this closes.
        call_id: String,
        /// Raw `ToolOutcome` integer.
        outcome: i32,
        /// What the model was shown, verbatim.
        content: String,
        /// The registry cut the result before recording it.
        truncated: bool,
        /// The turn this row belongs to.
        turn_id: String,
    },
}

/// One row of [`Projection::memory_index`]: what the always-loaded index of
/// ACTIVE records carries (DESIGN.md §5.2) — everything but the body.
///
/// `kind` stays a raw protobuf integer — the module convention: the
/// projection preserves, readers interpret.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemoryIndexEntry {
    /// Record id.
    pub id: String,
    /// "global" or a project id.
    pub namespace: String,
    /// Raw `MemoryRecord.Kind` integer.
    pub kind: i32,
    /// Record title.
    pub title: String,
    /// The one-line summary.
    pub summary: String,
}

/// What [`Projection::memory_record`] returns: the record as last written,
/// plus where replay says it went.
#[derive(Debug, Clone, PartialEq)]
pub struct MemoryRecordState {
    /// The record, rebuilt from columns; raw enum integers preserved.
    pub record: MemoryRecord,
    /// Id of the record that superseded this one, if one did.
    pub superseded_by: Option<String>,
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
    /// An event whose kind this build does not know — a `SessionEvent` or
    /// `MemoryEvent` oneof arm from a newer schema — writes no rows but still
    /// advances `last_seq`,
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
                Some(session_event::Event::ToolCallIssued(call)) => {
                    span.record("kind", "tool_call_issued");
                    insert_tool_call(&tx, event, call)?;
                }
                Some(session_event::Event::ToolResultRecorded(result)) => {
                    span.record("kind", "tool_result_recorded");
                    insert_tool_result(&tx, event, result)?;
                }
                None => {
                    span.record("kind", "unknown");
                    tracing::warn!(
                        seq = event.seq,
                        "session event of an unknown kind; skipping its rows"
                    );
                }
            },
            event::Payload::Memory(memory) => match memory.event.as_ref() {
                Some(memory_event::Event::RecordCreated(created)) => {
                    span.record("kind", "memory_record_created");
                    create_memory_record(&tx, event, created)?;
                }
                Some(memory_event::Event::RecordUpdated(updated)) => {
                    span.record("kind", "memory_record_updated");
                    update_memory_record(&tx, event, updated)?;
                }
                Some(memory_event::Event::RecordSuperseded(superseded)) => {
                    span.record("kind", "memory_record_superseded");
                    supersede_memory_record(&tx, event, superseded)?;
                }
                Some(memory_event::Event::RecordDeleted(deleted)) => {
                    span.record("kind", "memory_record_deleted");
                    delete_memory_record(&tx, event, deleted)?;
                }
                None => {
                    span.record("kind", "unknown");
                    tracing::warn!(
                        seq = event.seq,
                        "memory event of an unknown kind; skipping its rows"
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
        // `messages_by_session` makes each one an index seek. Both consider
        // only prose rows — a tool result is not a thing anyone said.
        let mut stmt = self.conn.prepare(
            "SELECT s.id, coalesce(s.title, ''), s.started_at,
                    coalesce((SELECT m.content FROM messages m
                              WHERE m.session_id = s.id AND m.kind = ?2 AND m.role = ?1
                              ORDER BY m.seq LIMIT 1), ''),
                    (SELECT MAX(m.ts) FROM messages m
                     WHERE m.session_id = s.id AND m.kind = ?2)
             FROM sessions s ORDER BY s.started_at, s.id",
        )?;
        let rows = stmt.query_map(rusqlite::params![Role::User as i32, KIND_MESSAGE], |row| {
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

    /// The conversation history of one session: every row kind, in seq order.
    ///
    /// # Errors
    ///
    /// [`Error::Sqlite`] if the index cannot be read, or holds a row this
    /// build cannot type — a `kind` it does not know, or a `call_index` no
    /// provider produces. Neither is reachable from an index this build
    /// wrote, so one means a hand-edited or foreign file.
    pub fn messages(&self, session_id: &str) -> Result<Vec<MessageRow>, Error> {
        let mut stmt = self.conn.prepare(
            "SELECT kind, role, content, partial, turn_id,
                    call_id, call_index, name, arguments_json, outcome, truncated
             FROM messages WHERE session_id = ?1 ORDER BY seq",
        )?;
        let rows = stmt.query_map([session_id], |row| match row.get::<_, i64>(0)? {
            KIND_MESSAGE => Ok(MessageRow::Message {
                role: row.get(1)?,
                content: row.get(2)?,
                partial: row.get(3)?,
                turn_id: row.get(4)?,
            }),
            KIND_TOOL_CALL => Ok(MessageRow::ToolCall {
                call_id: row.get(5)?,
                call_index: u32::try_from(row.get::<_, i64>(6)?)
                    .map_err(|_| bad_column(6, "call_index out of range"))?,
                name: row.get(7)?,
                arguments_json: row.get(8)?,
                turn_id: row.get(4)?,
            }),
            KIND_TOOL_RESULT => Ok(MessageRow::ToolResult {
                call_id: row.get(5)?,
                outcome: row.get(9)?,
                content: row.get(2)?,
                truncated: row.get(10)?,
                turn_id: row.get(4)?,
            }),
            other => Err(bad_column(0, &format!("unknown message kind {other}"))),
        })?;
        Ok(rows.collect::<Result<_, _>>()?)
    }

    /// The always-loaded index of the distilled tier: every ACTIVE record,
    /// bodies excluded (DESIGN.md §5.2).
    ///
    /// Ordered namespace → kind → title → id, so the index a session is
    /// primed with does not shuffle between calls.
    ///
    /// # Errors
    ///
    /// [`Error::Sqlite`] if the index cannot be read.
    pub fn memory_index(&self) -> Result<Vec<MemoryIndexEntry>, Error> {
        let mut stmt = self.conn.prepare(
            "SELECT id, namespace, kind, title, summary FROM memory_records
             WHERE status = ?1 ORDER BY namespace, kind, title, id",
        )?;
        let rows = stmt.query_map([memory_record::Status::Active as i32], |row| {
            Ok(MemoryIndexEntry {
                id: row.get(0)?,
                namespace: row.get(1)?,
                kind: row.get(2)?,
                title: row.get(3)?,
                summary: row.get(4)?,
            })
        })?;
        Ok(rows.collect::<Result<_, _>>()?)
    }

    /// One record, whole, whatever its status — a SUPERSEDED record is still
    /// readable ("you used to live at X"). `None` for an id the projection
    /// does not hold, deleted ones included: DELETED excludes entirely.
    ///
    /// # Errors
    ///
    /// [`Error::Sqlite`] if the index cannot be read, or holds `links` or
    /// `provenance` JSON this build cannot decode — not reachable from an
    /// index this build wrote, so one means a hand-edited or foreign file.
    pub fn memory_record(&self, id: &str) -> Result<Option<MemoryRecordState>, Error> {
        let row = self
            .conn
            .query_row(
                "SELECT kind, namespace, title, summary, body, links, provenance,
                        status, superseded_by
                 FROM memory_records WHERE id = ?1",
                [id],
                |row| {
                    Ok(MemoryRecordState {
                        record: MemoryRecord {
                            id: id.to_owned(),
                            kind: row.get(0)?,
                            namespace: row.get(1)?,
                            title: row.get(2)?,
                            summary: row.get(3)?,
                            body: row.get(4)?,
                            links: links_from_json(&row.get::<_, String>(5)?)
                                .map_err(|e| bad_json_column(5, &e))?,
                            provenance: provenance_from_json(
                                row.get::<_, Option<String>>(6)?.as_deref(),
                            )
                            .map_err(|e| bad_json_column(6, &e))?,
                            status: row.get(7)?,
                        },
                        superseded_by: row.get(8)?,
                    })
                },
            )
            .optional()?;
        Ok(row)
    }
}

/// A row value this build cannot type, as the `rusqlite` error the row mapper
/// must speak.
fn bad_column(index: usize, message: &str) -> rusqlite::Error {
    rusqlite::Error::FromSqlConversionFailure(
        index,
        rusqlite::types::Type::Integer,
        message.to_owned().into(),
    )
}

/// [`bad_column`], for a JSON text column that would not decode.
fn bad_json_column(index: usize, source: &serde_json::Error) -> rusqlite::Error {
    rusqlite::Error::FromSqlConversionFailure(
        index,
        rusqlite::types::Type::Text,
        source.to_string().into(),
    )
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

/// Writes the `messages` row for a `MessageAppended`, and its FTS row.
fn insert_message(
    tx: &Transaction<'_>,
    event: &Event,
    appended: &MessageAppended,
) -> Result<(), Error> {
    tx.execute(
        "INSERT INTO messages (session_id, seq, kind, turn_id, role, content, partial, ts)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
        rusqlite::params![
            &appended.session_id,
            seq_param(event.seq)?,
            KIND_MESSAGE,
            &appended.turn_id,
            // Raw enum integer, unknown values included.
            appended.role,
            &appended.content,
            appended.partial,
            epoch_micros(event.ts.as_ref()),
        ],
    )?;
    index_content(tx, event.seq, &appended.content)
}

/// Writes the `messages` row for a `ToolCallIssued`.
///
/// No FTS row: `content` is empty for a call, and arguments are deliberately
/// not indexed (DESIGN.md §3.1) — the call is findable by name.
fn insert_tool_call(
    tx: &Transaction<'_>,
    event: &Event,
    call: &ToolCallIssued,
) -> Result<(), Error> {
    tx.execute(
        "INSERT INTO messages
             (session_id, seq, kind, turn_id, content, call_id, call_index, name,
              arguments_json, ts)
         VALUES (?1, ?2, ?3, ?4, '', ?5, ?6, ?7, ?8, ?9)",
        rusqlite::params![
            &call.session_id,
            seq_param(event.seq)?,
            KIND_TOOL_CALL,
            &call.turn_id,
            &call.call_id,
            call.index,
            &call.name,
            &call.arguments_json,
            epoch_micros(event.ts.as_ref()),
        ],
    )?;
    Ok(())
}

/// Writes the `messages` row for a `ToolResultRecorded`, and its FTS row —
/// result text is searchable, tagged by `kind` through the join.
fn insert_tool_result(
    tx: &Transaction<'_>,
    event: &Event,
    result: &ToolResultRecorded,
) -> Result<(), Error> {
    tx.execute(
        "INSERT INTO messages
             (session_id, seq, kind, turn_id, content, call_id, outcome, truncated, ts)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
        rusqlite::params![
            &result.session_id,
            seq_param(event.seq)?,
            KIND_TOOL_RESULT,
            &result.turn_id,
            &result.content,
            &result.call_id,
            // Raw enum integer, unknown values included.
            result.outcome,
            result.truncated,
            epoch_micros(event.ts.as_ref()),
        ],
    )?;
    index_content(tx, event.seq, &result.content)
}

/// Adds one row's content to the FTS index, inside the caller's transaction.
///
/// Explicit statement, not a trigger: replay stays visible and deterministic,
/// and the write path stays this module's inserts and nothing else.
fn index_content(tx: &Transaction<'_>, seq: u64, content: &str) -> Result<(), Error> {
    tx.execute(
        "INSERT INTO messages_fts (rowid, content) VALUES (?1, ?2)",
        (seq_param(seq)?, content),
    )?;
    Ok(())
}

/// Writes the `memory_records` row for a `MemoryRecordCreated`.
///
/// A plain INSERT on purpose: a duplicate id is a constraint failure, the
/// state-table twin of `messages`' double-apply property. Status lands as
/// carried — sending ACTIVE is the writer's job.
fn create_memory_record(
    tx: &Transaction<'_>,
    event: &Event,
    created: &MemoryRecordCreated,
) -> Result<(), Error> {
    let Some(record) = created.record.as_ref() else {
        skip_recordless(event.seq, "created");
        return Ok(());
    };
    tx.execute(
        "INSERT INTO memory_records
             (id, kind, namespace, title, summary, body, links, provenance,
              status, superseded_by, created_seq, last_event_seq)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, NULL, ?10, ?10)",
        rusqlite::params![
            &record.id,
            // Raw enum integers, unknown values included — here and below.
            record.kind,
            &record.namespace,
            &record.title,
            &record.summary,
            &record.body,
            links_json(&record.links),
            provenance_json(record.provenance.as_ref()),
            record.status,
            seq_param(event.seq)?,
        ],
    )?;
    Ok(())
}

/// Overwrites the whole `memory_records` row for a `MemoryRecordUpdated` —
/// writes carry the whole record, never a diff, so replay is last write wins.
/// `superseded_by` clears with the overwrite; `created_seq` stays.
fn update_memory_record(
    tx: &Transaction<'_>,
    event: &Event,
    updated: &MemoryRecordUpdated,
) -> Result<(), Error> {
    let Some(record) = updated.record.as_ref() else {
        skip_recordless(event.seq, "updated");
        return Ok(());
    };
    let changed = tx.execute(
        "UPDATE memory_records
         SET kind = ?2, namespace = ?3, title = ?4, summary = ?5, body = ?6,
             links = ?7, provenance = ?8, status = ?9, superseded_by = NULL,
             last_event_seq = ?10
         WHERE id = ?1 AND last_event_seq < ?10",
        rusqlite::params![
            &record.id,
            record.kind,
            &record.namespace,
            &record.title,
            &record.summary,
            &record.body,
            links_json(&record.links),
            provenance_json(record.provenance.as_ref()),
            record.status,
            seq_param(event.seq)?,
        ],
    )?;
    if changed == 0 {
        return Err(Error::StaleMemoryEvent {
            seq: event.seq,
            id: record.id.clone(),
        });
    }
    Ok(())
}

/// Applies a `MemoryRecordSuperseded`: marks the old row SUPERSEDED, pointed
/// at its replacement, then upserts the replacement.
///
/// When the replacement reuses the superseded id, the row is simply replaced
/// with the new content — the log keeps the history in that case, the
/// projection doesn't.
fn supersede_memory_record(
    tx: &Transaction<'_>,
    event: &Event,
    superseded: &MemoryRecordSuperseded,
) -> Result<(), Error> {
    let Some(record) = superseded.record.as_ref() else {
        skip_recordless(event.seq, "superseded");
        return Ok(());
    };
    if record.id != superseded.superseded_id {
        let changed = tx.execute(
            "UPDATE memory_records
             SET status = ?2, superseded_by = ?3, last_event_seq = ?4
             WHERE id = ?1 AND last_event_seq < ?4",
            rusqlite::params![
                &superseded.superseded_id,
                memory_record::Status::Superseded as i32,
                &record.id,
                seq_param(event.seq)?,
            ],
        )?;
        if changed == 0 {
            if memory_record_exists(tx, &superseded.superseded_id)? {
                return Err(Error::StaleMemoryEvent {
                    seq: event.seq,
                    id: superseded.superseded_id.clone(),
                });
            }
            // A foreign log can supersede a record this projection never
            // held; the replacement still lands.
            tracing::warn!(
                seq = event.seq,
                id = %superseded.superseded_id,
                "supersede target not in the projection; upserting the replacement anyway"
            );
        }
    }
    upsert_memory_record(tx, event, record)
}

/// Inserts a supersede's replacement, or overwrites the row its id already
/// holds. `created_seq` survives an overwrite; `superseded_by` does not —
/// whatever the row was, it is now the fresh replacement.
fn upsert_memory_record(
    tx: &Transaction<'_>,
    event: &Event,
    record: &MemoryRecord,
) -> Result<(), Error> {
    let changed = tx.execute(
        "INSERT INTO memory_records
             (id, kind, namespace, title, summary, body, links, provenance,
              status, superseded_by, created_seq, last_event_seq)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, NULL, ?10, ?10)
         ON CONFLICT (id) DO UPDATE SET
             kind = excluded.kind, namespace = excluded.namespace,
             title = excluded.title, summary = excluded.summary,
             body = excluded.body, links = excluded.links,
             provenance = excluded.provenance, status = excluded.status,
             superseded_by = NULL, last_event_seq = excluded.last_event_seq
         WHERE memory_records.last_event_seq < excluded.last_event_seq",
        rusqlite::params![
            &record.id,
            record.kind,
            &record.namespace,
            &record.title,
            &record.summary,
            &record.body,
            links_json(&record.links),
            provenance_json(record.provenance.as_ref()),
            record.status,
            seq_param(event.seq)?,
        ],
    )?;
    if changed == 0 {
        return Err(Error::StaleMemoryEvent {
            seq: event.seq,
            id: record.id.clone(),
        });
    }
    Ok(())
}

/// Applies a `MemoryRecordDeleted`: removes the row outright, whatever its
/// status was — §5.2's purge, the one path that excludes a record entirely.
fn delete_memory_record(
    tx: &Transaction<'_>,
    event: &Event,
    deleted: &MemoryRecordDeleted,
) -> Result<(), Error> {
    let changed = tx.execute(
        "DELETE FROM memory_records WHERE id = ?1 AND last_event_seq < ?2",
        rusqlite::params![&deleted.id, seq_param(event.seq)?],
    )?;
    if changed == 0 {
        if memory_record_exists(tx, &deleted.id)? {
            return Err(Error::StaleMemoryEvent {
                seq: event.seq,
                id: deleted.id.clone(),
            });
        }
        // A foreign log can delete a record this projection never held.
        tracing::warn!(
            seq = event.seq,
            id = %deleted.id,
            "delete of an unknown memory record; nothing to remove"
        );
    }
    Ok(())
}

/// Whether `memory_records` holds `id` — what tells a stale guarded write
/// (an error) apart from a missing target (a warning).
fn memory_record_exists(tx: &Transaction<'_>, id: &str) -> Result<bool, Error> {
    let found: Option<i64> = tx
        .query_row("SELECT 1 FROM memory_records WHERE id = ?1", [id], |row| {
            row.get(0)
        })
        .optional()?;
    Ok(found.is_some())
}

/// A memory event whose oneof arm is known but whose record field is absent.
/// Skipped like an unknown kind: a foreign log must not fail wholesale.
fn skip_recordless(seq: u64, kind: &str) {
    tracing::warn!(
        seq,
        kind,
        "memory event carries no record; skipping its rows"
    );
}

/// The private JSON shape of one `provenance` entry. Prost types do not speak
/// serde, and the column format is this module's business alone.
#[derive(serde::Serialize, serde::Deserialize)]
struct ProvenanceEntryJson {
    session_id: String,
    ts: Option<TimestampJson>,
}

/// A protobuf timestamp kept whole — provenance must rebuild verbatim, so it
/// skips the lossy `epoch_micros` flattening the `messages` columns use.
#[derive(serde::Serialize, serde::Deserialize)]
struct TimestampJson {
    seconds: i64,
    nanos: i32,
}

fn links_json(links: &[String]) -> String {
    serde_json::to_string(links).expect("strings always serialize")
}

fn links_from_json(json: &str) -> Result<Vec<String>, serde_json::Error> {
    serde_json::from_str(json)
}

/// Provenance as the JSON its column holds. Absent stays absent (NULL), so
/// presence round-trips.
fn provenance_json(provenance: Option<&Provenance>) -> Option<String> {
    let entries: Vec<ProvenanceEntryJson> = provenance?
        .entries
        .iter()
        .map(|entry| ProvenanceEntryJson {
            session_id: entry.session_id.clone(),
            ts: entry.ts.as_ref().map(|ts| TimestampJson {
                seconds: ts.seconds,
                nanos: ts.nanos,
            }),
        })
        .collect();
    Some(serde_json::to_string(&entries).expect("strings and integers always serialize"))
}

fn provenance_from_json(json: Option<&str>) -> Result<Option<Provenance>, serde_json::Error> {
    let Some(json) = json else { return Ok(None) };
    let entries: Vec<ProvenanceEntryJson> = serde_json::from_str(json)?;
    Ok(Some(Provenance {
        entries: entries
            .into_iter()
            .map(|entry| ProvenanceEntry {
                session_id: entry.session_id,
                ts: entry.ts.map(|ts| Timestamp {
                    seconds: ts.seconds,
                    nanos: ts.nanos,
                }),
            })
            .collect(),
    }))
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
        Event, MemoryEvent, MemoryRecord, MemoryRecordCreated, MemoryRecordDeleted,
        MemoryRecordSuperseded, MemoryRecordUpdated, MessageAppended, Provenance, ProvenanceEntry,
        Role, SessionCreated, SessionEvent, Source, ToolCallIssued, ToolOutcome,
        ToolResultRecorded, event, memory_event, memory_record, session_event,
    };
    use prost_types::Timestamp;
    use rusqlite::OptionalExtension;
    use tempfile::TempDir;

    use super::{
        Error, MemoryIndexEntry, MessageRow, Projection, ReplayError, ReplayStats, SessionSummary,
        replay,
    };
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

    /// A `ToolCallIssued` in session `s-01`, turn `t-01`.
    fn tool_call(seq: u64, call_id: &str, index: u32, arguments: &str) -> Event {
        Event {
            seq,
            ts: Some(timestamp()),
            source: Source::Model as i32,
            payload: Some(event::Payload::Session(SessionEvent {
                event: Some(session_event::Event::ToolCallIssued(ToolCallIssued {
                    session_id: "s-01".to_string(),
                    turn_id: "t-01".to_string(),
                    call_id: call_id.to_string(),
                    index,
                    name: "lookup".to_string(),
                    arguments_json: arguments.to_string(),
                })),
            })),
        }
    }

    /// A `ToolResultRecorded` in session `s-01`, turn `t-01`. `outcome` stays
    /// a raw integer so tests can feed values this build does not know.
    fn tool_result(seq: u64, call_id: &str, outcome: i32, content: &str, truncated: bool) -> Event {
        Event {
            seq,
            ts: Some(timestamp()),
            source: Source::System as i32,
            payload: Some(event::Payload::Session(SessionEvent {
                event: Some(session_event::Event::ToolResultRecorded(
                    ToolResultRecorded {
                        session_id: "s-01".to_string(),
                        turn_id: "t-01".to_string(),
                        call_id: call_id.to_string(),
                        outcome,
                        content: content.to_string(),
                        truncated,
                    },
                )),
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

    /// Every table [`SCHEMA`](super::SCHEMA) creates. `messages_fts` being
    /// here is the proof the bundled `rusqlite` build ships FTS5 — creating
    /// the virtual table would fail without it. The `messages_fts_*` rows are
    /// its shadow tables (external content, so no `_content`).
    fn expected_tables() -> Vec<String> {
        [
            "memory_records",
            "messages",
            "messages_fts",
            "messages_fts_config",
            "messages_fts_data",
            "messages_fts_docsize",
            "messages_fts_idx",
            "projection_meta",
            "sessions",
        ]
        .into_iter()
        .map(str::to_string)
        .collect()
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

        assert_eq!(table_names(&projection), expected_tables());
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
        assert_eq!(table_names(&projection), expected_tables());
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

        let message: (String, i64, i64, i64, String, bool, Option<i64>) = projection
            .conn
            .query_row(
                "SELECT session_id, seq, kind, role, content, partial, ts FROM messages",
                [],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                        row.get(5)?,
                        row.get(6)?,
                    ))
                },
            )
            .expect("message row");
        assert_eq!(
            message,
            (
                "s-01".to_string(),
                1,
                super::KIND_MESSAGE,
                i64::from(Role::User as i32),
                "hello".to_string(),
                false,
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
        for (i, row) in conv.iter().enumerate() {
            let MessageRow::Message { role, content, .. } = row else {
                panic!("expected a prose row, got {row:?}");
            };
            assert_eq!(*role, Role::User as i32);
            assert_eq!(content, &format!("hello_{}", i + 1));
        }
    }

    /// One tool turn's rows, typed: the columns DESIGN.md §3.1 says the
    /// projection needs, kind by kind.
    #[test]
    fn a_tool_turn_projects_call_and_result_rows() {
        let mut projection = Projection::open(":memory:").expect("open");
        projection.apply(&session_created(0)).expect("apply");
        let mut asked = message_appended(1, "look this up");
        if let Some(event::Payload::Session(SessionEvent {
            event: Some(session_event::Event::MessageAppended(m)),
        })) = &mut asked.payload
        {
            m.turn_id = "t-01".to_string();
        }
        projection.apply(&asked).expect("apply");
        projection
            .apply(&tool_call(2, "c-a", 0, r#"{"q":1}"#))
            .expect("apply");
        projection
            .apply(&tool_result(
                3,
                "c-a",
                ToolOutcome::Ok as i32,
                "found it",
                false,
            ))
            .expect("apply");

        assert_eq!(
            projection.messages("s-01").expect("messages"),
            [
                MessageRow::Message {
                    role: Role::User as i32,
                    content: "look this up".to_string(),
                    partial: false,
                    turn_id: "t-01".to_string(),
                },
                MessageRow::ToolCall {
                    call_id: "c-a".to_string(),
                    call_index: 0,
                    name: "lookup".to_string(),
                    arguments_json: r#"{"q":1}"#.to_string(),
                    turn_id: "t-01".to_string(),
                },
                MessageRow::ToolResult {
                    call_id: "c-a".to_string(),
                    outcome: ToolOutcome::Ok as i32,
                    content: "found it".to_string(),
                    truncated: false,
                    turn_id: "t-01".to_string(),
                },
            ]
        );
    }

    /// The flags and the raw outcome integer must survive a log-in →
    /// state-out replay, unknown enum values included: the projection
    /// preserves, readers interpret.
    #[test]
    fn partial_truncated_and_unknown_outcomes_survive_replay() {
        let dir = TempDir::new().expect("temp dir");
        let mut log = Log::open(dir.path()).expect("open log");
        log.append(session_created(0)).expect("append");
        let mut cut = message_appended(1, "half a th");
        if let Some(event::Payload::Session(SessionEvent {
            event: Some(session_event::Event::MessageAppended(m)),
        })) = &mut cut.payload
        {
            m.partial = true;
        }
        log.append(cut).expect("append");
        log.append(tool_call(2, "c-a", 0, "{}")).expect("append");
        // Outcome 99 is from a newer schema; it must come back verbatim.
        log.append(tool_result(3, "c-a", 99, "cut resul [truncated]", true))
            .expect("append");

        let mut projection = Projection::open(":memory:").expect("open");
        replay(log.reader().expect("reader"), &mut projection).expect("replay");

        let rows = projection.messages("s-01").expect("messages");
        assert_eq!(
            rows[0],
            MessageRow::Message {
                role: Role::User as i32,
                content: "half a th".to_string(),
                partial: true,
                turn_id: String::new(),
            }
        );
        assert_eq!(
            rows[2],
            MessageRow::ToolResult {
                call_id: "c-a".to_string(),
                outcome: 99,
                content: "cut resul [truncated]".to_string(),
                truncated: true,
                turn_id: "t-01".to_string(),
            }
        );
    }

    /// FTS finds prose and result text, and the `kind` join is what excludes
    /// result rows — the filter `sessions_search` will default to (§3.1's
    /// open FTS question, proven answerable here).
    #[test]
    fn fts_matches_prose_and_results_and_kind_filters_them_apart() {
        let mut projection = Projection::open(":memory:").expect("open");
        projection.apply(&session_created(0)).expect("apply");
        projection
            .apply(&message_appended(1, "what is a walking skeleton"))
            .expect("apply");
        projection
            .apply(&tool_call(2, "c-a", 0, r#"{"needle":"argsecret"}"#))
            .expect("apply");
        projection
            .apply(&tool_result(
                3,
                "c-a",
                ToolOutcome::Ok as i32,
                "directory listing gruvbox",
                false,
            ))
            .expect("apply");

        let matches = |query: &str, kind: Option<i64>| -> Vec<i64> {
            let mut stmt = projection
                .conn
                .prepare(
                    "SELECT m.seq FROM messages_fts f JOIN messages m ON m.seq = f.rowid
                     WHERE messages_fts MATCH ?1 AND (?2 IS NULL OR m.kind = ?2)
                     ORDER BY m.seq",
                )
                .expect("prepare");
            stmt.query_map(rusqlite::params![query, kind], |row| row.get(0))
                .expect("query")
                .collect::<Result<Vec<i64>, _>>()
                .expect("rows")
        };

        assert_eq!(matches("skeleton", None), [1], "prose is indexed");
        assert_eq!(matches("gruvbox", None), [3], "result text is indexed");
        assert_eq!(
            matches("gruvbox", Some(super::KIND_MESSAGE)),
            [0i64; 0],
            "the kind join excludes the result row"
        );
        assert_eq!(
            matches("argsecret", None),
            [0i64; 0],
            "call arguments are not indexed"
        );
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
            .map(|row| match row {
                MessageRow::Message { content, .. } => content,
                other => panic!("expected a prose row, got {other:?}"),
            })
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

    /// Seq is the primary key on tool rows too, and the rollback covers the
    /// FTS insert that rides in the same transaction.
    #[test]
    fn double_applying_a_tool_row_fails_and_rolls_back_its_fts_row() {
        let mut projection = Projection::open(":memory:").expect("open");
        projection.apply(&session_created(0)).expect("apply");
        projection
            .apply(&tool_result(
                1,
                "c-a",
                ToolOutcome::Ok as i32,
                "gruvbox",
                false,
            ))
            .expect("apply");

        let err = projection
            .apply(&tool_result(
                1,
                "c-a",
                ToolOutcome::Ok as i32,
                "gruvbox",
                false,
            ))
            .expect_err("a duplicate seq must violate the primary key");
        assert!(matches!(err, Error::Sqlite(_)), "got: {err:?}");

        assert_eq!(row_count(&projection, "messages"), 1);
        let fts_hits: i64 = projection
            .conn
            .query_row(
                "SELECT count(*) FROM messages_fts WHERE messages_fts MATCH 'gruvbox'",
                [],
                |row| row.get(0),
            )
            .expect("fts count");
        assert_eq!(fts_hits, 1, "the second FTS insert rolled back");
        assert_eq!(projection.last_seq().expect("last_seq"), Some(1));
    }

    /// Version 1 is what a pre-tool-rows daemon left behind; open must
    /// refuse it (arcd then deletes the file and re-projects).
    #[test]
    fn an_index_from_another_schema_version_is_refused() {
        let dir = TempDir::new().expect("temp dir");
        let path = dir.path().join("index.db");

        let projection = Projection::open(&path).expect("open");
        projection
            .conn
            .execute(
                "UPDATE projection_meta SET value = 1 WHERE key = 'schema_version'",
                [],
            )
            .expect("age the version");
        drop(projection);

        let err = Projection::open(&path).expect_err("a foreign schema version must be refused");
        assert!(
            matches!(err, Error::SchemaVersion { found: 1, .. }),
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

    /// A log directory with one session, three messages, and a tool exchange
    /// (seqs 0..=5). The tiny segment cap forces several segments, so replay
    /// always crosses segment boundaries in these tests.
    fn build_log(dir: &Path) -> Log {
        let mut log = Log::open_with_max_segment_len(dir, 64).expect("open log");
        log.append(session_created(0)).expect("append");
        for (i, content) in ["alpha", "beta", "gamma"].iter().enumerate() {
            log.append(message_appended(i as u64 + 1, content))
                .expect("append");
        }
        log.append(tool_call(4, "c-a", 0, "{}")).expect("append");
        log.append(tool_result(
            5,
            "c-a",
            ToolOutcome::Ok as i32,
            "found",
            false,
        ))
        .expect("append");
        log
    }

    /// How many events [`build_log`] holds.
    const BUILT_EVENTS: u64 = 6;

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
                "SELECT 'm', session_id, seq, kind, coalesce(role, -1), content,
                        coalesce(call_id, ''), coalesce(outcome, -1), coalesce(ts, -1)
                 FROM messages ORDER BY seq",
            )
            .expect("prepare");
        rows.extend(
            stmt.query_map([], |row| {
                Ok(format!(
                    "{}|{}|{}|{}|{}|{}|{}|{}|{}",
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, i64>(3)?,
                    row.get::<_, i64>(4)?,
                    row.get::<_, String>(5)?,
                    row.get::<_, String>(6)?,
                    row.get::<_, i64>(7)?,
                    row.get::<_, i64>(8)?,
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
                applied: BUILT_EVENTS,
                skipped: 0
            },
            "a fresh index applies everything — seq 0 included"
        );
        replay(log.reader().expect("reader"), &mut second).expect("replay");

        assert_eq!(dump(&first), dump(&second));
        assert_eq!(first.last_seq().expect("last_seq"), Some(BUILT_EVENTS - 1));
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
                skipped: BUILT_EVENTS
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
        assert!(
            partial.applied < BUILT_EVENTS,
            "the partial replay must be partial"
        );

        // Resuming over the full log completes it...
        let stats = replay(log.reader().expect("reader"), &mut resumed).expect("resume");
        assert_eq!(stats.applied + stats.skipped, BUILT_EVENTS);
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
                "INSERT INTO messages (session_id, seq, kind, turn_id, role, content)
                 VALUES ('s-01', 2, 0, '', 0, 'x')",
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
                applied: BUILT_EVENTS - 2,
                skipped: 2
            }
        );

        let mut one_shot = Projection::open(":memory:").expect("open");
        replay(log.reader().expect("reader"), &mut one_shot).expect("one-shot replay");
        assert_eq!(dump(&projection), dump(&one_shot));
    }

    // --- memory records ---

    /// An ACTIVE record with a full provenance entry, so round-trips cover
    /// every field — the timestamp's nanos included.
    fn record(id: &str, title: &str, summary: &str) -> MemoryRecord {
        MemoryRecord {
            id: id.to_string(),
            kind: memory_record::Kind::Fact as i32,
            namespace: "global".to_string(),
            title: title.to_string(),
            summary: summary.to_string(),
            body: format!("{title}, at length"),
            links: vec!["mr-linked".to_string()],
            provenance: Some(Provenance {
                entries: vec![ProvenanceEntry {
                    session_id: "s-01".to_string(),
                    ts: Some(timestamp()),
                }],
            }),
            status: memory_record::Status::Active as i32,
        }
    }

    fn memory(seq: u64, event: memory_event::Event) -> Event {
        Event {
            seq,
            ts: Some(timestamp()),
            source: Source::Model as i32,
            payload: Some(event::Payload::Memory(MemoryEvent { event: Some(event) })),
        }
    }

    fn mem_created(seq: u64, record: MemoryRecord) -> Event {
        memory(
            seq,
            memory_event::Event::RecordCreated(MemoryRecordCreated {
                record: Some(record),
            }),
        )
    }

    fn mem_updated(seq: u64, record: MemoryRecord) -> Event {
        memory(
            seq,
            memory_event::Event::RecordUpdated(MemoryRecordUpdated {
                record: Some(record),
            }),
        )
    }

    fn mem_superseded(seq: u64, superseded_id: &str, record: MemoryRecord) -> Event {
        memory(
            seq,
            memory_event::Event::RecordSuperseded(MemoryRecordSuperseded {
                superseded_id: superseded_id.to_string(),
                record: Some(record),
            }),
        )
    }

    fn mem_deleted(seq: u64, id: &str) -> Event {
        memory(
            seq,
            memory_event::Event::RecordDeleted(MemoryRecordDeleted { id: id.to_string() }),
        )
    }

    /// The `memory_event::Event` inside one of the constructors above, in the
    /// shape `testkit::seed_memory_log` seeds.
    fn memory_payload(event: Event) -> memory_event::Event {
        match event.payload {
            Some(event::Payload::Memory(MemoryEvent { event: Some(inner) })) => inner,
            other => panic!("expected a memory payload, got {other:?}"),
        }
    }

    /// The ids [`Projection::memory_index`] lists, in its order.
    fn index_ids(projection: &Projection) -> Vec<String> {
        projection
            .memory_index()
            .expect("memory_index")
            .into_iter()
            .map(|entry| entry.id)
            .collect()
    }

    #[test]
    fn a_created_record_reads_back_whole_and_indexed() {
        let mut projection = Projection::open(":memory:").expect("open");
        let created = record("mr-a", "gruvbox", "the palette");

        projection
            .apply(&mem_created(0, created.clone()))
            .expect("apply");

        let state = projection
            .memory_record("mr-a")
            .expect("memory_record")
            .expect("the record exists");
        assert_eq!(state.record, created, "every field round-trips");
        assert_eq!(state.superseded_by, None);
        assert_eq!(
            projection.memory_index().expect("memory_index"),
            [MemoryIndexEntry {
                id: "mr-a".to_string(),
                namespace: "global".to_string(),
                kind: memory_record::Kind::Fact as i32,
                title: "gruvbox".to_string(),
                summary: "the palette".to_string(),
            }]
        );
    }

    #[test]
    fn an_update_overwrites_the_whole_record() {
        let mut projection = Projection::open(":memory:").expect("open");
        projection
            .apply(&mem_created(0, record("mr-a", "old title", "old summary")))
            .expect("apply");

        let mut newer = record("mr-a", "new title", "new summary");
        newer.kind = memory_record::Kind::Preference as i32;
        newer.links = vec![];
        newer.provenance = None;
        projection
            .apply(&mem_updated(1, newer.clone()))
            .expect("apply");

        let state = projection
            .memory_record("mr-a")
            .expect("memory_record")
            .expect("the record exists");
        assert_eq!(state.record, newer, "last write wins, field by field");
        assert_eq!(
            index_ids(&projection),
            ["mr-a"],
            "still one record, under the new title"
        );
    }

    #[test]
    fn an_update_of_a_missing_record_is_refused() {
        let mut projection = Projection::open(":memory:").expect("open");

        let err = projection
            .apply(&mem_updated(0, record("mr-none", "t", "s")))
            .expect_err("nothing to overwrite");

        assert!(
            matches!(err, Error::StaleMemoryEvent { seq: 0, ref id } if id == "mr-none"),
            "got: {err:?}"
        );
        assert_eq!(
            projection.last_seq().expect("last_seq"),
            None,
            "rolled back"
        );
    }

    #[test]
    fn a_supersede_retires_the_old_row_and_lands_the_replacement() {
        let mut projection = Projection::open(":memory:").expect("open");
        projection
            .apply(&mem_created(0, record("mr-a", "old address", "lives at X")))
            .expect("apply");

        let replacement = record("mr-b", "new address", "lives at Y");
        projection
            .apply(&mem_superseded(1, "mr-a", replacement.clone()))
            .expect("apply");

        assert_eq!(
            index_ids(&projection),
            ["mr-b"],
            "only the replacement is ACTIVE"
        );
        let old = projection
            .memory_record("mr-a")
            .expect("memory_record")
            .expect("SUPERSEDED is still readable");
        assert_eq!(old.record.status, memory_record::Status::Superseded as i32);
        assert_eq!(old.superseded_by, Some("mr-b".to_string()));
        assert_eq!(
            old.record.summary, "lives at X",
            "the old content survives for history"
        );
        let new = projection
            .memory_record("mr-b")
            .expect("memory_record")
            .expect("the replacement exists");
        assert_eq!(new.record, replacement);
        assert_eq!(new.superseded_by, None);
    }

    /// The proto allows the replacement to reuse the superseded id; the row
    /// is then simply replaced — the log keeps the history, not the table.
    #[test]
    fn a_supersede_reusing_the_id_replaces_the_row_in_place() {
        let mut projection = Projection::open(":memory:").expect("open");
        projection
            .apply(&mem_created(0, record("mr-a", "old address", "lives at X")))
            .expect("apply");

        let replacement = record("mr-a", "new address", "lives at Y");
        projection
            .apply(&mem_superseded(1, "mr-a", replacement.clone()))
            .expect("apply");

        assert_eq!(index_ids(&projection), ["mr-a"], "the row stays ACTIVE");
        let state = projection
            .memory_record("mr-a")
            .expect("memory_record")
            .expect("the record exists");
        assert_eq!(state.record, replacement);
        assert_eq!(state.superseded_by, None);
    }

    #[test]
    fn a_delete_excludes_the_record_entirely() {
        let mut projection = Projection::open(":memory:").expect("open");
        projection
            .apply(&mem_created(0, record("mr-a", "t", "s")))
            .expect("apply");

        projection.apply(&mem_deleted(1, "mr-a")).expect("apply");

        assert_eq!(
            projection.memory_record("mr-a").expect("memory_record"),
            None
        );
        assert_eq!(index_ids(&projection), [""; 0]);
        assert_eq!(projection.last_seq().expect("last_seq"), Some(1));
    }

    /// §5.2's purge applies whatever the status was: deleting a SUPERSEDED
    /// record removes the history row and leaves its replacement alone.
    #[test]
    fn deleting_a_superseded_record_removes_it_and_spares_the_replacement() {
        let mut projection = Projection::open(":memory:").expect("open");
        projection
            .apply(&mem_created(0, record("mr-a", "old", "s")))
            .expect("apply");
        projection
            .apply(&mem_superseded(1, "mr-a", record("mr-b", "new", "s")))
            .expect("apply");

        projection.apply(&mem_deleted(2, "mr-a")).expect("apply");

        assert_eq!(
            projection.memory_record("mr-a").expect("memory_record"),
            None
        );
        assert_eq!(index_ids(&projection), ["mr-b"]);
    }

    /// A foreign log can supersede a record this log never created; the
    /// replacement must still land (with a warning), not fail the replay.
    #[test]
    fn a_supersede_of_a_missing_target_still_lands_the_replacement() {
        let mut projection = Projection::open(":memory:").expect("open");

        projection
            .apply(&mem_superseded(0, "mr-ghost", record("mr-b", "t", "s")))
            .expect("apply");

        assert_eq!(
            projection.memory_record("mr-ghost").expect("memory_record"),
            None
        );
        assert_eq!(index_ids(&projection), ["mr-b"]);
    }

    /// Same replay-safety rule for the other anomaly: deleting what the
    /// projection never held warns, no-ops, and advances `last_seq`.
    #[test]
    fn a_delete_of_an_unknown_record_warns_and_no_ops() {
        let mut projection = Projection::open(":memory:").expect("open");

        projection
            .apply(&mem_deleted(0, "mr-ghost"))
            .expect("apply");

        assert_eq!(projection.last_seq().expect("last_seq"), Some(0));
        assert_eq!(index_ids(&projection), [""; 0]);
    }

    #[test]
    fn double_applying_a_create_fails_and_rolls_back() {
        let mut projection = Projection::open(":memory:").expect("open");
        projection
            .apply(&mem_created(0, record("mr-a", "t", "s")))
            .expect("apply");

        let err = projection
            .apply(&mem_created(0, record("mr-a", "t", "s")))
            .expect_err("a duplicate id must violate the primary key");

        assert!(matches!(err, Error::Sqlite(_)), "got: {err:?}");
        assert_eq!(row_count(&projection, "memory_records"), 1);
        assert_eq!(projection.last_seq().expect("last_seq"), Some(0));
    }

    #[test]
    fn double_applying_an_update_fails_and_rolls_back() {
        let mut projection = Projection::open(":memory:").expect("open");
        projection
            .apply(&mem_created(0, record("mr-a", "t", "s")))
            .expect("apply");
        projection
            .apply(&mem_updated(1, record("mr-a", "t2", "s2")))
            .expect("apply");

        let err = projection
            .apply(&mem_updated(1, record("mr-a", "t3", "s3")))
            .expect_err("the seq guard must refuse a replayed update");

        assert!(
            matches!(err, Error::StaleMemoryEvent { seq: 1, .. }),
            "got: {err:?}"
        );
        let state = projection
            .memory_record("mr-a")
            .expect("memory_record")
            .expect("the record exists");
        assert_eq!(state.record.title, "t2", "the refused write left no trace");
        assert_eq!(projection.last_seq().expect("last_seq"), Some(1));
    }

    #[test]
    fn double_applying_a_supersede_fails_and_rolls_back() {
        let mut projection = Projection::open(":memory:").expect("open");
        projection
            .apply(&mem_created(0, record("mr-a", "old", "s")))
            .expect("apply");
        projection
            .apply(&mem_superseded(1, "mr-a", record("mr-b", "new", "s")))
            .expect("apply");

        // Distinct ids: the old row's guard refuses first.
        let err = projection
            .apply(&mem_superseded(1, "mr-a", record("mr-b", "newer", "s")))
            .expect_err("the seq guard must refuse a replayed supersede");
        assert!(
            matches!(err, Error::StaleMemoryEvent { seq: 1, .. }),
            "got: {err:?}"
        );

        // Same id: the upsert's own guard refuses.
        projection
            .apply(&mem_superseded(2, "mr-b", record("mr-b", "renewed", "s")))
            .expect("apply");
        let err = projection
            .apply(&mem_superseded(2, "mr-b", record("mr-b", "again", "s")))
            .expect_err("the upsert guard must refuse a replayed same-id supersede");
        assert!(
            matches!(err, Error::StaleMemoryEvent { seq: 2, .. }),
            "got: {err:?}"
        );

        let state = projection
            .memory_record("mr-b")
            .expect("memory_record")
            .expect("the record exists");
        assert_eq!(
            state.record.title, "renewed",
            "the refused writes left no trace"
        );
        assert_eq!(projection.last_seq().expect("last_seq"), Some(2));
    }

    /// Delete's double-apply is indistinguishable from the unknown-id
    /// anomaly — the row is gone either way — so it takes the anomaly path:
    /// warn and no-op, never a spurious resurrection or failure.
    #[test]
    fn double_applying_a_delete_warns_and_no_ops() {
        let mut projection = Projection::open(":memory:").expect("open");
        projection
            .apply(&mem_created(0, record("mr-a", "t", "s")))
            .expect("apply");
        projection.apply(&mem_deleted(1, "mr-a")).expect("apply");

        projection
            .apply(&mem_deleted(1, "mr-a"))
            .expect("a re-delete no-ops");

        assert_eq!(
            projection.memory_record("mr-a").expect("memory_record"),
            None
        );
        assert_eq!(projection.last_seq().expect("last_seq"), Some(1));
    }

    /// Enum integers this build does not know come back verbatim — the
    /// projection preserves, readers interpret — and an unknown status is
    /// simply not ACTIVE, so it stays out of the index.
    #[test]
    fn unknown_kind_and_status_ints_survive_verbatim() {
        let mut projection = Projection::open(":memory:").expect("open");
        let mut foreign = record("mr-a", "t", "s");
        foreign.kind = 42;
        foreign.status = 9;

        projection
            .apply(&mem_created(0, foreign.clone()))
            .expect("apply");

        let state = projection
            .memory_record("mr-a")
            .expect("memory_record")
            .expect("the record exists");
        assert_eq!(state.record, foreign);
        assert_eq!(index_ids(&projection), [""; 0], "status 9 is not ACTIVE");
    }

    #[test]
    fn an_unknown_memory_event_kind_advances_last_seq_without_writing_rows() {
        let mut projection = Projection::open(":memory:").expect("open");

        let foreign = Event {
            seq: 0,
            ts: Some(timestamp()),
            source: Source::Model as i32,
            payload: Some(event::Payload::Memory(MemoryEvent { event: None })),
        };
        projection.apply(&foreign).expect("apply");

        assert_eq!(row_count(&projection, "memory_records"), 0);
        assert_eq!(projection.last_seq().expect("last_seq"), Some(0));
    }

    /// One log, both tiers: session and memory events interleaved project
    /// into their own tables without crosstalk — seeded through the testkit,
    /// so the seeder is exercised where the memory tests live.
    #[test]
    fn a_mixed_log_projects_both_tiers() {
        let dir = TempDir::new().expect("temp dir");
        crate::testkit::seed_log_payloads(
            &dir,
            vec![
                session_created(0).payload.expect("payload"),
                mem_created(0, record("mr-a", "t", "s"))
                    .payload
                    .expect("payload"),
                message_appended(0, "hello").payload.expect("payload"),
                mem_updated(0, record("mr-a", "t2", "s2"))
                    .payload
                    .expect("payload"),
            ],
        );

        let mut projection = Projection::open(":memory:").expect("open");
        let segments = discover_segments(dir.path()).expect("discover");
        let stats = replay(LogReader::new(segments), &mut projection).expect("replay");

        assert_eq!(stats.applied, 4);
        assert_eq!(projection.sessions().expect("sessions").len(), 1);
        assert_eq!(projection.messages("s-01").expect("messages").len(), 1);
        assert_eq!(index_ids(&projection), ["mr-a"]);
        assert_eq!(
            projection
                .memory_record("mr-a")
                .expect("memory_record")
                .expect("the record exists")
                .record
                .title,
            "t2"
        );
    }

    #[test]
    fn memory_replay_is_deterministic_across_fresh_indexes() {
        let dir = TempDir::new().expect("temp dir");
        crate::testkit::seed_memory_log(
            &dir,
            vec![
                memory_payload(mem_created(0, record("mr-a", "old address", "lives at X"))),
                memory_payload(mem_created(0, record("mr-c", "keeper", "stays put"))),
                memory_payload(mem_superseded(
                    0,
                    "mr-a",
                    record("mr-b", "new address", "lives at Y"),
                )),
                memory_payload(mem_deleted(0, "mr-c")),
            ],
        );

        let mut first = Projection::open(":memory:").expect("open");
        let mut second = Projection::open(":memory:").expect("open");
        let segments = discover_segments(dir.path()).expect("discover");
        replay(LogReader::new(segments.clone()), &mut first).expect("replay");
        replay(LogReader::new(segments), &mut second).expect("replay");

        assert_eq!(
            first.memory_index().expect("memory_index"),
            second.memory_index().expect("memory_index")
        );
        for id in ["mr-a", "mr-b", "mr-c"] {
            assert_eq!(
                first.memory_record(id).expect("memory_record"),
                second.memory_record(id).expect("memory_record"),
                "record {id} differs between replays"
            );
        }
        assert_eq!(index_ids(&first), ["mr-b"]);
    }
}
