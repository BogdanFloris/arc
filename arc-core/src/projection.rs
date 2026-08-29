use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard, PoisonError};

use arc_proto::v1::{
    Event, HistoryEntry, HistoryMessage, HistoryToolCall, HistoryToolResult, MemoryEvent,
    MemoryRecord, MemoryRecordCreated, MemoryRecordDeleted, MemoryRecordReviewed,
    MemoryRecordSuperseded, MemoryRecordUpdated, MessageAppended, Provenance, ProvenanceEntry,
    Role, SessionConsolidated, SessionCreated, SessionEvent, SessionTitled, ToolCallIssued,
    ToolResultRecorded, event, history_entry, memory_event, memory_record, session_event,
};
use prost_types::Timestamp;
use rusqlite::{Connection, OpenFlags, OptionalExtension, Transaction};

use crate::log;

// bump on any SCHEMA change; the daemon deletes the index and replays
// 9: messages gained the source column
// 10: messages gained input_tokens, output_tokens, elapsed_ms
// 11: sessions gained provider, model columns
// 12: sessions gained the source column
// 13: memory_records gained created_at, superseded_at
pub(crate) const SCHEMA_VERSION: u32 = 13;

const LAST_SEQ_KEY: &str = "last_seq";

const SCHEMA_VERSION_KEY: &str = "schema_version";

const SCHEMA: &str = "\
CREATE TABLE IF NOT EXISTS sessions (
    id             TEXT PRIMARY KEY,
    parent_session TEXT,
    fork_point     INTEGER,
    project        TEXT,
    title          TEXT,
    started_at     INTEGER,
    consolidated_through INTEGER,
    role           INTEGER NOT NULL DEFAULT 0,
    provider       TEXT,
    model          TEXT,
    source         INTEGER NOT NULL DEFAULT 0
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
    ts             INTEGER,
    provider_roundtrip BLOB,
    source         INTEGER,
    input_tokens   INTEGER,
    output_tokens  INTEGER,
    elapsed_ms     INTEGER
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
    last_event_seq INTEGER NOT NULL,
    changed_at     INTEGER,
    reviewed_at    INTEGER,
    created_at     INTEGER,
    superseded_at  INTEGER
);

CREATE TABLE IF NOT EXISTS projection_meta (
    key   TEXT PRIMARY KEY,
    value
);

CREATE TABLE IF NOT EXISTS session_grants (
    session_id TEXT    NOT NULL,
    root       TEXT    NOT NULL,
    read_write INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS session_grants_by_session ON session_grants (session_id);
";

pub(crate) const KIND_MESSAGE: i64 = 0;

const KIND_TOOL_CALL: i64 = 1;

const KIND_TOOL_RESULT: i64 = 2;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("projection index {path}: {source}")]
    Open {
        path: PathBuf,
        #[source]
        source: rusqlite::Error,
    },

    #[error("projection index {path}: schema version {found}, this build writes {expected}")]
    SchemaVersion {
        path: PathBuf,
        found: u32,
        expected: u32,
    },

    #[error("event {seq} has no payload; refusing to project")]
    MissingPayload { seq: u64 },

    #[error("sequence number {seq} is out of range for the index")]
    SeqOutOfRange { seq: i128 },

    #[error("memory event {seq}: record {id} is missing or not older than the event")]
    StaleMemoryEvent { seq: u64, id: String },

    #[error("projection index: {0}")]
    Sqlite(#[from] rusqlite::Error),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionSummary {
    pub id: String,
    pub title: String,
    pub started_at: Option<i64>,
    pub preview: String,
    pub last_at: Option<i64>,
    pub role: i32,
    pub project: Option<String>,
    /// The session that dispatched it; empty for a root conversation, which
    /// is what the picker's conversation/job split keys on (row 6.34/9.1).
    pub dispatched_by: String,
    /// Who created the session (row 9.5): the picker's actual conversation/job
    /// split, correct for all history unlike `dispatched_by` alone.
    pub source: i32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MessageRow {
    Message {
        role: i32,
        content: String,
        partial: bool,
        turn_id: String,
        source: i32,
        input_tokens: u32,
        output_tokens: u32,
        elapsed_ms: u32,
    },
    ToolCall {
        call_id: String,
        call_index: u32,
        name: String,
        arguments_json: String,
        turn_id: String,
        provider_roundtrip: Vec<u8>,
    },
    ToolResult {
        call_id: String,
        outcome: i32,
        content: String,
        truncated: bool,
        turn_id: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemoryIndexEntry {
    pub id: String,
    pub namespace: String,
    pub kind: i32,
    pub title: String,
    pub summary: String,
    pub body: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DueSession {
    pub session_id: String,
    pub latest_seq: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ReviewItem {
    pub record: MemoryRecord,
    pub changed_at: i64,
    pub superseded_by: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct MemoryRecordState {
    pub record: MemoryRecord,
    pub superseded_by: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReplayStats {
    pub applied: u64,
    pub skipped: u64,
}

#[derive(Debug, thiserror::Error)]
pub enum ReplayError {
    #[error("projection error: {source}")]
    Projection { source: Error },
    #[error("log error: {source}")]
    Log { source: log::Error },
}

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

#[derive(Debug)]
pub struct Projection {
    conn: Connection,
}

fn opened(path: &Path) -> impl Fn(rusqlite::Error) -> Error {
    let path = path.to_path_buf();
    move |source| Error::Open {
        path: path.clone(),
        source,
    }
}

impl Projection {
    pub fn in_memory() -> Result<Self, Error> {
        Self::open(Path::new(":memory:"))
    }

    #[tracing::instrument(
        level = "debug",
        name = "projection.open",
        skip_all,
        fields(
            path = %path.display(),
            schema_version = SCHEMA_VERSION,
        )
    )]
    pub fn open(path: &Path) -> Result<Self, Error> {
        let conn = Connection::open(path).map_err(opened(path))?;

        conn.query_row("PRAGMA journal_mode = WAL", [], |row| {
            row.get::<_, String>(0)
        })
        .map_err(opened(path))?;
        conn.pragma_update(None, "synchronous", "OFF")
            .map_err(opened(path))?;

        conn.execute_batch(SCHEMA).map_err(opened(path))?;

        let projection = Self { conn };
        projection.check_schema_version(path)?;
        Ok(projection)
    }

    fn check_schema_version(&self, path: &Path) -> Result<(), Error> {
        let found: Option<u32> = self
            .conn
            .query_row(
                "SELECT value FROM projection_meta WHERE key = ?1",
                (SCHEMA_VERSION_KEY,),
                |row| row.get(0),
            )
            .optional()
            .map_err(opened(path))?;

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
                    .map_err(opened(path))?;
                Ok(())
            }
        }
    }

    #[tracing::instrument(
        level = "debug",
        name = "projection.apply",
        skip_all,
        fields(
            seq = event.seq,
            kind = tracing::field::Empty,
        )
    )]
    pub(crate) fn apply(&mut self, event: &Event) -> Result<(), Error> {
        let Some(payload) = event.payload.as_ref() else {
            return Err(Error::MissingPayload { seq: event.seq });
        };
        tracing::Span::current().record("kind", event_kind(payload));

        let tx = self.conn.transaction()?;
        match payload {
            event::Payload::Session(SessionEvent { event: Some(kind) }) => match kind {
                session_event::Event::SessionCreated(created) => {
                    insert_session(&tx, event, created)?;
                }
                session_event::Event::MessageAppended(appended) => {
                    insert_message(&tx, event, appended)?;
                }
                session_event::Event::ToolCallIssued(call) => insert_tool_call(&tx, event, call)?,
                session_event::Event::SessionTitled(titled) => {
                    mark_titled(&tx, event, titled)?;
                }
                session_event::Event::ToolResultRecorded(result) => {
                    insert_tool_result(&tx, event, result)?;
                }
                session_event::Event::SessionConsolidated(consolidated) => {
                    mark_consolidated(&tx, event, consolidated)?;
                }
            },
            event::Payload::Memory(MemoryEvent { event: Some(kind) }) => match kind {
                memory_event::Event::RecordCreated(created) => {
                    create_memory_record(&tx, event, created)?;
                }
                memory_event::Event::RecordUpdated(updated) => {
                    update_memory_record(&tx, event, updated)?;
                }
                memory_event::Event::RecordSuperseded(superseded) => {
                    supersede_memory_record(&tx, event, superseded)?;
                }
                memory_event::Event::RecordDeleted(deleted) => {
                    delete_memory_record(&tx, event, deleted)?;
                }
                memory_event::Event::RecordReviewed(reviewed) => {
                    review_memory_record(&tx, event, reviewed)?;
                }
            },
            event::Payload::Session(_) => tracing::warn!(
                seq = event.seq,
                "session event of an unknown kind; skipping its rows"
            ),
            event::Payload::Memory(_) => tracing::warn!(
                seq = event.seq,
                "memory event of an unknown kind; skipping its rows"
            ),
        }
        set_last_seq(&tx, event.seq)?;
        tx.commit()?;

        Ok(())
    }

    pub(crate) fn last_seq(&self) -> Result<Option<u64>, Error> {
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

    pub(crate) fn session_role(&self, session_id: &str) -> Result<Option<i32>, Error> {
        let role = self
            .conn
            .query_row(
                "SELECT role FROM sessions WHERE id = ?1",
                [session_id],
                |row| row.get(0),
            )
            .optional()?;
        Ok(role)
    }

    pub(crate) fn session_started_at(&self, session_id: &str) -> Result<Option<i64>, Error> {
        let started: Option<Option<i64>> = self
            .conn
            .query_row(
                "SELECT started_at FROM sessions WHERE id = ?1",
                [session_id],
                |row| row.get(0),
            )
            .optional()?;
        Ok(started.flatten())
    }

    pub(crate) fn session_project(&self, session_id: &str) -> Result<Option<String>, Error> {
        let project: Option<Option<String>> = self
            .conn
            .query_row(
                "SELECT project FROM sessions WHERE id = ?1",
                [session_id],
                |row| row.get(0),
            )
            .optional()?;
        Ok(project.flatten())
    }

    /// The provider and model a session was recorded on. `None` for an
    /// unknown session; `Some(("", ""))` for one logged before 5.3 stamped
    /// them — absent, like an `Unspecified` role, means unpinned.
    pub(crate) fn session_identity(
        &self,
        session_id: &str,
    ) -> Result<Option<(String, String)>, Error> {
        let row: Option<(Option<String>, Option<String>)> = self
            .conn
            .query_row(
                "SELECT provider, model FROM sessions WHERE id = ?1",
                [session_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;
        Ok(row.map(|(provider, model)| (provider.unwrap_or_default(), model.unwrap_or_default())))
    }

    pub(crate) fn session_title(&self, session_id: &str) -> Result<Option<String>, Error> {
        let title: Option<String> = self
            .conn
            .query_row(
                "SELECT title FROM sessions WHERE id = ?1",
                [session_id],
                |row| row.get(0),
            )
            .optional()?;
        Ok(title.filter(|title| !title.is_empty()))
    }

    pub(crate) fn session_grants(&self, session_id: &str) -> Result<Vec<(String, bool)>, Error> {
        let mut stmt = self.conn.prepare(
            "SELECT root, read_write FROM session_grants WHERE session_id = ?1 ORDER BY rowid",
        )?;
        let rows = stmt.query_map([session_id], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, bool>(1)?))
        })?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }

    pub fn sessions(&self) -> Result<Vec<SessionSummary>, Error> {
        sessions(&self.conn)
    }

    pub fn messages(&self, session_id: &str) -> Result<Vec<MessageRow>, Error> {
        messages(&self.conn, session_id)
    }

    pub fn call_ids(&self, session_id: &str) -> Result<HashSet<String>, Error> {
        let mut stmt = self.conn.prepare(
            "SELECT call_id FROM messages
             WHERE session_id = ?1 AND kind = ?2 AND call_id IS NOT NULL",
        )?;
        let rows = stmt.query_map(rusqlite::params![session_id, KIND_TOOL_CALL], |row| {
            row.get::<_, String>(0)
        })?;
        Ok(rows.collect::<Result<HashSet<_>, _>>()?)
    }

    /// Job sessions with a recorded dispatching parent: `(session_id,
    /// parent_session, role)`. A job session created before 6.34 has no
    /// recorded parent and is absent here — it cannot be repaired.
    pub(crate) fn parented_job_sessions(&self) -> Result<Vec<(String, String, i32)>, Error> {
        let mut stmt = self.conn.prepare(
            "SELECT id, parent_session, role FROM sessions
             WHERE parent_session IS NOT NULL AND role IN (?1, ?2)",
        )?;
        let rows = stmt.query_map(
            rusqlite::params![
                arc_proto::v1::SessionRole::Executor as i32,
                arc_proto::v1::SessionRole::Archivist as i32
            ],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i32>(2)?,
                ))
            },
        )?;
        Ok(rows.collect::<Result<_, _>>()?)
    }

    /// Whether `parent_session_id` already carries a handback message for
    /// `child_session_id` (`record_handback`'s `"Job {id} finished/stopped"`
    /// prefix, system-sourced) — a job already concluded must not be handed
    /// back a second time.
    pub(crate) fn parent_has_handback_for(
        &self,
        parent_session_id: &str,
        child_session_id: &str,
    ) -> Result<bool, Error> {
        Ok(self.conn.query_row(
            "SELECT EXISTS(
                 SELECT 1 FROM messages
                 WHERE session_id = ?1 AND kind = ?2 AND source = ?3
                   AND content LIKE 'Job ' || ?4 || ' %'
             )",
            rusqlite::params![
                parent_session_id,
                KIND_MESSAGE,
                arc_proto::v1::Source::System as i32,
                child_session_id
            ],
            |row| row.get(0),
        )?)
    }

    /// A job session's summed usage across every message it carries: what a
    /// resumed job's spent counter seeds from, instead of restarting at zero.
    pub(crate) fn session_token_total(&self, session_id: &str) -> Result<u64, Error> {
        let total: i64 = self.conn.query_row(
            "SELECT COALESCE(SUM(input_tokens), 0) + COALESCE(SUM(output_tokens), 0)
             FROM messages WHERE session_id = ?1 AND kind = ?2",
            rusqlite::params![session_id, KIND_MESSAGE],
            |row| row.get(0),
        )?;
        Ok(u64::try_from(total).unwrap_or(0))
    }

    pub(crate) fn due_for_consolidation(
        &self,
        idle_cutoff_micros: i64,
    ) -> Result<Vec<DueSession>, Error> {
        let mut stmt = self.conn.prepare(
            "SELECT s.id, MAX(m.seq)
             FROM sessions s JOIN messages m ON m.session_id = s.id
             GROUP BY s.id
             HAVING MAX(m.ts) < ?1
                AND (s.consolidated_through IS NULL
                     OR MAX(m.seq) > s.consolidated_through)
             ORDER BY MAX(m.ts), s.id",
        )?;
        let rows = stmt.query_map([idle_cutoff_micros], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
        })?;
        rows.map(|row| {
            let (session_id, seq) = row?;
            Ok(DueSession {
                session_id,
                latest_seq: u64::try_from(seq).map_err(|_| Error::SeqOutOfRange {
                    seq: i128::from(seq),
                })?,
            })
        })
        .collect()
    }

    pub(crate) fn latest_seq(&self, session_id: &str) -> Result<Option<u64>, Error> {
        let stored: Option<i64> = self.conn.query_row(
            "SELECT MAX(seq) FROM messages WHERE session_id = ?1",
            [session_id],
            |row| row.get(0),
        )?;
        stored
            .map(|seq| {
                u64::try_from(seq).map_err(|_| Error::SeqOutOfRange {
                    seq: i128::from(seq),
                })
            })
            .transpose()
    }

    pub(crate) fn memory_index(&self) -> Result<Vec<MemoryIndexEntry>, Error> {
        let mut stmt = self.conn.prepare(
            "SELECT id, namespace, kind, title, summary, body FROM memory_records
             WHERE status = ?1 ORDER BY namespace, kind, title, id",
        )?;
        let rows = stmt.query_map([memory_record::Status::Active as i32], |row| {
            Ok(MemoryIndexEntry {
                id: row.get(0)?,
                namespace: row.get(1)?,
                kind: row.get(2)?,
                title: row.get(3)?,
                summary: row.get(4)?,
                body: row.get(5)?,
            })
        })?;
        Ok(rows.collect::<Result<_, _>>()?)
    }

    pub fn review_items(&self, since_micros: i64) -> Result<Vec<ReviewItem>, Error> {
        review_items(&self.conn, since_micros)
    }

    pub fn memory_active_at(&self, at_micros: i64) -> Result<Vec<MemoryRecord>, Error> {
        memory_active_at(&self.conn, at_micros)
    }

    pub(crate) fn memory_record(&self, id: &str) -> Result<Option<MemoryRecordState>, Error> {
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

// NULL (pre-existing events) or an out-of-range value both read as absent
fn nonneg_u32(value: Option<i64>) -> u32 {
    value
        .and_then(|value| u32::try_from(value).ok())
        .unwrap_or(0)
}

fn bad_column(index: usize, message: &str) -> rusqlite::Error {
    rusqlite::Error::FromSqlConversionFailure(
        index,
        rusqlite::types::Type::Integer,
        message.to_owned().into(),
    )
}

pub struct Reader {
    conn: Mutex<Connection>,
}

impl Reader {
    pub fn open(path: &Path) -> Result<Self, Error> {
        let conn = Connection::open_with_flags(
            path,
            OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )
        .map_err(opened(path))?;
        Ok(Self {
            conn: Mutex::new(conn),
        })
    }

    pub fn sessions(&self) -> Result<Vec<SessionSummary>, Error> {
        sessions(&self.conn())
    }

    pub fn transcript(&self, session_id: &str) -> Result<Vec<HistoryEntry>, Error> {
        Ok(messages(&self.conn(), session_id)?
            .into_iter()
            .map(history_entry)
            .collect())
    }

    pub fn review_items(&self, since_micros: i64) -> Result<Vec<ReviewItem>, Error> {
        review_items(&self.conn(), since_micros)
    }

    pub fn memory_active_at(&self, at_micros: i64) -> Result<Vec<MemoryRecord>, Error> {
        memory_active_at(&self.conn(), at_micros)
    }

    fn conn(&self) -> MutexGuard<'_, Connection> {
        self.conn.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum RebuildError {
    #[error("reading the log: {0}")]
    Log(#[from] log::Error),
    #[error("replaying the log: {0}")]
    Replay(#[from] ReplayError),
    #[error("comparing the index: {0}")]
    Index(#[from] Error),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RebuildReport {
    pub schema_version_live: Option<u32>,
    pub schema_version_replayed: Option<u32>,
    pub tables: Vec<TableDiff>,
}

impl RebuildReport {
    pub fn is_clean(&self) -> bool {
        self.schema_version_live == self.schema_version_replayed
            && self.tables.iter().all(|table| table.divergence.is_none())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableDiff {
    pub table: &'static str,
    pub rows_live: usize,
    pub rows_replayed: usize,
    pub divergence: Option<RowDivergence>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RowDivergence {
    pub key: String,
    pub live: Option<Vec<String>>,
    pub replayed: Option<Vec<String>>,
}

struct TableSpec {
    table: &'static str,
    key_columns: usize,
    select: &'static str,
}

// every column named explicitly and ordered by a stable key, so two
// independently-built databases compare byte-for-byte as text; FTS tables
// are derived from messages and carry nothing of their own to verify
const REBUILD_TABLES: &[TableSpec] = &[
    TableSpec {
        table: "sessions",
        key_columns: 1,
        select: "SELECT id, parent_session, fork_point, project, title, started_at, \
                 consolidated_through, role, provider, model, source \
                 FROM sessions ORDER BY id",
    },
    TableSpec {
        table: "messages",
        key_columns: 2,
        select: "SELECT session_id, seq, kind, turn_id, role, content, partial, call_id, \
                 call_index, name, arguments_json, outcome, truncated, ts, \
                 provider_roundtrip, source, input_tokens, output_tokens, elapsed_ms \
                 FROM messages ORDER BY session_id, seq",
    },
    TableSpec {
        table: "memory_records",
        key_columns: 1,
        select: "SELECT id, kind, namespace, title, summary, body, links, provenance, \
                 status, superseded_by, created_seq, last_event_seq, changed_at, reviewed_at, \
                 created_at, superseded_at \
                 FROM memory_records ORDER BY id",
    },
    TableSpec {
        table: "session_grants",
        key_columns: 2,
        select: "SELECT session_id, rowid, root, read_write \
                 FROM session_grants ORDER BY session_id, rowid",
    },
];

/// Replays the log into a fresh in-memory projection and diffs it against
/// the live index under one read transaction, so a running daemon can't
/// hand back a torn view mid-comparison.
pub fn rebuild(log_dir: &Path, index_path: &Path) -> Result<RebuildReport, RebuildError> {
    let mut replayed = Projection::in_memory()?;
    let reader = log::LogReader::new(log::discover_segments(log_dir)?);
    replay(reader, &mut replayed)?;

    let mut live_conn = Connection::open_with_flags(
        index_path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .map_err(opened(index_path))?;
    let live_tx = live_conn.transaction().map_err(Error::from)?;

    let schema_version_live = read_schema_version(&live_tx)?;
    let schema_version_replayed = read_schema_version(&replayed.conn)?;

    let mut tables = Vec::with_capacity(REBUILD_TABLES.len());
    for spec in REBUILD_TABLES {
        tables.push(diff_table(spec, &live_tx, &replayed.conn)?);
    }

    Ok(RebuildReport {
        schema_version_live,
        schema_version_replayed,
        tables,
    })
}

fn read_schema_version(conn: &Connection) -> Result<Option<u32>, Error> {
    Ok(conn
        .query_row(
            "SELECT value FROM projection_meta WHERE key = ?1",
            (SCHEMA_VERSION_KEY,),
            |row| row.get(0),
        )
        .optional()?)
}

fn diff_table(
    spec: &TableSpec,
    live: &Connection,
    replayed: &Connection,
) -> Result<TableDiff, Error> {
    let live_rows = fetch_rows(live, spec.select)?;
    let replayed_rows = fetch_rows(replayed, spec.select)?;
    let rows_live = live_rows.len();
    let rows_replayed = replayed_rows.len();

    let mut divergence = None;
    for i in 0..rows_live.max(rows_replayed) {
        let live_row = live_rows.get(i);
        let replayed_row = replayed_rows.get(i);
        if live_row != replayed_row {
            let key = live_row
                .or(replayed_row)
                .map(|row| row[..spec.key_columns].join(","))
                .unwrap_or_default();
            divergence = Some(RowDivergence {
                key,
                live: live_row.cloned(),
                replayed: replayed_row.cloned(),
            });
            break;
        }
    }

    Ok(TableDiff {
        table: spec.table,
        rows_live,
        rows_replayed,
        divergence,
    })
}

fn fetch_rows(conn: &Connection, select: &str) -> Result<Vec<Vec<String>>, Error> {
    let mut stmt = conn.prepare(select)?;
    let columns = stmt.column_count();
    let rows = stmt.query_map([], |row| {
        (0..columns)
            .map(|i| row.get::<_, rusqlite::types::Value>(i).map(value_text))
            .collect::<Result<Vec<_>, _>>()
    })?;
    Ok(rows.collect::<Result<_, _>>()?)
}

fn value_text(value: rusqlite::types::Value) -> String {
    use std::fmt::Write as _;

    use rusqlite::types::Value;
    match value {
        Value::Null => "NULL".to_owned(),
        Value::Integer(i) => i.to_string(),
        Value::Real(f) => f.to_string(),
        Value::Text(s) => s,
        Value::Blob(bytes) => bytes.iter().fold(String::new(), |mut hex, byte| {
            let _ = write!(hex, "{byte:02x}");
            hex
        }),
    }
}

pub(crate) fn sessions(conn: &Connection) -> Result<Vec<SessionSummary>, Error> {
    let mut stmt = conn.prepare(
        "SELECT s.id, coalesce(s.title, ''), s.started_at,
                coalesce((SELECT m.content FROM messages m
                          WHERE m.session_id = s.id AND m.kind = ?2 AND m.role = ?1
                          ORDER BY m.seq LIMIT 1), ''),
                (SELECT MAX(m.ts) FROM messages m
                 WHERE m.session_id = s.id AND m.kind = ?2),
                s.role, s.project, coalesce(s.parent_session, ''), s.source
         FROM sessions s ORDER BY s.started_at, s.id",
    )?;
    let rows = stmt.query_map(rusqlite::params![Role::User as i32, KIND_MESSAGE], |row| {
        Ok(SessionSummary {
            id: row.get(0)?,
            title: row.get(1)?,
            started_at: row.get(2)?,
            preview: row.get(3)?,
            last_at: row.get(4)?,
            role: row.get(5)?,
            project: row.get(6)?,
            dispatched_by: row.get(7)?,
            source: row.get(8)?,
        })
    })?;
    Ok(rows.collect::<Result<_, _>>()?)
}

pub(crate) fn messages(conn: &Connection, session_id: &str) -> Result<Vec<MessageRow>, Error> {
    let mut stmt = conn.prepare(
        "SELECT kind, role, content, partial, turn_id,
                call_id, call_index, name, arguments_json, outcome, truncated,
                provider_roundtrip, source, input_tokens, output_tokens, elapsed_ms
         FROM messages WHERE session_id = ?1 ORDER BY seq",
    )?;
    let rows = stmt.query_map([session_id], |row| match row.get::<_, i64>(0)? {
        KIND_MESSAGE => Ok(MessageRow::Message {
            role: row.get(1)?,
            content: row.get(2)?,
            partial: row.get(3)?,
            turn_id: row.get(4)?,
            source: row.get::<_, Option<i32>>(12)?.unwrap_or(0),
            input_tokens: nonneg_u32(row.get::<_, Option<i64>>(13)?),
            output_tokens: nonneg_u32(row.get::<_, Option<i64>>(14)?),
            elapsed_ms: nonneg_u32(row.get::<_, Option<i64>>(15)?),
        }),
        KIND_TOOL_CALL => Ok(MessageRow::ToolCall {
            call_id: row.get(5)?,
            call_index: u32::try_from(row.get::<_, i64>(6)?)
                .map_err(|_| bad_column(6, "call_index out of range"))?,
            name: row.get(7)?,
            arguments_json: row.get(8)?,
            turn_id: row.get(4)?,
            provider_roundtrip: row.get::<_, Option<Vec<u8>>>(11)?.unwrap_or_default(),
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

/// The `:review` pane's default lookback; the queue's live count uses the
/// same window so it matches what opening the pane shows.
pub const REVIEW_WINDOW_MICROS: i64 = 7 * 24 * 3_600 * 1_000_000;

pub(crate) fn review_items(conn: &Connection, since_micros: i64) -> Result<Vec<ReviewItem>, Error> {
    let mut stmt = conn.prepare(
        "SELECT id, kind, namespace, title, summary, body, links, provenance,
                status, superseded_by, changed_at
         FROM memory_records
         WHERE changed_at >= ?1
           AND (reviewed_at IS NULL OR reviewed_at < changed_at)
         ORDER BY changed_at, id",
    )?;
    let rows = stmt.query_map([since_micros], |row| {
        Ok(ReviewItem {
            record: MemoryRecord {
                id: row.get(0)?,
                kind: row.get(1)?,
                namespace: row.get(2)?,
                title: row.get(3)?,
                summary: row.get(4)?,
                body: row.get(5)?,
                links: links_from_json(&row.get::<_, String>(6)?)
                    .map_err(|e| bad_json_column(6, &e))?,
                provenance: provenance_from_json(row.get::<_, Option<String>>(7)?.as_deref())
                    .map_err(|e| bad_json_column(7, &e))?,
                status: row.get(8)?,
            },
            superseded_by: row.get(9)?,
            changed_at: row.get(10)?,
        })
    })?;
    Ok(rows.collect::<Result<_, _>>()?)
}

/// The records held at `at_micros`, each in its last-known form: a record
/// is active on [`created_at`, `superseded_at`). Content is not reconstructed —
/// the log holds that — and a purged record is absent from history too,
/// which is the point of a purge. Rows missing a timestamp (a create or
/// supersede event without one) are excluded rather than guessed.
fn memory_active_at(conn: &Connection, at_micros: i64) -> Result<Vec<MemoryRecord>, Error> {
    let mut stmt = conn.prepare(
        "SELECT id, kind, namespace, title, summary, body, links, provenance, status
         FROM memory_records
         WHERE created_at IS NOT NULL AND created_at <= ?1
           AND (status = ?2 OR (superseded_at IS NOT NULL AND superseded_at > ?1))
         ORDER BY namespace, kind, title, id",
    )?;
    let rows = stmt.query_map(
        rusqlite::params![at_micros, memory_record::Status::Active as i32],
        |row| {
            Ok(MemoryRecord {
                id: row.get(0)?,
                kind: row.get(1)?,
                namespace: row.get(2)?,
                title: row.get(3)?,
                summary: row.get(4)?,
                body: row.get(5)?,
                links: links_from_json(&row.get::<_, String>(6)?)
                    .map_err(|e| bad_json_column(6, &e))?,
                provenance: provenance_from_json(row.get::<_, Option<String>>(7)?.as_deref())
                    .map_err(|e| bad_json_column(7, &e))?,
                status: row.get(8)?,
            })
        },
    )?;
    Ok(rows.collect::<Result<_, _>>()?)
}

pub(crate) fn history_entry(row: MessageRow) -> HistoryEntry {
    let entry = match row {
        MessageRow::Message {
            role,
            content,
            partial,
            source,
            input_tokens,
            output_tokens,
            elapsed_ms,
            ..
        } => history_entry::Entry::Message(HistoryMessage {
            role,
            content,
            partial,
            source,
            input_tokens,
            output_tokens,
            elapsed_ms,
        }),
        MessageRow::ToolCall {
            call_id,
            name,
            arguments_json,
            ..
        } => history_entry::Entry::ToolCall(HistoryToolCall {
            call_id,
            name,
            arguments_json,
        }),
        MessageRow::ToolResult {
            call_id,
            outcome,
            truncated,
            ..
        } => history_entry::Entry::ToolResult(HistoryToolResult {
            call_id,
            outcome,
            truncated,
        }),
    };
    HistoryEntry { entry: Some(entry) }
}

fn event_kind(payload: &event::Payload) -> &'static str {
    match payload {
        event::Payload::Session(SessionEvent { event: Some(kind) }) => match kind {
            session_event::Event::SessionCreated(_) => "session_created",
            session_event::Event::MessageAppended(_) => "message_appended",
            session_event::Event::ToolCallIssued(_) => "tool_call_issued",
            session_event::Event::ToolResultRecorded(_) => "tool_result_recorded",
            session_event::Event::SessionConsolidated(_) => "session_consolidated",
            session_event::Event::SessionTitled(_) => "session_titled",
        },
        event::Payload::Memory(MemoryEvent { event: Some(kind) }) => match kind {
            memory_event::Event::RecordCreated(_) => "memory_record_created",
            memory_event::Event::RecordUpdated(_) => "memory_record_updated",
            memory_event::Event::RecordSuperseded(_) => "memory_record_superseded",
            memory_event::Event::RecordDeleted(_) => "memory_record_deleted",
            memory_event::Event::RecordReviewed(_) => "memory_record_reviewed",
        },
        event::Payload::Session(_) | event::Payload::Memory(_) => "unknown",
    }
}

pub(crate) fn bad_json_column(index: usize, source: &serde_json::Error) -> rusqlite::Error {
    rusqlite::Error::FromSqlConversionFailure(
        index,
        rusqlite::types::Type::Text,
        source.to_string().into(),
    )
}

fn insert_session(
    tx: &Transaction<'_>,
    event: &Event,
    created: &SessionCreated,
) -> Result<(), Error> {
    tx.execute(
        "INSERT INTO sessions
             (id, parent_session, fork_point, project, title, started_at, role, provider, model, source)
         VALUES (?1, ?2, NULL, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
        (
            &created.session_id,
            (!created.dispatched_by.is_empty()).then_some(&created.dispatched_by),
            (!created.project.is_empty()).then_some(&created.project),
            &created.title,
            epoch_micros(event.ts.as_ref()),
            created.role,
            (!created.provider.is_empty()).then_some(&created.provider),
            (!created.model.is_empty()).then_some(&created.model),
            event.source,
        ),
    )?;
    for grant in &created.grants {
        tx.execute(
            "INSERT INTO session_grants (session_id, root, read_write) VALUES (?1, ?2, ?3)",
            (&created.session_id, &grant.root, grant.read_write),
        )?;
    }
    Ok(())
}

fn mark_consolidated(
    tx: &Transaction<'_>,
    event: &Event,
    consolidated: &SessionConsolidated,
) -> Result<(), Error> {
    let changed = tx.execute(
        "UPDATE sessions SET consolidated_through = ?2
         WHERE id = ?1
           AND (consolidated_through IS NULL OR consolidated_through < ?2)",
        rusqlite::params![
            &consolidated.session_id,
            seq_param(consolidated.through_seq)?,
        ],
    )?;
    if changed == 0 {
        tracing::warn!(
            seq = event.seq,
            session_id = %consolidated.session_id,
            through_seq = consolidated.through_seq,
            "consolidation marker for a missing session or not past the current one; skipping"
        );
    }
    Ok(())
}

fn mark_titled(tx: &Transaction<'_>, event: &Event, titled: &SessionTitled) -> Result<(), Error> {
    let changed = tx.execute(
        "UPDATE sessions SET title = ?2 WHERE id = ?1",
        rusqlite::params![&titled.session_id, &titled.title],
    )?;
    if changed == 0 {
        tracing::warn!(
            seq = event.seq,
            session_id = %titled.session_id,
            "title event for a missing session; skipping"
        );
    }
    Ok(())
}

fn insert_message(
    tx: &Transaction<'_>,
    event: &Event,
    appended: &MessageAppended,
) -> Result<(), Error> {
    tx.execute(
        "INSERT INTO messages (session_id, seq, kind, turn_id, role, content, partial, ts, source,
                                input_tokens, output_tokens, elapsed_ms)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
        rusqlite::params![
            &appended.session_id,
            seq_param(event.seq)?,
            KIND_MESSAGE,
            &appended.turn_id,
            appended.role,
            &appended.content,
            appended.partial,
            epoch_micros(event.ts.as_ref()),
            event.source,
            appended.input_tokens,
            appended.output_tokens,
            appended.elapsed_ms,
        ],
    )?;
    index_content(tx, event.seq, &appended.content)
}

fn insert_tool_call(
    tx: &Transaction<'_>,
    event: &Event,
    call: &ToolCallIssued,
) -> Result<(), Error> {
    tx.execute(
        "INSERT INTO messages
             (session_id, seq, kind, turn_id, content, call_id, call_index, name,
              arguments_json, ts, provider_roundtrip)
         VALUES (?1, ?2, ?3, ?4, '', ?5, ?6, ?7, ?8, ?9, ?10)",
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
            &call.provider_roundtrip,
        ],
    )?;
    Ok(())
}

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
            result.outcome,
            result.truncated,
            epoch_micros(event.ts.as_ref()),
        ],
    )?;
    index_content(tx, event.seq, &result.content)
}

// content= tables have no triggers, so FTS rows go in by hand
fn index_content(tx: &Transaction<'_>, seq: u64, content: &str) -> Result<(), Error> {
    tx.execute(
        "INSERT INTO messages_fts (rowid, content) VALUES (?1, ?2)",
        (seq_param(seq)?, content),
    )?;
    Ok(())
}

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
              status, superseded_by, created_seq, last_event_seq, changed_at, created_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, NULL, ?10, ?10, ?11, ?11)",
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
            epoch_micros(event.ts.as_ref()),
        ],
    )?;
    Ok(())
}

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
             last_event_seq = ?10, changed_at = ?11
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
            epoch_micros(event.ts.as_ref()),
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
             SET status = ?2, superseded_by = ?3, last_event_seq = ?4, changed_at = ?5,
                 superseded_at = ?5
             WHERE id = ?1 AND last_event_seq < ?4",
            rusqlite::params![
                &superseded.superseded_id,
                memory_record::Status::Superseded as i32,
                &record.id,
                seq_param(event.seq)?,
                epoch_micros(event.ts.as_ref()),
            ],
        )?;
        if changed == 0 {
            if memory_record_exists(tx, &superseded.superseded_id)? {
                return Err(Error::StaleMemoryEvent {
                    seq: event.seq,
                    id: superseded.superseded_id.clone(),
                });
            }
            tracing::warn!(
                seq = event.seq,
                id = %superseded.superseded_id,
                "supersede target not in the projection; upserting the replacement anyway"
            );
        }
    }
    upsert_memory_record(tx, event, record)?;
    stamp_link_dependents(tx, &superseded.superseded_id, event)
}

fn upsert_memory_record(
    tx: &Transaction<'_>,
    event: &Event,
    record: &MemoryRecord,
) -> Result<(), Error> {
    // an in-place replacement keeps created_at — same record, revised, like
    // an update — and clears superseded_at so it reads active again
    let changed = tx.execute(
        "INSERT INTO memory_records
             (id, kind, namespace, title, summary, body, links, provenance,
              status, superseded_by, created_seq, last_event_seq, changed_at, created_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, NULL, ?10, ?10, ?11, ?11)
         ON CONFLICT (id) DO UPDATE SET
             kind = excluded.kind, namespace = excluded.namespace,
             title = excluded.title, summary = excluded.summary,
             body = excluded.body, links = excluded.links,
             provenance = excluded.provenance, status = excluded.status,
             superseded_by = NULL, last_event_seq = excluded.last_event_seq,
             changed_at = excluded.changed_at, superseded_at = NULL
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
            epoch_micros(event.ts.as_ref()),
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
        tracing::warn!(
            seq = event.seq,
            id = %deleted.id,
            "delete of an unknown memory record; nothing to remove"
        );
    }
    stamp_link_dependents(tx, &deleted.id, event)
}

// last_event_seq guards content-changing writes (StaleMemoryEvent); a
// dependent re-entering review is not a content change, so it stays put.
fn stamp_link_dependents(
    tx: &Transaction<'_>,
    target_id: &str,
    event: &Event,
) -> Result<(), Error> {
    let Some(at) = epoch_micros(event.ts.as_ref()) else {
        tracing::warn!(
            seq = event.seq,
            id = %target_id,
            "supersede/delete event carries no timestamp; dependents not re-queued"
        );
        return Ok(());
    };
    tx.execute(
        "UPDATE memory_records
         SET changed_at = ?2
         WHERE status = ?3
           AND id != ?1
           AND (changed_at IS NULL OR changed_at < ?2)
           AND EXISTS (SELECT 1 FROM json_each(memory_records.links)
                       WHERE json_each.value = ?1)",
        rusqlite::params![target_id, at, memory_record::Status::Active as i32],
    )?;
    Ok(())
}

fn review_memory_record(
    tx: &Transaction<'_>,
    event: &Event,
    reviewed: &MemoryRecordReviewed,
) -> Result<(), Error> {
    let Some(at) = epoch_micros(event.ts.as_ref()) else {
        tracing::warn!(
            seq = event.seq,
            id = %reviewed.record_id,
            "review event carries no timestamp; skipping"
        );
        return Ok(());
    };
    let changed = tx.execute(
        "UPDATE memory_records SET reviewed_at = ?2
         WHERE id = ?1 AND (reviewed_at IS NULL OR reviewed_at < ?2)",
        rusqlite::params![&reviewed.record_id, at],
    )?;
    if changed == 0 {
        tracing::warn!(
            seq = event.seq,
            id = %reviewed.record_id,
            "review of an unknown record or not past the current stamp; skipping"
        );
    }
    Ok(())
}

fn memory_record_exists(tx: &Transaction<'_>, id: &str) -> Result<bool, Error> {
    let found: Option<i64> = tx
        .query_row("SELECT 1 FROM memory_records WHERE id = ?1", [id], |row| {
            row.get(0)
        })
        .optional()?;
    Ok(found.is_some())
}

fn skip_recordless(seq: u64, kind: &str) {
    tracing::warn!(
        seq,
        kind,
        "memory event carries no record; skipping its rows"
    );
}

#[derive(serde::Serialize, serde::Deserialize)]
struct ProvenanceEntryJson {
    session_id: String,
    ts: Option<TimestampJson>,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct TimestampJson {
    seconds: i64,
    nanos: i32,
}

fn links_json(links: &[String]) -> String {
    serde_json::to_string(links).expect("strings always serialize")
}

pub(crate) fn links_from_json(json: &str) -> Result<Vec<String>, serde_json::Error> {
    serde_json::from_str(json)
}

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

pub(crate) fn provenance_from_json(
    json: Option<&str>,
) -> Result<Option<Provenance>, serde_json::Error> {
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

fn set_last_seq(tx: &Transaction<'_>, seq: u64) -> Result<(), Error> {
    tx.execute(
        "INSERT INTO projection_meta (key, value) VALUES (?1, ?2)
         ON CONFLICT (key) DO UPDATE SET value = excluded.value",
        (LAST_SEQ_KEY, seq_param(seq)?),
    )?;
    Ok(())
}

fn seq_param(seq: u64) -> Result<i64, Error> {
    i64::try_from(seq).map_err(|_| Error::SeqOutOfRange {
        seq: i128::from(seq),
    })
}

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
        Role, SessionConsolidated, SessionCreated, SessionEvent, SessionRole, SessionTitled,
        Source, ToolCallIssued, ToolOutcome, ToolResultRecorded, WorkspaceGrant, event,
        memory_event, memory_record, session_event,
    };
    use prost_types::Timestamp;
    use rusqlite::{Connection, OptionalExtension};
    use tempfile::TempDir;

    use super::{
        DueSession, Error, MemoryIndexEntry, MessageRow, Projection, ReplayError, ReplayStats,
        ReviewItem, SessionSummary, replay,
    };
    use crate::log::{Log, LogReader, discover_segments};

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
                    role: SessionRole::Executor as i32,
                    project: String::new(),
                    budget: None,
                    grants: Vec::new(),
                    dispatched_by: String::new(),
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
                    ..Default::default()
                })),
            })),
        }
    }

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
                    provider_roundtrip: Vec::new(),
                })),
            })),
        }
    }

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

    fn unknown_kind(seq: u64) -> Event {
        Event {
            seq,
            ts: Some(timestamp()),
            source: Source::System as i32,
            payload: Some(event::Payload::Session(SessionEvent { event: None })),
        }
    }

    #[derive(Debug, PartialEq)]
    struct SessionRow {
        id: String,
        parent_session: Option<String>,
        fork_point: Option<i64>,
        project: Option<String>,
        title: Option<String>,
        started_at: Option<i64>,
        role: i64,
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
            "session_grants",
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
        let projection = Projection::in_memory().expect("open");

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

        projection
            .apply(&message_appended(1, "hello"))
            .expect("apply");
        assert_eq!(projection.last_seq().expect("last_seq"), Some(1));
    }

    #[test]
    fn a_sessions_role_comes_back_and_an_unknown_session_has_none() {
        let mut projection = Projection::in_memory().expect("open");
        assert_eq!(projection.session_role("s-01").expect("role"), None);

        projection.apply(&session_created(0)).expect("apply");

        assert_eq!(
            projection.session_role("s-01").expect("role"),
            Some(SessionRole::Executor as i32)
        );
    }

    fn session_created_with_project(seq: u64, project: &str) -> Event {
        let mut event = session_created(seq);
        if let Some(event::Payload::Session(SessionEvent {
            event: Some(session_event::Event::SessionCreated(created)),
        })) = event.payload.as_mut()
        {
            created.project = project.to_owned();
        }
        event
    }

    #[test]
    fn a_sessions_project_comes_back_and_an_unknown_session_has_none() {
        let mut projection = Projection::in_memory().expect("open");
        assert_eq!(projection.session_project("s-01").expect("project"), None);

        projection
            .apply(&session_created_with_project(0, "arc"))
            .expect("apply");

        assert_eq!(
            projection.session_project("s-01").expect("project"),
            Some("arc".to_string())
        );
    }

    #[test]
    fn an_empty_project_answers_none() {
        let mut projection = Projection::in_memory().expect("open");

        projection
            .apply(&session_created_with_project(0, ""))
            .expect("apply");

        assert_eq!(projection.session_project("s-01").expect("project"), None);
    }

    fn session_created_with_grants(seq: u64, grants: Vec<WorkspaceGrant>) -> Event {
        let mut event = session_created(seq);
        if let Some(event::Payload::Session(SessionEvent {
            event: Some(session_event::Event::SessionCreated(created)),
        })) = event.payload.as_mut()
        {
            created.grants = grants;
        }
        event
    }

    #[test]
    fn a_grantless_session_has_no_recorded_grants() {
        let mut projection = Projection::in_memory().expect("open");
        projection.apply(&session_created(0)).expect("apply");

        assert_eq!(
            projection.session_grants("s-01").expect("grants"),
            Vec::new()
        );
    }

    #[test]
    fn session_grants_round_trip_through_replay_in_order() {
        let dir = TempDir::new().expect("temp dir");
        let grants = vec![
            WorkspaceGrant {
                root: "/home/bogdan/arc".to_owned(),
                read_write: true,
            },
            WorkspaceGrant {
                root: "/home/bogdan/notes".to_owned(),
                read_write: false,
            },
        ];

        let mut log = Log::open(dir.path()).expect("open log");
        log.append(session_created_with_grants(0, grants.clone()))
            .expect("append");
        drop(log);

        let mut projection = Projection::in_memory().expect("open");
        let log = Log::open(dir.path()).expect("reopen log");
        replay(log.reader().expect("reader"), &mut projection).expect("replay");

        assert_eq!(
            projection.session_grants("s-01").expect("grants"),
            vec![
                ("/home/bogdan/arc".to_owned(), true),
                ("/home/bogdan/notes".to_owned(), false),
            ]
        );
    }

    #[test]
    fn an_unknown_session_has_no_grants() {
        let projection = Projection::in_memory().expect("open");
        assert_eq!(
            projection.session_grants("s-ghost").expect("grants"),
            Vec::new()
        );
    }

    #[test]
    fn projects_a_session_and_a_message() {
        let mut projection = Projection::in_memory().expect("open");

        projection.apply(&session_created(0)).expect("apply");
        projection
            .apply(&message_appended(1, "hello"))
            .expect("apply");

        let session = projection
            .conn
            .query_row(
                "SELECT id, parent_session, fork_point, project, title, started_at, role
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
                        role: row.get(6)?,
                    })
                },
            )
            .expect("session row");
        assert_eq!(
            session,
            SessionRow {
                id: "s-01".to_string(),
                parent_session: None,
                fork_point: None,
                project: None,
                title: Some("first light".to_string()),
                started_at: Some(TS_MICROS),
                role: i64::from(SessionRole::Executor as i32),
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
        let mut projection = Projection::in_memory().expect("open");
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
        let mut projection = Projection::in_memory().expect("open");
        projection.apply(&session_created(0)).expect("apply");

        projection.apply(&unknown_kind(1)).expect("apply");

        assert_eq!(row_count(&projection, "sessions"), 1);
        assert_eq!(row_count(&projection, "messages"), 0);
        assert_eq!(projection.last_seq().expect("last_seq"), Some(1));
    }

    #[test]
    fn messages_come_back_in_seq_order() {
        let mut projection = Projection::in_memory().expect("open");
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

    #[test]
    fn a_tool_turn_projects_call_and_result_rows() {
        let mut projection = Projection::in_memory().expect("open");
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
                    source: Source::User as i32,
                    input_tokens: 0,
                    output_tokens: 0,
                    elapsed_ms: 0,
                },
                MessageRow::ToolCall {
                    call_id: "c-a".to_string(),
                    call_index: 0,
                    name: "lookup".to_string(),
                    arguments_json: r#"{"q":1}"#.to_string(),
                    turn_id: "t-01".to_string(),
                    provider_roundtrip: Vec::new(),
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
        log.append(tool_result(3, "c-a", 99, "cut resul [truncated]", true))
            .expect("append");

        let mut projection = Projection::in_memory().expect("open");
        replay(log.reader().expect("reader"), &mut projection).expect("replay");

        let rows = projection.messages("s-01").expect("messages");
        assert_eq!(
            rows[0],
            MessageRow::Message {
                role: Role::User as i32,
                content: "half a th".to_string(),
                partial: true,
                turn_id: String::new(),
                source: Source::User as i32,
                input_tokens: 0,
                output_tokens: 0,
                elapsed_ms: 0,
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

    #[test]
    fn a_message_appended_with_usage_populates_the_usage_columns() {
        let dir = TempDir::new().expect("temp dir");
        let mut log = Log::open(dir.path()).expect("open log");
        log.append(session_created(0)).expect("append");
        log.append(message_appended(1, "hi")).expect("append");
        let mut costed = message_appended(2, "the answer");
        if let Some(event::Payload::Session(SessionEvent {
            event: Some(session_event::Event::MessageAppended(m)),
        })) = &mut costed.payload
        {
            m.role = Role::Assistant as i32;
            m.input_tokens = 2345;
            m.output_tokens = 140;
            m.elapsed_ms = 1500;
        }
        log.append(costed).expect("append");

        let mut projection = Projection::in_memory().expect("open");
        replay(log.reader().expect("reader"), &mut projection).expect("replay");

        let rows = projection.messages("s-01").expect("messages");
        assert_eq!(
            rows[0],
            MessageRow::Message {
                role: Role::User as i32,
                content: "hi".to_string(),
                partial: false,
                turn_id: String::new(),
                source: Source::User as i32,
                input_tokens: 0,
                output_tokens: 0,
                elapsed_ms: 0,
            },
            "a zero-usage event leaves zeros"
        );
        assert_eq!(
            rows[1],
            MessageRow::Message {
                role: Role::Assistant as i32,
                content: "the answer".to_string(),
                partial: false,
                turn_id: String::new(),
                source: Source::User as i32,
                input_tokens: 2345,
                output_tokens: 140,
                elapsed_ms: 1500,
            },
            "the event's usage lands in the columns"
        );
    }

    #[test]
    fn fts_matches_prose_and_results_and_kind_filters_them_apart() {
        let mut projection = Projection::in_memory().expect("open");
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
                    role: SessionRole::Unspecified as i32,
                    project: String::new(),
                    budget: None,
                    grants: Vec::new(),
                    dispatched_by: String::new(),
                })),
            })),
        }
    }

    fn session_titled(seq: u64, id: &str, title: &str) -> Event {
        Event {
            seq,
            ts: Some(timestamp()),
            source: Source::System as i32,
            payload: Some(event::Payload::Session(SessionEvent {
                event: Some(session_event::Event::SessionTitled(SessionTitled {
                    session_id: id.to_string(),
                    title: title.to_string(),
                })),
            })),
        }
    }

    #[test]
    fn sessions_are_ordered_by_start_then_id() {
        let mut projection = Projection::in_memory().expect("open");
        assert_eq!(projection.sessions().expect("sessions"), []);

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
                role: SessionRole::Unspecified as i32,
                project: None,
                dispatched_by: String::new(),
                source: Source::User as i32,
            }
        );
        assert_eq!(sessions[0].started_at, None);
    }

    #[test]
    fn a_dispatched_sessions_summary_names_its_parent() {
        let mut projection = Projection::in_memory().expect("open");
        let root = session_created_as(0, "s-root", "root", Some(100));
        let mut child = session_created_as(1, "s-child", "child", Some(200));
        let event::Payload::Session(SessionEvent {
            event: Some(session_event::Event::SessionCreated(created)),
        }) = child.payload.as_mut().expect("payload")
        else {
            unreachable!("session_created_as always builds a SessionCreated")
        };
        created.dispatched_by = "s-root".to_string();
        projection.apply(&root).expect("apply root");
        projection.apply(&child).expect("apply child");

        let sessions = projection.sessions().expect("sessions");
        let root_summary = sessions.iter().find(|s| s.id == "s-root").expect("root");
        let child_summary = sessions.iter().find(|s| s.id == "s-child").expect("child");

        assert_eq!(root_summary.dispatched_by, "", "a root conversation");
        assert_eq!(child_summary.dispatched_by, "s-root");
    }

    #[test]
    fn a_sessions_summary_carries_its_creation_source() {
        let mut projection = Projection::in_memory().expect("open");
        let mut job = session_created_as(0, "s-job", "job", Some(100));
        job.source = Source::Model as i32;
        projection.apply(&job).expect("apply");

        let sessions = projection.sessions().expect("sessions");
        let job_summary = sessions.iter().find(|s| s.id == "s-job").expect("job");

        assert_eq!(job_summary.source, Source::Model as i32);
    }

    #[test]
    fn a_sessions_summary_carries_its_role_and_project() {
        let mut projection = Projection::in_memory().expect("open");
        let mut created = session_created(0);
        if let Some(event::Payload::Session(SessionEvent {
            event: Some(session_event::Event::SessionCreated(created)),
        })) = created.payload.as_mut()
        {
            created.project = "arc".to_string();
        }
        projection.apply(&created).expect("apply");

        let summary = &projection.sessions().expect("sessions")[0];
        assert_eq!(summary.role, SessionRole::Executor as i32);
        assert_eq!(summary.project.as_deref(), Some("arc"));
    }

    #[test]
    fn a_session_previews_its_first_user_message() {
        let mut projection = Projection::in_memory().expect("open");
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

    #[test]
    fn a_session_reports_when_it_was_last_spoken_in() {
        let mut projection = Projection::in_memory().expect("open");
        projection.apply(&session_created(0)).expect("apply");
        assert_eq!(
            projection.sessions().expect("sessions")[0].last_at,
            None,
            "a session nobody has spoken in has no last message"
        );

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
        let mut projection = Projection::in_memory().expect("open");
        projection
            .apply(&session_created_as(0, "s-01", "", Some(1)))
            .expect("apply");

        let sessions = projection.sessions().expect("sessions");

        assert_eq!(sessions.len(), 1);
        assert_eq!(sessions[0].title, "");
    }

    #[test]
    fn a_session_titled_event_sets_the_sessions_title() {
        let mut projection = Projection::in_memory().expect("open");
        projection
            .apply(&session_created_as(0, "s-01", "", Some(1)))
            .expect("apply");
        projection
            .apply(&session_titled(1, "s-01", "Palette bikeshed"))
            .expect("apply");

        assert_eq!(
            projection.sessions().expect("sessions")[0].title,
            "Palette bikeshed"
        );
    }

    #[test]
    fn the_latest_title_wins_on_replay() {
        let mut projection = Projection::in_memory().expect("open");
        projection
            .apply(&session_created_as(0, "s-01", "", Some(1)))
            .expect("apply");
        projection
            .apply(&session_titled(1, "s-01", "First guess"))
            .expect("apply");
        projection
            .apply(&session_titled(2, "s-01", "Better guess"))
            .expect("apply");

        assert_eq!(
            projection.sessions().expect("sessions")[0].title,
            "Better guess",
            "retitling appends another event; the latest wins"
        );
    }

    #[test]
    fn retitling_replays_deterministically_across_fresh_indexes() {
        let events = [
            session_created_as(0, "s-01", "", Some(1)),
            session_titled(1, "s-01", "First guess"),
            session_titled(2, "s-01", "Better guess"),
        ];

        let mut first = Projection::in_memory().expect("open");
        let mut second = Projection::in_memory().expect("open");
        for event in &events {
            first.apply(event).expect("apply");
            second.apply(event).expect("apply");
        }

        assert_eq!(
            first.sessions().expect("sessions"),
            second.sessions().expect("sessions")
        );
    }

    #[test]
    fn a_session_with_no_title_event_keeps_its_created_title() {
        let mut projection = Projection::in_memory().expect("open");
        projection
            .apply(&session_created_as(0, "s-01", "original title", Some(1)))
            .expect("apply");

        assert_eq!(
            projection.sessions().expect("sessions")[0].title,
            "original title"
        );
    }

    #[test]
    fn an_unknown_session_and_an_empty_session_are_both_just_empty() {
        let mut projection = Projection::in_memory().expect("open");
        assert_eq!(projection.messages("s-01").expect("messages"), []);

        projection.apply(&session_created(0)).expect("apply");
        assert_eq!(projection.messages("s-01").expect("messages"), []);
    }

    #[test]
    fn messages_of_other_sessions_stay_out() {
        let mut projection = Projection::in_memory().expect("open");
        projection.apply(&session_created(0)).expect("apply");
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
        let mut projection = Projection::in_memory().expect("open");
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

        assert_eq!(projection.last_seq().expect("last_seq"), Some(0));
        projection
            .apply(&message_appended(1, "hello"))
            .expect("apply after the rejected event");
        assert_eq!(projection.last_seq().expect("last_seq"), Some(1));
        assert_eq!(row_count(&projection, "messages"), 1);
    }

    #[test]
    fn applying_the_same_event_twice_fails_and_rolls_back() {
        let mut projection = Projection::in_memory().expect("open");
        projection.apply(&session_created(0)).expect("apply");
        projection
            .apply(&message_appended(1, "hello"))
            .expect("apply");

        let err = projection
            .apply(&message_appended(1, "hello"))
            .expect_err("a duplicate seq must violate the primary key");
        assert!(matches!(err, Error::Sqlite(_)), "got: {err:?}");

        assert_eq!(row_count(&projection, "messages"), 1);
        assert_eq!(projection.last_seq().expect("last_seq"), Some(1));
    }

    #[test]
    fn double_applying_a_tool_row_fails_and_rolls_back_its_fts_row() {
        let mut projection = Projection::in_memory().expect("open");
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
        let mut projection = Projection::in_memory().expect("open");
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

    const BUILT_EVENTS: u64 = 6;

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

        let mut first = Projection::in_memory().expect("open");
        let mut second = Projection::in_memory().expect("open");
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

        let mut projection = Projection::in_memory().expect("open");
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

        let segments = discover_segments(log.dir()).expect("discover");
        assert!(segments.len() > 1, "the test needs several segments");
        let mut resumed = Projection::in_memory().expect("open");
        let partial =
            replay(LogReader::new(segments[..1].to_vec()), &mut resumed).expect("partial replay");
        assert!(
            partial.applied < BUILT_EVENTS,
            "the partial replay must be partial"
        );

        let stats = replay(log.reader().expect("reader"), &mut resumed).expect("resume");
        assert_eq!(stats.applied + stats.skipped, BUILT_EVENTS);
        assert_eq!(stats.skipped, partial.applied, "resume skips what landed");

        let mut one_shot = Projection::in_memory().expect("open");
        replay(log.reader().expect("reader"), &mut one_shot).expect("one-shot replay");
        assert_eq!(dump(&resumed), dump(&one_shot));
    }

    #[test]
    fn a_failed_apply_keeps_the_resume_point_and_a_later_replay_completes() {
        let dir = TempDir::new().expect("temp dir");
        let log = build_log(dir.path());

        let mut projection = Projection::in_memory().expect("open");
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

        let mut one_shot = Projection::in_memory().expect("open");
        replay(log.reader().expect("reader"), &mut one_shot).expect("one-shot replay");
        assert_eq!(dump(&projection), dump(&one_shot));
    }

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

    fn memory_payload(event: Event) -> memory_event::Event {
        match event.payload {
            Some(event::Payload::Memory(MemoryEvent { event: Some(inner) })) => inner,
            other => panic!("expected a memory payload, got {other:?}"),
        }
    }

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
        let mut projection = Projection::in_memory().expect("open");
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
                body: "gruvbox, at length".to_string(),
            }]
        );
    }

    #[test]
    fn an_update_overwrites_the_whole_record() {
        let mut projection = Projection::in_memory().expect("open");
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
        let mut projection = Projection::in_memory().expect("open");

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
        let mut projection = Projection::in_memory().expect("open");
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

    #[test]
    fn a_supersede_reusing_the_id_replaces_the_row_in_place() {
        let mut projection = Projection::in_memory().expect("open");
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
        let mut projection = Projection::in_memory().expect("open");
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

    #[test]
    fn deleting_a_superseded_record_removes_it_and_spares_the_replacement() {
        let mut projection = Projection::in_memory().expect("open");
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

    #[test]
    fn a_supersede_of_a_missing_target_still_lands_the_replacement() {
        let mut projection = Projection::in_memory().expect("open");

        projection
            .apply(&mem_superseded(0, "mr-ghost", record("mr-b", "t", "s")))
            .expect("apply");

        assert_eq!(
            projection.memory_record("mr-ghost").expect("memory_record"),
            None
        );
        assert_eq!(index_ids(&projection), ["mr-b"]);
    }

    #[test]
    fn a_delete_of_an_unknown_record_warns_and_no_ops() {
        let mut projection = Projection::in_memory().expect("open");

        projection
            .apply(&mem_deleted(0, "mr-ghost"))
            .expect("apply");

        assert_eq!(projection.last_seq().expect("last_seq"), Some(0));
        assert_eq!(index_ids(&projection), [""; 0]);
    }

    #[test]
    fn double_applying_a_create_fails_and_rolls_back() {
        let mut projection = Projection::in_memory().expect("open");
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
        let mut projection = Projection::in_memory().expect("open");
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
        let mut projection = Projection::in_memory().expect("open");
        projection
            .apply(&mem_created(0, record("mr-a", "old", "s")))
            .expect("apply");
        projection
            .apply(&mem_superseded(1, "mr-a", record("mr-b", "new", "s")))
            .expect("apply");

        let err = projection
            .apply(&mem_superseded(1, "mr-a", record("mr-b", "newer", "s")))
            .expect_err("the seq guard must refuse a replayed supersede");
        assert!(
            matches!(err, Error::StaleMemoryEvent { seq: 1, .. }),
            "got: {err:?}"
        );

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

    #[test]
    fn double_applying_a_delete_warns_and_no_ops() {
        let mut projection = Projection::in_memory().expect("open");
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

    #[test]
    fn unknown_kind_and_status_ints_survive_verbatim() {
        let mut projection = Projection::in_memory().expect("open");
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
        let mut projection = Projection::in_memory().expect("open");

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

        let mut projection = Projection::in_memory().expect("open");
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

        let mut first = Projection::in_memory().expect("open");
        let mut second = Projection::in_memory().expect("open");
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

    fn message_in(seq: u64, session_id: &str, at_seconds: Option<i64>) -> Event {
        Event {
            seq,
            ts: at_seconds.map(|seconds| Timestamp { seconds, nanos: 0 }),
            source: Source::User as i32,
            payload: Some(event::Payload::Session(SessionEvent {
                event: Some(session_event::Event::MessageAppended(MessageAppended {
                    session_id: session_id.to_string(),
                    role: Role::User as i32,
                    content: "spoken".to_string(),
                    partial: false,
                    turn_id: String::new(),
                    ..Default::default()
                })),
            })),
        }
    }

    fn result_in(seq: u64, session_id: &str, at_seconds: i64) -> Event {
        Event {
            seq,
            ts: Some(Timestamp {
                seconds: at_seconds,
                nanos: 0,
            }),
            source: Source::System as i32,
            payload: Some(event::Payload::Session(SessionEvent {
                event: Some(session_event::Event::ToolResultRecorded(
                    ToolResultRecorded {
                        session_id: session_id.to_string(),
                        turn_id: "t-01".to_string(),
                        call_id: format!("c-{seq}"),
                        outcome: ToolOutcome::Ok as i32,
                        content: "tool said".to_string(),
                        truncated: false,
                    },
                )),
            })),
        }
    }

    fn consolidated(seq: u64, session_id: &str, through_seq: u64) -> Event {
        Event {
            seq,
            ts: Some(timestamp()),
            source: Source::System as i32,
            payload: Some(event::Payload::Session(SessionEvent {
                event: Some(session_event::Event::SessionConsolidated(
                    SessionConsolidated {
                        session_id: session_id.to_string(),
                        through_seq,
                        prompt_version: String::new(),
                    },
                )),
            })),
        }
    }

    fn coverage(projection: &Projection, id: &str) -> Option<i64> {
        projection
            .conn
            .query_row(
                "SELECT consolidated_through FROM sessions WHERE id = ?1",
                [id],
                |row| row.get(0),
            )
            .expect("coverage")
    }

    #[test]
    fn a_marker_sets_coverage_and_only_moves_forward() {
        let mut projection = Projection::in_memory().expect("open");
        projection
            .apply(&session_created_as(0, "s-01", "", Some(100)))
            .expect("apply");
        assert_eq!(coverage(&projection, "s-01"), None);

        projection
            .apply(&consolidated(1, "s-01", 5))
            .expect("apply");
        assert_eq!(coverage(&projection, "s-01"), Some(5));

        projection
            .apply(&consolidated(2, "s-01", 3))
            .expect("apply");
        projection
            .apply(&consolidated(3, "s-01", 5))
            .expect("apply");
        assert_eq!(coverage(&projection, "s-01"), Some(5));

        projection
            .apply(&consolidated(4, "s-01", 8))
            .expect("apply");
        assert_eq!(coverage(&projection, "s-01"), Some(8));

        projection
            .apply(&consolidated(5, "s-ghost", 2))
            .expect("apply");
        assert_eq!(projection.last_seq().expect("last_seq"), Some(5));
    }

    #[test]
    fn due_splits_never_consolidated_covered_and_grown_apart() {
        let mut projection = Projection::in_memory().expect("open");
        projection
            .apply(&session_created_as(0, "s-new", "", Some(50)))
            .expect("apply");
        projection
            .apply(&message_in(1, "s-new", Some(100)))
            .expect("apply");
        projection
            .apply(&session_created_as(2, "s-covered", "", Some(50)))
            .expect("apply");
        projection
            .apply(&message_in(3, "s-covered", Some(200)))
            .expect("apply");
        projection
            .apply(&consolidated(4, "s-covered", 3))
            .expect("apply");
        projection
            .apply(&session_created_as(5, "s-grown", "", Some(50)))
            .expect("apply");
        projection
            .apply(&message_in(6, "s-grown", Some(300)))
            .expect("apply");
        projection
            .apply(&consolidated(7, "s-grown", 6))
            .expect("apply");
        projection
            .apply(&message_in(8, "s-grown", Some(400)))
            .expect("apply");

        let due = projection
            .due_for_consolidation(1_000_000_000)
            .expect("due");

        assert_eq!(
            due,
            [
                DueSession {
                    session_id: "s-new".to_string(),
                    latest_seq: 1,
                },
                DueSession {
                    session_id: "s-grown".to_string(),
                    latest_seq: 8,
                },
            ]
        );
    }

    #[test]
    fn the_idle_boundary_is_strict() {
        let mut projection = Projection::in_memory().expect("open");
        projection
            .apply(&session_created_as(0, "s-01", "", Some(50)))
            .expect("apply");
        projection
            .apply(&message_in(1, "s-01", Some(100)))
            .expect("apply");

        assert_eq!(
            projection.due_for_consolidation(100_000_000).expect("due"),
            [],
            "an event at the cutoff is not yet idle"
        );
        assert_eq!(
            projection
                .due_for_consolidation(100_000_001)
                .expect("due")
                .len(),
            1
        );
    }

    #[test]
    fn tool_result_rows_count_as_activity() {
        let mut projection = Projection::in_memory().expect("open");
        projection
            .apply(&session_created_as(0, "s-01", "", Some(50)))
            .expect("apply");
        projection
            .apply(&message_in(1, "s-01", Some(100)))
            .expect("apply");
        projection.apply(&result_in(2, "s-01", 200)).expect("apply");

        assert_eq!(
            projection.due_for_consolidation(150_000_000).expect("due"),
            [],
            "the recent tool result keeps the session out"
        );
        assert_eq!(
            projection.due_for_consolidation(300_000_000).expect("due"),
            [DueSession {
                session_id: "s-01".to_string(),
                latest_seq: 2,
            }],
            "once idle, coverage extends through the tool result"
        );
    }

    #[test]
    fn a_session_with_no_timestamped_rows_is_never_due() {
        let mut projection = Projection::in_memory().expect("open");
        projection
            .apply(&session_created_as(0, "s-01", "", None))
            .expect("apply");
        projection
            .apply(&message_in(1, "s-01", None))
            .expect("apply");

        assert_eq!(projection.due_for_consolidation(i64::MAX).expect("due"), []);
    }

    #[test]
    fn an_empty_session_is_never_due() {
        let mut projection = Projection::in_memory().expect("open");
        projection
            .apply(&session_created_as(0, "s-01", "", Some(50)))
            .expect("apply");

        assert_eq!(projection.due_for_consolidation(i64::MAX).expect("due"), []);
    }

    #[test]
    fn latest_seq_tracks_any_row_kind() {
        let mut projection = Projection::in_memory().expect("open");
        projection
            .apply(&session_created_as(0, "s-01", "", Some(50)))
            .expect("apply");
        assert_eq!(projection.latest_seq("s-01").expect("latest_seq"), None);

        projection
            .apply(&message_in(1, "s-01", Some(100)))
            .expect("apply");
        assert_eq!(projection.latest_seq("s-01").expect("latest_seq"), Some(1));

        projection.apply(&result_in(2, "s-01", 200)).expect("apply");
        assert_eq!(projection.latest_seq("s-01").expect("latest_seq"), Some(2));
    }

    fn memory_at(seq: u64, at_micros: i64, event: memory_event::Event) -> Event {
        let mut wrapped = memory(seq, event);
        wrapped.ts = Some(Timestamp {
            seconds: at_micros / 1_000_000,
            nanos: i32::try_from((at_micros % 1_000_000) * 1_000).expect("in range"),
        });
        wrapped
    }

    fn reviewed(seq: u64, at_micros: i64, id: &str) -> Event {
        memory_at(
            seq,
            at_micros,
            memory_event::Event::RecordReviewed(super::MemoryRecordReviewed {
                record_id: id.to_string(),
            }),
        )
    }

    fn review_ids(projection: &Projection, since: i64) -> Vec<String> {
        projection
            .review_items(since)
            .expect("review_items")
            .into_iter()
            .map(|item| item.record.id)
            .collect()
    }

    #[test]
    fn created_records_enter_the_queue_in_changed_then_id_order() {
        let mut projection = Projection::in_memory().expect("open");
        let created = record("mr-b", "gruvbox", "the palette");
        projection
            .apply(&memory_at(
                0,
                200,
                memory_payload(mem_created(0, created.clone())),
            ))
            .expect("apply");
        projection
            .apply(&memory_at(
                1,
                100,
                memory_payload(mem_created(1, record("mr-a", "jj", "not git"))),
            ))
            .expect("apply");

        assert_eq!(review_ids(&projection, 0), ["mr-a", "mr-b"]);

        let items = projection.review_items(0).expect("review_items");
        assert_eq!(
            items[1],
            ReviewItem {
                record: created,
                changed_at: 200,
                superseded_by: None,
            },
            "the record comes back whole, with its bookkeeping"
        );
    }

    #[test]
    fn a_review_clears_the_record_and_a_later_change_re_enters_it() {
        let mut projection = Projection::in_memory().expect("open");
        projection
            .apply(&memory_at(
                0,
                100,
                memory_payload(mem_created(0, record("mr-a", "jj", "not git"))),
            ))
            .expect("apply");

        projection.apply(&reviewed(1, 150, "mr-a")).expect("apply");
        assert_eq!(review_ids(&projection, 0), Vec::<String>::new());

        projection
            .apply(&memory_at(
                2,
                200,
                memory_payload(mem_updated(2, record("mr-a", "jj", "jj everywhere"))),
            ))
            .expect("apply");
        assert_eq!(
            review_ids(&projection, 0),
            ["mr-a"],
            "a change after the accept re-enters the queue"
        );
    }

    #[test]
    fn the_window_boundary_is_inclusive() {
        let mut projection = Projection::in_memory().expect("open");
        projection
            .apply(&memory_at(
                0,
                100,
                memory_payload(mem_created(0, record("mr-a", "jj", "not git"))),
            ))
            .expect("apply");

        assert_eq!(review_ids(&projection, 100), ["mr-a"]);
        assert_eq!(review_ids(&projection, 101), Vec::<String>::new());
    }

    #[test]
    fn a_supersede_puts_both_rows_in_the_queue() {
        let mut projection = Projection::in_memory().expect("open");
        projection
            .apply(&memory_at(
                0,
                100,
                memory_payload(mem_created(0, record("mr-old", "address", "lives at X"))),
            ))
            .expect("apply");
        projection
            .apply(&reviewed(1, 150, "mr-old"))
            .expect("apply");

        projection
            .apply(&memory_at(
                2,
                200,
                memory_payload(mem_superseded(
                    2,
                    "mr-old",
                    record("mr-new", "address", "lives at Y"),
                )),
            ))
            .expect("apply");

        let items = projection.review_items(0).expect("review_items");
        assert_eq!(
            items
                .iter()
                .map(|item| (
                    item.record.id.as_str(),
                    item.record.status,
                    item.superseded_by.as_deref(),
                    item.changed_at,
                ))
                .collect::<Vec<_>>(),
            [
                ("mr-new", memory_record::Status::Active as i32, None, 200i64),
                (
                    "mr-old",
                    memory_record::Status::Superseded as i32,
                    Some("mr-new"),
                    200
                ),
            ],
            "the retired record and its replacement both await a verdict"
        );
    }

    #[test]
    fn a_deleted_record_leaves_the_queue_entirely() {
        let mut projection = Projection::in_memory().expect("open");
        projection
            .apply(&memory_at(
                0,
                100,
                memory_payload(mem_created(0, record("mr-a", "jj", "not git"))),
            ))
            .expect("apply");
        projection
            .apply(&memory_at(1, 200, memory_payload(mem_deleted(1, "mr-a"))))
            .expect("apply");

        assert_eq!(review_ids(&projection, 0), Vec::<String>::new());
    }

    #[test]
    fn a_review_of_an_unknown_record_warns_and_no_ops() {
        let mut projection = Projection::in_memory().expect("open");

        projection
            .apply(&reviewed(0, 100, "mr-ghost"))
            .expect("skipped, not an error");

        assert_eq!(projection.last_seq().expect("last_seq"), Some(0));
        assert_eq!(row_count(&projection, "memory_records"), 0);
    }

    #[test]
    fn the_review_stamp_only_moves_forward() {
        let mut projection = Projection::in_memory().expect("open");
        projection
            .apply(&memory_at(
                0,
                400,
                memory_payload(mem_created(0, record("mr-a", "jj", "not git"))),
            ))
            .expect("apply");
        projection.apply(&reviewed(1, 500, "mr-a")).expect("apply");

        projection.apply(&reviewed(2, 300, "mr-a")).expect("apply");

        assert_eq!(review_ids(&projection, 0), Vec::<String>::new());
        assert_eq!(projection.last_seq().expect("last_seq"), Some(2));
    }

    #[test]
    fn a_review_with_no_timestamp_is_skipped() {
        let mut projection = Projection::in_memory().expect("open");
        projection
            .apply(&memory_at(
                0,
                100,
                memory_payload(mem_created(0, record("mr-a", "jj", "not git"))),
            ))
            .expect("apply");

        let mut unstamped = reviewed(1, 0, "mr-a");
        unstamped.ts = None;
        projection.apply(&unstamped).expect("skipped, not an error");

        assert_eq!(
            review_ids(&projection, 0),
            ["mr-a"],
            "an unorderable review reviews nothing"
        );
    }

    #[test]
    fn a_record_with_no_timestamp_never_enters_the_queue() {
        let mut projection = Projection::in_memory().expect("open");
        let mut unstamped = mem_created(0, record("mr-a", "jj", "not git"));
        unstamped.ts = None;
        projection.apply(&unstamped).expect("apply");

        assert_eq!(review_ids(&projection, i64::MIN), Vec::<String>::new());
    }

    fn changed_at(projection: &Projection, id: &str) -> Option<i64> {
        projection
            .conn
            .query_row(
                "SELECT changed_at FROM memory_records WHERE id = ?1",
                [id],
                |row| row.get(0),
            )
            .expect("changed_at")
    }

    fn last_event_seq(projection: &Projection, id: &str) -> i64 {
        projection
            .conn
            .query_row(
                "SELECT last_event_seq FROM memory_records WHERE id = ?1",
                [id],
                |row| row.get(0),
            )
            .expect("last_event_seq")
    }

    #[test]
    fn a_supersede_stamps_changed_at_on_active_link_dependents() {
        let mut projection = Projection::in_memory().expect("open");
        let mut a = record("mr-a", "old address", "lives at X");
        a.links = vec![];
        projection
            .apply(&memory_at(0, 50, memory_payload(mem_created(0, a))))
            .expect("apply");

        let mut b = record("mr-b", "commute", "bikes past mr-a's place");
        b.links = vec!["mr-a".to_string()];
        projection
            .apply(&memory_at(1, 60, memory_payload(mem_created(1, b))))
            .expect("apply");

        let mut c = record("mr-c", "unrelated", "unrelated fact");
        c.links = vec![];
        projection
            .apply(&memory_at(2, 60, memory_payload(mem_created(2, c))))
            .expect("apply");

        assert_eq!(
            review_ids(&projection, 250),
            Vec::<String>::new(),
            "mr-b's changed_at of 60 falls outside a window starting at 250"
        );

        let mut a2 = record("mr-a2", "new address", "lives at Y");
        a2.links = vec![];
        projection
            .apply(&memory_at(
                3,
                300,
                memory_payload(mem_superseded(3, "mr-a", a2)),
            ))
            .expect("apply");

        assert_eq!(
            review_ids(&projection, 250),
            ["mr-a", "mr-a2", "mr-b"],
            "the supersede's own rows and mr-b, its link dependent, now land in the window"
        );
        assert_eq!(
            changed_at(&projection, "mr-c"),
            Some(60),
            "mr-c has no link to mr-a and stays untouched"
        );
    }

    #[test]
    fn a_delete_stamps_changed_at_on_active_link_dependents() {
        let mut projection = Projection::in_memory().expect("open");
        let mut a = record("mr-a", "old note", "to delete");
        a.links = vec![];
        projection
            .apply(&memory_at(0, 50, memory_payload(mem_created(0, a))))
            .expect("apply");

        let mut b = record("mr-b", "commute", "bikes past mr-a's place");
        b.links = vec!["mr-a".to_string()];
        projection
            .apply(&memory_at(1, 60, memory_payload(mem_created(1, b))))
            .expect("apply");

        let mut c = record("mr-c", "unrelated", "unrelated fact");
        c.links = vec![];
        projection
            .apply(&memory_at(2, 60, memory_payload(mem_created(2, c))))
            .expect("apply");

        assert_eq!(review_ids(&projection, 250), Vec::<String>::new());

        projection
            .apply(&memory_at(3, 300, memory_payload(mem_deleted(3, "mr-a"))))
            .expect("apply");

        assert_eq!(
            review_ids(&projection, 250),
            ["mr-b"],
            "the deleted mr-a leaves no row; mr-b, its link dependent, re-enters"
        );
        assert_eq!(
            changed_at(&projection, "mr-c"),
            Some(60),
            "mr-c has no link to mr-a and stays untouched"
        );
    }

    #[test]
    fn a_superseded_dependent_is_not_stamped() {
        let mut projection = Projection::in_memory().expect("open");
        let mut a = record("mr-a", "note", "target");
        a.links = vec![];
        projection
            .apply(&memory_at(0, 50, memory_payload(mem_created(0, a))))
            .expect("apply");

        let mut d = record("mr-d", "old dependent", "links to mr-a");
        d.links = vec!["mr-a".to_string()];
        projection
            .apply(&memory_at(1, 60, memory_payload(mem_created(1, d))))
            .expect("apply");

        let mut d2 = record("mr-d2", "new dependent", "supersedes mr-d");
        d2.links = vec![];
        projection
            .apply(&memory_at(
                2,
                100,
                memory_payload(mem_superseded(2, "mr-d", d2)),
            ))
            .expect("apply");

        projection
            .apply(&memory_at(3, 300, memory_payload(mem_deleted(3, "mr-a"))))
            .expect("apply");

        assert_eq!(
            changed_at(&projection, "mr-d"),
            Some(100),
            "a SUPERSEDED row does not re-enter review just because it once linked to mr-a"
        );
    }

    #[test]
    fn an_in_place_supersede_does_not_stamp_the_replacement_itself() {
        let mut projection = Projection::in_memory().expect("open");
        let mut e = record("mr-e", "old self", "self-linking record");
        e.links = vec![];
        projection
            .apply(&memory_at(0, 50, memory_payload(mem_created(0, e))))
            .expect("apply");

        let mut f = record("mr-f", "dependent", "leans on mr-e");
        f.links = vec!["mr-e".to_string()];
        projection
            .apply(&memory_at(1, 60, memory_payload(mem_created(1, f))))
            .expect("apply");

        let mut e2 = record("mr-e", "new self", "still self-linking");
        e2.links = vec!["mr-e".to_string()];
        projection
            .apply(&memory_at(
                2,
                300,
                memory_payload(mem_superseded(2, "mr-e", e2)),
            ))
            .expect("apply");

        assert_eq!(
            changed_at(&projection, "mr-e"),
            Some(300),
            "the replacement's own changed_at comes from the upsert, not a self-stamp"
        );
        assert_eq!(
            changed_at(&projection, "mr-f"),
            Some(300),
            "an unrelated dependent still gets stamped by the same in-place supersede"
        );
    }

    #[test]
    fn the_stamp_never_moves_changed_at_backwards() {
        let mut projection = Projection::in_memory().expect("open");
        let mut a = record("mr-a", "note", "target");
        a.links = vec![];
        projection
            .apply(&memory_at(0, 50, memory_payload(mem_created(0, a))))
            .expect("apply");

        let mut b = record("mr-b", "dependent", "leans on mr-a");
        b.links = vec!["mr-a".to_string()];
        projection
            .apply(&memory_at(1, 200, memory_payload(mem_created(1, b))))
            .expect("apply");

        projection
            .apply(&memory_at(2, 100, memory_payload(mem_deleted(2, "mr-a"))))
            .expect("apply");

        assert_eq!(
            changed_at(&projection, "mr-b"),
            Some(200),
            "a stamp older than the current changed_at is a no-op"
        );
    }

    #[test]
    fn the_stamp_leaves_last_event_seq_untouched_so_later_updates_still_apply() {
        let mut projection = Projection::in_memory().expect("open");
        let mut a = record("mr-a", "note", "target");
        a.links = vec![];
        projection
            .apply(&memory_at(0, 50, memory_payload(mem_created(0, a))))
            .expect("apply");

        let mut b = record("mr-b", "dependent", "leans on mr-a");
        b.links = vec!["mr-a".to_string()];
        projection
            .apply(&memory_at(1, 60, memory_payload(mem_created(1, b))))
            .expect("apply");

        projection
            .apply(&memory_at(2, 300, memory_payload(mem_deleted(2, "mr-a"))))
            .expect("apply");
        assert_eq!(
            last_event_seq(&projection, "mr-b"),
            1,
            "the stamp does not touch last_event_seq"
        );

        let mut b2 = record("mr-b", "dependent", "leans on mr-a, updated");
        b2.links = vec!["mr-a".to_string()];
        projection
            .apply(&memory_at(3, 400, memory_payload(mem_updated(3, b2))))
            .expect("a later legitimate update still applies");

        let state = projection
            .memory_record("mr-b")
            .expect("memory_record")
            .expect("the record exists");
        assert_eq!(state.record.summary, "leans on mr-a, updated");
    }

    #[test]
    fn a_stamp_re_enters_a_reviewed_dependent_into_the_queue() {
        let mut projection = Projection::in_memory().expect("open");
        let mut a = record("mr-a", "note", "target");
        a.links = vec![];
        projection
            .apply(&memory_at(0, 50, memory_payload(mem_created(0, a))))
            .expect("apply");

        let mut b = record("mr-b", "dependent", "leans on mr-a");
        b.links = vec!["mr-a".to_string()];
        projection
            .apply(&memory_at(1, 60, memory_payload(mem_created(1, b))))
            .expect("apply");

        projection.apply(&reviewed(2, 55, "mr-a")).expect("apply");
        projection.apply(&reviewed(3, 70, "mr-b")).expect("apply");
        assert_eq!(
            review_ids(&projection, 0),
            Vec::<String>::new(),
            "reviewed after their last change, mr-a and mr-b are both settled"
        );

        projection
            .apply(&memory_at(4, 300, memory_payload(mem_deleted(4, "mr-a"))))
            .expect("apply");

        assert_eq!(
            review_ids(&projection, 0),
            ["mr-b"],
            "the stamp moves changed_at past reviewed_at, re-opening mr-b"
        );
    }

    fn active_ids_at(projection: &Projection, at: i64) -> Vec<String> {
        projection
            .memory_active_at(at)
            .expect("memory_active_at")
            .into_iter()
            .map(|record| record.id)
            .collect()
    }

    #[test]
    fn active_at_walks_a_supersede_chain() {
        let mut projection = Projection::in_memory().expect("open");
        let a = record("mr-a", "address", "lives at X");
        projection
            .apply(&memory_at(0, 50, memory_payload(mem_created(0, a))))
            .expect("apply");
        let b = record("mr-b", "bicycle", "rides a bicycle");
        projection
            .apply(&memory_at(1, 60, memory_payload(mem_created(1, b))))
            .expect("apply");
        let a2 = record("mr-a2", "address", "lives at Y");
        projection
            .apply(&memory_at(
                2,
                100,
                memory_payload(mem_superseded(2, "mr-a", a2)),
            ))
            .expect("apply");

        assert_eq!(
            active_ids_at(&projection, 49),
            Vec::<String>::new(),
            "nothing was held before the first record"
        );
        assert_eq!(
            active_ids_at(&projection, 70),
            ["mr-a", "mr-b"],
            "the superseded mr-a was still the held fact at 70"
        );
        assert_eq!(
            active_ids_at(&projection, 100),
            ["mr-a2", "mr-b"],
            "the interval is [created_at, superseded_at): at 100 the replacement holds"
        );
    }

    #[test]
    fn a_purged_record_is_absent_from_every_point_in_time() {
        let mut projection = Projection::in_memory().expect("open");
        let a = record("mr-a", "address", "lives at X");
        projection
            .apply(&memory_at(0, 50, memory_payload(mem_created(0, a))))
            .expect("apply");
        projection
            .apply(&memory_at(1, 100, memory_payload(mem_deleted(1, "mr-a"))))
            .expect("apply");

        assert_eq!(
            active_ids_at(&projection, 70),
            Vec::<String>::new(),
            "a purge erases the record from history, not just from now"
        );
    }

    #[test]
    fn an_in_place_supersede_keeps_its_activation_time() {
        let mut projection = Projection::in_memory().expect("open");
        let a = record("mr-a", "address", "lives at X");
        projection
            .apply(&memory_at(0, 50, memory_payload(mem_created(0, a))))
            .expect("apply");
        let revised = record("mr-a", "address", "lives at X, corrected");
        projection
            .apply(&memory_at(
                1,
                100,
                memory_payload(mem_superseded(1, "mr-a", revised)),
            ))
            .expect("apply");

        let held = projection.memory_active_at(70).expect("memory_active_at");
        assert_eq!(
            held.iter().map(|r| r.id.as_str()).collect::<Vec<_>>(),
            ["mr-a"],
            "an in-place revision is the same record, held since 50"
        );
        assert_eq!(
            held[0].summary, "lives at X, corrected",
            "content is the last-known form, not a reconstruction"
        );
    }

    #[test]
    fn a_supersede_without_a_timestamp_excludes_the_row_rather_than_guessing() {
        let mut projection = Projection::in_memory().expect("open");
        let a = record("mr-a", "address", "lives at X");
        projection
            .apply(&memory_at(0, 50, memory_payload(mem_created(0, a))))
            .expect("apply");
        let a2 = record("mr-a2", "address", "lives at Y");
        let mut untimestamped = memory(1, memory_payload(mem_superseded(1, "mr-a", a2)));
        untimestamped.ts = None;
        projection.apply(&untimestamped).expect("apply");

        assert_eq!(
            active_ids_at(&projection, 70),
            Vec::<String>::new(),
            "with no supersede timestamp, mr-a's interval end is unknown and it is \
             excluded; the untimestamped mr-a2 is excluded by its missing created_at"
        );
    }

    #[test]
    fn a_system_sourced_message_carries_its_source_into_history() {
        let mut projection = Projection::in_memory().expect("open");
        projection.apply(&session_created(0)).expect("apply");
        let mut event = message_appended(1, "job s-x finished.");
        event.source = Source::System as i32;
        projection.apply(&event).expect("apply");

        let rows = projection.messages("s-01").expect("rows");
        let MessageRow::Message { source, .. } = &rows[0] else {
            panic!("expected a message row, got {:?}", rows[0]);
        };
        assert_eq!(*source, Source::System as i32);
        let entry = super::history_entry(rows.into_iter().next().expect("row"));
        let Some(arc_proto::v1::history_entry::Entry::Message(message)) = entry.entry else {
            panic!("expected a message entry");
        };
        assert_eq!(message.source, Source::System as i32);
    }

    fn build_rebuild_log(dir: &Path) -> Log {
        let mut log = Log::open(dir).expect("open log");
        log.append(session_created_with_grants(
            0,
            vec![WorkspaceGrant {
                root: "/home/bogdan/arc".to_owned(),
                read_write: true,
            }],
        ))
        .expect("append");
        log.append(message_appended(1, "hello")).expect("append");
        log.append(tool_call(2, "c-a", 0, "{}")).expect("append");
        log.append(tool_result(
            3,
            "c-a",
            ToolOutcome::Ok as i32,
            "found",
            false,
        ))
        .expect("append");
        log.append(mem_created(4, record("mr-1", "title", "summary")))
            .expect("append");
        log
    }

    fn build_live_index(log_dir: &Path, index_path: &Path) {
        let mut projection = Projection::open(index_path).expect("open live index");
        let log = Log::open(log_dir).expect("reopen log");
        replay(log.reader().expect("reader"), &mut projection).expect("replay");
    }

    #[test]
    fn rebuild_reports_identical_state_with_matching_counts() {
        let log_dir = TempDir::new().expect("log dir");
        build_rebuild_log(log_dir.path());
        let index_dir = TempDir::new().expect("index dir");
        let index_path = index_dir.path().join("index.db");
        build_live_index(log_dir.path(), &index_path);

        let report = super::rebuild(log_dir.path(), &index_path).expect("rebuild");

        assert!(report.is_clean(), "{report:?}");
        assert_eq!(report.schema_version_live, report.schema_version_replayed);
        let counts: Vec<(&str, usize)> = report
            .tables
            .iter()
            .map(|table| (table.table, table.rows_live))
            .collect();
        assert_eq!(
            counts,
            [
                ("sessions", 1),
                ("messages", 3),
                ("memory_records", 1),
                ("session_grants", 1),
            ]
        );
    }

    #[test]
    fn a_mutated_row_is_reported_as_a_divergence() {
        let log_dir = TempDir::new().expect("log dir");
        build_rebuild_log(log_dir.path());
        let index_dir = TempDir::new().expect("index dir");
        let index_path = index_dir.path().join("index.db");
        build_live_index(log_dir.path(), &index_path);
        {
            let conn = Connection::open(&index_path).expect("reopen for sabotage");
            conn.execute(
                "UPDATE sessions SET title = 'tampered' WHERE id = 's-01'",
                [],
            )
            .expect("mutate");
        }

        let report = super::rebuild(log_dir.path(), &index_path).expect("rebuild");

        assert!(!report.is_clean());
        let sessions = report
            .tables
            .iter()
            .find(|table| table.table == "sessions")
            .expect("sessions table");
        let divergence = sessions.divergence.as_ref().expect("divergence");
        assert_eq!(divergence.key, "s-01");
        assert_ne!(divergence.live, divergence.replayed);
    }

    #[test]
    fn an_extra_live_row_is_a_divergence_not_a_panic() {
        let log_dir = TempDir::new().expect("log dir");
        build_rebuild_log(log_dir.path());
        let index_dir = TempDir::new().expect("index dir");
        let index_path = index_dir.path().join("index.db");
        build_live_index(log_dir.path(), &index_path);
        {
            let conn = Connection::open(&index_path).expect("reopen for sabotage");
            conn.execute(
                "INSERT INTO memory_records
                     (id, kind, namespace, title, summary, body, links, status,
                      created_seq, last_event_seq)
                 VALUES ('mr-extra', 0, 'global', 'extra', 'extra', 'extra', '[]', 0, 999, 999)",
                [],
            )
            .expect("insert extra row");
        }

        let report = super::rebuild(log_dir.path(), &index_path).expect("rebuild");

        assert!(!report.is_clean());
        let memory_records = report
            .tables
            .iter()
            .find(|table| table.table == "memory_records")
            .expect("memory_records table");
        assert_eq!(memory_records.rows_live, 2);
        assert_eq!(memory_records.rows_replayed, 1);
        assert!(memory_records.divergence.is_some());
    }

    #[test]
    fn a_schema_version_mismatch_is_a_reported_divergence() {
        let log_dir = TempDir::new().expect("log dir");
        build_rebuild_log(log_dir.path());
        let index_dir = TempDir::new().expect("index dir");
        let index_path = index_dir.path().join("index.db");
        build_live_index(log_dir.path(), &index_path);
        {
            let conn = Connection::open(&index_path).expect("reopen for sabotage");
            conn.execute(
                "UPDATE projection_meta SET value = ?1 WHERE key = 'schema_version'",
                [i64::from(super::SCHEMA_VERSION) - 1],
            )
            .expect("age the version");
        }

        let report = super::rebuild(log_dir.path(), &index_path).expect("rebuild");

        assert!(!report.is_clean());
        assert_eq!(report.schema_version_live, Some(super::SCHEMA_VERSION - 1));
        assert_eq!(report.schema_version_replayed, Some(super::SCHEMA_VERSION));
    }

    #[test]
    fn a_missing_live_index_is_an_error_not_a_panic() {
        let log_dir = TempDir::new().expect("log dir");
        build_rebuild_log(log_dir.path());
        let missing = log_dir.path().join("does-not-exist.db");

        let err = super::rebuild(log_dir.path(), &missing).expect_err("missing index");
        assert!(matches!(err, super::RebuildError::Index(_)), "{err:?}");
    }

    #[test]
    fn a_missing_log_dir_is_an_error_not_a_panic() {
        let index_dir = TempDir::new().expect("index dir");
        let index_path = index_dir.path().join("index.db");
        drop(Projection::open(&index_path).expect("create empty index"));
        let missing_log = index_dir.path().join("no-such-log");

        let err = super::rebuild(&missing_log, &index_path).expect_err("missing log dir");
        assert!(matches!(err, super::RebuildError::Log(_)), "{err:?}");
    }
}
