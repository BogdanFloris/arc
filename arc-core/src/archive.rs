//! Read-only queries over the projection index: the archive tools' query
//! layer (DESIGN.md §5.5 — search cheap, read targeted).
//!
//! [`Archive`] owns its own read-only connection to `index.db`; it never
//! touches [`crate::projection::Projection`]'s write connection. The engine
//! commits every event before dispatching tools, so a mid-turn read here sees
//! the turn so far. No LLM anywhere in this path: every shape returns actual
//! rows from the index.
//!
//! The query sanitizer and the return shapes follow hermes-agent's
//! production-tested policy (`docs/prior-art-hermes.md` §2): protect quoted
//! phrases, quote terms the tokenizer would split, trim dangling operators,
//! and still catch the FTS syntax error at the execute site — a query FTS
//! cannot parse must surface as an answer naming the problem, never as a
//! silently empty result.

use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard, PoisonError};

use arc_proto::v1::Role;
use chrono::{DateTime, SecondsFormat};
use rusqlite::{Connection, OpenFlags, OptionalExtension};
use serde::Serialize;

use crate::projection::KIND_MESSAGE;

/// Longest raw query the sanitizer looks at; the rest is dropped.
const MAX_QUERY_CHARS: usize = 256;

/// Raw FTS hits fetched before the per-session dedupe.
const OVERFETCH: usize = 50;

/// Session slots a search returns after the dedupe.
const MAX_SESSIONS: usize = 5;

/// Per-message char budget in hydrated context and bookends.
const MESSAGE_BUDGET_CHARS: usize = 300;

/// Prose messages either side of the anchor in the hydrated window.
const WINDOW_RADIUS: usize = 2;

/// Messages per bookend: a session's opening and closing few.
const BOOKEND_LEN: usize = 2;

/// Prose rows a range read returns before it clips.
const MAX_RANGE_ROWS: usize = 50;

/// Everything that can go wrong in the archive query layer.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The index file could not be opened read-only.
    #[error("archive index {path}: {source}")]
    Open {
        /// Index file the open was against.
        path: PathBuf,
        /// Underlying failure.
        #[source]
        source: rusqlite::Error,
    },

    /// FTS could not parse the query, even sanitized. Callers answer "no
    /// results, and here is why" — this is a property of the query, not of
    /// the index.
    #[error("the query could not be parsed: {message}")]
    Query {
        /// What FTS objected to.
        message: String,
    },

    /// A statement against the index failed.
    #[error("archive index: {0}")]
    Sqlite(#[from] rusqlite::Error),
}

/// What a search returns: up to [`MAX_SESSIONS`] sessions, best BM25 hit
/// first, the top one hydrated.
#[derive(Debug, Serialize)]
pub struct SearchReply {
    /// One slot per session, best hit first.
    pub sessions: Vec<SessionHit>,
}

/// One session a search matched, anchored at its best-ranked hit.
#[derive(Debug, Serialize)]
pub struct SessionHit {
    /// The session, as `session_read` wants it.
    pub session_id: String,
    /// Session title, omitted while nothing writes titles.
    #[serde(skip_serializing_if = "String::is_empty")]
    pub title: String,
    /// When the session started, RFC 3339 UTC. Omitted if the event carried
    /// no timestamp.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub started_at: Option<String>,
    /// FTS snippet around the match.
    pub snippet: String,
    /// Seq of the matched row — the range anchor for `session_read`.
    pub anchor_seq: i64,
    /// Top hit only: prose messages around the anchor.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub context: Vec<ProseMessage>,
    /// Top hit only: the session's opening messages (the goal), minus any
    /// already in `context`.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub first: Vec<ProseMessage>,
    /// Top hit only: the session's closing messages (the resolution), minus
    /// any already in `context` or `first`.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub last: Vec<ProseMessage>,
}

/// One prose message, as the model reads it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ProseMessage {
    /// Log seq: what ranges are addressed by.
    pub seq: i64,
    /// `user` / `assistant` / `system`, or `role_<n>` for a value this build
    /// does not know.
    pub role: String,
    /// The message text, budget-clipped where the shape says so.
    pub content: String,
    /// The reply was cut before the model finished. Omitted when false.
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub partial: bool,
}

/// What a range read returns.
#[derive(Debug, Serialize)]
pub struct ReadReply {
    /// The session read.
    pub session_id: String,
    /// Prose rows in the requested seq range, in order.
    pub messages: Vec<ProseMessage>,
    /// The range held more than [`MAX_RANGE_ROWS`] prose rows and was cut.
    /// Omitted when false.
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub clipped: bool,
}

/// What an ends read returns: bookends only.
#[derive(Debug, Serialize)]
pub struct EndsReply {
    /// The session read.
    pub session_id: String,
    /// The session's first user/assistant messages.
    pub first: Vec<ProseMessage>,
    /// The session's last user/assistant messages, minus any in `first`.
    pub last: Vec<ProseMessage>,
}

/// A read-only view of the projection index.
pub struct Archive {
    conn: Mutex<Connection>,
}

impl Archive {
    /// Opens `path` read-only. The file must already exist — the projection
    /// writes it, this layer never does.
    ///
    /// # Errors
    ///
    /// [`Error::Open`] if the file cannot be opened.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, Error> {
        let path = path.as_ref();
        let conn = Connection::open_with_flags(
            path,
            OpenFlags::SQLITE_OPEN_READ_ONLY
                | OpenFlags::SQLITE_OPEN_URI
                | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )
        .map_err(|source| Error::Open {
            path: path.to_path_buf(),
            source,
        })?;
        Ok(Self {
            conn: Mutex::new(conn),
        })
    }

    /// FTS search over the archive: overfetch raw hits, dedupe to one slot
    /// per session keeping its best BM25 hit, hydrate the top session with a
    /// context window and bookends.
    ///
    /// Prose rows only by default; `include_tool_results` lifts the kind
    /// filter so tool output is searched too.
    ///
    /// # Errors
    ///
    /// - [`Error::Query`] if nothing searchable survives sanitization, or FTS
    ///   still rejects the sanitized query.
    /// - [`Error::Sqlite`] for any other index failure.
    #[tracing::instrument(
        name = "archive.search",
        skip_all,
        fields(
            query = tracing::field::Empty,
            raw_hits = tracing::field::Empty,
            sessions = tracing::field::Empty,
            include_tool_results,
        )
    )]
    pub fn search(
        &self,
        raw_query: &str,
        include_tool_results: bool,
    ) -> Result<SearchReply, Error> {
        let query = sanitize_query(raw_query);
        let span = tracing::Span::current();
        span.record("query", query.as_str());
        if query.is_empty() {
            return Err(Error::Query {
                message: "no searchable words in the query".to_owned(),
            });
        }

        let conn = self.lock();
        let mut stmt = conn.prepare(
            "SELECT m.session_id, m.seq, snippet(messages_fts, 0, '', '', '…', 12)
             FROM messages_fts JOIN messages m ON m.seq = messages_fts.rowid
             WHERE messages_fts MATCH ?1 AND (?2 OR m.kind = ?3)
             ORDER BY bm25(messages_fts)
             LIMIT ?4",
        )?;
        let mapped = stmt
            .query_map(
                rusqlite::params![query, include_tool_results, KIND_MESSAGE, limit(OVERFETCH)],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, String>(2)?,
                    ))
                },
            )
            .map_err(into_query_error)?;
        // The MATCH string is parsed at the first step, so the syntax error
        // for a query the sanitizer let through lands here — caught, per
        // hermes policy, not trusted to be impossible.
        let mut hits = Vec::new();
        for hit in mapped {
            hits.push(hit.map_err(into_query_error)?);
        }
        span.record("raw_hits", hits.len());

        let mut sessions: Vec<SessionHit> = Vec::new();
        for (session_id, anchor_seq, snippet) in hits {
            if sessions.iter().any(|s| s.session_id == session_id) {
                continue;
            }
            let (title, started_at) = conn
                .query_row(
                    "SELECT coalesce(title, ''), started_at FROM sessions WHERE id = ?1",
                    [&session_id],
                    |row| Ok((row.get::<_, String>(0)?, row.get::<_, Option<i64>>(1)?)),
                )
                .optional()?
                .unwrap_or((String::new(), None));
            sessions.push(SessionHit {
                session_id,
                title,
                started_at: started_at.and_then(format_micros),
                snippet,
                anchor_seq,
                context: Vec::new(),
                first: Vec::new(),
                last: Vec::new(),
            });
            if sessions.len() == MAX_SESSIONS {
                break;
            }
        }

        // Hydrating only the top hit: one call from hit to goal-and-
        // resolution, without five sessions' worth of context in an 8k
        // window.
        if let Some(top) = sessions.first_mut() {
            let context = prose_window(&conn, &top.session_id, top.anchor_seq)?;
            let (mut first, mut last) = bookends(&conn, &top.session_id)?;
            first.retain(|m| !context.iter().any(|c| c.seq == m.seq));
            last.retain(|m| !context.iter().any(|c| c.seq == m.seq));
            top.context = context;
            top.first = first;
            top.last = last;
        }

        span.record("sessions", sessions.len());
        Ok(SearchReply { sessions })
    }

    /// Prose rows of one session in a seq range, in order, capped at
    /// [`MAX_RANGE_ROWS`]. `None` means no such session.
    ///
    /// # Errors
    ///
    /// [`Error::Sqlite`] if the index cannot be read.
    #[tracing::instrument(
        name = "archive.read",
        skip_all,
        fields(session_id = %session_id, shape = "range", rows = tracing::field::Empty)
    )]
    pub fn read_range(
        &self,
        session_id: &str,
        start_seq: i64,
        end_seq: i64,
    ) -> Result<Option<ReadReply>, Error> {
        let conn = self.lock();
        if !session_exists(&conn, session_id)? {
            return Ok(None);
        }
        let mut stmt = conn.prepare(
            "SELECT seq, role, content, partial FROM messages
             WHERE session_id = ?1 AND kind = ?2 AND seq BETWEEN ?3 AND ?4
             ORDER BY seq LIMIT ?5",
        )?;
        let mapped = stmt.query_map(
            rusqlite::params![
                session_id,
                KIND_MESSAGE,
                start_seq,
                end_seq,
                limit(MAX_RANGE_ROWS + 1)
            ],
            prose_row,
        )?;
        let mut messages = Vec::new();
        for row in mapped {
            messages.push(row?);
        }
        let clipped = messages.len() > MAX_RANGE_ROWS;
        messages.truncate(MAX_RANGE_ROWS);
        tracing::Span::current().record("rows", messages.len());
        Ok(Some(ReadReply {
            session_id: session_id.to_owned(),
            messages,
            clipped,
        }))
    }

    /// A session's bookends: its first and last [`BOOKEND_LEN`]
    /// user/assistant messages. `None` means no such session.
    ///
    /// # Errors
    ///
    /// [`Error::Sqlite`] if the index cannot be read.
    #[tracing::instrument(
        name = "archive.read",
        skip_all,
        fields(session_id = %session_id, shape = "ends")
    )]
    pub fn ends(&self, session_id: &str) -> Result<Option<EndsReply>, Error> {
        let conn = self.lock();
        if !session_exists(&conn, session_id)? {
            return Ok(None);
        }
        let (first, last) = bookends(&conn, session_id)?;
        Ok(Some(EndsReply {
            session_id: session_id.to_owned(),
            first,
            last,
        }))
    }

    /// The connection, poisoned or not: a read-only handle has no state a
    /// panicked holder could have half-written.
    fn lock(&self) -> MutexGuard<'_, Connection> {
        self.conn.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// Makes raw model text safe for FTS5 `MATCH` — hermes policy in full.
///
/// Cap length; protect `"quoted phrases"` with a linear scan; keep purely
/// alphanumeric words bare; wrap anything else in quotes so `my-app.config.ts`
/// matches as a phrase instead of tokenizer-split AND-terms (quoting also
/// neutralizes the whole FTS5 special-char class); drop tokens with nothing
/// searchable in them; trim dangling `AND`/`OR`/`NOT`.
#[must_use]
pub fn sanitize_query(raw: &str) -> String {
    let capped: String = raw.chars().take(MAX_QUERY_CHARS).collect();
    let mut terms: Vec<String> = Vec::new();
    let mut rest = capped.as_str();
    loop {
        let Some(open) = rest.find('"') else {
            push_bare_terms(&mut terms, rest);
            break;
        };
        push_bare_terms(&mut terms, &rest[..open]);
        let after = &rest[open + 1..];
        // An unterminated quote runs to the end: the user meant a phrase.
        let (phrase, tail) = after
            .find('"')
            .map_or((after, ""), |close| (&after[..close], &after[close + 1..]));
        if phrase.chars().any(char::is_alphanumeric) {
            terms.push(format!("\"{phrase}\""));
        }
        rest = tail;
    }
    while terms.first().is_some_and(|t| is_operator(t)) {
        terms.remove(0);
    }
    while terms.last().is_some_and(|t| is_operator(t)) {
        terms.pop();
    }
    terms.join(" ")
}

/// Sanitizes one unquoted stretch into `terms`.
fn push_bare_terms(terms: &mut Vec<String>, text: &str) {
    for token in text.split_whitespace() {
        if !token.chars().any(char::is_alphanumeric) {
            continue;
        }
        if token.chars().all(char::is_alphanumeric) {
            terms.push(token.to_owned());
        } else {
            terms.push(format!("\"{}\"", token.replace('"', "")));
        }
    }
}

/// FTS5 boolean operators, which are only valid between terms.
fn is_operator(term: &str) -> bool {
    matches!(term, "AND" | "OR" | "NOT")
}

/// Wraps an error from executing a `MATCH` query. FTS5 parse failures become
/// [`Error::Query`] — a fact about the query — while anything else stays a
/// real index error.
fn into_query_error(source: rusqlite::Error) -> Error {
    let text = source.to_string();
    if text.contains("fts5") {
        Error::Query { message: text }
    } else {
        Error::Sqlite(source)
    }
}

/// Whether `sessions` has a row for `id`.
fn session_exists(conn: &Connection, id: &str) -> Result<bool, Error> {
    Ok(conn
        .query_row("SELECT 1 FROM sessions WHERE id = ?1", [id], |_| Ok(()))
        .optional()?
        .is_some())
}

/// Maps a `(seq, role, content, partial)` row.
fn prose_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<ProseMessage> {
    Ok(ProseMessage {
        seq: row.get(0)?,
        role: role_name(row.get(1)?),
        content: row.get(2)?,
        partial: row.get(3)?,
    })
}

/// Prose messages around the anchor: up to [`WINDOW_RADIUS`] each side, the
/// anchor row included when it is prose. Budget-clipped.
fn prose_window(
    conn: &Connection,
    session_id: &str,
    anchor_seq: i64,
) -> Result<Vec<ProseMessage>, Error> {
    let mut stmt = conn.prepare(
        "SELECT seq, role, content, partial FROM messages
         WHERE session_id = ?1 AND kind = ?2 AND seq <= ?3
         ORDER BY seq DESC LIMIT ?4",
    )?;
    let mapped = stmt.query_map(
        rusqlite::params![
            session_id,
            KIND_MESSAGE,
            anchor_seq,
            limit(WINDOW_RADIUS + 1)
        ],
        prose_row,
    )?;
    let mut window = Vec::new();
    for row in mapped {
        window.push(row?);
    }
    window.reverse();

    let mut stmt = conn.prepare(
        "SELECT seq, role, content, partial FROM messages
         WHERE session_id = ?1 AND kind = ?2 AND seq > ?3
         ORDER BY seq LIMIT ?4",
    )?;
    let mapped = stmt.query_map(
        rusqlite::params![session_id, KIND_MESSAGE, anchor_seq, limit(WINDOW_RADIUS)],
        prose_row,
    )?;
    for row in mapped {
        window.push(row?);
    }
    Ok(window.into_iter().map(clip_message).collect())
}

/// A session's first and last [`BOOKEND_LEN`] user/assistant messages,
/// budget-clipped, the overlap removed from `last` — a short session's goal
/// and resolution are the same messages once, not twice.
fn bookends(
    conn: &Connection,
    session_id: &str,
) -> Result<(Vec<ProseMessage>, Vec<ProseMessage>), Error> {
    let spoken = |order: &str| {
        format!(
            "SELECT seq, role, content, partial FROM messages
             WHERE session_id = ?1 AND kind = ?2 AND role IN (?3, ?4)
             ORDER BY seq {order} LIMIT ?5"
        )
    };
    let params = rusqlite::params![
        session_id,
        KIND_MESSAGE,
        Role::User as i32,
        Role::Assistant as i32,
        limit(BOOKEND_LEN)
    ];

    let mut stmt = conn.prepare(&spoken("ASC"))?;
    let mapped = stmt.query_map(params, prose_row)?;
    let mut first = Vec::new();
    for row in mapped {
        first.push(row?);
    }

    let mut stmt = conn.prepare(&spoken("DESC"))?;
    let mapped = stmt.query_map(params, prose_row)?;
    let mut last = Vec::new();
    for row in mapped {
        last.push(row?);
    }
    last.reverse();
    last.retain(|m| !first.iter().any(|f| f.seq == m.seq));

    Ok((
        first.into_iter().map(clip_message).collect(),
        last.into_iter().map(clip_message).collect(),
    ))
}

/// Applies the per-message char budget, with an explicit marker.
fn clip_message(mut message: ProseMessage) -> ProseMessage {
    if message.content.chars().count() > MESSAGE_BUDGET_CHARS {
        let cut: String = message.content.chars().take(MESSAGE_BUDGET_CHARS).collect();
        message.content = format!("{cut} [truncated]");
    }
    message
}

/// Speaker name for a raw role integer.
fn role_name(role: i64) -> String {
    match i32::try_from(role).map(Role::try_from) {
        Ok(Ok(Role::System)) => "system".to_owned(),
        Ok(Ok(Role::User)) => "user".to_owned(),
        Ok(Ok(Role::Assistant)) => "assistant".to_owned(),
        _ => format!("role_{role}"),
    }
}

/// Microseconds since the epoch as RFC 3339 UTC, or `None` if out of range.
fn format_micros(micros: i64) -> Option<String> {
    DateTime::from_timestamp_micros(micros).map(|dt| dt.to_rfc3339_opts(SecondsFormat::Secs, true))
}

/// A row-count cap as the signed integer `LIMIT` binds.
fn limit(n: usize) -> i64 {
    i64::try_from(n).expect("row caps are small constants")
}

#[cfg(test)]
mod tests {
    use arc_proto::v1::{
        MessageAppended, Role, SessionCreated, ToolOutcome, ToolResultRecorded, session_event,
    };
    use tempfile::TempDir;

    use super::{Archive, Error, format_micros, sanitize_query};
    use crate::log::Log;
    use crate::projection::{self, Projection};
    use crate::testkit;

    fn created(id: &str, title: &str) -> session_event::Event {
        session_event::Event::SessionCreated(SessionCreated {
            session_id: id.to_owned(),
            title: title.to_owned(),
            provider: "test".to_owned(),
            model: "test-model".to_owned(),
        })
    }

    fn said(session: &str, role: Role, content: &str) -> session_event::Event {
        session_event::Event::MessageAppended(MessageAppended {
            session_id: session.to_owned(),
            role: role as i32,
            content: content.to_owned(),
            partial: false,
            turn_id: String::new(),
        })
    }

    fn tool_answered(session: &str, content: &str) -> session_event::Event {
        session_event::Event::ToolResultRecorded(ToolResultRecorded {
            session_id: session.to_owned(),
            turn_id: "t-01".to_owned(),
            call_id: "c-01".to_owned(),
            outcome: ToolOutcome::Ok as i32,
            content: content.to_owned(),
            truncated: false,
        })
    }

    /// An archive over a projection built by replaying a seeded log — the
    /// production shape, minus the daemon.
    fn archive_over(events: Vec<session_event::Event>) -> (TempDir, Archive) {
        let dir = TempDir::new().expect("temp dir");
        testkit::seed_log(&dir, events);
        let log = Log::open(dir.path()).expect("open log");
        let index = dir.path().join("index.db");
        let mut projection = Projection::open(&index).expect("open projection");
        projection::replay(log.reader().expect("reader"), &mut projection).expect("replay");
        drop(projection);
        let archive = Archive::open(&index).expect("open archive");
        (dir, archive)
    }

    // --- sanitizer ---

    /// Hermes's measured failure list is the mandatory fixture set; the
    /// expected outputs pin the policy, not just "does not crash".
    #[test]
    fn the_sanitizer_rewrites_the_fixture_set() {
        for (raw, cleaned) in [
            ("it's", "\"it's\""),
            ("gateway/run.py", "\"gateway/run.py\""),
            ("user@host", "\"user@host\""),
            ("a,b", "\"a,b\""),
            ("50%", "\"50%\""),
            ("TODO: fix", "\"TODO:\" fix"),
            ("my-app.config.ts", "\"my-app.config.ts\""),
            ("walking skeleton", "walking skeleton"),
            ("\"exact phrase\" extra", "\"exact phrase\" extra"),
            ("\"unterminated phrase", "\"unterminated phrase\""),
            ("AND gruvbox OR", "gruvbox"),
            ("%%% ---", ""),
            ("\"\" \"--\"", ""),
        ] {
            assert_eq!(sanitize_query(raw), cleaned, "raw: {raw}");
        }
    }

    #[test]
    fn the_sanitizer_caps_length() {
        let long = "word ".repeat(200);
        let cleaned = sanitize_query(&long);
        assert!(cleaned.chars().count() <= 256, "{}", cleaned.len());
    }

    /// Every fixture executes against real FTS5 and finds its seeded row —
    /// the errors hermes measured, closed end to end.
    #[test]
    fn every_fixture_query_executes_and_matches() {
        let (_dir, archive) = archive_over(vec![
            created("s-fix", ""),
            said("s-fix", Role::User, "it's flaky on resume"),
            said("s-fix", Role::User, "run gateway/run.py before the deploy"),
            said("s-fix", Role::User, "ssh user@host and check the fans"),
            said("s-fix", Role::User, "the options are a,b and nothing else"),
            said("s-fix", Role::User, "we are 50% done with phase two"),
            said("s-fix", Role::User, "TODO: fix the resume race"),
        ]);

        for query in [
            "it's",
            "gateway/run.py",
            "user@host",
            "a,b",
            "50%",
            "TODO: fix",
        ] {
            let reply = archive.search(query, false).expect(query);
            assert_eq!(reply.sessions.len(), 1, "query: {query}");
            assert_eq!(reply.sessions[0].session_id, "s-fix", "query: {query}");
        }
    }

    /// The sanitizer is not trusted alone: a query that still breaks FTS —
    /// doubled interior operators survive sanitization — comes back as
    /// [`Error::Query`] naming the problem, never a swallowed empty.
    #[test]
    fn an_unparseable_query_is_a_query_error_not_an_empty_result() {
        let (_dir, archive) = archive_over(vec![
            created("s-01", ""),
            said("s-01", Role::User, "plain prose"),
        ]);

        let err = archive
            .search("prose AND OR bar", false)
            .expect_err("FTS must reject doubled operators");
        assert!(matches!(err, Error::Query { .. }), "got: {err:?}");

        let err = archive
            .search("%%%", false)
            .expect_err("nothing searchable must be a query error");
        assert!(matches!(err, Error::Query { .. }), "got: {err:?}");
    }

    // --- search shape ---

    #[test]
    fn search_dedupes_to_one_slot_per_session_and_caps_at_five() {
        let mut events = vec![
            created("s-best", ""),
            said("s-best", Role::User, "zebra zebra zebra zebra"),
            said("s-best", Role::User, "zebra again in the same session"),
        ];
        for i in 0..6 {
            let id = format!("s-{i}");
            events.push(created(&id, ""));
            events.push(said(
                &id,
                Role::User,
                &format!("a zebra sighting number {i}"),
            ));
        }
        let (_dir, archive) = archive_over(events);

        let reply = archive.search("zebra", false).expect("search");

        assert_eq!(reply.sessions.len(), 5, "seven matching sessions cap at 5");
        let mut ids: Vec<&str> = reply
            .sessions
            .iter()
            .map(|s| s.session_id.as_str())
            .collect();
        assert_eq!(
            ids.first(),
            Some(&"s-best"),
            "the repeat-heavy session ranks first"
        );
        ids.sort_unstable();
        ids.dedup();
        assert_eq!(ids.len(), 5, "one slot per session");
    }

    #[test]
    fn the_top_hit_is_hydrated_and_the_rest_are_not() {
        let mut events = vec![created("s-long", "a long talk")];
        for i in 0..4 {
            events.push(said("s-long", Role::User, &format!("warmup question {i}")));
        }
        // Term-heavy so BM25 ranks this row above the aside, deterministically.
        events.push(said(
            "s-long",
            Role::Assistant,
            "the quasar answer: quasar, in short",
        ));
        for i in 0..4 {
            events.push(said("s-long", Role::User, &format!("wind-down remark {i}")));
        }
        events.push(created("s-other", ""));
        events.push(said("s-other", Role::User, "a quasar aside"));
        let (_dir, archive) = archive_over(events);

        let reply = archive.search("quasar", false).expect("search");

        let top = &reply.sessions[0];
        assert_eq!(top.session_id, "s-long");
        assert_eq!(top.title, "a long talk");
        // Seqs: created=0, warmups 1..=4, answer=5, wind-downs 6..=9.
        assert_eq!(top.anchor_seq, 5);
        let context_seqs: Vec<i64> = top.context.iter().map(|m| m.seq).collect();
        assert_eq!(
            context_seqs,
            [3, 4, 5, 6, 7],
            "two either side of the anchor"
        );
        assert_eq!(
            top.first.iter().map(|m| m.seq).collect::<Vec<_>>(),
            [1, 2],
            "opening bookend, nothing duplicated from the window"
        );
        assert_eq!(
            top.last.iter().map(|m| m.seq).collect::<Vec<_>>(),
            [8, 9],
            "closing bookend, nothing duplicated from the window"
        );
        assert_eq!(top.context[2].role, "assistant");

        let other = reply
            .sessions
            .iter()
            .find(|s| s.session_id == "s-other")
            .expect("second session listed");
        assert!(other.context.is_empty() && other.first.is_empty() && other.last.is_empty());
        assert!(other.title.is_empty());
    }

    #[test]
    fn hydrated_messages_are_clipped_to_the_budget_with_a_marker() {
        let long = "nebula ".repeat(100);
        let (_dir, archive) =
            archive_over(vec![created("s-01", ""), said("s-01", Role::User, &long)]);

        let reply = archive.search("nebula", false).expect("search");

        let message = &reply.sessions[0].context[0];
        assert!(
            message.content.ends_with(" [truncated]"),
            "{}",
            message.content
        );
        assert!(message.content.chars().count() < long.chars().count());
    }

    #[test]
    fn tool_results_are_excluded_by_default_and_found_when_lifted() {
        let (_dir, archive) = archive_over(vec![
            created("s-01", ""),
            said("s-01", Role::User, "please list the devices"),
            tool_answered("s-01", "vulkanpin gpu listing output"),
        ]);

        let reply = archive.search("vulkanpin", false).expect("search");
        assert!(
            reply.sessions.is_empty(),
            "the default filter excludes tool output"
        );

        let reply = archive.search("vulkanpin", true).expect("search");
        assert_eq!(reply.sessions.len(), 1);
        assert_eq!(
            reply.sessions[0].anchor_seq, 2,
            "anchored at the result row"
        );
        // The hydrated window is prose only, so the model gets the human
        // frame around the machine hit.
        assert_eq!(
            reply.sessions[0]
                .context
                .iter()
                .map(|m| m.seq)
                .collect::<Vec<_>>(),
            [1]
        );
    }

    // --- range read and ends ---

    #[test]
    fn read_range_returns_prose_rows_in_order_unclipped() {
        let (_dir, archive) = archive_over(vec![
            created("s-01", ""),
            said("s-01", Role::User, "one"),
            tool_answered("s-01", "machine noise"),
            said("s-01", Role::Assistant, "two"),
            said("s-01", Role::User, "three"),
        ]);

        let reply = archive
            .read_range("s-01", 1, 3)
            .expect("read")
            .expect("session exists");

        assert_eq!(reply.session_id, "s-01");
        assert!(!reply.clipped);
        let rows: Vec<(i64, &str, &str)> = reply
            .messages
            .iter()
            .map(|m| (m.seq, m.role.as_str(), m.content.as_str()))
            .collect();
        assert_eq!(
            rows,
            [(1, "user", "one"), (3, "assistant", "two")],
            "prose only, tool rows and out-of-range rows stay out"
        );
    }

    #[test]
    fn an_oversized_range_is_clipped_and_says_so() {
        let mut events = vec![created("s-01", "")];
        for i in 0..60 {
            events.push(said("s-01", Role::User, &format!("message {i}")));
        }
        let (_dir, archive) = archive_over(events);

        let reply = archive
            .read_range("s-01", 0, 1000)
            .expect("read")
            .expect("session exists");

        assert_eq!(reply.messages.len(), 50);
        assert!(reply.clipped);
    }

    #[test]
    fn ends_returns_deduped_bookends_of_spoken_messages() {
        let (_dir, archive) = archive_over(vec![
            created("s-01", ""),
            said("s-01", Role::System, "a system note, not a bookend"),
            said("s-01", Role::User, "the goal"),
            said("s-01", Role::Assistant, "working on it"),
            said("s-01", Role::Assistant, "the resolution"),
        ]);

        let reply = archive.ends("s-01").expect("ends").expect("session exists");

        assert_eq!(
            reply.first.iter().map(|m| m.seq).collect::<Vec<_>>(),
            [2, 3],
            "system prose stays out of bookends"
        );
        assert_eq!(
            reply.last.iter().map(|m| m.seq).collect::<Vec<_>>(),
            [4],
            "the overlap with first is dropped, not repeated"
        );
    }

    #[test]
    fn an_unknown_session_reads_as_none() {
        let (_dir, archive) = archive_over(vec![created("s-01", "")]);

        assert!(archive.read_range("s-none", 0, 10).expect("read").is_none());
        assert!(archive.ends("s-none").expect("ends").is_none());
    }

    #[test]
    fn timestamps_format_as_rfc3339_utc() {
        assert_eq!(
            format_micros(1_700_000_000_123_456).as_deref(),
            Some("2023-11-14T22:13:20Z")
        );
    }
}
