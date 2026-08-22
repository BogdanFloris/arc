use std::fmt::Write as _;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard, PoisonError};

use arc_proto::v1::{Role, memory_record};
use chrono::{DateTime, SecondsFormat};
use rusqlite::{Connection, OpenFlags, OptionalExtension};
use serde::Serialize;

use crate::memory::kind_name;
use crate::projection::{KIND_MESSAGE, bad_json_column, links_from_json, provenance_from_json};

const MAX_QUERY_CHARS: usize = 256;

const OVERFETCH: usize = 50; // hits collapse per session, so fetch extra

const MAX_SESSIONS: usize = 5;

const MESSAGE_BUDGET_CHARS: usize = 300;

const WINDOW_RADIUS: usize = 2;

const BOOKEND_LEN: usize = 2;

const MAX_RANGE_ROWS: usize = 50;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("archive index {path}: {source}")]
    Open {
        path: PathBuf,
        #[source]
        source: rusqlite::Error,
    },

    #[error("the query could not be parsed: {message}")]
    Query { message: String },

    #[error("archive index: {0}")]
    Sqlite(#[from] rusqlite::Error),
}

#[derive(Debug, Serialize)]
pub struct SearchReply {
    pub sessions: Vec<SessionHit>,
}

#[derive(Debug, Serialize)]
pub struct SessionHit {
    pub session_id: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub title: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub started_at: Option<String>,
    pub snippet: String,
    pub anchor_seq: i64,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub context: Vec<ProseMessage>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub first: Vec<ProseMessage>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub last: Vec<ProseMessage>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ProseMessage {
    pub seq: i64,
    pub role: String,
    pub content: String,
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub partial: bool,
}

#[derive(Debug, Serialize)]
pub struct ReadReply {
    pub session_id: String,
    pub messages: Vec<ProseMessage>,
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub clipped: bool,
}

#[derive(Debug, Serialize)]
pub struct EndsReply {
    pub session_id: String,
    pub first: Vec<ProseMessage>,
    pub last: Vec<ProseMessage>,
}

#[derive(Debug, Serialize)]
pub struct MemoryHit {
    pub id: String,
    pub namespace: String,
    pub kind: String,
    pub title: String,
    pub summary: String,
}

#[derive(Debug, Serialize)]
pub struct MemoryRecordReply {
    pub id: String,
    pub namespace: String,
    pub kind: String,
    pub title: String,
    pub summary: String,
    pub body: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub links: Vec<String>,
    pub provenance: Vec<ProvenanceLine>,
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub superseded_by: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct ProvenanceLine {
    pub session_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ts: Option<String>,
}

pub struct Archive {
    conn: Mutex<Connection>,
}

impl Archive {
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
        exclude_session: Option<&str>,
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
               AND (?5 IS NULL OR m.session_id != ?5)
             ORDER BY bm25(messages_fts)
             LIMIT ?4",
        )?;
        let mapped = stmt
            .query_map(
                rusqlite::params![
                    query,
                    include_tool_results,
                    KIND_MESSAGE,
                    limit(OVERFETCH),
                    exclude_session
                ],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, String>(2)?,
                    ))
                },
            )
            .map_err(into_query_error)?;
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

    #[tracing::instrument(name = "memory.read", skip_all, fields(id = %id))]
    pub fn memory_record(&self, id: &str) -> Result<Option<MemoryRecordReply>, Error> {
        let row = self
            .lock()
            .query_row(
                "SELECT namespace, kind, title, summary, body, links, provenance,
                        status, superseded_by
                 FROM memory_records WHERE id = ?1",
                [id],
                |row| {
                    Ok(MemoryRecordReply {
                        id: id.to_owned(),
                        namespace: row.get(0)?,
                        kind: kind_name(row.get(1)?),
                        title: row.get(2)?,
                        summary: row.get(3)?,
                        body: row.get(4)?,
                        links: links_from_json(&row.get::<_, String>(5)?)
                            .map_err(|e| bad_json_column(5, &e))?,
                        provenance: provenance_lines(row.get::<_, Option<String>>(6)?.as_deref())
                            .map_err(|e| bad_json_column(6, &e))?,
                        status: status_name(row.get(7)?),
                        superseded_by: row.get(8)?,
                    })
                },
            )
            .optional()?;
        Ok(row)
    }

    #[tracing::instrument(
        name = "memory.search",
        skip_all,
        fields(query = %query, hits = tracing::field::Empty)
    )]
    pub fn memory_search(
        &self,
        query: &str,
        namespace: Option<&str>,
    ) -> Result<Vec<MemoryHit>, Error> {
        let words: Vec<String> = query.split_whitespace().map(like_pattern).collect();
        if words.is_empty() {
            return Err(Error::Query {
                message: "no searchable words in the query".to_owned(),
            });
        }
        let active = memory_record::Status::Active as i32;
        let mut sql = format!(
            "SELECT id, namespace, kind, title, summary FROM memory_records
             WHERE status = {active} AND (?1 IS NULL OR namespace = ?1)"
        );
        let mut params: Vec<Option<String>> = vec![namespace.map(str::to_owned)];
        for word in words {
            params.push(Some(word));
            let n = params.len();
            let _ = write!(
                sql,
                " AND (title LIKE ?{n} ESCAPE '\\'
                    OR summary LIKE ?{n} ESCAPE '\\'
                    OR body LIKE ?{n} ESCAPE '\\')"
            );
        }
        sql.push_str(" ORDER BY namespace, kind, title, id");

        let conn = self.lock();
        let mut stmt = conn.prepare(&sql)?;
        let mapped = stmt.query_map(rusqlite::params_from_iter(params), |row| {
            Ok(MemoryHit {
                id: row.get(0)?,
                namespace: row.get(1)?,
                kind: kind_name(row.get(2)?),
                title: row.get(3)?,
                summary: row.get(4)?,
            })
        })?;
        let mut hits = Vec::new();
        for hit in mapped {
            hits.push(hit?);
        }
        tracing::Span::current().record("hits", hits.len());
        Ok(hits)
    }

    fn lock(&self) -> MutexGuard<'_, Connection> {
        self.conn.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

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

fn is_operator(term: &str) -> bool {
    matches!(term, "AND" | "OR" | "NOT")
}

fn into_query_error(source: rusqlite::Error) -> Error {
    let text = source.to_string();
    if text.contains("fts5") {
        Error::Query { message: text }
    } else {
        Error::Sqlite(source)
    }
}

fn session_exists(conn: &Connection, id: &str) -> Result<bool, Error> {
    Ok(conn
        .query_row("SELECT 1 FROM sessions WHERE id = ?1", [id], |_| Ok(()))
        .optional()?
        .is_some())
}

fn prose_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<ProseMessage> {
    Ok(ProseMessage {
        seq: row.get(0)?,
        role: role_name(row.get(1)?),
        content: row.get(2)?,
        partial: row.get(3)?,
    })
}

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
    // walked backwards from the anchor; flip it to reading order
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

fn clip_message(mut message: ProseMessage) -> ProseMessage {
    if message.content.chars().count() > MESSAGE_BUDGET_CHARS {
        let cut: String = message.content.chars().take(MESSAGE_BUDGET_CHARS).collect();
        message.content = format!("{cut} [truncated]");
    }
    message
}

fn like_pattern(word: &str) -> String {
    let mut pattern = String::with_capacity(word.len() + 2);
    pattern.push('%');
    for c in word.chars() {
        if matches!(c, '%' | '_' | '\\') {
            pattern.push('\\');
        }
        pattern.push(c);
    }
    pattern.push('%');
    pattern
}

fn status_name(status: i32) -> String {
    use arc_proto::v1::memory_record::Status;
    match Status::try_from(status) {
        Ok(Status::Active) => "active".to_owned(),
        Ok(Status::Superseded) => "superseded".to_owned(),
        Ok(Status::Unspecified) | Err(_) => format!("status_{status}"),
    }
}

fn provenance_lines(json: Option<&str>) -> Result<Vec<ProvenanceLine>, serde_json::Error> {
    Ok(provenance_from_json(json)?
        .map(|provenance| {
            provenance
                .entries
                .into_iter()
                .map(|entry| ProvenanceLine {
                    session_id: entry.session_id,
                    ts: entry.ts.and_then(|ts| {
                        u32::try_from(ts.nanos).ok().and_then(|nanos| {
                            DateTime::from_timestamp(ts.seconds, nanos)
                                .map(|dt| dt.to_rfc3339_opts(SecondsFormat::Secs, true))
                        })
                    }),
                })
                .collect()
        })
        .unwrap_or_default())
}

fn role_name(role: i64) -> String {
    match i32::try_from(role).map(Role::try_from) {
        Ok(Ok(Role::System)) => "system".to_owned(),
        Ok(Ok(Role::User)) => "user".to_owned(),
        Ok(Ok(Role::Assistant)) => "assistant".to_owned(),
        _ => format!("role_{role}"),
    }
}

fn format_micros(micros: i64) -> Option<String> {
    DateTime::from_timestamp_micros(micros).map(|dt| dt.to_rfc3339_opts(SecondsFormat::Secs, true))
}

fn limit(n: usize) -> i64 {
    i64::try_from(n).expect("row caps are small constants")
}

#[cfg(test)]
mod tests {
    use arc_proto::v1::{
        MemoryRecord, MemoryRecordCreated, MemoryRecordSuperseded, MessageAppended, Provenance,
        ProvenanceEntry, Role, SessionCreated, ToolOutcome, ToolResultRecorded, memory_event,
        memory_record, session_event,
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
            let reply = archive.search(query, false, None).expect(query);
            assert_eq!(reply.sessions.len(), 1, "query: {query}");
            assert_eq!(reply.sessions[0].session_id, "s-fix", "query: {query}");
        }
    }

    #[test]
    fn an_unparseable_query_is_a_query_error_not_an_empty_result() {
        let (_dir, archive) = archive_over(vec![
            created("s-01", ""),
            said("s-01", Role::User, "plain prose"),
        ]);

        let err = archive
            .search("prose AND OR bar", false, None)
            .expect_err("FTS must reject doubled operators");
        assert!(matches!(err, Error::Query { .. }), "got: {err:?}");

        let err = archive
            .search("%%%", false, None)
            .expect_err("nothing searchable must be a query error");
        assert!(matches!(err, Error::Query { .. }), "got: {err:?}");
    }

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

        let reply = archive.search("zebra", false, None).expect("search");

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

        let reply = archive.search("quasar", false, None).expect("search");

        let top = &reply.sessions[0];
        assert_eq!(top.session_id, "s-long");
        assert_eq!(top.title, "a long talk");
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

        let reply = archive.search("nebula", false, None).expect("search");

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

        let reply = archive.search("vulkanpin", false, None).expect("search");
        assert!(
            reply.sessions.is_empty(),
            "the default filter excludes tool output"
        );

        let reply = archive.search("vulkanpin", true, None).expect("search");
        assert_eq!(reply.sessions.len(), 1);
        assert_eq!(
            reply.sessions[0].anchor_seq, 2,
            "anchored at the result row"
        );
        assert_eq!(
            reply.sessions[0]
                .context
                .iter()
                .map(|m| m.seq)
                .collect::<Vec<_>>(),
            [1]
        );
    }

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

    fn record(id: &str, namespace: &str, title: &str, summary: &str, body: &str) -> MemoryRecord {
        MemoryRecord {
            id: id.to_owned(),
            kind: memory_record::Kind::Fact as i32,
            namespace: namespace.to_owned(),
            title: title.to_owned(),
            summary: summary.to_owned(),
            body: body.to_owned(),
            links: Vec::new(),
            provenance: None,
            status: memory_record::Status::Active as i32,
        }
    }

    fn written(record: MemoryRecord) -> memory_event::Event {
        memory_event::Event::RecordCreated(MemoryRecordCreated {
            record: Some(record),
        })
    }

    fn memory_archive_over(events: Vec<memory_event::Event>) -> (TempDir, Archive) {
        let dir = TempDir::new().expect("temp dir");
        testkit::seed_memory_log(&dir, events);
        let archive = testkit::archive_at(&dir);
        (dir, archive)
    }

    #[test]
    fn memory_search_matches_words_case_insensitively_across_fields() {
        let (_dir, archive) = memory_archive_over(vec![
            written(record(
                "mr-1",
                "global",
                "Gruvbox",
                "the palette everywhere",
                "User prefers gruvbox via the terminal palette.",
            )),
            written(record(
                "mr-2",
                "global",
                "Erebor",
                "the NixOS box",
                "RTX 5070, pin Vulkan1.",
            )),
        ]);

        for query in ["PALETTE", "gruvbox terminal", "Terminal Palette"] {
            let hits = archive.memory_search(query, None).expect(query);
            assert_eq!(hits.len(), 1, "query: {query}");
            assert_eq!(hits[0].id, "mr-1", "query: {query}");
            assert_eq!(hits[0].kind, "fact");
            assert_eq!(hits[0].summary, "the palette everywhere");
        }
        assert!(
            archive
                .memory_search("gruvbox vulkan1", None)
                .expect("search")
                .is_empty()
        );
    }

    #[test]
    fn memory_search_filters_by_namespace() {
        let (_dir, archive) = memory_archive_over(vec![
            written(record("mr-g", "global", "Palette", "gruvbox", "gruvbox")),
            written(record(
                "mr-p",
                "arc",
                "Palette",
                "gruvbox in arc",
                "gruvbox",
            )),
        ]);

        let hits = archive
            .memory_search("gruvbox", Some("arc"))
            .expect("search");
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].id, "mr-p");

        let hits = archive.memory_search("gruvbox", None).expect("search");
        assert_eq!(hits.len(), 2, "no namespace means all namespaces");
    }

    #[test]
    fn memory_search_sees_only_active_records() {
        let (_dir, archive) = memory_archive_over(vec![
            written(record(
                "mr-old",
                "global",
                "Home",
                "lives at X",
                "lives at X",
            )),
            memory_event::Event::RecordSuperseded(MemoryRecordSuperseded {
                superseded_id: "mr-old".to_owned(),
                record: Some(record(
                    "mr-new",
                    "global",
                    "Home",
                    "lives at Y",
                    "lives at Y",
                )),
            }),
        ]);

        let hits = archive.memory_search("lives", None).expect("search");
        assert_eq!(hits.len(), 1, "the retired record stays out");
        assert_eq!(hits[0].id, "mr-new");
    }

    #[test]
    fn memory_search_treats_like_metacharacters_literally() {
        let (_dir, archive) = memory_archive_over(vec![
            written(record(
                "mr-pct", "global", "Progress", "50% done", "50% done",
            )),
            written(record(
                "mr-num",
                "global",
                "Progress",
                "505 items",
                "505 items",
            )),
        ]);

        let hits = archive.memory_search("50%", None).expect("search");
        assert_eq!(hits.len(), 1, "% must not act as a wildcard");
        assert_eq!(hits[0].id, "mr-pct");
    }

    #[test]
    fn an_empty_memory_query_is_a_query_error() {
        let (_dir, archive) = memory_archive_over(vec![]);

        let err = archive
            .memory_search("   ", None)
            .expect_err("nothing searchable must be a query error");
        assert!(matches!(err, Error::Query { .. }), "got: {err:?}");
    }

    #[test]
    fn memory_record_returns_the_whole_record_with_provenance() {
        let mut full = record(
            "mr-full",
            "global",
            "Gruvbox",
            "the palette",
            "User prefers gruvbox.",
        );
        full.links = vec!["mr-other".to_owned()];
        full.provenance = Some(Provenance {
            entries: vec![ProvenanceEntry {
                session_id: "s-taught".to_owned(),
                ts: Some(prost_types::Timestamp {
                    seconds: 1_700_000_000,
                    nanos: 0,
                }),
            }],
        });
        let (_dir, archive) = memory_archive_over(vec![written(full)]);

        let reply = archive
            .memory_record("mr-full")
            .expect("read")
            .expect("record exists");

        assert_eq!(reply.kind, "fact");
        assert_eq!(reply.status, "active");
        assert_eq!(reply.body, "User prefers gruvbox.");
        assert_eq!(reply.links, ["mr-other"]);
        assert_eq!(reply.superseded_by, None);
        assert_eq!(reply.provenance.len(), 1);
        assert_eq!(reply.provenance[0].session_id, "s-taught");
        assert_eq!(
            reply.provenance[0].ts.as_deref(),
            Some("2023-11-14T22:13:20Z")
        );
    }

    #[test]
    fn a_superseded_record_reads_back_pointing_at_its_replacement() {
        let (_dir, archive) = memory_archive_over(vec![
            written(record(
                "mr-old",
                "global",
                "Home",
                "lives at X",
                "lives at X",
            )),
            memory_event::Event::RecordSuperseded(MemoryRecordSuperseded {
                superseded_id: "mr-old".to_owned(),
                record: Some(record(
                    "mr-new",
                    "global",
                    "Home",
                    "lives at Y",
                    "lives at Y",
                )),
            }),
        ]);

        let reply = archive
            .memory_record("mr-old")
            .expect("read")
            .expect("still readable");
        assert_eq!(reply.status, "superseded");
        assert_eq!(reply.superseded_by.as_deref(), Some("mr-new"));

        assert!(
            archive.memory_record("mr-none").expect("read").is_none(),
            "an unknown id reads as None"
        );
    }
}
