use std::fmt::Write as _;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard, PoisonError};

use arc_proto::v1::{Role, Source, memory_record};
use chrono::{DateTime, SecondsFormat};
use rusqlite::{Connection, OpenFlags, OptionalExtension};
use serde::{Deserialize, Serialize};

use crate::memory::kind_name;
use crate::projection::{
    KIND_MESSAGE, KIND_SERVER_CALL, bad_json_column, links_from_json, provenance_from_json,
};

const MAX_QUERY_CHARS: usize = 256;
const OVERFETCH: usize = 50; // hits collapse per session, so fetch extra
const MAX_SESSIONS: usize = 5;
const MESSAGE_BUDGET_CHARS: usize = 300;
const WINDOW_RADIUS: usize = 2;
const BOOKEND_LEN: usize = 2;
const MAX_RANGE_ROWS: usize = 20;
const MAX_READ_BYTES: usize = 8 * 1024;

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

    #[error("invalid session read: {0}")]
    Read(String),
}

#[derive(Debug, Serialize)]
pub(crate) struct SearchReply {
    pub sessions: Vec<SessionHit>,
}

#[derive(Debug, Clone, Copy, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum SearchOrder {
    #[default]
    Relevance,
    Newest,
}

impl SearchOrder {
    fn as_str(self) -> &'static str {
        match self {
            Self::Relevance => "relevance",
            Self::Newest => "newest",
        }
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct SearchOptions {
    pub after: Option<i64>,
    pub before: Option<i64>,
    pub sort: SearchOrder,
}

#[derive(Debug, Serialize)]
pub(crate) struct SessionHit {
    pub session_id: String,
    pub origin: SessionOrigin,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub title: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub started_at: Option<String>,
    pub snippet: String,
    pub role: String,
    pub source: &'static str,
    pub ts: Option<String>,
    pub anchor_seq: i64,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub context: Vec<ProseMessage>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub first: Vec<ProseMessage>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub last: Vec<ProseMessage>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct ProseMessage {
    pub seq: i64,
    pub role: String,
    pub source: &'static str,
    pub content: String,
    pub ts: Option<String>,
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub partial: bool,
}

#[derive(Debug, Serialize)]
pub(crate) struct ReadReply {
    pub session_id: String,
    pub origin: SessionOrigin,
    pub messages: Vec<ProseMessage>,
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub clipped: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next: Option<ReadCursor>,
}

#[derive(Debug, Serialize)]
pub(crate) struct ReadCursor {
    pub start_seq: i64,
    pub start_offset: usize,
}

#[derive(Debug, Serialize)]
pub(crate) struct EndsReply {
    pub session_id: String,
    pub origin: SessionOrigin,
    pub first: Vec<ProseMessage>,
    pub last: Vec<ProseMessage>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct SessionOrigin {
    pub kind: &'static str,
    pub dispatched_by: Option<String>,
}

impl SessionOrigin {
    fn new(source: i32, dispatched_by: Option<String>) -> Self {
        let kind = if dispatched_by.is_some() || source == Source::Model as i32 {
            "job"
        } else if source == Source::User as i32 {
            "conversation"
        } else {
            "unknown"
        };
        Self {
            kind,
            dispatched_by,
        }
    }
}

#[derive(Debug, Serialize)]
pub(crate) struct MemoryHit {
    pub id: String,
    pub namespace: String,
    pub kind: String,
    pub title: String,
    pub summary: String,
    // Some(true) only under as_of, when the hit is a since-superseded record;
    // absent otherwise so today's reply JSON is unchanged
    #[serde(skip_serializing_if = "Option::is_none")]
    pub superseded: Option<bool>,
}

#[derive(Debug, Serialize)]
pub(crate) struct MemoryRecordReply {
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
pub(crate) struct ProvenanceLine {
    pub session_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ts: Option<String>,
}

pub struct Archive {
    conn: Mutex<Connection>,
}

impl Archive {
    pub fn open(path: &Path) -> Result<Self, Error> {
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
            sort = tracing::field::Empty,
            after = tracing::field::Empty,
            before = tracing::field::Empty,
        )
    )]
    pub(crate) fn search(
        &self,
        raw_query: &str,
        include_tool_results: bool,
        exclude_session: Option<&str>,
        options: SearchOptions,
    ) -> Result<SearchReply, Error> {
        let query = sanitize_query(raw_query);
        let span = tracing::Span::current();
        span.record("query", query.as_str());
        span.record("sort", options.sort.as_str());
        span.record("after", options.after);
        span.record("before", options.before);
        if query.is_empty() {
            return Err(Error::Query {
                message: "no searchable words in the query".to_owned(),
            });
        }

        let conn = self.lock();
        let mut stmt = conn.prepare(&search_sql(options.sort))?;
        let mapped = stmt
            .query_map(
                rusqlite::params![
                    query,
                    include_tool_results,
                    KIND_MESSAGE,
                    limit(OVERFETCH),
                    exclude_session,
                    options.after,
                    options.before
                ],
                hit_row,
            )
            .map_err(into_query_error)?;
        let mut hits = Vec::new();
        for hit in mapped {
            hits.push(hit.map_err(into_query_error)?);
        }
        span.record("raw_hits", hits.len());

        let mut sessions: Vec<SessionHit> = Vec::new();
        for hit in hits {
            if sessions.iter().any(|s| s.session_id == hit.session_id) {
                continue;
            }
            let (title, started_at, source, dispatched_by) = conn
                .query_row(
                    "SELECT coalesce(title, ''), started_at, source, dispatched_by
                     FROM sessions WHERE id = ?1",
                    [&hit.session_id],
                    |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, Option<i64>>(1)?,
                            row.get::<_, i32>(2)?,
                            row.get::<_, Option<String>>(3)?,
                        ))
                    },
                )
                .optional()?
                .unwrap_or_default();
            sessions.push(SessionHit {
                session_id: hit.session_id,
                origin: SessionOrigin::new(source, dispatched_by),
                title,
                started_at: started_at.and_then(format_micros),
                snippet: hit.snippet,
                role: hit.role,
                source: hit.source,
                ts: hit.ts,
                anchor_seq: hit.seq,
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
        fields(
            session_id = %session_id,
            shape = "range",
            rows = tracing::field::Empty,
            bytes = tracing::field::Empty,
            clipped = tracing::field::Empty,
        )
    )]
    pub(crate) fn read_range(
        &self,
        session_id: &str,
        start_seq: i64,
        end_seq: i64,
        start_offset: usize,
    ) -> Result<Option<ReadReply>, Error> {
        let conn = self.lock();
        let Some(origin) = session_origin(&conn, session_id)? else {
            return Ok(None);
        };
        let mut stmt = conn.prepare(
            "SELECT seq, role, content, partial, ts, source FROM messages
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
        let mut rows = Vec::new();
        for row in mapped {
            rows.push(row?);
        }
        if start_offset != 0
            && !rows.first().is_some_and(|row| {
                row.seq == start_seq
                    && start_offset < row.content.len()
                    && row.content.is_char_boundary(start_offset)
            })
        {
            return Err(Error::Read(
                "start_offset must point inside the message at start_seq, on a UTF-8 boundary. \
                 Use the next cursor from the previous page."
                    .to_owned(),
            ));
        }
        let mut reply = ReadReply {
            session_id: session_id.to_owned(),
            origin,
            messages: Vec::new(),
            clipped: false,
            next: None,
        };
        let mut rows = rows.into_iter().peekable();
        while let Some(mut message) = rows.next() {
            let offset = if message.seq == start_seq {
                start_offset
            } else {
                0
            };
            let content = message.content.split_off(offset);
            message.content.clear();
            let seq = message.seq;
            reply.messages.push(message);
            reply.next = rows.peek().map(|row| ReadCursor {
                start_seq: row.seq,
                start_offset: 0,
            });
            reply.clipped = reply.next.is_some();
            if content.len() <= MAX_READ_BYTES {
                reply
                    .messages
                    .last_mut()
                    .expect("current message")
                    .content
                    .clone_from(&content);
                if read_reply_bytes(&reply) <= MAX_READ_BYTES {
                    if reply.messages.len() == MAX_RANGE_ROWS {
                        break;
                    }
                    continue;
                }
            }

            let mut low = 0;
            let mut high = content.len().saturating_sub(1).min(MAX_READ_BYTES);
            while !content.is_char_boundary(high) {
                high -= 1;
            }
            while low < high {
                let mut cut = low + (high - low).div_ceil(2);
                while !content.is_char_boundary(cut) {
                    cut += 1;
                }
                set_read_fragment(&mut reply, &content, seq, offset, cut);
                if read_reply_bytes(&reply) <= MAX_READ_BYTES {
                    low = cut;
                } else {
                    high = cut - 1;
                    while !content.is_char_boundary(high) {
                        high -= 1;
                    }
                }
            }
            set_read_fragment(&mut reply, &content, seq, offset, low);
            if low == 0 {
                reply.messages.pop();
                if reply.messages.is_empty() {
                    return Err(Error::Read(
                        "page metadata exceeds the read budget".to_owned(),
                    ));
                }
            }
            break;
        }
        let span = tracing::Span::current();
        span.record("rows", reply.messages.len());
        span.record("bytes", read_reply_bytes(&reply));
        span.record("clipped", reply.clipped);
        Ok(Some(reply))
    }

    #[tracing::instrument(
        name = "archive.read",
        skip_all,
        fields(session_id = %session_id, shape = "ends")
    )]
    pub(crate) fn ends(&self, session_id: &str) -> Result<Option<EndsReply>, Error> {
        let conn = self.lock();
        let Some(origin) = session_origin(&conn, session_id)? else {
            return Ok(None);
        };
        let (first, last) = bookends(&conn, session_id)?;
        Ok(Some(EndsReply {
            session_id: session_id.to_owned(),
            origin,
            first,
            last,
        }))
    }

    #[tracing::instrument(name = "memory.read", skip_all, fields(id = %id))]
    pub(crate) fn memory_record(&self, id: &str) -> Result<Option<MemoryRecordReply>, Error> {
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
    pub(crate) fn memory_search(
        &self,
        query: &str,
        namespace: Option<&str>,
        as_of: Option<i64>,
    ) -> Result<Vec<MemoryHit>, Error> {
        let sanitized = sanitize_query(query);
        if sanitized.is_empty() {
            return Err(Error::Query {
                message: "no searchable words in the query".to_owned(),
            });
        }
        let active = memory_record::Status::Active as i32;
        let conn = self.lock();

        let hits = match memory_search_fts(&conn, &sanitized, namespace, as_of, active) {
            Ok(hits) if !hits.is_empty() => hits,
            Ok(_) => memory_search_like(&conn, query, namespace, as_of, active)?,
            Err(error) => {
                tracing::warn!(error = %error, "memory_fts match failed; falling back to LIKE");
                memory_search_like(&conn, query, namespace, as_of, active)?
            }
        };

        tracing::Span::current().record("hits", hits.len());
        Ok(hits)
    }

    fn lock(&self) -> MutexGuard<'_, Connection> {
        self.conn.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

// shared by the FTS and LIKE paths: an as_of'd row is held if it was
// created by then and either still active or superseded after that instant
fn memory_time_filter(ns_idx: usize, as_of_idx: usize, active: i32) -> String {
    format!(
        "(?{ns_idx} IS NULL OR r.namespace = ?{ns_idx})
         AND (
           (?{as_of_idx} IS NULL AND r.status = {active})
           OR (?{as_of_idx} IS NOT NULL
               AND r.created_at IS NOT NULL AND r.created_at <= ?{as_of_idx}
               AND (r.status = {active}
                    OR (r.superseded_at IS NOT NULL AND r.superseded_at > ?{as_of_idx})))
         )"
    )
}

fn memory_hit_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<MemoryHit> {
    let status: i32 = row.get(5)?;
    Ok(MemoryHit {
        id: row.get(0)?,
        namespace: row.get(1)?,
        kind: kind_name(row.get(2)?),
        title: row.get(3)?,
        summary: row.get(4)?,
        superseded: (status == memory_record::Status::Superseded as i32).then_some(true),
    })
}

fn memory_search_fts(
    conn: &Connection,
    query: &str,
    namespace: Option<&str>,
    as_of: Option<i64>,
    active: i32,
) -> Result<Vec<MemoryHit>, Error> {
    let sql = format!(
        "SELECT r.id, r.namespace, r.kind, r.title, r.summary, r.status
         FROM memory_fts f JOIN memory_records r ON r.id = f.record_id
         WHERE memory_fts MATCH ?1
           AND {}
         ORDER BY bm25(memory_fts, 0.0, 4.0, 2.0, 1.0)",
        memory_time_filter(2, 3, active)
    );
    let mut stmt = conn.prepare(&sql)?;
    let mapped = stmt.query_map(rusqlite::params![query, namespace, as_of], memory_hit_row)?;
    Ok(mapped.collect::<Result<_, _>>()?)
}

fn memory_search_like(
    conn: &Connection,
    query: &str,
    namespace: Option<&str>,
    as_of: Option<i64>,
    active: i32,
) -> Result<Vec<MemoryHit>, Error> {
    let words: Vec<String> = query.split_whitespace().map(like_pattern).collect();
    let mut sql = format!(
        "SELECT r.id, r.namespace, r.kind, r.title, r.summary, r.status
         FROM memory_records r
         WHERE {}",
        memory_time_filter(1, 2, active)
    );
    let mut params: Vec<rusqlite::types::Value> = vec![
        namespace.map_or(rusqlite::types::Value::Null, |ns| {
            rusqlite::types::Value::Text(ns.to_owned())
        }),
        as_of.map_or(
            rusqlite::types::Value::Null,
            rusqlite::types::Value::Integer,
        ),
    ];
    for word in words {
        params.push(rusqlite::types::Value::Text(word));
        let n = params.len();
        let _ = write!(
            sql,
            " AND (r.title LIKE ?{n} ESCAPE '\\'
                OR r.summary LIKE ?{n} ESCAPE '\\'
                OR r.body LIKE ?{n} ESCAPE '\\')"
        );
    }
    sql.push_str(" ORDER BY r.namespace, r.kind, r.title, r.id");

    let mut stmt = conn.prepare(&sql)?;
    let mapped = stmt.query_map(rusqlite::params_from_iter(params), memory_hit_row)?;
    Ok(mapped.collect::<Result<_, _>>()?)
}

pub(crate) fn sanitize_query(raw: &str) -> String {
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

fn session_origin(conn: &Connection, id: &str) -> Result<Option<SessionOrigin>, Error> {
    Ok(conn
        .query_row(
            "SELECT source, dispatched_by FROM sessions WHERE id = ?1",
            [id],
            |row| Ok(SessionOrigin::new(row.get(0)?, row.get(1)?)),
        )
        .optional()?)
}

fn event_source(source: Option<i32>) -> &'static str {
    match source.and_then(|source| Source::try_from(source).ok()) {
        Some(Source::User) => "user",
        Some(Source::Model) => "model",
        Some(Source::System) => "system",
        _ => "unknown",
    }
}

// a NULL ts satisfies neither >= nor <, so bounds drop undated hits by themselves
fn search_sql(sort: SearchOrder) -> String {
    let order = match sort {
        SearchOrder::Relevance => "bm25(messages_fts), m.seq DESC",
        SearchOrder::Newest => "m.ts DESC, m.seq DESC",
    };
    format!(
        "SELECT m.session_id, m.seq, m.kind, m.role, m.ts,
                snippet(messages_fts, 0, '', '', '…', 12), m.source
         FROM messages_fts JOIN messages m ON m.seq = messages_fts.rowid
         WHERE messages_fts MATCH ?1 AND (?2 OR m.kind = ?3)
           AND (?5 IS NULL OR m.session_id != ?5)
           AND (?6 IS NULL OR m.ts >= ?6)
           AND (?7 IS NULL OR m.ts < ?7)
         ORDER BY {order}
         LIMIT ?4"
    )
}

struct HitRow {
    session_id: String,
    seq: i64,
    role: String,
    source: &'static str,
    ts: Option<String>,
    snippet: String,
}

fn hit_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<HitRow> {
    let kind: i64 = row.get(2)?;
    Ok(HitRow {
        session_id: row.get(0)?,
        seq: row.get(1)?,
        role: hit_role(row.get(3)?, kind),
        source: event_source(row.get(6)?),
        ts: message_ts(row.get(4)?),
        snippet: row.get(5)?,
    })
}

fn hit_role(role: Option<i64>, kind: i64) -> String {
    match role {
        Some(role) => role_name(role),
        // Only results and server calls lack indexed roles.
        None if kind == KIND_SERVER_CALL => "server_call".to_owned(),
        None => "tool_result".to_owned(),
    }
}

fn prose_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<ProseMessage> {
    Ok(ProseMessage {
        seq: row.get(0)?,
        role: role_name(row.get(1)?),
        source: event_source(row.get(5)?),
        content: row.get(2)?,
        partial: row.get(3)?,
        ts: message_ts(row.get(4)?),
    })
}

fn prose_window(
    conn: &Connection,
    session_id: &str,
    anchor_seq: i64,
) -> Result<Vec<ProseMessage>, Error> {
    let mut stmt = conn.prepare(
        "SELECT seq, role, content, partial, ts, source FROM messages
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
        "SELECT seq, role, content, partial, ts, source FROM messages
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
            "SELECT seq, role, content, partial, ts, source FROM messages
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

fn read_reply_bytes(reply: &ReadReply) -> usize {
    serde_json::to_vec(reply)
        .expect("archive replies serialize")
        .len()
}

fn set_read_fragment(reply: &mut ReadReply, content: &str, seq: i64, offset: usize, cut: usize) {
    content[..cut].clone_into(&mut reply.messages.last_mut().expect("current message").content);
    reply.clipped = true;
    reply.next = Some(ReadCursor {
        start_seq: seq,
        start_offset: offset + cut,
    });
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

// AutoSi drops trailing zeros, never precision
fn message_ts(micros: Option<i64>) -> Option<String> {
    micros.and_then(|micros| {
        DateTime::from_timestamp_micros(micros)
            .map(|dt| dt.to_rfc3339_opts(SecondsFormat::AutoSi, true))
    })
}

fn limit(n: usize) -> i64 {
    i64::try_from(n).expect("row caps are small constants")
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arc_proto::v1::{
        MemoryRecord, MemoryRecordCreated, MemoryRecordDeleted, MemoryRecordSuperseded,
        MemoryRecordUpdated, MessageAppended, Provenance, ProvenanceEntry, Role, SessionCreated,
        SessionRole, Source, ToolOutcome, ToolResultRecorded, memory_event, memory_record,
        session_event,
    };
    use tempfile::TempDir;

    use super::{Archive, Error, SearchOptions, SearchOrder};
    use crate::testkit;

    const T0: i64 = 1_700_000_000_000_000;

    fn created(id: &str, title: &str) -> session_event::Event {
        session_event::Event::SessionCreated(SessionCreated {
            session_id: id.to_owned(),
            parent_session: String::new(),
            fork_point: 0,
            title: title.to_owned(),
            provider: "test".to_owned(),
            model: "test-model".to_owned(),
            role: SessionRole::Unspecified as i32,
            project: String::new(),
            budget: None,
            grants: Vec::new(),
            dispatched_by: String::new(),
            choice: String::new(),
            editing: String::new(),
            working_directory: String::new(),
            thinking: String::new(),
        })
    }

    fn said(session: &str, role: Role, content: &str) -> session_event::Event {
        session_event::Event::MessageAppended(MessageAppended {
            session_id: session.to_owned(),
            role: role as i32,
            content: content.to_owned(),
            partial: false,
            turn_id: String::new(),
            ..Default::default()
        })
    }

    fn tool_answered(session: &str, content: &str) -> session_event::Event {
        session_event::Event::ToolResultRecorded(ToolResultRecorded {
            changed_paths: Vec::new(),
            session_id: session.to_owned(),
            turn_id: "t-01".to_owned(),
            call_id: "c-01".to_owned(),
            outcome: ToolOutcome::Ok as i32,
            content: content.to_owned(),
            truncated: false,
        })
    }

    fn archive_over(events: Vec<session_event::Event>) -> (TempDir, Arc<Archive>) {
        archive_over_at(events.into_iter().map(|event| (event, None)).collect())
    }

    fn archive_over_at(
        events: Vec<(session_event::Event, Option<i64>)>,
    ) -> (TempDir, Arc<Archive>) {
        let dir = TempDir::new().expect("temp dir");
        testkit::seed_log_each(&dir, events);
        let archive = testkit::archive_at(&dir);
        (dir, archive)
    }

    fn newest() -> SearchOptions {
        SearchOptions {
            sort: SearchOrder::Newest,
            ..SearchOptions::default()
        }
    }

    fn session_ids(reply: &super::SearchReply) -> Vec<&str> {
        reply
            .sessions
            .iter()
            .map(|s| s.session_id.as_str())
            .collect()
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
            let reply = archive
                .search(query, false, None, SearchOptions::default())
                .expect(query);
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
            .search("prose AND OR bar", false, None, SearchOptions::default())
            .expect_err("FTS must reject doubled operators");
        assert!(matches!(err, Error::Query { .. }), "got: {err:?}");

        let err = archive
            .search("%%%", false, None, SearchOptions::default())
            .expect_err("nothing searchable must be a query error");
        assert!(matches!(err, Error::Query { .. }), "got: {err:?}");
    }

    #[test]
    fn an_abandoned_branchs_words_stay_searchable_in_the_archive() {
        use arc_proto::v1::{BranchMarked, branch_marked::Disposition};
        let mut branch = SessionCreated {
            session_id: "s-dead".to_owned(),
            parent_session: "s-root".to_owned(),
            fork_point: 1,
            ..match created("s-dead", "wrong turn") {
                session_event::Event::SessionCreated(c) => c,
                _ => unreachable!(),
            }
        };
        branch.title = "wrong turn".to_owned();
        let events = vec![
            created("s-root", "the trunk"),
            said("s-root", Role::User, "the good path"),
            session_event::Event::SessionCreated(branch),
            said("s-dead", Role::User, "an unrepeatable quixotic detour"),
            session_event::Event::BranchMarked(BranchMarked {
                session_id: "s-dead".to_owned(),
                disposition: Disposition::Abandoned as i32,
            }),
        ];
        let (_dir, archive) = archive_over(events);

        let reply = archive
            .search("quixotic", false, None, SearchOptions::default())
            .expect("search");
        assert_eq!(
            reply.sessions.first().map(|s| s.session_id.as_str()),
            Some("s-dead"),
            "abandoned never means erased: the archive still answers for it"
        );
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

        let reply = archive
            .search("zebra", false, None, SearchOptions::default())
            .expect("search");

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

        let reply = archive
            .search("quasar", false, None, SearchOptions::default())
            .expect("search");

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
    fn tool_results_are_excluded_by_default_and_found_when_lifted() {
        let (_dir, archive) = archive_over(vec![
            created("s-01", ""),
            said("s-01", Role::User, "please list the devices"),
            tool_answered("s-01", "vulkanpin gpu listing output"),
        ]);

        let reply = archive
            .search("vulkanpin", false, None, SearchOptions::default())
            .expect("search");
        assert!(
            reply.sessions.is_empty(),
            "the default filter excludes tool output"
        );

        let reply = archive
            .search("vulkanpin", true, None, SearchOptions::default())
            .expect("search");
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
    fn hits_previews_reads_and_ends_carry_roles_and_timestamps() {
        let (_dir, archive) = archive_over_at(vec![
            (created("s-01", "listing"), Some(T0)),
            (
                said("s-01", Role::User, "list the devices"),
                Some(T0 + 1_000_000),
            ),
            (
                tool_answered("s-01", "vulkanpin gpu listing output"),
                Some(T0 + 2_000_000),
            ),
            (
                said("s-01", Role::Assistant, "two devices, as you can see"),
                None,
            ),
        ]);

        let reply = archive
            .search("vulkanpin", true, None, SearchOptions::default())
            .expect("search");
        let hit = &reply.sessions[0];
        assert_eq!(hit.session_id, "s-01");
        assert_eq!(hit.role, "tool_result");
        assert_eq!(hit.source, "unknown");
        assert_eq!(hit.ts.as_deref(), Some("2023-11-14T22:13:22Z"));
        assert_eq!(
            messages(&hit.context),
            [
                ("user", "list the devices"),
                ("assistant", "two devices, as you can see")
            ]
        );
        assert_eq!(hit.context[0].ts.as_deref(), Some("2023-11-14T22:13:21Z"));
        assert_eq!(hit.context[1].ts, None);

        let ends = archive.ends("s-01").expect("ends").expect("session exists");
        assert_eq!(ends.first, hit.context);
        assert!(ends.last.is_empty());

        let read = archive
            .read_range("s-01", 0, i64::MAX, 0)
            .expect("read")
            .expect("session exists");
        assert_eq!(read.messages, hit.context);
    }

    #[test]
    fn retrieval_distinguishes_authorship_from_role_and_session_origin() {
        let dir = TempDir::new().expect("temp dir");
        let session = |id: &str, role: SessionRole, parent: &str| {
            session_event::Event::SessionCreated(SessionCreated {
                session_id: id.to_owned(),
                role: role as i32,
                dispatched_by: parent.to_owned(),
                ..Default::default()
            })
        };
        let events = [
            (session("human", SessionRole::Executor, ""), Source::User),
            (said("human", Role::User, "human choice"), Source::User),
            (said("human", Role::User, "system handback"), Source::System),
            (
                session("job", SessionRole::Executor, "human"),
                Source::Model,
            ),
            (said("job", Role::User, "job brief"), Source::Model),
            (said("job", Role::User, "human update"), Source::User),
            (
                session("old-job", SessionRole::Executor, "human"),
                Source::Model,
            ),
            (
                said("old-job", Role::User, "old job instruction"),
                Source::User,
            ),
            (
                session("legacy", SessionRole::Chat, ""),
                Source::Unspecified,
            ),
            (
                said("legacy", Role::User, "legacy note"),
                Source::Unspecified,
            ),
        ];
        testkit::seed_events(
            &dir,
            events
                .into_iter()
                .map(|(event, source)| arc_proto::v1::Event {
                    source: source as i32,
                    payload: Some(arc_proto::v1::event::Payload::Session(
                        arc_proto::v1::SessionEvent { event: Some(event) },
                    )),
                    ..Default::default()
                }),
        );
        let archive = testkit::archive_at(&dir);

        for (query, id, kind, parent, source) in [
            ("human choice", "human", "conversation", None, "user"),
            ("system handback", "human", "conversation", None, "system"),
            ("job brief", "job", "job", Some("human"), "model"),
            ("human update", "job", "job", Some("human"), "user"),
            (
                "old job instruction",
                "old-job",
                "job",
                Some("human"),
                "user",
            ),
            ("legacy note", "legacy", "unknown", None, "unknown"),
        ] {
            let reply = archive
                .search(query, false, None, SearchOptions::default())
                .expect("search");
            let hit = &reply.sessions[0];
            assert_eq!(hit.session_id, id);
            assert_eq!(hit.origin.kind, kind);
            assert_eq!(hit.origin.dispatched_by.as_deref(), parent);
            assert_eq!(hit.role, "user");
            assert_eq!(hit.source, source);
            let json = serde_json::to_value(hit).expect("serialized hit");
            assert_eq!(json["origin"]["kind"], kind);
            assert_eq!(json["source"], source);
            assert!(hit.context.iter().any(|message| {
                message.content == query && message.role == "user" && message.source == source
            }));
            let read = archive.read_range(id, 0, i64::MAX, 0).unwrap().unwrap();
            let ends = archive.ends(id).unwrap().unwrap();
            assert_eq!(read.origin, hit.origin);
            assert_eq!(ends.origin, hit.origin);
            assert!(
                read.messages
                    .iter()
                    .any(|message| { message.content == query && message.source == source })
            );
            assert!(
                ends.first
                    .iter()
                    .any(|message| { message.content == query && message.source == source })
            );
        }
    }

    #[test]
    fn session_origin_keeps_unrecorded_provenance_unknown() {
        for (source, parent, kind) in [
            (Source::User, None, "conversation"),
            (Source::Model, None, "job"),
            (Source::Unspecified, Some("parent"), "job"),
            (Source::User, Some("parent"), "job"),
            (Source::Unspecified, None, "unknown"),
            (Source::System, None, "unknown"),
        ] {
            let origin = super::SessionOrigin::new(source as i32, parent.map(str::to_owned));
            assert_eq!(origin.kind, kind);
            assert_eq!(origin.dispatched_by.as_deref(), parent);
        }
        assert_eq!(super::event_source(None), "unknown");
        assert_eq!(super::event_source(Some(99)), "unknown");
    }

    #[test]
    fn equal_ranks_tie_break_on_seq_and_sessions_anchor_their_newest_hit() {
        let (_dir, archive) = archive_over_at(vec![
            (created("s-01", ""), Some(T0)),
            (
                said("s-01", Role::User, "the tiebreaker run"),
                Some(T0 + 40_000_000),
            ),
            (
                said("s-01", Role::User, "the tiebreaker run"),
                Some(T0 + 10_000_000),
            ),
            (created("s-02", ""), Some(T0)),
            (
                said("s-02", Role::User, "the tiebreaker run"),
                Some(T0 + 30_000_000),
            ),
        ]);

        let query = "tiebreaker run";
        let relevant = archive
            .search(query, false, None, SearchOptions::default())
            .expect("search");
        assert_eq!(
            session_ids(&relevant),
            ["s-02", "s-01"],
            "equal bm25 falls back to seq DESC"
        );
        assert_eq!(
            relevant.sessions[1].ts.as_deref(),
            Some("2023-11-14T22:13:30Z"),
            "equal bm25 falls back to the later message"
        );

        let newest = archive
            .search(query, false, None, newest())
            .expect("search");
        assert_eq!(
            session_ids(&newest),
            ["s-01", "s-02"],
            "newest ranks by message time"
        );
        assert_eq!(
            newest.sessions[0].ts.as_deref(),
            Some("2023-11-14T22:14:00Z"),
            "the session anchors its newest hit"
        );
    }

    fn messages(messages: &[super::ProseMessage]) -> Vec<(&str, &str)> {
        messages
            .iter()
            .map(|m| (m.role.as_str(), m.content.as_str()))
            .collect()
    }

    #[test]
    fn read_pages_preserve_mixed_messages_and_stop_at_the_requested_end() {
        let originals: Vec<String> = [
            String::new(),
            "a".repeat(7900),
            "b".repeat(8000),
            "🦀".repeat(2100),
            "\"\\\n\u{0000}".repeat(2000),
            "c".repeat(32 * 1024),
            "last".to_owned(),
        ]
        .into_iter()
        .cycle()
        .take(28)
        .collect();
        let mut events = vec![(created("s-01", ""), Some(T0))];
        for (i, content) in originals.iter().enumerate() {
            let micros = T0 + i64::try_from(i + 1).expect("small fixture") * 1_000_000;
            events.push((tool_answered("s-01", "not prose"), Some(micros)));
            events.push((said("s-01", Role::Assistant, content), Some(micros)));
        }
        let last_seq = i64::try_from(events.len() - 1).expect("small fixture");
        events.push((
            said("s-01", Role::User, "outside the requested range"),
            Some(T0),
        ));
        let (_dir, archive) = archive_over_at(events);
        let mut cursor = super::ReadCursor {
            start_seq: 0,
            start_offset: 0,
        };
        let mut restored: Vec<super::ProseMessage> = Vec::new();
        let mut pages = 0;
        loop {
            let reply = archive
                .read_range("s-01", cursor.start_seq, last_seq, cursor.start_offset)
                .expect("read")
                .expect("session exists");
            assert!(super::read_reply_bytes(&reply) <= super::MAX_READ_BYTES);
            assert!(reply.messages.len() <= super::MAX_RANGE_ROWS);
            assert!(!reply.messages.is_empty());
            for message in reply.messages {
                assert_eq!(message.role, "assistant");
                assert_eq!(
                    message.ts,
                    super::message_ts(Some(message_time(message.seq))),
                    "every page of a message repeats its timestamp"
                );
                if let Some(last) = restored.last_mut().filter(|last| last.seq == message.seq) {
                    last.content.push_str(&message.content);
                } else {
                    assert!(restored.last().is_none_or(|last| last.seq < message.seq));
                    restored.push(message);
                }
            }
            pages += 1;
            assert!(pages < 100, "pagination terminates");
            let Some(next) = reply.next else {
                assert!(!reply.clipped);
                break;
            };
            assert!(reply.clipped);
            assert!(
                (next.start_seq, next.start_offset) > (cursor.start_seq, cursor.start_offset),
                "every continuation moves forward"
            );
            cursor = next;
        }
        assert!(pages > 1);
        assert_eq!(
            restored.into_iter().map(|m| m.content).collect::<Vec<_>>(),
            originals
        );
    }

    // Paired events place prose at even sequence numbers.
    fn message_time(seq: i64) -> i64 {
        T0 + (seq / 2) * 1_000_000
    }

    #[test]
    fn read_offsets_must_point_inside_the_exact_message_on_a_utf8_boundary() {
        let content = "é🦀tail";
        let (_dir, archive) = archive_over(vec![
            created("s-01", ""),
            said("s-01", Role::User, content),
            tool_answered("s-01", "not prose"),
            said("s-01", Role::User, "after the tool"),
        ]);
        let reply = archive
            .read_range("s-01", 0, i64::MAX, 0)
            .expect("read")
            .expect("session exists");
        let seq = reply.messages[0].seq;
        for (start_seq, offset) in [
            (seq, 1),
            (seq, 3),
            (seq, content.len()),
            (seq, usize::MAX),
            (seq - 1, 1),
            (seq + 1, 1),
            (i64::MAX, 1),
        ] {
            assert!(matches!(
                archive.read_range("s-01", start_seq, i64::MAX, offset),
                Err(Error::Read(_))
            ));
        }
        let tail = archive
            .read_range("s-01", seq, seq, "é🦀".len())
            .expect("read")
            .expect("session exists");
        assert_eq!(tail.messages[0].content, "tail");
        assert!(!tail.clipped);
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

    fn memory_archive_over(events: Vec<memory_event::Event>) -> (TempDir, Arc<Archive>) {
        let dir = TempDir::new().expect("temp dir");
        testkit::seed_memory_log(&dir, events);
        let archive = testkit::archive_at(&dir);
        (dir, archive)
    }

    fn memory_archive_over_each(
        events: Vec<(memory_event::Event, i64)>,
    ) -> (TempDir, Arc<Archive>) {
        let dir = TempDir::new().expect("temp dir");
        testkit::seed_memory_log_each(&dir, events);
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
            let hits = archive.memory_search(query, None, None).expect(query);
            assert_eq!(hits.len(), 1, "query: {query}");
            assert_eq!(hits[0].id, "mr-1", "query: {query}");
            assert_eq!(hits[0].kind, "fact");
            assert_eq!(hits[0].summary, "the palette everywhere");
        }
        assert!(
            archive
                .memory_search("gruvbox vulkan1", None, None)
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
            .memory_search("gruvbox", Some("arc"), None)
            .expect("search");
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].id, "mr-p");

        let hits = archive
            .memory_search("gruvbox", None, None)
            .expect("search");
        assert_eq!(hits.len(), 2, "no namespace means all namespaces");
    }

    #[test]
    fn as_of_surfaces_a_since_superseded_record_and_marks_it_so() {
        let (_dir, archive) = memory_archive_over_each(vec![
            (
                written(record(
                    "mr-old",
                    "global",
                    "Home",
                    "lives at X",
                    "lives at X",
                )),
                100,
            ),
            (
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
                200,
            ),
        ]);

        let hits = archive
            .memory_search("lives", None, Some(150))
            .expect("search");
        assert_eq!(hits.len(), 1, "held at t150: only the original");
        assert_eq!(hits[0].id, "mr-old");
        assert_eq!(hits[0].superseded, Some(true));

        let json = serde_json::to_string(&hits[0]).expect("json");
        assert!(json.contains("\"superseded\":true"), "{json}");

        let hits = archive.memory_search("lives", None, None).expect("search");
        assert_eq!(hits.len(), 1, "without as_of, only the active replacement");
        assert_eq!(hits[0].id, "mr-new");
        assert_eq!(hits[0].superseded, None);
        let json = serde_json::to_string(&hits[0]).expect("json");
        assert!(
            !json.contains("superseded"),
            "the field stays out of the reply when unused: {json}"
        );
    }

    #[test]
    fn as_of_before_creation_excludes_a_record_and_a_purge_excludes_it_at_any_as_of() {
        let (_dir, archive) = memory_archive_over_each(vec![(
            written(record(
                "mr-early",
                "global",
                "Early",
                "created late",
                "created late",
            )),
            200,
        )]);

        let hits = archive
            .memory_search("created", None, Some(100))
            .expect("search");
        assert!(hits.is_empty(), "as_of before created_at excludes the row");

        let (_dir, archive) = memory_archive_over_each(vec![
            (
                written(record(
                    "mr-gone",
                    "global",
                    "Gone",
                    "purged later",
                    "purged later",
                )),
                100,
            ),
            (
                memory_event::Event::RecordDeleted(MemoryRecordDeleted {
                    id: "mr-gone".to_owned(),
                }),
                300,
            ),
        ]);

        let hits = archive
            .memory_search("purged", None, Some(200))
            .expect("search");
        assert!(
            hits.is_empty(),
            "a purged record is gone from history too, even under as_of"
        );
    }

    #[test]
    fn a_substring_query_still_hits_via_the_like_fallback() {
        let (_dir, archive) = memory_archive_over(vec![written(record(
            "mr-1",
            "global",
            "Gruvbox",
            "the palette everywhere",
            "User prefers gruvbox via the terminal palette.",
        ))]);

        let hits = archive.memory_search("gruv", None, None).expect("search");
        assert_eq!(
            hits.len(),
            1,
            "FTS tokens don't match inside words; the LIKE fallback does"
        );
        assert_eq!(hits[0].id, "mr-1");
    }

    #[test]
    fn an_updated_record_is_searchable_by_new_content_not_old() {
        let (_dir, archive) = memory_archive_over(vec![
            written(record(
                "mr-1",
                "global",
                "Old title",
                "old summary",
                "mentions gruvbox",
            )),
            memory_event::Event::RecordUpdated(MemoryRecordUpdated {
                record: Some(record(
                    "mr-1",
                    "global",
                    "New title",
                    "new summary",
                    "mentions vulkan1",
                )),
            }),
        ]);

        let hits = archive
            .memory_search("vulkan1", None, None)
            .expect("search");
        assert_eq!(hits.len(), 1, "the FTS row resyncs to the new content");
        assert_eq!(hits[0].id, "mr-1");

        let hits = archive
            .memory_search("gruvbox", None, None)
            .expect("search");
        assert!(hits.is_empty(), "the old content no longer matches");
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
}
