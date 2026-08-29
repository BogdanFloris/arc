use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use arc_proto::v1::{MemoryRecordCreated, MemoryRecordSuperseded, Role, SessionRole, memory_event};
use futures::StreamExt as _;
use serde::Deserialize;

use super::{ExtractError, Extractor, SessionSnapshot};
use crate::memory::{index_line, kind_name};
use crate::projection::{MemoryIndexEntry, MessageRow};
use crate::provider::{CompletionDelta, CompletionRequest, Message, Provider, Stop, Thinking};
use crate::tool::builtin::memory::{mint_record, parse_kind};

pub const PROMPT_VERSION_V1: &str = "v1";

pub const PROMPT_V1: &str = r#"You are ARC's memory consolidation pass, reading one finished conversation.
Two questions decide what to extract: what did the user reveal about
themselves, and what did they express about how ARC should operate?
If nothing is worth saving, return an empty operations list and stop.

Do not capture:
- environment-dependent failures: the user can fix those, and the record
  outlives the fix
- negative claims about tools: "X is broken" hardens into refusals that
  outlive the problem
- transient errors that resolved: if retrying worked, the lesson is the
  retry pattern, not the failure
- unresolved dead ends dressed up as workflow

Phrase every record as a declarative fact, never as an imperative.
"User prefers concise replies" is right; "Always reply concisely" is wrong:
an imperative gets re-read as a directive in later sessions and can
override what the user is actually asking for.

The archive already remembers this conversation verbatim, searchably.
Extract only what must sit in the small always-loaded index; if it will be
stale in a week, it does not belong.

Before writing a new record, check the existing records listed after the
transcript. If one covers the same class of fact, extend or replace it
with a supersede operation instead of creating a narrow sibling.

Answer with strict JSON, nothing else after your thinking:
{"operations": []}
where each operation is one of
{"op": "write", "kind": "...", "title": "...", "summary": "...", "body": "...", "links": ["mr-..."]}
{"op": "supersede", "id": "mr-...", "kind": "...", "title": "...", "summary": "...", "body": "...", "links": []}
"kind" is one of person, project, preference, fact, decision. "summary" is
one declarative line; it appears in every future session. "links" is
optional related record ids. A supersede's "id" names the existing record
it replaces. An empty operations list means nothing was worth saving.
"#;

pub const PROMPT_VERSION_V2: &str = "v2";

pub const PROMPT_V2: &str = r#"You are ARC's memory consolidation pass, reading one finished conversation.
Most conversations contain nothing durable. Return {"operations": []}
unless a fact clearly earns a place in the small always-loaded index; an
empty list is the expected outcome, and a needless record is a failure,
not thoroughness.

Save a fact only if it would change ARC's replies in similar future
situations. Look, in order: corrections and mistakes the user pointed
out; stated preferences; stable facts about the user or their world.
Contrast: "User prefers short chapters" is worth saving; "User edited
chapter 3 today" is not — the archive already holds this conversation
verbatim and searchably.

Never capture: task progress or completed work; the conversation itself
("user asked about X"); anything the already-known section or the
existing records already cover; environment-dependent or transient
failures; negative claims about tools.

A record is a self-contained, present-tense declarative fact: names, not
pronouns; dates absolute; specifics kept specific ("Gamecube", never "a
console"). "User prefers concise replies" is right; "Always reply
concisely" is wrong — an imperative gets re-read as a directive later.

If an existing record covers the same fact and something changed, emit a
supersede of that record. If nothing changed, emit nothing for it.

Answer with strict JSON, nothing else after your thinking:
{"operations": []}
where each operation is one of
{"op": "write", "kind": "...", "title": "...", "summary": "...", "body": "...", "links": ["mr-..."]}
{"op": "supersede", "id": "mr-...", "kind": "...", "title": "...", "summary": "...", "body": "...", "links": []}
"kind" is one of person, project, preference, fact, decision. "summary" is
one declarative line; it appears in every future session. "links" is
optional related record ids. A supersede's "id" names the existing record
it replaces. An empty operations list means nothing was worth saving.
"#;

pub const PROMPT_VERSION_V3: &str = "v3";

pub const PROMPT_V3: &str = r#"You are ARC's memory consolidation pass, reading one finished conversation.
Most conversations contain nothing durable. Return {"operations": []}
unless a fact clearly earns a place in the small always-loaded index; an
empty list is the expected outcome, and a needless record is a failure,
not thoroughness.

Save a fact only if it would change ARC's replies in similar future
situations. Look, in order: corrections and mistakes the user pointed
out; stated preferences; stable facts about the user or their world.
Contrast: "User prefers short chapters" is worth saving; "User edited
chapter 3 today" is not — the archive already holds this conversation
verbatim and searchably.

Never capture: task progress or completed work; the conversation itself
("user asked about X"); anything the already-known section or the
existing records already cover; environment-dependent or transient
failures; negative claims about tools.

A record is a self-contained, present-tense declarative fact: names, not
pronouns; dates absolute; specifics kept specific ("Gamecube", never "a
console"). "User prefers concise replies" is right; "Always reply
concisely" is wrong — an imperative gets re-read as a directive later.

If an existing record covers the same fact and something changed, emit a
supersede of that record. If nothing changed, emit nothing for it.

Answer with strict JSON, nothing else after your thinking:
{"operations": []}
where each operation is one of
{"op": "write", "kind": "...", "namespace": "...", "title": "...", "summary": "...", "body": "...", "links": ["mr-..."]}
{"op": "supersede", "id": "mr-...", "kind": "...", "title": "...", "summary": "...", "body": "...", "links": []}
"kind" is one of person, project, preference, fact, decision. "namespace"
files the fact: choose from the namespaces listed in the input — a
project's name when the fact is about that project, "global" otherwise;
a supersede keeps its record's namespace. "summary" is
one declarative line; it appears in every future session. "links" is
optional related record ids. A supersede's "id" names the existing record
it replaces. An empty operations list means nothing was worth saving.
"#;

pub const PROMPT_VERSION_V4: &str = "v4";

pub const PROMPT_V4: &str = r#"You are ARC's memory consolidation pass, reading one finished conversation.
Most conversations contain nothing durable. Return {"operations": []}
unless a fact clearly earns a place in the small always-loaded index; an
empty list is the expected outcome, and a needless record is a failure,
not thoroughness.

Save a fact only if it would change ARC's replies in similar future
situations. Look, in order: corrections and mistakes the user pointed
out; stated preferences; stable facts about the user or their world.
Contrast: "User prefers short chapters" is worth saving; "User edited
chapter 3 today" is not — the archive already holds this conversation
verbatim and searchably.

Never capture: task progress or completed work; the conversation itself
("user asked about X"); anything the already-known section or the
existing records already cover; environment-dependent or transient
failures; negative claims about tools.

A record is a self-contained, present-tense declarative fact: names, not
pronouns; dates absolute; specifics kept specific ("Gamecube", never "a
console"). "User prefers concise replies" is right; "Always reply
concisely" is wrong — an imperative gets re-read as a directive later.

If an existing record covers the same fact and something changed, emit a
supersede of that record. If nothing changed, emit nothing for it.

Answer with strict JSON, nothing else after your thinking:
{"operations": []}
where each operation is one of
{"op": "write", "kind": "...", "namespace": "...", "title": "...", "summary": "...", "body": "...", "links": ["mr-..."]}
{"op": "supersede", "id": "mr-...", "kind": "...", "title": "...", "summary": "...", "body": "...", "links": []}
"kind" is one of person, project, preference, fact, decision. "namespace"
files the fact: choose from the namespaces listed in the input — a
project's name when the fact is about that project, "global" otherwise;
a supersede keeps its record's namespace. "summary" is
one declarative line; it appears in every future session. "links" names
ids from the existing-records list that this fact leans on — link a
record when this fact would need re-checking if that record changed;
otherwise leave links empty. A supersede's "id" names the existing record
it replaces. An empty operations list means nothing was worth saving.
"#;

pub const KNOWN_VERSIONS: &[(&str, &str)] = &[
    (PROMPT_VERSION_V1, PROMPT_V1),
    (PROMPT_VERSION_V2, PROMPT_V2),
    (PROMPT_VERSION_V3, PROMPT_V3),
    (PROMPT_VERSION_V4, PROMPT_V4),
];

pub const DEDUP_PROMPT_V1: &str = r#"You judge one candidate memory record against numbered existing records.
Think briefly, then answer with strict JSON, nothing else:
{"reasoning": "...", "duplicate_of": [], "supersedes": []}
duplicate_of: numbers of existing records stating the same fact — same
meaning counts even if the wording differs. supersedes: numbers of
existing records the candidate updates or contradicts — the same fact
with a changed value supersedes; it is not a duplicate. Records that
differ in numbers, dates, or qualifiers are never duplicates. Both lists
empty means the candidate is genuinely new.
"#;

const TRANSCRIPT_BUDGET: usize = 24_000;

const TOOL_SNIPPET: usize = 200;

const RECALLED_MARKER: &str = "\u{ab} [recalled — not extraction input]";

/// Tool results the extractor must not re-learn from: the model already
/// fetched this content from memory or the archive during the session.
const RECALL_TOOLS: &[&str] = &[
    "memory_read",
    "memory_search",
    "memory_write",
    "memory_supersede",
    "sessions_search",
    "session_read",
];

pub const TITLE_PROMPT: &str = "Write a short title for this conversation. \
At most six words, plain words, no quotes, no trailing punctuation. \
Reply with the title only.";

const TITLE_INPUT_CAP: usize = 500;

const TITLE_OUTPUT_CAP: usize = 60;

pub struct ModelExtractor {
    provider: Arc<dyn Provider>,
    model: String,
    thinking: Thinking,
    timeout: Duration,
    prompt: String,
    seed: Option<u64>,
    identity: Option<String>,
    namespaces: Vec<String>,
}

impl ModelExtractor {
    pub fn new(
        provider: Arc<dyn Provider>,
        model: &str,
        thinking: Thinking,
        timeout: Duration,
        identity: Option<String>,
        namespaces: Vec<String>,
    ) -> Self {
        Self {
            provider,
            model: model.to_owned(),
            thinking,
            timeout,
            prompt: PROMPT_V4.to_owned(),
            seed: None,
            identity,
            namespaces,
        }
    }

    pub(crate) fn pinned(
        provider: Arc<dyn Provider>,
        model: &str,
        timeout: Duration,
        prompt: &str,
        seed: u64,
        identity: Option<String>,
        namespaces: Vec<String>,
    ) -> Self {
        Self {
            provider,
            model: model.to_owned(),
            thinking: Thinking::Minimal,
            timeout,
            prompt: prompt.to_owned(),
            seed: Some(seed),
            identity,
            namespaces,
        }
    }
}

// FNV-1a — a session must seed the same way on every replay
pub(crate) fn session_seed(session_id: &str) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for byte in session_id.bytes() {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x100_0000_01b3);
    }
    hash
}

impl ModelExtractor {
    async fn completion_text(&self, request: CompletionRequest) -> Result<String, ExtractError> {
        let mut stream = self
            .provider
            .complete(request)
            .await
            .map_err(|error| ExtractError(format!("provider refused the request: {error}")))?;
        let mut text = String::new();
        let mut finished = false;
        while let Some(item) = stream.next().await {
            match item.map_err(|error| ExtractError(format!("stream failed: {error}")))? {
                CompletionDelta::Text(chunk) => text.push_str(&chunk),
                CompletionDelta::Reasoning(_)
                | CompletionDelta::ServerCall { .. }
                | CompletionDelta::ServerResponse { .. }
                | CompletionDelta::Grounding(_) => {}
                CompletionDelta::ToolCall(call) => {
                    return Err(ExtractError(format!(
                        "the model called {} with no tools offered",
                        call.name
                    )));
                }
                CompletionDelta::Done {
                    stop: Stop::EndTurn,
                    ..
                } => finished = true,
                CompletionDelta::Done {
                    stop: Stop::ToolCalls,
                    ..
                } => {
                    return Err(ExtractError(
                        "the model stopped for tool calls with no tools offered".to_owned(),
                    ));
                }
            }
        }
        if !finished {
            return Err(ExtractError(
                "stream cut before the model finished".to_owned(),
            ));
        }
        Ok(text)
    }

    /// Mechanical dedup between parsing and event conversion: stage 1 drops
    /// exact matches in code, stage 2 finds neighbors in code, stage 3 asks
    /// the model to judge only the ops with neighbors.
    async fn dedup(
        &self,
        operations: Vec<RawOperation>,
        index: &[MemoryIndexEntry],
        seed: u64,
    ) -> (Vec<RawOperation>, DedupStats) {
        let mut stats = DedupStats::default();
        let mut stage1 = Vec::with_capacity(operations.len());
        let mut seen_writes: Vec<(String, String)> = Vec::new();
        for op in operations {
            match op.op.as_str() {
                "write" => {
                    let norm_title = normalize(&op.title);
                    let norm_summary = normalize(&op.summary);
                    let index_dup = index.iter().any(|entry| {
                        normalize(&entry.title) == norm_title
                            && normalize(&entry.summary) == norm_summary
                    });
                    let batch_dup = seen_writes
                        .iter()
                        .any(|(title, summary)| *title == norm_title && *summary == norm_summary);
                    if index_dup || batch_dup {
                        stats.dropped += 1;
                        continue;
                    }
                    seen_writes.push((norm_title, norm_summary));
                    stage1.push(op);
                }
                "supersede" => {
                    let target = op
                        .id
                        .as_ref()
                        .and_then(|id| index.iter().find(|entry| entry.id == *id));
                    let touch = target.is_some_and(|target| {
                        normalize(&op.title) == normalize(&target.title)
                            && normalize(&op.summary) == normalize(&target.summary)
                            && normalize(&op.body) == normalize(&target.body)
                    });
                    if touch {
                        stats.dropped += 1;
                        continue;
                    }
                    stage1.push(op);
                }
                _ => stage1.push(op),
            }
        }

        let mut result = Vec::with_capacity(stage1.len());
        for op in stage1 {
            if op.op != "write" {
                result.push(op);
                continue;
            }
            let candidates = dedup_candidates(&op.title, &op.summary, index);
            if candidates.is_empty() {
                result.push(op);
                continue;
            }
            stats.calls += 1;
            match self.forced_choice(&op, &candidates, seed).await {
                Some(DedupChoice::Duplicate) => stats.dropped += 1,
                Some(DedupChoice::Supersede(target_id)) => {
                    stats.converted += 1;
                    result.push(RawOperation {
                        op: "supersede".to_owned(),
                        id: Some(target_id),
                        namespace: None,
                        kind: op.kind,
                        title: op.title,
                        summary: op.summary,
                        body: op.body,
                        links: op.links,
                    });
                }
                None => result.push(op),
            }
        }
        (result, stats)
    }

    /// A dedup call that fails to parse, errors, or times out keeps the
    /// write: the human review queue catches a duplicate, but a dropped
    /// fact is unrecoverable.
    async fn forced_choice(
        &self,
        op: &RawOperation,
        candidates: &[&MemoryIndexEntry],
        seed: u64,
    ) -> Option<DedupChoice> {
        let request = CompletionRequest {
            model: self.model.clone(),
            role: SessionRole::Archivist,
            thinking: self.thinking,
            system: Some(DEDUP_PROMPT_V1.to_owned()),
            messages: vec![Message::Text {
                role: Role::User,
                content: render_dedup_input(op, candidates),
                reasoning: None,
            }],
            tools: Vec::new(),
            seed: Some(seed),
            web: false,
        };
        let text = match tokio::time::timeout(self.timeout, self.completion_text(request)).await {
            Ok(Ok(text)) => text,
            Ok(Err(error)) => {
                tracing::warn!(%error, "dedup call failed; keeping the write");
                return None;
            }
            Err(_) => {
                tracing::warn!("dedup call timed out; keeping the write");
                return None;
            }
        };
        let reply: DedupReply = match serde_json::from_str(strip_residue(&text)) {
            Ok(reply) => reply,
            Err(error) => {
                tracing::warn!(%error, "unparseable dedup reply; keeping the write");
                return None;
            }
        };
        tracing::debug!(reasoning = %reply.reasoning, "dedup reasoning");
        apply_dedup_reply(&reply, candidates)
    }
}

impl Extractor for ModelExtractor {
    #[tracing::instrument(
        name = "consolidation.extract",
        skip_all,
        fields(
            task = "consolidation",
            session_id = %session.session_id,
            counter.dedup_dropped = tracing::field::Empty,
            counter.dedup_converted = tracing::field::Empty,
            dedup_calls = tracing::field::Empty,
        )
    )]
    async fn extract(
        &self,
        session: &SessionSnapshot,
    ) -> Result<Vec<memory_event::Event>, ExtractError> {
        let seed = self
            .seed
            .unwrap_or_else(|| session_seed(&session.session_id));
        let request = CompletionRequest {
            model: self.model.clone(),
            role: SessionRole::Archivist,
            thinking: self.thinking,
            system: Some(self.prompt.clone()),
            messages: vec![Message::Text {
                role: Role::User,
                content: render_input(session, self.identity.as_deref(), &self.namespaces),
                reasoning: None,
            }],
            tools: Vec::new(),
            seed: Some(seed),
            web: false,
        };
        let text = tokio::time::timeout(self.timeout, self.completion_text(request))
            .await
            .map_err(|_| {
                ExtractError(format!(
                    "model call timed out after {}s",
                    self.timeout.as_secs()
                ))
            })??;
        let operations = parse_operations(&text)?;
        tracing::debug!(operations = operations.len(), "extraction parsed");
        let (operations, stats) = self.dedup(operations, &session.memory_index, seed).await;
        let span = tracing::Span::current();
        if stats.dropped > 0 {
            span.record("counter.dedup_dropped", stats.dropped);
        }
        if stats.converted > 0 {
            span.record("counter.dedup_converted", stats.converted);
        }
        if stats.calls > 0 {
            span.record("dedup_calls", stats.calls);
        }
        operations
            .into_iter()
            .map(|op| to_event(op, session, &self.namespaces))
            .collect()
    }

    #[tracing::instrument(
        name = "consolidation.title",
        skip_all,
        fields(task = "consolidation", session_id = %session.session_id)
    )]
    async fn title(&self, session: &SessionSnapshot) -> Result<Option<String>, ExtractError> {
        let Some(prompt) = title_prompt(session) else {
            return Ok(None);
        };
        let request = CompletionRequest {
            model: self.model.clone(),
            role: SessionRole::Archivist,
            thinking: self.thinking,
            system: Some(TITLE_PROMPT.to_owned()),
            messages: vec![Message::Text {
                role: Role::User,
                content: prompt,
                reasoning: None,
            }],
            tools: Vec::new(),
            seed: Some(
                self.seed
                    .unwrap_or_else(|| session_seed(&session.session_id)),
            ),
            web: false,
        };
        let text = tokio::time::timeout(self.timeout, self.completion_text(request))
            .await
            .map_err(|_| {
                ExtractError(format!(
                    "title call timed out after {}s",
                    self.timeout.as_secs()
                ))
            })??;
        Ok(sanitize_title(&text))
    }
}

fn title_prompt(session: &SessionSnapshot) -> Option<String> {
    let first_user = first_message(session, Role::User)?;
    let first_assistant = first_message(session, Role::Assistant)?;
    Some(format!(
        "User: {}\nAssistant: {}",
        cap_chars(first_user, TITLE_INPUT_CAP),
        cap_chars(first_assistant, TITLE_INPUT_CAP),
    ))
}

fn first_message(session: &SessionSnapshot, role: Role) -> Option<&str> {
    session.rows.iter().find_map(|row| match row {
        MessageRow::Message {
            role: r, content, ..
        } if *r == role as i32 => Some(content.as_str()),
        _ => None,
    })
}

fn cap_chars(text: &str, limit: usize) -> String {
    text.chars().take(limit).collect()
}

fn sanitize_title(text: &str) -> Option<String> {
    let flattened: String = text
        .chars()
        .map(|c| if c == '\n' || c == '\r' { ' ' } else { c })
        .collect();
    let trimmed = flattened
        .trim()
        .trim_matches(|c: char| c == '"' || c == '\'')
        .trim();
    if trimmed.is_empty() {
        return None;
    }
    Some(cap_chars(trimmed, TITLE_OUTPUT_CAP))
}

fn render_input(
    session: &SessionSnapshot,
    identity: Option<&str>,
    namespaces: &[String],
) -> String {
    let mut calls: HashMap<&str, &str> = HashMap::new();
    let mut lines = Vec::with_capacity(session.rows.len());
    for row in &session.rows {
        if let MessageRow::ToolCall { call_id, name, .. } = row {
            calls.insert(call_id.as_str(), name.as_str());
        }
        lines.push(render_row(row, &calls));
    }
    let known = identity.unwrap_or("(none)");
    let index = if session.memory_index.is_empty() {
        "(none yet)".to_owned()
    } else {
        session
            .memory_index
            .iter()
            .map(index_line)
            .collect::<Vec<_>>()
            .join("\n")
    };
    format!(
        "[Session transcript]\n{}\n\n[Already known — never extract]\n{known}\n\n\
         [Namespaces]\n{}\n\n[Existing memory records]\n{index}",
        windowed(&lines),
        namespaces.join(", ")
    )
}

fn render_row(row: &MessageRow, calls: &HashMap<&str, &str>) -> String {
    match row {
        MessageRow::Message { role, content, .. } => {
            format!("{}: {content}", role_name(*role))
        }
        MessageRow::ToolCall {
            name,
            arguments_json,
            ..
        } => format!("\u{bb} {name}({})", snippet(arguments_json)),
        MessageRow::ToolResult {
            call_id, content, ..
        } => {
            let recalled = calls
                .get(call_id.as_str())
                .is_some_and(|name| RECALL_TOOLS.contains(name));
            if recalled {
                RECALLED_MARKER.to_owned()
            } else {
                format!("\u{ab} {}", snippet(content))
            }
        }
        // web content is transient, never a user fact; the extractor sees
        // that a search happened, not what came back
        MessageRow::ServerCall {
            name,
            arguments_json,
            ..
        } => format!(
            "\u{bb} {name}({}) [web \u{2014} not extraction input]",
            snippet(arguments_json)
        ),
    }
}

fn role_name(role: i32) -> String {
    match Role::try_from(role) {
        Ok(Role::User) => "user".to_owned(),
        Ok(Role::Assistant) => "assistant".to_owned(),
        Ok(Role::System) => "system".to_owned(),
        Ok(Role::Unspecified) | Err(_) => format!("role_{role}"),
    }
}

fn snippet(text: &str) -> String {
    let flat: String = text
        .chars()
        .take(TOOL_SNIPPET)
        .map(|c| if c == '\n' || c == '\r' { ' ' } else { c })
        .collect();
    if text.chars().count() > TOOL_SNIPPET {
        format!("{flat} [\u{2026}]")
    } else {
        flat
    }
}

fn windowed(lines: &[String]) -> String {
    let total: usize = lines.iter().map(|line| line.chars().count() + 1).sum();
    if total.saturating_sub(1) <= TRANSCRIPT_BUDGET {
        return lines.join("\n");
    }
    let mut kept = 0;
    let mut start = lines.len();
    while start > 0 {
        let cost = lines[start - 1].chars().count() + 1;
        if kept + cost > TRANSCRIPT_BUDGET {
            break;
        }
        kept += cost;
        start -= 1;
    }
    let start = start.min(lines.len() - 1);
    format!(
        "[transcript truncated: {start} earlier lines elided, the most recent follow]\n{}",
        lines[start..].join("\n")
    )
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawOperation {
    op: String,
    #[serde(default)]
    id: Option<String>,
    #[serde(default)]
    namespace: Option<String>,
    kind: String,
    title: String,
    summary: String,
    body: String,
    #[serde(default)]
    links: Vec<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Extraction {
    operations: Vec<RawOperation>,
}

fn parse_operations(text: &str) -> Result<Vec<RawOperation>, ExtractError> {
    let json = strip_residue(text);
    let extraction: Extraction = serde_json::from_str(json)
        .map_err(|error| ExtractError(format!("unparseable extraction: {error}")))?;
    Ok(extraction.operations)
}

fn strip_residue(text: &str) -> &str {
    let mut rest = text.trim();
    if let Some(after) = rest.strip_prefix("<think>") {
        rest = after
            .split_once("</think>")
            .map_or(after, |(_, tail)| tail)
            .trim();
    }
    if rest.starts_with("```") {
        if let Some((_, body)) = rest.split_once('\n') {
            rest = body.trim_end();
            rest = rest.strip_suffix("```").unwrap_or(rest);
        }
    }
    rest.trim()
}

fn normalize(text: &str) -> String {
    text.to_lowercase()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

const STOPWORDS: &[&str] = &[
    "a", "an", "the", "is", "are", "of", "to", "in", "on", "for", "and", "or", "with", "about",
    "user", "arc",
];

fn tokenize(normalized: &str) -> HashSet<&str> {
    normalized
        .split_whitespace()
        .filter(|word| !STOPWORDS.contains(word))
        .collect()
}

/// Entries sharing at least two content words with the op, top 3 by score,
/// ties broken by index order.
fn dedup_candidates<'a>(
    title: &str,
    summary: &str,
    index: &'a [MemoryIndexEntry],
) -> Vec<&'a MemoryIndexEntry> {
    let op_norm = normalize(&format!("{title} {summary}"));
    let op_words = tokenize(&op_norm);
    let mut scored: Vec<(usize, usize, &MemoryIndexEntry)> = index
        .iter()
        .enumerate()
        .filter_map(|(position, entry)| {
            let entry_norm = normalize(&format!("{} {}", entry.title, entry.summary));
            let score = tokenize(&entry_norm).intersection(&op_words).count();
            (score >= 2).then_some((score, position, entry))
        })
        .collect();
    scored.sort_by(|a, b| b.0.cmp(&a.0).then(a.1.cmp(&b.1)));
    scored
        .into_iter()
        .take(3)
        .map(|(_, _, entry)| entry)
        .collect()
}

#[derive(Default)]
struct DedupStats {
    dropped: usize,
    converted: usize,
    calls: usize,
}

enum DedupChoice {
    Duplicate,
    Supersede(String),
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct DedupReply {
    reasoning: String,
    #[serde(default)]
    duplicate_of: Vec<i64>,
    #[serde(default)]
    supersedes: Vec<i64>,
}

fn apply_dedup_reply(reply: &DedupReply, candidates: &[&MemoryIndexEntry]) -> Option<DedupChoice> {
    let len = candidates.len();
    let in_range = |number: i64| {
        usize::try_from(number)
            .ok()
            .filter(|n| *n >= 1 && *n <= len)
    };

    for &number in &reply.duplicate_of {
        if in_range(number).is_none() {
            tracing::warn!(
                number,
                "dedup duplicate_of names a nonexistent candidate; ignored"
            );
        }
    }
    if reply
        .duplicate_of
        .iter()
        .any(|&number| in_range(number).is_some())
    {
        return Some(DedupChoice::Duplicate);
    }

    for &number in &reply.supersedes {
        if in_range(number).is_none() {
            tracing::warn!(
                number,
                "dedup supersedes names a nonexistent candidate; ignored"
            );
        }
    }
    let mut hits: Vec<usize> = reply
        .supersedes
        .iter()
        .filter_map(|&number| in_range(number))
        .collect();
    if hits.is_empty() {
        return None;
    }
    hits.sort_unstable();
    if hits.len() > 1 {
        tracing::warn!(
            count = hits.len(),
            "dedup supersedes named more than one candidate; using the lowest"
        );
    }
    Some(DedupChoice::Supersede(candidates[hits[0] - 1].id.clone()))
}

fn render_dedup_input(op: &RawOperation, candidates: &[&MemoryIndexEntry]) -> String {
    use std::fmt::Write as _;

    let candidate = format!(
        "[Candidate]\nkind: {}\ntitle: {}\nsummary: {}\nbody: {}",
        op.kind, op.title, op.summary, op.body
    );
    let mut listed = String::from("[Existing records]");
    for (position, entry) in candidates.iter().enumerate() {
        let _ = write!(
            listed,
            "\n{}. kind: {}\n   namespace: {}\n   title: {}\n   summary: {}\n   body: {}",
            position + 1,
            kind_name(entry.kind),
            entry.namespace,
            entry.title,
            entry.summary,
            entry.body,
        );
    }
    format!("{candidate}\n\n{listed}")
}

fn to_event(
    op: RawOperation,
    session: &SessionSnapshot,
    namespaces: &[String],
) -> Result<memory_event::Event, ExtractError> {
    let Some(kind) = parse_kind(&op.kind) else {
        return Err(ExtractError(format!("unknown kind {:?}", op.kind)));
    };
    for (field, value) in [
        ("title", &op.title),
        ("summary", &op.summary),
        ("body", &op.body),
    ] {
        if value.trim().is_empty() {
            return Err(ExtractError(format!("empty {field}")));
        }
    }
    let namespace = op.namespace.as_deref().map(str::trim).and_then(|ns| {
        if namespaces.iter().any(|legal| legal == ns) {
            Some(ns.to_owned())
        } else {
            if !ns.is_empty() {
                tracing::warn!(namespace = ns, "unknown namespace; filing global");
            }
            None
        }
    });
    let RawOperation {
        op,
        id,
        title,
        summary,
        body,
        links,
        ..
    } = op;
    let known: HashSet<&str> = session
        .memory_index
        .iter()
        .map(|entry| entry.id.as_str())
        .collect();
    let mut seen = HashSet::new();
    let mut unknown = Vec::new();
    let links: Vec<String> = links
        .into_iter()
        .filter(|id| {
            if !known.contains(id.as_str()) {
                unknown.push(id.clone());
                return false;
            }
            seen.insert(id.clone())
        })
        .collect();
    if !unknown.is_empty() {
        tracing::warn!(ids = ?unknown, "extraction linked unknown record ids; dropped");
    }
    let mint = |namespace| {
        mint_record(
            kind,
            namespace,
            title,
            summary,
            body,
            links,
            &session.session_id,
        )
    };
    match op.as_str() {
        "write" => {
            if let Some(id) = id {
                return Err(ExtractError(format!("a write must not carry an id ({id})")));
            }
            Ok(memory_event::Event::RecordCreated(MemoryRecordCreated {
                record: Some(mint(namespace)),
            }))
        }
        "supersede" => {
            let Some(id) = id else {
                return Err(ExtractError("a supersede without an id".to_owned()));
            };
            let Some(target) = session.memory_index.iter().find(|entry| entry.id == id) else {
                return Err(ExtractError(format!("supersede of unknown record {id:?}")));
            };
            Ok(memory_event::Event::RecordSuperseded(
                MemoryRecordSuperseded {
                    superseded_id: id.clone(),
                    record: Some(mint(Some(target.namespace.clone()))),
                },
            ))
        }
        other => Err(ExtractError(format!("unknown op {other:?}"))),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::Arc;
    use std::time::Duration;

    use arc_proto::v1::{Role, Source, event, memory_event, memory_record, session_event};

    use crate::provider::Thinking;
    use tempfile::TempDir;

    use super::{
        KNOWN_VERSIONS, ModelExtractor, PROMPT_V1, PROMPT_V2, PROMPT_V3, PROMPT_V4,
        PROMPT_VERSION_V1, PROMPT_VERSION_V2, PROMPT_VERSION_V3, PROMPT_VERSION_V4, TITLE_PROMPT,
        TOOL_SNIPPET, TRANSCRIPT_BUDGET, dedup_candidates, normalize, render_input, sanitize_title,
        snippet, title_prompt, tokenize, windowed,
    };
    use crate::consolidation::{Extractor as _, Outcome, SessionSnapshot, run_pass};
    use crate::projection::{MemoryIndexEntry, MessageRow};
    use crate::provider::{
        CompletionDelta, CompletionRequest, CompletionStream, Error as ProviderError, Message,
        Provider, Stop,
    };
    use crate::testkit::{
        ScriptedProvider, channel, done_reply, engine, reopened_engine, replay_events,
        seed_memory_log, usage,
    };
    use crate::tool::Registry;

    const ALL_IDLE: i64 = i64::MAX;

    fn extraction_reply(json: &str) -> Vec<Result<CompletionDelta, ProviderError>> {
        vec![
            Ok(CompletionDelta::Reasoning("weighing durability".to_owned())),
            Ok(CompletionDelta::Reasoning(" of the exchange".to_owned())),
            Ok(CompletionDelta::Text(json.to_owned())),
            Ok(CompletionDelta::Done {
                usage: usage(),
                stop: Stop::EndTurn,
            }),
        ]
    }

    fn dedup_reply(json: &str) -> Vec<Result<CompletionDelta, ProviderError>> {
        vec![
            Ok(CompletionDelta::Reasoning(
                "comparing against neighbors".to_owned(),
            )),
            Ok(CompletionDelta::Text(json.to_owned())),
            Ok(CompletionDelta::Done {
                usage: usage(),
                stop: Stop::EndTurn,
            }),
        ]
    }

    fn entry(id: &str, title: &str, summary: &str) -> MemoryIndexEntry {
        entry_with_body(id, title, summary, "")
    }

    fn entry_with_body(id: &str, title: &str, summary: &str, body: &str) -> MemoryIndexEntry {
        entry_full(id, "global", title, summary, body)
    }

    fn entry_full(
        id: &str,
        namespace: &str,
        title: &str,
        summary: &str,
        body: &str,
    ) -> MemoryIndexEntry {
        MemoryIndexEntry {
            id: id.to_owned(),
            namespace: namespace.to_owned(),
            kind: memory_record::Kind::Fact as i32,
            title: title.to_owned(),
            summary: summary.to_owned(),
            body: body.to_owned(),
        }
    }

    fn snapshot(memory_index: Vec<MemoryIndexEntry>) -> SessionSnapshot {
        SessionSnapshot {
            session_id: "s-1".to_owned(),
            rows: vec![MessageRow::Message {
                role: Role::User as i32,
                content: "hi".to_owned(),
                partial: false,
                turn_id: "t-1".to_owned(),
                source: 0,
                input_tokens: 0,
                output_tokens: 0,
                elapsed_ms: 0,
                grounding_json: String::new(),
            }],
            latest_seq: 1,
            memory_index,
            role: arc_proto::v1::SessionRole::Concierge as i32,
        }
    }

    async fn extract_from(
        script: Vec<Result<CompletionDelta, ProviderError>>,
        index: Vec<MemoryIndexEntry>,
    ) -> Result<Vec<memory_event::Event>, crate::consolidation::ExtractError> {
        extract_scripted(vec![script], index).await
    }

    /// One script per model call the pass is expected to make, in order:
    /// the extraction call, then a dedup call per write op with neighbors.
    /// `ScriptedProvider` panics on script exhaustion, which proves a test
    /// that supplies only the extraction reply made no dedup call.
    async fn extract_scripted(
        scripts: Vec<Vec<Result<CompletionDelta, ProviderError>>>,
        index: Vec<MemoryIndexEntry>,
    ) -> Result<Vec<memory_event::Event>, crate::consolidation::ExtractError> {
        let provider = ScriptedProvider::scripted(scripts);
        let extractor = ModelExtractor::new(
            provider,
            "test-model",
            Thinking::Minimal,
            Duration::from_secs(5),
            None,
            vec!["global".to_owned(), "arc".to_owned()],
        );
        extractor.extract(&snapshot(index)).await
    }

    const WRITE_OP: &str = r#"{"operations":[{"op":"write","kind":"preference",
        "title":"Terse replies","summary":"User prefers short answers",
        "body":"User prefers short answers in chat.","links":[]}]}"#;

    /// Shares "coffee", "drinks", "every" with `overlap_neighbor`, three
    /// content words above the >= 2 candidate threshold.
    const OVERLAP_WRITE_OP: &str = r#"{"operations":[{"op":"write","kind":"fact",
        "title":"Coffee habit","summary":"drinks coffee every day",
        "body":"The user drinks coffee every day.","links":[]}]}"#;

    fn overlap_neighbor(id: &str, namespace: &str) -> MemoryIndexEntry {
        entry_full(
            id,
            namespace,
            "Coffee break",
            "drinks coffee every morning",
            "The user drinks coffee every morning.",
        )
    }

    #[tokio::test]
    async fn a_scripted_extraction_lands_records_marker_and_next_index() {
        let provider = ScriptedProvider::scripted(vec![
            done_reply("noted"),
            done_reply("  \"Terse replies\"\n"),
            extraction_reply(WRITE_OP),
            done_reply("hello again"),
        ]);
        let dir = TempDir::new().expect("temp dir");
        let (engine, run) = engine(&provider, &dir);
        let (tx, _rx) = channel();
        let reply = engine
            .send_message(&run, None, "remember: keep replies short", tx)
            .await
            .expect("send");

        let extractor = ModelExtractor::new(
            Arc::clone(&provider) as Arc<dyn Provider>,
            "test-model",
            Thinking::Minimal,
            Duration::from_secs(300),
            None,
            vec!["global".to_owned(), "arc".to_owned()],
        );
        let outcome = run_pass(
            &engine,
            &extractor,
            ALL_IDLE,
            PROMPT_VERSION_V4,
            &HashSet::new(),
        )
        .await
        .expect("pass");
        assert_eq!(
            outcome,
            Outcome::Consolidated {
                session_id: reply.session_id.clone(),
                through_seq: 2,
                records: 1,
                records_created: 1,
                records_superseded: 0,
            }
        );

        let title_request = &provider.requests()[1];
        assert_eq!(title_request.system.as_deref(), Some(TITLE_PROMPT));
        let [
            Message::Text {
                content: title_content,
                ..
            },
        ] = title_request.messages.as_slice()
        else {
            panic!(
                "expected one user message, got {:?}",
                title_request.messages
            );
        };
        assert_eq!(
            title_content,
            "User: remember: keep replies short\nAssistant: noted"
        );

        let request = &provider.requests()[2];
        assert_eq!(request.system.as_deref(), Some(PROMPT_V4));
        assert!(request.tools.is_empty());
        let [Message::Text { role, content, .. }] = request.messages.as_slice() else {
            panic!("expected one user message, got {:?}", request.messages);
        };
        assert_eq!(*role, Role::User);
        assert_eq!(
            content,
            "[Session transcript]\n\
             user: remember: keep replies short\n\
             assistant: noted\n\n\
             [Already known — never extract]\n\
             (none)\n\n\
             [Namespaces]\n\
             global, arc\n\n\
             [Existing memory records]\n\
             (none yet)"
        );

        let events = replay_events(dir.path());
        assert_eq!(events.len(), 6);
        let Some(event::Payload::Session(title_event)) = &events[3].payload else {
            panic!("expected the title event, got {:?}", events[3]);
        };
        let Some(session_event::Event::SessionTitled(titled)) = &title_event.event else {
            panic!("expected SessionTitled, got {title_event:?}");
        };
        assert_eq!(titled.session_id, reply.session_id);
        assert_eq!(
            titled.title, "Terse replies",
            "quotes and whitespace stripped"
        );

        assert_eq!(events[4].source, Source::System as i32);
        let Some(event::Payload::Memory(memory)) = &events[4].payload else {
            panic!("expected the record before the marker, got {:?}", events[4]);
        };
        let Some(memory_event::Event::RecordCreated(created)) = &memory.event else {
            panic!("expected RecordCreated, got {memory:?}");
        };
        let record = created.record.as_ref().expect("record");
        assert!(record.id.starts_with("mr-"), "{}", record.id);
        assert_eq!(record.kind, memory_record::Kind::Preference as i32);
        assert_eq!(record.namespace, "global");
        assert_eq!(record.status, memory_record::Status::Active as i32);
        let provenance = record.provenance.as_ref().expect("provenance");
        assert_eq!(provenance.entries.len(), 1);
        assert_eq!(provenance.entries[0].session_id, reply.session_id);
        assert!(provenance.entries[0].ts.is_some());
        let Some(event::Payload::Session(session)) = &events[5].payload else {
            panic!("expected the marker last, got {:?}", events[5]);
        };
        let Some(session_event::Event::SessionConsolidated(marker)) = &session.event else {
            panic!("expected SessionConsolidated, got {session:?}");
        };
        assert_eq!(marker.prompt_version, "v4");
        assert_eq!(marker.through_seq, 2);

        let (tx, _rx) = channel();
        engine
            .send_message(&run, Some(&reply.session_id), "hi again", tx)
            .await
            .expect("second send");
        let system = provider.requests()[3].system.clone().expect("system");
        assert!(system.contains("Terse replies"), "{system}");
        assert!(system.contains("User prefers short answers"), "{system}");
    }

    #[tokio::test]
    async fn a_supersede_op_retires_the_indexed_record() {
        let dir = TempDir::new().expect("temp dir");
        seed_memory_log(
            &dir,
            vec![memory_event::Event::RecordCreated(
                arc_proto::v1::MemoryRecordCreated {
                    record: Some(arc_proto::v1::MemoryRecord {
                        id: "mr-old".to_owned(),
                        kind: memory_record::Kind::Fact as i32,
                        namespace: "arc".to_owned(),
                        title: "Old address".to_owned(),
                        summary: "lives at X".to_owned(),
                        body: "The user lives at X.".to_owned(),
                        links: Vec::new(),
                        provenance: None,
                        status: memory_record::Status::Active as i32,
                    }),
                },
            )],
        );
        let provider = ScriptedProvider::scripted(vec![
            done_reply("noted"),
            done_reply("New address"),
            extraction_reply(
                r#"{"operations":[{"op":"supersede","id":"mr-old","kind":"fact",
                    "title":"New address","summary":"lives at Y",
                    "body":"The user moved to Y.","links":[]}]}"#,
            ),
        ]);
        let (engine, run) = reopened_engine(&provider, &dir, Registry::new(512));
        let (tx, _rx) = channel();
        let reply = engine
            .send_message(&run, None, "I moved to Y", tx)
            .await
            .expect("send");

        let extractor = ModelExtractor::new(
            Arc::clone(&provider) as Arc<dyn Provider>,
            "test-model",
            Thinking::Minimal,
            Duration::from_secs(300),
            None,
            vec!["global".to_owned(), "arc".to_owned()],
        );
        let outcome = run_pass(
            &engine,
            &extractor,
            ALL_IDLE,
            PROMPT_VERSION_V4,
            &HashSet::new(),
        )
        .await
        .expect("pass");
        assert!(matches!(outcome, Outcome::Consolidated { records: 1, .. }));

        let input = provider.requests()[2].messages.clone();
        let [Message::Text { content, .. }] = input.as_slice() else {
            panic!("expected one message");
        };
        assert!(
            content.contains("- arc/fact: Old address \u{2014} lives at X (id: mr-old)"),
            "{content}"
        );

        let events = replay_events(dir.path());
        let Some(event::Payload::Memory(memory)) = &events[events.len() - 2].payload else {
            panic!("expected the supersede before the marker");
        };
        let Some(memory_event::Event::RecordSuperseded(superseded)) = &memory.event else {
            panic!("expected RecordSuperseded, got {memory:?}");
        };
        assert_eq!(superseded.superseded_id, "mr-old");
        let replacement = superseded.record.as_ref().expect("record");
        assert_ne!(replacement.id, "mr-old");
        assert!(replacement.id.starts_with("mr-"), "{}", replacement.id);
        assert_eq!(replacement.namespace, "arc", "namespace inherited");
        let provenance = replacement.provenance.as_ref().expect("provenance");
        assert_eq!(provenance.entries[0].session_id, reply.session_id);
    }

    #[tokio::test]
    async fn a_garbage_title_reply_appends_nothing_and_the_pass_still_extracts() {
        let provider = ScriptedProvider::scripted(vec![
            done_reply("noted"),
            done_reply("   \n"),
            extraction_reply(WRITE_OP),
        ]);
        let dir = TempDir::new().expect("temp dir");
        let (engine, run) = engine(&provider, &dir);
        let (tx, _rx) = channel();
        engine
            .send_message(&run, None, "remember: keep replies short", tx)
            .await
            .expect("send");

        let extractor = ModelExtractor::new(
            Arc::clone(&provider) as Arc<dyn Provider>,
            "test-model",
            Thinking::Minimal,
            Duration::from_secs(300),
            None,
            vec!["global".to_owned(), "arc".to_owned()],
        );
        let outcome = run_pass(
            &engine,
            &extractor,
            ALL_IDLE,
            PROMPT_VERSION_V4,
            &HashSet::new(),
        )
        .await
        .expect("pass");
        assert!(
            matches!(outcome, Outcome::Consolidated { records: 1, .. }),
            "got: {outcome:?}"
        );

        for event in replay_events(dir.path()) {
            if let Some(event::Payload::Session(session)) = &event.payload {
                assert!(
                    !matches!(session.event, Some(session_event::Event::SessionTitled(_))),
                    "a blank reply must not title the session"
                );
            }
        }
    }

    #[test]
    fn sanitize_title_strips_quotes_whitespace_and_newlines() {
        assert_eq!(
            sanitize_title("  \"Terse replies\"\n"),
            Some("Terse replies".to_owned())
        );
        assert_eq!(
            sanitize_title("'lone quotes'"),
            Some("lone quotes".to_owned())
        );
        assert_eq!(sanitize_title("   \n\t  "), None, "blank after sanitizing");
        assert_eq!(sanitize_title(""), None);
    }

    #[test]
    fn sanitize_title_caps_at_sixty_chars() {
        let long = "word ".repeat(20);
        let title = sanitize_title(&long).expect("non-empty");
        assert_eq!(title.chars().count(), 60);
    }

    #[test]
    fn title_prompt_caps_each_side_at_five_hundred_chars_and_needs_both_roles() {
        let mut snapshot = snapshot(Vec::new());
        assert_eq!(title_prompt(&snapshot), None, "no assistant message yet");

        snapshot.rows.push(MessageRow::Message {
            role: Role::Assistant as i32,
            content: "y".repeat(600),
            partial: false,
            turn_id: "t-1".to_owned(),
            source: 0,
            input_tokens: 0,
            output_tokens: 0,
            elapsed_ms: 0,
            grounding_json: String::new(),
        });
        let prompt = title_prompt(&snapshot).expect("both roles present");
        assert!(prompt.starts_with("User: hi\nAssistant: "));
        assert_eq!(
            prompt
                .strip_prefix("User: hi\nAssistant: ")
                .expect("prefix")
                .chars()
                .count(),
            500
        );
    }

    #[tokio::test]
    async fn every_bad_batch_is_rejected_whole() {
        let cases: &[(&str, &str)] = &[
            ("this is not json", "unparseable"),
            (r#"{"operations":[],"note":"hi"}"#, "unparseable"),
            (
                r#"{"operations":[{"op":"vibe","kind":"fact","title":"t","summary":"s","body":"b"}]}"#,
                "unknown op",
            ),
            (
                r#"{"operations":[{"op":"write","kind":"vibe","title":"t","summary":"s","body":"b"}]}"#,
                "unknown kind",
            ),
            (
                r#"{"operations":[{"op":"write","kind":"fact","title":"t","summary":"  ","body":"b"}]}"#,
                "empty summary",
            ),
            (
                r#"{"operations":[{"op":"write","kind":"fact","title":"t","summary":"s"}]}"#,
                "unparseable",
            ),
            (
                r#"{"operations":[{"op":"write","id":"mr-x","kind":"fact","title":"t","summary":"s","body":"b"}]}"#,
                "must not carry an id",
            ),
            (
                r#"{"operations":[{"op":"supersede","kind":"fact","title":"t","summary":"s","body":"b"}]}"#,
                "without an id",
            ),
            (
                r#"{"operations":[{"op":"supersede","id":"mr-ghost","kind":"fact","title":"t","summary":"s","body":"b"}]}"#,
                "unknown record",
            ),
        ];
        for (text, needle) in cases {
            let err = extract_from(extraction_reply(text), vec![entry("mr-real", "Real", "is")])
                .await
                .expect_err(text);
            assert!(
                err.0.contains(needle),
                "case {text:?}: expected {needle:?} in {:?}",
                err.0
            );
        }

        let mixed = r#"{"operations":[
            {"op":"write","kind":"preference","title":"t","summary":"s","body":"b"},
            {"op":"supersede","id":"mr-ghost","kind":"fact","title":"t","summary":"s","body":"b"}]}"#;
        let err = extract_from(extraction_reply(mixed), Vec::new())
            .await
            .expect_err("the whole batch must fail");
        assert!(err.0.contains("unknown record"), "{}", err.0);
    }

    #[tokio::test]
    async fn residue_is_stripped_and_an_empty_list_extracts_nothing() {
        for text in [
            r#"{"operations": []}"#,
            "<think>nothing durable here</think>\n{\"operations\": []}",
            "```json\n{\"operations\": []}\n```",
        ] {
            let events = extract_from(extraction_reply(text), Vec::new())
                .await
                .unwrap_or_else(|error| panic!("{text:?}: {error}"));
            assert!(events.is_empty(), "{text:?}");
        }
    }

    #[tokio::test]
    async fn a_cut_stream_and_a_tool_stop_are_rejected() {
        let cut = vec![Ok(CompletionDelta::Text(WRITE_OP.to_owned()))];
        let err = extract_from(cut, Vec::new()).await.expect_err("cut");
        assert!(err.0.contains("cut"), "{}", err.0);

        let tool_stop = vec![Ok(CompletionDelta::Done {
            usage: usage(),
            stop: Stop::ToolCalls,
        })];
        let err = extract_from(tool_stop, Vec::new())
            .await
            .expect_err("tool stop");
        assert!(err.0.contains("no tools offered"), "{}", err.0);
    }

    #[derive(Debug)]
    struct Stalled;

    impl Provider for Stalled {
        fn name(&self) -> &'static str {
            "stalled"
        }

        fn complete(
            &self,
            _request: CompletionRequest,
        ) -> futures::future::BoxFuture<'_, Result<CompletionStream, ProviderError>> {
            Box::pin(std::future::pending())
        }
    }

    #[tokio::test]
    async fn a_hung_model_call_times_out_as_an_extract_error() {
        let extractor = ModelExtractor::new(
            Arc::new(Stalled),
            "test-model",
            Thinking::Minimal,
            Duration::from_millis(10),
            None,
            vec!["global".to_owned(), "arc".to_owned()],
        );
        let err = extractor
            .extract(&snapshot(Vec::new()))
            .await
            .expect_err("must time out");
        assert!(err.0.contains("timed out"), "{}", err.0);
    }

    #[tokio::test]
    async fn tool_rows_render_as_one_capped_line_each() {
        let mut snapshot = snapshot(Vec::new());
        snapshot.rows = vec![
            MessageRow::Message {
                role: Role::User as i32,
                content: "line one\nline two".to_owned(),
                partial: false,
                turn_id: "t".to_owned(),
                source: 0,
                input_tokens: 0,
                output_tokens: 0,
                elapsed_ms: 0,
                grounding_json: String::new(),
            },
            MessageRow::ToolCall {
                call_id: "c1".to_owned(),
                call_index: 0,
                name: "bash".to_owned(),
                arguments_json: r#"{"query":"palette"}"#.to_owned(),
                turn_id: "t".to_owned(),
                provider_roundtrip: Vec::new(),
            },
            MessageRow::ToolResult {
                call_id: "c1".to_owned(),
                outcome: 1,
                content: format!("row\n{}", "x".repeat(500)),
                truncated: false,
                turn_id: "t".to_owned(),
            },
        ];
        let input = render_input(&snapshot, None, &["global".to_owned()]);
        assert!(
            input.contains("user: line one\nline two"),
            "prose stays verbatim: {input}"
        );
        assert!(
            input.contains("\u{bb} bash({\"query\":\"palette\"})"),
            "{input}"
        );
        let result_line = input
            .lines()
            .find(|line| line.starts_with('\u{ab}'))
            .expect("a result line");
        assert!(result_line.ends_with("[\u{2026}]"), "{result_line}");
        assert!(
            result_line.chars().count() <= TOOL_SNIPPET + 10,
            "{result_line}"
        );
        assert!(!result_line.contains("row\nx"), "newlines flattened");
    }

    #[tokio::test]
    async fn a_recalled_memory_result_is_elided_but_a_bash_result_is_not() {
        let mut snapshot = snapshot(Vec::new());
        snapshot.rows = vec![
            MessageRow::ToolCall {
                call_id: "c1".to_owned(),
                call_index: 0,
                name: "memory_search".to_owned(),
                arguments_json: r#"{"query":"palette"}"#.to_owned(),
                turn_id: "t".to_owned(),
                provider_roundtrip: Vec::new(),
            },
            MessageRow::ToolResult {
                call_id: "c1".to_owned(),
                outcome: 1,
                content: "mr-1: User prefers dark mode".to_owned(),
                truncated: false,
                turn_id: "t".to_owned(),
            },
            MessageRow::ToolCall {
                call_id: "c2".to_owned(),
                call_index: 1,
                name: "bash".to_owned(),
                arguments_json: r#"{"cmd":"ls"}"#.to_owned(),
                turn_id: "t".to_owned(),
                provider_roundtrip: Vec::new(),
            },
            MessageRow::ToolResult {
                call_id: "c2".to_owned(),
                outcome: 1,
                content: "Cargo.toml\nsrc".to_owned(),
                truncated: false,
                turn_id: "t".to_owned(),
            },
        ];
        let input = render_input(&snapshot, None, &["global".to_owned()]);
        assert!(
            input.contains("\u{bb} memory_search({\"query\":\"palette\"})"),
            "the call line stays: {input}"
        );
        assert!(
            input.contains("\u{ab} [recalled \u{2014} not extraction input]"),
            "{input}"
        );
        assert!(
            !input.contains("User prefers dark mode"),
            "recalled content must not reach the extractor: {input}"
        );
        assert!(
            input.contains("\u{ab} Cargo.toml src"),
            "an unrelated tool's result still renders: {input}"
        );
    }

    #[test]
    fn identity_renders_in_the_already_known_section_or_none_absent() {
        let snapshot = snapshot(Vec::new());
        assert!(
            render_input(
                &snapshot,
                Some("The user is named Bogdan."),
                &["global".to_owned()]
            )
            .contains("[Already known — never extract]\nThe user is named Bogdan."),
        );
        assert!(
            render_input(&snapshot, None, &["global".to_owned()])
                .contains("[Already known — never extract]\n(none)"),
        );
    }

    #[test]
    fn the_snippet_keeps_short_payloads_whole() {
        assert_eq!(snippet("{\"q\":1}"), "{\"q\":1}");
    }

    #[test]
    fn a_long_transcript_is_windowed_to_its_tail() {
        let lines: Vec<String> = (0..600)
            .map(|n| format!("user: message number {n} padded {}", "p".repeat(30)))
            .collect();
        let windowed = windowed(&lines);
        assert!(
            windowed.starts_with("[transcript truncated: "),
            "{}",
            &windowed[..80]
        );
        assert!(windowed.ends_with(lines.last().expect("last").as_str()));
        assert!(!windowed.contains("message number 0 "), "the head dropped");
        let body: usize = windowed
            .lines()
            .skip(1)
            .map(|line| line.chars().count() + 1)
            .sum();
        assert!(body <= TRANSCRIPT_BUDGET + 1, "{body}");
    }

    #[test]
    fn a_short_transcript_is_untouched() {
        let lines = vec!["user: hi".to_owned(), "assistant: hello".to_owned()];
        assert_eq!(windowed(&lines), "user: hi\nassistant: hello");
    }

    #[test]
    fn prompt_v1_is_pinned() {
        assert_eq!(PROMPT_VERSION_V1, "v1");
        let pinned = r#"You are ARC's memory consolidation pass, reading one finished conversation.
Two questions decide what to extract: what did the user reveal about
themselves, and what did they express about how ARC should operate?
If nothing is worth saving, return an empty operations list and stop.

Do not capture:
- environment-dependent failures: the user can fix those, and the record
  outlives the fix
- negative claims about tools: "X is broken" hardens into refusals that
  outlive the problem
- transient errors that resolved: if retrying worked, the lesson is the
  retry pattern, not the failure
- unresolved dead ends dressed up as workflow

Phrase every record as a declarative fact, never as an imperative.
"User prefers concise replies" is right; "Always reply concisely" is wrong:
an imperative gets re-read as a directive in later sessions and can
override what the user is actually asking for.

The archive already remembers this conversation verbatim, searchably.
Extract only what must sit in the small always-loaded index; if it will be
stale in a week, it does not belong.

Before writing a new record, check the existing records listed after the
transcript. If one covers the same class of fact, extend or replace it
with a supersede operation instead of creating a narrow sibling.

Answer with strict JSON, nothing else after your thinking:
{"operations": []}
where each operation is one of
{"op": "write", "kind": "...", "title": "...", "summary": "...", "body": "...", "links": ["mr-..."]}
{"op": "supersede", "id": "mr-...", "kind": "...", "title": "...", "summary": "...", "body": "...", "links": []}
"kind" is one of person, project, preference, fact, decision. "summary" is
one declarative line; it appears in every future session. "links" is
optional related record ids. A supersede's "id" names the existing record
it replaces. An empty operations list means nothing was worth saving.
"#;
        assert_eq!(PROMPT_V1, pinned);
    }

    #[test]
    fn prompt_v2_is_pinned() {
        assert_eq!(PROMPT_VERSION_V2, "v2");
        let pinned = r#"You are ARC's memory consolidation pass, reading one finished conversation.
Most conversations contain nothing durable. Return {"operations": []}
unless a fact clearly earns a place in the small always-loaded index; an
empty list is the expected outcome, and a needless record is a failure,
not thoroughness.

Save a fact only if it would change ARC's replies in similar future
situations. Look, in order: corrections and mistakes the user pointed
out; stated preferences; stable facts about the user or their world.
Contrast: "User prefers short chapters" is worth saving; "User edited
chapter 3 today" is not — the archive already holds this conversation
verbatim and searchably.

Never capture: task progress or completed work; the conversation itself
("user asked about X"); anything the already-known section or the
existing records already cover; environment-dependent or transient
failures; negative claims about tools.

A record is a self-contained, present-tense declarative fact: names, not
pronouns; dates absolute; specifics kept specific ("Gamecube", never "a
console"). "User prefers concise replies" is right; "Always reply
concisely" is wrong — an imperative gets re-read as a directive later.

If an existing record covers the same fact and something changed, emit a
supersede of that record. If nothing changed, emit nothing for it.

Answer with strict JSON, nothing else after your thinking:
{"operations": []}
where each operation is one of
{"op": "write", "kind": "...", "title": "...", "summary": "...", "body": "...", "links": ["mr-..."]}
{"op": "supersede", "id": "mr-...", "kind": "...", "title": "...", "summary": "...", "body": "...", "links": []}
"kind" is one of person, project, preference, fact, decision. "summary" is
one declarative line; it appears in every future session. "links" is
optional related record ids. A supersede's "id" names the existing record
it replaces. An empty operations list means nothing was worth saving.
"#;
        assert_eq!(PROMPT_V2, pinned);
    }

    #[test]
    fn prompt_v3_is_pinned() {
        assert_eq!(PROMPT_VERSION_V3, "v3");
        let pinned = r#"You are ARC's memory consolidation pass, reading one finished conversation.
Most conversations contain nothing durable. Return {"operations": []}
unless a fact clearly earns a place in the small always-loaded index; an
empty list is the expected outcome, and a needless record is a failure,
not thoroughness.

Save a fact only if it would change ARC's replies in similar future
situations. Look, in order: corrections and mistakes the user pointed
out; stated preferences; stable facts about the user or their world.
Contrast: "User prefers short chapters" is worth saving; "User edited
chapter 3 today" is not — the archive already holds this conversation
verbatim and searchably.

Never capture: task progress or completed work; the conversation itself
("user asked about X"); anything the already-known section or the
existing records already cover; environment-dependent or transient
failures; negative claims about tools.

A record is a self-contained, present-tense declarative fact: names, not
pronouns; dates absolute; specifics kept specific ("Gamecube", never "a
console"). "User prefers concise replies" is right; "Always reply
concisely" is wrong — an imperative gets re-read as a directive later.

If an existing record covers the same fact and something changed, emit a
supersede of that record. If nothing changed, emit nothing for it.

Answer with strict JSON, nothing else after your thinking:
{"operations": []}
where each operation is one of
{"op": "write", "kind": "...", "namespace": "...", "title": "...", "summary": "...", "body": "...", "links": ["mr-..."]}
{"op": "supersede", "id": "mr-...", "kind": "...", "title": "...", "summary": "...", "body": "...", "links": []}
"kind" is one of person, project, preference, fact, decision. "namespace"
files the fact: choose from the namespaces listed in the input — a
project's name when the fact is about that project, "global" otherwise;
a supersede keeps its record's namespace. "summary" is
one declarative line; it appears in every future session. "links" is
optional related record ids. A supersede's "id" names the existing record
it replaces. An empty operations list means nothing was worth saving.
"#;
        assert_eq!(PROMPT_V3, pinned);
    }

    #[test]
    fn a_server_call_renders_its_query_and_never_its_response() {
        let line = super::render_row(
            &MessageRow::ServerCall {
                name: "google_search".to_owned(),
                arguments_json: r#"{"queries":["arc daemon"]}"#.to_owned(),
                response_json: "transient web content".to_owned(),
                turn_id: "t".to_owned(),
            },
            &std::collections::HashMap::new(),
        );
        assert!(line.contains("google_search"), "{line}");
        assert!(line.contains("not extraction input"), "{line}");
        assert!(
            !line.contains("transient web content"),
            "web content is never extraction input: {line}"
        );
    }

    #[test]
    fn v4_is_known_and_differs_from_v3_only_in_the_links_sentence() {
        assert!(
            KNOWN_VERSIONS
                .iter()
                .any(|&(version, prompt)| version == PROMPT_VERSION_V4 && prompt == PROMPT_V4)
        );
        let shared_header =
            "You are ARC's memory consolidation pass, reading one finished conversation.";
        assert!(PROMPT_V3.contains(shared_header));
        assert!(PROMPT_V4.contains(shared_header));

        let new_sentence = "would need re-checking if that record changed";
        assert!(
            !PROMPT_V3.contains(new_sentence),
            "v3 keeps its old, permissive links sentence"
        );
        assert!(
            PROMPT_V4.contains(new_sentence),
            "v4 replaces it with re-checking guidance"
        );
    }

    #[tokio::test]
    async fn a_v3_write_files_its_namespace_and_an_unknown_one_goes_global() {
        let filed = extract_from(
            extraction_reply(
                r#"{"operations":[{"op":"write","kind":"fact","namespace":"arc",
                    "title":"t","summary":"s","body":"b","links":[]}]}"#,
            ),
            Vec::new(),
        )
        .await
        .expect("filed");
        let [memory_event::Event::RecordCreated(created)] = filed.as_slice() else {
            panic!("expected one create");
        };
        assert_eq!(created.record.as_ref().expect("record").namespace, "arc");

        let unknown = extract_from(
            extraction_reply(
                r#"{"operations":[{"op":"write","kind":"fact","namespace":"vibes",
                    "title":"t","summary":"s","body":"b","links":[]}]}"#,
            ),
            Vec::new(),
        )
        .await
        .expect("kept");
        let [memory_event::Event::RecordCreated(created)] = unknown.as_slice() else {
            panic!("expected one create");
        };
        assert_eq!(
            created.record.as_ref().expect("record").namespace,
            "global",
            "misfiled beats lost: unknown namespaces fall to global"
        );
    }

    #[test]
    fn normalize_lowercases_collapses_whitespace_and_trims() {
        assert_eq!(
            normalize("  Terse   Replies\n\tare Good "),
            "terse replies are good"
        );
    }

    #[test]
    fn tokenize_drops_stopwords() {
        let words = tokenize("the user prefers a terse reply about arc");
        assert_eq!(words, HashSet::from_iter(["prefers", "terse", "reply"]));
    }

    #[test]
    fn dedup_candidates_takes_the_top_three_by_score_ties_broken_by_index_order() {
        let index = vec![
            entry("mr-1", "Coffee break", "drinks coffee this morning"),
            entry("mr-2", "Work log", "daily entries before lunch"),
            entry("mr-3", "Habit tracker", "tracks work hours"),
            entry("mr-4", "Coffee log", "daily brew notes"),
            entry("mr-5", "Morning routine", "starts before sunrise"),
            entry("mr-6", "Bicycle", "rides daily"),
        ];
        let candidates = dedup_candidates(
            "Morning coffee habit",
            "drinks coffee daily before work",
            &index,
        );
        assert_eq!(
            candidates
                .iter()
                .map(|entry| entry.id.as_str())
                .collect::<Vec<_>>(),
            ["mr-1", "mr-2", "mr-3"],
            "the two highest-scoring plus the lowest-index tie at score 2; \
             mr-6 scores 1 and never qualifies"
        );
    }

    #[test]
    fn dedup_candidates_is_empty_below_the_shared_word_threshold() {
        let index = vec![entry("mr-1", "Bicycle", "rides a bicycle to work")];
        assert!(
            dedup_candidates("Coffee habit", "drinks coffee every day", &index).is_empty(),
            "one shared word (\"work\" isn't even shared) must not clear the >= 2 bar"
        );
    }

    #[tokio::test]
    async fn an_exact_duplicate_write_is_dropped_with_no_dedup_call() {
        let events = extract_from(
            extraction_reply(WRITE_OP),
            vec![entry(
                "mr-1",
                "  terse REPLIES ",
                "user   prefers short answers",
            )],
        )
        .await
        .expect("extract");
        assert!(events.is_empty(), "{events:?}");
    }

    const BATCH_DUP_OP: &str = r#"{"operations":[
        {"op":"write","kind":"preference","title":"Terse replies",
         "summary":"User prefers short answers","body":"First body.","links":[]},
        {"op":"write","kind":"preference","title":"  terse   REPLIES ",
         "summary":"user   prefers short answers","body":"Second body.","links":[]}]}"#;

    #[tokio::test]
    async fn a_within_batch_duplicate_keeps_the_first_op() {
        let events = extract_from(extraction_reply(BATCH_DUP_OP), Vec::new())
            .await
            .expect("extract");
        assert_eq!(events.len(), 1);
        let memory_event::Event::RecordCreated(created) = &events[0] else {
            panic!("expected RecordCreated, got {:?}", events[0]);
        };
        assert_eq!(created.record.as_ref().expect("record").body, "First body.");
    }

    const TOUCH_SUPERSEDE_OP: &str = r#"{"operations":[{"op":"supersede","id":"mr-old","kind":"fact",
        "title":"Old address","summary":"lives at X","body":"The user lives at X.","links":[]}]}"#;

    #[tokio::test]
    async fn a_supersede_identical_to_its_target_is_dropped() {
        let events = extract_from(
            extraction_reply(TOUCH_SUPERSEDE_OP),
            vec![entry_with_body(
                "mr-old",
                "Old address",
                "lives at X",
                "The user lives at X.",
            )],
        )
        .await
        .expect("extract");
        assert!(events.is_empty(), "{events:?}");
    }

    #[tokio::test]
    async fn a_write_with_no_neighbors_makes_no_dedup_call() {
        let events = extract_from(
            extraction_reply(OVERLAP_WRITE_OP),
            vec![entry("mr-1", "Bicycle", "rides a bicycle to work")],
        )
        .await
        .expect("extract");
        assert_eq!(events.len(), 1);
        assert!(matches!(events[0], memory_event::Event::RecordCreated(_)));
    }

    const LINKED_WRITE_OP: &str = r#"{"operations":[{"op":"write","kind":"preference",
        "title":"Terse replies","summary":"User prefers short answers",
        "body":"User prefers short answers in chat.","links":["mr-real","mr-hallucinated"]}]}"#;

    #[tokio::test]
    async fn a_hallucinated_linked_id_is_dropped_and_the_real_one_kept() {
        let events = extract_from(
            extraction_reply(LINKED_WRITE_OP),
            vec![entry("mr-real", "Bicycle", "rides a bicycle to work")],
        )
        .await
        .expect("extract");
        assert_eq!(events.len(), 1);
        let memory_event::Event::RecordCreated(created) = &events[0] else {
            panic!("expected RecordCreated, got {:?}", events[0]);
        };
        let record = created.record.as_ref().expect("record");
        assert_eq!(
            record.links,
            ["mr-real"],
            "an id absent from the session's memory_index is dropped"
        );
    }

    #[tokio::test]
    async fn a_forced_choice_duplicate_of_drops_the_write() {
        let events = extract_scripted(
            vec![
                extraction_reply(OVERLAP_WRITE_OP),
                dedup_reply(r#"{"reasoning":"same fact","duplicate_of":[1],"supersedes":[]}"#),
            ],
            vec![overlap_neighbor("mr-1", "global")],
        )
        .await
        .expect("extract");
        assert!(events.is_empty(), "{events:?}");
    }

    #[tokio::test]
    async fn a_forced_choice_supersedes_converts_the_write_through_to_event() {
        let events = extract_scripted(
            vec![
                extraction_reply(OVERLAP_WRITE_OP),
                dedup_reply(r#"{"reasoning":"value changed","duplicate_of":[],"supersedes":[1]}"#),
            ],
            vec![overlap_neighbor("mr-1", "coffee-notes")],
        )
        .await
        .expect("extract");
        assert_eq!(events.len(), 1);
        let memory_event::Event::RecordSuperseded(superseded) = &events[0] else {
            panic!("expected RecordSuperseded, got {:?}", events[0]);
        };
        assert_eq!(superseded.superseded_id, "mr-1");
        let record = superseded.record.as_ref().expect("record");
        assert_ne!(record.id, "mr-1");
        assert_eq!(record.namespace, "coffee-notes", "namespace inherited");
        assert_eq!(record.title, "Coffee habit");
    }

    #[tokio::test]
    async fn a_forced_choice_with_both_lists_empty_keeps_the_write() {
        let events = extract_scripted(
            vec![
                extraction_reply(OVERLAP_WRITE_OP),
                dedup_reply(r#"{"reasoning":"unrelated","duplicate_of":[],"supersedes":[]}"#),
            ],
            vec![overlap_neighbor("mr-1", "global")],
        )
        .await
        .expect("extract");
        assert_eq!(events.len(), 1);
        assert!(matches!(events[0], memory_event::Event::RecordCreated(_)));
    }

    #[tokio::test]
    async fn an_unparseable_dedup_reply_keeps_the_write_and_extraction_still_succeeds() {
        let events = extract_scripted(
            vec![
                extraction_reply(OVERLAP_WRITE_OP),
                vec![
                    Ok(CompletionDelta::Text("not json at all".to_owned())),
                    Ok(CompletionDelta::Done {
                        usage: usage(),
                        stop: Stop::EndTurn,
                    }),
                ],
            ],
            vec![overlap_neighbor("mr-1", "global")],
        )
        .await
        .expect("extract");
        assert_eq!(events.len(), 1);
        assert!(matches!(events[0], memory_event::Event::RecordCreated(_)));
    }

    #[tokio::test]
    async fn an_out_of_range_duplicate_index_is_ignored_and_the_write_kept() {
        let events = extract_scripted(
            vec![
                extraction_reply(OVERLAP_WRITE_OP),
                dedup_reply(r#"{"reasoning":"miscounted","duplicate_of":[9],"supersedes":[]}"#),
            ],
            vec![overlap_neighbor("mr-1", "global")],
        )
        .await
        .expect("extract");
        assert_eq!(events.len(), 1);
        assert!(matches!(events[0], memory_event::Event::RecordCreated(_)));
    }
}
