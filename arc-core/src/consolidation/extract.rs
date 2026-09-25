use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use arc_proto::v1::{MemoryRecordCreated, MemoryRecordSuperseded, Role, SessionRole, memory_event};
use futures::StreamExt as _;
use serde::Deserialize;

use super::{ExtractError, Extractor, SessionSnapshot};
use crate::memory::index_line;
use crate::projection::{MemoryIndexEntry, MessageRow};
use crate::provider::{CompletionDelta, CompletionRequest, Message, Provider, Stop, Thinking};
use crate::tool::builtin::memory::{mint_record, parse_kind};

pub const PROMPT_VERSION: &str = "v5";

pub const PROMPT: &str = r#"Read the conversation and save only durable facts that would change ARC's future replies.
Most conversations need no memory: return {"operations":[]}.
Never save task progress, recalled facts, transient failures, or facts already known.
Use the shown existing records: omit unchanged facts, supersede changed facts by id,
and write only new facts. If unsure whether a fact is durable, omit it.
Records must be self-contained, present-tense facts, not instructions.
Use a listed namespace for new records; a supersede keeps its old namespace.
Reply with strict JSON only:
{"operations":[{"op":"write","kind":"fact","namespace":"global","title":"...","summary":"...","body":"...","links":[]}]}
An operation can instead use "op":"supersede" and "id":"mr-...".
Kinds: person, project, preference, fact, decision. Use [] for unused links.
"#;

const TRANSCRIPT_BUDGET: usize = 24_000;
const INDEX_BUDGET: usize = 6_000;
const INDEX_LIMIT: usize = 20;

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

pub const TITLE_PROMPT: &str = "Name the concrete task or topic that will help the user find this session later. \
Prefer the component, problem, or decision over generic words such as discussion, update, \
analysis, or work. Ignore greetings and assistant narration. Preserve useful project and \
component names. Examples: Session picker keyboard navigation; SPI display wiring; \
Compaction across tool batches. At most six words, plain words, no quotes or trailing \
punctuation. Reply with the title only. If the conversation contains only greetings \
or no identifiable topic, reply with an empty string.";

const TITLE_INPUT_CAP: usize = 500;

const TITLE_OUTPUT_CAP: usize = 60;

pub struct ModelExtractor {
    provider: Arc<dyn Provider>,
    model: String,
    thinking: Thinking,
    timeout: Duration,
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
            identity,
            namespaces,
        }
    }
}

fn session_seed(session_id: &str) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for byte in session_id.bytes() {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x100_0000_01b3);
    }
    hash
}

impl ModelExtractor {
    fn request(&self, session_id: &str, system: String, content: String) -> CompletionRequest {
        CompletionRequest {
            model: self.model.clone(),
            role: SessionRole::Archivist,
            thinking: self.thinking,
            thinking_updates: Vec::new(),
            system: Some(system),
            messages: vec![Message::Text {
                role: Role::User,
                content,
                reasoning: None,
            }],
            tools: Vec::new(),
            seed: Some(session_seed(session_id)),
            web: false,
            cache_key: Some(session_id.to_owned()),
        }
    }

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
                }
                | CompletionDelta::UnmeasuredDone {
                    stop: Stop::EndTurn,
                } => finished = true,
                CompletionDelta::Done {
                    stop: Stop::ToolCalls,
                    ..
                }
                | CompletionDelta::UnmeasuredDone {
                    stop: Stop::ToolCalls,
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
}

impl Extractor for ModelExtractor {
    #[tracing::instrument(
        name = "consolidation.extract",
        skip_all,
        fields(
            task = "consolidation",
            session_id = %session.session_id,
            counter.dedup_dropped = tracing::field::Empty,
        )
    )]
    async fn extract(
        &self,
        session: &SessionSnapshot,
    ) -> Result<Vec<memory_event::Event>, ExtractError> {
        let request = self.request(
            &session.session_id,
            PROMPT.to_owned(),
            render_input(session, self.identity.as_deref(), &self.namespaces),
        );
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
        let before = operations.len();
        let operations = drop_unchanged(operations, &session.memory_index);
        tracing::Span::current().record("counter.dedup_dropped", before - operations.len());
        operations
            .into_iter()
            .map(|op| to_event(op, session, &self.namespaces))
            .collect()
    }

    #[tracing::instrument(
        name = "consolidation.title",
        skip_all,
        fields(task = "consolidation", prompt_version = "title-v2", session_id = %session.session_id)
    )]
    async fn title(&self, session: &SessionSnapshot) -> Result<Option<String>, ExtractError> {
        let Some(prompt) = title_prompt(session) else {
            return Ok(None);
        };
        let request = self.request(&session.session_id, TITLE_PROMPT.to_owned(), prompt);
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

pub(super) fn title_prompt(session: &SessionSnapshot) -> Option<String> {
    let messages: Vec<(Role, &str)> = session
        .rows
        .iter()
        .filter_map(|row| match row {
            MessageRow::Message {
                role,
                source,
                content,
                ..
            } if *role == Role::User as i32 && *source != arc_proto::v1::Source::System as i32 => {
                Some((Role::User, content.as_str()))
            }
            MessageRow::Message { role, content, .. } if *role == Role::Assistant as i32 => {
                Some((Role::Assistant, content.as_str()))
            }
            _ => None,
        })
        .collect();
    let users: Vec<usize> = messages
        .iter()
        .enumerate()
        .filter(|(_, (role, _))| *role == Role::User)
        .map(|(index, _)| index)
        .collect();
    let assistants: Vec<usize> = messages
        .iter()
        .enumerate()
        .filter(|(_, (role, _))| *role == Role::Assistant)
        .map(|(index, _)| index)
        .collect();
    if users.is_empty() || assistants.is_empty() {
        return None;
    }
    let mut selected: Vec<usize> = users
        .iter()
        .take(3)
        .chain(users.iter().rev().take(3))
        .chain(assistants.first())
        .chain(assistants.last())
        .copied()
        .collect();
    selected.sort_unstable();
    selected.dedup();
    Some(
        selected
            .into_iter()
            .map(|index| {
                let (role, content) = messages[index];
                let label = if role == Role::User {
                    "User"
                } else {
                    "Assistant"
                };
                format!("{label}: {}", cap_chars(content, TITLE_INPUT_CAP))
            })
            .collect::<Vec<_>>()
            .join("\n"),
    )
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
    if trimmed.is_empty() || trimmed.chars().count() > TITLE_OUTPUT_CAP {
        return None;
    }
    Some(trimmed.to_owned())
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
    let index = relevant_index(session);
    format!(
        "[Session transcript]\n{}\n\n[Already known — never extract]\n{known}\n\n\
         [Namespaces]\n{}\n\n[Existing memory records]\n{index}",
        windowed(&lines),
        namespaces.join(", ")
    )
}

fn relevant_index(session: &SessionSnapshot) -> String {
    let user_text: String = session
        .rows
        .iter()
        .filter_map(|row| match row {
            MessageRow::Message { role, content, .. } if *role == Role::User as i32 => {
                Some(content.as_str())
            }
            _ => None,
        })
        .collect::<Vec<_>>()
        .join(" ");
    let user_words = tokenize(&user_text);
    let mut matches: Vec<_> = session
        .memory_index
        .iter()
        .enumerate()
        .filter_map(|(position, entry)| {
            let words = tokenize(&format!("{} {}", entry.title, entry.summary));
            let score = words.intersection(&user_words).count();
            (score > 0).then_some((score, position, entry))
        })
        .collect();
    matches.sort_by(|a, b| b.0.cmp(&a.0).then(a.1.cmp(&b.1)));

    let mut lines = Vec::new();
    let mut length = 0;
    for (_, _, entry) in matches.into_iter().take(INDEX_LIMIT) {
        let line = index_line(entry);
        if length + line.chars().count() + usize::from(!lines.is_empty()) > INDEX_BUDGET {
            continue;
        }
        length += line.chars().count() + usize::from(!lines.is_empty());
        lines.push(line);
    }
    if lines.is_empty() {
        "(none relevant)".to_owned()
    } else {
        lines.join("\n")
    }
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
    let text = lines.join("\n");
    let length = text.chars().count();
    if length <= TRANSCRIPT_BUDGET {
        return text;
    }
    let tail: String = text.chars().skip(length - TRANSCRIPT_BUDGET).collect();
    format!("[transcript truncated; latest text follows]\n{tail}")
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

fn tokenize(text: &str) -> HashSet<String> {
    text.to_lowercase()
        .split(|c: char| !c.is_alphanumeric())
        .filter(|word| word.len() > 2 && !STOPWORDS.contains(word))
        .map(str::to_owned)
        .collect()
}

fn drop_unchanged(operations: Vec<RawOperation>, index: &[MemoryIndexEntry]) -> Vec<RawOperation> {
    let mut seen: HashSet<_> = index
        .iter()
        .map(|entry| (normalize(&entry.title), normalize(&entry.summary)))
        .collect();
    operations
        .into_iter()
        .filter(|op| match op.op.as_str() {
            "write" => seen.insert((normalize(&op.title), normalize(&op.summary))),
            "supersede" => !index.iter().any(|entry| {
                op.id.as_deref() == Some(entry.id.as_str())
                    && normalize(&op.title) == normalize(&entry.title)
                    && normalize(&op.summary) == normalize(&entry.summary)
                    && normalize(&op.body) == normalize(&entry.body)
            }),
            _ => true,
        })
        .collect()
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
        INDEX_BUDGET, INDEX_LIMIT, ModelExtractor, PROMPT, PROMPT_VERSION, TITLE_PROMPT,
        TRANSCRIPT_BUDGET, render_input, windowed,
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
                attachments: Vec::new(),
            }],
            latest_seq: 1,
            memory_index,
            role: arc_proto::v1::SessionRole::Chat as i32,
            source: arc_proto::v1::Source::User as i32,
        }
    }

    async fn extract_from(
        script: Vec<Result<CompletionDelta, ProviderError>>,
        index: Vec<MemoryIndexEntry>,
    ) -> Result<Vec<memory_event::Event>, crate::consolidation::ExtractError> {
        extract_scripted(vec![script], index).await
    }

    async fn extract_scripted(
        scripts: Vec<Vec<Result<CompletionDelta, ProviderError>>>,
        index: Vec<MemoryIndexEntry>,
    ) -> Result<Vec<memory_event::Event>, crate::consolidation::ExtractError> {
        let provider = ScriptedProvider::scripted(scripts);
        let extractor = ModelExtractor::new(
            Arc::clone(&provider) as Arc<dyn Provider>,
            "test-model",
            Thinking::Minimal,
            Duration::from_secs(5),
            None,
            vec!["global".to_owned(), "arc".to_owned()],
        );
        let result = extractor.extract(&snapshot(index)).await;
        for request in provider.requests() {
            assert_eq!(request.cache_key.as_deref(), Some("s-1"));
        }
        result
    }

    const WRITE_OP: &str = r#"{"operations":[{"op":"write","kind":"preference",
        "title":"Terse replies","summary":"User prefers short answers",
        "body":"User prefers short answers in chat.","links":[]}]}"#;

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
        crate::consolidation::Titles::default()
            .run(&engine, &extractor)
            .await
            .expect("titles");
        let outcome = run_pass(
            &engine,
            &extractor,
            ALL_IDLE,
            PROMPT_VERSION,
            &HashSet::new(),
        )
        .await
        .expect("pass");
        assert_eq!(
            outcome,
            Outcome::Consolidated {
                session_id: reply.session_id.clone(),
                through_seq: 3,
                records: 1,
                records_created: 1,
                records_superseded: 0,
            }
        );

        let title_request = &provider.requests()[1];
        assert_eq!(
            title_request.cache_key.as_deref(),
            Some(reply.session_id.as_str())
        );
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
        assert_eq!(
            request.cache_key.as_deref(),
            Some(reply.session_id.as_str())
        );
        assert_eq!(request.system.as_deref(), Some(PROMPT));
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
             (none relevant)"
        );

        let events = replay_events(dir.path());
        assert_eq!(events.len(), 7);
        let Some(event::Payload::Session(title_event)) = &events[4].payload else {
            panic!("expected the title event, got {:?}", events[4]);
        };
        let Some(session_event::Event::SessionTitled(titled)) = &title_event.event else {
            panic!("expected SessionTitled, got {title_event:?}");
        };
        assert_eq!(titled.session_id, reply.session_id);
        assert_eq!(
            titled.title, "Terse replies",
            "quotes and whitespace stripped"
        );

        assert_eq!(events[5].source, Source::System as i32);
        let Some(event::Payload::Memory(memory)) = &events[5].payload else {
            panic!("expected the record before the marker, got {:?}", events[5]);
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
        let Some(event::Payload::Session(session)) = &events[6].payload else {
            panic!("expected the marker last, got {:?}", events[6]);
        };
        let Some(session_event::Event::SessionConsolidated(marker)) = &session.event else {
            panic!("expected SessionConsolidated, got {session:?}");
        };
        assert_eq!(marker.prompt_version, "v5");
        assert_eq!(marker.through_seq, 3);

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
            .send_message(&run, None, "I moved my address to Y", tx)
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
        crate::consolidation::Titles::default()
            .run(&engine, &extractor)
            .await
            .expect("titles");
        let outcome = run_pass(
            &engine,
            &extractor,
            ALL_IDLE,
            PROMPT_VERSION,
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
    fn a_long_transcript_is_windowed_to_its_tail() {
        let lines: Vec<String> = (0..600)
            .map(|n| format!("user: message number {n} padded {}", "p".repeat(30)))
            .collect();
        let windowed = windowed(&lines);
        assert!(windowed.starts_with("[transcript truncated;"));
        assert!(windowed.ends_with(lines.last().expect("last").as_str()));
        assert!(!windowed.contains("message number 0 "), "the head dropped");
        let body = windowed.split_once('\n').expect("marker").1;
        assert_eq!(body.chars().count(), TRANSCRIPT_BUDGET);
    }

    #[test]
    fn one_long_message_is_bounded_too() {
        let input = format!("user: {}", "x".repeat(TRANSCRIPT_BUDGET * 2));
        let body = windowed(&[input])
            .split_once('\n')
            .expect("marker")
            .1
            .to_owned();
        assert_eq!(body.chars().count(), TRANSCRIPT_BUDGET);
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

    #[tokio::test]
    async fn a_write_files_its_namespace_and_an_unknown_one_goes_global() {
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
    async fn a_near_match_needs_no_second_model_call() {
        let events = extract_scripted(
            vec![extraction_reply(OVERLAP_WRITE_OP)],
            vec![overlap_neighbor("mr-1", "coffee-notes")],
        )
        .await
        .expect("extract");
        assert_eq!(events.len(), 1);
        assert!(matches!(events[0], memory_event::Event::RecordCreated(_)));
    }

    #[test]
    fn the_existing_index_is_relevant_and_bounded() {
        let mut session = snapshot(
            (0..100)
                .map(|i| entry(&format!("mr-{i}"), "Coffee habit", &"coffee ".repeat(100)))
                .chain([entry("mr-unrelated", "Ski gear", "ski boots")])
                .collect(),
        );
        let MessageRow::Message { content, .. } = &mut session.rows[0] else {
            panic!("expected user message");
        };
        *content = "I drink coffee".to_owned();
        let input = render_input(&session, None, &["global".to_owned()]);
        let index = input
            .split("[Existing memory records]\n")
            .nth(1)
            .expect("index");
        assert!(index.chars().count() <= INDEX_BUDGET);
        assert!(index.lines().count() <= INDEX_LIMIT);
        assert!(index.contains("mr-0"));
        assert!(!index.contains("mr-unrelated"));
    }

    #[tokio::test]
    async fn a_model_supersede_needs_no_second_model_call() {
        let events = extract_scripted(
            vec![extraction_reply(
                r#"{"operations":[{"op":"supersede","id":"mr-1","kind":"fact",
                "title":"Coffee habit","summary":"drinks coffee every day",
                "body":"The user drinks coffee every day.","links":[]}]}"#,
            )],
            vec![overlap_neighbor("mr-1", "coffee-notes")],
        )
        .await
        .expect("extract");
        assert_eq!(events.len(), 1);
        let memory_event::Event::RecordSuperseded(superseded) = &events[0] else {
            panic!("expected supersede");
        };
        assert_eq!(superseded.superseded_id, "mr-1");
        assert_eq!(
            superseded.record.as_ref().expect("record").namespace,
            "coffee-notes"
        );
    }
}
