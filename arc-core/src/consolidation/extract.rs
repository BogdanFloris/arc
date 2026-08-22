//! Model-backed extraction (DESIGN.md §5.4): the versioned consolidation
//! prompt, the transcript rendering, and the reject-not-truncate validation
//! of what the model answers.
//!
//! [`ModelExtractor`] runs one completion on the same provider the engine
//! uses — the banked "consolidation uses the same local model" decision —
//! with its own system prompt, no tools, and thinking left on: extraction is
//! judgment, and 7.3's replay is the instrument that will say whether
//! thinking earns its latency (docs/prior-art-hermes.md §3).
//!
//! Validation is all-or-nothing (hermes §3): any operation that fails —
//! unparseable JSON, unknown op or kind, an empty required field, a
//! supersede of a record the snapshot's index does not hold — fails the
//! whole extraction, and the pass appends nothing. A partial batch is never
//! best-guessed into the distilled tier.

use std::sync::Arc;
use std::time::Duration;

use arc_proto::v1::{MemoryRecordCreated, MemoryRecordSuperseded, Role, memory_event};
use futures::StreamExt as _;
use serde::Deserialize;

use super::{ExtractError, Extractor, SessionSnapshot};
use crate::memory::index_line;
use crate::projection::MessageRow;
use crate::provider::{CompletionDelta, CompletionRequest, Message, Provider, Stop};
use crate::tool::memory::{mint_record, parse_kind};

/// The version string [`PROMPT_V1`] travels under: it lands in the
/// `SessionConsolidated` marker, and 7.3's replay diffs by it.
pub const PROMPT_VERSION_V1: &str = "v1";

/// The consolidation prompt, distilled from hermes' production curation
/// policy (docs/prior-art-hermes.md §1). Never edit this text: a prompt
/// change is a new constant and a new version string, so old markers keep
/// naming exactly what ran.
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

/// Every prompt version this build can run, for 7.3's replay: a new prompt
/// is a new entry here, never an edit to an old one.
pub const KNOWN_VERSIONS: &[(&str, &str)] = &[(PROMPT_VERSION_V1, PROMPT_V1)];

/// Longest rendered transcript, in chars. The tail is kept — the most
/// recent turns — behind an explicit truncation head-line. User messages
/// are windowed, never paraphrased (hermes' micro-compaction lesson).
const TRANSCRIPT_BUDGET: usize = 24_000;

/// Longest tool-line payload (arguments or result) kept, in chars: the
/// model needs the conversation, not tool dumps.
const TOOL_SNIPPET: usize = 200;

/// The 7.2 extractor: one completion per pass, on the engine's own provider.
pub struct ModelExtractor<P> {
    provider: Arc<P>,
    model: String,
    timeout: Duration,
    prompt: String,
    seed: Option<u64>,
}

impl<P: Provider> ModelExtractor<P> {
    /// `timeout` bounds the whole model call — consolidation's own generous
    /// dial, never the interactive one (hermes §3).
    #[must_use]
    pub fn new(provider: Arc<P>, model: impl Into<String>, timeout: Duration) -> Self {
        Self {
            provider,
            model: model.into(),
            timeout,
            prompt: PROMPT_V1.to_owned(),
            seed: None,
        }
    }

    /// Replay's prompt dial (task 7.3): run under this prompt text instead
    /// of [`PROMPT_V1`]. The live pass never calls this.
    #[must_use]
    pub fn with_prompt(mut self, prompt: impl Into<String>) -> Self {
        self.prompt = prompt.into();
        self
    }

    /// Replay's determinism dial: pin the sampler so run-to-run differences
    /// are attributable to the prompt. The live pass leaves it unset.
    #[must_use]
    pub fn with_seed(mut self, seed: u64) -> Self {
        self.seed = Some(seed);
        self
    }

    /// Runs the completion and concatenates its text, rejecting every shape
    /// that is not "prose then a clean end of turn".
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
                // Thinking is where the judgment happens; only the answer
                // after it is parsed.
                CompletionDelta::Reasoning(_) => {}
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
}

impl<P: Provider> Extractor for ModelExtractor<P> {
    #[tracing::instrument(
        name = "consolidation.extract",
        skip_all,
        fields(task = "consolidation", session_id = %session.session_id)
    )]
    async fn extract(
        &self,
        session: &SessionSnapshot,
    ) -> Result<Vec<memory_event::Event>, ExtractError> {
        let request = CompletionRequest {
            model: self.model.clone(),
            system: Some(self.prompt.clone()),
            messages: vec![Message::Text {
                role: Role::User,
                content: render_input(session),
            }],
            tools: Vec::new(),
            seed: self.seed,
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
        operations
            .into_iter()
            .map(|op| to_event(op, session))
            .collect()
    }
}

/// The extractor's whole input: the windowed transcript, then the memory
/// index — its only view of what already exists.
fn render_input(session: &SessionSnapshot) -> String {
    let lines: Vec<String> = session.rows.iter().map(render_row).collect();
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
        "[Session transcript]\n{}\n\n[Existing memory records]\n{index}",
        windowed(&lines)
    )
}

/// One transcript line per row: prose verbatim, tool traffic as compact
/// one-liners.
fn render_row(row: &MessageRow) -> String {
    match row {
        MessageRow::Message { role, content, .. } => {
            format!("{}: {content}", role_name(*role))
        }
        MessageRow::ToolCall {
            name,
            arguments_json,
            ..
        } => format!("\u{bb} {name}({})", snippet(arguments_json)),
        MessageRow::ToolResult { content, .. } => format!("\u{ab} {}", snippet(content)),
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

/// One line, capped at [`TOOL_SNIPPET`] chars, elision marked.
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

/// Joins the lines, keeping the most recent whole lines that fit
/// [`TRANSCRIPT_BUDGET`] behind an explicit truncation head-line.
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
    // Never window down to nothing: an oversized single line goes whole.
    let start = start.min(lines.len() - 1);
    format!(
        "[transcript truncated: {start} earlier lines elided, the most recent follow]\n{}",
        lines[start..].join("\n")
    )
}

/// One operation as the model wrote it. Strict serde: an unknown field
/// anywhere rejects the whole batch.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawOperation {
    op: String,
    #[serde(default)]
    id: Option<String>,
    kind: String,
    title: String,
    summary: String,
    body: String,
    #[serde(default)]
    links: Vec<String>,
}

/// The output contract's envelope.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Extraction {
    operations: Vec<RawOperation>,
}

/// Strips reasoning residue and fencing, then parses the strict contract.
fn parse_operations(text: &str) -> Result<Vec<RawOperation>, ExtractError> {
    let json = strip_residue(text);
    let extraction: Extraction = serde_json::from_str(json)
        .map_err(|error| ExtractError(format!("unparseable extraction: {error}")))?;
    Ok(extraction.operations)
}

/// Removes a terminated `<think>` block and a surrounding markdown fence.
/// Whatever remains must parse whole — reject, never truncate (hermes §3).
fn strip_residue(text: &str) -> &str {
    let mut rest = text.trim();
    if let Some(after) = rest.strip_prefix("<think>") {
        // An unterminated block means the model never stopped thinking; the
        // parse below rejects what is left.
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

/// One validated operation becomes one memory event, minted exactly like a
/// `memory_write` — fresh `mr-` id, ACTIVE, provenance naming the session.
fn to_event(
    op: RawOperation,
    session: &SessionSnapshot,
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
    let RawOperation {
        op,
        id,
        title,
        summary,
        body,
        links,
        ..
    } = op;
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
                record: Some(mint(None)),
            }))
        }
        "supersede" => {
            let Some(id) = id else {
                return Err(ExtractError("a supersede without an id".to_owned()));
            };
            // The replacement inherits the retired record's namespace: a
            // supersede corrects a record, it does not move it.
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
    use tempfile::TempDir;
    use tokio::sync::Mutex;

    use super::{
        ModelExtractor, PROMPT_V1, PROMPT_VERSION_V1, TOOL_SNIPPET, TRANSCRIPT_BUDGET,
        render_input, snippet, windowed,
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

    /// A scripted extraction turn: thinking first, then the answer text.
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
        MemoryIndexEntry {
            id: id.to_owned(),
            namespace: "global".to_owned(),
            kind: memory_record::Kind::Fact as i32,
            title: title.to_owned(),
            summary: summary.to_owned(),
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
            }],
            latest_seq: 1,
            memory_index,
        }
    }

    /// Extraction against a one-shot scripted stream, straight through the
    /// trait — the harness for every validation case.
    async fn extract_from(
        script: Vec<Result<CompletionDelta, ProviderError>>,
        index: Vec<MemoryIndexEntry>,
    ) -> Result<Vec<memory_event::Event>, crate::consolidation::ExtractError> {
        let provider = ScriptedProvider::scripted(vec![script]);
        let extractor = ModelExtractor::new(provider, "test-model", Duration::from_secs(5));
        extractor.extract(&snapshot(index)).await
    }

    const WRITE_OP: &str = r#"{"operations":[{"op":"write","kind":"preference",
        "title":"Terse replies","summary":"User prefers short answers",
        "body":"User prefers short answers in chat.","links":[]}]}"#;

    // --- the pass end to end, scripted ---

    /// The whole 7.2 chain: a scripted extraction turns a real session into
    /// a record and a marker in the log, provenance correct, and the next
    /// turn's index carries the new record.
    #[tokio::test]
    async fn a_scripted_extraction_lands_records_marker_and_next_index() {
        let provider = ScriptedProvider::scripted(vec![
            done_reply("noted"),
            extraction_reply(WRITE_OP),
            done_reply("hello again"),
        ]);
        let dir = TempDir::new().expect("temp dir");
        let engine = Mutex::new(engine(&provider, &dir));
        let (tx, _rx) = channel();
        let reply = engine
            .lock()
            .await
            .send_message(None, "remember: keep replies short", tx)
            .await
            .expect("send");

        let extractor = ModelExtractor::new(
            Arc::clone(&provider),
            "test-model",
            Duration::from_secs(300),
        );
        let outcome = run_pass(
            &engine,
            &extractor,
            ALL_IDLE,
            PROMPT_VERSION_V1,
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
            }
        );

        // The extraction request: its own prompt, no tools, thinking left on
        // (no /no_think anywhere), the rendered transcript then the index.
        let request = &provider.requests()[1];
        assert_eq!(request.system.as_deref(), Some(PROMPT_V1));
        assert!(request.tools.is_empty());
        let [Message::Text { role, content }] = request.messages.as_slice() else {
            panic!("expected one user message, got {:?}", request.messages);
        };
        assert_eq!(*role, Role::User);
        assert_eq!(
            content,
            "[Session transcript]\n\
             user: remember: keep replies short\n\
             assistant: noted\n\n\
             [Existing memory records]\n\
             (none yet)"
        );

        // The record, durable before the marker, sourced SYSTEM, minted like
        // a memory_write: mr- id, ACTIVE, provenance naming the session.
        let events = replay_events(&dir);
        assert_eq!(events.len(), 5);
        assert_eq!(events[3].source, Source::System as i32);
        let Some(event::Payload::Memory(memory)) = &events[3].payload else {
            panic!("expected the record before the marker, got {:?}", events[3]);
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
        let Some(event::Payload::Session(session)) = &events[4].payload else {
            panic!("expected the marker last, got {:?}", events[4]);
        };
        let Some(session_event::Event::SessionConsolidated(marker)) = &session.event else {
            panic!("expected SessionConsolidated, got {session:?}");
        };
        assert_eq!(marker.prompt_version, "v1");
        assert_eq!(marker.through_seq, 2);

        // The next turn's always-loaded index carries the extracted record.
        let (tx, _rx) = channel();
        engine
            .lock()
            .await
            .send_message(Some(&reply.session_id), "hi again", tx)
            .await
            .expect("second send");
        let system = provider.requests()[2].system.clone().expect("system");
        assert!(system.contains("Terse replies"), "{system}");
        assert!(system.contains("User prefers short answers"), "{system}");
    }

    /// A supersede op retires the named record and mints the replacement in
    /// the retired record's namespace.
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
            extraction_reply(
                r#"{"operations":[{"op":"supersede","id":"mr-old","kind":"fact",
                    "title":"New address","summary":"lives at Y",
                    "body":"The user moved to Y.","links":[]}]}"#,
            ),
        ]);
        let engine = Mutex::new(reopened_engine(&provider, &dir, Registry::new(512)));
        let (tx, _rx) = channel();
        let reply = engine
            .lock()
            .await
            .send_message(None, "I moved to Y", tx)
            .await
            .expect("send");

        let extractor = ModelExtractor::new(
            Arc::clone(&provider),
            "test-model",
            Duration::from_secs(300),
        );
        let outcome = run_pass(
            &engine,
            &extractor,
            ALL_IDLE,
            PROMPT_VERSION_V1,
            &HashSet::new(),
        )
        .await
        .expect("pass");
        assert!(matches!(outcome, Outcome::Consolidated { records: 1, .. }));

        // The snapshot's index reached the model as its merge context.
        let input = provider.requests()[1].messages.clone();
        let [Message::Text { content, .. }] = input.as_slice() else {
            panic!("expected one message");
        };
        assert!(
            content.contains("- arc/fact: Old address \u{2014} lives at X (id: mr-old)"),
            "{content}"
        );

        let events = replay_events(&dir);
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

    // --- validation: reject, never truncate ---

    /// Every rejection path fails the whole extraction with a message naming
    /// the problem; nothing is ever best-guessed into a partial batch.
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

        // One bad operation poisons the good one beside it.
        let mixed = r#"{"operations":[
            {"op":"write","kind":"preference","title":"t","summary":"s","body":"b"},
            {"op":"supersede","id":"mr-ghost","kind":"fact","title":"t","summary":"s","body":"b"}]}"#;
        let err = extract_from(extraction_reply(mixed), Vec::new())
            .await
            .expect_err("the whole batch must fail");
        assert!(err.0.contains("unknown record"), "{}", err.0);
    }

    /// Reasoning residue and fencing are stripped; the strict JSON after
    /// them still parses, and an empty list is a valid nothing-to-save.
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

    /// A provider that never answers: the timeout dial turns a hang into an
    /// `ExtractError` instead of a wedged pass.
    struct Stalled;

    impl Provider for Stalled {
        fn name(&self) -> &'static str {
            "stalled"
        }

        async fn complete(
            &self,
            _request: CompletionRequest,
        ) -> Result<CompletionStream, ProviderError> {
            std::future::pending().await
        }
    }

    #[tokio::test]
    async fn a_hung_model_call_times_out_as_an_extract_error() {
        let extractor =
            ModelExtractor::new(Arc::new(Stalled), "test-model", Duration::from_millis(10));
        let err = extractor
            .extract(&snapshot(Vec::new()))
            .await
            .expect_err("must time out");
        assert!(err.0.contains("timed out"), "{}", err.0);
    }

    // --- rendering ---

    #[tokio::test]
    async fn tool_rows_render_as_one_capped_line_each() {
        let mut snapshot = snapshot(Vec::new());
        snapshot.rows = vec![
            MessageRow::Message {
                role: Role::User as i32,
                content: "line one\nline two".to_owned(),
                partial: false,
                turn_id: "t".to_owned(),
            },
            MessageRow::ToolCall {
                call_id: "c1".to_owned(),
                call_index: 0,
                name: "memory_search".to_owned(),
                arguments_json: r#"{"query":"palette"}"#.to_owned(),
                turn_id: "t".to_owned(),
            },
            MessageRow::ToolResult {
                call_id: "c1".to_owned(),
                outcome: 1,
                content: format!("row\n{}", "x".repeat(500)),
                truncated: false,
                turn_id: "t".to_owned(),
            },
        ];
        let input = render_input(&snapshot);
        assert!(
            input.contains("user: line one\nline two"),
            "prose stays verbatim: {input}"
        );
        assert!(
            input.contains("\u{bb} memory_search({\"query\":\"palette\"})"),
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

    #[test]
    fn the_snippet_keeps_short_payloads_whole() {
        assert_eq!(snippet("{\"q\":1}"), "{\"q\":1}");
    }

    /// The budget keeps the tail: recent lines survive verbatim behind an
    /// explicit truncation head-line, old lines drop whole.
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

    // --- the prompt is pinned ---

    /// `PROMPT_V1` verbatim. A prompt change is a NEW constant and a NEW
    /// version string (v2, ...), never an edit to this text: the marker's
    /// `prompt_version` must keep naming exactly what ran, and 7.3's replay
    /// diffs by it.
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
}
