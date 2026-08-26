use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use arc_proto::v1::{
    MemoryRecord, MemoryRecordCreated, MemoryRecordSuperseded, Provenance, ProvenanceEntry,
    memory_event, memory_record,
};
use serde::Deserialize;

use crate::archive::{Archive, Error, MemoryHit};
use crate::provider::ToolDefinition;
use crate::store::now_ts;
use crate::tool::{Tool, ToolReply, ToolSource, TurnContext, to_json};

pub struct MemoryRead {
    archive: Arc<Archive>,
}

impl MemoryRead {
    pub fn new(archive: Arc<Archive>) -> Self {
        Self { archive }
    }
}

#[derive(Deserialize)]
struct ReadArgs {
    id: String,
}

impl Tool for MemoryRead {
    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: "memory_read".to_owned(),
            description: "Fetch one memory record in full — body, links, and the sessions \
                          it was learned in. Ids come from the memory index or memory_search."
                .to_owned(),
            parameters: serde_json::json!({
                "type": "object",
                "properties": {
                    "id": {"type": "string", "description": "Record id, e.g. mr-…"}
                },
                "required": ["id"]
            }),
        }
    }

    fn source(&self) -> ToolSource {
        ToolSource::Builtin
    }

    fn execute(
        &self,
        arguments_json: String,
        _ctx: TurnContext,
    ) -> Pin<Box<dyn Future<Output = ToolReply> + Send + '_>> {
        Box::pin(async move {
            let args: ReadArgs = match serde_json::from_str(&arguments_json) {
                Ok(args) => args,
                Err(error) => {
                    return ToolReply::error(format!(
                        "ERROR: bad memory_read arguments ({error}). Pass {{\"id\": \"mr-…\"}}."
                    ));
                }
            };
            match self.archive.memory_record(&args.id) {
                Ok(Some(record)) => ToolReply::ok(to_json(&record)),
                Ok(None) => unknown_record(&args.id),
                Err(error) => ToolReply::error(format!("ERROR: memory read failed ({error}).")),
            }
        })
    }
}

pub struct MemorySearch {
    archive: Arc<Archive>,
}

impl MemorySearch {
    pub fn new(archive: Arc<Archive>) -> Self {
        Self { archive }
    }
}

#[derive(Deserialize)]
struct SearchArgs {
    query: String,
    namespace: Option<String>,
}

#[derive(serde::Serialize)]
struct SearchReplyJson {
    records: Vec<MemoryHit>,
}

impl Tool for MemorySearch {
    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: "memory_search".to_owned(),
            description: "Search saved memory records by words in their title, summary, or \
                          body. Returns matching records without bodies; fetch one with \
                          memory_read."
                .to_owned(),
            parameters: serde_json::json!({
                "type": "object",
                "properties": {
                    "query": {"type": "string", "description": "Words to match."},
                    "namespace": {
                        "type": "string",
                        "description": "Only this namespace; omit for all."
                    }
                },
                "required": ["query"]
            }),
        }
    }

    fn source(&self) -> ToolSource {
        ToolSource::Builtin
    }

    fn execute(
        &self,
        arguments_json: String,
        _ctx: TurnContext,
    ) -> Pin<Box<dyn Future<Output = ToolReply> + Send + '_>> {
        Box::pin(async move {
            let args: SearchArgs = match serde_json::from_str(&arguments_json) {
                Ok(args) => args,
                Err(error) => {
                    return ToolReply::error(format!(
                        "ERROR: bad memory_search arguments ({error}). \
                         Pass {{\"query\": \"words to match\"}}."
                    ));
                }
            };
            match self
                .archive
                .memory_search(&args.query, args.namespace.as_deref())
            {
                Ok(records) if records.is_empty() => ToolReply::ok(
                    "No memory records match. For something from a past conversation, \
                     search sessions_search before giving up."
                        .to_owned(),
                ),
                Ok(records) => ToolReply::ok(to_json(&SearchReplyJson { records })),
                Err(Error::Query { message }) => ToolReply::ok(format!("No results: {message}.")),
                Err(error) => ToolReply::error(format!("ERROR: memory search failed ({error}).")),
            }
        })
    }
}

pub struct MemoryWrite;

pub struct MemorySupersede {
    archive: Arc<Archive>,
}

impl MemorySupersede {
    pub fn new(archive: Arc<Archive>) -> Self {
        Self { archive }
    }
}

#[derive(Deserialize)]
struct RecordArgs {
    kind: String,
    title: String,
    summary: String,
    body: String,
    namespace: Option<String>,
    #[serde(default)]
    links: Vec<String>,
}

#[derive(Deserialize)]
struct SupersedeArgs {
    id: String,
    #[serde(flatten)]
    record: RecordArgs,
}

fn record_properties() -> serde_json::Value {
    serde_json::json!({
        "kind": {
            "type": "string",
            "enum": ["person", "project", "preference", "fact", "decision"]
        },
        "title": {"type": "string", "description": "A few words naming the fact."},
        "summary": {
            "type": "string",
            "description": "One declarative line; shown in every session."
        },
        "body": {"type": "string", "description": "The full fact, markdown."},
        "namespace": {"type": "string", "description": "Project id; omit for global."},
        "links": {
            "type": "array",
            "items": {"type": "string"},
            "description": "Related record ids."
        }
    })
}

impl Tool for MemoryWrite {
    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: "memory_write".to_owned(),
            description: "Save a durable memory record. WHEN: the user states a preference, \
                          correction, or stable fact about themselves or their environment — \
                          the best memory stops the user repeating themselves. SKIP: trivia, \
                          task progress, anything sessions_search already answers; if it will \
                          be stale in a week it does not belong. Phrase records as declarative \
                          facts (\"User prefers X\"), never as instructions."
                .to_owned(),
            parameters: serde_json::json!({
                "type": "object",
                "properties": record_properties(),
                "required": ["kind", "title", "summary", "body"]
            }),
        }
    }

    fn source(&self) -> ToolSource {
        ToolSource::Builtin
    }

    fn execute(
        &self,
        arguments_json: String,
        ctx: TurnContext,
    ) -> Pin<Box<dyn Future<Output = ToolReply> + Send + '_>> {
        Box::pin(async move {
            let args: RecordArgs = match serde_json::from_str(&arguments_json) {
                Ok(args) => args,
                Err(error) => {
                    return ToolReply::error(format!(
                        "ERROR: bad memory_write arguments ({error}). Pass kind, title, \
                         summary, and body."
                    ));
                }
            };
            let record = match build_record(args, &ctx) {
                Ok(record) => record,
                Err(reply) => return reply,
            };
            let id = record.id.clone();
            ToolReply {
                content: format!("Saved (id: {id})."),
                ok: true,
                memory_events: vec![memory_event::Event::RecordCreated(MemoryRecordCreated {
                    record: Some(record),
                })],
                job_request: None,
            }
        })
    }
}

impl Tool for MemorySupersede {
    fn definition(&self) -> ToolDefinition {
        let mut properties = serde_json::Map::new();
        properties.insert(
            "id".to_owned(),
            serde_json::json!({"type": "string", "description": "Id of the record to replace."}),
        );
        if let serde_json::Value::Object(fields) = record_properties() {
            properties.extend(fields);
        }
        ToolDefinition {
            name: "memory_supersede".to_owned(),
            description: "Replace a memory record that is wrong or outdated. Pass the old id \
                          and the full corrected record; the old one is retired, not deleted."
                .to_owned(),
            parameters: serde_json::json!({
                "type": "object",
                "properties": properties,
                "required": ["id", "kind", "title", "summary", "body"]
            }),
        }
    }

    fn source(&self) -> ToolSource {
        ToolSource::Builtin
    }

    fn execute(
        &self,
        arguments_json: String,
        ctx: TurnContext,
    ) -> Pin<Box<dyn Future<Output = ToolReply> + Send + '_>> {
        Box::pin(async move {
            let args: SupersedeArgs = match serde_json::from_str(&arguments_json) {
                Ok(args) => args,
                Err(error) => {
                    return ToolReply::error(format!(
                        "ERROR: bad memory_supersede arguments ({error}). Pass id plus \
                         kind, title, summary, and body."
                    ));
                }
            };
            match self.archive.memory_record(&args.id) {
                Ok(Some(_)) => {}
                Ok(None) => return unknown_record(&args.id),
                Err(error) => {
                    return ToolReply::error(format!("ERROR: memory read failed ({error})."));
                }
            }
            let record = match build_record(args.record, &ctx) {
                Ok(record) => record,
                Err(reply) => return reply,
            };
            let content = format!("Superseded {} with {}.", args.id, record.id);
            ToolReply {
                content,
                ok: true,
                memory_events: vec![memory_event::Event::RecordSuperseded(
                    MemoryRecordSuperseded {
                        superseded_id: args.id,
                        record: Some(record),
                    },
                )],
                job_request: None,
            }
        })
    }
}

// an early-return-on-error reads best here; ToolReply is not on a hot path
#[allow(clippy::result_large_err)]
fn build_record(args: RecordArgs, ctx: &TurnContext) -> Result<MemoryRecord, ToolReply> {
    let Some(kind) = parse_kind(&args.kind) else {
        return Err(ToolReply::error(format!(
            "ERROR: unknown kind {:?}. Use person, project, preference, fact, \
             or decision.",
            args.kind
        )));
    };
    for (field, value) in [
        ("title", &args.title),
        ("summary", &args.summary),
        ("body", &args.body),
    ] {
        if value.trim().is_empty() {
            return Err(ToolReply::error(format!(
                "ERROR: {field} must not be empty."
            )));
        }
    }
    Ok(mint_record(
        kind,
        args.namespace,
        args.title,
        args.summary,
        args.body,
        args.links,
        &ctx.session_id,
    ))
}

pub(crate) fn parse_kind(name: &str) -> Option<memory_record::Kind> {
    match name {
        "person" => Some(memory_record::Kind::Person),
        "project" => Some(memory_record::Kind::Project),
        "preference" => Some(memory_record::Kind::Preference),
        "fact" => Some(memory_record::Kind::Fact),
        "decision" => Some(memory_record::Kind::Decision),
        _ => None,
    }
}

pub(crate) fn mint_record(
    kind: memory_record::Kind,
    namespace: Option<String>,
    title: String,
    summary: String,
    body: String,
    links: Vec<String>,
    session_id: &str,
) -> MemoryRecord {
    MemoryRecord {
        id: format!("mr-{}", uuid::Uuid::new_v4()),
        kind: kind as i32,
        namespace: namespace
            .filter(|namespace| !namespace.trim().is_empty())
            .unwrap_or_else(|| "global".to_owned()),
        title,
        summary,
        body,
        links,
        provenance: Some(Provenance {
            entries: vec![ProvenanceEntry {
                session_id: session_id.to_owned(),
                ts: Some(now_ts()),
            }],
        }),
        status: memory_record::Status::Active as i32,
    }
}

fn unknown_record(id: &str) -> ToolReply {
    ToolReply::error(format!(
        "ERROR: no memory record {id}. Ids come from the memory index or memory_search."
    ))
}

#[cfg(test)]
mod tests {
    use arc_proto::v1::{
        Event, MemoryRecord, MemoryRecordCreated, Source, ToolOutcome, event, memory_event,
        memory_record, session_event,
    };
    use tempfile::TempDir;

    use super::{MemoryRead, MemorySearch, MemorySupersede, MemoryWrite};
    use crate::testkit::{
        ScriptedProvider, archive_at, call, channel, done_reply, engine_with_tools_at,
        replay_events, seed_memory_log, tool_stop,
    };
    use crate::tool::{Registry, Tool as _, TurnContext};

    const WRITE_ARGS: &str = r#"{"kind":"preference","title":"Terse replies",
        "summary":"User prefers short answers","body":"User prefers short answers in chat."}"#;

    fn registry(tools: Vec<Box<dyn crate::tool::Tool>>) -> Registry {
        let mut registry = Registry::new(32 * 1024);
        for tool in tools {
            registry.register(tool);
        }
        registry
    }

    fn session_ev(event: &Event) -> &session_event::Event {
        match event.payload.as_ref() {
            Some(event::Payload::Session(session)) => session.event.as_ref().expect("event"),
            other => panic!("expected a session event, got {other:?}"),
        }
    }

    fn memory_ev(event: &Event) -> &memory_event::Event {
        match event.payload.as_ref() {
            Some(event::Payload::Memory(memory)) => memory.event.as_ref().expect("event"),
            other => panic!("expected a memory event, got {other:?}"),
        }
    }

    fn created_record(event: &Event) -> &MemoryRecord {
        match memory_ev(event) {
            memory_event::Event::RecordCreated(created) => created.record.as_ref().expect("record"),
            other => panic!("expected RecordCreated, got {other:?}"),
        }
    }

    fn seeded(id: &str, title: &str, summary: &str) -> memory_event::Event {
        memory_event::Event::RecordCreated(MemoryRecordCreated {
            record: Some(MemoryRecord {
                id: id.to_owned(),
                kind: memory_record::Kind::Fact as i32,
                namespace: "global".to_owned(),
                title: title.to_owned(),
                summary: summary.to_owned(),
                body: format!("{summary}, in full"),
                links: Vec::new(),
                provenance: None,
                status: memory_record::Status::Active as i32,
            }),
        })
    }

    #[tokio::test]
    async fn a_write_turn_appends_the_record_before_its_result() {
        let dir = TempDir::new().expect("temp dir");
        let provider = ScriptedProvider::scripted(vec![
            vec![
                Ok(call("c1", 0, "memory_write", WRITE_ARGS)),
                Ok(tool_stop()),
            ],
            done_reply("saved it"),
        ]);
        let (mut engine, run) =
            engine_with_tools_at(&provider, &dir, registry(vec![Box::new(MemoryWrite)]));
        let (tx, _rx) = channel();

        let reply = engine
            .send_message(&run, None, "remember this: terse replies", tx)
            .await
            .expect("send");

        let events = replay_events(dir.path());
        assert_eq!(events.len(), 6);
        assert!(matches!(
            session_ev(&events[2]),
            session_event::Event::ToolCallIssued(_)
        ));

        let record = created_record(&events[3]);
        assert_eq!(events[3].source, Source::Model as i32);
        assert!(record.id.starts_with("mr-"), "{}", record.id);
        assert_eq!(record.namespace, "global", "namespace defaults to global");
        assert_eq!(record.status, memory_record::Status::Active as i32);
        assert_eq!(record.kind, memory_record::Kind::Preference as i32);
        let provenance = record.provenance.as_ref().expect("provenance");
        assert_eq!(provenance.entries.len(), 1);
        assert_eq!(provenance.entries[0].session_id, reply.session_id);
        assert!(provenance.entries[0].ts.is_some());

        let session_event::Event::ToolResultRecorded(result) = session_ev(&events[4]) else {
            panic!("expected the result after the write, got {:?}", events[4]);
        };
        assert_eq!(result.outcome, ToolOutcome::Ok as i32);
        assert_eq!(result.content, format!("Saved (id: {}).", record.id));
    }

    #[tokio::test]
    async fn the_next_turn_carries_the_written_record_in_its_index() {
        let dir = TempDir::new().expect("temp dir");
        let provider = ScriptedProvider::scripted(vec![
            vec![
                Ok(call("c1", 0, "memory_write", WRITE_ARGS)),
                Ok(tool_stop()),
            ],
            done_reply("saved it"),
            done_reply("hello again"),
        ]);
        let (mut engine, run) =
            engine_with_tools_at(&provider, &dir, registry(vec![Box::new(MemoryWrite)]));
        let (tx, _rx) = channel();
        let reply = engine
            .send_message(&run, None, "remember this", tx)
            .await
            .expect("send");
        let (tx, _rx) = channel();
        engine
            .send_message(&run, Some(&reply.session_id), "what do you know?", tx)
            .await
            .expect("second send");

        let requests = provider.requests();
        let first_system = requests[0].system.as_deref().expect("system");
        assert!(
            !first_system.contains("Terse replies"),
            "the writing turn's snapshot predates the write"
        );
        let next_system = requests[2].system.as_deref().expect("system");
        assert!(next_system.contains("Terse replies"), "{next_system}");
        assert!(
            next_system.contains("User prefers short answers"),
            "{next_system}"
        );
    }

    #[tokio::test]
    async fn memory_read_round_trips_a_written_record() {
        let dir = TempDir::new().expect("temp dir");
        let provider = ScriptedProvider::scripted(vec![
            vec![
                Ok(call("c1", 0, "memory_write", WRITE_ARGS)),
                Ok(tool_stop()),
            ],
            done_reply("saved it"),
        ]);
        let (mut engine, run) =
            engine_with_tools_at(&provider, &dir, registry(vec![Box::new(MemoryWrite)]));
        let (tx, _rx) = channel();
        let reply = engine
            .send_message(&run, None, "remember this", tx)
            .await
            .expect("send");
        let id = created_record(&replay_events(dir.path())[3]).id.clone();

        let tool = MemoryRead::new(archive_at(&dir));
        let read = tool
            .execute(format!(r#"{{"id":"{id}"}}"#), TurnContext::default())
            .await;

        assert!(read.ok, "{}", read.content);
        assert!(
            read.content.contains("User prefers short answers in chat."),
            "{}",
            read.content
        );
        assert!(read.content.contains(&reply.session_id), "{}", read.content);
        assert!(read.content.contains("\"active\""), "{}", read.content);
    }

    #[tokio::test]
    async fn a_supersede_turn_retires_the_record_and_the_next_index_shows_the_replacement() {
        let dir = TempDir::new().expect("temp dir");
        seed_memory_log(&dir, vec![seeded("mr-old", "Old address", "lives at X")]);
        let provider = ScriptedProvider::scripted(vec![
            vec![
                Ok(call(
                    "c1",
                    0,
                    "memory_supersede",
                    r#"{"id":"mr-old","kind":"fact","title":"New address",
                        "summary":"lives at Y","body":"User moved to Y."}"#,
                )),
                Ok(tool_stop()),
            ],
            done_reply("updated"),
            done_reply("hello again"),
        ]);
        let registry = registry(vec![Box::new(MemorySupersede::new(archive_at(&dir)))]);
        let (mut engine, run) = engine_with_tools_at(&provider, &dir, registry);
        let (tx, _rx) = channel();
        let reply = engine
            .send_message(&run, None, "I moved to Y", tx)
            .await
            .expect("send");
        let (tx, _rx) = channel();
        engine
            .send_message(&run, Some(&reply.session_id), "where do I live?", tx)
            .await
            .expect("second send");

        let events = replay_events(dir.path());
        let memory_event::Event::RecordSuperseded(superseded) = memory_ev(&events[4]) else {
            panic!(
                "expected RecordSuperseded before the result, got {:?}",
                events[4]
            );
        };
        assert_eq!(superseded.superseded_id, "mr-old");
        let replacement = superseded.record.as_ref().expect("record");
        assert_ne!(replacement.id, "mr-old", "the replacement gets a fresh id");
        let session_event::Event::ToolResultRecorded(result) = session_ev(&events[5]) else {
            panic!("expected the result, got {:?}", events[5]);
        };
        assert_eq!(
            result.content,
            format!("Superseded mr-old with {}.", replacement.id)
        );

        let next_system = provider.requests()[2].system.clone().expect("system");
        assert!(next_system.contains("New address"), "{next_system}");
        assert!(
            !next_system.contains("Old address"),
            "the retired record left the index: {next_system}"
        );
    }

    #[tokio::test]
    async fn a_write_reply_is_events_not_writes() {
        let reply = MemoryWrite
            .execute(
                WRITE_ARGS.to_owned(),
                TurnContext {
                    session_id: "s-live".to_owned(),
                    turn_id: "t-1".to_owned(),
                    grants: None,
                },
            )
            .await;

        assert!(reply.ok);
        assert!(
            reply.content.starts_with("Saved (id: mr-"),
            "{}",
            reply.content
        );
        assert_eq!(reply.memory_events.len(), 1);
        let memory_event::Event::RecordCreated(created) = &reply.memory_events[0] else {
            panic!("expected RecordCreated");
        };
        let record = created.record.as_ref().expect("record");
        let provenance = record.provenance.as_ref().expect("provenance");
        assert_eq!(provenance.entries[0].session_id, "s-live");
    }

    #[tokio::test]
    async fn a_write_with_an_unknown_kind_or_blank_field_is_an_error() {
        let reply = MemoryWrite
            .execute(
                r#"{"kind":"vibe","title":"t","summary":"s","body":"b"}"#.to_owned(),
                TurnContext::default(),
            )
            .await;
        assert!(!reply.ok);
        assert!(reply.memory_events.is_empty());
        assert!(reply.content.contains("preference"), "{}", reply.content);

        let reply = MemoryWrite
            .execute(
                r#"{"kind":"fact","title":"t","summary":"  ","body":"b"}"#.to_owned(),
                TurnContext::default(),
            )
            .await;
        assert!(!reply.ok);
        assert!(reply.content.contains("summary"), "{}", reply.content);
    }

    #[tokio::test]
    async fn superseding_an_unknown_id_is_an_error_naming_it() {
        let dir = TempDir::new().expect("temp dir");
        seed_memory_log(&dir, vec![seeded("mr-real", "Real", "exists")]);
        let tool = MemorySupersede::new(archive_at(&dir));

        let reply = tool
            .execute(
                r#"{"id":"mr-ghost","kind":"fact","title":"t","summary":"s","body":"b"}"#
                    .to_owned(),
                TurnContext::default(),
            )
            .await;

        assert!(!reply.ok);
        assert!(reply.memory_events.is_empty(), "no event for a bad target");
        assert!(reply.content.contains("mr-ghost"), "{}", reply.content);
    }

    #[tokio::test]
    async fn memory_search_answers_compact_rows_and_honors_the_namespace() {
        let dir = TempDir::new().expect("temp dir");
        seed_memory_log(
            &dir,
            vec![seeded("mr-pal", "Gruvbox", "the palette everywhere")],
        );
        let tool = MemorySearch::new(archive_at(&dir));

        let reply = tool
            .execute(r#"{"query":"palette"}"#.to_owned(), TurnContext::default())
            .await;
        assert!(reply.ok);
        assert!(reply.content.contains("mr-pal"), "{}", reply.content);
        assert!(reply.content.contains("Gruvbox"), "{}", reply.content);
        assert!(
            !reply.content.contains("in full"),
            "bodies stay out of search results: {}",
            reply.content
        );

        let reply = tool
            .execute(
                r#"{"query":"palette","namespace":"arc"}"#.to_owned(),
                TurnContext::default(),
            )
            .await;
        assert!(reply.ok);
        assert!(
            reply.content.starts_with("No memory records match"),
            "the empty reply must point at sessions_search, got: {}",
            reply.content
        );
    }

    #[tokio::test]
    async fn memory_read_of_an_unknown_id_names_it() {
        let dir = TempDir::new().expect("temp dir");
        seed_memory_log(&dir, vec![seeded("mr-real", "Real", "exists")]);
        let tool = MemoryRead::new(archive_at(&dir));

        let reply = tool
            .execute(r#"{"id":"mr-nope"}"#.to_owned(), TurnContext::default())
            .await;

        assert!(!reply.ok);
        assert!(reply.content.contains("mr-nope"), "{}", reply.content);
        assert!(reply.content.contains("memory_search"), "{}", reply.content);
    }

    #[tokio::test]
    async fn malformed_arguments_are_actionable_errors() {
        let dir = TempDir::new().expect("temp dir");
        seed_memory_log(&dir, vec![seeded("mr-real", "Real", "exists")]);

        let reply = MemoryWrite
            .execute(r#"{"kind""#.to_owned(), TurnContext::default())
            .await;
        assert!(!reply.ok);
        assert!(reply.content.contains("memory_write"), "{}", reply.content);

        let reply = MemoryRead::new(archive_at(&dir))
            .execute("{}".to_owned(), TurnContext::default())
            .await;
        assert!(!reply.ok);
        assert!(reply.content.contains("memory_read"), "{}", reply.content);
    }
}
