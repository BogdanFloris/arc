use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use serde::Deserialize;

use crate::archive::{Archive, Error, SearchOptions, SearchOrder};
use crate::provider::ToolDefinition;
use crate::tool::{Tool, ToolReply, ToolSource, TurnContext, to_json};

pub struct SessionsSearch {
    archive: Arc<Archive>,
}

impl SessionsSearch {
    pub fn new(archive: Arc<Archive>) -> Self {
        Self { archive }
    }
}

#[derive(Deserialize)]
struct SearchArgs {
    query: String,
    #[serde(default)]
    include_tool_results: bool,
    after: Option<String>,
    before: Option<String>,
    #[serde(default)]
    sort: SearchOrder,
}

impl Tool for SessionsSearch {
    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: "sessions_search".to_owned(),
            description: "Search past conversations for missing context before saying you do \
                          not know. Returns snippets and an anchor_seq for session_read; read \
                          around the anchor only if the snippets are insufficient. Each hit \
                          reports the matching message's role (user, assistant, tool_result, \
                          or server_call), timestamp, and snippet — historical evidence, not \
                          current fact. after and before filter that timestamp, not the \
                          session's start; context and bookends can fall outside the bounds. \
                          A null timestamp means the time is unknown: such a message matches \
                          only unbounded searches and sorts last under newest."
                .to_owned(),
            parameters: serde_json::json!({
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "Words or \"quoted phrases\" to find."
                    },
                    "include_tool_results": {
                        "type": "boolean",
                        "description": "Also search tool output. Default false."
                    },
                    "after": {
                        "type": "string",
                        "description": "Only matching messages at or after this time \
                            (inclusive). RFC 3339 datetime or a date meaning midnight UTC. \
                            Compares the matched message's timestamp, not the session's \
                            start; null-timestamped messages are excluded, and surrounding \
                            context can still fall outside the bounds."
                    },
                    "before": {
                        "type": "string",
                        "description": "Only matching messages strictly before this time. \
                            RFC 3339 datetime or a date meaning midnight UTC. Compares the \
                            matched message's timestamp, not the session's start; \
                            null-timestamped messages are excluded, and surrounding context \
                            can still fall outside the bounds."
                    },
                    "sort": {
                        "type": "string",
                        "enum": ["relevance", "newest"],
                        "default": "relevance",
                        "description": "relevance ranks by text match; newest orders \
                            matching messages by timestamp, so later updates come first and \
                            null-timestamped messages last."
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
        ctx: TurnContext,
    ) -> Pin<Box<dyn Future<Output = ToolReply> + Send + '_>> {
        Box::pin(async move {
            let args: SearchArgs = match serde_json::from_str(&arguments_json) {
                Ok(args) => args,
                Err(error) => {
                    return ToolReply::error(format!(
                        "ERROR: bad sessions_search arguments ({error}). \
                         Pass {{\"query\": \"words to find\"}}; sort is \"relevance\" \
                         or \"newest\"; after and before are RFC 3339 datetimes or \
                         YYYY-MM-DD dates meaning midnight UTC."
                    ));
                }
            };
            let (after, before) = match parse_bounds(&args) {
                Ok(bounds) => bounds,
                Err(message) => return ToolReply::error(format!("ERROR: {message}")),
            };
            let exclude = (!ctx.session_id.is_empty()).then_some(ctx.session_id.as_str());
            match self.archive.search(
                &args.query,
                args.include_tool_results,
                exclude,
                SearchOptions {
                    after,
                    before,
                    sort: args.sort,
                },
            ) {
                Ok(reply) if reply.sessions.is_empty() => ToolReply::ok("No results.".to_owned()),
                Ok(reply) => ToolReply::ok(to_json(&reply)),
                Err(Error::Query { message }) => ToolReply::ok(format!(
                    "No results: {message}. Try plain words or \"quoted phrases\"."
                )),
                Err(error) => ToolReply::error(format!("ERROR: session search failed ({error}).")),
            }
        })
    }
}

pub struct SessionRead {
    archive: Arc<Archive>,
}

impl SessionRead {
    pub fn new(archive: Arc<Archive>) -> Self {
        Self { archive }
    }
}

#[derive(Deserialize)]
struct ReadArgs {
    session_id: String,
    start_seq: Option<i64>,
    end_seq: Option<i64>,
    #[serde(default)]
    start_offset: usize,
    #[serde(default)]
    ends: bool,
}

impl Tool for SessionRead {
    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: "session_read".to_owned(),
            description: "Read a narrow range of past messages, or opening/closing previews \
                          with ends. Every message carries its role and timestamp; ts is \
                          null when the log has no timestamp for it, which means the time is \
                          unknown, not the session's start. Pages contain at most 8 KiB of \
                          JSON and 20 messages; long messages span pages. To continue, use \
                          the next cursor's start_seq and start_offset with the same end_seq. \
                          Fetch another page only if needed."
                .to_owned(),
            parameters: serde_json::json!({
                "type": "object",
                "properties": {
                    "session_id": {"type": "string"},
                    "start_seq": {"type": "integer", "description": "First seq to read."},
                    "end_seq": {"type": "integer", "description": "Last seq to read."},
                    "start_offset": {
                        "type": "integer",
                        "minimum": 0,
                        "description": "UTF-8 byte offset within start_seq. Default 0; use the previous page's next cursor."
                    },
                    "ends": {
                        "type": "boolean",
                        "description": "Return the session's first and last messages \
                                        instead of a range."
                    }
                },
                "required": ["session_id"]
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
                        "ERROR: bad session_read arguments ({error}). Pass \
                         {{\"session_id\": \"...\", \"start_seq\": N, \"end_seq\": N}} \
                         or {{\"session_id\": \"...\", \"ends\": true}}."
                    ));
                }
            };

            if args.ends {
                if args.start_seq.is_some() || args.end_seq.is_some() || args.start_offset != 0 {
                    return ToolReply::error(
                        "ERROR: ends cannot be combined with a range or start_offset.".to_owned(),
                    );
                }
                return match self.archive.ends(&args.session_id) {
                    Ok(Some(reply)) => ToolReply::ok(to_json(&reply)),
                    Ok(None) => unknown_session(&args.session_id),
                    Err(error) => read_failed(&error),
                };
            }

            let (Some(start_seq), Some(end_seq)) = (args.start_seq, args.end_seq) else {
                return ToolReply::error(
                    "ERROR: pass both start_seq and end_seq, or ends: true. \
                     Whole sessions are not readable in one call."
                        .to_owned(),
                );
            };
            if start_seq > end_seq {
                return ToolReply::error(format!(
                    "ERROR: start_seq {start_seq} is after end_seq {end_seq}."
                ));
            }
            match self
                .archive
                .read_range(&args.session_id, start_seq, end_seq, args.start_offset)
            {
                Ok(Some(reply)) if reply.messages.is_empty() => ToolReply::ok(format!(
                    "No messages between seq {start_seq} and {end_seq} in session {}.",
                    args.session_id
                )),
                Ok(Some(reply)) => ToolReply::ok(to_json(&reply)),
                Ok(None) => unknown_session(&args.session_id),
                Err(error) => read_failed(&error),
            }
        })
    }
}

fn parse_bounds(args: &SearchArgs) -> Result<(Option<i64>, Option<i64>), String> {
    let after = parse_bound(args.after.as_deref(), "after")?;
    let before = parse_bound(args.before.as_deref(), "before")?;
    if matches!((after, before), (Some(after), Some(before)) if after >= before) {
        return Err(
            "after must be earlier than before; after is inclusive, before exclusive.".to_owned(),
        );
    }
    Ok((after, before))
}

fn parse_bound(raw: Option<&str>, name: &str) -> Result<Option<i64>, String> {
    let Some(raw) = raw else {
        return Ok(None);
    };
    if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(raw) {
        return Ok(Some(dt.timestamp_micros()));
    }
    if let Ok(date) = chrono::NaiveDate::parse_from_str(raw, "%Y-%m-%d") {
        let midnight = date
            .and_hms_micro_opt(0, 0, 0, 0)
            .expect("a fixed time of day is always valid");
        return Ok(Some(midnight.and_utc().timestamp_micros()));
    }
    Err(format!(
        "{name} {raw:?} is not a recognized time — use an RFC 3339 datetime \
         (e.g. 2026-03-15T14:00:00Z) or a bare date (e.g. 2026-03-15) meaning midnight UTC."
    ))
}

fn unknown_session(session_id: &str) -> ToolReply {
    ToolReply::error(format!(
        "ERROR: no session {session_id}. Session ids come from sessions_search."
    ))
}

fn read_failed(error: &Error) -> ToolReply {
    ToolReply::error(format!("ERROR: session read failed ({error})."))
}

#[cfg(test)]
mod tests {
    use arc_proto::v1::{
        MessageAppended, Role, SessionCreated, SessionRole, ToolOutcome, ToolResultRecorded,
        session_event,
    };
    use tempfile::TempDir;

    use super::{SessionRead, SessionsSearch};
    use crate::testkit::{
        ScriptedProvider, archive_at, call, channel, done_reply, engine_with_tools_at, replay_log,
        seed_log, seed_log_each, tool_stop,
    };
    use crate::tool::{Registry, Tool as _, ToolReply, TurnContext};

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

    fn search_registry(dir: &TempDir) -> Registry {
        let mut registry = Registry::new(32 * 1024);
        registry.register(Box::new(SessionsSearch::new(archive_at(dir))));
        registry
    }

    fn search_tool(dir: &TempDir) -> SessionsSearch {
        SessionsSearch::new(archive_at(dir))
    }

    fn logged_results(dir: &TempDir) -> Vec<ToolResultRecorded> {
        replay_log(dir.path())
            .into_iter()
            .filter_map(|event| match event {
                session_event::Event::ToolResultRecorded(result) => Some(result),
                _ => None,
            })
            .collect()
    }

    fn at(rfc3339: &str) -> i64 {
        let ts = chrono::DateTime::parse_from_rfc3339(rfc3339).expect("test timestamp");
        ts.timestamp_micros()
    }

    fn cans(dir: &TempDir) {
        seed_log_each(
            dir,
            [
                (created("s-cans", ""), Some(at("2026-01-05T09:00:00Z"))),
                (
                    said("s-cans", Role::User, "the cans arrived"),
                    Some(at("2026-01-05T10:00:00.123456Z")),
                ),
            ],
        );
    }

    fn printer_history(dir: &TempDir) {
        seed_log_each(
            dir,
            [
                (
                    created("s-first", "printer, first pass"),
                    Some(at("2026-01-05T09:00:00Z")),
                ),
                (
                    said("s-first", Role::User, "the printer"),
                    Some(at("2026-01-05T10:00:00Z")),
                ),
                (
                    created("s-later", "printer, second pass"),
                    Some(at("2026-01-06T09:00:00Z")),
                ),
                (
                    said("s-later", Role::User, "printer: prusa core one"),
                    Some(at("2026-02-10T10:00:00Z")),
                ),
            ],
        );
    }

    fn parsed(content: &str) -> serde_json::Value {
        serde_json::from_str(content).unwrap_or_else(|error| panic!("{error}: {content}"))
    }

    fn hits(content: &str) -> Vec<serde_json::Value> {
        if content == "No results." {
            return Vec::new();
        }
        let reply = parsed(content);
        reply["sessions"].as_array().expect("sessions").clone()
    }

    fn ids(found: &[serde_json::Value]) -> Vec<&str> {
        found
            .iter()
            .map(|hit| hit["session_id"].as_str().expect("session id"))
            .collect()
    }

    async fn raw(tool: &SessionsSearch, args: &str) -> ToolReply {
        tool.execute(args.to_owned(), TurnContext::default()).await
    }

    async fn search(tool: &SessionsSearch, args: &str) -> Vec<serde_json::Value> {
        let reply = raw(tool, args).await;
        assert!(reply.ok, "{args}: {}", reply.content);
        hits(&reply.content)
    }

    #[tokio::test]
    async fn a_live_search_turn_answers_from_the_snippet() {
        let dir = TempDir::new().expect("temp dir");
        seed_log(
            &dir,
            vec![
                created("s-colors", "tui colors"),
                said("s-colors", Role::User, "what palette does the tui use"),
                said(
                    "s-colors",
                    Role::Assistant,
                    "gruvbox via the terminal palette, orange accent",
                ),
                created("s-deploy", ""),
                said("s-deploy", Role::User, "how do we deploy arcd"),
                said("s-deploy", Role::Assistant, "nixos rebuild on erebor"),
            ],
        );
        let provider = ScriptedProvider::scripted(vec![
            vec![
                Ok(call("c1", 0, "sessions_search", r#"{"query":"gruvbox"}"#)),
                Ok(tool_stop()),
            ],
            done_reply("we said gruvbox with an orange accent"),
        ]);
        let registry = search_registry(&dir);
        let (engine, run) = engine_with_tools_at(&provider, &dir, registry);
        let (tx, _rx) = channel();

        engine
            .send_message(&run, None, "what did we say about colors?", tx)
            .await
            .expect("send");

        let results = logged_results(&dir);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].outcome, ToolOutcome::Ok as i32);
        let found = hits(&results[0].content);
        assert_eq!(
            ids(&found),
            ["s-colors"],
            "only the gruvbox session matches"
        );
        let snippet = found[0]["snippet"].as_str().expect("a snippet");
        assert!(snippet.contains("gruvbox"), "the snippet answers the turn");
        let anchor = found[0]["anchor_seq"].as_i64();
        assert!(
            anchor.is_some_and(|seq| seq > 0),
            "hits carry an anchor_seq"
        );
    }

    #[tokio::test]
    async fn a_live_search_never_returns_the_searching_session() {
        let dir = TempDir::new().expect("temp dir");
        seed_log(
            &dir,
            vec![
                created("s-colors", ""),
                said("s-colors", Role::User, "gruvbox everywhere please"),
            ],
        );
        let provider = ScriptedProvider::scripted(vec![
            vec![
                Ok(call("c1", 0, "sessions_search", r#"{"query":"gruvbox"}"#)),
                Ok(tool_stop()),
            ],
            done_reply("answered from history"),
        ]);
        let registry = search_registry(&dir);
        let (engine, run) = engine_with_tools_at(&provider, &dir, registry);
        let (tx, _rx) = channel();

        let reply = engine
            .send_message(&run, None, "gruvbox gruvbox gruvbox — what did we say?", tx)
            .await
            .expect("send");

        let results = logged_results(&dir);
        assert_eq!(results.len(), 1);
        let found = hits(&results[0].content);
        assert_eq!(ids(&found), ["s-colors"]);
        let leaked = found.iter().any(|id| *id == reply.session_id.as_str());
        assert!(!leaked, "the searching session appeared in its own results");
    }

    #[tokio::test]
    async fn the_default_filter_hides_tool_output_until_lifted() {
        let dir = TempDir::new().expect("temp dir");
        seed_log(
            &dir,
            vec![
                created("s-tools", ""),
                said("s-tools", Role::User, "run the device listing"),
                tool_answered("s-tools", "vulkanpin gpu listing output"),
            ],
        );
        let provider = ScriptedProvider::scripted(vec![
            vec![
                Ok(call("c1", 0, "sessions_search", r#"{"query":"vulkanpin"}"#)),
                Ok(tool_stop()),
            ],
            vec![
                Ok(call(
                    "c2",
                    0,
                    "sessions_search",
                    r#"{"query":"vulkanpin","include_tool_results":true}"#,
                )),
                Ok(tool_stop()),
            ],
            done_reply("found it in the tool output"),
        ]);
        let registry = search_registry(&dir);
        let (engine, run) = engine_with_tools_at(&provider, &dir, registry);
        let (tx, _rx) = channel();

        engine
            .send_message(&run, None, "question", tx)
            .await
            .expect("send");

        let results = logged_results(&dir);
        assert_eq!(results.len(), 3);
        let results = &results[1..];
        assert_eq!(results[0].outcome, ToolOutcome::Ok as i32);
        assert!(
            hits(&results[0].content).is_empty(),
            "tool output stays hidden"
        );
        let found = hits(&results[1].content);
        assert_eq!(
            ids(&found),
            ["s-tools"],
            "the lifted filter finds the tool output"
        );
        let snippet = found[0]["snippet"].as_str().expect("a snippet");
        assert!(
            snippet.contains("vulkanpin"),
            "the hit carries the tool output"
        );
    }

    #[tokio::test]
    async fn session_read_pages_a_long_message_through_the_registry_without_loss() {
        let dir = TempDir::new().expect("temp dir");
        let original = "exact text: 🦀é漢字\n\"\\\u{0000}".repeat(2000);
        seed_log(
            &dir,
            vec![created("s-01", ""), said("s-01", Role::User, &original)],
        );
        let mut registry = Registry::new(8 * 1024);
        registry.register(Box::new(SessionRead::new(archive_at(&dir))));
        let mut args = serde_json::json!({
            "session_id": "s-01",
            "start_seq": 0,
            "end_seq": i64::MAX,
        });
        let mut restored = String::new();
        let mut pages = 0;
        loop {
            let reply = registry
                .dispatch(
                    "session_read",
                    args.to_string(),
                    TurnContext::default(),
                    &[crate::tool::ToolSource::Builtin],
                )
                .await;
            assert!(reply.ok, "{}", reply.content);
            assert!(!reply.truncated);
            assert!(reply.content.len() <= 8 * 1024);
            let page = parsed(&reply.content);
            let messages = page["messages"].as_array().expect("messages");
            assert_eq!(messages.len(), 1);
            assert!(
                messages[0]["ts"].is_null(),
                "an unrecorded message time reads as null: {}",
                messages[0]
            );
            let fragment = messages[0]["content"].as_str().expect("fragment");
            assert!(!fragment.is_empty(), "every page makes progress");
            restored.push_str(fragment);
            pages += 1;
            assert!(pages < 100, "pagination terminates");
            if page["next"].is_null() {
                assert!(page["clipped"].is_null());
                break;
            }
            assert_eq!(page["clipped"], true);
            assert_eq!(
                usize::try_from(page["next"]["start_offset"].as_u64().expect("offset"))
                    .expect("small offset"),
                restored.len()
            );
            args["start_seq"] = page["next"]["start_seq"].clone();
            args["start_offset"] = page["next"]["start_offset"].clone();
        }
        assert!(pages > 1);
        assert_eq!(restored, original);
    }

    #[tokio::test]
    async fn session_read_ends_returns_the_bookends_shape() {
        let dir = TempDir::new().expect("temp dir");
        seed_log(
            &dir,
            vec![
                created("s-01", ""),
                said("s-01", Role::User, "the goal"),
                said("s-01", Role::Assistant, "the resolution"),
            ],
        );
        let tool = SessionRead::new(archive_at(&dir));

        let reply = tool
            .execute(
                r#"{"session_id":"s-01","ends":true}"#.to_owned(),
                TurnContext::default(),
            )
            .await;

        assert!(reply.ok, "{}", reply.content);
        let page = parsed(&reply.content);
        let first = page["first"].as_array().expect("first bookend");
        assert_eq!(first[0]["content"], "the goal", "{}", reply.content);
        assert_eq!(first[1]["content"], "the resolution");
        assert_eq!(page["last"], serde_json::json!([]));
    }

    #[tokio::test]
    async fn date_bounds_select_the_matching_message_not_the_session() {
        let dir = TempDir::new().expect("temp dir");
        printer_history(&dir);
        seed_log_each(
            &dir,
            [
                (
                    created("s-created-late", ""),
                    Some(at("2026-03-01T09:00:00Z")),
                ),
                (
                    said("s-created-late", Role::User, "an early printer note"),
                    Some(at("2026-01-20T10:00:00Z")),
                ),
            ],
        );
        let tool = search_tool(&dir);

        let bounded = search(&tool, r#"{"query":"printer","after":"2026-02-01"}"#).await;
        assert_eq!(
            ids(&bounded),
            ["s-later"],
            "the message date decides the match"
        );

        let before = search(&tool, r#"{"query":"printer","before":"2026-02-01"}"#).await;
        let mut earlier = ids(&before);
        earlier.sort_unstable();
        assert_eq!(
            earlier,
            ["s-created-late", "s-first"],
            "both early notes match"
        );
    }

    #[tokio::test]
    async fn utc_offsets_and_bare_dates_bound_the_same_instant() {
        let dir = TempDir::new().expect("temp dir");
        cans(&dir);
        let tool = search_tool(&dir);

        let found = search(&tool, r#"{"query":"cans"}"#).await;
        assert_eq!(
            found[0]["ts"], "2026-01-05T10:00:00.123456Z",
            "a hit keeps the message's exact time"
        );

        for (args, matched) in [
            (
                r#"{"query":"cans","after":"2026-01-05T11:30:00+02:00"}"#,
                true,
            ),
            (
                r#"{"query":"cans","after":"2026-01-05T13:00:00+02:00"}"#,
                false,
            ),
            (r#"{"query":"cans","after":"2026-01-05"}"#, true),
            (r#"{"query":"cans","before":"2026-01-06"}"#, true),
            (r#"{"query":"cans","before":"2026-01-05"}"#, false),
            (
                r#"{"query":"cans","after":"2026-01-05T10:00:00.123456Z"}"#,
                true,
            ),
            (
                r#"{"query":"cans","before":"2026-01-05T10:00:00.123456Z"}"#,
                false,
            ),
        ] {
            let found = search(&tool, args).await;
            assert_eq!(!found.is_empty(), matched, "{args}");
        }
    }

    #[tokio::test]
    async fn invalid_dates_bounds_and_sorts_are_tool_errors() {
        let dir = TempDir::new().expect("temp dir");
        let tool = search_tool(&dir);

        for (args, expected) in [
            (r#"{"query":"cans","after":"last tuesday"}"#, "after"),
            (r#"{"query":"cans","before":"2026-02-30"}"#, "before"),
            (
                r#"{"query":"cans","after":"2026-02-01","before":"2026-02-01"}"#,
                "earlier",
            ),
            (
                r#"{"query":"cans","after":"2026-03-01","before":"2026-02-01"}"#,
                "earlier",
            ),
            (r#"{"query":"cans","sort":"oldest"}"#, "sort"),
        ] {
            let reply = raw(&tool, args).await;
            assert!(!reply.ok, "{args}: {}", reply.content);
            assert!(reply.content.contains(expected), "{args}");
        }
    }

    #[tokio::test]
    async fn undated_messages_search_unbounded_but_never_satisfy_bounds() {
        let dir = TempDir::new().expect("temp dir");
        cans(&dir);
        seed_log_each(
            &dir,
            [
                (created("s-undated", ""), None),
                (said("s-undated", Role::User, "an undated cans note"), None),
            ],
        );
        let tool = search_tool(&dir);

        let unbounded = search(&tool, r#"{"query":"cans","include_tool_results":false}"#).await;
        assert_eq!(unbounded.len(), 2, "undated messages match without bounds");
        let undated = unbounded
            .iter()
            .find(|hit| hit["session_id"] == "s-undated")
            .expect("the undated session is still searchable");
        assert!(undated["ts"].is_null(), "unknown time reads as null");

        let newest = search(&tool, r#"{"query":"cans","sort":"newest"}"#).await;
        assert_eq!(
            ids(&newest),
            ["s-cans", "s-undated"],
            "the null time sorts last"
        );

        let bounded = search(&tool, r#"{"query":"cans","after":"2020-01-01"}"#).await;
        assert_eq!(
            ids(&bounded),
            ["s-cans"],
            "a null time cannot satisfy after"
        );
    }

    #[tokio::test]
    async fn newest_sort_anchors_the_later_user_decision() {
        let dir = TempDir::new().expect("temp dir");
        seed_log_each(
            &dir,
            [
                (
                    created("s-kit", "printer"),
                    Some(at("2026-01-05T09:00:00Z")),
                ),
                (
                    said(
                        "s-kit",
                        Role::Assistant,
                        "options: the core one kit, the core one enclosure, or waiting",
                    ),
                    Some(at("2026-01-05T10:00:00Z")),
                ),
                (
                    said("s-kit", Role::User, "decision: order the core one kit"),
                    Some(at("2026-03-01T10:00:00Z")),
                ),
            ],
        );
        let tool = search_tool(&dir);

        let suggestion = search(&tool, r#"{"query":"core one"}"#).await;
        let hit = &suggestion[0];
        assert_eq!(hit["anchor_seq"], 1, "relevance anchors the suggestion");
        assert_eq!(hit["role"], "assistant");
        assert_eq!(hit["ts"], "2026-01-05T10:00:00Z");

        let decision = search(&tool, r#"{"query":"core one","sort":"newest"}"#).await;
        let hit = &decision[0];
        assert_eq!(hit["anchor_seq"], 2, "newest anchors the user decision");
        assert_eq!(hit["role"], "user");
        assert_eq!(hit["ts"], "2026-03-01T10:00:00Z");
        let snippet = hit["snippet"].as_str().expect("a snippet");
        assert!(snippet.contains("order the core one kit"), "{hit}");
    }

    #[tokio::test]
    async fn a_live_turn_forwards_bounds_and_sort_to_the_archive() {
        let dir = TempDir::new().expect("temp dir");
        printer_history(&dir);
        let provider = ScriptedProvider::scripted(vec![
            vec![
                Ok(call(
                    "c1",
                    0,
                    "sessions_search",
                    r#"{"query":"printer","after":"2026-02-01","sort":"newest"}"#,
                )),
                Ok(tool_stop()),
            ],
            done_reply("the printer is now the prusa core one"),
        ]);
        let registry = search_registry(&dir);
        let (engine, run) = engine_with_tools_at(&provider, &dir, registry);
        let (tx, _rx) = channel();

        let reply = engine
            .send_message(&run, None, "which printer did we settle on?", tx)
            .await
            .expect("send");

        let results = logged_results(&dir);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].outcome, ToolOutcome::Ok as i32);
        let found = hits(&results[0].content);
        assert_eq!(ids(&found), ["s-later"], "the bound drops the first pass");
        assert!(!ids(&found).contains(&reply.session_id.as_str()));
    }

    #[tokio::test]
    async fn session_read_refuses_a_whole_session_dump() {
        let dir = TempDir::new().expect("temp dir");
        let tool = SessionRead::new(archive_at(&dir));

        let reply = tool
            .execute(
                r#"{"session_id":"s-01"}"#.to_owned(),
                TurnContext::default(),
            )
            .await;

        assert!(!reply.ok);
        assert!(reply.content.contains("start_seq"), "{}", reply.content);
        assert!(reply.content.contains("ends"), "{}", reply.content);
    }
}
