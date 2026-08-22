use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use serde::Deserialize;

use super::{Tool, ToolReply, TurnContext, to_json};
use crate::archive::{Archive, Error};
use crate::provider::ToolDefinition;

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
}

impl Tool for SessionsSearch {
    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: "sessions_search".to_owned(),
            description: "Search the archive of all past conversations. Use whenever the user \
                          asks about themselves, their preferences, or anything discussed in \
                          an earlier session — search before saying you do not know. Returns \
                          sessions with snippets and an anchor_seq for session_read."
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
                    }
                },
                "required": ["query"]
            }),
        }
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
                         Pass {{\"query\": \"words to find\"}}."
                    ));
                }
            };
            let exclude = (!ctx.session_id.is_empty()).then_some(ctx.session_id.as_str());
            match self
                .archive
                .search(&args.query, args.include_tool_results, exclude)
            {
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
    ends: bool,
}

impl Tool for SessionRead {
    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: "session_read".to_owned(),
            description: "Pull exact past context from one session by seq range \
                          (anchor_seq comes from sessions_search), or its opening \
                          and closing messages with ends."
                .to_owned(),
            parameters: serde_json::json!({
                "type": "object",
                "properties": {
                    "session_id": {"type": "string"},
                    "start_seq": {"type": "integer", "description": "First seq to read."},
                    "end_seq": {"type": "integer", "description": "Last seq to read."},
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
                .read_range(&args.session_id, start_seq, end_seq)
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
        MessageAppended, Role, SessionCreated, ToolOutcome, ToolResultRecorded, session_event,
    };
    use tempfile::TempDir;

    use super::{SessionRead, SessionsSearch};
    use crate::testkit::{
        ScriptedProvider, archive_at, call, channel, done_reply, engine_with_tools_at, replay_log,
        seed_log, tool_stop,
    };
    use crate::tool::{Registry, Tool as _, TurnContext};

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

    fn search_registry(dir: &TempDir) -> Registry {
        let mut registry = Registry::new(32 * 1024);
        registry.register(Box::new(SessionsSearch::new(archive_at(dir))));
        registry
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
        let mut engine = engine_with_tools_at(&provider, &dir, registry);
        let (tx, _rx) = channel();

        engine
            .send_message(None, "what did we say about colors?", tx)
            .await
            .expect("send");

        let results = logged_results(&dir);
        assert_eq!(results.len(), 1);
        let content = &results[0].content;
        assert_eq!(results[0].outcome, ToolOutcome::Ok as i32);
        assert!(content.contains("s-colors"), "{content}");
        assert!(content.contains("gruvbox"), "{content}");
        assert!(content.contains("anchor_seq"), "{content}");
        assert!(!content.contains("s-deploy"), "{content}");
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
        let mut engine = engine_with_tools_at(&provider, &dir, registry);
        let (tx, _rx) = channel();

        let reply = engine
            .send_message(None, "gruvbox gruvbox gruvbox — what did we say?", tx)
            .await
            .expect("send");

        let results = logged_results(&dir);
        assert_eq!(results.len(), 1);
        let content = &results[0].content;
        assert!(content.contains("s-colors"), "{content}");
        assert!(
            !content.contains(&reply.session_id),
            "the current session leaked into its own search: {content}"
        );
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
        let mut engine = engine_with_tools_at(&provider, &dir, registry);
        let (tx, _rx) = channel();

        engine
            .send_message(None, "question", tx)
            .await
            .expect("send");

        let results = logged_results(&dir);
        assert_eq!(results.len(), 3);
        let results = &results[1..];
        assert_eq!(results[0].content, "No results.");
        assert_eq!(results[0].outcome, ToolOutcome::Ok as i32);
        assert!(
            results[1].content.contains("s-tools"),
            "{}",
            results[1].content
        );
        assert!(
            results[1].content.contains("vulkanpin"),
            "{}",
            results[1].content
        );
    }

    #[tokio::test]
    async fn a_fixture_query_survives_the_live_path() {
        let dir = TempDir::new().expect("temp dir");
        seed_log(
            &dir,
            vec![
                created("s-todo", ""),
                said("s-todo", Role::User, "TODO: fix the resume race"),
            ],
        );
        let provider = ScriptedProvider::scripted(vec![
            vec![
                Ok(call("c1", 0, "sessions_search", r#"{"query":"TODO: fix"}"#)),
                Ok(tool_stop()),
            ],
            done_reply("noted"),
        ]);
        let registry = search_registry(&dir);
        let mut engine = engine_with_tools_at(&provider, &dir, registry);
        let (tx, _rx) = channel();

        engine
            .send_message(None, "question", tx)
            .await
            .expect("send");

        let results = logged_results(&dir);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].outcome, ToolOutcome::Ok as i32);
        assert!(
            results[0].content.contains("s-todo"),
            "{}",
            results[0].content
        );
    }

    fn seeded_dir() -> TempDir {
        let dir = TempDir::new().expect("temp dir");
        seed_log(
            &dir,
            vec![
                created("s-01", ""),
                said("s-01", Role::User, "the goal"),
                said("s-01", Role::Assistant, "the resolution"),
            ],
        );
        dir
    }

    #[tokio::test]
    async fn malformed_search_arguments_are_an_actionable_error() {
        let dir = seeded_dir();
        let tool = SessionsSearch::new(archive_at(&dir));

        let reply = tool
            .execute(r#"{"query""#.to_owned(), TurnContext::default())
            .await;

        assert!(!reply.ok);
        assert!(
            reply.content.contains("sessions_search"),
            "{}",
            reply.content
        );
        assert!(reply.content.contains("query"), "{}", reply.content);
    }

    #[tokio::test]
    async fn an_unsearchable_query_is_a_no_results_answer_naming_the_problem() {
        let dir = seeded_dir();
        let tool = SessionsSearch::new(archive_at(&dir));

        let reply = tool
            .execute(r#"{"query":"%%% ---"}"#.to_owned(), TurnContext::default())
            .await;

        assert!(reply.ok, "an unsearchable query is not a tool error");
        assert!(
            reply.content.starts_with("No results:"),
            "{}",
            reply.content
        );
    }

    #[tokio::test]
    async fn session_read_returns_the_range_as_json() {
        let dir = seeded_dir();
        let tool = SessionRead::new(archive_at(&dir));

        let reply = tool
            .execute(
                r#"{"session_id":"s-01","start_seq":1,"end_seq":2}"#.to_owned(),
                TurnContext::default(),
            )
            .await;

        assert!(reply.ok);
        assert!(reply.content.contains("the goal"), "{}", reply.content);
        assert!(
            reply.content.contains("the resolution"),
            "{}",
            reply.content
        );
        assert!(
            reply.content.contains("\"role\":\"assistant\""),
            "{}",
            reply.content
        );
    }

    #[tokio::test]
    async fn session_read_ends_returns_the_bookends_shape() {
        let dir = seeded_dir();
        let tool = SessionRead::new(archive_at(&dir));

        let reply = tool
            .execute(
                r#"{"session_id":"s-01","ends":true}"#.to_owned(),
                TurnContext::default(),
            )
            .await;

        assert!(reply.ok);
        assert!(reply.content.contains("\"first\""), "{}", reply.content);
        assert!(reply.content.contains("\"last\""), "{}", reply.content);
        assert!(reply.content.contains("the goal"), "{}", reply.content);
    }

    #[tokio::test]
    async fn session_read_refuses_a_whole_session_dump() {
        let dir = seeded_dir();
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

    #[tokio::test]
    async fn session_read_rejects_an_inverted_range() {
        let dir = seeded_dir();
        let tool = SessionRead::new(archive_at(&dir));

        let reply = tool
            .execute(
                r#"{"session_id":"s-01","start_seq":9,"end_seq":1}"#.to_owned(),
                TurnContext::default(),
            )
            .await;

        assert!(!reply.ok);
        assert!(reply.content.contains("start_seq 9"), "{}", reply.content);
    }

    #[tokio::test]
    async fn session_read_names_an_unknown_session() {
        let dir = seeded_dir();
        let tool = SessionRead::new(archive_at(&dir));

        let reply = tool
            .execute(
                r#"{"session_id":"s-none","start_seq":0,"end_seq":5}"#.to_owned(),
                TurnContext::default(),
            )
            .await;

        assert!(!reply.ok);
        assert!(reply.content.contains("s-none"), "{}", reply.content);
        assert!(
            reply.content.contains("sessions_search"),
            "{}",
            reply.content
        );
    }

    #[tokio::test]
    async fn an_empty_range_in_a_real_session_is_not_an_error() {
        let dir = seeded_dir();
        let tool = SessionRead::new(archive_at(&dir));

        let reply = tool
            .execute(
                r#"{"session_id":"s-01","start_seq":500,"end_seq":600}"#.to_owned(),
                TurnContext::default(),
            )
            .await;

        assert!(reply.ok);
        assert!(reply.content.contains("No messages"), "{}", reply.content);
    }
}
