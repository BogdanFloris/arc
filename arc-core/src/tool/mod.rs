//! The tool registry: what the model may call, and what running a call yields.
//!
//! [`Registry`] is the seam between the engine loop (4.2) and individual
//! tools. The engine offers [`Registry::definitions`] on each completion and
//! hands each tool call to [`Registry::dispatch`], which returns what the log
//! records: content, an outcome, a truncation flag.
//!
//! Two contract points live here rather than in tools (DESIGN.md §3.1):
//!
//! - **A failing tool is a result, not an error.** Unknown names, bad
//!   arguments, tool-internal failures — all come back as an ERROR outcome
//!   whose content is text the model reads and reasons about. `dispatch`
//!   cannot fail.
//! - **Truncation happens before the event is built.** Results are cut to
//!   `max_tool_result_bytes` — the real constraint is the model's context,
//!   not the log's 16 MiB cap — marked in the content, and flagged.
//!
//! Tools never produce UNKNOWN: that outcome is written only by the startup
//! closer for orphaned calls (4.3).

pub mod memory;
pub mod sessions;
pub mod time;

use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;

use crate::provider::ToolDefinition;

/// The turn a call runs inside, threaded through dispatch so a tool can know
/// where it is being called from — provenance for memory writes, the
/// current-session exclusion for `sessions_search`. Tools that need neither
/// ignore it.
#[derive(Debug, Clone, Default)]
pub struct TurnContext {
    pub session_id: String,
    pub turn_id: String,
}

/// What a tool answered. An error is a reply the model will read, with
/// `ok: false`; it is not a Rust error.
pub struct ToolReply {
    pub content: String,
    pub ok: bool,
    /// Events the engine appends durably before the result that reports them.
    /// Tools never touch the log or projection themselves (invariant 2).
    pub memory_events: Vec<arc_proto::v1::memory_event::Event>,
}

impl ToolReply {
    #[must_use]
    pub fn ok(content: impl Into<String>) -> Self {
        Self {
            content: content.into(),
            ok: true,
            memory_events: Vec::new(),
        }
    }

    #[must_use]
    pub fn error(content: impl Into<String>) -> Self {
        Self {
            content: content.into(),
            ok: false,
            memory_events: Vec::new(),
        }
    }
}

/// What [`Registry::dispatch`] yields: a [`ToolReply`] after the registry's
/// own policy — truncation, unknown-name handling — has been applied. The
/// engine builds `ToolResultRecorded` from this and nothing else.
pub struct DispatchOutcome {
    pub content: String,
    /// `false` → `TOOL_OUTCOME_ERROR`.
    pub ok: bool,
    pub truncated: bool,
    /// Passed through from the reply, untouched by truncation.
    pub memory_events: Vec<arc_proto::v1::memory_event::Event>,
}

/// One callable tool.
///
/// `execute` returns a boxed future because the registry holds tools as trait
/// objects — the first place dyn dispatch is genuinely needed — and an
/// `async fn` in a trait is not dyn-compatible. Implementors parse their own
/// `arguments_json`; a parse failure is an ERROR reply, not a panic.
pub trait Tool: Send + Sync {
    fn definition(&self) -> ToolDefinition;
    fn execute(
        &self,
        arguments_json: String,
        ctx: TurnContext,
    ) -> Pin<Box<dyn Future<Output = ToolReply> + Send + '_>>;
}

/// Serializes a reply shape. The shapes are plain structs, so failure is
/// unreachable; if it happens anyway the model sees an error, not a panic.
pub(crate) fn to_json<T: serde::Serialize>(value: &T) -> String {
    serde_json::to_string(value)
        .unwrap_or_else(|error| format!("ERROR: could not serialize the reply ({error})."))
}

pub struct Registry {
    tools: BTreeMap<String, Box<dyn Tool>>,
    max_tool_result_bytes: usize,
}

impl Registry {
    #[must_use]
    pub fn new(max_tool_result_bytes: usize) -> Self {
        Registry {
            tools: BTreeMap::new(),
            max_tool_result_bytes,
        }
    }

    /// Adds a tool under its definition's name — the one authority for what
    /// the tool is called, since it is the name the model is offered.
    ///
    /// # Panics
    ///
    /// In debug builds, on a duplicate name: registration happens once at
    /// startup, and a silent replacement would hide a wiring bug.
    pub fn register(&mut self, tool: Box<dyn Tool>) {
        let name = tool.definition().name;
        debug_assert!(
            !self.tools.contains_key(&name),
            "registry already contains the tool {name}"
        );
        self.tools.insert(name, tool);
    }

    /// Every registered tool's definition, in name order — `BTreeMap` keeps
    /// the offering stable across runs, so prompts stay reproducible.
    #[must_use]
    pub fn definitions(&self) -> Vec<ToolDefinition> {
        self.tools.values().map(|tool| tool.definition()).collect()
    }

    /// Runs the named tool and applies result policy. Infallible by design:
    /// every failure mode is an ERROR outcome the model can act on.
    #[tracing::instrument(
        name = "tool.dispatch",
        skip_all,
        fields(tool = name, outcome = tracing::field::Empty)
    )]
    pub async fn dispatch(
        &self,
        name: &str,
        arguments_json: String,
        ctx: TurnContext,
    ) -> DispatchOutcome {
        let span = tracing::Span::current();
        let Some(tool) = self.tools.get(name) else {
            span.record("outcome", "unknown-tool");
            return DispatchOutcome {
                content: format!("ERROR: Tool {name} is not available."),
                ok: false,
                truncated: false,
                memory_events: Vec::new(),
            };
        };
        let reply = tool.execute(arguments_json, ctx).await;
        span.record("outcome", if reply.ok { "ok" } else { "error" });
        let (content, truncated) = self.truncate(reply.content);
        DispatchOutcome {
            content,
            ok: reply.ok,
            truncated,
            memory_events: reply.memory_events,
        }
    }

    fn truncate(&self, content: String) -> (String, bool) {
        if content.len() <= self.max_tool_result_bytes {
            return (content, false);
        }
        let mut cut = self.max_tool_result_bytes;
        while !content.is_char_boundary(cut) {
            cut -= 1;
        }
        (format!("{} [truncated]", &content[..cut]), true)
    }
}

#[cfg(test)]
mod tests {
    use super::{DispatchOutcome, Registry, Tool, ToolReply, TurnContext};
    use crate::provider::ToolDefinition;
    use std::future::Future;
    use std::pin::Pin;

    /// A tool scripted by construction: fixed name, fixed reply.
    struct Scripted {
        name: &'static str,
        content: &'static str,
        ok: bool,
    }

    impl Tool for Scripted {
        fn definition(&self) -> ToolDefinition {
            ToolDefinition {
                name: self.name.to_owned(),
                description: String::new(),
                parameters: serde_json::json!({"type": "object", "properties": {}}),
            }
        }

        fn execute(
            &self,
            _arguments_json: String,
            _ctx: TurnContext,
        ) -> Pin<Box<dyn Future<Output = ToolReply> + Send + '_>> {
            let reply = if self.ok {
                ToolReply::ok(self.content)
            } else {
                ToolReply::error(self.content)
            };
            Box::pin(async move { reply })
        }
    }

    /// A tool that replies with the arguments it was handed.
    struct Echo;

    impl Tool for Echo {
        fn definition(&self) -> ToolDefinition {
            ToolDefinition {
                name: "echo".to_owned(),
                description: String::new(),
                parameters: serde_json::json!({"type": "object"}),
            }
        }

        fn execute(
            &self,
            arguments_json: String,
            _ctx: TurnContext,
        ) -> Pin<Box<dyn Future<Output = ToolReply> + Send + '_>> {
            Box::pin(async move { ToolReply::ok(arguments_json) })
        }
    }

    /// A tool that replies with the session id it was dispatched under.
    struct WhereAmI;

    impl Tool for WhereAmI {
        fn definition(&self) -> ToolDefinition {
            ToolDefinition {
                name: "where_am_i".to_owned(),
                description: String::new(),
                parameters: serde_json::json!({"type": "object"}),
            }
        }

        fn execute(
            &self,
            _arguments_json: String,
            ctx: TurnContext,
        ) -> Pin<Box<dyn Future<Output = ToolReply> + Send + '_>> {
            Box::pin(async move { ToolReply::ok(format!("{}/{}", ctx.session_id, ctx.turn_id)) })
        }
    }

    fn registry(tools: Vec<Box<dyn Tool>>) -> Registry {
        let mut registry = Registry::new(32 * 1024);
        for tool in tools {
            registry.register(tool);
        }
        registry
    }

    fn scripted(name: &'static str, content: &'static str, ok: bool) -> Box<dyn Tool> {
        Box::new(Scripted { name, content, ok })
    }

    #[tokio::test]
    async fn dispatch_routes_by_name() {
        let registry = registry(vec![
            scripted("alpha", "from alpha", true),
            scripted("beta", "from beta", true),
        ]);

        let DispatchOutcome { content, ok, .. } = registry
            .dispatch("beta", "{}".into(), TurnContext::default())
            .await;
        assert!(ok);
        assert_eq!(content, "from beta");
    }

    #[tokio::test]
    async fn arguments_reach_the_tool_verbatim() {
        let registry = registry(vec![Box::new(Echo)]);

        let outcome = registry
            .dispatch("echo", r#"{"q":"café"}"#.into(), TurnContext::default())
            .await;
        assert_eq!(outcome.content, r#"{"q":"café"}"#);
    }

    #[tokio::test]
    async fn dispatch_hands_the_turn_context_to_the_tool() {
        let mut registry = Registry::new(32 * 1024);
        registry.register(Box::new(WhereAmI));

        let ctx = TurnContext {
            session_id: "s-77".to_owned(),
            turn_id: "t-42".to_owned(),
        };
        let outcome = registry.dispatch("where_am_i", "{}".into(), ctx).await;
        assert_eq!(outcome.content, "s-77/t-42");
    }

    #[tokio::test]
    async fn an_unknown_tool_is_an_error_result_not_a_failure() {
        let registry = registry(vec![]);

        let outcome = registry
            .dispatch("missing", "{}".into(), TurnContext::default())
            .await;
        assert!(!outcome.ok);
        assert!(!outcome.truncated);
        assert!(outcome.content.contains("missing"), "{}", outcome.content);
    }

    #[tokio::test]
    async fn a_tool_error_keeps_its_outcome_and_text() {
        let registry = registry(vec![scripted("fails", "ERROR: no such record", false)]);

        let outcome = registry
            .dispatch("fails", "{}".into(), TurnContext::default())
            .await;
        assert!(!outcome.ok, "an error reply must not come back OK");
        assert_eq!(outcome.content, "ERROR: no such record");
    }

    #[tokio::test]
    async fn an_oversized_result_is_truncated_and_marked() {
        let mut registry = Registry::new(8);
        registry.register(scripted("big", "0123456789abcdef", true));

        let outcome = registry
            .dispatch("big", "{}".into(), TurnContext::default())
            .await;
        assert!(outcome.truncated);
        assert!(outcome.ok, "truncation is not an error");
        assert_eq!(outcome.content, "01234567 [truncated]");
    }

    #[tokio::test]
    async fn truncation_lands_on_a_char_boundary() {
        // "é" is two bytes; a cap of 3 falls inside the second one.
        let mut registry = Registry::new(3);
        registry.register(scripted("accents", "ééé", true));

        let outcome = registry
            .dispatch("accents", "{}".into(), TurnContext::default())
            .await;
        assert!(outcome.truncated);
        assert_eq!(outcome.content, "é [truncated]");
    }

    #[tokio::test]
    async fn a_result_at_the_cap_is_untouched() {
        let mut registry = Registry::new(4);
        registry.register(scripted("fits", "1234", true));

        let outcome = registry
            .dispatch("fits", "{}".into(), TurnContext::default())
            .await;
        assert!(!outcome.truncated);
        assert_eq!(outcome.content, "1234");
    }

    #[test]
    fn definitions_lists_every_tool_in_name_order() {
        let registry = registry(vec![
            scripted("zeta", "", true),
            scripted("alpha", "", true),
        ]);

        let names: Vec<String> = registry
            .definitions()
            .into_iter()
            .map(|def| def.name)
            .collect();
        assert_eq!(names, ["alpha", "zeta"]);
    }

    #[test]
    #[should_panic(expected = "already contains")]
    fn a_duplicate_registration_panics_in_debug() {
        let mut registry = Registry::new(1024);
        registry.register(scripted("twin", "", true));
        registry.register(scripted("twin", "", true));
    }
}
