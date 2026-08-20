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

pub mod time;

use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;

use crate::provider::ToolDefinition;

/// What a tool answered. An error is a reply the model will read, with
/// `ok: false`; it is not a Rust error.
pub struct ToolReply {
    pub content: String,
    pub ok: bool,
}

/// What [`Registry::dispatch`] yields: a [`ToolReply`] after the registry's
/// own policy — truncation, unknown-name handling — has been applied. The
/// engine builds `ToolResultRecorded` from this and nothing else.
pub struct DispatchOutcome {
    pub content: String,
    /// `false` → `TOOL_OUTCOME_ERROR`.
    pub ok: bool,
    pub truncated: bool,
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
    ) -> Pin<Box<dyn Future<Output = ToolReply> + Send + '_>>;
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
    pub async fn dispatch(&self, name: &str, arguments_json: String) -> DispatchOutcome {
        let span = tracing::Span::current();
        let Some(tool) = self.tools.get(name) else {
            span.record("outcome", "unknown-tool");
            return DispatchOutcome {
                content: format!("ERROR: Tool {name} is not available."),
                ok: false,
                truncated: false,
            };
        };
        let reply = tool.execute(arguments_json).await;
        span.record("outcome", if reply.ok { "ok" } else { "error" });
        let (content, truncated) = self.truncate(reply.content);
        DispatchOutcome {
            content,
            ok: reply.ok,
            truncated,
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
    use super::{DispatchOutcome, Registry, Tool, ToolReply};
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
        ) -> Pin<Box<dyn Future<Output = ToolReply> + Send + '_>> {
            let reply = ToolReply {
                content: self.content.to_owned(),
                ok: self.ok,
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
        ) -> Pin<Box<dyn Future<Output = ToolReply> + Send + '_>> {
            Box::pin(async move {
                ToolReply {
                    content: arguments_json,
                    ok: true,
                }
            })
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

        let DispatchOutcome { content, ok, .. } = registry.dispatch("beta", "{}".into()).await;
        assert!(ok);
        assert_eq!(content, "from beta");
    }

    #[tokio::test]
    async fn arguments_reach_the_tool_verbatim() {
        let registry = registry(vec![Box::new(Echo)]);

        let outcome = registry.dispatch("echo", r#"{"q":"café"}"#.into()).await;
        assert_eq!(outcome.content, r#"{"q":"café"}"#);
    }

    #[tokio::test]
    async fn an_unknown_tool_is_an_error_result_not_a_failure() {
        let registry = registry(vec![]);

        let outcome = registry.dispatch("missing", "{}".into()).await;
        assert!(!outcome.ok);
        assert!(!outcome.truncated);
        assert!(outcome.content.contains("missing"), "{}", outcome.content);
    }

    #[tokio::test]
    async fn a_tool_error_keeps_its_outcome_and_text() {
        let registry = registry(vec![scripted("fails", "ERROR: no such record", false)]);

        let outcome = registry.dispatch("fails", "{}".into()).await;
        assert!(!outcome.ok, "an error reply must not come back OK");
        assert_eq!(outcome.content, "ERROR: no such record");
    }

    #[tokio::test]
    async fn an_oversized_result_is_truncated_and_marked() {
        let mut registry = Registry::new(8);
        registry.register(scripted("big", "0123456789abcdef", true));

        let outcome = registry.dispatch("big", "{}".into()).await;
        assert!(outcome.truncated);
        assert!(outcome.ok, "truncation is not an error");
        assert_eq!(outcome.content, "01234567 [truncated]");
    }

    #[tokio::test]
    async fn truncation_lands_on_a_char_boundary() {
        // "é" is two bytes; a cap of 3 falls inside the second one.
        let mut registry = Registry::new(3);
        registry.register(scripted("accents", "ééé", true));

        let outcome = registry.dispatch("accents", "{}".into()).await;
        assert!(outcome.truncated);
        assert_eq!(outcome.content, "é [truncated]");
    }

    #[tokio::test]
    async fn a_result_at_the_cap_is_untouched() {
        let mut registry = Registry::new(4);
        registry.register(scripted("fits", "1234", true));

        let outcome = registry.dispatch("fits", "{}".into()).await;
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
