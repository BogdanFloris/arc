pub mod builtin;

use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;

use crate::provider::ToolDefinition;

#[derive(Debug, Clone, Default)]
pub struct TurnContext {
    pub session_id: String,
    pub turn_id: String,
}

pub struct ToolReply {
    pub content: String,
    pub ok: bool,
    pub memory_events: Vec<arc_proto::v1::memory_event::Event>,
}

impl ToolReply {
    pub fn ok(content: String) -> Self {
        Self {
            content,
            ok: true,
            memory_events: Vec::new(),
        }
    }

    pub fn error(content: String) -> Self {
        Self {
            content,
            ok: false,
            memory_events: Vec::new(),
        }
    }
}

pub(crate) struct DispatchOutcome {
    pub content: String,
    pub ok: bool,
    pub truncated: bool,
    pub memory_events: Vec<arc_proto::v1::memory_event::Event>,
}

/// Where a tool comes from. A session declares the sources it gets, so a tool
/// it was never offered is never in its context.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum ToolSource {
    Builtin,
    /// Resolved from the session provider's own grounding; no tools of ours.
    Web,
    Workspace,
}

impl ToolSource {
    pub const ALL: [ToolSource; 3] = [ToolSource::Builtin, ToolSource::Web, ToolSource::Workspace];
}

pub trait Tool: Send + Sync {
    fn definition(&self) -> ToolDefinition;
    fn source(&self) -> ToolSource;
    fn execute(
        &self,
        arguments_json: String,
        ctx: TurnContext,
    ) -> Pin<Box<dyn Future<Output = ToolReply> + Send + '_>>;
}

pub(crate) fn to_json<T: serde::Serialize>(value: &T) -> String {
    serde_json::to_string(value)
        .unwrap_or_else(|error| format!("ERROR: could not serialize the reply ({error})."))
}

pub struct Registry {
    tools: BTreeMap<String, Box<dyn Tool>>,
    max_tool_result_bytes: usize,
}

impl Registry {
    pub fn new(max_tool_result_bytes: usize) -> Self {
        Registry {
            tools: BTreeMap::new(),
            max_tool_result_bytes,
        }
    }

    pub fn register(&mut self, tool: Box<dyn Tool>) {
        let name = tool.definition().name;
        debug_assert!(
            !self.tools.contains_key(&name),
            "registry already contains the tool {name}"
        );
        self.tools.insert(name, tool);
    }

    pub(crate) fn definitions(&self, sources: &[ToolSource]) -> Vec<ToolDefinition> {
        self.tools
            .values()
            .filter(|tool| sources.contains(&tool.source()))
            .map(|tool| tool.definition())
            .collect()
    }

    #[tracing::instrument(
        name = "tool.dispatch",
        skip_all,
        fields(tool = name, outcome = tracing::field::Empty)
    )]
    pub(crate) async fn dispatch(
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
    use super::{DispatchOutcome, Registry, Tool, ToolReply, ToolSource, TurnContext};
    use crate::provider::ToolDefinition;
    use std::future::Future;
    use std::pin::Pin;

    struct Scripted {
        name: &'static str,
        content: &'static str,
        ok: bool,
        source: ToolSource,
    }

    impl Tool for Scripted {
        fn definition(&self) -> ToolDefinition {
            ToolDefinition {
                name: self.name.to_owned(),
                description: String::new(),
                parameters: serde_json::json!({"type": "object", "properties": {}}),
            }
        }

        fn source(&self) -> ToolSource {
            self.source
        }

        fn execute(
            &self,
            _arguments_json: String,
            _ctx: TurnContext,
        ) -> Pin<Box<dyn Future<Output = ToolReply> + Send + '_>> {
            let reply = if self.ok {
                ToolReply::ok(self.content.to_owned())
            } else {
                ToolReply::error(self.content.to_owned())
            };
            Box::pin(async move { reply })
        }
    }

    struct Echo;

    impl Tool for Echo {
        fn definition(&self) -> ToolDefinition {
            ToolDefinition {
                name: "echo".to_owned(),
                description: String::new(),
                parameters: serde_json::json!({"type": "object"}),
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
            Box::pin(async move { ToolReply::ok(arguments_json) })
        }
    }

    struct WhereAmI;

    impl Tool for WhereAmI {
        fn definition(&self) -> ToolDefinition {
            ToolDefinition {
                name: "where_am_i".to_owned(),
                description: String::new(),
                parameters: serde_json::json!({"type": "object"}),
            }
        }

        fn source(&self) -> ToolSource {
            ToolSource::Builtin
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

    fn names(registry: &Registry, sources: &[ToolSource]) -> Vec<String> {
        registry
            .definitions(sources)
            .into_iter()
            .map(|def| def.name)
            .collect()
    }

    fn scripted(name: &'static str, content: &'static str, ok: bool) -> Box<dyn Tool> {
        sourced(name, content, ok, ToolSource::Builtin)
    }

    fn sourced(
        name: &'static str,
        content: &'static str,
        ok: bool,
        source: ToolSource,
    ) -> Box<dyn Tool> {
        Box::new(Scripted {
            name,
            content,
            ok,
            source,
        })
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

        assert_eq!(names(&registry, &ToolSource::ALL), ["alpha", "zeta"]);
    }

    #[test]
    fn definitions_offers_only_the_sources_asked_for() {
        let registry = registry(vec![
            sourced("recall", "", true, ToolSource::Builtin),
            sourced("read", "", true, ToolSource::Workspace),
        ]);

        assert_eq!(names(&registry, &[ToolSource::Builtin]), ["recall"]);
        assert_eq!(names(&registry, &[ToolSource::Workspace]), ["read"]);
        assert_eq!(names(&registry, &ToolSource::ALL), ["read", "recall"]);
    }

    #[test]
    fn no_sources_offers_no_tools() {
        let registry = registry(vec![scripted("recall", "", true)]);

        assert!(names(&registry, &[]).is_empty());
    }

    #[test]
    fn the_web_source_holds_no_tools_of_ours() {
        let registry = registry(vec![
            sourced("recall", "", true, ToolSource::Builtin),
            sourced("read", "", true, ToolSource::Workspace),
        ]);

        assert!(names(&registry, &[ToolSource::Web]).is_empty());
    }

    #[test]
    #[should_panic(expected = "already contains")]
    fn a_duplicate_registration_panics_in_debug() {
        let mut registry = Registry::new(1024);
        registry.register(scripted("twin", "", true));
        registry.register(scripted("twin", "", true));
    }
}
