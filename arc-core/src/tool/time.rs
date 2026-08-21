//! The toy tool from the 1.1 spike, kept as the seam's proof and the manual
//! smoke test until the memory tools land (5.2/6.3).

use std::future::Future;
use std::pin::Pin;

use chrono::Local;

use super::{Tool, ToolReply, TurnContext};
use crate::provider::ToolDefinition;

/// Answers with the current local date and time. Takes no arguments and
/// ignores whatever it is handed — there is nothing to get wrong.
pub struct GetTime;

impl Tool for GetTime {
    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: "get_time".to_owned(),
            description: "Get the current local date and time.".to_owned(),
            parameters: serde_json::json!({"type": "object", "properties": {}}),
        }
    }

    fn execute(
        &self,
        _arguments_json: String,
        _ctx: TurnContext,
    ) -> Pin<Box<dyn Future<Output = ToolReply> + Send + '_>> {
        Box::pin(async {
            ToolReply::ok(Local::now().format("%Y-%m-%d %H:%M:%S %:z, %A").to_string())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::GetTime;
    use crate::tool::Registry;
    use chrono::Local;

    #[tokio::test]
    async fn get_time_answers_through_the_registry() {
        let mut registry = Registry::new(32 * 1024);
        registry.register(Box::new(GetTime));

        let outcome = registry
            .dispatch("get_time", "{}".into(), crate::tool::TurnContext::default())
            .await;
        assert!(outcome.ok);
        assert!(!outcome.truncated);
        // The year is the one stable substring; everything else moves.
        let year = Local::now().format("%Y").to_string();
        assert!(outcome.content.starts_with(&year), "{}", outcome.content);
    }

    #[test]
    fn the_definition_offers_no_parameters() {
        use crate::tool::Tool as _;

        let def = GetTime.definition();
        assert_eq!(def.name, "get_time");
        assert_eq!(def.parameters["properties"], serde_json::json!({}));
    }
}
