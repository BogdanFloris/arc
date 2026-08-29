use std::future::Future;
use std::pin::Pin;

use serde::Deserialize;

use crate::provider::ToolDefinition;
use crate::tool::{Tool, ToolReply, ToolSource, TurnContext};

/// Validates a `cancel_job` call and hands the engine the target session id.
/// It never touches the job itself — whether it's live, and stopping it if
/// so, is the supervisor's, once the turn's `Reply.cancels` reaches arcd.
pub struct CancelJob;

#[derive(Deserialize)]
struct CancelJobArgs {
    session_id: String,
}

impl Tool for CancelJob {
    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: "cancel_job".to_owned(),
            description: "Cancels a dispatched job the caller started. The job stops at its \
                          next await point and its handback confirms. Cancelling a job that \
                          already finished is a no-op; the jobs list will tell the truth about \
                          it."
            .to_owned(),
            parameters: serde_json::json!({
                "type": "object",
                "properties": {
                    "session_id": {
                        "type": "string",
                        "description": "The job's session id, from its dispatch \
                            acknowledgment or its handback."
                    },
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
            let args: CancelJobArgs = match serde_json::from_str(&arguments_json) {
                Ok(args) => args,
                Err(error) => {
                    return ToolReply::error(format!(
                        "ERROR: bad cancel_job arguments ({error}). Pass session_id."
                    ));
                }
            };
            if args.session_id.trim().is_empty() {
                return ToolReply::error("ERROR: session_id must not be empty.".to_owned());
            }
            ToolReply {
                content: format!(
                    "Cancellation requested for {}; if it was running, its handback will \
                     confirm.",
                    args.session_id
                ),
                ok: true,
                memory_events: Vec::new(),
                job_request: None,
                continue_request: None,
                cancel_request: Some(args.session_id),
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::CancelJob;
    use crate::tool::{Tool, TurnContext};

    fn args(session_id: &str) -> String {
        serde_json::json!({ "session_id": session_id }).to_string()
    }

    #[tokio::test]
    async fn valid_args_produce_a_cancel_request() {
        let reply = CancelJob
            .execute(args("s-child"), TurnContext::default())
            .await;

        assert!(reply.ok, "{}", reply.content);
        assert_eq!(reply.cancel_request, Some("s-child".to_owned()));
        assert!(reply.job_request.is_none());
        assert!(reply.continue_request.is_none());
        assert!(reply.content.contains("s-child"), "{}", reply.content);
    }

    #[tokio::test]
    async fn an_empty_session_id_is_an_error() {
        let reply = CancelJob.execute(args("   "), TurnContext::default()).await;

        assert!(!reply.ok);
        assert!(reply.cancel_request.is_none());
        assert!(reply.content.contains("session_id"), "{}", reply.content);
    }

    #[tokio::test]
    async fn bad_json_is_an_actionable_error() {
        let reply = CancelJob
            .execute("not json".to_owned(), TurnContext::default())
            .await;

        assert!(!reply.ok);
        assert!(reply.cancel_request.is_none());
        assert!(reply.content.contains("cancel_job"), "{}", reply.content);
    }

    #[test]
    fn the_definition_requires_session_id() {
        let definition = CancelJob.definition();
        assert_eq!(definition.name, "cancel_job");

        let required = definition.parameters["required"]
            .as_array()
            .expect("required array")
            .iter()
            .map(|v| v.as_str().expect("string"))
            .collect::<Vec<_>>();
        assert_eq!(required, ["session_id"]);
    }
}
