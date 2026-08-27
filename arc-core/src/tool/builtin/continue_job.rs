use std::future::Future;
use std::pin::Pin;

use serde::Deserialize;

use crate::provider::ToolDefinition;
use crate::tool::{ContinueRequest, Tool, ToolReply, ToolSource, TurnContext};

/// Validates a `continue_job` call and hands the engine a `ContinueRequest`.
/// It never touches the target session itself — whether it exists, and
/// whether it is a job, is engine-side.
pub struct ContinueJob;

#[derive(Deserialize)]
struct ContinueJobArgs {
    session_id: String,
    message: String,
}

impl Tool for ContinueJob {
    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: "continue_job".to_owned(),
            description: "Continues a dispatched job: queued if it is still running, resumed \
                          with its full context if it finished. Use it to change course or \
                          add work — and when new work belongs to a job that already ran, \
                          continue that job instead of dispatching a fresh one that starts \
                          from nothing. Never call it to fetch a result: the reply arrives \
                          on its own as a handback when the job finishes, this call returns \
                          only an acknowledgment, and each message costs a full job turn."
                .to_owned(),
            parameters: serde_json::json!({
                "type": "object",
                "properties": {
                    "session_id": {
                        "type": "string",
                        "description": "The job's session id, from its dispatch \
                            acknowledgment or its handback."
                    },
                    "message": {
                        "type": "string",
                        "description": "The follow-up the job receives as its next turn."
                    },
                },
                "required": ["session_id", "message"]
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
            let args: ContinueJobArgs = match serde_json::from_str(&arguments_json) {
                Ok(args) => args,
                Err(error) => {
                    return ToolReply::error(format!(
                        "ERROR: bad continue_job arguments ({error}). Pass session_id and \
                         message."
                    ));
                }
            };
            if args.session_id.trim().is_empty() {
                return ToolReply::error("ERROR: session_id must not be empty.".to_owned());
            }
            if args.message.trim().is_empty() {
                return ToolReply::error(
                    "ERROR: message must not be empty. The job needs something to react to."
                        .to_owned(),
                );
            }
            ToolReply {
                content: format!(
                    "Continuing job {}. Its reply arrives later as a handback; do not call \
                     continue_job again to fetch it.",
                    args.session_id
                ),
                ok: true,
                memory_events: Vec::new(),
                job_request: None,
                continue_request: Some(ContinueRequest {
                    session_id: args.session_id,
                    message: args.message,
                }),
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::ContinueJob;
    use crate::tool::{Tool, TurnContext};

    fn args(session_id: &str, message: &str) -> String {
        serde_json::json!({
            "session_id": session_id,
            "message": message,
        })
        .to_string()
    }

    #[tokio::test]
    async fn valid_args_produce_a_continue_request() {
        let reply = ContinueJob
            .execute(
                args("s-child", "check the linter too"),
                TurnContext::default(),
            )
            .await;

        assert!(reply.ok, "{}", reply.content);
        let request = reply.continue_request.expect("a continue request");
        assert_eq!(request.session_id, "s-child");
        assert_eq!(request.message, "check the linter too");
        assert!(reply.job_request.is_none());
    }

    #[tokio::test]
    async fn an_empty_session_id_is_an_error() {
        let reply = ContinueJob
            .execute(args("   ", "do more"), TurnContext::default())
            .await;

        assert!(!reply.ok);
        assert!(reply.continue_request.is_none());
        assert!(reply.content.contains("session_id"), "{}", reply.content);
    }

    #[tokio::test]
    async fn an_empty_message_is_an_error() {
        let reply = ContinueJob
            .execute(args("s-child", "   "), TurnContext::default())
            .await;

        assert!(!reply.ok);
        assert!(reply.continue_request.is_none());
        assert!(reply.content.contains("message"), "{}", reply.content);
    }

    #[tokio::test]
    async fn bad_json_is_an_actionable_error() {
        let reply = ContinueJob
            .execute("not json".to_owned(), TurnContext::default())
            .await;

        assert!(!reply.ok);
        assert!(reply.continue_request.is_none());
        assert!(reply.content.contains("continue_job"), "{}", reply.content);
    }

    #[test]
    fn the_definition_requires_session_id_and_message() {
        let definition = ContinueJob.definition();
        assert_eq!(definition.name, "continue_job");

        let required = definition.parameters["required"]
            .as_array()
            .expect("required array")
            .iter()
            .map(|v| v.as_str().expect("string"))
            .collect::<Vec<_>>();
        assert_eq!(required, ["session_id", "message"]);
    }
}
