use std::future::Future;
use std::pin::Pin;

use serde::Deserialize;

use crate::provider::ToolDefinition;
use crate::tool::{ContinueRequest, Tool, ToolReply, ToolSource, TurnContext};

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
            description: "Send a follow-up to an existing job. It queues if running or \
                          resumes with its context if finished. Results arrive automatically \
                          as handbacks; this does not fetch a result."
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
        ToolSource::Jobs
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
                changed_paths: Vec::new(),
                content: format!(
                    "Continuing job {}. Its reply will arrive as a handback.",
                    args.session_id
                ),
                ok: true,
                memory_events: Vec::new(),
                job_request: None,
                continue_request: Some(ContinueRequest {
                    session_id: args.session_id,
                    message: args.message,
                }),
                cancel_request: None,
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
}
