use std::future::Future;
use std::pin::Pin;

use arc_proto::v1::{Budget, SessionRole};
use serde::Deserialize;

use crate::provider::{ToolDefinition, role_label};
use crate::tool::{JobRequest, Tool, ToolReply, ToolSource, TurnContext};

/// Validates a dispatch call and hands the engine a `JobRequest`. It never
/// creates the child session itself — a tool cannot hold `&mut Engine`.
pub struct Dispatch {
    projects: Vec<String>,
    scratch: Option<String>,
}

impl Dispatch {
    pub fn new(mut projects: Vec<String>, scratch: Option<String>) -> Self {
        projects.sort_unstable();
        Self { projects, scratch }
    }

    // an early-return-on-error reads best here; ToolReply is not on a hot path
    #[allow(clippy::result_large_err)]
    fn resolve_project(&self, project: &str) -> Result<String, ToolReply> {
        if project == "none" {
            return self.scratch.clone().ok_or_else(|| {
                ToolReply::error(format!(
                    "ERROR: no scratch project is configured. Name one of the configured \
                     projects instead: {}.",
                    self.projects.join(", ")
                ))
            });
        }
        if self.projects.iter().any(|configured| configured == project) {
            return Ok(project.to_owned());
        }
        Err(ToolReply::error(format!(
            "ERROR: unknown project {project:?}. Use one of the configured projects \
             ({}), or \"none\" for the scratch project.",
            self.projects.join(", ")
        )))
    }
}

#[derive(Deserialize)]
struct DispatchArgs {
    role: String,
    project: String,
    brief: String,
    budget_tokens: u64,
    budget_minutes: u32,
}

impl Tool for Dispatch {
    fn definition(&self) -> ToolDefinition {
        let mut project_enum = self.projects.clone();
        project_enum.push("none".to_owned());
        ToolDefinition {
            name: "dispatch".to_owned(),
            description: "Start a job: a child session bound to a configured project, with \
                          its own role and budget. This call only starts the job and names \
                          the child session — it does not wait for the job to finish."
                .to_owned(),
            parameters: serde_json::json!({
                "type": "object",
                "properties": {
                    "role": {
                        "type": "string",
                        "enum": ["executor", "archivist"],
                        "description": "Who runs the job. executor: coding and workspace \
                            tasks. archivist: extraction and organization of memory. Recall \
                            questions are answered directly here, never dispatched."
                    },
                    "project": {
                        "type": "string",
                        "enum": project_enum,
                        "description": "The configured project the job binds to. \"none\" \
                            lands the job in the standing scratch project; the \
                            acknowledgment names where it went."
                    },
                    "brief": {
                        "type": "string",
                        "description": "The complete task brief the child session starts \
                            from. It must be self-contained — the child sees nothing of \
                            this conversation."
                    },
                    "budget_tokens": {
                        "type": "integer",
                        "description": "Token budget for the job. 0 means no token budget. \
                            A coding task typically needs 100000-500000; anything under \
                            50000 will stop a job mid-work."
                    },
                    "budget_minutes": {
                        "type": "integer",
                        "description": "Wall-clock budget for the job, in minutes. 0 means \
                            no time limit."
                    }
                },
                "required": ["role", "project", "brief", "budget_tokens", "budget_minutes"]
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
            let args: DispatchArgs = match serde_json::from_str(&arguments_json) {
                Ok(args) => args,
                Err(error) => {
                    return ToolReply::error(format!(
                        "ERROR: bad dispatch arguments ({error}). Pass role, project, \
                         brief, budget_tokens, and budget_minutes."
                    ));
                }
            };
            let role = match args.role.as_str() {
                "executor" => SessionRole::Executor,
                "archivist" => SessionRole::Archivist,
                other => {
                    return ToolReply::error(format!(
                        "ERROR: unknown role {other:?}. Use executor or archivist."
                    ));
                }
            };
            let project = match self.resolve_project(&args.project) {
                Ok(project) => project,
                Err(reply) => return reply,
            };
            if args.brief.trim().is_empty() {
                return ToolReply::error(
                    "ERROR: brief must not be empty. The child starts from nothing else — \
                     a self-contained brief is required."
                        .to_owned(),
                );
            }
            let budget = (args.budget_tokens != 0 || args.budget_minutes != 0).then_some(Budget {
                total_tokens: args.budget_tokens,
                wall_clock_seconds: args.budget_minutes.saturating_mul(60),
            });
            ToolReply {
                content: format!("Dispatching {} into {project}.", role_label(role)),
                ok: true,
                memory_events: Vec::new(),
                job_request: Some(JobRequest {
                    role,
                    project,
                    brief: args.brief,
                    budget,
                }),
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::Dispatch;
    use crate::tool::{Tool, TurnContext};
    use arc_proto::v1::{Budget, SessionRole};

    fn dispatch(projects: &[&str], scratch: Option<&str>) -> Dispatch {
        Dispatch::new(
            projects.iter().map(|p| (*p).to_owned()).collect(),
            scratch.map(str::to_owned),
        )
    }

    fn args(
        role: &str,
        project: &str,
        brief: &str,
        budget_tokens: u64,
        budget_minutes: u32,
    ) -> String {
        serde_json::json!({
            "role": role,
            "project": project,
            "brief": brief,
            "budget_tokens": budget_tokens,
            "budget_minutes": budget_minutes,
        })
        .to_string()
    }

    #[tokio::test]
    async fn a_valid_executor_dispatch_produces_a_resolved_job_request() {
        let tool = dispatch(&["arc"], None);

        let reply = tool
            .execute(
                args("executor", "arc", "fix the bug", 1000, 5),
                TurnContext::default(),
            )
            .await;

        assert!(reply.ok, "{}", reply.content);
        let job = reply.job_request.expect("a job request");
        assert_eq!(job.role, SessionRole::Executor);
        assert_eq!(job.project, "arc");
        assert_eq!(job.brief, "fix the bug");
        assert_eq!(
            job.budget,
            Some(Budget {
                total_tokens: 1000,
                wall_clock_seconds: 300,
            })
        );
    }

    #[tokio::test]
    async fn none_resolves_to_scratch_when_one_is_configured() {
        let tool = dispatch(&["arc"], Some("scratch"));

        let reply = tool
            .execute(
                args("archivist", "none", "tidy up notes", 0, 0),
                TurnContext::default(),
            )
            .await;

        assert!(reply.ok, "{}", reply.content);
        assert_eq!(reply.job_request.expect("a job request").project, "scratch");
    }

    #[tokio::test]
    async fn none_without_a_configured_scratch_is_an_actionable_error() {
        let tool = dispatch(&["arc"], None);

        let reply = tool
            .execute(
                args("executor", "none", "do something", 0, 0),
                TurnContext::default(),
            )
            .await;

        assert!(!reply.ok);
        assert!(reply.job_request.is_none());
        assert!(
            reply.content.contains("no scratch project"),
            "{}",
            reply.content
        );
        assert!(reply.content.contains("arc"), "{}", reply.content);
    }

    #[tokio::test]
    async fn an_empty_brief_is_an_error() {
        let tool = dispatch(&["arc"], None);

        let reply = tool
            .execute(args("executor", "arc", "   ", 0, 0), TurnContext::default())
            .await;

        assert!(!reply.ok);
        assert!(reply.job_request.is_none());
        assert!(reply.content.contains("brief"), "{}", reply.content);
    }

    #[tokio::test]
    async fn zero_budgets_resolve_to_no_budget() {
        let tool = dispatch(&["arc"], None);

        let reply = tool
            .execute(
                args("executor", "arc", "fix the bug", 0, 0),
                TurnContext::default(),
            )
            .await;

        assert!(reply.ok, "{}", reply.content);
        assert_eq!(reply.job_request.expect("a job request").budget, None);
    }

    #[tokio::test]
    async fn an_unknown_role_string_is_an_error() {
        let tool = dispatch(&["arc"], None);

        let reply = tool
            .execute(
                args("wizard", "arc", "fix the bug", 0, 0),
                TurnContext::default(),
            )
            .await;

        assert!(!reply.ok);
        assert!(reply.job_request.is_none());
        assert!(reply.content.contains("wizard"), "{}", reply.content);
    }

    #[tokio::test]
    async fn an_unknown_project_string_is_an_error() {
        let tool = dispatch(&["arc"], None);

        let reply = tool
            .execute(
                args("executor", "ghost", "fix the bug", 0, 0),
                TurnContext::default(),
            )
            .await;

        assert!(!reply.ok);
        assert!(reply.job_request.is_none());
        assert!(reply.content.contains("ghost"), "{}", reply.content);
    }

    #[test]
    fn the_definition_requires_every_field_and_carries_the_escape_values() {
        let tool = dispatch(&["arc", "scratch"], Some("scratch"));

        let definition = tool.definition();
        assert_eq!(definition.name, "dispatch");

        let required = definition.parameters["required"]
            .as_array()
            .expect("required array")
            .iter()
            .map(|v| v.as_str().expect("string"))
            .collect::<Vec<_>>();
        assert_eq!(
            required,
            [
                "role",
                "project",
                "brief",
                "budget_tokens",
                "budget_minutes"
            ]
        );

        let role_enum = definition.parameters["properties"]["role"]["enum"]
            .as_array()
            .expect("role enum");
        assert_eq!(role_enum, &["executor", "archivist"]);

        let project_enum = definition.parameters["properties"]["project"]["enum"]
            .as_array()
            .expect("project enum")
            .iter()
            .map(|v| v.as_str().expect("string").to_owned())
            .collect::<Vec<_>>();
        assert_eq!(
            project_enum,
            ["arc", "scratch", "none"],
            "an explicit escape value"
        );
    }
}
