use std::future::Future;
use std::pin::Pin;

use arc_proto::v1::{Budget, SessionRole};
use serde::Deserialize;

use crate::provider::{ToolDefinition, role_label};
use crate::tool::{Intent, JobRequest, Tool, ToolReply, ToolSource, TurnContext};

pub struct Dispatch {
    projects: Vec<(String, String)>,
    scratch: Option<String>,
}

impl Dispatch {
    pub fn new(mut projects: Vec<(String, String)>, scratch: Option<String>) -> Self {
        projects.sort_unstable_by(|a, b| a.0.cmp(&b.0));
        Self { projects, scratch }
    }

    fn names_joined(&self) -> String {
        self.projects
            .iter()
            .map(|(name, _)| name.as_str())
            .collect::<Vec<_>>()
            .join(", ")
    }

    fn project_description(&self) -> String {
        let mut parts = vec!["The configured project the job binds to.".to_owned()];
        for (name, description) in &self.projects {
            if description.is_empty() {
                parts.push(format!("{name}."));
            } else {
                parts.push(format!("{name}: {description}."));
            }
        }
        parts.push(
            "\"none\" means this session's own bound project; an unbound caller lands \
             in the standing scratch project when one is configured, and must \
             otherwise name a configured project."
                .to_owned(),
        );
        parts.join(" ")
    }

    // an early-return-on-error reads best here; ToolReply is not on a hot path
    #[allow(clippy::result_large_err)]
    fn resolve_project(&self, project: &str, ctx: &TurnContext) -> Result<String, ToolReply> {
        if project == "none" {
            if ctx.grants.is_some() {
                // bound: the engine resolves "none" to this session's own project
                return Ok(project.to_owned());
            }
            return self.scratch.clone().ok_or_else(|| {
                ToolReply::error(format!(
                    "ERROR: no scratch project is configured. Name one of the configured \
                     projects instead: {}.",
                    self.names_joined()
                ))
            });
        }
        if self.projects.iter().any(|(name, _)| name == project) {
            return Ok(project.to_owned());
        }
        Err(ToolReply::error(format!(
            "ERROR: unknown project {project:?}. Use one of the configured projects \
             ({}), or \"none\" for this session's own project or the standing scratch \
             project.",
            self.names_joined()
        )))
    }
}

#[derive(Deserialize)]
struct DispatchArgs {
    role: String,
    project: String,
    brief: String,
    intent: String,
    #[serde(default)]
    fresh: bool,
}

impl Tool for Dispatch {
    fn definition(&self) -> ToolDefinition {
        let mut project_enum: Vec<String> =
            self.projects.iter().map(|(name, _)| name.clone()).collect();
        project_enum.push("none".to_owned());
        ToolDefinition {
            name: "dispatch".to_owned(),
            description: "Start a job: a child session bound to a configured project, with \
                          its own role and budget. This call only starts the job and names \
                          the child session — it does not wait for the job to finish. \
                          Before dispatching, check whether a finished job already holds \
                          the needed context; continue_job continues it with that context \
                          intact."
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
                        "description": self.project_description(),
                    },
                    "brief": {
                        "type": "string",
                        "description": "The complete task brief the child session starts \
                            from. It must be self-contained — the child sees nothing of \
                            this conversation."
                    },
                    "intent": {
                        "type": "string",
                        "enum": ["analyze", "implement"],
                        "description": "analyze: the job reads and reports; the workspace \
                            stays untouched — its write tools are refused. implement: the \
                            job changes the workspace."
                    },
                    "fresh": {
                        "type": "boolean",
                        "description": "Start from nothing even though a finished job in \
                            this project keeps its context. A dispatch refused for that \
                            reason names the job to continue; set fresh only when its \
                            context is irrelevant to this task."
                    },
                },
                "required": ["role", "project", "brief", "intent"]
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
            let args: DispatchArgs = match serde_json::from_str(&arguments_json) {
                Ok(args) => args,
                Err(error) => {
                    return ToolReply::error(format!(
                        "ERROR: bad dispatch arguments ({error}). Pass role, project, \
                         brief, and intent."
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
            let project = match self.resolve_project(&args.project, &ctx) {
                Ok(project) => project,
                Err(reply) => return reply,
            };
            let intent = match args.intent.as_str() {
                "analyze" => Intent::Analyze,
                "implement" => Intent::Implement,
                other => {
                    return ToolReply::error(format!(
                        "ERROR: unknown intent {other:?}. Use analyze or implement."
                    ));
                }
            };
            if args.brief.trim().is_empty() {
                return ToolReply::error(
                    "ERROR: brief must not be empty. The child starts from nothing else — \
                     a self-contained brief is required."
                        .to_owned(),
                );
            }
            // budgets are suspended while daily use calibrates; 5.5 stays dormant
            let budget: Option<Budget> = None;
            ToolReply {
                content: format!("Dispatching {} into {project}.", role_label(role)),
                ok: true,
                memory_events: Vec::new(),
                job_request: Some(JobRequest {
                    role,
                    project,
                    brief: args.brief,
                    budget,
                    intent,
                    fresh: args.fresh,
                }),
                continue_request: None,
                cancel_request: None,
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::Dispatch;
    use crate::tool::workspace::{Grant, Grants, Mode};
    use crate::tool::{Tool, TurnContext};
    use arc_proto::v1::SessionRole;

    fn dispatch(projects: &[(&str, &str)], scratch: Option<&str>) -> Dispatch {
        Dispatch::new(
            projects
                .iter()
                .map(|(name, description)| ((*name).to_owned(), (*description).to_owned()))
                .collect(),
            scratch.map(str::to_owned),
        )
    }

    fn ctx_bound(root: &std::path::Path) -> TurnContext {
        let grants = Grants::new(vec![Grant::new(root, Mode::ReadWrite)]).expect("grants");
        TurnContext {
            session_id: String::new(),
            turn_id: String::new(),
            grants: Some(Arc::new(grants)),
            command_prefix: Vec::new(),
        }
    }

    fn args(role: &str, project: &str, brief: &str, intent: &str) -> String {
        serde_json::json!({
            "role": role,
            "project": project,
            "brief": brief,
            "intent": intent,
        })
        .to_string()
    }

    #[tokio::test]
    async fn a_valid_executor_dispatch_produces_a_resolved_job_request() {
        let tool = dispatch(&[("arc", "")], None);

        let reply = tool
            .execute(
                args("executor", "arc", "fix the bug", "implement"),
                TurnContext::default(),
            )
            .await;

        assert!(reply.ok, "{}", reply.content);
        let job = reply.job_request.expect("a job request");
        assert_eq!(job.role, SessionRole::Executor);
        assert_eq!(job.project, "arc");
        assert_eq!(job.brief, "fix the bug");
        assert_eq!(job.budget, None);
        assert_eq!(job.intent, crate::tool::Intent::Implement);
        assert!(!job.fresh, "fresh defaults to false when absent");
    }

    #[tokio::test]
    async fn a_dispatch_with_fresh_true_carries_it_on_the_job_request() {
        let tool = dispatch(&[("arc", "")], None);

        let reply = tool
            .execute(
                serde_json::json!({
                    "role": "executor",
                    "project": "arc",
                    "brief": "unrelated work",
                    "intent": "implement",
                    "fresh": true,
                })
                .to_string(),
                TurnContext::default(),
            )
            .await;

        assert!(reply.ok, "{}", reply.content);
        assert!(reply.job_request.expect("a job request").fresh);
    }

    #[tokio::test]
    async fn an_analyze_dispatch_produces_a_job_request_with_analyze_intent() {
        let tool = dispatch(&[("arc", "")], None);

        let reply = tool
            .execute(
                args("executor", "arc", "check consistency", "analyze"),
                TurnContext::default(),
            )
            .await;

        assert!(reply.ok, "{}", reply.content);
        let job = reply.job_request.expect("a job request");
        assert_eq!(job.intent, crate::tool::Intent::Analyze);
    }

    #[tokio::test]
    async fn none_in_a_bound_session_passes_through_unresolved() {
        // the tool only validates the value; the engine resolves "none" to
        // the calling session's own project, since only it knows that
        let dir = tempfile::TempDir::new().expect("tmp");
        let tool = dispatch(&[("arc", "")], None);

        let reply = tool
            .execute(
                args("archivist", "none", "tidy up notes", "implement"),
                ctx_bound(dir.path()),
            )
            .await;

        assert!(reply.ok, "{}", reply.content);
        assert_eq!(reply.job_request.expect("a job request").project, "none");
    }

    #[tokio::test]
    async fn none_resolves_to_scratch_when_one_is_configured() {
        let tool = dispatch(&[("arc", "")], Some("scratch"));

        let reply = tool
            .execute(
                args("archivist", "none", "tidy up notes", "implement"),
                TurnContext::default(),
            )
            .await;

        assert!(reply.ok, "{}", reply.content);
        assert_eq!(reply.job_request.expect("a job request").project, "scratch");
    }

    #[tokio::test]
    async fn none_from_an_unbound_session_without_a_configured_scratch_is_an_actionable_error() {
        let tool = dispatch(&[("arc", "")], None);

        let reply = tool
            .execute(
                args("executor", "none", "do something", "implement"),
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
        let tool = dispatch(&[("arc", "")], None);

        let reply = tool
            .execute(
                args("executor", "arc", "   ", "implement"),
                TurnContext::default(),
            )
            .await;

        assert!(!reply.ok);
        assert!(reply.job_request.is_none());
        assert!(reply.content.contains("brief"), "{}", reply.content);
    }

    #[tokio::test]
    async fn zero_budgets_resolve_to_no_budget() {
        let tool = dispatch(&[("arc", "")], None);

        let reply = tool
            .execute(
                args("executor", "arc", "fix the bug", "implement"),
                TurnContext::default(),
            )
            .await;

        assert!(reply.ok, "{}", reply.content);
        assert_eq!(reply.job_request.expect("a job request").budget, None);
    }

    #[tokio::test]
    async fn an_unknown_role_string_is_an_error() {
        let tool = dispatch(&[("arc", "")], None);

        let reply = tool
            .execute(
                args("wizard", "arc", "fix the bug", "implement"),
                TurnContext::default(),
            )
            .await;

        assert!(!reply.ok);
        assert!(reply.job_request.is_none());
        assert!(reply.content.contains("wizard"), "{}", reply.content);
    }

    #[tokio::test]
    async fn an_unknown_project_string_is_an_error() {
        let tool = dispatch(&[("arc", "")], None);

        let reply = tool
            .execute(
                args("executor", "ghost", "fix the bug", "implement"),
                TurnContext::default(),
            )
            .await;

        assert!(!reply.ok);
        assert!(reply.job_request.is_none());
        assert!(reply.content.contains("ghost"), "{}", reply.content);
    }

    #[tokio::test]
    async fn an_unknown_intent_string_names_both_options() {
        let tool = dispatch(&[("arc", "")], None);

        let reply = tool
            .execute(
                args("executor", "arc", "fix the bug", "yolo"),
                TurnContext::default(),
            )
            .await;

        assert!(!reply.ok);
        assert!(reply.job_request.is_none());
        assert!(reply.content.contains("analyze"), "{}", reply.content);
        assert!(reply.content.contains("implement"), "{}", reply.content);
    }

    #[tokio::test]
    async fn a_dispatch_missing_intent_is_a_bad_arguments_error() {
        let tool = dispatch(&[("arc", "")], None);

        let reply = tool
            .execute(
                serde_json::json!({
                    "role": "executor",
                    "project": "arc",
                    "brief": "fix the bug",
                })
                .to_string(),
                TurnContext::default(),
            )
            .await;

        assert!(!reply.ok);
        assert!(reply.job_request.is_none());
        assert!(reply.content.contains("intent"), "{}", reply.content);
    }

    #[test]
    fn the_definition_requires_every_field_and_carries_the_escape_values() {
        let tool = dispatch(&[("arc", ""), ("scratch", "")], Some("scratch"));

        let definition = tool.definition();
        assert_eq!(definition.name, "dispatch");

        let required = definition.parameters["required"]
            .as_array()
            .expect("required array")
            .iter()
            .map(|v| v.as_str().expect("string"))
            .collect::<Vec<_>>();
        assert_eq!(required, ["role", "project", "brief", "intent"]);
        assert!(
            definition.parameters["properties"]["fresh"].is_object(),
            "fresh is declared but never required"
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

        let intent_enum = definition.parameters["properties"]["intent"]["enum"]
            .as_array()
            .expect("intent enum")
            .iter()
            .map(|v| v.as_str().expect("string").to_owned())
            .collect::<Vec<_>>();
        assert_eq!(intent_enum, ["analyze", "implement"]);
    }

    #[test]
    fn project_descriptions_render_into_the_project_property_description() {
        let tool = dispatch(
            &[("arc", "ARC's own implementation repo"), ("scratch", "")],
            Some("scratch"),
        );

        let description = tool.definition().parameters["properties"]["project"]["description"]
            .as_str()
            .expect("string")
            .to_owned();

        assert!(
            description.contains("arc: ARC's own implementation repo."),
            "{description}"
        );
        assert!(
            description.contains("scratch.") && !description.contains("scratch:"),
            "an empty description contributes just the bare name: {description}"
        );
        assert!(
            description.contains("\"none\" means this session's own bound project"),
            "{description}"
        );
        assert!(
            description.contains("standing scratch project"),
            "{description}"
        );
    }
}
