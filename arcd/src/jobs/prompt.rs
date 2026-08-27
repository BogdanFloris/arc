use std::path::Path;

use tracing::warn;

use crate::identity;

/// Under ~200 tokens: where the job is and what its final message is for,
/// not a methodology essay. Pi-style: the model already knows how to code.
fn job_preamble(root: &Path) -> String {
    format!(
        "You are a coding agent inside ARC's harness, working non-interactively \
         in {}. Four workspace tools are available: read, write, edit, bash. Be \
         concise. Show file paths clearly. When you are done, your final message \
         is the job's report.",
        root.display()
    )
}

/// The preamble, plus the project's `AGENTS.md` verbatim if it has one.
/// Built once at spawn so it stays byte-stable for the job's lifetime.
pub(super) fn job_system_prompt(root: &Path) -> String {
    let preamble = job_preamble(root);
    match identity::load(&root.join("AGENTS.md")) {
        Ok(Some(agents)) => format!("{preamble}\n\n{agents}"),
        Ok(None) => preamble,
        Err(error) => {
            warn!(root = %root.display(), %error, "could not read AGENTS.md; using the preamble only");
            preamble
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use arc_core::provider::{CompletionDelta, Stop};
    use arc_core::session::DispatchedJob;
    use arc_core::testkit::{ScriptedProvider, Step, done_reply, usage};
    use arc_proto::v1::SessionRole;
    use tempfile::TempDir;

    use crate::jobs::Supervisor;
    use crate::jobs::tests_common::testkit::{
        child_session, engine_for_project, executor_runner, wait_for_message_count,
    };

    #[tokio::test]
    async fn a_spawned_jobs_system_prompt_carries_the_preamble_and_agents_md() {
        let dir = TempDir::new().expect("temp dir");
        let root = dir.path().join("proj");
        std::fs::create_dir_all(&root).expect("mkdir proj");
        std::fs::write(
            root.join("AGENTS.md"),
            "# Project rules\n\nUse jj, not git.\n",
        )
        .expect("write AGENTS.md");

        let concierge_provider = ScriptedProvider::scripted(vec![]);
        let executor_provider = ScriptedProvider::scripted(vec![done_reply("on it")]);
        let engine = engine_for_project(&dir, &root);
        let child_id = child_session(&engine, &concierge_provider);

        let runners =
            BTreeMap::from([(SessionRole::Executor, executor_runner(&executor_provider))]);
        let supervisor = Supervisor::new(Arc::clone(&engine), runners)
            .with_projects(BTreeMap::from([("arc".to_owned(), root.clone())]));

        supervisor.spawn(DispatchedJob {
            session_id: child_id.clone(),
            parent_session: "s-parent".to_owned(),
            role: SessionRole::Executor,
            project: "arc".to_owned(),
            brief: "fix the failing test".to_owned(),
            budget: None,
        });
        supervisor.shutdown().await;

        let system = executor_provider.requests()[0]
            .system
            .clone()
            .expect("a job runner gets a system prompt");
        assert!(
            system.contains("coding agent") && system.contains(&root.display().to_string()),
            "{system}"
        );
        assert!(system.contains("Use jj, not git."), "{system}");
    }

    #[tokio::test]
    async fn a_project_without_agents_md_gets_the_preamble_only() {
        let dir = TempDir::new().expect("temp dir");
        let root = dir.path().join("proj");
        std::fs::create_dir_all(&root).expect("mkdir proj");

        let concierge_provider = ScriptedProvider::scripted(vec![]);
        let executor_provider = ScriptedProvider::scripted(vec![done_reply("on it")]);
        let engine = engine_for_project(&dir, &root);
        let child_id = child_session(&engine, &concierge_provider);

        let runners =
            BTreeMap::from([(SessionRole::Executor, executor_runner(&executor_provider))]);
        let supervisor = Supervisor::new(Arc::clone(&engine), runners)
            .with_projects(BTreeMap::from([("arc".to_owned(), root.clone())]));

        supervisor.spawn(DispatchedJob {
            session_id: child_id.clone(),
            parent_session: "s-parent".to_owned(),
            role: SessionRole::Executor,
            project: "arc".to_owned(),
            brief: "fix the failing test".to_owned(),
            budget: None,
        });
        supervisor.shutdown().await;

        let system = executor_provider.requests()[0]
            .system
            .clone()
            .expect("a job runner gets a system prompt even with no AGENTS.md");
        assert_eq!(system, job_preamble(&root));
    }

    #[tokio::test]
    async fn a_jobs_system_prompt_is_byte_identical_across_its_turns() {
        let dir = TempDir::new().expect("temp dir");
        let root = dir.path().join("proj");
        std::fs::create_dir_all(&root).expect("mkdir proj");
        std::fs::write(root.join("AGENTS.md"), "Keep commits small.\n").expect("write AGENTS.md");

        let concierge_provider = ScriptedProvider::scripted(vec![]);
        let notify = Arc::new(tokio::sync::Notify::new());
        let executor_provider = ScriptedProvider::scripted_steps(vec![
            Step::Gated {
                before: vec![Ok(CompletionDelta::Text("working".to_owned()))],
                notify: Arc::clone(&notify),
                after: vec![Ok(CompletionDelta::Done {
                    usage: usage(),
                    stop: Stop::EndTurn,
                })],
            },
            Step::Immediate(done_reply("steer reply")),
        ]);
        let engine = engine_for_project(&dir, &root);
        let child_id = child_session(&engine, &concierge_provider);

        let runners =
            BTreeMap::from([(SessionRole::Executor, executor_runner(&executor_provider))]);
        let supervisor = Supervisor::new(Arc::clone(&engine), runners)
            .with_projects(BTreeMap::from([("arc".to_owned(), root.clone())]));

        supervisor.spawn(DispatchedJob {
            session_id: child_id.clone(),
            parent_session: "s-parent".to_owned(),
            role: SessionRole::Executor,
            project: "arc".to_owned(),
            brief: "fix the failing test".to_owned(),
            budget: None,
        });
        wait_for_message_count(dir.path(), &child_id, 1).await;
        assert!(supervisor.steer(&child_id, "also check the linter"));
        notify.notify_one();
        supervisor.shutdown().await;

        let requests = executor_provider.requests();
        assert_eq!(requests.len(), 2, "the initial turn and the steered turn");
        assert_eq!(
            requests[0].system, requests[1].system,
            "the system prompt is built once at spawn, not per turn"
        );
    }
}
