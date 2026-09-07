use std::path::Path;

use tracing::warn;

use crate::identity;

const DEVELOPMENT_LOOP: &str = "For cross-layer changes, define handoff interfaces before delegating. Build the \
     smallest end-to-end slice and get its focused tests passing before expanding. \
     Run checks spanning a child's files only after its handback says they are ready. \
     Inspect failure details before changing code; run the full suite after integration.";

fn job_preamble(root: &Path) -> String {
    format!(
        "You are a coding agent inside ARC's harness, working non-interactively \
         in {}. Be concise. Show file paths clearly. Comments only where AGENTS.md allows \
         one. When a brief asks for commits and holds several tasks, commit each \
         task on its own. When you are done, your final message is the job's \
         report. Edit only the files assigned in the brief. Workspace-wide formatting \
         belongs to integration: run it only if the brief assigns you integration and \
         confirms other writers have stopped. Otherwise report that formatting is needed.\n\n\
         {DEVELOPMENT_LOOP}",
        root.display()
    )
}

fn direct_preamble(root: &Path) -> String {
    format!(
        "You are a coding agent inside ARC's harness, working interactively with \
         the user in {}. Be concise. Show file paths clearly.\n\n\
         You do the work yourself. Dispatch only for work that should run beside you \
         or after the user leaves. \
         When you hand off the whole task, end your reply; the handback arrives on its own. \
         When you delegate part of a task, continue independent work. Do not edit \
         the same files as a running child. \
         Assign disjoint files in each brief. Reserve workspace-wide formatting for \
         integration after all writers have stopped; children report formatting needed. \
         Briefs are self-contained: the child sees nothing of this session. \
         Check a handback against the workspace with your own tools before \
         repeating it.\n\n{DEVELOPMENT_LOOP}",
        root.display()
    )
}

fn with_agents_md(root: &Path, preamble: String) -> String {
    match identity::load(&root.join("AGENTS.md")) {
        Ok(Some(agents)) => format!("{preamble}\n\n{agents}"),
        Ok(None) => preamble,
        Err(error) => {
            warn!(root = %root.display(), %error, "could not read AGENTS.md; using the preamble only");
            preamble
        }
    }
}

/// The job preamble, plus AGENTS.md. Built once at spawn so it stays
/// byte-stable for the job's lifetime.
pub(super) fn job_system_prompt(root: &Path) -> String {
    with_agents_md(root, job_preamble(root))
}

/// Built once when a session's task starts, like a job's, so the prefix
/// the prompt cache matches on stays byte-stable for every turn the task
/// runs.
pub(crate) fn direct_system_prompt(root: &Path, identity: Option<&str>) -> String {
    let preamble = match identity {
        Some(identity) => format!("{}\n\n{}", direct_preamble(root), identity.trim_end()),
        None => direct_preamble(root),
    };
    with_agents_md(root, preamble)
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
        child_session, engine_for_project, executor_runner, steer, wait_for_message_count,
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
        let supervisor = Supervisor::for_test(Arc::clone(&engine), runners)
            .with_projects(BTreeMap::from([("arc".to_owned(), root.clone().into())]));

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
        let supervisor = Supervisor::for_test(Arc::clone(&engine), runners)
            .with_projects(BTreeMap::from([("arc".to_owned(), root.clone().into())]));

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

    #[test]
    fn direct_system_prompt_says_interactive_and_carries_agents_md() {
        let dir = TempDir::new().expect("temp dir");
        let root = dir.path().join("proj");
        std::fs::create_dir_all(&root).expect("mkdir proj");
        std::fs::write(root.join("AGENTS.md"), "Use jj, not git.\n").expect("write AGENTS.md");

        let system = direct_system_prompt(&root, None);

        assert!(
            system.contains("working interactively with the user"),
            "{system}"
        );
        assert!(!system.contains("job's report"), "{system}");
        assert!(system.contains("Use jj, not git."), "{system}");
    }

    #[test]
    fn a_direct_prompt_layers_identity_between_the_preamble_and_agents_md() {
        let dir = TempDir::new().expect("temp dir");
        let root = dir.path().join("proj");
        std::fs::create_dir_all(&root).expect("mkdir proj");
        std::fs::write(root.join("AGENTS.md"), "Use jj, not git.\n").expect("write AGENTS.md");

        let system = direct_system_prompt(&root, Some("You are ARC.\n"));

        let preamble = system.find("coding agent").expect("preamble");
        let identity = system.find("You are ARC.").expect("identity");
        let agents = system.find("Use jj, not git.").expect("AGENTS.md");
        assert!(preamble < identity && identity < agents, "{system}");
    }

    #[test]
    fn only_the_direct_prompt_carries_dispatch_doctrine() {
        let dir = TempDir::new().expect("temp dir");
        let root = dir.path().join("proj");
        std::fs::create_dir_all(&root).expect("mkdir proj");

        let direct = direct_system_prompt(&root, None);
        let job = job_system_prompt(&root);

        assert!(
            direct.contains(
                "You do the work yourself. Dispatch only for work that should run beside you \
                 or after the user leaves. When you hand off the whole task, end your reply; \
                 the handback arrives on its own."
            ),
            "{direct}"
        );
        assert!(!job.contains("You do the work yourself"), "{job}");
        assert!(
            direct.contains("Briefs are self-contained: the child sees nothing of this session."),
            "{direct}"
        );
        assert!(
            direct.contains(
                "Check a handback against the workspace with your own tools before \
                 repeating it."
            ),
            "{direct}"
        );
        assert!(!job.contains("When you dispatch"), "{job}");
    }

    #[test]
    fn direct_and_job_preambles_differ_only_in_who_is_present() {
        let root = std::path::Path::new("/tmp/proj");

        let job = job_preamble(root);
        let direct = direct_preamble(root);

        assert_ne!(job, direct);
        assert!(job.contains("non-interactively"));
        assert!(direct.contains("interactively with the user"));
    }

    #[test]
    fn formatter_coordination_survives_without_project_guidance() {
        let root = Path::new("/project");
        let direct = direct_preamble(root);
        let job = job_preamble(root);
        assert!(direct.contains("Assign disjoint files in each brief."));
        assert!(direct.contains("integration after all writers have stopped"));
        assert!(job.contains("only if the brief assigns you integration"));
        assert!(job.contains("confirms other writers have stopped"));
        assert!(job.contains("Otherwise report that formatting is needed."));
    }

    #[test]
    fn both_coding_doors_keep_the_development_loop_without_changing_identity() {
        let dir = TempDir::new().unwrap();
        let identity = "Human-owned identity.\n";
        let direct = direct_system_prompt(dir.path(), Some(identity));
        let job = job_system_prompt(dir.path());
        for prompt in [&direct, &job] {
            assert_eq!(prompt.matches(DEVELOPMENT_LOOP).count(), 1);
            assert!(prompt.contains("define handoff interfaces before delegating"));
            assert!(prompt.contains("smallest end-to-end slice"));
            assert!(prompt.contains("only after its handback says they are ready"));
            assert!(prompt.contains("full suite after integration"));
        }
        assert!(direct.ends_with(identity.trim_end()));
        assert_eq!(direct, direct_system_prompt(dir.path(), Some(identity)));
        assert_eq!(job, job_system_prompt(dir.path()));
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
        let supervisor = Supervisor::for_test(Arc::clone(&engine), runners)
            .with_projects(BTreeMap::from([("arc".to_owned(), root.clone().into())]));

        supervisor.spawn(DispatchedJob {
            session_id: child_id.clone(),
            parent_session: "s-parent".to_owned(),
            role: SessionRole::Executor,
            project: "arc".to_owned(),
            brief: "fix the failing test".to_owned(),
            budget: None,
        });
        // queued before the brief turn is live, so it runs as a second
        // turn: two turns is what makes the prompt comparison meaningful
        assert!(steer(&supervisor, &child_id, "also check the linter"));
        wait_for_message_count(dir.path(), &child_id, 1).await;
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
