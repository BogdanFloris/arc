#![cfg(test)]

use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use arc_core::log::Log;
use arc_core::projection::Projection;
use arc_core::provider::{Provider, Thinking, ToolDefinition};
use arc_core::session::{Engine, ProjectSpec, Runner};
use arc_core::store::Store;
use arc_core::testkit::{ScriptedProvider, replay_log, runner};
use arc_core::tool::workspace::{Grant, Mode};
use arc_core::tool::{Registry, Tool, ToolReply, ToolSource, TurnContext};
use arc_proto::v1::{JobInfo, Notification, Role, SessionRole, notification};
use tempfile::TempDir;
use tokio::sync::broadcast;

pub(crate) mod testkit {
    use super::*;

    pub(crate) struct GatedTool {
        pub(crate) notify: Arc<tokio::sync::Notify>,
    }

    impl Tool for GatedTool {
        fn definition(&self) -> ToolDefinition {
            ToolDefinition {
                name: "slow_tool".to_owned(),
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
            _ctx: TurnContext,
        ) -> Pin<Box<dyn Future<Output = ToolReply> + Send + '_>> {
            let notify = Arc::clone(&self.notify);
            Box::pin(async move {
                notify.notified().await;
                ToolReply::ok("done".to_owned())
            })
        }
    }

    // "test-model": matches what a bootstrap/concierge runner's own identity
    // records for a child, since these engines never configure role_identities
    pub(crate) fn executor_runner(provider: &Arc<ScriptedProvider>) -> Runner {
        Runner {
            role: SessionRole::Executor,
            provider: Arc::clone(provider) as Arc<dyn Provider>,
            model: "test-model".to_owned(),
            thinking: Thinking::Default,
            system: None,
        }
    }

    pub(crate) fn child_session(engine: &Engine, concierge: &Arc<ScriptedProvider>) -> String {
        engine
            .create_bound_session(&runner(concierge), "arc", SessionRole::Executor, None)
            .expect("create the child durably, as dispatch already does")
    }

    pub(crate) fn parent_session(engine: &Engine, concierge: &Arc<ScriptedProvider>) -> String {
        engine
            .create_bound_session(&runner(concierge), "arc", SessionRole::Concierge, None)
            .expect("create the parent durably")
    }

    pub(crate) fn engine_for_project(dir: &TempDir, root: &std::path::Path) -> Arc<Engine> {
        let log = Log::open(dir.path()).expect("open log");
        let projection = Projection::in_memory().expect("open projection");
        Arc::new(
            Engine::new(Store::new(log, projection), Registry::new(512)).with_projects(
                BTreeMap::from([(
                    "arc".to_owned(),
                    ProjectSpec {
                        sources: Vec::new(),
                        grants: vec![Grant::new(root, Mode::ReadWrite)],
                        command_prefix: Vec::new(),
                    },
                )]),
            ),
        )
    }

    pub(crate) fn engine_for_project_notified(
        dir: &TempDir,
        root: &std::path::Path,
        notifier: broadcast::Sender<Notification>,
    ) -> Arc<Engine> {
        let log = Log::open(dir.path()).expect("open log");
        let projection = Projection::in_memory().expect("open projection");
        Arc::new(
            Engine::new(Store::new(log, projection), Registry::new(512))
                .with_projects(BTreeMap::from([(
                    "arc".to_owned(),
                    ProjectSpec {
                        sources: Vec::new(),
                        grants: vec![Grant::new(root, Mode::ReadWrite)],
                        command_prefix: Vec::new(),
                    },
                )]))
                .with_notifier(notifier),
        )
    }

    /// The next `job_changed` notification, skipping any `session_appended`
    /// pushes (e.g. from `child_session`'s own durable creation) in between.
    pub(crate) async fn job_changed(
        notifications: &mut broadcast::Receiver<Notification>,
    ) -> JobInfo {
        loop {
            let received = tokio::time::timeout(Duration::from_secs(5), notifications.recv())
                .await
                .expect("a notification within the timeout")
                .expect("the notifier stays open");
            if let Some(notification::Event::JobChanged(info)) = received.event {
                return info;
            }
        }
    }

    pub(crate) fn child_user_messages(
        dir: &std::path::Path,
        session_id: &str,
    ) -> Vec<(Role, String)> {
        replay_log(dir)
            .into_iter()
            .filter_map(|event| match event {
                arc_proto::v1::session_event::Event::MessageAppended(m)
                    if m.session_id == session_id =>
                {
                    Some((Role::try_from(m.role).expect("a known role"), m.content))
                }
                _ => None,
            })
            .collect()
    }

    /// Polls the log rather than sleeping a fixed duration: waits for actual
    /// state, not a guessed timing window.
    pub(crate) async fn wait_for_message_count(
        dir: &std::path::Path,
        session_id: &str,
        want: usize,
    ) {
        for _ in 0..400 {
            if child_user_messages(dir, session_id).len() >= want {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        panic!("timed out waiting for {want} messages in session {session_id}");
    }

    /// Polls the log for a tool call landing, so a test can advance a paused
    /// clock only once the tool is genuinely dispatched and running.
    pub(crate) async fn wait_for_tool_call_issued(dir: &std::path::Path, session_id: &str) {
        for _ in 0..400 {
            let issued = replay_log(dir).into_iter().any(|event| {
                matches!(
                    event,
                    arc_proto::v1::session_event::Event::ToolCallIssued(call)
                        if call.session_id == session_id
                )
            });
            if issued {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        panic!("timed out waiting for a tool call in session {session_id}");
    }

    pub(crate) fn only_job(listed: Vec<JobInfo>) -> JobInfo {
        assert_eq!(listed.len(), 1, "got: {listed:?}");
        listed.into_iter().next().expect("one job")
    }
}
