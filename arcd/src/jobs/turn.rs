use std::time::Duration;

use arc_core::footprint::{self, Mark};
use arc_core::provider::Usage;
use arc_core::session::{
    DispatchedJob, EngineEvent, Error as SessionError, Inbound, Reply, Runner,
};
use arc_proto::v1::{Budget, Notification, ReasoningDelta, Source, notification};
use tokio::sync::{mpsc, watch};
use tokio::time::Instant;
use tracing::{debug, info, warn};

use super::handback::{
    handback_cancelled, handback_clean, handback_failed, handback_over_budget, handback_user_reply,
};
use super::status::{JobState, notify_job_changed};
use super::{LiveMap, Shared, TurnEvent, route_cancels, route_continues, spawn_dispatched};

pub(super) const EVENT_BUFFER: usize = 64;
/// How long a turn may go without an engine event (a delta, reasoning, or
/// tool activity) before it's failed outright. A streaming HTTP body that
/// goes quiet without closing would otherwise hold the job open forever.
const JOB_SILENCE_TIMEOUT: Duration = Duration::from_secs(600);

/// A session's work: the message that starts it, and everything the task
/// needs to run turn after turn until its inbox runs dry.
pub(super) struct Task {
    pub(super) job: DispatchedJob,
    /// A dispatched job: it reports to a parent and rides the job strip.
    /// A session the user opened does neither.
    pub(super) dispatched: bool,
    pub(super) source: Source,
    /// The connection that sent the first message, if it asked to watch.
    pub(super) attached: Option<mpsc::Sender<TurnEvent>>,
    pub(super) spent_tokens: u64,
}

/// Everything reaching a turn from outside the engine.
struct Channels<'a> {
    inbox: &'a mut mpsc::UnboundedReceiver<Inbound>,
    drop_rx: &'a mut mpsc::UnboundedReceiver<()>,
    cancel: &'a mut watch::Receiver<bool>,
    attached: &'a mut Option<mpsc::Sender<TurnEvent>>,
}

/// Why a turn loop stopped short of a clean finish: both read the same
/// terminal `JobState::Failed`, so only the handback text tells them apart
/// (the caller wired to `k` gets a distinct reason from a genuine provider
/// failure).
enum EndReason {
    Failed,
    Cancelled,
}

/// One turn per message: the first, then whatever arrived while it ran.
/// The task ends when its inbox is empty at a turn boundary.
pub(super) async fn run_task(
    shared: Shared,
    runner: Runner,
    task: Task,
    mut inbox_rx: mpsc::UnboundedReceiver<Inbound>,
    mut cancel_rx: watch::Receiver<bool>,
    mut drop_rx: mpsc::UnboundedReceiver<()>,
) {
    let Task {
        job,
        dispatched,
        source,
        mut attached,
        mut spent_tokens,
    } = task;
    let session_id = job.session_id.clone();
    let start = Instant::now();
    let mut inbound = Inbound {
        content: job.brief.clone(),
        source,
    };
    let mut first = true;

    loop {
        let mark = if dispatched {
            footprint_mark(&shared, &job).await
        } else {
            None
        };
        let outcome = {
            let mut channels = Channels {
                inbox: &mut inbox_rx,
                drop_rx: &mut drop_rx,
                cancel: &mut cancel_rx,
                attached: &mut attached,
            };
            run_turn(
                &shared,
                &runner,
                &session_id,
                &inbound,
                dispatched,
                &mut channels,
            )
            .await
        };
        let mut reply = match outcome {
            TurnOutcome::Success(reply) => reply,
            TurnOutcome::Failure(error) => {
                end_task(
                    &shared,
                    &job,
                    dispatched,
                    &mut inbox_rx,
                    start,
                    &EndReason::Failed,
                );
                end_attached(&mut attached, error.map(Err)).await;
                return;
            }
            TurnOutcome::Cancelled => {
                end_task(
                    &shared,
                    &job,
                    dispatched,
                    &mut inbox_rx,
                    start,
                    &EndReason::Cancelled,
                );
                end_attached(&mut attached, Some(Err(SessionError::Cancelled))).await;
                return;
            }
        };

        spent_tokens += usage_tokens(reply.usage);
        if dispatched {
            if let Some(info) = shared.statuses.record_tokens(&session_id, spent_tokens) {
                notify_job_changed(shared.notifier.as_ref(), &shared.engine, info);
            }
        }
        spawn_dispatched(&shared, std::mem::take(&mut reply.jobs));
        route_continues(&shared, std::mem::take(&mut reply.continues));
        route_cancels(&shared, std::mem::take(&mut reply.cancels));

        drain_dropped(&mut drop_rx, &mut inbox_rx, &shared, &session_id);
        let cancelled = *cancel_rx.borrow();
        // before any stop is acted on: the turn that crossed a budget still
        // reports what it did
        if dispatched {
            let footprint = footprint_since(&shared, &job, mark).await;
            if !first && inbound.source == Source::User {
                handback_user_reply(&shared, &job, footprint.as_deref());
            } else {
                handback_clean(&shared, &job, footprint.as_deref());
            }
        }
        let breach = if dispatched && !cancelled {
            budget_breach(job.budget.as_ref(), spent_tokens, start.elapsed())
        } else {
            None
        };
        // the task's fate is settled before the connection hears the turn
        // ended: a message sent the instant the answer lands finds either
        // this task's inbox or no task at all, never one on its way out
        let next = if cancelled || breach.is_some() {
            None
        } else {
            next_inbound(&shared.live, &mut inbox_rx, &session_id)
        };
        end_attached(&mut attached, Some(Ok(reply))).await;

        if cancelled {
            end_task(
                &shared,
                &job,
                dispatched,
                &mut inbox_rx,
                start,
                &EndReason::Cancelled,
            );
            return;
        }
        if let Some(breach) = breach {
            warn_over_budget(&session_id, &breach);
            finish_now(&shared, &mut inbox_rx, &session_id);
            if let Some(info) =
                shared
                    .statuses
                    .finish(&session_id, JobState::OverBudget, start.elapsed())
            {
                notify_job_changed(shared.notifier.as_ref(), &shared.engine, info);
            }
            handback_over_budget(&shared, &job, &breach);
            return;
        }

        let Some(next) = next else {
            break;
        };
        if dispatched {
            if let Some(info) = shared.statuses.record_steer_consumed(&session_id) {
                notify_job_changed(shared.notifier.as_ref(), &shared.engine, info);
            }
        }
        inbound = next;
        first = false;
    }

    if dispatched {
        if let Some(info) = shared
            .statuses
            .finish(&session_id, JobState::Finished, start.elapsed())
        {
            notify_job_changed(shared.notifier.as_ref(), &shared.engine, info);
        }
    }
}

/// The next message, or `None` once the inbox is empty and the session has
/// left the live map. The recheck under the lock is what keeps a message
/// from slipping in between the last look and the removal.
fn next_inbound(
    live: &LiveMap,
    inbox_rx: &mut mpsc::UnboundedReceiver<Inbound>,
    session_id: &str,
) -> Option<Inbound> {
    match inbox_rx.try_recv() {
        Ok(inbound) => Some(inbound),
        Err(mpsc::error::TryRecvError::Disconnected) => None,
        Err(mpsc::error::TryRecvError::Empty) => {
            let mut live = live.lock().expect("live");
            if let Ok(inbound) = inbox_rx.try_recv() {
                return Some(inbound);
            }
            live.remove(session_id);
            None
        }
    }
}

async fn end_attached(
    attached: &mut Option<mpsc::Sender<TurnEvent>>,
    ended: Option<Result<Reply, SessionError>>,
) {
    let Some(events) = attached.take() else {
        return;
    };
    if let Some(ended) = ended {
        let _ = events.send(TurnEvent::Ended(ended)).await;
    }
}

async fn footprint_mark(shared: &Shared, job: &DispatchedJob) -> Option<Mark> {
    let project = shared.projects.get(&job.project)?;
    footprint::mark(&project.root, &project.command_prefix).await
}

async fn footprint_since(
    shared: &Shared,
    job: &DispatchedJob,
    mark: Option<Mark>,
) -> Option<String> {
    let project = shared.projects.get(&job.project)?;
    footprint::since(&mark?, &project.root, &project.command_prefix).await
}

fn end_task(
    shared: &Shared,
    job: &DispatchedJob,
    dispatched: bool,
    inbox_rx: &mut mpsc::UnboundedReceiver<Inbound>,
    start: Instant,
    reason: &EndReason,
) {
    finish_now(shared, inbox_rx, &job.session_id);
    if !dispatched {
        return;
    }
    if let Some(info) = shared
        .statuses
        .finish(&job.session_id, JobState::Failed, start.elapsed())
    {
        notify_job_changed(shared.notifier.as_ref(), &shared.engine, info);
    }
    match reason {
        EndReason::Failed => handback_failed(shared, job),
        EndReason::Cancelled => handback_cancelled(shared, job),
    }
}

enum TurnOutcome {
    Success(Reply),
    /// `None` for a turn that went silent: there is no error to report.
    Failure(Option<SessionError>),
    Cancelled,
}

fn usage_tokens(usage: Option<Usage>) -> u64 {
    usage.map_or(0, |usage| {
        u64::from(usage.input_tokens) + u64::from(usage.output_tokens)
    })
}

pub(super) enum BudgetBreach {
    Tokens { spent: u64, allowed: u64 },
    WallClock { elapsed: u64, allowed: u64 },
}

/// A zero field means that dimension is unlimited.
fn budget_breach(
    budget: Option<&Budget>,
    spent_tokens: u64,
    elapsed: Duration,
) -> Option<BudgetBreach> {
    let budget = budget?;
    if budget.total_tokens > 0 && spent_tokens >= budget.total_tokens {
        return Some(BudgetBreach::Tokens {
            spent: spent_tokens,
            allowed: budget.total_tokens,
        });
    }
    if budget.wall_clock_seconds > 0 && elapsed.as_secs() >= u64::from(budget.wall_clock_seconds) {
        return Some(BudgetBreach::WallClock {
            elapsed: elapsed.as_secs(),
            allowed: u64::from(budget.wall_clock_seconds),
        });
    }
    None
}

fn warn_over_budget(session_id: &str, breach: &BudgetBreach) {
    match breach {
        BudgetBreach::Tokens { spent, allowed } => warn!(
            session_id,
            spent, allowed, "job over its token budget; stopping at the next turn boundary"
        ),
        BudgetBreach::WallClock { elapsed, allowed } => warn!(
            session_id,
            elapsed, allowed, "job over its wall-clock budget; stopping at the next turn boundary"
        ),
    }
}

fn track_tool_calls(event: &EngineEvent, pending_tool_calls: &mut u32) {
    match event {
        EngineEvent::ToolCallStarted { .. } => *pending_tool_calls += 1,
        EngineEvent::ToolCallEnded { .. } => {
            *pending_tool_calls = pending_tool_calls.saturating_sub(1);
        }
        _ => {}
    }
}

fn handle_job_event(event: &EngineEvent, shared: &Shared, session_id: &str) {
    match event {
        EngineEvent::ToolCallStarted {
            name,
            arguments_json,
            ..
        } => {
            if let Some(info) = shared
                .statuses
                .record_tool_step(session_id, name, arguments_json)
            {
                notify_job_changed(shared.notifier.as_ref(), &shared.engine, info);
            }
        }
        // no socket initiated this turn, so the broadcast is the only path
        // a watching client has to the model's thinking
        EngineEvent::Reasoning(text) => {
            shared.statuses.touch_engine(session_id);
            if let Some(notifier) = shared.notifier.as_ref() {
                let _ = notifier.send(Notification {
                    event: Some(notification::Event::JobReasoning(ReasoningDelta {
                        session_id: session_id.to_owned(),
                        text: text.clone(),
                    })),
                });
            }
        }
        _ => shared.statuses.touch_engine(session_id),
    }
}

async fn handle_event(
    event: EngineEvent,
    shared: &Shared,
    session_id: &str,
    dispatched: bool,
    pending_tool_calls: &mut u32,
    deadline: &mut Instant,
    attached: &mut Option<mpsc::Sender<TurnEvent>>,
) {
    track_tool_calls(&event, pending_tool_calls);
    if dispatched {
        handle_job_event(&event, shared, session_id);
    }
    debug!(session_id, ?event, "turn event");
    *deadline = Instant::now() + JOB_SILENCE_TIMEOUT;
    let Some(events) = attached.as_ref() else {
        return;
    };
    if events.send(TurnEvent::Engine(event)).await.is_err() {
        info!(session_id, "the connection watching this turn went away");
        *attached = None;
    }
}

enum RawOutcome {
    Sent(Result<Reply, SessionError>),
    SilentTimeout,
    Cancelled,
}

/// Runs one turn to completion, failing it if the engine goes quiet for
/// `JOB_SILENCE_TIMEOUT` with no events, or if `cancel` fires. A pending
/// tool call suspends the timeout instead of tripping it: bash alone is
/// allowed to run silent for up to its own 600s cap, so a tool call in
/// flight is activity, not stall. A cancel drops `send` mid-await — the
/// same shape a crash leaves — instead of waiting the turn out. A
/// `DropSteers` request lands here too, so it empties the queue without
/// waiting for a long turn to finish first.
async fn run_turn(
    shared: &Shared,
    runner: &Runner,
    session_id: &str,
    inbound: &Inbound,
    dispatched: bool,
    channels: &mut Channels<'_>,
) -> TurnOutcome {
    let (events, mut rx) = mpsc::channel(EVENT_BUFFER);
    let send = shared.engine.send_message_from(
        runner,
        Some(session_id),
        &inbound.content,
        inbound.source,
        events,
    );
    tokio::pin!(send);

    let mut deadline = Instant::now() + JOB_SILENCE_TIMEOUT;
    let mut pending_tool_calls: u32 = 0;
    // once a sender's gone, stop polling it: an already-closed channel
    // would otherwise resolve immediately forever and spin the select loop
    let mut cancel_live = true;
    let mut drop_live = true;

    let outcome = loop {
        tokio::select! {
            result = &mut send => {
                // the engine can complete a fully-scripted turn with events
                // still buffered; drain them so fast tools count as steps
                while let Ok(event) = rx.try_recv() {
                    handle_event(event, shared, session_id, dispatched, &mut pending_tool_calls, &mut deadline, channels.attached).await;
                }
                break RawOutcome::Sent(result);
            }
            event = rx.recv() => {
                // send_message drops its sender exactly as it returns, so a
                // `None` here means `send` is already ready on the next poll
                let Some(event) = event else { continue };
                handle_event(event, shared, session_id, dispatched, &mut pending_tool_calls, &mut deadline, channels.attached).await;
            }
            () = tokio::time::sleep_until(deadline), if dispatched && pending_tool_calls == 0 => break RawOutcome::SilentTimeout,
            changed = channels.cancel.changed(), if cancel_live => match changed {
                Ok(()) => break RawOutcome::Cancelled,
                Err(_) => cancel_live = false,
            },
            dropped = channels.drop_rx.recv(), if drop_live => match dropped {
                Some(()) => drop_queued(channels.inbox, shared, session_id),
                None => drop_live = false,
            },
        }
    };

    match outcome {
        RawOutcome::Sent(Ok(reply)) => {
            info!(
                session_id = %session_id,
                input_tokens = reply.usage.map_or(0, |usage| usage.input_tokens),
                output_tokens = reply.usage.map_or(0, |usage| usage.output_tokens),
                "turn completed"
            );
            TurnOutcome::Success(reply)
        }
        RawOutcome::Sent(Err(error)) => {
            warn!(session_id = %session_id, %error, "turn failed");
            TurnOutcome::Failure(Some(error))
        }
        // dropping `send` here abandons the turn mid-flight, the same shape
        // as a crash: any durable but unresolved tool call is left for the
        // orphan-repair a restart already needs, not fixed up here
        RawOutcome::SilentTimeout => {
            warn!(
                session_id = %session_id,
                timeout_secs = JOB_SILENCE_TIMEOUT.as_secs(),
                "job turn silent past the timeout; failing it"
            );
            TurnOutcome::Failure(None)
        }
        RawOutcome::Cancelled => {
            info!(session_id = %session_id, "turn cancelled by the user");
            TurnOutcome::Cancelled
        }
    }
}

fn drop_queued(inbox_rx: &mut mpsc::UnboundedReceiver<Inbound>, shared: &Shared, session_id: &str) {
    let mut dropped = 0_usize;
    while inbox_rx.try_recv().is_ok() {
        dropped += 1;
    }
    if dropped > 0 {
        warn!(session_id, dropped, "dropping queued messages on request");
    }
    if let Some(info) = shared.statuses.drop_queued(session_id) {
        notify_job_changed(shared.notifier.as_ref(), &shared.engine, info);
    }
}

fn drain_dropped(
    drop_rx: &mut mpsc::UnboundedReceiver<()>,
    inbox_rx: &mut mpsc::UnboundedReceiver<Inbound>,
    shared: &Shared,
    session_id: &str,
) {
    let mut requested = false;
    while drop_rx.try_recv().is_ok() {
        requested = true;
    }
    if requested {
        drop_queued(inbox_rx, shared, session_id);
    }
}

fn finish_now(shared: &Shared, inbox_rx: &mut mpsc::UnboundedReceiver<Inbound>, session_id: &str) {
    shared.live.lock().expect("live").remove(session_id);
    let mut dropped = 0_usize;
    while inbox_rx.try_recv().is_ok() {
        dropped += 1;
    }
    shared.statuses.drop_queued(session_id);
    if dropped > 0 {
        warn!(
            session_id,
            dropped, "dropping queued messages as the session's task ends"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use arc_core::log::Log;
    use arc_core::projection::Projection;
    use arc_core::provider::{CompletionDelta, Error as ProviderError, Stop};
    use arc_core::session::{Engine, ProjectSpec};
    use arc_core::store::Store;
    use arc_core::testkit::{ScriptedProvider, Step, call, done_reply, runner, tool_stop, usage};
    use arc_core::tool::Registry;
    use arc_core::tool::ToolSource;
    use arc_core::tool::workspace::{Grant, Mode};
    use arc_proto::v1::{Role, SessionRole, job_info};
    use tempfile::TempDir;
    use tokio::sync::broadcast;

    use crate::jobs::Supervisor;
    use crate::jobs::handback::NO_REPLY;
    use crate::jobs::tests_common::testkit::{
        GatedTool, child_session, child_user_messages, engine_for_project, executor_runner,
        only_job, parent_session, steer, wait_for_message_count, wait_for_tool_call_issued,
    };

    #[tokio::test]
    async fn a_jobs_reasoning_deltas_broadcast_with_its_session_id() {
        let dir = TempDir::new().expect("temp dir");
        let root = dir.path().join("proj");
        std::fs::create_dir_all(&root).expect("mkdir proj");

        let concierge_provider = ScriptedProvider::scripted(vec![]);
        let executor_provider = ScriptedProvider::scripted(vec![vec![
            Ok(CompletionDelta::Reasoning("weighing".to_owned())),
            Ok(CompletionDelta::Reasoning(" options".to_owned())),
            Ok(CompletionDelta::Text("done".to_owned())),
            Ok(CompletionDelta::Done {
                usage: usage(),
                stop: Stop::EndTurn,
            }),
        ]]);

        let (notifier, mut notifications) = broadcast::channel(64);
        let engine = engine_for_project(&dir, &root);
        let child_id = child_session(&engine, &concierge_provider);

        let runners =
            BTreeMap::from([(SessionRole::Executor, executor_runner(&executor_provider))]);
        let supervisor = Supervisor::new(Arc::clone(&engine), runners).with_notifier(notifier);

        supervisor.spawn(DispatchedJob {
            session_id: child_id.clone(),
            parent_session: "s-parent".to_owned(),
            role: SessionRole::Executor,
            project: "arc".to_owned(),
            brief: "fix the failing test".to_owned(),
            budget: None,
        });
        supervisor.shutdown().await;

        let mut reasoning = Vec::new();
        while let Ok(received) = notifications.try_recv() {
            if let Some(arc_proto::v1::notification::Event::JobReasoning(delta)) = received.event {
                assert_eq!(delta.session_id, child_id, "tagged with the job's session");
                reasoning.push(delta.text);
            }
        }
        assert_eq!(
            reasoning.concat(),
            "weighing options",
            "every reasoning delta fanned out, in order"
        );
    }

    #[tokio::test]
    async fn a_failed_turn_drops_queued_steers_and_removes_the_live_entry() {
        let dir = TempDir::new().expect("temp dir");
        let root = dir.path().join("proj");
        std::fs::create_dir_all(&root).expect("mkdir proj");

        let concierge_provider = ScriptedProvider::scripted(vec![]);
        let notify = Arc::new(tokio::sync::Notify::new());
        let executor_provider = ScriptedProvider::scripted_steps(vec![Step::Gated {
            before: Vec::new(),
            notify: Arc::clone(&notify),
            after: vec![Err(ProviderError::InvalidRequest("boom".to_owned()))],
        }]);

        let engine = engine_for_project(&dir, &root);
        let child_id = child_session(&engine, &concierge_provider);

        let runners =
            BTreeMap::from([(SessionRole::Executor, executor_runner(&executor_provider))]);
        let supervisor = Supervisor::new(Arc::clone(&engine), runners);

        supervisor.spawn(DispatchedJob {
            session_id: child_id.clone(),
            parent_session: "s-parent".to_owned(),
            role: SessionRole::Executor,
            project: "arc".to_owned(),
            brief: "fix the failing test".to_owned(),
            budget: None,
        });

        // queued before the failing turn is live, so it waits for a turn
        // of its own — one the job never reaches
        assert!(steer(&supervisor, &child_id, "too late"));
        notify.notify_one();
        supervisor.shutdown().await;

        assert!(
            !steer(&supervisor, &child_id, "later still"),
            "a failed job removed its live entry; nothing is left to read a steer"
        );
        assert_eq!(
            child_user_messages(dir.path(), &child_id),
            [(Role::User, "fix the failing test".to_owned())],
            "the queued steer was dropped, not processed, after the failed turn"
        );
    }

    #[tokio::test]
    async fn a_token_budget_smaller_than_the_brief_turns_usage_stops_the_job_after_that_turn() {
        let dir = TempDir::new().expect("temp dir");
        let root = dir.path().join("proj");
        std::fs::create_dir_all(&root).expect("mkdir proj");

        let concierge_provider = ScriptedProvider::scripted(vec![]);
        let notify = Arc::new(tokio::sync::Notify::new());
        let executor_provider = ScriptedProvider::scripted_steps(vec![Step::Gated {
            before: vec![Ok(CompletionDelta::Text("working".to_owned()))],
            notify: Arc::clone(&notify),
            after: vec![Ok(CompletionDelta::Done {
                usage: usage(),
                stop: Stop::EndTurn,
            })],
        }]);

        let engine = engine_for_project(&dir, &root);
        let child_id = child_session(&engine, &concierge_provider);

        let runners =
            BTreeMap::from([(SessionRole::Executor, executor_runner(&executor_provider))]);
        let supervisor = Supervisor::new(Arc::clone(&engine), runners);

        // usage() reports 8 tokens combined; a cap of 5 is over budget as
        // soon as the brief turn lands, before any steer is even queued
        supervisor.spawn(DispatchedJob {
            session_id: child_id.clone(),
            parent_session: "s-parent".to_owned(),
            role: SessionRole::Executor,
            project: "arc".to_owned(),
            brief: "fix the failing test".to_owned(),
            budget: Some(Budget {
                total_tokens: 5,
                wall_clock_seconds: 0,
            }),
        });

        assert!(
            steer(&supervisor, &child_id, "too late"),
            "the job is still live when queued"
        );
        notify.notify_one();
        supervisor.shutdown().await;

        assert!(
            !steer(&supervisor, &child_id, "later still"),
            "the over-budget job removed its live entry"
        );
        assert_eq!(
            child_user_messages(dir.path(), &child_id),
            [
                (Role::User, "fix the failing test".to_owned()),
                (Role::Assistant, "working".to_owned()),
            ],
            "the brief turn ran to completion; the queued steer was dropped, not processed"
        );
    }

    #[tokio::test]
    async fn a_generous_token_budget_does_not_trip_and_steers_run_normally() {
        let dir = TempDir::new().expect("temp dir");
        let root = dir.path().join("proj");
        std::fs::create_dir_all(&root).expect("mkdir proj");

        let concierge_provider = ScriptedProvider::scripted(vec![]);
        let notify = Arc::new(tokio::sync::Notify::new());
        let executor_provider = ScriptedProvider::scripted_steps(vec![
            Step::Gated {
                before: vec![Ok(CompletionDelta::Text("on it".to_owned()))],
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
        let supervisor = Supervisor::new(Arc::clone(&engine), runners);

        supervisor.spawn(DispatchedJob {
            session_id: child_id.clone(),
            parent_session: "s-parent".to_owned(),
            role: SessionRole::Executor,
            project: "arc".to_owned(),
            brief: "fix the failing test".to_owned(),
            budget: Some(Budget {
                total_tokens: 100_000,
                wall_clock_seconds: 3600,
            }),
        });

        assert!(steer(&supervisor, &child_id, "also check the linter"));
        notify.notify_one();
        supervisor.shutdown().await;

        assert_eq!(
            child_user_messages(dir.path(), &child_id),
            [
                (Role::User, "fix the failing test".to_owned()),
                (Role::Assistant, "on it".to_owned()),
                (Role::User, "also check the linter".to_owned()),
                (Role::Assistant, "steer reply".to_owned()),
            ],
            "a budget with plenty of headroom never trips"
        );
    }

    #[tokio::test]
    async fn a_wall_clock_only_budget_stops_the_job_once_elapsed_time_exceeds_it() {
        let dir = TempDir::new().expect("temp dir");
        let root = dir.path().join("proj");
        std::fs::create_dir_all(&root).expect("mkdir proj");

        tokio::time::pause();

        let concierge_provider = ScriptedProvider::scripted(vec![]);
        let notify = Arc::new(tokio::sync::Notify::new());
        let executor_provider = ScriptedProvider::scripted_steps(vec![Step::Gated {
            before: vec![Ok(CompletionDelta::Text("first".to_owned()))],
            notify: Arc::clone(&notify),
            after: vec![Ok(CompletionDelta::Done {
                usage: usage(),
                stop: Stop::EndTurn,
            })],
        }]);

        let engine = engine_for_project(&dir, &root);
        let child_id = child_session(&engine, &concierge_provider);

        let runners =
            BTreeMap::from([(SessionRole::Executor, executor_runner(&executor_provider))]);
        let supervisor = Supervisor::new(Arc::clone(&engine), runners);

        // total_tokens: 0 means the token dimension is unlimited; only
        // wall-clock is enforced
        supervisor.spawn(DispatchedJob {
            session_id: child_id.clone(),
            parent_session: "s-parent".to_owned(),
            role: SessionRole::Executor,
            project: "arc".to_owned(),
            brief: "fix the failing test".to_owned(),
            budget: Some(Budget {
                total_tokens: 0,
                wall_clock_seconds: 1,
            }),
        });

        assert!(
            steer(&supervisor, &child_id, "too late"),
            "the job is still live when queued"
        );
        // the task's own clock starts when it first runs, so let the brief
        // turn reach the gate before moving the paused clock past the budget
        wait_for_message_count(dir.path(), &child_id, 1).await;
        // the job task is gated on a Notify, not a timer, so advancing the
        // paused clock here only changes what its Instant::elapsed() later
        // reports, it does not let the task run ahead
        tokio::time::advance(Duration::from_secs(2)).await;
        notify.notify_one();
        supervisor.shutdown().await;

        assert!(
            !steer(&supervisor, &child_id, "later still"),
            "the over-budget job removed its live entry"
        );
        assert_eq!(
            child_user_messages(dir.path(), &child_id),
            [
                (Role::User, "fix the failing test".to_owned()),
                (Role::Assistant, "first".to_owned()),
            ],
            "the brief turn ran to completion; the queued steer was dropped, not processed"
        );
    }

    #[tokio::test]
    async fn usage_accumulates_across_turns_and_stops_the_job_once_the_combined_total_crosses_the_cap()
     {
        let dir = TempDir::new().expect("temp dir");
        let root = dir.path().join("proj");
        std::fs::create_dir_all(&root).expect("mkdir proj");

        let concierge_provider = ScriptedProvider::scripted(vec![]);
        let notify = Arc::new(tokio::sync::Notify::new());
        let executor_provider = ScriptedProvider::scripted_steps(vec![
            Step::Gated {
                before: vec![Ok(CompletionDelta::Text("on it".to_owned()))],
                notify: Arc::clone(&notify),
                after: vec![Ok(CompletionDelta::Done {
                    usage: usage(),
                    stop: Stop::EndTurn,
                })],
            },
            Step::Immediate(done_reply("first steer done")),
        ]);

        let engine = engine_for_project(&dir, &root);
        let child_id = child_session(&engine, &concierge_provider);

        let runners =
            BTreeMap::from([(SessionRole::Executor, executor_runner(&executor_provider))]);
        let supervisor = Supervisor::new(Arc::clone(&engine), runners);

        // usage() reports 8 tokens per turn: the brief alone (8) stays under
        // a cap of 10, but the brief plus the first steer (16) crosses it,
        // so the check before the second steer is what stops the job
        supervisor.spawn(DispatchedJob {
            session_id: child_id.clone(),
            parent_session: "s-parent".to_owned(),
            role: SessionRole::Executor,
            project: "arc".to_owned(),
            brief: "fix the failing test".to_owned(),
            budget: Some(Budget {
                total_tokens: 10,
                wall_clock_seconds: 0,
            }),
        });

        assert!(steer(&supervisor, &child_id, "first steer"));
        assert!(steer(&supervisor, &child_id, "second steer"));
        notify.notify_one();
        supervisor.shutdown().await;

        assert!(
            !steer(&supervisor, &child_id, "third steer"),
            "the over-budget job removed its live entry"
        );
        assert_eq!(
            child_user_messages(dir.path(), &child_id),
            [
                (Role::User, "fix the failing test".to_owned()),
                (Role::Assistant, "on it".to_owned()),
                (Role::User, "first steer".to_owned()),
                (Role::Assistant, "first steer done".to_owned()),
            ],
            "the first steer ran; the second never did once the combined usage crossed the cap"
        );
    }

    #[tokio::test]
    async fn a_provider_silent_past_the_timeout_fails_the_turn_and_hands_back_that_it_failed() {
        tokio::time::pause();

        let dir = TempDir::new().expect("temp dir");
        let root = dir.path().join("proj");
        std::fs::create_dir_all(&root).expect("mkdir proj");

        let concierge_provider = ScriptedProvider::scripted(vec![]);
        // never notified: the stream never yields anything at all
        let gate = Arc::new(tokio::sync::Notify::new());
        let executor_provider = ScriptedProvider::scripted_steps(vec![Step::Gated {
            before: Vec::new(),
            notify: gate,
            after: Vec::new(),
        }]);

        let engine = engine_for_project(&dir, &root);
        let parent_id = parent_session(&engine, &concierge_provider);
        let child_id = child_session(&engine, &concierge_provider);

        let runners =
            BTreeMap::from([(SessionRole::Executor, executor_runner(&executor_provider))]);
        let supervisor = Supervisor::new(Arc::clone(&engine), runners);

        supervisor.spawn(DispatchedJob {
            session_id: child_id.clone(),
            parent_session: parent_id.clone(),
            role: SessionRole::Executor,
            project: "arc".to_owned(),
            brief: "fix the failing test".to_owned(),
            budget: None,
        });

        wait_for_message_count(dir.path(), &child_id, 1).await;
        tokio::time::advance(JOB_SILENCE_TIMEOUT + Duration::from_secs(1)).await;
        supervisor.shutdown().await;

        let job = only_job(supervisor.list());
        assert_eq!(
            job.state,
            job_info::State::Failed as i32,
            "a stream silent past the timeout fails the turn"
        );
        assert_eq!(
            child_user_messages(dir.path(), &parent_id),
            [(
                Role::User,
                format!("Job {child_id} stopped: the turn failed.\n{NO_REPLY}")
            )],
            "the silence timeout hands back like any other failed turn"
        );
    }

    #[tokio::test]
    async fn steady_events_past_the_timeout_duration_never_trip_the_silence_timeout() {
        tokio::time::pause();

        let dir = TempDir::new().expect("temp dir");
        let root = dir.path().join("proj");
        std::fs::create_dir_all(&root).expect("mkdir proj");

        let concierge_provider = ScriptedProvider::scripted(vec![]);
        let gate1 = Arc::new(tokio::sync::Notify::new());
        let gate2 = Arc::new(tokio::sync::Notify::new());
        // two silent gaps, each under JOB_SILENCE_TIMEOUT but summing well
        // past it: only a single gap that long should ever trip the job
        let executor_provider = ScriptedProvider::scripted_steps(vec![
            Step::Immediate(vec![
                Ok(CompletionDelta::Text("chunk1".to_owned())),
                Ok(call("c1", 0, "missing_tool", "{}")),
                Ok(tool_stop()),
            ]),
            Step::Gated {
                before: Vec::new(),
                notify: Arc::clone(&gate1),
                after: vec![
                    Ok(CompletionDelta::Text("chunk2".to_owned())),
                    Ok(call("c2", 0, "missing_tool", "{}")),
                    Ok(tool_stop()),
                ],
            },
            Step::Gated {
                before: Vec::new(),
                notify: Arc::clone(&gate2),
                after: vec![
                    Ok(CompletionDelta::Text("chunk3".to_owned())),
                    Ok(CompletionDelta::Done {
                        usage: usage(),
                        stop: Stop::EndTurn,
                    }),
                ],
            },
        ]);

        let engine = engine_for_project(&dir, &root);
        let child_id = child_session(&engine, &concierge_provider);

        let runners =
            BTreeMap::from([(SessionRole::Executor, executor_runner(&executor_provider))]);
        let supervisor = Supervisor::new(Arc::clone(&engine), runners);

        supervisor.spawn(DispatchedJob {
            session_id: child_id.clone(),
            parent_session: "s-parent".to_owned(),
            role: SessionRole::Executor,
            project: "arc".to_owned(),
            brief: "fix the failing test".to_owned(),
            budget: None,
        });

        // each gap is well under JOB_SILENCE_TIMEOUT on its own; only their
        // sum (800s) exceeds it
        let gap = Duration::from_secs(400);
        wait_for_message_count(dir.path(), &child_id, 2).await;
        tokio::time::advance(gap).await;
        gate1.notify_one();
        wait_for_message_count(dir.path(), &child_id, 3).await;
        tokio::time::advance(gap).await;
        gate2.notify_one();
        supervisor.shutdown().await;

        assert_eq!(
            only_job(supervisor.list()).state,
            job_info::State::Finished as i32,
            "events kept resetting the deadline, so the long total turn never tripped"
        );
    }

    #[tokio::test]
    async fn a_pending_tool_call_suspends_the_silence_timeout_while_it_runs() {
        tokio::time::pause();

        let dir = TempDir::new().expect("temp dir");
        let root = dir.path().join("proj");
        std::fs::create_dir_all(&root).expect("mkdir proj");

        let tool_gate = Arc::new(tokio::sync::Notify::new());
        let executor_provider = ScriptedProvider::scripted_steps(vec![
            Step::Immediate(vec![Ok(call("c1", 0, "slow_tool", "{}")), Ok(tool_stop())]),
            Step::Immediate(done_reply("finished after the slow tool")),
        ]);

        let mut registry = Registry::new(512);
        registry.register(Box::new(GatedTool {
            notify: Arc::clone(&tool_gate),
        }));
        let log = Log::open(dir.path()).expect("open log");
        let projection = Projection::in_memory().expect("open projection");
        let engine = Arc::new(
            Engine::new(Store::new(log, projection), registry).with_projects(BTreeMap::from([(
                "arc".to_owned(),
                ProjectSpec {
                    sources: vec![ToolSource::Builtin],
                    grants: vec![Grant::new(&root, Mode::ReadWrite)],
                    command_prefix: Vec::new(),
                },
            )])),
        );

        let bootstrap_provider = ScriptedProvider::scripted(vec![]);
        let child_id = engine
            .create_bound_session(
                &runner(&bootstrap_provider),
                "arc",
                SessionRole::Executor,
                None,
            )
            .expect("create the child durably");

        let runners =
            BTreeMap::from([(SessionRole::Executor, executor_runner(&executor_provider))]);
        let supervisor = Supervisor::new(Arc::clone(&engine), runners);

        supervisor.spawn(DispatchedJob {
            session_id: child_id.clone(),
            parent_session: "s-parent".to_owned(),
            role: SessionRole::Executor,
            project: "arc".to_owned(),
            brief: "run the slow tool".to_owned(),
            budget: None,
        });

        wait_for_tool_call_issued(dir.path(), &child_id).await;
        // well past JOB_SILENCE_TIMEOUT, but the tool call is still pending
        tokio::time::advance(JOB_SILENCE_TIMEOUT * 2).await;
        tool_gate.notify_one();
        supervisor.shutdown().await;

        assert_eq!(
            only_job(supervisor.list()).state,
            job_info::State::Finished as i32,
            "a pending tool call is activity, not silence"
        );
    }
}
