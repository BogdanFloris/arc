use std::future::Future;
use std::net::SocketAddr;
use std::ops::ControlFlow;
use std::sync::Arc;
use std::time::Duration;

use arc_core::projection::{Reader, ReviewItem, SessionSummary};
use arc_core::provider::role_label;
use arc_core::session::{Engine, EngineEvent, Error as SessionError, Reply};
use arc_core::store::Error as StoreError;
use arc_proto::v1::{
    ClientFrame, CreateSession, Delta, Error as WireError, JobList, MemoryReviewItem,
    MemoryReviewItems, MessageAccepted, Notification, ProjectList, ReasoningDelta,
    ReviewPredecessor, SendMessage, ServerFrame, SessionHistory, SessionInfo, SessionList,
    SessionRole, Source, StreamEnd, ToolCallEnded, ToolCallStarted, branch_marked, client_frame,
    server_frame,
};
use futures::{SinkExt as _, StreamExt as _};
use prost::Message as _;
use prost_types::Timestamp;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, mpsc, watch};
use tokio::task::JoinSet;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::Message as WsMessage;
use tracing::{info, warn};

use crate::jobs::{SendOutcome, Supervisor, TurnEvent};

type Socket = WebSocketStream<TcpStream>;

const SHUTDOWN_GRACE: Duration = Duration::from_secs(10);

pub async fn serve(
    listener: TcpListener,
    engine: Arc<Engine>,
    reads: Arc<Reader>,
    supervisor: Arc<Supervisor>,
    notifier: broadcast::Sender<Notification>,
    shutdown: impl Future<Output = ()> + Send,
) {
    let (closing, closing_rx) = watch::channel(false);
    let mut connections = JoinSet::new();
    tokio::pin!(shutdown);

    loop {
        tokio::select! {
            () = &mut shutdown => break,
            accepted = listener.accept() => match accepted {
                Ok((stream, peer)) => {
                    while connections.try_join_next().is_some() {}
                    connections.spawn(connection(
                        stream,
                        peer,
                        Arc::clone(&engine),
                        Arc::clone(&reads),
                        Arc::clone(&supervisor),
                        notifier.clone(),
                        closing_rx.clone(),
                    ));
                }
                Err(error) => warn!(%error, "accepting a connection failed"),
            },
        }
    }

    info!(connections = connections.len(), "no longer accepting");
    let _ = closing.send(true);
    drain(&mut connections).await;
}

async fn drain(connections: &mut JoinSet<()>) {
    let expired = tokio::time::timeout(SHUTDOWN_GRACE, async {
        while connections.join_next().await.is_some() {}
    })
    .await
    .is_err();

    if expired {
        warn!(
            remaining = connections.len(),
            "shutdown grace expired; abandoning connections"
        );
    }
}

struct Subscription {
    request_id: u64,
    rx: broadcast::Receiver<Notification>,
}

#[tracing::instrument(name = "server.connection", skip_all, fields(peer = %peer))]
async fn connection(
    stream: TcpStream,
    peer: SocketAddr,
    engine: Arc<Engine>,
    reads: Arc<Reader>,
    supervisor: Arc<Supervisor>,
    notifier: broadcast::Sender<Notification>,
    mut closing: watch::Receiver<bool>,
) {
    let mut ws = match tokio_tungstenite::accept_async(stream).await {
        Ok(ws) => ws,
        Err(error) => {
            warn!(%error, "websocket handshake failed");
            return;
        }
    };
    info!("client connected");

    let mut subscription: Option<Subscription> = None;

    loop {
        tokio::select! {
            () = told_to_close(&mut closing) => {
                info!("closing an idle connection");
                let _ = ws.close(None).await;
                break;
            }
            message = ws.next() => {
                match message {
                    Some(Ok(WsMessage::Binary(bytes))) => match ClientFrame::decode(bytes) {
                        Ok(frame) => {
                            if request(
                                &mut ws,
                                &engine,
                                &reads,
                                &supervisor,
                                &notifier,
                                &mut subscription,
                                frame,
                            )
                            .await
                            .is_break()
                            {
                                break;
                            }
                        }
                        Err(error) => {
                            warn!(%error, "undecodable client frame");
                            refuse(&mut ws, 0).await;
                            break;
                        }
                    },
                    Some(Ok(WsMessage::Text(_))) => {
                        warn!("text message on a binary protocol");
                        refuse(&mut ws, 0).await;
                        break;
                    }
                    Some(Ok(WsMessage::Ping(_) | WsMessage::Pong(_) | WsMessage::Frame(_))) => {}
                    Some(Ok(WsMessage::Close(_))) | None => break,
                    Some(Err(error)) => {
                        warn!(%error, "websocket read failed");
                        break;
                    }
                }
            }
            received = next_notification(&mut subscription) => {
                let sub = subscription.as_ref().expect("resolves only once subscribed");
                if !push_notification(&mut ws, sub.request_id, received).await {
                    break;
                }
            }
        }
    }

    info!("client disconnected");
}

async fn told_to_close(closing: &mut watch::Receiver<bool>) {
    let _ = closing.wait_for(|closing| *closing).await;
}

async fn next_notification(
    subscription: &mut Option<Subscription>,
) -> Result<Notification, broadcast::error::RecvError> {
    match subscription {
        Some(sub) => sub.rx.recv().await,
        None => std::future::pending().await,
    }
}

async fn push_notification(
    ws: &mut Socket,
    request_id: u64,
    received: Result<Notification, broadcast::error::RecvError>,
) -> bool {
    match received {
        Ok(notification) => {
            send_frame(
                ws,
                request_id,
                server_frame::Msg::Notification(notification),
            )
            .await
        }
        Err(broadcast::error::RecvError::Lagged(skipped)) => {
            warn!(
                skipped,
                "a subscriber lagged behind the notification broadcast"
            );
            true
        }
        Err(broadcast::error::RecvError::Closed) => true,
    }
}

#[tracing::instrument(
    name = "server.request",
    skip_all,
    fields(request_id = frame.request_id, kind = kind(&frame)),
)]
async fn request(
    ws: &mut Socket,
    engine: &Engine,
    reads: &Reader,
    supervisor: &Supervisor,
    notifier: &broadcast::Sender<Notification>,
    subscription: &mut Option<Subscription>,
    frame: ClientFrame,
) -> ControlFlow<()> {
    let reply = match frame.msg {
        Some(client_frame::Msg::SendMessage(send)) => {
            return send_message(ws, supervisor, frame.request_id, send).await;
        }
        Some(client_frame::Msg::ListSessions(_)) => reads
            .sessions()
            .map(|sessions| {
                server_frame::Msg::SessionList(SessionList {
                    sessions: sessions.iter().map(session_info).collect(),
                })
            })
            .map_err(SessionError::from),
        Some(client_frame::Msg::FetchHistory(fetch)) => session_history(reads, &fetch.session_id)
            .map(server_frame::Msg::SessionHistory)
            .map_err(SessionError::from),
        Some(client_frame::Msg::FetchStatus(fetch)) => Ok(
            match session_status(reads, supervisor, &fetch.session_id).await {
                Ok(status) => server_frame::Msg::SessionStatus(status),
                Err(error) => error_frame("status_unavailable", &error),
            },
        ),
        Some(client_frame::Msg::MemoryReviewList(list)) => reads
            .review_items(list.since_micros)
            .map(|items| {
                server_frame::Msg::MemoryReviewItems(MemoryReviewItems {
                    items: items.into_iter().map(review_item).collect(),
                })
            })
            .map_err(SessionError::from),
        Some(client_frame::Msg::MemoryReviewAccept(accept)) => engine
            .review_accept(&accept.record_id)
            .map(|()| accepted(String::new())),
        Some(client_frame::Msg::MemoryReviewDelete(delete)) => engine
            .review_delete(&delete.record_id)
            .map(|()| accepted(String::new())),
        Some(client_frame::Msg::ListJobs(_)) => Ok(server_frame::Msg::JobList(JobList {
            jobs: supervisor.list(),
        })),
        Some(client_frame::Msg::ListProjects(_)) => {
            Ok(server_frame::Msg::ProjectList(ProjectList {
                projects: supervisor.project_list().to_vec(),
            }))
        }
        Some(client_frame::Msg::ListModels(_)) => {
            engine.model_list().map(server_frame::Msg::ModelList)
        }
        Some(client_frame::Msg::SelectModel(select)) => {
            let role = SessionRole::try_from(select.role).unwrap_or_default();
            engine
                .select_model(role, &select.choice)
                .and_then(|()| engine.model_list())
                .map(server_frame::Msg::ModelList)
        }
        Some(client_frame::Msg::CancelJob(cancel)) => Ok(job_control(
            &cancel.session_id,
            supervisor.cancel(&cancel.session_id),
        )),
        Some(client_frame::Msg::DropSteers(drop)) => Ok(job_control(
            &drop.session_id,
            supervisor.drop_steers(&drop.session_id),
        )),
        Some(client_frame::Msg::CreateSession(create)) => {
            Ok(create_session(engine, supervisor, create))
        }
        Some(client_frame::Msg::ForkSession(fork)) => engine
            .fork_session_with_choice(&fork.session_id, fork.fork_point, &fork.choice)
            .map(accepted),
        Some(client_frame::Msg::MarkBranch(mark)) => {
            let disposition =
                branch_marked::Disposition::try_from(mark.disposition).unwrap_or_default();
            engine
                .mark_branch(&mark.session_id, disposition)
                .map(|()| accepted(String::new()))
        }
        Some(client_frame::Msg::CancelTurn(cancel)) => {
            Ok(if engine.cancel_turn(&cancel.session_id) {
                accepted(cancel.session_id)
            } else {
                error_frame(
                    "no_turn",
                    format!("no turn is running on session {}", cancel.session_id),
                )
            })
        }
        Some(client_frame::Msg::Subscribe(_)) => {
            *subscription = Some(Subscription {
                request_id: frame.request_id,
                rx: notifier.subscribe(),
            });
            return ControlFlow::Continue(());
        }
        Some(client_frame::Msg::CompactSession(compact)) => {
            Ok(compact_session(engine, supervisor, &compact.session_id).await)
        }
        None => {
            warn!("client frame with no request");
            refuse(ws, frame.request_id).await;
            return ControlFlow::Break(());
        }
    };
    flow(send_frame(ws, frame.request_id, reply.unwrap_or_else(session_error)).await)
}

async fn send_message(
    ws: &mut Socket,
    supervisor: &Supervisor,
    request_id: u64,
    send: SendMessage,
) -> ControlFlow<()> {
    let session_id = (!send.session_id.is_empty()).then_some(send.session_id.as_str());

    let result = supervisor.send(
        session_id,
        &send.content,
        Source::User,
        send.attachments,
        true,
    );
    match result {
        Ok(SendOutcome::Started { session_id, events }) => {
            forward(
                ws,
                request_id,
                session_id,
                events.expect("requested attached turn"),
            )
            .await
        }
        Ok(SendOutcome::Queued { session_id }) => {
            flow(send_queued(ws, request_id, &session_id).await)
        }
        Err(error) => flow(send_frame(ws, request_id, session_error(error)).await),
    }
}

async fn send_queued(ws: &mut Socket, request_id: u64, session_id: &str) -> bool {
    let accepted = server_frame::Msg::MessageAccepted(MessageAccepted {
        session_id: session_id.to_owned(),
    });
    if !send_frame(ws, request_id, accepted).await {
        return false;
    }
    let end = server_frame::Msg::StreamEnd(StreamEnd {
        session_id: session_id.to_owned(),
        input_tokens: 0,
        output_tokens: 0,
        partial: false,
        step_capped: false,
        grounding_json: String::new(),
        queued: true,
    });
    send_frame(ws, request_id, end).await
}

async fn forward(
    ws: &mut Socket,
    request_id: u64,
    mut session_id: String,
    mut rx: mpsc::Receiver<TurnEvent>,
) -> ControlFlow<()> {
    while let Some(event) = rx.recv().await {
        let event = match event {
            TurnEvent::Engine(event) => event,
            TurnEvent::Ended(ended) => {
                let msg = match ended {
                    Ok(reply) => stream_end(&reply),
                    Err(error) => {
                        warn!(%error, code = error_code(&error), "turn failed");
                        error_frame(error_code(&error), &error)
                    }
                };
                return flow(send_frame(ws, request_id, msg).await);
            }
        };
        let msg = match event {
            EngineEvent::JobsReady { .. } => continue,
            EngineEvent::Accepted { session_id: id } => {
                session_id.clone_from(&id);
                server_frame::Msg::MessageAccepted(MessageAccepted { session_id: id })
            }
            EngineEvent::Delta(text) => server_frame::Msg::Delta(Delta {
                session_id: session_id.clone(),
                text,
            }),
            EngineEvent::Reasoning(text) => server_frame::Msg::ReasoningDelta(ReasoningDelta {
                session_id: session_id.clone(),
                text,
            }),
            EngineEvent::ToolCallStarted {
                call_id,
                index,
                name,
                arguments_json,
            } => server_frame::Msg::ToolCallStarted(ToolCallStarted {
                session_id: session_id.clone(),
                call_id,
                index,
                name,
                arguments_json,
            }),
            EngineEvent::ToolCallEnded {
                call_id,
                outcome,
                content,
            } => server_frame::Msg::ToolCallEnded(ToolCallEnded {
                session_id: session_id.clone(),
                call_id,
                outcome: outcome as i32,
                content,
            }),
        };
        if !send_frame(ws, request_id, msg).await {
            return ControlFlow::Break(());
        }
    }
    flow(
        send_frame(
            ws,
            request_id,
            error_frame("internal", "the turn ended without a reply"),
        )
        .await,
    )
}

#[tracing::instrument(name = "server.session_status", skip_all, fields(session_id))]
async fn session_status(
    reads: &Reader,
    supervisor: &Supervisor,
    session_id: &str,
) -> anyhow::Result<arc_proto::v1::SessionStatus> {
    let session = reads
        .sessions()?
        .into_iter()
        .find(|s| s.id == session_id)
        .ok_or_else(|| anyhow::anyhow!("session not found"))?;
    let mut status = arc_proto::v1::SessionStatus {
        session_id: session_id.to_owned(),
        codex: session.provider == "codex",
        ..Default::default()
    };
    if let Some((context, observed_at)) = reads.context_status(session_id)? {
        status.context = Some(context);
        status.context_observed_at = observed_at;
    }
    if status.codex {
        if let Some(runner) = supervisor.status_runner(
            SessionRole::try_from(session.role).unwrap_or_default(),
            &session.provider,
            &session.model,
        ) {
            if let Ok(Some(allowance)) = runner.provider.allowance().await {
                status.allowance_observed_at = allowance.observed_at;
                status.allowance_stale = allowance.stale;
                status.allowance = allowance
                    .primary
                    .into_iter()
                    .chain(allowance.secondary)
                    .map(|window| arc_proto::v1::AllowanceWindow {
                        remaining_percent: remaining_percent(window.remaining_percent),
                        resets_at: window.resets_at_unix_seconds,
                        window_seconds: window.window_seconds,
                    })
                    .collect();
            }
        }
    }
    Ok(status)
}

#[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
fn remaining_percent(value: f64) -> u32 {
    value.clamp(0.0, 100.0).floor() as u32
}

fn accepted(session_id: String) -> server_frame::Msg {
    server_frame::Msg::MessageAccepted(MessageAccepted { session_id })
}

fn job_control(session_id: &str, succeeded: bool) -> server_frame::Msg {
    if succeeded {
        accepted(session_id.to_owned())
    } else {
        error_frame("unknown_job", format!("no live job named {session_id}"))
    }
}

async fn compact_session(
    engine: &Engine,
    supervisor: &Supervisor,
    session_id: &str,
) -> server_frame::Msg {
    if engine.turn_is_live(session_id) {
        return error_frame(
            "turn_running",
            format!("session {session_id} has a live turn"),
        );
    }
    let turn_id = uuid::Uuid::new_v4().to_string();
    let compacted = match supervisor.turn_runner(session_id) {
        Ok(served_by) => engine.compact(&served_by, session_id, &turn_id).await,
        Err(error) => Err(error),
    };
    compacted.map_or_else(session_error, |_| accepted(session_id.to_owned()))
}

fn create_session(
    engine: &Engine,
    supervisor: &Supervisor,
    create: CreateSession,
) -> server_frame::Msg {
    let role = SessionRole::try_from(create.role).unwrap_or(SessionRole::Unspecified);
    if !matches!(
        role,
        SessionRole::Chat | SessionRole::Code | SessionRole::Executor
    ) {
        error_frame(
            "unsupported_role",
            format!("cannot directly open a {} session", role_label(role)),
        )
    } else if let Some(runner) = supervisor.role_runner(role) {
        let result = if role == SessionRole::Chat && create.project.is_empty() {
            engine.create_session_with_choice(&runner, &create.choice)
        } else if role == SessionRole::Chat {
            Err(SessionError::UnknownProject {
                project: create.project,
            })
        } else {
            engine.create_direct_session_with_choice(&runner, &create.project, role, &create.choice)
        };
        result.map_or_else(session_error, accepted)
    } else {
        error_frame(
            "no_runner",
            format!("no runner is configured for the {} role", role_label(role)),
        )
    }
}

fn review_item(item: ReviewItem) -> MemoryReviewItem {
    MemoryReviewItem {
        record: Some(item.record),
        changed_at_micros: item.changed_at,
        supersedes: item
            .supersedes
            .into_iter()
            .map(|p| ReviewPredecessor {
                id: p.id,
                title: p.title,
            })
            .collect(),
    }
}

async fn refuse(ws: &mut Socket, request_id: u64) {
    send_frame(
        ws,
        request_id,
        error_frame("bad_frame", "not a decodable ClientFrame"),
    )
    .await;
    let _ = ws.close(None).await;
}

async fn send_frame(ws: &mut Socket, request_id: u64, msg: server_frame::Msg) -> bool {
    let frame = ServerFrame {
        request_id,
        msg: Some(msg),
    };
    match ws.send(WsMessage::binary(frame.encode_to_vec())).await {
        Ok(()) => true,
        Err(error) => {
            warn!(%error, "dropping a frame: the client went away");
            false
        }
    }
}

fn stream_end(reply: &Reply) -> server_frame::Msg {
    let usage = reply.usage.unwrap_or_default();
    server_frame::Msg::StreamEnd(StreamEnd {
        session_id: reply.session_id.clone(),
        input_tokens: usage.input_tokens,
        output_tokens: usage.output_tokens,
        partial: reply.partial,
        step_capped: reply.step_capped,
        grounding_json: reply.grounding_json.clone(),
        queued: false,
    })
}

fn session_history(
    reads: &Reader,
    session_id: &str,
) -> Result<SessionHistory, arc_core::projection::Error> {
    let entries = reads.transcript(session_id)?;
    let (parent_session, fork_point) = reads.fork_parent(session_id)?.unwrap_or_default();
    let branches = reads
        .branches_of(session_id)?
        .into_iter()
        .map(
            |(session_id, fork_point, title)| arc_proto::v1::BranchPointer {
                session_id,
                fork_point,
                title,
            },
        )
        .collect();
    Ok(SessionHistory {
        session_id: session_id.to_owned(),
        entries,
        parent_session,
        fork_point,
        branches,
    })
}

fn session_error(error: SessionError) -> server_frame::Msg {
    warn!(%error, code = error_code(&error), "request failed");
    error_frame(error_code(&error), error)
}

fn error_frame(code: &str, msg: impl std::fmt::Display) -> server_frame::Msg {
    server_frame::Msg::Error(WireError {
        code: code.to_owned(),
        msg: msg.to_string(),
    })
}

fn error_code(error: &SessionError) -> &'static str {
    match error {
        SessionError::EmptyMessage => "empty_message",
        SessionError::Attachment(_) => "invalid_attachment",
        SessionError::AttachmentsUnsupported { .. } => "unsupported_attachment",
        SessionError::NoRunner { .. } => "no_runner",
        SessionError::EmptyReply => "empty_reply",
        SessionError::CompactionFailed { .. } => "compaction_failed",
        SessionError::Cancelled => "cancelled",
        SessionError::RoleMismatch { .. } => "role_mismatch",
        SessionError::ModelMismatch { .. } => "model_mismatch",
        SessionError::MissingChoice { .. } => "missing_choice",
        SessionError::AmbiguousChoice { .. } => "ambiguous_choice",
        SessionError::Provider(_) => "provider",
        SessionError::UnknownProject { .. } => "unknown_project",
        SessionError::UnknownChoice { .. } => "unknown_choice",
        SessionError::UnknownSession { .. } => "unknown_session",
        SessionError::InvalidForkPoint { .. } => "invalid_fork_point",
        SessionError::NotABranch { .. } => "not_a_branch",
        SessionError::Store(StoreError::UnknownRecord { .. }) => "unknown_record",
        SessionError::Store(_) | SessionError::Projection(_) | SessionError::Grants { .. } => {
            "internal"
        }
    }
}

fn kind(frame: &ClientFrame) -> &'static str {
    match frame.msg {
        Some(client_frame::Msg::SendMessage(_)) => "send_message",
        Some(client_frame::Msg::ListSessions(_)) => "list_sessions",
        Some(client_frame::Msg::FetchHistory(_)) => "fetch_history",
        Some(client_frame::Msg::FetchStatus(_)) => "fetch_status",
        Some(client_frame::Msg::MemoryReviewList(_)) => "memory_review_list",
        Some(client_frame::Msg::MemoryReviewAccept(_)) => "memory_review_accept",
        Some(client_frame::Msg::MemoryReviewDelete(_)) => "memory_review_delete",
        Some(client_frame::Msg::ListJobs(_)) => "list_jobs",
        Some(client_frame::Msg::ListProjects(_)) => "list_projects",
        Some(client_frame::Msg::ListModels(_)) => "list_models",
        Some(client_frame::Msg::SelectModel(_)) => "select_model",
        Some(client_frame::Msg::Subscribe(_)) => "subscribe",
        Some(client_frame::Msg::CompactSession(_)) => "compact_session",
        Some(client_frame::Msg::CancelJob(_)) => "cancel_job",
        Some(client_frame::Msg::DropSteers(_)) => "drop_steers",
        Some(client_frame::Msg::CreateSession(_)) => "create_session",
        Some(client_frame::Msg::ForkSession(_)) => "fork_session",
        Some(client_frame::Msg::MarkBranch(_)) => "mark_branch",
        Some(client_frame::Msg::CancelTurn(_)) => "cancel_turn",
        None => "unknown",
    }
}

fn session_info(summary: &SessionSummary) -> SessionInfo {
    SessionInfo {
        id: summary.id.clone(),
        title: summary.title.clone(),
        started_at: summary.started_at.map(timestamp),
        preview: summary.preview.clone(),
        last_at: summary.last_at.map(timestamp),
        role: summary.role,
        project: summary.project.clone().unwrap_or_default(),
        dispatched_by: summary.dispatched_by.clone(),
        source: summary.source,
        parent_session: summary.parent_session.clone(),
        disposition: summary.disposition,
        provider: summary.provider.clone(),
        model: summary.model.clone(),
    }
}

fn timestamp(micros: i64) -> Timestamp {
    Timestamp {
        seconds: micros.div_euclid(1_000_000),
        nanos: i32::try_from(micros.rem_euclid(1_000_000) * 1_000).expect("sub-second nanos"),
    }
}

fn flow(connected: bool) -> ControlFlow<()> {
    if connected {
        ControlFlow::Continue(())
    } else {
        ControlFlow::Break(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, VecDeque};
    use std::sync::Mutex as StdMutex;

    use arc_core::log::Log;
    use arc_core::projection::Projection;
    use arc_core::provider::{
        CompletionDelta, CompletionRequest, CompletionStream, Error as ProviderError, Message,
        Stop, ToolCall,
    };
    use arc_core::store::Store;
    use arc_core::tool::{Registry, ToolSource};
    use arc_proto::v1::{
        CancelJob, CancelTurn, CompactSession, DropSteers, Event, FetchHistory, ForkSession,
        HistoryEntry, HistoryMessage, HistoryToolCall, HistoryToolResult, ImageAttachment,
        ListModels, ListSessions, MarkBranch, MemoryEvent, MemoryRecord, MemoryRecordCreated,
        MemoryReviewAccept, MemoryReviewDelete, MemoryReviewList, Notification, ProjectInfo, Role,
        SelectModel, SessionRole, Subscribe, ToolOutcome, event, job_info, memory_event,
        memory_record, notification, session_event,
    };
    use futures::stream;
    use tempfile::TempDir;
    use tokio::sync::oneshot;
    use tokio::task::JoinHandle;
    use tokio_tungstenite::MaybeTlsStream;

    use super::*;
    use arc_core::provider::{Provider, Thinking};
    use arc_core::session::ProjectSpec;
    use arc_core::session::Runner;
    use arc_core::testkit::{Canned, ScriptedProvider, Step, done_reply, replay_log, usage};

    const PATIENCE: Duration = Duration::from_secs(5);

    #[derive(Debug)]
    enum Script {
        Echo,
        Canned(VecDeque<Vec<Result<CompletionDelta, ProviderError>>>),
    }

    #[derive(Debug)]
    struct MockProvider {
        script: StdMutex<Script>,
        captured: StdMutex<Vec<CompletionRequest>>,
    }

    impl MockProvider {
        fn new(script: Script) -> Arc<Self> {
            Arc::new(Self {
                script: StdMutex::new(script),
                captured: StdMutex::new(Vec::new()),
            })
        }

        fn requests(&self) -> Vec<CompletionRequest> {
            self.captured.lock().expect("captured").clone()
        }
    }

    impl Provider for MockProvider {
        fn name(&self) -> &'static str {
            "mock"
        }

        fn supports_images(&self) -> bool {
            true
        }

        fn complete(
            &self,
            request: CompletionRequest,
        ) -> futures::future::BoxFuture<'_, Result<CompletionStream, ProviderError>> {
            Box::pin(async move {
                let items = match &mut *self.script.lock().expect("script") {
                    Script::Echo => {
                        let last = request
                            .messages
                            .iter()
                            .rev()
                            .find_map(|m| match m {
                                Message::Text {
                                    role: Role::User,
                                    content,
                                    ..
                                } => Some(content.clone()),
                                Message::UserWithAttachments { content, .. } => {
                                    Some(content.clone())
                                }
                                _ => None,
                            })
                            .unwrap_or_default();
                        vec![
                            Ok(CompletionDelta::Text(format!("re: {last}"))),
                            Ok(CompletionDelta::Done {
                                usage: usage(),
                                stop: Stop::EndTurn,
                            }),
                        ]
                    }
                    Script::Canned(calls) => calls.pop_front().expect("script exhausted"),
                };
                self.captured.lock().expect("captured").push(request);
                Ok(Box::pin(stream::iter(items)) as CompletionStream)
            })
        }
    }

    struct Harness {
        addr: SocketAddr,
        provider: Arc<MockProvider>,
        supervisor: Arc<Supervisor>,
        shutdown: Option<oneshot::Sender<()>>,
        server: JoinHandle<()>,
        dir: TempDir,
    }

    const SEEDED_AT_MICROS: i64 = 1_700_000_000_000_000;

    const TEST_IDENTITY: &str = "You are ARC, the test edition.";

    fn seeded_record(id: &str, title: &str) -> Event {
        Event {
            seq: 0,
            ts: Some(Timestamp {
                seconds: SEEDED_AT_MICROS / 1_000_000,
                nanos: 0,
            }),
            source: Source::System as i32,
            payload: Some(event::Payload::Memory(MemoryEvent {
                event: Some(memory_event::Event::RecordCreated(MemoryRecordCreated {
                    record: Some(MemoryRecord {
                        id: id.to_owned(),
                        kind: memory_record::Kind::Fact as i32,
                        namespace: "global".to_owned(),
                        title: title.to_owned(),
                        summary: "a summary".to_owned(),
                        body: "a body".to_owned(),
                        links: Vec::new(),
                        provenance: None,
                        status: memory_record::Status::Active as i32,
                    }),
                })),
            })),
        }
    }

    impl Harness {
        async fn start(script: Script) -> Self {
            Self::with_tools(script, Registry::new(512)).await
        }

        async fn with_tools(script: Script, registry: Registry) -> Self {
            Self::with_seed(
                script,
                registry,
                Vec::new(),
                BTreeMap::new(),
                BTreeMap::new(),
            )
            .await
        }

        async fn with_executor(
            script: Script,
            registry: Registry,
            executor: Script,
            projects: BTreeMap<String, ProjectSpec>,
        ) -> Self {
            let executor_provider = MockProvider::new(executor) as Arc<dyn Provider>;
            Self::with_executor_provider(script, registry, executor_provider, projects).await
        }

        async fn with_executor_provider(
            script: Script,
            registry: Registry,
            executor_provider: Arc<dyn Provider>,
            projects: BTreeMap<String, ProjectSpec>,
        ) -> Self {
            let executor_runner = Runner {
                role: SessionRole::Executor,
                provider: executor_provider,
                model: "test-model".to_owned(),
                thinking: Thinking::Default,
                system: None,
                compact_at: None,
                context_window: None,
                editing: arc_core::tool::Editing::Replacement,
            };
            Self::with_seed(
                script,
                registry,
                Vec::new(),
                BTreeMap::from([
                    (
                        SessionRole::Code,
                        Runner {
                            role: SessionRole::Code,
                            ..executor_runner.clone()
                        },
                    ),
                    (SessionRole::Executor, executor_runner),
                ]),
                projects,
            )
            .await
        }

        async fn with_seed(
            script: Script,
            registry: Registry,
            events: Vec<Event>,
            job_runners: BTreeMap<SessionRole, Runner>,
            projects: BTreeMap<String, ProjectSpec>,
        ) -> Self {
            let dir = TempDir::new().expect("temp dir");
            let mut log = Log::open(dir.path()).expect("open log");
            for event in events {
                log.append(event).expect("seed");
            }
            // a file, not :memory:, so the read handle can open the same index
            let index = dir.path().join("index.db");
            let mut projection = Projection::open(&index).expect("open projection");
            arc_core::projection::replay(log.reader().expect("reader"), &mut projection)
                .expect("replay");
            let provider = MockProvider::new(script);
            let (notifier, _receiver) = broadcast::channel(256);
            // mirrors daemon.rs: a dispatched job records the role's own
            // runner, not the dispatching chat's
            let role_identities = job_runners
                .iter()
                .map(|(role, runner)| {
                    (
                        *role,
                        (runner.provider.name().to_owned(), runner.model.clone()),
                    )
                })
                .collect();
            // mirrors daemon.rs: the same read-write root a project's spec
            // grants is what a job's (or a direct turn's) system prompt reads
            // AGENTS.md from
            let project_roots: BTreeMap<String, std::path::PathBuf> = projects
                .iter()
                .filter_map(|(name, spec)| {
                    spec.grants
                        .iter()
                        .find(|grant| grant.mode == arc_core::tool::workspace::Mode::ReadWrite)
                        .map(|grant| (name.clone(), grant.root.clone()))
                })
                .collect();
            let project_list = projects
                .keys()
                .map(|name| ProjectInfo {
                    name: name.clone(),
                    description: format!("about {name}"),
                    root: String::new(),
                })
                .collect();
            let engine = Arc::new(
                Engine::new(Store::new(log, projection), registry)
                    .with_projects(projects)
                    .with_notifier(notifier.clone())
                    .with_role_identities(role_identities),
            );
            let runner = Runner {
                role: SessionRole::Chat,
                provider: Arc::clone(&provider) as Arc<dyn Provider>,
                model: "test-model".to_owned(),
                thinking: Thinking::Default,
                system: Some("be terse".to_owned()),
                compact_at: None,
                context_window: None,
                editing: arc_core::tool::Editing::Replacement,
            };
            let reads = Arc::new(Reader::open(&index).expect("open reads"));
            let supervisor = Arc::new(
                Supervisor::for_test(Arc::clone(&engine), job_runners)
                    .with_projects(
                        project_roots
                            .into_iter()
                            .map(|(name, root)| (name, root.into()))
                            .collect(),
                    )
                    .with_notifier(notifier.clone())
                    .with_identity(Some(TEST_IDENTITY.to_owned()))
                    .with_chat(runner)
                    .with_project_list(project_list),
            );

            let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
            let addr = listener.local_addr().expect("local addr");
            let (shutdown, signal) = oneshot::channel();
            let server = tokio::spawn(serve(
                listener,
                engine,
                reads,
                Arc::clone(&supervisor),
                notifier,
                async {
                    let _ = signal.await;
                },
            ));

            Self {
                addr,
                provider,
                supervisor,
                shutdown: Some(shutdown),
                server,
                dir,
            }
        }

        async fn with_chat_provider(chat_provider: Arc<dyn Provider>, registry: Registry) -> Self {
            let dir = TempDir::new().expect("temp dir");
            let log = Log::open(dir.path()).expect("open log");
            let index = dir.path().join("index.db");
            let mut projection = Projection::open(&index).expect("open projection");
            arc_core::projection::replay(log.reader().expect("reader"), &mut projection)
                .expect("replay");
            let (notifier, _receiver) = broadcast::channel(256);
            let engine = Arc::new(
                Engine::new(Store::new(log, projection), registry).with_notifier(notifier.clone()),
            );
            let runner = Runner {
                role: SessionRole::Chat,
                provider: chat_provider,
                model: "test-model".to_owned(),
                thinking: Thinking::Default,
                system: Some("be terse".to_owned()),
                compact_at: None,
                context_window: None,
                editing: arc_core::tool::Editing::Replacement,
            };
            let reads = Arc::new(Reader::open(&index).expect("open reads"));
            let supervisor = Arc::new(
                Supervisor::for_test(Arc::clone(&engine), BTreeMap::new())
                    .with_notifier(notifier.clone())
                    .with_chat(runner),
            );

            let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
            let addr = listener.local_addr().expect("local addr");
            let (shutdown, signal) = oneshot::channel();
            let server = tokio::spawn(serve(
                listener,
                engine,
                reads,
                Arc::clone(&supervisor),
                notifier,
                async {
                    let _ = signal.await;
                },
            ));

            Self {
                addr,
                provider: MockProvider::new(Script::Echo),
                supervisor,
                shutdown: Some(shutdown),
                server,
                dir,
            }
        }

        async fn drain_jobs(&self) {
            self.supervisor.shutdown().await;
        }

        fn logged_events(&self) -> Vec<session_event::Event> {
            replay_log(self.dir.path())
        }

        async fn connect(&self) -> Client {
            let (ws, _) = tokio_tungstenite::connect_async(format!("ws://{}", self.addr))
                .await
                .expect("connect");
            ws
        }

        async fn stop(&mut self) {
            let _ = self.shutdown.take().expect("not stopped twice").send(());
            tokio::time::timeout(PATIENCE, &mut self.server)
                .await
                .expect("server stops within the grace")
                .expect("server task");
        }
    }

    type Client = WebSocketStream<MaybeTlsStream<TcpStream>>;

    async fn send(ws: &mut Client, request_id: u64, msg: client_frame::Msg) {
        let frame = ClientFrame {
            request_id,
            msg: Some(msg),
        };
        ws.send(WsMessage::binary(frame.encode_to_vec()))
            .await
            .expect("send");
    }

    fn say(session_id: &str, content: &str) -> client_frame::Msg {
        client_frame::Msg::SendMessage(SendMessage {
            session_id: session_id.to_owned(),
            content: content.to_owned(),
            attachments: Vec::new(),
        })
    }

    async fn next_message(ws: &mut Client) -> Option<WsMessage> {
        tokio::time::timeout(PATIENCE, ws.next())
            .await
            .expect("a message within PATIENCE")
            .map(|message| message.expect("websocket read"))
    }

    async fn next_frame(ws: &mut Client) -> ServerFrame {
        match next_message(ws).await {
            Some(WsMessage::Binary(bytes)) => ServerFrame::decode(bytes).expect("decode"),
            other => panic!("expected a binary frame, got {other:?}"),
        }
    }

    async fn turn(ws: &mut Client, request_id: u64) -> (String, String, server_frame::Msg) {
        let accepted = next_frame(ws).await;
        assert_eq!(accepted.request_id, request_id, "request_id is echoed");
        let session_id = match accepted.msg {
            Some(server_frame::Msg::MessageAccepted(m)) => m.session_id,
            other => panic!("expected MessageAccepted first, got {other:?}"),
        };
        assert!(!session_id.is_empty(), "the session must be named");

        let mut text = String::new();
        loop {
            let frame = next_frame(ws).await;
            assert_eq!(frame.request_id, request_id, "request_id is echoed");
            match frame.msg {
                Some(server_frame::Msg::Delta(delta)) => {
                    assert_eq!(delta.session_id, session_id, "deltas carry the session");
                    text.push_str(&delta.text);
                }
                Some(closing) => return (session_id, text, closing),
                None => panic!("a server frame with no message"),
            }
        }
    }

    use arc_proto::v1::history_entry;

    fn said(answer: &SessionHistory) -> Vec<(Role, &str)> {
        answer
            .entries
            .iter()
            .filter_map(|entry| match entry.entry.as_ref() {
                Some(history_entry::Entry::Message(m)) => Some((
                    Role::try_from(m.role).expect("a known role"),
                    m.content.as_str(),
                )),
                _ => None,
            })
            .collect()
    }

    fn ended(msg: server_frame::Msg) -> StreamEnd {
        match msg {
            server_frame::Msg::StreamEnd(end) => end,
            other => panic!("expected StreamEnd, got {other:?}"),
        }
    }

    fn failed(msg: server_frame::Msg) -> WireError {
        match msg {
            server_frame::Msg::Error(error) => error,
            other => panic!("expected Error, got {other:?}"),
        }
    }

    async fn history(ws: &mut Client, request_id: u64, session_id: &str) -> SessionHistory {
        send(
            ws,
            request_id,
            client_frame::Msg::FetchHistory(FetchHistory {
                session_id: session_id.to_owned(),
            }),
        )
        .await;
        let frame = next_frame(ws).await;
        assert_eq!(frame.request_id, request_id);
        match frame.msg {
            Some(server_frame::Msg::SessionHistory(history)) => {
                assert_eq!(
                    history.session_id, session_id,
                    "the answer names its session"
                );
                history
            }
            other => panic!("expected SessionHistory, got {other:?}"),
        }
    }

    async fn list(ws: &mut Client, request_id: u64) -> Vec<SessionInfo> {
        send(
            ws,
            request_id,
            client_frame::Msg::ListSessions(ListSessions {}),
        )
        .await;
        let frame = next_frame(ws).await;
        assert_eq!(frame.request_id, request_id);
        match frame.msg {
            Some(server_frame::Msg::SessionList(list)) => list.sessions,
            other => panic!("expected SessionList, got {other:?}"),
        }
    }

    async fn create_session(
        ws: &mut Client,
        request_id: u64,
        role: SessionRole,
        project: &str,
    ) -> server_frame::Msg {
        send(
            ws,
            request_id,
            client_frame::Msg::CreateSession(CreateSession {
                role: role as i32,
                project: project.to_owned(),
                choice: String::new(),
            }),
        )
        .await;
        let frame = next_frame(ws).await;
        assert_eq!(frame.request_id, request_id);
        frame.msg.expect("a server frame with no message")
    }

    #[tokio::test]
    async fn picture_bytes_cross_the_wire_and_history_shows_a_placeholder() {
        let mut harness = Harness::start(Script::Echo).await;
        let mut ws = harness.connect().await;
        send(
            &mut ws,
            7,
            client_frame::Msg::SendMessage(SendMessage {
                session_id: String::new(),
                content: "describe".to_owned(),
                attachments: vec![ImageAttachment {
                    name: "/tmp/screen.png".to_owned(),
                    media_type: "ignored".to_owned(),
                    data: b"\x89PNG\r\n\x1a\nwire bytes".to_vec(),
                }],
            }),
        )
        .await;
        let (session_id, text, _) = turn(&mut ws, 7).await;
        assert_eq!(text, "re: describe");

        let request = &harness.provider.requests()[0];
        assert!(matches!(
            &request.messages[0],
            Message::UserWithAttachments { attachments, .. }
                if attachments[0].name == "screen.png"
                    && attachments[0].media_type == "image/png"
                    && attachments[0].data == b"\x89PNG\r\n\x1a\nwire bytes"
        ));
        let history = history(&mut ws, 8, &session_id).await;
        let message = said(&history)
            .into_iter()
            .find(|(role, _)| *role == Role::User)
            .expect("user message");
        assert_eq!(message.1, "[image: screen.png]\ndescribe");

        harness.stop().await;
    }

    #[tokio::test]
    async fn a_message_into_a_live_turn_is_queued_and_answered_inside_it() {
        let gate = Arc::new(tokio::sync::Notify::new());
        let chat_provider = ScriptedProvider::scripted_steps(vec![
            Step::Gated {
                before: vec![Ok(CompletionDelta::Text("first".to_owned()))],
                notify: Arc::clone(&gate),
                after: vec![Ok(CompletionDelta::Done {
                    usage: usage(),
                    stop: Stop::EndTurn,
                })],
            },
            Step::Immediate(vec![
                Ok(CompletionDelta::Text(" and second".to_owned())),
                Ok(CompletionDelta::Done {
                    usage: usage(),
                    stop: Stop::EndTurn,
                }),
            ]),
        ]) as Arc<dyn Provider>;

        let mut harness = Harness::with_chat_provider(chat_provider, Registry::new(512)).await;
        let mut ws = harness.connect().await;

        send(&mut ws, 1, say("", "hello")).await;
        let session_id = match next_frame(&mut ws).await.msg {
            Some(server_frame::Msg::MessageAccepted(m)) => m.session_id,
            other => panic!("expected MessageAccepted, got {other:?}"),
        };
        wait_for_child_message_count(&harness, &session_id, 1).await;

        let mut second_ws = harness.connect().await;
        send(&mut second_ws, 2, say(&session_id, "and another thing")).await;
        match next_frame(&mut second_ws).await.msg {
            Some(server_frame::Msg::MessageAccepted(m)) => assert_eq!(m.session_id, session_id),
            other => panic!("expected MessageAccepted, got {other:?}"),
        }
        let queued = ended(next_frame(&mut second_ws).await.msg.expect("a message"));
        assert!(
            queued.queued,
            "the sender is told its message joined a turn already running"
        );
        assert_eq!((queued.input_tokens, queued.output_tokens), (0, 0));

        gate.notify_one();

        let mut text = String::new();
        let closing = loop {
            let frame = next_frame(&mut ws).await;
            assert_eq!(frame.request_id, 1, "one stream, one request");
            match frame.msg {
                Some(server_frame::Msg::Delta(delta)) => text.push_str(&delta.text),
                Some(closing) => break closing,
                None => panic!("a server frame with no message"),
            }
        };
        assert_eq!(
            text, "first and second",
            "the answer to the queued message streams in the turn that took it"
        );
        assert!(!ended(closing).queued);

        let messages: Vec<_> = harness
            .logged_events()
            .into_iter()
            .filter_map(|event| match event {
                session_event::Event::MessageAppended(m) if m.session_id == session_id => {
                    Some((Role::try_from(m.role).expect("a known role"), m.content))
                }
                _ => None,
            })
            .collect();
        assert_eq!(
            messages,
            [
                (Role::User, "hello".to_owned()),
                (Role::Assistant, "first".to_owned()),
                (Role::User, "and another thing".to_owned()),
                (Role::Assistant, " and second".to_owned()),
            ],
            "the queued message landed at the turn's next step boundary"
        );

        harness.stop().await;
    }

    #[tokio::test]
    async fn a_connection_dropped_mid_turn_never_stops_the_turn() {
        let gate = Arc::new(tokio::sync::Notify::new());
        let chat_provider = ScriptedProvider::scripted_steps(vec![Step::Gated {
            before: vec![Ok(CompletionDelta::Text("working".to_owned()))],
            notify: Arc::clone(&gate),
            after: vec![
                Ok(CompletionDelta::Text(" and done".to_owned())),
                Ok(CompletionDelta::Done {
                    usage: usage(),
                    stop: Stop::EndTurn,
                }),
            ],
        }]) as Arc<dyn Provider>;

        let mut harness = Harness::with_chat_provider(chat_provider, Registry::new(512)).await;
        let mut ws = harness.connect().await;

        send(&mut ws, 1, say("", "hello")).await;
        let session_id = match next_frame(&mut ws).await.msg {
            Some(server_frame::Msg::MessageAccepted(m)) => m.session_id,
            other => panic!("expected MessageAccepted, got {other:?}"),
        };
        wait_for_child_message_count(&harness, &session_id, 1).await;

        drop(ws);
        gate.notify_one();
        wait_for_child_message_count(&harness, &session_id, 2).await;

        let reply = harness
            .logged_events()
            .into_iter()
            .find_map(|event| match event {
                session_event::Event::MessageAppended(m)
                    if m.session_id == session_id && m.role == Role::Assistant as i32 =>
                {
                    Some(m)
                }
                _ => None,
            })
            .expect("the turn finished without its connection");
        assert_eq!(reply.content, "working and done");
        assert!(
            !reply.partial,
            "the turn ran to a whole reply, not a cut one"
        );

        harness.stop().await;
    }

    #[tokio::test]
    async fn status_round_trips_the_latest_measurement_and_survives_client_reconnect() {
        let mut harness = Harness::start(Script::Echo).await;
        let mut ws = harness.connect().await;
        send(&mut ws, 1, say("", "status")).await;
        let (session_id, _, _) = turn(&mut ws, 1).await;
        let url = format!("ws://{}", harness.addr);
        let mut client = arc_core::client::Client::connect(&url).await.unwrap();
        let first = client.fetch_status(&session_id).await.unwrap();
        assert_eq!(
            first.context.as_ref().unwrap().input_tokens,
            usage().input_tokens
        );
        assert_eq!(first.context.as_ref().unwrap().context_window, None);
        assert!(first.context_observed_at > 0);
        assert!(!first.codex);
        assert!(first.allowance.is_empty());
        drop(client);
        let mut client = arc_core::client::Client::connect(&url).await.unwrap();
        assert_eq!(client.fetch_status(&session_id).await.unwrap(), first);
        assert!(client.fetch_status("missing").await.is_err());
        harness.stop().await;
    }

    #[tokio::test]
    async fn an_empty_message_is_an_error_the_connection_survives() {
        let mut harness = Harness::start(Script::Echo).await;
        let mut ws = harness.connect().await;

        send(&mut ws, 1, say("", "   \n\t ")).await;
        let frame = next_frame(&mut ws).await;

        assert_eq!(frame.request_id, 1);
        let error = failed(frame.msg.expect("a message"));
        assert_eq!(error.code, "empty_message");
        assert!(!error.msg.is_empty(), "the code comes with an explanation");

        send(&mut ws, 2, say("", "hello")).await;
        let (_, text, closing) = turn(&mut ws, 2).await;
        assert_eq!(text, "re: hello");
        assert!(!ended(closing).partial);

        harness.stop().await;
    }

    #[tokio::test]
    async fn a_cut_stream_ends_partial() {
        let cut = vec![Ok(CompletionDelta::Text("half a th".to_owned()))];
        let mut harness = Harness::start(Script::Canned(VecDeque::from([cut]))).await;
        let mut ws = harness.connect().await;

        send(&mut ws, 1, say("", "hello")).await;
        let (session_id, text, closing) = turn(&mut ws, 1).await;

        assert_eq!(text, "half a th", "the client saw what was appended");
        let end = ended(closing);
        assert_eq!(end.session_id, session_id);
        assert!(end.partial, "a cut stream says so on the wire");
        assert_eq!((end.input_tokens, end.output_tokens), (0, 0));

        harness.stop().await;
    }

    #[tokio::test]
    async fn reasoning_and_tool_activity_are_forwarded_in_turn_order() {
        let script = Script::Canned(VecDeque::from([
            vec![
                Ok(CompletionDelta::Reasoning("let me look".to_owned())),
                Ok(CompletionDelta::ToolCall(ToolCall {
                    id: "t1".to_owned(),
                    index: 0,
                    name: "lookup".to_owned(),
                    arguments: r#"{"q":1}"#.to_owned(),
                    provider_roundtrip: Vec::new(),
                })),
                Ok(CompletionDelta::Done {
                    usage: usage(),
                    stop: Stop::ToolCalls,
                }),
            ],
            vec![
                Ok(CompletionDelta::Text("answer".to_owned())),
                Ok(CompletionDelta::Done {
                    usage: usage(),
                    stop: Stop::EndTurn,
                }),
            ],
        ]));
        let mut registry = Registry::new(512);
        registry.register(Box::new(Canned {
            name: "lookup",
            content: "found it",
            ok: true,
            source: ToolSource::Builtin,
        }));
        let mut harness = Harness::with_tools(script, registry).await;
        let mut ws = harness.connect().await;

        send(&mut ws, 1, say("", "hi")).await;

        let accepted = next_frame(&mut ws).await;
        let session_id = match accepted.msg {
            Some(server_frame::Msg::MessageAccepted(m)) => m.session_id,
            other => panic!("expected MessageAccepted first, got {other:?}"),
        };

        let mut middle = Vec::new();
        let end = loop {
            let frame = next_frame(&mut ws).await;
            assert_eq!(frame.request_id, 1, "request_id is echoed");
            match frame.msg.expect("a message") {
                server_frame::Msg::StreamEnd(end) => break end,
                msg => middle.push(msg),
            }
        };

        assert_eq!(
            middle,
            [
                server_frame::Msg::ReasoningDelta(ReasoningDelta {
                    session_id: session_id.clone(),
                    text: "let me look".to_owned(),
                }),
                server_frame::Msg::ToolCallStarted(ToolCallStarted {
                    session_id: session_id.clone(),
                    call_id: "t1".to_owned(),
                    index: 0,
                    name: "lookup".to_owned(),
                    arguments_json: r#"{"q":1}"#.to_owned(),
                }),
                server_frame::Msg::ToolCallEnded(ToolCallEnded {
                    session_id: session_id.clone(),
                    call_id: "t1".to_owned(),
                    outcome: ToolOutcome::Ok as i32,
                    content: "found it".to_owned(),
                }),
                server_frame::Msg::Delta(Delta {
                    session_id: session_id.clone(),
                    text: "answer".to_owned(),
                }),
            ]
        );
        assert_eq!(end.session_id, session_id);

        harness.stop().await;
    }

    #[tokio::test]
    async fn history_carries_tool_entries_alongside_prose_messages() {
        let script = Script::Canned(VecDeque::from([
            vec![
                Ok(CompletionDelta::ToolCall(ToolCall {
                    id: "t1".to_owned(),
                    index: 0,
                    name: "lookup".to_owned(),
                    arguments: "{}".to_owned(),
                    provider_roundtrip: Vec::new(),
                })),
                Ok(CompletionDelta::Done {
                    usage: usage(),
                    stop: Stop::ToolCalls,
                }),
            ],
            vec![
                Ok(CompletionDelta::Text("answer".to_owned())),
                Ok(CompletionDelta::Done {
                    usage: usage(),
                    stop: Stop::EndTurn,
                }),
            ],
        ]));
        let mut registry = Registry::new(512);
        registry.register(Box::new(Canned {
            name: "lookup",
            content: "found it",
            ok: true,
            source: ToolSource::Builtin,
        }));
        let mut harness = Harness::with_tools(script, registry).await;
        let mut ws = harness.connect().await;

        send(&mut ws, 1, say("", "hi")).await;
        let accepted = next_frame(&mut ws).await;
        let session_id = match accepted.msg {
            Some(server_frame::Msg::MessageAccepted(m)) => m.session_id,
            other => panic!("expected MessageAccepted first, got {other:?}"),
        };
        loop {
            if let Some(server_frame::Msg::StreamEnd(_)) = next_frame(&mut ws).await.msg {
                break;
            }
        }

        let answer = history(&mut ws, 2, &session_id).await;

        let prose = |role: Role, content: &str| HistoryMessage {
            role: role as i32,
            content: content.to_owned(),
            partial: false,
            source: match role {
                Role::Assistant => Source::Model as i32,
                _ => Source::User as i32,
            },
            ..Default::default()
        };
        assert_eq!(
            answer.entries[..3],
            [
                HistoryEntry {
                    entry: Some(history_entry::Entry::Message(prose(Role::User, "hi"))),
                    seq: 1,
                },
                HistoryEntry {
                    entry: Some(history_entry::Entry::ToolCall(HistoryToolCall {
                        call_id: "t1".to_owned(),
                        name: "lookup".to_owned(),
                        arguments_json: "{}".to_owned(),
                    })),
                    seq: 3,
                },
                HistoryEntry {
                    entry: Some(history_entry::Entry::ToolResult(HistoryToolResult {
                        call_id: "t1".to_owned(),
                        outcome: ToolOutcome::Ok as i32,
                        truncated: false,
                        content: "found it".to_owned(),
                    })),
                    seq: 4,
                },
            ],
            "the projection→history path stamps every entry with its log seq"
        );
        let Some(history_entry::Entry::Message(final_message)) = &answer.entries[3].entry else {
            panic!(
                "expected the final assistant message, got {:?}",
                answer.entries[3]
            );
        };
        assert_eq!(final_message.role, Role::Assistant as i32);
        assert_eq!(final_message.content, "answer");
        assert_eq!(final_message.source, Source::Model as i32);
        assert_eq!(
            (final_message.input_tokens, final_message.output_tokens),
            (2 * usage().input_tokens, 2 * usage().output_tokens),
            "usage accumulates across both completion steps"
        );
        assert_eq!(
            said(&answer),
            [(Role::User, "hi"), (Role::Assistant, "answer")],
            "the prose reads in order with the tool entries between"
        );

        harness.stop().await;
    }

    #[tokio::test]
    async fn bad_bytes_get_bad_frame_and_end_the_connection() {
        let mut harness = Harness::start(Script::Echo).await;
        let mut ws = harness.connect().await;

        ws.send(WsMessage::binary(vec![0x0a, 0xff, 0xff]))
            .await
            .expect("send");

        let frame = next_frame(&mut ws).await;
        assert_eq!(frame.request_id, 0, "no request id could be recovered");
        assert_eq!(failed(frame.msg.expect("a message")).code, "bad_frame");

        match next_message(&mut ws).await {
            Some(WsMessage::Close(_)) | None => {}
            other => panic!("expected the connection to close, got {other:?}"),
        }

        harness.stop().await;
    }

    async fn review_list(
        ws: &mut Client,
        request_id: u64,
        since_micros: i64,
    ) -> Vec<arc_proto::v1::MemoryReviewItem> {
        send(
            ws,
            request_id,
            client_frame::Msg::MemoryReviewList(MemoryReviewList { since_micros }),
        )
        .await;
        let frame = next_frame(ws).await;
        assert_eq!(frame.request_id, request_id);
        match frame.msg {
            Some(server_frame::Msg::MemoryReviewItems(items)) => items.items,
            other => panic!("expected MemoryReviewItems, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn review_list_answers_the_seeded_queue_and_accept_clears_a_record() {
        let mut harness = Harness::with_seed(
            Script::Echo,
            Registry::new(512),
            vec![seeded_record("m-a", "alpha"), seeded_record("m-b", "beta")],
            BTreeMap::new(),
            BTreeMap::new(),
        )
        .await;
        let mut ws = harness.connect().await;

        let items = review_list(&mut ws, 1, 0).await;
        let listed: Vec<_> = items
            .iter()
            .map(|item| {
                let record = item.record.as_ref().expect("a record");
                (
                    record.id.as_str(),
                    record.title.as_str(),
                    item.changed_at_micros,
                    item.supersedes.len(),
                )
            })
            .collect();
        assert_eq!(
            listed,
            [
                ("m-a", "alpha", SEEDED_AT_MICROS, 0),
                ("m-b", "beta", SEEDED_AT_MICROS, 0),
            ],
            "both records, whole, in (changed_at, id) order"
        );

        assert_eq!(review_list(&mut ws, 2, SEEDED_AT_MICROS + 1).await, []);

        send(
            &mut ws,
            3,
            client_frame::Msg::MemoryReviewAccept(MemoryReviewAccept {
                record_id: "m-a".to_owned(),
            }),
        )
        .await;
        let frame = next_frame(&mut ws).await;
        assert_eq!(frame.request_id, 3);
        match frame.msg {
            Some(server_frame::Msg::MessageAccepted(ack)) => {
                assert_eq!(ack.session_id, "", "a verdict ack names no session");
            }
            other => panic!("expected MessageAccepted, got {other:?}"),
        }

        let remaining: Vec<_> = review_list(&mut ws, 4, 0)
            .await
            .into_iter()
            .map(|item| item.record.expect("a record").id)
            .collect();
        assert_eq!(remaining, ["m-b"], "the accepted record left the queue");

        harness.stop().await;
    }

    #[tokio::test]
    async fn review_delete_removes_the_record_and_a_second_try_is_unknown() {
        let mut harness = Harness::with_seed(
            Script::Echo,
            Registry::new(512),
            vec![seeded_record("m-a", "alpha")],
            BTreeMap::new(),
            BTreeMap::new(),
        )
        .await;
        let mut ws = harness.connect().await;

        send(
            &mut ws,
            1,
            client_frame::Msg::MemoryReviewDelete(MemoryReviewDelete {
                record_id: "m-a".to_owned(),
            }),
        )
        .await;
        let frame = next_frame(&mut ws).await;
        assert!(
            matches!(frame.msg, Some(server_frame::Msg::MessageAccepted(_))),
            "got: {frame:?}"
        );
        assert_eq!(review_list(&mut ws, 2, 0).await, [], "the record is gone");

        send(
            &mut ws,
            3,
            client_frame::Msg::MemoryReviewDelete(MemoryReviewDelete {
                record_id: "m-a".to_owned(),
            }),
        )
        .await;
        let frame = next_frame(&mut ws).await;
        assert_eq!(frame.request_id, 3);
        let error = failed(frame.msg.expect("a message"));
        assert_eq!(error.code, "unknown_record");
        assert!(!error.msg.is_empty(), "the code comes with an explanation");
        assert_eq!(review_list(&mut ws, 4, 0).await, []);

        harness.stop().await;
    }

    #[tokio::test]
    async fn shutdown_closes_an_idle_connection() {
        let mut harness = Harness::start(Script::Echo).await;
        let mut ws = harness.connect().await;

        send(&mut ws, 1, say("", "hello")).await;
        turn(&mut ws, 1).await;

        harness.stop().await;

        match next_message(&mut ws).await {
            Some(WsMessage::Close(_)) | None => {}
            other => panic!("expected the connection to close, got {other:?}"),
        }
    }

    fn dispatch_call(brief: &str) -> ToolCall {
        let args = serde_json::json!({
            "role": "executor",
            "project": "arc",
            "brief": brief,
            "intent": "implement",
            "budget_tokens": 0,
            "budget_minutes": 0,
        })
        .to_string();
        ToolCall {
            id: "d1".to_owned(),
            index: 0,
            name: "dispatch".to_owned(),
            arguments: args,
            provider_roundtrip: Vec::new(),
        }
    }

    fn dispatching_chat(brief: &str) -> Script {
        Script::Canned(VecDeque::from([
            vec![
                Ok(CompletionDelta::ToolCall(dispatch_call(brief))),
                Ok(CompletionDelta::Done {
                    usage: usage(),
                    stop: Stop::ToolCalls,
                }),
            ],
            vec![
                Ok(CompletionDelta::Text("dispatched".to_owned())),
                Ok(CompletionDelta::Done {
                    usage: usage(),
                    stop: Stop::EndTurn,
                }),
            ],
            vec![
                Ok(CompletionDelta::Text("noted".to_owned())),
                Ok(CompletionDelta::Done {
                    usage: usage(),
                    stop: Stop::EndTurn,
                }),
            ],
        ]))
    }

    async fn run_turn_to_end(ws: &mut Client, request_id: u64) {
        loop {
            let frame = next_frame(ws).await;
            assert_eq!(frame.request_id, request_id, "request_id is echoed");
            if matches!(frame.msg, Some(server_frame::Msg::StreamEnd(_))) {
                return;
            }
        }
    }

    fn dispatch_registry_and_projects() -> (Registry, TempDir, BTreeMap<String, ProjectSpec>) {
        let mut registry = Registry::new(512);
        registry.register(Box::new(arc_core::tool::builtin::dispatch::Dispatch::new(
            vec![("arc".to_owned(), String::new())],
            None,
        )));
        let project_dir = TempDir::new().expect("project dir");
        let projects = BTreeMap::from([(
            "arc".to_owned(),
            ProjectSpec {
                sources: Vec::new(),
                grants: vec![arc_core::tool::workspace::Grant::new(
                    project_dir.path(),
                    arc_core::tool::workspace::Mode::ReadWrite,
                )],
                command_prefix: Vec::new(),
            },
        )]);
        (registry, project_dir, projects)
    }

    fn dispatched_child_id(harness: &Harness) -> String {
        harness
            .logged_events()
            .into_iter()
            .find_map(|event| match event {
                session_event::Event::SessionCreated(created)
                    if created.role == SessionRole::Executor as i32 =>
                {
                    Some(created.session_id)
                }
                _ => None,
            })
            .expect("the child session was created durably by dispatch")
    }

    async fn wait_for_child_message_count(harness: &Harness, child_id: &str, want: usize) {
        for _ in 0..400 {
            let count = harness
                .logged_events()
                .into_iter()
                .filter(
                    |event| matches!(event, session_event::Event::MessageAppended(m) if m.session_id == child_id),
                )
                .count();
            if count >= want {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        panic!("timed out waiting for {want} messages in session {child_id}");
    }

    #[tokio::test]
    async fn cancel_job_on_a_live_job_is_accepted_and_hands_back_cancelled() {
        let (registry, _project_dir, projects) = dispatch_registry_and_projects();

        // never notified: the turn stalls until the cancel drops it
        let gate = Arc::new(tokio::sync::Notify::new());
        let executor_provider = ScriptedProvider::scripted_steps(vec![Step::Gated {
            before: Vec::new(),
            notify: gate,
            after: Vec::new(),
        }]) as Arc<dyn Provider>;

        let mut harness = Harness::with_executor_provider(
            dispatching_chat("fix the failing test"),
            registry,
            executor_provider,
            projects,
        )
        .await;
        let mut ws = harness.connect().await;

        send(&mut ws, 1, say("", "start a job")).await;
        let accepted = next_frame(&mut ws).await;
        let parent_id = match accepted.msg {
            Some(server_frame::Msg::MessageAccepted(m)) => m.session_id,
            other => panic!("expected MessageAccepted, got {other:?}"),
        };
        run_turn_to_end(&mut ws, 1).await;

        let child_id = dispatched_child_id(&harness);
        wait_for_child_message_count(&harness, &child_id, 1).await;

        let mut cancel_ws = harness.connect().await;
        send(
            &mut cancel_ws,
            2,
            client_frame::Msg::CancelJob(CancelJob {
                session_id: child_id.clone(),
            }),
        )
        .await;
        let answer = next_frame(&mut cancel_ws).await;
        assert_eq!(answer.request_id, 2);
        match answer.msg {
            Some(server_frame::Msg::MessageAccepted(m)) => assert_eq!(m.session_id, child_id),
            other => panic!("expected MessageAccepted, got {other:?}"),
        }

        harness.drain_jobs().await;

        let parent_handback = harness
            .logged_events()
            .into_iter()
            .filter_map(|event| match event {
                session_event::Event::MessageAppended(m)
                    if m.session_id == parent_id && m.role == Role::User as i32 =>
                {
                    Some(m.content)
                }
                _ => None,
            })
            .next_back()
            .expect("the cancelled handback landed in the parent");
        assert!(
            parent_handback.contains("stopped: cancelled by the user"),
            "got: {parent_handback}"
        );

        harness.stop().await;
    }

    #[tokio::test]
    async fn job_controls_on_an_unknown_session_are_honest_errors() {
        let mut harness = Harness::start(Script::Echo).await;
        let mut ws = harness.connect().await;

        send(
            &mut ws,
            1,
            client_frame::Msg::CancelJob(CancelJob {
                session_id: "s-unknown".to_owned(),
            }),
        )
        .await;
        let frame = next_frame(&mut ws).await;
        assert_eq!(frame.request_id, 1);
        assert_eq!(failed(frame.msg.expect("a message")).code, "unknown_job");

        send(
            &mut ws,
            2,
            client_frame::Msg::DropSteers(DropSteers {
                session_id: "s-unknown".to_owned(),
            }),
        )
        .await;
        let frame = next_frame(&mut ws).await;
        assert_eq!(frame.request_id, 2);
        assert_eq!(failed(frame.msg.expect("a message")).code, "unknown_job");

        harness.stop().await;
    }

    #[tokio::test]
    async fn cancel_turn_on_a_live_chat_turn_ends_it_as_a_partial_reply() {
        let gate = Arc::new(tokio::sync::Notify::new());
        let chat_provider = ScriptedProvider::scripted_steps(vec![Step::Gated {
            before: vec![Ok(CompletionDelta::Text("working".to_owned()))],
            notify: Arc::clone(&gate),
            after: vec![Ok(CompletionDelta::Done {
                usage: usage(),
                stop: Stop::EndTurn,
            })],
        }]) as Arc<dyn Provider>;

        let mut harness = Harness::with_chat_provider(chat_provider, Registry::new(512)).await;
        let mut ws = harness.connect().await;

        send(&mut ws, 1, say("", "hello")).await;
        let accepted = next_frame(&mut ws).await;
        let session_id = match accepted.msg {
            Some(server_frame::Msg::MessageAccepted(m)) => m.session_id,
            other => panic!("expected MessageAccepted, got {other:?}"),
        };

        wait_for_child_message_count(&harness, &session_id, 1).await;

        let mut cancel_ws = harness.connect().await;
        send(
            &mut cancel_ws,
            2,
            client_frame::Msg::CancelTurn(CancelTurn {
                session_id: session_id.clone(),
            }),
        )
        .await;
        let answer = next_frame(&mut cancel_ws).await;
        assert_eq!(answer.request_id, 2);
        match answer.msg {
            Some(server_frame::Msg::MessageAccepted(m)) => assert_eq!(m.session_id, session_id),
            other => panic!("expected MessageAccepted, got {other:?}"),
        }

        let mut text = String::new();
        let closing = loop {
            let frame = next_frame(&mut ws).await;
            assert_eq!(frame.request_id, 1);
            match frame.msg {
                Some(server_frame::Msg::Delta(delta)) => text.push_str(&delta.text),
                Some(closing) => break closing,
                None => panic!("a server frame with no message"),
            }
        };
        assert_eq!(text, "working");
        assert!(
            ended(closing).partial,
            "the cancelled turn lands as a partial reply, not an error"
        );

        let assistant = harness
            .logged_events()
            .into_iter()
            .find_map(|event| match event {
                session_event::Event::MessageAppended(m)
                    if m.session_id == session_id && m.role == Role::Assistant as i32 =>
                {
                    Some(m)
                }
                _ => None,
            })
            .expect("the partial reply landed durably");
        assert_eq!(assistant.content, "working");
        assert!(assistant.partial);

        harness.stop().await;
    }

    #[tokio::test]
    async fn compact_session_refuses_a_live_turn() {
        let gate = Arc::new(tokio::sync::Notify::new());
        let chat_provider = ScriptedProvider::scripted_steps(vec![Step::Gated {
            before: vec![Ok(CompletionDelta::Text("working".to_owned()))],
            notify: Arc::clone(&gate),
            after: vec![Ok(CompletionDelta::Done {
                usage: usage(),
                stop: Stop::EndTurn,
            })],
        }]) as Arc<dyn Provider>;

        let mut harness = Harness::with_chat_provider(chat_provider, Registry::new(512)).await;
        let mut ws = harness.connect().await;

        send(&mut ws, 1, say("", "hello")).await;
        let accepted = next_frame(&mut ws).await;
        let session_id = match accepted.msg {
            Some(server_frame::Msg::MessageAccepted(m)) => m.session_id,
            other => panic!("expected MessageAccepted, got {other:?}"),
        };
        wait_for_child_message_count(&harness, &session_id, 1).await;

        let mut compact_ws = harness.connect().await;
        send(
            &mut compact_ws,
            2,
            client_frame::Msg::CompactSession(CompactSession {
                session_id: session_id.clone(),
            }),
        )
        .await;
        let answer = next_frame(&mut compact_ws).await;
        assert_eq!(answer.request_id, 2);
        assert_eq!(failed(answer.msg.expect("a message")).code, "turn_running");

        gate.notify_one();
        loop {
            let frame = next_frame(&mut ws).await;
            if matches!(frame.msg, Some(server_frame::Msg::StreamEnd(_))) {
                break;
            }
        }

        harness.stop().await;
    }

    #[tokio::test]
    async fn compact_session_on_an_idle_session_is_accepted() {
        let mut harness = Harness::start(Script::Echo).await;
        let mut ws = harness.connect().await;

        send(&mut ws, 1, say("", "hello")).await;
        let (session_id, _, _closing) = turn(&mut ws, 1).await;

        send(
            &mut ws,
            2,
            client_frame::Msg::CompactSession(CompactSession {
                session_id: session_id.clone(),
            }),
        )
        .await;
        let answer = next_frame(&mut ws).await;
        assert_eq!(answer.request_id, 2);
        match answer.msg {
            Some(server_frame::Msg::MessageAccepted(m)) => assert_eq!(m.session_id, session_id),
            other => panic!("expected MessageAccepted, got {other:?}"),
        }

        harness.stop().await;
    }

    #[tokio::test]
    async fn drop_steers_on_a_live_job_is_accepted_and_leaves_its_turn_alone() {
        let (registry, _project_dir, projects) = dispatch_registry_and_projects();

        let gate = Arc::new(tokio::sync::Notify::new());
        let executor_provider = ScriptedProvider::scripted_steps(vec![Step::Gated {
            before: vec![Ok(CompletionDelta::Text("working".to_owned()))],
            notify: Arc::clone(&gate),
            after: vec![Ok(CompletionDelta::Done {
                usage: usage(),
                stop: Stop::EndTurn,
            })],
        }]) as Arc<dyn Provider>;

        let mut harness = Harness::with_executor_provider(
            dispatching_chat("fix the failing test"),
            registry,
            executor_provider,
            projects,
        )
        .await;
        let mut ws = harness.connect().await;

        send(&mut ws, 1, say("", "start a job")).await;
        run_turn_to_end(&mut ws, 1).await;

        let child_id = dispatched_child_id(&harness);
        wait_for_child_message_count(&harness, &child_id, 1).await;

        let mut drop_ws = harness.connect().await;
        send(
            &mut drop_ws,
            3,
            client_frame::Msg::DropSteers(DropSteers {
                session_id: child_id.clone(),
            }),
        )
        .await;
        let answer = next_frame(&mut drop_ws).await;
        assert_eq!(answer.request_id, 3);
        match answer.msg {
            Some(server_frame::Msg::MessageAccepted(m)) => assert_eq!(m.session_id, child_id),
            other => panic!("expected MessageAccepted, got {other:?}"),
        }

        gate.notify_one();
        harness.drain_jobs().await;

        let child_messages: Vec<_> = harness
            .logged_events()
            .into_iter()
            .filter_map(|event| match event {
                session_event::Event::MessageAppended(m) if m.session_id == child_id => {
                    Some(m.content)
                }
                _ => None,
            })
            .collect();
        assert_eq!(
            child_messages,
            ["fix the failing test".to_owned(), "working".to_owned()],
            "the drop left the running turn alone"
        );

        harness.stop().await;
    }

    #[tokio::test]
    async fn create_session_opens_a_code_session_with_the_direct_prompt_and_a_job_keeps_its_own() {
        let (registry, project_dir, projects) = dispatch_registry_and_projects();
        std::fs::write(
            project_dir.path().join("AGENTS.md"),
            "Keep commits small.\n",
        )
        .expect("write AGENTS.md");

        let executor_provider =
            ScriptedProvider::scripted(vec![done_reply("on it"), done_reply("hi there")]);
        let mut harness = Harness::with_executor_provider(
            dispatching_chat("fix the failing test"),
            registry,
            Arc::clone(&executor_provider) as Arc<dyn Provider>,
            projects,
        )
        .await;
        let mut ws = harness.connect().await;

        send(&mut ws, 1, say("", "start a job")).await;
        run_turn_to_end(&mut ws, 1).await;
        harness.drain_jobs().await;

        let msg = create_session(&mut ws, 2, SessionRole::Code, "arc").await;
        let code_session_id = match msg {
            server_frame::Msg::MessageAccepted(accepted) => accepted.session_id,
            other => panic!("expected MessageAccepted, got {other:?}"),
        };
        assert!(!code_session_id.is_empty());
        assert_ne!(
            code_session_id,
            dispatched_child_id(&harness),
            "a :code session is its own session, not the dispatched job"
        );

        send(&mut ws, 3, say(&code_session_id, "hello")).await;
        let (session_id, text, closing) = turn(&mut ws, 3).await;
        assert_eq!(session_id, code_session_id);
        assert_eq!(text, "hi there");
        assert!(!ended(closing).partial);

        let requests = executor_provider.requests();
        assert_eq!(requests.len(), 2, "the job's turn, then the :code turn");
        assert_eq!(requests[0].role, SessionRole::Executor);
        assert_eq!(requests[1].role, SessionRole::Code);
        let job_system = requests[0]
            .system
            .clone()
            .expect("the job gets a system prompt");
        assert!(
            job_system.contains("non-interactively") && job_system.contains("job's report"),
            "{job_system}"
        );
        assert!(job_system.contains("Keep commits small."), "{job_system}");
        assert!(
            !job_system.contains(TEST_IDENTITY),
            "a dispatched job never carries identity: {job_system}"
        );

        let direct_system = requests[1]
            .system
            .clone()
            .expect("the :code turn gets a system prompt");
        assert!(
            direct_system.contains("interactively with the user"),
            "{direct_system}"
        );
        assert!(!direct_system.contains("job's report"), "{direct_system}");
        assert!(
            direct_system.contains("Keep commits small."),
            "{direct_system}"
        );
        assert!(
            direct_system.contains(TEST_IDENTITY),
            "the user is present, so the direct turn carries identity: {direct_system}"
        );

        let sessions = list(&mut ws, 4).await;
        let code_summary = sessions
            .iter()
            .find(|s| s.id == code_session_id)
            .expect("the :code session is listed");
        assert_eq!(code_summary.role, SessionRole::Code as i32);
        assert_eq!(
            code_summary.dispatched_by, "",
            "a :code session is a root conversation, not a dispatched child"
        );
        assert_eq!(
            code_summary.source,
            Source::User as i32,
            "a :code session is user-opened, not model-dispatched"
        );

        harness.stop().await;
    }

    #[tokio::test]
    async fn create_chat_session_rejects_unknown_choice_and_accepts_a_turn() {
        let mut harness = Harness::start(Script::Echo).await;
        let mut ws = harness.connect().await;

        send(
            &mut ws,
            1,
            client_frame::Msg::CreateSession(CreateSession {
                role: SessionRole::Chat as i32,
                project: String::new(),
                choice: "unconfigured".to_owned(),
            }),
        )
        .await;
        let response = next_frame(&mut ws).await;
        assert_eq!(failed(response.msg.expect("error")).code, "unknown_choice");

        let created = create_session(&mut ws, 2, SessionRole::Chat, "").await;
        let id = match created {
            server_frame::Msg::MessageAccepted(accepted) => accepted.session_id,
            other => panic!("expected session id, got {other:?}"),
        };
        send(&mut ws, 3, say(&id, "hello")).await;
        let (served, reply, _) = turn(&mut ws, 3).await;
        assert_eq!(served, id);
        assert_eq!(reply, "re: hello");

        harness.stop().await;
    }

    #[tokio::test]
    async fn fork_session_opens_a_new_branch_that_carries_the_parents_prefix() {
        let mut harness = Harness::start(Script::Echo).await;
        let mut ws = harness.connect().await;

        send(&mut ws, 1, say("", "hello")).await;
        let (session_id, _text, closing) = turn(&mut ws, 1).await;
        ended(closing);

        let answer = history(&mut ws, 2, &session_id).await;
        let fork_point = answer.entries[0].seq;

        send(
            &mut ws,
            3,
            client_frame::Msg::ForkSession(ForkSession {
                session_id: session_id.clone(),
                fork_point,
                choice: String::new(),
            }),
        )
        .await;
        let frame = next_frame(&mut ws).await;
        assert_eq!(frame.request_id, 3);
        let fork_id = match frame.msg {
            Some(server_frame::Msg::MessageAccepted(m)) => m.session_id,
            other => panic!("expected MessageAccepted, got {other:?}"),
        };
        assert_ne!(fork_id, session_id);

        let forked_history = history(&mut ws, 4, &fork_id).await;
        assert_eq!(
            said(&forked_history),
            [(Role::User, "hello")],
            "the branch inherits the parent's prefix through the fork point"
        );

        harness.stop().await;
    }

    #[tokio::test]
    async fn mark_branch_round_trips_and_the_list_shows_the_disposition() {
        let mut harness = Harness::start(Script::Echo).await;
        let mut ws = harness.connect().await;

        send(&mut ws, 1, say("", "hello")).await;
        let (session_id, _text, closing) = turn(&mut ws, 1).await;
        ended(closing);
        let fork_point = history(&mut ws, 2, &session_id).await.entries[0].seq;

        send(
            &mut ws,
            3,
            client_frame::Msg::ForkSession(ForkSession {
                session_id: session_id.clone(),
                fork_point,
                choice: String::new(),
            }),
        )
        .await;
        let fork_id = match next_frame(&mut ws).await.msg {
            Some(server_frame::Msg::MessageAccepted(m)) => m.session_id,
            other => panic!("expected MessageAccepted, got {other:?}"),
        };

        send(
            &mut ws,
            4,
            client_frame::Msg::MarkBranch(MarkBranch {
                session_id: fork_id.clone(),
                disposition: branch_marked::Disposition::Real as i32,
            }),
        )
        .await;
        let frame = next_frame(&mut ws).await;
        assert_eq!(frame.request_id, 4);
        assert!(matches!(
            frame.msg,
            Some(server_frame::Msg::MessageAccepted(_))
        ));

        let sessions = list(&mut ws, 5).await;
        let forked = sessions
            .iter()
            .find(|s| s.id == fork_id)
            .expect("the fork is listed");
        assert_eq!(forked.parent_session, session_id);
        assert_eq!(forked.disposition, branch_marked::Disposition::Real as i32);

        harness.stop().await;
    }

    #[tokio::test]
    async fn the_model_menu_lists_and_a_selection_answers_with_the_new_pick() {
        let (registry, _project_dir, projects) = dispatch_registry_and_projects();
        let harness = Harness::with_executor(
            Script::Canned(VecDeque::new()),
            registry,
            Script::Canned(VecDeque::new()),
            projects,
        )
        .await;
        let mut ws = harness.connect().await;

        send(&mut ws, 1, client_frame::Msg::ListModels(ListModels {})).await;
        let frame = next_frame(&mut ws).await;
        let listed = match frame.msg {
            Some(server_frame::Msg::ModelList(list)) => list.choices,
            other => panic!("expected ModelList, got {other:?}"),
        };
        assert_eq!(listed.len(), 2, "{listed:?}");
        for role in [SessionRole::Code, SessionRole::Executor] {
            let choice = listed
                .iter()
                .find(|c| c.role == role as i32)
                .expect("role choice");
            assert_eq!(choice.name, "test-model");
            assert!(choice.selected, "the only choice is the pick");
        }

        send(
            &mut ws,
            2,
            client_frame::Msg::SelectModel(SelectModel {
                role: SessionRole::Executor as i32,
                choice: "nope".to_owned(),
            }),
        )
        .await;
        let frame = next_frame(&mut ws).await;
        match frame.msg {
            Some(server_frame::Msg::Error(error)) => assert_eq!(error.code, "unknown_choice"),
            other => panic!("expected Error, got {other:?}"),
        }

        send(
            &mut ws,
            3,
            client_frame::Msg::SelectModel(SelectModel {
                role: SessionRole::Executor as i32,
                choice: "test-model".to_owned(),
            }),
        )
        .await;
        let frame = next_frame(&mut ws).await;
        match frame.msg {
            Some(server_frame::Msg::ModelList(list)) => assert!(list.choices[0].selected),
            other => panic!("expected ModelList, got {other:?}"),
        }
    }

    async fn subscribe(ws: &mut Client, request_id: u64) {
        send(ws, request_id, client_frame::Msg::Subscribe(Subscribe {})).await;
    }

    #[tokio::test]
    async fn a_subscribed_connection_is_pushed_job_changed_and_session_appended_notifications() {
        let (registry, _project_dir, projects) = dispatch_registry_and_projects();
        let executor_script = Script::Canned(VecDeque::from([vec![
            Ok(CompletionDelta::Text("on it".to_owned())),
            Ok(CompletionDelta::Done {
                usage: usage(),
                stop: Stop::EndTurn,
            }),
        ]]));
        let mut harness = Harness::with_executor(
            dispatching_chat("fix the failing test"),
            registry,
            executor_script,
            projects,
        )
        .await;

        let mut subscriber = harness.connect().await;
        subscribe(&mut subscriber, 9).await;

        let mut ws = harness.connect().await;
        send(&mut ws, 1, say("", "start a job")).await;
        let accepted = next_frame(&mut ws).await;
        let parent_id = match accepted.msg {
            Some(server_frame::Msg::MessageAccepted(m)) => m.session_id,
            other => panic!("expected MessageAccepted, got {other:?}"),
        };
        run_turn_to_end(&mut ws, 1).await;

        harness.drain_jobs().await;

        let mut saw_finished_job = false;
        let mut saw_handback_append = false;
        for _ in 0..40 {
            if saw_finished_job && saw_handback_append {
                break;
            }
            let frame = next_frame(&mut subscriber).await;
            assert_eq!(
                frame.request_id, 9,
                "pushes carry the subscribe's request id"
            );
            match frame.msg.expect("a message") {
                server_frame::Msg::Notification(Notification {
                    event: Some(notification::Event::JobChanged(job)),
                }) => {
                    if job.state == job_info::State::Finished as i32 {
                        saw_finished_job = true;
                    }
                }
                server_frame::Msg::Notification(Notification {
                    event: Some(notification::Event::SessionAppended(appended)),
                }) => {
                    if appended.session_id == parent_id {
                        saw_handback_append = true;
                    }
                }
                other => panic!("expected a Notification, got {other:?}"),
            }
        }
        assert!(
            saw_finished_job,
            "the finished job's state change was pushed"
        );
        assert!(saw_handback_append, "the handback's append was pushed");

        harness.stop().await;
    }

    /// The hard interleave guarantee: a notification must never land between
    /// two frames of the same streaming reply. A chat turn is gated
    /// mid-stream on this connection while a job finishes independently (its
    /// notifications queue up unread, since this connection's task is deep
    /// inside `request`, not back at the `select!` that reads them); only
    /// once the turn's `StreamEnd` is written does the connection loop
    /// return to `select!` and start draining the queued pushes.
    #[tokio::test]
    async fn a_notification_never_interleaves_a_streaming_turns_frames() {
        let dir = TempDir::new().expect("temp dir");
        let root = dir.path().join("proj");
        std::fs::create_dir_all(&root).expect("mkdir proj");
        let log = Log::open(dir.path()).expect("open log");
        let index = dir.path().join("index.db");
        let mut projection = Projection::open(&index).expect("open projection");
        arc_core::projection::replay(log.reader().expect("reader"), &mut projection)
            .expect("replay");

        let (notifier, _receiver) = broadcast::channel(256);

        let chat_gate = Arc::new(tokio::sync::Notify::new());
        let chat_provider = ScriptedProvider::scripted_steps(vec![
            Step::Gated {
                before: vec![Ok(CompletionDelta::Text("part one ".to_owned()))],
                notify: Arc::clone(&chat_gate),
                after: vec![
                    Ok(CompletionDelta::Text("part two".to_owned())),
                    Ok(CompletionDelta::Done {
                        usage: usage(),
                        stop: Stop::EndTurn,
                    }),
                ],
            },
            // the job's handback starts a turn on its own parent session
            Step::Immediate(done_reply("noted")),
        ]) as Arc<dyn Provider>;
        let executor_provider = ScriptedProvider::scripted(vec![done_reply("job done")]);

        let engine = Arc::new(
            Engine::new(Store::new(log, projection), Registry::new(512))
                .with_projects(BTreeMap::from([(
                    "arc".to_owned(),
                    ProjectSpec {
                        sources: Vec::new(),
                        grants: vec![arc_core::tool::workspace::Grant::new(
                            &root,
                            arc_core::tool::workspace::Mode::ReadWrite,
                        )],
                        command_prefix: Vec::new(),
                    },
                )]))
                .with_notifier(notifier.clone()),
        );
        let chat_runner = Runner {
            role: SessionRole::Chat,
            provider: chat_provider,
            model: "test-model".to_owned(),
            thinking: Thinking::Default,
            system: Some("be terse".to_owned()),
            compact_at: None,
            context_window: None,
            editing: arc_core::tool::Editing::Replacement,
        };
        let executor_runner = Runner {
            role: SessionRole::Executor,
            provider: Arc::clone(&executor_provider) as Arc<dyn Provider>,
            model: "test-model".to_owned(),
            thinking: Thinking::Default,
            system: None,
            compact_at: None,
            context_window: None,
            editing: arc_core::tool::Editing::Replacement,
        };
        let supervisor = Arc::new(
            Supervisor::for_test(
                Arc::clone(&engine),
                BTreeMap::from([(SessionRole::Executor, executor_runner)]),
            )
            .with_notifier(notifier.clone())
            .with_chat(chat_runner),
        );

        let reads = Arc::new(Reader::open(&index).expect("open reads"));
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("local addr");
        let (shutdown, signal) = oneshot::channel();
        let server = tokio::spawn(serve(
            listener,
            Arc::clone(&engine),
            reads,
            Arc::clone(&supervisor),
            notifier,
            async {
                let _ = signal.await;
            },
        ));

        let (mut ws, _) = tokio_tungstenite::connect_async(format!("ws://{addr}"))
            .await
            .expect("connect");
        subscribe(&mut ws, 9).await;

        send(&mut ws, 2, say("", "hello")).await;
        let accepted = next_frame(&mut ws).await;
        assert_eq!(accepted.request_id, 2);
        assert!(matches!(
            accepted.msg,
            Some(server_frame::Msg::MessageAccepted(_))
        ));
        let first_delta = next_frame(&mut ws).await;
        assert_eq!(first_delta.request_id, 2);
        match &first_delta.msg {
            Some(server_frame::Msg::Delta(delta)) => assert_eq!(delta.text, "part one "),
            other => panic!("expected the first delta, got {other:?}"),
        }

        // the turn is now stalled server-side; run a whole job to completion
        // (including its handback) while it stays that way
        let job_id = engine
            .create_bound_session(
                &supervisor
                    .role_runner(SessionRole::Executor)
                    .expect("runner"),
                "arc",
                SessionRole::Executor,
                None,
            )
            .expect("durable job");
        supervisor.spawn(arc_core::session::DispatchedJob {
            session_id: job_id,
            parent_session: "s-parent".to_owned(),
            role: SessionRole::Executor,
            project: "arc".to_owned(),
            brief: "fix the failing test".to_owned(),
            budget: None,
        });
        for _ in 0..400 {
            let handed_back = replay_log(dir.path()).into_iter().any(|event| {
                matches!(event, session_event::Event::MessageAppended(m) if m.session_id == "s-parent")
            });
            if handed_back {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }

        // only now release the gate: the turn's remaining frames must still
        // arrive with no notification spliced in between
        chat_gate.notify_one();

        let second_delta = next_frame(&mut ws).await;
        assert_eq!(second_delta.request_id, 2, "no push interleaves mid-turn");
        match &second_delta.msg {
            Some(server_frame::Msg::Delta(delta)) => assert_eq!(delta.text, "part two"),
            other => panic!("expected the second delta, got {other:?}"),
        }
        let end = next_frame(&mut ws).await;
        assert_eq!(end.request_id, 2, "no push interleaves mid-turn");
        assert!(matches!(end.msg, Some(server_frame::Msg::StreamEnd(_))));

        let mut saw_finished_job = false;
        for _ in 0..40 {
            if saw_finished_job {
                break;
            }
            let frame = next_frame(&mut ws).await;
            assert_eq!(frame.request_id, 9, "everything after StreamEnd is a push");
            match frame.msg.expect("a message") {
                server_frame::Msg::Notification(Notification {
                    event: Some(notification::Event::JobChanged(job)),
                }) if job.state == job_info::State::Finished as i32 => {
                    saw_finished_job = true;
                }
                server_frame::Msg::Notification(_) => {}
                other => panic!("expected a Notification, got {other:?}"),
            }
        }
        assert!(
            saw_finished_job,
            "the deferred push arrived once the turn ended"
        );

        let _ = shutdown.send(());
        tokio::time::timeout(PATIENCE, server)
            .await
            .expect("server stops within the grace")
            .expect("server task");
        supervisor.shutdown().await;
    }
}
