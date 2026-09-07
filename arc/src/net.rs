use arc_core::client::{Client, Error, Turn, TurnEvent};
use arc_proto::v1::{Notification, notification};
use tokio::sync::mpsc;

use crate::app::{Command, NetEvent, ReviewEntry};

fn branch_label(branch: &arc_proto::v1::BranchPointer) -> String {
    if branch.title.is_empty() {
        branch.session_id.chars().take(8).collect()
    } else {
        branch.title.clone()
    }
}

// with no request in flight, a pushed frame just sits unread in the socket;
// selecting over commands and the client's own frame read is what makes
// notifications arrive without a poll timer
pub async fn run(
    url: String,
    mut commands: mpsc::UnboundedReceiver<Command>,
    events: mpsc::UnboundedSender<NetEvent>,
) {
    let mut client: Option<Client> = None;

    loop {
        let Some(mut connected) = client.take() else {
            let Some(command) = commands.recv().await else {
                return;
            };
            client = match connect(&url, &events).await {
                Some(fresh) => handle(fresh, command, &events).await,
                None => None,
            };
            continue;
        };

        client = tokio::select! {
            command = commands.recv() => match command {
                Some(command) => handle(connected, command, &events).await,
                None => return,
            },
            result = connected.next_notification() => match result {
                Ok(notification) => {
                    dispatch(notification, &events);
                    Some(connected)
                }
                Err(error) => {
                    let _ = events.send(NetEvent::Disconnected {
                        reason: error.to_string(),
                    });
                    None
                }
            },
        };
    }
}

pub async fn run_metadata(
    url: String,
    mut requests: mpsc::UnboundedReceiver<()>,
    events: mpsc::UnboundedSender<NetEvent>,
) {
    let mut client: Option<Client> = None;
    loop {
        if let Some(connected) = client.as_mut() {
            tokio::select! {
                request = requests.recv() => {
                    if request.is_none() {
                        return;
                    }
                }
                notification = connected.next_notification() => match notification {
                    Ok(Notification {
                        event: Some(notification::Event::SessionAppended(_)),
                    }) => {}
                    Ok(_) => continue,
                    Err(error) => {
                        tracing::warn!(%error, "session metadata subscription failed");
                        client = None;
                        continue;
                    }
                }
            }
        } else if requests.recv().await.is_none() {
            return;
        }
        while requests.try_recv().is_ok() {}
        if let Some(connected) = client.as_mut() {
            while connected.poll_notification().is_some() {}
        }
        let result = async {
            if client.is_none() {
                let mut connected = Client::connect(&url).await?;
                connected.subscribe().await?;
                client = Some(connected);
            }
            client.as_mut().expect("connected").list_sessions().await
        }
        .await;
        match result {
            Ok(sessions) => {
                let _ = events.send(NetEvent::Sessions(sessions));
            }
            Err(error) => {
                tracing::warn!(%error, "session metadata refresh failed");
                client = None;
            }
        }
    }
}

pub async fn run_control(
    url: String,
    mut commands: mpsc::UnboundedReceiver<Command>,
    events: mpsc::UnboundedSender<NetEvent>,
) {
    let mut client: Option<Client> = None;
    while let Some(command) = commands.recv().await {
        let mut connected = match client.take() {
            Some(connected) => connected,
            None => match Client::connect(&url).await {
                Ok(connected) => connected,
                Err(error) => {
                    let _ = events.send(NetEvent::Disconnected {
                        reason: error.to_string(),
                    });
                    continue;
                }
            },
        };
        let result = match command {
            Command::CancelTurn { session_id } => connected.cancel_turn(&session_id).await,
            Command::SendLive {
                session_id,
                content,
            } => match connected.send_message(Some(&session_id), &content).await {
                Ok(turn) => drive_turn(turn, &events).await,
                Err(error) => Err(error),
            },
            _ => {
                client = Some(connected);
                continue;
            }
        };
        match result {
            Ok(()) => client = Some(connected),
            Err(Error::Server { code, msg }) => {
                let _ = events.send(NetEvent::Failed { code, msg });
                client = Some(connected);
            }
            Err(error) => {
                let _ = events.send(NetEvent::Disconnected {
                    reason: error.to_string(),
                });
            }
        }
    }
}

async fn connect(url: &str, events: &mpsc::UnboundedSender<NetEvent>) -> Option<Client> {
    let mut client = match Client::connect(url).await {
        Ok(client) => client,
        Err(error) => {
            let _ = events.send(NetEvent::Disconnected {
                reason: error.to_string(),
            });
            return None;
        }
    };
    if let Err(error) = client.subscribe().await {
        let _ = events.send(NetEvent::Disconnected {
            reason: error.to_string(),
        });
        return None;
    }
    // best-effort: the indicator seeds on the next push if this fails
    let since = chrono::Utc::now().timestamp_micros() - arc_core::projection::REVIEW_WINDOW_MICROS;
    if let Ok(items) = client.review_items(since).await {
        let pending = u32::try_from(items.len()).unwrap_or(u32::MAX);
        let _ = events.send(NetEvent::ReviewChanged(pending));
    }
    // seeds a launch-directory door guess; C still fetches its own fresh list
    if let Ok(projects) = client.projects().await {
        let _ = events.send(NetEvent::ProjectsSeeded(projects));
    }
    Some(client)
}

fn dispatch(notification: Notification, events: &mpsc::UnboundedSender<NetEvent>) {
    match notification.event {
        Some(notification::Event::SessionAppended(appended)) => {
            let _ = events.send(NetEvent::SessionAppended {
                session_id: appended.session_id,
            });
        }
        Some(notification::Event::JobChanged(job)) => {
            let _ = events.send(NetEvent::JobChanged(job));
        }
        Some(notification::Event::ReviewChanged(changed)) => {
            let _ = events.send(NetEvent::ReviewChanged(changed.pending));
        }
        Some(notification::Event::JobReasoning(delta)) => {
            let _ = events.send(NetEvent::JobReasoning {
                session_id: delta.session_id,
                text: delta.text,
            });
        }
        Some(notification::Event::ModelsChanged(list)) => {
            let _ = events.send(NetEvent::ModelItems(list.choices));
        }
        None => {}
    }
}

async fn handle(
    mut client: Client,
    command: Command,
    events: &mpsc::UnboundedSender<NetEvent>,
) -> Option<Client> {
    let result = match command {
        Command::List => list(&mut client, events).await,
        Command::History { session_id } => history(&mut client, &session_id, events).await,
        Command::Send {
            session_id,
            content,
        } => send(&mut client, session_id.as_deref(), &content, events).await,
        Command::ReviewList { since_micros } => {
            review_list(&mut client, since_micros, events).await
        }
        Command::ReviewAccept { record_id } => {
            verdict(client.review_accept(&record_id).await, events)
        }
        Command::ReviewDelete { record_id } => {
            verdict(client.review_delete(&record_id).await, events)
        }
        Command::ListJobs => list_jobs(&mut client, events).await,
        Command::ListProjects => list_projects(&mut client, events).await,
        Command::ListModels => models(client.models().await, events),
        Command::SelectModel { role, choice } => {
            models(client.select_model(role, &choice).await, events)
        }
        Command::CancelJob { session_id } => verdict(client.cancel_job(&session_id).await, events),
        Command::DropSteers { session_id } => {
            verdict(client.drop_steers(&session_id).await, events)
        }
        Command::CreateSession { role, project } => {
            create_session(&mut client, role, &project, events).await
        }
        Command::ForkSession {
            session_id,
            fork_point,
        } => fork_session(&mut client, &session_id, fork_point, events).await,
        Command::MarkBranch {
            session_id,
            disposition,
        } => mark_branch(&mut client, &session_id, disposition, events).await,
        Command::CompactSession { session_id } => {
            compact_session(&mut client, &session_id, events).await
        }
        // main.rs writes the OSC 52 sequence itself; this never reaches the
        // socket, and CancelTurn/SendLive go to run_control's own connection
        Command::CancelTurn { .. } | Command::SendLive { .. } | Command::Yank(_) => Ok(()),
    };
    match result {
        Ok(()) => Some(client),
        Err(error) => {
            let _ = events.send(NetEvent::Disconnected {
                reason: error.to_string(),
            });
            // dropping the client forces a reconnect on the next command
            None
        }
    }
}

async fn list(client: &mut Client, events: &mpsc::UnboundedSender<NetEvent>) -> Result<(), Error> {
    match client.list_sessions().await {
        Ok(sessions) => {
            let _ = events.send(NetEvent::Sessions(sessions));
            Ok(())
        }
        Err(Error::Server { code, msg }) => {
            let _ = events.send(NetEvent::Failed { code, msg });
            Ok(())
        }
        Err(error) => Err(error),
    }
}

async fn history(
    client: &mut Client,
    session_id: &str,
    events: &mpsc::UnboundedSender<NetEvent>,
) -> Result<(), Error> {
    match client.fetch_history(session_id).await {
        Ok(answer) => {
            let _ = events.send(NetEvent::History {
                branches: answer
                    .branches
                    .into_iter()
                    .map(|b| (b.fork_point, branch_label(&b)))
                    .collect(),
                session_id: session_id.to_owned(),
                entries: answer.entries,
                parent_session: answer.parent_session,
                fork_point: answer.fork_point,
            });
            Ok(())
        }
        Err(Error::Server { code, msg }) => {
            let _ = events.send(NetEvent::Failed { code, msg });
            Ok(())
        }
        Err(error) => Err(error),
    }
}

async fn review_list(
    client: &mut Client,
    since_micros: i64,
    events: &mpsc::UnboundedSender<NetEvent>,
) -> Result<(), Error> {
    match client.review_items(since_micros).await {
        Ok(items) => {
            let entries = items
                .into_iter()
                .filter_map(|item| {
                    let record = item.record?;
                    Some(ReviewEntry {
                        id: record.id,
                        kind: record.kind,
                        namespace: record.namespace,
                        title: record.title,
                        summary: record.summary,
                        body: record.body,
                        supersedes: item
                            .supersedes
                            .into_iter()
                            .map(|p| (p.id, p.title))
                            .collect(),
                    })
                })
                .collect();
            let _ = events.send(NetEvent::ReviewItems(entries));
            Ok(())
        }
        Err(Error::Server { code, msg }) => {
            let _ = events.send(NetEvent::Failed { code, msg });
            Ok(())
        }
        Err(error) => Err(error),
    }
}

async fn list_jobs(
    client: &mut Client,
    events: &mpsc::UnboundedSender<NetEvent>,
) -> Result<(), Error> {
    match client.jobs().await {
        Ok(jobs) => {
            let _ = events.send(NetEvent::JobItems(jobs));
            Ok(())
        }
        Err(Error::Server { code, msg }) => {
            let _ = events.send(NetEvent::Failed { code, msg });
            Ok(())
        }
        Err(error) => Err(error),
    }
}

async fn list_projects(
    client: &mut Client,
    events: &mpsc::UnboundedSender<NetEvent>,
) -> Result<(), Error> {
    match client.projects().await {
        Ok(projects) => {
            let _ = events.send(NetEvent::ProjectItems(projects));
            Ok(())
        }
        Err(Error::Server { code, msg }) => {
            let _ = events.send(NetEvent::Failed { code, msg });
            Ok(())
        }
        Err(error) => Err(error),
    }
}

fn models(
    outcome: Result<Vec<arc_proto::v1::ModelChoice>, Error>,
    events: &mpsc::UnboundedSender<NetEvent>,
) -> Result<(), Error> {
    match outcome {
        Ok(items) => {
            let _ = events.send(NetEvent::ModelItems(items));
            Ok(())
        }
        Err(Error::Server { code, msg }) => {
            let _ = events.send(NetEvent::Failed { code, msg });
            Ok(())
        }
        Err(error) => Err(error),
    }
}

async fn create_session(
    client: &mut Client,
    role: arc_proto::v1::SessionRole,
    project: &str,
    events: &mpsc::UnboundedSender<NetEvent>,
) -> Result<(), Error> {
    match client.create_session(role, project).await {
        Ok(session_id) => {
            let _ = events.send(NetEvent::SessionCreated { session_id });
            Ok(())
        }
        Err(Error::Server { code, msg }) => {
            let _ = events.send(NetEvent::Failed { code, msg });
            Ok(())
        }
        Err(error) => Err(error),
    }
}

async fn fork_session(
    client: &mut Client,
    session_id: &str,
    fork_point: u64,
    events: &mpsc::UnboundedSender<NetEvent>,
) -> Result<(), Error> {
    match client.fork_session(session_id, fork_point).await {
        Ok(session_id) => {
            let _ = events.send(NetEvent::SessionForked { session_id });
            Ok(())
        }
        Err(Error::Server { code, msg }) => {
            let _ = events.send(NetEvent::Failed { code, msg });
            Ok(())
        }
        Err(error) => Err(error),
    }
}

async fn mark_branch(
    client: &mut Client,
    session_id: &str,
    disposition: arc_proto::v1::branch_marked::Disposition,
    events: &mpsc::UnboundedSender<NetEvent>,
) -> Result<(), Error> {
    match client.mark_branch(session_id, disposition).await {
        Ok(()) => list(client, events).await,
        Err(Error::Server { code, msg }) => {
            let _ = events.send(NetEvent::Failed { code, msg });
            Ok(())
        }
        Err(error) => Err(error),
    }
}

async fn compact_session(
    client: &mut Client,
    session_id: &str,
    events: &mpsc::UnboundedSender<NetEvent>,
) -> Result<(), Error> {
    match client.compact_session(session_id).await {
        Ok(()) => {
            let _ = events.send(NetEvent::Compacted {
                session_id: session_id.to_owned(),
            });
            Ok(())
        }
        Err(Error::Server { code, msg }) => {
            let _ = events.send(NetEvent::Failed { code, msg });
            Ok(())
        }
        Err(error) => Err(error),
    }
}

fn verdict(
    result: Result<(), Error>,
    events: &mpsc::UnboundedSender<NetEvent>,
) -> Result<(), Error> {
    match result {
        Ok(()) => Ok(()),
        Err(Error::Server { code, msg }) => {
            let _ = events.send(NetEvent::Failed { code, msg });
            Ok(())
        }
        Err(error) => Err(error),
    }
}

async fn send(
    client: &mut Client,
    session_id: Option<&str>,
    content: &str,
    events: &mpsc::UnboundedSender<NetEvent>,
) -> Result<(), Error> {
    let turn = client.send_message(session_id, content).await?;
    drive_turn(turn, events).await
}

/// Drives a turn to completion, mapping each event to the app. Shared by the
/// main socket's `send()` and the control socket's `SendLive`, so a message
/// landing in an already-live turn is handled the same way either arrives.
async fn drive_turn(
    mut turn: Turn<'_>,
    events: &mpsc::UnboundedSender<NetEvent>,
) -> Result<(), Error> {
    while let Some(event) = turn.next().await? {
        let _ = events.send(map_turn_event(event));
    }
    Ok(())
}

fn map_turn_event(event: TurnEvent) -> NetEvent {
    match event {
        TurnEvent::Accepted { session_id } => NetEvent::Accepted { session_id },
        TurnEvent::Delta(text) => NetEvent::Delta(text),
        TurnEvent::Reasoning(text) => NetEvent::Reasoning(text),
        TurnEvent::ToolCallStarted {
            call_id,
            name,
            arguments_json,
            ..
        } => NetEvent::ToolStarted {
            call_id,
            name,
            arguments_json,
        },
        TurnEvent::ToolCallEnded {
            call_id,
            outcome,
            content,
        } => NetEvent::ToolEnded {
            call_id,
            outcome,
            content,
        },
        TurnEvent::End {
            input_tokens,
            output_tokens,
            partial,
            step_capped,
            grounding_json,
            queued,
        } => NetEvent::End {
            partial,
            input_tokens,
            output_tokens,
            step_capped,
            grounding_json,
            queued,
        },
        TurnEvent::Failed { code, msg } => NetEvent::Failed { code, msg },
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use arc_proto::v1::{
        ClientFrame, Delta, JobInfo, JobList, MemoryReviewItems, MessageAccepted, ProjectList,
        ServerFrame, SessionList, StreamEnd, client_frame, server_frame,
    };
    use futures::{SinkExt as _, StreamExt as _};
    use prost::Message as _;
    use tokio::net::TcpListener;
    use tokio_tungstenite::WebSocketStream;
    use tokio_tungstenite::tungstenite::Message as WsMessage;

    use super::*;

    const PATIENCE: Duration = Duration::from_secs(5);

    async fn expect_frame(ws: &mut WebSocketStream<tokio::net::TcpStream>) -> ClientFrame {
        loop {
            match ws
                .next()
                .await
                .expect("a client frame")
                .expect("no io error")
            {
                WsMessage::Binary(bytes) => return ClientFrame::decode(bytes).expect("decode"),
                WsMessage::Ping(_) | WsMessage::Pong(_) => {}
                other => panic!("expected a binary frame, got {other:?}"),
            }
        }
    }

    async fn reply(
        ws: &mut WebSocketStream<tokio::net::TcpStream>,
        request_id: u64,
        msg: server_frame::Msg,
    ) {
        let frame = ServerFrame {
            request_id,
            msg: Some(msg),
        };
        ws.send(WsMessage::binary(frame.encode_to_vec()))
            .await
            .expect("send");
    }

    async fn next_event(events: &mut mpsc::UnboundedReceiver<NetEvent>) -> NetEvent {
        tokio::time::timeout(PATIENCE, events.recv())
            .await
            .expect("an event arrives within PATIENCE")
            .expect("the event channel stays open")
    }

    #[tokio::test]
    async fn a_metadata_failure_does_not_end_the_turn_and_the_next_refresh_reconnects() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let url = format!("ws://{}", listener.local_addr().expect("address"));
        let (requests, receiver) = mpsc::unbounded_channel();
        let retry = requests.clone();
        let server = tokio::spawn(async move {
            for fail in [true, false] {
                let (stream, _) = listener.accept().await.expect("connection");
                let mut ws = tokio_tungstenite::accept_async(stream)
                    .await
                    .expect("handshake");
                assert!(matches!(
                    expect_frame(&mut ws).await.msg,
                    Some(client_frame::Msg::Subscribe(_))
                ));
                let request = expect_frame(&mut ws).await;
                assert!(matches!(
                    request.msg,
                    Some(client_frame::Msg::ListSessions(_))
                ));
                let response = if fail {
                    retry
                        .send(())
                        .expect("retry while first request is in flight");
                    server_frame::Msg::Error(arc_proto::v1::Error {
                        code: "busy".into(),
                        msg: "retry later".into(),
                    })
                } else {
                    server_frame::Msg::SessionList(SessionList { sessions: vec![] })
                };
                reply(&mut ws, request.request_id, response).await;
            }
        });
        let (events_tx, mut events) = mpsc::unbounded_channel();
        let task = tokio::spawn(run_metadata(url, receiver, events_tx));
        requests.send(()).expect("first refresh");
        assert_eq!(next_event(&mut events).await, NetEvent::Sessions(vec![]));
        drop(requests);
        task.await.expect("metadata task");
        server.await.expect("server");
        assert!(events.recv().await.is_none());
    }

    #[tokio::test]
    async fn metadata_bursts_coalesce_without_losing_in_flight_invalidations() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let url = format!("ws://{}", listener.local_addr().expect("address"));
        let (requests, receiver) = mpsc::unbounded_channel();
        let burst_requests = requests.clone();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("connection");
            let mut ws = tokio_tungstenite::accept_async(stream)
                .await
                .expect("handshake");
            let subscription = expect_frame(&mut ws).await;
            assert!(matches!(
                subscription.msg,
                Some(client_frame::Msg::Subscribe(_))
            ));
            for version in 0..3 {
                let list = expect_frame(&mut ws).await;
                assert!(matches!(list.msg, Some(client_frame::Msg::ListSessions(_))));
                if version < 2 {
                    for _ in 0..64 {
                        burst_requests.send(()).expect("refresh request");
                        reply(
                            &mut ws,
                            subscription.request_id,
                            server_frame::Msg::Notification(Notification {
                                event: Some(notification::Event::SessionAppended(
                                    arc_proto::v1::SessionAppended {
                                        session_id: "session".into(),
                                    },
                                )),
                            }),
                        )
                        .await;
                    }
                }
                reply(
                    &mut ws,
                    list.request_id,
                    server_frame::Msg::SessionList(SessionList {
                        sessions: vec![arc_proto::v1::SessionInfo {
                            id: "session".into(),
                            title: format!("Title {version}"),
                            ..Default::default()
                        }],
                    }),
                )
                .await;
            }
            assert!(
                tokio::time::timeout(std::time::Duration::from_millis(100), ws.next())
                    .await
                    .is_err(),
                "bursts need only one follow-up snapshot each"
            );
        });
        let (events_tx, mut events) = mpsc::unbounded_channel();
        for _ in 0..64 {
            requests.send(()).expect("initial burst");
        }
        let task = tokio::spawn(run_metadata(url, receiver, events_tx));
        for version in 0..3 {
            let NetEvent::Sessions(sessions) = next_event(&mut events).await else {
                panic!("metadata snapshot")
            };
            assert_eq!(sessions[0].title, format!("Title {version}"));
        }
        tokio::time::timeout(PATIENCE, server)
            .await
            .expect("bounded requests")
            .expect("server");
        drop(requests);
        task.await.expect("metadata task");
        assert!(events.recv().await.is_none());
    }

    #[tokio::test]
    async fn metadata_arrives_while_a_turn_is_still_streaming() {
        metadata_push_during_turn(false).await;
        metadata_push_during_turn(true).await;
    }

    async fn metadata_push_during_turn(during_reply: bool) {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let url = format!("ws://{}", listener.local_addr().expect("address"));
        let (release, wait) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("turn connection");
            let mut turn = tokio_tungstenite::accept_async(stream)
                .await
                .expect("handshake");
            assert!(matches!(
                expect_frame(&mut turn).await.msg,
                Some(client_frame::Msg::Subscribe(_))
            ));
            let review = expect_frame(&mut turn).await;
            assert!(matches!(
                review.msg,
                Some(client_frame::Msg::MemoryReviewList(_))
            ));
            reply(
                &mut turn,
                review.request_id,
                server_frame::Msg::MemoryReviewItems(MemoryReviewItems { items: vec![] }),
            )
            .await;
            let projects = expect_frame(&mut turn).await;
            assert!(matches!(
                projects.msg,
                Some(client_frame::Msg::ListProjects(_))
            ));
            reply(
                &mut turn,
                projects.request_id,
                server_frame::Msg::ProjectList(ProjectList { projects: vec![] }),
            )
            .await;
            let request = expect_frame(&mut turn).await;
            assert!(matches!(
                request.msg,
                Some(client_frame::Msg::SendMessage(_))
            ));
            reply(
                &mut turn,
                request.request_id,
                server_frame::Msg::MessageAccepted(MessageAccepted {
                    session_id: "new".into(),
                }),
            )
            .await;
            let (stream, _) = listener.accept().await.expect("metadata connection");
            let mut metadata = tokio_tungstenite::accept_async(stream)
                .await
                .expect("handshake");
            let subscription = expect_frame(&mut metadata).await;
            assert!(matches!(
                subscription.msg,
                Some(client_frame::Msg::Subscribe(_))
            ));
            let list = expect_frame(&mut metadata).await;
            let appended = server_frame::Msg::Notification(Notification {
                event: Some(notification::Event::SessionAppended(
                    arc_proto::v1::SessionAppended {
                        session_id: "new".into(),
                    },
                )),
            });
            if during_reply {
                reply(&mut metadata, subscription.request_id, appended.clone()).await;
            }
            assert!(matches!(list.msg, Some(client_frame::Msg::ListSessions(_))));
            reply(
                &mut metadata,
                list.request_id,
                server_frame::Msg::SessionList(SessionList {
                    sessions: vec![arc_proto::v1::SessionInfo {
                        id: "new".into(),
                        provider: "codex".into(),
                        model: "recorded".into(),
                        ..Default::default()
                    }],
                }),
            )
            .await;
            if !during_reply {
                reply(&mut metadata, subscription.request_id, appended).await;
            }
            let refresh = expect_frame(&mut metadata).await;
            assert!(matches!(
                refresh.msg,
                Some(client_frame::Msg::ListSessions(_))
            ));
            reply(
                &mut metadata,
                refresh.request_id,
                server_frame::Msg::SessionList(SessionList {
                    sessions: vec![arc_proto::v1::SessionInfo {
                        id: "new".into(),
                        title: "Saved task title".into(),
                        provider: "codex".into(),
                        model: "recorded".into(),
                        ..Default::default()
                    }],
                }),
            )
            .await;
            wait.await.expect("metadata received before turn ends");
            reply(
                &mut turn,
                request.request_id,
                server_frame::Msg::StreamEnd(StreamEnd {
                    session_id: "new".into(),
                    ..Default::default()
                }),
            )
            .await;
        });
        let (turn_commands, turn_rx) = mpsc::unbounded_channel();
        let (metadata_commands, metadata_rx) = mpsc::unbounded_channel();
        let (events_tx, mut events) = mpsc::unbounded_channel();
        let turn_task = tokio::spawn(run(url.clone(), turn_rx, events_tx.clone()));
        let metadata_task = tokio::spawn(run_metadata(url, metadata_rx, events_tx));
        turn_commands
            .send(Command::Send {
                session_id: Some("new".into()),
                content: "hello".into(),
            })
            .expect("send");
        assert_eq!(next_event(&mut events).await, NetEvent::ReviewChanged(0));
        assert_eq!(
            next_event(&mut events).await,
            NetEvent::ProjectsSeeded(vec![])
        );
        assert!(matches!(
            next_event(&mut events).await,
            NetEvent::Accepted { .. }
        ));
        metadata_commands.send(()).expect("list");
        let NetEvent::Sessions(sessions) = next_event(&mut events).await else {
            panic!("metadata before end")
        };
        assert_eq!(sessions[0].model, "recorded");
        let NetEvent::Sessions(sessions) = next_event(&mut events).await else {
            panic!("title refresh before end")
        };
        assert_eq!(sessions[0].title, "Saved task title");
        assert_eq!(sessions[0].model, "recorded");
        release.send(()).expect("release turn");
        assert!(matches!(
            next_event(&mut events).await,
            NetEvent::End { .. }
        ));
        server.await.expect("server");
        drop(turn_commands);
        drop(metadata_commands);
        turn_task.await.expect("turn task");
        metadata_task.await.expect("metadata task");
    }

    // a daemon restart looks like: the same address answers, but a fresh
    // accept — the client must re-dial and re-subscribe on its own
    #[tokio::test]
    async fn a_browsing_command_reconnects_after_the_daemon_restarts() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let url = format!("ws://{}", listener.local_addr().expect("local addr"));

        let job = JobInfo {
            session_id: "s-job".to_owned(),
            ..Default::default()
        };
        let job_for_server = job.clone();

        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept 1");
            let mut ws = tokio_tungstenite::accept_async(stream)
                .await
                .expect("handshake 1");
            assert!(matches!(
                expect_frame(&mut ws).await.msg,
                Some(client_frame::Msg::Subscribe(_))
            ));
            let review = expect_frame(&mut ws).await;
            assert!(matches!(
                review.msg,
                Some(client_frame::Msg::MemoryReviewList(_))
            ));
            reply(
                &mut ws,
                review.request_id,
                server_frame::Msg::MemoryReviewItems(MemoryReviewItems { items: vec![] }),
            )
            .await;
            let projects = expect_frame(&mut ws).await;
            assert!(matches!(
                projects.msg,
                Some(client_frame::Msg::ListProjects(_))
            ));
            reply(
                &mut ws,
                projects.request_id,
                server_frame::Msg::ProjectList(ProjectList { projects: vec![] }),
            )
            .await;
            let list = expect_frame(&mut ws).await;
            assert!(matches!(list.msg, Some(client_frame::Msg::ListSessions(_))));
            reply(
                &mut ws,
                list.request_id,
                server_frame::Msg::SessionList(SessionList { sessions: vec![] }),
            )
            .await;
            ws.close(None).await.expect("close conn 1");
            drop(ws);

            let (stream, _) = listener.accept().await.expect("accept 2");
            let mut ws = tokio_tungstenite::accept_async(stream)
                .await
                .expect("handshake 2");
            assert!(
                matches!(
                    expect_frame(&mut ws).await.msg,
                    Some(client_frame::Msg::Subscribe(_))
                ),
                "the reconnect re-subscribes before the command runs"
            );
            let review = expect_frame(&mut ws).await;
            assert!(matches!(
                review.msg,
                Some(client_frame::Msg::MemoryReviewList(_))
            ));
            reply(
                &mut ws,
                review.request_id,
                server_frame::Msg::MemoryReviewItems(MemoryReviewItems { items: vec![] }),
            )
            .await;
            let projects = expect_frame(&mut ws).await;
            assert!(matches!(
                projects.msg,
                Some(client_frame::Msg::ListProjects(_))
            ));
            reply(
                &mut ws,
                projects.request_id,
                server_frame::Msg::ProjectList(ProjectList { projects: vec![] }),
            )
            .await;
            let jobs = expect_frame(&mut ws).await;
            assert!(matches!(jobs.msg, Some(client_frame::Msg::ListJobs(_))));
            reply(
                &mut ws,
                jobs.request_id,
                server_frame::Msg::JobList(JobList {
                    jobs: vec![job_for_server],
                }),
            )
            .await;
        });

        let (commands, command_rx) = mpsc::unbounded_channel();
        let (event_tx, mut events) = mpsc::unbounded_channel();
        tokio::spawn(run(url, command_rx, event_tx));

        commands.send(Command::List).expect("net task alive");
        assert_eq!(
            next_event(&mut events).await,
            NetEvent::ReviewChanged(0),
            "connecting seeds the review indicator before the command runs"
        );
        assert_eq!(
            next_event(&mut events).await,
            NetEvent::ProjectsSeeded(vec![]),
            "connecting also seeds the project list, for a launch-directory door guess"
        );
        assert_eq!(next_event(&mut events).await, NetEvent::Sessions(vec![]));
        assert!(
            matches!(next_event(&mut events).await, NetEvent::Disconnected { .. }),
            "the daemon closing the socket surfaces on its own, with no command in flight"
        );

        commands.send(Command::ListJobs).expect("net task alive");
        assert_eq!(
            next_event(&mut events).await,
            NetEvent::ReviewChanged(0),
            "the reconnect seeds the indicator again"
        );
        assert_eq!(
            next_event(&mut events).await,
            NetEvent::ProjectsSeeded(vec![]),
            "the reconnect seeds the project list again too"
        );
        assert_eq!(
            next_event(&mut events).await,
            NetEvent::JobItems(vec![job]),
            "a browsing command, not a send, drove the reconnect"
        );

        tokio::time::timeout(PATIENCE, server)
            .await
            .expect("server finishes within PATIENCE")
            .expect("server task");
    }

    #[tokio::test]
    async fn run_control_sends_cancel_turn_and_surfaces_a_refusal() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let url = format!("ws://{}", listener.local_addr().expect("local addr"));

        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept");
            let mut ws = tokio_tungstenite::accept_async(stream)
                .await
                .expect("handshake");
            let frame = expect_frame(&mut ws).await;
            assert!(
                matches!(
                    &frame.msg,
                    Some(client_frame::Msg::CancelTurn(c)) if c.session_id == "s-idle"
                ),
                "got: {:?}",
                frame.msg
            );
            reply(
                &mut ws,
                frame.request_id,
                server_frame::Msg::Error(arc_proto::v1::Error {
                    code: "no_turn".to_owned(),
                    msg: "no turn is running on session s-idle".to_owned(),
                }),
            )
            .await;
        });

        let (commands, command_rx) = mpsc::unbounded_channel();
        let (event_tx, mut events) = mpsc::unbounded_channel();
        tokio::spawn(run_control(url, command_rx, event_tx));

        commands
            .send(Command::CancelTurn {
                session_id: "s-idle".to_owned(),
            })
            .expect("control task alive");
        assert_eq!(
            next_event(&mut events).await,
            NetEvent::Failed {
                code: "no_turn".to_owned(),
                msg: "no turn is running on session s-idle".to_owned(),
            }
        );

        tokio::time::timeout(PATIENCE, server)
            .await
            .expect("server finishes within PATIENCE")
            .expect("server task");
    }

    #[tokio::test]
    async fn a_send_live_queued_into_a_running_turn_emits_only_accepted_and_a_queued_end() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let url = format!("ws://{}", listener.local_addr().expect("local addr"));

        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept");
            let mut ws = tokio_tungstenite::accept_async(stream)
                .await
                .expect("handshake");
            let frame = expect_frame(&mut ws).await;
            assert!(
                matches!(
                    &frame.msg,
                    Some(client_frame::Msg::SendMessage(m)) if m.session_id == "s-1"
                ),
                "got: {:?}",
                frame.msg
            );
            reply(
                &mut ws,
                frame.request_id,
                server_frame::Msg::MessageAccepted(MessageAccepted {
                    session_id: "s-1".to_owned(),
                }),
            )
            .await;
            reply(
                &mut ws,
                frame.request_id,
                server_frame::Msg::StreamEnd(StreamEnd {
                    session_id: "s-1".to_owned(),
                    queued: true,
                    ..Default::default()
                }),
            )
            .await;
        });

        let (commands, command_rx) = mpsc::unbounded_channel();
        let (event_tx, mut events) = mpsc::unbounded_channel();
        tokio::spawn(run_control(url, command_rx, event_tx));

        commands
            .send(Command::SendLive {
                session_id: "s-1".to_owned(),
                content: "no, use GPIO 4".to_owned(),
            })
            .expect("control task alive");

        assert_eq!(
            next_event(&mut events).await,
            NetEvent::Accepted {
                session_id: "s-1".to_owned()
            }
        );
        assert_eq!(
            next_event(&mut events).await,
            NetEvent::End {
                partial: false,
                input_tokens: 0,
                output_tokens: 0,
                step_capped: false,
                grounding_json: String::new(),
                queued: true,
            },
            "the real reply streams on whichever request holds the live turn"
        );

        tokio::time::timeout(PATIENCE, server)
            .await
            .expect("server finishes within PATIENCE")
            .expect("server task");
    }

    #[tokio::test]
    async fn a_send_live_that_lands_a_fresh_turn_streams_it_like_the_main_socket_would() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let url = format!("ws://{}", listener.local_addr().expect("local addr"));

        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept");
            let mut ws = tokio_tungstenite::accept_async(stream)
                .await
                .expect("handshake");
            let frame = expect_frame(&mut ws).await;
            assert!(matches!(
                &frame.msg,
                Some(client_frame::Msg::SendMessage(m)) if m.session_id == "s-1"
            ));
            reply(
                &mut ws,
                frame.request_id,
                server_frame::Msg::MessageAccepted(MessageAccepted {
                    session_id: "s-1".to_owned(),
                }),
            )
            .await;
            reply(
                &mut ws,
                frame.request_id,
                server_frame::Msg::Delta(Delta {
                    session_id: "s-1".to_owned(),
                    text: "on it".to_owned(),
                }),
            )
            .await;
            reply(
                &mut ws,
                frame.request_id,
                server_frame::Msg::StreamEnd(StreamEnd {
                    session_id: "s-1".to_owned(),
                    queued: false,
                    ..Default::default()
                }),
            )
            .await;
        });

        let (commands, command_rx) = mpsc::unbounded_channel();
        let (event_tx, mut events) = mpsc::unbounded_channel();
        tokio::spawn(run_control(url, command_rx, event_tx));

        commands
            .send(Command::SendLive {
                session_id: "s-1".to_owned(),
                content: "one more thing".to_owned(),
            })
            .expect("control task alive");

        assert_eq!(
            next_event(&mut events).await,
            NetEvent::Accepted {
                session_id: "s-1".to_owned()
            }
        );
        assert_eq!(
            next_event(&mut events).await,
            NetEvent::Delta("on it".to_owned())
        );
        assert_eq!(
            next_event(&mut events).await,
            NetEvent::End {
                partial: false,
                input_tokens: 0,
                output_tokens: 0,
                step_capped: false,
                grounding_json: String::new(),
                queued: false,
            },
            "the previous turn had just ended, so this one runs and streams for real"
        );

        tokio::time::timeout(PATIENCE, server)
            .await
            .expect("server finishes within PATIENCE")
            .expect("server task");
    }

    #[test]
    fn a_pushed_job_reasoning_notification_dispatches_with_its_session_id() {
        let (events, mut rx) = mpsc::unbounded_channel();
        dispatch(
            Notification {
                event: Some(notification::Event::JobReasoning(
                    arc_proto::v1::ReasoningDelta {
                        session_id: "s-job".to_owned(),
                        text: "weighing options".to_owned(),
                    },
                )),
            },
            &events,
        );
        assert_eq!(
            rx.try_recv().expect("dispatched"),
            NetEvent::JobReasoning {
                session_id: "s-job".to_owned(),
                text: "weighing options".to_owned(),
            }
        );
    }

    #[test]
    fn a_pushed_review_changed_notification_dispatches_to_a_net_event() {
        let (events, mut rx) = mpsc::unbounded_channel();
        dispatch(
            Notification {
                event: Some(notification::Event::ReviewChanged(
                    arc_proto::v1::ReviewChanged { pending: 4 },
                )),
            },
            &events,
        );
        assert_eq!(
            rx.try_recv().expect("dispatched"),
            NetEvent::ReviewChanged(4)
        );
    }
}
