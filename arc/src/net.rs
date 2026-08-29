use arc_core::client::{Client, Error, TurnEvent};
use arc_proto::v1::{Notification, notification};
use tokio::sync::mpsc;

use crate::app::{Command, NetEvent, ReviewEntry};

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
        // main.rs writes the OSC 52 sequence itself; this never reaches the socket
        Command::Yank(_) => Ok(()),
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
                        superseded: !item.superseded_by.is_empty(),
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

/// `SessionForked` composes into the picker's own open-session path: set
/// `session_id`, fetch history — that IS rewind.
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

/// A branch's standing (row 2.4): a bare verdict wire-side, but the picker
/// wants the fresh disposition on screen, so a success also refreshes the list.
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
    let mut turn = client.send_message(session_id, content).await?;
    while let Some(event) = turn.next().await? {
        let event = match event {
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
            TurnEvent::ToolCallEnded { call_id, outcome } => {
                NetEvent::ToolEnded { call_id, outcome }
            }
            TurnEvent::End {
                input_tokens,
                output_tokens,
                partial,
                step_capped,
                grounding_json,
            } => NetEvent::End {
                partial,
                input_tokens,
                output_tokens,
                step_capped,
                grounding_json,
            },
            TurnEvent::Failed { code, msg } => NetEvent::Failed { code, msg },
        };
        let _ = events.send(event);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use arc_proto::v1::{
        ClientFrame, JobInfo, JobList, MemoryReviewItems, ServerFrame, SessionList, client_frame,
        server_frame,
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
            NetEvent::JobItems(vec![job]),
            "a browsing command, not a send, drove the reconnect"
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
