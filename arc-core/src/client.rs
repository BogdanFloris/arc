use std::collections::VecDeque;

use arc_proto::v1::{
    CancelJob, CancelTurn, ClientFrame, CreateSession, DropSteers, FetchHistory, ForkSession,
    JobInfo, ListJobs, ListProjects, ListSessions, MarkBranch, MemoryReviewAccept,
    MemoryReviewDelete, MemoryReviewItem, MemoryReviewList, Notification, ProjectInfo, SendMessage,
    ServerFrame, SessionHistory, SessionInfo, SessionRole, Subscribe, branch_marked, client_frame,
    server_frame,
};
use futures::{SinkExt as _, StreamExt as _};
use prost::Message as _;
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::Message as WsMessage;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, tungstenite};
use tracing::warn;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("connecting to the daemon failed: {0}")]
    Connect(#[source] tungstenite::Error),
    #[error("the connection failed: {0}")]
    Transport(#[source] tungstenite::Error),
    #[error("the daemon closed the connection")]
    Closed,
    #[error("the daemon refused the request ({code}): {msg}")]
    Server { code: String, msg: String },
    #[error("protocol violation: {0}")]
    Protocol(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TurnEvent {
    Accepted {
        session_id: String,
    },
    Delta(String),
    Reasoning(String),
    ToolCallStarted {
        call_id: String,
        index: u32,
        name: String,
        arguments_json: String,
    },
    ToolCallEnded {
        call_id: String,
        outcome: i32,
    },
    End {
        input_tokens: u32,
        output_tokens: u32,
        partial: bool,
        step_capped: bool,
        grounding_json: String,
    },
    Failed {
        code: String,
        msg: String,
    },
}

pub struct Client {
    ws: WebSocketStream<MaybeTlsStream<TcpStream>>,
    request_id: u64,
    subscription: Option<u64>,
    notifications: VecDeque<Notification>,
}

impl Client {
    #[tracing::instrument(name = "client.connect", skip_all, fields(url))]
    pub async fn connect(url: &str) -> Result<Self, Error> {
        let (ws, _) = tokio_tungstenite::connect_async(url)
            .await
            .map_err(Error::Connect)?;
        Ok(Self {
            ws,
            request_id: 0,
            subscription: None,
            notifications: VecDeque::new(),
        })
    }

    /// Opens the daemon's push stream. The reply never comes as a normal
    /// answer: notifications tagged with this request id surface through
    /// `poll_notification` instead.
    #[tracing::instrument(name = "client.subscribe", skip_all)]
    pub async fn subscribe(&mut self) -> Result<(), Error> {
        let id = self
            .send(client_frame::Msg::Subscribe(Subscribe {}))
            .await?;
        self.subscription = Some(id);
        Ok(())
    }

    pub fn poll_notification(&mut self) -> Option<Notification> {
        self.notifications.pop_front()
    }

    #[tracing::instrument(name = "client.next_notification", skip_all)]
    pub async fn next_notification(&mut self) -> Result<Notification, Error> {
        if let Some(notification) = self.notifications.pop_front() {
            return Ok(notification);
        }
        let frame = self.next_frame().await?;
        if Some(frame.request_id) != self.subscription {
            return Err(Error::Protocol(format!(
                "unsolicited frame for request {} outside any request",
                frame.request_id
            )));
        }
        match frame.msg {
            Some(server_frame::Msg::Notification(notification)) => Ok(notification),
            Some(other) => Err(unexpected("Notification", &other)),
            None => Err(Error::Protocol("a server frame with no message".to_owned())),
        }
    }

    #[tracing::instrument(name = "client.list_sessions", skip_all)]
    pub async fn list_sessions(&mut self) -> Result<Vec<SessionInfo>, Error> {
        let id = self
            .send(client_frame::Msg::ListSessions(ListSessions {}))
            .await?;
        match self.answer(id).await? {
            server_frame::Msg::SessionList(list) => Ok(list.sessions),
            server_frame::Msg::Error(error) => Err(Error::Server {
                code: error.code,
                msg: error.msg,
            }),
            other => Err(unexpected("SessionList", &other)),
        }
    }

    #[tracing::instrument(name = "client.fetch_history", skip_all, fields(session_id))]
    pub async fn fetch_history(&mut self, session_id: &str) -> Result<SessionHistory, Error> {
        let id = self
            .send(client_frame::Msg::FetchHistory(FetchHistory {
                session_id: session_id.to_owned(),
            }))
            .await?;
        match self.answer(id).await? {
            server_frame::Msg::SessionHistory(history) => Ok(history),
            server_frame::Msg::Error(error) => Err(Error::Server {
                code: error.code,
                msg: error.msg,
            }),
            other => Err(unexpected("SessionHistory", &other)),
        }
    }

    #[tracing::instrument(name = "client.review_items", skip_all, fields(since_micros))]
    pub async fn review_items(
        &mut self,
        since_micros: i64,
    ) -> Result<Vec<MemoryReviewItem>, Error> {
        let id = self
            .send(client_frame::Msg::MemoryReviewList(MemoryReviewList {
                since_micros,
            }))
            .await?;
        match self.answer(id).await? {
            server_frame::Msg::MemoryReviewItems(items) => Ok(items.items),
            server_frame::Msg::Error(error) => Err(Error::Server {
                code: error.code,
                msg: error.msg,
            }),
            other => Err(unexpected("MemoryReviewItems", &other)),
        }
    }

    #[tracing::instrument(name = "client.review_accept", skip_all, fields(record_id))]
    pub async fn review_accept(&mut self, record_id: &str) -> Result<(), Error> {
        let id = self
            .send(client_frame::Msg::MemoryReviewAccept(MemoryReviewAccept {
                record_id: record_id.to_owned(),
            }))
            .await?;
        self.verdict_ack(id).await
    }

    #[tracing::instrument(name = "client.review_delete", skip_all, fields(record_id))]
    pub async fn review_delete(&mut self, record_id: &str) -> Result<(), Error> {
        let id = self
            .send(client_frame::Msg::MemoryReviewDelete(MemoryReviewDelete {
                record_id: record_id.to_owned(),
            }))
            .await?;
        self.verdict_ack(id).await
    }

    #[tracing::instrument(name = "client.jobs", skip_all)]
    pub async fn jobs(&mut self) -> Result<Vec<JobInfo>, Error> {
        let id = self.send(client_frame::Msg::ListJobs(ListJobs {})).await?;
        match self.answer(id).await? {
            server_frame::Msg::JobList(list) => Ok(list.jobs),
            server_frame::Msg::Error(error) => Err(Error::Server {
                code: error.code,
                msg: error.msg,
            }),
            other => Err(unexpected("JobList", &other)),
        }
    }

    #[tracing::instrument(name = "client.projects", skip_all)]
    pub async fn projects(&mut self) -> Result<Vec<ProjectInfo>, Error> {
        let id = self
            .send(client_frame::Msg::ListProjects(ListProjects {}))
            .await?;
        match self.answer(id).await? {
            server_frame::Msg::ProjectList(list) => Ok(list.projects),
            server_frame::Msg::Error(error) => Err(Error::Server {
                code: error.code,
                msg: error.msg,
            }),
            other => Err(unexpected("ProjectList", &other)),
        }
    }

    #[tracing::instrument(name = "client.cancel_job", skip_all, fields(session_id))]
    pub async fn cancel_job(&mut self, session_id: &str) -> Result<(), Error> {
        let id = self
            .send(client_frame::Msg::CancelJob(CancelJob {
                session_id: session_id.to_owned(),
            }))
            .await?;
        self.verdict_ack(id).await
    }

    #[tracing::instrument(name = "client.cancel_turn", skip_all, fields(session_id))]
    pub async fn cancel_turn(&mut self, session_id: &str) -> Result<(), Error> {
        let id = self
            .send(client_frame::Msg::CancelTurn(CancelTurn {
                session_id: session_id.to_owned(),
            }))
            .await?;
        self.verdict_ack(id).await
    }

    #[tracing::instrument(name = "client.drop_steers", skip_all, fields(session_id))]
    pub async fn drop_steers(&mut self, session_id: &str) -> Result<(), Error> {
        let id = self
            .send(client_frame::Msg::DropSteers(DropSteers {
                session_id: session_id.to_owned(),
            }))
            .await?;
        self.verdict_ack(id).await
    }

    #[tracing::instrument(name = "client.create_session", skip_all, fields(project))]
    pub async fn create_session(
        &mut self,
        role: SessionRole,
        project: &str,
    ) -> Result<String, Error> {
        let id = self
            .send(client_frame::Msg::CreateSession(CreateSession {
                role: role as i32,
                project: project.to_owned(),
            }))
            .await?;
        match self.answer(id).await? {
            server_frame::Msg::MessageAccepted(accepted) => Ok(accepted.session_id),
            server_frame::Msg::Error(error) => Err(Error::Server {
                code: error.code,
                msg: error.msg,
            }),
            other => Err(unexpected("MessageAccepted", &other)),
        }
    }

    #[tracing::instrument(name = "client.fork_session", skip_all, fields(session_id))]
    pub async fn fork_session(
        &mut self,
        session_id: &str,
        fork_point: u64,
    ) -> Result<String, Error> {
        let id = self
            .send(client_frame::Msg::ForkSession(ForkSession {
                session_id: session_id.to_owned(),
                fork_point,
            }))
            .await?;
        match self.answer(id).await? {
            server_frame::Msg::MessageAccepted(accepted) => Ok(accepted.session_id),
            server_frame::Msg::Error(error) => Err(Error::Server {
                code: error.code,
                msg: error.msg,
            }),
            other => Err(unexpected("MessageAccepted", &other)),
        }
    }

    #[tracing::instrument(name = "client.mark_branch", skip_all, fields(session_id))]
    pub async fn mark_branch(
        &mut self,
        session_id: &str,
        disposition: branch_marked::Disposition,
    ) -> Result<(), Error> {
        let id = self
            .send(client_frame::Msg::MarkBranch(MarkBranch {
                session_id: session_id.to_owned(),
                disposition: disposition as i32,
            }))
            .await?;
        self.verdict_ack(id).await
    }

    async fn verdict_ack(&mut self, id: u64) -> Result<(), Error> {
        match self.answer(id).await? {
            server_frame::Msg::MessageAccepted(_) => Ok(()),
            server_frame::Msg::Error(error) => Err(Error::Server {
                code: error.code,
                msg: error.msg,
            }),
            other => Err(unexpected("MessageAccepted", &other)),
        }
    }

    #[tracing::instrument(name = "client.send_message", skip_all)]
    pub async fn send_message(
        &mut self,
        session_id: Option<&str>,
        content: &str,
    ) -> Result<Turn<'_>, Error> {
        let request_id = self
            .send(client_frame::Msg::SendMessage(SendMessage {
                session_id: session_id.unwrap_or_default().to_owned(),
                content: content.to_owned(),
            }))
            .await?;
        Ok(Turn {
            client: self,
            request_id,
            done: false,
        })
    }

    async fn send(&mut self, msg: client_frame::Msg) -> Result<u64, Error> {
        self.request_id += 1;
        let frame = ClientFrame {
            request_id: self.request_id,
            msg: Some(msg),
        };
        self.ws
            .send(WsMessage::binary(frame.encode_to_vec()))
            .await
            .map_err(Error::Transport)?;
        Ok(self.request_id)
    }

    async fn answer(&mut self, id: u64) -> Result<server_frame::Msg, Error> {
        loop {
            let frame = self.next_frame().await?;
            let pushed = Some(frame.request_id) == self.subscription
                && matches!(frame.msg, Some(server_frame::Msg::Notification(_)));
            if pushed {
                if let Some(server_frame::Msg::Notification(notification)) = frame.msg {
                    self.notifications.push_back(notification);
                }
                continue;
            }
            if frame.request_id != id {
                return Err(Error::Protocol(format!(
                    "answer for request {} while request {id} was in flight",
                    frame.request_id
                )));
            }
            return frame
                .msg
                .ok_or_else(|| Error::Protocol("a server frame with no message".to_owned()));
        }
    }

    async fn next_frame(&mut self) -> Result<ServerFrame, Error> {
        loop {
            match self.ws.next().await {
                Some(Ok(WsMessage::Binary(bytes))) => {
                    return ServerFrame::decode(bytes).map_err(|error| {
                        Error::Protocol(format!("undecodable server frame: {error}"))
                    });
                }
                Some(Ok(WsMessage::Text(_))) => {
                    return Err(Error::Protocol(
                        "text message on a binary protocol".to_owned(),
                    ));
                }
                Some(Ok(WsMessage::Ping(_) | WsMessage::Pong(_) | WsMessage::Frame(_))) => {}
                Some(Ok(WsMessage::Close(_))) | None => return Err(Error::Closed),
                Some(Err(error)) => {
                    warn!(%error, "websocket read failed");
                    return Err(Error::Transport(error));
                }
            }
        }
    }
}

pub struct Turn<'c> {
    client: &'c mut Client,
    request_id: u64,
    done: bool,
}

impl Turn<'_> {
    pub async fn next(&mut self) -> Result<Option<TurnEvent>, Error> {
        if self.done {
            return Ok(None);
        }
        let event = match self.client.answer(self.request_id).await? {
            server_frame::Msg::MessageAccepted(accepted) => TurnEvent::Accepted {
                session_id: accepted.session_id,
            },
            server_frame::Msg::Delta(delta) => TurnEvent::Delta(delta.text),
            server_frame::Msg::ReasoningDelta(delta) => TurnEvent::Reasoning(delta.text),
            server_frame::Msg::ToolCallStarted(started) => TurnEvent::ToolCallStarted {
                call_id: started.call_id,
                index: started.index,
                name: started.name,
                arguments_json: started.arguments_json,
            },
            server_frame::Msg::ToolCallEnded(ended) => TurnEvent::ToolCallEnded {
                call_id: ended.call_id,
                outcome: ended.outcome,
            },
            server_frame::Msg::StreamEnd(end) => {
                self.done = true;
                TurnEvent::End {
                    input_tokens: end.input_tokens,
                    output_tokens: end.output_tokens,
                    partial: end.partial,
                    step_capped: end.step_capped,
                    grounding_json: end.grounding_json,
                }
            }
            server_frame::Msg::Error(error) => {
                self.done = true;
                TurnEvent::Failed {
                    code: error.code,
                    msg: error.msg,
                }
            }
            other @ (server_frame::Msg::SessionList(_)
            | server_frame::Msg::SessionHistory(_)
            | server_frame::Msg::MemoryReviewItems(_)
            | server_frame::Msg::JobList(_)
            | server_frame::Msg::ProjectList(_)
            | server_frame::Msg::Notification(_)) => {
                return Err(unexpected("a turn frame", &other));
            }
        };
        Ok(Some(event))
    }
}

fn unexpected(wanted: &str, got: &server_frame::Msg) -> Error {
    let got = match got {
        server_frame::Msg::SessionList(_) => "SessionList",
        server_frame::Msg::MessageAccepted(_) => "MessageAccepted",
        server_frame::Msg::Delta(_) => "Delta",
        server_frame::Msg::StreamEnd(_) => "StreamEnd",
        server_frame::Msg::Error(_) => "Error",
        server_frame::Msg::SessionHistory(_) => "SessionHistory",
        server_frame::Msg::ReasoningDelta(_) => "ReasoningDelta",
        server_frame::Msg::ToolCallStarted(_) => "ToolCallStarted",
        server_frame::Msg::ToolCallEnded(_) => "ToolCallEnded",
        server_frame::Msg::MemoryReviewItems(_) => "MemoryReviewItems",
        server_frame::Msg::JobList(_) => "JobList",
        server_frame::Msg::ProjectList(_) => "ProjectList",
        server_frame::Msg::Notification(_) => "Notification",
    };
    Error::Protocol(format!("expected {wanted}, got {got}"))
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use arc_proto::v1::{
        Delta, Error as WireError, MemoryReviewItems, MessageAccepted, Notification,
        ReasoningDelta, SessionAppended, SessionList, StreamEnd, ToolCallEnded, ToolCallStarted,
        ToolOutcome, notification,
    };
    use prost_types::Timestamp;
    use tokio::net::TcpListener;
    use tokio::task::JoinHandle;

    use super::*;

    const PATIENCE: Duration = Duration::from_secs(5);

    struct Answer {
        request_id: Option<u64>,
        msg: server_frame::Msg,
    }

    fn echo(msg: server_frame::Msg) -> Answer {
        Answer {
            request_id: None,
            msg,
        }
    }

    fn fixed(request_id: u64, msg: server_frame::Msg) -> Answer {
        Answer {
            request_id: Some(request_id),
            msg,
        }
    }

    fn accepted(session_id: &str) -> server_frame::Msg {
        server_frame::Msg::MessageAccepted(MessageAccepted {
            session_id: session_id.to_owned(),
        })
    }

    fn delta(session_id: &str, text: &str) -> server_frame::Msg {
        server_frame::Msg::Delta(Delta {
            session_id: session_id.to_owned(),
            text: text.to_owned(),
        })
    }

    fn stream_end(session_id: &str, partial: bool) -> server_frame::Msg {
        server_frame::Msg::StreamEnd(StreamEnd {
            session_id: session_id.to_owned(),
            input_tokens: 3,
            output_tokens: 5,
            partial,
            step_capped: false,
            grounding_json: String::new(),
        })
    }

    fn error(code: &str, msg: &str) -> server_frame::Msg {
        server_frame::Msg::Error(WireError {
            code: code.to_owned(),
            msg: msg.to_owned(),
        })
    }

    async fn server(script: Vec<Vec<Answer>>) -> (String, JoinHandle<Vec<ClientFrame>>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let url = format!("ws://{}", listener.local_addr().expect("local addr"));

        let handle = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept");
            let mut ws = tokio_tungstenite::accept_async(stream)
                .await
                .expect("handshake");

            let mut received = Vec::new();
            for answers in script {
                let frame = loop {
                    match ws.next().await.expect("a client frame") {
                        Ok(WsMessage::Binary(bytes)) => {
                            break ClientFrame::decode(bytes).expect("decode");
                        }
                        Ok(WsMessage::Ping(_) | WsMessage::Pong(_)) => {}
                        other => panic!("expected a binary frame, got {other:?}"),
                    }
                };
                for answer in answers {
                    let out = ServerFrame {
                        request_id: answer.request_id.unwrap_or(frame.request_id),
                        msg: Some(answer.msg),
                    };
                    ws.send(WsMessage::binary(out.encode_to_vec()))
                        .await
                        .expect("send");
                }
                received.push(frame);
            }
            let _ = ws.close(None).await;
            received
        });

        (url, handle)
    }

    async fn received(handle: JoinHandle<Vec<ClientFrame>>) -> Vec<ClientFrame> {
        tokio::time::timeout(PATIENCE, handle)
            .await
            .expect("server finishes within PATIENCE")
            .expect("server task")
    }

    #[tokio::test]
    async fn list_sessions_round_trips() {
        let session = SessionInfo {
            preview: "hello arc".to_owned(),
            last_at: None,
            id: "s-1".to_owned(),
            title: String::new(),
            started_at: Some(Timestamp {
                seconds: 1,
                nanos: 0,
            }),
            role: 0,
            project: String::new(),
            dispatched_by: String::new(),
            source: 0,
            parent_session: String::new(),
            disposition: 0,
        };
        let list = server_frame::Msg::SessionList(SessionList {
            sessions: vec![session.clone()],
        });
        let (url, handle) = server(vec![vec![echo(list)]]).await;

        let mut client = Client::connect(&url).await.expect("connect");
        let sessions = client.list_sessions().await.expect("list");

        assert_eq!(sessions, [session]);
        let frames = received(handle).await;
        assert_eq!(frames.len(), 1);
        assert_ne!(frames[0].request_id, 0, "request ids are nonzero");
        assert!(matches!(
            frames[0].msg,
            Some(client_frame::Msg::ListSessions(_))
        ));
    }

    #[tokio::test]
    async fn review_items_round_trips() {
        let item = MemoryReviewItem {
            record: Some(arc_proto::v1::MemoryRecord {
                id: "m-01".to_owned(),
                ..Default::default()
            }),
            changed_at_micros: 1_700_000_000_000_000,
            superseded_by: String::new(),
            supersedes: Vec::new(),
        };
        let items = server_frame::Msg::MemoryReviewItems(MemoryReviewItems {
            items: vec![item.clone()],
        });
        let (url, handle) = server(vec![vec![echo(items)]]).await;

        let mut client = Client::connect(&url).await.expect("connect");
        let listed = client.review_items(123).await.expect("review_items");

        assert_eq!(listed, [item]);
        let frames = received(handle).await;
        match &frames[0].msg {
            Some(client_frame::Msg::MemoryReviewList(list)) => {
                assert_eq!(list.since_micros, 123);
            }
            other => panic!("expected MemoryReviewList, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn jobs_round_trips() {
        let job = arc_proto::v1::JobInfo {
            session_id: "s-job".to_owned(),
            role: arc_proto::v1::SessionRole::Executor as i32,
            project: "arc".to_owned(),
            state: arc_proto::v1::job_info::State::Running as i32,
            spent_tokens: 12,
            budget_tokens: 100,
            elapsed_seconds: 4,
            budget_seconds: 60,
            title: String::new(),
            tool_steps: 3,
            idle_seconds: 1,
            parent_session: "s-parent".to_owned(),
            queued_steers: 2,
            last_call: "bash cargo test".to_owned(),
        };
        let list = server_frame::Msg::JobList(arc_proto::v1::JobList {
            jobs: vec![job.clone()],
        });
        let (url, handle) = server(vec![vec![echo(list)]]).await;

        let mut client = Client::connect(&url).await.expect("connect");
        let jobs = client.jobs().await.expect("jobs");

        assert_eq!(jobs, [job]);
        let frames = received(handle).await;
        assert!(matches!(
            frames[0].msg,
            Some(client_frame::Msg::ListJobs(_))
        ));
    }

    #[tokio::test]
    async fn create_session_round_trips_the_new_sessions_id() {
        let (url, handle) = server(vec![vec![echo(accepted("s-code"))]]).await;

        let mut client = Client::connect(&url).await.expect("connect");
        let session_id = client
            .create_session(SessionRole::Executor, "arc")
            .await
            .expect("create_session");

        assert_eq!(session_id, "s-code");
        let frames = received(handle).await;
        match &frames[0].msg {
            Some(client_frame::Msg::CreateSession(create)) => {
                assert_eq!(create.role, SessionRole::Executor as i32);
                assert_eq!(create.project, "arc");
            }
            other => panic!("expected CreateSession, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn create_session_surfaces_the_daemons_refusal() {
        let (url, handle) = server(vec![vec![echo(error("unsupported_role", "no"))]]).await;

        let mut client = Client::connect(&url).await.expect("connect");
        let err = client
            .create_session(SessionRole::Concierge, "arc")
            .await
            .expect_err("a non-executor role is refused");

        assert!(matches!(err, Error::Server { code, .. } if code == "unsupported_role"));
        received(handle).await;
    }

    #[tokio::test]
    async fn fork_session_round_trips_the_new_sessions_id() {
        let (url, handle) = server(vec![vec![echo(accepted("s-fork"))]]).await;

        let mut client = Client::connect(&url).await.expect("connect");
        let session_id = client
            .fork_session("s-parent", 7)
            .await
            .expect("fork_session");

        assert_eq!(session_id, "s-fork");
        let frames = received(handle).await;
        match &frames[0].msg {
            Some(client_frame::Msg::ForkSession(fork)) => {
                assert_eq!(fork.session_id, "s-parent");
                assert_eq!(fork.fork_point, 7);
            }
            other => panic!("expected ForkSession, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn fork_session_surfaces_the_daemons_refusal() {
        let (url, handle) =
            server(vec![vec![echo(error("unknown_session", "no such parent"))]]).await;

        let mut client = Client::connect(&url).await.expect("connect");
        let err = client
            .fork_session("ghost", 1)
            .await
            .expect_err("an unknown parent is refused");

        assert!(matches!(err, Error::Server { code, .. } if code == "unknown_session"));
        received(handle).await;
    }

    #[tokio::test]
    async fn mark_branch_acks_and_a_refusal_is_a_server_error() {
        let (url, handle) = server(vec![
            vec![echo(accepted(""))],
            vec![echo(error(
                "not_a_branch",
                "session s-root is a root conversation",
            ))],
        ])
        .await;

        let mut client = Client::connect(&url).await.expect("connect");
        client
            .mark_branch("s-fork", branch_marked::Disposition::Real)
            .await
            .expect("mark");
        match client
            .mark_branch("s-root", branch_marked::Disposition::Abandoned)
            .await
        {
            Err(Error::Server { code, .. }) => assert_eq!(code, "not_a_branch"),
            other => panic!("expected Error::Server, got {other:?}"),
        }

        let sent: Vec<_> = received(handle)
            .await
            .into_iter()
            .map(|frame| frame.msg.expect("a message"))
            .collect();
        assert!(
            matches!(
                &sent[0],
                client_frame::Msg::MarkBranch(m)
                    if m.session_id == "s-fork"
                        && m.disposition == branch_marked::Disposition::Real as i32
            ),
            "got: {:?}",
            sent[0]
        );
        assert!(
            matches!(
                &sent[1],
                client_frame::Msg::MarkBranch(m)
                    if m.session_id == "s-root"
                        && m.disposition == branch_marked::Disposition::Abandoned as i32
            ),
            "got: {:?}",
            sent[1]
        );
    }

    #[tokio::test]
    async fn cancel_turn_acks_and_a_refusal_is_a_server_error() {
        let (url, handle) = server(vec![
            vec![echo(accepted("s-live"))],
            vec![echo(error(
                "no_turn",
                "no turn is running on session s-idle",
            ))],
        ])
        .await;

        let mut client = Client::connect(&url).await.expect("connect");
        client.cancel_turn("s-live").await.expect("cancel");
        match client.cancel_turn("s-idle").await {
            Err(Error::Server { code, .. }) => assert_eq!(code, "no_turn"),
            other => panic!("expected Error::Server, got {other:?}"),
        }

        let sent: Vec<_> = received(handle)
            .await
            .into_iter()
            .map(|frame| frame.msg.expect("a message"))
            .collect();
        assert!(
            matches!(&sent[0], client_frame::Msg::CancelTurn(c) if c.session_id == "s-live"),
            "got: {:?}",
            sent[0]
        );
        assert!(
            matches!(&sent[1], client_frame::Msg::CancelTurn(c) if c.session_id == "s-idle"),
            "got: {:?}",
            sent[1]
        );
    }

    #[tokio::test]
    async fn a_review_verdict_acks_and_a_refusal_is_a_server_error() {
        let ack = || accepted("");
        let (url, handle) = server(vec![
            vec![echo(ack())],
            vec![echo(ack())],
            vec![echo(error("unknown_record", "no memory record m-9"))],
        ])
        .await;

        let mut client = Client::connect(&url).await.expect("connect");
        client.review_accept("m-1").await.expect("accept");
        client.review_delete("m-2").await.expect("delete");
        match client.review_accept("m-9").await {
            Err(Error::Server { code, .. }) => assert_eq!(code, "unknown_record"),
            other => panic!("expected Error::Server, got {other:?}"),
        }

        let sent: Vec<_> = received(handle)
            .await
            .into_iter()
            .map(|frame| frame.msg.expect("a message"))
            .collect();
        assert!(
            matches!(&sent[0], client_frame::Msg::MemoryReviewAccept(a) if a.record_id == "m-1"),
            "got: {:?}",
            sent[0]
        );
        assert!(
            matches!(&sent[1], client_frame::Msg::MemoryReviewDelete(d) if d.record_id == "m-2"),
            "got: {:?}",
            sent[1]
        );
    }

    #[tokio::test]
    async fn a_turn_yields_accepted_deltas_and_end() {
        let (url, handle) = server(vec![vec![
            echo(accepted("s-1")),
            echo(delta("s-1", "hel")),
            echo(delta("s-1", "lo")),
            echo(stream_end("s-1", false)),
        ]])
        .await;

        let mut client = Client::connect(&url).await.expect("connect");
        let mut turn = client.send_message(None, "hi").await.expect("send");

        assert_eq!(
            turn.next().await.expect("accepted"),
            Some(TurnEvent::Accepted {
                session_id: "s-1".to_owned()
            })
        );
        assert_eq!(
            turn.next().await.expect("delta"),
            Some(TurnEvent::Delta("hel".to_owned()))
        );
        assert_eq!(
            turn.next().await.expect("delta"),
            Some(TurnEvent::Delta("lo".to_owned()))
        );
        assert_eq!(
            turn.next().await.expect("end"),
            Some(TurnEvent::End {
                input_tokens: 3,
                output_tokens: 5,
                partial: false,
                step_capped: false,
                grounding_json: String::new(),
            })
        );
        assert_eq!(turn.next().await.expect("closed"), None, "the turn is over");

        let frames = received(handle).await;
        match &frames[0].msg {
            Some(client_frame::Msg::SendMessage(send)) => {
                assert_eq!(send.session_id, "");
                assert_eq!(send.content, "hi");
            }
            other => panic!("expected SendMessage, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn tool_activity_and_reasoning_frames_are_yielded_in_order() {
        let (url, _handle) = server(vec![vec![
            echo(accepted("s-1")),
            echo(server_frame::Msg::ReasoningDelta(ReasoningDelta {
                session_id: "s-1".to_owned(),
                text: "let me look".to_owned(),
            })),
            echo(server_frame::Msg::ToolCallStarted(ToolCallStarted {
                session_id: "s-1".to_owned(),
                call_id: "call-aa".to_owned(),
                index: 0,
                name: "memory_search".to_owned(),
                arguments_json: r#"{"query":"budget"}"#.to_owned(),
            })),
            echo(server_frame::Msg::ToolCallEnded(ToolCallEnded {
                session_id: "s-1".to_owned(),
                call_id: "call-aa".to_owned(),
                outcome: ToolOutcome::Ok as i32,
            })),
            echo(delta("s-1", "hello")),
            echo(stream_end("s-1", false)),
        ]])
        .await;

        let mut client = Client::connect(&url).await.expect("connect");
        let mut turn = client.send_message(None, "hi").await.expect("send");

        let mut events = Vec::new();
        while let Some(event) = turn.next().await.expect("a turn event") {
            events.push(event);
        }

        assert_eq!(
            events,
            [
                TurnEvent::Accepted {
                    session_id: "s-1".to_owned()
                },
                TurnEvent::Reasoning("let me look".to_owned()),
                TurnEvent::ToolCallStarted {
                    call_id: "call-aa".to_owned(),
                    index: 0,
                    name: "memory_search".to_owned(),
                    arguments_json: r#"{"query":"budget"}"#.to_owned(),
                },
                TurnEvent::ToolCallEnded {
                    call_id: "call-aa".to_owned(),
                    outcome: ToolOutcome::Ok as i32,
                },
                TurnEvent::Delta("hello".to_owned()),
                TurnEvent::End {
                    input_tokens: 3,
                    output_tokens: 5,
                    partial: false,
                    step_capped: false,
                    grounding_json: String::new(),
                },
            ]
        );
    }

    #[tokio::test]
    async fn a_server_error_ends_the_turn_as_failed() {
        let (url, _handle) =
            server(vec![vec![echo(error("empty_message", "say something"))]]).await;

        let mut client = Client::connect(&url).await.expect("connect");
        let mut turn = client.send_message(Some("s-1"), " ").await.expect("send");

        assert_eq!(
            turn.next().await.expect("failed"),
            Some(TurnEvent::Failed {
                code: "empty_message".to_owned(),
                msg: "say something".to_owned()
            })
        );
        assert_eq!(turn.next().await.expect("closed"), None);
    }

    #[tokio::test]
    async fn a_list_error_frame_is_a_server_error() {
        let (url, _handle) = server(vec![vec![echo(error("internal", "index broke"))]]).await;

        let mut client = Client::connect(&url).await.expect("connect");
        match client.list_sessions().await {
            Err(Error::Server { code, msg }) => {
                assert_eq!(code, "internal");
                assert_eq!(msg, "index broke");
            }
            other => panic!("expected Error::Server, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn a_mismatched_request_id_is_a_protocol_error() {
        let (url, _handle) = server(vec![vec![fixed(999, accepted("s-1"))]]).await;

        let mut client = Client::connect(&url).await.expect("connect");
        let mut turn = client.send_message(None, "hi").await.expect("send");

        assert!(matches!(turn.next().await, Err(Error::Protocol(_))));
    }

    #[tokio::test]
    async fn a_close_mid_turn_is_closed() {
        let (url, _handle) = server(vec![vec![echo(accepted("s-1"))]]).await;

        let mut client = Client::connect(&url).await.expect("connect");
        let mut turn = client.send_message(None, "hi").await.expect("send");

        assert!(matches!(
            turn.next().await,
            Ok(Some(TurnEvent::Accepted { .. }))
        ));
        assert!(matches!(turn.next().await, Err(Error::Closed)));
    }

    #[tokio::test]
    async fn a_notification_arriving_while_answer_awaits_a_different_reply_is_queued() {
        let push = server_frame::Msg::Notification(Notification {
            event: Some(notification::Event::SessionAppended(SessionAppended {
                session_id: "s-1".to_owned(),
            })),
        });
        let list = server_frame::Msg::SessionList(SessionList { sessions: vec![] });
        let (url, _handle) = server(vec![vec![], vec![fixed(1, push), echo(list)]]).await;

        let mut client = Client::connect(&url).await.expect("connect");
        client.subscribe().await.expect("subscribe");
        assert_eq!(client.poll_notification(), None, "nothing queued yet");

        let sessions = client.list_sessions().await.expect("list_sessions");

        assert_eq!(
            sessions,
            [],
            "the queued push never masqueraded as the answer"
        );
        match client.poll_notification() {
            Some(Notification {
                event: Some(notification::Event::SessionAppended(appended)),
            }) => assert_eq!(appended.session_id, "s-1"),
            other => panic!("expected the queued SessionAppended, got {other:?}"),
        }
        assert_eq!(client.poll_notification(), None, "the queue drains once");
    }

    #[tokio::test]
    async fn next_notification_awaits_a_push_with_no_request_in_flight() {
        let push = server_frame::Msg::Notification(Notification {
            event: Some(notification::Event::SessionAppended(SessionAppended {
                session_id: "s-1".to_owned(),
            })),
        });
        let (url, _handle) = server(vec![vec![echo(push)]]).await;

        let mut client = Client::connect(&url).await.expect("connect");
        client.subscribe().await.expect("subscribe");

        match client.next_notification().await {
            Ok(Notification {
                event: Some(notification::Event::SessionAppended(appended)),
            }) => assert_eq!(appended.session_id, "s-1"),
            other => panic!("expected the pushed SessionAppended, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn next_notification_drains_a_push_queued_during_a_prior_answer() {
        let push = server_frame::Msg::Notification(Notification {
            event: Some(notification::Event::SessionAppended(SessionAppended {
                session_id: "s-1".to_owned(),
            })),
        });
        let list = server_frame::Msg::SessionList(SessionList { sessions: vec![] });
        let (url, _handle) = server(vec![vec![], vec![fixed(1, push), echo(list)]]).await;

        let mut client = Client::connect(&url).await.expect("connect");
        client.subscribe().await.expect("subscribe");
        client.list_sessions().await.expect("list_sessions");

        match client.next_notification().await {
            Ok(Notification {
                event: Some(notification::Event::SessionAppended(appended)),
            }) => assert_eq!(appended.session_id, "s-1"),
            other => panic!("expected the queued SessionAppended, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn a_frame_outside_any_request_that_is_not_a_notification_is_a_protocol_error() {
        let (url, _handle) = server(vec![vec![echo(accepted("s-1"))]]).await;

        let mut client = Client::connect(&url).await.expect("connect");
        client.subscribe().await.expect("subscribe");

        assert!(matches!(
            client.next_notification().await,
            Err(Error::Protocol(_))
        ));
    }

    #[tokio::test]
    async fn request_ids_increment_across_requests() {
        let list = || server_frame::Msg::SessionList(SessionList { sessions: vec![] });
        let (url, handle) = server(vec![vec![echo(list())], vec![echo(list())]]).await;

        let mut client = Client::connect(&url).await.expect("connect");
        client.list_sessions().await.expect("first");
        client.list_sessions().await.expect("second");

        let ids: Vec<u64> = received(handle)
            .await
            .iter()
            .map(|f| f.request_id)
            .collect();
        assert_eq!(ids, [1, 2]);
    }
}
