//! The client side of `wire.proto` (DESIGN.md §7).
//!
//! One request in flight at a time: the daemon answers in order, and a
//! [`Turn`] borrows the client mutably, so correlation is a check, not a
//! demultiplexer — every answer frame must echo the id of the one request
//! outstanding. Anything else is a protocol violation and the connection
//! is not worth trusting past it.
//!
//! Clients hold no durable state (DESIGN.md §7): everything here is a view
//! over the wire, safe to drop and reconnect.

use arc_proto::v1::{
    ClientFrame, ListSessions, SendMessage, ServerFrame, SessionInfo, client_frame, server_frame,
};
use futures::{SinkExt as _, StreamExt as _};
use prost::Message as _;
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::Message as WsMessage;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, tungstenite};
use tracing::warn;

/// A client-side failure.
///
/// [`Error::Server`] is the daemon saying no to one request — the connection
/// survives it. Every other variant means the connection is over or cannot be
/// trusted; drop the client and reconnect.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Connecting to the daemon failed.
    #[error("connecting to the daemon failed: {0}")]
    Connect(#[source] tungstenite::Error),
    /// The connection failed mid-use.
    #[error("the connection failed: {0}")]
    Transport(#[source] tungstenite::Error),
    /// The daemon closed the connection.
    #[error("the daemon closed the connection")]
    Closed,
    /// The daemon answered a request with an error frame.
    #[error("the daemon refused the request ({code}): {msg}")]
    Server {
        /// The stable `snake_case` code — the part to match on.
        code: String,
        /// The human-readable explanation beside it.
        msg: String,
    },
    /// The daemon sent something the protocol has no place for.
    #[error("protocol violation: {0}")]
    Protocol(String),
}

/// One event of a completion turn, in the order the wire defines: one
/// `Accepted`, zero or more `Delta`s, then exactly one `End` or `Failed`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TurnEvent {
    /// The daemon accepted the message and named the session.
    Accepted {
        /// The session's id — for a new session, the first time anyone knows it.
        session_id: String,
    },
    /// A piece of the model's reply.
    Delta(String),
    /// The turn finished; the reply is durable.
    End {
        /// Prompt tokens billed; 0 if the provider reported no usage.
        input_tokens: u32,
        /// Reply tokens billed; 0 if the provider reported no usage.
        output_tokens: u32,
        /// The reply was cut before the model finished; the deltas are what
        /// got logged.
        partial: bool,
    },
    /// The turn failed. The connection survives; send the next request.
    Failed {
        /// The stable `snake_case` code — the part to match on.
        code: String,
        /// The human-readable explanation beside it.
        msg: String,
    },
}

/// A connection to the daemon, speaking `wire.proto`.
pub struct Client {
    ws: WebSocketStream<MaybeTlsStream<TcpStream>>,
    /// The id of the last request sent; incremented before each send, so the
    /// first request is 1 and 0 (the unsolicited marker) is never used.
    request_id: u64,
}

impl Client {
    /// Connects to a daemon at `url` (e.g. `ws://127.0.0.1:8787`).
    ///
    /// # Errors
    ///
    /// [`Error::Connect`] if the daemon cannot be reached or the handshake
    /// fails.
    #[tracing::instrument(name = "client.connect", skip_all, fields(url))]
    pub async fn connect(url: &str) -> Result<Self, Error> {
        let (ws, _) = tokio_tungstenite::connect_async(url)
            .await
            .map_err(Error::Connect)?;
        Ok(Self { ws, request_id: 0 })
    }

    /// Asks the daemon for its sessions, oldest first.
    ///
    /// # Errors
    ///
    /// [`Error::Server`] if the daemon refused the request; any other variant
    /// means the connection is unusable.
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

    /// Sends a user message and returns the turn to read its events from.
    ///
    /// `session_id: None` starts a new session; the daemon names it in the
    /// turn's [`TurnEvent::Accepted`]. The turn borrows the client — read it
    /// to its end before the next request.
    ///
    /// # Errors
    ///
    /// [`Error::Transport`] if the message could not be written; a refused
    /// request surfaces later, as the turn's [`TurnEvent::Failed`].
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

    /// Sends one request frame and returns the id to correlate its answers on.
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

    /// The next answer frame for request `id`.
    ///
    /// A frame with any other id — including 0, the daemon's "no id could be
    /// recovered" — has no legitimate cause while `id` is the one request in
    /// flight, so it is a protocol error, not something to wait past.
    async fn answer(&mut self, id: u64) -> Result<server_frame::Msg, Error> {
        let frame = self.next_frame().await?;
        if frame.request_id != id {
            return Err(Error::Protocol(format!(
                "answer for request {} while request {id} was in flight",
                frame.request_id
            )));
        }
        frame
            .msg
            .ok_or_else(|| Error::Protocol("a server frame with no message".to_owned()))
    }

    /// The next decoded server frame, skipping the transport's own chatter.
    async fn next_frame(&mut self) -> Result<ServerFrame, Error> {
        loop {
            match self.ws.next().await {
                Some(Ok(WsMessage::Binary(bytes))) => {
                    return ServerFrame::decode(bytes).map_err(|error| {
                        Error::Protocol(format!("undecodable server frame: {error}"))
                    });
                }
                // Text is not this protocol — the server never sends it.
                Some(Ok(WsMessage::Text(_))) => {
                    return Err(Error::Protocol(
                        "text message on a binary protocol".to_owned(),
                    ));
                }
                // Pings and pongs are tungstenite's business, not ours.
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

/// One completion turn being read off the wire.
///
/// Yields events until the closing frame ([`TurnEvent::End`] or
/// [`TurnEvent::Failed`]), then `None`.
pub struct Turn<'c> {
    client: &'c mut Client,
    request_id: u64,
    done: bool,
}

impl Turn<'_> {
    /// The next event of the turn, or `None` once it has closed.
    ///
    /// # Errors
    ///
    /// Any error means the connection is unusable mid-turn; what was already
    /// streamed is all the client will see of the reply.
    pub async fn next(&mut self) -> Result<Option<TurnEvent>, Error> {
        if self.done {
            return Ok(None);
        }
        let event = match self.client.answer(self.request_id).await? {
            server_frame::Msg::MessageAccepted(accepted) => TurnEvent::Accepted {
                session_id: accepted.session_id,
            },
            server_frame::Msg::Delta(delta) => TurnEvent::Delta(delta.text),
            server_frame::Msg::StreamEnd(end) => {
                self.done = true;
                TurnEvent::End {
                    input_tokens: end.input_tokens,
                    output_tokens: end.output_tokens,
                    partial: end.partial,
                }
            }
            server_frame::Msg::Error(error) => {
                self.done = true;
                TurnEvent::Failed {
                    code: error.code,
                    msg: error.msg,
                }
            }
            other @ server_frame::Msg::SessionList(_) => {
                return Err(unexpected("a turn frame", &other));
            }
        };
        Ok(Some(event))
    }
}

/// A protocol error for a frame that answered the wrong question.
fn unexpected(wanted: &str, got: &server_frame::Msg) -> Error {
    let got = match got {
        server_frame::Msg::SessionList(_) => "SessionList",
        server_frame::Msg::MessageAccepted(_) => "MessageAccepted",
        server_frame::Msg::Delta(_) => "Delta",
        server_frame::Msg::StreamEnd(_) => "StreamEnd",
        server_frame::Msg::Error(_) => "Error",
    };
    Error::Protocol(format!("expected {wanted}, got {got}"))
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use arc_proto::v1::{Delta, Error as WireError, MessageAccepted, SessionList, StreamEnd};
    use prost_types::Timestamp;
    use tokio::net::TcpListener;
    use tokio::task::JoinHandle;

    use super::*;

    /// Longest any assertion waits. Generous enough never to fire on a loaded
    /// machine, short enough that a hang fails instead of hanging.
    const PATIENCE: Duration = Duration::from_secs(5);

    /// One scripted answer: `request_id: None` echoes the client's.
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
        })
    }

    fn error(code: &str, msg: &str) -> server_frame::Msg {
        server_frame::Msg::Error(WireError {
            code: code.to_owned(),
            msg: msg.to_owned(),
        })
    }

    /// A server that answers each client frame with the next script entry,
    /// then closes. Returns the URL to connect to and the handle that yields
    /// every frame the client sent.
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
            id: "s-1".to_owned(),
            title: String::new(),
            started_at: Some(Timestamp {
                seconds: 1,
                nanos: 0,
            }),
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
                partial: false
            })
        );
        assert_eq!(turn.next().await.expect("closed"), None, "the turn is over");

        // An empty session id on the wire is how "start a session" is said.
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
        // The script is exhausted: the server closes instead of finishing the
        // turn.
        assert!(matches!(turn.next().await, Err(Error::Closed)));
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
