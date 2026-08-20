//! The `WebSocket` server: `wire.proto` over localhost (DESIGN.md §7).
//!
//! One `ClientFrame` per binary message in, one or more `ServerFrame`s out,
//! `request_id` echoed on every one so a client can correlate. This module
//! decides nothing about sessions, models, or durability — it translates
//! between frames and [`Engine`] calls, which is exactly the split the engine's
//! [`EngineEvent`] was shaped for.
//!
//! # Concurrency
//!
//! One task per connection, and frames within a connection are handled one at
//! a time: the read loop does not look at the next message until the current
//! request has sent its last frame. A client that pipelines two requests gets
//! them answered in order, which is what the read loop's shape already gives.
//!
//! Across connections, the engine's mutex is the serializer. `send_message`
//! takes `&mut Engine`, so a completion holds the lock for its whole duration
//! and every other request — a list on another connection included — waits.
//! That is Phase 1's single-user reality (DESIGN.md §1), not a bug to route
//! around; when it stops being true, the fix is more than one engine, not a
//! finer lock.
//!
//! # Shutdown
//!
//! [`serve`] stops accepting, tells live connections to close, and gives
//! whatever is still in flight a bounded grace before the process leaves.

use std::future::Future;
use std::net::SocketAddr;
use std::ops::ControlFlow;
use std::sync::Arc;
use std::time::Duration;

use arc_core::projection::SessionSummary;
use arc_core::provider::Provider;
use arc_core::session::{Engine, EngineEvent, Error as SessionError, Reply};
use arc_proto::v1::{
    ClientFrame, Delta, Error as WireError, HistoryMessage, MessageAccepted, SendMessage,
    ServerFrame, SessionHistory, SessionInfo, SessionList, StreamEnd, client_frame, server_frame,
};
use futures::{SinkExt as _, StreamExt as _};
use prost::Message as _;
use prost_types::Timestamp;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Mutex, mpsc, watch};
use tokio::task::JoinSet;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::Message as WsMessage;
use tracing::{info, warn};

/// A connected client, server side.
type Socket = WebSocketStream<TcpStream>;

/// Engine events buffered between the completion and the socket.
///
/// Deep enough that a client reading at any reasonable pace never makes the
/// model wait, shallow enough that a stalled client stalls its own completion
/// instead of buffering a whole reply in memory.
const EVENT_BUFFER: usize = 64;

/// How long [`serve`] waits for in-flight requests after it stops accepting.
const SHUTDOWN_GRACE: Duration = Duration::from_secs(10);

/// Serves `wire.proto` on `listener` until `shutdown` resolves.
///
/// Every connection gets its own task; all of them share `engine`. Returns
/// once the listener is closed and the live connections have finished or the
/// grace period has run out.
pub async fn serve<P: Provider + 'static>(
    listener: TcpListener,
    engine: Arc<Mutex<Engine<P>>>,
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
                    // Reap first: a daemon that runs for months must not
                    // accumulate the join handles of every client it ever had.
                    while connections.try_join_next().is_some() {}
                    connections.spawn(connection(
                        stream,
                        peer,
                        Arc::clone(&engine),
                        closing_rx.clone(),
                    ));
                }
                // One refused connection is not a reason to stop serving the
                // others: log it and keep accepting.
                Err(error) => warn!(%error, "accepting a connection failed"),
            },
        }
    }

    info!(connections = connections.len(), "no longer accepting");
    // Idle connections take this as their cue. A connection with a completion
    // in flight sees it when it comes back around to its read loop, which is
    // what makes the grace below a grace and not a wait.
    let _ = closing.send(true);
    drain(&mut connections).await;
}

/// Waits out the live connections, bounded by [`SHUTDOWN_GRACE`].
async fn drain(connections: &mut JoinSet<()>) {
    let expired = tokio::time::timeout(SHUTDOWN_GRACE, async {
        while connections.join_next().await.is_some() {}
    })
    .await
    .is_err();

    if expired {
        // Whatever is left is dropped with the set: a completion mid-stream is
        // abandoned, losing at most the reply text the model had not finished.
        // That is the shape of loss a crash produces and the log already
        // tolerates it (DESIGN.md §3, §4) — worth more than an unbounded
        // shutdown.
        warn!(
            remaining = connections.len(),
            "shutdown grace expired; abandoning connections"
        );
    }
}

/// One client, from handshake to close.
#[tracing::instrument(name = "server.connection", skip_all, fields(peer = %peer))]
async fn connection<P: Provider>(
    stream: TcpStream,
    peer: SocketAddr,
    engine: Arc<Mutex<Engine<P>>>,
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

    loop {
        // Cancelling the read at shutdown is safe here in the only way that
        // matters: the connection is being closed anyway, so a half-read
        // message has nothing left to be read for.
        let message = tokio::select! {
            () = told_to_close(&mut closing) => {
                info!("closing an idle connection");
                let _ = ws.close(None).await;
                break;
            }
            message = ws.next() => message,
        };

        match message {
            Some(Ok(WsMessage::Binary(bytes))) => match ClientFrame::decode(bytes) {
                Ok(frame) => {
                    if request(&mut ws, &engine, frame).await.is_break() {
                        break;
                    }
                }
                // Past a decode failure the byte stream cannot be trusted —
                // the same reasoning that fuses the SSE parser. Say so and go.
                Err(error) => {
                    warn!(%error, "undecodable client frame");
                    refuse(&mut ws, 0).await;
                    break;
                }
            },
            // Text is not this protocol. Treating it as a bad frame beats
            // ignoring it: a client that sent the wrong message type hears
            // about it instead of waiting forever for a reply.
            Some(Ok(WsMessage::Text(_))) => {
                warn!("text message on a binary protocol");
                refuse(&mut ws, 0).await;
                break;
            }
            // Pings and pongs are tungstenite's business, not ours.
            Some(Ok(WsMessage::Ping(_) | WsMessage::Pong(_) | WsMessage::Frame(_))) => {}
            Some(Ok(WsMessage::Close(_))) | None => break,
            Some(Err(error)) => {
                warn!(%error, "websocket read failed");
                break;
            }
        }
    }

    info!("client disconnected");
}

/// Resolves once [`serve`] has stopped accepting.
///
/// A thin wrapper for one reason: `wait_for` hands back a borrow guard that is
/// not `Send`, and letting it into a `select!` arm would make the whole
/// connection task unspawnable. Dropping it here keeps that detail local.
async fn told_to_close(closing: &mut watch::Receiver<bool>) {
    let _ = closing.wait_for(|closing| *closing).await;
}

/// Handles one client frame, start to finish.
///
/// `Break` ends the connection: either the client is gone or it sent something
/// this protocol has no answer for.
#[tracing::instrument(
    name = "server.request",
    skip_all,
    fields(request_id = frame.request_id, kind = kind(&frame)),
)]
async fn request<P: Provider>(
    ws: &mut Socket,
    engine: &Mutex<Engine<P>>,
    frame: ClientFrame,
) -> ControlFlow<()> {
    match frame.msg {
        Some(client_frame::Msg::SendMessage(send)) => {
            send_message(ws, engine, frame.request_id, send).await
        }
        Some(client_frame::Msg::ListSessions(_)) => {
            list_sessions(ws, engine, frame.request_id).await
        }
        Some(client_frame::Msg::FetchHistory(fetch)) => {
            fetch_history(ws, engine, frame.request_id, &fetch.session_id).await
        }
        // A frame from a newer client, or one that asked for nothing. Either
        // way this binary cannot answer it, and guessing would be worse.
        None => {
            warn!("client frame with no request");
            refuse(ws, frame.request_id).await;
            ControlFlow::Break(())
        }
    }
}

/// Drives one completion, streaming its events out as they happen.
///
/// The engine is locked for the whole turn — see the module docs — and
/// released before the closing frame goes out, so the next request waits on
/// the model rather than on a socket write.
async fn send_message<P: Provider>(
    ws: &mut Socket,
    engine: &Mutex<Engine<P>>,
    request_id: u64,
    send: SendMessage,
) -> ControlFlow<()> {
    let (events, rx) = mpsc::channel(EVENT_BUFFER);
    // An empty session id means "start one" (DESIGN.md §7); the engine names it
    // and says so in `Accepted`.
    let session_id = (!send.session_id.is_empty()).then_some(send.session_id.as_str());

    let mut engine = engine.lock().await;
    // `join!`, not `select!`: both halves have to finish. The completion is
    // what makes the message durable, and the forward loop is the only thing
    // draining the events it emits.
    let (result, connected) = tokio::join!(
        engine.send_message(session_id, &send.content, events),
        forward(ws, request_id, send.session_id.clone(), rx),
    );
    drop(engine);

    if !connected {
        return ControlFlow::Break(());
    }

    let msg = match result {
        Ok(reply) => stream_end(&reply),
        Err(error) => {
            warn!(%error, code = error_code(&error), "request failed");
            error_frame(error_code(&error), &error)
        }
    };
    // A failed request is the client's problem, not the connection's: the next
    // frame is read as usual.
    flow(send_frame(ws, request_id, msg).await)
}

/// Forwards engine events to the client until the completion ends.
///
/// Returns whether the client is still there. `rx` is dropped on the way out,
/// so a completion whose client vanished finds a closed channel and runs to
/// its end unwatched instead of filling the buffer and stalling.
async fn forward(
    ws: &mut Socket,
    request_id: u64,
    mut session_id: String,
    mut rx: mpsc::Receiver<EngineEvent>,
) -> bool {
    while let Some(event) = rx.recv().await {
        let msg = match event {
            EngineEvent::Accepted { session_id: id } => {
                // For a new session this is the first time anyone knows the
                // id, and every `Delta` below has to carry it.
                session_id.clone_from(&id);
                server_frame::Msg::MessageAccepted(MessageAccepted { session_id: id })
            }
            EngineEvent::Delta(text) => server_frame::Msg::Delta(Delta {
                session_id: session_id.clone(),
                text,
            }),
            // 4.4 translates these to their wire frames; until then the
            // engine's tool activity is durable but not forwarded.
            EngineEvent::Reasoning(_)
            | EngineEvent::ToolCallStarted { .. }
            | EngineEvent::ToolCallEnded { .. } => continue,
        };
        if !send_frame(ws, request_id, msg).await {
            return false;
        }
    }
    true
}

/// Answers `ListSessions` from the index.
async fn list_sessions<P: Provider>(
    ws: &mut Socket,
    engine: &Mutex<Engine<P>>,
    request_id: u64,
) -> ControlFlow<()> {
    let listed = engine.lock().await.sessions();
    let msg = match listed {
        Ok(sessions) => server_frame::Msg::SessionList(SessionList {
            sessions: sessions.iter().map(session_info).collect(),
        }),
        Err(error) => {
            warn!(%error, "listing sessions failed");
            error_frame("internal", &error)
        }
    };
    flow(send_frame(ws, request_id, msg).await)
}

/// Tells the client its bytes were not a frame, then closes.
async fn refuse(ws: &mut Socket, request_id: u64) {
    send_frame(
        ws,
        request_id,
        error_frame("bad_frame", "not a decodable ClientFrame"),
    )
    .await;
    let _ = ws.close(None).await;
}

/// Encodes one server frame and writes it.
///
/// Returns whether the client is still there. A write failure is not an error
/// to report — there is no one left to report it to.
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

/// The closing frame of a successful turn.
fn stream_end(reply: &Reply) -> server_frame::Msg {
    // No usage means the stream was cut before the model billed anything;
    // zeros say that as plainly as the wire can.
    let usage = reply.usage.unwrap_or_default();
    server_frame::Msg::StreamEnd(StreamEnd {
        session_id: reply.session_id.clone(),
        input_tokens: usage.input_tokens,
        output_tokens: usage.output_tokens,
        partial: reply.partial,
    })
}

/// Answers one session's history from the projection.
///
/// A read, not a turn: it takes the engine lock only long enough to query, so
/// a client opening an old session does not wait behind a completion.
async fn fetch_history<P: Provider>(
    ws: &mut Socket,
    engine: &Mutex<Engine<P>>,
    request_id: u64,
    session_id: &str,
) -> ControlFlow<()> {
    let read = engine.lock().await.transcript(session_id);
    let msg = match read {
        Ok(messages) => server_frame::Msg::SessionHistory(SessionHistory {
            session_id: session_id.to_owned(),
            messages: messages
                .into_iter()
                .map(|(role, content)| HistoryMessage { role, content })
                .collect(),
        }),
        Err(error) => {
            warn!(%error, session_id, "reading history failed");
            error_frame("internal", &error)
        }
    };
    flow(send_frame(ws, request_id, msg).await)
}

/// An `Error` frame with a stable code and a human-readable message.
fn error_frame(code: &str, msg: impl std::fmt::Display) -> server_frame::Msg {
    server_frame::Msg::Error(WireError {
        code: code.to_owned(),
        msg: msg.to_string(),
    })
}

/// The wire code for a session error.
///
/// Codes are the client's contract — `snake_case`, stable, and matched on. The
/// message beside them is for a person and may change freely.
fn error_code(error: &SessionError) -> &'static str {
    match error {
        SessionError::EmptyMessage => "empty_message",
        SessionError::EmptyReply => "empty_reply",
        SessionError::Provider(_) => "provider",
        // Durable state is in doubt and no client can do anything about it.
        SessionError::Log(_) | SessionError::Projection(_) => "internal",
    }
}

/// The frame kind, for the request span.
fn kind(frame: &ClientFrame) -> &'static str {
    match frame.msg {
        Some(client_frame::Msg::SendMessage(_)) => "send_message",
        Some(client_frame::Msg::ListSessions(_)) => "list_sessions",
        Some(client_frame::Msg::FetchHistory(_)) => "fetch_history",
        None => "unknown",
    }
}

/// A projection row as the wire describes a session.
fn session_info(summary: &SessionSummary) -> SessionInfo {
    SessionInfo {
        id: summary.id.clone(),
        title: summary.title.clone(),
        started_at: summary.started_at.map(timestamp),
        preview: summary.preview.clone(),
        last_at: summary.last_at.map(timestamp),
    }
}

/// Microseconds since the Unix epoch back to a protobuf timestamp.
///
/// The projection stores microseconds — one sortable integer column — and the
/// wire speaks `Timestamp`; this is the only place the two meet. Euclidean
/// division keeps `nanos` non-negative for pre-epoch values, which protobuf
/// requires and a plain remainder would get wrong.
fn timestamp(micros: i64) -> Timestamp {
    Timestamp {
        seconds: micros.div_euclid(1_000_000),
        // Always in 0..1_000_000_000, so the conversion cannot fail.
        nanos: i32::try_from(micros.rem_euclid(1_000_000) * 1_000).unwrap_or(0),
    }
}

/// "The client is still there" as the read loop's control flow.
fn flow(connected: bool) -> ControlFlow<()> {
    if connected {
        ControlFlow::Continue(())
    } else {
        ControlFlow::Break(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::Mutex as StdMutex;

    use arc_core::log::Log;
    use arc_core::projection::Projection;
    use arc_core::provider::{
        CompletionDelta, CompletionRequest, CompletionStream, Error as ProviderError, Message,
        Stop, Usage,
    };
    use arc_core::tool::Registry;
    use arc_proto::v1::{FetchHistory, ListSessions, Role};
    use futures::stream;
    use tempfile::TempDir;
    use tokio::sync::oneshot;
    use tokio::task::JoinHandle;
    use tokio_tungstenite::MaybeTlsStream;

    use super::*;

    /// Longest any assertion waits on the socket. Generous enough never to fire
    /// on a loaded machine, short enough that a hang fails instead of hanging.
    const PATIENCE: Duration = Duration::from_secs(5);

    fn usage() -> Usage {
        Usage {
            input_tokens: 3,
            output_tokens: 5,
        }
    }

    /// What the mock provider does with a request.
    enum Script {
        /// Reply `re: <the last user message>`, then `Done`. Ties every reply
        /// to the request that caused it, which is what makes concurrent
        /// clients checkable.
        Echo,
        /// Yield exactly these items, one entry per call.
        Canned(VecDeque<Vec<Result<CompletionDelta, ProviderError>>>),
    }

    /// A provider with no network: it answers from [`Script`] and keeps every
    /// request it was given.
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

        async fn complete(
            &self,
            request: CompletionRequest,
        ) -> Result<CompletionStream, ProviderError> {
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
                            } => Some(content.clone()),
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
            Ok(Box::pin(stream::iter(items)))
        }
    }

    /// A running server over a temporary log, plus the handles to inspect and
    /// stop it.
    struct Harness {
        addr: SocketAddr,
        provider: Arc<MockProvider>,
        shutdown: Option<oneshot::Sender<()>>,
        server: JoinHandle<()>,
        _dir: TempDir,
    }

    impl Harness {
        async fn start(script: Script) -> Self {
            let dir = TempDir::new().expect("temp dir");
            let log = Log::open(dir.path()).expect("open log");
            let projection = Projection::open(":memory:").expect("open projection");
            let provider = MockProvider::new(script);
            let registry = Registry::new(512);
            let engine = Engine::new(
                log,
                projection,
                Arc::clone(&provider),
                "test-model",
                Some("be terse".to_owned()),
                registry,
                false,
            );

            let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
            let addr = listener.local_addr().expect("local addr");
            let (shutdown, signal) = oneshot::channel();
            let server = tokio::spawn(serve(listener, Arc::new(Mutex::new(engine)), async {
                let _ = signal.await;
            }));

            Self {
                addr,
                provider,
                shutdown: Some(shutdown),
                server,
                _dir: dir,
            }
        }

        async fn connect(&self) -> Client {
            let (ws, _) = tokio_tungstenite::connect_async(format!("ws://{}", self.addr))
                .await
                .expect("connect");
            ws
        }

        /// Signals shutdown and waits for the server to come back.
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
        })
    }

    /// The next raw message, or `None` if the server closed the connection.
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

    /// Reads one whole turn: `MessageAccepted`, every `Delta`, the closing
    /// frame. Returns the session id, the joined delta text, and the last
    /// frame's message.
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

    async fn history(ws: &mut Client, request_id: u64, session_id: &str) -> Vec<HistoryMessage> {
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
                history.messages
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

    #[tokio::test]
    async fn a_message_round_trips_accepted_deltas_and_stream_end() {
        let mut harness = Harness::start(Script::Echo).await;
        let mut ws = harness.connect().await;

        send(&mut ws, 7, say("", "hello")).await;
        let (session_id, text, closing) = turn(&mut ws, 7).await;

        assert_eq!(text, "re: hello");
        let end = ended(closing);
        assert_eq!(end.session_id, session_id);
        assert_eq!(end.input_tokens, usage().input_tokens);
        assert_eq!(end.output_tokens, usage().output_tokens);
        assert!(!end.partial);

        harness.stop().await;
    }

    #[tokio::test]
    async fn a_second_message_continues_the_session_with_its_history() {
        let mut harness = Harness::start(Script::Echo).await;
        let mut ws = harness.connect().await;

        send(&mut ws, 1, say("", "one")).await;
        let (session_id, first, _) = turn(&mut ws, 1).await;
        send(&mut ws, 2, say(&session_id, "two")).await;
        let (again, second, closing) = turn(&mut ws, 2).await;

        assert_eq!(first, "re: one");
        assert_eq!(second, "re: two");
        assert_eq!(again, session_id, "the same session, not a new one");
        assert!(!ended(closing).partial);

        // The second completion saw the first exchange: the log grew and the
        // history came back out of it.
        let requests = harness.provider.requests();
        assert_eq!(requests.len(), 2);
        let turns: Vec<(Role, &str)> = requests[1]
            .messages
            .iter()
            .map(|m| match m {
                Message::Text { role, content } => (*role, content.as_str()),
                other => panic!("expected a text message, got {other:?}"),
            })
            .collect();
        assert_eq!(
            turns,
            [
                (Role::User, "one"),
                (Role::Assistant, "re: one"),
                (Role::User, "two"),
            ]
        );

        harness.stop().await;
    }

    #[tokio::test]
    async fn list_sessions_is_empty_before_and_names_the_session_after() {
        let mut harness = Harness::start(Script::Echo).await;
        let mut ws = harness.connect().await;

        assert_eq!(list(&mut ws, 1).await, [], "nothing has happened yet");

        send(&mut ws, 2, say("", "hello")).await;
        let (session_id, _, _) = turn(&mut ws, 2).await;

        let sessions = list(&mut ws, 3).await;
        assert_eq!(sessions.len(), 1);
        assert_eq!(sessions[0].id, session_id);
        assert_eq!(sessions[0].title, "", "sessions are unnamed in Phase 1");
        assert!(
            sessions[0].started_at.is_some(),
            "the projection's micros became a Timestamp"
        );

        harness.stop().await;
    }

    /// What a client needs to reopen an old session and see what was said.
    #[tokio::test]
    async fn history_returns_the_whole_conversation_in_order() {
        let mut harness = Harness::start(Script::Echo).await;
        let mut ws = harness.connect().await;

        send(&mut ws, 1, say("", "hello")).await;
        let (session_id, _, _) = turn(&mut ws, 1).await;
        send(&mut ws, 2, say(&session_id, "again")).await;
        turn(&mut ws, 2).await;

        let messages = history(&mut ws, 3, &session_id).await;

        let said: Vec<(Role, &str)> = messages
            .iter()
            .map(|m| {
                (
                    Role::try_from(m.role).expect("a known role"),
                    m.content.as_str(),
                )
            })
            .collect();
        assert_eq!(
            said,
            [
                (Role::User, "hello"),
                (Role::Assistant, "re: hello"),
                (Role::User, "again"),
                (Role::Assistant, "re: again"),
            ],
            "both turns, in the order they happened"
        );

        // A read, not a turn: the connection is good for the next request.
        assert!(
            history(&mut ws, 4, "no-such-session").await.is_empty(),
            "an unknown session reads as an empty one"
        );

        harness.stop().await;
    }

    /// The picker labels rows with this, so it has to be the opening line.
    #[tokio::test]
    async fn listed_sessions_preview_their_first_user_message() {
        let mut harness = Harness::start(Script::Echo).await;
        let mut ws = harness.connect().await;

        send(&mut ws, 1, say("", "what is a walking skeleton?")).await;
        let (session_id, _, _) = turn(&mut ws, 1).await;
        send(&mut ws, 2, say(&session_id, "a second question")).await;
        turn(&mut ws, 2).await;

        let sessions = list(&mut ws, 3).await;

        assert_eq!(sessions[0].preview, "what is a walking skeleton?");
        assert_eq!(
            sessions[0].title, "",
            "preview is not a title; titles wait for Phase 2"
        );

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

        // Same connection, next request: unaffected.
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
    async fn bad_bytes_get_bad_frame_and_end_the_connection() {
        let mut harness = Harness::start(Script::Echo).await;
        let mut ws = harness.connect().await;

        // Field 1 as a length-delimited value with an absurd length: not a
        // ClientFrame under any schema version.
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

    #[tokio::test]
    async fn a_frame_with_no_request_is_also_a_bad_frame() {
        let mut harness = Harness::start(Script::Echo).await;
        let mut ws = harness.connect().await;

        let frame = ClientFrame {
            request_id: 9,
            msg: None,
        };
        ws.send(WsMessage::binary(frame.encode_to_vec()))
            .await
            .expect("send");

        let answer = next_frame(&mut ws).await;
        assert_eq!(answer.request_id, 9, "a known request id is still echoed");
        assert_eq!(failed(answer.msg.expect("a message")).code, "bad_frame");

        harness.stop().await;
    }

    #[tokio::test]
    async fn two_connections_are_serialized_and_both_get_their_own_reply() {
        let mut harness = Harness::start(Script::Echo).await;
        let mut first = harness.connect().await;
        let mut second = harness.connect().await;

        // Both requests are in flight before either is read: the engine's
        // mutex, not the client, decides the order they run in.
        send(&mut first, 1, say("", "alpha")).await;
        send(&mut second, 2, say("", "beta")).await;

        let (alpha_session, alpha, alpha_end) = turn(&mut first, 1).await;
        let (beta_session, beta, beta_end) = turn(&mut second, 2).await;

        assert_eq!(alpha, "re: alpha", "no crossed replies");
        assert_eq!(beta, "re: beta");
        assert_ne!(alpha_session, beta_session, "two sessions, not one");
        assert!(!ended(alpha_end).partial);
        assert!(!ended(beta_end).partial);

        // Both landed in the log, and each completion saw only its own turn.
        let sessions = list(&mut first, 3).await;
        assert_eq!(sessions.len(), 2);
        for request in harness.provider.requests() {
            assert_eq!(request.messages.len(), 1, "no history bled across sessions");
            assert_eq!(request.system.as_deref(), Some("be terse"));
        }

        harness.stop().await;
    }

    #[tokio::test]
    async fn shutdown_closes_an_idle_connection() {
        let mut harness = Harness::start(Script::Echo).await;
        let mut ws = harness.connect().await;

        // One completed request, then nothing in flight.
        send(&mut ws, 1, say("", "hello")).await;
        turn(&mut ws, 1).await;

        harness.stop().await;

        match next_message(&mut ws).await {
            Some(WsMessage::Close(_)) | None => {}
            other => panic!("expected the connection to close, got {other:?}"),
        }
    }
}
