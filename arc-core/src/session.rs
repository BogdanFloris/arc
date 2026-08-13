//! The session engine: the conversation loop over log, projection, and provider.
//!
//! [`Engine`] owns the only write path a conversation has. One call to
//! [`Engine::send_message`] is: make the user's message durable, drive the model,
//! make the reply durable. The log is the source of truth at every step.
//!
//! # How a completion ends
//!
//! The three stream endings map to log by one rule: the log records what the user
//! saw and errors are reported, never archived as messages (DESIGN.md §4).
//!
//! - `Done` seen → the reply is appended whole, `partial: false`.
//! - Stream cut after text → the text is appended with `partial: true`.
//! - An error, or a cut before any text → nothing model-side is appended; the
//!   user's message and the session stay in the log, and the error goes to
//!   the caller.
//!
//! # Streaming to callers
//!
//! [`EngineEvent`]s mirror the wire protocol event for event, so the daemon's
//! socket layer is a translator, not a decision-maker. `Accepted` is sent
//! before the provider is called: the caller learns its session id even if
//! the model fails instantly, because by then the message is already durable.
//! A closed channel means the client went away — the completion is driven to
//! its end and appended regardless; durability does not depend on anyone
//! watching.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use arc_proto::v1::{Event, MessageAppended, Role, SessionCreated, SessionEvent, Source};
use arc_proto::v1::{event, session_event};
use futures::StreamExt as _;
use prost_types::Timestamp;
use tokio::sync::mpsc;

use crate::log::{self, Log};
use crate::projection::{self, Projection, SessionSummary};
use crate::provider::{self, CompletionDelta, CompletionRequest, Message, Provider, Usage};

/// The conversation loop, generic over the model backend so tests drive it
/// with a scripted provider and no network.
///
/// `&mut self` on [`send_message`](Engine::send_message): one completion
/// at a time per engine. The daemon serializes callers around it.
pub struct Engine<P> {
    log: Log,
    projection: Projection,
    provider: Arc<P>,
    model: String,
    /// The identity file's content, when there is one (DESIGN.md §5.1).
    system: Option<String>,
}

/// What the engine reports to its caller while a message is in flight.
///
/// Mirrors the wire protocol: `Accepted` → `MessageAccepted`, `Delta` →
/// `Delta`; the returned [`Reply`] carries what `StreamEnd` needs, and a
/// returned error is the `Error` frame.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EngineEvent {
    Accepted { session_id: String },
    Delta(String),
}

/// The outcome of one [`Engine::send_message`] call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Reply {
    pub session_id: String,
    pub seq: u64,
    pub usage: Option<Usage>,
    pub partial: bool,
}

/// Everything one conversation turn can fail with.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Appending to the event log failed. Durable state is in doubt; the
    /// caller should treat this as fatal.
    #[error("session log append: {0}")]
    Log(#[from] log::Error),

    /// The index refused an event the log accepted — log and projection are
    /// out of step, which is a bug, not a runtime condition.
    #[error("session projection: {0}")]
    Projection(#[from] projection::Error),

    /// The provider failed, eagerly or mid-stream. Any text that arrived
    /// first is already in the log, marked partial.
    #[error("session provider: {0}")]
    Provider(#[from] provider::Error),

    /// A whitespace-only message. Nothing was appended.
    #[error("refusing to send an empty message")]
    EmptyMessage,

    /// The stream ended before the first token. No assistant message was
    /// appended: there was nothing seen to record.
    #[error("the model produced no reply")]
    EmptyReply,
}

impl<P: Provider> Engine<P> {
    /// An engine over an open log and its index.
    ///
    /// `system` is the identity file's content when present; it rides every
    /// completion as the system prompt.
    pub fn new(
        log: Log,
        projection: Projection,
        provider: Arc<P>,
        model: impl Into<String>,
        system: Option<String>,
    ) -> Self {
        Self {
            log,
            projection,
            provider,
            model: model.into(),
            system,
        }
    }

    /// One conversation turn: durably append the user's message (creating the
    /// session when `session_id` is `None`), drive the model, durably append
    /// what came back.
    ///
    /// Progress is reported on `events` — [`EngineEvent::Accepted`] first,
    /// then each text chunk. A closed channel is not an error: the completion
    /// is driven to its end and appended regardless.
    ///
    /// # Errors
    ///
    /// - [`Error::EmptyMessage`] for whitespace-only content; nothing appended.
    /// - [`Error::Log`] / [`Error::Projection`] if durability fails; fatal.
    /// - [`Error::Provider`] if the model call fails. Text that arrived before
    ///   a mid-stream failure is appended with `partial: true` first.
    /// - [`Error::EmptyReply`] if the stream ended before the first token.
    #[tracing::instrument(
        level = "info",
        name = "session.send_message",
        skip_all,
        fields(
            model = %self.model,
            session_id = tracing::field::Empty,
            new_session = tracing::field::Empty,
            outcome = tracing::field::Empty,
            assistant_seq = tracing::field::Empty,
        )
    )]
    pub async fn send_message(
        &mut self,
        session_id: Option<&str>,
        content: &str,
        events: mpsc::Sender<EngineEvent>,
    ) -> Result<Reply, Error> {
        if content.trim().is_empty() {
            return Err(Error::EmptyMessage);
        }
        let span = tracing::Span::current();
        let (session_id, new_session) = match session_id {
            Some(id) => (id.to_owned(), false),
            None => (uuid::Uuid::new_v4().to_string(), true),
        };
        span.record("session_id", session_id.as_str());
        span.record("new_session", new_session);

        if new_session {
            self.record(
                Source::User,
                session_event::Event::SessionCreated(SessionCreated {
                    session_id: session_id.clone(),
                    title: String::new(),
                    provider: self.provider.name().to_owned(),
                    model: self.model.clone(),
                }),
            )?;
        }
        self.record(
            Source::User,
            session_event::Event::MessageAppended(MessageAppended {
                session_id: session_id.clone(),
                role: Role::User as i32,
                content: content.to_owned(),
                partial: false,
            }),
        )?;

        // Durable from here on: tell the caller where its message lives.
        let _ = events
            .send(EngineEvent::Accepted {
                session_id: session_id.clone(),
            })
            .await;

        // History includes the message just appended, so nothing is added on
        // top of it here.
        let request = CompletionRequest {
            model: self.model.clone(),
            system: self.system.clone(),
            messages: self.history(&session_id)?,
        };

        let mut stream = self.provider.complete(request).await?;
        let mut text = String::new();
        let mut usage = None;
        let ending = loop {
            match stream.next().await {
                Some(Ok(CompletionDelta::Text(chunk))) => {
                    text.push_str(&chunk);
                    let _ = events.send(EngineEvent::Delta(chunk)).await;
                }
                // The stream contract says `Done` is the last item; trusting
                // it saves a poll that could only return `None`.
                Some(Ok(CompletionDelta::Done { usage: done })) => {
                    usage = Some(done);
                    break Ending::Done;
                }
                Some(Err(error)) => break Ending::Failed(error),
                None => break Ending::Cut,
            }
        };

        match ending {
            Ending::Done => {
                let seq = self.append_reply(&session_id, &text, false)?;
                span.record("outcome", "done");
                span.record("assistant_seq", seq);
                Ok(Reply {
                    session_id,
                    seq,
                    usage,
                    partial: false,
                })
            }
            Ending::Cut if text.is_empty() => {
                span.record("outcome", "error");
                Err(Error::EmptyReply)
            }
            Ending::Cut => {
                let seq = self.append_reply(&session_id, &text, true)?;
                span.record("outcome", "partial");
                span.record("assistant_seq", seq);
                Ok(Reply {
                    session_id,
                    seq,
                    usage: None,
                    partial: true,
                })
            }
            Ending::Failed(error) => {
                // The text seen so far is appended before the error is
                // surfaced — an append failure takes precedence, because a
                // durability problem outranks a provider problem.
                if !text.is_empty() {
                    let seq = self.append_reply(&session_id, &text, true)?;
                    span.record("assistant_seq", seq);
                }
                span.record("outcome", "error");
                Err(error.into())
            }
        }
    }

    /// Every session, oldest first (see [`Projection::sessions`]).
    ///
    /// The engine is the façade over durable state: callers above it — the
    /// daemon's socket layer — never reach past it to the log or the index.
    ///
    /// # Errors
    ///
    /// [`Error::Projection`] if the index cannot be read.
    pub fn sessions(&self) -> Result<Vec<SessionSummary>, Error> {
        Ok(self.projection.sessions()?)
    }

    /// Appends one event to the log and applies it to the index, as a pair.
    ///
    /// The log stamps the seq; the same event, seq included, then goes into
    /// the projection so both stay in lockstep. Replay remains the cold-start
    /// path only.
    fn record(&mut self, source: Source, payload: session_event::Event) -> Result<u64, Error> {
        let mut event = Event {
            seq: 0, // added by the log
            ts: Some(now_ts()),
            source: source as i32,
            payload: Some(event::Payload::Session(SessionEvent {
                event: Some(payload),
            })),
        };
        let seq = self.log.append(event.clone())?;
        event.seq = seq;
        self.projection.apply(&event)?;
        Ok(seq)
    }

    /// Appends the model's reply.
    fn append_reply(&mut self, session_id: &str, text: &str, partial: bool) -> Result<u64, Error> {
        self.record(
            Source::Model,
            session_event::Event::MessageAppended(MessageAppended {
                session_id: session_id.to_owned(),
                role: Role::Assistant as i32,
                content: text.to_owned(),
                partial,
            }),
        )
    }

    /// The session's history as provider messages.
    ///
    /// Roles the provider vocabulary has no place for — a value from a newer
    /// schema, or a system role that should never be in message history — are
    /// skipped with a warning rather than failing the turn: an old binary
    /// must be able to converse over a newer log.
    fn history(&self, session_id: &str) -> Result<Vec<Message>, Error> {
        let mut messages = Vec::new();
        for (role, content) in self.projection.messages(session_id)? {
            if let Ok(mapped @ (Role::User | Role::Assistant)) = Role::try_from(role) {
                messages.push(Message {
                    role: mapped,
                    content,
                });
            } else {
                tracing::warn!(role, "skipping history message with an unmappable role");
            }
        }
        Ok(messages)
    }
}

/// How the provider stream ended; see the module docs.
enum Ending {
    Done,
    Cut,
    Failed(provider::Error),
}

/// The current wall clock as a protobuf timestamp.
///
/// The engine is where clock-reading lives — the log deliberately stamps
/// nothing but seq.
fn now_ts() -> Timestamp {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    Timestamp {
        seconds: i64::try_from(elapsed.as_secs()).unwrap_or(i64::MAX),
        nanos: i32::try_from(elapsed.subsec_nanos()).unwrap_or(0),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::{Arc, Mutex};

    use arc_proto::v1::{Role, Source, session_event};
    use futures::stream;
    use tempfile::TempDir;
    use tokio::sync::mpsc;

    use super::{Engine, EngineEvent, Error};
    use crate::log::{Log, LogReader, discover_segments};
    use crate::projection::Projection;
    use crate::provider::{
        CompletionDelta, CompletionRequest, CompletionStream, Error as ProviderError, Provider,
        Usage,
    };

    /// A scripted provider: each `complete` call captures its request and
    /// yields the next script entry.
    struct MockProvider {
        script: Mutex<VecDeque<Vec<Result<CompletionDelta, ProviderError>>>>,
        captured: Mutex<Vec<CompletionRequest>>,
    }

    impl MockProvider {
        fn scripted(calls: Vec<Vec<Result<CompletionDelta, ProviderError>>>) -> Arc<Self> {
            Arc::new(Self {
                script: Mutex::new(calls.into()),
                captured: Mutex::new(Vec::new()),
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
            self.captured.lock().expect("captured").push(request);
            let items = self
                .script
                .lock()
                .expect("script")
                .pop_front()
                .expect("script exhausted");
            Ok(Box::pin(stream::iter(items)))
        }
    }

    fn usage() -> Usage {
        Usage {
            input_tokens: 3,
            output_tokens: 5,
        }
    }

    fn done_reply(text: &str) -> Vec<Result<CompletionDelta, ProviderError>> {
        vec![
            Ok(CompletionDelta::Text(text.to_owned())),
            Ok(CompletionDelta::Done { usage: usage() }),
        ]
    }

    /// An engine over a fresh log and in-memory index.
    fn engine(provider: &Arc<MockProvider>, dir: &TempDir) -> Engine<MockProvider> {
        let log = Log::open(dir.path()).expect("open log");
        let projection = Projection::open(":memory:").expect("open projection");
        Engine::new(
            log,
            projection,
            Arc::clone(provider),
            "test-model",
            Some("be terse".to_owned()),
        )
    }

    fn channel() -> (mpsc::Sender<EngineEvent>, mpsc::Receiver<EngineEvent>) {
        mpsc::channel(64)
    }

    fn drain(rx: &mut mpsc::Receiver<EngineEvent>) -> Vec<EngineEvent> {
        let mut events = Vec::new();
        while let Ok(event) = rx.try_recv() {
            events.push(event);
        }
        events
    }

    /// Every event in the engine's log, replayed through the real reader.
    fn replay_log(engine: &Engine<MockProvider>) -> Vec<session_event::Event> {
        let segments = discover_segments(engine.log.dir()).expect("discover");
        LogReader::new(segments)
            .map(|result| {
                let event = result.expect("replay");
                match event.payload.expect("payload") {
                    arc_proto::v1::event::Payload::Session(session) => {
                        session.event.expect("session event")
                    }
                }
            })
            .collect()
    }

    fn appended(event: &session_event::Event) -> &arc_proto::v1::MessageAppended {
        match event {
            session_event::Event::MessageAppended(m) => m,
            other @ session_event::Event::SessionCreated(_) => {
                panic!("expected a message, got {other:?}")
            }
        }
    }

    #[tokio::test]
    async fn a_new_session_logs_created_user_and_assistant() {
        let provider = MockProvider::scripted(vec![done_reply("hello there")]);
        let dir = TempDir::new().expect("temp dir");
        let mut engine = engine(&provider, &dir);
        let (tx, mut rx) = channel();

        let reply = engine
            .send_message(None, "hi", tx)
            .await
            .expect("send_message");

        assert_eq!(reply.usage, Some(usage()));
        assert!(!reply.partial);
        assert_eq!(reply.seq, 2);

        let events = replay_log(&engine);
        assert_eq!(events.len(), 3);
        let session_event::Event::SessionCreated(created) = &events[0] else {
            panic!("expected SessionCreated first, got {:?}", events[0]);
        };
        assert_eq!(created.session_id, reply.session_id);
        assert_eq!(created.provider, "mock");
        assert_eq!(created.model, "test-model");

        let user = appended(&events[1]);
        assert_eq!(
            (user.role, user.content.as_str()),
            (Role::User as i32, "hi")
        );
        assert!(!user.partial);
        let assistant = appended(&events[2]);
        assert_eq!(assistant.role, Role::Assistant as i32);
        assert_eq!(assistant.content, "hello there");
        assert!(!assistant.partial);

        // The projection kept pace without a replay.
        assert_eq!(engine.projection.last_seq().expect("last_seq"), Some(2));
        assert_eq!(
            engine
                .projection
                .messages(&reply.session_id)
                .expect("messages")
                .len(),
            2
        );

        // Channel saw Accepted first, then the delta.
        let events = drain(&mut rx);
        assert_eq!(
            events,
            [
                EngineEvent::Accepted {
                    session_id: reply.session_id.clone()
                },
                EngineEvent::Delta("hello there".to_owned()),
            ]
        );
    }

    #[tokio::test]
    async fn a_second_message_reuses_the_session_and_sends_history() {
        let provider =
            MockProvider::scripted(vec![done_reply("first reply"), done_reply("second reply")]);
        let dir = TempDir::new().expect("temp dir");
        let mut engine = engine(&provider, &dir);

        let (tx, _rx) = channel();
        let first = engine
            .send_message(None, "one", tx)
            .await
            .expect("first send");
        let (tx, _rx) = channel();
        engine
            .send_message(Some(&first.session_id), "two", tx)
            .await
            .expect("second send");

        let events = replay_log(&engine);
        assert_eq!(events.len(), 5, "exactly one SessionCreated");

        let requests = provider.requests();
        assert_eq!(requests[1].system.as_deref(), Some("be terse"));
        let turns: Vec<(Role, &str)> = requests[1]
            .messages
            .iter()
            .map(|m| (m.role, m.content.as_str()))
            .collect();
        assert_eq!(
            turns,
            [
                (Role::User, "one"),
                (Role::Assistant, "first reply"),
                (Role::User, "two"),
            ],
            "history in order, current message last, nothing duplicated"
        );
    }

    #[tokio::test]
    async fn sessions_lists_what_send_message_created() {
        let provider = MockProvider::scripted(vec![done_reply("one"), done_reply("two")]);
        let dir = TempDir::new().expect("temp dir");
        let mut engine = engine(&provider, &dir);
        assert_eq!(engine.sessions().expect("sessions"), []);

        let (tx, _rx) = channel();
        let first = engine
            .send_message(None, "a", tx)
            .await
            .expect("first send");
        let (tx, _rx) = channel();
        let second = engine
            .send_message(None, "b", tx)
            .await
            .expect("second send");

        let listed = engine.sessions().expect("sessions");
        // Both were created inside the same second, so their relative order is
        // whatever the tie-break says; membership is what this asserts.
        let mut ids: Vec<&str> = listed.iter().map(|s| s.id.as_str()).collect();
        ids.sort_unstable();
        let mut expected = vec![first.session_id.as_str(), second.session_id.as_str()];
        expected.sort_unstable();
        assert_eq!(ids, expected);
        assert!(listed.iter().all(|s| s.title.is_empty()));
        assert!(listed.iter().all(|s| s.started_at.is_some()));
    }

    #[tokio::test]
    async fn a_cut_stream_appends_a_partial_reply() {
        let provider = MockProvider::scripted(vec![vec![Ok(CompletionDelta::Text(
            "partial tex".to_owned(),
        ))]]);
        let dir = TempDir::new().expect("temp dir");
        let mut engine = engine(&provider, &dir);
        let (tx, _rx) = channel();

        let reply = engine.send_message(None, "hi", tx).await.expect("send");

        assert!(reply.partial);
        assert_eq!(reply.usage, None);
        let events = replay_log(&engine);
        let assistant = appended(&events[2]);
        assert!(assistant.partial);
        assert_eq!(assistant.content, "partial tex");
    }

    #[tokio::test]
    async fn an_error_after_text_appends_partial_and_surfaces_the_error() {
        let provider = MockProvider::scripted(vec![vec![
            Ok(CompletionDelta::Text("some tex".to_owned())),
            Err(ProviderError::MalformedStream("boom".to_owned())),
        ]]);
        let dir = TempDir::new().expect("temp dir");
        let mut engine = engine(&provider, &dir);
        let (tx, _rx) = channel();

        let err = engine
            .send_message(None, "hi", tx)
            .await
            .expect_err("must surface");

        assert!(matches!(err, Error::Provider(_)), "got: {err:?}");
        let events = replay_log(&engine);
        assert_eq!(events.len(), 3, "the partial text was still appended");
        let assistant = appended(&events[2]);
        assert!(assistant.partial);
        assert_eq!(assistant.content, "some tex");
    }

    #[tokio::test]
    async fn an_error_before_text_appends_no_reply() {
        let provider = MockProvider::scripted(vec![vec![Err(ProviderError::MalformedStream(
            "instant".to_owned(),
        ))]]);
        let dir = TempDir::new().expect("temp dir");
        let mut engine = engine(&provider, &dir);
        let (tx, _rx) = channel();

        let err = engine
            .send_message(None, "hi", tx)
            .await
            .expect_err("must surface");

        assert!(matches!(err, Error::Provider(_)), "got: {err:?}");
        let events = replay_log(&engine);
        assert_eq!(
            events.len(),
            2,
            "session and user message survive, nothing else"
        );
    }

    #[tokio::test]
    async fn a_cut_before_any_text_is_an_empty_reply() {
        let provider = MockProvider::scripted(vec![vec![]]);
        let dir = TempDir::new().expect("temp dir");
        let mut engine = engine(&provider, &dir);
        let (tx, _rx) = channel();

        let err = engine
            .send_message(None, "hi", tx)
            .await
            .expect_err("must surface");

        assert!(matches!(err, Error::EmptyReply), "got: {err:?}");
        assert_eq!(replay_log(&engine).len(), 2);
    }

    #[tokio::test]
    async fn a_dropped_receiver_does_not_lose_the_append() {
        let provider = MockProvider::scripted(vec![done_reply("nobody watched")]);
        let dir = TempDir::new().expect("temp dir");
        let mut engine = engine(&provider, &dir);
        let (tx, rx) = channel();
        drop(rx);

        let reply = engine.send_message(None, "hi", tx).await.expect("send");

        assert!(!reply.partial);
        let events = replay_log(&engine);
        assert_eq!(appended(&events[2]).content, "nobody watched");
    }

    #[tokio::test]
    async fn an_empty_message_is_refused_before_anything_is_appended() {
        let provider = MockProvider::scripted(vec![]);
        let dir = TempDir::new().expect("temp dir");
        let mut engine = engine(&provider, &dir);
        let (tx, _rx) = channel();

        let err = engine
            .send_message(None, "  \n\t ", tx)
            .await
            .expect_err("must refuse");

        assert!(matches!(err, Error::EmptyMessage), "got: {err:?}");
        assert_eq!(replay_log(&engine).len(), 0, "log untouched");
        assert!(provider.requests().is_empty(), "provider never called");
    }

    #[tokio::test]
    async fn an_unmappable_role_in_history_is_skipped() {
        let provider = MockProvider::scripted(vec![done_reply("ok")]);
        let dir = TempDir::new().expect("temp dir");
        let mut engine = engine(&provider, &dir);

        // A message from a newer schema, planted through the engine's own
        // log-and-index pair so both stay consistent.
        engine
            .record(
                Source::System,
                session_event::Event::SessionCreated(arc_proto::v1::SessionCreated {
                    session_id: "s-old".to_owned(),
                    title: String::new(),
                    provider: "mock".to_owned(),
                    model: "test-model".to_owned(),
                }),
            )
            .expect("record");
        engine
            .record(
                Source::System,
                session_event::Event::MessageAppended(arc_proto::v1::MessageAppended {
                    session_id: "s-old".to_owned(),
                    role: 99,
                    content: "from the future".to_owned(),
                    partial: false,
                }),
            )
            .expect("record");

        let (tx, _rx) = channel();
        engine
            .send_message(Some("s-old"), "hi", tx)
            .await
            .expect("send");

        let requests = provider.requests();
        let turns: Vec<&str> = requests[0]
            .messages
            .iter()
            .map(|m| m.content.as_str())
            .collect();
        assert_eq!(turns, ["hi"], "the unmappable message stayed out");
    }
}
