//! Smoke tests for the generated protobuf types: encode/decode round trips,
//! forward compatibility with additive schema changes, and proto3 defaults.

use arc_proto::v1::{
    ClientFrame, Delta, Error, Event, MessageAccepted, MessageAppended, Role, SendMessage,
    ServerFrame, SessionCreated, SessionEvent, SessionInfo, SessionList, Source, StreamEnd,
    client_frame, event, server_frame, session_event,
};
use prost::Message;
use prost_types::Timestamp;

/// Fixed so tests are deterministic; no wall-clock reads anywhere here.
fn ts() -> Timestamp {
    Timestamp {
        seconds: 1_700_000_000,
        nanos: 123_456_789,
    }
}

fn round_trip<M: Message + Default + PartialEq + std::fmt::Debug>(msg: &M) {
    let bytes = msg.encode_to_vec();
    let decoded = M::decode(bytes.as_slice()).expect("decode");
    assert_eq!(*msg, decoded);
}

fn session_created_event() -> Event {
    Event {
        seq: 1,
        ts: Some(ts()),
        source: Source::System as i32,
        payload: Some(event::Payload::Session(SessionEvent {
            event: Some(session_event::Event::SessionCreated(SessionCreated {
                session_id: "s-01".to_string(),
                title: "first light".to_string(),
                provider: "gemini".to_string(),
                model: "gemini-2.5-pro".to_string(),
            })),
        })),
    }
}

fn message_appended_event() -> Event {
    Event {
        seq: 2,
        ts: Some(ts()),
        source: Source::User as i32,
        payload: Some(event::Payload::Session(SessionEvent {
            event: Some(session_event::Event::MessageAppended(MessageAppended {
                session_id: "s-01".to_string(),
                role: Role::User as i32,
                content: "hello arc".to_string(),
                partial: false,
            })),
        })),
    }
}

#[test]
fn event_session_created_round_trips() {
    round_trip(&session_created_event());
}

#[test]
fn event_message_appended_round_trips() {
    round_trip(&message_appended_event());
}

#[test]
fn client_frame_send_message_round_trips() {
    round_trip(&ClientFrame {
        request_id: 7,
        msg: Some(client_frame::Msg::SendMessage(SendMessage {
            session_id: String::new(), // empty id = create a new session
            content: "hello arc".to_string(),
        })),
    });
}

#[test]
fn server_frame_arms_round_trip() {
    let arms = [
        server_frame::Msg::SessionList(SessionList {
            sessions: vec![SessionInfo {
                id: "s-01".to_string(),
                title: "first light".to_string(),
                started_at: Some(ts()),
            }],
        }),
        server_frame::Msg::MessageAccepted(MessageAccepted {
            session_id: "s-01".to_string(),
        }),
        server_frame::Msg::Delta(Delta {
            session_id: "s-01".to_string(),
            text: "streamed ".to_string(),
        }),
        server_frame::Msg::StreamEnd(StreamEnd {
            session_id: "s-01".to_string(),
            input_tokens: 128,
            output_tokens: 64,
        }),
        server_frame::Msg::Error(Error {
            code: "provider_unavailable".to_string(),
            msg: "upstream returned 503".to_string(),
        }),
    ];

    for arm in arms {
        round_trip(&ServerFrame {
            request_id: 7,
            msg: Some(arm),
        });
    }
}

/// A reader on an old binary must still decode events written by a newer one.
/// Simulates that by appending a field this schema version has never seen.
#[test]
fn unknown_field_is_skipped() {
    let original = message_appended_event();
    let mut bytes = original.encode_to_vec();

    // Field 100, wire type 2 (length-delimited): key = (100 << 3) | 2 = 802,
    // encoded as a varint (0xa2 0x06), then the length, then the payload.
    bytes.extend_from_slice(&[0xa2, 0x06, 0x03, 0xde, 0xad, 0xbe]);

    let decoded = Event::decode(bytes.as_slice()).expect("decode with unknown field");
    assert_eq!(decoded, original);
}

/// proto3 has no presence for scalars: an empty payload is a valid message with
/// every field at its default. The log reader depends on this.
#[test]
fn empty_bytes_decode_to_defaults() {
    let decoded = Event::decode(&[][..]).expect("decode empty");

    assert_eq!(decoded, Event::default());
    assert_eq!(decoded.seq, 0);
    assert!(decoded.ts.is_none());
    assert_eq!(decoded.source, Source::Unspecified as i32);
    assert!(decoded.payload.is_none());
}
