use arc_proto::v1::{
    Budget, ClientFrame, Delta, Error, Event, HistoryEntry, HistoryMessage, HistoryToolCall,
    HistoryToolResult, MemoryEvent, MemoryRecord, MemoryRecordCreated, MemoryRecordDeleted,
    MemoryRecordReviewed, MemoryRecordSuperseded, MemoryRecordUpdated, MemoryReviewAccept,
    MemoryReviewDelete, MemoryReviewItem, MemoryReviewItems, MemoryReviewList, MessageAccepted,
    MessageAppended, Provenance, ProvenanceEntry, ReasoningDelta, Role, SendMessage, ServerFrame,
    SessionConsolidated, SessionCreated, SessionEvent, SessionHistory, SessionInfo, SessionList,
    SessionRole, Source, StreamEnd, ToolCallEnded, ToolCallIssued, ToolCallStarted, ToolOutcome,
    ToolResultRecorded, WorkspaceGrant, client_frame, event, history_entry, memory_event,
    memory_record, server_frame, session_event,
};
use prost::Message;
use prost_types::Timestamp;

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
                role: SessionRole::Executor as i32,
                project: "arc".to_string(),
                budget: Some(Budget {
                    total_tokens: 250_000,
                    wall_clock_seconds: 1_200,
                }),
                grants: vec![
                    WorkspaceGrant {
                        root: "/home/bogdan/arc".to_string(),
                        read_write: true,
                    },
                    WorkspaceGrant {
                        root: "/home/bogdan/notes".to_string(),
                        read_write: false,
                    },
                ],
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
                turn_id: "t-01".to_string(),
                input_tokens: 12,
                output_tokens: 34,
                elapsed_ms: 5600,
            })),
        })),
    }
}

fn tool_call_issued_event() -> Event {
    Event {
        seq: 3,
        ts: Some(ts()),
        source: Source::Model as i32,
        payload: Some(event::Payload::Session(SessionEvent {
            event: Some(session_event::Event::ToolCallIssued(ToolCallIssued {
                session_id: "s-01".to_string(),
                turn_id: "t-01".to_string(),
                call_id: "call-aa".to_string(),
                index: 1,
                name: "memory_search".to_string(),
                arguments_json: r#"{"query":"hello"}"#.to_string(),
                provider_roundtrip: vec![0xde, 0xad, 0x00, 0xbe, 0xef],
            })),
        })),
    }
}

fn tool_result_recorded_event() -> Event {
    Event {
        seq: 4,
        ts: Some(ts()),
        source: Source::System as i32,
        payload: Some(event::Payload::Session(SessionEvent {
            event: Some(session_event::Event::ToolResultRecorded(
                ToolResultRecorded {
                    session_id: "s-01".to_string(),
                    turn_id: "t-01".to_string(),
                    call_id: "call-aa".to_string(),
                    outcome: ToolOutcome::Ok as i32,
                    content: "one record".to_string(),
                    truncated: true,
                },
            )),
        })),
    }
}

fn session_consolidated_event() -> Event {
    Event {
        seq: 9,
        ts: Some(ts()),
        source: Source::System as i32,
        payload: Some(event::Payload::Session(SessionEvent {
            event: Some(session_event::Event::SessionConsolidated(
                SessionConsolidated {
                    session_id: "s-01".to_string(),
                    through_seq: 4,
                    prompt_version: String::new(),
                },
            )),
        })),
    }
}

fn memory_record() -> MemoryRecord {
    MemoryRecord {
        id: "m-01".to_string(),
        kind: memory_record::Kind::Preference as i32,
        namespace: "global".to_string(),
        title: "version control".to_string(),
        summary: "uses jj, not git".to_string(),
        body: "jj for everything; git only where a tool insists.".to_string(),
        links: vec!["m-02".to_string(), "m-07".to_string()],
        provenance: Some(Provenance {
            entries: vec![ProvenanceEntry {
                session_id: "s-01".to_string(),
                ts: Some(ts()),
            }],
        }),
        status: memory_record::Status::Active as i32,
    }
}

fn memory_payload_event(seq: u64, source: Source, event: memory_event::Event) -> Event {
    Event {
        seq,
        ts: Some(ts()),
        source: source as i32,
        payload: Some(event::Payload::Memory(MemoryEvent { event: Some(event) })),
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
fn event_tool_call_issued_round_trips() {
    round_trip(&tool_call_issued_event());
}

#[test]
fn event_tool_result_recorded_round_trips() {
    round_trip(&tool_result_recorded_event());
}

#[test]
fn event_session_consolidated_round_trips() {
    round_trip(&session_consolidated_event());
}

#[test]
fn event_memory_record_created_round_trips() {
    round_trip(&memory_payload_event(
        5,
        Source::Model,
        memory_event::Event::RecordCreated(MemoryRecordCreated {
            record: Some(memory_record()),
        }),
    ));
}

#[test]
fn event_memory_record_updated_round_trips() {
    round_trip(&memory_payload_event(
        6,
        Source::Model,
        memory_event::Event::RecordUpdated(MemoryRecordUpdated {
            record: Some(MemoryRecord {
                body: "jj for everything, no exceptions.".to_string(),
                ..memory_record()
            }),
        }),
    ));
}

#[test]
fn event_memory_record_superseded_round_trips() {
    round_trip(&memory_payload_event(
        7,
        Source::Model,
        memory_event::Event::RecordSuperseded(MemoryRecordSuperseded {
            superseded_id: "m-01".to_string(),
            record: Some(MemoryRecord {
                id: "m-09".to_string(),
                ..memory_record()
            }),
        }),
    ));
}

#[test]
fn event_memory_record_deleted_round_trips() {
    round_trip(&memory_payload_event(
        8,
        Source::User,
        memory_event::Event::RecordDeleted(MemoryRecordDeleted {
            id: "m-01".to_string(),
        }),
    ));
}

#[test]
fn event_memory_record_reviewed_round_trips() {
    round_trip(&memory_payload_event(
        9,
        Source::User,
        memory_event::Event::RecordReviewed(MemoryRecordReviewed {
            record_id: "m-01".to_string(),
        }),
    ));
}

#[test]
fn client_frame_review_arms_round_trip() {
    let arms = [
        client_frame::Msg::MemoryReviewList(MemoryReviewList {
            since_micros: 1_700_000_000_000_000,
        }),
        client_frame::Msg::MemoryReviewAccept(MemoryReviewAccept {
            record_id: "m-01".to_string(),
        }),
        client_frame::Msg::MemoryReviewDelete(MemoryReviewDelete {
            record_id: "m-01".to_string(),
        }),
    ];
    for arm in arms {
        round_trip(&ClientFrame {
            request_id: 7,
            msg: Some(arm),
        });
    }
}

#[test]
fn server_frame_review_items_round_trips() {
    round_trip(&ServerFrame {
        request_id: 7,
        msg: Some(server_frame::Msg::MemoryReviewItems(MemoryReviewItems {
            items: vec![MemoryReviewItem {
                record: Some(memory_record()),
                changed_at_micros: 1_700_000_000_000_000,
                superseded_by: "m-09".to_string(),
            }],
        })),
    });
}

#[test]
fn client_frame_send_message_round_trips() {
    round_trip(&ClientFrame {
        request_id: 7,
        msg: Some(client_frame::Msg::SendMessage(SendMessage {
            session_id: String::new(),
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
                preview: "hello arc".to_string(),
                last_at: Some(ts()),
            }],
        }),
        server_frame::Msg::SessionHistory(SessionHistory {
            session_id: "s-01".to_string(),
            entries: vec![
                HistoryEntry {
                    entry: Some(history_entry::Entry::Message(HistoryMessage {
                        role: Role::User as i32,
                        content: "hello arc".to_string(),
                        partial: false,
                        source: 0,
                        input_tokens: 12,
                        output_tokens: 34,
                        elapsed_ms: 5600,
                    })),
                },
                HistoryEntry {
                    entry: Some(history_entry::Entry::ToolCall(HistoryToolCall {
                        call_id: "call-aa".to_string(),
                        name: "memory_search".to_string(),
                        arguments_json: String::new(),
                    })),
                },
                HistoryEntry {
                    entry: Some(history_entry::Entry::ToolResult(HistoryToolResult {
                        call_id: "call-aa".to_string(),
                        outcome: ToolOutcome::Ok as i32,
                        truncated: true,
                    })),
                },
            ],
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
            partial: true,
        }),
        server_frame::Msg::Error(Error {
            code: "provider_unavailable".to_string(),
            msg: "upstream returned 503".to_string(),
        }),
        server_frame::Msg::ReasoningDelta(ReasoningDelta {
            session_id: "s-01".to_string(),
            text: "let me look that up ".to_string(),
        }),
        server_frame::Msg::ToolCallStarted(ToolCallStarted {
            session_id: "s-01".to_string(),
            call_id: "call-aa".to_string(),
            index: 1,
            name: "memory_search".to_string(),
            arguments_json: String::new(),
        }),
        server_frame::Msg::ToolCallEnded(ToolCallEnded {
            session_id: "s-01".to_string(),
            call_id: "call-aa".to_string(),
            outcome: ToolOutcome::Error as i32,
        }),
    ];

    for arm in arms {
        round_trip(&ServerFrame {
            request_id: 7,
            msg: Some(arm),
        });
    }
}

#[test]
fn unknown_field_is_skipped() {
    let original = message_appended_event();
    let mut bytes = original.encode_to_vec();

    bytes.extend_from_slice(&[0xa2, 0x06, 0x03, 0xde, 0xad, 0xbe]);

    let decoded = Event::decode(bytes.as_slice()).expect("decode with unknown field");
    assert_eq!(decoded, original);
}

#[test]
fn empty_bytes_decode_to_defaults() {
    let decoded = Event::decode(&[][..]).expect("decode empty");

    assert_eq!(decoded, Event::default());
    assert_eq!(decoded.seq, 0);
    assert!(decoded.ts.is_none());
    assert_eq!(decoded.source, Source::Unspecified as i32);
    assert!(decoded.payload.is_none());
}

#[test]
fn a_session_created_from_before_role_project_and_budget_still_decodes() {
    let before = [
        0x0a, 0x04, 0x73, 0x2d, 0x30, 0x31, 0x12, 0x0b, 0x66, 0x69, 0x72, 0x73, 0x74, 0x20, 0x6c,
        0x69, 0x67, 0x68, 0x74, 0x1a, 0x06, 0x67, 0x65, 0x6d, 0x69, 0x6e, 0x69, 0x22, 0x0e, 0x67,
        0x65, 0x6d, 0x69, 0x6e, 0x69, 0x2d, 0x32, 0x2e, 0x35, 0x2d, 0x70, 0x72, 0x6f,
    ];

    let decoded = SessionCreated::decode(&before[..])
        .expect("decode bytes written before the fields existed");

    assert_eq!(decoded.session_id, "s-01");
    assert_eq!(decoded.model, "gemini-2.5-pro");
    assert_eq!(decoded.role, SessionRole::Unspecified as i32);
    assert!(decoded.project.is_empty());
    assert!(decoded.budget.is_none());
}
