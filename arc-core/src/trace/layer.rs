use std::{
    collections::HashMap,
    fmt,
    path::{Path, PathBuf},
    sync::{
        Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};

use arc_proto::perfetto::{
    BuiltinClock, ClockSnapshot, CounterDescriptor, DebugAnnotation, ProcessDescriptor,
    TracePacket, TrackDescriptor, TrackEvent, clock_snapshot, counter_descriptor, track_event,
};
use tracing::{
    Event, Subscriber,
    field::{Field, Visit},
    span::{Attributes, Id, Record},
};
use tracing_subscriber::{Layer, layer::Context, registry::LookupSpan};

use super::writer::PacketWriter;

const COUNTER_PREFIX: &str = "counter.";

const SEQUENCE_ID: u32 = 1;

const CATEGORY: &str = "arc";

pub struct PerfettoLayer {
    writer: Mutex<PacketWriter>,
    broken: AtomicBool,
    next_uuid: AtomicU64,
    process_track: u64,
    sessions: Mutex<HashMap<String, u64>>,
    counters: Mutex<HashMap<String, u64>>,
}

impl PerfettoLayer {
    pub(super) fn create(dir: &Path, process_name: &str) -> std::io::Result<(Self, PathBuf)> {
        let (writer, path) = PacketWriter::create(dir)?;
        let layer = Self {
            writer: Mutex::new(writer),
            broken: AtomicBool::new(false),
            next_uuid: AtomicU64::new(2),
            process_track: 1,
            sessions: Mutex::new(HashMap::new()),
            counters: Mutex::new(HashMap::new()),
        };

        layer.emit(TracePacket {
            clock_snapshot: Some(ClockSnapshot {
                clocks: vec![clock_snapshot::Clock {
                    clock_id: Some(BuiltinClock::Realtime as u32),
                    timestamp: Some(now()),
                }],
                primary_trace_clock: BuiltinClock::Realtime as i32,
            }),
            ..packet()
        });

        layer.emit(TracePacket {
            track_descriptor: Some(TrackDescriptor {
                uuid: Some(layer.process_track),
                name: process_name.to_owned(),
                process: Some(ProcessDescriptor {
                    pid: i32::try_from(std::process::id()).ok(),
                    process_name: process_name.to_owned(),
                }),
                ..TrackDescriptor::default()
            }),
            ..packet()
        });

        Ok((layer, path))
    }

    fn emit(&self, packet: TracePacket) {
        if self.broken.load(Ordering::Relaxed) {
            return;
        }
        let Ok(mut writer) = self.writer.lock() else {
            self.broken.store(true, Ordering::Relaxed);
            return;
        };
        if let Err(error) = writer.write(packet) {
            self.broken.store(true, Ordering::Relaxed);
            eprintln!("perfetto trace stopped: {error}");
        }
    }

    fn uuid(&self) -> u64 {
        self.next_uuid.fetch_add(1, Ordering::Relaxed)
    }

    fn session_track(&self, session_id: &str) -> u64 {
        let Ok(mut sessions) = self.sessions.lock() else {
            return self.process_track;
        };
        if let Some(track) = sessions.get(session_id) {
            return *track;
        }
        let track = self.uuid();
        sessions.insert(session_id.to_owned(), track);
        drop(sessions);

        let short: String = session_id.chars().take(8).collect();
        self.emit(TracePacket {
            track_descriptor: Some(TrackDescriptor {
                uuid: Some(track),
                name: format!("session {short}"),
                parent_uuid: Some(self.process_track),
                ..TrackDescriptor::default()
            }),
            ..packet()
        });
        track
    }

    fn counter_track(&self, name: &str) -> u64 {
        let Ok(mut counters) = self.counters.lock() else {
            return self.process_track;
        };
        if let Some(track) = counters.get(name) {
            return *track;
        }
        let track = self.uuid();
        counters.insert(name.to_owned(), track);
        drop(counters);

        self.emit(TracePacket {
            track_descriptor: Some(TrackDescriptor {
                uuid: Some(track),
                name: name.to_owned(),
                parent_uuid: Some(self.process_track),
                counter: Some(CounterDescriptor {
                    unit: counter_descriptor::Unit::Count as i32,
                    ..CounterDescriptor::default()
                }),
                ..TrackDescriptor::default()
            }),
            ..packet()
        });
        track
    }

    fn emit_counters(&self, fields: &Fields, timestamp: u64) {
        for (name, value) in &fields.counters {
            let track = self.counter_track(name);
            self.emit(TracePacket {
                timestamp: Some(timestamp),
                track_event: Some(TrackEvent {
                    r#type: track_event::Type::Counter as i32,
                    track_uuid: Some(track),
                    double_counter_value: Some(*value),
                    ..TrackEvent::default()
                }),
                ..packet()
            });
        }
    }
}

impl<S> Layer<S> for PerfettoLayer
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, ctx: Context<'_, S>) {
        let Some(span) = ctx.span(id) else { return };
        let mut fields = Fields::default();
        attrs.record(&mut fields);
        span.extensions_mut().insert(SpanData {
            track: self.uuid(),
            started: now(),
            fields,
        });
    }

    fn on_record(&self, id: &Id, values: &Record<'_>, ctx: Context<'_, S>) {
        let Some(span) = ctx.span(id) else { return };
        let mut extensions = span.extensions_mut();
        if let Some(data) = extensions.get_mut::<SpanData>() {
            values.record(&mut data.fields);
        }
    }

    fn on_event(&self, event: &Event<'_>, ctx: Context<'_, S>) {
        let mut fields = Fields::default();
        event.record(&mut fields);

        let track = ctx
            .event_span(event)
            .and_then(|span| span.extensions().get::<SpanData>().map(|data| data.track))
            .unwrap_or(self.process_track);

        let timestamp = now();
        self.emit_counters(&fields, timestamp);

        if fields.annotations.is_empty() && fields.message.is_none() {
            return;
        }

        let name = fields
            .message
            .clone()
            .unwrap_or_else(|| event.metadata().name().to_owned());
        self.emit(TracePacket {
            timestamp: Some(timestamp),
            track_event: Some(TrackEvent {
                r#type: track_event::Type::Instant as i32,
                name,
                track_uuid: Some(track),
                categories: vec![CATEGORY.to_owned()],
                debug_annotations: fields.annotations,
                ..TrackEvent::default()
            }),
            ..packet()
        });
    }

    fn on_close(&self, id: Id, ctx: Context<'_, S>) {
        let Some(span) = ctx.span(&id) else { return };
        let Some(data) = span.extensions_mut().remove::<SpanData>() else {
            return;
        };
        let ended = now();

        let session = data.fields.session.as_deref().filter(|session| {
            !span.scope().skip(1).any(|ancestor| {
                ancestor
                    .extensions()
                    .get::<SpanData>()
                    .and_then(|data| data.fields.session.clone())
                    .is_some_and(|inherited| inherited == *session)
            })
        });
        let parent = match session {
            Some(session) => self.session_track(session),
            None => span
                .parent()
                .and_then(|parent| parent.extensions().get::<SpanData>().map(|data| data.track))
                .unwrap_or(self.process_track),
        };

        self.emit(TracePacket {
            track_descriptor: Some(TrackDescriptor {
                uuid: Some(data.track),
                name: span.name().to_owned(),
                parent_uuid: Some(parent),
                ..TrackDescriptor::default()
            }),
            ..packet()
        });
        self.emit(TracePacket {
            timestamp: Some(data.started),
            track_event: Some(TrackEvent {
                r#type: track_event::Type::SliceBegin as i32,
                name: span.name().to_owned(),
                track_uuid: Some(data.track),
                categories: vec![CATEGORY.to_owned()],
                ..TrackEvent::default()
            }),
            ..packet()
        });
        self.emit_counters(&data.fields, ended);
        self.emit(TracePacket {
            timestamp: Some(ended),
            track_event: Some(TrackEvent {
                r#type: track_event::Type::SliceEnd as i32,
                track_uuid: Some(data.track),
                debug_annotations: data.fields.annotations,
                ..TrackEvent::default()
            }),
            ..packet()
        });
    }
}

struct SpanData {
    track: u64,
    started: u64,
    fields: Fields,
}

#[derive(Default)]
struct Fields {
    annotations: Vec<DebugAnnotation>,
    session: Option<String>,
    counters: Vec<(String, f64)>,
    message: Option<String>,
}

impl Fields {
    fn counter_name(field: &Field) -> Option<&str> {
        field.name().strip_prefix(COUNTER_PREFIX)
    }

    fn annotate(&mut self, field: &Field, annotation: DebugAnnotation) {
        if field.name() == "message" {
            self.message = Some(annotation.string_value.clone());
        }
        self.annotations.push(annotation);
    }
}

impl Visit for Fields {
    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == "session_id" && !value.is_empty() {
            self.session = Some(value.to_owned());
        }
        self.annotate(
            field,
            DebugAnnotation {
                name: field.name().to_owned(),
                string_value: value.to_owned(),
                ..DebugAnnotation::default()
            },
        );
    }

    fn record_i64(&mut self, field: &Field, value: i64) {
        if let Some(name) = Self::counter_name(field) {
            #[allow(clippy::cast_precision_loss)]
            self.counters.push((name.to_owned(), value as f64));
            return;
        }
        self.annotate(
            field,
            DebugAnnotation {
                name: field.name().to_owned(),
                int_value: Some(value),
                ..DebugAnnotation::default()
            },
        );
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        if let Some(name) = Self::counter_name(field) {
            #[allow(clippy::cast_precision_loss)]
            self.counters.push((name.to_owned(), value as f64));
            return;
        }
        self.annotate(
            field,
            DebugAnnotation {
                name: field.name().to_owned(),
                uint_value: Some(value),
                ..DebugAnnotation::default()
            },
        );
    }

    fn record_f64(&mut self, field: &Field, value: f64) {
        if let Some(name) = Self::counter_name(field) {
            self.counters.push((name.to_owned(), value));
            return;
        }
        self.annotate(
            field,
            DebugAnnotation {
                name: field.name().to_owned(),
                double_value: Some(value),
                ..DebugAnnotation::default()
            },
        );
    }

    fn record_bool(&mut self, field: &Field, value: bool) {
        self.annotate(
            field,
            DebugAnnotation {
                name: field.name().to_owned(),
                bool_value: Some(value),
                ..DebugAnnotation::default()
            },
        );
    }

    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        self.record_str(field, &format!("{value:?}"));
    }

    fn record_error(&mut self, field: &Field, value: &(dyn std::error::Error + 'static)) {
        self.record_str(field, &value.to_string());
    }
}

fn packet() -> TracePacket {
    TracePacket {
        timestamp: None,
        timestamp_clock_id: Some(BuiltinClock::Realtime as u32),
        clock_snapshot: None,
        track_event: None,
        track_descriptor: None,
        trusted_packet_sequence_id: Some(SEQUENCE_ID),
    }
}

fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
        .try_into()
        .unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use arc_proto::perfetto::Trace;
    use prost::Message as _;
    use tracing_subscriber::prelude::*;

    use super::*;

    fn capture(work: impl FnOnce()) -> Trace {
        let dir = tempfile::tempdir().expect("a temp dir");
        let (layer, path) = super::super::perfetto(dir.path(), "test").expect("a trace file");
        let subscriber = tracing_subscriber::registry().with(layer);
        tracing::subscriber::with_default(subscriber, work);

        let bytes = std::fs::read(&path).expect("the trace file");
        Trace::decode(bytes.as_slice()).expect("a decodable trace")
    }

    fn tracks(trace: &Trace) -> Vec<(u64, String, Option<u64>)> {
        trace
            .packet
            .iter()
            .filter_map(|packet| packet.track_descriptor.as_ref())
            .map(|track| {
                (
                    track.uuid.unwrap_or_default(),
                    track.name.clone(),
                    track.parent_uuid,
                )
            })
            .collect()
    }

    fn events(trace: &Trace) -> Vec<(i32, String, u64)> {
        trace
            .packet
            .iter()
            .filter_map(|packet| packet.track_event.as_ref())
            .map(|event| {
                (
                    event.r#type,
                    event.name.clone(),
                    event.track_uuid.unwrap_or_default(),
                )
            })
            .collect()
    }

    fn track_named(trace: &Trace, name: &str) -> (u64, Option<u64>) {
        let (uuid, _, parent) = tracks(trace)
            .into_iter()
            .find(|(_, track, _)| track == name)
            .unwrap_or_else(|| panic!("a track named {name}, in {:?}", tracks(trace)));
        (uuid, parent)
    }

    #[test]
    fn a_span_becomes_a_slice_on_its_own_track() {
        let trace = capture(|| {
            tracing::info_span!("work").in_scope(|| {});
        });

        let (work, parent) = track_named(&trace, "work");
        assert_eq!(
            parent,
            Some(1),
            "spans with no session hang off the process"
        );

        let slices: Vec<_> = events(&trace)
            .into_iter()
            .filter(|(_, _, on)| *on == work)
            .collect();
        assert_eq!(
            slices,
            vec![
                (
                    track_event::Type::SliceBegin as i32,
                    "work".to_owned(),
                    work
                ),
                (track_event::Type::SliceEnd as i32, String::new(), work),
            ]
        );

        let (begin, end) = (
            trace.packet.iter().find(|p| {
                p.track_event.as_ref().is_some_and(|e| {
                    e.r#type == track_event::Type::SliceBegin as i32 && e.track_uuid == Some(work)
                })
            }),
            trace.packet.iter().find(|p| {
                p.track_event.as_ref().is_some_and(|e| {
                    e.r#type == track_event::Type::SliceEnd as i32 && e.track_uuid == Some(work)
                })
            }),
        );
        assert!(
            begin.and_then(|p| p.timestamp) <= end.and_then(|p| p.timestamp),
            "a slice ends no earlier than it begins"
        );
    }

    #[test]
    fn nested_spans_nest_under_the_session_that_named_them() {
        let trace = capture(|| {
            let outer = tracing::info_span!("outer", session_id = tracing::field::Empty);
            let entered = outer.enter();
            outer.record("session_id", "abcdef0123456789");
            tracing::info_span!("inner").in_scope(|| {});
            drop(entered);
        });

        let (session, session_parent) = track_named(&trace, "session abcdef01");
        assert_eq!(session_parent, Some(1), "sessions hang off the process");

        let (outer, outer_parent) = track_named(&trace, "outer");
        assert_eq!(outer_parent, Some(session));

        let (_, inner_parent) = track_named(&trace, "inner");
        assert_eq!(inner_parent, Some(outer), "the span tree is the track tree");
    }

    #[test]
    fn a_session_opens_one_row_however_deep_the_span_that_names_it() {
        let trace = capture(|| {
            tracing::info_span!("client connected").in_scope(|| {
                tracing::info_span!("server.request").in_scope(|| {
                    tracing::info_span!("session.send_message", session_id = "abcdef0123456789")
                        .in_scope(|| {
                            tracing::info_span!("openai.complete", session_id = "abcdef0123456789")
                                .in_scope(|| {});
                        });
                });
            });
        });

        let (session, _) = track_named(&trace, "session abcdef01");
        let (send, send_parent) = track_named(&trace, "session.send_message");
        assert_eq!(
            send_parent,
            Some(session),
            "the turn hangs off the session, not off the connection"
        );

        let (_, complete_parent) = track_named(&trace, "openai.complete");
        assert_eq!(
            complete_parent,
            Some(send),
            "a span already inside that session stays nested"
        );
    }

    #[test]
    fn an_event_lands_on_its_span_and_carries_its_fields() {
        let trace = capture(|| {
            tracing::info_span!("work").in_scope(|| {
                tracing::info!(outcome = "done", tokens = 7, "completion finished");
            });
        });

        let (work, _) = track_named(&trace, "work");
        let instant = trace
            .packet
            .iter()
            .filter_map(|packet| packet.track_event.as_ref())
            .find(|event| event.r#type == track_event::Type::Instant as i32)
            .expect("an instant");

        assert_eq!(instant.name, "completion finished");
        assert_eq!(instant.track_uuid, Some(work), "inside the span's slice");

        let annotations: Vec<_> = instant
            .debug_annotations
            .iter()
            .map(|annotation| (annotation.name.as_str(), annotation.int_value))
            .collect();
        assert!(annotations.contains(&("tokens", Some(7))));
        assert!(annotations.contains(&("outcome", None)), "a string field");
    }

    #[test]
    fn counter_fields_become_samples_on_their_own_track() {
        let trace = capture(|| {
            tracing::info_span!("work").in_scope(|| {
                tracing::info!(counter.output_tokens = 42_u64, "completion finished");
            });
        });

        let (tokens, parent) = track_named(&trace, "output_tokens");
        assert_eq!(parent, Some(1), "counters are the process's, not a span's");

        let counter = trace
            .packet
            .iter()
            .filter_map(|packet| packet.track_event.as_ref())
            .find(|event| event.r#type == track_event::Type::Counter as i32)
            .expect("a counter sample");
        assert_eq!(counter.track_uuid, Some(tokens));
        assert_eq!(counter.double_counter_value, Some(42.0));

        let instant = trace
            .packet
            .iter()
            .filter_map(|packet| packet.track_event.as_ref())
            .find(|event| event.r#type == track_event::Type::Instant as i32)
            .expect("the event itself is still an instant");
        assert!(
            instant
                .debug_annotations
                .iter()
                .all(|annotation| annotation.name != "counter.output_tokens"),
            "a counter is drawn once, as a counter"
        );
    }

    #[test]
    fn a_counter_only_event_draws_no_instant() {
        let trace = capture(|| {
            tracing::info!(counter.queue_depth = 3_u64);
        });

        assert!(
            !events(&trace)
                .iter()
                .any(|(kind, _, _)| *kind == track_event::Type::Instant as i32),
            "nothing to say, so nothing is drawn"
        );
    }
}
