//! `arcd run` — startup composition and lifecycle.
//!
//! [`Daemon::start`] is the whole of the daemon's wiring: open the log, catch
//! the index up to it, close orphaned tool calls, and hand back the session
//! engine over all of it. [`Daemon::serve`] is the lifecycle: bind the socket,
//! announce readiness, serve until a shutdown signal, stop.
//!
//! Nothing here decides anything — every rule lives in `arc-core` (DESIGN.md
//! §2). What this file owns is the order things happen in, and the property
//! that all of it happens before the daemon claims to be ready. A half-started
//! daemon is worse than one that refused to start, so any failure below comes
//! straight back out to `main`.
//!
//! Startup I/O is synchronous — the log and projection layers are deliberately
//! blocking — and runs before anything is being served, so it does not matter
//! that it holds the runtime thread.

use anyhow::{Context as _, Result};
use arc_core::archive::Archive;
use arc_core::consolidation::extract::{ModelExtractor, PROMPT_VERSION_V1};
use arc_core::consolidation::{self, Extractor};
use arc_core::log::Log;
use arc_core::orphan;
use arc_core::projection::{self, Projection};
use arc_core::provider::Provider;
use arc_core::provider::openai::OpenAiCompat;
use arc_core::session::Engine;
use arc_core::tool::Registry;
use arc_core::tool::memory::{MemoryRead, MemorySearch, MemorySupersede, MemoryWrite};
use arc_core::tool::sessions::{SessionRead, SessionsSearch};
use arc_core::tool::time::GetTime;
use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::net::TcpListener;
use tokio::signal::unix::{SignalKind, signal};
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tracing::{error, info, warn};

use crate::config::{Config, ConsolidationConfig, ProviderChoice};
use crate::dirs::DataDirs;
use crate::identity;
use crate::llama::Sidecar;
use crate::server;

/// Builds the configured provider, then runs the daemon over it.
///
/// This match is the one place `provider = "..."` becomes a type. The daemon
/// stays generic over [`Provider`] below it; the trait's dyn-compatibility
/// question (see `arc-core::provider`) stays open, because a per-daemon
/// choice needs only this dispatch — per-completion choice is Phase 3's
/// problem.
///
/// For the local provider, the sidecar starts before the log is touched —
/// the daemon's slowest dependency goes first — and is killed after the
/// server stops, whatever way it stops.
///
/// # Errors
///
/// Whatever [`Sidecar::start`], [`Daemon::start`], or [`Daemon::serve`]
/// refused on.
pub async fn run(config: Config, dirs: DataDirs) -> Result<()> {
    match config.provider {
        ProviderChoice::Local => {
            let sidecar = Sidecar::start(&config.llama, &config.model()).await?;
            let provider = OpenAiCompat::new(sidecar.endpoint());
            let served = match Daemon::start(config, dirs, provider) {
                Ok(daemon) => daemon.serve().await,
                Err(error) => Err(error),
            };
            // Both arms end here: a daemon that failed to start must not
            // leave a model server holding the GPU.
            sidecar.stop().await;
            served
        }
    }
}

/// A started daemon: everything durable is open, nothing is being served yet.
pub struct Daemon<P: Provider> {
    config: Config,
    dirs: DataDirs,

    /// The session engine, holding the log, the index, the provider, and the
    /// identity file.
    ///
    /// The mutex serializes completions daemon-wide: `send_message` takes
    /// `&mut Engine`, so one turn runs at a time and everything else waits.
    /// That is Phase 1's single-user reality (DESIGN.md §1), stated once here
    /// rather than worked around in the server.
    engine: Arc<Mutex<Engine<P>>>,

    /// The engine's provider, cloned to the consolidation extractor: both
    /// run on the same sidecar (banked: consolidation uses the same model).
    provider: Arc<P>,
}

impl<P: Provider + 'static> Daemon<P> {
    /// Opens everything the daemon needs, in dependency order.
    ///
    /// # Errors
    ///
    /// If the directories cannot be created, the log cannot be opened or
    /// recovered, or the index cannot be opened or replayed. All of these mean
    /// durable state is not in a known condition, which is a refusal to start.
    #[tracing::instrument(name = "daemon.start", skip_all, fields(data_dir = %dirs.root().display()))]
    pub fn start(config: Config, dirs: DataDirs, provider: P) -> Result<Self> {
        dirs.create()
            .with_context(|| format!("preparing {}", dirs.root().display()))?;

        // Recovery happens inside `open`: a torn tail is sealed, never
        // truncated, and the append point comes back with it (DESIGN.md §3).
        let mut log = Log::open(dirs.log())
            .with_context(|| format!("opening the event log at {}", dirs.log().display()))?;
        info!(
            next_seq = log.next_seq(),
            segment = %log.current_segment().display(),
            "event log ready"
        );

        let mut projection = open_index(dirs.index())?;
        let reader = log.reader().context("listing log segments for replay")?;
        let stats = projection::replay(reader, &mut projection)
            .context("replaying the event log into the index")?;
        info!(
            applied = stats.applied,
            skipped = stats.skipped,
            "index caught up with the log"
        );

        // The resume contract: a durable call with no durable
        // result is closed as UNKNOWN before anything is served, so the log
        // the engine works over has an answer for every call.
        let reader = log
            .reader()
            .context("listing log segments for the orphan scan")?;
        let closed = orphan::close_orphans(reader, &mut log, &mut projection)
            .context("closing orphaned tool calls")?;
        for orphan in &closed {
            info!(
                session_id = %orphan.session_id,
                call_id = %orphan.call_id,
                "closed an orphaned tool call as UNKNOWN"
            );
        }

        let identity = identity::load(dirs.identity()).context("loading the identity file")?;
        if let Some(text) = &identity {
            info!(chars = text.len(), "identity file loaded");
        } else {
            info!("no identity file, running without one");
        }

        let mut registry = Registry::new(config.max_tool_result_bytes);
        registry.register(Box::new(GetTime));
        // The archive tools own read-only connections to the index the
        // projection above just caught up; they never touch its writer.
        let open_archive = |tool: &str| {
            Archive::open(dirs.index())
                .with_context(|| format!("opening the index read-only for {tool}"))
        };
        registry.register(Box::new(SessionsSearch::new(open_archive(
            "sessions_search",
        )?)));
        registry.register(Box::new(SessionRead::new(open_archive("session_read")?)));
        registry.register(Box::new(MemoryRead::new(open_archive("memory_read")?)));
        registry.register(Box::new(MemorySearch::new(open_archive("memory_search")?)));
        registry.register(Box::new(MemoryWrite));
        registry.register(Box::new(MemorySupersede::new(open_archive(
            "memory_supersede",
        )?)));

        let provider = Arc::new(provider);
        let engine = Engine::new(
            log,
            projection,
            Arc::clone(&provider),
            config.model(),
            identity,
            registry,
            config.no_think,
        );

        Ok(Self {
            config,
            dirs,
            engine: Arc::new(Mutex::new(engine)),
            provider,
        })
    }

    /// Binds the socket, announces readiness, and serves until a shutdown
    /// signal.
    ///
    /// # Errors
    ///
    /// If `bind` is already taken or cannot be bound. Nothing has been served
    /// at that point, so it comes straight back out like any startup failure.
    pub async fn serve(self) -> Result<()> {
        let listener = TcpListener::bind(self.config.bind)
            .await
            .with_context(|| format!("binding {}", self.config.bind))?;
        // Port 0 in the config is a real answer, so report what was bound
        // rather than what was asked for.
        let bound = listener.local_addr().unwrap_or(self.config.bind);

        info!(
            model = self.config.model(),
            provider = ?self.config.provider,
            bind = %bound,
            data_dir = %self.dirs.root().display(),
            version = env!("CARGO_PKG_VERSION"),
            "arcd ready"
        );

        let consolidation = consolidation_task(
            self.config.consolidation,
            self.config.model(),
            Arc::clone(&self.engine),
            Arc::clone(&self.provider),
        );

        server::serve(listener, self.engine, shutdown()).await;

        // The tick dies with the server: a pass mid-extraction is abandoned,
        // which loses nothing durable — it had appended nothing yet.
        if let Some(task) = consolidation {
            task.abort();
        }

        info!("stopped");
        Ok(())
    }
}

/// How often the daemon looks for an idle session to consolidate.
const CONSOLIDATION_TICK: Duration = Duration::from_secs(60);

/// Spawns the consolidation tick, or nothing while the config keeps it off.
///
/// One pass per tick, one session per pass — the concurrency bound is one.
/// A failed pass logs and yields; it never wedges the daemon (DESIGN.md
/// §5.4, docs/prior-art-hermes.md §3).
fn consolidation_task<P: Provider + 'static>(
    config: ConsolidationConfig,
    model: String,
    engine: Arc<Mutex<Engine<P>>>,
    provider: Arc<P>,
) -> Option<JoinHandle<()>> {
    if !config.enabled {
        info!("consolidation disabled");
        return None;
    }
    let idle = Duration::from_secs(config.idle_seconds);
    let extractor =
        ModelExtractor::new(provider, model, Duration::from_secs(config.timeout_seconds));
    info!(
        idle_seconds = config.idle_seconds,
        timeout_seconds = config.timeout_seconds,
        prompt_version = PROMPT_VERSION_V1,
        "consolidation enabled"
    );
    Some(tokio::spawn(async move {
        let mut tick = tokio::time::interval(CONSOLIDATION_TICK);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut strikes = Strikes::default();
        loop {
            tick.tick().await;
            let Some(cutoff) = idle_cutoff_micros(idle) else {
                warn!("no usable clock reading; skipping this consolidation tick");
                continue;
            };
            tick_once(&engine, &extractor, cutoff, &mut strikes).await;
        }
    }))
}

/// One tick: one pass over the first due session outside the strike list,
/// plus the strikes bookkeeping. Outcomes land on the pass's own span; only
/// failure needs log lines here.
async fn tick_once<P: Provider, E: Extractor>(
    engine: &Mutex<Engine<P>>,
    extractor: &E,
    cutoff: i64,
    strikes: &mut Strikes,
) {
    let pass =
        consolidation::run_pass(engine, extractor, cutoff, PROMPT_VERSION_V1, strikes.skip());
    match pass.await {
        Ok(consolidation::Outcome::Consolidated { session_id, .. }) => {
            strikes.succeeded(&session_id);
        }
        Ok(_) => {}
        Err(consolidation::Error::Extractor { session_id, source }) => {
            warn!(%session_id, error = %source, "extraction failed; yielding until the next tick");
            if strikes.strike(session_id.clone()) {
                warn!(
                    %session_id,
                    limit = STRIKE_LIMIT,
                    "session hit the strike limit; skipping it until the daemon restarts"
                );
            }
        }
        Err(error) => warn!(%error, "consolidation pass failed; yielding until the next tick"),
    }
}

/// Extraction failures at [`STRIKE_LIMIT`] park the session (hermes §3:
/// three strikes, then skip), so one forever-failing session cannot wedge
/// the queue. In-process on purpose — a restart forgets the map, and
/// retrying then is harmless.
const STRIKE_LIMIT: u32 = 3;

#[derive(Default)]
struct Strikes {
    failures: HashMap<String, u32>,
    skip: HashSet<String>,
}

impl Strikes {
    fn skip(&self) -> &HashSet<String> {
        &self.skip
    }

    /// Counts one failure; true exactly when this one crossed the limit,
    /// so the caller logs the skip loudly once.
    fn strike(&mut self, session_id: String) -> bool {
        let count = self.failures.entry(session_id.clone()).or_insert(0);
        *count += 1;
        *count >= STRIKE_LIMIT && self.skip.insert(session_id)
    }

    fn succeeded(&mut self, session_id: &str) {
        self.failures.remove(session_id);
    }
}

/// Now minus the idle window, in the projection's epoch-microseconds unit.
fn idle_cutoff_micros(idle: Duration) -> Option<i64> {
    let now = SystemTime::now().duration_since(UNIX_EPOCH).ok()?;
    let now = i64::try_from(now.as_micros()).ok()?;
    let idle = i64::try_from(idle.as_micros()).unwrap_or(i64::MAX);
    Some(now.saturating_sub(idle))
}

/// Opens the index, deleting and recreating it when it was written by another
/// schema version.
///
/// The index is documented disposable (DESIGN.md §5.3): a version bump is a
/// planned rebuild — the replay in [`Daemon::start`] refills the fresh file
/// from the log — not a failure, and refusing to start over one would be
/// friction. Any other open error still refuses: those mean durable state is
/// not in a known condition.
fn open_index(path: &Path) -> Result<Projection> {
    let opening = || format!("opening the index at {}", path.display());
    match Projection::open(path) {
        Ok(projection) => Ok(projection),
        Err(projection::Error::SchemaVersion {
            found, expected, ..
        }) => {
            warn!(
                found,
                expected,
                path = %path.display(),
                "index written by another schema version; deleting it to rebuild from the log"
            );
            // The WAL siblings go too: a stale journal must not outlive the
            // database it belonged to.
            for suffix in ["", "-wal", "-shm"] {
                let mut file = path.as_os_str().to_owned();
                file.push(suffix);
                if let Err(error) = std::fs::remove_file(Path::new(&file)) {
                    if error.kind() != std::io::ErrorKind::NotFound {
                        return Err(error)
                            .with_context(|| format!("deleting stale index file {file:?}"));
                    }
                }
            }
            Projection::open(path).with_context(opening)
        }
        Err(error) => Err(error).with_context(opening),
    }
}

/// Resolves when the daemon should stop: Ctrl-C, or `SIGTERM`.
///
/// `SIGTERM` is what a service manager sends, and it is not optional here —
/// dying on the default handler skips the shutdown path, and the llama-server
/// sidecar is orphaned holding the model's several GiB of VRAM.
///
/// A signal handler that cannot be installed counts as a stop: a daemon no one
/// can shut down is worse than one that refuses to run, and it fails loudly at
/// startup rather than quietly at the end.
async fn shutdown() {
    let mut terminate = match signal(SignalKind::terminate()) {
        Ok(terminate) => terminate,
        Err(error) => {
            error!(%error, "the SIGTERM handler could not be installed; stopping");
            return;
        }
    };

    tokio::select! {
        result = tokio::signal::ctrl_c() => match result {
            Ok(()) => info!("interrupted"),
            Err(error) => error!(%error, "the shutdown signal handler failed; stopping"),
        },
        _ = terminate.recv() => info!("terminated"),
    }
}

#[cfg(test)]
mod tests {
    use arc_core::provider::{CompletionRequest, CompletionStream, Error as ProviderError};
    use arc_proto::v1::{Event, SessionCreated, SessionEvent, event, session_event};
    use tempfile::TempDir;

    use super::*;
    use crate::dirs::DataDirs;

    /// Startup opens durable state; it must never need the model.
    struct NeverCalled;

    impl Provider for NeverCalled {
        fn name(&self) -> &'static str {
            "never"
        }

        async fn complete(
            &self,
            _request: CompletionRequest,
        ) -> Result<CompletionStream, ProviderError> {
            panic!("startup must not call the provider")
        }
    }

    /// The index is disposable by contract: an index stamped with another
    /// schema version is deleted and rebuilt from the log, and the daemon
    /// starts. Any other open failure still refuses — that path stays as
    /// `open_index`'s error arm.
    #[tokio::test]
    async fn a_foreign_schema_version_index_is_deleted_and_rebuilt() {
        let temp = TempDir::new().expect("temp dir");
        let dirs = DataDirs::new(temp.path().join("data"));
        dirs.create().expect("create dirs");

        // A log with one session in it...
        let mut log = Log::open(dirs.log()).expect("open log");
        log.append(Event {
            seq: 0, // added by the log
            ts: None,
            source: arc_proto::v1::Source::User as i32,
            payload: Some(event::Payload::Session(SessionEvent {
                event: Some(session_event::Event::SessionCreated(SessionCreated {
                    session_id: "s-01".to_owned(),
                    title: String::new(),
                    provider: "never".to_owned(),
                    model: "test-model".to_owned(),
                })),
            })),
        })
        .expect("append");
        drop(log);

        // ...and an index a previous build left at schema version 1.
        drop(Projection::open(dirs.index()).expect("create index"));
        let conn = rusqlite::Connection::open(dirs.index()).expect("open raw");
        conn.execute(
            "UPDATE projection_meta SET value = 1 WHERE key = 'schema_version'",
            [],
        )
        .expect("age the version");
        drop(conn);

        let daemon =
            Daemon::start(Config::default(), dirs, NeverCalled).expect("start over a stale index");

        // The rebuilt index was refilled from the log before serving.
        let sessions = daemon.engine.lock().await.sessions().expect("sessions");
        assert_eq!(sessions.len(), 1);
        assert_eq!(sessions[0].id, "s-01");
    }

    /// The config gate: the default (disabled) config spawns no tick at all,
    /// and an enabled one does. The pass itself is arc-core's to test.
    #[tokio::test]
    async fn the_consolidation_tick_only_runs_when_enabled() {
        let temp = TempDir::new().expect("temp dir");
        let dirs = DataDirs::new(temp.path().join("data"));
        let daemon = Daemon::start(Config::default(), dirs, NeverCalled).expect("start");

        assert!(
            consolidation_task(
                Config::default().consolidation,
                "test-model".to_owned(),
                Arc::clone(&daemon.engine),
                Arc::clone(&daemon.provider),
            )
            .is_none(),
            "disabled by default: no task, so a tick can do nothing"
        );

        let enabled = ConsolidationConfig {
            enabled: true,
            idle_seconds: 1800,
            timeout_seconds: 300,
        };
        let task = consolidation_task(
            enabled,
            "test-model".to_owned(),
            Arc::clone(&daemon.engine),
            Arc::clone(&daemon.provider),
        )
        .expect("enabled spawns the tick");
        task.abort();
    }

    /// An extractor that fails every session and records which it saw.
    struct AlwaysFailing(std::sync::Mutex<Vec<String>>);

    impl arc_core::consolidation::Extractor for AlwaysFailing {
        async fn extract(
            &self,
            session: &arc_core::consolidation::SessionSnapshot,
        ) -> Result<Vec<arc_proto::v1::memory_event::Event>, arc_core::consolidation::ExtractError>
        {
            self.0
                .lock()
                .expect("seen")
                .push(session.session_id.clone());
            Err(arc_core::consolidation::ExtractError(
                "scripted failure".to_owned(),
            ))
        }
    }

    /// Seeds one session with one timestamped user message, so it reads as
    /// idle since `at_micros`.
    fn seed_idle_session(log: &mut Log, session_id: &str, at_micros: i64) {
        use arc_proto::v1::{MessageAppended, Role, Source};
        let events = [
            session_event::Event::SessionCreated(SessionCreated {
                session_id: session_id.to_owned(),
                title: String::new(),
                provider: "never".to_owned(),
                model: "test-model".to_owned(),
            }),
            session_event::Event::MessageAppended(MessageAppended {
                session_id: session_id.to_owned(),
                role: Role::User as i32,
                content: "hello".to_owned(),
                partial: false,
                turn_id: format!("{session_id}-t1"),
            }),
        ];
        for event in events {
            log.append(Event {
                seq: 0, // added by the log
                ts: Some(prost_types::Timestamp {
                    seconds: at_micros / 1_000_000,
                    nanos: i32::try_from(at_micros % 1_000_000).expect("micros") * 1_000,
                }),
                source: Source::User as i32,
                payload: Some(event::Payload::Session(SessionEvent { event: Some(event) })),
            })
            .expect("append");
        }
    }

    /// The three-strikes path end to end: a session whose extraction fails
    /// three ticks is skipped with the strike map, and the next due session
    /// gets the slot from then on.
    #[tokio::test]
    async fn three_strikes_skip_the_session_and_the_next_due_proceeds() {
        let temp = TempDir::new().expect("temp dir");
        let dirs = DataDirs::new(temp.path().join("data"));
        dirs.create().expect("create dirs");
        let mut log = Log::open(dirs.log()).expect("open log");
        // s-a idles longer than s-b, so it is first in the due order.
        seed_idle_session(&mut log, "s-a", 1_000_000);
        seed_idle_session(&mut log, "s-b", 2_000_000);
        drop(log);
        let daemon = Daemon::start(Config::default(), dirs, NeverCalled).expect("start");

        let extractor = AlwaysFailing(std::sync::Mutex::new(Vec::new()));
        let mut strikes = Strikes::default();
        for _ in 0..4 {
            tick_once(&daemon.engine, &extractor, i64::MAX, &mut strikes).await;
        }

        let seen = extractor.0.lock().expect("seen").clone();
        assert_eq!(
            seen,
            ["s-a", "s-a", "s-a", "s-b"],
            "three failures park s-a; the fourth tick moves on to s-b"
        );
        assert!(strikes.skip().contains("s-a"));
        assert!(!strikes.skip().contains("s-b"), "s-b has strikes to spare");
    }

    /// The loud-once contract: only the strike that crosses the limit
    /// reports true, and a success wipes the count clean.
    #[test]
    fn strikes_report_the_crossing_once_and_reset_on_success() {
        let mut strikes = Strikes::default();
        assert!(!strikes.strike("s-x".to_owned()));
        assert!(!strikes.strike("s-x".to_owned()));
        strikes.succeeded("s-x");
        assert!(!strikes.strike("s-x".to_owned()), "the count restarted");
        assert!(!strikes.strike("s-x".to_owned()));
        assert!(strikes.strike("s-x".to_owned()), "the third in a row skips");
        assert!(
            !strikes.strike("s-x".to_owned()),
            "already skipped: never loud twice"
        );
    }
}
