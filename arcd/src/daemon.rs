use anyhow::{Context as _, Result};
use arc_core::archive::Archive;
use arc_core::consolidation::extract::{ModelExtractor, PROMPT_VERSION_V1};
use arc_core::consolidation::{self, Extractor};
use arc_core::log::Log;
use arc_core::orphan;
use arc_core::projection::{self, Projection, Reader};
use arc_core::provider::any::AnyProvider;
use arc_core::provider::{Provider, Thinking, role_label};
use arc_core::secrets::Secrets;
use arc_core::session::Engine;
use arc_core::store::Store;
use arc_core::tool::Registry;
use arc_core::tool::memory::{MemoryRead, MemorySearch, MemorySupersede, MemoryWrite};
use arc_core::tool::sessions::{SessionRead, SessionsSearch};
use arc_core::tool::time::GetTime;
use arc_proto::v1::SessionRole;
use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::net::TcpListener;
use tokio::signal::unix::{SignalKind, signal};
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tracing::{error, info, warn};

use crate::config::{Config, ConsolidationConfig};
use crate::dirs::DataDirs;
use crate::identity;
use crate::llama::Sidecar;
use crate::roles::Roles;
use crate::server;

pub async fn run(config: Config, dirs: DataDirs) -> Result<()> {
    let sidecar = Sidecar::start(&config.llama, &config.model()).await?;
    let secrets = Secrets::new(dirs.secrets());
    let roles = match Roles::resolve(&config, sidecar.endpoint(), &secrets) {
        Ok(roles) => roles,
        Err(error) => {
            sidecar.stop().await;
            return Err(error);
        }
    };
    let served = match Daemon::start(config, dirs, roles) {
        Ok(daemon) => daemon.serve().await,
        Err(error) => Err(error),
    };
    sidecar.stop().await;
    served
}

pub struct Daemon {
    config: Config,
    dirs: DataDirs,

    engine: Arc<Mutex<Engine<AnyProvider>>>,

    reads: Arc<Reader>,

    roles: Roles,
}

impl Daemon {
    #[tracing::instrument(name = "daemon.start", skip_all, fields(data_dir = %dirs.root().display()))]
    pub fn start(config: Config, dirs: DataDirs, roles: Roles) -> Result<Self> {
        dirs.create()
            .with_context(|| format!("preparing {}", dirs.root().display()))?;

        let log = Log::open(dirs.log())
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

        let mut store = Store::new(log, projection);
        let reader = store
            .reader()
            .context("listing log segments for the orphan scan")?;
        let closed =
            orphan::close_orphans(reader, &mut store).context("closing orphaned tool calls")?;
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
        let archive = Arc::new(
            Archive::open(dirs.index())
                .with_context(|| format!("opening {} read-only", dirs.index().display()))?,
        );
        registry.register(Box::new(SessionsSearch::new(Arc::clone(&archive))));
        registry.register(Box::new(SessionRead::new(Arc::clone(&archive))));
        registry.register(Box::new(MemoryRead::new(Arc::clone(&archive))));
        registry.register(Box::new(MemorySearch::new(Arc::clone(&archive))));
        registry.register(Box::new(MemoryWrite));
        registry.register(Box::new(MemorySupersede::new(archive)));

        let reads = Arc::new(
            Reader::open(dirs.index())
                .with_context(|| format!("opening {} for reads", dirs.index().display()))?,
        );

        let concierge = roles.concierge();
        let engine = Engine::new(
            store,
            Arc::clone(&concierge.provider),
            &concierge.model,
            SessionRole::Concierge,
            identity,
            registry,
            concierge.thinking,
        );

        Ok(Self {
            config,
            dirs,
            engine: Arc::new(Mutex::new(engine)),
            reads,
            roles,
        })
    }

    pub async fn serve(self) -> Result<()> {
        let listener = TcpListener::bind(self.config.bind)
            .await
            .with_context(|| format!("binding {}", self.config.bind))?;
        let bound = listener.local_addr().unwrap_or(self.config.bind);

        for (role, resolved) in [
            (SessionRole::Concierge, self.roles.concierge()),
            (SessionRole::Executor, self.roles.executor()),
            (SessionRole::Archivist, self.roles.archivist()),
        ] {
            info!(
                role = role_label(role),
                provider = resolved.provider.name(),
                model = resolved.model,
                thinking = resolved.thinking.label(),
                endpoint = resolved.provider.endpoint(),
                "role resolved"
            );
        }
        info!(
            bind = %bound,
            data_dir = %self.dirs.root().display(),
            version = env!("CARGO_PKG_VERSION"),
            "arcd ready"
        );

        let archivist = self.roles.archivist();
        let consolidation = consolidation_task(
            self.config.consolidation,
            &archivist.model,
            archivist.thinking,
            Arc::clone(&self.engine),
            Arc::clone(&archivist.provider),
        );

        server::serve(listener, self.engine, self.reads, shutdown()).await;

        if let Some(task) = consolidation {
            task.abort();
        }

        info!("stopped");
        Ok(())
    }
}

const CONSOLIDATION_TICK: Duration = Duration::from_secs(60);

fn consolidation_task<P: Provider + 'static>(
    config: ConsolidationConfig,
    model: &str,
    thinking: Thinking,
    engine: Arc<Mutex<Engine<P>>>,
    provider: Arc<P>,
) -> Option<JoinHandle<()>> {
    if !config.enabled {
        info!("consolidation disabled");
        return None;
    }
    let idle = Duration::from_secs(config.idle_seconds);
    let extractor = ModelExtractor::new(
        provider,
        model,
        thinking,
        Duration::from_secs(config.timeout_seconds),
    );
    info!(
        idle_seconds = config.idle_seconds,
        timeout_seconds = config.timeout_seconds,
        prompt_version = PROMPT_VERSION_V1,
        "consolidation enabled"
    );
    Some(tokio::spawn(async move {
        let mut tick = tokio::time::interval(CONSOLIDATION_TICK);
        // a slow pass must not make the missed ticks fire back to back
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

    fn strike(&mut self, session_id: String) -> bool {
        let count = self.failures.entry(session_id.clone()).or_insert(0);
        *count += 1;
        *count >= STRIKE_LIMIT && self.skip.insert(session_id)
    }

    fn succeeded(&mut self, session_id: &str) {
        self.failures.remove(session_id);
    }
}

fn idle_cutoff_micros(idle: Duration) -> Option<i64> {
    let now = SystemTime::now().duration_since(UNIX_EPOCH).ok()?;
    let now = i64::try_from(now.as_micros()).ok()?;
    let idle = i64::try_from(idle.as_micros()).unwrap_or(i64::MAX);
    Some(now.saturating_sub(idle))
}

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
    use arc_proto::v1::{Event, SessionCreated, SessionEvent, event, session_event};
    use tempfile::TempDir;

    use super::*;
    use crate::dirs::DataDirs;

    // port 1 refuses every connection: startup must not reach a provider
    fn unreachable_roles() -> Roles {
        Roles::resolve(
            &Config::default(),
            "http://127.0.0.1:1",
            &Secrets::new(Path::new("/nonexistent")),
        )
        .expect("no roles configured")
    }

    #[tokio::test]
    async fn a_foreign_schema_version_index_is_deleted_and_rebuilt() {
        let temp = TempDir::new().expect("temp dir");
        let dirs = DataDirs::new(&temp.path().join("data"));
        dirs.create().expect("create dirs");

        let mut log = Log::open(dirs.log()).expect("open log");
        log.append(Event {
            seq: 0,
            ts: None,
            source: arc_proto::v1::Source::User as i32,
            payload: Some(event::Payload::Session(SessionEvent {
                event: Some(session_event::Event::SessionCreated(SessionCreated {
                    session_id: "s-01".to_owned(),
                    title: String::new(),
                    provider: "never".to_owned(),
                    model: "test-model".to_owned(),
                    role: arc_proto::v1::SessionRole::Unspecified as i32,
                    project: String::new(),
                    budget: None,
                })),
            })),
        })
        .expect("append");
        drop(log);

        drop(Projection::open(dirs.index()).expect("create index"));
        let conn = rusqlite::Connection::open(dirs.index()).expect("open raw");
        conn.execute(
            "UPDATE projection_meta SET value = 1 WHERE key = 'schema_version'",
            [],
        )
        .expect("age the version");
        drop(conn);

        let daemon = Daemon::start(Config::default(), dirs, unreachable_roles())
            .expect("start over a stale index");

        let sessions = daemon.engine.lock().await.sessions().expect("sessions");
        assert_eq!(sessions.len(), 1);
        assert_eq!(sessions[0].id, "s-01");
    }

    #[tokio::test]
    async fn the_consolidation_tick_only_runs_when_enabled() {
        let temp = TempDir::new().expect("temp dir");
        let dirs = DataDirs::new(&temp.path().join("data"));
        let daemon = Daemon::start(Config::default(), dirs, unreachable_roles()).expect("start");

        assert!(
            consolidation_task(
                Config::default().consolidation,
                "test-model",
                Thinking::Minimal,
                Arc::clone(&daemon.engine),
                Arc::clone(&daemon.roles.archivist().provider),
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
            "test-model",
            Thinking::Minimal,
            Arc::clone(&daemon.engine),
            Arc::clone(&daemon.roles.archivist().provider),
        )
        .expect("enabled spawns the tick");
        task.abort();
    }

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

    fn seed_idle_session(log: &mut Log, session_id: &str, at_micros: i64) {
        use arc_proto::v1::{MessageAppended, Role, Source};
        let events = [
            session_event::Event::SessionCreated(SessionCreated {
                session_id: session_id.to_owned(),
                title: String::new(),
                provider: "never".to_owned(),
                model: "test-model".to_owned(),
                role: arc_proto::v1::SessionRole::Unspecified as i32,
                project: String::new(),
                budget: None,
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
                seq: 0,
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

    #[tokio::test]
    async fn three_strikes_skip_the_session_and_the_next_due_proceeds() {
        let temp = TempDir::new().expect("temp dir");
        let dirs = DataDirs::new(&temp.path().join("data"));
        dirs.create().expect("create dirs");
        let mut log = Log::open(dirs.log()).expect("open log");
        seed_idle_session(&mut log, "s-a", 1_000_000);
        seed_idle_session(&mut log, "s-b", 2_000_000);
        drop(log);
        let daemon = Daemon::start(Config::default(), dirs, unreachable_roles()).expect("start");

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
