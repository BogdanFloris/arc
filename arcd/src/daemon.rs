use anyhow::{Context as _, Result};
use arc_core::archive::Archive;
use arc_core::consolidation::extract::{ModelExtractor, PROMPT_VERSION_V3};
use arc_core::consolidation::{self, Extractor};
use arc_core::log::Log;
use arc_core::orphan;
use arc_core::projection::{self, Projection, Reader};
use arc_core::provider::role_label;
use arc_core::secrets::Secrets;
use arc_core::session::{Engine, ProjectSpec, Runner};
use arc_core::store::Store;
use arc_core::tool::Registry;
use arc_core::tool::builtin;
use arc_core::tool::workspace::{self, Grant, Mode, Workspace};
use arc_proto::v1::{Notification, ReviewChanged, SessionRole, notification};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::net::TcpListener;
use tokio::signal::unix::{SignalKind, signal};
use tokio::sync::broadcast;
use tokio::task::JoinHandle;
use tracing::{error, info, warn};

use crate::config::{Config, ConsolidationConfig};
use crate::dirs::DataDirs;
use crate::identity;
use crate::jobs::Supervisor;
use crate::llama::Sidecar;
use crate::roles::Roles;
use crate::server;

pub async fn run(config: Config, dirs: DataDirs) -> Result<()> {
    let sidecar = Sidecar::start(&config.llama, &config.model()).await?;
    let identity = identity::load(dirs.identity()).context("loading the identity file")?;
    if let Some(text) = &identity {
        info!(chars = text.len(), "identity file loaded");
    } else {
        info!("no identity file, running without one");
    }
    let identity_for_consolidation = identity.clone();
    let secrets = Secrets::new(dirs.secrets());
    let roles = match Roles::resolve(&config, sidecar.endpoint(), &secrets, identity) {
        Ok(roles) => roles,
        Err(error) => {
            sidecar.stop().await;
            return Err(error);
        }
    };
    let served = match Daemon::start(config, dirs, roles, identity_for_consolidation) {
        Ok(daemon) => daemon.serve().await,
        Err(error) => Err(error),
    };
    sidecar.stop().await;
    served
}

/// Capacity of the notification broadcast: generous enough that a slow
/// subscriber only lags under sustained job/session churn, not a burst.
const NOTIFICATION_CAPACITY: usize = 256;

pub struct Daemon {
    config: Config,
    dirs: DataDirs,

    engine: Arc<Engine>,

    reads: Arc<Reader>,

    roles: Roles,

    notifier: broadcast::Sender<Notification>,

    identity: Option<String>,
}

impl Daemon {
    #[tracing::instrument(name = "daemon.start", skip_all, fields(data_dir = %dirs.root().display()))]
    pub fn start(
        config: Config,
        dirs: DataDirs,
        roles: Roles,
        identity: Option<String>,
    ) -> Result<Self> {
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

        let archive = Arc::new(
            Archive::open(dirs.index())
                .with_context(|| format!("opening {} read-only", dirs.index().display()))?,
        );
        let (project_names, scratch) = dispatch_projects(&config);
        let mut registry = Registry::new(config.max_tool_result_bytes);
        for tool in builtin::tools(archive, project_names, scratch) {
            registry.register(tool);
        }
        for tool in workspace::tools(Arc::new(Workspace::new())) {
            registry.register(tool);
        }

        let reads = Arc::new(
            Reader::open(dirs.index())
                .with_context(|| format!("opening {} for reads", dirs.index().display()))?,
        );

        let projects = config
            .projects
            .iter()
            .map(|(name, project)| (name.clone(), project_spec(project)))
            .collect();
        let role_identities = roles
            .all()
            .into_iter()
            .map(|runner| {
                (
                    runner.role,
                    (runner.provider.name().to_owned(), runner.model.clone()),
                )
            })
            .collect();
        let (notifier, _receiver) = broadcast::channel(NOTIFICATION_CAPACITY);
        let engine = Engine::new(store, registry)
            .with_projects(projects)
            .with_role_identities(role_identities)
            .with_notifier(notifier.clone());

        Ok(Self {
            config,
            dirs,
            engine: Arc::new(engine),
            reads,
            roles,
            notifier,
            identity,
        })
    }

    pub async fn serve(self) -> Result<()> {
        let listener = TcpListener::bind(self.config.bind)
            .await
            .with_context(|| format!("binding {}", self.config.bind))?;
        let bound = listener.local_addr().unwrap_or(self.config.bind);

        for runner in self.roles.all() {
            info!(
                role = role_label(runner.role),
                provider = runner.provider.name(),
                model = runner.model,
                thinking = runner.thinking.label(),
                endpoint = runner.provider.endpoint(),
                "role resolved"
            );
        }
        info!(
            bind = %bound,
            data_dir = %self.dirs.root().display(),
            version = env!("CARGO_PKG_VERSION"),
            "arcd ready"
        );

        let mut namespaces = vec!["global".to_owned()];
        namespaces.extend(self.config.projects.keys().cloned());
        let consolidation = consolidation_task(
            self.config.consolidation,
            self.roles.archivist(),
            Arc::clone(&self.engine),
            self.identity.clone(),
            namespaces,
            self.notifier.clone(),
        );

        let job_runners = BTreeMap::from([
            (SessionRole::Executor, self.roles.executor().clone()),
            (SessionRole::Archivist, self.roles.archivist().clone()),
        ]);
        let project_roots = self
            .config
            .projects
            .iter()
            .map(|(name, project)| (name.clone(), project.root.clone()))
            .collect();
        let supervisor = Arc::new(
            Supervisor::new(Arc::clone(&self.engine), job_runners)
                .with_projects(project_roots)
                .with_notifier(self.notifier.clone())
                .with_concierge(self.roles.concierge().clone()),
        );
        // after orphan repair (already done in `start`), before serving
        supervisor.repair_restart_handbacks().await;

        server::serve(
            listener,
            self.engine,
            self.roles.concierge().clone(),
            self.reads,
            Arc::clone(&supervisor),
            self.notifier,
            shutdown(),
        )
        .await;

        if let Some(task) = consolidation {
            task.abort();
        }
        supervisor.shutdown().await;

        info!("stopped");
        Ok(())
    }
}

const CONSOLIDATION_TICK: Duration = Duration::from_secs(60);

fn consolidation_task(
    config: ConsolidationConfig,
    archivist: &Runner,
    engine: Arc<Engine>,
    identity: Option<String>,
    namespaces: Vec<String>,
    notifier: broadcast::Sender<Notification>,
) -> Option<JoinHandle<()>> {
    if !config.enabled {
        info!("consolidation disabled");
        return None;
    }
    let idle = Duration::from_secs(config.idle_seconds);
    let extractor = ModelExtractor::new(
        Arc::clone(&archivist.provider),
        &archivist.model,
        archivist.thinking,
        Duration::from_secs(config.timeout_seconds),
        identity,
        namespaces,
    );
    info!(
        idle_seconds = config.idle_seconds,
        timeout_seconds = config.timeout_seconds,
        prompt_version = PROMPT_VERSION_V3,
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
            tick_once(&engine, &extractor, cutoff, &mut strikes, &notifier).await;
        }
    }))
}

async fn tick_once<E: Extractor>(
    engine: &Engine,
    extractor: &E,
    cutoff: i64,
    strikes: &mut Strikes,
    notifier: &broadcast::Sender<Notification>,
) {
    let pass =
        consolidation::run_pass(engine, extractor, cutoff, PROMPT_VERSION_V3, strikes.skip());
    match pass.await {
        Ok(consolidation::Outcome::Consolidated {
            session_id,
            records,
            ..
        }) => {
            strikes.succeeded(&session_id);
            // consolidation writes memory events straight through the store,
            // bypassing the engine's own notify-on-append
            if records > 0 {
                notify_review_changed(engine, notifier);
            }
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

fn notify_review_changed(engine: &Engine, notifier: &broadcast::Sender<Notification>) {
    match engine.review_pending() {
        Ok(pending) => {
            let _ = notifier.send(Notification {
                event: Some(notification::Event::ReviewChanged(ReviewChanged {
                    pending,
                })),
            });
        }
        Err(error) => warn!(%error, "reading the review queue after consolidation failed"),
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

/// What `dispatch` may bind a job to: every configured project's name and
/// description, plus the scratch project if a project is literally named
/// `scratch`.
fn dispatch_projects(config: &Config) -> (Vec<(String, String)>, Option<String>) {
    let names = config
        .projects
        .iter()
        .map(|(name, project)| (name.clone(), project.description.clone()))
        .collect();
    let scratch = config
        .projects
        .contains_key("scratch")
        .then(|| "scratch".to_owned());
    (names, scratch)
}

fn project_spec(project: &crate::config::ProjectConfig) -> ProjectSpec {
    let sources = project
        .sources
        .iter()
        .map(|source| source.resolve())
        .collect();
    let mut grants = vec![Grant::new(project.root.clone(), Mode::ReadWrite)];
    grants.extend(
        project
            .read_only
            .iter()
            .map(|root| Grant::new(root.clone(), Mode::ReadOnly)),
    );
    ProjectSpec {
        sources,
        grants,
        command_prefix: project.command_prefix.clone(),
    }
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
    use arc_proto::v1::{Event, SessionCreated, SessionEvent, SessionRole, event, session_event};
    use tempfile::TempDir;

    use super::*;
    use crate::config::{ProjectConfig, ToolSource};
    use crate::dirs::DataDirs;

    fn project_config(root: &str, description: &str) -> ProjectConfig {
        ProjectConfig {
            root: std::path::PathBuf::from(root),
            description: description.to_owned(),
            read_only: Vec::new(),
            sources: vec![ToolSource::Builtin],
            command_prefix: Vec::new(),
        }
    }

    #[test]
    fn project_spec_carries_the_configured_command_prefix() {
        let mut config = project_config("/tmp/arc", "");
        config.command_prefix = vec!["nix".to_owned(), "develop".to_owned(), "-c".to_owned()];

        let spec = project_spec(&config);

        assert_eq!(spec.command_prefix, ["nix", "develop", "-c"]);
    }

    #[test]
    fn project_spec_defaults_to_no_command_prefix() {
        let config = project_config("/tmp/arc", "");

        let spec = project_spec(&config);

        assert!(spec.command_prefix.is_empty());
    }

    #[test]
    fn dispatch_projects_lists_every_project_and_finds_the_scratch_one() {
        let mut config = Config::default();
        config.projects.insert(
            "arc".to_owned(),
            project_config("/tmp/arc", "ARC's own implementation repo"),
        );
        config
            .projects
            .insert("scratch".to_owned(), project_config("/tmp/scratch", ""));

        let (names, scratch) = dispatch_projects(&config);

        assert_eq!(
            names,
            [
                ("arc".to_owned(), "ARC's own implementation repo".to_owned()),
                ("scratch".to_owned(), String::new()),
            ]
        );
        assert_eq!(scratch, Some("scratch".to_owned()));
    }

    #[test]
    fn dispatch_projects_without_a_scratch_project_names_none() {
        let mut config = Config::default();
        config
            .projects
            .insert("arc".to_owned(), project_config("/tmp/arc", ""));

        let (names, scratch) = dispatch_projects(&config);

        assert_eq!(names, [("arc".to_owned(), String::new())]);
        assert_eq!(scratch, None);
    }

    // port 1 refuses every connection: startup must not reach a provider
    fn unreachable_roles() -> Roles {
        Roles::resolve(
            &Config::default(),
            "http://127.0.0.1:1",
            &Secrets::new(Path::new("/nonexistent")),
            None,
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
                    grants: Vec::new(),
                    dispatched_by: String::new(),
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

        let daemon = Daemon::start(Config::default(), dirs, unreachable_roles(), None)
            .expect("start over a stale index");

        let sessions = daemon.engine.sessions().expect("sessions");
        assert_eq!(sessions.len(), 1);
        assert_eq!(sessions[0].id, "s-01");
    }

    #[tokio::test]
    async fn the_consolidation_tick_only_runs_when_enabled() {
        let temp = TempDir::new().expect("temp dir");
        let dirs = DataDirs::new(&temp.path().join("data"));
        let daemon =
            Daemon::start(Config::default(), dirs, unreachable_roles(), None).expect("start");

        assert!(
            consolidation_task(
                Config::default().consolidation,
                daemon.roles.archivist(),
                Arc::clone(&daemon.engine),
                None,
                vec!["global".to_owned()],
                daemon.notifier.clone(),
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
            daemon.roles.archivist(),
            Arc::clone(&daemon.engine),
            None,
            vec!["global".to_owned()],
            daemon.notifier.clone(),
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
                grants: Vec::new(),
                dispatched_by: String::new(),
            }),
            session_event::Event::MessageAppended(MessageAppended {
                session_id: session_id.to_owned(),
                role: Role::User as i32,
                content: "hello".to_owned(),
                partial: false,
                turn_id: format!("{session_id}-t1"),
                ..Default::default()
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
        let daemon =
            Daemon::start(Config::default(), dirs, unreachable_roles(), None).expect("start");

        let extractor = AlwaysFailing(std::sync::Mutex::new(Vec::new()));
        let mut strikes = Strikes::default();
        for _ in 0..4 {
            tick_once(
                &daemon.engine,
                &extractor,
                i64::MAX,
                &mut strikes,
                &daemon.notifier,
            )
            .await;
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

    struct Scripted(Vec<arc_proto::v1::memory_event::Event>);

    impl arc_core::consolidation::Extractor for Scripted {
        async fn extract(
            &self,
            _session: &arc_core::consolidation::SessionSnapshot,
        ) -> Result<Vec<arc_proto::v1::memory_event::Event>, arc_core::consolidation::ExtractError>
        {
            Ok(self.0.clone())
        }
    }

    fn created_record(id: &str) -> arc_proto::v1::memory_event::Event {
        use arc_proto::v1::{MemoryRecord, MemoryRecordCreated, memory_record};
        arc_proto::v1::memory_event::Event::RecordCreated(MemoryRecordCreated {
            record: Some(MemoryRecord {
                id: id.to_owned(),
                kind: memory_record::Kind::Fact as i32,
                namespace: "global".to_owned(),
                title: "extracted".to_owned(),
                summary: "an extracted fact".to_owned(),
                body: "the body".to_owned(),
                links: Vec::new(),
                provenance: None,
                status: memory_record::Status::Active as i32,
            }),
        })
    }

    #[tokio::test]
    async fn a_consolidation_pass_that_lands_a_record_broadcasts_review_changed() {
        let temp = TempDir::new().expect("temp dir");
        let dirs = DataDirs::new(&temp.path().join("data"));
        dirs.create().expect("create dirs");
        let mut log = Log::open(dirs.log()).expect("open log");
        seed_idle_session(&mut log, "s-a", 1_000_000);
        drop(log);
        let daemon =
            Daemon::start(Config::default(), dirs, unreachable_roles(), None).expect("start");
        let mut notifications = daemon.notifier.subscribe();

        let extractor = Scripted(vec![created_record("m-1")]);
        let mut strikes = Strikes::default();
        tick_once(
            &daemon.engine,
            &extractor,
            i64::MAX,
            &mut strikes,
            &daemon.notifier,
        )
        .await;

        let notification = notifications
            .try_recv()
            .expect("a review_changed notification was pushed");
        match notification.event {
            Some(notification::Event::ReviewChanged(changed)) => {
                assert_eq!(changed.pending, 1, "the landed record is pending review");
            }
            other => panic!("expected ReviewChanged, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn a_consolidation_pass_that_extracts_nothing_pushes_no_notification() {
        let temp = TempDir::new().expect("temp dir");
        let dirs = DataDirs::new(&temp.path().join("data"));
        dirs.create().expect("create dirs");
        let mut log = Log::open(dirs.log()).expect("open log");
        seed_idle_session(&mut log, "s-a", 1_000_000);
        drop(log);
        let daemon =
            Daemon::start(Config::default(), dirs, unreachable_roles(), None).expect("start");
        let mut notifications = daemon.notifier.subscribe();

        let extractor = Scripted(Vec::new());
        let mut strikes = Strikes::default();
        tick_once(
            &daemon.engine,
            &extractor,
            i64::MAX,
            &mut strikes,
            &daemon.notifier,
        )
        .await;

        assert!(
            notifications.try_recv().is_err(),
            "nothing landed, so nothing to notify"
        );
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

    #[tokio::test]
    async fn a_bound_session_gets_the_configured_projects_grants() {
        let temp = TempDir::new().expect("temp dir");
        let project_root = temp.path().join("proj");
        std::fs::create_dir_all(&project_root).expect("mkdir proj");
        let notes_root = temp.path().join("notes");
        std::fs::create_dir_all(&notes_root).expect("mkdir notes");

        let mut config = Config::default();
        config.projects.insert(
            "arc".to_owned(),
            ProjectConfig {
                root: project_root.clone(),
                description: String::new(),
                read_only: vec![notes_root.clone()],
                sources: vec![ToolSource::Builtin, ToolSource::Workspace],
                command_prefix: Vec::new(),
            },
        );
        let dirs = DataDirs::new(&temp.path().join("data"));
        let log_dir = dirs.log().to_path_buf();
        let daemon = Daemon::start(config, dirs, unreachable_roles(), None).expect("start");

        let session_id = daemon
            .engine
            .create_bound_session(
                daemon.roles.concierge(),
                "arc",
                SessionRole::Concierge,
                None,
            )
            .expect("create a bound session");

        let log = Log::open(&log_dir).expect("reopen log");
        let created = log
            .reader()
            .expect("reader")
            .map(|result| result.expect("event"))
            .find_map(|event| match event.payload {
                Some(event::Payload::Session(SessionEvent {
                    event: Some(session_event::Event::SessionCreated(created)),
                })) if created.session_id == session_id => Some(created),
                _ => None,
            })
            .expect("the bound session was recorded");

        let root = project_root.canonicalize().expect("canon");
        let notes = notes_root.canonicalize().expect("canon");
        assert_eq!(
            created.grants,
            [
                arc_proto::v1::WorkspaceGrant {
                    root: root.to_string_lossy().into_owned(),
                    read_write: true,
                },
                arc_proto::v1::WorkspaceGrant {
                    root: notes.to_string_lossy().into_owned(),
                    read_write: false,
                },
            ]
        );
    }
}
