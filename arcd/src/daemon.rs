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
use arc_core::log::Log;
use arc_core::orphan;
use arc_core::projection::{self, Projection};
use arc_core::provider::Provider;
use arc_core::provider::openai::OpenAiCompat;
use arc_core::session::Engine;
use arc_core::tool::Registry;
use arc_core::tool::sessions::{SessionRead, SessionsSearch};
use arc_core::tool::time::GetTime;
use std::path::Path;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::signal::unix::{SignalKind, signal};
use tokio::sync::Mutex;
use tracing::{error, info, warn};

use crate::config::{Config, ProviderChoice};
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

        let engine = Engine::new(
            log,
            projection,
            Arc::new(provider),
            config.model(),
            identity,
            registry,
            config.no_think,
        );

        Ok(Self {
            config,
            dirs,
            engine: Arc::new(Mutex::new(engine)),
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

        server::serve(listener, self.engine, shutdown()).await;

        info!("stopped");
        Ok(())
    }
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
}
