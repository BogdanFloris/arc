//! `arcd run` — startup composition and lifecycle.
//!
//! [`Daemon::start`] is the whole of the daemon's wiring: open the log, catch
//! the index up to it, build the provider, and hand back the session engine
//! over all three. [`Daemon::serve`] is the lifecycle: bind the socket,
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
use arc_core::log::Log;
use arc_core::projection::{self, Projection};
use arc_core::provider::antigravity::Antigravity;
use arc_core::provider::oauth::{OauthConfig, TokenManager};
use arc_core::session::Engine;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::Mutex;
use tracing::{error, info};

use crate::config::Config;
use crate::dirs::DataDirs;
use crate::identity;
use crate::server;

/// A started daemon: everything durable is open, nothing is being served yet.
pub struct Daemon {
    config: Config,
    dirs: DataDirs,

    /// The session engine, holding the log, the index, the provider, and the
    /// identity file.
    ///
    /// The mutex serializes completions daemon-wide: `send_message` takes
    /// `&mut Engine`, so one turn runs at a time and everything else waits.
    /// That is Phase 1's single-user reality (DESIGN.md §1), stated once here
    /// rather than worked around in the server.
    engine: Arc<Mutex<Engine<Antigravity>>>,
}

impl Daemon {
    /// Opens everything the daemon needs, in dependency order.
    ///
    /// # Errors
    ///
    /// If the directories cannot be created, the log cannot be opened or
    /// recovered, or the index cannot be opened or replayed. All of these mean
    /// durable state is not in a known condition, which is a refusal to start.
    #[tracing::instrument(name = "daemon.start", skip_all, fields(data_dir = %dirs.root().display()))]
    pub fn start(config: Config, dirs: DataDirs) -> Result<Self> {
        dirs.create()
            .with_context(|| format!("preparing {}", dirs.root().display()))?;

        // Recovery happens inside `open`: a torn tail is sealed, never
        // truncated, and the append point comes back with it (DESIGN.md §3).
        let log = Log::open(dirs.log())
            .with_context(|| format!("opening the event log at {}", dirs.log().display()))?;
        info!(
            next_seq = log.next_seq(),
            segment = %log.current_segment().display(),
            "event log ready"
        );

        let mut projection = Projection::open(dirs.index())
            .with_context(|| format!("opening the index at {}", dirs.index().display()))?;
        let reader = log.reader().context("listing log segments for replay")?;
        let stats = projection::replay(reader, &mut projection)
            .context("replaying the event log into the index")?;
        info!(
            applied = stats.applied,
            skipped = stats.skipped,
            "index caught up with the log"
        );

        let tokens = TokenManager::new(OauthConfig::default(), dirs.tokens());
        let provider = Antigravity::new(Arc::new(tokens));
        let identity = identity::load(dirs.identity()).context("loading the identity file")?;
        if let Some(text) = &identity {
            info!(chars = text.len(), "identity file loaded");
        } else {
            info!("no identity file, running without one");
        }

        let engine = Engine::new(
            log,
            projection,
            Arc::new(provider),
            config.model.as_str(),
            identity,
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
            model = self.config.model,
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

/// Resolves when the daemon should stop.
///
/// A signal handler that cannot be installed counts as a stop: a daemon no one
/// can shut down is worse than one that refuses to run, and it fails loudly at
/// startup rather than quietly at the end.
async fn shutdown() {
    match tokio::signal::ctrl_c().await {
        Ok(()) => info!("shutdown signal received"),
        Err(error) => error!(%error, "the shutdown signal handler failed; stopping"),
    }
}
