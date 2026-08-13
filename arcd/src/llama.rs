//! The llama.cpp sidecar: `arcd` owns the local model server's process.
//!
//! `llama-server` is spawned at startup, polled on `/health` until the model
//! is loaded, and killed when the daemon stops (DESIGN.md §6, amendment
//! 2026-08-13 — the daemon supervises, rather than a systemd unit). A monitor
//! task holds the child: an exit nobody asked for is logged loudly, and
//! dropping the [`Sidecar`] is itself the kill signal, so no path out of the
//! daemon — error or clean — leaves a stray model server holding gigabytes of
//! VRAM.
//!
//! No auto-restart in Phase 1. A sidecar that dies mid-run makes completions
//! fail with a transport error the client renders as a fault, which is
//! honest; respawning in a loop over a bad flag or an OOM would not be.
//! Restart policy is a decision for later, noted in TASKS.md, not improvised
//! here.
//!
//! The kill is SIGKILL, deliberately: `llama-server` holds no state worth
//! flushing, and a graceful-shutdown dance would buy nothing but a slower
//! exit.

use std::process::Stdio;
use std::time::Duration;

use anyhow::{Context as _, Result, bail};
use tokio::process::{Child, Command};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tracing::{error, info, warn};

use crate::config::LlamaConfig;

/// How long a cold start may take: a model load reads gigabytes from disk.
const READY_TIMEOUT: Duration = Duration::from_secs(180);

/// Gap between `/health` polls while loading.
const POLL: Duration = Duration::from_millis(500);

/// A supervised `llama-server`, ready to answer completions.
pub struct Sidecar {
    /// Where the server listens. `OpenAiCompat`'s endpoint.
    endpoint: String,

    /// Tells the monitor the daemon wants the child dead. Dropped unsent, it
    /// says the same thing — see [`monitor`].
    kill: Option<oneshot::Sender<()>>,

    /// The monitor task, joined by [`stop`](Self::stop) so shutdown does not
    /// return before the process is gone.
    monitor: JoinHandle<()>,
}

impl Sidecar {
    /// Spawns `llama-server` for `config` and waits until it answers.
    ///
    /// `model` becomes the server's `--alias`, so the name completions run as
    /// is also the name the server reports.
    ///
    /// # Errors
    ///
    /// If the model file is missing, the binary cannot be spawned, the
    /// process exits before becoming ready, or [`READY_TIMEOUT`] passes
    /// first. All of these are startup refusals: a daemon whose default
    /// provider cannot answer should not claim to be ready.
    #[tracing::instrument(name = "llama.start", skip_all, fields(model_file = %config.model_file.display(), port = config.port))]
    pub async fn start(config: &LlamaConfig, model: &str) -> Result<Self> {
        if !config.model_file.is_file() {
            bail!(
                "llama.model_file {} does not exist — download the default with `just model`, \
                 or point it at a GGUF you have",
                config.model_file.display()
            );
        }

        let endpoint = format!("http://127.0.0.1:{}", config.port);
        let mut child = Command::new(&config.server)
            .arg("--model")
            .arg(&config.model_file)
            .arg("--host")
            .arg("127.0.0.1")
            .arg("--port")
            .arg(config.port.to_string())
            .arg("--alias")
            .arg(model)
            .args(&config.args)
            .stdin(Stdio::null())
            // stdout/stderr inherited: the server's log is part of the
            // daemon's, and hiding it would hide every model-load error.
            .kill_on_drop(true)
            .spawn()
            .with_context(|| {
                format!(
                    "spawning {} — is llama.cpp installed and on PATH?",
                    config.server.display()
                )
            })?;

        wait_ready(&mut child, &endpoint).await?;
        info!(%endpoint, model, "llama-server ready");

        let (kill, killed) = oneshot::channel();
        let monitor = tokio::spawn(monitor(child, killed));
        Ok(Self {
            endpoint,
            kill: Some(kill),
            monitor,
        })
    }

    /// Where the server listens.
    #[must_use]
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    /// Kills the sidecar and waits until it is gone.
    pub async fn stop(mut self) {
        // Dropping the sender is the signal; `send` would say nothing more.
        self.kill.take();
        if let Err(error) = (&mut self.monitor).await {
            warn!(%error, "the sidecar monitor task failed");
        }
    }
}

/// Owns the child until it exits or the daemon asks for it to be killed.
///
/// The kill signal is the channel closing, not a message: an explicit
/// [`Sidecar::stop`] and a `Sidecar` dropped on an error path both close it,
/// so every way out of the daemon kills the child the same way.
async fn monitor(mut child: Child, killed: oneshot::Receiver<()>) {
    tokio::select! {
        status = child.wait() => match status {
            // No one asked: turns will fail with a transport error until the
            // daemon is restarted. Loud, and deliberately not auto-healed.
            Ok(status) => error!(%status, "llama-server exited unexpectedly"),
            Err(error) => error!(%error, "waiting on llama-server failed"),
        },
        _ = killed => {
            if let Err(error) = child.kill().await {
                warn!(%error, "killing llama-server failed");
            }
        }
    }
}

/// Polls `/health` until the server answers 200, the child exits, or the
/// timeout passes.
///
/// `llama-server` answers 503 while the model is loading and 200 once it can
/// serve; anything transport-shaped just means "not yet".
async fn wait_ready(child: &mut Child, endpoint: &str) -> Result<()> {
    let http = reqwest::Client::new();
    let url = format!("{endpoint}/health");
    let deadline = Instant::now() + READY_TIMEOUT;

    loop {
        if let Some(status) = child.try_wait().context("checking on llama-server")? {
            bail!(
                "llama-server exited with {status} before becoming ready — \
                 its log above says why"
            );
        }
        if let Ok(response) = http.get(&url).send().await {
            if response.status().is_success() {
                return Ok(());
            }
        }
        if Instant::now() >= deadline {
            let _ = child.start_kill();
            bail!(
                "llama-server did not become ready within {}s",
                READY_TIMEOUT.as_secs()
            );
        }
        tokio::time::sleep(POLL).await;
    }
}
