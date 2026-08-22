use std::process::Stdio;
use std::time::Duration;

use anyhow::{Context as _, Result, bail};
use tokio::process::{Child, Command};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tracing::{error, info, warn};

use crate::config::LlamaConfig;

const READY_TIMEOUT: Duration = Duration::from_secs(180);

const POLL: Duration = Duration::from_millis(500);

pub struct Sidecar {
    endpoint: String,

    kill: Option<oneshot::Sender<()>>,

    monitor: JoinHandle<()>,
}

impl Sidecar {
    #[tracing::instrument(name = "llama.start", skip_all, fields(model_file = %config.model_file.display(), port = config.port))]
    pub async fn start(config: &LlamaConfig, model: &str) -> Result<Self> {
        if !config.model_file.is_file() {
            bail!(
                "llama.model_file {} does not exist — download the default with `just model`, \
                 or point it at a GGUF you have",
                config.model_file.display()
            );
        }

        let device = match &config.device {
            Some(wanted) => Some(resolve_device(config, wanted).await?),
            None => None,
        };

        let endpoint = format!("http://127.0.0.1:{}", config.port);
        let mut command = Command::new(&config.server);
        command
            .arg("--model")
            .arg(&config.model_file)
            .arg("--host")
            .arg("127.0.0.1")
            .arg("--port")
            .arg(config.port.to_string())
            .arg("--alias")
            .arg(model)
            .args(&config.args);
        if let Some(id) = &device {
            command.arg("--device").arg(id);
        }
        let mut child = command
            .stdin(Stdio::null())
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

    #[must_use]
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    pub async fn stop(mut self) {
        // dropping the sender is the kill signal
        self.kill.take();
        if let Err(error) = (&mut self.monitor).await {
            warn!(%error, "the sidecar monitor task failed");
        }
    }
}

async fn resolve_device(config: &LlamaConfig, wanted: &str) -> Result<String> {
    let output = Command::new(&config.server)
        .arg("--list-devices")
        .stdin(Stdio::null())
        .output()
        .await
        .with_context(|| format!("running {} --list-devices", config.server.display()))?;
    let listing = String::from_utf8_lossy(&output.stdout);
    let Some((id, name)) = find_device(&listing, wanted) else {
        bail!(
            "no model device matching {wanted:?} — `{} --list-devices` says:\n{}",
            config.server.display(),
            listing.trim_end()
        );
    };
    info!(device = %id, %name, "pinned the model device by name");
    Ok(id)
}

fn find_device(listing: &str, wanted: &str) -> Option<(String, String)> {
    let wanted = wanted.to_lowercase();
    for line in listing.lines() {
        let Some((id, rest)) = line.trim().split_once(':') else {
            continue;
        };
        let (id, name) = (id.trim(), rest.trim());
        if id.is_empty() || id.contains(char::is_whitespace) || name.is_empty() {
            continue;
        }
        if name.to_lowercase().contains(&wanted) {
            return Some((id.to_owned(), name.to_owned()));
        }
    }
    None
}

async fn monitor(mut child: Child, killed: oneshot::Receiver<()>) {
    tokio::select! {
        status = child.wait() => match status {
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

#[cfg(test)]
mod tests {
    use super::find_device;

    const LISTING: &str = "Available devices:\n  \
        Vulkan0: NVIDIA GeForce RTX 5070 (12227 MiB, 8861 MiB free)\n  \
        Vulkan1: AMD Ryzen 9 9900X 12-Core Processor (RADV RAPHAEL_MENDOCINO) (16137 MiB, 10371 MiB free)\n";

    #[test]
    fn finds_a_device_by_case_insensitive_substring() {
        assert_eq!(
            find_device(LISTING, "rtx 5070"),
            Some((
                "Vulkan0".to_owned(),
                "NVIDIA GeForce RTX 5070 (12227 MiB, 8861 MiB free)".to_owned()
            ))
        );
        assert_eq!(
            find_device(LISTING, "RADV").map(|(id, _)| id),
            Some("Vulkan1".to_owned())
        );
    }

    #[test]
    fn no_match_is_none_not_a_guess() {
        assert_eq!(find_device(LISTING, "RTX 4090"), None);
        assert_eq!(find_device("", "RTX 5070"), None);
    }

    #[test]
    fn the_first_of_several_matches_wins() {
        assert_eq!(
            find_device(LISTING, "MiB").map(|(id, _)| id),
            Some("Vulkan0".to_owned())
        );
    }

    #[test]
    fn prose_lines_are_not_devices() {
        let noisy =
            "warning: something: happened here\nAvailable devices:\n  CUDA0: NVIDIA T4 (16 GiB)\n";
        assert_eq!(
            find_device(noisy, "t4").map(|(id, _)| id),
            Some("CUDA0".to_owned())
        );
    }
}
