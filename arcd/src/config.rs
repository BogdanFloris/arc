//! The config file: `arc.toml`, every field optional.
//!
//! ```toml
//! data_dir = "data"
//! provider = "local"           # the only choice today
//! model    = "qwen3-8b"        # default: the model file's stem, see [`Config::model`]
//! bind     = "127.0.0.1:8787"
//!
//! [llama]                      # the local sidecar, when provider = "local"
//! server     = "llama-server"
//! model_file = "data/models/Qwen3-8B-Q4_K_M.gguf"
//! port       = 8080
//! device     = "RTX 5070"      # pin by name; resolved to an index at start
//! args       = ["-ngl", "99"]
//!
//! [consolidation]              # the idle-timeout pass (DESIGN.md §5.4)
//! enabled      = false         # off until 7.2 gives the pass content
//! idle_seconds = 1800
//! ```

use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use anyhow::{Context as _, Result};
use serde::{Deserialize, Serialize};

/// Directory all runtime state hangs off (DESIGN.md §10).
const DEFAULT_DATA_DIR: &str = "data";

/// Localhost, per DESIGN.md §7: the socket never leaves the machine
const DEFAULT_BIND: &str = "127.0.0.1:8787";

/// The `llama-server` binary, resolved from `PATH` when not a path.
const DEFAULT_LLAMA_SERVER: &str = "llama-server";

/// Where `just model` puts the default local model.
const DEFAULT_MODEL_FILE: &str = "data/models/Qwen3-8B-Q4_K_M.gguf";

/// The sidecar's port on localhost.
const DEFAULT_LLAMA_PORT: u16 = 8080;

/// Longest tool result the registry keeps
const DEFAULT_MAX_TOOL_RESULT_BYTES: usize = 32 * 1024;

/// How long a session must sit untouched before consolidation covers it.
const DEFAULT_IDLE_SECONDS: u64 = 1800;

/// The resolved daemon configuration.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct Config {
    /// Root of the runtime state tree. Relative paths resolve against the
    /// working directory, which for a hand-started daemon is the repo root.
    pub data_dir: PathBuf,

    /// Which backend answers completions. Only `local` for now; the enum
    /// stays so the next provider is a new variant, not a new config shape.
    pub provider: ProviderChoice,

    /// Model name for completions. `None` resolves per provider — see
    /// [`Config::model`].
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,

    /// Address the `WebSocket` server binds.
    pub bind: SocketAddr,

    /// The local sidecar, read only when `provider = "local"`.
    pub llama: LlamaConfig,

    /// The maximum amount of content bytes for a tool result.
    pub max_tool_result_bytes: usize,

    /// Append `/no_think` to interactive turns' system prompt
    pub no_think: bool,

    /// The idle-timeout consolidation pass (DESIGN.md §5.4).
    pub consolidation: ConsolidationConfig,
}

/// The consolidation trigger's dials — its own section, hermes-style:
/// background work never borrows the interactive path's config.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct ConsolidationConfig {
    /// Off by default: until 7.2 gives the pass content, nothing should be
    /// marking real sessions as covered.
    pub enabled: bool,

    /// How long a session must sit untouched before a pass covers it.
    pub idle_seconds: u64,
}

impl Default for ConsolidationConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            idle_seconds: DEFAULT_IDLE_SECONDS,
        }
    }
}

/// Which provider implementation the daemon runs with.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum ProviderChoice {
    /// The llama.cpp sidecar, spoken to as an OpenAI-compatible endpoint.
    Local,
}

/// The `llama-server` sidecar `arcd` supervises.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct LlamaConfig {
    /// The binary to spawn. A bare name is resolved from `PATH`.
    pub server: PathBuf,

    /// The GGUF to load. The default is where `just model` downloads to.
    pub model_file: PathBuf,

    /// Port the sidecar listens on, always on 127.0.0.1.
    pub port: u16,

    /// Pin the model to the device whose `--list-devices` name contains this
    /// string, case-insensitive, resolved at every start. Names survive
    /// reboots; backend indexes do not (2026-08-20: Vulkan order flipped and
    /// the model silently landed on the iGPU). No match refuses startup.
    /// Unset, the server picks its own devices. Do not also pass `--device`
    /// in `args`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub device: Option<String>,

    /// Extra `llama-server` arguments, passed through verbatim — GPU offload
    /// (`-ngl`), context size (`-c`), `MoE` offload (`--n-cpu-moe`), whatever
    /// the model wants. A passthrough rather than named fields, because
    /// llama.cpp's flag surface changes faster than this config should.
    pub args: Vec<String>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from(DEFAULT_DATA_DIR),
            provider: ProviderChoice::Local,
            model: None,
            bind: DEFAULT_BIND.parse().expect("default bind address is valid"),
            llama: LlamaConfig::default(),
            max_tool_result_bytes: DEFAULT_MAX_TOOL_RESULT_BYTES,
            no_think: true,
            consolidation: ConsolidationConfig::default(),
        }
    }
}

impl Default for LlamaConfig {
    fn default() -> Self {
        Self {
            server: PathBuf::from(DEFAULT_LLAMA_SERVER),
            model_file: PathBuf::from(DEFAULT_MODEL_FILE),
            port: DEFAULT_LLAMA_PORT,
            device: None,
            args: Vec::new(),
        }
    }
}

impl Config {
    /// Loads the config at `path`, or the defaults if there is no file there.
    ///
    /// # Errors
    ///
    /// If the file exists but cannot be read, is not TOML, holds a key this
    /// build does not know, or holds a value of the wrong shape — an
    /// unparseable `bind` included.
    pub fn load(path: &Path) -> Result<Self> {
        let text = match std::fs::read_to_string(path) {
            Ok(text) => text,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(Self::default()),
            Err(err) => {
                return Err(err).with_context(|| format!("reading config {}", path.display()));
            }
        };
        toml::from_str(&text).with_context(|| format!("parsing config {}", path.display()))
    }

    /// The model name completions run as: what the engine records in the log
    /// and what goes in the request.
    ///
    /// Unset, it resolves to the model file's stem, so the log names the
    /// weights that actually answered (`llama-server` serves one model and
    /// ignores the name, but the log should not).
    #[must_use]
    pub fn model(&self) -> String {
        if let Some(model) = &self.model {
            return model.clone();
        }
        match self.provider {
            ProviderChoice::Local => self.llama.model_file.file_stem().map_or_else(
                || "local".to_owned(),
                |stem| stem.to_string_lossy().into_owned(),
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{Config, ProviderChoice};
    use std::net::SocketAddr;
    use std::path::PathBuf;

    #[test]
    fn an_empty_file_is_the_defaults() {
        let config: Config = toml::from_str("").expect("empty config parses");
        assert_eq!(config, Config::default());
        assert_eq!(config.data_dir, PathBuf::from("data"));
        assert_eq!(config.provider, ProviderChoice::Local);
        assert_eq!(config.model, None);
        assert_eq!(config.bind, "127.0.0.1:8787".parse::<SocketAddr>().unwrap());
        assert_eq!(config.llama.server, PathBuf::from("llama-server"));
        assert_eq!(config.llama.port, 8080);
        assert!(config.llama.args.is_empty());
        assert!(!config.consolidation.enabled, "off until 7.2");
        assert_eq!(config.consolidation.idle_seconds, 1800);
    }

    #[test]
    fn a_missing_file_is_the_defaults_and_a_present_one_wins() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("arc.toml");
        assert_eq!(
            Config::load(&path).expect("missing is fine"),
            Config::default()
        );

        std::fs::write(&path, "model = \"gemini-3.1-pro\"\n").expect("write config");
        let config = Config::load(&path).expect("present config loads");
        assert_eq!(config.model.as_deref(), Some("gemini-3.1-pro"));
        assert_eq!(config.data_dir, Config::default().data_dir);
    }

    #[test]
    fn every_field_round_trips() {
        let config = Config {
            data_dir: PathBuf::from("/srv/arc"),
            provider: ProviderChoice::Local,
            model: Some("qwen".to_owned()),
            bind: "127.0.0.1:9000".parse().expect("valid address"),
            llama: super::LlamaConfig {
                server: PathBuf::from("/opt/llama/llama-server"),
                model_file: PathBuf::from("/models/q.gguf"),
                port: 9090,
                device: Some("RTX 5070".to_owned()),
                args: vec!["-ngl".to_owned(), "99".to_owned()],
            },
            max_tool_result_bytes: 512,
            no_think: false,
            consolidation: super::ConsolidationConfig {
                enabled: true,
                idle_seconds: 600,
            },
        };
        let text = toml::to_string(&config).expect("serializes");
        assert_eq!(toml::from_str::<Config>(&text).expect("parses"), config);
    }

    #[test]
    fn an_unknown_key_is_rejected() {
        let err = toml::from_str::<Config>("modle = \"gemini-3.1-pro\"\n")
            .expect_err("a typo must not be ignored");
        assert!(err.to_string().contains("modle"), "{err}");
    }

    #[test]
    fn an_unknown_llama_key_is_rejected() {
        let err = toml::from_str::<Config>("[llama]\nprot = 9090\n")
            .expect_err("a typo must not be ignored");
        assert!(err.to_string().contains("prot"), "{err}");
    }

    #[test]
    fn an_unknown_consolidation_key_is_rejected() {
        let err = toml::from_str::<Config>("[consolidation]\nidle_secs = 60\n")
            .expect_err("a typo must not be ignored");
        assert!(err.to_string().contains("idle_secs"), "{err}");
    }

    #[test]
    fn an_unknown_provider_is_rejected() {
        let err = toml::from_str::<Config>("provider = \"openai\"\n")
            .expect_err("an unknown provider must not load");
        assert!(err.to_string().contains("openai"), "{err}");
    }

    #[test]
    fn an_unparseable_bind_is_rejected_at_load() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("arc.toml");
        std::fs::write(&path, "bind = \"127.0.0.1\"\n").expect("write config");

        let err = Config::load(&path).expect_err("a portless address must not load");
        let chain = format!("{err:#}");
        assert!(chain.contains("parsing config"), "{chain}");
    }
}
