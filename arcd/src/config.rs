use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use anyhow::{Context as _, Result};
use serde::{Deserialize, Serialize};

const DEFAULT_DATA_DIR: &str = "data";

const DEFAULT_BIND: &str = "127.0.0.1:8787";

const DEFAULT_LLAMA_SERVER: &str = "llama-server";

const DEFAULT_MODEL_FILE: &str = "data/models/Qwen3-8B-Q4_K_M.gguf";

const DEFAULT_LLAMA_PORT: u16 = 8080;

const DEFAULT_MAX_TOOL_RESULT_BYTES: usize = 32 * 1024;

const DEFAULT_IDLE_SECONDS: u64 = 1800;

const DEFAULT_CONSOLIDATION_TIMEOUT_SECONDS: u64 = 300;

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct Config {
    pub data_dir: PathBuf,

    pub provider: ProviderChoice,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,

    pub bind: SocketAddr,

    pub llama: LlamaConfig,

    pub max_tool_result_bytes: usize,

    pub no_think: bool,

    pub consolidation: ConsolidationConfig,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct ConsolidationConfig {
    pub enabled: bool,

    pub idle_seconds: u64,

    pub timeout_seconds: u64,
}

impl Default for ConsolidationConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            idle_seconds: DEFAULT_IDLE_SECONDS,
            timeout_seconds: DEFAULT_CONSOLIDATION_TIMEOUT_SECONDS,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum ProviderChoice {
    Local,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct LlamaConfig {
    pub server: PathBuf,

    pub model_file: PathBuf,

    pub port: u16,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub device: Option<String>,

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
        assert!(!config.consolidation.enabled, "off in code defaults");
        assert_eq!(config.consolidation.idle_seconds, 1800);
        assert_eq!(config.consolidation.timeout_seconds, 300);
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
                timeout_seconds: 120,
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
