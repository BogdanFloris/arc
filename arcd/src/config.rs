//! The config file: `arc.toml`, every field optional.
//!
//! ```toml
//! data_dir = "data"
//! model    = "gemini-3.6-flash"
//! bind     = "127.0.0.1:8787"
//! ```

use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use anyhow::{Context as _, Result};
use serde::{Deserialize, Serialize};

/// Directory all runtime state hangs off (DESIGN.md §10).
const DEFAULT_DATA_DIR: &str = "data";

/// Model used when a request does not name one.
const DEFAULT_MODEL: &str = "gemini-3.6-flash";

/// Localhost, per DESIGN.md §7: the socket never leaves the machine
const DEFAULT_BIND: &str = "127.0.0.1:8787";

/// The resolved daemon configuration.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct Config {
    /// Root of the runtime state tree. Relative paths resolve against the
    /// working directory, which for a hand-started daemon is the repo root.
    pub data_dir: PathBuf,

    /// Default model for completions.
    pub model: String,

    /// Address the `WebSocket` server binds. Unused until task 5.4 and parsed
    /// now anyway: a typo should fail at startup, not on the day the server
    /// lands.
    pub bind: SocketAddr,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from(DEFAULT_DATA_DIR),
            model: DEFAULT_MODEL.to_owned(),
            bind: DEFAULT_BIND.parse().expect("default bind address is valid"),
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
}

#[cfg(test)]
mod tests {
    use super::Config;
    use std::net::SocketAddr;
    use std::path::PathBuf;

    #[test]
    fn an_empty_file_is_the_defaults() {
        let config: Config = toml::from_str("").expect("empty config parses");
        assert_eq!(config, Config::default());
        assert_eq!(config.data_dir, PathBuf::from("data"));
        assert_eq!(config.model, "gemini-3.6-flash");
        assert_eq!(config.bind, "127.0.0.1:8787".parse::<SocketAddr>().unwrap());
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
        assert_eq!(config.model, "gemini-3.1-pro");
        assert_eq!(config.data_dir, Config::default().data_dir);
    }

    #[test]
    fn every_field_round_trips() {
        let config = Config {
            data_dir: PathBuf::from("/srv/arc"),
            model: "gemini-3.1-pro".to_owned(),
            bind: "127.0.0.1:9000".parse().expect("valid address"),
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
    fn an_unparseable_bind_is_rejected_at_load() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("arc.toml");
        std::fs::write(&path, "bind = \"127.0.0.1\"\n").expect("write config");

        let err = Config::load(&path).expect_err("a portless address must not load");
        let chain = format!("{err:#}");
        assert!(chain.contains("parsing config"), "{chain}");
    }
}
