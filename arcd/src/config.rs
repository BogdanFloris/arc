use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use anyhow::{Context as _, Result, bail, ensure};
use arc_core::provider::Thinking;
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

    pub bind: SocketAddr,

    pub llama: LlamaConfig,

    pub max_tool_result_bytes: usize,

    pub consolidation: ConsolidationConfig,

    pub roles: RolesConfig,

    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    pub projects: BTreeMap<String, ProjectConfig>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct RolesConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub concierge: Option<RoleConfig>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub executor: Option<RoleConfig>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub archivist: Option<RoleConfig>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RoleConfig {
    pub provider: RoleProvider,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub endpoint: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key: Option<String>,

    #[serde(default)]
    pub thinking: Thinking,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RoleProvider {
    Local,
    #[serde(rename = "openai_compat")]
    OpenAiCompat,
    Gemini,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ProjectConfig {
    pub root: PathBuf,

    /// One line grounding what this project is, for `dispatch`'s schema.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub description: String,

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub read_only: Vec<PathBuf>,

    pub sources: Vec<ToolSource>,

    /// Wraps `bash`'s child invocation, e.g. `["nix", "develop", "-c"]`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub command_prefix: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolSource {
    Builtin,
    Workspace,
}

impl ToolSource {
    pub fn resolve(self) -> arc_core::tool::ToolSource {
        match self {
            ToolSource::Builtin => arc_core::tool::ToolSource::Builtin,
            ToolSource::Workspace => arc_core::tool::ToolSource::Workspace,
        }
    }
}

impl RolesConfig {
    fn configured(&self) -> impl Iterator<Item = (&'static str, &RoleConfig)> {
        [
            ("concierge", self.concierge.as_ref()),
            ("executor", self.executor.as_ref()),
            ("archivist", self.archivist.as_ref()),
        ]
        .into_iter()
        .filter_map(|(name, role)| role.map(|role| (name, role)))
    }
}

impl RoleConfig {
    fn validate(&self, name: &str) -> Result<()> {
        match self.provider {
            RoleProvider::Local => {
                ensure!(
                    self.endpoint.is_none(),
                    "role `{name}` runs on the sidecar, which owns its own endpoint"
                );
                ensure!(
                    self.key.is_none(),
                    "role `{name}` runs on the sidecar, which takes no key"
                );
            }
            RoleProvider::OpenAiCompat => ensure!(
                self.endpoint.is_some(),
                "role `{name}` needs an endpoint: openai_compat has no default"
            ),
            RoleProvider::Gemini => ensure!(
                self.key.is_some(),
                "role `{name}` needs a key: gemini has no unauthenticated endpoint"
            ),
        }
        if !matches!(self.provider, RoleProvider::Local) {
            ensure!(
                self.model.as_ref().is_some_and(|model| !model.is_empty()),
                "role `{name}` needs a model: only the sidecar can name its own"
            );
        }
        match (self.provider, self.thinking) {
            (_, Thinking::Default)
            | (RoleProvider::Gemini, _)
            | (RoleProvider::Local, Thinking::Minimal) => {}
            (RoleProvider::Local, level) => bail!(
                "role `{name}`: the sidecar reads `/no_think` out of the prompt and has no `{}` level; \
                 use `minimal` or leave it unset",
                level.label()
            ),
            (RoleProvider::OpenAiCompat, _) => bail!(
                "role `{name}`: no openai_compat endpoint is known to accept `reasoning_effort`, \
                 and an unknown field is a 400; measure one before configuring it"
            ),
        }
        if let Some(endpoint) = &self.endpoint {
            let trimmed = endpoint.trim_end_matches('/');
            ensure!(
                !trimmed.ends_with("/v1") && !trimmed.ends_with("/v1beta/openai/v1"),
                "role `{name}`: endpoint {endpoint} must not end in `/v1` — arcd appends the version and the path, \
                 so a published base URL of `https://host/x/v1` is configured as `https://host/x`"
            );
        }
        Ok(())
    }
}

impl ProjectConfig {
    fn validate(&self, name: &str) -> Result<()> {
        ensure!(
            self.root.is_absolute(),
            "project `{name}`: root {} must be an absolute path, and `~` is not expanded",
            self.root.display()
        );
        for grant in &self.read_only {
            ensure!(
                grant.is_absolute(),
                "project `{name}`: read-only grant {} must be an absolute path, and `~` is not expanded",
                grant.display()
            );
            if grant.starts_with(&self.root) || self.root.starts_with(grant) {
                bail!(
                    "project `{name}`: read-only grant {} overlaps the read-write root {}; grants are separate roots, not holes",
                    grant.display(),
                    self.root.display()
                );
            }
        }
        Ok(())
    }
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

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct LlamaConfig {
    pub server: PathBuf,

    pub model_file: PathBuf,

    /// The alias the sidecar serves under. Defaults to the GGUF's file stem.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,

    pub port: u16,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub device: Option<String>,

    pub args: Vec<String>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from(DEFAULT_DATA_DIR),
            bind: DEFAULT_BIND.parse().expect("default bind address is valid"),
            llama: LlamaConfig::default(),
            max_tool_result_bytes: DEFAULT_MAX_TOOL_RESULT_BYTES,
            consolidation: ConsolidationConfig::default(),
            roles: RolesConfig::default(),
            projects: BTreeMap::new(),
        }
    }
}

impl Default for LlamaConfig {
    fn default() -> Self {
        Self {
            server: PathBuf::from(DEFAULT_LLAMA_SERVER),
            model_file: PathBuf::from(DEFAULT_MODEL_FILE),
            model: None,
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
        let config: Self =
            toml::from_str(&text).with_context(|| format!("parsing config {}", path.display()))?;
        config
            .validate()
            .with_context(|| format!("in config {}", path.display()))?;
        Ok(config)
    }

    fn validate(&self) -> Result<()> {
        for (name, role) in self.roles.configured() {
            role.validate(name)?;
        }
        for (name, project) in &self.projects {
            project.validate(name)?;
        }
        Ok(())
    }

    pub fn model(&self) -> String {
        if let Some(model) = &self.llama.model {
            return model.clone();
        }
        self.llama.model_file.file_stem().map_or_else(
            || "local".to_owned(),
            |stem| stem.to_string_lossy().into_owned(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::{Config, ProjectConfig, RoleConfig, RoleProvider, RolesConfig, ToolSource};
    use arc_core::provider::Thinking;
    use std::collections::BTreeMap;
    use std::net::SocketAddr;
    use std::path::PathBuf;

    #[test]
    fn an_empty_file_is_the_defaults() {
        let config: Config = toml::from_str("").expect("empty config parses");
        assert_eq!(config, Config::default());
        assert_eq!(config.data_dir, PathBuf::from("data"));
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

        std::fs::write(&path, "[llama]\nmodel = \"qwen3-8b\"\n").expect("write config");
        let config = Config::load(&path).expect("present config loads");
        assert_eq!(config.model(), "qwen3-8b");
        assert_eq!(config.data_dir, Config::default().data_dir);
    }

    #[test]
    fn every_field_round_trips() {
        let config = Config {
            data_dir: PathBuf::from("/srv/arc"),
            bind: "127.0.0.1:9000".parse().expect("valid address"),
            llama: super::LlamaConfig {
                server: PathBuf::from("/opt/llama/llama-server"),
                model_file: PathBuf::from("/models/q.gguf"),
                model: Some("qwen".to_owned()),
                port: 9090,
                device: Some("RTX 5070".to_owned()),
                args: vec!["-ngl".to_owned(), "99".to_owned()],
            },
            max_tool_result_bytes: 512,
            consolidation: super::ConsolidationConfig {
                enabled: true,
                idle_seconds: 600,
                timeout_seconds: 120,
            },
            roles: RolesConfig {
                concierge: Some(RoleConfig {
                    provider: RoleProvider::Gemini,
                    model: Some("gemini-3.7-flash".to_owned()),
                    endpoint: None,
                    key: Some("gemini".to_owned()),
                    thinking: Thinking::Low,
                }),
                executor: Some(RoleConfig {
                    provider: RoleProvider::OpenAiCompat,
                    model: Some("deepseek-v4-pro".to_owned()),
                    endpoint: Some("http://127.0.0.1:4096".to_owned()),
                    key: Some("opencode-go".to_owned()),
                    thinking: Thinking::Default,
                }),
                archivist: Some(RoleConfig {
                    provider: RoleProvider::Local,
                    model: None,
                    endpoint: None,
                    key: None,
                    thinking: Thinking::Minimal,
                }),
            },
            projects: BTreeMap::from([(
                "arc".to_owned(),
                ProjectConfig {
                    root: PathBuf::from("/home/bogdan/arc"),
                    description: "ARC's own implementation repo".to_owned(),
                    read_only: vec![PathBuf::from("/home/bogdan/notes")],
                    sources: vec![ToolSource::Builtin, ToolSource::Workspace],
                    command_prefix: vec!["nix".to_owned(), "develop".to_owned(), "-c".to_owned()],
                },
            )]),
        };
        let text = toml::to_string(&config).expect("serializes");
        assert_eq!(toml::from_str::<Config>(&text).expect("parses"), config);
    }

    fn parse(text: &str) -> Config {
        let config: Config = toml::from_str(text).expect("parses");
        config.validate().expect("validates");
        config
    }

    fn rejected(text: &str) -> String {
        let config: Config = toml::from_str(text).expect("parses");
        config
            .validate()
            .expect_err("must not validate")
            .to_string()
    }

    #[test]
    fn an_empty_file_configures_no_roles_and_no_projects() {
        let config = parse("");
        assert_eq!(config.roles, RolesConfig::default());
        assert!(config.projects.is_empty());
    }

    #[test]
    fn each_role_resolves_to_a_provider_and_a_model() {
        let config = parse(
            r#"
[roles.concierge]
provider = "gemini"
model    = "gemini-3.7-flash"
key      = "gemini"

[roles.executor]
provider = "openai_compat"
model    = "deepseek-v4-pro"
endpoint = "http://127.0.0.1:4096"

[roles.archivist]
provider = "local"
"#,
        );

        let concierge = config.roles.concierge.expect("concierge is configured");
        assert_eq!(concierge.provider, RoleProvider::Gemini);
        assert_eq!(concierge.model.as_deref(), Some("gemini-3.7-flash"));
        let executor = config.roles.executor.expect("executor is configured");
        assert_eq!(executor.endpoint.as_deref(), Some("http://127.0.0.1:4096"));
        let archivist = config.roles.archivist.expect("archivist is configured");
        assert_eq!(archivist.provider, RoleProvider::Local);
        assert_eq!(archivist.model, None, "the sidecar names its own model");
    }

    #[test]
    fn a_role_that_is_not_a_role_is_rejected() {
        let err = toml::from_str::<Config>("[roles.hands]\nprovider = \"local\"\n")
            .expect_err("an unknown role must not load");
        assert!(err.to_string().contains("hands"), "{err}");
    }

    #[test]
    fn an_unknown_role_provider_is_rejected() {
        let err = toml::from_str::<Config>("[roles.executor]\nprovider = \"anthropic\"\n")
            .expect_err("an unconfigurable provider must not load");
        assert!(err.to_string().contains("anthropic"), "{err}");
    }

    #[test]
    fn a_hosted_role_without_a_model_is_rejected() {
        let err = rejected("[roles.concierge]\nprovider = \"gemini\"\nkey = \"gemini\"\n");
        assert!(err.contains("concierge") && err.contains("model"), "{err}");
    }

    #[test]
    fn a_gemini_role_without_a_key_is_rejected() {
        let err = rejected("[roles.concierge]\nprovider = \"gemini\"\nmodel = \"flash\"\n");
        assert!(err.contains("concierge") && err.contains("key"), "{err}");
    }

    #[test]
    fn an_endpoint_that_already_carries_the_version_is_rejected() {
        let err = rejected(
            "[roles.executor]\nprovider = \"openai_compat\"\nmodel = \"deepseek-v4-flash\"\nendpoint = \"https://opencode.ai/zen/go/v1\"\n",
        );
        assert!(err.contains("executor") && err.contains("/v1"), "{err}");
    }

    #[test]
    fn a_local_role_with_a_key_is_rejected() {
        let err = rejected("[roles.archivist]\nprovider = \"local\"\nkey = \"gemini\"\n");
        assert!(err.contains("archivist") && err.contains("key"), "{err}");
    }

    #[test]
    fn an_openai_compat_role_without_an_endpoint_is_rejected() {
        let err = rejected("[roles.executor]\nprovider = \"openai_compat\"\nmodel = \"glm-5.3\"\n");
        assert!(
            err.contains("executor") && err.contains("endpoint"),
            "{err}"
        );
    }

    #[test]
    fn a_local_role_with_an_endpoint_is_rejected() {
        let err = rejected(
            "[roles.archivist]\nprovider = \"local\"\nendpoint = \"http://127.0.0.1:8080/v1\"\n",
        );
        assert!(
            err.contains("archivist") && err.contains("sidecar"),
            "{err}"
        );
    }

    #[test]
    fn a_project_resolves_to_a_root_grants_and_sources() {
        let config = parse(
            r#"
[projects.arc]
root      = "/home/bogdan/arc"
read_only = ["/home/bogdan/notes"]
sources   = ["builtin", "workspace"]
"#,
        );

        let project = &config.projects["arc"];
        assert_eq!(project.root, PathBuf::from("/home/bogdan/arc"));
        assert_eq!(project.read_only, [PathBuf::from("/home/bogdan/notes")]);
        assert_eq!(
            project.sources,
            [ToolSource::Builtin, ToolSource::Workspace]
        );
    }

    #[test]
    fn a_project_description_is_optional_and_defaults_to_empty() {
        let config = parse(
            r#"
[projects.arc]
root    = "/home/bogdan/arc"
sources = []
"#,
        );
        assert_eq!(config.projects["arc"].description, "");
    }

    #[test]
    fn a_project_description_is_carried_through() {
        let config = parse(
            r#"
[projects.arc]
root        = "/home/bogdan/arc"
description = "ARC's own implementation repo"
sources     = []
"#,
        );
        assert_eq!(
            config.projects["arc"].description,
            "ARC's own implementation repo"
        );
    }

    #[test]
    fn a_project_command_prefix_is_optional_and_defaults_to_empty() {
        let config = parse(
            r#"
[projects.arc]
root    = "/home/bogdan/arc"
sources = []
"#,
        );
        assert!(config.projects["arc"].command_prefix.is_empty());
    }

    #[test]
    fn a_project_command_prefix_is_carried_through() {
        let config = parse(
            r#"
[projects.arc]
root           = "/home/bogdan/arc"
sources        = []
command_prefix = ["nix", "develop", "-c"]
"#,
        );
        assert_eq!(
            config.projects["arc"].command_prefix,
            ["nix", "develop", "-c"]
        );
    }

    #[test]
    fn an_unknown_tool_source_is_rejected() {
        let err = toml::from_str::<Config>(
            "[projects.arc]\nroot = \"/home/bogdan/arc\"\nsources = [\"mcp\"]\n",
        )
        .expect_err("a source that does not exist must not load");
        assert!(err.to_string().contains("mcp"), "{err}");
    }

    #[test]
    fn a_project_path_that_is_not_absolute_is_rejected() {
        let err = rejected("[projects.arc]\nroot = \"~/arc\"\nsources = []\n");
        assert!(err.contains("absolute"), "{err}");

        let err = rejected(
            "[projects.arc]\nroot = \"/home/bogdan/arc\"\nread_only = [\"notes\"]\nsources = []\n",
        );
        assert!(err.contains("absolute"), "{err}");
    }

    #[test]
    fn a_read_only_grant_inside_the_read_write_root_is_rejected() {
        let err = rejected(
            "[projects.arc]\nroot = \"/home/bogdan/arc\"\nread_only = [\"/home/bogdan/arc/data\"]\nsources = []\n",
        );
        assert!(err.contains("overlaps"), "{err}");
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
    fn a_top_level_provider_key_is_rejected_now_that_roles_choose_one() {
        let err = toml::from_str::<Config>("provider = \"local\"\n")
            .expect_err("the daemon-wide provider key is gone");
        assert!(err.to_string().contains("provider"), "{err}");
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
