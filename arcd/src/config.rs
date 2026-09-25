use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use anyhow::{Context as _, Result, bail, ensure};
use arc_core::provider::Thinking;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct Config {
    pub data_dir: PathBuf,
    pub bind: SocketAddr,
    pub llama: LlamaConfig,
    pub max_tool_result_bytes: usize,
    pub consolidation: ConsolidationConfig,
    pub compaction: CompactionConfig,
    pub roles: RolesConfig,

    /// Named presets a role may list in `choices`: what is possible. The
    /// selection among them is an event in the log, never a config edit.
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    pub models: BTreeMap<String, RoleConfig>,

    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    pub projects: BTreeMap<String, ProjectConfig>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct RolesConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub assistant: Option<RoleConfig>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub executor: Option<RoleConfig>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub archivist: Option<RoleConfig>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RoleConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub provider: Option<RoleProvider>,

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub choices: Vec<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub endpoint: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub key: Option<String>,

    #[serde(default)]
    pub thinking: Thinking,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub context_window: Option<u32>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub editing: Option<arc_core::tool::Editing>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RoleProvider {
    Local,
    #[serde(rename = "openai_compat")]
    OpenAiCompat,
    Gemini,
    Codex,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ProjectConfig {
    pub root: PathBuf,

    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub description: String,
}

impl RolesConfig {
    fn configured(&self) -> impl Iterator<Item = (&'static str, &RoleConfig)> {
        [
            ("assistant", self.assistant.as_ref()),
            ("executor", self.executor.as_ref()),
            ("archivist", self.archivist.as_ref()),
        ]
        .into_iter()
        .filter_map(|(name, role)| role.map(|role| (name, role)))
    }
}

impl RoleConfig {
    fn validate(&self, name: &str, models: &BTreeMap<String, RoleConfig>) -> Result<()> {
        if !self.choices.is_empty() {
            ensure!(
                self.provider.is_none()
                    && self.model.is_none()
                    && self.endpoint.is_none()
                    && self.key.is_none()
                    && self.context_window.is_none()
                    && self.thinking == Thinking::Default
                    && self.editing.is_none(),
                "role `{name}` lists choices, so it declares nothing else inline; the presets carry it"
            );
            for choice in &self.choices {
                ensure!(
                    models.contains_key(choice),
                    "role `{name}` chooses `{choice}`, which is not a `[models.{choice}]` preset"
                );
                ensure!(
                    self.choices.iter().filter(|it| *it == choice).count() == 1,
                    "role `{name}` lists `{choice}` twice"
                );
            }
            return Ok(());
        }
        let Some(provider) = self.provider else {
            bail!("role `{name}` needs a provider, or a list of choices");
        };
        match provider {
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
            RoleProvider::Codex => ensure!(
                self.key.is_some(),
                "role `{name}` needs a key naming the credential file `arcd login codex` writes"
            ),
        }
        if !matches!(provider, RoleProvider::Local) {
            ensure!(
                self.model.as_ref().is_some_and(|model| !model.is_empty()),
                "role `{name}` needs a model: only the sidecar can name its own"
            );
        }
        match (provider, self.thinking) {
            (_, Thinking::Default)
            | (RoleProvider::Gemini | RoleProvider::OpenAiCompat | RoleProvider::Codex, _)
            | (RoleProvider::Local, Thinking::Minimal) => {}
            (RoleProvider::Local, level) => bail!(
                "role `{name}`: the sidecar reads `/no_think` out of the prompt and has no `{}` level; \
                 use `minimal` or leave it unset",
                level.label()
            ),
        }
        if let Some(endpoint) = &self.endpoint {
            let trimmed = endpoint.trim_end_matches('/');
            ensure!(
                !trimmed.ends_with("/v1"),
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
            idle_seconds: 1800,
            timeout_seconds: 300,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct CompactionConfig {
    pub fraction: f32,
}

impl Default for CompactionConfig {
    fn default() -> Self {
        Self { fraction: 0.8 }
    }
}

impl CompactionConfig {
    fn validate(self) -> Result<()> {
        ensure!(
            self.fraction > 0.0 && self.fraction <= 1.0,
            "compaction fraction {} must be in (0, 1]",
            self.fraction
        );
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct LlamaConfig {
    pub server: PathBuf,
    pub model_file: PathBuf,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    pub port: u16,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub device: Option<String>,
    pub args: Vec<String>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from("data"),
            bind: SocketAddr::from(([127, 0, 0, 1], 8787)),
            llama: LlamaConfig::default(),
            max_tool_result_bytes: 32 * 1024,
            consolidation: ConsolidationConfig::default(),
            compaction: CompactionConfig::default(),
            roles: RolesConfig::default(),
            models: BTreeMap::new(),
            projects: BTreeMap::new(),
        }
    }
}

impl Default for LlamaConfig {
    fn default() -> Self {
        Self {
            server: PathBuf::from("llama-server"),
            model_file: PathBuf::from("data/models/Qwen3-8B-Q4_K_M.gguf"),
            model: None,
            port: 8080,
            device: None,
            args: Vec::new(),
        }
    }
}

impl Config {
    pub fn needs_sidecar(&self) -> bool {
        [
            self.roles.assistant.as_ref(),
            self.roles.executor.as_ref(),
            self.roles.archivist.as_ref(),
        ]
        .into_iter()
        .any(|role| match role {
            None => true,
            Some(role) if role.choices.is_empty() => role.provider == Some(RoleProvider::Local),
            Some(role) => role.choices.iter().any(|name| {
                self.models
                    .get(name)
                    .is_some_and(|preset| preset.provider == Some(RoleProvider::Local))
            }),
        })
    }

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
        self.compaction.validate()?;
        for (name, preset) in &self.models {
            ensure!(
                preset.choices.is_empty(),
                "preset `models.{name}` cannot itself list choices"
            );
            preset.validate(&format!("models.{name}"), &BTreeMap::new())?;
        }
        for (name, role) in self.roles.configured() {
            role.validate(name, &self.models)?;
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
    use super::{Config, RoleProvider};
    use arc_core::provider::Thinking;
    use std::path::PathBuf;

    #[test]
    fn hosted_roles_skip_the_sidecar_but_local_choices_and_defaults_need_it() {
        let mut config: Config = toml::from_str(
            r#"
[models.hosted]
provider = "gemini"
model = "test"
key = "test"
[models.local]
provider = "local"
[roles.assistant]
choices = ["hosted"]
[roles.executor]
choices = ["hosted"]
[roles.archivist]
choices = ["hosted"]
"#,
        )
        .expect("config");
        config.validate().expect("valid");
        assert!(!config.needs_sidecar());
        config.roles.assistant = Some(config.models["local"].clone());
        assert!(config.needs_sidecar());
        config.roles.assistant = Some(config.models["hosted"].clone());
        assert!(!config.needs_sidecar());
        config
            .roles
            .executor
            .as_mut()
            .unwrap()
            .choices
            .push("local".to_owned());
        assert!(config.needs_sidecar());
        config.roles.executor.as_mut().unwrap().choices.pop();
        config.roles.archivist = Some(config.models["local"].clone());
        assert!(config.needs_sidecar());
        config.roles.archivist = None;
        assert!(config.needs_sidecar());
        assert!(Config::default().needs_sidecar());
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
    fn a_role_may_choose_among_presets_and_the_menu_is_checked() {
        let config = parse(
            r#"
[roles.executor]
choices = ["flash", "sol"]

[models.flash]
provider = "openai_compat"
model    = "deepseek-v4-flash"
endpoint = "https://opencode.ai/zen/go"
key      = "opencode-go"

[models.sol]
provider = "codex"
model    = "gpt-5.6-sol"
key      = "codex"
thinking = "high"
"#,
        );
        let executor = config.roles.executor.expect("configured");
        assert_eq!(executor.choices, ["flash", "sol"]);
        assert_eq!(executor.provider, None);
        assert_eq!(config.models["sol"].thinking, Thinking::High);

        let err = rejected("[roles.executor]\nchoices = [\"nope\"]\n");
        assert!(
            err.contains("nope") && err.contains("[models.nope]"),
            "{err}"
        );

        let err = rejected(
            r#"
[roles.executor]
choices  = ["flash"]
provider = "local"

[models.flash]
provider = "local"
"#,
        );
        assert!(err.contains("nothing else inline"), "{err}");

        let err = rejected(
            r#"
[roles.executor]
choices = ["flash", "flash"]

[models.flash]
provider = "local"
"#,
        );
        assert!(err.contains("twice"), "{err}");

        let err = rejected("[models.bad]\nchoices = [\"x\"]\n");
        assert!(
            err.contains("models.bad") && err.contains("choices"),
            "{err}"
        );

        let err = rejected("[models.bad]\nprovider = \"gemini\"\nkey = \"gemini\"\n");
        assert!(err.contains("models.bad") && err.contains("model"), "{err}");

        let err = rejected("[roles.executor]\nmodel = \"x\"\n");
        assert!(err.contains("provider, or a list of choices"), "{err}");
    }

    #[test]
    fn a_codex_role_parses_and_needs_a_credential_name() {
        let config = parse(
            "[roles.executor]\nprovider = \"codex\"\nmodel = \"gpt-5.5\"\nkey = \"codex\"\nthinking = \"medium\"\n",
        );
        let executor = config.roles.executor.expect("configured");
        assert_eq!(executor.provider, Some(RoleProvider::Codex));
        assert_eq!(executor.endpoint, None, "the backend has a default");

        let err = rejected("[roles.executor]\nprovider = \"codex\"\nmodel = \"gpt-5.5\"\n");
        assert!(
            err.contains("executor") && err.contains("arcd login codex"),
            "{err}"
        );
    }

    #[test]
    fn an_endpoint_that_already_carries_the_version_is_rejected() {
        let err = rejected(
            "[roles.executor]\nprovider = \"openai_compat\"\nmodel = \"deepseek-v4-flash\"\nendpoint = \"https://opencode.ai/zen/go/v1\"\n",
        );
        assert!(err.contains("executor") && err.contains("/v1"), "{err}");
    }

    #[test]
    fn a_project_contains_only_context() {
        let config = parse(
            r#"
[projects.arc]
root      = "/home/bogdan/arc"
description = "ARC implementation"

[projects.scratch]
root = "/tmp"
description = "Scratch workspace"
"#,
        );

        let serialized = toml::to_string(&config).expect("serializes");
        assert_eq!(parse(&serialized), config);
        let project = &config.projects["arc"];
        assert_eq!(project.description, "ARC implementation");
        assert_eq!(config.projects["scratch"].description, "Scratch workspace");
        assert_eq!(project.root, PathBuf::from("/home/bogdan/arc"));
    }

    #[test]
    fn a_project_path_that_is_not_absolute_is_rejected() {
        let err = rejected("[projects.arc]\nroot = \"~/arc\"\n");
        assert!(err.contains("absolute"), "{err}");
    }

    #[test]
    fn an_unknown_key_is_rejected() {
        let err = toml::from_str::<Config>("modle = \"gemini-3.1-pro\"\n")
            .expect_err("a typo must not be ignored");
        assert!(err.to_string().contains("modle"), "{err}");
    }

    #[test]
    fn compaction_fraction_must_be_in_zero_to_one() {
        let err = rejected("[compaction]\nfraction = 0.0\n");
        assert!(err.contains("(0, 1]"), "{err}");

        let err = rejected("[compaction]\nfraction = 1.5\n");
        assert!(err.contains("(0, 1]"), "{err}");

        // the upper bound is inclusive
        parse("[compaction]\nfraction = 1.0\n");
    }
}
