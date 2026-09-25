use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use anyhow::{Context as _, Result};
use arc_core::provider::codex::{self, Codex};
use arc_core::provider::gemini::Gemini;
use arc_core::provider::openai::OpenAiCompat;
use arc_core::provider::sidecar::Sidecar;
use arc_core::provider::{Provider, Thinking, gemini, role_label};
use arc_core::secrets::Secrets;
use arc_core::session::{ModelChoice, Runner};
use arc_proto::v1::SessionRole;

use crate::config::{Config, RoleConfig, RoleProvider};

#[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
fn compact_at_for(context_window: u32, fraction: f32) -> u32 {
    (f64::from(context_window) * f64::from(fraction)) as u32
}

#[derive(Debug)]
pub struct Roles {
    chat: Vec<(String, Runner)>,
    executor: Vec<(String, Runner)>,
    archivist: Vec<(String, Runner)>,
}

impl Roles {
    pub fn resolve(
        config: &Config,
        sidecar_endpoint: &str,
        secrets: &Secrets,
        identity: Option<String>,
    ) -> Result<Self> {
        let mut built = Built::new(sidecar_endpoint, secrets);
        Ok(Self {
            chat: built.role(
                SessionRole::Chat,
                config.roles.assistant.as_ref(),
                config,
                identity,
            )?,
            executor: built.role(
                SessionRole::Executor,
                config.roles.executor.as_ref(),
                config,
                None,
            )?,
            archivist: built.role(
                SessionRole::Archivist,
                config.roles.archivist.as_ref(),
                config,
                None,
            )?,
        })
    }

    pub fn chat(&self) -> &Runner {
        &self.chat[0].1
    }

    pub fn executor(&self) -> &Runner {
        &self.executor[0].1
    }

    pub fn archivist(&self) -> &Runner {
        &self.archivist[0].1
    }

    pub fn all(&self) -> [&Runner; 3] {
        [self.chat(), self.executor(), self.archivist()]
    }

    pub fn menus(&self) -> BTreeMap<SessionRole, Vec<(String, Runner)>> {
        BTreeMap::from([
            (SessionRole::Chat, self.chat.clone()),
            (SessionRole::Executor, self.executor.clone()),
            (SessionRole::Archivist, self.archivist.clone()),
        ])
    }

    pub fn choices(&self) -> BTreeMap<SessionRole, Vec<ModelChoice>> {
        self.menus()
            .into_iter()
            .map(|(role, menu)| {
                (
                    role,
                    menu.into_iter()
                        .map(|(name, runner)| ModelChoice {
                            name,
                            provider: runner.provider.name().to_owned(),
                            model: runner.model,
                            thinking: runner.thinking,
                            editing: runner.editing,
                        })
                        .collect(),
                )
            })
            .collect()
    }
}

struct Built<'a> {
    sidecar_endpoint: &'a str,
    secrets: &'a Secrets,
    providers: HashMap<Client, Arc<dyn Provider>>,
}

#[derive(PartialEq, Eq, Hash)]
struct Client {
    kind: RoleProvider,
    endpoint: String,
    key: Option<String>,
}

impl<'a> Built<'a> {
    fn new(sidecar_endpoint: &'a str, secrets: &'a Secrets) -> Self {
        Self {
            sidecar_endpoint,
            secrets,
            providers: HashMap::new(),
        }
    }

    fn role(
        &mut self,
        role: SessionRole,
        configured: Option<&RoleConfig>,
        config: &Config,
        system: Option<String>,
    ) -> Result<Vec<(String, Runner)>> {
        let name = role_label(role);
        let Some(configured) = configured else {
            let runner = Runner {
                role,
                provider: self.sidecar(),
                model: config.model(),
                thinking: Thinking::Default,
                system,
                compact_at: None,
                context_window: None,
                editing: arc_core::tool::Editing::Replacement,
            };
            return Ok(vec![(runner.model.clone(), runner)]);
        };
        if configured.choices.is_empty() {
            let runner = self.runner(role, name, configured, config, system)?;
            return Ok(vec![(runner.model.clone(), runner)]);
        }
        let mut menu = Vec::with_capacity(configured.choices.len());
        for (position, choice) in configured.choices.iter().enumerate() {
            let preset = config
                .models
                .get(choice)
                .expect("config validation requires every choice to be a preset");
            match self.runner(role, choice, preset, config, system.clone()) {
                Ok(runner) => menu.push((choice.clone(), runner)),
                Err(error) if position == 0 => {
                    return Err(
                        error.context(format!("the `{name}` role's default choice `{choice}`"))
                    );
                }
                Err(error) => {
                    tracing::warn!(role = name, choice, error = %error, "a model choice is unavailable");
                }
            }
        }
        Ok(menu)
    }

    fn runner(
        &mut self,
        role: SessionRole,
        name: &str,
        configured: &RoleConfig,
        config: &Config,
        system: Option<String>,
    ) -> Result<Runner> {
        let thinking = configured.thinking;
        let compact_at = configured
            .context_window
            .map(|window| compact_at_for(window, config.compaction.fraction));
        let (provider, model) = self.provider_for(name, configured, config)?;
        let editing = configured
            .editing
            .unwrap_or_else(|| arc_core::tool::Editing::for_provider(provider.name()));
        Ok(Runner {
            role,
            provider,
            model,
            thinking,
            system,
            compact_at,
            context_window: configured.context_window,
            editing,
        })
    }

    fn provider_for(
        &mut self,
        name: &str,
        configured: &RoleConfig,
        config: &Config,
    ) -> Result<(Arc<dyn Provider>, String)> {
        let key = configured.key.clone();
        let provider = configured
            .provider
            .expect("config validation requires a provider on an inline role or preset");
        Ok(match provider {
            RoleProvider::Local => (
                self.sidecar(),
                configured.model.clone().unwrap_or_else(|| config.model()),
            ),
            RoleProvider::OpenAiCompat => {
                let endpoint = configured
                    .endpoint
                    .clone()
                    .expect("config validation requires an endpoint for openai_compat");
                (
                    self.shared(name, RoleProvider::OpenAiCompat, endpoint, key)?,
                    configured
                        .model
                        .clone()
                        .expect("config validation requires a model for openai_compat"),
                )
            }
            RoleProvider::Gemini => {
                let endpoint = configured
                    .endpoint
                    .clone()
                    .unwrap_or_else(|| gemini::DEFAULT_ENDPOINT.to_owned());
                (
                    self.shared(name, RoleProvider::Gemini, endpoint, key)?,
                    configured
                        .model
                        .clone()
                        .expect("config validation requires a model for gemini"),
                )
            }
            RoleProvider::Codex => {
                let endpoint = configured
                    .endpoint
                    .clone()
                    .unwrap_or_else(|| codex::DEFAULT_ENDPOINT.to_owned());
                (
                    self.shared(name, RoleProvider::Codex, endpoint, key)?,
                    configured
                        .model
                        .clone()
                        .expect("config validation requires a model for codex"),
                )
            }
        })
    }

    fn sidecar(&mut self) -> Arc<dyn Provider> {
        self.shared(
            "sidecar",
            RoleProvider::Local,
            self.sidecar_endpoint.to_owned(),
            None,
        )
        .expect("the sidecar takes no key, so nothing can be read")
    }

    fn shared(
        &mut self,
        name: &str,
        kind: RoleProvider,
        endpoint: String,
        key: Option<String>,
    ) -> Result<Arc<dyn Provider>> {
        let client = Client {
            kind,
            endpoint,
            key,
        };
        if let Some(built) = self.providers.get(&client) {
            return Ok(Arc::clone(built));
        }

        // the codex credential is a token file the provider keeps current, not a key to copy
        if kind == RoleProvider::Codex {
            let credential = client
                .key
                .as_deref()
                .expect("config validation requires a credential name for codex");
            let provider: Arc<dyn Provider> = Arc::new(
                Codex::open(&client.endpoint, self.secrets.clone(), credential)
                    .with_context(|| format!("the credential for the `{name}` role"))?,
            );
            self.providers.insert(client, Arc::clone(&provider));
            return Ok(provider);
        }

        let key = client
            .key
            .as_deref()
            .map(|secret| {
                self.secrets
                    .read(secret)
                    .with_context(|| format!("the key for the `{name}` role"))
            })
            .transpose()?;
        let provider: Arc<dyn Provider> = match kind {
            RoleProvider::Local => Arc::new(Sidecar::new(&client.endpoint)),
            RoleProvider::OpenAiCompat => Arc::new(match key {
                Some(key) => OpenAiCompat::keyed(&client.endpoint, key),
                None => OpenAiCompat::new(&client.endpoint),
            }),
            RoleProvider::Gemini => Arc::new(Gemini::new(
                &client.endpoint,
                key.expect("config validation requires a key for gemini"),
            )),
            RoleProvider::Codex => unreachable!("codex is built above"),
        };
        self.providers.insert(client, Arc::clone(&provider));
        Ok(provider)
    }
}

#[cfg(test)]
mod tests {
    use super::Roles;
    use crate::config::Config;
    use arc_core::secrets::Secrets;
    use std::os::unix::fs::PermissionsExt as _;

    const SIDECAR: &str = "http://127.0.0.1:8080";

    fn resolved(text: &str) -> Roles {
        let dir = tempfile::tempdir().expect("temp dir");
        with_secrets(text, dir.path(), &[]).expect("resolves")
    }

    fn with_secrets(
        text: &str,
        dir: &std::path::Path,
        keys: &[(&str, &str)],
    ) -> anyhow::Result<Roles> {
        for (name, body) in keys {
            let path = dir.join(name);
            std::fs::write(&path, body).expect("write secret");
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).expect("chmod");
        }
        let config: Config = toml::from_str(text).expect("parses");
        Roles::resolve(&config, SIDECAR, &Secrets::new(dir), None)
    }

    #[test]
    fn only_the_assistant_has_identity() {
        let dir = tempfile::tempdir().expect("temp dir");
        let config: Config = toml::from_str("").expect("parses");
        let roles = Roles::resolve(
            &config,
            SIDECAR,
            &Secrets::new(dir.path()),
            Some("You are ARC.\n".to_owned()),
        )
        .expect("resolves");

        let system = roles.chat().system.as_deref().expect("a system");
        assert_eq!(system, "You are ARC.\n");
        assert_eq!(roles.executor().system, None);
        assert_eq!(roles.archivist().system, None);
    }

    #[test]
    fn assistant_and_executor_select_independently() {
        use arc_proto::v1::SessionRole;

        let worker = r#"
[models.sol]
provider = "local"
model = "sol"
[models.astra]
provider = "local"
model = "astra"
[roles.assistant]
choices = ["astra"]
[roles.executor]
choices = ["sol", "astra"]
"#;
        let roles = resolved(worker);
        assert_eq!(roles.menus()[&SessionRole::Chat][0].1.model, "astra");
        assert_eq!(roles.menus()[&SessionRole::Executor][0].1.model, "sol");
        assert_eq!(roles.all().len(), 3);
    }

    #[test]
    fn model_preset_overrides_the_provider_editing_default() {
        let roles = resolved(
            "[models.patch_local]\nprovider = \"local\"\nediting = \"patch\"\n\
             [roles.executor]\nchoices = [\"patch_local\"]\n",
        );
        assert_eq!(roles.executor().editing, arc_core::tool::Editing::Patch);
        assert_eq!(
            roles.choices()[&arc_proto::v1::SessionRole::Executor][0].editing,
            arc_core::tool::Editing::Patch
        );
    }

    #[test]
    fn roles_on_one_endpoint_share_one_provider() {
        let roles = resolved(
            r#"
[roles.assistant]
provider = "openai_compat"
model    = "deepseek-v4-flash"
endpoint = "http://127.0.0.1:4096"

[roles.executor]
provider = "openai_compat"
model    = "deepseek-v4-pro"
endpoint = "http://127.0.0.1:4096"
"#,
        );

        assert!(
            std::sync::Arc::ptr_eq(&roles.chat().provider, &roles.executor().provider),
            "one endpoint, one client"
        );
        assert!(
            !std::sync::Arc::ptr_eq(&roles.chat().provider, &roles.archivist().provider),
            "the sidecar is a different endpoint"
        );
    }

    #[test]
    fn one_endpoint_with_two_keys_gets_two_clients() {
        let dir = tempfile::tempdir().expect("temp dir");
        let roles = with_secrets(
            r#"
[roles.assistant]
provider = "openai_compat"
model    = "grok-4.5"
endpoint = "https://shared.example/v1"
key      = "personal"

[roles.executor]
provider = "openai_compat"
model    = "deepseek-v4-pro"
endpoint = "https://shared.example/v1"
key      = "work"
"#,
            dir.path(),
            &[("personal", "sk-a"), ("work", "sk-b")],
        )
        .expect("resolves");

        assert!(
            !std::sync::Arc::ptr_eq(&roles.chat().provider, &roles.executor().provider),
            "same endpoint, different keys: they must not share a client"
        );
    }

    #[test]
    fn a_codex_role_opens_its_credential_and_a_missing_one_names_the_login() {
        let dir = tempfile::tempdir().expect("temp dir");
        let config = r#"
[roles.executor]
provider = "codex"
model    = "gpt-5.5"
key      = "codex"
"#;

        let err = with_secrets(config, dir.path(), &[]).expect_err("no credential yet");
        let chain = format!("{err:#}");
        assert!(
            chain.contains("executor") && chain.contains("arcd login codex"),
            "{chain}"
        );

        // an unsigned JWT whose claims name chatgpt_account_id acct_1
        let token = "eyJhbGciOiJub25lIn0.eyJodHRwczovL2FwaS5vcGVuYWkuY29tL2F1dGgiOnsiY2hhdGdwdF9hY2NvdW50X2lkIjoiYWNjdF8xIn19.sig";
        let credential = serde_json::json!({
            "access_token": token,
            "refresh_token": "rt",
            "expires_at": u64::MAX,
        })
        .to_string();
        let roles = with_secrets(config, dir.path(), &[("codex", &credential)])
            .expect("the credential is there now");
        assert_eq!(roles.executor().provider.name(), "codex");
        assert_eq!(
            roles.executor().provider.endpoint(),
            arc_core::provider::codex::DEFAULT_ENDPOINT
        );
        let rendered = format!("{roles:?}");
        assert!(
            !rendered.contains("eyJhbGciOiJub25lIn0"),
            "a token reached a Debug line: {rendered}"
        );
    }

    #[test]
    fn a_role_with_choices_runs_the_first_and_only_warns_about_the_rest() {
        let dir = tempfile::tempdir().expect("temp dir");
        let config = r#"
[roles.executor]
choices = ["flash", "sol"]

[models.flash]
provider       = "openai_compat"
model          = "deepseek-v4-flash"
endpoint       = "https://opencode.example"
key            = "opencode-go"
thinking       = "low"
context_window = 100000

[models.sol]
provider = "codex"
model    = "gpt-5.6-sol"
key      = "codex"

"#;

        let roles = with_secrets(config, dir.path(), &[("opencode-go", "sk-go")])
            .expect("the default choice resolves; the codex credential is only missing");
        let executor = roles.executor();
        assert_eq!(executor.provider.name(), "openai-compat");
        assert_eq!(executor.model, "deepseek-v4-flash");
        assert_eq!(executor.thinking, arc_core::provider::Thinking::Low);
        assert_eq!(executor.compact_at, Some(80_000));
        let menu = roles.menus();
        assert_eq!(
            menu[&arc_proto::v1::SessionRole::Executor]
                .iter()
                .map(|(name, _)| name.as_str())
                .collect::<Vec<_>>(),
            ["flash"],
            "the unavailable choice is left off the menu"
        );
        assert_eq!(
            roles.choices()[&arc_proto::v1::SessionRole::Chat][0].name,
            Config::default().model(),
            "a role without choices is a one-entry menu named by its model"
        );

        let empty = tempfile::tempdir().expect("temp dir");
        let err =
            with_secrets(config, empty.path(), &[]).expect_err("the default's key is missing");
        assert!(format!("{err:#}").contains("opencode-go"), "{err:#}");
    }
}
