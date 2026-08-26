use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Context as _, Result};
use arc_core::provider::gemini::Gemini;
use arc_core::provider::openai::OpenAiCompat;
use arc_core::provider::sidecar::Sidecar;
use arc_core::provider::{Provider, Thinking, gemini, role_label};
use arc_core::secrets::Secrets;
use arc_core::session::Runner;
use arc_proto::v1::SessionRole;

use crate::config::{Config, RoleConfig, RoleProvider};

#[derive(Debug)]
pub struct Roles {
    concierge: Runner,
    executor: Runner,
    archivist: Runner,
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
            // the identity file loads for the concierge and nowhere else
            concierge: built.role(
                SessionRole::Concierge,
                config.roles.concierge.as_ref(),
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

    pub fn concierge(&self) -> &Runner {
        &self.concierge
    }

    pub fn executor(&self) -> &Runner {
        &self.executor
    }

    pub fn archivist(&self) -> &Runner {
        &self.archivist
    }

    pub fn all(&self) -> [&Runner; 3] {
        [&self.concierge, &self.executor, &self.archivist]
    }
}

struct Built<'a> {
    sidecar_endpoint: &'a str,
    secrets: &'a Secrets,
    providers: HashMap<Client, Arc<dyn Provider>>,
}

// two roles on one endpoint share a client only if they also share a key
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
    ) -> Result<Runner> {
        let name = role_label(role);
        let Some(configured) = configured else {
            return Ok(Runner {
                role,
                provider: self.sidecar(),
                model: config.model(),
                thinking: Thinking::Default,
                system,
            });
        };
        let thinking = configured.thinking;
        let key = configured.key.clone();
        let (provider, model) = match configured.provider {
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
        };
        Ok(Runner {
            role,
            provider,
            model,
            thinking,
            system,
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
    fn an_unconfigured_role_falls_back_to_the_sidecar() {
        let roles = resolved("");

        for role in [roles.concierge(), roles.executor(), roles.archivist()] {
            assert_eq!(role.provider.name(), "local");
            assert_eq!(role.provider.endpoint(), SIDECAR);
            assert_eq!(role.model, Config::default().model());
        }
    }

    #[test]
    fn each_role_resolves_to_its_own_provider_and_model() {
        let roles = resolved(
            r#"
[roles.concierge]
provider = "openai_compat"
model    = "deepseek-v4-flash"
endpoint = "http://127.0.0.1:4096"

[roles.executor]
provider = "openai_compat"
model    = "deepseek-v4-pro"
endpoint = "http://127.0.0.1:4096"

[roles.archivist]
provider = "local"
"#,
        );

        assert_eq!(roles.concierge().model, "deepseek-v4-flash");
        assert_eq!(roles.executor().model, "deepseek-v4-pro");
        assert_eq!(roles.concierge().provider.name(), "openai-compat");
        assert_eq!(roles.archivist().provider.name(), "local");
        assert_eq!(roles.archivist().model, Config::default().model());
    }

    #[test]
    fn roles_on_one_endpoint_share_one_provider() {
        let roles = resolved(
            r#"
[roles.concierge]
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
            std::sync::Arc::ptr_eq(&roles.concierge().provider, &roles.executor().provider),
            "one endpoint, one client"
        );
        assert!(
            !std::sync::Arc::ptr_eq(&roles.concierge().provider, &roles.archivist().provider),
            "the sidecar is a different endpoint"
        );
    }

    #[test]
    fn a_local_role_may_name_a_model_the_sidecar_was_started_with() {
        let roles = resolved("[roles.archivist]\nprovider = \"local\"\nmodel = \"qwen3-8b\"\n");

        assert_eq!(roles.archivist().model, "qwen3-8b");
        assert_eq!(roles.archivist().provider.endpoint(), SIDECAR);
    }

    #[test]
    fn a_keyed_role_reads_its_key_and_a_missing_one_names_the_role() {
        let dir = tempfile::tempdir().expect("temp dir");
        let config = r#"
[roles.executor]
provider = "openai_compat"
model    = "deepseek-v4-pro"
endpoint = "https://opencode.example/v1"
key      = "opencode-go"
"#;

        let err = with_secrets(config, dir.path(), &[]).expect_err("the key is not there");
        let chain = format!("{err:#}");
        assert!(chain.contains("opencode-go"), "{chain}");

        let roles = with_secrets(config, dir.path(), &[("opencode-go", "sk-go-123\n")])
            .expect("the key is there now");
        assert_eq!(roles.executor().provider.name(), "openai-compat");
    }

    #[test]
    fn one_endpoint_with_two_keys_gets_two_clients() {
        let dir = tempfile::tempdir().expect("temp dir");
        let roles = with_secrets(
            r#"
[roles.concierge]
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
            !std::sync::Arc::ptr_eq(&roles.concierge().provider, &roles.executor().provider),
            "same endpoint, different keys: they must not share a client"
        );
    }

    #[test]
    fn a_key_never_appears_in_debug_output() {
        let dir = tempfile::tempdir().expect("temp dir");
        let roles = with_secrets(
            r#"
[roles.executor]
provider = "openai_compat"
model    = "deepseek-v4-pro"
endpoint = "https://opencode.example/v1"
key      = "opencode-go"
"#,
            dir.path(),
            &[("opencode-go", "sk-supersecret-value")],
        )
        .expect("resolves");

        let rendered = format!("{roles:?}");
        assert!(
            !rendered.contains("sk-supersecret-value"),
            "a key reached a Debug line: {rendered}"
        );
        assert!(rendered.contains("redacted"), "{rendered}");
    }

    #[test]
    fn a_gemini_role_resolves_to_its_own_provider_on_the_published_endpoint() {
        let dir = tempfile::tempdir().expect("temp dir");
        let roles = with_secrets(
            r#"
[roles.concierge]
provider = "gemini"
model    = "gemini-3.7-flash"
key      = "gemini"
"#,
            dir.path(),
            &[("gemini", "sk-gemini")],
        )
        .expect("resolves");

        let concierge = roles.concierge();
        assert_eq!(concierge.provider.name(), "gemini");
        assert_eq!(concierge.model, "gemini-3.7-flash");
        assert_eq!(
            concierge.provider.endpoint(),
            arc_core::provider::gemini::DEFAULT_ENDPOINT
        );
    }

    #[test]
    fn a_gemini_role_without_its_key_names_the_role_that_wanted_it() {
        let dir = tempfile::tempdir().expect("temp dir");

        let err = with_secrets(
            "[roles.concierge]\nprovider = \"gemini\"\nmodel = \"flash\"\nkey = \"gemini\"\n",
            dir.path(),
            &[],
        )
        .expect_err("no key, no provider");

        let chain = format!("{err:#}");
        assert!(
            chain.contains("concierge") && chain.contains("gemini"),
            "{chain}"
        );
    }
}
