use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Context as _, Result, bail};
use arc_core::provider::any::AnyProvider;
use arc_core::secrets::Secrets;

use crate::config::{Config, RoleConfig, RoleProvider};

#[derive(Clone, Debug)]
pub struct Resolved {
    pub provider: Arc<AnyProvider>,
    pub model: String,
}

#[derive(Debug)]
pub struct Roles {
    concierge: Resolved,
    executor: Resolved,
    archivist: Resolved,
}

impl Roles {
    pub fn resolve(config: &Config, sidecar_endpoint: &str, secrets: &Secrets) -> Result<Self> {
        let mut built = Built::new(sidecar_endpoint, secrets);
        Ok(Self {
            concierge: built.role("concierge", config.roles.concierge.as_ref(), config)?,
            executor: built.role("executor", config.roles.executor.as_ref(), config)?,
            archivist: built.role("archivist", config.roles.archivist.as_ref(), config)?,
        })
    }

    pub fn concierge(&self) -> &Resolved {
        &self.concierge
    }

    pub fn executor(&self) -> &Resolved {
        &self.executor
    }

    pub fn archivist(&self) -> &Resolved {
        &self.archivist
    }
}

struct Built<'a> {
    sidecar_endpoint: &'a str,
    secrets: &'a Secrets,
    providers: HashMap<Client, Arc<AnyProvider>>,
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

    fn role(&mut self, name: &str, role: Option<&RoleConfig>, config: &Config) -> Result<Resolved> {
        let Some(role) = role else {
            return Ok(Resolved {
                provider: self.sidecar(),
                model: config.model(),
            });
        };
        match role.provider {
            RoleProvider::Local => Ok(Resolved {
                provider: self.sidecar(),
                model: role.model.clone().unwrap_or_else(|| config.model()),
            }),
            RoleProvider::OpenAiCompat => {
                let endpoint = role
                    .endpoint
                    .clone()
                    .expect("config validation requires an endpoint for openai_compat");
                Ok(Resolved {
                    provider: self.shared(
                        RoleProvider::OpenAiCompat,
                        endpoint,
                        role.key.clone(),
                    )?,
                    model: role
                        .model
                        .clone()
                        .expect("config validation requires a model for openai_compat"),
                })
            }
            RoleProvider::Gemini => bail!(
                "role `{name}` is configured for gemini, which has no provider yet; \
                 it echoes a per-call thought_signature the OpenAI-compatible path cannot carry"
            ),
        }
    }

    fn sidecar(&mut self) -> Arc<AnyProvider> {
        self.shared(RoleProvider::Local, self.sidecar_endpoint.to_owned(), None)
            .expect("the sidecar takes no key, so nothing can be read")
    }

    fn shared(
        &mut self,
        kind: RoleProvider,
        endpoint: String,
        key: Option<String>,
    ) -> Result<Arc<AnyProvider>> {
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
            .map(|name| {
                self.secrets
                    .read(name)
                    .with_context(|| format!("the key for a {kind:?} role"))
            })
            .transpose()?;
        let provider = Arc::new(match kind {
            RoleProvider::Local => AnyProvider::local(&client.endpoint),
            RoleProvider::OpenAiCompat => AnyProvider::openai_compat(&client.endpoint, key),
            // 3.3 adds the variant; this match makes it a compile error, not a silent fallback
            RoleProvider::Gemini => {
                unreachable!("a gemini role is rejected before a provider is built")
            }
        });
        self.providers.insert(client, Arc::clone(&provider));
        Ok(provider)
    }
}

#[cfg(test)]
mod tests {
    use super::Roles;
    use crate::config::Config;
    use arc_core::provider::Provider as _;
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
        Roles::resolve(&config, SIDECAR, &Secrets::new(dir))
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
endpoint = "http://127.0.0.1:4096/v1"

[roles.executor]
provider = "openai_compat"
model    = "deepseek-v4-pro"
endpoint = "http://127.0.0.1:4096/v1"

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
endpoint = "http://127.0.0.1:4096/v1"

[roles.executor]
provider = "openai_compat"
model    = "deepseek-v4-pro"
endpoint = "http://127.0.0.1:4096/v1"
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
    fn a_gemini_role_fails_loudly_until_it_has_a_provider() {
        let config: Config = toml::from_str(
            "[roles.concierge]\nprovider = \"gemini\"\nmodel = \"gemini-3.7-flash\"\n",
        )
        .expect("parses");

        let dir = tempfile::tempdir().expect("temp dir");
        let err = Roles::resolve(&config, SIDECAR, &Secrets::new(dir.path()))
            .expect_err("gemini must not silently run on the openai-compatible path")
            .to_string();
        assert!(err.contains("concierge") && err.contains("gemini"), "{err}");
    }
}
