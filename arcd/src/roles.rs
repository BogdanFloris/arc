use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Result, bail};
use arc_core::provider::any::AnyProvider;

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
    pub fn resolve(config: &Config, sidecar_endpoint: &str) -> Result<Self> {
        let mut built = Built::new(sidecar_endpoint);
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
    providers: HashMap<(RoleProvider, String), Arc<AnyProvider>>,
}

impl<'a> Built<'a> {
    fn new(sidecar_endpoint: &'a str) -> Self {
        Self {
            sidecar_endpoint,
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
                    provider: self.shared(RoleProvider::OpenAiCompat, endpoint),
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
        self.shared(RoleProvider::Local, self.sidecar_endpoint.to_owned())
    }

    fn shared(&mut self, kind: RoleProvider, endpoint: String) -> Arc<AnyProvider> {
        Arc::clone(
            self.providers
                .entry((kind, endpoint))
                .or_insert_with_key(|(kind, endpoint)| {
                    Arc::new(match kind {
                        RoleProvider::Local => AnyProvider::local(endpoint),
                        RoleProvider::OpenAiCompat => AnyProvider::openai_compat(endpoint),
                        // 3.3 adds the variant; this match makes it a compile error, not a silent fallback
                        RoleProvider::Gemini => {
                            unreachable!("a gemini role is rejected before a provider is built")
                        }
                    })
                }),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::Roles;
    use crate::config::Config;
    use arc_core::provider::Provider as _;

    const SIDECAR: &str = "http://127.0.0.1:8080";

    fn resolved(text: &str) -> Roles {
        let config: Config = toml::from_str(text).expect("parses");
        Roles::resolve(&config, SIDECAR).expect("resolves")
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
    fn a_gemini_role_fails_loudly_until_it_has_a_provider() {
        let config: Config = toml::from_str(
            "[roles.concierge]\nprovider = \"gemini\"\nmodel = \"gemini-3.7-flash\"\n",
        )
        .expect("parses");

        let err = Roles::resolve(&config, SIDECAR)
            .expect_err("gemini must not silently run on the openai-compatible path")
            .to_string();
        assert!(err.contains("concierge") && err.contains("gemini"), "{err}");
    }
}
