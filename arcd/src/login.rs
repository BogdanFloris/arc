use std::time::Duration;

use anyhow::{Context as _, Result};
use arc_core::provider::codex::auth::{
    DEFAULT_AUTH_ENDPOINT, DEVICE_CODE_TIMEOUT, request_device_code, wait_for_device_approval,
};
use arc_core::secrets::Secrets;

use crate::config::{Config, RoleProvider};
use crate::dirs::DataDirs;

const DEFAULT_CREDENTIAL: &str = "codex";

/// The credential file the config expects: the `key` of the first codex
/// role, or `codex` when no role is on codex yet.
pub fn credential_name(config: &Config) -> &str {
    [
        config.roles.concierge.as_ref(),
        config.roles.executor.as_ref(),
        config.roles.archivist.as_ref(),
    ]
    .into_iter()
    .flatten()
    .find(|role| role.provider == RoleProvider::Codex)
    .and_then(|role| role.key.as_deref())
    .unwrap_or(DEFAULT_CREDENTIAL)
}

pub async fn codex(config: &Config, dirs: &DataDirs) -> Result<()> {
    dirs.create()
        .with_context(|| format!("preparing {}", dirs.root().display()))?;
    let secrets = Secrets::new(dirs.secrets());
    let name = credential_name(config);
    let http = reqwest::Client::new();

    let device = request_device_code(&http, DEFAULT_AUTH_ENDPOINT)
        .await
        .context("requesting a device code")?;
    println!("Open {} and enter the code:", device.verification_url);
    println!();
    println!("    {}", device.user_code);
    println!();
    println!(
        "Waiting for approval (the code expires in {} minutes)...",
        DEVICE_CODE_TIMEOUT.as_secs() / 60
    );

    let credential = wait_for_device_approval(&http, DEFAULT_AUTH_ENDPOINT, &device, timeout())
        .await
        .context("waiting for the device code to be approved")?;
    secrets
        .write(name, &credential.to_json())
        .with_context(|| format!("saving the credential as `{name}`"))?;
    println!(
        "Signed in. Credential saved to {}; restart arcd to use it.",
        secrets.path(name).display()
    );
    Ok(())
}

fn timeout() -> Duration {
    DEVICE_CODE_TIMEOUT
}

#[cfg(test)]
mod tests {
    use super::credential_name;
    use crate::config::Config;

    #[test]
    fn the_credential_name_is_the_codex_roles_key_or_the_default() {
        let config: Config = toml::from_str("").expect("parses");
        assert_eq!(credential_name(&config), "codex");

        let config: Config = toml::from_str(
            "[roles.executor]\nprovider = \"codex\"\nmodel = \"gpt-5.5\"\nkey = \"chatgpt-pro\"\n",
        )
        .expect("parses");
        assert_eq!(credential_name(&config), "chatgpt-pro");
    }
}
