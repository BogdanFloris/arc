//! `arcd login` — the OAuth flow, run by hand.
//!
//! All of it lives in `arc_core::provider::oauth`, prompts included; the
//! daemon's job is to say which token file (from [`DataDirs`]) and to make a
//! failure legible.

use anyhow::{Context as _, Result};
use arc_core::provider::oauth::{self, OauthConfig};

use crate::dirs::DataDirs;

/// Runs the loopback flow and writes the token file.
///
/// The directories are created first: the flow writes into `secrets/`, and it
/// should not be the thing that discovers the directory is missing.
///
/// # Errors
///
/// If the directories cannot be created, the flow does not complete, or the
/// tokens it wrote do not yield a bearer token.
pub async fn run(dirs: &DataDirs) -> Result<()> {
    dirs.create()
        .with_context(|| format!("preparing {}", dirs.root().display()))?;

    let manager = oauth::login(&OauthConfig::default(), dirs.tokens())
        .await
        .context("OAuth login failed")?;

    // Proves the file just written is loadable and its token usable, so a
    // successful login means the daemon will not fail on first completion.
    manager
        .bearer()
        .await
        .context("the stored tokens did not yield a bearer token")?;

    println!("Signed in. Token file: {}", manager.path().display());
    Ok(())
}
