//! Runs the Google OAuth loopback flow and writes the token file.
//!
//! The daemon does not exist yet, so this is how sign-in gets exercised:
//!
//! ```text
//! cargo run -p arc-core --example login
//! cargo run -p arc-core --example login -- /path/to/tokens.json
//! ```
//!
//! It prints a URL, waits for the browser to come back to the loopback
//! redirect, and writes `data/secrets/google_oauth.json` — the same path
//! `arcd` will pass. Run it from the repository root so that path lands under
//! the gitignored `data/`.
//!
//! Nothing is printed but the URL and a confirmation. Tokens stay in the file.

use std::path::PathBuf;

use anyhow::Context as _;
use arc_core::provider::oauth::{self, OauthConfig};

/// Where `arcd` will look for the tokens (DESIGN.md §10).
const DEFAULT_TOKEN_PATH: &str = "data/secrets/google_oauth.json";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // The login span reports the scope count and the resulting expiry, never a
    // token. Seeing that is half the point of running this by hand.
    tracing_subscriber::fmt().init();

    let path = std::env::args_os()
        .nth(1)
        .map_or_else(|| PathBuf::from(DEFAULT_TOKEN_PATH), PathBuf::from);

    let manager = oauth::login(&OauthConfig::default(), &path)
        .await
        .context("OAuth login failed")?;

    // Proves the file that was just written is loadable and the token usable,
    // which is the part 4.3 depends on.
    manager
        .bearer()
        .await
        .context("the stored tokens did not yield a bearer token")?;
    println!("Token file at {} is usable.", manager.path().display());

    Ok(())
}
