//! Records one real Antigravity SSE response as a test fixture.
//!
//! Task 4.4 parses these bytes, and a parser tested only against bytes its
//! author invented tests the author's imagination. So this sends one tiny
//! completion to the live endpoint and writes the raw response body — framing
//! untouched, nothing reformatted — to a file:
//!
//! ```text
//! cargo run -p arc-core --example capture_sse
//! cargo run -p arc-core --example capture_sse -- gemini-3-pro-low out.sse
//! ```
//!
//! It needs the token file [`login`](../login.rs) writes, and it is run by
//! hand: nothing in `just test` touches the network.
//!
//! # Before committing what it wrote
//!
//! Read the file. Response bodies carry no credential, but they do carry ids
//! that identify an account and a request — `traceId`, `responseId`, and the
//! project id if it appears — and those are replaced with obvious placeholders.
//! Frames, blank lines and `data:` prefixes are left exactly as they arrived,
//! because that framing is the thing 4.4's parser is being tested against.

use std::io::Write as _;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Context as _;
use arc_core::provider::antigravity::Antigravity;
use arc_core::provider::oauth::{OauthConfig, TokenManager};
use arc_core::provider::{CompletionRequest, Message};
use arc_proto::v1::Role;

/// Where `arcd` keeps the tokens (DESIGN.md §10).
const TOKEN_PATH: &str = "data/secrets/google_oauth.json";

/// Where 4.4 will look for the fixture.
const FIXTURE_PATH: &str = "arc-core/tests/fixtures/antigravity_stream.sse";

/// A flash-class model: cheap, fast, and enough to produce a few frames.
const DEFAULT_MODEL: &str = "gemini-3-flash";

/// Short enough that the whole capture is readable, long enough to arrive in
/// more than one frame.
const PROMPT: &str = "Reply with exactly: hello arc";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // The onboarding and request spans are half the point of running this by
    // hand: they show the resolved project and the status before any parsing.
    tracing_subscriber::fmt().init();

    let mut args = std::env::args_os().skip(1);
    let model = args.next().map_or_else(
        || DEFAULT_MODEL.to_owned(),
        |arg| arg.to_string_lossy().into_owned(),
    );
    let path = args
        .next()
        .map_or_else(|| PathBuf::from(FIXTURE_PATH), PathBuf::from);

    let tokens = TokenManager::new(OauthConfig::default(), TOKEN_PATH);
    let provider = Antigravity::new(Arc::new(tokens));

    println!("endpoint: {}", provider.endpoint());
    println!("project:  {}", provider.project().await?);

    let request = CompletionRequest {
        model,
        system: Some("You are terse.".to_owned()),
        messages: vec![Message {
            role: Role::User,
            content: PROMPT.to_owned(),
        }],
    };

    let mut response = provider
        .send(&request)
        .await
        .context("the completion was rejected before it streamed")?;

    println!("status:   {}", response.status());
    for (name, value) in response.headers() {
        println!("header:   {name}: {}", value.to_str().unwrap_or("<binary>"));
    }

    // Read chunk by chunk rather than with `text()`, so the file holds the
    // bytes as they arrived and the chunk count says something about how the
    // backend frames them.
    let mut raw: Vec<u8> = Vec::new();
    let mut chunks = 0u32;
    while let Some(chunk) = response
        .chunk()
        .await
        .context("the stream broke mid-response")?
    {
        chunks += 1;
        raw.extend_from_slice(&chunk);
    }

    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("could not create {}", parent.display()))?;
    }
    std::fs::File::create(&path)
        .and_then(|mut file| file.write_all(&raw))
        .with_context(|| format!("could not write {}", path.display()))?;

    println!(
        "\nwrote {} bytes in {chunks} chunks to {}",
        raw.len(),
        path.display()
    );
    println!("Read it and replace any account, project or trace id before committing.");
    Ok(())
}
