//! Smoke test for the whole provider stack: one real completion through
//! `Provider::complete`, deltas printed as they arrive.
//!
//! Needs a signed-in token file (`cargo run -p arcd -- login`).
//!
//! ```text
//! cargo run -p arc-core --example complete -- "your prompt here"
//! ```

use std::sync::Arc;

use arc_core::provider::antigravity::Antigravity;
use arc_core::provider::oauth::{OauthConfig, TokenManager};
use arc_core::provider::{CompletionDelta, CompletionRequest, Message, Provider};
use arc_proto::v1::Role;
use futures::StreamExt;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let prompt = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "Reply with exactly: hello arc".to_owned());

    let tokens = TokenManager::new(OauthConfig::default(), "data/secrets/google_oauth.json");
    let provider = Antigravity::new(Arc::new(tokens));

    let mut stream = provider
        .complete(CompletionRequest {
            model: "gemini-3-flash".to_owned(),
            system: None,
            messages: vec![Message {
                role: Role::User,
                content: prompt,
            }],
        })
        .await?;

    while let Some(item) = stream.next().await {
        match item? {
            CompletionDelta::Text(text) => print!("{text}"),
            CompletionDelta::Done { usage } => println!(
                "\n[done: {} in, {} out]",
                usage.input_tokens, usage.output_tokens
            ),
        }
    }
    Ok(())
}
