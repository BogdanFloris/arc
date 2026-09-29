use std::{future::Future, pin::Pin, time::Duration};

use futures::StreamExt;
use serde::Deserialize;
use tracing::instrument;

use crate::{
    provider::ToolDefinition,
    tool::{Tool, ToolReply, ToolSource, TurnContext},
};

const ENDPOINT: &str = "https://mcp.exa.ai/mcp?tools=web_search_exa,web_fetch_exa";
const MAX_RESPONSE_BYTES: usize = 256 * 1024;
const MAX_OUTPUT_CHARS: usize = 24_000;

#[derive(Deserialize)]
struct SearchArgs {
    query: String,
}

#[derive(Deserialize)]
struct FetchArgs {
    url: String,
}

pub struct WebSearch;
pub struct WebFetch;

impl Tool for WebSearch {
    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: "web_search".into(),
            description: "Search the web and return sourced results.".into(),
            parameters: serde_json::json!({"type":"object","properties":{"query":{"type":"string"}},"required":["query"]}),
        }
    }

    fn source(&self) -> ToolSource {
        ToolSource::SharedWeb
    }

    fn execute(
        &self,
        input: String,
        _: TurnContext,
    ) -> Pin<Box<dyn Future<Output = ToolReply> + Send + '_>> {
        Box::pin(async move {
            let args: SearchArgs = match serde_json::from_str::<SearchArgs>(&input) {
                Ok(args) if !args.query.trim().is_empty() && args.query.len() <= 2048 => args,
                _ => {
                    return ToolReply::error(
                        "ERROR: web_search requires a query of at most 2048 bytes.".into(),
                    );
                }
            };
            call(
                "web_search_exa",
                serde_json::json!({"query":args.query,"objective":args.query}),
            )
            .await
        })
    }
}

impl Tool for WebFetch {
    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: "web_fetch".into(),
            description: "Fetch a public HTTP or HTTPS page as readable text.".into(),
            parameters: serde_json::json!({"type":"object","properties":{"url":{"type":"string"}},"required":["url"]}),
        }
    }

    fn source(&self) -> ToolSource {
        ToolSource::SharedWeb
    }

    fn execute(
        &self,
        input: String,
        _: TurnContext,
    ) -> Pin<Box<dyn Future<Output = ToolReply> + Send + '_>> {
        Box::pin(async move {
            let args: FetchArgs = match serde_json::from_str(&input) {
                Ok(args) => args,
                Err(_) => return ToolReply::error("ERROR: web_fetch requires a URL.".into()),
            };
            let url = match validate_url(&args.url) {
                Ok(url) => url,
                Err(error) => return ToolReply::error(format!("ERROR: {error}")),
            };
            call(
                "web_fetch_exa",
                serde_json::json!({"urls":[url.as_str()],"maxCharacters":24000}),
            )
            .await
        })
    }
}

fn validate_url(raw: &str) -> Result<reqwest::Url, &'static str> {
    let url = reqwest::Url::parse(raw).map_err(|_| "invalid web URL.")?;
    if !matches!(url.scheme(), "http" | "https")
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
    {
        return Err("only HTTP and HTTPS URLs without credentials are supported.");
    }
    Ok(url)
}

#[instrument(skip(arguments), fields(backend = "exa"))]
async fn call(tool: &str, arguments: serde_json::Value) -> ToolReply {
    call_at(ENDPOINT, tool, arguments).await
}

async fn call_at(endpoint: &str, tool: &str, arguments: serde_json::Value) -> ToolReply {
    let result = async {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(25))
            .redirect(reqwest::redirect::Policy::limited(3))
            .user_agent(concat!("arc/", env!("CARGO_PKG_VERSION")))
            .build()?;
        let response = client
            .post(endpoint)
            .header("accept", "application/json, text/event-stream")
            .json(&serde_json::json!({
                "jsonrpc":"2.0","id":1,"method":"tools/call",
                "params":{"name":tool,"arguments":arguments}
            }))
            .send()
            .await?
            .error_for_status()?;
        let mut stream = response.bytes_stream();
        let mut bytes = Vec::new();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk?;
            let remaining = MAX_RESPONSE_BYTES - bytes.len();
            bytes.extend_from_slice(&chunk[..chunk.len().min(remaining)]);
            if bytes.len() == MAX_RESPONSE_BYTES {
                break;
            }
        }
        parse_response(&String::from_utf8_lossy(&bytes))
    }
    .await;
    match result {
        Ok(content) => ToolReply::ok(content),
        Err(_) => ToolReply::error("ERROR: web service request failed.".into()),
    }
}

fn parse_response(body: &str) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    let payloads: Vec<_> = body
        .lines()
        .filter_map(|line| line.strip_prefix("data: "))
        .collect();
    let payloads = if payloads.is_empty() {
        vec![body]
    } else {
        payloads
    };
    let mut outputs = Vec::new();
    for payload in payloads {
        let value: serde_json::Value = serde_json::from_str(payload)?;
        if value.get("error").is_some() {
            return Err(std::io::Error::other("service returned JSON-RPC error").into());
        }
        let result = value
            .get("result")
            .ok_or_else(|| std::io::Error::other("missing result"))?;
        if result.get("isError").and_then(serde_json::Value::as_bool) == Some(true) {
            return Err(std::io::Error::other("service reported tool error").into());
        }
        if let Some(entries) = result.get("content").and_then(|v| v.as_array()) {
            outputs.extend(entries.iter().filter_map(|entry| {
                entry
                    .get("text")
                    .and_then(|v| v.as_str())
                    .map(str::to_owned)
            }));
        }
    }
    if outputs.is_empty() {
        return Err(std::io::Error::other("service returned no text").into());
    }
    Ok(outputs.join("\n").chars().take(MAX_OUTPUT_CHARS).collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_json_and_sse_mcp_replies_and_joins_text() {
        assert_eq!(
            parse_response(r#"{"result":{"content":[{"text":"one"},{"text":"two"}]}}"#).unwrap(),
            "one\ntwo"
        );
        assert_eq!(
            parse_response(
                "event: message\ndata: {\"result\":{\"content\":[{\"text\":\"ok\"}]}}\n\n"
            )
            .unwrap(),
            "ok"
        );
    }

    #[test]
    fn rejects_json_rpc_and_tool_errors() {
        assert!(parse_response(r#"{"error":{"message":"no"}}"#).is_err());
        assert!(
            parse_response(r#"{"result":{"isError":true,"content":[{"text":"failed"}]}}"#).is_err()
        );
    }

    #[test]
    fn caps_output_and_rejects_empty_content() {
        let text = "x".repeat(MAX_OUTPUT_CHARS + 10);
        let body = serde_json::json!({"result":{"content":[{"text":text}]}});
        assert_eq!(
            parse_response(&body.to_string()).unwrap().len(),
            MAX_OUTPUT_CHARS
        );
        assert!(parse_response(r#"{"result":{"content":[]}}"#).is_err());
    }

    #[test]
    fn rejects_unsupported_and_credential_bearing_urls() {
        for raw in ["file:///etc/passwd", "https://user:secret@example.com/"] {
            assert!(validate_url(raw).is_err());
        }
        assert_eq!(
            validate_url("https://example.com/page").unwrap().as_str(),
            "https://example.com/page"
        );
    }

    #[tokio::test]
    async fn fetch_uses_exa_urls_array_schema() {
        use wiremock::{
            Mock, MockServer, ResponseTemplate,
            matchers::{body_json, method, path},
        };
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/mcp"))
            .and(body_json(serde_json::json!({
                "jsonrpc":"2.0","id":1,"method":"tools/call",
                "params":{"name":"web_fetch_exa","arguments":{
                    "urls":["https://example.com/page"],"maxCharacters":24000
                }}
            })))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result":{"content":[{"text":"page text"}]}
            })))
            .mount(&server)
            .await;
        let reply = call_at(
            &format!("{}/mcp", server.uri()),
            "web_fetch_exa",
            serde_json::json!({"urls":["https://example.com/page"],"maxCharacters":24000}),
        )
        .await;
        assert!(reply.ok, "{}", reply.content);
        assert_eq!(reply.content, "page text");
    }
}
