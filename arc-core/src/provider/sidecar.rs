use futures::future::BoxFuture;

use crate::provider::openai::OpenAiCompat;
use crate::provider::{CompletionRequest, CompletionStream, Error, Provider, Thinking};

const NAME: &str = "local";

#[derive(Debug)]
pub struct Sidecar(OpenAiCompat);

impl Sidecar {
    pub fn new(endpoint: &str) -> Self {
        Self(OpenAiCompat::new(endpoint))
    }
}

impl Provider for Sidecar {
    fn name(&self) -> &'static str {
        NAME
    }

    fn endpoint(&self) -> &str {
        self.0.endpoint()
    }

    fn complete(
        &self,
        request: CompletionRequest,
    ) -> BoxFuture<'_, Result<CompletionStream, Error>> {
        self.0.complete(no_think(request))
    }
}

// qwen reads `/no_think` out of the prompt
fn no_think(mut request: CompletionRequest) -> CompletionRequest {
    if request.thinking != Thinking::Minimal {
        return request;
    }
    let mut prompt = request.system.unwrap_or_default();
    if !prompt.is_empty() {
        prompt.push('\n');
    }
    prompt.push_str("/no_think");
    request.system = Some(prompt);
    request
}

#[cfg(test)]
mod tests {
    use super::{Sidecar, no_think};
    use crate::provider::{CompletionRequest, Provider as _, Thinking};
    use arc_proto::v1::SessionRole;

    fn request(thinking: Thinking, system: Option<&str>) -> CompletionRequest {
        CompletionRequest {
            model: "qwen3-8b".to_owned(),
            role: SessionRole::Archivist,
            thinking,
            system: system.map(str::to_owned),
            messages: Vec::new(),
            tools: Vec::new(),
            seed: None,
            web: false,
        }
    }

    #[test]
    fn minimal_appends_the_marker_after_everything_else() {
        assert_eq!(
            no_think(request(Thinking::Minimal, Some("be terse")))
                .system
                .as_deref(),
            Some("be terse\n/no_think"),
            "the marker lands last, after the memory block"
        );
    }

    #[test]
    fn any_other_level_leaves_the_prompt_alone() {
        assert_eq!(
            no_think(request(Thinking::Default, Some("be terse")))
                .system
                .as_deref(),
            Some("be terse")
        );
    }

    #[test]
    fn the_marker_is_the_whole_prompt_when_there_is_nothing_else() {
        assert_eq!(
            no_think(request(Thinking::Minimal, None)).system.as_deref(),
            Some("/no_think"),
            "no leading newline when the prompt was empty"
        );
    }

    #[test]
    fn the_sidecar_is_named_apart_from_a_hosted_endpoint() {
        let sidecar = Sidecar::new("http://127.0.0.1:8080/");

        assert_eq!(sidecar.name(), "local");
        assert_eq!(sidecar.endpoint(), "http://127.0.0.1:8080");
    }
}
