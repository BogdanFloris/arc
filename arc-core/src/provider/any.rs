use crate::provider::gemini::Gemini;
use crate::provider::openai::OpenAiCompat;
use crate::provider::{CompletionRequest, CompletionStream, Error, Provider, Thinking};

#[derive(Debug)]
pub enum AnyProvider {
    Local(OpenAiCompat),

    OpenAiCompat(OpenAiCompat),

    Gemini(Gemini),
}

impl AnyProvider {
    // the sidecar speaks the same wire protocol; the variants exist so the log
    // can tell it apart from a hosted endpoint
    pub fn local(endpoint: &str) -> Self {
        Self::Local(OpenAiCompat::new(endpoint))
    }

    pub fn openai_compat(endpoint: &str, key: Option<String>) -> Self {
        Self::OpenAiCompat(match key {
            Some(key) => OpenAiCompat::keyed(endpoint, key),
            None => OpenAiCompat::new(endpoint),
        })
    }

    pub fn gemini(endpoint: &str, key: String) -> Self {
        Self::Gemini(Gemini::new(endpoint, key))
    }

    pub fn endpoint(&self) -> &str {
        match self {
            Self::Local(inner) | Self::OpenAiCompat(inner) => inner.endpoint(),
            Self::Gemini(inner) => inner.endpoint(),
        }
    }

    pub fn is_local(&self) -> bool {
        matches!(self, Self::Local(_))
    }
}

impl Provider for AnyProvider {
    fn name(&self) -> &'static str {
        match self {
            Self::Local(_) => "local",
            Self::OpenAiCompat(inner) => inner.name(),
            Self::Gemini(inner) => inner.name(),
        }
    }

    async fn complete(&self, request: CompletionRequest) -> Result<CompletionStream, Error> {
        match self {
            Self::Local(inner) => inner.complete(no_think(request)).await,
            Self::OpenAiCompat(inner) => inner.complete(request).await,
            Self::Gemini(inner) => inner.complete(request).await,
        }
    }
}

// Qwen reads `/no_think` out of the prompt; it has no request field for this
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
    use super::AnyProvider;
    use crate::provider::{CompletionRequest, Provider as _, Thinking};
    use arc_proto::v1::SessionRole;

    #[test]
    fn the_sidecar_and_a_hosted_endpoint_are_named_apart_in_the_log() {
        let sidecar = AnyProvider::local("http://127.0.0.1:8080");
        let hosted = AnyProvider::openai_compat("http://127.0.0.1:4096/v1", None);

        assert_eq!(sidecar.name(), "local");
        assert_eq!(hosted.name(), "openai-compat");
        assert_ne!(sidecar.name(), hosted.name());
    }

    #[test]
    fn only_the_sidecar_gets_the_no_think_marker() {
        let request = |thinking| CompletionRequest {
            model: "qwen3-8b".to_owned(),
            role: SessionRole::Archivist,
            thinking,
            system: Some("be terse".to_owned()),
            messages: Vec::new(),
            tools: Vec::new(),
            seed: None,
        };

        assert_eq!(
            super::no_think(request(Thinking::Minimal))
                .system
                .as_deref(),
            Some("be terse\n/no_think"),
            "the marker lands last, after the memory block"
        );
        assert_eq!(
            super::no_think(request(Thinking::Default))
                .system
                .as_deref(),
            Some("be terse"),
            "any other level leaves the prompt alone"
        );
    }

    #[test]
    fn the_marker_is_the_whole_prompt_when_there_is_nothing_else() {
        let request = CompletionRequest {
            model: "qwen3-8b".to_owned(),
            role: SessionRole::Archivist,
            thinking: Thinking::Minimal,
            system: None,
            messages: Vec::new(),
            tools: Vec::new(),
            seed: None,
        };

        assert_eq!(
            super::no_think(request).system.as_deref(),
            Some("/no_think"),
            "no leading newline when the prompt was empty"
        );
    }

    #[test]
    fn a_trailing_slash_does_not_change_the_endpoint() {
        assert_eq!(
            AnyProvider::openai_compat("http://127.0.0.1:4096/v1/", None).endpoint(),
            "http://127.0.0.1:4096/v1"
        );
    }
}
