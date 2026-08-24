use crate::provider::openai::OpenAiCompat;
use crate::provider::{CompletionRequest, CompletionStream, Error, Provider};

#[derive(Debug)]
pub enum AnyProvider {
    Local(OpenAiCompat),

    OpenAiCompat(OpenAiCompat),
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

    pub fn endpoint(&self) -> &str {
        match self {
            Self::Local(inner) | Self::OpenAiCompat(inner) => inner.endpoint(),
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
        }
    }

    async fn complete(&self, request: CompletionRequest) -> Result<CompletionStream, Error> {
        match self {
            Self::Local(inner) | Self::OpenAiCompat(inner) => inner.complete(request).await,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::AnyProvider;
    use crate::provider::Provider as _;

    #[test]
    fn the_sidecar_and_a_hosted_endpoint_are_named_apart_in_the_log() {
        let sidecar = AnyProvider::local("http://127.0.0.1:8080");
        let hosted = AnyProvider::openai_compat("http://127.0.0.1:4096/v1", None);

        assert_eq!(sidecar.name(), "local");
        assert_eq!(hosted.name(), "openai-compat");
        assert_ne!(sidecar.name(), hosted.name());
    }

    #[test]
    fn a_trailing_slash_does_not_change_the_endpoint() {
        assert_eq!(
            AnyProvider::openai_compat("http://127.0.0.1:4096/v1/", None).endpoint(),
            "http://127.0.0.1:4096/v1"
        );
    }
}
