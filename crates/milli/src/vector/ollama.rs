use std::time::Instant;

use rayon::iter::{IntoParallelIterator as _, ParallelIterator as _};
use rayon::slice::ParallelSlice as _;

use super::error::{EmbedError, EmbedErrorKind, NewEmbedderError, NewEmbedderErrorKind};
use super::rest::{Embedder as RestEmbedder, EmbedderOptions as RestEmbedderOptions};
use super::DistributionShift;
use crate::error::FaultSource;
use crate::vector::Embedding;
use crate::ThreadPoolNoAbort;

#[derive(Debug)]
pub struct Embedder {
    rest_embedder: RestEmbedder,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct EmbedderOptions {
    pub embedding_model: String,
    pub url: Option<String>,
    pub api_key: Option<String>,
    pub distribution: Option<DistributionShift>,
    pub dimensions: Option<usize>,
}

impl EmbedderOptions {
    pub fn with_default_model(
        api_key: Option<String>,
        url: Option<String>,
        dimensions: Option<usize>,
    ) -> Self {
        Self {
            embedding_model: "nomic-embed-text".into(),
            api_key,
            url,
            distribution: None,
            dimensions,
        }
    }

    fn into_rest_embedder_config(self) -> Result<RestEmbedderOptions, NewEmbedderError> {
        let url = self.url.unwrap_or_else(get_ollama_path);
        let model = self.embedding_model.as_str();

        // **warning**: do not swap these two `if`s, as the second one is always true when the first one is.
        let (request, response) = if url.ends_with("/api/embeddings") {
            (
                serde_json::json!({"model": model, "input": [super::rest::REQUEST_PLACEHOLDER, super::rest::REPEAT_PLACEHOLDER]}),
                serde_json::json!({"embeddings": [super::rest::RESPONSE_PLACEHOLDER, super::rest::REPEAT_PLACEHOLDER]}),
            )
        } else if url.ends_with("/api/embed") {
            (
                serde_json::json!({
                    "model": model,
                    "prompt": super::rest::REQUEST_PLACEHOLDER,
                }),
                serde_json::json!({
                    "embedding": super::rest::RESPONSE_PLACEHOLDER,
                }),
            )
        } else {
            return Err(NewEmbedderError::ollama_unsupported_url(url));
        };
        Ok(RestEmbedderOptions {
            api_key: self.api_key,
            dimensions: self.dimensions,
            distribution: self.distribution,
            url,
            request,
            response,
            headers: Default::default(),
        })
    }
}

impl Embedder {
    pub fn new(options: EmbedderOptions) -> Result<Self, NewEmbedderError> {
        let rest_embedder = match RestEmbedder::new(
            options.into_rest_embedder_config()?,
            super::rest::ConfigurationSource::Ollama,
        ) {
            Ok(embedder) => embedder,
            Err(NewEmbedderError {
                kind:
                    NewEmbedderErrorKind::CouldNotDetermineDimension(EmbedError {
                        kind: super::error::EmbedErrorKind::RestOtherStatusCode(404, error),
                        fault: _,
                    }),
                fault: _,
            }) => {
                return Err(NewEmbedderError::could_not_determine_dimension(
                    EmbedError::ollama_model_not_found(error),
                ))
            }
            Err(error) => return Err(error),
        };

        Ok(Self { rest_embedder })
    }

    pub fn embed<S: AsRef<str> + serde::Serialize>(
        &self,
        texts: &[S],
        deadline: Option<Instant>,
    ) -> Result<Vec<Embedding>, EmbedError> {
        match self.rest_embedder.embed_ref(texts, deadline) {
            Ok(embeddings) => Ok(embeddings),
            Err(EmbedError { kind: EmbedErrorKind::RestOtherStatusCode(404, error), fault: _ }) => {
                Err(EmbedError::ollama_model_not_found(error))
            }
            Err(error) => Err(error),
        }
    }

    pub fn embed_chunks(
        &self,
        text_chunks: Vec<Vec<String>>,
        threads: &ThreadPoolNoAbort,
    ) -> Result<Vec<Vec<Embedding>>, EmbedError> {
        threads
            .install(move || {
                text_chunks.into_par_iter().map(move |chunk| self.embed(&chunk, None)).collect()
            })
            .map_err(|error| EmbedError {
                kind: EmbedErrorKind::PanicInThreadPool(error),
                fault: FaultSource::Bug,
            })?
    }

    pub(crate) fn embed_chunks_ref(
        &self,
        texts: &[&str],
        threads: &ThreadPoolNoAbort,
    ) -> Result<Vec<Vec<f32>>, EmbedError> {
        threads
            .install(move || {
                let embeddings: Result<Vec<Vec<Embedding>>, _> = texts
                    .par_chunks(self.prompt_count_in_chunk_hint())
                    .map(move |chunk| self.embed(chunk, None))
                    .collect();

                let embeddings = embeddings?;
                Ok(embeddings.into_iter().flatten().collect())
            })
            .map_err(|error| EmbedError {
                kind: EmbedErrorKind::PanicInThreadPool(error),
                fault: FaultSource::Bug,
            })?
    }

    pub fn chunk_count_hint(&self) -> usize {
        self.rest_embedder.chunk_count_hint()
    }

    pub fn prompt_count_in_chunk_hint(&self) -> usize {
        self.rest_embedder.prompt_count_in_chunk_hint()
    }

    pub fn dimensions(&self) -> usize {
        self.rest_embedder.dimensions()
    }

    pub fn distribution(&self) -> Option<DistributionShift> {
        self.rest_embedder.distribution()
    }
}

fn get_ollama_path() -> String {
    // Important: Hostname not enough, has to be entire path to embeddings endpoint
    std::env::var("MEILI_OLLAMA_URL").unwrap_or("http://localhost:11434/api/embeddings".to_string())
}
