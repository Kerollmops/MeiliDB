use std::collections::BTreeMap;
use std::path::PathBuf;

use bumpalo::Bump;
use hf_hub::api::sync::ApiError;

use super::parsed_vectors::ParsedVectorsDiff;
use super::rest::ConfigurationSource;
use crate::error::FaultSource;
use crate::update::new::vector_document::VectorDocument;
use crate::{FieldDistribution, PanicCatched};

#[derive(Debug, thiserror::Error)]
#[error("Error while generating embeddings: {inner}")]
pub struct Error {
    pub inner: Box<ErrorKind>,
}

impl<I: Into<ErrorKind>> From<I> for Error {
    fn from(value: I) -> Self {
        Self { inner: Box::new(value.into()) }
    }
}

impl Error {
    pub fn fault(&self) -> FaultSource {
        match &*self.inner {
            ErrorKind::NewEmbedderError(inner) => inner.fault,
            ErrorKind::EmbedError(inner) => inner.fault,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ErrorKind {
    #[error(transparent)]
    NewEmbedderError(#[from] NewEmbedderError),
    #[error(transparent)]
    EmbedError(#[from] EmbedError),
}

#[derive(Debug, thiserror::Error)]
#[error("{fault}: {kind}")]
pub struct EmbedError {
    pub kind: EmbedErrorKind,
    pub fault: FaultSource,
}

#[derive(Debug, thiserror::Error)]
pub enum EmbedErrorKind {
    #[error("could not tokenize:\n  - {0}")]
    Tokenize(Box<dyn std::error::Error + Send + Sync>),
    #[error("unexpected tensor shape:\n  - {0}")]
    TensorShape(candle_core::Error),
    #[error("unexpected tensor value:\n  - {0}")]
    TensorValue(candle_core::Error),
    #[error("could not run model:\n  - {0}")]
    ModelForward(candle_core::Error),
    #[error("attempt to embed the following text in a configuration where embeddings must be user provided:\n  - `{0}`")]
    ManualEmbed(String),
    #[error("model not found. Meilisearch will not automatically download models from the Ollama library, please pull the model manually{}", option_info(.0.as_deref(), "server replied with "))]
    OllamaModelNotFoundError(Option<String>),
    #[error("error deserializing the response body as JSON:\n  - {0}")]
    RestResponseDeserialization(std::io::Error),
    #[error("expected a response containing {0} embeddings, got only {1}")]
    RestResponseEmbeddingCount(usize, usize),
    #[error("could not authenticate against {embedding} server{server_reply}{hint}", embedding=match *.1 {
        ConfigurationSource::User => "embedding",
        ConfigurationSource::OpenAi => "OpenAI",
        ConfigurationSource::Ollama => "Ollama"
    },
    server_reply=option_info(.0.as_deref(), "server replied with "),
    hint=match *.1 {
        ConfigurationSource::User => "\n  - Hint: Check the `apiKey` parameter in the embedder configuration",
        ConfigurationSource::OpenAi => "\n  - Hint: Check the `apiKey` parameter in the embedder configuration, and the `MEILI_OPENAI_API_KEY` and `OPENAI_API_KEY` environment variables",
        ConfigurationSource::Ollama => "\n  - Hint: Check the `apiKey` parameter in the embedder configuration"
    })]
    RestUnauthorized(Option<String>, ConfigurationSource),
    #[error("sent too many requests to embedding server{}", option_info(.0.as_deref(), "server replied with "))]
    RestTooManyRequests(Option<String>),
    #[error("sent a bad request to embedding server{}{}",
    if ConfigurationSource::User == *.1 {
        "\n  - Hint: check that the `request` in the embedder configuration matches the remote server's API"
    } else {
        ""
    },
    option_info(.0.as_deref(), "server replied with "))]
    RestBadRequest(Option<String>, ConfigurationSource),
    #[error("received internal error HTTP {} from embedding server{}", .0, option_info(.1.as_deref(), "server replied with "))]
    RestInternalServerError(u16, Option<String>),
    #[error("received unexpected HTTP {} from embedding server{}", .0, option_info(.1.as_deref(), "server replied with "))]
    RestOtherStatusCode(u16, Option<String>),
    #[error("could not reach embedding server:\n  - {0}")]
    RestNetwork(ureq::Transport),
    #[error("error extracting embeddings from the response:\n  - {0}")]
    RestExtractionError(String),
    #[error("was expecting embeddings of dimension `{0}`, got embeddings of dimensions `{1}`")]
    UnexpectedDimension(usize, usize),
    #[error("no embedding was produced")]
    MissingEmbedding,
    #[error(transparent)]
    PanicInThreadPool(#[from] PanicCatched),
}

fn option_info(info: Option<&str>, prefix: &str) -> String {
    match info {
        Some(info) => format!("\n  - {prefix}`{info}`"),
        None => String::new(),
    }
}

impl EmbedError {
    pub fn tokenize(inner: Box<dyn std::error::Error + Send + Sync>) -> Self {
        Self { kind: EmbedErrorKind::Tokenize(inner), fault: FaultSource::Runtime }
    }

    pub fn tensor_shape(inner: candle_core::Error) -> Self {
        Self { kind: EmbedErrorKind::TensorShape(inner), fault: FaultSource::Bug }
    }

    pub fn tensor_value(inner: candle_core::Error) -> Self {
        Self { kind: EmbedErrorKind::TensorValue(inner), fault: FaultSource::Bug }
    }

    pub fn model_forward(inner: candle_core::Error) -> Self {
        Self { kind: EmbedErrorKind::ModelForward(inner), fault: FaultSource::Runtime }
    }

    pub(crate) fn embed_on_manual_embedder(texts: String) -> EmbedError {
        Self { kind: EmbedErrorKind::ManualEmbed(texts), fault: FaultSource::User }
    }

    pub(crate) fn ollama_model_not_found(inner: Option<String>) -> EmbedError {
        Self { kind: EmbedErrorKind::OllamaModelNotFoundError(inner), fault: FaultSource::User }
    }

    pub(crate) fn rest_response_deserialization(error: std::io::Error) -> EmbedError {
        Self {
            kind: EmbedErrorKind::RestResponseDeserialization(error),
            fault: FaultSource::Runtime,
        }
    }

    pub(crate) fn rest_response_embedding_count(expected: usize, got: usize) -> EmbedError {
        Self {
            kind: EmbedErrorKind::RestResponseEmbeddingCount(expected, got),
            fault: FaultSource::Runtime,
        }
    }

    pub(crate) fn rest_unauthorized(
        error_response: Option<String>,
        configuration_source: ConfigurationSource,
    ) -> EmbedError {
        Self {
            kind: EmbedErrorKind::RestUnauthorized(error_response, configuration_source),
            fault: FaultSource::User,
        }
    }

    pub(crate) fn rest_too_many_requests(error_response: Option<String>) -> EmbedError {
        Self {
            kind: EmbedErrorKind::RestTooManyRequests(error_response),
            fault: FaultSource::Runtime,
        }
    }

    pub(crate) fn rest_bad_request(
        error_response: Option<String>,
        configuration_source: ConfigurationSource,
    ) -> EmbedError {
        Self {
            kind: EmbedErrorKind::RestBadRequest(error_response, configuration_source),
            fault: FaultSource::User,
        }
    }

    pub(crate) fn rest_internal_server_error(
        code: u16,
        error_response: Option<String>,
    ) -> EmbedError {
        Self {
            kind: EmbedErrorKind::RestInternalServerError(code, error_response),
            fault: FaultSource::Runtime,
        }
    }

    pub(crate) fn rest_other_status_code(code: u16, error_response: Option<String>) -> EmbedError {
        Self {
            kind: EmbedErrorKind::RestOtherStatusCode(code, error_response),
            fault: FaultSource::Undecided,
        }
    }

    pub(crate) fn rest_network(transport: ureq::Transport) -> EmbedError {
        Self { kind: EmbedErrorKind::RestNetwork(transport), fault: FaultSource::Runtime }
    }

    pub(crate) fn rest_unexpected_dimension(expected: usize, got: usize) -> EmbedError {
        Self {
            kind: EmbedErrorKind::UnexpectedDimension(expected, got),
            fault: FaultSource::Runtime,
        }
    }
    pub(crate) fn missing_embedding() -> EmbedError {
        Self { kind: EmbedErrorKind::MissingEmbedding, fault: FaultSource::Undecided }
    }

    pub(crate) fn rest_extraction_error(error: String) -> EmbedError {
        Self { kind: EmbedErrorKind::RestExtractionError(error), fault: FaultSource::Runtime }
    }
}

#[derive(Debug, thiserror::Error)]
#[error("{fault}: {kind}")]
pub struct NewEmbedderError {
    pub kind: NewEmbedderErrorKind,
    pub fault: FaultSource,
}

impl NewEmbedderError {
    pub fn open_config(config_filename: PathBuf, inner: std::io::Error) -> NewEmbedderError {
        let open_config = OpenConfig { filename: config_filename, inner };

        Self { kind: NewEmbedderErrorKind::OpenConfig(open_config), fault: FaultSource::Runtime }
    }

    pub fn deserialize_config(
        model_name: String,
        config: String,
        config_filename: PathBuf,
        inner: serde_json::Error,
    ) -> NewEmbedderError {
        match serde_json::from_str(&config) {
            Ok(value) => {
                let value: serde_json::Value = value;
                let architectures = match value.get("architectures") {
                    Some(serde_json::Value::Array(architectures)) => architectures
                        .iter()
                        .filter_map(|value| match value {
                            serde_json::Value::String(s) => Some(s.to_owned()),
                            _ => None,
                        })
                        .collect(),
                    _ => vec![],
                };

                let unsupported_model = UnsupportedModel { model_name, inner, architectures };
                Self {
                    kind: NewEmbedderErrorKind::UnsupportedModel(unsupported_model),
                    fault: FaultSource::User,
                }
            }
            Err(error) => {
                let deserialize_config =
                    DeserializeConfig { model_name, filename: config_filename, inner: error };
                Self {
                    kind: NewEmbedderErrorKind::DeserializeConfig(deserialize_config),
                    fault: FaultSource::Runtime,
                }
            }
        }
    }

    pub fn open_tokenizer(
        tokenizer_filename: PathBuf,
        inner: Box<dyn std::error::Error + Send + Sync>,
    ) -> NewEmbedderError {
        let open_tokenizer = OpenTokenizer { filename: tokenizer_filename, inner };
        Self {
            kind: NewEmbedderErrorKind::OpenTokenizer(open_tokenizer),
            fault: FaultSource::Runtime,
        }
    }

    pub fn new_api_fail(inner: ApiError) -> Self {
        Self { kind: NewEmbedderErrorKind::NewApiFail(inner), fault: FaultSource::Bug }
    }

    pub fn api_get(inner: ApiError) -> Self {
        Self { kind: NewEmbedderErrorKind::ApiGet(inner), fault: FaultSource::Undecided }
    }

    pub fn pytorch_weight(inner: candle_core::Error) -> Self {
        Self { kind: NewEmbedderErrorKind::PytorchWeight(inner), fault: FaultSource::Runtime }
    }

    pub fn safetensor_weight(inner: candle_core::Error) -> Self {
        Self { kind: NewEmbedderErrorKind::SafetensorWeight(inner), fault: FaultSource::Runtime }
    }

    pub fn load_model(inner: candle_core::Error) -> Self {
        Self { kind: NewEmbedderErrorKind::LoadModel(inner), fault: FaultSource::Runtime }
    }

    pub fn could_not_determine_dimension(inner: EmbedError) -> NewEmbedderError {
        Self {
            kind: NewEmbedderErrorKind::CouldNotDetermineDimension(inner),
            fault: FaultSource::Runtime,
        }
    }

    pub(crate) fn rest_could_not_parse_template(message: String) -> NewEmbedderError {
        Self {
            kind: NewEmbedderErrorKind::CouldNotParseTemplate(message),
            fault: FaultSource::User,
        }
    }

    pub(crate) fn ollama_unsupported_url(url: String) -> NewEmbedderError {
        Self { kind: NewEmbedderErrorKind::OllamaUnsupportedUrl(url), fault: FaultSource::User }
    }
}

#[derive(Debug, thiserror::Error)]
#[error("could not open config at {filename}: {inner}")]
pub struct OpenConfig {
    pub filename: PathBuf,
    pub inner: std::io::Error,
}

#[derive(Debug, thiserror::Error)]
#[error("for model '{model_name}', could not deserialize config at {filename} as JSON: {inner}")]
pub struct DeserializeConfig {
    pub model_name: String,
    pub filename: PathBuf,
    pub inner: serde_json::Error,
}

#[derive(Debug, thiserror::Error)]
#[error("model `{model_name}` appears to be unsupported{}\n  - inner error: {inner}",
if architectures.is_empty() {
    "\n  - Note: only models with architecture \"BertModel\" are supported.".to_string()
} else {
    format!("\n  - Note: model has declared architectures `{architectures:?}`, only models with architecture `\"BertModel\"` are supported.")
})]
pub struct UnsupportedModel {
    pub model_name: String,
    pub inner: serde_json::Error,
    pub architectures: Vec<String>,
}

#[derive(Debug, thiserror::Error)]
#[error("could not open tokenizer at {filename}: {inner}")]
pub struct OpenTokenizer {
    pub filename: PathBuf,
    #[source]
    pub inner: Box<dyn std::error::Error + Send + Sync>,
}

#[derive(Debug, thiserror::Error)]
pub enum NewEmbedderErrorKind {
    // hf
    #[error(transparent)]
    OpenConfig(OpenConfig),
    #[error(transparent)]
    DeserializeConfig(DeserializeConfig),
    #[error(transparent)]
    UnsupportedModel(UnsupportedModel),
    #[error(transparent)]
    OpenTokenizer(OpenTokenizer),
    #[error("could not build weights from Pytorch weights:\n  - {0}")]
    PytorchWeight(candle_core::Error),
    #[error("could not build weights from Safetensor weights:\n  - {0}")]
    SafetensorWeight(candle_core::Error),
    #[error("could not spawn HG_HUB API client:\n  - {0}")]
    NewApiFail(ApiError),
    #[error("fetching file from HG_HUB failed:\n  - {0}")]
    ApiGet(ApiError),
    #[error("could not determine model dimensions:\n  - test embedding failed with {0}")]
    CouldNotDetermineDimension(EmbedError),
    #[error("loading model failed:\n  - {0}")]
    LoadModel(candle_core::Error),
    #[error("{0}")]
    CouldNotParseTemplate(String),
    #[error("unsupported Ollama URL.\n  - For `ollama` sources, the URL must end with `/api/embed` or `/api/embeddings`\n  - Got `{0}`")]
    OllamaUnsupportedUrl(String),
}

pub struct PossibleEmbeddingMistakes {
    vectors_mistakes: BTreeMap<String, u64>,
}

impl PossibleEmbeddingMistakes {
    pub fn new(field_distribution: &FieldDistribution) -> Self {
        let mut vectors_mistakes = BTreeMap::new();
        let builder = levenshtein_automata::LevenshteinAutomatonBuilder::new(2, true);
        let automata = builder.build_dfa("_vectors");
        for (field, count) in field_distribution {
            if *count == 0 {
                continue;
            }
            if field.contains('.') {
                continue;
            }
            match automata.eval(field) {
                levenshtein_automata::Distance::Exact(0) => continue,
                levenshtein_automata::Distance::Exact(_) => {
                    vectors_mistakes.insert(field.to_string(), *count);
                }
                levenshtein_automata::Distance::AtLeast(_) => continue,
            }
        }

        Self { vectors_mistakes }
    }

    pub fn vector_mistakes(&self) -> impl Iterator<Item = (&str, u64)> {
        self.vectors_mistakes.iter().map(|(misspelling, count)| (misspelling.as_str(), *count))
    }

    pub fn embedder_mistakes<'a>(
        &'a self,
        embedder_name: &'a str,
        unused_vectors_distributions: &'a UnusedVectorsDistribution,
    ) -> impl Iterator<Item = (&'a str, u64)> + 'a {
        let builder = levenshtein_automata::LevenshteinAutomatonBuilder::new(2, true);
        let automata = builder.build_dfa(embedder_name);

        unused_vectors_distributions.0.iter().filter_map(move |(field, count)| {
            match automata.eval(field) {
                levenshtein_automata::Distance::Exact(0) => None,
                levenshtein_automata::Distance::Exact(_) => Some((field.as_str(), *count)),
                levenshtein_automata::Distance::AtLeast(_) => None,
            }
        })
    }

    pub fn embedder_mistakes_bump<'a, 'doc: 'a>(
        &'a self,
        embedder_name: &'a str,
        unused_vectors_distribution: &'a UnusedVectorsDistributionBump<'doc>,
    ) -> impl Iterator<Item = (&'a str, u64)> + 'a {
        let builder = levenshtein_automata::LevenshteinAutomatonBuilder::new(2, true);
        let automata = builder.build_dfa(embedder_name);

        unused_vectors_distribution.0.iter().filter_map(move |(field, count)| {
            match automata.eval(field) {
                levenshtein_automata::Distance::Exact(0) => None,
                levenshtein_automata::Distance::Exact(_) => Some((*field, *count)),
                levenshtein_automata::Distance::AtLeast(_) => None,
            }
        })
    }
}

#[derive(Default)]
pub struct UnusedVectorsDistribution(BTreeMap<String, u64>);

impl UnusedVectorsDistribution {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn append(&mut self, parsed_vectors_diff: ParsedVectorsDiff) {
        for name in parsed_vectors_diff.into_new_vectors_keys_iter() {
            *self.0.entry(name).or_default() += 1;
        }
    }
}

pub struct UnusedVectorsDistributionBump<'doc>(
    hashbrown::HashMap<&'doc str, u64, hashbrown::DefaultHashBuilder, &'doc Bump>,
);

impl<'doc> UnusedVectorsDistributionBump<'doc> {
    pub fn new_in(doc_alloc: &'doc Bump) -> Self {
        Self(hashbrown::HashMap::new_in(doc_alloc))
    }

    pub fn append(&mut self, vectors: &impl VectorDocument<'doc>) -> Result<(), crate::Error> {
        for res in vectors.iter_vectors() {
            let (embedder_name, entry) = res?;
            if !entry.has_configured_embedder {
                *self.0.entry(embedder_name).or_default() += 1;
            }
        }
        Ok(())
    }
}
