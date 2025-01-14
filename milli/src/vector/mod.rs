use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use arroy::distances::{BinaryQuantizedCosine, Cosine};
use arroy::ItemId;
use deserr::{DeserializeError, Deserr};
use heed::{RoTxn, RwTxn, Unspecified};
use ordered_float::OrderedFloat;
use roaring::RoaringBitmap;
use serde::{Deserialize, Serialize};

use self::error::{EmbedError, NewEmbedderError};
use crate::prompt::{Prompt, PromptData};
use crate::ThreadPoolNoAbort;

pub mod error;
pub mod hf;
pub mod json_template;
pub mod manual;
pub mod openai;
pub mod parsed_vectors;
pub mod settings;

pub mod ollama;
pub mod rest;

pub use self::error::Error;

pub type Embedding = Vec<f32>;

pub const REQUEST_PARALLELISM: usize = 40;

pub struct ArroyWrapper {
    quantized: bool,
    embedder_index: u8,
    database: arroy::Database<Unspecified>,
}

impl ArroyWrapper {
    pub fn new(
        database: arroy::Database<Unspecified>,
        embedder_index: u8,
        quantized: bool,
    ) -> Self {
        Self { database, embedder_index, quantized }
    }

    pub fn embedder_index(&self) -> u8 {
        self.embedder_index
    }

    fn readers<'a, D: arroy::Distance>(
        &'a self,
        rtxn: &'a RoTxn<'a>,
        db: arroy::Database<D>,
    ) -> impl Iterator<Item = Result<arroy::Reader<D>, arroy::Error>> + 'a {
        arroy_db_range_for_embedder(self.embedder_index).map_while(move |index| {
            match arroy::Reader::open(rtxn, index, db) {
                Ok(reader) => match reader.is_empty(rtxn) {
                    Ok(false) => Some(Ok(reader)),
                    Ok(true) => None,
                    Err(e) => Some(Err(e)),
                },
                Err(arroy::Error::MissingMetadata(_)) => None,
                Err(e) => Some(Err(e)),
            }
        })
    }

    pub fn dimensions(&self, rtxn: &RoTxn) -> Result<usize, arroy::Error> {
        let first_id = arroy_db_range_for_embedder(self.embedder_index).next().unwrap();
        if self.quantized {
            Ok(arroy::Reader::open(rtxn, first_id, self.quantized_db())?.dimensions())
        } else {
            Ok(arroy::Reader::open(rtxn, first_id, self.angular_db())?.dimensions())
        }
    }

    pub fn build_and_quantize<R: rand::Rng + rand::SeedableRng>(
        &mut self,
        wtxn: &mut RwTxn,
        rng: &mut R,
        dimension: usize,
        quantizing: bool,
        cancel: &(impl Fn() -> bool + Sync + Send),
    ) -> Result<(), arroy::Error> {
        for index in arroy_db_range_for_embedder(self.embedder_index) {
            if self.quantized {
                let writer = arroy::Writer::new(self.quantized_db(), index, dimension);
                if writer.need_build(wtxn)? {
                    writer.builder(rng).build(wtxn)?
                } else if writer.is_empty(wtxn)? {
                    break;
                }
            } else {
                let writer = arroy::Writer::new(self.angular_db(), index, dimension);
                // If we are quantizing the databases, we can't know from meilisearch
                // if the db was empty but still contained the wrong metadata, thus we need
                // to quantize everything and can't stop early. Since this operation can
                // only happens once in the life of an embedder, it's not very performances
                // sensitive.
                if quantizing && !self.quantized {
                    let writer = writer.prepare_changing_distance::<BinaryQuantizedCosine>(wtxn)?;
                    writer.builder(rng).cancel(cancel).build(wtxn)?;
                } else if writer.need_build(wtxn)? {
                    writer.builder(rng).cancel(cancel).build(wtxn)?;
                } else if writer.is_empty(wtxn)? {
                    break;
                }
            }
        }
        Ok(())
    }

    /// Overwrite all the embeddings associated with the index and item ID.
    /// /!\ It won't remove embeddings after the last passed embedding, which can leave stale embeddings.
    ///     You should call `del_items` on the `item_id` before calling this method.
    /// /!\ Cannot insert more than u8::MAX embeddings; after inserting u8::MAX embeddings, all the remaining ones will be silently ignored.
    pub fn add_items(
        &self,
        wtxn: &mut RwTxn,
        item_id: arroy::ItemId,
        embeddings: &Embeddings<f32>,
    ) -> Result<(), arroy::Error> {
        let dimension = embeddings.dimension();
        for (index, vector) in
            arroy_db_range_for_embedder(self.embedder_index).zip(embeddings.iter())
        {
            if self.quantized {
                arroy::Writer::new(self.quantized_db(), index, dimension)
                    .add_item(wtxn, item_id, vector)?
            } else {
                arroy::Writer::new(self.angular_db(), index, dimension)
                    .add_item(wtxn, item_id, vector)?
            }
        }
        Ok(())
    }

    /// Add one document int for this index where we can find an empty spot.
    pub fn add_item(
        &self,
        wtxn: &mut RwTxn,
        item_id: arroy::ItemId,
        vector: &[f32],
    ) -> Result<(), arroy::Error> {
        if self.quantized {
            self._add_item(wtxn, self.quantized_db(), item_id, vector)
        } else {
            self._add_item(wtxn, self.angular_db(), item_id, vector)
        }
    }

    fn _add_item<D: arroy::Distance>(
        &self,
        wtxn: &mut RwTxn,
        db: arroy::Database<D>,
        item_id: arroy::ItemId,
        vector: &[f32],
    ) -> Result<(), arroy::Error> {
        let dimension = vector.len();

        for index in arroy_db_range_for_embedder(self.embedder_index) {
            let writer = arroy::Writer::new(db, index, dimension);
            if !writer.contains_item(wtxn, item_id)? {
                writer.add_item(wtxn, item_id, vector)?;
                break;
            }
        }
        Ok(())
    }

    /// Delete all embeddings from a specific `item_id`
    pub fn del_items(
        &self,
        wtxn: &mut RwTxn,
        dimension: usize,
        item_id: arroy::ItemId,
    ) -> Result<(), arroy::Error> {
        for index in arroy_db_range_for_embedder(self.embedder_index) {
            if self.quantized {
                let writer = arroy::Writer::new(self.quantized_db(), index, dimension);
                if !writer.del_item(wtxn, item_id)? {
                    break;
                }
            } else {
                let writer = arroy::Writer::new(self.angular_db(), index, dimension);
                if !writer.del_item(wtxn, item_id)? {
                    break;
                }
            }
        }

        Ok(())
    }

    /// Delete one item.
    pub fn del_item(
        &self,
        wtxn: &mut RwTxn,
        item_id: arroy::ItemId,
        vector: &[f32],
    ) -> Result<bool, arroy::Error> {
        if self.quantized {
            self._del_item(wtxn, self.quantized_db(), item_id, vector)
        } else {
            self._del_item(wtxn, self.angular_db(), item_id, vector)
        }
    }

    fn _del_item<D: arroy::Distance>(
        &self,
        wtxn: &mut RwTxn,
        db: arroy::Database<D>,
        item_id: arroy::ItemId,
        vector: &[f32],
    ) -> Result<bool, arroy::Error> {
        let dimension = vector.len();
        let mut deleted_index = None;

        for index in arroy_db_range_for_embedder(self.embedder_index) {
            let writer = arroy::Writer::new(db, index, dimension);
            let Some(candidate) = writer.item_vector(wtxn, item_id)? else {
                // uses invariant: vectors are packed in the first writers.
                break;
            };
            if candidate == vector {
                writer.del_item(wtxn, item_id)?;
                deleted_index = Some(index);
            }
        }

        // 🥲 enforce invariant: vectors are packed in the first writers.
        if let Some(deleted_index) = deleted_index {
            let mut last_index_with_a_vector = None;
            for index in
                arroy_db_range_for_embedder(self.embedder_index).skip(deleted_index as usize)
            {
                let writer = arroy::Writer::new(db, index, dimension);
                let Some(candidate) = writer.item_vector(wtxn, item_id)? else {
                    break;
                };
                last_index_with_a_vector = Some((index, candidate));
            }
            if let Some((last_index, vector)) = last_index_with_a_vector {
                let writer = arroy::Writer::new(db, last_index, dimension);
                writer.del_item(wtxn, item_id)?;
                let writer = arroy::Writer::new(db, deleted_index, dimension);
                writer.add_item(wtxn, item_id, &vector)?;
            }
        }
        Ok(deleted_index.is_some())
    }

    pub fn clear(&self, wtxn: &mut RwTxn, dimension: usize) -> Result<(), arroy::Error> {
        for index in arroy_db_range_for_embedder(self.embedder_index) {
            if self.quantized {
                let writer = arroy::Writer::new(self.quantized_db(), index, dimension);
                if writer.is_empty(wtxn)? {
                    break;
                }
                writer.clear(wtxn)?;
            } else {
                let writer = arroy::Writer::new(self.angular_db(), index, dimension);
                if writer.is_empty(wtxn)? {
                    break;
                }
                writer.clear(wtxn)?;
            }
        }
        Ok(())
    }

    pub fn contains_item(
        &self,
        rtxn: &RoTxn,
        dimension: usize,
        item: arroy::ItemId,
    ) -> Result<bool, arroy::Error> {
        for index in arroy_db_range_for_embedder(self.embedder_index) {
            let contains = if self.quantized {
                let writer = arroy::Writer::new(self.quantized_db(), index, dimension);
                if writer.is_empty(rtxn)? {
                    break;
                }
                writer.contains_item(rtxn, item)?
            } else {
                let writer = arroy::Writer::new(self.angular_db(), index, dimension);
                if writer.is_empty(rtxn)? {
                    break;
                }
                writer.contains_item(rtxn, item)?
            };
            if contains {
                return Ok(contains);
            }
        }
        Ok(false)
    }

    pub fn nns_by_item(
        &self,
        rtxn: &RoTxn,
        item: ItemId,
        limit: usize,
        filter: Option<&RoaringBitmap>,
    ) -> Result<Vec<(ItemId, f32)>, arroy::Error> {
        if self.quantized {
            self._nns_by_item(rtxn, self.quantized_db(), item, limit, filter)
        } else {
            self._nns_by_item(rtxn, self.angular_db(), item, limit, filter)
        }
    }

    fn _nns_by_item<D: arroy::Distance>(
        &self,
        rtxn: &RoTxn,
        db: arroy::Database<D>,
        item: ItemId,
        limit: usize,
        filter: Option<&RoaringBitmap>,
    ) -> Result<Vec<(ItemId, f32)>, arroy::Error> {
        let mut results = Vec::new();

        for reader in self.readers(rtxn, db) {
            let reader = reader?;
            let mut searcher = reader.nns(limit);
            if let Some(filter) = filter {
                searcher.candidates(filter);
            }

            if let Some(mut ret) = searcher.by_item(rtxn, item)? {
                results.append(&mut ret);
            } else {
                break;
            }
        }
        results.sort_unstable_by_key(|(_, distance)| OrderedFloat(*distance));
        Ok(results)
    }

    pub fn nns_by_vector(
        &self,
        rtxn: &RoTxn,
        vector: &[f32],
        limit: usize,
        filter: Option<&RoaringBitmap>,
    ) -> Result<Vec<(ItemId, f32)>, arroy::Error> {
        if self.quantized {
            self._nns_by_vector(rtxn, self.quantized_db(), vector, limit, filter)
        } else {
            self._nns_by_vector(rtxn, self.angular_db(), vector, limit, filter)
        }
    }

    fn _nns_by_vector<D: arroy::Distance>(
        &self,
        rtxn: &RoTxn,
        db: arroy::Database<D>,
        vector: &[f32],
        limit: usize,
        filter: Option<&RoaringBitmap>,
    ) -> Result<Vec<(ItemId, f32)>, arroy::Error> {
        let mut results = Vec::new();

        for reader in self.readers(rtxn, db) {
            let reader = reader?;
            let mut searcher = reader.nns(limit);
            if let Some(filter) = filter {
                searcher.candidates(filter);
            }

            results.append(&mut searcher.by_vector(rtxn, vector)?);
        }

        results.sort_unstable_by_key(|(_, distance)| OrderedFloat(*distance));

        Ok(results)
    }

    pub fn item_vectors(&self, rtxn: &RoTxn, item_id: u32) -> Result<Vec<Vec<f32>>, arroy::Error> {
        let mut vectors = Vec::new();

        if self.quantized {
            for reader in self.readers(rtxn, self.quantized_db()) {
                if let Some(vec) = reader?.item_vector(rtxn, item_id)? {
                    vectors.push(vec);
                } else {
                    break;
                }
            }
        } else {
            for reader in self.readers(rtxn, self.angular_db()) {
                if let Some(vec) = reader?.item_vector(rtxn, item_id)? {
                    vectors.push(vec);
                } else {
                    break;
                }
            }
        }
        Ok(vectors)
    }

    fn angular_db(&self) -> arroy::Database<Cosine> {
        self.database.remap_data_type()
    }

    fn quantized_db(&self) -> arroy::Database<BinaryQuantizedCosine> {
        self.database.remap_data_type()
    }
}

/// One or multiple embeddings stored consecutively in a flat vector.
pub struct Embeddings<F> {
    data: Vec<F>,
    dimension: usize,
}

impl<F> Embeddings<F> {
    /// Declares an empty  vector of embeddings of the specified dimensions.
    pub fn new(dimension: usize) -> Self {
        Self { data: Default::default(), dimension }
    }

    /// Declares a vector of embeddings containing a single element.
    ///
    /// The dimension is inferred from the length of the passed embedding.
    pub fn from_single_embedding(embedding: Vec<F>) -> Self {
        Self { dimension: embedding.len(), data: embedding }
    }

    /// Declares a vector of embeddings from its components.
    ///
    /// `data.len()` must be a multiple of `dimension`, otherwise an error is returned.
    pub fn from_inner(data: Vec<F>, dimension: usize) -> Result<Self, Vec<F>> {
        let mut this = Self::new(dimension);
        this.append(data)?;
        Ok(this)
    }

    /// Returns the number of embeddings in this vector of embeddings.
    pub fn embedding_count(&self) -> usize {
        self.data.len() / self.dimension
    }

    /// Dimension of a single embedding.
    pub fn dimension(&self) -> usize {
        self.dimension
    }

    /// Deconstructs self into the inner flat vector.
    pub fn into_inner(self) -> Vec<F> {
        self.data
    }

    /// A reference to the inner flat vector.
    pub fn as_inner(&self) -> &[F] {
        &self.data
    }

    /// Iterates over the embeddings contained in the flat vector.
    pub fn iter(&self) -> impl Iterator<Item = &'_ [F]> + '_ {
        self.data.as_slice().chunks_exact(self.dimension)
    }

    /// Push an embedding at the end of the embeddings.
    ///
    /// If `embedding.len() != self.dimension`, then the push operation fails.
    pub fn push(&mut self, mut embedding: Vec<F>) -> Result<(), Vec<F>> {
        if embedding.len() != self.dimension {
            return Err(embedding);
        }
        self.data.append(&mut embedding);
        Ok(())
    }

    /// Append a flat vector of embeddings a the end of the embeddings.
    ///
    /// If `embeddings.len() % self.dimension != 0`, then the append operation fails.
    pub fn append(&mut self, mut embeddings: Vec<F>) -> Result<(), Vec<F>> {
        if embeddings.len() % self.dimension != 0 {
            return Err(embeddings);
        }
        self.data.append(&mut embeddings);
        Ok(())
    }
}

/// An embedder can be used to transform text into embeddings.
#[derive(Debug)]
pub enum Embedder {
    /// An embedder based on running local models, fetched from the Hugging Face Hub.
    HuggingFace(hf::Embedder),
    /// An embedder based on making embedding queries against the OpenAI API.
    OpenAi(openai::Embedder),
    /// An embedder based on the user providing the embeddings in the documents and queries.
    UserProvided(manual::Embedder),
    /// An embedder based on making embedding queries against an <https://ollama.com> embedding server.
    Ollama(ollama::Embedder),
    /// An embedder based on making embedding queries against a generic JSON/REST embedding server.
    Rest(rest::Embedder),
}

/// Configuration for an embedder.
#[derive(Debug, Clone, Default, serde::Deserialize, serde::Serialize)]
pub struct EmbeddingConfig {
    /// Options of the embedder, specific to each kind of embedder
    pub embedder_options: EmbedderOptions,
    /// Document template
    pub prompt: PromptData,
    /// If this embedder is binary quantized
    pub quantized: Option<bool>,
    // TODO: add metrics and anything needed
}

impl EmbeddingConfig {
    pub fn quantized(&self) -> bool {
        self.quantized.unwrap_or_default()
    }
}

/// Map of embedder configurations.
///
/// Each configuration is mapped to a name.
#[derive(Clone, Default)]
pub struct EmbeddingConfigs(HashMap<String, (Arc<Embedder>, Arc<Prompt>, bool)>);

impl EmbeddingConfigs {
    /// Create the map from its internal component.s
    pub fn new(data: HashMap<String, (Arc<Embedder>, Arc<Prompt>, bool)>) -> Self {
        Self(data)
    }

    /// Get an embedder configuration and template from its name.
    pub fn get(&self, name: &str) -> Option<(Arc<Embedder>, Arc<Prompt>, bool)> {
        self.0.get(name).cloned()
    }

    pub fn inner_as_ref(&self) -> &HashMap<String, (Arc<Embedder>, Arc<Prompt>, bool)> {
        &self.0
    }

    pub fn into_inner(self) -> HashMap<String, (Arc<Embedder>, Arc<Prompt>, bool)> {
        self.0
    }
}

impl IntoIterator for EmbeddingConfigs {
    type Item = (String, (Arc<Embedder>, Arc<Prompt>, bool));

    type IntoIter =
        std::collections::hash_map::IntoIter<String, (Arc<Embedder>, Arc<Prompt>, bool)>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

/// Options of an embedder, specific to each kind of embedder.
#[derive(Debug, Clone, Hash, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum EmbedderOptions {
    HuggingFace(hf::EmbedderOptions),
    OpenAi(openai::EmbedderOptions),
    Ollama(ollama::EmbedderOptions),
    UserProvided(manual::EmbedderOptions),
    Rest(rest::EmbedderOptions),
}

impl Default for EmbedderOptions {
    fn default() -> Self {
        Self::HuggingFace(Default::default())
    }
}

impl Embedder {
    /// Spawns a new embedder built from its options.
    pub fn new(options: EmbedderOptions) -> std::result::Result<Self, NewEmbedderError> {
        Ok(match options {
            EmbedderOptions::HuggingFace(options) => Self::HuggingFace(hf::Embedder::new(options)?),
            EmbedderOptions::OpenAi(options) => Self::OpenAi(openai::Embedder::new(options)?),
            EmbedderOptions::Ollama(options) => Self::Ollama(ollama::Embedder::new(options)?),
            EmbedderOptions::UserProvided(options) => {
                Self::UserProvided(manual::Embedder::new(options))
            }
            EmbedderOptions::Rest(options) => {
                Self::Rest(rest::Embedder::new(options, rest::ConfigurationSource::User)?)
            }
        })
    }

    /// Embed one or multiple texts.
    ///
    /// Each text can be embedded as one or multiple embeddings.
    pub fn embed(
        &self,
        texts: Vec<String>,
        deadline: Option<Instant>,
    ) -> std::result::Result<Vec<Embeddings<f32>>, EmbedError> {
        match self {
            Embedder::HuggingFace(embedder) => embedder.embed(texts),
            Embedder::OpenAi(embedder) => embedder.embed(texts, deadline),
            Embedder::Ollama(embedder) => embedder.embed(texts, deadline),
            Embedder::UserProvided(embedder) => embedder.embed(texts),
            Embedder::Rest(embedder) => embedder.embed(texts, deadline),
        }
    }

    #[tracing::instrument(level = "debug", skip_all, target = "search")]
    pub fn embed_one(
        &self,
        text: String,
        deadline: Option<Instant>,
    ) -> std::result::Result<Embedding, EmbedError> {
        let mut embeddings = self.embed(vec![text], deadline)?;
        let embeddings = embeddings.pop().ok_or_else(EmbedError::missing_embedding)?;
        Ok(if embeddings.iter().nth(1).is_some() {
            tracing::warn!("Ignoring embeddings past the first one in long search query");
            embeddings.iter().next().unwrap().to_vec()
        } else {
            embeddings.into_inner()
        })
    }

    /// Embed multiple chunks of texts.
    ///
    /// Each chunk is composed of one or multiple texts.
    pub fn embed_chunks(
        &self,
        text_chunks: Vec<Vec<String>>,
        threads: &ThreadPoolNoAbort,
    ) -> std::result::Result<Vec<Vec<Embeddings<f32>>>, EmbedError> {
        match self {
            Embedder::HuggingFace(embedder) => embedder.embed_chunks(text_chunks),
            Embedder::OpenAi(embedder) => embedder.embed_chunks(text_chunks, threads),
            Embedder::Ollama(embedder) => embedder.embed_chunks(text_chunks, threads),
            Embedder::UserProvided(embedder) => embedder.embed_chunks(text_chunks),
            Embedder::Rest(embedder) => embedder.embed_chunks(text_chunks, threads),
        }
    }

    /// Indicates the preferred number of chunks to pass to [`Self::embed_chunks`]
    pub fn chunk_count_hint(&self) -> usize {
        match self {
            Embedder::HuggingFace(embedder) => embedder.chunk_count_hint(),
            Embedder::OpenAi(embedder) => embedder.chunk_count_hint(),
            Embedder::Ollama(embedder) => embedder.chunk_count_hint(),
            Embedder::UserProvided(_) => 1,
            Embedder::Rest(embedder) => embedder.chunk_count_hint(),
        }
    }

    /// Indicates the preferred number of texts in a single chunk passed to [`Self::embed`]
    pub fn prompt_count_in_chunk_hint(&self) -> usize {
        match self {
            Embedder::HuggingFace(embedder) => embedder.prompt_count_in_chunk_hint(),
            Embedder::OpenAi(embedder) => embedder.prompt_count_in_chunk_hint(),
            Embedder::Ollama(embedder) => embedder.prompt_count_in_chunk_hint(),
            Embedder::UserProvided(_) => 1,
            Embedder::Rest(embedder) => embedder.prompt_count_in_chunk_hint(),
        }
    }

    /// Indicates the dimensions of a single embedding produced by the embedder.
    pub fn dimensions(&self) -> usize {
        match self {
            Embedder::HuggingFace(embedder) => embedder.dimensions(),
            Embedder::OpenAi(embedder) => embedder.dimensions(),
            Embedder::Ollama(embedder) => embedder.dimensions(),
            Embedder::UserProvided(embedder) => embedder.dimensions(),
            Embedder::Rest(embedder) => embedder.dimensions(),
        }
    }

    /// An optional distribution used to apply an affine transformation to the similarity score of a document.
    pub fn distribution(&self) -> Option<DistributionShift> {
        match self {
            Embedder::HuggingFace(embedder) => embedder.distribution(),
            Embedder::OpenAi(embedder) => embedder.distribution(),
            Embedder::Ollama(embedder) => embedder.distribution(),
            Embedder::UserProvided(embedder) => embedder.distribution(),
            Embedder::Rest(embedder) => embedder.distribution(),
        }
    }

    pub fn uses_document_template(&self) -> bool {
        match self {
            Embedder::HuggingFace(_)
            | Embedder::OpenAi(_)
            | Embedder::Ollama(_)
            | Embedder::Rest(_) => true,
            Embedder::UserProvided(_) => false,
        }
    }
}

/// Describes the mean and sigma of distribution of embedding similarity in the embedding space.
///
/// The intended use is to make the similarity score more comparable to the regular ranking score.
/// This allows to correct effects where results are too "packed" around a certain value.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Deserialize, Serialize)]
#[serde(from = "DistributionShiftSerializable")]
#[serde(into = "DistributionShiftSerializable")]
pub struct DistributionShift {
    /// Value where the results are "packed".
    ///
    /// Similarity scores are translated so that they are packed around 0.5 instead
    pub current_mean: OrderedFloat<f32>,

    /// standard deviation of a similarity score.
    ///
    /// Set below 0.4 to make the results less packed around the mean, and above 0.4 to make them more packed.
    pub current_sigma: OrderedFloat<f32>,
}

impl<E> Deserr<E> for DistributionShift
where
    E: DeserializeError,
{
    fn deserialize_from_value<V: deserr::IntoValue>(
        value: deserr::Value<V>,
        location: deserr::ValuePointerRef<'_>,
    ) -> Result<Self, E> {
        let value = DistributionShiftSerializable::deserialize_from_value(value, location)?;
        if value.mean < 0. || value.mean > 1. {
            return Err(deserr::take_cf_content(E::error::<std::convert::Infallible>(
                None,
                deserr::ErrorKind::Unexpected {
                    msg: format!(
                        "the distribution mean must be in the range [0, 1], got {}",
                        value.mean
                    ),
                },
                location,
            )));
        }
        if value.sigma <= 0. || value.sigma > 1. {
            return Err(deserr::take_cf_content(E::error::<std::convert::Infallible>(
                None,
                deserr::ErrorKind::Unexpected {
                    msg: format!(
                        "the distribution sigma must be in the range ]0, 1], got {}",
                        value.sigma
                    ),
                },
                location,
            )));
        }

        Ok(value.into())
    }
}

#[derive(Serialize, Deserialize, Deserr)]
#[serde(deny_unknown_fields)]
#[deserr(deny_unknown_fields)]
struct DistributionShiftSerializable {
    mean: f32,
    sigma: f32,
}

impl From<DistributionShift> for DistributionShiftSerializable {
    fn from(
        DistributionShift {
            current_mean: OrderedFloat(current_mean),
            current_sigma: OrderedFloat(current_sigma),
        }: DistributionShift,
    ) -> Self {
        Self { mean: current_mean, sigma: current_sigma }
    }
}

impl From<DistributionShiftSerializable> for DistributionShift {
    fn from(DistributionShiftSerializable { mean, sigma }: DistributionShiftSerializable) -> Self {
        Self { current_mean: OrderedFloat(mean), current_sigma: OrderedFloat(sigma) }
    }
}

impl DistributionShift {
    /// `None` if sigma <= 0.
    pub fn new(mean: f32, sigma: f32) -> Option<Self> {
        if sigma <= 0.0 {
            None
        } else {
            Some(Self { current_mean: OrderedFloat(mean), current_sigma: OrderedFloat(sigma) })
        }
    }

    pub fn shift(&self, score: f32) -> f32 {
        let current_mean = self.current_mean.0;
        let current_sigma = self.current_sigma.0;
        // <https://math.stackexchange.com/a/2894689>
        // We're somewhat abusively mapping the distribution of distances to a gaussian.
        // The parameters we're given is the mean and sigma of the native result distribution.
        // We're using them to retarget the distribution to a gaussian centered on 0.5 with a sigma of 0.4.

        let target_mean = 0.5;
        let target_sigma = 0.4;

        // a^2 sig1^2 = sig2^2 => a^2 = sig2^2 / sig1^2 => a = sig2 / sig1, assuming a, sig1, and sig2 positive.
        let factor = target_sigma / current_sigma;
        // a*mu1 + b = mu2 => b = mu2 - a*mu1
        let offset = target_mean - (factor * current_mean);

        let mut score = factor * score + offset;

        // clamp the final score in the ]0, 1] interval.
        if score <= 0.0 {
            score = f32::EPSILON;
        }
        if score > 1.0 {
            score = 1.0;
        }

        score
    }
}

/// Whether CUDA is supported in this version of Meilisearch.
pub const fn is_cuda_enabled() -> bool {
    cfg!(feature = "cuda")
}

pub fn arroy_db_range_for_embedder(embedder_id: u8) -> impl Iterator<Item = u16> {
    let embedder_id = (embedder_id as u16) << 8;

    (0..=u8::MAX).map(move |k| embedder_id | (k as u16))
}
