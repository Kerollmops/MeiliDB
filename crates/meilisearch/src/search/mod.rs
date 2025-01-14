use core::fmt;
use std::cmp::min;
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use deserr::Deserr;
use either::Either;
use index_scheduler::RoFeatures;
use indexmap::IndexMap;
use meilisearch_auth::IndexSearchRules;
use meilisearch_types::deserr::DeserrJsonError;
use meilisearch_types::error::deserr_codes::*;
use meilisearch_types::error::{Code, ResponseError};
use meilisearch_types::heed::RoTxn;
use meilisearch_types::index_uid::IndexUid;
use meilisearch_types::locales::Locale;
use meilisearch_types::milli::score_details::{ScoreDetails, ScoringStrategy};
use meilisearch_types::milli::vector::parsed_vectors::ExplicitVectors;
use meilisearch_types::milli::vector::Embedder;
use meilisearch_types::milli::{
    FacetValueHit, InternalError, OrderBy, SearchForFacetValues, TimeBudget,
};
use meilisearch_types::settings::DEFAULT_PAGINATION_MAX_TOTAL_HITS;
use meilisearch_types::{milli, Document};
use milli::tokenizer::{Language, TokenizerBuilder};
use milli::{
    AscDesc, FieldId, FieldsIdsMap, Filter, FormatOptions, Index, LocalizedAttributesRule,
    MatchBounds, MatcherBuilder, SortError, TermsMatchingStrategy, DEFAULT_VALUES_PER_FACET,
};
use regex::Regex;
use serde::Serialize;
use serde_json::{json, Value};
#[cfg(test)]
mod mod_test;
use utoipa::ToSchema;

use crate::error::MeilisearchHttpError;

mod federated;
pub use federated::{
    perform_federated_search, FederatedSearch, FederatedSearchResult, Federation,
    FederationOptions, MergeFacets,
};

mod ranking_rules;

type MatchesPosition = BTreeMap<String, Vec<MatchBounds>>;

pub const DEFAULT_SEARCH_OFFSET: fn() -> usize = || 0;
pub const DEFAULT_SEARCH_LIMIT: fn() -> usize = || 20;
pub const DEFAULT_CROP_LENGTH: fn() -> usize = || 10;
pub const DEFAULT_CROP_MARKER: fn() -> String = || "…".to_string();
pub const DEFAULT_HIGHLIGHT_PRE_TAG: fn() -> String = || "<em>".to_string();
pub const DEFAULT_HIGHLIGHT_POST_TAG: fn() -> String = || "</em>".to_string();
pub const DEFAULT_SEMANTIC_RATIO: fn() -> SemanticRatio = || SemanticRatio(0.5);

#[derive(Clone, Default, PartialEq, Deserr, ToSchema)]
#[deserr(error = DeserrJsonError, rename_all = camelCase, deny_unknown_fields)]
pub struct SearchQuery {
    #[deserr(default, error = DeserrJsonError<InvalidSearchQ>)]
    pub q: Option<String>,
    #[deserr(default, error = DeserrJsonError<InvalidSearchVector>)]
    pub vector: Option<Vec<f32>>,
    #[deserr(default, error = DeserrJsonError<InvalidSearchHybridQuery>)]
    pub hybrid: Option<HybridQuery>,
    #[deserr(default = DEFAULT_SEARCH_OFFSET(), error = DeserrJsonError<InvalidSearchOffset>)]
    #[schema(default = DEFAULT_SEARCH_OFFSET)]
    pub offset: usize,
    #[deserr(default = DEFAULT_SEARCH_LIMIT(), error = DeserrJsonError<InvalidSearchLimit>)]
    #[schema(default = DEFAULT_SEARCH_LIMIT)]
    pub limit: usize,
    #[deserr(default, error = DeserrJsonError<InvalidSearchPage>)]
    pub page: Option<usize>,
    #[deserr(default, error = DeserrJsonError<InvalidSearchHitsPerPage>)]
    pub hits_per_page: Option<usize>,
    #[deserr(default, error = DeserrJsonError<InvalidSearchAttributesToRetrieve>)]
    pub attributes_to_retrieve: Option<BTreeSet<String>>,
    #[deserr(default, error = DeserrJsonError<InvalidSearchRetrieveVectors>)]
    pub retrieve_vectors: bool,
    #[deserr(default, error = DeserrJsonError<InvalidSearchAttributesToCrop>)]
    pub attributes_to_crop: Option<Vec<String>>,
    #[deserr(error = DeserrJsonError<InvalidSearchCropLength>, default = DEFAULT_CROP_LENGTH())]
    #[schema(default = DEFAULT_CROP_LENGTH)]
    pub crop_length: usize,
    #[deserr(default, error = DeserrJsonError<InvalidSearchAttributesToHighlight>)]
    pub attributes_to_highlight: Option<HashSet<String>>,
    #[deserr(default, error = DeserrJsonError<InvalidSearchShowMatchesPosition>)]
    pub show_matches_position: bool,
    #[deserr(default, error = DeserrJsonError<InvalidSearchShowRankingScore>)]
    pub show_ranking_score: bool,
    #[deserr(default, error = DeserrJsonError<InvalidSearchShowRankingScoreDetails>)]
    pub show_ranking_score_details: bool,
    #[deserr(default, error = DeserrJsonError<InvalidSearchFilter>)]
    pub filter: Option<Value>,
    #[deserr(default, error = DeserrJsonError<InvalidSearchSort>)]
    pub sort: Option<Vec<String>>,
    #[deserr(default, error = DeserrJsonError<InvalidSearchDistinct>)]
    pub distinct: Option<String>,
    #[deserr(default, error = DeserrJsonError<InvalidSearchFacets>)]
    pub facets: Option<Vec<String>>,
    #[deserr(error = DeserrJsonError<InvalidSearchHighlightPreTag>, default = DEFAULT_HIGHLIGHT_PRE_TAG())]
    #[schema(default = DEFAULT_HIGHLIGHT_PRE_TAG)]
    pub highlight_pre_tag: String,
    #[deserr(error = DeserrJsonError<InvalidSearchHighlightPostTag>, default = DEFAULT_HIGHLIGHT_POST_TAG())]
    #[schema(default = DEFAULT_HIGHLIGHT_POST_TAG)]
    pub highlight_post_tag: String,
    #[deserr(error = DeserrJsonError<InvalidSearchCropMarker>, default = DEFAULT_CROP_MARKER())]
    #[schema(default = DEFAULT_CROP_MARKER)]
    pub crop_marker: String,
    #[deserr(default, error = DeserrJsonError<InvalidSearchMatchingStrategy>)]
    pub matching_strategy: MatchingStrategy,
    #[deserr(default, error = DeserrJsonError<InvalidSearchAttributesToSearchOn>)]
    pub attributes_to_search_on: Option<Vec<String>>,
    #[deserr(default, error = DeserrJsonError<InvalidSearchRankingScoreThreshold>)]
    pub ranking_score_threshold: Option<RankingScoreThreshold>,
    #[deserr(default, error = DeserrJsonError<InvalidSearchLocales>)]
    pub locales: Option<Vec<Locale>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Deserr, ToSchema)]
#[deserr(try_from(f64) = TryFrom::try_from -> InvalidSearchRankingScoreThreshold)]
pub struct RankingScoreThreshold(f64);
impl std::convert::TryFrom<f64> for RankingScoreThreshold {
    type Error = InvalidSearchRankingScoreThreshold;

    fn try_from(f: f64) -> Result<Self, Self::Error> {
        // the suggested "fix" is: `!(0.0..=1.0).contains(&f)`` which is allegedly less readable
        #[allow(clippy::manual_range_contains)]
        if f > 1.0 || f < 0.0 {
            Err(InvalidSearchRankingScoreThreshold)
        } else {
            Ok(RankingScoreThreshold(f))
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Deserr)]
#[deserr(try_from(f64) = TryFrom::try_from -> InvalidSimilarRankingScoreThreshold)]
pub struct RankingScoreThresholdSimilar(f64);

impl std::convert::TryFrom<f64> for RankingScoreThresholdSimilar {
    type Error = InvalidSimilarRankingScoreThreshold;

    fn try_from(f: f64) -> Result<Self, Self::Error> {
        // the suggested "fix" is: `!(0.0..=1.0).contains(&f)`` which is allegedly less readable
        #[allow(clippy::manual_range_contains)]
        if f > 1.0 || f < 0.0 {
            Err(InvalidSimilarRankingScoreThreshold)
        } else {
            Ok(Self(f))
        }
    }
}

// Since this structure is logged A LOT we're going to reduce the number of things it logs to the bare minimum.
// - Only what IS used, we know everything else is set to None so there is no need to print it
// - Re-order the most important field to debug first
impl fmt::Debug for SearchQuery {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Self {
            q,
            vector,
            hybrid,
            offset,
            limit,
            page,
            hits_per_page,
            attributes_to_retrieve,
            retrieve_vectors,
            attributes_to_crop,
            crop_length,
            attributes_to_highlight,
            show_matches_position,
            show_ranking_score,
            show_ranking_score_details,
            filter,
            sort,
            distinct,
            facets,
            highlight_pre_tag,
            highlight_post_tag,
            crop_marker,
            matching_strategy,
            attributes_to_search_on,
            ranking_score_threshold,
            locales,
        } = self;

        let mut debug = f.debug_struct("SearchQuery");

        // First, everything related to the number of documents to retrieve
        debug.field("limit", &limit).field("offset", &offset);
        if let Some(page) = page {
            debug.field("page", &page);
        }
        if let Some(hits_per_page) = hits_per_page {
            debug.field("hits_per_page", &hits_per_page);
        }

        // Then, everything related to the queries
        if let Some(q) = q {
            debug.field("q", &q);
        }
        if *retrieve_vectors {
            debug.field("retrieve_vectors", &retrieve_vectors);
        }
        if let Some(v) = vector {
            if v.len() < 10 {
                debug.field("vector", &v);
            } else {
                debug.field(
                    "vector",
                    &format!("[{}, {}, {}, ... {} dimensions]", v[0], v[1], v[2], v.len()),
                );
            }
        }
        if let Some(hybrid) = hybrid {
            debug.field("hybrid", &hybrid);
        }
        if let Some(attributes_to_search_on) = attributes_to_search_on {
            debug.field("attributes_to_search_on", &attributes_to_search_on);
        }
        if let Some(filter) = filter {
            debug.field("filter", &filter);
        }
        if let Some(sort) = sort {
            debug.field("sort", &sort);
        }
        if let Some(distinct) = distinct {
            debug.field("distinct", &distinct);
        }
        if let Some(facets) = facets {
            debug.field("facets", &facets);
        }
        debug.field("matching_strategy", &matching_strategy);

        // Then everything related to the formatting
        debug.field("crop_length", &crop_length);
        if *show_matches_position {
            debug.field("show_matches_position", show_matches_position);
        }
        if *show_ranking_score {
            debug.field("show_ranking_score", show_ranking_score);
        }
        if *show_ranking_score_details {
            debug.field("self.show_ranking_score_details", show_ranking_score_details);
        }
        debug.field("crop_length", &crop_length);
        if let Some(facets) = facets {
            debug.field("facets", &facets);
        }
        if let Some(attributes_to_retrieve) = attributes_to_retrieve {
            debug.field("attributes_to_retrieve", &attributes_to_retrieve);
        }
        if let Some(attributes_to_crop) = attributes_to_crop {
            debug.field("attributes_to_crop", &attributes_to_crop);
        }
        if let Some(attributes_to_highlight) = attributes_to_highlight {
            debug.field("attributes_to_highlight", &attributes_to_highlight);
        }
        debug.field("highlight_pre_tag", &highlight_pre_tag);
        debug.field("highlight_post_tag", &highlight_post_tag);
        debug.field("crop_marker", &crop_marker);
        if let Some(ranking_score_threshold) = ranking_score_threshold {
            debug.field("ranking_score_threshold", &ranking_score_threshold);
        }

        if let Some(locales) = locales {
            debug.field("locales", &locales);
        }

        debug.finish()
    }
}

#[derive(Debug, Clone, Default, PartialEq, Deserr, ToSchema)]
#[deserr(error = DeserrJsonError<InvalidSearchHybridQuery>, rename_all = camelCase, deny_unknown_fields)]
pub struct HybridQuery {
    #[deserr(default, error = DeserrJsonError<InvalidSearchSemanticRatio>, default)]
    #[schema(value_type = f32, default)]
    pub semantic_ratio: SemanticRatio,
    #[deserr(error = DeserrJsonError<InvalidSearchEmbedder>)]
    pub embedder: String,
}

#[derive(Clone)]
pub enum SearchKind {
    KeywordOnly,
    SemanticOnly { embedder_name: String, embedder: Arc<Embedder>, quantized: bool },
    Hybrid { embedder_name: String, embedder: Arc<Embedder>, quantized: bool, semantic_ratio: f32 },
}

impl SearchKind {
    pub(crate) fn semantic(
        index_scheduler: &index_scheduler::IndexScheduler,
        index_uid: String,
        index: &Index,
        embedder_name: &str,
        vector_len: Option<usize>,
    ) -> Result<Self, ResponseError> {
        let (embedder_name, embedder, quantized) = Self::embedder(
            index_scheduler,
            index_uid,
            index,
            embedder_name,
            vector_len,
            Route::Search,
        )?;
        Ok(Self::SemanticOnly { embedder_name, embedder, quantized })
    }

    pub(crate) fn hybrid(
        index_scheduler: &index_scheduler::IndexScheduler,
        index_uid: String,
        index: &Index,
        embedder_name: &str,
        semantic_ratio: f32,
        vector_len: Option<usize>,
    ) -> Result<Self, ResponseError> {
        let (embedder_name, embedder, quantized) = Self::embedder(
            index_scheduler,
            index_uid,
            index,
            embedder_name,
            vector_len,
            Route::Search,
        )?;
        Ok(Self::Hybrid { embedder_name, embedder, quantized, semantic_ratio })
    }

    pub(crate) fn embedder(
        index_scheduler: &index_scheduler::IndexScheduler,
        index_uid: String,
        index: &Index,
        embedder_name: &str,
        vector_len: Option<usize>,
        route: Route,
    ) -> Result<(String, Arc<Embedder>, bool), ResponseError> {
        let embedder_configs = index.embedding_configs(&index.read_txn()?)?;
        let embedders = index_scheduler.embedders(index_uid, embedder_configs)?;

        let (embedder, _, quantized) = embedders
            .get(embedder_name)
            .ok_or(match route {
                Route::Search | Route::MultiSearch => {
                    milli::UserError::InvalidSearchEmbedder(embedder_name.to_owned())
                }
                Route::Similar => {
                    milli::UserError::InvalidSimilarEmbedder(embedder_name.to_owned())
                }
            })
            .map_err(milli::Error::from)?;

        if let Some(vector_len) = vector_len {
            if vector_len != embedder.dimensions() {
                return Err(meilisearch_types::milli::Error::UserError(
                    meilisearch_types::milli::UserError::InvalidVectorDimensions {
                        expected: embedder.dimensions(),
                        found: vector_len,
                    },
                )
                .into());
            }
        }

        Ok((embedder_name.to_owned(), embedder, quantized))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Deserr)]
#[deserr(try_from(f32) = TryFrom::try_from -> InvalidSearchSemanticRatio)]
pub struct SemanticRatio(f32);

impl Default for SemanticRatio {
    fn default() -> Self {
        DEFAULT_SEMANTIC_RATIO()
    }
}

impl std::convert::TryFrom<f32> for SemanticRatio {
    type Error = InvalidSearchSemanticRatio;

    fn try_from(f: f32) -> Result<Self, Self::Error> {
        // the suggested "fix" is: `!(0.0..=1.0).contains(&f)`` which is allegedly less readable
        #[allow(clippy::manual_range_contains)]
        if f > 1.0 || f < 0.0 {
            Err(InvalidSearchSemanticRatio)
        } else {
            Ok(SemanticRatio(f))
        }
    }
}

impl std::ops::Deref for SemanticRatio {
    type Target = f32;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl SearchQuery {
    pub fn is_finite_pagination(&self) -> bool {
        self.page.or(self.hits_per_page).is_some()
    }
}

/// A `SearchQuery` + an index UID and optional FederationOptions.
// This struct contains the fields of `SearchQuery` inline.
// This is because neither deserr nor serde support `flatten` when using `deny_unknown_fields.
// The `From<SearchQueryWithIndex>` implementation ensures both structs remain up to date.
#[derive(Debug, Clone, PartialEq, Deserr, ToSchema)]
#[deserr(error = DeserrJsonError, rename_all = camelCase, deny_unknown_fields)]
#[schema(rename_all = "camelCase")]
pub struct SearchQueryWithIndex {
    #[deserr(error = DeserrJsonError<InvalidIndexUid>, missing_field_error = DeserrJsonError::missing_index_uid)]
    pub index_uid: IndexUid,
    #[deserr(default, error = DeserrJsonError<InvalidSearchQ>)]
    pub q: Option<String>,
    #[deserr(default, error = DeserrJsonError<InvalidSearchQ>)]
    pub vector: Option<Vec<f32>>,
    #[deserr(default, error = DeserrJsonError<InvalidSearchHybridQuery>)]
    pub hybrid: Option<HybridQuery>,
    #[deserr(default, error = DeserrJsonError<InvalidSearchOffset>)]
    pub offset: Option<usize>,
    #[deserr(default, error = DeserrJsonError<InvalidSearchLimit>)]
    pub limit: Option<usize>,
    #[deserr(default, error = DeserrJsonError<InvalidSearchPage>)]
    pub page: Option<usize>,
    #[deserr(default, error = DeserrJsonError<InvalidSearchHitsPerPage>)]
    pub hits_per_page: Option<usize>,
    #[deserr(default, error = DeserrJsonError<InvalidSearchAttributesToRetrieve>)]
    pub attributes_to_retrieve: Option<BTreeSet<String>>,
    #[deserr(default, error = DeserrJsonError<InvalidSearchRetrieveVectors>)]
    pub retrieve_vectors: bool,
    #[deserr(default, error = DeserrJsonError<InvalidSearchAttributesToCrop>)]
    pub attributes_to_crop: Option<Vec<String>>,
    #[deserr(default, error = DeserrJsonError<InvalidSearchCropLength>, default = DEFAULT_CROP_LENGTH())]
    pub crop_length: usize,
    #[deserr(default, error = DeserrJsonError<InvalidSearchAttributesToHighlight>)]
    pub attributes_to_highlight: Option<HashSet<String>>,
    #[deserr(default, error = DeserrJsonError<InvalidSearchShowRankingScore>, default)]
    pub show_ranking_score: bool,
    #[deserr(default, error = DeserrJsonError<InvalidSearchShowRankingScoreDetails>, default)]
    pub show_ranking_score_details: bool,
    #[deserr(default, error = DeserrJsonError<InvalidSearchShowMatchesPosition>, default)]
    pub show_matches_position: bool,
    #[deserr(default, error = DeserrJsonError<InvalidSearchFilter>)]
    pub filter: Option<Value>,
    #[deserr(default, error = DeserrJsonError<InvalidSearchSort>)]
    pub sort: Option<Vec<String>>,
    #[deserr(default, error = DeserrJsonError<InvalidSearchDistinct>)]
    pub distinct: Option<String>,
    #[deserr(default, error = DeserrJsonError<InvalidSearchFacets>)]
    pub facets: Option<Vec<String>>,
    #[deserr(default, error = DeserrJsonError<InvalidSearchHighlightPreTag>, default = DEFAULT_HIGHLIGHT_PRE_TAG())]
    pub highlight_pre_tag: String,
    #[deserr(default, error = DeserrJsonError<InvalidSearchHighlightPostTag>, default = DEFAULT_HIGHLIGHT_POST_TAG())]
    pub highlight_post_tag: String,
    #[deserr(default, error = DeserrJsonError<InvalidSearchCropMarker>, default = DEFAULT_CROP_MARKER())]
    pub crop_marker: String,
    #[deserr(default, error = DeserrJsonError<InvalidSearchMatchingStrategy>, default)]
    pub matching_strategy: MatchingStrategy,
    #[deserr(default, error = DeserrJsonError<InvalidSearchAttributesToSearchOn>, default)]
    pub attributes_to_search_on: Option<Vec<String>>,
    #[deserr(default, error = DeserrJsonError<InvalidSearchRankingScoreThreshold>, default)]
    pub ranking_score_threshold: Option<RankingScoreThreshold>,
    #[deserr(default, error = DeserrJsonError<InvalidSearchLocales>, default)]
    pub locales: Option<Vec<Locale>>,

    #[deserr(default)]
    pub federation_options: Option<FederationOptions>,
}

impl SearchQueryWithIndex {
    pub fn has_pagination(&self) -> Option<&'static str> {
        if self.offset.is_some() {
            Some("offset")
        } else if self.limit.is_some() {
            Some("limit")
        } else if self.page.is_some() {
            Some("page")
        } else if self.hits_per_page.is_some() {
            Some("hitsPerPage")
        } else {
            None
        }
    }

    pub fn has_facets(&self) -> Option<&[String]> {
        self.facets.as_deref().filter(|v| !v.is_empty())
    }

    pub fn into_index_query_federation(self) -> (IndexUid, SearchQuery, Option<FederationOptions>) {
        let SearchQueryWithIndex {
            index_uid,
            federation_options,
            q,
            vector,
            offset,
            limit,
            page,
            hits_per_page,
            attributes_to_retrieve,
            retrieve_vectors,
            attributes_to_crop,
            crop_length,
            attributes_to_highlight,
            show_ranking_score,
            show_ranking_score_details,
            show_matches_position,
            filter,
            sort,
            distinct,
            facets,
            highlight_pre_tag,
            highlight_post_tag,
            crop_marker,
            matching_strategy,
            attributes_to_search_on,
            hybrid,
            ranking_score_threshold,
            locales,
        } = self;
        (
            index_uid,
            SearchQuery {
                q,
                vector,
                offset: offset.unwrap_or(DEFAULT_SEARCH_OFFSET()),
                limit: limit.unwrap_or(DEFAULT_SEARCH_LIMIT()),
                page,
                hits_per_page,
                attributes_to_retrieve,
                retrieve_vectors,
                attributes_to_crop,
                crop_length,
                attributes_to_highlight,
                show_ranking_score,
                show_ranking_score_details,
                show_matches_position,
                filter,
                sort,
                distinct,
                facets,
                highlight_pre_tag,
                highlight_post_tag,
                crop_marker,
                matching_strategy,
                attributes_to_search_on,
                hybrid,
                ranking_score_threshold,
                locales,
                // do not use ..Default::default() here,
                // rather add any missing field from `SearchQuery` to `SearchQueryWithIndex`
            },
            federation_options,
        )
    }
}

#[derive(Debug, Clone, PartialEq, Deserr, ToSchema)]
#[deserr(error = DeserrJsonError, rename_all = camelCase, deny_unknown_fields)]
pub struct SimilarQuery {
    #[deserr(error = DeserrJsonError<InvalidSimilarId>)]
    #[schema(value_type = String)]
    pub id: ExternalDocumentId,
    #[deserr(default = DEFAULT_SEARCH_OFFSET(), error = DeserrJsonError<InvalidSimilarOffset>)]
    pub offset: usize,
    #[deserr(default = DEFAULT_SEARCH_LIMIT(), error = DeserrJsonError<InvalidSimilarLimit>)]
    pub limit: usize,
    #[deserr(default, error = DeserrJsonError<InvalidSimilarFilter>)]
    pub filter: Option<Value>,
    #[deserr(error = DeserrJsonError<InvalidSimilarEmbedder>)]
    pub embedder: String,
    #[deserr(default, error = DeserrJsonError<InvalidSimilarAttributesToRetrieve>)]
    pub attributes_to_retrieve: Option<BTreeSet<String>>,
    #[deserr(default, error = DeserrJsonError<InvalidSimilarRetrieveVectors>)]
    pub retrieve_vectors: bool,
    #[deserr(default, error = DeserrJsonError<InvalidSimilarShowRankingScore>, default)]
    pub show_ranking_score: bool,
    #[deserr(default, error = DeserrJsonError<InvalidSimilarShowRankingScoreDetails>, default)]
    pub show_ranking_score_details: bool,
    #[deserr(default, error = DeserrJsonError<InvalidSimilarRankingScoreThreshold>, default)]
    #[schema(value_type = f64)]
    pub ranking_score_threshold: Option<RankingScoreThresholdSimilar>,
}

#[derive(Debug, Clone, PartialEq, Deserr)]
#[deserr(try_from(Value) = TryFrom::try_from -> InvalidSimilarId)]
pub struct ExternalDocumentId(String);

impl AsRef<str> for ExternalDocumentId {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl ExternalDocumentId {
    pub fn into_inner(self) -> String {
        self.0
    }
}

impl TryFrom<String> for ExternalDocumentId {
    type Error = InvalidSimilarId;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        serde_json::Value::String(value).try_into()
    }
}

impl TryFrom<Value> for ExternalDocumentId {
    type Error = InvalidSimilarId;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        Ok(Self(milli::documents::validate_document_id_value(value).map_err(|_| InvalidSimilarId)?))
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Deserr, ToSchema)]
#[deserr(rename_all = camelCase)]
pub enum MatchingStrategy {
    /// Remove query words from last to first
    Last,
    /// All query words are mandatory
    All,
    /// Remove query words from the most frequent to the least
    Frequency,
}

impl Default for MatchingStrategy {
    fn default() -> Self {
        Self::Last
    }
}

impl From<MatchingStrategy> for TermsMatchingStrategy {
    fn from(other: MatchingStrategy) -> Self {
        match other {
            MatchingStrategy::Last => Self::Last,
            MatchingStrategy::All => Self::All,
            MatchingStrategy::Frequency => Self::Frequency,
        }
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Deserr)]
#[deserr(rename_all = camelCase)]
pub enum FacetValuesSort {
    /// Facet values are sorted in alphabetical order, ascending from A to Z.
    #[default]
    Alpha,
    /// Facet values are sorted by decreasing count.
    /// The count is the number of records containing this facet value in the results of the query.
    Count,
}

impl From<FacetValuesSort> for OrderBy {
    fn from(val: FacetValuesSort) -> Self {
        match val {
            FacetValuesSort::Alpha => OrderBy::Lexicographic,
            FacetValuesSort::Count => OrderBy::Count,
        }
    }
}

#[derive(Debug, Clone, Serialize, PartialEq, ToSchema)]
pub struct SearchHit {
    #[serde(flatten)]
    #[schema(additional_properties, inline, value_type = HashMap<String, Value>)]
    pub document: Document,
    #[serde(rename = "_formatted", skip_serializing_if = "Document::is_empty")]
    #[schema(additional_properties, value_type = HashMap<String, Value>)]
    pub formatted: Document,
    #[serde(rename = "_matchesPosition", skip_serializing_if = "Option::is_none")]
    pub matches_position: Option<MatchesPosition>,
    #[serde(rename = "_rankingScore", skip_serializing_if = "Option::is_none")]
    pub ranking_score: Option<f64>,
    #[serde(rename = "_rankingScoreDetails", skip_serializing_if = "Option::is_none")]
    pub ranking_score_details: Option<serde_json::Map<String, serde_json::Value>>,
}

#[derive(Serialize, Clone, PartialEq, ToSchema)]
#[serde(rename_all = "camelCase")]
#[schema(rename_all = "camelCase")]
pub struct SearchResult {
    pub hits: Vec<SearchHit>,
    pub query: String,
    pub processing_time_ms: u128,
    #[serde(flatten)]
    pub hits_info: HitsInfo,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(value_type = Option<BTreeMap<String, Value>>)]
    pub facet_distribution: Option<BTreeMap<String, IndexMap<String, u64>>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub facet_stats: Option<BTreeMap<String, FacetStats>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub semantic_hit_count: Option<u32>,

    // These fields are only used for analytics purposes
    #[serde(skip)]
    pub degraded: bool,
    #[serde(skip)]
    pub used_negative_operator: bool,
}

impl fmt::Debug for SearchResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let SearchResult {
            hits,
            query,
            processing_time_ms,
            hits_info,
            facet_distribution,
            facet_stats,
            semantic_hit_count,
            degraded,
            used_negative_operator,
        } = self;

        let mut debug = f.debug_struct("SearchResult");
        // The most important thing when looking at a search result is the time it took to process
        debug.field("processing_time_ms", &processing_time_ms);
        debug.field("hits", &format!("[{} hits returned]", hits.len()));
        debug.field("query", &query);
        debug.field("hits_info", &hits_info);
        if *used_negative_operator {
            debug.field("used_negative_operator", used_negative_operator);
        }
        if *degraded {
            debug.field("degraded", degraded);
        }
        if let Some(facet_distribution) = facet_distribution {
            debug.field("facet_distribution", &facet_distribution);
        }
        if let Some(facet_stats) = facet_stats {
            debug.field("facet_stats", &facet_stats);
        }
        if let Some(semantic_hit_count) = semantic_hit_count {
            debug.field("semantic_hit_count", &semantic_hit_count);
        }

        debug.finish()
    }
}

#[derive(Serialize, Debug, Clone, PartialEq, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct SimilarResult {
    pub hits: Vec<SearchHit>,
    pub id: String,
    pub processing_time_ms: u128,
    #[serde(flatten)]
    pub hits_info: HitsInfo,
}

#[derive(Serialize, Debug, Clone, PartialEq, ToSchema)]
#[serde(rename_all = "camelCase")]
#[schema(rename_all = "camelCase")]
pub struct SearchResultWithIndex {
    pub index_uid: String,
    #[serde(flatten)]
    pub result: SearchResult,
}

#[derive(Serialize, Debug, Clone, PartialEq, Eq, ToSchema)]
#[serde(untagged)]
pub enum HitsInfo {
    #[serde(rename_all = "camelCase")]
    #[schema(rename_all = "camelCase")]
    Pagination { hits_per_page: usize, page: usize, total_pages: usize, total_hits: usize },
    #[serde(rename_all = "camelCase")]
    #[schema(rename_all = "camelCase")]
    OffsetLimit { limit: usize, offset: usize, estimated_total_hits: usize },
}

#[derive(Serialize, Debug, Clone, PartialEq, ToSchema)]
pub struct FacetStats {
    pub min: f64,
    pub max: f64,
}

#[derive(Serialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct FacetSearchResult {
    pub facet_hits: Vec<FacetValueHit>,
    pub facet_query: Option<String>,
    pub processing_time_ms: u128,
}

/// Incorporate search rules in search query
pub fn add_search_rules(filter: &mut Option<Value>, rules: IndexSearchRules) {
    *filter = match (filter.take(), rules.filter) {
        (None, rules_filter) => rules_filter,
        (filter, None) => filter,
        (Some(filter), Some(rules_filter)) => {
            let filter = match filter {
                Value::Array(filter) => filter,
                filter => vec![filter],
            };
            let rules_filter = match rules_filter {
                Value::Array(rules_filter) => rules_filter,
                rules_filter => vec![rules_filter],
            };

            Some(Value::Array([filter, rules_filter].concat()))
        }
    }
}

fn prepare_search<'t>(
    index: &'t Index,
    rtxn: &'t RoTxn,
    query: &'t SearchQuery,
    search_kind: &SearchKind,
    time_budget: TimeBudget,
    features: RoFeatures,
) -> Result<(milli::Search<'t>, bool, usize, usize), ResponseError> {
    let mut search = index.search(rtxn);
    search.time_budget(time_budget);
    if let Some(ranking_score_threshold) = query.ranking_score_threshold {
        search.ranking_score_threshold(ranking_score_threshold.0);
    }

    if let Some(distinct) = &query.distinct {
        search.distinct(distinct.clone());
    }

    match search_kind {
        SearchKind::KeywordOnly => {
            if let Some(q) = &query.q {
                search.query(q);
            }
        }
        SearchKind::SemanticOnly { embedder_name, embedder, quantized } => {
            let vector = match query.vector.clone() {
                Some(vector) => vector,
                None => {
                    let span = tracing::trace_span!(target: "search::vector", "embed_one");
                    let _entered = span.enter();

                    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);

                    embedder
                        .embed_one(query.q.clone().unwrap(), Some(deadline))
                        .map_err(milli::vector::Error::from)
                        .map_err(milli::Error::from)?
                }
            };

            search.semantic(embedder_name.clone(), embedder.clone(), *quantized, Some(vector));
        }
        SearchKind::Hybrid { embedder_name, embedder, quantized, semantic_ratio: _ } => {
            if let Some(q) = &query.q {
                search.query(q);
            }
            // will be embedded in hybrid search if necessary
            search.semantic(
                embedder_name.clone(),
                embedder.clone(),
                *quantized,
                query.vector.clone(),
            );
        }
    }

    if let Some(ref searchable) = query.attributes_to_search_on {
        search.searchable_attributes(searchable);
    }

    let is_finite_pagination = query.is_finite_pagination();
    search.terms_matching_strategy(query.matching_strategy.into());

    let max_total_hits = index
        .pagination_max_total_hits(rtxn)
        .map_err(milli::Error::from)?
        .map(|x| x as usize)
        .unwrap_or(DEFAULT_PAGINATION_MAX_TOTAL_HITS);

    search.exhaustive_number_hits(is_finite_pagination);
    search.scoring_strategy(
        if query.show_ranking_score
            || query.show_ranking_score_details
            || query.ranking_score_threshold.is_some()
        {
            ScoringStrategy::Detailed
        } else {
            ScoringStrategy::Skip
        },
    );

    // compute the offset on the limit depending on the pagination mode.
    let (offset, limit) = if is_finite_pagination {
        let limit = query.hits_per_page.unwrap_or_else(DEFAULT_SEARCH_LIMIT);
        let page = query.page.unwrap_or(1);

        // page 0 gives a limit of 0 forcing Meilisearch to return no document.
        page.checked_sub(1).map_or((0, 0), |p| (limit * p, limit))
    } else {
        (query.offset, query.limit)
    };

    // Make sure that a user can't get more documents than the hard limit,
    // we align that on the offset too.
    let offset = min(offset, max_total_hits);
    let limit = min(limit, max_total_hits.saturating_sub(offset));

    search.offset(offset);
    search.limit(limit);

    if let Some(ref filter) = query.filter {
        if let Some(facets) = parse_filter(filter, Code::InvalidSearchFilter, features)? {
            search.filter(facets);
        }
    }

    if let Some(ref sort) = query.sort {
        let sort = match sort.iter().map(|s| AscDesc::from_str(s)).collect() {
            Ok(sorts) => sorts,
            Err(asc_desc_error) => {
                return Err(milli::Error::from(SortError::from(asc_desc_error)).into())
            }
        };

        search.sort_criteria(sort);
    }

    if let Some(ref locales) = query.locales {
        search.locales(locales.iter().copied().map(Into::into).collect());
    }

    Ok((search, is_finite_pagination, max_total_hits, offset))
}

pub fn perform_search(
    index_uid: String,
    index: &Index,
    query: SearchQuery,
    search_kind: SearchKind,
    retrieve_vectors: RetrieveVectors,
    features: RoFeatures,
) -> Result<SearchResult, ResponseError> {
    let before_search = Instant::now();
    let rtxn = index.read_txn()?;
    let time_budget = match index.search_cutoff(&rtxn)? {
        Some(cutoff) => TimeBudget::new(Duration::from_millis(cutoff)),
        None => TimeBudget::default(),
    };

    let (search, is_finite_pagination, max_total_hits, offset) =
        prepare_search(index, &rtxn, &query, &search_kind, time_budget, features)?;

    let (
        milli::SearchResult {
            documents_ids,
            matching_words,
            candidates,
            document_scores,
            degraded,
            used_negative_operator,
        },
        semantic_hit_count,
    ) = search_from_kind(index_uid, search_kind, search)?;

    let SearchQuery {
        q,
        limit,
        page,
        hits_per_page,
        attributes_to_retrieve,
        // use the enum passed as parameter
        retrieve_vectors: _,
        attributes_to_crop,
        crop_length,
        attributes_to_highlight,
        show_matches_position,
        show_ranking_score,
        show_ranking_score_details,
        sort,
        facets,
        highlight_pre_tag,
        highlight_post_tag,
        crop_marker,
        locales,
        // already used in prepare_search
        vector: _,
        hybrid: _,
        offset: _,
        ranking_score_threshold: _,
        matching_strategy: _,
        attributes_to_search_on: _,
        filter: _,
        distinct: _,
    } = query;

    let format = AttributesFormat {
        attributes_to_retrieve,
        retrieve_vectors,
        attributes_to_highlight,
        attributes_to_crop,
        crop_length,
        crop_marker,
        highlight_pre_tag,
        highlight_post_tag,
        show_matches_position,
        sort,
        show_ranking_score,
        show_ranking_score_details,
        locales: locales.map(|l| l.iter().copied().map(Into::into).collect()),
    };

    let documents = make_hits(
        index,
        &rtxn,
        format,
        matching_words,
        documents_ids.iter().copied().zip(document_scores.iter()),
    )?;

    let number_of_hits = min(candidates.len() as usize, max_total_hits);
    let hits_info = if is_finite_pagination {
        let hits_per_page = hits_per_page.unwrap_or_else(DEFAULT_SEARCH_LIMIT);
        // If hit_per_page is 0, then pages can't be computed and so we respond 0.
        let total_pages = (number_of_hits + hits_per_page.saturating_sub(1))
            .checked_div(hits_per_page)
            .unwrap_or(0);

        HitsInfo::Pagination {
            hits_per_page,
            page: page.unwrap_or(1),
            total_pages,
            total_hits: number_of_hits,
        }
    } else {
        HitsInfo::OffsetLimit { limit, offset, estimated_total_hits: number_of_hits }
    };

    let (facet_distribution, facet_stats) = facets
        .map(move |facets| {
            compute_facet_distribution_stats(&facets, index, &rtxn, candidates, Route::Search)
        })
        .transpose()?
        .map(|ComputedFacets { distribution, stats }| (distribution, stats))
        .unzip();

    let result = SearchResult {
        hits: documents,
        hits_info,
        query: q.unwrap_or_default(),
        processing_time_ms: before_search.elapsed().as_millis(),
        facet_distribution,
        facet_stats,
        degraded,
        used_negative_operator,
        semantic_hit_count,
    };
    Ok(result)
}

#[derive(Debug, Clone, Default, Serialize, ToSchema)]
pub struct ComputedFacets {
    #[schema(value_type = BTreeMap<String, BTreeMap<String, u64>>)]
    pub distribution: BTreeMap<String, IndexMap<String, u64>>,
    pub stats: BTreeMap<String, FacetStats>,
}

pub enum Route {
    Search,
    MultiSearch,
    Similar,
}

fn compute_facet_distribution_stats<S: AsRef<str>>(
    facets: &[S],
    index: &Index,
    rtxn: &RoTxn,
    candidates: roaring::RoaringBitmap,
    route: Route,
) -> Result<ComputedFacets, ResponseError> {
    let mut facet_distribution = index.facets_distribution(rtxn);

    let max_values_by_facet = index
        .max_values_per_facet(rtxn)
        .map_err(milli::Error::from)?
        .map(|x| x as usize)
        .unwrap_or(DEFAULT_VALUES_PER_FACET);

    facet_distribution.max_values_per_facet(max_values_by_facet);

    let sort_facet_values_by = index.sort_facet_values_by(rtxn).map_err(milli::Error::from)?;

    // add specific facet if there is no placeholder
    if facets.iter().all(|f| f.as_ref() != "*") {
        let fields: Vec<_> =
            facets.iter().map(|n| (n, sort_facet_values_by.get(n.as_ref()))).collect();
        facet_distribution.facets(fields);
    }

    let distribution = facet_distribution
        .candidates(candidates)
        .default_order_by(sort_facet_values_by.get("*"))
        .execute()
        .map_err(|error| match (error, route) {
            (
                error @ milli::Error::UserError(milli::UserError::InvalidFacetsDistribution {
                    ..
                }),
                Route::MultiSearch,
            ) => ResponseError::from_msg(error.to_string(), Code::InvalidMultiSearchFacets),
            (error, _) => error.into(),
        })?;
    let stats = facet_distribution.compute_stats()?;
    let stats = stats.into_iter().map(|(k, (min, max))| (k, FacetStats { min, max })).collect();
    Ok(ComputedFacets { distribution, stats })
}

pub fn search_from_kind(
    index_uid: String,
    search_kind: SearchKind,
    search: milli::Search<'_>,
) -> Result<(milli::SearchResult, Option<u32>), MeilisearchHttpError> {
    let (milli_result, semantic_hit_count) = match &search_kind {
        SearchKind::KeywordOnly => {
            let results = search
                .execute()
                .map_err(|e| MeilisearchHttpError::from_milli(e, Some(index_uid.to_string())))?;
            (results, None)
        }
        SearchKind::SemanticOnly { .. } => {
            let results = search
                .execute()
                .map_err(|e| MeilisearchHttpError::from_milli(e, Some(index_uid.to_string())))?;
            let semantic_hit_count = results.document_scores.len() as u32;
            (results, Some(semantic_hit_count))
        }
        SearchKind::Hybrid { semantic_ratio, .. } => search
            .execute_hybrid(*semantic_ratio)
            .map_err(|e| MeilisearchHttpError::from_milli(e, Some(index_uid)))?,
    };
    Ok((milli_result, semantic_hit_count))
}

struct AttributesFormat {
    attributes_to_retrieve: Option<BTreeSet<String>>,
    retrieve_vectors: RetrieveVectors,
    attributes_to_highlight: Option<HashSet<String>>,
    attributes_to_crop: Option<Vec<String>>,
    crop_length: usize,
    crop_marker: String,
    highlight_pre_tag: String,
    highlight_post_tag: String,
    show_matches_position: bool,
    sort: Option<Vec<String>>,
    show_ranking_score: bool,
    show_ranking_score_details: bool,
    locales: Option<Vec<Language>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetrieveVectors {
    /// Remove the `_vectors` field
    ///
    /// this is the behavior when the vectorStore feature is enabled, and `retrieveVectors` is `false`
    Hide,
    /// Retrieve vectors from the DB and merge them into the `_vectors` field
    ///
    /// this is the behavior when the vectorStore feature is enabled, and `retrieveVectors` is `true`
    Retrieve,
}

impl RetrieveVectors {
    pub fn new(retrieve_vector: bool) -> Self {
        if retrieve_vector {
            Self::Retrieve
        } else {
            Self::Hide
        }
    }
}

struct HitMaker<'a> {
    index: &'a Index,
    rtxn: &'a RoTxn<'a>,
    fields_ids_map: FieldsIdsMap,
    displayed_ids: BTreeSet<FieldId>,
    vectors_fid: Option<FieldId>,
    retrieve_vectors: RetrieveVectors,
    to_retrieve_ids: BTreeSet<FieldId>,
    embedding_configs: Vec<milli::index::IndexEmbeddingConfig>,
    formatter_builder: MatcherBuilder<'a>,
    formatted_options: BTreeMap<FieldId, FormatOptions>,
    show_ranking_score: bool,
    show_ranking_score_details: bool,
    sort: Option<Vec<String>>,
    show_matches_position: bool,
    locales: Option<Vec<Language>>,
}

impl<'a> HitMaker<'a> {
    pub fn tokenizer<'b>(
        dictionary: Option<&'b [&'b str]>,
        separators: Option<&'b [&'b str]>,
    ) -> milli::tokenizer::Tokenizer<'b> {
        let mut tokenizer_builder = TokenizerBuilder::default();
        tokenizer_builder.create_char_map(true);

        if let Some(separators) = separators {
            tokenizer_builder.separators(separators);
        }

        if let Some(dictionary) = dictionary {
            tokenizer_builder.words_dict(dictionary);
        }

        tokenizer_builder.into_tokenizer()
    }

    pub fn formatter_builder(
        matching_words: milli::MatchingWords,
        tokenizer: milli::tokenizer::Tokenizer<'_>,
    ) -> MatcherBuilder<'_> {
        let formatter_builder = MatcherBuilder::new(matching_words, tokenizer);

        formatter_builder
    }

    pub fn new(
        index: &'a Index,
        rtxn: &'a RoTxn<'a>,
        format: AttributesFormat,
        mut formatter_builder: MatcherBuilder<'a>,
    ) -> milli::Result<Self> {
        formatter_builder.crop_marker(format.crop_marker);
        formatter_builder.highlight_prefix(format.highlight_pre_tag);
        formatter_builder.highlight_suffix(format.highlight_post_tag);

        let fields_ids_map = index.fields_ids_map(rtxn)?;
        let displayed_ids = index
            .displayed_fields_ids(rtxn)?
            .map(|fields| fields.into_iter().collect::<BTreeSet<_>>());

        let vectors_fid = fields_ids_map.id(milli::constants::RESERVED_VECTORS_FIELD_NAME);

        let vectors_is_hidden = match (&displayed_ids, vectors_fid) {
            // displayed_ids is a wildcard, so `_vectors` can be displayed regardless of its fid
            (None, _) => false,
            // vectors has no fid, so check its explicit name
            (Some(_), None) => {
                // unwrap as otherwise we'd go to the first one
                let displayed_names = index.displayed_fields(rtxn)?.unwrap();
                !displayed_names.contains(&milli::constants::RESERVED_VECTORS_FIELD_NAME)
            }
            // displayed_ids is a finit list, so hide if `_vectors` is not part of it
            (Some(map), Some(vectors_fid)) => map.contains(&vectors_fid),
        };

        let displayed_ids =
            displayed_ids.unwrap_or_else(|| fields_ids_map.iter().map(|(id, _)| id).collect());

        let retrieve_vectors = if let RetrieveVectors::Retrieve = format.retrieve_vectors {
            if vectors_is_hidden {
                RetrieveVectors::Hide
            } else {
                RetrieveVectors::Retrieve
            }
        } else {
            format.retrieve_vectors
        };

        let fids = |attrs: &BTreeSet<String>| {
            let mut ids = BTreeSet::new();
            for attr in attrs {
                if attr == "*" {
                    ids.clone_from(&displayed_ids);
                    break;
                }

                if let Some(id) = fields_ids_map.id(attr) {
                    ids.insert(id);
                }
            }
            ids
        };
        let to_retrieve_ids: BTreeSet<_> = format
            .attributes_to_retrieve
            .as_ref()
            .map(fids)
            .unwrap_or_else(|| displayed_ids.clone())
            .intersection(&displayed_ids)
            .cloned()
            .collect();

        let attr_to_highlight = format.attributes_to_highlight.unwrap_or_default();
        let attr_to_crop = format.attributes_to_crop.unwrap_or_default();
        let formatted_options = compute_formatted_options(
            &attr_to_highlight,
            &attr_to_crop,
            format.crop_length,
            &to_retrieve_ids,
            &fields_ids_map,
            &displayed_ids,
        );

        let embedding_configs = index.embedding_configs(rtxn)?;

        Ok(Self {
            index,
            rtxn,
            fields_ids_map,
            displayed_ids,
            vectors_fid,
            retrieve_vectors,
            to_retrieve_ids,
            embedding_configs,
            formatter_builder,
            formatted_options,
            show_ranking_score: format.show_ranking_score,
            show_ranking_score_details: format.show_ranking_score_details,
            show_matches_position: format.show_matches_position,
            sort: format.sort,
            locales: format.locales,
        })
    }

    pub fn make_hit(&self, id: u32, score: &[ScoreDetails]) -> milli::Result<SearchHit> {
        let (_, obkv) =
            self.index.iter_documents(self.rtxn, std::iter::once(id))?.next().unwrap()?;

        // First generate a document with all the displayed fields
        let displayed_document = make_document(&self.displayed_ids, &self.fields_ids_map, obkv)?;

        let add_vectors_fid =
            self.vectors_fid.filter(|_fid| self.retrieve_vectors == RetrieveVectors::Retrieve);

        // select the attributes to retrieve
        let attributes_to_retrieve = self
            .to_retrieve_ids
            .iter()
            // skip the vectors_fid if RetrieveVectors::Hide
            .filter(|fid| match self.vectors_fid {
                Some(vectors_fid) => {
                    !(self.retrieve_vectors == RetrieveVectors::Hide && **fid == vectors_fid)
                }
                None => true,
            })
            // need to retrieve the existing `_vectors` field if the `RetrieveVectors::Retrieve`
            .chain(add_vectors_fid.iter())
            .map(|&fid| self.fields_ids_map.name(fid).expect("Missing field name"));

        let mut document =
            permissive_json_pointer::select_values(&displayed_document, attributes_to_retrieve);

        if self.retrieve_vectors == RetrieveVectors::Retrieve {
            // Clippy is wrong
            #[allow(clippy::manual_unwrap_or_default)]
            let mut vectors = match document.remove("_vectors") {
                Some(Value::Object(map)) => map,
                _ => Default::default(),
            };
            for (name, vector) in self.index.embeddings(self.rtxn, id)? {
                let user_provided = self
                    .embedding_configs
                    .iter()
                    .find(|conf| conf.name == name)
                    .is_some_and(|conf| conf.user_provided.contains(id));
                let embeddings =
                    ExplicitVectors { embeddings: Some(vector.into()), regenerate: !user_provided };
                vectors.insert(
                    name,
                    serde_json::to_value(embeddings).map_err(InternalError::SerdeJson)?,
                );
            }
            document.insert("_vectors".into(), vectors.into());
        }

        let localized_attributes =
            self.index.localized_attributes_rules(self.rtxn)?.unwrap_or_default();

        let (matches_position, formatted) = format_fields(
            &displayed_document,
            &self.fields_ids_map,
            &self.formatter_builder,
            &self.formatted_options,
            self.show_matches_position,
            &self.displayed_ids,
            self.locales.as_deref(),
            &localized_attributes,
        )?;

        if let Some(sort) = self.sort.as_ref() {
            insert_geo_distance(sort, &mut document);
        }

        let ranking_score =
            self.show_ranking_score.then(|| ScoreDetails::global_score(score.iter()));
        let ranking_score_details =
            self.show_ranking_score_details.then(|| ScoreDetails::to_json_map(score.iter()));

        let hit = SearchHit {
            document,
            formatted,
            matches_position,
            ranking_score_details,
            ranking_score,
        };

        Ok(hit)
    }
}

fn make_hits<'a>(
    index: &Index,
    rtxn: &RoTxn<'_>,
    format: AttributesFormat,
    matching_words: milli::MatchingWords,
    documents_ids_scores: impl Iterator<Item = (u32, &'a Vec<ScoreDetails>)> + 'a,
) -> milli::Result<Vec<SearchHit>> {
    let mut documents = Vec::new();

    let dictionary = index.dictionary(rtxn)?;
    let dictionary: Option<Vec<_>> =
        dictionary.as_ref().map(|x| x.iter().map(String::as_str).collect());
    let separators = index.allowed_separators(rtxn)?;
    let separators: Option<Vec<_>> =
        separators.as_ref().map(|x| x.iter().map(String::as_str).collect());

    let tokenizer = HitMaker::tokenizer(dictionary.as_deref(), separators.as_deref());

    let formatter_builder = HitMaker::formatter_builder(matching_words, tokenizer);

    let hit_maker = HitMaker::new(index, rtxn, format, formatter_builder)?;

    for (id, score) in documents_ids_scores {
        documents.push(hit_maker.make_hit(id, score)?);
    }
    Ok(documents)
}

pub fn perform_facet_search(
    index: &Index,
    search_query: SearchQuery,
    facet_query: Option<String>,
    facet_name: String,
    search_kind: SearchKind,
    features: RoFeatures,
    locales: Option<Vec<Language>>,
) -> Result<FacetSearchResult, ResponseError> {
    let before_search = Instant::now();
    let rtxn = index.read_txn()?;
    let time_budget = match index.search_cutoff(&rtxn)? {
        Some(cutoff) => TimeBudget::new(Duration::from_millis(cutoff)),
        None => TimeBudget::default(),
    };

    if !index.facet_search(&rtxn)? {
        return Err(ResponseError::from_msg(
            "The facet search is disabled for this index".to_string(),
            Code::FacetSearchDisabled,
        ));
    }

    // In the faceted search context, we want to use the intersection between the locales provided by the user
    // and the locales of the facet string.
    // If the facet string is not localized, we **ignore** the locales provided by the user because the facet data has no locale.
    // If the user does not provide locales, we use the locales of the facet string.
    let localized_attributes = index.localized_attributes_rules(&rtxn)?.unwrap_or_default();
    let localized_attributes_locales =
        localized_attributes.into_iter().find(|attr| attr.match_str(&facet_name));
    let locales = localized_attributes_locales.map(|attr| {
        attr.locales
            .into_iter()
            .filter(|locale| locales.as_ref().map_or(true, |locales| locales.contains(locale)))
            .collect()
    });

    let (search, _, _, _) =
        prepare_search(index, &rtxn, &search_query, &search_kind, time_budget, features)?;
    let mut facet_search = SearchForFacetValues::new(
        facet_name,
        search,
        matches!(search_kind, SearchKind::Hybrid { .. }),
    );
    if let Some(facet_query) = &facet_query {
        facet_search.query(facet_query);
    }
    if let Some(max_facets) = index.max_values_per_facet(&rtxn)? {
        facet_search.max_values(max_facets as usize);
    }

    if let Some(locales) = locales {
        facet_search.locales(locales);
    }

    Ok(FacetSearchResult {
        facet_hits: facet_search.execute()?,
        facet_query,
        processing_time_ms: before_search.elapsed().as_millis(),
    })
}

pub fn perform_similar(
    index: &Index,
    query: SimilarQuery,
    embedder_name: String,
    embedder: Arc<Embedder>,
    quantized: bool,
    retrieve_vectors: RetrieveVectors,
    features: RoFeatures,
) -> Result<SimilarResult, ResponseError> {
    let before_search = Instant::now();
    let rtxn = index.read_txn()?;

    let SimilarQuery {
        id,
        offset,
        limit,
        filter: _,
        embedder: _,
        attributes_to_retrieve,
        retrieve_vectors: _,
        show_ranking_score,
        show_ranking_score_details,
        ranking_score_threshold,
    } = query;

    // using let-else rather than `?` so that the borrow checker identifies we're always returning here,
    // preventing a use-after-move
    let Some(internal_id) = index.external_documents_ids().get(&rtxn, &id)? else {
        return Err(ResponseError::from_msg(
            MeilisearchHttpError::DocumentNotFound(id.into_inner()).to_string(),
            Code::NotFoundSimilarId,
        ));
    };

    let mut similar = milli::Similar::new(
        internal_id,
        offset,
        limit,
        index,
        &rtxn,
        embedder_name,
        embedder,
        quantized,
    );

    if let Some(ref filter) = query.filter {
        if let Some(facets) = parse_filter(filter, Code::InvalidSimilarFilter, features)? {
            similar.filter(facets);
        }
    }

    if let Some(ranking_score_threshold) = ranking_score_threshold {
        similar.ranking_score_threshold(ranking_score_threshold.0);
    }

    let milli::SearchResult {
        documents_ids,
        matching_words: _,
        candidates,
        document_scores,
        degraded: _,
        used_negative_operator: _,
    } = similar.execute().map_err(|err| match err {
        milli::Error::UserError(milli::UserError::InvalidFilter(_)) => {
            ResponseError::from_msg(err.to_string(), Code::InvalidSimilarFilter)
        }
        err => err.into(),
    })?;

    let format = AttributesFormat {
        attributes_to_retrieve,
        retrieve_vectors,
        attributes_to_highlight: None,
        attributes_to_crop: None,
        crop_length: DEFAULT_CROP_LENGTH(),
        crop_marker: DEFAULT_CROP_MARKER(),
        highlight_pre_tag: DEFAULT_HIGHLIGHT_PRE_TAG(),
        highlight_post_tag: DEFAULT_HIGHLIGHT_POST_TAG(),
        show_matches_position: false,
        sort: None,
        show_ranking_score,
        show_ranking_score_details,
        locales: None,
    };

    let hits = make_hits(
        index,
        &rtxn,
        format,
        Default::default(),
        documents_ids.iter().copied().zip(document_scores.iter()),
    )?;

    let max_total_hits = index
        .pagination_max_total_hits(&rtxn)
        .map_err(milli::Error::from)?
        .map(|x| x as usize)
        .unwrap_or(DEFAULT_PAGINATION_MAX_TOTAL_HITS);

    let number_of_hits = min(candidates.len() as usize, max_total_hits);
    let hits_info = HitsInfo::OffsetLimit { limit, offset, estimated_total_hits: number_of_hits };

    let result = SimilarResult {
        hits,
        hits_info,
        id: id.into_inner(),
        processing_time_ms: before_search.elapsed().as_millis(),
    };
    Ok(result)
}

pub fn insert_geo_distance(sorts: &[String], document: &mut Document) {
    lazy_static::lazy_static! {
        static ref GEO_REGEX: Regex =
            Regex::new(r"_geoPoint\(\s*([[:digit:].\-]+)\s*,\s*([[:digit:].\-]+)\s*\)").unwrap();
    };
    if let Some(capture_group) = sorts.iter().find_map(|sort| GEO_REGEX.captures(sort)) {
        // TODO: TAMO: milli encountered an internal error, what do we want to do?
        let base = [capture_group[1].parse().unwrap(), capture_group[2].parse().unwrap()];
        let geo_point = &document.get("_geo").unwrap_or(&json!(null));
        if let Some((lat, lng)) =
            extract_geo_value(&geo_point["lat"]).zip(extract_geo_value(&geo_point["lng"]))
        {
            let distance = milli::distance_between_two_points(&base, &[lat, lng]);
            document.insert("_geoDistance".to_string(), json!(distance.round() as usize));
        }
    }
}

fn extract_geo_value(value: &Value) -> Option<f64> {
    match value {
        Value::Number(n) => n.as_f64(),
        Value::String(s) => s.parse().ok(),
        _ => None,
    }
}

fn compute_formatted_options(
    attr_to_highlight: &HashSet<String>,
    attr_to_crop: &[String],
    query_crop_length: usize,
    to_retrieve_ids: &BTreeSet<FieldId>,
    fields_ids_map: &FieldsIdsMap,
    displayed_ids: &BTreeSet<FieldId>,
) -> BTreeMap<FieldId, FormatOptions> {
    let mut formatted_options = BTreeMap::new();

    add_highlight_to_formatted_options(
        &mut formatted_options,
        attr_to_highlight,
        fields_ids_map,
        displayed_ids,
    );

    add_crop_to_formatted_options(
        &mut formatted_options,
        attr_to_crop,
        query_crop_length,
        fields_ids_map,
        displayed_ids,
    );

    // Should not return `_formatted` if no valid attributes to highlight/crop
    if !formatted_options.is_empty() {
        add_non_formatted_ids_to_formatted_options(&mut formatted_options, to_retrieve_ids);
    }

    formatted_options
}

fn add_highlight_to_formatted_options(
    formatted_options: &mut BTreeMap<FieldId, FormatOptions>,
    attr_to_highlight: &HashSet<String>,
    fields_ids_map: &FieldsIdsMap,
    displayed_ids: &BTreeSet<FieldId>,
) {
    for attr in attr_to_highlight {
        let new_format = FormatOptions { highlight: true, crop: None };

        if attr == "*" {
            for id in displayed_ids {
                formatted_options.insert(*id, new_format);
            }
            break;
        }

        if let Some(id) = fields_ids_map.id(attr) {
            if displayed_ids.contains(&id) {
                formatted_options.insert(id, new_format);
            }
        }
    }
}

fn add_crop_to_formatted_options(
    formatted_options: &mut BTreeMap<FieldId, FormatOptions>,
    attr_to_crop: &[String],
    crop_length: usize,
    fields_ids_map: &FieldsIdsMap,
    displayed_ids: &BTreeSet<FieldId>,
) {
    for attr in attr_to_crop {
        let mut split = attr.rsplitn(2, ':');
        let (attr_name, attr_len) = match split.next().zip(split.next()) {
            Some((len, name)) => {
                let crop_len = len.parse::<usize>().unwrap_or(crop_length);
                (name, crop_len)
            }
            None => (attr.as_str(), crop_length),
        };

        if attr_name == "*" {
            for id in displayed_ids {
                formatted_options
                    .entry(*id)
                    .and_modify(|f| f.crop = Some(attr_len))
                    .or_insert(FormatOptions { highlight: false, crop: Some(attr_len) });
            }
        }

        if let Some(id) = fields_ids_map.id(attr_name) {
            if displayed_ids.contains(&id) {
                formatted_options
                    .entry(id)
                    .and_modify(|f| f.crop = Some(attr_len))
                    .or_insert(FormatOptions { highlight: false, crop: Some(attr_len) });
            }
        }
    }
}

fn add_non_formatted_ids_to_formatted_options(
    formatted_options: &mut BTreeMap<FieldId, FormatOptions>,
    to_retrieve_ids: &BTreeSet<FieldId>,
) {
    for id in to_retrieve_ids {
        formatted_options.entry(*id).or_insert(FormatOptions { highlight: false, crop: None });
    }
}

fn make_document(
    displayed_attributes: &BTreeSet<FieldId>,
    field_ids_map: &FieldsIdsMap,
    obkv: &obkv::KvReaderU16,
) -> milli::Result<Document> {
    let mut document = serde_json::Map::new();

    // recreate the original json
    for (key, value) in obkv.iter() {
        let value = serde_json::from_slice(value).map_err(InternalError::SerdeJson)?;
        let key = field_ids_map.name(key).expect("Missing field name").to_string();

        document.insert(key, value);
    }

    // select the attributes to retrieve
    let displayed_attributes = displayed_attributes
        .iter()
        .map(|&fid| field_ids_map.name(fid).expect("Missing field name"));

    let document = permissive_json_pointer::select_values(&document, displayed_attributes);
    Ok(document)
}

#[allow(clippy::too_many_arguments)]
fn format_fields(
    document: &Document,
    field_ids_map: &FieldsIdsMap,
    builder: &MatcherBuilder<'_>,
    formatted_options: &BTreeMap<FieldId, FormatOptions>,
    compute_matches: bool,
    displayable_ids: &BTreeSet<FieldId>,
    locales: Option<&[Language]>,
    localized_attributes: &[LocalizedAttributesRule],
) -> milli::Result<(Option<MatchesPosition>, Document)> {
    let mut matches_position = compute_matches.then(BTreeMap::new);
    let mut document = document.clone();

    // reduce the formatted option list to the attributes that should be formatted,
    // instead of all the attributes to display.
    let formatting_fields_options: Vec<_> = formatted_options
        .iter()
        .filter(|(_, option)| option.should_format())
        .map(|(fid, option)| (field_ids_map.name(*fid).unwrap(), option))
        .collect();

    // select the attributes to retrieve
    let displayable_names =
        displayable_ids.iter().map(|&fid| field_ids_map.name(fid).expect("Missing field name"));
    permissive_json_pointer::map_leaf_values(
        &mut document,
        displayable_names,
        |key, array_indices, value| {
            // To get the formatting option of each key we need to see all the rules that applies
            // to the value and merge them together. eg. If a user said he wanted to highlight `doggo`
            // and crop `doggo.name`. `doggo.name` needs to be highlighted + cropped while `doggo.age` is only
            // highlighted.
            // Warn: The time to compute the format list scales with the number of fields to format;
            // cumulated with map_leaf_values that iterates over all the nested fields, it gives a quadratic complexity:
            // d*f where d is the total number of fields to display and f is the total number of fields to format.
            let format = formatting_fields_options
                .iter()
                .filter(|(name, _option)| {
                    milli::is_faceted_by(name, key) || milli::is_faceted_by(key, name)
                })
                .map(|(_, option)| **option)
                .reduce(|acc, option| acc.merge(option));
            let mut infos = Vec::new();

            // if no locales has been provided, we try to find the locales in the localized_attributes.
            let locales = locales.or_else(|| {
                localized_attributes
                    .iter()
                    .find(|rule| rule.match_str(key))
                    .map(LocalizedAttributesRule::locales)
            });

            *value = format_value(
                std::mem::take(value),
                builder,
                format,
                &mut infos,
                compute_matches,
                array_indices,
                locales,
            );

            if let Some(matches) = matches_position.as_mut() {
                if !infos.is_empty() {
                    matches.insert(key.to_owned(), infos);
                }
            }
        },
    );

    let selectors = formatted_options
        .keys()
        // This unwrap must be safe since we got the ids from the fields_ids_map just
        // before.
        .map(|&fid| field_ids_map.name(fid).unwrap());
    let document = permissive_json_pointer::select_values(&document, selectors);

    Ok((matches_position, document))
}

fn format_value(
    value: Value,
    builder: &MatcherBuilder<'_>,
    format_options: Option<FormatOptions>,
    infos: &mut Vec<MatchBounds>,
    compute_matches: bool,
    array_indices: &[usize],
    locales: Option<&[Language]>,
) -> Value {
    match value {
        Value::String(old_string) => {
            let mut matcher = builder.build(&old_string, locales);
            if compute_matches {
                let matches = matcher.matches(array_indices);
                infos.extend_from_slice(&matches[..]);
            }

            match format_options {
                Some(format_options) => {
                    let value = matcher.format(format_options);
                    Value::String(value.into_owned())
                }
                None => Value::String(old_string),
            }
        }
        // `map_leaf_values` makes sure this is only called for leaf fields
        Value::Array(_) => unreachable!(),
        Value::Object(_) => unreachable!(),
        Value::Number(number) => {
            let s = number.to_string();

            let mut matcher = builder.build(&s, locales);
            if compute_matches {
                let matches = matcher.matches(array_indices);
                infos.extend_from_slice(&matches[..]);
            }

            match format_options {
                Some(format_options) => {
                    let value = matcher.format(format_options);
                    Value::String(value.into_owned())
                }
                None => Value::String(s),
            }
        }
        value => value,
    }
}

pub(crate) fn parse_filter(
    facets: &Value,
    filter_parsing_error_code: Code,
    features: RoFeatures,
) -> Result<Option<Filter>, ResponseError> {
    let filter = match facets {
        Value::String(expr) => Filter::from_str(expr).map_err(|e| e.into()),
        Value::Array(arr) => parse_filter_array(arr).map_err(|e| e.into()),
        v => Err(MeilisearchHttpError::InvalidExpression(&["String", "Array"], v.clone()).into()),
    };
    let filter = filter.map_err(|err: ResponseError| {
        ResponseError::from_msg(err.to_string(), filter_parsing_error_code)
    })?;

    if let Some(ref filter) = filter {
        // If the contains operator is used while the contains filter features is not enabled, errors out
        if let Some((token, error)) =
            filter.use_contains_operator().zip(features.check_contains_filter().err())
        {
            return Err(ResponseError::from_msg(
                token.as_external_error(error).to_string(),
                Code::FeatureNotEnabled,
            ));
        }
    }

    Ok(filter)
}

fn parse_filter_array(arr: &[Value]) -> Result<Option<Filter>, MeilisearchHttpError> {
    let mut ands = Vec::new();
    for value in arr {
        match value {
            Value::String(s) => ands.push(Either::Right(s.as_str())),
            Value::Array(arr) => {
                let mut ors = Vec::new();
                for value in arr {
                    match value {
                        Value::String(s) => ors.push(s.as_str()),
                        v => {
                            return Err(MeilisearchHttpError::InvalidExpression(
                                &["String"],
                                v.clone(),
                            ))
                        }
                    }
                }
                ands.push(Either::Left(ors));
            }
            v => {
                return Err(MeilisearchHttpError::InvalidExpression(
                    &["String", "[String]"],
                    v.clone(),
                ))
            }
        }
    }

    Filter::from_array(ands).map_err(|e| MeilisearchHttpError::from_milli(e, None))
}
