use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::fmt;
use std::iter::Zip;
use std::rc::Rc;
use std::str::FromStr as _;
use std::time::Duration;
use std::vec::{IntoIter, Vec};

use actix_http::StatusCode;
use index_scheduler::{IndexScheduler, RoFeatures};
use indexmap::IndexMap;
use meilisearch_types::deserr::DeserrJsonError;
use meilisearch_types::error::deserr_codes::{
    InvalidMultiSearchFacetsByIndex, InvalidMultiSearchMaxValuesPerFacet,
    InvalidMultiSearchMergeFacets, InvalidMultiSearchWeight, InvalidSearchLimit,
    InvalidSearchOffset,
};
use meilisearch_types::error::ResponseError;
use meilisearch_types::index_uid::IndexUid;
use meilisearch_types::milli::score_details::{ScoreDetails, ScoreValue};
use meilisearch_types::milli::{self, DocumentId, OrderBy, TimeBudget};
use roaring::RoaringBitmap;
use serde::Serialize;
use utoipa::ToSchema;

use super::ranking_rules::{self, RankingRules};
use super::{
    compute_facet_distribution_stats, prepare_search, AttributesFormat, ComputedFacets, FacetStats,
    HitMaker, HitsInfo, RetrieveVectors, SearchHit, SearchKind, SearchQuery, SearchQueryWithIndex,
};
use crate::error::MeilisearchHttpError;
use crate::routes::indexes::search::search_kind;

pub const DEFAULT_FEDERATED_WEIGHT: f64 = 1.0;

#[derive(Debug, Default, Clone, Copy, PartialEq, deserr::Deserr, ToSchema)]
#[deserr(error = DeserrJsonError, rename_all = camelCase, deny_unknown_fields)]
pub struct FederationOptions {
    #[deserr(default, error = DeserrJsonError<InvalidMultiSearchWeight>)]
    #[schema(value_type = f64)]
    pub weight: Weight,
}

#[derive(Debug, Clone, Copy, PartialEq, deserr::Deserr)]
#[deserr(try_from(f64) = TryFrom::try_from -> InvalidMultiSearchWeight)]
pub struct Weight(f64);

impl Default for Weight {
    fn default() -> Self {
        Weight(DEFAULT_FEDERATED_WEIGHT)
    }
}

impl std::convert::TryFrom<f64> for Weight {
    type Error = InvalidMultiSearchWeight;

    fn try_from(f: f64) -> Result<Self, Self::Error> {
        if f < 0.0 {
            Err(InvalidMultiSearchWeight)
        } else {
            Ok(Weight(f))
        }
    }
}

impl std::ops::Deref for Weight {
    type Target = f64;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Debug, deserr::Deserr, ToSchema)]
#[deserr(error = DeserrJsonError, rename_all = camelCase, deny_unknown_fields)]
#[schema(rename_all = "camelCase")]
pub struct Federation {
    #[deserr(default = super::DEFAULT_SEARCH_LIMIT(), error = DeserrJsonError<InvalidSearchLimit>)]
    pub limit: usize,
    #[deserr(default = super::DEFAULT_SEARCH_OFFSET(), error = DeserrJsonError<InvalidSearchOffset>)]
    pub offset: usize,
    #[deserr(default, error = DeserrJsonError<InvalidMultiSearchFacetsByIndex>)]
    pub facets_by_index: BTreeMap<IndexUid, Option<Vec<String>>>,
    #[deserr(default, error = DeserrJsonError<InvalidMultiSearchMergeFacets>)]
    pub merge_facets: Option<MergeFacets>,
}

#[derive(Copy, Clone, Debug, deserr::Deserr, Default, ToSchema)]
#[deserr(error = DeserrJsonError<InvalidMultiSearchMergeFacets>, rename_all = camelCase, deny_unknown_fields)]
#[schema(rename_all = "camelCase")]
pub struct MergeFacets {
    #[deserr(default, error = DeserrJsonError<InvalidMultiSearchMaxValuesPerFacet>)]
    pub max_values_per_facet: Option<usize>,
}

#[derive(Debug, deserr::Deserr, ToSchema)]
#[deserr(error = DeserrJsonError, rename_all = camelCase, deny_unknown_fields)]
#[schema(rename_all = "camelCase")]
pub struct FederatedSearch {
    pub queries: Vec<SearchQueryWithIndex>,
    #[deserr(default)]
    pub federation: Option<Federation>,
}

#[derive(Serialize, Clone, ToSchema)]
#[serde(rename_all = "camelCase")]
#[schema(rename_all = "camelCase")]
pub struct FederatedSearchResult {
    pub hits: Vec<SearchHit>,
    pub processing_time_ms: u128,
    #[serde(flatten)]
    pub hits_info: HitsInfo,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub semantic_hit_count: Option<u32>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(value_type = Option<BTreeMap<String, BTreeMap<String, u64>>>)]
    pub facet_distribution: Option<BTreeMap<String, IndexMap<String, u64>>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub facet_stats: Option<BTreeMap<String, FacetStats>>,
    #[serde(skip_serializing_if = "FederatedFacets::is_empty")]
    pub facets_by_index: FederatedFacets,

    // These fields are only used for analytics purposes
    #[serde(skip)]
    pub degraded: bool,
    #[serde(skip)]
    pub used_negative_operator: bool,
}

impl fmt::Debug for FederatedSearchResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let FederatedSearchResult {
            hits,
            processing_time_ms,
            hits_info,
            semantic_hit_count,
            degraded,
            used_negative_operator,
            facet_distribution,
            facet_stats,
            facets_by_index,
        } = self;

        let mut debug = f.debug_struct("SearchResult");
        // The most important thing when looking at a search result is the time it took to process
        debug.field("processing_time_ms", &processing_time_ms);
        debug.field("hits", &format!("[{} hits returned]", hits.len()));
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
        if !facets_by_index.is_empty() {
            debug.field("facets_by_index", &facets_by_index);
        }

        debug.finish()
    }
}

struct WeightedScore<'a> {
    details: &'a [ScoreDetails],
    weight: f64,
}

impl<'a> WeightedScore<'a> {
    pub fn new(details: &'a [ScoreDetails], weight: f64) -> Self {
        Self { details, weight }
    }

    pub fn weighted_global_score(&self) -> f64 {
        ScoreDetails::global_score(self.details.iter()) * self.weight
    }

    pub fn compare_weighted_global_scores(&self, other: &Self) -> Ordering {
        self.weighted_global_score()
            .partial_cmp(&other.weighted_global_score())
            // both are numbers, possibly infinite
            .unwrap()
    }

    pub fn compare(&self, other: &Self) -> Ordering {
        let mut left_it = ScoreDetails::score_values(self.details.iter());
        let mut right_it = ScoreDetails::score_values(other.details.iter());

        loop {
            let left = left_it.next();
            let right = right_it.next();

            match (left, right) {
                (None, None) => return Ordering::Equal,
                (None, Some(_)) => return Ordering::Less,
                (Some(_), None) => return Ordering::Greater,
                (Some(ScoreValue::Score(left)), Some(ScoreValue::Score(right))) => {
                    let left = left * self.weight;
                    let right = right * other.weight;
                    if (left - right).abs() <= f64::EPSILON {
                        continue;
                    }
                    return left.partial_cmp(&right).unwrap();
                }
                (Some(ScoreValue::Sort(left)), Some(ScoreValue::Sort(right))) => {
                    match left.partial_cmp(right) {
                        Some(Ordering::Equal) => continue,
                        Some(order) => return order,
                        None => return self.compare_weighted_global_scores(other),
                    }
                }
                (Some(ScoreValue::GeoSort(left)), Some(ScoreValue::GeoSort(right))) => {
                    match left.partial_cmp(right) {
                        Some(Ordering::Equal) => continue,
                        Some(order) => return order,
                        None => {
                            return self.compare_weighted_global_scores(other);
                        }
                    }
                }
                // not comparable details, use global
                (Some(ScoreValue::Score(_)), Some(_))
                | (Some(_), Some(ScoreValue::Score(_)))
                | (Some(ScoreValue::GeoSort(_)), Some(ScoreValue::Sort(_)))
                | (Some(ScoreValue::Sort(_)), Some(ScoreValue::GeoSort(_))) => {
                    let left_count = left_it.count();
                    let right_count = right_it.count();
                    // compare how many remaining groups of rules each side has.
                    // the group with the most remaining groups wins.
                    return left_count
                        .cmp(&right_count)
                        // breaks ties with the global ranking score
                        .then_with(|| self.compare_weighted_global_scores(other));
                }
            }
        }
    }
}

struct QueryByIndex {
    query: SearchQuery,
    federation_options: FederationOptions,
    query_index: usize,
}

struct SearchResultByQuery<'a> {
    documents_ids: Vec<DocumentId>,
    document_scores: Vec<Vec<ScoreDetails>>,
    federation_options: FederationOptions,
    hit_maker: HitMaker<'a>,
    query_index: usize,
}

struct SearchResultByQueryIter<'a> {
    it: Zip<IntoIter<DocumentId>, IntoIter<Vec<ScoreDetails>>>,
    federation_options: FederationOptions,
    hit_maker: Rc<HitMaker<'a>>,
    query_index: usize,
}

impl<'a> SearchResultByQueryIter<'a> {
    fn new(
        SearchResultByQuery {
            documents_ids,
            document_scores,
            federation_options,
            hit_maker,
            query_index,
        }: SearchResultByQuery<'a>,
    ) -> Self {
        let it = documents_ids.into_iter().zip(document_scores);
        Self { it, federation_options, hit_maker: Rc::new(hit_maker), query_index }
    }
}

struct SearchResultByQueryIterItem<'a> {
    docid: DocumentId,
    score: Vec<ScoreDetails>,
    federation_options: FederationOptions,
    hit_maker: Rc<HitMaker<'a>>,
    query_index: usize,
}

fn merge_index_local_results(
    results_by_query: Vec<SearchResultByQuery<'_>>,
) -> impl Iterator<Item = SearchResultByQueryIterItem> + '_ {
    itertools::kmerge_by(
        results_by_query.into_iter().map(SearchResultByQueryIter::new),
        |left: &SearchResultByQueryIterItem, right: &SearchResultByQueryIterItem| {
            let left_score = WeightedScore::new(&left.score, *left.federation_options.weight);
            let right_score = WeightedScore::new(&right.score, *right.federation_options.weight);

            match left_score.compare(&right_score) {
                // the biggest score goes first
                Ordering::Greater => true,
                // break ties using query index
                Ordering::Equal => left.query_index < right.query_index,
                Ordering::Less => false,
            }
        },
    )
}

fn merge_index_global_results(
    results_by_index: Vec<SearchResultByIndex>,
) -> impl Iterator<Item = SearchHitByIndex> {
    itertools::kmerge_by(
        results_by_index.into_iter().map(|result_by_index| result_by_index.hits.into_iter()),
        |left: &SearchHitByIndex, right: &SearchHitByIndex| {
            let left_score = WeightedScore::new(&left.score, *left.federation_options.weight);
            let right_score = WeightedScore::new(&right.score, *right.federation_options.weight);

            match left_score.compare(&right_score) {
                // the biggest score goes first
                Ordering::Greater => true,
                // break ties using query index
                Ordering::Equal => left.query_index < right.query_index,
                Ordering::Less => false,
            }
        },
    )
}

impl<'a> Iterator for SearchResultByQueryIter<'a> {
    type Item = SearchResultByQueryIterItem<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        let (docid, score) = self.it.next()?;
        Some(SearchResultByQueryIterItem {
            docid,
            score,
            federation_options: self.federation_options,
            hit_maker: Rc::clone(&self.hit_maker),
            query_index: self.query_index,
        })
    }
}

struct SearchHitByIndex {
    hit: SearchHit,
    score: Vec<ScoreDetails>,
    federation_options: FederationOptions,
    query_index: usize,
}

struct SearchResultByIndex {
    index: String,
    hits: Vec<SearchHitByIndex>,
    estimated_total_hits: usize,
    degraded: bool,
    used_negative_operator: bool,
    facets: Option<ComputedFacets>,
}

#[derive(Debug, Clone, Default, Serialize, ToSchema)]
pub struct FederatedFacets(pub BTreeMap<String, ComputedFacets>);

impl FederatedFacets {
    pub fn insert(&mut self, index: String, facets: Option<ComputedFacets>) {
        if let Some(facets) = facets {
            self.0.insert(index, facets);
        }
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn merge(
        self,
        MergeFacets { max_values_per_facet }: MergeFacets,
        facet_order: BTreeMap<String, (String, OrderBy)>,
    ) -> Option<ComputedFacets> {
        if self.is_empty() {
            return None;
        }

        let mut distribution: BTreeMap<String, _> = Default::default();
        let mut stats: BTreeMap<String, FacetStats> = Default::default();

        for facets_by_index in self.0.into_values() {
            for (facet, index_distribution) in facets_by_index.distribution {
                match distribution.entry(facet) {
                    std::collections::btree_map::Entry::Vacant(entry) => {
                        entry.insert(index_distribution);
                    }
                    std::collections::btree_map::Entry::Occupied(mut entry) => {
                        let distribution = entry.get_mut();

                        for (value, index_count) in index_distribution {
                            distribution
                                .entry(value)
                                .and_modify(|count| *count += index_count)
                                .or_insert(index_count);
                        }
                    }
                }
            }

            for (facet, index_stats) in facets_by_index.stats {
                match stats.entry(facet) {
                    std::collections::btree_map::Entry::Vacant(entry) => {
                        entry.insert(index_stats);
                    }
                    std::collections::btree_map::Entry::Occupied(mut entry) => {
                        let stats = entry.get_mut();

                        stats.min = f64::min(stats.min, index_stats.min);
                        stats.max = f64::max(stats.max, index_stats.max);
                    }
                }
            }
        }

        // fixup order
        for (facet, values) in &mut distribution {
            let order_by = facet_order.get(facet).map(|(_, order)| *order).unwrap_or_default();

            match order_by {
                OrderBy::Lexicographic => {
                    values.sort_unstable_by(|left, _, right, _| left.cmp(right))
                }
                OrderBy::Count => {
                    values.sort_unstable_by(|_, left, _, right| {
                        left.cmp(right)
                            // biggest first
                            .reverse()
                    })
                }
            }

            if let Some(max_values_per_facet) = max_values_per_facet {
                values.truncate(max_values_per_facet)
            };
        }

        Some(ComputedFacets { distribution, stats })
    }
}

pub fn perform_federated_search(
    index_scheduler: &IndexScheduler,
    queries: Vec<SearchQueryWithIndex>,
    mut federation: Federation,
    features: RoFeatures,
) -> Result<FederatedSearchResult, ResponseError> {
    let before_search = std::time::Instant::now();

    // this implementation partition the queries by index to guarantee an important property:
    // - all the queries to a particular index use the same read transaction.
    // This is an important property, otherwise we cannot guarantee the self-consistency of the results.

    // 1. partition queries by index
    let mut queries_by_index: BTreeMap<String, Vec<QueryByIndex>> = Default::default();
    for (query_index, federated_query) in queries.into_iter().enumerate() {
        if let Some(pagination_field) = federated_query.has_pagination() {
            return Err(MeilisearchHttpError::PaginationInFederatedQuery(
                query_index,
                pagination_field,
            )
            .into());
        }

        if let Some(facets) = federated_query.has_facets() {
            let facets = facets.to_owned();
            return Err(MeilisearchHttpError::FacetsInFederatedQuery(
                query_index,
                federated_query.index_uid.into_inner(),
                facets,
            )
            .into());
        }

        let (index_uid, query, federation_options) = federated_query.into_index_query_federation();

        queries_by_index.entry(index_uid.into_inner()).or_default().push(QueryByIndex {
            query,
            federation_options: federation_options.unwrap_or_default(),
            query_index,
        })
    }

    // 2. perform queries, merge and make hits index by index
    let required_hit_count = federation.limit + federation.offset;

    // In step (2), semantic_hit_count will be set to Some(0) if any search kind uses semantic
    // Then in step (3), we'll update its value if there is any semantic search
    let mut semantic_hit_count = None;
    let mut results_by_index = Vec::with_capacity(queries_by_index.len());
    let mut previous_query_data: Option<(RankingRules, usize, String)> = None;

    // remember the order and name of first index for each facet when merging with index settings
    // to detect if the order is inconsistent for a facet.
    let mut facet_order: Option<BTreeMap<String, (String, OrderBy)>> = match federation.merge_facets
    {
        Some(MergeFacets { .. }) => Some(Default::default()),
        _ => None,
    };

    for (index_uid, queries) in queries_by_index {
        let first_query_index = queries.first().map(|query| query.query_index);

        let index = match index_scheduler.index(&index_uid) {
            Ok(index) => index,
            Err(err) => {
                let mut err = ResponseError::from(err);
                // Patch the HTTP status code to 400 as it defaults to 404 for `index_not_found`, but
                // here the resource not found is not part of the URL.
                err.code = StatusCode::BAD_REQUEST;
                if let Some(query_index) = first_query_index {
                    err.message = format!("Inside `.queries[{}]`: {}", query_index, err.message);
                }
                return Err(err);
            }
        };

        // Important: this is the only transaction we'll use for this index during this federated search
        let rtxn = index.read_txn()?;

        let criteria = index.criteria(&rtxn)?;

        let dictionary = index.dictionary(&rtxn)?;
        let dictionary: Option<Vec<_>> =
            dictionary.as_ref().map(|x| x.iter().map(String::as_str).collect());
        let separators = index.allowed_separators(&rtxn)?;
        let separators: Option<Vec<_>> =
            separators.as_ref().map(|x| x.iter().map(String::as_str).collect());

        // each query gets its individual cutoff
        let cutoff = index.search_cutoff(&rtxn)?;

        let mut degraded = false;
        let mut used_negative_operator = false;
        let mut candidates = RoaringBitmap::new();

        let facets_by_index = federation.facets_by_index.remove(&index_uid).flatten();

        // TODO: recover the max size + facets_by_index as return value of this function so as not to ask it for all queries
        if let Err(mut error) =
            check_facet_order(&mut facet_order, &index_uid, &facets_by_index, &index, &rtxn)
        {
            error.message = format!(
                "Inside `.federation.facetsByIndex.{index_uid}`: {error}{}",
                if let Some(query_index) = first_query_index {
                    format!("\n - Note: index `{index_uid}` used in `.queries[{query_index}]`")
                } else {
                    Default::default()
                }
            );
            return Err(error);
        }

        // 2.1. Compute all candidates for each query in the index
        let mut results_by_query = Vec::with_capacity(queries.len());

        for QueryByIndex { query, federation_options, query_index } in queries {
            // use an immediately invoked lambda to capture the result without returning from the function

            let res: Result<(), ResponseError> = (|| {
                let search_kind =
                    search_kind(&query, index_scheduler, index_uid.to_string(), &index)?;

                let canonicalization_kind = match (&search_kind, &query.q) {
                    (SearchKind::SemanticOnly { .. }, _) => {
                        ranking_rules::CanonicalizationKind::Vector
                    }
                    (_, Some(q)) if !q.is_empty() => ranking_rules::CanonicalizationKind::Keyword,
                    _ => ranking_rules::CanonicalizationKind::Placeholder,
                };

                let sort = if let Some(sort) = &query.sort {
                    let sorts: Vec<_> =
                        match sort.iter().map(|s| milli::AscDesc::from_str(s)).collect() {
                            Ok(sorts) => sorts,
                            Err(asc_desc_error) => {
                                return Err(milli::Error::from(milli::SortError::from(
                                    asc_desc_error,
                                ))
                                .into())
                            }
                        };
                    Some(sorts)
                } else {
                    None
                };

                let ranking_rules = ranking_rules::RankingRules::new(
                    criteria.clone(),
                    sort,
                    query.matching_strategy.into(),
                    canonicalization_kind,
                );

                if let Some((previous_ranking_rules, previous_query_index, previous_index_uid)) =
                    previous_query_data.take()
                {
                    if let Err(error) = ranking_rules.is_compatible_with(&previous_ranking_rules) {
                        return Err(error.to_response_error(
                            &ranking_rules,
                            &previous_ranking_rules,
                            query_index,
                            previous_query_index,
                            &index_uid,
                            &previous_index_uid,
                        ));
                    }
                    previous_query_data = if previous_ranking_rules.constraint_count()
                        > ranking_rules.constraint_count()
                    {
                        Some((previous_ranking_rules, previous_query_index, previous_index_uid))
                    } else {
                        Some((ranking_rules, query_index, index_uid.clone()))
                    };
                } else {
                    previous_query_data = Some((ranking_rules, query_index, index_uid.clone()));
                }

                match search_kind {
                    SearchKind::KeywordOnly => {}
                    _ => semantic_hit_count = Some(0),
                }

                let retrieve_vectors = RetrieveVectors::new(query.retrieve_vectors);

                let time_budget = match cutoff {
                    Some(cutoff) => TimeBudget::new(Duration::from_millis(cutoff)),
                    None => TimeBudget::default(),
                };

                let (mut search, _is_finite_pagination, _max_total_hits, _offset) =
                    prepare_search(&index, &rtxn, &query, &search_kind, time_budget, features)?;

                search.scoring_strategy(milli::score_details::ScoringStrategy::Detailed);
                search.offset(0);
                search.limit(required_hit_count);

                let (result, _semantic_hit_count) =
                    super::search_from_kind(index_uid.to_string(), search_kind, search)?;
                let format = AttributesFormat {
                    attributes_to_retrieve: query.attributes_to_retrieve,
                    retrieve_vectors,
                    attributes_to_highlight: query.attributes_to_highlight,
                    attributes_to_crop: query.attributes_to_crop,
                    crop_length: query.crop_length,
                    crop_marker: query.crop_marker,
                    highlight_pre_tag: query.highlight_pre_tag,
                    highlight_post_tag: query.highlight_post_tag,
                    show_matches_position: query.show_matches_position,
                    sort: query.sort,
                    show_ranking_score: query.show_ranking_score,
                    show_ranking_score_details: query.show_ranking_score_details,
                    locales: query.locales.map(|l| l.iter().copied().map(Into::into).collect()),
                };

                let milli::SearchResult {
                    matching_words,
                    candidates: query_candidates,
                    documents_ids,
                    document_scores,
                    degraded: query_degraded,
                    used_negative_operator: query_used_negative_operator,
                } = result;

                candidates |= query_candidates;
                degraded |= query_degraded;
                used_negative_operator |= query_used_negative_operator;

                let tokenizer = HitMaker::tokenizer(dictionary.as_deref(), separators.as_deref());

                let formatter_builder = HitMaker::formatter_builder(matching_words, tokenizer);

                let hit_maker =
                    HitMaker::new(&index, &rtxn, format, formatter_builder).map_err(|e| {
                        MeilisearchHttpError::from_milli(e, Some(index_uid.to_string()))
                    })?;

                results_by_query.push(SearchResultByQuery {
                    federation_options,
                    hit_maker,
                    query_index,
                    documents_ids,
                    document_scores,
                });
                Ok(())
            })();

            if let Err(mut error) = res {
                error.message = format!("Inside `.queries[{query_index}]`: {}", error.message);
                return Err(error);
            }
        }
        // 2.2. merge inside index
        let mut documents_seen = RoaringBitmap::new();
        let merged_result: Result<Vec<_>, ResponseError> =
            merge_index_local_results(results_by_query)
                // skip documents we've already seen & mark that we saw the current document
                .filter(|SearchResultByQueryIterItem { docid, .. }| documents_seen.insert(*docid))
                .take(required_hit_count)
                // 2.3 make hits
                .map(
                    |SearchResultByQueryIterItem {
                         docid,
                         score,
                         federation_options,
                         hit_maker,
                         query_index,
                     }| {
                        let mut hit = hit_maker.make_hit(docid, &score)?;
                        let weighted_score =
                            ScoreDetails::global_score(score.iter()) * (*federation_options.weight);

                        let _federation = serde_json::json!(
                            {
                                "indexUid": index_uid,
                                "queriesPosition": query_index,
                                "weightedRankingScore": weighted_score,
                            }
                        );
                        hit.document.insert("_federation".to_string(), _federation);
                        Ok(SearchHitByIndex { hit, score, federation_options, query_index })
                    },
                )
                .collect();

        let merged_result = merged_result?;

        let estimated_total_hits = candidates.len() as usize;

        let facets = facets_by_index
            .map(|facets_by_index| {
                compute_facet_distribution_stats(
                    &facets_by_index,
                    &index,
                    &rtxn,
                    candidates,
                    super::Route::MultiSearch,
                )
            })
            .transpose()
            .map_err(|mut error| {
                error.message = format!(
                    "Inside `.federation.facetsByIndex.{index_uid}`: {}{}",
                    error.message,
                    if let Some(query_index) = first_query_index {
                        format!("\n - Note: index `{index_uid}` used in `.queries[{query_index}]`")
                    } else {
                        Default::default()
                    }
                );
                error
            })?;

        results_by_index.push(SearchResultByIndex {
            index: index_uid,
            hits: merged_result,
            estimated_total_hits,
            degraded,
            used_negative_operator,
            facets,
        });
    }

    // bonus step, make sure to return an error if an index wants a non-faceted field, even if no query actually uses that index.
    for (index_uid, facets) in federation.facets_by_index {
        let index = match index_scheduler.index(&index_uid) {
            Ok(index) => index,
            Err(err) => {
                let mut err = ResponseError::from(err);
                // Patch the HTTP status code to 400 as it defaults to 404 for `index_not_found`, but
                // here the resource not found is not part of the URL.
                err.code = StatusCode::BAD_REQUEST;
                err.message = format!(
                    "Inside `.federation.facetsByIndex.{index_uid}`: {}\n - Note: index `{index_uid}` is not used in queries",
                    err.message
                );
                return Err(err);
            }
        };

        // Important: this is the only transaction we'll use for this index during this federated search
        let rtxn = index.read_txn()?;

        if let Err(mut error) =
            check_facet_order(&mut facet_order, &index_uid, &facets, &index, &rtxn)
        {
            error.message = format!(
                "Inside `.federation.facetsByIndex.{index_uid}`: {error}\n - Note: index `{index_uid}` is not used in queries",
            );
            return Err(error);
        }

        if let Some(facets) = facets {
            if let Err(mut error) = compute_facet_distribution_stats(
                &facets,
                &index,
                &rtxn,
                Default::default(),
                super::Route::MultiSearch,
            ) {
                error.message =
                    format!("Inside `.federation.facetsByIndex.{index_uid}`: {}\n - Note: index `{index_uid}` is not used in queries", error.message);
                return Err(error);
            }
        }
    }

    // 3. merge hits and metadata across indexes
    // 3.1 merge metadata
    let (estimated_total_hits, degraded, used_negative_operator, facets) = {
        let mut estimated_total_hits = 0;
        let mut degraded = false;
        let mut used_negative_operator = false;

        let mut facets: FederatedFacets = FederatedFacets::default();

        for SearchResultByIndex {
            index,
            hits: _,
            estimated_total_hits: estimated_total_hits_by_index,
            facets: facets_by_index,
            degraded: degraded_by_index,
            used_negative_operator: used_negative_operator_by_index,
        } in &mut results_by_index
        {
            estimated_total_hits += *estimated_total_hits_by_index;
            degraded |= *degraded_by_index;
            used_negative_operator |= *used_negative_operator_by_index;

            let facets_by_index = std::mem::take(facets_by_index);
            let index = std::mem::take(index);

            facets.insert(index, facets_by_index);
        }

        (estimated_total_hits, degraded, used_negative_operator, facets)
    };

    // 3.2 merge hits
    let merged_hits: Vec<_> = merge_index_global_results(results_by_index)
        .skip(federation.offset)
        .take(federation.limit)
        .inspect(|hit| {
            if let Some(semantic_hit_count) = &mut semantic_hit_count {
                if hit.score.iter().any(|score| matches!(&score, ScoreDetails::Vector(_))) {
                    *semantic_hit_count += 1;
                }
            }
        })
        .map(|hit| hit.hit)
        .collect();

    let (facet_distribution, facet_stats, facets_by_index) =
        match federation.merge_facets.zip(facet_order) {
            Some((merge_facets, facet_order)) => {
                let facets = facets.merge(merge_facets, facet_order);

                let (facet_distribution, facet_stats) = facets
                    .map(|ComputedFacets { distribution, stats }| (distribution, stats))
                    .unzip();

                (facet_distribution, facet_stats, FederatedFacets::default())
            }
            None => (None, None, facets),
        };

    let search_result = FederatedSearchResult {
        hits: merged_hits,
        processing_time_ms: before_search.elapsed().as_millis(),
        hits_info: HitsInfo::OffsetLimit {
            limit: federation.limit,
            offset: federation.offset,
            estimated_total_hits,
        },
        semantic_hit_count,
        degraded,
        used_negative_operator,
        facet_distribution,
        facet_stats,
        facets_by_index,
    };

    Ok(search_result)
}

fn check_facet_order(
    facet_order: &mut Option<BTreeMap<String, (String, OrderBy)>>,
    current_index: &str,
    facets_by_index: &Option<Vec<String>>,
    index: &milli::Index,
    rtxn: &milli::heed::RoTxn<'_>,
) -> Result<(), ResponseError> {
    if let (Some(facet_order), Some(facets_by_index)) = (facet_order, facets_by_index) {
        let index_facet_order = index.sort_facet_values_by(rtxn)?;
        for facet in facets_by_index {
            let index_facet_order = index_facet_order.get(facet);
            let (previous_index, previous_facet_order) = facet_order
                .entry(facet.to_owned())
                .or_insert_with(|| (current_index.to_owned(), index_facet_order));
            if previous_facet_order != &index_facet_order {
                return Err(MeilisearchHttpError::InconsistentFacetOrder {
                    facet: facet.clone(),
                    previous_facet_order: *previous_facet_order,
                    previous_uid: previous_index.clone(),
                    current_uid: current_index.to_owned(),
                    index_facet_order,
                }
                .into());
            }
        }
    };
    Ok(())
}
