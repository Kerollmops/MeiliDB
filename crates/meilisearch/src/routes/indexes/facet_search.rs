use std::collections::{BinaryHeap, HashSet};

use actix_web::web::Data;
use actix_web::{web, HttpRequest, HttpResponse};
use deserr::actix_web::AwebJson;
use index_scheduler::IndexScheduler;
use meilisearch_types::deserr::DeserrJsonError;
use meilisearch_types::error::deserr_codes::*;
use meilisearch_types::error::ResponseError;
use meilisearch_types::index_uid::IndexUid;
use meilisearch_types::locales::Locale;
use serde_json::Value;
use tracing::debug;
use utoipa::{OpenApi, ToSchema};

use crate::analytics::{Aggregate, Analytics};
use crate::extractors::authentication::policies::*;
use crate::extractors::authentication::GuardedData;
use crate::routes::indexes::search::search_kind;
use crate::search::{
    add_search_rules, perform_facet_search, FacetSearchResult, HybridQuery, MatchingStrategy,
    RankingScoreThreshold, SearchQuery, SearchResult, DEFAULT_CROP_LENGTH, DEFAULT_CROP_MARKER,
    DEFAULT_HIGHLIGHT_POST_TAG, DEFAULT_HIGHLIGHT_PRE_TAG, DEFAULT_SEARCH_LIMIT,
    DEFAULT_SEARCH_OFFSET,
};
use crate::search_queue::SearchQueue;

#[derive(OpenApi)]
#[openapi(
    paths(search),
    tags(
        (
            name = "Facet Search",
            description = "The `/facet-search` route allows you to search for facet values. Facet search supports prefix search and typo tolerance. The returned hits are sorted lexicographically in ascending order. You can configure how facets are sorted using the sortFacetValuesBy property of the faceting index settings.",
            external_docs(url = "https://www.meilisearch.com/docs/reference/api/facet_search"),
        ),
    ),
)]
pub struct FacetSearchApi;

pub fn configure(cfg: &mut web::ServiceConfig) {
    cfg.service(web::resource("").route(web::post().to(search)));
}

// # Important
//
// Intentionally don't use `deny_unknown_fields` to ignore search parameters sent by user
#[derive(Debug, Clone, Default, PartialEq, deserr::Deserr, ToSchema)]
#[deserr(error = DeserrJsonError, rename_all = camelCase)]
pub struct FacetSearchQuery {
    #[deserr(default, error = DeserrJsonError<InvalidFacetSearchQuery>)]
    pub facet_query: Option<String>,
    #[deserr(error = DeserrJsonError<InvalidFacetSearchFacetName>, missing_field_error = DeserrJsonError::missing_facet_search_facet_name)]
    pub facet_name: String,
    #[deserr(default, error = DeserrJsonError<InvalidSearchQ>)]
    pub q: Option<String>,
    #[deserr(default, error = DeserrJsonError<InvalidSearchVector>)]
    pub vector: Option<Vec<f32>>,
    #[deserr(default, error = DeserrJsonError<InvalidSearchHybridQuery>)]
    pub hybrid: Option<HybridQuery>,
    #[deserr(default, error = DeserrJsonError<InvalidSearchFilter>)]
    pub filter: Option<Value>,
    #[deserr(default, error = DeserrJsonError<InvalidSearchMatchingStrategy>, default)]
    pub matching_strategy: MatchingStrategy,
    #[deserr(default, error = DeserrJsonError<InvalidSearchAttributesToSearchOn>, default)]
    pub attributes_to_search_on: Option<Vec<String>>,
    #[deserr(default, error = DeserrJsonError<InvalidSearchRankingScoreThreshold>, default)]
    pub ranking_score_threshold: Option<RankingScoreThreshold>,
    #[deserr(default, error = DeserrJsonError<InvalidSearchLocales>, default)]
    pub locales: Option<Vec<Locale>>,
}

#[derive(Default)]
pub struct FacetSearchAggregator {
    // requests
    total_received: usize,
    total_succeeded: usize,
    time_spent: BinaryHeap<usize>,

    // The set of all facetNames that were used
    facet_names: HashSet<String>,

    // As there been any other parameter than the facetName or facetQuery ones?
    additional_search_parameters_provided: bool,
}

impl FacetSearchAggregator {
    #[allow(clippy::field_reassign_with_default)]
    pub fn from_query(query: &FacetSearchQuery) -> Self {
        let FacetSearchQuery {
            facet_query: _,
            facet_name,
            vector,
            q,
            filter,
            matching_strategy,
            attributes_to_search_on,
            hybrid,
            ranking_score_threshold,
            locales,
        } = query;

        Self {
            total_received: 1,
            facet_names: Some(facet_name.clone()).into_iter().collect(),
            additional_search_parameters_provided: q.is_some()
                || vector.is_some()
                || filter.is_some()
                || *matching_strategy != MatchingStrategy::default()
                || attributes_to_search_on.is_some()
                || hybrid.is_some()
                || ranking_score_threshold.is_some()
                || locales.is_some(),
            ..Default::default()
        }
    }

    pub fn succeed(&mut self, result: &FacetSearchResult) {
        let FacetSearchResult { facet_hits: _, facet_query: _, processing_time_ms } = result;
        self.total_succeeded = 1;
        self.time_spent.push(*processing_time_ms as usize);
    }
}

impl Aggregate for FacetSearchAggregator {
    fn event_name(&self) -> &'static str {
        "Facet Searched POST"
    }

    fn aggregate(mut self: Box<Self>, new: Box<Self>) -> Box<Self> {
        for time in new.time_spent {
            self.time_spent.push(time);
        }

        Box::new(Self {
            total_received: self.total_received.saturating_add(new.total_received),
            total_succeeded: self.total_succeeded.saturating_add(new.total_succeeded),
            time_spent: self.time_spent,
            facet_names: self.facet_names.union(&new.facet_names).cloned().collect(),
            additional_search_parameters_provided: self.additional_search_parameters_provided
                | new.additional_search_parameters_provided,
        })
    }

    fn into_event(self: Box<Self>) -> serde_json::Value {
        let Self {
            total_received,
            total_succeeded,
            time_spent,
            facet_names,
            additional_search_parameters_provided,
        } = *self;
        // the index of the 99th percentage of value
        let percentile_99th = 0.99 * (total_succeeded as f64 - 1.) + 1.;
        // we get all the values in a sorted manner
        let time_spent = time_spent.into_sorted_vec();
        // We are only interested by the slowest value of the 99th fastest results
        let time_spent = time_spent.get(percentile_99th as usize);

        serde_json::json!({
            "requests": {
                "99th_response_time":  time_spent.map(|t| format!("{:.2}", t)),
                "total_succeeded": total_succeeded,
                "total_failed": total_received.saturating_sub(total_succeeded), // just to be sure we never panics
                "total_received": total_received,
            },
            "facets": {
                "total_distinct_facet_count": facet_names.len(),
                "additional_search_parameters_provided": additional_search_parameters_provided,
            },
        })
    }
}

/// Perform a facet search
///
/// Search for a facet value within a given facet.
#[utoipa::path(
    post,
    path = "{indexUid}/facet-search",
    tag = "Facet Search",
    security(("Bearer" = ["search", "*"])),
    params(("indexUid", example = "movies", description = "Index Unique Identifier", nullable = false)),
    request_body = FacetSearchQuery,
    responses(
        (status = 200, description = "The documents are returned", body = SearchResult, content_type = "application/json", example = json!(
            {
              "hits": [
                {
                  "id": 2770,
                  "title": "American Pie 2",
                  "poster": "https://image.tmdb.org/t/p/w1280/q4LNgUnRfltxzp3gf1MAGiK5LhV.jpg",
                  "overview": "The whole gang are back and as close as ever. They decide to get even closer by spending the summer together at a beach house. They decide to hold the biggest…",
                  "release_date": 997405200
                },
                {
                  "id": 190859,
                  "title": "American Sniper",
                  "poster": "https://image.tmdb.org/t/p/w1280/svPHnYE7N5NAGO49dBmRhq0vDQ3.jpg",
                  "overview": "U.S. Navy SEAL Chris Kyle takes his sole mission—protect his comrades—to heart and becomes one of the most lethal snipers in American history. His pinpoint accuracy not only saves countless lives but also makes him a prime…",
                  "release_date": 1418256000
                }
              ],
              "offset": 0,
              "limit": 2,
              "estimatedTotalHits": 976,
              "processingTimeMs": 35,
              "query": "american "
            }
        )),
        (status = 404, description = "Index not found", body = ResponseError, content_type = "application/json", example = json!(
            {
                "message": "Index `movies` not found.",
                "code": "index_not_found",
                "type": "invalid_request",
                "link": "https://docs.meilisearch.com/errors#index_not_found"
            }
        )),
        (status = 401, description = "The authorization header is missing", body = ResponseError, content_type = "application/json", example = json!(
            {
                "message": "The Authorization header is missing. It must use the bearer authorization method.",
                "code": "missing_authorization_header",
                "type": "auth",
                "link": "https://docs.meilisearch.com/errors#missing_authorization_header"
            }
        )),
    )
)]
pub async fn search(
    index_scheduler: GuardedData<ActionPolicy<{ actions::SEARCH }>, Data<IndexScheduler>>,
    search_queue: Data<SearchQueue>,
    index_uid: web::Path<String>,
    params: AwebJson<FacetSearchQuery, DeserrJsonError>,
    req: HttpRequest,
    analytics: web::Data<Analytics>,
) -> Result<HttpResponse, ResponseError> {
    let index_uid = IndexUid::try_from(index_uid.into_inner())?;

    let query = params.into_inner();
    debug!(parameters = ?query, "Facet search");

    let mut aggregate = FacetSearchAggregator::from_query(&query);

    let facet_query = query.facet_query.clone();
    let facet_name = query.facet_name.clone();
    let locales = query.locales.clone().map(|l| l.into_iter().map(Into::into).collect());
    let mut search_query = SearchQuery::from(query);

    // Tenant token search_rules.
    if let Some(search_rules) = index_scheduler.filters().get_index_search_rules(&index_uid) {
        add_search_rules(&mut search_query.filter, search_rules);
    }

    let index = index_scheduler.index(&index_uid)?;
    let search_kind = search_kind(&search_query, &index_scheduler, index_uid.to_string(), &index)?;
    let permit = search_queue.try_get_search_permit().await?;
    let search_result = tokio::task::spawn_blocking(move || {
        perform_facet_search(
            &index,
            search_query,
            facet_query,
            facet_name,
            search_kind,
            index_scheduler.features(),
            locales,
        )
    })
    .await;
    permit.drop().await;
    let search_result = search_result?;

    if let Ok(ref search_result) = search_result {
        aggregate.succeed(search_result);
    }
    analytics.publish(aggregate, &req);

    let search_result = search_result?;

    debug!(returns = ?search_result, "Facet search");
    Ok(HttpResponse::Ok().json(search_result))
}

impl From<FacetSearchQuery> for SearchQuery {
    fn from(value: FacetSearchQuery) -> Self {
        let FacetSearchQuery {
            facet_query: _,
            facet_name: _,
            q,
            vector,
            filter,
            matching_strategy,
            attributes_to_search_on,
            hybrid,
            ranking_score_threshold,
            locales,
        } = value;

        SearchQuery {
            q,
            offset: DEFAULT_SEARCH_OFFSET(),
            limit: DEFAULT_SEARCH_LIMIT(),
            page: None,
            hits_per_page: None,
            attributes_to_retrieve: None,
            retrieve_vectors: false,
            attributes_to_crop: None,
            crop_length: DEFAULT_CROP_LENGTH(),
            attributes_to_highlight: None,
            show_matches_position: false,
            show_ranking_score: false,
            show_ranking_score_details: false,
            filter,
            sort: None,
            distinct: None,
            facets: None,
            highlight_pre_tag: DEFAULT_HIGHLIGHT_PRE_TAG(),
            highlight_post_tag: DEFAULT_HIGHLIGHT_POST_TAG(),
            crop_marker: DEFAULT_CROP_MARKER(),
            matching_strategy,
            vector,
            attributes_to_search_on,
            hybrid,
            ranking_score_threshold,
            locales,
        }
    }
}
