use actix_web::web::Data;
use actix_web::{web, HttpRequest, HttpResponse};
use deserr::actix_web::{AwebJson, AwebQueryParameter};
use index_scheduler::IndexScheduler;
use meilisearch_types::deserr::query_params::Param;
use meilisearch_types::deserr::{DeserrJsonError, DeserrQueryParamError};
use meilisearch_types::error::deserr_codes::*;
use meilisearch_types::error::ResponseError;
use meilisearch_types::index_uid::IndexUid;
use meilisearch_types::locales::Locale;
use meilisearch_types::milli;
use meilisearch_types::serde_cs::vec::CS;
use serde_json::Value;
use tracing::debug;
use utoipa::{IntoParams, OpenApi};

use crate::analytics::Analytics;
use crate::error::MeilisearchHttpError;
use crate::extractors::authentication::policies::*;
use crate::extractors::authentication::GuardedData;
use crate::extractors::sequential_extractor::SeqHandler;
use crate::metrics::MEILISEARCH_DEGRADED_SEARCH_REQUESTS;
use crate::routes::indexes::search_analytics::{SearchAggregator, SearchGET, SearchPOST};
use crate::search::{
    add_search_rules, perform_search, HybridQuery, MatchingStrategy, RankingScoreThreshold,
    RetrieveVectors, SearchKind, SearchQuery, SearchResult, SemanticRatio, DEFAULT_CROP_LENGTH,
    DEFAULT_CROP_MARKER, DEFAULT_HIGHLIGHT_POST_TAG, DEFAULT_HIGHLIGHT_PRE_TAG,
    DEFAULT_SEARCH_LIMIT, DEFAULT_SEARCH_OFFSET, DEFAULT_SEMANTIC_RATIO,
};
use crate::search_queue::SearchQueue;

#[derive(OpenApi)]
#[openapi(
    paths(search_with_url_query, search_with_post),
    tags(
        (
            name = "Search",
            description = "Meilisearch exposes two routes to perform searches:

- A POST route: this is the preferred route when using API authentication, as it allows [preflight request](https://developer.mozilla.org/en-US/docs/Glossary/Preflight_request) caching and better performance.
- A GET route: the usage of this route is discouraged, unless you have good reason to do otherwise (specific caching abilities for example)",
            external_docs(url = "https://www.meilisearch.com/docs/reference/api/search"),
        ),
    ),
)]
pub struct SearchApi;

pub fn configure(cfg: &mut web::ServiceConfig) {
    cfg.service(
        web::resource("")
            .route(web::get().to(SeqHandler(search_with_url_query)))
            .route(web::post().to(SeqHandler(search_with_post))),
    );
}

#[derive(Debug, deserr::Deserr, IntoParams)]
#[deserr(error = DeserrQueryParamError, rename_all = camelCase, deny_unknown_fields)]
#[into_params(rename_all = "camelCase", parameter_in = Query)]
pub struct SearchQueryGet {
    #[deserr(default, error = DeserrQueryParamError<InvalidSearchQ>)]
    q: Option<String>,
    #[deserr(default, error = DeserrQueryParamError<InvalidSearchVector>)]
    #[param(value_type = Vec<f32>, explode = false)]
    vector: Option<CS<f32>>,
    #[deserr(default = Param(DEFAULT_SEARCH_OFFSET()), error = DeserrQueryParamError<InvalidSearchOffset>)]
    #[param(value_type = usize, default = DEFAULT_SEARCH_OFFSET)]
    offset: Param<usize>,
    #[deserr(default = Param(DEFAULT_SEARCH_LIMIT()), error = DeserrQueryParamError<InvalidSearchLimit>)]
    #[param(value_type = usize, default = DEFAULT_SEARCH_LIMIT)]
    limit: Param<usize>,
    #[deserr(default, error = DeserrQueryParamError<InvalidSearchPage>)]
    #[param(value_type = Option<usize>)]
    page: Option<Param<usize>>,
    #[deserr(default, error = DeserrQueryParamError<InvalidSearchHitsPerPage>)]
    #[param(value_type = Option<usize>)]
    hits_per_page: Option<Param<usize>>,
    #[deserr(default, error = DeserrQueryParamError<InvalidSearchAttributesToRetrieve>)]
    #[param(value_type = Vec<String>, explode = false)]
    attributes_to_retrieve: Option<CS<String>>,
    #[deserr(default, error = DeserrQueryParamError<InvalidSearchRetrieveVectors>)]
    #[param(value_type = bool, default)]
    retrieve_vectors: Param<bool>,
    #[deserr(default, error = DeserrQueryParamError<InvalidSearchAttributesToCrop>)]
    #[param(value_type = Vec<String>, explode = false)]
    attributes_to_crop: Option<CS<String>>,
    #[deserr(default = Param(DEFAULT_CROP_LENGTH()), error = DeserrQueryParamError<InvalidSearchCropLength>)]
    #[param(value_type = usize, default = DEFAULT_CROP_LENGTH)]
    crop_length: Param<usize>,
    #[deserr(default, error = DeserrQueryParamError<InvalidSearchAttributesToHighlight>)]
    #[param(value_type = Vec<String>, explode = false)]
    attributes_to_highlight: Option<CS<String>>,
    #[deserr(default, error = DeserrQueryParamError<InvalidSearchFilter>)]
    filter: Option<String>,
    #[deserr(default, error = DeserrQueryParamError<InvalidSearchSort>)]
    sort: Option<String>,
    #[deserr(default, error = DeserrQueryParamError<InvalidSearchDistinct>)]
    distinct: Option<String>,
    #[deserr(default, error = DeserrQueryParamError<InvalidSearchShowMatchesPosition>)]
    #[param(value_type = bool)]
    show_matches_position: Param<bool>,
    #[deserr(default, error = DeserrQueryParamError<InvalidSearchShowRankingScore>)]
    #[param(value_type = bool)]
    show_ranking_score: Param<bool>,
    #[deserr(default, error = DeserrQueryParamError<InvalidSearchShowRankingScoreDetails>)]
    #[param(value_type = bool)]
    show_ranking_score_details: Param<bool>,
    #[deserr(default, error = DeserrQueryParamError<InvalidSearchFacets>)]
    #[param(value_type = Vec<String>, explode = false)]
    facets: Option<CS<String>>,
    #[deserr(default = DEFAULT_HIGHLIGHT_PRE_TAG(), error = DeserrQueryParamError<InvalidSearchHighlightPreTag>)]
    #[param(default = DEFAULT_HIGHLIGHT_PRE_TAG)]
    highlight_pre_tag: String,
    #[deserr(default = DEFAULT_HIGHLIGHT_POST_TAG(), error = DeserrQueryParamError<InvalidSearchHighlightPostTag>)]
    #[param(default = DEFAULT_HIGHLIGHT_POST_TAG)]
    highlight_post_tag: String,
    #[deserr(default = DEFAULT_CROP_MARKER(), error = DeserrQueryParamError<InvalidSearchCropMarker>)]
    #[param(default = DEFAULT_CROP_MARKER)]
    crop_marker: String,
    #[deserr(default, error = DeserrQueryParamError<InvalidSearchMatchingStrategy>)]
    matching_strategy: MatchingStrategy,
    #[deserr(default, error = DeserrQueryParamError<InvalidSearchAttributesToSearchOn>)]
    #[param(value_type = Vec<String>, explode = false)]
    pub attributes_to_search_on: Option<CS<String>>,
    #[deserr(default, error = DeserrQueryParamError<InvalidSearchEmbedder>)]
    pub hybrid_embedder: Option<String>,
    #[deserr(default, error = DeserrQueryParamError<InvalidSearchSemanticRatio>)]
    #[param(value_type = f32)]
    pub hybrid_semantic_ratio: Option<SemanticRatioGet>,
    #[deserr(default, error = DeserrQueryParamError<InvalidSearchRankingScoreThreshold>)]
    #[param(value_type = f32)]
    pub ranking_score_threshold: Option<RankingScoreThresholdGet>,
    #[deserr(default, error = DeserrQueryParamError<InvalidSearchLocales>)]
    #[param(value_type = Vec<Locale>, explode = false)]
    pub locales: Option<CS<Locale>>,
}

#[derive(Debug, Clone, Copy, PartialEq, deserr::Deserr)]
#[deserr(try_from(String) = TryFrom::try_from -> InvalidSearchRankingScoreThreshold)]
pub struct RankingScoreThresholdGet(RankingScoreThreshold);

impl std::convert::TryFrom<String> for RankingScoreThresholdGet {
    type Error = InvalidSearchRankingScoreThreshold;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        let f: f64 = s.parse().map_err(|_| InvalidSearchRankingScoreThreshold)?;
        Ok(RankingScoreThresholdGet(RankingScoreThreshold::try_from(f)?))
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, deserr::Deserr)]
#[deserr(try_from(String) = TryFrom::try_from -> InvalidSearchSemanticRatio)]
pub struct SemanticRatioGet(SemanticRatio);

impl std::convert::TryFrom<String> for SemanticRatioGet {
    type Error = InvalidSearchSemanticRatio;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        let f: f32 = s.parse().map_err(|_| InvalidSearchSemanticRatio)?;
        Ok(SemanticRatioGet(SemanticRatio::try_from(f)?))
    }
}

impl std::ops::Deref for SemanticRatioGet {
    type Target = SemanticRatio;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl TryFrom<SearchQueryGet> for SearchQuery {
    type Error = ResponseError;

    fn try_from(other: SearchQueryGet) -> Result<Self, Self::Error> {
        let filter = match other.filter {
            Some(f) => match serde_json::from_str(&f) {
                Ok(v) => Some(v),
                _ => Some(Value::String(f)),
            },
            None => None,
        };

        let hybrid = match (other.hybrid_embedder, other.hybrid_semantic_ratio) {
            (None, None) => None,
            (None, Some(_)) => {
                return Err(ResponseError::from_msg(
                    "`hybridEmbedder` is mandatory when `hybridSemanticRatio` is present".into(),
                    meilisearch_types::error::Code::InvalidSearchHybridQuery,
                ));
            }
            (Some(embedder), None) => {
                Some(HybridQuery { semantic_ratio: DEFAULT_SEMANTIC_RATIO(), embedder })
            }
            (Some(embedder), Some(semantic_ratio)) => {
                Some(HybridQuery { semantic_ratio: *semantic_ratio, embedder })
            }
        };

        if other.vector.is_some() && hybrid.is_none() {
            return Err(ResponseError::from_msg(
                "`hybridEmbedder` is mandatory when `vector` is present".into(),
                meilisearch_types::error::Code::MissingSearchHybrid,
            ));
        }

        Ok(Self {
            q: other.q,
            vector: other.vector.map(CS::into_inner),
            offset: other.offset.0,
            limit: other.limit.0,
            page: other.page.as_deref().copied(),
            hits_per_page: other.hits_per_page.as_deref().copied(),
            attributes_to_retrieve: other.attributes_to_retrieve.map(|o| o.into_iter().collect()),
            retrieve_vectors: other.retrieve_vectors.0,
            attributes_to_crop: other.attributes_to_crop.map(|o| o.into_iter().collect()),
            crop_length: other.crop_length.0,
            attributes_to_highlight: other.attributes_to_highlight.map(|o| o.into_iter().collect()),
            filter,
            sort: other.sort.map(|attr| fix_sort_query_parameters(&attr)),
            distinct: other.distinct,
            show_matches_position: other.show_matches_position.0,
            show_ranking_score: other.show_ranking_score.0,
            show_ranking_score_details: other.show_ranking_score_details.0,
            facets: other.facets.map(|o| o.into_iter().collect()),
            highlight_pre_tag: other.highlight_pre_tag,
            highlight_post_tag: other.highlight_post_tag,
            crop_marker: other.crop_marker,
            matching_strategy: other.matching_strategy,
            attributes_to_search_on: other.attributes_to_search_on.map(|o| o.into_iter().collect()),
            hybrid,
            ranking_score_threshold: other.ranking_score_threshold.map(|o| o.0),
            locales: other.locales.map(|o| o.into_iter().collect()),
        })
    }
}

// TODO: TAMO: split on :asc, and :desc, instead of doing some weird things

/// Transform the sort query parameter into something that matches the post expected format.
pub fn fix_sort_query_parameters(sort_query: &str) -> Vec<String> {
    let mut sort_parameters = Vec::new();
    let mut merge = false;
    for current_sort in sort_query.trim_matches('"').split(',').map(|s| s.trim()) {
        if current_sort.starts_with("_geoPoint(") {
            sort_parameters.push(current_sort.to_string());
            merge = true;
        } else if merge && !sort_parameters.is_empty() {
            let s = sort_parameters.last_mut().unwrap();
            s.push(',');
            s.push_str(current_sort);
            if current_sort.ends_with("):desc") || current_sort.ends_with("):asc") {
                merge = false;
            }
        } else {
            sort_parameters.push(current_sort.to_string());
            merge = false;
        }
    }
    sort_parameters
}

/// Search an index with GET
///
/// Search for documents matching a specific query in the given index.
#[utoipa::path(
    get,
    path = "/{indexUid}/search",
    tags = ["Indexes", "Search"],
    security(("Bearer" = ["search", "*"])),
    params(
        ("indexUid" = String, Path, example = "movies", description = "Index Unique Identifier", nullable = false),
        SearchQueryGet
    ),
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
pub async fn search_with_url_query(
    index_scheduler: GuardedData<ActionPolicy<{ actions::SEARCH }>, Data<IndexScheduler>>,
    search_queue: web::Data<SearchQueue>,
    index_uid: web::Path<String>,
    params: AwebQueryParameter<SearchQueryGet, DeserrQueryParamError>,
    req: HttpRequest,
    analytics: web::Data<Analytics>,
) -> Result<HttpResponse, ResponseError> {
    debug!(parameters = ?params, "Search get");
    let index_uid = IndexUid::try_from(index_uid.into_inner())?;

    let mut query: SearchQuery = params.into_inner().try_into()?;

    // Tenant token search_rules.
    if let Some(search_rules) = index_scheduler.filters().get_index_search_rules(&index_uid) {
        add_search_rules(&mut query.filter, search_rules);
    }

    let mut aggregate = SearchAggregator::<SearchGET>::from_query(&query);

    let index = index_scheduler.index(&index_uid)?;

    let search_kind =
        search_kind(&query, index_scheduler.get_ref(), index_uid.to_string(), &index)?;
    let retrieve_vector = RetrieveVectors::new(query.retrieve_vectors);
    let permit = search_queue.try_get_search_permit().await?;
    let search_result = tokio::task::spawn_blocking(move || {
        perform_search(
            index_uid.to_string(),
            &index,
            query,
            search_kind,
            retrieve_vector,
            index_scheduler.features(),
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

    debug!(returns = ?search_result, "Search get");
    Ok(HttpResponse::Ok().json(search_result))
}

/// Search with POST
///
/// Search for documents matching a specific query in the given index.
#[utoipa::path(
    post,
    path = "/{indexUid}/search",
    tags = ["Indexes", "Search"],
    security(("Bearer" = ["search", "*"])),
    params(
        ("indexUid", example = "movies", description = "Index Unique Identifier", nullable = false),
    ),
    request_body = SearchQuery,
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
pub async fn search_with_post(
    index_scheduler: GuardedData<ActionPolicy<{ actions::SEARCH }>, Data<IndexScheduler>>,
    search_queue: web::Data<SearchQueue>,
    index_uid: web::Path<String>,
    params: AwebJson<SearchQuery, DeserrJsonError>,
    req: HttpRequest,
    analytics: web::Data<Analytics>,
) -> Result<HttpResponse, ResponseError> {
    let index_uid = IndexUid::try_from(index_uid.into_inner())?;

    let mut query = params.into_inner();
    debug!(parameters = ?query, "Search post");

    // Tenant token search_rules.
    if let Some(search_rules) = index_scheduler.filters().get_index_search_rules(&index_uid) {
        add_search_rules(&mut query.filter, search_rules);
    }

    let mut aggregate = SearchAggregator::<SearchPOST>::from_query(&query);

    let index = index_scheduler.index(&index_uid)?;

    let search_kind =
        search_kind(&query, index_scheduler.get_ref(), index_uid.to_string(), &index)?;
    let retrieve_vectors = RetrieveVectors::new(query.retrieve_vectors);

    let permit = search_queue.try_get_search_permit().await?;
    let search_result = tokio::task::spawn_blocking(move || {
        perform_search(
            index_uid.to_string(),
            &index,
            query,
            search_kind,
            retrieve_vectors,
            index_scheduler.features(),
        )
    })
    .await;
    permit.drop().await;
    let search_result = search_result?;
    if let Ok(ref search_result) = search_result {
        aggregate.succeed(search_result);
        if search_result.degraded {
            MEILISEARCH_DEGRADED_SEARCH_REQUESTS.inc();
        }
    }
    analytics.publish(aggregate, &req);

    let search_result = search_result?;

    debug!(returns = ?search_result, "Search post");
    Ok(HttpResponse::Ok().json(search_result))
}

pub fn search_kind(
    query: &SearchQuery,
    index_scheduler: &IndexScheduler,
    index_uid: String,
    index: &milli::Index,
) -> Result<SearchKind, ResponseError> {
    // handle with care, the order of cases matters, the semantics is subtle
    match (query.q.as_deref(), &query.hybrid, query.vector.as_deref()) {
        // empty query, no vector => placeholder search
        (Some(q), _, None) if q.trim().is_empty() => Ok(SearchKind::KeywordOnly),
        // no query, no vector => placeholder search
        (None, _, None) => Ok(SearchKind::KeywordOnly),
        // hybrid.semantic_ratio == 1.0 => vector
        (_, Some(HybridQuery { semantic_ratio, embedder }), v) if **semantic_ratio == 1.0 => {
            SearchKind::semantic(index_scheduler, index_uid, index, embedder, v.map(|v| v.len()))
        }
        // hybrid.semantic_ratio == 0.0 => keyword
        (_, Some(HybridQuery { semantic_ratio, embedder: _ }), _) if **semantic_ratio == 0.0 => {
            Ok(SearchKind::KeywordOnly)
        }
        // no query, hybrid, vector => semantic
        (None, Some(HybridQuery { semantic_ratio: _, embedder }), Some(v)) => {
            SearchKind::semantic(index_scheduler, index_uid, index, embedder, Some(v.len()))
        }
        // query, no hybrid, no vector => keyword
        (Some(_), None, None) => Ok(SearchKind::KeywordOnly),
        // query, hybrid, maybe vector => hybrid
        (Some(_), Some(HybridQuery { semantic_ratio, embedder }), v) => SearchKind::hybrid(
            index_scheduler,
            index_uid,
            index,
            embedder,
            **semantic_ratio,
            v.map(|v| v.len()),
        ),

        (_, None, Some(_)) => Err(MeilisearchHttpError::MissingSearchHybrid.into()),
    }
}
