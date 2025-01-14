use actix_http::StatusCode;
use actix_web::web::{self, Data};
use actix_web::{HttpRequest, HttpResponse};
use deserr::actix_web::AwebJson;
use index_scheduler::IndexScheduler;
use meilisearch_types::deserr::DeserrJsonError;
use meilisearch_types::error::ResponseError;
use meilisearch_types::keys::actions;
use serde::Serialize;
use tracing::debug;
use utoipa::{OpenApi, ToSchema};

use super::multi_search_analytics::MultiSearchAggregator;
use crate::analytics::Analytics;
use crate::error::MeilisearchHttpError;
use crate::extractors::authentication::policies::ActionPolicy;
use crate::extractors::authentication::{AuthenticationError, GuardedData};
use crate::extractors::sequential_extractor::SeqHandler;
use crate::routes::indexes::search::search_kind;
use crate::search::{
    add_search_rules, perform_federated_search, perform_search, FederatedSearch,
    FederatedSearchResult, RetrieveVectors, SearchQueryWithIndex, SearchResultWithIndex,
};
use crate::search_queue::SearchQueue;

#[derive(OpenApi)]
#[openapi(
    paths(multi_search_with_post),
    tags((
        name = "Multi-search",
        description = "The `/multi-search` route allows you to perform multiple search queries on one or more indexes by bundling them into a single HTTP request. Multi-search is also known as federated search.",
        external_docs(url = "https://www.meilisearch.com/docs/reference/api/multi_search"),
    )),
)]
pub struct MultiSearchApi;

pub fn configure(cfg: &mut web::ServiceConfig) {
    cfg.service(web::resource("").route(web::post().to(SeqHandler(multi_search_with_post))));
}

#[derive(Serialize, ToSchema)]
pub struct SearchResults {
    results: Vec<SearchResultWithIndex>,
}

/// Perform a multi-search
///
/// Bundle multiple search queries in a single API request. Use this endpoint to search through multiple indexes at once.
#[utoipa::path(
    post,
    path = "",
    tag = "Multi-search",
    security(("Bearer" = ["search", "*"])),
    responses(
        (status = OK, description = "Non federated multi-search", body = SearchResults, content_type = "application/json", example = json!(
            {
                "results":[
                    {
                        "indexUid":"movies",
                        "hits":[
                            {
                                "id":13682,
                                "title":"Pooh's Heffalump Movie",
                            },
                        ],
                        "query":"pooh",
                        "processingTimeMs":26,
                        "limit":1,
                        "offset":0,
                        "estimatedTotalHits":22
                    },
                    {
                        "indexUid":"movies",
                        "hits":[
                            {
                                "id":12,
                                "title":"Finding Nemo",
                            },
                        ],
                        "query":"nemo",
                        "processingTimeMs":5,
                        "limit":1,
                        "offset":0,
                        "estimatedTotalHits":11
                    },
                    {
                        "indexUid":"movie_ratings",
                        "hits":[
                            {
                                "id":"Us",
                                "director": "Jordan Peele",
                            }
                        ],
                        "query":"Us",
                        "processingTimeMs":0,
                        "limit":1,
                        "offset":0,
                        "estimatedTotalHits":1
                    }
                ]
            }
        )),
        (status = OK, description = "Federated multi-search", body = FederatedSearchResult, content_type = "application/json", example = json!(
            {
                "hits": [
                    {
                        "id": 42,
                        "title": "Batman returns",
                        "overview": "The overview of batman returns",
                        "_federation": {
                            "indexUid": "movies",
                            "queriesPosition": 0
                        }
                    },
                    {
                        "comicsId": "batman-killing-joke",
                        "description": "This comic is really awesome",
                        "title": "Batman: the killing joke",
                        "_federation": {
                            "indexUid": "comics",
                            "queriesPosition": 1
                        }
                    },
                ],
                "processingTimeMs": 0,
                "limit": 20,
                "offset": 0,
                "estimatedTotalHits": 2,
                "semanticHitCount": 0
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
pub async fn multi_search_with_post(
    index_scheduler: GuardedData<ActionPolicy<{ actions::SEARCH }>, Data<IndexScheduler>>,
    search_queue: Data<SearchQueue>,
    params: AwebJson<FederatedSearch, DeserrJsonError>,
    req: HttpRequest,
    analytics: web::Data<Analytics>,
) -> Result<HttpResponse, ResponseError> {
    // Since we don't want to process half of the search requests and then get a permit refused
    // we're going to get one permit for the whole duration of the multi-search request.
    let permit = search_queue.try_get_search_permit().await?;

    let federated_search = params.into_inner();

    let mut multi_aggregate = MultiSearchAggregator::from_federated_search(&federated_search);

    let FederatedSearch { mut queries, federation } = federated_search;

    let features = index_scheduler.features();

    // regardless of federation, check authorization and apply search rules
    let auth = 'check_authorization: {
        for (query_index, federated_query) in queries.iter_mut().enumerate() {
            let index_uid = federated_query.index_uid.as_str();
            // Check index from API key
            if !index_scheduler.filters().is_index_authorized(index_uid) {
                break 'check_authorization Err(AuthenticationError::InvalidToken)
                    .with_index(query_index);
            }
            // Apply search rules from tenant token
            if let Some(search_rules) = index_scheduler.filters().get_index_search_rules(index_uid)
            {
                add_search_rules(&mut federated_query.filter, search_rules);
            }
        }
        Ok(())
    };

    auth.map_err(|(mut err, query_index)| {
        // Add the query index that failed as context for the error message.
        // We're doing it only here and not directly in the `WithIndex` trait so that the `with_index` function returns a different type
        // of result and we can benefit from static typing.
        err.message = format!("Inside `.queries[{query_index}]`: {}", err.message);
        err
    })?;

    let response = match federation {
        Some(federation) => {
            let search_result = tokio::task::spawn_blocking(move || {
                perform_federated_search(&index_scheduler, queries, federation, features)
            })
            .await;
            permit.drop().await;

            if let Ok(Ok(_)) = search_result {
                multi_aggregate.succeed();
            }

            analytics.publish(multi_aggregate, &req);
            HttpResponse::Ok().json(search_result??)
        }
        None => {
            // Explicitly expect a `(ResponseError, usize)` for the error type rather than `ResponseError` only,
            // so that `?` doesn't work if it doesn't use `with_index`, ensuring that it is not forgotten in case of code
            // changes.
            let search_results: Result<_, (ResponseError, usize)> = async {
                let mut search_results = Vec::with_capacity(queries.len());
                for (query_index, (index_uid, query, federation_options)) in queries
                    .into_iter()
                    .map(SearchQueryWithIndex::into_index_query_federation)
                    .enumerate()
                {
                    debug!(on_index = query_index, parameters = ?query, "Multi-search");

                    if federation_options.is_some() {
                        return Err((
                            MeilisearchHttpError::FederationOptionsInNonFederatedRequest(
                                query_index,
                            )
                            .into(),
                            query_index,
                        ));
                    }

                    let index = index_scheduler
                        .index(&index_uid)
                        .map_err(|err| {
                            let mut err = ResponseError::from(err);
                            // Patch the HTTP status code to 400 as it defaults to 404 for `index_not_found`, but
                            // here the resource not found is not part of the URL.
                            err.code = StatusCode::BAD_REQUEST;
                            err
                        })
                        .with_index(query_index)?;

                    let index_uid_str = index_uid.to_string();

                    let search_kind = search_kind(
                        &query,
                        index_scheduler.get_ref(),
                        index_uid_str.clone(),
                        &index,
                    )
                    .with_index(query_index)?;
                    let retrieve_vector = RetrieveVectors::new(query.retrieve_vectors);

                    let search_result = tokio::task::spawn_blocking(move || {
                        perform_search(
                            index_uid_str.clone(),
                            &index,
                            query,
                            search_kind,
                            retrieve_vector,
                            features,
                        )
                    })
                    .await
                    .with_index(query_index)?;

                    search_results.push(SearchResultWithIndex {
                        index_uid: index_uid.into_inner(),
                        result: search_result.with_index(query_index)?,
                    });
                }
                Ok(search_results)
            }
            .await;
            permit.drop().await;

            if search_results.is_ok() {
                multi_aggregate.succeed();
            }
            analytics.publish(multi_aggregate, &req);

            let search_results = search_results.map_err(|(mut err, query_index)| {
                // Add the query index that failed as context for the error message.
                // We're doing it only here and not directly in the `WithIndex` trait so that the `with_index` function returns a different type
                // of result and we can benefit from static typing.
                err.message = format!("Inside `.queries[{query_index}]`: {}", err.message);
                err
            })?;

            debug!(returns = ?search_results, "Multi-search");

            HttpResponse::Ok().json(SearchResults { results: search_results })
        }
    };

    Ok(response)
}

/// Local `Result` extension trait to avoid `map_err` boilerplate.
trait WithIndex {
    type T;
    /// convert the error type inside of the `Result` to a `ResponseError`, and return a couple of it + the usize.
    fn with_index(self, index: usize) -> Result<Self::T, (ResponseError, usize)>;
}

impl<T, E: Into<ResponseError>> WithIndex for Result<T, E> {
    type T = T;
    fn with_index(self, index: usize) -> Result<T, (ResponseError, usize)> {
        self.map_err(|err| (err.into(), index))
    }
}
