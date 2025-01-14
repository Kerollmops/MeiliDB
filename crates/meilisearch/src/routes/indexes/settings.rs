use actix_web::web::Data;
use actix_web::{web, HttpRequest, HttpResponse};
use deserr::actix_web::AwebJson;
use index_scheduler::IndexScheduler;
use meilisearch_types::deserr::DeserrJsonError;
use meilisearch_types::error::ResponseError;
use meilisearch_types::index_uid::IndexUid;
use meilisearch_types::settings::{
    settings, SecretPolicy, SettingEmbeddingSettings, Settings, Unchecked,
};
use meilisearch_types::tasks::KindWithContent;
use tracing::debug;
use utoipa::OpenApi;

use super::settings_analytics::*;
use crate::analytics::Analytics;
use crate::extractors::authentication::policies::*;
use crate::extractors::authentication::GuardedData;
use crate::routes::{get_task_id, is_dry_run, SummarizedTaskView};
use crate::Opt;

/// This macro generates the routes for the settings.
///
/// It takes a list of settings and generates a module for each setting.
/// Each module contains the `get`, `update` and `delete` routes for the setting.
///
/// It also generates a `configure` function that configures the routes for the settings.
macro_rules! make_setting_routes {
    ($({route: $route:literal, update_verb: $update_verb:ident, value_type: $type:ty, err_type: $err_ty:ty, attr: $attr:ident, camelcase_attr: $camelcase_attr:literal, analytics: $analytics:ident},)*) => {
        $(
            make_setting_route!($route, $update_verb, $type, $err_ty, $attr, $camelcase_attr, $analytics);
        )*

        #[derive(OpenApi)]
        #[openapi(
            paths(update_all, get_all, delete_all, $( $attr::get, $attr::update, $attr::delete,)*),
            tags(
                (
                    name = "Settings",
                    description = "Use the /settings route to customize search settings for a given index. You can either modify all index settings at once using the update settings endpoint, or use a child route to configure a single setting.",
                    external_docs(url = "https://www.meilisearch.com/docs/reference/api/settings"),
                ),
            ),
        )]
        pub struct SettingsApi;

        pub fn configure(cfg: &mut web::ServiceConfig) {
            use crate::extractors::sequential_extractor::SeqHandler;
            cfg.service(
                web::resource("")
                .route(web::patch().to(SeqHandler(update_all)))
                .route(web::get().to(SeqHandler(get_all)))
                .route(web::delete().to(SeqHandler(delete_all))))
                $(.service($attr::resources()))*;
        }

        pub const ALL_SETTINGS_NAMES: &[&str] = &[$(stringify!($attr)),*];
    };
}

#[macro_export]
macro_rules! make_setting_route {
    ($route:literal, $update_verb:ident, $type:ty, $err_ty:ty, $attr:ident, $camelcase_attr:literal, $analytics:ident) => {
        pub mod $attr {
            use actix_web::web::Data;
            use actix_web::{web, HttpRequest, HttpResponse, Resource};
            use index_scheduler::IndexScheduler;
            use meilisearch_types::error::ResponseError;
            use meilisearch_types::index_uid::IndexUid;
            use meilisearch_types::milli::update::Setting;
            use meilisearch_types::settings::{settings, Settings};
            use meilisearch_types::tasks::KindWithContent;
            use tracing::debug;
            use $crate::analytics::Analytics;
            use $crate::extractors::authentication::policies::*;
            use $crate::extractors::authentication::GuardedData;
            use $crate::extractors::sequential_extractor::SeqHandler;
            use $crate::Opt;
            use $crate::routes::{is_dry_run, get_task_id, SummarizedTaskView};
            #[allow(unused_imports)]
            use super::*;

            #[utoipa::path(
                delete,
                path = concat!("{indexUid}/settings", $route),
                tag = "Settings",
                security(("Bearer" = ["settings.update", "settings.*", "*"])),
                operation_id = concat!("delete", $camelcase_attr),
                summary = concat!("Reset ", $camelcase_attr),
                description = concat!("Reset an index's ", $camelcase_attr, " to its default value"),
                params(("indexUid", example = "movies", description = "Index Unique Identifier", nullable = false)),
                request_body = $type,
                responses(
                    (status = 200, description = "Task successfully enqueued", body = SummarizedTaskView, content_type = "application/json", example = json!(
                        {
                            "taskUid": 147,
                            "indexUid": "movies",
                            "status": "enqueued",
                            "type": "settingsUpdate",
                            "enqueuedAt": "2024-08-08T17:05:55.791772Z"
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
            pub async fn delete(
                index_scheduler: GuardedData<
                    ActionPolicy<{ actions::SETTINGS_UPDATE }>,
                    Data<IndexScheduler>,
                >,
                index_uid: web::Path<String>,
                req: HttpRequest,
                opt: web::Data<Opt>,
            ) -> Result<HttpResponse, ResponseError> {
                let index_uid = IndexUid::try_from(index_uid.into_inner())?;

                let new_settings = Settings { $attr: Setting::Reset.into(), ..Default::default() };

                let allow_index_creation =
                    index_scheduler.filters().allow_index_creation(&index_uid);

                let task = KindWithContent::SettingsUpdate {
                    index_uid: index_uid.to_string(),
                    new_settings: Box::new(new_settings),
                    is_deletion: true,
                    allow_index_creation,
                };
                let uid = get_task_id(&req, &opt)?;
                let dry_run = is_dry_run(&req, &opt)?;
                let task: SummarizedTaskView =
                    tokio::task::spawn_blocking(move || index_scheduler.register(task, uid, dry_run))
                        .await??
                        .into();

                debug!(returns = ?task, "Delete settings");
                Ok(HttpResponse::Accepted().json(task))
            }


            #[utoipa::path(
                $update_verb,
                path = concat!("{indexUid}/settings", $route),
                tag = "Settings",
                security(("Bearer" = ["settings.update", "settings.*", "*"])),
                operation_id = concat!(stringify!($update_verb), $camelcase_attr),
                summary = concat!("Update ", $camelcase_attr),
                description = concat!("Update an index's user defined ", $camelcase_attr),
                params(("indexUid", example = "movies", description = "Index Unique Identifier", nullable = false)),
                request_body = $type,
                responses(
                    (status = 200, description = "Task successfully enqueued", body = SummarizedTaskView, content_type = "application/json", example = json!(
                        {
                            "taskUid": 147,
                            "indexUid": "movies",
                            "status": "enqueued",
                            "type": "settingsUpdate",
                            "enqueuedAt": "2024-08-08T17:05:55.791772Z"
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
            pub async fn update(
                index_scheduler: GuardedData<
                    ActionPolicy<{ actions::SETTINGS_UPDATE }>,
                    Data<IndexScheduler>,
                >,
                index_uid: actix_web::web::Path<String>,
                body: deserr::actix_web::AwebJson<Option<$type>, $err_ty>,
                req: HttpRequest,
                opt: web::Data<Opt>,
                analytics: web::Data<Analytics>,
            ) -> std::result::Result<HttpResponse, ResponseError> {
                let index_uid = IndexUid::try_from(index_uid.into_inner())?;

                let body = body.into_inner();
                debug!(parameters = ?body, "Update settings");

                #[allow(clippy::redundant_closure_call)]
                analytics.publish(
                    $crate::routes::indexes::settings_analytics::$analytics::new(body.as_ref()).into_settings(),
                    &req,
                );

                let new_settings = Settings {
                    $attr: match body {
                        Some(inner_body) => Setting::Set(inner_body).into(),
                        None => Setting::Reset.into(),
                    },
                    ..Default::default()
                };

                let new_settings = $crate::routes::indexes::settings::validate_settings(
                    new_settings,
                    &index_scheduler,
                )?;

                let allow_index_creation =
                    index_scheduler.filters().allow_index_creation(&index_uid);

                let task = KindWithContent::SettingsUpdate {
                    index_uid: index_uid.to_string(),
                    new_settings: Box::new(new_settings),
                    is_deletion: false,
                    allow_index_creation,
                };
                let uid = get_task_id(&req, &opt)?;
                let dry_run = is_dry_run(&req, &opt)?;
                let task: SummarizedTaskView =
                    tokio::task::spawn_blocking(move || index_scheduler.register(task, uid, dry_run))
                        .await??
                        .into();

                debug!(returns = ?task, "Update settings");
                Ok(HttpResponse::Accepted().json(task))
            }


            #[utoipa::path(
                get,
                path = concat!("{indexUid}/settings", $route),
                tag = "Settings",
                summary = concat!("Get ", $camelcase_attr),
                description = concat!("Get an user defined ", $camelcase_attr),
                security(("Bearer" = ["settings.get", "settings.*", "*"])),
                operation_id = concat!("get", $camelcase_attr),
                params(("indexUid", example = "movies", description = "Index Unique Identifier", nullable = false)),
                responses(
                    (status = 200, description = concat!($camelcase_attr, " is returned"), body = $type, content_type = "application/json", example = json!(
                        <$type>::default()
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
            pub async fn get(
                index_scheduler: GuardedData<
                    ActionPolicy<{ actions::SETTINGS_GET }>,
                    Data<IndexScheduler>,
                >,
                index_uid: actix_web::web::Path<String>,
            ) -> std::result::Result<HttpResponse, ResponseError> {
                let index_uid = IndexUid::try_from(index_uid.into_inner())?;

                let index = index_scheduler.index(&index_uid)?;
                let rtxn = index.read_txn()?;
                let settings = settings(&index, &rtxn, meilisearch_types::settings::SecretPolicy::HideSecrets)?;

                debug!(returns = ?settings, "Update settings");

                Ok(HttpResponse::Ok().json(settings.$attr))
            }

            pub fn resources() -> Resource {
                Resource::new($route)
                    .route(web::get().to(SeqHandler(get)))
                    .route(web::$update_verb().to(SeqHandler(update)))
                    .route(web::delete().to(SeqHandler(delete)))
            }
        }
    };
}

make_setting_routes!(
    {
        route: "/filterable-attributes",
        update_verb: put,
        value_type: std::collections::BTreeSet<String>,
        err_type: meilisearch_types::deserr::DeserrJsonError<
            meilisearch_types::error::deserr_codes::InvalidSettingsFilterableAttributes,
        >,
        attr: filterable_attributes,
        camelcase_attr: "filterableAttributes",
        analytics: FilterableAttributesAnalytics
    },
    {
        route: "/sortable-attributes",
        update_verb: put,
        value_type: std::collections::BTreeSet<String>,
        err_type: meilisearch_types::deserr::DeserrJsonError<
            meilisearch_types::error::deserr_codes::InvalidSettingsSortableAttributes,
        >,
        attr: sortable_attributes,
        camelcase_attr: "sortableAttributes",
        analytics: SortableAttributesAnalytics
    },
    {
        route: "/displayed-attributes",
        update_verb: put,
        value_type: Vec<String>,
        err_type: meilisearch_types::deserr::DeserrJsonError<
            meilisearch_types::error::deserr_codes::InvalidSettingsDisplayedAttributes,
        >,
        attr: displayed_attributes,
        camelcase_attr: "displayedAttributes",
        analytics: DisplayedAttributesAnalytics
    },
    {
        route: "/typo-tolerance",
        update_verb: patch,
        value_type: meilisearch_types::settings::TypoSettings,
        err_type: meilisearch_types::deserr::DeserrJsonError<
            meilisearch_types::error::deserr_codes::InvalidSettingsTypoTolerance,
        >,
        attr: typo_tolerance,
        camelcase_attr: "typoTolerance",
        analytics: TypoToleranceAnalytics
    },
    {
        route: "/searchable-attributes",
        update_verb: put,
        value_type: Vec<String>,
        err_type: meilisearch_types::deserr::DeserrJsonError<
            meilisearch_types::error::deserr_codes::InvalidSettingsSearchableAttributes,
        >,
        attr: searchable_attributes,
        camelcase_attr: "searchableAttributes",
        analytics: SearchableAttributesAnalytics
    },
    {
        route: "/stop-words",
        update_verb: put,
        value_type: std::collections::BTreeSet<String>,
        err_type: meilisearch_types::deserr::DeserrJsonError<
            meilisearch_types::error::deserr_codes::InvalidSettingsStopWords,
        >,
        attr: stop_words,
        camelcase_attr: "stopWords",
        analytics: StopWordsAnalytics
    },
    {
        route: "/non-separator-tokens",
        update_verb: put,
        value_type: std::collections::BTreeSet<String>,
        err_type: meilisearch_types::deserr::DeserrJsonError<
            meilisearch_types::error::deserr_codes::InvalidSettingsNonSeparatorTokens,
        >,
        attr: non_separator_tokens,
        camelcase_attr: "nonSeparatorTokens",
        analytics: NonSeparatorTokensAnalytics
    },
    {
        route: "/separator-tokens",
        update_verb: put,
        value_type: std::collections::BTreeSet<String>,
        err_type: meilisearch_types::deserr::DeserrJsonError<
            meilisearch_types::error::deserr_codes::InvalidSettingsSeparatorTokens,
        >,
        attr: separator_tokens,
        camelcase_attr: "separatorTokens",
        analytics: SeparatorTokensAnalytics
    },
    {
        route: "/dictionary",
        update_verb: put,
        value_type: std::collections::BTreeSet<String>,
        err_type: meilisearch_types::deserr::DeserrJsonError<
            meilisearch_types::error::deserr_codes::InvalidSettingsDictionary,
        >,
        attr: dictionary,
        camelcase_attr: "dictionary",
        analytics: DictionaryAnalytics
    },
    {
        route: "/synonyms",
        update_verb: put,
        value_type: std::collections::BTreeMap<String, Vec<String>>,
        err_type: meilisearch_types::deserr::DeserrJsonError<
            meilisearch_types::error::deserr_codes::InvalidSettingsSynonyms,
        >,
        attr: synonyms,
        camelcase_attr: "synonyms",
        analytics: SynonymsAnalytics
    },
    {
        route: "/distinct-attribute",
        update_verb: put,
        value_type: String,
        err_type: meilisearch_types::deserr::DeserrJsonError<
            meilisearch_types::error::deserr_codes::InvalidSettingsDistinctAttribute,
        >,
        attr: distinct_attribute,
        camelcase_attr: "distinctAttribute",
        analytics: DistinctAttributeAnalytics
    },
    {
        route: "/proximity-precision",
        update_verb: put,
        value_type: meilisearch_types::settings::ProximityPrecisionView,
        err_type: meilisearch_types::deserr::DeserrJsonError<
            meilisearch_types::error::deserr_codes::InvalidSettingsProximityPrecision,
        >,
        attr: proximity_precision,
        camelcase_attr: "proximityPrecision",
        analytics: ProximityPrecisionAnalytics
    },
    {
        route: "/localized-attributes",
        update_verb: put,
        value_type: Vec<meilisearch_types::locales::LocalizedAttributesRuleView>,
        err_type: meilisearch_types::deserr::DeserrJsonError<
            meilisearch_types::error::deserr_codes::InvalidSettingsLocalizedAttributes,
        >,
        attr: localized_attributes,
        camelcase_attr: "localizedAttributes",
        analytics: LocalesAnalytics
    },
    {
        route: "/ranking-rules",
        update_verb: put,
        value_type: Vec<meilisearch_types::settings::RankingRuleView>,
        err_type: meilisearch_types::deserr::DeserrJsonError<
            meilisearch_types::error::deserr_codes::InvalidSettingsRankingRules,
        >,
        attr: ranking_rules,
        camelcase_attr: "rankingRules",
        analytics: RankingRulesAnalytics
    },
    {
        route: "/faceting",
        update_verb: patch,
        value_type: meilisearch_types::settings::FacetingSettings,
        err_type: meilisearch_types::deserr::DeserrJsonError<
            meilisearch_types::error::deserr_codes::InvalidSettingsFaceting,
        >,
        attr: faceting,
        camelcase_attr: "faceting",
        analytics: FacetingAnalytics
    },
    {
        route: "/pagination",
        update_verb: patch,
        value_type: meilisearch_types::settings::PaginationSettings,
        err_type: meilisearch_types::deserr::DeserrJsonError<
            meilisearch_types::error::deserr_codes::InvalidSettingsPagination,
        >,
        attr: pagination,
        camelcase_attr: "pagination",
        analytics: PaginationAnalytics
    },
    {
        route: "/embedders",
        update_verb: patch,
        value_type: std::collections::BTreeMap<String, SettingEmbeddingSettings>,
        err_type: meilisearch_types::deserr::DeserrJsonError<
            meilisearch_types::error::deserr_codes::InvalidSettingsEmbedders,
        >,
        attr: embedders,
        camelcase_attr: "embedders",
        analytics: EmbeddersAnalytics
    },
    {
        route: "/search-cutoff-ms",
        update_verb: put,
        value_type: u64,
        err_type: meilisearch_types::deserr::DeserrJsonError<
            meilisearch_types::error::deserr_codes::InvalidSettingsSearchCutoffMs,
        >,
        attr: search_cutoff_ms,
        camelcase_attr: "searchCutoffMs",
        analytics: SearchCutoffMsAnalytics
    },
    {
        route: "/facet-search",
        update_verb: put,
        value_type: bool,
        err_type: meilisearch_types::deserr::DeserrJsonError<
            meilisearch_types::error::deserr_codes::InvalidSettingsFacetSearch,
        >,
        attr: facet_search,
        camelcase_attr: "facetSearch",
        analytics: FacetSearchAnalytics
    },
    {
        route: "/prefix-search",
        update_verb: put,
        value_type: meilisearch_types::settings::PrefixSearchSettings,
        err_type: meilisearch_types::deserr::DeserrJsonError<
            meilisearch_types::error::deserr_codes::InvalidSettingsPrefixSearch,
        >,
        attr: prefix_search,
        camelcase_attr: "prefixSearch",
        analytics: PrefixSearchAnalytics
    },
);

#[utoipa::path(
    patch,
    path = "{indexUid}/settings",
    tag = "Settings",
    security(("Bearer" = ["settings.update", "settings.*", "*"])),
    params(("indexUid", example = "movies", description = "Index Unique Identifier", nullable = false)),
    request_body = Settings<Unchecked>,
    responses(
        (status = 200, description = "Task successfully enqueued", body = SummarizedTaskView, content_type = "application/json", example = json!(
            {
                "taskUid": 147,
                "indexUid": "movies",
                "status": "enqueued",
                "type": "settingsUpdate",
                "enqueuedAt": "2024-08-08T17:05:55.791772Z"
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
/// Update settings
///
/// Update the settings of an index.
/// Passing null to an index setting will reset it to its default value.
/// Updates in the settings route are partial. This means that any parameters not provided in the body will be left unchanged.
/// If the provided index does not exist, it will be created.
pub async fn update_all(
    index_scheduler: GuardedData<ActionPolicy<{ actions::SETTINGS_UPDATE }>, Data<IndexScheduler>>,
    index_uid: web::Path<String>,
    body: AwebJson<Settings<Unchecked>, DeserrJsonError>,
    req: HttpRequest,
    opt: web::Data<Opt>,
    analytics: web::Data<Analytics>,
) -> Result<HttpResponse, ResponseError> {
    let index_uid = IndexUid::try_from(index_uid.into_inner())?;

    let new_settings = body.into_inner();
    debug!(parameters = ?new_settings, "Update all settings");
    let new_settings = validate_settings(new_settings, &index_scheduler)?;

    analytics.publish(
        SettingsAnalytics {
            ranking_rules: RankingRulesAnalytics::new(new_settings.ranking_rules.as_ref().set()),
            searchable_attributes: SearchableAttributesAnalytics::new(
                new_settings.searchable_attributes.as_ref().set(),
            ),
            displayed_attributes: DisplayedAttributesAnalytics::new(
                new_settings.displayed_attributes.as_ref().set(),
            ),
            sortable_attributes: SortableAttributesAnalytics::new(
                new_settings.sortable_attributes.as_ref().set(),
            ),
            filterable_attributes: FilterableAttributesAnalytics::new(
                new_settings.filterable_attributes.as_ref().set(),
            ),
            distinct_attribute: DistinctAttributeAnalytics::new(
                new_settings.distinct_attribute.as_ref().set(),
            ),
            proximity_precision: ProximityPrecisionAnalytics::new(
                new_settings.proximity_precision.as_ref().set(),
            ),
            typo_tolerance: TypoToleranceAnalytics::new(new_settings.typo_tolerance.as_ref().set()),
            faceting: FacetingAnalytics::new(new_settings.faceting.as_ref().set()),
            pagination: PaginationAnalytics::new(new_settings.pagination.as_ref().set()),
            stop_words: StopWordsAnalytics::new(new_settings.stop_words.as_ref().set()),
            synonyms: SynonymsAnalytics::new(new_settings.synonyms.as_ref().set()),
            embedders: EmbeddersAnalytics::new(new_settings.embedders.as_ref().set()),
            search_cutoff_ms: SearchCutoffMsAnalytics::new(
                new_settings.search_cutoff_ms.as_ref().set(),
            ),
            locales: LocalesAnalytics::new(new_settings.localized_attributes.as_ref().set()),
            dictionary: DictionaryAnalytics::new(new_settings.dictionary.as_ref().set()),
            separator_tokens: SeparatorTokensAnalytics::new(
                new_settings.separator_tokens.as_ref().set(),
            ),
            non_separator_tokens: NonSeparatorTokensAnalytics::new(
                new_settings.non_separator_tokens.as_ref().set(),
            ),
            facet_search: FacetSearchAnalytics::new(new_settings.facet_search.as_ref().set()),
            prefix_search: PrefixSearchAnalytics::new(new_settings.prefix_search.as_ref().set()),
        },
        &req,
    );

    let allow_index_creation = index_scheduler.filters().allow_index_creation(&index_uid);
    let index_uid = IndexUid::try_from(index_uid.into_inner())?.into_inner();
    let task = KindWithContent::SettingsUpdate {
        index_uid,
        new_settings: Box::new(new_settings),
        is_deletion: false,
        allow_index_creation,
    };
    let uid = get_task_id(&req, &opt)?;
    let dry_run = is_dry_run(&req, &opt)?;
    let task: SummarizedTaskView =
        tokio::task::spawn_blocking(move || index_scheduler.register(task, uid, dry_run))
            .await??
            .into();

    debug!(returns = ?task, "Update all settings");
    Ok(HttpResponse::Accepted().json(task))
}

#[utoipa::path(
    get,
    path = "{indexUid}/settings",
    tag = "Settings",
    security(("Bearer" = ["settings.update", "settings.*", "*"])),
    params(("indexUid", example = "movies", description = "Index Unique Identifier", nullable = false)),
    responses(
        (status = 200, description = "Settings are returned", body = Settings<Unchecked>, content_type = "application/json", example = json!(
            Settings::<Unchecked>::default()
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
/// All settings
///
/// This route allows you to retrieve, configure, or reset all of an index's settings at once.
pub async fn get_all(
    index_scheduler: GuardedData<ActionPolicy<{ actions::SETTINGS_GET }>, Data<IndexScheduler>>,
    index_uid: web::Path<String>,
) -> Result<HttpResponse, ResponseError> {
    let index_uid = IndexUid::try_from(index_uid.into_inner())?;

    let index = index_scheduler.index(&index_uid)?;
    let rtxn = index.read_txn()?;
    let new_settings = settings(&index, &rtxn, SecretPolicy::HideSecrets)?;
    debug!(returns = ?new_settings, "Get all settings");
    Ok(HttpResponse::Ok().json(new_settings))
}

#[utoipa::path(
    delete,
    path = "{indexUid}/settings",
    tag = "Settings",
    security(("Bearer" = ["settings.update", "settings.*", "*"])),
    params(("indexUid", example = "movies", description = "Index Unique Identifier", nullable = false)),
    responses(
        (status = 200, description = "Task successfully enqueued", body = SummarizedTaskView, content_type = "application/json", example = json!(
            {
                "taskUid": 147,
                "indexUid": "movies",
                "status": "enqueued",
                "type": "settingsUpdate",
                "enqueuedAt": "2024-08-08T17:05:55.791772Z"
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
/// Reset settings
///
/// Reset all the settings of an index to their default value.
pub async fn delete_all(
    index_scheduler: GuardedData<ActionPolicy<{ actions::SETTINGS_UPDATE }>, Data<IndexScheduler>>,
    index_uid: web::Path<String>,
    req: HttpRequest,
    opt: web::Data<Opt>,
) -> Result<HttpResponse, ResponseError> {
    let index_uid = IndexUid::try_from(index_uid.into_inner())?;

    let new_settings = Settings::cleared().into_unchecked();

    let allow_index_creation = index_scheduler.filters().allow_index_creation(&index_uid);
    let index_uid = IndexUid::try_from(index_uid.into_inner())?.into_inner();
    let task = KindWithContent::SettingsUpdate {
        index_uid,
        new_settings: Box::new(new_settings),
        is_deletion: true,
        allow_index_creation,
    };
    let uid = get_task_id(&req, &opt)?;
    let dry_run = is_dry_run(&req, &opt)?;
    let task: SummarizedTaskView =
        tokio::task::spawn_blocking(move || index_scheduler.register(task, uid, dry_run))
            .await??
            .into();

    debug!(returns = ?task, "Delete all settings");
    Ok(HttpResponse::Accepted().json(task))
}

fn validate_settings(
    settings: Settings<Unchecked>,
    _index_scheduler: &IndexScheduler,
) -> Result<Settings<Unchecked>, ResponseError> {
    Ok(settings.validate()?)
}
