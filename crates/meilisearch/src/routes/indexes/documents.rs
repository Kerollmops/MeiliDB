use std::collections::HashSet;
use std::io::{ErrorKind, Seek as _};
use std::marker::PhantomData;

use actix_web::http::header::CONTENT_TYPE;
use actix_web::web::Data;
use actix_web::{web, HttpMessage, HttpRequest, HttpResponse};
use bstr::ByteSlice as _;
use deserr::actix_web::{AwebJson, AwebQueryParameter};
use deserr::Deserr;
use futures::StreamExt;
use index_scheduler::{IndexScheduler, RoFeatures, TaskId};
use meilisearch_types::deserr::query_params::Param;
use meilisearch_types::deserr::{DeserrJsonError, DeserrQueryParamError};
use meilisearch_types::document_formats::{read_csv, read_json, read_ndjson, PayloadType};
use meilisearch_types::error::deserr_codes::*;
use meilisearch_types::error::{Code, ResponseError};
use meilisearch_types::heed::RoTxn;
use meilisearch_types::index_uid::IndexUid;
use meilisearch_types::milli::update::IndexDocumentsMethod;
use meilisearch_types::milli::vector::parsed_vectors::ExplicitVectors;
use meilisearch_types::milli::DocumentId;
use meilisearch_types::star_or::OptionStarOrList;
use meilisearch_types::tasks::KindWithContent;
use meilisearch_types::{milli, Document, Index};
use mime::Mime;
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tempfile::tempfile;
use tokio::fs::File;
use tokio::io::{AsyncSeekExt, AsyncWriteExt, BufWriter};
use tracing::debug;
use utoipa::{IntoParams, OpenApi, ToSchema};

use crate::analytics::{Aggregate, AggregateMethod, Analytics};
use crate::error::MeilisearchHttpError;
use crate::error::PayloadError::ReceivePayload;
use crate::extractors::authentication::policies::*;
use crate::extractors::authentication::GuardedData;
use crate::extractors::payload::Payload;
use crate::extractors::sequential_extractor::SeqHandler;
use crate::routes::{
    get_task_id, is_dry_run, PaginationView, SummarizedTaskView, PAGINATION_DEFAULT_LIMIT,
};
use crate::search::{parse_filter, RetrieveVectors};
use crate::{aggregate_methods, Opt};

static ACCEPTED_CONTENT_TYPE: Lazy<Vec<String>> = Lazy::new(|| {
    vec!["application/json".to_string(), "application/x-ndjson".to_string(), "text/csv".to_string()]
});

/// Extracts the mime type from the content type and return
/// a meilisearch error if anything bad happen.
fn extract_mime_type(req: &HttpRequest) -> Result<Option<Mime>, MeilisearchHttpError> {
    match req.mime_type() {
        Ok(Some(mime)) => Ok(Some(mime)),
        Ok(None) => Ok(None),
        Err(_) => match req.headers().get(CONTENT_TYPE) {
            Some(content_type) => Err(MeilisearchHttpError::InvalidContentType(
                content_type.as_bytes().as_bstr().to_string(),
                ACCEPTED_CONTENT_TYPE.clone(),
            )),
            None => Err(MeilisearchHttpError::MissingContentType(ACCEPTED_CONTENT_TYPE.clone())),
        },
    }
}

#[derive(Deserialize)]
pub struct DocumentParam {
    index_uid: String,
    document_id: String,
}

#[derive(OpenApi)]
#[openapi(
    paths(get_document, get_documents, delete_document, replace_documents, update_documents, clear_all_documents, delete_documents_batch, delete_documents_by_filter, edit_documents_by_function, documents_by_query_post),
    tags(
        (
            name = "Documents",
            description = "Documents are objects composed of fields that can store any type of data. Each field contains an attribute and its associated value. Documents are stored inside [indexes](https://www.meilisearch.com/docs/learn/getting_started/indexes).",
            external_docs(url = "https://www.meilisearch.com/docs/learn/getting_started/documents"),
        ),
    ),
)]
pub struct DocumentsApi;

pub fn configure(cfg: &mut web::ServiceConfig) {
    cfg.service(
        web::resource("")
            .route(web::get().to(SeqHandler(get_documents)))
            .route(web::post().to(SeqHandler(replace_documents)))
            .route(web::put().to(SeqHandler(update_documents)))
            .route(web::delete().to(SeqHandler(clear_all_documents))),
    )
    // these routes need to be before the /documents/{document_id} to match properly
    .service(
        web::resource("/delete-batch").route(web::post().to(SeqHandler(delete_documents_batch))),
    )
    .service(web::resource("/delete").route(web::post().to(SeqHandler(delete_documents_by_filter))))
    .service(web::resource("/edit").route(web::post().to(SeqHandler(edit_documents_by_function))))
    .service(web::resource("/fetch").route(web::post().to(SeqHandler(documents_by_query_post))))
    .service(
        web::resource("/{document_id}")
            .route(web::get().to(SeqHandler(get_document)))
            .route(web::delete().to(SeqHandler(delete_document))),
    );
}

#[derive(Debug, Deserr, IntoParams, ToSchema)]
#[deserr(error = DeserrQueryParamError, rename_all = camelCase, deny_unknown_fields)]
#[into_params(rename_all = "camelCase", parameter_in = Query)]
#[schema(rename_all = "camelCase")]
pub struct GetDocument {
    #[deserr(default, error = DeserrQueryParamError<InvalidDocumentFields>)]
    #[param(value_type = Option<Vec<String>>)]
    #[schema(value_type = Option<Vec<String>>)]
    fields: OptionStarOrList<String>,
    #[deserr(default, error = DeserrQueryParamError<InvalidDocumentRetrieveVectors>)]
    #[param(value_type = Option<bool>)]
    #[schema(value_type = Option<bool>)]
    retrieve_vectors: Param<bool>,
}

aggregate_methods!(
    DocumentsGET => "Documents Fetched GET",
    DocumentsPOST => "Documents Fetched POST",
);

#[derive(Serialize)]
pub struct DocumentsFetchAggregator<Method: AggregateMethod> {
    // a call on ../documents/:doc_id
    per_document_id: bool,
    // if a filter was used
    per_filter: bool,

    #[serde(rename = "vector.retrieve_vectors")]
    retrieve_vectors: bool,

    // pagination
    #[serde(rename = "pagination.max_limit")]
    max_limit: usize,
    #[serde(rename = "pagination.max_offset")]
    max_offset: usize,

    marker: std::marker::PhantomData<Method>,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum DocumentFetchKind {
    PerDocumentId { retrieve_vectors: bool },
    Normal { with_filter: bool, limit: usize, offset: usize, retrieve_vectors: bool },
}

impl<Method: AggregateMethod> DocumentsFetchAggregator<Method> {
    pub fn from_query(query: &DocumentFetchKind) -> Self {
        let (limit, offset, retrieve_vectors) = match query {
            DocumentFetchKind::PerDocumentId { retrieve_vectors } => (1, 0, *retrieve_vectors),
            DocumentFetchKind::Normal { limit, offset, retrieve_vectors, .. } => {
                (*limit, *offset, *retrieve_vectors)
            }
        };

        Self {
            per_document_id: matches!(query, DocumentFetchKind::PerDocumentId { .. }),
            per_filter: matches!(query, DocumentFetchKind::Normal { with_filter, .. } if *with_filter),
            max_limit: limit,
            max_offset: offset,
            retrieve_vectors,

            marker: PhantomData,
        }
    }
}

impl<Method: AggregateMethod> Aggregate for DocumentsFetchAggregator<Method> {
    fn event_name(&self) -> &'static str {
        Method::event_name()
    }

    fn aggregate(self: Box<Self>, new: Box<Self>) -> Box<Self> {
        Box::new(Self {
            per_document_id: self.per_document_id | new.per_document_id,
            per_filter: self.per_filter | new.per_filter,
            retrieve_vectors: self.retrieve_vectors | new.retrieve_vectors,
            max_limit: self.max_limit.max(new.max_limit),
            max_offset: self.max_offset.max(new.max_offset),
            marker: PhantomData,
        })
    }

    fn into_event(self: Box<Self>) -> serde_json::Value {
        serde_json::to_value(*self).unwrap_or_default()
    }
}

/// Get one document
///
/// Get one document from its primary key.
#[utoipa::path(
    get,
    path = "{indexUid}/documents/{documentId}",
    tag = "Documents",
    security(("Bearer" = ["documents.get", "documents.*", "*"])),
    params(
        ("indexUid" = String, Path, example = "movies", description = "Index Unique Identifier", nullable = false),
        ("documentId" = String, Path, example = "85087", description = "The document identifier", nullable = false),
        GetDocument,
   ),
    responses(
        (status = 200, description = "The document is returned", body = serde_json::Value, content_type = "application/json", example = json!(
            {
                "id": 25684,
                "title": "American Ninja 5",
                "poster": "https://image.tmdb.org/t/p/w1280/iuAQVI4mvjI83wnirpD8GVNRVuY.jpg",
                "overview": "When a scientists daughter is kidnapped, American Ninja, attempts to find her, but this time he teams up with a youngster he has trained in the ways of the ninja.",
                "release_date": 725846400
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
        (status = 404, description = "Document not found", body = ResponseError, content_type = "application/json", example = json!(
            {
              "message": "Document `a` not found.",
              "code": "document_not_found",
              "type": "invalid_request",
              "link": "https://docs.meilisearch.com/errors#document_not_found"
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
pub async fn get_document(
    index_scheduler: GuardedData<ActionPolicy<{ actions::DOCUMENTS_GET }>, Data<IndexScheduler>>,
    document_param: web::Path<DocumentParam>,
    params: AwebQueryParameter<GetDocument, DeserrQueryParamError>,
    req: HttpRequest,
    analytics: web::Data<Analytics>,
) -> Result<HttpResponse, ResponseError> {
    let DocumentParam { index_uid, document_id } = document_param.into_inner();
    debug!(parameters = ?params, "Get document");
    let index_uid = IndexUid::try_from(index_uid)?;

    let GetDocument { fields, retrieve_vectors: param_retrieve_vectors } = params.into_inner();
    let attributes_to_retrieve = fields.merge_star_and_none();

    let retrieve_vectors = RetrieveVectors::new(param_retrieve_vectors.0);

    analytics.publish(
        DocumentsFetchAggregator::<DocumentsGET> {
            retrieve_vectors: param_retrieve_vectors.0,
            per_document_id: true,
            per_filter: false,
            max_limit: 0,
            max_offset: 0,
            marker: PhantomData,
        },
        &req,
    );

    let index = index_scheduler.index(&index_uid)?;
    let document =
        retrieve_document(&index, &document_id, attributes_to_retrieve, retrieve_vectors)?;
    debug!(returns = ?document, "Get document");
    Ok(HttpResponse::Ok().json(document))
}

#[derive(Serialize)]
pub struct DocumentsDeletionAggregator {
    per_document_id: bool,
    clear_all: bool,
    per_batch: bool,
    per_filter: bool,
}

impl Aggregate for DocumentsDeletionAggregator {
    fn event_name(&self) -> &'static str {
        "Documents Deleted"
    }

    fn aggregate(self: Box<Self>, new: Box<Self>) -> Box<Self> {
        Box::new(Self {
            per_document_id: self.per_document_id | new.per_document_id,
            clear_all: self.clear_all | new.clear_all,
            per_batch: self.per_batch | new.per_batch,
            per_filter: self.per_filter | new.per_filter,
        })
    }

    fn into_event(self: Box<Self>) -> serde_json::Value {
        serde_json::to_value(*self).unwrap_or_default()
    }
}

/// Delete a document
///
/// Delete a single document by id.
#[utoipa::path(
    delete,
    path = "{indexUid}/documents/{documentId}",
    tag = "Documents",
    security(("Bearer" = ["documents.delete", "documents.*", "*"])),
    params(
        ("indexUid" = String, Path, example = "movies", description = "Index Unique Identifier", nullable = false),
        ("documentId" = String, Path, example = "853", description = "Document Identifier", nullable = false),
    ),
    responses(
        (status = 200, description = "Task successfully enqueued", body = SummarizedTaskView, content_type = "application/json", example = json!(
            {
                "taskUid": 147,
                "indexUid": null,
                "status": "enqueued",
                "type": "documentAdditionOrUpdate",
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
pub async fn delete_document(
    index_scheduler: GuardedData<ActionPolicy<{ actions::DOCUMENTS_DELETE }>, Data<IndexScheduler>>,
    path: web::Path<DocumentParam>,
    req: HttpRequest,
    opt: web::Data<Opt>,
    analytics: web::Data<Analytics>,
) -> Result<HttpResponse, ResponseError> {
    let DocumentParam { index_uid, document_id } = path.into_inner();
    let index_uid = IndexUid::try_from(index_uid)?;

    analytics.publish(
        DocumentsDeletionAggregator {
            per_document_id: true,
            clear_all: false,
            per_batch: false,
            per_filter: false,
        },
        &req,
    );

    let task = KindWithContent::DocumentDeletion {
        index_uid: index_uid.to_string(),
        documents_ids: vec![document_id],
    };
    let uid = get_task_id(&req, &opt)?;
    let dry_run = is_dry_run(&req, &opt)?;
    let task: SummarizedTaskView =
        tokio::task::spawn_blocking(move || index_scheduler.register(task, uid, dry_run))
            .await??
            .into();
    debug!("returns: {:?}", task);
    Ok(HttpResponse::Accepted().json(task))
}

#[derive(Debug, Deserr, IntoParams)]
#[deserr(error = DeserrQueryParamError, rename_all = camelCase, deny_unknown_fields)]
#[into_params(rename_all = "camelCase", parameter_in = Query)]
pub struct BrowseQueryGet {
    #[param(default, value_type = Option<usize>)]
    #[deserr(default, error = DeserrQueryParamError<InvalidDocumentOffset>)]
    offset: Param<usize>,
    #[param(default, value_type = Option<usize>)]
    #[deserr(default = Param(PAGINATION_DEFAULT_LIMIT), error = DeserrQueryParamError<InvalidDocumentLimit>)]
    limit: Param<usize>,
    #[param(default, value_type = Option<Vec<String>>)]
    #[deserr(default, error = DeserrQueryParamError<InvalidDocumentFields>)]
    fields: OptionStarOrList<String>,
    #[param(default, value_type = Option<bool>)]
    #[deserr(default, error = DeserrQueryParamError<InvalidDocumentRetrieveVectors>)]
    retrieve_vectors: Param<bool>,
    #[param(default, value_type = Option<String>, example = "popularity > 1000")]
    #[deserr(default, error = DeserrQueryParamError<InvalidDocumentFilter>)]
    filter: Option<String>,
}

#[derive(Debug, Deserr, ToSchema)]
#[deserr(error = DeserrJsonError, rename_all = camelCase, deny_unknown_fields)]
#[schema(rename_all = "camelCase")]
pub struct BrowseQuery {
    #[schema(default, example = 150)]
    #[deserr(default, error = DeserrJsonError<InvalidDocumentOffset>)]
    offset: usize,
    #[schema(default = 20, example = 1)]
    #[deserr(default = PAGINATION_DEFAULT_LIMIT, error = DeserrJsonError<InvalidDocumentLimit>)]
    limit: usize,
    #[schema(example = json!(["title, description"]))]
    #[deserr(default, error = DeserrJsonError<InvalidDocumentFields>)]
    fields: Option<Vec<String>>,
    #[schema(default, example = true)]
    #[deserr(default, error = DeserrJsonError<InvalidDocumentRetrieveVectors>)]
    retrieve_vectors: bool,
    #[schema(default, value_type = Option<Value>, example = "popularity > 1000")]
    #[deserr(default, error = DeserrJsonError<InvalidDocumentFilter>)]
    filter: Option<Value>,
}

/// Get documents with POST
///
/// Get a set of documents.
#[utoipa::path(
    post,
    path = "{indexUid}/documents/fetch",
    tag = "Documents",
    security(("Bearer" = ["documents.delete", "documents.*", "*"])),
    params(("indexUid", example = "movies", description = "Index Unique Identifier", nullable = false)),
    request_body = BrowseQuery,
    responses(
        (status = 200, description = "Task successfully enqueued", body = PaginationView<serde_json::Value>, content_type = "application/json", example = json!(
            {
                "results":[
                    {
                        "title":"The Travels of Ibn Battuta",
                        "genres":[
                            "Travel",
                            "Adventure"
                        ],
                        "language":"English",
                        "rating":4.5
                    },
                    {
                        "title":"Pride and Prejudice",
                        "genres":[
                            "Classics",
                            "Fiction",
                            "Romance",
                            "Literature"
                        ],
                        "language":"English",
                        "rating":4
                    },
                ],
                "offset":0,
                "limit":2,
                "total":5
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
pub async fn documents_by_query_post(
    index_scheduler: GuardedData<ActionPolicy<{ actions::DOCUMENTS_GET }>, Data<IndexScheduler>>,
    index_uid: web::Path<String>,
    body: AwebJson<BrowseQuery, DeserrJsonError>,
    req: HttpRequest,
    analytics: web::Data<Analytics>,
) -> Result<HttpResponse, ResponseError> {
    let body = body.into_inner();
    debug!(parameters = ?body, "Get documents POST");

    analytics.publish(
        DocumentsFetchAggregator::<DocumentsPOST> {
            per_filter: body.filter.is_some(),
            retrieve_vectors: body.retrieve_vectors,
            max_limit: body.limit,
            max_offset: body.offset,
            per_document_id: false,
            marker: PhantomData,
        },
        &req,
    );

    documents_by_query(&index_scheduler, index_uid, body)
}

/// Get documents
///
/// Get documents by batches.
#[utoipa::path(
    get,
    path = "{indexUid}/documents",
    tag = "Documents",
    security(("Bearer" = ["documents.get", "documents.*", "*"])),
    params(
        ("indexUid", example = "movies", description = "Index Unique Identifier", nullable = false),
        BrowseQueryGet
    ),
    responses(
        (status = 200, description = "The documents are returned", body = PaginationView<serde_json::Value>, content_type = "application/json", example = json!(
            {
                "results": [
                    {
                        "id": 25684,
                        "title": "American Ninja 5",
                        "poster": "https://image.tmdb.org/t/p/w1280/iuAQVI4mvjI83wnirpD8GVNRVuY.jpg",
                        "overview": "When a scientists daughter is kidnapped, American Ninja, attempts to find her, but this time he teams up with a youngster he has trained in the ways of the ninja.",
                        "release_date": 725846400
                    },
                    {
                        "id": 45881,
                        "title": "The Bridge of San Luis Rey",
                        "poster": "https://image.tmdb.org/t/p/w500/4X7quIcdkc24Cveg5XdpfRqxtYA.jpg",
                        "overview": "The Bridge of San Luis Rey is American author Thornton Wilder's second novel, first published in 1927 to worldwide acclaim. It tells the story of several interrelated people who die in the collapse of an Inca rope-fiber suspension bridge in Peru, and the events that lead up to their being on the bridge.[ A friar who has witnessed the tragic accident then goes about inquiring into the lives of the victims, seeking some sort of cosmic answer to the question of why each had to die. The novel won the Pulitzer Prize in 1928.",
                        "release_date": 1072915200
                    }
                ],
                "limit": 20,
                "offset": 0,
                "total": 2
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
pub async fn get_documents(
    index_scheduler: GuardedData<ActionPolicy<{ actions::DOCUMENTS_GET }>, Data<IndexScheduler>>,
    index_uid: web::Path<String>,
    params: AwebQueryParameter<BrowseQueryGet, DeserrQueryParamError>,
    req: HttpRequest,
    analytics: web::Data<Analytics>,
) -> Result<HttpResponse, ResponseError> {
    debug!(parameters = ?params, "Get documents GET");

    let BrowseQueryGet { limit, offset, fields, retrieve_vectors, filter } = params.into_inner();

    let filter = match filter {
        Some(f) => match serde_json::from_str(&f) {
            Ok(v) => Some(v),
            _ => Some(Value::String(f)),
        },
        None => None,
    };

    let query = BrowseQuery {
        offset: offset.0,
        limit: limit.0,
        fields: fields.merge_star_and_none(),
        retrieve_vectors: retrieve_vectors.0,
        filter,
    };

    analytics.publish(
        DocumentsFetchAggregator::<DocumentsGET> {
            per_filter: query.filter.is_some(),
            retrieve_vectors: query.retrieve_vectors,
            max_limit: query.limit,
            max_offset: query.offset,
            per_document_id: false,
            marker: PhantomData,
        },
        &req,
    );

    documents_by_query(&index_scheduler, index_uid, query)
}

fn documents_by_query(
    index_scheduler: &IndexScheduler,
    index_uid: web::Path<String>,
    query: BrowseQuery,
) -> Result<HttpResponse, ResponseError> {
    let index_uid = IndexUid::try_from(index_uid.into_inner())?;
    let BrowseQuery { offset, limit, fields, retrieve_vectors, filter } = query;

    let retrieve_vectors = RetrieveVectors::new(retrieve_vectors);

    let index = index_scheduler.index(&index_uid)?;
    let (total, documents) = retrieve_documents(
        &index,
        offset,
        limit,
        filter,
        fields,
        retrieve_vectors,
        index_scheduler.features(),
    )?;

    let ret = PaginationView::new(offset, limit, total as usize, documents);

    debug!(returns = ?ret, "Get documents");
    Ok(HttpResponse::Ok().json(ret))
}

#[derive(Deserialize, Debug, Deserr, IntoParams)]
#[deserr(error = DeserrQueryParamError, rename_all = camelCase, deny_unknown_fields)]
#[into_params(parameter_in = Query, rename_all = "camelCase")]
pub struct UpdateDocumentsQuery {
    /// The primary key of the documents. primaryKey is optional. If you want to set the primary key of your index through this route,
    /// it only has to be done the first time you add documents to the index. After which it will be ignored if given.
    #[param(example = "id")]
    #[deserr(default, error = DeserrQueryParamError<InvalidIndexPrimaryKey>)]
    pub primary_key: Option<String>,
    /// Customize the csv delimiter when importing CSV documents.
    #[param(value_type = char, default = ",", example = ";")]
    #[deserr(default, try_from(char) = from_char_csv_delimiter -> DeserrQueryParamError<InvalidDocumentCsvDelimiter>, error = DeserrQueryParamError<InvalidDocumentCsvDelimiter>)]
    pub csv_delimiter: Option<u8>,
}

fn from_char_csv_delimiter(
    c: char,
) -> Result<Option<u8>, DeserrQueryParamError<InvalidDocumentCsvDelimiter>> {
    if c.is_ascii() {
        Ok(Some(c as u8))
    } else {
        Err(DeserrQueryParamError::new(
            format!("csv delimiter must be an ascii character. Found: `{}`", c),
            Code::InvalidDocumentCsvDelimiter,
        ))
    }
}

aggregate_methods!(
    Replaced => "Documents Added",
    Updated => "Documents Updated",
);

#[derive(Serialize)]
pub struct DocumentsAggregator<T: AggregateMethod> {
    payload_types: HashSet<String>,
    primary_key: HashSet<String>,
    index_creation: bool,
    #[serde(skip)]
    method: PhantomData<T>,
}

impl<Method: AggregateMethod> Aggregate for DocumentsAggregator<Method> {
    fn event_name(&self) -> &'static str {
        Method::event_name()
    }

    fn aggregate(self: Box<Self>, new: Box<Self>) -> Box<Self> {
        Box::new(Self {
            payload_types: self.payload_types.union(&new.payload_types).cloned().collect(),
            primary_key: self.primary_key.union(&new.primary_key).cloned().collect(),
            index_creation: self.index_creation | new.index_creation,
            method: PhantomData,
        })
    }

    fn into_event(self: Box<Self>) -> serde_json::Value {
        serde_json::to_value(self).unwrap_or_default()
    }
}

/// Add or replace documents
///
/// Add a list of documents or replace them if they already exist.
///
/// If you send an already existing document (same id) the whole existing document will be overwritten by the new document. Fields previously in the document not present in the new document are removed.
///
/// For a partial update of the document see Add or update documents route.
/// > info
/// > If the provided index does not exist, it will be created.
/// > info
/// > Use the reserved `_geo` object to add geo coordinates to a document. `_geo` is an object made of `lat` and `lng` field.
/// >
/// > When the vectorStore feature is enabled you can use the reserved `_vectors` field in your documents.
/// > It can accept an array of floats, multiple arrays of floats in an outer array or an object.
/// > This object accepts keys corresponding to the different embedders defined your index settings.
#[utoipa::path(
    post,
    path = "{indexUid}/documents",
    tag = "Documents",
    security(("Bearer" = ["documents.add", "documents.*", "*"])),
    params(
        ("indexUid", example = "movies", description = "Index Unique Identifier", nullable = false),
        // Here we can use the post version of the browse query since it contains the exact same parameter
        UpdateDocumentsQuery,
    ),
    request_body = serde_json::Value,
    responses(
        (status = 200, description = "Task successfully enqueued", body = SummarizedTaskView, content_type = "application/json", example = json!(
            {
                "taskUid": 147,
                "indexUid": null,
                "status": "enqueued",
                "type": "documentAdditionOrUpdate",
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
pub async fn replace_documents(
    index_scheduler: GuardedData<ActionPolicy<{ actions::DOCUMENTS_ADD }>, Data<IndexScheduler>>,
    index_uid: web::Path<String>,
    params: AwebQueryParameter<UpdateDocumentsQuery, DeserrQueryParamError>,
    body: Payload,
    req: HttpRequest,
    opt: web::Data<Opt>,
    analytics: web::Data<Analytics>,
) -> Result<HttpResponse, ResponseError> {
    let index_uid = IndexUid::try_from(index_uid.into_inner())?;

    debug!(parameters = ?params, "Replace documents");
    let params = params.into_inner();

    let mut content_types = HashSet::new();
    let content_type = req
        .headers()
        .get(CONTENT_TYPE)
        .and_then(|s| s.to_str().ok())
        .unwrap_or("unknown")
        .to_string();
    content_types.insert(content_type);
    let mut primary_keys = HashSet::new();
    if let Some(primary_key) = params.primary_key.clone() {
        primary_keys.insert(primary_key);
    }
    analytics.publish(
        DocumentsAggregator::<Replaced> {
            payload_types: content_types,
            primary_key: primary_keys,
            index_creation: index_scheduler.index_exists(&index_uid).map_or(true, |x| !x),
            method: PhantomData,
        },
        &req,
    );

    let allow_index_creation = index_scheduler.filters().allow_index_creation(&index_uid);
    let uid = get_task_id(&req, &opt)?;
    let dry_run = is_dry_run(&req, &opt)?;
    let task = document_addition(
        extract_mime_type(&req)?,
        index_scheduler,
        index_uid,
        params.primary_key,
        params.csv_delimiter,
        body,
        IndexDocumentsMethod::ReplaceDocuments,
        uid,
        dry_run,
        allow_index_creation,
    )
    .await?;
    debug!(returns = ?task, "Replace documents");

    Ok(HttpResponse::Accepted().json(task))
}

/// Add or update documents
///
/// Add a list of documents or update them if they already exist.
/// If you send an already existing document (same id) the old document will be only partially updated according to the fields of the new document. Thus, any fields not present in the new document are kept and remained unchanged.
/// To completely overwrite a document, see Add or replace documents route.
/// > info
/// > If the provided index does not exist, it will be created.
/// > info
/// > Use the reserved `_geo` object to add geo coordinates to a document. `_geo` is an object made of `lat` and `lng` field.
/// >
/// > When the vectorStore feature is enabled you can use the reserved `_vectors` field in your documents.
/// > It can accept an array of floats, multiple arrays of floats in an outer array or an object.
/// > This object accepts keys corresponding to the different embedders defined your index settings.
#[utoipa::path(
    put,
    path = "{indexUid}/documents",
    tag = "Documents",
    security(("Bearer" = ["documents.add", "documents.*", "*"])),
    params(
        ("indexUid", example = "movies", description = "Index Unique Identifier", nullable = false),
        // Here we can use the post version of the browse query since it contains the exact same parameter
        UpdateDocumentsQuery,
    ),
    request_body = serde_json::Value,
    responses(
        (status = 200, description = "Task successfully enqueued", body = SummarizedTaskView, content_type = "application/json", example = json!(
            {
                "taskUid": 147,
                "indexUid": null,
                "status": "enqueued",
                "type": "documentAdditionOrUpdate",
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
pub async fn update_documents(
    index_scheduler: GuardedData<ActionPolicy<{ actions::DOCUMENTS_ADD }>, Data<IndexScheduler>>,
    index_uid: web::Path<String>,
    params: AwebQueryParameter<UpdateDocumentsQuery, DeserrQueryParamError>,
    body: Payload,
    req: HttpRequest,
    opt: web::Data<Opt>,
    analytics: web::Data<Analytics>,
) -> Result<HttpResponse, ResponseError> {
    let index_uid = IndexUid::try_from(index_uid.into_inner())?;

    let params = params.into_inner();
    debug!(parameters = ?params, "Update documents");

    let mut content_types = HashSet::new();
    let content_type = req
        .headers()
        .get(CONTENT_TYPE)
        .and_then(|s| s.to_str().ok())
        .unwrap_or("unknown")
        .to_string();
    content_types.insert(content_type);
    let mut primary_keys = HashSet::new();
    if let Some(primary_key) = params.primary_key.clone() {
        primary_keys.insert(primary_key);
    }
    analytics.publish(
        DocumentsAggregator::<Updated> {
            payload_types: content_types,
            primary_key: primary_keys,
            index_creation: index_scheduler.index_exists(&index_uid).map_or(true, |x| !x),
            method: PhantomData,
        },
        &req,
    );

    let allow_index_creation = index_scheduler.filters().allow_index_creation(&index_uid);
    let uid = get_task_id(&req, &opt)?;
    let dry_run = is_dry_run(&req, &opt)?;
    let task = document_addition(
        extract_mime_type(&req)?,
        index_scheduler,
        index_uid,
        params.primary_key,
        params.csv_delimiter,
        body,
        IndexDocumentsMethod::UpdateDocuments,
        uid,
        dry_run,
        allow_index_creation,
    )
    .await?;
    debug!(returns = ?task, "Update documents");

    Ok(HttpResponse::Accepted().json(task))
}

#[allow(clippy::too_many_arguments)]
async fn document_addition(
    mime_type: Option<Mime>,
    index_scheduler: GuardedData<ActionPolicy<{ actions::DOCUMENTS_ADD }>, Data<IndexScheduler>>,
    index_uid: IndexUid,
    primary_key: Option<String>,
    csv_delimiter: Option<u8>,
    body: Payload,
    method: IndexDocumentsMethod,
    task_id: Option<TaskId>,
    dry_run: bool,
    allow_index_creation: bool,
) -> Result<SummarizedTaskView, MeilisearchHttpError> {
    let format = match (
        mime_type.as_ref().map(|m| (m.type_().as_str(), m.subtype().as_str())),
        csv_delimiter,
    ) {
        (Some(("application", "json")), None) => PayloadType::Json,
        (Some(("application", "x-ndjson")), None) => PayloadType::Ndjson,
        (Some(("text", "csv")), None) => PayloadType::Csv { delimiter: b',' },
        (Some(("text", "csv")), Some(delimiter)) => PayloadType::Csv { delimiter },

        (Some(("application", "json")), Some(_)) => {
            return Err(MeilisearchHttpError::CsvDelimiterWithWrongContentType(String::from(
                "application/json",
            )))
        }
        (Some(("application", "x-ndjson")), Some(_)) => {
            return Err(MeilisearchHttpError::CsvDelimiterWithWrongContentType(String::from(
                "application/x-ndjson",
            )))
        }
        (Some((type_, subtype)), _) => {
            return Err(MeilisearchHttpError::InvalidContentType(
                format!("{}/{}", type_, subtype),
                ACCEPTED_CONTENT_TYPE.clone(),
            ))
        }
        (None, _) => {
            return Err(MeilisearchHttpError::MissingContentType(ACCEPTED_CONTENT_TYPE.clone()))
        }
    };

    let (uuid, mut update_file) = index_scheduler.queue.create_update_file(dry_run)?;
    let documents_count = match format {
        PayloadType::Ndjson => {
            let (path, file) = update_file.into_parts();
            let file = match file {
                Some(file) => {
                    let (file, path) = file.into_parts();
                    let mut file = copy_body_to_file(file, body, format).await?;
                    file.rewind().map_err(|e| {
                        index_scheduler::Error::FileStore(file_store::Error::IoError(e))
                    })?;
                    Some(tempfile::NamedTempFile::from_parts(file, path))
                }
                None => None,
            };

            let documents_count = tokio::task::spawn_blocking(move || {
                let documents_count = file.as_ref().map_or(Ok(0), |ntf| {
                    read_ndjson(ntf.as_file()).map_err(MeilisearchHttpError::DocumentFormat)
                })?;

                let update_file = file_store::File::from_parts(path, file);
                update_file.persist()?;

                Ok(documents_count)
            })
            .await?;

            Ok(documents_count)
        }
        PayloadType::Json | PayloadType::Csv { delimiter: _ } => {
            let temp_file = match tempfile() {
                Ok(file) => file,
                Err(e) => return Err(MeilisearchHttpError::Payload(ReceivePayload(Box::new(e)))),
            };

            let read_file = copy_body_to_file(temp_file, body, format).await?;
            tokio::task::spawn_blocking(move || {
                let documents_count = match format {
                    PayloadType::Json => read_json(&read_file, &mut update_file)?,
                    PayloadType::Csv { delimiter } => {
                        read_csv(&read_file, &mut update_file, delimiter)?
                    }
                    PayloadType::Ndjson => {
                        unreachable!("We already wrote the user content into the update file")
                    }
                };
                // we NEED to persist the file here because we moved the `udpate_file` in another task.
                update_file.persist()?;
                Ok(documents_count)
            })
            .await
        }
    };

    let documents_count = match documents_count {
        Ok(Ok(documents_count)) => documents_count,
        // in this case the file has not possibly be persisted.
        Ok(Err(e)) => return Err(e),
        Err(e) => {
            // Here the file MAY have been persisted or not.
            // We don't know thus we ignore the file not found error.
            match index_scheduler.queue.delete_update_file(uuid) {
                Ok(()) => (),
                Err(index_scheduler::Error::FileStore(file_store::Error::IoError(e)))
                    if e.kind() == ErrorKind::NotFound => {}
                Err(e) => {
                    tracing::warn!(
                        index_uuid = %uuid,
                        "Unknown error happened while deleting a malformed update file: {e}"
                    );
                }
            }
            // We still want to return the original error to the end user.
            return Err(e.into());
        }
    };

    let task = KindWithContent::DocumentAdditionOrUpdate {
        method,
        content_file: uuid,
        documents_count,
        primary_key,
        allow_index_creation,
        index_uid: index_uid.to_string(),
    };

    let scheduler = index_scheduler.clone();
    let task = match tokio::task::spawn_blocking(move || scheduler.register(task, task_id, dry_run))
        .await?
    {
        Ok(task) => task,
        Err(e) => {
            index_scheduler.queue.delete_update_file(uuid)?;
            return Err(e.into());
        }
    };

    Ok(task.into())
}

async fn copy_body_to_file(
    output: std::fs::File,
    mut body: Payload,
    format: PayloadType,
) -> Result<std::fs::File, MeilisearchHttpError> {
    let async_file = File::from_std(output);
    let mut buffer = BufWriter::new(async_file);
    let mut buffer_write_size: usize = 0;
    while let Some(result) = body.next().await {
        let byte = result?;

        if byte.is_empty() && buffer_write_size == 0 {
            return Err(MeilisearchHttpError::MissingPayload(format));
        }

        match buffer.write_all(&byte).await {
            Ok(()) => buffer_write_size += 1,
            Err(e) => return Err(MeilisearchHttpError::Payload(ReceivePayload(Box::new(e)))),
        }
    }
    if let Err(e) = buffer.flush().await {
        return Err(MeilisearchHttpError::Payload(ReceivePayload(Box::new(e))));
    }
    if buffer_write_size == 0 {
        return Err(MeilisearchHttpError::MissingPayload(format));
    }
    if let Err(e) = buffer.seek(std::io::SeekFrom::Start(0)).await {
        return Err(MeilisearchHttpError::Payload(ReceivePayload(Box::new(e))));
    }
    let read_file = buffer.into_inner().into_std().await;
    Ok(read_file)
}

/// Delete documents by batch
///
/// Delete a set of documents based on an array of document ids.
#[utoipa::path(
    post,
    path = "{indexUid}/delete-batch",
    tag = "Documents",
    security(("Bearer" = ["documents.delete", "documents.*", "*"])),
    params(
        ("indexUid", example = "movies", description = "Index Unique Identifier", nullable = false),
    ),
    request_body = Vec<Value>,
    responses(
        (status = 200, description = "Task successfully enqueued", body = SummarizedTaskView, content_type = "application/json", example = json!(
            {
                "taskUid": 147,
                "indexUid": null,
                "status": "enqueued",
                "type": "documentAdditionOrUpdate",
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
pub async fn delete_documents_batch(
    index_scheduler: GuardedData<ActionPolicy<{ actions::DOCUMENTS_DELETE }>, Data<IndexScheduler>>,
    index_uid: web::Path<String>,
    body: web::Json<Vec<Value>>,
    req: HttpRequest,
    opt: web::Data<Opt>,
    analytics: web::Data<Analytics>,
) -> Result<HttpResponse, ResponseError> {
    debug!(parameters = ?body, "Delete documents by batch");
    let index_uid = IndexUid::try_from(index_uid.into_inner())?;

    analytics.publish(
        DocumentsDeletionAggregator {
            per_batch: true,
            per_document_id: false,
            clear_all: false,
            per_filter: false,
        },
        &req,
    );

    let ids = body
        .iter()
        .map(|v| v.as_str().map(String::from).unwrap_or_else(|| v.to_string()))
        .collect();

    let task =
        KindWithContent::DocumentDeletion { index_uid: index_uid.to_string(), documents_ids: ids };
    let uid = get_task_id(&req, &opt)?;
    let dry_run = is_dry_run(&req, &opt)?;
    let task: SummarizedTaskView =
        tokio::task::spawn_blocking(move || index_scheduler.register(task, uid, dry_run))
            .await??
            .into();

    debug!(returns = ?task, "Delete documents by batch");
    Ok(HttpResponse::Accepted().json(task))
}

#[derive(Debug, Deserr, ToSchema)]
#[deserr(error = DeserrJsonError, rename_all = camelCase, deny_unknown_fields)]
#[schema(rename_all = "camelCase")]
pub struct DocumentDeletionByFilter {
    #[deserr(error = DeserrJsonError<InvalidDocumentFilter>, missing_field_error = DeserrJsonError::missing_document_filter)]
    filter: Value,
}

/// Delete documents by filter
///
/// Delete a set of documents based on a filter.
#[utoipa::path(
    post,
    path = "{indexUid}/documents/delete",
    tag = "Documents",
    security(("Bearer" = ["documents.delete", "documents.*", "*"])),
    params(("indexUid", example = "movies", description = "Index Unique Identifier", nullable = false)),
    request_body = DocumentDeletionByFilter,
    responses(
        (status = ACCEPTED, description = "Task successfully enqueued", body = SummarizedTaskView, content_type = "application/json", example = json!(
            {
                "taskUid": 147,
                "indexUid": null,
                "status": "enqueued",
                "type": "documentDeletion",
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
pub async fn delete_documents_by_filter(
    index_scheduler: GuardedData<ActionPolicy<{ actions::DOCUMENTS_DELETE }>, Data<IndexScheduler>>,
    index_uid: web::Path<String>,
    body: AwebJson<DocumentDeletionByFilter, DeserrJsonError>,
    req: HttpRequest,
    opt: web::Data<Opt>,
    analytics: web::Data<Analytics>,
) -> Result<HttpResponse, ResponseError> {
    debug!(parameters = ?body, "Delete documents by filter");
    let index_uid = IndexUid::try_from(index_uid.into_inner())?;
    let index_uid = index_uid.into_inner();
    let filter = body.into_inner().filter;

    analytics.publish(
        DocumentsDeletionAggregator {
            per_filter: true,
            per_document_id: false,
            clear_all: false,
            per_batch: false,
        },
        &req,
    );

    // we ensure the filter is well formed before enqueuing it
    crate::search::parse_filter(&filter, Code::InvalidDocumentFilter, index_scheduler.features())?
        .ok_or(MeilisearchHttpError::EmptyFilter)?;

    let task = KindWithContent::DocumentDeletionByFilter { index_uid, filter_expr: filter };

    let uid = get_task_id(&req, &opt)?;
    let dry_run = is_dry_run(&req, &opt)?;
    let task: SummarizedTaskView =
        tokio::task::spawn_blocking(move || index_scheduler.register(task, uid, dry_run))
            .await??
            .into();

    debug!(returns = ?task, "Delete documents by filter");
    Ok(HttpResponse::Accepted().json(task))
}

#[derive(Debug, Deserr, ToSchema)]
#[deserr(error = DeserrJsonError, rename_all = camelCase, deny_unknown_fields)]
pub struct DocumentEditionByFunction {
    /// A string containing a RHAI function.
    #[deserr(default, error = DeserrJsonError<InvalidDocumentFilter>)]
    pub filter: Option<Value>,
    /// A string containing a filter expression.
    #[deserr(default, error = DeserrJsonError<InvalidDocumentEditionContext>)]
    pub context: Option<Value>,
    /// An object with data Meilisearch should make available for the editing function.
    #[deserr(error = DeserrJsonError<InvalidDocumentEditionFunctionFilter>, missing_field_error = DeserrJsonError::missing_document_edition_function)]
    pub function: String,
}

#[derive(Serialize)]
struct EditDocumentsByFunctionAggregator {
    // Set to true if at least one request was filtered
    filtered: bool,
    // Set to true if at least one request contained a context
    with_context: bool,

    index_creation: bool,
}

impl Aggregate for EditDocumentsByFunctionAggregator {
    fn event_name(&self) -> &'static str {
        "Documents Edited By Function"
    }

    fn aggregate(self: Box<Self>, new: Box<Self>) -> Box<Self> {
        Box::new(Self {
            filtered: self.filtered | new.filtered,
            with_context: self.with_context | new.with_context,
            index_creation: self.index_creation | new.index_creation,
        })
    }

    fn into_event(self: Box<Self>) -> serde_json::Value {
        serde_json::to_value(*self).unwrap_or_default()
    }
}

/// Edit documents by function.
///
/// Use a [RHAI function](https://rhai.rs/book/engine/hello-world.html) to edit one or more documents directly in Meilisearch.
#[utoipa::path(
    post,
    path = "{indexUid}/documents/edit",
    tag = "Documents",
    security(("Bearer" = ["documents.*", "*"])),
    params(
        ("indexUid", example = "movies", description = "Index Unique Identifier", nullable = false),
    ),
    request_body = DocumentEditionByFunction,
    responses(
        (status = 202, description = "Task successfully enqueued", body = SummarizedTaskView, content_type = "application/json", example = json!(
            {
                "taskUid": 147,
                "indexUid": null,
                "status": "enqueued",
                "type": "documentDeletion",
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
pub async fn edit_documents_by_function(
    index_scheduler: GuardedData<ActionPolicy<{ actions::DOCUMENTS_ALL }>, Data<IndexScheduler>>,
    index_uid: web::Path<String>,
    params: AwebJson<DocumentEditionByFunction, DeserrJsonError>,
    req: HttpRequest,
    opt: web::Data<Opt>,
    analytics: web::Data<Analytics>,
) -> Result<HttpResponse, ResponseError> {
    debug!(parameters = ?params, "Edit documents by function");

    index_scheduler
        .features()
        .check_edit_documents_by_function("Using the documents edit route")?;

    let index_uid = IndexUid::try_from(index_uid.into_inner())?;
    let index_uid = index_uid.into_inner();
    let params = params.into_inner();

    analytics.publish(
        EditDocumentsByFunctionAggregator {
            filtered: params.filter.is_some(),
            with_context: params.context.is_some(),
            index_creation: index_scheduler.index(&index_uid).is_err(),
        },
        &req,
    );

    let DocumentEditionByFunction { filter, context, function } = params;
    let engine = milli::rhai::Engine::new();
    if let Err(e) = engine.compile(&function) {
        return Err(ResponseError::from_msg(e.to_string(), Code::BadRequest));
    }

    if let Some(ref filter) = filter {
        // we ensure the filter is well formed before enqueuing it
        crate::search::parse_filter(
            filter,
            Code::InvalidDocumentFilter,
            index_scheduler.features(),
        )?
        .ok_or(MeilisearchHttpError::EmptyFilter)?;
    }
    let task = KindWithContent::DocumentEdition {
        index_uid,
        filter_expr: filter,
        context: match context {
            Some(Value::Object(m)) => Some(m),
            None => None,
            _ => {
                return Err(ResponseError::from_msg(
                    "The context must be an object".to_string(),
                    Code::InvalidDocumentEditionContext,
                ))
            }
        },
        function,
    };

    let uid = get_task_id(&req, &opt)?;
    let dry_run = is_dry_run(&req, &opt)?;
    let task: SummarizedTaskView =
        tokio::task::spawn_blocking(move || index_scheduler.register(task, uid, dry_run))
            .await??
            .into();

    debug!(returns = ?task, "Edit documents by function");
    Ok(HttpResponse::Accepted().json(task))
}

/// Delete all documents
///
/// Delete all documents in the specified index.
#[utoipa::path(
    delete,
    path = "{indexUid}/documents",
    tag = "Documents",
    security(("Bearer" = ["documents.delete", "documents.*", "*"])),
    params(("indexUid", example = "movies", description = "Index Unique Identifier", nullable = false)),
    responses(
        (status = 200, description = "Task successfully enqueued", body = SummarizedTaskView, content_type = "application/json", example = json!(
            {
                "taskUid": 147,
                "indexUid": null,
                "status": "enqueued",
                "type": "documentDeletion",
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
pub async fn clear_all_documents(
    index_scheduler: GuardedData<ActionPolicy<{ actions::DOCUMENTS_DELETE }>, Data<IndexScheduler>>,
    index_uid: web::Path<String>,
    req: HttpRequest,
    opt: web::Data<Opt>,
    analytics: web::Data<Analytics>,
) -> Result<HttpResponse, ResponseError> {
    let index_uid = IndexUid::try_from(index_uid.into_inner())?;
    analytics.publish(
        DocumentsDeletionAggregator {
            clear_all: true,
            per_document_id: false,
            per_batch: false,
            per_filter: false,
        },
        &req,
    );

    let task = KindWithContent::DocumentClear { index_uid: index_uid.to_string() };
    let uid = get_task_id(&req, &opt)?;
    let dry_run = is_dry_run(&req, &opt)?;
    let task: SummarizedTaskView =
        tokio::task::spawn_blocking(move || index_scheduler.register(task, uid, dry_run))
            .await??
            .into();

    debug!(returns = ?task, "Delete all documents");
    Ok(HttpResponse::Accepted().json(task))
}

fn some_documents<'a, 't: 'a>(
    index: &'a Index,
    rtxn: &'t RoTxn,
    doc_ids: impl IntoIterator<Item = DocumentId> + 'a,
    retrieve_vectors: RetrieveVectors,
) -> Result<impl Iterator<Item = Result<Document, ResponseError>> + 'a, ResponseError> {
    let fields_ids_map = index.fields_ids_map(rtxn)?;
    let all_fields: Vec<_> = fields_ids_map.iter().map(|(id, _)| id).collect();
    let embedding_configs = index.embedding_configs(rtxn)?;

    Ok(index.iter_documents(rtxn, doc_ids)?.map(move |ret| {
        ret.map_err(ResponseError::from).and_then(|(key, document)| -> Result<_, ResponseError> {
            let mut document = milli::obkv_to_json(&all_fields, &fields_ids_map, document)?;
            match retrieve_vectors {
                RetrieveVectors::Hide => {
                    document.remove("_vectors");
                }
                RetrieveVectors::Retrieve => {
                    // Clippy is simply wrong
                    #[allow(clippy::manual_unwrap_or_default)]
                    let mut vectors = match document.remove("_vectors") {
                        Some(Value::Object(map)) => map,
                        _ => Default::default(),
                    };
                    for (name, vector) in index.embeddings(rtxn, key)? {
                        let user_provided = embedding_configs
                            .iter()
                            .find(|conf| conf.name == name)
                            .is_some_and(|conf| conf.user_provided.contains(key));
                        let embeddings = ExplicitVectors {
                            embeddings: Some(vector.into()),
                            regenerate: !user_provided,
                        };
                        vectors.insert(
                            name,
                            serde_json::to_value(embeddings).map_err(MeilisearchHttpError::from)?,
                        );
                    }
                    document.insert("_vectors".into(), vectors.into());
                }
            }

            Ok(document)
        })
    }))
}

fn retrieve_documents<S: AsRef<str>>(
    index: &Index,
    offset: usize,
    limit: usize,
    filter: Option<Value>,
    attributes_to_retrieve: Option<Vec<S>>,
    retrieve_vectors: RetrieveVectors,
    features: RoFeatures,
) -> Result<(u64, Vec<Document>), ResponseError> {
    let rtxn = index.read_txn()?;
    let filter = &filter;
    let filter = if let Some(filter) = filter {
        parse_filter(filter, Code::InvalidDocumentFilter, features)?
    } else {
        None
    };

    let candidates = if let Some(filter) = filter {
        filter.evaluate(&rtxn, index).map_err(|err| match err {
            milli::Error::UserError(milli::UserError::InvalidFilter(_)) => {
                ResponseError::from_msg(err.to_string(), Code::InvalidDocumentFilter)
            }
            e => e.into(),
        })?
    } else {
        index.documents_ids(&rtxn)?
    };

    let (it, number_of_documents) = {
        let number_of_documents = candidates.len();
        (
            some_documents(
                index,
                &rtxn,
                candidates.into_iter().skip(offset).take(limit),
                retrieve_vectors,
            )?,
            number_of_documents,
        )
    };

    let documents: Vec<_> = it
        .map(|document| {
            Ok(match &attributes_to_retrieve {
                Some(attributes_to_retrieve) => permissive_json_pointer::select_values(
                    &document?,
                    attributes_to_retrieve.iter().map(|s| s.as_ref()).chain(
                        (retrieve_vectors == RetrieveVectors::Retrieve).then_some("_vectors"),
                    ),
                ),
                None => document?,
            })
        })
        .collect::<Result<_, ResponseError>>()?;

    Ok((number_of_documents, documents))
}

fn retrieve_document<S: AsRef<str>>(
    index: &Index,
    doc_id: &str,
    attributes_to_retrieve: Option<Vec<S>>,
    retrieve_vectors: RetrieveVectors,
) -> Result<Document, ResponseError> {
    let txn = index.read_txn()?;

    let internal_id = index
        .external_documents_ids()
        .get(&txn, doc_id)?
        .ok_or_else(|| MeilisearchHttpError::DocumentNotFound(doc_id.to_string()))?;

    let document = some_documents(index, &txn, Some(internal_id), retrieve_vectors)?
        .next()
        .ok_or_else(|| MeilisearchHttpError::DocumentNotFound(doc_id.to_string()))??;

    let document = match &attributes_to_retrieve {
        Some(attributes_to_retrieve) => permissive_json_pointer::select_values(
            &document,
            attributes_to_retrieve
                .iter()
                .map(|s| s.as_ref())
                .chain((retrieve_vectors == RetrieveVectors::Retrieve).then_some("_vectors")),
        ),
        None => document,
    };

    Ok(document)
}
