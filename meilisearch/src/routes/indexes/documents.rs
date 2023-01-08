use std::convert::Infallible;
use std::fmt;
use std::io::ErrorKind;
use std::num::ParseIntError;
use std::str::FromStr;

use actix_web::http::header::CONTENT_TYPE;
use actix_web::web::Data;
use actix_web::{web, HttpMessage, HttpRequest, HttpResponse};
use bstr::ByteSlice;
use deserr::{DeserializeError, DeserializeFromValue, IntoValue, MergeWithError, ValuePointerRef};
use futures::StreamExt;
use index_scheduler::IndexScheduler;
use log::debug;
use meilisearch_types::document_formats::{read_csv, read_json, read_ndjson, PayloadType};
use meilisearch_types::error::{unwrap_any, Code, ErrorCode, ResponseError};
use meilisearch_types::heed::RoTxn;
use meilisearch_types::index_uid::IndexUid;
use meilisearch_types::milli::update::IndexDocumentsMethod;
use meilisearch_types::star_or::StarOr;
use meilisearch_types::tasks::KindWithContent;
use meilisearch_types::{milli, Document, Index};
use mime::Mime;
use once_cell::sync::Lazy;
use serde::Deserialize;
use serde_cs::vec::CS;
use serde_json::Value;
use tempfile::tempfile;
use tokio::fs::File;
use tokio::io::{AsyncSeekExt, AsyncWriteExt, BufWriter};

use crate::analytics::{Analytics, DocumentDeletionKind};
use crate::error::MeilisearchHttpError;
use crate::error::PayloadError::ReceivePayload;
use crate::extractors::authentication::policies::*;
use crate::extractors::authentication::GuardedData;
use crate::extractors::payload::Payload;
use crate::extractors::query_parameters::QueryParameter;
use crate::extractors::sequential_extractor::SeqHandler;
use crate::routes::{fold_star_or, PaginationView, SummarizedTaskView};

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

pub fn configure(cfg: &mut web::ServiceConfig) {
    cfg.service(
        web::resource("")
            .route(web::get().to(SeqHandler(get_all_documents)))
            .route(web::post().to(SeqHandler(add_documents)))
            .route(web::put().to(SeqHandler(update_documents)))
            .route(web::delete().to(SeqHandler(clear_all_documents))),
    )
    // this route needs to be before the /documents/{document_id} to match properly
    .service(web::resource("/delete-batch").route(web::post().to(SeqHandler(delete_documents))))
    .service(
        web::resource("/{document_id}")
            .route(web::get().to(SeqHandler(get_document)))
            .route(web::delete().to(SeqHandler(delete_document))),
    );
}

#[derive(Deserialize, Debug, DeserializeFromValue)]
#[deserr(rename_all = camelCase, deny_unknown_fields)]
pub struct GetDocument {
    fields: Option<CS<StarOr<String>>>,
}

#[derive(Debug)]
pub struct GetDocumentDeserrError {
    error: String,
    code: Code,
}

impl std::fmt::Display for GetDocumentDeserrError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.error)
    }
}

impl std::error::Error for GetDocumentDeserrError {}
impl ErrorCode for GetDocumentDeserrError {
    fn error_code(&self) -> Code {
        self.code
    }
}

impl MergeWithError<GetDocumentDeserrError> for GetDocumentDeserrError {
    fn merge(
        _self_: Option<Self>,
        other: GetDocumentDeserrError,
        _merge_location: ValuePointerRef,
    ) -> Result<Self, Self> {
        Err(other)
    }
}

impl DeserializeError for GetDocumentDeserrError {
    fn error<V: IntoValue>(
        _self_: Option<Self>,
        error: deserr::ErrorKind<V>,
        location: ValuePointerRef,
    ) -> Result<Self, Self> {
        let error = unwrap_any(deserr::serde_json::JsonError::error(None, error, location)).0;

        let code = match location.last_field() {
            Some("fields") => Code::InvalidDocumentFields,
            _ => Code::BadRequest,
        };

        Err(GetDocumentDeserrError { error, code })
    }
}

pub async fn get_document(
    index_scheduler: GuardedData<ActionPolicy<{ actions::DOCUMENTS_GET }>, Data<IndexScheduler>>,
    path: web::Path<DocumentParam>,
    params: QueryParameter<GetDocument, GetDocumentDeserrError>,
) -> Result<HttpResponse, ResponseError> {
    let GetDocument { fields } = params.into_inner();
    let attributes_to_retrieve = fields.and_then(fold_star_or);

    let index = index_scheduler.index(&path.index_uid)?;
    let document = retrieve_document(&index, &path.document_id, attributes_to_retrieve)?;
    debug!("returns: {:?}", document);
    Ok(HttpResponse::Ok().json(document))
}

pub async fn delete_document(
    index_scheduler: GuardedData<ActionPolicy<{ actions::DOCUMENTS_DELETE }>, Data<IndexScheduler>>,
    path: web::Path<DocumentParam>,
    req: HttpRequest,
    analytics: web::Data<dyn Analytics>,
) -> Result<HttpResponse, ResponseError> {
    analytics.delete_documents(DocumentDeletionKind::PerDocumentId, &req);

    let DocumentParam { document_id, index_uid } = path.into_inner();
    let task = KindWithContent::DocumentDeletion { index_uid, documents_ids: vec![document_id] };
    let task: SummarizedTaskView =
        tokio::task::spawn_blocking(move || index_scheduler.register(task)).await??.into();
    debug!("returns: {:?}", task);
    Ok(HttpResponse::Accepted().json(task))
}

#[derive(Deserialize, Debug, DeserializeFromValue)]
#[deserr(rename_all = camelCase, deny_unknown_fields)]
pub struct BrowseQuery {
    #[deserr(default, from(&String) = FromStr::from_str -> ParseIntError)]
    offset: usize,
    #[deserr(default = crate::routes::PAGINATION_DEFAULT_LIMIT(), from(&String) = FromStr::from_str -> ParseIntError)]
    limit: usize,
    fields: Option<CS<StarOr<String>>>,
}

#[derive(Debug)]
pub struct BrowseQueryDeserrError {
    error: String,
    code: Code,
}

impl std::fmt::Display for BrowseQueryDeserrError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.error)
    }
}

impl std::error::Error for BrowseQueryDeserrError {}
impl ErrorCode for BrowseQueryDeserrError {
    fn error_code(&self) -> Code {
        self.code
    }
}

impl MergeWithError<BrowseQueryDeserrError> for BrowseQueryDeserrError {
    fn merge(
        _self_: Option<Self>,
        other: BrowseQueryDeserrError,
        _merge_location: ValuePointerRef,
    ) -> Result<Self, Self> {
        Err(other)
    }
}

impl DeserializeError for BrowseQueryDeserrError {
    fn error<V: IntoValue>(
        _self_: Option<Self>,
        error: deserr::ErrorKind<V>,
        location: ValuePointerRef,
    ) -> Result<Self, Self> {
        let error = unwrap_any(deserr::serde_json::JsonError::error(None, error, location)).0;

        let code = match location.last_field() {
            Some("fields") => Code::InvalidDocumentFields,
            Some("offset") => Code::InvalidDocumentOffset,
            Some("limit") => Code::InvalidDocumentLimit,
            _ => Code::BadRequest,
        };

        Err(BrowseQueryDeserrError { error, code })
    }
}

impl MergeWithError<ParseIntError> for BrowseQueryDeserrError {
    fn merge(
        _self_: Option<Self>,
        other: ParseIntError,
        merge_location: ValuePointerRef,
    ) -> Result<Self, Self> {
        BrowseQueryDeserrError::error::<Infallible>(
            None,
            deserr::ErrorKind::Unexpected { msg: other.to_string() },
            merge_location,
        )
    }
}

pub async fn get_all_documents(
    index_scheduler: GuardedData<ActionPolicy<{ actions::DOCUMENTS_GET }>, Data<IndexScheduler>>,
    index_uid: web::Path<String>,
    params: QueryParameter<BrowseQuery, BrowseQueryDeserrError>,
) -> Result<HttpResponse, ResponseError> {
    debug!("called with params: {:?}", params);
    let BrowseQuery { limit, offset, fields } = params.into_inner();
    let attributes_to_retrieve = fields.and_then(fold_star_or);

    let index = index_scheduler.index(&index_uid)?;
    let (total, documents) = retrieve_documents(&index, offset, limit, attributes_to_retrieve)?;

    let ret = PaginationView::new(offset, limit, total as usize, documents);

    debug!("returns: {:?}", ret);
    Ok(HttpResponse::Ok().json(ret))
}

#[derive(Deserialize, Debug, DeserializeFromValue)]
#[deserr(rename_all = camelCase, deny_unknown_fields)]
pub struct UpdateDocumentsQuery {
    pub primary_key: Option<String>,
}

#[derive(Debug)]
pub struct UpdateDocumentsQueryDeserrError {
    error: String,
    code: Code,
}

impl std::fmt::Display for UpdateDocumentsQueryDeserrError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.error)
    }
}

impl std::error::Error for UpdateDocumentsQueryDeserrError {}
impl ErrorCode for UpdateDocumentsQueryDeserrError {
    fn error_code(&self) -> Code {
        self.code
    }
}

impl MergeWithError<UpdateDocumentsQueryDeserrError> for UpdateDocumentsQueryDeserrError {
    fn merge(
        _self_: Option<Self>,
        other: UpdateDocumentsQueryDeserrError,
        _merge_location: ValuePointerRef,
    ) -> Result<Self, Self> {
        Err(other)
    }
}

impl DeserializeError for UpdateDocumentsQueryDeserrError {
    fn error<V: IntoValue>(
        _self_: Option<Self>,
        error: deserr::ErrorKind<V>,
        location: ValuePointerRef,
    ) -> Result<Self, Self> {
        let error = unwrap_any(deserr::serde_json::JsonError::error(None, error, location)).0;

        let code = match location.last_field() {
            Some("primaryKey") => Code::InvalidIndexPrimaryKey,
            _ => Code::BadRequest,
        };

        Err(UpdateDocumentsQueryDeserrError { error, code })
    }
}

pub async fn add_documents(
    index_scheduler: GuardedData<ActionPolicy<{ actions::DOCUMENTS_ADD }>, Data<IndexScheduler>>,
    index_uid: web::Path<String>,
    params: QueryParameter<UpdateDocumentsQuery, UpdateDocumentsQueryDeserrError>,
    body: Payload,
    req: HttpRequest,
    analytics: web::Data<dyn Analytics>,
) -> Result<HttpResponse, ResponseError> {
    debug!("called with params: {:?}", params);
    let params = params.into_inner();

    analytics.add_documents(&params, index_scheduler.index(&index_uid).is_err(), &req);

    let allow_index_creation = index_scheduler.filters().allow_index_creation;
    let task = document_addition(
        extract_mime_type(&req)?,
        index_scheduler,
        index_uid.into_inner(),
        params.primary_key,
        body,
        IndexDocumentsMethod::ReplaceDocuments,
        allow_index_creation,
    )
    .await?;

    Ok(HttpResponse::Accepted().json(task))
}

pub async fn update_documents(
    index_scheduler: GuardedData<ActionPolicy<{ actions::DOCUMENTS_ADD }>, Data<IndexScheduler>>,
    path: web::Path<String>,
    params: QueryParameter<UpdateDocumentsQuery, UpdateDocumentsQueryDeserrError>,
    body: Payload,
    req: HttpRequest,
    analytics: web::Data<dyn Analytics>,
) -> Result<HttpResponse, ResponseError> {
    debug!("called with params: {:?}", params);
    let index_uid = path.into_inner();

    analytics.update_documents(&params, index_scheduler.index(&index_uid).is_err(), &req);

    let allow_index_creation = index_scheduler.filters().allow_index_creation;
    let task = document_addition(
        extract_mime_type(&req)?,
        index_scheduler,
        index_uid,
        params.into_inner().primary_key,
        body,
        IndexDocumentsMethod::UpdateDocuments,
        allow_index_creation,
    )
    .await?;

    Ok(HttpResponse::Accepted().json(task))
}

async fn document_addition(
    mime_type: Option<Mime>,
    index_scheduler: GuardedData<ActionPolicy<{ actions::DOCUMENTS_ADD }>, Data<IndexScheduler>>,
    index_uid: String,
    primary_key: Option<String>,
    mut body: Payload,
    method: IndexDocumentsMethod,
    allow_index_creation: bool,
) -> Result<SummarizedTaskView, MeilisearchHttpError> {
    let format = match mime_type.as_ref().map(|m| (m.type_().as_str(), m.subtype().as_str())) {
        Some(("application", "json")) => PayloadType::Json,
        Some(("application", "x-ndjson")) => PayloadType::Ndjson,
        Some(("text", "csv")) => PayloadType::Csv,
        Some((type_, subtype)) => {
            return Err(MeilisearchHttpError::InvalidContentType(
                format!("{}/{}", type_, subtype),
                ACCEPTED_CONTENT_TYPE.clone(),
            ))
        }
        None => {
            return Err(MeilisearchHttpError::MissingContentType(ACCEPTED_CONTENT_TYPE.clone()))
        }
    };

    // is your indexUid valid?
    let index_uid = IndexUid::try_from(index_uid)?.into_inner();

    let (uuid, mut update_file) = index_scheduler.create_update_file()?;

    let temp_file = match tempfile() {
        Ok(file) => file,
        Err(e) => return Err(MeilisearchHttpError::Payload(ReceivePayload(Box::new(e)))),
    };

    let async_file = File::from_std(temp_file);
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
    let documents_count = tokio::task::spawn_blocking(move || {
        let documents_count = match format {
            PayloadType::Json => read_json(&read_file, update_file.as_file_mut())?,
            PayloadType::Csv => read_csv(&read_file, update_file.as_file_mut())?,
            PayloadType::Ndjson => read_ndjson(&read_file, update_file.as_file_mut())?,
        };
        // we NEED to persist the file here because we moved the `udpate_file` in another task.
        update_file.persist()?;
        Ok(documents_count)
    })
    .await;

    let documents_count = match documents_count {
        Ok(Ok(documents_count)) => documents_count,
        // in this case the file has not possibly be persisted.
        Ok(Err(e)) => return Err(e),
        Err(e) => {
            // Here the file MAY have been persisted or not.
            // We don't know thus we ignore the file not found error.
            match index_scheduler.delete_update_file(uuid) {
                Ok(()) => (),
                Err(index_scheduler::Error::FileStore(file_store::Error::IoError(e)))
                    if e.kind() == ErrorKind::NotFound => {}
                Err(e) => {
                    log::warn!("Unknown error happened while deleting a malformed update file with uuid {uuid}: {e}");
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
        index_uid,
    };

    let scheduler = index_scheduler.clone();
    let task = match tokio::task::spawn_blocking(move || scheduler.register(task)).await? {
        Ok(task) => task,
        Err(e) => {
            index_scheduler.delete_update_file(uuid)?;
            return Err(e.into());
        }
    };

    debug!("returns: {:?}", task);
    Ok(task.into())
}

pub async fn delete_documents(
    index_scheduler: GuardedData<ActionPolicy<{ actions::DOCUMENTS_DELETE }>, Data<IndexScheduler>>,
    path: web::Path<String>,
    body: web::Json<Vec<Value>>,
    req: HttpRequest,
    analytics: web::Data<dyn Analytics>,
) -> Result<HttpResponse, ResponseError> {
    debug!("called with params: {:?}", body);

    analytics.delete_documents(DocumentDeletionKind::PerBatch, &req);

    let ids = body
        .iter()
        .map(|v| v.as_str().map(String::from).unwrap_or_else(|| v.to_string()))
        .collect();

    let task =
        KindWithContent::DocumentDeletion { index_uid: path.into_inner(), documents_ids: ids };
    let task: SummarizedTaskView =
        tokio::task::spawn_blocking(move || index_scheduler.register(task)).await??.into();

    debug!("returns: {:?}", task);
    Ok(HttpResponse::Accepted().json(task))
}

pub async fn clear_all_documents(
    index_scheduler: GuardedData<ActionPolicy<{ actions::DOCUMENTS_DELETE }>, Data<IndexScheduler>>,
    path: web::Path<String>,
    req: HttpRequest,
    analytics: web::Data<dyn Analytics>,
) -> Result<HttpResponse, ResponseError> {
    analytics.delete_documents(DocumentDeletionKind::ClearAll, &req);

    let task = KindWithContent::DocumentClear { index_uid: path.into_inner() };
    let task: SummarizedTaskView =
        tokio::task::spawn_blocking(move || index_scheduler.register(task)).await??.into();

    debug!("returns: {:?}", task);
    Ok(HttpResponse::Accepted().json(task))
}

fn all_documents<'a>(
    index: &Index,
    rtxn: &'a RoTxn,
) -> Result<impl Iterator<Item = Result<Document, ResponseError>> + 'a, ResponseError> {
    let fields_ids_map = index.fields_ids_map(rtxn)?;
    let all_fields: Vec<_> = fields_ids_map.iter().map(|(id, _)| id).collect();

    Ok(index.all_documents(rtxn)?.map(move |ret| {
        ret.map_err(ResponseError::from).and_then(|(_key, document)| -> Result<_, ResponseError> {
            Ok(milli::obkv_to_json(&all_fields, &fields_ids_map, document)?)
        })
    }))
}

fn retrieve_documents<S: AsRef<str>>(
    index: &Index,
    offset: usize,
    limit: usize,
    attributes_to_retrieve: Option<Vec<S>>,
) -> Result<(u64, Vec<Document>), ResponseError> {
    let rtxn = index.read_txn()?;

    let mut documents = Vec::new();
    for document in all_documents(index, &rtxn)?.skip(offset).take(limit) {
        let document = match &attributes_to_retrieve {
            Some(attributes_to_retrieve) => permissive_json_pointer::select_values(
                &document?,
                attributes_to_retrieve.iter().map(|s| s.as_ref()),
            ),
            None => document?,
        };
        documents.push(document);
    }

    let number_of_documents = index.number_of_documents(&rtxn)?;
    Ok((number_of_documents, documents))
}

fn retrieve_document<S: AsRef<str>>(
    index: &Index,
    doc_id: &str,
    attributes_to_retrieve: Option<Vec<S>>,
) -> Result<Document, ResponseError> {
    let txn = index.read_txn()?;

    let fields_ids_map = index.fields_ids_map(&txn)?;
    let all_fields: Vec<_> = fields_ids_map.iter().map(|(id, _)| id).collect();

    let internal_id = index
        .external_documents_ids(&txn)?
        .get(doc_id.as_bytes())
        .ok_or_else(|| MeilisearchHttpError::DocumentNotFound(doc_id.to_string()))?;

    let document = index
        .documents(&txn, std::iter::once(internal_id))?
        .into_iter()
        .next()
        .map(|(_, d)| d)
        .ok_or_else(|| MeilisearchHttpError::DocumentNotFound(doc_id.to_string()))?;

    let document = meilisearch_types::milli::obkv_to_json(&all_fields, &fields_ids_map, document)?;
    let document = match &attributes_to_retrieve {
        Some(attributes_to_retrieve) => permissive_json_pointer::select_values(
            &document,
            attributes_to_retrieve.iter().map(|s| s.as_ref()),
        ),
        None => document,
    };

    Ok(document)
}
