use milli::Object;
use serde::{Deserialize, Serialize};
use time::{Duration, OffsetDateTime};
use utoipa::ToSchema;

use crate::batches::BatchId;
use crate::error::ResponseError;
use crate::settings::{Settings, Unchecked};
use crate::tasks::{serialize_duration, Details, IndexSwap, Kind, Status, Task, TaskId};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
#[schema(rename_all = "camelCase")]
pub struct TaskView {
    /// The unique sequential identifier of the task.
    #[schema(value_type = u32, example = 4312)]
    pub uid: TaskId,
    /// The unique identifier of the index where this task is operated.
    #[schema(value_type = Option<u32>, example = json!("movies"))]
    pub batch_uid: Option<BatchId>,
    #[serde(default)]
    pub index_uid: Option<String>,
    pub status: Status,
    /// The type of the task.
    #[serde(rename = "type")]
    pub kind: Kind,
    /// The uid of the task that performed the taskCancelation if the task has been canceled.
    #[schema(value_type = Option<u32>, example = json!(4326))]
    pub canceled_by: Option<TaskId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<DetailsView>,
    pub error: Option<ResponseError>,
    /// Total elasped time the engine was in processing state expressed as a `ISO-8601` duration format.
    #[schema(value_type = Option<String>, example = json!(null))]
    #[serde(serialize_with = "serialize_duration", default)]
    pub duration: Option<Duration>,
    /// An `RFC 3339` format for date/time/duration.
    #[schema(value_type = String, example = json!("2024-08-08_14:12:09.393Z"))]
    #[serde(with = "time::serde::rfc3339")]
    pub enqueued_at: OffsetDateTime,
    /// An `RFC 3339` format for date/time/duration.
    #[schema(value_type = String, example = json!("2024-08-08_14:12:09.393Z"))]
    #[serde(with = "time::serde::rfc3339::option", default)]
    pub started_at: Option<OffsetDateTime>,
    /// An `RFC 3339` format for date/time/duration.
    #[schema(value_type = String, example = json!("2024-08-08_14:12:09.393Z"))]
    #[serde(with = "time::serde::rfc3339::option", default)]
    pub finished_at: Option<OffsetDateTime>,
}

impl TaskView {
    pub fn from_task(task: &Task) -> TaskView {
        TaskView {
            uid: task.uid,
            batch_uid: task.batch_uid,
            index_uid: task.index_uid().map(ToOwned::to_owned),
            status: task.status,
            kind: task.kind.as_kind(),
            canceled_by: task.canceled_by,
            details: task.details.clone().map(DetailsView::from),
            error: task.error.clone(),
            duration: task.started_at.zip(task.finished_at).map(|(start, end)| end - start),
            enqueued_at: task.enqueued_at,
            started_at: task.started_at,
            finished_at: task.finished_at,
        }
    }
}

#[derive(Default, Debug, PartialEq, Eq, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
#[schema(rename_all = "camelCase")]
pub struct DetailsView {
    /// Number of documents received for documentAdditionOrUpdate task.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub received_documents: Option<u64>,
    /// Number of documents finally indexed for documentAdditionOrUpdate task or a documentAdditionOrUpdate batch of tasks.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub indexed_documents: Option<Option<u64>>,
    /// Number of documents edited for editDocumentByFunction task.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub edited_documents: Option<Option<u64>>,
    /// Value for the primaryKey field encountered if any for indexCreation or indexUpdate task.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub primary_key: Option<Option<String>>,
    /// Number of provided document ids for the documentDeletion task.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub provided_ids: Option<usize>,
    /// Number of documents finally deleted for documentDeletion and indexDeletion tasks.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deleted_documents: Option<Option<u64>>,
    /// Number of tasks that match the request for taskCancelation or taskDeletion tasks.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub matched_tasks: Option<u64>,
    /// Number of tasks canceled for taskCancelation.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub canceled_tasks: Option<Option<u64>>,
    /// Number of tasks deleted for taskDeletion.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deleted_tasks: Option<Option<u64>>,
    /// Original filter query for taskCancelation or taskDeletion tasks.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub original_filter: Option<Option<String>>,
    /// Identifier generated for the dump for dumpCreation task.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dump_uid: Option<Option<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub context: Option<Option<Object>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub function: Option<String>,
    /// [Learn more about the settings in this guide](https://www.meilisearch.com/docs/reference/api/settings).
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(flatten)]
    pub settings: Option<Box<Settings<Unchecked>>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub swaps: Option<Vec<IndexSwap>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub upgrade_from: Option<String>,
}

impl DetailsView {
    pub fn accumulate(&mut self, other: &Self) {
        *self = Self {
            received_documents: match (self.received_documents, other.received_documents) {
                (None, None) => None,
                (None, Some(doc)) | (Some(doc), None) => Some(doc),
                (Some(left), Some(right)) => Some(left + right),
            },
            indexed_documents: match (self.indexed_documents, other.indexed_documents) {
                (None, None) => None,
                (None, Some(None)) | (Some(None), None) | (Some(None), Some(None)) => Some(None),
                (None | Some(None), Some(Some(doc))) | (Some(Some(doc)), None | Some(None)) => {
                    Some(Some(doc))
                }
                (Some(Some(left)), Some(Some(right))) => Some(Some(left + right)),
            },
            edited_documents: match (self.edited_documents, other.edited_documents) {
                (None, None) => None,
                (None, Some(None)) | (Some(None), None) | (Some(None), Some(None)) => Some(None),
                (None | Some(None), Some(Some(doc))) | (Some(Some(doc)), None | Some(None)) => {
                    Some(Some(doc))
                }
                (Some(Some(left)), Some(Some(right))) => Some(Some(left + right)),
            },
            primary_key: match (&self.primary_key, &other.primary_key) {
                (None, None) => None,
                (None, Some(None)) | (Some(None), None) | (Some(None), Some(None)) => Some(None),
                (None | Some(None), Some(Some(doc))) | (Some(Some(doc)), None | Some(None)) => {
                    Some(Some(doc.to_string()))
                }
                // In the case we receive multiple primary keys (which shouldn't happens) we only return the first one encountered.
                (Some(Some(left)), Some(Some(_right))) => Some(Some(left.to_string())),
            },
            provided_ids: match (self.provided_ids, other.provided_ids) {
                (None, None) => None,
                (None, Some(ids)) | (Some(ids), None) => Some(ids),
                (Some(left), Some(right)) => Some(left + right),
            },
            deleted_documents: match (self.deleted_documents, other.deleted_documents) {
                (None, None) => None,
                (None, Some(None)) | (Some(None), None) | (Some(None), Some(None)) => Some(None),
                (None | Some(None), Some(Some(doc))) | (Some(Some(doc)), None | Some(None)) => {
                    Some(Some(doc))
                }
                (Some(Some(left)), Some(Some(right))) => Some(Some(left + right)),
            },
            matched_tasks: match (self.matched_tasks, other.matched_tasks) {
                (None, None) => None,
                (None, Some(task)) | (Some(task), None) => Some(task),
                (Some(left), Some(right)) => Some(left + right),
            },
            canceled_tasks: match (self.canceled_tasks, other.canceled_tasks) {
                (None, None) => None,
                (None, Some(None)) | (Some(None), None) | (Some(None), Some(None)) => Some(None),
                (None | Some(None), Some(Some(task))) | (Some(Some(task)), None | Some(None)) => {
                    Some(Some(task))
                }
                (Some(Some(left)), Some(Some(right))) => Some(Some(left + right)),
            },
            deleted_tasks: match (self.deleted_tasks, other.deleted_tasks) {
                (None, None) => None,
                (None, Some(None)) | (Some(None), None) | (Some(None), Some(None)) => Some(None),
                (None | Some(None), Some(Some(task))) | (Some(Some(task)), None | Some(None)) => {
                    Some(Some(task))
                }
                (Some(Some(left)), Some(Some(right))) => Some(Some(left + right)),
            },
            original_filter: match (&self.original_filter, &other.original_filter) {
                (None, None) => None,
                (None, Some(None)) | (Some(None), None) | (Some(None), Some(None)) => Some(None),
                (None | Some(None), Some(Some(filter)))
                | (Some(Some(filter)), None | Some(None)) => Some(Some(filter.to_string())),
                // In this case, we cannot really merge both filters or return an array so we're going to return
                // all the conditions one after the other.
                (Some(Some(left)), Some(Some(right))) => Some(Some(format!("{left}&{right}"))),
            },
            dump_uid: match (&self.dump_uid, &other.dump_uid) {
                (None, None) => None,
                (None, Some(None)) | (Some(None), None) | (Some(None), Some(None)) => Some(None),
                (None | Some(None), Some(Some(dump_uid)))
                | (Some(Some(dump_uid)), None | Some(None)) => Some(Some(dump_uid.to_string())),
                // We should never be able to batch multiple dumps at the same time. So we return
                // the first one we encounter but that shouldn't be an issue anyway.
                (Some(Some(left)), Some(Some(_right))) => Some(Some(left.to_string())),
            },
            context: match (&self.context, &other.context) {
                (None, None) => None,
                (None, Some(None)) | (Some(None), None) | (Some(None), Some(None)) => Some(None),
                (None | Some(None), Some(Some(ctx))) | (Some(Some(ctx)), None | Some(None)) => {
                    Some(Some(ctx.clone()))
                }
                // We should never be able to batch multiple documents edited at the same time. So we return
                // the first one we encounter but that shouldn't be an issue anyway.
                (Some(Some(left)), Some(Some(_right))) => Some(Some(left.clone())),
            },
            function: match (&self.function, &other.function) {
                (None, None) => None,
                (None, Some(fun)) | (Some(fun), None) => Some(fun.to_string()),
                // We should never be able to batch multiple documents edited at the same time. So we return
                // the first one we encounter but that shouldn't be an issue anyway.
                (Some(left), Some(_right)) => Some(left.to_string()),
            },
            settings: match (self.settings.clone(), other.settings.clone()) {
                (None, None) => None,
                (None, Some(settings)) | (Some(settings), None) => Some(settings),
                (Some(mut left), Some(right)) => {
                    left.merge(&right);
                    Some(left)
                }
            },
            swaps: match (self.swaps.clone(), other.swaps.clone()) {
                (None, None) => None,
                (None, Some(swaps)) | (Some(swaps), None) => Some(swaps),
                (Some(mut left), Some(mut right)) => {
                    left.append(&mut right);
                    Some(left)
                }
            },
            upgrade_from: match (self.upgrade_from.clone(), other.upgrade_from.clone()) {
                (None, None) => None,
                (None, Some(from)) | (Some(from), None) => Some(from),
                (Some(_), Some(from)) => Some(from),
            },
        }
    }
}

impl From<Details> for DetailsView {
    fn from(details: Details) -> Self {
        match details {
            Details::DocumentAdditionOrUpdate { received_documents, indexed_documents } => {
                DetailsView {
                    received_documents: Some(received_documents),
                    indexed_documents: Some(indexed_documents),
                    ..DetailsView::default()
                }
            }
            Details::DocumentEdition {
                deleted_documents,
                edited_documents,
                original_filter,
                context,
                function,
            } => DetailsView {
                deleted_documents: Some(deleted_documents),
                edited_documents: Some(edited_documents),
                original_filter: Some(original_filter),
                context: Some(context),
                function: Some(function),
                ..DetailsView::default()
            },
            Details::SettingsUpdate { mut settings } => {
                settings.hide_secrets();
                DetailsView { settings: Some(settings), ..DetailsView::default() }
            }
            Details::IndexInfo { primary_key } => {
                DetailsView { primary_key: Some(primary_key), ..DetailsView::default() }
            }
            Details::DocumentDeletion {
                provided_ids: received_document_ids,
                deleted_documents,
            } => DetailsView {
                provided_ids: Some(received_document_ids),
                deleted_documents: Some(deleted_documents),
                original_filter: Some(None),
                ..DetailsView::default()
            },
            Details::DocumentDeletionByFilter { original_filter, deleted_documents } => {
                DetailsView {
                    provided_ids: Some(0),
                    original_filter: Some(Some(original_filter)),
                    deleted_documents: Some(deleted_documents),
                    ..DetailsView::default()
                }
            }
            Details::ClearAll { deleted_documents } => {
                DetailsView { deleted_documents: Some(deleted_documents), ..DetailsView::default() }
            }
            Details::TaskCancelation { matched_tasks, canceled_tasks, original_filter } => {
                DetailsView {
                    matched_tasks: Some(matched_tasks),
                    canceled_tasks: Some(canceled_tasks),
                    original_filter: Some(Some(original_filter)),
                    ..DetailsView::default()
                }
            }
            Details::TaskDeletion { matched_tasks, deleted_tasks, original_filter } => {
                DetailsView {
                    matched_tasks: Some(matched_tasks),
                    deleted_tasks: Some(deleted_tasks),
                    original_filter: Some(Some(original_filter)),
                    ..DetailsView::default()
                }
            }
            Details::Dump { dump_uid } => {
                DetailsView { dump_uid: Some(dump_uid), ..DetailsView::default() }
            }
            Details::IndexSwap { swaps } => {
                DetailsView { swaps: Some(swaps), ..Default::default() }
            }
            Details::UpgradeDatabase { from } => DetailsView {
                upgrade_from: Some(format!("v{}.{}.{}", from.0, from.1, from.2)),
                ..Default::default()
            },
        }
    }
}
