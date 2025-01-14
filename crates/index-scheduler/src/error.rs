use std::fmt::Display;

use meilisearch_types::batches::BatchId;
use meilisearch_types::error::{Code, ErrorCode};
use meilisearch_types::tasks::{Kind, Status};
use meilisearch_types::{heed, milli};
use thiserror::Error;

use crate::TaskId;

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum DateField {
    BeforeEnqueuedAt,
    AfterEnqueuedAt,
    BeforeStartedAt,
    AfterStartedAt,
    BeforeFinishedAt,
    AfterFinishedAt,
}

impl Display for DateField {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DateField::BeforeEnqueuedAt => write!(f, "beforeEnqueuedAt"),
            DateField::AfterEnqueuedAt => write!(f, "afterEnqueuedAt"),
            DateField::BeforeStartedAt => write!(f, "beforeStartedAt"),
            DateField::AfterStartedAt => write!(f, "afterStartedAt"),
            DateField::BeforeFinishedAt => write!(f, "beforeFinishedAt"),
            DateField::AfterFinishedAt => write!(f, "afterFinishedAt"),
        }
    }
}

impl From<DateField> for Code {
    fn from(date: DateField) -> Self {
        match date {
            DateField::BeforeEnqueuedAt => Code::InvalidTaskBeforeEnqueuedAt,
            DateField::AfterEnqueuedAt => Code::InvalidTaskAfterEnqueuedAt,
            DateField::BeforeStartedAt => Code::InvalidTaskBeforeStartedAt,
            DateField::AfterStartedAt => Code::InvalidTaskAfterStartedAt,
            DateField::BeforeFinishedAt => Code::InvalidTaskBeforeFinishedAt,
            DateField::AfterFinishedAt => Code::InvalidTaskAfterFinishedAt,
        }
    }
}

#[allow(clippy::large_enum_variant)]
#[derive(Error, Debug)]
pub enum Error {
    #[error("{1}")]
    WithCustomErrorCode(Code, Box<Self>),
    #[error("Received bad task id: {received} should be >= to {expected}.")]
    BadTaskId { received: TaskId, expected: TaskId },
    #[error("Index `{0}` not found.")]
    IndexNotFound(String),
    #[error("Index `{0}` already exists.")]
    IndexAlreadyExists(String),
    #[error(
        "Indexes must be declared only once during a swap. `{0}` was specified several times."
    )]
    SwapDuplicateIndexFound(String),
    #[error(
        "Indexes must be declared only once during a swap. {} were specified several times.",
        .0.iter().map(|s| format!("`{}`", s)).collect::<Vec<_>>().join(", ")
    )]
    SwapDuplicateIndexesFound(Vec<String>),
    #[error("Index `{0}` not found.")]
    SwapIndexNotFound(String),
    #[error("Meilisearch cannot receive write operations because the limit of the task database has been reached. Please delete tasks to continue performing write operations.")]
    NoSpaceLeftInTaskQueue,
    #[error(
        "Indexes {} not found.",
        .0.iter().map(|s| format!("`{}`", s)).collect::<Vec<_>>().join(", ")
    )]
    SwapIndexesNotFound(Vec<String>),
    #[error("Corrupted dump.")]
    CorruptedDump,
    #[error(
        "Task `{field}` `{date}` is invalid. It should follow the YYYY-MM-DD or RFC 3339 date-time format."
    )]
    InvalidTaskDate { field: DateField, date: String },
    #[error("Task uid `{task_uid}` is invalid. It should only contain numeric characters.")]
    InvalidTaskUid { task_uid: String },
    #[error("Batch uid `{batch_uid}` is invalid. It should only contain numeric characters.")]
    InvalidBatchUid { batch_uid: String },
    #[error(
        "Task status `{status}` is invalid. Available task statuses are {}.",
            enum_iterator::all::<Status>()
                .map(|s| format!("`{s}`"))
                .collect::<Vec<String>>()
                .join(", ")
    )]
    InvalidTaskStatuses { status: String },
    #[error(
        "Task type `{type_}` is invalid. Available task types are {}",
            enum_iterator::all::<Kind>()
                .map(|s| format!("`{s}`"))
                .collect::<Vec<String>>()
                .join(", ")
    )]
    InvalidTaskTypes { type_: String },
    #[error(
        "Task canceledBy `{canceled_by}` is invalid. It should only contains numeric characters separated by `,` character."
    )]
    InvalidTaskCanceledBy { canceled_by: String },
    #[error(
        "{index_uid} is not a valid index uid. Index uid can be an integer or a string containing only alphanumeric characters, hyphens (-) and underscores (_), and can not be more than 400 bytes."
    )]
    InvalidIndexUid { index_uid: String },
    #[error("Task `{0}` not found.")]
    TaskNotFound(TaskId),
    #[error("Batch `{0}` not found.")]
    BatchNotFound(BatchId),
    #[error("Query parameters to filter the tasks to delete are missing. Available query parameters are: `uids`, `indexUids`, `statuses`, `types`, `canceledBy`, `beforeEnqueuedAt`, `afterEnqueuedAt`, `beforeStartedAt`, `afterStartedAt`, `beforeFinishedAt`, `afterFinishedAt`.")]
    TaskDeletionWithEmptyQuery,
    #[error("Query parameters to filter the tasks to cancel are missing. Available query parameters are: `uids`, `indexUids`, `statuses`, `types`, `canceledBy`, `beforeEnqueuedAt`, `afterEnqueuedAt`, `beforeStartedAt`, `afterStartedAt`, `beforeFinishedAt`, `afterFinishedAt`.")]
    TaskCancelationWithEmptyQuery,
    #[error("Aborted task")]
    AbortedTask,

    #[error(transparent)]
    Dump(#[from] dump::Error),
    #[error(transparent)]
    Heed(#[from] heed::Error),
    #[error("{}", match .index_uid {
        Some(uid) if !uid.is_empty() => format!("Index `{}`: {error}", uid),
        _ => format!("{error}")
    })]
    Milli { error: milli::Error, index_uid: Option<String> },
    #[error("An unexpected crash occurred when processing the task.")]
    ProcessBatchPanicked,
    #[error(transparent)]
    FileStore(#[from] file_store::Error),
    #[error(transparent)]
    IoError(#[from] std::io::Error),
    #[error(transparent)]
    Persist(#[from] tempfile::PersistError),
    #[error(transparent)]
    FeatureNotEnabled(#[from] FeatureNotEnabledError),

    #[error(transparent)]
    Anyhow(#[from] anyhow::Error),

    // Irrecoverable errors:
    #[error(transparent)]
    CreateBatch(Box<Self>),
    #[error("Corrupted task queue.")]
    CorruptedTaskQueue,
    #[error(transparent)]
    TaskDatabaseUpdate(Box<Self>),
    #[error(transparent)]
    HeedTransaction(heed::Error),

    #[cfg(test)]
    #[error("Planned failure for tests.")]
    PlannedFailure,
}

#[derive(Debug, thiserror::Error)]
#[error(
    "{disabled_action} requires enabling the `{feature}` experimental feature. See {issue_link}"
)]
pub struct FeatureNotEnabledError {
    pub disabled_action: &'static str,
    pub feature: &'static str,
    pub issue_link: &'static str,
}

impl Error {
    pub fn is_recoverable(&self) -> bool {
        match self {
            Error::IndexNotFound(_)
            | Error::WithCustomErrorCode(_, _)
            | Error::BadTaskId { .. }
            | Error::IndexAlreadyExists(_)
            | Error::SwapDuplicateIndexFound(_)
            | Error::SwapDuplicateIndexesFound(_)
            | Error::SwapIndexNotFound(_)
            | Error::NoSpaceLeftInTaskQueue
            | Error::SwapIndexesNotFound(_)
            | Error::CorruptedDump
            | Error::InvalidTaskDate { .. }
            | Error::InvalidTaskUid { .. }
            | Error::InvalidBatchUid { .. }
            | Error::InvalidTaskStatuses { .. }
            | Error::InvalidTaskTypes { .. }
            | Error::InvalidTaskCanceledBy { .. }
            | Error::InvalidIndexUid { .. }
            | Error::TaskNotFound(_)
            | Error::BatchNotFound(_)
            | Error::TaskDeletionWithEmptyQuery
            | Error::TaskCancelationWithEmptyQuery
            | Error::AbortedTask
            | Error::Dump(_)
            | Error::Heed(_)
            | Error::Milli { .. }
            | Error::ProcessBatchPanicked
            | Error::FileStore(_)
            | Error::IoError(_)
            | Error::Persist(_)
            | Error::FeatureNotEnabled(_)
            | Error::Anyhow(_) => true,
            Error::CreateBatch(_)
            | Error::CorruptedTaskQueue
            | Error::TaskDatabaseUpdate(_)
            | Error::HeedTransaction(_) => false,
            #[cfg(test)]
            Error::PlannedFailure => false,
        }
    }

    pub fn with_custom_error_code(self, code: Code) -> Self {
        Self::WithCustomErrorCode(code, Box::new(self))
    }

    pub fn from_milli(err: milli::Error, index_uid: Option<String>) -> Self {
        match err {
            milli::Error::UserError(milli::UserError::InvalidFilter(_)) => {
                Self::Milli { error: err, index_uid }
                    .with_custom_error_code(Code::InvalidDocumentFilter)
            }
            milli::Error::UserError(milli::UserError::InvalidFilterExpression { .. }) => {
                Self::Milli { error: err, index_uid }
                    .with_custom_error_code(Code::InvalidDocumentFilter)
            }
            _ => Self::Milli { error: err, index_uid },
        }
    }
}

impl ErrorCode for Error {
    fn error_code(&self) -> Code {
        match self {
            Error::WithCustomErrorCode(code, _) => *code,
            Error::BadTaskId { .. } => Code::BadRequest,
            Error::IndexNotFound(_) => Code::IndexNotFound,
            Error::IndexAlreadyExists(_) => Code::IndexAlreadyExists,
            Error::SwapDuplicateIndexesFound(_) => Code::InvalidSwapDuplicateIndexFound,
            Error::SwapDuplicateIndexFound(_) => Code::InvalidSwapDuplicateIndexFound,
            Error::SwapIndexNotFound(_) => Code::IndexNotFound,
            Error::SwapIndexesNotFound(_) => Code::IndexNotFound,
            Error::InvalidTaskDate { field, .. } => (*field).into(),
            Error::InvalidTaskUid { .. } => Code::InvalidTaskUids,
            Error::InvalidBatchUid { .. } => Code::InvalidBatchUids,
            Error::InvalidTaskStatuses { .. } => Code::InvalidTaskStatuses,
            Error::InvalidTaskTypes { .. } => Code::InvalidTaskTypes,
            Error::InvalidTaskCanceledBy { .. } => Code::InvalidTaskCanceledBy,
            Error::InvalidIndexUid { .. } => Code::InvalidIndexUid,
            Error::TaskNotFound(_) => Code::TaskNotFound,
            Error::BatchNotFound(_) => Code::BatchNotFound,
            Error::TaskDeletionWithEmptyQuery => Code::MissingTaskFilters,
            Error::TaskCancelationWithEmptyQuery => Code::MissingTaskFilters,
            // TODO: not sure of the Code to use
            Error::NoSpaceLeftInTaskQueue => Code::NoSpaceLeftOnDevice,
            Error::Dump(e) => e.error_code(),
            Error::Milli { error, .. } => error.error_code(),
            Error::ProcessBatchPanicked => Code::Internal,
            Error::Heed(e) => e.error_code(),
            Error::HeedTransaction(e) => e.error_code(),
            Error::FileStore(e) => e.error_code(),
            Error::IoError(e) => e.error_code(),
            Error::Persist(e) => e.error_code(),
            Error::FeatureNotEnabled(_) => Code::FeatureNotEnabled,

            // Irrecoverable errors
            Error::Anyhow(_) => Code::Internal,
            Error::CorruptedTaskQueue => Code::Internal,
            Error::CorruptedDump => Code::Internal,
            Error::TaskDatabaseUpdate(_) => Code::Internal,
            Error::CreateBatch(_) => Code::Internal,

            // This one should never be seen by the end user
            Error::AbortedTask => Code::Internal,

            #[cfg(test)]
            Error::PlannedFailure => Code::Internal,
        }
    }
}
