use core::fmt;
use std::collections::HashSet;
use std::fmt::{Display, Write};
use std::str::FromStr;

use enum_iterator::Sequence;
use milli::update::IndexDocumentsMethod;
use milli::Object;
use roaring::RoaringBitmap;
use serde::{Deserialize, Serialize, Serializer};
use time::{Duration, OffsetDateTime};
use utoipa::ToSchema;
use uuid::Uuid;

use crate::batches::BatchId;
use crate::error::ResponseError;
use crate::keys::Key;
use crate::settings::{Settings, Unchecked};
use crate::InstanceUid;

pub type TaskId = u32;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Task {
    pub uid: TaskId,
    pub batch_uid: Option<BatchId>,

    #[serde(with = "time::serde::rfc3339")]
    pub enqueued_at: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339::option")]
    pub started_at: Option<OffsetDateTime>,
    #[serde(with = "time::serde::rfc3339::option")]
    pub finished_at: Option<OffsetDateTime>,

    pub error: Option<ResponseError>,
    pub canceled_by: Option<TaskId>,
    pub details: Option<Details>,

    pub status: Status,
    pub kind: KindWithContent,
}

impl Task {
    pub fn index_uid(&self) -> Option<&str> {
        use KindWithContent::*;

        match &self.kind {
            DumpCreation { .. }
            | SnapshotCreation
            | TaskCancelation { .. }
            | TaskDeletion { .. }
            | UpgradeDatabase { .. }
            | IndexSwap { .. } => None,
            DocumentAdditionOrUpdate { index_uid, .. }
            | DocumentEdition { index_uid, .. }
            | DocumentDeletion { index_uid, .. }
            | DocumentDeletionByFilter { index_uid, .. }
            | DocumentClear { index_uid }
            | SettingsUpdate { index_uid, .. }
            | IndexCreation { index_uid, .. }
            | IndexUpdate { index_uid, .. }
            | IndexDeletion { index_uid } => Some(index_uid),
        }
    }

    /// Return the list of indexes updated by this tasks.
    pub fn indexes(&self) -> Vec<&str> {
        self.kind.indexes()
    }

    /// Return the content-uuid if there is one
    pub fn content_uuid(&self) -> Option<Uuid> {
        match self.kind {
            KindWithContent::DocumentAdditionOrUpdate { content_file, .. } => Some(content_file),
            KindWithContent::DocumentEdition { .. }
            | KindWithContent::DocumentDeletion { .. }
            | KindWithContent::DocumentDeletionByFilter { .. }
            | KindWithContent::DocumentClear { .. }
            | KindWithContent::SettingsUpdate { .. }
            | KindWithContent::IndexDeletion { .. }
            | KindWithContent::IndexCreation { .. }
            | KindWithContent::IndexUpdate { .. }
            | KindWithContent::IndexSwap { .. }
            | KindWithContent::TaskCancelation { .. }
            | KindWithContent::TaskDeletion { .. }
            | KindWithContent::DumpCreation { .. }
            | KindWithContent::SnapshotCreation
            | KindWithContent::UpgradeDatabase { .. } => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum KindWithContent {
    DocumentAdditionOrUpdate {
        index_uid: String,
        primary_key: Option<String>,
        method: IndexDocumentsMethod,
        content_file: Uuid,
        documents_count: u64,
        allow_index_creation: bool,
    },
    DocumentDeletion {
        index_uid: String,
        documents_ids: Vec<String>,
    },
    DocumentDeletionByFilter {
        index_uid: String,
        filter_expr: serde_json::Value,
    },
    DocumentEdition {
        index_uid: String,
        filter_expr: Option<serde_json::Value>,
        context: Option<milli::Object>,
        function: String,
    },
    DocumentClear {
        index_uid: String,
    },
    SettingsUpdate {
        index_uid: String,
        new_settings: Box<Settings<Unchecked>>,
        is_deletion: bool,
        allow_index_creation: bool,
    },
    IndexDeletion {
        index_uid: String,
    },
    IndexCreation {
        index_uid: String,
        primary_key: Option<String>,
    },
    IndexUpdate {
        index_uid: String,
        primary_key: Option<String>,
    },
    IndexSwap {
        swaps: Vec<IndexSwap>,
    },
    TaskCancelation {
        query: String,
        tasks: RoaringBitmap,
    },
    TaskDeletion {
        query: String,
        tasks: RoaringBitmap,
    },
    DumpCreation {
        keys: Vec<Key>,
        instance_uid: Option<InstanceUid>,
    },
    SnapshotCreation,
    UpgradeDatabase {
        from: (u32, u32, u32),
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct IndexSwap {
    pub indexes: (String, String),
}

impl KindWithContent {
    pub fn as_kind(&self) -> Kind {
        match self {
            KindWithContent::DocumentAdditionOrUpdate { .. } => Kind::DocumentAdditionOrUpdate,
            KindWithContent::DocumentEdition { .. } => Kind::DocumentEdition,
            KindWithContent::DocumentDeletion { .. } => Kind::DocumentDeletion,
            KindWithContent::DocumentDeletionByFilter { .. } => Kind::DocumentDeletion,
            KindWithContent::DocumentClear { .. } => Kind::DocumentDeletion,
            KindWithContent::SettingsUpdate { .. } => Kind::SettingsUpdate,
            KindWithContent::IndexCreation { .. } => Kind::IndexCreation,
            KindWithContent::IndexDeletion { .. } => Kind::IndexDeletion,
            KindWithContent::IndexUpdate { .. } => Kind::IndexUpdate,
            KindWithContent::IndexSwap { .. } => Kind::IndexSwap,
            KindWithContent::TaskCancelation { .. } => Kind::TaskCancelation,
            KindWithContent::TaskDeletion { .. } => Kind::TaskDeletion,
            KindWithContent::DumpCreation { .. } => Kind::DumpCreation,
            KindWithContent::SnapshotCreation => Kind::SnapshotCreation,
            KindWithContent::UpgradeDatabase { .. } => Kind::UpgradeDatabase,
        }
    }

    pub fn indexes(&self) -> Vec<&str> {
        use KindWithContent::*;

        match self {
            DumpCreation { .. }
            | SnapshotCreation
            | TaskCancelation { .. }
            | TaskDeletion { .. }
            | UpgradeDatabase { .. } => vec![],
            DocumentAdditionOrUpdate { index_uid, .. }
            | DocumentEdition { index_uid, .. }
            | DocumentDeletion { index_uid, .. }
            | DocumentDeletionByFilter { index_uid, .. }
            | DocumentClear { index_uid }
            | SettingsUpdate { index_uid, .. }
            | IndexCreation { index_uid, .. }
            | IndexUpdate { index_uid, .. }
            | IndexDeletion { index_uid } => vec![index_uid],
            IndexSwap { swaps } => {
                let mut indexes = HashSet::<&str>::default();
                for swap in swaps {
                    indexes.insert(swap.indexes.0.as_str());
                    indexes.insert(swap.indexes.1.as_str());
                }
                indexes.into_iter().collect()
            }
        }
    }

    /// Returns the default `Details` that correspond to this `KindWithContent`,
    /// `None` if it cannot be generated.
    pub fn default_details(&self) -> Option<Details> {
        match self {
            KindWithContent::DocumentAdditionOrUpdate { documents_count, .. } => {
                Some(Details::DocumentAdditionOrUpdate {
                    received_documents: *documents_count,
                    indexed_documents: None,
                })
            }
            KindWithContent::DocumentEdition { index_uid: _, filter_expr, context, function } => {
                Some(Details::DocumentEdition {
                    deleted_documents: None,
                    edited_documents: None,
                    original_filter: filter_expr.as_ref().map(|v| v.to_string()),
                    context: context.clone(),
                    function: function.clone(),
                })
            }
            KindWithContent::DocumentDeletion { index_uid: _, documents_ids } => {
                Some(Details::DocumentDeletion {
                    provided_ids: documents_ids.len(),
                    deleted_documents: None,
                })
            }
            KindWithContent::DocumentDeletionByFilter { index_uid: _, filter_expr } => {
                Some(Details::DocumentDeletionByFilter {
                    original_filter: filter_expr.to_string(),
                    deleted_documents: None,
                })
            }
            KindWithContent::DocumentClear { .. } | KindWithContent::IndexDeletion { .. } => {
                Some(Details::ClearAll { deleted_documents: None })
            }
            KindWithContent::SettingsUpdate { new_settings, .. } => {
                Some(Details::SettingsUpdate { settings: new_settings.clone() })
            }
            KindWithContent::IndexCreation { primary_key, .. }
            | KindWithContent::IndexUpdate { primary_key, .. } => {
                Some(Details::IndexInfo { primary_key: primary_key.clone() })
            }
            KindWithContent::IndexSwap { swaps } => {
                Some(Details::IndexSwap { swaps: swaps.clone() })
            }
            KindWithContent::TaskCancelation { query, tasks } => Some(Details::TaskCancelation {
                matched_tasks: tasks.len(),
                canceled_tasks: None,
                original_filter: query.clone(),
            }),
            KindWithContent::TaskDeletion { query, tasks } => Some(Details::TaskDeletion {
                matched_tasks: tasks.len(),
                deleted_tasks: None,
                original_filter: query.clone(),
            }),
            KindWithContent::DumpCreation { .. } => Some(Details::Dump { dump_uid: None }),
            KindWithContent::SnapshotCreation => None,
            KindWithContent::UpgradeDatabase { .. } => None,
        }
    }

    pub fn default_finished_details(&self) -> Option<Details> {
        match self {
            KindWithContent::DocumentAdditionOrUpdate { documents_count, .. } => {
                Some(Details::DocumentAdditionOrUpdate {
                    received_documents: *documents_count,
                    indexed_documents: Some(0),
                })
            }
            KindWithContent::DocumentEdition { index_uid: _, filter_expr, context, function } => {
                Some(Details::DocumentEdition {
                    deleted_documents: Some(0),
                    edited_documents: Some(0),
                    original_filter: filter_expr.as_ref().map(|v| v.to_string()),
                    context: context.clone(),
                    function: function.clone(),
                })
            }
            KindWithContent::DocumentDeletion { index_uid: _, documents_ids } => {
                Some(Details::DocumentDeletion {
                    provided_ids: documents_ids.len(),
                    deleted_documents: Some(0),
                })
            }
            KindWithContent::DocumentDeletionByFilter { index_uid: _, filter_expr } => {
                Some(Details::DocumentDeletionByFilter {
                    original_filter: filter_expr.to_string(),
                    deleted_documents: Some(0),
                })
            }
            KindWithContent::DocumentClear { .. } => {
                Some(Details::ClearAll { deleted_documents: None })
            }
            KindWithContent::SettingsUpdate { new_settings, .. } => {
                Some(Details::SettingsUpdate { settings: new_settings.clone() })
            }
            KindWithContent::IndexDeletion { .. } => None,
            KindWithContent::IndexCreation { primary_key, .. }
            | KindWithContent::IndexUpdate { primary_key, .. } => {
                Some(Details::IndexInfo { primary_key: primary_key.clone() })
            }
            KindWithContent::IndexSwap { .. } => {
                todo!()
            }
            KindWithContent::TaskCancelation { query, tasks } => Some(Details::TaskCancelation {
                matched_tasks: tasks.len(),
                canceled_tasks: Some(0),
                original_filter: query.clone(),
            }),
            KindWithContent::TaskDeletion { query, tasks } => Some(Details::TaskDeletion {
                matched_tasks: tasks.len(),
                deleted_tasks: Some(0),
                original_filter: query.clone(),
            }),
            KindWithContent::DumpCreation { .. } => Some(Details::Dump { dump_uid: None }),
            KindWithContent::SnapshotCreation => None,
            KindWithContent::UpgradeDatabase { .. } => None,
        }
    }
}

impl From<&KindWithContent> for Option<Details> {
    fn from(kind: &KindWithContent) -> Self {
        match kind {
            KindWithContent::DocumentAdditionOrUpdate { documents_count, .. } => {
                Some(Details::DocumentAdditionOrUpdate {
                    received_documents: *documents_count,
                    indexed_documents: None,
                })
            }
            KindWithContent::DocumentEdition { .. } => None,
            KindWithContent::DocumentDeletion { .. } => None,
            KindWithContent::DocumentDeletionByFilter { .. } => None,
            KindWithContent::DocumentClear { .. } => None,
            KindWithContent::SettingsUpdate { new_settings, .. } => {
                Some(Details::SettingsUpdate { settings: new_settings.clone() })
            }
            KindWithContent::IndexDeletion { .. } => None,
            KindWithContent::IndexCreation { primary_key, .. } => {
                Some(Details::IndexInfo { primary_key: primary_key.clone() })
            }
            KindWithContent::IndexUpdate { primary_key, .. } => {
                Some(Details::IndexInfo { primary_key: primary_key.clone() })
            }
            KindWithContent::IndexSwap { .. } => None,
            KindWithContent::TaskCancelation { query, tasks } => Some(Details::TaskCancelation {
                matched_tasks: tasks.len(),
                canceled_tasks: None,
                original_filter: query.clone(),
            }),
            KindWithContent::TaskDeletion { query, tasks } => Some(Details::TaskDeletion {
                matched_tasks: tasks.len(),
                deleted_tasks: None,
                original_filter: query.clone(),
            }),
            KindWithContent::DumpCreation { .. } => Some(Details::Dump { dump_uid: None }),
            KindWithContent::SnapshotCreation => None,
            KindWithContent::UpgradeDatabase { .. } => None,
        }
    }
}

/// The status of a task.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    Sequence,
    PartialOrd,
    Ord,
    ToSchema,
)]
#[schema(example = json!(Status::Processing))]
#[serde(rename_all = "camelCase")]
pub enum Status {
    Enqueued,
    Processing,
    Succeeded,
    Failed,
    Canceled,
}

impl Display for Status {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Status::Enqueued => write!(f, "enqueued"),
            Status::Processing => write!(f, "processing"),
            Status::Succeeded => write!(f, "succeeded"),
            Status::Failed => write!(f, "failed"),
            Status::Canceled => write!(f, "canceled"),
        }
    }
}

impl FromStr for Status {
    type Err = ParseTaskStatusError;

    fn from_str(status: &str) -> Result<Self, Self::Err> {
        if status.eq_ignore_ascii_case("enqueued") {
            Ok(Status::Enqueued)
        } else if status.eq_ignore_ascii_case("processing") {
            Ok(Status::Processing)
        } else if status.eq_ignore_ascii_case("succeeded") {
            Ok(Status::Succeeded)
        } else if status.eq_ignore_ascii_case("failed") {
            Ok(Status::Failed)
        } else if status.eq_ignore_ascii_case("canceled") {
            Ok(Status::Canceled)
        } else {
            Err(ParseTaskStatusError(status.to_owned()))
        }
    }
}

#[derive(Debug)]
pub struct ParseTaskStatusError(pub String);
impl fmt::Display for ParseTaskStatusError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "`{}` is not a valid task status. Available statuses are {}.",
            self.0,
            enum_iterator::all::<Status>()
                .map(|s| format!("`{s}`"))
                .collect::<Vec<String>>()
                .join(", ")
        )
    }
}
impl std::error::Error for ParseTaskStatusError {}

/// The type of the task.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    Sequence,
    PartialOrd,
    Ord,
    ToSchema,
)]
#[serde(rename_all = "camelCase")]
#[schema(rename_all = "camelCase", example = json!(enum_iterator::all::<Kind>().collect::<Vec<_>>()))]
pub enum Kind {
    DocumentAdditionOrUpdate,
    DocumentEdition,
    DocumentDeletion,
    SettingsUpdate,
    IndexCreation,
    IndexDeletion,
    IndexUpdate,
    IndexSwap,
    TaskCancelation,
    TaskDeletion,
    DumpCreation,
    SnapshotCreation,
    UpgradeDatabase,
}

impl Kind {
    pub fn related_to_one_index(&self) -> bool {
        match self {
            Kind::DocumentAdditionOrUpdate
            | Kind::DocumentEdition
            | Kind::DocumentDeletion
            | Kind::SettingsUpdate
            | Kind::IndexCreation
            | Kind::IndexDeletion
            | Kind::IndexUpdate => true,
            Kind::IndexSwap
            | Kind::TaskCancelation
            | Kind::TaskDeletion
            | Kind::DumpCreation
            | Kind::UpgradeDatabase
            | Kind::SnapshotCreation => false,
        }
    }
}
impl Display for Kind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Kind::DocumentAdditionOrUpdate => write!(f, "documentAdditionOrUpdate"),
            Kind::DocumentEdition => write!(f, "documentEdition"),
            Kind::DocumentDeletion => write!(f, "documentDeletion"),
            Kind::SettingsUpdate => write!(f, "settingsUpdate"),
            Kind::IndexCreation => write!(f, "indexCreation"),
            Kind::IndexDeletion => write!(f, "indexDeletion"),
            Kind::IndexUpdate => write!(f, "indexUpdate"),
            Kind::IndexSwap => write!(f, "indexSwap"),
            Kind::TaskCancelation => write!(f, "taskCancelation"),
            Kind::TaskDeletion => write!(f, "taskDeletion"),
            Kind::DumpCreation => write!(f, "dumpCreation"),
            Kind::SnapshotCreation => write!(f, "snapshotCreation"),
            Kind::UpgradeDatabase => write!(f, "upgradeDatabase"),
        }
    }
}
impl FromStr for Kind {
    type Err = ParseTaskKindError;

    fn from_str(kind: &str) -> Result<Self, Self::Err> {
        if kind.eq_ignore_ascii_case("indexCreation") {
            Ok(Kind::IndexCreation)
        } else if kind.eq_ignore_ascii_case("indexUpdate") {
            Ok(Kind::IndexUpdate)
        } else if kind.eq_ignore_ascii_case("indexSwap") {
            Ok(Kind::IndexSwap)
        } else if kind.eq_ignore_ascii_case("indexDeletion") {
            Ok(Kind::IndexDeletion)
        } else if kind.eq_ignore_ascii_case("documentAdditionOrUpdate") {
            Ok(Kind::DocumentAdditionOrUpdate)
        } else if kind.eq_ignore_ascii_case("documentEdition") {
            Ok(Kind::DocumentEdition)
        } else if kind.eq_ignore_ascii_case("documentDeletion") {
            Ok(Kind::DocumentDeletion)
        } else if kind.eq_ignore_ascii_case("settingsUpdate") {
            Ok(Kind::SettingsUpdate)
        } else if kind.eq_ignore_ascii_case("taskCancelation") {
            Ok(Kind::TaskCancelation)
        } else if kind.eq_ignore_ascii_case("taskDeletion") {
            Ok(Kind::TaskDeletion)
        } else if kind.eq_ignore_ascii_case("dumpCreation") {
            Ok(Kind::DumpCreation)
        } else if kind.eq_ignore_ascii_case("snapshotCreation") {
            Ok(Kind::SnapshotCreation)
        } else {
            Err(ParseTaskKindError(kind.to_owned()))
        }
    }
}

#[derive(Debug)]
pub struct ParseTaskKindError(pub String);
impl fmt::Display for ParseTaskKindError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "`{}` is not a valid task type. Available types are {}.",
            self.0,
            enum_iterator::all::<Kind>()
                .map(|k| format!(
                    "`{}`",
                    // by default serde is going to insert `"` around the value.
                    serde_json::to_string(&k).unwrap().trim_matches('"')
                ))
                .collect::<Vec<String>>()
                .join(", ")
        )
    }
}
impl std::error::Error for ParseTaskKindError {}

#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub enum Details {
    DocumentAdditionOrUpdate {
        received_documents: u64,
        indexed_documents: Option<u64>,
    },
    SettingsUpdate {
        settings: Box<Settings<Unchecked>>,
    },
    IndexInfo {
        primary_key: Option<String>,
    },
    DocumentDeletion {
        provided_ids: usize,
        deleted_documents: Option<u64>,
    },
    DocumentDeletionByFilter {
        original_filter: String,
        deleted_documents: Option<u64>,
    },
    DocumentEdition {
        deleted_documents: Option<u64>,
        edited_documents: Option<u64>,
        original_filter: Option<String>,
        context: Option<Object>,
        function: String,
    },
    ClearAll {
        deleted_documents: Option<u64>,
    },
    TaskCancelation {
        matched_tasks: u64,
        canceled_tasks: Option<u64>,
        original_filter: String,
    },
    TaskDeletion {
        matched_tasks: u64,
        deleted_tasks: Option<u64>,
        original_filter: String,
    },
    Dump {
        dump_uid: Option<String>,
    },
    IndexSwap {
        swaps: Vec<IndexSwap>,
    },
    UpgradeDatabase {
        from: (usize, usize, usize),
    },
}

impl Details {
    pub fn to_failed(&self) -> Self {
        let mut details = self.clone();
        match &mut details {
            Self::DocumentAdditionOrUpdate { indexed_documents, .. } => {
                *indexed_documents = Some(0)
            }
            Self::DocumentEdition { edited_documents, .. } => *edited_documents = Some(0),
            Self::DocumentDeletion { deleted_documents, .. } => *deleted_documents = Some(0),
            Self::DocumentDeletionByFilter { deleted_documents, .. } => {
                *deleted_documents = Some(0)
            }
            Self::ClearAll { deleted_documents } => *deleted_documents = Some(0),
            Self::TaskCancelation { canceled_tasks, .. } => *canceled_tasks = Some(0),
            Self::TaskDeletion { deleted_tasks, .. } => *deleted_tasks = Some(0),
            Self::SettingsUpdate { .. }
            | Self::IndexInfo { .. }
            | Self::Dump { .. }
            | Self::UpgradeDatabase { .. }
            | Self::IndexSwap { .. } => (),
        }

        details
    }
}

/// Serialize a `time::Duration` as a best effort ISO 8601 while waiting for
/// https://github.com/time-rs/time/issues/378.
/// This code is a port of the old code of time that was removed in 0.2.
pub fn serialize_duration<S: Serializer>(
    duration: &Option<Duration>,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    match duration {
        Some(duration) => {
            // technically speaking, negative duration is not valid ISO 8601
            if duration.is_negative() {
                return serializer.serialize_none();
            }

            const SECS_PER_DAY: i64 = Duration::DAY.whole_seconds();
            let secs = duration.whole_seconds();
            let days = secs / SECS_PER_DAY;
            let secs = secs - days * SECS_PER_DAY;
            let hasdate = days != 0;
            let nanos = duration.subsec_nanoseconds();
            let hastime = (secs != 0 || nanos != 0) || !hasdate;

            // all the following unwrap can't fail
            let mut res = String::new();
            write!(&mut res, "P").unwrap();

            if hasdate {
                write!(&mut res, "{}D", days).unwrap();
            }

            const NANOS_PER_MILLI: i32 = Duration::MILLISECOND.subsec_nanoseconds();
            const NANOS_PER_MICRO: i32 = Duration::MICROSECOND.subsec_nanoseconds();

            if hastime {
                if nanos == 0 {
                    write!(&mut res, "T{}S", secs).unwrap();
                } else if nanos % NANOS_PER_MILLI == 0 {
                    write!(&mut res, "T{}.{:03}S", secs, nanos / NANOS_PER_MILLI).unwrap();
                } else if nanos % NANOS_PER_MICRO == 0 {
                    write!(&mut res, "T{}.{:06}S", secs, nanos / NANOS_PER_MICRO).unwrap();
                } else {
                    write!(&mut res, "T{}.{:09}S", secs, nanos).unwrap();
                }
            }

            serializer.serialize_str(&res)
        }
        None => serializer.serialize_none(),
    }
}

#[cfg(test)]
mod tests {
    use super::Details;
    use crate::heed::types::SerdeJson;
    use crate::heed::{BytesDecode, BytesEncode};

    #[test]
    fn bad_deser() {
        let details = Details::TaskDeletion {
            matched_tasks: 1,
            deleted_tasks: None,
            original_filter: "hello".to_owned(),
        };
        let serialised = SerdeJson::<Details>::bytes_encode(&details).unwrap();
        let deserialised = SerdeJson::<Details>::bytes_decode(&serialised).unwrap();
        meili_snap::snapshot!(format!("{:?}", details), @r###"TaskDeletion { matched_tasks: 1, deleted_tasks: None, original_filter: "hello" }"###);
        meili_snap::snapshot!(format!("{:?}", deserialised), @r###"TaskDeletion { matched_tasks: 1, deleted_tasks: None, original_filter: "hello" }"###);
    }
}
