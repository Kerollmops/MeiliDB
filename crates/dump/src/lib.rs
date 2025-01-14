#![allow(clippy::type_complexity)]
#![allow(clippy::wrong_self_convention)]

use meilisearch_types::batches::BatchId;
use meilisearch_types::error::ResponseError;
use meilisearch_types::keys::Key;
use meilisearch_types::milli::update::IndexDocumentsMethod;
use meilisearch_types::settings::Unchecked;
use meilisearch_types::tasks::{Details, IndexSwap, KindWithContent, Status, Task, TaskId};
use meilisearch_types::InstanceUid;
use roaring::RoaringBitmap;
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

mod error;
mod reader;
mod writer;

pub use error::Error;
pub use reader::{DumpReader, UpdateFile};
pub use writer::DumpWriter;

const CURRENT_DUMP_VERSION: Version = Version::V6;

type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Metadata {
    pub dump_version: Version,
    pub db_version: String,
    #[serde(with = "time::serde::rfc3339")]
    pub dump_date: OffsetDateTime,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IndexMetadata {
    pub uid: String,
    pub primary_key: Option<String>,
    #[serde(with = "time::serde::rfc3339")]
    pub created_at: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339")]
    pub updated_at: OffsetDateTime,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
pub enum Version {
    V1,
    V2,
    V3,
    V4,
    V5,
    V6,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskDump {
    pub uid: TaskId,
    // The batch ID were introduced in v1.12, everything prior to this version will be `None`.
    #[serde(default)]
    pub batch_uid: Option<BatchId>,
    #[serde(default)]
    pub index_uid: Option<String>,
    pub status: Status,
    #[serde(rename = "type")]
    pub kind: KindDump,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub canceled_by: Option<TaskId>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<Details>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<ResponseError>,

    #[serde(with = "time::serde::rfc3339")]
    pub enqueued_at: OffsetDateTime,
    #[serde(
        with = "time::serde::rfc3339::option",
        skip_serializing_if = "Option::is_none",
        default
    )]
    pub started_at: Option<OffsetDateTime>,
    #[serde(
        with = "time::serde::rfc3339::option",
        skip_serializing_if = "Option::is_none",
        default
    )]
    pub finished_at: Option<OffsetDateTime>,
}

// A `Kind` specific version made for the dump. If modified you may break the dump.
#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum KindDump {
    DocumentImport {
        primary_key: Option<String>,
        method: IndexDocumentsMethod,
        documents_count: u64,
        allow_index_creation: bool,
    },
    DocumentDeletion {
        documents_ids: Vec<String>,
    },
    DocumentClear,
    DocumentDeletionByFilter {
        filter: serde_json::Value,
    },
    DocumentEdition {
        filter: Option<serde_json::Value>,
        context: Option<serde_json::Map<String, serde_json::Value>>,
        function: String,
    },
    Settings {
        settings: Box<meilisearch_types::settings::Settings<Unchecked>>,
        is_deletion: bool,
        allow_index_creation: bool,
    },
    IndexDeletion,
    IndexCreation {
        primary_key: Option<String>,
    },
    IndexUpdate {
        primary_key: Option<String>,
    },
    IndexSwap {
        swaps: Vec<IndexSwap>,
    },
    TaskCancelation {
        query: String,
        tasks: RoaringBitmap,
    },
    TasksDeletion {
        query: String,
        tasks: RoaringBitmap,
    },
    DumpCreation {
        keys: Vec<Key>,
        instance_uid: Option<InstanceUid>,
    },
    SnapshotCreation,
}

impl From<Task> for TaskDump {
    fn from(task: Task) -> Self {
        TaskDump {
            uid: task.uid,
            batch_uid: task.batch_uid,
            index_uid: task.index_uid().map(|uid| uid.to_string()),
            status: task.status,
            kind: task.kind.into(),
            canceled_by: task.canceled_by,
            details: task.details,
            error: task.error,
            enqueued_at: task.enqueued_at,
            started_at: task.started_at,
            finished_at: task.finished_at,
        }
    }
}

impl From<KindWithContent> for KindDump {
    fn from(kind: KindWithContent) -> Self {
        match kind {
            KindWithContent::DocumentAdditionOrUpdate {
                primary_key,
                method,
                documents_count,
                allow_index_creation,
                ..
            } => KindDump::DocumentImport {
                primary_key,
                method,
                documents_count,
                allow_index_creation,
            },
            KindWithContent::DocumentDeletion { documents_ids, .. } => {
                KindDump::DocumentDeletion { documents_ids }
            }
            KindWithContent::DocumentDeletionByFilter { filter_expr, .. } => {
                KindDump::DocumentDeletionByFilter { filter: filter_expr }
            }
            KindWithContent::DocumentEdition { filter_expr, context, function, .. } => {
                KindDump::DocumentEdition { filter: filter_expr, context, function }
            }
            KindWithContent::DocumentClear { .. } => KindDump::DocumentClear,
            KindWithContent::SettingsUpdate {
                new_settings,
                is_deletion,
                allow_index_creation,
                ..
            } => KindDump::Settings { settings: new_settings, is_deletion, allow_index_creation },
            KindWithContent::IndexDeletion { .. } => KindDump::IndexDeletion,
            KindWithContent::IndexCreation { primary_key, .. } => {
                KindDump::IndexCreation { primary_key }
            }
            KindWithContent::IndexUpdate { primary_key, .. } => {
                KindDump::IndexUpdate { primary_key }
            }
            KindWithContent::IndexSwap { swaps } => KindDump::IndexSwap { swaps },
            KindWithContent::TaskCancelation { query, tasks } => {
                KindDump::TaskCancelation { query, tasks }
            }
            KindWithContent::TaskDeletion { query, tasks } => {
                KindDump::TasksDeletion { query, tasks }
            }
            KindWithContent::DumpCreation { keys, instance_uid } => {
                KindDump::DumpCreation { keys, instance_uid }
            }
            KindWithContent::SnapshotCreation => KindDump::SnapshotCreation,
        }
    }
}

#[cfg(test)]
pub(crate) mod test {
    use std::fs::File;
    use std::io::Seek;
    use std::str::FromStr;

    use big_s::S;
    use maplit::{btreemap, btreeset};
    use meilisearch_types::facet_values_sort::FacetValuesSort;
    use meilisearch_types::features::RuntimeTogglableFeatures;
    use meilisearch_types::index_uid_pattern::IndexUidPattern;
    use meilisearch_types::keys::{Action, Key};
    use meilisearch_types::milli;
    use meilisearch_types::milli::update::Setting;
    use meilisearch_types::settings::{Checked, FacetingSettings, Settings};
    use meilisearch_types::tasks::{Details, Status};
    use serde_json::{json, Map, Value};
    use time::macros::datetime;
    use uuid::Uuid;

    use crate::reader::Document;
    use crate::{DumpReader, DumpWriter, IndexMetadata, KindDump, TaskDump, Version};

    pub fn create_test_instance_uid() -> Uuid {
        Uuid::parse_str("9e15e977-f2ae-4761-943f-1eaf75fd736d").unwrap()
    }

    pub fn create_test_index_metadata() -> IndexMetadata {
        IndexMetadata {
            uid: S("doggo"),
            primary_key: None,
            created_at: datetime!(2022-11-20 12:00 UTC),
            updated_at: datetime!(2022-11-21 00:00 UTC),
        }
    }

    pub fn create_test_documents() -> Vec<Map<String, Value>> {
        vec![
            json!({ "id": 1, "race": "golden retriever", "name": "paul", "age": 4 })
                .as_object()
                .unwrap()
                .clone(),
            json!({ "id": 2, "race": "bernese mountain", "name": "tamo", "age": 6 })
                .as_object()
                .unwrap()
                .clone(),
            json!({ "id": 3, "race": "great pyrenees", "name": "patou", "age": 5 })
                .as_object()
                .unwrap()
                .clone(),
        ]
    }

    pub fn create_test_settings() -> Settings<Checked> {
        let settings = Settings {
            displayed_attributes: Setting::Set(vec![S("race"), S("name")]).into(),
            searchable_attributes: Setting::Set(vec![S("name"), S("race")]).into(),
            filterable_attributes: Setting::Set(btreeset! { S("race"), S("age") }),
            sortable_attributes: Setting::Set(btreeset! { S("age") }),
            ranking_rules: Setting::NotSet,
            stop_words: Setting::NotSet,
            non_separator_tokens: Setting::NotSet,
            separator_tokens: Setting::NotSet,
            dictionary: Setting::NotSet,
            synonyms: Setting::NotSet,
            distinct_attribute: Setting::NotSet,
            proximity_precision: Setting::NotSet,
            typo_tolerance: Setting::NotSet,
            faceting: Setting::Set(FacetingSettings {
                max_values_per_facet: Setting::Set(111),
                sort_facet_values_by: Setting::Set(
                    btreemap! { S("age") => FacetValuesSort::Count },
                ),
            }),
            pagination: Setting::NotSet,
            embedders: Setting::NotSet,
            search_cutoff_ms: Setting::NotSet,
            localized_attributes: Setting::NotSet,
            facet_search: Setting::NotSet,
            prefix_search: Setting::NotSet,
            _kind: std::marker::PhantomData,
        };
        settings.check()
    }

    pub fn create_test_tasks() -> Vec<(TaskDump, Option<Vec<Document>>)> {
        vec![
            (
                TaskDump {
                    uid: 0,
                    batch_uid: Some(0),
                    index_uid: Some(S("doggo")),
                    status: Status::Succeeded,
                    kind: KindDump::DocumentImport {
                        method: milli::update::IndexDocumentsMethod::UpdateDocuments,
                        allow_index_creation: true,
                        primary_key: Some(S("bone")),
                        documents_count: 12,
                    },
                    canceled_by: None,
                    details: Some(Details::DocumentAdditionOrUpdate {
                        received_documents: 12,
                        indexed_documents: Some(10),
                    }),
                    error: None,
                    enqueued_at: datetime!(2022-11-11 0:00 UTC),
                    started_at: Some(datetime!(2022-11-20 0:00 UTC)),
                    finished_at: Some(datetime!(2022-11-21 0:00 UTC)),
                },
                None,
            ),
            (
                TaskDump {
                    uid: 1,
                    batch_uid: None,
                    index_uid: Some(S("doggo")),
                    status: Status::Enqueued,
                    kind: KindDump::DocumentImport {
                        method: milli::update::IndexDocumentsMethod::UpdateDocuments,
                        allow_index_creation: true,
                        primary_key: None,
                        documents_count: 2,
                    },
                    canceled_by: None,
                    details: Some(Details::DocumentAdditionOrUpdate {
                        received_documents: 2,
                        indexed_documents: None,
                    }),
                    error: None,
                    enqueued_at: datetime!(2022-11-11 0:00 UTC),
                    started_at: None,
                    finished_at: None,
                },
                Some(vec![
                    json!({ "id": 4, "race": "leonberg" }).as_object().unwrap().clone(),
                    json!({ "id": 5, "race": "patou" }).as_object().unwrap().clone(),
                ]),
            ),
            (
                TaskDump {
                    uid: 5,
                    batch_uid: None,
                    index_uid: Some(S("catto")),
                    status: Status::Enqueued,
                    kind: KindDump::IndexDeletion,
                    canceled_by: None,
                    details: None,
                    error: None,
                    enqueued_at: datetime!(2022-11-15 0:00 UTC),
                    started_at: None,
                    finished_at: None,
                },
                None,
            ),
        ]
    }

    pub fn create_test_api_keys() -> Vec<Key> {
        vec![
            Key {
                description: Some(S("The main key to manage all the doggos")),
                name: Some(S("doggos_key")),
                uid: Uuid::from_str("9f8a34da-b6b2-42f0-939b-dbd4c3448655").unwrap(),
                actions: vec![Action::DocumentsAll],
                indexes: vec![IndexUidPattern::from_str("doggos").unwrap()],
                expires_at: Some(datetime!(4130-03-14 12:21 UTC)),
                created_at: datetime!(1960-11-15 0:00 UTC),
                updated_at: datetime!(2022-11-10 0:00 UTC),
            },
            Key {
                description: Some(S("The master key for everything and even the doggos")),
                name: Some(S("master_key")),
                uid: Uuid::from_str("4622f717-1c00-47bb-a494-39d76a49b591").unwrap(),
                actions: vec![Action::All],
                indexes: vec![IndexUidPattern::all()],
                expires_at: None,
                created_at: datetime!(0000-01-01 00:01 UTC),
                updated_at: datetime!(1964-05-04 17:25 UTC),
            },
            Key {
                description: Some(S("The useless key to for nothing nor the doggos")),
                name: Some(S("useless_key")),
                uid: Uuid::from_str("fb80b58b-0a34-412f-8ba7-1ce868f8ac5c").unwrap(),
                actions: vec![],
                indexes: vec![],
                expires_at: None,
                created_at: datetime!(400-02-29 0:00 UTC),
                updated_at: datetime!(1024-02-29 0:00 UTC),
            },
        ]
    }

    pub fn create_test_dump() -> File {
        let instance_uid = create_test_instance_uid();
        let dump = DumpWriter::new(Some(instance_uid)).unwrap();

        // ========== Adding an index
        let documents = create_test_documents();
        let settings = create_test_settings();

        let mut index = dump.create_index("doggos", &create_test_index_metadata()).unwrap();
        for document in &documents {
            index.push_document(document).unwrap();
        }
        index.flush().unwrap();
        index.settings(&settings).unwrap();

        // ========== pushing the task queue
        let tasks = create_test_tasks();

        let mut task_queue = dump.create_tasks_queue().unwrap();
        for (task, update_file) in &tasks {
            let mut update = task_queue.push_task(task).unwrap();
            if let Some(update_file) = update_file {
                for u in update_file {
                    update.push_document(u).unwrap();
                }
            }
        }
        task_queue.flush().unwrap();

        // ========== pushing the api keys
        let api_keys = create_test_api_keys();

        let mut keys = dump.create_keys().unwrap();
        for key in &api_keys {
            keys.push_key(key).unwrap();
        }
        keys.flush().unwrap();

        // ========== experimental features
        let features = create_test_features();

        dump.create_experimental_features(features).unwrap();

        // create the dump
        let mut file = tempfile::tempfile().unwrap();
        dump.persist_to(&mut file).unwrap();
        file.rewind().unwrap();

        file
    }

    fn create_test_features() -> RuntimeTogglableFeatures {
        RuntimeTogglableFeatures::default()
    }

    #[test]
    fn test_creating_and_read_dump() {
        let mut file = create_test_dump();
        let mut dump = DumpReader::open(&mut file).unwrap();

        // ==== checking the top level infos
        assert_eq!(dump.version(), Version::V6);
        assert!(dump.date().is_some());
        assert_eq!(dump.instance_uid().unwrap().unwrap(), create_test_instance_uid());

        // ==== checking the index
        let mut indexes = dump.indexes().unwrap();
        let mut index = indexes.next().unwrap().unwrap();
        assert!(indexes.next().is_none()); // there was only one index in the dump

        for (document, expected) in index.documents().unwrap().zip(create_test_documents()) {
            assert_eq!(document.unwrap(), expected);
        }
        assert_eq!(index.settings().unwrap(), create_test_settings());
        assert_eq!(index.metadata(), &create_test_index_metadata());

        drop(index);
        drop(indexes);

        // ==== checking the task queue
        for (task, expected) in dump.tasks().unwrap().zip(create_test_tasks()) {
            let (task, content_file) = task.unwrap();
            assert_eq!(task, expected.0);

            if let Some(expected_update) = expected.1 {
                assert!(
                    content_file.is_some(),
                    "A content file was expected for the task {}.",
                    expected.0.uid
                );
                let updates = content_file.unwrap().collect::<Result<Vec<_>, _>>().unwrap();
                assert_eq!(updates, expected_update);
            }
        }

        // ==== checking the keys
        for (key, expected) in dump.keys().unwrap().zip(create_test_api_keys()) {
            assert_eq!(key.unwrap(), expected);
        }

        // ==== checking the features
        let expected = create_test_features();
        assert_eq!(dump.features().unwrap().unwrap(), expected);
    }
}
