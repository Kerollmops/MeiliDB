use std::collections::BTreeMap;

use big_s::S;
use meili_snap::{json_string, snapshot};
use meilisearch_types::milli::index::IndexEmbeddingConfig;
use meilisearch_types::milli::update::IndexDocumentsMethod::*;
use meilisearch_types::milli::{self};
use meilisearch_types::settings::SettingEmbeddingSettings;
use meilisearch_types::tasks::{IndexSwap, KindWithContent};
use roaring::RoaringBitmap;

use crate::insta_snapshot::snapshot_index_scheduler;
use crate::test_utils::Breakpoint::*;
use crate::test_utils::{
    index_creation_task, read_json, replace_document_import_task, sample_documents,
};
use crate::IndexScheduler;

#[test]
fn insert_task_while_another_task_is_processing() {
    let (index_scheduler, mut handle) = IndexScheduler::test(true, vec![]);

    index_scheduler.register(index_creation_task("index_a", "id"), None, false).unwrap();
    snapshot!(snapshot_index_scheduler(&index_scheduler), name: "registered_the_first_task");

    handle.advance_till([Start, BatchCreated]);
    snapshot!(snapshot_index_scheduler(&index_scheduler), name: "after_batch_creation");

    // while the task is processing can we register another task?
    index_scheduler.register(index_creation_task("index_b", "id"), None, false).unwrap();
    snapshot!(snapshot_index_scheduler(&index_scheduler), name: "registered_the_second_task");

    index_scheduler
        .register(KindWithContent::IndexDeletion { index_uid: S("index_a") }, None, false)
        .unwrap();
    snapshot!(snapshot_index_scheduler(&index_scheduler), name: "registered_the_third_task");
}

#[test]
fn test_task_is_processing() {
    let (index_scheduler, mut handle) = IndexScheduler::test(true, vec![]);

    index_scheduler.register(index_creation_task("index_a", "id"), None, false).unwrap();
    snapshot!(snapshot_index_scheduler(&index_scheduler), name: "registered_a_task");

    handle.advance_till([Start, BatchCreated]);
    assert!(index_scheduler.is_task_processing().unwrap());
}

/// We send a lot of tasks but notify the tasks scheduler only once as
/// we send them very fast, we must make sure that they are all processed.
#[test]
fn process_tasks_inserted_without_new_signal() {
    let (index_scheduler, mut handle) = IndexScheduler::test(true, vec![]);

    index_scheduler
        .register(
            KindWithContent::IndexCreation { index_uid: S("doggos"), primary_key: None },
            None,
            false,
        )
        .unwrap();
    snapshot!(snapshot_index_scheduler(&index_scheduler), name: "registered_the_first_task");

    index_scheduler
        .register(
            KindWithContent::IndexCreation { index_uid: S("cattos"), primary_key: None },
            None,
            false,
        )
        .unwrap();
    snapshot!(snapshot_index_scheduler(&index_scheduler), name: "registered_the_second_task");

    index_scheduler
        .register(KindWithContent::IndexDeletion { index_uid: S("doggos") }, None, false)
        .unwrap();
    snapshot!(snapshot_index_scheduler(&index_scheduler), name: "registered_the_third_task");

    handle.advance_one_successful_batch();
    snapshot!(snapshot_index_scheduler(&index_scheduler), name: "processed_the_first_task");

    handle.advance_one_successful_batch();
    snapshot!(snapshot_index_scheduler(&index_scheduler), name: "processed_the_second_task");

    handle.advance_one_successful_batch();
    snapshot!(snapshot_index_scheduler(&index_scheduler), name: "processed_the_third_task");
}

#[test]
fn process_tasks_without_autobatching() {
    let (index_scheduler, mut handle) = IndexScheduler::test(false, vec![]);

    index_scheduler
        .register(
            KindWithContent::IndexCreation { index_uid: S("doggos"), primary_key: None },
            None,
            false,
        )
        .unwrap();
    snapshot!(snapshot_index_scheduler(&index_scheduler), name: "registered_the_first_task");

    index_scheduler
        .register(KindWithContent::DocumentClear { index_uid: S("doggos") }, None, false)
        .unwrap();
    snapshot!(snapshot_index_scheduler(&index_scheduler), name: "registered_the_second_task");

    index_scheduler
        .register(KindWithContent::DocumentClear { index_uid: S("doggos") }, None, false)
        .unwrap();
    snapshot!(snapshot_index_scheduler(&index_scheduler), name: "registered_the_third_task");

    index_scheduler
        .register(KindWithContent::DocumentClear { index_uid: S("doggos") }, None, false)
        .unwrap();
    snapshot!(snapshot_index_scheduler(&index_scheduler), name: "registered_the_fourth_task");

    handle.advance_one_successful_batch();
    snapshot!(snapshot_index_scheduler(&index_scheduler), name: "first");

    handle.advance_one_successful_batch();
    snapshot!(snapshot_index_scheduler(&index_scheduler), name: "second");

    handle.advance_one_successful_batch();
    snapshot!(snapshot_index_scheduler(&index_scheduler), name: "third");

    handle.advance_one_successful_batch();
    snapshot!(snapshot_index_scheduler(&index_scheduler), name: "fourth");
}

#[test]
fn task_deletion_undeleteable() {
    let (index_scheduler, mut handle) = IndexScheduler::test(true, vec![]);

    let (file0, documents_count0) = sample_documents(&index_scheduler, 0, 0);
    let (file1, documents_count1) = sample_documents(&index_scheduler, 1, 1);
    file0.persist().unwrap();
    file1.persist().unwrap();

    let to_enqueue = [
        index_creation_task("catto", "mouse"),
        replace_document_import_task("catto", None, 0, documents_count0),
        replace_document_import_task("doggo", Some("bone"), 1, documents_count1),
    ];

    for task in to_enqueue {
        let _ = index_scheduler.register(task, None, false).unwrap();
        index_scheduler.assert_internally_consistent();
    }

    // here we have registered all the tasks, but the index scheduler
    // has not progressed at all
    snapshot!(snapshot_index_scheduler(&index_scheduler), name: "initial_tasks_enqueued");

    index_scheduler
        .register(
            KindWithContent::TaskDeletion {
                query: "test_query".to_owned(),
                tasks: RoaringBitmap::from_iter([0, 1]),
            },
            None,
            false,
        )
        .unwrap();
    // again, no progress made at all, but one more task is registered
    snapshot!(snapshot_index_scheduler(&index_scheduler), name: "task_deletion_enqueued");

    // now we create the first batch
    handle.advance_till([Start, BatchCreated]);

    // the task deletion should now be "processing"
    snapshot!(snapshot_index_scheduler(&index_scheduler), name: "task_deletion_processing");

    handle.advance_till([InsideProcessBatch, ProcessBatchSucceeded, AfterProcessing]);
    // after the task deletion is processed, no task should actually have been deleted,
    // because the tasks with ids 0 and 1 were still "enqueued", and thus undeleteable
    // the "task deletion" task should be marked as "succeeded" and, in its details, the
    // number of deleted tasks should be 0
    snapshot!(snapshot_index_scheduler(&index_scheduler), name: "task_deletion_done");
}

#[test]
fn task_deletion_deleteable() {
    let (index_scheduler, mut handle) = IndexScheduler::test(true, vec![]);

    let (file0, documents_count0) = sample_documents(&index_scheduler, 0, 0);
    let (file1, documents_count1) = sample_documents(&index_scheduler, 1, 1);
    file0.persist().unwrap();
    file1.persist().unwrap();

    let to_enqueue = [
        replace_document_import_task("catto", None, 0, documents_count0),
        replace_document_import_task("doggo", Some("bone"), 1, documents_count1),
    ];

    for task in to_enqueue {
        let _ = index_scheduler.register(task, None, false).unwrap();
        index_scheduler.assert_internally_consistent();
    }
    snapshot!(snapshot_index_scheduler(&index_scheduler), name: "initial_tasks_enqueued");

    handle.advance_one_successful_batch();
    // first addition of documents should be successful
    snapshot!(snapshot_index_scheduler(&index_scheduler), name: "initial_tasks_processed");

    // Now we delete the first task
    index_scheduler
        .register(
            KindWithContent::TaskDeletion {
                query: "test_query".to_owned(),
                tasks: RoaringBitmap::from_iter([0]),
            },
            None,
            false,
        )
        .unwrap();
    snapshot!(snapshot_index_scheduler(&index_scheduler), name: "after_registering_the_task_deletion");

    handle.advance_one_successful_batch();
    snapshot!(snapshot_index_scheduler(&index_scheduler), name: "task_deletion_processed");
}

#[test]
fn task_deletion_delete_same_task_twice() {
    let (index_scheduler, mut handle) = IndexScheduler::test(true, vec![]);

    let (file0, documents_count0) = sample_documents(&index_scheduler, 0, 0);
    let (file1, documents_count1) = sample_documents(&index_scheduler, 1, 1);
    file0.persist().unwrap();
    file1.persist().unwrap();

    let to_enqueue = [
        replace_document_import_task("catto", None, 0, documents_count0),
        replace_document_import_task("doggo", Some("bone"), 1, documents_count1),
    ];

    for task in to_enqueue {
        let _ = index_scheduler.register(task, None, false).unwrap();
        index_scheduler.assert_internally_consistent();
    }
    snapshot!(snapshot_index_scheduler(&index_scheduler), name: "initial_tasks_enqueued");

    handle.advance_one_successful_batch();
    // first addition of documents should be successful
    snapshot!(snapshot_index_scheduler(&index_scheduler), name: "initial_tasks_processed");

    // Now we delete the first task multiple times in a row
    for _ in 0..2 {
        index_scheduler
            .register(
                KindWithContent::TaskDeletion {
                    query: "test_query".to_owned(),
                    tasks: RoaringBitmap::from_iter([0]),
                },
                None,
                false,
            )
            .unwrap();
        index_scheduler.assert_internally_consistent();
    }
    handle.advance_one_successful_batch();

    snapshot!(snapshot_index_scheduler(&index_scheduler), name: "task_deletion_processed");
}

#[test]
fn document_addition_and_index_deletion() {
    let (index_scheduler, mut handle) = IndexScheduler::test(true, vec![]);

    let content = r#"
        {
            "id": 1,
            "doggo": "bob"
        }"#;

    index_scheduler
        .register(
            KindWithContent::IndexCreation { index_uid: S("doggos"), primary_key: None },
            None,
            false,
        )
        .unwrap();
    snapshot!(snapshot_index_scheduler(&index_scheduler), name: "registered_the_first_task");

    let (uuid, mut file) = index_scheduler.queue.create_update_file_with_uuid(0).unwrap();
    let documents_count = read_json(content.as_bytes(), &mut file).unwrap();
    file.persist().unwrap();
    index_scheduler
        .register(
            KindWithContent::DocumentAdditionOrUpdate {
                index_uid: S("doggos"),
                primary_key: Some(S("id")),
                method: ReplaceDocuments,
                content_file: uuid,
                documents_count,
                allow_index_creation: true,
            },
            None,
            false,
        )
        .unwrap();
    snapshot!(snapshot_index_scheduler(&index_scheduler), name: "registered_the_second_task");

    index_scheduler
        .register(KindWithContent::IndexDeletion { index_uid: S("doggos") }, None, false)
        .unwrap();
    snapshot!(snapshot_index_scheduler(&index_scheduler), name: "registered_the_third_task");

    handle.advance_one_successful_batch(); // The index creation.
    snapshot!(snapshot_index_scheduler(&index_scheduler), name: "before_index_creation");
    handle.advance_one_successful_batch(); // // after the execution of the two tasks in a single batch.
    snapshot!(snapshot_index_scheduler(&index_scheduler), name: "both_task_succeeded");
}

#[test]
fn do_not_batch_task_of_different_indexes() {
    let (index_scheduler, mut handle) = IndexScheduler::test(true, vec![]);
    let index_names = ["doggos", "cattos", "girafos"];

    for name in index_names {
        index_scheduler
            .register(
                KindWithContent::IndexCreation { index_uid: name.to_string(), primary_key: None },
                None,
                false,
            )
            .unwrap();
        index_scheduler.assert_internally_consistent();
    }

    for name in index_names {
        index_scheduler
            .register(KindWithContent::DocumentClear { index_uid: name.to_string() }, None, false)
            .unwrap();
        index_scheduler.assert_internally_consistent();
    }

    for _ in 0..(index_names.len() * 2) {
        handle.advance_one_successful_batch();
        index_scheduler.assert_internally_consistent();
    }

    snapshot!(snapshot_index_scheduler(&index_scheduler), name: "all_tasks_processed");
}

#[test]
fn swap_indexes() {
    let (index_scheduler, mut handle) = IndexScheduler::test(true, vec![]);

    let to_enqueue = [
        index_creation_task("a", "id"),
        index_creation_task("b", "id"),
        index_creation_task("c", "id"),
        index_creation_task("d", "id"),
    ];

    for task in to_enqueue {
        let _ = index_scheduler.register(task, None, false).unwrap();
        index_scheduler.assert_internally_consistent();
    }

    handle.advance_one_successful_batch();
    snapshot!(snapshot_index_scheduler(&index_scheduler), name: "create_a");
    handle.advance_one_successful_batch();
    snapshot!(snapshot_index_scheduler(&index_scheduler), name: "create_b");
    handle.advance_one_successful_batch();
    snapshot!(snapshot_index_scheduler(&index_scheduler), name: "create_c");
    handle.advance_one_successful_batch();
    snapshot!(snapshot_index_scheduler(&index_scheduler), name: "create_d");

    index_scheduler
        .register(
            KindWithContent::IndexSwap {
                swaps: vec![
                    IndexSwap { indexes: ("a".to_owned(), "b".to_owned()) },
                    IndexSwap { indexes: ("c".to_owned(), "d".to_owned()) },
                ],
            },
            None,
            false,
        )
        .unwrap();
    snapshot!(snapshot_index_scheduler(&index_scheduler), name: "first_swap_registered");
    index_scheduler
        .register(
            KindWithContent::IndexSwap {
                swaps: vec![IndexSwap { indexes: ("a".to_owned(), "c".to_owned()) }],
            },
            None,
            false,
        )
        .unwrap();
    snapshot!(snapshot_index_scheduler(&index_scheduler), name: "two_swaps_registered");

    handle.advance_one_successful_batch();
    snapshot!(snapshot_index_scheduler(&index_scheduler), name: "first_swap_processed");

    handle.advance_one_successful_batch();
    snapshot!(snapshot_index_scheduler(&index_scheduler), name: "second_swap_processed");

    index_scheduler.register(KindWithContent::IndexSwap { swaps: vec![] }, None, false).unwrap();
    handle.advance_one_successful_batch();
    snapshot!(snapshot_index_scheduler(&index_scheduler), name: "third_empty_swap_processed");
}

#[test]
fn swap_indexes_errors() {
    let (index_scheduler, mut handle) = IndexScheduler::test(true, vec![]);

    let to_enqueue = [
        index_creation_task("a", "id"),
        index_creation_task("b", "id"),
        index_creation_task("c", "id"),
        index_creation_task("d", "id"),
    ];

    for task in to_enqueue {
        let _ = index_scheduler.register(task, None, false).unwrap();
        index_scheduler.assert_internally_consistent();
    }
    handle.advance_n_successful_batches(4);
    snapshot!(snapshot_index_scheduler(&index_scheduler), name: "after_the_index_creation");

    let first_snap = snapshot_index_scheduler(&index_scheduler);
    snapshot!(first_snap, name: "initial_tasks_processed");

    let err = index_scheduler
        .register(
            KindWithContent::IndexSwap {
                swaps: vec![
                    IndexSwap { indexes: ("a".to_owned(), "b".to_owned()) },
                    IndexSwap { indexes: ("b".to_owned(), "a".to_owned()) },
                ],
            },
            None,
            false,
        )
        .unwrap_err();
    snapshot!(format!("{err}"), @"Indexes must be declared only once during a swap. `a`, `b` were specified several times.");

    let second_snap = snapshot_index_scheduler(&index_scheduler);
    assert_eq!(first_snap, second_snap);

    // Index `e` does not exist, but we don't check its existence yet
    index_scheduler
        .register(
            KindWithContent::IndexSwap {
                swaps: vec![
                    IndexSwap { indexes: ("a".to_owned(), "b".to_owned()) },
                    IndexSwap { indexes: ("c".to_owned(), "e".to_owned()) },
                    IndexSwap { indexes: ("d".to_owned(), "f".to_owned()) },
                ],
            },
            None,
            false,
        )
        .unwrap();
    handle.advance_one_failed_batch();
    // Now the first swap should have an error message saying `e` and `f` do not exist
    snapshot!(snapshot_index_scheduler(&index_scheduler), name: "first_swap_failed");
}

#[test]
fn document_addition_and_index_deletion_on_unexisting_index() {
    let (index_scheduler, mut handle) = IndexScheduler::test(true, vec![]);

    let content = r#"
        {
            "id": 1,
            "doggo": "bob"
        }"#;

    let (uuid, mut file) = index_scheduler.queue.create_update_file_with_uuid(0).unwrap();
    let documents_count = read_json(content.as_bytes(), &mut file).unwrap();
    file.persist().unwrap();
    index_scheduler
        .register(
            KindWithContent::DocumentAdditionOrUpdate {
                index_uid: S("doggos"),
                primary_key: Some(S("id")),
                method: ReplaceDocuments,
                content_file: uuid,
                documents_count,
                allow_index_creation: true,
            },
            None,
            false,
        )
        .unwrap();
    index_scheduler
        .register(KindWithContent::IndexDeletion { index_uid: S("doggos") }, None, false)
        .unwrap();

    snapshot!(snapshot_index_scheduler(&index_scheduler));

    handle.advance_n_successful_batches(1);

    snapshot!(snapshot_index_scheduler(&index_scheduler));
}

#[test]
fn cancel_enqueued_task() {
    let (index_scheduler, mut handle) = IndexScheduler::test(true, vec![]);

    let (file0, documents_count0) = sample_documents(&index_scheduler, 0, 0);
    file0.persist().unwrap();

    let to_enqueue = [
        replace_document_import_task("catto", None, 0, documents_count0),
        KindWithContent::TaskCancelation {
            query: "test_query".to_owned(),
            tasks: RoaringBitmap::from_iter([0]),
        },
    ];
    for task in to_enqueue {
        let _ = index_scheduler.register(task, None, false).unwrap();
        index_scheduler.assert_internally_consistent();
    }

    snapshot!(snapshot_index_scheduler(&index_scheduler), name: "initial_tasks_enqueued");
    handle.advance_one_successful_batch();
    snapshot!(snapshot_index_scheduler(&index_scheduler), name: "cancel_processed");
}

#[test]
fn cancel_succeeded_task() {
    let (index_scheduler, mut handle) = IndexScheduler::test(true, vec![]);

    let (file0, documents_count0) = sample_documents(&index_scheduler, 0, 0);
    file0.persist().unwrap();

    let _ = index_scheduler
        .register(replace_document_import_task("catto", None, 0, documents_count0), None, false)
        .unwrap();
    snapshot!(snapshot_index_scheduler(&index_scheduler), name: "registered_the_first_task");

    handle.advance_one_successful_batch();
    snapshot!(snapshot_index_scheduler(&index_scheduler), name: "initial_task_processed");

    index_scheduler
        .register(
            KindWithContent::TaskCancelation {
                query: "test_query".to_owned(),
                tasks: RoaringBitmap::from_iter([0]),
            },
            None,
            false,
        )
        .unwrap();

    handle.advance_one_successful_batch();
    snapshot!(snapshot_index_scheduler(&index_scheduler), name: "cancel_processed");
}

#[test]
fn cancel_processing_task() {
    let (index_scheduler, mut handle) = IndexScheduler::test(true, vec![]);

    let (file0, documents_count0) = sample_documents(&index_scheduler, 0, 0);
    file0.persist().unwrap();

    let _ = index_scheduler
        .register(replace_document_import_task("catto", None, 0, documents_count0), None, false)
        .unwrap();
    snapshot!(snapshot_index_scheduler(&index_scheduler), name: "registered_the_first_task");

    handle.advance_till([Start, BatchCreated, InsideProcessBatch]);
    snapshot!(snapshot_index_scheduler(&index_scheduler), name: "initial_task_processing");

    index_scheduler
        .register(
            KindWithContent::TaskCancelation {
                query: "test_query".to_owned(),
                tasks: RoaringBitmap::from_iter([0]),
            },
            None,
            false,
        )
        .unwrap();

    snapshot!(snapshot_index_scheduler(&index_scheduler), name: "cancel_task_registered");
    // Now we check that we can reach the AbortedIndexation error handling
    handle.advance_till([AbortedIndexation]);
    snapshot!(snapshot_index_scheduler(&index_scheduler), name: "aborted_indexation");

    // handle.advance_till([Start, BatchCreated, BeforeProcessing, AfterProcessing]);
    handle.advance_one_successful_batch();
    snapshot!(snapshot_index_scheduler(&index_scheduler), name: "cancel_processed");
}

#[test]
fn cancel_mix_of_tasks() {
    let (index_scheduler, mut handle) = IndexScheduler::test(true, vec![]);

    let (file0, documents_count0) = sample_documents(&index_scheduler, 0, 0);
    file0.persist().unwrap();
    let (file1, documents_count1) = sample_documents(&index_scheduler, 1, 1);
    file1.persist().unwrap();
    let (file2, documents_count2) = sample_documents(&index_scheduler, 2, 2);
    file2.persist().unwrap();

    let to_enqueue = [
        replace_document_import_task("catto", None, 0, documents_count0),
        replace_document_import_task("beavero", None, 1, documents_count1),
        replace_document_import_task("wolfo", None, 2, documents_count2),
    ];
    for task in to_enqueue {
        let _ = index_scheduler.register(task, None, false).unwrap();
        index_scheduler.assert_internally_consistent();
    }
    handle.advance_one_successful_batch();
    snapshot!(snapshot_index_scheduler(&index_scheduler), name: "first_task_processed");

    handle.advance_till([Start, BatchCreated, InsideProcessBatch]);
    index_scheduler
        .register(
            KindWithContent::TaskCancelation {
                query: "test_query".to_owned(),
                tasks: RoaringBitmap::from_iter([0, 1, 2]),
            },
            None,
            false,
        )
        .unwrap();
    snapshot!(snapshot_index_scheduler(&index_scheduler), name: "processing_second_task_cancel_enqueued");

    handle.advance_till([AbortedIndexation]);
    snapshot!(snapshot_index_scheduler(&index_scheduler), name: "aborted_indexation");

    handle.advance_one_successful_batch();
    snapshot!(snapshot_index_scheduler(&index_scheduler), name: "cancel_processed");
}

#[test]
fn test_settings_update() {
    use meilisearch_types::settings::{Settings, Unchecked};
    use milli::update::Setting;

    let (index_scheduler, mut handle) = IndexScheduler::test(true, vec![]);

    let mut new_settings: Box<Settings<Unchecked>> = Box::default();
    let mut embedders = BTreeMap::default();
    let embedding_settings = milli::vector::settings::EmbeddingSettings {
        source: Setting::Set(milli::vector::settings::EmbedderSource::Rest),
        api_key: Setting::Set(S("My super secret")),
        url: Setting::Set(S("http://localhost:7777")),
        dimensions: Setting::Set(4),
        request: Setting::Set(serde_json::json!("{{text}}")),
        response: Setting::Set(serde_json::json!("{{embedding}}")),
        ..Default::default()
    };
    embedders
        .insert(S("default"), SettingEmbeddingSettings { inner: Setting::Set(embedding_settings) });
    new_settings.embedders = Setting::Set(embedders);

    index_scheduler
        .register(
            KindWithContent::SettingsUpdate {
                index_uid: S("doggos"),
                new_settings,
                is_deletion: false,
                allow_index_creation: true,
            },
            None,
            false,
        )
        .unwrap();
    index_scheduler.assert_internally_consistent();

    snapshot!(snapshot_index_scheduler(&index_scheduler), name: "after_registering_settings_task");

    {
        let rtxn = index_scheduler.read_txn().unwrap();
        let task = index_scheduler.queue.tasks.get_task(&rtxn, 0).unwrap().unwrap();
        let task = meilisearch_types::task_view::TaskView::from_task(&task);
        insta::assert_json_snapshot!(task.details);
    }

    handle.advance_n_successful_batches(1);
    snapshot!(snapshot_index_scheduler(&index_scheduler), name: "settings_update_processed");

    {
        let rtxn = index_scheduler.read_txn().unwrap();
        let task = index_scheduler.queue.tasks.get_task(&rtxn, 0).unwrap().unwrap();
        let task = meilisearch_types::task_view::TaskView::from_task(&task);
        insta::assert_json_snapshot!(task.details);
    }

    // has everything being pushed successfully in milli?
    let index = index_scheduler.index("doggos").unwrap();
    let rtxn = index.read_txn().unwrap();

    let configs = index.embedding_configs(&rtxn).unwrap();
    let IndexEmbeddingConfig { name, config, user_provided } = configs.first().unwrap();
    insta::assert_snapshot!(name, @"default");
    insta::assert_debug_snapshot!(user_provided, @"RoaringBitmap<[]>");
    insta::assert_json_snapshot!(config.embedder_options);
}

#[test]
fn simple_new() {
    crate::IndexScheduler::test(true, vec![]);
}

#[test]
fn basic_get_stats() {
    let (index_scheduler, mut handle) = IndexScheduler::test(true, vec![]);

    let kind = index_creation_task("catto", "mouse");
    let _task = index_scheduler.register(kind, None, false).unwrap();
    let kind = index_creation_task("doggo", "sheep");
    let _task = index_scheduler.register(kind, None, false).unwrap();
    let kind = index_creation_task("whalo", "fish");
    let _task = index_scheduler.register(kind, None, false).unwrap();

    snapshot!(json_string!(index_scheduler.get_stats().unwrap()), @r#"
    {
      "indexes": {
        "catto": 1,
        "doggo": 1,
        "whalo": 1
      },
      "statuses": {
        "canceled": 0,
        "enqueued": 3,
        "failed": 0,
        "processing": 0,
        "succeeded": 0
      },
      "types": {
        "documentAdditionOrUpdate": 0,
        "documentDeletion": 0,
        "documentEdition": 0,
        "dumpCreation": 0,
        "indexCreation": 3,
        "indexDeletion": 0,
        "indexSwap": 0,
        "indexUpdate": 0,
        "settingsUpdate": 0,
        "snapshotCreation": 0,
        "taskCancelation": 0,
        "taskDeletion": 0,
        "upgradeDatabase": 0
      }
    }
    "#);

    handle.advance_till([Start, BatchCreated]);
    snapshot!(json_string!(index_scheduler.get_stats().unwrap()), @r#"
    {
      "indexes": {
        "catto": 1,
        "doggo": 1,
        "whalo": 1
      },
      "statuses": {
        "canceled": 0,
        "enqueued": 2,
        "failed": 0,
        "processing": 1,
        "succeeded": 0
      },
      "types": {
        "documentAdditionOrUpdate": 0,
        "documentDeletion": 0,
        "documentEdition": 0,
        "dumpCreation": 0,
        "indexCreation": 3,
        "indexDeletion": 0,
        "indexSwap": 0,
        "indexUpdate": 0,
        "settingsUpdate": 0,
        "snapshotCreation": 0,
        "taskCancelation": 0,
        "taskDeletion": 0,
        "upgradeDatabase": 0
      }
    }
    "#);

    handle.advance_till([
        InsideProcessBatch,
        InsideProcessBatch,
        ProcessBatchSucceeded,
        AfterProcessing,
        Start,
        BatchCreated,
    ]);
    snapshot!(json_string!(index_scheduler.get_stats().unwrap()), @r#"
    {
      "indexes": {
        "catto": 1,
        "doggo": 1,
        "whalo": 1
      },
      "statuses": {
        "canceled": 0,
        "enqueued": 1,
        "failed": 0,
        "processing": 1,
        "succeeded": 1
      },
      "types": {
        "documentAdditionOrUpdate": 0,
        "documentDeletion": 0,
        "documentEdition": 0,
        "dumpCreation": 0,
        "indexCreation": 3,
        "indexDeletion": 0,
        "indexSwap": 0,
        "indexUpdate": 0,
        "settingsUpdate": 0,
        "snapshotCreation": 0,
        "taskCancelation": 0,
        "taskDeletion": 0,
        "upgradeDatabase": 0
      }
    }
    "#);

    // now we make one more batch, the started_at field of the new tasks will be past `second_start_time`
    handle.advance_till([
        InsideProcessBatch,
        InsideProcessBatch,
        ProcessBatchSucceeded,
        AfterProcessing,
        Start,
        BatchCreated,
    ]);
    snapshot!(json_string!(index_scheduler.get_stats().unwrap()), @r#"
    {
      "indexes": {
        "catto": 1,
        "doggo": 1,
        "whalo": 1
      },
      "statuses": {
        "canceled": 0,
        "enqueued": 0,
        "failed": 0,
        "processing": 1,
        "succeeded": 2
      },
      "types": {
        "documentAdditionOrUpdate": 0,
        "documentDeletion": 0,
        "documentEdition": 0,
        "dumpCreation": 0,
        "indexCreation": 3,
        "indexDeletion": 0,
        "indexSwap": 0,
        "indexUpdate": 0,
        "settingsUpdate": 0,
        "snapshotCreation": 0,
        "taskCancelation": 0,
        "taskDeletion": 0,
        "upgradeDatabase": 0
      }
    }
    "#);
}

#[test]
fn cancel_processing_dump() {
    let (index_scheduler, mut handle) = IndexScheduler::test(true, vec![]);

    let dump_creation = KindWithContent::DumpCreation { keys: Vec::new(), instance_uid: None };
    let dump_cancellation = KindWithContent::TaskCancelation {
        query: "cancel dump".to_owned(),
        tasks: RoaringBitmap::from_iter([0]),
    };
    let _ = index_scheduler.register(dump_creation, None, false).unwrap();
    snapshot!(snapshot_index_scheduler(&index_scheduler), name: "after_dump_register");
    handle.advance_till([Start, BatchCreated, InsideProcessBatch]);

    let _ = index_scheduler.register(dump_cancellation, None, false).unwrap();
    snapshot!(snapshot_index_scheduler(&index_scheduler), name: "cancel_registered");

    snapshot!(format!("{:?}", handle.advance()), @"AbortedIndexation");

    handle.advance_one_successful_batch();
    snapshot!(snapshot_index_scheduler(&index_scheduler), name: "cancel_processed");
}
