use std::collections::BTreeSet;
use std::fmt::Write;

use meilisearch_types::batches::Batch;
use meilisearch_types::heed::types::{SerdeBincode, SerdeJson, Str};
use meilisearch_types::heed::{Database, RoTxn};
use meilisearch_types::milli::{CboRoaringBitmapCodec, RoaringBitmapCodec, BEU32};
use meilisearch_types::tasks::{Details, Kind, Status, Task};
use roaring::RoaringBitmap;

use crate::index_mapper::IndexMapper;
use crate::{IndexScheduler, BEI128};

pub fn snapshot_index_scheduler(scheduler: &IndexScheduler) -> String {
    // Since we'll snapshot the index right afterward, we don't need to ensure it's internally consistent for every run.
    // We can only do it for the release run, where the function runs way faster.
    #[cfg(not(debug_assertions))]
    scheduler.assert_internally_consistent();

    let IndexScheduler {
        cleanup_enabled: _,
        processing_tasks,
        env,
        queue,
        scheduler,

        index_mapper,
        features: _,
        webhook_url: _,
        webhook_authorization_header: _,
        test_breakpoint_sdr: _,
        planned_failures: _,
        run_loop_iteration: _,
        embedders: _,
    } = scheduler;

    let rtxn = env.read_txn().unwrap();

    let mut snap = String::new();

    let processing = processing_tasks.read().unwrap().clone();
    snap.push_str(&format!("### Autobatching Enabled = {}\n", scheduler.autobatching_enabled));
    snap.push_str(&format!(
        "### Processing batch {:?}:\n",
        processing.batch.as_ref().map(|batch| batch.uid)
    ));
    snap.push_str(&snapshot_bitmap(&processing.processing));
    if let Some(ref batch) = processing.batch {
        snap.push('\n');
        snap.push_str(&snapshot_batch(&batch.to_batch()));
    }
    snap.push_str("\n----------------------------------------------------------------------\n");

    snap.push_str("### All Tasks:\n");
    snap.push_str(&snapshot_all_tasks(&rtxn, queue.tasks.all_tasks));
    snap.push_str("----------------------------------------------------------------------\n");

    snap.push_str("### Status:\n");
    snap.push_str(&snapshot_status(&rtxn, queue.tasks.status));
    snap.push_str("----------------------------------------------------------------------\n");

    snap.push_str("### Kind:\n");
    snap.push_str(&snapshot_kind(&rtxn, queue.tasks.kind));
    snap.push_str("----------------------------------------------------------------------\n");

    snap.push_str("### Index Tasks:\n");
    snap.push_str(&snapshot_index_tasks(&rtxn, queue.tasks.index_tasks));
    snap.push_str("----------------------------------------------------------------------\n");

    snap.push_str("### Index Mapper:\n");
    snap.push_str(&snapshot_index_mapper(&rtxn, index_mapper));
    snap.push_str("\n----------------------------------------------------------------------\n");

    snap.push_str("### Canceled By:\n");
    snap.push_str(&snapshot_canceled_by(&rtxn, queue.tasks.canceled_by));
    snap.push_str("\n----------------------------------------------------------------------\n");

    snap.push_str("### Enqueued At:\n");
    snap.push_str(&snapshot_date_db(&rtxn, queue.tasks.enqueued_at));
    snap.push_str("----------------------------------------------------------------------\n");

    snap.push_str("### Started At:\n");
    snap.push_str(&snapshot_date_db(&rtxn, queue.tasks.started_at));
    snap.push_str("----------------------------------------------------------------------\n");

    snap.push_str("### Finished At:\n");
    snap.push_str(&snapshot_date_db(&rtxn, queue.tasks.finished_at));
    snap.push_str("----------------------------------------------------------------------\n");

    snap.push_str("### All Batches:\n");
    snap.push_str(&snapshot_all_batches(&rtxn, queue.batches.all_batches));
    snap.push_str("----------------------------------------------------------------------\n");

    snap.push_str("### Batch to tasks mapping:\n");
    snap.push_str(&snapshot_batches_to_tasks_mappings(&rtxn, queue.batch_to_tasks_mapping));
    snap.push_str("----------------------------------------------------------------------\n");

    snap.push_str("### Batches Status:\n");
    snap.push_str(&snapshot_status(&rtxn, queue.batches.status));
    snap.push_str("----------------------------------------------------------------------\n");

    snap.push_str("### Batches Kind:\n");
    snap.push_str(&snapshot_kind(&rtxn, queue.batches.kind));
    snap.push_str("----------------------------------------------------------------------\n");

    snap.push_str("### Batches Index Tasks:\n");
    snap.push_str(&snapshot_index_tasks(&rtxn, queue.batches.index_tasks));
    snap.push_str("----------------------------------------------------------------------\n");

    snap.push_str("### Batches Enqueued At:\n");
    snap.push_str(&snapshot_date_db(&rtxn, queue.batches.enqueued_at));
    snap.push_str("----------------------------------------------------------------------\n");

    snap.push_str("### Batches Started At:\n");
    snap.push_str(&snapshot_date_db(&rtxn, queue.batches.started_at));
    snap.push_str("----------------------------------------------------------------------\n");

    snap.push_str("### Batches Finished At:\n");
    snap.push_str(&snapshot_date_db(&rtxn, queue.batches.finished_at));
    snap.push_str("----------------------------------------------------------------------\n");

    snap.push_str("### File Store:\n");
    snap.push_str(&snapshot_file_store(&queue.file_store));
    snap.push_str("\n----------------------------------------------------------------------\n");

    snap
}

pub fn snapshot_file_store(file_store: &file_store::FileStore) -> String {
    let mut snap = String::new();
    // we store the uuid in a `BTreeSet` to keep them ordered.
    let all_uuids = file_store.all_uuids().unwrap().collect::<Result<BTreeSet<_>, _>>().unwrap();
    for uuid in all_uuids {
        snap.push_str(&format!("{uuid}\n"));
    }
    snap
}

pub fn snapshot_bitmap(r: &RoaringBitmap) -> String {
    let mut snap = String::new();
    snap.push('[');
    for x in r {
        snap.push_str(&format!("{x},"));
    }
    snap.push(']');
    snap
}

pub fn snapshot_all_tasks(rtxn: &RoTxn, db: Database<BEU32, SerdeJson<Task>>) -> String {
    let mut snap = String::new();
    let iter = db.iter(rtxn).unwrap();
    for next in iter {
        let (task_id, task) = next.unwrap();
        snap.push_str(&format!("{task_id} {}\n", snapshot_task(&task)));
    }
    snap
}

pub fn snapshot_all_batches(rtxn: &RoTxn, db: Database<BEU32, SerdeJson<Batch>>) -> String {
    let mut snap = String::new();
    let iter = db.iter(rtxn).unwrap();
    for next in iter {
        let (batch_id, batch) = next.unwrap();
        snap.push_str(&format!("{batch_id} {}\n", snapshot_batch(&batch)));
    }
    snap
}

pub fn snapshot_batches_to_tasks_mappings(
    rtxn: &RoTxn,
    db: Database<BEU32, CboRoaringBitmapCodec>,
) -> String {
    let mut snap = String::new();
    let iter = db.iter(rtxn).unwrap();
    for next in iter {
        let (batch_id, tasks) = next.unwrap();
        snap.push_str(&format!("{batch_id} {}\n", snapshot_bitmap(&tasks)));
    }
    snap
}

pub fn snapshot_date_db(rtxn: &RoTxn, db: Database<BEI128, CboRoaringBitmapCodec>) -> String {
    let mut snap = String::new();
    let iter = db.iter(rtxn).unwrap();
    for next in iter {
        let (_timestamp, task_ids) = next.unwrap();
        snap.push_str(&format!("[timestamp] {}\n", snapshot_bitmap(&task_ids)));
    }
    snap
}

pub fn snapshot_task(task: &Task) -> String {
    let mut snap = String::new();
    let Task {
        uid,
        batch_uid,
        enqueued_at: _,
        started_at: _,
        finished_at: _,
        error,
        canceled_by,
        details,
        status,
        kind,
    } = task;
    snap.push('{');
    snap.push_str(&format!("uid: {uid}, "));
    if let Some(batch_uid) = batch_uid {
        snap.push_str(&format!("batch_uid: {batch_uid}, "));
    }
    snap.push_str(&format!("status: {status}, "));
    if let Some(canceled_by) = canceled_by {
        snap.push_str(&format!("canceled_by: {canceled_by}, "));
    }
    if let Some(error) = error {
        snap.push_str(&format!("error: {error:?}, "));
    }
    if let Some(details) = details {
        snap.push_str(&format!("details: {}, ", &snapshot_details(details)));
    }
    snap.push_str(&format!("kind: {kind:?}"));

    snap.push('}');
    snap
}

fn snapshot_details(d: &Details) -> String {
    match d {
        Details::DocumentAdditionOrUpdate {
            received_documents,
            indexed_documents,
        } => {
            format!("{{ received_documents: {received_documents}, indexed_documents: {indexed_documents:?} }}")
        }
        Details::DocumentEdition {
            deleted_documents,
            edited_documents,
            original_filter,
            context,
            function,
        } => {
            format!(
                "{{ deleted_documents: {deleted_documents:?}, edited_documents: {edited_documents:?}, context: {context:?}, function: {function:?}, original_filter: {original_filter:?} }}"
            )
        }
        Details::SettingsUpdate { settings } => {
            format!("{{ settings: {settings:?} }}")
        }
        Details::IndexInfo { primary_key } => {
            format!("{{ primary_key: {primary_key:?} }}")
        }
        Details::DocumentDeletion {
            provided_ids: received_document_ids,
            deleted_documents,
        } => format!("{{ received_document_ids: {received_document_ids}, deleted_documents: {deleted_documents:?} }}"),
        Details::DocumentDeletionByFilter { original_filter, deleted_documents } => format!(
           "{{ original_filter: {original_filter}, deleted_documents: {deleted_documents:?} }}"
        ),
        Details::ClearAll { deleted_documents } => {
            format!("{{ deleted_documents: {deleted_documents:?} }}")
        },
        Details::TaskCancelation {
            matched_tasks,
            canceled_tasks,
            original_filter,
        } => {
            format!("{{ matched_tasks: {matched_tasks:?}, canceled_tasks: {canceled_tasks:?}, original_filter: {original_filter:?} }}")
        }
        Details::TaskDeletion {
            matched_tasks,
            deleted_tasks,
            original_filter,
        } => {
            format!("{{ matched_tasks: {matched_tasks:?}, deleted_tasks: {deleted_tasks:?}, original_filter: {original_filter:?} }}")
        },
        Details::Dump { dump_uid } => {
            format!("{{ dump_uid: {dump_uid:?} }}")
        },
        Details::IndexSwap { swaps } => {
            format!("{{ swaps: {swaps:?} }}")
        }
        Details::UpgradeDatabase { from } => {
            format!("{{ from: v{}.{}.{} }}", from.0, from.1, from.2)
        }
    }
}

pub fn snapshot_status(
    rtxn: &RoTxn,
    db: Database<SerdeBincode<Status>, RoaringBitmapCodec>,
) -> String {
    let mut snap = String::new();
    let iter = db.iter(rtxn).unwrap();
    for next in iter {
        let (status, task_ids) = next.unwrap();
        writeln!(snap, "{status} {}", snapshot_bitmap(&task_ids)).unwrap();
    }
    snap
}
pub fn snapshot_kind(rtxn: &RoTxn, db: Database<SerdeBincode<Kind>, RoaringBitmapCodec>) -> String {
    let mut snap = String::new();
    let iter = db.iter(rtxn).unwrap();
    for next in iter {
        let (kind, task_ids) = next.unwrap();
        let kind = serde_json::to_string(&kind).unwrap();
        writeln!(snap, "{kind} {}", snapshot_bitmap(&task_ids)).unwrap();
    }
    snap
}

pub fn snapshot_index_tasks(rtxn: &RoTxn, db: Database<Str, RoaringBitmapCodec>) -> String {
    let mut snap = String::new();
    let iter = db.iter(rtxn).unwrap();
    for next in iter {
        let (index, task_ids) = next.unwrap();
        writeln!(snap, "{index} {}", snapshot_bitmap(&task_ids)).unwrap();
    }
    snap
}
pub fn snapshot_canceled_by(rtxn: &RoTxn, db: Database<BEU32, RoaringBitmapCodec>) -> String {
    let mut snap = String::new();
    let iter = db.iter(rtxn).unwrap();
    for next in iter {
        let (kind, task_ids) = next.unwrap();
        writeln!(snap, "{kind} {}", snapshot_bitmap(&task_ids)).unwrap();
    }
    snap
}

pub fn snapshot_batch(batch: &Batch) -> String {
    let mut snap = String::new();
    let Batch { uid, details, stats, started_at, finished_at, progress: _ } = batch;
    if let Some(finished_at) = finished_at {
        assert!(finished_at > started_at);
    }
    snap.push('{');
    snap.push_str(&format!("uid: {uid}, "));
    snap.push_str(&format!("details: {}, ", serde_json::to_string(details).unwrap()));
    snap.push_str(&format!("stats: {}, ", serde_json::to_string(stats).unwrap()));
    snap.push('}');
    snap
}

pub fn snapshot_index_mapper(rtxn: &RoTxn, mapper: &IndexMapper) -> String {
    let mut s = String::new();
    let names = mapper.index_names(rtxn).unwrap();

    for name in names {
        let stats = mapper.stats_of(rtxn, &name).unwrap();
        s.push_str(&format!(
            "{name}: {{ number_of_documents: {}, field_distribution: {:?} }}\n",
            stats.number_of_documents, stats.field_distribution
        ));
    }

    s
}
