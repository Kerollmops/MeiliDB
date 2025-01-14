use std::fmt;

use meilisearch_types::heed::RoTxn;
use meilisearch_types::milli::update::IndexDocumentsMethod;
use meilisearch_types::settings::{Settings, Unchecked};
use meilisearch_types::tasks::{Kind, KindWithContent, Status, Task};
use roaring::RoaringBitmap;
use uuid::Uuid;

use super::autobatcher::{self, BatchKind};
use crate::utils::ProcessingBatch;
use crate::{Error, IndexScheduler, Result};

/// Represents a combination of tasks that can all be processed at the same time.
///
/// A batch contains the set of tasks that it represents (accessible through
/// [`self.ids()`](Batch::ids)), as well as additional information on how to
/// be processed.
#[derive(Debug)]
pub(crate) enum Batch {
    TaskCancelation {
        /// The task cancelation itself.
        task: Task,
    },
    TaskDeletions(Vec<Task>),
    SnapshotCreation(Vec<Task>),
    Dump(Task),
    IndexOperation {
        op: IndexOperation,
        must_create_index: bool,
    },
    IndexCreation {
        index_uid: String,
        primary_key: Option<String>,
        task: Task,
    },
    IndexUpdate {
        index_uid: String,
        primary_key: Option<String>,
        task: Task,
    },
    IndexDeletion {
        index_uid: String,
        tasks: Vec<Task>,
        index_has_been_created: bool,
    },
    IndexSwap {
        task: Task,
    },
    UpgradeDatabase {
        tasks: Vec<Task>,
    },
}

#[derive(Debug)]
pub(crate) enum DocumentOperation {
    Add(Uuid),
    Delete(Vec<String>),
}

/// A [batch](Batch) that combines multiple tasks operating on an index.
#[derive(Debug)]
pub(crate) enum IndexOperation {
    DocumentOperation {
        index_uid: String,
        primary_key: Option<String>,
        method: IndexDocumentsMethod,
        operations: Vec<DocumentOperation>,
        tasks: Vec<Task>,
    },
    DocumentEdition {
        index_uid: String,
        task: Task,
    },
    DocumentDeletion {
        index_uid: String,
        tasks: Vec<Task>,
    },
    DocumentClear {
        index_uid: String,
        tasks: Vec<Task>,
    },
    Settings {
        index_uid: String,
        // The boolean indicates if it's a settings deletion or creation.
        settings: Vec<(bool, Settings<Unchecked>)>,
        tasks: Vec<Task>,
    },
    DocumentClearAndSetting {
        index_uid: String,
        cleared_tasks: Vec<Task>,

        // The boolean indicates if it's a settings deletion or creation.
        settings: Vec<(bool, Settings<Unchecked>)>,
        settings_tasks: Vec<Task>,
    },
}

impl Batch {
    /// Return the task ids associated with this batch.
    pub fn ids(&self) -> RoaringBitmap {
        match self {
            Batch::TaskCancelation { task, .. }
            | Batch::Dump(task)
            | Batch::IndexCreation { task, .. }
            | Batch::IndexUpdate { task, .. } => {
                RoaringBitmap::from_sorted_iter(std::iter::once(task.uid)).unwrap()
            }
            Batch::SnapshotCreation(tasks)
            | Batch::TaskDeletions(tasks)
            | Batch::UpgradeDatabase { tasks }
            | Batch::IndexDeletion { tasks, .. } => {
                RoaringBitmap::from_iter(tasks.iter().map(|task| task.uid))
            }
            Batch::IndexOperation { op, .. } => match op {
                IndexOperation::DocumentOperation { tasks, .. }
                | IndexOperation::Settings { tasks, .. }
                | IndexOperation::DocumentDeletion { tasks, .. }
                | IndexOperation::DocumentClear { tasks, .. } => {
                    RoaringBitmap::from_iter(tasks.iter().map(|task| task.uid))
                }
                IndexOperation::DocumentEdition { task, .. } => {
                    RoaringBitmap::from_sorted_iter(std::iter::once(task.uid)).unwrap()
                }
                IndexOperation::DocumentClearAndSetting {
                    cleared_tasks: tasks,
                    settings_tasks: other,
                    ..
                } => RoaringBitmap::from_iter(tasks.iter().chain(other).map(|task| task.uid)),
            },
            Batch::IndexSwap { task } => {
                RoaringBitmap::from_sorted_iter(std::iter::once(task.uid)).unwrap()
            }
        }
    }

    /// Return the index UID associated with this batch
    pub fn index_uid(&self) -> Option<&str> {
        use Batch::*;
        match self {
            TaskCancelation { .. }
            | TaskDeletions(_)
            | SnapshotCreation(_)
            | Dump(_)
            | UpgradeDatabase { .. }
            | IndexSwap { .. } => None,
            IndexOperation { op, .. } => Some(op.index_uid()),
            IndexCreation { index_uid, .. }
            | IndexUpdate { index_uid, .. }
            | IndexDeletion { index_uid, .. } => Some(index_uid),
        }
    }
}

impl fmt::Display for Batch {
    /// A text used when we debug the profiling reports.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let index_uid = self.index_uid();
        let tasks = self.ids();
        match self {
            Batch::TaskCancelation { .. } => f.write_str("TaskCancelation")?,
            Batch::TaskDeletions(_) => f.write_str("TaskDeletion")?,
            Batch::SnapshotCreation(_) => f.write_str("SnapshotCreation")?,
            Batch::Dump(_) => f.write_str("Dump")?,
            Batch::IndexOperation { op, .. } => write!(f, "{op}")?,
            Batch::IndexCreation { .. } => f.write_str("IndexCreation")?,
            Batch::IndexUpdate { .. } => f.write_str("IndexUpdate")?,
            Batch::IndexDeletion { .. } => f.write_str("IndexDeletion")?,
            Batch::IndexSwap { .. } => f.write_str("IndexSwap")?,
            Batch::UpgradeDatabase { .. } => f.write_str("UpgradeDatabase")?,
        };
        match index_uid {
            Some(name) => f.write_fmt(format_args!(" on {name:?} from tasks: {tasks:?}")),
            None => f.write_fmt(format_args!(" from tasks: {tasks:?}")),
        }
    }
}

impl IndexOperation {
    pub fn index_uid(&self) -> &str {
        match self {
            IndexOperation::DocumentOperation { index_uid, .. }
            | IndexOperation::DocumentEdition { index_uid, .. }
            | IndexOperation::DocumentDeletion { index_uid, .. }
            | IndexOperation::DocumentClear { index_uid, .. }
            | IndexOperation::Settings { index_uid, .. }
            | IndexOperation::DocumentClearAndSetting { index_uid, .. } => index_uid,
        }
    }
}

impl fmt::Display for IndexOperation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            IndexOperation::DocumentOperation { .. } => {
                f.write_str("IndexOperation::DocumentOperation")
            }
            IndexOperation::DocumentEdition { .. } => {
                f.write_str("IndexOperation::DocumentEdition")
            }
            IndexOperation::DocumentDeletion { .. } => {
                f.write_str("IndexOperation::DocumentDeletion")
            }
            IndexOperation::DocumentClear { .. } => f.write_str("IndexOperation::DocumentClear"),
            IndexOperation::Settings { .. } => f.write_str("IndexOperation::Settings"),
            IndexOperation::DocumentClearAndSetting { .. } => {
                f.write_str("IndexOperation::DocumentClearAndSetting")
            }
        }
    }
}

impl IndexScheduler {
    /// Convert an [`BatchKind`](crate::autobatcher::BatchKind) into a [`Batch`].
    ///
    /// ## Arguments
    /// - `rtxn`: read transaction
    /// - `index_uid`: name of the index affected by the operations of the autobatch
    /// - `batch`: the result of the autobatcher
    pub(crate) fn create_next_batch_index(
        &self,
        rtxn: &RoTxn,
        index_uid: String,
        batch: BatchKind,
        current_batch: &mut ProcessingBatch,
        must_create_index: bool,
    ) -> Result<Option<Batch>> {
        match batch {
            BatchKind::DocumentClear { ids } => Ok(Some(Batch::IndexOperation {
                op: IndexOperation::DocumentClear {
                    tasks: self.queue.get_existing_tasks_for_processing_batch(
                        rtxn,
                        current_batch,
                        ids,
                    )?,
                    index_uid,
                },
                must_create_index,
            })),
            BatchKind::DocumentEdition { id } => {
                let mut task =
                    self.queue.tasks.get_task(rtxn, id)?.ok_or(Error::CorruptedTaskQueue)?;
                current_batch.processing(Some(&mut task));
                match &task.kind {
                    KindWithContent::DocumentEdition { index_uid, .. } => {
                        Ok(Some(Batch::IndexOperation {
                            op: IndexOperation::DocumentEdition {
                                index_uid: index_uid.clone(),
                                task,
                            },
                            must_create_index: false,
                        }))
                    }
                    _ => unreachable!(),
                }
            }
            BatchKind::DocumentOperation { method, operation_ids, .. } => {
                let tasks = self.queue.get_existing_tasks_for_processing_batch(
                    rtxn,
                    current_batch,
                    operation_ids,
                )?;
                let primary_key = tasks
                    .iter()
                    .find_map(|task| match task.kind {
                        KindWithContent::DocumentAdditionOrUpdate { ref primary_key, .. } => {
                            // we want to stop on the first document addition
                            Some(primary_key.clone())
                        }
                        KindWithContent::DocumentDeletion { .. } => None,
                        _ => unreachable!(),
                    })
                    .flatten();

                let mut operations = Vec::new();

                for task in tasks.iter() {
                    match task.kind {
                        KindWithContent::DocumentAdditionOrUpdate { content_file, .. } => {
                            operations.push(DocumentOperation::Add(content_file));
                        }
                        KindWithContent::DocumentDeletion { ref documents_ids, .. } => {
                            operations.push(DocumentOperation::Delete(documents_ids.clone()));
                        }
                        _ => unreachable!(),
                    }
                }

                Ok(Some(Batch::IndexOperation {
                    op: IndexOperation::DocumentOperation {
                        index_uid,
                        primary_key,
                        method,
                        operations,
                        tasks,
                    },
                    must_create_index,
                }))
            }
            BatchKind::DocumentDeletion { deletion_ids, includes_by_filter: _ } => {
                let tasks = self.queue.get_existing_tasks_for_processing_batch(
                    rtxn,
                    current_batch,
                    deletion_ids,
                )?;

                Ok(Some(Batch::IndexOperation {
                    op: IndexOperation::DocumentDeletion { index_uid, tasks },
                    must_create_index,
                }))
            }
            BatchKind::Settings { settings_ids, .. } => {
                let tasks = self.queue.get_existing_tasks_for_processing_batch(
                    rtxn,
                    current_batch,
                    settings_ids,
                )?;

                let mut settings = Vec::new();
                for task in &tasks {
                    match task.kind {
                        KindWithContent::SettingsUpdate {
                            ref new_settings, is_deletion, ..
                        } => settings.push((is_deletion, *new_settings.clone())),
                        _ => unreachable!(),
                    }
                }

                Ok(Some(Batch::IndexOperation {
                    op: IndexOperation::Settings { index_uid, settings, tasks },
                    must_create_index,
                }))
            }
            BatchKind::ClearAndSettings { other, settings_ids, allow_index_creation } => {
                let (index_uid, settings, settings_tasks) = match self
                    .create_next_batch_index(
                        rtxn,
                        index_uid,
                        BatchKind::Settings { settings_ids, allow_index_creation },
                        current_batch,
                        must_create_index,
                    )?
                    .unwrap()
                {
                    Batch::IndexOperation {
                        op: IndexOperation::Settings { index_uid, settings, tasks, .. },
                        ..
                    } => (index_uid, settings, tasks),
                    _ => unreachable!(),
                };
                let (index_uid, cleared_tasks) = match self
                    .create_next_batch_index(
                        rtxn,
                        index_uid,
                        BatchKind::DocumentClear { ids: other },
                        current_batch,
                        must_create_index,
                    )?
                    .unwrap()
                {
                    Batch::IndexOperation {
                        op: IndexOperation::DocumentClear { index_uid, tasks },
                        ..
                    } => (index_uid, tasks),
                    _ => unreachable!(),
                };

                Ok(Some(Batch::IndexOperation {
                    op: IndexOperation::DocumentClearAndSetting {
                        index_uid,
                        cleared_tasks,
                        settings,
                        settings_tasks,
                    },
                    must_create_index,
                }))
            }
            BatchKind::IndexCreation { id } => {
                let mut task =
                    self.queue.tasks.get_task(rtxn, id)?.ok_or(Error::CorruptedTaskQueue)?;
                current_batch.processing(Some(&mut task));
                let (index_uid, primary_key) = match &task.kind {
                    KindWithContent::IndexCreation { index_uid, primary_key } => {
                        (index_uid.clone(), primary_key.clone())
                    }
                    _ => unreachable!(),
                };
                Ok(Some(Batch::IndexCreation { index_uid, primary_key, task }))
            }
            BatchKind::IndexUpdate { id } => {
                let mut task =
                    self.queue.tasks.get_task(rtxn, id)?.ok_or(Error::CorruptedTaskQueue)?;
                current_batch.processing(Some(&mut task));
                let primary_key = match &task.kind {
                    KindWithContent::IndexUpdate { primary_key, .. } => primary_key.clone(),
                    _ => unreachable!(),
                };
                Ok(Some(Batch::IndexUpdate { index_uid, primary_key, task }))
            }
            BatchKind::IndexDeletion { ids } => Ok(Some(Batch::IndexDeletion {
                index_uid,
                index_has_been_created: must_create_index,
                tasks: self.queue.get_existing_tasks_for_processing_batch(
                    rtxn,
                    current_batch,
                    ids,
                )?,
            })),
            BatchKind::IndexSwap { id } => {
                let mut task =
                    self.queue.tasks.get_task(rtxn, id)?.ok_or(Error::CorruptedTaskQueue)?;
                current_batch.processing(Some(&mut task));
                Ok(Some(Batch::IndexSwap { task }))
            }
        }
    }

    /// Create the next batch to be processed;
    /// 1. We get the *last* task to cancel.
    /// 2. We get the *next* task to delete.
    /// 3. We get the *next* snapshot to process.
    /// 4. We get the *next* dump to process.
    /// 5. We get the *next* tasks to process for a specific index.
    #[tracing::instrument(level = "trace", skip(self, rtxn), target = "indexing::scheduler")]
    pub(crate) fn create_next_batch(
        &self,
        rtxn: &RoTxn,
    ) -> Result<Option<(Batch, ProcessingBatch)>> {
        #[cfg(test)]
        self.maybe_fail(crate::test_utils::FailureLocation::InsideCreateBatch)?;

        let batch_id = self.queue.batches.next_batch_id(rtxn)?;
        let mut current_batch = ProcessingBatch::new(batch_id);

        let enqueued = &self.queue.tasks.get_status(rtxn, Status::Enqueued)?;

        // 0. The priority over everything is to upgrade the instance
        let upgrade = self.queue.tasks.get_kind(rtxn, Kind::UpgradeDatabase)? & enqueued;
        // There shouldn't be multiple upgrade tasks but just in case we're going to batch all of them at the same time
        if !upgrade.is_empty() {
            let mut tasks = self.queue.tasks.get_existing_tasks(rtxn, upgrade)?;
            current_batch.processing(&mut tasks);
            return Ok(Some((Batch::UpgradeDatabase { tasks }, current_batch)));
        }

        // 1. we get the last task to cancel.
        let to_cancel = self.queue.tasks.get_kind(rtxn, Kind::TaskCancelation)? & enqueued;
        if let Some(task_id) = to_cancel.max() {
            let mut task =
                self.queue.tasks.get_task(rtxn, task_id)?.ok_or(Error::CorruptedTaskQueue)?;
            current_batch.processing(Some(&mut task));
            return Ok(Some((Batch::TaskCancelation { task }, current_batch)));
        }

        // 2. we get the next task to delete
        let to_delete = self.queue.tasks.get_kind(rtxn, Kind::TaskDeletion)? & enqueued;
        if !to_delete.is_empty() {
            let mut tasks = self.queue.tasks.get_existing_tasks(rtxn, to_delete)?;
            current_batch.processing(&mut tasks);
            return Ok(Some((Batch::TaskDeletions(tasks), current_batch)));
        }

        // 3. we batch the snapshot.
        let to_snapshot = self.queue.tasks.get_kind(rtxn, Kind::SnapshotCreation)? & enqueued;
        if !to_snapshot.is_empty() {
            let mut tasks = self.queue.tasks.get_existing_tasks(rtxn, to_snapshot)?;
            current_batch.processing(&mut tasks);
            return Ok(Some((Batch::SnapshotCreation(tasks), current_batch)));
        }

        // 4. we batch the dumps.
        let to_dump = self.queue.tasks.get_kind(rtxn, Kind::DumpCreation)? & enqueued;
        if let Some(to_dump) = to_dump.min() {
            let mut task =
                self.queue.tasks.get_task(rtxn, to_dump)?.ok_or(Error::CorruptedTaskQueue)?;
            current_batch.processing(Some(&mut task));
            return Ok(Some((Batch::Dump(task), current_batch)));
        }

        // 5. We make a batch from the unprioritised tasks. Start by taking the next enqueued task.
        let task_id = if let Some(task_id) = enqueued.min() { task_id } else { return Ok(None) };
        let mut task =
            self.queue.tasks.get_task(rtxn, task_id)?.ok_or(Error::CorruptedTaskQueue)?;

        // If the task is not associated with any index, verify that it is an index swap and
        // create the batch directly. Otherwise, get the index name associated with the task
        // and use the autobatcher to batch the enqueued tasks associated with it

        let index_name = if let Some(&index_name) = task.indexes().first() {
            index_name
        } else {
            assert!(matches!(&task.kind, KindWithContent::IndexSwap { swaps } if swaps.is_empty()));
            current_batch.processing(Some(&mut task));
            return Ok(Some((Batch::IndexSwap { task }, current_batch)));
        };

        let index_already_exists = self.index_mapper.exists(rtxn, index_name)?;
        let mut primary_key = None;
        if index_already_exists {
            let index = self.index_mapper.index(rtxn, index_name)?;
            let rtxn = index.read_txn()?;
            primary_key = index.primary_key(&rtxn)?.map(|pk| pk.to_string());
        }

        let index_tasks = self.queue.tasks.index_tasks(rtxn, index_name)? & enqueued;

        // If autobatching is disabled we only take one task at a time.
        // Otherwise, we take only a maximum of tasks to create batches.
        let tasks_limit = if self.scheduler.autobatching_enabled {
            self.scheduler.max_number_of_batched_tasks
        } else {
            1
        };

        let mut enqueued = Vec::new();
        let mut total_size: u64 = 0;
        for task_id in index_tasks.into_iter().take(tasks_limit) {
            let task = self
                .queue
                .tasks
                .get_task(rtxn, task_id)
                .and_then(|task| task.ok_or(Error::CorruptedTaskQueue))?;

            if let Some(uuid) = task.content_uuid() {
                let content_size = self.queue.file_store.compute_size(uuid)?;
                total_size = total_size.saturating_add(content_size);
            }

            if total_size > self.scheduler.batched_tasks_size_limit && !enqueued.is_empty() {
                break;
            }

            enqueued.push((task.uid, task.kind));
        }

        if let Some((batchkind, create_index)) =
            autobatcher::autobatch(enqueued, index_already_exists, primary_key.as_deref())
        {
            return Ok(self
                .create_next_batch_index(
                    rtxn,
                    index_name.to_string(),
                    batchkind,
                    &mut current_batch,
                    create_index,
                )?
                .map(|batch| (batch, current_batch)));
        }

        // If we found no tasks then we were notified for something that got autobatched
        // somehow and there is nothing to do.
        Ok(None)
    }
}
