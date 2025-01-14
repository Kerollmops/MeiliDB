mod batches;
#[cfg(test)]
mod batches_test;
mod tasks;
#[cfg(test)]
mod tasks_test;
#[cfg(test)]
mod test;

use std::collections::BTreeMap;
use std::time::Duration;

use file_store::FileStore;
use meilisearch_types::batches::BatchId;
use meilisearch_types::heed::{Database, Env, RoTxn, RwTxn};
use meilisearch_types::milli::{CboRoaringBitmapCodec, BEU32};
use meilisearch_types::tasks::{Kind, KindWithContent, Status, Task};
use roaring::RoaringBitmap;
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;
use uuid::Uuid;

pub(crate) use self::batches::BatchQueue;
pub(crate) use self::tasks::TaskQueue;
use crate::processing::ProcessingTasks;
use crate::utils::{
    check_index_swap_validity, filter_out_references_to_newer_tasks, ProcessingBatch,
};
use crate::{Error, IndexSchedulerOptions, Result, TaskId};

/// Database const names for the `IndexScheduler`.
mod db_name {
    pub const BATCH_TO_TASKS_MAPPING: &str = "batch-to-tasks-mapping";
}

/// Defines a subset of tasks to be retrieved from the [`IndexScheduler`].
///
/// An empty/default query (where each field is set to `None`) matches all tasks.
/// Each non-null field restricts the set of tasks further.
#[derive(Default, Debug, Clone, PartialEq, Eq)]
pub struct Query {
    /// The maximum number of tasks to be matched
    pub limit: Option<u32>,
    /// The minimum [task id](`meilisearch_types::tasks::Task::uid`) to be matched
    pub from: Option<u32>,
    /// The order used to return the tasks. By default the newest tasks are returned first and the boolean is `false`.
    pub reverse: Option<bool>,
    /// The [task ids](`meilisearch_types::tasks::Task::uid`) to be matched
    pub uids: Option<Vec<TaskId>>,
    /// The [batch ids](`meilisearch_types::batches::Batch::uid`) to be matched
    pub batch_uids: Option<Vec<BatchId>>,
    /// The allowed [statuses](`meilisearch_types::tasks::Task::status`) of the matched tasls
    pub statuses: Option<Vec<Status>>,
    /// The allowed [kinds](meilisearch_types::tasks::Kind) of the matched tasks.
    ///
    /// The kind of a task is given by:
    /// ```
    /// # use meilisearch_types::tasks::{Task, Kind};
    /// # fn doc_func(task: Task) -> Kind {
    /// task.kind.as_kind()
    /// # }
    /// ```
    pub types: Option<Vec<Kind>>,
    /// The allowed [index ids](meilisearch_types::tasks::Task::index_uid) of the matched tasks
    pub index_uids: Option<Vec<String>>,
    /// The [task ids](`meilisearch_types::tasks::Task::uid`) of the [`TaskCancelation`](meilisearch_types::tasks::Task::Kind::TaskCancelation) tasks
    /// that canceled the matched tasks.
    pub canceled_by: Option<Vec<TaskId>>,
    /// Exclusive upper bound of the matched tasks' [`enqueued_at`](meilisearch_types::tasks::Task::enqueued_at) field.
    pub before_enqueued_at: Option<OffsetDateTime>,
    /// Exclusive lower bound of the matched tasks' [`enqueued_at`](meilisearch_types::tasks::Task::enqueued_at) field.
    pub after_enqueued_at: Option<OffsetDateTime>,
    /// Exclusive upper bound of the matched tasks' [`started_at`](meilisearch_types::tasks::Task::started_at) field.
    pub before_started_at: Option<OffsetDateTime>,
    /// Exclusive lower bound of the matched tasks' [`started_at`](meilisearch_types::tasks::Task::started_at) field.
    pub after_started_at: Option<OffsetDateTime>,
    /// Exclusive upper bound of the matched tasks' [`finished_at`](meilisearch_types::tasks::Task::finished_at) field.
    pub before_finished_at: Option<OffsetDateTime>,
    /// Exclusive lower bound of the matched tasks' [`finished_at`](meilisearch_types::tasks::Task::finished_at) field.
    pub after_finished_at: Option<OffsetDateTime>,
}

impl Query {
    /// Return `true` if every field of the query is set to `None`, such that the query
    /// matches all tasks.
    pub fn is_empty(&self) -> bool {
        matches!(
            self,
            Query {
                limit: None,
                from: None,
                reverse: None,
                uids: None,
                batch_uids: None,
                statuses: None,
                types: None,
                index_uids: None,
                canceled_by: None,
                before_enqueued_at: None,
                after_enqueued_at: None,
                before_started_at: None,
                after_started_at: None,
                before_finished_at: None,
                after_finished_at: None,
            }
        )
    }

    /// Add an [index id](meilisearch_types::tasks::Task::index_uid) to the list of permitted indexes.
    pub fn with_index(self, index_uid: String) -> Self {
        let mut index_vec = self.index_uids.unwrap_or_default();
        index_vec.push(index_uid);
        Self { index_uids: Some(index_vec), ..self }
    }

    // Removes the `from` and `limit` restrictions from the query.
    // Useful to get the total number of tasks matching a filter.
    pub fn without_limits(self) -> Self {
        Query { limit: None, from: None, ..self }
    }
}

/// Structure which holds meilisearch's indexes and schedules the tasks
/// to be performed on them.
pub struct Queue {
    pub(crate) tasks: tasks::TaskQueue,
    pub(crate) batches: batches::BatchQueue,

    /// Matches a batch id with the associated task ids.
    pub(crate) batch_to_tasks_mapping: Database<BEU32, CboRoaringBitmapCodec>,

    /// The list of files referenced by the tasks.
    pub(crate) file_store: FileStore,

    /// The max number of tasks allowed before the scheduler starts to delete
    /// the finished tasks automatically.
    pub(crate) max_number_of_tasks: usize,
}

impl Queue {
    pub(crate) fn private_clone(&self) -> Queue {
        Queue {
            tasks: self.tasks.private_clone(),
            batches: self.batches.private_clone(),
            batch_to_tasks_mapping: self.batch_to_tasks_mapping,
            file_store: self.file_store.clone(),
            max_number_of_tasks: self.max_number_of_tasks,
        }
    }

    /// Create an index scheduler and start its run loop.
    pub(crate) fn new(
        env: &Env,
        wtxn: &mut RwTxn,
        options: &IndexSchedulerOptions,
    ) -> Result<Self> {
        // allow unreachable_code to get rids of the warning in the case of a test build.
        Ok(Self {
            file_store: FileStore::new(&options.update_file_path)?,
            batch_to_tasks_mapping: env
                .create_database(wtxn, Some(db_name::BATCH_TO_TASKS_MAPPING))?,
            tasks: TaskQueue::new(env, wtxn)?,
            batches: BatchQueue::new(env, wtxn)?,
            max_number_of_tasks: options.max_number_of_tasks,
        })
    }

    /// Returns the whole set of tasks that belongs to this batch.
    pub(crate) fn tasks_in_batch(&self, rtxn: &RoTxn, batch_id: BatchId) -> Result<RoaringBitmap> {
        Ok(self.batch_to_tasks_mapping.get(rtxn, &batch_id)?.unwrap_or_default())
    }

    /// Convert an iterator to a `Vec` of tasks and edit the `ProcessingBatch` to add the given tasks.
    ///
    /// The tasks MUST exist, or a `CorruptedTaskQueue` error will be thrown.
    pub(crate) fn get_existing_tasks_for_processing_batch(
        &self,
        rtxn: &RoTxn,
        processing_batch: &mut ProcessingBatch,
        tasks: impl IntoIterator<Item = TaskId>,
    ) -> Result<Vec<Task>> {
        tasks
            .into_iter()
            .map(|task_id| {
                let mut task = self
                    .tasks
                    .get_task(rtxn, task_id)
                    .and_then(|task| task.ok_or(Error::CorruptedTaskQueue));
                processing_batch.processing(&mut task);
                task
            })
            .collect::<Result<_>>()
    }

    pub(crate) fn write_batch(
        &self,
        wtxn: &mut RwTxn,
        batch: ProcessingBatch,
        tasks: &RoaringBitmap,
    ) -> Result<()> {
        self.batch_to_tasks_mapping.put(wtxn, &batch.uid, tasks)?;
        self.batches.write_batch(wtxn, batch)?;
        Ok(())
    }

    pub(crate) fn delete_persisted_task_data(&self, task: &Task) -> Result<()> {
        match task.content_uuid() {
            Some(content_file) => self.delete_update_file(content_file),
            None => Ok(()),
        }
    }

    /// Delete a file from the index scheduler.
    ///
    /// Counterpart to the [`create_update_file`](IndexScheduler::create_update_file) method.
    pub fn delete_update_file(&self, uuid: Uuid) -> Result<()> {
        Ok(self.file_store.delete(uuid)?)
    }

    /// Create a file and register it in the index scheduler.
    ///
    /// The returned file and uuid can be used to associate
    /// some data to a task. The file will be kept until
    /// the task has been fully processed.
    pub fn create_update_file(&self, dry_run: bool) -> Result<(Uuid, file_store::File)> {
        if dry_run {
            Ok((Uuid::nil(), file_store::File::dry_file()?))
        } else {
            Ok(self.file_store.new_update()?)
        }
    }

    #[cfg(test)]
    pub fn create_update_file_with_uuid(&self, uuid: u128) -> Result<(Uuid, file_store::File)> {
        Ok(self.file_store.new_update_with_uuid(uuid)?)
    }

    /// The size on disk taken by all the updates files contained in the `IndexScheduler`, in bytes.
    pub fn compute_update_file_size(&self) -> Result<u64> {
        Ok(self.file_store.compute_total_size()?)
    }

    pub fn register(
        &self,
        wtxn: &mut RwTxn,
        kind: &KindWithContent,
        task_id: Option<TaskId>,
        dry_run: bool,
    ) -> Result<Task> {
        let next_task_id = self.tasks.next_task_id(wtxn)?;

        if let Some(uid) = task_id {
            if uid < next_task_id {
                return Err(Error::BadTaskId { received: uid, expected: next_task_id });
            }
        }

        let mut task = Task {
            uid: task_id.unwrap_or(next_task_id),
            // The batch is defined once we starts processing the task
            batch_uid: None,
            enqueued_at: OffsetDateTime::now_utc(),
            started_at: None,
            finished_at: None,
            error: None,
            canceled_by: None,
            details: kind.default_details(),
            status: Status::Enqueued,
            kind: kind.clone(),
        };
        // For deletion and cancelation tasks, we want to make extra sure that they
        // don't attempt to delete/cancel tasks that are newer than themselves.
        filter_out_references_to_newer_tasks(&mut task);
        // If the register task is an index swap task, verify that it is well-formed
        // (that it does not contain duplicate indexes).
        check_index_swap_validity(&task)?;

        // At this point the task is going to be registered and no further checks will be done
        if dry_run {
            return Ok(task);
        }

        // Get rid of the mutability.
        let task = task;
        self.tasks.register(wtxn, &task)?;

        Ok(task)
    }

    /// Register a task to cleanup the task queue if needed
    pub fn cleanup_task_queue(&self, wtxn: &mut RwTxn) -> Result<()> {
        let nb_tasks = self.tasks.all_task_ids(wtxn)?.len();
        // if we have less than 1M tasks everything is fine
        if nb_tasks < self.max_number_of_tasks as u64 {
            return Ok(());
        }

        let finished = self.tasks.status.get(wtxn, &Status::Succeeded)?.unwrap_or_default()
            | self.tasks.status.get(wtxn, &Status::Failed)?.unwrap_or_default()
            | self.tasks.status.get(wtxn, &Status::Canceled)?.unwrap_or_default();

        let to_delete = RoaringBitmap::from_iter(finished.into_iter().rev().take(100_000));

        // /!\ the len must be at least 2 or else we might enter an infinite loop where we only delete
        //     the deletion tasks we enqueued ourselves.
        if to_delete.len() < 2 {
            tracing::warn!("The task queue is almost full, but no task can be deleted yet.");
            // the only thing we can do is hope that the user tasks are going to finish
            return Ok(());
        }

        tracing::info!(
            "The task queue is almost full. Deleting the oldest {} finished tasks.",
            to_delete.len()
        );

        // it's safe to unwrap here because we checked the len above
        let newest_task_id = to_delete.iter().last().unwrap();
        let last_task_to_delete =
            self.tasks.get_task(wtxn, newest_task_id)?.ok_or(Error::CorruptedTaskQueue)?;

        // increase time by one nanosecond so that the enqueuedAt of the last task to delete is also lower than that date.
        let delete_before = last_task_to_delete.enqueued_at + Duration::from_nanos(1);

        self.register(
            wtxn,
            &KindWithContent::TaskDeletion {
                query: format!(
                    "?beforeEnqueuedAt={}&statuses=succeeded,failed,canceled",
                    delete_before.format(&Rfc3339).map_err(|_| Error::CorruptedTaskQueue)?,
                ),
                tasks: to_delete,
            },
            None,
            false,
        )?;

        Ok(())
    }

    pub fn get_stats(
        &self,
        rtxn: &RoTxn,
        processing: &ProcessingTasks,
    ) -> Result<BTreeMap<String, BTreeMap<String, u64>>> {
        let mut res = BTreeMap::new();
        let processing_tasks = processing.processing.len();

        res.insert(
            "statuses".to_string(),
            enum_iterator::all::<Status>()
                .map(|s| {
                    let tasks = self.tasks.get_status(rtxn, s)?.len();
                    match s {
                        Status::Enqueued => Ok((s.to_string(), tasks - processing_tasks)),
                        Status::Processing => Ok((s.to_string(), processing_tasks)),
                        s => Ok((s.to_string(), tasks)),
                    }
                })
                .collect::<Result<BTreeMap<String, u64>>>()?,
        );
        res.insert(
            "types".to_string(),
            enum_iterator::all::<Kind>()
                .map(|s| Ok((s.to_string(), self.tasks.get_kind(rtxn, s)?.len())))
                .collect::<Result<BTreeMap<String, u64>>>()?,
        );
        res.insert(
            "indexes".to_string(),
            self.tasks
                .index_tasks
                .iter(rtxn)?
                .map(|res| Ok(res.map(|(name, bitmap)| (name.to_string(), bitmap.len()))?))
                .collect::<Result<BTreeMap<String, u64>>>()?,
        );

        Ok(res)
    }
}
