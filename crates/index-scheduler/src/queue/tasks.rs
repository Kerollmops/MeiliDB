use std::ops::{Bound, RangeBounds};

use meilisearch_types::heed::types::{DecodeIgnore, SerdeBincode, SerdeJson, Str};
use meilisearch_types::heed::{Database, Env, RoTxn, RwTxn};
use meilisearch_types::milli::{CboRoaringBitmapCodec, RoaringBitmapCodec, BEU32};
use meilisearch_types::tasks::{Kind, Status, Task};
use roaring::{MultiOps, RoaringBitmap};
use time::OffsetDateTime;

use super::{Query, Queue};
use crate::processing::ProcessingTasks;
use crate::utils::{self, insert_task_datetime, keep_ids_within_datetimes, map_bound};
use crate::{Error, Result, TaskId, BEI128};

/// Database const names for the `IndexScheduler`.
mod db_name {
    pub const ALL_TASKS: &str = "all-tasks";
    pub const STATUS: &str = "status";
    pub const KIND: &str = "kind";
    pub const INDEX_TASKS: &str = "index-tasks";
    pub const CANCELED_BY: &str = "canceled_by";
    pub const ENQUEUED_AT: &str = "enqueued-at";
    pub const STARTED_AT: &str = "started-at";
    pub const FINISHED_AT: &str = "finished-at";
}

pub struct TaskQueue {
    /// The main database, it contains all the tasks accessible by their Id.
    pub(crate) all_tasks: Database<BEU32, SerdeJson<Task>>,

    /// All the tasks ids grouped by their status.
    // TODO we should not be able to serialize a `Status::Processing` in this database.
    pub(crate) status: Database<SerdeBincode<Status>, RoaringBitmapCodec>,
    /// All the tasks ids grouped by their kind.
    pub(crate) kind: Database<SerdeBincode<Kind>, RoaringBitmapCodec>,
    /// Store the tasks associated to an index.
    pub(crate) index_tasks: Database<Str, RoaringBitmapCodec>,
    /// Store the tasks that were canceled by a task uid
    pub(crate) canceled_by: Database<BEU32, RoaringBitmapCodec>,
    /// Store the task ids of tasks which were enqueued at a specific date
    pub(crate) enqueued_at: Database<BEI128, CboRoaringBitmapCodec>,
    /// Store the task ids of finished tasks which started being processed at a specific date
    pub(crate) started_at: Database<BEI128, CboRoaringBitmapCodec>,
    /// Store the task ids of tasks which finished at a specific date
    pub(crate) finished_at: Database<BEI128, CboRoaringBitmapCodec>,
}

impl TaskQueue {
    pub(crate) fn private_clone(&self) -> TaskQueue {
        TaskQueue {
            all_tasks: self.all_tasks,
            status: self.status,
            kind: self.kind,
            index_tasks: self.index_tasks,
            canceled_by: self.canceled_by,
            enqueued_at: self.enqueued_at,
            started_at: self.started_at,
            finished_at: self.finished_at,
        }
    }

    pub(crate) fn new(env: &Env, wtxn: &mut RwTxn) -> Result<Self> {
        Ok(Self {
            all_tasks: env.create_database(wtxn, Some(db_name::ALL_TASKS))?,
            status: env.create_database(wtxn, Some(db_name::STATUS))?,
            kind: env.create_database(wtxn, Some(db_name::KIND))?,
            index_tasks: env.create_database(wtxn, Some(db_name::INDEX_TASKS))?,
            canceled_by: env.create_database(wtxn, Some(db_name::CANCELED_BY))?,
            enqueued_at: env.create_database(wtxn, Some(db_name::ENQUEUED_AT))?,
            started_at: env.create_database(wtxn, Some(db_name::STARTED_AT))?,
            finished_at: env.create_database(wtxn, Some(db_name::FINISHED_AT))?,
        })
    }

    pub(crate) fn last_task_id(&self, rtxn: &RoTxn) -> Result<Option<TaskId>> {
        Ok(self.all_tasks.remap_data_type::<DecodeIgnore>().last(rtxn)?.map(|(k, _)| k + 1))
    }

    pub(crate) fn next_task_id(&self, rtxn: &RoTxn) -> Result<TaskId> {
        Ok(self.last_task_id(rtxn)?.unwrap_or_default())
    }

    pub(crate) fn all_task_ids(&self, rtxn: &RoTxn) -> Result<RoaringBitmap> {
        enum_iterator::all().map(|s| self.get_status(rtxn, s)).union()
    }

    pub(crate) fn get_task(&self, rtxn: &RoTxn, task_id: TaskId) -> Result<Option<Task>> {
        Ok(self.all_tasks.get(rtxn, &task_id)?)
    }

    pub(crate) fn update_task(&self, wtxn: &mut RwTxn, task: &Task) -> Result<()> {
        let old_task = self.get_task(wtxn, task.uid)?.ok_or(Error::CorruptedTaskQueue)?;

        debug_assert!(old_task != *task);
        debug_assert_eq!(old_task.uid, task.uid);
        debug_assert!(old_task.batch_uid.is_none() && task.batch_uid.is_some());
        debug_assert!(
            old_task.batch_uid.is_none() && task.batch_uid.is_some(),
            "\n==> old: {old_task:?}\n==> new: {task:?}"
        );

        if old_task.status != task.status {
            self.update_status(wtxn, old_task.status, |bitmap| {
                bitmap.remove(task.uid);
            })?;
            self.update_status(wtxn, task.status, |bitmap| {
                bitmap.insert(task.uid);
            })?;
        }

        if old_task.kind.as_kind() != task.kind.as_kind() {
            self.update_kind(wtxn, old_task.kind.as_kind(), |bitmap| {
                bitmap.remove(task.uid);
            })?;
            self.update_kind(wtxn, task.kind.as_kind(), |bitmap| {
                bitmap.insert(task.uid);
            })?;
        }

        assert_eq!(
            old_task.enqueued_at, task.enqueued_at,
            "Cannot update a task's enqueued_at time"
        );
        if old_task.started_at != task.started_at {
            assert!(old_task.started_at.is_none(), "Cannot update a task's started_at time");
            if let Some(started_at) = task.started_at {
                insert_task_datetime(wtxn, self.started_at, started_at, task.uid)?;
            }
        }
        if old_task.finished_at != task.finished_at {
            assert!(old_task.finished_at.is_none(), "Cannot update a task's finished_at time");
            if let Some(finished_at) = task.finished_at {
                insert_task_datetime(wtxn, self.finished_at, finished_at, task.uid)?;
            }
        }

        self.all_tasks.put(wtxn, &task.uid, task)?;
        Ok(())
    }

    /// Returns the whole set of tasks that belongs to this index.
    pub(crate) fn index_tasks(&self, rtxn: &RoTxn, index: &str) -> Result<RoaringBitmap> {
        Ok(self.index_tasks.get(rtxn, index)?.unwrap_or_default())
    }

    pub(crate) fn update_index(
        &self,
        wtxn: &mut RwTxn,
        index: &str,
        f: impl Fn(&mut RoaringBitmap),
    ) -> Result<()> {
        let mut tasks = self.index_tasks(wtxn, index)?;
        f(&mut tasks);
        if tasks.is_empty() {
            self.index_tasks.delete(wtxn, index)?;
        } else {
            self.index_tasks.put(wtxn, index, &tasks)?;
        }

        Ok(())
    }

    pub(crate) fn get_status(&self, rtxn: &RoTxn, status: Status) -> Result<RoaringBitmap> {
        Ok(self.status.get(rtxn, &status)?.unwrap_or_default())
    }

    pub(crate) fn put_status(
        &self,
        wtxn: &mut RwTxn,
        status: Status,
        bitmap: &RoaringBitmap,
    ) -> Result<()> {
        Ok(self.status.put(wtxn, &status, bitmap)?)
    }

    pub(crate) fn update_status(
        &self,
        wtxn: &mut RwTxn,
        status: Status,
        f: impl Fn(&mut RoaringBitmap),
    ) -> Result<()> {
        let mut tasks = self.get_status(wtxn, status)?;
        f(&mut tasks);
        self.put_status(wtxn, status, &tasks)?;

        Ok(())
    }

    pub(crate) fn get_kind(&self, rtxn: &RoTxn, kind: Kind) -> Result<RoaringBitmap> {
        Ok(self.kind.get(rtxn, &kind)?.unwrap_or_default())
    }

    pub(crate) fn put_kind(
        &self,
        wtxn: &mut RwTxn,
        kind: Kind,
        bitmap: &RoaringBitmap,
    ) -> Result<()> {
        Ok(self.kind.put(wtxn, &kind, bitmap)?)
    }

    pub(crate) fn update_kind(
        &self,
        wtxn: &mut RwTxn,
        kind: Kind,
        f: impl Fn(&mut RoaringBitmap),
    ) -> Result<()> {
        let mut tasks = self.get_kind(wtxn, kind)?;
        f(&mut tasks);
        self.put_kind(wtxn, kind, &tasks)?;

        Ok(())
    }

    /// Convert an iterator to a `Vec` of tasks. The tasks MUST exist or a
    /// `CorruptedTaskQueue` error will be thrown.
    pub(crate) fn get_existing_tasks(
        &self,
        rtxn: &RoTxn,
        tasks: impl IntoIterator<Item = TaskId>,
    ) -> Result<Vec<Task>> {
        tasks
            .into_iter()
            .map(|task_id| {
                self.get_task(rtxn, task_id).and_then(|task| task.ok_or(Error::CorruptedTaskQueue))
            })
            .collect::<Result<_>>()
    }

    pub(crate) fn register(&self, wtxn: &mut RwTxn, task: &Task) -> Result<()> {
        self.all_tasks.put(wtxn, &task.uid, task)?;

        for index in task.indexes() {
            self.update_index(wtxn, index, |bitmap| {
                bitmap.insert(task.uid);
            })?;
        }

        self.update_status(wtxn, Status::Enqueued, |bitmap| {
            bitmap.insert(task.uid);
        })?;

        self.update_kind(wtxn, task.kind.as_kind(), |bitmap| {
            bitmap.insert(task.uid);
        })?;

        utils::insert_task_datetime(wtxn, self.enqueued_at, task.enqueued_at, task.uid)?;

        Ok(())
    }
}

impl Queue {
    /// Return the task ids matched by the given query from the index scheduler's point of view.
    pub(crate) fn get_task_ids(
        &self,
        rtxn: &RoTxn,
        query: &Query,
        processing_tasks: &ProcessingTasks,
    ) -> Result<RoaringBitmap> {
        let ProcessingTasks { batch: processing_batch, processing: processing_tasks, progress: _ } =
            processing_tasks;
        let Query {
            limit,
            from,
            reverse,
            uids,
            batch_uids,
            statuses,
            types,
            index_uids,
            canceled_by,
            before_enqueued_at,
            after_enqueued_at,
            before_started_at,
            after_started_at,
            before_finished_at,
            after_finished_at,
        } = query;

        let mut tasks = self.tasks.all_task_ids(rtxn)?;

        if let Some(from) = from {
            let range = if reverse.unwrap_or_default() {
                u32::MIN..*from
            } else {
                from.saturating_add(1)..u32::MAX
            };
            tasks.remove_range(range);
        }

        if let Some(batch_uids) = batch_uids {
            let mut batch_tasks = RoaringBitmap::new();
            for batch_uid in batch_uids {
                if processing_batch.as_ref().map_or(false, |batch| batch.uid == *batch_uid) {
                    batch_tasks |= &**processing_tasks;
                } else {
                    batch_tasks |= self.tasks_in_batch(rtxn, *batch_uid)?;
                }
            }
            tasks &= batch_tasks;
        }

        if let Some(status) = statuses {
            let mut status_tasks = RoaringBitmap::new();
            for status in status {
                match status {
                    // special case for Processing tasks
                    Status::Processing => {
                        status_tasks |= &**processing_tasks;
                    }
                    status => status_tasks |= &self.tasks.get_status(rtxn, *status)?,
                };
            }
            if !status.contains(&Status::Processing) {
                tasks -= &**processing_tasks;
            }
            tasks &= status_tasks;
        }

        if let Some(uids) = uids {
            let uids = RoaringBitmap::from_iter(uids);
            tasks &= &uids;
        }

        if let Some(canceled_by) = canceled_by {
            let mut all_canceled_tasks = RoaringBitmap::new();
            for cancel_task_uid in canceled_by {
                if let Some(canceled_by_uid) = self.tasks.canceled_by.get(rtxn, cancel_task_uid)? {
                    all_canceled_tasks |= canceled_by_uid;
                }
            }

            // if the canceled_by has been specified but no task
            // matches then we prefer matching zero than all tasks.
            if all_canceled_tasks.is_empty() {
                return Ok(RoaringBitmap::new());
            } else {
                tasks &= all_canceled_tasks;
            }
        }

        if let Some(kind) = types {
            let mut kind_tasks = RoaringBitmap::new();
            for kind in kind {
                kind_tasks |= self.tasks.get_kind(rtxn, *kind)?;
            }
            tasks &= &kind_tasks;
        }

        if let Some(index) = index_uids {
            let mut index_tasks = RoaringBitmap::new();
            for index in index {
                index_tasks |= self.tasks.index_tasks(rtxn, index)?;
            }
            tasks &= &index_tasks;
        }

        // For the started_at filter, we need to treat the part of the tasks that are processing from the part of the
        // tasks that are not processing. The non-processing ones are filtered normally while the processing ones
        // are entirely removed unless the in-memory startedAt variable falls within the date filter.
        // Once we have filtered the two subsets, we put them back together and assign it back to `tasks`.
        tasks = {
            let (mut filtered_non_processing_tasks, mut filtered_processing_tasks) =
                (&tasks - &**processing_tasks, &tasks & &**processing_tasks);

            // special case for Processing tasks
            // A closure that clears the filtered_processing_tasks if their started_at date falls outside the given bounds
            let mut clear_filtered_processing_tasks =
                |start: Bound<OffsetDateTime>, end: Bound<OffsetDateTime>| {
                    let start = map_bound(start, |b| b.unix_timestamp_nanos());
                    let end = map_bound(end, |b| b.unix_timestamp_nanos());
                    let is_within_dates = RangeBounds::contains(
                        &(start, end),
                        &processing_batch
                            .as_ref()
                            .map_or_else(OffsetDateTime::now_utc, |batch| batch.started_at)
                            .unix_timestamp_nanos(),
                    );
                    if !is_within_dates {
                        filtered_processing_tasks.clear();
                    }
                };
            match (after_started_at, before_started_at) {
                (None, None) => (),
                (None, Some(before)) => {
                    clear_filtered_processing_tasks(Bound::Unbounded, Bound::Excluded(*before))
                }
                (Some(after), None) => {
                    clear_filtered_processing_tasks(Bound::Excluded(*after), Bound::Unbounded)
                }
                (Some(after), Some(before)) => clear_filtered_processing_tasks(
                    Bound::Excluded(*after),
                    Bound::Excluded(*before),
                ),
            };

            keep_ids_within_datetimes(
                rtxn,
                &mut filtered_non_processing_tasks,
                self.tasks.started_at,
                *after_started_at,
                *before_started_at,
            )?;
            filtered_non_processing_tasks | filtered_processing_tasks
        };

        keep_ids_within_datetimes(
            rtxn,
            &mut tasks,
            self.tasks.enqueued_at,
            *after_enqueued_at,
            *before_enqueued_at,
        )?;

        keep_ids_within_datetimes(
            rtxn,
            &mut tasks,
            self.tasks.finished_at,
            *after_finished_at,
            *before_finished_at,
        )?;

        if let Some(limit) = limit {
            tasks = if query.reverse.unwrap_or_default() {
                tasks.into_iter().take(*limit as usize).collect()
            } else {
                tasks.into_iter().rev().take(*limit as usize).collect()
            };
        }

        Ok(tasks)
    }

    pub(crate) fn get_task_ids_from_authorized_indexes(
        &self,
        rtxn: &RoTxn,
        query: &Query,
        filters: &meilisearch_auth::AuthFilter,
        processing_tasks: &ProcessingTasks,
    ) -> Result<(RoaringBitmap, u64)> {
        // compute all tasks matching the filter by ignoring the limits, to find the number of tasks matching
        // the filter.
        // As this causes us to compute the filter twice it is slightly inefficient, but doing it this way spares
        // us from modifying the underlying implementation, and the performance remains sufficient.
        // Should this change, we would modify `get_task_ids` to directly return the number of matching tasks.
        let total_tasks =
            self.get_task_ids(rtxn, &query.clone().without_limits(), processing_tasks)?;
        let mut tasks = self.get_task_ids(rtxn, query, processing_tasks)?;

        // If the query contains a list of index uid or there is a finite list of authorized indexes,
        // then we must exclude all the kinds that aren't associated to one and only one index.
        if query.index_uids.is_some() || !filters.all_indexes_authorized() {
            for kind in enum_iterator::all::<Kind>().filter(|kind| !kind.related_to_one_index()) {
                tasks -= self.tasks.get_kind(rtxn, kind)?;
            }
        }

        // Any task that is internally associated with a non-authorized index
        // must be discarded.
        if !filters.all_indexes_authorized() {
            let all_indexes_iter = self.tasks.index_tasks.iter(rtxn)?;
            for result in all_indexes_iter {
                let (index, index_tasks) = result?;
                if !filters.is_index_authorized(index) {
                    tasks -= index_tasks;
                }
            }
        }

        Ok((tasks, total_tasks.len()))
    }

    pub(crate) fn get_tasks_from_authorized_indexes(
        &self,
        rtxn: &RoTxn,
        query: &Query,
        filters: &meilisearch_auth::AuthFilter,
        processing_tasks: &ProcessingTasks,
    ) -> Result<(Vec<Task>, u64)> {
        let (tasks, total) =
            self.get_task_ids_from_authorized_indexes(rtxn, query, filters, processing_tasks)?;
        let tasks = if query.reverse.unwrap_or_default() {
            Box::new(tasks.into_iter()) as Box<dyn Iterator<Item = u32>>
        } else {
            Box::new(tasks.into_iter().rev()) as Box<dyn Iterator<Item = u32>>
        };
        let tasks = self
            .tasks
            .get_existing_tasks(rtxn, tasks.take(query.limit.unwrap_or(u32::MAX) as usize))?;

        let ProcessingTasks { batch, processing, progress: _ } = processing_tasks;

        let ret = tasks.into_iter();
        if processing.is_empty() || batch.is_none() {
            Ok((ret.collect(), total))
        } else {
            // Safe because we ensured there was a batch in the previous branch
            let batch = batch.as_ref().unwrap();
            Ok((
                ret.map(|task| {
                    if processing.contains(task.uid) {
                        Task {
                            status: Status::Processing,
                            batch_uid: Some(batch.uid),
                            started_at: Some(batch.started_at),
                            ..task
                        }
                    } else {
                        task
                    }
                })
                .collect(),
                total,
            ))
        }
    }
}
