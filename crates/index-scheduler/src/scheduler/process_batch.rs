use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::atomic::Ordering;

use meilisearch_types::batches::BatchId;
use meilisearch_types::heed::{RoTxn, RwTxn};
use meilisearch_types::milli::progress::{Progress, VariableNameStep};
use meilisearch_types::milli::{self};
use meilisearch_types::tasks::{Details, IndexSwap, KindWithContent, Status, Task};
use milli::update::Settings as MilliSettings;
use roaring::RoaringBitmap;

use super::create_batch::Batch;
use crate::processing::{
    AtomicBatchStep, AtomicTaskStep, CreateIndexProgress, DeleteIndexProgress,
    InnerSwappingTwoIndexes, SwappingTheIndexes, TaskCancelationProgress, TaskDeletionProgress,
    UpdateIndexProgress,
};
use crate::utils::{self, swap_index_uid_in_task, ProcessingBatch};
use crate::{Error, IndexScheduler, Result, TaskId};

impl IndexScheduler {
    /// Apply the operation associated with the given batch.
    ///
    /// ## Return
    /// The list of tasks that were processed. The metadata of each task in the returned
    /// list is updated accordingly, with the exception of the its date fields
    /// [`finished_at`](meilisearch_types::tasks::Task::finished_at) and [`started_at`](meilisearch_types::tasks::Task::started_at).
    #[tracing::instrument(level = "trace", skip(self, batch, progress), target = "indexing::scheduler", fields(batch=batch.to_string()))]
    pub(crate) fn process_batch(
        &self,
        batch: Batch,
        current_batch: &mut ProcessingBatch,
        progress: Progress,
    ) -> Result<Vec<Task>> {
        #[cfg(test)]
        {
            self.maybe_fail(crate::test_utils::FailureLocation::InsideProcessBatch)?;
            self.maybe_fail(crate::test_utils::FailureLocation::PanicInsideProcessBatch)?;
            self.breakpoint(crate::test_utils::Breakpoint::InsideProcessBatch);
        }

        match batch {
            Batch::TaskCancelation { mut task } => {
                // 1. Retrieve the tasks that matched the query at enqueue-time.
                let matched_tasks =
                    if let KindWithContent::TaskCancelation { tasks, query: _ } = &task.kind {
                        tasks
                    } else {
                        unreachable!()
                    };

                let rtxn = self.env.read_txn()?;
                let mut canceled_tasks = self.cancel_matched_tasks(
                    &rtxn,
                    task.uid,
                    current_batch,
                    matched_tasks,
                    &progress,
                )?;

                task.status = Status::Succeeded;
                match &mut task.details {
                    Some(Details::TaskCancelation {
                        matched_tasks: _,
                        canceled_tasks: canceled_tasks_details,
                        original_filter: _,
                    }) => {
                        *canceled_tasks_details = Some(canceled_tasks.len() as u64);
                    }
                    _ => unreachable!(),
                }

                canceled_tasks.push(task);

                Ok(canceled_tasks)
            }
            Batch::TaskDeletions(mut tasks) => {
                // 1. Retrieve the tasks that matched the query at enqueue-time.
                let mut matched_tasks = RoaringBitmap::new();

                for task in tasks.iter() {
                    if let KindWithContent::TaskDeletion { tasks, query: _ } = &task.kind {
                        matched_tasks |= tasks;
                    } else {
                        unreachable!()
                    }
                }

                let mut wtxn = self.env.write_txn()?;
                let mut deleted_tasks =
                    self.delete_matched_tasks(&mut wtxn, &matched_tasks, &progress)?;
                wtxn.commit()?;

                for task in tasks.iter_mut() {
                    task.status = Status::Succeeded;
                    let KindWithContent::TaskDeletion { tasks, query: _ } = &task.kind else {
                        unreachable!()
                    };

                    let deleted_tasks_count = deleted_tasks.intersection_len(tasks);
                    deleted_tasks -= tasks;

                    match &mut task.details {
                        Some(Details::TaskDeletion {
                            matched_tasks: _,
                            deleted_tasks,
                            original_filter: _,
                        }) => {
                            *deleted_tasks = Some(deleted_tasks_count);
                        }
                        _ => unreachable!(),
                    }
                }
                Ok(tasks)
            }
            Batch::SnapshotCreation(tasks) => self.process_snapshot(progress, tasks),
            Batch::Dump(task) => self.process_dump_creation(progress, task),
            Batch::IndexOperation { op, must_create_index } => {
                let index_uid = op.index_uid().to_string();
                let index = if must_create_index {
                    // create the index if it doesn't already exist
                    let wtxn = self.env.write_txn()?;
                    self.index_mapper.create_index(wtxn, &index_uid, None)?
                } else {
                    let rtxn = self.env.read_txn()?;
                    self.index_mapper.index(&rtxn, &index_uid)?
                };

                // the index operation can take a long time, so save this handle to make it available to the search for the duration of the tick
                self.index_mapper
                    .set_currently_updating_index(Some((index_uid.clone(), index.clone())));

                let mut index_wtxn = index.write_txn()?;
                let tasks = self.apply_index_operation(&mut index_wtxn, &index, op, progress)?;

                {
                    let span = tracing::trace_span!(target: "indexing::scheduler", "commit");
                    let _entered = span.enter();

                    index_wtxn.commit()?;
                }

                // if the update processed successfully, we're going to store the new
                // stats of the index. Since the tasks have already been processed and
                // this is a non-critical operation. If it fails, we should not fail
                // the entire batch.
                let res = || -> Result<()> {
                    let index_rtxn = index.read_txn()?;
                    let stats = crate::index_mapper::IndexStats::new(&index, &index_rtxn)
                        .map_err(|e| Error::from_milli(e, Some(index_uid.to_string())))?;
                    let mut wtxn = self.env.write_txn()?;
                    self.index_mapper.store_stats_of(&mut wtxn, &index_uid, &stats)?;
                    wtxn.commit()?;
                    Ok(())
                }();

                match res {
                    Ok(_) => (),
                    Err(e) => tracing::error!(
                        error = &e as &dyn std::error::Error,
                        "Could not write the stats of the index"
                    ),
                }

                Ok(tasks)
            }
            Batch::IndexCreation { index_uid, primary_key, task } => {
                progress.update_progress(CreateIndexProgress::CreatingTheIndex);

                let wtxn = self.env.write_txn()?;
                if self.index_mapper.exists(&wtxn, &index_uid)? {
                    return Err(Error::IndexAlreadyExists(index_uid));
                }
                self.index_mapper.create_index(wtxn, &index_uid, None)?;

                self.process_batch(
                    Batch::IndexUpdate { index_uid, primary_key, task },
                    current_batch,
                    progress,
                )
            }
            Batch::IndexUpdate { index_uid, primary_key, mut task } => {
                progress.update_progress(UpdateIndexProgress::UpdatingTheIndex);
                let rtxn = self.env.read_txn()?;
                let index = self.index_mapper.index(&rtxn, &index_uid)?;

                if let Some(primary_key) = primary_key.clone() {
                    let mut index_wtxn = index.write_txn()?;
                    let mut builder = MilliSettings::new(
                        &mut index_wtxn,
                        &index,
                        self.index_mapper.indexer_config(),
                    );
                    builder.set_primary_key(primary_key);
                    let must_stop_processing = self.scheduler.must_stop_processing.clone();
                    builder
                        .execute(
                            |indexing_step| tracing::debug!(update = ?indexing_step),
                            || must_stop_processing.get(),
                        )
                        .map_err(|e| Error::from_milli(e, Some(index_uid.to_string())))?;
                    index_wtxn.commit()?;
                }

                // drop rtxn before starting a new wtxn on the same db
                rtxn.commit()?;

                task.status = Status::Succeeded;
                task.details = Some(Details::IndexInfo { primary_key });

                // if the update processed successfully, we're going to store the new
                // stats of the index. Since the tasks have already been processed and
                // this is a non-critical operation. If it fails, we should not fail
                // the entire batch.
                let res = || -> Result<()> {
                    let mut wtxn = self.env.write_txn()?;
                    let index_rtxn = index.read_txn()?;
                    let stats = crate::index_mapper::IndexStats::new(&index, &index_rtxn)
                        .map_err(|e| Error::from_milli(e, Some(index_uid.clone())))?;
                    self.index_mapper.store_stats_of(&mut wtxn, &index_uid, &stats)?;
                    wtxn.commit()?;
                    Ok(())
                }();

                match res {
                    Ok(_) => (),
                    Err(e) => tracing::error!(
                        error = &e as &dyn std::error::Error,
                        "Could not write the stats of the index"
                    ),
                }

                Ok(vec![task])
            }
            Batch::IndexDeletion { index_uid, index_has_been_created, mut tasks } => {
                progress.update_progress(DeleteIndexProgress::DeletingTheIndex);
                let wtxn = self.env.write_txn()?;

                // it's possible that the index doesn't exist
                let number_of_documents = || -> Result<u64> {
                    let index = self.index_mapper.index(&wtxn, &index_uid)?;
                    let index_rtxn = index.read_txn()?;
                    index
                        .number_of_documents(&index_rtxn)
                        .map_err(|e| Error::from_milli(e, Some(index_uid.to_string())))
                }()
                .unwrap_or_default();

                // The write transaction is directly owned and committed inside.
                match self.index_mapper.delete_index(wtxn, &index_uid) {
                    Ok(()) => (),
                    Err(Error::IndexNotFound(_)) if index_has_been_created => (),
                    Err(e) => return Err(e),
                }

                // We set all the tasks details to the default value.
                for task in &mut tasks {
                    task.status = Status::Succeeded;
                    task.details = match &task.kind {
                        KindWithContent::IndexDeletion { .. } => {
                            Some(Details::ClearAll { deleted_documents: Some(number_of_documents) })
                        }
                        otherwise => otherwise.default_finished_details(),
                    };
                }

                Ok(tasks)
            }
            Batch::IndexSwap { mut task } => {
                progress.update_progress(SwappingTheIndexes::EnsuringCorrectnessOfTheSwap);

                let mut wtxn = self.env.write_txn()?;
                let swaps = if let KindWithContent::IndexSwap { swaps } = &task.kind {
                    swaps
                } else {
                    unreachable!()
                };
                let mut not_found_indexes = BTreeSet::new();
                for IndexSwap { indexes: (lhs, rhs) } in swaps {
                    for index in [lhs, rhs] {
                        let index_exists = self.index_mapper.index_exists(&wtxn, index)?;
                        if !index_exists {
                            not_found_indexes.insert(index);
                        }
                    }
                }
                if !not_found_indexes.is_empty() {
                    if not_found_indexes.len() == 1 {
                        return Err(Error::SwapIndexNotFound(
                            not_found_indexes.into_iter().next().unwrap().clone(),
                        ));
                    } else {
                        return Err(Error::SwapIndexesNotFound(
                            not_found_indexes.into_iter().cloned().collect(),
                        ));
                    }
                }
                progress.update_progress(SwappingTheIndexes::SwappingTheIndexes);
                for (step, swap) in swaps.iter().enumerate() {
                    progress.update_progress(VariableNameStep::<SwappingTheIndexes>::new(
                        format!("swapping index {} and {}", swap.indexes.0, swap.indexes.1),
                        step as u32,
                        swaps.len() as u32,
                    ));
                    self.apply_index_swap(
                        &mut wtxn,
                        &progress,
                        task.uid,
                        &swap.indexes.0,
                        &swap.indexes.1,
                    )?;
                }
                wtxn.commit()?;
                task.status = Status::Succeeded;
                Ok(vec![task])
            }
            Batch::UpgradeDatabase { tasks } => self.process_upgrade(progress, tasks),
        }
    }

    /// Swap the index `lhs` with the index `rhs`.
    fn apply_index_swap(
        &self,
        wtxn: &mut RwTxn,
        progress: &Progress,
        task_id: u32,
        lhs: &str,
        rhs: &str,
    ) -> Result<()> {
        progress.update_progress(InnerSwappingTwoIndexes::RetrieveTheTasks);
        // 1. Verify that both lhs and rhs are existing indexes
        let index_lhs_exists = self.index_mapper.index_exists(wtxn, lhs)?;
        if !index_lhs_exists {
            return Err(Error::IndexNotFound(lhs.to_owned()));
        }
        let index_rhs_exists = self.index_mapper.index_exists(wtxn, rhs)?;
        if !index_rhs_exists {
            return Err(Error::IndexNotFound(rhs.to_owned()));
        }

        // 2. Get the task set for index = name that appeared before the index swap task
        let mut index_lhs_task_ids = self.queue.tasks.index_tasks(wtxn, lhs)?;
        index_lhs_task_ids.remove_range(task_id..);
        let mut index_rhs_task_ids = self.queue.tasks.index_tasks(wtxn, rhs)?;
        index_rhs_task_ids.remove_range(task_id..);

        // 3. before_name -> new_name in the task's KindWithContent
        progress.update_progress(InnerSwappingTwoIndexes::UpdateTheTasks);
        let tasks_to_update = &index_lhs_task_ids | &index_rhs_task_ids;
        let (atomic, task_progress) = AtomicTaskStep::new(tasks_to_update.len() as u32);
        progress.update_progress(task_progress);

        for task_id in tasks_to_update {
            let mut task =
                self.queue.tasks.get_task(wtxn, task_id)?.ok_or(Error::CorruptedTaskQueue)?;
            swap_index_uid_in_task(&mut task, (lhs, rhs));
            self.queue.tasks.all_tasks.put(wtxn, &task_id, &task)?;
            atomic.fetch_add(1, Ordering::Relaxed);
        }

        // 4. remove the task from indexuid = before_name
        // 5. add the task to indexuid = after_name
        progress.update_progress(InnerSwappingTwoIndexes::UpdateTheIndexesMetadata);
        self.queue.tasks.update_index(wtxn, lhs, |lhs_tasks| {
            *lhs_tasks -= &index_lhs_task_ids;
            *lhs_tasks |= &index_rhs_task_ids;
        })?;
        self.queue.tasks.update_index(wtxn, rhs, |rhs_tasks| {
            *rhs_tasks -= &index_rhs_task_ids;
            *rhs_tasks |= &index_lhs_task_ids;
        })?;

        // 6. Swap in the index mapper
        self.index_mapper.swap(wtxn, lhs, rhs)?;

        Ok(())
    }

    /// Delete each given task from all the databases (if it is deleteable).
    ///
    /// Return the number of tasks that were actually deleted.
    fn delete_matched_tasks(
        &self,
        wtxn: &mut RwTxn,
        matched_tasks: &RoaringBitmap,
        progress: &Progress,
    ) -> Result<RoaringBitmap> {
        progress.update_progress(TaskDeletionProgress::DeletingTasksDateTime);

        // 1. Remove from this list the tasks that we are not allowed to delete
        let enqueued_tasks = self.queue.tasks.get_status(wtxn, Status::Enqueued)?;
        let processing_tasks = &self.processing_tasks.read().unwrap().processing.clone();

        let all_task_ids = self.queue.tasks.all_task_ids(wtxn)?;
        let mut to_delete_tasks = all_task_ids & matched_tasks;
        to_delete_tasks -= &**processing_tasks;
        to_delete_tasks -= &enqueued_tasks;

        // 2. We now have a list of tasks to delete, delete them

        let mut affected_indexes = HashSet::new();
        let mut affected_statuses = HashSet::new();
        let mut affected_kinds = HashSet::new();
        let mut affected_canceled_by = RoaringBitmap::new();
        // The tasks that have been removed *per batches*.
        let mut affected_batches: HashMap<BatchId, RoaringBitmap> = HashMap::new();

        let (atomic_progress, task_progress) = AtomicTaskStep::new(to_delete_tasks.len() as u32);
        progress.update_progress(task_progress);
        for task_id in to_delete_tasks.iter() {
            let task =
                self.queue.tasks.get_task(wtxn, task_id)?.ok_or(Error::CorruptedTaskQueue)?;

            affected_indexes.extend(task.indexes().into_iter().map(|x| x.to_owned()));
            affected_statuses.insert(task.status);
            affected_kinds.insert(task.kind.as_kind());
            // Note: don't delete the persisted task data since
            // we can only delete succeeded, failed, and canceled tasks.
            // In each of those cases, the persisted data is supposed to
            // have been deleted already.
            utils::remove_task_datetime(
                wtxn,
                self.queue.tasks.enqueued_at,
                task.enqueued_at,
                task.uid,
            )?;
            if let Some(started_at) = task.started_at {
                utils::remove_task_datetime(
                    wtxn,
                    self.queue.tasks.started_at,
                    started_at,
                    task.uid,
                )?;
            }
            if let Some(finished_at) = task.finished_at {
                utils::remove_task_datetime(
                    wtxn,
                    self.queue.tasks.finished_at,
                    finished_at,
                    task.uid,
                )?;
            }
            if let Some(canceled_by) = task.canceled_by {
                affected_canceled_by.insert(canceled_by);
            }
            if let Some(batch_uid) = task.batch_uid {
                affected_batches.entry(batch_uid).or_default().insert(task_id);
            }
            atomic_progress.fetch_add(1, Ordering::Relaxed);
        }

        progress.update_progress(TaskDeletionProgress::DeletingTasksMetadata);
        let (atomic_progress, task_progress) = AtomicTaskStep::new(
            (affected_indexes.len() + affected_statuses.len() + affected_kinds.len()) as u32,
        );
        progress.update_progress(task_progress);
        for index in affected_indexes.iter() {
            self.queue.tasks.update_index(wtxn, index, |bitmap| *bitmap -= &to_delete_tasks)?;
            atomic_progress.fetch_add(1, Ordering::Relaxed);
        }

        for status in affected_statuses.iter() {
            self.queue.tasks.update_status(wtxn, *status, |bitmap| *bitmap -= &to_delete_tasks)?;
            atomic_progress.fetch_add(1, Ordering::Relaxed);
        }

        for kind in affected_kinds.iter() {
            self.queue.tasks.update_kind(wtxn, *kind, |bitmap| *bitmap -= &to_delete_tasks)?;
            atomic_progress.fetch_add(1, Ordering::Relaxed);
        }

        progress.update_progress(TaskDeletionProgress::DeletingTasks);
        let (atomic_progress, task_progress) = AtomicTaskStep::new(to_delete_tasks.len() as u32);
        progress.update_progress(task_progress);
        for task in to_delete_tasks.iter() {
            self.queue.tasks.all_tasks.delete(wtxn, &task)?;
            atomic_progress.fetch_add(1, Ordering::Relaxed);
        }
        for canceled_by in affected_canceled_by {
            if let Some(mut tasks) = self.queue.tasks.canceled_by.get(wtxn, &canceled_by)? {
                tasks -= &to_delete_tasks;
                if tasks.is_empty() {
                    self.queue.tasks.canceled_by.delete(wtxn, &canceled_by)?;
                } else {
                    self.queue.tasks.canceled_by.put(wtxn, &canceled_by, &tasks)?;
                }
            }
        }
        progress.update_progress(TaskDeletionProgress::DeletingBatches);
        let (atomic_progress, batch_progress) = AtomicBatchStep::new(affected_batches.len() as u32);
        progress.update_progress(batch_progress);
        for (batch_id, to_delete_tasks) in affected_batches {
            if let Some(mut tasks) = self.queue.batch_to_tasks_mapping.get(wtxn, &batch_id)? {
                tasks -= &to_delete_tasks;
                // We must remove the batch entirely
                if tasks.is_empty() {
                    self.queue.batches.all_batches.delete(wtxn, &batch_id)?;
                    self.queue.batch_to_tasks_mapping.delete(wtxn, &batch_id)?;
                }
                // Anyway, we must remove the batch from all its reverse indexes.
                // The only way to do that is to check

                for index in affected_indexes.iter() {
                    let index_tasks = self.queue.tasks.index_tasks(wtxn, index)?;
                    let remaining_index_tasks = index_tasks & &tasks;
                    if remaining_index_tasks.is_empty() {
                        self.queue.batches.update_index(wtxn, index, |bitmap| {
                            bitmap.remove(batch_id);
                        })?;
                    }
                }

                for status in affected_statuses.iter() {
                    let status_tasks = self.queue.tasks.get_status(wtxn, *status)?;
                    let remaining_status_tasks = status_tasks & &tasks;
                    if remaining_status_tasks.is_empty() {
                        self.queue.batches.update_status(wtxn, *status, |bitmap| {
                            bitmap.remove(batch_id);
                        })?;
                    }
                }

                for kind in affected_kinds.iter() {
                    let kind_tasks = self.queue.tasks.get_kind(wtxn, *kind)?;
                    let remaining_kind_tasks = kind_tasks & &tasks;
                    if remaining_kind_tasks.is_empty() {
                        self.queue.batches.update_kind(wtxn, *kind, |bitmap| {
                            bitmap.remove(batch_id);
                        })?;
                    }
                }
            }
            atomic_progress.fetch_add(1, Ordering::Relaxed);
        }

        Ok(to_delete_tasks)
    }

    /// Cancel each given task from all the databases (if it is cancelable).
    ///
    /// Returns the list of tasks that matched the filter and must be written in the database.
    fn cancel_matched_tasks(
        &self,
        rtxn: &RoTxn,
        cancel_task_id: TaskId,
        current_batch: &mut ProcessingBatch,
        matched_tasks: &RoaringBitmap,
        progress: &Progress,
    ) -> Result<Vec<Task>> {
        progress.update_progress(TaskCancelationProgress::RetrievingTasks);

        // 1. Remove from this list the tasks that we are not allowed to cancel
        //    Notice that only the _enqueued_ ones are cancelable and we should
        //    have already aborted the indexation of the _processing_ ones
        let cancelable_tasks = self.queue.tasks.get_status(rtxn, Status::Enqueued)?;
        let tasks_to_cancel = cancelable_tasks & matched_tasks;

        let (task_progress, progress_obj) = AtomicTaskStep::new(tasks_to_cancel.len() as u32);
        progress.update_progress(progress_obj);

        // 2. We now have a list of tasks to cancel, cancel them
        let mut tasks = self.queue.tasks.get_existing_tasks(
            rtxn,
            tasks_to_cancel.iter().inspect(|_| {
                task_progress.fetch_add(1, Ordering::Relaxed);
            }),
        )?;

        progress.update_progress(TaskCancelationProgress::UpdatingTasks);
        let (task_progress, progress_obj) = AtomicTaskStep::new(tasks_to_cancel.len() as u32);
        progress.update_progress(progress_obj);
        for task in tasks.iter_mut() {
            task.status = Status::Canceled;
            task.canceled_by = Some(cancel_task_id);
            task.details = task.details.as_ref().map(|d| d.to_failed());
            current_batch.processing(Some(task));
            task_progress.fetch_add(1, Ordering::Relaxed);
        }

        Ok(tasks)
    }
}
