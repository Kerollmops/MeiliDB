mod autobatcher;
#[cfg(test)]
mod autobatcher_test;
mod create_batch;
mod process_batch;
mod process_dump_creation;
mod process_index_operation;
mod process_snapshot_creation;
mod process_upgrade;
#[cfg(test)]
mod test;
#[cfg(test)]
mod test_document_addition;
#[cfg(test)]
mod test_embedders;
#[cfg(test)]
mod test_failure;

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::Arc;

use meilisearch_types::error::ResponseError;
use meilisearch_types::milli;
use meilisearch_types::tasks::Status;
use rayon::current_num_threads;
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use roaring::RoaringBitmap;
use synchronoise::SignalEvent;

use crate::processing::{AtomicTaskStep, BatchProgress};
use crate::{Error, IndexScheduler, IndexSchedulerOptions, Result, TickOutcome};

#[derive(Default, Clone, Debug)]
pub struct MustStopProcessing(Arc<AtomicBool>);

impl MustStopProcessing {
    pub fn get(&self) -> bool {
        self.0.load(Ordering::Relaxed)
    }

    pub fn must_stop(&self) {
        self.0.store(true, Ordering::Relaxed);
    }

    pub fn reset(&self) {
        self.0.store(false, Ordering::Relaxed);
    }
}

pub struct Scheduler {
    /// A boolean that can be set to true to stop the currently processing tasks.
    pub must_stop_processing: MustStopProcessing,

    /// Get a signal when a batch needs to be processed.
    pub(crate) wake_up: Arc<SignalEvent>,

    /// Whether auto-batching is enabled or not.
    pub(crate) autobatching_enabled: bool,

    /// The maximum number of tasks that will be batched together.
    pub(crate) max_number_of_batched_tasks: usize,

    /// The maximum size, in bytes, of tasks in a batch.
    pub(crate) batched_tasks_size_limit: u64,

    /// The path used to create the dumps.
    pub(crate) dumps_path: PathBuf,

    /// The path used to create the snapshots.
    pub(crate) snapshots_path: PathBuf,

    /// The path to the folder containing the auth LMDB env.
    pub(crate) auth_path: PathBuf,

    /// The path to the version file of Meilisearch.
    pub(crate) version_file_path: PathBuf,
}

impl Scheduler {
    pub(crate) fn private_clone(&self) -> Scheduler {
        Scheduler {
            must_stop_processing: self.must_stop_processing.clone(),
            wake_up: self.wake_up.clone(),
            autobatching_enabled: self.autobatching_enabled,
            max_number_of_batched_tasks: self.max_number_of_batched_tasks,
            batched_tasks_size_limit: self.batched_tasks_size_limit,
            dumps_path: self.dumps_path.clone(),
            snapshots_path: self.snapshots_path.clone(),
            auth_path: self.auth_path.clone(),
            version_file_path: self.version_file_path.clone(),
        }
    }

    pub fn new(options: &IndexSchedulerOptions) -> Scheduler {
        Scheduler {
            must_stop_processing: MustStopProcessing::default(),
            // we want to start the loop right away in case meilisearch was ctrl+Ced while processing things
            wake_up: Arc::new(SignalEvent::auto(true)),
            autobatching_enabled: options.autobatching_enabled,
            max_number_of_batched_tasks: options.max_number_of_batched_tasks,
            batched_tasks_size_limit: options.batched_tasks_size_limit,
            dumps_path: options.dumps_path.clone(),
            snapshots_path: options.snapshots_path.clone(),
            auth_path: options.auth_path.clone(),
            version_file_path: options.version_file_path.clone(),
        }
    }
}

impl IndexScheduler {
    /// Perform one iteration of the run loop.
    ///
    /// 1. See if we need to cleanup the task queue
    /// 2. Find the next batch of tasks to be processed.
    /// 3. Update the information of these tasks following the start of their processing.
    /// 4. Update the in-memory list of processed tasks accordingly.
    /// 5. Process the batch:
    ///    - perform the actions of each batched task
    ///    - update the information of each batched task following the end
    ///      of their processing.
    /// 6. Reset the in-memory list of processed tasks.
    ///
    /// Returns the number of processed tasks.
    pub(crate) fn tick(&self) -> Result<TickOutcome> {
        #[cfg(test)]
        {
            *self.run_loop_iteration.write().unwrap() += 1;
            self.breakpoint(crate::test_utils::Breakpoint::Start);
        }

        if self.cleanup_enabled {
            let mut wtxn = self.env.write_txn()?;
            self.queue.cleanup_task_queue(&mut wtxn)?;
            wtxn.commit()?;
        }

        let rtxn = self.env.read_txn().map_err(Error::HeedTransaction)?;
        let (batch, mut processing_batch) =
            match self.create_next_batch(&rtxn).map_err(|e| Error::CreateBatch(Box::new(e)))? {
                Some(batch) => batch,
                None => return Ok(TickOutcome::WaitForSignal),
            };
        let index_uid = batch.index_uid().map(ToOwned::to_owned);
        drop(rtxn);

        // 1. store the starting date with the bitmap of processing tasks.
        let mut ids = batch.ids();
        let processed_tasks = ids.len();

        // We reset the must_stop flag to be sure that we don't stop processing tasks
        self.scheduler.must_stop_processing.reset();
        let progress = self
            .processing_tasks
            .write()
            .unwrap()
            // We can clone the processing batch here because we don't want its modification to affect the view of the processing batches
            .start_processing(processing_batch.clone(), ids.clone());

        #[cfg(test)]
        self.breakpoint(crate::test_utils::Breakpoint::BatchCreated);

        // 2. Process the tasks
        let res = {
            let cloned_index_scheduler = self.private_clone();
            let processing_batch = &mut processing_batch;
            let progress = progress.clone();
            std::thread::scope(|s| {
                let handle = std::thread::Builder::new()
                    .name(String::from("batch-operation"))
                    .spawn_scoped(s, move || {
                        cloned_index_scheduler.process_batch(batch, processing_batch, progress)
                    })
                    .unwrap();
                handle.join().unwrap_or(Err(Error::ProcessBatchPanicked))
            })
        };

        // Reset the currently updating index to relinquish the index handle
        self.index_mapper.set_currently_updating_index(None);

        #[cfg(test)]
        self.maybe_fail(crate::test_utils::FailureLocation::AcquiringWtxn)?;

        progress.update_progress(BatchProgress::WritingTasksToDisk);
        processing_batch.finished();
        let mut wtxn = self.env.write_txn().map_err(Error::HeedTransaction)?;
        let mut canceled = RoaringBitmap::new();

        match res {
            Ok(tasks) => {
                #[cfg(test)]
                self.breakpoint(crate::test_utils::Breakpoint::ProcessBatchSucceeded);

                let (task_progress, task_progress_obj) = AtomicTaskStep::new(tasks.len() as u32);
                progress.update_progress(task_progress_obj);
                let mut success = 0;
                let mut failure = 0;
                let mut canceled_by = None;

                #[allow(unused_variables)]
                for (i, mut task) in tasks.into_iter().enumerate() {
                    task_progress.fetch_add(1, Ordering::Relaxed);
                    processing_batch.update(&mut task);
                    if task.status == Status::Canceled {
                        canceled.insert(task.uid);
                        canceled_by = task.canceled_by;
                    }

                    #[cfg(test)]
                    self.maybe_fail(
                        crate::test_utils::FailureLocation::UpdatingTaskAfterProcessBatchSuccess {
                            task_uid: i as u32,
                        },
                    )?;

                    match task.error {
                        Some(_) => failure += 1,
                        None => success += 1,
                    }

                    self.queue
                        .tasks
                        .update_task(&mut wtxn, &task)
                        .map_err(|e| Error::TaskDatabaseUpdate(Box::new(e)))?;
                }
                if let Some(canceled_by) = canceled_by {
                    self.queue.tasks.canceled_by.put(&mut wtxn, &canceled_by, &canceled)?;
                }
                tracing::info!("A batch of tasks was successfully completed with {success} successful tasks and {failure} failed tasks.");
            }
            // If we have an abortion error we must stop the tick here and re-schedule tasks.
            Err(Error::Milli {
                error: milli::Error::InternalError(milli::InternalError::AbortedIndexation),
                ..
            })
            | Err(Error::AbortedTask) => {
                #[cfg(test)]
                self.breakpoint(crate::test_utils::Breakpoint::AbortedIndexation);
                wtxn.abort();

                tracing::info!("A batch of tasks was aborted.");
                // We make sure that we don't call `stop_processing` on the `processing_tasks`,
                // this is because we want to let the next tick call `create_next_batch` and keep
                // the `started_at` date times and `processings` of the current processing tasks.
                // This date time is used by the task cancelation to store the right `started_at`
                // date in the task on disk.
                return Ok(TickOutcome::TickAgain(0));
            }
            // If an index said it was full, we need to:
            // 1. identify which index is full
            // 2. close the associated environment
            // 3. resize it
            // 4. re-schedule tasks
            Err(Error::Milli {
                error: milli::Error::UserError(milli::UserError::MaxDatabaseSizeReached),
                ..
            }) if index_uid.is_some() => {
                // fixme: add index_uid to match to avoid the unwrap
                let index_uid = index_uid.unwrap();
                // fixme: handle error more gracefully? not sure when this could happen
                self.index_mapper.resize_index(&wtxn, &index_uid)?;
                wtxn.abort();

                tracing::info!("The max database size was reached. Resizing the index.");

                return Ok(TickOutcome::TickAgain(0));
            }
            // In case of a failure we must get back and patch all the tasks with the error.
            Err(err) => {
                #[cfg(test)]
                self.breakpoint(crate::test_utils::Breakpoint::ProcessBatchFailed);
                let (task_progress, task_progress_obj) = AtomicTaskStep::new(ids.len() as u32);
                progress.update_progress(task_progress_obj);

                let error: ResponseError = err.into();
                for id in ids.iter() {
                    task_progress.fetch_add(1, Ordering::Relaxed);
                    let mut task = self
                        .queue
                        .tasks
                        .get_task(&wtxn, id)
                        .map_err(|e| Error::TaskDatabaseUpdate(Box::new(e)))?
                        .ok_or(Error::CorruptedTaskQueue)?;
                    task.status = Status::Failed;
                    task.error = Some(error.clone());
                    task.details = task.details.map(|d| d.to_failed());
                    processing_batch.update(&mut task);

                    #[cfg(test)]
                    self.maybe_fail(
                        crate::test_utils::FailureLocation::UpdatingTaskAfterProcessBatchFailure,
                    )?;

                    tracing::error!("Batch failed {}", error);

                    self.queue
                        .tasks
                        .update_task(&mut wtxn, &task)
                        .map_err(|e| Error::TaskDatabaseUpdate(Box::new(e)))?;
                }
            }
        }

        // We must re-add the canceled task so they're part of the same batch.
        ids |= canceled;
        self.queue.write_batch(&mut wtxn, processing_batch, &ids)?;

        #[cfg(test)]
        self.maybe_fail(crate::test_utils::FailureLocation::CommittingWtxn)?;

        wtxn.commit().map_err(Error::HeedTransaction)?;

        // We should stop processing AFTER everything is processed and written to disk otherwise, a batch (which only lives in RAM) may appear in the processing task
        // and then become « not found » for some time until the commit everything is written and the final commit is made.
        self.processing_tasks.write().unwrap().stop_processing();

        // Once the tasks are committed, we should delete all the update files associated ASAP to avoid leaking files in case of a restart
        tracing::debug!("Deleting the update files");

        //We take one read transaction **per thread**. Then, every thread is going to pull out new IDs from the roaring bitmap with the help of an atomic shared index into the bitmap
        let idx = AtomicU32::new(0);
        (0..current_num_threads()).into_par_iter().try_for_each(|_| -> Result<()> {
            let rtxn = self.read_txn()?;
            while let Some(id) = ids.select(idx.fetch_add(1, Ordering::Relaxed)) {
                let task = self
                    .queue
                    .tasks
                    .get_task(&rtxn, id)
                    .map_err(|e| Error::TaskDatabaseUpdate(Box::new(e)))?
                    .ok_or(Error::CorruptedTaskQueue)?;
                if let Err(e) = self.queue.delete_persisted_task_data(&task) {
                    tracing::error!(
                        "Failure to delete the content files associated with task {}. Error: {e}",
                        task.uid
                    );
                }
            }
            Ok(())
        })?;

        // We shouldn't crash the tick function if we can't send data to the webhook.
        let _ = self.notify_webhook(&ids);

        #[cfg(test)]
        self.breakpoint(crate::test_utils::Breakpoint::AfterProcessing);

        Ok(TickOutcome::TickAgain(processed_tasks))
    }
}
