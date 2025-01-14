/*!
This crate defines the index scheduler, which is responsible for:
1. Keeping references to meilisearch's indexes and mapping them to their
   user-defined names.
2. Scheduling tasks given by the user and executing them, in batch if possible.

When an `IndexScheduler` is created, a new thread containing a reference to the
scheduler is created. This thread runs the scheduler's run loop, where the
scheduler waits to be woken up to process new tasks. It wakes up when:

1. it is launched for the first time
2. a new task is registered
3. a batch of tasks has been processed

It is only within this thread that the scheduler is allowed to process tasks.
On the other hand, the publicly accessible methods of the scheduler can be
called asynchronously from any thread. These methods can either query the
content of the scheduler or enqueue new tasks.
*/

mod dump;
pub mod error;
mod features;
mod index_mapper;
#[cfg(test)]
mod insta_snapshot;
mod lru;
mod processing;
mod queue;
mod scheduler;
#[cfg(test)]
mod test_utils;
pub mod upgrade;
mod utils;
pub mod uuid_codec;

pub type Result<T, E = Error> = std::result::Result<T, E>;
pub type TaskId = u32;

use std::collections::{BTreeMap, HashMap};
use std::io::{self, BufReader, Read};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use dump::Dump;
pub use error::Error;
pub use features::RoFeatures;
use flate2::bufread::GzEncoder;
use flate2::Compression;
use meilisearch_types::batches::Batch;
use meilisearch_types::features::{InstanceTogglableFeatures, RuntimeTogglableFeatures};
use meilisearch_types::heed::byteorder::BE;
use meilisearch_types::heed::types::I128;
use meilisearch_types::heed::{self, Env, RoTxn};
use meilisearch_types::milli::index::IndexEmbeddingConfig;
use meilisearch_types::milli::update::IndexerConfig;
use meilisearch_types::milli::vector::{Embedder, EmbedderOptions, EmbeddingConfigs};
use meilisearch_types::milli::{self, Index};
use meilisearch_types::task_view::TaskView;
use meilisearch_types::tasks::{KindWithContent, Task};
use processing::ProcessingTasks;
pub use queue::Query;
use queue::Queue;
use roaring::RoaringBitmap;
use scheduler::Scheduler;
use time::OffsetDateTime;

use crate::index_mapper::IndexMapper;
use crate::utils::clamp_to_page_size;

pub(crate) type BEI128 = I128<BE>;

#[derive(Debug)]
pub struct IndexSchedulerOptions {
    /// The path to the version file of Meilisearch.
    pub version_file_path: PathBuf,
    /// The path to the folder containing the auth LMDB env.
    pub auth_path: PathBuf,
    /// The path to the folder containing the task databases.
    pub tasks_path: PathBuf,
    /// The path to the file store containing the files associated to the tasks.
    pub update_file_path: PathBuf,
    /// The path to the folder containing meilisearch's indexes.
    pub indexes_path: PathBuf,
    /// The path to the folder containing the snapshots.
    pub snapshots_path: PathBuf,
    /// The path to the folder containing the dumps.
    pub dumps_path: PathBuf,
    /// The URL on which we must send the tasks statuses
    pub webhook_url: Option<String>,
    /// The value we will send into the Authorization HTTP header on the webhook URL
    pub webhook_authorization_header: Option<String>,
    /// The maximum size, in bytes, of the task index.
    pub task_db_size: usize,
    /// The size, in bytes, with which a meilisearch index is opened the first time of each meilisearch index.
    pub index_base_map_size: usize,
    /// Whether we open a meilisearch index with the MDB_WRITEMAP option or not.
    pub enable_mdb_writemap: bool,
    /// The size, in bytes, by which the map size of an index is increased when it resized due to being full.
    pub index_growth_amount: usize,
    /// The number of indexes that can be concurrently opened in memory.
    pub index_count: usize,
    /// Configuration used during indexing for each meilisearch index.
    pub indexer_config: Arc<IndexerConfig>,
    /// Set to `true` iff the index scheduler is allowed to automatically
    /// batch tasks together, to process multiple tasks at once.
    pub autobatching_enabled: bool,
    /// Set to `true` iff the index scheduler is allowed to automatically
    /// delete the finished tasks when there are too many tasks.
    pub cleanup_enabled: bool,
    /// The maximum number of tasks stored in the task queue before starting
    /// to auto schedule task deletions.
    pub max_number_of_tasks: usize,
    /// If the autobatcher is allowed to automatically batch tasks
    /// it will only batch this defined number of tasks at once.
    pub max_number_of_batched_tasks: usize,
    /// If the autobatcher is allowed to automatically batch tasks
    /// it will only batch this defined maximum size (in bytes) of tasks at once.
    pub batched_tasks_size_limit: u64,
    /// The experimental features enabled for this instance.
    pub instance_features: InstanceTogglableFeatures,
    /// The experimental features enabled for this instance.
    pub auto_upgrade: bool,
}

/// Structure which holds meilisearch's indexes and schedules the tasks
/// to be performed on them.
pub struct IndexScheduler {
    /// The LMDB environment which the DBs are associated with.
    pub(crate) env: Env,

    /// The list of tasks currently processing
    pub(crate) processing_tasks: Arc<RwLock<ProcessingTasks>>,

    /// The queue containing both the tasks and the batches.
    pub queue: queue::Queue,

    pub scheduler: scheduler::Scheduler,

    /// In charge of creating, opening, storing and returning indexes.
    pub(crate) index_mapper: IndexMapper,

    /// In charge of fetching and setting the status of experimental features.
    features: features::FeatureData,

    /// Whether we should automatically cleanup the task queue or not.
    pub(crate) cleanup_enabled: bool,

    /// The webhook url we should send tasks to after processing every batches.
    pub(crate) webhook_url: Option<String>,
    /// The Authorization header to send to the webhook URL.
    pub(crate) webhook_authorization_header: Option<String>,

    embedders: Arc<RwLock<HashMap<EmbedderOptions, Arc<Embedder>>>>,

    // ================= test
    // The next entry is dedicated to the tests.
    /// Provide a way to set a breakpoint in multiple part of the scheduler.
    ///
    /// See [self.breakpoint()](`IndexScheduler::breakpoint`) for an explanation.
    #[cfg(test)]
    test_breakpoint_sdr: crossbeam_channel::Sender<(test_utils::Breakpoint, bool)>,

    /// A list of planned failures within the [`tick`](IndexScheduler::tick) method of the index scheduler.
    ///
    /// The first field is the iteration index and the second field identifies a location in the code.
    #[cfg(test)]
    planned_failures: Vec<(usize, test_utils::FailureLocation)>,

    /// A counter that is incremented before every call to [`tick`](IndexScheduler::tick)
    #[cfg(test)]
    run_loop_iteration: Arc<RwLock<usize>>,
}

impl IndexScheduler {
    fn private_clone(&self) -> IndexScheduler {
        IndexScheduler {
            env: self.env.clone(),
            processing_tasks: self.processing_tasks.clone(),
            queue: self.queue.private_clone(),
            scheduler: self.scheduler.private_clone(),

            index_mapper: self.index_mapper.clone(),
            cleanup_enabled: self.cleanup_enabled,
            webhook_url: self.webhook_url.clone(),
            webhook_authorization_header: self.webhook_authorization_header.clone(),
            embedders: self.embedders.clone(),
            #[cfg(test)]
            test_breakpoint_sdr: self.test_breakpoint_sdr.clone(),
            #[cfg(test)]
            planned_failures: self.planned_failures.clone(),
            #[cfg(test)]
            run_loop_iteration: self.run_loop_iteration.clone(),
            features: self.features.clone(),
        }
    }

    /// Create an index scheduler and start its run loop.
    #[allow(private_interfaces)] // because test_utils is private
    pub fn new(
        options: IndexSchedulerOptions,
        #[cfg(test)] test_breakpoint_sdr: crossbeam_channel::Sender<(test_utils::Breakpoint, bool)>,
        #[cfg(test)] planned_failures: Vec<(usize, test_utils::FailureLocation)>,
    ) -> Result<Self> {
        std::fs::create_dir_all(&options.tasks_path)?;
        std::fs::create_dir_all(&options.update_file_path)?;
        std::fs::create_dir_all(&options.indexes_path)?;
        std::fs::create_dir_all(&options.dumps_path)?;

        if cfg!(windows) && options.enable_mdb_writemap {
            // programmer error if this happens: in normal use passing the option on Windows is an error in main
            panic!("Windows doesn't support the MDB_WRITEMAP LMDB option");
        }

        let task_db_size = clamp_to_page_size(options.task_db_size);
        let budget = if options.indexer_config.skip_index_budget {
            IndexBudget {
                map_size: options.index_base_map_size,
                index_count: options.index_count,
                task_db_size,
            }
        } else {
            Self::index_budget(
                &options.tasks_path,
                options.index_base_map_size,
                task_db_size,
                options.index_count,
            )
        };

        let env = unsafe {
            heed::EnvOpenOptions::new()
                .max_dbs(19)
                .map_size(budget.task_db_size)
                .open(&options.tasks_path)
        }?;

        let features = features::FeatureData::new(&env, options.instance_features)?;

        let mut wtxn = env.write_txn()?;
        let queue = Queue::new(&env, &mut wtxn, &options)?;
        let index_mapper = IndexMapper::new(&env, &mut wtxn, &options, budget)?;
        wtxn.commit()?;

        // allow unreachable_code to get rids of the warning in the case of a test build.
        let this = Self {
            processing_tasks: Arc::new(RwLock::new(ProcessingTasks::new())),
            queue,
            scheduler: Scheduler::new(&options),

            index_mapper,
            env,
            cleanup_enabled: options.cleanup_enabled,
            webhook_url: options.webhook_url,
            webhook_authorization_header: options.webhook_authorization_header,
            embedders: Default::default(),

            #[cfg(test)]
            test_breakpoint_sdr,
            #[cfg(test)]
            planned_failures,
            #[cfg(test)]
            run_loop_iteration: Arc::new(RwLock::new(0)),
            features,
        };

        this.run();
        Ok(this)
    }

    /// Return `Ok(())` if the index scheduler is able to access one of its database.
    pub fn health(&self) -> Result<()> {
        let rtxn = self.env.read_txn()?;
        self.queue.batch_to_tasks_mapping.first(&rtxn)?;
        Ok(())
    }

    fn index_budget(
        tasks_path: &Path,
        base_map_size: usize,
        mut task_db_size: usize,
        max_index_count: usize,
    ) -> IndexBudget {
        #[cfg(windows)]
        const DEFAULT_BUDGET: usize = 6 * 1024 * 1024 * 1024 * 1024; // 6 TiB, 1 index
        #[cfg(not(windows))]
        const DEFAULT_BUDGET: usize = 80 * 1024 * 1024 * 1024 * 1024; // 80 TiB, 18 indexes

        let budget = if Self::is_good_heed(tasks_path, DEFAULT_BUDGET) {
            DEFAULT_BUDGET
        } else {
            tracing::debug!("determining budget with dichotomic search");
            utils::dichotomic_search(DEFAULT_BUDGET / 2, |map_size| {
                Self::is_good_heed(tasks_path, map_size)
            })
        };

        tracing::debug!("memmap budget: {budget}B");
        let mut budget = budget / 2;
        if task_db_size > (budget / 2) {
            task_db_size = clamp_to_page_size(budget * 2 / 5);
            tracing::debug!(
                "Decreasing max size of task DB to {task_db_size}B due to constrained memory space"
            );
        }
        budget -= task_db_size;

        // won't be mutated again
        let budget = budget;
        let task_db_size = task_db_size;

        tracing::debug!("index budget: {budget}B");
        let mut index_count = budget / base_map_size;
        if index_count < 2 {
            // take a bit less than half than the budget to make sure we can always afford to open an index
            let map_size = (budget * 2) / 5;
            // single index of max budget
            tracing::debug!("1 index of {map_size}B can be opened simultaneously.");
            return IndexBudget { map_size, index_count: 1, task_db_size };
        }
        // give us some space for an additional index when the cache is already full
        // decrement is OK because index_count >= 2.
        index_count -= 1;
        if index_count > max_index_count {
            index_count = max_index_count;
        }
        tracing::debug!("Up to {index_count} indexes of {base_map_size}B opened simultaneously.");
        IndexBudget { map_size: base_map_size, index_count, task_db_size }
    }

    fn is_good_heed(tasks_path: &Path, map_size: usize) -> bool {
        if let Ok(env) = unsafe {
            heed::EnvOpenOptions::new().map_size(clamp_to_page_size(map_size)).open(tasks_path)
        } {
            env.prepare_for_closing().wait();
            true
        } else {
            // We're treating all errors equally here, not only allocation errors.
            // This means there's a possiblity for the budget to lower due to errors different from allocation errors.
            // For persistent errors, this is OK as long as the task db is then reopened normally without ignoring the error this time.
            // For transient errors, this could lead to an instance with too low a budget.
            // However transient errors are: 1) less likely than persistent errors 2) likely to cause other issues down the line anyway.
            false
        }
    }

    pub fn read_txn(&self) -> Result<RoTxn> {
        self.env.read_txn().map_err(|e| e.into())
    }

    /// Start the run loop for the given index scheduler.
    ///
    /// This function will execute in a different thread and must be called
    /// only once per index scheduler.
    fn run(&self) {
        let run = self.private_clone();
        std::thread::Builder::new()
            .name(String::from("scheduler"))
            .spawn(move || {
                #[cfg(test)]
                run.breakpoint(test_utils::Breakpoint::Init);

                run.scheduler.wake_up.wait_timeout(std::time::Duration::from_secs(60));

                loop {
                    let ret = catch_unwind(AssertUnwindSafe(|| run.tick()));
                    match ret {
                        Ok(Ok(TickOutcome::TickAgain(_))) => (),
                        Ok(Ok(TickOutcome::WaitForSignal)) => run.scheduler.wake_up.wait(),
                        Ok(Err(e)) => {
                            tracing::error!("{e}");
                            // Wait one second when an irrecoverable error occurs.
                            if !e.is_recoverable() {
                                std::thread::sleep(Duration::from_secs(1));
                            }
                        }
                        Err(_panic) => {
                            tracing::error!("Internal error: Unexpected panic in the `IndexScheduler::run` method.");

                        }
                    }
                }
            })
            .unwrap();
    }

    pub fn indexer_config(&self) -> &IndexerConfig {
        &self.index_mapper.indexer_config
    }

    /// Return the real database size (i.e.: The size **with** the free pages)
    pub fn size(&self) -> Result<u64> {
        Ok(self.env.real_disk_size()?)
    }

    /// Return the used database size (i.e.: The size **without** the free pages)
    pub fn used_size(&self) -> Result<u64> {
        Ok(self.env.non_free_pages_size()?)
    }

    /// Return the index corresponding to the name.
    ///
    /// * If the index wasn't opened before, the index will be opened.
    /// * If the index doesn't exist on disk, the `IndexNotFoundError` is thrown.
    ///
    /// ### Note
    ///
    /// As an `Index` requires a large swath of the virtual memory address space, correct usage of an `Index` does not
    /// keep its handle for too long.
    ///
    /// Some configurations also can't reasonably open multiple indexes at once.
    /// If you need to fetch information from or perform an action on all indexes,
    /// see the `try_for_each_index` function.
    pub fn index(&self, name: &str) -> Result<Index> {
        self.index_mapper.index(&self.env.read_txn()?, name)
    }

    /// Return the boolean referring if index exists.
    pub fn index_exists(&self, name: &str) -> Result<bool> {
        self.index_mapper.index_exists(&self.env.read_txn()?, name)
    }

    /// Return the name of all indexes without opening them.
    pub fn index_names(&self) -> Result<Vec<String>> {
        let rtxn = self.env.read_txn()?;
        self.index_mapper.index_names(&rtxn)
    }

    /// Attempts `f` for each index that exists known to the index scheduler.
    ///
    /// It is preferable to use this function rather than a loop that opens all indexes, as a way to avoid having all indexes opened,
    /// which is unsupported in general.
    ///
    /// Since `f` is allowed to return a result, and `Index` is cloneable, it is still possible to wrongly build e.g. a vector of
    /// all the indexes, but this function makes it harder and so less likely to do accidentally.
    ///
    /// If many indexes exist, this operation can take time to complete (in the order of seconds for a 1000 of indexes) as it needs to open
    /// all the indexes.
    pub fn try_for_each_index<U, V>(&self, f: impl FnMut(&str, &Index) -> Result<U>) -> Result<V>
    where
        V: FromIterator<U>,
    {
        let rtxn = self.env.read_txn()?;
        self.index_mapper.try_for_each_index(&rtxn, f)
    }

    /// Returns the total number of indexes available for the specified filter.
    /// And a `Vec` of the index_uid + its stats
    pub fn get_paginated_indexes_stats(
        &self,
        filters: &meilisearch_auth::AuthFilter,
        from: usize,
        limit: usize,
    ) -> Result<(usize, Vec<(String, index_mapper::IndexStats)>)> {
        let rtxn = self.read_txn()?;

        let mut total = 0;
        let mut iter = self
            .index_mapper
            .index_mapping
            .iter(&rtxn)?
            // in case of an error we want to keep the value to return it
            .filter(|ret| {
                ret.as_ref().map_or(true, |(name, _uuid)| filters.is_index_authorized(name))
            })
            .inspect(|_| total += 1)
            .skip(from);
        let ret = iter
            .by_ref()
            .take(limit)
            .map(|ret| ret.map_err(Error::from))
            .map(|ret| {
                ret.and_then(|(name, uuid)| {
                    self.index_mapper.index_stats.get(&rtxn, &uuid).map_err(Error::from).and_then(
                        |stat| {
                            stat.map(|stat| (name.to_string(), stat))
                                .ok_or(Error::CorruptedTaskQueue)
                        },
                    )
                })
            })
            .collect::<Result<Vec<(String, index_mapper::IndexStats)>>>();

        // We must iterate on the rest of the indexes to compute the total
        iter.for_each(drop);

        ret.map(|ret| (total, ret))
    }

    /// The returned structure contains:
    /// 1. The name of the property being observed can be `statuses`, `types`, or `indexes`.
    /// 2. The name of the specific data related to the property can be `enqueued` for the `statuses`, `settingsUpdate` for the `types`, or the name of the index for the `indexes`, for example.
    /// 3. The number of times the properties appeared.
    pub fn get_stats(&self) -> Result<BTreeMap<String, BTreeMap<String, u64>>> {
        self.queue.get_stats(&self.read_txn()?, &self.processing_tasks.read().unwrap())
    }

    // Return true if there is at least one task that is processing.
    pub fn is_task_processing(&self) -> Result<bool> {
        Ok(!self.processing_tasks.read().unwrap().processing.is_empty())
    }

    /// Return true iff there is at least one task associated with this index
    /// that is processing.
    pub fn is_index_processing(&self, index: &str) -> Result<bool> {
        let rtxn = self.env.read_txn()?;
        let processing_tasks = self.processing_tasks.read().unwrap().processing.clone();
        let index_tasks = self.queue.tasks.index_tasks(&rtxn, index)?;
        let nbr_index_processing_tasks = processing_tasks.intersection_len(&index_tasks);
        Ok(nbr_index_processing_tasks > 0)
    }

    /// Return the tasks matching the query from the user's point of view along
    /// with the total number of tasks matching the query, ignoring from and limit.
    ///
    /// There are two differences between an internal query and a query executed by
    /// the user.
    ///
    /// 1. IndexSwap tasks are not publicly associated with any index, but they are associated
    ///    with many indexes internally.
    /// 2. The user may not have the rights to access the tasks (internally) associated with all indexes.
    pub fn get_tasks_from_authorized_indexes(
        &self,
        query: &Query,
        filters: &meilisearch_auth::AuthFilter,
    ) -> Result<(Vec<Task>, u64)> {
        let rtxn = self.read_txn()?;
        let processing = self.processing_tasks.read().unwrap();
        self.queue.get_tasks_from_authorized_indexes(&rtxn, query, filters, &processing)
    }

    /// Return the task ids matching the query along with the total number of tasks
    /// by ignoring the from and limit parameters from the user's point of view.
    ///
    /// There are two differences between an internal query and a query executed by
    /// the user.
    ///
    /// 1. IndexSwap tasks are not publicly associated with any index, but they are associated
    ///    with many indexes internally.
    /// 2. The user may not have the rights to access the tasks (internally) associated with all indexes.
    pub fn get_task_ids_from_authorized_indexes(
        &self,
        query: &Query,
        filters: &meilisearch_auth::AuthFilter,
    ) -> Result<(RoaringBitmap, u64)> {
        let rtxn = self.read_txn()?;
        let processing = self.processing_tasks.read().unwrap();
        self.queue.get_task_ids_from_authorized_indexes(&rtxn, query, filters, &processing)
    }

    /// Return the batches matching the query from the user's point of view along
    /// with the total number of batches matching the query, ignoring from and limit.
    ///
    /// There are two differences between an internal query and a query executed by
    /// the user.
    ///
    /// 1. IndexSwap tasks are not publicly associated with any index, but they are associated
    ///    with many indexes internally.
    /// 2. The user may not have the rights to access the tasks (internally) associated with all indexes.
    pub fn get_batches_from_authorized_indexes(
        &self,
        query: &Query,
        filters: &meilisearch_auth::AuthFilter,
    ) -> Result<(Vec<Batch>, u64)> {
        let rtxn = self.read_txn()?;
        let processing = self.processing_tasks.read().unwrap();
        self.queue.get_batches_from_authorized_indexes(&rtxn, query, filters, &processing)
    }

    /// Return the batch ids matching the query along with the total number of batches
    /// by ignoring the from and limit parameters from the user's point of view.
    ///
    /// There are two differences between an internal query and a query executed by
    /// the user.
    ///
    /// 1. IndexSwap tasks are not publicly associated with any index, but they are associated
    ///    with many indexes internally.
    /// 2. The user may not have the rights to access the tasks (internally) associated with all indexes.
    pub fn get_batch_ids_from_authorized_indexes(
        &self,
        query: &Query,
        filters: &meilisearch_auth::AuthFilter,
    ) -> Result<(RoaringBitmap, u64)> {
        let rtxn = self.read_txn()?;
        let processing = self.processing_tasks.read().unwrap();
        self.queue.get_batch_ids_from_authorized_indexes(&rtxn, query, filters, &processing)
    }

    /// Register a new task in the scheduler.
    ///
    /// If it fails and data was associated with the task, it tries to delete the associated data.
    pub fn register(
        &self,
        kind: KindWithContent,
        task_id: Option<TaskId>,
        dry_run: bool,
    ) -> Result<Task> {
        // if the task doesn't delete anything and 50% of the task queue is full, we must refuse to enqueue the incomming task
        if !matches!(&kind, KindWithContent::TaskDeletion { tasks, .. } if !tasks.is_empty())
            && (self.env.non_free_pages_size()? * 100) / self.env.info().map_size as u64 > 40
        {
            return Err(Error::NoSpaceLeftInTaskQueue);
        }

        let mut wtxn = self.env.write_txn()?;
        let task = self.queue.register(&mut wtxn, &kind, task_id, dry_run)?;

        // If the registered task is a task cancelation
        // we inform the processing tasks to stop (if necessary).
        if let KindWithContent::TaskCancelation { tasks, .. } = kind {
            let tasks_to_cancel = RoaringBitmap::from_iter(tasks);
            if self.processing_tasks.read().unwrap().must_cancel_processing_tasks(&tasks_to_cancel)
            {
                self.scheduler.must_stop_processing.must_stop();
            }
        }

        if let Err(e) = wtxn.commit() {
            self.queue.delete_persisted_task_data(&task)?;
            return Err(e.into());
        }

        // notify the scheduler loop to execute a new tick
        self.scheduler.wake_up.signal();
        Ok(task)
    }

    /// Register a new task coming from a dump in the scheduler.
    /// By taking a mutable ref we're pretty sure no one will ever import a dump while actix is running.
    pub fn register_dumped_task(&mut self) -> Result<Dump> {
        Dump::new(self)
    }

    /// Create a new index without any associated task.
    pub fn create_raw_index(
        &self,
        name: &str,
        date: Option<(OffsetDateTime, OffsetDateTime)>,
    ) -> Result<Index> {
        let wtxn = self.env.write_txn()?;
        let index = self.index_mapper.create_index(wtxn, name, date)?;
        Ok(index)
    }

    pub fn refresh_index_stats(&self, name: &str) -> Result<()> {
        let mut mapper_wtxn = self.env.write_txn()?;
        let index = self.index_mapper.index(&mapper_wtxn, name)?;
        let index_rtxn = index.read_txn()?;

        let stats = crate::index_mapper::IndexStats::new(&index, &index_rtxn)
            .map_err(|e| Error::from_milli(e, Some(name.to_string())))?;

        self.index_mapper.store_stats_of(&mut mapper_wtxn, name, &stats)?;
        mapper_wtxn.commit()?;
        Ok(())
    }

    /// Once the tasks changes have been committed we must send all the tasks that were updated to our webhook if there is one.
    fn notify_webhook(&self, updated: &RoaringBitmap) -> Result<()> {
        if let Some(ref url) = self.webhook_url {
            struct TaskReader<'a, 'b> {
                rtxn: &'a RoTxn<'a>,
                index_scheduler: &'a IndexScheduler,
                tasks: &'b mut roaring::bitmap::Iter<'b>,
                buffer: Vec<u8>,
                written: usize,
            }

            impl<'a, 'b> Read for TaskReader<'a, 'b> {
                fn read(&mut self, mut buf: &mut [u8]) -> std::io::Result<usize> {
                    if self.buffer.is_empty() {
                        match self.tasks.next() {
                            None => return Ok(0),
                            Some(task_id) => {
                                let task = self
                                    .index_scheduler
                                    .queue
                                    .tasks
                                    .get_task(self.rtxn, task_id)
                                    .map_err(|err| io::Error::new(io::ErrorKind::Other, err))?
                                    .ok_or_else(|| {
                                        io::Error::new(
                                            io::ErrorKind::Other,
                                            Error::CorruptedTaskQueue,
                                        )
                                    })?;

                                serde_json::to_writer(
                                    &mut self.buffer,
                                    &TaskView::from_task(&task),
                                )?;
                                self.buffer.push(b'\n');
                            }
                        }
                    }

                    let mut to_write = &self.buffer[self.written..];
                    let wrote = io::copy(&mut to_write, &mut buf)?;
                    self.written += wrote as usize;

                    // we wrote everything and must refresh our buffer on the next call
                    if self.written == self.buffer.len() {
                        self.written = 0;
                        self.buffer.clear();
                    }

                    Ok(wrote as usize)
                }
            }

            let rtxn = self.env.read_txn()?;

            let task_reader = TaskReader {
                rtxn: &rtxn,
                index_scheduler: self,
                tasks: &mut updated.into_iter(),
                buffer: Vec::with_capacity(50), // on average a task is around ~100 bytes
                written: 0,
            };

            // let reader = GzEncoder::new(BufReader::new(task_reader), Compression::default());
            let reader = GzEncoder::new(BufReader::new(task_reader), Compression::default());
            let request = ureq::post(url)
                .timeout(Duration::from_secs(30))
                .set("Content-Encoding", "gzip")
                .set("Content-Type", "application/x-ndjson");
            let request = match &self.webhook_authorization_header {
                Some(header) => request.set("Authorization", header),
                None => request,
            };

            if let Err(e) = request.send(reader) {
                tracing::error!("While sending data to the webhook: {e}");
            }
        }

        Ok(())
    }

    pub fn index_stats(&self, index_uid: &str) -> Result<IndexStats> {
        let is_indexing = self.is_index_processing(index_uid)?;
        let rtxn = self.read_txn()?;
        let index_stats = self.index_mapper.stats_of(&rtxn, index_uid)?;

        Ok(IndexStats { is_indexing, inner_stats: index_stats })
    }

    pub fn features(&self) -> RoFeatures {
        self.features.features()
    }

    pub fn put_runtime_features(&self, features: RuntimeTogglableFeatures) -> Result<()> {
        let wtxn = self.env.write_txn().map_err(Error::HeedTransaction)?;
        self.features.put_runtime_features(wtxn, features)?;
        Ok(())
    }

    // TODO: consider using a type alias or a struct embedder/template
    pub fn embedders(
        &self,
        index_uid: String,
        embedding_configs: Vec<IndexEmbeddingConfig>,
    ) -> Result<EmbeddingConfigs> {
        let res: Result<_> = embedding_configs
            .into_iter()
            .map(
                |IndexEmbeddingConfig {
                     name,
                     config: milli::vector::EmbeddingConfig { embedder_options, prompt, quantized },
                     ..
                 }| {
                    let prompt = Arc::new(
                        prompt
                            .try_into()
                            .map_err(meilisearch_types::milli::Error::from)
                            .map_err(|err| Error::from_milli(err, Some(index_uid.clone())))?,
                    );
                    // optimistically return existing embedder
                    {
                        let embedders = self.embedders.read().unwrap();
                        if let Some(embedder) = embedders.get(&embedder_options) {
                            return Ok((
                                name,
                                (embedder.clone(), prompt, quantized.unwrap_or_default()),
                            ));
                        }
                    }

                    // add missing embedder
                    let embedder = Arc::new(
                        Embedder::new(embedder_options.clone())
                            .map_err(meilisearch_types::milli::vector::Error::from)
                            .map_err(|err| {
                                Error::from_milli(err.into(), Some(index_uid.clone()))
                            })?,
                    );
                    {
                        let mut embedders = self.embedders.write().unwrap();
                        embedders.insert(embedder_options, embedder.clone());
                    }
                    Ok((name, (embedder, prompt, quantized.unwrap_or_default())))
                },
            )
            .collect();
        res.map(EmbeddingConfigs::new)
    }
}

/// The outcome of calling the [`IndexScheduler::tick`] function.
pub enum TickOutcome {
    /// The scheduler should immediately attempt another `tick`.
    ///
    /// The `usize` field contains the number of processed tasks.
    TickAgain(u64),
    /// The scheduler should wait for an external signal before attempting another `tick`.
    WaitForSignal,
}

/// How many indexes we can afford to have open simultaneously.
struct IndexBudget {
    /// Map size of an index.
    map_size: usize,
    /// Maximum number of simultaneously opened indexes.
    index_count: usize,
    /// For very constrained systems we might need to reduce the base task_db_size so we can accept at least one index.
    task_db_size: usize,
}

/// The statistics that can be computed from an `Index` object and the scheduler.
///
/// Compared with `index_mapper::IndexStats`, it adds the scheduling status.
#[derive(Debug)]
pub struct IndexStats {
    /// Whether this index is currently performing indexation, according to the scheduler.
    pub is_indexing: bool,
    /// Internal stats computed from the index.
    pub inner_stats: index_mapper::IndexStats,
}
