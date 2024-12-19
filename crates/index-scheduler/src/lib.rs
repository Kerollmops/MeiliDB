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

mod autobatcher;
mod batch;
pub mod error;
mod features;
mod index_mapper;
#[cfg(test)]
mod insta_snapshot;
mod lru;
mod processing;
mod utils;
pub mod uuid_codec;

pub type Result<T> = std::result::Result<T, Error>;
pub type TaskId = u32;

use std::collections::{BTreeMap, HashMap};
use std::io::{self, BufReader, Read};
use std::ops::{Bound, RangeBounds};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering::{self, Relaxed};
use std::sync::atomic::{AtomicBool, AtomicU32};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use dump::{KindDump, TaskDump, UpdateFile};
pub use error::Error;
pub use features::RoFeatures;
use file_store::FileStore;
use flate2::bufread::GzEncoder;
use flate2::Compression;
use meilisearch_types::batches::{Batch, BatchId};
use meilisearch_types::error::ResponseError;
use meilisearch_types::features::{InstanceTogglableFeatures, RuntimeTogglableFeatures};
use meilisearch_types::heed::byteorder::BE;
use meilisearch_types::heed::types::{SerdeBincode, SerdeJson, Str, I128};
use meilisearch_types::heed::{self, Database, Env, PutFlags, RoTxn, RwTxn};
use meilisearch_types::milli::documents::DocumentsBatchBuilder;
use meilisearch_types::milli::index::IndexEmbeddingConfig;
use meilisearch_types::milli::update::IndexerConfig;
use meilisearch_types::milli::vector::{Embedder, EmbedderOptions, EmbeddingConfigs};
use meilisearch_types::milli::{self, CboRoaringBitmapCodec, Index, RoaringBitmapCodec, BEU32};
use meilisearch_types::task_view::TaskView;
use meilisearch_types::tasks::{Kind, KindWithContent, Status, Task};
use processing::ProcessingTasks;
use rayon::current_num_threads;
use rayon::prelude::{IntoParallelIterator, ParallelIterator};
use roaring::RoaringBitmap;
use synchronoise::SignalEvent;
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;
use utils::{filter_out_references_to_newer_tasks, keep_ids_within_datetimes, map_bound};
use uuid::Uuid;

use crate::index_mapper::IndexMapper;
use crate::processing::{AtomicTaskStep, BatchProgress};
use crate::utils::{check_index_swap_validity, clamp_to_page_size};

pub(crate) type BEI128 = I128<BE>;

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

#[derive(Default, Clone, Debug)]
struct MustStopProcessing(Arc<AtomicBool>);

impl MustStopProcessing {
    fn get(&self) -> bool {
        self.0.load(Relaxed)
    }

    fn must_stop(&self) {
        self.0.store(true, Relaxed);
    }

    fn reset(&self) {
        self.0.store(false, Relaxed);
    }
}

/// Database const names for the `IndexScheduler`.
mod db_name {
    pub const ALL_TASKS: &str = "all-tasks";
    pub const ALL_BATCHES: &str = "all-batches";
    pub const BATCH_TO_TASKS_MAPPING: &str = "batch-to-tasks-mapping";
    pub const STATUS: &str = "status";
    pub const KIND: &str = "kind";
    pub const INDEX_TASKS: &str = "index-tasks";
    pub const CANCELED_BY: &str = "canceled_by";
    pub const ENQUEUED_AT: &str = "enqueued-at";
    pub const STARTED_AT: &str = "started-at";
    pub const FINISHED_AT: &str = "finished-at";

    pub const BATCH_STATUS: &str = "batch-status";
    pub const BATCH_KIND: &str = "batch-kind";
    pub const BATCH_INDEX_TASKS: &str = "batch-index-tasks";
    pub const BATCH_ENQUEUED_AT: &str = "batch-enqueued-at";
    pub const BATCH_STARTED_AT: &str = "batch-started-at";
    pub const BATCH_FINISHED_AT: &str = "batch-finished-at";
}

#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Breakpoint {
    // this state is only encountered while creating the scheduler in the test suite.
    Init,

    Start,
    BatchCreated,
    BeforeProcessing,
    AfterProcessing,
    AbortedIndexation,
    ProcessBatchSucceeded,
    ProcessBatchFailed,
    InsideProcessBatch,
}

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
    pub indexer_config: IndexerConfig,
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
    /// The experimental features enabled for this instance.
    pub instance_features: InstanceTogglableFeatures,
}

/// Structure which holds meilisearch's indexes and schedules the tasks
/// to be performed on them.
pub struct IndexScheduler {
    /// The LMDB environment which the DBs are associated with.
    pub(crate) env: Env,

    /// A boolean that can be set to true to stop the currently processing tasks.
    pub(crate) must_stop_processing: MustStopProcessing,

    /// The list of tasks currently processing
    pub(crate) processing_tasks: Arc<RwLock<ProcessingTasks>>,

    /// The list of files referenced by the tasks
    pub(crate) file_store: FileStore,

    /// The main database, it contains all the tasks accessible by their Id.
    pub(crate) all_tasks: Database<BEU32, SerdeJson<Task>>,

    /// Contains all the batches accessible by their Id.
    pub(crate) all_batches: Database<BEU32, SerdeJson<Batch>>,

    /// Matches a batch id with the associated task ids.
    pub(crate) batch_to_tasks_mapping: Database<BEU32, CboRoaringBitmapCodec>,

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

    /// All the batches containing a task matching the selected status.
    pub(crate) batch_status: Database<SerdeBincode<Status>, RoaringBitmapCodec>,
    /// All the batches ids grouped by the kind of their task.
    pub(crate) batch_kind: Database<SerdeBincode<Kind>, RoaringBitmapCodec>,
    /// Store the batches associated to an index.
    pub(crate) batch_index_tasks: Database<Str, RoaringBitmapCodec>,
    /// Store the batches containing tasks which were enqueued at a specific date
    pub(crate) batch_enqueued_at: Database<BEI128, CboRoaringBitmapCodec>,
    /// Store the batches containing finished tasks started at a specific date
    pub(crate) batch_started_at: Database<BEI128, CboRoaringBitmapCodec>,
    /// Store the batches containing tasks finished at a specific date
    pub(crate) batch_finished_at: Database<BEI128, CboRoaringBitmapCodec>,

    /// In charge of creating, opening, storing and returning indexes.
    pub(crate) index_mapper: IndexMapper,

    /// In charge of fetching and setting the status of experimental features.
    features: features::FeatureData,

    /// Get a signal when a batch needs to be processed.
    pub(crate) wake_up: Arc<SignalEvent>,

    /// Whether auto-batching is enabled or not.
    pub(crate) autobatching_enabled: bool,

    /// Whether we should automatically cleanup the task queue or not.
    pub(crate) cleanup_enabled: bool,

    /// The max number of tasks allowed before the scheduler starts to delete
    /// the finished tasks automatically.
    pub(crate) max_number_of_tasks: usize,

    /// The maximum number of tasks that will be batched together.
    pub(crate) max_number_of_batched_tasks: usize,

    /// The webhook url we should send tasks to after processing every batches.
    pub(crate) webhook_url: Option<String>,
    /// The Authorization header to send to the webhook URL.
    pub(crate) webhook_authorization_header: Option<String>,

    /// The path used to create the dumps.
    pub(crate) dumps_path: PathBuf,

    /// The path used to create the snapshots.
    pub(crate) snapshots_path: PathBuf,

    /// The path to the folder containing the auth LMDB env.
    pub(crate) auth_path: PathBuf,

    /// The path to the version file of Meilisearch.
    pub(crate) version_file_path: PathBuf,

    embedders: Arc<RwLock<HashMap<EmbedderOptions, Arc<Embedder>>>>,

    // ================= test
    // The next entry is dedicated to the tests.
    /// Provide a way to set a breakpoint in multiple part of the scheduler.
    ///
    /// See [self.breakpoint()](`IndexScheduler::breakpoint`) for an explanation.
    #[cfg(test)]
    test_breakpoint_sdr: crossbeam_channel::Sender<(Breakpoint, bool)>,

    /// A list of planned failures within the [`tick`](IndexScheduler::tick) method of the index scheduler.
    ///
    /// The first field is the iteration index and the second field identifies a location in the code.
    #[cfg(test)]
    planned_failures: Vec<(usize, tests::FailureLocation)>,

    /// A counter that is incremented before every call to [`tick`](IndexScheduler::tick)
    #[cfg(test)]
    run_loop_iteration: Arc<RwLock<usize>>,
}

impl IndexScheduler {
    fn private_clone(&self) -> IndexScheduler {
        IndexScheduler {
            env: self.env.clone(),
            must_stop_processing: self.must_stop_processing.clone(),
            processing_tasks: self.processing_tasks.clone(),
            file_store: self.file_store.clone(),
            all_tasks: self.all_tasks,
            all_batches: self.all_batches,
            batch_to_tasks_mapping: self.batch_to_tasks_mapping,

            // Tasks reverse index
            status: self.status,
            kind: self.kind,
            index_tasks: self.index_tasks,
            canceled_by: self.canceled_by,
            enqueued_at: self.enqueued_at,
            started_at: self.started_at,
            finished_at: self.finished_at,

            // Batches reverse index
            batch_status: self.batch_status,
            batch_kind: self.batch_kind,
            batch_index_tasks: self.batch_index_tasks,
            batch_enqueued_at: self.batch_enqueued_at,
            batch_started_at: self.batch_started_at,
            batch_finished_at: self.batch_finished_at,

            index_mapper: self.index_mapper.clone(),
            wake_up: self.wake_up.clone(),
            autobatching_enabled: self.autobatching_enabled,
            cleanup_enabled: self.cleanup_enabled,
            max_number_of_tasks: self.max_number_of_tasks,
            max_number_of_batched_tasks: self.max_number_of_batched_tasks,
            snapshots_path: self.snapshots_path.clone(),
            dumps_path: self.dumps_path.clone(),
            auth_path: self.auth_path.clone(),
            version_file_path: self.version_file_path.clone(),
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
}

impl IndexScheduler {
    /// Create an index scheduler and start its run loop.
    pub fn new(
        options: IndexSchedulerOptions,
        #[cfg(test)] test_breakpoint_sdr: crossbeam_channel::Sender<(Breakpoint, bool)>,
        #[cfg(test)] planned_failures: Vec<(usize, tests::FailureLocation)>,
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
                .open(options.tasks_path)
        }?;

        let features = features::FeatureData::new(&env, options.instance_features)?;

        let file_store = FileStore::new(&options.update_file_path)?;

        let mut wtxn = env.write_txn()?;
        let all_tasks = env.create_database(&mut wtxn, Some(db_name::ALL_TASKS))?;
        let all_batches = env.create_database(&mut wtxn, Some(db_name::ALL_BATCHES))?;
        let batch_to_tasks_mapping =
            env.create_database(&mut wtxn, Some(db_name::BATCH_TO_TASKS_MAPPING))?;

        let status = env.create_database(&mut wtxn, Some(db_name::STATUS))?;
        let kind = env.create_database(&mut wtxn, Some(db_name::KIND))?;
        let index_tasks = env.create_database(&mut wtxn, Some(db_name::INDEX_TASKS))?;
        let canceled_by = env.create_database(&mut wtxn, Some(db_name::CANCELED_BY))?;
        let enqueued_at = env.create_database(&mut wtxn, Some(db_name::ENQUEUED_AT))?;
        let started_at = env.create_database(&mut wtxn, Some(db_name::STARTED_AT))?;
        let finished_at = env.create_database(&mut wtxn, Some(db_name::FINISHED_AT))?;

        let batch_status = env.create_database(&mut wtxn, Some(db_name::BATCH_STATUS))?;
        let batch_kind = env.create_database(&mut wtxn, Some(db_name::BATCH_KIND))?;
        let batch_index_tasks = env.create_database(&mut wtxn, Some(db_name::BATCH_INDEX_TASKS))?;
        let batch_enqueued_at = env.create_database(&mut wtxn, Some(db_name::BATCH_ENQUEUED_AT))?;
        let batch_started_at = env.create_database(&mut wtxn, Some(db_name::BATCH_STARTED_AT))?;
        let batch_finished_at = env.create_database(&mut wtxn, Some(db_name::BATCH_FINISHED_AT))?;
        wtxn.commit()?;

        // allow unreachable_code to get rids of the warning in the case of a test build.
        let this = Self {
            must_stop_processing: MustStopProcessing::default(),
            processing_tasks: Arc::new(RwLock::new(ProcessingTasks::new())),
            file_store,
            all_tasks,
            all_batches,
            batch_to_tasks_mapping,
            // Task reverse indexes
            status,
            kind,
            index_tasks,
            canceled_by,
            enqueued_at,
            started_at,
            finished_at,

            // Batch reverse indexes
            batch_status,
            batch_kind,
            batch_index_tasks,
            batch_enqueued_at,
            batch_started_at,
            batch_finished_at,

            index_mapper: IndexMapper::new(
                &env,
                options.indexes_path,
                budget.map_size,
                options.index_growth_amount,
                budget.index_count,
                options.enable_mdb_writemap,
                options.indexer_config,
            )?,
            env,
            // we want to start the loop right away in case meilisearch was ctrl+Ced while processing things
            wake_up: Arc::new(SignalEvent::auto(true)),
            autobatching_enabled: options.autobatching_enabled,
            cleanup_enabled: options.cleanup_enabled,
            max_number_of_tasks: options.max_number_of_tasks,
            max_number_of_batched_tasks: options.max_number_of_batched_tasks,
            dumps_path: options.dumps_path,
            snapshots_path: options.snapshots_path,
            auth_path: options.auth_path,
            version_file_path: options.version_file_path,
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
        self.all_tasks.first(&rtxn)?;
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
                run.breakpoint(Breakpoint::Init);

                run.wake_up.wait_timeout(std::time::Duration::from_secs(60));

                loop {
                    let ret = catch_unwind(AssertUnwindSafe(|| run.tick()));
                    match ret {
                        Ok(Ok(TickOutcome::TickAgain(_))) => (),
                        Ok(Ok(TickOutcome::WaitForSignal)) => run.wake_up.wait(),
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
        let rtxn = self.env.read_txn()?;
        self.index_mapper.index(&rtxn, name)
    }
    /// Return the boolean referring if index exists.
    pub fn index_exists(&self, name: &str) -> Result<bool> {
        let rtxn = self.env.read_txn()?;
        self.index_mapper.index_exists(&rtxn, name)
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

    /// Return the task ids matched by the given query from the index scheduler's point of view.
    pub(crate) fn get_task_ids(&self, rtxn: &RoTxn, query: &Query) -> Result<RoaringBitmap> {
        let ProcessingTasks { batch: processing_batch, processing: processing_tasks, progress: _ } =
            self.processing_tasks.read().unwrap().clone();
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

        let mut tasks = self.all_task_ids(rtxn)?;

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
                    batch_tasks |= &*processing_tasks;
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
                        status_tasks |= &*processing_tasks;
                    }
                    status => status_tasks |= &self.get_status(rtxn, *status)?,
                };
            }
            if !status.contains(&Status::Processing) {
                tasks -= &*processing_tasks;
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
                if let Some(canceled_by_uid) = self.canceled_by.get(rtxn, cancel_task_uid)? {
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
                kind_tasks |= self.get_kind(rtxn, *kind)?;
            }
            tasks &= &kind_tasks;
        }

        if let Some(index) = index_uids {
            let mut index_tasks = RoaringBitmap::new();
            for index in index {
                index_tasks |= self.index_tasks(rtxn, index)?;
            }
            tasks &= &index_tasks;
        }

        // For the started_at filter, we need to treat the part of the tasks that are processing from the part of the
        // tasks that are not processing. The non-processing ones are filtered normally while the processing ones
        // are entirely removed unless the in-memory startedAt variable falls within the date filter.
        // Once we have filtered the two subsets, we put them back together and assign it back to `tasks`.
        tasks = {
            let (mut filtered_non_processing_tasks, mut filtered_processing_tasks) =
                (&tasks - &*processing_tasks, &tasks & &*processing_tasks);

            // special case for Processing tasks
            // A closure that clears the filtered_processing_tasks if their started_at date falls outside the given bounds
            let clear_filtered_processing_tasks =
                |start: Bound<OffsetDateTime>, end: Bound<OffsetDateTime>| {
                    let start = map_bound(start, |b| b.unix_timestamp_nanos());
                    let end = map_bound(end, |b| b.unix_timestamp_nanos());
                    let is_within_dates = RangeBounds::contains(
                        &(start, end),
                        &processing_batch
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
                self.started_at,
                *after_started_at,
                *before_started_at,
            )?;
            filtered_non_processing_tasks | filtered_processing_tasks
        };

        keep_ids_within_datetimes(
            rtxn,
            &mut tasks,
            self.enqueued_at,
            *after_enqueued_at,
            *before_enqueued_at,
        )?;

        keep_ids_within_datetimes(
            rtxn,
            &mut tasks,
            self.finished_at,
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

    /// Return the batch ids matched by the given query from the index scheduler's point of view.
    pub(crate) fn get_batch_ids(
        &self,
        rtxn: &RoTxn,
        processing: &ProcessingTasks,
        query: &Query,
    ) -> Result<RoaringBitmap> {
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

        let mut batches = self.all_batch_ids(rtxn)?;
        if let Some(batch_id) = processing.batch.as_ref().map(|batch| batch.uid) {
            batches.insert(batch_id);
        }

        if let Some(from) = from {
            let range = if reverse.unwrap_or_default() {
                u32::MIN..*from
            } else {
                from.saturating_add(1)..u32::MAX
            };
            batches.remove_range(range);
        }

        if let Some(batch_uids) = &batch_uids {
            let batches_uids = RoaringBitmap::from_iter(batch_uids);
            batches &= batches_uids;
        }

        if let Some(status) = &statuses {
            let mut status_batches = RoaringBitmap::new();
            for status in status {
                match status {
                    // special case for Processing batches
                    Status::Processing => {
                        if let Some(batch_id) = processing.batch.as_ref().map(|batch| batch.uid) {
                            status_batches.insert(batch_id);
                        }
                    }
                    // Enqueued tasks are not stored in batches
                    Status::Enqueued => (),
                    status => status_batches |= &self.get_batch_status(rtxn, *status)?,
                };
            }
            if !status.contains(&Status::Processing) {
                if let Some(ref batch) = processing.batch {
                    batches.remove(batch.uid);
                }
            }
            batches &= status_batches;
        }

        if let Some(task_uids) = &uids {
            let mut batches_by_task_uids = RoaringBitmap::new();
            for task_uid in task_uids {
                if let Some(task) = self.get_task(rtxn, *task_uid)? {
                    if let Some(batch_uid) = task.batch_uid {
                        batches_by_task_uids.insert(batch_uid);
                    }
                }
            }
            batches &= batches_by_task_uids;
        }

        // There is no database for this query, we must retrieve the task queried by the client and ensure it's valid
        if let Some(canceled_by) = &canceled_by {
            let mut all_canceled_batches = RoaringBitmap::new();
            for cancel_uid in canceled_by {
                if let Some(task) = self.get_task(rtxn, *cancel_uid)? {
                    if task.kind.as_kind() == Kind::TaskCancelation
                        && task.status == Status::Succeeded
                    {
                        if let Some(batch_uid) = task.batch_uid {
                            all_canceled_batches.insert(batch_uid);
                        }
                    }
                }
            }

            // if the canceled_by has been specified but no batch
            // matches then we prefer matching zero than all batches.
            if all_canceled_batches.is_empty() {
                return Ok(RoaringBitmap::new());
            } else {
                batches &= all_canceled_batches;
            }
        }

        if let Some(kind) = &types {
            let mut kind_batches = RoaringBitmap::new();
            for kind in kind {
                kind_batches |= self.get_batch_kind(rtxn, *kind)?;
                if let Some(uid) = processing
                    .batch
                    .as_ref()
                    .and_then(|batch| batch.kinds.contains(kind).then_some(batch.uid))
                {
                    kind_batches.insert(uid);
                }
            }
            batches &= &kind_batches;
        }

        if let Some(index) = &index_uids {
            let mut index_batches = RoaringBitmap::new();
            for index in index {
                index_batches |= self.index_batches(rtxn, index)?;
                if let Some(uid) = processing
                    .batch
                    .as_ref()
                    .and_then(|batch| batch.indexes.contains(index).then_some(batch.uid))
                {
                    index_batches.insert(uid);
                }
            }
            batches &= &index_batches;
        }

        // For the started_at filter, we need to treat the part of the batches that are processing from the part of the
        // batches that are not processing. The non-processing ones are filtered normally while the processing ones
        // are entirely removed unless the in-memory startedAt variable falls within the date filter.
        // Once we have filtered the two subsets, we put them back together and assign it back to `batches`.
        batches = {
            let (mut filtered_non_processing_batches, mut filtered_processing_batches) =
                (&batches - &*processing.processing, &batches & &*processing.processing);

            // special case for Processing batches
            // A closure that clears the filtered_processing_batches if their started_at date falls outside the given bounds
            let mut clear_filtered_processing_batches =
                |start: Bound<OffsetDateTime>, end: Bound<OffsetDateTime>| {
                    let start = map_bound(start, |b| b.unix_timestamp_nanos());
                    let end = map_bound(end, |b| b.unix_timestamp_nanos());
                    let is_within_dates = RangeBounds::contains(
                        &(start, end),
                        &processing
                            .batch
                            .as_ref()
                            .map_or_else(OffsetDateTime::now_utc, |batch| batch.started_at)
                            .unix_timestamp_nanos(),
                    );
                    if !is_within_dates {
                        filtered_processing_batches.clear();
                    }
                };
            match (after_started_at, before_started_at) {
                (None, None) => (),
                (None, Some(before)) => {
                    clear_filtered_processing_batches(Bound::Unbounded, Bound::Excluded(*before))
                }
                (Some(after), None) => {
                    clear_filtered_processing_batches(Bound::Excluded(*after), Bound::Unbounded)
                }
                (Some(after), Some(before)) => clear_filtered_processing_batches(
                    Bound::Excluded(*after),
                    Bound::Excluded(*before),
                ),
            };

            keep_ids_within_datetimes(
                rtxn,
                &mut filtered_non_processing_batches,
                self.batch_started_at,
                *after_started_at,
                *before_started_at,
            )?;
            filtered_non_processing_batches | filtered_processing_batches
        };

        keep_ids_within_datetimes(
            rtxn,
            &mut batches,
            self.batch_enqueued_at,
            *after_enqueued_at,
            *before_enqueued_at,
        )?;

        keep_ids_within_datetimes(
            rtxn,
            &mut batches,
            self.batch_finished_at,
            *after_finished_at,
            *before_finished_at,
        )?;

        if let Some(limit) = limit {
            batches = if query.reverse.unwrap_or_default() {
                batches.into_iter().take(*limit as usize).collect()
            } else {
                batches.into_iter().rev().take(*limit as usize).collect()
            };
        }

        Ok(batches)
    }

    /// The returned structure contains:
    /// 1. The name of the property being observed can be `statuses`, `types`, or `indexes`.
    /// 2. The name of the specific data related to the property can be `enqueued` for the `statuses`, `settingsUpdate` for the `types`, or the name of the index for the `indexes`, for example.
    /// 3. The number of times the properties appeared.
    pub fn get_stats(&self) -> Result<BTreeMap<String, BTreeMap<String, u64>>> {
        let rtxn = self.read_txn()?;

        let mut res = BTreeMap::new();

        let processing_tasks = { self.processing_tasks.read().unwrap().processing.len() };

        res.insert(
            "statuses".to_string(),
            enum_iterator::all::<Status>()
                .map(|s| {
                    let tasks = self.get_status(&rtxn, s)?.len();
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
                .map(|s| Ok((s.to_string(), self.get_kind(&rtxn, s)?.len())))
                .collect::<Result<BTreeMap<String, u64>>>()?,
        );
        res.insert(
            "indexes".to_string(),
            self.index_tasks
                .iter(&rtxn)?
                .map(|res| Ok(res.map(|(name, bitmap)| (name.to_string(), bitmap.len()))?))
                .collect::<Result<BTreeMap<String, u64>>>()?,
        );

        Ok(res)
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
        let index_tasks = self.index_tasks(&rtxn, index)?;
        let nbr_index_processing_tasks = processing_tasks.intersection_len(&index_tasks);
        Ok(nbr_index_processing_tasks > 0)
    }

    /// Return the task ids matching the query along with the total number of tasks
    /// by ignoring the from and limit parameters from the user's point of view.
    ///
    /// There are two differences between an internal query and a query executed by
    /// the user.
    ///
    /// 1. IndexSwap tasks are not publicly associated with any index, but they are associated
    /// with many indexes internally.
    /// 2. The user may not have the rights to access the tasks (internally) associated with all indexes.
    pub fn get_task_ids_from_authorized_indexes(
        &self,
        rtxn: &RoTxn,
        query: &Query,
        filters: &meilisearch_auth::AuthFilter,
    ) -> Result<(RoaringBitmap, u64)> {
        // compute all tasks matching the filter by ignoring the limits, to find the number of tasks matching
        // the filter.
        // As this causes us to compute the filter twice it is slightly inefficient, but doing it this way spares
        // us from modifying the underlying implementation, and the performance remains sufficient.
        // Should this change, we would modify `get_task_ids` to directly return the number of matching tasks.
        let total_tasks = self.get_task_ids(rtxn, &query.clone().without_limits())?;
        let mut tasks = self.get_task_ids(rtxn, query)?;

        // If the query contains a list of index uid or there is a finite list of authorized indexes,
        // then we must exclude all the kinds that aren't associated to one and only one index.
        if query.index_uids.is_some() || !filters.all_indexes_authorized() {
            for kind in enum_iterator::all::<Kind>().filter(|kind| !kind.related_to_one_index()) {
                tasks -= self.get_kind(rtxn, kind)?;
            }
        }

        // Any task that is internally associated with a non-authorized index
        // must be discarded.
        if !filters.all_indexes_authorized() {
            let all_indexes_iter = self.index_tasks.iter(rtxn)?;
            for result in all_indexes_iter {
                let (index, index_tasks) = result?;
                if !filters.is_index_authorized(index) {
                    tasks -= index_tasks;
                }
            }
        }

        Ok((tasks, total_tasks.len()))
    }

    /// Return the batch ids matching the query along with the total number of batches
    /// by ignoring the from and limit parameters from the user's point of view.
    ///
    /// There are two differences between an internal query and a query executed by
    /// the user.
    ///
    /// 1. IndexSwap tasks are not publicly associated with any index, but they are associated
    /// with many indexes internally.
    /// 2. The user may not have the rights to access the tasks (internally) associated with all indexes.
    fn get_batch_ids_from_authorized_indexes(
        &self,
        rtxn: &RoTxn,
        processing: &ProcessingTasks,
        query: &Query,
        filters: &meilisearch_auth::AuthFilter,
    ) -> Result<(RoaringBitmap, u64)> {
        // compute all batches matching the filter by ignoring the limits, to find the number of batches matching
        // the filter.
        // As this causes us to compute the filter twice it is slightly inefficient, but doing it this way spares
        // us from modifying the underlying implementation, and the performance remains sufficient.
        // Should this change, we would modify `get_batch_ids` to directly return the number of matching batches.
        let total_batches =
            self.get_batch_ids(rtxn, processing, &query.clone().without_limits())?;
        let mut batches = self.get_batch_ids(rtxn, processing, query)?;

        // If the query contains a list of index uid or there is a finite list of authorized indexes,
        // then we must exclude all the batches that only contains tasks associated to multiple indexes.
        // This works because we don't autobatch tasks associated to multiple indexes with tasks associated
        // to a single index. e.g: IndexSwap cannot be batched with IndexCreation.
        if query.index_uids.is_some() || !filters.all_indexes_authorized() {
            for kind in enum_iterator::all::<Kind>().filter(|kind| !kind.related_to_one_index()) {
                batches -= self.get_kind(rtxn, kind)?;
                if let Some(batch) = processing.batch.as_ref() {
                    if batch.kinds.contains(&kind) {
                        batches.remove(batch.uid);
                    }
                }
            }
        }

        // Any batch that is internally associated with at least one authorized index
        // must be returned.
        if !filters.all_indexes_authorized() {
            let mut valid_indexes = RoaringBitmap::new();
            let mut forbidden_indexes = RoaringBitmap::new();

            let all_indexes_iter = self.batch_index_tasks.iter(rtxn)?;
            for result in all_indexes_iter {
                let (index, index_tasks) = result?;
                if filters.is_index_authorized(index) {
                    valid_indexes |= index_tasks;
                } else {
                    forbidden_indexes |= index_tasks;
                }
            }
            if let Some(batch) = processing.batch.as_ref() {
                for index in &batch.indexes {
                    if filters.is_index_authorized(index) {
                        valid_indexes.insert(batch.uid);
                    } else {
                        forbidden_indexes.insert(batch.uid);
                    }
                }
            }

            // If a batch had ONE valid task then it should be returned
            let invalid_batches = forbidden_indexes - valid_indexes;

            batches -= invalid_batches;
        }

        Ok((batches, total_batches.len()))
    }

    /// Return the tasks matching the query from the user's point of view along
    /// with the total number of tasks matching the query, ignoring from and limit.
    ///
    /// There are two differences between an internal query and a query executed by
    /// the user.
    ///
    /// 1. IndexSwap tasks are not publicly associated with any index, but they are associated
    /// with many indexes internally.
    /// 2. The user may not have the rights to access the tasks (internally) associated with all indexes.
    pub fn get_tasks_from_authorized_indexes(
        &self,
        query: Query,
        filters: &meilisearch_auth::AuthFilter,
    ) -> Result<(Vec<Task>, u64)> {
        let rtxn = self.env.read_txn()?;

        let (tasks, total) = self.get_task_ids_from_authorized_indexes(&rtxn, &query, filters)?;
        let tasks = if query.reverse.unwrap_or_default() {
            Box::new(tasks.into_iter()) as Box<dyn Iterator<Item = u32>>
        } else {
            Box::new(tasks.into_iter().rev()) as Box<dyn Iterator<Item = u32>>
        };
        let tasks =
            self.get_existing_tasks(&rtxn, tasks.take(query.limit.unwrap_or(u32::MAX) as usize))?;

        let ProcessingTasks { batch, processing, progress } =
            self.processing_tasks.read().map_err(|_| Error::CorruptedTaskQueue)?.clone();

        // ignored for now, might be added to batch details later
        let _ = progress;

        let ret = tasks.into_iter();
        if processing.is_empty() || batch.is_none() {
            Ok((ret.collect(), total))
        } else {
            // Safe because we ensured there was a batch in the previous branch
            let batch = batch.unwrap();
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

    /// Return the batches matching the query from the user's point of view along
    /// with the total number of batches matching the query, ignoring from and limit.
    ///
    /// There are two differences between an internal query and a query executed by
    /// the user.
    ///
    /// 1. IndexSwap tasks are not publicly associated with any index, but they are associated
    /// with many indexes internally.
    /// 2. The user may not have the rights to access the tasks (internally) associated with all indexes.
    pub fn get_batches_from_authorized_indexes(
        &self,
        query: Query,
        filters: &meilisearch_auth::AuthFilter,
    ) -> Result<(Vec<Batch>, u64)> {
        let rtxn = self.env.read_txn()?;
        let processing = self.processing_tasks.read().unwrap().clone();

        let (batches, total) =
            self.get_batch_ids_from_authorized_indexes(&rtxn, &processing, &query, filters)?;
        let batches = if query.reverse.unwrap_or_default() {
            Box::new(batches.into_iter()) as Box<dyn Iterator<Item = u32>>
        } else {
            Box::new(batches.into_iter().rev()) as Box<dyn Iterator<Item = u32>>
        };

        let batches = self.get_existing_batches(
            &rtxn,
            &processing,
            batches.take(query.limit.unwrap_or(u32::MAX) as usize),
        )?;

        Ok((batches, total))
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
        let mut wtxn = self.env.write_txn()?;

        // if the task doesn't delete anything and 50% of the task queue is full, we must refuse to enqueue the incomming task
        if !matches!(&kind, KindWithContent::TaskDeletion { tasks, .. } if !tasks.is_empty())
            && (self.env.non_free_pages_size()? * 100) / self.env.info().map_size as u64 > 40
        {
            return Err(Error::NoSpaceLeftInTaskQueue);
        }

        let next_task_id = self.next_task_id(&wtxn)?;

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

        self.all_tasks.put_with_flags(&mut wtxn, PutFlags::APPEND, &task.uid, &task)?;

        for index in task.indexes() {
            self.update_index(&mut wtxn, index, |bitmap| {
                bitmap.insert(task.uid);
            })?;
        }

        self.update_status(&mut wtxn, Status::Enqueued, |bitmap| {
            bitmap.insert(task.uid);
        })?;

        self.update_kind(&mut wtxn, task.kind.as_kind(), |bitmap| {
            bitmap.insert(task.uid);
        })?;

        utils::insert_task_datetime(&mut wtxn, self.enqueued_at, task.enqueued_at, task.uid)?;

        if let Err(e) = wtxn.commit() {
            self.delete_persisted_task_data(&task)?;
            return Err(e.into());
        }

        // If the registered task is a task cancelation
        // we inform the processing tasks to stop (if necessary).
        if let KindWithContent::TaskCancelation { tasks, .. } = kind {
            let tasks_to_cancel = RoaringBitmap::from_iter(tasks);
            if self.processing_tasks.read().unwrap().must_cancel_processing_tasks(&tasks_to_cancel)
            {
                self.must_stop_processing.must_stop();
            }
        }

        // notify the scheduler loop to execute a new tick
        self.wake_up.signal();

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

    /// Delete a file from the index scheduler.
    ///
    /// Counterpart to the [`create_update_file`](IndexScheduler::create_update_file) method.
    pub fn delete_update_file(&self, uuid: Uuid) -> Result<()> {
        Ok(self.file_store.delete(uuid)?)
    }

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
    fn tick(&self) -> Result<TickOutcome> {
        #[cfg(test)]
        {
            *self.run_loop_iteration.write().unwrap() += 1;
            self.breakpoint(Breakpoint::Start);
        }

        if self.cleanup_enabled {
            self.cleanup_task_queue()?;
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
        self.must_stop_processing.reset();
        let progress = self
            .processing_tasks
            .write()
            .unwrap()
            // We can clone the processing batch here because we don't want its modification to affect the view of the processing batches
            .start_processing(processing_batch.clone(), ids.clone());

        #[cfg(test)]
        self.breakpoint(Breakpoint::BatchCreated);

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
        self.maybe_fail(tests::FailureLocation::AcquiringWtxn)?;

        progress.update_progress(BatchProgress::WritingTasksToDisk);
        processing_batch.finished();
        let mut wtxn = self.env.write_txn().map_err(Error::HeedTransaction)?;
        let mut canceled = RoaringBitmap::new();

        match res {
            Ok(tasks) => {
                #[cfg(test)]
                self.breakpoint(Breakpoint::ProcessBatchSucceeded);

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
                        tests::FailureLocation::UpdatingTaskAfterProcessBatchSuccess {
                            task_uid: i as u32,
                        },
                    )?;

                    match task.error {
                        Some(_) => failure += 1,
                        None => success += 1,
                    }

                    self.update_task(&mut wtxn, &task)
                        .map_err(|e| Error::TaskDatabaseUpdate(Box::new(e)))?;
                }
                if let Some(canceled_by) = canceled_by {
                    self.canceled_by.put(&mut wtxn, &canceled_by, &canceled)?;
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
                self.breakpoint(Breakpoint::AbortedIndexation);
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
                self.breakpoint(Breakpoint::ProcessBatchFailed);
                let (task_progress, task_progress_obj) = AtomicTaskStep::new(ids.len() as u32);
                progress.update_progress(task_progress_obj);

                let error: ResponseError = err.into();
                for id in ids.iter() {
                    task_progress.fetch_add(1, Ordering::Relaxed);
                    let mut task = self
                        .get_task(&wtxn, id)
                        .map_err(|e| Error::TaskDatabaseUpdate(Box::new(e)))?
                        .ok_or(Error::CorruptedTaskQueue)?;
                    task.status = Status::Failed;
                    task.error = Some(error.clone());
                    task.details = task.details.map(|d| d.to_failed());
                    processing_batch.update(&mut task);

                    #[cfg(test)]
                    self.maybe_fail(tests::FailureLocation::UpdatingTaskAfterProcessBatchFailure)?;

                    tracing::error!("Batch failed {}", error);

                    self.update_task(&mut wtxn, &task)
                        .map_err(|e| Error::TaskDatabaseUpdate(Box::new(e)))?;
                }
            }
        }

        // We must re-add the canceled task so they're part of the same batch.
        ids |= canceled;
        self.write_batch(&mut wtxn, processing_batch, &ids)?;

        #[cfg(test)]
        self.maybe_fail(tests::FailureLocation::CommittingWtxn)?;

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
                    .get_task(&rtxn, id)
                    .map_err(|e| Error::TaskDatabaseUpdate(Box::new(e)))?
                    .ok_or(Error::CorruptedTaskQueue)?;
                if let Err(e) = self.delete_persisted_task_data(&task) {
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
        self.breakpoint(Breakpoint::AfterProcessing);

        Ok(TickOutcome::TickAgain(processed_tasks))
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

    /// Register a task to cleanup the task queue if needed
    fn cleanup_task_queue(&self) -> Result<()> {
        let rtxn = self.env.read_txn().map_err(Error::HeedTransaction)?;

        let nb_tasks = self.all_task_ids(&rtxn)?.len();
        // if we have less than 1M tasks everything is fine
        if nb_tasks < self.max_number_of_tasks as u64 {
            return Ok(());
        }

        let finished = self.status.get(&rtxn, &Status::Succeeded)?.unwrap_or_default()
            | self.status.get(&rtxn, &Status::Failed)?.unwrap_or_default()
            | self.status.get(&rtxn, &Status::Canceled)?.unwrap_or_default();

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
            self.get_task(&rtxn, newest_task_id)?.ok_or(Error::CorruptedTaskQueue)?;
        drop(rtxn);

        // increase time by one nanosecond so that the enqueuedAt of the last task to delete is also lower than that date.
        let delete_before = last_task_to_delete.enqueued_at + Duration::from_nanos(1);

        self.register(
            KindWithContent::TaskDeletion {
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

    pub(crate) fn delete_persisted_task_data(&self, task: &Task) -> Result<()> {
        match task.content_uuid() {
            Some(content_file) => self.delete_update_file(content_file),
            None => Ok(()),
        }
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

    /// Blocks the thread until the test handle asks to progress to/through this breakpoint.
    ///
    /// Two messages are sent through the channel for each breakpoint.
    /// The first message is `(b, false)` and the second message is `(b, true)`.
    ///
    /// Since the channel has a capacity of zero, the `send` and `recv` calls wait for each other.
    /// So when the index scheduler calls `test_breakpoint_sdr.send(b, false)`, it blocks
    /// the thread until the test catches up by calling `test_breakpoint_rcv.recv()` enough.
    /// From the test side, we call `recv()` repeatedly until we find the message `(breakpoint, false)`.
    /// As soon as we find it, the index scheduler is unblocked but then wait again on the call to
    /// `test_breakpoint_sdr.send(b, true)`. This message will only be able to send once the
    /// test asks to progress to the next `(b2, false)`.
    #[cfg(test)]
    fn breakpoint(&self, b: Breakpoint) {
        // We send two messages. The first one will sync with the call
        // to `handle.wait_until(b)`. The second one will block until the
        // the next call to `handle.wait_until(..)`.
        self.test_breakpoint_sdr.send((b, false)).unwrap();
        // This one will only be able to be sent if the test handle stays alive.
        // If it fails, then it means that we have exited the test.
        // By crashing with `unwrap`, we kill the run loop.
        self.test_breakpoint_sdr.send((b, true)).unwrap();
    }
}

pub struct Dump<'a> {
    index_scheduler: &'a IndexScheduler,
    wtxn: RwTxn<'a>,

    indexes: HashMap<String, RoaringBitmap>,
    statuses: HashMap<Status, RoaringBitmap>,
    kinds: HashMap<Kind, RoaringBitmap>,
}

impl<'a> Dump<'a> {
    pub(crate) fn new(index_scheduler: &'a mut IndexScheduler) -> Result<Self> {
        // While loading a dump no one should be able to access the scheduler thus I can block everything.
        let wtxn = index_scheduler.env.write_txn()?;

        Ok(Dump {
            index_scheduler,
            wtxn,
            indexes: HashMap::new(),
            statuses: HashMap::new(),
            kinds: HashMap::new(),
        })
    }

    /// Register a new task coming from a dump in the scheduler.
    /// By taking a mutable ref we're pretty sure no one will ever import a dump while actix is running.
    pub fn register_dumped_task(
        &mut self,
        task: TaskDump,
        content_file: Option<Box<UpdateFile>>,
    ) -> Result<Task> {
        let content_uuid = match content_file {
            Some(content_file) if task.status == Status::Enqueued => {
                let (uuid, mut file) = self.index_scheduler.create_update_file(false)?;
                let mut builder = DocumentsBatchBuilder::new(&mut file);
                for doc in content_file {
                    builder.append_json_object(&doc?)?;
                }
                builder.into_inner()?;
                file.persist()?;

                Some(uuid)
            }
            // If the task isn't `Enqueued` then just generate a recognisable `Uuid`
            // in case we try to open it later.
            _ if task.status != Status::Enqueued => Some(Uuid::nil()),
            _ => None,
        };

        let task = Task {
            uid: task.uid,
            batch_uid: task.batch_uid,
            enqueued_at: task.enqueued_at,
            started_at: task.started_at,
            finished_at: task.finished_at,
            error: task.error,
            canceled_by: task.canceled_by,
            details: task.details,
            status: task.status,
            kind: match task.kind {
                KindDump::DocumentImport {
                    primary_key,
                    method,
                    documents_count,
                    allow_index_creation,
                } => KindWithContent::DocumentAdditionOrUpdate {
                    index_uid: task.index_uid.ok_or(Error::CorruptedDump)?,
                    primary_key,
                    method,
                    content_file: content_uuid.ok_or(Error::CorruptedDump)?,
                    documents_count,
                    allow_index_creation,
                },
                KindDump::DocumentDeletion { documents_ids } => KindWithContent::DocumentDeletion {
                    documents_ids,
                    index_uid: task.index_uid.ok_or(Error::CorruptedDump)?,
                },
                KindDump::DocumentDeletionByFilter { filter } => {
                    KindWithContent::DocumentDeletionByFilter {
                        filter_expr: filter,
                        index_uid: task.index_uid.ok_or(Error::CorruptedDump)?,
                    }
                }
                KindDump::DocumentEdition { filter, context, function } => {
                    KindWithContent::DocumentEdition {
                        index_uid: task.index_uid.ok_or(Error::CorruptedDump)?,
                        filter_expr: filter,
                        context,
                        function,
                    }
                }
                KindDump::DocumentClear => KindWithContent::DocumentClear {
                    index_uid: task.index_uid.ok_or(Error::CorruptedDump)?,
                },
                KindDump::Settings { settings, is_deletion, allow_index_creation } => {
                    KindWithContent::SettingsUpdate {
                        index_uid: task.index_uid.ok_or(Error::CorruptedDump)?,
                        new_settings: settings,
                        is_deletion,
                        allow_index_creation,
                    }
                }
                KindDump::IndexDeletion => KindWithContent::IndexDeletion {
                    index_uid: task.index_uid.ok_or(Error::CorruptedDump)?,
                },
                KindDump::IndexCreation { primary_key } => KindWithContent::IndexCreation {
                    index_uid: task.index_uid.ok_or(Error::CorruptedDump)?,
                    primary_key,
                },
                KindDump::IndexUpdate { primary_key } => KindWithContent::IndexUpdate {
                    index_uid: task.index_uid.ok_or(Error::CorruptedDump)?,
                    primary_key,
                },
                KindDump::IndexSwap { swaps } => KindWithContent::IndexSwap { swaps },
                KindDump::TaskCancelation { query, tasks } => {
                    KindWithContent::TaskCancelation { query, tasks }
                }
                KindDump::TasksDeletion { query, tasks } => {
                    KindWithContent::TaskDeletion { query, tasks }
                }
                KindDump::DumpCreation { keys, instance_uid } => {
                    KindWithContent::DumpCreation { keys, instance_uid }
                }
                KindDump::SnapshotCreation => KindWithContent::SnapshotCreation,
            },
        };

        self.index_scheduler.all_tasks.put(&mut self.wtxn, &task.uid, &task)?;

        for index in task.indexes() {
            match self.indexes.get_mut(index) {
                Some(bitmap) => {
                    bitmap.insert(task.uid);
                }
                None => {
                    let mut bitmap = RoaringBitmap::new();
                    bitmap.insert(task.uid);
                    self.indexes.insert(index.to_string(), bitmap);
                }
            };
        }

        utils::insert_task_datetime(
            &mut self.wtxn,
            self.index_scheduler.enqueued_at,
            task.enqueued_at,
            task.uid,
        )?;

        // we can't override the started_at & finished_at, so we must only set it if the tasks is finished and won't change
        if matches!(task.status, Status::Succeeded | Status::Failed | Status::Canceled) {
            if let Some(started_at) = task.started_at {
                utils::insert_task_datetime(
                    &mut self.wtxn,
                    self.index_scheduler.started_at,
                    started_at,
                    task.uid,
                )?;
            }
            if let Some(finished_at) = task.finished_at {
                utils::insert_task_datetime(
                    &mut self.wtxn,
                    self.index_scheduler.finished_at,
                    finished_at,
                    task.uid,
                )?;
            }
        }

        self.statuses.entry(task.status).or_default().insert(task.uid);
        self.kinds.entry(task.kind.as_kind()).or_default().insert(task.uid);

        Ok(task)
    }

    /// Commit all the changes and exit the importing dump state
    pub fn finish(mut self) -> Result<()> {
        for (index, bitmap) in self.indexes {
            self.index_scheduler.index_tasks.put(&mut self.wtxn, &index, &bitmap)?;
        }
        for (status, bitmap) in self.statuses {
            self.index_scheduler.put_status(&mut self.wtxn, status, &bitmap)?;
        }
        for (kind, bitmap) in self.kinds {
            self.index_scheduler.put_kind(&mut self.wtxn, kind, &bitmap)?;
        }

        self.wtxn.commit()?;
        self.index_scheduler.wake_up.signal();

        Ok(())
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

#[cfg(test)]
mod tests {
    use std::io::{BufWriter, Write};
    use std::time::Instant;

    use big_s::S;
    use crossbeam_channel::RecvTimeoutError;
    use file_store::File;
    use insta::assert_json_snapshot;
    use maplit::btreeset;
    use meili_snap::{json_string, snapshot};
    use meilisearch_auth::AuthFilter;
    use meilisearch_types::document_formats::DocumentFormatError;
    use meilisearch_types::error::ErrorCode;
    use meilisearch_types::index_uid_pattern::IndexUidPattern;
    use meilisearch_types::milli::obkv_to_json;
    use meilisearch_types::milli::update::IndexDocumentsMethod::{
        ReplaceDocuments, UpdateDocuments,
    };
    use meilisearch_types::milli::update::Setting;
    use meilisearch_types::milli::vector::settings::EmbeddingSettings;
    use meilisearch_types::settings::Unchecked;
    use meilisearch_types::tasks::IndexSwap;
    use meilisearch_types::VERSION_FILE_NAME;
    use tempfile::{NamedTempFile, TempDir};
    use time::Duration;
    use uuid::Uuid;
    use Breakpoint::*;

    use super::*;
    use crate::insta_snapshot::{snapshot_bitmap, snapshot_index_scheduler};

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum FailureLocation {
        InsideCreateBatch,
        InsideProcessBatch,
        PanicInsideProcessBatch,
        AcquiringWtxn,
        UpdatingTaskAfterProcessBatchSuccess { task_uid: u32 },
        UpdatingTaskAfterProcessBatchFailure,
        CommittingWtxn,
    }

    impl IndexScheduler {
        pub fn test(
            autobatching_enabled: bool,
            planned_failures: Vec<(usize, FailureLocation)>,
        ) -> (Self, IndexSchedulerHandle) {
            Self::test_with_custom_config(planned_failures, |config| {
                config.autobatching_enabled = autobatching_enabled;
            })
        }

        pub fn test_with_custom_config(
            planned_failures: Vec<(usize, FailureLocation)>,
            configuration: impl Fn(&mut IndexSchedulerOptions),
        ) -> (Self, IndexSchedulerHandle) {
            let tempdir = TempDir::new().unwrap();
            let (sender, receiver) = crossbeam_channel::bounded(0);

            let indexer_config = IndexerConfig { skip_index_budget: true, ..Default::default() };

            let mut options = IndexSchedulerOptions {
                version_file_path: tempdir.path().join(VERSION_FILE_NAME),
                auth_path: tempdir.path().join("auth"),
                tasks_path: tempdir.path().join("db_path"),
                update_file_path: tempdir.path().join("file_store"),
                indexes_path: tempdir.path().join("indexes"),
                snapshots_path: tempdir.path().join("snapshots"),
                dumps_path: tempdir.path().join("dumps"),
                webhook_url: None,
                webhook_authorization_header: None,
                task_db_size: 1000 * 1000 * 10, // 10 MB, we don't use MiB on purpose.
                index_base_map_size: 1000 * 1000, // 1 MB, we don't use MiB on purpose.
                enable_mdb_writemap: false,
                index_growth_amount: 1000 * 1000 * 1000 * 1000, // 1 TB
                index_count: 5,
                indexer_config,
                autobatching_enabled: true,
                cleanup_enabled: true,
                max_number_of_tasks: 1_000_000,
                max_number_of_batched_tasks: usize::MAX,
                instance_features: Default::default(),
            };
            configuration(&mut options);

            let index_scheduler = Self::new(options, sender, planned_failures).unwrap();

            // To be 100% consistent between all test we're going to start the scheduler right now
            // and ensure it's in the expected starting state.
            let breakpoint = match receiver.recv_timeout(std::time::Duration::from_secs(10)) {
                Ok(b) => b,
                Err(RecvTimeoutError::Timeout) => {
                    panic!("The scheduler seems to be waiting for a new task while your test is waiting for a breakpoint.")
                }
                Err(RecvTimeoutError::Disconnected) => panic!("The scheduler crashed."),
            };
            assert_eq!(breakpoint, (Init, false));
            let index_scheduler_handle = IndexSchedulerHandle {
                _tempdir: tempdir,
                index_scheduler: index_scheduler.private_clone(),
                test_breakpoint_rcv: receiver,
                last_breakpoint: breakpoint.0,
            };

            (index_scheduler, index_scheduler_handle)
        }

        /// Return a [`PlannedFailure`](Error::PlannedFailure) error if a failure is planned
        /// for the given location and current run loop iteration.
        pub fn maybe_fail(&self, location: FailureLocation) -> Result<()> {
            if self.planned_failures.contains(&(*self.run_loop_iteration.read().unwrap(), location))
            {
                match location {
                    FailureLocation::PanicInsideProcessBatch => {
                        panic!("simulated panic")
                    }
                    _ => Err(Error::PlannedFailure),
                }
            } else {
                Ok(())
            }
        }
    }

    /// Return a `KindWithContent::IndexCreation` task
    fn index_creation_task(index: &'static str, primary_key: &'static str) -> KindWithContent {
        KindWithContent::IndexCreation { index_uid: S(index), primary_key: Some(S(primary_key)) }
    }
    /// Create a `KindWithContent::DocumentImport` task that imports documents.
    ///
    /// - `index_uid` is given as parameter
    /// - `primary_key` is given as parameter
    /// - `method` is set to `ReplaceDocuments`
    /// - `content_file` is given as parameter
    /// - `documents_count` is given as parameter
    /// - `allow_index_creation` is set to `true`
    fn replace_document_import_task(
        index: &'static str,
        primary_key: Option<&'static str>,
        content_file_uuid: u128,
        documents_count: u64,
    ) -> KindWithContent {
        KindWithContent::DocumentAdditionOrUpdate {
            index_uid: S(index),
            primary_key: primary_key.map(ToOwned::to_owned),
            method: ReplaceDocuments,
            content_file: Uuid::from_u128(content_file_uuid),
            documents_count,
            allow_index_creation: true,
        }
    }

    /// Adapting to the new json reading interface
    pub fn read_json(
        bytes: &[u8],
        write: impl Write,
    ) -> std::result::Result<u64, DocumentFormatError> {
        let temp_file = NamedTempFile::new().unwrap();
        let mut buffer = BufWriter::new(temp_file.reopen().unwrap());
        buffer.write_all(bytes).unwrap();
        buffer.flush().unwrap();
        meilisearch_types::document_formats::read_json(temp_file.as_file(), write)
    }

    /// Create an update file with the given file uuid.
    ///
    /// The update file contains just one simple document whose id is given by `document_id`.
    ///
    /// The uuid of the file and its documents count is returned.
    fn sample_documents(
        index_scheduler: &IndexScheduler,
        file_uuid: u128,
        document_id: usize,
    ) -> (File, u64) {
        let content = format!(
            r#"
        {{
            "id" : "{document_id}"
        }}"#
        );

        let (_uuid, mut file) = index_scheduler.create_update_file_with_uuid(file_uuid).unwrap();
        let documents_count = read_json(content.as_bytes(), &mut file).unwrap();
        (file, documents_count)
    }

    pub struct IndexSchedulerHandle {
        _tempdir: TempDir,
        index_scheduler: IndexScheduler,
        test_breakpoint_rcv: crossbeam_channel::Receiver<(Breakpoint, bool)>,
        last_breakpoint: Breakpoint,
    }

    impl IndexSchedulerHandle {
        /// Advance the scheduler to the next tick.
        /// Panic
        /// * If the scheduler is waiting for a task to be registered.
        /// * If the breakpoint queue is in a bad state.
        #[track_caller]
        fn advance(&mut self) -> Breakpoint {
            let (breakpoint_1, b) = match self
                .test_breakpoint_rcv
                .recv_timeout(std::time::Duration::from_secs(50))
            {
                Ok(b) => b,
                Err(RecvTimeoutError::Timeout) => {
                    let state = snapshot_index_scheduler(&self.index_scheduler);
                    panic!("The scheduler seems to be waiting for a new task while your test is waiting for a breakpoint.\n{state}")
                }
                Err(RecvTimeoutError::Disconnected) => {
                    let state = snapshot_index_scheduler(&self.index_scheduler);
                    panic!("The scheduler crashed.\n{state}")
                }
            };
            // if we've already encountered a breakpoint we're supposed to be stuck on the false
            // and we expect the same variant with the true to come now.
            assert_eq!(
                (breakpoint_1, b),
                (self.last_breakpoint, true),
                "Internal error in the test suite. In the previous iteration I got `({:?}, false)` and now I got `({:?}, {:?})`.",
                self.last_breakpoint,
                breakpoint_1,
                b,
            );

            let (breakpoint_2, b) = match self
                .test_breakpoint_rcv
                .recv_timeout(std::time::Duration::from_secs(50))
            {
                Ok(b) => b,
                Err(RecvTimeoutError::Timeout) => {
                    let state = snapshot_index_scheduler(&self.index_scheduler);
                    panic!("The scheduler seems to be waiting for a new task while your test is waiting for a breakpoint.\n{state}")
                }
                Err(RecvTimeoutError::Disconnected) => {
                    let state = snapshot_index_scheduler(&self.index_scheduler);
                    panic!("The scheduler crashed.\n{state}")
                }
            };
            assert!(!b, "Found the breakpoint handle in a bad state. Check your test suite");

            self.last_breakpoint = breakpoint_2;

            breakpoint_2
        }

        /// Advance the scheduler until all the provided breakpoints are reached in order.
        #[track_caller]
        fn advance_till(&mut self, breakpoints: impl IntoIterator<Item = Breakpoint>) {
            for breakpoint in breakpoints {
                let b = self.advance();
                assert_eq!(
                    b,
                    breakpoint,
                    "Was expecting the breakpoint `{:?}` but instead got `{:?}`.\n{}",
                    breakpoint,
                    b,
                    snapshot_index_scheduler(&self.index_scheduler)
                );
            }
        }

        /// Wait for `n` successful batches.
        #[track_caller]
        fn advance_n_successful_batches(&mut self, n: usize) {
            for _ in 0..n {
                self.advance_one_successful_batch();
            }
        }

        /// Wait for `n` failed batches.
        #[track_caller]
        fn advance_n_failed_batches(&mut self, n: usize) {
            for _ in 0..n {
                self.advance_one_failed_batch();
            }
        }

        // Wait for one successful batch.
        #[track_caller]
        fn advance_one_successful_batch(&mut self) {
            self.advance_till([Start, BatchCreated]);
            loop {
                match self.advance() {
                    // the process_batch function can call itself recursively, thus we need to
                    // accept as may InsideProcessBatch as possible before moving to the next state.
                    InsideProcessBatch => (),
                    // the batch went successfully, we can stop the loop and go on with the next states.
                    ProcessBatchSucceeded => break,
                    AbortedIndexation => panic!("The batch was aborted.\n{}", snapshot_index_scheduler(&self.index_scheduler)),
                    ProcessBatchFailed => {
                        while self.advance() != Start {}
                        panic!("The batch failed.\n{}", snapshot_index_scheduler(&self.index_scheduler))
                    },
                    breakpoint => panic!("Encountered an impossible breakpoint `{:?}`, this is probably an issue with the test suite.", breakpoint),
                }
            }

            self.advance_till([AfterProcessing]);
        }

        // Wait for one failed batch.
        #[track_caller]
        fn advance_one_failed_batch(&mut self) {
            self.advance_till([Start, BatchCreated]);
            loop {
                match self.advance() {
                    // the process_batch function can call itself recursively, thus we need to
                    // accept as may InsideProcessBatch as possible before moving to the next state.
                    InsideProcessBatch => (),
                    // the batch went failed, we can stop the loop and go on with the next states.
                    ProcessBatchFailed => break,
                    ProcessBatchSucceeded => panic!("The batch succeeded. (and it wasn't supposed to sorry)\n{}", snapshot_index_scheduler(&self.index_scheduler)),
                    AbortedIndexation => panic!("The batch was aborted.\n{}", snapshot_index_scheduler(&self.index_scheduler)),
                    breakpoint => panic!("Encountered an impossible breakpoint `{:?}`, this is probably an issue with the test suite.", breakpoint),
                }
            }
            self.advance_till([AfterProcessing]);
        }
    }

    #[test]
    fn register() {
        // In this test, the handle doesn't make any progress, we only check that the tasks are registered
        let (index_scheduler, mut _handle) = IndexScheduler::test(true, vec![]);

        let kinds = [
            index_creation_task("catto", "mouse"),
            replace_document_import_task("catto", None, 0, 12),
            replace_document_import_task("catto", None, 1, 50),
            replace_document_import_task("doggo", Some("bone"), 2, 5000),
        ];
        let (_, file) = index_scheduler.create_update_file_with_uuid(0).unwrap();
        file.persist().unwrap();
        let (_, file) = index_scheduler.create_update_file_with_uuid(1).unwrap();
        file.persist().unwrap();
        let (_, file) = index_scheduler.create_update_file_with_uuid(2).unwrap();
        file.persist().unwrap();

        for (idx, kind) in kinds.into_iter().enumerate() {
            let k = kind.as_kind();
            let task = index_scheduler.register(kind, None, false).unwrap();
            index_scheduler.assert_internally_consistent();

            assert_eq!(task.uid, idx as u32);
            assert_eq!(task.status, Status::Enqueued);
            assert_eq!(task.kind.as_kind(), k);
        }

        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "everything_is_successfully_registered");
    }

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
    fn document_addition() {
        let (index_scheduler, mut handle) = IndexScheduler::test(true, vec![]);

        let content = r#"
        {
            "id": 1,
            "doggo": "bob"
        }"#;

        let (uuid, mut file) = index_scheduler.create_update_file_with_uuid(0).unwrap();
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
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "after_register");

        handle.advance_till([Start, BatchCreated]);
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "after_the_batch_creation");

        handle.advance_till([InsideProcessBatch, ProcessBatchSucceeded, AfterProcessing]);
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "once_everything_is_processed");
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

        let (uuid, mut file) = index_scheduler.create_update_file_with_uuid(0).unwrap();
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
    fn document_addition_and_document_deletion() {
        let (index_scheduler, mut handle) = IndexScheduler::test(true, vec![]);

        let content = r#"[
            { "id": 1, "doggo": "jean bob" },
            { "id": 2, "catto": "jorts" },
            { "id": 3, "doggo": "bork" }
        ]"#;

        let (uuid, mut file) = index_scheduler.create_update_file_with_uuid(0).unwrap();
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
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "registered_the_first_task");
        index_scheduler
            .register(
                KindWithContent::DocumentDeletion {
                    index_uid: S("doggos"),
                    documents_ids: vec![S("1"), S("2")],
                },
                None,
                false,
            )
            .unwrap();
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "registered_the_second_task");

        handle.advance_one_successful_batch(); // The addition AND deletion should've been batched together
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "after_processing_the_batch");

        let index = index_scheduler.index("doggos").unwrap();
        let rtxn = index.read_txn().unwrap();
        let mut buffer = Vec::new();
        let dictionary = index.document_decompression_dictionary(&rtxn).unwrap();
        let field_ids_map = index.fields_ids_map(&rtxn).unwrap();
        let field_ids = field_ids_map.ids().collect::<Vec<_>>();
        let documents = index
            .all_compressed_documents(&rtxn)
            .unwrap()
            .map(|ret| {
                let (_id, compressed_doc) = ret.unwrap();
                let doc = compressed_doc
                    .decompress_with_optional_dictionary(&mut buffer, dictionary.as_ref())
                    .unwrap();
                obkv_to_json(&field_ids, &field_ids_map, doc).unwrap()
            })
            .collect::<Vec<_>>();
        snapshot!(serde_json::to_string_pretty(&documents).unwrap(), name: "documents");
    }

    #[test]
    fn document_deletion_and_document_addition() {
        let (index_scheduler, mut handle) = IndexScheduler::test(true, vec![]);
        index_scheduler
            .register(
                KindWithContent::DocumentDeletion {
                    index_uid: S("doggos"),
                    documents_ids: vec![S("1"), S("2")],
                },
                None,
                false,
            )
            .unwrap();
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "registered_the_first_task");

        let content = r#"[
            { "id": 1, "doggo": "jean bob" },
            { "id": 2, "catto": "jorts" },
            { "id": 3, "doggo": "bork" }
        ]"#;

        let (uuid, mut file) = index_scheduler.create_update_file_with_uuid(0).unwrap();
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

        // The deletion should have failed because it can't create an index
        handle.advance_one_failed_batch();
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "after_failing_the_deletion");

        // The addition should works
        handle.advance_one_successful_batch();
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "after_last_successful_addition");

        let index = index_scheduler.index("doggos").unwrap();
        let rtxn = index.read_txn().unwrap();
        let mut buffer = Vec::new();
        let dictionary = index.document_decompression_dictionary(&rtxn).unwrap();
        let field_ids_map = index.fields_ids_map(&rtxn).unwrap();
        let field_ids = field_ids_map.ids().collect::<Vec<_>>();
        let documents = index
            .all_compressed_documents(&rtxn)
            .unwrap()
            .map(|ret| {
                let (_id, compressed_doc) = ret.unwrap();
                let doc = compressed_doc
                    .decompress_with_optional_dictionary(&mut buffer, dictionary.as_ref())
                    .unwrap();
                obkv_to_json(&field_ids, &field_ids_map, doc).unwrap()
            })
            .collect::<Vec<_>>();
        snapshot!(serde_json::to_string_pretty(&documents).unwrap(), name: "documents");
    }

    #[test]
    fn fail_in_process_batch_for_document_deletion() {
        let (index_scheduler, mut handle) = IndexScheduler::test(true, vec![]);

        use meilisearch_types::settings::{Settings, Unchecked};
        let mut new_settings: Box<Settings<Unchecked>> = Box::default();
        new_settings.filterable_attributes = Setting::Set(btreeset!(S("catto")));

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

        let content = r#"[
            { "id": 1, "doggo": "jean bob" },
            { "id": 2, "catto": "jorts" },
            { "id": 3, "doggo": "bork" }
        ]"#;

        let (uuid, mut file) = index_scheduler.create_update_file_with_uuid(0).unwrap();
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
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "registered_the_setting_and_document_addition");

        handle.advance_one_successful_batch();
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "after_adding_the_settings");
        handle.advance_one_successful_batch();
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "after_adding_the_documents");

        index_scheduler
            .register(
                KindWithContent::DocumentDeletion {
                    index_uid: S("doggos"),
                    documents_ids: vec![S("1")],
                },
                None,
                false,
            )
            .unwrap();
        // This one should not be catched by Meilisearch but it's still nice to handle it because if one day we break the filters it could happens
        index_scheduler
            .register(
                KindWithContent::DocumentDeletionByFilter {
                    index_uid: S("doggos"),
                    filter_expr: serde_json::json!(true),
                },
                None,
                false,
            )
            .unwrap();
        // Should fail because the ids are not filterable
        index_scheduler
            .register(
                KindWithContent::DocumentDeletionByFilter {
                    index_uid: S("doggos"),
                    filter_expr: serde_json::json!("id = 2"),
                },
                None,
                false,
            )
            .unwrap();
        index_scheduler
            .register(
                KindWithContent::DocumentDeletionByFilter {
                    index_uid: S("doggos"),
                    filter_expr: serde_json::json!("catto EXISTS"),
                },
                None,
                false,
            )
            .unwrap();
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "registered_the_document_deletions");

        // Everything should be batched together
        handle.advance_one_successful_batch();
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "after_removing_the_documents");

        let index = index_scheduler.index("doggos").unwrap();
        let rtxn = index.read_txn().unwrap();
        let field_ids_map = index.fields_ids_map(&rtxn).unwrap();
        let field_ids = field_ids_map.ids().collect::<Vec<_>>();
        let dictionary = index.document_decompression_dictionary(&rtxn).unwrap();
        let mut buffer = Vec::new();
        let documents = index
            .all_compressed_documents(&rtxn)
            .unwrap()
            .map(|ret| {
                let (_docid, compressed_doc) = ret.unwrap();
                let doc = compressed_doc
                    .decompress_with_optional_dictionary(&mut buffer, dictionary.as_ref())
                    .unwrap();
                obkv_to_json(&field_ids, &field_ids_map, doc).unwrap()
            })
            .collect::<Vec<_>>();
        snapshot!(serde_json::to_string_pretty(&documents).unwrap(), name: "documents_remaining_should_only_be_bork");
    }

    #[test]
    fn do_not_batch_task_of_different_indexes() {
        let (index_scheduler, mut handle) = IndexScheduler::test(true, vec![]);
        let index_names = ["doggos", "cattos", "girafos"];

        for name in index_names {
            index_scheduler
                .register(
                    KindWithContent::IndexCreation {
                        index_uid: name.to_string(),
                        primary_key: None,
                    },
                    None,
                    false,
                )
                .unwrap();
            index_scheduler.assert_internally_consistent();
        }

        for name in index_names {
            index_scheduler
                .register(
                    KindWithContent::DocumentClear { index_uid: name.to_string() },
                    None,
                    false,
                )
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

        index_scheduler
            .register(KindWithContent::IndexSwap { swaps: vec![] }, None, false)
            .unwrap();
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

        let (uuid, mut file) = index_scheduler.create_update_file_with_uuid(0).unwrap();
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
    fn test_document_replace() {
        let (index_scheduler, mut handle) = IndexScheduler::test(true, vec![]);

        for i in 0..10 {
            let content = format!(
                r#"{{
                    "id": {},
                    "doggo": "bob {}"
                }}"#,
                i, i
            );

            let (uuid, mut file) = index_scheduler.create_update_file_with_uuid(i).unwrap();
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
            index_scheduler.assert_internally_consistent();
        }
        snapshot!(snapshot_index_scheduler(&index_scheduler));

        // everything should be batched together.
        handle.advance_n_successful_batches(1);
        snapshot!(snapshot_index_scheduler(&index_scheduler));

        // has everything being pushed successfully in milli?
        let index = index_scheduler.index("doggos").unwrap();
        let rtxn = index.read_txn().unwrap();
        let mut buffer = Vec::new();
        let dictionary = index.document_decompression_dictionary(&rtxn).unwrap();
        let field_ids_map = index.fields_ids_map(&rtxn).unwrap();
        let field_ids = field_ids_map.ids().collect::<Vec<_>>();
        let documents = index
            .all_compressed_documents(&rtxn)
            .unwrap()
            .map(|ret| {
                let (_id, compressed_doc) = ret.unwrap();
                let doc = compressed_doc
                    .decompress_with_optional_dictionary(&mut buffer, dictionary.as_ref())
                    .unwrap();
                obkv_to_json(&field_ids, &field_ids_map, doc).unwrap()
            })
            .collect::<Vec<_>>();
        snapshot!(serde_json::to_string_pretty(&documents).unwrap(), name: "documents");
    }

    #[test]
    fn test_document_update() {
        let (index_scheduler, mut handle) = IndexScheduler::test(true, vec![]);

        for i in 0..10 {
            let content = format!(
                r#"{{
                    "id": {},
                    "doggo": "bob {}"
                }}"#,
                i, i
            );

            let (uuid, mut file) = index_scheduler.create_update_file_with_uuid(i).unwrap();
            let documents_count = read_json(content.as_bytes(), &mut file).unwrap();
            file.persist().unwrap();
            index_scheduler
                .register(
                    KindWithContent::DocumentAdditionOrUpdate {
                        index_uid: S("doggos"),
                        primary_key: Some(S("id")),
                        method: UpdateDocuments,
                        content_file: uuid,
                        documents_count,
                        allow_index_creation: true,
                    },
                    None,
                    false,
                )
                .unwrap();
            index_scheduler.assert_internally_consistent();
        }
        snapshot!(snapshot_index_scheduler(&index_scheduler));

        // everything should be batched together.
        handle.advance_n_successful_batches(1);
        snapshot!(snapshot_index_scheduler(&index_scheduler));

        // has everything being pushed successfully in milli?
        let index = index_scheduler.index("doggos").unwrap();
        let rtxn = index.read_txn().unwrap();
        let mut buffer = Vec::new();
        let dictionary = index.document_decompression_dictionary(&rtxn).unwrap();
        let field_ids_map = index.fields_ids_map(&rtxn).unwrap();
        let field_ids = field_ids_map.ids().collect::<Vec<_>>();
        let documents = index
            .all_compressed_documents(&rtxn)
            .unwrap()
            .map(|ret| {
                let (_id, compressed_doc) = ret.unwrap();
                let doc = compressed_doc
                    .decompress_with_optional_dictionary(&mut buffer, dictionary.as_ref())
                    .unwrap();
                obkv_to_json(&field_ids, &field_ids_map, doc).unwrap()
            })
            .collect::<Vec<_>>();
        snapshot!(serde_json::to_string_pretty(&documents).unwrap(), name: "documents");
    }

    #[test]
    fn test_mixed_document_addition() {
        let (index_scheduler, mut handle) = IndexScheduler::test(true, vec![]);

        for i in 0..10 {
            let method = if i % 2 == 0 { UpdateDocuments } else { ReplaceDocuments };

            let content = format!(
                r#"{{
                    "id": {},
                    "doggo": "bob {}"
                }}"#,
                i, i
            );

            let (uuid, mut file) = index_scheduler.create_update_file_with_uuid(i).unwrap();
            let documents_count = read_json(content.as_bytes(), &mut file).unwrap();
            file.persist().unwrap();
            index_scheduler
                .register(
                    KindWithContent::DocumentAdditionOrUpdate {
                        index_uid: S("doggos"),
                        primary_key: Some(S("id")),
                        method,
                        content_file: uuid,
                        documents_count,
                        allow_index_creation: true,
                    },
                    None,
                    false,
                )
                .unwrap();
            index_scheduler.assert_internally_consistent();
        }
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "after_registering_the_10_tasks");

        // Only half of the task should've been processed since we can't autobatch replace and update together.
        handle.advance_n_successful_batches(5);
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "five_tasks_processed");

        handle.advance_n_successful_batches(5);
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "all_tasks_processed");

        // has everything being pushed successfully in milli?
        let index = index_scheduler.index("doggos").unwrap();
        let rtxn = index.read_txn().unwrap();
        let mut buffer = Vec::new();
        let dictionary = index.document_decompression_dictionary(&rtxn).unwrap();
        let field_ids_map = index.fields_ids_map(&rtxn).unwrap();
        let field_ids = field_ids_map.ids().collect::<Vec<_>>();
        let documents = index
            .all_compressed_documents(&rtxn)
            .unwrap()
            .map(|ret| {
                let (_id, compressed_doc) = ret.unwrap();
                let doc = compressed_doc
                    .decompress_with_optional_dictionary(&mut buffer, dictionary.as_ref())
                    .unwrap();
                obkv_to_json(&field_ids, &field_ids_map, doc).unwrap()
            })
            .collect::<Vec<_>>();
        snapshot!(serde_json::to_string_pretty(&documents).unwrap(), name: "documents");
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
        embedders.insert(S("default"), Setting::Set(embedding_settings));
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
            let task = index_scheduler.get_task(&rtxn, 0).unwrap().unwrap();
            let task = meilisearch_types::task_view::TaskView::from_task(&task);
            insta::assert_json_snapshot!(task.details);
        }

        handle.advance_n_successful_batches(1);
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "settings_update_processed");

        {
            let rtxn = index_scheduler.read_txn().unwrap();
            let task = index_scheduler.get_task(&rtxn, 0).unwrap().unwrap();
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
    fn test_document_replace_without_autobatching() {
        let (index_scheduler, mut handle) = IndexScheduler::test(false, vec![]);

        for i in 0..10 {
            let content = format!(
                r#"{{
                    "id": {},
                    "doggo": "bob {}"
                }}"#,
                i, i
            );

            let (uuid, mut file) = index_scheduler.create_update_file_with_uuid(i).unwrap();
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
            index_scheduler.assert_internally_consistent();
        }
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "after_registering_the_10_tasks");

        // Nothing should be batched thus half of the tasks are processed.
        handle.advance_n_successful_batches(5);
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "five_tasks_processed");

        // Everything is processed.
        handle.advance_n_successful_batches(5);
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "all_tasks_processed");

        // has everything being pushed successfully in milli?
        let index = index_scheduler.index("doggos").unwrap();
        let rtxn = index.read_txn().unwrap();
        let mut buffer = Vec::new();
        let dictionary = index.document_decompression_dictionary(&rtxn).unwrap();
        let field_ids_map = index.fields_ids_map(&rtxn).unwrap();
        let field_ids = field_ids_map.ids().collect::<Vec<_>>();
        let documents = index
            .all_compressed_documents(&rtxn)
            .unwrap()
            .map(|ret| {
                let (_id, compressed_doc) = ret.unwrap();
                let doc = compressed_doc
                    .decompress_with_optional_dictionary(&mut buffer, dictionary.as_ref())
                    .unwrap();
                obkv_to_json(&field_ids, &field_ids_map, doc).unwrap()
            })
            .collect::<Vec<_>>();
        snapshot!(serde_json::to_string_pretty(&documents).unwrap(), name: "documents");
    }

    #[test]
    fn test_document_update_without_autobatching() {
        let (index_scheduler, mut handle) = IndexScheduler::test(false, vec![]);

        for i in 0..10 {
            let content = format!(
                r#"{{
                    "id": {},
                    "doggo": "bob {}"
                }}"#,
                i, i
            );

            let (uuid, mut file) = index_scheduler.create_update_file_with_uuid(i).unwrap();
            let documents_count = read_json(content.as_bytes(), &mut file).unwrap();
            file.persist().unwrap();
            index_scheduler
                .register(
                    KindWithContent::DocumentAdditionOrUpdate {
                        index_uid: S("doggos"),
                        primary_key: Some(S("id")),
                        method: UpdateDocuments,
                        content_file: uuid,
                        documents_count,
                        allow_index_creation: true,
                    },
                    None,
                    false,
                )
                .unwrap();
            index_scheduler.assert_internally_consistent();
        }
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "after_registering_the_10_tasks");

        // Nothing should be batched thus half of the tasks are processed.
        handle.advance_n_successful_batches(5);
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "five_tasks_processed");

        // Everything is processed.
        handle.advance_n_successful_batches(5);
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "all_tasks_processed");

        // has everything being pushed successfully in milli?
        let index = index_scheduler.index("doggos").unwrap();
        let rtxn = index.read_txn().unwrap();
        let mut buffer = Vec::new();
        let dictionary = index.document_decompression_dictionary(&rtxn).unwrap();
        let field_ids_map = index.fields_ids_map(&rtxn).unwrap();
        let field_ids = field_ids_map.ids().collect::<Vec<_>>();
        let documents = index
            .all_compressed_documents(&rtxn)
            .unwrap()
            .map(|ret| {
                let (_id, compressed_doc) = ret.unwrap();
                let doc = compressed_doc
                    .decompress_with_optional_dictionary(&mut buffer, dictionary.as_ref())
                    .unwrap();
                obkv_to_json(&field_ids, &field_ids_map, doc).unwrap()
            })
            .collect::<Vec<_>>();
        snapshot!(serde_json::to_string_pretty(&documents).unwrap(), name: "documents");
    }

    #[macro_export]
    macro_rules! debug_snapshot {
        ($value:expr, @$snapshot:literal) => {{
            let value = format!("{:?}", $value);
            meili_snap::snapshot!(value, @$snapshot);
        }};
    }

    #[test]
    fn simple_new() {
        crate::IndexScheduler::test(true, vec![]);
    }

    #[test]
    fn query_tasks_from_and_limit() {
        let (index_scheduler, mut handle) = IndexScheduler::test(true, vec![]);

        let kind = index_creation_task("doggo", "bone");
        let _task = index_scheduler.register(kind, None, false).unwrap();
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "registered_the_first_task");
        let kind = index_creation_task("whalo", "plankton");
        let _task = index_scheduler.register(kind, None, false).unwrap();
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "registered_the_second_task");
        let kind = index_creation_task("catto", "his_own_vomit");
        let _task = index_scheduler.register(kind, None, false).unwrap();
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "registered_the_third_task");

        handle.advance_n_successful_batches(3);
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "processed_all_tasks");

        let rtxn = index_scheduler.env.read_txn().unwrap();
        let query = Query { limit: Some(0), ..Default::default() };
        let (tasks, _) = index_scheduler
            .get_task_ids_from_authorized_indexes(&rtxn, &query, &AuthFilter::default())
            .unwrap();
        snapshot!(snapshot_bitmap(&tasks), @"[]");

        let query = Query { limit: Some(1), ..Default::default() };
        let (tasks, _) = index_scheduler
            .get_task_ids_from_authorized_indexes(&rtxn, &query, &AuthFilter::default())
            .unwrap();
        snapshot!(snapshot_bitmap(&tasks), @"[2,]");

        let query = Query { limit: Some(2), ..Default::default() };
        let (tasks, _) = index_scheduler
            .get_task_ids_from_authorized_indexes(&rtxn, &query, &AuthFilter::default())
            .unwrap();
        snapshot!(snapshot_bitmap(&tasks), @"[1,2,]");

        let query = Query { from: Some(1), ..Default::default() };
        let (tasks, _) = index_scheduler
            .get_task_ids_from_authorized_indexes(&rtxn, &query, &AuthFilter::default())
            .unwrap();
        snapshot!(snapshot_bitmap(&tasks), @"[0,1,]");

        let query = Query { from: Some(2), ..Default::default() };
        let (tasks, _) = index_scheduler
            .get_task_ids_from_authorized_indexes(&rtxn, &query, &AuthFilter::default())
            .unwrap();
        snapshot!(snapshot_bitmap(&tasks), @"[0,1,2,]");

        let query = Query { from: Some(1), limit: Some(1), ..Default::default() };
        let (tasks, _) = index_scheduler
            .get_task_ids_from_authorized_indexes(&rtxn, &query, &AuthFilter::default())
            .unwrap();
        snapshot!(snapshot_bitmap(&tasks), @"[1,]");

        let query = Query { from: Some(1), limit: Some(2), ..Default::default() };
        let (tasks, _) = index_scheduler
            .get_task_ids_from_authorized_indexes(&rtxn, &query, &AuthFilter::default())
            .unwrap();
        snapshot!(snapshot_bitmap(&tasks), @"[0,1,]");
    }

    #[test]
    fn query_tasks_simple() {
        let start_time = OffsetDateTime::now_utc();

        let (index_scheduler, mut handle) =
            IndexScheduler::test(true, vec![(3, FailureLocation::InsideProcessBatch)]);

        let kind = index_creation_task("catto", "mouse");
        let _task = index_scheduler.register(kind, None, false).unwrap();
        let kind = index_creation_task("doggo", "sheep");
        let _task = index_scheduler.register(kind, None, false).unwrap();
        let kind = index_creation_task("whalo", "fish");
        let _task = index_scheduler.register(kind, None, false).unwrap();

        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "start");

        handle.advance_till([Start, BatchCreated]);

        let rtxn = index_scheduler.env.read_txn().unwrap();

        let query = Query { statuses: Some(vec![Status::Processing]), ..Default::default() };
        let (tasks, _) = index_scheduler
            .get_task_ids_from_authorized_indexes(&rtxn, &query, &AuthFilter::default())
            .unwrap();
        snapshot!(snapshot_bitmap(&tasks), @"[0,]"); // only the processing tasks in the first tick

        let query = Query { statuses: Some(vec![Status::Enqueued]), ..Default::default() };
        let (tasks, _) = index_scheduler
            .get_task_ids_from_authorized_indexes(&rtxn, &query, &AuthFilter::default())
            .unwrap();
        snapshot!(snapshot_bitmap(&tasks), @"[1,2,]"); // only the enqueued tasks in the first tick

        let query = Query {
            statuses: Some(vec![Status::Enqueued, Status::Processing]),
            ..Default::default()
        };
        let (tasks, _) = index_scheduler
            .get_task_ids_from_authorized_indexes(&rtxn, &query, &AuthFilter::default())
            .unwrap();
        snapshot!(snapshot_bitmap(&tasks), @"[0,1,2,]"); // both enqueued and processing tasks in the first tick

        let query = Query {
            statuses: Some(vec![Status::Enqueued, Status::Processing]),
            after_started_at: Some(start_time),
            ..Default::default()
        };
        let (tasks, _) = index_scheduler
            .get_task_ids_from_authorized_indexes(&rtxn, &query, &AuthFilter::default())
            .unwrap();
        // both enqueued and processing tasks in the first tick, but limited to those with a started_at
        // that comes after the start of the test, which should excludes the enqueued tasks
        snapshot!(snapshot_bitmap(&tasks), @"[0,]");

        let query = Query {
            statuses: Some(vec![Status::Enqueued, Status::Processing]),
            before_started_at: Some(start_time),
            ..Default::default()
        };
        let (tasks, _) = index_scheduler
            .get_task_ids_from_authorized_indexes(&rtxn, &query, &AuthFilter::default())
            .unwrap();
        // both enqueued and processing tasks in the first tick, but limited to those with a started_at
        // that comes before the start of the test, which should excludes all of them
        snapshot!(snapshot_bitmap(&tasks), @"[]");

        let query = Query {
            statuses: Some(vec![Status::Enqueued, Status::Processing]),
            after_started_at: Some(start_time),
            before_started_at: Some(start_time + Duration::minutes(1)),
            ..Default::default()
        };
        let (tasks, _) = index_scheduler
            .get_task_ids_from_authorized_indexes(&rtxn, &query, &AuthFilter::default())
            .unwrap();
        // both enqueued and processing tasks in the first tick, but limited to those with a started_at
        // that comes after the start of the test and before one minute after the start of the test,
        // which should exclude the enqueued tasks and include the only processing task
        snapshot!(snapshot_bitmap(&tasks), @"[0,]");

        handle.advance_till([
            InsideProcessBatch,
            InsideProcessBatch,
            ProcessBatchSucceeded,
            AfterProcessing,
            Start,
            BatchCreated,
        ]);

        let rtxn = index_scheduler.env.read_txn().unwrap();

        let second_start_time = OffsetDateTime::now_utc();

        let query = Query {
            statuses: Some(vec![Status::Succeeded, Status::Processing]),
            after_started_at: Some(start_time),
            before_started_at: Some(start_time + Duration::minutes(1)),
            ..Default::default()
        };
        let (tasks, _) = index_scheduler
            .get_task_ids_from_authorized_indexes(&rtxn, &query, &AuthFilter::default())
            .unwrap();
        // both succeeded and processing tasks in the first tick, but limited to those with a started_at
        // that comes after the start of the test and before one minute after the start of the test,
        // which should include all tasks
        snapshot!(snapshot_bitmap(&tasks), @"[0,1,]");

        let query = Query {
            statuses: Some(vec![Status::Succeeded, Status::Processing]),
            before_started_at: Some(start_time),
            ..Default::default()
        };
        let (tasks, _) = index_scheduler
            .get_task_ids_from_authorized_indexes(&rtxn, &query, &AuthFilter::default())
            .unwrap();
        // both succeeded and processing tasks in the first tick, but limited to those with a started_at
        // that comes before the start of the test, which should exclude all tasks
        snapshot!(snapshot_bitmap(&tasks), @"[]");

        let query = Query {
            statuses: Some(vec![Status::Enqueued, Status::Succeeded, Status::Processing]),
            after_started_at: Some(second_start_time),
            before_started_at: Some(second_start_time + Duration::minutes(1)),
            ..Default::default()
        };
        let (tasks, _) = index_scheduler
            .get_task_ids_from_authorized_indexes(&rtxn, &query, &AuthFilter::default())
            .unwrap();
        // both succeeded and processing tasks in the first tick, but limited to those with a started_at
        // that comes after the start of the second part of the test and before one minute after the
        // second start of the test, which should exclude all tasks
        snapshot!(snapshot_bitmap(&tasks), @"[]");

        // now we make one more batch, the started_at field of the new tasks will be past `second_start_time`
        handle.advance_till([
            InsideProcessBatch,
            InsideProcessBatch,
            ProcessBatchSucceeded,
            AfterProcessing,
            Start,
            BatchCreated,
        ]);

        let rtxn = index_scheduler.env.read_txn().unwrap();

        let (tasks, _) = index_scheduler
            .get_task_ids_from_authorized_indexes(&rtxn, &query, &AuthFilter::default())
            .unwrap();
        // we run the same query to verify that, and indeed find that the last task is matched
        snapshot!(snapshot_bitmap(&tasks), @"[2,]");

        let query = Query {
            statuses: Some(vec![Status::Enqueued, Status::Succeeded, Status::Processing]),
            after_started_at: Some(second_start_time),
            before_started_at: Some(second_start_time + Duration::minutes(1)),
            ..Default::default()
        };
        let (tasks, _) = index_scheduler
            .get_task_ids_from_authorized_indexes(&rtxn, &query, &AuthFilter::default())
            .unwrap();
        // enqueued, succeeded, or processing tasks started after the second part of the test, should
        // again only return the last task
        snapshot!(snapshot_bitmap(&tasks), @"[2,]");

        handle.advance_till([ProcessBatchFailed, AfterProcessing]);
        let rtxn = index_scheduler.read_txn().unwrap();

        // now the last task should have failed
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "end");
        let (tasks, _) = index_scheduler
            .get_task_ids_from_authorized_indexes(&rtxn, &query, &AuthFilter::default())
            .unwrap();
        // so running the last query should return nothing
        snapshot!(snapshot_bitmap(&tasks), @"[]");

        let query = Query {
            statuses: Some(vec![Status::Failed]),
            after_started_at: Some(second_start_time),
            before_started_at: Some(second_start_time + Duration::minutes(1)),
            ..Default::default()
        };
        let (tasks, _) = index_scheduler
            .get_task_ids_from_authorized_indexes(&rtxn, &query, &AuthFilter::default())
            .unwrap();
        // but the same query on failed tasks should return the last task
        snapshot!(snapshot_bitmap(&tasks), @"[2,]");

        let query = Query {
            statuses: Some(vec![Status::Failed]),
            after_started_at: Some(second_start_time),
            before_started_at: Some(second_start_time + Duration::minutes(1)),
            ..Default::default()
        };
        let (tasks, _) = index_scheduler
            .get_task_ids_from_authorized_indexes(&rtxn, &query, &AuthFilter::default())
            .unwrap();
        // but the same query on failed tasks should return the last task
        snapshot!(snapshot_bitmap(&tasks), @"[2,]");

        let query = Query {
            statuses: Some(vec![Status::Failed]),
            uids: Some(vec![1]),
            after_started_at: Some(second_start_time),
            before_started_at: Some(second_start_time + Duration::minutes(1)),
            ..Default::default()
        };
        let (tasks, _) = index_scheduler
            .get_task_ids_from_authorized_indexes(&rtxn, &query, &AuthFilter::default())
            .unwrap();
        // same query but with an invalid uid
        snapshot!(snapshot_bitmap(&tasks), @"[]");

        let query = Query {
            statuses: Some(vec![Status::Failed]),
            uids: Some(vec![2]),
            after_started_at: Some(second_start_time),
            before_started_at: Some(second_start_time + Duration::minutes(1)),
            ..Default::default()
        };
        let (tasks, _) = index_scheduler
            .get_task_ids_from_authorized_indexes(&rtxn, &query, &AuthFilter::default())
            .unwrap();
        // same query but with a valid uid
        snapshot!(snapshot_bitmap(&tasks), @"[2,]");
    }

    #[test]
    fn query_tasks_special_rules() {
        let (index_scheduler, mut handle) =
            IndexScheduler::test(true, vec![(3, FailureLocation::InsideProcessBatch)]);

        let kind = index_creation_task("catto", "mouse");
        let _task = index_scheduler.register(kind, None, false).unwrap();
        let kind = index_creation_task("doggo", "sheep");
        let _task = index_scheduler.register(kind, None, false).unwrap();
        let kind = KindWithContent::IndexSwap {
            swaps: vec![IndexSwap { indexes: ("catto".to_owned(), "doggo".to_owned()) }],
        };
        let _task = index_scheduler.register(kind, None, false).unwrap();
        let kind = KindWithContent::IndexSwap {
            swaps: vec![IndexSwap { indexes: ("catto".to_owned(), "whalo".to_owned()) }],
        };
        let _task = index_scheduler.register(kind, None, false).unwrap();

        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "start");

        handle.advance_till([Start, BatchCreated]);

        let rtxn = index_scheduler.env.read_txn().unwrap();

        let query = Query { index_uids: Some(vec!["catto".to_owned()]), ..Default::default() };
        let (tasks, _) = index_scheduler
            .get_task_ids_from_authorized_indexes(&rtxn, &query, &AuthFilter::default())
            .unwrap();
        // only the first task associated with catto is returned, the indexSwap tasks are excluded!
        snapshot!(snapshot_bitmap(&tasks), @"[0,]");

        let query = Query { index_uids: Some(vec!["catto".to_owned()]), ..Default::default() };
        let (tasks, _) = index_scheduler
            .get_task_ids_from_authorized_indexes(
                &rtxn,
                &query,
                &AuthFilter::with_allowed_indexes(
                    vec![IndexUidPattern::new_unchecked("doggo")].into_iter().collect(),
                ),
            )
            .unwrap();
        // we have asked for only the tasks associated with catto, but are only authorized to retrieve the tasks
        // associated with doggo -> empty result
        snapshot!(snapshot_bitmap(&tasks), @"[]");

        let query = Query::default();
        let (tasks, _) = index_scheduler
            .get_task_ids_from_authorized_indexes(
                &rtxn,
                &query,
                &AuthFilter::with_allowed_indexes(
                    vec![IndexUidPattern::new_unchecked("doggo")].into_iter().collect(),
                ),
            )
            .unwrap();
        // we asked for all the tasks, but we are only authorized to retrieve the doggo tasks
        // -> only the index creation of doggo should be returned
        snapshot!(snapshot_bitmap(&tasks), @"[1,]");

        let query = Query::default();
        let (tasks, _) = index_scheduler
            .get_task_ids_from_authorized_indexes(
                &rtxn,
                &query,
                &AuthFilter::with_allowed_indexes(
                    vec![
                        IndexUidPattern::new_unchecked("catto"),
                        IndexUidPattern::new_unchecked("doggo"),
                    ]
                    .into_iter()
                    .collect(),
                ),
            )
            .unwrap();
        // we asked for all the tasks, but we are only authorized to retrieve the doggo and catto tasks
        // -> all tasks except the swap of catto with whalo are returned
        snapshot!(snapshot_bitmap(&tasks), @"[0,1,]");

        let query = Query::default();
        let (tasks, _) = index_scheduler
            .get_task_ids_from_authorized_indexes(&rtxn, &query, &AuthFilter::default())
            .unwrap();
        // we asked for all the tasks with all index authorized -> all tasks returned
        snapshot!(snapshot_bitmap(&tasks), @"[0,1,2,3,]");
    }

    #[test]
    fn query_tasks_canceled_by() {
        let (index_scheduler, mut handle) =
            IndexScheduler::test(true, vec![(3, FailureLocation::InsideProcessBatch)]);

        let kind = index_creation_task("catto", "mouse");
        let _ = index_scheduler.register(kind, None, false).unwrap();
        let kind = index_creation_task("doggo", "sheep");
        let _ = index_scheduler.register(kind, None, false).unwrap();
        let kind = KindWithContent::IndexSwap {
            swaps: vec![IndexSwap { indexes: ("catto".to_owned(), "doggo".to_owned()) }],
        };
        let _task = index_scheduler.register(kind, None, false).unwrap();

        handle.advance_n_successful_batches(1);
        let kind = KindWithContent::TaskCancelation {
            query: "test_query".to_string(),
            tasks: [0, 1, 2, 3].into_iter().collect(),
        };
        let task_cancelation = index_scheduler.register(kind, None, false).unwrap();
        handle.advance_n_successful_batches(1);

        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "start");

        let rtxn = index_scheduler.read_txn().unwrap();
        let query = Query { canceled_by: Some(vec![task_cancelation.uid]), ..Query::default() };
        let (tasks, _) = index_scheduler
            .get_task_ids_from_authorized_indexes(&rtxn, &query, &AuthFilter::default())
            .unwrap();
        // 0 is not returned because it was not canceled, 3 is not returned because it is the uid of the
        // taskCancelation itself
        snapshot!(snapshot_bitmap(&tasks), @"[1,2,]");

        let query = Query { canceled_by: Some(vec![task_cancelation.uid]), ..Query::default() };
        let (tasks, _) = index_scheduler
            .get_task_ids_from_authorized_indexes(
                &rtxn,
                &query,
                &AuthFilter::with_allowed_indexes(
                    vec![IndexUidPattern::new_unchecked("doggo")].into_iter().collect(),
                ),
            )
            .unwrap();
        // Return only 1 because the user is not authorized to see task 2
        snapshot!(snapshot_bitmap(&tasks), @"[1,]");
    }

    #[test]
    fn query_batches_from_and_limit() {
        let (index_scheduler, mut handle) = IndexScheduler::test(true, vec![]);

        let kind = index_creation_task("doggo", "bone");
        let _task = index_scheduler.register(kind, None, false).unwrap();
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "registered_the_first_task");
        let kind = index_creation_task("whalo", "plankton");
        let _task = index_scheduler.register(kind, None, false).unwrap();
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "registered_the_second_task");
        let kind = index_creation_task("catto", "his_own_vomit");
        let _task = index_scheduler.register(kind, None, false).unwrap();
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "registered_the_third_task");

        handle.advance_n_successful_batches(3);
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "processed_all_tasks");

        let proc = index_scheduler.processing_tasks.read().unwrap().clone();
        let rtxn = index_scheduler.env.read_txn().unwrap();
        let query = Query { limit: Some(0), ..Default::default() };
        let (batches, _) = index_scheduler
            .get_batch_ids_from_authorized_indexes(&rtxn, &proc, &query, &AuthFilter::default())
            .unwrap();
        snapshot!(snapshot_bitmap(&batches), @"[]");

        let query = Query { limit: Some(1), ..Default::default() };
        let (batches, _) = index_scheduler
            .get_batch_ids_from_authorized_indexes(&rtxn, &proc, &query, &AuthFilter::default())
            .unwrap();
        snapshot!(snapshot_bitmap(&batches), @"[2,]");

        let query = Query { limit: Some(2), ..Default::default() };
        let (batches, _) = index_scheduler
            .get_batch_ids_from_authorized_indexes(&rtxn, &proc, &query, &AuthFilter::default())
            .unwrap();
        snapshot!(snapshot_bitmap(&batches), @"[1,2,]");

        let query = Query { from: Some(1), ..Default::default() };
        let (batches, _) = index_scheduler
            .get_batch_ids_from_authorized_indexes(&rtxn, &proc, &query, &AuthFilter::default())
            .unwrap();
        snapshot!(snapshot_bitmap(&batches), @"[0,1,]");

        let query = Query { from: Some(2), ..Default::default() };
        let (batches, _) = index_scheduler
            .get_batch_ids_from_authorized_indexes(&rtxn, &proc, &query, &AuthFilter::default())
            .unwrap();
        snapshot!(snapshot_bitmap(&batches), @"[0,1,2,]");

        let query = Query { from: Some(1), limit: Some(1), ..Default::default() };
        let (batches, _) = index_scheduler
            .get_batch_ids_from_authorized_indexes(&rtxn, &proc, &query, &AuthFilter::default())
            .unwrap();
        snapshot!(snapshot_bitmap(&batches), @"[1,]");

        let query = Query { from: Some(1), limit: Some(2), ..Default::default() };
        let (batches, _) = index_scheduler
            .get_batch_ids_from_authorized_indexes(&rtxn, &proc, &query, &AuthFilter::default())
            .unwrap();
        snapshot!(snapshot_bitmap(&batches), @"[0,1,]");
    }

    #[test]
    fn query_batches_simple() {
        let start_time = OffsetDateTime::now_utc();

        let (index_scheduler, mut handle) =
            IndexScheduler::test(true, vec![(3, FailureLocation::InsideProcessBatch)]);

        let kind = index_creation_task("catto", "mouse");
        let _task = index_scheduler.register(kind, None, false).unwrap();
        let kind = index_creation_task("doggo", "sheep");
        let _task = index_scheduler.register(kind, None, false).unwrap();
        let kind = index_creation_task("whalo", "fish");
        let _task = index_scheduler.register(kind, None, false).unwrap();

        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "start");

        handle.advance_till([Start, BatchCreated]);

        let rtxn = index_scheduler.env.read_txn().unwrap();
        let proc = index_scheduler.processing_tasks.read().unwrap().clone();

        let query = Query { statuses: Some(vec![Status::Processing]), ..Default::default() };
        let (mut batches, _) = index_scheduler
            .get_batches_from_authorized_indexes(query.clone(), &AuthFilter::default())
            .unwrap();
        assert_eq!(batches.len(), 1);
        batches[0].started_at = OffsetDateTime::UNIX_EPOCH;
        // Insta cannot snapshot our batches because the batch stats contains an enum as key: https://github.com/mitsuhiko/insta/issues/689
        let batch = serde_json::to_string_pretty(&batches[0]).unwrap();
        snapshot!(batch, @r#"
        {
          "uid": 0,
          "details": {
            "primaryKey": "mouse"
          },
          "stats": {
            "totalNbTasks": 1,
            "status": {
              "processing": 1
            },
            "types": {
              "indexCreation": 1
            },
            "indexUids": {
              "catto": 1
            }
          },
          "startedAt": "1970-01-01T00:00:00Z",
          "finishedAt": null
        }
        "#);

        let query = Query { statuses: Some(vec![Status::Enqueued]), ..Default::default() };
        let (batches, _) = index_scheduler
            .get_batch_ids_from_authorized_indexes(&rtxn, &proc, &query, &AuthFilter::default())
            .unwrap();
        snapshot!(snapshot_bitmap(&batches), @"[]"); // The batches don't contains any enqueued tasks

        let query = Query {
            statuses: Some(vec![Status::Enqueued, Status::Processing]),
            ..Default::default()
        };
        let (batches, _) = index_scheduler
            .get_batch_ids_from_authorized_indexes(&rtxn, &proc, &query, &AuthFilter::default())
            .unwrap();
        snapshot!(snapshot_bitmap(&batches), @"[0,]"); // both enqueued and processing tasks in the first tick

        let query = Query {
            statuses: Some(vec![Status::Enqueued, Status::Processing]),
            after_started_at: Some(start_time),
            ..Default::default()
        };
        let (batches, _) = index_scheduler
            .get_batch_ids_from_authorized_indexes(&rtxn, &proc, &query, &AuthFilter::default())
            .unwrap();
        // both enqueued and processing tasks in the first tick, but limited to those with a started_at
        // that comes after the start of the test, which should excludes the enqueued tasks
        snapshot!(snapshot_bitmap(&batches), @"[0,]");

        let query = Query {
            statuses: Some(vec![Status::Enqueued, Status::Processing]),
            before_started_at: Some(start_time),
            ..Default::default()
        };
        let (batches, _) = index_scheduler
            .get_batch_ids_from_authorized_indexes(&rtxn, &proc, &query, &AuthFilter::default())
            .unwrap();
        // both enqueued and processing tasks in the first tick, but limited to those with a started_at
        // that comes before the start of the test, which should excludes all of them
        snapshot!(snapshot_bitmap(&batches), @"[]");

        let query = Query {
            statuses: Some(vec![Status::Enqueued, Status::Processing]),
            after_started_at: Some(start_time),
            before_started_at: Some(start_time + Duration::minutes(1)),
            ..Default::default()
        };
        let (batches, _) = index_scheduler
            .get_batch_ids_from_authorized_indexes(&rtxn, &proc, &query, &AuthFilter::default())
            .unwrap();
        // both enqueued and processing tasks in the first tick, but limited to those with a started_at
        // that comes after the start of the test and before one minute after the start of the test,
        // which should exclude the enqueued tasks and include the only processing task
        snapshot!(snapshot_bitmap(&batches), @"[0,]");

        handle.advance_till([
            InsideProcessBatch,
            InsideProcessBatch,
            ProcessBatchSucceeded,
            AfterProcessing,
            Start,
            BatchCreated,
        ]);
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "after-advancing-a-bit");

        let rtxn = index_scheduler.env.read_txn().unwrap();
        let proc = index_scheduler.processing_tasks.read().unwrap().clone();

        let second_start_time = OffsetDateTime::now_utc();

        let query = Query {
            statuses: Some(vec![Status::Succeeded, Status::Processing]),
            after_started_at: Some(start_time),
            before_started_at: Some(start_time + Duration::minutes(1)),
            ..Default::default()
        };
        let (batches, _) = index_scheduler
            .get_batch_ids_from_authorized_indexes(&rtxn, &proc, &query, &AuthFilter::default())
            .unwrap();
        // both succeeded and processing tasks in the first tick, but limited to those with a started_at
        // that comes after the start of the test and before one minute after the start of the test,
        // which should include all tasks
        snapshot!(snapshot_bitmap(&batches), @"[0,1,]");

        let query = Query {
            statuses: Some(vec![Status::Succeeded, Status::Processing]),
            before_started_at: Some(start_time),
            ..Default::default()
        };
        let (batches, _) = index_scheduler
            .get_batch_ids_from_authorized_indexes(&rtxn, &proc, &query, &AuthFilter::default())
            .unwrap();
        // both succeeded and processing tasks in the first tick, but limited to those with a started_at
        // that comes before the start of the test, which should exclude all tasks
        snapshot!(snapshot_bitmap(&batches), @"[]");

        let query = Query {
            statuses: Some(vec![Status::Enqueued, Status::Succeeded, Status::Processing]),
            after_started_at: Some(second_start_time),
            before_started_at: Some(second_start_time + Duration::minutes(1)),
            ..Default::default()
        };
        let (batches, _) = index_scheduler
            .get_batch_ids_from_authorized_indexes(&rtxn, &proc, &query, &AuthFilter::default())
            .unwrap();
        // both succeeded and processing tasks in the first tick, but limited to those with a started_at
        // that comes after the start of the second part of the test and before one minute after the
        // second start of the test, which should exclude all tasks
        snapshot!(snapshot_bitmap(&batches), @"[]");

        // now we make one more batch, the started_at field of the new tasks will be past `second_start_time`
        handle.advance_till([
            InsideProcessBatch,
            InsideProcessBatch,
            ProcessBatchSucceeded,
            AfterProcessing,
            Start,
            BatchCreated,
        ]);

        let rtxn = index_scheduler.env.read_txn().unwrap();
        let proc = index_scheduler.processing_tasks.read().unwrap().clone();

        let (batches, _) = index_scheduler
            .get_batch_ids_from_authorized_indexes(&rtxn, &proc, &query, &AuthFilter::default())
            .unwrap();
        // we run the same query to verify that, and indeed find that the last task is matched
        snapshot!(snapshot_bitmap(&batches), @"[2,]");

        let query = Query {
            statuses: Some(vec![Status::Enqueued, Status::Succeeded, Status::Processing]),
            after_started_at: Some(second_start_time),
            before_started_at: Some(second_start_time + Duration::minutes(1)),
            ..Default::default()
        };
        let (batches, _) = index_scheduler
            .get_batch_ids_from_authorized_indexes(&rtxn, &proc, &query, &AuthFilter::default())
            .unwrap();
        // enqueued, succeeded, or processing tasks started after the second part of the test, should
        // again only return the last task
        snapshot!(snapshot_bitmap(&batches), @"[2,]");

        handle.advance_till([ProcessBatchFailed, AfterProcessing]);
        let rtxn = index_scheduler.read_txn().unwrap();
        let proc = index_scheduler.processing_tasks.read().unwrap().clone();

        // now the last task should have failed
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "end");
        let (batches, _) = index_scheduler
            .get_batch_ids_from_authorized_indexes(&rtxn, &proc, &query, &AuthFilter::default())
            .unwrap();
        // so running the last query should return nothing
        snapshot!(snapshot_bitmap(&batches), @"[]");

        let query = Query {
            statuses: Some(vec![Status::Failed]),
            after_started_at: Some(second_start_time),
            before_started_at: Some(second_start_time + Duration::minutes(1)),
            ..Default::default()
        };
        let (batches, _) = index_scheduler
            .get_batch_ids_from_authorized_indexes(&rtxn, &proc, &query, &AuthFilter::default())
            .unwrap();
        // but the same query on failed tasks should return the last task
        snapshot!(snapshot_bitmap(&batches), @"[2,]");

        let query = Query {
            statuses: Some(vec![Status::Failed]),
            after_started_at: Some(second_start_time),
            before_started_at: Some(second_start_time + Duration::minutes(1)),
            ..Default::default()
        };
        let (batches, _) = index_scheduler
            .get_batch_ids_from_authorized_indexes(&rtxn, &proc, &query, &AuthFilter::default())
            .unwrap();
        // but the same query on failed tasks should return the last task
        snapshot!(snapshot_bitmap(&batches), @"[2,]");

        let query = Query {
            statuses: Some(vec![Status::Failed]),
            uids: Some(vec![1]),
            after_started_at: Some(second_start_time),
            before_started_at: Some(second_start_time + Duration::minutes(1)),
            ..Default::default()
        };
        let (batches, _) = index_scheduler
            .get_batch_ids_from_authorized_indexes(&rtxn, &proc, &query, &AuthFilter::default())
            .unwrap();
        // same query but with an invalid uid
        snapshot!(snapshot_bitmap(&batches), @"[]");

        let query = Query {
            statuses: Some(vec![Status::Failed]),
            uids: Some(vec![2]),
            after_started_at: Some(second_start_time),
            before_started_at: Some(second_start_time + Duration::minutes(1)),
            ..Default::default()
        };
        let (batches, _) = index_scheduler
            .get_batch_ids_from_authorized_indexes(&rtxn, &proc, &query, &AuthFilter::default())
            .unwrap();
        // same query but with a valid uid
        snapshot!(snapshot_bitmap(&batches), @"[2,]");
    }

    #[test]
    fn query_batches_special_rules() {
        let (index_scheduler, mut handle) =
            IndexScheduler::test(true, vec![(3, FailureLocation::InsideProcessBatch)]);

        let kind = index_creation_task("catto", "mouse");
        let _task = index_scheduler.register(kind, None, false).unwrap();
        let kind = index_creation_task("doggo", "sheep");
        let _task = index_scheduler.register(kind, None, false).unwrap();
        let kind = KindWithContent::IndexSwap {
            swaps: vec![IndexSwap { indexes: ("catto".to_owned(), "doggo".to_owned()) }],
        };
        let _task = index_scheduler.register(kind, None, false).unwrap();
        let kind = KindWithContent::IndexSwap {
            swaps: vec![IndexSwap { indexes: ("catto".to_owned(), "whalo".to_owned()) }],
        };
        let _task = index_scheduler.register(kind, None, false).unwrap();

        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "start");

        handle.advance_till([Start, BatchCreated]);

        let rtxn = index_scheduler.env.read_txn().unwrap();
        let proc = index_scheduler.processing_tasks.read().unwrap().clone();

        let query = Query { index_uids: Some(vec!["catto".to_owned()]), ..Default::default() };
        let (batches, _) = index_scheduler
            .get_batch_ids_from_authorized_indexes(&rtxn, &proc, &query, &AuthFilter::default())
            .unwrap();
        // only the first task associated with catto is returned, the indexSwap tasks are excluded!
        snapshot!(snapshot_bitmap(&batches), @"[0,]");

        let query = Query { index_uids: Some(vec!["catto".to_owned()]), ..Default::default() };
        let (batches, _) = index_scheduler
            .get_batch_ids_from_authorized_indexes(
                &rtxn,
                &proc,
                &query,
                &AuthFilter::with_allowed_indexes(
                    vec![IndexUidPattern::new_unchecked("doggo")].into_iter().collect(),
                ),
            )
            .unwrap();
        // we have asked for only the tasks associated with catto, but are only authorized to retrieve the tasks
        // associated with doggo -> empty result
        snapshot!(snapshot_bitmap(&batches), @"[]");

        drop(rtxn);
        // We're going to advance and process all the batches for the next query to actually hit the db
        handle.advance_till([
            InsideProcessBatch,
            InsideProcessBatch,
            ProcessBatchSucceeded,
            AfterProcessing,
        ]);
        handle.advance_one_successful_batch();
        handle.advance_n_failed_batches(2);
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "after-processing-everything");
        let rtxn = index_scheduler.env.read_txn().unwrap();

        let query = Query::default();
        let (batches, _) = index_scheduler
            .get_batch_ids_from_authorized_indexes(
                &rtxn,
                &proc,
                &query,
                &AuthFilter::with_allowed_indexes(
                    vec![IndexUidPattern::new_unchecked("doggo")].into_iter().collect(),
                ),
            )
            .unwrap();
        // we asked for all the tasks, but we are only authorized to retrieve the doggo tasks
        // -> only the index creation of doggo should be returned
        snapshot!(snapshot_bitmap(&batches), @"[1,]");

        let query = Query::default();
        let (batches, _) = index_scheduler
            .get_batch_ids_from_authorized_indexes(
                &rtxn,
                &proc,
                &query,
                &AuthFilter::with_allowed_indexes(
                    vec![
                        IndexUidPattern::new_unchecked("catto"),
                        IndexUidPattern::new_unchecked("doggo"),
                    ]
                    .into_iter()
                    .collect(),
                ),
            )
            .unwrap();
        // we asked for all the tasks, but we are only authorized to retrieve the doggo and catto tasks
        // -> all tasks except the swap of catto with whalo are returned
        snapshot!(snapshot_bitmap(&batches), @"[0,1,]");

        let query = Query::default();
        let (batches, _) = index_scheduler
            .get_batch_ids_from_authorized_indexes(&rtxn, &proc, &query, &AuthFilter::default())
            .unwrap();
        // we asked for all the tasks with all index authorized -> all tasks returned
        snapshot!(snapshot_bitmap(&batches), @"[0,1,2,3,]");
    }

    #[test]
    fn query_batches_canceled_by() {
        let (index_scheduler, mut handle) =
            IndexScheduler::test(true, vec![(3, FailureLocation::InsideProcessBatch)]);

        let kind = index_creation_task("catto", "mouse");
        let _ = index_scheduler.register(kind, None, false).unwrap();
        let kind = index_creation_task("doggo", "sheep");
        let _ = index_scheduler.register(kind, None, false).unwrap();
        let kind = KindWithContent::IndexSwap {
            swaps: vec![IndexSwap { indexes: ("catto".to_owned(), "doggo".to_owned()) }],
        };
        let _task = index_scheduler.register(kind, None, false).unwrap();

        handle.advance_n_successful_batches(1);
        let kind = KindWithContent::TaskCancelation {
            query: "test_query".to_string(),
            tasks: [0, 1, 2, 3].into_iter().collect(),
        };
        let task_cancelation = index_scheduler.register(kind, None, false).unwrap();
        handle.advance_n_successful_batches(1);

        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "start");

        let rtxn = index_scheduler.read_txn().unwrap();
        let proc = index_scheduler.processing_tasks.read().unwrap().clone();
        let query = Query { canceled_by: Some(vec![task_cancelation.uid]), ..Query::default() };
        let (batches, _) = index_scheduler
            .get_batch_ids_from_authorized_indexes(&rtxn, &proc, &query, &AuthFilter::default())
            .unwrap();
        // The batch zero was the index creation task, the 1 is the task cancellation
        snapshot!(snapshot_bitmap(&batches), @"[1,]");

        let query = Query { canceled_by: Some(vec![task_cancelation.uid]), ..Query::default() };
        let (batches, _) = index_scheduler
            .get_batch_ids_from_authorized_indexes(
                &rtxn,
                &proc,
                &query,
                &AuthFilter::with_allowed_indexes(
                    vec![IndexUidPattern::new_unchecked("doggo")].into_iter().collect(),
                ),
            )
            .unwrap();
        // Return only 1 because the user is not authorized to see task 2
        snapshot!(snapshot_bitmap(&batches), @"[1,]");
    }

    #[test]
    fn fail_in_process_batch_for_index_creation() {
        let (index_scheduler, mut handle) =
            IndexScheduler::test(true, vec![(1, FailureLocation::InsideProcessBatch)]);

        let kind = index_creation_task("catto", "mouse");

        let _task = index_scheduler.register(kind, None, false).unwrap();
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "after_register");

        handle.advance_one_failed_batch();

        // Still in the first iteration
        assert_eq!(*index_scheduler.run_loop_iteration.read().unwrap(), 1);
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "index_creation_failed");
    }

    #[test]
    fn fail_in_process_batch_for_document_addition() {
        let (index_scheduler, mut handle) =
            IndexScheduler::test(true, vec![(1, FailureLocation::InsideProcessBatch)]);

        let content = r#"
        {
            "id": 1,
            "doggo": "bob"
        }"#;

        let (uuid, mut file) = index_scheduler.create_update_file_with_uuid(0).unwrap();
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
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "registered_the_first_task");
        handle.advance_till([Start, BatchCreated]);

        snapshot!(
            snapshot_index_scheduler(&index_scheduler),
            name: "document_addition_batch_created"
        );

        handle.advance_till([ProcessBatchFailed, AfterProcessing]);
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "document_addition_failed");
    }

    #[test]
    fn fail_in_update_task_after_process_batch_success_for_document_addition() {
        let (index_scheduler, mut handle) = IndexScheduler::test(
            true,
            vec![(1, FailureLocation::UpdatingTaskAfterProcessBatchSuccess { task_uid: 0 })],
        );

        let content = r#"
        {
            "id": 1,
            "doggo": "bob"
        }"#;

        let (uuid, mut file) = index_scheduler.create_update_file_with_uuid(0).unwrap();
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
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "registered_the_first_task");

        handle.advance_till([Start]);
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "document_addition_succeeded_but_index_scheduler_not_updated");

        handle.advance_till([BatchCreated, InsideProcessBatch, ProcessBatchSucceeded]);
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "after_batch_succeeded");

        // At this point the next time the scheduler will try to progress it should encounter
        // a critical failure and have to wait for 1s before retrying anything.

        let before_failure = Instant::now();
        handle.advance_till([Start]);
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "after_failing_to_commit");
        let failure_duration = before_failure.elapsed();
        assert!(failure_duration.as_millis() >= 1000);

        handle.advance_till([
            BatchCreated,
            InsideProcessBatch,
            ProcessBatchSucceeded,
            AfterProcessing,
        ]);
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "task_successfully_processed");
    }

    #[test]
    fn test_document_addition_cant_create_index_without_index() {
        // We're going to autobatch multiple document addition that don't have
        // the right to create an index while there is no index currently.
        // Thus, everything should be batched together and a IndexDoesNotExists
        // error should be throwed.
        let (index_scheduler, mut handle) = IndexScheduler::test(true, vec![]);

        for i in 0..10 {
            let content = format!(
                r#"{{
                    "id": {},
                    "doggo": "bob {}"
                }}"#,
                i, i
            );

            let (uuid, mut file) = index_scheduler.create_update_file_with_uuid(i).unwrap();
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
                        allow_index_creation: false,
                    },
                    None,
                    false,
                )
                .unwrap();
            index_scheduler.assert_internally_consistent();
        }
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "after_registering_the_10_tasks");

        // Everything should be batched together.
        handle.advance_till([
            Start,
            BatchCreated,
            InsideProcessBatch,
            ProcessBatchFailed,
            AfterProcessing,
        ]);
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "after_processing_the_10_tasks");

        // The index should not exist.
        snapshot!(matches!(index_scheduler.index_exists("doggos"), Ok(true)), @"false");
    }

    #[test]
    fn test_document_addition_cant_create_index_without_index_without_autobatching() {
        // We're going to execute multiple document addition that don't have
        // the right to create an index while there is no index currently.
        // Since the auto-batching is disabled, every task should be processed
        // sequentially and throw an IndexDoesNotExists.
        let (index_scheduler, mut handle) = IndexScheduler::test(false, vec![]);

        for i in 0..10 {
            let content = format!(
                r#"{{
                    "id": {},
                    "doggo": "bob {}"
                }}"#,
                i, i
            );

            let (uuid, mut file) = index_scheduler.create_update_file_with_uuid(i).unwrap();
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
                        allow_index_creation: false,
                    },
                    None,
                    false,
                )
                .unwrap();
            index_scheduler.assert_internally_consistent();
        }
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "after_registering_the_10_tasks");

        // Nothing should be batched thus half of the tasks are processed.
        handle.advance_n_failed_batches(5);
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "five_tasks_processed");

        // Everything is processed.
        handle.advance_n_failed_batches(5);
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "all_tasks_processed");

        // The index should not exist.
        snapshot!(matches!(index_scheduler.index_exists("doggos"), Ok(true)), @"false");
    }

    #[test]
    fn test_document_addition_cant_create_index_with_index() {
        // We're going to autobatch multiple document addition that don't have
        // the right to create an index while there is already an index.
        // Thus, everything should be batched together and no error should be
        // throwed.
        let (index_scheduler, mut handle) = IndexScheduler::test(true, vec![]);

        // Create the index.
        index_scheduler
            .register(
                KindWithContent::IndexCreation { index_uid: S("doggos"), primary_key: None },
                None,
                false,
            )
            .unwrap();
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "registered_the_first_task");
        handle.advance_one_successful_batch();
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "processed_the_first_task");

        for i in 0..10 {
            let content = format!(
                r#"{{
                    "id": {},
                    "doggo": "bob {}"
                }}"#,
                i, i
            );

            let (uuid, mut file) = index_scheduler.create_update_file_with_uuid(i).unwrap();
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
                        allow_index_creation: false,
                    },
                    None,
                    false,
                )
                .unwrap();
            index_scheduler.assert_internally_consistent();
        }
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "after_registering_the_10_tasks");

        // Everything should be batched together.
        handle.advance_n_successful_batches(1);
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "after_processing_the_10_tasks");

        // Has everything being pushed successfully in milli?
        let index = index_scheduler.index("doggos").unwrap();
        let rtxn = index.read_txn().unwrap();
        let mut buffer = Vec::new();
        let dictionary = index.document_decompression_dictionary(&rtxn).unwrap();
        let field_ids_map = index.fields_ids_map(&rtxn).unwrap();
        let field_ids = field_ids_map.ids().collect::<Vec<_>>();
        let documents = index
            .all_compressed_documents(&rtxn)
            .unwrap()
            .map(|ret| {
                let (_id, compressed_doc) = ret.unwrap();
                let doc = compressed_doc
                    .decompress_with_optional_dictionary(&mut buffer, dictionary.as_ref())
                    .unwrap();
                obkv_to_json(&field_ids, &field_ids_map, doc).unwrap()
            })
            .collect::<Vec<_>>();
        snapshot!(serde_json::to_string_pretty(&documents).unwrap(), name: "documents");
    }

    #[test]
    fn test_document_addition_cant_create_index_with_index_without_autobatching() {
        // We're going to execute multiple document addition that don't have
        // the right to create an index while there is no index currently.
        // Since the autobatching is disabled, every tasks should be processed
        // sequentially and throw an IndexDoesNotExists.
        let (index_scheduler, mut handle) = IndexScheduler::test(false, vec![]);

        // Create the index.
        index_scheduler
            .register(
                KindWithContent::IndexCreation { index_uid: S("doggos"), primary_key: None },
                None,
                false,
            )
            .unwrap();
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "registered_the_first_task");
        handle.advance_one_successful_batch();
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "processed_the_first_task");

        for i in 0..10 {
            let content = format!(
                r#"{{
                    "id": {},
                    "doggo": "bob {}"
                }}"#,
                i, i
            );

            let (uuid, mut file) = index_scheduler.create_update_file_with_uuid(i).unwrap();
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
                        allow_index_creation: false,
                    },
                    None,
                    false,
                )
                .unwrap();
            index_scheduler.assert_internally_consistent();
        }
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "after_registering_the_10_tasks");

        // Nothing should be batched thus half of the tasks are processed.
        handle.advance_n_successful_batches(5);
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "five_tasks_processed");

        // Everything is processed.
        handle.advance_n_successful_batches(5);
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "all_tasks_processed");

        // Has everything being pushed successfully in milli?
        let index = index_scheduler.index("doggos").unwrap();
        let rtxn = index.read_txn().unwrap();
        let mut buffer = Vec::new();
        let dictionary = index.document_decompression_dictionary(&rtxn).unwrap();
        let field_ids_map = index.fields_ids_map(&rtxn).unwrap();
        let field_ids = field_ids_map.ids().collect::<Vec<_>>();
        let documents = index
            .all_compressed_documents(&rtxn)
            .unwrap()
            .map(|ret| {
                let (_id, compressed_doc) = ret.unwrap();
                let doc = compressed_doc
                    .decompress_with_optional_dictionary(&mut buffer, dictionary.as_ref())
                    .unwrap();
                obkv_to_json(&field_ids, &field_ids_map, doc).unwrap()
            })
            .collect::<Vec<_>>();
        snapshot!(serde_json::to_string_pretty(&documents).unwrap(), name: "documents");
    }

    #[test]
    fn test_document_addition_mixed_rights_with_index() {
        // We're going to autobatch multiple document addition.
        // - The index already exists
        // - The first document addition don't have the right to create an index
        //   can it batch with the other one?
        let (index_scheduler, mut handle) = IndexScheduler::test(true, vec![]);

        // Create the index.
        index_scheduler
            .register(
                KindWithContent::IndexCreation { index_uid: S("doggos"), primary_key: None },
                None,
                false,
            )
            .unwrap();
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "registered_the_first_task");
        handle.advance_one_successful_batch();
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "processed_the_first_task");

        for i in 0..10 {
            let content = format!(
                r#"{{
                    "id": {},
                    "doggo": "bob {}"
                }}"#,
                i, i
            );
            let allow_index_creation = i % 2 != 0;

            let (uuid, mut file) = index_scheduler.create_update_file_with_uuid(i).unwrap();
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
                        allow_index_creation,
                    },
                    None,
                    false,
                )
                .unwrap();
            index_scheduler.assert_internally_consistent();
        }
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "after_registering_the_10_tasks");

        // Everything should be batched together.
        handle.advance_one_successful_batch();
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "all_tasks_processed");

        // Has everything being pushed successfully in milli?
        let index = index_scheduler.index("doggos").unwrap();
        let rtxn = index.read_txn().unwrap();
        let mut buffer = Vec::new();
        let dictionary = index.document_decompression_dictionary(&rtxn).unwrap();
        let field_ids_map = index.fields_ids_map(&rtxn).unwrap();
        let field_ids = field_ids_map.ids().collect::<Vec<_>>();
        let documents = index
            .all_compressed_documents(&rtxn)
            .unwrap()
            .map(|ret| {
                let (_id, compressed_doc) = ret.unwrap();
                let doc = compressed_doc
                    .decompress_with_optional_dictionary(&mut buffer, dictionary.as_ref())
                    .unwrap();
                obkv_to_json(&field_ids, &field_ids_map, doc).unwrap()
            })
            .collect::<Vec<_>>();
        snapshot!(serde_json::to_string_pretty(&documents).unwrap(), name: "documents");
    }

    #[test]
    fn test_document_addition_mixed_right_without_index_starts_with_cant_create() {
        // We're going to autobatch multiple document addition.
        // - The index does not exists
        // - The first document addition don't have the right to create an index
        // - The second do. They should not batch together.
        // - The second should batch with everything else as it's going to create an index.
        let (index_scheduler, mut handle) = IndexScheduler::test(true, vec![]);

        for i in 0..10 {
            let content = format!(
                r#"{{
                    "id": {},
                    "doggo": "bob {}"
                }}"#,
                i, i
            );
            let allow_index_creation = i % 2 != 0;

            let (uuid, mut file) = index_scheduler.create_update_file_with_uuid(i).unwrap();
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
                        allow_index_creation,
                    },
                    None,
                    false,
                )
                .unwrap();
            index_scheduler.assert_internally_consistent();
        }
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "after_registering_the_10_tasks");

        // A first batch should be processed with only the first documentAddition that's going to fail.
        handle.advance_one_failed_batch();
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "only_first_task_failed");

        // Everything else should be batched together.
        handle.advance_one_successful_batch();
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "all_tasks_processed");

        // Has everything being pushed successfully in milli?
        let index = index_scheduler.index("doggos").unwrap();
        let rtxn = index.read_txn().unwrap();
        let mut buffer = Vec::new();
        let dictionary = index.document_decompression_dictionary(&rtxn).unwrap();
        let field_ids_map = index.fields_ids_map(&rtxn).unwrap();
        let field_ids = field_ids_map.ids().collect::<Vec<_>>();
        let documents = index
            .all_compressed_documents(&rtxn)
            .unwrap()
            .map(|ret| {
                let (_id, compressed_doc) = ret.unwrap();
                let doc = compressed_doc
                    .decompress_with_optional_dictionary(&mut buffer, dictionary.as_ref())
                    .unwrap();
                obkv_to_json(&field_ids, &field_ids_map, doc).unwrap()
            })
            .collect::<Vec<_>>();
        snapshot!(serde_json::to_string_pretty(&documents).unwrap(), name: "documents");
    }

    #[test]
    fn test_document_addition_with_multiple_primary_key() {
        let (index_scheduler, mut handle) = IndexScheduler::test(true, vec![]);

        for (id, primary_key) in ["id", "bork", "bloup"].iter().enumerate() {
            let content = format!(
                r#"{{
                    "id": {id},
                    "doggo": "jean bob"
                }}"#,
            );
            let (uuid, mut file) =
                index_scheduler.create_update_file_with_uuid(id as u128).unwrap();
            let documents_count = read_json(content.as_bytes(), &mut file).unwrap();
            assert_eq!(documents_count, 1);
            file.persist().unwrap();

            index_scheduler
                .register(
                    KindWithContent::DocumentAdditionOrUpdate {
                        index_uid: S("doggos"),
                        primary_key: Some(S(primary_key)),
                        method: ReplaceDocuments,
                        content_file: uuid,
                        documents_count,
                        allow_index_creation: true,
                    },
                    None,
                    false,
                )
                .unwrap();
            index_scheduler.assert_internally_consistent();
        }

        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "after_registering_the_3_tasks");

        // A first batch should be processed with only the first documentAddition.
        handle.advance_one_successful_batch();
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "only_first_task_succeed");

        handle.advance_one_successful_batch();
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "second_task_fails");

        handle.advance_one_successful_batch();
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "third_task_fails");

        // Is the primary key still what we expect?
        let index = index_scheduler.index("doggos").unwrap();
        let rtxn = index.read_txn().unwrap();
        let mut buffer = Vec::new();
        let dictionary = index.document_decompression_dictionary(&rtxn).unwrap();
        let primary_key = index.primary_key(&rtxn).unwrap().unwrap();
        snapshot!(primary_key, @"id");

        // Is the document still the one we expect?.
        let field_ids_map = index.fields_ids_map(&rtxn).unwrap();
        let field_ids = field_ids_map.ids().collect::<Vec<_>>();
        let documents = index
            .all_compressed_documents(&rtxn)
            .unwrap()
            .map(|ret| {
                let (_id, compressed_doc) = ret.unwrap();
                let doc = compressed_doc
                    .decompress_with_optional_dictionary(&mut buffer, dictionary.as_ref())
                    .unwrap();
                obkv_to_json(&field_ids, &field_ids_map, doc).unwrap()
            })
            .collect::<Vec<_>>();
        snapshot!(serde_json::to_string_pretty(&documents).unwrap(), name: "documents");
    }

    #[test]
    fn test_document_addition_with_multiple_primary_key_batch_wrong_key() {
        let (index_scheduler, mut handle) = IndexScheduler::test(true, vec![]);

        for (id, primary_key) in ["id", "bork", "bork"].iter().enumerate() {
            let content = format!(
                r#"{{
                    "id": {id},
                    "doggo": "jean bob"
                }}"#,
            );
            let (uuid, mut file) =
                index_scheduler.create_update_file_with_uuid(id as u128).unwrap();
            let documents_count = read_json(content.as_bytes(), &mut file).unwrap();
            assert_eq!(documents_count, 1);
            file.persist().unwrap();

            index_scheduler
                .register(
                    KindWithContent::DocumentAdditionOrUpdate {
                        index_uid: S("doggos"),
                        primary_key: Some(S(primary_key)),
                        method: ReplaceDocuments,
                        content_file: uuid,
                        documents_count,
                        allow_index_creation: true,
                    },
                    None,
                    false,
                )
                .unwrap();
            index_scheduler.assert_internally_consistent();
        }

        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "after_registering_the_3_tasks");

        // A first batch should be processed with only the first documentAddition.
        handle.advance_one_successful_batch();
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "only_first_task_succeed");

        handle.advance_one_successful_batch();
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "second_and_third_tasks_fails");

        // Is the primary key still what we expect?
        let index = index_scheduler.index("doggos").unwrap();
        let rtxn = index.read_txn().unwrap();
        let mut buffer = Vec::new();
        let dictionary = index.document_decompression_dictionary(&rtxn).unwrap();
        let primary_key = index.primary_key(&rtxn).unwrap().unwrap();
        snapshot!(primary_key, @"id");

        // Is the document still the one we expect?.
        let field_ids_map = index.fields_ids_map(&rtxn).unwrap();
        let field_ids = field_ids_map.ids().collect::<Vec<_>>();
        let documents = index
            .all_compressed_documents(&rtxn)
            .unwrap()
            .map(|ret| {
                let (_id, compressed_doc) = ret.unwrap();
                let doc = compressed_doc
                    .decompress_with_optional_dictionary(&mut buffer, dictionary.as_ref())
                    .unwrap();
                obkv_to_json(&field_ids, &field_ids_map, doc).unwrap()
            })
            .collect::<Vec<_>>();
        snapshot!(serde_json::to_string_pretty(&documents).unwrap(), name: "documents");
    }

    #[test]
    fn test_document_addition_with_bad_primary_key() {
        let (index_scheduler, mut handle) = IndexScheduler::test(true, vec![]);

        for (id, primary_key) in ["bork", "bork", "id", "bork", "id"].iter().enumerate() {
            let content = format!(
                r#"{{
                    "id": {id},
                    "doggo": "jean bob"
                }}"#,
            );
            let (uuid, mut file) =
                index_scheduler.create_update_file_with_uuid(id as u128).unwrap();
            let documents_count = read_json(content.as_bytes(), &mut file).unwrap();
            assert_eq!(documents_count, 1);
            file.persist().unwrap();

            index_scheduler
                .register(
                    KindWithContent::DocumentAdditionOrUpdate {
                        index_uid: S("doggos"),
                        primary_key: Some(S(primary_key)),
                        method: ReplaceDocuments,
                        content_file: uuid,
                        documents_count,
                        allow_index_creation: true,
                    },
                    None,
                    false,
                )
                .unwrap();
            index_scheduler.assert_internally_consistent();
        }

        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "after_registering_the_5_tasks");

        // A first batch should be processed with only the first two documentAddition.
        // it should fails because the documents don't contains any `bork` field.
        // NOTE: it's marked as successful because the batch didn't fails, it's the individual tasks that failed.
        handle.advance_one_successful_batch();
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "first_and_second_task_fails");

        // The primary key should be set to none since we failed the batch.
        let index = index_scheduler.index("doggos").unwrap();
        let rtxn = index.read_txn().unwrap();
        let primary_key = index.primary_key(&rtxn).unwrap();
        snapshot!(primary_key.is_none(), @"true");

        // The second batch should succeed and only contains one task.
        handle.advance_one_successful_batch();
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "third_task_succeeds");

        // The primary key should be set to `id` since this batch succeeded.
        let index = index_scheduler.index("doggos").unwrap();
        let rtxn = index.read_txn().unwrap();
        let primary_key = index.primary_key(&rtxn).unwrap().unwrap();
        snapshot!(primary_key, @"id");

        // We're trying to `bork` again, but now there is already a primary key set for this index.
        // NOTE: it's marked as successful because the batch didn't fails, it's the individual tasks that failed.
        handle.advance_one_successful_batch();
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "fourth_task_fails");

        // Finally the last task should succeed since its primary key is the same as the valid one.
        handle.advance_one_successful_batch();
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "fifth_task_succeeds");

        // Is the primary key still what we expect?
        let index = index_scheduler.index("doggos").unwrap();
        let rtxn = index.read_txn().unwrap();
        let mut buffer = Vec::new();
        let dictionary = index.document_decompression_dictionary(&rtxn).unwrap();
        let primary_key = index.primary_key(&rtxn).unwrap().unwrap();
        snapshot!(primary_key, @"id");

        // Is the document still the one we expect?.
        let field_ids_map = index.fields_ids_map(&rtxn).unwrap();
        let field_ids = field_ids_map.ids().collect::<Vec<_>>();
        let documents = index
            .all_compressed_documents(&rtxn)
            .unwrap()
            .map(|ret| {
                let (_id, compressed_doc) = ret.unwrap();
                let doc = compressed_doc
                    .decompress_with_optional_dictionary(&mut buffer, dictionary.as_ref())
                    .unwrap();
                obkv_to_json(&field_ids, &field_ids_map, doc).unwrap()
            })
            .collect::<Vec<_>>();
        snapshot!(serde_json::to_string_pretty(&documents).unwrap(), name: "documents");
    }

    #[test]
    fn test_document_addition_with_set_and_null_primary_key() {
        let (index_scheduler, mut handle) = IndexScheduler::test(true, vec![]);

        for (id, primary_key) in
            [None, Some("bork"), Some("paw"), None, None, Some("paw")].into_iter().enumerate()
        {
            let content = format!(
                r#"{{
                    "paw": {id},
                    "doggo": "jean bob"
                }}"#,
            );
            let (uuid, mut file) =
                index_scheduler.create_update_file_with_uuid(id as u128).unwrap();
            let documents_count = read_json(content.as_bytes(), &mut file).unwrap();
            assert_eq!(documents_count, 1);
            file.persist().unwrap();

            index_scheduler
                .register(
                    KindWithContent::DocumentAdditionOrUpdate {
                        index_uid: S("doggos"),
                        primary_key: primary_key.map(|pk| pk.to_string()),
                        method: ReplaceDocuments,
                        content_file: uuid,
                        documents_count,
                        allow_index_creation: true,
                    },
                    None,
                    false,
                )
                .unwrap();
            index_scheduler.assert_internally_consistent();
        }

        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "after_registering_the_6_tasks");

        // A first batch should contains only one task that fails because we can't infer the primary key.
        // NOTE: it's marked as successful because the batch didn't fails, it's the individual tasks that failed.
        handle.advance_one_successful_batch();
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "first_task_fails");

        // The second batch should contains only one task that fails because we bork is not a valid primary key.
        // NOTE: it's marked as successful because the batch didn't fails, it's the individual tasks that failed.
        handle.advance_one_successful_batch();
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "second_task_fails");

        // No primary key should be set at this point.
        let index = index_scheduler.index("doggos").unwrap();
        let rtxn = index.read_txn().unwrap();
        let primary_key = index.primary_key(&rtxn).unwrap();
        snapshot!(primary_key.is_none(), @"true");

        // The third batch should succeed and only contains one task.
        handle.advance_one_successful_batch();
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "third_task_succeeds");

        // The primary key should be set to `id` since this batch succeeded.
        let index = index_scheduler.index("doggos").unwrap();
        let rtxn = index.read_txn().unwrap();
        let primary_key = index.primary_key(&rtxn).unwrap().unwrap();
        snapshot!(primary_key, @"paw");

        // We should be able to batch together the next two tasks that don't specify any primary key
        // + the last task that matches the current primary-key. Everything should succeed.
        handle.advance_one_successful_batch();
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "all_other_tasks_succeeds");

        // Is the primary key still what we expect?
        let index = index_scheduler.index("doggos").unwrap();
        let rtxn = index.read_txn().unwrap();
        let mut buffer = Vec::new();
        let dictionary = index.document_decompression_dictionary(&rtxn).unwrap();
        let primary_key = index.primary_key(&rtxn).unwrap().unwrap();
        snapshot!(primary_key, @"paw");

        // Is the document still the one we expect?.
        let field_ids_map = index.fields_ids_map(&rtxn).unwrap();
        let field_ids = field_ids_map.ids().collect::<Vec<_>>();
        let documents = index
            .all_compressed_documents(&rtxn)
            .unwrap()
            .map(|ret| {
                let (_id, compressed_doc) = ret.unwrap();
                let doc = compressed_doc
                    .decompress_with_optional_dictionary(&mut buffer, dictionary.as_ref())
                    .unwrap();
                obkv_to_json(&field_ids, &field_ids_map, doc).unwrap()
            })
            .collect::<Vec<_>>();
        snapshot!(serde_json::to_string_pretty(&documents).unwrap(), name: "documents");
    }

    #[test]
    fn test_document_addition_with_set_and_null_primary_key_inference_works() {
        let (index_scheduler, mut handle) = IndexScheduler::test(true, vec![]);

        for (id, primary_key) in [None, Some("bork"), Some("doggoid"), None, None, Some("doggoid")]
            .into_iter()
            .enumerate()
        {
            let content = format!(
                r#"{{
                    "doggoid": {id},
                    "doggo": "jean bob"
                }}"#,
            );
            let (uuid, mut file) =
                index_scheduler.create_update_file_with_uuid(id as u128).unwrap();
            let documents_count = read_json(content.as_bytes(), &mut file).unwrap();
            assert_eq!(documents_count, 1);
            file.persist().unwrap();

            index_scheduler
                .register(
                    KindWithContent::DocumentAdditionOrUpdate {
                        index_uid: S("doggos"),
                        primary_key: primary_key.map(|pk| pk.to_string()),
                        method: ReplaceDocuments,
                        content_file: uuid,
                        documents_count,
                        allow_index_creation: true,
                    },
                    None,
                    false,
                )
                .unwrap();
            index_scheduler.assert_internally_consistent();
        }

        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "after_registering_the_6_tasks");

        // A first batch should contains only one task that succeed and sets the primary key to `doggoid`.
        handle.advance_one_successful_batch();
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "first_task_succeed");

        // Checking the primary key.
        let index = index_scheduler.index("doggos").unwrap();
        let rtxn = index.read_txn().unwrap();
        let primary_key = index.primary_key(&rtxn).unwrap();
        snapshot!(primary_key.is_none(), @"false");

        // The second batch should contains only one task that fails because it tries to update the primary key to `bork`.
        handle.advance_one_successful_batch();
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "second_task_fails");

        // The third batch should succeed and only contains one task.
        handle.advance_one_successful_batch();
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "third_task_succeeds");

        // We should be able to batch together the next two tasks that don't specify any primary key
        // + the last task that matches the current primary-key. Everything should succeed.
        handle.advance_one_successful_batch();
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "all_other_tasks_succeeds");

        // Is the primary key still what we expect?
        let index = index_scheduler.index("doggos").unwrap();
        let rtxn = index.read_txn().unwrap();
        let mut buffer = Vec::new();
        let dictionary = index.document_decompression_dictionary(&rtxn).unwrap();
        let primary_key = index.primary_key(&rtxn).unwrap().unwrap();
        snapshot!(primary_key, @"doggoid");

        // Is the document still the one we expect?.
        let field_ids_map = index.fields_ids_map(&rtxn).unwrap();
        let field_ids = field_ids_map.ids().collect::<Vec<_>>();
        let documents = index
            .all_compressed_documents(&rtxn)
            .unwrap()
            .map(|ret| {
                let (_id, compressed_doc) = ret.unwrap();
                let doc = compressed_doc
                    .decompress_with_optional_dictionary(&mut buffer, dictionary.as_ref())
                    .unwrap();
                obkv_to_json(&field_ids, &field_ids_map, doc).unwrap()
            })
            .collect::<Vec<_>>();
        snapshot!(serde_json::to_string_pretty(&documents).unwrap(), name: "documents");
    }

    #[test]
    fn panic_in_process_batch_for_index_creation() {
        let (index_scheduler, mut handle) =
            IndexScheduler::test(true, vec![(1, FailureLocation::PanicInsideProcessBatch)]);

        let kind = index_creation_task("catto", "mouse");

        let _task = index_scheduler.register(kind, None, false).unwrap();
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "registered_the_first_task");

        handle.advance_till([Start, BatchCreated, ProcessBatchFailed, AfterProcessing]);

        // Still in the first iteration
        assert_eq!(*index_scheduler.run_loop_iteration.read().unwrap(), 1);
        // No matter what happens in process_batch, the index_scheduler should be internally consistent
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "index_creation_failed");
    }

    #[test]
    fn test_task_queue_is_full() {
        let (index_scheduler, mut handle) =
            IndexScheduler::test_with_custom_config(vec![], |config| {
                // that's the minimum map size possible
                config.task_db_size = 1048576;
            });

        index_scheduler
            .register(
                KindWithContent::IndexCreation { index_uid: S("doggo"), primary_key: None },
                None,
                false,
            )
            .unwrap();
        handle.advance_one_successful_batch();
        // on average this task takes ~600 bytes
        loop {
            let result = index_scheduler.register(
                KindWithContent::IndexCreation { index_uid: S("doggo"), primary_key: None },
                None,
                false,
            );
            if result.is_err() {
                break;
            }
            handle.advance_one_failed_batch();
        }
        index_scheduler.assert_internally_consistent();

        // at this point the task DB shoud have reached its limit and we should not be able to register new tasks
        let result = index_scheduler
            .register(
                KindWithContent::IndexCreation { index_uid: S("doggo"), primary_key: None },
                None,
                false,
            )
            .unwrap_err();
        snapshot!(result, @"Meilisearch cannot receive write operations because the limit of the task database has been reached. Please delete tasks to continue performing write operations.");
        // we won't be able to test this error in an integration test thus as a best effort test I still ensure the error return the expected error code
        snapshot!(format!("{:?}", result.error_code()), @"NoSpaceLeftOnDevice");

        // Even the task deletion that doesn't delete anything shouldn't be accepted
        let result = index_scheduler
            .register(
                KindWithContent::TaskDeletion { query: S("test"), tasks: RoaringBitmap::new() },
                None,
                false,
            )
            .unwrap_err();
        snapshot!(result, @"Meilisearch cannot receive write operations because the limit of the task database has been reached. Please delete tasks to continue performing write operations.");
        // we won't be able to test this error in an integration test thus as a best effort test I still ensure the error return the expected error code
        snapshot!(format!("{:?}", result.error_code()), @"NoSpaceLeftOnDevice");

        // But a task deletion that delete something should works
        index_scheduler
            .register(
                KindWithContent::TaskDeletion { query: S("test"), tasks: (0..100).collect() },
                None,
                false,
            )
            .unwrap();
        handle.advance_one_successful_batch();

        // Now we should be able to enqueue a few tasks again
        index_scheduler
            .register(
                KindWithContent::IndexCreation { index_uid: S("doggo"), primary_key: None },
                None,
                false,
            )
            .unwrap();
        handle.advance_one_failed_batch();
    }

    #[test]
    fn test_auto_deletion_of_tasks() {
        let (index_scheduler, mut handle) =
            IndexScheduler::test_with_custom_config(vec![], |config| {
                config.max_number_of_tasks = 2;
            });

        index_scheduler
            .register(
                KindWithContent::IndexCreation { index_uid: S("doggo"), primary_key: None },
                None,
                false,
            )
            .unwrap();
        handle.advance_one_successful_batch();

        index_scheduler
            .register(
                KindWithContent::IndexCreation { index_uid: S("doggo"), primary_key: None },
                None,
                false,
            )
            .unwrap();
        handle.advance_one_failed_batch();

        // at this point the max number of tasks is reached
        // we can still enqueue multiple tasks
        index_scheduler
            .register(
                KindWithContent::IndexCreation { index_uid: S("doggo"), primary_key: None },
                None,
                false,
            )
            .unwrap();
        index_scheduler
            .register(
                KindWithContent::IndexCreation { index_uid: S("doggo"), primary_key: None },
                None,
                false,
            )
            .unwrap();

        let rtxn = index_scheduler.env.read_txn().unwrap();
        let tasks = index_scheduler.get_task_ids(&rtxn, &Query { ..Default::default() }).unwrap();
        let tasks = index_scheduler.get_existing_tasks(&rtxn, tasks).unwrap();
        snapshot!(json_string!(tasks, { "[].enqueuedAt" => "[date]", "[].startedAt" => "[date]", "[].finishedAt" => "[date]" }), name: "task_queue_is_full");
        drop(rtxn);

        // now we're above the max number of tasks
        // and if we try to advance in the tick function a new task deletion should be enqueued
        handle.advance_till([Start, BatchCreated]);
        let rtxn = index_scheduler.env.read_txn().unwrap();
        let tasks = index_scheduler.get_task_ids(&rtxn, &Query { ..Default::default() }).unwrap();
        let tasks = index_scheduler.get_existing_tasks(&rtxn, tasks).unwrap();
        snapshot!(json_string!(tasks, { "[].enqueuedAt" => "[date]", "[].startedAt" => "[date]", "[].finishedAt" => "[date]", ".**.original_filter" => "[filter]", ".**.query" => "[query]" }), name: "task_deletion_have_been_enqueued");
        drop(rtxn);

        handle.advance_till([InsideProcessBatch, ProcessBatchSucceeded, AfterProcessing]);
        let rtxn = index_scheduler.env.read_txn().unwrap();
        let tasks = index_scheduler.get_task_ids(&rtxn, &Query { ..Default::default() }).unwrap();
        let tasks = index_scheduler.get_existing_tasks(&rtxn, tasks).unwrap();
        snapshot!(json_string!(tasks, { "[].enqueuedAt" => "[date]", "[].startedAt" => "[date]", "[].finishedAt" => "[date]", ".**.original_filter" => "[filter]", ".**.query" => "[query]" }), name: "task_deletion_have_been_processed");
        drop(rtxn);

        handle.advance_one_failed_batch();
        // a new task deletion has been enqueued
        handle.advance_one_successful_batch();
        let rtxn = index_scheduler.env.read_txn().unwrap();
        let tasks = index_scheduler.get_task_ids(&rtxn, &Query { ..Default::default() }).unwrap();
        let tasks = index_scheduler.get_existing_tasks(&rtxn, tasks).unwrap();
        snapshot!(json_string!(tasks, { "[].enqueuedAt" => "[date]", "[].startedAt" => "[date]", "[].finishedAt" => "[date]", ".**.original_filter" => "[filter]", ".**.query" => "[query]" }), name: "after_the_second_task_deletion");
        drop(rtxn);

        handle.advance_one_failed_batch();
        handle.advance_one_successful_batch();
        let rtxn = index_scheduler.env.read_txn().unwrap();
        let tasks = index_scheduler.get_task_ids(&rtxn, &Query { ..Default::default() }).unwrap();
        let tasks = index_scheduler.get_existing_tasks(&rtxn, tasks).unwrap();
        snapshot!(json_string!(tasks, { "[].enqueuedAt" => "[date]", "[].startedAt" => "[date]", "[].finishedAt" => "[date]", ".**.original_filter" => "[filter]", ".**.query" => "[query]" }), name: "everything_has_been_processed");
        drop(rtxn);
    }

    #[test]
    fn test_disable_auto_deletion_of_tasks() {
        let (index_scheduler, mut handle) =
            IndexScheduler::test_with_custom_config(vec![], |config| {
                config.cleanup_enabled = false;
                config.max_number_of_tasks = 2;
            });

        index_scheduler
            .register(
                KindWithContent::IndexCreation { index_uid: S("doggo"), primary_key: None },
                None,
                false,
            )
            .unwrap();
        handle.advance_one_successful_batch();

        index_scheduler
            .register(
                KindWithContent::IndexCreation { index_uid: S("doggo"), primary_key: None },
                None,
                false,
            )
            .unwrap();
        handle.advance_one_failed_batch();

        // at this point the max number of tasks is reached
        // we can still enqueue multiple tasks
        index_scheduler
            .register(
                KindWithContent::IndexCreation { index_uid: S("doggo"), primary_key: None },
                None,
                false,
            )
            .unwrap();
        index_scheduler
            .register(
                KindWithContent::IndexCreation { index_uid: S("doggo"), primary_key: None },
                None,
                false,
            )
            .unwrap();

        let rtxn = index_scheduler.env.read_txn().unwrap();
        let tasks = index_scheduler.get_task_ids(&rtxn, &Query { ..Default::default() }).unwrap();
        let tasks = index_scheduler.get_existing_tasks(&rtxn, tasks).unwrap();
        snapshot!(json_string!(tasks, { "[].enqueuedAt" => "[date]", "[].startedAt" => "[date]", "[].finishedAt" => "[date]" }), name: "task_queue_is_full");
        drop(rtxn);

        // now we're above the max number of tasks
        // and if we try to advance in the tick function no new task deletion should be enqueued
        handle.advance_till([Start, BatchCreated]);
        let rtxn = index_scheduler.env.read_txn().unwrap();
        let tasks = index_scheduler.get_task_ids(&rtxn, &Query { ..Default::default() }).unwrap();
        let tasks = index_scheduler.get_existing_tasks(&rtxn, tasks).unwrap();
        snapshot!(json_string!(tasks, { "[].enqueuedAt" => "[date]", "[].startedAt" => "[date]", "[].finishedAt" => "[date]", ".**.original_filter" => "[filter]", ".**.query" => "[query]" }), name: "task_deletion_have_not_been_enqueued");
        drop(rtxn);
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

        snapshot!(json_string!(index_scheduler.get_stats().unwrap()), @r###"
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
            "taskDeletion": 0
          }
        }
        "###);

        handle.advance_till([Start, BatchCreated]);
        snapshot!(json_string!(index_scheduler.get_stats().unwrap()), @r###"
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
            "taskDeletion": 0
          }
        }
        "###);

        handle.advance_till([
            InsideProcessBatch,
            InsideProcessBatch,
            ProcessBatchSucceeded,
            AfterProcessing,
            Start,
            BatchCreated,
        ]);
        snapshot!(json_string!(index_scheduler.get_stats().unwrap()), @r###"
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
            "taskDeletion": 0
          }
        }
        "###);

        // now we make one more batch, the started_at field of the new tasks will be past `second_start_time`
        handle.advance_till([
            InsideProcessBatch,
            InsideProcessBatch,
            ProcessBatchSucceeded,
            AfterProcessing,
            Start,
            BatchCreated,
        ]);
        snapshot!(json_string!(index_scheduler.get_stats().unwrap()), @r###"
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
            "taskDeletion": 0
          }
        }
        "###);
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

    #[test]
    fn basic_set_taskid() {
        let (index_scheduler, _handle) = IndexScheduler::test(true, vec![]);

        let kind = KindWithContent::IndexCreation { index_uid: S("doggo"), primary_key: None };
        let task = index_scheduler.register(kind, None, false).unwrap();
        snapshot!(task.uid, @"0");

        let kind = KindWithContent::IndexCreation { index_uid: S("doggo"), primary_key: None };
        let task = index_scheduler.register(kind, Some(12), false).unwrap();
        snapshot!(task.uid, @"12");

        let kind = KindWithContent::IndexCreation { index_uid: S("doggo"), primary_key: None };
        let error = index_scheduler.register(kind, Some(5), false).unwrap_err();
        snapshot!(error, @"Received bad task id: 5 should be >= to 13.");
    }

    #[test]
    fn dry_run() {
        let (index_scheduler, _handle) = IndexScheduler::test(true, vec![]);

        let kind = KindWithContent::IndexCreation { index_uid: S("doggo"), primary_key: None };
        let task = index_scheduler.register(kind, None, true).unwrap();
        snapshot!(task.uid, @"0");
        snapshot!(snapshot_index_scheduler(&index_scheduler), @r"
        ### Autobatching Enabled = true
        ### Processing batch None:
        []
        ----------------------------------------------------------------------
        ### All Tasks:
        ----------------------------------------------------------------------
        ### Status:
        ----------------------------------------------------------------------
        ### Kind:
        ----------------------------------------------------------------------
        ### Index Tasks:
        ----------------------------------------------------------------------
        ### Index Mapper:

        ----------------------------------------------------------------------
        ### Canceled By:

        ----------------------------------------------------------------------
        ### Enqueued At:
        ----------------------------------------------------------------------
        ### Started At:
        ----------------------------------------------------------------------
        ### Finished At:
        ----------------------------------------------------------------------
        ### All Batches:
        ----------------------------------------------------------------------
        ### Batch to tasks mapping:
        ----------------------------------------------------------------------
        ### Batches Status:
        ----------------------------------------------------------------------
        ### Batches Kind:
        ----------------------------------------------------------------------
        ### Batches Index Tasks:
        ----------------------------------------------------------------------
        ### Batches Enqueued At:
        ----------------------------------------------------------------------
        ### Batches Started At:
        ----------------------------------------------------------------------
        ### Batches Finished At:
        ----------------------------------------------------------------------
        ### File Store:

        ----------------------------------------------------------------------
        ");

        let kind = KindWithContent::IndexCreation { index_uid: S("doggo"), primary_key: None };
        let task = index_scheduler.register(kind, Some(12), true).unwrap();
        snapshot!(task.uid, @"12");
        snapshot!(snapshot_index_scheduler(&index_scheduler), @r"
        ### Autobatching Enabled = true
        ### Processing batch None:
        []
        ----------------------------------------------------------------------
        ### All Tasks:
        ----------------------------------------------------------------------
        ### Status:
        ----------------------------------------------------------------------
        ### Kind:
        ----------------------------------------------------------------------
        ### Index Tasks:
        ----------------------------------------------------------------------
        ### Index Mapper:

        ----------------------------------------------------------------------
        ### Canceled By:

        ----------------------------------------------------------------------
        ### Enqueued At:
        ----------------------------------------------------------------------
        ### Started At:
        ----------------------------------------------------------------------
        ### Finished At:
        ----------------------------------------------------------------------
        ### All Batches:
        ----------------------------------------------------------------------
        ### Batch to tasks mapping:
        ----------------------------------------------------------------------
        ### Batches Status:
        ----------------------------------------------------------------------
        ### Batches Kind:
        ----------------------------------------------------------------------
        ### Batches Index Tasks:
        ----------------------------------------------------------------------
        ### Batches Enqueued At:
        ----------------------------------------------------------------------
        ### Batches Started At:
        ----------------------------------------------------------------------
        ### Batches Finished At:
        ----------------------------------------------------------------------
        ### File Store:

        ----------------------------------------------------------------------
        ");
    }

    #[test]
    fn import_vectors() {
        use meilisearch_types::settings::{Settings, Unchecked};
        use milli::update::Setting;

        let (index_scheduler, mut handle) = IndexScheduler::test(true, vec![]);

        let mut new_settings: Box<Settings<Unchecked>> = Box::default();
        let mut embedders = BTreeMap::default();
        let embedding_settings = milli::vector::settings::EmbeddingSettings {
            source: Setting::Set(milli::vector::settings::EmbedderSource::Rest),
            api_key: Setting::Set(S("My super secret")),
            url: Setting::Set(S("http://localhost:7777")),
            dimensions: Setting::Set(384),
            request: Setting::Set(serde_json::json!("{{text}}")),
            response: Setting::Set(serde_json::json!("{{embedding}}")),
            ..Default::default()
        };
        embedders.insert(S("A_fakerest"), Setting::Set(embedding_settings));

        let embedding_settings = milli::vector::settings::EmbeddingSettings {
            source: Setting::Set(milli::vector::settings::EmbedderSource::HuggingFace),
            model: Setting::Set(S("sentence-transformers/all-MiniLM-L6-v2")),
            revision: Setting::Set(S("e4ce9877abf3edfe10b0d82785e83bdcb973e22e")),
            document_template: Setting::Set(S("{{doc.doggo}} the {{doc.breed}} best doggo")),
            ..Default::default()
        };
        embedders.insert(S("B_small_hf"), Setting::Set(embedding_settings));

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

        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "after_registering_settings_task_vectors");

        {
            let rtxn = index_scheduler.read_txn().unwrap();
            let task = index_scheduler.get_task(&rtxn, 0).unwrap().unwrap();
            let task = meilisearch_types::task_view::TaskView::from_task(&task);
            insta::assert_json_snapshot!(task.details);
        }

        handle.advance_one_successful_batch();
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "settings_update_processed_vectors");

        {
            let rtxn = index_scheduler.read_txn().unwrap();
            let task = index_scheduler.get_task(&rtxn, 0).unwrap().unwrap();
            let task = meilisearch_types::task_view::TaskView::from_task(&task);
            insta::assert_json_snapshot!(task.details);
        }

        let (fakerest_name, simple_hf_name, beagle_embed, lab_embed, patou_embed) = {
            let index = index_scheduler.index("doggos").unwrap();
            let rtxn = index.read_txn().unwrap();

            let configs = index.embedding_configs(&rtxn).unwrap();
            // for consistency with the below
            #[allow(clippy::get_first)]
            let IndexEmbeddingConfig { name, config: fakerest_config, user_provided } =
                configs.get(0).unwrap();
            insta::assert_snapshot!(name, @"A_fakerest");
            insta::assert_debug_snapshot!(user_provided, @"RoaringBitmap<[]>");
            insta::assert_json_snapshot!(fakerest_config.embedder_options);
            let fakerest_name = name.clone();

            let IndexEmbeddingConfig { name, config: simple_hf_config, user_provided } =
                configs.get(1).unwrap();
            insta::assert_snapshot!(name, @"B_small_hf");
            insta::assert_debug_snapshot!(user_provided, @"RoaringBitmap<[]>");
            insta::assert_json_snapshot!(simple_hf_config.embedder_options);
            let simple_hf_name = name.clone();

            let configs = index_scheduler.embedders("doggos".to_string(), configs).unwrap();
            let (hf_embedder, _, _) = configs.get(&simple_hf_name).unwrap();
            let beagle_embed =
                hf_embedder.embed_one(S("Intel the beagle best doggo"), None).unwrap();
            let lab_embed = hf_embedder.embed_one(S("Max the lab best doggo"), None).unwrap();
            let patou_embed = hf_embedder.embed_one(S("kefir the patou best doggo"), None).unwrap();
            (fakerest_name, simple_hf_name, beagle_embed, lab_embed, patou_embed)
        };

        // add one doc, specifying vectors

        let doc = serde_json::json!(
            {
                "id": 0,
                "doggo": "Intel",
                "breed": "beagle",
                "_vectors": {
                    &fakerest_name: {
                        // this will never trigger regeneration, which is good because we can't actually generate with
                        // this embedder
                        "regenerate": false,
                        "embeddings": beagle_embed,
                    },
                    &simple_hf_name: {
                        // this will be regenerated on updates
                        "regenerate": true,
                        "embeddings": lab_embed,
                    },
                    "noise": [0.1, 0.2, 0.3]
                }
            }
        );

        let (uuid, mut file) = index_scheduler.create_update_file_with_uuid(0u128).unwrap();
        let documents_count = read_json(doc.to_string().as_bytes(), &mut file).unwrap();
        assert_eq!(documents_count, 1);
        file.persist().unwrap();

        index_scheduler
            .register(
                KindWithContent::DocumentAdditionOrUpdate {
                    index_uid: S("doggos"),
                    primary_key: Some(S("id")),
                    method: UpdateDocuments,
                    content_file: uuid,
                    documents_count,
                    allow_index_creation: true,
                },
                None,
                false,
            )
            .unwrap();

        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "after adding Intel");

        handle.advance_one_successful_batch();

        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "adding Intel succeeds");

        // check embeddings
        {
            let index = index_scheduler.index("doggos").unwrap();
            let rtxn = index.read_txn().unwrap();
            let mut buffer = Vec::new();
            let dictionary = index.document_decompression_dictionary(&rtxn).unwrap();

            // Ensure the document have been inserted into the relevant bitamp
            let configs = index.embedding_configs(&rtxn).unwrap();
            // for consistency with the below
            #[allow(clippy::get_first)]
            let IndexEmbeddingConfig { name, config: _, user_provided: user_defined } =
                configs.get(0).unwrap();
            insta::assert_snapshot!(name, @"A_fakerest");
            insta::assert_debug_snapshot!(user_defined, @"RoaringBitmap<[0]>");

            let IndexEmbeddingConfig { name, config: _, user_provided } = configs.get(1).unwrap();
            insta::assert_snapshot!(name, @"B_small_hf");
            insta::assert_debug_snapshot!(user_provided, @"RoaringBitmap<[]>");

            let embeddings = index.embeddings(&rtxn, 0).unwrap();

            assert_json_snapshot!(embeddings[&simple_hf_name][0] == lab_embed, @"true");
            assert_json_snapshot!(embeddings[&fakerest_name][0] == beagle_embed, @"true");

            let (_id, compressed_doc) =
                index.compressed_documents(&rtxn, std::iter::once(0)).unwrap().remove(0);
            let fields_ids_map = index.fields_ids_map(&rtxn).unwrap();
            let doc = compressed_doc
                .decompress_with_optional_dictionary(&mut buffer, dictionary.as_ref())
                .unwrap();
            let doc = obkv_to_json(
                &[
                    fields_ids_map.id("doggo").unwrap(),
                    fields_ids_map.id("breed").unwrap(),
                    fields_ids_map.id("_vectors").unwrap(),
                ],
                &fields_ids_map,
                doc,
            )
            .unwrap();
            assert_json_snapshot!(doc, {"._vectors.A_fakerest.embeddings" => "[vector]"});
        }

        // update the doc, specifying vectors

        let doc = serde_json::json!(
                    {
                        "id": 0,
                        "doggo": "kefir",
                        "breed": "patou",
                    }
        );

        let (uuid, mut file) = index_scheduler.create_update_file_with_uuid(1u128).unwrap();
        let documents_count = read_json(doc.to_string().as_bytes(), &mut file).unwrap();
        assert_eq!(documents_count, 1);
        file.persist().unwrap();

        index_scheduler
            .register(
                KindWithContent::DocumentAdditionOrUpdate {
                    index_uid: S("doggos"),
                    primary_key: None,
                    method: UpdateDocuments,
                    content_file: uuid,
                    documents_count,
                    allow_index_creation: true,
                },
                None,
                false,
            )
            .unwrap();

        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "Intel to kefir");

        handle.advance_one_successful_batch();
        snapshot!(snapshot_index_scheduler(&index_scheduler), name: "Intel to kefir succeeds");

        {
            // check embeddings
            {
                let index = index_scheduler.index("doggos").unwrap();
                let rtxn = index.read_txn().unwrap();
                let mut buffer = Vec::new();
                let dictionary = index.document_decompression_dictionary(&rtxn).unwrap();

                // Ensure the document have been inserted into the relevant bitamp
                let configs = index.embedding_configs(&rtxn).unwrap();
                // for consistency with the below
                #[allow(clippy::get_first)]
                let IndexEmbeddingConfig { name, config: _, user_provided: user_defined } =
                    configs.get(0).unwrap();
                insta::assert_snapshot!(name, @"A_fakerest");
                insta::assert_debug_snapshot!(user_defined, @"RoaringBitmap<[0]>");

                let IndexEmbeddingConfig { name, config: _, user_provided } =
                    configs.get(1).unwrap();
                insta::assert_snapshot!(name, @"B_small_hf");
                insta::assert_debug_snapshot!(user_provided, @"RoaringBitmap<[]>");

                let embeddings = index.embeddings(&rtxn, 0).unwrap();

                // automatically changed to patou because set to regenerate
                assert_json_snapshot!(embeddings[&simple_hf_name][0] == patou_embed, @"true");
                // remained beagle
                assert_json_snapshot!(embeddings[&fakerest_name][0] == beagle_embed, @"true");

                let (_id, compressed_doc) =
                    index.compressed_documents(&rtxn, std::iter::once(0)).unwrap().remove(0);
                let fields_ids_map = index.fields_ids_map(&rtxn).unwrap();
                let doc = compressed_doc
                    .decompress_with_optional_dictionary(&mut buffer, dictionary.as_ref())
                    .unwrap();
                let doc = obkv_to_json(
                    &[
                        fields_ids_map.id("doggo").unwrap(),
                        fields_ids_map.id("breed").unwrap(),
                        fields_ids_map.id("_vectors").unwrap(),
                    ],
                    &fields_ids_map,
                    doc,
                )
                .unwrap();
                assert_json_snapshot!(doc, {"._vectors.A_fakerest.embeddings" => "[vector]"});
            }
        }
    }

    #[test]
    fn import_vectors_first_and_embedder_later() {
        let (index_scheduler, mut handle) = IndexScheduler::test(true, vec![]);

        let content = serde_json::json!(
            [
                {
                    "id": 0,
                    "doggo": "kefir",
                },
                {
                    "id": 1,
                    "doggo": "intel",
                    "_vectors": {
                        "my_doggo_embedder": vec![1; 384],
                        "unknown embedder": vec![1, 2, 3],
                    }
                },
                {
                    "id": 2,
                    "doggo": "max",
                    "_vectors": {
                        "my_doggo_embedder": {
                            "regenerate": false,
                            "embeddings": vec![2; 384],
                        },
                        "unknown embedder": vec![4, 5],
                    },
                },
                {
                    "id": 3,
                    "doggo": "marcel",
                    "_vectors": {
                        "my_doggo_embedder": {
                            "regenerate": true,
                            "embeddings": vec![3; 384],
                        },
                    },
                },
                {
                    "id": 4,
                    "doggo": "sora",
                    "_vectors": {
                        "my_doggo_embedder": {
                            "regenerate": true,
                        },
                    },
                },
            ]
        );

        let (uuid, mut file) = index_scheduler.create_update_file_with_uuid(0_u128).unwrap();
        let documents_count =
            read_json(serde_json::to_string_pretty(&content).unwrap().as_bytes(), &mut file)
                .unwrap();
        snapshot!(documents_count, @"5");
        file.persist().unwrap();

        index_scheduler
            .register(
                KindWithContent::DocumentAdditionOrUpdate {
                    index_uid: S("doggos"),
                    primary_key: None,
                    method: ReplaceDocuments,
                    content_file: uuid,
                    documents_count,
                    allow_index_creation: true,
                },
                None,
                false,
            )
            .unwrap();
        handle.advance_one_successful_batch();

        let index = index_scheduler.index("doggos").unwrap();
        let rtxn = index.read_txn().unwrap();
        let mut buffer = Vec::new();
        let dictionary = index.document_decompression_dictionary(&rtxn).unwrap();
        let field_ids_map = index.fields_ids_map(&rtxn).unwrap();
        let field_ids = field_ids_map.ids().collect::<Vec<_>>();
        let documents = index
            .all_compressed_documents(&rtxn)
            .unwrap()
            .map(|ret| {
                let (_id, compressed_doc) = ret.unwrap();
                let doc = compressed_doc
                    .decompress_with_optional_dictionary(&mut buffer, dictionary.as_ref())
                    .unwrap();
                obkv_to_json(&field_ids, &field_ids_map, doc).unwrap()
            })
            .collect::<Vec<_>>();
        snapshot!(serde_json::to_string(&documents).unwrap(), name: "documents after initial push");

        let setting = meilisearch_types::settings::Settings::<Unchecked> {
            embedders: Setting::Set(maplit::btreemap! {
                S("my_doggo_embedder") => Setting::Set(EmbeddingSettings {
                    source: Setting::Set(milli::vector::settings::EmbedderSource::HuggingFace),
                    model: Setting::Set(S("sentence-transformers/all-MiniLM-L6-v2")),
                    revision: Setting::Set(S("e4ce9877abf3edfe10b0d82785e83bdcb973e22e")),
                    document_template: Setting::Set(S("{{doc.doggo}}")),
                    ..Default::default()
                })
            }),
            ..Default::default()
        };
        index_scheduler
            .register(
                KindWithContent::SettingsUpdate {
                    index_uid: S("doggos"),
                    new_settings: Box::new(setting),
                    is_deletion: false,
                    allow_index_creation: false,
                },
                None,
                false,
            )
            .unwrap();
        index_scheduler.assert_internally_consistent();
        handle.advance_one_successful_batch();
        index_scheduler.assert_internally_consistent();

        let index = index_scheduler.index("doggos").unwrap();
        let rtxn = index.read_txn().unwrap();
        let mut buffer = Vec::new();
        let dictionary = index.document_decompression_dictionary(&rtxn).unwrap();
        let field_ids_map = index.fields_ids_map(&rtxn).unwrap();
        let field_ids = field_ids_map.ids().collect::<Vec<_>>();
        let documents = index
            .all_compressed_documents(&rtxn)
            .unwrap()
            .map(|ret| {
                let (_id, compressed_doc) = ret.unwrap();
                let doc = compressed_doc
                    .decompress_with_optional_dictionary(&mut buffer, dictionary.as_ref())
                    .unwrap();
                obkv_to_json(&field_ids, &field_ids_map, doc).unwrap()
            })
            .collect::<Vec<_>>();
        // the all the vectors linked to the new specified embedder have been removed
        // Only the unknown embedders stays in the document DB
        snapshot!(serde_json::to_string(&documents).unwrap(), @r###"[{"id":0,"doggo":"kefir"},{"id":1,"doggo":"intel","_vectors":{"unknown embedder":[1.0,2.0,3.0]}},{"id":2,"doggo":"max","_vectors":{"unknown embedder":[4.0,5.0]}},{"id":3,"doggo":"marcel"},{"id":4,"doggo":"sora"}]"###);
        let conf = index.embedding_configs(&rtxn).unwrap();
        // even though we specified the vector for the ID 3, it shouldn't be marked
        // as user provided since we explicitely marked it as NOT user provided.
        snapshot!(format!("{conf:#?}"), @r###"
        [
            IndexEmbeddingConfig {
                name: "my_doggo_embedder",
                config: EmbeddingConfig {
                    embedder_options: HuggingFace(
                        EmbedderOptions {
                            model: "sentence-transformers/all-MiniLM-L6-v2",
                            revision: Some(
                                "e4ce9877abf3edfe10b0d82785e83bdcb973e22e",
                            ),
                            distribution: None,
                        },
                    ),
                    prompt: PromptData {
                        template: "{{doc.doggo}}",
                        max_bytes: Some(
                            400,
                        ),
                    },
                    quantized: None,
                },
                user_provided: RoaringBitmap<[1, 2]>,
            },
        ]
        "###);
        let docid = index.external_documents_ids.get(&rtxn, "0").unwrap().unwrap();
        let embeddings = index.embeddings(&rtxn, docid).unwrap();
        let embedding = &embeddings["my_doggo_embedder"];
        assert!(!embedding.is_empty(), "{embedding:?}");

        // the document with the id 3 should keep its original embedding
        let docid = index.external_documents_ids.get(&rtxn, "3").unwrap().unwrap();
        let embeddings = index.embeddings(&rtxn, docid).unwrap();
        let embeddings = &embeddings["my_doggo_embedder"];

        snapshot!(embeddings.len(), @"1");
        assert!(embeddings[0].iter().all(|i| *i == 3.0), "{:?}", embeddings[0]);

        // If we update marcel it should regenerate its embedding automatically

        let content = serde_json::json!(
            [
                {
                    "id": 3,
                    "doggo": "marvel",
                },
                {
                    "id": 4,
                    "doggo": "sorry",
                },
            ]
        );

        let (uuid, mut file) = index_scheduler.create_update_file_with_uuid(1_u128).unwrap();
        let documents_count =
            read_json(serde_json::to_string_pretty(&content).unwrap().as_bytes(), &mut file)
                .unwrap();
        snapshot!(documents_count, @"2");
        file.persist().unwrap();

        index_scheduler
            .register(
                KindWithContent::DocumentAdditionOrUpdate {
                    index_uid: S("doggos"),
                    primary_key: None,
                    method: UpdateDocuments,
                    content_file: uuid,
                    documents_count,
                    allow_index_creation: true,
                },
                None,
                false,
            )
            .unwrap();
        handle.advance_one_successful_batch();

        // the document with the id 3 should have its original embedding updated
        let rtxn = index.read_txn().unwrap();
        let mut buffer = Vec::new();
        let dictionary = index.document_decompression_dictionary(&rtxn).unwrap();
        let docid = index.external_documents_ids.get(&rtxn, "3").unwrap().unwrap();
        let (_id, compressed_doc) =
            index.compressed_documents(&rtxn, Some(docid)).unwrap().remove(0);
        let doc = compressed_doc
            .decompress_with_optional_dictionary(&mut buffer, dictionary.as_ref())
            .unwrap();
        let doc = obkv_to_json(&field_ids, &field_ids_map, doc).unwrap();
        snapshot!(json_string!(doc), @r###"
        {
          "id": 3,
          "doggo": "marvel"
        }
        "###);

        let embeddings = index.embeddings(&rtxn, docid).unwrap();
        let embedding = &embeddings["my_doggo_embedder"];

        assert!(!embedding.is_empty());
        assert!(!embedding[0].iter().all(|i| *i == 3.0), "{:?}", embedding[0]);

        // the document with the id 4 should generate an embedding
        let docid = index.external_documents_ids.get(&rtxn, "4").unwrap().unwrap();
        let embeddings = index.embeddings(&rtxn, docid).unwrap();
        let embedding = &embeddings["my_doggo_embedder"];

        assert!(!embedding.is_empty());
    }

    #[test]
    fn delete_document_containing_vector() {
        // 1. Add an embedder
        // 2. Push two documents containing a simple vector
        // 3. Delete the first document
        // 4. The user defined roaring bitmap shouldn't contains the id of the first document anymore
        // 5. Clear the index
        // 6. The user defined roaring bitmap shouldn't contains the id of the second document
        let (index_scheduler, mut handle) = IndexScheduler::test(true, vec![]);

        let setting = meilisearch_types::settings::Settings::<Unchecked> {
            embedders: Setting::Set(maplit::btreemap! {
                S("manual") => Setting::Set(EmbeddingSettings {
                    source: Setting::Set(milli::vector::settings::EmbedderSource::UserProvided),
                    dimensions: Setting::Set(3),
                    ..Default::default()
                })
            }),
            ..Default::default()
        };
        index_scheduler
            .register(
                KindWithContent::SettingsUpdate {
                    index_uid: S("doggos"),
                    new_settings: Box::new(setting),
                    is_deletion: false,
                    allow_index_creation: true,
                },
                None,
                false,
            )
            .unwrap();
        handle.advance_one_successful_batch();

        let content = serde_json::json!(
            [
                {
                    "id": 0,
                    "doggo": "kefir",
                    "_vectors": {
                        "manual": vec![0, 0, 0],
                    }
                },
                {
                    "id": 1,
                    "doggo": "intel",
                    "_vectors": {
                        "manual": vec![1, 1, 1],
                    }
                },
            ]
        );

        let (uuid, mut file) = index_scheduler.create_update_file_with_uuid(0_u128).unwrap();
        let documents_count =
            read_json(serde_json::to_string_pretty(&content).unwrap().as_bytes(), &mut file)
                .unwrap();
        snapshot!(documents_count, @"2");
        file.persist().unwrap();

        index_scheduler
            .register(
                KindWithContent::DocumentAdditionOrUpdate {
                    index_uid: S("doggos"),
                    primary_key: None,
                    method: ReplaceDocuments,
                    content_file: uuid,
                    documents_count,
                    allow_index_creation: false,
                },
                None,
                false,
            )
            .unwrap();
        handle.advance_one_successful_batch();

        index_scheduler
            .register(
                KindWithContent::DocumentDeletion {
                    index_uid: S("doggos"),
                    documents_ids: vec![S("1")],
                },
                None,
                false,
            )
            .unwrap();
        handle.advance_one_successful_batch();

        let index = index_scheduler.index("doggos").unwrap();
        let rtxn = index.read_txn().unwrap();
        let mut buffer = Vec::new();
        let dictionary = index.document_decompression_dictionary(&rtxn).unwrap();
        let field_ids_map = index.fields_ids_map(&rtxn).unwrap();
        let field_ids = field_ids_map.ids().collect::<Vec<_>>();
        let documents = index
            .all_compressed_documents(&rtxn)
            .unwrap()
            .map(|ret| {
                let (_id, compressed_doc) = ret.unwrap();
                let doc = compressed_doc
                    .decompress_with_optional_dictionary(&mut buffer, dictionary.as_ref())
                    .unwrap();
                obkv_to_json(&field_ids, &field_ids_map, doc).unwrap()
            })
            .collect::<Vec<_>>();
        snapshot!(serde_json::to_string(&documents).unwrap(), @r###"[{"id":0,"doggo":"kefir"}]"###);
        let conf = index.embedding_configs(&rtxn).unwrap();
        snapshot!(format!("{conf:#?}"), @r###"
        [
            IndexEmbeddingConfig {
                name: "manual",
                config: EmbeddingConfig {
                    embedder_options: UserProvided(
                        EmbedderOptions {
                            dimensions: 3,
                            distribution: None,
                        },
                    ),
                    prompt: PromptData {
                        template: "{% for field in fields %}{% if field.is_searchable and field.value != nil %}{{ field.name }}: {{ field.value }}\n{% endif %}{% endfor %}",
                        max_bytes: Some(
                            400,
                        ),
                    },
                    quantized: None,
                },
                user_provided: RoaringBitmap<[0]>,
            },
        ]
        "###);
        let docid = index.external_documents_ids.get(&rtxn, "0").unwrap().unwrap();
        let embeddings = index.embeddings(&rtxn, docid).unwrap();
        let embedding = &embeddings["manual"];
        assert!(!embedding.is_empty(), "{embedding:?}");

        index_scheduler
            .register(KindWithContent::DocumentClear { index_uid: S("doggos") }, None, false)
            .unwrap();
        handle.advance_one_successful_batch();

        let index = index_scheduler.index("doggos").unwrap();
        let rtxn = index.read_txn().unwrap();
        let mut buffer = Vec::new();
        let dictionary = index.document_decompression_dictionary(&rtxn).unwrap();
        let field_ids_map = index.fields_ids_map(&rtxn).unwrap();
        let field_ids = field_ids_map.ids().collect::<Vec<_>>();
        let documents = index
            .all_compressed_documents(&rtxn)
            .unwrap()
            .map(|ret| {
                let (_id, compressed_doc) = ret.unwrap();
                let doc = compressed_doc
                    .decompress_with_optional_dictionary(&mut buffer, dictionary.as_ref())
                    .unwrap();
                obkv_to_json(&field_ids, &field_ids_map, doc).unwrap()
            })
            .collect::<Vec<_>>();
        snapshot!(serde_json::to_string(&documents).unwrap(), @"[]");
        let conf = index.embedding_configs(&rtxn).unwrap();
        snapshot!(format!("{conf:#?}"), @r###"
        [
            IndexEmbeddingConfig {
                name: "manual",
                config: EmbeddingConfig {
                    embedder_options: UserProvided(
                        EmbedderOptions {
                            dimensions: 3,
                            distribution: None,
                        },
                    ),
                    prompt: PromptData {
                        template: "{% for field in fields %}{% if field.is_searchable and field.value != nil %}{{ field.name }}: {{ field.value }}\n{% endif %}{% endfor %}",
                        max_bytes: Some(
                            400,
                        ),
                    },
                    quantized: None,
                },
                user_provided: RoaringBitmap<[]>,
            },
        ]
        "###);
    }

    #[test]
    fn delete_embedder_with_user_provided_vectors() {
        // 1. Add two embedders
        // 2. Push two documents containing a simple vector
        // 3. The documents must not contain the vectors after the update as they are in the vectors db
        // 3. Delete the embedders
        // 4. The documents contain the vectors again
        let (index_scheduler, mut handle) = IndexScheduler::test(true, vec![]);

        let setting = meilisearch_types::settings::Settings::<Unchecked> {
            embedders: Setting::Set(maplit::btreemap! {
                S("manual") => Setting::Set(EmbeddingSettings {
                    source: Setting::Set(milli::vector::settings::EmbedderSource::UserProvided),
                    dimensions: Setting::Set(3),
                    ..Default::default()
                }),
                S("my_doggo_embedder") => Setting::Set(EmbeddingSettings {
                    source: Setting::Set(milli::vector::settings::EmbedderSource::HuggingFace),
                    model: Setting::Set(S("sentence-transformers/all-MiniLM-L6-v2")),
                    revision: Setting::Set(S("e4ce9877abf3edfe10b0d82785e83bdcb973e22e")),
                    document_template: Setting::Set(S("{{doc.doggo}}")),
                    ..Default::default()
                }),
            }),
            ..Default::default()
        };
        index_scheduler
            .register(
                KindWithContent::SettingsUpdate {
                    index_uid: S("doggos"),
                    new_settings: Box::new(setting),
                    is_deletion: false,
                    allow_index_creation: true,
                },
                None,
                false,
            )
            .unwrap();
        handle.advance_one_successful_batch();

        let content = serde_json::json!(
            [
                {
                    "id": 0,
                    "doggo": "kefir",
                    "_vectors": {
                        "manual": vec![0, 0, 0],
                        "my_doggo_embedder": vec![1; 384],
                    }
                },
                {
                    "id": 1,
                    "doggo": "intel",
                    "_vectors": {
                        "manual": vec![1, 1, 1],
                    }
                },
            ]
        );

        let (uuid, mut file) = index_scheduler.create_update_file_with_uuid(0_u128).unwrap();
        let documents_count =
            read_json(serde_json::to_string_pretty(&content).unwrap().as_bytes(), &mut file)
                .unwrap();
        snapshot!(documents_count, @"2");
        file.persist().unwrap();

        index_scheduler
            .register(
                KindWithContent::DocumentAdditionOrUpdate {
                    index_uid: S("doggos"),
                    primary_key: None,
                    method: ReplaceDocuments,
                    content_file: uuid,
                    documents_count,
                    allow_index_creation: false,
                },
                None,
                false,
            )
            .unwrap();
        handle.advance_one_successful_batch();

        {
            let index = index_scheduler.index("doggos").unwrap();
            let rtxn = index.read_txn().unwrap();
            let mut buffer = Vec::new();
            let dictionary = index.document_decompression_dictionary(&rtxn).unwrap();
            let field_ids_map = index.fields_ids_map(&rtxn).unwrap();
            let field_ids = field_ids_map.ids().collect::<Vec<_>>();
            let documents = index
                .all_compressed_documents(&rtxn)
                .unwrap()
                .map(|ret| {
                    let (_id, compressed_doc) = ret.unwrap();
                    let doc = compressed_doc
                        .decompress_with_optional_dictionary(&mut buffer, dictionary.as_ref())
                        .unwrap();
                    obkv_to_json(&field_ids, &field_ids_map, doc).unwrap()
                })
                .collect::<Vec<_>>();
            snapshot!(serde_json::to_string(&documents).unwrap(), @r###"[{"id":0,"doggo":"kefir"},{"id":1,"doggo":"intel"}]"###);
        }

        {
            let setting = meilisearch_types::settings::Settings::<Unchecked> {
                embedders: Setting::Set(maplit::btreemap! {
                    S("manual") => Setting::Reset,
                }),
                ..Default::default()
            };
            index_scheduler
                .register(
                    KindWithContent::SettingsUpdate {
                        index_uid: S("doggos"),
                        new_settings: Box::new(setting),
                        is_deletion: false,
                        allow_index_creation: true,
                    },
                    None,
                    false,
                )
                .unwrap();
            handle.advance_one_successful_batch();
        }

        {
            let index = index_scheduler.index("doggos").unwrap();
            let rtxn = index.read_txn().unwrap();
            let mut buffer = Vec::new();
            let dictionary = index.document_decompression_dictionary(&rtxn).unwrap();
            let field_ids_map = index.fields_ids_map(&rtxn).unwrap();
            let field_ids = field_ids_map.ids().collect::<Vec<_>>();
            let documents = index
                .all_compressed_documents(&rtxn)
                .unwrap()
                .map(|ret| {
                    let (_id, compressed_doc) = ret.unwrap();
                    let doc = compressed_doc
                        .decompress_with_optional_dictionary(&mut buffer, dictionary.as_ref())
                        .unwrap();
                    obkv_to_json(&field_ids, &field_ids_map, doc).unwrap()
                })
                .collect::<Vec<_>>();
            snapshot!(serde_json::to_string(&documents).unwrap(), @r###"[{"id":0,"doggo":"kefir","_vectors":{"manual":{"embeddings":[[0.0,0.0,0.0]],"regenerate":false}}},{"id":1,"doggo":"intel","_vectors":{"manual":{"embeddings":[[1.0,1.0,1.0]],"regenerate":false}}}]"###);
        }

        {
            let setting = meilisearch_types::settings::Settings::<Unchecked> {
                embedders: Setting::Reset,
                ..Default::default()
            };
            index_scheduler
                .register(
                    KindWithContent::SettingsUpdate {
                        index_uid: S("doggos"),
                        new_settings: Box::new(setting),
                        is_deletion: false,
                        allow_index_creation: true,
                    },
                    None,
                    false,
                )
                .unwrap();
            handle.advance_one_successful_batch();
        }

        {
            let index = index_scheduler.index("doggos").unwrap();
            let rtxn = index.read_txn().unwrap();
            let mut buffer = Vec::new();
            let dictionary = index.document_decompression_dictionary(&rtxn).unwrap();
            let field_ids_map = index.fields_ids_map(&rtxn).unwrap();
            let field_ids = field_ids_map.ids().collect::<Vec<_>>();
            let documents = index
                .all_compressed_documents(&rtxn)
                .unwrap()
                .map(|ret| {
                    let (_id, compressed_doc) = ret.unwrap();
                    let doc = compressed_doc
                        .decompress_with_optional_dictionary(&mut buffer, dictionary.as_ref())
                        .unwrap();
                    obkv_to_json(&field_ids, &field_ids_map, doc).unwrap()
                })
                .collect::<Vec<_>>();

            // FIXME: redaction
            snapshot!(json_string!(serde_json::to_string(&documents).unwrap(), { "[]._vectors.doggo_embedder.embeddings" => "[vector]" }),  @r###""[{\"id\":0,\"doggo\":\"kefir\",\"_vectors\":{\"manual\":{\"embeddings\":[[0.0,0.0,0.0]],\"regenerate\":false},\"my_doggo_embedder\":{\"embeddings\":[[1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0]],\"regenerate\":false}}},{\"id\":1,\"doggo\":\"intel\",\"_vectors\":{\"manual\":{\"embeddings\":[[1.0,1.0,1.0]],\"regenerate\":false}}}]""###);
        }
    }
}
