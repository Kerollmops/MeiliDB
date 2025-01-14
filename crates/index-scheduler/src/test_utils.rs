use std::io::{BufWriter, Write};
use std::sync::Arc;

use file_store::File;
use meilisearch_types::document_formats::DocumentFormatError;
use meilisearch_types::milli::update::IndexDocumentsMethod::ReplaceDocuments;
use uuid::Uuid;

use crate::insta_snapshot::snapshot_index_scheduler;
use crate::{Error, IndexScheduler, IndexSchedulerOptions};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Breakpoint {
    // this state is only encountered while creating the scheduler in the test suite.
    Init,

    Start,
    BatchCreated,
    AfterProcessing,
    AbortedIndexation,
    ProcessBatchSucceeded,
    ProcessBatchFailed,
    InsideProcessBatch,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FailureLocation {
    InsideCreateBatch,
    InsideProcessBatch,
    PanicInsideProcessBatch,
    AcquiringWtxn,
    UpdatingTaskAfterProcessBatchSuccess { task_uid: u32 },
    UpdatingTaskAfterProcessBatchFailure,
    CommittingWtxn,
}

use big_s::S;
use crossbeam_channel::RecvTimeoutError;
use meilisearch_types::milli::update::IndexerConfig;
use meilisearch_types::tasks::KindWithContent;
use meilisearch_types::VERSION_FILE_NAME;
use tempfile::{NamedTempFile, TempDir};
use Breakpoint::*;

impl IndexScheduler {
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
    pub(crate) fn breakpoint(&self, b: Breakpoint) {
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

impl IndexScheduler {
    pub(crate) fn test(
        autobatching_enabled: bool,
        planned_failures: Vec<(usize, FailureLocation)>,
    ) -> (Self, IndexSchedulerHandle) {
        Self::test_with_custom_config(planned_failures, |config| {
            config.autobatching_enabled = autobatching_enabled;
        })
    }

    pub(crate) fn test_with_custom_config(
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
            indexer_config: Arc::new(indexer_config),
            autobatching_enabled: true,
            cleanup_enabled: true,
            max_number_of_tasks: 1_000_000,
            max_number_of_batched_tasks: usize::MAX,
            batched_tasks_size_limit: u64::MAX,
            instance_features: Default::default(),
            auto_upgrade: true, // Don't cost much and will ensure the happy path works
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
    pub(crate) fn maybe_fail(&self, location: FailureLocation) -> crate::Result<()> {
        if self.planned_failures.contains(&(*self.run_loop_iteration.read().unwrap(), location)) {
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
pub(crate) fn index_creation_task(
    index: &'static str,
    primary_key: &'static str,
) -> KindWithContent {
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
pub(crate) fn replace_document_import_task(
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
pub(crate) fn read_json(
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
pub(crate) fn sample_documents(
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

    let (_uuid, mut file) = index_scheduler.queue.create_update_file_with_uuid(file_uuid).unwrap();
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
    pub(crate) fn advance(&mut self) -> Breakpoint {
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
    pub(crate) fn advance_till(&mut self, breakpoints: impl IntoIterator<Item = Breakpoint>) {
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
    pub(crate) fn advance_n_successful_batches(&mut self, n: usize) {
        for _ in 0..n {
            self.advance_one_successful_batch();
        }
    }

    /// Wait for `n` failed batches.
    #[track_caller]
    pub(crate) fn advance_n_failed_batches(&mut self, n: usize) {
        for _ in 0..n {
            self.advance_one_failed_batch();
        }
    }

    // Wait for one successful batch.
    #[track_caller]
    pub(crate) fn advance_one_successful_batch(&mut self) {
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
    pub(crate) fn advance_one_failed_batch(&mut self) {
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
