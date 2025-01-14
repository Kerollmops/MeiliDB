use std::ffi::OsStr;
use std::fs;
use std::sync::atomic::Ordering;

use meilisearch_types::heed::CompactionOption;
use meilisearch_types::milli::progress::{Progress, VariableNameStep};
use meilisearch_types::milli::{self};
use meilisearch_types::tasks::{Status, Task};
use meilisearch_types::{compression, VERSION_FILE_NAME};

use crate::processing::{AtomicUpdateFileStep, SnapshotCreationProgress};
use crate::{Error, IndexScheduler, Result};

impl IndexScheduler {
    pub(super) fn process_snapshot(
        &self,
        progress: Progress,
        mut tasks: Vec<Task>,
    ) -> Result<Vec<Task>> {
        progress.update_progress(SnapshotCreationProgress::StartTheSnapshotCreation);

        fs::create_dir_all(&self.scheduler.snapshots_path)?;
        let temp_snapshot_dir = tempfile::tempdir()?;

        // 1. Snapshot the version file.
        let dst = temp_snapshot_dir.path().join(VERSION_FILE_NAME);
        fs::copy(&self.scheduler.version_file_path, dst)?;

        // 2. Snapshot the index-scheduler LMDB env
        //
        // When we call copy_to_file, LMDB opens a read transaction by itself,
        // we can't provide our own. It is an issue as we would like to know
        // the update files to copy but new ones can be enqueued between the copy
        // of the env and the new transaction we open to retrieve the enqueued tasks.
        // So we prefer opening a new transaction after copying the env and copy more
        // update files than not enough.
        //
        // Note that there cannot be any update files deleted between those
        // two read operations as the task processing is synchronous.

        // 2.1 First copy the LMDB env of the index-scheduler
        progress.update_progress(SnapshotCreationProgress::SnapshotTheIndexScheduler);
        let dst = temp_snapshot_dir.path().join("tasks");
        fs::create_dir_all(&dst)?;
        self.env.copy_to_file(dst.join("data.mdb"), CompactionOption::Enabled)?;

        // 2.2 Create a read transaction on the index-scheduler
        let rtxn = self.env.read_txn()?;

        // 2.3 Create the update files directory
        let update_files_dir = temp_snapshot_dir.path().join("update_files");
        fs::create_dir_all(&update_files_dir)?;

        // 2.4 Only copy the update files of the enqueued tasks
        progress.update_progress(SnapshotCreationProgress::SnapshotTheUpdateFiles);
        let enqueued = self.queue.tasks.get_status(&rtxn, Status::Enqueued)?;
        let (atomic, update_file_progress) = AtomicUpdateFileStep::new(enqueued.len() as u32);
        progress.update_progress(update_file_progress);
        for task_id in enqueued {
            let task =
                self.queue.tasks.get_task(&rtxn, task_id)?.ok_or(Error::CorruptedTaskQueue)?;
            if let Some(content_uuid) = task.content_uuid() {
                let src = self.queue.file_store.get_update_path(content_uuid);
                let dst = update_files_dir.join(content_uuid.to_string());
                fs::copy(src, dst)?;
            }
            atomic.fetch_add(1, Ordering::Relaxed);
        }

        // 3. Snapshot every indexes
        progress.update_progress(SnapshotCreationProgress::SnapshotTheIndexes);
        let index_mapping = self.index_mapper.index_mapping;
        let nb_indexes = index_mapping.len(&rtxn)? as u32;

        for (i, result) in index_mapping.iter(&rtxn)?.enumerate() {
            let (name, uuid) = result?;
            progress.update_progress(VariableNameStep::<SnapshotCreationProgress>::new(
                name, i as u32, nb_indexes,
            ));
            let index = self.index_mapper.index(&rtxn, name)?;
            let dst = temp_snapshot_dir.path().join("indexes").join(uuid.to_string());
            fs::create_dir_all(&dst)?;
            index
                .copy_to_file(dst.join("data.mdb"), CompactionOption::Enabled)
                .map_err(|e| Error::from_milli(e, Some(name.to_string())))?;
        }

        drop(rtxn);

        // 4. Snapshot the auth LMDB env
        progress.update_progress(SnapshotCreationProgress::SnapshotTheApiKeys);
        let dst = temp_snapshot_dir.path().join("auth");
        fs::create_dir_all(&dst)?;
        // TODO We can't use the open_auth_store_env function here but we should
        let auth = unsafe {
            milli::heed::EnvOpenOptions::new()
                .map_size(1024 * 1024 * 1024) // 1 GiB
                .max_dbs(2)
                .open(&self.scheduler.auth_path)
        }?;
        auth.copy_to_file(dst.join("data.mdb"), CompactionOption::Enabled)?;

        // 5. Copy and tarball the flat snapshot
        progress.update_progress(SnapshotCreationProgress::CreateTheTarball);
        // 5.1 Find the original name of the database
        // TODO find a better way to get this path
        let mut base_path = self.env.path().to_owned();
        base_path.pop();
        let db_name = base_path.file_name().and_then(OsStr::to_str).unwrap_or("data.ms");

        // 5.2 Tarball the content of the snapshot in a tempfile with a .snapshot extension
        let snapshot_path = self.scheduler.snapshots_path.join(format!("{}.snapshot", db_name));
        let temp_snapshot_file = tempfile::NamedTempFile::new_in(&self.scheduler.snapshots_path)?;
        compression::to_tar_gz(temp_snapshot_dir.path(), temp_snapshot_file.path())?;
        let file = temp_snapshot_file.persist(snapshot_path)?;

        // 5.3 Change the permission to make the snapshot readonly
        let mut permissions = file.metadata()?.permissions();
        permissions.set_readonly(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            #[allow(clippy::non_octal_unix_permissions)]
            //                     rwxrwxrwx
            permissions.set_mode(0b100100100);
        }

        file.set_permissions(permissions)?;

        for task in &mut tasks {
            task.status = Status::Succeeded;
        }

        Ok(tasks)
    }
}
