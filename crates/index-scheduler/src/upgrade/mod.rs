use std::path::Path;

use meilisearch_types::{
    heed,
    tasks::{KindWithContent, Status, Task},
};
use time::OffsetDateTime;
use tracing::info;

use crate::queue::TaskQueue;

pub fn upgrade_task_queue(tasks_path: &Path, version: (u32, u32, u32)) -> anyhow::Result<()> {
    info!("Upgrading the task queue");
    let env = unsafe {
        heed::EnvOpenOptions::new()
            .max_dbs(19)
            // Since that's the only database memory-mapped currently we don't need to check the budget yet
            .map_size(100 * 1024 * 1024)
            .open(tasks_path)
    }?;
    let mut wtxn = env.write_txn()?;
    let queue = TaskQueue::new(&env, &mut wtxn)?;
    let uid = queue.next_task_id(&wtxn)?;
    queue.register(
        &mut wtxn,
        &Task {
            uid,
            batch_uid: None,
            enqueued_at: OffsetDateTime::now_utc(),
            started_at: None,
            finished_at: None,
            error: None,
            canceled_by: None,
            details: None,
            status: Status::Enqueued,
            kind: KindWithContent::UpgradeDatabase { from: version },
        },
    )?;
    wtxn.commit()?;
    // Should be pretty much instantaneous since we're the only one reading this env
    env.prepare_for_closing().wait();
    Ok(())
}
