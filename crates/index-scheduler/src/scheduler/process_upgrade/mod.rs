use meilisearch_types::{
    milli,
    milli::progress::{Progress, VariableNameStep},
    tasks::{KindWithContent, Status, Task},
    versioning::{VERSION_MAJOR, VERSION_MINOR, VERSION_PATCH},
};

use crate::{processing::UpgradeDatabaseProgress, Error, IndexScheduler, Result};

impl IndexScheduler {
    pub(super) fn process_upgrade(
        &self,
        progress: Progress,
        mut tasks: Vec<Task>,
    ) -> Result<Vec<Task>> {
        progress.update_progress(UpgradeDatabaseProgress::EnsuringCorrectnessOfTheSwap);

        // Since we should not have multiple upgrade tasks, we're only going to process the latest one:
        let KindWithContent::UpgradeDatabase { from } = tasks.last().unwrap().kind else {
            unreachable!()
        };

        enum UpgradeIndex {}
        let indexes = self.index_names()?;

        for (i, uid) in indexes.iter().enumerate() {
            progress.update_progress(VariableNameStep::<UpgradeIndex>::new(
                format!("Upgrading index `{uid}`"),
                i as u32,
                indexes.len() as u32,
            ));
            let index = self.index(uid)?;
            milli::update::upgrade::upgrade(&index, from, progress.clone());
        }

        for task in tasks.iter_mut() {
            task.status = Status::Succeeded;
        }

        Ok(tasks)
    }
}
