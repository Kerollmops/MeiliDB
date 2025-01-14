/*!
The autobatcher is responsible for combining the next enqueued
tasks affecting a single index into a [batch](crate::batch::Batch).

The main function of the autobatcher is [`next_autobatch`].
*/

use std::ops::ControlFlow::{self, Break, Continue};

use meilisearch_types::milli::update::IndexDocumentsMethod::{
    self, ReplaceDocuments, UpdateDocuments,
};
use meilisearch_types::tasks::TaskId;

use crate::KindWithContent;

/// Succinctly describes a task's [`Kind`](meilisearch_types::tasks::Kind)
/// for the purpose of simplifying the implementation of the autobatcher.
///
/// Only the non-prioritised tasks that can be grouped in a batch have a corresponding [`AutobatchKind`]
enum AutobatchKind {
    DocumentImport {
        method: IndexDocumentsMethod,
        allow_index_creation: bool,
        primary_key: Option<String>,
    },
    DocumentEdition,
    DocumentDeletion {
        by_filter: bool,
    },
    DocumentClear,
    Settings {
        allow_index_creation: bool,
    },
    IndexCreation,
    IndexDeletion,
    IndexUpdate,
    IndexSwap,
}

impl AutobatchKind {
    #[rustfmt::skip]
    fn allow_index_creation(&self) -> Option<bool> {
        match self {
            AutobatchKind::DocumentImport { allow_index_creation, .. }
            | AutobatchKind::Settings { allow_index_creation, .. } => Some(*allow_index_creation),
            _ => None,
        }
    }

    fn primary_key(&self) -> Option<Option<&str>> {
        match self {
            AutobatchKind::DocumentImport { primary_key, .. } => Some(primary_key.as_deref()),
            _ => None,
        }
    }
}

impl From<KindWithContent> for AutobatchKind {
    fn from(kind: KindWithContent) -> Self {
        match kind {
            KindWithContent::DocumentAdditionOrUpdate {
                method,
                allow_index_creation,
                primary_key,
                ..
            } => AutobatchKind::DocumentImport { method, allow_index_creation, primary_key },
            KindWithContent::DocumentEdition { .. } => AutobatchKind::DocumentEdition,
            KindWithContent::DocumentDeletion { .. } => {
                AutobatchKind::DocumentDeletion { by_filter: false }
            }
            KindWithContent::DocumentClear { .. } => AutobatchKind::DocumentClear,
            KindWithContent::DocumentDeletionByFilter { .. } => {
                AutobatchKind::DocumentDeletion { by_filter: true }
            }
            KindWithContent::SettingsUpdate { allow_index_creation, is_deletion, .. } => {
                AutobatchKind::Settings {
                    allow_index_creation: allow_index_creation && !is_deletion,
                }
            }
            KindWithContent::IndexDeletion { .. } => AutobatchKind::IndexDeletion,
            KindWithContent::IndexCreation { .. } => AutobatchKind::IndexCreation,
            KindWithContent::IndexUpdate { .. } => AutobatchKind::IndexUpdate,
            KindWithContent::IndexSwap { .. } => AutobatchKind::IndexSwap,
            KindWithContent::TaskCancelation { .. }
            | KindWithContent::TaskDeletion { .. }
            | KindWithContent::DumpCreation { .. }
            | KindWithContent::UpgradeDatabase { .. }
            | KindWithContent::SnapshotCreation => {
                panic!("The autobatcher should never be called with tasks that don't apply to an index.")
            }
        }
    }
}

#[derive(Debug)]
pub enum BatchKind {
    DocumentClear {
        ids: Vec<TaskId>,
    },
    DocumentOperation {
        method: IndexDocumentsMethod,
        allow_index_creation: bool,
        primary_key: Option<String>,
        operation_ids: Vec<TaskId>,
    },
    DocumentEdition {
        id: TaskId,
    },
    DocumentDeletion {
        deletion_ids: Vec<TaskId>,
        includes_by_filter: bool,
    },
    ClearAndSettings {
        other: Vec<TaskId>,
        allow_index_creation: bool,
        settings_ids: Vec<TaskId>,
    },
    Settings {
        allow_index_creation: bool,
        settings_ids: Vec<TaskId>,
    },
    IndexDeletion {
        ids: Vec<TaskId>,
    },
    IndexCreation {
        id: TaskId,
    },
    IndexUpdate {
        id: TaskId,
    },
    IndexSwap {
        id: TaskId,
    },
}

impl BatchKind {
    #[rustfmt::skip]
    fn allow_index_creation(&self) -> Option<bool> {
        match self {
            BatchKind::DocumentOperation { allow_index_creation, .. }
            | BatchKind::ClearAndSettings { allow_index_creation, .. }
            | BatchKind::Settings { allow_index_creation, .. } => Some(*allow_index_creation),
            _ => None,
        }
    }

    fn primary_key(&self) -> Option<Option<&str>> {
        match self {
            BatchKind::DocumentOperation { primary_key, .. } => Some(primary_key.as_deref()),
            _ => None,
        }
    }
}

impl BatchKind {
    /// Returns a `ControlFlow::Break` if you must stop right now.
    /// The boolean tell you if an index has been created by the batched task.
    /// To ease the writing of the code. `true` can be returned when you don't need to create an index
    /// but false can't be returned if you needs to create an index.
    // TODO use an AutoBatchKind as input
    pub fn new(
        task_id: TaskId,
        kind: KindWithContent,
        primary_key: Option<&str>,
    ) -> (ControlFlow<BatchKind, BatchKind>, bool) {
        use AutobatchKind as K;

        match AutobatchKind::from(kind) {
            K::IndexCreation => (Break(BatchKind::IndexCreation { id: task_id }), true),
            K::IndexDeletion => (Break(BatchKind::IndexDeletion { ids: vec![task_id] }), false),
            K::IndexUpdate => (Break(BatchKind::IndexUpdate { id: task_id }), false),
            K::IndexSwap => (Break(BatchKind::IndexSwap { id: task_id }), false),
            K::DocumentClear => (Continue(BatchKind::DocumentClear { ids: vec![task_id] }), false),
            K::DocumentImport { method, allow_index_creation, primary_key: pk }
                if primary_key.is_none() || pk.is_none() || primary_key == pk.as_deref() =>
            {
                (
                    Continue(BatchKind::DocumentOperation {
                        method,
                        allow_index_creation,
                        primary_key: pk,
                        operation_ids: vec![task_id],
                    }),
                    allow_index_creation,
                )
            }
            // if the primary key set in the task was different than ours we should stop and make this batch fail asap.
            K::DocumentImport { method, allow_index_creation, primary_key } => (
                Break(BatchKind::DocumentOperation {
                    method,
                    allow_index_creation,
                    primary_key,
                    operation_ids: vec![task_id],
                }),
                allow_index_creation,
            ),
            K::DocumentEdition => (Break(BatchKind::DocumentEdition { id: task_id }), false),
            K::DocumentDeletion { by_filter: includes_by_filter } => (
                Continue(BatchKind::DocumentDeletion {
                    deletion_ids: vec![task_id],
                    includes_by_filter,
                }),
                false,
            ),
            K::Settings { allow_index_creation } => (
                Continue(BatchKind::Settings { allow_index_creation, settings_ids: vec![task_id] }),
                allow_index_creation,
            ),
        }
    }

    /// Returns a `ControlFlow::Break` if you must stop right now.
    /// The boolean tell you if an index has been created by the batched task.
    /// To ease the writing of the code. `true` can be returned when you don't need to create an index
    /// but false can't be returned if you needs to create an index.
    #[rustfmt::skip]
    fn accumulate(self, id: TaskId, kind: AutobatchKind, index_already_exists: bool, primary_key: Option<&str>) -> ControlFlow<BatchKind, BatchKind> {
        use AutobatchKind as K;

        match (self, kind) {
            // We don't batch any of these operations
            (this, K::IndexCreation | K::IndexUpdate | K::IndexSwap | K::DocumentEdition) => Break(this),
            // We must not batch tasks that don't have the same index creation rights if the index doesn't already exists.
            (this, kind) if !index_already_exists && this.allow_index_creation() == Some(false) && kind.allow_index_creation() == Some(true) => {
                Break(this)
            },
            // NOTE: We need to negate the whole condition since we're checking if we need to break instead of continue.
            //       I wrote it this way because it's easier to understand than the other way around.
            (this, kind) if !(
                // 1. If both task don't interact with primary key -> we can continue
                (this.primary_key().is_none() && kind.primary_key().is_none()) ||
                // 2. Else ->
                (
                    // 2.1 If we already have a primary-key ->
                    (
                        primary_key.is_some() &&
                        // 2.1.1 If the task we're trying to accumulate have a pk it must be equal to our primary key
                        // 2.1.2 If the task don't have a primary-key -> we can continue
                        kind.primary_key().map_or(true, |pk| pk == primary_key)
                    ) ||
                    // 2.2 If we don't have a primary-key ->
                    (
                        // 2.2.1 If both the batch and the task have a primary key they should be equal
                        // 2.2.2 If the batch is set to Some(None), the task should be too
                        // 2.2.3 If the batch is set to None -> we can continue
                        this.primary_key().zip(kind.primary_key()).map_or(true, |(this, kind)| this == kind)
                    )
                )

                ) // closing the negation

            => {
                Break(this)
            },
            // The index deletion can batch with everything but must stop after
            (
                BatchKind::DocumentClear { mut ids }
                | BatchKind::DocumentDeletion { deletion_ids: mut ids, includes_by_filter: _ }
                | BatchKind::DocumentOperation { method: _, allow_index_creation: _, primary_key: _, operation_ids: mut ids }
                | BatchKind::Settings { allow_index_creation: _, settings_ids: mut ids },
                K::IndexDeletion,
            ) => {
                ids.push(id);
                Break(BatchKind::IndexDeletion { ids })
            }
            (
                BatchKind::ClearAndSettings { settings_ids: mut ids, allow_index_creation: _, mut other },
                K::IndexDeletion,
            ) => {
                ids.push(id);
                ids.append(&mut other);
                Break(BatchKind::IndexDeletion { ids })
            }

            (
                BatchKind::DocumentClear { mut ids },
                K::DocumentClear | K::DocumentDeletion { by_filter: _ },
            ) => {
                ids.push(id);
                Continue(BatchKind::DocumentClear { ids })
            }
            (
                this @ BatchKind::DocumentClear { .. },
                K::DocumentImport { .. } | K::Settings { .. },
            ) => Break(this),
            (
                BatchKind::DocumentOperation { method: _, allow_index_creation: _, primary_key: _, mut operation_ids },
                K::DocumentClear,
            ) => {
                operation_ids.push(id);
                Continue(BatchKind::DocumentClear { ids: operation_ids })
            }

            // we can autobatch the same kind of document additions / updates
            (
                BatchKind::DocumentOperation { method: ReplaceDocuments, allow_index_creation, primary_key: _, mut operation_ids },
                K::DocumentImport { method: ReplaceDocuments, primary_key: pk, .. },
            ) => {
                operation_ids.push(id);
                Continue(BatchKind::DocumentOperation {
                    method: ReplaceDocuments,
                    allow_index_creation,
                    operation_ids,
                    primary_key: pk,
                })
            }
            (
                BatchKind::DocumentOperation { method: UpdateDocuments, allow_index_creation, primary_key: _, mut operation_ids },
                K::DocumentImport { method: UpdateDocuments, primary_key: pk, .. },
            ) => {
                operation_ids.push(id);
                Continue(BatchKind::DocumentOperation {
                    method: UpdateDocuments,
                    allow_index_creation,
                    primary_key: pk,
                    operation_ids,
                })
            }
            (
                BatchKind::DocumentOperation { method, allow_index_creation, primary_key, mut operation_ids },
                K::DocumentDeletion { by_filter: false },
            ) => {
                operation_ids.push(id);

                Continue(BatchKind::DocumentOperation {
                    method,
                    allow_index_creation,
                    primary_key,
                    operation_ids,
                })
            }
            // We can't batch a document operation with a delete by filter
            (
                this @ BatchKind::DocumentOperation { .. },
                K::DocumentDeletion { by_filter: true },
            ) => {
                Break(this)
            }
            // but we can't autobatch documents if it's not the same kind
            // this match branch MUST be AFTER the previous one
            (
                this @ BatchKind::DocumentOperation { .. },
                K::DocumentImport { .. },
            ) => Break(this),

            (
                this @ BatchKind::DocumentOperation { .. },
                K::Settings { .. },
            ) => Break(this),

            (BatchKind::DocumentDeletion { mut deletion_ids, includes_by_filter: _ }, K::DocumentClear) => {
                deletion_ids.push(id);
                Continue(BatchKind::DocumentClear { ids: deletion_ids })
            }
            // we can't autobatch the deletion and import if the document deletion contained a filter
            (
                this @ BatchKind::DocumentDeletion { deletion_ids: _, includes_by_filter: true },
                K::DocumentImport { .. }
            ) => Break(this),
            // we can autobatch the deletion and import if the index already exists
            (
                BatchKind::DocumentDeletion { mut deletion_ids, includes_by_filter: false },
                K::DocumentImport { method, allow_index_creation, primary_key }
            ) if index_already_exists => {
                deletion_ids.push(id);

                Continue(BatchKind::DocumentOperation {
                    method,
                    allow_index_creation,
                    primary_key,
                    operation_ids: deletion_ids,
                })
            }
            // we can autobatch the deletion and import if both can't create an index
            (
                BatchKind::DocumentDeletion { mut deletion_ids, includes_by_filter: false },
                K::DocumentImport { method, allow_index_creation, primary_key }
            ) if !allow_index_creation => {
                deletion_ids.push(id);

                Continue(BatchKind::DocumentOperation {
                    method,
                    allow_index_creation,
                    primary_key,
                    operation_ids: deletion_ids,
                })
            }
            // we can't autobatch a deletion and an import if the index does not exists but would be created by an addition
            (
                this @ BatchKind::DocumentDeletion { .. },
                K::DocumentImport { .. }
            ) => {
                Break(this)
            }
            (BatchKind::DocumentDeletion { mut deletion_ids, includes_by_filter }, K::DocumentDeletion { by_filter }) => {
                deletion_ids.push(id);
                Continue(BatchKind::DocumentDeletion { deletion_ids, includes_by_filter: includes_by_filter | by_filter })
            }
            (this @ BatchKind::DocumentDeletion { .. }, K::Settings { .. }) => Break(this),

            (
                BatchKind::Settings { settings_ids, allow_index_creation },
                K::DocumentClear,
            ) => Continue(BatchKind::ClearAndSettings {
                settings_ids,
                allow_index_creation,
                other: vec![id],
            }),
            (
                this @ BatchKind::Settings { .. },
                K::DocumentImport { .. } | K::DocumentDeletion { .. },
            ) => Break(this),
            (
                BatchKind::Settings { mut settings_ids, allow_index_creation },
                K::Settings { .. },
            ) => {
                settings_ids.push(id);
                Continue(BatchKind::Settings {
                    allow_index_creation,
                    settings_ids,
                })
            }

            (
                BatchKind::ClearAndSettings { mut other, settings_ids, allow_index_creation },
                K::DocumentClear,
            ) => {
                other.push(id);
                Continue(BatchKind::ClearAndSettings {
                    other,
                    settings_ids,
                    allow_index_creation,
                })
            }
            (this @ BatchKind::ClearAndSettings { .. }, K::DocumentImport { .. }) => Break(this),
            (
                BatchKind::ClearAndSettings {
                    mut other,
                    settings_ids,
                    allow_index_creation,
                },
                K::DocumentDeletion { .. },
            ) => {
                other.push(id);
                Continue(BatchKind::ClearAndSettings {
                    other,
                    settings_ids,
                    allow_index_creation,
                })
            }
            (
                BatchKind::ClearAndSettings { mut settings_ids, other, allow_index_creation },
                K::Settings { .. },
            ) => {
                settings_ids.push(id);
                Continue(BatchKind::ClearAndSettings {
                    other,
                    settings_ids,
                    allow_index_creation,
                })
            }

            (
                BatchKind::IndexCreation { .. }
                | BatchKind::IndexDeletion { .. }
                | BatchKind::IndexUpdate { .. }
                | BatchKind::IndexSwap { .. }
                | BatchKind::DocumentEdition { .. },
                _,
            ) => {
                unreachable!()
            }
        }
    }
}

/// Create a batch from an ordered list of tasks.
///
/// ## Preconditions
/// 1. The tasks must be enqueued and given in the order in which they were enqueued
/// 2. The tasks must not be prioritised tasks (e.g. task cancellation, dump, snapshot, task deletion)
/// 3. The tasks must all be related to the same index
///
/// ## Return
/// `None` if the list of tasks is empty. Otherwise, an [`AutoBatch`] that represents
/// a subset of the given tasks.
pub fn autobatch(
    enqueued: Vec<(TaskId, KindWithContent)>,
    index_already_exists: bool,
    primary_key: Option<&str>,
) -> Option<(BatchKind, bool)> {
    let mut enqueued = enqueued.into_iter();
    let (id, kind) = enqueued.next()?;

    // index_exist will keep track of if the index should exist at this point after the tasks we batched.
    let mut index_exist = index_already_exists;

    let (mut acc, must_create_index) = match BatchKind::new(id, kind, primary_key) {
        (Continue(acc), create) => (acc, create),
        (Break(acc), create) => return Some((acc, create)),
    };

    // if an index has been created in the previous step we can consider it as existing.
    index_exist |= must_create_index;

    for (id, kind) in enqueued {
        acc = match acc.accumulate(id, kind.into(), index_exist, primary_key) {
            Continue(acc) => acc,
            Break(acc) => return Some((acc, must_create_index)),
        };
    }

    Some((acc, must_create_index))
}
