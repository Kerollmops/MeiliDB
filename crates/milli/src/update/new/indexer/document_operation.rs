use std::sync::atomic::Ordering;

use bumpalo::collections::CollectIn;
use bumpalo::Bump;
use bumparaw_collections::RawMap;
use hashbrown::hash_map::Entry;
use heed::RoTxn;
use memmap2::Mmap;
use rayon::slice::ParallelSlice;
use rustc_hash::FxBuildHasher;
use serde_json::value::RawValue;
use serde_json::Deserializer;

use super::super::document_change::DocumentChange;
use super::document_changes::{DocumentChangeContext, DocumentChanges};
use super::guess_primary_key::retrieve_or_guess_primary_key;
use crate::documents::PrimaryKey;
use crate::progress::{AtomicPayloadStep, Progress};
use crate::update::new::document::Versions;
use crate::update::new::steps::IndexingStep;
use crate::update::new::thread_local::MostlySend;
use crate::update::new::{Deletion, Insertion, Update};
use crate::update::{AvailableIds, IndexDocumentsMethod};
use crate::{DocumentId, Error, FieldsIdsMap, Index, InternalError, Result, UserError};

pub struct DocumentOperation<'pl> {
    operations: Vec<Payload<'pl>>,
    method: MergeMethod,
}

impl<'pl> DocumentOperation<'pl> {
    pub fn new(method: IndexDocumentsMethod) -> Self {
        Self { operations: Default::default(), method: MergeMethod::from(method) }
    }

    /// TODO please give me a type
    /// The payload is expected to be in the NDJSON format
    pub fn add_documents(&mut self, payload: &'pl Mmap) -> Result<()> {
        #[cfg(unix)]
        payload.advise(memmap2::Advice::Sequential)?;
        self.operations.push(Payload::Addition(&payload[..]));
        Ok(())
    }

    pub fn delete_documents(&mut self, to_delete: &'pl [&'pl str]) {
        self.operations.push(Payload::Deletion(to_delete))
    }

    #[allow(clippy::too_many_arguments)]
    #[tracing::instrument(level = "trace", skip_all, target = "indexing::document_operation")]
    pub fn into_changes<MSP>(
        self,
        indexer: &'pl Bump,
        index: &Index,
        rtxn: &'pl RoTxn<'pl>,
        primary_key_from_op: Option<&'pl str>,
        new_fields_ids_map: &mut FieldsIdsMap,
        must_stop_processing: &MSP,
        progress: Progress,
    ) -> Result<(DocumentOperationChanges<'pl>, Vec<PayloadStats>, Option<PrimaryKey<'pl>>)>
    where
        MSP: Fn() -> bool,
    {
        progress.update_progress(IndexingStep::PreparingPayloads);
        let Self { operations, method } = self;

        let documents_ids = index.documents_ids(rtxn)?;
        let mut operations_stats = Vec::new();
        let mut available_docids = AvailableIds::new(&documents_ids);
        let mut docids_version_offsets = hashbrown::HashMap::new();
        let mut primary_key = None;

        let payload_count = operations.len();
        let (step, progress_step) = AtomicPayloadStep::new(payload_count as u32);
        progress.update_progress(progress_step);

        for (payload_index, operation) in operations.into_iter().enumerate() {
            if must_stop_processing() {
                return Err(InternalError::AbortedIndexation.into());
            }
            step.store(payload_index as u32, Ordering::Relaxed);

            let mut bytes = 0;
            let result = match operation {
                Payload::Addition(payload) => extract_addition_payload_changes(
                    indexer,
                    index,
                    rtxn,
                    primary_key_from_op,
                    &mut primary_key,
                    new_fields_ids_map,
                    &mut available_docids,
                    &mut bytes,
                    &docids_version_offsets,
                    method,
                    payload,
                ),
                Payload::Deletion(to_delete) => extract_deletion_payload_changes(
                    index,
                    rtxn,
                    &mut available_docids,
                    &docids_version_offsets,
                    method,
                    to_delete,
                ),
            };

            let mut document_count = 0;
            let error = match result {
                Ok(new_docids_version_offsets) => {
                    document_count = new_docids_version_offsets.len() as u64;
                    // If we don't have any error then we can merge the content of this payload
                    // into to main payload. Else we just drop this payload extraction.
                    merge_version_offsets(&mut docids_version_offsets, new_docids_version_offsets);
                    None
                }
                Err(Error::UserError(user_error)) => Some(user_error),
                Err(e) => return Err(e),
            };
            operations_stats.push(PayloadStats { document_count, bytes, error });
        }
        step.store(payload_count as u32, Ordering::Relaxed);

        // TODO We must drain the HashMap into a Vec because rayon::hash_map::IntoIter: !Clone
        let mut docids_version_offsets: bumpalo::collections::vec::Vec<_> =
            docids_version_offsets.drain().collect_in(indexer);

        // Reorder the offsets to make sure we iterate on the file sequentially
        // And finally sort them
        docids_version_offsets.sort_unstable_by_key(|(_, po)| method.sort_key(&po.operations));

        let docids_version_offsets = docids_version_offsets.into_bump_slice();
        Ok((DocumentOperationChanges { docids_version_offsets }, operations_stats, primary_key))
    }
}

impl Default for DocumentOperation<'_> {
    fn default() -> Self {
        DocumentOperation::new(IndexDocumentsMethod::default())
    }
}

#[allow(clippy::too_many_arguments)]
fn extract_addition_payload_changes<'r, 'pl: 'r>(
    indexer: &'pl Bump,
    index: &Index,
    rtxn: &'r RoTxn<'r>,
    primary_key_from_op: Option<&'r str>,
    primary_key: &mut Option<PrimaryKey<'r>>,
    new_fields_ids_map: &mut FieldsIdsMap,
    available_docids: &mut AvailableIds,
    bytes: &mut u64,
    main_docids_version_offsets: &hashbrown::HashMap<&'pl str, PayloadOperations<'pl>>,
    method: MergeMethod,
    payload: &'pl [u8],
) -> Result<hashbrown::HashMap<&'pl str, PayloadOperations<'pl>>> {
    let mut new_docids_version_offsets = hashbrown::HashMap::<&str, PayloadOperations<'pl>>::new();

    let mut previous_offset = 0;
    let mut iter = Deserializer::from_slice(payload).into_iter::<&RawValue>();
    while let Some(doc) = iter.next().transpose().map_err(InternalError::SerdeJson)? {
        *bytes = previous_offset as u64;

        // Only guess the primary key if it is the first document
        let retrieved_primary_key = if previous_offset == 0 {
            let doc = RawMap::from_raw_value_and_hasher(doc, FxBuildHasher, indexer)
                .map(Some)
                .map_err(UserError::SerdeJson)?;

            let result = retrieve_or_guess_primary_key(
                rtxn,
                index,
                new_fields_ids_map,
                primary_key_from_op,
                doc,
            );

            let (pk, _has_been_changed) = match result {
                Ok(Ok(pk)) => pk,
                Ok(Err(user_error)) => return Err(Error::UserError(user_error)),
                Err(error) => return Err(error),
            };

            primary_key.get_or_insert(pk)
        } else {
            // primary key was retrieved in the first iteration or in a previous payload
            primary_key.as_ref().unwrap()
        };

        let external_id = match retrieved_primary_key.extract_fields_and_docid(
            doc,
            new_fields_ids_map,
            indexer,
        ) {
            Ok(edi) => edi,
            Err(e) => return Err(e),
        };

        let external_id = external_id.to_de();
        let current_offset = iter.byte_offset();
        let document_offset = DocumentOffset { content: &payload[previous_offset..current_offset] };

        match main_docids_version_offsets.get(external_id) {
            None => {
                match index.external_documents_ids().get(rtxn, external_id) {
                    Ok(Some(docid)) => match new_docids_version_offsets.entry(external_id) {
                        Entry::Occupied(mut entry) => {
                            entry.get_mut().push_addition(document_offset)
                        }
                        Entry::Vacant(entry) => {
                            entry.insert(PayloadOperations::new_addition(
                                method,
                                docid,
                                false, // is new
                                document_offset,
                            ));
                        }
                    },
                    Ok(None) => match new_docids_version_offsets.entry(external_id) {
                        Entry::Occupied(mut entry) => {
                            entry.get_mut().push_addition(document_offset)
                        }
                        Entry::Vacant(entry) => {
                            let docid = match available_docids.next() {
                                Some(docid) => docid,
                                None => return Err(UserError::DocumentLimitReached.into()),
                            };
                            entry.insert(PayloadOperations::new_addition(
                                method,
                                docid,
                                true, // is new
                                document_offset,
                            ));
                        }
                    },
                    Err(e) => return Err(e.into()),
                }
            }
            Some(payload_operations) => match new_docids_version_offsets.entry(external_id) {
                Entry::Occupied(mut entry) => entry.get_mut().push_addition(document_offset),
                Entry::Vacant(entry) => {
                    entry.insert(PayloadOperations::new_addition(
                        method,
                        payload_operations.docid,
                        payload_operations.is_new,
                        document_offset,
                    ));
                }
            },
        }

        previous_offset = iter.byte_offset();
    }

    Ok(new_docids_version_offsets)
}

fn extract_deletion_payload_changes<'s, 'pl: 's>(
    index: &Index,
    rtxn: &RoTxn,
    available_docids: &mut AvailableIds,
    main_docids_version_offsets: &hashbrown::HashMap<&'s str, PayloadOperations<'pl>>,
    method: MergeMethod,
    to_delete: &'pl [&'pl str],
) -> Result<hashbrown::HashMap<&'s str, PayloadOperations<'pl>>> {
    let mut new_docids_version_offsets = hashbrown::HashMap::<&str, PayloadOperations<'pl>>::new();

    for external_id in to_delete {
        match main_docids_version_offsets.get(external_id) {
            None => {
                match index.external_documents_ids().get(rtxn, external_id) {
                    Ok(Some(docid)) => {
                        match new_docids_version_offsets.entry(external_id) {
                            Entry::Occupied(mut entry) => entry.get_mut().push_deletion(),
                            Entry::Vacant(entry) => {
                                entry.insert(PayloadOperations::new_deletion(
                                    method, docid, false, // is new
                                ));
                            }
                        }
                    }
                    Ok(None) => {
                        let docid = match available_docids.next() {
                            Some(docid) => docid,
                            None => return Err(UserError::DocumentLimitReached.into()),
                        };
                        match new_docids_version_offsets.entry(external_id) {
                            Entry::Occupied(mut entry) => entry.get_mut().push_deletion(),
                            Entry::Vacant(entry) => {
                                entry.insert(PayloadOperations::new_deletion(
                                    method, docid, true, // is new
                                ));
                            }
                        }
                    }
                    Err(e) => return Err(e.into()),
                }
            }
            Some(payload_operations) => match new_docids_version_offsets.entry(external_id) {
                Entry::Occupied(mut entry) => entry.get_mut().push_deletion(),
                Entry::Vacant(entry) => {
                    entry.insert(PayloadOperations::new_deletion(
                        method,
                        payload_operations.docid,
                        payload_operations.is_new,
                    ));
                }
            },
        }
    }

    Ok(new_docids_version_offsets)
}

fn merge_version_offsets<'s, 'pl>(
    main: &mut hashbrown::HashMap<&'s str, PayloadOperations<'pl>>,
    new: hashbrown::HashMap<&'s str, PayloadOperations<'pl>>,
) {
    // We cannot swap like nothing because documents
    // operations must be in the right order.
    if main.is_empty() {
        return *main = new;
    }

    for (key, new_payload) in new {
        match main.entry(key) {
            Entry::Occupied(mut entry) => entry.get_mut().append_operations(new_payload.operations),
            Entry::Vacant(entry) => {
                entry.insert(new_payload);
            }
        }
    }
}

impl<'pl> DocumentChanges<'pl> for DocumentOperationChanges<'pl> {
    type Item = (&'pl str, PayloadOperations<'pl>);

    fn iter(
        &self,
        chunk_size: usize,
    ) -> impl rayon::prelude::IndexedParallelIterator<Item = impl AsRef<[Self::Item]>> {
        self.docids_version_offsets.par_chunks(chunk_size)
    }

    fn item_to_document_change<'doc, T: MostlySend + 'doc>(
        &'doc self,
        context: &'doc DocumentChangeContext<T>,
        item: &'doc Self::Item,
    ) -> Result<Option<DocumentChange<'doc>>>
    where
        'pl: 'doc,
    {
        let (external_doc, payload_operations) = item;
        payload_operations.merge_method.merge(
            payload_operations.docid,
            external_doc,
            payload_operations.is_new,
            &context.doc_alloc,
            &payload_operations.operations[..],
        )
    }

    fn len(&self) -> usize {
        self.docids_version_offsets.len()
    }
}

pub struct DocumentOperationChanges<'pl> {
    docids_version_offsets: &'pl [(&'pl str, PayloadOperations<'pl>)],
}

pub enum Payload<'pl> {
    Addition(&'pl [u8]),
    Deletion(&'pl [&'pl str]),
}

pub struct PayloadStats {
    pub bytes: u64,
    pub document_count: u64,
    pub error: Option<UserError>,
}

pub struct PayloadOperations<'pl> {
    /// The internal document id of the document.
    pub docid: DocumentId,
    /// Wether this document is not in the current database (visible by the rtxn).
    pub is_new: bool,
    /// The operations to perform, in order, on this document.
    pub operations: Vec<InnerDocOp<'pl>>,
    /// The merge method we are using to merge payloads and documents.
    merge_method: MergeMethod,
}

impl<'pl> PayloadOperations<'pl> {
    fn new_deletion(merge_method: MergeMethod, docid: DocumentId, is_new: bool) -> Self {
        Self { docid, is_new, operations: vec![InnerDocOp::Deletion], merge_method }
    }

    fn new_addition(
        merge_method: MergeMethod,
        docid: DocumentId,
        is_new: bool,
        offset: DocumentOffset<'pl>,
    ) -> Self {
        Self { docid, is_new, operations: vec![InnerDocOp::Addition(offset)], merge_method }
    }
}

impl<'pl> PayloadOperations<'pl> {
    fn push_addition(&mut self, offset: DocumentOffset<'pl>) {
        if self.merge_method.useless_previous_changes() {
            self.operations.clear();
        }
        self.operations.push(InnerDocOp::Addition(offset))
    }

    fn push_deletion(&mut self) {
        self.operations.clear();
        self.operations.push(InnerDocOp::Deletion);
    }

    fn append_operations(&mut self, mut operations: Vec<InnerDocOp<'pl>>) {
        debug_assert!(!operations.is_empty());
        if self.merge_method.useless_previous_changes() {
            self.operations.clear();
        }
        self.operations.append(&mut operations);
    }
}

#[derive(Clone)]
pub enum InnerDocOp<'pl> {
    Addition(DocumentOffset<'pl>),
    Deletion,
}

/// Represents an offset where a document lives
/// in an mmapped grenad reader file.
#[derive(Clone)]
pub struct DocumentOffset<'pl> {
    /// The mmapped payload files.
    pub content: &'pl [u8],
}

trait MergeChanges {
    /// Whether the payloads in the list of operations are useless or not.
    fn useless_previous_changes(&self) -> bool;

    /// Returns a key that is used to order the payloads the right way.
    fn sort_key(&self, docops: &[InnerDocOp]) -> usize;

    fn merge<'doc>(
        &self,
        docid: DocumentId,
        external_docid: &'doc str,
        is_new: bool,
        doc_alloc: &'doc Bump,
        operations: &'doc [InnerDocOp],
    ) -> Result<Option<DocumentChange<'doc>>>;
}

#[derive(Debug, Clone, Copy)]
enum MergeMethod {
    ForReplacement(MergeDocumentForReplacement),
    ForUpdates(MergeDocumentForUpdates),
}

impl MergeChanges for MergeMethod {
    fn useless_previous_changes(&self) -> bool {
        match self {
            MergeMethod::ForReplacement(merge) => merge.useless_previous_changes(),
            MergeMethod::ForUpdates(merge) => merge.useless_previous_changes(),
        }
    }

    fn sort_key(&self, docops: &[InnerDocOp]) -> usize {
        match self {
            MergeMethod::ForReplacement(merge) => merge.sort_key(docops),
            MergeMethod::ForUpdates(merge) => merge.sort_key(docops),
        }
    }

    fn merge<'doc>(
        &self,
        docid: DocumentId,
        external_docid: &'doc str,
        is_new: bool,
        doc_alloc: &'doc Bump,
        operations: &'doc [InnerDocOp],
    ) -> Result<Option<DocumentChange<'doc>>> {
        match self {
            MergeMethod::ForReplacement(merge) => {
                merge.merge(docid, external_docid, is_new, doc_alloc, operations)
            }
            MergeMethod::ForUpdates(merge) => {
                merge.merge(docid, external_docid, is_new, doc_alloc, operations)
            }
        }
    }
}

impl From<IndexDocumentsMethod> for MergeMethod {
    fn from(method: IndexDocumentsMethod) -> Self {
        match method {
            IndexDocumentsMethod::ReplaceDocuments => {
                MergeMethod::ForReplacement(MergeDocumentForReplacement)
            }
            IndexDocumentsMethod::UpdateDocuments => {
                MergeMethod::ForUpdates(MergeDocumentForUpdates)
            }
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct MergeDocumentForReplacement;

impl MergeChanges for MergeDocumentForReplacement {
    fn useless_previous_changes(&self) -> bool {
        true
    }

    /// Reorders to read only the last change.
    fn sort_key(&self, docops: &[InnerDocOp]) -> usize {
        let f = |ido: &_| match ido {
            InnerDocOp::Addition(add) => Some(add.content.as_ptr() as usize),
            InnerDocOp::Deletion => None,
        };
        docops.iter().rev().find_map(f).unwrap_or(0)
    }

    /// Returns only the most recent version of a document based on the updates from the payloads.
    ///
    /// This function is only meant to be used when doing a replacement and not an update.
    fn merge<'doc>(
        &self,
        docid: DocumentId,
        external_doc: &'doc str,
        is_new: bool,
        doc_alloc: &'doc Bump,
        operations: &'doc [InnerDocOp],
    ) -> Result<Option<DocumentChange<'doc>>> {
        match operations.last() {
            Some(InnerDocOp::Addition(DocumentOffset { content })) => {
                let document = serde_json::from_slice(content).unwrap();
                let document =
                    RawMap::from_raw_value_and_hasher(document, FxBuildHasher, doc_alloc)
                        .map_err(UserError::SerdeJson)?;

                if is_new {
                    Ok(Some(DocumentChange::Insertion(Insertion::create(
                        docid,
                        external_doc,
                        Versions::single(document),
                    ))))
                } else {
                    Ok(Some(DocumentChange::Update(Update::create(
                        docid,
                        external_doc,
                        Versions::single(document),
                        true,
                    ))))
                }
            }
            Some(InnerDocOp::Deletion) => {
                return if is_new {
                    Ok(None)
                } else {
                    let deletion = Deletion::create(docid, external_doc);
                    Ok(Some(DocumentChange::Deletion(deletion)))
                };
            }
            None => unreachable!("We must not have empty set of operations on a document"),
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct MergeDocumentForUpdates;

impl MergeChanges for MergeDocumentForUpdates {
    fn useless_previous_changes(&self) -> bool {
        false
    }

    /// Reorders to read the first changes first so that it's faster to read the first one and then the rest.
    fn sort_key(&self, docops: &[InnerDocOp]) -> usize {
        let f = |ido: &_| match ido {
            InnerDocOp::Addition(add) => Some(add.content.as_ptr() as usize),
            InnerDocOp::Deletion => None,
        };
        docops.iter().find_map(f).unwrap_or(0)
    }

    /// Reads the previous version of a document from the database, the new versions
    /// in the grenad update files and merges them to generate a new boxed obkv.
    ///
    /// This function is only meant to be used when doing an update and not a replacement.
    fn merge<'doc>(
        &self,
        docid: DocumentId,
        external_docid: &'doc str,
        is_new: bool,
        doc_alloc: &'doc Bump,
        operations: &'doc [InnerDocOp],
    ) -> Result<Option<DocumentChange<'doc>>> {
        if operations.is_empty() {
            unreachable!("We must not have empty set of operations on a document");
        }

        let last_deletion = operations.iter().rposition(|op| matches!(op, InnerDocOp::Deletion));
        let operations = &operations[last_deletion.map_or(0, |i| i + 1)..];

        let has_deletion = last_deletion.is_some();

        if operations.is_empty() {
            return if is_new {
                Ok(None)
            } else {
                let deletion = Deletion::create(docid, external_docid);
                Ok(Some(DocumentChange::Deletion(deletion)))
            };
        }

        let versions = match operations {
            [single] => {
                let DocumentOffset { content } = match single {
                    InnerDocOp::Addition(offset) => offset,
                    InnerDocOp::Deletion => {
                        unreachable!("Deletion in document operations")
                    }
                };
                let document = serde_json::from_slice(content).unwrap();
                let document =
                    RawMap::from_raw_value_and_hasher(document, FxBuildHasher, doc_alloc)
                        .map_err(UserError::SerdeJson)?;

                Some(Versions::single(document))
            }
            operations => {
                let versions = operations.iter().map(|operation| {
                    let DocumentOffset { content } = match operation {
                        InnerDocOp::Addition(offset) => offset,
                        InnerDocOp::Deletion => {
                            unreachable!("Deletion in document operations")
                        }
                    };

                    let document = serde_json::from_slice(content).unwrap();
                    let document =
                        RawMap::from_raw_value_and_hasher(document, FxBuildHasher, doc_alloc)
                            .map_err(UserError::SerdeJson)?;
                    Ok(document)
                });
                Versions::multiple(versions)?
            }
        };

        let Some(versions) = versions else { return Ok(None) };

        if is_new {
            Ok(Some(DocumentChange::Insertion(Insertion::create(docid, external_docid, versions))))
        } else {
            Ok(Some(DocumentChange::Update(Update::create(
                docid,
                external_docid,
                versions,
                has_deletion,
            ))))
        }
    }
}
