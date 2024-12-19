use std::cell::RefCell;
use std::collections::HashMap;
use std::mem::size_of;
use std::ops::DerefMut as _;

use bumpalo::collections::vec::Vec as BumpVec;
use bumpalo::Bump;
use heed::RoTxn;

use super::tokenize_document::{tokenizer_builder, DocumentTokenizer};
use crate::update::new::extract::cache::BalancedCaches;
use crate::update::new::extract::perm_json_p::contained_in;
use crate::update::new::indexer::document_changes::{
    extract, DocumentChangeContext, DocumentChanges, Extractor, IndexingContext,
};
use crate::update::new::ref_cell_ext::RefCellExt as _;
use crate::update::new::steps::IndexingStep;
use crate::update::new::thread_local::{FullySend, MostlySend, ThreadLocal};
use crate::update::new::DocumentChange;
use crate::update::GrenadParameters;
use crate::{bucketed_position, DocumentId, FieldId, Index, Result, MAX_POSITION_PER_ATTRIBUTE};

const MAX_COUNTED_WORDS: usize = 30;

pub struct WordDocidsBalancedCaches<'extractor> {
    word_fid_docids: BalancedCaches<'extractor>,
    word_docids: BalancedCaches<'extractor>,
    exact_word_docids: BalancedCaches<'extractor>,
    word_position_docids: BalancedCaches<'extractor>,
    fid_word_count_docids: BalancedCaches<'extractor>,
    fid_word_count: HashMap<FieldId, (Option<usize>, Option<usize>)>,
    current_docid: Option<DocumentId>,
}

unsafe impl<'extractor> MostlySend for WordDocidsBalancedCaches<'extractor> {}

impl<'extractor> WordDocidsBalancedCaches<'extractor> {
    pub fn new_in(buckets: usize, max_memory: Option<usize>, alloc: &'extractor Bump) -> Self {
        Self {
            word_fid_docids: BalancedCaches::new_in(buckets, max_memory, alloc),
            word_docids: BalancedCaches::new_in(buckets, max_memory, alloc),
            exact_word_docids: BalancedCaches::new_in(buckets, max_memory, alloc),
            word_position_docids: BalancedCaches::new_in(buckets, max_memory, alloc),
            fid_word_count_docids: BalancedCaches::new_in(buckets, max_memory, alloc),
            fid_word_count: HashMap::new(),
            current_docid: None,
        }
    }

    fn insert_add_u32(
        &mut self,
        field_id: FieldId,
        position: u16,
        word: &str,
        exact: bool,
        docid: u32,
        bump: &Bump,
    ) -> Result<()> {
        let word_bytes = word.as_bytes();
        if exact {
            self.exact_word_docids.insert_add_u32(word_bytes, docid)?;
        } else {
            self.word_docids.insert_add_u32(word_bytes, docid)?;
        }

        let buffer_size = word_bytes.len() + 1 + size_of::<FieldId>();
        let mut buffer = BumpVec::with_capacity_in(buffer_size, bump);

        buffer.clear();
        buffer.extend_from_slice(word_bytes);
        buffer.push(0);
        buffer.extend_from_slice(&field_id.to_be_bytes());
        self.word_fid_docids.insert_add_u32(&buffer, docid)?;

        let position = bucketed_position(position);
        buffer.clear();
        buffer.extend_from_slice(word_bytes);
        buffer.push(0);
        buffer.extend_from_slice(&position.to_be_bytes());
        self.word_position_docids.insert_add_u32(&buffer, docid)?;

        if self.current_docid.map_or(false, |id| docid != id) {
            self.flush_fid_word_count(&mut buffer)?;
        }

        self.fid_word_count
            .entry(field_id)
            .and_modify(|(_current_count, new_count)| *new_count.get_or_insert(0) += 1)
            .or_insert((None, Some(1)));
        self.current_docid = Some(docid);

        Ok(())
    }

    fn insert_del_u32(
        &mut self,
        field_id: FieldId,
        position: u16,
        word: &str,
        exact: bool,
        docid: u32,
        bump: &Bump,
    ) -> Result<()> {
        let word_bytes = word.as_bytes();
        if exact {
            self.exact_word_docids.insert_del_u32(word_bytes, docid)?;
        } else {
            self.word_docids.insert_del_u32(word_bytes, docid)?;
        }

        let buffer_size = word_bytes.len() + 1 + size_of::<FieldId>();
        let mut buffer = BumpVec::with_capacity_in(buffer_size, bump);

        buffer.clear();
        buffer.extend_from_slice(word_bytes);
        buffer.push(0);
        buffer.extend_from_slice(&field_id.to_be_bytes());
        self.word_fid_docids.insert_del_u32(&buffer, docid)?;

        let position = bucketed_position(position);
        buffer.clear();
        buffer.extend_from_slice(word_bytes);
        buffer.push(0);
        buffer.extend_from_slice(&position.to_be_bytes());
        self.word_position_docids.insert_del_u32(&buffer, docid)?;

        if self.current_docid.map_or(false, |id| docid != id) {
            self.flush_fid_word_count(&mut buffer)?;
        }

        self.fid_word_count
            .entry(field_id)
            .and_modify(|(current_count, _new_count)| *current_count.get_or_insert(0) += 1)
            .or_insert((Some(1), None));

        self.current_docid = Some(docid);

        Ok(())
    }

    fn flush_fid_word_count(&mut self, buffer: &mut BumpVec<u8>) -> Result<()> {
        for (fid, (current_count, new_count)) in self.fid_word_count.drain() {
            if current_count != new_count {
                if let Some(current_count) =
                    current_count.filter(|current_count| *current_count <= MAX_COUNTED_WORDS)
                {
                    buffer.clear();
                    buffer.extend_from_slice(&fid.to_be_bytes());
                    buffer.push(current_count as u8);
                    self.fid_word_count_docids
                        .insert_del_u32(buffer, self.current_docid.unwrap())?;
                }
                if let Some(new_count) =
                    new_count.filter(|new_count| *new_count <= MAX_COUNTED_WORDS)
                {
                    buffer.clear();
                    buffer.extend_from_slice(&fid.to_be_bytes());
                    buffer.push(new_count as u8);
                    self.fid_word_count_docids
                        .insert_add_u32(buffer, self.current_docid.unwrap())?;
                }
            }
        }

        Ok(())
    }
}

pub struct WordDocidsCaches<'extractor> {
    pub word_docids: Vec<BalancedCaches<'extractor>>,
    pub word_fid_docids: Vec<BalancedCaches<'extractor>>,
    pub exact_word_docids: Vec<BalancedCaches<'extractor>>,
    pub word_position_docids: Vec<BalancedCaches<'extractor>>,
    pub fid_word_count_docids: Vec<BalancedCaches<'extractor>>,
}

impl<'extractor> WordDocidsCaches<'extractor> {
    fn new() -> Self {
        Self {
            word_docids: Vec::new(),
            word_fid_docids: Vec::new(),
            exact_word_docids: Vec::new(),
            word_position_docids: Vec::new(),
            fid_word_count_docids: Vec::new(),
        }
    }

    fn push(&mut self, other: WordDocidsBalancedCaches<'extractor>) -> Result<()> {
        let WordDocidsBalancedCaches {
            word_docids,
            word_fid_docids,
            exact_word_docids,
            word_position_docids,
            fid_word_count_docids,
            fid_word_count: _,
            current_docid: _,
        } = other;

        self.word_docids.push(word_docids);
        self.word_fid_docids.push(word_fid_docids);
        self.exact_word_docids.push(exact_word_docids);
        self.word_position_docids.push(word_position_docids);
        self.fid_word_count_docids.push(fid_word_count_docids);

        Ok(())
    }
}

pub struct WordDocidsExtractorData<'a> {
    tokenizer: &'a DocumentTokenizer<'a>,
    grenad_parameters: &'a GrenadParameters,
    buckets: usize,
}

impl<'a, 'extractor> Extractor<'extractor> for WordDocidsExtractorData<'a> {
    type Data = RefCell<Option<WordDocidsBalancedCaches<'extractor>>>;

    fn init_data(&self, extractor_alloc: &'extractor Bump) -> Result<Self::Data> {
        Ok(RefCell::new(Some(WordDocidsBalancedCaches::new_in(
            self.buckets,
            self.grenad_parameters.max_memory_by_thread(),
            extractor_alloc,
        ))))
    }

    fn process<'doc>(
        &self,
        changes: impl Iterator<Item = Result<DocumentChange<'doc>>>,
        context: &DocumentChangeContext<Self::Data>,
    ) -> Result<()> {
        for change in changes {
            let change = change?;
            WordDocidsExtractors::extract_document_change(context, self.tokenizer, change)?;
        }
        Ok(())
    }
}

pub struct WordDocidsExtractors;

impl WordDocidsExtractors {
    pub fn run_extraction<'pl, 'fid, 'indexer, 'index, 'extractor, DC: DocumentChanges<'pl>, MSP>(
        document_changes: &DC,
        indexing_context: IndexingContext<'fid, 'indexer, 'index, MSP>,
        extractor_allocs: &'extractor mut ThreadLocal<FullySend<Bump>>,
        step: IndexingStep,
    ) -> Result<WordDocidsCaches<'extractor>>
    where
        MSP: Fn() -> bool + Sync,
    {
        let index = indexing_context.index;
        let rtxn = index.read_txn()?;

        let stop_words = index.stop_words(&rtxn)?;
        let allowed_separators = index.allowed_separators(&rtxn)?;
        let allowed_separators: Option<Vec<_>> =
            allowed_separators.as_ref().map(|s| s.iter().map(String::as_str).collect());
        let dictionary = index.dictionary(&rtxn)?;
        let dictionary: Option<Vec<_>> =
            dictionary.as_ref().map(|s| s.iter().map(String::as_str).collect());
        let builder = tokenizer_builder(
            stop_words.as_ref(),
            allowed_separators.as_deref(),
            dictionary.as_deref(),
        );
        let tokenizer = builder.into_tokenizer();

        let attributes_to_extract = Self::attributes_to_extract(&rtxn, index)?;
        let attributes_to_skip = Self::attributes_to_skip(&rtxn, index)?;
        let localized_attributes_rules =
            index.localized_attributes_rules(&rtxn)?.unwrap_or_default();

        let document_tokenizer = DocumentTokenizer {
            tokenizer: &tokenizer,
            attribute_to_extract: attributes_to_extract.as_deref(),
            attribute_to_skip: attributes_to_skip.as_slice(),
            localized_attributes_rules: &localized_attributes_rules,
            max_positions_per_attributes: MAX_POSITION_PER_ATTRIBUTE,
        };

        let datastore = ThreadLocal::new();

        {
            let span =
                tracing::trace_span!(target: "indexing::documents::extract", "docids_extraction");
            let _entered = span.enter();

            let extractor = WordDocidsExtractorData {
                tokenizer: &document_tokenizer,
                grenad_parameters: indexing_context.grenad_parameters,
                buckets: rayon::current_num_threads(),
            };

            extract(
                document_changes,
                &extractor,
                indexing_context,
                extractor_allocs,
                &datastore,
                step,
            )?;
        }

        let mut merger = WordDocidsCaches::new();
        for cache in datastore.into_iter().flat_map(RefCell::into_inner) {
            merger.push(cache)?;
        }

        Ok(merger)
    }

    fn extract_document_change(
        context: &DocumentChangeContext<RefCell<Option<WordDocidsBalancedCaches>>>,
        document_tokenizer: &DocumentTokenizer,
        document_change: DocumentChange,
    ) -> Result<()> {
        let index = &context.index;
        let rtxn = &context.rtxn;
        let mut cached_sorter_ref = context.data.borrow_mut_or_yield();
        let cached_sorter = cached_sorter_ref.as_mut().unwrap();
        let mut new_fields_ids_map = context.new_fields_ids_map.borrow_mut_or_yield();
        let new_fields_ids_map = new_fields_ids_map.deref_mut();
        let doc_alloc = &context.doc_alloc;

        let exact_attributes = index.exact_attributes(rtxn)?;
        let is_exact_attribute =
            |fname: &str| exact_attributes.iter().any(|attr| contained_in(fname, attr));
        match document_change {
            DocumentChange::Deletion(inner) => {
                let mut token_fn = |fname: &str, fid, pos, word: &str| {
                    cached_sorter.insert_del_u32(
                        fid,
                        pos,
                        word,
                        is_exact_attribute(fname),
                        inner.docid(),
                        doc_alloc,
                    )
                };
                document_tokenizer.tokenize_document(
                    inner.current(rtxn, index, context.db_fields_ids_map)?,
                    new_fields_ids_map,
                    &mut token_fn,
                )?;
            }
            DocumentChange::Update(inner) => {
                if !inner.has_changed_for_fields(
                    document_tokenizer.attribute_to_extract,
                    &context.rtxn,
                    context.index,
                    context.db_fields_ids_map,
                )? {
                    return Ok(());
                }

                let mut token_fn = |fname: &str, fid, pos, word: &str| {
                    cached_sorter.insert_del_u32(
                        fid,
                        pos,
                        word,
                        is_exact_attribute(fname),
                        inner.docid(),
                        doc_alloc,
                    )
                };
                document_tokenizer.tokenize_document(
                    inner.current(rtxn, index, context.db_fields_ids_map)?,
                    new_fields_ids_map,
                    &mut token_fn,
                )?;

                let mut token_fn = |fname: &str, fid, pos, word: &str| {
                    cached_sorter.insert_add_u32(
                        fid,
                        pos,
                        word,
                        is_exact_attribute(fname),
                        inner.docid(),
                        doc_alloc,
                    )
                };
                document_tokenizer.tokenize_document(
                    inner.merged(rtxn, index, context.db_fields_ids_map)?,
                    new_fields_ids_map,
                    &mut token_fn,
                )?;
            }
            DocumentChange::Insertion(inner) => {
                let mut token_fn = |fname: &str, fid, pos, word: &str| {
                    cached_sorter.insert_add_u32(
                        fid,
                        pos,
                        word,
                        is_exact_attribute(fname),
                        inner.docid(),
                        doc_alloc,
                    )
                };
                document_tokenizer.tokenize_document(
                    inner.inserted(),
                    new_fields_ids_map,
                    &mut token_fn,
                )?;
            }
        }

        let buffer_size = size_of::<FieldId>();
        let mut buffer = BumpVec::with_capacity_in(buffer_size, &context.doc_alloc);
        cached_sorter.flush_fid_word_count(&mut buffer)
    }

    fn attributes_to_extract<'a>(
        rtxn: &'a RoTxn,
        index: &'a Index,
    ) -> Result<Option<Vec<&'a str>>> {
        index.user_defined_searchable_fields(rtxn).map_err(Into::into)
    }

    fn attributes_to_skip<'a>(_rtxn: &'a RoTxn, _index: &'a Index) -> Result<Vec<&'a str>> {
        Ok(Vec::new())
    }
}
