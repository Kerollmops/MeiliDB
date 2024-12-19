mod extract_word_docids;
mod extract_word_pair_proximity_docids;
mod tokenize_document;

use std::cell::RefCell;
use std::marker::PhantomData;

use bumpalo::Bump;
pub use extract_word_docids::{WordDocidsCaches, WordDocidsExtractors};
pub use extract_word_pair_proximity_docids::WordPairProximityDocidsExtractor;
use heed::RoTxn;
use tokenize_document::{tokenizer_builder, DocumentTokenizer};

use super::cache::BalancedCaches;
use super::DocidsExtractor;
use crate::update::new::indexer::document_changes::{
    extract, DocumentChangeContext, DocumentChanges, Extractor, IndexingContext,
};
use crate::update::new::steps::IndexingStep;
use crate::update::new::thread_local::{FullySend, ThreadLocal};
use crate::update::new::DocumentChange;
use crate::update::GrenadParameters;
use crate::{Index, Result, MAX_POSITION_PER_ATTRIBUTE};

pub struct SearchableExtractorData<'a, EX: SearchableExtractor> {
    tokenizer: &'a DocumentTokenizer<'a>,
    grenad_parameters: &'a GrenadParameters,
    buckets: usize,
    _ex: PhantomData<EX>,
}

impl<'a, 'extractor, EX: SearchableExtractor + Sync> Extractor<'extractor>
    for SearchableExtractorData<'a, EX>
{
    type Data = RefCell<BalancedCaches<'extractor>>;

    fn init_data(&self, extractor_alloc: &'extractor Bump) -> Result<Self::Data> {
        Ok(RefCell::new(BalancedCaches::new_in(
            self.buckets,
            self.grenad_parameters.max_memory_by_thread(),
            extractor_alloc,
        )))
    }

    fn process<'doc>(
        &self,
        changes: impl Iterator<Item = Result<DocumentChange<'doc>>>,
        context: &DocumentChangeContext<Self::Data>,
    ) -> Result<()> {
        for change in changes {
            let change = change?;
            EX::extract_document_change(context, self.tokenizer, change)?;
        }
        Ok(())
    }
}

pub trait SearchableExtractor: Sized + Sync {
    fn run_extraction<'pl, 'fid, 'indexer, 'index, 'extractor, DC: DocumentChanges<'pl>, MSP>(
        document_changes: &DC,
        indexing_context: IndexingContext<'fid, 'indexer, 'index, MSP>,
        extractor_allocs: &'extractor mut ThreadLocal<FullySend<Bump>>,
        step: IndexingStep,
    ) -> Result<Vec<BalancedCaches<'extractor>>>
    where
        MSP: Fn() -> bool + Sync,
    {
        let rtxn = indexing_context.index.read_txn()?;
        let stop_words = indexing_context.index.stop_words(&rtxn)?;
        let allowed_separators = indexing_context.index.allowed_separators(&rtxn)?;
        let allowed_separators: Option<Vec<_>> =
            allowed_separators.as_ref().map(|s| s.iter().map(String::as_str).collect());
        let dictionary = indexing_context.index.dictionary(&rtxn)?;
        let dictionary: Option<Vec<_>> =
            dictionary.as_ref().map(|s| s.iter().map(String::as_str).collect());
        let mut builder = tokenizer_builder(
            stop_words.as_ref(),
            allowed_separators.as_deref(),
            dictionary.as_deref(),
        );
        let tokenizer = builder.build();

        let attributes_to_extract = Self::attributes_to_extract(&rtxn, indexing_context.index)?;
        let attributes_to_skip = Self::attributes_to_skip(&rtxn, indexing_context.index)?;
        let localized_attributes_rules =
            indexing_context.index.localized_attributes_rules(&rtxn)?.unwrap_or_default();

        let document_tokenizer = DocumentTokenizer {
            tokenizer: &tokenizer,
            attribute_to_extract: attributes_to_extract.as_deref(),
            attribute_to_skip: attributes_to_skip.as_slice(),
            localized_attributes_rules: &localized_attributes_rules,
            max_positions_per_attributes: MAX_POSITION_PER_ATTRIBUTE,
        };

        let extractor_data: SearchableExtractorData<Self> = SearchableExtractorData {
            tokenizer: &document_tokenizer,
            grenad_parameters: indexing_context.grenad_parameters,
            buckets: rayon::current_num_threads(),
            _ex: PhantomData,
        };

        let datastore = ThreadLocal::new();

        {
            let span =
                tracing::trace_span!(target: "indexing::documents::extract", "docids_extraction");
            let _entered = span.enter();
            extract(
                document_changes,
                &extractor_data,
                indexing_context,
                extractor_allocs,
                &datastore,
                step,
            )?;
        }

        Ok(datastore.into_iter().map(RefCell::into_inner).collect())
    }

    fn extract_document_change(
        context: &DocumentChangeContext<RefCell<BalancedCaches>>,
        document_tokenizer: &DocumentTokenizer,
        document_change: DocumentChange,
    ) -> Result<()>;

    fn attributes_to_extract<'a>(rtxn: &'a RoTxn, index: &'a Index)
        -> Result<Option<Vec<&'a str>>>;

    fn attributes_to_skip<'a>(rtxn: &'a RoTxn, index: &'a Index) -> Result<Vec<&'a str>>;
}

impl<T: SearchableExtractor> DocidsExtractor for T {
    fn run_extraction<'pl, 'fid, 'indexer, 'index, 'extractor, DC: DocumentChanges<'pl>, MSP>(
        document_changes: &DC,
        indexing_context: IndexingContext<'fid, 'indexer, 'index, MSP>,
        extractor_allocs: &'extractor mut ThreadLocal<FullySend<Bump>>,
        step: IndexingStep,
    ) -> Result<Vec<BalancedCaches<'extractor>>>
    where
        MSP: Fn() -> bool + Sync,
    {
        Self::run_extraction(document_changes, indexing_context, extractor_allocs, step)
    }
}
