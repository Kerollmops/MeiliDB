use std::cell::RefCell;
use std::sync::atomic::{self, AtomicUsize};

use bumpalo::Bump;
use heed::{RoTxn, RwTxn};
use rayon::iter::{ParallelBridge, ParallelIterator as _};
use roaring::RoaringBitmap;
use zstd::bulk::Compressor;
use zstd::dict::{from_continuous, EncoderDictionary};

use crate::heed_codec::CompressedObkvU16;
use crate::update::new::document::Document as _;
use crate::update::new::indexer::document_changes::{
    DocumentChangeContext, DocumentChanges, Extractor, IndexingContext,
};
use crate::update::new::indexer::extract;
use crate::update::new::ref_cell_ext::RefCellExt as _;
use crate::update::new::steps::IndexingStep;
use crate::update::new::thread_local::{FullySend, MostlySend, ThreadLocal};
use crate::update::new::DocumentChange;
use crate::{Index, Result};

/// The compression level to use when compressing documents.
const COMPRESSION_LEVEL: i32 = 19;
/// The number of documents required as a sample for generating
/// the compression dictionary.
const SAMPLE_SIZE: usize = 10_000;
/// The maximum size the document compression dictionary can be.
const DICTIONARY_MAX_SIZE: usize = 64_000;
/// The maximum number of documents we accept to compress if they
/// have not already been compressed in the database. If this threshold
/// is reached, we do not generate a dictionary and continue as is.
const COMPRESS_LIMIT: usize = 5_000_000;
/// This is 10KiB.
const TEN_KIB: usize = 10 * 1024;

/// A function dedicated to use the existing or generate an appropriate
/// document compression dictionay based on the documents available in
/// the database and the ones in the payload.
///
/// If it has to compute a new compression dictionary it immediately
/// writes the dictionary in the database and compresses the documents
/// that are not part of the current update with it.
///
/// If there are too many documents already in the database and no
/// compression dictionary we prefer not to generate a dictionary to avoid
/// compressing all of the documents and potentially blow up disk space.
#[tracing::instrument(level = "trace", skip_all, target = "indexing::documents")]
pub fn retrieve_or_compute_document_compression_dictionary<'pl, 'extractor, DC, MSP>(
    index: &Index,
    wtxn: &mut RwTxn<'_>,
    document_changes: &DC,
    indexing_context: IndexingContext<MSP>,
    extractor_allocs: &'extractor mut ThreadLocal<FullySend<Bump>>,
) -> Result<Option<EncoderDictionary<'static>>>
where
    DC: DocumentChanges<'pl>,
    MSP: Fn() -> bool + Sync,
{
    let number_of_documents = index.number_of_documents(wtxn)? as usize;
    match index.document_compression_raw_dictionary(wtxn)? {
        Some(dict) => Ok(Some(EncoderDictionary::copy(dict, COMPRESSION_LEVEL))),
        None if number_of_documents >= COMPRESS_LIMIT => Ok(None),
        None if number_of_documents + document_changes.len() < SAMPLE_SIZE => Ok(None),
        None => {
            let mut sample_data = Vec::new();
            let mut sample_sizes = Vec::new();
            let datastore = ThreadLocal::with_capacity(rayon::current_num_threads());
            let extractor = CompressorExtractor {
                total_documents_to_extract: SAMPLE_SIZE,
                extracted_documents_count: AtomicUsize::new(0),
            };

            // We first collect all the documents for the database into a buffer.
            for result in index.all_compressed_documents(wtxn)? {
                let (_docid, compressed_document) = result?;
                // The documents are not compressed with any dictionary at this point.
                let document = compressed_document.as_non_compressed();
                sample_data.extend_from_slice(document.as_bytes());
                sample_sizes.push(document.as_bytes().len());
            }

            // This extraction only takes care about documents replacements
            // and not updates (merges). The merged documents are ignored as
            // we will only use the previous version of them in the database,
            // just above.
            extract(
                document_changes,
                &extractor,
                indexing_context,
                extractor_allocs,
                &datastore,
                IndexingStep::PreparingCompressionDictionary,
            )?;

            let mut all_documents_seen = RoaringBitmap::new();
            for data in datastore {
                let CompressorExtractorData { documents_seen, fields, fields_count, must_stop: _ } =
                    data.into_inner();

                all_documents_seen |= documents_seen;

                let mut fields_iter = fields.into_iter();
                for field_count in fields_count {
                    let mut document_fields_size = 0;
                    for field in fields_iter.by_ref().take(field_count) {
                        sample_data.extend_from_slice(field);
                        document_fields_size += field.len();
                    }
                    sample_sizes.push(document_fields_size);
                }

                debug_assert_eq!(
                    fields_iter.count(),
                    0,
                    "We must have consumed all the documents' \
                    fields but there were some remaining ones"
                );
            }

            // We avoid generating a dictionary if most (> 1/3) of the sample sizes are
            // smaller than 8 bytes, or if the sample data size is smaller than 10KiB.
            //
            // <https://github.com/facebook/zstd/blob/0218c8de0fa77bbd87e75f2ea70ba00b93460e15/lib/zdict.h#L190-L209>
            if sample_sizes.iter().filter(|s| **s < 8).count() > sample_sizes.len() / 3
                || sample_data.len() < TEN_KIB
            {
                return Ok(None);
            }

            let dictionary = from_continuous(&sample_data, &sample_sizes, DICTIONARY_MAX_SIZE)?;
            index.put_document_compression_dictionary(wtxn, &dictionary)?;
            let encoder_dictionary = EncoderDictionary::copy(&dictionary, COMPRESSION_LEVEL);

            let all_documents = index.documents_ids(wtxn)?;
            let documents_to_compress = all_documents - all_documents_seen;
            let datastore = ThreadLocal::with_capacity(rayon::max_num_threads());
            let pi = documents_to_compress.into_iter().par_bridge().map(|docid| {
                let data = datastore.get_or_try(|| {
                    crate::Result::Ok(RefCell::new(ParallelCompressionData {
                        rtxn: index.read_txn()?,
                        compressor: Compressor::with_dictionary(COMPRESSION_LEVEL, &dictionary)?,
                    }))
                })?;

                let mut data = data.borrow_mut_or_yield();
                let ParallelCompressionData { rtxn, compressor } = &mut *data;

                let compressed_document = index.compressed_document(rtxn, docid)?.unwrap();
                // The documents are not compressed with any dictionary at this point.
                let document = compressed_document.as_non_compressed();
                let compressed = CompressedObkvU16::with_compressor(document, compressor)?;
                Ok((docid, compressed)) as crate::Result<_>
            });

            // We compress in parallel and sequentially write the documents
            // in the database using the above parallel iterator.
            rayon_par_bridge::par_bridge(100, pi, |seq_iter| {
                for result in seq_iter {
                    let (docid, compressed_document) = result?;
                    index.documents.put(wtxn, &docid, &compressed_document)?;
                }
                Ok(()) as crate::Result<_>
            })?;

            Ok(Some(encoder_dictionary))
        }
    }
}

/// Used when we are compressing documents in parallel.
struct ParallelCompressionData<'extractor> {
    rtxn: RoTxn<'extractor>,
    compressor: Compressor<'extractor>,
}

unsafe impl<'extractor> MostlySend for RefCell<ParallelCompressionData<'extractor>> {}

struct CompressorExtractor {
    /// The total number of documents we must extract from all threads.
    total_documents_to_extract: usize,
    /// The combined, shared, number of extracted documents.
    extracted_documents_count: AtomicUsize,
}

#[derive(Default)]
struct CompressorExtractorData<'extractor> {
    /// The set of documents impacted by this update: deleted, modified, or updated.
    documents_seen: RoaringBitmap,
    /// The field content in JSON but as bytes.
    fields: Vec<&'extractor [u8]>,
    /// The number of fields associated to single documents.
    /// It is used to provide good sample to the dictionary generator.
    fields_count: Vec<usize>,
    /// We extracted the expected count of documents, we can skip everything now.
    must_stop: bool,
}

unsafe impl<'extractor> MostlySend for RefCell<CompressorExtractorData<'extractor>> {}

impl<'extractor> Extractor<'extractor> for CompressorExtractor {
    type Data = RefCell<CompressorExtractorData<'extractor>>;

    fn init_data<'doc>(
        &'doc self,
        _extractor_alloc: &'extractor bumpalo::Bump,
    ) -> crate::Result<Self::Data> {
        Ok(RefCell::new(CompressorExtractorData::default()))
    }

    fn process<'doc>(
        &'doc self,
        changes: impl Iterator<Item = crate::Result<DocumentChange<'doc>>>,
        context: &'doc DocumentChangeContext<'_, 'extractor, '_, '_, Self::Data>,
    ) -> crate::Result<()> {
        let mut data = context.data.borrow_mut_or_yield();

        for change in changes {
            if data.must_stop {
                return Ok(());
            }

            let change = change?;
            let docid = match change {
                DocumentChange::Deletion(deletion) => deletion.docid(),
                DocumentChange::Update(update) => update.docid(),
                DocumentChange::Insertion(insertion) => {
                    let mut fields_count = 0;
                    for result in insertion.inserted().iter_top_level_fields() {
                        let (_field_name, raw_value) = result?;
                        let bytes = raw_value.get().as_bytes();
                        data.fields.push(context.extractor_alloc.alloc_slice_copy(bytes));
                        fields_count += 1;
                    }

                    let previous_count =
                        self.extracted_documents_count.fetch_add(1, atomic::Ordering::SeqCst);
                    data.must_stop = previous_count >= self.total_documents_to_extract;
                    data.fields_count.push(fields_count);

                    insertion.docid()
                }
            };

            let is_new = data.documents_seen.insert(docid);
            debug_assert!(is_new, "We must not see the same documents multiple times");
        }

        Ok(())
    }
}
