mod enrich;
mod extract;
mod helpers;
mod transform;
mod typed_chunk;

use std::collections::{HashMap, HashSet};
use std::io::{BufWriter, Read, Seek, Write};
use std::iter;
use std::num::NonZeroU32;
use std::sync::Arc;

use crossbeam_channel::{Receiver, Sender};
use enrich::enrich_documents_batch;
use grenad::{Merger, MergerBuilder};
use heed::types::{Bytes, Str};
use heed::{Database, PutFlags};
use rand::SeedableRng as _;
use roaring::RoaringBitmap;
use serde::{Deserialize, Serialize};
use slice_group_by::GroupBy;
use tracing::debug;
use typed_chunk::{write_typed_chunk_into_index, ChunkAccumulator, TypedChunk};

pub use self::enrich::{extract_finite_float_from_value, DocumentId};
pub use self::helpers::*;
pub use self::transform::{Transform, TransformOutput};
use super::new::StdResult;
use crate::documents::{obkv_to_object, DocumentsBatchReader};
use crate::error::{Error, InternalError, UserError};
use crate::heed_codec::{CompressedObkvCodec, CompressedObkvU16};
use crate::index::{PrefixSearch, PrefixSettings};
use crate::thread_pool_no_abort::ThreadPoolNoAbortBuilder;
pub use crate::update::index_documents::helpers::CursorClonableMmap;
use crate::update::{
    IndexerConfig, UpdateIndexingStep, WordPrefixDocids, WordPrefixIntegerDocids, WordsPrefixesFst,
};
use crate::vector::{ArroyWrapper, EmbeddingConfigs};
use crate::{CboRoaringBitmapCodec, Index, Result, BEU32};

static MERGED_DATABASE_COUNT: usize = 7;
static PREFIX_DATABASE_COUNT: usize = 4;
static TOTAL_POSTING_DATABASE_COUNT: usize = MERGED_DATABASE_COUNT + PREFIX_DATABASE_COUNT;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DocumentAdditionResult {
    /// The number of documents that were indexed during the update
    pub indexed_documents: u64,
    /// The total number of documents in the index after the update
    pub number_of_documents: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[non_exhaustive]
pub enum IndexDocumentsMethod {
    /// Replace the previous document with the new one,
    /// removing all the already known attributes.
    ReplaceDocuments,

    /// Merge the previous version of the document with the new version,
    /// replacing old attributes values with the new ones and add the new attributes.
    UpdateDocuments,
}

impl Default for IndexDocumentsMethod {
    fn default() -> Self {
        Self::ReplaceDocuments
    }
}

pub struct IndexDocuments<'t, 'i, 'a, FP, FA> {
    wtxn: &'t mut heed::RwTxn<'i>,
    index: &'i Index,
    config: IndexDocumentsConfig,
    indexer_config: &'a IndexerConfig,
    transform: Option<Transform<'a, 'i>>,
    progress: FP,
    should_abort: FA,
    added_documents: u64,
    deleted_documents: u64,
    embedders: EmbeddingConfigs,
}

#[derive(Default, Debug, Clone)]
pub struct IndexDocumentsConfig {
    pub words_positions_level_group_size: Option<NonZeroU32>,
    pub words_positions_min_level_size: Option<NonZeroU32>,
    pub update_method: IndexDocumentsMethod,
    pub autogenerate_docids: bool,
}

impl<'t, 'i, 'a, FP, FA> IndexDocuments<'t, 'i, 'a, FP, FA>
where
    FP: Fn(UpdateIndexingStep) + Sync + Send,
    FA: Fn() -> bool + Sync + Send,
{
    pub fn new(
        wtxn: &'t mut heed::RwTxn<'i>,
        index: &'i Index,
        indexer_config: &'a IndexerConfig,
        config: IndexDocumentsConfig,
        progress: FP,
        should_abort: FA,
    ) -> Result<IndexDocuments<'t, 'i, 'a, FP, FA>> {
        let transform = Some(Transform::new(
            wtxn,
            index,
            indexer_config,
            config.update_method,
            config.autogenerate_docids,
        )?);

        Ok(IndexDocuments {
            transform,
            config,
            indexer_config,
            progress,
            should_abort,
            wtxn,
            index,
            added_documents: 0,
            deleted_documents: 0,
            embedders: Default::default(),
        })
    }

    /// Adds a batch of documents to the current builder.
    ///
    /// Since the documents are progressively added to the writer, a failure will cause only
    /// return an error and not the `IndexDocuments` struct as it is invalid to use it afterward.
    ///
    /// Returns the number of documents added to the builder.
    #[tracing::instrument(level = "trace", skip_all, target = "indexing::documents")]
    pub fn add_documents<R: Read + Seek>(
        mut self,
        reader: DocumentsBatchReader<R>,
    ) -> Result<(Self, StdResult<u64, UserError>)> {
        // Early return when there is no document to add
        if reader.is_empty() {
            return Ok((self, Ok(0)));
        }

        // We check for user errors in this validator and if there is one, we can return
        // the `IndexDocument` struct as it is valid to send more documents into it.
        // However, if there is an internal error we throw it away!
        let enriched_documents_reader = match enrich_documents_batch(
            self.wtxn,
            self.index,
            self.config.autogenerate_docids,
            reader,
        )? {
            Ok(reader) => reader,
            Err(user_error) => return Ok((self, Err(user_error))),
        };

        let indexed_documents =
            self.transform.as_mut().expect("Invalid document addition state").read_documents(
                enriched_documents_reader,
                self.wtxn,
                &self.progress,
                &self.should_abort,
            )? as u64;

        self.added_documents += indexed_documents;

        Ok((self, Ok(indexed_documents)))
    }

    pub fn with_embedders(mut self, embedders: EmbeddingConfigs) -> Self {
        self.embedders = embedders;
        self
    }

    #[tracing::instrument(
        level = "trace"
        skip_all,
        target = "indexing::documents",
        name = "index_documents"
    )]
    pub fn execute(mut self) -> Result<DocumentAdditionResult> {
        if self.added_documents == 0 && self.deleted_documents == 0 {
            let number_of_documents = self.index.number_of_documents(self.wtxn)?;
            return Ok(DocumentAdditionResult { indexed_documents: 0, number_of_documents });
        }
        let output = self
            .transform
            .take()
            .expect("Invalid document addition state")
            .output_from_sorter(self.wtxn, &self.progress)?;

        let indexed_documents = output.documents_count as u64;
        let number_of_documents = self.execute_raw(output)?;

        Ok(DocumentAdditionResult { indexed_documents, number_of_documents })
    }

    /// Returns the total number of documents in the index after the update.
    #[tracing::instrument(
        level = "trace",
        skip_all,
        target = "indexing::details",
        name = "index_documents_raw"
    )]
    pub fn execute_raw(mut self, output: TransformOutput) -> Result<u64>
    where
        FP: Fn(UpdateIndexingStep) + Sync,
        FA: Fn() -> bool + Sync,
    {
        let TransformOutput {
            primary_key,
            mut settings_diff,
            field_distribution,
            documents_count,
            original_documents,
            flattened_documents,
        } = output;

        // update the internal facet and searchable list,
        // because they might have changed due to the nested documents flattening.
        settings_diff.new.recompute_facets(self.wtxn, self.index)?;
        settings_diff.new.recompute_searchables(self.wtxn, self.index)?;

        let settings_diff = Arc::new(settings_diff);
        let embedders_configs = Arc::new(self.index.embedding_configs(self.wtxn)?);

        let possible_embedding_mistakes =
            crate::vector::error::PossibleEmbeddingMistakes::new(&field_distribution);

        let backup_pool;
        let pool = match self.indexer_config.thread_pool {
            Some(ref pool) => pool,
            None => {
                // We initialize a backup pool with the default
                // settings if none have already been set.
                #[allow(unused_mut)]
                let mut pool_builder = ThreadPoolNoAbortBuilder::new();

                #[cfg(test)]
                {
                    pool_builder = pool_builder.num_threads(1);
                }

                backup_pool = pool_builder.build()?;
                &backup_pool
            }
        };

        // create LMDB writer channel
        let (lmdb_writer_sx, lmdb_writer_rx): (
            Sender<Result<TypedChunk>>,
            Receiver<Result<TypedChunk>>,
        ) = crossbeam_channel::unbounded();

        // get the primary key field id
        let primary_key_id = settings_diff.new.fields_ids_map.id(&primary_key).unwrap();

        let pool_params = GrenadParameters {
            chunk_compression_type: self.indexer_config.chunk_compression_type,
            chunk_compression_level: self.indexer_config.chunk_compression_level,
            max_memory: self.indexer_config.max_memory,
            max_nb_chunks: self.indexer_config.max_nb_chunks, // default value, may be chosen.
        };
        let documents_chunk_size = match self.indexer_config.documents_chunk_size {
            Some(chunk_size) => chunk_size,
            None => {
                let default_chunk_size = 1024 * 1024 * 4; // 4MiB
                let min_chunk_size = 1024 * 512; // 512KiB

                // compute the chunk size from the number of available threads and the inputed data size.
                let total_size = match flattened_documents.as_ref() {
                    Some(flattened_documents) => flattened_documents.metadata().map(|m| m.len()),
                    None => Ok(default_chunk_size as u64),
                };
                let current_num_threads = pool.current_num_threads();
                // if we have more than 2 thread, create a number of chunk equal to 3/4 threads count
                let chunk_count = if current_num_threads > 2 {
                    (current_num_threads * 3 / 4).max(2)
                } else {
                    current_num_threads
                };
                total_size
                    .map_or(default_chunk_size, |size| (size as usize) / chunk_count)
                    .max(min_chunk_size)
            }
        };

        let original_documents = match original_documents {
            Some(original_documents) => Some(grenad::Reader::new(original_documents)?),
            None => None,
        };
        let flattened_documents = match flattened_documents {
            Some(flattened_documents) => Some(grenad::Reader::new(flattened_documents)?),
            None => None,
        };

        let max_positions_per_attributes = self.indexer_config.max_positions_per_attributes;

        let mut final_documents_ids = RoaringBitmap::new();
        let mut databases_seen = 0;
        let mut word_position_docids = None;
        let mut word_fid_docids = None;
        let mut word_docids = None;
        let mut exact_word_docids = None;
        let mut chunk_accumulator = ChunkAccumulator::default();
        let mut dimension = HashMap::new();

        let current_span = tracing::Span::current();

        // Run extraction pipeline in parallel.
        pool.install(|| {
                let settings_diff_cloned = settings_diff.clone();
                rayon::spawn(move || {
                    let child_span = tracing::trace_span!(target: "indexing::details", parent: &current_span, "extract_and_send_grenad_chunks");
                    let _enter = child_span.enter();

                    // split obkv file into several chunks
                    let original_chunk_iter = match original_documents {
                        Some(original_documents) => {
                            grenad_obkv_into_chunks(original_documents,pool_params,documents_chunk_size).map(either::Left)
                        },
                        None => Ok(either::Right(iter::empty())),
                    };

                    // split obkv file into several chunks
                    let flattened_chunk_iter = match flattened_documents {
                        Some(flattened_documents) => {
                            grenad_obkv_into_chunks(flattened_documents, pool_params, documents_chunk_size).map(either::Left)
                        },
                        None => Ok(either::Right(iter::empty())),
                    };

                    let result = original_chunk_iter.and_then(|original_chunk| {
                        let flattened_chunk = flattened_chunk_iter?;
                        // extract all databases from the chunked obkv douments
                        extract::data_from_obkv_documents(
                            original_chunk,
                            flattened_chunk,
                            pool_params,
                            lmdb_writer_sx.clone(),
                            primary_key_id,
                            embedders_configs.clone(),
                            settings_diff_cloned,
                            max_positions_per_attributes,
                            Arc::new(possible_embedding_mistakes)
                        )
                    });

                    if let Err(e) = result {
                        let _ = lmdb_writer_sx.send(Err(e));
                    }

                    // needs to be dropped to avoid channel waiting lock.
                    drop(lmdb_writer_sx);
                });

                (self.progress)(UpdateIndexingStep::MergeDataIntoFinalDatabase {
                    databases_seen,
                    total_databases: TOTAL_POSTING_DATABASE_COUNT,
                });

                loop {
                    if (self.should_abort)() {
                        return Err(Error::InternalError(InternalError::AbortedIndexation));
                    }

                    match lmdb_writer_rx.clone().recv_timeout(std::time::Duration::from_millis(500)) {
                        Err(status) => {
                            if let Some(typed_chunks) = chunk_accumulator.pop_longest() {
                                let (docids, is_merged_database) =
                                    write_typed_chunk_into_index(self.wtxn, self.index, &settings_diff, typed_chunks)?;
                                if !docids.is_empty() {
                                    final_documents_ids |= docids;
                                    let documents_seen_count = final_documents_ids.len();
                                    (self.progress)(UpdateIndexingStep::IndexDocuments {
                                        documents_seen: documents_seen_count as usize,
                                        total_documents: documents_count,
                                    });
                                    debug!(documents = documents_seen_count, total = documents_count, "Seen");
                                }
                                if is_merged_database {
                                    databases_seen += 1;
                                    (self.progress)(UpdateIndexingStep::MergeDataIntoFinalDatabase {
                                        databases_seen,
                                        total_databases: TOTAL_POSTING_DATABASE_COUNT,
                                    });
                                }
                            // If no more chunk remains in the chunk accumulator and the channel is disconected, break.
                            } else if status == crossbeam_channel::RecvTimeoutError::Disconnected {
                                break;
                            } else {
                                rayon::yield_now();
                            }
                        }
                        Ok(result) => {
                            let typed_chunk = match result? {
                                TypedChunk::WordDocids {
                                    word_docids_reader,
                                    exact_word_docids_reader,
                                    word_fid_docids_reader,
                                } => {
                                    let cloneable_chunk =
                                        unsafe { as_cloneable_grenad(&word_docids_reader)? };
                                    let word_docids = word_docids.get_or_insert_with(|| {
                                        MergerBuilder::new(MergeDeladdCboRoaringBitmaps)
                                    });
                                    word_docids.push(cloneable_chunk.into_cursor()?);
                                    let cloneable_chunk =
                                        unsafe { as_cloneable_grenad(&exact_word_docids_reader)? };
                                    let exact_word_docids =
                                        exact_word_docids.get_or_insert_with(|| {
                                            MergerBuilder::new(
                                                MergeDeladdCboRoaringBitmaps,
                                            )
                                        });
                                    exact_word_docids.push(cloneable_chunk.into_cursor()?);
                                    let cloneable_chunk =
                                        unsafe { as_cloneable_grenad(&word_fid_docids_reader)? };
                                    let word_fid_docids = word_fid_docids.get_or_insert_with(|| {
                                        MergerBuilder::new(MergeDeladdCboRoaringBitmaps)
                                    });
                                    word_fid_docids.push(cloneable_chunk.into_cursor()?);
                                    TypedChunk::WordDocids {
                                        word_docids_reader,
                                        exact_word_docids_reader,
                                        word_fid_docids_reader,
                                    }
                                }
                                TypedChunk::WordPositionDocids(chunk) => {
                                    let cloneable_chunk = unsafe { as_cloneable_grenad(&chunk)? };
                                    let word_position_docids =
                                        word_position_docids.get_or_insert_with(|| {
                                            MergerBuilder::new(
                                                MergeDeladdCboRoaringBitmaps,
                                            )
                                        });
                                    word_position_docids.push(cloneable_chunk.into_cursor()?);
                                    TypedChunk::WordPositionDocids(chunk)
                                }
                                TypedChunk::VectorPoints {
                                    expected_dimension,
                                    remove_vectors,
                                    embeddings,
                                    manual_vectors,
                                    embedder_name,
                                    add_to_user_provided,
                                    remove_from_user_provided,
                                } => {
                                    dimension.insert(embedder_name.clone(), expected_dimension);
                                    TypedChunk::VectorPoints {
                                        remove_vectors,
                                        embeddings,
                                        expected_dimension,
                                        manual_vectors,
                                        embedder_name,
                                        add_to_user_provided,
                                        remove_from_user_provided,
                                    }
                                }
                                otherwise => otherwise,
                            };

                            chunk_accumulator.insert(typed_chunk);
                        }
                    }
                }

                Ok(())
            }).map_err(InternalError::from)??;

        // We write the field distribution into the main database
        self.index.put_field_distribution(self.wtxn, &field_distribution)?;

        // We write the primary key field id into the main database
        self.index.put_primary_key(self.wtxn, &primary_key)?;
        let number_of_documents = self.index.number_of_documents(self.wtxn)?;
        let mut rng = rand::rngs::StdRng::seed_from_u64(42);

        // If an embedder wasn't used in the typedchunk but must be binary quantized
        // we should insert it in `dimension`
        for (name, action) in settings_diff.embedding_config_updates.iter() {
            if action.is_being_quantized && !dimension.contains_key(name.as_str()) {
                let index = self.index.embedder_category_id.get(self.wtxn, name)?.ok_or(
                    InternalError::DatabaseMissingEntry {
                        db_name: "embedder_category_id",
                        key: None,
                    },
                )?;
                let reader =
                    ArroyWrapper::new(self.index.vector_arroy, index, action.was_quantized);
                let dim = reader.dimensions(self.wtxn)?;
                dimension.insert(name.to_string(), dim);
            }
        }

        for (embedder_name, dimension) in dimension {
            let wtxn = &mut *self.wtxn;
            let vector_arroy = self.index.vector_arroy;
            let cancel = &self.should_abort;

            let embedder_index = self.index.embedder_category_id.get(wtxn, &embedder_name)?.ok_or(
                InternalError::DatabaseMissingEntry { db_name: "embedder_category_id", key: None },
            )?;
            let embedder_config = settings_diff.embedding_config_updates.get(&embedder_name);
            let was_quantized = settings_diff
                .old
                .embedding_configs
                .get(&embedder_name)
                .map_or(false, |conf| conf.2);
            let is_quantizing = embedder_config.map_or(false, |action| action.is_being_quantized);

            pool.install(|| {
                let mut writer = ArroyWrapper::new(vector_arroy, embedder_index, was_quantized);
                writer.build_and_quantize(wtxn, &mut rng, dimension, is_quantizing, cancel)?;
                Result::Ok(())
            })
            .map_err(InternalError::from)??;
        }

        self.execute_prefix_databases(
            word_docids.map(MergerBuilder::build),
            exact_word_docids.map(MergerBuilder::build),
            word_position_docids.map(MergerBuilder::build),
            word_fid_docids.map(MergerBuilder::build),
        )?;

        // This call contains an internal condition to ensure we do not always
        // generate compression dictionaries and always compress documents.
        self.manage_compression_dictionary()?;

        Ok(number_of_documents)
    }

    #[tracing::instrument(
        level = "trace",
        skip_all,
        target = "indexing::prefix",
        name = "index_documents_prefix_databases"
    )]
    pub fn execute_prefix_databases(
        &mut self,
        word_docids: Option<Merger<CursorClonableMmap, MergeDeladdCboRoaringBitmaps>>,
        exact_word_docids: Option<Merger<CursorClonableMmap, MergeDeladdCboRoaringBitmaps>>,
        word_position_docids: Option<Merger<CursorClonableMmap, MergeDeladdCboRoaringBitmaps>>,
        word_fid_docids: Option<Merger<CursorClonableMmap, MergeDeladdCboRoaringBitmaps>>,
    ) -> Result<()>
    where
        FP: Fn(UpdateIndexingStep) + Sync,
        FA: Fn() -> bool + Sync,
    {
        // Merged databases are already been indexed, we start from this count;
        let mut databases_seen = MERGED_DATABASE_COUNT;

        if (self.should_abort)() {
            return Err(Error::InternalError(InternalError::AbortedIndexation));
        }

        databases_seen += 1;
        (self.progress)(UpdateIndexingStep::MergeDataIntoFinalDatabase {
            databases_seen,
            total_databases: TOTAL_POSTING_DATABASE_COUNT,
        });

        if (self.should_abort)() {
            return Err(Error::InternalError(InternalError::AbortedIndexation));
        }

        let previous_words_prefixes_fst =
            self.index.words_prefixes_fst(self.wtxn)?.map_data(|cow| cow.into_owned())?;

        // Run the words prefixes update operation.
        let PrefixSettings { prefix_count_threshold, max_prefix_length, compute_prefixes } =
            self.index.prefix_settings(self.wtxn)?;

        // If the prefix search is enabled at indexing time, we compute the prefixes.
        if compute_prefixes == PrefixSearch::IndexingTime {
            let mut builder = WordsPrefixesFst::new(self.wtxn, self.index);
            builder.threshold(prefix_count_threshold);
            builder.max_prefix_length(max_prefix_length);
            builder.execute()?;
        } else {
            // If the prefix search is disabled at indexing time, we delete the previous words prefixes fst.
            // And all the associated docids databases.
            self.index.delete_words_prefixes_fst(self.wtxn)?;
            self.index.word_prefix_docids.clear(self.wtxn)?;
            self.index.exact_word_prefix_docids.clear(self.wtxn)?;
            self.index.word_prefix_position_docids.clear(self.wtxn)?;
            self.index.word_prefix_fid_docids.clear(self.wtxn)?;

            databases_seen += 3;
            (self.progress)(UpdateIndexingStep::MergeDataIntoFinalDatabase {
                databases_seen,
                total_databases: TOTAL_POSTING_DATABASE_COUNT,
            });

            return Ok(());
        }

        if (self.should_abort)() {
            return Err(Error::InternalError(InternalError::AbortedIndexation));
        }

        let current_prefix_fst;
        let common_prefix_fst_words_tmp;
        let common_prefix_fst_words: Vec<_>;
        let new_prefix_fst_words;
        let del_prefix_fst_words;

        {
            let span = tracing::trace_span!(target: "indexing::details", "compute_prefix_diffs");
            let _entered = span.enter();

            current_prefix_fst = self.index.words_prefixes_fst(self.wtxn)?;

            // We retrieve the common words between the previous and new prefix word fst.
            common_prefix_fst_words_tmp = fst_stream_into_vec(
                previous_words_prefixes_fst.op().add(&current_prefix_fst).intersection(),
            );
            common_prefix_fst_words = common_prefix_fst_words_tmp
                .as_slice()
                .linear_group_by_key(|x| x.chars().next().unwrap())
                .collect();

            // We retrieve the newly added words between the previous and new prefix word fst.
            new_prefix_fst_words = fst_stream_into_vec(
                current_prefix_fst.op().add(&previous_words_prefixes_fst).difference(),
            );

            // We compute the set of prefixes that are no more part of the prefix fst.
            del_prefix_fst_words = fst_stream_into_hashset(
                previous_words_prefixes_fst.op().add(&current_prefix_fst).difference(),
            );
        }

        databases_seen += 1;
        (self.progress)(UpdateIndexingStep::MergeDataIntoFinalDatabase {
            databases_seen,
            total_databases: TOTAL_POSTING_DATABASE_COUNT,
        });

        if (self.should_abort)() {
            return Err(Error::InternalError(InternalError::AbortedIndexation));
        }

        if let Some(word_docids) = word_docids {
            execute_word_prefix_docids(
                self.wtxn,
                word_docids,
                self.index.word_docids,
                self.index.word_prefix_docids,
                self.indexer_config,
                &new_prefix_fst_words,
                &common_prefix_fst_words,
                &del_prefix_fst_words,
            )?;
        }

        if let Some(exact_word_docids) = exact_word_docids {
            execute_word_prefix_docids(
                self.wtxn,
                exact_word_docids,
                self.index.exact_word_docids,
                self.index.exact_word_prefix_docids,
                self.indexer_config,
                &new_prefix_fst_words,
                &common_prefix_fst_words,
                &del_prefix_fst_words,
            )?;
        }

        if (self.should_abort)() {
            return Err(Error::InternalError(InternalError::AbortedIndexation));
        }

        databases_seen += 1;
        (self.progress)(UpdateIndexingStep::MergeDataIntoFinalDatabase {
            databases_seen,
            total_databases: TOTAL_POSTING_DATABASE_COUNT,
        });

        if let Some(word_position_docids) = word_position_docids {
            // Run the words prefix position docids update operation.
            let mut builder = WordPrefixIntegerDocids::new(
                self.wtxn,
                self.index.word_prefix_position_docids,
                self.index.word_position_docids,
            );
            builder.chunk_compression_type = self.indexer_config.chunk_compression_type;
            builder.chunk_compression_level = self.indexer_config.chunk_compression_level;
            builder.max_nb_chunks = self.indexer_config.max_nb_chunks;
            builder.max_memory = self.indexer_config.max_memory;

            builder.execute(
                word_position_docids,
                &new_prefix_fst_words,
                &common_prefix_fst_words,
                &del_prefix_fst_words,
            )?;
        }
        if let Some(word_fid_docids) = word_fid_docids {
            // Run the words prefix fid docids update operation.
            let mut builder = WordPrefixIntegerDocids::new(
                self.wtxn,
                self.index.word_prefix_fid_docids,
                self.index.word_fid_docids,
            );
            builder.chunk_compression_type = self.indexer_config.chunk_compression_type;
            builder.chunk_compression_level = self.indexer_config.chunk_compression_level;
            builder.max_nb_chunks = self.indexer_config.max_nb_chunks;
            builder.max_memory = self.indexer_config.max_memory;
            builder.execute(
                word_fid_docids,
                &new_prefix_fst_words,
                &common_prefix_fst_words,
                &del_prefix_fst_words,
            )?;
        }

        if (self.should_abort)() {
            return Err(Error::InternalError(InternalError::AbortedIndexation));
        }

        databases_seen += 1;
        (self.progress)(UpdateIndexingStep::MergeDataIntoFinalDatabase {
            databases_seen,
            total_databases: TOTAL_POSTING_DATABASE_COUNT,
        });

        Ok(())
    }

    /// Computes a new dictionay and compress the documents with it in the database.
    ///
    /// Documents still need to be directly compressed when being written in the database and a dictionary exists.
    #[tracing::instrument(
        level = "trace",
        skip_all,
        target = "indexing::compression",
        name = "compress_documents_database"
    )]
    pub fn manage_compression_dictionary(&mut self) -> Result<()> {
        /// The size of the dictionary generated from a sample of the documents already
        /// in the database. It will be used when compressing and decompressing documents.
        const COMPRESSION_DICTIONARY_SIZE: usize = 64_000;
        /// The minimum number of documents to trigger the generation of the compression dictionary.
        const COMPRESSION_ON_NUMBER_OF_DOCUMENTS: usize = 10_000;

        if self.index.number_of_documents(self.wtxn)? < COMPRESSION_ON_NUMBER_OF_DOCUMENTS as u64
            || self.index.document_compression_dictionary(self.wtxn)?.is_some()
        {
            return Ok(());
        }

        let mut sample_file = tempfile::tempfile().map(BufWriter::new)?;
        let mut sample_sizes = Vec::new();
        // TODO make this 1_000 be 10k and const
        let documents = self.index.documents.remap_types::<BEU32, Bytes>();
        for result in documents.iter(self.wtxn)?.take(COMPRESSION_ON_NUMBER_OF_DOCUMENTS) {
            let (_id, bytes) = result?;
            sample_file.write_all(bytes)?;
            sample_sizes.push(bytes.len());
        }

        let sample_file = sample_file.into_inner().map_err(|ie| ie.into_error())?;
        let sample_data = unsafe { memmap2::Mmap::map(&sample_file)? };
        let dictionary =
            zstd::dict::from_continuous(&sample_data, &sample_sizes, COMPRESSION_DICTIONARY_SIZE)?;
        self.index.put_document_compression_dictionary(self.wtxn, &dictionary)?;
        // safety: We just set the dictionary above. It must be there when we get it back.
        let dictionary = self.index.document_compression_dictionary(self.wtxn)?.unwrap();

        let mut iter = self.index.documents.iter_mut(self.wtxn)?;
        while let Some(result) = iter.next() {
            let (docid, document) = result?;
            let document = document.as_non_compressed();
            let compressed = CompressedObkvU16::with_dictionary(document, &dictionary)?;
            // safety: the compressed document is entirely owned
            unsafe {
                iter.put_current_with_options::<CompressedObkvCodec>(
                    PutFlags::empty(),
                    &docid,
                    &compressed,
                )?;
            }
        }

        Ok(())
    }
}

/// Run the word prefix docids update operation.
#[allow(clippy::too_many_arguments)]
#[tracing::instrument(
    level = "trace",
    skip_all,
    target = "indexing::prefix",
    name = "index_documents_word_prefix_docids"
)]
fn execute_word_prefix_docids(
    txn: &mut heed::RwTxn<'_>,
    merger: Merger<CursorClonableMmap, MergeDeladdCboRoaringBitmaps>,
    word_docids_db: Database<Str, CboRoaringBitmapCodec>,
    word_prefix_docids_db: Database<Str, CboRoaringBitmapCodec>,
    indexer_config: &IndexerConfig,
    new_prefix_fst_words: &[String],
    common_prefix_fst_words: &[&[String]],
    del_prefix_fst_words: &HashSet<Vec<u8>>,
) -> Result<()> {
    let mut builder = WordPrefixDocids::new(txn, word_docids_db, word_prefix_docids_db);
    builder.chunk_compression_type = indexer_config.chunk_compression_type;
    builder.chunk_compression_level = indexer_config.chunk_compression_level;
    builder.max_nb_chunks = indexer_config.max_nb_chunks;
    builder.max_memory = indexer_config.max_memory;
    builder.execute(merger, new_prefix_fst_words, common_prefix_fst_words, del_prefix_fst_words)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use big_s::S;
    use bumpalo::Bump;
    use fst::IntoStreamer;
    use heed::RwTxn;
    use maplit::hashset;

    use super::*;
    use crate::documents::mmap_from_objects;
    use crate::index::tests::TempIndex;
    use crate::index::IndexEmbeddingConfig;
    use crate::progress::Progress;
    use crate::search::TermsMatchingStrategy;
    use crate::update::new::indexer;
    use crate::update::Setting;
    use crate::{db_snap, Filter, Search, UserError};

    #[test]
    fn simple_document_replacement() {
        let index = TempIndex::new();

        // First we send 3 documents with ids from 1 to 3.
        index
            .add_documents(documents!([
                { "id": 1, "name": "kevin" },
                { "id": 2, "name": "kevina" },
                { "id": 3, "name": "benoit" }
            ]))
            .unwrap();

        // Check that there is 3 documents now.
        let rtxn = index.read_txn().unwrap();
        let count = index.number_of_documents(&rtxn).unwrap();
        assert_eq!(count, 3);
        drop(rtxn);

        // Second we send 1 document with id 1, to erase the previous ones.
        index.add_documents(documents!([ { "id": 1, "name": "updated kevin" } ])).unwrap();

        // Check that there is **always** 3 documents.
        let rtxn = index.read_txn().unwrap();
        let count = index.number_of_documents(&rtxn).unwrap();
        assert_eq!(count, 3);
        drop(rtxn);

        // Third we send 3 documents again to replace the existing ones.
        index
            .add_documents(documents!([
                { "id": 1, "name": "updated second kevin" },
                { "id": 2, "name": "updated kevina" },
                { "id": 3, "name": "updated benoit" }
            ]))
            .unwrap();

        // Check that there is **always** 3 documents.
        let rtxn = index.read_txn().unwrap();
        let count = index.number_of_documents(&rtxn).unwrap();
        assert_eq!(count, 3);
        let count = index.all_compressed_documents(&rtxn).unwrap().count();
        assert_eq!(count, 3);

        drop(rtxn);
    }

    #[test]
    fn simple_document_merge() {
        let mut index = TempIndex::new();
        let mut buffer = Vec::new();
        index.index_documents_config.update_method = IndexDocumentsMethod::UpdateDocuments;

        // First we send 3 documents with duplicate ids and
        // change the index method to merge documents.
        index
            .add_documents(documents!([
                { "id": 1, "name": "kevin" },
                { "id": 1, "name": "kevina" },
                { "id": 1, "name": "benoit" }
            ]))
            .unwrap();

        // Check that there is only 1 document now.
        let rtxn = index.read_txn().unwrap();
        let count = index.number_of_documents(&rtxn).unwrap();
        assert_eq!(count, 1);

        // Check that we get only one document from the database.
        let mut compressed_docs = index.compressed_documents(&rtxn, Some(0)).unwrap();
        assert_eq!(compressed_docs.len(), 1);
        let (id, compressed_doc) = compressed_docs.remove(0);
        assert_eq!(id, 0);
        let dictionary = index.document_decompression_dictionary(&rtxn).unwrap();
        let doc = compressed_doc
            .decompress_with_optional_dictionary(&mut buffer, dictionary.as_ref())
            .unwrap();

        // Check that this document is equal to the last one sent.
        let mut doc_iter = doc.iter();
        assert_eq!(doc_iter.next(), Some((0, &b"1"[..])));
        assert_eq!(doc_iter.next(), Some((1, &br#""benoit""#[..])));
        assert_eq!(doc_iter.next(), None);
        drop(dictionary);
        drop(rtxn);

        // Second we send 1 document with id 1, to force it to be merged with the previous one.
        index.add_documents(documents!([ { "id": 1, "age": 25 } ])).unwrap();

        // Check that there is **always** 1 document.
        let rtxn = index.read_txn().unwrap();
        let count = index.number_of_documents(&rtxn).unwrap();
        assert_eq!(count, 1);

        // Check that we get only one document from the database.
        let mut compressed_docs = index.compressed_documents(&rtxn, Some(0)).unwrap();
        assert_eq!(compressed_docs.len(), 1);
        let (id, compressed_doc) = compressed_docs.remove(0);
        assert_eq!(id, 0);
        let dictionary = index.document_decompression_dictionary(&rtxn).unwrap();
        let doc = compressed_doc
            .decompress_with_optional_dictionary(&mut buffer, dictionary.as_ref())
            .unwrap();

        // Check that this document is equal to the last one sent.
        let mut doc_iter = doc.iter();
        assert_eq!(doc_iter.next(), Some((0, &b"1"[..])));
        assert_eq!(doc_iter.next(), Some((1, &br#""benoit""#[..])));
        assert_eq!(doc_iter.next(), Some((2, &b"25"[..])));
        assert_eq!(doc_iter.next(), None);
        drop(dictionary);
        drop(rtxn);
    }

    #[test]
    fn empty_update() {
        let index = TempIndex::new();

        // First we send 0 documents and only headers.
        index.add_documents(documents!([])).unwrap();

        // Check that there is no documents.
        let rtxn = index.read_txn().unwrap();
        let count = index.number_of_documents(&rtxn).unwrap();
        assert_eq!(count, 0);
        drop(rtxn);
    }

    #[test]
    fn invalid_documents_ids() {
        let index = TempIndex::new();

        // First we send 1 document with an invalid id.
        // There is a space in the document id.
        index.add_documents(documents!([ { "id": "brume bleue", "name": "kevin" } ])).unwrap_err();

        // Then we send 1 document with a valid id.
        index.add_documents(documents!([ { "id": 32, "name": "kevin" } ])).unwrap();

        // Check that there is 1 document now.
        let rtxn = index.read_txn().unwrap();
        let count = index.number_of_documents(&rtxn).unwrap();
        assert_eq!(count, 1);
        drop(rtxn);
    }

    #[test]
    fn complex_documents() {
        let index = TempIndex::new();

        // First we send 3 documents with an id for only one of them.
        index
            .add_documents(documents!([
                { "id": 0, "name": "kevin", "object": { "key1": "value1", "key2": "value2" } },
                { "id": 1, "name": "kevina", "array": ["I", "am", "fine"] },
                { "id": 2, "name": "benoit", "array_of_object": [{ "wow": "amazing" }] }
            ]))
            .unwrap();

        // Check that there is 1 documents now.
        let rtxn = index.read_txn().unwrap();

        // Search for a sub object value
        let result = index.search(&rtxn).query(r#""value2""#).execute().unwrap();
        assert_eq!(result.documents_ids, vec![0]);

        // Search for a sub array value
        let result = index.search(&rtxn).query(r#""fine""#).execute().unwrap();
        assert_eq!(result.documents_ids, vec![1]);

        // Search for a sub array sub object key
        let result = index.search(&rtxn).query(r#""amazing""#).execute().unwrap();
        assert_eq!(result.documents_ids, vec![2]);

        drop(rtxn);
    }

    #[test]
    fn simple_documents_replace() {
        let mut index = TempIndex::new();
        index.index_documents_config.update_method = IndexDocumentsMethod::ReplaceDocuments;

        index.add_documents(documents!([
          { "id": 2,    "title": "Pride and Prejudice",                    "author": "Jane Austin",              "genre": "romance",    "price": 3.5, "_geo": { "lat": 12, "lng": 42 } },
          { "id": 456,  "title": "Le Petit Prince",                        "author": "Antoine de Saint-Exupéry", "genre": "adventure" , "price": 10.0 },
          { "id": 1,    "title": "Alice In Wonderland",                    "author": "Lewis Carroll",            "genre": "fantasy",    "price": 25.99 },
          { "id": 1344, "title": "The Hobbit",                             "author": "J. R. R. Tolkien",         "genre": "fantasy" },
          { "id": 4,    "title": "Harry Potter and the Half-Blood Prince", "author": "J. K. Rowling",            "genre": "fantasy" },
          { "id": 42,   "title": "The Hitchhiker's Guide to the Galaxy",   "author": "Douglas Adams", "_geo": { "lat": 35, "lng": 23 } }
        ])).unwrap();

        db_snap!(index, word_docids, "initial");

        index.index_documents_config.update_method = IndexDocumentsMethod::UpdateDocuments;

        index
            .add_documents(documents!([
                {"id":4,"title":"Harry Potter and the Half-Blood Princess"},
                {"id":456,"title":"The Little Prince"}
            ]))
            .unwrap();

        index
            .add_documents(documents!([
                { "id": 2, "author": "J. Austen", "date": "1813" }
            ]))
            .unwrap();

        // Check that there is **always** 6 documents.
        let rtxn = index.read_txn().unwrap();
        let count = index.number_of_documents(&rtxn).unwrap();
        assert_eq!(count, 6);
        let count = index.all_compressed_documents(&rtxn).unwrap().count();
        assert_eq!(count, 6);

        db_snap!(index, word_docids, "updated");

        drop(rtxn);
    }

    #[test]
    fn mixed_geo_documents() {
        let mut index = TempIndex::new();
        index.index_documents_config.update_method = IndexDocumentsMethod::ReplaceDocuments;

        // We send 6 documents and mix the ones that have _geo and those that don't have it.
        index
            .add_documents(documents!([
              { "id": 2, "price": 3.5, "_geo": { "lat": 12, "lng": 42 } },
              { "id": 456 },
              { "id": 1 },
              { "id": 1344 },
              { "id": 4 },
              { "id": 42, "_geo": { "lat": 35, "lng": 23 } }
            ]))
            .unwrap();

        index
            .update_settings(|settings| {
                settings.set_filterable_fields(hashset!(S("_geo")));
            })
            .unwrap();
    }

    #[test]
    fn geo_error() {
        let mut index = TempIndex::new();
        index.index_documents_config.update_method = IndexDocumentsMethod::ReplaceDocuments;

        index
            .update_settings(|settings| {
                settings.set_filterable_fields(hashset!(S("_geo")));
            })
            .unwrap();

        let error = index
            .add_documents(documents!([
              { "id": 0, "_geo": { "lng": 42 } }
            ]))
            .unwrap_err();
        assert_eq!(
            &error.to_string(),
            r#"Could not find latitude in the document with the id: `"0"`. Was expecting a `_geo.lat` field."#
        );

        let error = index
            .add_documents(documents!([
              { "id": 0, "_geo": { "lat": 42 } }
            ]))
            .unwrap_err();
        assert_eq!(
            &error.to_string(),
            r#"Could not find longitude in the document with the id: `"0"`. Was expecting a `_geo.lng` field."#
        );

        let error = index
            .add_documents(documents!([
              { "id": 0, "_geo": { "lat": "lol", "lng": 42 } }
            ]))
            .unwrap_err();
        assert_eq!(
            &error.to_string(),
            r#"Could not parse latitude in the document with the id: `"0"`. Was expecting a finite number but instead got `"lol"`."#
        );

        let error = index
            .add_documents(documents!([
              { "id": 0, "_geo": { "lat": [12, 13], "lng": 42 } }
            ]))
            .unwrap_err();
        assert_eq!(
            &error.to_string(),
            r#"Could not parse latitude in the document with the id: `"0"`. Was expecting a finite number but instead got `[12,13]`."#
        );

        let error = index
            .add_documents(documents!([
              { "id": 0, "_geo": { "lat": 12, "lng": "hello" } }
            ]))
            .unwrap_err();
        assert_eq!(
            &error.to_string(),
            r#"Could not parse longitude in the document with the id: `"0"`. Was expecting a finite number but instead got `"hello"`."#
        );
    }

    #[test]
    fn delete_documents_then_insert() {
        let index = TempIndex::new();

        index
            .add_documents(documents!([
                { "objectId": 123, "title": "Pride and Prejudice", "comment": "A great book" },
                { "objectId": 456, "title": "Le Petit Prince",     "comment": "A french book" },
                { "objectId": 1,   "title": "Alice In Wonderland", "comment": "A weird book" },
                { "objectId": 30,  "title": "Hamlet", "_geo": { "lat": 12, "lng": 89 } }
            ]))
            .unwrap();

        // Delete not all of the documents but some of them.
        index.delete_document("30");

        let txn = index.read_txn().unwrap();
        assert_eq!(index.primary_key(&txn).unwrap(), Some("objectId"));

        let external_documents_ids = index.external_documents_ids();
        assert!(external_documents_ids.get(&txn, "30").unwrap().is_none());

        index
            .add_documents(documents!([
                { "objectId": 30,  "title": "Hamlet", "_geo": { "lat": 12, "lng": 89 } }
            ]))
            .unwrap();

        let wtxn = index.write_txn().unwrap();
        let external_documents_ids = index.external_documents_ids();
        assert!(external_documents_ids.get(&wtxn, "30").unwrap().is_some());
        wtxn.commit().unwrap();

        index
            .add_documents(documents!([
                { "objectId": 30,  "title": "Hamlet", "_geo": { "lat": 12, "lng": 89 } }
            ]))
            .unwrap();
    }

    #[test]
    fn index_more_than_256_fields() {
        let index = TempIndex::new();

        let mut big_object = serde_json::Map::new();
        big_object.insert(S("id"), serde_json::Value::from("wow"));
        for i in 0..1000 {
            let key = i.to_string();
            big_object.insert(key, serde_json::Value::from("I am a text!"));
        }

        let documents = mmap_from_objects([big_object]);
        index.add_documents(documents).unwrap();
    }

    #[test]
    fn index_more_than_1000_positions_in_a_field() {
        let index = TempIndex::new_with_map_size(4096 * 100_000); // 400 MB
        let mut content = String::with_capacity(382101);
        for i in 0..=u16::MAX {
            content.push_str(&format!("{i} "));
        }
        index
            .add_documents(documents!({
                "id": "wow",
                "content": content
            }))
            .unwrap();

        let rtxn = index.read_txn().unwrap();

        assert!(index.word_docids.get(&rtxn, "0").unwrap().is_some());
        assert!(index.word_docids.get(&rtxn, "64").unwrap().is_some());
        assert!(index.word_docids.get(&rtxn, "256").unwrap().is_some());
        assert!(index.word_docids.get(&rtxn, "1024").unwrap().is_some());
        assert!(index.word_docids.get(&rtxn, "32768").unwrap().is_some());
        assert!(index.word_docids.get(&rtxn, "65535").unwrap().is_some());
    }

    #[test]
    fn index_documents_with_zeroes() {
        let index = TempIndex::new();

        index
            .add_documents(documents!([
                {
                    "id": 2,
                    "title": "Prideand Prejudice",
                    "au{hor": "Jane Austin",
                    "genre": "romance",
                    "price$": "3.5$",
                },
                {
                    "id": 456,
                    "title": "Le Petit Prince",
                    "au{hor": "Antoine de Saint-Exupéry",
                    "genre": "adventure",
                    "price$": "10.0$",
                },
                {
                    "id": 1,
                    "title": "Wonderland",
                    "au{hor": "Lewis Carroll",
                    "genre": "fantasy",
                    "price$": "25.99$",
                },
                {
                    "id": 4,
                    "title": "Harry Potter ing fantasy\0lood Prince",
                    "au{hor": "J. K. Rowling",
                    "genre": "fantasy\0",
                },
            ]))
            .unwrap();
    }

    #[test]
    fn index_documents_with_nested_fields() {
        let index = TempIndex::new();

        index
            .add_documents(documents!([
                {
                    "id": 0,
                    "title": "The zeroth document",
                },
                {
                    "id": 1,
                    "title": "The first document",
                    "nested": {
                        "object": "field",
                        "machin": "bidule",
                    },
                },
                {
                    "id": 2,
                    "title": "The second document",
                    "nested": [
                        "array",
                        {
                            "object": "field",
                        },
                        {
                            "prout": "truc",
                            "machin": "lol",
                        },
                    ],
                },
                {
                    "id": 3,
                    "title": "The third document",
                    "nested": "I lied",
                },
            ]))
            .unwrap();

        index
            .update_settings(|settings| {
                let searchable_fields = vec![S("title"), S("nested.object"), S("nested.machin")];
                settings.set_searchable_fields(searchable_fields);

                let faceted_fields = hashset!(S("title"), S("nested.object"), S("nested.machin"));
                settings.set_filterable_fields(faceted_fields);
            })
            .unwrap();

        let rtxn = index.read_txn().unwrap();

        let facets = index.faceted_fields(&rtxn).unwrap();
        assert_eq!(facets, hashset!(S("title"), S("nested.object"), S("nested.machin")));

        // testing the simple query search
        let mut search = crate::Search::new(&rtxn, &index);
        search.query("document");
        search.terms_matching_strategy(TermsMatchingStrategy::default());
        // all documents should be returned
        let crate::SearchResult { documents_ids, .. } = search.execute().unwrap();
        assert_eq!(documents_ids.len(), 4);

        search.query("zeroth");
        let crate::SearchResult { documents_ids, .. } = search.execute().unwrap();
        assert_eq!(documents_ids, vec![0]);
        search.query("first");
        let crate::SearchResult { documents_ids, .. } = search.execute().unwrap();
        assert_eq!(documents_ids, vec![1]);
        search.query("second");
        let crate::SearchResult { documents_ids, .. } = search.execute().unwrap();
        assert_eq!(documents_ids, vec![2]);
        search.query("third");
        let crate::SearchResult { documents_ids, .. } = search.execute().unwrap();
        assert_eq!(documents_ids, vec![3]);

        search.query("field");
        let crate::SearchResult { documents_ids, .. } = search.execute().unwrap();
        assert_eq!(documents_ids, vec![1, 2]);

        search.query("lol");
        let crate::SearchResult { documents_ids, .. } = search.execute().unwrap();
        assert_eq!(documents_ids, vec![2]);

        search.query("object");
        let crate::SearchResult { documents_ids, .. } = search.execute().unwrap();
        assert!(documents_ids.is_empty());

        search.query("array");
        let crate::SearchResult { documents_ids, .. } = search.execute().unwrap();
        assert!(documents_ids.is_empty()); // nested is not searchable

        search.query("lied");
        let crate::SearchResult { documents_ids, .. } = search.execute().unwrap();
        assert!(documents_ids.is_empty()); // nested is not searchable

        // testing the filters
        let mut search = crate::Search::new(&rtxn, &index);
        search.filter(crate::Filter::from_str(r#"title = "The first document""#).unwrap().unwrap());
        let crate::SearchResult { documents_ids, .. } = search.execute().unwrap();
        assert_eq!(documents_ids, vec![1]);

        search.filter(crate::Filter::from_str(r#"nested.object = field"#).unwrap().unwrap());
        let crate::SearchResult { documents_ids, .. } = search.execute().unwrap();
        assert_eq!(documents_ids, vec![1, 2]);

        search.filter(crate::Filter::from_str(r#"nested.machin = bidule"#).unwrap().unwrap());
        let crate::SearchResult { documents_ids, .. } = search.execute().unwrap();
        assert_eq!(documents_ids, vec![1]);

        search.filter(crate::Filter::from_str(r#"nested = array"#).unwrap().unwrap());
        let error = search.execute().map(|_| unreachable!()).unwrap_err(); // nested is not filterable
        assert!(matches!(error, crate::Error::UserError(crate::UserError::InvalidFilter(_))));

        search.filter(crate::Filter::from_str(r#"nested = "I lied""#).unwrap().unwrap());
        let error = search.execute().map(|_| unreachable!()).unwrap_err(); // nested is not filterable
        assert!(matches!(error, crate::Error::UserError(crate::UserError::InvalidFilter(_))));
    }

    #[test]
    fn index_documents_with_nested_primary_key() {
        let index = TempIndex::new();

        index
            .update_settings(|settings| {
                settings.set_primary_key("complex.nested.id".to_owned());
            })
            .unwrap();

        index
            .add_documents(documents!([
                {
                    "complex": {
                        "nested": {
                            "id": 0,
                        },
                    },
                    "title": "The zeroth document",
                },
                {
                    "complex.nested": {
                        "id": 1,
                    },
                    "title": "The first document",
                },
                {
                    "complex": {
                        "nested.id": 2,
                    },
                    "title": "The second document",
                },
                {
                    "complex.nested.id": 3,
                    "title": "The third document",
                },
            ]))
            .unwrap();

        let rtxn = index.read_txn().unwrap();

        // testing the simple query search
        let mut search = crate::Search::new(&rtxn, &index);
        search.query("document");
        search.terms_matching_strategy(TermsMatchingStrategy::default());
        // all documents should be returned
        let crate::SearchResult { documents_ids, .. } = search.execute().unwrap();
        assert_eq!(documents_ids.len(), 4);

        search.query("zeroth");
        let crate::SearchResult { documents_ids, .. } = search.execute().unwrap();
        assert_eq!(documents_ids, vec![0]);
        search.query("first");
        let crate::SearchResult { documents_ids, .. } = search.execute().unwrap();
        assert_eq!(documents_ids, vec![1]);
        search.query("second");
        let crate::SearchResult { documents_ids, .. } = search.execute().unwrap();
        assert_eq!(documents_ids, vec![2]);
        search.query("third");
        let crate::SearchResult { documents_ids, .. } = search.execute().unwrap();
        assert_eq!(documents_ids, vec![3]);
    }

    #[test]
    fn retrieve_a_b_nested_document_id() {
        let index = TempIndex::new();

        index
            .update_settings(|settings| {
                settings.set_primary_key("a.b".to_owned());
            })
            .unwrap();

        // There must be an issue with the primary key no present in the given document
        index.add_documents(documents!({ "a" : { "b" : { "c" :  1 }}})).unwrap_err();
    }

    #[test]
    fn retrieve_a_b_c_nested_document_id() {
        let index = TempIndex::new();

        index
            .update_settings(|settings| {
                settings.set_primary_key("a.b.c".to_owned());
            })
            .unwrap();
        index.add_documents(documents!({ "a" : { "b" : { "c" :  1 }}})).unwrap();

        let rtxn = index.read_txn().unwrap();
        let all_documents_count = index.all_compressed_documents(&rtxn).unwrap().count();
        assert_eq!(all_documents_count, 1);
        let external_documents_ids = index.external_documents_ids();
        assert!(external_documents_ids.get(&rtxn, "1").unwrap().is_some());
    }

    #[test]
    fn test_facets_generation() {
        let index = TempIndex::new();

        index
            .add_documents(documents!([
                {
                    "id": 0,
                    "dog": {
                        "race": {
                            "bernese mountain": "zeroth",
                        },
                    },
                },
                {
                    "id": 1,
                    "dog.race": {
                        "bernese mountain": "first",
                    },
                },
                {
                    "id": 2,
                    "dog.race.bernese mountain": "second",
                },
                {
                    "id": 3,
                    "dog": {
                        "race.bernese mountain": "third"
                    },
                },
            ]))
            .unwrap();

        index
            .update_settings(|settings| {
                settings.set_filterable_fields(hashset!(String::from("dog")));
            })
            .unwrap();

        db_snap!(index, facet_id_string_docids, @r###"
        3   0  first        1  [1, ]
        3   0  second       1  [2, ]
        3   0  third        1  [3, ]
        3   0  zeroth       1  [0, ]
        "###);
        db_snap!(index, field_id_docid_facet_strings, @r###"
        3   0    zeroth       zeroth
        3   1    first        first
        3   2    second       second
        3   3    third        third
        "###);

        let rtxn = index.read_txn().unwrap();

        let hidden = index.faceted_fields(&rtxn).unwrap();

        assert_eq!(hidden, hashset!(S("dog"), S("dog.race"), S("dog.race.bernese mountain")));

        for (s, i) in [("zeroth", 0), ("first", 1), ("second", 2), ("third", 3)] {
            let mut search = crate::Search::new(&rtxn, &index);
            let filter = format!(r#""dog.race.bernese mountain" = {s}"#);
            search.filter(crate::Filter::from_str(&filter).unwrap().unwrap());
            let crate::SearchResult { documents_ids, .. } = search.execute().unwrap();
            assert_eq!(documents_ids, vec![i]);
        }
        // Reset the settings
        index
            .update_settings(|settings| {
                settings.reset_filterable_fields();
            })
            .unwrap();

        db_snap!(index, facet_id_string_docids, @"");
        db_snap!(index, field_id_docid_facet_strings, @"");

        let rtxn = index.read_txn().unwrap();

        let facets = index.faceted_fields(&rtxn).unwrap();

        assert_eq!(facets, hashset!());

        // update the settings to test the sortable
        index
            .update_settings(|settings| {
                settings.set_sortable_fields(hashset!(S("dog.race")));
            })
            .unwrap();

        db_snap!(index, facet_id_string_docids, @r###"
        3   0  first        1  [1, ]
        3   0  second       1  [2, ]
        3   0  third        1  [3, ]
        3   0  zeroth       1  [0, ]
        "###);
        db_snap!(index, field_id_docid_facet_strings, @r###"
        3   0    zeroth       zeroth
        3   1    first        first
        3   2    second       second
        3   3    third        third
        "###);

        let rtxn = index.read_txn().unwrap();

        let facets = index.faceted_fields(&rtxn).unwrap();

        assert_eq!(facets, hashset!(S("dog.race"), S("dog.race.bernese mountain")));

        let mut search = crate::Search::new(&rtxn, &index);
        search.sort_criteria(vec![crate::AscDesc::Asc(crate::Member::Field(S(
            "dog.race.bernese mountain",
        )))]);
        let crate::SearchResult { documents_ids, .. } = search.execute().unwrap();
        assert_eq!(documents_ids, vec![1, 2, 3, 0]);
    }

    #[test]
    fn index_2_times_documents_split_by_zero_document_indexation() {
        let index = TempIndex::new();

        index
            .add_documents(documents!([
                {"id": 0, "name": "Kerollmops", "score": 78},
                {"id": 1, "name": "ManyTheFish", "score": 75},
                {"id": 2, "name": "Ferdi", "score": 39},
                {"id": 3, "name": "Tommy", "score": 33}
            ]))
            .unwrap();

        // Check that there is 4 document now.
        let rtxn = index.read_txn().unwrap();
        let count = index.number_of_documents(&rtxn).unwrap();
        assert_eq!(count, 4);

        index.add_documents(documents!([])).unwrap();

        // Check that there is 4 document now.
        let rtxn = index.read_txn().unwrap();
        let count = index.number_of_documents(&rtxn).unwrap();
        assert_eq!(count, 4);

        index
            .add_documents(documents!([
                {"id": 0, "name": "Kerollmops", "score": 78},
                {"id": 1, "name": "ManyTheFish", "score": 75},
                {"id": 2, "name": "Ferdi", "score": 39},
                {"id": 3, "name": "Tommy", "score": 33}
            ]))
            .unwrap();

        // Check that there is 4 document now.
        let rtxn = index.read_txn().unwrap();
        let count = index.number_of_documents(&rtxn).unwrap();
        assert_eq!(count, 4);
    }

    #[cfg(feature = "chinese")]
    #[test]
    fn test_meilisearch_1714() {
        let index = TempIndex::new();

        index
            .add_documents(documents!([
              {"id": "123", "title": "小化妆包" },
              {"id": "456", "title": "Ipad 包" }
            ]))
            .unwrap();

        let rtxn = index.read_txn().unwrap();

        // Only the first document should match.
        let count = index.word_docids.get(&rtxn, "huàzhuāngbāo").unwrap().unwrap().len();
        assert_eq!(count, 1);

        // Only the second document should match.
        let count = index.word_docids.get(&rtxn, "bāo").unwrap().unwrap().len();
        assert_eq!(count, 1);

        let mut search = crate::Search::new(&rtxn, &index);
        search.query("化妆包");
        search.terms_matching_strategy(TermsMatchingStrategy::default());

        // only 1 document should be returned
        let crate::SearchResult { documents_ids, .. } = search.execute().unwrap();
        assert_eq!(documents_ids.len(), 1);
    }

    /// We try to index documents with words that are too long here,
    /// it should not return any error.
    #[test]
    fn text_with_too_long_words() {
        let index = TempIndex::new();

        index
            .add_documents(documents!([
              {"id": 1, "title": "a".repeat(256) },
              {"id": 2, "title": "b".repeat(512) },
              {"id": 3, "title": format!("{} {}", "c".repeat(250), "d".repeat(250)) },
            ]))
            .unwrap();
    }

    #[test]
    fn text_with_too_long_keys() {
        let index = TempIndex::new();
        let script = "https://bug.example.com/meilisearch/milli.saml2?ROLE=Programmer-1337&SAMLRequest=Cy1ytcZT1Po%2L2IY2y9Unru8rgnW4qWfPiI0EpT7P8xjJV8PeQikRL%2E8D9A4pj9tmbymbQCQwGmGjPMK7qwXFPX4DH52JO2b7n6TXjuR7zkIFuYdzdY2rwRNBPgCL7ihclEm9zyIjKZQ%2JTqiwfXxWjnI0KEYQYHdwd6Q%2Fx%28BDLNsvmL54CCY2F4RWeRs4eqWfn%2EHqxlhreFzax4AiQ2tgOtV5thOaaWqrhZD%2Py70nuyZWNTKwciGI43AoHg6PThANsQ5rAY5amzN%2ufbs1swETUXlLZuOut5YGpYPZfY6STJWNp4QYSUOUXBZpdElYsH7UHZ7VhJycgyt%28aTK0GW6GbKne2tJM0hgSczOqndg6RFa9WsnSBi4zMcaEfYur4WlSsHDYInF9ROousKqVMZ6H8%2gbUissaLh1eXRGo8KEJbyEHbhVVKGD%28kx4cfKjx9fT3pkeDTdvDrVn25jIzi9wHyt9l1lWc8ICnCvXCVUPP%2BjBG4wILR29gMV9Ux2QOieQm2%2Fycybhr8sBGCl30mHC7blvWt%2T3mrCHQoS3VK49PZNPqBZO9C7vOjOWoszNkJx4QckWV%2FZFvbpzUUkiBiehr9F%2FvQSxz9lzv68GwbTu9fr638p%2FQM%3D&RelayState=https%3A%2F%example.bug.com%2Fde&SigAlg=http%3A%2F%2Fwww.w3.org%2F2000%2F09%2Fxmldsig%23rsa-sha1&Signature=AZFpkhFFII7PodiewTovaGnLQKUVZp0qOCCcBIUkJ6P5by3lE3Lldj9pKaFu4wz4j%2B015HEhDvF0LlAmwwES85vdGh%2FpD%2cIQPRUEjdCbQkQDd3dy1mMXbpXxSe4QYcv9Ni7tqNTQxekpO1gE7rtg6zC66EU55uM9aj9abGQ034Vly%2F6IJ08bvAq%2B%2FB9KruLstuiNWnlXTfNGsOxGLK7%2BXr94LTkat8m%2FMan6Qr95%2KeR5TmmqaQIE4N9H6o4TopT7mXr5CF2Z3";

        // Create 200 documents with a long text
        let content = {
            let documents_iter = (0..200i32)
                .map(|i| serde_json::json!({ "id": i, "script": script }))
                .filter_map(|json| match json {
                    serde_json::Value::Object(object) => Some(object),
                    _ => None,
                });
            mmap_from_objects(documents_iter)
        };
        // Index those 200 long documents
        index.add_documents(content).unwrap();

        // Index one long document
        index
            .add_documents(documents!([
              {"id": 400, "script": script },
            ]))
            .unwrap();
    }

    #[test]
    fn index_documents_in_multiple_transforms() {
        let index = TempIndex::new();

        let doc1 = documents! {[{
            "id": 228142,
            "title": "asdsad",
            "state": "automated",
            "priority": "normal",
            "public_uid": "37ccf021",
            "project_id": 78207,
            "branch_id_number": 0
        }]};

        let doc2 = documents! {[{
            "id": 228143,
            "title": "something",
            "state": "automated",
            "priority": "normal",
            "public_uid": "39c6499b",
            "project_id": 78207,
            "branch_id_number": 0
        }]};

        {
            let mut wtxn = index.write_txn().unwrap();
            index.put_primary_key(&mut wtxn, "id").unwrap();
            wtxn.commit().unwrap();
        }

        index.add_documents(doc1).unwrap();
        index.add_documents(doc2).unwrap();

        let rtxn = index.read_txn().unwrap();

        let map = index.external_documents_ids().to_hash_map(&rtxn).unwrap();
        let ids = map.values().collect::<HashSet<_>>();

        assert_eq!(ids.len(), map.len());
    }

    #[test]
    fn index_documents_check_exists_database() {
        let content = || {
            documents!([
                {
                    "id": 0,
                    "colour": 0,
                },
                {
                    "id": 1,
                    "colour": []
                },
                {
                    "id": 2,
                    "colour": {}
                },
                {
                    "id": 3,
                    "colour": null
                },
                {
                    "id": 4,
                    "colour": [1]
                },
                {
                    "id": 5
                },
                {
                    "id": 6,
                    "colour": {
                        "green": 1
                    }
                },
                {
                    "id": 7,
                    "colour": {
                        "green": {
                            "blue": []
                        }
                    }
                }
            ])
        };

        let check_ok = |index: &Index| {
            let rtxn = index.read_txn().unwrap();
            let facets = index.faceted_fields(&rtxn).unwrap();
            assert_eq!(facets, hashset!(S("colour"), S("colour.green"), S("colour.green.blue")));

            let colour_id = index.fields_ids_map(&rtxn).unwrap().id("colour").unwrap();
            let colour_green_id = index.fields_ids_map(&rtxn).unwrap().id("colour.green").unwrap();
            let colour_green_blue_id =
                index.fields_ids_map(&rtxn).unwrap().id("colour.green.blue").unwrap();

            let bitmap_colour =
                index.facet_id_exists_docids.get(&rtxn, &colour_id).unwrap().unwrap();
            assert_eq!(bitmap_colour.into_iter().collect::<Vec<_>>(), vec![0, 1, 2, 3, 4, 6, 7]);

            let bitmap_colour_green =
                index.facet_id_exists_docids.get(&rtxn, &colour_green_id).unwrap().unwrap();
            assert_eq!(bitmap_colour_green.into_iter().collect::<Vec<_>>(), vec![6, 7]);

            let bitmap_colour_blue =
                index.facet_id_exists_docids.get(&rtxn, &colour_green_blue_id).unwrap().unwrap();
            assert_eq!(bitmap_colour_blue.into_iter().collect::<Vec<_>>(), vec![7]);
        };

        let faceted_fields = hashset!(S("colour"));

        let index = TempIndex::new();
        index.add_documents(content()).unwrap();
        index
            .update_settings(|settings| {
                settings.set_filterable_fields(faceted_fields.clone());
            })
            .unwrap();
        check_ok(&index);

        let index = TempIndex::new();
        index
            .update_settings(|settings| {
                settings.set_filterable_fields(faceted_fields.clone());
            })
            .unwrap();
        index.add_documents(content()).unwrap();
        check_ok(&index);
    }

    #[test]
    fn index_documents_check_is_null_database() {
        let content = || {
            documents!([
                {
                    "id": 0,
                    "colour": null,
                },
                {
                    "id": 1,
                    "colour": [null], // must not be returned
                },
                {
                    "id": 6,
                    "colour": {
                        "green": null
                    }
                },
                {
                    "id": 7,
                    "colour": {
                        "green": {
                            "blue": null
                        }
                    }
                },
                {
                    "id": 8,
                    "colour": 0,
                },
                {
                    "id": 9,
                    "colour": []
                },
                {
                    "id": 10,
                    "colour": {}
                },
                {
                    "id": 12,
                    "colour": [1]
                },
                {
                    "id": 13
                },
                {
                    "id": 14,
                    "colour": {
                        "green": 1
                    }
                },
                {
                    "id": 15,
                    "colour": {
                        "green": {
                            "blue": []
                        }
                    }
                }
            ])
        };

        let check_ok = |index: &Index| {
            let rtxn = index.read_txn().unwrap();
            let facets = index.faceted_fields(&rtxn).unwrap();
            assert_eq!(facets, hashset!(S("colour"), S("colour.green"), S("colour.green.blue")));

            let colour_id = index.fields_ids_map(&rtxn).unwrap().id("colour").unwrap();
            let colour_green_id = index.fields_ids_map(&rtxn).unwrap().id("colour.green").unwrap();
            let colour_blue_id =
                index.fields_ids_map(&rtxn).unwrap().id("colour.green.blue").unwrap();

            let bitmap_null_colour =
                index.facet_id_is_null_docids.get(&rtxn, &colour_id).unwrap().unwrap();
            assert_eq!(bitmap_null_colour.into_iter().collect::<Vec<_>>(), vec![0]);

            let bitmap_colour_green =
                index.facet_id_is_null_docids.get(&rtxn, &colour_green_id).unwrap().unwrap();
            assert_eq!(bitmap_colour_green.into_iter().collect::<Vec<_>>(), vec![2]);

            let bitmap_colour_blue =
                index.facet_id_is_null_docids.get(&rtxn, &colour_blue_id).unwrap().unwrap();
            assert_eq!(bitmap_colour_blue.into_iter().collect::<Vec<_>>(), vec![3]);
        };

        let faceted_fields = hashset!(S("colour"));

        let index = TempIndex::new();
        index.add_documents(content()).unwrap();
        index
            .update_settings(|settings| {
                settings.set_filterable_fields(faceted_fields.clone());
            })
            .unwrap();
        check_ok(&index);

        let index = TempIndex::new();
        index
            .update_settings(|settings| {
                settings.set_filterable_fields(faceted_fields.clone());
            })
            .unwrap();
        index.add_documents(content()).unwrap();
        check_ok(&index);
    }

    #[test]
    fn index_documents_check_is_empty_database() {
        let content = || {
            documents!([
                {"id": 0, "tags": null },
                {"id": 1, "tags": [null] },
                {"id": 2, "tags": [] },
                {"id": 3, "tags": ["hello","world"] },
                {"id": 4, "tags": [""] },
                {"id": 5 },
                {"id": 6, "tags": {} },
                {"id": 7, "tags": {"green": "cool"} },
                {"id": 8, "tags": {"green": ""} },
                {"id": 9, "tags": "" },
                {"id": 10, "tags": { "green": null } },
                {"id": 11, "tags": { "green": { "blue": null } } },
                {"id": 12, "tags": { "green": { "blue": [] } } }
            ])
        };

        let check_ok = |index: &Index| {
            let rtxn = index.read_txn().unwrap();
            let facets = index.faceted_fields(&rtxn).unwrap();
            assert_eq!(facets, hashset!(S("tags"), S("tags.green"), S("tags.green.blue")));

            let tags_id = index.fields_ids_map(&rtxn).unwrap().id("tags").unwrap();
            let tags_green_id = index.fields_ids_map(&rtxn).unwrap().id("tags.green").unwrap();
            let tags_blue_id = index.fields_ids_map(&rtxn).unwrap().id("tags.green.blue").unwrap();

            let bitmap_empty_tags =
                index.facet_id_is_empty_docids.get(&rtxn, &tags_id).unwrap().unwrap();
            assert_eq!(bitmap_empty_tags.into_iter().collect::<Vec<_>>(), vec![2, 6, 9]);

            let bitmap_tags_green =
                index.facet_id_is_empty_docids.get(&rtxn, &tags_green_id).unwrap().unwrap();
            assert_eq!(bitmap_tags_green.into_iter().collect::<Vec<_>>(), vec![8]);

            let bitmap_tags_blue =
                index.facet_id_is_empty_docids.get(&rtxn, &tags_blue_id).unwrap().unwrap();
            assert_eq!(bitmap_tags_blue.into_iter().collect::<Vec<_>>(), vec![12]);
        };

        let faceted_fields = hashset!(S("tags"));

        let index = TempIndex::new();
        index.add_documents(content()).unwrap();
        index
            .update_settings(|settings| {
                settings.set_filterable_fields(faceted_fields.clone());
            })
            .unwrap();
        check_ok(&index);

        let index = TempIndex::new();
        index
            .update_settings(|settings| {
                settings.set_filterable_fields(faceted_fields.clone());
            })
            .unwrap();
        index.add_documents(content()).unwrap();
        check_ok(&index);
    }

    #[test]
    fn primary_key_must_not_contain_floats() {
        let index = TempIndex::new_with_map_size(4096 * 100);

        let doc1 = documents! {[{
            "id": -228142,
            "title": "asdsad",
        }]};

        let doc2 = documents! {[{
            "id": 228143.56,
            "title": "something",
        }]};

        let doc3 = documents! {[{
            "id": -228143.56,
            "title": "something",
        }]};

        let doc4 = documents! {[{
            "id": 2.0,
            "title": "something",
        }]};

        let rtxn = index.inner.read_txn().unwrap();
        let db_fields_ids_map = index.inner.fields_ids_map(&rtxn).unwrap();
        let mut new_fields_ids_map = db_fields_ids_map.clone();

        let mut indexer = indexer::DocumentOperation::new(IndexDocumentsMethod::ReplaceDocuments);
        indexer.add_documents(&doc1).unwrap();
        indexer.add_documents(&doc2).unwrap();
        indexer.add_documents(&doc3).unwrap();
        indexer.add_documents(&doc4).unwrap();

        let indexer_alloc = Bump::new();
        let (_document_changes, operation_stats, _primary_key) = indexer
            .into_changes(
                &indexer_alloc,
                &index.inner,
                &rtxn,
                None,
                &mut new_fields_ids_map,
                &|| false,
                Progress::default(),
            )
            .unwrap();

        assert_eq!(operation_stats.iter().filter(|ps| ps.error.is_none()).count(), 1);
        assert_eq!(operation_stats.iter().filter(|ps| ps.error.is_some()).count(), 3);
    }

    #[test]
    fn primary_key_must_not_contain_whitespace() {
        let index = TempIndex::new();

        let doc1 = documents! {[{
            "id": " 1",
            "title": "asdsad",
        }]};

        let doc2 = documents! {[{
            "id": "\t2",
            "title": "something",
        }]};

        let doc3 = documents! {[{
            "id": "\r3",
            "title": "something",
        }]};

        let doc4 = documents! {[{
            "id": "\n4",
            "title": "something",
        }]};

        index.add_documents(doc1).unwrap_err();
        index.add_documents(doc2).unwrap_err();
        index.add_documents(doc3).unwrap_err();
        index.add_documents(doc4).unwrap_err();
    }

    #[test]
    fn primary_key_inference() {
        let index = TempIndex::new();

        let doc_no_id = documents! {[{
            "title": "asdsad",
            "state": "automated",
            "priority": "normal",
            "branch_id_number": 0
        }]};
        assert!(matches!(
            index.add_documents(doc_no_id),
            Err(Error::UserError(UserError::NoPrimaryKeyCandidateFound))
        ));

        let doc_multiple_ids = documents! {[{
            "id": 228143,
            "title": "something",
            "state": "automated",
            "priority": "normal",
            "public_uid": "39c6499b",
            "project_id": 78207,
            "branch_id_number": 0
        }]};

        let Err(Error::UserError(UserError::MultiplePrimaryKeyCandidatesFound { candidates })) =
            index.add_documents(doc_multiple_ids)
        else {
            panic!("Expected Error::UserError(MultiplePrimaryKeyCandidatesFound)")
        };

        assert_eq!(candidates, vec![S("id"), S("project_id"), S("public_uid"),]);

        let doc_inferable = documents! {[{
            "video": "test.mp4",
            "id": 228143,
            "title": "something",
            "state": "automated",
            "priority": "normal",
            "public_uid_": "39c6499b",
            "project_id_": 78207,
            "branch_id_number": 0
        }]};

        index.add_documents(doc_inferable).unwrap();

        let txn = index.read_txn().unwrap();

        assert_eq!(index.primary_key(&txn).unwrap().unwrap(), "id");
    }

    #[test]
    fn long_words_must_be_skipped() {
        let index = TempIndex::new();

        // this is obviousy too long
        let long_word = "lol".repeat(1000);
        let doc1 = documents! {[{
            "id": "1",
            "title": long_word,
        }]};

        index.add_documents(doc1).unwrap();

        let rtxn = index.read_txn().unwrap();
        let words_fst = index.words_fst(&rtxn).unwrap();
        assert!(!words_fst.contains(&long_word));
    }

    #[test]
    fn long_facet_values_must_not_crash() {
        let index = TempIndex::new();

        // this is obviousy too long
        let long_word = "lol".repeat(1000);
        let doc1 = documents! {[{
            "id": "1",
            "title": long_word,
        }]};

        index
            .update_settings(|settings| {
                settings.set_filterable_fields(hashset! { S("title") });
            })
            .unwrap();

        index.add_documents(doc1).unwrap();
    }

    #[cfg(feature = "default")]
    #[test]
    fn store_detected_script_and_language_per_document_during_indexing() {
        use charabia::{Language, Script};
        let index = TempIndex::new();
        index
            .add_documents(documents!([
                { "id": 1, "title": "The quick (\"brown\") fox can't jump 32.3 feet, right? Brr, it's 29.3°F!" },
                { "id": 2, "title": "人人生而自由﹐在尊嚴和權利上一律平等。他們賦有理性和良心﹐並應以兄弟關係的精神互相對待。" },
                { "id": 3, "title": "הַשּׁוּעָל הַמָּהִיר (״הַחוּם״) לֹא יָכוֹל לִקְפֹּץ 9.94 מֶטְרִים, נָכוֹן? ברר, 1.5°C- בַּחוּץ!" },
                { "id": 4, "title": "関西国際空港限定トートバッグ すもももももももものうち" },
                { "id": 5, "title": "ภาษาไทยง่ายนิดเดียว" },
                { "id": 6, "title": "The quick 在尊嚴和權利上一律平等。" },
            ]))
            .unwrap();

        let rtxn = index.read_txn().unwrap();
        let key_jpn = (Script::Cj, Language::Jpn);
        let key_cmn = (Script::Cj, Language::Cmn);
        let cj_jpn_docs = index.script_language_documents_ids(&rtxn, &key_jpn).unwrap().unwrap();
        let cj_cmn_docs = index.script_language_documents_ids(&rtxn, &key_cmn).unwrap().unwrap();
        let expected_cj_jpn_docids = [3].iter().collect();
        assert_eq!(cj_jpn_docs, expected_cj_jpn_docids);
        let expected_cj_cmn_docids = [1, 5].iter().collect();
        assert_eq!(cj_cmn_docs, expected_cj_cmn_docids);
    }

    #[test]
    fn add_and_delete_documents_in_single_transform() {
        let mut index = TempIndex::new();
        index.index_documents_config.update_method = IndexDocumentsMethod::UpdateDocuments;

        let mut wtxn = index.write_txn().unwrap();
        let indexer_config = &index.indexer_config;
        let rtxn = index.inner.read_txn().unwrap();
        let db_fields_ids_map = index.inner.fields_ids_map(&rtxn).unwrap();
        let mut new_fields_ids_map = db_fields_ids_map.clone();

        let documents = documents!([
            { "id": 1, "doggo": "kevin" },
            { "id": 2, "doggo": { "name": "bob", "age": 20 } },
            { "id": 3, "name": "jean", "age": 25 },
        ]);

        let indexer_alloc = Bump::new();
        let embedders = EmbeddingConfigs::default();
        let mut indexer = indexer::DocumentOperation::new(IndexDocumentsMethod::ReplaceDocuments);
        indexer.add_documents(&documents).unwrap();
        indexer.delete_documents(&["2"]);
        let (document_changes, _operation_stats, primary_key) = indexer
            .into_changes(
                &indexer_alloc,
                &index.inner,
                &rtxn,
                None,
                &mut new_fields_ids_map,
                &|| false,
                Progress::default(),
            )
            .unwrap();

        indexer::index(
            &mut wtxn,
            &index.inner,
            &crate::ThreadPoolNoAbortBuilder::new().build().unwrap(),
            indexer_config.grenad_parameters(),
            &db_fields_ids_map,
            new_fields_ids_map,
            primary_key,
            &document_changes,
            embedders,
            &|| false,
            &Progress::default(),
        )
        .unwrap();
        wtxn.commit().unwrap();

        db_snap!(index, documents, @r###"
        {"id":1,"doggo":"kevin"}
        {"id":3,"name":"jean","age":25}
        "###);
    }

    #[test]
    fn add_update_and_delete_documents_in_single_transform() {
        let mut index = TempIndex::new();
        index.index_documents_config.update_method = IndexDocumentsMethod::UpdateDocuments;

        let mut wtxn = index.write_txn().unwrap();
        let indexer_config = &index.indexer_config;
        let rtxn = index.inner.read_txn().unwrap();
        let db_fields_ids_map = index.inner.fields_ids_map(&rtxn).unwrap();
        let mut new_fields_ids_map = db_fields_ids_map.clone();

        let documents = documents!([
            { "id": 1, "doggo": "kevin" },
            { "id": 2, "doggo": { "name": "bob", "age": 20 } },
            { "id": 3, "name": "jean", "age": 25 },
        ]);
        let mut indexer = indexer::DocumentOperation::new(IndexDocumentsMethod::UpdateDocuments);
        indexer.add_documents(&documents).unwrap();

        let documents = documents!([
            { "id": 2, "catto": "jorts" },
            { "id": 3, "legs": 4 },
        ]);
        indexer.add_documents(&documents).unwrap();
        indexer.delete_documents(&["1", "2"]);

        let indexer_alloc = Bump::new();
        let embedders = EmbeddingConfigs::default();
        let (document_changes, _operation_stats, primary_key) = indexer
            .into_changes(
                &indexer_alloc,
                &index.inner,
                &rtxn,
                None,
                &mut new_fields_ids_map,
                &|| false,
                Progress::default(),
            )
            .unwrap();

        indexer::index(
            &mut wtxn,
            &index.inner,
            &crate::ThreadPoolNoAbortBuilder::new().build().unwrap(),
            indexer_config.grenad_parameters(),
            &db_fields_ids_map,
            new_fields_ids_map,
            primary_key,
            &document_changes,
            embedders,
            &|| false,
            &Progress::default(),
        )
        .unwrap();
        wtxn.commit().unwrap();

        db_snap!(index, documents, @r###"
        {"id":3,"name":"jean","age":25,"legs":4}
        "###);
    }

    #[test]
    fn add_document_and_in_another_transform_update_and_delete_documents() {
        let index = TempIndex::new();

        let mut wtxn = index.write_txn().unwrap();
        let indexer_config = &index.indexer_config;
        let rtxn = index.inner.read_txn().unwrap();
        let db_fields_ids_map = index.inner.fields_ids_map(&rtxn).unwrap();
        let mut new_fields_ids_map = db_fields_ids_map.clone();

        let documents = documents!([
            { "id": 1, "doggo": "kevin" },
            { "id": 2, "doggo": { "name": "bob", "age": 20 } },
            { "id": 3, "name": "jean", "age": 25 },
        ]);
        let indexer_alloc = Bump::new();
        let embedders = EmbeddingConfigs::default();
        let mut indexer = indexer::DocumentOperation::new(IndexDocumentsMethod::UpdateDocuments);
        indexer.add_documents(&documents).unwrap();

        let (document_changes, _operation_stats, primary_key) = indexer
            .into_changes(
                &indexer_alloc,
                &index.inner,
                &rtxn,
                None,
                &mut new_fields_ids_map,
                &|| false,
                Progress::default(),
            )
            .unwrap();

        indexer::index(
            &mut wtxn,
            &index.inner,
            &crate::ThreadPoolNoAbortBuilder::new().build().unwrap(),
            indexer_config.grenad_parameters(),
            &db_fields_ids_map,
            new_fields_ids_map,
            primary_key,
            &document_changes,
            embedders,
            &|| false,
            &Progress::default(),
        )
        .unwrap();
        wtxn.commit().unwrap();

        db_snap!(index, documents, @r###"
        {"id":1,"doggo":"kevin"}
        {"id":2,"doggo":{"name":"bob","age":20}}
        {"id":3,"name":"jean","age":25}
        "###);

        // A first batch of documents has been inserted

        let mut wtxn = index.write_txn().unwrap();
        let indexer_config = &index.indexer_config;
        let rtxn = index.inner.read_txn().unwrap();
        let db_fields_ids_map = index.inner.fields_ids_map(&rtxn).unwrap();
        let mut new_fields_ids_map = db_fields_ids_map.clone();

        let documents = documents!([
            { "id": 2, "catto": "jorts" },
            { "id": 3, "legs": 4 },
        ]);
        let indexer_alloc = Bump::new();
        let embedders = EmbeddingConfigs::default();
        let mut indexer = indexer::DocumentOperation::new(IndexDocumentsMethod::UpdateDocuments);
        indexer.add_documents(&documents).unwrap();
        indexer.delete_documents(&["1", "2"]);

        let (document_changes, _operation_stats, primary_key) = indexer
            .into_changes(
                &indexer_alloc,
                &index.inner,
                &rtxn,
                None,
                &mut new_fields_ids_map,
                &|| false,
                Progress::default(),
            )
            .unwrap();

        indexer::index(
            &mut wtxn,
            &index.inner,
            &crate::ThreadPoolNoAbortBuilder::new().build().unwrap(),
            indexer_config.grenad_parameters(),
            &db_fields_ids_map,
            new_fields_ids_map,
            primary_key,
            &document_changes,
            embedders,
            &|| false,
            &Progress::default(),
        )
        .unwrap();
        wtxn.commit().unwrap();

        db_snap!(index, documents, @r###"
        {"id":3,"name":"jean","age":25,"legs":4}
        "###);
    }

    #[test]
    fn delete_document_and_then_add_documents_in_the_same_transform() {
        let index = TempIndex::new();

        let mut wtxn = index.write_txn().unwrap();
        let indexer_config = &index.indexer_config;
        let rtxn = index.inner.read_txn().unwrap();
        let db_fields_ids_map = index.inner.fields_ids_map(&rtxn).unwrap();
        let mut new_fields_ids_map = db_fields_ids_map.clone();

        let indexer_alloc = Bump::new();
        let embedders = EmbeddingConfigs::default();
        let mut indexer = indexer::DocumentOperation::new(IndexDocumentsMethod::UpdateDocuments);
        indexer.delete_documents(&["1", "2"]);

        let documents = documents!([
            { "id": 2, "doggo": { "name": "jean", "age": 20 } },
            { "id": 3, "name": "bob", "age": 25 },
        ]);
        indexer.add_documents(&documents).unwrap();

        let (document_changes, _operation_stats, primary_key) = indexer
            .into_changes(
                &indexer_alloc,
                &index.inner,
                &rtxn,
                None,
                &mut new_fields_ids_map,
                &|| false,
                Progress::default(),
            )
            .unwrap();

        indexer::index(
            &mut wtxn,
            &index.inner,
            &crate::ThreadPoolNoAbortBuilder::new().build().unwrap(),
            indexer_config.grenad_parameters(),
            &db_fields_ids_map,
            new_fields_ids_map,
            primary_key,
            &document_changes,
            embedders,
            &|| false,
            &Progress::default(),
        )
        .unwrap();
        wtxn.commit().unwrap();

        db_snap!(index, documents, @r###"
        {"id":2,"doggo":{"name":"jean","age":20}}
        {"id":3,"name":"bob","age":25}
        "###);
    }

    #[test]
    fn delete_the_same_document_multiple_time() {
        let index = TempIndex::new();

        let mut wtxn = index.write_txn().unwrap();
        let indexer_config = &index.indexer_config;
        let rtxn = index.inner.read_txn().unwrap();
        let db_fields_ids_map = index.inner.fields_ids_map(&rtxn).unwrap();
        let mut new_fields_ids_map = db_fields_ids_map.clone();

        let indexer_alloc = Bump::new();
        let embedders = EmbeddingConfigs::default();
        let mut indexer = indexer::DocumentOperation::new(IndexDocumentsMethod::UpdateDocuments);

        indexer.delete_documents(&["1", "2", "1", "2"]);

        let documents = documents!([
            { "id": 1, "doggo": "kevin" },
            { "id": 2, "doggo": { "name": "jean", "age": 20 } },
            { "id": 3, "name": "bob", "age": 25 },
        ]);
        indexer.add_documents(&documents).unwrap();

        indexer.delete_documents(&["1", "2", "1", "2"]);

        let (document_changes, _operation_stats, primary_key) = indexer
            .into_changes(
                &indexer_alloc,
                &index.inner,
                &rtxn,
                None,
                &mut new_fields_ids_map,
                &|| false,
                Progress::default(),
            )
            .unwrap();

        indexer::index(
            &mut wtxn,
            &index.inner,
            &crate::ThreadPoolNoAbortBuilder::new().build().unwrap(),
            indexer_config.grenad_parameters(),
            &db_fields_ids_map,
            new_fields_ids_map,
            primary_key,
            &document_changes,
            embedders,
            &|| false,
            &Progress::default(),
        )
        .unwrap();
        wtxn.commit().unwrap();

        db_snap!(index, documents, @r###"
        {"id":3,"name":"bob","age":25}
        "###);
    }

    #[test]
    fn add_document_and_in_another_transform_delete_the_document_then_add_it_again() {
        let index = TempIndex::new();

        let mut wtxn = index.write_txn().unwrap();
        let indexer_config = &index.indexer_config;
        let rtxn = index.inner.read_txn().unwrap();
        let db_fields_ids_map = index.inner.fields_ids_map(&rtxn).unwrap();
        let mut new_fields_ids_map = db_fields_ids_map.clone();

        let indexer_alloc = Bump::new();
        let embedders = EmbeddingConfigs::default();
        let mut indexer = indexer::DocumentOperation::new(IndexDocumentsMethod::UpdateDocuments);

        let documents = documents!([
            { "id": 1, "doggo": "kevin" },
        ]);
        indexer.add_documents(&documents).unwrap();

        let (document_changes, _operation_stats, primary_key) = indexer
            .into_changes(
                &indexer_alloc,
                &index.inner,
                &rtxn,
                None,
                &mut new_fields_ids_map,
                &|| false,
                Progress::default(),
            )
            .unwrap();

        indexer::index(
            &mut wtxn,
            &index.inner,
            &crate::ThreadPoolNoAbortBuilder::new().build().unwrap(),
            indexer_config.grenad_parameters(),
            &db_fields_ids_map,
            new_fields_ids_map,
            primary_key,
            &document_changes,
            embedders,
            &|| false,
            &Progress::default(),
        )
        .unwrap();
        wtxn.commit().unwrap();

        db_snap!(index, documents, @r###"
        {"id":1,"doggo":"kevin"}
        "###);

        // A first batch of documents has been inserted

        let mut wtxn = index.write_txn().unwrap();
        let indexer_config = &index.indexer_config;
        let rtxn = index.inner.read_txn().unwrap();
        let db_fields_ids_map = index.inner.fields_ids_map(&rtxn).unwrap();
        let mut new_fields_ids_map = db_fields_ids_map.clone();

        let indexer_alloc = Bump::new();
        let embedders = EmbeddingConfigs::default();
        let mut indexer = indexer::DocumentOperation::new(IndexDocumentsMethod::ReplaceDocuments);

        indexer.delete_documents(&["1"]);

        let documents = documents!([
            { "id": 1, "catto": "jorts" },
        ]);

        indexer.add_documents(&documents).unwrap();

        let (document_changes, _operation_stats, primary_key) = indexer
            .into_changes(
                &indexer_alloc,
                &index.inner,
                &rtxn,
                None,
                &mut new_fields_ids_map,
                &|| false,
                Progress::default(),
            )
            .unwrap();

        indexer::index(
            &mut wtxn,
            &index.inner,
            &crate::ThreadPoolNoAbortBuilder::new().build().unwrap(),
            indexer_config.grenad_parameters(),
            &db_fields_ids_map,
            new_fields_ids_map,
            primary_key,
            &document_changes,
            embedders,
            &|| false,
            &Progress::default(),
        )
        .unwrap();
        wtxn.commit().unwrap();

        db_snap!(index, documents, @r###"
        {"id":1,"catto":"jorts"}
        "###);
    }

    #[test]
    fn test_word_fid_position() {
        let index = TempIndex::new();

        index
            .add_documents(documents!([
              {"id": 0, "text": "sun flowers are looking at the sun" },
              {"id": 1, "text": "sun flowers are looking at the sun" },
              {"id": 2, "text": "the sun is shining today" },
              {
                "id": 3,
                "text": "a a a a a a a a a a a a a a a a a
                a a a a a a a a a a a a a a a a a a a a a a a a a a
                a a a a a a a a a a a a a a a a a a a a a a a a a a
                a a a a a a a a a a a a a a a a a a a a a a a a a a
                a a a a a a a a a a a a a a a a a a a a a a a a a a
                a a a a a a a a a a a a a a a a a a a a a a a a a a
                a a a a a a a a a a a a a a a a a a a a a "
             }
            ]))
            .unwrap();

        db_snap!(index, word_fid_docids, 1, @"bf3355e493330de036c8823ddd1dbbd9");
        db_snap!(index, word_position_docids, 1, @"896d54b29ed79c4c6f14084f326dcf6f");

        index
            .add_documents(documents!([
              {"id": 4, "text": "sun flowers are looking at the sun" },
              {"id": 5, "text2": "sun flowers are looking at the sun" },
              {"id": 6, "text": "b b b" },
              {
                "id": 7,
                "text2": "a a a a"
             }
            ]))
            .unwrap();

        db_snap!(index, word_fid_docids, 2, @"a48d3f88db33f94bc23110a673ea49e4");
        db_snap!(index, word_position_docids, 2, @"3c9e66c6768ae2cf42b46b2c46e46a83");

        // Delete not all of the documents but some of them.
        index.delete_documents(vec!["0".into(), "3".into()]);

        db_snap!(index, word_fid_docids, 3, @"4c2e2a1832e5802796edc1638136d933");
        db_snap!(index, word_position_docids, 3, @"74f556b91d161d997a89468b4da1cb8f");
    }

    /// Index multiple different number of vectors in documents.
    /// Vectors must be of the same length.
    #[test]
    fn test_multiple_vectors() {
        use crate::vector::settings::EmbeddingSettings;
        let index = TempIndex::new();

        index
            .update_settings(|settings| {
                let mut embedders = BTreeMap::default();
                embedders.insert(
                    "manual".to_string(),
                    Setting::Set(EmbeddingSettings {
                        source: Setting::Set(crate::vector::settings::EmbedderSource::UserProvided),
                        model: Setting::NotSet,
                        revision: Setting::NotSet,
                        api_key: Setting::NotSet,
                        dimensions: Setting::Set(3),
                        document_template: Setting::NotSet,
                        document_template_max_bytes: Setting::NotSet,
                        url: Setting::NotSet,
                        request: Setting::NotSet,
                        response: Setting::NotSet,
                        distribution: Setting::NotSet,
                        headers: Setting::NotSet,
                        binary_quantized: Setting::NotSet,
                    }),
                );
                settings.set_embedder_settings(embedders);
            })
            .unwrap();

        index
            .add_documents(
                documents!([{"id": 0, "_vectors": { "manual": [[0, 1, 2], [3, 4, 5]] } }]),
            )
            .unwrap();
        index.add_documents(documents!([{"id": 1, "_vectors": { "manual": [6, 7, 8] }}])).unwrap();
        index
               .add_documents(
                   documents!([{"id": 2, "_vectors": { "manual": [[9, 10, 11], [12, 13, 14], [15, 16, 17]] }}]),
               )
               .unwrap();

        let rtxn = index.read_txn().unwrap();
        let mut embedding_configs = index.embedding_configs(&rtxn).unwrap();
        let IndexEmbeddingConfig { name: embedder_name, config: embedder, user_provided } =
            embedding_configs.pop().unwrap();
        insta::assert_snapshot!(embedder_name, @"manual");
        insta::assert_debug_snapshot!(user_provided, @"RoaringBitmap<[0, 1, 2]>");
        let embedder =
            std::sync::Arc::new(crate::vector::Embedder::new(embedder.embedder_options).unwrap());
        let res = index
            .search(&rtxn)
            .semantic(embedder_name, embedder, false, Some([0.0, 1.0, 2.0].to_vec()))
            .execute()
            .unwrap();
        assert_eq!(res.documents_ids.len(), 3);
    }

    #[test]
    fn reproduce_the_bug() {
        /*
            [milli/examples/fuzz.rs:69] &batches = [
            Batch(
                [
                    AddDoc(
                        { "id": 1, "doggo": "bernese" }, => internal 0
                    ),
                ],
            ),
            Batch(
                [
                    DeleteDoc(
                        1, => delete internal 0
                    ),
                    AddDoc(
                        { "id": 0, "catto": "jorts" }, => internal 1
                    ),
                ],
            ),
            Batch(
                [
                    AddDoc(
                        { "id": 1, "catto": "jorts" }, => internal 2
                    ),
                ],
            ),
        ]
        */
        let index = TempIndex::new();

        // START OF BATCH

        println!("--- ENTERING BATCH 1");

        let mut wtxn = index.write_txn().unwrap();
        let indexer_config = &index.indexer_config;
        let rtxn = index.inner.read_txn().unwrap();
        let db_fields_ids_map = index.inner.fields_ids_map(&rtxn).unwrap();
        let mut new_fields_ids_map = db_fields_ids_map.clone();

        let indexer_alloc = Bump::new();
        let embedders = EmbeddingConfigs::default();
        let mut indexer = indexer::DocumentOperation::new(IndexDocumentsMethod::ReplaceDocuments);

        // OP

        let documents = documents!([
            { "id": 1, "doggo": "bernese" },
        ]);
        indexer.add_documents(&documents).unwrap();

        // FINISHING
        let (document_changes, _operation_stats, primary_key) = indexer
            .into_changes(
                &indexer_alloc,
                &index.inner,
                &rtxn,
                None,
                &mut new_fields_ids_map,
                &|| false,
                Progress::default(),
            )
            .unwrap();

        indexer::index(
            &mut wtxn,
            &index.inner,
            &crate::ThreadPoolNoAbortBuilder::new().build().unwrap(),
            indexer_config.grenad_parameters(),
            &db_fields_ids_map,
            new_fields_ids_map,
            primary_key,
            &document_changes,
            embedders,
            &|| false,
            &Progress::default(),
        )
        .unwrap();
        wtxn.commit().unwrap();

        db_snap!(index, documents, @r###"
        {"id":1,"doggo":"bernese"}
        "###);
        db_snap!(index, external_documents_ids, @r###"
        docids:
        1                        0
        "###);

        // A first batch of documents has been inserted

        // BATCH 2

        println!("--- ENTERING BATCH 2");

        let mut wtxn = index.write_txn().unwrap();
        let indexer_config = &index.indexer_config;
        let rtxn = index.inner.read_txn().unwrap();
        let db_fields_ids_map = index.inner.fields_ids_map(&rtxn).unwrap();
        let mut new_fields_ids_map = db_fields_ids_map.clone();

        let indexer_alloc = Bump::new();
        let embedders = EmbeddingConfigs::default();
        let mut indexer = indexer::DocumentOperation::new(IndexDocumentsMethod::ReplaceDocuments);

        indexer.delete_documents(&["1"]);

        let documents = documents!([
            { "id": 0, "catto": "jorts" },
        ]);
        indexer.add_documents(&documents).unwrap();

        let (document_changes, _operation_stats, primary_key) = indexer
            .into_changes(
                &indexer_alloc,
                &index.inner,
                &rtxn,
                None,
                &mut new_fields_ids_map,
                &|| false,
                Progress::default(),
            )
            .unwrap();

        indexer::index(
            &mut wtxn,
            &index.inner,
            &crate::ThreadPoolNoAbortBuilder::new().build().unwrap(),
            indexer_config.grenad_parameters(),
            &db_fields_ids_map,
            new_fields_ids_map,
            primary_key,
            &document_changes,
            embedders,
            &|| false,
            &Progress::default(),
        )
        .unwrap();
        wtxn.commit().unwrap();

        db_snap!(index, documents, @r###"
        {"id":0,"catto":"jorts"}
        "###);

        db_snap!(index, external_documents_ids, @r###"
        docids:
        0                        1
        "###);

        // BATCH 3

        println!("--- ENTERING BATCH 3");

        let mut wtxn = index.write_txn().unwrap();
        let indexer_config = &index.indexer_config;
        let rtxn = index.inner.read_txn().unwrap();
        let db_fields_ids_map = index.inner.fields_ids_map(&rtxn).unwrap();
        let mut new_fields_ids_map = db_fields_ids_map.clone();

        let indexer_alloc = Bump::new();
        let embedders = EmbeddingConfigs::default();
        let mut indexer = indexer::DocumentOperation::new(IndexDocumentsMethod::ReplaceDocuments);

        let documents = documents!([
            { "id": 1, "catto": "jorts" },
        ]);
        indexer.add_documents(&documents).unwrap();

        let (document_changes, _operation_stats, primary_key) = indexer
            .into_changes(
                &indexer_alloc,
                &index.inner,
                &rtxn,
                None,
                &mut new_fields_ids_map,
                &|| false,
                Progress::default(),
            )
            .unwrap();

        indexer::index(
            &mut wtxn,
            &index.inner,
            &crate::ThreadPoolNoAbortBuilder::new().build().unwrap(),
            indexer_config.grenad_parameters(),
            &db_fields_ids_map,
            new_fields_ids_map,
            primary_key,
            &document_changes,
            embedders,
            &|| false,
            &Progress::default(),
        )
        .unwrap();
        wtxn.commit().unwrap();

        db_snap!(index, documents, @r###"
        {"id":1,"catto":"jorts"}
        {"id":0,"catto":"jorts"}
        "###);

        // Ensuring all the returned IDs actually exists
        let rtxn = index.read_txn().unwrap();
        let res = index.search(&rtxn).execute().unwrap();
        index.compressed_documents(&rtxn, res.documents_ids).unwrap();
    }

    fn delete_documents<'t>(
        wtxn: &mut RwTxn<'t>,
        index: &'t TempIndex,
        external_ids: &[&str],
    ) -> Vec<u32> {
        let external_document_ids = index.external_documents_ids();
        let ids_to_delete: Vec<u32> = external_ids
            .iter()
            .map(|id| external_document_ids.get(wtxn, id).unwrap().unwrap())
            .collect();

        // Delete some documents.
        index
            .delete_documents_using_wtxn(
                wtxn,
                external_ids.iter().map(ToString::to_string).collect(),
            )
            .unwrap();

        ids_to_delete
    }

    #[test]
    fn delete_documents_with_numbers_as_primary_key() {
        let index = TempIndex::new();

        let mut wtxn = index.write_txn().unwrap();
        index
            .add_documents_using_wtxn(
                &mut wtxn,
                documents!([
                    { "id": 0, "name": "kevin", "object": { "key1": "value1", "key2": "value2" } },
                    { "id": 1, "name": "kevina", "array": ["I", "am", "fine"] },
                    { "id": 2, "name": "benoit", "array_of_object": [{ "wow": "amazing" }] }
                ]),
            )
            .unwrap();
        wtxn.commit().unwrap();

        let mut wtxn = index.write_txn().unwrap(); // delete those documents, ids are synchronous therefore 0, 1, and 2.
        index.delete_documents_using_wtxn(&mut wtxn, vec![S("0"), S("1"), S("2")]).unwrap();
        wtxn.commit().unwrap();

        // All these snapshots should be empty since the database was cleared
        db_snap!(index, documents_ids);
        db_snap!(index, word_docids);
        db_snap!(index, word_pair_proximity_docids);
        db_snap!(index, facet_id_exists_docids);

        let rtxn = index.read_txn().unwrap();

        assert!(index.field_distribution(&rtxn).unwrap().is_empty());
    }

    #[test]
    fn delete_documents_with_strange_primary_key() {
        let index = TempIndex::new();

        index
            .update_settings(|settings| settings.set_searchable_fields(vec!["name".to_string()]))
            .unwrap();

        let mut wtxn = index.write_txn().unwrap();
        index
            .add_documents_using_wtxn(
                &mut wtxn,
                documents!([
                    { "mysuperid": 0, "name": "kevin" },
                    { "mysuperid": 1, "name": "kevina" },
                    { "mysuperid": 2, "name": "benoit" }
                ]),
            )
            .unwrap();
        wtxn.commit().unwrap();

        let mut wtxn = index.write_txn().unwrap();
        // Delete not all of the documents but some of them.
        index.delete_documents_using_wtxn(&mut wtxn, vec![S("0"), S("1")]).unwrap();

        wtxn.commit().unwrap();

        db_snap!(index, documents_ids);
        db_snap!(index, word_docids);
        db_snap!(index, word_pair_proximity_docids);
    }

    #[test]
    fn filtered_placeholder_search_should_not_return_deleted_documents() {
        let index = TempIndex::new();

        let mut wtxn = index.write_txn().unwrap();
        index
            .update_settings_using_wtxn(&mut wtxn, |settings| {
                settings.set_primary_key(S("docid"));
                settings.set_filterable_fields(hashset! { S("label"), S("label2") });
            })
            .unwrap();
        wtxn.commit().unwrap();

        let mut wtxn = index.write_txn().unwrap();
        index
            .add_documents_using_wtxn(
                &mut wtxn,
                documents!([
                    { "docid": "1_4",  "label": ["sign"] },
                    { "docid": "1_5",  "label": ["letter"] },
                    { "docid": "1_7",  "label": ["abstract","cartoon","design","pattern"] },
                    { "docid": "1_36", "label": ["drawing","painting","pattern"] },
                    { "docid": "1_37", "label": ["art","drawing","outdoor"] },
                    { "docid": "1_38", "label": ["aquarium","art","drawing"] },
                    { "docid": "1_39", "label": ["abstract"] },
                    { "docid": "1_40", "label": ["cartoon"] },
                    { "docid": "1_41", "label": ["art","drawing"] },
                    { "docid": "1_42", "label": ["art","pattern"] },
                    { "docid": "1_43", "label": ["abstract","art","drawing","pattern"] },
                    { "docid": "1_44", "label": ["drawing"] },
                    { "docid": "1_45", "label": ["art"] },
                    { "docid": "1_46", "label": ["abstract","colorfulness","pattern"] },
                    { "docid": "1_47", "label": ["abstract","pattern"] },
                    { "docid": "1_52", "label": ["abstract","cartoon"] },
                    { "docid": "1_57", "label": ["abstract","drawing","pattern"] },
                    { "docid": "1_58", "label": ["abstract","art","cartoon"] },
                    { "docid": "1_68", "label": ["design"] },
                    { "docid": "1_69", "label": ["geometry"] },
                    { "docid": "1_70", "label2": ["geometry", 1.2] },
                    { "docid": "1_71", "label2": ["design", 2.2] },
                    { "docid": "1_72", "label2": ["geometry", 1.2] }
                ]),
            )
            .unwrap();

        wtxn.commit().unwrap();

        let mut wtxn = index.write_txn().unwrap();
        delete_documents(&mut wtxn, &index, &["1_4", "1_70", "1_72"]);
        wtxn.commit().unwrap();

        let rtxn = index.read_txn().unwrap();
        // Placeholder search with filter
        let filter = Filter::from_str("label = sign").unwrap().unwrap();
        let results = index.search(&rtxn).filter(filter).execute().unwrap();
        assert!(results.documents_ids.is_empty());

        db_snap!(index, word_docids);
        db_snap!(index, facet_id_f64_docids);
        db_snap!(index, word_pair_proximity_docids);
        db_snap!(index, facet_id_exists_docids);
        db_snap!(index, facet_id_string_docids);
    }

    #[test]
    fn placeholder_search_should_not_return_deleted_documents() {
        let index = TempIndex::new();

        index
            .update_settings(|settings| {
                settings.set_primary_key(S("docid"));
            })
            .unwrap();

        index
            .add_documents(documents!([
                { "docid": "1_4",  "label": ["sign"] },
                { "docid": "1_5",  "label": ["letter"] },
                { "docid": "1_7",  "label": ["abstract","cartoon","design","pattern"] },
                { "docid": "1_36", "label": ["drawing","painting","pattern"] },
                { "docid": "1_37", "label": ["art","drawing","outdoor"] },
                { "docid": "1_38", "label": ["aquarium","art","drawing"] },
                { "docid": "1_39", "label": ["abstract"] },
                { "docid": "1_40", "label": ["cartoon"] },
                { "docid": "1_41", "label": ["art","drawing"] },
                { "docid": "1_42", "label": ["art","pattern"] },
                { "docid": "1_43", "label": ["abstract","art","drawing","pattern"] },
                { "docid": "1_44", "label": ["drawing"] },
                { "docid": "1_45", "label": ["art"] },
                { "docid": "1_46", "label": ["abstract","colorfulness","pattern"] },
                { "docid": "1_47", "label": ["abstract","pattern"] },
                { "docid": "1_52", "label": ["abstract","cartoon"] },
                { "docid": "1_57", "label": ["abstract","drawing","pattern"] },
                { "docid": "1_58", "label": ["abstract","art","cartoon"] },
                { "docid": "1_68", "label": ["design"] },
                { "docid": "1_69", "label": ["geometry"] },
                { "docid": "1_70", "label2": ["geometry", 1.2] },
                { "docid": "1_71", "label2": ["design", 2.2] },
                { "docid": "1_72", "label2": ["geometry", 1.2] }
            ]))
            .unwrap();

        let mut wtxn = index.write_txn().unwrap();

        let deleted_internal_ids = delete_documents(&mut wtxn, &index, &["1_4"]);

        wtxn.commit().unwrap();

        // Placeholder search
        let rtxn = index.static_read_txn().unwrap();

        let results = index.search(&rtxn).execute().unwrap();
        assert!(!results.documents_ids.is_empty());
        for id in results.documents_ids.iter() {
            assert!(
                !deleted_internal_ids.contains(id),
                "The document {} was supposed to be deleted",
                id
            );
        }

        drop(rtxn);
    }

    #[test]
    fn search_should_not_return_deleted_documents() {
        let index = TempIndex::new();

        index
            .update_settings(|settings| {
                settings.set_primary_key(S("docid"));
            })
            .unwrap();

        index
            .add_documents(documents!([
                { "docid": "1_4",  "label": ["sign"] },
                { "docid": "1_5",  "label": ["letter"] },
                { "docid": "1_7",  "label": ["abstract","cartoon","design","pattern"] },
                { "docid": "1_36", "label": ["drawing","painting","pattern"] },
                { "docid": "1_37", "label": ["art","drawing","outdoor"] },
                { "docid": "1_38", "label": ["aquarium","art","drawing"] },
                { "docid": "1_39", "label": ["abstract"] },
                { "docid": "1_40", "label": ["cartoon"] },
                { "docid": "1_41", "label": ["art","drawing"] },
                { "docid": "1_42", "label": ["art","pattern"] },
                { "docid": "1_43", "label": ["abstract","art","drawing","pattern"] },
                { "docid": "1_44", "label": ["drawing"] },
                { "docid": "1_45", "label": ["art"] },
                { "docid": "1_46", "label": ["abstract","colorfulness","pattern"] },
                { "docid": "1_47", "label": ["abstract","pattern"] },
                { "docid": "1_52", "label": ["abstract","cartoon"] },
                { "docid": "1_57", "label": ["abstract","drawing","pattern"] },
                { "docid": "1_58", "label": ["abstract","art","cartoon"] },
                { "docid": "1_68", "label": ["design"] },
                { "docid": "1_69", "label": ["geometry"] },
                { "docid": "1_70", "label2": ["geometry", 1.2] },
                { "docid": "1_71", "label2": ["design", 2.2] },
                { "docid": "1_72", "label2": ["geometry", 1.2] }
            ]))
            .unwrap();

        let mut wtxn = index.write_txn().unwrap();
        let deleted_internal_ids = delete_documents(&mut wtxn, &index, &["1_7", "1_52"]);
        wtxn.commit().unwrap();

        // search for abstract
        let rtxn = index.read_txn().unwrap();
        let results = index.search(&rtxn).query("abstract").execute().unwrap();
        assert!(!results.documents_ids.is_empty());
        for id in results.documents_ids.iter() {
            assert!(
                !deleted_internal_ids.contains(id),
                "The document {} was supposed to be deleted",
                id
            );
        }
    }

    #[test]
    fn geo_filtered_placeholder_search_should_not_return_deleted_documents() {
        let index = TempIndex::new();

        let mut wtxn = index.write_txn().unwrap();
        index
            .update_settings_using_wtxn(&mut wtxn, |settings| {
                settings.set_primary_key(S("id"));
                settings.set_filterable_fields(hashset!(S("_geo")));
                settings.set_sortable_fields(hashset!(S("_geo")));
            })
            .unwrap();
        wtxn.commit().unwrap();

        let mut wtxn = index.write_txn().unwrap();
        index.add_documents_using_wtxn(&mut wtxn, documents!([
            { "id": "1",  "city": "Lille",             "_geo": { "lat": 50.6299, "lng": 3.0569 } },
            { "id": "2",  "city": "Mons-en-Barœul",    "_geo": { "lat": 50.6415, "lng": 3.1106 } },
            { "id": "3",  "city": "Hellemmes",         "_geo": { "lat": 50.6312, "lng": 3.1106 } },
            { "id": "4",  "city": "Villeneuve-d'Ascq", "_geo": { "lat": 50.6224, "lng": 3.1476 } },
            { "id": "5",  "city": "Hem",               "_geo": { "lat": 50.6552, "lng": 3.1897 } },
            { "id": "6",  "city": "Roubaix",           "_geo": { "lat": 50.6924, "lng": 3.1763 } },
            { "id": "7",  "city": "Tourcoing",         "_geo": { "lat": 50.7263, "lng": 3.1541 } },
            { "id": "8",  "city": "Mouscron",          "_geo": { "lat": 50.7453, "lng": 3.2206 } },
            { "id": "9",  "city": "Tournai",           "_geo": { "lat": 50.6053, "lng": 3.3758 } },
            { "id": "10", "city": "Ghent",             "_geo": { "lat": 51.0537, "lng": 3.6957 } },
            { "id": "11", "city": "Brussels",          "_geo": { "lat": 50.8466, "lng": 4.3370 } },
            { "id": "12", "city": "Charleroi",         "_geo": { "lat": 50.4095, "lng": 4.4347 } },
            { "id": "13", "city": "Mons",              "_geo": { "lat": 50.4502, "lng": 3.9623 } },
            { "id": "14", "city": "Valenciennes",      "_geo": { "lat": 50.3518, "lng": 3.5326 } },
            { "id": "15", "city": "Arras",             "_geo": { "lat": 50.2844, "lng": 2.7637 } },
            { "id": "16", "city": "Cambrai",           "_geo": { "lat": 50.1793, "lng": 3.2189 } },
            { "id": "17", "city": "Bapaume",           "_geo": { "lat": 50.1112, "lng": 2.8547 } },
            { "id": "18", "city": "Amiens",            "_geo": { "lat": 49.9314, "lng": 2.2710 } },
            { "id": "19", "city": "Compiègne",         "_geo": { "lat": 49.4449, "lng": 2.7913 } },
            { "id": "20", "city": "Paris",             "_geo": { "lat": 48.9021, "lng": 2.3708 } }
        ])).unwrap();
        wtxn.commit().unwrap();

        let mut wtxn = index.write_txn().unwrap();
        let external_ids_to_delete = ["5", "6", "7", "12", "17", "19"];
        let deleted_internal_ids = delete_documents(&mut wtxn, &index, &external_ids_to_delete);

        // Placeholder search with geo filter
        let filter = Filter::from_str("_geoRadius(50.6924, 3.1763, 20000)").unwrap().unwrap();
        let results = index.search(&wtxn).filter(filter).execute().unwrap();
        assert!(!results.documents_ids.is_empty());
        for id in results.documents_ids.iter() {
            assert!(
                !deleted_internal_ids.contains(id),
                "The document {} was supposed to be deleted",
                id
            );
        }

        wtxn.commit().unwrap();

        db_snap!(index, facet_id_f64_docids);
        db_snap!(index, facet_id_string_docids);
    }

    #[test]
    fn get_documents_should_not_return_deleted_documents() {
        let index = TempIndex::new();

        let mut wtxn = index.write_txn().unwrap();
        index
            .update_settings_using_wtxn(&mut wtxn, |settings| {
                settings.set_primary_key(S("docid"));
            })
            .unwrap();

        index
            .add_documents_using_wtxn(
                &mut wtxn,
                documents!([
                    { "docid": "1_4",  "label": ["sign"] },
                    { "docid": "1_5",  "label": ["letter"] },
                    { "docid": "1_7",  "label": ["abstract","cartoon","design","pattern"] },
                    { "docid": "1_36", "label": ["drawing","painting","pattern"] },
                    { "docid": "1_37", "label": ["art","drawing","outdoor"] },
                    { "docid": "1_38", "label": ["aquarium","art","drawing"] },
                    { "docid": "1_39", "label": ["abstract"] },
                    { "docid": "1_40", "label": ["cartoon"] },
                    { "docid": "1_41", "label": ["art","drawing"] },
                    { "docid": "1_42", "label": ["art","pattern"] },
                    { "docid": "1_43", "label": ["abstract","art","drawing","pattern"] },
                    { "docid": "1_44", "label": ["drawing"] },
                    { "docid": "1_45", "label": ["art"] },
                    { "docid": "1_46", "label": ["abstract","colorfulness","pattern"] },
                    { "docid": "1_47", "label": ["abstract","pattern"] },
                    { "docid": "1_52", "label": ["abstract","cartoon"] },
                    { "docid": "1_57", "label": ["abstract","drawing","pattern"] },
                    { "docid": "1_58", "label": ["abstract","art","cartoon"] },
                    { "docid": "1_68", "label": ["design"] },
                    { "docid": "1_69", "label": ["geometry"] },
                    { "docid": "1_70", "label2": ["geometry", 1.2] },
                    { "docid": "1_71", "label2": ["design", 2.2] },
                    { "docid": "1_72", "label2": ["geometry", 1.2] }
                ]),
            )
            .unwrap();
        wtxn.commit().unwrap();

        let mut wtxn = index.write_txn().unwrap();
        let deleted_external_ids = ["1_7", "1_52"];
        let deleted_internal_ids = delete_documents(&mut wtxn, &index, &deleted_external_ids);
        wtxn.commit().unwrap();

        let rtxn = index.read_txn().unwrap();
        // list all documents
        let results = index.all_compressed_documents(&rtxn).unwrap();
        for result in results {
            let (id, _) = result.unwrap();
            assert!(
                !deleted_internal_ids.contains(&id),
                "The document {} was supposed to be deleted",
                id
            );
        }

        // list internal document ids
        let results = index.documents_ids(&rtxn).unwrap();
        for id in results {
            assert!(
                !deleted_internal_ids.contains(&id),
                "The document {} was supposed to be deleted",
                id
            );
        }

        // get internal docids from deleted external document ids
        let results = index.external_documents_ids();
        for id in deleted_external_ids {
            assert!(
                results.get(&rtxn, id).unwrap().is_none(),
                "The document {} was supposed to be deleted",
                id
            );
        }
        drop(rtxn);
    }

    #[test]
    fn stats_should_not_return_deleted_documents() {
        let index = TempIndex::new();

        index
            .update_settings(|settings| {
                settings.set_primary_key(S("docid"));
            })
            .unwrap();

        index.add_documents(documents!([
            { "docid": "1_4",  "label": ["sign"]},
            { "docid": "1_5",  "label": ["letter"]},
            { "docid": "1_7",  "label": ["abstract","cartoon","design","pattern"], "title": "Mickey Mouse"},
            { "docid": "1_36", "label": ["drawing","painting","pattern"]},
            { "docid": "1_37", "label": ["art","drawing","outdoor"]},
            { "docid": "1_38", "label": ["aquarium","art","drawing"], "title": "Nemo"},
            { "docid": "1_39", "label": ["abstract"]},
            { "docid": "1_40", "label": ["cartoon"]},
            { "docid": "1_41", "label": ["art","drawing"]},
            { "docid": "1_42", "label": ["art","pattern"]},
            { "docid": "1_43", "label": ["abstract","art","drawing","pattern"], "number": 32i32},
            { "docid": "1_44", "label": ["drawing"], "number": 44i32},
            { "docid": "1_45", "label": ["art"]},
            { "docid": "1_46", "label": ["abstract","colorfulness","pattern"]},
            { "docid": "1_47", "label": ["abstract","pattern"]},
            { "docid": "1_52", "label": ["abstract","cartoon"]},
            { "docid": "1_57", "label": ["abstract","drawing","pattern"]},
            { "docid": "1_58", "label": ["abstract","art","cartoon"]},
            { "docid": "1_68", "label": ["design"]},
            { "docid": "1_69", "label": ["geometry"]}
        ])).unwrap();

        let mut wtxn = index.write_txn().unwrap();

        delete_documents(&mut wtxn, &index, &["1_7", "1_52"]);
        wtxn.commit().unwrap();

        let rtxn = index.read_txn().unwrap();

        // count internal documents
        let results = index.number_of_documents(&rtxn).unwrap();
        assert_eq!(18, results);

        // count field distribution
        let results = index.field_distribution(&rtxn).unwrap();
        assert_eq!(Some(&18), results.get("label"));
        assert_eq!(Some(&1), results.get("title"));
        assert_eq!(Some(&2), results.get("number"));

        rtxn.commit().unwrap();
    }

    #[test]
    fn delete_words_exact_attributes() {
        let index = TempIndex::new();

        index
            .update_settings(|settings| {
                settings.set_primary_key(S("id"));
                settings.set_searchable_fields(vec![S("text"), S("exact")]);
                settings.set_exact_attributes(vec![S("exact")].into_iter().collect());
            })
            .unwrap();

        index
            .add_documents(documents!([
                { "id": 0, "text": "hello" },
                { "id": 1, "exact": "hello"}
            ]))
            .unwrap();
        db_snap!(index, word_docids, 1, @r###"
        hello            [0, ]
        "###);
        db_snap!(index, exact_word_docids, 1, @r###"
        hello            [1, ]
        "###);
        db_snap!(index, words_fst, 1, @"300000000000000001084cfcfc2ce1000000016000000090ea47f");

        let mut wtxn = index.write_txn().unwrap();
        let deleted_internal_ids = delete_documents(&mut wtxn, &index, &["1"]);
        wtxn.commit().unwrap();

        db_snap!(index, word_docids, 2, @r###"
        hello            [0, ]
        "###);
        db_snap!(index, exact_word_docids, 2, @"");
        db_snap!(index, words_fst, 2, @"300000000000000001084cfcfc2ce1000000016000000090ea47f");

        insta::assert_snapshot!(format!("{deleted_internal_ids:?}"), @"[1]");
        let txn = index.read_txn().unwrap();
        let words = index.words_fst(&txn).unwrap().into_stream().into_strs().unwrap();
        insta::assert_snapshot!(format!("{words:?}"), @r###"["hello"]"###);

        let mut s = Search::new(&txn, &index);
        s.query("hello");
        let crate::SearchResult { documents_ids, .. } = s.execute().unwrap();
        insta::assert_snapshot!(format!("{documents_ids:?}"), @"[0]");
    }
}
