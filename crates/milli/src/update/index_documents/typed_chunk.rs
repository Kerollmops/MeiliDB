use std::collections::BTreeSet;
use std::convert::TryInto;
use std::fs::File;
use std::io::{self, BufReader};

use bytemuck::allocation::pod_collect_to_vec;
use grenad::{MergeFunction, Merger, MergerBuilder};
use heed::types::Bytes;
use heed::{BytesDecode, RwTxn};
use obkv::{KvReader, KvReaderU16, KvWriter};
use roaring::RoaringBitmap;

use super::helpers::{
    self, merge_deladd_cbo_roaring_bitmaps_into_cbo_roaring_bitmap, valid_lmdb_key,
    CursorClonableMmap, KeepFirst, MergeDeladdBtreesetString, MergeDeladdCboRoaringBitmaps,
    MergeIgnoreValues,
};
use crate::external_documents_ids::{DocumentOperation, DocumentOperationKind};
use crate::facet::FacetType;
use crate::heed_codec::CompressedObkvU16;
use crate::index::db_name::DOCUMENTS;
use crate::index::IndexEmbeddingConfig;
use crate::proximity::MAX_DISTANCE;
use crate::update::del_add::{deladd_serialize_add_side, DelAdd, KvReaderDelAdd};
use crate::update::facet::FacetsUpdate;
use crate::update::index_documents::helpers::{
    as_cloneable_grenad, try_split_array_at, KeepLatestObkv,
};
use crate::update::settings::InnerIndexSettingsDiff;
use crate::vector::ArroyWrapper;
use crate::{
    lat_lng_to_xyz, CboRoaringBitmapCodec, DocumentId, FieldId, GeoPoint, Index, InternalError,
    Result, SerializationError, U8StrStrCodec,
};

/// This struct accumulates and group the TypedChunks
/// and is able to give the biggest accumulated group to index them all together
/// with a merger.
#[derive(Default)]
pub(crate) struct ChunkAccumulator {
    inner: Vec<Vec<TypedChunk>>,
}

impl ChunkAccumulator {
    pub fn pop_longest(&mut self) -> Option<Vec<TypedChunk>> {
        match self.inner.iter().max_by_key(|v| v.len()) {
            Some(left) => {
                let position = self.inner.iter().position(|right| left.len() == right.len());
                position.map(|p| self.inner.remove(p)).filter(|v| !v.is_empty())
            }
            None => None,
        }
    }

    pub fn insert(&mut self, chunk: TypedChunk) {
        match self
            .inner
            .iter()
            .position(|right| right.first().map_or(false, |right| chunk.mergeable_with(right)))
        {
            Some(position) => {
                let v = self.inner.get_mut(position).unwrap();
                v.push(chunk);
            }
            None => self.inner.push(vec![chunk]),
        }
    }
}

pub(crate) enum TypedChunk {
    FieldIdDocidFacetStrings(grenad::Reader<CursorClonableMmap>),
    FieldIdDocidFacetNumbers(grenad::Reader<CursorClonableMmap>),
    Documents(grenad::Reader<CursorClonableMmap>),
    FieldIdWordCountDocids(grenad::Reader<BufReader<File>>),
    WordDocids {
        word_docids_reader: grenad::Reader<BufReader<File>>,
        exact_word_docids_reader: grenad::Reader<BufReader<File>>,
        word_fid_docids_reader: grenad::Reader<BufReader<File>>,
    },
    WordPositionDocids(grenad::Reader<BufReader<File>>),
    WordPairProximityDocids(grenad::Reader<BufReader<File>>),
    FieldIdFacetStringDocids((grenad::Reader<BufReader<File>>, grenad::Reader<BufReader<File>>)),
    FieldIdFacetNumberDocids(grenad::Reader<BufReader<File>>),
    FieldIdFacetExistsDocids(grenad::Reader<BufReader<File>>),
    FieldIdFacetIsNullDocids(grenad::Reader<BufReader<File>>),
    FieldIdFacetIsEmptyDocids(grenad::Reader<BufReader<File>>),
    GeoPoints(grenad::Reader<BufReader<File>>),
    VectorPoints {
        remove_vectors: grenad::Reader<BufReader<File>>,
        embeddings: Option<grenad::Reader<BufReader<File>>>,
        expected_dimension: usize,
        manual_vectors: grenad::Reader<BufReader<File>>,
        embedder_name: String,
        add_to_user_provided: RoaringBitmap,
        remove_from_user_provided: RoaringBitmap,
    },
}

impl TypedChunk {
    fn mergeable_with(&self, other: &Self) -> bool {
        use TypedChunk::*;
        match (self, other) {
            (FieldIdDocidFacetStrings(_), FieldIdDocidFacetStrings(_))
            | (FieldIdDocidFacetNumbers(_), FieldIdDocidFacetNumbers(_))
            | (Documents(_), Documents(_))
            | (FieldIdWordCountDocids(_), FieldIdWordCountDocids(_))
            | (WordDocids { .. }, WordDocids { .. })
            | (WordPositionDocids(_), WordPositionDocids(_))
            | (WordPairProximityDocids(_), WordPairProximityDocids(_))
            | (FieldIdFacetStringDocids(_), FieldIdFacetStringDocids(_))
            | (FieldIdFacetNumberDocids(_), FieldIdFacetNumberDocids(_))
            | (FieldIdFacetExistsDocids(_), FieldIdFacetExistsDocids(_))
            | (FieldIdFacetIsNullDocids(_), FieldIdFacetIsNullDocids(_))
            | (FieldIdFacetIsEmptyDocids(_), FieldIdFacetIsEmptyDocids(_))
            | (GeoPoints(_), GeoPoints(_)) => true,
            (
                VectorPoints { embedder_name: left, expected_dimension: left_dim, .. },
                VectorPoints { embedder_name: right, expected_dimension: right_dim, .. },
            ) => left == right && left_dim == right_dim,
            _ => false,
        }
    }
}

/// Write typed chunk in the corresponding LMDB database of the provided index.
/// Return new documents seen.
#[tracing::instrument(level = "trace", skip_all, target = "indexing::write_db")]
pub(crate) fn write_typed_chunk_into_index(
    wtxn: &mut RwTxn<'_>,
    index: &Index,
    settings_diff: &InnerIndexSettingsDiff,
    typed_chunks: Vec<TypedChunk>,
) -> Result<(RoaringBitmap, bool)> {
    let mut is_merged_database = false;
    match typed_chunks[0] {
        TypedChunk::Documents(_) => {
            let span = tracing::trace_span!(target: "indexing::write_db", "documents");
            let _entered = span.enter();

            let fields_ids_map = index.fields_ids_map(wtxn)?;
            let vectors_fid =
                fields_ids_map.id(crate::vector::parsed_vectors::RESERVED_VECTORS_FIELD_NAME);

            let mut builder = MergerBuilder::new(KeepLatestObkv);
            for typed_chunk in typed_chunks {
                let TypedChunk::Documents(chunk) = typed_chunk else {
                    unreachable!();
                };

                builder.push(chunk.into_cursor()?);
            }
            let merger = builder.build();
            let mut operations: Vec<DocumentOperation> = Default::default();

            let mut docids = index.documents_ids(wtxn)?;
            let mut iter = merger.into_stream_merger_iter()?;

            let embedders: BTreeSet<_> = index
                .embedding_configs(wtxn)?
                .into_iter()
                .map(|IndexEmbeddingConfig { name, .. }| name)
                .collect();
            let dictionary = index.document_compression_dictionary(wtxn)?;
            let mut vectors_buffer = Vec::new();
            while let Some((key, reader)) = iter.next()? {
                let mut writer: KvWriter<_, FieldId> = KvWriter::memory();
                let reader: &KvReader<FieldId> = reader.into();

                let (document_id_bytes, external_id_bytes) = try_split_array_at(key)
                    .ok_or(SerializationError::Decoding { db_name: Some(DOCUMENTS) })?;
                let docid = DocumentId::from_be_bytes(document_id_bytes);
                let external_id = std::str::from_utf8(external_id_bytes)?;

                for (field_id, value) in reader.iter() {
                    let del_add_reader = KvReaderDelAdd::from_slice(value);

                    if let Some(addition) = del_add_reader.get(DelAdd::Addition) {
                        let addition = if vectors_fid == Some(field_id) {
                            'vectors: {
                                vectors_buffer.clear();
                                let Ok(mut vectors) =
                                    crate::vector::parsed_vectors::ParsedVectors::from_bytes(
                                        addition,
                                    )
                                else {
                                    // if the `_vectors` field cannot be parsed as map of vectors, just write it as-is
                                    break 'vectors Some(addition);
                                };
                                vectors.retain_not_embedded_vectors(&embedders);
                                let crate::vector::parsed_vectors::ParsedVectors(vectors) = vectors;
                                if vectors.is_empty() {
                                    // skip writing empty `_vectors` map
                                    break 'vectors None;
                                }

                                serde_json::to_writer(&mut vectors_buffer, &vectors)
                                    .map_err(InternalError::SerdeJson)?;
                                Some(vectors_buffer.as_slice())
                            }
                        } else {
                            Some(addition)
                        };

                        if let Some(addition) = addition {
                            writer.insert(field_id, addition)?;
                        }
                    }
                }

                let db = index.documents.remap_data_type::<Bytes>();

                if !writer.is_empty() {
                    let uncompressed_document_bytes = writer.into_inner().unwrap();
                    match dictionary.as_ref() {
                        Some(dictionary) => {
                            let doc = KvReaderU16::from_slice(&uncompressed_document_bytes);
                            let compressed = CompressedObkvU16::with_dictionary(doc, dictionary)?;
                            db.put(wtxn, &docid, compressed.as_bytes())?
                        }
                        None => db.put(wtxn, &docid, &uncompressed_document_bytes)?,
                    }
                    operations.push(DocumentOperation {
                        external_id: external_id.to_string(),
                        internal_id: docid,
                        kind: DocumentOperationKind::Create,
                    });
                    docids.insert(docid);
                } else {
                    db.delete(wtxn, &docid)?;
                    operations.push(DocumentOperation {
                        external_id: external_id.to_string(),
                        internal_id: docid,
                        kind: DocumentOperationKind::Delete,
                    });
                    docids.remove(docid);
                }
            }
            let external_documents_docids = index.external_documents_ids();
            external_documents_docids.apply(wtxn, operations)?;
            index.put_documents_ids(wtxn, &docids)?;
        }
        TypedChunk::FieldIdWordCountDocids(_) => {
            let span =
                tracing::trace_span!(target: "indexing::write_db", "field_id_word_count_docids");
            let _entered = span.enter();

            let mut builder = MergerBuilder::new(MergeDeladdCboRoaringBitmaps);
            for typed_chunk in typed_chunks {
                let TypedChunk::FieldIdWordCountDocids(chunk) = typed_chunk else {
                    unreachable!();
                };

                builder.push(chunk.into_cursor()?);
            }
            let merger = builder.build();

            write_entries_into_database(
                merger,
                &index.field_id_word_count_docids,
                wtxn,
                deladd_serialize_add_side,
                merge_deladd_cbo_roaring_bitmaps_into_cbo_roaring_bitmap,
            )?;
            is_merged_database = true;
        }
        TypedChunk::WordDocids { .. } => {
            let span = tracing::trace_span!(target: "indexing::write_db", "word_docids");
            let _entered = span.enter();

            let mut word_docids_builder = MergerBuilder::new(MergeDeladdCboRoaringBitmaps);
            let mut exact_word_docids_builder = MergerBuilder::new(MergeDeladdCboRoaringBitmaps);
            let mut word_fid_docids_builder = MergerBuilder::new(MergeDeladdCboRoaringBitmaps);
            let mut fst_merger_builder = MergerBuilder::new(MergeIgnoreValues);
            for typed_chunk in typed_chunks {
                let TypedChunk::WordDocids {
                    word_docids_reader,
                    exact_word_docids_reader,
                    word_fid_docids_reader,
                } = typed_chunk
                else {
                    unreachable!();
                };
                let clonable_word_docids = unsafe { as_cloneable_grenad(&word_docids_reader) }?;
                let clonable_exact_word_docids =
                    unsafe { as_cloneable_grenad(&exact_word_docids_reader) }?;

                word_docids_builder.push(word_docids_reader.into_cursor()?);
                exact_word_docids_builder.push(exact_word_docids_reader.into_cursor()?);
                word_fid_docids_builder.push(word_fid_docids_reader.into_cursor()?);
                fst_merger_builder.push(clonable_word_docids.into_cursor()?);
                fst_merger_builder.push(clonable_exact_word_docids.into_cursor()?);
            }

            let word_docids_merger = word_docids_builder.build();
            write_entries_into_database(
                word_docids_merger,
                &index.word_docids,
                wtxn,
                deladd_serialize_add_side,
                merge_deladd_cbo_roaring_bitmaps_into_cbo_roaring_bitmap,
            )?;

            let exact_word_docids_merger = exact_word_docids_builder.build();
            write_entries_into_database(
                exact_word_docids_merger,
                &index.exact_word_docids,
                wtxn,
                deladd_serialize_add_side,
                merge_deladd_cbo_roaring_bitmaps_into_cbo_roaring_bitmap,
            )?;

            let word_fid_docids_merger = word_fid_docids_builder.build();
            write_entries_into_database(
                word_fid_docids_merger,
                &index.word_fid_docids,
                wtxn,
                deladd_serialize_add_side,
                merge_deladd_cbo_roaring_bitmaps_into_cbo_roaring_bitmap,
            )?;

            // create fst from word docids
            let fst_merger = fst_merger_builder.build();
            let fst = merge_word_docids_reader_into_fst(fst_merger)?;
            let db_fst = index.words_fst(wtxn)?;

            // merge new fst with database fst
            let union_stream = fst.op().add(db_fst.stream()).union();
            let mut builder = fst::SetBuilder::memory();
            builder.extend_stream(union_stream)?;
            let fst = builder.into_set();
            index.put_words_fst(wtxn, &fst)?;
            is_merged_database = true;
        }
        TypedChunk::WordPositionDocids(_) => {
            let span = tracing::trace_span!(target: "indexing::write_db", "word_position_docids");
            let _entered = span.enter();

            let mut builder = MergerBuilder::new(MergeDeladdCboRoaringBitmaps);
            for typed_chunk in typed_chunks {
                let TypedChunk::WordPositionDocids(chunk) = typed_chunk else {
                    unreachable!();
                };

                builder.push(chunk.into_cursor()?);
            }
            let merger = builder.build();

            write_entries_into_database(
                merger,
                &index.word_position_docids,
                wtxn,
                deladd_serialize_add_side,
                merge_deladd_cbo_roaring_bitmaps_into_cbo_roaring_bitmap,
            )?;
            is_merged_database = true;
        }
        TypedChunk::FieldIdFacetNumberDocids(_) => {
            let span =
                tracing::trace_span!(target: "indexing::write_db","field_id_facet_number_docids");
            let _entered = span.enter();

            let mut builder = MergerBuilder::new(MergeDeladdCboRoaringBitmaps);
            let mut data_size = 0;
            for typed_chunk in typed_chunks {
                let TypedChunk::FieldIdFacetNumberDocids(facet_id_number_docids) = typed_chunk
                else {
                    unreachable!();
                };

                data_size += facet_id_number_docids.len();
                builder.push(facet_id_number_docids.into_cursor()?);
            }
            let merger = builder.build();

            let indexer = FacetsUpdate::new(index, FacetType::Number, merger, None, data_size);
            indexer.execute(wtxn)?;
            is_merged_database = true;
        }
        TypedChunk::FieldIdFacetStringDocids(_) => {
            let span =
                tracing::trace_span!(target: "indexing::write_db", "field_id_facet_string_docids");
            let _entered = span.enter();

            let mut facet_id_string_builder = MergerBuilder::new(MergeDeladdCboRoaringBitmaps);
            let mut normalized_facet_id_string_builder =
                MergerBuilder::new(MergeDeladdBtreesetString);
            let mut data_size = 0;
            for typed_chunk in typed_chunks {
                let TypedChunk::FieldIdFacetStringDocids((
                    facet_id_string_docids,
                    normalized_facet_id_string_docids,
                )) = typed_chunk
                else {
                    unreachable!();
                };

                data_size += facet_id_string_docids.len();
                facet_id_string_builder.push(facet_id_string_docids.into_cursor()?);
                normalized_facet_id_string_builder
                    .push(normalized_facet_id_string_docids.into_cursor()?);
            }
            let facet_id_string_merger = facet_id_string_builder.build();
            let normalized_facet_id_string_merger = normalized_facet_id_string_builder.build();

            let indexer = FacetsUpdate::new(
                index,
                FacetType::String,
                facet_id_string_merger,
                Some(normalized_facet_id_string_merger),
                data_size,
            );
            indexer.execute(wtxn)?;
            is_merged_database = true;
        }
        TypedChunk::FieldIdFacetExistsDocids(_) => {
            let span =
                tracing::trace_span!(target: "indexing::write_db", "field_id_facet_exists_docids");
            let _entered = span.enter();

            let mut builder = MergerBuilder::new(MergeDeladdCboRoaringBitmaps);
            for typed_chunk in typed_chunks {
                let TypedChunk::FieldIdFacetExistsDocids(chunk) = typed_chunk else {
                    unreachable!();
                };

                builder.push(chunk.into_cursor()?);
            }
            let merger = builder.build();

            write_entries_into_database(
                merger,
                &index.facet_id_exists_docids,
                wtxn,
                deladd_serialize_add_side,
                merge_deladd_cbo_roaring_bitmaps_into_cbo_roaring_bitmap,
            )?;
            is_merged_database = true;
        }
        TypedChunk::FieldIdFacetIsNullDocids(_) => {
            let span =
                tracing::trace_span!(target: "indexing::write_db", "field_id_facet_is_null_docids");
            let _entered = span.enter();

            let mut builder = MergerBuilder::new(MergeDeladdCboRoaringBitmaps);
            for typed_chunk in typed_chunks {
                let TypedChunk::FieldIdFacetIsNullDocids(chunk) = typed_chunk else {
                    unreachable!();
                };

                builder.push(chunk.into_cursor()?);
            }
            let merger = builder.build();

            write_entries_into_database(
                merger,
                &index.facet_id_is_null_docids,
                wtxn,
                deladd_serialize_add_side,
                merge_deladd_cbo_roaring_bitmaps_into_cbo_roaring_bitmap,
            )?;
            is_merged_database = true;
        }
        TypedChunk::FieldIdFacetIsEmptyDocids(_) => {
            let span = tracing::trace_span!(target: "indexing::write_db", "field_id_facet_is_empty_docids");
            let _entered = span.enter();

            let mut builder = MergerBuilder::new(MergeDeladdCboRoaringBitmaps);
            for typed_chunk in typed_chunks {
                let TypedChunk::FieldIdFacetIsEmptyDocids(chunk) = typed_chunk else {
                    unreachable!();
                };

                builder.push(chunk.into_cursor()?);
            }
            let merger = builder.build();

            write_entries_into_database(
                merger,
                &index.facet_id_is_empty_docids,
                wtxn,
                deladd_serialize_add_side,
                merge_deladd_cbo_roaring_bitmaps_into_cbo_roaring_bitmap,
            )?;
            is_merged_database = true;
        }
        TypedChunk::WordPairProximityDocids(_) => {
            let span =
                tracing::trace_span!(target: "indexing::write_db", "word_pair_proximity_docids");
            let _entered = span.enter();

            let mut builder = MergerBuilder::new(MergeDeladdCboRoaringBitmaps);
            for typed_chunk in typed_chunks {
                let TypedChunk::WordPairProximityDocids(chunk) = typed_chunk else {
                    unreachable!();
                };

                builder.push(chunk.into_cursor()?);
            }
            let merger = builder.build();

            if settings_diff.only_additional_fields.is_some() {
                write_proximity_entries_into_database_additional_searchables(
                    merger,
                    &index.word_pair_proximity_docids,
                    wtxn,
                )?;
            } else {
                write_entries_into_database(
                    merger,
                    &index.word_pair_proximity_docids,
                    wtxn,
                    deladd_serialize_add_side,
                    merge_deladd_cbo_roaring_bitmaps_into_cbo_roaring_bitmap,
                )?;
            }

            is_merged_database = true;
        }
        TypedChunk::FieldIdDocidFacetNumbers(_) => {
            let span =
                tracing::trace_span!(target: "indexing::write_db", "field_id_docid_facet_numbers");
            let _entered = span.enter();

            let mut builder = MergerBuilder::new(KeepFirst);
            for typed_chunk in typed_chunks {
                let TypedChunk::FieldIdDocidFacetNumbers(chunk) = typed_chunk else {
                    unreachable!();
                };

                builder.push(chunk.into_cursor()?);
            }
            let merger = builder.build();

            let index_fid_docid_facet_numbers =
                index.field_id_docid_facet_f64s.remap_types::<Bytes, Bytes>();
            let mut iter = merger.into_stream_merger_iter()?;
            while let Some((key, value)) = iter.next()? {
                let reader = KvReaderDelAdd::from_slice(value);
                if valid_lmdb_key(key) {
                    match (reader.get(DelAdd::Deletion), reader.get(DelAdd::Addition)) {
                        (None, None) => {}
                        (None, Some(new)) => index_fid_docid_facet_numbers.put(wtxn, key, new)?,
                        (Some(_), None) => {
                            index_fid_docid_facet_numbers.delete(wtxn, key)?;
                        }
                        (Some(_), Some(new)) => {
                            index_fid_docid_facet_numbers.put(wtxn, key, new)?
                        }
                    }
                }
            }
        }
        TypedChunk::FieldIdDocidFacetStrings(_) => {
            let span =
                tracing::trace_span!(target: "indexing::write_db", "field_id_docid_facet_strings");
            let _entered = span.enter();

            let mut builder = MergerBuilder::new(KeepFirst);
            for typed_chunk in typed_chunks {
                let TypedChunk::FieldIdDocidFacetStrings(chunk) = typed_chunk else {
                    unreachable!();
                };

                builder.push(chunk.into_cursor()?);
            }
            let merger = builder.build();

            let index_fid_docid_facet_strings =
                index.field_id_docid_facet_strings.remap_types::<Bytes, Bytes>();
            let mut iter = merger.into_stream_merger_iter()?;
            while let Some((key, value)) = iter.next()? {
                let reader = KvReaderDelAdd::from_slice(value);
                if valid_lmdb_key(key) {
                    match (reader.get(DelAdd::Deletion), reader.get(DelAdd::Addition)) {
                        (None, None) => {}
                        (None, Some(new)) => index_fid_docid_facet_strings.put(wtxn, key, new)?,
                        (Some(_), None) => {
                            index_fid_docid_facet_strings.delete(wtxn, key)?;
                        }
                        (Some(_), Some(new)) => {
                            index_fid_docid_facet_strings.put(wtxn, key, new)?
                        }
                    }
                }
            }
        }
        TypedChunk::GeoPoints(_) => {
            let span = tracing::trace_span!(target: "indexing::write_db", "geo_points");
            let _entered = span.enter();

            let mut builder = MergerBuilder::new(KeepFirst);
            for typed_chunk in typed_chunks {
                let TypedChunk::GeoPoints(chunk) = typed_chunk else {
                    unreachable!();
                };

                builder.push(chunk.into_cursor()?);
            }
            let merger = builder.build();

            let mut rtree = index.geo_rtree(wtxn)?.unwrap_or_default();
            let mut geo_faceted_docids = index.geo_faceted_documents_ids(wtxn)?;

            let mut iter = merger.into_stream_merger_iter()?;
            while let Some((key, value)) = iter.next()? {
                // convert the key back to a u32 (4 bytes)
                let docid = key.try_into().map(DocumentId::from_be_bytes).unwrap();

                let deladd_obkv = KvReaderDelAdd::from_slice(value);
                if let Some(value) = deladd_obkv.get(DelAdd::Deletion) {
                    let geopoint = extract_geo_point(value, docid);
                    rtree.remove(&geopoint);
                    geo_faceted_docids.remove(docid);
                }
                if let Some(value) = deladd_obkv.get(DelAdd::Addition) {
                    let geopoint = extract_geo_point(value, docid);
                    rtree.insert(geopoint);
                    geo_faceted_docids.insert(docid);
                }
            }
            index.put_geo_rtree(wtxn, &rtree)?;
            index.put_geo_faceted_documents_ids(wtxn, &geo_faceted_docids)?;
        }
        TypedChunk::VectorPoints { .. } => {
            let span = tracing::trace_span!(target: "indexing::write_db", "vector_points");
            let _entered = span.enter();

            let mut remove_vectors_builder = MergerBuilder::new(KeepFirst);
            let mut manual_vectors_builder = MergerBuilder::new(KeepFirst);
            let mut embeddings_builder = MergerBuilder::new(KeepFirst);
            let mut add_to_user_provided = RoaringBitmap::new();
            let mut remove_from_user_provided = RoaringBitmap::new();
            let mut params = None;
            for typed_chunk in typed_chunks {
                let TypedChunk::VectorPoints {
                    remove_vectors,
                    manual_vectors,
                    embeddings,
                    expected_dimension,
                    embedder_name,
                    add_to_user_provided: aud,
                    remove_from_user_provided: rud,
                } = typed_chunk
                else {
                    unreachable!();
                };

                params = Some((expected_dimension, embedder_name));

                remove_vectors_builder.push(remove_vectors.into_cursor()?);
                manual_vectors_builder.push(manual_vectors.into_cursor()?);
                if let Some(embeddings) = embeddings {
                    embeddings_builder.push(embeddings.into_cursor()?);
                }
                add_to_user_provided |= aud;
                remove_from_user_provided |= rud;
            }

            // typed chunks has always at least 1 chunk.
            let Some((expected_dimension, embedder_name)) = params else { unreachable!() };

            let mut embedding_configs = index.embedding_configs(wtxn)?;
            let index_embedder_config = embedding_configs
                .iter_mut()
                .find(|IndexEmbeddingConfig { name, .. }| name == &embedder_name)
                .unwrap();
            index_embedder_config.user_provided -= remove_from_user_provided;
            index_embedder_config.user_provided |= add_to_user_provided;

            index.put_embedding_configs(wtxn, embedding_configs)?;

            let embedder_index = index.embedder_category_id.get(wtxn, &embedder_name)?.ok_or(
                InternalError::DatabaseMissingEntry { db_name: "embedder_category_id", key: None },
            )?;
            let binary_quantized = settings_diff
                .old
                .embedding_configs
                .get(&embedder_name)
                .map_or(false, |conf| conf.2);
            // FIXME: allow customizing distance
            let writer = ArroyWrapper::new(index.vector_arroy, embedder_index, binary_quantized);

            // remove vectors for docids we want them removed
            let merger = remove_vectors_builder.build();
            let mut iter = merger.into_stream_merger_iter()?;
            while let Some((key, _)) = iter.next()? {
                let docid = key.try_into().map(DocumentId::from_be_bytes).unwrap();
                writer.del_items(wtxn, expected_dimension, docid)?;
            }

            // add generated embeddings
            let merger = embeddings_builder.build();
            let mut iter = merger.into_stream_merger_iter()?;
            while let Some((key, value)) = iter.next()? {
                let docid = key.try_into().map(DocumentId::from_be_bytes).unwrap();
                let data = pod_collect_to_vec(value);
                // it is a code error to have embeddings and not expected_dimension
                let embeddings = crate::vector::Embeddings::from_inner(data, expected_dimension)
                    // code error if we somehow got the wrong dimension
                    .unwrap();

                if embeddings.embedding_count() > usize::from(u8::MAX) {
                    let external_docid = if let Ok(Some(Ok(index))) = index
                        .external_id_of(wtxn, std::iter::once(docid))
                        .map(|it| it.into_iter().next())
                    {
                        index
                    } else {
                        format!("internal docid={docid}")
                    };
                    return Err(crate::Error::UserError(crate::UserError::TooManyVectors(
                        external_docid,
                        embeddings.embedding_count(),
                    )));
                }
                writer.add_items(wtxn, docid, &embeddings)?;
            }

            // perform the manual diff
            let merger = manual_vectors_builder.build();
            let mut iter = merger.into_stream_merger_iter()?;
            while let Some((key, value)) = iter.next()? {
                // convert the key back to a u32 (4 bytes)
                let (left, _index) = try_split_array_at(key).unwrap();
                let docid = DocumentId::from_be_bytes(left);

                let vector_deladd_obkv = KvReaderDelAdd::from_slice(value);
                if let Some(value) = vector_deladd_obkv.get(DelAdd::Deletion) {
                    let vector: Vec<f32> = pod_collect_to_vec(value);

                    writer.del_item(wtxn, docid, &vector)?;
                }

                if let Some(value) = vector_deladd_obkv.get(DelAdd::Addition) {
                    let vector = pod_collect_to_vec(value);

                    // overflow was detected during vector extraction.
                    writer.add_item(wtxn, docid, &vector)?;
                }
            }

            tracing::debug!("Finished vector chunk for {}", embedder_name);
        }
    }

    Ok((RoaringBitmap::new(), is_merged_database))
}

/// Converts the latitude and longitude back to an xyz GeoPoint.
pub fn extract_geo_point(value: &[u8], docid: DocumentId) -> GeoPoint {
    let (lat, tail) = helpers::try_split_array_at::<u8, 8>(value).unwrap();
    let (lng, _) = helpers::try_split_array_at::<u8, 8>(tail).unwrap();
    let point = [f64::from_ne_bytes(lat), f64::from_ne_bytes(lng)];
    let xyz_point = lat_lng_to_xyz(&point);
    GeoPoint::new(xyz_point, (docid, point))
}

fn merge_word_docids_reader_into_fst<MF>(
    merger: Merger<CursorClonableMmap, MF>,
) -> Result<fst::Set<Vec<u8>>>
where
    MF: MergeFunction,
    crate::Error: From<MF::Error>,
{
    let mut iter = merger.into_stream_merger_iter()?;
    let mut builder = fst::SetBuilder::memory();

    while let Some((k, _)) = iter.next()? {
        builder.insert(k)?;
    }

    Ok(builder.into_set())
}

/// Write provided entries in database using serialize_value function.
/// merge_values function is used if an entry already exist in the database.
#[tracing::instrument(level = "trace", skip_all, target = "indexing::write_db")]
fn write_entries_into_database<R, K, V, FS, FM, MF>(
    merger: Merger<R, MF>,
    database: &heed::Database<K, V>,
    wtxn: &mut RwTxn<'_>,
    serialize_value: FS,
    merge_values: FM,
) -> Result<()>
where
    R: io::Read + io::Seek,
    FS: for<'a> Fn(&'a [u8], &'a mut Vec<u8>) -> Result<&'a [u8]>,
    FM: for<'a> Fn(&[u8], &[u8], &'a mut Vec<u8>) -> Result<Option<&'a [u8]>>,
    MF: MergeFunction,
    crate::Error: From<MF::Error>,
{
    let mut buffer = Vec::new();
    let database = database.remap_types::<Bytes, Bytes>();

    let mut iter = merger.into_stream_merger_iter()?;
    while let Some((key, value)) = iter.next()? {
        if valid_lmdb_key(key) {
            buffer.clear();
            let value = match database.get(wtxn, key)? {
                Some(prev_value) => merge_values(value, prev_value, &mut buffer)?,
                None => Some(serialize_value(value, &mut buffer)?),
            };
            match value {
                Some(value) => database.put(wtxn, key, value)?,
                None => {
                    database.delete(wtxn, key)?;
                }
            }
        }
    }
    Ok(())
}

/// Akin to the `write_entries_into_database` function but specialized
/// for the case when we only index additional searchable fields only.
#[tracing::instrument(level = "trace", skip_all, target = "indexing::write_db")]
fn write_proximity_entries_into_database_additional_searchables<R, MF>(
    merger: Merger<R, MF>,
    database: &heed::Database<U8StrStrCodec, CboRoaringBitmapCodec>,
    wtxn: &mut RwTxn<'_>,
) -> Result<()>
where
    R: io::Read + io::Seek,
    MF: MergeFunction,
    crate::Error: From<MF::Error>,
{
    let mut iter = merger.into_stream_merger_iter()?;
    while let Some((key, value)) = iter.next()? {
        if valid_lmdb_key(key) {
            let (proximity_to_insert, word1, word2) =
                U8StrStrCodec::bytes_decode(key).map_err(heed::Error::Decoding)?;
            let data_to_insert = match KvReaderDelAdd::from_slice(value).get(DelAdd::Addition) {
                Some(value) => {
                    CboRoaringBitmapCodec::bytes_decode(value).map_err(heed::Error::Decoding)?
                }
                None => continue,
            };

            let mut data_to_remove = RoaringBitmap::new();
            for prox in 1..(MAX_DISTANCE as u8) {
                let key = (prox, word1, word2);
                let database_value = database.get(wtxn, &key)?.unwrap_or_default();
                let value = if prox == proximity_to_insert {
                    // Proximity that should be changed.
                    // Union values and remove lower proximity data
                    (&database_value | &data_to_insert) - &data_to_remove
                } else {
                    // Remove lower proximity data
                    &database_value - &data_to_remove
                };

                // add the current data in data_to_remove for the next proximities
                data_to_remove |= &value;

                if database_value != value {
                    database.put(wtxn, &key, &value)?;
                }
            }
        }
    }
    Ok(())
}
