use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::convert::TryInto;
use std::fs::File;
use std::path::Path;

use heed::types::*;
use heed::{CompactionOption, Database, RoTxn, RwTxn, Unspecified};
use roaring::RoaringBitmap;
use rstar::RTree;
use serde::{Deserialize, Serialize};
use zstd::dict::{DecoderDictionary, EncoderDictionary};

use crate::documents::PrimaryKey;
use crate::error::{InternalError, UserError};
use crate::fields_ids_map::FieldsIdsMap;
use crate::heed_codec::facet::{
    FacetGroupKeyCodec, FacetGroupValueCodec, FieldDocIdFacetF64Codec, FieldDocIdFacetStringCodec,
    FieldIdCodec, OrderedF64Codec,
};
use crate::heed_codec::{
    BEU16StrCodec, CompressedKvReaderU16, CompressedObkvCodec, FstSetCodec, StrBEU16Codec,
    StrRefCodec,
};
use crate::order_by_map::OrderByMap;
use crate::proximity::ProximityPrecision;
use crate::vector::parsed_vectors::RESERVED_VECTORS_FIELD_NAME;
use crate::vector::{ArroyWrapper, Embedding, EmbeddingConfig};
use crate::{
    default_criteria, CboRoaringBitmapCodec, Criterion, DocumentId, ExternalDocumentsIds,
    FacetDistribution, FieldDistribution, FieldId, FieldIdMapMissingEntry, FieldIdWordCountCodec,
    FieldidsWeightsMap, GeoPoint, LocalizedAttributesRule, Result, RoaringBitmapCodec,
    RoaringBitmapLenCodec, Search, U8StrStrCodec, Weight, BEU16, BEU32, BEU64,
};

pub const DEFAULT_MIN_WORD_LEN_ONE_TYPO: u8 = 5;
pub const DEFAULT_MIN_WORD_LEN_TWO_TYPOS: u8 = 9;

pub mod main_key {
    pub const CRITERIA_KEY: &str = "criteria";
    pub const DISPLAYED_FIELDS_KEY: &str = "displayed-fields";
    pub const DISTINCT_FIELD_KEY: &str = "distinct-field-key";
    pub const DOCUMENTS_IDS_KEY: &str = "documents-ids";
    pub const HIDDEN_FACETED_FIELDS_KEY: &str = "hidden-faceted-fields";
    pub const FILTERABLE_FIELDS_KEY: &str = "filterable-fields";
    pub const SORTABLE_FIELDS_KEY: &str = "sortable-fields";
    pub const FIELD_DISTRIBUTION_KEY: &str = "fields-distribution";
    pub const FIELDS_IDS_MAP_KEY: &str = "fields-ids-map";
    pub const FIELDIDS_WEIGHTS_MAP_KEY: &str = "fieldids-weights-map";
    pub const GEO_FACETED_DOCUMENTS_IDS_KEY: &str = "geo-faceted-documents-ids";
    pub const GEO_RTREE_KEY: &str = "geo-rtree";
    pub const PRIMARY_KEY_KEY: &str = "primary-key";
    pub const SEARCHABLE_FIELDS_KEY: &str = "searchable-fields";
    pub const USER_DEFINED_SEARCHABLE_FIELDS_KEY: &str = "user-defined-searchable-fields";
    pub const STOP_WORDS_KEY: &str = "stop-words";
    pub const NON_SEPARATOR_TOKENS_KEY: &str = "non-separator-tokens";
    pub const SEPARATOR_TOKENS_KEY: &str = "separator-tokens";
    pub const DICTIONARY_KEY: &str = "dictionary";
    pub const SYNONYMS_KEY: &str = "synonyms";
    pub const USER_DEFINED_SYNONYMS_KEY: &str = "user-defined-synonyms";
    pub const WORDS_FST_KEY: &str = "words-fst";
    pub const WORDS_PREFIXES_FST_KEY: &str = "words-prefixes-fst";
    pub const CREATED_AT_KEY: &str = "created-at";
    pub const UPDATED_AT_KEY: &str = "updated-at";
    pub const AUTHORIZE_TYPOS: &str = "authorize-typos";
    pub const ONE_TYPO_WORD_LEN: &str = "one-typo-word-len";
    pub const TWO_TYPOS_WORD_LEN: &str = "two-typos-word-len";
    pub const EXACT_WORDS: &str = "exact-words";
    pub const EXACT_ATTRIBUTES: &str = "exact-attributes";
    pub const MAX_VALUES_PER_FACET: &str = "max-values-per-facet";
    pub const SORT_FACET_VALUES_BY: &str = "sort-facet-values-by";
    pub const PAGINATION_MAX_TOTAL_HITS: &str = "pagination-max-total-hits";
    pub const PROXIMITY_PRECISION: &str = "proximity-precision";
    pub const EMBEDDING_CONFIGS: &str = "embedding_configs";
    pub const SEARCH_CUTOFF: &str = "search_cutoff";
    pub const DOCUMENT_COMPRESSION_DICTIONARY: &str = "document-compression-dictionary";
    pub const LOCALIZED_ATTRIBUTES_RULES: &str = "localized_attributes_rules";
    pub const FACET_SEARCH: &str = "facet_search";
    pub const PREFIX_SEARCH: &str = "prefix_search";
}

pub mod db_name {
    pub const MAIN: &str = "main";
    pub const WORD_DOCIDS: &str = "word-docids";
    pub const EXACT_WORD_DOCIDS: &str = "exact-word-docids";
    pub const WORD_PREFIX_DOCIDS: &str = "word-prefix-docids";
    pub const EXACT_WORD_PREFIX_DOCIDS: &str = "exact-word-prefix-docids";
    pub const EXTERNAL_DOCUMENTS_IDS: &str = "external-documents-ids";
    pub const DOCID_WORD_POSITIONS: &str = "docid-word-positions";
    pub const WORD_PAIR_PROXIMITY_DOCIDS: &str = "word-pair-proximity-docids";
    pub const WORD_POSITION_DOCIDS: &str = "word-position-docids";
    pub const WORD_FIELD_ID_DOCIDS: &str = "word-field-id-docids";
    pub const WORD_PREFIX_POSITION_DOCIDS: &str = "word-prefix-position-docids";
    pub const WORD_PREFIX_FIELD_ID_DOCIDS: &str = "word-prefix-field-id-docids";
    pub const FIELD_ID_WORD_COUNT_DOCIDS: &str = "field-id-word-count-docids";
    pub const FACET_ID_F64_DOCIDS: &str = "facet-id-f64-docids";
    pub const FACET_ID_EXISTS_DOCIDS: &str = "facet-id-exists-docids";
    pub const FACET_ID_IS_NULL_DOCIDS: &str = "facet-id-is-null-docids";
    pub const FACET_ID_IS_EMPTY_DOCIDS: &str = "facet-id-is-empty-docids";
    pub const FACET_ID_STRING_DOCIDS: &str = "facet-id-string-docids";
    pub const FACET_ID_NORMALIZED_STRING_STRINGS: &str = "facet-id-normalized-string-strings";
    pub const FACET_ID_STRING_FST: &str = "facet-id-string-fst";
    pub const FIELD_ID_DOCID_FACET_F64S: &str = "field-id-docid-facet-f64s";
    pub const FIELD_ID_DOCID_FACET_STRINGS: &str = "field-id-docid-facet-strings";
    pub const VECTOR_EMBEDDER_CATEGORY_ID: &str = "vector-embedder-category-id";
    pub const VECTOR_ARROY: &str = "vector-arroy";
    pub const DOCUMENTS: &str = "documents";
}

#[derive(Clone)]
pub struct Index {
    /// The LMDB environment which this index is associated with.
    pub(crate) env: heed::Env,

    /// Contains many different types (e.g. the fields ids map).
    pub(crate) main: Database<Unspecified, Unspecified>,

    /// Maps the external documents ids with the internal document id.
    pub external_documents_ids: Database<Str, BEU32>,

    /// A word and all the documents ids containing the word.
    pub word_docids: Database<Str, CboRoaringBitmapCodec>,

    /// A word and all the documents ids containing the word, from attributes for which typos are not allowed.
    pub exact_word_docids: Database<Str, CboRoaringBitmapCodec>,

    /// A prefix of word and all the documents ids containing this prefix.
    pub word_prefix_docids: Database<Str, CboRoaringBitmapCodec>,

    /// A prefix of word and all the documents ids containing this prefix, from attributes for which typos are not allowed.
    pub exact_word_prefix_docids: Database<Str, CboRoaringBitmapCodec>,

    /// Maps the proximity between a pair of words with all the docids where this relation appears.
    pub word_pair_proximity_docids: Database<U8StrStrCodec, CboRoaringBitmapCodec>,

    /// Maps the word and the position with the docids that corresponds to it.
    pub word_position_docids: Database<StrBEU16Codec, CboRoaringBitmapCodec>,
    /// Maps the word and the field id with the docids that corresponds to it.
    pub word_fid_docids: Database<StrBEU16Codec, CboRoaringBitmapCodec>,

    /// Maps the field id and the word count with the docids that corresponds to it.
    pub field_id_word_count_docids: Database<FieldIdWordCountCodec, CboRoaringBitmapCodec>,
    /// Maps the word prefix and a position with all the docids where the prefix appears at the position.
    pub word_prefix_position_docids: Database<StrBEU16Codec, CboRoaringBitmapCodec>,
    /// Maps the word prefix and a field id with all the docids where the prefix appears inside the field
    pub word_prefix_fid_docids: Database<StrBEU16Codec, CboRoaringBitmapCodec>,

    /// Maps the facet field id and the docids for which this field exists
    pub facet_id_exists_docids: Database<FieldIdCodec, CboRoaringBitmapCodec>,
    /// Maps the facet field id and the docids for which this field is set as null
    pub facet_id_is_null_docids: Database<FieldIdCodec, CboRoaringBitmapCodec>,
    /// Maps the facet field id and the docids for which this field is considered empty
    pub facet_id_is_empty_docids: Database<FieldIdCodec, CboRoaringBitmapCodec>,

    /// Maps the facet field id and ranges of numbers with the docids that corresponds to them.
    pub facet_id_f64_docids: Database<FacetGroupKeyCodec<OrderedF64Codec>, FacetGroupValueCodec>,
    /// Maps the facet field id and ranges of strings with the docids that corresponds to them.
    pub facet_id_string_docids: Database<FacetGroupKeyCodec<StrRefCodec>, FacetGroupValueCodec>,
    /// Maps the facet field id of the normalized-for-search string facets with their original versions.
    pub facet_id_normalized_string_strings: Database<BEU16StrCodec, SerdeJson<BTreeSet<String>>>,
    /// Maps the facet field id of the string facets with an FST containing all the facets values.
    pub facet_id_string_fst: Database<BEU16, FstSetCodec>,

    /// Maps the document id, the facet field id and the numbers.
    pub field_id_docid_facet_f64s: Database<FieldDocIdFacetF64Codec, Unit>,
    /// Maps the document id, the facet field id and the strings.
    pub field_id_docid_facet_strings: Database<FieldDocIdFacetStringCodec, Str>,

    /// Maps an embedder name to its id in the arroy store.
    pub embedder_category_id: Database<Str, U8>,
    /// Vector store based on arroy™.
    pub vector_arroy: arroy::Database<Unspecified>,

    /// Maps the document id to the document as an obkv store.
    pub(crate) documents: Database<BEU32, CompressedObkvCodec>,
}

impl Index {
    pub fn new_with_creation_dates<P: AsRef<Path>>(
        mut options: heed::EnvOpenOptions,
        path: P,
        created_at: time::OffsetDateTime,
        updated_at: time::OffsetDateTime,
    ) -> Result<Index> {
        use db_name::*;

        options.max_dbs(25);

        let env = unsafe { options.open(path) }?;
        let mut wtxn = env.write_txn()?;
        let main = env.database_options().name(MAIN).create(&mut wtxn)?;
        let word_docids = env.create_database(&mut wtxn, Some(WORD_DOCIDS))?;
        let external_documents_ids =
            env.create_database(&mut wtxn, Some(EXTERNAL_DOCUMENTS_IDS))?;
        let exact_word_docids = env.create_database(&mut wtxn, Some(EXACT_WORD_DOCIDS))?;
        let word_prefix_docids = env.create_database(&mut wtxn, Some(WORD_PREFIX_DOCIDS))?;
        let exact_word_prefix_docids =
            env.create_database(&mut wtxn, Some(EXACT_WORD_PREFIX_DOCIDS))?;
        let word_pair_proximity_docids =
            env.create_database(&mut wtxn, Some(WORD_PAIR_PROXIMITY_DOCIDS))?;
        let word_position_docids = env.create_database(&mut wtxn, Some(WORD_POSITION_DOCIDS))?;
        let word_fid_docids = env.create_database(&mut wtxn, Some(WORD_FIELD_ID_DOCIDS))?;
        let field_id_word_count_docids =
            env.create_database(&mut wtxn, Some(FIELD_ID_WORD_COUNT_DOCIDS))?;
        let word_prefix_position_docids =
            env.create_database(&mut wtxn, Some(WORD_PREFIX_POSITION_DOCIDS))?;
        let word_prefix_fid_docids =
            env.create_database(&mut wtxn, Some(WORD_PREFIX_FIELD_ID_DOCIDS))?;
        let facet_id_f64_docids = env.create_database(&mut wtxn, Some(FACET_ID_F64_DOCIDS))?;
        let facet_id_string_docids =
            env.create_database(&mut wtxn, Some(FACET_ID_STRING_DOCIDS))?;
        let facet_id_normalized_string_strings =
            env.create_database(&mut wtxn, Some(FACET_ID_NORMALIZED_STRING_STRINGS))?;
        let facet_id_string_fst = env.create_database(&mut wtxn, Some(FACET_ID_STRING_FST))?;
        let facet_id_exists_docids =
            env.create_database(&mut wtxn, Some(FACET_ID_EXISTS_DOCIDS))?;
        let facet_id_is_null_docids =
            env.create_database(&mut wtxn, Some(FACET_ID_IS_NULL_DOCIDS))?;
        let facet_id_is_empty_docids =
            env.create_database(&mut wtxn, Some(FACET_ID_IS_EMPTY_DOCIDS))?;
        let field_id_docid_facet_f64s =
            env.create_database(&mut wtxn, Some(FIELD_ID_DOCID_FACET_F64S))?;
        let field_id_docid_facet_strings =
            env.create_database(&mut wtxn, Some(FIELD_ID_DOCID_FACET_STRINGS))?;
        // vector stuff
        let embedder_category_id =
            env.create_database(&mut wtxn, Some(VECTOR_EMBEDDER_CATEGORY_ID))?;
        let vector_arroy = env.create_database(&mut wtxn, Some(VECTOR_ARROY))?;

        let documents = env.create_database(&mut wtxn, Some(DOCUMENTS))?;
        wtxn.commit()?;

        Index::set_creation_dates(&env, main, created_at, updated_at)?;

        Ok(Index {
            env,
            main,
            external_documents_ids,
            word_docids,
            exact_word_docids,
            word_prefix_docids,
            exact_word_prefix_docids,
            word_pair_proximity_docids,
            word_position_docids,
            word_fid_docids,
            word_prefix_position_docids,
            word_prefix_fid_docids,
            field_id_word_count_docids,
            facet_id_f64_docids,
            facet_id_string_docids,
            facet_id_normalized_string_strings,
            facet_id_string_fst,
            facet_id_exists_docids,
            facet_id_is_null_docids,
            facet_id_is_empty_docids,
            field_id_docid_facet_f64s,
            field_id_docid_facet_strings,
            vector_arroy,
            embedder_category_id,
            documents,
        })
    }

    pub fn new<P: AsRef<Path>>(options: heed::EnvOpenOptions, path: P) -> Result<Index> {
        let now = time::OffsetDateTime::now_utc();
        Self::new_with_creation_dates(options, path, now, now)
    }

    fn set_creation_dates(
        env: &heed::Env,
        main: Database<Unspecified, Unspecified>,
        created_at: time::OffsetDateTime,
        updated_at: time::OffsetDateTime,
    ) -> heed::Result<()> {
        let mut txn = env.write_txn()?;
        // The db was just created, we update its metadata with the relevant information.
        let main = main.remap_types::<Str, SerdeJson<OffsetDateTime>>();
        if main.get(&txn, main_key::CREATED_AT_KEY)?.is_none() {
            main.put(&mut txn, main_key::UPDATED_AT_KEY, &OffsetDateTime(updated_at))?;
            main.put(&mut txn, main_key::CREATED_AT_KEY, &OffsetDateTime(created_at))?;
            txn.commit()?;
        }
        Ok(())
    }

    /// Create a write transaction to be able to write into the index.
    pub fn write_txn(&self) -> heed::Result<RwTxn<'_>> {
        self.env.write_txn()
    }

    /// Create a read transaction to be able to read the index.
    pub fn read_txn(&self) -> heed::Result<RoTxn<'_>> {
        self.env.read_txn()
    }

    /// Create a static read transaction to be able to read the index without keeping a reference to it.
    pub fn static_read_txn(&self) -> heed::Result<RoTxn<'static>> {
        self.env.clone().static_read_txn()
    }

    /// Returns the canonicalized path where the heed `Env` of this `Index` lives.
    pub fn path(&self) -> &Path {
        self.env.path()
    }

    /// Returns the size used by the index without the cached pages.
    pub fn used_size(&self) -> Result<u64> {
        Ok(self.env.non_free_pages_size()?)
    }

    /// Returns the real size used by the index.
    pub fn on_disk_size(&self) -> Result<u64> {
        Ok(self.env.real_disk_size()?)
    }

    /// Returns the map size the underlying environment was opened with, in bytes.
    ///
    /// This value does not represent the current on-disk size of the index.
    ///
    /// This value is the maximum between the map size passed during the opening of the index
    /// and the on-disk size of the index at the time of opening.
    pub fn map_size(&self) -> usize {
        self.env.info().map_size
    }

    pub fn copy_to_file<P: AsRef<Path>>(&self, path: P, option: CompactionOption) -> Result<File> {
        self.env.copy_to_file(path, option).map_err(Into::into)
    }

    /// Returns an `EnvClosingEvent` that can be used to wait for the closing event,
    /// multiple threads can wait on this event.
    ///
    /// Make sure that you drop all the copies of `Index`es you have, env closing are triggered
    /// when all references are dropped, the last one will eventually close the environment.
    pub fn prepare_for_closing(self) -> heed::EnvClosingEvent {
        self.env.prepare_for_closing()
    }

    /* document compression dictionary */

    /// Writes the dictionnary that will further be used to compress the documents.
    pub fn put_document_compression_dictionary(
        &self,
        wtxn: &mut RwTxn,
        dictionary: &[u8],
    ) -> heed::Result<()> {
        self.main.remap_types::<Str, Bytes>().put(
            wtxn,
            main_key::DOCUMENT_COMPRESSION_DICTIONARY,
            dictionary,
        )
    }

    /// Deletes the document compression dictionary.
    pub fn delete_document_compression_dictionary(&self, wtxn: &mut RwTxn) -> heed::Result<bool> {
        self.main.remap_key_type::<Str>().delete(wtxn, main_key::DOCUMENT_COMPRESSION_DICTIONARY)
    }

    /// Returns the optional raw bytes dictionary to be used when reading or writing the OBKV documents.
    pub fn document_compression_raw_dictionary<'t>(
        &self,
        rtxn: &'t RoTxn,
    ) -> heed::Result<Option<&'t [u8]>> {
        self.main.remap_types::<Str, Bytes>().get(rtxn, main_key::DOCUMENT_COMPRESSION_DICTIONARY)
    }

    pub fn document_decompression_dictionary<'t>(
        &self,
        rtxn: &'t RoTxn,
    ) -> heed::Result<Option<DecoderDictionary<'t>>> {
        self.document_compression_raw_dictionary(rtxn).map(|opt| opt.map(DecoderDictionary::new))
    }

    pub fn document_compression_dictionary(
        &self,
        rtxn: &RoTxn,
    ) -> heed::Result<Option<EncoderDictionary<'static>>> {
        const COMPRESSION_LEVEL: i32 = 19;
        self.document_compression_raw_dictionary(rtxn)
            .map(|opt| opt.map(|bytes| EncoderDictionary::copy(bytes, COMPRESSION_LEVEL)))
    }

    /* documents ids */

    /// Writes the documents ids that corresponds to the user-ids-documents-ids FST.
    pub(crate) fn put_documents_ids(
        &self,
        wtxn: &mut RwTxn<'_>,
        docids: &RoaringBitmap,
    ) -> heed::Result<()> {
        self.main.remap_types::<Str, RoaringBitmapCodec>().put(
            wtxn,
            main_key::DOCUMENTS_IDS_KEY,
            docids,
        )
    }

    /// Returns the internal documents ids.
    pub fn documents_ids(&self, rtxn: &RoTxn<'_>) -> heed::Result<RoaringBitmap> {
        Ok(self
            .main
            .remap_types::<Str, RoaringBitmapCodec>()
            .get(rtxn, main_key::DOCUMENTS_IDS_KEY)?
            .unwrap_or_default())
    }

    /// Returns the number of documents indexed in the database.
    pub fn number_of_documents(&self, rtxn: &RoTxn<'_>) -> Result<u64> {
        let count = self
            .main
            .remap_types::<Str, RoaringBitmapLenCodec>()
            .get(rtxn, main_key::DOCUMENTS_IDS_KEY)?;
        Ok(count.unwrap_or_default())
    }

    /* primary key */

    /// Writes the documents primary key, this is the field name that is used to store the id.
    pub(crate) fn put_primary_key(
        &self,
        wtxn: &mut RwTxn<'_>,
        primary_key: &str,
    ) -> heed::Result<()> {
        self.set_updated_at(wtxn, &time::OffsetDateTime::now_utc())?;
        self.main.remap_types::<Str, Str>().put(wtxn, main_key::PRIMARY_KEY_KEY, primary_key)
    }

    /// Deletes the primary key of the documents, this can be done to reset indexes settings.
    pub(crate) fn delete_primary_key(&self, wtxn: &mut RwTxn<'_>) -> heed::Result<bool> {
        self.main.remap_key_type::<Str>().delete(wtxn, main_key::PRIMARY_KEY_KEY)
    }

    /// Returns the documents primary key, `None` if it hasn't been defined.
    pub fn primary_key<'t>(&self, rtxn: &'t RoTxn<'_>) -> heed::Result<Option<&'t str>> {
        self.main.remap_types::<Str, Str>().get(rtxn, main_key::PRIMARY_KEY_KEY)
    }

    /* external documents ids */

    /// Returns the external documents ids map which associate the external ids
    /// with the internal ids (i.e. `u32`).
    pub fn external_documents_ids(&self) -> ExternalDocumentsIds {
        ExternalDocumentsIds::new(self.external_documents_ids)
    }

    /* fields ids map */

    /// Writes the fields ids map which associate the documents keys with an internal field id
    /// (i.e. `u8`), this field id is used to identify fields in the obkv documents.
    pub(crate) fn put_fields_ids_map(
        &self,
        wtxn: &mut RwTxn<'_>,
        map: &FieldsIdsMap,
    ) -> heed::Result<()> {
        self.main.remap_types::<Str, SerdeJson<FieldsIdsMap>>().put(
            wtxn,
            main_key::FIELDS_IDS_MAP_KEY,
            map,
        )
    }

    /// Returns the fields ids map which associate the documents keys with an internal field id
    /// (i.e. `u8`), this field id is used to identify fields in the obkv documents.
    pub fn fields_ids_map(&self, rtxn: &RoTxn<'_>) -> heed::Result<FieldsIdsMap> {
        Ok(self
            .main
            .remap_types::<Str, SerdeJson<FieldsIdsMap>>()
            .get(rtxn, main_key::FIELDS_IDS_MAP_KEY)?
            .unwrap_or_default())
    }

    /* fieldids weights map */
    // This maps the fields ids to their weights.
    // Their weights is defined by the ordering of the searchable attributes.

    /// Writes the fieldids weights map which associates the field ids to their weights
    pub(crate) fn put_fieldids_weights_map(
        &self,
        wtxn: &mut RwTxn<'_>,
        map: &FieldidsWeightsMap,
    ) -> heed::Result<()> {
        self.main.remap_types::<Str, SerdeJson<_>>().put(
            wtxn,
            main_key::FIELDIDS_WEIGHTS_MAP_KEY,
            map,
        )
    }

    /// Get the fieldids weights map which associates the field ids to their weights
    pub fn fieldids_weights_map(&self, rtxn: &RoTxn<'_>) -> heed::Result<FieldidsWeightsMap> {
        self.main
            .remap_types::<Str, SerdeJson<_>>()
            .get(rtxn, main_key::FIELDIDS_WEIGHTS_MAP_KEY)?
            .map(Ok)
            .unwrap_or_else(|| {
                Ok(FieldidsWeightsMap::from_field_id_map_without_searchable(
                    &self.fields_ids_map(rtxn)?,
                ))
            })
    }

    /// Delete the fieldsids weights map
    pub fn delete_fieldids_weights_map(&self, wtxn: &mut RwTxn<'_>) -> heed::Result<bool> {
        self.main.remap_key_type::<Str>().delete(wtxn, main_key::FIELDIDS_WEIGHTS_MAP_KEY)
    }

    pub fn searchable_fields_and_weights<'a>(
        &self,
        rtxn: &'a RoTxn<'a>,
    ) -> Result<Vec<(Cow<'a, str>, FieldId, Weight)>> {
        let fid_map = self.fields_ids_map(rtxn)?;
        let weight_map = self.fieldids_weights_map(rtxn)?;
        let searchable = self.searchable_fields(rtxn)?;

        searchable
            .into_iter()
            .map(|field| -> Result<_> {
                let fid = fid_map.id(&field).ok_or_else(|| FieldIdMapMissingEntry::FieldName {
                    field_name: field.to_string(),
                    process: "searchable_fields_and_weights",
                })?;
                let weight = weight_map
                    .weight(fid)
                    .ok_or(InternalError::FieldidsWeightsMapMissingEntry { key: fid })?;

                Ok((field, fid, weight))
            })
            .collect()
    }

    /* geo rtree */

    /// Writes the provided `rtree` which associates coordinates to documents ids.
    pub(crate) fn put_geo_rtree(
        &self,
        wtxn: &mut RwTxn<'_>,
        rtree: &RTree<GeoPoint>,
    ) -> heed::Result<()> {
        self.main.remap_types::<Str, SerdeBincode<RTree<GeoPoint>>>().put(
            wtxn,
            main_key::GEO_RTREE_KEY,
            rtree,
        )
    }

    /// Delete the `rtree` which associates coordinates to documents ids.
    pub(crate) fn delete_geo_rtree(&self, wtxn: &mut RwTxn<'_>) -> heed::Result<bool> {
        self.main.remap_key_type::<Str>().delete(wtxn, main_key::GEO_RTREE_KEY)
    }

    /// Returns the `rtree` which associates coordinates to documents ids.
    pub fn geo_rtree(&self, rtxn: &RoTxn<'_>) -> Result<Option<RTree<GeoPoint>>> {
        match self
            .main
            .remap_types::<Str, SerdeBincode<RTree<GeoPoint>>>()
            .get(rtxn, main_key::GEO_RTREE_KEY)?
        {
            Some(rtree) => Ok(Some(rtree)),
            None => Ok(None),
        }
    }

    /* geo faceted */

    /// Writes the documents ids that are faceted with a _geo field.
    pub(crate) fn put_geo_faceted_documents_ids(
        &self,
        wtxn: &mut RwTxn<'_>,
        docids: &RoaringBitmap,
    ) -> heed::Result<()> {
        self.main.remap_types::<Str, RoaringBitmapCodec>().put(
            wtxn,
            main_key::GEO_FACETED_DOCUMENTS_IDS_KEY,
            docids,
        )
    }

    /// Delete the documents ids that are faceted with a _geo field.
    pub(crate) fn delete_geo_faceted_documents_ids(
        &self,
        wtxn: &mut RwTxn<'_>,
    ) -> heed::Result<bool> {
        self.main.remap_key_type::<Str>().delete(wtxn, main_key::GEO_FACETED_DOCUMENTS_IDS_KEY)
    }

    /// Retrieve all the documents ids that are faceted with a _geo field.
    pub fn geo_faceted_documents_ids(&self, rtxn: &RoTxn<'_>) -> heed::Result<RoaringBitmap> {
        match self
            .main
            .remap_types::<Str, RoaringBitmapCodec>()
            .get(rtxn, main_key::GEO_FACETED_DOCUMENTS_IDS_KEY)?
        {
            Some(docids) => Ok(docids),
            None => Ok(RoaringBitmap::new()),
        }
    }
    /* field distribution */

    /// Writes the field distribution which associates every field name with
    /// the number of times it occurs in the documents.
    pub(crate) fn put_field_distribution(
        &self,
        wtxn: &mut RwTxn<'_>,
        distribution: &FieldDistribution,
    ) -> heed::Result<()> {
        self.main.remap_types::<Str, SerdeJson<FieldDistribution>>().put(
            wtxn,
            main_key::FIELD_DISTRIBUTION_KEY,
            distribution,
        )
    }

    /// Returns the field distribution which associates every field name with
    /// the number of times it occurs in the documents.
    pub fn field_distribution(&self, rtxn: &RoTxn<'_>) -> heed::Result<FieldDistribution> {
        Ok(self
            .main
            .remap_types::<Str, SerdeJson<FieldDistribution>>()
            .get(rtxn, main_key::FIELD_DISTRIBUTION_KEY)?
            .unwrap_or_default())
    }

    /* displayed fields */

    /// Writes the fields that must be displayed in the defined order.
    /// There must be not be any duplicate field id.
    pub(crate) fn put_displayed_fields(
        &self,
        wtxn: &mut RwTxn<'_>,
        fields: &[&str],
    ) -> heed::Result<()> {
        self.main.remap_types::<Str, SerdeBincode<&[&str]>>().put(
            wtxn,
            main_key::DISPLAYED_FIELDS_KEY,
            &fields,
        )
    }

    /// Deletes the displayed fields ids, this will make the engine to display
    /// all the documents attributes in the order of the `FieldsIdsMap`.
    pub(crate) fn delete_displayed_fields(&self, wtxn: &mut RwTxn<'_>) -> heed::Result<bool> {
        self.main.remap_key_type::<Str>().delete(wtxn, main_key::DISPLAYED_FIELDS_KEY)
    }

    /// Returns the displayed fields in the order they were set by the user. If it returns
    /// `None` it means that all the attributes are set as displayed in the order of the `FieldsIdsMap`.
    pub fn displayed_fields<'t>(&self, rtxn: &'t RoTxn<'_>) -> heed::Result<Option<Vec<&'t str>>> {
        self.main
            .remap_types::<Str, SerdeBincode<Vec<&'t str>>>()
            .get(rtxn, main_key::DISPLAYED_FIELDS_KEY)
    }

    /// Identical to `displayed_fields`, but returns the ids instead.
    pub fn displayed_fields_ids(&self, rtxn: &RoTxn<'_>) -> Result<Option<Vec<FieldId>>> {
        match self.displayed_fields(rtxn)? {
            Some(fields) => {
                let fields_ids_map = self.fields_ids_map(rtxn)?;
                let mut fields_ids = Vec::new();
                for name in fields.into_iter() {
                    if let Some(field_id) = fields_ids_map.id(name) {
                        fields_ids.push(field_id);
                    }
                }
                Ok(Some(fields_ids))
            }
            None => Ok(None),
        }
    }

    /* remove hidden fields */
    pub fn remove_hidden_fields(
        &self,
        rtxn: &RoTxn<'_>,
        fields: impl IntoIterator<Item = impl AsRef<str>>,
    ) -> Result<(BTreeSet<String>, bool)> {
        let mut valid_fields =
            fields.into_iter().map(|f| f.as_ref().to_string()).collect::<BTreeSet<String>>();

        let fields_len = valid_fields.len();

        if let Some(dn) = self.displayed_fields(rtxn)? {
            let displayable_names = dn.iter().map(|s| s.to_string()).collect();
            valid_fields = &valid_fields & &displayable_names;
        }

        let hidden_fields = fields_len > valid_fields.len();
        Ok((valid_fields, hidden_fields))
    }

    /* searchable fields */

    /// Write the user defined searchable fields and generate the real searchable fields from the specified fields ids map.
    pub(crate) fn put_all_searchable_fields_from_fields_ids_map(
        &self,
        wtxn: &mut RwTxn<'_>,
        user_fields: &[&str],
        non_searchable_fields_ids: &[FieldId],
        fields_ids_map: &FieldsIdsMap,
    ) -> Result<()> {
        // We can write the user defined searchable fields as-is.
        self.put_user_defined_searchable_fields(wtxn, user_fields)?;

        let mut weights = FieldidsWeightsMap::default();

        // Now we generate the real searchable fields:
        // 1. Take the user defined searchable fields as-is to keep the priority defined by the attributes criterion.
        // 2. Iterate over the user defined searchable fields.
        // 3. If a user defined field is a subset of a field defined in the fields_ids_map
        // (ie doggo.name is a subset of doggo) right after doggo and with the same weight.
        let mut real_fields = Vec::new();

        for (id, field_from_map) in fields_ids_map.iter() {
            for (weight, user_field) in user_fields.iter().enumerate() {
                if crate::is_faceted_by(field_from_map, user_field)
                    && !real_fields.contains(&field_from_map)
                    && !non_searchable_fields_ids.contains(&id)
                {
                    real_fields.push(field_from_map);

                    let weight: u16 =
                        weight.try_into().map_err(|_| UserError::AttributeLimitReached)?;
                    weights.insert(id, weight);
                }
            }
        }

        self.put_searchable_fields(wtxn, &real_fields)?;
        self.put_fieldids_weights_map(wtxn, &weights)?;
        Ok(())
    }

    pub(crate) fn delete_all_searchable_fields(&self, wtxn: &mut RwTxn<'_>) -> heed::Result<bool> {
        let did_delete_searchable = self.delete_searchable_fields(wtxn)?;
        let did_delete_user_defined = self.delete_user_defined_searchable_fields(wtxn)?;
        self.delete_fieldids_weights_map(wtxn)?;
        Ok(did_delete_searchable || did_delete_user_defined)
    }

    /// Writes the searchable fields, when this list is specified, only these are indexed.
    fn put_searchable_fields(&self, wtxn: &mut RwTxn<'_>, fields: &[&str]) -> heed::Result<()> {
        self.main.remap_types::<Str, SerdeBincode<&[&str]>>().put(
            wtxn,
            main_key::SEARCHABLE_FIELDS_KEY,
            &fields,
        )
    }

    /// Deletes the searchable fields, when no fields are specified, all fields are indexed.
    fn delete_searchable_fields(&self, wtxn: &mut RwTxn<'_>) -> heed::Result<bool> {
        self.main.remap_key_type::<Str>().delete(wtxn, main_key::SEARCHABLE_FIELDS_KEY)
    }

    /// Returns the searchable fields, those are the fields that are indexed,
    pub fn searchable_fields<'t>(&self, rtxn: &'t RoTxn<'_>) -> heed::Result<Vec<Cow<'t, str>>> {
        self.main
            .remap_types::<Str, SerdeBincode<Vec<&'t str>>>()
            .get(rtxn, main_key::SEARCHABLE_FIELDS_KEY)?
            .map(|fields| Ok(fields.into_iter().map(Cow::Borrowed).collect()))
            .unwrap_or_else(|| {
                Ok(self
                    .fields_ids_map(rtxn)?
                    .names()
                    .filter(|name| !crate::is_faceted_by(name, RESERVED_VECTORS_FIELD_NAME))
                    .map(|field| Cow::Owned(field.to_string()))
                    .collect())
            })
    }

    /// Identical to `searchable_fields`, but returns the ids instead.
    pub fn searchable_fields_ids(&self, rtxn: &RoTxn<'_>) -> Result<Vec<FieldId>> {
        let fields = self.searchable_fields(rtxn)?;
        let fields_ids_map = self.fields_ids_map(rtxn)?;
        let mut fields_ids = Vec::new();
        for name in fields {
            if let Some(field_id) = fields_ids_map.id(&name) {
                fields_ids.push(field_id);
            }
        }
        Ok(fields_ids)
    }

    /// Writes the searchable fields, when this list is specified, only these are indexed.
    pub(crate) fn put_user_defined_searchable_fields(
        &self,
        wtxn: &mut RwTxn<'_>,
        fields: &[&str],
    ) -> heed::Result<()> {
        self.main.remap_types::<Str, SerdeBincode<_>>().put(
            wtxn,
            main_key::USER_DEFINED_SEARCHABLE_FIELDS_KEY,
            &fields,
        )
    }

    /// Deletes the searchable fields, when no fields are specified, all fields are indexed.
    pub(crate) fn delete_user_defined_searchable_fields(
        &self,
        wtxn: &mut RwTxn<'_>,
    ) -> heed::Result<bool> {
        self.main.remap_key_type::<Str>().delete(wtxn, main_key::USER_DEFINED_SEARCHABLE_FIELDS_KEY)
    }

    /// Returns the user defined searchable fields.
    pub fn user_defined_searchable_fields<'t>(
        &self,
        rtxn: &'t RoTxn<'t>,
    ) -> heed::Result<Option<Vec<&'t str>>> {
        self.main
            .remap_types::<Str, SerdeBincode<Vec<_>>>()
            .get(rtxn, main_key::USER_DEFINED_SEARCHABLE_FIELDS_KEY)
    }

    /// Identical to `user_defined_searchable_fields`, but returns ids instead.
    pub fn user_defined_searchable_fields_ids(
        &self,
        rtxn: &RoTxn<'_>,
    ) -> Result<Option<Vec<FieldId>>> {
        match self.user_defined_searchable_fields(rtxn)? {
            Some(fields) => {
                let fields_ids_map = self.fields_ids_map(rtxn)?;
                let mut fields_ids = Vec::new();
                for name in fields {
                    if let Some(field_id) = fields_ids_map.id(name) {
                        fields_ids.push(field_id);
                    }
                }
                Ok(Some(fields_ids))
            }
            None => Ok(None),
        }
    }

    /* filterable fields */

    /// Writes the filterable fields names in the database.
    pub(crate) fn put_filterable_fields(
        &self,
        wtxn: &mut RwTxn<'_>,
        fields: &HashSet<String>,
    ) -> heed::Result<()> {
        self.main.remap_types::<Str, SerdeJson<_>>().put(
            wtxn,
            main_key::FILTERABLE_FIELDS_KEY,
            fields,
        )
    }

    /// Deletes the filterable fields ids in the database.
    pub(crate) fn delete_filterable_fields(&self, wtxn: &mut RwTxn<'_>) -> heed::Result<bool> {
        self.main.remap_key_type::<Str>().delete(wtxn, main_key::FILTERABLE_FIELDS_KEY)
    }

    /// Returns the filterable fields names.
    pub fn filterable_fields(&self, rtxn: &RoTxn<'_>) -> heed::Result<HashSet<String>> {
        Ok(self
            .main
            .remap_types::<Str, SerdeJson<_>>()
            .get(rtxn, main_key::FILTERABLE_FIELDS_KEY)?
            .unwrap_or_default())
    }

    /// Identical to `filterable_fields`, but returns ids instead.
    pub fn filterable_fields_ids(&self, rtxn: &RoTxn<'_>) -> Result<HashSet<FieldId>> {
        let fields = self.filterable_fields(rtxn)?;
        let fields_ids_map = self.fields_ids_map(rtxn)?;

        let mut fields_ids = HashSet::new();
        for name in fields {
            if let Some(field_id) = fields_ids_map.id(&name) {
                fields_ids.insert(field_id);
            }
        }

        Ok(fields_ids)
    }

    /* sortable fields */

    /// Writes the sortable fields names in the database.
    pub(crate) fn put_sortable_fields(
        &self,
        wtxn: &mut RwTxn<'_>,
        fields: &HashSet<String>,
    ) -> heed::Result<()> {
        self.main.remap_types::<Str, SerdeJson<_>>().put(
            wtxn,
            main_key::SORTABLE_FIELDS_KEY,
            fields,
        )
    }

    /// Deletes the sortable fields ids in the database.
    pub(crate) fn delete_sortable_fields(&self, wtxn: &mut RwTxn<'_>) -> heed::Result<bool> {
        self.main.remap_key_type::<Str>().delete(wtxn, main_key::SORTABLE_FIELDS_KEY)
    }

    /// Returns the sortable fields names.
    pub fn sortable_fields(&self, rtxn: &RoTxn<'_>) -> heed::Result<HashSet<String>> {
        Ok(self
            .main
            .remap_types::<Str, SerdeJson<_>>()
            .get(rtxn, main_key::SORTABLE_FIELDS_KEY)?
            .unwrap_or_default())
    }

    /// Identical to `sortable_fields`, but returns ids instead.
    pub fn sortable_fields_ids(&self, rtxn: &RoTxn<'_>) -> Result<HashSet<FieldId>> {
        let fields = self.sortable_fields(rtxn)?;
        let fields_ids_map = self.fields_ids_map(rtxn)?;
        Ok(fields.into_iter().filter_map(|name| fields_ids_map.id(&name)).collect())
    }

    /* faceted fields */

    /// Writes the faceted fields in the database.
    pub(crate) fn put_faceted_fields(
        &self,
        wtxn: &mut RwTxn<'_>,
        fields: &HashSet<String>,
    ) -> heed::Result<()> {
        self.main.remap_types::<Str, SerdeJson<_>>().put(
            wtxn,
            main_key::HIDDEN_FACETED_FIELDS_KEY,
            fields,
        )
    }

    /// Returns the faceted fields names.
    pub fn faceted_fields(&self, rtxn: &RoTxn<'_>) -> heed::Result<HashSet<String>> {
        Ok(self
            .main
            .remap_types::<Str, SerdeJson<_>>()
            .get(rtxn, main_key::HIDDEN_FACETED_FIELDS_KEY)?
            .unwrap_or_default())
    }

    /// Identical to `faceted_fields`, but returns ids instead.
    pub fn faceted_fields_ids(&self, rtxn: &RoTxn<'_>) -> Result<HashSet<FieldId>> {
        let fields = self.faceted_fields(rtxn)?;
        let fields_ids_map = self.fields_ids_map(rtxn)?;

        let mut fields_ids = HashSet::new();
        for name in fields {
            if let Some(field_id) = fields_ids_map.id(&name) {
                fields_ids.insert(field_id);
            }
        }

        Ok(fields_ids)
    }

    /* faceted documents ids */

    /// Returns the user defined faceted fields names.
    ///
    /// The user faceted fields are the union of all the filterable, sortable, distinct, and Asc/Desc fields.
    pub fn user_defined_faceted_fields(&self, rtxn: &RoTxn<'_>) -> Result<HashSet<String>> {
        let filterable_fields = self.filterable_fields(rtxn)?;
        let sortable_fields = self.sortable_fields(rtxn)?;
        let distinct_field = self.distinct_field(rtxn)?;
        let asc_desc_fields =
            self.criteria(rtxn)?.into_iter().filter_map(|criterion| match criterion {
                Criterion::Asc(field) | Criterion::Desc(field) => Some(field),
                _otherwise => None,
            });

        let mut faceted_fields = filterable_fields;
        faceted_fields.extend(sortable_fields);
        faceted_fields.extend(asc_desc_fields);
        if let Some(field) = distinct_field {
            faceted_fields.insert(field.to_owned());
        }

        Ok(faceted_fields)
    }

    /// Identical to `user_defined_faceted_fields`, but returns ids instead.
    pub fn user_defined_faceted_fields_ids(&self, rtxn: &RoTxn<'_>) -> Result<HashSet<FieldId>> {
        let fields = self.user_defined_faceted_fields(rtxn)?;
        let fields_ids_map = self.fields_ids_map(rtxn)?;

        let mut fields_ids = HashSet::new();
        for name in fields {
            if let Some(field_id) = fields_ids_map.id(&name) {
                fields_ids.insert(field_id);
            }
        }

        Ok(fields_ids)
    }

    /* faceted documents ids */

    /// Retrieve all the documents which contain this field id set as null
    pub fn null_faceted_documents_ids(
        &self,
        rtxn: &RoTxn<'_>,
        field_id: FieldId,
    ) -> heed::Result<RoaringBitmap> {
        match self.facet_id_is_null_docids.get(rtxn, &field_id)? {
            Some(docids) => Ok(docids),
            None => Ok(RoaringBitmap::new()),
        }
    }

    /// Retrieve all the documents which contain this field id and that is considered empty
    pub fn empty_faceted_documents_ids(
        &self,
        rtxn: &RoTxn<'_>,
        field_id: FieldId,
    ) -> heed::Result<RoaringBitmap> {
        match self.facet_id_is_empty_docids.get(rtxn, &field_id)? {
            Some(docids) => Ok(docids),
            None => Ok(RoaringBitmap::new()),
        }
    }

    /// Retrieve all the documents which contain this field id
    pub fn exists_faceted_documents_ids(
        &self,
        rtxn: &RoTxn<'_>,
        field_id: FieldId,
    ) -> heed::Result<RoaringBitmap> {
        match self.facet_id_exists_docids.get(rtxn, &field_id)? {
            Some(docids) => Ok(docids),
            None => Ok(RoaringBitmap::new()),
        }
    }

    /* distinct field */

    pub(crate) fn put_distinct_field(
        &self,
        wtxn: &mut RwTxn<'_>,
        distinct_field: &str,
    ) -> heed::Result<()> {
        self.main.remap_types::<Str, Str>().put(wtxn, main_key::DISTINCT_FIELD_KEY, distinct_field)
    }

    pub fn distinct_field<'a>(&self, rtxn: &'a RoTxn<'_>) -> heed::Result<Option<&'a str>> {
        self.main.remap_types::<Str, Str>().get(rtxn, main_key::DISTINCT_FIELD_KEY)
    }

    pub(crate) fn delete_distinct_field(&self, wtxn: &mut RwTxn<'_>) -> heed::Result<bool> {
        self.main.remap_key_type::<Str>().delete(wtxn, main_key::DISTINCT_FIELD_KEY)
    }

    /* criteria */

    pub(crate) fn put_criteria(
        &self,
        wtxn: &mut RwTxn<'_>,
        criteria: &[Criterion],
    ) -> heed::Result<()> {
        self.main.remap_types::<Str, SerdeJson<&[Criterion]>>().put(
            wtxn,
            main_key::CRITERIA_KEY,
            &criteria,
        )
    }

    pub(crate) fn delete_criteria(&self, wtxn: &mut RwTxn<'_>) -> heed::Result<bool> {
        self.main.remap_key_type::<Str>().delete(wtxn, main_key::CRITERIA_KEY)
    }

    pub fn criteria(&self, rtxn: &RoTxn<'_>) -> heed::Result<Vec<Criterion>> {
        match self
            .main
            .remap_types::<Str, SerdeJson<Vec<Criterion>>>()
            .get(rtxn, main_key::CRITERIA_KEY)?
        {
            Some(criteria) => Ok(criteria),
            None => Ok(default_criteria()),
        }
    }

    /* words fst */

    /// Writes the FST which is the words dictionary of the engine.
    pub(crate) fn put_words_fst<A: AsRef<[u8]>>(
        &self,
        wtxn: &mut RwTxn<'_>,
        fst: &fst::Set<A>,
    ) -> heed::Result<()> {
        self.main.remap_types::<Str, Bytes>().put(
            wtxn,
            main_key::WORDS_FST_KEY,
            fst.as_fst().as_bytes(),
        )
    }

    /// Returns the FST which is the words dictionary of the engine.
    pub fn words_fst<'t>(&self, rtxn: &'t RoTxn<'_>) -> Result<fst::Set<Cow<'t, [u8]>>> {
        match self.main.remap_types::<Str, Bytes>().get(rtxn, main_key::WORDS_FST_KEY)? {
            Some(bytes) => Ok(fst::Set::new(bytes)?.map_data(Cow::Borrowed)?),
            None => Ok(fst::Set::default().map_data(Cow::Owned)?),
        }
    }

    /* stop words */

    pub(crate) fn put_stop_words<A: AsRef<[u8]>>(
        &self,
        wtxn: &mut RwTxn<'_>,
        fst: &fst::Set<A>,
    ) -> heed::Result<()> {
        self.main.remap_types::<Str, Bytes>().put(
            wtxn,
            main_key::STOP_WORDS_KEY,
            fst.as_fst().as_bytes(),
        )
    }

    pub(crate) fn delete_stop_words(&self, wtxn: &mut RwTxn<'_>) -> heed::Result<bool> {
        self.main.remap_key_type::<Str>().delete(wtxn, main_key::STOP_WORDS_KEY)
    }

    pub fn stop_words<'t>(&self, rtxn: &'t RoTxn<'t>) -> Result<Option<fst::Set<&'t [u8]>>> {
        match self.main.remap_types::<Str, Bytes>().get(rtxn, main_key::STOP_WORDS_KEY)? {
            Some(bytes) => Ok(Some(fst::Set::new(bytes)?)),
            None => Ok(None),
        }
    }

    /* non separator tokens */

    pub(crate) fn put_non_separator_tokens(
        &self,
        wtxn: &mut RwTxn<'_>,
        set: &BTreeSet<String>,
    ) -> heed::Result<()> {
        self.main.remap_types::<Str, SerdeBincode<_>>().put(
            wtxn,
            main_key::NON_SEPARATOR_TOKENS_KEY,
            set,
        )
    }

    pub(crate) fn delete_non_separator_tokens(&self, wtxn: &mut RwTxn<'_>) -> heed::Result<bool> {
        self.main.remap_key_type::<Str>().delete(wtxn, main_key::NON_SEPARATOR_TOKENS_KEY)
    }

    pub fn non_separator_tokens(&self, rtxn: &RoTxn<'_>) -> Result<Option<BTreeSet<String>>> {
        Ok(self
            .main
            .remap_types::<Str, SerdeBincode<BTreeSet<String>>>()
            .get(rtxn, main_key::NON_SEPARATOR_TOKENS_KEY)?)
    }

    /* separator tokens */

    pub(crate) fn put_separator_tokens(
        &self,
        wtxn: &mut RwTxn<'_>,
        set: &BTreeSet<String>,
    ) -> heed::Result<()> {
        self.main.remap_types::<Str, SerdeBincode<_>>().put(
            wtxn,
            main_key::SEPARATOR_TOKENS_KEY,
            set,
        )
    }

    pub(crate) fn delete_separator_tokens(&self, wtxn: &mut RwTxn<'_>) -> heed::Result<bool> {
        self.main.remap_key_type::<Str>().delete(wtxn, main_key::SEPARATOR_TOKENS_KEY)
    }

    pub fn separator_tokens(&self, rtxn: &RoTxn<'_>) -> Result<Option<BTreeSet<String>>> {
        Ok(self
            .main
            .remap_types::<Str, SerdeBincode<BTreeSet<String>>>()
            .get(rtxn, main_key::SEPARATOR_TOKENS_KEY)?)
    }

    /* separators easing method */

    pub fn allowed_separators(&self, rtxn: &RoTxn<'_>) -> Result<Option<BTreeSet<String>>> {
        let default_separators =
            charabia::separators::DEFAULT_SEPARATORS.iter().map(|s| s.to_string());
        let mut separators: Option<BTreeSet<_>> = None;
        if let Some(mut separator_tokens) = self.separator_tokens(rtxn)? {
            separator_tokens.extend(default_separators.clone());
            separators = Some(separator_tokens);
        }

        if let Some(non_separator_tokens) = self.non_separator_tokens(rtxn)? {
            separators = separators
                .or_else(|| Some(default_separators.collect()))
                .map(|separators| &separators - &non_separator_tokens);
        }

        Ok(separators)
    }

    /* dictionary */

    pub(crate) fn put_dictionary(
        &self,
        wtxn: &mut RwTxn<'_>,
        set: &BTreeSet<String>,
    ) -> heed::Result<()> {
        self.main.remap_types::<Str, SerdeBincode<_>>().put(wtxn, main_key::DICTIONARY_KEY, set)
    }

    pub(crate) fn delete_dictionary(&self, wtxn: &mut RwTxn<'_>) -> heed::Result<bool> {
        self.main.remap_key_type::<Str>().delete(wtxn, main_key::DICTIONARY_KEY)
    }

    pub fn dictionary(&self, rtxn: &RoTxn<'_>) -> Result<Option<BTreeSet<String>>> {
        Ok(self
            .main
            .remap_types::<Str, SerdeBincode<BTreeSet<String>>>()
            .get(rtxn, main_key::DICTIONARY_KEY)?)
    }

    /* synonyms */

    pub(crate) fn put_synonyms(
        &self,
        wtxn: &mut RwTxn<'_>,
        synonyms: &HashMap<Vec<String>, Vec<Vec<String>>>,
        user_defined_synonyms: &BTreeMap<String, Vec<String>>,
    ) -> heed::Result<()> {
        self.main.remap_types::<Str, SerdeBincode<_>>().put(
            wtxn,
            main_key::SYNONYMS_KEY,
            synonyms,
        )?;
        self.main.remap_types::<Str, SerdeBincode<_>>().put(
            wtxn,
            main_key::USER_DEFINED_SYNONYMS_KEY,
            user_defined_synonyms,
        )
    }

    pub(crate) fn delete_synonyms(&self, wtxn: &mut RwTxn<'_>) -> heed::Result<bool> {
        self.main.remap_key_type::<Str>().delete(wtxn, main_key::SYNONYMS_KEY)?;
        self.main.remap_key_type::<Str>().delete(wtxn, main_key::USER_DEFINED_SYNONYMS_KEY)
    }

    pub fn user_defined_synonyms(
        &self,
        rtxn: &RoTxn<'_>,
    ) -> heed::Result<BTreeMap<String, Vec<String>>> {
        Ok(self
            .main
            .remap_types::<Str, SerdeBincode<_>>()
            .get(rtxn, main_key::USER_DEFINED_SYNONYMS_KEY)?
            .unwrap_or_default())
    }

    pub fn synonyms(
        &self,
        rtxn: &RoTxn<'_>,
    ) -> heed::Result<HashMap<Vec<String>, Vec<Vec<String>>>> {
        Ok(self
            .main
            .remap_types::<Str, SerdeBincode<_>>()
            .get(rtxn, main_key::SYNONYMS_KEY)?
            .unwrap_or_default())
    }

    pub fn words_synonyms<S: AsRef<str>>(
        &self,
        rtxn: &RoTxn<'_>,
        words: &[S],
    ) -> heed::Result<Option<Vec<Vec<String>>>> {
        let words: Vec<_> = words.iter().map(|s| s.as_ref().to_owned()).collect();
        Ok(self.synonyms(rtxn)?.remove(&words))
    }

    /* words prefixes fst */

    /// Writes the FST which is the words prefixes dictionary of the engine.
    pub(crate) fn put_words_prefixes_fst<A: AsRef<[u8]>>(
        &self,
        wtxn: &mut RwTxn<'_>,
        fst: &fst::Set<A>,
    ) -> heed::Result<()> {
        self.main.remap_types::<Str, Bytes>().put(
            wtxn,
            main_key::WORDS_PREFIXES_FST_KEY,
            fst.as_fst().as_bytes(),
        )
    }

    pub(crate) fn delete_words_prefixes_fst(&self, wtxn: &mut RwTxn<'_>) -> heed::Result<bool> {
        self.main.remap_key_type::<Str>().delete(wtxn, main_key::WORDS_PREFIXES_FST_KEY)
    }

    /// Returns the FST which is the words prefixes dictionary of the engine.
    pub fn words_prefixes_fst<'t>(&self, rtxn: &'t RoTxn<'t>) -> Result<fst::Set<Cow<'t, [u8]>>> {
        match self.main.remap_types::<Str, Bytes>().get(rtxn, main_key::WORDS_PREFIXES_FST_KEY)? {
            Some(bytes) => Ok(fst::Set::new(bytes)?.map_data(Cow::Borrowed)?),
            None => Ok(fst::Set::default().map_data(Cow::Owned)?),
        }
    }

    /* word documents count */

    /// Returns the number of documents ids associated with the given word,
    /// it is much faster than deserializing the bitmap and getting the length of it.
    pub fn word_documents_count(&self, rtxn: &RoTxn<'_>, word: &str) -> heed::Result<Option<u64>> {
        self.word_docids.remap_data_type::<RoaringBitmapLenCodec>().get(rtxn, word)
    }

    /* documents */

    /// Returns a document by using the document id.
    pub fn compressed_document<'t>(
        &self,
        rtxn: &'t RoTxn,
        id: DocumentId,
    ) -> Result<Option<CompressedKvReaderU16<'t>>> {
        self.documents.get(rtxn, &id).map_err(Into::into)
    }

    /// Returns an iterator over the requested compressed documents. The next item will be an error if a document is missing.
    pub fn iter_compressed_documents<'a, 't: 'a>(
        &'a self,
        rtxn: &'t RoTxn<'t>,
        ids: impl IntoIterator<Item = DocumentId> + 'a,
    ) -> Result<impl Iterator<Item = Result<(DocumentId, CompressedKvReaderU16<'t>)>> + 'a> {
        Ok(ids.into_iter().flat_map(move |id| {
            self.compressed_document(rtxn, id)
                .map(|opt| opt.map(|compressed| (id, compressed)))
                .transpose()
        }))
    }

    /// Returns a [`Vec`] of the requested documents. Returns an error if a document is missing.
    pub fn compressed_documents<'t>(
        &self,
        rtxn: &'t RoTxn<'t>,
        ids: impl IntoIterator<Item = DocumentId>,
    ) -> Result<Vec<(DocumentId, CompressedKvReaderU16<'t>)>> {
        self.iter_compressed_documents(rtxn, ids)?.collect()
    }

    /// Returns an iterator over all the documents in the index.
    pub fn all_compressed_documents<'a, 't: 'a>(
        &'a self,
        rtxn: &'t RoTxn<'t>,
    ) -> Result<impl Iterator<Item = Result<(DocumentId, CompressedKvReaderU16<'t>)>> + 'a> {
        self.iter_compressed_documents(rtxn, self.documents_ids(rtxn)?)
    }

    pub fn external_id_of<'a, 't: 'a>(
        &'a self,
        rtxn: &'t RoTxn<'t>,
        ids: impl IntoIterator<Item = DocumentId> + 'a,
    ) -> Result<impl IntoIterator<Item = Result<String>> + 'a> {
        let fields = self.fields_ids_map(rtxn)?;

        // uses precondition "never called on an empty index"
        let primary_key = self.primary_key(rtxn)?.ok_or(InternalError::DatabaseMissingEntry {
            db_name: db_name::MAIN,
            key: Some(main_key::PRIMARY_KEY_KEY),
        })?;
        let primary_key = PrimaryKey::new(primary_key, &fields).ok_or_else(|| {
            InternalError::FieldIdMapMissingEntry(crate::FieldIdMapMissingEntry::FieldName {
                field_name: primary_key.to_owned(),
                process: "external_id_of",
            })
        })?;
        let dictionary =
            self.document_compression_raw_dictionary(rtxn)?.map(DecoderDictionary::copy);
        let mut buffer = Vec::new();
        Ok(self.iter_compressed_documents(rtxn, ids)?.map(move |entry| -> Result<_> {
            let (_docid, compressed_obkv) = entry?;
            let obkv = compressed_obkv
                .decompress_with_optional_dictionary(&mut buffer, dictionary.as_ref())?;
            match primary_key.document_id(obkv, &fields)? {
                Ok(document_id) => Ok(document_id),
                Err(_) => Err(InternalError::DocumentsError(
                    crate::documents::Error::InvalidDocumentFormat,
                )
                .into()),
            }
        }))
    }

    pub fn facets_distribution<'a>(&'a self, rtxn: &'a RoTxn<'a>) -> FacetDistribution<'a> {
        FacetDistribution::new(rtxn, self)
    }

    pub fn search<'a>(&'a self, rtxn: &'a RoTxn<'a>) -> Search<'a> {
        Search::new(rtxn, self)
    }

    /// Returns the index creation time.
    pub fn created_at(&self, rtxn: &RoTxn<'_>) -> Result<time::OffsetDateTime> {
        Ok(self
            .main
            .remap_types::<Str, SerdeJson<OffsetDateTime>>()
            .get(rtxn, main_key::CREATED_AT_KEY)?
            .ok_or(InternalError::DatabaseMissingEntry {
                db_name: db_name::MAIN,
                key: Some(main_key::CREATED_AT_KEY),
            })?
            .0)
    }

    /// Returns the index last updated time.
    pub fn updated_at(&self, rtxn: &RoTxn<'_>) -> Result<time::OffsetDateTime> {
        Ok(self
            .main
            .remap_types::<Str, SerdeJson<OffsetDateTime>>()
            .get(rtxn, main_key::UPDATED_AT_KEY)?
            .ok_or(InternalError::DatabaseMissingEntry {
                db_name: db_name::MAIN,
                key: Some(main_key::UPDATED_AT_KEY),
            })?
            .0)
    }

    pub(crate) fn set_updated_at(
        &self,
        wtxn: &mut RwTxn<'_>,
        time: &time::OffsetDateTime,
    ) -> heed::Result<()> {
        self.main.remap_types::<Str, SerdeJson<OffsetDateTime>>().put(
            wtxn,
            main_key::UPDATED_AT_KEY,
            &OffsetDateTime(*time),
        )
    }

    pub fn authorize_typos(&self, txn: &RoTxn<'_>) -> heed::Result<bool> {
        // It is not possible to put a bool in heed with OwnedType, so we put a u8 instead. We
        // identify 0 as being false, and anything else as true. The absence of a value is true,
        // because by default, we authorize typos.
        match self.main.remap_types::<Str, U8>().get(txn, main_key::AUTHORIZE_TYPOS)? {
            Some(0) => Ok(false),
            _ => Ok(true),
        }
    }

    pub(crate) fn put_authorize_typos(&self, txn: &mut RwTxn<'_>, flag: bool) -> heed::Result<()> {
        // It is not possible to put a bool in heed with OwnedType, so we put a u8 instead. We
        // identify 0 as being false, and anything else as true. The absence of a value is true,
        // because by default, we authorize typos.
        self.main.remap_types::<Str, U8>().put(txn, main_key::AUTHORIZE_TYPOS, &(flag as u8))?;

        Ok(())
    }

    pub fn min_word_len_one_typo(&self, txn: &RoTxn<'_>) -> heed::Result<u8> {
        // It is not possible to put a bool in heed with OwnedType, so we put a u8 instead. We
        // identify 0 as being false, and anything else as true. The absence of a value is true,
        // because by default, we authorize typos.
        Ok(self
            .main
            .remap_types::<Str, U8>()
            .get(txn, main_key::ONE_TYPO_WORD_LEN)?
            .unwrap_or(DEFAULT_MIN_WORD_LEN_ONE_TYPO))
    }

    pub(crate) fn put_min_word_len_one_typo(
        &self,
        txn: &mut RwTxn<'_>,
        val: u8,
    ) -> heed::Result<()> {
        // It is not possible to put a bool in heed with OwnedType, so we put a u8 instead. We
        // identify 0 as being false, and anything else as true. The absence of a value is true,
        // because by default, we authorize typos.
        self.main.remap_types::<Str, U8>().put(txn, main_key::ONE_TYPO_WORD_LEN, &val)?;
        Ok(())
    }

    pub fn min_word_len_two_typos(&self, txn: &RoTxn<'_>) -> heed::Result<u8> {
        // It is not possible to put a bool in heed with OwnedType, so we put a u8 instead. We
        // identify 0 as being false, and anything else as true. The absence of a value is true,
        // because by default, we authorize typos.
        Ok(self
            .main
            .remap_types::<Str, U8>()
            .get(txn, main_key::TWO_TYPOS_WORD_LEN)?
            .unwrap_or(DEFAULT_MIN_WORD_LEN_TWO_TYPOS))
    }

    pub(crate) fn put_min_word_len_two_typos(
        &self,
        txn: &mut RwTxn<'_>,
        val: u8,
    ) -> heed::Result<()> {
        // It is not possible to put a bool in heed with OwnedType, so we put a u8 instead. We
        // identify 0 as being false, and anything else as true. The absence of a value is true,
        // because by default, we authorize typos.
        self.main.remap_types::<Str, U8>().put(txn, main_key::TWO_TYPOS_WORD_LEN, &val)?;
        Ok(())
    }

    /// List the words on which typo are not allowed
    pub fn exact_words<'t>(&self, txn: &'t RoTxn<'t>) -> Result<Option<fst::Set<Cow<'t, [u8]>>>> {
        match self.main.remap_types::<Str, Bytes>().get(txn, main_key::EXACT_WORDS)? {
            Some(bytes) => Ok(Some(fst::Set::new(bytes)?.map_data(Cow::Borrowed)?)),
            None => Ok(None),
        }
    }

    pub(crate) fn put_exact_words<A: AsRef<[u8]>>(
        &self,
        txn: &mut RwTxn<'_>,
        words: &fst::Set<A>,
    ) -> Result<()> {
        self.main.remap_types::<Str, Bytes>().put(
            txn,
            main_key::EXACT_WORDS,
            words.as_fst().as_bytes(),
        )?;
        Ok(())
    }

    /// Returns the exact attributes: attributes for which typo is disallowed.
    pub fn exact_attributes<'t>(&self, txn: &'t RoTxn<'t>) -> Result<Vec<&'t str>> {
        Ok(self
            .main
            .remap_types::<Str, SerdeBincode<Vec<&str>>>()
            .get(txn, main_key::EXACT_ATTRIBUTES)?
            .unwrap_or_default())
    }

    /// Returns the list of exact attributes field ids.
    pub fn exact_attributes_ids(&self, txn: &RoTxn<'_>) -> Result<HashSet<FieldId>> {
        let attrs = self.exact_attributes(txn)?;
        let fid_map = self.fields_ids_map(txn)?;
        Ok(attrs.iter().filter_map(|attr| fid_map.id(attr)).collect())
    }

    /// Writes the exact attributes to the database.
    pub(crate) fn put_exact_attributes(&self, txn: &mut RwTxn<'_>, attrs: &[&str]) -> Result<()> {
        self.main.remap_types::<Str, SerdeBincode<&[&str]>>().put(
            txn,
            main_key::EXACT_ATTRIBUTES,
            &attrs,
        )?;
        Ok(())
    }

    /// Clears the exact attributes from the store.
    pub(crate) fn delete_exact_attributes(&self, txn: &mut RwTxn<'_>) -> heed::Result<bool> {
        self.main.remap_key_type::<Str>().delete(txn, main_key::EXACT_ATTRIBUTES)
    }

    pub fn max_values_per_facet(&self, txn: &RoTxn<'_>) -> heed::Result<Option<u64>> {
        self.main.remap_types::<Str, BEU64>().get(txn, main_key::MAX_VALUES_PER_FACET)
    }

    pub(crate) fn put_max_values_per_facet(
        &self,
        txn: &mut RwTxn<'_>,
        val: u64,
    ) -> heed::Result<()> {
        self.main.remap_types::<Str, BEU64>().put(txn, main_key::MAX_VALUES_PER_FACET, &val)
    }

    pub(crate) fn delete_max_values_per_facet(&self, txn: &mut RwTxn<'_>) -> heed::Result<bool> {
        self.main.remap_key_type::<Str>().delete(txn, main_key::MAX_VALUES_PER_FACET)
    }

    pub fn sort_facet_values_by(&self, txn: &RoTxn<'_>) -> heed::Result<OrderByMap> {
        let orders = self
            .main
            .remap_types::<Str, SerdeJson<OrderByMap>>()
            .get(txn, main_key::SORT_FACET_VALUES_BY)?
            .unwrap_or_default();
        Ok(orders)
    }

    pub(crate) fn put_sort_facet_values_by(
        &self,
        txn: &mut RwTxn<'_>,
        val: &OrderByMap,
    ) -> heed::Result<()> {
        self.main.remap_types::<Str, SerdeJson<_>>().put(txn, main_key::SORT_FACET_VALUES_BY, &val)
    }

    pub(crate) fn delete_sort_facet_values_by(&self, txn: &mut RwTxn<'_>) -> heed::Result<bool> {
        self.main.remap_key_type::<Str>().delete(txn, main_key::SORT_FACET_VALUES_BY)
    }

    pub fn pagination_max_total_hits(&self, txn: &RoTxn<'_>) -> heed::Result<Option<u64>> {
        self.main.remap_types::<Str, BEU64>().get(txn, main_key::PAGINATION_MAX_TOTAL_HITS)
    }

    pub(crate) fn put_pagination_max_total_hits(
        &self,
        txn: &mut RwTxn<'_>,
        val: u64,
    ) -> heed::Result<()> {
        self.main.remap_types::<Str, BEU64>().put(txn, main_key::PAGINATION_MAX_TOTAL_HITS, &val)
    }

    pub(crate) fn delete_pagination_max_total_hits(
        &self,
        txn: &mut RwTxn<'_>,
    ) -> heed::Result<bool> {
        self.main.remap_key_type::<Str>().delete(txn, main_key::PAGINATION_MAX_TOTAL_HITS)
    }

    pub fn proximity_precision(&self, txn: &RoTxn<'_>) -> heed::Result<Option<ProximityPrecision>> {
        self.main
            .remap_types::<Str, SerdeBincode<ProximityPrecision>>()
            .get(txn, main_key::PROXIMITY_PRECISION)
    }

    pub(crate) fn put_proximity_precision(
        &self,
        txn: &mut RwTxn<'_>,
        val: ProximityPrecision,
    ) -> heed::Result<()> {
        self.main.remap_types::<Str, SerdeBincode<ProximityPrecision>>().put(
            txn,
            main_key::PROXIMITY_PRECISION,
            &val,
        )
    }

    pub(crate) fn delete_proximity_precision(&self, txn: &mut RwTxn<'_>) -> heed::Result<bool> {
        self.main.remap_key_type::<Str>().delete(txn, main_key::PROXIMITY_PRECISION)
    }

    pub fn prefix_search(&self, txn: &RoTxn<'_>) -> heed::Result<Option<PrefixSearch>> {
        self.main.remap_types::<Str, SerdeBincode<PrefixSearch>>().get(txn, main_key::PREFIX_SEARCH)
    }

    pub(crate) fn put_prefix_search(
        &self,
        txn: &mut RwTxn<'_>,
        val: PrefixSearch,
    ) -> heed::Result<()> {
        self.main.remap_types::<Str, SerdeBincode<PrefixSearch>>().put(
            txn,
            main_key::PREFIX_SEARCH,
            &val,
        )
    }

    pub(crate) fn delete_prefix_search(&self, txn: &mut RwTxn<'_>) -> heed::Result<bool> {
        self.main.remap_key_type::<Str>().delete(txn, main_key::PREFIX_SEARCH)
    }

    pub fn facet_search(&self, txn: &RoTxn<'_>) -> heed::Result<bool> {
        self.main
            .remap_types::<Str, SerdeBincode<bool>>()
            .get(txn, main_key::FACET_SEARCH)
            .map(|v| v.unwrap_or(true))
    }

    pub(crate) fn put_facet_search(&self, txn: &mut RwTxn<'_>, val: bool) -> heed::Result<()> {
        self.main.remap_types::<Str, SerdeBincode<bool>>().put(txn, main_key::FACET_SEARCH, &val)
    }

    pub(crate) fn delete_facet_search(&self, txn: &mut RwTxn<'_>) -> heed::Result<bool> {
        self.main.remap_key_type::<Str>().delete(txn, main_key::FACET_SEARCH)
    }

    pub fn localized_attributes_rules(
        &self,
        rtxn: &RoTxn<'_>,
    ) -> heed::Result<Option<Vec<LocalizedAttributesRule>>> {
        self.main
            .remap_types::<Str, SerdeJson<Vec<LocalizedAttributesRule>>>()
            .get(rtxn, main_key::LOCALIZED_ATTRIBUTES_RULES)
    }

    pub(crate) fn put_localized_attributes_rules(
        &self,
        txn: &mut RwTxn<'_>,
        val: Vec<LocalizedAttributesRule>,
    ) -> heed::Result<()> {
        self.main.remap_types::<Str, SerdeJson<Vec<LocalizedAttributesRule>>>().put(
            txn,
            main_key::LOCALIZED_ATTRIBUTES_RULES,
            &val,
        )
    }

    pub(crate) fn delete_localized_attributes_rules(
        &self,
        txn: &mut RwTxn<'_>,
    ) -> heed::Result<bool> {
        self.main.remap_key_type::<Str>().delete(txn, main_key::LOCALIZED_ATTRIBUTES_RULES)
    }

    /// Put the embedding configs:
    /// 1. The name of the embedder
    /// 2. The configuration option for this embedder
    /// 3. The list of documents with a user provided embedding
    pub(crate) fn put_embedding_configs(
        &self,
        wtxn: &mut RwTxn<'_>,
        configs: Vec<IndexEmbeddingConfig>,
    ) -> heed::Result<()> {
        self.main.remap_types::<Str, SerdeJson<Vec<IndexEmbeddingConfig>>>().put(
            wtxn,
            main_key::EMBEDDING_CONFIGS,
            &configs,
        )
    }

    pub(crate) fn delete_embedding_configs(&self, wtxn: &mut RwTxn<'_>) -> heed::Result<bool> {
        self.main.remap_key_type::<Str>().delete(wtxn, main_key::EMBEDDING_CONFIGS)
    }

    pub fn embedding_configs(&self, rtxn: &RoTxn<'_>) -> Result<Vec<IndexEmbeddingConfig>> {
        Ok(self
            .main
            .remap_types::<Str, SerdeJson<Vec<IndexEmbeddingConfig>>>()
            .get(rtxn, main_key::EMBEDDING_CONFIGS)?
            .unwrap_or_default())
    }

    pub(crate) fn put_search_cutoff(&self, wtxn: &mut RwTxn<'_>, cutoff: u64) -> heed::Result<()> {
        self.main.remap_types::<Str, BEU64>().put(wtxn, main_key::SEARCH_CUTOFF, &cutoff)
    }

    pub fn search_cutoff(&self, rtxn: &RoTxn<'_>) -> Result<Option<u64>> {
        Ok(self.main.remap_types::<Str, BEU64>().get(rtxn, main_key::SEARCH_CUTOFF)?)
    }

    pub(crate) fn delete_search_cutoff(&self, wtxn: &mut RwTxn<'_>) -> heed::Result<bool> {
        self.main.remap_key_type::<Str>().delete(wtxn, main_key::SEARCH_CUTOFF)
    }

    pub fn embeddings(
        &self,
        rtxn: &RoTxn<'_>,
        docid: DocumentId,
    ) -> Result<BTreeMap<String, Vec<Embedding>>> {
        let mut res = BTreeMap::new();
        let embedding_configs = self.embedding_configs(rtxn)?;
        for config in embedding_configs {
            let embedder_id = self.embedder_category_id.get(rtxn, &config.name)?.unwrap();
            let reader =
                ArroyWrapper::new(self.vector_arroy, embedder_id, config.config.quantized());
            let embeddings = reader.item_vectors(rtxn, docid)?;
            res.insert(config.name.to_owned(), embeddings);
        }
        Ok(res)
    }

    pub fn prefix_settings(&self, rtxn: &RoTxn<'_>) -> Result<PrefixSettings> {
        let compute_prefixes = self.prefix_search(rtxn)?.unwrap_or_default();
        Ok(PrefixSettings { compute_prefixes, max_prefix_length: 4, prefix_count_threshold: 100 })
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct IndexEmbeddingConfig {
    pub name: String,
    pub config: EmbeddingConfig,
    pub user_provided: RoaringBitmap,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct PrefixSettings {
    pub prefix_count_threshold: usize,
    pub max_prefix_length: usize,
    pub compute_prefixes: PrefixSearch,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase")]
pub enum PrefixSearch {
    #[default]
    IndexingTime,
    Disabled,
}

#[derive(Serialize, Deserialize)]
#[serde(transparent)]
struct OffsetDateTime(#[serde(with = "time::serde::rfc3339")] time::OffsetDateTime);

#[cfg(test)]
pub(crate) mod tests {
    use std::collections::HashSet;
    use std::ops::Deref;

    use big_s::S;
    use bumpalo::Bump;
    use heed::{EnvOpenOptions, RwTxn};
    use maplit::{btreemap, hashset};
    use memmap2::Mmap;
    use tempfile::TempDir;

    use crate::error::{Error, InternalError};
    use crate::index::{DEFAULT_MIN_WORD_LEN_ONE_TYPO, DEFAULT_MIN_WORD_LEN_TWO_TYPOS};
    use crate::progress::Progress;
    use crate::update::new::indexer;
    use crate::update::settings::InnerIndexSettings;
    use crate::update::{
        self, IndexDocumentsConfig, IndexDocumentsMethod, IndexerConfig, Setting, Settings,
    };
    use crate::vector::settings::{EmbedderSource, EmbeddingSettings};
    use crate::vector::EmbeddingConfigs;
    use crate::{
        db_snap, obkv_to_json, Filter, Index, Search, SearchResult, ThreadPoolNoAbortBuilder,
    };

    pub(crate) struct TempIndex {
        pub inner: Index,
        pub indexer_config: IndexerConfig,
        pub index_documents_config: IndexDocumentsConfig,
        _tempdir: TempDir,
    }

    impl Deref for TempIndex {
        type Target = Index;

        fn deref(&self) -> &Self::Target {
            &self.inner
        }
    }

    impl TempIndex {
        /// Creates a temporary index
        pub fn new_with_map_size(size: usize) -> Self {
            let mut options = EnvOpenOptions::new();
            options.map_size(size);
            let _tempdir = TempDir::new_in(".").unwrap();
            let inner = Index::new(options, _tempdir.path()).unwrap();
            let indexer_config = IndexerConfig::default();
            let index_documents_config = IndexDocumentsConfig::default();
            Self { inner, indexer_config, index_documents_config, _tempdir }
        }
        /// Creates a temporary index, with a default `4096 * 2000` size. This should be enough for
        /// most tests.
        pub fn new() -> Self {
            Self::new_with_map_size(4096 * 2000)
        }

        pub fn add_documents_using_wtxn<'t>(
            &'t self,
            wtxn: &mut RwTxn<'t>,
            documents: Mmap,
        ) -> Result<(), crate::error::Error> {
            let local_pool;
            let indexer_config = &self.indexer_config;
            let pool = match &indexer_config.thread_pool {
                Some(pool) => pool,
                None => {
                    local_pool = ThreadPoolNoAbortBuilder::new().build().unwrap();
                    &local_pool
                }
            };

            let rtxn = self.inner.read_txn()?;
            let db_fields_ids_map = self.inner.fields_ids_map(&rtxn)?;
            let mut new_fields_ids_map = db_fields_ids_map.clone();

            let embedders =
                InnerIndexSettings::from_index(&self.inner, &rtxn, None)?.embedding_configs;
            let mut indexer =
                indexer::DocumentOperation::new(self.index_documents_config.update_method);
            indexer.add_documents(&documents).unwrap();

            let indexer_alloc = Bump::new();
            let (document_changes, operation_stats, primary_key) = indexer.into_changes(
                &indexer_alloc,
                &self.inner,
                &rtxn,
                None,
                &mut new_fields_ids_map,
                &|| false,
                Progress::default(),
            )?;

            if let Some(error) = operation_stats.into_iter().find_map(|stat| stat.error) {
                return Err(error.into());
            }

            pool.install(|| {
                indexer::index(
                    wtxn,
                    &self.inner,
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
            })
            .unwrap()?;

            Ok(())
        }

        pub fn add_documents(&self, documents: Mmap) -> Result<(), crate::error::Error> {
            let mut wtxn = self.write_txn().unwrap();
            self.add_documents_using_wtxn(&mut wtxn, documents)?;
            wtxn.commit().unwrap();
            Ok(())
        }

        pub fn update_settings(
            &self,
            update: impl Fn(&mut Settings<'_, '_, '_>),
        ) -> Result<(), crate::error::Error> {
            let mut wtxn = self.write_txn().unwrap();
            self.update_settings_using_wtxn(&mut wtxn, update)?;
            wtxn.commit().unwrap();
            Ok(())
        }

        pub fn update_settings_using_wtxn<'t>(
            &'t self,
            wtxn: &mut RwTxn<'t>,
            update: impl Fn(&mut Settings<'_, '_, '_>),
        ) -> Result<(), crate::error::Error> {
            let mut builder = update::Settings::new(wtxn, &self.inner, &self.indexer_config);
            update(&mut builder);
            builder.execute(drop, || false)?;
            Ok(())
        }

        pub fn delete_documents_using_wtxn<'t>(
            &'t self,
            wtxn: &mut RwTxn<'t>,
            external_document_ids: Vec<String>,
        ) -> Result<(), crate::error::Error> {
            let local_pool;
            let indexer_config = &self.indexer_config;
            let pool = match &indexer_config.thread_pool {
                Some(pool) => pool,
                None => {
                    local_pool = ThreadPoolNoAbortBuilder::new().build().unwrap();
                    &local_pool
                }
            };

            let rtxn = self.inner.read_txn()?;
            let db_fields_ids_map = self.inner.fields_ids_map(&rtxn)?;
            let mut new_fields_ids_map = db_fields_ids_map.clone();

            let embedders =
                InnerIndexSettings::from_index(&self.inner, &rtxn, None)?.embedding_configs;

            let mut indexer =
                indexer::DocumentOperation::new(self.index_documents_config.update_method);
            let external_document_ids: Vec<_> =
                external_document_ids.iter().map(AsRef::as_ref).collect();
            indexer.delete_documents(external_document_ids.as_slice());

            let indexer_alloc = Bump::new();
            let (document_changes, operation_stats, primary_key) = indexer.into_changes(
                &indexer_alloc,
                &self.inner,
                &rtxn,
                None,
                &mut new_fields_ids_map,
                &|| false,
                Progress::default(),
            )?;

            if let Some(error) = operation_stats.into_iter().find_map(|stat| stat.error) {
                return Err(error.into());
            }

            pool.install(|| {
                indexer::index(
                    wtxn,
                    &self.inner,
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
            })
            .unwrap()?;

            Ok(())
        }

        pub fn delete_documents(&self, external_document_ids: Vec<String>) {
            let mut wtxn = self.write_txn().unwrap();

            self.delete_documents_using_wtxn(&mut wtxn, external_document_ids).unwrap();

            wtxn.commit().unwrap();
        }

        pub fn delete_document(&self, external_document_id: &str) {
            self.delete_documents(vec![external_document_id.to_string()])
        }
    }

    #[test]
    fn aborting_indexation() {
        use std::sync::atomic::AtomicBool;
        use std::sync::atomic::Ordering::Relaxed;

        let index = TempIndex::new();
        let mut wtxn = index.inner.write_txn().unwrap();
        let should_abort = AtomicBool::new(false);

        let local_pool;
        let indexer_config = &index.indexer_config;
        let pool = match &indexer_config.thread_pool {
            Some(pool) => pool,
            None => {
                local_pool = ThreadPoolNoAbortBuilder::new().build().unwrap();
                &local_pool
            }
        };

        let rtxn = index.inner.read_txn().unwrap();
        let db_fields_ids_map = index.inner.fields_ids_map(&rtxn).unwrap();
        let mut new_fields_ids_map = db_fields_ids_map.clone();

        let embedders = EmbeddingConfigs::default();
        let mut indexer = indexer::DocumentOperation::new(IndexDocumentsMethod::ReplaceDocuments);
        let payload = documents!([
            { "id": 1, "name": "kevin" },
            { "id": 2, "name": "bob", "age": 20 },
            { "id": 2, "name": "bob", "age": 20 },
        ]);
        indexer.add_documents(&payload).unwrap();

        let indexer_alloc = Bump::new();
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

        should_abort.store(true, Relaxed);

        let err = pool
            .install(|| {
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
                    &|| should_abort.load(Relaxed),
                    &Progress::default(),
                )
            })
            .unwrap()
            .unwrap_err();

        assert!(matches!(err, Error::InternalError(InternalError::AbortedIndexation)));
    }

    #[test]
    fn initial_field_distribution() {
        let index = TempIndex::new();
        index
            .add_documents(documents!([
                { "id": 1, "name": "kevin" },
                { "id": 2, "name": "bob", "age": 20 },
                { "id": 2, "name": "bob", "age": 20 },
            ]))
            .unwrap();

        db_snap!(index, field_distribution, @r###"
        age              1      |
        id               2      |
        name             2      |
        "###);

        db_snap!(index, word_docids,
        @r###"
        1                [0, ]
        2                [1, ]
        20               [1, ]
        bob              [1, ]
        kevin            [0, ]
        "###
        );

        // we add all the documents a second time. we are supposed to get the same
        // field_distribution in the end
        index
            .add_documents(documents!([
                { "id": 1, "name": "kevin" },
                { "id": 2, "name": "bob", "age": 20 },
                { "id": 2, "name": "bob", "age": 20 },
            ]))
            .unwrap();

        db_snap!(index, field_distribution,
            @r###"
        age              1      |
        id               2      |
        name             2      |
        "###
        );

        // then we update a document by removing one field and another by adding one field
        index
            .add_documents(documents!([
                { "id": 1, "name": "kevin", "has_dog": true },
                { "id": 2, "name": "bob" }
            ]))
            .unwrap();

        db_snap!(index, field_distribution,
            @r###"
        has_dog          1      |
        id               2      |
        name             2      |
        "###
        );
    }

    #[test]
    fn put_and_retrieve_disable_typo() {
        let index = TempIndex::new();
        let mut txn = index.write_txn().unwrap();
        // default value is true
        assert!(index.authorize_typos(&txn).unwrap());
        // set to false
        index.put_authorize_typos(&mut txn, false).unwrap();
        txn.commit().unwrap();

        let txn = index.read_txn().unwrap();
        assert!(!index.authorize_typos(&txn).unwrap());
    }

    #[test]
    fn set_min_word_len_for_typos() {
        let index = TempIndex::new();
        let mut txn = index.write_txn().unwrap();

        assert_eq!(index.min_word_len_one_typo(&txn).unwrap(), DEFAULT_MIN_WORD_LEN_ONE_TYPO);
        assert_eq!(index.min_word_len_two_typos(&txn).unwrap(), DEFAULT_MIN_WORD_LEN_TWO_TYPOS);

        index.put_min_word_len_one_typo(&mut txn, 3).unwrap();
        index.put_min_word_len_two_typos(&mut txn, 15).unwrap();

        txn.commit().unwrap();

        let txn = index.read_txn().unwrap();
        assert_eq!(index.min_word_len_one_typo(&txn).unwrap(), 3);
        assert_eq!(index.min_word_len_two_typos(&txn).unwrap(), 15);
    }

    #[test]
    fn add_documents_and_set_searchable_fields() {
        let index = TempIndex::new();
        index
            .add_documents(documents!([
                { "id": 1, "doggo": "kevin" },
                { "id": 2, "doggo": { "name": "bob", "age": 20 } },
                { "id": 3, "name": "jean", "age": 25 },
            ]))
            .unwrap();
        index
            .update_settings(|settings| {
                settings.set_searchable_fields(vec![S("doggo"), S("name")]);
            })
            .unwrap();

        // ensure we get the right real searchable fields + user defined searchable fields
        let rtxn = index.read_txn().unwrap();

        let real = index.searchable_fields(&rtxn).unwrap();
        assert_eq!(real, &["doggo", "name", "doggo.name", "doggo.age"]);

        let user_defined = index.user_defined_searchable_fields(&rtxn).unwrap().unwrap();
        assert_eq!(user_defined, &["doggo", "name"]);
    }

    #[test]
    fn set_searchable_fields_and_add_documents() {
        let index = TempIndex::new();

        index
            .update_settings(|settings| {
                settings.set_searchable_fields(vec![S("doggo"), S("name")]);
            })
            .unwrap();

        // ensure we get the right real searchable fields + user defined searchable fields
        let rtxn = index.read_txn().unwrap();

        let real = index.searchable_fields(&rtxn).unwrap();
        assert_eq!(real, &["doggo", "name"]);
        let user_defined = index.user_defined_searchable_fields(&rtxn).unwrap().unwrap();
        assert_eq!(user_defined, &["doggo", "name"]);

        index
            .add_documents(documents!([
                { "id": 1, "doggo": "kevin" },
                { "id": 2, "doggo": { "name": "bob", "age": 20 } },
                { "id": 3, "name": "jean", "age": 25 },
            ]))
            .unwrap();

        // ensure we get the right real searchable fields + user defined searchable fields
        let rtxn = index.read_txn().unwrap();

        let real = index.searchable_fields(&rtxn).unwrap();
        assert_eq!(real, &["doggo", "name", "doggo.name", "doggo.age"]);

        let user_defined = index.user_defined_searchable_fields(&rtxn).unwrap().unwrap();
        assert_eq!(user_defined, &["doggo", "name"]);
    }

    #[test]
    fn test_basic_geo_bounding_box() {
        let index = TempIndex::new();

        index
            .update_settings(|settings| {
                settings.set_filterable_fields(hashset! { S("_geo") });
            })
            .unwrap();
        index
            .add_documents(documents!([
                { "id": 0, "_geo": { "lat": "0", "lng": "0" } },
                { "id": 1, "_geo": { "lat": 0, "lng": "-175" } },
                { "id": 2, "_geo": { "lat": "0", "lng": 175 } },
                { "id": 3, "_geo": { "lat": 85, "lng": 0 } },
                { "id": 4, "_geo": { "lat": "-85", "lng": "0" } },
            ]))
            .unwrap();

        // ensure we get the right real searchable fields + user defined searchable fields
        let rtxn = index.read_txn().unwrap();
        let mut search = index.search(&rtxn);

        // exact match a document
        let search_result = search
            .filter(Filter::from_str("_geoBoundingBox([0, 0], [0, 0])").unwrap().unwrap())
            .execute()
            .unwrap();
        insta::assert_debug_snapshot!(search_result.candidates, @"RoaringBitmap<[0]>");

        // match a document in the middle of the rectangle
        let search_result = search
            .filter(Filter::from_str("_geoBoundingBox([10, 10], [-10, -10])").unwrap().unwrap())
            .execute()
            .unwrap();
        insta::assert_debug_snapshot!(search_result.candidates, @"RoaringBitmap<[0]>");

        // select everything
        let search_result = search
            .filter(Filter::from_str("_geoBoundingBox([90, 180], [-90, -180])").unwrap().unwrap())
            .execute()
            .unwrap();
        insta::assert_debug_snapshot!(search_result.candidates, @"RoaringBitmap<[0, 1, 2, 3, 4]>");

        // go on the edge of the longitude
        let search_result = search
            .filter(Filter::from_str("_geoBoundingBox([0, -170], [0, 180])").unwrap().unwrap())
            .execute()
            .unwrap();
        insta::assert_debug_snapshot!(search_result.candidates, @"RoaringBitmap<[1]>");

        // go on the other edge of the longitude
        let search_result = search
            .filter(Filter::from_str("_geoBoundingBox([0, -180], [0, 170])").unwrap().unwrap())
            .execute()
            .unwrap();
        insta::assert_debug_snapshot!(search_result.candidates, @"RoaringBitmap<[2]>");

        // wrap around the longitude
        let search_result = search
            .filter(Filter::from_str("_geoBoundingBox([0, -170], [0, 170])").unwrap().unwrap())
            .execute()
            .unwrap();
        insta::assert_debug_snapshot!(search_result.candidates, @"RoaringBitmap<[1, 2]>");

        // go on the edge of the latitude
        let search_result = search
            .filter(Filter::from_str("_geoBoundingBox([90, 0], [80, 0])").unwrap().unwrap())
            .execute()
            .unwrap();
        insta::assert_debug_snapshot!(search_result.candidates, @"RoaringBitmap<[3]>");

        // go on the edge of the latitude
        let search_result = search
            .filter(Filter::from_str("_geoBoundingBox([-80, 0], [-90, 0])").unwrap().unwrap())
            .execute()
            .unwrap();
        insta::assert_debug_snapshot!(search_result.candidates, @"RoaringBitmap<[4]>");

        // the requests that don't make sense

        // try to wrap around the latitude
        let error = search
            .filter(Filter::from_str("_geoBoundingBox([-80, 0], [80, 0])").unwrap().unwrap())
            .execute()
            .unwrap_err();
        insta::assert_snapshot!(
            error,
            @r###"
        The top latitude `-80` is below the bottom latitude `80`.
        32:33 _geoBoundingBox([-80, 0], [80, 0])
        "###
        );

        // send a top latitude lower than the bottow latitude
        let error = search
            .filter(Filter::from_str("_geoBoundingBox([-10, 0], [10, 0])").unwrap().unwrap())
            .execute()
            .unwrap_err();
        insta::assert_snapshot!(
            error,
            @r###"
        The top latitude `-10` is below the bottom latitude `10`.
        32:33 _geoBoundingBox([-10, 0], [10, 0])
        "###
        );
    }

    #[test]
    fn test_contains() {
        let index = TempIndex::new();

        index
            .update_settings(|settings| {
                settings.set_filterable_fields(hashset! { S("doggo") });
            })
            .unwrap();
        index
            .add_documents(documents!([
                { "id": 0, "doggo": "kefir" },
                { "id": 1, "doggo": "kefirounet" },
                { "id": 2, "doggo": "kefkef" },
                { "id": 3, "doggo": "fifir" },
                { "id": 4, "doggo": "boubou" },
                { "id": 5 },
            ]))
            .unwrap();

        let rtxn = index.read_txn().unwrap();
        let mut search = index.search(&rtxn);
        let search_result = search
            .filter(Filter::from_str("doggo CONTAINS kefir").unwrap().unwrap())
            .execute()
            .unwrap();
        insta::assert_debug_snapshot!(search_result.candidates, @"RoaringBitmap<[0, 1]>");
        let mut search = index.search(&rtxn);
        let search_result = search
            .filter(Filter::from_str("doggo CONTAINS KEF").unwrap().unwrap())
            .execute()
            .unwrap();
        insta::assert_debug_snapshot!(search_result.candidates, @"RoaringBitmap<[0, 1, 2]>");
        let mut search = index.search(&rtxn);
        let search_result = search
            .filter(Filter::from_str("doggo NOT CONTAINS fir").unwrap().unwrap())
            .execute()
            .unwrap();
        insta::assert_debug_snapshot!(search_result.candidates, @"RoaringBitmap<[2, 4, 5]>");
    }

    #[test]
    fn replace_documents_external_ids_and_soft_deletion_check() {
        use big_s::S;
        use maplit::hashset;

        let index = TempIndex::new();

        index
            .update_settings(|settings| {
                settings.set_primary_key("id".to_owned());
                settings.set_filterable_fields(hashset! { S("doggo") });
            })
            .unwrap();

        let mut docs = vec![];
        for i in 0..4 {
            docs.push(serde_json::json!(
                { "id": i, "doggo": i }
            ));
        }
        index.add_documents(documents!(docs)).unwrap();

        db_snap!(index, documents_ids, @"[0, 1, 2, 3, ]");
        db_snap!(index, external_documents_ids, 1, @r###"
        docids:
        0                        0
        1                        1
        2                        2
        3                        3
        "###);
        db_snap!(index, facet_id_f64_docids, 1, @r###"
        1   0  0      1  [0, ]
        1   0  1      1  [1, ]
        1   0  2      1  [2, ]
        1   0  3      1  [3, ]
        "###);

        let mut docs = vec![];
        for i in 0..3 {
            docs.push(serde_json::json!(
                { "id": i, "doggo": i + 1 }
            ));
        }
        index.add_documents(documents!(docs)).unwrap();

        db_snap!(index, documents_ids, @"[0, 1, 2, 3, ]");
        db_snap!(index, external_documents_ids, 2, @r###"
        docids:
        0                        0
        1                        1
        2                        2
        3                        3
        "###);
        db_snap!(index, facet_id_f64_docids, 2, @r###"
        1   0  1      1  [0, ]
        1   0  2      1  [1, ]
        1   0  3      1  [2, 3, ]
        "###);

        index
            .add_documents(documents!([{ "id": 3, "doggo": 4 }, { "id": 3, "doggo": 5 },{ "id": 3, "doggo": 4 }]))
            .unwrap();

        db_snap!(index, documents_ids, @"[0, 1, 2, 3, ]");
        db_snap!(index, external_documents_ids, 3, @r###"
        docids:
        0                        0
        1                        1
        2                        2
        3                        3
        "###);
        db_snap!(index, facet_id_f64_docids, 3, @r###"
        1   0  1      1  [0, ]
        1   0  2      1  [1, ]
        1   0  3      1  [2, ]
        1   0  4      1  [3, ]
        "###);

        index
            .update_settings(|settings| {
                settings.set_distinct_field("id".to_owned());
            })
            .unwrap();

        db_snap!(index, documents_ids, @"[0, 1, 2, 3, ]");
        db_snap!(index, external_documents_ids, 3, @r###"
        docids:
        0                        0
        1                        1
        2                        2
        3                        3
        "###);
        db_snap!(index, facet_id_f64_docids, 3, @r###"
        0   0  0      1  [0, ]
        0   0  1      1  [1, ]
        0   0  2      1  [2, ]
        0   0  3      1  [3, ]
        1   0  1      1  [0, ]
        1   0  2      1  [1, ]
        1   0  3      1  [2, ]
        1   0  4      1  [3, ]
        "###);
    }

    #[test]
    fn bug_3021_first() {
        // https://github.com/meilisearch/meilisearch/issues/3021
        let mut index = TempIndex::new();
        index.index_documents_config.update_method = IndexDocumentsMethod::ReplaceDocuments;

        index
            .update_settings(|settings| {
                settings.set_primary_key("primary_key".to_owned());
            })
            .unwrap();

        index
            .add_documents(documents!([
                { "primary_key": 38 },
                { "primary_key": 34 }
            ]))
            .unwrap();

        db_snap!(index, documents_ids, @"[0, 1, ]");
        db_snap!(index, external_documents_ids, 1, @r###"
        docids:
        34                       1
        38                       0
        "###);

        index.delete_document("34");

        db_snap!(index, documents_ids, @"[0, ]");
        db_snap!(index, external_documents_ids, 2, @r###"
        docids:
        38                       0
        "###);

        index
            .update_settings(|s| {
                s.set_searchable_fields(vec![]);
            })
            .unwrap();

        // The key point of the test is to verify that the external documents ids
        // do not contain any entry for previously soft-deleted document ids
        db_snap!(index, documents_ids, @"[0, ]");
        db_snap!(index, external_documents_ids, 3, @r###"
        docids:
        38                       0
        "###);

        // So that this document addition works correctly now.
        // It would be wrongly interpreted as a replacement before
        index.add_documents(documents!({ "primary_key": 34 })).unwrap();

        db_snap!(index, documents_ids, @"[0, 1, ]");
        db_snap!(index, external_documents_ids, 4, @r###"
        docids:
        34                       1
        38                       0
        "###);

        // We do the test again, but deleting the document with id 0 instead of id 1 now
        index.delete_document("38");

        db_snap!(index, documents_ids, @"[1, ]");
        db_snap!(index, external_documents_ids, 5, @r###"
        docids:
        34                       1
        "###);

        index
            .update_settings(|s| {
                s.set_searchable_fields(vec!["primary_key".to_owned()]);
            })
            .unwrap();

        db_snap!(index, documents_ids, @"[1, ]");
        db_snap!(index, external_documents_ids, 6, @r###"
        docids:
        34                       1
        "###);

        // And adding lots of documents afterwards instead of just one.
        // These extra subtests don't add much, but it's better than nothing.
        index
            .add_documents(documents!([
                { "primary_key": 38 },
                { "primary_key": 39 },
                { "primary_key": 41 },
                { "primary_key": 40 },
                { "primary_key": 41 },
                { "primary_key": 42 },
            ]))
            .unwrap();

        db_snap!(index, documents_ids, @"[0, 1, 2, 3, 4, 5, ]");
        db_snap!(index, external_documents_ids, 7, @r###"
        docids:
        34                       1
        38                       0
        39                       2
        40                       4
        41                       3
        42                       5
        "###);
    }

    #[test]
    fn simple_delete() {
        let mut index = TempIndex::new();
        index.index_documents_config.update_method = IndexDocumentsMethod::UpdateDocuments;
        index
            .add_documents(documents!([
                { "id": 30 },
                { "id": 34 }
            ]))
            .unwrap();

        db_snap!(index, documents_ids, @"[0, 1, ]");
        db_snap!(index, external_documents_ids, 1, @r###"
        docids:
        30                       0
        34                       1"###);

        index.delete_document("34");

        db_snap!(index, documents_ids, @"[0, ]");
        db_snap!(index, external_documents_ids, 2, @r###"
        docids:
        30                       0
        "###);
    }

    #[test]
    fn bug_3021_second() {
        // https://github.com/meilisearch/meilisearch/issues/3021
        let mut index = TempIndex::new();
        index.index_documents_config.update_method = IndexDocumentsMethod::UpdateDocuments;

        index
            .update_settings(|settings| {
                settings.set_primary_key("primary_key".to_owned());
            })
            .unwrap();

        index
            .add_documents(documents!([
                { "primary_key": 30 },
                { "primary_key": 34 }
            ]))
            .unwrap();

        db_snap!(index, documents_ids, @"[0, 1, ]");
        db_snap!(index, external_documents_ids, 1, @r###"
        docids:
        30                       0
        34                       1
        "###);

        index.delete_document("34");

        db_snap!(index, documents_ids, @"[0, ]");
        db_snap!(index, external_documents_ids, 2, @r###"
        docids:
        30                       0
        "###);

        index
            .update_settings(|s| {
                s.set_searchable_fields(vec![]);
            })
            .unwrap();

        // The key point of the test is to verify that the external documents ids
        // do not contain any entry for previously soft-deleted document ids
        db_snap!(index, documents_ids, @"[0, ]");
        db_snap!(index, external_documents_ids, 3, @r###"
        docids:
        30                       0
        "###);

        // So that when we add a new document
        index.add_documents(documents!({ "primary_key": 35, "b": 2 })).unwrap();

        db_snap!(index, documents_ids, @"[0, 1, ]");
        // The external documents ids don't have several external ids pointing to the same
        // internal document id
        db_snap!(index, external_documents_ids, 4, @r###"
        docids:
        30                       0
        35                       1
        "###);

        // And when we add 34 again, we don't replace document 35
        index.add_documents(documents!({ "primary_key": 34, "a": 1 })).unwrap();

        // And document 35 still exists, is not deleted
        db_snap!(index, documents_ids, @"[0, 1, 2, ]");
        db_snap!(index, external_documents_ids, 5, @r###"
        docids:
        30                       0
        34                       2
        35                       1
        "###);

        let rtxn = index.read_txn().unwrap();
        let dictionary = index.document_decompression_dictionary(&rtxn).unwrap();
        let (_docid, compressed_obkv) = index.compressed_documents(&rtxn, [0]).unwrap().remove(0);
        let mut buffer = Vec::new();
        let obkv = compressed_obkv
            .decompress_with_optional_dictionary(&mut buffer, dictionary.as_ref())
            .unwrap();
        let json = obkv_to_json(&[0, 1, 2], &index.fields_ids_map(&rtxn).unwrap(), obkv).unwrap();
        insta::assert_debug_snapshot!(json, @r###"
        {
            "primary_key": Number(30),
        }
        "###);

        // Furthermore, when we retrieve document 34, it is not the result of merging 35 with 34
        let (_docid, compressed_obkv) = index.compressed_documents(&rtxn, [2]).unwrap().remove(0);
        let obkv = compressed_obkv
            .decompress_with_optional_dictionary(&mut buffer, dictionary.as_ref())
            .unwrap();
        let json = obkv_to_json(&[0, 1, 2], &index.fields_ids_map(&rtxn).unwrap(), obkv).unwrap();
        insta::assert_debug_snapshot!(json, @r###"
        {
            "primary_key": Number(34),
            "a": Number(1),
        }
        "###);

        drop(dictionary);
        drop(rtxn);

        // Add new documents again
        index
            .add_documents(
                documents!([{ "primary_key": 37 }, { "primary_key": 38 }, { "primary_key": 39 }]),
            )
            .unwrap();

        db_snap!(index, documents_ids, @"[0, 1, 2, 3, 4, 5, ]");
        db_snap!(index, external_documents_ids, 6, @r###"
        docids:
        30                       0
        34                       2
        35                       1
        37                       3
        38                       4
        39                       5
        "###);
    }

    #[test]
    fn bug_3021_third() {
        // https://github.com/meilisearch/meilisearch/issues/3021
        let mut index = TempIndex::new();
        index.index_documents_config.update_method = IndexDocumentsMethod::UpdateDocuments;

        index
            .update_settings(|settings| {
                settings.set_primary_key("primary_key".to_owned());
            })
            .unwrap();

        index
            .add_documents(documents!([
                { "primary_key": 3 },
                { "primary_key": 4 },
                { "primary_key": 5 }
            ]))
            .unwrap();

        db_snap!(index, documents_ids, @"[0, 1, 2, ]");
        db_snap!(index, external_documents_ids, 1, @r###"
        docids:
        3                        0
        4                        1
        5                        2
        "###);

        index.delete_document("3");

        db_snap!(index, documents_ids, @"[1, 2, ]");
        db_snap!(index, external_documents_ids, 2, @r###"
        docids:
        4                        1
        5                        2
        "###);

        index.add_documents(documents!([{ "primary_key": "4", "a": 2 }])).unwrap();

        db_snap!(index, documents_ids, @"[1, 2, ]");
        db_snap!(index, external_documents_ids, 2, @r###"
        docids:
        4                        1
        5                        2
        "###);

        index
            .add_documents(documents!([
                { "primary_key": "3" },
            ]))
            .unwrap();

        db_snap!(index, documents_ids, @"[0, 1, 2, ]");
        db_snap!(index, external_documents_ids, 2, @r###"
        docids:
        3                        0
        4                        1
        5                        2
        "###);
    }

    #[test]
    fn bug_3021_fourth() {
        // https://github.com/meilisearch/meilisearch/issues/3021
        let mut index = TempIndex::new();
        index.index_documents_config.update_method = IndexDocumentsMethod::UpdateDocuments;

        index
            .update_settings(|settings| {
                settings.set_primary_key("primary_key".to_owned());
            })
            .unwrap();

        index
            .add_documents(documents!([
                { "primary_key": 11 },
                { "primary_key": 4 },
            ]))
            .unwrap();

        db_snap!(index, documents_ids, @"[0, 1, ]");
        db_snap!(index, external_documents_ids, @r###"
        docids:
        11                       0
        4                        1
        "###);
        db_snap!(index, fields_ids_map, @r###"
        0   primary_key      |
        "###);
        db_snap!(index, searchable_fields, @r###"["primary_key"]"###);
        db_snap!(index, fieldids_weights_map, @r###"
        fid weight
        0   0   |
        "###);

        index
            .add_documents(documents!([
                { "primary_key": 4, "a": 0 },
                { "primary_key": 1 },
            ]))
            .unwrap();

        db_snap!(index, documents_ids, @"[0, 1, 2, ]");
        db_snap!(index, external_documents_ids, @r###"
        docids:
        1                        2
        11                       0
        4                        1
        "###);
        db_snap!(index, fields_ids_map, @r###"
        0   primary_key      |
        1   a                |
        "###);
        db_snap!(index, searchable_fields, @r###"["primary_key", "a"]"###);
        db_snap!(index, fieldids_weights_map, @r###"
        fid weight
        0   0   |
        1   0   |
        "###);

        index.delete_documents(Default::default());

        db_snap!(index, documents_ids, @"[0, 1, 2, ]");
        db_snap!(index, external_documents_ids, @r###"
        docids:
        1                        2
        11                       0
        4                        1
        "###);
        db_snap!(index, fields_ids_map, @r###"
        0   primary_key      |
        1   a                |
        "###);
        db_snap!(index, searchable_fields, @r###"["primary_key", "a"]"###);
        db_snap!(index, fieldids_weights_map, @r###"
        fid weight
        0   0   |
        1   0   |
        "###);

        index
            .add_documents(documents!([
                { "primary_key": 4, "a": 1 },
                { "primary_key": 1, "a": 0 },
            ]))
            .unwrap();

        db_snap!(index, documents_ids, @"[0, 1, 2, ]");
        db_snap!(index, external_documents_ids, @r###"
        docids:
        1                        2
        11                       0
        4                        1
        "###);
        db_snap!(index, fields_ids_map, @r###"
        0   primary_key      |
        1   a                |
        "###);
        db_snap!(index, searchable_fields, @r###"["primary_key", "a"]"###);
        db_snap!(index, fieldids_weights_map, @r###"
        fid weight
        0   0   |
        1   0   |
        "###);

        let rtxn = index.read_txn().unwrap();
        let search = Search::new(&rtxn, &index);
        let SearchResult {
            matching_words: _,
            candidates: _,
            document_scores: _,
            mut documents_ids,
            degraded: _,
            used_negative_operator: _,
        } = search.execute().unwrap();
        let primary_key_id = index.fields_ids_map(&rtxn).unwrap().id("primary_key").unwrap();
        documents_ids.sort_unstable();
        let compressed_docs = index.compressed_documents(&rtxn, documents_ids).unwrap();
        let dictionary = index.document_decompression_dictionary(&rtxn).unwrap();
        let mut buffer = Vec::new();
        let mut all_ids = HashSet::new();
        for (_docid, compressed) in compressed_docs {
            let doc = compressed
                .decompress_with_optional_dictionary(&mut buffer, dictionary.as_ref())
                .unwrap();
            let id = doc.get(primary_key_id).unwrap();
            assert!(all_ids.insert(id.to_vec()));
        }
    }

    #[test]
    fn bug_3007() {
        // https://github.com/meilisearch/meilisearch/issues/3007

        use crate::error::{GeoError, UserError};
        let index = TempIndex::new();

        // Given is an index with a geo field NOT contained in the sortable_fields of the settings
        index
            .update_settings(|settings| {
                settings.set_primary_key("id".to_string());
                settings.set_filterable_fields(HashSet::from(["_geo".to_string()]));
            })
            .unwrap();

        // happy path
        index.add_documents(documents!({ "id" : 5, "_geo": {"lat": 12.0, "lng": 11.0}})).unwrap();

        db_snap!(index, geo_faceted_documents_ids);

        // both are unparseable, we expect GeoError::BadLatitudeAndLongitude
        let err1 = index
            .add_documents(
                documents!({ "id" : 6, "_geo": {"lat": "unparseable", "lng": "unparseable"}}),
            )
            .unwrap_err();
        assert!(matches!(
            err1,
            Error::UserError(UserError::InvalidGeoField(GeoError::BadLatitudeAndLongitude { .. }))
        ));

        db_snap!(index, geo_faceted_documents_ids); // ensure that no more document was inserted
    }

    #[test]
    fn unexpected_extra_fields_in_geo_field() {
        let index = TempIndex::new();

        index
            .update_settings(|settings| {
                settings.set_primary_key("id".to_string());
                settings.set_filterable_fields(HashSet::from(["_geo".to_string()]));
            })
            .unwrap();

        let err = index
            .add_documents(
                documents!({ "id" : "doggo", "_geo": { "lat": 1, "lng": 2, "doggo": "are the best" }}),
            )
            .unwrap_err();
        insta::assert_snapshot!(err, @r###"The `_geo` field in the document with the id: `"doggo"` contains the following unexpected fields: `{"doggo":"are the best"}`."###);

        db_snap!(index, geo_faceted_documents_ids); // ensure that no documents were inserted

        // multiple fields and complex values
        let err = index
            .add_documents(
                documents!({ "id" : "doggo", "_geo": { "lat": 1, "lng": 2, "doggo": "are the best", "and": { "all": ["cats", { "are": "beautiful" } ] } } }),
            )
            .unwrap_err();
        insta::assert_snapshot!(err, @r###"The `_geo` field in the document with the id: `"doggo"` contains the following unexpected fields: `{"and":{"all":["cats",{"are":"beautiful"}]},"doggo":"are the best"}`."###);

        db_snap!(index, geo_faceted_documents_ids); // ensure that no documents were inserted
    }

    #[test]
    fn swapping_searchable_attributes() {
        // See https://github.com/meilisearch/meilisearch/issues/4484

        let index = TempIndex::new();

        index
            .update_settings(|settings| {
                settings.set_searchable_fields(vec![S("name")]);
                settings.set_filterable_fields(HashSet::from([S("age")]));
            })
            .unwrap();

        index
            .add_documents(documents!({ "id": 1, "name": "Many", "age": 28, "realName": "Maxime" }))
            .unwrap();
        db_snap!(index, fields_ids_map, @r###"
        0   name             |
        1   id               |
        2   age              |
        3   realName         |
        "###);
        db_snap!(index, searchable_fields, @r###"["name"]"###);
        db_snap!(index, fieldids_weights_map, @r###"
        fid weight
        0   0   |
        "###);

        index
            .update_settings(|settings| {
                settings.set_searchable_fields(vec![S("name"), S("realName")]);
                settings.set_filterable_fields(HashSet::from([S("age")]));
            })
            .unwrap();

        // The order of the field id map shouldn't change
        db_snap!(index, fields_ids_map, @r###"
        0   name             |
        1   id               |
        2   age              |
        3   realName         |
        "###);
        db_snap!(index, searchable_fields, @r###"["name", "realName"]"###);
        db_snap!(index, fieldids_weights_map, @r###"
        fid weight
        0   0   |
        3   1   |
        "###);
    }

    #[test]
    fn attribute_weights_after_swapping_searchable_attributes() {
        // See https://github.com/meilisearch/meilisearch/issues/4484

        let index = TempIndex::new();

        index
            .update_settings(|settings| {
                settings.set_searchable_fields(vec![S("name"), S("beverage")]);
            })
            .unwrap();

        index
            .add_documents(documents!([
                { "id": 0, "name": "kefir", "beverage": "water" },
                { "id": 1, "name": "tamo",  "beverage": "kefir" }
            ]))
            .unwrap();

        let rtxn = index.read_txn().unwrap();
        let mut search = index.search(&rtxn);
        let results = search.query("kefir").execute().unwrap();

        // We should find kefir the dog first
        insta::assert_debug_snapshot!(results.documents_ids, @r###"
        [
            0,
            1,
        ]
        "###);

        index
            .update_settings(|settings| {
                settings.set_searchable_fields(vec![S("beverage"), S("name")]);
            })
            .unwrap();

        let rtxn = index.read_txn().unwrap();
        let mut search = index.search(&rtxn);
        let results = search.query("kefir").execute().unwrap();

        // We should find tamo first
        insta::assert_debug_snapshot!(results.documents_ids, @r###"
        [
            1,
            0,
        ]
        "###);
    }

    #[test]
    fn vectors_are_never_indexed_as_searchable_or_filterable() {
        let index = TempIndex::new();

        index
            .add_documents(documents!([
                { "id": 0, "_vectors": { "doggo": [2345] } },
                { "id": 1, "_vectors": { "doggo": [6789] } },
            ]))
            .unwrap();

        db_snap!(index, fields_ids_map, @r###"
        0   id               |
        1   _vectors         |
        "###);
        db_snap!(index, searchable_fields, @r###"["id"]"###);
        db_snap!(index, fieldids_weights_map, @r###"
        fid weight
        0   0   |
        "###);

        let rtxn = index.read_txn().unwrap();
        let mut search = index.search(&rtxn);
        let results = search.query("2345").execute().unwrap();
        assert!(results.candidates.is_empty());
        drop(rtxn);

        index
            .update_settings(|settings| {
                settings.set_searchable_fields(vec![S("_vectors"), S("_vectors.doggo")]);
                settings.set_filterable_fields(hashset![S("_vectors"), S("_vectors.doggo")]);
            })
            .unwrap();

        db_snap!(index, fields_ids_map, @r###"
        0   id               |
        1   _vectors         |
        2   _vectors.doggo   |
        "###);
        db_snap!(index, searchable_fields, @"[]");
        db_snap!(index, fieldids_weights_map, @r###"
        fid weight
        "###);

        let rtxn = index.read_txn().unwrap();
        let mut search = index.search(&rtxn);
        let results = search.query("2345").execute().unwrap();
        assert!(results.candidates.is_empty());

        let mut search = index.search(&rtxn);
        let results = search
            .filter(Filter::from_str("_vectors.doggo = 6789").unwrap().unwrap())
            .execute()
            .unwrap();
        assert!(results.candidates.is_empty());

        index
            .update_settings(|settings| {
                settings.set_embedder_settings(btreemap! {
                    S("doggo") => Setting::Set(EmbeddingSettings {
                        dimensions: Setting::Set(1),
                        source: Setting::Set(EmbedderSource::UserProvided),
                        ..EmbeddingSettings::default()}),
                });
            })
            .unwrap();

        db_snap!(index, fields_ids_map, @r###"
        0   id               |
        1   _vectors         |
        2   _vectors.doggo   |
        "###);
        db_snap!(index, searchable_fields, @"[]");
        db_snap!(index, fieldids_weights_map, @r###"
        fid weight
        "###);

        let rtxn = index.read_txn().unwrap();
        let mut search = index.search(&rtxn);
        let results = search.query("2345").execute().unwrap();
        assert!(results.candidates.is_empty());

        let mut search = index.search(&rtxn);
        let results = search
            .filter(Filter::from_str("_vectors.doggo = 6789").unwrap().unwrap())
            .execute()
            .unwrap();
        assert!(results.candidates.is_empty());
    }
}
