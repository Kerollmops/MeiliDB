use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::convert::TryInto;
use std::num::NonZeroUsize;
use std::result::Result as StdResult;
use std::sync::Arc;

use charabia::{Normalize, Tokenizer, TokenizerBuilder};
use deserr::{DeserializeError, Deserr};
use itertools::{EitherOrBoth, Itertools};
use roaring::RoaringBitmap;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use time::OffsetDateTime;

use super::del_add::DelAddOperation;
use super::index_documents::{IndexDocumentsConfig, Transform};
use super::IndexerConfig;
use crate::criterion::Criterion;
use crate::error::UserError;
use crate::index::{
    IndexEmbeddingConfig, PrefixSearch, DEFAULT_MIN_WORD_LEN_ONE_TYPO,
    DEFAULT_MIN_WORD_LEN_TWO_TYPOS,
};
use crate::order_by_map::OrderByMap;
use crate::prompt::default_max_bytes;
use crate::proximity::ProximityPrecision;
use crate::update::index_documents::IndexDocumentsMethod;
use crate::update::{IndexDocuments, UpdateIndexingStep};
use crate::vector::parsed_vectors::RESERVED_VECTORS_FIELD_NAME;
use crate::vector::settings::{
    check_set, check_unset, EmbedderAction, EmbedderSource, EmbeddingSettings, ReindexAction,
    WriteBackToDocuments,
};
use crate::vector::{Embedder, EmbeddingConfig, EmbeddingConfigs};
use crate::{FieldId, FieldsIdsMap, Index, LocalizedAttributesRule, LocalizedFieldIds, Result};

#[derive(Debug, Clone, PartialEq, Eq, Copy)]
pub enum Setting<T> {
    Set(T),
    Reset,
    NotSet,
}

impl<T, E> Deserr<E> for Setting<T>
where
    T: Deserr<E>,
    E: DeserializeError,
{
    fn deserialize_from_value<V: deserr::IntoValue>(
        value: deserr::Value<V>,
        location: deserr::ValuePointerRef<'_>,
    ) -> std::result::Result<Self, E> {
        match value {
            deserr::Value::Null => Ok(Setting::Reset),
            _ => T::deserialize_from_value(value, location).map(Setting::Set),
        }
    }
}

impl<T> Default for Setting<T> {
    fn default() -> Self {
        Self::NotSet
    }
}

impl<T> Setting<T> {
    pub fn set(self) -> Option<T> {
        match self {
            Self::Set(value) => Some(value),
            _ => None,
        }
    }

    pub fn some_or_not_set(option: Option<T>) -> Self {
        match option {
            Some(value) => Setting::Set(value),
            None => Setting::NotSet,
        }
    }

    pub const fn as_ref(&self) -> Setting<&T> {
        match *self {
            Self::Set(ref value) => Setting::Set(value),
            Self::Reset => Setting::Reset,
            Self::NotSet => Setting::NotSet,
        }
    }

    pub const fn is_not_set(&self) -> bool {
        matches!(self, Self::NotSet)
    }

    /// If `Self` is `Reset`, then map self to `Set` with the provided `val`.
    pub fn or_reset(self, val: T) -> Self {
        match self {
            Self::Reset => Self::Set(val),
            otherwise => otherwise,
        }
    }

    /// Returns other if self is not set.
    pub fn or(self, other: Self) -> Self {
        match self {
            Setting::Set(_) | Setting::Reset => self,
            Setting::NotSet => other,
        }
    }

    /// Returns `true` if applying the new setting changed this setting
    pub fn apply(&mut self, new: Self) -> bool
    where
        T: PartialEq + Eq,
    {
        if let Setting::NotSet = new {
            return false;
        }
        if self == &new {
            return false;
        }
        *self = new;
        true
    }
}

impl<T: Serialize> Serialize for Setting<T> {
    fn serialize<S>(&self, serializer: S) -> StdResult<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            Self::Set(value) => Some(value),
            // Usually not_set isn't serialized by setting skip_serializing_if field attribute
            Self::NotSet | Self::Reset => None,
        }
        .serialize(serializer)
    }
}

impl<'de, T: Deserialize<'de>> Deserialize<'de> for Setting<T> {
    fn deserialize<D>(deserializer: D) -> StdResult<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Deserialize::deserialize(deserializer).map(|x| match x {
            Some(x) => Self::Set(x),
            None => Self::Reset, // Reset is forced by sending null value
        })
    }
}

pub struct Settings<'a, 't, 'i> {
    wtxn: &'t mut heed::RwTxn<'i>,
    index: &'i Index,

    indexer_config: &'a IndexerConfig,

    searchable_fields: Setting<Vec<String>>,
    displayed_fields: Setting<Vec<String>>,
    filterable_fields: Setting<HashSet<String>>,
    sortable_fields: Setting<HashSet<String>>,
    criteria: Setting<Vec<Criterion>>,
    stop_words: Setting<BTreeSet<String>>,
    non_separator_tokens: Setting<BTreeSet<String>>,
    separator_tokens: Setting<BTreeSet<String>>,
    dictionary: Setting<BTreeSet<String>>,
    distinct_field: Setting<String>,
    synonyms: Setting<BTreeMap<String, Vec<String>>>,
    primary_key: Setting<String>,
    authorize_typos: Setting<bool>,
    min_word_len_two_typos: Setting<u8>,
    min_word_len_one_typo: Setting<u8>,
    exact_words: Setting<BTreeSet<String>>,
    /// Attributes on which typo tolerance is disabled.
    exact_attributes: Setting<HashSet<String>>,
    max_values_per_facet: Setting<usize>,
    sort_facet_values_by: Setting<OrderByMap>,
    pagination_max_total_hits: Setting<usize>,
    proximity_precision: Setting<ProximityPrecision>,
    embedder_settings: Setting<BTreeMap<String, Setting<EmbeddingSettings>>>,
    search_cutoff: Setting<u64>,
    localized_attributes_rules: Setting<Vec<LocalizedAttributesRule>>,
    prefix_search: Setting<PrefixSearch>,
    facet_search: Setting<bool>,
}

impl<'a, 't, 'i> Settings<'a, 't, 'i> {
    pub fn new(
        wtxn: &'t mut heed::RwTxn<'i>,
        index: &'i Index,
        indexer_config: &'a IndexerConfig,
    ) -> Settings<'a, 't, 'i> {
        Settings {
            wtxn,
            index,
            searchable_fields: Setting::NotSet,
            displayed_fields: Setting::NotSet,
            filterable_fields: Setting::NotSet,
            sortable_fields: Setting::NotSet,
            criteria: Setting::NotSet,
            stop_words: Setting::NotSet,
            non_separator_tokens: Setting::NotSet,
            separator_tokens: Setting::NotSet,
            dictionary: Setting::NotSet,
            distinct_field: Setting::NotSet,
            synonyms: Setting::NotSet,
            primary_key: Setting::NotSet,
            authorize_typos: Setting::NotSet,
            exact_words: Setting::NotSet,
            min_word_len_two_typos: Setting::NotSet,
            min_word_len_one_typo: Setting::NotSet,
            exact_attributes: Setting::NotSet,
            max_values_per_facet: Setting::NotSet,
            sort_facet_values_by: Setting::NotSet,
            pagination_max_total_hits: Setting::NotSet,
            proximity_precision: Setting::NotSet,
            embedder_settings: Setting::NotSet,
            search_cutoff: Setting::NotSet,
            localized_attributes_rules: Setting::NotSet,
            prefix_search: Setting::NotSet,
            facet_search: Setting::NotSet,
            indexer_config,
        }
    }

    pub fn reset_searchable_fields(&mut self) {
        self.searchable_fields = Setting::Reset;
    }

    pub fn set_searchable_fields(&mut self, names: Vec<String>) {
        self.searchable_fields = Setting::Set(names);
    }

    pub fn reset_displayed_fields(&mut self) {
        self.displayed_fields = Setting::Reset;
    }

    pub fn set_displayed_fields(&mut self, names: Vec<String>) {
        self.displayed_fields = Setting::Set(names);
    }

    pub fn reset_filterable_fields(&mut self) {
        self.filterable_fields = Setting::Reset;
    }

    pub fn set_filterable_fields(&mut self, names: HashSet<String>) {
        self.filterable_fields = Setting::Set(names);
    }

    pub fn set_sortable_fields(&mut self, names: HashSet<String>) {
        self.sortable_fields = Setting::Set(names);
    }

    pub fn reset_sortable_fields(&mut self) {
        self.sortable_fields = Setting::Reset;
    }

    pub fn reset_criteria(&mut self) {
        self.criteria = Setting::Reset;
    }

    pub fn set_criteria(&mut self, criteria: Vec<Criterion>) {
        self.criteria = Setting::Set(criteria);
    }

    pub fn reset_stop_words(&mut self) {
        self.stop_words = Setting::Reset;
    }

    pub fn set_stop_words(&mut self, stop_words: BTreeSet<String>) {
        self.stop_words =
            if stop_words.is_empty() { Setting::Reset } else { Setting::Set(stop_words) }
    }

    pub fn reset_non_separator_tokens(&mut self) {
        self.non_separator_tokens = Setting::Reset;
    }

    pub fn set_non_separator_tokens(&mut self, non_separator_tokens: BTreeSet<String>) {
        self.non_separator_tokens = if non_separator_tokens.is_empty() {
            Setting::Reset
        } else {
            Setting::Set(non_separator_tokens)
        }
    }

    pub fn reset_separator_tokens(&mut self) {
        self.separator_tokens = Setting::Reset;
    }

    pub fn set_separator_tokens(&mut self, separator_tokens: BTreeSet<String>) {
        self.separator_tokens = if separator_tokens.is_empty() {
            Setting::Reset
        } else {
            Setting::Set(separator_tokens)
        }
    }

    pub fn reset_dictionary(&mut self) {
        self.dictionary = Setting::Reset;
    }

    pub fn set_dictionary(&mut self, dictionary: BTreeSet<String>) {
        self.dictionary =
            if dictionary.is_empty() { Setting::Reset } else { Setting::Set(dictionary) }
    }

    pub fn reset_distinct_field(&mut self) {
        self.distinct_field = Setting::Reset;
    }

    pub fn set_distinct_field(&mut self, distinct_field: String) {
        self.distinct_field = Setting::Set(distinct_field);
    }

    pub fn reset_synonyms(&mut self) {
        self.synonyms = Setting::Reset;
    }

    pub fn set_synonyms(&mut self, synonyms: BTreeMap<String, Vec<String>>) {
        self.synonyms = if synonyms.is_empty() { Setting::Reset } else { Setting::Set(synonyms) }
    }

    pub fn reset_primary_key(&mut self) {
        self.primary_key = Setting::Reset;
    }

    pub fn set_primary_key(&mut self, primary_key: String) {
        self.primary_key = Setting::Set(primary_key);
    }

    pub fn set_autorize_typos(&mut self, val: bool) {
        self.authorize_typos = Setting::Set(val);
    }

    pub fn reset_authorize_typos(&mut self) {
        self.authorize_typos = Setting::Reset;
    }

    pub fn set_min_word_len_two_typos(&mut self, val: u8) {
        self.min_word_len_two_typos = Setting::Set(val);
    }

    pub fn reset_min_word_len_two_typos(&mut self) {
        self.min_word_len_two_typos = Setting::Reset;
    }

    pub fn set_min_word_len_one_typo(&mut self, val: u8) {
        self.min_word_len_one_typo = Setting::Set(val);
    }

    pub fn reset_min_word_len_one_typo(&mut self) {
        self.min_word_len_one_typo = Setting::Reset;
    }

    pub fn set_exact_words(&mut self, words: BTreeSet<String>) {
        self.exact_words = Setting::Set(words);
    }

    pub fn reset_exact_words(&mut self) {
        self.exact_words = Setting::Reset;
    }

    pub fn set_exact_attributes(&mut self, attrs: HashSet<String>) {
        self.exact_attributes = Setting::Set(attrs);
    }

    pub fn reset_exact_attributes(&mut self) {
        self.exact_attributes = Setting::Reset;
    }

    pub fn set_max_values_per_facet(&mut self, value: usize) {
        self.max_values_per_facet = Setting::Set(value);
    }

    pub fn reset_max_values_per_facet(&mut self) {
        self.max_values_per_facet = Setting::Reset;
    }

    pub fn set_sort_facet_values_by(&mut self, value: OrderByMap) {
        self.sort_facet_values_by = Setting::Set(value);
    }

    pub fn reset_sort_facet_values_by(&mut self) {
        self.sort_facet_values_by = Setting::Reset;
    }

    pub fn set_pagination_max_total_hits(&mut self, value: usize) {
        self.pagination_max_total_hits = Setting::Set(value);
    }

    pub fn reset_pagination_max_total_hits(&mut self) {
        self.pagination_max_total_hits = Setting::Reset;
    }

    pub fn set_proximity_precision(&mut self, value: ProximityPrecision) {
        self.proximity_precision = Setting::Set(value);
    }

    pub fn reset_proximity_precision(&mut self) {
        self.proximity_precision = Setting::Reset;
    }

    pub fn set_embedder_settings(&mut self, value: BTreeMap<String, Setting<EmbeddingSettings>>) {
        self.embedder_settings = Setting::Set(value);
    }

    pub fn reset_embedder_settings(&mut self) {
        self.embedder_settings = Setting::Reset;
    }

    pub fn set_search_cutoff(&mut self, value: u64) {
        self.search_cutoff = Setting::Set(value);
    }

    pub fn reset_search_cutoff(&mut self) {
        self.search_cutoff = Setting::Reset;
    }

    pub fn set_localized_attributes_rules(&mut self, value: Vec<LocalizedAttributesRule>) {
        self.localized_attributes_rules = Setting::Set(value);
    }

    pub fn reset_localized_attributes_rules(&mut self) {
        self.localized_attributes_rules = Setting::Reset;
    }

    pub fn set_prefix_search(&mut self, value: PrefixSearch) {
        self.prefix_search = Setting::Set(value);
    }

    pub fn reset_prefix_search(&mut self) {
        self.prefix_search = Setting::Reset;
    }

    pub fn set_facet_search(&mut self, value: bool) {
        self.facet_search = Setting::Set(value);
    }

    pub fn reset_facet_search(&mut self) {
        self.facet_search = Setting::Reset;
    }

    #[tracing::instrument(
        level = "trace"
        skip(self, progress_callback, should_abort, settings_diff),
        target = "indexing::documents"
    )]
    fn reindex<FP, FA>(
        &mut self,
        progress_callback: &FP,
        should_abort: &FA,
        settings_diff: InnerIndexSettingsDiff,
    ) -> Result<()>
    where
        FP: Fn(UpdateIndexingStep) + Sync,
        FA: Fn() -> bool + Sync,
    {
        // if the settings are set before any document update, we don't need to do anything, and
        // will set the primary key during the first document addition.
        if self.index.number_of_documents(self.wtxn)? == 0 {
            return Ok(());
        }

        let transform = Transform::new(
            self.wtxn,
            self.index,
            self.indexer_config,
            IndexDocumentsMethod::ReplaceDocuments,
            false,
        )?;

        // We clear the databases and remap the documents fields based on the new `FieldsIdsMap`.
        let output = transform.prepare_for_documents_reindexing(self.wtxn, settings_diff)?;

        // We index the generated `TransformOutput` which must contain
        // all the documents with fields in the newly defined searchable order.
        let indexing_builder = IndexDocuments::new(
            self.wtxn,
            self.index,
            self.indexer_config,
            IndexDocumentsConfig::default(),
            &progress_callback,
            &should_abort,
        )?;

        indexing_builder.execute_raw(output)?;

        Ok(())
    }

    fn update_displayed(&mut self) -> Result<bool> {
        match self.displayed_fields {
            Setting::Set(ref fields) => {
                // fields are deduplicated, only the first occurrence is taken into account
                let names: Vec<_> = fields.iter().unique().map(String::as_str).collect();
                self.index.put_displayed_fields(self.wtxn, &names)?;
            }
            Setting::Reset => {
                self.index.delete_displayed_fields(self.wtxn)?;
            }
            Setting::NotSet => return Ok(false),
        }
        Ok(true)
    }

    fn update_distinct_field(&mut self) -> Result<bool> {
        match self.distinct_field {
            Setting::Set(ref attr) => {
                self.index.put_distinct_field(self.wtxn, attr)?;
            }
            Setting::Reset => {
                self.index.delete_distinct_field(self.wtxn)?;
            }
            Setting::NotSet => return Ok(false),
        }
        Ok(true)
    }

    /// Updates the index's searchable attributes.
    fn update_searchable(&mut self) -> Result<bool> {
        match self.searchable_fields {
            Setting::Set(ref fields) => {
                // Check to see if the searchable fields changed before doing anything else
                let old_fields = self.index.searchable_fields(self.wtxn)?;
                let did_change = {
                    let new_fields = fields.iter().map(String::as_str).collect::<Vec<_>>();
                    new_fields != old_fields
                };
                if !did_change {
                    return Ok(false);
                }

                // Since we're updating the settings we can only add new fields at the end of the field id map
                let mut fields_ids_map = self.index.fields_ids_map(self.wtxn)?;
                // fields are deduplicated, only the first occurrence is taken into account
                let names = fields.iter().unique().map(String::as_str).collect::<Vec<_>>();

                // Add all the searchable attributes to the field map, and then add the
                // remaining fields from the old field map to the new one
                for name in names.iter() {
                    // The fields ids map won't change the field id of already present elements thus only the
                    // new fields will be inserted.
                    fields_ids_map.insert(name).ok_or(UserError::AttributeLimitReached)?;
                }

                self.index.put_all_searchable_fields_from_fields_ids_map(
                    self.wtxn,
                    &names,
                    &fields_ids_map.nested_ids(RESERVED_VECTORS_FIELD_NAME),
                    &fields_ids_map,
                )?;
                self.index.put_fields_ids_map(self.wtxn, &fields_ids_map)?;
                Ok(true)
            }
            Setting::Reset => Ok(self.index.delete_all_searchable_fields(self.wtxn)?),
            Setting::NotSet => Ok(false),
        }
    }

    fn update_stop_words(&mut self) -> Result<bool> {
        match self.stop_words {
            Setting::Set(ref stop_words) => {
                let current = self.index.stop_words(self.wtxn)?;

                // Apply an unlossy normalization on stop_words
                let stop_words: BTreeSet<String> = stop_words
                    .iter()
                    .map(|w| w.as_str().normalize(&Default::default()).into_owned())
                    .collect();

                // since we can't compare a BTreeSet with an FST we are going to convert the
                // BTreeSet to an FST and then compare bytes per bytes the two FSTs.
                let fst = fst::Set::from_iter(stop_words.into_iter())?;

                // Does the new FST differ from the previous one?
                if current
                    .map_or(true, |current| current.as_fst().as_bytes() != fst.as_fst().as_bytes())
                {
                    // we want to re-create our FST.
                    self.index.put_stop_words(self.wtxn, &fst)?;
                    Ok(true)
                } else {
                    Ok(false)
                }
            }
            Setting::Reset => Ok(self.index.delete_stop_words(self.wtxn)?),
            Setting::NotSet => Ok(false),
        }
    }

    fn update_non_separator_tokens(&mut self) -> Result<bool> {
        let changes = match self.non_separator_tokens {
            Setting::Set(ref non_separator_tokens) => {
                let current = self.index.non_separator_tokens(self.wtxn)?;

                // Does the new list differ from the previous one?
                if current.map_or(true, |current| &current != non_separator_tokens) {
                    self.index.put_non_separator_tokens(self.wtxn, non_separator_tokens)?;
                    true
                } else {
                    false
                }
            }
            Setting::Reset => self.index.delete_non_separator_tokens(self.wtxn)?,
            Setting::NotSet => false,
        };

        // the synonyms must be updated if non separator tokens have been updated.
        if changes && self.synonyms == Setting::NotSet {
            self.synonyms = Setting::Set(self.index.user_defined_synonyms(self.wtxn)?);
        }

        Ok(changes)
    }

    fn update_separator_tokens(&mut self) -> Result<bool> {
        let changes = match self.separator_tokens {
            Setting::Set(ref separator_tokens) => {
                let current = self.index.separator_tokens(self.wtxn)?;

                // Does the new list differ from the previous one?
                if current.map_or(true, |current| &current != separator_tokens) {
                    self.index.put_separator_tokens(self.wtxn, separator_tokens)?;
                    true
                } else {
                    false
                }
            }
            Setting::Reset => self.index.delete_separator_tokens(self.wtxn)?,
            Setting::NotSet => false,
        };

        // the synonyms must be updated if separator tokens have been updated.
        if changes && self.synonyms == Setting::NotSet {
            self.synonyms = Setting::Set(self.index.user_defined_synonyms(self.wtxn)?);
        }

        Ok(changes)
    }

    fn update_dictionary(&mut self) -> Result<bool> {
        let changes = match self.dictionary {
            Setting::Set(ref dictionary) => {
                let current = self.index.dictionary(self.wtxn)?;

                // Does the new list differ from the previous one?
                if current.map_or(true, |current| &current != dictionary) {
                    self.index.put_dictionary(self.wtxn, dictionary)?;
                    true
                } else {
                    false
                }
            }
            Setting::Reset => self.index.delete_dictionary(self.wtxn)?,
            Setting::NotSet => false,
        };

        // the synonyms must be updated if dictionary has been updated.
        if changes && self.synonyms == Setting::NotSet {
            self.synonyms = Setting::Set(self.index.user_defined_synonyms(self.wtxn)?);
        }

        Ok(changes)
    }

    fn update_synonyms(&mut self) -> Result<bool> {
        match self.synonyms {
            Setting::Set(ref user_synonyms) => {
                fn normalize(tokenizer: &Tokenizer<'_>, text: &str) -> Vec<String> {
                    tokenizer
                        .tokenize(text)
                        .filter_map(|token| {
                            if token.is_word() && !token.lemma().is_empty() {
                                Some(token.lemma().to_string())
                            } else {
                                None
                            }
                        })
                        .collect::<Vec<_>>()
                }

                let mut builder = TokenizerBuilder::new();
                let stop_words = self.index.stop_words(self.wtxn)?;
                if let Some(ref stop_words) = stop_words {
                    builder.stop_words(stop_words);
                }

                let separators = self.index.allowed_separators(self.wtxn)?;
                let separators: Option<Vec<_>> =
                    separators.as_ref().map(|x| x.iter().map(String::as_str).collect());
                if let Some(ref separators) = separators {
                    builder.separators(separators);
                }

                let dictionary = self.index.dictionary(self.wtxn)?;
                let dictionary: Option<Vec<_>> =
                    dictionary.as_ref().map(|x| x.iter().map(String::as_str).collect());
                if let Some(ref dictionary) = dictionary {
                    builder.words_dict(dictionary);
                }

                let tokenizer = builder.build();

                let mut new_synonyms = HashMap::new();
                for (word, synonyms) in user_synonyms {
                    // Normalize both the word and associated synonyms.
                    let normalized_word = normalize(&tokenizer, word);
                    let normalized_synonyms: Vec<_> = synonyms
                        .iter()
                        .map(|synonym| normalize(&tokenizer, synonym))
                        .filter(|synonym| !synonym.is_empty())
                        .collect();

                    // Store the normalized synonyms under the normalized word,
                    // merging the possible duplicate words.
                    if !normalized_word.is_empty() && !normalized_synonyms.is_empty() {
                        let entry = new_synonyms.entry(normalized_word).or_insert_with(Vec::new);
                        entry.extend(normalized_synonyms.into_iter());
                    }
                }

                // Make sure that we don't have duplicate synonyms.
                new_synonyms.iter_mut().for_each(|(_, synonyms)| {
                    synonyms.sort_unstable();
                    synonyms.dedup();
                });

                let old_synonyms = self.index.synonyms(self.wtxn)?;

                if new_synonyms != old_synonyms {
                    self.index.put_synonyms(self.wtxn, &new_synonyms, user_synonyms)?;
                    Ok(true)
                } else {
                    Ok(false)
                }
            }
            Setting::Reset => Ok(self.index.delete_synonyms(self.wtxn)?),
            Setting::NotSet => Ok(false),
        }
    }

    fn update_exact_attributes(&mut self) -> Result<bool> {
        match self.exact_attributes {
            Setting::Set(ref attrs) => {
                let old_attrs = self.index.exact_attributes(self.wtxn)?;
                let old_attrs = old_attrs.into_iter().map(String::from).collect::<HashSet<_>>();

                if attrs != &old_attrs {
                    let attrs = attrs.iter().map(String::as_str).collect::<Vec<_>>();
                    self.index.put_exact_attributes(self.wtxn, &attrs)?;
                    Ok(true)
                } else {
                    Ok(false)
                }
            }
            Setting::Reset => Ok(self.index.delete_exact_attributes(self.wtxn)?),
            Setting::NotSet => Ok(false),
        }
    }

    fn update_filterable(&mut self) -> Result<()> {
        match self.filterable_fields {
            Setting::Set(ref fields) => {
                let mut new_facets = HashSet::new();
                for name in fields {
                    new_facets.insert(name.clone());
                }
                self.index.put_filterable_fields(self.wtxn, &new_facets)?;
            }
            Setting::Reset => {
                self.index.delete_filterable_fields(self.wtxn)?;
            }
            Setting::NotSet => (),
        }
        Ok(())
    }

    fn update_sortable(&mut self) -> Result<()> {
        match self.sortable_fields {
            Setting::Set(ref fields) => {
                let mut new_fields = HashSet::new();
                for name in fields {
                    new_fields.insert(name.clone());
                }
                self.index.put_sortable_fields(self.wtxn, &new_fields)?;
            }
            Setting::Reset => {
                self.index.delete_sortable_fields(self.wtxn)?;
            }
            Setting::NotSet => (),
        }
        Ok(())
    }

    fn update_criteria(&mut self) -> Result<()> {
        match &self.criteria {
            Setting::Set(criteria) => {
                self.index.put_criteria(self.wtxn, criteria)?;
            }
            Setting::Reset => {
                self.index.delete_criteria(self.wtxn)?;
            }
            Setting::NotSet => (),
        }
        Ok(())
    }

    fn update_primary_key(&mut self) -> Result<()> {
        match self.primary_key {
            Setting::Set(ref primary_key) => {
                if self.index.number_of_documents(self.wtxn)? == 0 {
                    let mut fields_ids_map = self.index.fields_ids_map(self.wtxn)?;
                    fields_ids_map.insert(primary_key).ok_or(UserError::AttributeLimitReached)?;
                    self.index.put_fields_ids_map(self.wtxn, &fields_ids_map)?;
                    self.index.put_primary_key(self.wtxn, primary_key)?;
                    Ok(())
                } else {
                    let curr_primary_key = self.index.primary_key(self.wtxn)?.unwrap().to_string();
                    if primary_key == &curr_primary_key {
                        Ok(())
                    } else {
                        Err(UserError::PrimaryKeyCannotBeChanged(curr_primary_key).into())
                    }
                }
            }
            Setting::Reset => {
                if self.index.number_of_documents(self.wtxn)? == 0 {
                    self.index.delete_primary_key(self.wtxn)?;
                    Ok(())
                } else {
                    let primary_key = self.index.primary_key(self.wtxn)?.unwrap();
                    Err(UserError::PrimaryKeyCannotBeChanged(primary_key.to_string()).into())
                }
            }
            Setting::NotSet => Ok(()),
        }
    }

    fn update_authorize_typos(&mut self) -> Result<()> {
        match self.authorize_typos {
            Setting::Set(flag) => {
                self.index.put_authorize_typos(self.wtxn, flag)?;
                Ok(())
            }
            Setting::Reset => {
                self.index.put_authorize_typos(self.wtxn, true)?;
                Ok(())
            }
            Setting::NotSet => Ok(()),
        }
    }

    fn update_min_typo_word_len(&mut self) -> Result<()> {
        let one = self.min_word_len_one_typo.or_reset(DEFAULT_MIN_WORD_LEN_ONE_TYPO);
        let two = self.min_word_len_two_typos.or_reset(DEFAULT_MIN_WORD_LEN_TWO_TYPOS);
        match (one, two) {
            (Setting::Set(one), Setting::Set(two)) => {
                if one > two {
                    return Err(UserError::InvalidMinTypoWordLenSetting(one, two).into());
                } else {
                    self.index.put_min_word_len_one_typo(self.wtxn, one)?;
                    self.index.put_min_word_len_two_typos(self.wtxn, two)?;
                }
            }
            (Setting::Set(one), _) => {
                let two = self.index.min_word_len_two_typos(self.wtxn)?;
                if one > two {
                    return Err(UserError::InvalidMinTypoWordLenSetting(one, two).into());
                } else {
                    self.index.put_min_word_len_one_typo(self.wtxn, one)?;
                }
            }
            (_, Setting::Set(two)) => {
                let one = self.index.min_word_len_one_typo(self.wtxn)?;
                if one > two {
                    return Err(UserError::InvalidMinTypoWordLenSetting(one, two).into());
                } else {
                    self.index.put_min_word_len_two_typos(self.wtxn, two)?;
                }
            }
            _ => (),
        }

        Ok(())
    }

    fn update_exact_words(&mut self) -> Result<()> {
        match self.exact_words {
            Setting::Set(ref mut words) => {
                fn normalize(tokenizer: &Tokenizer<'_>, text: &str) -> String {
                    tokenizer.tokenize(text).map(|token| token.lemma().to_string()).collect()
                }

                let mut builder = TokenizerBuilder::new();
                let stop_words = self.index.stop_words(self.wtxn)?;
                if let Some(ref stop_words) = stop_words {
                    builder.stop_words(stop_words);
                }
                let tokenizer = builder.build();

                let mut words: Vec<_> =
                    words.iter().map(|word| normalize(&tokenizer, word)).collect();

                // normalization could reorder words
                words.sort_unstable();

                let words = fst::Set::from_iter(words.iter())?;
                self.index.put_exact_words(self.wtxn, &words)?;
            }
            Setting::Reset => {
                self.index.put_exact_words(self.wtxn, &fst::Set::default())?;
            }
            Setting::NotSet => (),
        }

        Ok(())
    }

    fn update_max_values_per_facet(&mut self) -> Result<()> {
        match self.max_values_per_facet {
            Setting::Set(max) => {
                self.index.put_max_values_per_facet(self.wtxn, max as u64)?;
            }
            Setting::Reset => {
                self.index.delete_max_values_per_facet(self.wtxn)?;
            }
            Setting::NotSet => (),
        }

        Ok(())
    }

    fn update_sort_facet_values_by(&mut self) -> Result<()> {
        match self.sort_facet_values_by.as_ref() {
            Setting::Set(value) => {
                self.index.put_sort_facet_values_by(self.wtxn, value)?;
            }
            Setting::Reset => {
                self.index.delete_sort_facet_values_by(self.wtxn)?;
            }
            Setting::NotSet => (),
        }

        Ok(())
    }

    fn update_pagination_max_total_hits(&mut self) -> Result<()> {
        match self.pagination_max_total_hits {
            Setting::Set(max) => {
                self.index.put_pagination_max_total_hits(self.wtxn, max as u64)?;
            }
            Setting::Reset => {
                self.index.delete_pagination_max_total_hits(self.wtxn)?;
            }
            Setting::NotSet => (),
        }

        Ok(())
    }

    fn update_proximity_precision(&mut self) -> Result<bool> {
        let changed = match self.proximity_precision {
            Setting::Set(new) => {
                let old = self.index.proximity_precision(self.wtxn)?;
                if old == Some(new) {
                    false
                } else {
                    self.index.put_proximity_precision(self.wtxn, new)?;
                    old.is_some() || new != ProximityPrecision::default()
                }
            }
            Setting::Reset => self.index.delete_proximity_precision(self.wtxn)?,
            Setting::NotSet => false,
        };

        Ok(changed)
    }

    fn update_prefix_search(&mut self) -> Result<bool> {
        let changed = match self.prefix_search {
            Setting::Set(new) => {
                let old = self.index.prefix_search(self.wtxn)?;
                if old == Some(new) {
                    false
                } else {
                    self.index.put_prefix_search(self.wtxn, new)?;
                    old.is_some() || new != PrefixSearch::default()
                }
            }
            Setting::Reset => self.index.delete_prefix_search(self.wtxn)?,
            Setting::NotSet => false,
        };

        Ok(changed)
    }

    fn update_facet_search(&mut self) -> Result<bool> {
        let changed = match self.facet_search {
            Setting::Set(new) => {
                let old = self.index.facet_search(self.wtxn)?;
                if old == new {
                    false
                } else {
                    self.index.put_facet_search(self.wtxn, new)?;
                    true
                }
            }
            Setting::Reset => self.index.delete_facet_search(self.wtxn)?,
            Setting::NotSet => false,
        };

        Ok(changed)
    }

    fn update_embedding_configs(&mut self) -> Result<BTreeMap<String, EmbedderAction>> {
        match std::mem::take(&mut self.embedder_settings) {
            Setting::Set(configs) => self.update_embedding_configs_set(configs),
            Setting::Reset => {
                // all vectors should be written back to documents
                let old_configs = self.index.embedding_configs(self.wtxn)?;
                let remove_all: Result<BTreeMap<String, EmbedderAction>> = old_configs
                    .into_iter()
                    .map(|IndexEmbeddingConfig { name, config, user_provided }| -> Result<_> {
                        let embedder_id =
                            self.index.embedder_category_id.get(self.wtxn, &name)?.ok_or(
                                crate::InternalError::DatabaseMissingEntry {
                                    db_name: crate::index::db_name::VECTOR_EMBEDDER_CATEGORY_ID,
                                    key: None,
                                },
                            )?;
                        Ok((
                            name,
                            EmbedderAction::with_write_back(
                                WriteBackToDocuments { embedder_id, user_provided },
                                config.quantized(),
                            ),
                        ))
                    })
                    .collect();

                let remove_all = remove_all?;

                self.index.embedder_category_id.clear(self.wtxn)?;
                self.index.delete_embedding_configs(self.wtxn)?;
                Ok(remove_all)
            }
            Setting::NotSet => Ok(Default::default()),
        }
    }

    fn update_embedding_configs_set(
        &mut self,
        configs: BTreeMap<String, Setting<EmbeddingSettings>>,
    ) -> Result<BTreeMap<String, EmbedderAction>> {
        use crate::vector::settings::SettingsDiff;

        let old_configs = self.index.embedding_configs(self.wtxn)?;
        let old_configs: BTreeMap<String, (EmbeddingSettings, RoaringBitmap)> = old_configs
            .into_iter()
            .map(|IndexEmbeddingConfig { name, config, user_provided }| {
                (name, (config.into(), user_provided))
            })
            .collect();
        let mut updated_configs = BTreeMap::new();
        let mut embedder_actions = BTreeMap::new();
        for joined in old_configs
            .into_iter()
            .merge_join_by(configs.into_iter(), |(left, _), (right, _)| left.cmp(right))
        {
            match joined {
                // updated config
                EitherOrBoth::Both((name, (old, user_provided)), (_, new)) => {
                    let was_quantized = old.binary_quantized.set().unwrap_or_default();
                    let settings_diff = SettingsDiff::from_settings(&name, old, new)?;
                    match settings_diff {
                        SettingsDiff::Remove => {
                            tracing::debug!(
                                embedder = name,
                                user_provided = user_provided.len(),
                                "removing embedder"
                            );
                            let embedder_id =
                                self.index.embedder_category_id.get(self.wtxn, &name)?.ok_or(
                                    crate::InternalError::DatabaseMissingEntry {
                                        db_name: crate::index::db_name::VECTOR_EMBEDDER_CATEGORY_ID,
                                        key: None,
                                    },
                                )?;
                            // free id immediately
                            self.index.embedder_category_id.delete(self.wtxn, &name)?;
                            embedder_actions.insert(
                                name,
                                EmbedderAction::with_write_back(
                                    WriteBackToDocuments { embedder_id, user_provided },
                                    was_quantized,
                                ),
                            );
                        }
                        SettingsDiff::Reindex { action, updated_settings, quantize } => {
                            tracing::debug!(
                                embedder = name,
                                user_provided = user_provided.len(),
                                ?action,
                                "reindex embedder"
                            );
                            embedder_actions.insert(
                                name.clone(),
                                EmbedderAction::with_reindex(action, was_quantized)
                                    .with_is_being_quantized(quantize),
                            );
                            let new =
                                validate_embedding_settings(Setting::Set(updated_settings), &name)?;
                            updated_configs.insert(name, (new, user_provided));
                        }
                        SettingsDiff::UpdateWithoutReindex { updated_settings, quantize } => {
                            tracing::debug!(
                                embedder = name,
                                user_provided = user_provided.len(),
                                "update without reindex embedder"
                            );
                            let new =
                                validate_embedding_settings(Setting::Set(updated_settings), &name)?;
                            if quantize {
                                embedder_actions.insert(
                                    name.clone(),
                                    EmbedderAction::default().with_is_being_quantized(true),
                                );
                            }
                            updated_configs.insert(name, (new, user_provided));
                        }
                    }
                }
                // unchanged config
                EitherOrBoth::Left((name, (setting, user_provided))) => {
                    tracing::debug!(embedder = name, "unchanged embedder");
                    updated_configs.insert(name, (Setting::Set(setting), user_provided));
                }
                // new config
                EitherOrBoth::Right((name, mut setting)) => {
                    tracing::debug!(embedder = name, "new embedder");
                    // apply the default source in case the source was not set so that it gets validated
                    crate::vector::settings::EmbeddingSettings::apply_default_source(&mut setting);
                    crate::vector::settings::EmbeddingSettings::apply_default_openai_model(
                        &mut setting,
                    );
                    let setting = validate_embedding_settings(setting, &name)?;
                    embedder_actions.insert(
                        name.clone(),
                        EmbedderAction::with_reindex(ReindexAction::FullReindex, false),
                    );
                    updated_configs.insert(name, (setting, RoaringBitmap::new()));
                }
            }
        }
        let mut free_indices: [bool; u8::MAX as usize] = [true; u8::MAX as usize];
        for res in self.index.embedder_category_id.iter(self.wtxn)? {
            let (_name, id) = res?;
            free_indices[id as usize] = false;
        }
        let mut free_indices = free_indices.iter_mut().enumerate();
        let mut find_free_index =
            move || free_indices.find(|(_, free)| **free).map(|(index, _)| index as u8);
        for (name, action) in embedder_actions.iter() {
            // ignore actions that are not possible for a new embedder
            if matches!(action.reindex(), Some(ReindexAction::FullReindex))
                && self.index.embedder_category_id.get(self.wtxn, name)?.is_none()
            {
                let id =
                    find_free_index().ok_or(UserError::TooManyEmbedders(updated_configs.len()))?;
                tracing::debug!(embedder = name, id, "assigning free id to new embedder");
                self.index.embedder_category_id.put(self.wtxn, name, &id)?;
            }
        }
        let updated_configs: Vec<IndexEmbeddingConfig> = updated_configs
            .into_iter()
            .filter_map(|(name, (config, user_provided))| match config {
                Setting::Set(config) => {
                    Some(IndexEmbeddingConfig { name, config: config.into(), user_provided })
                }
                Setting::Reset => None,
                Setting::NotSet => Some(IndexEmbeddingConfig {
                    name,
                    config: EmbeddingSettings::default().into(),
                    user_provided,
                }),
            })
            .collect();
        if updated_configs.is_empty() {
            self.index.delete_embedding_configs(self.wtxn)?;
        } else {
            self.index.put_embedding_configs(self.wtxn, updated_configs)?;
        }
        Ok(embedder_actions)
    }

    fn update_search_cutoff(&mut self) -> Result<bool> {
        let changed = match self.search_cutoff {
            Setting::Set(new) => {
                let old = self.index.search_cutoff(self.wtxn)?;
                if old == Some(new) {
                    false
                } else {
                    self.index.put_search_cutoff(self.wtxn, new)?;
                    true
                }
            }
            Setting::Reset => self.index.delete_search_cutoff(self.wtxn)?,
            Setting::NotSet => false,
        };

        Ok(changed)
    }

    fn update_localized_attributes_rules(&mut self) -> Result<()> {
        match &self.localized_attributes_rules {
            Setting::Set(new) => {
                let old = self.index.localized_attributes_rules(self.wtxn)?;
                if old.as_ref() != Some(new) {
                    self.index.put_localized_attributes_rules(self.wtxn, new.clone())?;
                }
            }
            Setting::Reset => {
                self.index.delete_localized_attributes_rules(self.wtxn)?;
            }
            Setting::NotSet => (),
        }

        Ok(())
    }

    pub fn execute<FP, FA>(mut self, progress_callback: FP, should_abort: FA) -> Result<()>
    where
        FP: Fn(UpdateIndexingStep) + Sync,
        FA: Fn() -> bool + Sync,
    {
        self.index.set_updated_at(self.wtxn, &OffsetDateTime::now_utc())?;

        let old_inner_settings = InnerIndexSettings::from_index(self.index, self.wtxn, None)?;

        // never trigger re-indexing
        self.update_displayed()?;
        self.update_distinct_field()?;
        self.update_criteria()?;
        self.update_primary_key()?;
        self.update_authorize_typos()?;
        self.update_min_typo_word_len()?;
        self.update_exact_words()?;
        self.update_max_values_per_facet()?;
        self.update_sort_facet_values_by()?;
        self.update_pagination_max_total_hits()?;
        self.update_search_cutoff()?;

        // could trigger re-indexing
        self.update_filterable()?;
        self.update_sortable()?;
        self.update_stop_words()?;
        self.update_non_separator_tokens()?;
        self.update_separator_tokens()?;
        self.update_dictionary()?;
        self.update_synonyms()?;
        self.update_searchable()?;
        self.update_exact_attributes()?;
        self.update_proximity_precision()?;
        self.update_prefix_search()?;
        self.update_facet_search()?;
        self.update_localized_attributes_rules()?;

        let embedding_config_updates = self.update_embedding_configs()?;

        let mut new_inner_settings = InnerIndexSettings::from_index(self.index, self.wtxn, None)?;
        new_inner_settings.recompute_facets(self.wtxn, self.index)?;

        let primary_key_id = self
            .index
            .primary_key(self.wtxn)?
            .and_then(|name| new_inner_settings.fields_ids_map.id(name));
        let settings_update_only = true;
        let inner_settings_diff = InnerIndexSettingsDiff::new(
            old_inner_settings,
            new_inner_settings,
            primary_key_id,
            embedding_config_updates,
            settings_update_only,
        );

        if inner_settings_diff.any_reindexing_needed() {
            self.reindex(&progress_callback, &should_abort, inner_settings_diff)?;
        }

        Ok(())
    }
}

pub struct InnerIndexSettingsDiff {
    pub(crate) old: InnerIndexSettings,
    pub(crate) new: InnerIndexSettings,
    pub(crate) primary_key_id: Option<FieldId>,
    pub(crate) embedding_config_updates: BTreeMap<String, EmbedderAction>,
    pub(crate) settings_update_only: bool,
    /// The set of only the additional searchable fields.
    /// If any other searchable field has been modified, is set to None.
    pub(crate) only_additional_fields: Option<HashSet<String>>,

    // Cache the check to see if all the stop_words, allowed_separators, dictionary,
    // exact_attributes, proximity_precision are different.
    pub(crate) cache_reindex_searchable_without_user_defined: bool,
    // Cache the check to see if the user_defined_searchables are different.
    pub(crate) cache_user_defined_searchables: bool,
    // Cache the check to see if the exact_attributes are different.
    pub(crate) cache_exact_attributes: bool,
}

impl InnerIndexSettingsDiff {
    #[tracing::instrument(level = "trace", skip_all, target = "indexing::settings")]
    pub(crate) fn new(
        old_settings: InnerIndexSettings,
        new_settings: InnerIndexSettings,
        primary_key_id: Option<FieldId>,
        mut embedding_config_updates: BTreeMap<String, EmbedderAction>,
        settings_update_only: bool,
    ) -> Self {
        let only_additional_fields = match (
            &old_settings.user_defined_searchable_fields,
            &new_settings.user_defined_searchable_fields,
        ) {
            (None, None) | (Some(_), None) | (None, Some(_)) => None, // None means *
            (Some(old), Some(new)) => {
                let old: HashSet<_> = old.iter().cloned().collect();
                let new: HashSet<_> = new.iter().cloned().collect();
                if old.difference(&new).next().is_none() {
                    // if no field has been removed return only the additional ones
                    Some(&new - &old).filter(|x| !x.is_empty())
                } else {
                    None
                }
            }
        };

        let cache_reindex_searchable_without_user_defined = {
            old_settings.stop_words.as_ref().map(|set| set.as_fst().as_bytes())
                != new_settings.stop_words.as_ref().map(|set| set.as_fst().as_bytes())
                || old_settings.allowed_separators != new_settings.allowed_separators
                || old_settings.dictionary != new_settings.dictionary
                || old_settings.proximity_precision != new_settings.proximity_precision
                || old_settings.prefix_search != new_settings.prefix_search
                || old_settings.localized_searchable_fields_ids
                    != new_settings.localized_searchable_fields_ids
        };

        let cache_exact_attributes = old_settings.exact_attributes != new_settings.exact_attributes;

        let cache_user_defined_searchables = old_settings.user_defined_searchable_fields
            != new_settings.user_defined_searchable_fields;

        // if the user-defined searchables changed, then we need to reindex prompts.
        if cache_user_defined_searchables {
            for (embedder_name, (config, _, _quantized)) in
                new_settings.embedding_configs.inner_as_ref()
            {
                let was_quantized =
                    old_settings.embedding_configs.get(embedder_name).map_or(false, |conf| conf.2);
                // skip embedders that don't use document templates
                if !config.uses_document_template() {
                    continue;
                }

                // note: this could currently be entry.or_insert(..), but we're future-proofing with an explicit match
                // this always makes the code clearer by explicitly handling the cases
                match embedding_config_updates.entry(embedder_name.clone()) {
                    std::collections::btree_map::Entry::Vacant(entry) => {
                        entry.insert(EmbedderAction::with_reindex(
                            ReindexAction::RegeneratePrompts,
                            was_quantized,
                        ));
                    }
                    std::collections::btree_map::Entry::Occupied(entry) => {
                        let EmbedderAction {
                            was_quantized: _,
                            is_being_quantized: _,
                            write_back: _, // We are deleting this embedder, so no point in regeneration
                            reindex: _,    // We are already fully reindexing
                        } = entry.get();
                    }
                };
            }
        }

        InnerIndexSettingsDiff {
            old: old_settings,
            new: new_settings,
            primary_key_id,
            embedding_config_updates,
            settings_update_only,
            only_additional_fields,
            cache_reindex_searchable_without_user_defined,
            cache_user_defined_searchables,
            cache_exact_attributes,
        }
    }

    pub fn any_reindexing_needed(&self) -> bool {
        self.reindex_searchable() || self.reindex_facets() || self.reindex_vectors()
    }

    pub fn reindex_searchable(&self) -> bool {
        self.cache_reindex_searchable_without_user_defined
            || self.cache_exact_attributes
            || self.cache_user_defined_searchables
    }

    pub fn reindex_proximities(&self) -> bool {
        // if any searchable settings force the reindexing
        (self.cache_reindex_searchable_without_user_defined || self.cache_user_defined_searchables)
        // and if any settings needs the proximity database created
            && (self.old.proximity_precision == ProximityPrecision::ByAttribute
                || self.new.proximity_precision == ProximityPrecision::ByAttribute)
    }

    pub fn reindex_searchable_id(&self, id: FieldId) -> Option<DelAddOperation> {
        if self.cache_reindex_searchable_without_user_defined || self.cache_exact_attributes {
            Some(DelAddOperation::DeletionAndAddition)
        } else if let Some(only_additional_fields) = &self.only_additional_fields {
            let additional_field = self.new.fields_ids_map.name(id).unwrap();
            if only_additional_fields.contains(additional_field) {
                Some(DelAddOperation::Addition)
            } else {
                None
            }
        } else if self.cache_user_defined_searchables {
            Some(DelAddOperation::DeletionAndAddition)
        } else {
            None
        }
    }

    pub fn facet_fids_changed(&self) -> bool {
        let existing_fields = &self.new.existing_fields;
        if existing_fields.iter().any(|field| field.contains('.')) {
            return true;
        }

        let old_faceted_fields = &self.old.user_defined_faceted_fields;
        if old_faceted_fields.iter().any(|field| field.contains('.')) {
            return true;
        }

        // If there is new faceted fields we indicate that we must reindex as we must
        // index new fields as facets. It means that the distinct attribute,
        // an Asc/Desc criterion or a filtered attribute as be added or removed.
        let new_faceted_fields = &self.new.user_defined_faceted_fields;
        if new_faceted_fields.iter().any(|field| field.contains('.')) {
            return true;
        }

        (existing_fields - old_faceted_fields) != (existing_fields - new_faceted_fields)
    }

    pub fn global_facet_settings_changed(&self) -> bool {
        self.old.localized_faceted_fields_ids != self.new.localized_faceted_fields_ids
            || self.old.facet_search != self.new.facet_search
    }

    pub fn reindex_facets(&self) -> bool {
        self.facet_fids_changed() || self.global_facet_settings_changed()
    }

    pub fn reindex_vectors(&self) -> bool {
        !self.embedding_config_updates.is_empty()
    }

    pub fn settings_update_only(&self) -> bool {
        self.settings_update_only
    }

    pub fn run_geo_indexing(&self) -> bool {
        self.old.geo_fields_ids != self.new.geo_fields_ids
            || (!self.settings_update_only && self.new.geo_fields_ids.is_some())
    }

    pub fn modified_faceted_fields(&self) -> HashSet<String> {
        &self.old.user_defined_faceted_fields ^ &self.new.user_defined_faceted_fields
    }
}

#[derive(Clone)]
pub(crate) struct InnerIndexSettings {
    pub stop_words: Option<fst::Set<Vec<u8>>>,
    pub allowed_separators: Option<BTreeSet<String>>,
    pub dictionary: Option<BTreeSet<String>>,
    pub fields_ids_map: FieldsIdsMap,
    pub user_defined_faceted_fields: HashSet<String>,
    pub user_defined_searchable_fields: Option<Vec<String>>,
    pub faceted_fields_ids: HashSet<FieldId>,
    pub searchable_fields_ids: Vec<FieldId>,
    pub exact_attributes: HashSet<FieldId>,
    pub proximity_precision: ProximityPrecision,
    pub embedding_configs: EmbeddingConfigs,
    pub existing_fields: HashSet<String>,
    pub geo_fields_ids: Option<(FieldId, FieldId)>,
    pub non_searchable_fields_ids: Vec<FieldId>,
    pub non_faceted_fields_ids: Vec<FieldId>,
    pub localized_searchable_fields_ids: LocalizedFieldIds,
    pub localized_faceted_fields_ids: LocalizedFieldIds,
    pub prefix_search: PrefixSearch,
    pub facet_search: bool,
}

impl InnerIndexSettings {
    pub fn from_index(
        index: &Index,
        rtxn: &heed::RoTxn<'_>,
        embedding_configs: Option<EmbeddingConfigs>,
    ) -> Result<Self> {
        let stop_words = index.stop_words(rtxn)?;
        let stop_words = stop_words.map(|sw| sw.map_data(Vec::from).unwrap());
        let allowed_separators = index.allowed_separators(rtxn)?;
        let dictionary = index.dictionary(rtxn)?;
        let mut fields_ids_map = index.fields_ids_map(rtxn)?;
        let user_defined_searchable_fields = index.user_defined_searchable_fields(rtxn)?;
        let user_defined_searchable_fields =
            user_defined_searchable_fields.map(|sf| sf.into_iter().map(String::from).collect());
        let user_defined_faceted_fields = index.user_defined_faceted_fields(rtxn)?;
        let mut searchable_fields_ids = index.searchable_fields_ids(rtxn)?;
        let mut faceted_fields_ids = index.faceted_fields_ids(rtxn)?;
        let exact_attributes = index.exact_attributes_ids(rtxn)?;
        let proximity_precision = index.proximity_precision(rtxn)?.unwrap_or_default();
        let embedding_configs = match embedding_configs {
            Some(embedding_configs) => embedding_configs,
            None => embedders(index.embedding_configs(rtxn)?)?,
        };
        let prefix_search = index.prefix_search(rtxn)?.unwrap_or_default();
        let facet_search = index.facet_search(rtxn)?;
        let existing_fields: HashSet<_> = index
            .field_distribution(rtxn)?
            .into_iter()
            .filter_map(|(field, count)| (count != 0).then_some(field))
            .collect();
        // index.fields_ids_map($a)? ==>> fields_ids_map
        let geo_fields_ids = match fields_ids_map.id("_geo") {
            Some(gfid) => {
                let is_sortable = index.sortable_fields_ids(rtxn)?.contains(&gfid);
                let is_filterable = index.filterable_fields_ids(rtxn)?.contains(&gfid);
                // if `_geo` is faceted then we get the `lat` and `lng`
                if is_sortable || is_filterable {
                    let field_ids = fields_ids_map
                        .insert("_geo.lat")
                        .zip(fields_ids_map.insert("_geo.lng"))
                        .ok_or(UserError::AttributeLimitReached)?;
                    Some(field_ids)
                } else {
                    None
                }
            }
            None => None,
        };
        let localized_attributes_rules = index.localized_attributes_rules(rtxn)?;
        let localized_searchable_fields_ids = LocalizedFieldIds::new(
            &localized_attributes_rules,
            &fields_ids_map,
            searchable_fields_ids.iter().cloned(),
        );
        let localized_faceted_fields_ids = LocalizedFieldIds::new(
            &localized_attributes_rules,
            &fields_ids_map,
            faceted_fields_ids.iter().cloned(),
        );

        let vectors_fids = fields_ids_map.nested_ids(RESERVED_VECTORS_FIELD_NAME);
        searchable_fields_ids.retain(|id| !vectors_fids.contains(id));
        faceted_fields_ids.retain(|id| !vectors_fids.contains(id));

        Ok(Self {
            stop_words,
            allowed_separators,
            dictionary,
            fields_ids_map,
            user_defined_faceted_fields,
            user_defined_searchable_fields,
            faceted_fields_ids,
            searchable_fields_ids,
            exact_attributes,
            proximity_precision,
            embedding_configs,
            existing_fields,
            geo_fields_ids,
            non_searchable_fields_ids: vectors_fids.clone(),
            non_faceted_fields_ids: vectors_fids.clone(),
            localized_searchable_fields_ids,
            localized_faceted_fields_ids,
            prefix_search,
            facet_search,
        })
    }

    // find and insert the new field ids
    pub fn recompute_facets(&mut self, wtxn: &mut heed::RwTxn<'_>, index: &Index) -> Result<()> {
        let new_facets = self
            .fields_ids_map
            .iter()
            .filter(|(fid, _field)| !self.non_faceted_fields_ids.contains(fid))
            .filter(|(_fid, field)| crate::is_faceted(field, &self.user_defined_faceted_fields))
            .map(|(_fid, field)| field.to_string())
            .collect();
        index.put_faceted_fields(wtxn, &new_facets)?;

        self.faceted_fields_ids = index.faceted_fields_ids(wtxn)?;
        let localized_attributes_rules = index.localized_attributes_rules(wtxn)?;
        self.localized_faceted_fields_ids = LocalizedFieldIds::new(
            &localized_attributes_rules,
            &self.fields_ids_map,
            self.faceted_fields_ids.iter().cloned(),
        );
        Ok(())
    }

    // find and insert the new field ids
    pub fn recompute_searchables(
        &mut self,
        wtxn: &mut heed::RwTxn<'_>,
        index: &Index,
    ) -> Result<()> {
        let searchable_fields = self
            .user_defined_searchable_fields
            .as_ref()
            .map(|searchable| searchable.iter().map(|s| s.as_str()).collect::<Vec<_>>());

        // in case new fields were introduced we're going to recreate the searchable fields.
        if let Some(searchable_fields) = searchable_fields {
            index.put_all_searchable_fields_from_fields_ids_map(
                wtxn,
                &searchable_fields,
                &self.non_searchable_fields_ids,
                &self.fields_ids_map,
            )?;
        }
        self.searchable_fields_ids = index.searchable_fields_ids(wtxn)?;
        let localized_attributes_rules = index.localized_attributes_rules(wtxn)?;
        self.localized_searchable_fields_ids = LocalizedFieldIds::new(
            &localized_attributes_rules,
            &self.fields_ids_map,
            self.searchable_fields_ids.iter().cloned(),
        );

        Ok(())
    }
}

fn embedders(embedding_configs: Vec<IndexEmbeddingConfig>) -> Result<EmbeddingConfigs> {
    let res: Result<_> = embedding_configs
        .into_iter()
        .map(
            |IndexEmbeddingConfig {
                 name,
                 config: EmbeddingConfig { embedder_options, prompt, quantized },
                 ..
             }| {
                let prompt = Arc::new(prompt.try_into().map_err(crate::Error::from)?);

                let embedder = Arc::new(
                    Embedder::new(embedder_options.clone())
                        .map_err(crate::vector::Error::from)
                        .map_err(crate::Error::from)?,
                );
                Ok((name, (embedder, prompt, quantized.unwrap_or_default())))
            },
        )
        .collect();
    res.map(EmbeddingConfigs::new)
}

fn validate_prompt(
    name: &str,
    new: Setting<EmbeddingSettings>,
) -> Result<Setting<EmbeddingSettings>> {
    match new {
        Setting::Set(EmbeddingSettings {
            source,
            model,
            revision,
            api_key,
            dimensions,
            document_template: Setting::Set(template),
            document_template_max_bytes,
            url,
            request,
            response,
            distribution,
            headers,
            binary_quantized: binary_quantize,
        }) => {
            let max_bytes = match document_template_max_bytes.set() {
                Some(max_bytes) => NonZeroUsize::new(max_bytes).ok_or_else(|| {
                    crate::error::UserError::InvalidSettingsDocumentTemplateMaxBytes {
                        embedder_name: name.to_owned(),
                    }
                })?,
                None => default_max_bytes(),
            };

            // validate
            let template = crate::prompt::Prompt::new(
                template,
                // always specify a max_bytes
                Some(max_bytes),
            )
            .map(|prompt| crate::prompt::PromptData::from(prompt).template)
            .map_err(|inner| UserError::InvalidPromptForEmbeddings(name.to_owned(), inner))?;

            Ok(Setting::Set(EmbeddingSettings {
                source,
                model,
                revision,
                api_key,
                dimensions,
                document_template: Setting::Set(template),
                document_template_max_bytes,
                url,
                request,
                response,
                distribution,
                headers,
                binary_quantized: binary_quantize,
            }))
        }
        new => Ok(new),
    }
}

pub fn validate_embedding_settings(
    settings: Setting<EmbeddingSettings>,
    name: &str,
) -> Result<Setting<EmbeddingSettings>> {
    let settings = validate_prompt(name, settings)?;
    let Setting::Set(settings) = settings else { return Ok(settings) };
    let EmbeddingSettings {
        source,
        model,
        revision,
        api_key,
        dimensions,
        document_template,
        document_template_max_bytes,
        url,
        request,
        response,
        distribution,
        headers,
        binary_quantized: binary_quantize,
    } = settings;

    if let Some(0) = dimensions.set() {
        return Err(crate::error::UserError::InvalidSettingsDimensions {
            embedder_name: name.to_owned(),
        }
        .into());
    }

    if let Some(url) = url.as_ref().set() {
        url::Url::parse(url).map_err(|error| crate::error::UserError::InvalidUrl {
            embedder_name: name.to_owned(),
            inner_error: error,
            url: url.to_owned(),
        })?;
    }

    if let Some(request) = request.as_ref().set() {
        let request = crate::vector::rest::Request::new(request.to_owned())
            .map_err(|error| crate::UserError::VectorEmbeddingError(error.into()))?;
        if let Some(response) = response.as_ref().set() {
            crate::vector::rest::Response::new(response.to_owned(), &request)
                .map_err(|error| crate::UserError::VectorEmbeddingError(error.into()))?;
        }
    }

    let Some(inferred_source) = source.set() else {
        return Ok(Setting::Set(EmbeddingSettings {
            source,
            model,
            revision,
            api_key,
            dimensions,
            document_template,
            document_template_max_bytes,
            url,
            request,
            response,
            distribution,
            headers,
            binary_quantized: binary_quantize,
        }));
    };
    match inferred_source {
        EmbedderSource::OpenAi => {
            check_unset(&revision, EmbeddingSettings::REVISION, inferred_source, name)?;

            check_unset(&request, EmbeddingSettings::REQUEST, inferred_source, name)?;
            check_unset(&response, EmbeddingSettings::RESPONSE, inferred_source, name)?;
            check_unset(&headers, EmbeddingSettings::HEADERS, inferred_source, name)?;

            if let Setting::Set(model) = &model {
                let model = crate::vector::openai::EmbeddingModel::from_name(model.as_str())
                    .ok_or(crate::error::UserError::InvalidOpenAiModel {
                        embedder_name: name.to_owned(),
                        model: model.clone(),
                    })?;
                if let Setting::Set(dimensions) = dimensions {
                    if !model.supports_overriding_dimensions()
                        && dimensions != model.default_dimensions()
                    {
                        return Err(crate::error::UserError::InvalidOpenAiModelDimensions {
                            embedder_name: name.to_owned(),
                            model: model.name(),
                            dimensions,
                            expected_dimensions: model.default_dimensions(),
                        }
                        .into());
                    }
                    if dimensions > model.default_dimensions() {
                        return Err(crate::error::UserError::InvalidOpenAiModelDimensionsMax {
                            embedder_name: name.to_owned(),
                            model: model.name(),
                            dimensions,
                            max_dimensions: model.default_dimensions(),
                        }
                        .into());
                    }
                }
            }
        }
        EmbedderSource::Ollama => {
            check_set(&model, EmbeddingSettings::MODEL, inferred_source, name)?;
            check_unset(&revision, EmbeddingSettings::REVISION, inferred_source, name)?;

            check_unset(&request, EmbeddingSettings::REQUEST, inferred_source, name)?;
            check_unset(&response, EmbeddingSettings::RESPONSE, inferred_source, name)?;
            check_unset(&headers, EmbeddingSettings::HEADERS, inferred_source, name)?;
        }
        EmbedderSource::HuggingFace => {
            check_unset(&api_key, EmbeddingSettings::API_KEY, inferred_source, name)?;
            check_unset(&dimensions, EmbeddingSettings::DIMENSIONS, inferred_source, name)?;

            check_unset(&url, EmbeddingSettings::URL, inferred_source, name)?;
            check_unset(&request, EmbeddingSettings::REQUEST, inferred_source, name)?;
            check_unset(&response, EmbeddingSettings::RESPONSE, inferred_source, name)?;
            check_unset(&headers, EmbeddingSettings::HEADERS, inferred_source, name)?;
        }
        EmbedderSource::UserProvided => {
            check_unset(&model, EmbeddingSettings::MODEL, inferred_source, name)?;
            check_unset(&revision, EmbeddingSettings::REVISION, inferred_source, name)?;
            check_unset(&api_key, EmbeddingSettings::API_KEY, inferred_source, name)?;
            check_unset(
                &document_template,
                EmbeddingSettings::DOCUMENT_TEMPLATE,
                inferred_source,
                name,
            )?;
            check_unset(
                &document_template_max_bytes,
                EmbeddingSettings::DOCUMENT_TEMPLATE_MAX_BYTES,
                inferred_source,
                name,
            )?;
            check_set(&dimensions, EmbeddingSettings::DIMENSIONS, inferred_source, name)?;

            check_unset(&url, EmbeddingSettings::URL, inferred_source, name)?;
            check_unset(&request, EmbeddingSettings::REQUEST, inferred_source, name)?;
            check_unset(&response, EmbeddingSettings::RESPONSE, inferred_source, name)?;
            check_unset(&headers, EmbeddingSettings::HEADERS, inferred_source, name)?;
        }
        EmbedderSource::Rest => {
            check_unset(&model, EmbeddingSettings::MODEL, inferred_source, name)?;
            check_unset(&revision, EmbeddingSettings::REVISION, inferred_source, name)?;
            check_set(&url, EmbeddingSettings::URL, inferred_source, name)?;
            check_set(&request, EmbeddingSettings::REQUEST, inferred_source, name)?;
            check_set(&response, EmbeddingSettings::RESPONSE, inferred_source, name)?;
        }
    }
    Ok(Setting::Set(EmbeddingSettings {
        source,
        model,
        revision,
        api_key,
        dimensions,
        document_template,
        document_template_max_bytes,
        url,
        request,
        response,
        distribution,
        headers,
        binary_quantized: binary_quantize,
    }))
}

#[cfg(test)]
mod tests {
    use big_s::S;
    use heed::types::Bytes;
    use maplit::{btreemap, btreeset, hashset};
    use meili_snap::snapshot;

    use super::*;
    use crate::error::Error;
    use crate::index::tests::TempIndex;
    use crate::update::ClearDocuments;
    use crate::{db_snap, Criterion, Filter, SearchResult};

    #[test]
    fn set_and_reset_searchable_fields() {
        let index = TempIndex::new();

        // First we send 3 documents with ids from 1 to 3.
        let mut wtxn = index.write_txn().unwrap();

        index
            .add_documents_using_wtxn(
                &mut wtxn,
                documents!([
                    { "id": 1, "name": "kevin", "age": 23 },
                    { "id": 2, "name": "kevina", "age": 21},
                    { "id": 3, "name": "benoit", "age": 34 }
                ]),
            )
            .unwrap();

        // We change the searchable fields to be the "name" field only.
        index
            .update_settings_using_wtxn(&mut wtxn, |settings| {
                settings.set_searchable_fields(vec!["name".into()]);
            })
            .unwrap();

        wtxn.commit().unwrap();

        db_snap!(index, fields_ids_map, @r###"
        0   id               |
        1   name             |
        2   age              |
        "###);
        db_snap!(index, searchable_fields, @r###"["name"]"###);
        db_snap!(index, fieldids_weights_map, @r###"
        fid weight
        1   0   |
        "###);

        // Check that the searchable field is correctly set to "name" only.
        let rtxn = index.read_txn().unwrap();
        let mut buffer = Vec::new();
        let dictionary = index.document_decompression_dictionary(&rtxn).unwrap();
        // When we search for something that is not in
        // the searchable fields it must not return any document.
        let result = index.search(&rtxn).query("23").execute().unwrap();
        assert_eq!(result.documents_ids, Vec::<u32>::new());

        // When we search for something that is in the searchable fields
        // we must find the appropriate document.
        let result = index.search(&rtxn).query(r#""kevin""#).execute().unwrap();
        let mut compressed_documents =
            index.compressed_documents(&rtxn, result.documents_ids).unwrap();
        let fid_map = index.fields_ids_map(&rtxn).unwrap();
        assert_eq!(compressed_documents.len(), 1);
        let (_id, compressed_document) = compressed_documents.remove(0);
        let document = compressed_document
            .decompress_with_optional_dictionary(&mut buffer, dictionary.as_ref())
            .unwrap();

        assert_eq!(document.get(fid_map.id("name").unwrap()), Some(&br#""kevin""#[..]));
        drop(dictionary);
        drop(rtxn);

        // We change the searchable fields to be the "name" field only.
        index
            .update_settings(|settings| {
                settings.reset_searchable_fields();
            })
            .unwrap();

        db_snap!(index, fields_ids_map, @r###"
        0   id               |
        1   name             |
        2   age              |
        "###);
        db_snap!(index, searchable_fields, @r###"["id", "name", "age"]"###);
        db_snap!(index, fieldids_weights_map, @r###"
        fid weight
        0   0   |
        1   0   |
        2   0   |
        "###);

        // Check that the searchable field have been reset and documents are found now.
        let rtxn = index.read_txn().unwrap();
        let dictionary = index.document_decompression_dictionary(&rtxn).unwrap();
        let fid_map = index.fields_ids_map(&rtxn).unwrap();
        let user_defined_searchable_fields = index.user_defined_searchable_fields(&rtxn).unwrap();
        snapshot!(format!("{user_defined_searchable_fields:?}"), @"None");
        // the searchable fields should contain all the fields
        let searchable_fields = index.searchable_fields(&rtxn).unwrap();
        snapshot!(format!("{searchable_fields:?}"), @r###"["id", "name", "age"]"###);
        let result = index.search(&rtxn).query("23").execute().unwrap();
        assert_eq!(result.documents_ids.len(), 1);
        let mut compressed_documents =
            index.compressed_documents(&rtxn, result.documents_ids).unwrap();
        let (_id, compressed_document) = compressed_documents.remove(0);
        let document = compressed_document
            .decompress_with_optional_dictionary(&mut buffer, dictionary.as_ref())
            .unwrap();
        assert_eq!(document.get(fid_map.id("name").unwrap()), Some(&br#""kevin""#[..]));
    }

    #[test]
    fn mixup_searchable_with_displayed_fields() {
        let index = TempIndex::new();

        let mut wtxn = index.write_txn().unwrap();
        // First we send 3 documents with ids from 1 to 3.
        index
            .add_documents_using_wtxn(
                &mut wtxn,
                documents!([
                    { "id": 0, "name": "kevin", "age": 23},
                    { "id": 1, "name": "kevina", "age": 21 },
                    { "id": 2, "name": "benoit", "age": 34 }
                ]),
            )
            .unwrap();

        // In the same transaction we change the displayed fields to be only the "age".
        // We also change the searchable fields to be the "name" field only.
        index
            .update_settings_using_wtxn(&mut wtxn, |settings| {
                settings.set_displayed_fields(vec!["age".into()]);
                settings.set_searchable_fields(vec!["name".into()]);
            })
            .unwrap();
        wtxn.commit().unwrap();

        // Check that the displayed fields are correctly set to `None` (default value).
        let rtxn = index.read_txn().unwrap();
        let fields_ids = index.displayed_fields(&rtxn).unwrap();
        assert_eq!(fields_ids.unwrap(), (&["age"][..]));
        drop(rtxn);

        // We change the searchable fields to be the "name" field only.
        index
            .update_settings(|settings| {
                settings.reset_searchable_fields();
            })
            .unwrap();

        // Check that the displayed fields always contains only the "age" field.
        let rtxn = index.read_txn().unwrap();
        let fields_ids = index.displayed_fields(&rtxn).unwrap();
        assert_eq!(fields_ids.unwrap(), &["age"][..]);
    }

    #[test]
    fn default_displayed_fields() {
        let index = TempIndex::new();

        // First we send 3 documents with ids from 1 to 3.
        index
            .add_documents(documents!([
                { "id": 0, "name": "kevin", "age": 23},
                { "id": 1, "name": "kevina", "age": 21 },
                { "id": 2, "name": "benoit", "age": 34 }
            ]))
            .unwrap();

        // Check that the displayed fields are correctly set to `None` (default value).
        let rtxn = index.read_txn().unwrap();
        let fields_ids = index.displayed_fields(&rtxn).unwrap();
        assert_eq!(fields_ids, None);
    }

    #[test]
    fn set_and_reset_displayed_field() {
        let index = TempIndex::new();

        let mut wtxn = index.write_txn().unwrap();
        index
            .add_documents_using_wtxn(
                &mut wtxn,
                documents!([
                    { "id": 0, "name": "kevin", "age": 23},
                    { "id": 1, "name": "kevina", "age": 21 },
                    { "id": 2, "name": "benoit", "age": 34 }
                ]),
            )
            .unwrap();
        index
            .update_settings_using_wtxn(&mut wtxn, |settings| {
                settings.set_displayed_fields(vec!["age".into()]);
            })
            .unwrap();
        wtxn.commit().unwrap();

        // Check that the displayed fields are correctly set to only the "age" field.
        let rtxn = index.read_txn().unwrap();
        let fields_ids = index.displayed_fields(&rtxn).unwrap();
        assert_eq!(fields_ids.unwrap(), &["age"][..]);
        drop(rtxn);

        // We reset the fields ids to become `None`, the default value.
        index
            .update_settings(|settings| {
                settings.reset_displayed_fields();
            })
            .unwrap();

        // Check that the displayed fields are correctly set to `None` (default value).
        let rtxn = index.read_txn().unwrap();
        let fields_ids = index.displayed_fields(&rtxn).unwrap();
        assert_eq!(fields_ids, None);
    }

    #[test]
    fn set_filterable_fields() {
        let index = TempIndex::new();

        // Set the filterable fields to be the age.
        index
            .update_settings(|settings| {
                settings.set_filterable_fields(hashset! { S("age") });
            })
            .unwrap();

        // Then index some documents.
        index
            .add_documents(documents!([
                { "id": 0, "name": "kevin", "age": 23},
                { "id": 1, "name": "kevina", "age": 21 },
                { "id": 2, "name": "benoit", "age": 34 }
            ]))
            .unwrap();

        // Check that the displayed fields are correctly set.
        let rtxn = index.read_txn().unwrap();
        let mut buffer = Vec::new();
        let dictionary = index.document_decompression_dictionary(&rtxn).unwrap();
        let fields_ids = index.filterable_fields(&rtxn).unwrap();
        assert_eq!(fields_ids, hashset! { S("age") });
        // Only count the field_id 0 and level 0 facet values.
        // TODO we must support typed CSVs for numbers to be understood.
        let fidmap = index.fields_ids_map(&rtxn).unwrap();
        for result in index.all_compressed_documents(&rtxn).unwrap() {
            let (_id, compressed_document) = result.unwrap();
            let document = compressed_document
                .decompress_with_optional_dictionary(&mut buffer, dictionary.as_ref())
                .unwrap();
            let json =
                crate::obkv_to_json(&fidmap.ids().collect::<Vec<_>>(), &fidmap, document).unwrap();
            println!("json: {:?}", json);
        }
        let count = index
            .facet_id_f64_docids
            .remap_key_type::<Bytes>()
            // The faceted field id is 2u16
            .prefix_iter(&rtxn, &[0, 2, 0])
            .unwrap()
            .count();
        assert_eq!(count, 3);
        drop(dictionary);
        drop(rtxn);

        // Index a little more documents with new and current facets values.
        index
            .add_documents(documents!([
                { "id": 3, "name": "kevin2", "age": 23},
                { "id": 4, "name": "kevina2", "age": 21 },
                { "id": 5, "name": "benoit", "age": 35 }
            ]))
            .unwrap();

        let rtxn = index.read_txn().unwrap();
        // Only count the field_id 0 and level 0 facet values.
        let count = index
            .facet_id_f64_docids
            .remap_key_type::<Bytes>()
            .prefix_iter(&rtxn, &[0, 2, 0])
            .unwrap()
            .count();
        assert_eq!(count, 4);

        // Set the filterable fields to be the age and the name.
        index
            .update_settings(|settings| {
                settings.set_filterable_fields(hashset! { S("age"),  S("name") });
            })
            .unwrap();

        // Check that the displayed fields are correctly set.
        let rtxn = index.read_txn().unwrap();
        let fields_ids = index.filterable_fields(&rtxn).unwrap();
        assert_eq!(fields_ids, hashset! { S("age"),  S("name") });

        let rtxn = index.read_txn().unwrap();
        // Only count the field_id 2 and level 0 facet values.
        let count = index
            .facet_id_f64_docids
            .remap_key_type::<Bytes>()
            .prefix_iter(&rtxn, &[0, 2, 0])
            .unwrap()
            .count();
        assert_eq!(count, 4);

        let rtxn = index.read_txn().unwrap();
        // Only count the field_id 1 and level 0 facet values.
        let count = index
            .facet_id_string_docids
            .remap_key_type::<Bytes>()
            .prefix_iter(&rtxn, &[0, 1])
            .unwrap()
            .count();
        assert_eq!(count, 5);

        // Remove the age from the filterable fields.
        index
            .update_settings(|settings| {
                settings.set_filterable_fields(hashset! { S("name") });
            })
            .unwrap();

        // Check that the displayed fields are correctly set.
        let rtxn = index.read_txn().unwrap();
        let fields_ids = index.filterable_fields(&rtxn).unwrap();
        assert_eq!(fields_ids, hashset! { S("name") });

        let rtxn = index.read_txn().unwrap();
        // Only count the field_id 2 and level 0 facet values.
        let count = index
            .facet_id_f64_docids
            .remap_key_type::<Bytes>()
            .prefix_iter(&rtxn, &[0, 2, 0])
            .unwrap()
            .count();
        assert_eq!(count, 0);

        let rtxn = index.read_txn().unwrap();
        // Only count the field_id 1 and level 0 facet values.
        let count = index
            .facet_id_string_docids
            .remap_key_type::<Bytes>()
            .prefix_iter(&rtxn, &[0, 1])
            .unwrap()
            .count();
        assert_eq!(count, 5);
    }

    #[test]
    fn set_asc_desc_field() {
        let index = TempIndex::new();
        let mut buffer = Vec::new();

        // Set the filterable fields to be the age.
        index
            .update_settings(|settings| {
                settings.set_displayed_fields(vec![S("name")]);
                settings.set_criteria(vec![Criterion::Asc("age".to_owned())]);
            })
            .unwrap();

        // Then index some documents.
        index
            .add_documents(documents!([
                { "id": 0, "name": "kevin", "age": 23},
                { "id": 1, "name": "kevina", "age": 21 },
                { "id": 2, "name": "benoit", "age": 34 }
            ]))
            .unwrap();

        // Run an empty query just to ensure that the search results are ordered.
        let rtxn = index.read_txn().unwrap();
        let dictionary = index.document_decompression_dictionary(&rtxn).unwrap();
        let SearchResult { documents_ids, .. } = index.search(&rtxn).execute().unwrap();
        let compressed_documents = index.compressed_documents(&rtxn, documents_ids).unwrap();

        // Fetch the documents "age" field in the ordre in which the documents appear.
        let age_field_id = index.fields_ids_map(&rtxn).unwrap().id("age").unwrap();
        let iter = compressed_documents.into_iter().map(|(_, compressed_doc)| {
            let doc = compressed_doc
                .decompress_with_optional_dictionary(&mut buffer, dictionary.as_ref())
                .unwrap();
            let bytes = doc.get(age_field_id).unwrap();
            let string = std::str::from_utf8(bytes).unwrap();
            string.parse::<u32>().unwrap()
        });

        assert_eq!(iter.collect::<Vec<_>>(), vec![21, 23, 34]);
    }

    #[test]
    fn set_distinct_field() {
        let index = TempIndex::new();

        // Set the filterable fields to be the age.
        index
            .update_settings(|settings| {
                // Don't display the generated `id` field.
                settings.set_displayed_fields(vec![S("name"), S("age")]);
                settings.set_distinct_field(S("age"));
            })
            .unwrap();

        // Then index some documents.
        index
            .add_documents(documents!([
                { "id": 0, "name": "kevin",  "age": 23 },
                { "id": 1, "name": "kevina", "age": 21 },
                { "id": 2, "name": "benoit", "age": 34 },
                { "id": 3, "name": "bernard", "age": 34 },
                { "id": 4, "name": "bertrand", "age": 34 },
                { "id": 5, "name": "bernie", "age": 34 },
                { "id": 6, "name": "ben", "age": 34 }
            ]))
            .unwrap();

        // Run an empty query just to ensure that the search results are ordered.
        let rtxn = index.read_txn().unwrap();
        let SearchResult { documents_ids, .. } = index.search(&rtxn).execute().unwrap();

        // There must be at least one document with a 34 as the age.
        assert_eq!(documents_ids.len(), 3);
    }

    #[test]
    fn set_nested_distinct_field() {
        let index = TempIndex::new();

        // Set the filterable fields to be the age.
        index
            .update_settings(|settings| {
                // Don't display the generated `id` field.
                settings.set_displayed_fields(vec![S("person")]);
                settings.set_distinct_field(S("person.age"));
            })
            .unwrap();

        // Then index some documents.
        index
            .add_documents(documents!([
                { "id": 0, "person": { "name": "kevin", "age": 23 }},
                { "id": 1, "person": { "name": "kevina", "age": 21 }},
                { "id": 2, "person": { "name": "benoit", "age": 34 }},
                { "id": 3, "person": { "name": "bernard", "age": 34 }},
                { "id": 4, "person": { "name": "bertrand", "age": 34 }},
                { "id": 5, "person": { "name": "bernie", "age": 34 }},
                { "id": 6, "person": { "name": "ben", "age": 34 }}
            ]))
            .unwrap();

        // Run an empty query just to ensure that the search results are ordered.
        let rtxn = index.read_txn().unwrap();
        let SearchResult { documents_ids, .. } = index.search(&rtxn).execute().unwrap();

        // There must be at least one document with a 34 as the age.
        assert_eq!(documents_ids.len(), 3);
    }

    #[test]
    fn default_stop_words() {
        let index = TempIndex::new();

        // First we send 3 documents with ids from 1 to 3.
        index
            .add_documents(documents!([
                { "id": 0, "name": "kevin", "age": 23},
                { "id": 1, "name": "kevina", "age": 21 },
                { "id": 2, "name": "benoit", "age": 34 }
            ]))
            .unwrap();

        // Ensure there is no stop_words by default
        let rtxn = index.read_txn().unwrap();
        let stop_words = index.stop_words(&rtxn).unwrap();
        assert!(stop_words.is_none());
    }

    #[test]
    fn set_and_reset_stop_words() {
        let index = TempIndex::new();

        let mut wtxn = index.write_txn().unwrap();
        // First we send 3 documents with ids from 1 to 3.
        index
            .add_documents_using_wtxn(
                &mut wtxn,
                documents!([
                    { "id": 0, "name": "kevin", "age": 23, "maxim": "I love dogs" },
                    { "id": 1, "name": "kevina", "age": 21, "maxim": "Doggos are the best" },
                    { "id": 2, "name": "benoit", "age": 34, "maxim": "The crepes are really good" },
                ]),
            )
            .unwrap();

        // In the same transaction we provide some stop_words
        let set = btreeset! { "i".to_string(), "the".to_string(), "are".to_string() };
        index
            .update_settings_using_wtxn(&mut wtxn, |settings| {
                settings.set_stop_words(set.clone());
            })
            .unwrap();

        wtxn.commit().unwrap();

        // Ensure stop_words are effectively stored
        let rtxn = index.read_txn().unwrap();
        let stop_words = index.stop_words(&rtxn).unwrap();
        assert!(stop_words.is_some()); // at this point the index should return something

        let stop_words = stop_words.unwrap();
        let expected = fst::Set::from_iter(&set).unwrap();
        assert_eq!(stop_words.as_fst().as_bytes(), expected.as_fst().as_bytes());

        // when we search for something that is a non prefix stop_words it should be ignored
        // thus we should get a placeholder search (all the results = 3)
        let result = index.search(&rtxn).query("the ").execute().unwrap();
        assert_eq!(result.documents_ids.len(), 3);
        let result = index.search(&rtxn).query("i ").execute().unwrap();
        assert_eq!(result.documents_ids.len(), 3);
        let result = index.search(&rtxn).query("are ").execute().unwrap();
        assert_eq!(result.documents_ids.len(), 3);

        let result = index.search(&rtxn).query("dog").execute().unwrap();
        assert_eq!(result.documents_ids.len(), 2); // we have two maxims talking about doggos
        let result = index.search(&rtxn).query("benoît").execute().unwrap();
        assert_eq!(result.documents_ids.len(), 1); // there is one benoit in our data

        // now we'll reset the stop_words and ensure it's None
        index
            .update_settings(|settings| {
                settings.reset_stop_words();
            })
            .unwrap();

        let rtxn = index.read_txn().unwrap();
        let stop_words = index.stop_words(&rtxn).unwrap();
        assert!(stop_words.is_none());

        // now we can search for the stop words
        let result = index.search(&rtxn).query("the").execute().unwrap();
        assert_eq!(result.documents_ids.len(), 2);
        let result = index.search(&rtxn).query("i").execute().unwrap();
        assert_eq!(result.documents_ids.len(), 1);
        let result = index.search(&rtxn).query("are").execute().unwrap();
        assert_eq!(result.documents_ids.len(), 2);

        // the rest of the search is still not impacted
        let result = index.search(&rtxn).query("dog").execute().unwrap();
        assert_eq!(result.documents_ids.len(), 2); // we have two maxims talking about doggos
        let result = index.search(&rtxn).query("benoît").execute().unwrap();
        assert_eq!(result.documents_ids.len(), 1); // there is one benoit in our data
    }

    #[test]
    fn set_and_reset_synonyms() {
        let index = TempIndex::new();

        let mut wtxn = index.write_txn().unwrap();
        // Send 3 documents with ids from 1 to 3.
        index
            .add_documents_using_wtxn(
                &mut wtxn,
                documents!([
                    { "id": 0, "name": "kevin", "age": 23, "maxim": "I love dogs"},
                    { "id": 1, "name": "kevina", "age": 21, "maxim": "Doggos are the best"},
                    { "id": 2, "name": "benoit", "age": 34, "maxim": "The crepes are really good"},
                ]),
            )
            .unwrap();

        // In the same transaction provide some synonyms
        index
            .update_settings_using_wtxn(&mut wtxn, |settings| {
                settings.set_synonyms(btreemap! {
                    "blini".to_string() => vec!["crepes".to_string()],
                    "super like".to_string() => vec!["love".to_string()],
                    "puppies".to_string() => vec!["dogs".to_string(), "doggos".to_string()]
                });
            })
            .unwrap();
        wtxn.commit().unwrap();

        // Ensure synonyms are effectively stored
        let rtxn = index.read_txn().unwrap();
        let synonyms = index.synonyms(&rtxn).unwrap();
        assert!(!synonyms.is_empty()); // at this point the index should return something

        // Check that we can use synonyms
        let result = index.search(&rtxn).query("blini").execute().unwrap();
        assert_eq!(result.documents_ids.len(), 1);
        let result = index.search(&rtxn).query("super like").execute().unwrap();
        assert_eq!(result.documents_ids.len(), 1);
        let result = index.search(&rtxn).query("puppies").execute().unwrap();
        assert_eq!(result.documents_ids.len(), 2);

        // Reset the synonyms
        index
            .update_settings(|settings| {
                settings.reset_synonyms();
            })
            .unwrap();

        // Ensure synonyms are reset
        let rtxn = index.read_txn().unwrap();
        let synonyms = index.synonyms(&rtxn).unwrap();
        assert!(synonyms.is_empty());

        // Check that synonyms are no longer work
        let result = index.search(&rtxn).query("blini").execute().unwrap();
        assert!(result.documents_ids.is_empty());
        let result = index.search(&rtxn).query("super like").execute().unwrap();
        assert!(result.documents_ids.is_empty());
        let result = index.search(&rtxn).query("puppies").execute().unwrap();
        assert!(result.documents_ids.is_empty());
    }

    #[test]
    fn thai_synonyms() {
        let index = TempIndex::new();

        let mut wtxn = index.write_txn().unwrap();
        // Send 3 documents with ids from 1 to 3.
        index
            .add_documents_using_wtxn(
                &mut wtxn,
                documents!([
                    { "id": 0, "name": "ยี่ปุ่น" },
                    { "id": 1, "name": "ญี่ปุ่น" },
                ]),
            )
            .unwrap();

        // In the same transaction provide some synonyms
        index
            .update_settings_using_wtxn(&mut wtxn, |settings| {
                settings.set_synonyms(btreemap! {
                    "japanese".to_string() => vec![S("ญี่ปุ่น"), S("ยี่ปุ่น")],
                });
            })
            .unwrap();
        wtxn.commit().unwrap();

        // Ensure synonyms are effectively stored
        let rtxn = index.read_txn().unwrap();
        let synonyms = index.synonyms(&rtxn).unwrap();
        assert!(!synonyms.is_empty()); // at this point the index should return something

        // Check that we can use synonyms
        let result = index.search(&rtxn).query("japanese").execute().unwrap();
        assert_eq!(result.documents_ids.len(), 2);
    }

    #[test]
    fn setting_searchable_recomputes_other_settings() {
        let index = TempIndex::new();

        // Set all the settings except searchable
        index
            .update_settings(|settings| {
                settings.set_displayed_fields(vec!["hello".to_string()]);
                settings.set_filterable_fields(hashset! { S("age"), S("toto") });
                settings.set_criteria(vec![Criterion::Asc(S("toto"))]);
            })
            .unwrap();

        // check the output
        let rtxn = index.read_txn().unwrap();
        assert_eq!(&["hello"][..], index.displayed_fields(&rtxn).unwrap().unwrap());
        // since no documents have been pushed the primary key is still unset
        assert!(index.primary_key(&rtxn).unwrap().is_none());
        assert_eq!(vec![Criterion::Asc("toto".to_string())], index.criteria(&rtxn).unwrap());
        drop(rtxn);

        // We set toto and age as searchable to force reordering of the fields
        index
            .update_settings(|settings| {
                settings.set_searchable_fields(vec!["toto".to_string(), "age".to_string()]);
            })
            .unwrap();

        let rtxn = index.read_txn().unwrap();
        assert_eq!(&["hello"][..], index.displayed_fields(&rtxn).unwrap().unwrap());
        assert!(index.primary_key(&rtxn).unwrap().is_none());
        assert_eq!(vec![Criterion::Asc("toto".to_string())], index.criteria(&rtxn).unwrap());
    }

    #[test]
    fn setting_not_filterable_cant_filter() {
        let index = TempIndex::new();

        // Set all the settings except searchable
        index
            .update_settings(|settings| {
                settings.set_displayed_fields(vec!["hello".to_string()]);
                // It is only Asc(toto), there is a facet database but it is denied to filter with toto.
                settings.set_criteria(vec![Criterion::Asc(S("toto"))]);
            })
            .unwrap();

        let rtxn = index.read_txn().unwrap();
        let filter = Filter::from_str("toto = 32").unwrap().unwrap();
        let _ = filter.evaluate(&rtxn, &index).unwrap_err();
    }

    #[test]
    fn setting_primary_key() {
        let index = TempIndex::new();

        let mut wtxn = index.write_txn().unwrap();
        // Set the primary key settings
        index
            .update_settings_using_wtxn(&mut wtxn, |settings| {
                settings.set_primary_key(S("mykey"));
            })
            .unwrap();
        wtxn.commit().unwrap();
        let mut wtxn = index.write_txn().unwrap();
        assert_eq!(index.primary_key(&wtxn).unwrap(), Some("mykey"));

        // Then index some documents with the "mykey" primary key.
        index
            .add_documents_using_wtxn(
                &mut wtxn,
                documents!([
                    { "mykey": 1, "name": "kevin",  "age": 23 },
                    { "mykey": 2, "name": "kevina", "age": 21 },
                    { "mykey": 3, "name": "benoit", "age": 34 },
                    { "mykey": 4, "name": "bernard", "age": 34 },
                    { "mykey": 5, "name": "bertrand", "age": 34 },
                    { "mykey": 6, "name": "bernie", "age": 34 },
                    { "mykey": 7, "name": "ben", "age": 34 }
                ]),
            )
            .unwrap();
        wtxn.commit().unwrap();

        // Updating settings with the same primary key should do nothing
        let mut wtxn = index.write_txn().unwrap();
        index
            .update_settings_using_wtxn(&mut wtxn, |settings| {
                settings.set_primary_key(S("mykey"));
            })
            .unwrap();
        assert_eq!(index.primary_key(&wtxn).unwrap(), Some("mykey"));
        wtxn.commit().unwrap();

        // Updating the settings with a different (or no) primary key causes an error
        let mut wtxn = index.write_txn().unwrap();
        let error = index
            .update_settings_using_wtxn(&mut wtxn, |settings| {
                settings.reset_primary_key();
            })
            .unwrap_err();
        assert!(matches!(error, Error::UserError(UserError::PrimaryKeyCannotBeChanged(_))));
        wtxn.abort();

        // But if we clear the database...
        let mut wtxn = index.write_txn().unwrap();
        let builder = ClearDocuments::new(&mut wtxn, &index);
        builder.execute().unwrap();
        wtxn.commit().unwrap();

        // ...we can change the primary key
        index
            .update_settings(|settings| {
                settings.set_primary_key(S("myid"));
            })
            .unwrap();
    }

    #[test]
    fn setting_impact_relevancy() {
        let index = TempIndex::new();
        let mut buffer = Vec::new();

        // Set the genres setting
        index
            .update_settings(|settings| {
                settings.set_filterable_fields(hashset! { S("genres") });
            })
            .unwrap();

        index.add_documents(documents!([
          {
            "id": 11,
            "title": "Star Wars",
            "overview":
              "Princess Leia is captured and held hostage by the evil Imperial forces in their effort to take over the galactic Empire. Venturesome Luke Skywalker and dashing captain Han Solo team together with the loveable robot duo R2-D2 and C-3PO to rescue the beautiful princess and restore peace and justice in the Empire.",
            "genres": ["Adventure", "Action", "Science Fiction"],
            "poster": "https://image.tmdb.org/t/p/w500/6FfCtAuVAW8XJjZ7eWeLibRLWTw.jpg",
            "release_date": 233366400
          },
          {
            "id": 30,
            "title": "Magnetic Rose",
            "overview": "",
            "genres": ["Animation", "Science Fiction"],
            "poster": "https://image.tmdb.org/t/p/w500/gSuHDeWemA1menrwfMRChnSmMVN.jpg",
            "release_date": 819676800
          }
        ])).unwrap();

        let rtxn = index.read_txn().unwrap();
        let SearchResult { documents_ids, .. } = index.search(&rtxn).query("S").execute().unwrap();
        let first_id = documents_ids[0];
        let documents = index.compressed_documents(&rtxn, documents_ids).unwrap();
        let (_, compressed_content) = documents.iter().find(|(id, _)| *id == first_id).unwrap();
        let dictionary = index.document_decompression_dictionary(&rtxn).unwrap();
        let content = compressed_content
            .decompress_with_optional_dictionary(&mut buffer, dictionary.as_ref())
            .unwrap();

        let fid = index.fields_ids_map(&rtxn).unwrap().id("title").unwrap();
        let line = std::str::from_utf8(content.get(fid).unwrap()).unwrap();
        assert_eq!(line, r#""Star Wars""#);
    }

    #[test]
    fn test_disable_typo() {
        let index = TempIndex::new();

        let mut txn = index.write_txn().unwrap();
        assert!(index.authorize_typos(&txn).unwrap());

        index
            .update_settings_using_wtxn(&mut txn, |settings| {
                settings.set_autorize_typos(false);
            })
            .unwrap();

        assert!(!index.authorize_typos(&txn).unwrap());
    }

    #[test]
    fn update_min_word_len_for_typo() {
        let index = TempIndex::new();

        // Set the genres setting
        index
            .update_settings(|settings| {
                settings.set_min_word_len_one_typo(8);
                settings.set_min_word_len_two_typos(8);
            })
            .unwrap();

        let txn = index.read_txn().unwrap();
        assert_eq!(index.min_word_len_one_typo(&txn).unwrap(), 8);
        assert_eq!(index.min_word_len_two_typos(&txn).unwrap(), 8);

        index
            .update_settings(|settings| {
                settings.reset_min_word_len_one_typo();
                settings.reset_min_word_len_two_typos();
            })
            .unwrap();

        let txn = index.read_txn().unwrap();
        assert_eq!(index.min_word_len_one_typo(&txn).unwrap(), DEFAULT_MIN_WORD_LEN_ONE_TYPO);
        assert_eq!(index.min_word_len_two_typos(&txn).unwrap(), DEFAULT_MIN_WORD_LEN_TWO_TYPOS);
    }

    #[test]
    fn update_invalid_min_word_len_for_typo() {
        let index = TempIndex::new();

        // Set the genres setting
        index
            .update_settings(|settings| {
                settings.set_min_word_len_one_typo(10);
                settings.set_min_word_len_two_typos(7);
            })
            .unwrap_err();
    }

    #[test]
    fn update_exact_words_normalization() {
        let index = TempIndex::new();

        let mut txn = index.write_txn().unwrap();
        // Set the genres setting
        index
            .update_settings_using_wtxn(&mut txn, |settings| {
                let words = btreeset! { S("Ab"), S("ac") };
                settings.set_exact_words(words);
            })
            .unwrap();

        let exact_words = index.exact_words(&txn).unwrap().unwrap();
        for word in exact_words.into_fst().stream().into_str_vec().unwrap() {
            assert!(word.0 == "ac" || word.0 == "ab");
        }
    }

    #[test]
    fn test_correct_settings_init() {
        let index = TempIndex::new();

        index
            .update_settings(|settings| {
                // we don't actually update the settings, just check their content
                let Settings {
                    wtxn: _,
                    index: _,
                    indexer_config: _,
                    searchable_fields,
                    displayed_fields,
                    filterable_fields,
                    sortable_fields,
                    criteria,
                    stop_words,
                    non_separator_tokens,
                    separator_tokens,
                    dictionary,
                    distinct_field,
                    synonyms,
                    primary_key,
                    authorize_typos,
                    min_word_len_two_typos,
                    min_word_len_one_typo,
                    exact_words,
                    exact_attributes,
                    max_values_per_facet,
                    sort_facet_values_by,
                    pagination_max_total_hits,
                    proximity_precision,
                    embedder_settings,
                    search_cutoff,
                    localized_attributes_rules,
                    prefix_search,
                    facet_search,
                } = settings;
                assert!(matches!(searchable_fields, Setting::NotSet));
                assert!(matches!(displayed_fields, Setting::NotSet));
                assert!(matches!(filterable_fields, Setting::NotSet));
                assert!(matches!(sortable_fields, Setting::NotSet));
                assert!(matches!(criteria, Setting::NotSet));
                assert!(matches!(stop_words, Setting::NotSet));
                assert!(matches!(non_separator_tokens, Setting::NotSet));
                assert!(matches!(separator_tokens, Setting::NotSet));
                assert!(matches!(dictionary, Setting::NotSet));
                assert!(matches!(distinct_field, Setting::NotSet));
                assert!(matches!(synonyms, Setting::NotSet));
                assert!(matches!(primary_key, Setting::NotSet));
                assert!(matches!(authorize_typos, Setting::NotSet));
                assert!(matches!(min_word_len_two_typos, Setting::NotSet));
                assert!(matches!(min_word_len_one_typo, Setting::NotSet));
                assert!(matches!(exact_words, Setting::NotSet));
                assert!(matches!(exact_attributes, Setting::NotSet));
                assert!(matches!(max_values_per_facet, Setting::NotSet));
                assert!(matches!(sort_facet_values_by, Setting::NotSet));
                assert!(matches!(pagination_max_total_hits, Setting::NotSet));
                assert!(matches!(proximity_precision, Setting::NotSet));
                assert!(matches!(embedder_settings, Setting::NotSet));
                assert!(matches!(search_cutoff, Setting::NotSet));
                assert!(matches!(localized_attributes_rules, Setting::NotSet));
                assert!(matches!(prefix_search, Setting::NotSet));
                assert!(matches!(facet_search, Setting::NotSet));
            })
            .unwrap();
    }

    #[test]
    fn settings_must_ignore_soft_deleted() {
        use serde_json::json;

        let index = TempIndex::new();

        let mut docs = vec![];
        for i in 0..10 {
            docs.push(json!({ "id": i, "title": format!("{:x}", i) }));
        }
        index.add_documents(documents! { docs }).unwrap();

        index.delete_documents((0..5).map(|id| id.to_string()).collect());

        let mut wtxn = index.write_txn().unwrap();
        index
            .update_settings_using_wtxn(&mut wtxn, |settings| {
                settings.set_searchable_fields(vec!["id".to_string()]);
            })
            .unwrap();
        wtxn.commit().unwrap();

        let rtxn = index.write_txn().unwrap();
        let docs: StdResult<Vec<_>, _> = index.all_compressed_documents(&rtxn).unwrap().collect();
        let docs = docs.unwrap();
        assert_eq!(docs.len(), 5);
    }
}
