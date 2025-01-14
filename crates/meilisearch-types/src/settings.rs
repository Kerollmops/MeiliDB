use std::collections::{BTreeMap, BTreeSet};
use std::convert::Infallible;
use std::fmt;
use std::marker::PhantomData;
use std::num::NonZeroUsize;
use std::ops::{ControlFlow, Deref};
use std::str::FromStr;

use deserr::{DeserializeError, Deserr, ErrorKind, MergeWithError, ValuePointerRef};
use fst::IntoStreamer;
use milli::index::{IndexEmbeddingConfig, PrefixSearch};
use milli::proximity::ProximityPrecision;
use milli::update::Setting;
use milli::{Criterion, CriterionError, Index, DEFAULT_VALUES_PER_FACET};
use serde::{Deserialize, Serialize, Serializer};
use utoipa::ToSchema;

use crate::deserr::DeserrJsonError;
use crate::error::deserr_codes::*;
use crate::facet_values_sort::FacetValuesSort;
use crate::locales::LocalizedAttributesRuleView;

/// The maximum number of results that the engine
/// will be able to return in one search call.
pub const DEFAULT_PAGINATION_MAX_TOTAL_HITS: usize = 1000;

fn serialize_with_wildcard<S>(
    field: &Setting<Vec<String>>,
    s: S,
) -> std::result::Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let wildcard = vec!["*".to_string()];
    match field {
        Setting::Set(value) => Some(value),
        Setting::Reset => Some(&wildcard),
        Setting::NotSet => None,
    }
    .serialize(s)
}

#[derive(Clone, Default, Debug, Serialize, PartialEq, Eq, ToSchema)]
pub struct Checked;

#[derive(Clone, Default, Debug, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
pub struct Unchecked;

impl<E> Deserr<E> for Unchecked
where
    E: DeserializeError,
{
    fn deserialize_from_value<V: deserr::IntoValue>(
        _value: deserr::Value<V>,
        _location: deserr::ValuePointerRef,
    ) -> Result<Self, E> {
        unreachable!()
    }
}

fn validate_min_word_size_for_typo_setting<E: DeserializeError>(
    s: MinWordSizeTyposSetting,
    location: ValuePointerRef,
) -> Result<MinWordSizeTyposSetting, E> {
    if let (Setting::Set(one), Setting::Set(two)) = (s.one_typo, s.two_typos) {
        if one > two {
            return Err(deserr::take_cf_content(E::error::<Infallible>(None, ErrorKind::Unexpected { msg: format!("`minWordSizeForTypos` setting is invalid. `oneTypo` and `twoTypos` fields should be between `0` and `255`, and `twoTypos` should be greater or equals to `oneTypo` but found `oneTypo: {one}` and twoTypos: {two}`.") }, location)));
        }
    }
    Ok(s)
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq, Deserr, ToSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
#[deserr(deny_unknown_fields, rename_all = camelCase, validate = validate_min_word_size_for_typo_setting -> DeserrJsonError<InvalidSettingsTypoTolerance>)]
pub struct MinWordSizeTyposSetting {
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default)]
    #[schema(value_type = Option<u8>, example = json!(5))]
    pub one_typo: Setting<u8>,
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default)]
    #[schema(value_type = Option<u8>, example = json!(9))]
    pub two_typos: Setting<u8>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq, Deserr, ToSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
#[deserr(deny_unknown_fields, rename_all = camelCase, where_predicate = __Deserr_E: deserr::MergeWithError<DeserrJsonError<InvalidSettingsTypoTolerance>>)]
pub struct TypoSettings {
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default)]
    #[schema(value_type = Option<bool>, example = json!(true))]
    pub enabled: Setting<bool>,
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default, error = DeserrJsonError<InvalidSettingsTypoTolerance>)]
    #[schema(value_type = Option<MinWordSizeTyposSetting>, example = json!({ "oneTypo": 5, "twoTypo": 9 }))]
    pub min_word_size_for_typos: Setting<MinWordSizeTyposSetting>,
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default)]
    #[schema(value_type = Option<BTreeSet<String>>, example = json!(["iPhone", "phone"]))]
    pub disable_on_words: Setting<BTreeSet<String>>,
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default)]
    #[schema(value_type = Option<BTreeSet<String>>, example = json!(["uuid", "url"]))]
    pub disable_on_attributes: Setting<BTreeSet<String>>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq, Deserr, ToSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
#[deserr(rename_all = camelCase, deny_unknown_fields)]
pub struct FacetingSettings {
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default)]
    #[schema(value_type = Option<usize>, example = json!(10))]
    pub max_values_per_facet: Setting<usize>,
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default)]
    #[schema(value_type = Option<BTreeMap<String, FacetValuesSort>>, example = json!({ "genre": FacetValuesSort::Count }))]
    pub sort_facet_values_by: Setting<BTreeMap<String, FacetValuesSort>>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq, Deserr, ToSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
#[deserr(rename_all = camelCase, deny_unknown_fields)]
pub struct PaginationSettings {
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default)]
    #[schema(value_type = Option<usize>, example = json!(250))]
    pub max_total_hits: Setting<usize>,
}

impl MergeWithError<milli::CriterionError> for DeserrJsonError<InvalidSettingsRankingRules> {
    fn merge(
        _self_: Option<Self>,
        other: milli::CriterionError,
        merge_location: ValuePointerRef,
    ) -> ControlFlow<Self, Self> {
        Self::error::<Infallible>(
            None,
            ErrorKind::Unexpected { msg: other.to_string() },
            merge_location,
        )
    }
}

#[derive(Default, Serialize, Deserialize, PartialEq, Eq, Clone, ToSchema)]
#[repr(transparent)]
#[serde(transparent)]
/// "Technical" type that is required due to utoipa.
///
/// We did not find a way to implement [`utoipa::ToSchema`] for the [`Setting`] enum,
/// but most types can use the `value_type` macro parameter to workaround that issue.
///
/// However that type is used in the settings route, including through the macro that auto-generate
/// all the settings route, so we can't remap the `value_type`.
pub struct SettingEmbeddingSettings {
    #[schema(inline, value_type = Option<crate::milli::vector::settings::EmbeddingSettings>)]
    pub inner: Setting<crate::milli::vector::settings::EmbeddingSettings>,
}

impl fmt::Debug for SettingEmbeddingSettings {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.inner.fmt(f)
    }
}

impl<E: DeserializeError> Deserr<E> for SettingEmbeddingSettings {
    fn deserialize_from_value<V: deserr::IntoValue>(
        value: deserr::Value<V>,
        location: ValuePointerRef,
    ) -> Result<Self, E> {
        Setting::<crate::milli::vector::settings::EmbeddingSettings>::deserialize_from_value(
            value, location,
        )
        .map(|inner| Self { inner })
    }
}

/// Holds all the settings for an index. `T` can either be `Checked` if they represents settings
/// whose validity is guaranteed, or `Unchecked` if they need to be validated. In the later case, a
/// call to `check` will return a `Settings<Checked>` from a `Settings<Unchecked>`.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq, Deserr, ToSchema)]
#[serde(
    deny_unknown_fields,
    rename_all = "camelCase",
    bound(serialize = "T: Serialize", deserialize = "T: Deserialize<'static>")
)]
#[deserr(error = DeserrJsonError, rename_all = camelCase, deny_unknown_fields)]
#[schema(rename_all = "camelCase")]
pub struct Settings<T> {
    /// Fields displayed in the returned documents.
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default, error = DeserrJsonError<InvalidSettingsDisplayedAttributes>)]
    #[schema(value_type = Option<Vec<String>>, example = json!(["id", "title", "description", "url"]))]
    pub displayed_attributes: WildcardSetting,
    /// Fields in which to search for matching query words sorted by order of importance.
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default, error = DeserrJsonError<InvalidSettingsSearchableAttributes>)]
    #[schema(value_type = Option<Vec<String>>, example = json!(["title", "description"]))]
    pub searchable_attributes: WildcardSetting,
    /// Attributes to use for faceting and filtering. See [Filtering and Faceted Search](https://www.meilisearch.com/docs/learn/filtering_and_sorting/search_with_facet_filters).
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default, error = DeserrJsonError<InvalidSettingsFilterableAttributes>)]
    #[schema(value_type = Option<Vec<String>>, example = json!(["release_date", "genre"]))]
    pub filterable_attributes: Setting<BTreeSet<String>>,
    /// Attributes to use when sorting search results.
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default, error = DeserrJsonError<InvalidSettingsSortableAttributes>)]
    #[schema(value_type = Option<Vec<String>>, example = json!(["release_date"]))]
    pub sortable_attributes: Setting<BTreeSet<String>>,
    /// List of ranking rules sorted by order of importance. The order is customizable.
    /// [A list of ordered built-in ranking rules](https://www.meilisearch.com/docs/learn/relevancy/relevancy).
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default, error = DeserrJsonError<InvalidSettingsRankingRules>)]
    #[schema(value_type = Option<Vec<String>>, example = json!([RankingRuleView::Words, RankingRuleView::Typo, RankingRuleView::Proximity, RankingRuleView::Attribute, RankingRuleView::Exactness]))]
    pub ranking_rules: Setting<Vec<RankingRuleView>>,
    /// List of words ignored when present in search queries.
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default, error = DeserrJsonError<InvalidSettingsStopWords>)]
    #[schema(value_type = Option<Vec<String>>, example = json!(["the", "a", "them", "their"]))]
    pub stop_words: Setting<BTreeSet<String>>,
    /// List of characters not delimiting where one term begins and ends.
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default, error = DeserrJsonError<InvalidSettingsNonSeparatorTokens>)]
    #[schema(value_type = Option<Vec<String>>, example = json!([" ", "\n"]))]
    pub non_separator_tokens: Setting<BTreeSet<String>>,
    /// List of characters delimiting where one term begins and ends.
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default, error = DeserrJsonError<InvalidSettingsSeparatorTokens>)]
    #[schema(value_type = Option<Vec<String>>, example = json!(["S"]))]
    pub separator_tokens: Setting<BTreeSet<String>>,
    /// List of strings Meilisearch should parse as a single term.
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default, error = DeserrJsonError<InvalidSettingsDictionary>)]
    #[schema(value_type = Option<Vec<String>>, example = json!(["iPhone pro"]))]
    pub dictionary: Setting<BTreeSet<String>>,
    /// List of associated words treated similarly. A word associated to an array of word as synonyms.
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default, error = DeserrJsonError<InvalidSettingsSynonyms>)]
    #[schema(value_type = Option<BTreeMap<String, Vec<String>>>, example = json!({ "he": ["she", "they", "them"], "phone": ["iPhone", "android"]}))]
    pub synonyms: Setting<BTreeMap<String, Vec<String>>>,
    /// Search returns documents with distinct (different) values of the given field.
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default, error = DeserrJsonError<InvalidSettingsDistinctAttribute>)]
    #[schema(value_type = Option<String>, example = json!("sku"))]
    pub distinct_attribute: Setting<String>,
    /// Precision level when calculating the proximity ranking rule.
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default, error = DeserrJsonError<InvalidSettingsProximityPrecision>)]
    #[schema(value_type = Option<String>, example = json!(ProximityPrecisionView::ByAttribute))]
    pub proximity_precision: Setting<ProximityPrecisionView>,
    /// Customize typo tolerance feature.
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default, error = DeserrJsonError<InvalidSettingsTypoTolerance>)]
    #[schema(value_type = Option<TypoSettings>, example = json!({ "enabled": true, "disableOnAttributes": ["title"]}))]
    pub typo_tolerance: Setting<TypoSettings>,
    /// Faceting settings.
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default, error = DeserrJsonError<InvalidSettingsFaceting>)]
    #[schema(value_type = Option<FacetingSettings>, example = json!({ "maxValuesPerFacet": 10, "sortFacetValuesBy": { "genre": FacetValuesSort::Count }}))]
    pub faceting: Setting<FacetingSettings>,
    /// Pagination settings.
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default, error = DeserrJsonError<InvalidSettingsPagination>)]
    #[schema(value_type = Option<PaginationSettings>, example = json!({ "maxValuesPerFacet": 10, "sortFacetValuesBy": { "genre": FacetValuesSort::Count }}))]
    pub pagination: Setting<PaginationSettings>,

    /// Embedder required for performing semantic search queries.
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default, error = DeserrJsonError<InvalidSettingsEmbedders>)]
    #[schema(value_type = Option<BTreeMap<String, SettingEmbeddingSettings>>)]
    pub embedders: Setting<BTreeMap<String, SettingEmbeddingSettings>>,
    /// Maximum duration of a search query.
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default, error = DeserrJsonError<InvalidSettingsSearchCutoffMs>)]
    #[schema(value_type = Option<u64>, example = json!(50))]
    pub search_cutoff_ms: Setting<u64>,
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default, error = DeserrJsonError<InvalidSettingsLocalizedAttributes>)]
    #[schema(value_type = Option<Vec<LocalizedAttributesRuleView>>, example = json!(50))]
    pub localized_attributes: Setting<Vec<LocalizedAttributesRuleView>>,
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default, error = DeserrJsonError<InvalidSettingsFacetSearch>)]
    #[schema(value_type = Option<bool>, example = json!(true))]
    pub facet_search: Setting<bool>,
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default, error = DeserrJsonError<InvalidSettingsPrefixSearch>)]
    #[schema(value_type = Option<PrefixSearchSettings>, example = json!("Hemlo"))]
    pub prefix_search: Setting<PrefixSearchSettings>,

    #[serde(skip)]
    #[deserr(skip)]
    pub _kind: PhantomData<T>,
}

impl<T> Settings<T> {
    pub fn hide_secrets(&mut self) {
        let Setting::Set(embedders) = &mut self.embedders else {
            return;
        };

        for mut embedder in embedders.values_mut() {
            let SettingEmbeddingSettings { inner: Setting::Set(embedder) } = &mut embedder else {
                continue;
            };

            let Setting::Set(api_key) = &mut embedder.api_key else {
                continue;
            };

            Self::hide_secret(api_key);
        }
    }

    fn hide_secret(secret: &mut String) {
        match secret.len() {
            x if x < 10 => {
                secret.replace_range(.., "XXX...");
            }
            x if x < 20 => {
                secret.replace_range(2.., "XXXX...");
            }
            x if x < 30 => {
                secret.replace_range(3.., "XXXXX...");
            }
            _x => {
                secret.replace_range(5.., "XXXXXX...");
            }
        }
    }
}

impl Settings<Checked> {
    pub fn cleared() -> Settings<Checked> {
        Settings {
            displayed_attributes: Setting::Reset.into(),
            searchable_attributes: Setting::Reset.into(),
            filterable_attributes: Setting::Reset,
            sortable_attributes: Setting::Reset,
            ranking_rules: Setting::Reset,
            stop_words: Setting::Reset,
            synonyms: Setting::Reset,
            non_separator_tokens: Setting::Reset,
            separator_tokens: Setting::Reset,
            dictionary: Setting::Reset,
            distinct_attribute: Setting::Reset,
            proximity_precision: Setting::Reset,
            typo_tolerance: Setting::Reset,
            faceting: Setting::Reset,
            pagination: Setting::Reset,
            embedders: Setting::Reset,
            search_cutoff_ms: Setting::Reset,
            localized_attributes: Setting::Reset,
            facet_search: Setting::Reset,
            prefix_search: Setting::Reset,
            _kind: PhantomData,
        }
    }

    pub fn into_unchecked(self) -> Settings<Unchecked> {
        let Self {
            displayed_attributes,
            searchable_attributes,
            filterable_attributes,
            sortable_attributes,
            ranking_rules,
            stop_words,
            non_separator_tokens,
            separator_tokens,
            dictionary,
            synonyms,
            distinct_attribute,
            proximity_precision,
            typo_tolerance,
            faceting,
            pagination,
            embedders,
            search_cutoff_ms,
            localized_attributes: localized_attributes_rules,
            facet_search,
            prefix_search,
            _kind,
        } = self;

        Settings {
            displayed_attributes,
            searchable_attributes,
            filterable_attributes,
            sortable_attributes,
            ranking_rules,
            stop_words,
            non_separator_tokens,
            separator_tokens,
            dictionary,
            synonyms,
            distinct_attribute,
            proximity_precision,
            typo_tolerance,
            faceting,
            pagination,
            embedders,
            search_cutoff_ms,
            localized_attributes: localized_attributes_rules,
            facet_search,
            prefix_search,
            _kind: PhantomData,
        }
    }
}

impl Settings<Unchecked> {
    pub fn check(self) -> Settings<Checked> {
        let displayed_attributes = match self.displayed_attributes.0 {
            Setting::Set(fields) => {
                if fields.iter().any(|f| f == "*") {
                    Setting::Reset
                } else {
                    Setting::Set(fields)
                }
            }
            otherwise => otherwise,
        };

        let searchable_attributes = match self.searchable_attributes.0 {
            Setting::Set(fields) => {
                if fields.iter().any(|f| f == "*") {
                    Setting::Reset
                } else {
                    Setting::Set(fields)
                }
            }
            otherwise => otherwise,
        };

        Settings {
            displayed_attributes: displayed_attributes.into(),
            searchable_attributes: searchable_attributes.into(),
            filterable_attributes: self.filterable_attributes,
            sortable_attributes: self.sortable_attributes,
            ranking_rules: self.ranking_rules,
            stop_words: self.stop_words,
            synonyms: self.synonyms,
            non_separator_tokens: self.non_separator_tokens,
            separator_tokens: self.separator_tokens,
            dictionary: self.dictionary,
            distinct_attribute: self.distinct_attribute,
            proximity_precision: self.proximity_precision,
            typo_tolerance: self.typo_tolerance,
            faceting: self.faceting,
            pagination: self.pagination,
            embedders: self.embedders,
            search_cutoff_ms: self.search_cutoff_ms,
            localized_attributes: self.localized_attributes,
            facet_search: self.facet_search,
            prefix_search: self.prefix_search,
            _kind: PhantomData,
        }
    }

    pub fn validate(self) -> Result<Self, milli::Error> {
        self.validate_embedding_settings()
    }

    fn validate_embedding_settings(mut self) -> Result<Self, milli::Error> {
        let Setting::Set(mut configs) = self.embedders else { return Ok(self) };
        for (name, config) in configs.iter_mut() {
            let config_to_check = std::mem::take(config);
            let checked_config =
                milli::update::validate_embedding_settings(config_to_check.inner, name)?;
            *config = SettingEmbeddingSettings { inner: checked_config };
        }
        self.embedders = Setting::Set(configs);
        Ok(self)
    }

    pub fn merge(&mut self, other: &Self) {
        // For most settings only the latest version is kept
        *self = Self {
            displayed_attributes: other
                .displayed_attributes
                .clone()
                .or(self.displayed_attributes.clone()),
            searchable_attributes: other
                .searchable_attributes
                .clone()
                .or(self.searchable_attributes.clone()),
            filterable_attributes: other
                .filterable_attributes
                .clone()
                .or(self.filterable_attributes.clone()),
            sortable_attributes: other
                .sortable_attributes
                .clone()
                .or(self.sortable_attributes.clone()),
            ranking_rules: other.ranking_rules.clone().or(self.ranking_rules.clone()),
            stop_words: other.stop_words.clone().or(self.stop_words.clone()),
            non_separator_tokens: other
                .non_separator_tokens
                .clone()
                .or(self.non_separator_tokens.clone()),
            separator_tokens: other.separator_tokens.clone().or(self.separator_tokens.clone()),
            dictionary: other.dictionary.clone().or(self.dictionary.clone()),
            synonyms: other.synonyms.clone().or(self.synonyms.clone()),
            distinct_attribute: other
                .distinct_attribute
                .clone()
                .or(self.distinct_attribute.clone()),
            proximity_precision: other.proximity_precision.or(self.proximity_precision),
            typo_tolerance: other.typo_tolerance.clone().or(self.typo_tolerance.clone()),
            faceting: other.faceting.clone().or(self.faceting.clone()),
            pagination: other.pagination.clone().or(self.pagination.clone()),
            search_cutoff_ms: other.search_cutoff_ms.or(self.search_cutoff_ms),
            localized_attributes: other
                .localized_attributes
                .clone()
                .or(self.localized_attributes.clone()),
            embedders: match (self.embedders.clone(), other.embedders.clone()) {
                (Setting::NotSet, set) | (set, Setting::NotSet) => set,
                (Setting::Set(_) | Setting::Reset, Setting::Reset) => Setting::Reset,
                (Setting::Reset, Setting::Set(embedder)) => Setting::Set(embedder),

                // If both are set we must merge the embeddings settings
                (Setting::Set(mut this), Setting::Set(other)) => {
                    for (k, v) in other {
                        this.insert(k, v);
                    }
                    Setting::Set(this)
                }
            },
            prefix_search: other.prefix_search.or(self.prefix_search),
            facet_search: other.facet_search.or(self.facet_search),
            _kind: PhantomData,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct Facets {
    pub level_group_size: Option<NonZeroUsize>,
    pub min_level_size: Option<NonZeroUsize>,
}

pub fn apply_settings_to_builder(
    settings: &Settings<Checked>,
    builder: &mut milli::update::Settings,
) {
    let Settings {
        displayed_attributes,
        searchable_attributes,
        filterable_attributes,
        sortable_attributes,
        ranking_rules,
        stop_words,
        non_separator_tokens,
        separator_tokens,
        dictionary,
        synonyms,
        distinct_attribute,
        proximity_precision,
        typo_tolerance,
        faceting,
        pagination,
        embedders,
        search_cutoff_ms,
        localized_attributes: localized_attributes_rules,
        facet_search,
        prefix_search,
        _kind,
    } = settings;

    match searchable_attributes.deref() {
        Setting::Set(ref names) => builder.set_searchable_fields(names.clone()),
        Setting::Reset => builder.reset_searchable_fields(),
        Setting::NotSet => (),
    }

    match displayed_attributes.deref() {
        Setting::Set(ref names) => builder.set_displayed_fields(names.clone()),
        Setting::Reset => builder.reset_displayed_fields(),
        Setting::NotSet => (),
    }

    match filterable_attributes {
        Setting::Set(ref facets) => {
            builder.set_filterable_fields(facets.clone().into_iter().collect())
        }
        Setting::Reset => builder.reset_filterable_fields(),
        Setting::NotSet => (),
    }

    match sortable_attributes {
        Setting::Set(ref fields) => builder.set_sortable_fields(fields.iter().cloned().collect()),
        Setting::Reset => builder.reset_sortable_fields(),
        Setting::NotSet => (),
    }

    match ranking_rules {
        Setting::Set(ref criteria) => {
            builder.set_criteria(criteria.iter().map(|c| c.clone().into()).collect())
        }
        Setting::Reset => builder.reset_criteria(),
        Setting::NotSet => (),
    }

    match stop_words {
        Setting::Set(ref stop_words) => builder.set_stop_words(stop_words.clone()),
        Setting::Reset => builder.reset_stop_words(),
        Setting::NotSet => (),
    }

    match non_separator_tokens {
        Setting::Set(ref non_separator_tokens) => {
            builder.set_non_separator_tokens(non_separator_tokens.clone())
        }
        Setting::Reset => builder.reset_non_separator_tokens(),
        Setting::NotSet => (),
    }

    match separator_tokens {
        Setting::Set(ref separator_tokens) => {
            builder.set_separator_tokens(separator_tokens.clone())
        }
        Setting::Reset => builder.reset_separator_tokens(),
        Setting::NotSet => (),
    }

    match dictionary {
        Setting::Set(ref dictionary) => builder.set_dictionary(dictionary.clone()),
        Setting::Reset => builder.reset_dictionary(),
        Setting::NotSet => (),
    }

    match synonyms {
        Setting::Set(ref synonyms) => builder.set_synonyms(synonyms.clone().into_iter().collect()),
        Setting::Reset => builder.reset_synonyms(),
        Setting::NotSet => (),
    }

    match distinct_attribute {
        Setting::Set(ref attr) => builder.set_distinct_field(attr.clone()),
        Setting::Reset => builder.reset_distinct_field(),
        Setting::NotSet => (),
    }

    match proximity_precision {
        Setting::Set(ref precision) => builder.set_proximity_precision((*precision).into()),
        Setting::Reset => builder.reset_proximity_precision(),
        Setting::NotSet => (),
    }

    match localized_attributes_rules {
        Setting::Set(ref rules) => builder
            .set_localized_attributes_rules(rules.iter().cloned().map(|r| r.into()).collect()),
        Setting::Reset => builder.reset_localized_attributes_rules(),
        Setting::NotSet => (),
    }

    match typo_tolerance {
        Setting::Set(ref value) => {
            match value.enabled {
                Setting::Set(val) => builder.set_autorize_typos(val),
                Setting::Reset => builder.reset_authorize_typos(),
                Setting::NotSet => (),
            }

            match value.min_word_size_for_typos {
                Setting::Set(ref setting) => {
                    match setting.one_typo {
                        Setting::Set(val) => builder.set_min_word_len_one_typo(val),
                        Setting::Reset => builder.reset_min_word_len_one_typo(),
                        Setting::NotSet => (),
                    }
                    match setting.two_typos {
                        Setting::Set(val) => builder.set_min_word_len_two_typos(val),
                        Setting::Reset => builder.reset_min_word_len_two_typos(),
                        Setting::NotSet => (),
                    }
                }
                Setting::Reset => {
                    builder.reset_min_word_len_one_typo();
                    builder.reset_min_word_len_two_typos();
                }
                Setting::NotSet => (),
            }

            match value.disable_on_words {
                Setting::Set(ref words) => {
                    builder.set_exact_words(words.clone());
                }
                Setting::Reset => builder.reset_exact_words(),
                Setting::NotSet => (),
            }

            match value.disable_on_attributes {
                Setting::Set(ref words) => {
                    builder.set_exact_attributes(words.iter().cloned().collect())
                }
                Setting::Reset => builder.reset_exact_attributes(),
                Setting::NotSet => (),
            }
        }
        Setting::Reset => {
            // all typo settings need to be reset here.
            builder.reset_authorize_typos();
            builder.reset_min_word_len_one_typo();
            builder.reset_min_word_len_two_typos();
            builder.reset_exact_words();
            builder.reset_exact_attributes();
        }
        Setting::NotSet => (),
    }

    match faceting {
        Setting::Set(FacetingSettings { max_values_per_facet, sort_facet_values_by }) => {
            match max_values_per_facet {
                Setting::Set(val) => builder.set_max_values_per_facet(*val),
                Setting::Reset => builder.reset_max_values_per_facet(),
                Setting::NotSet => (),
            }
            match sort_facet_values_by {
                Setting::Set(val) => builder.set_sort_facet_values_by(
                    val.iter().map(|(name, order)| (name.clone(), (*order).into())).collect(),
                ),
                Setting::Reset => builder.reset_sort_facet_values_by(),
                Setting::NotSet => (),
            }
        }
        Setting::Reset => {
            builder.reset_max_values_per_facet();
            builder.reset_sort_facet_values_by();
        }
        Setting::NotSet => (),
    }

    match pagination {
        Setting::Set(ref value) => match value.max_total_hits {
            Setting::Set(val) => builder.set_pagination_max_total_hits(val),
            Setting::Reset => builder.reset_pagination_max_total_hits(),
            Setting::NotSet => (),
        },
        Setting::Reset => builder.reset_pagination_max_total_hits(),
        Setting::NotSet => (),
    }

    match embedders {
        Setting::Set(value) => builder.set_embedder_settings(
            value.iter().map(|(k, v)| (k.clone(), v.inner.clone())).collect(),
        ),
        Setting::Reset => builder.reset_embedder_settings(),
        Setting::NotSet => (),
    }

    match search_cutoff_ms {
        Setting::Set(cutoff) => builder.set_search_cutoff(*cutoff),
        Setting::Reset => builder.reset_search_cutoff(),
        Setting::NotSet => (),
    }

    match prefix_search {
        Setting::Set(prefix_search) => {
            builder.set_prefix_search(PrefixSearch::from(*prefix_search))
        }
        Setting::Reset => builder.reset_prefix_search(),
        Setting::NotSet => (),
    }

    match facet_search {
        Setting::Set(facet_search) => builder.set_facet_search(*facet_search),
        Setting::Reset => builder.reset_facet_search(),
        Setting::NotSet => (),
    }
}

pub enum SecretPolicy {
    RevealSecrets,
    HideSecrets,
}

pub fn settings(
    index: &Index,
    rtxn: &crate::heed::RoTxn,
    secret_policy: SecretPolicy,
) -> Result<Settings<Checked>, milli::Error> {
    let displayed_attributes =
        index.displayed_fields(rtxn)?.map(|fields| fields.into_iter().map(String::from).collect());

    let searchable_attributes = index
        .user_defined_searchable_fields(rtxn)?
        .map(|fields| fields.into_iter().map(String::from).collect());

    let filterable_attributes = index.filterable_fields(rtxn)?.into_iter().collect();

    let sortable_attributes = index.sortable_fields(rtxn)?.into_iter().collect();

    let criteria = index.criteria(rtxn)?;

    let stop_words = index
        .stop_words(rtxn)?
        .map(|stop_words| -> Result<BTreeSet<_>, milli::Error> {
            Ok(stop_words.stream().into_strs()?.into_iter().collect())
        })
        .transpose()?
        .unwrap_or_default();

    let non_separator_tokens = index.non_separator_tokens(rtxn)?.unwrap_or_default();
    let separator_tokens = index.separator_tokens(rtxn)?.unwrap_or_default();
    let dictionary = index.dictionary(rtxn)?.unwrap_or_default();

    let distinct_field = index.distinct_field(rtxn)?.map(String::from);

    let proximity_precision = index.proximity_precision(rtxn)?.map(ProximityPrecisionView::from);

    let synonyms = index.user_defined_synonyms(rtxn)?;

    let min_typo_word_len = MinWordSizeTyposSetting {
        one_typo: Setting::Set(index.min_word_len_one_typo(rtxn)?),
        two_typos: Setting::Set(index.min_word_len_two_typos(rtxn)?),
    };

    let disabled_words = match index.exact_words(rtxn)? {
        Some(fst) => fst.into_stream().into_strs()?.into_iter().collect(),
        None => BTreeSet::new(),
    };

    let disabled_attributes = index.exact_attributes(rtxn)?.into_iter().map(String::from).collect();

    let typo_tolerance = TypoSettings {
        enabled: Setting::Set(index.authorize_typos(rtxn)?),
        min_word_size_for_typos: Setting::Set(min_typo_word_len),
        disable_on_words: Setting::Set(disabled_words),
        disable_on_attributes: Setting::Set(disabled_attributes),
    };

    let faceting = FacetingSettings {
        max_values_per_facet: Setting::Set(
            index
                .max_values_per_facet(rtxn)?
                .map(|x| x as usize)
                .unwrap_or(DEFAULT_VALUES_PER_FACET),
        ),
        sort_facet_values_by: Setting::Set(
            index
                .sort_facet_values_by(rtxn)?
                .into_iter()
                .map(|(name, sort)| (name, sort.into()))
                .collect(),
        ),
    };

    let pagination = PaginationSettings {
        max_total_hits: Setting::Set(
            index
                .pagination_max_total_hits(rtxn)?
                .map(|x| x as usize)
                .unwrap_or(DEFAULT_PAGINATION_MAX_TOTAL_HITS),
        ),
    };

    let embedders: BTreeMap<_, _> = index
        .embedding_configs(rtxn)?
        .into_iter()
        .map(|IndexEmbeddingConfig { name, config, .. }| {
            (name, SettingEmbeddingSettings { inner: Setting::Set(config.into()) })
        })
        .collect();
    let embedders = Setting::Set(embedders);

    let search_cutoff_ms = index.search_cutoff(rtxn)?;

    let localized_attributes_rules = index.localized_attributes_rules(rtxn)?;

    let prefix_search = index.prefix_search(rtxn)?.map(PrefixSearchSettings::from);

    let facet_search = index.facet_search(rtxn)?;

    let mut settings = Settings {
        displayed_attributes: match displayed_attributes {
            Some(attrs) => Setting::Set(attrs),
            None => Setting::Reset,
        }
        .into(),
        searchable_attributes: match searchable_attributes {
            Some(attrs) => Setting::Set(attrs),
            None => Setting::Reset,
        }
        .into(),
        filterable_attributes: Setting::Set(filterable_attributes),
        sortable_attributes: Setting::Set(sortable_attributes),
        ranking_rules: Setting::Set(criteria.iter().map(|c| c.clone().into()).collect()),
        stop_words: Setting::Set(stop_words),
        non_separator_tokens: Setting::Set(non_separator_tokens),
        separator_tokens: Setting::Set(separator_tokens),
        dictionary: Setting::Set(dictionary),
        distinct_attribute: match distinct_field {
            Some(field) => Setting::Set(field),
            None => Setting::Reset,
        },
        proximity_precision: Setting::Set(proximity_precision.unwrap_or_default()),
        synonyms: Setting::Set(synonyms),
        typo_tolerance: Setting::Set(typo_tolerance),
        faceting: Setting::Set(faceting),
        pagination: Setting::Set(pagination),
        embedders,
        search_cutoff_ms: match search_cutoff_ms {
            Some(cutoff) => Setting::Set(cutoff),
            None => Setting::Reset,
        },
        localized_attributes: match localized_attributes_rules {
            Some(rules) => Setting::Set(rules.into_iter().map(|r| r.into()).collect()),
            None => Setting::Reset,
        },
        prefix_search: Setting::Set(prefix_search.unwrap_or_default()),
        facet_search: Setting::Set(facet_search),
        _kind: PhantomData,
    };

    if let SecretPolicy::HideSecrets = secret_policy {
        settings.hide_secrets()
    }
    Ok(settings)
}

#[derive(Debug, Clone, PartialEq, Eq, Deserr, ToSchema)]
#[deserr(try_from(&String) = FromStr::from_str -> CriterionError)]
pub enum RankingRuleView {
    /// Sorted by decreasing number of matched query terms.
    /// Query words at the front of an attribute is considered better than if it was at the back.
    Words,
    /// Sorted by increasing number of typos.
    Typo,
    /// Sorted by increasing distance between matched query terms.
    Proximity,
    /// Documents with quey words contained in more important
    /// attributes are considered better.
    Attribute,
    /// Dynamically sort at query time the documents. None, one or multiple Asc/Desc sortable
    /// attributes can be used in place of this criterion at query time.
    Sort,
    /// Sorted by the similarity of the matched words with the query words.
    Exactness,
    /// Sorted by the increasing value of the field specified.
    Asc(String),
    /// Sorted by the decreasing value of the field specified.
    Desc(String),
}
impl Serialize for RankingRuleView {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&format!("{}", Criterion::from(self.clone())))
    }
}
impl<'de> Deserialize<'de> for RankingRuleView {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct Visitor;
        impl<'de> serde::de::Visitor<'de> for Visitor {
            type Value = RankingRuleView;
            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                write!(formatter, "the name of a valid ranking rule (string)")
            }
            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                let criterion = Criterion::from_str(v).map_err(|_| {
                    E::invalid_value(serde::de::Unexpected::Str(v), &"a valid ranking rule")
                })?;
                Ok(RankingRuleView::from(criterion))
            }
        }
        deserializer.deserialize_str(Visitor)
    }
}
impl FromStr for RankingRuleView {
    type Err = <Criterion as FromStr>::Err;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(RankingRuleView::from(Criterion::from_str(s)?))
    }
}
impl fmt::Display for RankingRuleView {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        fmt::Display::fmt(&Criterion::from(self.clone()), f)
    }
}
impl From<Criterion> for RankingRuleView {
    fn from(value: Criterion) -> Self {
        match value {
            Criterion::Words => RankingRuleView::Words,
            Criterion::Typo => RankingRuleView::Typo,
            Criterion::Proximity => RankingRuleView::Proximity,
            Criterion::Attribute => RankingRuleView::Attribute,
            Criterion::Sort => RankingRuleView::Sort,
            Criterion::Exactness => RankingRuleView::Exactness,
            Criterion::Asc(x) => RankingRuleView::Asc(x),
            Criterion::Desc(x) => RankingRuleView::Desc(x),
        }
    }
}
impl From<RankingRuleView> for Criterion {
    fn from(value: RankingRuleView) -> Self {
        match value {
            RankingRuleView::Words => Criterion::Words,
            RankingRuleView::Typo => Criterion::Typo,
            RankingRuleView::Proximity => Criterion::Proximity,
            RankingRuleView::Attribute => Criterion::Attribute,
            RankingRuleView::Sort => Criterion::Sort,
            RankingRuleView::Exactness => Criterion::Exactness,
            RankingRuleView::Asc(x) => Criterion::Asc(x),
            RankingRuleView::Desc(x) => Criterion::Desc(x),
        }
    }
}

#[derive(Default, Debug, Clone, Copy, PartialEq, Eq, Deserr, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
#[deserr(error = DeserrJsonError<InvalidSettingsProximityPrecision>, rename_all = camelCase, deny_unknown_fields)]
pub enum ProximityPrecisionView {
    #[default]
    ByWord,
    ByAttribute,
}

impl From<ProximityPrecision> for ProximityPrecisionView {
    fn from(value: ProximityPrecision) -> Self {
        match value {
            ProximityPrecision::ByWord => ProximityPrecisionView::ByWord,
            ProximityPrecision::ByAttribute => ProximityPrecisionView::ByAttribute,
        }
    }
}
impl From<ProximityPrecisionView> for ProximityPrecision {
    fn from(value: ProximityPrecisionView) -> Self {
        match value {
            ProximityPrecisionView::ByWord => ProximityPrecision::ByWord,
            ProximityPrecisionView::ByAttribute => ProximityPrecision::ByAttribute,
        }
    }
}

#[derive(Debug, Clone, Default, Deserialize, PartialEq, Eq)]
pub struct WildcardSetting(Setting<Vec<String>>);

impl WildcardSetting {
    pub fn or(self, other: Self) -> Self {
        Self(self.0.or(other.0))
    }
}

impl From<Setting<Vec<String>>> for WildcardSetting {
    fn from(setting: Setting<Vec<String>>) -> Self {
        Self(setting)
    }
}

impl Serialize for WildcardSetting {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serialize_with_wildcard(&self.0, serializer)
    }
}

impl<E: deserr::DeserializeError> Deserr<E> for WildcardSetting {
    fn deserialize_from_value<V: deserr::IntoValue>(
        value: deserr::Value<V>,
        location: ValuePointerRef<'_>,
    ) -> Result<Self, E> {
        Ok(Self(Setting::deserialize_from_value(value, location)?))
    }
}

impl std::ops::Deref for WildcardSetting {
    type Target = Setting<Vec<String>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Default, Debug, Clone, Copy, PartialEq, Eq, Deserr, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
#[schema(rename_all = "camelCase")]
#[deserr(error = DeserrJsonError<InvalidSettingsPrefixSearch>, rename_all = camelCase, deny_unknown_fields)]
pub enum PrefixSearchSettings {
    #[default]
    IndexingTime,
    Disabled,
}

impl From<PrefixSearch> for PrefixSearchSettings {
    fn from(value: PrefixSearch) -> Self {
        match value {
            PrefixSearch::IndexingTime => PrefixSearchSettings::IndexingTime,
            PrefixSearch::Disabled => PrefixSearchSettings::Disabled,
        }
    }
}
impl From<PrefixSearchSettings> for PrefixSearch {
    fn from(value: PrefixSearchSettings) -> Self {
        match value {
            PrefixSearchSettings::IndexingTime => PrefixSearch::IndexingTime,
            PrefixSearchSettings::Disabled => PrefixSearch::Disabled,
        }
    }
}

#[cfg(test)]
pub(crate) mod test {
    use super::*;

    #[test]
    fn test_setting_check() {
        // test no changes
        let settings = Settings {
            displayed_attributes: Setting::Set(vec![String::from("hello")]).into(),
            searchable_attributes: Setting::Set(vec![String::from("hello")]).into(),
            filterable_attributes: Setting::NotSet,
            sortable_attributes: Setting::NotSet,
            ranking_rules: Setting::NotSet,
            stop_words: Setting::NotSet,
            non_separator_tokens: Setting::NotSet,
            separator_tokens: Setting::NotSet,
            dictionary: Setting::NotSet,
            synonyms: Setting::NotSet,
            distinct_attribute: Setting::NotSet,
            proximity_precision: Setting::NotSet,
            typo_tolerance: Setting::NotSet,
            faceting: Setting::NotSet,
            pagination: Setting::NotSet,
            embedders: Setting::NotSet,
            localized_attributes: Setting::NotSet,
            search_cutoff_ms: Setting::NotSet,
            facet_search: Setting::NotSet,
            prefix_search: Setting::NotSet,
            _kind: PhantomData::<Unchecked>,
        };

        let checked = settings.clone().check();
        assert_eq!(settings.displayed_attributes, checked.displayed_attributes);
        assert_eq!(settings.searchable_attributes, checked.searchable_attributes);

        // test wildcard
        // test no changes
        let settings = Settings {
            displayed_attributes: Setting::Set(vec![String::from("*")]).into(),
            searchable_attributes: Setting::Set(vec![String::from("hello"), String::from("*")])
                .into(),
            filterable_attributes: Setting::NotSet,
            sortable_attributes: Setting::NotSet,
            ranking_rules: Setting::NotSet,
            stop_words: Setting::NotSet,
            non_separator_tokens: Setting::NotSet,
            separator_tokens: Setting::NotSet,
            dictionary: Setting::NotSet,
            synonyms: Setting::NotSet,
            distinct_attribute: Setting::NotSet,
            proximity_precision: Setting::NotSet,
            typo_tolerance: Setting::NotSet,
            faceting: Setting::NotSet,
            pagination: Setting::NotSet,
            embedders: Setting::NotSet,
            localized_attributes: Setting::NotSet,
            search_cutoff_ms: Setting::NotSet,
            facet_search: Setting::NotSet,
            prefix_search: Setting::NotSet,
            _kind: PhantomData::<Unchecked>,
        };

        let checked = settings.check();
        assert_eq!(checked.displayed_attributes, Setting::Reset.into());
        assert_eq!(checked.searchable_attributes, Setting::Reset.into());
    }
}
