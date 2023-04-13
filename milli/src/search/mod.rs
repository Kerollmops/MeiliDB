use std::borrow::Cow;
use std::collections::hash_map::{Entry, HashMap};
use std::fmt;
use std::mem::take;
use std::result::Result as StdResult;
use std::str::Utf8Error;
use std::time::Instant;

use charabia::TokenizerBuilder;
use distinct::{Distinct, DocIter, FacetDistinct, NoopDistinct};
use fst::automaton::Str;
use fst::{Automaton, IntoStreamer, Streamer};
use levenshtein_automata::{LevenshteinAutomatonBuilder as LevBuilder, DFA};
use log::debug;
use once_cell::sync::Lazy;
use roaring::bitmap::RoaringBitmap;

pub use self::facet::{FacetDistribution, Filter, DEFAULT_VALUES_PER_FACET};
use self::fst_utils::{Complement, Intersection, StartsWith, Union};
pub use self::matches::{
    FormatOptions, MatchBounds, Matcher, MatcherBuilder, MatchingWord, MatchingWords,
};
use self::query_tree::QueryTreeBuilder;
use crate::error::UserError;
use crate::heed_codec::facet::{FacetGroupKey, FacetGroupValue};
use crate::index::db_name::EXACT_WORD_DOCIDS;
use crate::search::criteria::r#final::{Final, FinalResult};
use crate::search::criteria::InitialCandidates;
use crate::{fields_ids_map, AscDesc, Criterion, DocumentId, Index, Member, Result, BEU16};

// Building these factories is not free.
static LEVDIST0: Lazy<LevBuilder> = Lazy::new(|| LevBuilder::new(0, true));
static LEVDIST1: Lazy<LevBuilder> = Lazy::new(|| LevBuilder::new(1, true));
static LEVDIST2: Lazy<LevBuilder> = Lazy::new(|| LevBuilder::new(2, true));

/// The maximum number of facets returned by the facet search route.
const MAX_NUMBER_OF_FACETS: usize = 1000;

mod criteria;
mod distinct;
pub mod facet;
mod fst_utils;
mod matches;
mod query_tree;

pub struct Search<'a> {
    query: Option<String>,
    // this should be linked to the String in the query
    filter: Option<Filter<'a>>,
    offset: usize,
    limit: usize,
    sort_criteria: Option<Vec<AscDesc>>,
    terms_matching_strategy: TermsMatchingStrategy,
    authorize_typos: bool,
    words_limit: usize,
    exhaustive_number_hits: bool,
    criterion_implementation_strategy: CriterionImplementationStrategy,
    rtxn: &'a heed::RoTxn<'a>,
    index: &'a Index,
}

impl<'a> Search<'a> {
    pub fn new(rtxn: &'a heed::RoTxn, index: &'a Index) -> Search<'a> {
        Search {
            query: None,
            filter: None,
            offset: 0,
            limit: 20,
            sort_criteria: None,
            terms_matching_strategy: TermsMatchingStrategy::default(),
            authorize_typos: true,
            exhaustive_number_hits: false,
            words_limit: 10,
            criterion_implementation_strategy: CriterionImplementationStrategy::default(),
            rtxn,
            index,
        }
    }

    pub fn query(&mut self, query: impl Into<String>) -> &mut Search<'a> {
        self.query = Some(query.into());
        self
    }

    pub fn offset(&mut self, offset: usize) -> &mut Search<'a> {
        self.offset = offset;
        self
    }

    pub fn limit(&mut self, limit: usize) -> &mut Search<'a> {
        self.limit = limit;
        self
    }

    pub fn sort_criteria(&mut self, criteria: Vec<AscDesc>) -> &mut Search<'a> {
        self.sort_criteria = Some(criteria);
        self
    }

    pub fn terms_matching_strategy(&mut self, value: TermsMatchingStrategy) -> &mut Search<'a> {
        self.terms_matching_strategy = value;
        self
    }

    pub fn authorize_typos(&mut self, value: bool) -> &mut Search<'a> {
        self.authorize_typos = value;
        self
    }

    pub fn words_limit(&mut self, value: usize) -> &mut Search<'a> {
        self.words_limit = value;
        self
    }

    pub fn filter(&mut self, condition: Filter<'a>) -> &mut Search<'a> {
        self.filter = Some(condition);
        self
    }

    /// Force the search to exhastivelly compute the number of candidates,
    /// this will increase the search time but allows finite pagination.
    pub fn exhaustive_number_hits(&mut self, exhaustive_number_hits: bool) -> &mut Search<'a> {
        self.exhaustive_number_hits = exhaustive_number_hits;
        self
    }

    pub fn criterion_implementation_strategy(
        &mut self,
        strategy: CriterionImplementationStrategy,
    ) -> &mut Search<'a> {
        self.criterion_implementation_strategy = strategy;
        self
    }

    fn is_typo_authorized(&self) -> Result<bool> {
        let index_authorizes_typos = self.index.authorize_typos(self.rtxn)?;
        // only authorize typos if both the index and the query allow it.
        Ok(self.authorize_typos && index_authorizes_typos)
    }

    pub fn execute(&self) -> Result<SearchResult> {
        // We create the query tree by spliting the query into tokens.
        let before = Instant::now();
        let (query_tree, primitive_query, matching_words) = match self.query.as_ref() {
            Some(query) => {
                let mut builder = QueryTreeBuilder::new(self.rtxn, self.index)?;
                builder.terms_matching_strategy(self.terms_matching_strategy);

                builder.authorize_typos(self.is_typo_authorized()?);

                builder.words_limit(self.words_limit);
                // We make sure that the analyzer is aware of the stop words
                // this ensures that the query builder is able to properly remove them.
                let mut tokbuilder = TokenizerBuilder::new();
                let stop_words = self.index.stop_words(self.rtxn)?;
                if let Some(ref stop_words) = stop_words {
                    tokbuilder.stop_words(stop_words);
                }

                let script_lang_map = self.index.script_language(self.rtxn)?;
                if !script_lang_map.is_empty() {
                    tokbuilder.allow_list(&script_lang_map);
                }

                let tokenizer = tokbuilder.build();
                let tokens = tokenizer.tokenize(query);
                builder
                    .build(tokens)?
                    .map_or((None, None, None), |(qt, pq, mw)| (Some(qt), Some(pq), Some(mw)))
            }
            None => (None, None, None),
        };

        debug!("query tree: {:?} took {:.02?}", query_tree, before.elapsed());

        // We create the original candidates with the facet conditions results.
        let before = Instant::now();
        let filtered_candidates = match &self.filter {
            Some(condition) => Some(condition.evaluate(self.rtxn, self.index)?),
            None => None,
        };

        debug!("facet candidates: {:?} took {:.02?}", filtered_candidates, before.elapsed());

        // We check that we are allowed to use the sort criteria, we check
        // that they are declared in the sortable fields.
        if let Some(sort_criteria) = &self.sort_criteria {
            let sortable_fields = self.index.sortable_fields(self.rtxn)?;
            for asc_desc in sort_criteria {
                match asc_desc.member() {
                    Member::Field(ref field) if !crate::is_faceted(field, &sortable_fields) => {
                        return Err(UserError::InvalidSortableAttribute {
                            field: field.to_string(),
                            valid_fields: sortable_fields.into_iter().collect(),
                        })?
                    }
                    Member::Geo(_) if !sortable_fields.contains("_geo") => {
                        return Err(UserError::InvalidSortableAttribute {
                            field: "_geo".to_string(),
                            valid_fields: sortable_fields.into_iter().collect(),
                        })?
                    }
                    _ => (),
                }
            }
        }

        // We check that the sort ranking rule exists and throw an
        // error if we try to use it and that it doesn't.
        let sort_ranking_rule_missing = !self.index.criteria(self.rtxn)?.contains(&Criterion::Sort);
        let empty_sort_criteria = self.sort_criteria.as_ref().map_or(true, |s| s.is_empty());
        if sort_ranking_rule_missing && !empty_sort_criteria {
            return Err(UserError::SortRankingRuleMissing.into());
        }

        let criteria_builder = criteria::CriteriaBuilder::new(self.rtxn, self.index)?;

        match self.index.distinct_field(self.rtxn)? {
            None => {
                let criteria = criteria_builder.build::<NoopDistinct>(
                    query_tree,
                    primitive_query,
                    filtered_candidates,
                    self.sort_criteria.clone(),
                    self.exhaustive_number_hits,
                    None,
                    self.criterion_implementation_strategy,
                )?;
                self.perform_sort(NoopDistinct, matching_words.unwrap_or_default(), criteria)
            }
            Some(name) => {
                let field_ids_map = self.index.fields_ids_map(self.rtxn)?;
                match field_ids_map.id(name) {
                    Some(fid) => {
                        let distinct = FacetDistinct::new(fid, self.index, self.rtxn);

                        let criteria = criteria_builder.build(
                            query_tree,
                            primitive_query,
                            filtered_candidates,
                            self.sort_criteria.clone(),
                            self.exhaustive_number_hits,
                            Some(distinct.clone()),
                            self.criterion_implementation_strategy,
                        )?;
                        self.perform_sort(distinct, matching_words.unwrap_or_default(), criteria)
                    }
                    None => Ok(SearchResult::default()),
                }
            }
        }
    }

    fn perform_sort<D: Distinct>(
        &self,
        mut distinct: D,
        matching_words: MatchingWords,
        mut criteria: Final,
    ) -> Result<SearchResult> {
        let mut offset = self.offset;
        let mut initial_candidates = InitialCandidates::Estimated(RoaringBitmap::new());
        let mut excluded_candidates = self.index.soft_deleted_documents_ids(self.rtxn)?;
        let mut documents_ids = Vec::new();

        while let Some(FinalResult { candidates, initial_candidates: ic, .. }) =
            criteria.next(&excluded_candidates)?
        {
            debug!("Number of candidates found {}", candidates.len());

            let excluded = take(&mut excluded_candidates);
            let mut candidates = distinct.distinct(candidates, excluded);

            initial_candidates |= ic;

            if offset != 0 {
                let discarded = candidates.by_ref().take(offset).count();
                offset = offset.saturating_sub(discarded);
            }

            for candidate in candidates.by_ref().take(self.limit - documents_ids.len()) {
                documents_ids.push(candidate?);
            }

            excluded_candidates |= candidates.into_excluded();

            if documents_ids.len() == self.limit {
                break;
            }
        }

        initial_candidates.map_inplace(|c| c - excluded_candidates);

        Ok(SearchResult {
            matching_words,
            candidates: initial_candidates.into_inner(),
            documents_ids,
        })
    }
}

impl fmt::Debug for Search<'_> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let Search {
            query,
            filter,
            offset,
            limit,
            sort_criteria,
            terms_matching_strategy,
            authorize_typos,
            words_limit,
            exhaustive_number_hits,
            criterion_implementation_strategy,
            rtxn: _,
            index: _,
        } = self;
        f.debug_struct("Search")
            .field("query", query)
            .field("filter", filter)
            .field("offset", offset)
            .field("limit", limit)
            .field("sort_criteria", sort_criteria)
            .field("terms_matching_strategy", terms_matching_strategy)
            .field("authorize_typos", authorize_typos)
            .field("exhaustive_number_hits", exhaustive_number_hits)
            .field("criterion_implementation_strategy", criterion_implementation_strategy)
            .field("words_limit", words_limit)
            .finish()
    }
}

#[derive(Default, Debug)]
pub struct SearchResult {
    pub matching_words: MatchingWords,
    pub candidates: RoaringBitmap,
    // TODO those documents ids should be associated with their criteria scores.
    pub documents_ids: Vec<DocumentId>,
}

#[derive(Debug, Default, Clone, Copy)]
pub enum CriterionImplementationStrategy {
    OnlyIterative,
    OnlySetBased,
    #[default]
    Dynamic,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TermsMatchingStrategy {
    // remove last word first
    Last,
    // remove first word first
    First,
    // remove more frequent word first
    Frequency,
    // remove smallest word first
    Size,
    // only one of the word is mandatory
    Any,
    // all words are mandatory
    All,
}

impl Default for TermsMatchingStrategy {
    fn default() -> Self {
        Self::Last
    }
}

pub type WordDerivationsCache = HashMap<(String, bool, u8), Vec<(String, u8)>>;

pub fn word_derivations<'c>(
    word: &str,
    is_prefix: bool,
    max_typo: u8,
    fst: &fst::Set<Cow<[u8]>>,
    cache: &'c mut WordDerivationsCache,
) -> StdResult<&'c [(String, u8)], Utf8Error> {
    match cache.entry((word.to_string(), is_prefix, max_typo)) {
        Entry::Occupied(entry) => Ok(entry.into_mut()),
        Entry::Vacant(entry) => {
            let mut derived_words = Vec::new();
            if max_typo == 0 {
                if is_prefix {
                    let prefix = Str::new(word).starts_with();
                    let mut stream = fst.search(prefix).into_stream();

                    while let Some(word) = stream.next() {
                        let word = std::str::from_utf8(word)?;
                        derived_words.push((word.to_string(), 0));
                    }
                } else if fst.contains(word) {
                    derived_words.push((word.to_string(), 0));
                }
            } else if max_typo == 1 {
                let dfa = build_dfa(word, 1, is_prefix);
                let starts = StartsWith(Str::new(get_first(word)));
                let mut stream = fst.search_with_state(Intersection(starts, &dfa)).into_stream();

                while let Some((word, state)) = stream.next() {
                    let word = std::str::from_utf8(word)?;
                    let d = dfa.distance(state.1);
                    derived_words.push((word.to_string(), d.to_u8()));
                }
            } else {
                let starts = StartsWith(Str::new(get_first(word)));
                let first = Intersection(build_dfa(word, 1, is_prefix), Complement(&starts));
                let second_dfa = build_dfa(word, 2, is_prefix);
                let second = Intersection(&second_dfa, &starts);
                let automaton = Union(first, &second);

                let mut stream = fst.search_with_state(automaton).into_stream();

                while let Some((found_word, state)) = stream.next() {
                    let found_word = std::str::from_utf8(found_word)?;
                    // in the case the typo is on the first letter, we know the number of typo
                    // is two
                    if get_first(found_word) != get_first(word) {
                        derived_words.push((found_word.to_string(), 2));
                    } else {
                        // Else, we know that it is the second dfa that matched and compute the
                        // correct distance
                        let d = second_dfa.distance((state.1).0);
                        derived_words.push((found_word.to_string(), d.to_u8()));
                    }
                }
            }
            Ok(entry.insert(derived_words))
        }
    }
}

fn get_first(s: &str) -> &str {
    match s.chars().next() {
        Some(c) => &s[..c.len_utf8()],
        None => panic!("unexpected empty query"),
    }
}

pub fn build_dfa(word: &str, typos: u8, is_prefix: bool) -> DFA {
    let lev = match typos {
        0 => &LEVDIST0,
        1 => &LEVDIST1,
        _ => &LEVDIST2,
    };

    if is_prefix {
        lev.build_prefix_dfa(word)
    } else {
        lev.build_dfa(word)
    }
}

pub struct SearchForFacetValue<'a> {
    query: Option<String>,
    facet: String,
    search_query: Search<'a>,
}

impl<'a> SearchForFacetValue<'a> {
    fn new(facet: String, search_query: Search<'a>) -> SearchForFacetValue<'a> {
        SearchForFacetValue { query: None, facet, search_query }
    }

    fn query(&mut self, query: impl Into<String>) -> &mut Self {
        self.query = Some(query.into());
        self
    }

    fn execute(&self) -> Result<Vec<FacetSearchResult>> {
        let index = self.search_query.index;
        let rtxn = self.search_query.rtxn;

        let sortable_fields = index.sortable_fields(rtxn)?;
        if !sortable_fields.contains(&self.facet) {
            // TODO create a new type of error
            return Err(UserError::InvalidSortableAttribute {
                field: self.facet.clone(),
                valid_fields: sortable_fields.into_iter().collect(),
            })?;
        }

        let fields_ids_map = index.fields_ids_map(rtxn)?;
        let (field_id, fst) = match fields_ids_map.id(&self.facet) {
            Some(fid) => {
                match self.search_query.index.facet_id_string_fst.get(rtxn, &BEU16::new(fid))? {
                    Some(fst) => (fid, fst),
                    None => todo!("return an error, is the user trying to search in numbers?"),
                }
            }
            None => todo!("return an internal error bug"),
        };

        let search_candidates = self.search_query.execute()?.candidates;

        match self.query.as_ref() {
            Some(query) => {
                let is_prefix = true;
                let starts = StartsWith(Str::new(get_first(query)));
                let first = Intersection(build_dfa(query, 1, is_prefix), Complement(&starts));
                let second_dfa = build_dfa(query, 2, is_prefix);
                let second = Intersection(&second_dfa, &starts);
                let automaton = Union(first, &second);

                let mut stream = fst.search(automaton).into_stream();
                let mut result = vec![];
                let mut length = 0;
                while let Some(facet_value) = stream.next() {
                    let value = std::str::from_utf8(facet_value)?;
                    let key = FacetGroupKey { field_id, level: 0, left_bound: value };
                    let docids = match index.facet_id_string_docids.get(rtxn, &key)? {
                        Some(FacetGroupValue { bitmap, .. }) => bitmap,
                        None => todo!("return an internal error"),
                    };
                    let count = search_candidates.intersection_len(&docids);
                    if count != 0 {
                        result.push(FacetSearchResult { value: value.to_string(), count });
                        length += 1;
                    }
                    if length >= MAX_NUMBER_OF_FACETS {
                        break;
                    }
                }

                Ok(result)
            }
            None => {
                let mut stream = fst.stream();
                let mut result = vec![];
                let mut length = 0;
                while let Some(facet_value) = stream.next() {
                    let value = std::str::from_utf8(facet_value)?;
                    let key = FacetGroupKey { field_id, level: 0, left_bound: value };
                    let docids = match index.facet_id_string_docids.get(rtxn, &key)? {
                        Some(FacetGroupValue { bitmap, .. }) => bitmap,
                        None => todo!("return an internal error"),
                    };
                    let count = search_candidates.intersection_len(&docids);
                    if count != 0 {
                        result.push(FacetSearchResult { value: value.to_string(), count });
                        length += 1;
                    }
                    if length >= MAX_NUMBER_OF_FACETS {
                        break;
                    }
                }

                Ok(result)
            }
        }
    }
}

pub struct FacetSearchResult {
    /// The original facet value
    pub value: String,
    /// The number of documents associated to this facet
    pub count: u64,
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::index::tests::TempIndex;

    #[cfg(feature = "default")]
    #[test]
    fn test_kanji_language_detection() {
        let index = TempIndex::new();

        index
            .add_documents(documents!([
                { "id": 0, "title": "The quick (\"brown\") fox can't jump 32.3 feet, right? Brr, it's 29.3°F!" },
                { "id": 1, "title": "東京のお寿司。" },
                { "id": 2, "title": "הַשּׁוּעָל הַמָּהִיר (״הַחוּם״) לֹא יָכוֹל לִקְפֹּץ 9.94 מֶטְרִים, נָכוֹן? ברר, 1.5°C- בַּחוּץ!" }
            ]))
            .unwrap();

        let txn = index.write_txn().unwrap();
        let mut search = Search::new(&txn, &index);

        search.query("東京");
        let SearchResult { documents_ids, .. } = search.execute().unwrap();

        assert_eq!(documents_ids, vec![1]);
    }

    #[test]
    fn test_is_authorized_typos() {
        let index = TempIndex::new();
        let mut txn = index.write_txn().unwrap();

        let mut search = Search::new(&txn, &index);

        // default is authorized
        assert!(search.is_typo_authorized().unwrap());

        search.authorize_typos(false);
        assert!(!search.is_typo_authorized().unwrap());

        index.put_authorize_typos(&mut txn, false).unwrap();
        txn.commit().unwrap();

        let txn = index.read_txn().unwrap();
        let mut search = Search::new(&txn, &index);

        assert!(!search.is_typo_authorized().unwrap());

        search.authorize_typos(true);
        assert!(!search.is_typo_authorized().unwrap());
    }

    #[test]
    fn test_one_typos_tolerance() {
        let fst = fst::Set::from_iter(["zealand"].iter()).unwrap().map_data(Cow::Owned).unwrap();
        let mut cache = HashMap::new();
        let found = word_derivations("zealend", false, 1, &fst, &mut cache).unwrap();

        assert_eq!(found, &[("zealand".to_string(), 1)]);
    }

    #[test]
    fn test_one_typos_first_letter() {
        let fst = fst::Set::from_iter(["zealand"].iter()).unwrap().map_data(Cow::Owned).unwrap();
        let mut cache = HashMap::new();
        let found = word_derivations("sealand", false, 1, &fst, &mut cache).unwrap();

        assert_eq!(found, &[]);
    }

    #[test]
    fn test_two_typos_tolerance() {
        let fst = fst::Set::from_iter(["zealand"].iter()).unwrap().map_data(Cow::Owned).unwrap();
        let mut cache = HashMap::new();
        let found = word_derivations("zealemd", false, 2, &fst, &mut cache).unwrap();

        assert_eq!(found, &[("zealand".to_string(), 2)]);
    }

    #[test]
    fn test_two_typos_first_letter() {
        let fst = fst::Set::from_iter(["zealand"].iter()).unwrap().map_data(Cow::Owned).unwrap();
        let mut cache = HashMap::new();
        let found = word_derivations("sealand", false, 2, &fst, &mut cache).unwrap();

        assert_eq!(found, &[("zealand".to_string(), 2)]);
    }

    #[test]
    fn test_prefix() {
        let fst = fst::Set::from_iter(["zealand"].iter()).unwrap().map_data(Cow::Owned).unwrap();
        let mut cache = HashMap::new();
        let found = word_derivations("ze", true, 0, &fst, &mut cache).unwrap();

        assert_eq!(found, &[("zealand".to_string(), 0)]);
    }

    #[test]
    fn test_bad_prefix() {
        let fst = fst::Set::from_iter(["zealand"].iter()).unwrap().map_data(Cow::Owned).unwrap();
        let mut cache = HashMap::new();
        let found = word_derivations("se", true, 0, &fst, &mut cache).unwrap();

        assert_eq!(found, &[]);
    }

    #[test]
    fn test_prefix_with_typo() {
        let fst = fst::Set::from_iter(["zealand"].iter()).unwrap().map_data(Cow::Owned).unwrap();
        let mut cache = HashMap::new();
        let found = word_derivations("zae", true, 1, &fst, &mut cache).unwrap();

        assert_eq!(found, &[("zealand".to_string(), 1)]);
    }
}
