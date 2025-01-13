mod bucket_sort;
mod db_cache;
mod distinct;
mod geo_sort;
mod graph_based_ranking_rule;
mod interner;
mod limits;
mod logger;
pub mod matches;
mod query_graph;
mod query_term;
mod ranking_rule_graph;
mod ranking_rules;
mod resolve_query_graph;
mod small_bitmap;

mod exact_attribute;
mod sort;
mod vector_sort;

#[cfg(test)]
mod tests;

use std::collections::HashSet;

use bucket_sort::{bucket_sort, BucketSortOutput};
use charabia::{Language, TokenizerBuilder};
use db_cache::DatabaseCache;
use exact_attribute::ExactAttribute;
use graph_based_ranking_rule::{Exactness, Fid, Position, Proximity, Typo};
use heed::RoTxn;
use interner::{DedupInterner, Interner};
pub use logger::visual::VisualSearchLogger;
pub use logger::{DefaultSearchLogger, SearchLogger};
use query_graph::{QueryGraph, QueryNode};
use query_term::{
    located_query_terms_from_tokens, ExtractedTokens, LocatedQueryTerm, Phrase, QueryTerm,
};
use ranking_rules::{
    BoxRankingRule, PlaceholderQuery, RankingRule, RankingRuleOutput, RankingRuleQueryTrait,
};
use resolve_query_graph::{compute_query_graph_docids, PhraseDocIdsCache};
use roaring::RoaringBitmap;
use sort::Sort;

use self::distinct::facet_string_values;
use self::geo_sort::GeoSort;
pub use self::geo_sort::Strategy as GeoSortStrategy;
use self::graph_based_ranking_rule::Words;
use self::interner::Interned;
use self::vector_sort::VectorSort;
use crate::localized_attributes_rules::LocalizedFieldIds;
use crate::score_details::{ScoreDetails, ScoringStrategy};
use crate::search::new::distinct::apply_distinct_rule;
use crate::vector::Embedder;
use crate::{
    AscDesc, DocumentId, FieldId, Filter, Index, Member, Result, TermsMatchingStrategy, TimeBudget,
    UserError, Weight,
};

/// A structure used throughout the execution of a search query.
pub struct SearchContext<'ctx> {
    pub index: &'ctx Index,
    pub txn: &'ctx RoTxn<'ctx>,
    pub db_cache: DatabaseCache<'ctx>,
    pub word_interner: DedupInterner<String>,
    pub phrase_interner: DedupInterner<Phrase>,
    pub term_interner: Interner<QueryTerm>,
    pub phrase_docids: PhraseDocIdsCache,
    pub restricted_fids: Option<RestrictedFids>,
}

impl<'ctx> SearchContext<'ctx> {
    pub fn new(index: &'ctx Index, txn: &'ctx RoTxn<'ctx>) -> Result<Self> {
        let searchable_fids = index.searchable_fields_and_weights(txn)?;
        let exact_attributes_ids = index.exact_attributes_ids(txn)?;

        let mut exact = Vec::new();
        let mut tolerant = Vec::new();
        for (_name, fid, weight) in searchable_fids {
            if exact_attributes_ids.contains(&fid) {
                exact.push((fid, weight));
            } else {
                tolerant.push((fid, weight));
            }
        }

        Ok(Self {
            index,
            txn,
            db_cache: <_>::default(),
            word_interner: <_>::default(),
            phrase_interner: <_>::default(),
            term_interner: <_>::default(),
            phrase_docids: <_>::default(),
            restricted_fids: None,
        })
    }

    pub fn attributes_to_search_on(
        &mut self,
        attributes_to_search_on: &'ctx [String],
    ) -> Result<()> {
        let user_defined_searchable = self.index.user_defined_searchable_fields(self.txn)?;
        let searchable_fields_weights = self.index.searchable_fields_and_weights(self.txn)?;
        let exact_attributes_ids = self.index.exact_attributes_ids(self.txn)?;

        let mut wildcard = false;

        let mut restricted_fids = RestrictedFids::default();
        for field_name in attributes_to_search_on {
            if field_name == "*" {
                wildcard = true;
                // we cannot early exit as we want to returns error in case of unknown fields
                continue;
            }
            let searchable_weight =
                searchable_fields_weights.iter().find(|(name, _, _)| name == field_name);
            let (fid, weight) = match searchable_weight {
                // The Field id exist and the field is searchable
                Some((_name, fid, weight)) => (*fid, *weight),
                // The field is not searchable but the user didn't define any searchable attributes
                None if user_defined_searchable.is_none() => continue,
                // The field is not searchable => User error
                None => {
                    let (valid_fields, hidden_fields) = self.index.remove_hidden_fields(
                        self.txn,
                        searchable_fields_weights.iter().map(|(name, _, _)| name),
                    )?;

                    let field = field_name.to_string();
                    return Err(UserError::InvalidSearchableAttribute {
                        field,
                        valid_fields,
                        hidden_fields,
                    }
                    .into());
                }
            };

            if exact_attributes_ids.contains(&fid) {
                restricted_fids.exact.push((fid, weight));
            } else {
                restricted_fids.tolerant.push((fid, weight));
            };
        }

        if wildcard {
            self.restricted_fids = None;
        } else {
            self.restricted_fids = Some(restricted_fids);
        }

        Ok(())
    }
}

#[derive(Clone, Copy, PartialEq, PartialOrd, Ord, Eq)]
pub enum Word {
    Original(Interned<String>),
    Derived(Interned<String>),
}

impl Word {
    pub fn interned(&self) -> Interned<String> {
        match self {
            Word::Original(word) => *word,
            Word::Derived(word) => *word,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct RestrictedFids {
    pub tolerant: Vec<(FieldId, Weight)>,
    pub exact: Vec<(FieldId, Weight)>,
}

impl RestrictedFids {
    pub fn contains(&self, fid: &FieldId) -> bool {
        self.tolerant.iter().any(|(id, _)| id == fid) || self.exact.iter().any(|(id, _)| id == fid)
    }
}

/// Apply the [`TermsMatchingStrategy`] to the query graph and resolve it.
fn resolve_maximally_reduced_query_graph(
    ctx: &mut SearchContext<'_>,
    universe: &RoaringBitmap,
    query_graph: &QueryGraph,
    matching_strategy: TermsMatchingStrategy,
    logger: &mut dyn SearchLogger<QueryGraph>,
) -> Result<RoaringBitmap> {
    let mut graph = query_graph.clone();

    let nodes_to_remove = match matching_strategy {
        TermsMatchingStrategy::Last => query_graph
            .removal_order_for_terms_matching_strategy_last(ctx)
            .iter()
            .flat_map(|x| x.iter())
            .collect(),
        TermsMatchingStrategy::Frequency => query_graph
            .removal_order_for_terms_matching_strategy_frequency(ctx)?
            .iter()
            .flat_map(|x| x.iter())
            .collect(),
        TermsMatchingStrategy::All => vec![],
    };
    graph.remove_nodes_keep_edges(&nodes_to_remove);

    logger.query_for_initial_universe(&graph);
    let docids = compute_query_graph_docids(ctx, &graph, universe)?;

    Ok(docids)
}

#[tracing::instrument(level = "trace", skip_all, target = "search::universe")]
fn resolve_universe(
    ctx: &mut SearchContext<'_>,
    initial_universe: &RoaringBitmap,
    query_graph: &QueryGraph,
    matching_strategy: TermsMatchingStrategy,
    logger: &mut dyn SearchLogger<QueryGraph>,
) -> Result<RoaringBitmap> {
    resolve_maximally_reduced_query_graph(
        ctx,
        initial_universe,
        query_graph,
        matching_strategy,
        logger,
    )
}

#[tracing::instrument(level = "trace", skip_all, target = "search::query")]
fn resolve_negative_words(
    ctx: &mut SearchContext<'_>,
    universe: Option<&RoaringBitmap>,
    negative_words: &[Word],
) -> Result<RoaringBitmap> {
    let mut negative_bitmap = RoaringBitmap::new();
    for &word in negative_words {
        if let Some(bitmap) = ctx.word_docids(universe, word)? {
            negative_bitmap |= bitmap;
        }
    }
    Ok(negative_bitmap)
}

#[tracing::instrument(level = "trace", skip_all, target = "search::query")]
fn resolve_negative_phrases(
    ctx: &mut SearchContext<'_>,
    negative_phrases: &[LocatedQueryTerm],
) -> Result<RoaringBitmap> {
    let mut negative_bitmap = RoaringBitmap::new();
    for term in negative_phrases {
        let query_term = ctx.term_interner.get(term.value);
        if let Some(phrase) = query_term.original_phrase() {
            negative_bitmap |= ctx.get_phrase_docids(phrase)?;
        }
    }
    Ok(negative_bitmap)
}

/// Return the list of initialised ranking rules to be used for a placeholder search.
fn get_ranking_rules_for_placeholder_search<'ctx>(
    ctx: &SearchContext<'ctx>,
    sort_criteria: &Option<Vec<AscDesc>>,
    geo_strategy: geo_sort::Strategy,
) -> Result<Vec<BoxRankingRule<'ctx, PlaceholderQuery>>> {
    let mut sort = false;
    let mut sorted_fields = HashSet::new();
    let mut geo_sorted = false;
    let mut ranking_rules: Vec<BoxRankingRule<'ctx, PlaceholderQuery>> = vec![];
    let settings_ranking_rules = ctx.index.criteria(ctx.txn)?;
    for rr in settings_ranking_rules {
        match rr {
            // These rules need a query to have an effect; ignore them in placeholder search
            crate::Criterion::Words
            | crate::Criterion::Typo
            | crate::Criterion::Attribute
            | crate::Criterion::Proximity
            | crate::Criterion::Exactness => continue,
            crate::Criterion::Sort => {
                if sort {
                    continue;
                }
                resolve_sort_criteria(
                    sort_criteria,
                    ctx,
                    &mut ranking_rules,
                    &mut sorted_fields,
                    &mut geo_sorted,
                    geo_strategy,
                )?;
                sort = true;
            }
            crate::Criterion::Asc(field_name) => {
                if sorted_fields.contains(&field_name) {
                    continue;
                }
                sorted_fields.insert(field_name.clone());
                ranking_rules.push(Box::new(Sort::new(ctx.index, ctx.txn, field_name, true)?));
            }
            crate::Criterion::Desc(field_name) => {
                if sorted_fields.contains(&field_name) {
                    continue;
                }
                sorted_fields.insert(field_name.clone());
                ranking_rules.push(Box::new(Sort::new(ctx.index, ctx.txn, field_name, false)?));
            }
        }
    }
    Ok(ranking_rules)
}

#[allow(clippy::too_many_arguments)]
fn get_ranking_rules_for_vector<'ctx>(
    ctx: &SearchContext<'ctx>,
    sort_criteria: &Option<Vec<AscDesc>>,
    geo_strategy: geo_sort::Strategy,
    limit_plus_offset: usize,
    target: &[f32],
    embedder_name: &str,
    embedder: &Embedder,
    quantized: bool,
) -> Result<Vec<BoxRankingRule<'ctx, PlaceholderQuery>>> {
    // query graph search

    let mut sort = false;
    let mut sorted_fields = HashSet::new();
    let mut geo_sorted = false;

    let mut vector = false;
    let mut ranking_rules: Vec<BoxRankingRule<'ctx, PlaceholderQuery>> = vec![];

    let settings_ranking_rules = ctx.index.criteria(ctx.txn)?;
    for rr in settings_ranking_rules {
        match rr {
            crate::Criterion::Words
            | crate::Criterion::Typo
            | crate::Criterion::Proximity
            | crate::Criterion::Attribute
            | crate::Criterion::Exactness => {
                if !vector {
                    let vector_candidates = ctx.index.documents_ids(ctx.txn)?;
                    let vector_sort = VectorSort::new(
                        ctx,
                        target.to_vec(),
                        vector_candidates,
                        limit_plus_offset,
                        embedder_name,
                        embedder,
                        quantized,
                    )?;
                    ranking_rules.push(Box::new(vector_sort));
                    vector = true;
                }
            }
            crate::Criterion::Sort => {
                if sort {
                    continue;
                }
                resolve_sort_criteria(
                    sort_criteria,
                    ctx,
                    &mut ranking_rules,
                    &mut sorted_fields,
                    &mut geo_sorted,
                    geo_strategy,
                )?;
                sort = true;
            }
            crate::Criterion::Asc(field_name) => {
                if sorted_fields.contains(&field_name) {
                    continue;
                }
                sorted_fields.insert(field_name.clone());
                ranking_rules.push(Box::new(Sort::new(ctx.index, ctx.txn, field_name, true)?));
            }
            crate::Criterion::Desc(field_name) => {
                if sorted_fields.contains(&field_name) {
                    continue;
                }
                sorted_fields.insert(field_name.clone());
                ranking_rules.push(Box::new(Sort::new(ctx.index, ctx.txn, field_name, false)?));
            }
        }
    }

    Ok(ranking_rules)
}

/// Return the list of initialised ranking rules to be used for a query graph search.
fn get_ranking_rules_for_query_graph_search<'ctx>(
    ctx: &SearchContext<'ctx>,
    sort_criteria: &Option<Vec<AscDesc>>,
    geo_strategy: geo_sort::Strategy,
    terms_matching_strategy: TermsMatchingStrategy,
) -> Result<Vec<BoxRankingRule<'ctx, QueryGraph>>> {
    // query graph search
    let mut words = false;
    let mut typo = false;
    let mut proximity = false;
    let mut sort = false;
    let mut attribute = false;
    let mut exactness = false;
    let mut sorted_fields = HashSet::new();
    let mut geo_sorted = false;

    // Don't add the `words` ranking rule if the term matching strategy is `All`
    if matches!(terms_matching_strategy, TermsMatchingStrategy::All) {
        words = true;
    }

    let mut ranking_rules: Vec<BoxRankingRule<'ctx, QueryGraph>> = vec![];
    let settings_ranking_rules = ctx.index.criteria(ctx.txn)?;
    for rr in settings_ranking_rules {
        // Add Words before any of: typo, proximity, attribute
        match rr {
            crate::Criterion::Typo
            | crate::Criterion::Attribute
            | crate::Criterion::Proximity
            | crate::Criterion::Exactness => {
                if !words {
                    ranking_rules.push(Box::new(Words::new(terms_matching_strategy)));
                    words = true;
                }
            }
            _ => {}
        }
        match rr {
            crate::Criterion::Words => {
                if words {
                    continue;
                }
                ranking_rules.push(Box::new(Words::new(terms_matching_strategy)));
                words = true;
            }
            crate::Criterion::Typo => {
                if typo {
                    continue;
                }
                typo = true;
                ranking_rules.push(Box::new(Typo::new(None)));
            }
            crate::Criterion::Proximity => {
                if proximity {
                    continue;
                }
                proximity = true;
                ranking_rules.push(Box::new(Proximity::new(None)));
            }
            crate::Criterion::Attribute => {
                if attribute {
                    continue;
                }
                attribute = true;
                ranking_rules.push(Box::new(Fid::new(None)));
                ranking_rules.push(Box::new(Position::new(None)));
            }
            crate::Criterion::Sort => {
                if sort {
                    continue;
                }
                resolve_sort_criteria(
                    sort_criteria,
                    ctx,
                    &mut ranking_rules,
                    &mut sorted_fields,
                    &mut geo_sorted,
                    geo_strategy,
                )?;
                sort = true;
            }
            crate::Criterion::Exactness => {
                if exactness {
                    continue;
                }
                ranking_rules.push(Box::new(ExactAttribute::new()));
                ranking_rules.push(Box::new(Exactness::new()));
                exactness = true;
            }
            crate::Criterion::Asc(field_name) => {
                if sorted_fields.contains(&field_name) {
                    continue;
                }
                sorted_fields.insert(field_name.clone());
                ranking_rules.push(Box::new(Sort::new(ctx.index, ctx.txn, field_name, true)?));
            }
            crate::Criterion::Desc(field_name) => {
                if sorted_fields.contains(&field_name) {
                    continue;
                }
                sorted_fields.insert(field_name.clone());
                ranking_rules.push(Box::new(Sort::new(ctx.index, ctx.txn, field_name, false)?));
            }
        }
    }
    Ok(ranking_rules)
}

fn resolve_sort_criteria<'ctx, Query: RankingRuleQueryTrait>(
    sort_criteria: &Option<Vec<AscDesc>>,
    ctx: &SearchContext<'ctx>,
    ranking_rules: &mut Vec<BoxRankingRule<'ctx, Query>>,
    sorted_fields: &mut HashSet<String>,
    geo_sorted: &mut bool,
    geo_strategy: geo_sort::Strategy,
) -> Result<()> {
    let sort_criteria = sort_criteria.clone().unwrap_or_default();
    ranking_rules.reserve(sort_criteria.len());
    for criterion in sort_criteria {
        match criterion {
            AscDesc::Asc(Member::Field(field_name)) => {
                if sorted_fields.contains(&field_name) {
                    continue;
                }
                sorted_fields.insert(field_name.clone());
                ranking_rules.push(Box::new(Sort::new(ctx.index, ctx.txn, field_name, true)?));
            }
            AscDesc::Desc(Member::Field(field_name)) => {
                if sorted_fields.contains(&field_name) {
                    continue;
                }
                sorted_fields.insert(field_name.clone());
                ranking_rules.push(Box::new(Sort::new(ctx.index, ctx.txn, field_name, false)?));
            }
            AscDesc::Asc(Member::Geo(point)) => {
                if *geo_sorted {
                    continue;
                }
                let geo_faceted_docids = ctx.index.geo_faceted_documents_ids(ctx.txn)?;
                ranking_rules.push(Box::new(GeoSort::new(
                    geo_strategy,
                    geo_faceted_docids,
                    point,
                    true,
                )?));
            }
            AscDesc::Desc(Member::Geo(point)) => {
                if *geo_sorted {
                    continue;
                }
                let geo_faceted_docids = ctx.index.geo_faceted_documents_ids(ctx.txn)?;
                ranking_rules.push(Box::new(GeoSort::new(
                    geo_strategy,
                    geo_faceted_docids,
                    point,
                    false,
                )?));
            }
        };
    }
    Ok(())
}

#[tracing::instrument(level = "debug", skip_all, target = "search::universe")]
pub fn filtered_universe(
    index: &Index,
    txn: &RoTxn<'_>,
    filters: &Option<Filter<'_>>,
) -> Result<RoaringBitmap> {
    Ok(if let Some(filters) = filters {
        filters.evaluate(txn, index)?
    } else {
        index.documents_ids(txn)?
    })
}

#[allow(clippy::too_many_arguments)]
pub fn execute_vector_search(
    ctx: &mut SearchContext<'_>,
    vector: &[f32],
    scoring_strategy: ScoringStrategy,
    universe: RoaringBitmap,
    sort_criteria: &Option<Vec<AscDesc>>,
    distinct: &Option<String>,
    geo_strategy: geo_sort::Strategy,
    from: usize,
    length: usize,
    embedder_name: &str,
    embedder: &Embedder,
    quantized: bool,
    time_budget: TimeBudget,
    ranking_score_threshold: Option<f64>,
) -> Result<PartialSearchResult> {
    check_sort_criteria(ctx, sort_criteria.as_ref())?;

    // FIXME: input universe = universe & documents_with_vectors
    // for now if we're computing embeddings for ALL documents, we can assume that this is just universe
    let ranking_rules = get_ranking_rules_for_vector(
        ctx,
        sort_criteria,
        geo_strategy,
        from + length,
        vector,
        embedder_name,
        embedder,
        quantized,
    )?;

    let mut placeholder_search_logger = logger::DefaultSearchLogger;
    let placeholder_search_logger: &mut dyn SearchLogger<PlaceholderQuery> =
        &mut placeholder_search_logger;

    let BucketSortOutput { docids, scores, all_candidates, degraded } = bucket_sort(
        ctx,
        ranking_rules,
        &PlaceholderQuery,
        distinct.as_deref(),
        &universe,
        from,
        length,
        scoring_strategy,
        placeholder_search_logger,
        time_budget,
        ranking_score_threshold,
    )?;

    Ok(PartialSearchResult {
        candidates: all_candidates,
        document_scores: scores,
        documents_ids: docids,
        located_query_terms: None,
        degraded,
        used_negative_operator: false,
    })
}

#[allow(clippy::too_many_arguments)]
#[tracing::instrument(level = "trace", skip_all, target = "search::main")]
pub fn execute_search(
    ctx: &mut SearchContext<'_>,
    query: Option<&str>,
    terms_matching_strategy: TermsMatchingStrategy,
    scoring_strategy: ScoringStrategy,
    exhaustive_number_hits: bool,
    mut universe: RoaringBitmap,
    sort_criteria: &Option<Vec<AscDesc>>,
    distinct: &Option<String>,
    geo_strategy: geo_sort::Strategy,
    from: usize,
    length: usize,
    words_limit: Option<usize>,
    placeholder_search_logger: &mut dyn SearchLogger<PlaceholderQuery>,
    query_graph_logger: &mut dyn SearchLogger<QueryGraph>,
    time_budget: TimeBudget,
    ranking_score_threshold: Option<f64>,
    locales: Option<&Vec<Language>>,
) -> Result<PartialSearchResult> {
    check_sort_criteria(ctx, sort_criteria.as_ref())?;

    let mut used_negative_operator = false;
    let mut located_query_terms = None;
    let query_terms = if let Some(query) = query {
        let span = tracing::trace_span!(target: "search::tokens", "tokenizer_builder");
        let entered = span.enter();

        // We make sure that the analyzer is aware of the stop words
        // this ensures that the query builder is able to properly remove them.
        let mut tokbuilder = TokenizerBuilder::new();
        let stop_words = ctx.index.stop_words(ctx.txn)?;
        if let Some(ref stop_words) = stop_words {
            tokbuilder.stop_words(stop_words);
        }

        let separators = ctx.index.allowed_separators(ctx.txn)?;
        let separators: Option<Vec<_>> =
            separators.as_ref().map(|x| x.iter().map(String::as_str).collect());
        if let Some(ref separators) = separators {
            tokbuilder.separators(separators);
        }

        let dictionary = ctx.index.dictionary(ctx.txn)?;
        let dictionary: Option<Vec<_>> =
            dictionary.as_ref().map(|x| x.iter().map(String::as_str).collect());
        if let Some(ref dictionary) = dictionary {
            tokbuilder.words_dict(dictionary);
        }

        let db_locales;
        match locales {
            Some(locales) => {
                if !locales.is_empty() {
                    tokbuilder.allow_list(locales);
                }
            }
            None => {
                // If no locales are specified, we use the locales specified in the localized attributes rules
                let localized_attributes_rules = ctx.index.localized_attributes_rules(ctx.txn)?;
                let fields_ids_map = ctx.index.fields_ids_map(ctx.txn)?;
                let searchable_fields = ctx.index.searchable_fields_ids(ctx.txn)?;

                let localized_fields = match &ctx.restricted_fids {
                    // if AttributeToSearchOn is set, use the restricted list of ids
                    Some(restricted_fids) => {
                        let iter = restricted_fids
                            .exact
                            .iter()
                            .chain(restricted_fids.tolerant.iter())
                            .map(|(fid, _)| *fid);

                        LocalizedFieldIds::new(&localized_attributes_rules, &fields_ids_map, iter)
                    }
                    // Otherwise use the full list of ids coming from the index searchable fields
                    None => LocalizedFieldIds::new(
                        &localized_attributes_rules,
                        &fields_ids_map,
                        searchable_fields.into_iter(),
                    ),
                };

                db_locales = localized_fields.all_locales();
                if !db_locales.is_empty() {
                    tokbuilder.allow_list(&db_locales);
                }
            }
        };

        let tokenizer = tokbuilder.build();
        drop(entered);

        let span = tracing::trace_span!(target: "search::tokens", "tokenize");
        let entered = span.enter();
        let tokens = tokenizer.tokenize(query);
        drop(entered);

        let ExtractedTokens { query_terms, negative_words, negative_phrases } =
            located_query_terms_from_tokens(ctx, tokens, words_limit)?;
        used_negative_operator = !negative_words.is_empty() || !negative_phrases.is_empty();

        let ignored_documents = resolve_negative_words(ctx, Some(&universe), &negative_words)?;
        let ignored_phrases = resolve_negative_phrases(ctx, &negative_phrases)?;

        universe -= ignored_documents;
        universe -= ignored_phrases;

        if query_terms.is_empty() {
            // Do a placeholder search instead
            None
        } else {
            Some(query_terms)
        }
    } else {
        None
    };

    let bucket_sort_output = if let Some(query_terms) = query_terms {
        let (graph, new_located_query_terms) = QueryGraph::from_query(ctx, &query_terms)?;
        located_query_terms = Some(new_located_query_terms);

        let ranking_rules = get_ranking_rules_for_query_graph_search(
            ctx,
            sort_criteria,
            geo_strategy,
            terms_matching_strategy,
        )?;

        universe &=
            resolve_universe(ctx, &universe, &graph, terms_matching_strategy, query_graph_logger)?;

        bucket_sort(
            ctx,
            ranking_rules,
            &graph,
            distinct.as_deref(),
            &universe,
            from,
            length,
            scoring_strategy,
            query_graph_logger,
            time_budget,
            ranking_score_threshold,
        )?
    } else {
        let ranking_rules =
            get_ranking_rules_for_placeholder_search(ctx, sort_criteria, geo_strategy)?;
        bucket_sort(
            ctx,
            ranking_rules,
            &PlaceholderQuery,
            distinct.as_deref(),
            &universe,
            from,
            length,
            scoring_strategy,
            placeholder_search_logger,
            time_budget,
            ranking_score_threshold,
        )?
    };

    let BucketSortOutput { docids, scores, mut all_candidates, degraded } = bucket_sort_output;
    let fields_ids_map = ctx.index.fields_ids_map(ctx.txn)?;

    // The candidates is the universe unless the exhaustive number of hits
    // is requested and a distinct attribute is set.
    if exhaustive_number_hits {
        let distinct_field = match distinct.as_deref() {
            Some(distinct) => Some(distinct),
            None => ctx.index.distinct_field(ctx.txn)?,
        };

        if let Some(f) = distinct_field {
            if let Some(distinct_fid) = fields_ids_map.id(f) {
                all_candidates = apply_distinct_rule(ctx, distinct_fid, &all_candidates)?.remaining;
            }
        }
    }

    Ok(PartialSearchResult {
        candidates: all_candidates,
        document_scores: scores,
        documents_ids: docids,
        located_query_terms,
        degraded,
        used_negative_operator,
    })
}

fn check_sort_criteria(
    ctx: &SearchContext<'_>,
    sort_criteria: Option<&Vec<AscDesc>>,
) -> Result<()> {
    let sort_criteria = if let Some(sort_criteria) = sort_criteria {
        sort_criteria
    } else {
        return Ok(());
    };

    if sort_criteria.is_empty() {
        return Ok(());
    }

    // We check that the sort ranking rule exists and throw an
    // error if we try to use it and that it doesn't.
    let sort_ranking_rule_missing = !ctx.index.criteria(ctx.txn)?.contains(&crate::Criterion::Sort);
    if sort_ranking_rule_missing {
        return Err(UserError::SortRankingRuleMissing.into());
    }

    // We check that we are allowed to use the sort criteria, we check
    // that they are declared in the sortable fields.
    let sortable_fields = ctx.index.sortable_fields(ctx.txn)?;
    for asc_desc in sort_criteria {
        match asc_desc.member() {
            Member::Field(ref field) if !crate::is_faceted(field, &sortable_fields) => {
                let (valid_fields, hidden_fields) =
                    ctx.index.remove_hidden_fields(ctx.txn, sortable_fields)?;

                return Err(UserError::InvalidSortableAttribute {
                    field: field.to_string(),
                    valid_fields,
                    hidden_fields,
                }
                .into());
            }
            Member::Geo(_) if !sortable_fields.contains("_geo") => {
                let (valid_fields, hidden_fields) =
                    ctx.index.remove_hidden_fields(ctx.txn, sortable_fields)?;

                return Err(UserError::InvalidSortableAttribute {
                    field: "_geo".to_string(),
                    valid_fields,
                    hidden_fields,
                }
                .into());
            }
            _ => (),
        }
    }

    Ok(())
}

pub struct PartialSearchResult {
    pub located_query_terms: Option<Vec<LocatedQueryTerm>>,
    pub candidates: RoaringBitmap,
    pub documents_ids: Vec<DocumentId>,
    pub document_scores: Vec<Vec<ScoreDetails>>,

    pub degraded: bool,
    pub used_negative_operator: bool,
}
