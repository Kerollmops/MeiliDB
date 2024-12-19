use std::cmp::Ordering;

use heed::types::{Bytes, DecodeIgnore, Str};
use heed::RwTxn;
use itertools::{merge_join_by, EitherOrBoth};

use super::document_changes::IndexingContext;
use crate::facet::FacetType;
use crate::index::main_key::{WORDS_FST_KEY, WORDS_PREFIXES_FST_KEY};
use crate::update::del_add::DelAdd;
use crate::update::new::facet_search_builder::FacetSearchBuilder;
use crate::update::new::steps::IndexingStep;
use crate::update::new::word_fst_builder::{PrefixData, PrefixDelta, WordFstBuilder};
use crate::update::new::words_prefix_docids::{
    compute_exact_word_prefix_docids, compute_word_prefix_docids, compute_word_prefix_fid_docids,
    compute_word_prefix_position_docids,
};
use crate::update::new::FacetFieldIdsDelta;
use crate::update::{FacetsUpdateBulk, GrenadParameters};
use crate::{GlobalFieldsIdsMap, Index, Result};

pub(super) fn postprocess<MSP>(
    indexing_context: IndexingContext<MSP>,
    wtxn: &mut RwTxn<'_>,
    global_fields_ids_map: GlobalFieldsIdsMap<'_>,
    facet_field_ids_delta: FacetFieldIdsDelta,
) -> Result<()>
where
    MSP: Fn() -> bool + Sync,
{
    let index = indexing_context.index;
    indexing_context.progress.update_progress(IndexingStep::PostProcessingFacets);
    if index.facet_search(wtxn)? {
        compute_facet_search_database(index, wtxn, global_fields_ids_map)?;
    }
    compute_facet_level_database(index, wtxn, facet_field_ids_delta)?;
    indexing_context.progress.update_progress(IndexingStep::PostProcessingWords);
    if let Some(prefix_delta) = compute_word_fst(index, wtxn)? {
        compute_prefix_database(index, wtxn, prefix_delta, indexing_context.grenad_parameters)?;
    };
    Ok(())
}

#[tracing::instrument(level = "trace", skip_all, target = "indexing::prefix")]
fn compute_prefix_database(
    index: &Index,
    wtxn: &mut RwTxn,
    prefix_delta: PrefixDelta,
    grenad_parameters: &GrenadParameters,
) -> Result<()> {
    let PrefixDelta { modified, deleted } = prefix_delta;
    // Compute word prefix docids
    compute_word_prefix_docids(wtxn, index, &modified, &deleted, grenad_parameters)?;
    // Compute exact word prefix docids
    compute_exact_word_prefix_docids(wtxn, index, &modified, &deleted, grenad_parameters)?;
    // Compute word prefix fid docids
    compute_word_prefix_fid_docids(wtxn, index, &modified, &deleted, grenad_parameters)?;
    // Compute word prefix position docids
    compute_word_prefix_position_docids(wtxn, index, &modified, &deleted, grenad_parameters)
}

#[tracing::instrument(level = "trace", skip_all, target = "indexing")]
fn compute_word_fst(index: &Index, wtxn: &mut RwTxn) -> Result<Option<PrefixDelta>> {
    let rtxn = index.read_txn()?;
    let words_fst = index.words_fst(&rtxn)?;
    let mut word_fst_builder = WordFstBuilder::new(&words_fst)?;
    let prefix_settings = index.prefix_settings(&rtxn)?;
    word_fst_builder.with_prefix_settings(prefix_settings);

    let previous_words = index.word_docids.iter(&rtxn)?.remap_data_type::<Bytes>();
    let current_words = index.word_docids.iter(wtxn)?.remap_data_type::<Bytes>();
    for eob in merge_join_by(previous_words, current_words, |lhs, rhs| match (lhs, rhs) {
        (Ok((l, _)), Ok((r, _))) => l.cmp(r),
        (Err(_), _) | (_, Err(_)) => Ordering::Equal,
    }) {
        match eob {
            EitherOrBoth::Both(lhs, rhs) => {
                let (word, lhs_bytes) = lhs?;
                let (_, rhs_bytes) = rhs?;
                if lhs_bytes != rhs_bytes {
                    word_fst_builder.register_word(DelAdd::Addition, word.as_ref())?;
                }
            }
            EitherOrBoth::Left(result) => {
                let (word, _) = result?;
                word_fst_builder.register_word(DelAdd::Deletion, word.as_ref())?;
            }
            EitherOrBoth::Right(result) => {
                let (word, _) = result?;
                word_fst_builder.register_word(DelAdd::Addition, word.as_ref())?;
            }
        }
    }

    let (word_fst_mmap, prefix_data) = word_fst_builder.build(index, &rtxn)?;
    index.main.remap_types::<Str, Bytes>().put(wtxn, WORDS_FST_KEY, &word_fst_mmap)?;
    if let Some(PrefixData { prefixes_fst_mmap, prefix_delta }) = prefix_data {
        index.main.remap_types::<Str, Bytes>().put(
            wtxn,
            WORDS_PREFIXES_FST_KEY,
            &prefixes_fst_mmap,
        )?;
        Ok(Some(prefix_delta))
    } else {
        Ok(None)
    }
}

#[tracing::instrument(level = "trace", skip_all, target = "indexing::facet_search")]
fn compute_facet_search_database(
    index: &Index,
    wtxn: &mut RwTxn,
    global_fields_ids_map: GlobalFieldsIdsMap,
) -> Result<()> {
    let rtxn = index.read_txn()?;
    let localized_attributes_rules = index.localized_attributes_rules(&rtxn)?;
    let mut facet_search_builder = FacetSearchBuilder::new(
        global_fields_ids_map,
        localized_attributes_rules.unwrap_or_default(),
    );

    let previous_facet_id_string_docids = index
        .facet_id_string_docids
        .iter(&rtxn)?
        .remap_data_type::<DecodeIgnore>()
        .filter(|r| r.as_ref().map_or(true, |(k, _)| k.level == 0));
    let current_facet_id_string_docids = index
        .facet_id_string_docids
        .iter(wtxn)?
        .remap_data_type::<DecodeIgnore>()
        .filter(|r| r.as_ref().map_or(true, |(k, _)| k.level == 0));
    for eob in merge_join_by(
        previous_facet_id_string_docids,
        current_facet_id_string_docids,
        |lhs, rhs| match (lhs, rhs) {
            (Ok((l, _)), Ok((r, _))) => l.cmp(r),
            (Err(_), _) | (_, Err(_)) => Ordering::Equal,
        },
    ) {
        match eob {
            EitherOrBoth::Both(lhs, rhs) => {
                let (_, _) = lhs?;
                let (_, _) = rhs?;
            }
            EitherOrBoth::Left(result) => {
                let (key, _) = result?;
                facet_search_builder.register_from_key(DelAdd::Deletion, key)?;
            }
            EitherOrBoth::Right(result) => {
                let (key, _) = result?;
                facet_search_builder.register_from_key(DelAdd::Addition, key)?;
            }
        }
    }

    facet_search_builder.merge_and_write(index, wtxn, &rtxn)
}

#[tracing::instrument(level = "trace", skip_all, target = "indexing::facet_field_ids")]
fn compute_facet_level_database(
    index: &Index,
    wtxn: &mut RwTxn,
    facet_field_ids_delta: FacetFieldIdsDelta,
) -> Result<()> {
    if let Some(modified_facet_string_ids) = facet_field_ids_delta.modified_facet_string_ids() {
        let span = tracing::trace_span!(target: "indexing::facet_field_ids", "string");
        let _entered = span.enter();
        FacetsUpdateBulk::new_not_updating_level_0(
            index,
            modified_facet_string_ids,
            FacetType::String,
        )
        .execute(wtxn)?;
    }
    if let Some(modified_facet_number_ids) = facet_field_ids_delta.modified_facet_number_ids() {
        let span = tracing::trace_span!(target: "indexing::facet_field_ids", "number");
        let _entered = span.enter();
        FacetsUpdateBulk::new_not_updating_level_0(
            index,
            modified_facet_number_ids,
            FacetType::Number,
        )
        .execute(wtxn)?;
    }

    Ok(())
}
