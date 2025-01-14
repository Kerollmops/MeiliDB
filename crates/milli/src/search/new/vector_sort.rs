use std::iter::FromIterator;

use roaring::RoaringBitmap;

use super::ranking_rules::{RankingRule, RankingRuleOutput, RankingRuleQueryTrait};
use crate::score_details::{self, ScoreDetails};
use crate::vector::{ArroyWrapper, DistributionShift, Embedder};
use crate::{DocumentId, Result, SearchContext, SearchLogger};

pub struct VectorSort<Q: RankingRuleQueryTrait> {
    query: Option<Q>,
    target: Vec<f32>,
    vector_candidates: RoaringBitmap,
    cached_sorted_docids: std::vec::IntoIter<(DocumentId, f32)>,
    limit: usize,
    distribution_shift: Option<DistributionShift>,
    embedder_index: u8,
    quantized: bool,
}

impl<Q: RankingRuleQueryTrait> VectorSort<Q> {
    pub fn new(
        ctx: &SearchContext<'_>,
        target: Vec<f32>,
        vector_candidates: RoaringBitmap,
        limit: usize,
        embedder_name: &str,
        embedder: &Embedder,
        quantized: bool,
    ) -> Result<Self> {
        let embedder_index = ctx
            .index
            .embedder_category_id
            .get(ctx.txn, embedder_name)?
            .ok_or_else(|| crate::UserError::InvalidSearchEmbedder(embedder_name.to_owned()))?;

        Ok(Self {
            query: None,
            target,
            vector_candidates,
            cached_sorted_docids: Default::default(),
            limit,
            distribution_shift: embedder.distribution(),
            embedder_index,
            quantized,
        })
    }

    fn fill_buffer(
        &mut self,
        ctx: &mut SearchContext<'_>,
        vector_candidates: &RoaringBitmap,
    ) -> Result<()> {
        let target = &self.target;

        let reader = ArroyWrapper::new(ctx.index.vector_arroy, self.embedder_index, self.quantized);
        let results = reader.nns_by_vector(ctx.txn, target, self.limit, Some(vector_candidates))?;
        self.cached_sorted_docids = results.into_iter();

        Ok(())
    }
}

impl<'ctx, Q: RankingRuleQueryTrait> RankingRule<'ctx, Q> for VectorSort<Q> {
    fn id(&self) -> String {
        "vector_sort".to_owned()
    }

    #[tracing::instrument(level = "trace", skip_all, target = "search::vector_sort")]
    fn start_iteration(
        &mut self,
        ctx: &mut SearchContext<'ctx>,
        _logger: &mut dyn SearchLogger<Q>,
        universe: &RoaringBitmap,
        query: &Q,
    ) -> Result<()> {
        assert!(self.query.is_none());

        self.query = Some(query.clone());
        let vector_candidates = &self.vector_candidates & universe;
        self.fill_buffer(ctx, &vector_candidates)?;
        Ok(())
    }

    #[allow(clippy::only_used_in_recursion)]
    #[tracing::instrument(level = "trace", skip_all, target = "search::vector_sort")]
    fn next_bucket(
        &mut self,
        ctx: &mut SearchContext<'ctx>,
        _logger: &mut dyn SearchLogger<Q>,
        universe: &RoaringBitmap,
    ) -> Result<Option<RankingRuleOutput<Q>>> {
        let query = self.query.as_ref().unwrap().clone();
        let vector_candidates = &self.vector_candidates & universe;

        if vector_candidates.is_empty() {
            return Ok(Some(RankingRuleOutput {
                query,
                candidates: universe.clone(),
                score: ScoreDetails::Vector(score_details::Vector { similarity: None }),
            }));
        }

        for (docid, distance) in self.cached_sorted_docids.by_ref() {
            if vector_candidates.contains(docid) {
                let score = 1.0 - distance;
                let score = self
                    .distribution_shift
                    .map(|distribution| distribution.shift(score))
                    .unwrap_or(score);
                return Ok(Some(RankingRuleOutput {
                    query,
                    candidates: RoaringBitmap::from_iter([docid]),
                    score: ScoreDetails::Vector(score_details::Vector { similarity: Some(score) }),
                }));
            }
        }

        // if we got out of this loop it means we've exhausted our cache.
        // we need to refill it and run the function again.
        self.fill_buffer(ctx, &vector_candidates)?;

        // we tried filling the buffer, but it remained empty 😢
        // it means we don't actually have any document remaining in the universe with a vector.
        // => exit
        if self.cached_sorted_docids.len() == 0 {
            return Ok(Some(RankingRuleOutput {
                query,
                candidates: universe.clone(),
                score: ScoreDetails::Vector(score_details::Vector { similarity: None }),
            }));
        }

        self.next_bucket(ctx, _logger, universe)
    }

    #[tracing::instrument(level = "trace", skip_all, target = "search::vector_sort")]
    fn end_iteration(&mut self, _ctx: &mut SearchContext<'ctx>, _logger: &mut dyn SearchLogger<Q>) {
        self.query = None;
    }
}
