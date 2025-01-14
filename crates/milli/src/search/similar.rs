use std::sync::Arc;

use roaring::RoaringBitmap;

use crate::score_details::{self, ScoreDetails};
use crate::vector::{ArroyWrapper, Embedder};
use crate::{filtered_universe, DocumentId, Filter, Index, Result, SearchResult};

pub struct Similar<'a> {
    id: DocumentId,
    // this should be linked to the String in the query
    filter: Option<Filter<'a>>,
    offset: usize,
    limit: usize,
    rtxn: &'a heed::RoTxn<'a>,
    index: &'a Index,
    embedder_name: String,
    embedder: Arc<Embedder>,
    ranking_score_threshold: Option<f64>,
    quantized: bool,
}

impl<'a> Similar<'a> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        id: DocumentId,
        offset: usize,
        limit: usize,
        index: &'a Index,
        rtxn: &'a heed::RoTxn<'a>,
        embedder_name: String,
        embedder: Arc<Embedder>,
        quantized: bool,
    ) -> Self {
        Self {
            id,
            filter: None,
            offset,
            limit,
            rtxn,
            index,
            embedder_name,
            embedder,
            ranking_score_threshold: None,
            quantized,
        }
    }

    pub fn filter(&mut self, filter: Filter<'a>) -> &mut Self {
        self.filter = Some(filter);
        self
    }

    pub fn ranking_score_threshold(&mut self, ranking_score_threshold: f64) -> &mut Self {
        self.ranking_score_threshold = Some(ranking_score_threshold);
        self
    }

    pub fn execute(&self) -> Result<SearchResult> {
        let mut universe = filtered_universe(self.index, self.rtxn, &self.filter)?;

        // we never want to receive the docid
        universe.remove(self.id);

        let universe = universe;

        let embedder_index =
            self.index.embedder_category_id.get(self.rtxn, &self.embedder_name)?.ok_or_else(
                || crate::UserError::InvalidSimilarEmbedder(self.embedder_name.to_owned()),
            )?;

        let reader = ArroyWrapper::new(self.index.vector_arroy, embedder_index, self.quantized);
        let results = reader.nns_by_item(
            self.rtxn,
            self.id,
            self.limit + self.offset + 1,
            Some(&universe),
        )?;

        let mut documents_ids = Vec::with_capacity(self.limit);
        let mut document_scores = Vec::with_capacity(self.limit);
        // list of documents we've already seen, so that we don't return the same document multiple times.
        // initialized to the target document, that we never want to return.
        let mut documents_seen = RoaringBitmap::new();
        documents_seen.insert(self.id);

        let mut candidates = universe;

        for (docid, distance) in results
            .into_iter()
            // skip documents we've already seen & mark that we saw the current document
            .filter(|(docid, _)| documents_seen.insert(*docid))
            .skip(self.offset)
            // take **after** filter and skip so that we get exactly limit elements if available
            .take(self.limit)
        {
            let score = 1.0 - distance;
            let score = self
                .embedder
                .distribution()
                .map(|distribution| distribution.shift(score))
                .unwrap_or(score);

            let score_details =
                vec![ScoreDetails::Vector(score_details::Vector { similarity: Some(score) })];

            let score = ScoreDetails::global_score(score_details.iter());

            if let Some(ranking_score_threshold) = &self.ranking_score_threshold {
                if score < *ranking_score_threshold {
                    // this document is no longer a candidate
                    candidates.remove(docid);
                    // any document after this one is no longer a candidate either, so restrict the set to documents already seen.
                    candidates &= documents_seen;
                    break;
                }
            }

            documents_ids.push(docid);
            document_scores.push(score_details);
        }

        Ok(SearchResult {
            matching_words: Default::default(),
            candidates,
            documents_ids,
            document_scores,
            degraded: false,
            used_negative_operator: false,
        })
    }
}
