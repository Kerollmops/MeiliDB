//! Everything that is related to document ranking.

pub mod criterion;
mod query_builder;
mod distinct_map;

use crate::{Match, DocumentId};

pub use self::query_builder::{QueryBuilder, DistinctQueryBuilder};

#[inline]
fn match_query_index(a: &Match, b: &Match) -> bool {
    a.query_index == b.query_index
}

/// A `Document` is an association of a DocumentId and all its associated matches.
///
/// The matches are used to sort documents using the criteria.
#[derive(Debug, Clone)]
pub struct Document {
    pub id: DocumentId,
    pub matches: Vec<Match>,
}

impl Document {
    /// Create one with one match.
    pub fn new(doc: DocumentId, match_: Match) -> Self {
        unsafe { Self::from_sorted_matches(doc, vec![match_]) }
    }

    /// Create one with a list of matches that are sorted before.
    pub fn from_matches(doc: DocumentId, mut matches: Vec<Match>) -> Self {
        matches.sort_unstable();
        unsafe { Self::from_sorted_matches(doc, matches) }
    }

    /// Create one with a list of pre-sorted matches.
    pub unsafe fn from_sorted_matches(id: DocumentId, matches: Vec<Match>) -> Self {
        Self { id, matches }
    }
}
