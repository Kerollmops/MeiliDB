use std::cmp::{self, Ordering};
use std::ops::Deref;

use rocksdb::DB;
use group_by::GroupBy;

use crate::rank::{match_query_index, Document};
use crate::rank::criterion::Criterion;
use crate::database::DatabaseView;
use crate::Match;

const MAX_DISTANCE: u32 = 8;

#[inline]
fn index_proximity(lhs: u32, rhs: u32) -> u32 {
    if lhs < rhs {
        cmp::min(rhs - lhs, MAX_DISTANCE)
    } else {
        cmp::min(lhs - rhs, MAX_DISTANCE) + 1
    }
}

#[inline]
fn attribute_proximity(lhs: &Match, rhs: &Match) -> u32 {
    if lhs.attribute.attribute() != rhs.attribute.attribute() { return MAX_DISTANCE }
    index_proximity(lhs.attribute.word_index(), rhs.attribute.word_index())
}

#[inline]
fn min_proximity(lhs: &[Match], rhs: &[Match]) -> u32 {
    let mut min_prox = u32::max_value();
    for a in lhs {
        for b in rhs {
            min_prox = cmp::min(min_prox, attribute_proximity(a, b));
        }
    }
    min_prox
}

#[inline]
fn matches_proximity(matches: &[Match]) -> u32 {
    let mut proximity = 0;
    let mut iter = GroupBy::new(matches, match_query_index);

    // iterate over groups by windows of size 2
    let mut last = iter.next();
    while let (Some(lhs), Some(rhs)) = (last, iter.next()) {
        proximity += min_proximity(lhs, rhs);
        last = Some(rhs);
    }

    proximity
}

/// Measure the sum of proximities between the different words of each documents,
/// a document with a proximity that is shorter is considered better.
#[derive(Debug, Clone, Copy)]
pub struct WordsProximity;

impl<D> Criterion<D> for WordsProximity
where D: Deref<Target=DB>
{
    #[inline]
    fn evaluate(&self, lhs: &Document, rhs: &Document, _: &DatabaseView<D>) -> Ordering {
        let lhs = matches_proximity(&lhs.matches);
        let rhs = matches_proximity(&rhs.matches);

        lhs.cmp(&rhs)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::Attribute;

    #[test]
    fn three_different_attributes() {

        // "soup" "of the" "the day"
        //
        // { id: 0, attr: 0, attr_index: 0 }
        // { id: 1, attr: 1, attr_index: 0 }
        // { id: 2, attr: 1, attr_index: 1 }
        // { id: 2, attr: 2, attr_index: 0 }
        // { id: 3, attr: 3, attr_index: 1 }

        let matches = &[
            Match { query_index: 0, attribute: Attribute::new(0, 0), ..Match::zero() },
            Match { query_index: 1, attribute: Attribute::new(1, 0), ..Match::zero() },
            Match { query_index: 2, attribute: Attribute::new(1, 1), ..Match::zero() },
            Match { query_index: 2, attribute: Attribute::new(2, 0), ..Match::zero() },
            Match { query_index: 3, attribute: Attribute::new(3, 1), ..Match::zero() },
        ];

        //   soup -> of = 8
        // + of -> the  = 1
        // + the -> day = 8 (not 1)
        assert_eq!(matches_proximity(matches), 17);
    }

    #[test]
    fn two_different_attributes() {

        // "soup day" "soup of the day"
        //
        // { id: 0, attr: 0, attr_index: 0 }
        // { id: 0, attr: 1, attr_index: 0 }
        // { id: 1, attr: 1, attr_index: 1 }
        // { id: 2, attr: 1, attr_index: 2 }
        // { id: 3, attr: 0, attr_index: 1 }
        // { id: 3, attr: 1, attr_index: 3 }

        let matches = &[
            Match { query_index: 0, attribute: Attribute::new(0, 0), ..Match::zero() },
            Match { query_index: 0, attribute: Attribute::new(1, 0), ..Match::zero() },
            Match { query_index: 1, attribute: Attribute::new(1, 1), ..Match::zero() },
            Match { query_index: 2, attribute: Attribute::new(1, 2), ..Match::zero() },
            Match { query_index: 3, attribute: Attribute::new(0, 1), ..Match::zero() },
            Match { query_index: 3, attribute: Attribute::new(1, 3), ..Match::zero() },
        ];

        //   soup -> of = 1
        // + of -> the  = 1
        // + the -> day = 1
        assert_eq!(matches_proximity(matches), 3);
    }
}
