use std::cmp::Ordering;
use std::ops::Deref;
use std::marker;

use rocksdb::DB;
use serde::de::DeserializeOwned;

use crate::rank::criterion::Criterion;
use crate::database::DatabaseView;
use crate::rank::Document;

/// An helper struct that permit to sort documents by
/// some of their stored attributes.
///
/// # Note
///
/// If a document cannot be deserialized it will be considered [`None`][].
///
/// Deserialized documents are compared like `Some(doc0).cmp(&Some(doc1))`,
/// so you must check the [`Ord`] of `Option` implementation.
///
/// [`None`]: https://doc.rust-lang.org/std/option/enum.Option.html#variant.None
/// [`Ord`]: https://doc.rust-lang.org/std/option/enum.Option.html#impl-Ord
///
/// # Example
///
/// ```
/// use serde_derive::Deserialize;
/// use meilidb::rank::criterion::*;
/// use meilidb::rocksdb::DB;
///
/// #[derive(Deserialize, PartialOrd, Ord, PartialEq, Eq)]
/// struct TimeOnly {
///     time: String,
/// }
///
/// let builder = CriteriaBuilder::<&DB>::new()
///        .add(SumOfTypos)
///        .add(NumberOfWords)
///        .add(WordsProximity)
///        .add(SumOfWordsAttribute)
///        .add(SumOfWordsPosition)
///        .add(Exact)
///        .add(SortBy::<TimeOnly>::new())
///        .add(DocumentId);
///
/// let criterion = builder.build();
///
/// ```
#[derive(Default)]
pub struct SortBy<T> {
    _phantom: marker::PhantomData<T>,
}

impl<T> SortBy<T> {
    pub fn new() -> Self {
        SortBy { _phantom: marker::PhantomData }
    }
}

impl<T, D> Criterion<D> for SortBy<T>
where D: Deref<Target=DB>,
      T: DeserializeOwned + Ord,
{
    fn evaluate(&self, lhs: &Document, rhs: &Document, view: &DatabaseView<D>) -> Ordering {
        let lhs = match view.retrieve_document::<T>(lhs.id) {
            Ok(doc) => Some(doc),
            Err(e) => { eprintln!("{}", e); None },
        };

        let rhs = match view.retrieve_document::<T>(rhs.id) {
            Ok(doc) => Some(doc),
            Err(e) => { eprintln!("{}", e); None },
        };

        lhs.cmp(&rhs)
    }
}
