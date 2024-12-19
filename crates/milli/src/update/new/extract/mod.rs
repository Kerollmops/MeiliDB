mod cache;
mod documents;
mod faceted;
mod geo;
mod searchable;
mod vectors;

use bumpalo::Bump;
pub use cache::{
    merge_caches_sorted, transpose_and_freeze_caches, BalancedCaches, DelAddRoaringBitmap,
};
pub use documents::*;
pub use faceted::*;
pub use geo::*;
pub use searchable::*;
pub use vectors::EmbeddingExtractor;

use super::indexer::document_changes::{DocumentChanges, IndexingContext};
use super::steps::IndexingStep;
use super::thread_local::{FullySend, ThreadLocal};
use crate::Result;

pub trait DocidsExtractor {
    fn run_extraction<'pl, 'fid, 'indexer, 'index, 'extractor, DC: DocumentChanges<'pl>, MSP>(
        document_changes: &DC,
        indexing_context: IndexingContext<'fid, 'indexer, 'index, MSP>,
        extractor_allocs: &'extractor mut ThreadLocal<FullySend<Bump>>,
        step: IndexingStep,
    ) -> Result<Vec<BalancedCaches<'extractor>>>
    where
        MSP: Fn() -> bool + Sync;
}

/// TODO move in permissive json pointer
pub mod perm_json_p {
    use serde_json::{Map, Value};

    use crate::Result;
    const SPLIT_SYMBOL: char = '.';

    /// Returns `true` if the `selector` match the `key`.
    ///
    /// ```text
    /// Example:
    /// `animaux`           match `animaux`
    /// `animaux.chien`     match `animaux`
    /// `animaux.chien`     match `animaux`
    /// `animaux.chien.nom` match `animaux`
    /// `animaux.chien.nom` match `animaux.chien`
    /// -----------------------------------------
    /// `animaux`    doesn't match `animaux.chien`
    /// `animaux.`   doesn't match `animaux`
    /// `animaux.ch` doesn't match `animaux.chien`
    /// `animau`     doesn't match `animaux`
    /// ```
    pub fn contained_in(selector: &str, key: &str) -> bool {
        selector.starts_with(key)
            && selector[key.len()..].chars().next().map(|c| c == SPLIT_SYMBOL).unwrap_or(true)
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum Depth {
        /// The perm json ptr is currently on the field of an object
        OnBaseKey,
        /// The perm json ptr is currently inside of an array
        InsideArray,
    }

    pub fn seek_leaf_values_in_object(
        value: &Map<String, Value>,
        selectors: Option<&[&str]>,
        skip_selectors: &[&str],
        base_key: &str,
        base_depth: Depth,
        seeker: &mut impl FnMut(&str, Depth, &Value) -> Result<()>,
    ) -> Result<()> {
        if value.is_empty() {
            seeker(base_key, base_depth, &Value::Object(Map::with_capacity(0)))?;
        }

        for (key, value) in value.iter() {
            let base_key = if base_key.is_empty() {
                key.to_string()
            } else {
                format!("{}{}{}", base_key, SPLIT_SYMBOL, key)
            };

            // here if the user only specified `doggo` we need to iterate in all the fields of `doggo`
            // so we check the contained_in on both side
            let selection = select_field(&base_key, selectors, skip_selectors);
            if selection != Selection::Skip {
                match value {
                    Value::Object(object) => {
                        if selection == Selection::Select {
                            seeker(&base_key, Depth::OnBaseKey, value)?;
                        }

                        seek_leaf_values_in_object(
                            object,
                            selectors,
                            skip_selectors,
                            &base_key,
                            Depth::OnBaseKey,
                            seeker,
                        )
                    }
                    Value::Array(array) => {
                        if selection == Selection::Select {
                            seeker(&base_key, Depth::OnBaseKey, value)?;
                        }

                        seek_leaf_values_in_array(
                            array,
                            selectors,
                            skip_selectors,
                            &base_key,
                            Depth::OnBaseKey,
                            seeker,
                        )
                    }
                    value => seeker(&base_key, Depth::OnBaseKey, value),
                }?;
            }
        }

        Ok(())
    }

    pub fn seek_leaf_values_in_array(
        values: &[Value],
        selectors: Option<&[&str]>,
        skip_selectors: &[&str],
        base_key: &str,
        base_depth: Depth,
        seeker: &mut impl FnMut(&str, Depth, &Value) -> Result<()>,
    ) -> Result<()> {
        if values.is_empty() {
            seeker(base_key, base_depth, &Value::Array(vec![]))?;
        }

        for value in values {
            match value {
                Value::Object(object) => seek_leaf_values_in_object(
                    object,
                    selectors,
                    skip_selectors,
                    base_key,
                    Depth::InsideArray,
                    seeker,
                ),
                Value::Array(array) => seek_leaf_values_in_array(
                    array,
                    selectors,
                    skip_selectors,
                    base_key,
                    Depth::InsideArray,
                    seeker,
                ),
                value => seeker(base_key, Depth::InsideArray, value),
            }?;
        }

        Ok(())
    }

    pub fn select_field(
        field_name: &str,
        selectors: Option<&[&str]>,
        skip_selectors: &[&str],
    ) -> Selection {
        if skip_selectors.iter().any(|skip_selector| {
            contained_in(skip_selector, field_name) || contained_in(field_name, skip_selector)
        }) {
            Selection::Skip
        } else if let Some(selectors) = selectors {
            let mut selection = Selection::Skip;
            for selector in selectors {
                if contained_in(field_name, selector) {
                    selection = Selection::Select;
                    break;
                } else if contained_in(selector, field_name) {
                    selection = Selection::Parent;
                }
            }
            selection
        } else {
            Selection::Select
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum Selection {
        /// The field is a parent of the of a nested field that must be selected
        Parent,
        /// The field must be selected
        Select,
        /// The field must be skipped
        Skip,
    }
}
