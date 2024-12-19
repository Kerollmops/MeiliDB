use bumparaw_collections::RawMap;
use heed::RoTxn;
use rustc_hash::FxBuildHasher;

use crate::documents::{PrimaryKey, DEFAULT_PRIMARY_KEY};
use crate::update::new::StdResult;
use crate::{FieldsIdsMap, Index, Result, UserError};

/// Returns the primary key that has already been set for this index or the
/// one we will guess by searching for the first key that contains "id" as a substring,
/// and whether the primary key changed
pub fn retrieve_or_guess_primary_key<'a>(
    rtxn: &'a RoTxn<'a>,
    index: &Index,
    new_fields_ids_map: &mut FieldsIdsMap,
    primary_key_from_op: Option<&'a str>,
    first_document: Option<RawMap<'a, FxBuildHasher>>,
) -> Result<StdResult<(PrimaryKey<'a>, bool), UserError>> {
    // make sure that we have a declared primary key, either fetching it from the index or attempting to guess it.

    // do we have an existing declared primary key?
    let (primary_key, has_changed) = if let Some(primary_key_from_db) = index.primary_key(rtxn)? {
        // did we request a primary key in the operation?
        match primary_key_from_op {
            // we did, and it is different from the DB one
            Some(primary_key_from_op) if primary_key_from_op != primary_key_from_db => {
                return Ok(Err(UserError::PrimaryKeyCannotBeChanged(
                    primary_key_from_db.to_string(),
                )));
            }
            _ => (primary_key_from_db, false),
        }
    } else {
        // no primary key in the DB => let's set one
        // did we request a primary key in the operation?
        let primary_key = if let Some(primary_key_from_op) = primary_key_from_op {
            // set primary key from operation
            primary_key_from_op
        } else {
            // guess primary key
            let first_document = match first_document {
                Some(document) => document,
                // previous indexer when no pk is set + we send an empty payload => index_primary_key_no_candidate_found
                None => return Ok(Err(UserError::NoPrimaryKeyCandidateFound)),
            };

            let guesses: Result<Vec<&str>> = first_document
                .keys()
                .filter_map(|name| {
                    let Some(_) = new_fields_ids_map.insert(name) else {
                        return Some(Err(UserError::AttributeLimitReached.into()));
                    };
                    name.to_lowercase().ends_with(DEFAULT_PRIMARY_KEY).then_some(Ok(name))
                })
                .collect();

            let mut guesses = guesses?;

            // sort the keys in lexicographical order, so that fields are always in the same order.
            guesses.sort_unstable();

            match guesses.as_slice() {
                [] => return Ok(Err(UserError::NoPrimaryKeyCandidateFound)),
                [name] => {
                    tracing::info!("Primary key was not specified in index. Inferred to '{name}'");
                    *name
                }
                multiple => {
                    return Ok(Err(UserError::MultiplePrimaryKeyCandidatesFound {
                        candidates: multiple
                            .iter()
                            .map(|candidate| candidate.to_string())
                            .collect(),
                    }))
                }
            }
        };
        (primary_key, true)
    };

    match PrimaryKey::new_or_insert(primary_key, new_fields_ids_map) {
        Ok(primary_key) => Ok(Ok((primary_key, has_changed))),
        Err(err) => Ok(Err(err)),
    }
}
