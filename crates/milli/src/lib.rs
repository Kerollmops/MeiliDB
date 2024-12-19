#![cfg_attr(all(test, fuzzing), feature(no_coverage))]
#![allow(clippy::type_complexity)]

#[cfg(not(windows))]
#[cfg(test)]
#[global_allocator]
pub static ALLOC: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[macro_use]
pub mod documents;

mod asc_desc;
mod criterion;
mod error;
mod external_documents_ids;
pub mod facet;
mod fields_ids_map;
pub mod heed_codec;
pub mod index;
mod localized_attributes_rules;
pub mod order_by_map;
pub mod prompt;
pub mod proximity;
pub mod score_details;
mod search;
mod thread_pool_no_abort;
pub mod update;
pub mod vector;

#[cfg(test)]
#[macro_use]
pub mod snapshot_tests;
mod fieldids_weights_map;
pub mod progress;

use std::collections::{BTreeMap, HashMap};
use std::convert::{TryFrom, TryInto};
use std::fmt;
use std::hash::BuildHasherDefault;

use charabia::normalizer::{CharNormalizer, CompatibilityDecompositionNormalizer};
pub use filter_parser::{Condition, FilterCondition, Span, Token};
use fxhash::{FxHasher32, FxHasher64};
pub use grenad::CompressionType;
pub use search::new::{
    execute_search, filtered_universe, DefaultSearchLogger, GeoSortStrategy, SearchContext,
    SearchLogger, VisualSearchLogger,
};
use serde_json::Value;
pub use thread_pool_no_abort::{PanicCatched, ThreadPoolNoAbort, ThreadPoolNoAbortBuilder};
pub use {charabia as tokenizer, heed, rhai, zstd};

pub use self::asc_desc::{AscDesc, AscDescError, Member, SortError};
pub use self::criterion::{default_criteria, Criterion, CriterionError};
pub use self::error::{
    Error, FieldIdMapMissingEntry, InternalError, SerializationError, UserError,
};
pub use self::external_documents_ids::ExternalDocumentsIds;
pub use self::fieldids_weights_map::FieldidsWeightsMap;
pub use self::fields_ids_map::{FieldsIdsMap, GlobalFieldsIdsMap};
pub use self::heed_codec::{
    BEU16StrCodec, BEU32StrCodec, BoRoaringBitmapCodec, BoRoaringBitmapLenCodec,
    CboRoaringBitmapCodec, CboRoaringBitmapLenCodec, FieldIdWordCountCodec, ObkvCodec,
    RoaringBitmapCodec, RoaringBitmapLenCodec, StrBEU32Codec, U8StrStrCodec,
    UncheckedU8StrStrCodec,
};
pub use self::index::Index;
pub use self::localized_attributes_rules::LocalizedAttributesRule;
use self::localized_attributes_rules::LocalizedFieldIds;
pub use self::search::facet::{FacetValueHit, SearchForFacetValues};
pub use self::search::similar::Similar;
pub use self::search::{
    FacetDistribution, Filter, FormatOptions, MatchBounds, MatcherBuilder, MatchingWords, OrderBy,
    Search, SearchResult, SemanticSearch, TermsMatchingStrategy, DEFAULT_VALUES_PER_FACET,
};

pub type Result<T> = std::result::Result<T, error::Error>;

pub type Attribute = u32;
pub type BEU16 = heed::types::U16<heed::byteorder::BE>;
pub type BEU32 = heed::types::U32<heed::byteorder::BE>;
pub type BEU64 = heed::types::U64<heed::byteorder::BE>;
pub type DocumentId = u32;
pub type FastMap4<K, V> = HashMap<K, V, BuildHasherDefault<FxHasher32>>;
pub type FastMap8<K, V> = HashMap<K, V, BuildHasherDefault<FxHasher64>>;
pub type FieldDistribution = BTreeMap<String, u64>;
pub type FieldId = u16;
pub type Weight = u16;
pub type Object = serde_json::Map<String, serde_json::Value>;
pub type Position = u32;
pub type RelativePosition = u16;
pub type SmallString32 = smallstr::SmallString<[u8; 32]>;
pub type Prefix = smallstr::SmallString<[u8; 16]>;
pub type SmallVec16<T> = smallvec::SmallVec<[T; 16]>;
pub type SmallVec32<T> = smallvec::SmallVec<[T; 32]>;
pub type SmallVec8<T> = smallvec::SmallVec<[T; 8]>;

/// A GeoPoint is a point in cartesian plan, called xyz_point in the code. Its metadata
/// is a tuple composed of 1. the DocumentId of the associated document and 2. the original point
/// expressed in term of latitude and longitude.
pub type GeoPoint = rstar::primitives::GeomWithData<[f64; 3], (DocumentId, [f64; 2])>;

/// The maximum length a LMDB key can be.
///
/// Note that the actual allowed length is a little bit higher, but
/// we keep a margin of safety.
const MAX_LMDB_KEY_LENGTH: usize = 500;

/// The maximum length a field value can be when inserted in an LMDB key.
///
/// This number is determined by the keys of the different facet databases
/// and adding a margin of safety.
pub const MAX_FACET_VALUE_LENGTH: usize = MAX_LMDB_KEY_LENGTH - 32;

/// The maximum length a word can be
pub const MAX_WORD_LENGTH: usize = MAX_LMDB_KEY_LENGTH / 2;

pub const MAX_POSITION_PER_ATTRIBUTE: u32 = u16::MAX as u32 + 1;

#[derive(Clone)]
pub struct TimeBudget {
    started_at: std::time::Instant,
    budget: std::time::Duration,

    /// When testing the time budget, ensuring we did more than iteration of the bucket sort can be useful.
    /// But to avoid being flaky, the only option is to add the ability to stop after a specific number of calls instead of a `Duration`.
    #[cfg(test)]
    stop_after: Option<(std::sync::Arc<std::sync::atomic::AtomicUsize>, usize)>,
}

impl fmt::Debug for TimeBudget {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TimeBudget")
            .field("started_at", &self.started_at)
            .field("budget", &self.budget)
            .field("left", &(self.budget - self.started_at.elapsed()))
            .finish()
    }
}

impl Default for TimeBudget {
    fn default() -> Self {
        Self::new(std::time::Duration::from_millis(1500))
    }
}

impl TimeBudget {
    pub fn new(budget: std::time::Duration) -> Self {
        Self {
            started_at: std::time::Instant::now(),
            budget,

            #[cfg(test)]
            stop_after: None,
        }
    }

    pub fn max() -> Self {
        Self::new(std::time::Duration::from_secs(u64::MAX))
    }

    #[cfg(test)]
    pub fn with_stop_after(mut self, stop_after: usize) -> Self {
        use std::sync::atomic::AtomicUsize;
        use std::sync::Arc;

        self.stop_after = Some((Arc::new(AtomicUsize::new(0)), stop_after));
        self
    }

    pub fn exceeded(&self) -> bool {
        #[cfg(test)]
        if let Some((current, stop_after)) = &self.stop_after {
            let current = current.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            if current >= *stop_after {
                return true;
            } else {
                // if a number has been specified then we ignore entirely the time budget
                return false;
            }
        }

        self.started_at.elapsed() > self.budget
    }
}

// Convert an absolute word position into a relative position.
// Return the field id of the attribute related to the absolute position
// and the relative position in the attribute.
pub fn relative_from_absolute_position(absolute: Position) -> (FieldId, RelativePosition) {
    ((absolute >> 16) as u16, (absolute & 0xFFFF) as u16)
}

// Compute the absolute word position with the field id of the attribute and relative position in the attribute.
pub fn absolute_from_relative_position(field_id: FieldId, relative: RelativePosition) -> Position {
    (field_id as u32) << 16 | (relative as u32)
}
// TODO: this is wrong, but will do for now
/// Compute the "bucketed" absolute position from the field id and relative position in the field.
///
/// In a bucketed position, the accuracy of the relative position is reduced exponentially as it gets larger.
pub fn bucketed_position(relative: u16) -> u16 {
    // The first few relative positions are kept intact.
    if relative < 16 {
        relative
    } else if relative < 24 {
        // Relative positions between 16 and 24 all become equal to 24
        24
    } else {
        // Then, groups of positions that have the same base-2 logarithm are reduced to
        // the same relative position: the smallest power of 2 that is greater than them
        (relative as f64).log2().ceil().exp2() as u16
    }
}

/// Transform a raw obkv store into a JSON Object.
pub fn obkv_to_json(
    displayed_fields: &[FieldId],
    fields_ids_map: &FieldsIdsMap,
    obkv: &obkv::KvReaderU16,
) -> Result<Object> {
    displayed_fields
        .iter()
        .copied()
        .flat_map(|id| obkv.get(id).map(|value| (id, value)))
        .map(|(id, value)| {
            let name = fields_ids_map.name(id).ok_or(error::FieldIdMapMissingEntry::FieldId {
                field_id: id,
                process: "obkv_to_json",
            })?;
            let value = serde_json::from_slice(value).map_err(error::InternalError::SerdeJson)?;
            Ok((name.to_owned(), value))
        })
        .collect()
}

/// Transform every field of a raw obkv store into a JSON Object.
pub fn all_obkv_to_json(obkv: &obkv::KvReaderU16, fields_ids_map: &FieldsIdsMap) -> Result<Object> {
    let all_keys = obkv.iter().map(|(k, _v)| k).collect::<Vec<_>>();
    obkv_to_json(all_keys.as_slice(), fields_ids_map, obkv)
}

/// Transform a JSON value into a string that can be indexed.
pub fn json_to_string(value: &Value) -> Option<String> {
    fn inner(value: &Value, output: &mut String) -> bool {
        use std::fmt::Write;
        match value {
            Value::Null => false,
            Value::Bool(boolean) => write!(output, "{}", boolean).is_ok(),
            Value::Number(number) => write!(output, "{}", number).is_ok(),
            Value::String(string) => write!(output, "{}", string).is_ok(),
            Value::Array(array) => {
                let mut count = 0;
                for value in array {
                    if inner(value, output) {
                        output.push_str(". ");
                        count += 1;
                    }
                }
                // check that at least one value was written
                count != 0
            }
            Value::Object(object) => {
                let mut buffer = String::new();
                let mut count = 0;
                for (key, value) in object {
                    buffer.clear();
                    let _ = write!(&mut buffer, "{}: ", key);
                    if inner(value, &mut buffer) {
                        buffer.push_str(". ");
                        // We write the "key: value. " pair only when
                        // we are sure that the value can be written.
                        output.push_str(&buffer);
                        count += 1;
                    }
                }
                // check that at least one value was written
                count != 0
            }
        }
    }

    let mut string = String::new();
    if inner(value, &mut string) {
        Some(string)
    } else {
        None
    }
}

/// Divides one slice into two at an index, returns `None` if mid is out of bounds.
fn try_split_at<T>(slice: &[T], mid: usize) -> Option<(&[T], &[T])> {
    if mid <= slice.len() {
        Some(slice.split_at(mid))
    } else {
        None
    }
}

/// Divides one slice into an array and the tail at an index,
/// returns `None` if `N` is out of bounds.
fn try_split_array_at<T, const N: usize>(slice: &[T]) -> Option<([T; N], &[T])>
where
    [T; N]: for<'a> TryFrom<&'a [T]>,
{
    let (head, tail) = try_split_at(slice, N)?;
    let head = head.try_into().ok()?;
    Some((head, tail))
}

/// Return the distance between two points in meters. Each points are composed of two f64,
/// one latitude and one longitude.
pub fn distance_between_two_points(a: &[f64; 2], b: &[f64; 2]) -> f64 {
    let a = geoutils::Location::new(a[0], a[1]);
    let b = geoutils::Location::new(b[0], b[1]);

    a.haversine_distance_to(&b).meters()
}

/// Convert a point expressed in terms of latitude and longitude to a point in the
/// cartesian coordinate expressed in terms of x, y and z.
pub fn lat_lng_to_xyz(coord: &[f64; 2]) -> [f64; 3] {
    let [lat, lng] = coord.map(|f| f.to_radians());
    let x = lat.cos() * lng.cos();
    let y = lat.cos() * lng.sin();
    let z = lat.sin();

    [x, y, z]
}

/// Returns `true` if the field match one of the faceted fields.
/// See the function [`is_faceted_by`] below to see what “matching” means.
pub fn is_faceted(field: &str, faceted_fields: impl IntoIterator<Item = impl AsRef<str>>) -> bool {
    faceted_fields.into_iter().any(|facet| is_faceted_by(field, facet.as_ref()))
}

/// Returns `true` if the field match the facet.
/// ```
/// use milli::is_faceted_by;
/// // -- the valid basics
/// assert!(is_faceted_by("animaux", "animaux"));
/// assert!(is_faceted_by("animaux.chien", "animaux"));
/// assert!(is_faceted_by("animaux.chien.race.bouvier bernois.fourrure.couleur", "animaux"));
/// assert!(is_faceted_by("animaux.chien.race.bouvier bernois.fourrure.couleur", "animaux.chien"));
/// assert!(is_faceted_by("animaux.chien.race.bouvier bernois.fourrure.couleur", "animaux.chien.race.bouvier bernois"));
/// assert!(is_faceted_by("animaux.chien.race.bouvier bernois.fourrure.couleur", "animaux.chien.race.bouvier bernois.fourrure"));
/// assert!(is_faceted_by("animaux.chien.race.bouvier bernois.fourrure.couleur", "animaux.chien.race.bouvier bernois.fourrure.couleur"));
///
/// // -- the wrongs
/// assert!(!is_faceted_by("chien", "chat"));
/// assert!(!is_faceted_by("animaux", "animaux.chien"));
/// assert!(!is_faceted_by("animaux.chien", "animaux.chat"));
///
/// // -- the strange edge cases
/// assert!(!is_faceted_by("animaux.chien", "anima"));
/// assert!(!is_faceted_by("animaux.chien", "animau"));
/// assert!(!is_faceted_by("animaux.chien", "animaux."));
/// assert!(!is_faceted_by("animaux.chien", "animaux.c"));
/// assert!(!is_faceted_by("animaux.chien", "animaux.ch"));
/// assert!(!is_faceted_by("animaux.chien", "animaux.chi"));
/// assert!(!is_faceted_by("animaux.chien", "animaux.chie"));
/// ```
pub fn is_faceted_by(field: &str, facet: &str) -> bool {
    field.starts_with(facet) && field[facet.len()..].chars().next().map_or(true, |c| c == '.')
}

pub fn normalize_facet(original: &str) -> String {
    CompatibilityDecompositionNormalizer.normalize_str(original.trim()).to_lowercase()
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn json_to_string_object() {
        let value = json!({
            "name": "John Doe",
            "age": 43,
            "not_there": null,
        });

        let string = json_to_string(&value).unwrap();
        assert_eq!(string, "name: John Doe. age: 43. ");
    }

    #[test]
    fn json_to_string_array() {
        let value = json!([
            { "name": "John Doe" },
            43,
            "hello",
            [ "I", "am", "fine" ],
            null,
        ]);

        let string = json_to_string(&value).unwrap();
        // We don't care about having two point (.) after the other as
        // the distance of hard separators is clamped to 8 anyway.
        assert_eq!(string, "name: John Doe. . 43. hello. I. am. fine. . ");
    }

    #[test]
    fn test_relative_position_conversion() {
        assert_eq!((0x0000, 0x0000), relative_from_absolute_position(0x00000000));
        assert_eq!((0x0000, 0xFFFF), relative_from_absolute_position(0x0000FFFF));
        assert_eq!((0xFFFF, 0x0000), relative_from_absolute_position(0xFFFF0000));
        assert_eq!((0xFF00, 0xFF00), relative_from_absolute_position(0xFF00FF00));
        assert_eq!((0xFF00, 0x00FF), relative_from_absolute_position(0xFF0000FF));
        assert_eq!((0x1234, 0x5678), relative_from_absolute_position(0x12345678));
        assert_eq!((0xFFFF, 0xFFFF), relative_from_absolute_position(0xFFFFFFFF));
    }

    #[test]
    fn test_absolute_position_conversion() {
        assert_eq!(0x00000000, absolute_from_relative_position(0x0000, 0x0000));
        assert_eq!(0x0000FFFF, absolute_from_relative_position(0x0000, 0xFFFF));
        assert_eq!(0xFFFF0000, absolute_from_relative_position(0xFFFF, 0x0000));
        assert_eq!(0xFF00FF00, absolute_from_relative_position(0xFF00, 0xFF00));
        assert_eq!(0xFF0000FF, absolute_from_relative_position(0xFF00, 0x00FF));
        assert_eq!(0x12345678, absolute_from_relative_position(0x1234, 0x5678));
        assert_eq!(0xFFFFFFFF, absolute_from_relative_position(0xFFFF, 0xFFFF));
    }

    #[test]
    fn test_all_obkv_to_json() {
        let mut fields_ids_map = FieldsIdsMap::new();
        let id1 = fields_ids_map.insert("field1").unwrap();
        let id2 = fields_ids_map.insert("field2").unwrap();

        let mut writer = obkv::KvWriterU16::memory();
        writer.insert(id1, b"1234").unwrap();
        writer.insert(id2, b"4321").unwrap();
        let contents = writer.into_inner().unwrap();
        let obkv = obkv::KvReaderU16::from_slice(&contents);

        let expected = json!({
            "field1": 1234,
            "field2": 4321,
        });
        let expected = expected.as_object().unwrap();
        let actual = all_obkv_to_json(obkv, &fields_ids_map).unwrap();

        assert_eq!(&actual, expected);
    }
}
