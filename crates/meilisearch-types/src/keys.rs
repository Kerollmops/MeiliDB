use std::convert::Infallible;
use std::hash::Hash;
use std::str::FromStr;

use bitflags::{bitflags, Flags};
use deserr::{take_cf_content, DeserializeError, Deserr, MergeWithError, ValuePointerRef};
use enum_iterator::Sequence;
use milli::update::Setting;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use time::format_description::well_known::Rfc3339;
use time::macros::{format_description, time};
use time::{Date, OffsetDateTime, PrimitiveDateTime};
use uuid::Uuid;

use crate::deserr::{immutable_field_error, DeserrError, DeserrJsonError};
use crate::error::deserr_codes::*;
use crate::error::{Code, ErrorCode, ParseOffsetDateTimeError};
use crate::index_uid_pattern::{IndexUidPattern, IndexUidPatternFormatError};

pub type KeyId = Uuid;

impl<C: Default + ErrorCode> MergeWithError<IndexUidPatternFormatError> for DeserrJsonError<C> {
    fn merge(
        _self_: Option<Self>,
        other: IndexUidPatternFormatError,
        merge_location: deserr::ValuePointerRef,
    ) -> std::ops::ControlFlow<Self, Self> {
        DeserrError::error::<Infallible>(
            None,
            deserr::ErrorKind::Unexpected { msg: other.to_string() },
            merge_location,
        )
    }
}

#[derive(Debug, Deserr)]
#[deserr(error = DeserrJsonError, rename_all = camelCase, deny_unknown_fields)]
pub struct CreateApiKey {
    #[deserr(default, error = DeserrJsonError<InvalidApiKeyDescription>)]
    pub description: Option<String>,
    #[deserr(default, error = DeserrJsonError<InvalidApiKeyName>)]
    pub name: Option<String>,
    #[deserr(default = Uuid::new_v4(), error = DeserrJsonError<InvalidApiKeyUid>, try_from(&String) = Uuid::from_str -> uuid::Error)]
    pub uid: KeyId,
    #[deserr(error = DeserrJsonError<InvalidApiKeyActions>, missing_field_error = DeserrJsonError::missing_api_key_actions)]
    pub actions: Vec<Action>,
    #[deserr(error = DeserrJsonError<InvalidApiKeyIndexes>, missing_field_error = DeserrJsonError::missing_api_key_indexes)]
    pub indexes: Vec<IndexUidPattern>,
    #[deserr(error = DeserrJsonError<InvalidApiKeyExpiresAt>, try_from(Option<String>) = parse_expiration_date -> ParseOffsetDateTimeError, missing_field_error = DeserrJsonError::missing_api_key_expires_at)]
    pub expires_at: Option<OffsetDateTime>,
}

impl CreateApiKey {
    pub fn to_key(self) -> Key {
        let CreateApiKey { description, name, uid, actions, indexes, expires_at } = self;
        let now = OffsetDateTime::now_utc();
        Key {
            description,
            name,
            uid,
            actions,
            indexes,
            expires_at,
            created_at: now,
            updated_at: now,
        }
    }
}

fn deny_immutable_fields_api_key(
    field: &str,
    accepted: &[&str],
    location: ValuePointerRef,
) -> DeserrJsonError {
    match field {
        "uid" => immutable_field_error(field, accepted, Code::ImmutableApiKeyUid),
        "actions" => immutable_field_error(field, accepted, Code::ImmutableApiKeyActions),
        "indexes" => immutable_field_error(field, accepted, Code::ImmutableApiKeyIndexes),
        "expiresAt" => immutable_field_error(field, accepted, Code::ImmutableApiKeyExpiresAt),
        "createdAt" => immutable_field_error(field, accepted, Code::ImmutableApiKeyCreatedAt),
        "updatedAt" => immutable_field_error(field, accepted, Code::ImmutableApiKeyUpdatedAt),
        _ => deserr::take_cf_content(DeserrJsonError::<BadRequest>::error::<Infallible>(
            None,
            deserr::ErrorKind::UnknownKey { key: field, accepted },
            location,
        )),
    }
}

#[derive(Debug, Deserr)]
#[deserr(error = DeserrJsonError, rename_all = camelCase, deny_unknown_fields = deny_immutable_fields_api_key)]
pub struct PatchApiKey {
    #[deserr(default, error = DeserrJsonError<InvalidApiKeyDescription>)]
    pub description: Setting<String>,
    #[deserr(default, error = DeserrJsonError<InvalidApiKeyName>)]
    pub name: Setting<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct Key {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    pub uid: KeyId,
    pub actions: Vec<Action>,
    pub indexes: Vec<IndexUidPattern>,
    #[serde(with = "time::serde::rfc3339::option")]
    pub expires_at: Option<OffsetDateTime>,
    #[serde(with = "time::serde::rfc3339")]
    pub created_at: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339")]
    pub updated_at: OffsetDateTime,
}

impl Key {
    pub fn default_admin() -> Self {
        let now = OffsetDateTime::now_utc();
        let uid = Uuid::new_v4();
        Self {
            name: Some("Default Admin API Key".to_string()),
            description: Some("Use it for anything that is not a search operation. Caution! Do not expose it on a public frontend".to_string()),
            uid,
            actions: vec![Action::All],
            indexes: vec![IndexUidPattern::all()],
            expires_at: None,
            created_at: now,
            updated_at: now,
        }
    }

    pub fn default_search() -> Self {
        let now = OffsetDateTime::now_utc();
        let uid = Uuid::new_v4();
        Self {
            name: Some("Default Search API Key".to_string()),
            description: Some("Use it to search from the frontend".to_string()),
            uid,
            actions: vec![Action::Search],
            indexes: vec![IndexUidPattern::all()],
            expires_at: None,
            created_at: now,
            updated_at: now,
        }
    }
}

fn parse_expiration_date(
    string: Option<String>,
) -> std::result::Result<Option<OffsetDateTime>, ParseOffsetDateTimeError> {
    let Some(string) = string else { return Ok(None) };
    let datetime = if let Ok(datetime) = OffsetDateTime::parse(&string, &Rfc3339) {
        datetime
    } else if let Ok(primitive_datetime) = PrimitiveDateTime::parse(
        &string,
        format_description!(
            "[year repr:full base:calendar]-[month repr:numerical]-[day]T[hour]:[minute]:[second]"
        ),
    ) {
        primitive_datetime.assume_utc()
    } else if let Ok(primitive_datetime) = PrimitiveDateTime::parse(
        &string,
        format_description!(
            "[year repr:full base:calendar]-[month repr:numerical]-[day] [hour]:[minute]:[second]"
        ),
    ) {
        primitive_datetime.assume_utc()
    } else if let Ok(date) = Date::parse(
        &string,
        format_description!("[year repr:full base:calendar]-[month repr:numerical]-[day]"),
    ) {
        PrimitiveDateTime::new(date, time!(00:00)).assume_utc()
    } else {
        return Err(ParseOffsetDateTimeError(string));
    };
    if datetime > OffsetDateTime::now_utc() {
        Ok(Some(datetime))
    } else {
        Err(ParseOffsetDateTimeError(string))
    }
}

bitflags! {
    #[derive(Copy, Clone, Debug, Eq, PartialEq, Hash, PartialOrd, Ord)]
    #[repr(transparent)]
    // NOTE: For `Sequence` impl to work, the values of these must be in ascending order
    pub struct Action: u32 {
        const Search = 1;
        // Documents
        const DocumentsAdd = 1 << 1;
        const DocumentsGet = 1 << 2;
        const DocumentsDelete = 1 << 3;
        const DocumentsAll = Self::DocumentsAdd.bits() | Self::DocumentsGet.bits() | Self::DocumentsDelete.bits();
        // Indexes
        const IndexesAdd = 1 << 4;
        const IndexesGet = 1 << 5;
        const IndexesUpdate = 1 << 6;
        const IndexesDelete = 1 << 7;
        const IndexesSwap = 1 << 8;
        const IndexesAll = Self::IndexesAdd.bits() | Self::IndexesGet.bits() | Self::IndexesUpdate.bits() | Self::IndexesDelete.bits() | Self::IndexesSwap.bits();
        // Tasks
        const TasksCancel = 1 << 9;
        const TasksDelete = 1 << 10;
        const TasksGet = 1 << 11;
        const TasksAll = Self::TasksCancel.bits() | Self::TasksDelete.bits() | Self::TasksGet.bits();
        // Settings
        const SettingsGet = 1 << 12;
        const SettingsUpdate = 1 << 13;
        const SettingsAll = Self::SettingsGet.bits() | Self::SettingsUpdate.bits();
        // Stats
        const StatsGet = 1 << 14;
        const StatsAll = Self::StatsGet.bits();
        // Metrics
        const MetricsGet = 1 << 15;
        const MetricsAll = Self::MetricsGet.bits();
        // Dumps
        const DumpsCreate = 1 << 16;
        const DumpsAll = Self::DumpsCreate.bits();
        // Snapshots
        const SnapshotsCreate = 1 << 17;
        const SnapshotsAll = Self::SnapshotsCreate.bits();
        // Keys without an "all" version
        const Version = 1 << 18;
        const KeysAdd = 1 << 19;
        const KeysGet = 1 << 20;
        const KeysUpdate = 1 << 21;
        const KeysDelete = 1 << 22;
        const ExperimentalFeaturesGet = 1 << 23;
        const ExperimentalFeaturesUpdate = 1 << 24;
        // All
        const All = 0xFFFFFFFF >> (32 - 1 - 24);
    }
}

impl Action {
    const SERDE_MAP_ARR: [(&'static str, Self); 34] = [
        ("search", Self::Search),
        ("documents.add", Self::DocumentsAdd),
        ("documents.get", Self::DocumentsGet),
        ("documents.delete", Self::DocumentsDelete),
        ("documents.*", Self::DocumentsAll),
        ("indexes.create", Self::IndexesAdd),
        ("indexes.get", Self::IndexesGet),
        ("indexes.update", Self::IndexesUpdate),
        ("indexes.delete", Self::IndexesDelete),
        ("indexes.swap", Self::IndexesSwap),
        ("indexes.*", Self::IndexesAll),
        ("tasks.cancel", Self::TasksCancel),
        ("tasks.delete", Self::TasksDelete),
        ("tasks.get", Self::TasksGet),
        ("tasks.*", Self::TasksAll),
        ("settings.get", Self::SettingsGet),
        ("settings.update", Self::SettingsUpdate),
        ("settings.*", Self::SettingsAll),
        ("stats.get", Self::StatsGet),
        ("stats.*", Self::StatsAll),
        ("metrics.get", Self::MetricsGet),
        ("metrics.*", Self::MetricsAll),
        ("dumps.create", Self::DumpsCreate),
        ("dumps.*", Self::DumpsAll),
        ("snapshots.create", Self::SnapshotsCreate),
        ("snapshots.*", Self::SnapshotsAll),
        ("version", Self::Version),
        ("keys.create", Self::KeysAdd),
        ("keys.get", Self::KeysGet),
        ("keys.update", Self::KeysUpdate),
        ("keys.delete", Self::KeysDelete),
        ("experimental.get", Self::ExperimentalFeaturesGet),
        ("experimental.update", Self::ExperimentalFeaturesUpdate),
        ("*", Self::All),
    ];

    fn get_action(v: &str) -> Option<Action> {
        Self::SERDE_MAP_ARR
            .iter()
            .find(|(serde_name, _)| &v == serde_name)
            .map(|(_, action)| *action)
    }

    fn get_action_serde_name(v: &Action) -> &'static str {
        Self::SERDE_MAP_ARR
            .iter()
            .find(|(_, action)| v == action)
            .map(|(serde_name, _)| serde_name)
            .expect("an action is missing a matching serialized value")
    }

    // when we remove "all" flags, this will give us the exact index
    fn get_potential_index(&self) -> usize {
        if self.is_empty() {
            return 0;
        }

        // most significant bit for u32
        let msb = 1u32 << (31 - self.bits().leading_zeros());

        // index of the single set bit
        msb.trailing_zeros() as usize
    }
}

pub mod actions {
    use super::Action as A;

    pub const SEARCH: u32 = A::Search.bits();
    pub const DOCUMENTS_ADD: u32 = A::DocumentsAdd.bits();
    pub const DOCUMENTS_GET: u32 = A::DocumentsGet.bits();
    pub const DOCUMENTS_DELETE: u32 = A::DocumentsDelete.bits();
    pub const DOCUMENTS_ALL: u32 = A::DocumentsAll.bits();
    pub const INDEXES_CREATE: u32 = A::IndexesAdd.bits();
    pub const INDEXES_GET: u32 = A::IndexesGet.bits();
    pub const INDEXES_UPDATE: u32 = A::IndexesUpdate.bits();
    pub const INDEXES_DELETE: u32 = A::IndexesDelete.bits();
    pub const INDEXES_SWAP: u32 = A::IndexesSwap.bits();
    pub const INDEXES_ALL: u32 = A::IndexesAll.bits();
    pub const TASKS_CANCEL: u32 = A::TasksCancel.bits();
    pub const TASKS_DELETE: u32 = A::TasksDelete.bits();
    pub const TASKS_GET: u32 = A::TasksGet.bits();
    pub const TASKS_ALL: u32 = A::TasksAll.bits();
    pub const SETTINGS_GET: u32 = A::SettingsGet.bits();
    pub const SETTINGS_UPDATE: u32 = A::SettingsUpdate.bits();
    pub const SETTINGS_ALL: u32 = A::SettingsAll.bits();
    pub const STATS_GET: u32 = A::StatsGet.bits();
    pub const STATS_ALL: u32 = A::StatsAll.bits();
    pub const METRICS_GET: u32 = A::MetricsGet.bits();
    pub const METRICS_ALL: u32 = A::MetricsAll.bits();
    pub const DUMPS_CREATE: u32 = A::DumpsCreate.bits();
    pub const DUMPS_ALL: u32 = A::DumpsAll.bits();
    pub const SNAPSHOTS_CREATE: u32 = A::SnapshotsCreate.bits();
    pub const SNAPSHOTS_ALL: u32 = A::SnapshotsAll.bits();
    pub const VERSION: u32 = A::Version.bits();
    pub const KEYS_CREATE: u32 = A::KeysAdd.bits();
    pub const KEYS_GET: u32 = A::KeysGet.bits();
    pub const KEYS_UPDATE: u32 = A::KeysUpdate.bits();
    pub const KEYS_DELETE: u32 = A::KeysDelete.bits();
    pub const EXPERIMENTAL_FEATURES_GET: u32 = A::ExperimentalFeaturesGet.bits();
    pub const EXPERIMENTAL_FEATURES_UPDATE: u32 = A::ExperimentalFeaturesUpdate.bits();
    pub const ALL: u32 = A::All.bits();
}

impl<E: DeserializeError> Deserr<E> for Action {
    fn deserialize_from_value<V: deserr::IntoValue>(
        value: deserr::Value<V>,
        location: deserr::ValuePointerRef<'_>,
    ) -> Result<Self, E> {
        match value {
            deserr::Value::String(s) => match Self::get_action(&s) {
                Some(action) => Ok(action),
                None => Err(deserr::take_cf_content(E::error::<std::convert::Infallible>(
                    None,
                    deserr::ErrorKind::UnknownValue {
                        value: &s,
                        accepted: &Self::SERDE_MAP_ARR.map(|(ser_action, _)| ser_action),
                    },
                    location,
                ))),
            },
            _ => Err(take_cf_content(E::error(
                None,
                deserr::ErrorKind::IncorrectValueKind {
                    actual: value,
                    accepted: &[deserr::ValueKind::String],
                },
                location,
            ))),
        }
    }
}

impl Serialize for Action {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(Self::get_action_serde_name(self))
    }
}

impl<'de> Deserialize<'de> for Action {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct Visitor;
        impl<'de> serde::de::Visitor<'de> for Visitor {
            type Value = Action;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                write!(formatter, "the name of a valid action (string)")
            }

            fn visit_str<E>(self, s: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                match Self::Value::get_action(s) {
                    Some(action) => Ok(action),
                    None => Err(E::invalid_value(serde::de::Unexpected::Str(s), &"a valid action")),
                }
            }
        }

        deserializer.deserialize_str(Visitor)
    }
}

// TODO: Once "all" type flags are removed, simplify
//       Essentially `get_potential_index` will give the exact index, +1 the exact next, -1 the exact previous
impl Sequence for Action {
    const CARDINALITY: usize = Self::FLAGS.len();

    fn next(&self) -> Option<Self> {
        let mut potential_next_index = self.get_potential_index() + 1;

        loop {
            if let Some(next_flag) = Self::FLAGS.get(potential_next_index).map(|v| v.value()) {
                if next_flag > self {
                    return Some(*next_flag);
                }

                potential_next_index += 1;
            } else {
                return None;
            }
        }
    }

    fn previous(&self) -> Option<Self> {
        // -2 because of "all" type flags that represent a single flag, otherwise -1 would suffice
        let initial_potential_index = self.get_potential_index();
        if initial_potential_index == 0 {
            return None;
        }

        let mut potential_previous_index: usize =
            if initial_potential_index == 1 { 0 } else { initial_potential_index - 2 };

        let mut previous_item: Option<Self> = None;
        let mut pre_previous_item: Option<Self> = None;

        loop {
            if let Some(next_flag) = Self::FLAGS.get(potential_previous_index).map(|v| v.value()) {
                if next_flag > self {
                    return pre_previous_item;
                }

                pre_previous_item = previous_item;
                previous_item = Some(*next_flag);
                potential_previous_index += 1;
            } else {
                return pre_previous_item;
            }
        }
    }

    fn first() -> Option<Self> {
        Self::FLAGS.first().map(|v| *v.value())
    }

    fn last() -> Option<Self> {
        Self::FLAGS.last().map(|v| *v.value())
    }
}
