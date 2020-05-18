use std::fmt::Write as _;
use std::hash::{Hash, Hasher};

use indexmap::IndexMap;
use meilisearch_schema::IndexedPos;
use meilisearch_types::DocumentId;
use serde_json::Value;
use siphasher::sip::SipHasher;

use crate::raw_indexer::RawIndexer;
use crate::serde::SerializerError;
use crate::Number;

/// Returns the number of words indexed or `None` if the type
pub fn index_value(
    indexer: &mut RawIndexer,
    document_id: DocumentId,
    indexed_pos: IndexedPos,
    value: &Value,
) -> Option<usize>
{
    match value {
        Value::Null => None,
        Value::Bool(boolean) => {
            let text = boolean.to_string();
            let number_of_words = indexer.index_text(document_id, indexed_pos, &text);
            Some(number_of_words)
        },
        Value::Number(number) => {
            let text = number.to_string();
            Some(indexer.index_text(document_id, indexed_pos, &text))
        },
        Value::String(string) => {
            Some(indexer.index_text(document_id, indexed_pos, &string))
        },
        Value::Array(_) => {
            let text = value_to_string(value);
            Some(indexer.index_text(document_id, indexed_pos, &text))
        },
        Value::Object(_) => {
            let text = value_to_string(value);
            Some(indexer.index_text(document_id, indexed_pos, &text))
        },
    }
}

/// Transforms the JSON Value type into a String.
pub fn value_to_string(value: &Value) -> String {
    fn internal_value_to_string(string: &mut String, value: &Value) {
        match value {
            Value::Null => (),
            Value::Bool(boolean) => { let _ = write!(string, "{}", &boolean); },
            Value::Number(number) => { let _ = write!(string, "{}", &number); },
            Value::String(text) => string.push_str(&text),
            Value::Array(array) => {
                for value in array {
                    internal_value_to_string(string, value);
                    let _ = string.write_str(". ");
                }
            },
            Value::Object(object) => {
                for (key, value) in object {
                    string.push_str(key);
                    let _ = string.write_str(". ");
                    internal_value_to_string(string, value);
                    let _ = string.write_str(". ");
                }
            },
        }
    }

    let mut string = String::new();
    internal_value_to_string(&mut string, value);
    string
}

/// Transforms the JSON Value type into a Number.
pub fn value_to_number(value: &Value) -> Option<Number> {
    use std::str::FromStr;

    match value {
        Value::Null => None,
        Value::Bool(boolean) => Some(Number::Unsigned(*boolean as u64)),
        Value::Number(number) => Number::from_str(&number.to_string()).ok(), // TODO improve that
        Value::String(string) => Number::from_str(string).ok(),
        Value::Array(_array) => None,
        Value::Object(_object) => None,
    }
}

/// Validates a string representation to be a correct document id and
/// returns the hash of the given type, this is the way we produce documents ids.
pub fn compute_document_id(string: &str) -> Result<DocumentId, SerializerError> {
    if string.chars().all(|x| x.is_ascii_alphanumeric() || x == '-' || x == '_') {
        let mut s = SipHasher::new();
        string.hash(&mut s);
        Ok(DocumentId(s.finish()))
    } else {
        Err(SerializerError::InvalidDocumentIdFormat)
    }
}

/// Extracts and validates the document id of a document.
pub fn extract_document_id(primary_key: &str, document: &IndexMap<String, Value>) -> Result<DocumentId, SerializerError> {
    match document.get(primary_key) {
        Some(value) => {
            let string = match value {
                Value::Number(number) => number.to_string(),
                Value::String(string) => string.clone(),
                _ => return Err(SerializerError::InvalidDocumentIdFormat),
            };
            compute_document_id(&string)
        }
        None => Err(SerializerError::DocumentIdNotFound),
    }
}
