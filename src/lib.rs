pub mod automaton;
pub mod database;
pub mod rank;
pub mod tokenizer;

mod data;

use std::fmt;

pub use rocksdb;
pub use self::tokenizer::Tokenizer;

/// Represent an internally generated document unique identifier.
///
/// It is used to inform the database the document you want to deserialize.
/// Helpful for custom ranking.
#[derive(Debug, Copy, Clone, Eq, PartialEq, PartialOrd, Ord, Hash)]
pub struct DocumentId(pub u64);

/// Represent an attribute number along with the word index
/// according to the tokenizer used.
///
/// It can accept up to 1024 attributes and word positions
/// can be maximum 2^22.
#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Attribute(u32);

impl Attribute {
    /// Construct an `Attribute` from an attribute number and
    /// the word position of a match according to the tokenizer used.
    ///
    /// # Panics
    ///
    /// The attribute must not be greater than 1024
    /// and the word index not greater than 2^22.
    fn new(attribute: u16, index: u32) -> Attribute {
        assert!(attribute & 0b1111_1100_0000_0000 == 0);
        assert!(index & 0b1111_1111_1100_0000_0000_0000_0000 == 0);

        let attribute = (attribute as u32) << 22;
        Attribute(attribute | index)
    }

    pub fn attribute(&self) -> u16 {
        (self.0 >> 22) as u16
    }

    pub fn word_index(&self) -> u32 {
        self.0 & 0b0000_0000_0011_1111_1111_1111_1111
    }
}

impl fmt::Debug for Attribute {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Attribute")
            .field("attribute", &self.attribute())
            .field("word_index", &self.word_index())
            .finish()
    }
}

/// Represent a word position in bytes along with the length of it.
///
/// It can represent words byte index to maximum 2^22 and
/// up to words of length 1024.
#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct WordArea(u32);

impl WordArea {
    /// Construct a `WordArea` from a word position in bytes
    /// and the length of it.
    ///
    /// # Panics
    ///
    /// The byte index must not be greater than 2^22
    /// and the length not greater than 1024.
    fn new(byte_index: u32, length: u16) -> WordArea {
        assert!(byte_index & 0b1111_1111_1100_0000_0000_0000_0000 == 0);
        assert!(length & 0b1111_1100_0000_0000 == 0);

        let byte_index = byte_index << 10;
        WordArea(byte_index | (length as u32))
    }

    pub fn byte_index(&self) -> u32 {
        self.0 >> 10
    }

    pub fn length(&self) -> u16 {
        (self.0 & 0b0000_0000_0000_0000_0011_1111_1111) as u16
    }
}

impl fmt::Debug for WordArea {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("WordArea")
            .field("byte_index", &self.byte_index())
            .field("length", &self.length())
            .finish()
    }
}

/// This structure represent the position of a word
/// in a document and its attributes.
///
/// This is stored in the map, generated at index time,
/// extracted and interpreted at search time.
#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(C)]
pub struct DocIndex {
    /// The document identifier where the word was found.
    pub document_id: DocumentId,

    /// The attribute in the document where the word was found
    /// along with the index in it.
    pub attribute: Attribute,

    /// The position in bytes where the word was found
    /// along with the length of it.
    ///
    /// It informs on the original word area in the text indexed
    /// without needing to run the tokenizer again.
    pub word_area: WordArea,
}

/// This structure represent a matching word with informations
/// on the location of the word in the document.
///
/// The order of the field is important because it defines
/// the way these structures are ordered between themselves.
///
/// The word in itself is not important.
// TODO do data oriented programming ? very arrays ?
#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Match {
    /// The word index in the query sentence.
    /// Same as the `attribute_index` but for the query words.
    ///
    /// Used to retrieve the automaton that match this word.
    pub query_index: u32,

    /// The distance the word has with the query word
    /// (i.e. the Levenshtein distance).
    pub distance: u8,

    /// The attribute in the document where the word was found
    /// along with the index in it.
    pub attribute: Attribute,

    /// Whether the word that match is an exact match or a prefix.
    pub is_exact: bool,

    /// The position in bytes where the word was found
    /// along with the length of it.
    ///
    /// It informs on the original word area in the text indexed
    /// without needing to run the tokenizer again.
    pub word_area: WordArea,
}

impl Match {
    pub fn zero() -> Self {
        Match {
            query_index: 0,
            distance: 0,
            attribute: Attribute::new(0, 0),
            is_exact: false,
            word_area: WordArea::new(0, 0),
        }
    }

    pub fn max() -> Self {
        Match {
            query_index: u32::max_value(),
            distance: u8::max_value(),
            attribute: Attribute(u32::max_value()),
            is_exact: true,
            word_area: WordArea(u32::max_value()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use quickcheck::{quickcheck, TestResult};
    use std::mem;

    #[test]
    fn docindex_mem_size() {
        assert_eq!(mem::size_of::<DocIndex>(), 16);
    }

    quickcheck! {
        fn qc_attribute(gen_attr: u16, gen_index: u32) -> TestResult {
            if gen_attr > 2_u16.pow(10) || gen_index > 2_u32.pow(22) {
                return TestResult::discard()
            }

            let attribute = Attribute::new(gen_attr, gen_index);

            let valid_attribute = attribute.attribute() == gen_attr;
            let valid_index = attribute.word_index() == gen_index;

            TestResult::from_bool(valid_attribute && valid_index)
        }

        fn qc_attribute_ord(gen_attr: u16, gen_index: u32) -> TestResult {
            if gen_attr >= 2_u16.pow(10) || gen_index >= 2_u32.pow(22) {
                return TestResult::discard()
            }

            let a = Attribute::new(gen_attr, gen_index);
            let b = Attribute::new(gen_attr + 1, gen_index + 1);

            TestResult::from_bool(a < b)
        }

        fn qc_word_area(gen_byte_index: u32, gen_length: u16) -> TestResult {
            if gen_byte_index > 2_u32.pow(22) || gen_length > 2_u16.pow(10) {
                return TestResult::discard()
            }

            let word_area = WordArea::new(gen_byte_index, gen_length);

            let valid_char_index = word_area.byte_index() == gen_byte_index;
            let valid_length = word_area.length() == gen_length;

            TestResult::from_bool(valid_char_index && valid_length)
        }

        fn qc_word_area_ord(gen_byte_index: u32, gen_length: u16) -> TestResult {
            if gen_byte_index >= 2_u32.pow(22) || gen_length >= 2_u16.pow(10) {
                return TestResult::discard()
            }

            let a = WordArea::new(gen_byte_index, gen_length);
            let b = WordArea::new(gen_byte_index + 1, gen_length + 1);

            TestResult::from_bool(a < b)
        }
    }
}
