mod beu16_str_codec;
mod beu32_str_codec;
mod byte_slice_ref;
pub mod facet;
mod field_id_word_count_codec;
mod fst_set_codec;
mod obkv_codec;
mod roaring_bitmap;
mod roaring_bitmap_length;
mod str_beu32_codec;
mod str_ref;
mod str_str_u8_codec;
pub mod version;

pub use byte_slice_ref::BytesRefCodec;
use heed::BoxedError;
pub use str_ref::StrRefCodec;
use thiserror::Error;

pub use self::beu16_str_codec::BEU16StrCodec;
pub use self::beu32_str_codec::BEU32StrCodec;
pub use self::field_id_word_count_codec::FieldIdWordCountCodec;
pub use self::fst_set_codec::FstSetCodec;
pub use self::obkv_codec::ObkvCodec;
pub use self::roaring_bitmap::{BoRoaringBitmapCodec, CboRoaringBitmapCodec, RoaringBitmapCodec};
pub use self::roaring_bitmap_length::{
    BoRoaringBitmapLenCodec, CboRoaringBitmapLenCodec, RoaringBitmapLenCodec,
};
pub use self::str_beu32_codec::{StrBEU16Codec, StrBEU32Codec};
pub use self::str_str_u8_codec::{U8StrStrCodec, UncheckedU8StrStrCodec};

pub trait BytesDecodeOwned {
    type DItem;

    fn bytes_decode_owned(bytes: &[u8]) -> Result<Self::DItem, BoxedError>;
}

#[derive(Error, Debug)]
#[error("the slice is too short")]
pub struct SliceTooShortError;
