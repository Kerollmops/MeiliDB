use std::mem::size_of;
use std::{borrow::Cow, mem::size_of_val};

use byteorder::{BigEndian, ByteOrder};
use heed::{BoxedError, BytesDecode, BytesEncode};

const VERSION_SIZE: usize = std::mem::size_of::<u32>() * 3;

#[derive(thiserror::Error, Debug)]
#[error(
    "Could not decode the version: Expected {} bytes but instead received {0} bytes",
    VERSION_SIZE
)]
pub struct DecodeVersionError(usize);

pub struct VersionCodec;
impl<'a> BytesEncode<'a> for VersionCodec {
    type EItem = (u32, u32, u32);

    fn bytes_encode(item: &'a Self::EItem) -> Result<Cow<'a, [u8]>, BoxedError> {
        let mut ret = Vec::with_capacity(size_of::<u32>() * 3);
        ret.extend(&item.0.to_be_bytes());
        ret.extend(&item.1.to_be_bytes());
        ret.extend(&item.2.to_be_bytes());
        Ok(Cow::Owned(ret))
    }
}
impl<'a> BytesDecode<'a> for VersionCodec {
    type DItem = (u32, u32, u32);

    fn bytes_decode(bytes: &'a [u8]) -> Result<Self::DItem, BoxedError> {
        if bytes.len() != VERSION_SIZE {
            Err(Box::new(DecodeVersionError(bytes.len())))
        } else {
            let major = BigEndian::read_u32(bytes);
            let bytes = &bytes[size_of_val(&major)..];
            let minor = BigEndian::read_u32(bytes);
            let bytes = &bytes[size_of_val(&major)..];
            let patch = BigEndian::read_u32(bytes);

            Ok((major, minor, patch))
        }
    }
}
