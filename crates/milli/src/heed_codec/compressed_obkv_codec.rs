use std::borrow::Cow;
use std::io;
use std::io::ErrorKind;

use bumpalo::Bump;
use heed::BoxedError;
use obkv::KvReaderU16;
use zstd::bulk::{Compressor, Decompressor};
use zstd::dict::{DecoderDictionary, EncoderDictionary};

pub struct CompressedObkvCodec;

impl<'a> heed::BytesDecode<'a> for CompressedObkvCodec {
    type DItem = CompressedKvReaderU16<'a>;

    fn bytes_decode(bytes: &'a [u8]) -> Result<Self::DItem, BoxedError> {
        Ok(CompressedKvReaderU16(bytes))
    }
}

impl heed::BytesEncode<'_> for CompressedObkvCodec {
    type EItem = CompressedObkvU16;

    fn bytes_encode(item: &Self::EItem) -> Result<Cow<[u8]>, BoxedError> {
        Ok(Cow::Borrowed(&item.0))
    }
}

// TODO Make this an unsized slice wrapper instead?
//      &'a CompressedKvReaderU16([u8])
pub struct CompressedKvReaderU16<'a>(&'a [u8]);

impl<'a> CompressedKvReaderU16<'a> {
    /// Decompresses the KvReader into the buffer using the provided dictionnary.
    pub fn decompress_with<'b>(
        &self,
        buffer: &'b mut Vec<u8>,
        dictionary: &DecoderDictionary,
    ) -> io::Result<&'b KvReaderU16> {
        const TWO_GIGABYTES: usize = 2 * 1024 * 1024 * 1024;

        let mut decompressor = Decompressor::with_prepared_dictionary(dictionary)?;
        let mut max_size = self.0.len() * 4;
        let size = loop {
            buffer.resize(max_size, 0);
            match decompressor.decompress_to_buffer(self.0, &mut buffer[..max_size]) {
                Ok(size) => break size,
                // TODO don't do that !!! But what should I do?
                Err(e) if e.kind() == ErrorKind::Other && max_size <= TWO_GIGABYTES => {
                    max_size *= 2
                }
                Err(e) => return Err(e),
            }
        };
        Ok(KvReaderU16::from_slice(&buffer[..size]))
    }

    pub fn decompress_into_bump<'b>(
        &self,
        bump: &'b Bump,
        dictionary: &DecoderDictionary,
    ) -> io::Result<&'b KvReaderU16> {
        let mut buffer = Vec::new();
        self.decompress_with(&mut buffer, dictionary)?;
        Ok(KvReaderU16::from_slice(bump.alloc_slice_copy(&buffer)))
    }

    /// Returns the KvReader like it is not compressed.
    /// Happends when there is no dictionary yet.
    pub fn as_non_compressed(&self) -> &'a KvReaderU16 {
        KvReaderU16::from_slice(self.0)
    }

    /// Decompresses this KvReader if necessary.
    pub fn decompress_with_optional_dictionary<'b>(
        &self,
        buffer: &'b mut Vec<u8>,
        dictionary: Option<&DecoderDictionary>,
    ) -> io::Result<&'b KvReaderU16>
    where
        'a: 'b,
    {
        match dictionary {
            Some(dict) => self.decompress_with(buffer, dict),
            None => Ok(self.as_non_compressed()),
        }
    }

    pub fn into_owned_with_dictionary(
        &self,
        dictionary: &DecoderDictionary<'_>,
    ) -> io::Result<Box<KvReaderU16>> {
        let mut buffer = Vec::new();
        let reader = self.decompress_with(&mut buffer, dictionary)?;
        // Make sure the Vec is exactly the size of the reader
        let size = reader.as_bytes().len();
        buffer.resize(size, 0);
        Ok(buffer.into_boxed_slice().into())
    }
}

pub struct CompressedObkvU16(Vec<u8>);

impl CompressedObkvU16 {
    pub fn with_dictionary(
        input: &KvReaderU16,
        dictionary: &EncoderDictionary,
    ) -> io::Result<Self> {
        let mut compressor = Compressor::with_prepared_dictionary(dictionary)?;
        Self::with_compressor(input, &mut compressor)
    }

    pub fn with_compressor(input: &KvReaderU16, compressor: &mut Compressor) -> io::Result<Self> {
        compressor.compress(input.as_bytes()).map(CompressedObkvU16)
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}
