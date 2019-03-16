use std::slice::from_raw_parts;
use std::mem::size_of;
use std::error::Error;

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use sdset::Set;

use crate::shared_data_cursor::{SharedDataCursor, FromSharedDataCursor};
use crate::write_to_bytes::WriteToBytes;
use crate::data::SharedData;
use crate::DocumentId;

use super::into_u8_slice;

#[derive(Default, Clone)]
pub struct DocIds(SharedData);

impl DocIds {
    pub fn new(ids: &Set<DocumentId>) -> DocIds {
        let bytes = unsafe { into_u8_slice(ids.as_slice()) };
        let data = SharedData::from_bytes(bytes.to_vec());
        DocIds(data)
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

impl AsRef<Set<DocumentId>> for DocIds {
    fn as_ref(&self) -> &Set<DocumentId> {
        let slice = &self.0;
        let ptr = slice.as_ptr() as *const DocumentId;
        let len = slice.len() / size_of::<DocumentId>();
        let slice = unsafe { from_raw_parts(ptr, len) };
        Set::new_unchecked(slice)
    }
}

impl FromSharedDataCursor for DocIds {
    type Error = Box<Error>;

    fn from_shared_data_cursor(cursor: &mut SharedDataCursor) -> Result<DocIds, Self::Error> {
        let len = cursor.read_u64::<LittleEndian>()? as usize;
        let data = cursor.extract(len);

        Ok(DocIds(data))
    }
}

impl WriteToBytes for DocIds {
    fn write_to_bytes(&self, bytes: &mut Vec<u8>) {
        let len = self.0.len() as u64;
        bytes.write_u64::<LittleEndian>(len).unwrap();
        bytes.extend_from_slice(&self.0);
    }
}
