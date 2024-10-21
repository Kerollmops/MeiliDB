use std::collections::BTreeSet;

use heed::RoTxn;
use raw_collections::RawMap;
use serde_json::value::RawValue;

use super::{KvReaderFieldId, KvWriterFieldId};
use crate::documents::FieldIdMapper;
use crate::vector::parsed_vectors::RESERVED_VECTORS_FIELD_NAME;
use crate::{DocumentId, Index, InternalError, Result};

/// A view into a document that can represent either the current version from the DB,
/// the update data from payload or other means, or the merged updated version.
///
/// The 'doc lifetime is meant to live sufficiently for the document to be handled by the extractors.
pub trait Document<'doc> {
    /// Iterate over all **top-level** fields of the document, returning their name and raw JSON value.
    ///
    /// - The returned values *may* contain nested fields.
    /// - The `_vectors` and `_geo` fields are **ignored** by this method, meaning  they are **not returned** by this method.
    fn iter_top_level_fields(&self) -> impl Iterator<Item = Result<(&'doc str, &'doc RawValue)>>;

    /// Returns the unparsed value of the `_vectors` field from the document data.
    ///
    /// This field alone is insufficient to retrieve vectors, as they may be stored in a dedicated location in the database.
    /// Use a [`super::vector_document::VectorDocument`] to access the vector.
    ///
    /// This method is meant as a convenience for implementors of [`super::vector_document::VectorDocument`].
    fn vectors_field(&self) -> Result<Option<&'doc RawValue>>;

    /// Returns the unparsed value of the `_geo` field from the document data.
    ///
    /// This field alone is insufficient to retrieve geo data, as they may be stored in a dedicated location in the database.
    /// Use a [`super::geo_document::GeoDocument`] to access the vector.
    ///
    /// This method is meant as a convenience for implementors of [`super::geo_document::GeoDocument`].
    fn geo_field(&self) -> Result<Option<&'doc RawValue>>;
}

pub struct DocumentFromDb<'t, Mapper: FieldIdMapper>
where
    Mapper: FieldIdMapper,
{
    fields_ids_map: &'t Mapper,
    content: &'t KvReaderFieldId,
}

impl<'t, Mapper: FieldIdMapper> Clone for DocumentFromDb<'t, Mapper> {
    #[inline]
    fn clone(&self) -> Self {
        *self
    }
}
impl<'t, Mapper: FieldIdMapper> Copy for DocumentFromDb<'t, Mapper> {}

impl<'t, Mapper: FieldIdMapper> Document<'t> for DocumentFromDb<'t, Mapper> {
    fn iter_top_level_fields(&self) -> impl Iterator<Item = Result<(&'t str, &'t RawValue)>> {
        let mut it = self.content.iter();

        std::iter::from_fn(move || {
            let (fid, value) = it.next()?;

            let res = (|| {
                let value =
                    serde_json::from_slice(value).map_err(crate::InternalError::SerdeJson)?;

                let name = self.fields_ids_map.name(fid).ok_or(
                    InternalError::FieldIdMapMissingEntry(crate::FieldIdMapMissingEntry::FieldId {
                        field_id: fid,
                        process: "getting current document",
                    }),
                )?;
                Ok((name, value))
            })();

            Some(res)
        })
    }

    fn vectors_field(&self) -> Result<Option<&'t RawValue>> {
        self.field(RESERVED_VECTORS_FIELD_NAME)
    }

    fn geo_field(&self) -> Result<Option<&'t RawValue>> {
        self.field("_geo")
    }
}

impl<'t, Mapper: FieldIdMapper> DocumentFromDb<'t, Mapper> {
    pub fn new(
        docid: DocumentId,
        rtxn: &'t RoTxn,
        index: &'t Index,
        db_fields_ids_map: &'t Mapper,
    ) -> Result<Option<Self>> {
        index.documents.get(rtxn, &docid).map_err(crate::Error::from).map(|reader| {
            reader.map(|reader| Self { fields_ids_map: db_fields_ids_map, content: reader })
        })
    }

    pub fn field(&self, name: &str) -> Result<Option<&'t RawValue>> {
        let Some(fid) = self.fields_ids_map.id(name) else {
            return Ok(None);
        };
        let Some(value) = self.content.get(fid) else { return Ok(None) };
        Ok(Some(serde_json::from_slice(value).map_err(InternalError::SerdeJson)?))
    }
}

#[derive(Clone, Copy)]
pub struct DocumentFromVersions<'doc> {
    versions: Versions<'doc>,
}

impl<'doc> DocumentFromVersions<'doc> {
    pub fn new(versions: Versions<'doc>) -> Self {
        Self { versions }
    }
}

impl<'doc> Document<'doc> for DocumentFromVersions<'doc> {
    fn iter_top_level_fields(&self) -> impl Iterator<Item = Result<(&'doc str, &'doc RawValue)>> {
        self.versions.iter_top_level_fields().map(Ok)
    }

    fn vectors_field(&self) -> Result<Option<&'doc RawValue>> {
        Ok(self.versions.vectors_field())
    }

    fn geo_field(&self) -> Result<Option<&'doc RawValue>> {
        Ok(self.versions.geo_field())
    }
}

pub struct MergedDocument<'doc, 't, Mapper: FieldIdMapper> {
    new_doc: DocumentFromVersions<'doc>,
    db: Option<DocumentFromDb<'t, Mapper>>,
}

impl<'doc, 't, Mapper: FieldIdMapper> MergedDocument<'doc, 't, Mapper> {
    pub fn new(
        new_doc: DocumentFromVersions<'doc>,
        db: Option<DocumentFromDb<'t, Mapper>>,
    ) -> Self {
        Self { new_doc, db }
    }

    pub fn with_db(
        docid: DocumentId,
        rtxn: &'t RoTxn,
        index: &'t Index,
        db_fields_ids_map: &'t Mapper,
        new_doc: DocumentFromVersions<'doc>,
    ) -> Result<Self> {
        let db = DocumentFromDb::new(docid, rtxn, index, db_fields_ids_map)?;
        Ok(Self { new_doc, db })
    }

    pub fn without_db(new_doc: DocumentFromVersions<'doc>) -> Self {
        Self { new_doc, db: None }
    }
}

impl<'d, 'doc: 'd, 't: 'd, Mapper: FieldIdMapper> Document<'d>
    for MergedDocument<'doc, 't, Mapper>
{
    fn iter_top_level_fields(&self) -> impl Iterator<Item = Result<(&'d str, &'d RawValue)>> {
        let mut new_doc_it = self.new_doc.iter_top_level_fields();
        let mut db_it = self.db.iter().flat_map(|db| db.iter_top_level_fields());
        let mut seen_fields = BTreeSet::new();

        std::iter::from_fn(move || {
            if let Some(next) = new_doc_it.next() {
                if let Ok((name, _)) = next {
                    seen_fields.insert(name);
                }
                return Some(next);
            }
            loop {
                match db_it.next()? {
                    Ok((name, value)) => {
                        if seen_fields.contains(name) {
                            continue;
                        }
                        return Some(Ok((name, value)));
                    }
                    Err(err) => return Some(Err(err)),
                }
            }
        })
    }

    fn vectors_field(&self) -> Result<Option<&'d RawValue>> {
        if let Some(vectors) = self.new_doc.vectors_field()? {
            return Ok(Some(vectors));
        }

        let Some(db) = self.db else { return Ok(None) };

        db.vectors_field()
    }

    fn geo_field(&self) -> Result<Option<&'d RawValue>> {
        if let Some(geo) = self.new_doc.geo_field()? {
            return Ok(Some(geo));
        }

        let Some(db) = self.db else { return Ok(None) };

        db.geo_field()
    }
}

impl<'doc, D> Document<'doc> for &D
where
    D: Document<'doc>,
{
    fn iter_top_level_fields(&self) -> impl Iterator<Item = Result<(&'doc str, &'doc RawValue)>> {
        D::iter_top_level_fields(self)
    }

    fn vectors_field(&self) -> Result<Option<&'doc RawValue>> {
        D::vectors_field(self)
    }

    fn geo_field(&self) -> Result<Option<&'doc RawValue>> {
        D::geo_field(self)
    }
}

/// Turn this document into an obkv, whose fields are indexed by the provided `FieldIdMapper`.
///
/// The produced obkv is suitable for storing into the documents DB, meaning:
///
/// - It contains the contains of `_vectors` that are not configured as an embedder
/// - It contains all the top-level fields of the document, with their raw JSON value as value.
///
/// # Panics
///
/// - If the document contains a top-level field that is not present in `fields_ids_map`.
///
pub fn write_to_obkv<'s, 'a, 'b>(
    document: &'s impl Document<'s>,
    vector_document: Option<()>,
    fields_ids_map: &'a impl FieldIdMapper,
    mut document_buffer: &'a mut Vec<u8>,
) -> Result<&'a KvReaderFieldId>
where
    's: 'a,
    's: 'b,
{
    // will be used in 'inject_vectors
    let vectors_value: Box<RawValue>;

    document_buffer.clear();
    let mut unordered_field_buffer = Vec::new();
    unordered_field_buffer.clear();

    let mut writer = KvWriterFieldId::new(&mut document_buffer);

    for res in document.iter_top_level_fields() {
        let (field_name, value) = res?;
        let field_id = fields_ids_map.id(field_name).unwrap();
        unordered_field_buffer.push((field_id, value));
    }

    'inject_vectors: {
        let Some(vector_document) = vector_document else { break 'inject_vectors };

        let Some(vectors_fid) = fields_ids_map.id(RESERVED_VECTORS_FIELD_NAME) else {
            break 'inject_vectors;
        };
        /*
        let mut vectors = BTreeMap::new();
        for (name, entry) in vector_document.iter_vectors() {
            if entry.has_configured_embedder {
                continue; // we don't write vectors with configured embedder in documents
            }
            vectors.insert(
                name,
                serde_json::json!({
                    "regenerate": entry.regenerate,
                    // TODO: consider optimizing the shape of embedders here to store an array of f32 rather than a JSON object
                    "embeddings": entry.embeddings,
                }),
            );
        }

        vectors_value = serde_json::value::to_raw_value(&vectors).unwrap();
        unordered_field_buffer.push((vectors_fid, &vectors_value));*/
    }

    unordered_field_buffer.sort_by_key(|(fid, _)| *fid);
    for (fid, value) in unordered_field_buffer.iter() {
        writer.insert(*fid, value.get().as_bytes()).unwrap();
    }

    writer.finish().unwrap();
    Ok(KvReaderFieldId::from_slice(document_buffer))
}

pub type Entry<'doc> = (&'doc str, &'doc RawValue);

#[derive(Clone, Copy)]
pub struct Versions<'doc> {
    data: &'doc [Entry<'doc>],
    vectors: Option<&'doc RawValue>,
    geo: Option<&'doc RawValue>,
}

impl<'doc> Versions<'doc> {
    pub fn multiple(
        mut versions: impl Iterator<Item = Result<RawMap<'doc>>>,
    ) -> Result<Option<Self>> {
        let Some(data) = versions.next() else { return Ok(None) };
        let mut data = data?;
        for future_version in versions {
            let future_version = future_version?;
            for (field, value) in future_version {
                data.insert(field, value);
            }
        }
        Ok(Some(Self::single(data)))
    }

    pub fn single(version: RawMap<'doc>) -> Self {
        let vectors_id = version.get_index(RESERVED_VECTORS_FIELD_NAME);
        let geo_id = version.get_index("_geo");
        let mut data = version.into_vec();
        let geo = geo_id.map(|geo_id| data.remove(geo_id).1);
        let vectors = vectors_id.map(|vectors_id| data.remove(vectors_id).1);

        let data = data.into_bump_slice();

        Self { data, geo, vectors }
    }

    pub fn iter_top_level_fields(&self) -> impl Iterator<Item = Entry<'doc>> {
        self.data.iter().copied()
    }

    pub fn vectors_field(&self) -> Option<&'doc RawValue> {
        self.vectors
    }

    pub fn geo_field(&self) -> Option<&'doc RawValue> {
        self.geo
    }
}