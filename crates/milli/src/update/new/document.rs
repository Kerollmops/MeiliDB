use std::collections::{BTreeMap, BTreeSet};

use bumpalo::Bump;
use bumparaw_collections::RawMap;
use heed::RoTxn;
use rustc_hash::FxBuildHasher;
use serde_json::value::RawValue;
use zstd::dict::DecoderDictionary;

use super::vector_document::VectorDocument;
use super::{KvReaderFieldId, KvWriterFieldId};
use crate::documents::FieldIdMapper;
use crate::vector::parsed_vectors::RESERVED_VECTORS_FIELD_NAME;
use crate::{DocumentId, GlobalFieldsIdsMap, Index, InternalError, Result, UserError};

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

    /// Number of top level fields, **excluding** `_vectors` and `_geo`
    fn top_level_fields_count(&self) -> usize;

    /// Get the **top-level** with the specified name, if exists.
    ///
    /// - The `_vectors` and `_geo` fields are **ignored** by this method, meaning e.g. `top_level_field("_vectors")` will return `Ok(None)`
    fn top_level_field(&self, k: &str) -> Result<Option<&'doc RawValue>>;

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

#[derive(Debug)]
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

        std::iter::from_fn(move || loop {
            let (fid, value) = it.next()?;
            let name = match self.fields_ids_map.name(fid).ok_or(
                InternalError::FieldIdMapMissingEntry(crate::FieldIdMapMissingEntry::FieldId {
                    field_id: fid,
                    process: "getting current document",
                }),
            ) {
                Ok(name) => name,
                Err(error) => return Some(Err(error.into())),
            };

            if name == RESERVED_VECTORS_FIELD_NAME || name == "_geo" {
                continue;
            }

            let res = (|| {
                let value =
                    serde_json::from_slice(value).map_err(crate::InternalError::SerdeJson)?;

                Ok((name, value))
            })();

            return Some(res);
        })
    }

    fn vectors_field(&self) -> Result<Option<&'t RawValue>> {
        self.field(RESERVED_VECTORS_FIELD_NAME)
    }

    fn geo_field(&self) -> Result<Option<&'t RawValue>> {
        self.field("_geo")
    }

    fn top_level_fields_count(&self) -> usize {
        let has_vectors_field = self.vectors_field().unwrap_or(None).is_some();
        let has_geo_field = self.geo_field().unwrap_or(None).is_some();
        let count = self.content.iter().count();
        match (has_vectors_field, has_geo_field) {
            (true, true) => count - 2,
            (true, false) | (false, true) => count - 1,
            (false, false) => count,
        }
    }

    fn top_level_field(&self, k: &str) -> Result<Option<&'t RawValue>> {
        if k == RESERVED_VECTORS_FIELD_NAME || k == "_geo" {
            return Ok(None);
        }
        self.field(k)
    }
}

impl<'t, Mapper: FieldIdMapper> DocumentFromDb<'t, Mapper> {
    pub fn new(
        docid: DocumentId,
        rtxn: &'t RoTxn,
        index: &'t Index,
        db_fields_ids_map: &'t Mapper,
        db_document_decompression_dictionary: Option<&DecoderDictionary<'static>>,
        doc_alloc: &'t Bump,
    ) -> Result<Option<Self>> {
        match index.compressed_document(rtxn, docid)? {
            Some(compressed) => {
                let content = match db_document_decompression_dictionary {
                    Some(dictionary) => compressed.decompress_into_bump(doc_alloc, dictionary)?,
                    None => compressed.as_non_compressed(),
                };
                Ok(Some(Self { fields_ids_map: db_fields_ids_map, content }))
            }
            None => Ok(None),
        }
    }

    pub fn field(&self, name: &str) -> Result<Option<&'t RawValue>> {
        let Some(fid) = self.fields_ids_map.id(name) else {
            return Ok(None);
        };
        let Some(value) = self.content.get(fid) else { return Ok(None) };
        Ok(Some(serde_json::from_slice(value).map_err(InternalError::SerdeJson)?))
    }
}

#[derive(Debug)]
pub struct DocumentFromVersions<'a, 'doc> {
    versions: &'a Versions<'doc>,
}

impl<'a, 'doc> DocumentFromVersions<'a, 'doc> {
    pub fn new(versions: &'a Versions<'doc>) -> Self {
        Self { versions }
    }
}

impl<'a, 'doc> Document<'doc> for DocumentFromVersions<'a, 'doc> {
    fn iter_top_level_fields(&self) -> impl Iterator<Item = Result<(&'doc str, &'doc RawValue)>> {
        self.versions.iter_top_level_fields().map(Ok)
    }

    fn vectors_field(&self) -> Result<Option<&'doc RawValue>> {
        Ok(self.versions.vectors_field())
    }

    fn geo_field(&self) -> Result<Option<&'doc RawValue>> {
        Ok(self.versions.geo_field())
    }

    fn top_level_fields_count(&self) -> usize {
        let has_vectors_field = self.vectors_field().unwrap_or(None).is_some();
        let has_geo_field = self.geo_field().unwrap_or(None).is_some();
        let count = self.versions.len();
        match (has_vectors_field, has_geo_field) {
            (true, true) => count - 2,
            (true, false) | (false, true) => count - 1,
            (false, false) => count,
        }
    }

    fn top_level_field(&self, k: &str) -> Result<Option<&'doc RawValue>> {
        Ok(self.versions.top_level_field(k))
    }
}

#[derive(Debug)]
pub struct MergedDocument<'a, 'doc, 't, Mapper: FieldIdMapper> {
    new_doc: DocumentFromVersions<'a, 'doc>,
    db: Option<DocumentFromDb<'t, Mapper>>,
}

impl<'a, 'doc, 't, Mapper: FieldIdMapper> MergedDocument<'a, 'doc, 't, Mapper> {
    pub fn with_db(
        docid: DocumentId,
        rtxn: &'t RoTxn,
        index: &'t Index,
        db_fields_ids_map: &'t Mapper,
        db_document_decompression_dictionary: Option<&'t DecoderDictionary<'static>>,
        doc_alloc: &'t Bump,
        new_doc: DocumentFromVersions<'a, 'doc>,
    ) -> Result<Self> {
        let db = DocumentFromDb::new(
            docid,
            rtxn,
            index,
            db_fields_ids_map,
            db_document_decompression_dictionary,
            doc_alloc,
        )?;
        Ok(Self { new_doc, db })
    }

    pub fn without_db(new_doc: DocumentFromVersions<'a, 'doc>) -> Self {
        Self { new_doc, db: None }
    }
}

impl<'d, 'doc: 'd, 't: 'd, Mapper: FieldIdMapper> Document<'d>
    for MergedDocument<'d, 'doc, 't, Mapper>
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

        match &self.db {
            Some(db) => db.vectors_field(),
            None => Ok(None),
        }
    }

    fn geo_field(&self) -> Result<Option<&'d RawValue>> {
        if let Some(geo) = self.new_doc.geo_field()? {
            return Ok(Some(geo));
        }

        match &self.db {
            Some(db) => db.geo_field(),
            None => Ok(None),
        }
    }

    fn top_level_fields_count(&self) -> usize {
        self.iter_top_level_fields().count()
    }

    fn top_level_field(&self, k: &str) -> Result<Option<&'d RawValue>> {
        if let Some(f) = self.new_doc.top_level_field(k)? {
            return Ok(Some(f));
        }
        if let Some(db) = &self.db {
            return db.field(k);
        }
        Ok(None)
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

    fn top_level_fields_count(&self) -> usize {
        D::top_level_fields_count(self)
    }

    fn top_level_field(&self, k: &str) -> Result<Option<&'doc RawValue>> {
        D::top_level_field(self, k)
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
pub fn write_to_obkv<'s, 'a, 'map, 'buffer>(
    document: &'s impl Document<'s>,
    vector_document: Option<&'s impl VectorDocument<'s>>,
    fields_ids_map: &'a mut GlobalFieldsIdsMap<'map>,
    mut document_buffer: &'a mut bumpalo::collections::Vec<'buffer, u8>,
) -> Result<&'a KvReaderFieldId>
where
    's: 'a,
{
    // will be used in 'inject_vectors
    let vectors_value: Box<RawValue>;

    document_buffer.clear();
    let mut unordered_field_buffer = Vec::new();
    unordered_field_buffer.clear();

    let mut writer = KvWriterFieldId::new(&mut document_buffer);

    for res in document.iter_top_level_fields() {
        let (field_name, value) = res?;
        let field_id =
            fields_ids_map.id_or_insert(field_name).ok_or(UserError::AttributeLimitReached)?;
        unordered_field_buffer.push((field_id, value));
    }

    'inject_vectors: {
        let Some(vector_document) = vector_document else { break 'inject_vectors };

        let mut vectors = BTreeMap::new();
        for res in vector_document.iter_vectors() {
            let (name, entry) = res?;
            if entry.has_configured_embedder {
                continue; // we don't write vectors with configured embedder in documents
            }
            vectors.insert(
                name,
                if entry.implicit {
                    serde_json::json!(entry.embeddings)
                } else {
                    serde_json::json!({
                        "regenerate": entry.regenerate,
                        // TODO: consider optimizing the shape of embedders here to store an array of f32 rather than a JSON object
                        "embeddings": entry.embeddings,
                    })
                },
            );
        }

        if vectors.is_empty() {
            break 'inject_vectors;
        }

        let vectors_fid = fields_ids_map
            .id_or_insert(RESERVED_VECTORS_FIELD_NAME)
            .ok_or(UserError::AttributeLimitReached)?;

        vectors_value = serde_json::value::to_raw_value(&vectors).unwrap();
        unordered_field_buffer.push((vectors_fid, &vectors_value));
    }

    if let Some(geo_value) = document.geo_field()? {
        let fid = fields_ids_map.id_or_insert("_geo").ok_or(UserError::AttributeLimitReached)?;
        fields_ids_map.id_or_insert("_geo.lat").ok_or(UserError::AttributeLimitReached)?;
        fields_ids_map.id_or_insert("_geo.lng").ok_or(UserError::AttributeLimitReached)?;
        unordered_field_buffer.push((fid, geo_value));
    }

    unordered_field_buffer.sort_by_key(|(fid, _)| *fid);
    for (fid, value) in unordered_field_buffer.iter() {
        writer.insert(*fid, value.get().as_bytes()).unwrap();
    }

    writer.finish().unwrap();
    Ok(KvReaderFieldId::from_slice(document_buffer))
}

pub type Entry<'doc> = (&'doc str, &'doc RawValue);

#[derive(Debug)]
pub struct Versions<'doc> {
    data: RawMap<'doc, FxBuildHasher>,
}

impl<'doc> Versions<'doc> {
    pub fn multiple(
        mut versions: impl Iterator<Item = Result<RawMap<'doc, FxBuildHasher>>>,
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

    pub fn single(version: RawMap<'doc, FxBuildHasher>) -> Self {
        Self { data: version }
    }

    pub fn iter_top_level_fields(&self) -> impl Iterator<Item = (&'doc str, &'doc RawValue)> + '_ {
        self.data.iter().filter(|(k, _)| *k != RESERVED_VECTORS_FIELD_NAME && *k != "_geo")
    }

    pub fn vectors_field(&self) -> Option<&'doc RawValue> {
        self.data.get(RESERVED_VECTORS_FIELD_NAME)
    }

    pub fn geo_field(&self) -> Option<&'doc RawValue> {
        self.data.get("_geo")
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    pub fn top_level_field(&self, k: &str) -> Option<&'doc RawValue> {
        if k == RESERVED_VECTORS_FIELD_NAME || k == "_geo" {
            return None;
        }
        self.data.get(k)
    }
}
