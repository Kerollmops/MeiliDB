use std::collections::BTreeSet;

use bumpalo::Bump;
use bumparaw_collections::RawMap;
use deserr::{Deserr, IntoValue};
use heed::RoTxn;
use rustc_hash::FxBuildHasher;
use serde::Serialize;
use serde_json::value::RawValue;
use zstd::dict::DecoderDictionary;

use super::document::{Document, DocumentFromDb, DocumentFromVersions, Versions};
use super::indexer::de::DeserrRawValue;
use crate::documents::FieldIdMapper;
use crate::index::IndexEmbeddingConfig;
use crate::vector::parsed_vectors::{
    RawVectors, RawVectorsError, VectorOrArrayOfVectors, RESERVED_VECTORS_FIELD_NAME,
};
use crate::vector::{ArroyWrapper, Embedding, EmbeddingConfigs};
use crate::{DocumentId, Index, InternalError, Result, UserError};

#[derive(Serialize)]
#[serde(untagged)]
pub enum Embeddings<'doc> {
    FromJsonExplicit(&'doc RawValue),
    FromJsonImplicityUserProvided(&'doc RawValue),
    FromDb(Vec<Embedding>),
}
impl<'doc> Embeddings<'doc> {
    pub fn into_vec(
        self,
        doc_alloc: &'doc Bump,
        embedder_name: &str,
    ) -> std::result::Result<Vec<Embedding>, deserr::errors::JsonError> {
        match self {
            Embeddings::FromJsonExplicit(value) => {
                let vectors_ref = deserr::ValuePointerRef::Key {
                    key: RESERVED_VECTORS_FIELD_NAME,
                    prev: &deserr::ValuePointerRef::Origin,
                };
                let embedders_ref =
                    deserr::ValuePointerRef::Key { key: embedder_name, prev: &vectors_ref };

                let embeddings_ref =
                    deserr::ValuePointerRef::Key { key: "embeddings", prev: &embedders_ref };

                let v: VectorOrArrayOfVectors = VectorOrArrayOfVectors::deserialize_from_value(
                    DeserrRawValue::new_in(value, doc_alloc).into_value(),
                    embeddings_ref,
                )?;
                Ok(v.into_array_of_vectors().unwrap_or_default())
            }
            Embeddings::FromJsonImplicityUserProvided(value) => {
                let vectors_ref = deserr::ValuePointerRef::Key {
                    key: RESERVED_VECTORS_FIELD_NAME,
                    prev: &deserr::ValuePointerRef::Origin,
                };
                let embedders_ref =
                    deserr::ValuePointerRef::Key { key: embedder_name, prev: &vectors_ref };

                let v: VectorOrArrayOfVectors = VectorOrArrayOfVectors::deserialize_from_value(
                    DeserrRawValue::new_in(value, doc_alloc).into_value(),
                    embedders_ref,
                )?;
                Ok(v.into_array_of_vectors().unwrap_or_default())
            }
            Embeddings::FromDb(vec) => Ok(vec),
        }
    }
}

pub struct VectorEntry<'doc> {
    pub has_configured_embedder: bool,
    pub embeddings: Option<Embeddings<'doc>>,
    pub regenerate: bool,
    pub implicit: bool,
}

pub trait VectorDocument<'doc> {
    fn iter_vectors(&self) -> impl Iterator<Item = Result<(&'doc str, VectorEntry<'doc>)>>;

    fn vectors_for_key(&self, key: &str) -> Result<Option<VectorEntry<'doc>>>;
}

pub struct VectorDocumentFromDb<'t> {
    docid: DocumentId,
    embedding_config: Vec<IndexEmbeddingConfig>,
    index: &'t Index,
    vectors_field: Option<RawMap<'t, FxBuildHasher>>,
    rtxn: &'t RoTxn<'t>,
    doc_alloc: &'t Bump,
}

impl<'t> VectorDocumentFromDb<'t> {
    pub fn new<Mapper: FieldIdMapper>(
        docid: DocumentId,
        index: &'t Index,
        rtxn: &'t RoTxn,
        db_fields_ids_map: &'t Mapper,
        db_document_decompression_dictionary: Option<&'t DecoderDictionary<'static>>,
        doc_alloc: &'t Bump,
    ) -> Result<Option<Self>> {
        let Some(document) = DocumentFromDb::new(
            docid,
            rtxn,
            index,
            db_fields_ids_map,
            db_document_decompression_dictionary,
            doc_alloc,
        )?
        else {
            return Ok(None);
        };
        let vectors = document.vectors_field()?;
        let vectors_field = match vectors {
            Some(vectors) => Some(
                RawMap::from_raw_value_and_hasher(vectors, FxBuildHasher, doc_alloc)
                    .map_err(InternalError::SerdeJson)?,
            ),
            None => None,
        };

        let embedding_config = index.embedding_configs(rtxn)?;

        Ok(Some(Self { docid, embedding_config, index, vectors_field, rtxn, doc_alloc }))
    }

    fn entry_from_db(
        &self,
        embedder_id: u8,
        config: &IndexEmbeddingConfig,
    ) -> Result<VectorEntry<'t>> {
        let reader =
            ArroyWrapper::new(self.index.vector_arroy, embedder_id, config.config.quantized());
        let vectors = reader.item_vectors(self.rtxn, self.docid)?;

        Ok(VectorEntry {
            has_configured_embedder: true,
            embeddings: Some(Embeddings::FromDb(vectors)),
            regenerate: !config.user_provided.contains(self.docid),
            implicit: false,
        })
    }
}

impl<'t> VectorDocument<'t> for VectorDocumentFromDb<'t> {
    fn iter_vectors(&self) -> impl Iterator<Item = Result<(&'t str, VectorEntry<'t>)>> {
        self.embedding_config
            .iter()
            .map(|config| {
                let embedder_id =
                    self.index.embedder_category_id.get(self.rtxn, &config.name)?.unwrap();
                let entry = self.entry_from_db(embedder_id, config)?;
                let config_name = self.doc_alloc.alloc_str(config.name.as_str());
                Ok((&*config_name, entry))
            })
            .chain(self.vectors_field.iter().flat_map(|map| map.iter()).map(|(name, value)| {
                Ok((
                    name,
                    entry_from_raw_value(value, false).map_err(|_| {
                        InternalError::Serialization(crate::SerializationError::Decoding {
                            db_name: Some(crate::index::db_name::VECTOR_ARROY),
                        })
                    })?,
                ))
            }))
    }

    fn vectors_for_key(&self, key: &str) -> Result<Option<VectorEntry<'t>>> {
        Ok(match self.index.embedder_category_id.get(self.rtxn, key)? {
            Some(embedder_id) => {
                let config =
                    self.embedding_config.iter().find(|config| config.name == key).unwrap();
                Some(self.entry_from_db(embedder_id, config)?)
            }
            None => match self.vectors_field.as_ref().and_then(|obkv| obkv.get(key)) {
                Some(embedding_from_doc) => {
                    Some(entry_from_raw_value(embedding_from_doc, false).map_err(|_| {
                        InternalError::Serialization(crate::SerializationError::Decoding {
                            db_name: Some(crate::index::db_name::VECTOR_ARROY),
                        })
                    })?)
                }
                None => None,
            },
        })
    }
}

fn entry_from_raw_value_user<'doc>(
    external_docid: &str,
    embedder_name: &str,
    value: &'doc RawValue,
    has_configured_embedder: bool,
) -> Result<VectorEntry<'doc>> {
    entry_from_raw_value(value, has_configured_embedder).map_err(|error| {
        UserError::InvalidVectorsEmbedderConf {
            document_id: external_docid.to_string(),
            error: error.msg(embedder_name),
        }
        .into()
    })
}

fn entry_from_raw_value(
    value: &RawValue,
    has_configured_embedder: bool,
) -> std::result::Result<VectorEntry<'_>, RawVectorsError> {
    let value: RawVectors = RawVectors::from_raw_value(value)?;

    Ok(match value {
        RawVectors::Explicit(raw_explicit_vectors) => VectorEntry {
            has_configured_embedder,
            embeddings: raw_explicit_vectors.embeddings.map(Embeddings::FromJsonExplicit),
            regenerate: raw_explicit_vectors.regenerate,
            implicit: false,
        },
        RawVectors::ImplicitlyUserProvided(value) => VectorEntry {
            has_configured_embedder,
            // implicitly user provided always provide embeddings
            // `None` here means that there are no embeddings
            embeddings: Some(
                value
                    .map(Embeddings::FromJsonImplicityUserProvided)
                    .unwrap_or(Embeddings::FromDb(Default::default())),
            ),
            regenerate: false,
            implicit: true,
        },
    })
}

pub struct VectorDocumentFromVersions<'doc> {
    external_document_id: &'doc str,
    vectors: RawMap<'doc, FxBuildHasher>,
    embedders: &'doc EmbeddingConfigs,
}

impl<'doc> VectorDocumentFromVersions<'doc> {
    pub fn new(
        external_document_id: &'doc str,
        versions: &Versions<'doc>,
        bump: &'doc Bump,
        embedders: &'doc EmbeddingConfigs,
    ) -> Result<Option<Self>> {
        let document = DocumentFromVersions::new(versions);
        if let Some(vectors_field) = document.vectors_field()? {
            let vectors = RawMap::from_raw_value_and_hasher(vectors_field, FxBuildHasher, bump)
                .map_err(UserError::SerdeJson)?;
            Ok(Some(Self { external_document_id, vectors, embedders }))
        } else {
            Ok(None)
        }
    }
}

impl<'doc> VectorDocument<'doc> for VectorDocumentFromVersions<'doc> {
    fn iter_vectors(&self) -> impl Iterator<Item = Result<(&'doc str, VectorEntry<'doc>)>> {
        self.vectors.iter().map(|(embedder, vectors)| {
            let vectors = entry_from_raw_value_user(
                self.external_document_id,
                embedder,
                vectors,
                self.embedders.contains(embedder),
            )?;
            Ok((embedder, vectors))
        })
    }

    fn vectors_for_key(&self, key: &str) -> Result<Option<VectorEntry<'doc>>> {
        let Some(vectors) = self.vectors.get(key) else { return Ok(None) };
        let vectors = entry_from_raw_value_user(
            self.external_document_id,
            key,
            vectors,
            self.embedders.contains(key),
        )?;
        Ok(Some(vectors))
    }
}

pub struct MergedVectorDocument<'doc> {
    new_doc: Option<VectorDocumentFromVersions<'doc>>,
    db: Option<VectorDocumentFromDb<'doc>>,
}

impl<'doc> MergedVectorDocument<'doc> {
    #[allow(clippy::too_many_arguments)]
    pub fn with_db<Mapper: FieldIdMapper>(
        docid: DocumentId,
        external_document_id: &'doc str,
        index: &'doc Index,
        rtxn: &'doc RoTxn,
        db_fields_ids_map: &'doc Mapper,
        db_document_decompression_dictionary: Option<&'doc DecoderDictionary<'static>>,
        versions: &Versions<'doc>,
        doc_alloc: &'doc Bump,
        embedders: &'doc EmbeddingConfigs,
    ) -> Result<Option<Self>> {
        let db = VectorDocumentFromDb::new(
            docid,
            index,
            rtxn,
            db_fields_ids_map,
            db_document_decompression_dictionary,
            doc_alloc,
        )?;
        let new_doc =
            VectorDocumentFromVersions::new(external_document_id, versions, doc_alloc, embedders)?;
        Ok(if db.is_none() && new_doc.is_none() { None } else { Some(Self { new_doc, db }) })
    }

    pub fn without_db(
        external_document_id: &'doc str,
        versions: &Versions<'doc>,
        doc_alloc: &'doc Bump,
        embedders: &'doc EmbeddingConfigs,
    ) -> Result<Option<Self>> {
        let Some(new_doc) =
            VectorDocumentFromVersions::new(external_document_id, versions, doc_alloc, embedders)?
        else {
            return Ok(None);
        };
        Ok(Some(Self { new_doc: Some(new_doc), db: None }))
    }
}

impl<'doc> VectorDocument<'doc> for MergedVectorDocument<'doc> {
    fn iter_vectors(&self) -> impl Iterator<Item = Result<(&'doc str, VectorEntry<'doc>)>> {
        let mut new_doc_it = self.new_doc.iter().flat_map(|new_doc| new_doc.iter_vectors());
        let mut db_it = self.db.iter().flat_map(|db| db.iter_vectors());
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

    fn vectors_for_key(&self, key: &str) -> Result<Option<VectorEntry<'doc>>> {
        if let Some(new_doc) = &self.new_doc {
            if let Some(entry) = new_doc.vectors_for_key(key)? {
                return Ok(Some(entry));
            }
        }

        let Some(db) = self.db.as_ref() else { return Ok(None) };
        db.vectors_for_key(key)
    }
}
