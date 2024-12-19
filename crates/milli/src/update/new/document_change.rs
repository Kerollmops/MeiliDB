use bumpalo::Bump;
use heed::RoTxn;
use zstd::dict::DecoderDictionary;

use super::document::{
    Document as _, DocumentFromDb, DocumentFromVersions, MergedDocument, Versions,
};
use super::extract::perm_json_p;
use super::vector_document::{
    MergedVectorDocument, VectorDocumentFromDb, VectorDocumentFromVersions,
};
use crate::documents::FieldIdMapper;
use crate::vector::EmbeddingConfigs;
use crate::{DocumentId, Index, Result};

pub enum DocumentChange<'doc> {
    Deletion(Deletion<'doc>),
    Update(Update<'doc>),
    Insertion(Insertion<'doc>),
}

pub struct Deletion<'doc> {
    docid: DocumentId,
    external_document_id: &'doc str,
}

pub struct Update<'doc> {
    docid: DocumentId,
    external_document_id: &'doc str,
    new: Versions<'doc>,
    has_deletion: bool,
}

pub struct Insertion<'doc> {
    docid: DocumentId,
    external_document_id: &'doc str,
    new: Versions<'doc>,
}

impl<'doc> DocumentChange<'doc> {
    pub fn docid(&self) -> DocumentId {
        match &self {
            Self::Deletion(inner) => inner.docid(),
            Self::Update(inner) => inner.docid(),
            Self::Insertion(inner) => inner.docid(),
        }
    }

    pub fn external_docid(&self) -> &'doc str {
        match self {
            DocumentChange::Deletion(deletion) => deletion.external_document_id(),
            DocumentChange::Update(update) => update.external_document_id(),
            DocumentChange::Insertion(insertion) => insertion.external_document_id(),
        }
    }
}

impl<'doc> Deletion<'doc> {
    pub fn create(docid: DocumentId, external_document_id: &'doc str) -> Self {
        Self { docid, external_document_id }
    }

    pub fn docid(&self) -> DocumentId {
        self.docid
    }

    pub fn external_document_id(&self) -> &'doc str {
        self.external_document_id
    }

    pub fn current<'a, Mapper: FieldIdMapper>(
        &self,
        rtxn: &'a RoTxn,
        index: &'a Index,
        mapper: &'a Mapper,
        dictionary: Option<&'a DecoderDictionary<'static>>,
        doc_alloc: &'a Bump,
    ) -> Result<DocumentFromDb<'a, Mapper>> {
        Ok(DocumentFromDb::new(self.docid, rtxn, index, mapper, dictionary, doc_alloc)?.ok_or(
            crate::error::UserError::UnknownInternalDocumentId { document_id: self.docid },
        )?)
    }
}

impl<'doc> Insertion<'doc> {
    pub fn create(docid: DocumentId, external_document_id: &'doc str, new: Versions<'doc>) -> Self {
        Insertion { docid, external_document_id, new }
    }

    pub fn docid(&self) -> DocumentId {
        self.docid
    }

    pub fn external_document_id(&self) -> &'doc str {
        self.external_document_id
    }

    pub fn inserted(&self) -> DocumentFromVersions<'_, 'doc> {
        DocumentFromVersions::new(&self.new)
    }

    pub fn inserted_vectors(
        &self,
        doc_alloc: &'doc Bump,
        embedders: &'doc EmbeddingConfigs,
    ) -> Result<Option<VectorDocumentFromVersions<'doc>>> {
        VectorDocumentFromVersions::new(self.external_document_id, &self.new, doc_alloc, embedders)
    }
}

impl<'doc> Update<'doc> {
    pub fn create(
        docid: DocumentId,
        external_document_id: &'doc str,
        new: Versions<'doc>,
        has_deletion: bool,
    ) -> Self {
        Update { docid, new, external_document_id, has_deletion }
    }

    pub fn docid(&self) -> DocumentId {
        self.docid
    }

    pub fn external_document_id(&self) -> &'doc str {
        self.external_document_id
    }
    pub fn current<'a, Mapper: FieldIdMapper>(
        &self,
        rtxn: &'a RoTxn,
        index: &'a Index,
        mapper: &'a Mapper,
        dictionary: Option<&'a DecoderDictionary<'static>>,
        doc_alloc: &'a Bump,
    ) -> Result<DocumentFromDb<'a, Mapper>> {
        Ok(DocumentFromDb::new(self.docid, rtxn, index, mapper, dictionary, doc_alloc)?.ok_or(
            crate::error::UserError::UnknownInternalDocumentId { document_id: self.docid },
        )?)
    }

    pub fn current_vectors<'a, Mapper: FieldIdMapper>(
        &self,
        rtxn: &'a RoTxn,
        index: &'a Index,
        mapper: &'a Mapper,
        dictionary: Option<&'a DecoderDictionary<'static>>,
        doc_alloc: &'a Bump,
    ) -> Result<VectorDocumentFromDb<'a>> {
        Ok(VectorDocumentFromDb::new(self.docid, index, rtxn, mapper, dictionary, doc_alloc)?
            .ok_or(crate::error::UserError::UnknownInternalDocumentId {
                document_id: self.docid,
            })?)
    }

    pub fn updated(&self) -> DocumentFromVersions<'_, 'doc> {
        DocumentFromVersions::new(&self.new)
    }

    pub fn merged<'t, Mapper: FieldIdMapper>(
        &self,
        rtxn: &'t RoTxn,
        index: &'t Index,
        mapper: &'t Mapper,
        dictionary: Option<&'t DecoderDictionary<'static>>,
        doc_alloc: &'t Bump,
    ) -> Result<MergedDocument<'_, 'doc, 't, Mapper>> {
        if self.has_deletion {
            Ok(MergedDocument::without_db(DocumentFromVersions::new(&self.new)))
        } else {
            MergedDocument::with_db(
                self.docid,
                rtxn,
                index,
                mapper,
                dictionary,
                doc_alloc,
                DocumentFromVersions::new(&self.new),
            )
        }
    }

    /// Returns whether the updated version of the document is different from the current version for the passed subset of fields.
    ///
    /// `true` if at least one top-level-field that is a exactly a member of field or a parent of a member of field changed.
    /// Otherwise `false`.
    pub fn has_changed_for_fields<'t, Mapper: FieldIdMapper>(
        &self,
        fields: Option<&[&str]>,
        rtxn: &'t RoTxn,
        index: &'t Index,
        mapper: &'t Mapper,
        dictionary: Option<&'t DecoderDictionary<'static>>,
        doc_alloc: &'t Bump,
    ) -> Result<bool> {
        let mut changed = false;
        let mut cached_current = None;
        let mut updated_selected_field_count = 0;

        for entry in self.updated().iter_top_level_fields() {
            let (key, updated_value) = entry?;

            if perm_json_p::select_field(key, fields, &[]) == perm_json_p::Selection::Skip {
                continue;
            }

            updated_selected_field_count += 1;
            let current = match cached_current {
                Some(current) => current,
                None => self.current(rtxn, index, mapper, dictionary, doc_alloc)?,
            };
            let current_value = current.top_level_field(key)?;
            let Some(current_value) = current_value else {
                changed = true;
                break;
            };

            if current_value.get() != updated_value.get() {
                changed = true;
                break;
            }
            cached_current = Some(current);
        }

        if !self.has_deletion {
            // no field deletion, so fields that don't appear in `updated` cannot have changed
            return Ok(changed);
        }

        if changed {
            return Ok(true);
        }

        // we saw all updated fields, and set `changed` if any field wasn't in `current`.
        // so if there are as many fields in `current` as in `updated`, then nothing changed.
        // If there is any more fields in `current`, then they are missing in `updated`.
        let has_deleted_fields = {
            let current = match cached_current {
                Some(current) => current,
                None => self.current(rtxn, index, mapper, dictionary, doc_alloc)?,
            };

            let mut current_selected_field_count = 0;
            for entry in current.iter_top_level_fields() {
                let (key, _) = entry?;

                if perm_json_p::select_field(key, fields, &[]) == perm_json_p::Selection::Skip {
                    continue;
                }
                current_selected_field_count += 1;
            }

            current_selected_field_count != updated_selected_field_count
        };

        Ok(has_deleted_fields)
    }

    pub fn updated_vectors(
        &self,
        doc_alloc: &'doc Bump,
        embedders: &'doc EmbeddingConfigs,
    ) -> Result<Option<VectorDocumentFromVersions<'doc>>> {
        VectorDocumentFromVersions::new(self.external_document_id, &self.new, doc_alloc, embedders)
    }

    pub fn merged_vectors<Mapper: FieldIdMapper>(
        &self,
        rtxn: &'doc RoTxn,
        index: &'doc Index,
        mapper: &'doc Mapper,
        dictionary: Option<&'doc DecoderDictionary<'static>>,
        doc_alloc: &'doc Bump,
        embedders: &'doc EmbeddingConfigs,
    ) -> Result<Option<MergedVectorDocument<'doc>>> {
        if self.has_deletion {
            MergedVectorDocument::without_db(
                self.external_document_id,
                &self.new,
                doc_alloc,
                embedders,
            )
        } else {
            MergedVectorDocument::with_db(
                self.docid,
                self.external_document_id,
                index,
                rtxn,
                mapper,
                dictionary,
                &self.new,
                doc_alloc,
                embedders,
            )
        }
    }
}
