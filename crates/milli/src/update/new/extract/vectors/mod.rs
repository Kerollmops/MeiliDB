use std::cell::RefCell;

use bumpalo::collections::Vec as BVec;
use bumpalo::Bump;
use hashbrown::{DefaultHashBuilder, HashMap};

use super::cache::DelAddRoaringBitmap;
use crate::error::FaultSource;
use crate::prompt::Prompt;
use crate::update::new::channel::EmbeddingSender;
use crate::update::new::indexer::document_changes::{DocumentChangeContext, Extractor};
use crate::update::new::thread_local::MostlySend;
use crate::update::new::vector_document::VectorDocument;
use crate::update::new::DocumentChange;
use crate::vector::error::{
    EmbedErrorKind, PossibleEmbeddingMistakes, UnusedVectorsDistributionBump,
};
use crate::vector::{Embedder, Embedding, EmbeddingConfigs};
use crate::{DocumentId, FieldDistribution, InternalError, Result, ThreadPoolNoAbort, UserError};

pub struct EmbeddingExtractor<'a, 'b> {
    embedders: &'a EmbeddingConfigs,
    sender: EmbeddingSender<'a, 'b>,
    possible_embedding_mistakes: PossibleEmbeddingMistakes,
    threads: &'a ThreadPoolNoAbort,
}

impl<'a, 'b> EmbeddingExtractor<'a, 'b> {
    pub fn new(
        embedders: &'a EmbeddingConfigs,
        sender: EmbeddingSender<'a, 'b>,
        field_distribution: &'a FieldDistribution,
        threads: &'a ThreadPoolNoAbort,
    ) -> Self {
        let possible_embedding_mistakes = PossibleEmbeddingMistakes::new(field_distribution);
        Self { embedders, sender, threads, possible_embedding_mistakes }
    }
}

pub struct EmbeddingExtractorData<'extractor>(
    pub HashMap<String, DelAddRoaringBitmap, DefaultHashBuilder, &'extractor Bump>,
);

unsafe impl MostlySend for EmbeddingExtractorData<'_> {}

impl<'a, 'b, 'extractor> Extractor<'extractor> for EmbeddingExtractor<'a, 'b> {
    type Data = RefCell<EmbeddingExtractorData<'extractor>>;

    fn init_data<'doc>(&'doc self, extractor_alloc: &'extractor Bump) -> crate::Result<Self::Data> {
        Ok(RefCell::new(EmbeddingExtractorData(HashMap::new_in(extractor_alloc))))
    }

    fn process<'doc>(
        &'doc self,
        changes: impl Iterator<Item = crate::Result<DocumentChange<'doc>>>,
        context: &'doc DocumentChangeContext<Self::Data>,
    ) -> crate::Result<()> {
        let embedders = self.embedders.inner_as_ref();
        let mut unused_vectors_distribution =
            UnusedVectorsDistributionBump::new_in(&context.doc_alloc);

        let mut all_chunks = BVec::with_capacity_in(embedders.len(), &context.doc_alloc);
        for (embedder_name, (embedder, prompt, _is_quantized)) in embedders {
            let embedder_id =
                context.index.embedder_category_id.get(&context.rtxn, embedder_name)?.ok_or_else(
                    || InternalError::DatabaseMissingEntry {
                        db_name: "embedder_category_id",
                        key: None,
                    },
                )?;
            all_chunks.push(Chunks::new(
                embedder,
                embedder_id,
                embedder_name,
                prompt,
                context.data,
                &self.possible_embedding_mistakes,
                self.threads,
                self.sender,
                &context.doc_alloc,
            ))
        }

        for change in changes {
            let change = change?;
            match change {
                DocumentChange::Deletion(deletion) => {
                    // vector deletion is handled by document sender,
                    // we still need to accomodate deletion from user_provided
                    for chunks in &mut all_chunks {
                        // regenerate: true means we delete from user_provided
                        chunks.set_regenerate(deletion.docid(), true);
                    }
                }
                DocumentChange::Update(update) => {
                    let old_vectors = update.current_vectors(
                        &context.rtxn,
                        context.index,
                        context.db_fields_ids_map,
                        context.db_document_decompression_dictionary,
                        &context.doc_alloc,
                    )?;
                    let new_vectors = update.updated_vectors(&context.doc_alloc, self.embedders)?;

                    if let Some(new_vectors) = &new_vectors {
                        unused_vectors_distribution.append(new_vectors)?;
                    }

                    for chunks in &mut all_chunks {
                        let embedder_name = chunks.embedder_name();
                        let prompt = chunks.prompt();

                        let old_vectors = old_vectors.vectors_for_key(embedder_name)?.unwrap();
                        if let Some(new_vectors) = new_vectors.as_ref().and_then(|new_vectors| {
                            new_vectors.vectors_for_key(embedder_name).transpose()
                        }) {
                            let new_vectors = new_vectors?;
                            if old_vectors.regenerate != new_vectors.regenerate {
                                chunks.set_regenerate(update.docid(), new_vectors.regenerate);
                            }
                            // do we have set embeddings?
                            if let Some(embeddings) = new_vectors.embeddings {
                                chunks.set_vectors(
                                    update.docid(),
                                    embeddings
                                        .into_vec(&context.doc_alloc, embedder_name)
                                        .map_err(|error| UserError::InvalidVectorsEmbedderConf {
                                            document_id: update.external_document_id().to_string(),
                                            error: error.to_string(),
                                        })?,
                                );
                            } else if new_vectors.regenerate {
                                let new_rendered = prompt.render_document(
                                    update.external_document_id(),
                                    update.current(
                                        &context.rtxn,
                                        context.index,
                                        context.db_fields_ids_map,
                                        context.db_document_decompression_dictionary,
                                        &context.doc_alloc,
                                    )?,
                                    context.new_fields_ids_map,
                                    &context.doc_alloc,
                                )?;
                                let old_rendered = prompt.render_document(
                                    update.external_document_id(),
                                    update.merged(
                                        &context.rtxn,
                                        context.index,
                                        context.db_fields_ids_map,
                                        context.db_document_decompression_dictionary,
                                        &context.doc_alloc,
                                    )?,
                                    context.new_fields_ids_map,
                                    &context.doc_alloc,
                                )?;
                                if new_rendered != old_rendered {
                                    chunks.set_autogenerated(
                                        update.docid(),
                                        update.external_document_id(),
                                        new_rendered,
                                        &unused_vectors_distribution,
                                    )?;
                                }
                            }
                        } else if old_vectors.regenerate {
                            let old_rendered = prompt.render_document(
                                update.external_document_id(),
                                update.current(
                                    &context.rtxn,
                                    context.index,
                                    context.db_fields_ids_map,
                                    context.db_document_decompression_dictionary,
                                    &context.doc_alloc,
                                )?,
                                context.new_fields_ids_map,
                                &context.doc_alloc,
                            )?;
                            let new_rendered = prompt.render_document(
                                update.external_document_id(),
                                update.merged(
                                    &context.rtxn,
                                    context.index,
                                    context.db_fields_ids_map,
                                    context.db_document_decompression_dictionary,
                                    &context.doc_alloc,
                                )?,
                                context.new_fields_ids_map,
                                &context.doc_alloc,
                            )?;
                            if new_rendered != old_rendered {
                                chunks.set_autogenerated(
                                    update.docid(),
                                    update.external_document_id(),
                                    new_rendered,
                                    &unused_vectors_distribution,
                                )?;
                            }
                        }
                    }
                }
                DocumentChange::Insertion(insertion) => {
                    let new_vectors =
                        insertion.inserted_vectors(&context.doc_alloc, self.embedders)?;
                    if let Some(new_vectors) = &new_vectors {
                        unused_vectors_distribution.append(new_vectors)?;
                    }

                    for chunks in &mut all_chunks {
                        let embedder_name = chunks.embedder_name();
                        let prompt = chunks.prompt();
                        // if no inserted vectors, then regenerate: true + no embeddings => autogenerate
                        if let Some(new_vectors) = new_vectors.as_ref().and_then(|new_vectors| {
                            new_vectors.vectors_for_key(embedder_name).transpose()
                        }) {
                            let new_vectors = new_vectors?;
                            chunks.set_regenerate(insertion.docid(), new_vectors.regenerate);
                            if let Some(embeddings) = new_vectors.embeddings {
                                chunks.set_vectors(
                                    insertion.docid(),
                                    embeddings
                                        .into_vec(&context.doc_alloc, embedder_name)
                                        .map_err(|error| UserError::InvalidVectorsEmbedderConf {
                                            document_id: insertion
                                                .external_document_id()
                                                .to_string(),
                                            error: error.to_string(),
                                        })?,
                                );
                            } else if new_vectors.regenerate {
                                let rendered = prompt.render_document(
                                    insertion.external_document_id(),
                                    insertion.inserted(),
                                    context.new_fields_ids_map,
                                    &context.doc_alloc,
                                )?;
                                chunks.set_autogenerated(
                                    insertion.docid(),
                                    insertion.external_document_id(),
                                    rendered,
                                    &unused_vectors_distribution,
                                )?;
                            }
                        } else {
                            let rendered = prompt.render_document(
                                insertion.external_document_id(),
                                insertion.inserted(),
                                context.new_fields_ids_map,
                                &context.doc_alloc,
                            )?;
                            chunks.set_autogenerated(
                                insertion.docid(),
                                insertion.external_document_id(),
                                rendered,
                                &unused_vectors_distribution,
                            )?;
                        }
                    }
                }
            }
        }

        for chunk in all_chunks {
            chunk.drain(&unused_vectors_distribution)?;
        }
        Ok(())
    }
}

// **Warning**: the destructor of this struct is not normally run, make sure that all its fields:
// 1. don't have side effects tied to they destructors
// 2. if allocated, are allocated inside of the bumpalo
//
// Currently this is the case as:
// 1. BVec are inside of the bumaplo
// 2. All other fields are either trivial (u8) or references.
struct Chunks<'a, 'b, 'extractor> {
    texts: BVec<'a, &'a str>,
    ids: BVec<'a, DocumentId>,

    embedder: &'a Embedder,
    embedder_id: u8,
    embedder_name: &'a str,
    prompt: &'a Prompt,
    possible_embedding_mistakes: &'a PossibleEmbeddingMistakes,
    user_provided: &'a RefCell<EmbeddingExtractorData<'extractor>>,
    threads: &'a ThreadPoolNoAbort,
    sender: EmbeddingSender<'a, 'b>,
    has_manual_generation: Option<&'a str>,
}

impl<'a, 'b, 'extractor> Chunks<'a, 'b, 'extractor> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        embedder: &'a Embedder,
        embedder_id: u8,
        embedder_name: &'a str,
        prompt: &'a Prompt,
        user_provided: &'a RefCell<EmbeddingExtractorData<'extractor>>,
        possible_embedding_mistakes: &'a PossibleEmbeddingMistakes,
        threads: &'a ThreadPoolNoAbort,
        sender: EmbeddingSender<'a, 'b>,
        doc_alloc: &'a Bump,
    ) -> Self {
        let capacity = embedder.prompt_count_in_chunk_hint() * embedder.chunk_count_hint();
        let texts = BVec::with_capacity_in(capacity, doc_alloc);
        let ids = BVec::with_capacity_in(capacity, doc_alloc);
        Self {
            texts,
            ids,
            embedder,
            prompt,
            possible_embedding_mistakes,
            threads,
            sender,
            embedder_id,
            embedder_name,
            user_provided,
            has_manual_generation: None,
        }
    }

    pub fn set_autogenerated(
        &mut self,
        docid: DocumentId,
        external_docid: &'a str,
        rendered: &'a str,
        unused_vectors_distribution: &UnusedVectorsDistributionBump,
    ) -> Result<()> {
        let is_manual = matches!(&self.embedder, &Embedder::UserProvided(_));
        if is_manual {
            self.has_manual_generation.get_or_insert(external_docid);
        }

        if self.texts.len() < self.texts.capacity() {
            self.texts.push(rendered);
            self.ids.push(docid);
            return Ok(());
        }

        Self::embed_chunks(
            &mut self.texts,
            &mut self.ids,
            self.embedder,
            self.embedder_id,
            self.embedder_name,
            self.possible_embedding_mistakes,
            unused_vectors_distribution,
            self.threads,
            self.sender,
            self.has_manual_generation.take(),
        )
    }

    pub fn drain(
        mut self,
        unused_vectors_distribution: &UnusedVectorsDistributionBump,
    ) -> Result<()> {
        let res = Self::embed_chunks(
            &mut self.texts,
            &mut self.ids,
            self.embedder,
            self.embedder_id,
            self.embedder_name,
            self.possible_embedding_mistakes,
            unused_vectors_distribution,
            self.threads,
            self.sender,
            self.has_manual_generation,
        );
        // optimization: don't run bvec dtors as they only contain bumpalo allocated stuff
        std::mem::forget(self);
        res
    }

    #[allow(clippy::too_many_arguments)]
    pub fn embed_chunks(
        texts: &mut BVec<'a, &'a str>,
        ids: &mut BVec<'a, DocumentId>,
        embedder: &Embedder,
        embedder_id: u8,
        embedder_name: &str,
        possible_embedding_mistakes: &PossibleEmbeddingMistakes,
        unused_vectors_distribution: &UnusedVectorsDistributionBump,
        threads: &ThreadPoolNoAbort,
        sender: EmbeddingSender<'a, 'b>,
        has_manual_generation: Option<&'a str>,
    ) -> Result<()> {
        if let Some(external_docid) = has_manual_generation {
            let mut msg = format!(
                r"While embedding documents for embedder `{embedder_name}`: no vectors provided for document `{}`{}",
                external_docid,
                if ids.len() > 1 {
                    format!(" and at least {} other document(s)", ids.len() - 1)
                } else {
                    "".to_string()
                }
            );

            msg += &format!("\n- Note: `{embedder_name}` has `source: userProvided`, so documents must provide embeddings as an array in `_vectors.{embedder_name}`.");

            let mut hint_count = 0;

            for (vector_misspelling, count) in possible_embedding_mistakes.vector_mistakes().take(2)
            {
                msg += &format!("\n- Hint: try replacing `{vector_misspelling}` by `_vectors` in {count} document(s).");
                hint_count += 1;
            }

            for (embedder_misspelling, count) in possible_embedding_mistakes
                .embedder_mistakes_bump(embedder_name, unused_vectors_distribution)
                .take(2)
            {
                msg += &format!("\n- Hint: try replacing `_vectors.{embedder_misspelling}` by `_vectors.{embedder_name}` in {count} document(s).");
                hint_count += 1;
            }

            if hint_count == 0 {
                msg += &format!(
                    "\n- Hint: opt-out for a document with `_vectors.{embedder_name}: null`"
                );
            }

            return Err(crate::Error::UserError(crate::UserError::DocumentEmbeddingError(msg)));
        }

        let res = match embedder.embed_chunks_ref(texts.as_slice(), threads) {
            Ok(embeddings) => {
                for (docid, embedding) in ids.into_iter().zip(embeddings) {
                    sender.set_vector(*docid, embedder_id, embedding).unwrap();
                }
                Ok(())
            }
            Err(error) => {
                if let FaultSource::Bug = error.fault {
                    Err(crate::Error::InternalError(crate::InternalError::VectorEmbeddingError(
                        error.into(),
                    )))
                } else {
                    let mut msg = format!(
                        r"While embedding documents for embedder `{embedder_name}`: {error}"
                    );

                    if let EmbedErrorKind::ManualEmbed(_) = &error.kind {
                        msg += &format!("\n- Note: `{embedder_name}` has `source: userProvided`, so documents must provide embeddings as an array in `_vectors.{embedder_name}`.");
                    }

                    let mut hint_count = 0;

                    for (vector_misspelling, count) in
                        possible_embedding_mistakes.vector_mistakes().take(2)
                    {
                        msg += &format!("\n- Hint: try replacing `{vector_misspelling}` by `_vectors` in {count} document(s).");
                        hint_count += 1;
                    }

                    for (embedder_misspelling, count) in possible_embedding_mistakes
                        .embedder_mistakes_bump(embedder_name, unused_vectors_distribution)
                        .take(2)
                    {
                        msg += &format!("\n- Hint: try replacing `_vectors.{embedder_misspelling}` by `_vectors.{embedder_name}` in {count} document(s).");
                        hint_count += 1;
                    }

                    if hint_count == 0 {
                        if let EmbedErrorKind::ManualEmbed(_) = &error.kind {
                            msg += &format!(
                                "\n- Hint: opt-out for a document with `_vectors.{embedder_name}: null`"
                            );
                        }
                    }

                    Err(crate::Error::UserError(crate::UserError::DocumentEmbeddingError(msg)))
                }
            }
        };
        texts.clear();
        ids.clear();
        res
    }

    pub fn prompt(&self) -> &'a Prompt {
        self.prompt
    }

    pub fn embedder_name(&self) -> &'a str {
        self.embedder_name
    }

    fn set_regenerate(&self, docid: DocumentId, regenerate: bool) {
        let mut user_provided = self.user_provided.borrow_mut();
        let user_provided = user_provided.0.entry_ref(self.embedder_name).or_default();
        if regenerate {
            // regenerate == !user_provided
            user_provided.insert_del_u32(docid);
        } else {
            user_provided.insert_add_u32(docid);
        }
    }

    fn set_vectors(&self, docid: DocumentId, embeddings: Vec<Embedding>) {
        self.sender.set_vectors(docid, self.embedder_id, embeddings).unwrap();
    }
}
