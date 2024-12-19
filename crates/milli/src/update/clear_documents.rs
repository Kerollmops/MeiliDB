use heed::RwTxn;
use roaring::RoaringBitmap;
use time::OffsetDateTime;

use crate::{FieldDistribution, Index, Result};

pub struct ClearDocuments<'t, 'i> {
    wtxn: &'t mut RwTxn<'i>,
    index: &'i Index,
}

impl<'t, 'i> ClearDocuments<'t, 'i> {
    pub fn new(wtxn: &'t mut RwTxn<'i>, index: &'i Index) -> ClearDocuments<'t, 'i> {
        ClearDocuments { wtxn, index }
    }

    #[tracing::instrument(
        level = "trace",
        skip(self),
        target = "indexing::documents",
        name = "clear_documents"
    )]
    pub fn execute(self) -> Result<u64> {
        self.index.set_updated_at(self.wtxn, &OffsetDateTime::now_utc())?;
        let Index {
            env: _env,
            main: _main,
            external_documents_ids,
            word_docids,
            exact_word_docids,
            word_prefix_docids,
            exact_word_prefix_docids,
            word_pair_proximity_docids,
            word_position_docids,
            word_fid_docids,
            field_id_word_count_docids,
            word_prefix_position_docids,
            word_prefix_fid_docids,
            facet_id_f64_docids,
            facet_id_string_docids,
            facet_id_normalized_string_strings,
            facet_id_string_fst,
            facet_id_exists_docids,
            facet_id_is_null_docids,
            facet_id_is_empty_docids,
            field_id_docid_facet_f64s,
            field_id_docid_facet_strings,
            vector_arroy,
            embedder_category_id: _,
            documents,
        } = self.index;

        let empty_roaring = RoaringBitmap::default();

        // We retrieve the number of documents ids that we are deleting.
        let number_of_documents = self.index.number_of_documents(self.wtxn)?;

        // We clean some of the main engine datastructures.
        self.index.put_words_fst(self.wtxn, &fst::Set::default())?;
        self.index.put_words_prefixes_fst(self.wtxn, &fst::Set::default())?;
        self.index.put_documents_ids(self.wtxn, &empty_roaring)?;
        self.index.put_field_distribution(self.wtxn, &FieldDistribution::default())?;
        self.index.delete_geo_rtree(self.wtxn)?;
        self.index.delete_geo_faceted_documents_ids(self.wtxn)?;
        self.index.delete_document_compression_dictionary(self.wtxn)?;

        // Remove all user-provided bits from the configs
        let mut configs = self.index.embedding_configs(self.wtxn)?;
        for config in configs.iter_mut() {
            config.user_provided.clear();
        }
        self.index.put_embedding_configs(self.wtxn, configs)?;

        // Clear the other databases.
        external_documents_ids.clear(self.wtxn)?;
        word_docids.clear(self.wtxn)?;
        exact_word_docids.clear(self.wtxn)?;
        word_prefix_docids.clear(self.wtxn)?;
        exact_word_prefix_docids.clear(self.wtxn)?;
        word_pair_proximity_docids.clear(self.wtxn)?;
        word_position_docids.clear(self.wtxn)?;
        word_fid_docids.clear(self.wtxn)?;
        field_id_word_count_docids.clear(self.wtxn)?;
        word_prefix_position_docids.clear(self.wtxn)?;
        word_prefix_fid_docids.clear(self.wtxn)?;
        facet_id_f64_docids.clear(self.wtxn)?;
        facet_id_normalized_string_strings.clear(self.wtxn)?;
        facet_id_string_fst.clear(self.wtxn)?;
        facet_id_exists_docids.clear(self.wtxn)?;
        facet_id_is_null_docids.clear(self.wtxn)?;
        facet_id_is_empty_docids.clear(self.wtxn)?;
        facet_id_string_docids.clear(self.wtxn)?;
        field_id_docid_facet_f64s.clear(self.wtxn)?;
        field_id_docid_facet_strings.clear(self.wtxn)?;
        // vector
        vector_arroy.clear(self.wtxn)?;

        documents.clear(self.wtxn)?;

        Ok(number_of_documents)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::tests::TempIndex;

    #[test]
    fn clear_documents() {
        let index = TempIndex::new();

        let mut wtxn = index.write_txn().unwrap();
        index
            .add_documents_using_wtxn(&mut wtxn, documents!([
                { "id": 0, "name": "kevin", "age": 20 },
                { "id": 1, "name": "kevina" },
                { "id": 2, "name": "benoit", "country": "France", "_geo": { "lng": 42, "lat": 35 } }
            ]))
            .unwrap();

        // Clear all documents from the database.
        let builder = ClearDocuments::new(&mut wtxn, &index);
        assert_eq!(builder.execute().unwrap(), 3);
        wtxn.commit().unwrap();

        let rtxn = index.read_txn().unwrap();

        // the value is 7 because there is `[id, name, age, country, _geo, _geo.lng, _geo.lat]`
        assert_eq!(index.fields_ids_map(&rtxn).unwrap().len(), 7);

        assert!(index.words_fst(&rtxn).unwrap().is_empty());
        assert!(index.words_prefixes_fst(&rtxn).unwrap().is_empty());
        assert!(index.external_documents_ids().is_empty(&rtxn).unwrap());
        assert!(index.documents_ids(&rtxn).unwrap().is_empty());
        assert!(index.field_distribution(&rtxn).unwrap().is_empty());
        assert!(index.geo_rtree(&rtxn).unwrap().is_none());
        assert!(index.geo_faceted_documents_ids(&rtxn).unwrap().is_empty());

        assert!(index.word_docids.is_empty(&rtxn).unwrap());
        assert!(index.word_prefix_docids.is_empty(&rtxn).unwrap());
        assert!(index.word_pair_proximity_docids.is_empty(&rtxn).unwrap());
        assert!(index.field_id_word_count_docids.is_empty(&rtxn).unwrap());
        assert!(index.facet_id_f64_docids.is_empty(&rtxn).unwrap());
        assert!(index.facet_id_string_docids.is_empty(&rtxn).unwrap());
        assert!(index.field_id_docid_facet_f64s.is_empty(&rtxn).unwrap());
        assert!(index.field_id_docid_facet_strings.is_empty(&rtxn).unwrap());
        assert!(index.documents.is_empty(&rtxn).unwrap());
    }
}
