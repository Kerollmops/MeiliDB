use std::fs::File;
use std::io::{BufReader, Read};

use flate2::bufread::GzDecoder;
use serde::Deserialize;
use tempfile::TempDir;

use self::compat::v4_to_v5::CompatV4ToV5;
use self::compat::v5_to_v6::{CompatIndexV5ToV6, CompatV5ToV6};
use self::v5::V5Reader;
use self::v6::{V6IndexReader, V6Reader};
use crate::{Result, Version};

mod compat;

mod v1;
mod v2;
mod v3;
mod v4;
mod v5;
mod v6;

pub type Document = serde_json::Map<String, serde_json::Value>;
pub type UpdateFile = dyn Iterator<Item = Result<Document>>;

pub enum DumpReader {
    Current(V6Reader),
    Compat(CompatV5ToV6),
}

impl DumpReader {
    pub fn open(dump: impl Read) -> Result<DumpReader> {
        let path = TempDir::new()?;
        let mut dump = BufReader::new(dump);
        let gz = GzDecoder::new(&mut dump);
        let mut archive = tar::Archive::new(gz);
        archive.unpack(path.path())?;

        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct MetadataVersion {
            pub dump_version: Version,
        }
        let mut meta_file = File::open(path.path().join("metadata.json"))?;
        let MetadataVersion { dump_version } = serde_json::from_reader(&mut meta_file)?;

        match dump_version {
            Version::V1 => {
                Ok(v1::V1Reader::open(path)?.to_v2().to_v3().to_v4().to_v5().to_v6().into())
            }
            Version::V2 => Ok(v2::V2Reader::open(path)?.to_v3().to_v4().to_v5().to_v6().into()),
            Version::V3 => Ok(v3::V3Reader::open(path)?.to_v4().to_v5().to_v6().into()),
            Version::V4 => Ok(v4::V4Reader::open(path)?.to_v5().to_v6().into()),
            Version::V5 => Ok(v5::V5Reader::open(path)?.to_v6().into()),
            Version::V6 => Ok(v6::V6Reader::open(path)?.into()),
        }
    }

    pub fn version(&self) -> crate::Version {
        match self {
            DumpReader::Current(current) => current.version(),
            DumpReader::Compat(compat) => compat.version(),
        }
    }

    pub fn date(&self) -> Option<time::OffsetDateTime> {
        match self {
            DumpReader::Current(current) => current.date(),
            DumpReader::Compat(compat) => compat.date(),
        }
    }

    pub fn instance_uid(&self) -> Result<Option<uuid::Uuid>> {
        match self {
            DumpReader::Current(current) => current.instance_uid(),
            DumpReader::Compat(compat) => compat.instance_uid(),
        }
    }

    pub fn indexes(&self) -> Result<Box<dyn Iterator<Item = Result<DumpIndexReader>> + '_>> {
        match self {
            DumpReader::Current(current) => {
                let indexes = Box::new(current.indexes()?.map(|res| res.map(DumpIndexReader::from)))
                    as Box<dyn Iterator<Item = Result<DumpIndexReader>> + '_>;
                Ok(indexes)
            }
            DumpReader::Compat(compat) => {
                let indexes = Box::new(compat.indexes()?.map(|res| res.map(DumpIndexReader::from)))
                    as Box<dyn Iterator<Item = Result<DumpIndexReader>> + '_>;
                Ok(indexes)
            }
        }
    }

    pub fn tasks(
        &mut self,
    ) -> Result<Box<dyn Iterator<Item = Result<(v6::Task, Option<Box<UpdateFile>>)>> + '_>> {
        match self {
            DumpReader::Current(current) => Ok(current.tasks()),
            DumpReader::Compat(compat) => compat.tasks(),
        }
    }

    pub fn keys(&mut self) -> Result<Box<dyn Iterator<Item = Result<v6::Key>> + '_>> {
        match self {
            DumpReader::Current(current) => Ok(current.keys()),
            DumpReader::Compat(compat) => compat.keys(),
        }
    }

    pub fn features(&self) -> Result<Option<v6::RuntimeTogglableFeatures>> {
        match self {
            DumpReader::Current(current) => Ok(current.features()),
            DumpReader::Compat(compat) => compat.features(),
        }
    }
}

impl From<V6Reader> for DumpReader {
    fn from(value: V6Reader) -> Self {
        DumpReader::Current(value)
    }
}

impl From<CompatV5ToV6> for DumpReader {
    fn from(value: CompatV5ToV6) -> Self {
        DumpReader::Compat(value)
    }
}

impl From<V5Reader> for DumpReader {
    fn from(value: V5Reader) -> Self {
        DumpReader::Compat(value.to_v6())
    }
}

impl From<CompatV4ToV5> for DumpReader {
    fn from(value: CompatV4ToV5) -> Self {
        DumpReader::Compat(value.to_v6())
    }
}

pub enum DumpIndexReader {
    Current(v6::V6IndexReader),
    Compat(Box<CompatIndexV5ToV6>),
}

impl DumpIndexReader {
    pub fn new_v6(v6: v6::V6IndexReader) -> DumpIndexReader {
        DumpIndexReader::Current(v6)
    }

    pub fn metadata(&self) -> &crate::IndexMetadata {
        match self {
            DumpIndexReader::Current(v6) => v6.metadata(),
            DumpIndexReader::Compat(compat) => compat.metadata(),
        }
    }

    pub fn documents(&mut self) -> Result<Box<dyn Iterator<Item = Result<Document>> + '_>> {
        match self {
            DumpIndexReader::Current(v6) => v6
                .documents()
                .map(|iter| Box::new(iter) as Box<dyn Iterator<Item = Result<Document>> + '_>),
            DumpIndexReader::Compat(compat) => compat
                .documents()
                .map(|iter| Box::new(iter) as Box<dyn Iterator<Item = Result<Document>> + '_>),
        }
    }

    pub fn settings(&mut self) -> Result<v6::Settings<v6::Checked>> {
        match self {
            DumpIndexReader::Current(v6) => v6.settings(),
            DumpIndexReader::Compat(compat) => compat.settings(),
        }
    }
}

impl From<V6IndexReader> for DumpIndexReader {
    fn from(value: V6IndexReader) -> Self {
        DumpIndexReader::Current(value)
    }
}

impl From<CompatIndexV5ToV6> for DumpIndexReader {
    fn from(value: CompatIndexV5ToV6) -> Self {
        DumpIndexReader::Compat(Box::new(value))
    }
}

#[cfg(test)]
pub(crate) mod test {
    use std::fs::File;

    use meili_snap::insta;

    use super::*;
    use crate::reader::v6::RuntimeTogglableFeatures;

    #[test]
    fn import_dump_v6_with_vectors() {
        // dump containing two indexes
        //
        // "vector", configured with an embedder
        // contains:
        // - one document with an overriden vector,
        // - one document with a natural vector
        // - one document with a _vectors map containing one additional embedder name and a natural vector
        // - one document with a _vectors map containing one additional embedder name and an overriden vector
        //
        // "novector", no embedder
        // contains:
        // - a document without vector
        // - a document with a random _vectors field
        let dump = File::open("tests/assets/v6-with-vectors.dump").unwrap();
        let mut dump = DumpReader::open(dump).unwrap();

        // top level infos
        insta::assert_snapshot!(dump.date().unwrap(), @"2024-05-16 15:51:34.151044 +00:00:00");
        insta::assert_debug_snapshot!(dump.instance_uid().unwrap(), @"None");

        // tasks
        let tasks = dump.tasks().unwrap().collect::<Result<Vec<_>>>().unwrap();
        let (tasks, update_files): (Vec<_>, Vec<_>) = tasks.into_iter().unzip();
        meili_snap::snapshot_hash!(meili_snap::json_string!(tasks), @"2b8a72d6bc6ba79980491966437daaf9");
        assert_eq!(update_files.len(), 10);
        assert!(update_files[0].is_none()); // the dump creation
        assert!(update_files[1].is_none());
        assert!(update_files[2].is_none());
        assert!(update_files[3].is_none());
        assert!(update_files[4].is_none());
        assert!(update_files[5].is_none());
        assert!(update_files[6].is_none());
        assert!(update_files[7].is_none());
        assert!(update_files[8].is_none());
        assert!(update_files[9].is_none());

        // indexes
        let mut indexes = dump.indexes().unwrap().collect::<Result<Vec<_>>>().unwrap();
        // the index are not ordered in any way by default
        indexes.sort_by_key(|index| index.metadata().uid.to_string());

        let mut vector_index = indexes.pop().unwrap();
        let mut novector_index = indexes.pop().unwrap();
        assert!(indexes.is_empty());

        // vector

        insta::assert_json_snapshot!(vector_index.metadata(), @r###"
        {
          "uid": "vector",
          "primaryKey": "id",
          "createdAt": "2024-05-16T15:33:17.240962Z",
          "updatedAt": "2024-05-16T15:40:55.723052Z"
        }
        "###);

        insta::assert_json_snapshot!(vector_index.settings().unwrap());

        {
            let documents: Result<Vec<_>> = vector_index.documents().unwrap().collect();
            let mut documents = documents.unwrap();
            assert_eq!(documents.len(), 4);

            documents.sort_by_key(|doc| doc.get("id").unwrap().to_string());

            {
                let document = documents.pop().unwrap();
                insta::assert_json_snapshot!(document);
            }

            {
                let document = documents.pop().unwrap();
                insta::assert_json_snapshot!(document);
            }

            {
                let document = documents.pop().unwrap();
                insta::assert_json_snapshot!(document);
            }

            {
                let document = documents.pop().unwrap();
                insta::assert_json_snapshot!(document);
            }
        }

        // novector

        insta::assert_json_snapshot!(novector_index.metadata(), @r###"
        {
          "uid": "novector",
          "primaryKey": "id",
          "createdAt": "2024-05-16T15:33:03.568055Z",
          "updatedAt": "2024-05-16T15:33:07.530217Z"
        }
        "###);

        insta::assert_json_snapshot!(novector_index.settings().unwrap().embedders, @"null");

        {
            let documents: Result<Vec<_>> = novector_index.documents().unwrap().collect();
            let mut documents = documents.unwrap();
            assert_eq!(documents.len(), 2);

            documents.sort_by_key(|doc| doc.get("id").unwrap().to_string());

            {
                let document = documents.pop().unwrap();
                insta::assert_json_snapshot!(document, @r###"
                {
                  "id": "e1",
                  "other": "random1",
                  "_vectors": "toto"
                }
                "###);
            }

            {
                let document = documents.pop().unwrap();
                insta::assert_json_snapshot!(document, @r###"
                {
                  "id": "e0",
                  "other": "random0"
                }
                "###);
            }
        }

        assert_eq!(dump.features().unwrap().unwrap(), RuntimeTogglableFeatures::default());
    }

    #[test]
    fn import_dump_v6_experimental() {
        let dump = File::open("tests/assets/v6-with-experimental.dump").unwrap();
        let mut dump = DumpReader::open(dump).unwrap();

        // top level infos
        insta::assert_snapshot!(dump.date().unwrap(), @"2023-07-06 7:10:27.21958 +00:00:00");
        insta::assert_debug_snapshot!(dump.instance_uid().unwrap(), @"None");

        // tasks
        let tasks = dump.tasks().unwrap().collect::<Result<Vec<_>>>().unwrap();
        let (tasks, update_files): (Vec<_>, Vec<_>) = tasks.into_iter().unzip();
        meili_snap::snapshot_hash!(meili_snap::json_string!(tasks), @"3ddf6169b0a3703c5d770971f036fc5d");
        assert_eq!(update_files.len(), 2);
        assert!(update_files[0].is_none()); // the dump creation
        assert!(update_files[1].is_none()); // the processed document addition

        // keys
        let keys = dump.keys().unwrap().collect::<Result<Vec<_>>>().unwrap();
        meili_snap::snapshot_hash!(meili_snap::json_string!(keys), @"13c2da155e9729c2344688cab29af71d");

        // indexes
        let mut indexes = dump.indexes().unwrap().collect::<Result<Vec<_>>>().unwrap();
        // the index are not ordered in any way by default
        indexes.sort_by_key(|index| index.metadata().uid.to_string());

        let mut test = indexes.pop().unwrap();
        assert!(indexes.is_empty());

        insta::assert_json_snapshot!(test.metadata(), @r###"
        {
          "uid": "test",
          "primaryKey": "id",
          "createdAt": "2023-07-06T07:07:41.364694Z",
          "updatedAt": "2023-07-06T07:07:41.396114Z"
        }
        "###);

        assert_eq!(test.documents().unwrap().count(), 1);

        assert_eq!(dump.features().unwrap().unwrap(), RuntimeTogglableFeatures::default());
    }

    #[test]
    fn import_dump_v5() {
        let dump = File::open("tests/assets/v5.dump").unwrap();
        let mut dump = DumpReader::open(dump).unwrap();

        // top level infos
        insta::assert_snapshot!(dump.date().unwrap(), @"2022-10-04 15:55:10.344982459 +00:00:00");
        insta::assert_snapshot!(dump.instance_uid().unwrap().unwrap(), @"9e15e977-f2ae-4761-943f-1eaf75fd736d");

        // tasks
        let tasks = dump.tasks().unwrap().collect::<Result<Vec<_>>>().unwrap();
        let (tasks, update_files): (Vec<_>, Vec<_>) = tasks.into_iter().unzip();
        meili_snap::snapshot_hash!(meili_snap::json_string!(tasks), @"4b03e23e740b27bfb9d2a1faffe512e2");
        assert_eq!(update_files.len(), 22);
        assert!(update_files[0].is_none()); // the dump creation
        assert!(update_files[1].is_some()); // the enqueued document addition
        assert!(update_files[2..].iter().all(|u| u.is_none())); // everything already processed

        // keys
        let keys = dump.keys().unwrap().collect::<Result<Vec<_>>>().unwrap();
        meili_snap::snapshot_hash!(meili_snap::json_string!(keys), @"c9d2b467fe2fca0b35580d8a999808fb");

        // indexes
        let mut indexes = dump.indexes().unwrap().collect::<Result<Vec<_>>>().unwrap();
        // the index are not ordered in any way by default
        indexes.sort_by_key(|index| index.metadata().uid.to_string());

        let mut products = indexes.pop().unwrap();
        let mut movies = indexes.pop().unwrap();
        let mut spells = indexes.pop().unwrap();
        assert!(indexes.is_empty());

        // products
        insta::assert_json_snapshot!(products.metadata(), @r###"
        {
          "uid": "products",
          "primaryKey": "sku",
          "createdAt": "2022-10-04T15:51:35.939396731Z",
          "updatedAt": "2022-10-04T15:55:01.897325373Z"
        }
        "###);

        insta::assert_json_snapshot!(products.settings().unwrap());
        let documents = products.documents().unwrap().collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(documents.len(), 10);
        meili_snap::snapshot_hash!(format!("{:#?}", documents), @"b01c8371aea4c7171af0d4d846a2bdca");

        // movies
        insta::assert_json_snapshot!(movies.metadata(), @r###"
        {
          "uid": "movies",
          "primaryKey": "id",
          "createdAt": "2022-10-04T15:51:35.291992167Z",
          "updatedAt": "2022-10-04T15:55:10.33561842Z"
        }
        "###);

        insta::assert_json_snapshot!(movies.settings().unwrap());
        let documents = movies.documents().unwrap().collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(documents.len(), 200);
        meili_snap::snapshot_hash!(format!("{:#?}", documents), @"e962baafd2fbae4cdd14e876053b0c5a");

        // spells
        insta::assert_json_snapshot!(spells.metadata(), @r###"
        {
          "uid": "dnd_spells",
          "primaryKey": "index",
          "createdAt": "2022-10-04T15:51:37.381094632Z",
          "updatedAt": "2022-10-04T15:55:02.394503431Z"
        }
        "###);

        insta::assert_json_snapshot!(spells.settings().unwrap());
        let documents = spells.documents().unwrap().collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(documents.len(), 10);
        meili_snap::snapshot_hash!(format!("{:#?}", documents), @"235016433dd04262c7f2da01d1e808ce");

        assert_eq!(dump.features().unwrap(), None);
    }

    #[test]
    fn import_dump_v4() {
        let dump = File::open("tests/assets/v4.dump").unwrap();
        let mut dump = DumpReader::open(dump).unwrap();

        // top level infos
        insta::assert_snapshot!(dump.date().unwrap(), @"2022-10-06 12:53:49.131989609 +00:00:00");
        insta::assert_snapshot!(dump.instance_uid().unwrap().unwrap(), @"9e15e977-f2ae-4761-943f-1eaf75fd736d");

        // tasks
        let tasks = dump.tasks().unwrap().collect::<Result<Vec<_>>>().unwrap();
        let (tasks, update_files): (Vec<_>, Vec<_>) = tasks.into_iter().unzip();
        meili_snap::snapshot_hash!(meili_snap::json_string!(tasks), @"c1b06a5ca60d5805483c16c5b3ff61ef");
        assert_eq!(update_files.len(), 10);
        assert!(update_files[0].is_some()); // the enqueued document addition
        assert!(update_files[1..].iter().all(|u| u.is_none())); // everything already processed

        // keys
        let keys = dump.keys().unwrap().collect::<Result<Vec<_>>>().unwrap();
        meili_snap::snapshot_hash!(meili_snap::json_string!(keys, { "[].uid" => "[uuid]" }), @"d751713988987e9331980363e24189ce");

        // indexes
        let mut indexes = dump.indexes().unwrap().collect::<Result<Vec<_>>>().unwrap();
        // the index are not ordered in any way by default
        indexes.sort_by_key(|index| index.metadata().uid.to_string());

        let mut products = indexes.pop().unwrap();
        let mut movies = indexes.pop().unwrap();
        let mut spells = indexes.pop().unwrap();
        assert!(indexes.is_empty());

        // products
        insta::assert_json_snapshot!(products.metadata(), @r###"
        {
          "uid": "products",
          "primaryKey": "sku",
          "createdAt": "2022-10-06T12:53:39.360187055Z",
          "updatedAt": "2022-10-06T12:53:40.603035979Z"
        }
        "###);

        insta::assert_json_snapshot!(products.settings().unwrap());
        let documents = products.documents().unwrap().collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(documents.len(), 10);
        meili_snap::snapshot_hash!(format!("{:#?}", documents), @"b01c8371aea4c7171af0d4d846a2bdca");

        // movies
        insta::assert_json_snapshot!(movies.metadata(), @r###"
        {
          "uid": "movies",
          "primaryKey": "id",
          "createdAt": "2022-10-06T12:53:38.710611568Z",
          "updatedAt": "2022-10-06T12:53:49.785862546Z"
        }
        "###);

        insta::assert_json_snapshot!(movies.settings().unwrap());
        let documents = movies.documents().unwrap().collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(documents.len(), 110);
        meili_snap::snapshot_hash!(format!("{:#?}", documents), @"786022a66ecb992c8a2a60fee070a5ab");

        // spells
        insta::assert_json_snapshot!(spells.metadata(), @r###"
        {
          "uid": "dnd_spells",
          "primaryKey": "index",
          "createdAt": "2022-10-06T12:53:40.831649057Z",
          "updatedAt": "2022-10-06T12:53:41.116036186Z"
        }
        "###);

        insta::assert_json_snapshot!(spells.settings().unwrap());
        let documents = spells.documents().unwrap().collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(documents.len(), 10);
        meili_snap::snapshot_hash!(format!("{:#?}", documents), @"235016433dd04262c7f2da01d1e808ce");
    }

    #[test]
    fn import_dump_v3() {
        let dump = File::open("tests/assets/v3.dump").unwrap();
        let mut dump = DumpReader::open(dump).unwrap();

        // top level infos
        insta::assert_snapshot!(dump.date().unwrap(), @"2022-10-07 11:39:03.709153554 +00:00:00");
        assert_eq!(dump.instance_uid().unwrap(), None);

        // tasks
        let tasks = dump.tasks().unwrap().collect::<Result<Vec<_>>>().unwrap();
        let (tasks, update_files): (Vec<_>, Vec<_>) = tasks.into_iter().unzip();
        meili_snap::snapshot_hash!(meili_snap::json_string!(tasks), @"0e203b6095f7c68dbdf788321dcc8215");
        assert_eq!(update_files.len(), 10);
        assert!(update_files[0].is_some()); // the enqueued document addition
        assert!(update_files[1..].iter().all(|u| u.is_none())); // everything already processed

        // keys
        let keys = dump.keys().unwrap().collect::<Result<Vec<_>>>().unwrap();
        meili_snap::snapshot_hash!(meili_snap::json_string!(keys), @"d751713988987e9331980363e24189ce");

        // indexes
        let mut indexes = dump.indexes().unwrap().collect::<Result<Vec<_>>>().unwrap();
        // the index are not ordered in any way by default
        indexes.sort_by_key(|index| index.metadata().uid.to_string());

        let mut products = indexes.pop().unwrap();
        let mut movies2 = indexes.pop().unwrap();
        let mut movies = indexes.pop().unwrap();
        let mut spells = indexes.pop().unwrap();
        assert!(indexes.is_empty());

        // products
        insta::assert_json_snapshot!(products.metadata(), { ".createdAt" => "[now]", ".updatedAt" => "[now]" }, @r###"
        {
          "uid": "products",
          "primaryKey": "sku",
          "createdAt": "[now]",
          "updatedAt": "[now]"
        }
        "###);

        insta::assert_json_snapshot!(products.settings().unwrap());
        let documents = products.documents().unwrap().collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(documents.len(), 10);
        meili_snap::snapshot_hash!(format!("{:#?}", documents), @"548284a84de510f71e88e6cdea495cf5");

        // movies
        insta::assert_json_snapshot!(movies.metadata(), { ".createdAt" => "[now]", ".updatedAt" => "[now]" }, @r###"
        {
          "uid": "movies",
          "primaryKey": "id",
          "createdAt": "[now]",
          "updatedAt": "[now]"
        }
        "###);

        insta::assert_json_snapshot!(movies.settings().unwrap());
        let documents = movies.documents().unwrap().collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(documents.len(), 110);
        meili_snap::snapshot_hash!(format!("{:#?}", documents), @"d153b5a81d8b3cdcbe1dec270b574022");

        // movies2
        insta::assert_json_snapshot!(movies2.metadata(), { ".createdAt" => "[now]", ".updatedAt" => "[now]" }, @r###"
        {
          "uid": "movies_2",
          "primaryKey": null,
          "createdAt": "[now]",
          "updatedAt": "[now]"
        }
        "###);

        insta::assert_json_snapshot!(movies2.settings().unwrap());
        let documents = movies2.documents().unwrap().collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(documents.len(), 0);
        meili_snap::snapshot_hash!(format!("{:#?}", documents), @"d751713988987e9331980363e24189ce");

        // spells
        insta::assert_json_snapshot!(spells.metadata(), { ".createdAt" => "[now]", ".updatedAt" => "[now]" }, @r###"
        {
          "uid": "dnd_spells",
          "primaryKey": "index",
          "createdAt": "[now]",
          "updatedAt": "[now]"
        }
        "###);

        insta::assert_json_snapshot!(spells.settings().unwrap());
        let documents = spells.documents().unwrap().collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(documents.len(), 10);
        meili_snap::snapshot_hash!(format!("{:#?}", documents), @"235016433dd04262c7f2da01d1e808ce");
    }

    #[test]
    fn import_dump_v2() {
        let dump = File::open("tests/assets/v2.dump").unwrap();
        let mut dump = DumpReader::open(dump).unwrap();

        // top level infos
        insta::assert_snapshot!(dump.date().unwrap(), @"2022-10-09 20:27:59.904096267 +00:00:00");
        assert_eq!(dump.instance_uid().unwrap(), None);

        // tasks
        let tasks = dump.tasks().unwrap().collect::<Result<Vec<_>>>().unwrap();
        let (tasks, update_files): (Vec<_>, Vec<_>) = tasks.into_iter().unzip();
        meili_snap::snapshot_hash!(meili_snap::json_string!(tasks), @"d216c7f90f538ffbb2a059531d7ac89a");
        assert_eq!(update_files.len(), 9);
        assert!(update_files[0].is_some()); // the enqueued document addition
        assert!(update_files[1..].iter().all(|u| u.is_none())); // everything already processed

        // keys
        let keys = dump.keys().unwrap().collect::<Result<Vec<_>>>().unwrap();
        meili_snap::snapshot_hash!(meili_snap::json_string!(keys), @"d751713988987e9331980363e24189ce");

        // indexes
        let mut indexes = dump.indexes().unwrap().collect::<Result<Vec<_>>>().unwrap();
        // the index are not ordered in any way by default
        indexes.sort_by_key(|index| index.metadata().uid.to_string());

        let mut products = indexes.pop().unwrap();
        let mut movies2 = indexes.pop().unwrap();
        let mut movies = indexes.pop().unwrap();
        let mut spells = indexes.pop().unwrap();
        assert!(indexes.is_empty());

        // products
        insta::assert_json_snapshot!(products.metadata(), @r###"
        {
          "uid": "products",
          "primaryKey": "sku",
          "createdAt": "2022-10-09T20:27:22.688964637Z",
          "updatedAt": "2022-10-09T20:27:23.951017769Z"
        }
        "###);

        insta::assert_json_snapshot!(products.settings().unwrap());
        let documents = products.documents().unwrap().collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(documents.len(), 10);
        meili_snap::snapshot_hash!(format!("{:#?}", documents), @"548284a84de510f71e88e6cdea495cf5");

        // movies
        insta::assert_json_snapshot!(movies.metadata(), @r###"
        {
          "uid": "movies",
          "primaryKey": "id",
          "createdAt": "2022-10-09T20:27:22.197788495Z",
          "updatedAt": "2022-10-09T20:28:01.93111053Z"
        }
        "###);

        insta::assert_json_snapshot!(movies.settings().unwrap());
        let documents = movies.documents().unwrap().collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(documents.len(), 110);
        meili_snap::snapshot_hash!(format!("{:#?}", documents), @"d153b5a81d8b3cdcbe1dec270b574022");

        // movies2
        insta::assert_json_snapshot!(movies2.metadata(), { ".createdAt" => "[now]", ".updatedAt" => "[now]" }, @r###"
        {
          "uid": "movies_2",
          "primaryKey": null,
          "createdAt": "[now]",
          "updatedAt": "[now]"
        }
        "###);

        insta::assert_json_snapshot!(movies2.settings().unwrap());
        let documents = movies2.documents().unwrap().collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(documents.len(), 0);
        meili_snap::snapshot_hash!(format!("{:#?}", documents), @"d751713988987e9331980363e24189ce");

        // spells
        insta::assert_json_snapshot!(spells.metadata(), @r###"
        {
          "uid": "dnd_spells",
          "primaryKey": "index",
          "createdAt": "2022-10-09T20:27:24.242683494Z",
          "updatedAt": "2022-10-09T20:27:24.312809641Z"
        }
        "###);

        insta::assert_json_snapshot!(spells.settings().unwrap());
        let documents = spells.documents().unwrap().collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(documents.len(), 10);
        meili_snap::snapshot_hash!(format!("{:#?}", documents), @"235016433dd04262c7f2da01d1e808ce");
    }

    #[test]
    fn import_dump_v2_from_meilisearch_v0_22_0_issue_3435() {
        let dump = File::open("tests/assets/v2-v0.22.0.dump").unwrap();
        let mut dump = DumpReader::open(dump).unwrap();

        // top level infos
        insta::assert_snapshot!(dump.date().unwrap(), @"2023-01-30 16:26:09.247261 +00:00:00");
        assert_eq!(dump.instance_uid().unwrap(), None);

        // tasks
        let tasks = dump.tasks().unwrap().collect::<Result<Vec<_>>>().unwrap();
        let (tasks, update_files): (Vec<_>, Vec<_>) = tasks.into_iter().unzip();
        meili_snap::snapshot_hash!(meili_snap::json_string!(tasks), @"e27999f1112632222cb84f6cffff7c5f");
        assert_eq!(update_files.len(), 8);
        assert!(update_files[0..].iter().all(|u| u.is_none())); // everything already processed

        // keys
        let keys = dump.keys().unwrap().collect::<Result<Vec<_>>>().unwrap();
        meili_snap::snapshot_hash!(meili_snap::json_string!(keys), @"d751713988987e9331980363e24189ce");

        // indexes
        let mut indexes = dump.indexes().unwrap().collect::<Result<Vec<_>>>().unwrap();
        // the index are not ordered in any way by default
        indexes.sort_by_key(|index| index.metadata().uid.to_string());

        let mut products = indexes.pop().unwrap();
        let mut movies = indexes.pop().unwrap();
        let mut spells = indexes.pop().unwrap();
        assert!(indexes.is_empty());

        // products
        insta::assert_json_snapshot!(products.metadata(), @r###"
        {
          "uid": "products",
          "primaryKey": "sku",
          "createdAt": "2023-01-30T16:25:56.595257Z",
          "updatedAt": "2023-01-30T16:25:58.70348Z"
        }
        "###);

        insta::assert_json_snapshot!(products.settings().unwrap());
        let documents = products.documents().unwrap().collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(documents.len(), 10);
        meili_snap::snapshot_hash!(format!("{:#?}", documents), @"548284a84de510f71e88e6cdea495cf5");

        // movies
        insta::assert_json_snapshot!(movies.metadata(), @r###"
        {
          "uid": "movies",
          "primaryKey": "id",
          "createdAt": "2023-01-30T16:25:56.192178Z",
          "updatedAt": "2023-01-30T16:25:56.455714Z"
        }
        "###);

        insta::assert_json_snapshot!(movies.settings().unwrap());
        let documents = movies.documents().unwrap().collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(documents.len(), 10);
        meili_snap::snapshot_hash!(format!("{:#?}", documents), @"0227598af846e574139ee0b80e03a720");

        // spells
        insta::assert_json_snapshot!(spells.metadata(), @r###"
        {
          "uid": "dnd_spells",
          "primaryKey": "index",
          "createdAt": "2023-01-30T16:25:58.876405Z",
          "updatedAt": "2023-01-30T16:25:59.079906Z"
        }
        "###);

        insta::assert_json_snapshot!(spells.settings().unwrap());
        let documents = spells.documents().unwrap().collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(documents.len(), 10);
        meili_snap::snapshot_hash!(format!("{:#?}", documents), @"235016433dd04262c7f2da01d1e808ce");
    }

    #[test]
    fn import_dump_v1() {
        let dump = File::open("tests/assets/v1.dump").unwrap();
        let mut dump = DumpReader::open(dump).unwrap();

        // top level infos
        assert_eq!(dump.date(), None);
        assert_eq!(dump.instance_uid().unwrap(), None);

        // tasks
        let tasks = dump.tasks().unwrap().collect::<Result<Vec<_>>>().unwrap();
        let (tasks, update_files): (Vec<_>, Vec<_>) = tasks.into_iter().unzip();
        meili_snap::snapshot_hash!(meili_snap::json_string!(tasks), @"0155a664b0cf62aae23db5138b6b03d7");
        assert_eq!(update_files.len(), 9);
        assert!(update_files[..].iter().all(|u| u.is_none())); // no update file in dump v1

        // keys
        let keys = dump.keys().unwrap().collect::<Result<Vec<_>>>().unwrap();
        meili_snap::snapshot!(meili_snap::json_string!(keys), @"[]");
        meili_snap::snapshot_hash!(meili_snap::json_string!(keys), @"d751713988987e9331980363e24189ce");

        // indexes
        let mut indexes = dump.indexes().unwrap().collect::<Result<Vec<_>>>().unwrap();
        // the index are not ordered in any way by default
        indexes.sort_by_key(|index| index.metadata().uid.to_string());

        let mut products = indexes.pop().unwrap();
        let mut movies = indexes.pop().unwrap();
        let mut spells = indexes.pop().unwrap();
        assert!(indexes.is_empty());

        // products
        insta::assert_json_snapshot!(products.metadata(), @r###"
        {
          "uid": "products",
          "primaryKey": "sku",
          "createdAt": "2022-10-02T13:23:39.976870431Z",
          "updatedAt": "2022-10-02T13:27:54.353262482Z"
        }
        "###);

        insta::assert_json_snapshot!(products.settings().unwrap());
        let documents = products.documents().unwrap().collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(documents.len(), 10);
        meili_snap::snapshot_hash!(format!("{:#?}", documents), @"b01c8371aea4c7171af0d4d846a2bdca");

        // movies
        insta::assert_json_snapshot!(movies.metadata(), @r###"
        {
          "uid": "movies",
          "primaryKey": "id",
          "createdAt": "2022-10-02T13:15:29.477512777Z",
          "updatedAt": "2022-10-02T13:21:12.671204856Z"
        }
        "###);

        insta::assert_json_snapshot!(movies.settings().unwrap());
        let documents = movies.documents().unwrap().collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(documents.len(), 10);
        meili_snap::snapshot_hash!(format!("{:#?}", documents), @"b63dbed5bbc059f3e32bc471ae699bf5");

        // spells
        insta::assert_json_snapshot!(spells.metadata(), @r###"
        {
          "uid": "dnd_spells",
          "primaryKey": "index",
          "createdAt": "2022-10-02T13:38:26.358882984Z",
          "updatedAt": "2022-10-02T13:38:26.385609433Z"
        }
        "###);

        insta::assert_json_snapshot!(spells.settings().unwrap());
        let documents = spells.documents().unwrap().collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(documents.len(), 10);
        meili_snap::snapshot_hash!(format!("{:#?}", documents), @"aa24c0cfc733d66c396237ad44263bed");
    }
}
