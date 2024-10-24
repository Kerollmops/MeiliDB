use std::fs::{read_dir, read_to_string, remove_file, File};
use std::io::{BufWriter, Write};
use std::path::PathBuf;

use anyhow::{bail, Context};
use clap::{Parser, Subcommand};
use dump::{DumpWriter, IndexMetadata};
use file_store::FileStore;
use meilisearch_auth::AuthController;
use meilisearch_types::heed::types::{SerdeJson, Str};
use meilisearch_types::heed::{Database, Env, EnvOpenOptions, RoTxn, RwTxn, Unspecified};
use meilisearch_types::milli::vector::parsed_vectors::RESERVED_VECTORS_FIELD_NAME;
use meilisearch_types::tasks::{Status, Task};
use meilisearch_types::versioning::{create_version_file, get_version, parse_version};
use meilisearch_types::{milli, Index};
use milli::documents::{obkv_to_object, DocumentsBatchReader};
use milli::index::{db_name, main_key};
use milli::vector::parsed_vectors::{ExplicitVectors, VectorOrArrayOfVectors};
use milli::{obkv_to_json, BEU32};
use serde_json::Value::Object;
use time::macros::format_description;
use time::OffsetDateTime;
use uuid_codec::UuidCodec;

mod uuid_codec;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    /// The database path where the Meilisearch is running.
    #[arg(long, default_value = "data.ms/")]
    db_path: PathBuf,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Clears the task queue and make it empty.
    ///
    /// This command can be safely executed even if Meilisearch is running and processing tasks.
    /// Once the task queue is empty you can restart Meilisearch and no more tasks must be visible,
    /// even the ones that were processing. However, it's highly possible that you see the processing
    /// tasks in the queue again with an associated internal error message.
    ClearTaskQueue,

    /// Exports a dump from the Meilisearch database.
    ///
    /// Make sure to run this command when Meilisearch is not running or running but not processing tasks.
    /// If tasks are being processed while a dump is being exported there are chances for the dump to be
    /// malformed with missing tasks.
    ///
    /// TODO Verify this claim or make sure it cannot happen and we can export dumps
    ///      without caring about killing Meilisearch first!
    ExportADump {
        /// The directory in which the dump will be created.
        #[arg(long, default_value = "dumps/")]
        dump_dir: PathBuf,

        /// Skip dumping the enqueued or processing tasks.
        ///
        /// Can be useful when there are a lot of them and it is not particularly useful
        /// to keep them. Note that only the enqueued tasks takes up space so skipping
        /// the processed ones is not particularly interesting.
        #[arg(long)]
        skip_enqueued_tasks: bool,
    },

    /// Exports the documents of an index in NDJSON format from a Meilisearch index to stdout.
    ///
    /// This command can be executed on a running Meilisearch database. However, please note that
    /// it will maintain a read-only transaction for the duration of the extraction process.
    ExportDocuments {
        /// The index name to export the documents from.
        #[arg(long)]
        index_name: String,

        /// Do not export vectors with the documents.
        #[arg(long)]
        ignore_vectors: bool,
    },

    /// Attempts to upgrade from one major version to the next without a dump.
    ///
    /// Make sure to run this commmand when Meilisearch is not running!
    /// If Meilisearch is running while executing this command, the database could be corrupted
    /// (contain data from both the old and the new versions)
    ///
    /// Supported upgrade paths:
    ///
    /// - v1.9.0 -> v1.10.0
    OfflineUpgrade {
        #[arg(long)]
        target_version: String,
    },
}

fn main() -> anyhow::Result<()> {
    let Cli { db_path, command } = Cli::parse();

    let detected_version = get_version(&db_path).context("While checking the version file")?;

    match command {
        Command::ClearTaskQueue => clear_task_queue(db_path),
        Command::ExportADump { dump_dir, skip_enqueued_tasks } => {
            export_a_dump(db_path, dump_dir, skip_enqueued_tasks)
        }
        Command::ExportDocuments { index_name, ignore_vectors } => {
            export_documents(db_path, index_name, ignore_vectors)
        }
        Command::OfflineUpgrade { target_version } => {
            let target_version = parse_version(&target_version).context("While parsing `--target-version`. Make sure `--target-version` is in the format MAJOR.MINOR.PATCH")?;
            OfflineUpgrade { db_path, current_version: detected_version, target_version }.upgrade()
        }
    }
}

struct OfflineUpgrade {
    db_path: PathBuf,
    current_version: (String, String, String),
    target_version: (String, String, String),
}

impl OfflineUpgrade {
    fn upgrade(self) -> anyhow::Result<()> {
        // TODO: if we make this process support more versions, introduce a more flexible way of checking for the version
        // currently only supports v1.9 to v1.10
        let (current_major, current_minor, current_patch) = &self.current_version;

        match (current_major.as_str(), current_minor.as_str(), current_patch.as_str()) {
            ("1", "9", _) => {}
            _ => {
                bail!("Unsupported current version {current_major}.{current_minor}.{current_patch}. Can only upgrade from v1.9")
            }
        }

        let (target_major, target_minor, target_patch) = &self.target_version;

        match (target_major.as_str(), target_minor.as_str(), target_patch.as_str()) {
            ("1", "10", _) => {}
            _ => {
                bail!("Unsupported target version {target_major}.{target_minor}.{target_patch}. Can only upgrade to v1.10")
            }
        }

        println!("Upgrading from {current_major}.{current_minor}.{current_patch} to {target_major}.{target_minor}.{target_patch}");

        self.v1_9_to_v1_10()?;

        println!("Writing VERSION file");

        create_version_file(&self.db_path, target_major, target_minor, target_patch)
            .context("while writing VERSION file after the upgrade")?;

        println!("Success");

        Ok(())
    }

    fn v1_9_to_v1_10(&self) -> anyhow::Result<()> {
        // 2 changes here

        // 1. date format. needs to be done before opening the Index
        // 2. REST embedders. We don't support this case right now, so bail

        let index_scheduler_path = self.db_path.join("tasks");
        let env = unsafe { EnvOpenOptions::new().max_dbs(100).open(&index_scheduler_path) }
            .with_context(|| {
                format!("While trying to open {:?}", index_scheduler_path.display())
            })?;

        let mut sched_wtxn = env.write_txn()?;

        let index_mapping: Database<Str, UuidCodec> =
            try_opening_database(&env, &sched_wtxn, "index-mapping")?;

        let index_stats: Database<UuidCodec, Unspecified> =
            try_opening_database(&env, &sched_wtxn, "index-stats").with_context(|| {
                format!("While trying to open {:?}", index_scheduler_path.display())
            })?;

        let index_count =
            index_mapping.len(&sched_wtxn).context("while reading the number of indexes")?;

        // FIXME: not ideal, we have to pre-populate all indexes to prevent double borrow of sched_wtxn
        // 1. immutably for the iteration
        // 2. mutably for updating index stats
        let indexes: Vec<_> = index_mapping
            .iter(&sched_wtxn)?
            .map(|res| res.map(|(uid, uuid)| (uid.to_owned(), uuid)))
            .collect();

        let mut rest_embedders = Vec::new();

        let mut unwrapped_indexes = Vec::new();

        // check that update can take place
        for (index_index, result) in indexes.into_iter().enumerate() {
            let (uid, uuid) = result?;
            let index_path = self.db_path.join("indexes").join(uuid.to_string());

            println!(
                "[{}/{index_count}]Checking that update can take place for  `{uid}` at `{}`",
                index_index + 1,
                index_path.display()
            );

            let index_env = unsafe {
                // FIXME: fetch the 25 magic number from the index file
                EnvOpenOptions::new().max_dbs(25).open(&index_path).with_context(|| {
                    format!("while opening index {uid} at '{}'", index_path.display())
                })?
            };

            let index_txn = index_env.read_txn().with_context(|| {
                format!(
                    "while obtaining a write transaction for index {uid} at {}",
                    index_path.display()
                )
            })?;

            println!("\t- Checking for incompatible embedders (REST embedders)");
            let rest_embedders_for_index = find_rest_embedders(&uid, &index_env, &index_txn)?;

            if rest_embedders_for_index.is_empty() {
                unwrapped_indexes.push((uid, uuid));
            } else {
                // no need to add to unwrapped indexes because we'll exit early
                rest_embedders.push((uid, rest_embedders_for_index));
            }
        }

        if !rest_embedders.is_empty() {
            let rest_embedders = rest_embedders
                .into_iter()
                .flat_map(|(index, embedders)| std::iter::repeat(index.clone()).zip(embedders))
                .map(|(index, embedder)| format!("\t- embedder `{embedder}` in index `{index}`"))
                .collect::<Vec<_>>()
                .join("\n");
            bail!("The update cannot take place because there are REST embedder(s). Remove them before proceeding with the update:\n{rest_embedders}\n\n\
            The database has not been modified and is still a valid v1.9 database.");
        }

        println!("Update can take place, updating");

        for (index_index, (uid, uuid)) in unwrapped_indexes.into_iter().enumerate() {
            let index_path = self.db_path.join("indexes").join(uuid.to_string());

            println!(
                "[{}/{index_count}]Updating index `{uid}` at `{}`",
                index_index + 1,
                index_path.display()
            );

            let index_env = unsafe {
                // FIXME: fetch the 25 magic number from the index file
                EnvOpenOptions::new().max_dbs(25).open(&index_path).with_context(|| {
                    format!("while opening index {uid} at '{}'", index_path.display())
                })?
            };

            let mut index_wtxn = index_env.write_txn().with_context(|| {
                format!(
                    "while obtaining a write transaction for index `{uid}` at `{}`",
                    index_path.display()
                )
            })?;

            println!("\t- Updating index stats");
            update_index_stats(index_stats, &uid, uuid, &mut sched_wtxn)?;
            println!("\t- Updating date format");
            update_date_format(&uid, &index_env, &mut index_wtxn)?;

            index_wtxn.commit().with_context(|| {
                format!(
                    "while committing the write txn for index `{uid}` at {}",
                    index_path.display()
                )
            })?;
        }

        sched_wtxn.commit().context("while committing the write txn for the index-scheduler")?;

        println!("Upgrading database succeeded");

        Ok(())
    }
}

pub mod v1_9 {
    pub type FieldDistribution = std::collections::BTreeMap<String, u64>;

    /// The statistics that can be computed from an `Index` object.
    #[derive(serde::Serialize, serde::Deserialize, Debug)]
    pub struct IndexStats {
        /// Number of documents in the index.
        pub number_of_documents: u64,
        /// Size taken up by the index' DB, in bytes.
        ///
        /// This includes the size taken by both the used and free pages of the DB, and as the free pages
        /// are not returned to the disk after a deletion, this number is typically larger than
        /// `used_database_size` that only includes the size of the used pages.
        pub database_size: u64,
        /// Size taken by the used pages of the index' DB, in bytes.
        ///
        /// As the DB backend does not return to the disk the pages that are not currently used by the DB,
        /// this value is typically smaller than `database_size`.
        pub used_database_size: u64,
        /// Association of every field name with the number of times it occurs in the documents.
        pub field_distribution: FieldDistribution,
        /// Creation date of the index.
        pub created_at: time::OffsetDateTime,
        /// Date of the last update of the index.
        pub updated_at: time::OffsetDateTime,
    }

    use serde::{Deserialize, Serialize};

    #[derive(Debug, Deserialize, Serialize)]
    pub struct IndexEmbeddingConfig {
        pub name: String,
        pub config: EmbeddingConfig,
    }

    #[derive(Debug, Clone, Default, serde::Deserialize, serde::Serialize)]
    pub struct EmbeddingConfig {
        /// Options of the embedder, specific to each kind of embedder
        pub embedder_options: EmbedderOptions,
    }

    /// Options of an embedder, specific to each kind of embedder.
    #[derive(Debug, Clone, Hash, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
    pub enum EmbedderOptions {
        HuggingFace(hf::EmbedderOptions),
        OpenAi(openai::EmbedderOptions),
        Ollama(ollama::EmbedderOptions),
        UserProvided(manual::EmbedderOptions),
        Rest(rest::EmbedderOptions),
    }

    impl Default for EmbedderOptions {
        fn default() -> Self {
            Self::OpenAi(openai::EmbedderOptions { api_key: None, dimensions: None })
        }
    }

    mod hf {
        #[derive(Debug, Clone, Hash, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
        pub struct EmbedderOptions {
            pub model: String,
            pub revision: Option<String>,
        }
    }
    mod openai {

        #[derive(Debug, Clone, Hash, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
        pub struct EmbedderOptions {
            pub api_key: Option<String>,
            pub dimensions: Option<usize>,
        }
    }
    mod ollama {
        #[derive(Debug, Clone, Hash, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
        pub struct EmbedderOptions {
            pub embedding_model: String,
            pub url: Option<String>,
            pub api_key: Option<String>,
        }
    }
    mod manual {
        #[derive(Debug, Clone, Hash, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
        pub struct EmbedderOptions {
            pub dimensions: usize,
        }
    }
    mod rest {
        #[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize, serde::Serialize, Hash)]
        pub struct EmbedderOptions {
            pub api_key: Option<String>,
            pub dimensions: Option<usize>,
            pub url: String,
            pub input_field: Vec<String>,
            // path to the array of embeddings
            pub path_to_embeddings: Vec<String>,
            // shape of a single embedding
            pub embedding_object: Vec<String>,
        }
    }

    pub type OffsetDateTime = time::OffsetDateTime;
}

pub mod v1_10 {
    use crate::v1_9;

    pub type FieldDistribution = std::collections::BTreeMap<String, u64>;

    /// The statistics that can be computed from an `Index` object.
    #[derive(serde::Serialize, serde::Deserialize, Debug)]
    pub struct IndexStats {
        /// Number of documents in the index.
        pub number_of_documents: u64,
        /// Size taken up by the index' DB, in bytes.
        ///
        /// This includes the size taken by both the used and free pages of the DB, and as the free pages
        /// are not returned to the disk after a deletion, this number is typically larger than
        /// `used_database_size` that only includes the size of the used pages.
        pub database_size: u64,
        /// Size taken by the used pages of the index' DB, in bytes.
        ///
        /// As the DB backend does not return to the disk the pages that are not currently used by the DB,
        /// this value is typically smaller than `database_size`.
        pub used_database_size: u64,
        /// Association of every field name with the number of times it occurs in the documents.
        pub field_distribution: FieldDistribution,
        /// Creation date of the index.
        #[serde(with = "time::serde::rfc3339")]
        pub created_at: time::OffsetDateTime,
        /// Date of the last update of the index.
        #[serde(with = "time::serde::rfc3339")]
        pub updated_at: time::OffsetDateTime,
    }

    impl From<v1_9::IndexStats> for IndexStats {
        fn from(
            v1_9::IndexStats {
                number_of_documents,
                database_size,
                used_database_size,
                field_distribution,
                created_at,
                updated_at,
            }: v1_9::IndexStats,
        ) -> Self {
            IndexStats {
                number_of_documents,
                database_size,
                used_database_size,
                field_distribution,
                created_at,
                updated_at,
            }
        }
    }

    #[derive(serde::Serialize, serde::Deserialize)]
    #[serde(transparent)]
    pub struct OffsetDateTime(#[serde(with = "time::serde::rfc3339")] pub time::OffsetDateTime);
}

fn update_index_stats(
    index_stats: Database<UuidCodec, Unspecified>,
    index_uid: &str,
    index_uuid: uuid::Uuid,
    sched_wtxn: &mut RwTxn,
) -> anyhow::Result<()> {
    let ctx = || format!("while updating index stats for index `{index_uid}`");

    let stats: Option<v1_9::IndexStats> = index_stats
        .remap_data_type::<SerdeJson<v1_9::IndexStats>>()
        .get(sched_wtxn, &index_uuid)
        .with_context(ctx)?;

    if let Some(stats) = stats {
        let stats: v1_10::IndexStats = stats.into();

        index_stats
            .remap_data_type::<SerdeJson<v1_10::IndexStats>>()
            .put(sched_wtxn, &index_uuid, &stats)
            .with_context(ctx)?;
    }

    Ok(())
}

fn update_date_format(
    index_uid: &str,
    index_env: &Env,
    index_wtxn: &mut RwTxn,
) -> anyhow::Result<()> {
    let main = try_opening_poly_database(index_env, index_wtxn, db_name::MAIN)
        .with_context(|| format!("while updating date format for index `{index_uid}`"))?;

    date_round_trip(index_wtxn, index_uid, main, main_key::CREATED_AT_KEY)?;
    date_round_trip(index_wtxn, index_uid, main, main_key::UPDATED_AT_KEY)?;

    Ok(())
}

fn find_rest_embedders(
    index_uid: &str,
    index_env: &Env,
    index_txn: &RoTxn,
) -> anyhow::Result<Vec<String>> {
    let main = try_opening_poly_database(index_env, index_txn, db_name::MAIN)
        .with_context(|| format!("while checking REST embedders for index `{index_uid}`"))?;

    let mut rest_embedders = vec![];

    for config in main
        .remap_types::<Str, SerdeJson<Vec<v1_9::IndexEmbeddingConfig>>>()
        .get(index_txn, main_key::EMBEDDING_CONFIGS)?
        .unwrap_or_default()
    {
        if let v1_9::EmbedderOptions::Rest(_) = config.config.embedder_options {
            rest_embedders.push(config.name);
        }
    }

    Ok(rest_embedders)
}

fn date_round_trip(
    wtxn: &mut RwTxn,
    index_uid: &str,
    db: Database<Unspecified, Unspecified>,
    key: &str,
) -> anyhow::Result<()> {
    let datetime =
        db.remap_types::<Str, SerdeJson<v1_9::OffsetDateTime>>().get(wtxn, key).with_context(
            || format!("could not read `{key}` while updating date format for index `{index_uid}`"),
        )?;

    if let Some(datetime) = datetime {
        db.remap_types::<Str, SerdeJson<v1_10::OffsetDateTime>>()
            .put(wtxn, key, &v1_10::OffsetDateTime(datetime))
            .with_context(|| {
                format!(
                    "could not write `{key}` while updating date format for index `{index_uid}`"
                )
            })?;
    }

    Ok(())
}

/// Clears the task queue located at `db_path`.
fn clear_task_queue(db_path: PathBuf) -> anyhow::Result<()> {
    let path = db_path.join("tasks");
    let env = unsafe { EnvOpenOptions::new().max_dbs(100).open(&path) }
        .with_context(|| format!("While trying to open {:?}", path.display()))?;

    eprintln!("Deleting tasks from the database...");

    let mut wtxn = env.write_txn()?;
    let all_tasks = try_opening_poly_database(&env, &wtxn, "all-tasks")?;
    let total = all_tasks.len(&wtxn)?;
    let status = try_opening_poly_database(&env, &wtxn, "status")?;
    let kind = try_opening_poly_database(&env, &wtxn, "kind")?;
    let index_tasks = try_opening_poly_database(&env, &wtxn, "index-tasks")?;
    let canceled_by = try_opening_poly_database(&env, &wtxn, "canceled_by")?;
    let enqueued_at = try_opening_poly_database(&env, &wtxn, "enqueued-at")?;
    let started_at = try_opening_poly_database(&env, &wtxn, "started-at")?;
    let finished_at = try_opening_poly_database(&env, &wtxn, "finished-at")?;

    try_clearing_poly_database(&mut wtxn, all_tasks, "all-tasks")?;
    try_clearing_poly_database(&mut wtxn, status, "status")?;
    try_clearing_poly_database(&mut wtxn, kind, "kind")?;
    try_clearing_poly_database(&mut wtxn, index_tasks, "index-tasks")?;
    try_clearing_poly_database(&mut wtxn, canceled_by, "canceled_by")?;
    try_clearing_poly_database(&mut wtxn, enqueued_at, "enqueued-at")?;
    try_clearing_poly_database(&mut wtxn, started_at, "started-at")?;
    try_clearing_poly_database(&mut wtxn, finished_at, "finished-at")?;

    wtxn.commit().context("While committing the transaction")?;

    eprintln!("Successfully deleted {total} tasks from the tasks database!");
    eprintln!("Deleting the content files from disk...");

    let mut count = 0usize;
    let update_files = db_path.join("update_files");
    let entries = read_dir(&update_files).with_context(|| {
        format!("While trying to read the content of {:?}", update_files.display())
    })?;
    for result in entries {
        match result {
            Ok(ent) => match remove_file(ent.path()) {
                Ok(_) => count += 1,
                Err(e) => eprintln!("Error while deleting {:?}: {}", ent.path().display(), e),
            },
            Err(e) => {
                eprintln!("Error while reading a file in {:?}: {}", update_files.display(), e)
            }
        }
    }

    eprintln!("Successfully deleted {count} content files from disk!");

    Ok(())
}

fn try_opening_database<KC: 'static, DC: 'static>(
    env: &Env,
    rtxn: &RoTxn,
    db_name: &str,
) -> anyhow::Result<Database<KC, DC>> {
    env.open_database(rtxn, Some(db_name))
        .with_context(|| format!("While opening the {db_name:?} database"))?
        .with_context(|| format!("Missing the {db_name:?} database"))
}

fn try_opening_poly_database(
    env: &Env,
    rtxn: &RoTxn,
    db_name: &str,
) -> anyhow::Result<Database<Unspecified, Unspecified>> {
    env.database_options()
        .name(db_name)
        .open(rtxn)
        .with_context(|| format!("While opening the {db_name:?} poly database"))?
        .with_context(|| format!("Missing the {db_name:?} poly database"))
}

fn try_clearing_poly_database(
    wtxn: &mut RwTxn,
    database: Database<Unspecified, Unspecified>,
    db_name: &str,
) -> anyhow::Result<()> {
    database.clear(wtxn).with_context(|| format!("While clearing the {db_name:?} database"))
}

/// Exports a dump into the dump directory.
fn export_a_dump(
    db_path: PathBuf,
    dump_dir: PathBuf,
    skip_enqueued_tasks: bool,
) -> anyhow::Result<()> {
    let started_at = OffsetDateTime::now_utc();

    // 1. Extracts the instance UID from disk
    let instance_uid_path = db_path.join("instance-uid");
    let instance_uid = match read_to_string(&instance_uid_path) {
        Ok(content) => match content.trim().parse() {
            Ok(uuid) => Some(uuid),
            Err(e) => {
                eprintln!("Impossible to parse instance-uid: {e}");
                None
            }
        },
        Err(e) => {
            eprintln!("Impossible to read {}: {}", instance_uid_path.display(), e);
            None
        }
    };

    let dump = DumpWriter::new(instance_uid).context("While creating a new dump")?;
    let file_store =
        FileStore::new(db_path.join("update_files")).context("While opening the FileStore")?;

    let index_scheduler_path = db_path.join("tasks");
    let env = unsafe { EnvOpenOptions::new().max_dbs(100).open(&index_scheduler_path) }
        .with_context(|| format!("While trying to open {:?}", index_scheduler_path.display()))?;

    eprintln!("Dumping the keys...");

    // 2. dump the keys
    let auth_store = AuthController::new(&db_path, &None)
        .with_context(|| format!("While opening the auth store at {}", db_path.display()))?;
    let mut dump_keys = dump.create_keys()?;
    let mut count = 0;
    for key in auth_store.list_keys()? {
        dump_keys.push_key(&key)?;
        count += 1;
    }
    dump_keys.flush()?;

    eprintln!("Successfully dumped {count} keys!");

    let rtxn = env.read_txn()?;
    let all_tasks: Database<BEU32, SerdeJson<Task>> =
        try_opening_database(&env, &rtxn, "all-tasks")?;
    let index_mapping: Database<Str, UuidCodec> =
        try_opening_database(&env, &rtxn, "index-mapping")?;

    if skip_enqueued_tasks {
        eprintln!("Skip dumping the enqueued tasks...");
    } else {
        eprintln!("Dumping the enqueued tasks...");

        // 3. dump the tasks
        let mut dump_tasks = dump.create_tasks_queue()?;
        let mut count = 0;
        for ret in all_tasks.iter(&rtxn)? {
            let (_, t) = ret?;
            let status = t.status;
            let content_file = t.content_uuid();
            let mut dump_content_file = dump_tasks.push_task(&t.into())?;

            // 3.1. Dump the `content_file` associated with the task if there is one and the task is not finished yet.
            if let Some(content_file_uuid) = content_file {
                if status == Status::Enqueued {
                    let content_file = file_store.get_update(content_file_uuid)?;

                    let reader =
                        DocumentsBatchReader::from_reader(content_file).with_context(|| {
                            format!("While reading content file {:?}", content_file_uuid)
                        })?;

                    let (mut cursor, documents_batch_index) = reader.into_cursor_and_fields_index();
                    while let Some(doc) = cursor.next_document().with_context(|| {
                        format!("While iterating on content file {:?}", content_file_uuid)
                    })? {
                        dump_content_file
                            .push_document(&obkv_to_object(&doc, &documents_batch_index)?)?;
                    }
                    dump_content_file.flush()?;
                    count += 1;
                }
            }
        }
        dump_tasks.flush()?;

        eprintln!("Successfully dumped {count} enqueued tasks!");
    }

    eprintln!("Dumping the indexes...");

    // 4. Dump the indexes
    let mut count = 0;
    for result in index_mapping.iter(&rtxn)? {
        let (uid, uuid) = result?;
        let index_path = db_path.join("indexes").join(uuid.to_string());
        let index = Index::new(EnvOpenOptions::new(), &index_path).with_context(|| {
            format!("While trying to open the index at path {:?}", index_path.display())
        })?;

        let rtxn = index.read_txn()?;
        let metadata = IndexMetadata {
            uid: uid.to_owned(),
            primary_key: index.primary_key(&rtxn)?.map(String::from),
            created_at: index.created_at(&rtxn)?,
            updated_at: index.updated_at(&rtxn)?,
        };
        let mut index_dumper = dump.create_index(uid, &metadata)?;

        let fields_ids_map = index.fields_ids_map(&rtxn)?;
        let all_fields: Vec<_> = fields_ids_map.iter().map(|(id, _)| id).collect();

        // 4.1. Dump the documents
        for ret in index.all_documents(&rtxn)? {
            let (_id, doc) = ret?;
            let document = obkv_to_json(&all_fields, &fields_ids_map, doc)?;
            index_dumper.push_document(&document)?;
        }

        // 4.2. Dump the settings
        let settings = meilisearch_types::settings::settings(
            &index,
            &rtxn,
            meilisearch_types::settings::SecretPolicy::RevealSecrets,
        )?;
        index_dumper.settings(&settings)?;
        count += 1;
    }

    eprintln!("Successfully dumped {count} indexes!");
    // We will not dump experimental feature settings
    eprintln!("The tool is not dumping experimental features, please set them by hand afterward");

    let dump_uid = started_at.format(format_description!(
        "[year repr:full][month repr:numerical][day padding:zero]-[hour padding:zero][minute padding:zero][second padding:zero][subsecond digits:3]"
    )).unwrap();

    let path = dump_dir.join(format!("{}.dump", dump_uid));
    let file = File::create(&path)?;
    dump.persist_to(BufWriter::new(file))?;

    eprintln!("Dump exported at path {:?}", path.display());

    Ok(())
}

fn export_documents(
    db_path: PathBuf,
    index_name: String,
    ignore_vectors: bool,
) -> anyhow::Result<()> {
    let index_scheduler_path = db_path.join("tasks");
    let env = unsafe { EnvOpenOptions::new().max_dbs(100).open(&index_scheduler_path) }
        .with_context(|| format!("While trying to open {:?}", index_scheduler_path.display()))?;

    let rtxn = env.read_txn()?;
    let index_mapping: Database<Str, UuidCodec> =
        try_opening_database(&env, &rtxn, "index-mapping")?;

    for result in index_mapping.iter(&rtxn)? {
        let (uid, uuid) = result?;
        if uid == index_name {
            let index_path = db_path.join("indexes").join(uuid.to_string());
            let index = Index::new(EnvOpenOptions::new(), &index_path).with_context(|| {
                format!("While trying to open the index at path {:?}", index_path.display())
            })?;

            let rtxn = index.read_txn()?;
            let fields_ids_map = index.fields_ids_map(&rtxn)?;
            let all_fields: Vec<_> = fields_ids_map.iter().map(|(id, _)| id).collect();
            let embedding_configs = index.embedding_configs(&rtxn)?;

            let mut stdout = BufWriter::new(std::io::stdout());
            for ret in index.all_documents(&rtxn)? {
                let (id, doc) = ret?;
                let mut document = obkv_to_json(&all_fields, &fields_ids_map, doc)?;

                if !ignore_vectors {
                    'inject_vectors: {
                        let embeddings = index.embeddings(&rtxn, id)?;

                        if embeddings.is_empty() {
                            break 'inject_vectors;
                        }

                        let vectors = document
                            .entry(RESERVED_VECTORS_FIELD_NAME)
                            .or_insert(Object(Default::default()));

                        let Object(vectors) = vectors else {
                            return Err(meilisearch_types::milli::Error::UserError(
                                meilisearch_types::milli::UserError::InvalidVectorsMapType {
                                    document_id: {
                                        if let Ok(Some(Ok(index))) = index
                                            .external_id_of(&rtxn, std::iter::once(id))
                                            .map(|it| it.into_iter().next())
                                        {
                                            index
                                        } else {
                                            format!("internal docid={id}")
                                        }
                                    },
                                    value: vectors.clone(),
                                },
                            )
                            .into());
                        };

                        for (embedder_name, embeddings) in embeddings {
                            let user_provided = embedding_configs
                                .iter()
                                .find(|conf| conf.name == embedder_name)
                                .is_some_and(|conf| conf.user_provided.contains(id));

                            let embeddings = ExplicitVectors {
                                embeddings: Some(VectorOrArrayOfVectors::from_array_of_vectors(
                                    embeddings,
                                )),
                                regenerate: !user_provided,
                            };
                            vectors
                                .insert(embedder_name, serde_json::to_value(embeddings).unwrap());
                        }
                    }
                }

                serde_json::to_writer(&mut stdout, &document)?;
            }

            stdout.flush()?;
        } else {
            eprintln!("Found index {uid} but it's not the right index...");
        }
    }

    Ok(())
}
