use std::fs::{read_dir, read_to_string, remove_file, File};
use std::io::BufWriter;
use std::path::PathBuf;
use std::time::Instant;

use anyhow::Context;
use clap::{Parser, Subcommand};
use dump::{DumpWriter, IndexMetadata};
use file_store::FileStore;
use meilisearch_auth::AuthController;
use meilisearch_types::heed::types::{SerdeJson, Str};
use meilisearch_types::heed::{
    CompactionOption, Database, Env, EnvOpenOptions, RoTxn, RwTxn, Unspecified,
};
use meilisearch_types::milli::documents::{obkv_to_object, DocumentsBatchReader};
use meilisearch_types::milli::{obkv_to_json, BEU32};
use meilisearch_types::tasks::{Status, Task};
use meilisearch_types::versioning::{get_version, parse_version};
use meilisearch_types::Index;
use time::macros::format_description;
use time::OffsetDateTime;
use upgrade::OfflineUpgrade;
use uuid_codec::UuidCodec;

mod upgrade;
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

    /// Attempts to upgrade from one major version to the next without a dump.
    ///
    /// Make sure to run this commmand when Meilisearch is not running!
    /// If Meilisearch is running while executing this command, the database could be corrupted
    /// (contain data from both the old and the new versions)
    ///
    /// Supported upgrade paths:
    ///
    /// - v1.9.x -> v1.10.x -> v1.11.x -> v1.12.x
    OfflineUpgrade {
        #[arg(long)]
        target_version: String,
    },

    /// Compact the index by using LMDB.
    ///
    /// You must run this command while Meilisearch is off. The reason is that Meilisearch keep the
    /// indexes opened and this compaction operation writes into another file. Meilisearch will not
    /// switch to the new file.
    ///
    /// **Another possibility** is to keep Meilisearch running to serve search requests, run the
    /// compaction and once done, close and immediately reopen Meilisearch. This way Meilisearch
    /// will reopened the data.mdb file when rebooting and see the newly compacted file, ignoring
    /// the previous non-compacted data.
    ///
    /// Note that the compaction will open the index, copy and compact the index into another file
    /// **on the same disk as the index** and replace the previous index with the newly compacted
    /// one. Which means that the disk must have enough room for at most two time the index size.
    ///
    /// To make sure not to loose any data, this tool takes a mutable transaction on the index
    /// before running the copy and compaction. This way the current indexation must finish before
    /// the compaction operation can start. Once the compaction is done, the big index is replaced
    /// by the compacted one and the mutable transaction is released.
    CompactIndex { index_name: String },
}

fn main() -> anyhow::Result<()> {
    let Cli { db_path, command } = Cli::parse();

    let detected_version = get_version(&db_path).context("While checking the version file")?;

    match command {
        Command::ClearTaskQueue => clear_task_queue(db_path),
        Command::ExportADump { dump_dir, skip_enqueued_tasks } => {
            export_a_dump(db_path, dump_dir, skip_enqueued_tasks)
        }
        Command::OfflineUpgrade { target_version } => {
            let target_version = parse_version(&target_version).context("While parsing `--target-version`. Make sure `--target-version` is in the format MAJOR.MINOR.PATCH")?;
            OfflineUpgrade { db_path, current_version: detected_version, target_version }.upgrade()
        }
        Command::CompactIndex { index_name } => compact_index(db_path, &index_name),
    }
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
) -> Result<(), anyhow::Error> {
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
                            .push_document(&obkv_to_object(doc, &documents_batch_index)?)?;
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

fn compact_index(db_path: PathBuf, index_name: &str) -> anyhow::Result<()> {
    let index_scheduler_path = db_path.join("tasks");
    let env = unsafe { EnvOpenOptions::new().max_dbs(100).open(&index_scheduler_path) }
        .with_context(|| format!("While trying to open {:?}", index_scheduler_path.display()))?;

    let rtxn = env.read_txn()?;
    let index_mapping: Database<Str, UuidCodec> =
        try_opening_database(&env, &rtxn, "index-mapping")?;

    for result in index_mapping.iter(&rtxn)? {
        let (uid, uuid) = result?;

        if uid != index_name {
            eprintln!("Found index {uid} and skipping it");
            continue;
        } else {
            eprintln!("Found index {uid} 🎉");
        }

        let index_path = db_path.join("indexes").join(uuid.to_string());
        let index = Index::new(EnvOpenOptions::new(), &index_path).with_context(|| {
            format!("While trying to open the index at path {:?}", index_path.display())
        })?;

        eprintln!("Awaiting for a mutable transaction...");
        let _wtxn = index.write_txn().context("While awaiting for a write transaction")?;

        // We create and immediately drop the file because the
        let non_compacted_index_file_path = index_path.join("data.mdb");
        let compacted_index_file_path = index_path.join("data.mdb.cpy");

        eprintln!("Compacting the index...");
        let before_compaction = Instant::now();
        let new_file = index
            .copy_to_file(&compacted_index_file_path, CompactionOption::Enabled)
            .with_context(|| format!("While compacting {}", compacted_index_file_path.display()))?;

        let after_size = new_file.metadata()?.len();
        let before_size = std::fs::metadata(&non_compacted_index_file_path)
            .with_context(|| {
                format!(
                    "While retrieving the metadata of {}",
                    non_compacted_index_file_path.display(),
                )
            })?
            .len();

        let reduction = before_size as f64 / after_size as f64;
        println!("Compaction successful. Took around {:.2?}", before_compaction.elapsed());
        eprintln!("The index went from {before_size} bytes to {after_size} bytes ({reduction:.2}x reduction)");

        eprintln!("Replacing the non-compacted index by the compacted one...");
        std::fs::rename(&compacted_index_file_path, &non_compacted_index_file_path).with_context(
            || {
                format!(
                    "While renaming {} into {}",
                    compacted_index_file_path.display(),
                    non_compacted_index_file_path.display(),
                )
            },
        )?;

        drop(new_file);

        println!("Everything's done 🎉");
    }

    Ok(())
}
