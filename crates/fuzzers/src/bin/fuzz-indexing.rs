use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use arbitrary::{Arbitrary, Unstructured};
use bumpalo::Bump;
use clap::Parser;
use either::Either;
use fuzzers::Operation;
use milli::documents::mmap_from_objects;
use milli::heed::EnvOpenOptions;
use milli::progress::Progress;
use milli::update::new::indexer;
use milli::update::{IndexDocumentsMethod, IndexerConfig};
use milli::vector::EmbeddingConfigs;
use milli::Index;
use serde_json::Value;
use tempfile::TempDir;

#[derive(Debug, Arbitrary)]
struct Batch([Operation; 5]);

#[derive(Debug, Clone, Parser)]
struct Opt {
    /// The number of fuzzer to run in parallel.
    #[clap(long)]
    par: Option<NonZeroUsize>,
    // We need to put a lot of newlines in the following documentation or else everything gets collapsed on one line
    /// The path in which the databases will be created.
    /// Using a ramdisk is recommended.
    ///
    /// Linux:
    ///
    /// sudo mount -t tmpfs -o size=2g tmpfs ramdisk # to create it
    ///
    /// sudo umount ramdisk # to remove it
    ///
    /// MacOS:
    ///
    /// diskutil erasevolume HFS+ 'RAM Disk' `hdiutil attach -nobrowse -nomount ram://4194304 # create it
    ///
    /// hdiutil detach /dev/:the_disk
    #[clap(long)]
    path: Option<PathBuf>,
}

fn main() {
    let opt = Opt::parse();
    let progression: &'static AtomicUsize = Box::leak(Box::new(AtomicUsize::new(0)));
    let stop: &'static AtomicBool = Box::leak(Box::new(AtomicBool::new(false)));

    let par = opt.par.unwrap_or_else(|| std::thread::available_parallelism().unwrap()).get();
    let mut handles = Vec::with_capacity(par);

    for _ in 0..par {
        let opt = opt.clone();

        let handle = std::thread::spawn(move || {
            let mut options = EnvOpenOptions::new();
            options.map_size(1024 * 1024 * 1024 * 1024);
            let tempdir = match opt.path {
                Some(path) => TempDir::new_in(path).unwrap(),
                None => TempDir::new().unwrap(),
            };
            let index = Index::new(options, tempdir.path()).unwrap();
            let indexer_config = IndexerConfig::default();

            std::thread::scope(|s| {
                loop {
                    if stop.load(Ordering::Relaxed) {
                        return;
                    }
                    let v: Vec<u8> =
                        std::iter::repeat_with(|| fastrand::u8(..)).take(1000).collect();

                    let mut data = Unstructured::new(&v);
                    let batches = <[Batch; 5]>::arbitrary(&mut data).unwrap();
                    // will be used to display the error once a thread crashes
                    let dbg_input = format!("{:#?}", batches);

                    let handle = s.spawn(|| {
                        let mut wtxn = index.write_txn().unwrap();
                        let rtxn = index.read_txn().unwrap();

                        for batch in batches {
                            let db_fields_ids_map = index.fields_ids_map(&rtxn).unwrap();
                            let mut new_fields_ids_map = db_fields_ids_map.clone();

                            let indexer_alloc = Bump::new();
                            let embedders = EmbeddingConfigs::default();
                            let mut indexer = indexer::DocumentOperation::new(
                                IndexDocumentsMethod::ReplaceDocuments,
                            );

                            let mut operations = Vec::new();
                            for op in batch.0 {
                                match op {
                                    Operation::AddDoc(doc) => {
                                        let object = match doc.to_d() {
                                            Value::Object(object) => object,
                                            _ => unreachable!(),
                                        };
                                        let documents = mmap_from_objects(vec![object]);
                                        operations.push(Either::Left(documents));
                                    }
                                    Operation::DeleteDoc(id) => {
                                        let id = indexer_alloc.alloc_str(&id.to_s());
                                        let ids = indexer_alloc.alloc_slice_copy(&[&*id]);
                                        operations.push(Either::Right(ids));
                                    }
                                }
                            }

                            for op in &operations {
                                match op {
                                    Either::Left(documents) => {
                                        indexer.add_documents(documents).unwrap()
                                    }
                                    Either::Right(ids) => indexer.delete_documents(ids),
                                }
                            }

                            let (document_changes, _operation_stats, primary_key) = indexer
                                .into_changes(
                                    &indexer_alloc,
                                    &index,
                                    &rtxn,
                                    None,
                                    &mut new_fields_ids_map,
                                    &|| false,
                                    Progress::default(),
                                )
                                .unwrap();

                            indexer::index(
                                &mut wtxn,
                                &index,
                                &milli::ThreadPoolNoAbortBuilder::new().build().unwrap(),
                                indexer_config.grenad_parameters(),
                                &db_fields_ids_map,
                                new_fields_ids_map,
                                primary_key,
                                &document_changes,
                                embedders,
                                &|| false,
                                &Progress::default(),
                            )
                            .unwrap();

                            // after executing a batch we check if the database is corrupted
                            let res = index.search(&wtxn).execute().unwrap();
                            index.compressed_documents(&wtxn, res.documents_ids).unwrap();
                            progression.fetch_add(1, Ordering::Relaxed);
                        }
                        wtxn.abort();
                    });
                    if let err @ Err(_) = handle.join() {
                        stop.store(true, Ordering::Relaxed);
                        err.expect(&dbg_input);
                    }
                }
            });
        });
        handles.push(handle);
    }

    std::thread::spawn(|| {
        let mut last_value = 0;
        let start = std::time::Instant::now();
        loop {
            let total = progression.load(Ordering::Relaxed);
            let elapsed = start.elapsed().as_secs();
            if elapsed > 3600 {
                // after 1 hour, stop the fuzzer, success
                std::process::exit(0);
            }
            println!(
                "Has been running for {:?} seconds. Tested {} new values for a total of {}.",
                elapsed,
                total - last_value,
                total
            );
            last_value = total;
            std::thread::sleep(Duration::from_secs(1));
        }
    });

    for handle in handles {
        handle.join().unwrap();
    }
}
