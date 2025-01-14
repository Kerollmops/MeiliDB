use std::any::TypeId;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use actix_web::http::header::USER_AGENT;
use actix_web::HttpRequest;
use byte_unit::Byte;
use index_scheduler::IndexScheduler;
use meilisearch_auth::{AuthController, AuthFilter};
use meilisearch_types::features::RuntimeTogglableFeatures;
use meilisearch_types::InstanceUid;
use once_cell::sync::Lazy;
use segment::message::{Identify, Track, User};
use segment::{AutoBatcher, Batcher, HttpClient};
use serde::Serialize;
use serde_json::{json, Value};
use sysinfo::{Disks, System};
use time::OffsetDateTime;
use tokio::select;
use tokio::sync::mpsc::{self, Receiver, Sender};
use uuid::Uuid;

use super::{config_user_id_path, Aggregate, MEILISEARCH_CONFIG_PATH};
use crate::option::{
    default_http_addr, IndexerOpts, LogMode, MaxMemory, MaxThreads, ScheduleSnapshot,
};
use crate::routes::{create_all_stats, Stats};
use crate::Opt;

const ANALYTICS_HEADER: &str = "X-Meilisearch-Client";

/// Write the instance-uid in the `data.ms` and in `~/.config/MeiliSearch/path-to-db-instance-uid`. Ignore the errors.
fn write_user_id(db_path: &Path, user_id: &InstanceUid) {
    let _ = fs::write(db_path.join("instance-uid"), user_id.to_string());
    if let Some((meilisearch_config_path, user_id_path)) =
        MEILISEARCH_CONFIG_PATH.as_ref().zip(config_user_id_path(db_path))
    {
        let _ = fs::create_dir_all(meilisearch_config_path);
        let _ = fs::write(user_id_path, user_id.to_string());
    }
}

const SEGMENT_API_KEY: &str = "P3FWhhEsJiEDCuEHpmcN9DHcK4hVfBvb";

pub fn extract_user_agents(request: &HttpRequest) -> HashSet<String> {
    request
        .headers()
        .get(ANALYTICS_HEADER)
        .or_else(|| request.headers().get(USER_AGENT))
        .and_then(|header| header.to_str().ok())
        .unwrap_or("unknown")
        .split(';')
        .map(str::trim)
        .map(ToString::to_string)
        .collect()
}

pub struct Message {
    // Since the type_id is solved statically we cannot retrieve it from the Box.
    // Thus we have to send it in the message directly.
    type_id: TypeId,
    // Same for the aggregate function.
    #[allow(clippy::type_complexity)]
    aggregator_function: fn(Box<dyn Aggregate>, Box<dyn Aggregate>) -> Option<Box<dyn Aggregate>>,
    event: Event,
}

pub struct Event {
    original: Box<dyn Aggregate>,
    timestamp: OffsetDateTime,
    user_agents: HashSet<String>,
    total: usize,
}

/// This function should always be called on the same type. If `this` and `other`
/// aren't the same type the function will do nothing and return `None`.
fn downcast_aggregate<ConcreteType: Aggregate>(
    old: Box<dyn Aggregate>,
    new: Box<dyn Aggregate>,
) -> Option<Box<dyn Aggregate>> {
    if old.is::<ConcreteType>() && new.is::<ConcreteType>() {
        // Both the two following lines cannot fail, but just to be sure we don't crash, we're still avoiding unwrapping
        let this = old.downcast::<ConcreteType>().ok()?;
        let other = new.downcast::<ConcreteType>().ok()?;
        Some(ConcreteType::aggregate(this, other))
    } else {
        None
    }
}

impl Message {
    pub fn new<T: Aggregate>(event: T, request: &HttpRequest) -> Self {
        Self {
            type_id: TypeId::of::<T>(),
            event: Event {
                original: Box::new(event),
                timestamp: OffsetDateTime::now_utc(),
                user_agents: extract_user_agents(request),
                total: 1,
            },
            aggregator_function: downcast_aggregate::<T>,
        }
    }
}

pub struct SegmentAnalytics {
    pub instance_uid: InstanceUid,
    pub user: User,
    pub sender: Sender<Message>,
}

impl SegmentAnalytics {
    #[allow(clippy::new_ret_no_self)]
    pub async fn new(
        opt: &Opt,
        index_scheduler: Arc<IndexScheduler>,
        auth_controller: Arc<AuthController>,
    ) -> Option<Arc<Self>> {
        let instance_uid = super::find_user_id(&opt.db_path);
        let first_time_run = instance_uid.is_none();
        let instance_uid = instance_uid.unwrap_or_else(Uuid::new_v4);
        write_user_id(&opt.db_path, &instance_uid);

        let client = reqwest::Client::builder().connect_timeout(Duration::from_secs(10)).build();

        // if reqwest throws an error we won't be able to send analytics
        if client.is_err() {
            return None;
        }

        let client =
            HttpClient::new(client.unwrap(), "https://telemetry.meilisearch.com".to_string());
        let user = User::UserId { user_id: instance_uid.to_string() };
        let mut batcher = AutoBatcher::new(client, Batcher::new(None), SEGMENT_API_KEY.to_string());

        // If Meilisearch is Launched for the first time:
        // 1. Send an event Launched associated to the user `total_launch`.
        // 2. Batch an event Launched with the real instance-id and send it in one hour.
        if first_time_run {
            let _ = batcher
                .push(Track {
                    user: User::UserId { user_id: "total_launch".to_string() },
                    event: "Launched".to_string(),
                    ..Default::default()
                })
                .await;
            let _ = batcher.flush().await;
            let _ = batcher
                .push(Track {
                    user: user.clone(),
                    event: "Launched".to_string(),
                    ..Default::default()
                })
                .await;
        }

        let (sender, inbox) = mpsc::channel(100); // How many analytics can we bufferize

        let segment = Box::new(Segment {
            inbox,
            user: user.clone(),
            opt: opt.clone(),
            batcher,
            events: HashMap::new(),
        });
        tokio::spawn(segment.run(index_scheduler.clone(), auth_controller.clone()));

        let this = Self { instance_uid, sender, user: user.clone() };

        Some(Arc::new(this))
    }
}

/// This structure represent the `infos` field we send in the analytics.
/// It's quite close to the `Opt` structure except all sensitive informations
/// have been simplified to a boolean.
/// It's send as-is in amplitude thus you should never update a name of the
/// struct without the approval of the PM.
#[derive(Debug, Clone, Serialize)]
struct Infos {
    env: String,
    experimental_contains_filter: bool,
    experimental_vector_store: bool,
    experimental_enable_metrics: bool,
    experimental_edit_documents_by_function: bool,
    experimental_search_queue_size: usize,
    experimental_drop_search_after: usize,
    experimental_nb_searches_per_core: usize,
    experimental_logs_mode: LogMode,
    experimental_dumpless_upgrade: bool,
    experimental_replication_parameters: bool,
    experimental_enable_logs_route: bool,
    experimental_reduce_indexing_memory_usage: bool,
    experimental_max_number_of_batched_tasks: usize,
    experimental_limit_batched_tasks_total_size: u64,
    gpu_enabled: bool,
    db_path: bool,
    import_dump: bool,
    dump_dir: bool,
    ignore_missing_dump: bool,
    ignore_dump_if_db_exists: bool,
    import_snapshot: bool,
    schedule_snapshot: Option<u64>,
    snapshot_dir: bool,
    ignore_missing_snapshot: bool,
    ignore_snapshot_if_db_exists: bool,
    http_addr: bool,
    http_payload_size_limit: Byte,
    task_queue_webhook: bool,
    task_webhook_authorization_header: bool,
    log_level: String,
    max_indexing_memory: MaxMemory,
    max_indexing_threads: MaxThreads,
    with_configuration_file: bool,
    ssl_auth_path: bool,
    ssl_cert_path: bool,
    ssl_key_path: bool,
    ssl_ocsp_path: bool,
    ssl_require_auth: bool,
    ssl_resumption: bool,
    ssl_tickets: bool,
}

impl Infos {
    pub fn new(options: Opt, features: RuntimeTogglableFeatures) -> Self {
        // We wants to decompose this whole struct by hand to be sure we don't forget
        // to add analytics when we add a field in the Opt.
        // Thus we must not insert `..` at the end.
        let Opt {
            db_path,
            experimental_contains_filter,
            experimental_enable_metrics,
            experimental_search_queue_size,
            experimental_drop_search_after,
            experimental_nb_searches_per_core,
            experimental_logs_mode,
            experimental_dumpless_upgrade,
            experimental_replication_parameters,
            experimental_enable_logs_route,
            experimental_reduce_indexing_memory_usage,
            experimental_max_number_of_batched_tasks,
            experimental_limit_batched_tasks_total_size,
            http_addr,
            master_key: _,
            env,
            task_webhook_url,
            task_webhook_authorization_header,
            max_index_size: _,
            max_task_db_size: _,
            http_payload_size_limit,
            ssl_cert_path,
            ssl_key_path,
            ssl_auth_path,
            ssl_ocsp_path,
            ssl_require_auth,
            ssl_resumption,
            ssl_tickets,
            import_snapshot,
            ignore_missing_snapshot,
            ignore_snapshot_if_db_exists,
            snapshot_dir,
            schedule_snapshot,
            import_dump,
            ignore_missing_dump,
            ignore_dump_if_db_exists,
            dump_dir,
            log_level,
            indexer_options,
            config_file_path,
            no_analytics: _,
        } = options;

        let schedule_snapshot = match schedule_snapshot {
            ScheduleSnapshot::Disabled => None,
            ScheduleSnapshot::Enabled(interval) => Some(interval),
        };

        let IndexerOpts { max_indexing_memory, max_indexing_threads, skip_index_budget: _ } =
            indexer_options;

        let RuntimeTogglableFeatures {
            vector_store,
            metrics,
            logs_route,
            edit_documents_by_function,
            contains_filter,
        } = features;

        // We're going to override every sensible information.
        // We consider information sensible if it contains a path, an address, or a key.
        Self {
            env,
            experimental_contains_filter: experimental_contains_filter | contains_filter,
            experimental_vector_store: vector_store,
            experimental_edit_documents_by_function: edit_documents_by_function,
            experimental_enable_metrics: experimental_enable_metrics | metrics,
            experimental_search_queue_size,
            experimental_drop_search_after: experimental_drop_search_after.into(),
            experimental_nb_searches_per_core: experimental_nb_searches_per_core.into(),
            experimental_logs_mode,
            experimental_dumpless_upgrade,
            experimental_replication_parameters,
            experimental_enable_logs_route: experimental_enable_logs_route | logs_route,
            experimental_reduce_indexing_memory_usage,
            gpu_enabled: meilisearch_types::milli::vector::is_cuda_enabled(),
            db_path: db_path != PathBuf::from("./data.ms"),
            import_dump: import_dump.is_some(),
            dump_dir: dump_dir != PathBuf::from("dumps/"),
            ignore_missing_dump,
            ignore_dump_if_db_exists,
            import_snapshot: import_snapshot.is_some(),
            schedule_snapshot,
            snapshot_dir: snapshot_dir != PathBuf::from("snapshots/"),
            ignore_missing_snapshot,
            ignore_snapshot_if_db_exists,
            http_addr: http_addr != default_http_addr(),
            http_payload_size_limit,
            experimental_max_number_of_batched_tasks,
            experimental_limit_batched_tasks_total_size,
            task_queue_webhook: task_webhook_url.is_some(),
            task_webhook_authorization_header: task_webhook_authorization_header.is_some(),
            log_level: log_level.to_string(),
            max_indexing_memory,
            max_indexing_threads,
            with_configuration_file: config_file_path.is_some(),
            ssl_auth_path: ssl_auth_path.is_some(),
            ssl_cert_path: ssl_cert_path.is_some(),
            ssl_key_path: ssl_key_path.is_some(),
            ssl_ocsp_path: ssl_ocsp_path.is_some(),
            ssl_require_auth,
            ssl_resumption,
            ssl_tickets,
        }
    }
}

pub struct Segment {
    inbox: Receiver<Message>,
    user: User,
    opt: Opt,
    batcher: AutoBatcher,
    events: HashMap<TypeId, Event>,
}

impl Segment {
    fn compute_traits(opt: &Opt, stats: Stats, features: RuntimeTogglableFeatures) -> Value {
        static FIRST_START_TIMESTAMP: Lazy<Instant> = Lazy::new(Instant::now);
        static SYSTEM: Lazy<Value> = Lazy::new(|| {
            let disks = Disks::new_with_refreshed_list();
            let mut sys = System::new_all();
            sys.refresh_all();
            let kernel_version = System::kernel_version()
                .and_then(|k| k.split_once('-').map(|(k, _)| k.to_string()));
            json!({
                    "distribution": System::name(),
                    "kernel_version": kernel_version,
                    "cores": sys.cpus().len(),
                    "ram_size": sys.total_memory(),
                    "disk_size": disks.iter().map(|disk| disk.total_space()).max(),
                    "server_provider": std::env::var("MEILI_SERVER_PROVIDER").ok(),
            })
        });
        let number_of_documents =
            stats.indexes.values().map(|index| index.number_of_documents).collect::<Vec<u64>>();

        json!({
            "start_since_days": FIRST_START_TIMESTAMP.elapsed().as_secs() / (60 * 60 * 24), // one day
            "system": *SYSTEM,
            "stats": {
                "database_size": stats.database_size,
                "indexes_number": stats.indexes.len(),
                "documents_number": number_of_documents,
            },
            "infos": Infos::new(opt.clone(), features),
        })
    }

    async fn run(
        mut self,
        index_scheduler: Arc<IndexScheduler>,
        auth_controller: Arc<AuthController>,
    ) {
        const INTERVAL: Duration = Duration::from_secs(60 * 60); // one hour
                                                                 // The first batch must be sent after one hour.
        let mut interval =
            tokio::time::interval_at(tokio::time::Instant::now() + INTERVAL, INTERVAL);

        loop {
            select! {
                _ = interval.tick() => {
                    self.tick(index_scheduler.clone(), auth_controller.clone()).await;
                },
                Some(msg) = self.inbox.recv() => {
                    self.handle_msg(msg);
               }
            }
        }
    }

    fn handle_msg(&mut self, Message { type_id, aggregator_function, event }: Message) {
        let new_event = match self.events.remove(&type_id) {
            Some(old) => {
                // The function should never fail since we retrieved the corresponding TypeId in the map. But in the unfortunate
                // case it could happens we're going to silently ignore the error
                let Some(original) = (aggregator_function)(old.original, event.original) else {
                    return;
                };
                Event {
                    original,
                    // We always want to return the FIRST timestamp ever encountered
                    timestamp: old.timestamp,
                    user_agents: old.user_agents.union(&event.user_agents).cloned().collect(),
                    total: old.total.saturating_add(event.total),
                }
            }
            None => event,
        };
        self.events.insert(type_id, new_event);
    }

    async fn tick(
        &mut self,
        index_scheduler: Arc<IndexScheduler>,
        auth_controller: Arc<AuthController>,
    ) {
        if let Ok(stats) = create_all_stats(
            index_scheduler.clone().into(),
            auth_controller.into(),
            &AuthFilter::default(),
        ) {
            // Replace the version number with the prototype name if any.
            let version = build_info::DescribeResult::from_build()
                .and_then(|describe| describe.as_prototype())
                .unwrap_or(env!("CARGO_PKG_VERSION"));

            let _ = self
                .batcher
                .push(Identify {
                    context: Some(json!({
                        "app": {
                            "version": version.to_string(),
                        },
                    })),
                    user: self.user.clone(),
                    traits: Self::compute_traits(
                        &self.opt,
                        stats,
                        index_scheduler.features().runtime_features(),
                    ),
                    ..Default::default()
                })
                .await;
        }

        // We empty the list of events
        let events = std::mem::take(&mut self.events);

        for (_, event) in events {
            let Event { original, timestamp, user_agents, total } = event;
            let name = original.event_name();
            let mut properties = original.into_event();
            if properties["user-agent"].is_null() {
                properties["user-agent"] = json!(user_agents);
            };
            if properties["requests"]["total_received"].is_null() {
                properties["requests"]["total_received"] = total.into();
            };

            let _ = self
                .batcher
                .push(Track {
                    user: self.user.clone(),
                    event: name.to_string(),
                    properties,
                    timestamp: Some(timestamp),
                    ..Default::default()
                })
                .await;
        }

        let _ = self.batcher.flush().await;
    }
}
