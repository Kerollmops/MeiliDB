use std::env::VarError;
use std::ffi::OsStr;
use std::fmt::Display;
use std::io::{BufReader, Read};
use std::num::{NonZeroUsize, ParseIntError};
use std::ops::Deref;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use std::{env, fmt, fs};

use byte_unit::{Byte, ParseError, UnitType};
use clap::Parser;
use meilisearch_types::features::InstanceTogglableFeatures;
use meilisearch_types::milli::update::IndexerConfig;
use meilisearch_types::milli::ThreadPoolNoAbortBuilder;
use rustls::server::{ServerSessionMemoryCache, WebPkiClientVerifier};
use rustls::RootCertStore;
use rustls_pemfile::{certs, rsa_private_keys};
use serde::{Deserialize, Serialize};
use sysinfo::{MemoryRefreshKind, RefreshKind, System};
use url::Url;

const POSSIBLE_ENV: [&str; 2] = ["development", "production"];

const MEILI_DB_PATH: &str = "MEILI_DB_PATH";
const MEILI_HTTP_ADDR: &str = "MEILI_HTTP_ADDR";
const MEILI_MASTER_KEY: &str = "MEILI_MASTER_KEY";
const MEILI_ENV: &str = "MEILI_ENV";
const MEILI_TASK_WEBHOOK_URL: &str = "MEILI_TASK_WEBHOOK_URL";
const MEILI_TASK_WEBHOOK_AUTHORIZATION_HEADER: &str = "MEILI_TASK_WEBHOOK_AUTHORIZATION_HEADER";
const MEILI_NO_ANALYTICS: &str = "MEILI_NO_ANALYTICS";
const MEILI_HTTP_PAYLOAD_SIZE_LIMIT: &str = "MEILI_HTTP_PAYLOAD_SIZE_LIMIT";
const MEILI_SSL_CERT_PATH: &str = "MEILI_SSL_CERT_PATH";
const MEILI_SSL_KEY_PATH: &str = "MEILI_SSL_KEY_PATH";
const MEILI_SSL_AUTH_PATH: &str = "MEILI_SSL_AUTH_PATH";
const MEILI_SSL_OCSP_PATH: &str = "MEILI_SSL_OCSP_PATH";
const MEILI_SSL_REQUIRE_AUTH: &str = "MEILI_SSL_REQUIRE_AUTH";
const MEILI_SSL_RESUMPTION: &str = "MEILI_SSL_RESUMPTION";
const MEILI_SSL_TICKETS: &str = "MEILI_SSL_TICKETS";
const MEILI_IMPORT_SNAPSHOT: &str = "MEILI_IMPORT_SNAPSHOT";
const MEILI_IGNORE_MISSING_SNAPSHOT: &str = "MEILI_IGNORE_MISSING_SNAPSHOT";
const MEILI_IGNORE_SNAPSHOT_IF_DB_EXISTS: &str = "MEILI_IGNORE_SNAPSHOT_IF_DB_EXISTS";
const MEILI_SNAPSHOT_DIR: &str = "MEILI_SNAPSHOT_DIR";
const MEILI_SCHEDULE_SNAPSHOT: &str = "MEILI_SCHEDULE_SNAPSHOT";
const MEILI_IMPORT_DUMP: &str = "MEILI_IMPORT_DUMP";
const MEILI_IGNORE_MISSING_DUMP: &str = "MEILI_IGNORE_MISSING_DUMP";
const MEILI_IGNORE_DUMP_IF_DB_EXISTS: &str = "MEILI_IGNORE_DUMP_IF_DB_EXISTS";
const MEILI_DUMP_DIR: &str = "MEILI_DUMP_DIR";
const MEILI_LOG_LEVEL: &str = "MEILI_LOG_LEVEL";
const MEILI_EXPERIMENTAL_LOGS_MODE: &str = "MEILI_EXPERIMENTAL_LOGS_MODE";
const MEILI_EXPERIMENTAL_DUMPLESS_UPGRADE: &str = "MEILI_EXPERIMENTAL_DUMPLESS_UPGRADE";
const MEILI_EXPERIMENTAL_REPLICATION_PARAMETERS: &str = "MEILI_EXPERIMENTAL_REPLICATION_PARAMETERS";
const MEILI_EXPERIMENTAL_ENABLE_LOGS_ROUTE: &str = "MEILI_EXPERIMENTAL_ENABLE_LOGS_ROUTE";
const MEILI_EXPERIMENTAL_CONTAINS_FILTER: &str = "MEILI_EXPERIMENTAL_CONTAINS_FILTER";
const MEILI_EXPERIMENTAL_ENABLE_METRICS: &str = "MEILI_EXPERIMENTAL_ENABLE_METRICS";
const MEILI_EXPERIMENTAL_SEARCH_QUEUE_SIZE: &str = "MEILI_EXPERIMENTAL_SEARCH_QUEUE_SIZE";
const MEILI_EXPERIMENTAL_DROP_SEARCH_AFTER: &str = "MEILI_EXPERIMENTAL_DROP_SEARCH_AFTER";
const MEILI_EXPERIMENTAL_NB_SEARCHES_PER_CORE: &str = "MEILI_EXPERIMENTAL_NB_SEARCHES_PER_CORE";
const MEILI_EXPERIMENTAL_REDUCE_INDEXING_MEMORY_USAGE: &str =
    "MEILI_EXPERIMENTAL_REDUCE_INDEXING_MEMORY_USAGE";
const MEILI_EXPERIMENTAL_MAX_NUMBER_OF_BATCHED_TASKS: &str =
    "MEILI_EXPERIMENTAL_MAX_NUMBER_OF_BATCHED_TASKS";
const MEILI_EXPERIMENTAL_LIMIT_BATCHED_TASKS_TOTAL_SIZE: &str =
    "MEILI_EXPERIMENTAL_LIMIT_BATCHED_TASKS_SIZE";

const DEFAULT_CONFIG_FILE_PATH: &str = "./config.toml";
const DEFAULT_DB_PATH: &str = "./data.ms";
const DEFAULT_HTTP_ADDR: &str = "localhost:7700";
const DEFAULT_ENV: &str = "development";
const DEFAULT_HTTP_PAYLOAD_SIZE_LIMIT: &str = "100 MB";
const DEFAULT_SNAPSHOT_DIR: &str = "snapshots/";
const DEFAULT_SNAPSHOT_INTERVAL_SEC: u64 = 86400;
const DEFAULT_SNAPSHOT_INTERVAL_SEC_STR: &str = "86400";
const DEFAULT_DUMP_DIR: &str = "dumps/";

const MEILI_MAX_INDEXING_MEMORY: &str = "MEILI_MAX_INDEXING_MEMORY";
const MEILI_MAX_INDEXING_THREADS: &str = "MEILI_MAX_INDEXING_THREADS";
const DEFAULT_LOG_EVERY_N: usize = 100_000;

// Each environment (index and task-db) is taking space in the virtual address space.
// Ideally, indexes can occupy 2TiB each to avoid having to manually resize them.
// The actual size of the virtual address space is computed at startup to determine how many 2TiB indexes can be
// opened simultaneously.
pub const INDEX_SIZE: u64 = 2 * 1024 * 1024 * 1024 * 1024; // 2 TiB
pub const TASK_DB_SIZE: u64 = 20 * 1024 * 1024 * 1024; // 20 GiB

#[derive(Debug, Default, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum LogMode {
    #[default]
    Human,
    Json,
}

impl Display for LogMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LogMode::Human => Display::fmt("HUMAN", f),
            LogMode::Json => Display::fmt("JSON", f),
        }
    }
}

impl FromStr for LogMode {
    type Err = LogModeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim().to_lowercase().as_str() {
            "human" => Ok(LogMode::Human),
            "json" => Ok(LogMode::Json),
            _ => Err(LogModeError(s.to_owned())),
        }
    }
}

#[derive(Debug, thiserror::Error)]
#[error("Unsupported log mode level `{0}`. Supported values are `HUMAN` and `JSON`.")]
pub struct LogModeError(String);

#[derive(Debug, Default, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum LogLevel {
    Off,
    Error,
    Warn,
    #[default]
    Info,
    Debug,
    Trace,
}

#[derive(Debug)]
pub struct LogLevelError {
    pub given_log_level: String,
}

impl Display for LogLevelError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(
            f,
            "Log level '{}' is invalid. Accepted values are 'OFF', 'ERROR', 'WARN', 'INFO', 'DEBUG', and 'TRACE'.",
            self.given_log_level
        )
    }
}

impl Display for LogLevel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LogLevel::Off => Display::fmt("OFF", f),
            LogLevel::Error => Display::fmt("ERROR", f),
            LogLevel::Warn => Display::fmt("WARN", f),
            LogLevel::Info => Display::fmt("INFO", f),
            LogLevel::Debug => Display::fmt("DEBUG", f),
            LogLevel::Trace => Display::fmt("TRACE", f),
        }
    }
}

impl std::error::Error for LogLevelError {}

impl FromStr for LogLevel {
    type Err = LogLevelError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim().to_lowercase().as_str() {
            "off" => Ok(LogLevel::Off),
            "error" => Ok(LogLevel::Error),
            "warn" => Ok(LogLevel::Warn),
            "info" => Ok(LogLevel::Info),
            "debug" => Ok(LogLevel::Debug),
            "trace" => Ok(LogLevel::Trace),
            _ => Err(LogLevelError { given_log_level: s.to_owned() }),
        }
    }
}

#[derive(Debug, Clone, Parser, Deserialize)]
#[clap(version, next_display_order = None)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct Opt {
    /// Designates the location where database files will be created and retrieved.
    #[clap(long, env = MEILI_DB_PATH, default_value_os_t = default_db_path())]
    #[serde(default = "default_db_path")]
    pub db_path: PathBuf,

    /// Sets the HTTP address and port Meilisearch will use.
    #[clap(long, env = MEILI_HTTP_ADDR, default_value_t = default_http_addr())]
    #[serde(default = "default_http_addr")]
    pub http_addr: String,

    /// Sets the instance's master key, automatically protecting all routes except `GET /health`.
    #[clap(long, env = MEILI_MASTER_KEY)]
    pub master_key: Option<String>,

    /// Configures the instance's environment. Value must be either `production` or `development`.
    #[clap(long, env = MEILI_ENV, default_value_t = default_env(), value_parser = POSSIBLE_ENV)]
    #[serde(default = "default_env")]
    pub env: String,

    /// Called whenever a task finishes so a third party can be notified.
    #[clap(long, env = MEILI_TASK_WEBHOOK_URL)]
    pub task_webhook_url: Option<Url>,

    /// The Authorization header to send on the webhook URL whenever
    /// a task finishes so a third party can be notified.
    #[clap(long, env = MEILI_TASK_WEBHOOK_AUTHORIZATION_HEADER)]
    pub task_webhook_authorization_header: Option<String>,

    /// Deactivates Meilisearch's built-in telemetry when provided.
    ///
    /// Meilisearch automatically collects data from all instances that
    /// do not opt out using this flag. All gathered data is used solely
    /// for the purpose of improving Meilisearch, and can be deleted
    /// at any time.
    #[serde(default)] // we can't send true
    #[clap(long, env = MEILI_NO_ANALYTICS)]
    pub no_analytics: bool,

    /// Sets the maximum size of the index. Value must be given in bytes or explicitly
    /// stating a base unit (for instance: 107374182400, '107.7Gb', or '107374 Mb').
    #[clap(skip = default_max_index_size())]
    #[serde(skip, default = "default_max_index_size")]
    pub max_index_size: Byte,

    /// Sets the maximum size of the task database. Value must be given in bytes or explicitly stating a
    /// base unit (for instance: 107374182400, '107.7Gb', or '107374 Mb').
    #[clap(skip = default_max_task_db_size())]
    #[serde(skip, default = "default_max_task_db_size")]
    pub max_task_db_size: Byte,

    /// Sets the maximum size of accepted payloads. Value must be given in bytes or explicitly stating a
    /// base unit (for instance: 107374182400, '107.7Gb', or '107374 Mb').
    #[clap(long, env = MEILI_HTTP_PAYLOAD_SIZE_LIMIT, default_value_t = default_http_payload_size_limit())]
    #[serde(default = "default_http_payload_size_limit")]
    pub http_payload_size_limit: Byte,

    /// Sets the server's SSL certificates.
    #[clap(long, env = MEILI_SSL_CERT_PATH, value_parser)]
    pub ssl_cert_path: Option<PathBuf>,

    /// Sets the server's SSL key files.
    #[clap(long, env = MEILI_SSL_KEY_PATH, value_parser)]
    pub ssl_key_path: Option<PathBuf>,

    /// Enables client authentication in the specified path.
    #[clap(long, env = MEILI_SSL_AUTH_PATH, value_parser)]
    pub ssl_auth_path: Option<PathBuf>,

    /// Sets the server's OCSP file. *Optional*
    ///
    /// Reads DER-encoded OCSP response from OCSPFILE and staple to certificate.
    #[clap(long, env = MEILI_SSL_OCSP_PATH, value_parser)]
    pub ssl_ocsp_path: Option<PathBuf>,

    /// Makes SSL authentication mandatory.
    #[serde(default)]
    #[clap(long, env = MEILI_SSL_REQUIRE_AUTH)]
    pub ssl_require_auth: bool,

    /// Activates SSL session resumption.
    #[serde(default)]
    #[clap(long, env = MEILI_SSL_RESUMPTION)]
    pub ssl_resumption: bool,

    /// Activates SSL tickets.
    #[serde(default)]
    #[clap(long, env = MEILI_SSL_TICKETS)]
    pub ssl_tickets: bool,

    /// Launches Meilisearch after importing a previously-generated snapshot at the given filepath.
    #[clap(long, env = MEILI_IMPORT_SNAPSHOT)]
    pub import_snapshot: Option<PathBuf>,

    /// Prevents a Meilisearch instance from throwing an error when `--import-snapshot`
    /// does not point to a valid snapshot file.
    ///
    /// This command will throw an error if `--import-snapshot` is not defined.
    #[clap(
        long,
        env = MEILI_IGNORE_MISSING_SNAPSHOT,
        requires = "import_snapshot"
    )]
    #[serde(default)]
    pub ignore_missing_snapshot: bool,

    /// Prevents a Meilisearch instance with an existing database from throwing an
    /// error when using `--import-snapshot`. Instead, the snapshot will be ignored
    /// and Meilisearch will launch using the existing database.
    ///
    /// This command will throw an error if `--import-snapshot` is not defined.
    #[clap(
        long,
        env = MEILI_IGNORE_SNAPSHOT_IF_DB_EXISTS,
        requires = "import_snapshot"
    )]
    #[serde(default)]
    pub ignore_snapshot_if_db_exists: bool,

    /// Sets the directory where Meilisearch will store snapshots.
    #[clap(long, env = MEILI_SNAPSHOT_DIR, default_value_os_t = default_snapshot_dir())]
    #[serde(default = "default_snapshot_dir")]
    pub snapshot_dir: PathBuf,

    /// Activates scheduled snapshots when provided. Snapshots are disabled by default.
    ///
    /// When provided with a value, defines the interval between each snapshot, in seconds.
    #[clap(long,env = MEILI_SCHEDULE_SNAPSHOT, num_args(0..=1), value_parser=parse_schedule_snapshot, default_value_t, default_missing_value=default_snapshot_interval_sec(),  value_name = "SNAPSHOT_INTERVAL_SEC")]
    #[serde(default, deserialize_with = "schedule_snapshot_deserialize")]
    pub schedule_snapshot: ScheduleSnapshot,

    /// Imports the dump file located at the specified path. Path must point to a `.dump` file.
    /// If a database already exists, Meilisearch will throw an error and abort launch.
    #[clap(long, env = MEILI_IMPORT_DUMP, conflicts_with = "import_snapshot")]
    pub import_dump: Option<PathBuf>,

    /// Prevents Meilisearch from throwing an error when `--import-dump` does not point to
    /// a valid dump file. Instead, Meilisearch will start normally without importing any dump.
    ///
    /// This option will trigger an error if `--import-dump` is not defined.
    #[clap(long, env = MEILI_IGNORE_MISSING_DUMP, requires = "import_dump")]
    #[serde(default)]
    pub ignore_missing_dump: bool,

    /// Prevents a Meilisearch instance with an existing database from throwing an error
    /// when using `--import-dump`. Instead, the dump will be ignored and Meilisearch will
    /// launch using the existing database.
    ///
    /// This option will trigger an error if `--import-dump` is not defined.
    #[clap(long, env = MEILI_IGNORE_DUMP_IF_DB_EXISTS, requires = "import_dump")]
    #[serde(default)]
    pub ignore_dump_if_db_exists: bool,

    /// Sets the directory where Meilisearch will create dump files.
    #[clap(long, env = MEILI_DUMP_DIR, default_value_os_t = default_dump_dir())]
    #[serde(default = "default_dump_dir")]
    pub dump_dir: PathBuf,

    /// Defines how much detail should be present in Meilisearch's logs.
    ///
    /// Meilisearch currently supports six log levels, listed in order of
    /// increasing verbosity: OFF, ERROR, WARN, INFO, DEBUG, TRACE.
    #[clap(long, env = MEILI_LOG_LEVEL, default_value_t)]
    #[serde(default)]
    pub log_level: LogLevel,

    /// Experimental contains filter feature. For more information,
    /// see: <https://github.com/orgs/meilisearch/discussions/763>
    ///
    /// Enables the experimental contains filter operator.
    #[clap(long, env = MEILI_EXPERIMENTAL_CONTAINS_FILTER)]
    #[serde(default)]
    pub experimental_contains_filter: bool,

    /// Experimental metrics feature. For more information,
    /// see: <https://github.com/meilisearch/meilisearch/discussions/3518>
    ///
    /// Enables the Prometheus metrics on the `GET /metrics` endpoint.
    #[clap(long, env = MEILI_EXPERIMENTAL_ENABLE_METRICS)]
    #[serde(default)]
    pub experimental_enable_metrics: bool,

    /// Experimental search queue size. For more information,
    /// see: <https://github.com/orgs/meilisearch/discussions/729>
    ///
    /// Lets you customize the size of the search queue. Meilisearch processes
    /// your search requests as fast as possible but once the queue is full
    /// it starts returning HTTP 503, Service Unavailable.
    ///
    /// The default value is 1000.
    #[clap(long, env = MEILI_EXPERIMENTAL_SEARCH_QUEUE_SIZE, default_value_t = default_experimental_search_queue_size())]
    #[serde(default = "default_experimental_search_queue_size")]
    pub experimental_search_queue_size: usize,

    /// Experimental drop search after. For more information,
    /// see: <https://github.com/orgs/meilisearch/discussions/783>
    ///
    /// Let you customize after how many seconds Meilisearch should consider
    /// a search request irrelevant and drop it.
    ///
    /// The default value is 60.
    #[clap(long, env = MEILI_EXPERIMENTAL_DROP_SEARCH_AFTER, default_value_t = default_drop_search_after())]
    #[serde(default = "default_drop_search_after")]
    pub experimental_drop_search_after: NonZeroUsize,

    /// Experimental number of searches per core. For more information,
    /// see: <https://github.com/orgs/meilisearch/discussions/784>
    ///
    /// Lets you customize how many search requests can run on each core concurrently.
    /// The default value is 4.
    #[clap(long, env = MEILI_EXPERIMENTAL_NB_SEARCHES_PER_CORE, default_value_t = default_nb_searches_per_core())]
    #[serde(default = "default_nb_searches_per_core")]
    pub experimental_nb_searches_per_core: NonZeroUsize,

    /// Experimental logs mode feature. For more information,
    /// see: <https://github.com/orgs/meilisearch/discussions/723>
    ///
    /// Change the mode of the logs on the console.
    #[clap(long, env = MEILI_EXPERIMENTAL_LOGS_MODE, default_value_t)]
    #[serde(default)]
    pub experimental_logs_mode: LogMode,

    /// Experimental dumpless upgrade. For more information, see: <https://github.com/orgs/meilisearch/discussions/723>
    ///
    /// When set, Meilisearch will auto-update its database without using a dump.
    #[clap(long, env = MEILI_EXPERIMENTAL_DUMPLESS_UPGRADE, default_value_t)]
    #[serde(default)]
    pub experimental_dumpless_upgrade: bool,

    /// Experimental logs route feature. For more information,
    /// see: <https://github.com/orgs/meilisearch/discussions/721>
    ///
    /// Enables the log routes on the `POST /logs/stream`, `POST /logs/stderr` endpoints,
    /// and the `DELETE /logs/stream` to stop receiving logs.
    #[clap(long, env = MEILI_EXPERIMENTAL_ENABLE_LOGS_ROUTE)]
    #[serde(default)]
    pub experimental_enable_logs_route: bool,

    /// Enable multiple features that helps you to run meilisearch in a replicated context.
    /// For more information, see: <https://github.com/orgs/meilisearch/discussions/725>
    ///
    /// - /!\ Disable the automatic clean up of old processed tasks, you're in charge of that now
    /// - Lets you specify a custom task ID upon registering a task
    /// - Lets you execute dry-register a task (get an answer from the route but nothing is actually
    ///   registered in meilisearch and it won't be processed)
    #[clap(long, env = MEILI_EXPERIMENTAL_REPLICATION_PARAMETERS)]
    #[serde(default)]
    pub experimental_replication_parameters: bool,

    /// Experimental RAM reduction during indexing, do not use in production,
    /// see: <https://github.com/meilisearch/product/discussions/652>
    #[clap(long, env = MEILI_EXPERIMENTAL_REDUCE_INDEXING_MEMORY_USAGE)]
    #[serde(default)]
    pub experimental_reduce_indexing_memory_usage: bool,

    /// Experimentally reduces the maximum number of tasks that will be processed at once,
    /// see: <https://github.com/orgs/meilisearch/discussions/713>
    #[clap(long, env = MEILI_EXPERIMENTAL_MAX_NUMBER_OF_BATCHED_TASKS, default_value_t = default_limit_batched_tasks())]
    #[serde(default = "default_limit_batched_tasks")]
    pub experimental_max_number_of_batched_tasks: usize,

    /// Experimentally reduces the maximum total size, in bytes, of tasks that will be processed at once,
    /// see: <https://github.com/orgs/meilisearch/discussions/801>
    #[clap(long, env = MEILI_EXPERIMENTAL_LIMIT_BATCHED_TASKS_TOTAL_SIZE, default_value_t = default_limit_batched_tasks_total_size())]
    #[serde(default = "default_limit_batched_tasks_total_size")]
    pub experimental_limit_batched_tasks_total_size: u64,

    #[serde(flatten)]
    #[clap(flatten)]
    pub indexer_options: IndexerOpts,

    /// Set the path to a configuration file that should be used to setup the engine.
    /// Format must be TOML.
    #[clap(long)]
    pub config_file_path: Option<PathBuf>,
}

impl Opt {
    /// Whether analytics should be enabled or not.
    pub fn analytics(&self) -> bool {
        !self.no_analytics
    }

    /// Build a new Opt from config file, env vars and cli args.
    pub fn try_build() -> anyhow::Result<(Self, Option<PathBuf>)> {
        // Parse the args to get the config_file_path.
        let mut opts = Opt::parse();
        let mut config_read_from = None;
        let user_specified_config_file_path = opts
            .config_file_path
            .clone()
            .or_else(|| env::var("MEILI_CONFIG_FILE_PATH").map(PathBuf::from).ok());
        let config_file_path = user_specified_config_file_path
            .clone()
            .unwrap_or_else(|| PathBuf::from(DEFAULT_CONFIG_FILE_PATH));

        match std::fs::read_to_string(&config_file_path) {
            Ok(config) => {
                // If the file is successfully read, we deserialize it with `toml`.
                let opt_from_config = toml::from_str::<Opt>(&config)?;
                // Return an error if config file contains 'config_file_path'
                // Using that key in the config file doesn't make sense bc it creates a logical loop (config file referencing itself)
                if opt_from_config.config_file_path.is_some() {
                    anyhow::bail!("`config_file_path` is not supported in the configuration file")
                }
                // We inject the values from the toml in the corresponding env vars if needs be. Doing so, we respect the priority toml < env vars < cli args.
                opt_from_config.export_to_env();
                // Once injected we parse the cli args once again to take the new env vars into scope.
                opts = Opt::parse();
                config_read_from = Some(config_file_path);
            }
            Err(e) => {
                if let Some(path) = user_specified_config_file_path {
                    // If we have an error while reading the file defined by the user.
                    anyhow::bail!(
                        "unable to open or read the {:?} configuration file: {}.",
                        path,
                        e,
                    )
                }
            }
        }

        Ok((opts, config_read_from))
    }

    /// Exports the opts values to their corresponding env vars if they are not set.
    fn export_to_env(self) {
        let Opt {
            db_path,
            http_addr,
            master_key,
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
            snapshot_dir,
            schedule_snapshot,
            dump_dir,
            log_level,
            indexer_options,
            import_snapshot: _,
            ignore_missing_snapshot: _,
            ignore_snapshot_if_db_exists: _,
            import_dump: _,
            ignore_missing_dump: _,
            ignore_dump_if_db_exists: _,
            config_file_path: _,
            no_analytics,
            experimental_contains_filter,
            experimental_enable_metrics,
            experimental_search_queue_size,
            experimental_drop_search_after,
            experimental_nb_searches_per_core,
            experimental_logs_mode,
            experimental_dumpless_upgrade,
            experimental_enable_logs_route,
            experimental_replication_parameters,
            experimental_reduce_indexing_memory_usage,
            experimental_max_number_of_batched_tasks,
            experimental_limit_batched_tasks_total_size,
        } = self;
        export_to_env_if_not_present(MEILI_DB_PATH, db_path);
        export_to_env_if_not_present(MEILI_HTTP_ADDR, http_addr);
        if let Some(master_key) = master_key {
            export_to_env_if_not_present(MEILI_MASTER_KEY, master_key);
        }
        export_to_env_if_not_present(MEILI_ENV, env);
        if let Some(task_webhook_url) = task_webhook_url {
            export_to_env_if_not_present(MEILI_TASK_WEBHOOK_URL, task_webhook_url.to_string());
        }
        if let Some(task_webhook_authorization_header) = task_webhook_authorization_header {
            export_to_env_if_not_present(
                MEILI_TASK_WEBHOOK_AUTHORIZATION_HEADER,
                task_webhook_authorization_header,
            );
        }

        export_to_env_if_not_present(MEILI_NO_ANALYTICS, no_analytics.to_string());
        export_to_env_if_not_present(
            MEILI_HTTP_PAYLOAD_SIZE_LIMIT,
            http_payload_size_limit.to_string(),
        );
        if let Some(ssl_cert_path) = ssl_cert_path {
            export_to_env_if_not_present(MEILI_SSL_CERT_PATH, ssl_cert_path);
        }
        if let Some(ssl_key_path) = ssl_key_path {
            export_to_env_if_not_present(MEILI_SSL_KEY_PATH, ssl_key_path);
        }
        if let Some(ssl_auth_path) = ssl_auth_path {
            export_to_env_if_not_present(MEILI_SSL_AUTH_PATH, ssl_auth_path);
        }
        if let Some(ssl_ocsp_path) = ssl_ocsp_path {
            export_to_env_if_not_present(MEILI_SSL_OCSP_PATH, ssl_ocsp_path);
        }
        export_to_env_if_not_present(MEILI_SSL_REQUIRE_AUTH, ssl_require_auth.to_string());
        export_to_env_if_not_present(MEILI_SSL_RESUMPTION, ssl_resumption.to_string());
        export_to_env_if_not_present(MEILI_SSL_TICKETS, ssl_tickets.to_string());
        export_to_env_if_not_present(MEILI_SNAPSHOT_DIR, snapshot_dir);
        if let Some(snapshot_interval) = schedule_snapshot_to_env(schedule_snapshot) {
            export_to_env_if_not_present(MEILI_SCHEDULE_SNAPSHOT, snapshot_interval)
        }

        export_to_env_if_not_present(MEILI_DUMP_DIR, dump_dir);
        export_to_env_if_not_present(MEILI_LOG_LEVEL, log_level.to_string());
        export_to_env_if_not_present(
            MEILI_EXPERIMENTAL_CONTAINS_FILTER,
            experimental_contains_filter.to_string(),
        );
        export_to_env_if_not_present(
            MEILI_EXPERIMENTAL_ENABLE_METRICS,
            experimental_enable_metrics.to_string(),
        );
        export_to_env_if_not_present(
            MEILI_EXPERIMENTAL_SEARCH_QUEUE_SIZE,
            experimental_search_queue_size.to_string(),
        );
        export_to_env_if_not_present(
            MEILI_EXPERIMENTAL_DROP_SEARCH_AFTER,
            experimental_drop_search_after.to_string(),
        );
        export_to_env_if_not_present(
            MEILI_EXPERIMENTAL_NB_SEARCHES_PER_CORE,
            experimental_nb_searches_per_core.to_string(),
        );
        export_to_env_if_not_present(
            MEILI_EXPERIMENTAL_LOGS_MODE,
            experimental_logs_mode.to_string(),
        );
        export_to_env_if_not_present(
            MEILI_EXPERIMENTAL_DUMPLESS_UPGRADE,
            experimental_dumpless_upgrade.to_string(),
        );
        export_to_env_if_not_present(
            MEILI_EXPERIMENTAL_REPLICATION_PARAMETERS,
            experimental_replication_parameters.to_string(),
        );
        export_to_env_if_not_present(
            MEILI_EXPERIMENTAL_ENABLE_LOGS_ROUTE,
            experimental_enable_logs_route.to_string(),
        );
        export_to_env_if_not_present(
            MEILI_EXPERIMENTAL_REDUCE_INDEXING_MEMORY_USAGE,
            experimental_reduce_indexing_memory_usage.to_string(),
        );
        export_to_env_if_not_present(
            MEILI_EXPERIMENTAL_MAX_NUMBER_OF_BATCHED_TASKS,
            experimental_max_number_of_batched_tasks.to_string(),
        );
        export_to_env_if_not_present(
            MEILI_EXPERIMENTAL_LIMIT_BATCHED_TASKS_TOTAL_SIZE,
            experimental_limit_batched_tasks_total_size.to_string(),
        );
        indexer_options.export_to_env();
    }

    pub fn get_ssl_config(&self) -> anyhow::Result<Option<rustls::ServerConfig>> {
        if let (Some(cert_path), Some(key_path)) = (&self.ssl_cert_path, &self.ssl_key_path) {
            let config = rustls::ServerConfig::builder();

            let config = match &self.ssl_auth_path {
                Some(auth_path) => {
                    let roots = load_certs(auth_path.to_path_buf())?;
                    let mut client_auth_roots = RootCertStore::empty();
                    for root in roots {
                        client_auth_roots.add(root).unwrap();
                    }
                    let mut client_verifier =
                        WebPkiClientVerifier::builder(client_auth_roots.into());
                    if !self.ssl_require_auth {
                        client_verifier = client_verifier.allow_unauthenticated();
                    }
                    config.with_client_cert_verifier(client_verifier.build()?)
                }
                None => config.with_no_client_auth(),
            };

            let certs = load_certs(cert_path.to_path_buf())?;
            let privkey = load_private_key(key_path.to_path_buf())?;
            let ocsp = load_ocsp(&self.ssl_ocsp_path)?;
            let mut config = config
                .with_single_cert_with_ocsp(certs, privkey, ocsp)
                .map_err(|_| anyhow::anyhow!("bad certificates/private key"))?;

            config.key_log = Arc::new(rustls::KeyLogFile::new());

            if self.ssl_resumption {
                config.session_storage = ServerSessionMemoryCache::new(256);
            }

            if self.ssl_tickets {
                config.ticketer = rustls::crypto::ring::Ticketer::new().unwrap();
            }

            Ok(Some(config))
        } else {
            Ok(None)
        }
    }

    pub(crate) fn to_instance_features(&self) -> InstanceTogglableFeatures {
        InstanceTogglableFeatures {
            metrics: self.experimental_enable_metrics,
            logs_route: self.experimental_enable_logs_route,
            contains_filter: self.experimental_contains_filter,
        }
    }
}

#[derive(Debug, Default, Clone, Parser, Deserialize)]
pub struct IndexerOpts {
    /// Sets the maximum amount of RAM Meilisearch can use when indexing. By default, Meilisearch
    /// uses no more than two thirds of available memory.
    #[clap(long, env = MEILI_MAX_INDEXING_MEMORY, default_value_t)]
    #[serde(default)]
    pub max_indexing_memory: MaxMemory,

    /// Sets the maximum number of threads Meilisearch can use during indexation. By default, the
    /// indexer avoids using more than half of a machine's total processing units. This ensures
    /// Meilisearch is always ready to perform searches, even while you are updating an index.
    #[clap(long, env = MEILI_MAX_INDEXING_THREADS, default_value_t)]
    #[serde(default)]
    pub max_indexing_threads: MaxThreads,

    /// Whether or not we want to determine the budget of virtual memory address space we have available dynamically
    /// (the default), or statically.
    ///
    /// Determining the budget of virtual memory address space dynamically takes some time on some systems (such as macOS)
    /// and may make tests non-deterministic, so we want to skip it in tests.
    #[clap(skip)]
    #[serde(skip)]
    pub skip_index_budget: bool,
}

impl IndexerOpts {
    /// Exports the values to their corresponding env vars if they are not set.
    pub fn export_to_env(self) {
        let IndexerOpts { max_indexing_memory, max_indexing_threads, skip_index_budget: _ } = self;
        if let Some(max_indexing_memory) = max_indexing_memory.0 {
            export_to_env_if_not_present(
                MEILI_MAX_INDEXING_MEMORY,
                max_indexing_memory.to_string(),
            );
        }
        export_to_env_if_not_present(
            MEILI_MAX_INDEXING_THREADS,
            max_indexing_threads.0.to_string(),
        );
    }
}

impl TryFrom<&IndexerOpts> for IndexerConfig {
    type Error = anyhow::Error;

    fn try_from(other: &IndexerOpts) -> Result<Self, Self::Error> {
        let thread_pool = ThreadPoolNoAbortBuilder::new()
            .thread_name(|index| format!("indexing-thread:{index}"))
            .num_threads(*other.max_indexing_threads)
            .build()?;

        Ok(Self {
            log_every_n: Some(DEFAULT_LOG_EVERY_N),
            max_memory: other.max_indexing_memory.map(|b| b.as_u64() as usize),
            thread_pool: Some(thread_pool),
            max_positions_per_attributes: None,
            skip_index_budget: other.skip_index_budget,
            ..Default::default()
        })
    }
}

/// A type used to detect the max memory available and use 2/3 of it.
#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
pub struct MaxMemory(Option<Byte>);

impl FromStr for MaxMemory {
    type Err = ParseError;

    fn from_str(s: &str) -> Result<MaxMemory, Self::Err> {
        Byte::from_str(s).map(Some).map(MaxMemory)
    }
}

impl Default for MaxMemory {
    fn default() -> MaxMemory {
        MaxMemory(total_memory_bytes().map(|bytes| bytes * 2 / 3).map(Byte::from_u64))
    }
}

impl fmt::Display for MaxMemory {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self.0 {
            Some(memory) => {
                write!(f, "{}", memory.get_appropriate_unit(UnitType::Binary))
            }
            None => f.write_str("unknown"),
        }
    }
}

impl Deref for MaxMemory {
    type Target = Option<Byte>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl MaxMemory {
    pub fn unlimited() -> Self {
        Self(None)
    }
}

/// Returns the total amount of bytes available or `None` if this system isn't supported.
fn total_memory_bytes() -> Option<u64> {
    if sysinfo::IS_SUPPORTED_SYSTEM {
        let mem_kind = RefreshKind::nothing().with_memory(MemoryRefreshKind::nothing().with_ram());
        let mut system = System::new_with_specifics(mem_kind);
        system.refresh_memory();
        Some(system.total_memory())
    } else {
        None
    }
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
pub struct MaxThreads(usize);

impl FromStr for MaxThreads {
    type Err = ParseIntError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        usize::from_str(s).map(Self)
    }
}

impl Default for MaxThreads {
    fn default() -> Self {
        MaxThreads(num_cpus::get() / 2)
    }
}

impl fmt::Display for MaxThreads {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Deref for MaxThreads {
    type Target = usize;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

fn load_certs(
    filename: PathBuf,
) -> anyhow::Result<Vec<rustls::pki_types::CertificateDer<'static>>> {
    let certfile =
        fs::File::open(filename).map_err(|_| anyhow::anyhow!("cannot open certificate file"))?;
    let mut reader = BufReader::new(certfile);
    certs(&mut reader)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| anyhow::anyhow!("cannot read certificate file"))
}

fn load_private_key(
    filename: PathBuf,
) -> anyhow::Result<rustls::pki_types::PrivateKeyDer<'static>> {
    let rsa_keys = {
        let keyfile = fs::File::open(filename.clone())
            .map_err(|_| anyhow::anyhow!("cannot open private key file"))?;
        let mut reader = BufReader::new(keyfile);
        rsa_private_keys(&mut reader)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| anyhow::anyhow!("file contains invalid rsa private key"))?
    };

    let pkcs8_keys = {
        let keyfile = fs::File::open(filename)
            .map_err(|_| anyhow::anyhow!("cannot open private key file"))?;
        let mut reader = BufReader::new(keyfile);
        rustls_pemfile::pkcs8_private_keys(&mut reader).collect::<Result<Vec<_>, _>>().map_err(
            |_| {
                anyhow::anyhow!(
                    "file contains invalid pkcs8 private key (encrypted keys not supported)"
                )
            },
        )?
    };

    // prefer to load pkcs8 keys
    if !pkcs8_keys.is_empty() {
        Ok(rustls::pki_types::PrivateKeyDer::Pkcs8(pkcs8_keys[0].clone_key()))
    } else {
        assert!(!rsa_keys.is_empty());
        Ok(rustls::pki_types::PrivateKeyDer::Pkcs1(rsa_keys[0].clone_key()))
    }
}

fn load_ocsp(filename: &Option<PathBuf>) -> anyhow::Result<Vec<u8>> {
    let mut ret = Vec::new();

    if let Some(ref name) = filename {
        fs::File::open(name)
            .map_err(|_| anyhow::anyhow!("cannot open ocsp file"))?
            .read_to_end(&mut ret)
            .map_err(|_| anyhow::anyhow!("cannot read oscp file"))?;
    }

    Ok(ret)
}

/// Checks if the key is defined in the environment variables.
/// If not, inserts it with the given value.
pub fn export_to_env_if_not_present<T>(key: &str, value: T)
where
    T: AsRef<OsStr>,
{
    if let Err(VarError::NotPresent) = std::env::var(key) {
        std::env::set_var(key, value);
    }
}

/// Functions used to get default value for `Opt` fields, needs to be function because of serde's default attribute.

fn default_db_path() -> PathBuf {
    PathBuf::from(DEFAULT_DB_PATH)
}

pub fn default_http_addr() -> String {
    DEFAULT_HTTP_ADDR.to_string()
}

fn default_env() -> String {
    DEFAULT_ENV.to_string()
}

fn default_max_index_size() -> Byte {
    Byte::from_u64(INDEX_SIZE)
}

fn default_max_task_db_size() -> Byte {
    Byte::from_u64(TASK_DB_SIZE)
}

fn default_http_payload_size_limit() -> Byte {
    Byte::from_str(DEFAULT_HTTP_PAYLOAD_SIZE_LIMIT).unwrap()
}

fn default_limit_batched_tasks() -> usize {
    usize::MAX
}

fn default_limit_batched_tasks_total_size() -> u64 {
    u64::MAX
}

fn default_snapshot_dir() -> PathBuf {
    PathBuf::from(DEFAULT_SNAPSHOT_DIR)
}

fn default_snapshot_interval_sec() -> &'static str {
    DEFAULT_SNAPSHOT_INTERVAL_SEC_STR
}

fn default_dump_dir() -> PathBuf {
    PathBuf::from(DEFAULT_DUMP_DIR)
}

fn default_experimental_search_queue_size() -> usize {
    1000
}

fn default_drop_search_after() -> NonZeroUsize {
    NonZeroUsize::new(60).unwrap()
}

fn default_nb_searches_per_core() -> NonZeroUsize {
    NonZeroUsize::new(4).unwrap()
}

/// Indicates if a snapshot was scheduled, and if yes with which interval.
#[derive(Debug, Default, Copy, Clone, Deserialize, Serialize)]
pub enum ScheduleSnapshot {
    /// Scheduled snapshots are disabled.
    #[default]
    Disabled,
    /// Snapshots are scheduled at the specified interval, in seconds.
    Enabled(u64),
}

impl Display for ScheduleSnapshot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ScheduleSnapshot::Disabled => write!(f, ""),
            ScheduleSnapshot::Enabled(value) => write!(f, "{}", value),
        }
    }
}

impl FromStr for ScheduleSnapshot {
    type Err = ParseIntError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s {
            "" => ScheduleSnapshot::Disabled,
            s => ScheduleSnapshot::Enabled(s.parse()?),
        })
    }
}

fn parse_schedule_snapshot(s: &str) -> Result<ScheduleSnapshot, ParseIntError> {
    Ok(if s.is_empty() { ScheduleSnapshot::Disabled } else { ScheduleSnapshot::from_str(s)? })
}

fn schedule_snapshot_to_env(schedule_snapshot: ScheduleSnapshot) -> Option<String> {
    match schedule_snapshot {
        ScheduleSnapshot::Enabled(snapshot_delay) => Some(snapshot_delay.to_string()),
        _ => None,
    }
}

fn schedule_snapshot_deserialize<'de, D>(deserializer: D) -> Result<ScheduleSnapshot, D::Error>
where
    D: serde::Deserializer<'de>,
{
    struct BoolOrInt;

    impl<'de> serde::de::Visitor<'de> for BoolOrInt {
        type Value = ScheduleSnapshot;

        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
            formatter.write_str("integer or boolean")
        }

        fn visit_bool<E>(self, value: bool) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            Ok(if value {
                ScheduleSnapshot::Enabled(DEFAULT_SNAPSHOT_INTERVAL_SEC)
            } else {
                ScheduleSnapshot::Disabled
            })
        }

        fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            Ok(ScheduleSnapshot::Enabled(v as u64))
        }

        fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            Ok(ScheduleSnapshot::Enabled(v))
        }

        fn visit_none<E>(self) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            Ok(ScheduleSnapshot::Disabled)
        }

        fn visit_unit<E>(self) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            Ok(ScheduleSnapshot::Disabled)
        }
    }
    deserializer.deserialize_any(BoolOrInt)
}
