use std::env;
use std::io::{stderr, LineWriter, Write};
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use std::thread::available_parallelism;

use actix_web::http::KeepAlive;
use actix_web::web::Data;
use actix_web::HttpServer;
use index_scheduler::IndexScheduler;
use is_terminal::IsTerminal;
use meilisearch::analytics::Analytics;
use meilisearch::option::LogMode;
use meilisearch::search_queue::SearchQueue;
use meilisearch::{
    analytics, create_app, setup_meilisearch, LogRouteHandle, LogRouteType, LogStderrHandle,
    LogStderrType, Opt, SubscriberForSecondLayer,
};
use meilisearch_auth::{generate_master_key, AuthController, MASTER_KEY_MIN_SIZE};
use mimalloc::MiMalloc;
use termcolor::{Color, ColorChoice, ColorSpec, StandardStream, WriteColor};
use tracing::level_filters::LevelFilter;
use tracing_subscriber::layer::SubscriberExt as _;
use tracing_subscriber::Layer;

#[global_allocator]
static ALLOC: MiMalloc = MiMalloc;

fn default_log_route_layer() -> LogRouteType {
    None.with_filter(tracing_subscriber::filter::Targets::new().with_target("", LevelFilter::OFF))
}

fn default_log_stderr_layer(opt: &Opt) -> LogStderrType {
    let layer = tracing_subscriber::fmt::layer()
        .with_writer(|| LineWriter::new(std::io::stderr()))
        .with_span_events(tracing_subscriber::fmt::format::FmtSpan::CLOSE);

    let layer = match opt.experimental_logs_mode {
        LogMode::Human => Box::new(layer)
            as Box<dyn tracing_subscriber::Layer<SubscriberForSecondLayer> + Send + Sync>,
        LogMode::Json => Box::new(layer.json())
            as Box<dyn tracing_subscriber::Layer<SubscriberForSecondLayer> + Send + Sync>,
    };

    layer.with_filter(
        tracing_subscriber::filter::Targets::new()
            .with_target("", LevelFilter::from_str(&opt.log_level.to_string()).unwrap()),
    )
}

/// does all the setup before meilisearch is launched
fn setup(opt: &Opt) -> anyhow::Result<(LogRouteHandle, LogStderrHandle)> {
    let (route_layer, route_layer_handle) =
        tracing_subscriber::reload::Layer::new(default_log_route_layer());
    let route_layer: tracing_subscriber::reload::Layer<_, _> = route_layer;

    let (stderr_layer, stderr_layer_handle) =
        tracing_subscriber::reload::Layer::new(default_log_stderr_layer(opt));
    let route_layer: tracing_subscriber::reload::Layer<_, _> = route_layer;

    let subscriber = tracing_subscriber::registry().with(route_layer).with(stderr_layer);

    // set the subscriber as the default for the application
    tracing::subscriber::set_global_default(subscriber).unwrap();

    Ok((route_layer_handle, stderr_layer_handle))
}

fn on_panic(info: &std::panic::PanicInfo) {
    let info = info.to_string().replace('\n', " ");
    tracing::error!(%info);
}

#[actix_web::main]
async fn main() -> anyhow::Result<()> {
    try_main().await.inspect_err(|error| {
        tracing::error!(%error);
        let mut current = error.source();
        let mut depth = 0;
        while let Some(source) = current {
            tracing::info!(%source, depth, "Error caused by");
            current = source.source();
            depth += 1;
        }
    })
}

async fn try_main() -> anyhow::Result<()> {
    let (opt, config_read_from) = Opt::try_build()?;

    std::panic::set_hook(Box::new(on_panic));

    anyhow::ensure!(
        !(cfg!(windows) && opt.experimental_reduce_indexing_memory_usage),
        "The `experimental-reduce-indexing-memory-usage` flag is not supported on Windows"
    );

    let log_handle = setup(&opt)?;

    match (opt.env.as_ref(), &opt.master_key) {
        ("production", Some(master_key)) if master_key.len() < MASTER_KEY_MIN_SIZE => {
            anyhow::bail!(
                "The master key must be at least {MASTER_KEY_MIN_SIZE} bytes in a production environment. The provided key is only {} bytes.

{}",
                master_key.len(),
                generated_master_key_message(),
            )
        }
        ("production", None) => {
            anyhow::bail!(
                "You must provide a master key to secure your instance in a production environment. It can be specified via the MEILI_MASTER_KEY environment variable or the --master-key launch option.

{}",
                generated_master_key_message()
            )
        }
        // No error; continue
        _ => (),
    }

    let (index_scheduler, auth_controller) = setup_meilisearch(&opt)?;

    #[cfg(all(not(debug_assertions), feature = "analytics"))]
    let analytics = if !opt.no_analytics {
        analytics::SegmentAnalytics::new(&opt, index_scheduler.clone(), auth_controller.clone())
            .await
    } else {
        analytics::MockAnalytics::new(&opt)
    };
    #[cfg(any(debug_assertions, not(feature = "analytics")))]
    let analytics = analytics::MockAnalytics::new(&opt);

    print_launch_resume(&opt, analytics.clone(), config_read_from);

    run_http(index_scheduler, auth_controller, opt, log_handle, analytics).await?;

    Ok(())
}

async fn run_http(
    index_scheduler: Arc<IndexScheduler>,
    auth_controller: Arc<AuthController>,
    opt: Opt,
    logs: (LogRouteHandle, LogStderrHandle),
    analytics: Arc<dyn Analytics>,
) -> anyhow::Result<()> {
    let enable_dashboard = &opt.env == "development";
    let opt_clone = opt.clone();
    let index_scheduler = Data::from(index_scheduler);
    let auth_controller = Data::from(auth_controller);
    let search_queue = SearchQueue::new(
        opt.experimental_search_queue_size,
        available_parallelism().unwrap_or(NonZeroUsize::new(2).unwrap()),
    );
    let search_queue = Data::new(search_queue);

    let http_server = HttpServer::new(move || {
        create_app(
            index_scheduler.clone(),
            auth_controller.clone(),
            search_queue.clone(),
            opt.clone(),
            logs.clone(),
            analytics.clone(),
            enable_dashboard,
        )
    })
    // Disable signals allows the server to terminate immediately when a user enter CTRL-C
    .disable_signals()
    .keep_alive(KeepAlive::Os);

    if let Some(config) = opt_clone.get_ssl_config()? {
        http_server.bind_rustls_0_23(opt_clone.http_addr, config)?.run().await?;
    } else {
        if opt_clone.http_addr.starts_with("/") {
            http_server.bind_uds(&opt_clone.http_addr)?.run().await?
        } else {
            http_server.bind(&opt_clone.http_addr)?.run().await?
        }
    }
    Ok(())
}

pub fn print_launch_resume(
    opt: &Opt,
    analytics: Arc<dyn Analytics>,
    config_read_from: Option<PathBuf>,
) {
    let build_info = build_info::BuildInfo::from_build();

    let protocol =
        if opt.ssl_cert_path.is_some() && opt.ssl_key_path.is_some() { 
            "https"
        } else if opt.http_addr.starts_with("/") {
            "unix"
        } else {
            "http"
        };
    let ascii_name = r#"
888b     d888          d8b 888 d8b                                            888
8888b   d8888          Y8P 888 Y8P                                            888
88888b.d88888              888                                                888
888Y88888P888  .d88b.  888 888 888 .d8888b   .d88b.   8888b.  888d888 .d8888b 88888b.
888 Y888P 888 d8P  Y8b 888 888 888 88K      d8P  Y8b     "88b 888P"  d88P"    888 "88b
888  Y8P  888 88888888 888 888 888 "Y8888b. 88888888 .d888888 888    888      888  888
888   "   888 Y8b.     888 888 888      X88 Y8b.     888  888 888    Y88b.    888  888
888       888  "Y8888  888 888 888  88888P'  "Y8888  "Y888888 888     "Y8888P 888  888
"#;

    eprintln!("{}", ascii_name);

    eprintln!(
        "Config file path:\t{:?}",
        config_read_from
            .map(|config_file_path| config_file_path.display().to_string())
            .unwrap_or_else(|| "none".to_string())
    );
    eprintln!("Database path:\t\t{:?}", opt.db_path);
    eprintln!("Server listening on:\t\"{}://{}\"", protocol, opt.http_addr);
    eprintln!("Environment:\t\t{:?}", opt.env);
    eprintln!("Commit SHA:\t\t{:?}", build_info.commit_sha1.unwrap_or("unknown"));
    eprintln!(
        "Commit date:\t\t{:?}",
        build_info
            .commit_timestamp
            .and_then(|commit_timestamp| commit_timestamp
                .format(&time::format_description::well_known::Rfc3339)
                .ok())
            .unwrap_or("unknown".into())
    );
    eprintln!("Package version:\t{:?}", env!("CARGO_PKG_VERSION").to_string());
    if let Some(prototype) = build_info.describe.and_then(|describe| describe.as_prototype()) {
        eprintln!("Prototype:\t\t{:?}", prototype);
    }

    #[cfg(all(not(debug_assertions), feature = "analytics"))]
    {
        if !opt.no_analytics {
            eprintln!(
                "
Thank you for using Meilisearch!

\nWe collect anonymized analytics to improve our product and your experience. To learn more, including how to turn off analytics, visit our dedicated documentation page: https://www.meilisearch.com/docs/learn/what_is_meilisearch/telemetry

Anonymous telemetry:\t\"Enabled\""
            );
        } else {
            eprintln!("Anonymous telemetry:\t\"Disabled\"");
        }
    }

    if let Some(instance_uid) = analytics.instance_uid() {
        eprintln!("Instance UID:\t\t\"{}\"", instance_uid);
    }

    eprintln!();

    match (opt.env.as_ref(), &opt.master_key) {
        ("production", Some(_)) => {
            eprintln!("A master key has been set. Requests to Meilisearch won't be authorized unless you provide an authentication key.");
        }
        ("development", Some(master_key)) => {
            eprintln!("A master key has been set. Requests to Meilisearch won't be authorized unless you provide an authentication key.");

            if master_key.len() < MASTER_KEY_MIN_SIZE {
                print_master_key_too_short_warning()
            }
        }
        ("development", None) => print_missing_master_key_warning(),
        // unreachable because Opt::try_build above would have failed already if any other value had been produced
        _ => unreachable!(),
    }

    eprintln!();
    eprintln!("Check out Meilisearch Cloud!\thttps://www.meilisearch.com/cloud?utm_campaign=oss&utm_source=engine&utm_medium=cli");
    eprintln!("Documentation:\t\t\thttps://www.meilisearch.com/docs");
    eprintln!("Source code:\t\t\thttps://github.com/meilisearch/meilisearch");
    eprintln!("Discord:\t\t\thttps://discord.meilisearch.com");
    eprintln!();
}

const WARNING_BG_COLOR: Option<Color> = Some(Color::Ansi256(178));
const WARNING_FG_COLOR: Option<Color> = Some(Color::Ansi256(0));

fn print_master_key_too_short_warning() {
    let choice = if stderr().is_terminal() { ColorChoice::Auto } else { ColorChoice::Never };
    let mut stderr = StandardStream::stderr(choice);
    stderr
        .set_color(
            ColorSpec::new().set_bg(WARNING_BG_COLOR).set_fg(WARNING_FG_COLOR).set_bold(true),
        )
        .unwrap();
    writeln!(stderr, "\n").unwrap();
    writeln!(
        stderr,
        " Meilisearch started with a master key considered unsafe for use in a production environment.

 A master key of at least {MASTER_KEY_MIN_SIZE} bytes will be required when switching to a production environment."
    )
    .unwrap();
    stderr.reset().unwrap();
    writeln!(stderr).unwrap();

    eprintln!("\n{}", generated_master_key_message());
    eprintln!(
        "\nRestart Meilisearch with the argument above to use this new and secure master key."
    )
}

fn print_missing_master_key_warning() {
    let choice = if stderr().is_terminal() { ColorChoice::Auto } else { ColorChoice::Never };
    let mut stderr = StandardStream::stderr(choice);
    stderr
        .set_color(
            ColorSpec::new().set_bg(WARNING_BG_COLOR).set_fg(WARNING_FG_COLOR).set_bold(true),
        )
        .unwrap();
    writeln!(stderr, "\n").unwrap();
    writeln!(
    stderr,
    " No master key was found. The server will accept unidentified requests.

 A master key of at least {MASTER_KEY_MIN_SIZE} bytes will be required when switching to a production environment."
)
.unwrap();
    stderr.reset().unwrap();
    writeln!(stderr).unwrap();

    eprintln!("\n{}", generated_master_key_message());
    eprintln!(
        "\nRestart Meilisearch with the argument above to use this new and secure master key."
    )
}

fn generated_master_key_message() -> String {
    format!(
        "We generated a new secure master key for you (you can safely use this token):

>> --master-key {} <<",
        generate_master_key()
    )
}
