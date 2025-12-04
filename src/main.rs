mod clients;
mod config;
mod extract_file;
mod extractor;
mod service;

use std::{
    env,
    ffi::OsString,
    path::{Path, PathBuf},
    sync::{Arc, OnceLock},
    time::{Duration, Instant, SystemTime},
};

use crate::clients::Client;
use crate::config::Config;
use crate::extract_file::ExtractFile;
use crate::extractor::Extractor;

use anyhow::{Context, Result};
use clap::{ArgAction, Parser, crate_version};
use russh::client::KeyboardInteractiveAuthResponse;
use russh_sftp::client::{SftpSession, fs::DirEntry};
use time::{UtcOffset, format_description::BorrowedFormatItem, macros::format_description};
#[cfg(not(unix))]
use tokio::signal;
use tokio::sync::mpsc::{Receiver, channel};
use tokio::{
    runtime::{Builder, Runtime},
    sync::mpsc::Sender,
};
use tracing::{Level, debug, error, trace, warn};
use tracing::{info, level_filters::LevelFilter};
use tracing_subscriber::{
    Layer,
    fmt::{self},
};
use tracing_subscriber::{fmt::time::OffsetTime, layer::SubscriberExt};
use windows_service::{define_windows_service, service_dispatcher};

#[derive(Parser, Debug)]
#[command(next_line_help = true)]
pub struct Cli {
    /// Run in CLI mode
    #[arg(long, action = ArgAction::SetTrue)]
    cli: bool,

    /// Install as Windows service
    #[arg(long, action = ArgAction::SetTrue)]
    install: bool,
    /// Uninstall Windows service
    #[arg(long, action = ArgAction::SetTrue)]
    uninstall: bool,
    /// Reinstall Windows service
    #[arg(long, action = ArgAction::SetTrue)]
    reinstall: bool,

    /// Only turn on for russh debug. It produces a *lot* of log messages.
    #[arg(long, action = ArgAction::SetTrue)]
    debug_russh: bool,

    // Config overlay arguments
    /// Destination for downloaded files. eg F:/arcuftp/epiodbc
    #[arg(long)]
    destination: Option<PathBuf>,
    /// Logging level - see "https://docs.rs/tracing-core/latest/src/tracing_core/metadata.rs.html#553" for how this is parsed.  Default: warn
    #[arg(short, long)]
    log_level: Option<Level>,
    /// Number of download queues.
    #[arg(long, short)]
    queue_count: Option<usize>,
    /// Number of concurrent threads. You probably want --queue_count.
    #[arg(long)]
    threads: Option<usize>,
    /// Run in debug mode. Does not wait for remote ARCU Extract jobs and only runs through once.
    #[arg(long, action = ArgAction::SetTrue)]
    debug: bool,
    /// Run and download, but don't move to `destination` directory. Useful for troubleshooting.
    #[arg(long, action = ArgAction::SetTrue)]
    dry: bool,
    /// Override source ssh/sftp host
    #[arg(long)]
    hostname: Option<String>,
    /// Override source ssh/sftp port. Default: 22
    #[arg(long)]
    port: Option<u16>,
    /// Override source ssh/sftp authentication username
    #[arg(long)]
    username: Option<String>,
    /// Override source ssh/sftp authentication password
    #[arg(long)]
    password: Option<String>,

    // Todo: Remove once experimental mode is primary
    /// Legacy only: Run in single-threaded mode
    #[arg(long, action = ArgAction::SetTrue)]
    sync: bool,
    /// Legacy only: Skip checksum validation
    #[arg(long, action = ArgAction::SetTrue)]
    no_check: bool,
    /// Run in experimental mode.
    #[arg(long, action = ArgAction::SetTrue)]
    experimental: bool,
}

const DAY: u64 = 86400;
const HOUR: u64 = 3600;
const HALF_HOUR: u64 = 1800;

pub static TIMER: OnceLock<OffsetTime<&[BorrowedFormatItem<'_>]>> = OnceLock::new();
pub static START: OnceLock<Instant> = OnceLock::new();

define_windows_service!(ffi_service_main, service_main);
pub fn main() -> anyhow::Result<()> {
    let start_time = START.get_or_init(Instant::now);

    // let cli = crate::setup().expect("Could not set up application");
    let cli = Cli::parse();
    config::set_current_dir();

    let config = Config::new().with_cli(&cli);

    let timer = TIMER.get_or_init(|| {
        let timer_format =
            format_description!("[year]-[month]-[day] [hour]:[minute]:[second] [period]");
        OffsetTime::new(
            UtcOffset::current_local_offset().expect("Could not get local timezone"),
            timer_format,
        )
    });

    // Only turn on for russh debug. It produces a *lot* of log messages.
    if cli.debug_russh {
        use tracing_log::LogTracer;
        LogTracer::init()?;
    }

    let appender = tracing_appender::rolling::daily(r#".\logs"#, "downloader.log");
    let (non_blocking_file, _guard) = tracing_appender::non_blocking(appender);

    let file_log = fmt::layer()
        .with_timer(timer)
        .with_level(true)
        .with_writer(non_blocking_file)
        .with_ansi(false)
        .with_filter(LevelFilter::from_level(config.log_level));
    let logger = tracing_subscriber::registry();
    let logger = logger.with(file_log);

    let console_log = if cli.cli {
        Some(
            fmt::layer()
                .with_timer(TIMER.get().unwrap())
                .with_ansi(false)
                .with_filter(LevelFilter::from_level(config.log_level)),
        )
    } else {
        None
    };

    let logging = logger.with(console_log);
    tracing::subscriber::set_global_default(logging).expect("Unable to set up logging");

    trace!("Running extract downloader v{}", crate_version!());
    trace!(
        "Startup complete. Running in {}",
        env::current_dir().unwrap_or(PathBuf::from("\\?")).display()
    );

    #[cfg(debug_assertions)]
    {
        _ = std::fs::remove_dir_all(r#".\extracts\"#);
        // _ = std::fs::remove_dir_all(&_config.destination_path);
    }

    if cli.install {
        return service::install();
    }
    if cli.uninstall {
        return service::uninstall();
    }
    if cli.reinstall {
        return service::reinstall();
    }

    if cli.cli {
        let runtime = get_tokio_runtime(Some(config));
        trace!("Running as cli: {runtime:#?}");

        let (shutdown_tx, shutdown_rx) = channel(8);

        runtime.spawn(shutdown_signal(shutdown_tx));

        runtime.spawn(async {
            loop {
                let runtime = tokio::runtime::Handle::current().metrics();
                trace!(
                    "runtime watch: tasks alive: {} queue_depth: {}",
                    runtime.num_alive_tasks(),
                    runtime.global_queue_depth()
                );
                // trace!("{}", runtime.with_current_subscriber)
                tokio::time::sleep(Duration::from_secs(30)).await;
            }
        });

        let ret = runtime.block_on(run(async || {}, shutdown_rx));

        let stop_time = Instant::now();
        trace!(
            "Ran for {}",
            stop_time.duration_since(*start_time).as_secs_f64()
        );

        return ret;
    }

    if let Err(e) = service_dispatcher::start(service::SERVICE_NAME, ffi_service_main) {
        error!("Error running service: {e}");
    };
    Ok(())
}

fn service_main(_arguments: Vec<OsString>) {
    let runtime = get_tokio_runtime(None);
    trace!("Running as service");

    if let Err(e) = runtime.block_on(service::run_service()) {
        error!("Service failed: {:?}", e);
    }
}

fn get_tokio_runtime(config: Option<Config>) -> Runtime {
    let config = if let Some(config) = config {
        config
    } else {
        let cli = Cli::parse();
        Config::new().with_cli(&cli)
    };

    let mut runtime = Builder::new_multi_thread();
    if let Some(threads) = config.threads {
        runtime.worker_threads(threads);
        runtime
    } else {
        runtime
    }
    .enable_io()
    .enable_time()
    .build()
    .unwrap()
}

async fn shutdown_signal(shutdown_channel: Sender<()>) {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(not(unix))]
    let mut terminate =
        signal::windows::ctrl_close().expect("failed to install Ctrl+Close handler");

    tokio::select! {
        _ = ctrl_c => { shutdown_channel.send(()).await.expect("Could not send shutdown message") },
        _ = terminate.recv() => { shutdown_channel.send(()).await.expect("Could not send shutdown message") },
    }
}

pub async fn run<F>(callback: F, mut shutdown_channel: Receiver<()>) -> anyhow::Result<()>
where
    F: AsyncFnOnce() -> (),
{
    info!("Running main loop...");
    let cli = Cli::parse();
    let config = Config::new().with_cli(&cli);

    if config.experimental {
        let mut extractor = Extractor::new(config.clone());
        loop {
            tokio::select! {
                _ = shutdown_channel.recv() => {
                    debug!("Received shutdown signal.");
                    break;
                }
                extractor_result = extractor.watch() => {
                    match extractor_result {
                        Ok(()) => info!("Extractor succeeded"),
                        Err(e) => {
                            error!("Extractor failed: {e}"); //?;
                            _ = std::fs::remove_dir_all(r#".\extracts\"#);
                            continue;
                        }
                    }
                }
            }
            extractor.close().await;
            if config.debug {
                debug!(
                    "Exiting due to running in debug. If this is not intended, compile with 'cargo build --release'"
                );
                break;
            }
        }
    } else {
        loop {
            let mut done = false;
            match process(&config, &cli).await {
                Ok(is_done) => done = is_done,
                Err(e) => error!("Error processing {e}"),
            }

            let mut sleep_time = Duration::from_secs(60);

            if config.debug {
                debug!(
                    "Exiting due to running in debug. If this is not intended, compile with 'cargo build --release'"
                );
                break;
            }
            if done {
                debug!(
                    "Full completion of processing detected. Waiting 90 minutes before checking for new files."
                );
                sleep_time = Duration::from_secs(HOUR + (HOUR / 2));
            }

            tokio::select! {
                _ = shutdown_channel.recv() => {
                    debug!("Received shutdown signal.");
                    break;
                }
                _ = tokio::time::sleep(sleep_time) => ()

            }
        }

        debug!("Main run loop ended, running callback");
        callback().await;
    }

    let stop_time = Instant::now();
    let start_time = START.get().unwrap();
    trace!(
        "Ran for {}",
        stop_time.duration_since(*start_time).as_secs_f64()
    );
    Ok(())
}

async fn process(config: &Config, cli: &Cli) -> anyhow::Result<bool> {
    info!("Processing...");
    let session = Arc::from(connect(config).await?);
    // session.set_timeout(60).await;
    let mut last_file = None;

    let mut files_found: Vec<ExtractFile> = Vec::new();
    for sym in &config.sym_names() {
        let dir = format!("/SYM/SYM{sym}/SQLEXTRACT");
        let mut sym_files = get_files(&session, &dir, |_| true).await?;

        let dir = format!("/SYM/SYM{sym}/LETTERSPECS");

        sym_files.append(
            &mut get_files(&session, &dir, |f| {
                !f.file_name().contains("DataSupp.")
                    && (f.file_name().starts_with("EXTRACT.")
                        || f.file_name().starts_with("FMT.")
                        || f.file_name().starts_with("MACRO.")
                        || f.file_name() == "CD_Trial_Bal_for_"
                        || f.file_name() == "Episys_DataSupp_Statistics.txt"
                        || f.file_name() == "Episys_Database__Extract_Stats"
                        || f.file_name() == "EXTRACT_LASTFILE.txt")
            })
            .await?,
        );

        files_found.append(
            &mut (sym_files
                .iter()
                .map(|f| {
                    ExtractFile::new(
                        *sym,
                        f.to_string(),
                        f[f.rfind('/').unwrap() + 1..].to_string(),
                        String::new(),
                    )
                })
                .collect()),
        );
    }

    // Todo: implement a better way to wait for job completion
    if !files_found.is_empty() && !config.debug {
        debug!("Waiting 1 hour for batch job to complete.");
        tokio::time::sleep(Duration::from_secs(HOUR)).await; //
    }

    trace!("{files_found:#?}");

    // Todo: adjust this to a task queue over the number of available workers minus one.
    // The old method of downloading every file synchonously was prone to system io errors.

    let results = if cli.sync {
        trace!("Running in sync mode");
        let results = Vec::new();
        for file in &files_found {
            let mut results = Vec::new();
            results.push(file.clone().download(session.clone()).await);
        }
        results
    } else {
        // Download files asyncronously from remote.
        // Retries the downloads once.
        trace!("Running in async mode");
        let mut set = tokio::task::JoinSet::new();
        for file in &files_found {
            set.spawn(file.clone().download(session.clone()));
        }
        set.join_all().await
    };

    for res in results {
        let res = match res {
            Ok(r) => r,
            Err(e) => {
                error!("Error downloading file {e}");
                continue;
            }
        };
        for f in files_found.iter_mut().filter(|f| f.file_name == res.0) {
            trace!("Saving success results for '{}'", f.file_name);
            f.local_path = res.1.clone();
            f.is_remote_empty = res.2;
            f.download_error = false;
        }
    }
    // for file in files_found
    //     .iter_mut()
    //     .filter(|f| f.local_path.is_empty() || f.download_error)
    // {
    //     warn!("{0} Failed to download. Retrying...", file.file_name);
    //     let result = file.clone().download(session.clone()).await?;
    //     file.local_path = result.1;
    // }
    let while_limit = 50;
    let mut while_count = 0;
    while files_found.iter().any(|f| f.download_error) {
        while_count += 1;
        if while_count > while_limit {
            break;
        }
        for file in files_found
            .iter_mut()
            .filter(|f| f.local_path.is_empty() || f.download_error)
        {
            warn!("{0} Failed to download. Retrying...", file.file_name);
            file.is_remote_empty = false;
            let result = match file.clone().download(session.clone()).await {
                Ok(v) => {
                    file.download_error = false;
                    v
                }
                Err(e) => {
                    error!("Download error! {e}");
                    file.download_error = true;
                    continue;
                }
            };
            file.local_path = result.1;
        }
    }
    session.close().await?;

    #[allow(unused)]
    let mut datasupp_processed = false;

    for (i, file) in files_found.iter().enumerate() {
        match &*file.file_name {
            "EXTRACT_LASTFILE.txt" => last_file = Some(i),
            "Episys_DataSupp_Statistics.txt" => {
                continue;

                #[allow(unreachable_code)]
                // todo: handle datasupp details later, but the
                // error this reports on ARCU is non-critical; not a priority.
                let datasupp_files = files_found.clone();
                let datasupp_files = datasupp_files
                    .iter()
                    .filter(|f| f.file_name == file.file_name)
                    .collect::<Vec<&ExtractFile>>();
                if datasupp_files.len() > 1 {
                    let _ = combine_datasupp(datasupp_files, &mut datasupp_processed);
                }
            }
            _ => {}
        }
    }

    if let Some(last_file) = last_file {
        debug!("Last file found. Moving other files.");
        if !std::fs::exists(&config.destination).context("Attempting to verify destination path")? {
            std::fs::create_dir(&config.destination).context("")?;
        }
        let mut moved = Vec::new();
        for file in files_found
            .iter()
            .filter(|f| f.file_name != "EXTRACT_LASTFILE.txt")
        {
            if !moved.contains(&file.local_path) {
                debug!("Moving {}", file.local_path);
                moved.push(file.local_path.clone());
                let mut file_destination = config.destination.clone();

                // These files hit length limits for converting to letter files
                // so, we'll rename to the expected format here
                if file.file_name == *"Episys_Database__Extract_Stats" {
                    file_destination.push("Episys_Database__Extract_Statistics.txt");
                } else if file.file_name.starts_with("CD_Trial_Bal_for_") {
                    // IE CD_Trial_Bal_for_01_03_24
                    let date = &file.file_name.trim_start_matches("CD_Trial_Bal_for_");
                    file_destination
                        .push("Close_Day_Trial_Balance_for_".to_owned() + date + ".txt");
                } else {
                    file_destination.push(&file.file_name);
                }

                std::fs::rename(&file.local_path, &file_destination).context(format!(
                    "Failed to move file '{}' to '{}'",
                    file.local_path,
                    &file_destination.display()
                ))?
            }
        }

        debug!("Moving last file.");
        let last_file = files_found.get(last_file).unwrap();
        let mut file_destination = config.destination.clone();
        file_destination.push(&last_file.file_name);
        std::fs::rename(&last_file.local_path, file_destination)?;

        debug!("Checking for leftover files");
        for file in files_found {
            if Path::exists(&PathBuf::from(&file.local_path)) {
                warn!("Leftover file found at {}", file.local_path);
                if file.sym != config.syms.last().unwrap().number {
                    let _ = std::fs::remove_file(file.local_path);
                } else {
                    std::fs::rename(file.local_path, format!(".\\extracts\\{}", file.file_name))?
                }
            }
        }

        return Ok(true);
    }

    Ok(false)
}

async fn connect(config: &Config) -> anyhow::Result<SftpSession> {
    debug!("Connecting to host '{}'", config.sftp.hostname);
    let russh_config = russh::client::Config::default();

    let sh = Client {};
    let mut session = russh::client::connect(
        Arc::new(russh_config),
        (&*config.sftp.hostname, config.sftp.port),
        sh,
    )
    .await?;

    let mut interactive_response = session
        .authenticate_keyboard_interactive_start(config.sftp.username.clone(), None)
        .await?;

    loop {
        match interactive_response {
            KeyboardInteractiveAuthResponse::Success => {
                trace!("Keyboard authentication successful");
                break;
            }
            KeyboardInteractiveAuthResponse::Failure { .. } => {
                error!("Keyboard authentication failure");
                return Err(russh::Error::NotAuthenticated.into());
            }
            KeyboardInteractiveAuthResponse::InfoRequest {
                ref name,
                ref instructions,
                ref prompts,
            } => {
                trace!("Received {name}, {instructions}. Prompts {:?} ", prompts);

                let resp = prompts
                    .iter()
                    .filter(|p| p.prompt.to_lowercase().contains("password"))
                    .map(|_| config.sftp.password.clone())
                    .collect::<Vec<String>>();
                interactive_response = session
                    .authenticate_keyboard_interactive_respond(resp)
                    .await?;
            }
        }
    }

    let channel = session.channel_open_session().await?;
    channel.request_subsystem(true, "sftp").await?;

    Ok(SftpSession::new(channel.into_stream()).await?)
}

/// Finds and pulls files from a given directory which were modified in the last day.
/// Returns a list of file paths.
/// get_files will traverse subdirectories, but only one level deep.
/// the 'filter' parameter is an additional iter::filter predicate for the files in the directory.
async fn get_files<P>(session: &SftpSession, dir: &String, filter: P) -> anyhow::Result<Vec<String>>
where
    P: FnMut(&DirEntry) -> bool,
{
    let mut result = Vec::new();

    // If running debug, we can
    // grab the last day of files. If not, just the last hour should do it.
    let file_age = if cfg!(debug_assertions) {
        DAY
    } else {
        HOUR + HALF_HOUR
    };

    for entry in session.read_dir(dir).await?.filter(filter).filter(|f| {
        f.metadata().modified().unwrap()
            > SystemTime::now()
                .checked_sub(Duration::from_secs(file_age))
                .unwrap()
    }) {
        if entry.file_type().is_dir() {
            debug!("dir: {dir} {:?}", entry.file_name());
            let dir = format!("{dir}/{}", entry.file_name());
            for file in session.read_dir(&dir).await? {
                if !file.file_type().is_dir() {
                    debug!("file in dir: {dir} {:?}", file.file_name());

                    result.push(format!("{dir}/{}", file.file_name()));
                }
            }
        } else {
            debug!("file in dir: {dir} {:?}", entry.file_name());
            result.push(format!("{dir}/{}", entry.file_name()));
        }
    }

    Ok(result)
}

#[allow(dead_code, unused)] // incomplete function
fn combine_datasupp(files: Vec<&ExtractFile>, status: &mut bool) -> Result<ExtractFile> {
    debug!("Combining datasupp statistics files");
    if *status || files.len() == 1 {
        return Ok((*files.last().unwrap()).clone());
    }

    struct Total {
        tot: u64,
        full: bool,
    }

    let mut account_total = Total {
        tot: 0,
        full: false,
    };
    let mut loan_total = Total {
        tot: 0,
        full: false,
    };
    let mut share_total = Total {
        tot: 0,
        full: false,
    };

    // 358 file
    // A:000000017911
    // A:000000017911
    // A:000000017911
    // A:000000017911
    // L:000000013916
    // L:000000013915
    // L:000000013915
    // L:000000013915
    // 658 file
    // AccountDataSupp:000000071644
    // LoanDataSupp:000000055661
    // ShareDataSupp:000000133527

    for file in files {
        let content = std::fs::read_to_string(&file.local_path)?;
        let content: Vec<&str> = content.split("\r\n").collect();

        for line in content {
            let Some((label, content)) = line.split_once(':') else {
                continue;
            };

            let (mut label, mut count) = ("", "");
            let split = line.split_once(':');
            if let Some(split) = split {
                (label, count) = split;
            }

            debug!("{label}, {count}")
        }
    }

    Ok(ExtractFile {
        sym: 658,
        sym_path: String::new(),
        file_name: String::new(),
        local_path: String::new(),
        is_remote_empty: false,
        download_error: false,
    })
}
