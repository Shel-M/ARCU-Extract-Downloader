mod config;
mod extract_file;

use std::{
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, SystemTime},
};

use crate::config::Config;
use crate::extract_file::ExtractFile;

use anyhow::{anyhow, Context, Result};
use clap::{ArgAction, Parser};
use runas::Command;
use russh::{
    client::{self, KeyboardInteractiveAuthResponse},
    keys::PublicKey,
    ChannelId,
};
use russh_sftp::client::{fs::DirEntry, SftpSession};
use time::{macros::format_description, UtcOffset};
use tracing::{debug, error, trace, warn, Level};
use tracing::{info, level_filters::LevelFilter};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::{fmt, Layer};

#[derive(Parser, Debug)]
struct Cli {
    #[arg(long, action = ArgAction::SetTrue)]
    stdout: bool,

    #[arg(short, long, default_value_t = Level::WARN)]
    log: Level,

    #[arg(long, default_value_t = String::new())]
    host: String,

    #[arg(long, action = ArgAction::SetTrue)]
    service: bool,

    #[arg(long, action = ArgAction::SetTrue)]
    install: bool,
    #[arg(long, action = ArgAction::SetTrue)]
    uninstall: bool,
}

const DAY: u64 = 86400;
const HOUR: u64 = 3600;

#[tokio::main]
pub async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    let timer_format =
        format_description!("[year]-[month]-[day] [hour]:[minute]:[second] [period]");
    let timer = fmt::time::OffsetTime::new(UtcOffset::current_local_offset()?, timer_format);
    #[allow(unused)]
    let appender = tracing_appender::rolling::daily("./logs", "log_test.log");
    let (non_blocking_file, _guard) = tracing_appender::non_blocking(appender);

    let file_log = tracing_subscriber::fmt::layer()
        .with_timer(timer.clone())
        .with_level(true)
        .with_writer(non_blocking_file)
        .with_ansi(false)
        .with_filter(LevelFilter::from_level(cli.log));

    let console_log = if cli.stdout {
        Some(
            tracing_subscriber::fmt::layer()
                .with_timer(timer)
                .with_ansi(false)
                .with_filter(LevelFilter::from_level(cli.log)),
        )
    } else {
        None
    };

    let logging = tracing_subscriber::registry()
        .with(file_log)
        .with(console_log);
    tracing::subscriber::set_global_default(logging).expect("Unable to set up logging");
    trace!("Startup complete");

    trace!("Collecting configuration");

    if cli.install {
        let bin_path = std::env::current_exe()
            .context("Couldn't get executable path.")?
            .as_path()
            .canonicalize()
            .context("Couldn't get executable path.")?;
        // let bin_path = bin_path.to_str();
        // if bin_path.is_none() {
        //     return Err(anyhow!("Couldn't get executable path."));
        // }
        // let bin_path = bin_path.unwrap();
        // println!("{}", &(&bin_path)[4..]);
        // return Ok(());
        // let bin_path = &bin_path[4..];

        let status = Command::new("sc.exe")
            .arg("create")
            .arg("ARCU PCCU Extract Downloader")
            .arg("binPath=".to_owned())
            .arg(bin_path)
            .status()
            .context("Error installing service")?;
        if !status.success() {
            return Err(anyhow!(
                "Could not install service: {}",
                status.code().unwrap_or(-99)
            ));
        }
        return Ok(());
    }
    if cli.uninstall {
        let status = Command::new("sc.exe")
            .arg("delete")
            .arg("ARCU PCCU Extract Downloader")
            .status()
            .context("Error uninstalling service")?;
        if !status.success() {
            return Err(anyhow!(
                "Could not uninstall service: {}",
                status.code().unwrap_or(-99)
            ));
        }
        return Ok(());
    }

    if cli.service {
        windows_services::Service::new().can_stop().run(|command| {
            info!("Windows Service Command: {command:#?}");
            tokio::task::spawn_blocking(async || run().await);
        })
    } else {
        run().await
    }
}

async fn run() -> anyhow::Result<()> {
    let (config, cli) = (Config::new(), Cli::parse());
    loop {
        let mut done = false;
        match process(&config, &cli).await {
            Ok(is_done) => done = is_done,
            Err(e) => error!("Error processing {e}"),
        }

        if cfg!(debug_assertions) {
            debug!("Exiting due to running in debug. If this is not intended, compile with 'cargo build --release'");
            break Ok(());
        }
        if done {
            debug!(
                "Full completion of processing detected. Waiting one hour before checking for new files."
            );
            tokio::time::sleep(Duration::from_secs(HOUR + 1800)).await;
        } else {
            tokio::time::sleep(Duration::from_secs(60)).await;
        }
    }
}

async fn process(config: &Config, cli: &Cli) -> anyhow::Result<bool> {
    let session = Arc::from(connect(config, &cli.host).await?);
    let mut last_file = None;

    let mut files_found: Vec<ExtractFile> = Vec::new();
    for sym in &config.syms {
        let dir = format!("/SYM/SYM{sym}/SQLEXTRACT");
        let mut sym_files = get_files(&session, &dir, |_| true).await?;

        let dir = format!("/SYM/SYM{sym}/LETTERSPECS");

        sym_files.append(
            &mut get_files(&session, &dir, |f| {
                !f.file_name().contains("DataSupp.")
                    && (f.file_name().starts_with("EXTRACT.")
                        || f.file_name().starts_with("FMT.")
                        || f.file_name().starts_with("MACRO.")
                        || f.file_name() == ("CD_Trial_Bal_for_")
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
    // if !files_found.is_empty() {
    //     debug!("Waiting 1 hour for batch job to complete.");
    //     tokio::time::sleep(Duration::from_secs(HOUR)).await; //
    // }

    trace!("{files_found:#?}");
    // Download files asyncronously from remote.
    // Retries the downloads once.
    //let mut set = JoinSet::new();
    let mut results = Vec::new();
    for file in &files_found {
        results.push(file.clone().download(session.clone()).await);
    }
    //let results = set.join_all().await;

    for res in results {
        let res = match res {
            Ok(r) => r,
            Err(e) => {
                error!("Error downloading file {e}");
                continue;
            }
        };
        for f in files_found.iter_mut().filter(|f| f.file_name == res.0) {
            f.local_path = res.1.clone();
            f.is_empty = res.2;
        }
    }
    for file in files_found.iter_mut().filter(|f| f.local_path.is_empty()) {
        warn!("{0} Failed to download. Retrying...", file.file_name);
        let result = file.clone().download(session.clone()).await?;
        file.local_path = result.1;
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

    if last_file.is_some() {
        debug!("Last file found. Moving other files.");
        let mut moved = Vec::new();
        for file in files_found
            .iter()
            .filter(|f| f.file_name != "EXTRACT_LASTFILE.txt")
        {
            if !moved.contains(&file.local_path) {
                debug!("Moving {}", file.local_path);
                moved.push(file.local_path.clone());
                let mut file_destination = config.destination_path.clone();

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
        let last_file = files_found.get(last_file.unwrap()).unwrap();
        let mut file_destination = config.destination_path.clone();
        file_destination.push(&last_file.file_name);
        std::fs::rename(&last_file.local_path, file_destination)?;

        debug!("Checking for leftover files");
        for file in files_found {
            if Path::exists(&PathBuf::from(&file.local_path)) {
                warn!("Leftover file found at {}", file.local_path);
                if file.sym != *config.syms.last().unwrap() {
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

async fn connect(config: &Config, host: &String) -> anyhow::Result<SftpSession> {
    let host = if !host.is_empty() {
        if !host.contains(":") {
            warn!("No port on hostname '{host}'. Defaulting to 22.")
        }
        host
    } else {
        &config.host.to_string()
    };

    let host_split = host
        .split(':')
        .map(|s| s.to_string())
        .collect::<Vec<String>>();
    let host = host_split[0].clone();
    let port = host_split[1].parse().unwrap_or(22);

    debug!("Connecting to host '{}'", host);
    let russh_config = russh::client::Config::default();

    let sh = Client {};
    let mut session = russh::client::connect(Arc::new(russh_config), (host, port), sh).await?;

    let mut interactive_response = session
        .authenticate_keyboard_interactive_start(config.username.clone(), None)
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
                    .map(|_| config.password.clone())
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
        HOUR + 1800
    };

    for entry in session.read_dir(dir).await?.filter(filter).filter(|f| {
        f.metadata().modified().unwrap()
            > SystemTime::now()
                .checked_sub(Duration::from_secs(file_age))
                .unwrap()
    }) {
        debug!("file in dir: {dir} {:?}", entry.file_name());

        if entry.file_type().is_dir() {
            let dir = format!("{dir}/{}", entry.file_name());
            for file in session.read_dir(&dir).await? {
                if !file.file_type().is_dir() {
                    debug!("file in dir: {dir} {:?}", file.file_name());

                    result.push(format!("{dir}/{}", file.file_name()));
                }
            }
        } else {
            result.push(format!("{dir}/{}", entry.file_name()));
        }
    }

    Ok(result)
}

struct Client {}

impl client::Handler for Client {
    type Error = anyhow::Error;

    async fn check_server_key(
        &mut self,
        _server_public_key: &PublicKey,
    ) -> Result<bool, Self::Error> {
        //info!("check_server_key: {:?}", server_public_key);
        Ok(true)
    }

    async fn data(
        &mut self,
        _channel: ChannelId,
        _data: &[u8],
        _session: &mut client::Session,
    ) -> Result<(), Self::Error> {
        //trace!("data on channel {:?}: {}", channel, data.len());
        Ok(())
    }
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
            if split.is_some() {
                (label, count) = split.unwrap();
            }

            debug!("{label}, {count}")
        }
    }

    Ok(ExtractFile {
        sym: 658,
        sym_path: String::new(),
        file_name: String::new(),
        local_path: String::new(),
        is_empty: false,
    })
}
