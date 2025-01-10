use std::{
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, SystemTime},
};

use anyhow::{Context, Result};
use async_trait::async_trait;
use clap::{ArgAction, Parser};
use dotenvy::{dotenv, var};
use russh::{
    client::{self, KeyboardInteractiveAuthResponse},
    keys::PublicKey,
    ChannelId,
};
use russh_sftp::client::{fs::DirEntry, SftpSession};
use time::{macros::format_description, UtcOffset};
use tokio::fs;
use tracing::level_filters::LevelFilter;
use tracing::{debug, error, info, trace, warn, Level};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::{fmt, Layer};

#[derive(Parser, Debug)]
struct CLI {
    #[arg(long, action = ArgAction::SetTrue)]
    stdout: bool,

    #[arg(short, long, default_value_t = Level::WARN)]
    log: Level,

    #[arg(long, default_value_t = String::new())]
    host: String,
}

#[derive(Debug)]
struct Config {
    username: String,
    password: String,
    host: String,
    syms: Vec<u16>,
    destination_path: PathBuf,
}

impl Config {
    fn new() -> Self {
        dotenv().ok();
        let host = dotenvy::var("SYM_HOSTNAME").unwrap().to_string();
        if !host.ends_with(":22") && host.contains(':') {
            warn!("Unrecognized port detected. Continuing as configured.")
        }

        let syms = var("SYMS").unwrap().to_string();
        let password = var("SFTP_PASSWORD")
            .unwrap()
            .to_string()
            .replace(r#"\\"#, r#"\"#);

        Self {
            password,
            host,

            username: var("SFTP_USERNAME").unwrap().to_string(),
            destination_path: PathBuf::from(var("DESTINATION").unwrap().to_string()),

            syms: syms
                .trim_matches(['[', ']'])
                .split(',')
                .map(|s| {
                    trace!("Parsing {s} to integer");
                    s.trim().parse().unwrap()
                })
                .collect(),
        }
    }
}

const DAY: u64 = 86400;
const HOUR: u64 = 3600;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = CLI::parse();

    let timer_format =
        format_description!("[year]-[month]-[day] [hour]:[minute]:[second] [period]");
    let timer = fmt::time::OffsetTime::new(UtcOffset::current_local_offset()?, timer_format);

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

    let config = Config::new();

    loop {
        let mut done = false;
        match process(&config, &cli).await {
            Ok(process_done) => done = process_done,
            Err(e) => error!("Error processing {e}"),
        }

        if cfg!(debug_assertions) {
            debug!("Exiting due to running in debug. If this is not intended, compile with 'cargo build --release'");
            break;
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

    Ok(())
}

async fn process(config: &Config, cli: &CLI) -> anyhow::Result<bool> {
    let session = Arc::from(connect(&config, &cli.host).await?);
    let mut last_file = None;

    let mut files_found: Vec<ExtractFile> = Vec::new();
    for sym in &config.syms {
        let dir = format!("/SYM/SYM{}/SQLEXTRACT", sym);
        let mut sym_files = get_files(&session, &dir, |_| true).await?;

        let dir = format!("/SYM/SYM{}/LETTERSPECS", sym);

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
    if !files_found.is_empty() {
        debug!("Waiting 1 hour for batch job to complete.");
        tokio::time::sleep(Duration::from_secs(HOUR)).await; //
    }

    trace!("{files_found:#?}");
    // Download files asyncronously from remote.
    // Retries downloads once.
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
            KeyboardInteractiveAuthResponse::Failure => {
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

    for entry in session
        .read_dir(dir)
        .await?
        .into_iter()
        .filter(filter)
        .filter(|f| {
            f.metadata().modified().unwrap()
                > SystemTime::now()
                    .checked_sub(Duration::from_secs(file_age))
                    .unwrap()
        })
    {
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

#[async_trait]
impl client::Handler for Client {
    type Error = anyhow::Error;

    async fn check_server_key(
        &mut self,
        server_public_key: &PublicKey,
    ) -> Result<bool, Self::Error> {
        info!("check_server_key: {:?}", server_public_key);
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

#[derive(Debug, Clone)]
struct ExtractFile {
    sym: u16,
    sym_path: String,
    file_name: String,
    local_path: String,
    is_empty: bool,
}

impl ExtractFile {
    fn new(sym: u16, sym_path: String, file_name: String, local_path: String) -> Self {
        ExtractFile {
            sym,
            sym_path,
            file_name,
            local_path,
            is_empty: false,
        }
    }

    async fn download(self, session: Arc<SftpSession>) -> anyhow::Result<(String, String, bool)> {
        let local_dir = format!(r#".\extracts\{}"#, self.sym);
        let local_path = format!("{}\\{}", local_dir, self.file_name);

        debug!(
            "Downloading {} from {} to {}",
            self.file_name, self.sym_path, local_path
        );

        if !self.local_path.is_empty() {
            warn!("File '{}' already downloaded!", self.file_name);
            return Ok((self.file_name, local_path, self.is_empty));
        }

        if !Path::new(&local_dir).exists() {
            debug!(
                "local '{}' directory does not exist. Creating now.",
                local_dir
            );
            fs::create_dir_all(&local_dir)
                .await
                .context(format!("Failed to create extract dir {local_dir}"))?;
        }

        if fs::try_exists(&local_path).await.context(format!(
            "Failed to check if local file '{local_path}' exists."
        ))? {
            if fs::read_to_string(&local_path)
                .await
                .context(format!(
                    "Failed to check if local file '{local_path}' was empty."
                ))?
                .is_empty()
                && !self.is_empty
            {
                warn!(
                    "Empty file '{}' exists on local disk at '{}'. Overwriting...",
                    self.file_name, local_path
                );
                fs::remove_file(&local_path)
                    .await
                    .context(format!("Failed to overwrite empty local file {local_path}"))?;
            } else {
                debug!(
                    "File '{}' exists on local disk at '{}'. Skipping.",
                    self.file_name, local_path
                );

                return Ok((self.file_name, local_path, self.is_empty));
            }
        } else {
            debug!("Creating local file {}", local_path);
            fs::File::create_new(&local_path)
                .await
                .context(format!("Failed to create {local_path}"))?;
        }

        let mut local_md5 = md5::Context::new();
        let mut remote_md5 = md5::Context::new();

        debug!("Reading {} from remote directory", self.file_name);
        let data_len = session
            .metadata(&self.sym_path)
            .await
            .context(format!("Could not read file len for {}", self.sym_path))?;
        let mut sftp_data = session.read(&self.sym_path).await.context(format!(
            "Failed to read data from remote directory for {}",
            self.sym_path
        ))?;
        while sftp_data.len() < data_len.len().try_into().unwrap() {
            warn!("sftp_data too short. Redownloading.");
            tokio::time::sleep(Duration::from_secs(5)).await;
            sftp_data = session.read(&self.sym_path).await.context(format!(
                "Failed to read data from remote directory for {}",
                self.sym_path
            ))?;
        }
        let sftp_data_empty = sftp_data.is_empty();
        let data = String::from_utf8(sftp_data).context(format!(
            "Could not convert remote data for '{}' to utf8",
            self.sym_path
        ))?;
        let data = data.replace('\n', "\r\n");
        remote_md5.consume(&data);

        debug!("Writing {} to local file at {}", self.file_name, local_path);
        fs::write(&local_path, data).await.context(format!(
            "Could not write local file '{local_path}' with remote data."
        ))?;

        let local_file_contents = fs::read_to_string(&local_path).await.context(format!(
            "Could not read local file '{local_path}' back to check validity"
        ))?;
        local_md5.consume(&local_file_contents);

        let remote_check = remote_md5.compute();
        let local_check = local_md5.compute();
        if remote_check != local_check {
            error!("File checksums do not match!");
            fs::remove_file(&local_path).await.context(format!(
                "Could not remove file with invalid checksum {local_path}"
            ))?;
            return Err(std::io::Error::last_os_error().into());
        }

        return Ok((self.file_name, local_path, sftp_data_empty));
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
