use serde::{de, Deserialize};
use std::{
    env,
    fs::{self, read_to_string},
    path::PathBuf,
    str::FromStr,
};
use tracing::Level;

use crate::Cli;

pub fn set_current_dir() {
    let exe_path = env::current_exe().expect("Could not get executable path");
    let mut working_directory = PathBuf::new();
    for comp in exe_path.components() {
        let comp_str = comp
            .as_os_str()
            .to_str()
            .expect("Could not break down executable path");

        if comp_str.ends_with(".exe") {
            break;
        }

        working_directory = working_directory.join(comp);

        if comp_str == "extract-downloader" {
            break;
        }
    }
    println!("{}", working_directory.display());
    env::set_current_dir(&working_directory).expect("Could not set working directory");
}

#[derive(Deserialize)]
struct ConfigOptions {
    destination: PathBuf,
    download_queues: Option<usize>,
    threads: Option<usize>,

    sftp: Sftp,
    #[serde(rename = "sym")]
    syms: Vec<Sym>,

    #[serde(deserialize_with = "deserialize_level")]
    log_level: Option<Level>,
}

#[derive(Debug)]
pub struct Config {
    pub destination: PathBuf,
    pub log_level: Level,
    pub download_queues: usize,
    pub threads: Option<usize>,

    pub sftp: Sftp,
    pub syms: Vec<Sym>,
}

impl Config {
    pub fn new() -> Self {
        let config_string =
            read_to_string("./config.toml").expect("Unable to find or open config.toml");

        let ret: ConfigOptions = toml::from_str::<ConfigOptions>(&config_string)
            .expect("Could not deserialize config.toml");
        ret.into()
    }

    pub fn with_cli(mut self, cli: &Cli) -> Self {
        if let Some(ref destination) = cli.destination {
            assert!(
                fs::exists(destination).is_ok_and(|v| v),
                "Argument --destination '{}' does not exist or may be an invalid path!",
                destination.display()
            );
            self.destination = destination.clone();
        }
        if let Some(log_level) = cli.log_level {
            self.log_level = log_level;
        }
        if let Some(download_queues) = cli.queue_count {
            self.download_queues = download_queues
        }
        if let Some(threads) = cli.threads {
            self.threads = Some(threads)
        }

        if let Some(ref hostname) = cli.hostname {
            self.sftp.hostname = hostname.clone();
        }
        if let Some(port) = cli.port {
            self.sftp.port = port;
        }
        if let Some(ref username) = cli.username {
            self.sftp.username = username.clone();
        }
        if let Some(ref password) = cli.password {
            self.sftp.password = password.clone();
        }

        self
    }

    pub fn sym_names(&self) -> Vec<u16> {
        self.syms.iter().map(|s| s.number).collect()
    }
}

impl From<ConfigOptions> for Config {
    fn from(value: ConfigOptions) -> Self {
        assert!(
            fs::exists(&value.destination).is_ok_and(|v| v),
            "Argument --destination '{}' does not exist or may be an invalid path!",
            value.destination.display()
        );

        Self {
            destination: value.destination,
            threads: value.threads,

            sftp: value.sftp,
            syms: value.syms,

            download_queues: value.download_queues.unwrap_or(3),
            log_level: value.log_level.unwrap_or(Level::WARN),
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct Sftp {
    pub hostname: String,
    pub port: u16,
    pub username: String,
    pub password: String,
}

#[derive(Debug, Deserialize)]
pub struct Sym {
    pub number: u16,
    #[allow(unused)] // Todo: remove allow
    pub extract_job: String,
}

fn deserialize_level<'de, D>(deserializer: D) -> Result<Option<Level>, D::Error>
where
    D: de::Deserializer<'de>,
{
    let value = Option::<String>::deserialize(deserializer)?;
    if let Some(value) = value {
        Level::from_str(&value)
            .map(Some)
            .map_err(serde::de::Error::custom)
    } else {
        Ok(None)
    }
}
