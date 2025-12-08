use serde::{Deserialize, de};
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
    debug: Option<bool>,
    dry: Option<bool>,

    sftp: Sftp,
    #[serde(rename = "sym")]
    syms: Vec<Sym>,

    #[serde(deserialize_with = "deserialize_level")]
    log_level: Option<Level>,

    // todo: remove
    experimental: Option<bool>,
}

#[derive(Debug, Clone)]
pub struct Config {
    pub destination: PathBuf,
    pub log_level: Level,
    pub download_queues: usize,
    pub threads: Option<usize>,
    pub debug: bool,
    pub dry: bool,
    pub experimental: bool,

    pub sftp: Sftp,
    pub syms: Vec<Sym>,
}

impl Config {
    pub fn new() -> Self {
        let config_string =
            read_to_string("./config.toml").expect("Unable to find or open config.toml");

        let config_options: ConfigOptions = toml::from_str::<ConfigOptions>(&config_string)
            .expect("Could not deserialize config.toml");

        let config: Self = config_options.into();

        config.check_config_panics("config.toml ");
        config
    }

    pub fn with_cli(mut self, cli: &Cli) -> Self {
        if let Some(ref destination) = cli.destination {
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
        self.debug = self.debug || cli.debug;
        self.dry = self.dry || cli.dry;
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

        self.experimental = self.experimental || cli.experimental;

        self.check_config_panics("Argument --");
        self
    }

    pub fn sym_names(&self) -> Vec<u16> {
        self.syms.iter().map(|s| s.number).collect()
    }
    pub fn sym_jobs(&self) -> Vec<&String> {
        self.syms.iter().map(|s| &s.extract_job).collect()
    }

    fn check_config_panics(&self, source: &str) {
        let mut errors = Vec::new();

        if !fs::exists(&self.destination).is_ok_and(|v| v) {
            errors.push(format!(
                "{source}destination '{}' does not exist or may be an invalid path!",
                self.destination.display()
            ));
        }

        match self.syms.len() {
            0 => errors.push(format!("{source}syms not defined.")),
            1 => {
                let sym = &self.syms[0];
                if matches!(
                    sym.extract_part,
                    SymExtractPart::PreClose | SymExtractPart::PostClose
                ) {
                    errors.push(format!("{source}sym {} extract_part should be excluded, 'Single', or there should be two syms defined", sym.number))
                }
            }
            2 => {
                let mut syms = [&self.syms[0], &self.syms[1]];
                syms.sort_by_key(|s| s.extract_part);

                if !matches!(syms[0].extract_part, SymExtractPart::PreClose) {
                    errors.push(format!("{source}sym {} extract_part should be defined as 'PreClose' or 'PostClose' for split extract jobs", 
                        syms[0].number))
                }
                if !matches!(syms[1].extract_part, SymExtractPart::PostClose) {
                    errors.push(format!("{source}sym {} extract_part should be defined as 'PreClose' or 'PostClose' for split extract jobs", 
                        syms[1].number))
                }
            }
            _ => errors.push(format!("{source}syms cannot have more than 2 items.")),
        }

        if !errors.is_empty() {
            for config_error in errors {
                println!("{config_error}");
            }
            panic!("Configuration has errors!")
        }
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
            debug: value.debug.unwrap_or(cfg!(debug_assertions)),
            dry: value.dry.unwrap_or(false),
            experimental: value.experimental.unwrap_or(false),

            sftp: value.sftp,
            syms: value.syms,

            download_queues: value.download_queues.unwrap_or(3),
            log_level: value.log_level.unwrap_or(Level::WARN),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct Sftp {
    pub hostname: String,
    pub port: u16,
    pub username: String,
    pub password: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Sym {
    pub number: u16,
    pub extract_job: String,
    pub extract_part: SymExtractPart,
}

#[derive(Debug, Copy, Clone, Deserialize, Eq, Ord, PartialEq, PartialOrd, Default)]
pub enum SymExtractPart {
    #[default]
    Unknown,
    Single,
    PreClose,
    PostClose,
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
