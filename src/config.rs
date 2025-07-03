use dotenvy::{dotenv, var};
use std::path::PathBuf;
use tracing::{trace, warn};

#[derive(Debug)]
pub struct Config {
    pub username: String,
    pub password: String,
    pub host: String,
    pub syms: Vec<u16>,
    pub destination_path: PathBuf,
}

impl Config {
    pub fn new() -> Self {
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
