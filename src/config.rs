use dotenvy::{dotenv, var};
use std::{env, path::PathBuf};
use tracing::{trace, warn};

#[derive(Debug)]
pub struct Config {
    pub username: String,
    pub password: String,
    pub host: String,
    pub port: u16,
    pub syms: Vec<u16>,
    pub destination_path: PathBuf,
}

impl Config {
    pub fn new() -> Self {
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

        dotenv().ok();

        let host = dotenvy::var("SYM_HOSTNAME").unwrap().to_string();
        if !host.ends_with(":22") && host.contains(':') {
            warn!("Unrecognized port detected. Continuing as configured.")
        }

        let host_split = host.split(':').collect::<Vec<_>>();
        let host = host_split[0].to_string();
        let port = if host_split.len() == 2 {
            host_split[1].parse().unwrap_or(22)
        } else {
            22
        };

        let syms = var("SYMS").unwrap().to_string();
        let password = var("SFTP_PASSWORD")
            .unwrap()
            .to_string()
            .replace(r#"\\"#, r#"\"#);

        Self {
            password,
            host,
            port,

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
