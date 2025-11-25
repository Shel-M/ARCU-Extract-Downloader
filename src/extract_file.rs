use anyhow::{anyhow, Context};
use clap::Parser;
use russh_sftp::client::SftpSession;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::fs;
use tokio::time::sleep;
use tracing::{debug, error, info, warn};

use crate::Cli;

#[derive(Debug, Clone)]
pub struct ExtractFile {
    pub sym: u16,
    pub sym_path: String,
    pub file_name: String,
    pub local_path: String,
    pub is_remote_empty: bool,
    pub download_error: bool,
}

impl ExtractFile {
    pub fn new(sym: u16, sym_path: String, file_name: String, local_path: String) -> Self {
        ExtractFile {
            sym,
            sym_path,
            file_name,
            local_path,
            is_remote_empty: false,
            download_error: true,
        }
    }

    pub async fn download(
        self,
        session: Arc<SftpSession>,
    ) -> anyhow::Result<(String, String, bool)> {
        let local_dir = format!(r#".\extracts\{}"#, self.sym);
        let local_file = format!("{}\\{}", local_dir, self.file_name);

        debug!(
            "Downloading {} from {} to {}",
            self.file_name, self.sym_path, local_file
        );

        // if !self.local_path.is_empty() {
        //     warn!("File '{}' already downloaded!", self.file_name);
        //     return Ok((self.file_name, local_path, self.is_remote_empty));
        // }

        if !Path::new(&local_dir).exists() {
            debug!(
                "local '{}' directory does not exist. Creating now.",
                local_dir
            );
            fs::create_dir_all(&local_dir)
                .await
                .context(format!("Failed to create extract dir {local_dir}"))?;
        }

        let last_modified = session.metadata(&self.sym_path).await?.modified()?;
        while last_modified
            > SystemTime::now()
                .checked_sub(Duration::from_secs(60 * 20))
                .ok_or(anyhow!("Couldn't subtract from current time"))?
        {
            debug!(
                "File modified in the last 20 minutes ({:#?}) - waiting for writes to finish.",
                last_modified
            );
            sleep(Duration::from_secs(60)).await;
        }

        if fs::try_exists(&local_file).await.context(format!(
            "Failed to check if local file '{local_file}' exists."
        ))? {
            if !fs::read_to_string(&local_file)
                .await
                .context(format!(
                    "Failed to check if local file '{local_file}' was empty."
                ))?
                .is_empty()
                && self.is_remote_empty
            {
                warn!(
                    "Empty file '{}' exists on local disk at '{}'. Overwriting...",
                    self.file_name, local_file
                );
                info!(
                    "{} && {}",
                    !fs::read_to_string(&local_file)
                        .await
                        .context(format!(
                            "Failed to check if local file '{local_file}' was empty."
                        ))?
                        .is_empty(),
                    self.local_path.is_empty()
                );
                fs::remove_file(&local_file)
                    .await
                    .context(format!("Failed to overwrite empty local file {local_file}"))?;
            } else if self.download_error {
                warn!(
                    "Errored file '{}' exists on local disk at '{}'. Overwriting...",
                    self.file_name, local_file
                );
                fs::remove_file(&local_file)
                    .await
                    .context(format!("Failed to overwrite empty local file {local_file}"))?;
            } else {
                debug!(
                    "File '{}' exists on local disk at '{}'. Skipping.",
                    self.file_name, local_file
                );
                info!(
                    "{} && {}",
                    !fs::read_to_string(&local_file)
                        .await
                        .context(format!(
                            "Failed to check if local file '{local_file}' was empty."
                        ))?
                        .is_empty(),
                    self.local_path.is_empty()
                );

                return Ok((self.file_name, local_file, self.is_remote_empty));
            }
        } else {
            debug!("Creating local file {}", local_file);
            fs::File::create_new(&local_file)
                .await
                .context(format!("Failed to create {local_file}"))?;
        }

        let cli = Cli::parse();
        if !cli.no_check {
            let mut local_md5 = md5::Context::new();
            let mut remote_md5 = md5::Context::new();

            debug!("Reading {} from remote directory", self.file_name);
            let data_len = session
                .metadata(&self.sym_path)
                .await
                .context(format!("Could not read file len for {}", self.sym_path))?;

            // Todo: this downloads the file a second time for validation. Change that.
            let sftp_res = session.read(&self.sym_path).await;
            let mut sftp_data = match sftp_res {
                Ok(v) => v,
                Err(e) => {
                    error!("{e:#?}");
                    error!("{:#?}", std::io::Error::last_os_error());

                    return Err(e).context(format!(
                        "Failed to read data from remote directory for {}",
                        self.sym_path
                    ));
                }
            };

            while sftp_data.len() < data_len.len().try_into()? {
                warn!("sftp_data too short. Redownloading.");
                tokio::time::sleep(Duration::from_secs(5)).await;
                let sftp_res = session.read(&self.sym_path).await;
                sftp_data = match sftp_res {
                    Ok(v) => v,
                    Err(e) => {
                        return Err(e).context(format!(
                            "Failed to read data from remote directory for {}",
                            self.sym_path
                        ));
                    }
                };
                // sftp_data = session.read(&self.sym_path).await.context(format!(
                //     "Failed to read data from remote directory for {}",
                //     self.sym_path
                // ))?;
            }
            // let sftp_data_empty = sftp_data.is_empty();
            let mut data = String::from_utf8(sftp_data).context(format!(
                "Could not convert remote data for '{}' to utf8",
                self.sym_path
            ))?;
            if !data.contains("\r\n") {
                data = data.replace('\n', "\r\n");
            }
            remote_md5.consume(&data);

            debug!("Writing {} to local file at {}", self.file_name, local_file);
            fs::write(&local_file, data).await.context(format!(
                "Could not write local file '{local_file}' with remote data."
            ))?;

            let local_file_contents = fs::read_to_string(&local_file).await.context(format!(
                "Could not read local file '{local_file}' back to check validity"
            ))?;
            local_md5.consume(&local_file_contents);

            let remote_check = remote_md5.finalize();
            let local_check = local_md5.finalize();
            if remote_check != local_check {
                error!("File checksums do not match!");
                fs::remove_file(&local_file).await.context(format!(
                    "Could not remove file with invalid checksum {local_file}"
                ))?;
                return Err(std::io::Error::last_os_error().into());
            } else {
                let remote_check = format!("{:x}", remote_check);
                let local_check = format!("{:x}", local_check);
                info!(
                    "File checksums match! ...{} == ...{}",
                    &remote_check[remote_check.len() - 4..],
                    &local_check[local_check.len() - 4..]
                )
            }
        }

        Ok((
            self.file_name,
            local_file,
            session.metadata(self.sym_path).await?.is_empty(),
        ))
    }
}
