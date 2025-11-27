#![allow(unused)]
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use anyhow::Context;
use russh_sftp::client::fs::DirEntry;
use tokio::fs;
use tokio::sync::RwLock;
use tracing::{debug, error, info, trace, warn};

use crate::clients::{SftpClient, SshClient};
use crate::config::Config;
use crate::{DAY, HALF_HOUR, HOUR};

pub struct Extractor {
    sftp_session: Option<Arc<RwLock<SftpClient>>>,
    ssh_session: Option<Arc<RwLock<SshClient>>>,

    config: Config,

    sync: bool,
    checksum: bool,
    queue_count: usize,
}

// Note: Check running jobs on Symitar with "/SYM/MACROS/QUEUECONTROL -BATCH"

impl Extractor {
    pub fn new(config: Config) -> Self {
        let handle = tokio::runtime::Handle::current();
        Self {
            config,

            sftp_session: None,
            ssh_session: None,
            sync: false, // todo: Change default to async when ready
            checksum: true,
            // 3 download queues is the sweet spot it seems
            queue_count: 3, //tokio::runtime::RuntimeMetrics::num_workers(&handle.metrics()),
        }
    }

    pub fn syncronous(&mut self, sync: bool) {
        self.sync = sync
    }

    pub fn checksum(&mut self, checksum: bool) {
        self.checksum = checksum
    }

    pub fn queues(&mut self, queue_count: Option<usize>) {
        let Some(queue_count) = queue_count else {
            return;
        };
        if queue_count > 0 {
            self.queue_count = queue_count;
        }
    }

    pub async fn close(&mut self) {
        self.get_sftp().write().await.close();
        self.get_ssh().write().await.close();
    }

    pub async fn process(&mut self) -> anyhow::Result<()> {
        info!("Processing...");

        self.connect_sftp().await?;
        self.connect_ssh().await?;

        // Todo: Check and wait for completion of running ARCU extracts.

        let mut files = Vec::new();
        for sym in &self.config.syms {
            let local_path = format!(r#".\extracts\{}"#, sym.number);
            if !Path::new(&local_path).exists() {
                fs::create_dir_all(&local_path)
                    .await
                    .context(format!("Failed to create extract dir {}", local_path))?;
            }

            let dir = format!("/SYM/SYM{}/SQLEXTRACT", sym.number);
            files.append(&mut self.init_files(sym.number, dir, |_| true).await?);

            let dir = format!("/SYM/SYM{}/LETTERSPECS", sym.number);
            files.append(
                &mut self
                    .init_files(sym.number, dir, |f| {
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
        }
        trace!("{files:#?}");
        files.sort_by(|a, b| (a.len as i128 - b.len as i128).cmp(&((a.len) as i128)));
        files.reverse();

        let sftp = self.get_sftp();

        'retry: for retry in 0..4 {
            if retry > 0 {
                warn!("Retrying failed files. Retry number {retry}");
                if files.iter().filter(|f| f.error || !f.complete).count() == 0 {
                    debug!("No files to download on run {retry}.");
                    break 'retry;
                }
            }

            let queue_count = if self.sync { 1 } else { self.queue_count };

            // Todo: Make queues a distinct struct, with message passing for monitoring and logging
            // of all queues at once.
            let mut queues: Vec<Vec<ExtractFile>> = Vec::with_capacity(self.queue_count);
            for _ in 0..self.queue_count {
                queues.push(Vec::with_capacity(files.len() / self.queue_count + 1));
            }

            let mut completed = Vec::new();
            while let Some(file) = files.pop() {
                if !file.error && file.complete {
                    completed.push(file);
                    continue;
                }

                // Don't do the next calculation if unnecessary
                if queue_count == 1 {
                    queues[0].push(file);
                    continue;
                }

                // Insert file into the queue with the smallest total file length
                let queue = queues
                    .iter()
                    .enumerate()
                    .map(|q| (q.0, q.1.iter().map(|f| f.len).sum::<u64>()))
                    .min_by_key(|v| v.1)
                    .unwrap()
                    .0;
                queues[queue].push(file);
            }

            let mut task_set = tokio::task::JoinSet::new();
            for (q, queue) in queues.into_iter().enumerate() {
                task_set.spawn(Self::queue_download(sftp.clone(), queue, q));
            }
            let queues = task_set.join_all().await;

            for mut queue in queues {
                files.append(&mut queue);
            }
        }

        // Non-feature parity Todo: Parse and build datasupp file
        if files
            .iter()
            .filter(|f| f.file_name == "EXTRACT_LASTFILE.txt")
            .count()
            == 0
        {
            return Err(anyhow::anyhow!("No Lastfile in file listing?"));
        }
        // Todo: Move files

        Ok(())
    }

    async fn init_files<P>(
        &self,
        sym: u16,
        dir: String,
        filter: P,
    ) -> anyhow::Result<Vec<ExtractFile>>
    where
        P: FnMut(&DirEntry) -> bool,
    {
        let mut files = Vec::new();

        // Note: File age check may not be necessary once we're also correctly watching for the ARCU job
        // completion.

        // If running debug, we can
        // grab the last day of files. If not, just the last hour should do it.
        let file_age = if cfg!(debug_assertions) {
            DAY
        } else {
            HOUR + HALF_HOUR
        };
        let file_age = DAY;

        let client = self.get_sftp();
        let client = client.read().await;
        let shell = self.get_ssh();

        for entry in client.read_dir(&dir).await?.filter(filter).filter(|f| {
            f.metadata().modified().unwrap()
                > SystemTime::now()
                    .checked_sub(Duration::from_secs(file_age))
                    .unwrap()
        }) {
            if entry.file_type().is_dir() {
                let dir = format!("{dir}/{}", entry.file_name());
                debug!("Polling files in dir: {dir}");
                for file in client.read_dir(&dir).await? {
                    if !file.file_type().is_dir() {
                        debug!("file in dir: {dir} \"{}\"", &file.file_name());
                        let len = file.metadata().len();
                        files.push(ExtractFile::new(file, &dir, sym, len));
                    }
                }
            } else {
                debug!("file in dir: {dir} {:?}", &entry.file_name());
                let len = entry.metadata().len();
                files.push(ExtractFile::new(entry, &dir, sym, len));
            }
        }

        let ssh = self.get_ssh();
        if self.sync {
            for file in files.iter_mut() {
                file.checksum = Self::get_checksum(ssh.clone(), &file.remote_path).await?;
            }
        } else {
            'retry: for retry in 0..10 {
                if retry > 0 {
                    warn!("Retrying failed files. Retry number {retry}");
                    if files.iter().filter(|f| f.error).count() == 0 {
                        debug!("No files to checksum on run {retry}.");
                        break 'retry;
                    }
                }

                let queue_count = if self.sync || retry > 0 { 1 } else { 3 };

                let mut queues: Vec<Vec<ExtractFile>> = Vec::new();
                for _ in 0..queue_count {
                    queues.push(Vec::with_capacity(files.len() / queue_count + 1));
                }

                let mut i = 0;
                let mut completed = Vec::new();
                while let Some(file) = files.pop() {
                    if !file.error && file.complete {
                        completed.push(file);
                        continue;
                    }

                    queues[i % queue_count].push(file);
                    i += 1;
                }

                let mut task_set = tokio::task::JoinSet::new();
                for (q, queue) in queues.into_iter().enumerate() {
                    task_set.spawn(Self::queue_checksum(ssh.clone(), queue, q));
                }
                let queues = task_set.join_all().await;

                for mut queue in queues {
                    files.append(&mut queue);
                }
            }
        }

        Ok(files)
    }

    async fn get_checksum(ssh: Arc<RwLock<SshClient>>, path: &str) -> anyhow::Result<String> {
        let ssh = ssh.read().await;
        let checksum = ssh.call(format!("csum -h MD5 {path}",)).await;
        let checksum = if let Err(e) = checksum {
            error!("{e:#?}");
            return Err(e).context("Could not retrieve checksum for file");
        } else {
            checksum?
        };
        trace!("Checksum command returned '{checksum:#?}'");
        let checksum = checksum
            .0
            .first()
            .unwrap_or(&String::new())
            .split_whitespace()
            .next()
            .map(String::from)
            .unwrap_or(String::new());
        trace!("Received checksum {checksum}");
        Ok(checksum)
    }

    async fn queue_checksum(
        ssh: Arc<RwLock<SshClient>>,
        mut queue: Vec<ExtractFile>,
        q_number: usize,
    ) -> Vec<ExtractFile> {
        for file in queue.iter_mut() {
            debug!(
                "Retrieving checksum for '{}' in queue {q_number}",
                file.file_name
            );
            match Self::get_checksum(ssh.clone(), &file.remote_path).await {
                Ok(s) => {
                    file.checksum = s;
                    file.error = false;
                }
                Err(e) => {
                    error!("Error retrieving checksum: {e}");
                    file.error = true;
                }
            }
        }
        queue
    }

    async fn file_checksum(ssh: Arc<RwLock<SshClient>>, mut file: ExtractFile) -> ExtractFile {
        debug!("Checksumming '{}'", file.file_name);
        match Self::get_checksum(ssh.clone(), &file.remote_path).await {
            Ok(s) => {
                file.checksum = s;
                file.error = false;
            }
            Err(e) => {
                error!("Error retrieving checksum: {e}");
                file.error = true;
            }
        }
        file
    }

    async fn download(sftp: Arc<RwLock<SftpClient>>, file: &mut ExtractFile) -> anyhow::Result<()> {
        let local_file = format!("{}\\{}", file.local_path, file.file_name);

        if file.error {
            _ = fs::remove_file(&local_file).await;
            file.error = false;
        }

        let sftp = sftp.read().await;

        let sftp_file = sftp.read(&file.remote_path).await?;

        let mut md5 = md5::compute(&sftp_file);
        let md5_result = format!("{md5:x}");

        debug!("file '{}'", file.file_name);
        debug!("md5 of file:  {md5_result}");
        debug!("md5 from AIX: {}", file.checksum);

        if md5_result == file.checksum {
            debug!("len: {}", sftp_file.len());
            let sftp_contents =
                String::from_utf8(sftp_file).context("Could not convert file to String.")?;

            let sftp_contents = if sftp_contents.contains('\n') && !sftp_contents.contains("\r\n") {
                sftp_contents.replace('\n', "\r\n")
            } else {
                sftp_contents
            };
            fs::write(&file.local_path, sftp_contents).await?;
        }

        file.complete = true;
        Ok(())
    }

    async fn queue_download(
        sftp: Arc<RwLock<SftpClient>>,
        mut queue: Vec<ExtractFile>,
        q_number: usize,
    ) -> Vec<ExtractFile> {
        for file in queue.iter_mut() {
            debug!("Downloading '{}' in queue {q_number}", file.file_name);
            Self::download(sftp.clone(), file).await;
        }
        queue
    }

    async fn file_download(sftp: Arc<RwLock<SftpClient>>, mut file: ExtractFile) -> ExtractFile {
        debug!("Downloading '{}'", file.file_name);
        Self::download(sftp.clone(), &mut file).await;
        file
    }

    pub async fn connect_sftp(&mut self) -> anyhow::Result<()> {
        if self.sftp_session.as_ref().is_none()
            || self
                .sftp_session
                .clone()
                .unwrap()
                .read()
                .await
                .is_closed()
                .await
        {
            self.sftp_session = Some(Arc::new(RwLock::new(
                SftpClient::connect(&self.config)
                    .await
                    .context("Could not connect via SFTP")?,
            )));
        }
        Ok(())
    }

    pub fn get_sftp(&self) -> Arc<RwLock<SftpClient>> {
        self.sftp_session
            .clone()
            .expect("sftp_session not initialized?")
    }

    pub async fn connect_ssh(&mut self) -> anyhow::Result<()> {
        if self.ssh_session.as_ref().is_none()
            || self
                .ssh_session
                .clone()
                .unwrap()
                .read()
                .await
                .is_closed()
                .await
        {
            self.ssh_session = Some(Arc::new(RwLock::new(
                SshClient::connect(&self.config)
                    .await
                    .context("Could not connect via SFTP")?,
            )));
        }
        Ok(())
    }

    pub fn get_ssh(&self) -> Arc<RwLock<SshClient>> {
        self.ssh_session
            .clone()
            .expect("sftp_session not initialized?")
    }
}

#[derive(Debug, Default)]
struct ExtractFile {
    file_name: String,
    sym: u16,
    remote_path: String,
    local_path: String,

    checksum: String,
    len: u64,
    error: bool,
    complete: bool,
}

impl ExtractFile {
    fn new(entry: DirEntry, path: &String, sym: u16, len: u64) -> Self {
        let file_name = entry.file_name();
        ExtractFile {
            file_name: file_name.clone(),
            sym,
            remote_path: format!(r#"{path}/{file_name}"#),
            local_path: format!(r#".\extracts\{}\{file_name}"#, sym),
            len,
            ..Default::default()
        }
    }

    async fn download(&mut self, destination_path: String) -> anyhow::Result<()> {
        todo!();
        Ok(())
    }
}
