use std::fmt::Display;
use std::fs::read_to_string;
// #![allow(unused)]
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use anyhow::anyhow;
use russh_sftp::client::fs::DirEntry;
use tokio::fs;
use tokio::sync::RwLock;
use tracing::{debug, error, info, trace, warn};

use crate::clients::{SftpClient, SshClient};
use crate::config::{Config, Sym, SymExtractPart};

pub struct Extractor {
    sftp_session: Option<Arc<RwLock<SftpClient>>>,
    ssh_session: Option<Arc<RwLock<SshClient>>>,

    config: Config,

    queue_count: usize,
}

impl Extractor {
    pub fn new(config: Config) -> Self {
        let queue_count = config.download_queues;
        Self {
            config,
            queue_count,

            sftp_session: None,
            ssh_session: None,
        }
    }

    pub async fn close(&mut self) {
        let _ = self.get_sftp().write().await.close().await;
        let _ = self.get_ssh().write().await.close().await;

        self.sftp_session = None;
        self.ssh_session = None;
    }

    pub async fn watch(&mut self) -> anyhow::Result<()> {
        self.connect_ssh().await?;
        self.connect_sftp().await?;

        while !self.extracts_running().await? && !self.config.debug {
            self.close().await;

            debug!("Waiting 15 minutes for ARCU Extracts to be running...");
            tokio::time::sleep(Duration::from_secs(300 * 3)).await;

            self.connect_ssh().await?;
            self.connect_sftp().await?;
        }

        self.process().await
    }

    pub async fn process(&mut self) -> anyhow::Result<()> {
        info!("Processing...");
        self.connect_ssh().await?;
        self.connect_sftp().await?;

        trace!("Checking for ARCU Extracts to be finished.");
        while self.extracts_running().await? {
            debug!("Waiting 5 minutes for ARCU Extracts to be finished...");
            tokio::time::sleep(Duration::from_secs(300)).await
        }

        // let mut files = Vec::new();
        // let sym = self.config.syms.iter().find(|s| s.number == 658).unwrap();
        // let dir = format!("/SYM/SYM{}/LETTERSPECS", sym.number);
        // files.append(
        //     &mut self
        //         .init_files(sym, dir, |f| {
        //             !f.file_name().contains("DataSupp.")
        //             &&
        //             // && (f.file_name().starts_with("EXTRACT.")
        //             //     || f.file_name().starts_with("FMT.")
        //             //     || f.file_name().starts_with("MACRO.")
        //             //    ||
        //             f.file_name().starts_with("CD_Trial_Bal_for_")
        //             //     || f.file_name() == "CD_Trial"
        //             //     || f.file_name() == "Episys_DataSupp_Statistics.txt"
        //             //     || f.file_name() == "Episys_Database__Extract_Stats"
        //             //     || f.file_name() == "EXTRACT_LASTFILE.txt")
        //         })
        //         .await?,
        // );
        //
        // assert!(
        //     files.iter().any(|f| f.file_name.starts_with("CD")),
        //     "CD Trial Bal Not Found"
        // );

        let mut files = Vec::new();
        for sym in &self.config.syms {
            let local_path = format!(r#".\extracts\{}"#, sym.number);
            if !Path::new(&local_path).exists() {
                fs::create_dir_all(&local_path)
                    .await
                    .context(format!("Failed to create extract dir {}", local_path))?;
            }

            let dir = format!("/SYM/SYM{}/SQLEXTRACT", sym.number);
            files.append(
                &mut self
                    .init_files(sym, dir, |d| {
                        !d.file_type().is_dir()
                            || (d.file_type().is_dir() && d.file_name().starts_with("EPIO"))
                    })
                    .await?,
            );

            let dir = format!("/SYM/SYM{}/LETTERSPECS", sym.number);
            files.append(
                &mut self
                    .init_files(sym, dir, |f| {
                        !f.file_name().contains("DataSupp.")
                            && (f.file_name().starts_with("EXTRACT.")
                                || f.file_name().starts_with("FMT.")
                                || f.file_name().starts_with("MACRO.")
                                || f.file_name().starts_with("CD_Trial_Bal_for_")
                                || f.file_name() == "Episys_DataSupp_Statistics.txt"
                                || f.file_name() == "Episys_Database__Extract_Stats"
                                || f.file_name() == "EXTRACT_LASTFILE.txt")
                    })
                    .await?,
            );
        }
        assert!(
            files.iter().any(|f| f.file_name.starts_with("CD")),
            "CD Trial Bal Not Found"
        );

        files.sort_by_key(|k| k.len);
        trace!("{files:#?}");

        let sftp = self.get_sftp();

        'retry: for retry in 0..4 {
            if retry > 0 {
                warn!("Retrying failed files. Retry number {retry}");
                if files.iter().filter(|f| f.error || !f.complete).count() == 0 {
                    debug!("No files to download on run {retry}.");
                    break 'retry;
                }
            }

            // Non-feature parity todo: Make queues a distinct struct, with message passing for monitoring and logging
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
                if self.queue_count == 1 {
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
                task_set.spawn(Self::queue_downloads(sftp.clone(), queue, q));
            }
            let queues = task_set.join_all().await;

            for ref mut queue in queues {
                files.append(queue);
            }

            if files.iter().filter(|f| f.error || !f.complete).count() == 0 {
                break;
            }
        }

        let files_len = files.len();
        files.sort_by_key(|f| f.len);
        files.dedup_by(|a, b| {
            a.remote_path
                .eq_ignore_ascii_case(&b.remote_path.to_ascii_lowercase())
        });
        if files_len < files.len() {
            return Err(anyhow::anyhow!("File duplication detected."));
        }

        // Todo: If we have two syms, find the datasupp statistics files, remove from files, send to
        // function that uses them to create and returns a new stats ExtractFile.
        // Maybe just hand in the mutable files list?
        combine_datasupp_stats(&mut files);

        if files
            .iter()
            .filter(|f| f.file_name == "EXTRACT_LASTFILE.txt")
            .count()
            == 0
        {
            return Err(anyhow::anyhow!("No Lastfile in file listing?"));
        }

        if !self.config.dry {
            self.move_files(files)?;
        } else {
            debug!("Not moving files to destination due to running as 'dry'");
        }

        Ok(())
    }

    async fn init_files<P>(
        &self,
        sym: &Sym,
        dir: String,
        filter: P,
    ) -> anyhow::Result<Vec<ExtractFile>>
    where
        P: FnMut(&DirEntry) -> bool,
    {
        let mut files = Vec::new();

        let client = self.get_sftp();
        let client = client.read().await;

        for entry in client.read_dir(&dir).await?.filter(filter) {
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
                debug!("file in dir: {dir} \"{}\"", &entry.file_name());
                let len = entry.metadata().len();
                files.push(ExtractFile::new(entry, &dir, sym, len));
            }
        }

        let ssh = self.get_ssh();
        'retry: for retry in 0..10 {
            let queue_count = if retry > 0 {
                warn!("Retrying failed files. Retry number {retry}");
                if files.iter().filter(|f| f.error).count() == 0 {
                    debug!("No files to checksum on run {retry}.");
                    break 'retry;
                }

                1
            } else {
                3
            };

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
                task_set.spawn(Self::get_checksums(ssh.clone(), queue, q));
            }
            let queues = task_set.join_all().await;

            for mut queue in queues {
                files.append(&mut queue);
            }

            if files.iter().filter(|f| f.error).count() == 0 {
                break;
            }
        }

        Ok(files)
    }

    fn move_files(&self, files: Vec<ExtractFile>) -> anyhow::Result<()> {
        if !std::fs::exists(&self.config.destination)
            .context("Attempting to verify destination path")?
        {
            std::fs::create_dir(&self.config.destination).context("")?;
        }

        trace!("Moving files: \n{files:#?}");
        for file in files
            .iter()
            .filter(|f| f.file_name != "EXTRACT_LASTFILE.txt")
        {
            debug!("Moving {}", file.local_path);
            let mut file_destination = self.config.destination.clone();

            // These files hit length limits for converting to letter files
            // so, we'll rename to the expected format here
            if file.file_name == *"Episys_Database__Extract_Stats" {
                file_destination.push("Episys_Database__Extract_Statistics.txt");
            } else if file.file_name.starts_with("CD_Trial_Bal_for_") {
                // IE CD_Trial_Bal_for_01_03_24
                let date = &file.file_name.trim_start_matches("CD_Trial_Bal_for_");
                file_destination.push("Close_Day_Trial_Balance_for_".to_owned() + date + ".txt");
            } else {
                file_destination.push(&file.file_name);
            }

            std::fs::rename(&file.local_path, &file_destination).context(format!(
                "Failed to move file '{}' to '{}'",
                file.local_path,
                &file_destination.display()
            ))?
        }

        debug!("Moving last file.");

        let mut last_file = None;
        for (i, file) in files.iter().enumerate() {
            if let "EXTRACT_LASTFILE.txt" = &*file.file_name {
                last_file = Some(i)
            };
        }
        let Some(last_file) = last_file else {
            return Err(anyhow!("No Last File found?"));
        };

        let last_file = files.get(last_file).unwrap();
        let mut file_destination = self.config.destination.clone();
        file_destination.push(&last_file.file_name);
        std::fs::rename(&last_file.local_path, file_destination)?;

        debug!("Checking for leftover files");
        for file in files {
            if Path::exists(&PathBuf::from(&file.local_path)) {
                warn!("Leftover file found at {}", file.local_path);
                if file.sym != self.config.syms.last().unwrap().number {
                    let _ = std::fs::remove_file(file.local_path);
                } else {
                    std::fs::rename(file.local_path, format!(".\\extracts\\{}", file.file_name))?
                }
            }
        }

        Ok(())
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

    async fn extracts_running(&self) -> anyhow::Result<bool> {
        let running_jobs = BatchJobs::try_new(self.get_ssh()).await?;
        trace!("running_jobs: {running_jobs:#?}");

        let arcu_running = running_jobs
            .iter()
            .any(|b| self.config.sym_jobs().contains(&&b.job_name));
        debug!("ARCU Extracts running? {arcu_running}");

        Ok(arcu_running)
    }

    async fn get_checksums(
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

    async fn queue_downloads(
        sftp: Arc<RwLock<SftpClient>>,
        mut queue: Vec<ExtractFile>,
        q_number: usize,
    ) -> Vec<ExtractFile> {
        for file in queue.iter_mut() {
            debug!(
                "Downloading '{}' in queue {q_number} (len: {})",
                file.file_name, file.len
            );
            if let Err(e) = Self::download(sftp.clone(), file).await {
                error!("Error downloading file '{}' ({})", file.file_name, e);
                file.error = true;
            };
        }
        queue
    }

    async fn download(sftp: Arc<RwLock<SftpClient>>, file: &mut ExtractFile) -> anyhow::Result<()> {
        let local_file = format!("{}\\{}", file.local_path, file.file_name);

        if file.error {
            _ = fs::remove_file(&local_file).await;
            file.error = false;
        }

        let sftp = sftp.read().await;

        let sftp_file = sftp.read(&file.remote_path).await?;

        let md5 = md5::compute(&sftp_file);
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
                    .context("Could not connect via SSH")?,
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

#[allow(unused)]
#[derive(Debug, Default)]
struct ExtractFile {
    file_name: String,
    sym: u16,
    extract_part: SymExtractPart,
    remote_path: String,
    local_path: String,

    checksum: String,
    len: u64,
    error: bool,
    complete: bool,
}

impl ExtractFile {
    fn new(entry: DirEntry, path: &String, sym: &Sym, len: u64) -> Self {
        let file_name = entry.file_name();

        ExtractFile {
            file_name: file_name.clone(),
            sym: sym.number,
            extract_part: sym.extract_part,
            remote_path: format!(r#"{path}/{file_name}"#),
            local_path: format!(r#".\extracts\{}\{file_name}"#, sym.number),
            len,
            ..Default::default()
        }
    }
}

#[allow(unused)]
#[derive(Debug)]
struct BatchJobs {
    queue: i64,
    status: BatchQueueStatus,
    job_name: String,
}

impl BatchJobs {
    async fn try_new(ssh: Arc<RwLock<SshClient>>) -> anyhow::Result<Vec<Self>> {
        let queues = BatchQueues::try_new(ssh).await?;

        Ok(queues
            .into_iter()
            .filter(|q| match &q.status {
                BatchQueueStatus::Ready => false,
                BatchQueueStatus::Running => true,
                BatchQueueStatus::Queued => true,
                BatchQueueStatus::Unknown(s) => {
                    error!("Unknown batch queue status? {s}");
                    false
                }
            })
            .map(|q| Self {
                queue: q.queue,
                status: q.status,
                job_name: q.job_name,
            })
            .collect())
    }
}

#[allow(unused)]
#[derive(Debug)]
struct BatchQueues {
    queue: i64,
    status: BatchQueueStatus,
    device: String,
    job_seq: Option<u64>,
    job_name: String,
    user: String,
}

impl BatchQueues {
    async fn try_new(ssh: Arc<RwLock<SshClient>>) -> anyhow::Result<Vec<Self>> {
        let command_response = Self::get_batch_running(ssh).await?;

        Self::try_from_command(command_response)
    }

    async fn get_batch_running(ssh: Arc<RwLock<SshClient>>) -> anyhow::Result<Vec<String>> {
        let ssh = ssh.read().await;
        let queues = ssh.call("/SYM/MACROS/QUEUECONTROL -BATCH").await;
        let queues = if let Err(e) = queues {
            error!("{e:#?}");
            return Err(e).context("Could not retrieve batch queues");
        } else {
            queues?
        };
        trace!("Queuecontrol command returned '{queues:#?}'");

        let queues = queues.0.iter().map(String::from).collect::<Vec<String>>();

        Ok(queues)
    }

    fn try_from_command(value: Vec<String>) -> anyhow::Result<Vec<Self>> {
        let mut value_iter = value.iter();
        let _header = value_iter.next();

        let Some(dash_line) = value_iter.next() else {
            return Err(anyhow!("No dashed line found in values"));
        };
        let lengths = dash_line.split_whitespace();
        if lengths.clone().count() != 6 {
            return Err(anyhow!("Not enough dashed lines for line lengths?"));
        }
        let mut tot_length = 0;
        let lengths = lengths
            .map(|s| {
                let len = s.len() + 1;
                tot_length += len;
                tot_length
            })
            .collect::<Vec<usize>>();
        trace!("Command position/length indexes {lengths:?}");

        let mut res = Vec::new();
        let mut queue = -1;
        for batch_line in value_iter {
            let q_line = batch_line[..lengths[0]]
                .trim()
                .chars()
                .filter(|c| c.is_numeric())
                .collect::<String>();
            if !q_line.is_empty() {
                queue = q_line.parse()?;
            }

            let (job_seq, job_name, user) = if batch_line.len() > lengths[4] {
                (
                    Some(
                        batch_line[lengths[2]..lengths[3]]
                            .trim()
                            .to_string()
                            .parse::<u64>()?,
                    ),
                    batch_line[lengths[3]..lengths[4]].trim().to_string(),
                    batch_line[lengths[4]..].trim().to_string(),
                )
            } else {
                (None, String::new(), String::new())
            };

            res.push(BatchQueues {
                queue,

                device: batch_line[lengths[0]..lengths[1]].trim().into(),
                status: batch_line[lengths[1]..lengths[2].min(batch_line.len())]
                    .trim()
                    .into(),
                job_seq,
                job_name,
                user,
            });
        }

        trace!("BatchQueues: {res:#?}");

        Ok(res)
    }
}

#[derive(Debug)]
enum BatchQueueStatus {
    Ready,
    Running,
    Queued,
    Unknown(String),
}

impl From<&str> for BatchQueueStatus {
    fn from(value: &str) -> Self {
        match value.to_lowercase().trim() {
            "ready" => Self::Ready,
            "running" => Self::Running,
            "queued" => Self::Queued,
            _ => Self::Unknown(value.to_string()),
        }
    }
}

struct DataSupp {
    accounts: u64,
    loans: u64,
    shares: u64,
}

impl Display for DataSupp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            r#"AccountDataSupp:{:012}
LoanDataSupp:{:012}
ShareDataSupp:{:012}
"#,
            self.accounts, self.loans, self.shares
        )
    }
}

fn combine_datasupp_stats(files: &mut Vec<ExtractFile>) {
    let mut datasupp_out = DataSupp {
        accounts: 0,
        loans: 0,
        shares: 0,
    };

    let mut datasupps = Vec::new();
    for (i, _) in files
        .iter()
        .enumerate()
        .filter(|(_, f)| f.file_name == "Episys_DataSupp_Statistics.txt")
    {
        datasupps.push(i)
    }
    if datasupps.len() == 1 {
        return;
    }

    let mut datasupp_files = datasupps
        .iter()
        .map(|i| files.swap_remove(*i))
        .collect::<Vec<ExtractFile>>();
    datasupp_files.sort_by_key(|f| f.extract_part);

    // Process PreClose file content
    let content = read_to_string(&datasupp_files[0].local_path).unwrap_or_else(|_| {
        panic!(
            "Cannot read datasupp file at '{}'",
            datasupp_files[0].local_path
        )
    });
    let content = content.lines().collect::<Vec<&str>>();

    for line in content {
        let pos = line
            .find(':')
            .expect("Episys_DataSupp_Statistics.txt malformed");
        let value = line[pos + 1..]
            .parse::<u64>()
            .expect("Episys_DataSupp_Statistics.txt malformed");

        if line.starts_with('A') {
            datasupp_out.accounts += value
        } else if line.starts_with('L') {
            datasupp_out.loans += value
        } else if line.starts_with('S') {
            datasupp_out.shares += value
        };
    }

    // Process PostClose file content
    let content = read_to_string(&datasupp_files[1].local_path).unwrap_or_else(|_| {
        panic!(
            "Cannot read datasupp file at '{}'",
            datasupp_files[1].local_path
        )
    });
    let content = content.lines().collect::<Vec<&str>>();

    for line in content {
        let pos = line
            .find(':')
            .expect("Episys_DataSupp_Statistics.txt malformed");
        let value = line[pos + 1..]
            .parse::<u64>()
            .expect("Episys_DataSupp_Statistics.txt malformed");

        if line.starts_with("AccountDataSupp") && datasupp_out.accounts == 0 {
            datasupp_out.accounts = value
        } else if line.starts_with("LoanDataSupp") && datasupp_out.loans == 0 {
            datasupp_out.loans += value
        } else if line.starts_with("ShareDataSupp") && datasupp_out.shares == 0 {
            datasupp_out.shares += value
        };
    }

    let mut output_file = datasupp_files
        .pop()
        .expect("Impossible empty datasupp_files");
    output_file.local_path = format!(r#".\extracts\{}"#, output_file.file_name);

    std::fs::write(&output_file.local_path, datasupp_out.to_string())
        .expect("Could not write computed datasupp file");

    files.push(output_file);
}
