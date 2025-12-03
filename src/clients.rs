use std::borrow::Cow;
use std::fmt::Display;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use russh::client::{self, Handle, KeyboardInteractiveAuthResponse};
use russh::{ChannelId, keys::PublicKey};
use russh::{ChannelMsg, Disconnect, Preferred};
use russh_sftp::client::SftpSession;
use russh_sftp::client::fs::ReadDir;
use tracing::{debug, error, trace};

use crate::config::Config;

pub struct SftpClient {
    session: SftpSession,
    closed: bool,
}

impl SftpClient {
    pub async fn connect(config: &Config) -> anyhow::Result<Self> {
        debug!("Connecting to host '{}'", config.sftp.hostname);
        // let russh_config = russh::client::Config {
        //     channel_buffer_size: 400,
        //     ..Default::default()
        // };
        let russh_config = client::Config {
            channel_buffer_size: 400,
            // inactivity_timeout: Some(Duration::from_secs(5)),
            preferred: Preferred {
                kex: Cow::Owned(vec![
                    russh::kex::CURVE25519,
                    russh::kex::CURVE25519_PRE_RFC_8731,
                    russh::kex::EXTENSION_SUPPORT_AS_CLIENT,
                ]),
                ..Default::default()
            },
            ..<_>::default()
        };

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

        trace!("Connected SFTP session");
        Ok(Self {
            session: SftpSession::new(channel.into_stream()).await?,
            closed: false,
        })
    }

    pub async fn read<P: Into<String>>(&self, path: P) -> anyhow::Result<Vec<u8>> {
        Ok(self.session.read(path).await?)
    }

    pub async fn close(&mut self) -> anyhow::Result<()> {
        self.session
            .close()
            .await
            .context("Could not close session")?;
        self.closed = true;
        Ok(())
    }

    pub async fn is_closed(&self) -> bool {
        self.closed
    }

    pub async fn read_dir(&self, path: &String) -> anyhow::Result<ReadDir> {
        Ok(self.session.read_dir(path).await?)
    }
}

pub struct SshClient {
    pub session: Handle<Client>,
}

impl SshClient {
    pub async fn connect(config: &Config) -> anyhow::Result<Self> {
        println!("Connecting to host '{}'", config.sftp.hostname);
        // let russh_config = russh::client::Config::default();
        let russh_config = client::Config {
            // inactivity_timeout: Some(Duration::from_secs(5)),
            preferred: Preferred {
                kex: Cow::Owned(vec![
                    russh::kex::CURVE25519,
                    russh::kex::CURVE25519_PRE_RFC_8731,
                    russh::kex::EXTENSION_SUPPORT_AS_CLIENT,
                ]),
                ..Default::default()
            },
            ..<_>::default()
        };

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
                    println!("Keyboard authentication successful");
                    break;
                }
                KeyboardInteractiveAuthResponse::Failure { .. } => {
                    println!("Keyboard authentication failure");
                    return Err(russh::Error::NotAuthenticated.into());
                }
                KeyboardInteractiveAuthResponse::InfoRequest {
                    ref name,
                    ref instructions,
                    ref prompts,
                } => {
                    println!("Received {name}, {instructions}. Prompts {:?} ", prompts);

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

        trace!("Connected SSH session");
        Ok(Self { session })
    }

    pub async fn is_closed(&self) -> bool {
        self.session.is_closed()
    }

    pub async fn call<T: Display>(&self, command: T) -> anyhow::Result<(Vec<String>, u32)> {
        trace!("Sending '{command}'");
        let mut channel = self.session.channel_open_session().await?;
        channel.exec(true, command.to_string()).await?;

        let mut code = None;
        let mut command_stdout = Vec::new();

        loop {
            // There's an event available on the session channel
            let Some(msg) = channel.wait().await else {
                break;
            };
            match msg {
                // Write data to the terminal
                ChannelMsg::Data { ref data } => {
                    command_stdout.push(data.to_vec());
                }
                // The command has returned an exit code
                ChannelMsg::ExitStatus { exit_status } => {
                    code = Some(exit_status);
                    // cannot leave the loop immediately, there might still be more data to receive
                }
                _ => {}
            }
        }

        let mut command_out = Vec::new();
        for line in command_stdout {
            let utf8_line = String::from_utf8(line)?;
            trace!("received line from command: '{utf8_line}'");
            let mut utf8_lines = utf8_line.lines().map(String::from).collect();
            command_out.append(&mut utf8_lines);
        }

        Ok((command_out, code.expect("program did not exit cleanly")))
    }

    pub async fn close(&mut self) -> anyhow::Result<()> {
        self.session
            .disconnect(Disconnect::ByApplication, "", "English")
            .await?;
        Ok(())
    }
}

pub struct Client {}

impl client::Handler for Client {
    type Error = anyhow::Error;

    async fn check_server_key(
        &mut self,
        _server_public_key: &PublicKey,
    ) -> Result<bool, Self::Error> {
        trace!("check_server_key: {:?}", _server_public_key);
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
