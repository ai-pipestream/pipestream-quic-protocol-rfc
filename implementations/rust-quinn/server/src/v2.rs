//! Runnable V2 endpoints and typed durable client commands.
use anyhow::{Result, bail};
use clap::{Args, Subcommand};
use pipestream_quic::{
    persistence::PhysicalLimits,
    v2::*,
    v2_client::{journal, session, transport},
};
use std::{net::SocketAddr, path::PathBuf};

mod applications;
mod client;
mod configuration;
mod server;

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Explicitly initialize new authority history and its paired object root.
    InitAuthority {
        #[command(flatten)]
        storage: configuration::Storage,
    },
    /// Open existing history. Missing or incompatible state is an error.
    Serve {
        #[command(flatten)]
        storage: configuration::Storage,
        #[arg(long, default_value = "127.0.0.1:0")]
        bind: SocketAddr,
        #[arg(long)]
        cert: PathBuf,
        #[arg(long)]
        key: PathBuf,
        #[arg(long)]
        client_ca: PathBuf,
        /// Trusted DNS/IP authority and port used in result locators.
        #[arg(long)]
        result_authority: String,
        #[arg(long)]
        ready_file: Option<PathBuf>,
        #[arg(long, default_value_t=16*1024*1024)]
        object_limit: u64,
    },
    /// Read the authenticated owner's sequence without creating a session.
    NextSequence {
        #[command(flatten)]
        connection: Connection,
    },
    /// Persist original creation intent locally without transmitting it.
    InitClient {
        #[command(flatten)]
        journal: ClientJournal,
    },
    /// Reopen original intent, then replay creation or attach to that binding.
    Client {
        #[command(flatten)]
        journal: ClientJournal,
        #[command(flatten)]
        connection: Connection,
        #[command(subcommand)]
        operation: client::Operation,
    },
    /// Operator action against existing history. Does not create a session.
    Revoke {
        #[command(flatten)]
        storage: configuration::Storage,
        #[arg(long)]
        owner: String,
        #[arg(long)]
        generation: u64,
    },
}

#[derive(Debug, Args)]
pub struct Connection {
    #[arg(long)]
    connect: SocketAddr,
    #[arg(long)]
    ca: PathBuf,
    #[arg(long)]
    cert: PathBuf,
    #[arg(long)]
    key: PathBuf,
    #[arg(long, default_value = "localhost")]
    server_name: String,
    #[arg(long, default_value_t=16*1024*1024)]
    object_limit: u64,
}
impl Connection {
    fn endpoint(&self) -> Result<session::Endpoint> {
        let mut options = transport::Options::default();
        options.offer.object_limit = Number(self.object_limit);
        Ok(session::Endpoint {
            local: if self.connect.is_ipv4() {
                "0.0.0.0:0"
            } else {
                "[::]:0"
            }
            .parse()?,
            remote: self.connect,
            server_name: self.server_name.clone(),
            security: transport::Security::new(
                configuration::roots(&self.ca)?,
                Some((
                    configuration::certificates(&self.cert)?,
                    configuration::key(&self.key)?,
                )),
            )?,
            transport: options,
        })
    }
}

#[derive(Debug, Args)]
pub struct ClientJournal {
    #[arg(long)]
    journal: PathBuf,
    #[arg(long)]
    authority: String,
    #[arg(long)]
    owner: String,
    #[arg(long)]
    creation_sequence: u64,
    #[arg(long, default_value_t = 60000)]
    max_execution_ms: u64,
    #[arg(long, default_value_t = 3600000)]
    output_retention_ms: u64,
    #[arg(long, default_value_t = 86400000)]
    receipt_retention_ms: u64,
    /// Omit result delivery from the immutable profile combination.
    #[arg(long)]
    no_results: bool,
}
impl ClientJournal {
    fn creation(&self) -> journal::Creation {
        journal::Creation {
            authority: IdentityLabel(self.authority.clone()),
            owner: IdentityLabel(self.owner.clone()),
            creation_sequence: Id(self.creation_sequence),
            policy: Policy {
                execution_limit_ms: Duration(self.max_execution_ms),
                output_retention_ms: Duration(self.output_retention_ms),
                receipt_retention_ms: Duration(self.receipt_retention_ms),
            },
            results: !self.no_results,
        }
    }
    async fn open(&self, fresh: bool) -> Result<journal::Journal> {
        let creation = self.creation();
        creation.request(Id(1))?;
        Ok(if fresh {
            journal::Journal::initialize(
                self.journal.clone(),
                creation,
                journal::JournalLimits::default(),
                PhysicalLimits::default(),
                journal::Options::default(),
            )
            .await?
        } else {
            journal::Journal::open(
                self.journal.clone(),
                creation,
                journal::JournalLimits::default(),
                PhysicalLimits::default(),
                journal::Options::default(),
            )
            .await?
        })
    }
}

pub async fn run(command: Command) -> Result<()> {
    match command {
        Command::InitAuthority { storage } => {
            let _ = storage.open(true)?;
            println!("AUTHORITY_INITIALIZED {}", storage.authority);
        }
        Command::Serve {
            storage,
            bind,
            cert,
            key,
            client_ca,
            result_authority,
            ready_file,
            object_limit,
        } => {
            server::serve(
                storage,
                server::Network {
                    bind,
                    cert,
                    key,
                    client_ca,
                    result_authority,
                    ready_file,
                    object_limit,
                },
            )
            .await?;
        }
        Command::NextSequence { connection } => {
            let mut endpoint = connection.endpoint()?;
            endpoint.transport.offer.supported = vec![ProfileId(DURABLE_WORK.into())];
            endpoint.transport.offer.required = endpoint.transport.offer.supported.clone();
            let transport = transport::Transport::connect(
                endpoint.local,
                endpoint.remote,
                &endpoint.server_name,
                endpoint.security,
                endpoint.transport,
            )
            .await?;
            let reply = transport
                .exchange(
                    Control::Session(Session::NextSequence { request: Id(1) }),
                    None,
                )
                .await;
            transport.close();
            transport.closed().await;
            match reply? {
                transport::Reply::Control(Control::Session(Session::Sequence {
                    next_creation_sequence,
                    ..
                })) => println!("NEXT_SEQUENCE {}", next_creation_sequence.0),
                transport::Reply::Control(Control::Refusal(r)) => {
                    return Err(session::Failure::Refused(r).into());
                }
                _ => bail!("invalid creation-sequence response"),
            }
        }
        Command::InitClient { journal } => {
            let stored = journal.open(true).await?;
            stored.shutdown().await?;
            println!(
                "CLIENT_INITIALIZED {} {} {}",
                journal.authority, journal.owner, journal.creation_sequence
            );
        }
        Command::Client {
            journal,
            connection,
            operation,
        } => {
            let endpoint = connection.endpoint()?;
            let client = session::Client::connect(
                endpoint,
                journal.open(false).await?,
                session::Options::default(),
            )
            .await?;
            let result = client::run(&client, operation, connection.object_limit).await;
            let closed = client.shutdown().await;
            result?;
            closed?;
        }
        Command::Revoke {
            storage,
            owner,
            generation,
        } => {
            let (store, _payloads, _) = storage.open(false)?;
            store.revoke_session(&SessionIdentity {
                authority: IdentityLabel(storage.authority),
                owner: IdentityLabel(owner),
                generation: Id(generation),
            })?;
            println!("SESSION_REVOKED {generation}");
        }
    }
    Ok(())
}
