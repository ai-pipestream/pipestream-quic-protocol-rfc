use anyhow::Result;
use clap::{Parser, Subcommand};
use pipestream_quic::{
    ClaimRedemption,
    authentication::{AuthenticationPolicy, ClientIdentity},
    recursive::{
        ExemplarProcessor, MAX_CHUNKS_PER_ENTITY, RecursiveClientOptions, RecursiveServerOptions,
        begin_durable_yield, finish_durable_yield, run_recursive_scenario, run_sealed_scenario,
        serve_recursive_authenticated,
    },
    transport::{ClientOptions, ServerOptions, send, serve},
};
use std::{net::SocketAddr, path::PathBuf};

#[derive(Debug, Parser)]
#[command(name = "pipestream-quinn")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    Serve {
        #[arg(long, default_value = "127.0.0.1:0")]
        bind: SocketAddr,
        #[arg(long)]
        cert: PathBuf,
        #[arg(long)]
        key: PathBuf,
        #[arg(long)]
        output_dir: PathBuf,
        #[arg(long)]
        ready_file: Option<PathBuf>,
        #[arg(long, default_value_t = false)]
        once: bool,
    },
    Send {
        #[arg(long)]
        connect: SocketAddr,
        #[arg(long)]
        ca: PathBuf,
        #[arg(long, default_value = "localhost")]
        server_name: String,
        #[arg(long)]
        entity_id: u32,
        #[arg(long)]
        input: PathBuf,
        #[arg(long, default_value = "application/octet-stream")]
        content_type: String,
        #[arg(long)]
        parent_id: Option<u32>,
    },
    ServeRecursive {
        #[arg(long, default_value = "127.0.0.1:0")]
        bind: SocketAddr,
        #[arg(long)]
        cert: PathBuf,
        #[arg(long)]
        key: PathBuf,
        #[arg(long)]
        state_db: PathBuf,
        #[arg(long)]
        entity_dir: PathBuf,
        #[arg(long)]
        ready_file: Option<PathBuf>,
        #[arg(long, default_value_t = false)]
        once: bool,
        #[arg(long, default_value_t = 7)]
        max_scope_depth: u8,
        #[arg(long, default_value_t = 1_000_000)]
        max_entities_per_scope: u32,
        #[arg(long, default_value_t = pipestream_quic::MAX_PAYLOAD)]
        max_entity_bytes: usize,
        #[arg(long, default_value_t = MAX_CHUNKS_PER_ENTITY)]
        max_chunks_per_entity: u64,
        #[arg(long, default_value_t = 256)]
        max_concurrent_connections: usize,
        #[arg(long, requires_all = ["authority", "principal_map"])]
        client_ca: Option<PathBuf>,
        #[arg(long, requires_all = ["client_ca", "principal_map"])]
        authority: Option<String>,
        #[arg(long, requires_all = ["client_ca", "authority"])]
        principal_map: Option<PathBuf>,
    },
    RecursiveScenario {
        #[command(flatten)]
        connection: ConnectionArgs,
        #[arg(long)]
        session_id: String,
    },
    SealedScenario {
        #[command(flatten)]
        connection: ConnectionArgs,
        #[arg(long)]
        session_id: String,
    },
    BeginYield {
        #[command(flatten)]
        connection: ConnectionArgs,
        #[arg(long)]
        session_id: String,
    },
    Redeem {
        #[command(flatten)]
        connection: ConnectionArgs,
        #[arg(long)]
        session_id: String,
        #[arg(long)]
        claim_id: u64,
        #[arg(long)]
        state_checksum: String,
    },
}

#[derive(Debug, clap::Args)]
struct ConnectionArgs {
    #[arg(long)]
    connect: SocketAddr,
    #[arg(long)]
    ca: PathBuf,
    #[arg(long, default_value = "localhost")]
    server_name: String,
    #[arg(long, requires = "client_key")]
    client_cert: Option<PathBuf>,
    #[arg(long, requires = "client_cert")]
    client_key: Option<PathBuf>,
}

impl ConnectionArgs {
    fn options(self) -> RecursiveClientOptions {
        RecursiveClientOptions {
            remote: self.connect,
            ca_certificate: self.ca,
            server_name: self.server_name,
            identity: self
                .client_cert
                .zip(self.client_key)
                .map(|(certificate, private_key)| ClientIdentity {
                    certificate,
                    private_key,
                }),
        }
    }
}

#[tokio::main]
async fn main() {
    if let Err(error) = run(Cli::parse()).await {
        eprintln!("{error:#}");
        std::process::exit(1);
    }
}

async fn run(cli: Cli) -> Result<()> {
    match cli.command {
        Command::Serve {
            bind,
            cert,
            key,
            output_dir,
            ready_file,
            once,
        } => {
            serve(ServerOptions {
                bind,
                certificate: cert,
                private_key: key,
                output_directory: output_dir,
                ready_file,
                once,
            })
            .await
        }
        Command::Send {
            connect,
            ca,
            server_name,
            entity_id,
            input,
            content_type,
            parent_id,
        } => {
            send(ClientOptions {
                remote: connect,
                ca_certificate: ca,
                server_name,
                entity_id,
                input,
                content_type,
                parent_id,
            })
            .await
        }
        Command::ServeRecursive {
            bind,
            cert,
            key,
            state_db,
            entity_dir,
            ready_file,
            once,
            max_scope_depth,
            max_entities_per_scope,
            max_entity_bytes,
            max_chunks_per_entity,
            max_concurrent_connections,
            client_ca,
            authority,
            principal_map,
        } => {
            let authentication = match (client_ca, authority, principal_map) {
                (None, None, None) => None,
                (Some(ca), Some(authority), Some(map)) => {
                    Some(AuthenticationPolicy::from_files(authority, &ca, &map)?)
                }
                _ => anyhow::bail!(
                    "client-ca, authority, and principal-map must be supplied together"
                ),
            };
            serve_recursive_authenticated(
                RecursiveServerOptions {
                    bind,
                    certificate: cert,
                    private_key: key,
                    state_database: state_db,
                    entity_directory: entity_dir,
                    ready_file,
                    once,
                    max_scope_depth,
                    max_entities_per_scope,
                    max_entity_bytes,
                    max_chunks_per_entity,
                    max_concurrent_connections,
                },
                ExemplarProcessor::default(),
                authentication,
            )
            .await
        }
        Command::RecursiveScenario {
            connection,
            session_id,
        } => {
            let result = run_recursive_scenario(&connection.options(), &session_id).await?;
            println!(
                "RECURSIVE_OK {} {} {}",
                session_id,
                result.nested_digest.entities_processed,
                result.child_digest.entities_processed
            );
            Ok(())
        }
        Command::SealedScenario {
            connection,
            session_id,
        } => {
            tokio::time::timeout(
                std::time::Duration::from_secs(30),
                run_sealed_scenario(&connection.options(), &session_id),
            )
            .await??;
            println!("SEALED_OK {session_id}");
            Ok(())
        }
        Command::BeginYield {
            connection,
            session_id,
        } => {
            let claim = begin_durable_yield(&connection.options(), &session_id).await?;
            println!(
                "CLAIM {} {} {}",
                claim.session_id,
                claim.claim_id,
                encode_hex(&claim.state_checksum)
            );
            Ok(())
        }
        Command::Redeem {
            connection,
            session_id,
            claim_id,
            state_checksum,
        } => {
            finish_durable_yield(
                &connection.options(),
                &ClaimRedemption {
                    session_id,
                    claim_id,
                    state_checksum: decode_checksum(&state_checksum)?,
                    acknowledged: false,
                },
            )
            .await?;
            println!("REDEEMED {claim_id}");
            Ok(())
        }
    }
}

fn encode_hex(bytes: &[u8; 32]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(64);
    for byte in bytes {
        output.push(char::from(HEX[(byte >> 4) as usize]));
        output.push(char::from(HEX[(byte & 0x0f) as usize]));
    }
    output
}

fn decode_checksum(value: &str) -> Result<[u8; 32]> {
    if value.len() != 64 {
        anyhow::bail!("state checksum must contain 64 hexadecimal characters");
    }
    let mut output = [0; 32];
    for (index, chunk) in value.as_bytes().chunks_exact(2).enumerate() {
        let text = std::str::from_utf8(chunk)?;
        output[index] = u8::from_str_radix(text, 16)?;
    }
    Ok(output)
}
