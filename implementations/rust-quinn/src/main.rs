use anyhow::Result;
use clap::{Parser, Subcommand};
use pipestream_quic::transport::{ClientOptions, ServerOptions, send, serve};
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
    }
}
