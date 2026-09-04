//! Rust coordinator that scatters one payload across three Layer 0 servers.

use anyhow::{Context, Result, bail};
use clap::Parser;
use pipestream_quic::transport::{ClientOptions, send};
use std::{
    fs,
    net::SocketAddr,
    path::{Path, PathBuf},
};

#[derive(Debug, Parser)]
#[command(about = "Scatter one payload across Java, Rust, and C++ Layer 0 servers")]
struct Arguments {
    #[arg(long)]
    input: PathBuf,
    #[arg(long)]
    ca: PathBuf,
    #[arg(long, default_value = "localhost")]
    server_name: String,
    #[arg(long)]
    java_server: SocketAddr,
    #[arg(long)]
    rust_server: SocketAddr,
    #[arg(long)]
    cpp_server: SocketAddr,
    #[arg(long)]
    java_output: PathBuf,
    #[arg(long)]
    rust_output: PathBuf,
    #[arg(long)]
    cpp_output: PathBuf,
    #[arg(long, default_value_t = 77)]
    parent_id: u32,
    #[arg(long, default_value_t = 301)]
    first_entity_id: u32,
}

#[tokio::main]
async fn main() -> Result<()> {
    let arguments = Arguments::parse();
    let payload = fs::read(&arguments.input)
        .with_context(|| format!("read {}", arguments.input.display()))?;
    let chunks = split(&payload);
    let temporary = tempfile::tempdir().context("create chunk staging directory")?;
    let paths = [
        temporary.path().join("java.bin"),
        temporary.path().join("rust.bin"),
        temporary.path().join("cpp.bin"),
    ];
    for (path, chunk) in paths.iter().zip(chunks.iter()) {
        fs::write(path, chunk).with_context(|| format!("stage {}", path.display()))?;
    }

    let [java_id, rust_id, cpp_id] = entity_ids(arguments.first_entity_id)?;
    let java = client_options(&arguments, arguments.java_server, java_id, paths[0].clone());
    let rust = client_options(&arguments, arguments.rust_server, rust_id, paths[1].clone());
    let cpp = client_options(&arguments, arguments.cpp_server, cpp_id, paths[2].clone());
    tokio::try_join!(send(java), send(rust), send(cpp)).context("scatter entities")?;

    let received = [
        read_child(&arguments.java_output, java_id, arguments.parent_id)?,
        read_child(&arguments.rust_output, rust_id, arguments.parent_id)?,
        read_child(&arguments.cpp_output, cpp_id, arguments.parent_id)?,
    ]
    .concat();
    if received != payload {
        bail!("rehydrated payload differs from the input");
    }
    println!(
        "RUST SCATTER COMPLETE parent={} entities={java_id},{rust_id},{cpp_id}",
        arguments.parent_id
    );
    Ok(())
}

fn client_options(
    arguments: &Arguments,
    remote: SocketAddr,
    entity_id: u32,
    input: PathBuf,
) -> ClientOptions {
    ClientOptions {
        remote,
        ca_certificate: arguments.ca.clone(),
        server_name: arguments.server_name.clone(),
        entity_id,
        input,
        content_type: "application/octet-stream".to_owned(),
        parent_id: Some(arguments.parent_id),
    }
}

fn entity_ids(first: u32) -> Result<[u32; 3]> {
    let second = first.checked_add(1).context("entity ID range overflow")?;
    let third = first.checked_add(2).context("entity ID range overflow")?;
    if first == 0 || third > pipestream_quic::MAX_ENTITY_ID {
        bail!("three consecutive assignable entity IDs are required");
    }
    Ok([first, second, third])
}

fn split(payload: &[u8]) -> [&[u8]; 3] {
    let first = payload.len() / 3;
    let second = first + (payload.len() - first) / 2;
    [
        &payload[..first],
        &payload[first..second],
        &payload[second..],
    ]
}

fn read_child(output: &Path, entity_id: u32, parent_id: u32) -> Result<Vec<u8>> {
    let observed_parent = fs::read_to_string(output.join(format!("{entity_id}.parent")))
        .with_context(|| format!("read parent identity for entity {entity_id}"))?;
    if observed_parent.trim() != parent_id.to_string() {
        bail!("entity {entity_id} lost parent identity {parent_id}");
    }
    fs::read(output.join(format!("{entity_id}.bin")))
        .with_context(|| format!("read received entity {entity_id}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn split_reassembles_exactly() {
        for length in 0..16 {
            let input: Vec<_> = (0..length).collect();
            assert_eq!(input, split(&input).concat());
        }
    }

    #[test]
    fn reserved_entity_range_is_refused() {
        assert!(entity_ids(0).is_err());
        assert!(entity_ids(pipestream_quic::MAX_ENTITY_ID - 2).is_ok());
        assert!(entity_ids(pipestream_quic::MAX_ENTITY_ID - 1).is_err());
        assert!(entity_ids(pipestream_quic::MAX_ENTITY_ID).is_err());
    }
}
