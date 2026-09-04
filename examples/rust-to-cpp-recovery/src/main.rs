//! Durable Rust sender recovery against a C++/MsQuic Layer 0 server.

use anyhow::{Context, Result, bail};
use clap::{Parser, Subcommand};
use pipestream_quic::transport::{ClientOptions, send};
use sha2::{Digest, Sha256};
use std::{
    fs,
    io::Write,
    net::SocketAddr,
    path::{Path, PathBuf},
};
use tempfile::NamedTempFile;

const JOURNAL_MAGIC: &[u8; 8] = b"PSRJ0001";
const JOURNAL_LENGTH: usize = 8 + 1 + 4 + 32;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum State {
    Pending = 0,
    Complete = 1,
}

#[derive(Debug, Clone, Copy)]
enum InstallMode {
    Create,
    Replace,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Journal {
    state: State,
    entity_id: u32,
    digest: [u8; 32],
}

#[derive(Debug, Parser)]
#[command(about = "Prepare or recover a durable PipeStream Layer 0 send")]
struct Arguments {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Persist an immutable sender record before attempting transport.
    Prepare {
        #[arg(long)]
        journal: PathBuf,
        #[arg(long)]
        input: PathBuf,
        #[arg(long)]
        entity_id: u32,
    },
    /// Validate and replay a pending record to a C++ or compatible server.
    Recover {
        #[arg(long)]
        journal: PathBuf,
        #[arg(long)]
        input: PathBuf,
        #[arg(long)]
        connect: SocketAddr,
        #[arg(long)]
        ca: PathBuf,
        #[arg(long, default_value = "localhost")]
        server_name: String,
        #[arg(long, default_value = "application/octet-stream")]
        content_type: String,
        #[arg(long)]
        parent_id: Option<u32>,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    match Arguments::parse().command {
        Command::Prepare {
            journal,
            input,
            entity_id,
        } => {
            validate_entity_id(entity_id)?;
            let digest = digest_file(&input)?;
            write_journal(
                &journal,
                &Journal {
                    state: State::Pending,
                    entity_id,
                    digest,
                },
                InstallMode::Create,
            )?;
            println!("RUST RECOVERY PREPARED entity={entity_id}");
        }
        Command::Recover {
            journal,
            input,
            connect,
            ca,
            server_name,
            content_type,
            parent_id,
        } => {
            let mut record = read_journal(&journal)?;
            if record.state != State::Pending {
                bail!("journal is already complete");
            }
            if digest_file(&input)? != record.digest {
                bail!("staged entity does not match its durable digest");
            }
            send(ClientOptions {
                remote: connect,
                ca_certificate: ca,
                server_name,
                entity_id: record.entity_id,
                input,
                content_type,
                parent_id,
            })
            .await
            .context("replay pending entity")?;
            record.state = State::Complete;
            write_journal(&journal, &record, InstallMode::Replace)?;
            println!("RUST RECOVERY COMPLETE entity={}", record.entity_id);
        }
    }
    Ok(())
}

fn validate_entity_id(entity_id: u32) -> Result<()> {
    if entity_id == 0 || entity_id > pipestream_quic::MAX_ENTITY_ID {
        bail!("entity ID is reserved");
    }
    Ok(())
}

fn digest_file(path: &Path) -> Result<[u8; 32]> {
    let bytes = fs::read(path).with_context(|| format!("read {}", path.display()))?;
    Ok(Sha256::digest(bytes).into())
}

fn encode_journal(record: &Journal) -> [u8; JOURNAL_LENGTH] {
    let mut bytes = [0u8; JOURNAL_LENGTH];
    bytes[..8].copy_from_slice(JOURNAL_MAGIC);
    bytes[8] = record.state as u8;
    bytes[9..13].copy_from_slice(&record.entity_id.to_be_bytes());
    bytes[13..].copy_from_slice(&record.digest);
    bytes
}

fn decode_journal(bytes: &[u8]) -> Result<Journal> {
    if bytes.len() != JOURNAL_LENGTH || &bytes[..8] != JOURNAL_MAGIC {
        bail!("invalid recovery journal format");
    }
    let state = match bytes[8] {
        0 => State::Pending,
        1 => State::Complete,
        value => bail!("invalid recovery journal state {value}"),
    };
    let entity_id = u32::from_be_bytes(bytes[9..13].try_into().expect("fixed journal field"));
    let digest = bytes[13..].try_into().expect("fixed journal field");
    Ok(Journal {
        state,
        entity_id,
        digest,
    })
}

fn read_journal(path: &Path) -> Result<Journal> {
    decode_journal(&fs::read(path).with_context(|| format!("read {}", path.display()))?)
}

fn write_journal(path: &Path, record: &Journal, mode: InstallMode) -> Result<()> {
    let parent = path
        .parent()
        .filter(|value| !value.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    fs::create_dir_all(parent).with_context(|| format!("create {}", parent.display()))?;
    let mut temporary =
        NamedTempFile::new_in(parent).context("create recovery journal temp file")?;
    temporary
        .write_all(&encode_journal(record))
        .context("write recovery journal")?;
    temporary
        .as_file()
        .sync_all()
        .context("sync recovery journal")?;
    let installed = match mode {
        InstallMode::Create => temporary.persist_noclobber(path),
        InstallMode::Replace => temporary.persist(path),
    };
    installed
        .map_err(|error| error.error)
        .with_context(|| format!("install {}", path.display()))?;
    fs::File::open(parent)
        .with_context(|| format!("open {} for directory sync", parent.display()))?
        .sync_all()
        .with_context(|| format!("sync {}", parent.display()))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn journal_round_trip_is_exact() {
        let expected = Journal {
            state: State::Pending,
            entity_id: 201,
            digest: [0x5a; 32],
        };
        assert_eq!(
            expected,
            decode_journal(&encode_journal(&expected)).unwrap()
        );
    }

    #[test]
    fn corrupt_journal_is_refused() {
        let mut bytes = encode_journal(&Journal {
            state: State::Pending,
            entity_id: 201,
            digest: [0x5a; 32],
        });
        bytes[0] ^= 1;
        assert_eq!(
            "invalid recovery journal format",
            decode_journal(&bytes).unwrap_err().to_string()
        );
    }

    #[test]
    fn prepare_does_not_replace_an_existing_journal() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("sender.journal");
        let original = Journal {
            state: State::Complete,
            entity_id: 201,
            digest: [0x5a; 32],
        };
        write_journal(&path, &original, InstallMode::Create).unwrap();
        let replacement = Journal {
            state: State::Pending,
            entity_id: 202,
            digest: [0xa5; 32],
        };
        assert!(write_journal(&path, &replacement, InstallMode::Create).is_err());
        assert_eq!(original, read_journal(&path).unwrap());
    }

    #[test]
    fn reserved_entity_ids_are_refused() {
        assert!(validate_entity_id(0).is_err());
        assert!(validate_entity_id(1).is_ok());
        assert!(validate_entity_id(pipestream_quic::MAX_ENTITY_ID).is_ok());
        assert!(validate_entity_id(pipestream_quic::MAX_ENTITY_ID + 1).is_err());
    }
}
