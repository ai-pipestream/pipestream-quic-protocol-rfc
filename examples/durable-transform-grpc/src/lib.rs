//! Shared durable core for the streaming-gRPC baseline.
//!
//! Implements the application guarantees gRPC transport alone does not
//! supply (contract section 8): immutable operation identities with
//! params digests, commit-before-ACK receipts, attempt fencing, manifest
//! commitments, read pins, and replayable cleanup. SQLite is the durable
//! store on both baseline processes; the PipeStream arm comparison notes
//! any residual storage-setting difference explicitly.
use anyhow::{Context, Result, bail};
use rusqlite::{Connection, params};
use sha2::{Digest as _, Sha256};
use std::path::Path;

pub mod proto {
    tonic::include_proto!("pipestream.workload.v1");
}

/// Terminal outcome names shared by worker replies and coordinator logic.
pub const COMMITTED: &str = "COMMITTED";
pub const RETRYABLE: &str = "RETRYABLE";
pub const CONFLICT: &str = "CONFLICT";
pub const EXPIRED: &str = "EXPIRED";
pub const UNAUTHORIZED: &str = "UNAUTHORIZED";
pub const NOT_FOUND: &str = "NOT_FOUND";
pub const CANCELLED: &str = "CANCELLED";

/// SQLite durability contract (section 8/9): WAL for crash-safe commits,
/// FULL synchronous, commit-before-ACK everywhere below.
pub fn open_durable(path: &Path) -> Result<Connection> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let conn = Connection::open(path)?;
    conn.pragma_update(None, "journal_mode", "WAL")?;
    conn.pragma_update(None, "synchronous", "FULL")?;
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS operations(
           op_id BLOB PRIMARY KEY, params_digest BLOB NOT NULL,
           kind TEXT NOT NULL, state TEXT NOT NULL, attempt INTEGER NOT NULL,
           committed_at_ms INTEGER NOT NULL, detail TEXT NOT NULL);
         CREATE TABLE IF NOT EXISTS chunks(
           ordinal INTEGER PRIMARY KEY, op_id BLOB NOT NULL,
           input_sha256 BLOB NOT NULL, output_sha256 BLOB,
           output_len INTEGER NOT NULL DEFAULT 0, state TEXT NOT NULL,
           attempt INTEGER NOT NULL DEFAULT 0,
           available_until_ms INTEGER NOT NULL DEFAULT 0);
         CREATE TABLE IF NOT EXISTS pins(
           nonce BLOB PRIMARY KEY, ordinal INTEGER NOT NULL,
           expires_ms INTEGER NOT NULL);
         CREATE TABLE IF NOT EXISTS gc_receipts(
           ordinal INTEGER PRIMARY KEY, reclaimed_at_ms INTEGER NOT NULL);",
    )?;
    Ok(conn)
}

pub fn wall_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|t| t.as_millis() as u64)
        .unwrap_or(0)
}

/// Length-prefixed params digest binding every immutable submission field.
pub fn params_digest(
    authority: &str,
    owner: &str,
    generation: u64,
    ordinal: u64,
    operation_id: &[u8; 16],
    total_length: u64,
    input_sha256: &[u8; 32],
    execution_deadline_ms: u64,
) -> [u8; 32] {
    let mut h = Sha256::new();
    for part in [authority.as_bytes(), owner.as_bytes()] {
        h.update((part.len() as u64).to_le_bytes());
        h.update(part);
    }
    for v in [generation, ordinal, total_length, execution_deadline_ms] {
        h.update(v.to_le_bytes());
    }
    h.update([16u8]);
    h.update(operation_id);
    h.update([32u8]);
    h.update(input_sha256);
    h.finalize().into()
}

/// Deterministic nonzero 16-octet operation ID (mirrors the coordinator).
pub fn operation_id(seed: u64, worker: u64, kind: &str, ordinal: u64) -> [u8; 16] {
    let mut h = Sha256::new();
    h.update(b"workload-op-v1");
    h.update(seed.to_le_bytes());
    h.update(worker.to_le_bytes());
    h.update(kind.as_bytes());
    h.update(ordinal.to_le_bytes());
    let d: [u8; 32] = h.finalize().into();
    let mut id: [u8; 16] = d[..16].try_into().unwrap();
    if id == [0; 16] {
        id[15] = 1;
    }
    id
}

pub fn hex_id(id: &[u8; 16]) -> String {
    id.iter().map(|b| format!("{b:02x}")).collect()
}

pub fn parse_id(text: &str) -> Result<[u8; 16]> {
    if text.len() != 32 || !text.bytes().all(|b| b.is_ascii_hexdigit()) {
        bail!("operation ID must be 32 hex digits");
    }
    let mut id = [0u8; 16];
    for (i, c) in text.as_bytes().chunks(2).enumerate() {
        id[i] = u8::from_str_radix(std::str::from_utf8(c)?, 16)?;
    }
    if id == [0; 16] {
        bail!("operation ID cannot be zero");
    }
    Ok(id)
}

/// Map a verified client leaf DER certificate to its owner label via the
/// same `sha256<TAB>principal` TSV used by the PipeStream arm.
pub fn owner_of(leaf_der: &[u8], map_path: &Path) -> Result<String> {
    let fp: [u8; 32] = Sha256::digest(leaf_der).into();
    let text = std::fs::read_to_string(map_path)?;
    let mut lines = text.lines();
    if lines.next() != Some("sha256\tprincipal") {
        bail!("principal map needs sha256<TAB>principal header");
    }
    let want: String = fp.iter().map(|b| format!("{b:02x}")).collect();
    for line in lines {
        if let Some((fp, owner)) = line.split_once('\t') {
            if fp == want {
                return Ok(owner.to_string());
            }
        }
    }
    bail!("unmapped client identity")
}

/// fsync a file and its containing directory.
pub fn sync_file(path: &Path) -> Result<()> {
    let f = std::fs::File::open(path)?;
    f.sync_all()?;
    let dir = std::fs::File::open(path.parent().context("no parent")?)?;
    dir.sync_all()?;
    Ok(())
}
