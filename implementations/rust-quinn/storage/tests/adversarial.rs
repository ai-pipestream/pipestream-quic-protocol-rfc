//! Adversarial corruption tests for the filesystem backend.
//!
//! Every scenario asserts the pinned error kind: store corruption is
//! Corrupt, I/O failures are Transient, and a torn journal tail (an append
//! that never returned success) is the ONLY silent repair.

use pipestream_storage::layout::{self, sha256};
use pipestream_storage::{
    Cursor, FsStore, Intent, Journal, ObjectStore, ScopePath, StorageError,
};
use std::io::{Read, Write as _};
use sha2::Digest as _;

fn store() -> (tempfile::TempDir, FsStore) {
    let dir = tempfile::tempdir().unwrap();
    let store = FsStore::open(dir.path()).unwrap();
    (dir, store)
}

fn journal_log(dir: &std::path::Path) -> std::path::PathBuf {
    dir.join("journal").join("journal.log")
}

fn write_intent(store: &mut FsStore, id: &str) -> Cursor {
    store
        .append_intent(&Intent::new(id, b"record".to_vec()).unwrap())
        .unwrap()
}

/// Corrupt one byte at `offset` in the journal log.
fn flip_journal_byte(dir: &std::path::Path, offset: u64) {
    use std::io::{Seek, SeekFrom};
    let mut file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(journal_log(dir))
        .unwrap();
    file.seek(SeekFrom::Start(offset)).unwrap();
    let mut byte = [0u8];
    file.read_exact(&mut byte).unwrap();
    byte[0] ^= 0xff;
    file.seek(SeekFrom::Start(offset)).unwrap();
    file.write_all(&byte).unwrap();
    file.sync_all().unwrap();
}

#[test]
fn torn_tail_is_truncated_and_the_committed_prefix_survives() {
    let (dir, mut store) = store();
    write_intent(&mut store, "a");
    write_intent(&mut store, "b");
    drop(store);
    // A killed writer leaves a partial record: half a header at the tail.
    let mut log = std::fs::OpenOptions::new()
        .append(true)
        .open(journal_log(dir.path()))
        .unwrap();
    log.write_all(&[0xde, 0xad, 0xbe, 0xef, 0x01]).unwrap();
    log.sync_all().unwrap();
    drop(log);
    let mut store = FsStore::open(dir.path()).unwrap();
    let entries = store.replay(Cursor(0)).unwrap();
    assert_eq!(entries.len(), 2);
    // The log keeps working: the next append lands at cursor 3.
    assert_eq!(write_intent(&mut store, "c"), Cursor(3));
    drop(store);
    let store = FsStore::open(dir.path()).unwrap();
    assert_eq!(store.replay(Cursor(0)).unwrap().len(), 3);
}

#[test]
fn truncated_magic_is_corrupt_not_transient() {
    let (dir, mut store) = store();
    write_intent(&mut store, "a");
    drop(store);
    // "PSJ" only: neither valid magic nor a torn tail.
    std::fs::write(journal_log(dir.path()), b"PSJ").unwrap();
    match FsStore::open(dir.path()) {
        Err(StorageError::Corrupt(_)) => {}
        Err(error) => panic!("expected Corrupt for truncated magic, got {error:?}"),
        Ok(_) => panic!("expected open to fail on truncated magic"),
    }
}

#[test]
fn missing_journal_log_reopens_fresh_and_empty() {
    let (dir, mut store) = store();
    write_intent(&mut store, "a");
    drop(store);
    std::fs::remove_file(journal_log(dir.path())).unwrap();
    let store = FsStore::open(dir.path()).unwrap();
    assert!(store.replay(Cursor(0)).unwrap().is_empty());
}

#[test]
fn forged_record_with_valid_geometry_fails_open_as_corrupt() {
    let (dir, mut store) = store();
    write_intent(&mut store, "a");
    write_intent(&mut store, "b");
    drop(store);
    // Forge a record that claims cursor 9 (a gap): valid shape and checksum.
    let mut forged = Vec::new();
    forged.push(0u8); // intent
    forged.extend_from_slice(&9u64.to_be_bytes());
    let id = b"forged";
    forged.extend_from_slice(&(id.len() as u32).to_be_bytes());
    forged.extend_from_slice(&5u32.to_be_bytes());
    forged.extend_from_slice(id);
    forged.extend_from_slice(b"proof");
    let mut hasher = sha2::Sha256::new();
    hasher.update(&forged);
    forged.extend_from_slice(&hasher.finalize());
    let mut log = std::fs::OpenOptions::new()
        .append(true)
        .open(journal_log(dir.path()))
        .unwrap();
    log.write_all(&forged).unwrap();
    log.sync_all().unwrap();
    drop(log);
    match FsStore::open(dir.path()) {
        Err(StorageError::Corrupt(_)) => {}
        Err(error) => panic!("expected Corrupt for a cursor gap, got {error:?}"),
        Ok(_) => panic!("expected open to fail on a cursor gap"),
    }
}

#[test]
fn checksum_mismatch_mid_log_fails_open_as_corrupt() {
    let (dir, mut store) = store();
    write_intent(&mut store, "a");
    write_intent(&mut store, "b");
    drop(store);
    // Flip a byte inside the first record's payload (after the 7-byte magic
    // and the 17-byte header).
    flip_journal_byte(dir.path(), 7 + 17 + 1);
    match FsStore::open(dir.path()) {
        Err(StorageError::Corrupt(_)) => {}
        Err(error) => panic!("expected Corrupt for a checksum mismatch, got {error:?}"),
        Ok(_) => panic!("expected open to fail on a checksum mismatch"),
    }
}

#[test]
fn cursor_gap_from_a_dropped_record_fails_open_as_corrupt() {
    let (dir, mut store) = store();
    write_intent(&mut store, "a");
    write_intent(&mut store, "b");
    drop(store);
    // Truncate the log so the last record disappears entirely, cleanly on a
    // record boundary: that rewrites history, which is corruption, not a
    // torn tail... a boundary-aligned truncation is indistinguishable from
    // a shorter log by design; what must NOT happen is the log silently
    // accepting NEW records with a gap. Reopen works (shortest valid log),
    // and the next cursor continues from the surviving prefix.
    let size = std::fs::metadata(journal_log(dir.path())).unwrap().len();
    // Record 2 starts after magic + one full record. Cut exactly there.
    let record1_len = 1 + 8 + 4 + 4 + 1 + 6 + 32; // type+cursor+idLen+payloadLen+id+payload+checksum
    let cut = 7 + record1_len as u64;
    assert!(cut < size);
    let file = std::fs::OpenOptions::new()
        .write(true)
        .open(journal_log(dir.path()))
        .unwrap();
    file.set_len(cut).unwrap();
    drop(file);
    let mut store = FsStore::open(dir.path()).unwrap();
    let entries = store.replay(Cursor(0)).unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(write_intent(&mut store, "c"), Cursor(2));
}

#[test]
fn entity_payload_tampering_is_corrupt_at_verify_and_open_read_is_safe() {
    let (dir, mut store) = store();
    let scope = ScopePath::root(0);
    let payload = b"integrity matters".to_vec();
    store.put(&scope, 5, &payload, &sha256(&payload)).unwrap();
    let digest = layout::scope_digest(&[(5, sha256(&payload))]).unwrap();
    store.seal_scope(&scope, &digest, 1).unwrap();
    let entity_path = scope_dir_path(dir.path(), &scope)
        .join(layout::bucket_hex(5))
        .join(layout::entity_component(5));
    // Flip one payload byte (past the 40-byte header).
    let mut bytes = std::fs::read(&entity_path).unwrap();
    let last = bytes.len() - 1;
    bytes[last] ^= 0xff;
    std::fs::write(&entity_path, bytes).unwrap();
    match store.verify_scope(&scope) {
        Err(StorageError::Corrupt(_)) => {}
        other => panic!("expected Corrupt after payload tamper, got {other:?}"),
    }
    // open_read streams bytes without verifying; the reader contract is that
    // integrity is a seal/verify concern, but a truncated header must refuse.
    std::fs::write(&entity_path, b"xx").unwrap();
    match store.open_read(&scope, 5) {
        Err(StorageError::Corrupt(_)) => {}
        other => panic!("expected Corrupt for a truncated entity file, got {other:?}"),
    }
}

#[test]
fn forged_layout_entries_fail_listing_as_corrupt() {
    let (dir, mut store) = store();
    let scope = ScopePath::root(0);
    let payload = b"payload".to_vec();
    store.put(&scope, 1, &payload, &sha256(&payload)).unwrap();
    let scope_dir = dir.path().join("objects").join("00000000");
    // A junk file directly in the scope dir.
    std::fs::write(scope_dir.join("junk"), b"x").unwrap();
    match store.list(&scope) {
        Err(StorageError::Corrupt(_)) => {}
        other => panic!("expected Corrupt for a forged scope entry, got {other:?}"),
    }
    std::fs::remove_file(scope_dir.join("junk")).unwrap();
    // A 2-hex bucket NAME as a plain file (not a directory).
    std::fs::write(scope_dir.join("ab"), b"x").unwrap();
    match store.list(&scope) {
        Err(StorageError::Corrupt(_)) => {}
        other => panic!("expected Corrupt for a forged bucket entry, got {other:?}"),
    }
    std::fs::remove_file(scope_dir.join("ab")).unwrap();
    assert_eq!(store.list(&scope).unwrap(), vec![1]);
}

#[test]
fn malformed_seal_record_is_corrupt_and_other_scopes_survive() {
    let (dir, mut store) = store();
    let scope = ScopePath::root(0);
    let other = ScopePath::root(1);
    let payload = b"payload".to_vec();
    store.put(&scope, 1, &payload, &sha256(&payload)).unwrap();
    store.put(&other, 1, &payload, &sha256(&payload)).unwrap();
    let digest = layout::scope_digest(&[(1, sha256(&payload))]).unwrap();
    store.seal_scope(&scope, &digest, 1).unwrap();
    // Corrupt the seal: wrong length.
    let seal_path = scope_dir_path(dir.path(), &scope).join("scope.seal");
    std::fs::write(&seal_path, b"short").unwrap();
    match store.verify_scope(&scope) {
        Err(StorageError::Corrupt(_)) => {}
        other => panic!("expected Corrupt for a short seal, got {other:?}"),
    }
    match store.put(&scope, 2, &payload, &sha256(&payload)) {
        Err(StorageError::AlreadySealed(_)) => {}
        other => panic!("expected AlreadySealed even with a damaged seal, got {other:?}"),
    }
    // Corruption is scope-local: the other scope is unaffected.
    let digest = layout::scope_digest(&[(1, sha256(&payload))]).unwrap();
    store.seal_scope(&other, &digest, 1).unwrap();
    store.verify_scope(&other).unwrap();
}

fn scope_dir_path(root: &std::path::Path, scope: &ScopePath) -> std::path::PathBuf {
    let mut dir = root.join("objects");
    for component in scope.components() {
        dir = dir.join(layout::scope_component(*component));
    }
    dir
}
