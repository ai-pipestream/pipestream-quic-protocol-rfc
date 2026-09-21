//! Concurrency stress tests for the storage SPI (memory and filesystem
//! backends, no docker). Invariants under contention: exactly one winner
//! per conflict, no publish-after-seal, live reads survive retire, and the
//! journal's replay is total-ordered and duplicate-free.

use pipestream_storage::layout::{self, sha256};
use pipestream_storage::{Cursor, FsStore, Intent, MemoryStore, PutWriter as _, ScopePath, StorageError};
use std::io::{Read, Write as _};
use std::sync::{Barrier, Mutex};

/// Drives one storm implementation against the in-memory backend.
fn storm_memory(test: fn(&mut MemoryStore)) {
    test(&mut MemoryStore::new());
}

/// Drives one storm implementation against the filesystem backend.
fn storm_fs(test: fn(&mut FsStore)) {
    let dir = tempfile::tempdir().unwrap();
    test(&mut FsStore::open(dir.path()).unwrap());
}

#[test]
fn conflicting_put_storm_yields_exactly_one_winner() {
    storm_memory(conflicting_put_storm_impl);
    storm_fs(conflicting_put_storm_impl);
}

fn conflicting_put_storm_impl<B: pipestream_storage::contract::Backend + Send + Sync>(store: &mut B) {
    let scope = ScopePath::root(0);
    let shared = Mutex::new(&mut *store);
    let barrier = Barrier::new(16);
    let outcomes: Vec<_> = std::thread::scope(|threads| {
        let handles: Vec<_> = (0..16u32)
            .map(|variant| {
                let shared = &shared;
                let barrier = &barrier;
                let scope = &scope;
                threads.spawn(move || {
                    let payload = format!("variant-{variant}").into_bytes();
                    barrier.wait();
                    shared
                        .lock()
                        .unwrap()
                        .put(scope, 1, &payload, &sha256(&payload))
                        .map(|_| payload)
                })
            })
            .collect();
        handles.into_iter().map(|h| h.join().unwrap()).collect()
    });
    let winners: Vec<_> = outcomes.iter().filter_map(|r| r.as_ref().ok()).collect();
    assert_eq!(winners.len(), 1);
    for outcome in &outcomes {
        match outcome {
            Ok(_) => {}
            Err(StorageError::Corrupt(_)) => {}
            Err(error) => panic!("loser must fail Corrupt, got {error:?}"),
        }
    }
}

#[test]
fn put_seal_race_never_publishes_after_seal() {
    storm_memory(put_seal_race_impl);
    storm_fs(put_seal_race_impl);
}

fn put_seal_race_impl<B: pipestream_storage::contract::Backend + Send>(store: &mut B) {
    let scope = ScopePath::root(1);
    let shared = Mutex::new(&mut *store);
    let barrier = Barrier::new(8);
    let published: Mutex<Vec<u32>> = Mutex::new(Vec::new());
    std::thread::scope(|threads| {
        for variant in 0..8u32 {
            let shared = &shared;
            let barrier = &barrier;
            let published = &published;
            let scope = &scope;
            threads.spawn(move || {
                let payload = format!("racing-{variant}").into_bytes();
                barrier.wait();
                if shared
                    .lock()
                    .unwrap()
                    .put(scope, 100 + variant, &payload, &sha256(&payload))
                    .is_ok()
                {
                    published.lock().unwrap().push(100 + variant);
                }
            });
        }
    });
    // Seal exactly the winners and verify; a late put must refuse.
    let guard = shared.lock().unwrap();
    let winners = published.lock().unwrap();
    let digests: Vec<_> = winners
        .iter()
        .map(|id| (*id, sha256(format!("racing-{}", id - 100).as_bytes())))
        .collect();
    let digest = layout::scope_digest(&digests).unwrap();
    let mut guard = guard;
    guard
        .seal_scope(&scope, &digest, winners.len() as u64)
        .unwrap();
    guard.verify_scope(&scope).unwrap();
    let late = b"late";
    match guard.put(&scope, 999, late, &sha256(late)) {
        Err(StorageError::AlreadySealed(_)) => {}
        other => panic!("put after seal: expected AlreadySealed, got {other:?}"),
    }
    assert_eq!(guard.list(&scope).unwrap().len(), winners.len());
}

#[test]
fn readers_open_during_retire_still_read_everything() {
    storm_memory(reader_during_retire_impl);
    storm_fs(reader_during_retire_impl);
}

fn reader_during_retire_impl<B: pipestream_storage::contract::Backend + Send + Sync>(store: &mut B)
where
    B::Reader: Send,
{
    let scope = ScopePath::root(2);
    for entity in 0..8u32 {
        let payload = format!("held-{entity}").into_bytes();
        store.put(&scope, entity, &payload, &sha256(&payload)).unwrap();
    }
    // Open every reader before retiring.
    let readers: Vec<_> = (0..8u32)
        .map(|entity| store.open_read(&scope, entity).unwrap())
        .collect();
    let retired = store.retire(&scope, u64::MAX).unwrap();
    assert_eq!(retired, 8);
    let shared_readers = Mutex::new(readers);
    let barrier = Barrier::new(8);
    std::thread::scope(|threads| {
        for index in 0..8usize {
            let shared_readers = &shared_readers;
            let barrier = &barrier;
            threads.spawn(move || {
                barrier.wait();
                let mut bytes = Vec::new();
                {
                    let mut guard = shared_readers.lock().unwrap();
                    guard[index].read_to_end(&mut bytes).unwrap();
                }
                assert_eq!(bytes, format!("held-{index}").into_bytes());
            });
        }
    });
    assert!(store.list(&scope).unwrap().is_empty());
}

#[test]
fn journal_append_commit_storm_stays_ordered_and_duplicate_free() {
    storm_memory(journal_storm_impl);
    storm_fs(journal_storm_impl);
}

fn journal_storm_impl<B: pipestream_storage::contract::Backend + Send>(store: &mut B) {
    let shared = Mutex::new(&mut *store);
    let barrier = Barrier::new(16);
    std::thread::scope(|threads| {
        for variant in 0..16u32 {
            let shared = &shared;
            let barrier = &barrier;
            threads.spawn(move || {
                let id = format!("intent-{variant}");
                barrier.wait();
                let intent = Intent::new(&id, format!("work-{variant}").into_bytes()).unwrap();
                let cursor = shared.lock().unwrap().append_intent(&intent).unwrap();
                // Retry with identical bytes mid-storm: same cursor, no dup.
                let again = shared.lock().unwrap().append_intent(&intent).unwrap();
                assert_eq!(cursor, again);
                if variant % 2 == 0 {
                    shared
                        .lock()
                        .unwrap()
                        .commit(&id, format!("done-{variant}").as_bytes())
                        .unwrap();
                }
            });
        }
    });
    let entries = store.replay(Cursor(0)).unwrap();
    assert_eq!(entries.len(), 16, "every intent exactly once");
    let mut ids: Vec<&str> = entries.iter().map(|e| e.intent.id()).collect();
    ids.sort_unstable();
    ids.dedup();
    assert_eq!(ids.len(), 16);
    let mut last = Cursor(0);
    for entry in &entries {
        assert!(entry.cursor > last, "cursors strictly increase");
        last = entry.cursor;
        if let Some(cursor) = entry.commit_cursor {
            assert!(cursor > entry.cursor, "commit after its intent");
        }
    }
    let committed = entries.iter().filter(|e| e.committed()).count();
    assert_eq!(committed, 8);
}

#[test]
fn seal_during_concurrent_verify_is_never_torn() {
    storm_memory(seal_verify_race_impl);
    storm_fs(seal_verify_race_impl);
}

fn seal_verify_race_impl<B: pipestream_storage::contract::Backend + Send>(store: &mut B) {
    let scope = ScopePath::root(3);
    let payload = b"steady".to_vec();
    store.put(&scope, 1, &payload, &sha256(&payload)).unwrap();
    let shared = Mutex::new(&mut *store);
    let barrier = Barrier::new(2);
    std::thread::scope(|threads| {
        let shared = &shared;
        let barrier = &barrier;
        let scope = &scope;
        threads.spawn(|| {
            barrier.wait();
            for _ in 0..200 {
                // Before the seal: NotFound. After: Ok. Never a torn
                // Corrupt from a half-written seal.
                match shared.lock().unwrap().verify_scope(scope) {
                    Err(StorageError::NotFound(_)) | Ok(_) => {}
                    other => panic!("torn verify during seal: {other:?}"),
                }
            }
        });
        threads.spawn(|| {
            barrier.wait();
            let digest = layout::scope_digest(&[(1, sha256(b"steady"))]).unwrap();
            shared.lock().unwrap().seal_scope(scope, &digest, 1).unwrap();
        });
    });
}

#[test]
fn concurrent_begin_put_writers_to_one_entity_yield_one_winner() {
    storm_memory(streaming_put_storm_impl);
    storm_fs(streaming_put_storm_impl);
}

fn streaming_put_storm_impl<B: pipestream_storage::contract::Backend + Send>(store: &mut B) {
    let scope = ScopePath::root(4);
    let shared = Mutex::new(&mut *store);
    let barrier = Barrier::new(8);
    let outcomes: Vec<_> = std::thread::scope(|threads| {
        let handles: Vec<_> = (0..8u32)
            .map(|variant| {
                let shared = &shared;
                let barrier = &barrier;
                let scope = &scope;
                threads.spawn(move || {
                    let payload = format!("streamed-{variant}").into_bytes();
                    let digest = sha256(&payload);
                    barrier.wait();
                    let mut guard = shared.lock().unwrap();
                    let writer = guard.begin_put(scope, 1, &digest);
                    match writer {
                        Err(error) => Err(error),
                        Ok(mut writer) => {
                            writer.write_all(&payload).unwrap();
                            writer.finish().map(|_| payload)
                        }
                    }
                })
            })
            .collect();
        handles.into_iter().map(|h| h.join().unwrap()).collect()
    });
    let winners: Vec<_> = outcomes.iter().filter_map(|r| r.as_ref().ok()).collect();
    assert_eq!(winners.len(), 1);
    for outcome in &outcomes {
        match outcome {
            Ok(_) => {}
            Err(StorageError::Corrupt(_)) => {}
            Err(error) => panic!("loser must fail Corrupt, got {error:?}"),
        }
    }
}
