//! Resource-leak audit for the storage SPI backends.
//!
//! These tests measure process resources — open file descriptors
//! (`/proc/self/fd`), threads (`/proc/self/status`), resident memory — and
//! on-disk side effects across many open/use/close cycles, asserting each
//! backend returns to baseline. One global mutex serializes the whole
//! binary: the test harness runs every test on its own thread, and a
//! concurrently running test would otherwise pollute the fd/thread counts
//! of the one currently measuring. Docker-backed tests skip when no daemon
//! answers; the service backends drop their private tokio runtimes on
//! detached threads by design, so their counts are settled with a timeout
//! rather than read the instant the last cycle returns.

mod common;

use pipestream_storage::{
    Cursor, Digest32, FsStore, Intent, Journal, MemoryStore, ObjectStore, PutWriter, ScopePath,
    StorageError, sha256,
};
use std::collections::BTreeSet;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard};
use std::time::{Duration, Instant};

/// Cycles for the fs open/close audits: enough that a one-fd-per-cycle
/// leak shows up as a delta of hundreds, far above any noise.
const FS_CYCLES: usize = 200;
/// Cycles for the docker-backed services: connections and runtimes are
/// heavier, so fewer cycles against a generous-but-bounded budget.
const SERVICE_CYCLES: u32 = 50;
/// Put/retire cycles for the in-memory RSS audit.
const MEMORY_CYCLES: usize = 10_000;
/// One-time warm-up slack for the fs fd audits (first-use infrastructure).
const FS_FD_SLACK: usize = 2;
/// Net fd growth tolerated across 50 service cycles: lazy connection pools
/// may hold a handful of sockets open briefly after the last drop.
const SERVICE_FD_BUDGET: usize = 20;
/// Net thread growth tolerated across 50 service cycles: the service
/// backends drop their private runtimes on detached threads by design, so
/// a few may still be draining when the last cycle returns.
const SERVICE_THREAD_BUDGET: usize = 8;
/// RSS growth budget for the memory backend's 10k put+retire cycles.
const RSS_BUDGET_KIB: usize = 32 * 1024;
/// How long to wait for asynchronous backend drops to drain.
const SETTLE_TIMEOUT: Duration = Duration::from_secs(30);

static SERIAL: Mutex<()> = Mutex::new(());

/// Serializes every test in this binary (see the module docs).
fn serial() -> MutexGuard<'static, ()> {
    SERIAL.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// False on systems without a procfs, where the resource audits skip.
fn proc_self_available() -> bool {
    Path::new("/proc/self/fd").is_dir() && Path::new("/proc/self/status").is_file()
}

/// Number of open file descriptors of this process. The directory handle
/// used to read `/proc/self/fd` counts itself while the iterator is alive;
/// baseline and after-measurement include it alike, so deltas are exact.
fn fd_count() -> usize {
    std::fs::read_dir("/proc/self/fd")
        .map(|entries| entries.flatten().count())
        .unwrap_or(0)
}

/// Reads a numeric field (`Threads:`, `VmRSS:`) out of `/proc/self/status`.
fn status_field(prefix: &str) -> usize {
    std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|status| {
            status
                .lines()
                .find(|line| line.starts_with(prefix))
                .and_then(|line| line.split_whitespace().nth(1))
                .and_then(|value| value.parse().ok())
        })
        .unwrap_or(0)
}

fn thread_count() -> usize {
    status_field("Threads:")
}

fn vm_rss_kib() -> usize {
    status_field("VmRSS:")
}

/// Polls `probe` until `within` holds or the settle timeout elapses, and
/// returns the last probed value. Service backends close sockets and
/// runtimes on detached drop threads by design, so the instant the last
/// cycle returns is not the instant the resources are gone.
fn settle(mut probe: impl FnMut() -> usize, within: impl Fn(usize) -> bool) -> usize {
    let deadline = Instant::now() + SETTLE_TIMEOUT;
    loop {
        let value = probe();
        if within(value) || Instant::now() >= deadline {
            return value;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// Top-level entry names of a directory (for escape detection).
fn top_level_names(dir: &Path) -> BTreeSet<String> {
    std::fs::read_dir(dir)
        .map(|entries| {
            entries
                .flatten()
                .map(|entry| entry.file_name().to_string_lossy().into_owned())
                .collect()
        })
        .unwrap_or_default()
}

/// True for the fs backend's scratch grammar (`write-*.tmp`, `scratch/`).
fn is_scratch_like(name: &str) -> bool {
    (name.starts_with("write-") && name.ends_with(".tmp")) || name == "scratch"
}

/// True for a lowercase hex name of exactly `len` characters.
fn is_hex(name: &str, len: usize) -> bool {
    name.len() == len && name.bytes().all(|byte| byte.is_ascii_hexdigit())
}

/// Which grammar slot a directory fills in the pinned fs layout.
#[derive(Clone, Copy)]
enum Slot {
    /// The store root: `objects/` and `journal/` only.
    Root,
    /// `objects/`: `writer.lock` plus 8-hex scope directories.
    Objects,
    /// A scope directory: `scope.seal`, 2-hex bucket dirs, 8-hex child scopes.
    Scope,
    /// A fanout bucket: 8-hex entity files only.
    Bucket,
    /// `journal/`: `writer.lock` and `journal.log` only.
    Journal,
}

/// Walks an fs store tree, collecting every entry that does not match the
/// pinned layout grammar — scratch files, stray directories, anything.
fn walk_fs_grammar(dir: &Path, slot: Slot, offenders: &mut Vec<PathBuf>) {
    let entries = match std::fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(error) => {
            offenders.push(PathBuf::from(format!("{}: {error}", dir.display())));
            return;
        }
    };
    for entry in entries.flatten() {
        let path = entry.path();
        let name = entry.file_name().to_string_lossy().into_owned();
        let is_dir = path.is_dir();
        let child = match (slot, is_dir, name.as_str()) {
            (Slot::Root, true, "objects") => Some(Slot::Objects),
            (Slot::Root, true, "journal") => Some(Slot::Journal),
            (Slot::Objects, false, "writer.lock") => None,
            (Slot::Objects, true, _) if is_hex(&name, 8) => Some(Slot::Scope),
            (Slot::Scope, false, "scope.seal") => None,
            (Slot::Scope, true, _) if is_hex(&name, 2) => Some(Slot::Bucket),
            (Slot::Scope, true, _) if is_hex(&name, 8) => Some(Slot::Scope),
            (Slot::Bucket, false, _) if is_hex(&name, 8) => None,
            (Slot::Journal, false, "writer.lock" | "journal.log") => None,
            _ => {
                offenders.push(path);
                continue;
            }
        };
        if let Some(child) = child {
            walk_fs_grammar(&path, child, offenders);
        }
    }
}

/// Collects every scratch-grammar entry (`write-*.tmp`, `scratch/`) under
/// `dir`, wherever in the tree it sits.
fn collect_scratch(dir: &Path, found: &mut Vec<PathBuf>) {
    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            let name = entry.file_name().to_string_lossy().into_owned();
            if is_scratch_like(&name) {
                found.push(path.clone());
            }
            if path.is_dir() {
                collect_scratch(&path, found);
            }
        }
    }
}

/// One full fs store cycle: open, four puts, seal, verify, a complete
/// read, then close — half the cycles through `close()`, half through a
/// plain drop. Fresh root per cycle so every write path runs for real.
fn fs_full_cycle(use_close: bool) {
    let dir = tempfile::tempdir().expect("tempdir");
    let scope = ScopePath::root(0);
    let mut store = FsStore::open(dir.path()).expect("open");
    let mut sealed = Vec::new();
    for entity in 0..4u32 {
        let payload = format!("payload-{entity}").into_bytes();
        let digest = sha256(&payload);
        store.put(&scope, entity, &payload, &digest).expect("put");
        sealed.push((entity, digest));
    }
    let seal = pipestream_storage::layout::scope_digest(&sealed).expect("scope digest");
    store.seal_scope(&scope, &seal, 4).expect("seal");
    store.verify_scope(&scope).expect("verify");
    let mut reader = store.open_read(&scope, 2).expect("open_read");
    let mut bytes = Vec::new();
    reader.read_to_end(&mut bytes).expect("read");
    assert_eq!(bytes, b"payload-2");
    drop(reader);
    if use_close {
        store.close().expect("close");
    } else {
        drop(store);
    }
}

/// One fs cycle that leaves the reader open across the store's close:
/// the reader holds its own fd, which must survive the close and be
/// released when the reader itself drops.
fn fs_reader_close_cycle(use_close: bool) {
    let dir = tempfile::tempdir().expect("tempdir");
    let scope = ScopePath::root(0);
    let payload: &[u8] = b"held-across-close";
    let mut store = FsStore::open(dir.path()).expect("open");
    store.put(&scope, 1, payload, &sha256(payload)).expect("put");
    let mut reader = store.open_read(&scope, 1).expect("open_read");
    if use_close {
        store.close().expect("close");
    } else {
        drop(store);
    }
    let mut bytes = Vec::new();
    reader.read_to_end(&mut bytes).expect("read after close");
    assert_eq!(bytes, payload);
    drop(reader);
}

#[test]
fn fs_open_close_cycles_return_fds_and_threads_to_baseline() {
    let _guard = serial();
    if !proc_self_available() {
        eprintln!("/proc unavailable: skipping fs fd audit");
        return;
    }
    // Warm-up absorbs one-time first-use infrastructure before the baseline.
    fs_full_cycle(true);
    fs_full_cycle(false);
    let baseline_fds = fd_count();
    let baseline_threads = thread_count();
    for cycle in 0..FS_CYCLES {
        fs_full_cycle(cycle % 2 == 0);
    }
    let after_fds = fd_count();
    let after_threads = thread_count();
    eprintln!(
        "fs open/close audit: {FS_CYCLES} cycles, baseline {baseline_fds} fds/{baseline_threads} threads, after {after_fds} fds/{after_threads} threads"
    );
    assert!(
        after_fds <= baseline_fds + FS_FD_SLACK,
        "fd count grew {} across {FS_CYCLES} fs open/close cycles",
        after_fds.saturating_sub(baseline_fds)
    );
    assert!(
        after_threads <= baseline_threads,
        "thread count grew {} across {FS_CYCLES} fs open/close cycles",
        after_threads.saturating_sub(baseline_threads)
    );
}

#[test]
fn fs_reader_held_across_close_releases_every_fd() {
    let _guard = serial();
    if !proc_self_available() {
        eprintln!("/proc unavailable: skipping fs reader fd audit");
        return;
    }
    fs_reader_close_cycle(true); // warm-up
    let baseline_fds = fd_count();
    let baseline_threads = thread_count();
    for cycle in 0..FS_CYCLES {
        fs_reader_close_cycle(cycle % 2 == 0);
    }
    let after_fds = fd_count();
    let after_threads = thread_count();
    eprintln!(
        "fs reader-across-close audit: {FS_CYCLES} cycles, baseline {baseline_fds} fds/{baseline_threads} threads, after {after_fds} fds/{after_threads} threads"
    );
    assert!(
        after_fds <= baseline_fds + FS_FD_SLACK,
        "fd count grew {} across {FS_CYCLES} reader-across-close cycles",
        after_fds.saturating_sub(baseline_fds)
    );
    assert!(
        after_threads <= baseline_threads,
        "thread count grew {} across {FS_CYCLES} reader-across-close cycles",
        after_threads.saturating_sub(baseline_threads)
    );
}

/// A live fs reader holds the object: closing the store — or retiring the
/// entity, which unlinks the file — must not invalidate the read, and once
/// the reader drops, every fd is gone.
#[test]
fn fs_reader_survives_store_close_and_retire() {
    let _guard = serial();
    if !proc_self_available() {
        eprintln!("/proc unavailable: skipping fs reader survival audit");
        return;
    }
    let payload: &[u8] = b"reader survival payload";
    let digest = sha256(payload);
    reader_survival_round(payload, &digest, false); // warm-up
    let baseline_fds = fd_count();
    let baseline_threads = thread_count();
    // Store closed first, reader read afterwards.
    reader_survival_round(payload, &digest, false);
    // Entity retired (unlinked) underneath the live reader.
    reader_survival_round(payload, &digest, true);
    let after_fds = fd_count();
    let after_threads = thread_count();
    eprintln!(
        "fs reader survival audit: baseline {baseline_fds} fds/{baseline_threads} threads, after {after_fds} fds/{after_threads} threads"
    );
    assert!(
        after_fds <= baseline_fds + FS_FD_SLACK,
        "fd count grew {} across reader survival rounds",
        after_fds.saturating_sub(baseline_fds)
    );
    assert!(
        after_threads <= baseline_threads,
        "thread count grew {} across reader survival rounds",
        after_threads.saturating_sub(baseline_threads)
    );
}

/// One survival round: put, open a reader, optionally retire the entity
/// (sealed scope retires whole, unlinking the file), close the store, then
/// read the full payload from the still-valid fd.
fn reader_survival_round(payload: &[u8], digest: &Digest32, retire: bool) {
    let dir = tempfile::tempdir().expect("tempdir");
    let scope = ScopePath::root(0);
    let mut store = FsStore::open(dir.path()).expect("open");
    store.put(&scope, 1, payload, digest).expect("put");
    let mut reader = store.open_read(&scope, 1).expect("open_read");
    if retire {
        let seal = pipestream_storage::layout::scope_digest(&[(1, *digest)]).expect("scope digest");
        store.seal_scope(&scope, &seal, 1).expect("seal");
        assert_eq!(store.retire(&scope, u64::MAX).expect("retire"), 1);
    }
    store.close().expect("close");
    let mut bytes = Vec::new();
    reader.read_to_end(&mut bytes).expect("read past close");
    assert_eq!(bytes, payload);
    drop(reader);
}

/// Scratch discipline: after regular traffic, a failed put (bad digest),
/// and an aborted streaming writer, only pinned-grammar files may exist
/// under the store root, and nothing at all may appear outside it — not in
/// a sibling directory, not in the process working directory, not in the
/// shared temp directory.
#[test]
fn fs_scratch_grammar_holds_and_nothing_escapes_the_root() {
    let _guard = serial();
    let store_dir = tempfile::tempdir().expect("store dir");
    let ambient_dir = tempfile::tempdir().expect("ambient dir");
    let cwd = std::env::current_dir().expect("cwd");
    let tmp = std::env::temp_dir();
    let cwd_before = top_level_names(&cwd);
    let tmp_before = top_level_names(&tmp);

    let scope = ScopePath::root(0);
    let mut store = FsStore::open(store_dir.path()).expect("open");
    // Regular traffic: entities (one of them streamed), a seal, journal records.
    let mut sealed = Vec::new();
    for entity in 0..2u32 {
        let payload = format!("grammar-{entity}").into_bytes();
        let digest = sha256(&payload);
        store.put(&scope, entity, &payload, &digest).expect("put");
        sealed.push((entity, digest));
    }
    {
        let payload = b"grammar-2-streamed";
        let digest = sha256(payload);
        let mut writer = store.begin_put(&scope, 2, &digest).expect("begin_put");
        writer.write_all(payload).expect("stream write");
        writer.finish().expect("finish");
        sealed.push((2, digest));
    }
    let seal = pipestream_storage::layout::scope_digest(&sealed).expect("scope digest");
    store.seal_scope(&scope, &seal, 3).expect("seal");
    store.verify_scope(&scope).expect("verify");
    let intent = Intent::new("grammar-1", b"record".to_vec()).expect("intent");
    assert_eq!(store.append_intent(&intent).expect("append"), Cursor(1));
    store.commit("grammar-1", b"done").expect("commit");
    assert!(store.replay(Cursor(0)).expect("replay")[0].committed());

    // A put whose digest does not match must fail before touching disk.
    // (On an unsealed scope: a sealed scope is AlreadySealed before the
    // digest is ever checked, per the pinned seal-before-digest ordering.)
    let open_scope = ScopePath::root(7);
    let wrong = sha256(b"different bytes");
    match store.put(&open_scope, 9, b"real bytes", &wrong) {
        Err(StorageError::Corrupt(_)) => {}
        other => panic!("expected Corrupt for a bad-digest put, got {other:?}"),
    }
    // A streaming put abandoned mid-stream must abort (same unsealed scope:
    // a sealed scope rejects begin_put with AlreadySealed before any write).
    {
        let full = b"the complete streamed payload";
        let mut writer = store.begin_put(&open_scope, 10, &sha256(full)).expect("begin_put");
        writer.write_all(&full[..8]).expect("partial write");
        // Dropped without finish: aborts.
    }
    store.close().expect("close");

    // Only grammar files exist under the root...
    let mut offenders = Vec::new();
    walk_fs_grammar(store_dir.path(), Slot::Root, &mut offenders);
    assert!(
        offenders.is_empty(),
        "non-grammar entries under the store root: {offenders:?}"
    );
    let mut scratch = Vec::new();
    collect_scratch(store_dir.path(), &mut scratch);
    assert!(scratch.is_empty(), "scratch files under the store root: {scratch:?}");
    // ...the sibling "ambient" directory was never touched...
    let ambient = top_level_names(ambient_dir.path());
    assert!(ambient.is_empty(), "ambient directory received files: {ambient:?}");
    // ...nothing appeared or vanished in the process working directory...
    assert_eq!(top_level_names(&cwd), cwd_before, "cwd entries changed");
    // ...and no scratch-grammar file appeared in the shared temp directory.
    let tmp_after = top_level_names(&tmp);
    let escaped: Vec<&String> = tmp_after
        .difference(&tmp_before)
        .filter(|name| is_scratch_like(name))
        .collect();
    assert!(
        escaped.is_empty(),
        "scratch escaped into {}: {escaped:?}",
        tmp.display()
    );
}

/// A streaming writer dropped without finish aborts the write: the entity
/// never becomes visible and no scratch survives the store's close.
#[test]
fn fs_begin_put_abort_publishes_nothing_and_leaves_no_scratch() {
    let _guard = serial();
    let dir = tempfile::tempdir().expect("tempdir");
    let scope = ScopePath::root(0);
    let mut store = FsStore::open(dir.path()).expect("open");
    let full = b"a payload that is never fully written";
    {
        let mut writer = store.begin_put(&scope, 42, &sha256(full)).expect("begin_put");
        writer.write_all(&full[..10]).expect("partial write");
        // Aborted: dropped without finish.
    }
    match store.open_read(&scope, 42) {
        Err(StorageError::NotFound(_)) => {}
        other => panic!("aborted put must stay invisible, got {other:?}"),
    }
    assert!(store.list(&scope).expect("list").is_empty());
    assert!(store.list_scopes().expect("list_scopes").is_empty());
    store.close().expect("close");
    let mut scratch = Vec::new();
    collect_scratch(dir.path(), &mut scratch);
    assert!(scratch.is_empty(), "aborted put left scratch behind: {scratch:?}");
    // The only trace may be grammar-conformant directories (an empty bucket).
    let mut offenders = Vec::new();
    walk_fs_grammar(dir.path(), Slot::Root, &mut offenders);
    assert!(offenders.is_empty(), "non-grammar entries: {offenders:?}");
}

/// The in-memory backend holds no fds and no threads; 10k put+retire
/// cycles through a shared clone must not accumulate resident memory
/// either. The RSS budget is generous — allocators keep arenas — but far
/// below what a per-cycle retention leak would cost.
#[test]
fn memory_store_put_retire_cycles_hold_rss() {
    let _guard = serial();
    if !proc_self_available() {
        eprintln!("/proc unavailable: skipping memory RSS audit");
        return;
    }
    let store = MemoryStore::new();
    // Clones share state; run the cycles through a second handle so any
    // per-handle accumulation shows up too. The original stays alive.
    let mut shared = store.clone();
    let scope = ScopePath::root(7);
    let payload = vec![0xa5u8; 256];
    let digest = sha256(&payload);
    let cycle = |store: &mut MemoryStore| {
        store.put(&scope, 1, &payload, &digest).expect("put");
        assert_eq!(store.retire(&scope, 1).expect("retire"), 1);
    };
    for _ in 0..500 {
        cycle(&mut shared); // warm up allocator arenas before measuring
    }
    let baseline_rss = vm_rss_kib();
    let baseline_fds = fd_count();
    let baseline_threads = thread_count();
    for _ in 0..MEMORY_CYCLES {
        cycle(&mut shared);
    }
    let after_rss = vm_rss_kib();
    let after_fds = fd_count();
    let after_threads = thread_count();
    let growth = after_rss.saturating_sub(baseline_rss);
    eprintln!(
        "memory audit: {MEMORY_CYCLES} put+retire cycles, RSS {baseline_rss} -> {after_rss} KiB (delta {growth} KiB), fds {baseline_fds} -> {after_fds}, threads {baseline_threads} -> {after_threads}"
    );
    assert!(
        after_fds <= baseline_fds,
        "memory store grew fds by {}",
        after_fds.saturating_sub(baseline_fds)
    );
    assert!(
        after_threads <= baseline_threads,
        "memory store grew threads by {}",
        after_threads.saturating_sub(baseline_threads)
    );
    assert!(
        growth < RSS_BUDGET_KIB,
        "RSS grew {growth} KiB across {MEMORY_CYCLES} put+retire cycles (budget {RSS_BUDGET_KIB} KiB)"
    );
}

/// Runs one warm-up cycle, lets the detached drop thread drain, then
/// returns the quiet (fd, thread) baseline for a service cycle audit.
fn quiet_baseline(warmup: impl FnOnce()) -> (usize, usize) {
    warmup();
    // The warm-up store's drop thread closes its sockets asynchronously;
    // give it a moment so the baseline does not carry the warm-up's fds.
    std::thread::sleep(Duration::from_millis(500));
    let pre = fd_count();
    settle(fd_count, |n| n <= pre + SERVICE_FD_BUDGET);
    (fd_count(), thread_count())
}

/// Asserts the post-cycle counts stayed within budget, after settling.
fn assert_service_drift(backend: &str, baseline_fds: usize, baseline_threads: usize) {
    let after_fds = settle(fd_count, |n| n < baseline_fds + SERVICE_FD_BUDGET);
    let after_threads = settle(thread_count, |n| {
        n <= baseline_threads + SERVICE_THREAD_BUDGET
    });
    eprintln!(
        "{backend} audit: baseline {baseline_fds} fds/{baseline_threads} threads, after {after_fds} fds/{after_threads} threads"
    );
    assert!(
        after_fds < baseline_fds + SERVICE_FD_BUDGET,
        "{backend}: fd count grew {} across {SERVICE_CYCLES} open/drop cycles (budget < {SERVICE_FD_BUDGET})",
        after_fds.saturating_sub(baseline_fds)
    );
    assert!(
        after_threads <= baseline_threads + SERVICE_THREAD_BUDGET,
        "{backend}: thread count grew {} across {SERVICE_CYCLES} open/drop cycles (budget {SERVICE_THREAD_BUDGET})",
        after_threads.saturating_sub(baseline_threads)
    );
}

/// Opens an S3 store, retrying through MinIO's startup window.
fn open_s3_with_retry(config: &pipestream_storage::S3Config) -> pipestream_storage::S3Store {
    let mut attempt = 0;
    loop {
        match pipestream_storage::S3Store::open(config) {
            Ok(store) => return store,
            Err(_) if attempt < 50 => {
                attempt += 1;
                std::thread::sleep(Duration::from_millis(200));
            }
            Err(error) => panic!("open s3 store: {error}"),
        }
    }
}

/// Opens a mongo store, retrying through the container's startup window.
fn open_mongo_with_retry(config: &pipestream_storage::MongoConfig) -> pipestream_storage::MongoStore {
    let mut attempt = 0;
    loop {
        match pipestream_storage::MongoStore::open(config) {
            Ok(store) => return store,
            Err(_) if attempt < 100 => {
                attempt += 1;
                std::thread::sleep(Duration::from_millis(200));
            }
            Err(error) => panic!("open mongo store: {error}"),
        }
    }
}

/// Opens a kafka journal, retrying through the broker's startup window.
fn open_kafka_with_retry(config: &pipestream_storage::KafkaConfig) -> pipestream_storage::KafkaJournal {
    let mut attempt = 0;
    loop {
        match pipestream_storage::KafkaJournal::open(config) {
            Ok(journal) => return journal,
            Err(_) if attempt < 150 => {
                attempt += 1;
                std::thread::sleep(Duration::from_millis(200));
            }
            Err(error) => panic!("open kafka journal: {error}"),
        }
    }
}

#[test]
fn postgres_open_put_drop_cycles_release_fds() {
    let _guard = serial();
    if !proc_self_available() {
        eprintln!("/proc unavailable: skipping postgres fd audit");
        return;
    }
    let Some(pg) = common::PostgresContainer::start() else {
        eprintln!("docker unavailable: skipping postgres fd audit");
        return;
    };
    let url = pg.fresh_database();
    let scope = ScopePath::root(0);
    let payload: &[u8] = b"postgres leak audit payload";
    let digest = sha256(payload);
    let (baseline_fds, baseline_threads) = quiet_baseline(|| {
        let mut store = pipestream_storage::PostgresStore::open(&url).expect("open");
        store.put(&scope, 0, payload, &digest).expect("put");
        drop(store);
    });
    for entity in 1..=SERVICE_CYCLES {
        let mut store = pipestream_storage::PostgresStore::open(&url).expect("open");
        store.put(&scope, entity, payload, &digest).expect("put");
        drop(store);
    }
    // State persists across drops: a fresh handle reads the warm-up entity.
    {
        let store = pipestream_storage::PostgresStore::open(&url).expect("reopen");
        let mut reader = store.open_read(&scope, 0).expect("open_read after reopen");
        let mut bytes = Vec::new();
        reader.read_to_end(&mut bytes).expect("read after reopen");
        assert_eq!(bytes, payload);
    }
    assert_service_drift("postgres", baseline_fds, baseline_threads);
}

#[test]
fn mongo_open_put_drop_cycles_release_fds() {
    let _guard = serial();
    if !proc_self_available() {
        eprintln!("/proc unavailable: skipping mongo fd audit");
        return;
    }
    let Some(mongo) = common::MongoContainer::start() else {
        eprintln!("docker unavailable: skipping mongo fd audit");
        return;
    };
    let config = mongo.fresh_config();
    let scope = ScopePath::root(0);
    let payload: &[u8] = b"mongo leak audit payload";
    let digest = sha256(payload);
    let (baseline_fds, baseline_threads) = quiet_baseline(|| {
        let mut store = open_mongo_with_retry(&config);
        store.put(&scope, 0, payload, &digest).expect("put");
        drop(store);
    });
    for entity in 1..=SERVICE_CYCLES {
        let mut store = open_mongo_with_retry(&config);
        store.put(&scope, entity, payload, &digest).expect("put");
        drop(store);
    }
    // State persists across drops: a fresh handle reads the warm-up entity.
    {
        let store = open_mongo_with_retry(&config);
        let mut reader = store.open_read(&scope, 0).expect("open_read after reopen");
        let mut bytes = Vec::new();
        reader.read_to_end(&mut bytes).expect("read after reopen");
        assert_eq!(bytes, payload);
    }
    assert_service_drift("mongo", baseline_fds, baseline_threads);
}

#[test]
fn s3_open_put_drop_cycles_release_fds() {
    let _guard = serial();
    if !proc_self_available() {
        eprintln!("/proc unavailable: skipping s3 fd audit");
        return;
    }
    let Some(minio) = common::MinioContainer::start() else {
        eprintln!("docker unavailable: skipping s3 fd audit");
        return;
    };
    let config = minio.fresh_config();
    let scope = ScopePath::root(0);
    let payload: &[u8] = b"s3 leak audit payload";
    let digest = sha256(payload);
    let (baseline_fds, baseline_threads) = quiet_baseline(|| {
        let mut store = open_s3_with_retry(&config);
        store.put(&scope, 0, payload, &digest).expect("put");
        drop(store);
    });
    for entity in 1..=SERVICE_CYCLES {
        let mut store = open_s3_with_retry(&config);
        store.put(&scope, entity, payload, &digest).expect("put");
        drop(store);
    }
    // Every cycle reopens the same bucket; a final read proves persistence.
    {
        let store = open_s3_with_retry(&config);
        let mut reader = store.open_read(&scope, 0).expect("open_read after reopen");
        let mut bytes = Vec::new();
        reader.read_to_end(&mut bytes).expect("read after reopen");
        assert_eq!(bytes, payload);
    }
    assert_service_drift("s3", baseline_fds, baseline_threads);
}

#[test]
fn kafka_open_append_drop_cycles_release_fds() {
    let _guard = serial();
    if !proc_self_available() {
        eprintln!("/proc unavailable: skipping kafka fd audit");
        return;
    }
    let Some(kafka) = common::KafkaContainer::start() else {
        eprintln!("docker unavailable: skipping kafka fd audit");
        return;
    };
    let config = kafka.fresh_config();
    let (baseline_fds, baseline_threads) = quiet_baseline(|| {
        let mut journal = open_kafka_with_retry(&config);
        let intent = Intent::new("warmup-0", b"record".to_vec()).expect("intent");
        journal.append_intent(&intent).expect("append");
        drop(journal);
    });
    for cycle in 0..SERVICE_CYCLES {
        let mut journal = open_kafka_with_retry(&config);
        let intent = Intent::new(format!("cycle-{cycle}"), b"record".to_vec()).expect("intent");
        journal.append_intent(&intent).expect("append");
        drop(journal);
    }
    // Every cycle reloaded the topic at open; a final replay proves the
    // appends were durable across all those drops.
    {
        let journal = open_kafka_with_retry(&config);
        let entries = journal.replay(Cursor(0)).expect("replay");
        assert_eq!(entries.len(), (SERVICE_CYCLES + 1) as usize);
    }
    assert_service_drift("kafka", baseline_fds, baseline_threads);
}
