use super::*;
use pipestream_core::ProtocolError;
use std::io::Cursor;

fn key(id: u32) -> EntityKey {
    EntityKey {
        scope_id: 0,
        entity_id: id,
    }
}
fn tiny() -> RetainedLimits {
    RetainedLimits {
        bytes: 4096,
        principal_bytes: 2048,
        objects: 4,
        principal_objects: 2,
        staging_bytes: 1024,
        staging_objects: 2,
        principals: 4,
    }
}
fn install(
    root: &Arc<RetainedRoot>,
    owner: Option<&PrincipalBinding>,
    session: &str,
    id: u32,
    body: &[u8],
) -> io::Result<()> {
    root.install(
        owner,
        session,
        Some(key(id)),
        body.len() as u64,
        Sha256::digest(body).into(),
        Cursor::new(body),
    )
}
pub(super) fn assert_limit(error: io::Error) {
    assert_eq!(
        error
            .get_ref()
            .and_then(|e| e.downcast_ref::<ProtocolError>())
            .map(|e| e.code),
        Some(pipestream_core::ERROR_LIMIT_EXCEEDED),
        "{error}"
    );
}

#[test]
fn principal_and_global_reservations_survive_reopen_and_do_not_charge_replay_twice() {
    let dir = tempfile::tempdir().unwrap();
    let root = RetainedRoot::open(dir.path().to_owned(), Some(tiny())).unwrap();
    let alice = PrincipalBinding::new("issuer", "alice").unwrap();
    let bob = PrincipalBinding::new("issuer", "bob").unwrap();
    install(&root, Some(&alice), "alice", 1, b"one").unwrap();
    install(&root, Some(&alice), "alice", 2, b"two").unwrap();
    let before = root.usage(None).unwrap();
    install(&root, Some(&alice), "alice", 1, b"one").unwrap();
    assert_eq!(root.usage(None).unwrap(), before);
    assert_limit(install(&root, Some(&alice), "alice", 3, b"three").unwrap_err());
    install(&root, Some(&bob), "bob", 1, b"one").unwrap();
    install(&root, Some(&bob), "bob", 2, b"two").unwrap();
    assert_limit(install(&root, None, "anonymous", 1, b"one").unwrap_err());
    let usage = root.usage(None).unwrap();
    assert_eq!(usage.objects, 4);
    assert_eq!(usage.staging_objects, 0);
    drop(root);
    let reopened = RetainedRoot::open(dir.path().to_owned(), None).unwrap();
    assert_eq!(reopened.usage(None).unwrap(), usage);
    assert_eq!(reopened.limits(), tiny());
    assert!(RetainedRoot::open(dir.path().to_owned(), Some(RetainedLimits::default())).is_err());
}

#[test]
fn wrong_owner_or_authority_cannot_read_or_extend_a_retained_session() {
    let dir = tempfile::tempdir().unwrap();
    let root = RetainedRoot::open(dir.path().to_owned(), None).unwrap();
    let alice = PrincipalBinding::new("issuer", "alice").unwrap();
    install(&root, Some(&alice), "owned", 1, b"data").unwrap();
    for owner in [
        None,
        Some(PrincipalBinding::new("issuer", "bob").unwrap()),
        Some(PrincipalBinding::new("other", "alice").unwrap()),
    ] {
        for id in [1, 2] {
            let error = install(&root, owner.as_ref(), "owned", id, b"data").unwrap_err();
            assert_eq!(
                error
                    .get_ref()
                    .and_then(|e| e.downcast_ref::<ProtocolError>())
                    .map(|e| e.code),
                Some(pipestream_core::authorization::ERROR_UNAUTHORIZED)
            );
        }
        assert!(
            root.load(
                owner.as_ref(),
                "owned",
                key(1),
                4,
                Sha256::digest(b"data").into()
            )
            .is_err()
        );
    }
    assert_eq!(root.usage(None).unwrap().objects, 1);
}

struct Partial {
    emitted: bool,
}
impl Read for Partial {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        if self.emitted {
            return Err(io::Error::other("injected reader failure"));
        }
        self.emitted = true;
        out[..2].copy_from_slice(b"da");
        Ok(2)
    }
}

#[test]
fn interrupted_prefix_remains_charged_and_same_input_resumes_after_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let root = RetainedRoot::open(dir.path().to_owned(), Some(tiny())).unwrap();
    assert!(
        root.install(
            None,
            "interrupted",
            Some(key(1)),
            4,
            Sha256::digest(b"data").into(),
            Partial { emitted: false }
        )
        .is_err()
    );
    let charged = root.usage(None).unwrap();
    assert_eq!(charged.bytes, 548);
    assert_eq!(charged.staging_bytes, 4);
    drop(root);
    let reopened = RetainedRoot::open(dir.path().to_owned(), None).unwrap();
    assert_eq!(reopened.usage(None).unwrap(), charged);
    assert!(
        reopened
            .load(
                None,
                "interrupted",
                key(1),
                4,
                Sha256::digest(b"data").into()
            )
            .is_err()
    );
    install(&reopened, None, "interrupted", 1, b"data").unwrap();
    assert_eq!(reopened.usage(None).unwrap().bytes, charged.bytes);
    assert_eq!(reopened.usage(None).unwrap().staging_objects, 0);
    let mut body = Vec::new();
    reopened
        .load(
            None,
            "interrupted",
            key(1),
            4,
            Sha256::digest(b"data").into(),
        )
        .unwrap()
        .reader()
        .read_to_end(&mut body)
        .unwrap();
    assert_eq!(body, b"data");
}

#[test]
fn missing_receipt_or_changed_payload_is_not_successful_work() {
    let dir = tempfile::tempdir().unwrap();
    let root = RetainedRoot::open(dir.path().to_owned(), None).unwrap();
    install(&root, None, "work", 1, b"data").unwrap();
    let data = dir.path().join("work/scope-0/entity-1.bin");
    fs::write(&data, b"bad!").unwrap();
    assert!(
        root.load(None, "work", key(1), 4, Sha256::digest(b"data").into())
            .is_err()
    );
    assert!(install(&root, None, "work", 1, b"data").is_err());
    fs::remove_file(suffix(&data, ".done")).unwrap();
    assert!(
        root.load(None, "work", key(1), 4, Sha256::digest(b"data").into())
            .is_err()
    );
}

#[test]
fn growing_reader_cannot_write_beyond_its_reserved_length() {
    let dir = tempfile::tempdir().unwrap();
    let root = RetainedRoot::open(dir.path().to_owned(), Some(tiny())).unwrap();
    assert!(
        root.install(
            None,
            "overflow",
            Some(key(1)),
            2,
            Sha256::digest(b"ok").into(),
            Cursor::new(b"oversized")
        )
        .is_err()
    );
    assert_eq!(
        fs::metadata(dir.path().join("overflow/scope-0/entity-1.bin.stage"))
            .unwrap()
            .len(),
        0
    );
    assert_eq!(root.usage(None).unwrap().staging_bytes, 2);
    assert_eq!(root.usage(None).unwrap().objects, 1);
}

#[test]
fn nonempty_legacy_layout_and_policy_changes_are_not_converted() {
    let dir = tempfile::tempdir().unwrap();
    fs::write(dir.path().join("old.bin"), b"keep").unwrap();
    assert!(RetainedRoot::open(dir.path().to_owned(), None).is_err());
    assert_eq!(fs::read(dir.path().join("old.bin")).unwrap(), b"keep");
    assert!(!dir.path().join(".retained-policy").exists());
    let dir = tempfile::tempdir().unwrap();
    let root = RetainedRoot::open(dir.path().to_owned(), Some(tiny())).unwrap();
    fs::write(
        dir.path().join(".retained-policy"),
        RetainedLimits::default().encode(),
    )
    .unwrap();
    assert!(install(&root, None, "work", 1, b"data").is_err());
}

#[test]
fn abrupt_exit_during_payload_copy_keeps_reservation_and_resumes_without_overwrite() {
    const CHILD: &str = "PIPESTREAM_RETAINED_CRASH_CHILD";
    if let Some(path) = std::env::var_os(CHILD) {
        struct CrashReader(bool);
        impl Read for CrashReader {
            fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
                if self.0 {
                    std::process::exit(43);
                }
                self.0 = true;
                buffer[..2].copy_from_slice(b"da");
                Ok(2)
            }
        }
        let root = RetainedRoot::open(PathBuf::from(path), Some(tiny())).unwrap();
        root.install(
            None,
            "crashed",
            Some(key(1)),
            4,
            Sha256::digest(b"data").into(),
            CrashReader(false),
        )
        .unwrap();
        panic!("crash reader returned");
    }
    let dir = tempfile::tempdir().unwrap();
    let storage = dir.path().join("new-parent/entities");
    let child = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "recursive::retained::tests::abrupt_exit_during_payload_copy_keeps_reservation_and_resumes_without_overwrite", "--nocapture"])
        .env(CHILD, &storage).output().unwrap();
    assert_eq!(
        child.status.code(),
        Some(43),
        "{}",
        String::from_utf8_lossy(&child.stderr)
    );
    let root = RetainedRoot::open(storage.clone(), None).unwrap();
    assert_eq!(root.usage(None).unwrap().staging_bytes, 4);
    assert_eq!(root.usage(None).unwrap().objects, 1);
    assert!(!storage.join("crashed/scope-0/entity-1.bin").exists());
    install(&root, None, "crashed", 1, b"data").unwrap();
    assert_eq!(
        fs::read(storage.join("crashed/scope-0/entity-1.bin")).unwrap(),
        b"data"
    );
    assert_eq!(root.usage(None).unwrap().staging_objects, 0);
}

#[test]
fn writer_lock_is_retained_by_payload_readers_and_spool_loans() {
    const CHILD: &str = "PIPESTREAM_RETAINED_LOCK_CHILD";
    if let Some(path) = std::env::var_os(CHILD) {
        let opened = RetainedRoot::open(PathBuf::from(path), None);
        std::process::exit(if opened.is_ok() { 0 } else { 44 });
    }
    let dir = tempfile::tempdir().unwrap();
    let store = super::super::FileEntityStore::open(dir.path()).unwrap();
    use super::super::EntityStore;
    store.put("leased", key(1), b"data").unwrap();
    let payload = store
        .load_payload(None, "leased", key(1), 4, Sha256::digest(b"data").into())
        .unwrap();
    let spool = store.spool().clone();
    drop(store);
    let probe = || {
        std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "recursive::retained::tests::writer_lock_is_retained_by_payload_readers_and_spool_loans", "--nocapture"])
        .env(CHILD, dir.path()).output().unwrap().status.code()
    };
    assert_eq!(probe(), Some(44));
    drop(spool);
    assert_eq!(probe(), Some(44));
    drop(payload);
    assert_eq!(probe(), Some(0));
}

#[test]
fn a_stalled_copy_does_not_hold_the_global_accounting_mutex() {
    let dir = tempfile::tempdir().unwrap();
    let root = RetainedRoot::open(dir.path().to_owned(), Some(tiny())).unwrap();
    let alice = PrincipalBinding::new("issuer", "alice").unwrap();
    let bob = PrincipalBinding::new("issuer", "bob").unwrap();
    let (entered_tx, entered_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    struct HeldReader {
        entered: Option<std::sync::mpsc::Sender<()>>,
        release: std::sync::mpsc::Receiver<()>,
        source: Cursor<&'static [u8]>,
    }
    impl Read for HeldReader {
        fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
            if let Some(entered) = self.entered.take() {
                entered.send(()).unwrap();
                self.release.recv().unwrap();
            }
            self.source.read(buffer)
        }
    }
    std::thread::scope(|scope| {
        let writer = scope.spawn(|| {
            root.install(
                Some(&alice),
                "alice",
                Some(key(1)),
                4,
                Sha256::digest(b"data").into(),
                HeldReader {
                    entered: Some(entered_tx),
                    release: release_rx,
                    source: Cursor::new(b"data"),
                },
            )
        });
        entered_rx
            .recv_timeout(std::time::Duration::from_secs(2))
            .unwrap();
        assert_eq!(root.usage(None).unwrap().staging_objects, 1);
        install(&root, Some(&bob), "bob", 1, b"other").unwrap();
        assert_eq!(root.usage(None).unwrap().objects, 2);
        assert_limit(install(&root, Some(&alice), "alice", 1, b"data").unwrap_err());
        release_tx.send(()).unwrap();
        writer.join().unwrap().unwrap();
    });
    assert_eq!(root.usage(None).unwrap().staging_objects, 0);
}

#[test]
fn incomplete_metadata_does_not_hide_its_charge_or_block_existing_committed_work() {
    for prefix_length in [0, 8, 64, 128, 511] {
        let dir = tempfile::tempdir().unwrap();
        let root = RetainedRoot::open(dir.path().to_owned(), Some(tiny())).unwrap();
        install(&root, None, "ready", 1, b"data").unwrap();
        let principal = PrincipalBinding::new("issuer", "alice").unwrap();
        let record = Record::new(
            Some(&principal),
            "partial",
            Some(key(1)),
            4,
            Sha256::digest(b"data").into(),
        )
        .unwrap();
        let base = record.path(dir.path());
        fs::create_dir_all(base.parent().unwrap()).unwrap();
        // Crash image before metadata publication finishes. No body may have
        // been staged yet because metadata fsync precedes opening the stage.
        write_new(&suffix(&base, ".meta"), &record.encode()[..prefix_length]).unwrap();
        drop(root);
        let reopened = RetainedRoot::open(dir.path().to_owned(), None).unwrap();
        assert_eq!(reopened.usage(None).unwrap().incomplete_metadata, 1);
        assert_eq!(reopened.usage(None).unwrap().objects, 2);
        assert_eq!(reopened.usage(None).unwrap().bytes, 548 + 512);
        assert!(
            reopened
                .load(None, "ready", key(1), 4, Sha256::digest(b"data").into())
                .is_ok()
        );
        install(&reopened, Some(&principal), "partial", 1, b"data").unwrap();
        assert_eq!(reopened.usage(None).unwrap().incomplete_metadata, 0);
        assert_eq!(reopened.usage(None).unwrap().objects, 2);
        assert_eq!(reopened.usage(None).unwrap().bytes, 2 * 548);
        assert_eq!(reopened.usage(Some(Some(&principal))).unwrap().objects, 1);
    }
}

#[test]
fn partial_receipt_cannot_admit_work_but_matching_replay_finishes_publication() {
    let dir = tempfile::tempdir().unwrap();
    let root = RetainedRoot::open(dir.path().to_owned(), Some(tiny())).unwrap();
    let record = Record::new(
        None,
        "receipt",
        Some(key(1)),
        4,
        Sha256::digest(b"data").into(),
    )
    .unwrap();
    let base = record.path(dir.path());
    fs::create_dir_all(base.parent().unwrap()).unwrap();
    write_new(&suffix(&base, ".meta"), &record.encode()).unwrap();
    write_new(&suffix(&base, ".stage"), b"data").unwrap();
    fs::hard_link(suffix(&base, ".stage"), &base).unwrap();
    write_new(&suffix(&base, ".done"), &record.encode()[480..496]).unwrap();
    drop(root);
    let root = RetainedRoot::open(dir.path().to_owned(), None).unwrap();
    assert!(
        root.load(None, "receipt", key(1), 4, record.digest)
            .is_err()
    );
    assert_eq!(root.usage(None).unwrap().staging_bytes, 4);
    install(&root, None, "receipt", 1, b"data").unwrap();
    assert!(root.load(None, "receipt", key(1), 4, record.digest).is_ok());
    assert_eq!(root.usage(None).unwrap().staging_bytes, 0);
    assert!(!suffix(&base, ".stage").exists());
}

#[test]
fn interrupted_directory_creation_is_counted_and_does_not_block_existing_work() {
    let dir = tempfile::tempdir().unwrap();
    let root = RetainedRoot::open(dir.path().to_owned(), Some(tiny())).unwrap();
    install(&root, None, "ready", 1, b"data").unwrap();
    for session in ["unfinished1", "unfinished2", "unfinished3"] {
        fs::create_dir_all(dir.path().join(session).join("scope-0")).unwrap();
    }
    drop(root);
    let root = RetainedRoot::open(dir.path().to_owned(), None).unwrap();
    assert_eq!(root.usage(None).unwrap().directories, 8);
    assert_eq!(root.usage(None).unwrap().objects, 1);
    assert!(
        root.load(None, "ready", key(1), 4, Sha256::digest(b"data").into())
            .is_ok()
    );
    assert_limit(install(&root, None, "new-session", 1, b"data").unwrap_err());
    assert!(!dir.path().join("new-session").exists());
    install(&root, None, "unfinished1", 1, b"data").unwrap();
    assert_eq!(root.usage(None).unwrap().directories, 8);
    drop(root);
    let root = RetainedRoot::open(dir.path().to_owned(), None).unwrap();
    assert_eq!(root.usage(None).unwrap().directories, 8);
    assert_eq!(root.usage(None).unwrap().objects, 2);
    assert!(dir.path().join("unfinished2/scope-0").is_dir());
}

#[test]
fn retry_before_metadata_creation_reuses_the_live_reservation() {
    let dir = tempfile::tempdir().unwrap();
    let root = RetainedRoot::open(dir.path().to_owned(), Some(tiny())).unwrap();
    let record = Record::new(
        None,
        "unfinished",
        Some(key(1)),
        4,
        Sha256::digest(b"data").into(),
    )
    .unwrap();
    // The operation reserved space but never reached directory or metadata I/O.
    drop(root.start(record).unwrap());
    assert!(!dir.path().join("unfinished").exists());
    assert_eq!(root.usage(None).unwrap().directories, 2);
    install(&root, None, "unfinished", 1, b"data").unwrap();
    assert_eq!(root.usage(None).unwrap().objects, 1);
    assert_eq!(root.usage(None).unwrap().staging_objects, 0);
}

#[test]
fn incomplete_metadata_refusal_preserves_unattributed_credit() {
    let dir = tempfile::tempdir().unwrap();
    let root = RetainedRoot::open(dir.path().to_owned(), Some(tiny())).unwrap();
    let alice = PrincipalBinding::new("issuer", "alice").unwrap();
    let bob = PrincipalBinding::new("issuer", "bob").unwrap();
    install(&root, Some(&alice), "alice", 1, b"data").unwrap();
    install(&root, Some(&alice), "alice", 2, b"data").unwrap();
    let record = Record::new(
        Some(&alice),
        "partial",
        Some(key(1)),
        4,
        Sha256::digest(b"data").into(),
    )
    .unwrap();
    let base = record.path(dir.path());
    fs::create_dir_all(base.parent().unwrap()).unwrap();
    write_new(&suffix(&base, ".meta"), &record.encode()[..511]).unwrap();
    drop(root);
    let root = RetainedRoot::open(dir.path().to_owned(), None).unwrap();
    let before = root.usage(None).unwrap();
    assert_eq!(before.incomplete_metadata, 1);
    assert_limit(install(&root, Some(&alice), "partial", 1, b"data").unwrap_err());
    assert!(install(&root, Some(&bob), "partial", 1, b"data").is_err());
    assert_eq!(root.usage(None).unwrap(), before);
    assert_eq!(
        fs::read(suffix(&base, ".meta")).unwrap(),
        record.encode()[..511]
    );
    assert!(!suffix(&base, ".stage").exists());
    drop(root);
    assert_eq!(
        RetainedRoot::open(dir.path().to_owned(), None)
            .unwrap()
            .usage(None)
            .unwrap(),
        before
    );
}

#[cfg(unix)]
#[test]
fn only_the_matching_payload_staging_pair_may_share_an_inode() {
    for layout in 0..4 {
        let dir = tempfile::tempdir().unwrap();
        let external = tempfile::tempdir().unwrap();
        let root = RetainedRoot::open(dir.path().to_owned(), Some(tiny())).unwrap();
        install(&root, None, "work", 1, b"data").unwrap();
        let base = dir.path().join("work/scope-0/entity-1.bin");
        let stage = suffix(&base, ".stage");
        match layout {
            0 => fs::hard_link(&base, external.path().join("alias")).unwrap(),
            1 => fs::write(&stage, b"data").unwrap(),
            2 => {
                fs::hard_link(&base, &stage).unwrap();
                fs::hard_link(&base, external.path().join("alias")).unwrap();
            }
            _ => std::os::unix::fs::symlink(&base, &stage).unwrap(),
        }
        assert!(install(&root, None, "work", 1, b"data").is_err());
        drop(root);
        assert!(RetainedRoot::open(dir.path().to_owned(), None).is_err());
        assert_eq!(fs::read(&base).unwrap(), b"data");
    }
}

#[test]
fn foreign_or_excess_empty_directories_are_not_free_capacity() {
    for invalid in ["work/scope-00", "work/not-a-scope", "bad.session"] {
        let dir = tempfile::tempdir().unwrap();
        drop(RetainedRoot::open(dir.path().to_owned(), Some(tiny())).unwrap());
        fs::create_dir_all(dir.path().join(invalid)).unwrap();
        assert!(RetainedRoot::open(dir.path().to_owned(), None).is_err());
        assert!(dir.path().join(invalid).is_dir());
    }
    let dir = tempfile::tempdir().unwrap();
    drop(RetainedRoot::open(dir.path().to_owned(), Some(tiny())).unwrap());
    for id in 0..9 {
        fs::create_dir(dir.path().join(format!("orphan{id}"))).unwrap();
    }
    assert_limit(RetainedRoot::open(dir.path().to_owned(), None).unwrap_err());
}

#[test]
fn byte_staging_and_principal_budgets_refuse_before_new_disk_creation() {
    let cases = [
        RetainedLimits {
            bytes: 547,
            principal_bytes: 547,
            staging_bytes: 512,
            ..tiny()
        },
        RetainedLimits {
            principal_bytes: 547,
            ..tiny()
        },
        RetainedLimits {
            staging_bytes: 3,
            ..tiny()
        },
    ];
    for limits in cases {
        let dir = tempfile::tempdir().unwrap();
        let root = RetainedRoot::open(dir.path().to_owned(), Some(limits)).unwrap();
        assert_limit(install(&root, None, "refused", 1, b"data").unwrap_err());
        assert_eq!(root.usage(None).unwrap(), RetainedUsage::default());
        assert!(!dir.path().join("refused").exists());
    }
    let dir = tempfile::tempdir().unwrap();
    let root = RetainedRoot::open(
        dir.path().to_owned(),
        Some(RetainedLimits {
            principals: 1,
            staging_objects: 1,
            ..tiny()
        }),
    )
    .unwrap();
    let alice = PrincipalBinding::new("issuer", "alice").unwrap();
    let bob = PrincipalBinding::new("issuer", "bob").unwrap();
    assert!(
        root.install(
            Some(&alice),
            "alice",
            Some(key(1)),
            4,
            Sha256::digest(b"data").into(),
            Partial { emitted: false }
        )
        .is_err()
    );
    let pending = root.usage(None).unwrap();
    assert_limit(install(&root, Some(&alice), "alice", 2, b"data").unwrap_err());
    assert_eq!(root.usage(None).unwrap(), pending);
    install(&root, Some(&alice), "alice", 1, b"data").unwrap();
    let complete = root.usage(None).unwrap();
    assert_limit(install(&root, Some(&bob), "bob", 1, b"data").unwrap_err());
    assert_eq!(root.usage(None).unwrap(), complete);
    assert!(!dir.path().join("bob").exists());
    drop(root);
    assert_eq!(
        RetainedRoot::open(dir.path().to_owned(), None)
            .unwrap()
            .usage(None)
            .unwrap(),
        complete
    );
}

#[test]
fn lineage_uses_the_same_retained_owner_and_byte_accounting() {
    let dir = tempfile::tempdir().unwrap();
    let root = RetainedRoot::open(dir.path().to_owned(), Some(tiny())).unwrap();
    let alice = PrincipalBinding::new("issuer", "alice").unwrap();
    let lineage = [7; 32];
    let digest = Sha256::digest(lineage).into();
    install(&root, Some(&alice), "work", 1, b"data").unwrap();
    root.reserve_lineage(Some(&alice), "work").unwrap();
    root.install(Some(&alice), "work", None, 32, digest, Cursor::new(lineage))
        .unwrap();
    let before = root.usage(None).unwrap();
    assert_eq!(before.bytes, 548 + lineage::CHARGE);
    assert_eq!(before.objects, 2);
    assert_eq!(root.usage(Some(Some(&alice))).unwrap().bytes, before.bytes);
    root.install(Some(&alice), "work", None, 32, digest, Cursor::new(lineage))
        .unwrap();
    assert_eq!(root.usage(None).unwrap(), before);
    assert!(
        root.install(None, "work", None, 32, digest, Cursor::new(lineage))
            .is_err()
    );
    assert_limit(install(&root, Some(&alice), "work", 2, b"data").unwrap_err());
    drop(root);
    let root = RetainedRoot::open(dir.path().to_owned(), None).unwrap();
    assert_eq!(root.usage(None).unwrap(), before);
    assert_eq!(
        fs::read(dir.path().join("work/lineage.sha256")).unwrap(),
        lineage
    );
}
