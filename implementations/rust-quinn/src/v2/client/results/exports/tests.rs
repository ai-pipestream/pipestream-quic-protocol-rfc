use super::super::tests::{fill, open, reference};
use super::*;
use std::process::Command;

thread_local! { static FAIL_SYNC: std::cell::Cell<usize> = const { std::cell::Cell::new(0) }; }
pub(super) fn fail_sync() -> bool {
    FAIL_SYNC.with(|flag| {
        let remaining = flag.get();
        flag.set(remaining.saturating_sub(1));
        remaining == 1
    })
}

#[test]
fn sync_failures_keep_ambiguous_exports_unusable_until_audited_reopen() {
    for removing in [false, true] {
        for sync in [1, 2] {
            let directory = tempfile::tempdir().unwrap();
            let selected = reference(directory.path(), "alice", b"abc");
            let copies = open(&directory.path().join("copies"), true);
            drop(fill(&copies, &selected, b"abc"));
            let path = directory.path().join("exports");
            let store = exports(&path, true);
            let id = OperationId([1; 16]);
            if removing {
                store
                    .export(id, &selected, &mut copies.find(&selected).unwrap().unwrap())
                    .unwrap();
            }
            FAIL_SYNC.with(|flag| flag.set(sync));
            if removing {
                assert!(store.remove(id).is_err());
            } else {
                assert!(
                    store
                        .export(id, &selected, &mut copies.find(&selected).unwrap().unwrap())
                        .is_err()
                );
            }
            assert!(store.usage().is_err());
            assert!(store.remove(id).is_err());
            assert!(
                store
                    .export(id, &selected, &mut copies.find(&selected).unwrap().unwrap())
                    .is_err()
            );
            drop(store);
            let store = exports(&path, false);
            if removing {
                assert!(!path.join(format!("file-{}", key(id).unwrap())).exists());
                assert_eq!(store.usage().unwrap().intents, usize::from(sync == 1));
                store.remove(id).unwrap();
                assert_eq!(store.usage().unwrap().charged_bytes, 0);
            } else {
                assert_eq!(store.usage().unwrap().charged_bytes, 115);
                let exported = store
                    .export(id, &selected, &mut copies.find(&selected).unwrap().unwrap())
                    .unwrap();
                assert_eq!(fs::read(exported.path).unwrap(), b"abc");
                assert_eq!(exported.replayed, sync == 2);
            }
        }
    }
}

#[test]
fn audit_counts_unfinished_intent_bytes_before_removing_any_file() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("exports");
    let policy = ExportPolicy {
        objects: 2,
        bytes: 112,
    };
    drop(
        ExportStore::initialize(
            &path,
            IdentityLabel("issuer-a".into()),
            IdentityLabel("alice".into()),
            policy,
        )
        .unwrap(),
    );
    let paths = [
        path.join(format!("intent-{}", "01".repeat(16))),
        path.join(format!("intent-{}", "02".repeat(16))),
    ];
    for path in &paths {
        fs::write(path, [0; RECORD]).unwrap();
    }
    assert!(
        ExportStore::open(
            &path,
            IdentityLabel("issuer-a".into()),
            IdentityLabel("alice".into()),
            policy
        )
        .is_err()
    );
    for path in paths {
        assert_eq!(fs::metadata(path).unwrap().len(), RECORD as u64);
    }
}

pub(super) fn crash_point(point: &str) {
    if std::env::var("PIPESTREAM_EXPORT_CRASH").ok().as_deref() == Some(point) {
        std::process::exit(78);
    }
}
fn policy() -> ExportPolicy {
    ExportPolicy {
        objects: 1,
        bytes: 112 + 1537,
    }
}
fn exports(path: &Path, fresh: bool) -> ExportStore {
    let open = if fresh {
        ExportStore::initialize
    } else {
        ExportStore::open
    };
    open(
        path,
        IdentityLabel("issuer-a".into()),
        IdentityLabel("alice".into()),
        policy(),
    )
    .unwrap()
}

#[test]
fn raw_exports_replay_exactly_preserve_identity_and_charge_pending_and_complete_bytes() {
    let directory = tempfile::tempdir().unwrap();
    let bytes = vec![0xda; 1537];
    let selected = reference(directory.path(), "alice", &bytes);
    let copies = open(&directory.path().join("copies"), true);
    drop(fill(&copies, &selected, &bytes));
    let path = directory.path().join("exports");
    let store = exports(&path, true);
    let id = OperationId([1; 16]);
    let first = store
        .export(id, &selected, &mut copies.find(&selected).unwrap().unwrap())
        .unwrap();
    assert!(!first.replayed);
    assert_eq!(fs::read(&first.path).unwrap(), bytes);
    assert_eq!(store.usage().unwrap().charged_bytes, 1649);
    assert!(
        store
            .export(
                OperationId([2; 16]),
                &selected,
                &mut copies.find(&selected).unwrap().unwrap()
            )
            .is_err()
    );
    drop(store);
    let store = exports(&path, false);
    assert!(
        store
            .export(id, &selected, &mut copies.find(&selected).unwrap().unwrap())
            .unwrap()
            .replayed
    );
    // Another valid selection with the same bytes but a different owner must not
    // repurpose the local export identity or use this owner's output directory.
    let bob = reference(directory.path(), "bob", &bytes);
    assert!(
        store
            .export(id, &bob, &mut copies.find(&selected).unwrap().unwrap())
            .is_err()
    );
    assert_eq!(fs::read(&first.path).unwrap(), bytes);
    assert!(store.remove(id).unwrap());
    assert!(!store.remove(id).unwrap());
    assert_eq!(store.usage().unwrap().charged_bytes, 0);
    assert!(copies.find(&selected).unwrap().is_some());
}

#[test]
fn damaged_existing_exports_are_not_overwritten_and_consumed_sources_do_not_publish_prefixes() {
    let directory = tempfile::tempdir().unwrap();
    let selected = reference(directory.path(), "alice", b"abc");
    let copies = open(&directory.path().join("copies"), true);
    drop(fill(&copies, &selected, b"abc"));
    let path = directory.path().join("exports");
    let store = exports(&path, true);
    let id = OperationId([1; 16]);
    let mut source = copies.find(&selected).unwrap().unwrap();
    source.read_chunk(&mut [0; 1]).unwrap();
    assert!(store.export(id, &selected, &mut source).is_err());
    assert!(!path.join(format!("file-{}", key(id).unwrap())).exists());
    assert!(store.usage().is_err());
    drop(store);
    let store = exports(&path, false);
    assert_eq!(store.usage().unwrap().recovered_stages, 1);
    let exported = store
        .export(id, &selected, &mut copies.find(&selected).unwrap().unwrap())
        .unwrap();
    fs::write(&exported.path, b"abd").unwrap();
    assert!(
        store
            .export(id, &selected, &mut copies.find(&selected).unwrap().unwrap())
            .is_err()
    );
    assert_eq!(fs::read(exported.path).unwrap(), b"abd");
}

#[test]
fn exported_empty_objects_still_have_immutable_intent_and_owner_checks() {
    let directory = tempfile::tempdir().unwrap();
    let selected = reference(directory.path(), "alice", &[]);
    let copies = open(&directory.path().join("copies"), true);
    drop(fill(&copies, &selected, &[]));
    let path = directory.path().join("exports");
    let store = exports(&path, true);
    let exported = store
        .export(
            OperationId([1; 16]),
            &selected,
            &mut copies.find(&selected).unwrap().unwrap(),
        )
        .unwrap();
    assert_eq!(fs::metadata(exported.path).unwrap().len(), 0);
    assert_eq!(store.usage().unwrap().charged_bytes, 112);
    assert!(
        ExportStore::open(
            &path,
            IdentityLabel("issuer-a".into()),
            IdentityLabel("alice".into()),
            policy()
        )
        .is_err()
    );
    drop(store);
    assert!(
        ExportStore::open(
            &path,
            IdentityLabel("issuer-a".into()),
            IdentityLabel("bob".into()),
            policy()
        )
        .is_err()
    );
    fs::write(path.join("unrelated"), b"keep").unwrap();
    fs::write(path.join(format!("intent-{}", "02".repeat(16))), []).unwrap();
    assert!(
        ExportStore::open(
            &path,
            IdentityLabel("issuer-a".into()),
            IdentityLabel("alice".into()),
            policy()
        )
        .is_err()
    );
    assert_eq!(fs::read(path.join("unrelated")).unwrap(), b"keep");
    assert!(path.join(format!("intent-{}", "02".repeat(16))).exists());
}

#[test]
fn subprocess_crashes_resume_original_export_intent_without_new_remote_work() {
    for point in ["intent", "body", "installed", "synced", "removed"] {
        let directory = tempfile::tempdir().unwrap();
        let output = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "v2::client::results::exports::tests::crash_child",
                "--nocapture",
            ])
            .env("PIPESTREAM_EXPORT_CRASH", point)
            .env("PIPESTREAM_EXPORT_DIR", directory.path())
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(78), "{output:?}");
        let selected = reference(directory.path(), "alice", b"abc");
        let copies = open(&directory.path().join("copies"), false);
        let store = exports(&directory.path().join("exports"), false);
        if point == "removed" {
            assert_eq!(store.usage().unwrap().intents, 0);
        } else {
            assert_eq!(store.usage().unwrap().charged_bytes, 115);
            let exported = store
                .export(
                    OperationId([1; 16]),
                    &selected,
                    &mut copies.find(&selected).unwrap().unwrap(),
                )
                .unwrap();
            assert_eq!(fs::read(exported.path).unwrap(), b"abc");
            assert_eq!(exported.replayed, matches!(point, "installed" | "synced"));
        }
    }
}

#[test]
fn crash_child() {
    let Ok(directory) = std::env::var("PIPESTREAM_EXPORT_DIR") else {
        return;
    };
    let directory = Path::new(&directory);
    let selected = reference(directory, "alice", b"abc");
    let copies = open(&directory.join("copies"), true);
    drop(fill(&copies, &selected, b"abc"));
    let store = exports(&directory.join("exports"), true);
    store
        .export(
            OperationId([1; 16]),
            &selected,
            &mut copies.find(&selected).unwrap().unwrap(),
        )
        .unwrap();
    store.remove(OperationId([1; 16])).unwrap();
    panic!("selected crash point was not reached");
}
