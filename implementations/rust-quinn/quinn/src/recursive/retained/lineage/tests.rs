use super::*;
use crate::recursive::{EntityStore, FileEntityStore, SpoolLimits};
use std::io::Cursor;

fn limits() -> RetainedLimits {
    RetainedLimits {
        bytes: CHARGE + 548,
        principal_bytes: CHARGE + 548,
        objects: 2,
        principal_objects: 2,
        staging_bytes: 4,
        staging_objects: 1,
        principals: 1,
    }
}

fn key() -> EntityKey {
    EntityKey {
        scope_id: 0,
        entity_id: 1,
    }
}

#[test]
fn admitted_payload_protects_lineage_at_full_object_byte_and_staging_quota() {
    let dir = tempfile::tempdir().unwrap();
    let files =
        FileEntityStore::open_with_limits(dir.path(), SpoolLimits::default(), limits()).unwrap();
    files.put("work", key(), b"data").unwrap();
    let before = files.retained_usage().unwrap();
    assert_eq!(before.bytes, limits().bytes);
    assert_eq!(before.objects, limits().objects);
    assert_eq!(before.lineage_reservations, 1);
    assert_eq!(before.staging_bytes, 0);
    assert!(
        !dir.path().join("work/lineage.sha256").exists(),
        "reservation is not a digest"
    );
    super::super::tests::assert_limit(
        files
            .put(
                "work",
                EntityKey {
                    entity_id: 2,
                    ..key()
                },
                b"data",
            )
            .unwrap_err(),
    );
    super::super::tests::assert_limit(files.reserve_lineage(None, "other").unwrap_err());
    files.put_lineage(None, "work", [7; 32]).unwrap();
    assert_eq!(files.retained_usage().unwrap(), before);
    assert_eq!(
        fs::read(dir.path().join("work/lineage.sha256")).unwrap(),
        [7; 32]
    );
    files.put_lineage(None, "work", [7; 32]).unwrap();
    assert!(files.put_lineage(None, "work", [8; 32]).is_err());
    drop(files);
    let files = FileEntityStore::open(dir.path()).unwrap();
    assert_eq!(files.retained_usage().unwrap(), before);
    files.put_lineage(None, "work", [7; 32]).unwrap();
}

#[test]
fn partial_markers_remain_charged_until_matching_durable_reservation() {
    let alice = PrincipalBinding::new("issuer", "alice").unwrap();
    let reservation = Reservation::new(Some(&alice), "work").unwrap();
    for length in [0, 1, 8, 80, 396, 480, 511, 512] {
        let dir = tempfile::tempdir().unwrap();
        let root = RetainedRoot::open(dir.path().to_owned(), Some(limits())).unwrap();
        fs::create_dir(dir.path().join("work")).unwrap();
        fs::write(
            reservation.path(dir.path()),
            &reservation.encode()[..length],
        )
        .unwrap();
        drop(root);
        let files = FileEntityStore::open(dir.path()).unwrap();
        let initial = files.retained_usage().unwrap();
        assert_eq!(initial.bytes, CHARGE);
        assert_eq!(initial.lineage_reservations, 1);
        assert_eq!(
            initial.incomplete_lineage_reservations,
            u64::from(length < RECORD_BYTES)
        );
        if length < RECORD_BYTES {
            assert!(files.put_lineage(Some(&alice), "work", [9; 32]).is_err());
        }
        files.reserve_lineage(Some(&alice), "work").unwrap();
        assert_eq!(files.retained_usage().unwrap().bytes, CHARGE);
        assert_eq!(
            files
                .retained_usage()
                .unwrap()
                .incomplete_lineage_reservations,
            0
        );
        assert_eq!(
            files.principal_retained_usage(Some(&alice)).unwrap().bytes,
            CHARGE
        );
        files.put_lineage(Some(&alice), "work", [9; 32]).unwrap();
    }
}

#[test]
fn missing_corrupt_or_foreign_marker_never_becomes_free_admission_capacity() {
    for corrupt_marker in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let files = FileEntityStore::open(dir.path()).unwrap();
        files.put("work", key(), b"data").unwrap();
        let marker = dir.path().join("work/lineage.reserve");
        if corrupt_marker {
            let mut bytes = fs::read(&marker).unwrap();
            bytes[0] ^= 1;
            fs::write(&marker, bytes).unwrap();
        } else {
            fs::remove_file(&marker).unwrap();
        }
        assert!(
            files
                .load_payload(None, "work", key(), 4, Sha256::digest(b"data").into())
                .is_err()
        );
        assert!(files.put_lineage(None, "work", [9; 32]).is_err());
        assert!(files.put("work", key(), b"data").is_err());
        drop(files);
        assert!(FileEntityStore::open(dir.path()).is_err());
        assert_eq!(
            fs::read(dir.path().join("work/scope-0/entity-1.bin")).unwrap(),
            b"data"
        );
    }
    let dir = tempfile::tempdir().unwrap();
    let files = FileEntityStore::open(dir.path()).unwrap();
    let alice = PrincipalBinding::new("issuer", "alice").unwrap();
    files.reserve_lineage(Some(&alice), "work").unwrap();
    let before = files.retained_usage().unwrap();
    for principal in [
        None,
        Some(PrincipalBinding::new("issuer", "bob").unwrap()),
        Some(PrincipalBinding::new("other", "alice").unwrap()),
    ] {
        let error = files
            .reserve_lineage(principal.as_ref(), "work")
            .unwrap_err();
        assert_eq!(
            error
                .get_ref()
                .and_then(|error| error.downcast_ref::<pipestream_core::ProtocolError>())
                .map(|error| error.code),
            Some(pipestream_core::authorization::ERROR_UNAUTHORIZED)
        );
        assert!(
            files
                .put_lineage(principal.as_ref(), "work", [8; 32])
                .is_err()
        );
    }
    assert_eq!(files.retained_usage().unwrap(), before);
}

#[test]
fn interrupted_lineage_publication_reopens_without_ordinary_staging_credit() {
    let dir = tempfile::tempdir().unwrap();
    let files =
        FileEntityStore::open_with_limits(dir.path(), SpoolLimits::default(), limits()).unwrap();
    files.put("work", key(), b"data").unwrap();
    let before = files.retained_usage().unwrap();
    let root = &files.retained;
    let body = [7; 32];
    assert!(
        root.install(
            None,
            "work",
            None,
            32,
            Sha256::digest(body).into(),
            Cursor::new(&body[..16])
        )
        .is_err()
    );
    assert_eq!(files.retained_usage().unwrap(), before);
    assert_eq!(
        fs::metadata(dir.path().join("work/lineage.sha256.stage"))
            .unwrap()
            .len(),
        16
    );
    drop(files);
    let files = FileEntityStore::open(dir.path()).unwrap();
    assert_eq!(files.retained_usage().unwrap(), before);
    files.put_lineage(None, "work", body).unwrap();
    assert!(!dir.path().join("work/lineage.sha256.stage").exists());
    assert_eq!(files.retained_usage().unwrap(), before);
}

#[test]
fn abrupt_exit_after_payload_installation_keeps_lineage_headroom() {
    const CHILD: &str = "PIPESTREAM_LINEAGE_RESERVATION_CRASH_ROOT";
    if let Some(path) = std::env::var_os(CHILD) {
        let files =
            FileEntityStore::open_with_limits(path, SpoolLimits::default(), limits()).unwrap();
        files.put("work", key(), b"data").unwrap();
        std::process::exit(37);
    }
    let dir = tempfile::tempdir().unwrap();
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "recursive::retained::lineage::tests::abrupt_exit_after_payload_installation_keeps_lineage_headroom", "--nocapture"])
        .env(CHILD, dir.path()).output().unwrap();
    assert_eq!(
        output.status.code(),
        Some(37),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let files = FileEntityStore::open(dir.path()).unwrap();
    assert_eq!(files.retained_usage().unwrap().bytes, limits().bytes);
    files.put_lineage(None, "work", [7; 32]).unwrap();
}

#[test]
fn partial_lineage_metadata_uses_prepaid_capacity_without_overwriting_a_different_digest() {
    let body = [7; 32];
    let record = Record::new(None, "work", None, 32, Sha256::digest(body).into()).unwrap();
    for length in [0, 1, 8, 396, 412, 444, 480, 511] {
        let dir = tempfile::tempdir().unwrap();
        let files = FileEntityStore::open_with_limits(dir.path(), SpoolLimits::default(), limits())
            .unwrap();
        files.put("work", key(), b"data").unwrap();
        let before = files.retained_usage().unwrap();
        let metadata = suffix(&record.path(dir.path()), ".meta");
        fs::write(&metadata, &record.encode()[..length]).unwrap();
        drop(files);
        let files = FileEntityStore::open(dir.path()).unwrap();
        assert_eq!(files.retained_usage().unwrap(), before);
        if length >= 444 {
            assert!(files.put_lineage(None, "work", [8; 32]).is_err());
            assert_eq!(fs::read(&metadata).unwrap(), record.encode()[..length]);
            assert_eq!(files.retained_usage().unwrap(), before);
        }
        files.put_lineage(None, "work", body).unwrap();
        assert_eq!(files.retained_usage().unwrap(), before);
        assert_eq!(fs::read(record.path(dir.path())).unwrap(), body);
    }
}

#[test]
fn lineage_publication_and_receipt_crash_images_replay_at_full_quota() {
    let body = [7; 32];
    let record = Record::new(None, "work", None, 32, Sha256::digest(body).into()).unwrap();
    for receipt_length in [None, Some(0), Some(1), Some(16), Some(31), Some(32)] {
        let dir = tempfile::tempdir().unwrap();
        let files = FileEntityStore::open_with_limits(dir.path(), SpoolLimits::default(), limits())
            .unwrap();
        files.put("work", key(), b"data").unwrap();
        let before = files.retained_usage().unwrap();
        let base = record.path(dir.path());
        fs::write(suffix(&base, ".meta"), record.encode()).unwrap();
        fs::write(suffix(&base, ".stage"), body).unwrap();
        fs::hard_link(suffix(&base, ".stage"), &base).unwrap();
        if let Some(length) = receipt_length {
            fs::write(suffix(&base, ".done"), &record.encode()[480..480 + length]).unwrap();
        }
        drop(files);
        let files = FileEntityStore::open(dir.path()).unwrap();
        assert_eq!(files.retained_usage().unwrap(), before);
        assert!(files.put_lineage(None, "work", [8; 32]).is_err());
        files.put_lineage(None, "work", body).unwrap();
        assert_eq!(files.retained_usage().unwrap(), before);
        assert!(!suffix(&base, ".stage").exists());
        assert_eq!(
            fs::read(suffix(&base, ".done")).unwrap(),
            record.encode()[480..]
        );
    }
}

#[test]
fn refused_partial_marker_promotion_preserves_its_unattributed_charge() {
    let dir = tempfile::tempdir().unwrap();
    let alice = PrincipalBinding::new("issuer", "alice").unwrap();
    let reservation = Reservation::new(Some(&alice), "second").unwrap();
    let root = RetainedRoot::open(
        dir.path().to_owned(),
        Some(RetainedLimits {
            bytes: CHARGE * 2,
            principal_bytes: CHARGE,
            objects: 2,
            principal_objects: 1,
            ..limits()
        }),
    )
    .unwrap();
    root.reserve_lineage(Some(&alice), "first").unwrap();
    fs::create_dir(dir.path().join("second")).unwrap();
    fs::write(reservation.path(dir.path()), &reservation.encode()[..396]).unwrap();
    drop(root);
    let files = FileEntityStore::open(dir.path()).unwrap();
    let before = files.retained_usage().unwrap();
    assert_eq!(before.bytes, CHARGE * 2);
    assert_eq!(before.incomplete_lineage_reservations, 1);
    for _ in 0..2 {
        super::super::tests::assert_limit(
            files.reserve_lineage(Some(&alice), "second").unwrap_err(),
        );
        assert_eq!(files.retained_usage().unwrap(), before);
        assert_eq!(
            files.principal_retained_usage(Some(&alice)).unwrap().bytes,
            CHARGE
        );
        assert_eq!(
            fs::read(reservation.path(dir.path())).unwrap(),
            reservation.encode()[..396]
        );
    }
    files.put_lineage(Some(&alice), "first", [7; 32]).unwrap();
}

#[test]
fn retry_before_marker_creation_keeps_the_same_reservation() {
    let dir = tempfile::tempdir().unwrap();
    let files =
        FileEntityStore::open_with_limits(dir.path(), SpoolLimits::default(), limits()).unwrap();
    let parent = dir.path().join("work");
    fs::write(&parent, b"directory creation fault").unwrap();
    assert!(files.reserve_lineage(None, "work").is_err());
    let before = files.retained_usage().unwrap();
    assert_eq!(before.bytes, CHARGE);
    fs::remove_file(parent).unwrap();
    files.reserve_lineage(None, "work").unwrap();
    assert_eq!(files.retained_usage().unwrap(), before);
    files.put_lineage(None, "work", [7; 32]).unwrap();
}

#[test]
fn invalid_payload_identity_does_not_create_a_lineage_reservation() {
    let dir = tempfile::tempdir().unwrap();
    let files = FileEntityStore::open(dir.path()).unwrap();
    let before = files.retained_usage().unwrap();
    for entity_id in [0, u32::MAX] {
        assert!(
            files
                .put("work", EntityKey { entity_id, ..key() }, b"data")
                .is_err()
        );
        assert_eq!(files.retained_usage().unwrap(), before);
        assert!(!dir.path().join("work").exists());
    }
}

#[test]
fn concurrent_reservations_cannot_overbook_a_principal_and_old_policy_refuses() {
    let dir = tempfile::tempdir().unwrap();
    let files = Arc::new(
        FileEntityStore::open_with_limits(
            dir.path(),
            SpoolLimits::default(),
            RetainedLimits {
                bytes: CHARGE * 2,
                principal_bytes: CHARGE,
                objects: 2,
                principal_objects: 1,
                staging_bytes: 32,
                staging_objects: 2,
                principals: 2,
            },
        )
        .unwrap(),
    );
    let barrier = std::sync::Barrier::new(8);
    let accepted = std::thread::scope(|threads| {
        let handles: Vec<_> = (0..8)
            .map(|id| {
                let files = &files;
                let barrier = &barrier;
                threads.spawn(move || {
                    barrier.wait();
                    files
                        .reserve_lineage(
                            Some(&PrincipalBinding::new("issuer", "alice").unwrap()),
                            &format!("session-{id}"),
                        )
                        .is_ok()
                })
            })
            .collect();
        handles
            .into_iter()
            .filter_map(|handle| handle.join().unwrap().then_some(()))
            .count()
    });
    assert_eq!(accepted, 1);
    assert_eq!(files.retained_usage().unwrap().lineage_reservations, 1);
    files
        .reserve_lineage(
            Some(&PrincipalBinding::new("issuer", "bob").unwrap()),
            "bob",
        )
        .unwrap();
    assert_eq!(files.retained_usage().unwrap().lineage_reservations, 2);
    let policy = dir.path().join(".retained-policy");
    let mut bytes = fs::read(&policy).unwrap();
    bytes[..8].copy_from_slice(b"PSRET001");
    let checksum = Sha256::digest(&bytes[..64]);
    bytes[64..].copy_from_slice(&checksum);
    fs::write(&policy, &bytes).unwrap();
    drop(files);
    assert!(FileEntityStore::open(dir.path()).is_err());
    assert_eq!(fs::read(&policy).unwrap(), bytes);
}
