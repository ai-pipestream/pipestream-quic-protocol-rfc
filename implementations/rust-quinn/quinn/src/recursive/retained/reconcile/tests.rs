use super::*;
use crate::recursive::{EntityStore, FileEntityStore, SpoolLimits};
use pipestream_core::{
    EntityHeader, LayerSupport, ProtocolError,
    execution::{ExecutionKey, ExecutionStage},
    jobs::{JobOutput, ProcessOutcome},
    persistence::SessionStore,
    session::{EntityState, NewEntity, Session},
};
use std::{
    io::Cursor,
    time::{Duration, Instant},
};

fn key(id: u32) -> EntityKey {
    EntityKey {
        scope_id: 0,
        entity_id: id,
    }
}
fn base(root: &Path, id: u32) -> PathBuf {
    root.join("work/scope-0").join(format!("entity-{id}.bin"))
}
fn fixture() -> (tempfile::TempDir, SqliteSessionStore, FileEntityStore) {
    let dir = tempfile::tempdir().unwrap();
    let store = SqliteSessionStore::open(dir.path().join("sessions.sqlite3")).unwrap();
    let files = FileEntityStore::open(dir.path().join("payloads")).unwrap();
    files.bind_session_store(&store).unwrap();
    (dir, store, files)
}
fn queued(body: &[u8], owner: Option<PrincipalBinding>) -> (Session, ExecutionKey) {
    let mut session = Session::new("work", 7, 100).unwrap();
    if let Some(owner) = owner {
        session.bind_owner(owner).unwrap();
    }
    let digest = Sha256::digest(body).into();
    let entity = session
        .add_root(NewEntity {
            entity_id: 1,
            layer: 0,
            payload_digest: digest,
            policy: None,
        })
        .unwrap();
    session.transition(entity, EntityState::Processing).unwrap();
    let key = ExecutionKey {
        entity,
        stage: ExecutionStage::Process,
    };
    let header = EntityHeader {
        entity_id: 1,
        parent_id: None,
        scope_id: None,
        parent_scope_id: None,
        layer: 0,
        content_type: None,
        payload_length: Some(body.len() as u64),
        checksum: Some(digest),
        metadata: BTreeMap::new(),
        chunk_info: None,
        completion_policy: None,
    };
    session
        .enqueue_job(
            key,
            JobInput::Process {
                header,
                length: body.len() as u64,
                digest,
                layers: LayerSupport::LAYER1,
            },
            1,
        )
        .unwrap();
    (session, key)
}
fn reconcile(root: &Path, store: &SqliteSessionStore) -> Reconciliation {
    FileEntityStore::reconcile(root, SpoolLimits::default(), store).unwrap()
}

#[test]
fn full_quota_orphan_reclaims_body_but_keeps_identity_and_restores_at_original_limit() {
    let dir = tempfile::tempdir().unwrap();
    let store = SqliteSessionStore::open(dir.path().join("sessions.sqlite3")).unwrap();
    let root = dir.path().join("payloads");
    let bytes = 1120 + 512 + 32 + 1024;
    let limits = RetainedLimits {
        bytes,
        principal_bytes: bytes,
        objects: 2,
        principal_objects: 2,
        staging_bytes: 1024,
        staging_objects: 1,
        principals: 1,
    };
    let files = FileEntityStore::open_with_limits(&root, SpoolLimits::default(), limits).unwrap();
    files.bind_session_store(&store).unwrap();
    let payload = vec![7; 1024];
    files.put("work", key(1), &payload).unwrap();
    let before = files.retained_usage().unwrap();
    assert_eq!(before.bytes, bytes);
    assert_eq!(before.objects, 2);
    let metadata = fs::read(suffix(&base(&root, 1), ".meta")).unwrap();
    drop(files);
    let report = reconcile(&root, &store);
    assert_eq!(report.before, before);
    assert_eq!(report.after.bytes, 1120 + 512);
    assert_eq!(report.after.objects, before.objects);
    assert_eq!(report.orphan_bodies_removed, 1);
    assert_eq!(report.commitments_retained, 1);
    assert_eq!(report.file_bytes_removed, 1024 + 32);
    assert!(!base(&root, 1).exists());
    assert_eq!(
        fs::read(suffix(&base(&root, 1), ".commit")).unwrap(),
        metadata
    );
    assert_eq!(reconcile(&root, &store).file_bytes_removed, 0);
    let files = FileEntityStore::open(&root).unwrap();
    assert!(
        files
            .load_payload(None, "work", key(1), 1024, Sha256::digest(&payload).into())
            .is_err()
    );
    assert!(files.put("work", key(1), b"changed").is_err());
    assert_eq!(files.retained_usage().unwrap(), report.after);
    files.put("work", key(1), &payload).unwrap();
    assert_eq!(files.retained_usage().unwrap(), before);
    assert_eq!(
        fs::read(suffix(&base(&root, 1), ".meta")).unwrap(),
        metadata
    );
    assert!(!suffix(&base(&root, 1), ".commit").exists());
    assert!(store.list_session_ids().unwrap().is_empty());
    let (session, _) = queued(&payload, None);
    let original = store.create(&session).unwrap();
    drop(files);
    assert_eq!(reconcile(&root, &store).orphan_bodies_removed, 0);
    assert_eq!(store.load("work").unwrap().unwrap(), original);
}

#[test]
fn every_admitted_state_is_protected_including_refused_revoked_and_waiting_parent() {
    for mode in [
        "queued",
        "running",
        "finished",
        "refused",
        "revoked",
        "dehydrating",
    ] {
        let (_dir, store, files) = fixture();
        let root = files.root().to_owned();
        let owner = PrincipalBinding::new("issuer", "alice").unwrap();
        files
            .retained
            .install_payload(
                Some(&owner),
                "work",
                key(1),
                3,
                Sha256::digest(b"job").into(),
                Cursor::new(b"job"),
            )
            .unwrap();
        files
            .retained
            .install_payload(
                Some(&owner),
                "work",
                key(2),
                6,
                Sha256::digest(b"orphan").into(),
                Cursor::new(b"orphan"),
            )
            .unwrap();
        let (mut session, job) = queued(b"job", Some(owner.clone()));
        if mode != "queued" {
            let lease = session
                .acquire_job(Some(&owner), job, 2, 100)
                .unwrap()
                .unwrap();
            match mode {
                "finished" => {
                    session
                        .publish_job(Some(&owner), &lease, 3, |s| {
                            s.complete_entity(job.entity, [4; 32])?;
                            Ok(JobOutput::Processed(ProcessOutcome::Complete))
                        })
                        .unwrap();
                }
                "dehydrating" => {
                    session
                        .publish_job(Some(&owner), &lease, 3, |s| {
                            s.begin_dehydrating(job.entity)?;
                            Ok(JobOutput::Processed(ProcessOutcome::Dehydrate))
                        })
                        .unwrap();
                }
                "refused" => session
                    .refuse_job(
                        Some(&owner),
                        &lease,
                        3,
                        &ProtocolError::new(
                            pipestream_core::ERROR_ENTITY_INVALID,
                            "PIPESTREAM_ENTITY_INVALID",
                            "test refusal",
                        ),
                    )
                    .unwrap(),
                "revoked" => {
                    session.owner.as_mut().unwrap().revoked = true;
                }
                _ => (),
            }
        }
        let original = store.create(&session).unwrap();
        drop(files);
        let report = reconcile(&root, &store);
        assert_eq!(report.orphan_bodies_removed, 1, "{mode}");
        assert_eq!(fs::read(base(&root, 1)).unwrap(), b"job");
        assert!(!base(&root, 2).exists());
        assert_eq!(store.load("work").unwrap().unwrap(), original);
    }
}

#[test]
fn final_owner_release_unlocks_despite_an_inherited_file_description() {
    let (_dir, store, files) = fixture();
    let root = files.root().to_owned();
    files.put("work", key(1), b"orphan").unwrap();
    // An unrelated fork/exec child temporarily inherits this open-file
    // description even with CLOEXEC. A duplicate models that interval without
    // scheduling-dependent process creation or an unsafe post-fork callback.
    let inherited = files.retained._lock.file.try_clone().unwrap();
    drop(files);
    let report = reconcile(&root, &store);
    assert_eq!(report.orphan_bodies_removed, 1);
    assert_eq!(inherited.metadata().unwrap().len(), 0);
    drop(inherited);
}

#[test]
fn inherited_nonowner_guard_does_not_unlock_the_original_owner() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("lock");
    let file = File::create(&path).unwrap();
    let owner = RootLock::acquire(file).unwrap();
    // Model the child's process identity without fork or unsafe test code.
    let copied = RootLock {
        file: owner.file.try_clone().unwrap(),
        owner_process: std::process::id().wrapping_add(1),
    };
    drop(copied);
    let other = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&path)
        .unwrap();
    assert_eq!(
        lock_root(&other).unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
    drop(owner);
    lock_root(&other).unwrap();
}

#[test]
fn missing_corrupt_or_caller_managed_input_refuses_before_any_orphan_or_spool_deletion() {
    for fault in [
        "missing",
        "corrupt",
        "manual",
        "owner",
        "orphan",
        "index",
        "lineage",
        "partialreceipt",
    ] {
        let (_dir, store, files) = fixture();
        let root = files.root().to_owned();
        files.put("work", key(1), b"job").unwrap();
        files.put("work", key(2), b"orphan").unwrap();
        fs::create_dir(root.join(".spool")).unwrap();
        let spool = root.join(".spool/pipestream-Abc123");
        fs::write(&spool, b"abandoned").unwrap();
        let (mut session, _) = queued(b"job", None);
        if fault == "manual" {
            session.jobs.clear();
        }
        if fault == "owner" {
            session.owner = Some(pipestream_core::authorization::SessionOwner {
                binding: PrincipalBinding::new("issuer", "alice").unwrap(),
                revoked: false,
            });
        }
        let original = store.create(&session).unwrap();
        drop(files);
        match fault {
            "missing" => fs::remove_file(base(&root, 1)).unwrap(),
            "partialreceipt" => {
                OpenOptions::new()
                    .write(true)
                    .open(suffix(&base(&root, 1), ".done"))
                    .unwrap()
                    .set_len(8)
                    .unwrap();
                fs::remove_file(base(&root, 1)).unwrap();
            }
            "corrupt" => fs::write(base(&root, 1), b"bad").unwrap(),
            "orphan" => fs::write(base(&root, 2), b"xxxxxx").unwrap(),
            "lineage" => {
                fs::remove_file(root.join("work/lineage.reserve")).unwrap();
            }
            "index" => {
                rusqlite::Connection::open(store.path())
                    .unwrap()
                    .execute_batch("UPDATE pipestream_jobs SET image=zeroblob(32)")
                    .unwrap();
            }
            _ => (),
        }
        assert!(
            FileEntityStore::reconcile(&root, SpoolLimits::default(), &store).is_err(),
            "{fault}"
        );
        assert!(base(&root, 2).is_file(), "{fault}");
        assert_eq!(fs::read(spool).unwrap(), b"abandoned");
        assert!(!suffix(&base(&root, 2), ".commit").exists());
        assert_eq!(store.load("work").unwrap().unwrap(), original);
    }
}

#[test]
fn wrong_unbound_and_nonexistent_roots_refuse_without_bootstrap() {
    let (dir, store, files) = fixture();
    let root = files.root().to_owned();
    files.put("work", key(1), b"orphan").unwrap();
    drop(files);
    let foreign = SqliteSessionStore::open(dir.path().join("foreign.sqlite3")).unwrap();
    assert!(FileEntityStore::reconcile(&root, SpoolLimits::default(), &foreign).is_err());
    assert!(base(&root, 1).is_file());
    let missing = dir.path().join("absent");
    assert!(FileEntityStore::reconcile(&missing, SpoolLimits::default(), &store).is_err());
    assert!(!missing.exists());
    let unbound = dir.path().join("unbound");
    drop(FileEntityStore::open(&unbound).unwrap());
    assert!(FileEntityStore::reconcile(&unbound, SpoolLimits::default(), &store).is_err());
    assert!(!unbound.join(".session-store").exists());
}

#[tokio::test]
async fn live_handles_retained_readers_and_spool_loans_exclude_reclamation() {
    let (_dir, store, files) = fixture();
    let root = files.root().to_owned();
    files.put("work", key(1), b"orphan").unwrap();
    assert!(FileEntityStore::reconcile(&root, SpoolLimits::default(), &store).is_err());
    let payload = files
        .load_payload(None, "work", key(1), 6, Sha256::digest(b"orphan").into())
        .unwrap();
    let connection = files.spool.connection(None, 1024).unwrap();
    let loan = connection
        .create()
        .await
        .unwrap()
        .append(b"loan")
        .await
        .unwrap()
        .finish()
        .await
        .unwrap();
    drop(connection);
    drop(files);
    assert!(FileEntityStore::reconcile(&root, SpoolLimits::default(), &store).is_err());
    drop(payload);
    assert!(FileEntityStore::reconcile(&root, SpoolLimits::default(), &store).is_err());
    drop(loan);
    let standalone =
        super::super::super::spool::SpoolStore::new(root.join(".spool"), SpoolLimits::default())
            .unwrap();
    assert!(FileEntityStore::reconcile(&root, SpoolLimits::default(), &store).is_err());
    drop(standalone);
    assert_eq!(reconcile(&root, &store).orphan_bodies_removed, 1);
}

#[test]
fn incomplete_copies_reclaim_stage_but_keep_immutable_digest_and_partial_metadata() {
    let (_dir, store, files) = fixture();
    let root = files.root().to_owned();
    assert!(
        files
            .retained
            .install_payload(
                None,
                "work",
                key(1),
                6,
                Sha256::digest(b"abcdef").into(),
                Cursor::new(b"abc")
            )
            .is_err()
    );
    // An interrupted metadata prefix has no full identity or digest yet. It is
    // deliberately preserved and globally charged, not guessed into an orphan.
    fs::write(suffix(&base(&root, 2), ".meta"), b"PSOBJ001").unwrap();
    fs::create_dir(root.join(".spool")).unwrap();
    fs::write(root.join(".spool/pipestream-Abc123"), b"unreceived").unwrap();
    drop(files);
    let report = reconcile(&root, &store);
    assert_eq!(report.staging_files_removed, 1);
    assert_eq!(report.spool_files_removed, 1);
    assert_eq!(report.after.incomplete_metadata, 1);
    assert_eq!(report.after.staging_bytes, 0);
    assert_eq!(
        fs::read(suffix(&base(&root, 2), ".meta")).unwrap(),
        b"PSOBJ001"
    );
    let files = FileEntityStore::open(&root).unwrap();
    assert!(files.put("work", key(1), b"abcxyz").is_err());
    files.put("work", key(1), b"abcdef").unwrap();
    assert_eq!(fs::read(base(&root, 1)).unwrap(), b"abcdef");
}

#[test]
fn interrupted_restoration_is_fully_charged_and_reconciles_without_new_metadata() {
    let (_dir, store, files) = fixture();
    let root = files.root().to_owned();
    files.put("work", key(1), b"abcdef").unwrap();
    let original = files.retained_usage().unwrap();
    drop(files);
    reconcile(&root, &store);
    let files = FileEntityStore::open(&root).unwrap();
    assert!(
        files
            .retained
            .install_payload(
                None,
                "work",
                key(1),
                6,
                Sha256::digest(b"abcdef").into(),
                Cursor::new(b"abc")
            )
            .is_err()
    );
    assert_eq!(files.retained_usage().unwrap().bytes, original.bytes);
    assert_eq!(files.retained_usage().unwrap().staging_bytes, 6);
    assert!(!suffix(&base(&root, 1), ".commit").exists());
    drop(files);
    let reopened = FileEntityStore::open(&root).unwrap();
    assert_eq!(reopened.retained_usage().unwrap().bytes, original.bytes);
    assert_eq!(reopened.retained_usage().unwrap().staging_bytes, 6);
    drop(reopened);
    let report = reconcile(&root, &store);
    assert_eq!(report.staging_files_removed, 1);
    assert_eq!(report.after.bytes, 1120 + 512);
    let files = FileEntityStore::open(&root).unwrap();
    files.put("work", key(1), b"abcdef").unwrap();
    assert_eq!(files.retained_usage().unwrap(), original);
}

#[test]
fn simultaneous_restorations_share_one_identity_and_never_double_charge() {
    let (_dir, store, files) = fixture();
    let root = files.root().to_owned();
    files.put("work", key(1), b"abcdef").unwrap();
    let original = files.retained_usage().unwrap();
    drop(files);
    reconcile(&root, &store);
    let files = Arc::new(FileEntityStore::open(&root).unwrap());
    let barrier = Arc::new(std::sync::Barrier::new(4));
    let attempts: Vec<_> = (0..4)
        .map(|_| {
            let files = files.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                barrier.wait();
                files.put("work", key(1), b"abcdef")
            })
        })
        .collect();
    let mut successes = 0;
    for attempt in attempts {
        match attempt.join().unwrap() {
            Ok(()) => successes += 1,
            Err(error) => super::super::tests::assert_limit(error),
        }
    }
    assert!(successes >= 1);
    assert_eq!(files.retained_usage().unwrap(), original);
    assert_eq!(fs::read(base(&root, 1)).unwrap(), b"abcdef");
}

#[test]
fn rejected_full_length_stage_can_be_reclaimed_without_accepting_its_bad_bytes() {
    let (_dir, store, files) = fixture();
    let root = files.root().to_owned();
    let digest = Sha256::digest(b"abcdef").into();
    assert!(
        files
            .retained
            .install_payload(None, "work", key(1), 6, digest, Cursor::new(b"xxxxxx"))
            .is_err()
    );
    assert!(!base(&root, 1).exists());
    assert!(!suffix(&base(&root, 1), ".done").exists());
    assert_eq!(
        fs::read(suffix(&base(&root, 1), ".stage")).unwrap(),
        b"xxxxxx"
    );
    let metadata = fs::read(suffix(&base(&root, 1), ".meta")).unwrap();
    drop(files);
    let report = reconcile(&root, &store);
    assert_eq!(report.staging_files_removed, 1);
    assert_eq!(report.orphan_bodies_removed, 0);
    assert_eq!(
        fs::read(suffix(&base(&root, 1), ".commit")).unwrap(),
        metadata
    );
    let files = FileEntityStore::open(&root).unwrap();
    assert!(files.put("work", key(1), b"xxxxxx").is_err());
    files.put("work", key(1), b"abcdef").unwrap();
    assert!(store.list_session_ids().unwrap().is_empty());
}

#[test]
fn storage_faults_at_each_phase_are_replayable_and_never_mutate_sessions() {
    for phase in [
        Phase::Audited,
        Phase::StagingRemoved,
        Phase::CommitmentPublished,
        Phase::BodyRemoved,
    ] {
        let (_dir, store, files) = fixture();
        let root = files.root().to_owned();
        files.put("work", key(1), b"job").unwrap();
        files.put("work", key(2), b"orphan").unwrap();
        let original = store.create(&queued(b"job", None).0).unwrap();
        drop(files);
        assert!(
            reconcile_with(root.clone(), SpoolLimits::default(), &store, |at| {
                if at == phase {
                    Err(io::Error::other("injected storage failure"))
                } else {
                    Ok(())
                }
            })
            .is_err()
        );
        assert_eq!(store.load("work").unwrap().unwrap(), original);
        let reopened = FileEntityStore::open(&root).unwrap();
        let before = reopened.retained_usage().unwrap();
        assert!(
            reopened
                .load_payload(None, "work", key(1), 3, Sha256::digest(b"job").into())
                .is_ok()
        );
        drop(reopened);
        let report = reconcile(&root, &store);
        assert_eq!(report.before, before);
        assert_eq!(report.commitments_retained, 1);
        assert!(!base(&root, 2).exists());
        assert!(base(&root, 1).is_file());
        assert_eq!(store.load("work").unwrap().unwrap(), original);
    }
}

#[test]
fn exclusive_maintenance_keeps_writer_and_openers_out_until_files_finish() {
    let (_dir, store, files) = fixture();
    let root = files.root().to_owned();
    files.put("work", key(1), b"orphan").unwrap();
    drop(files);
    let writer = rusqlite::Connection::open(store.path()).unwrap();
    writer.busy_timeout(Duration::ZERO).unwrap();
    reconcile_with(root.clone(), SpoolLimits::default(), &store, |_| {
        assert!(writer.execute_batch("BEGIN IMMEDIATE").is_err());
        assert!(FileEntityStore::open(&root).is_err());
        assert!(
            super::super::super::spool::SpoolStore::new(
                root.join(".spool"),
                SpoolLimits::default()
            )
            .is_err()
        );
        Ok(())
    })
    .unwrap();
    writer.execute_batch("BEGIN IMMEDIATE; ROLLBACK").unwrap();
    assert!(FileEntityStore::open(&root).is_ok());
}

#[test]
fn unknown_spools_aliases_and_duplicate_or_corrupt_commitments_refuse_without_deletion() {
    for fault in [
        "unknown",
        "symlink",
        "hardlink",
        "duplicate",
        "corrupt",
        "oversized",
    ] {
        let (dir, store, files) = fixture();
        let root = files.root().to_owned();
        files.put("work", key(1), b"orphan").unwrap();
        drop(files);
        reconcile(&root, &store);
        let committed = suffix(&base(&root, 1), ".commit");
        fs::create_dir(root.join(".spool")).unwrap();
        let spool = root.join(".spool/pipestream-Abc123");
        fs::write(&spool, b"keep").unwrap();
        match fault {
            "unknown" => fs::write(root.join(".spool/user-document"), b"keep").unwrap(),
            "symlink" => {
                std::os::unix::fs::symlink(&spool, root.join(".spool/pipestream-Def456")).unwrap()
            }
            "hardlink" => fs::hard_link(&spool, dir.path().join("outside-spool")).unwrap(),
            "duplicate" => {
                fs::copy(&committed, suffix(&base(&root, 1), ".meta")).unwrap();
            }
            "corrupt" => fs::write(&committed, [0; 512]).unwrap(),
            "oversized" => {
                OpenOptions::new()
                    .write(true)
                    .open(&committed)
                    .unwrap()
                    .set_len(513)
                    .unwrap();
            }
            _ => unreachable!(),
        }
        assert!(
            FileEntityStore::reconcile(&root, SpoolLimits::default(), &store).is_err(),
            "{fault}"
        );
        assert_eq!(fs::read(&spool).unwrap(), b"keep");
    }
}

#[test]
fn old_payload_policy_is_refused_without_conversion() {
    let (_dir, store, files) = fixture();
    let root = files.root().to_owned();
    files.put("work", key(1), b"orphan").unwrap();
    drop(files);
    let policy = root.join(".retained-policy");
    let mut bytes = fs::read(&policy).unwrap();
    bytes[..8].copy_from_slice(b"PSRET003");
    let checksum = Sha256::digest(&bytes[..64]);
    bytes[64..].copy_from_slice(&checksum);
    fs::write(&policy, &bytes).unwrap();
    assert!(FileEntityStore::reconcile(&root, SpoolLimits::default(), &store).is_err());
    assert!(FileEntityStore::open(&root).is_err());
    assert_eq!(fs::read(policy).unwrap(), bytes);
    assert_eq!(fs::read(base(&root, 1)).unwrap(), b"orphan");
}

#[test]
fn process_exit_at_reclamation_phases_retains_commitments_and_replays() {
    const CHILD: &str = "PIPESTREAM_RECONCILE_CHILD";
    if let Some(path) = std::env::var_os(CHILD) {
        let dir = PathBuf::from(path);
        let phase: usize = std::env::var("PIPESTREAM_RECONCILE_PHASE")
            .unwrap()
            .parse()
            .unwrap();
        let store = SqliteSessionStore::open(dir.join("sessions.sqlite3")).unwrap();
        reconcile_with(dir.join("payloads"), SpoolLimits::default(), &store, |at| {
            if at as usize == phase {
                std::process::exit(73);
            }
            Ok(())
        })
        .unwrap();
        panic!("crash probe was not reached");
    }
    for phase in [
        Phase::Audited,
        Phase::StagingRemoved,
        Phase::CommitmentPublished,
        Phase::BodyRemoved,
    ] {
        let (dir, store, files) = fixture();
        let root = files.root().to_owned();
        files.put("work", key(1), b"orphan").unwrap();
        let metadata = fs::read(suffix(&base(&root, 1), ".meta")).unwrap();
        drop(files);
        let log = File::create(dir.path().join("child.log")).unwrap();
        let mut child = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "recursive::retained::reconcile::tests::process_exit_at_reclamation_phases_retains_commitments_and_replays", "--nocapture"])
            .env(CHILD, dir.path()).env("PIPESTREAM_RECONCILE_PHASE", (phase as usize).to_string())
            .stdout(log.try_clone().unwrap()).stderr(log).spawn().unwrap();
        let deadline = Instant::now() + Duration::from_secs(15);
        let status = loop {
            if let Some(status) = child.try_wait().unwrap() {
                break status;
            }
            if Instant::now() >= deadline {
                child.kill().unwrap();
                child.wait().unwrap();
                panic!("reconciliation child timed out");
            }
            std::thread::sleep(Duration::from_millis(10));
        };
        assert_eq!(
            status.code(),
            Some(73),
            "{}",
            fs::read_to_string(dir.path().join("child.log")).unwrap()
        );
        reconcile(&root, &store);
        assert_eq!(
            fs::read(suffix(&base(&root, 1), ".commit")).unwrap(),
            metadata
        );
        assert!(!base(&root, 1).exists());
        let files = FileEntityStore::open(&root).unwrap();
        assert!(files.put("work", key(1), b"change").is_err());
        files.put("work", key(1), b"orphan").unwrap();
        assert!(store.list_session_ids().unwrap().is_empty());
    }
}

#[test]
fn standalone_spool_handle_in_another_process_holds_the_retained_root_lock() {
    const CHILD: &str = "PIPESTREAM_RECONCILE_SPOOL_OWNER";
    if let Some(path) = std::env::var_os(CHILD) {
        let dir = PathBuf::from(path);
        let _spool = super::super::super::spool::SpoolStore::new(
            dir.join("payloads/.spool"),
            SpoolLimits::default(),
        )
        .unwrap();
        fs::write(dir.join("ready"), b"owned").unwrap();
        let deadline = Instant::now() + Duration::from_secs(15);
        while !dir.join("release").exists() {
            assert!(
                Instant::now() < deadline,
                "parent did not release spool child"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
        return;
    }
    let (dir, store, files) = fixture();
    let root = files.root().to_owned();
    files.put("work", key(1), b"orphan").unwrap();
    drop(files);
    let log = File::create(dir.path().join("child.log")).unwrap();
    let mut child = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "recursive::retained::reconcile::tests::standalone_spool_handle_in_another_process_holds_the_retained_root_lock", "--nocapture"])
        .env(CHILD, dir.path()).stdout(log.try_clone().unwrap()).stderr(log).spawn().unwrap();
    let deadline = Instant::now() + Duration::from_secs(15);
    while !dir.path().join("ready").exists() {
        if let Some(status) = child.try_wait().unwrap() {
            panic!(
                "spool child exited {status}: {}",
                fs::read_to_string(dir.path().join("child.log")).unwrap()
            );
        }
        if Instant::now() >= deadline {
            child.kill().unwrap();
            child.wait().unwrap();
            panic!("spool child startup timed out");
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    let result = FileEntityStore::reconcile(&root, SpoolLimits::default(), &store);
    fs::write(dir.path().join("release"), b"done").unwrap();
    let status = loop {
        if let Some(status) = child.try_wait().unwrap() {
            break status;
        }
        if Instant::now() >= deadline {
            child.kill().unwrap();
            child.wait().unwrap();
            panic!("spool child teardown timed out");
        }
        std::thread::sleep(Duration::from_millis(10));
    };
    assert!(
        status.success(),
        "{}",
        fs::read_to_string(dir.path().join("child.log")).unwrap()
    );
    assert!(
        matches!(result, Err(StoreError::Io(error)) if error.kind() == io::ErrorKind::WouldBlock)
    );
    assert_eq!(fs::read(base(&root, 1)).unwrap(), b"orphan");
    assert_eq!(reconcile(&root, &store).orphan_bodies_removed, 1);
}
