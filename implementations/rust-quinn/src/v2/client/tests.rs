use super::*;
use std::thread;
mod observations;

fn creation() -> Creation {
    Creation {
        authority: IdentityLabel("issuer-a".into()),
        owner: IdentityLabel("alice".into()),
        creation_sequence: Id(1),
        policy: Policy {
            execution_limit_ms: Duration(60000),
            output_retention_ms: Duration(120000),
            receipt_retention_ms: Duration(180000),
        },
        results: true,
    }
}
fn selection() -> Capabilities {
    Capabilities {
        response: ResponseFlag(1),
        supported: vec![
            ProfileId(DURABLE_WORK.into()),
            ProfileId(RESULT_DELIVERY.into()),
        ],
        required: vec![
            ProfileId(DURABLE_WORK.into()),
            ProfileId(RESULT_DELIVERY.into()),
        ],
        control_limit: ControlLimit(65536),
        stream_limit: ConcurrencyLimit(4),
        pending_limit: ConcurrencyLimit(16),
        object_limit: Number(1 << 20),
        stream_idle_ms: IdleMs(5000),
        stream_lifetime_ms: LifetimeMs(30000),
    }
}
fn binding() -> Control {
    Control::Session(Session::Binding {
        request: Id(99),
        authority: creation().authority,
        owner: creation().owner,
        generation: Id(7),
        creation_sequence: Id(1),
        policy: creation().policy,
        limits: Limits {
            scopes: Id(100),
            entities: Id(10000),
            operations: Id(10000),
            retained_input_bytes: Number(1 << 20),
            retained_output_bytes: Number(1 << 20),
            active_jobs: Id(100),
        },
    })
}
fn work() -> WorkKey {
    WorkKey {
        scope: Number(0),
        producer: Producer(0),
        entity: Id(1),
    }
}
fn declare() -> Intent {
    Intent {
        operation: OperationId([1; 16]),
        mutation: Mutation::Declare {
            scope: Number(0),
            entity_ids: vec![Id(1)],
            seal: true,
        },
    }
}
fn receipt(journal: &Journal, intent: &Intent, body: Outcome) -> OperationReceipt {
    OperationReceipt {
        operation: intent.operation,
        request_digest: intent
            .mutation
            .digest(&journal.identity().unwrap(), Producer(0), intent.operation)
            .unwrap(),
        body,
    }
}
fn declared(journal: &Journal, intent: &Intent) -> OperationReceipt {
    receipt(
        journal,
        intent,
        Outcome::Declared {
            scope: Number(0),
            producer: Producer(0),
            accepted_count: BatchCount(1),
            declared: Number(1),
            seal: Some(Digest([2; 32])),
        },
    )
}
fn new(path: &Path, operations: u64) -> Journal {
    Journal::initialize(
        path,
        creation(),
        JournalLimits {
            operations: Id(operations),
            ..Default::default()
        },
        PhysicalLimits::default(),
    )
    .unwrap()
}
fn bound(path: &Path, operations: u64) -> Journal {
    let journal = new(path, operations);
    journal.record_binding(&binding(), &selection()).unwrap();
    journal
}
fn code(result: Result<()>, expected: ErrorCode) {
    assert!(
        matches!(&result, Err(JournalError::Protocol(e)) if e.code == expected),
        "{result:?}"
    );
}

#[test]
fn creation_and_uncertain_intent_survive_exclusive_reopen_with_fresh_request_numbers() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("journal.sqlite");
    let journal = new(&path, 2);
    assert_eq!(
        journal.creation().request(Id(1)).unwrap(),
        creation().request(Id(1)).unwrap()
    );
    code(journal.prepare(&declare()), ErrorCode::NotReady);
    assert!(journal.binding().unwrap().is_none());
    drop(journal);
    let journal = Journal::open(
        &path,
        creation(),
        JournalLimits {
            operations: Id(2),
            ..Default::default()
        },
        PhysicalLimits::default(),
    )
    .unwrap();
    journal.record_binding(&binding(), &selection()).unwrap();
    journal.prepare(&declare()).unwrap();
    journal.prepare(&declare()).unwrap();
    assert!(journal.receipt(declare().operation).unwrap().is_none());
    drop(journal);
    let journal = Journal::open(
        &path,
        creation(),
        JournalLimits {
            operations: Id(2),
            ..Default::default()
        },
        PhysicalLimits::default(),
    )
    .unwrap();
    let intent = journal.intent(declare().operation).unwrap();
    assert_eq!(intent, declare());
    assert_eq!(request_id(&intent.control(Id(41)).unwrap()), Some(Id(41)));
    assert_eq!(
        journal.unresolved(Number(0), PageLimit(1)).unwrap(),
        vec![(Id(1), intent.clone())]
    );
    let received = declared(&journal, &intent);
    journal.record_receipt(&received).unwrap();
    journal.record_receipt(&received).unwrap();
    assert_eq!(journal.receipt(intent.operation).unwrap(), Some(received));
    assert!(
        journal
            .unresolved(Number(0), PageLimit(1))
            .unwrap()
            .is_empty()
    );
    journal.prepare(&intent).unwrap(); // a replay never becomes a fresh mutation
}

#[test]
fn immutable_binding_and_required_profiles_are_checked_before_receipt_storage() {
    let directory = tempfile::tempdir().unwrap();
    let journal = new(&directory.path().join("journal.sqlite"), 8);
    let mut changed = binding();
    let Control::Session(Session::Binding { owner, .. }) = &mut changed else {
        unreachable!()
    };
    *owner = IdentityLabel("mallory".into());
    code(
        journal.record_binding(&changed, &selection()),
        ErrorCode::IntegrityError,
    );
    let mut profiles = selection();
    profiles.supported.pop();
    profiles.required.pop();
    code(
        journal.record_binding(&binding(), &profiles),
        ErrorCode::ExtensionUnsupported,
    );
    assert!(journal.binding().unwrap().is_none());
    journal.record_binding(&binding(), &selection()).unwrap();
    let mut changed = binding();
    let Control::Session(Session::Binding { generation, .. }) = &mut changed else {
        unreachable!()
    };
    *generation = Id(8);
    code(
        journal.record_binding(&changed, &selection()),
        ErrorCode::IntegrityError,
    );
    assert_eq!(journal.identity().unwrap().generation, Id(7));
}

#[test]
fn every_mutation_round_trips_and_checks_its_typed_receipt() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("journal.sqlite");
    let journal = bound(&path, 16);
    let parameters = AdmitParameters {
        work: work(),
        input: Input {
            length: Number(3),
            sha256: Digest([8; 32]),
            content_type: ApplicationLabel("text/plain".into()),
        },
        application: ApplicationLabel("copy-v1".into()),
        mode: Mode(2),
        execution_ms: Duration(1000),
        outputs: OutputBudget {
            count: BatchCount(1),
            total_bytes: Number(3),
        },
    };
    let cases = [
        (
            Mutation::Admit(parameters),
            Outcome::Admitted {
                work: work(),
                attempt: Id(1),
                admitted_at: Number(100),
                deadline: Number(1100),
                child: Some(ChildScope {
                    scope: Id(1),
                    producer: Producer(1),
                }),
            },
        ),
        (declare().mutation, declared(&journal, &declare()).body),
        (
            Mutation::Retry {
                work: work(),
                expected_attempt: Id(2),
            },
            Outcome::Retried {
                work: work(),
                expected_attempt: Id(2),
                replacement_attempt: Id(3),
                accepted_at: Number(123),
            },
        ),
        (
            Mutation::Cancel { work: work() },
            Outcome::Cancelled {
                work: work(),
                accepted_at: Number(123),
                disposition: Disposition(0),
                state_at_commit: State::CANCELLING,
            },
        ),
        (
            Mutation::Skip { work: work() },
            Outcome::Skipped {
                work: work(),
                accepted_at: Number(123),
                // Cancellation won. Skip can only report that existing terminal state.
                disposition: Disposition(1),
                state_at_commit: State::CANCELLED,
            },
        ),
        (
            Mutation::ScopeCancel { scope: Number(0) },
            Outcome::ScopeCancelled {
                scope: Number(0),
                accepted_at: Number(123),
            },
        ),
    ];
    for (index, (mutation, outcome)) in cases.into_iter().enumerate() {
        let intent = Intent {
            operation: OperationId([index as u8 + 1; 16]),
            mutation,
        };
        journal.prepare(&intent).unwrap();
        assert_eq!(journal.intent(intent.operation).unwrap(), intent);
        let received = receipt(&journal, &intent, outcome);
        let mut wrong = received.clone();
        wrong.request_digest.0[0] ^= 1;
        code(journal.record_receipt(&wrong), ErrorCode::IntegrityError);
        let mut wrong = received.clone();
        wrong.body = Outcome::ScopeCancelled {
            scope: Number(999),
            accepted_at: Number(123),
        };
        code(journal.record_receipt(&wrong), ErrorCode::IntegrityError);
        journal.record_receipt(&received).unwrap();
        assert_eq!(journal.receipt(intent.operation).unwrap(), Some(received));
    }
    drop(journal);
    let journal = Journal::open(
        &path,
        creation(),
        JournalLimits {
            operations: Id(16),
            ..Default::default()
        },
        PhysicalLimits::default(),
    )
    .unwrap();
    assert!(
        journal
            .unresolved(Number(0), PageLimit(256))
            .unwrap()
            .is_empty()
    );
}

#[test]
fn changed_intent_or_receipt_and_quota_exhaustion_never_evict_the_original() {
    let directory = tempfile::tempdir().unwrap();
    let journal = bound(&directory.path().join("journal.sqlite"), 1);
    journal.prepare(&declare()).unwrap();
    let mut changed = declare();
    changed.mutation = Mutation::ScopeCancel { scope: Number(0) };
    code(journal.prepare(&changed), ErrorCode::Conflict);
    changed.operation = OperationId([9; 16]);
    code(journal.prepare(&changed), ErrorCode::LimitExceeded);
    let received = declared(&journal, &declare());
    journal.record_receipt(&received).unwrap();
    let mut changed = received.clone();
    let Outcome::Declared { seal, .. } = &mut changed.body else {
        unreachable!()
    };
    *seal = Some(Digest([3; 32]));
    code(journal.record_receipt(&changed), ErrorCode::IntegrityError);
    assert_eq!(journal.intent(declare().operation).unwrap(), declare());
    assert_eq!(
        journal.receipt(declare().operation).unwrap(),
        Some(received)
    );
}

#[test]
fn concurrent_preparations_serialize_exact_identity_and_enforce_one_shared_quota() {
    let directory = tempfile::tempdir().unwrap();
    let journal = bound(&directory.path().join("journal.sqlite"), 1);
    let barrier = Arc::new(std::sync::Barrier::new(5));
    let mut tasks = Vec::new();
    for _ in 0..4 {
        let journal = journal.clone();
        let barrier = barrier.clone();
        tasks.push(thread::spawn(move || {
            barrier.wait();
            journal.prepare(&declare())
        }));
    }
    barrier.wait();
    for task in tasks {
        task.join().unwrap().unwrap();
    }
    assert_eq!(
        journal.unresolved(Number(0), PageLimit(256)).unwrap().len(),
        1
    );
    let mut other = declare();
    other.operation = OperationId([2; 16]);
    code(journal.prepare(&other), ErrorCode::LimitExceeded);
}

#[test]
fn reopen_refuses_missing_empty_changed_configuration_and_corrupt_records() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("journal.sqlite");
    assert!(
        Journal::open(
            &path,
            creation(),
            JournalLimits::default(),
            PhysicalLimits::default()
        )
        .is_err()
    );
    assert!(!path.exists());
    let journal = bound(&path, 2);
    journal.prepare(&declare()).unwrap();
    drop(journal);
    assert!(
        Journal::initialize(
            &path,
            creation(),
            JournalLimits {
                operations: Id(2),
                ..Default::default()
            },
            PhysicalLimits::default()
        )
        .is_err()
    );
    let mut changed = creation();
    changed.owner = IdentityLabel("mallory".into());
    assert!(
        Journal::open(
            &path,
            changed,
            JournalLimits {
                operations: Id(2),
                ..Default::default()
            },
            PhysicalLimits::default()
        )
        .is_err()
    );
    assert!(
        Journal::open(
            &path,
            creation(),
            JournalLimits {
                operations: Id(3),
                ..Default::default()
            },
            PhysicalLimits::default()
        )
        .is_err()
    );
    let journal = Journal::open(
        &path,
        creation(),
        JournalLimits {
            operations: Id(2),
            ..Default::default()
        },
        PhysicalLimits::default(),
    )
    .unwrap();
    let connection = journal.connect().unwrap();
    connection
        .execute("UPDATE operations SET intent=zeroblob(1048576)", [])
        .unwrap();
    drop(connection);
    drop(journal);
    assert!(matches!(
        Journal::open(
            &path,
            creation(),
            JournalLimits {
                operations: Id(2),
                ..Default::default()
            },
            PhysicalLimits::default()
        ),
        Err(JournalError::Corrupt(_))
    ));
    let empty = directory.path().join("empty.sqlite");
    std::fs::File::create_new(&empty).unwrap();
    assert!(
        Journal::open(
            &empty,
            creation(),
            JournalLimits::default(),
            PhysicalLimits::default()
        )
        .is_err()
    );
}

#[test]
fn journal_record_checksum_binds_operation_namespace_and_session_generation() {
    let identity = SessionIdentity {
        authority: creation().authority,
        owner: creation().owner,
        generation: Id(7),
    };
    let image = storage::intent_image(&declare(), &identity).unwrap();
    assert_eq!(
        storage::decode_intent(&image, declare().operation, &identity).unwrap(),
        declare()
    );
    let mut other = identity.clone();
    other.generation = Id(8);
    assert!(storage::decode_intent(&image, declare().operation, &other).is_err());
    assert!(storage::decode_intent(&image, OperationId([2; 16]), &identity).is_err());
    let mut damaged = image;
    damaged[4] ^= 1;
    assert!(storage::decode_intent(&damaged, declare().operation, &identity).is_err());
}

#[test]
fn incompatible_reopen_does_not_change_the_existing_sqlite_journal_mode() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("journal.sqlite");
    let journal = bound(&path, 4);
    let connection = journal.connect().unwrap();
    connection
        .execute_batch("PRAGMA journal_mode=DELETE; PRAGMA user_version=99;")
        .unwrap();
    drop(connection);
    drop(journal);
    assert!(
        Journal::open(
            &path,
            creation(),
            JournalLimits {
                operations: Id(4),
                ..Default::default()
            },
            PhysicalLimits::default()
        )
        .is_err()
    );
    let connection = Connection::open_with_flags(&path, OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
    let mode: String = connection
        .query_row("PRAGMA journal_mode", [], |row| row.get(0))
        .unwrap();
    assert_eq!(
        mode, "delete",
        "refusing another format must not reconfigure its storage"
    );
}

#[test]
fn recovery_pages_are_bounded_and_do_not_renumber_unresolved_operations() {
    let directory = tempfile::tempdir().unwrap();
    let journal = bound(&directory.path().join("journal.sqlite"), 4);
    for index in 1..=4 {
        let mut intent = declare();
        intent.operation = OperationId([index; 16]);
        journal.prepare(&intent).unwrap();
    }
    journal
        .record_receipt(&declared(&journal, &declare()))
        .unwrap();
    let first = journal.unresolved(Number(0), PageLimit(2)).unwrap();
    assert_eq!(
        first.iter().map(|(id, _)| id.0).collect::<Vec<_>>(),
        vec![2, 3]
    );
    let last = journal.unresolved(Number(3), PageLimit(2)).unwrap();
    assert_eq!(last[0].0, Id(4));
    assert_eq!(last.len(), 1);
    assert!(
        journal
            .unresolved(Number(4), PageLimit(2))
            .unwrap()
            .is_empty()
    );
    assert!(journal.unresolved(Number(0), PageLimit(257)).is_err());
}

#[test]
fn client_journal_kill_child() {
    let Some(path) = std::env::var_os("PIPESTREAM_V2_JOURNAL_KILL_PATH") else {
        return;
    };
    let journal = bound(Path::new(&path), 4);
    journal.prepare(&declare()).unwrap();
    if std::env::var_os("PIPESTREAM_V2_JOURNAL_KILL_OBSERVATIONS").is_some() {
        observations::persist_for_crash(&journal);
    }
    if std::env::var_os("PIPESTREAM_V2_JOURNAL_KILL_SCOPES").is_some() {
        observations::persist_scope_for_crash(&journal);
    }
    use std::io::Write;
    println!("journal-committed");
    std::io::stdout().flush().unwrap();
    loop {
        thread::park();
    }
}

#[test]
fn failed_local_receipt_commit_preserves_uncertainty_and_exact_replay() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("journal.sqlite");
    let journal = bound(&path, 4);
    journal.prepare(&declare()).unwrap();
    let received = declared(&journal, &declare());
    let connection = journal.connect().unwrap();
    connection.execute_batch("CREATE TRIGGER refuse_receipt BEFORE UPDATE OF receipt ON operations BEGIN SELECT RAISE(ABORT,'test storage fault'); END;").unwrap();
    assert!(matches!(
        journal.record_receipt(&received),
        Err(JournalError::Database(_))
    ));
    assert!(journal.receipt(declare().operation).unwrap().is_none());
    assert_eq!(
        journal.unresolved(Number(0), PageLimit(1)).unwrap(),
        vec![(Id(1), declare())]
    );
    connection
        .execute_batch("DROP TRIGGER refuse_receipt;")
        .unwrap();
    drop(connection);
    drop(journal);
    let journal = Journal::open(
        &path,
        creation(),
        JournalLimits {
            operations: Id(4),
            ..Default::default()
        },
        PhysicalLimits::default(),
    )
    .unwrap();
    journal.prepare(&declare()).unwrap();
    journal.record_receipt(&received).unwrap();
    assert_eq!(
        journal.receipt(declare().operation).unwrap(),
        Some(received)
    );
}

#[test]
fn physical_exhaustion_refuses_new_intent_without_losing_old_unknown_work() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("journal.sqlite");
    let limits = PhysicalLimits {
        database_bytes: 131072,
        wal_bytes: 131072,
        journal_bytes: 131072,
        shared_memory_bytes: 65536,
    };
    let journal = Journal::initialize(&path, creation(), JournalLimits::default(), limits).unwrap();
    let initial = journal.physical_usage().unwrap();
    eprintln!(
        "client format-3 initialized: database={} WAL={} bytes",
        initial.database_bytes, initial.wal_bytes
    );
    journal.record_binding(&binding(), &selection()).unwrap();
    journal.prepare(&declare()).unwrap();
    let mut refused = false;
    for index in 2..=128 {
        let intent = Intent {
            operation: OperationId([index; 16]),
            mutation: Mutation::Declare {
                scope: Number(0),
                entity_ids: (1..=256).map(|n| Id(MAX_NUMBER - 256 + n)).collect(),
                seal: true,
            },
        };
        match journal.prepare(&intent) {
            Ok(()) => {}
            Err(JournalError::Protocol(e)) if e.code == ErrorCode::LimitExceeded => {
                assert!(
                    matches!(journal.intent(intent.operation), Err(JournalError::Protocol(e)) if e.code == ErrorCode::NotFound)
                );
                refused = true;
                break;
            }
            result => panic!("unexpected result {result:?}"),
        }
    }
    assert!(refused, "128 KiB database must not accept this inventory");
    assert_eq!(journal.intent(declare().operation).unwrap(), declare());
    assert!(journal.receipt(declare().operation).unwrap().is_none());
    let usage = journal.physical_usage().unwrap();
    assert!(usage.database_bytes <= limits.database_bytes && usage.wal_bytes <= limits.wal_bytes);
    drop(journal);
    let journal = Journal::open(&path, creation(), JournalLimits::default(), limits).unwrap();
    assert_eq!(journal.intent(declare().operation).unwrap(), declare());
}

#[test]
fn exhausted_cursor_refuses_instead_of_sqlite_random_rowid_fallback() {
    let directory = tempfile::tempdir().unwrap();
    let journal = bound(&directory.path().join("journal.sqlite"), 4);
    journal.prepare(&declare()).unwrap();
    let connection = journal.connect().unwrap();
    connection
        .execute("UPDATE operations SET row_id=?1", [i64::MAX])
        .unwrap();
    let mut other = declare();
    other.operation = OperationId([2; 16]);
    code(journal.prepare(&other), ErrorCode::LimitExceeded);
    assert_eq!(
        journal.unresolved(Number(0), PageLimit(1)).unwrap()[0].0,
        Id(MAX_NUMBER)
    );
}

#[test]
fn forced_process_exit_after_commit_preserves_the_original_unknown_operation() {
    let (_directory, journal) = kill_and_reopen(0);
    assert_eq!(journal.intent(declare().operation).unwrap(), declare());
    assert!(journal.receipt(declare().operation).unwrap().is_none());
    journal.prepare(&declare()).unwrap();
    assert_eq!(
        journal.unresolved(Number(0), PageLimit(256)).unwrap().len(),
        1
    );
}

#[test]
fn forced_process_exit_preserves_terminal_observation_manifest_and_selected_index() {
    let (_directory, journal) = kill_and_reopen(1);
    observations::verify_crash_recovery(&journal);
}

#[test]
fn forced_process_exit_preserves_verified_membership_and_root_coverage() {
    let (_directory, journal) = kill_and_reopen(2);
    observations::verify_scope_crash_recovery(&journal);
}

fn kill_and_reopen(mode: u8) -> (tempfile::TempDir, Journal) {
    use std::io::{BufRead, BufReader};
    struct Child(std::process::Child);
    impl Drop for Child {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("journal.sqlite");
    let mut command = std::process::Command::new(std::env::current_exe().unwrap());
    command
        .args([
            "--exact",
            "v2::client::tests::client_journal_kill_child",
            "--nocapture",
        ])
        .env("PIPESTREAM_V2_JOURNAL_KILL_PATH", &path)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::inherit());
    command.env_remove("PIPESTREAM_V2_JOURNAL_KILL_OBSERVATIONS");
    command.env_remove("PIPESTREAM_V2_JOURNAL_KILL_SCOPES");
    if mode == 1 {
        command.env("PIPESTREAM_V2_JOURNAL_KILL_OBSERVATIONS", "1");
    } else if mode == 2 {
        command.env("PIPESTREAM_V2_JOURNAL_KILL_SCOPES", "1");
    }
    let mut child = Child(command.spawn().unwrap());
    let stdout = child.0.stdout.take().unwrap();
    let (sent, receive) = std::sync::mpsc::channel();
    let reader = thread::spawn(move || {
        let ready = BufReader::new(stdout)
            .lines()
            .any(|line| line.is_ok_and(|s| s == "journal-committed"));
        let _ = sent.send(ready);
    });
    assert!(receive.recv_timeout(Elapsed::from_secs(10)).unwrap());
    child.0.kill().unwrap();
    let exit = child.0.wait().unwrap();
    assert!(!exit.success());
    reader.join().unwrap();
    let journal = Journal::open(
        &path,
        creation(),
        JournalLimits {
            operations: Id(4),
            ..Default::default()
        },
        PhysicalLimits::default(),
    )
    .unwrap();
    (directory, journal)
}
