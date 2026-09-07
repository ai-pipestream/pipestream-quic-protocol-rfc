use super::*;

#[test]
fn impossible_admission_receipts_cannot_be_saved_without_a_view() {
    for field in 0..3 {
        let directory = tempfile::tempdir().unwrap();
        let journal = bound(&directory.path().join("client.sqlite"), 16);
        let first = admit(&journal);
        let mut intent = journal.intent(first.operation).unwrap();
        intent.operation = OperationId([10; 16]);
        let Mutation::Admit(p) = &mut intent.mutation else {
            unreachable!()
        };
        match field {
            0 => p.work.producer = Producer(1),
            1 => p.execution_ms = Duration(60001),
            _ => journal.record_receipt(&first).unwrap(),
        }
        let body = Outcome::Admitted {
            work: p.work.clone(),
            attempt: Id(1),
            admitted_at: Number(100),
            deadline: Number(100 + p.execution_ms.0),
            child: None,
        };
        if field == 0 {
            // The immutable-intent codec already refuses foreign input producers,
            // before there can be an accepted receipt to validate.
            code(journal.prepare(&intent), ErrorCode::FrameError);
            continue;
        }
        journal.prepare(&intent).unwrap();
        code(
            journal.record_receipt(&receipt(&journal, &intent, body)),
            ErrorCode::IntegrityError,
        );
        assert!(journal.receipt(intent.operation).unwrap().is_none());
    }
}

#[test]
fn oversized_manifest_hits_physical_cap_without_partial_evidence_or_eviction() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("client.sqlite");
    let physical = PhysicalLimits {
        database_bytes: 131072,
        wal_bytes: 131072,
        journal_bytes: 131072,
        shared_memory_bytes: 65536,
    };
    let journal =
        Journal::initialize(&path, creation(), JournalLimits::default(), physical).unwrap();
    journal.record_binding(&binding(), &selection()).unwrap();
    journal.observe_work(Id(3), &success()).unwrap();
    let original = journal
        .remember_reference(&manifest(), OutputIndex(0))
        .unwrap();
    let mut large = manifest();
    large.work.entity = Id(MAX_NUMBER);
    let host = [
        "h".repeat(63),
        "h".repeat(63),
        "h".repeat(63),
        "h".repeat(61),
    ]
    .join(".");
    large.outputs = (0..256).map(|n| {
        let mut output = large.outputs[0].clone();
        output.index = OutputIndex(n);
        output.content_type = ApplicationLabel("x".repeat(128));
        output.locator.0 = format!("pipestream://{host}:7443/v2/sessions/7/scopes/0/producers/0/entities/{MAX_NUMBER}/attempts/1/outputs/{n}");
        output
    }).collect();
    assert!(large.encode().unwrap().len() > 131072);
    refusal(
        journal.remember_reference(&large, OutputIndex(255)),
        ErrorCode::LimitExceeded,
    );
    let connection = journal.connect().unwrap();
    assert_eq!(
        connection
            .query_row("SELECT count(*) FROM manifests", [], |r| r.get::<_, i64>(0))
            .unwrap(),
        1
    );
    drop(connection);
    let usage = journal.physical_usage().unwrap();
    assert!(
        usage.database_bytes <= physical.database_bytes && usage.wal_bytes <= physical.wal_bytes
    );
    drop(journal);
    let journal = Journal::open(&path, creation(), JournalLimits::default(), physical).unwrap();
    assert_eq!(
        journal
            .retained_reference(&work(), Id(1), OutputIndex(0))
            .unwrap(),
        original
    );
    assert_eq!(
        journal.observed_work(&work()).unwrap().unwrap().view,
        success()
    );
}

#[test]
fn independent_observation_and_selection_quotas_never_evict_evidence() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("client.sqlite");
    let limits = JournalLimits {
        operations: Id(16),
        observations: Id(1),
    };
    let journal =
        Journal::initialize(&path, creation(), limits.clone(), PhysicalLimits::default()).unwrap();
    journal.record_binding(&binding(), &selection()).unwrap();
    journal.observe_work(Id(2), &view()).unwrap();
    let mut other = declared_view();
    other.work.entity = Id(2);
    refusal(
        journal.observe_work(Id(1), &other),
        ErrorCode::LimitExceeded,
    );
    let mut completed = success();
    let m = completed.manifest.as_mut().unwrap();
    let mut second = m.outputs[0].clone();
    second.index = OutputIndex(1);
    second.locator.0 = second.locator.0.replace("outputs/0", "outputs/1");
    m.outputs.push(second);
    journal.observe_work(Id(3), &completed).unwrap();
    let reference = journal
        .remember_reference(completed.manifest.as_ref().unwrap(), OutputIndex(1))
        .unwrap();
    refusal(
        journal.remember_reference(completed.manifest.as_ref().unwrap(), OutputIndex(0)),
        ErrorCode::LimitExceeded,
    );
    assert_eq!(
        journal
            .remember_reference(completed.manifest.as_ref().unwrap(), OutputIndex(1))
            .unwrap(),
        reference
    );
    let mut other = manifest();
    other.work.entity = Id(2);
    other.outputs[0].locator.0 = other.outputs[0]
        .locator
        .0
        .replace("entities/1/", "entities/2/");
    code(journal.remember_manifest(&other), ErrorCode::LimitExceeded);
    drop(journal);
    let journal = Journal::open(&path, creation(), limits, PhysicalLimits::default()).unwrap();
    assert_eq!(
        journal.observed_work(&work()).unwrap().unwrap().view,
        completed
    );
    assert_eq!(
        journal
            .retained_reference(&work(), Id(1), OutputIndex(1))
            .unwrap(),
        reference
    );
}

#[test]
fn missing_or_corrupt_evidence_and_changed_normalized_keys_refuse_reopen() {
    for mutation in [
        "UPDATE operations SET entity=2",
        "UPDATE observations SET entity=2",
        "UPDATE observations SET image=zeroblob(length(image))",
        "UPDATE manifests SET image=zeroblob(length(image))",
        "UPDATE result_references SET output_index=1",
        "UPDATE result_references SET image=zeroblob(length(image))",
        "DELETE FROM manifests",
        "PRAGMA user_version=1",
    ] {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("client.sqlite");
        let journal = bound(&path, 16);
        journal.record_receipt(&admit(&journal)).unwrap();
        journal.observe_work(Id(3), &success()).unwrap();
        journal
            .remember_reference(&manifest(), OutputIndex(0))
            .unwrap();
        let connection = journal.connect().unwrap();
        connection.execute_batch(mutation).unwrap();
        drop(connection);
        drop(journal);
        assert!(
            Journal::open(
                &path,
                creation(),
                JournalLimits {
                    operations: Id(16),
                    ..Default::default()
                },
                PhysicalLimits::default()
            )
            .is_err(),
            "{mutation}"
        );
    }
}

#[test]
fn zero_outputs_and_inputless_cancellation_remain_valid_but_never_invent_references() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("client.sqlite");
    let journal = bound(&path, 16);
    let mut completed = success();
    completed.manifest.as_mut().unwrap().outputs.clear();
    journal.observe_work(Id(3), &completed).unwrap();
    refusal(
        journal.remember_reference(completed.manifest.as_ref().unwrap(), OutputIndex(0)),
        ErrorCode::NotFound,
    );
    let mut cancelled = declared_view();
    cancelled.work.entity = Id(2);
    cancelled.state = State::CANCELLED;
    cancelled.terminal_at = Some(Number(200));
    cancelled.receipt_until = Some(Number(180200));
    journal.observe_work(Id(2), &cancelled).unwrap();
    drop(journal);
    let journal = reopen(&path);
    assert_eq!(
        journal
            .observed_work(&cancelled.work)
            .unwrap()
            .unwrap()
            .view,
        cancelled
    );
    assert_eq!(
        journal.observed_work(&work()).unwrap().unwrap().view,
        completed
    );
}

#[test]
fn profile_policy_and_root_identity_are_checked_without_a_known_admission() {
    let directory = tempfile::tempdir().unwrap();
    let journal = bound(&directory.path().join("client.sqlite"), 16);
    let mut wrong = view();
    wrong.work.producer = Producer(1);
    refusal(
        journal.observe_work(Id(2), &wrong),
        ErrorCode::IntegrityError,
    );
    let mut wrong = success();
    wrong.receipt_until = Some(Number(180201));
    refusal(
        journal.observe_work(Id(3), &wrong),
        ErrorCode::IntegrityError,
    );
    let mut wrong = view();
    wrong.deadline = Some(Number(60101));
    refusal(
        journal.observe_work(Id(2), &wrong),
        ErrorCode::IntegrityError,
    );
    let mut creation = creation();
    creation.results = false;
    let journal = Journal::initialize(
        &directory.path().join("work-only.sqlite"),
        creation,
        JournalLimits::default(),
        PhysicalLimits::default(),
    )
    .unwrap();
    let mut selected = selection();
    selected.supported.pop();
    selected.required.pop();
    journal.record_binding(&binding(), &selected).unwrap();
    code(
        journal.remember_manifest(&manifest()),
        ErrorCode::ExtensionUnsupported,
    );
    let mut completed = success();
    completed.manifest = None;
    completed.output_until = None;
    journal.observe_work(Id(3), &completed).unwrap();
    completed.work.entity = Id(2);
    completed.terminal_at = Some(Number(1100));
    completed.receipt_until = Some(Number(181100));
    refusal(
        journal.observe_work(Id(3), &completed),
        ErrorCode::IntegrityError,
    );
}
