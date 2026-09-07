use super::*;

#[test]
fn scope_and_checkpoint_write_failures_preserve_uncertainty_and_rollback_members() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("client.sqlite");
    let journal = bound(&path, 16);
    let original = declare();
    journal.prepare(&original).unwrap();
    let root = Fixture::root(&[1]);
    let connection = journal.connect().unwrap();
    connection.execute_batch("CREATE TRIGGER fail_scope BEFORE INSERT ON scopes BEGIN SELECT RAISE(ABORT,'scope commit fault'); END;").unwrap();
    let (request, response) = root.page(&journal, 0, &root.ids, State::SUCCEEDED, true, false);
    assert!(matches!(
        journal.observe_scope_page(&request, &response),
        Err(JournalError::Database(_))
    ));
    assert_eq!(
        connection
            .query_row("SELECT count(*) FROM scope_members", [], |r| r
                .get::<_, i64>(0))
            .unwrap(),
        0
    );
    assert!(journal.scope_observation(Number(0)).unwrap().is_none());
    connection.execute_batch("DROP TRIGGER fail_scope").unwrap();
    root.observe(&journal, State::SUCCEEDED);
    journal.observe_work(Id(3), &success()).unwrap();
    let summary = root.summary(&journal, &[success()], &[], 210);
    connection.execute_batch("CREATE TRIGGER fail_coverage BEFORE INSERT ON scope_coverage BEGIN SELECT RAISE(ABORT,'coverage commit fault'); END;").unwrap();
    assert!(matches!(
        journal.record_checkpoint(&summary),
        Err(JournalError::Database(_))
    ));
    refusal(journal.root_completion(Id(9)), ErrorCode::NotReady);
    assert!(journal.covered_scope(Number(0)).unwrap().is_none());
    connection
        .execute_batch("DROP TRIGGER fail_coverage")
        .unwrap();
    drop(connection);
    drop(journal);
    let journal = reopen(&path);
    assert_eq!(journal.intent(original.operation).unwrap(), original);
    assert!(journal.receipt(original.operation).unwrap().is_none());
    journal.record_checkpoint(&summary).unwrap();
    assert_eq!(journal.covered_scope(Number(0)).unwrap(), Some(summary));
}

#[test]
fn bounded_member_inventory_rolls_back_a_page_without_evicting_prior_scopes() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("client.sqlite");
    let limits = JournalLimits {
        operations: Id(16),
        observations: Id(3),
    };
    let journal =
        Journal::initialize(&path, creation(), limits.clone(), PhysicalLimits::default()).unwrap();
    journal.record_binding(&binding(), &selection()).unwrap();
    let root = Fixture::root(&[1, 2]);
    root.observe(&journal, State::DECLARED);
    let child = Fixture {
        scope: Number(1),
        producer: Producer(0),
        parent: Some(work()),
        ids: vec![Id(10), Id(11)],
    };
    let (request, response) = child.page(&journal, 0, &child.ids, State::DECLARED, true, false);
    refusal(
        journal.observe_scope_page(&request, &response),
        ErrorCode::LimitExceeded,
    );
    assert!(journal.scope_observation(Number(1)).unwrap().is_none());
    assert_eq!(
        journal
            .scope_members(Number(0), Number(0), PageLimit(256))
            .unwrap()
            .len(),
        2
    );
    let (request, response) = child.page(&journal, 0, &child.ids[..1], State::DECLARED, true, true);
    journal.observe_scope_page(&request, &response).unwrap();
    let (request, response) =
        child.page(&journal, 10, &child.ids[1..], State::DECLARED, true, false);
    refusal(
        journal.observe_scope_page(&request, &response),
        ErrorCode::LimitExceeded,
    );
    drop(journal);
    let journal = Journal::open(&path, creation(), limits, PhysicalLimits::default()).unwrap();
    assert!(
        journal
            .scope_observation(Number(0))
            .unwrap()
            .unwrap()
            .membership_verified
    );
    assert!(
        !journal
            .scope_observation(Number(1))
            .unwrap()
            .unwrap()
            .membership_verified
    );
    assert_eq!(
        journal
            .scope_members(Number(1), Number(0), PageLimit(1))
            .unwrap()[0]
            .work
            .entity,
        Id(10)
    );
}

#[test]
fn corrupted_scope_indexes_images_or_missing_descendant_proof_refuse_reopen() {
    for mutation in [
        "UPDATE scopes SET producer=0 WHERE scope=1",
        "UPDATE scopes SET parent_entity=2 WHERE scope=1",
        "UPDATE scopes SET image=zeroblob(length(image)) WHERE scope=0",
        "UPDATE scope_members SET entity=2 WHERE scope=0",
        "UPDATE scope_members SET image=zeroblob(length(image)) WHERE scope=1",
        "UPDATE scope_coverage SET image=zeroblob(length(image)) WHERE scope=1",
        "DELETE FROM scope_coverage WHERE scope=1",
        "DELETE FROM scope_members WHERE scope=1",
        "DELETE FROM scopes WHERE scope=1",
        "DELETE FROM observations WHERE scope=1",
        "PRAGMA user_version=2",
    ] {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("client.sqlite");
        let journal = bound(&path, 16);
        let (branch, child, root, nested) = branch_views();
        root.observe(&journal, State::SUCCEEDED);
        journal.observe_work(Id(4), &branch).unwrap();
        nested.observe(&journal, State::SUCCEEDED);
        journal.observe_work(Id(3), &child).unwrap();
        let child_summary = nested.summary(&journal, &[child], &[], 210);
        journal.record_checkpoint(&child_summary).unwrap();
        journal
            .record_checkpoint(&root.summary(&journal, &[branch], &[child_summary], 260))
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
