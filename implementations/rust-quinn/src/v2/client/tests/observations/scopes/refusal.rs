use super::*;

#[test]
fn declaration_receipts_and_page_inventory_cross_validate_in_both_arrival_orders() {
    for receipt_first in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let journal = bound(&directory.path().join("client.sqlite"), 16);
        let root = Fixture::root(&[1, 3]);
        let intent = Intent {
            operation: OperationId([11; 16]),
            mutation: Mutation::Declare {
                scope: Number(0),
                entity_ids: vec![Id(2)],
                seal: true,
            },
        };
        journal.prepare(&intent).unwrap();
        let received = receipt(
            &journal,
            &intent,
            Outcome::Declared {
                scope: Number(0),
                producer: Producer(0),
                accepted_count: BatchCount(1),
                declared: Number(2),
                seal: Some(root.seal(&journal)),
            },
        );
        if receipt_first {
            journal.record_receipt(&received).unwrap();
            let (request, response) =
                root.page(&journal, 0, &root.ids, State::DECLARED, true, false);
            refusal(
                journal.observe_scope_page(&request, &response),
                ErrorCode::IntegrityError,
            );
        } else {
            root.observe(&journal, State::DECLARED);
            code(journal.record_receipt(&received), ErrorCode::IntegrityError);
            assert!(journal.receipt(received.operation).unwrap().is_none());
        }
    }
}

#[test]
fn sealing_receipt_verifies_cached_unsealed_membership_or_rolls_back_atomically() {
    for wrong in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let journal = bound(&directory.path().join("client.sqlite"), 16);
        let root = Fixture::root(&[1, 2, 3]);
        let (request, response) = root.page(&journal, 0, &root.ids, State::DECLARED, false, false);
        assert!(
            !journal
                .observe_scope_page(&request, &response)
                .unwrap()
                .membership_verified
        );
        let intent = Intent {
            operation: OperationId([11; 16]),
            mutation: Mutation::Declare {
                scope: Number(0),
                entity_ids: vec![Id(3)],
                seal: true,
            },
        };
        journal.prepare(&intent).unwrap();
        let mut seal = root.seal(&journal);
        if wrong {
            seal.0[0] ^= 1;
        }
        let received = receipt(
            &journal,
            &intent,
            Outcome::Declared {
                scope: Number(0),
                producer: Producer(0),
                accepted_count: BatchCount(1),
                declared: Number(3),
                seal: Some(seal),
            },
        );
        if wrong {
            code(journal.record_receipt(&received), ErrorCode::IntegrityError);
            assert!(journal.receipt(received.operation).unwrap().is_none());
            assert!(
                !journal
                    .scope_observation(Number(0))
                    .unwrap()
                    .unwrap()
                    .membership_verified
            );
        } else {
            journal.record_receipt(&received).unwrap();
            assert!(
                journal
                    .scope_observation(Number(0))
                    .unwrap()
                    .unwrap()
                    .membership_verified
            );
        }
    }
}

#[test]
fn parent_scope_metadata_rejects_impossible_child_identity_in_both_arrival_orders() {
    for child_first in [false, true] {
        for wrong_producer in [false, true] {
            let directory = tempfile::tempdir().unwrap();
            let journal = bound(&directory.path().join("client.sqlite"), 16);
            let parent = Fixture {
                scope: Number(1),
                producer: Producer(1),
                parent: Some(work()),
                ids: vec![],
            };
            let child = Fixture {
                scope: Number(2),
                producer: Producer(0),
                parent: Some(WorkKey {
                    scope: Number(1),
                    producer: Producer(if wrong_producer { 0 } else { 1 }),
                    entity: Id(9),
                }),
                ids: vec![],
            };
            let (first, second) = if child_first {
                (&child, &parent)
            } else {
                (&parent, &child)
            };
            first.observe(&journal, State::DECLARED);
            let (request, response) = second.page(&journal, 0, &[], State::DECLARED, true, false);
            refusal(
                journal.observe_scope_page(&request, &response),
                ErrorCode::IntegrityError,
            );
            assert!(journal.scope_observation(second.scope).unwrap().is_none());
            assert!(journal.scope_observation(first.scope).unwrap().is_some());
        }
    }
}

#[test]
fn immutable_parent_allocation_rejects_changed_child_metadata_in_both_arrival_orders() {
    for child_first in [false, true] {
        for changed_field in 0..3 {
            let directory = tempfile::tempdir().unwrap();
            let path = directory.path().join("client.sqlite");
            let journal = bound(&path, 16);
            let (branch, _, _, mut nested) = branch_views();
            match changed_field {
                0 => nested.scope = Number(2),
                1 => nested.producer = Producer(0),
                _ => nested.parent.as_mut().unwrap().entity = Id(2),
            }
            if child_first {
                nested.observe(&journal, State::DECLARED);
                refusal(
                    journal.observe_work(Id(4), &branch),
                    ErrorCode::IntegrityError,
                );
                assert!(journal.observed_work(&branch.work).unwrap().is_none());
            } else {
                journal.observe_work(Id(4), &branch).unwrap();
                let (request, response) =
                    nested.page(&journal, 0, &nested.ids, State::DECLARED, true, false);
                refusal(
                    journal.observe_scope_page(&request, &response),
                    ErrorCode::IntegrityError,
                );
            }
            drop(journal);
            let journal = reopen(&path);
            assert_eq!(
                journal.scope_observation(nested.scope).unwrap().is_some(),
                child_first
            );
            assert_eq!(
                journal.observed_work(&branch.work).unwrap().is_some(),
                !child_first
            );
        }
    }
}

#[test]
fn sealing_receipt_rechecks_pending_child_membership_in_both_arrival_orders() {
    for child_first in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("client.sqlite");
        let journal = bound(&path, 16);
        let root = Fixture::root(&[1]);
        let child = Fixture {
            scope: Number(1),
            producer: Producer(1),
            parent: Some(WorkKey {
                scope: Number(0),
                producer: Producer(0),
                entity: Id(9),
            }),
            ids: vec![],
        };
        let (request, response) = root.page(&journal, 0, &root.ids, State::DECLARED, false, false);
        journal.observe_scope_page(&request, &response).unwrap();
        let intent = Intent {
            operation: OperationId([11; 16]),
            mutation: Mutation::Declare {
                scope: Number(0),
                entity_ids: vec![],
                seal: true,
            },
        };
        journal.prepare(&intent).unwrap();
        let received = receipt(
            &journal,
            &intent,
            Outcome::Declared {
                scope: Number(0),
                producer: Producer(0),
                accepted_count: BatchCount(0),
                declared: Number(1),
                seal: Some(root.seal(&journal)),
            },
        );
        if child_first {
            child.observe(&journal, State::DECLARED);
            code(journal.record_receipt(&received), ErrorCode::IntegrityError);
            assert!(journal.receipt(received.operation).unwrap().is_none());
        } else {
            journal.record_receipt(&received).unwrap();
            let (request, response) = child.page(&journal, 0, &[], State::DECLARED, true, false);
            refusal(
                journal.observe_scope_page(&request, &response),
                ErrorCode::IntegrityError,
            );
        }
        drop(journal);
        let journal = reopen(&path);
        assert_eq!(
            journal
                .scope_observation(Number(0))
                .unwrap()
                .unwrap()
                .membership_verified,
            !child_first
        );
        assert_eq!(
            journal.scope_observation(Number(1)).unwrap().is_some(),
            child_first
        );
    }
}

#[test]
fn count_matching_inventory_must_still_hash_to_the_expected_seal() {
    let directory = tempfile::tempdir().unwrap();
    let journal = bound(&directory.path().join("client.sqlite"), 16);
    let root = Fixture::root(&[1, 2, 3]);
    let (request, response) = root.page(
        &journal,
        0,
        &[Id(1), Id(4), Id(5)],
        State::DECLARED,
        true,
        false,
    );
    refusal(
        journal.observe_scope_page(&request, &response),
        ErrorCode::IntegrityError,
    );
    assert!(journal.scope_observation(Number(0)).unwrap().is_none());
}

#[test]
fn pages_refuse_wrong_correlation_omissions_and_changed_full_seal_atomically() {
    for field in 0..6 {
        let directory = tempfile::tempdir().unwrap();
        let journal = bound(&directory.path().join("client.sqlite"), 16);
        let root = Fixture::root(&[1, 2, 3]);
        let (request, response) =
            root.page(&journal, 0, &root.ids[..2], State::DECLARED, true, true);
        assert!(
            !journal
                .observe_scope_page(&request, &response)
                .unwrap()
                .membership_verified
        );
        let (mut request, mut response) =
            root.page(&journal, 0, &root.ids, State::DECLARED, true, false);
        let Control::Scope(Scope::PageResponse {
            request: id,
            entries,
            seal,
            more,
            ..
        }) = &mut response
        else {
            unreachable!()
        };
        match field {
            0 => *id = Id(2),
            1 => {
                let Control::Scope(Scope::Page { after_entity, .. }) = &mut request else {
                    unreachable!()
                };
                *after_entity = Number(1);
            }
            2 => {
                entries.remove(1);
                *more = true;
            }
            3 => seal.as_mut().unwrap().0[0] ^= 1,
            4 => {
                entries.pop();
            }
            _ => {
                let Control::Scope(Scope::Page { limit, .. }) = &mut request else {
                    unreachable!()
                };
                *limit = PageLimit(2);
            }
        }
        refusal(
            journal.observe_scope_page(&request, &response),
            ErrorCode::IntegrityError,
        );
        assert_eq!(
            journal
                .scope_members(Number(0), Number(0), PageLimit(256))
                .unwrap()
                .len(),
            2
        );
        assert!(
            !journal
                .scope_observation(Number(0))
                .unwrap()
                .unwrap()
                .membership_verified
        );
    }
}

#[test]
fn checkpoint_refuses_changed_counts_roots_times_and_terminal_evidence() {
    let directory = tempfile::tempdir().unwrap();
    let journal = bound(&directory.path().join("client.sqlite"), 16);
    let root = Fixture::root(&[1]);
    root.observe(&journal, State::SUCCEEDED);
    let summary = root.summary(&journal, &[success()], &[], 210);
    code(journal.record_checkpoint(&summary), ErrorCode::NotReady);
    journal.observe_work(Id(3), &success()).unwrap();
    for field in 0..4 {
        let mut wrong = summary.clone();
        match field {
            0 => {
                wrong.counts.success = Number(0);
                wrong.counts.failure = Number(1);
            }
            1 => wrong.status_root.0[0] ^= 1,
            2 => wrong.closed_at = Number(199),
            _ => wrong.seal.0[0] ^= 1,
        }
        code(journal.record_checkpoint(&wrong), ErrorCode::IntegrityError);
    }
    journal.record_checkpoint(&summary).unwrap();
    let mut wrong = summary.clone();
    wrong.closed_at = Number(211);
    code(journal.record_checkpoint(&wrong), ErrorCode::IntegrityError);
    let (request, response) = root.page(&journal, 0, &root.ids, State::CANCELLED, true, false);
    refusal(
        journal.observe_scope_page(&request, &response),
        ErrorCode::IntegrityError,
    );
    assert_eq!(journal.covered_scope(Number(0)).unwrap(), Some(summary));
}

#[test]
fn work_outside_verified_membership_is_refused_in_both_arrival_orders() {
    for page_first in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let journal = bound(&directory.path().join("client.sqlite"), 16);
        let root = Fixture::root(&[2]);
        if page_first {
            root.observe(&journal, State::DECLARED);
            refusal(
                journal.observe_work(Id(3), &success()),
                ErrorCode::IntegrityError,
            );
            code(
                journal.remember_manifest(&manifest()),
                ErrorCode::IntegrityError,
            );
        } else {
            journal.observe_work(Id(3), &success()).unwrap();
            let (request, response) =
                root.page(&journal, 0, &root.ids, State::DECLARED, true, false);
            refusal(
                journal.observe_scope_page(&request, &response),
                ErrorCode::IntegrityError,
            );
            assert!(journal.scope_observation(Number(0)).unwrap().is_none());
        }
    }
}

#[test]
fn accepted_scope_fence_excludes_later_publication_regardless_of_reply_order() {
    for first in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let journal = bound(&directory.path().join("client.sqlite"), 16);
        let intent = Intent {
            operation: OperationId([7; 16]),
            mutation: Mutation::ScopeCancel { scope: Number(0) },
        };
        journal.prepare(&intent).unwrap();
        let fence = receipt(
            &journal,
            &intent,
            Outcome::ScopeCancelled {
                scope: Number(0),
                accepted_at: Number(150),
            },
        );
        if first {
            journal.record_receipt(&fence).unwrap();
            refusal(
                journal.observe_work(Id(3), &success()),
                ErrorCode::IntegrityError,
            );
            code(
                journal.remember_manifest(&manifest()),
                ErrorCode::IntegrityError,
            );
        } else {
            journal.observe_work(Id(3), &success()).unwrap();
            code(journal.record_receipt(&fence), ErrorCode::IntegrityError);
            assert!(journal.receipt(fence.operation).unwrap().is_none());
        }
    }
}
