use super::*;
mod boundaries;
mod scopes;

pub(super) fn persist_scope_for_crash(journal: &Journal) {
    scopes::persist_for_crash(journal);
}
pub(super) fn verify_scope_crash_recovery(journal: &Journal) {
    scopes::verify_crash_recovery(journal);
}

pub(super) fn persist_for_crash(journal: &Journal) {
    journal.observe_work(Id(3), &success()).unwrap();
    journal
        .remember_reference(&manifest(), OutputIndex(0))
        .unwrap();
}
pub(super) fn verify_crash_recovery(journal: &Journal) {
    assert_eq!(
        journal.observed_work(&work()).unwrap().unwrap(),
        ObservedWork {
            revision: Id(3),
            view: success()
        }
    );
    let reference = journal
        .retained_reference(&work(), Id(1), OutputIndex(0))
        .unwrap();
    assert_eq!(reference.manifest(), &manifest());
    assert_eq!(reference.index(), OutputIndex(0));
    assert_eq!(
        journal.unresolved(Number(0), PageLimit(256)).unwrap().len(),
        1
    );
}

fn admit(journal: &Journal) -> OperationReceipt {
    let intent = Intent {
        operation: OperationId([2; 16]),
        mutation: Mutation::Admit(AdmitParameters {
            work: work(),
            input: input(),
            application: ApplicationLabel("copy-v1".into()),
            mode: Mode(0),
            execution_ms: Duration(1000),
            outputs: OutputBudget {
                count: BatchCount(2),
                total_bytes: Number(3),
            },
        }),
    };
    journal.prepare(&intent).unwrap();
    receipt(
        journal,
        &intent,
        Outcome::Admitted {
            work: work(),
            attempt: Id(1),
            admitted_at: Number(100),
            deadline: Number(1100),
            child: None,
        },
    )
}
fn input() -> Input {
    Input {
        length: Number(3),
        sha256: Digest([8; 32]),
        content_type: ApplicationLabel("text/plain".into()),
    }
}
fn manifest() -> Manifest {
    Manifest {
        version: Literal,
        authority: creation().authority, owner: creation().owner, generation: Id(7), work: work(), attempt: Id(1),
        input_sha256: input().sha256, committed_at: Number(200), available_until: Number(120200),
        outputs: vec![Output {
            index: OutputIndex(0), length: Number(3), sha256: Digest([9; 32]), content_type: ApplicationLabel("text/plain".into()),
            locator: ResultLocator("pipestream://untrusted-hint.invalid:7443/v2/sessions/7/scopes/0/producers/0/entities/1/attempts/1/outputs/0".into()),
        }],
    }
}
fn view() -> WorkView {
    WorkView {
        work: work(),
        state: State::ACTIVE,
        attempt: Number(1),
        input: Some(input()),
        admitted_at: Some(Number(100)),
        deadline: Some(Number(1100)),
        terminal_at: None,
        receipt_until: None,
        output_until: None,
        child: None,
        manifest: None,
        diagnostic: None,
    }
}
fn success() -> WorkView {
    WorkView {
        state: State::SUCCEEDED,
        terminal_at: Some(Number(200)),
        receipt_until: Some(Number(180200)),
        output_until: Some(Number(120200)),
        manifest: Some(manifest()),
        ..view()
    }
}
fn declared_view() -> WorkView {
    WorkView {
        state: State::DECLARED,
        attempt: Number(0),
        input: None,
        admitted_at: None,
        deadline: None,
        ..view()
    }
}
fn reopen(path: &Path) -> Journal {
    Journal::open(
        path,
        creation(),
        JournalLimits {
            operations: Id(16),
            ..Default::default()
        },
        PhysicalLimits::default(),
    )
    .unwrap()
}
fn refusal<T: std::fmt::Debug>(result: Result<T>, expected: ErrorCode) {
    code(result.map(|_| ()), expected);
}
fn cancelled(journal: &Journal, skip: bool, disposition: u64, state: State) -> OperationReceipt {
    let intent = Intent {
        operation: OperationId([3; 16]),
        mutation: if skip {
            Mutation::Skip { work: work() }
        } else {
            Mutation::Cancel { work: work() }
        },
    };
    journal.prepare(&intent).unwrap();
    let body = if skip {
        Outcome::Skipped {
            work: work(),
            accepted_at: Number(150),
            disposition: Disposition(disposition),
            state_at_commit: state,
        }
    } else {
        Outcome::Cancelled {
            work: work(),
            accepted_at: Number(150),
            disposition: Disposition(disposition),
            state_at_commit: state,
        }
    };
    receipt(journal, &intent, body)
}
fn retried(journal: &Journal) -> OperationReceipt {
    let intent = Intent {
        operation: OperationId([4; 16]),
        mutation: Mutation::Retry {
            work: work(),
            expected_attempt: Id(1),
        },
    };
    journal.prepare(&intent).unwrap();
    receipt(
        journal,
        &intent,
        Outcome::Retried {
            work: work(),
            expected_attempt: Id(1),
            replacement_attempt: Id(2),
            accepted_at: Number(150),
        },
    )
}

#[test]
fn observations_preserve_revision_order_and_immutable_terminal_evidence_on_reopen() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("client.sqlite");
    let journal = bound(&path, 16);
    let admission = admit(&journal);
    journal.record_receipt(&admission).unwrap();
    journal.observe_work(Id(2), &view()).unwrap();
    let terminal = journal.observe_work(Id(3), &success()).unwrap();
    assert_eq!(
        journal.observe_work(Id(1), &declared_view()).unwrap(),
        terminal
    );
    let mut changed = view();
    changed.state = State::CANCELLING;
    refusal(
        journal.observe_work(Id(3), &changed),
        ErrorCode::IntegrityError,
    );
    refusal(
        journal.observe_work(Id(4), &changed),
        ErrorCode::IntegrityError,
    );
    drop(journal);
    let journal = reopen(&path);
    assert_eq!(journal.observed_work(&work()).unwrap(), Some(terminal));
    assert_eq!(
        journal.receipt(admission.operation).unwrap(),
        Some(admission)
    );
}

#[test]
fn views_and_manifests_enforce_known_admission_and_exact_retention_policy() {
    let directory = tempfile::tempdir().unwrap();
    let journal = bound(&directory.path().join("client.sqlite"), 16);
    journal.record_receipt(&admit(&journal)).unwrap();
    for field in 0..6 {
        let mut changed = view();
        match field {
            0 => changed.input.as_mut().unwrap().sha256.0[0] ^= 1,
            1 => changed.input.as_mut().unwrap().length = Number(4),
            2 => changed.input.as_mut().unwrap().content_type = ApplicationLabel("other".into()),
            3 => changed.admitted_at = Some(Number(101)),
            4 => changed.deadline = Some(Number(1101)),
            _ => {
                changed.child = Some(ChildScope {
                    scope: Id(1),
                    producer: Producer(0),
                })
            }
        }
        refusal(
            journal.observe_work(Id(2), &changed),
            ErrorCode::IntegrityError,
        );
    }
    for field in 0..7 {
        let mut changed = manifest();
        match field {
            0 => changed.authority = IdentityLabel("other".into()),
            1 => changed.owner = IdentityLabel("other".into()),
            2 => changed.input_sha256.0[0] ^= 1,
            3 => changed.available_until = Number(120201),
            4 => changed.outputs[0].length = Number(4),
            5 => {
                changed.committed_at = Number(99);
                changed.available_until = Number(120099);
            }
            _ => {
                changed.committed_at = Number(1100);
                changed.available_until = Number(121100);
            }
        }
        code(
            journal.remember_manifest(&changed),
            ErrorCode::IntegrityError,
        );
    }
    assert!(journal.observed_work(&work()).unwrap().is_none());
    journal.observe_work(Id(3), &success()).unwrap();
}

#[test]
fn full_manifest_and_explicit_output_selection_survive_reopen_without_uri_credentials() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("client.sqlite");
    let journal = bound(&path, 16);
    journal.remember_manifest(&manifest()).unwrap();
    refusal(
        journal.retained_reference(&work(), Id(1), OutputIndex(0)),
        ErrorCode::NotFound,
    );
    refusal(
        journal.remember_reference(&manifest(), OutputIndex(1)),
        ErrorCode::NotFound,
    );
    let reference = journal
        .remember_reference(&manifest(), OutputIndex(0))
        .unwrap();
    assert_eq!(reference.manifest(), &manifest());
    assert_eq!(reference.index(), OutputIndex(0));
    assert_eq!(
        reference.attach(Id(7)).unwrap(),
        Control::Session(Session::Attach {
            request: Id(7),
            authority: creation().authority,
            owner: creation().owner,
            generation: Id(7),
        })
    );
    assert_eq!(
        reference.read(Id(8)).unwrap(),
        Control::Result(ResultMessage::Read {
            request: Id(8),
            work: work(),
            attempt: Id(1),
            index: OutputIndex(0),
            expected_sha256: Digest([9; 32]),
        })
    );
    drop(journal);
    let journal = reopen(&path);
    assert_eq!(
        journal
            .retained_reference(&work(), Id(1), OutputIndex(0))
            .unwrap(),
        reference
    );
    refusal(
        journal.retained_reference(&work(), Id(2), OutputIndex(0)),
        ErrorCode::NotFound,
    );
    assert!(
        journal
            .unresolved(Number(0), PageLimit(1))
            .unwrap()
            .is_empty()
    );
}

#[test]
fn accepted_fence_or_replacement_refuses_conflicting_success_in_both_arrival_orders() {
    for action in 0..3 {
        for receipt_first in [false, true] {
            for observation in [false, true] {
                let directory = tempfile::tempdir().unwrap();
                let journal = bound(&directory.path().join("client.sqlite"), 16);
                let received = match action {
                    0 => cancelled(&journal, false, 0, State::CANCELLING),
                    1 => cancelled(&journal, true, 0, State::SKIPPED),
                    _ => retried(&journal),
                };
                let save = || {
                    if observation {
                        journal.observe_work(Id(3), &success()).map(|_| ())
                    } else {
                        journal.remember_manifest(&manifest())
                    }
                };
                if receipt_first {
                    journal.record_receipt(&received).unwrap();
                    code(save(), ErrorCode::IntegrityError);
                } else {
                    save().unwrap();
                    code(journal.record_receipt(&received), ErrorCode::IntegrityError);
                    assert!(journal.receipt(received.operation).unwrap().is_none());
                }
            }
        }
    }
}

#[test]
fn observation_and_reference_failure_roll_back_the_contained_manifest() {
    for table in ["observations", "result_references"] {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("client.sqlite");
        let journal = bound(&path, 16);
        let connection = journal.connect().unwrap();
        connection.execute_batch(&format!("CREATE TRIGGER reject BEFORE INSERT ON {table} BEGIN SELECT RAISE(ABORT, 'injected commit failure'); END;")).unwrap();
        let result = if table == "observations" {
            journal.observe_work(Id(3), &success()).map(|_| ())
        } else {
            journal
                .remember_reference(&manifest(), OutputIndex(0))
                .map(|_| ())
        };
        assert!(
            matches!(result, Err(JournalError::Database(_))),
            "{result:?}"
        );
        for checked in ["observations", "manifests", "result_references"] {
            assert_eq!(
                connection
                    .query_row(&format!("SELECT count(*) FROM {checked}"), [], |r| r
                        .get::<_, i64>(0))
                    .unwrap(),
                0
            );
        }
        connection.execute_batch("DROP TRIGGER reject").unwrap();
        drop(connection);
        drop(journal);
        let journal = reopen(&path);
        journal.observe_work(Id(3), &success()).unwrap();
        journal
            .remember_reference(&manifest(), OutputIndex(0))
            .unwrap();
    }
}

#[test]
fn valid_replacement_and_terminal_fence_receipts_remain_valid_on_reopen() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("client.sqlite");
    let journal = bound(&path, 16);
    journal.record_receipt(&admit(&journal)).unwrap();
    journal.record_receipt(&retried(&journal)).unwrap();
    let mut completed = success();
    completed.attempt = Number(2);
    let manifest = completed.manifest.as_mut().unwrap();
    manifest.attempt = Id(2);
    manifest.outputs[0].locator.0 = manifest.outputs[0]
        .locator
        .0
        .replace("attempts/1/", "attempts/2/");
    journal.observe_work(Id(5), &completed).unwrap();
    // A delayed snapshot is retained only if it is compatible; it does not roll back the retry.
    assert_eq!(
        journal.observe_work(Id(2), &view()).unwrap().view,
        completed
    );
    let mut cancellation = cancelled(&journal, false, 1, State::SUCCEEDED);
    let Outcome::Cancelled { accepted_at, .. } = &mut cancellation.body else {
        unreachable!()
    };
    *accepted_at = Number(201);
    journal.record_receipt(&cancellation).unwrap();
    let selected = journal
        .remember_reference(completed.manifest.as_ref().unwrap(), OutputIndex(0))
        .unwrap();
    drop(journal);
    let journal = reopen(&path);
    assert_eq!(
        journal.observed_work(&work()).unwrap().unwrap().view,
        completed
    );
    assert_eq!(
        journal
            .retained_reference(&work(), Id(2), OutputIndex(0))
            .unwrap(),
        selected
    );
}

#[test]
fn cancellation_and_retry_waits_cannot_turn_into_success_for_the_fenced_attempt() {
    for state in [State::CANCELLING, State::AWAITING_RETRY] {
        for first in [false, true] {
            let directory = tempfile::tempdir().unwrap();
            let journal = bound(&directory.path().join("client.sqlite"), 16);
            let mut waiting = view();
            waiting.state = state;
            if state == State::AWAITING_RETRY {
                waiting.diagnostic = Some(Diagnostic {
                    code: DiagnosticCode(1),
                    detail: Detail("retryable".into()),
                });
            }
            if first {
                journal.observe_work(Id(3), &waiting).unwrap();
                refusal(
                    journal.observe_work(Id(4), &view()),
                    ErrorCode::IntegrityError,
                );
                code(
                    journal.remember_manifest(&manifest()),
                    ErrorCode::IntegrityError,
                );
            } else {
                journal.remember_manifest(&manifest()).unwrap();
                refusal(
                    journal.observe_work(Id(3), &waiting),
                    ErrorCode::IntegrityError,
                );
            }
        }
    }
}

#[test]
fn receipt_pairs_refuse_conflicting_fences_and_retry_times_without_an_observation() {
    for reversed in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let journal = bound(&directory.path().join("client.sqlite"), 16);
        let cancel = cancelled(&journal, false, 0, State::CANCELLING);
        let intent = Intent {
            operation: OperationId([5; 16]),
            mutation: Mutation::Skip { work: work() },
        };
        journal.prepare(&intent).unwrap();
        let skip = receipt(
            &journal,
            &intent,
            Outcome::Skipped {
                work: work(),
                accepted_at: Number(151),
                disposition: Disposition(0),
                state_at_commit: State::CANCELLING,
            },
        );
        let (first, second) = if reversed {
            (&skip, &cancel)
        } else {
            (&cancel, &skip)
        };
        journal.record_receipt(first).unwrap();
        code(journal.record_receipt(second), ErrorCode::IntegrityError);
    }
    for time in [99, 1100] {
        for reversed in [false, true] {
            let directory = tempfile::tempdir().unwrap();
            let journal = bound(&directory.path().join("client.sqlite"), 16);
            let admitted = admit(&journal);
            let mut retry = retried(&journal);
            let Outcome::Retried { accepted_at, .. } = &mut retry.body else {
                unreachable!()
            };
            *accepted_at = Number(time);
            let (first, second) = if reversed {
                (&retry, &admitted)
            } else {
                (&admitted, &retry)
            };
            journal.record_receipt(first).unwrap();
            code(journal.record_receipt(second), ErrorCode::IntegrityError);
        }
    }
}
