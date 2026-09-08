use super::*;
use std::sync::atomic::{AtomicBool, Ordering};

#[derive(Clone, Copy)]
enum CompletionCase {
    Unsealed,
    SealedUnadmitted,
    SealedAdmitted,
}

struct CompletionExpansion(CompletionCase);
impl Application for CompletionExpansion {
    fn expansion(&self) -> Option<&dyn Expansion> {
        Some(self)
    }

    fn execute(&self, _: &mut WorkContext) -> Result<ApplicationOutcome> {
        Ok(ApplicationOutcome::Succeeded)
    }
}

struct YieldThenComplete(AtomicBool);
impl Application for YieldThenComplete {
    fn expansion(&self) -> Option<&dyn Expansion> {
        Some(self)
    }

    fn execute(&self, _: &mut WorkContext) -> Result<ApplicationOutcome> {
        Ok(ApplicationOutcome::Succeeded)
    }
}

impl Expansion for YieldThenComplete {
    fn expand(&self, context: &mut ExpansionContext<'_>) -> Result<ExpansionOutcome> {
        context.declare(context.operation(Id(1))?, &[Id(1)], true)?;
        if self.0.swap(false, Ordering::SeqCst) {
            Ok(ExpansionOutcome::Yield)
        } else {
            Ok(ExpansionOutcome::Complete)
        }
    }
}

impl Expansion for CompletionExpansion {
    fn expand(&self, context: &mut ExpansionContext<'_>) -> Result<ExpansionOutcome> {
        let sealed = !matches!(self.0, CompletionCase::Unsealed);
        context.declare(context.operation(Id(1))?, &[Id(1)], sealed)?;
        if matches!(self.0, CompletionCase::SealedAdmitted) {
            let parameters = AdmitParameters {
                work: WorkKey {
                    scope: Number(context.child_scope().0),
                    producer: Producer(1),
                    entity: Id(1),
                },
                input: Input {
                    length: Number(1),
                    sha256: Digest(Sha256::digest(b"x").into()),
                    content_type: ApplicationLabel("text/plain".into()),
                },
                application: ApplicationLabel("test/v1".into()),
                mode: Mode(0),
                execution_ms: Duration(1000),
                outputs: OutputBudget {
                    count: BatchCount(0),
                    total_bytes: Number(0),
                },
            };
            let now = Instant::now();
            match context.receive_input(context.operation(Id(2))?, parameters, now)? {
                InputReception::Replay(_) => {}
                InputReception::Receiving(mut receiving) => {
                    receiving.receive(b"x", now)?;
                    match context.prepare_input(receiving.finish(now)?)? {
                        InputPreparation::Replay(_) => {}
                        InputPreparation::Ready(prepared) => {
                            context.admit_input(*prepared)?;
                        }
                    }
                }
            }
        }
        Ok(ExpansionOutcome::Complete)
    }
}

#[test]
fn complete_with_unsealed_membership_is_not_ready_and_keeps_current_attempt_incomplete() {
    let fixture = Fixture::new(Arc::new(CompletionExpansion(CompletionCase::Unsealed)));
    fixture.admit(2, 0, 0);
    let before = geometry(&fixture);

    refuse(fixture.run(), ErrorCode::NotReady);
    let view = fixture.view();
    let job = fixture.job();
    assert_eq!((view.state, view.attempt), (State::ACTIVE, Number(1)));
    assert!(!job.expansion_complete);
    assert_eq!(job.stage, Number(1));
    assert_eq!(job.attempt, Id(1));
    assert_eq!(job.lease, Number(1));
    assert!(job.lease_until.is_some());
    assert_eq!(geometry(&fixture), before);
    fixture.store.integrity_check().unwrap();
}

#[test]
fn complete_with_sealed_but_unadmitted_child_is_not_ready_and_keeps_lease_current() {
    let fixture = Fixture::new(Arc::new(CompletionExpansion(
        CompletionCase::SealedUnadmitted,
    )));
    fixture.admit(2, 0, 0);
    let before = geometry(&fixture);

    refuse(fixture.run(), ErrorCode::NotReady);
    let view = fixture.view();
    let job = fixture.job();
    assert_eq!((view.state, view.attempt), (State::ACTIVE, Number(1)));
    assert!(!job.expansion_complete);
    assert_eq!(job.stage, Number(1));
    assert_eq!(job.attempt, Id(1));
    assert_eq!(job.lease, Number(1));
    assert!(job.lease_until.is_some());
    let child = WorkKey {
        scope: Number(1),
        producer: Producer(1),
        entity: Id(1),
    };
    let child_view = fixture
        .store
        .work_view(&fixture.binding.identity, &child, Number(0))
        .unwrap()
        .1;
    assert_eq!(
        (child_view.state, child_view.attempt),
        (State::DECLARED, Number(0))
    );
    assert!(child_view.input.is_none());
    assert!(child_view.admitted_at.is_none());
    let mut connection = fixture.store.connect().unwrap();
    let tx = connection.transaction().unwrap();
    refuse(
        load(&tx, &fixture.binding.identity, &child),
        ErrorCode::NotReady,
    );
    drop(tx);
    drop(connection);
    assert_eq!(geometry(&fixture), before);
    fixture.store.integrity_check().unwrap();
}

#[test]
fn complete_with_real_admitted_unexecuted_child_enters_waiting_children() {
    let fixture = Fixture::new(Arc::new(CompletionExpansion(
        CompletionCase::SealedAdmitted,
    )));
    fixture.admit(2, 0, 0);

    let parent = fixture.run().unwrap();
    assert_eq!(
        (parent.state, parent.attempt),
        (State::WAITING_CHILDREN, Number(1))
    );
    let job = fixture.job();
    assert!(job.expansion_complete);
    assert_eq!(job.stage, Number(2));
    assert_eq!(job.attempt, Id(1));
    assert!(job.lease_until.is_none());
    let child = WorkKey {
        scope: Number(1),
        producer: Producer(1),
        entity: Id(1),
    };
    let child_view = fixture
        .store
        .work_view(&fixture.binding.identity, &child, Number(0))
        .unwrap()
        .1;
    assert_eq!(
        (child_view.state, child_view.attempt),
        (State::ACTIVE, Number(1))
    );
    fixture.store.integrity_check().unwrap();
    fixture.reopen().integrity_check().unwrap();
}

#[test]
fn complete_accepts_real_terminal_child_that_never_had_an_input() {
    let fixture = Fixture::new(Arc::new(YieldThenComplete(AtomicBool::new(true))));
    fixture.admit(2, 0, 0);
    assert_eq!(fixture.run().unwrap().state, State::ACTIVE);
    let child = WorkKey {
        scope: Number(1),
        producer: Producer(1),
        entity: Id(1),
    };
    fixture
        .store
        .skip_work(&fixture.binding.identity, OperationId([90; 16]), &child)
        .unwrap();
    assert_eq!(
        fixture
            .store
            .work_view(&fixture.binding.identity, &child, Number(0))
            .unwrap()
            .1
            .state,
        State::SKIPPED
    );

    let parent = fixture.run().unwrap();
    assert_eq!(
        (parent.state, parent.attempt),
        (State::WAITING_CHILDREN, Number(1))
    );
    assert!(fixture.job().expansion_complete);
    fixture.store.integrity_check().unwrap();
    fixture.reopen().integrity_check().unwrap();
}

fn geometry(fixture: &Fixture) -> ((u64, usize), (u64, usize)) {
    let mut connection = fixture.store.connect().unwrap();
    let tx = connection.transaction().unwrap();
    let (row, _, _, _, _) = load(&tx, &fixture.binding.identity, &fixture.key()).unwrap();
    let work = records::header(&tx, work_target(row)).unwrap();
    let job = records::header(&tx, job_target(row)).unwrap();
    ((work.credits, work.capacity), (job.credits, job.capacity))
}

#[test]
fn recovery_rejects_checksummed_completed_expansion_with_unadmitted_obligation() {
    let fixture = Fixture::new(Arc::new(YieldThenComplete(AtomicBool::new(true))));
    fixture.admit(2, 0, 0);
    assert_eq!(fixture.run().unwrap().state, State::ACTIVE);
    let mut connection = fixture.store.connect().unwrap();
    let tx = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .unwrap();
    let (row, mut job, job_revision, mut view, work_revision) =
        load(&tx, &fixture.binding.identity, &fixture.key()).unwrap();
    assert!(!job.expansion_complete && job.lease_until.is_none());
    assert_eq!(job.stage, Number(0));
    job.expansion_complete = true;
    job.stage = Number(2);
    view.state = State::WAITING_CHILDREN;
    records::replace(&tx, work_target(row), work_revision, &view, false).unwrap();
    records::replace(&tx, job_target(row), job_revision, &job, false).unwrap();
    tx.commit().unwrap();
    drop(connection);

    assert!(matches!(
        fixture.store.integrity_check(),
        Err(StoreError::Corrupt(_))
    ));
    assert!(matches!(
        AuthorityStore::open(
            &fixture.directory.path().join("authority.sqlite"),
            fixture.store.authority.clone(),
            fixture.store.policy.clone(),
            fixture.store.physical.limits,
            fixture.clock.clone(),
            fixture.auth.clone(),
        ),
        Err(StoreError::Corrupt(_))
    ));
}
