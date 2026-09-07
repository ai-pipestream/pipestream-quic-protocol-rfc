use super::*;
mod refusal;
mod storage;

pub(super) fn persist_for_crash(journal: &Journal) {
    let root = Fixture::root(&[1]);
    root.observe(journal, State::SUCCEEDED);
    journal.observe_work(Id(3), &success()).unwrap();
    journal
        .record_checkpoint(&root.summary(journal, &[success()], &[], 210))
        .unwrap();
}
pub(super) fn verify_crash_recovery(journal: &Journal) {
    let root = Fixture::root(&[1]);
    assert!(
        journal
            .scope_observation(Number(0))
            .unwrap()
            .unwrap()
            .membership_verified
    );
    assert_eq!(
        journal
            .scope_members(Number(0), Number(0), PageLimit(1))
            .unwrap()[0]
            .work,
        work()
    );
    assert_eq!(
        journal.covered_scope(Number(0)).unwrap(),
        Some(root.summary(journal, &[success()], &[], 210))
    );
    assert!(matches!(
        journal.root_completion(Id(10)).unwrap(),
        Control::Drain(Drain::Complete {
            generation: Id(7),
            ..
        })
    ));
    assert_eq!(
        journal.unresolved(Number(0), PageLimit(256)).unwrap().len(),
        1
    );
}

struct Fixture {
    scope: Number,
    producer: Producer,
    parent: Option<WorkKey>,
    ids: Vec<Id>,
}
impl Fixture {
    fn root(ids: &[u64]) -> Self {
        Self {
            scope: Number(0),
            producer: Producer(0),
            parent: None,
            ids: ids.iter().copied().map(Id).collect(),
        }
    }
    fn seal(&self, journal: &Journal) -> Digest {
        scope_seal(
            &journal.identity().unwrap(),
            self.scope,
            self.producer,
            self.parent.as_ref(),
            Number(self.ids.len() as u64),
            self.ids.iter().copied(),
        )
        .unwrap()
    }
    fn page(
        &self,
        journal: &Journal,
        after: u64,
        ids: &[Id],
        state: State,
        sealed: bool,
        more: bool,
    ) -> (Control, Control) {
        (
            Control::Scope(Scope::Page {
                request: Id(1),
                scope: self.scope,
                after_entity: Number(after),
                limit: PageLimit(256),
            }),
            Control::Scope(Scope::PageResponse {
                request: Id(1),
                scope: self.scope,
                producer: self.producer,
                parent: self.parent.clone(),
                sealed,
                seal: sealed.then(|| self.seal(journal)),
                declared: Number(self.ids.len() as u64),
                entries: ids
                    .iter()
                    .map(|id| ScopeEntry { entity: *id, state })
                    .collect(),
                more,
            }),
        )
    }
    fn observe(&self, journal: &Journal, state: State) -> ScopeObservation {
        let (request, response) = self.page(journal, 0, &self.ids, state, true, false);
        journal.observe_scope_page(&request, &response).unwrap()
    }
    fn summary(
        &self,
        journal: &Journal,
        views: &[WorkView],
        children: &[ScopeSummary],
        closed: u64,
    ) -> ScopeSummary {
        let mut counts = Counts {
            success: Number(0),
            failure: Number(0),
            cancelled: Number(0),
            skipped: Number(0),
        };
        let mut root = StatusRoot::default();
        for view in views {
            let count = match view.state {
                State::SUCCEEDED => &mut counts.success,
                State::FAILED => &mut counts.failure,
                State::CANCELLED => &mut counts.cancelled,
                State::SKIPPED => &mut counts.skipped,
                _ => panic!("nonterminal fixture"),
            };
            count.0 += 1;
            root.push(
                StatusLeaf {
                    work: view.work.clone(),
                    state: view.state,
                    attempt: view.attempt,
                    manifest_digest: view
                        .manifest
                        .as_ref()
                        .map(Manifest::digest)
                        .transpose()
                        .unwrap(),
                    child_status_root: view.child.as_ref().map(|child| {
                        children
                            .iter()
                            .find(|s| s.scope.0 == child.scope.0)
                            .unwrap()
                            .status_root
                    }),
                }
                .digest()
                .unwrap(),
            )
            .unwrap();
        }
        ScopeSummary {
            scope: self.scope,
            producer: self.producer,
            parent: self.parent.clone(),
            seal: self.seal(journal),
            declared: Number(self.ids.len() as u64),
            counts,
            status_root: root.finish(),
            closed_at: Number(closed),
        }
    }
}
fn cancelled_view(entity: u64) -> WorkView {
    WorkView {
        work: WorkKey {
            entity: Id(entity),
            ..work()
        },
        state: State::CANCELLED,
        terminal_at: Some(Number(200)),
        receipt_until: Some(Number(180200)),
        ..declared_view()
    }
}
fn branch_views() -> (WorkView, WorkView, Fixture, Fixture) {
    let mut branch = success();
    branch.child = Some(ChildScope {
        scope: Id(1),
        producer: Producer(1),
    });
    branch.terminal_at = Some(Number(250));
    branch.receipt_until = Some(Number(180250));
    branch.output_until = Some(Number(120250));
    branch.manifest.as_mut().unwrap().committed_at = Number(250);
    branch.manifest.as_mut().unwrap().available_until = Number(120250);
    let mut child = success();
    child.work = WorkKey {
        scope: Number(1),
        producer: Producer(1),
        entity: Id(5),
    };
    child.manifest.as_mut().unwrap().work = child.work.clone();
    child.manifest.as_mut().unwrap().outputs[0].locator.0 = "pipestream://untrusted-hint.invalid:7443/v2/sessions/7/scopes/1/producers/1/entities/5/attempts/1/outputs/0".into();
    let nested = Fixture {
        scope: Number(1),
        producer: Producer(1),
        parent: Some(work()),
        ids: vec![Id(5)],
    };
    (branch, child, Fixture::root(&[1]), nested)
}

#[test]
fn empty_page_is_not_coverage_but_verified_empty_seal_can_close_and_reopen() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("client.sqlite");
    let journal = bound(&path, 16);
    let root = Fixture::root(&[]);
    let (request, response) = root.page(&journal, 0, &[], State::DECLARED, false, false);
    assert!(
        !journal
            .observe_scope_page(&request, &response)
            .unwrap()
            .membership_verified
    );
    let summary = root.summary(&journal, &[], &[], 100);
    code(journal.record_checkpoint(&summary), ErrorCode::NotReady);
    refusal(journal.root_completion(Id(2)), ErrorCode::NotReady);
    assert!(root.observe(&journal, State::DECLARED).membership_verified);
    journal.record_checkpoint(&summary).unwrap();
    drop(journal);
    let journal = reopen(&path);
    assert_eq!(
        journal.covered_scope(Number(0)).unwrap(),
        Some(summary.clone())
    );
    assert_eq!(
        journal.root_completion(Id(9)).unwrap(),
        Control::Drain(Drain::Complete {
            request: Id(9),
            generation: Id(7),
            root_summary: summary
        })
    );
}

#[test]
fn out_of_order_overlapping_pages_merge_without_claiming_missing_members_or_views() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("client.sqlite");
    let journal = bound(&path, 16);
    let root = Fixture::root(&(1..=300).collect::<Vec<_>>());
    let (request, response) = root.page(
        &journal,
        256,
        &root.ids[256..],
        State::CANCELLED,
        true,
        false,
    );
    assert!(
        !journal
            .observe_scope_page(&request, &response)
            .unwrap()
            .membership_verified
    );
    drop(journal);
    let journal = reopen(&path);
    let (request, response) =
        root.page(&journal, 0, &root.ids[..256], State::CANCELLED, true, true);
    assert!(
        journal
            .observe_scope_page(&request, &response)
            .unwrap()
            .membership_verified
    );
    let (request, response) = root.page(
        &journal,
        200,
        &root.ids[200..],
        State::DECLARED,
        true,
        false,
    );
    assert!(
        journal
            .observe_scope_page(&request, &response)
            .unwrap()
            .membership_verified
    );
    let members = journal
        .scope_members(Number(0), Number(200), PageLimit(256))
        .unwrap();
    assert_eq!(members.len(), 100);
    assert!(members.iter().all(|m| m.terminal == Some(State::CANCELLED)));
    let views: Vec<_> = (1..=300).map(cancelled_view).collect();
    let summary = root.summary(&journal, &views, &[], 210);
    code(journal.record_checkpoint(&summary), ErrorCode::NotReady);
    for view in &views {
        journal.observe_work(Id(2), view).unwrap();
    }
    journal.record_checkpoint(&summary).unwrap();
    drop(journal);
    let journal = reopen(&path);
    assert_eq!(journal.covered_scope(Number(0)).unwrap(), Some(summary));
}

#[test]
fn root_coverage_waits_for_verified_descendant_and_original_parent_allocation() {
    for child_first in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("client.sqlite");
        let journal = bound(&path, 16);
        let (branch, child, root, nested) = branch_views();
        if child_first {
            nested.observe(&journal, State::SUCCEEDED);
            assert!(journal.scope_observation(Number(0)).unwrap().is_none());
            refusal(journal.root_completion(Id(3)), ErrorCode::NotReady);
            // Receiving a child does not invent the parent's admission. The pending
            // relationship must survive reopen and become checkable later.
            assert!(journal.observed_work(&branch.work).unwrap().is_none());
        }
        drop(journal);
        let journal = reopen(&path);
        root.observe(&journal, State::SUCCEEDED);
        journal.observe_work(Id(4), &branch).unwrap();
        let child_summary = nested.summary(&journal, std::slice::from_ref(&child), &[], 210);
        let root_summary = root.summary(
            &journal,
            std::slice::from_ref(&branch),
            std::slice::from_ref(&child_summary),
            260,
        );
        code(
            journal.record_checkpoint(&root_summary),
            ErrorCode::NotReady,
        );
        if !child_first {
            nested.observe(&journal, State::SUCCEEDED);
        }
        journal.observe_work(Id(3), &child).unwrap();
        code(
            journal.record_checkpoint(&root_summary),
            ErrorCode::NotReady,
        );
        journal.record_checkpoint(&child_summary).unwrap();
        refusal(journal.root_completion(Id(3)), ErrorCode::NotReady);
        journal.record_checkpoint(&root_summary).unwrap();
        drop(journal);
        let journal = reopen(&path);
        assert_eq!(
            journal.covered_scope(Number(1)).unwrap(),
            Some(child_summary)
        );
        assert_eq!(
            journal.covered_scope(Number(0)).unwrap(),
            Some(root_summary)
        );
    }
}

#[test]
fn strict_parent_success_refuses_failed_child_but_failed_parent_still_waits_for_children() {
    for parent_failed in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let journal = bound(&directory.path().join("client.sqlite"), 16);
        let (mut branch, mut child, root, nested) = branch_views();
        if parent_failed {
            branch.state = State::FAILED;
            branch.terminal_at = Some(Number(150));
            branch.receipt_until = Some(Number(180150));
            branch.output_until = None;
            branch.manifest = None;
            branch.diagnostic = Some(Diagnostic {
                code: DiagnosticCode(1),
                detail: Detail("failed before child closure".into()),
            });
        } else {
            child.state = State::FAILED;
            child.output_until = None;
            child.manifest = None;
            child.diagnostic = Some(Diagnostic {
                code: DiagnosticCode(1),
                detail: Detail("child failed".into()),
            });
        }
        root.observe(&journal, branch.state);
        journal.observe_work(Id(4), &branch).unwrap();
        nested.observe(&journal, child.state);
        journal.observe_work(Id(3), &child).unwrap();
        let child_summary = nested.summary(&journal, &[child], &[], 210);
        let root_summary = root.summary(
            &journal,
            &[branch],
            std::slice::from_ref(&child_summary),
            260,
        );
        code(
            journal.record_checkpoint(&root_summary),
            ErrorCode::NotReady,
        );
        journal.record_checkpoint(&child_summary).unwrap();
        if parent_failed {
            journal.record_checkpoint(&root_summary).unwrap();
        } else {
            code(
                journal.record_checkpoint(&root_summary),
                ErrorCode::IntegrityError,
            );
        }
    }
}
