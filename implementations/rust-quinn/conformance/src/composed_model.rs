//! Bounded composition of attempts, subtree fences, closure and output pins.
//!
//! One declared branch can allocate one scope containing zero or one leaf.
//! Both work items have at most two attempts and one symbolic output per attempt.
//! Two durable worker epochs allow one restart; stale callbacks retain epoch 1.
//! Metadata operations are atomic; publication has a separately staged commit.
//! Existing models cover request replay, clocks and larger membership sets.
//! This model does not substitute for real storage or wire failure tests.

use super::work_model::{Counterexample, Report, explore_states};
use anyhow::{Result, bail, ensure};

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
enum Outcome {
    Success,
    Failed,
    Cancelled,
    Skipped,
}

impl Outcome {
    fn bucket(self) -> usize {
        match self {
            Self::Success => 0,
            Self::Failed => 1,
            Self::Cancelled => 2,
            Self::Skipped => 3,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
enum Phase {
    #[default]
    Absent,
    Declared,
    Active,
    Terminal(Outcome),
}

impl Phase {
    fn terminal(self) -> bool {
        matches!(self, Self::Terminal(_))
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
struct Work {
    phase: Phase,
    attempt: u8,
    stop: Option<Outcome>,
    files: u8,
    output_expired: bool,
    reading: bool,
}

#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
struct Summary {
    declared: u8,
    counts: [u8; 4],
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
struct State {
    work: [Work; 2],
    sealed: [bool; 2],
    closed: [Option<Summary>; 2],
    root_fence: bool,
    revoked: bool,
    pending: Option<(usize, u8, u8)>,
    worker_epoch: u8,
    online: bool,
    connected: bool,
    drained: bool,
}

impl Default for State {
    fn default() -> Self {
        Self {
            work: [
                Work {
                    phase: Phase::Declared,
                    ..Work::default()
                },
                Work::default(),
            ],
            sealed: [false; 2],
            closed: [None; 2],
            root_fence: false,
            revoked: false,
            pending: None,
            worker_epoch: 1,
            online: true,
            connected: true,
            drained: false,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Action {
    Admit(usize),
    DeclareChild,
    Seal(usize),
    Retry(usize),
    Install(usize, u8),
    BeginPublish(usize, u8, u8),
    Commit,
    Fail(usize),
    Fence(usize, Outcome),
    CancelRoot,
    Settle(usize),
    Revoke,
    Close(usize),
    ExpireOutput(usize),
    BeginRead(usize, bool),
    EndRead(usize),
    Reclaim(usize),
    Crash,
    Restart,
    Disconnect,
    Connect,
    Drain,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Fault {
    None,
    IgnoresAncestorFence,
    PublishesBeforeChildren,
    DropsDependencyPin,
    DropsReadPin,
    RetryReplacesSubtree,
    StalePublication,
    StaleWorkerLease,
    CrashLosesFence,
    SettlesParentTooEarly,
    ClosesBeforeDescendants,
    RevocationLeavesDeclarations,
}

const NEGATIVE_CONTROLS: [Fault; 11] = [
    Fault::IgnoresAncestorFence,
    Fault::PublishesBeforeChildren,
    Fault::DropsDependencyPin,
    Fault::DropsReadPin,
    Fault::RetryReplacesSubtree,
    Fault::StalePublication,
    Fault::StaleWorkerLease,
    Fault::CrashLosesFence,
    Fault::SettlesParentTooEarly,
    Fault::ClosesBeforeDescendants,
    Fault::RevocationLeavesDeclarations,
];

impl State {
    fn child_exists(self) -> bool {
        self.work[0].attempt > 0
    }

    fn authorized(self) -> bool {
        self.online && self.connected && !self.revoked
    }

    fn stop(self, id: usize) -> Option<Outcome> {
        self.work[id].stop.or_else(|| {
            if self.root_fence || (id == 1 && self.work[0].stop.is_some()) {
                Some(Outcome::Cancelled)
            } else {
                None
            }
        })
    }

    fn descendants_closed(self) -> bool {
        !self.child_exists() || self.closed[1].is_some()
    }

    fn strict_children(self) -> bool {
        self.closed[1].is_some()
            && matches!(
                self.work[1].phase,
                Phase::Absent | Phase::Terminal(Outcome::Success)
            )
    }

    fn summary(self, scope: usize) -> Summary {
        let mut summary = Summary::default();
        if self.work[scope].phase != Phase::Absent {
            summary.declared = 1;
        }
        if let Phase::Terminal(outcome) = self.work[scope].phase {
            summary.counts[outcome.bucket()] = 1;
        }
        summary
    }

    fn dependency_pin(self, id: usize) -> bool {
        id == 1 && self.child_exists() && !self.work[0].phase.terminal()
    }

    fn cancel_root(&mut self) {
        self.root_fence = true;
        self.sealed[0] = true;
        if self.child_exists() {
            self.sealed[1] = true;
        }
        if !self.work[0].phase.terminal() && self.work[0].stop.is_none() {
            self.work[0].stop = Some(Outcome::Cancelled);
        }
    }

    fn step(mut self, action: Action, fault: Fault) -> Self {
        if !self.online {
            if action == Action::Restart && self.worker_epoch < 2 {
                self.worker_epoch += 1;
                self.online = true;
            }
            return self;
        }
        match action {
            Action::Admit(id)
                if self.authorized()
                    && self.work[id].phase == Phase::Declared
                    && self.stop(id).is_none() =>
            {
                self.work[id].phase = Phase::Active;
                self.work[id].attempt = 1;
            }
            Action::DeclareChild
                if self.authorized()
                    && self.child_exists()
                    && !self.sealed[1]
                    && self.work[1].phase == Phase::Absent
                    && self.stop(1).is_none() =>
            {
                self.work[1].phase = Phase::Declared;
            }
            Action::Seal(scope) if self.authorized() && (scope == 0 || self.child_exists()) => {
                self.sealed[scope] = true;
            }
            Action::Retry(id)
                if self.authorized()
                    && self.work[id].phase == Phase::Active
                    && self.work[id].attempt == 1
                    && self.stop(id).is_none() =>
            {
                self.work[id].attempt = 2;
                if id == 0 && fault == Fault::RetryReplacesSubtree {
                    self.work[1] = Work::default();
                    self.sealed[1] = false;
                    self.closed[1] = None;
                }
            }
            Action::Install(id, attempt)
                if self.work[id].phase == Phase::Active
                    && self.work[id].attempt == attempt
                    && self.stop(id).is_none()
                    && !self.revoked =>
            {
                self.work[id].files |= 1 << attempt;
            }
            Action::BeginPublish(id, attempt, worker_epoch) if self.pending.is_none() => {
                // A buggy/late application can request publication at any point.
                // The authoritative commit must independently reject it.
                self.pending = Some((id, attempt, worker_epoch));
            }
            Action::Commit => {
                if let Some((id, attempt, worker_epoch)) = self.pending.take() {
                    let fence = if id == 1 && fault == Fault::IgnoresAncestorFence {
                        self.work[id].stop
                    } else {
                        self.stop(id)
                    };
                    if self.work[id].phase == Phase::Active
                        && (worker_epoch == self.worker_epoch
                            || (fault == Fault::StaleWorkerLease
                                && worker_epoch < self.worker_epoch))
                        && (self.work[id].attempt == attempt || fault == Fault::StalePublication)
                        && self.work[id].files & (1 << attempt) != 0
                        && fence.is_none()
                        && !self.revoked
                        && (id == 1
                            || self.strict_children()
                            || fault == Fault::PublishesBeforeChildren)
                    {
                        self.work[id].phase = Phase::Terminal(Outcome::Success);
                    }
                }
            }
            Action::Fail(id) if self.work[id].phase == Phase::Active && self.stop(id).is_none() => {
                // Failure/deadline settlement does not erase a child scope.
                self.work[id].phase = Phase::Terminal(Outcome::Failed);
            }
            Action::Fence(id, outcome)
                if self.authorized()
                    && self.work[id].phase != Phase::Absent
                    && !self.work[id].phase.terminal()
                    && self.stop(id).is_none() =>
            {
                self.work[id].stop = Some(outcome);
                if id == 0 && self.child_exists() {
                    self.sealed[1] = true;
                }
            }
            Action::CancelRoot if self.authorized() => self.cancel_root(),
            Action::Settle(id)
                if self.work[id].phase != Phase::Absent
                    && !self.work[id].phase.terminal()
                    && (id == 1
                        || self.descendants_closed()
                        || fault == Fault::SettlesParentTooEarly) =>
            {
                if let Some(outcome) = self.stop(id) {
                    self.work[id].phase = Phase::Terminal(outcome);
                }
            }
            Action::Revoke => {
                self.revoked = true;
                if fault != Fault::RevocationLeavesDeclarations || self.child_exists() {
                    self.cancel_root();
                }
            }
            Action::Close(scope)
                if (scope == 0 || self.child_exists())
                    && self.sealed[scope]
                    && (self.work[scope].phase == Phase::Absent
                        || self.work[scope].phase.terminal())
                    && (scope == 1
                        || self.descendants_closed()
                        || fault == Fault::ClosesBeforeDescendants) =>
            {
                self.closed[scope] = Some(self.summary(scope));
            }
            Action::ExpireOutput(id)
                if self.work[id].phase == Phase::Terminal(Outcome::Success) =>
            {
                self.work[id].output_expired = true;
            }
            Action::BeginRead(id, owner)
                if owner
                    && self.authorized()
                    && !self.work[id].output_expired
                    && self.work[id].phase == Phase::Terminal(Outcome::Success) =>
            {
                self.work[id].reading = true;
            }
            Action::EndRead(id) => self.work[id].reading = false,
            Action::Reclaim(id) if self.work[id].phase.terminal() => {
                let promised = self.work[id].phase == Phase::Terminal(Outcome::Success)
                    && (!self.work[id].output_expired
                        || (self.work[id].reading && fault != Fault::DropsReadPin)
                        || (self.dependency_pin(id) && fault != Fault::DropsDependencyPin));
                self.work[id].files &= if promised {
                    1 << self.work[id].attempt
                } else {
                    0
                };
            }
            Action::Crash => {
                self.online = false;
                self.connected = false;
                self.pending = None;
                for work in &mut self.work {
                    work.reading = false;
                    if fault == Fault::CrashLosesFence {
                        work.stop = None;
                    }
                }
            }
            Action::Disconnect => {
                self.connected = false;
                for work in &mut self.work {
                    work.reading = false;
                }
            }
            Action::Connect => self.connected = true,
            Action::Drain
                if self.authorized()
                    && self.closed[0].is_some()
                    && self.pending.is_none()
                    && self.work.iter().all(|work| !work.reading) =>
            {
                self.drained = true;
            }
            _ => {}
        }
        self
    }
}

fn invariant(before: State, action: Action, after: State) -> Result<()> {
    for id in 0..2 {
        let old = before.work[id];
        let work = after.work[id];
        ensure!(
            work.attempt >= old.attempt,
            "attempt or child identity was recycled"
        );
        if old.phase != Phase::Absent {
            ensure!(
                work.phase != Phase::Absent,
                "declared descendant disappeared"
            );
        }
        if old.phase.terminal() {
            ensure!(
                work.phase == old.phase && work.attempt == old.attempt,
                "terminal work outcome or producing attempt changed"
            );
        }
        if old.stop.is_some() {
            ensure!(
                work.stop == old.stop,
                "accepted cancellation/skip fence disappeared or changed"
            );
        }
        if work.phase == Phase::Terminal(Outcome::Success) {
            if !work.output_expired || work.reading || after.dependency_pin(id) {
                ensure!(
                    work.files & (1 << work.attempt) != 0,
                    "promised output lost despite retention, read or dependency pin"
                );
            }
            if old.phase != work.phase {
                ensure!(
                    before.pending == Some((id, work.attempt, before.worker_epoch)),
                    "stale attempt or worker lease published output"
                );
                ensure!(
                    before.stop(id).is_none() && !before.revoked,
                    "publication crossed an accepted ancestor or owner fence"
                );
                ensure!(
                    old.files & (1 << work.attempt) != 0,
                    "success references uninstalled output"
                );
                if id == 0 {
                    ensure!(
                        before.strict_children(),
                        "parent published without successful sealed child closure"
                    );
                }
            }
        }
        if id == 0
            && !old.phase.terminal()
            && matches!(
                work.phase,
                Phase::Terminal(Outcome::Cancelled | Outcome::Skipped)
            )
        {
            ensure!(
                after.descendants_closed(),
                "parent cancellation settled before descendant closure"
            );
        }
        if before.sealed[id] {
            ensure!(
                after.sealed[id] && before.summary(id).declared == after.summary(id).declared,
                "sealed membership changed"
            );
        }
        if let Some(summary) = before.closed[id] {
            ensure!(
                after.closed[id] == Some(summary),
                "committed closure summary changed"
            );
        }
        if let Some(summary) = after.closed[id] {
            ensure!(
                after.sealed[id],
                "unsealed scope acquired a completion summary"
            );
            ensure!(
                summary == after.summary(id)
                    && summary.counts.iter().sum::<u8>() == summary.declared,
                "closure omitted a declared obligation or final count bucket"
            );
            ensure!(
                id == 1 || after.descendants_closed(),
                "root closed before its descendant scope"
            );
        }
    }
    ensure!(
        !after.revoked || after.root_fence,
        "revocation left unresolved declarations unfenced"
    );
    if matches!(action, Action::Retry(0)) && after.work[0].attempt > before.work[0].attempt {
        ensure!(
            after.work[1] == before.work[1]
                && after.sealed[1] == before.sealed[1]
                && after.closed[1] == before.closed[1],
            "parent retry replaced its immutable descendant scope"
        );
    }
    if action == Action::Crash || action == Action::Disconnect {
        let mut retained = before;
        retained.connected = false;
        for work in &mut retained.work {
            work.reading = false;
        }
        if action == Action::Crash {
            retained.online = false;
            retained.pending = None;
        }
        ensure!(
            retained == after,
            "transport loss changed durable lifecycle state"
        );
    }
    if matches!(action, Action::BeginRead(_, false))
        || (matches!(action, Action::BeginRead(_, _)) && before.revoked)
    {
        ensure!(
            after == before,
            "unauthorized result access acquired a read lease"
        );
    }
    if !before.drained && after.drained {
        ensure!(
            before.closed[0].is_some() && before.descendants_closed(),
            "drain used an incomplete root cut"
        );
    }
    Ok(())
}

fn actions() -> Vec<Action> {
    let mut actions = vec![
        Action::DeclareChild,
        Action::CancelRoot,
        Action::Commit,
        Action::Revoke,
        Action::Crash,
        Action::Restart,
        Action::Disconnect,
        Action::Connect,
        Action::Drain,
    ];
    for id in 0..2 {
        actions.extend([
            Action::Admit(id),
            Action::Seal(id),
            Action::Retry(id),
            Action::Fail(id),
            Action::Fence(id, Outcome::Cancelled),
            Action::Fence(id, Outcome::Skipped),
            Action::Settle(id),
            Action::Close(id),
            Action::ExpireOutput(id),
            Action::BeginRead(id, true),
            Action::BeginRead(id, false),
            Action::EndRead(id),
            Action::Reclaim(id),
        ]);
        for attempt in 1..=2 {
            actions.push(Action::Install(id, attempt));
            for worker_epoch in 1..=2 {
                actions.push(Action::BeginPublish(id, attempt, worker_epoch));
            }
        }
    }
    actions
}

fn explore(
    depth: usize,
    max_states: usize,
    fault: Fault,
) -> Result<Result<Report, Counterexample<Action>>> {
    explore_states(
        State::default(),
        &actions(),
        depth,
        max_states,
        |state, action| state.step(action, fault),
        invariant,
    )
}

pub fn run(depth: usize, max_states: usize) -> Result<()> {
    println!(
        "composed model bounds: one branch, zero/one descendant, two attempts each, two worker epochs (one restart), symbolic output expiry/read pins; depth {depth}"
    );
    let report = match explore(depth, max_states, Fault::None)? {
        Ok(report) => report,
        Err(error) => bail!(
            "composed invariant failed: {}; trace: {:?}",
            error.reason,
            error.trace
        ),
    };
    println!(
        "composed safety passed: {} states, {} checked edges, deepest {}, {} states at depth boundary",
        report.states, report.edges, report.deepest, report.frontier
    );
    for fault in NEGATIVE_CONTROLS {
        let counterexample = match explore(depth, max_states, fault)? {
            Err(error) => error,
            Ok(_) => bail!("INCONCLUSIVE: composed negative control {fault:?} was not detected"),
        };
        println!(
            "composed negative control {fault:?}: {}; shortest trace: {:?}",
            counterexample.reason, counterexample.trace
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn execute(actions: &[Action]) -> State {
        let mut state = State::default();
        for action in actions {
            let next = state.step(*action, Fault::None);
            invariant(state, *action, next).unwrap();
            state = next;
        }
        state
    }

    #[test]
    fn bounded_composition_preserves_invariants() {
        let report = explore(8, 1_000_000, Fault::None).unwrap().unwrap();
        assert!(report.states > 1_000);
        assert_eq!(report.deepest, 8);
    }

    #[test]
    fn cancelled_branch_fences_delayed_child_before_batched_settlement() {
        let state = execute(&[
            Action::Admit(0),
            Action::DeclareChild,
            Action::Admit(1),
            Action::Install(1, 1),
            Action::BeginPublish(1, 1, 1),
            Action::Fence(0, Outcome::Skipped),
            Action::Disconnect,
            Action::Commit,
            Action::Crash,
            Action::Restart,
            Action::Settle(0),
            Action::Settle(1),
            Action::Close(1),
            Action::Settle(0),
            Action::Connect,
            Action::Seal(0),
            Action::Close(0),
            Action::Drain,
        ]);
        assert_eq!(state.work[0].phase, Phase::Terminal(Outcome::Skipped));
        assert_eq!(state.work[1].phase, Phase::Terminal(Outcome::Cancelled));
        assert!(state.drained);
    }

    #[test]
    fn child_output_outlives_external_expiry_until_parent_and_read_settle() {
        let state = execute(&[
            Action::Admit(0),
            Action::DeclareChild,
            Action::Admit(1),
            Action::Seal(1),
            Action::Install(1, 1),
            Action::BeginPublish(1, 1, 1),
            Action::Commit,
            Action::Close(1),
            Action::BeginRead(1, true),
            Action::ExpireOutput(1),
            Action::Reclaim(1),
            Action::Retry(0),
            Action::Install(0, 2),
            Action::BeginPublish(0, 2, 1),
            Action::Commit,
            Action::Reclaim(1),
        ]);
        assert_eq!(state.work[0].phase, Phase::Terminal(Outcome::Success));
        assert_eq!(state.work[0].attempt, 2);
        assert_ne!(
            state.work[1].files, 0,
            "the existing read still pins output"
        );
        let released = state
            .step(Action::EndRead(1), Fault::None)
            .step(Action::Reclaim(1), Fault::None);
        assert_eq!(released.work[1].files, 0);
        assert_eq!(released.work[1].phase, Phase::Terminal(Outcome::Success));
    }

    #[test]
    fn parent_failure_does_not_erase_missing_child_obligation() {
        let state = execute(&[
            Action::Admit(0),
            Action::DeclareChild,
            Action::Seal(0),
            Action::Seal(1),
            Action::Fail(0),
            Action::Close(0),
            Action::Drain,
        ]);
        assert!(state.closed[0].is_none());
        assert!(!state.drained);
        assert_eq!(state.work[1].phase, Phase::Declared);
    }

    #[test]
    fn revocation_settles_even_an_unadmitted_declared_root() {
        let state = execute(&[
            Action::Revoke,
            Action::Crash,
            Action::Restart,
            Action::Settle(0),
            Action::Close(0),
        ]);
        assert_eq!(state.work[0].phase, Phase::Terminal(Outcome::Cancelled));
        assert!(!state.child_exists());
        assert_eq!(state.closed[0].unwrap().counts, [0, 0, 1, 0]);
    }

    #[test]
    fn model_budget_failure_is_not_a_proof() {
        assert!(explore(32, 1, Fault::None).is_err());
    }

    #[test]
    fn restart_fences_old_worker_without_changing_logical_attempt() {
        let state = execute(&[
            Action::Admit(0),
            Action::Seal(1),
            Action::Close(1),
            Action::Install(0, 1),
            Action::BeginPublish(0, 1, 1),
            Action::Crash,
            Action::Restart,
            Action::BeginPublish(0, 1, 1),
            Action::Commit,
        ]);
        assert_eq!(state.work[0].phase, Phase::Active);
        assert_eq!(state.work[0].attempt, 1);
        assert_eq!(state.worker_epoch, 2);
        let resumed = state.step(Action::BeginPublish(0, 1, 2), Fault::None);
        let committed = resumed.step(Action::Commit, Fault::None);
        invariant(resumed, Action::Commit, committed).unwrap();
        assert_eq!(committed.work[0].phase, Phase::Terminal(Outcome::Success));
        assert_eq!(committed.work[0].attempt, 1);
    }

    #[test]
    fn every_composed_negative_control_has_a_short_counterexample() {
        for fault in NEGATIVE_CONTROLS {
            let counterexample = explore(12, 1_000_000, fault).unwrap().unwrap_err();
            assert!(!counterexample.trace.is_empty(), "{fault:?}");
        }
    }
}
