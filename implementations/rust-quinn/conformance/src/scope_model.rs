//! Independent closure model: two root entities and two possible descendants.
//! Membership commitments are symbolic, not encoded frames or Merkle hashes.

use super::work_model::{Counterexample, Report, explore_states};
use anyhow::{Result, bail, ensure};

#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
enum Entity {
    #[default]
    Undeclared,
    Declared,
    Admitted,
    Success,
    Failed,
    Cancelled,
    Skipped,
}

impl Entity {
    fn bucket(self) -> Option<usize> {
        match self {
            Self::Success => Some(0),
            Self::Failed => Some(1),
            Self::Cancelled => Some(2),
            Self::Skipped => Some(3),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
struct Summary {
    members: u8,
    counts: [u8; 4],
}

#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
struct State {
    entities: [Entity; 4],
    child_exists: bool,
    sealed: [bool; 2],
    closed: [Option<Summary>; 2],
    drain: Option<(u8, Summary)>,
}

#[derive(Clone, Copy, Debug)]
enum Action {
    Declare(usize),
    Admit(usize),
    Complete(usize),
    Fail(usize),
    Cancel(usize),
    Skip(usize),
    Seal(usize),
    Close(usize),
    Goaway(usize),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Fault {
    None,
    IgnoresSeal,
    IgnoresMissing,
    IgnoresDescendants,
    OmitsSkipped,
    WrongShutdownScope,
    ReopensSeal,
}

const NEGATIVE_CONTROLS: [Fault; 6] = [
    Fault::IgnoresSeal,
    Fault::IgnoresMissing,
    Fault::IgnoresDescendants,
    Fault::OmitsSkipped,
    Fault::WrongShutdownScope,
    Fault::ReopensSeal,
];

fn range(scope: usize) -> std::ops::Range<usize> {
    scope * 2..scope * 2 + 2
}

impl State {
    fn summary(self, scope: usize) -> Summary {
        let mut summary = Summary::default();
        for id in range(scope) {
            if self.entities[id] != Entity::Undeclared {
                summary.members |= 1 << id;
            }
            if let Some(bucket) = self.entities[id].bucket() {
                summary.counts[bucket] += 1;
            }
        }
        summary
    }

    fn ready(self, scope: usize, fault: Fault) -> bool {
        (scope == 0 || self.child_exists)
            && (self.sealed[scope] || fault == Fault::IgnoresSeal)
            && range(scope).all(|id| {
                self.entities[id] == Entity::Undeclared
                    || self.entities[id].bucket().is_some()
                    || (fault == Fault::IgnoresMissing && self.entities[id] == Entity::Declared)
            })
            && (scope == 1
                || !self.child_exists
                || self.closed[1].is_some()
                || fault == Fault::IgnoresDescendants)
    }

    fn step(mut self, action: Action, fault: Fault) -> Self {
        match action {
            Action::Declare(id)
                if self.entities[id] == Entity::Undeclared
                    && (!self.sealed[id / 2] || fault == Fault::ReopensSeal)
                    && (id < 2 || self.child_exists) =>
            {
                self.entities[id] = Entity::Declared;
            }
            Action::Admit(id) if self.entities[id] == Entity::Declared => {
                self.entities[id] = Entity::Admitted;
                // Root entity 0 has immutable application type "branch".
                if id == 0 {
                    self.child_exists = true;
                }
            }
            Action::Complete(id)
                if self.entities[id] == Entity::Admitted
                    && (id != 0
                        || (self.closed[1].is_some()
                            && self.summary(1).counts[1..].iter().all(|count| *count == 0))) =>
            {
                self.entities[id] = Entity::Success;
            }
            Action::Fail(id) if self.entities[id] == Entity::Admitted => {
                // A failed parent cannot make outstanding descendants vanish.
                self.entities[id] = Entity::Failed;
            }
            Action::Cancel(id) | Action::Skip(id)
                if matches!(self.entities[id], Entity::Declared | Entity::Admitted) =>
            {
                self.entities[id] = if matches!(action, Action::Skip(_)) {
                    Entity::Skipped
                } else {
                    Entity::Cancelled
                };
                if id == 0 && self.child_exists {
                    // Authoritative subtree cancellation freezes membership and
                    // settles existing nonterminal descendants atomically.
                    self.sealed[1] = true;
                    for child in range(1) {
                        if matches!(self.entities[child], Entity::Declared | Entity::Admitted) {
                            self.entities[child] = Entity::Cancelled;
                        }
                    }
                }
            }
            Action::Seal(scope) if scope == 0 || self.child_exists => self.sealed[scope] = true,
            Action::Close(scope) if self.ready(scope, fault) => {
                let mut summary = self.summary(scope);
                if fault == Fault::OmitsSkipped {
                    summary.counts[3] = 0;
                }
                self.closed[scope] = Some(summary);
            }
            Action::Goaway(scope) if scope == 0 || fault == Fault::WrongShutdownScope => {
                if let Some(summary) = self.closed[scope] {
                    self.drain = Some((scope as u8, summary));
                }
            }
            _ => {}
        }
        self
    }
}

fn invariant(before: State, _: Action, after: State) -> Result<()> {
    for scope in 0..2 {
        if before.sealed[scope] {
            ensure!(
                after.summary(scope).members == before.summary(scope).members,
                "a sealed scope's membership changed"
            );
        }
        if let Some(summary) = after.closed[scope] {
            ensure!(
                after.sealed[scope],
                "a scope closed without an immutable seal"
            );
            ensure!(
                range(scope).all(|id| after.entities[id] == Entity::Undeclared
                    || after.entities[id].bucket().is_some()),
                "a declared member was omitted from closure"
            );
            ensure!(
                scope != 0 || !after.child_exists || after.closed[1].is_some(),
                "a root closed before its descendant scope"
            );
            ensure!(
                summary == after.summary(scope),
                "completion counters omitted a final state"
            );
            ensure!(
                summary
                    .counts
                    .iter()
                    .map(|count| u32::from(*count))
                    .sum::<u32>()
                    == summary.members.count_ones(),
                "final counts do not partition the declared set"
            );
        }
        if before.closed[scope].is_some() {
            ensure!(
                before.closed[scope] == after.closed[scope],
                "a committed scope summary changed"
            );
        }
    }
    if let Some((scope, summary)) = after.drain {
        ensure!(scope == 0, "connection shutdown used a non-root scope cut");
        ensure!(
            after.closed[0] == Some(summary),
            "shutdown omitted the acknowledged root cut"
        );
    }
    Ok(())
}

fn explore(
    depth: usize,
    max_states: usize,
    fault: Fault,
) -> Result<Result<Report, Counterexample<Action>>> {
    let mut actions = Vec::new();
    for id in 0..4 {
        actions.extend([
            Action::Declare(id),
            Action::Admit(id),
            Action::Complete(id),
            Action::Fail(id),
            Action::Cancel(id),
            Action::Skip(id),
        ]);
    }
    for scope in 0..2 {
        actions.extend([
            Action::Seal(scope),
            Action::Close(scope),
            Action::Goaway(scope),
        ]);
    }
    explore_states(
        State::default(),
        &actions,
        depth,
        max_states,
        |state, action| state.step(action, fault),
        invariant,
    )
}

pub fn run(depth: usize, max_states: usize) -> Result<()> {
    println!(
        "scope model bounds: two root entities, one child scope with two entities, at most {depth} transitions; symbolic membership, four final-count buckets"
    );
    let report = match explore(depth, max_states, Fault::None)? {
        Ok(report) => report,
        Err(counterexample) => bail!(
            "scope invariant failed: {}; trace: {:?}",
            counterexample.reason,
            counterexample.trace
        ),
    };
    println!(
        "scope safety passed: {} states, {} checked edges, deepest {}, {} states at depth boundary",
        report.states, report.edges, report.deepest, report.frontier
    );
    for fault in NEGATIVE_CONTROLS {
        let counterexample = match explore(depth, max_states, fault)? {
            Err(counterexample) => counterexample,
            Ok(_) => bail!(
                "INCONCLUSIVE: scope negative control {fault:?} was not detected within depth {depth}"
            ),
        };
        println!(
            "scope negative control {fault:?}: {}; shortest trace: {:?}",
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
    fn finite_scope_graph_is_exhausted_not_only_sampled() {
        let report = explore(32, 100_000, Fault::None).unwrap().unwrap();
        assert_eq!(report.frontier, 0);
        assert!(report.states > 1_000);
    }

    #[test]
    fn incorrect_closure_counters_and_scope_cuts_are_detected() {
        for fault in NEGATIVE_CONTROLS {
            assert!(explore(16, 100_000, fault).unwrap().is_err(), "{fault:?}");
        }
    }

    #[test]
    fn missing_child_blocks_parent_even_after_root_seal() {
        let state = execute(&[
            Action::Declare(0),
            Action::Admit(0),
            Action::Declare(2),
            Action::Seal(0),
            Action::Seal(1),
            Action::Complete(0),
            Action::Close(1),
            Action::Close(0),
            Action::Goaway(0),
        ]);
        assert_eq!(state.entities[0], Entity::Admitted);
        assert_eq!(state.closed, [None, None]);
        assert_eq!(state.drain, None);
    }

    #[test]
    fn cancelled_unadmitted_child_is_counted_but_not_successful() {
        let state = execute(&[
            Action::Declare(0),
            Action::Admit(0),
            Action::Declare(2),
            Action::Cancel(2),
            Action::Seal(1),
            Action::Close(1),
            Action::Complete(0),
            Action::Fail(0),
            Action::Seal(0),
            Action::Close(0),
            Action::Goaway(0),
        ]);
        assert_eq!(state.closed[1].unwrap().counts, [0, 0, 1, 0]);
        assert_eq!(state.closed[0].unwrap().counts, [0, 1, 0, 0]);
        assert!(state.drain.is_some());
    }

    #[test]
    fn parent_cancellation_preserves_and_settles_descendant_membership() {
        let state = execute(&[
            Action::Declare(0),
            Action::Admit(0),
            Action::Declare(2),
            Action::Declare(3),
            Action::Admit(3),
            Action::Cancel(0),
            Action::Close(1),
            Action::Seal(0),
            Action::Close(0),
            Action::Goaway(0),
        ]);
        assert_eq!(state.closed[1].unwrap().members, 0b1100);
        assert_eq!(state.closed[1].unwrap().counts, [0, 0, 2, 0]);
        assert_eq!(state.closed[0].unwrap().counts, [0, 0, 1, 0]);
        assert!(state.drain.is_some());
    }
}
