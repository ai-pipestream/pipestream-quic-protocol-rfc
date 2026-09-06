//! Independent finite-state exploration, not an implementation or wire oracle.
//!
//! A metadata commit is atomic; output installation is a separate durable step.
//! Crashes erase transactions/replies, not committed state or installed files.
//! These assumptions must separately be tested against each real storage stack.

use anyhow::{Result, bail, ensure};
use std::collections::HashMap;

const LAST_TIME: u8 = 6;
const EXECUTION_INTERVAL: u8 = 3;
const REPLAY_INTERVAL: u8 = 2;

#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
enum Phase {
    #[default]
    Declared,
    Active,
    Success,
    Cancelled,
    Failed,
}

impl Phase {
    fn terminal(self) -> bool {
        matches!(self, Self::Success | Self::Cancelled | Self::Failed)
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
struct Work {
    phase: Phase,
    attempt: u8,
    input_retained: bool,
    result_attempt: Option<u8>,
    execution_deadline: Option<u8>,
    retain_until: Option<u8>,
    expired: bool,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
enum Reply {
    Pending(u8),
    Terminal(Phase, u8, u8),
    Expired,
    Unauthorized,
    Conflict,
    InvalidInput,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
enum Mutation {
    Admit { owner: bool, valid: bool },
    Retry { owner: bool, expected: u8 },
    Cancel { owner: bool },
    Publish { attempt: u8 },
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
enum Action {
    Begin(Mutation),
    Commit,
    InstallOutput(u8),
    Read { owner: bool },
    Deliver,
    Disconnect,
    Crash,
    Restart,
    Connect,
    Revoke,
    SettleRevoked,
    Tick,
    SettleDeadline,
    Expire,
    Reconcile,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
struct State {
    work: Work,
    pending: Option<Mutation>,
    reply: Option<Reply>,
    observed_terminal: Option<(Phase, u8, u8)>,
    output_files: u8,
    now: u8,
    revoked: bool,
    online: bool,
    connected: bool,
}

impl Default for State {
    fn default() -> Self {
        Self {
            work: Work::default(),
            pending: None,
            reply: None,
            observed_terminal: None,
            output_files: 0,
            now: 0,
            revoked: false,
            online: true,
            connected: true,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Fault {
    None,
    DisconnectFinalizes,
    StalePublishes,
    ExpiresActive,
    ReusesExpired,
    LeaksUnauthorized,
    DeletesPublished,
    RetryAfterDeadline,
    RetryExtendsDeadline,
}

const NEGATIVE_CONTROLS: [Fault; 8] = [
    Fault::DisconnectFinalizes,
    Fault::StalePublishes,
    Fault::ExpiresActive,
    Fault::ReusesExpired,
    Fault::LeaksUnauthorized,
    Fault::DeletesPublished,
    Fault::RetryAfterDeadline,
    Fault::RetryExtendsDeadline,
];

fn actions() -> Vec<Action> {
    let mut actions = vec![
        Action::Commit,
        Action::Deliver,
        Action::Disconnect,
        Action::Crash,
        Action::Restart,
        Action::Connect,
        Action::Revoke,
        Action::SettleRevoked,
        Action::Tick,
        Action::SettleDeadline,
        Action::Expire,
        Action::Reconcile,
    ];
    for owner in [false, true] {
        actions.push(Action::Read { owner });
        actions.push(Action::Begin(Mutation::Cancel { owner }));
        for valid in [false, true] {
            actions.push(Action::Begin(Mutation::Admit { owner, valid }));
        }
        for expected in [1, 2] {
            actions.push(Action::Begin(Mutation::Retry { owner, expected }));
        }
    }
    for attempt in [1, 2] {
        actions.push(Action::InstallOutput(attempt));
        actions.push(Action::Begin(Mutation::Publish { attempt }));
    }
    actions
}

impl State {
    fn view(self) -> Reply {
        if self.work.expired {
            Reply::Expired
        } else if self.work.phase.terminal() {
            Reply::Terminal(
                self.work.phase,
                self.work.attempt,
                self.work.retain_until.expect("terminal retention"),
            )
        } else {
            Reply::Pending(self.work.attempt)
        }
    }

    fn settle(&mut self, phase: Phase) {
        self.work.phase = phase;
        self.work.retain_until = Some(self.now + REPLAY_INTERVAL);
    }

    fn mutate(&mut self, mutation: Mutation, fault: Fault) -> Reply {
        let owner = match mutation {
            Mutation::Admit { owner, .. }
            | Mutation::Retry { owner, .. }
            | Mutation::Cancel { owner } => owner,
            Mutation::Publish { .. } => true,
        };
        // Checked at commit as well as connection setup. Revocation can race
        // with a transaction begun by a previously authorized connection.
        if !owner || self.revoked {
            return Reply::Unauthorized;
        }
        if self.work.expired {
            if fault == Fault::ReusesExpired && matches!(mutation, Mutation::Admit { .. }) {
                self.work = Work::default();
            } else {
                return Reply::Expired;
            }
        }
        match mutation {
            Mutation::Admit { valid: false, .. } => Reply::InvalidInput,
            Mutation::Admit { valid: true, .. } => {
                if self.work.phase == Phase::Declared {
                    self.work.phase = Phase::Active;
                    self.work.attempt = 1;
                    self.work.input_retained = true;
                    self.work.execution_deadline = Some(self.now + EXECUTION_INTERVAL);
                }
                // Replaying admission is observational, including after success.
                self.view()
            }
            Mutation::Retry { expected, .. } => {
                if self.work.phase != Phase::Active {
                    return Reply::Conflict;
                }
                if self.work.attempt == expected && expected < 2 {
                    if self.now >= self.work.execution_deadline.expect("admitted deadline")
                        && fault != Fault::RetryAfterDeadline
                    {
                        return Reply::Conflict;
                    }
                    self.work.attempt += 1;
                    if fault == Fault::RetryExtendsDeadline {
                        self.work.execution_deadline = Some(self.now + EXECUTION_INTERVAL);
                    }
                } else if self.work.attempt != expected + 1 {
                    return Reply::Conflict;
                }
                // In this bound each expected attempt has one immutable retry
                // operation. Its replay returns the same replacement generation.
                self.view()
            }
            Mutation::Cancel { .. } => {
                if !self.work.phase.terminal() {
                    self.settle(Phase::Cancelled);
                }
                self.view()
            }
            Mutation::Publish { attempt } => {
                if self.work.phase != Phase::Active
                    || (attempt != self.work.attempt && fault != Fault::StalePublishes)
                    || self.output_files & (1 << attempt) == 0
                    || self.now >= self.work.execution_deadline.expect("admitted deadline")
                {
                    return Reply::Conflict;
                }
                self.work.result_attempt = Some(attempt);
                self.settle(Phase::Success);
                self.view()
            }
        }
    }

    fn step(mut self, action: Action, fault: Fault) -> Self {
        match action {
            Action::Begin(mutation) if self.online && self.connected && self.pending.is_none() => {
                self.pending = Some(mutation);
                self.reply = None;
            }
            Action::Commit if self.online => {
                if let Some(mutation) = self.pending.take() {
                    let reply = self.mutate(mutation, fault);
                    // A transaction may commit after the connection disappears.
                    self.reply = self.connected.then_some(reply);
                }
            }
            Action::InstallOutput(attempt)
                if self.online && attempt <= self.work.attempt && self.work.input_retained =>
            {
                // A superseded worker can finish staging, but not publish.
                self.output_files |= 1 << attempt;
            }
            Action::Read { owner } if self.online && self.connected => {
                self.reply = Some(
                    if (!owner || self.revoked) && fault != Fault::LeaksUnauthorized {
                        Reply::Unauthorized
                    } else {
                        self.view()
                    },
                );
            }
            Action::Deliver if self.connected => {
                if let Some(Reply::Terminal(phase, attempt, until)) = self.reply.take() {
                    self.observed_terminal = Some((phase, attempt, until));
                }
            }
            Action::Disconnect => {
                self.connected = false;
                self.reply = None;
                if fault == Fault::DisconnectFinalizes && self.work.phase == Phase::Active {
                    self.settle(Phase::Failed);
                }
            }
            Action::Crash => {
                self.online = false;
                self.connected = false;
                self.pending = None;
                self.reply = None;
            }
            Action::Restart => self.online = true,
            Action::Connect if self.online => self.connected = true,
            Action::Revoke => self.revoked = true,
            Action::SettleRevoked if self.online && self.revoked && !self.work.phase.terminal() => {
                self.settle(Phase::Cancelled);
            }
            Action::Tick if self.now < LAST_TIME => self.now += 1,
            Action::SettleDeadline
                if self.online
                    && self.work.phase == Phase::Active
                    && self.now >= self.work.execution_deadline.expect("admitted deadline") =>
            {
                self.settle(Phase::Failed);
            }
            Action::Expire
                if self.online
                    && (self
                        .work
                        .retain_until
                        .is_some_and(|until| self.now >= until)
                        || (fault == Fault::ExpiresActive
                            && self.work.phase == Phase::Active
                            && self.now >= REPLAY_INTERVAL)) =>
            {
                self.work.expired = true;
                self.work.input_retained = false;
                if let Some(attempt) = self.work.result_attempt {
                    self.output_files &= !(1 << attempt);
                }
            }
            Action::Reconcile if !self.online => {
                // Offline/exclusive cleanup retains every published, unexpired
                // result. Orphan installations may be reclaimed independently.
                let protected = if self.work.expired || fault == Fault::DeletesPublished {
                    0
                } else {
                    self.work.result_attempt.map_or(0, |attempt| 1 << attempt)
                };
                self.output_files &= protected;
            }
            _ => {}
        }
        self
    }
}

fn invariant(before: State, action: Action, after: State) -> Result<()> {
    let work = after.work;
    ensure!(work.attempt <= 2, "attempt bound exceeded");
    ensure!(
        !work.expired || work.phase.terminal(),
        "active work was evicted before authoritative settlement"
    );
    ensure!(
        work.phase != Phase::Active || (work.input_retained && work.attempt > 0),
        "admitted work lost its immutable input or attempt"
    );
    ensure!(
        work.phase.terminal() == work.retain_until.is_some(),
        "terminal retention must start at terminal commit"
    );
    ensure!(
        (work.phase == Phase::Success) == work.result_attempt.is_some(),
        "success and result commitment were not published atomically"
    );
    if let Some(attempt) = work.result_attempt {
        ensure!(
            attempt == work.attempt,
            "a stale attempt published a result"
        );
        ensure!(
            work.expired || after.output_files & (1 << attempt) != 0,
            "published output was not retained for its promised interval"
        );
    }
    if before.work.phase.terminal() {
        ensure!(
            (
                work.phase,
                work.attempt,
                work.result_attempt,
                work.retain_until
            ) == (
                before.work.phase,
                before.work.attempt,
                before.work.result_attempt,
                before.work.retain_until
            ),
            "terminal identity/outcome changed, or expired work was readmitted"
        );
    }
    if before.work.attempt > 0 {
        ensure!(
            work.execution_deadline == before.work.execution_deadline,
            "retry or replay extended the accepted execution deadline"
        );
        if work.attempt > before.work.attempt {
            ensure!(
                before.now < before.work.execution_deadline.expect("admitted deadline"),
                "retry authorized a new attempt after the execution deadline"
            );
        }
    }
    if let Some((phase, attempt, until)) = after.observed_terminal {
        ensure!(
            (work.phase, work.attempt, work.retain_until) == (phase, attempt, Some(until)),
            "the client observed an uncommitted or contradictory terminal outcome"
        );
    }
    if matches!(
        action,
        Action::Disconnect
            | Action::Crash
            | Action::Restart
            | Action::Connect
            | Action::Read { .. }
    ) {
        ensure!(
            before.work == work,
            "transport or lookup invented an authoritative work transition"
        );
    }
    if let Action::Read { owner } = action
        && before.online
        && before.connected
        && (!owner || before.revoked)
    {
        ensure!(
            after.reply == Some(Reply::Unauthorized),
            "unauthorized lookup disclosed retained work"
        );
    }
    if action == Action::Commit && before.online {
        let unauthorized = match before.pending {
            Some(Mutation::Admit { owner, .. })
            | Some(Mutation::Retry { owner, .. })
            | Some(Mutation::Cancel { owner }) => !owner || before.revoked,
            Some(Mutation::Publish { .. }) => before.revoked,
            None => false,
        };
        if unauthorized || matches!(before.pending, Some(Mutation::Admit { valid: false, .. })) {
            ensure!(
                before.work == work,
                "a refused mutation changed durable work"
            );
        }
    }
    Ok(())
}

#[derive(Debug)]
pub(super) struct Report {
    pub(super) states: usize,
    pub(super) edges: usize,
    pub(super) frontier: usize,
    pub(super) deepest: usize,
}

#[derive(Debug)]
pub(super) struct Counterexample<A> {
    pub(super) reason: String,
    pub(super) trace: Vec<A>,
}

struct Node<S, A> {
    state: S,
    predecessor: Option<(usize, A)>,
    depth: usize,
}

fn trace<S, A: Copy>(nodes: &[Node<S, A>], mut at: usize, action: A) -> Vec<A> {
    let mut trace = vec![action];
    while let Some((parent, action)) = nodes[at].predecessor {
        trace.push(action);
        at = parent;
    }
    trace.reverse();
    trace
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

pub(super) fn explore_states<S, A>(
    initial: S,
    actions: &[A],
    depth: usize,
    max_states: usize,
    step: impl Fn(S, A) -> S,
    invariant: impl Fn(S, A, S) -> Result<()>,
) -> Result<Result<Report, Counterexample<A>>>
where
    S: Copy + Eq + std::hash::Hash,
    A: Copy,
{
    ensure!(depth > 0 && max_states > 0, "model bounds must be positive");
    let mut visited = HashMap::from([(initial, 0usize)]);
    let mut nodes = vec![Node {
        state: initial,
        predecessor: None,
        depth: 0,
    }];
    let mut report = Report {
        states: 0,
        edges: 0,
        frontier: 0,
        deepest: 0,
    };
    let mut at = 0;
    while at < nodes.len() {
        let state = nodes[at].state;
        let level = nodes[at].depth;
        report.deepest = report.deepest.max(level);
        if level == depth {
            report.frontier += 1;
            at += 1;
            continue;
        }
        for action in actions {
            let next = step(state, *action);
            report.edges += 1;
            if let Err(error) = invariant(state, *action, next) {
                return Ok(Err(Counterexample {
                    reason: error.to_string(),
                    trace: trace(&nodes, at, *action),
                }));
            }
            if let std::collections::hash_map::Entry::Vacant(entry) = visited.entry(next) {
                ensure!(
                    nodes.len() < max_states,
                    "INCONCLUSIVE: reached the {max_states}-state budget at depth {}; increase --max-states",
                    level + 1
                );
                entry.insert(nodes.len());
                nodes.push(Node {
                    state: next,
                    predecessor: Some((at, *action)),
                    depth: level + 1,
                });
            }
        }
        at += 1;
    }
    report.states = nodes.len();
    Ok(Ok(report))
}

pub fn run(depth: usize, max_states: usize) -> Result<()> {
    println!(
        "durable-work model bounds: one declared work item, two attempts, two caller classes, time 0..={LAST_TIME}, at most {depth} transitions"
    );
    println!(
        "assumptions: atomic metadata commit, immutable symbolic input, separately installed output; no wire, database, cryptographic, scope-composition or liveness proof"
    );
    let report = match explore(depth, max_states, Fault::None)? {
        Ok(report) => report,
        Err(counterexample) => bail!(
            "model invariant failed: {}; trace: {:?}",
            counterexample.reason,
            counterexample.trace
        ),
    };
    println!(
        "bounded safety passed: {} states, {} checked edges, deepest {}, {} states at depth boundary",
        report.states, report.edges, report.deepest, report.frontier
    );
    for fault in NEGATIVE_CONTROLS {
        let counterexample = match explore(depth, max_states, fault)? {
            Err(counterexample) => counterexample,
            Ok(_) => bail!(
                "INCONCLUSIVE: negative control {fault:?} was not detected within depth {depth}"
            ),
        };
        println!(
            "negative control {fault:?}: {}; shortest trace: {:?}",
            counterexample.reason, counterexample.trace
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const ADMIT: Action = Action::Begin(Mutation::Admit {
        owner: true,
        valid: true,
    });
    const RETRY: Action = Action::Begin(Mutation::Retry {
        owner: true,
        expected: 1,
    });

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
    fn bounded_failure_interleavings_preserve_lifecycle() {
        let report = explore(10, 1_000_000, Fault::None).unwrap().unwrap();
        assert!(report.states > 1_000);
        assert!(report.edges > report.states);
        assert_eq!(report.deepest, 10);
    }

    #[test]
    fn known_incorrect_semantics_produce_counterexamples() {
        for fault in NEGATIVE_CONTROLS {
            let counterexample = explore(12, 1_000_000, fault).unwrap().unwrap_err();
            assert!(!counterexample.trace.is_empty(), "{fault:?}");
        }
    }

    #[test]
    fn insufficient_exploration_budget_is_not_success() {
        assert!(
            explore(12, 1, Fault::None)
                .unwrap_err()
                .to_string()
                .contains("INCONCLUSIVE")
        );
        assert!(explore(0, 100, Fault::None).is_err());
    }

    #[test]
    fn commit_can_follow_disconnect_and_replay_does_not_execute_again() {
        let state = execute(&[
            ADMIT,
            Action::Disconnect,
            Action::Commit,
            Action::Crash,
            Action::Restart,
            Action::Connect,
            ADMIT,
            Action::Commit,
            RETRY,
            Action::Commit,
            Action::Disconnect,
            Action::Connect,
            RETRY,
            Action::Commit,
        ]);
        assert_eq!(state.work.phase, Phase::Active);
        assert_eq!(state.work.attempt, 2);
        assert_eq!(state.reply, Some(Reply::Pending(2)));
    }

    #[test]
    fn completion_before_lost_ack_remains_retrievable_after_restart() {
        let state = execute(&[
            ADMIT,
            Action::Commit,
            Action::InstallOutput(1),
            Action::Begin(Mutation::Publish { attempt: 1 }),
            Action::Commit,
            Action::Crash,
            Action::Restart,
            Action::Connect,
            Action::Read { owner: true },
            Action::Deliver,
        ]);
        assert_eq!(state.observed_terminal, Some((Phase::Success, 1, 2)));
        assert_eq!(state.output_files, 1 << 1);
    }

    #[test]
    fn cancellation_revocation_and_retry_fence_delayed_publication() {
        for race in [
            vec![
                Action::Begin(Mutation::Cancel { owner: true }),
                Action::Commit,
            ],
            vec![Action::Revoke],
            vec![RETRY, Action::Commit],
        ] {
            let mut actions = vec![ADMIT, Action::Commit, Action::InstallOutput(1)];
            actions.extend(race);
            actions.extend([
                Action::Begin(Mutation::Publish { attempt: 1 }),
                Action::Commit,
            ]);
            assert_ne!(execute(&actions).work.phase, Phase::Success);
        }
    }

    #[test]
    fn expiry_does_not_extend_on_replay_or_delete_active_work() {
        let active = execute(&[
            ADMIT,
            Action::Commit,
            Action::Tick,
            Action::Tick,
            Action::Expire,
            Action::Read { owner: true },
        ]);
        assert_eq!(active.reply, Some(Reply::Pending(1)));
        let terminal = execute(&[
            ADMIT,
            Action::Commit,
            Action::InstallOutput(1),
            Action::Begin(Mutation::Publish { attempt: 1 }),
            Action::Commit,
            Action::Tick,
            ADMIT,
            Action::Commit,
            Action::Tick,
            Action::Expire,
            ADMIT,
            Action::Commit,
        ]);
        assert_eq!(terminal.work.retain_until, Some(2));
        assert_eq!(terminal.reply, Some(Reply::Expired));
        assert_eq!(terminal.work.phase, Phase::Success);
        assert_eq!(terminal.output_files, 0);
    }

    #[test]
    fn retry_after_deadline_is_refused_without_eviction_or_new_attempt() {
        let state = execute(&[
            ADMIT,
            Action::Commit,
            Action::Tick,
            Action::Tick,
            Action::Tick,
            RETRY,
            Action::Commit,
        ]);
        assert_eq!(state.reply, Some(Reply::Conflict));
        assert_eq!(state.work.attempt, 1);
        assert_eq!(state.work.execution_deadline, Some(3));
        assert_eq!(state.work.phase, Phase::Active);
        assert!(!state.work.expired);
    }

    #[test]
    fn revocation_is_rechecked_at_commit_and_cleanup_keeps_live_results() {
        let revoked = execute(&[ADMIT, Action::Revoke, Action::Commit]);
        assert_eq!(revoked.work.phase, Phase::Declared);
        assert_eq!(revoked.reply, Some(Reply::Unauthorized));
        let cleaned = execute(&[
            ADMIT,
            Action::Commit,
            Action::InstallOutput(1),
            RETRY,
            Action::Commit,
            Action::InstallOutput(2),
            Action::Begin(Mutation::Publish { attempt: 2 }),
            Action::Commit,
            Action::Crash,
            Action::Reconcile,
        ]);
        assert_eq!(cleaned.work.result_attempt, Some(2));
        assert_eq!(cleaned.output_files, 1 << 2);
    }
}
