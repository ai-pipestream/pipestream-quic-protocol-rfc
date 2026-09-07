//! Bounded pull workers. The durable job table is the queue: no caller must
//! resubmit keys after restart and no volatile queue owns an admitted obligation.

use super::*;
use std::{
    collections::BTreeMap,
    sync::{Condvar, Mutex, MutexGuard},
    thread::{self, JoinHandle},
};

#[derive(Clone, Debug)]
pub struct PoolConfig {
    /// Application callback threads. One additional thread drives settlement.
    pub workers: usize,
    pub workers_per_owner: usize,
    /// Maximum durable records inspected by one discovery pass.
    pub scan_batch: usize,
    /// Retry discovery after an idle/refused pass, even without a notification.
    pub idle_poll_ms: u64,
}

#[derive(Clone, Debug, Default)]
pub struct PoolSnapshot {
    pub active: usize,
    pub inspected: u64,
    pub completed: u64,
    pub awaiting_retry: u64,
    pub waiting_children: u64,
    pub yielded: u64,
    pub refused: u64,
    pub last_refusal: Option<Diagnostic>,
    pub stopping: bool,
    pub faulted: bool,
    pub settled: u64,
    pub sealed_scopes: u64,
    pub closed_scopes: u64,
}

#[derive(Default)]
struct PoolState {
    cursor: i64,
    owners: BTreeMap<String, usize>,
    snapshot: PoolSnapshot,
}
struct Shared {
    state: Mutex<PoolState>,
    wake: Condvar,
    config: PoolConfig,
    executor: Executor,
    _pin: payload::WorkerPoolPin,
}
impl Shared {
    fn state(&self) -> MutexGuard<'_, PoolState> {
        match self.state.lock() {
            Ok(state) => state,
            Err(error) => {
                let mut state = error.into_inner();
                state.snapshot.faulted = true;
                state.snapshot.stopping = true;
                state.snapshot.last_refusal =
                    Some(diag(ErrorCode::InternalError, "worker pool state poisoned"));
                state
            }
        }
    }
}

/// Runs actual callbacks on a fixed number of threads, never on a control reader.
/// An additional maintenance thread drives settlement independently of callbacks.
/// `request_stop` is nonblocking; `shutdown` joins in-flight callbacks. Applications
/// must return/cooperate with their context fences; arbitrary code is not forcibly
/// preempted. Dropping the pool requests stop but does not wait for callback code.
pub struct WorkerPool {
    shared: Arc<Shared>,
    workers: Vec<JoinHandle<()>>,
}

impl Executor {
    pub fn start_workers(&self, config: PoolConfig) -> Result<WorkerPool> {
        if config.workers == 0
            || config.workers > 128
            || config.workers as u64 > self.store.policy.active_jobs.0
            || config.workers_per_owner == 0
            || config.workers_per_owner > config.workers
            || config.scan_batch == 0
            || config.scan_batch > 256
            || config.idle_poll_ms == 0
            || config.idle_poll_ms > 60_000
        {
            return Err(protocol(
                ErrorCode::LimitExceeded,
                "invalid bounded worker configuration",
            ));
        }
        let mut connection = self.store.connect()?;
        let tx = connection.transaction()?;
        bound_payloads(&tx, &self.payloads)?;
        drop(tx);
        drop(connection);
        let shared = Arc::new(Shared {
            state: Mutex::new(PoolState::default()),
            wake: Condvar::new(),
            config,
            executor: self.clone(),
            _pin: self.payloads.pin_worker_pool()?,
        });
        let mut pool = WorkerPool {
            shared,
            workers: Vec::new(),
        };
        for index in 0..=pool.shared.config.workers {
            let shared = pool.shared.clone();
            let maintenance = index == pool.shared.config.workers;
            match thread::Builder::new()
                .name(if maintenance {
                    "pipestream-v2-settlement".into()
                } else {
                    format!("pipestream-v2-{index}")
                })
                .spawn(move || {
                    if maintenance {
                        reconcile(shared)
                    } else {
                        worker(shared)
                    }
                }) {
                Ok(handle) => pool.workers.push(handle),
                Err(error) => {
                    // Construction may already have dispatched durable work;
                    // stop and join those threads before returning the error.
                    pool.shutdown()?;
                    return Err(error.into());
                }
            }
        }
        Ok(pool)
    }
}

impl WorkerPool {
    pub fn snapshot(&self) -> PoolSnapshot {
        self.shared.state().snapshot.clone()
    }
    /// Optional latency hint after admission/retry. Correctness does not depend
    /// on it: polling discovers commits made by any connection or before restart.
    pub fn wake(&self) {
        self.shared.wake.notify_all();
    }
    pub fn request_stop(&self) {
        self.shared.state().snapshot.stopping = true;
        self.wake();
    }
    pub fn shutdown(mut self) -> Result<PoolSnapshot> {
        self.request_stop();
        let mut panicked = false;
        for worker in self.workers.drain(..) {
            panicked |= worker.join().is_err();
        }
        if panicked {
            return Err(StoreError::Corrupt(
                "worker thread panicked outside application callback",
            ));
        }
        Ok(self.snapshot())
    }
}
impl Drop for WorkerPool {
    fn drop(&mut self) {
        self.request_stop();
    }
}

fn fault(state: &mut PoolState, error: &StoreError) {
    state.snapshot.refused = state.snapshot.refused.saturating_add(1);
    state.snapshot.last_refusal = Some(diagnostic(error));
    // Corruption and unknown storage failures need operator attention, not an
    // infinite retry loop. Named clock/capacity/auth/fence refusals can recover.
    let recoverable = match error {
        StoreError::Protocol(error) => matches!(
            error.code,
            ErrorCode::Unauthorized
                | ErrorCode::LimitExceeded
                | ErrorCode::NotFound
                | ErrorCode::Expired
                | ErrorCode::Conflict
                | ErrorCode::NotReady
                | ErrorCode::DeadlineExceeded
                | ErrorCode::Cancelled
                | ErrorCode::ApplicationUnsupported
                | ErrorCode::ClockUnsafe
                | ErrorCode::AlreadyTerminal
        ),
        StoreError::Database(rusqlite::Error::SqliteFailure(code, _)) => {
            matches!(
                code.code,
                rusqlite::ErrorCode::DatabaseBusy | rusqlite::ErrorCode::DatabaseLocked
            )
        }
        _ => false,
    };
    if !recoverable {
        state.snapshot.faulted = true;
        state.snapshot.stopping = true;
    }
}

type Candidate = (SessionIdentity, WorkKey);
fn discover(shared: &Shared, state: &mut PoolState) -> Result<Option<Candidate>> {
    let mut connection = shared.executor.store.connect()?;
    let tx = connection.transaction()?;
    let now = shared.executor.store.check_clock(&tx)?;
    let mut statement = tx.prepare("SELECT j.work_row,w.generation,s.owner FROM jobs j JOIN work w ON w.row_id=j.work_row JOIN sessions s ON s.generation=w.generation WHERE NOT EXISTS(SELECT 1 FROM retirements r WHERE r.generation=s.generation) AND j.work_row>?1 ORDER BY j.work_row LIMIT ?2")?;
    let mut rows = statement.query(params![state.cursor, shared.config.scan_batch as i64])?;
    let mut inspected = 0;
    while let Some(row) = rows.next()? {
        let id = row.get(0)?;
        state.cursor = id;
        state.snapshot.inspected = state.snapshot.inspected.saturating_add(1);
        inspected += 1;
        let (_, job): (_, jobs::JobRecord) = records::read(&tx, job_target(id))?;
        if !job.executor_live
            || job.stage == Number(3)
            || job.lease_until.is_some_and(|until| now < until)
        {
            continue;
        }
        let owner: String = row.get(2)?;
        if state.owners.get(&owner).copied().unwrap_or(0) >= shared.config.workers_per_owner {
            continue;
        }
        let identity = SessionIdentity {
            authority: shared.executor.store.authority.clone(),
            owner: IdentityLabel(owner.clone()),
            generation: Id(number(row, 1)?),
        };
        // Discovery is not an execution grant. The worker's committing claim
        // repeats authorization, scope/deadline/child and resource checks.
        state.snapshot.active += 1;
        *state.owners.entry(owner).or_default() += 1;
        return Ok(Some((identity, job.parameters.work)));
    }
    if inspected < shared.config.scan_batch {
        state.cursor = 0;
    }
    Ok(None)
}

fn worker(shared: Arc<Shared>) {
    loop {
        let candidate = {
            let mut state = shared.state();
            if state.snapshot.stopping {
                return;
            }
            match discover(&shared, &mut state) {
                Ok(value) => value,
                Err(error) => {
                    fault(&mut state, &error);
                    None
                }
            }
        };
        let mut backoff = true;
        if let Some((identity, key)) = candidate {
            let result = shared.executor.run(&identity, &key);
            let mut state = shared.state();
            state.snapshot.active -= 1;
            let owner = state
                .owners
                .get_mut(&identity.owner.0)
                .expect("discovered owner");
            *owner -= 1;
            if *owner == 0 {
                state.owners.remove(&identity.owner.0);
            }
            match result {
                Ok(view) => {
                    if view.state == State::AWAITING_RETRY {
                        state.snapshot.awaiting_retry =
                            state.snapshot.awaiting_retry.saturating_add(1);
                    } else if view.state == State::WAITING_CHILDREN {
                        state.snapshot.waiting_children =
                            state.snapshot.waiting_children.saturating_add(1);
                    } else if view.state == State::ACTIVE {
                        state.snapshot.yielded = state.snapshot.yielded.saturating_add(1);
                    } else if view.state.is_terminal() {
                        state.snapshot.completed = state.snapshot.completed.saturating_add(1);
                    }
                    // A voluntary yield can report temporary child-admission
                    // pressure. Do not spin on the same durable candidate while
                    // another worker or reader still holds the needed capacity.
                    backoff = view.state == State::ACTIVE;
                }
                Err(error) => fault(&mut state, &error),
            }
            if !backoff || state.snapshot.stopping {
                shared.wake.notify_all();
            }
        }
        if backoff {
            let state = shared.state();
            if state.snapshot.stopping {
                return;
            }
            let _ = shared
                .wake
                .wait_timeout(state, Elapsed::from_millis(shared.config.idle_poll_ms));
        }
    }
}

fn reconcile(shared: Arc<Shared>) {
    let mut cursor = ReconcileCursor::default();
    loop {
        if shared.state().snapshot.stopping {
            return;
        }
        let result = shared
            .executor
            .store
            .reconcile(&mut cursor, shared.config.scan_batch);
        let mut state = shared.state();
        match result {
            Ok(progress) => {
                state.snapshot.settled = state
                    .snapshot
                    .settled
                    .saturating_add(progress.settled_work as u64);
                state.snapshot.sealed_scopes = state
                    .snapshot
                    .sealed_scopes
                    .saturating_add(progress.sealed_scopes as u64);
                state.snapshot.closed_scopes = state
                    .snapshot
                    .closed_scopes
                    .saturating_add(progress.closed_scopes as u64);
                if progress.settled_work + progress.closed_scopes != 0 {
                    shared.wake.notify_all();
                }
            }
            Err(error) => {
                fault(&mut state, &error);
                if state.snapshot.stopping {
                    shared.wake.notify_all();
                }
            }
        }
        if state.snapshot.stopping {
            return;
        }
        let _ = shared
            .wake
            .wait_timeout(state, Elapsed::from_millis(shared.config.idle_poll_ms));
    }
}
