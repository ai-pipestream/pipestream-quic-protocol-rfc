//! Authority execution and independent read/retention/retirement maintenance.
//! Setup and explicit joining are blocking operations; stop/health observation
//! do not wait for application callbacks or storage. This is not a QUIC listener.
use super::*;
use pipestream_core::v2::authority::{
    RetentionCursor, RetirementCursor,
    execution::{Executor, PoolConfig, PoolSnapshot, ResultEndpoint, WorkerPool},
    ingress::Applications,
    results::ReadCursor,
};
use std::{
    sync::atomic::{AtomicBool, Ordering},
    thread::{self, JoinHandle},
};

#[derive(Clone, Debug)]
pub struct Options {
    pub workers: PoolConfig,
    pub lease_ms: pipestream_core::v2::Duration,
    /// Bounds cursor steps, not the existing full accounting/eligibility audits.
    pub maintenance_batch: usize,
    pub maintenance_interval: Elapsed,
}
impl Default for Options {
    fn default() -> Self {
        Self {
            workers: PoolConfig {
                workers: 4,
                workers_per_owner: 2,
                scan_batch: 32,
                idle_poll_ms: 20,
            },
            lease_ms: pipestream_core::v2::Duration(30000),
            maintenance_batch: 32,
            maintenance_interval: Elapsed::from_millis(20),
        }
    }
}
impl Options {
    fn validate(&self) -> Result<(), Error> {
        if !(1..=256).contains(&self.maintenance_batch)
            || self.maintenance_interval < Elapsed::from_millis(1)
            || self.maintenance_interval > Elapsed::from_secs(60)
        {
            return Err(error(
                ErrorCode::LimitExceeded,
                "invalid runtime maintenance limits",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug)]
pub enum Component {
    Reads,
    Retention,
    Retirement,
}
impl Component {
    fn index(self) -> usize {
        match self {
            Self::Reads => 0,
            Self::Retention => 1,
            Self::Retirement => 2,
        }
    }
}
#[derive(Clone, Debug, Default)]
pub struct MaintenanceSnapshot {
    pub passes: [u64; 3],
    pub read_leases_closed: u64,
    pub files_removed: u64,
    pub sessions_retired: u64,
    pub refusals: [u64; 3],
    pub last_refusal: [Option<ErrorCode>; 3],
    /// None component identifies corruption of the shared status lock itself.
    pub fault: Option<(Option<Component>, ErrorCode)>,
    pub stopping: bool,
}
#[derive(Clone, Debug)]
pub struct Snapshot {
    /// None means the worker discovery lock is busy, not that execution is idle.
    pub execution: Option<PoolSnapshot>,
    /// None means another observer holds the short maintenance-state lock.
    pub maintenance: Option<MaintenanceSnapshot>,
    /// Execution/maintenance threads only. Connection metadata and input/output
    /// file jobs have separate owners and are not asserted idle by this field.
    pub finished: bool,
}
struct Shared {
    authority: Authority,
    options: Options,
    stop: AtomicBool,
    snapshot: Mutex<MaintenanceSnapshot>,
}
struct Maintenance {
    shared: Arc<Shared>,
    threads: Vec<JoinHandle<()>>,
}
impl Maintenance {
    fn start(authority: Authority, options: Options) -> Result<Self, Error> {
        let mut value = Self {
            shared: Arc::new(Shared {
                authority,
                options,
                stop: AtomicBool::new(false),
                snapshot: Mutex::new(MaintenanceSnapshot::default()),
            }),
            threads: Vec::new(),
        };
        for component in [
            Component::Reads,
            Component::Retention,
            Component::Retirement,
        ] {
            let shared = value.shared.clone();
            let handle = thread::Builder::new()
                .name(format!("pipestream-v2-maintenance-{}", component.index()))
                .spawn(move || {
                    if std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        maintain(&shared, component)
                    }))
                    .is_err()
                    {
                        record(
                            &shared,
                            component,
                            Err(StoreError::Corrupt("maintenance panicked")),
                        );
                    }
                })
                .map_err(|_| {
                    error(
                        ErrorCode::InternalError,
                        "maintenance thread creation failed",
                    )
                })?;
            value.threads.push(handle);
        }
        Ok(value)
    }
    fn request_stop(&self) {
        self.shared.stop.store(true, Ordering::Release);
        // Thread parking keeps a wake token, including when stop precedes park.
        for thread in &self.threads {
            thread.thread().unpark();
        }
    }
    fn snapshot(&self) -> Option<MaintenanceSnapshot> {
        match self.shared.snapshot.try_lock() {
            Ok(value) => {
                let mut value = value.clone();
                value.stopping |= self.shared.stop.load(Ordering::Acquire);
                Some(value)
            }
            Err(std::sync::TryLockError::WouldBlock) => None,
            Err(std::sync::TryLockError::Poisoned(_)) => Some(MaintenanceSnapshot {
                fault: Some((None, ErrorCode::InternalError)),
                stopping: true,
                ..Default::default()
            }),
        }
    }
    fn is_finished(&self) -> bool {
        self.threads.iter().all(JoinHandle::is_finished)
    }
    fn join(&mut self) -> Result<(), Error> {
        self.request_stop();
        let mut panicked = false;
        for thread in self.threads.drain(..) {
            panicked |= thread.join().is_err();
        }
        if panicked {
            return Err(error(
                ErrorCode::InternalError,
                "maintenance thread panicked",
            ));
        }
        Ok(())
    }
}
impl Drop for Maintenance {
    fn drop(&mut self) {
        self.request_stop();
    }
}

/// One authority's fixed execution pool plus three independent maintenance
/// threads. A blocked callback or retention pass does not occupy read maintenance.
/// Drop requests stop without joining; live threads retain roots and resource pins.
pub struct Runtime {
    execution: Option<WorkerPool>,
    maintenance: Maintenance,
}
impl Authority {
    /// Blocking setup: validates/audits paired storage and then starts workers.
    /// Call outside the async connection reader. Existing durable jobs are found
    /// without a client resubmitting them. No profile is advertised by this API.
    /// If maintenance startup fails after the execution pool started, discovered
    /// jobs may still finish; stop does not retract their accepted obligations.
    pub fn start_runtime(
        &self,
        applications: Arc<Applications>,
        endpoint: ResultEndpoint,
        supported: Capabilities,
        options: Options,
    ) -> Result<Runtime, Error> {
        options.validate()?;
        let executor = Executor::new(
            self.store.clone(),
            self.payloads.clone(),
            applications,
            endpoint,
            supported,
            options.lease_ms,
        )
        .map_err(storage)?;
        let execution = executor
            .start_workers(options.workers.clone())
            .map_err(storage)?;
        let maintenance = Maintenance::start(self.clone(), options)?;
        Ok(Runtime {
            execution: Some(execution),
            maintenance,
        })
    }
}
impl Runtime {
    pub fn snapshot(&self) -> Snapshot {
        Snapshot {
            execution: self.execution.as_ref().and_then(WorkerPool::try_snapshot),
            maintenance: self.maintenance.snapshot(),
            finished: self.is_finished(),
        }
    }
    pub fn wake(&self) {
        if let Some(execution) = &self.execution {
            execution.wake();
        }
        for thread in &self.maintenance.threads {
            thread.thread().unpark();
        }
    }
    /// Stop future discovery without asserting cancellation or completion of work.
    /// A discovered callback or storage operation may still be running afterward.
    pub fn request_stop(&self) {
        if let Some(execution) = &self.execution {
            execution.request_stop();
        }
        self.maintenance.request_stop();
    }
    pub fn is_finished(&self) -> bool {
        self.execution.as_ref().is_none_or(WorkerPool::is_finished)
            && self.maintenance.is_finished()
    }
    /// Blocking join, not a deadline-bounded async operation. An async owner can
    /// request_stop and poll is_finished before joining. Arbitrary callbacks are
    /// not forcibly preempted, and timeout cannot release their live resource pins.
    pub fn shutdown(mut self) -> Result<Snapshot, Error> {
        self.request_stop();
        let execution = self
            .execution
            .take()
            .expect("runtime owns its pool")
            .shutdown();
        let maintenance = self.maintenance.join();
        let execution = execution.map_err(storage)?;
        maintenance?;
        Ok(Snapshot {
            execution: Some(execution),
            maintenance: self.maintenance.snapshot(),
            finished: true,
        })
    }
}
impl Drop for Runtime {
    fn drop(&mut self) {
        self.request_stop();
    }
}

fn recoverable(error: &StoreError) -> bool {
    if error.is_storage_contention() {
        return true;
    }
    match error {
        StoreError::Protocol(error) => matches!(
            error.code,
            ErrorCode::ClockUnsafe | ErrorCode::LimitExceeded
        ),
        _ => false,
    }
}
fn record(shared: &Shared, component: Component, result: Result<u64, StoreError>) {
    let mut state = match shared.snapshot.lock() {
        Ok(state) => state,
        Err(poisoned) => {
            shared.stop.store(true, Ordering::Release);
            let mut state = poisoned.into_inner();
            state.fault = Some((None, ErrorCode::InternalError));
            return;
        }
    };
    let index = component.index();
    state.passes[index] = state.passes[index].saturating_add(1);
    match result {
        Ok(count) => {
            let counter = match component {
                Component::Reads => &mut state.read_leases_closed,
                Component::Retention => &mut state.files_removed,
                Component::Retirement => &mut state.sessions_retired,
            };
            *counter = counter.saturating_add(count);
        }
        Err(error) => {
            let fatal = !recoverable(&error);
            let code = storage(error).code;
            state.refusals[index] = state.refusals[index].saturating_add(1);
            state.last_refusal[index] = Some(code);
            if fatal {
                state.fault = Some((Some(component), code));
                shared.stop.store(true, Ordering::Release);
            }
        }
    }
}
fn maintain(shared: &Shared, component: Component) {
    let mut reads = ReadCursor::default();
    let mut retention = RetentionCursor::default();
    let mut retirement = RetirementCursor::default();
    while !shared.stop.load(Ordering::Acquire) {
        let authority = &shared.authority;
        let batch = shared.options.maintenance_batch;
        let result = match component {
            Component::Reads => authority
                .results
                .maintain(&mut reads, batch, std::time::Instant::now())
                .map(|report| report.closed as u64),
            Component::Retention => authority
                .store
                .reclaim(&authority.payloads, &mut retention, batch)
                .map(|report| report.removed_files as u64),
            Component::Retirement => authority
                .store
                .retire(&authority.payloads, &mut retirement, batch)
                .map(|report| u64::from(report.completed)),
        };
        record(shared, component, result);
        if !shared.stop.load(Ordering::Acquire) {
            thread::park_timeout(shared.options.maintenance_interval);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maintenance_retries_only_explicit_clock_capacity_and_storage_contention() {
        for code in [ErrorCode::ClockUnsafe, ErrorCode::LimitExceeded] {
            assert!(recoverable(&error(code, "test refusal").into()));
        }
        for code in [
            ErrorCode::Unauthorized,
            ErrorCode::Conflict,
            ErrorCode::NotFound,
            ErrorCode::Expired,
            ErrorCode::InternalError,
            ErrorCode::OutputUnavailable,
        ] {
            assert!(!recoverable(&error(code, "test refusal").into()));
        }
        for (code, retry) in [
            (rusqlite::ffi::SQLITE_BUSY, true),
            (rusqlite::ffi::SQLITE_LOCKED, true),
            (rusqlite::ffi::SQLITE_CORRUPT, false),
            (rusqlite::ffi::SQLITE_IOERR, false),
        ] {
            let error = StoreError::Database(rusqlite::Error::SqliteFailure(
                rusqlite::ffi::Error::new(code),
                None,
            ));
            assert_eq!(recoverable(&error), retry);
        }
        assert!(!recoverable(&StoreError::Io(std::io::Error::other(
            "broken storage"
        ))));
        assert!(!recoverable(&StoreError::Corrupt("broken record")));
    }
}
