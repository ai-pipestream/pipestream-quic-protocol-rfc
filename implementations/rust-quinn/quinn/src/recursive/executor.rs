//! Store-scoped asynchronous dispatch. Physical permits outlive cancelled waiters.

use super::*;
use pipestream_core::persistence::ReadyJob;
use std::{
    collections::BTreeSet,
    sync::{Mutex, OnceLock, Weak},
};
use tokio::task::{JoinHandle, JoinSet};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExecutionLimits {
    pub workers: usize,
    pub workers_per_principal: usize,
}

impl Default for ExecutionLimits {
    fn default() -> Self {
        Self {
            workers: 4,
            workers_per_principal: 2,
        }
    }
}

impl ExecutionLimits {
    pub(super) fn validate(self) -> Result<(), ProtocolError> {
        if self.workers == 0
            || self.workers > 64
            || self.workers_per_principal == 0
            || self.workers_per_principal > self.workers
        {
            return Err(limit_error("invalid execution worker limits"));
        }
        Ok(())
    }
}

type PrincipalKey = Option<(String, String)>;
type JobKey = (String, ExecutionKey);

#[derive(Default)]
struct Active {
    principals: BTreeMap<PrincipalKey, usize>,
    jobs: BTreeSet<JobKey>,
}

pub(super) struct WorkerPool {
    limits: ExecutionLimits,
    active: Mutex<Active>,
}

impl WorkerPool {
    pub(super) fn open(path: &Path, limits: ExecutionLimits) -> Result<Arc<Self>, ProtocolError> {
        Self::open_kind(path, limits, false)
    }

    pub(super) fn admission(
        path: &Path,
        limits: ExecutionLimits,
    ) -> Result<Arc<Self>, ProtocolError> {
        Self::open_kind(path, limits, true)
    }

    fn open_kind(
        path: &Path,
        limits: ExecutionLimits,
        admission: bool,
    ) -> Result<Arc<Self>, ProtocolError> {
        type Pools = BTreeMap<(PathBuf, bool), Weak<WorkerPool>>;
        static POOLS: OnceLock<Mutex<Pools>> = OnceLock::new();
        limits.validate()?;
        let path = (fs::canonicalize(path).map_err(storage_error)?, admission);
        let mut pools = POOLS
            .get_or_init(Default::default)
            .lock()
            .map_err(storage_error)?;
        pools.retain(|_, pool| pool.strong_count() != 0);
        if let Some(existing) = pools.get(&path).and_then(Weak::upgrade) {
            if existing.limits != limits {
                return Err(limit_error("active executor limits differ for this store"));
            }
            return Ok(existing);
        }
        let pool = Arc::new(Self {
            limits,
            active: Mutex::new(Active::default()),
        });
        pools.insert(path, Arc::downgrade(&pool));
        Ok(pool)
    }

    pub(super) fn acquire(
        self: &Arc<Self>,
        principal: Option<&PrincipalBinding>,
        session_id: &str,
        key: ExecutionKey,
    ) -> Result<Option<WorkerPermit>, ProtocolError> {
        let principal = principal.map(|p| (p.authority.clone(), p.principal.clone()));
        let key = (session_id.to_owned(), key);
        let mut active = self.active.lock().map_err(storage_error)?;
        if active.jobs.len() >= self.limits.workers
            || active.jobs.contains(&key)
            || active.principals.get(&principal).copied().unwrap_or(0)
                >= self.limits.workers_per_principal
        {
            return Ok(None);
        }
        active.jobs.insert(key.clone());
        *active.principals.entry(principal.clone()).or_default() += 1;
        Ok(Some(WorkerPermit {
            pool: self.clone(),
            principal,
            key,
        }))
    }

    fn active_count(&self) -> Result<usize, ProtocolError> {
        Ok(self.active.lock().map_err(storage_error)?.jobs.len())
    }
}

pub(super) struct WorkerPermit {
    pool: Arc<WorkerPool>,
    principal: PrincipalKey,
    key: JobKey,
}

impl Drop for WorkerPermit {
    fn drop(&mut self) {
        if let Ok(mut active) = self.pool.active.lock() {
            active.jobs.remove(&self.key);
            let count = active
                .principals
                .get_mut(&self.principal)
                .expect("worker principal is charged");
            *count -= 1;
            if *count == 0 {
                active.principals.remove(&self.principal);
            }
        }
    }
}

/// Dropping this handle stops dispatch, not already-started synchronous callbacks.
pub struct ExecutorHandle {
    pub(super) task: JoinHandle<Result<(), ProtocolError>>,
    pool: Arc<WorkerPool>,
}

impl ExecutorHandle {
    /// Stop dispatch and wait up to the grace period. Returns the store-wide active count.
    /// A nonzero count means callbacks still own their physical permits and leases.
    pub async fn shutdown(mut self, grace: Duration) -> Result<usize, ProtocolError> {
        self.task.abort();
        let _ = (&mut self.task).await;
        let deadline = tokio::time::Instant::now()
            .checked_add(grace)
            .ok_or_else(|| limit_error("shutdown grace exceeds clock"))?;
        loop {
            let active = self.pool.active_count()?;
            if active == 0 || tokio::time::Instant::now() >= deadline {
                return Ok(active);
            }
            tokio::time::sleep_until(
                deadline.min(tokio::time::Instant::now() + Duration::from_millis(5)),
            )
            .await;
        }
    }
}

impl Drop for ExecutorHandle {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl<P: EntityProcessor, E: EntityStore> RecursiveService<P, E> {
    /// Audit retained jobs, then start periodic bounded execution independent of connections.
    pub fn start_executor(&self) -> Result<ExecutorHandle, ProtocolError> {
        let pool = WorkerPool::open(self.store.path(), self.execution_limits)?;
        let service = self.clone();
        let dispatch_pool = pool.clone();
        let task = tokio::spawn(async move {
            let audit = service.store.clone();
            tokio::task::spawn_blocking(move || audit.integrity_check())
                .await
                .map_err(storage_error)?
                .map_err(store_error)?;
            let mut workers = JoinSet::new();
            let mut tick = tokio::time::interval(Duration::from_millis(10));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    result = workers.join_next(), if !workers.is_empty() => {
                        match result {
                            Some(Ok(Ok(()))) => {},
                            Some(Ok(Err(error))) => eprintln!("job publication did not commit: {error}"),
                            Some(Err(error)) => return Err(storage_error(error)),
                            None => {},
                        }
                    }
                    _ = tick.tick() => {
                        if workers.len() >= service.execution_limits.workers { continue; }
                        let store = service.store.clone();
                        let ready = tokio::task::spawn_blocking(move || {
                            store.ready_jobs(now_micros().map_err(StoreError::Io)?, store.job_limits().total)
                        }).await.map_err(storage_error)?.map_err(store_error)?;
                        for job in ready {
                            if workers.len() >= service.execution_limits.workers { break; }
                            if !service.permits_job(&job) { continue; }
                            let Some(permit) = dispatch_pool.acquire(job.principal.as_ref(), &job.session_id, job.key)? else { continue; };
                            let mut executor = service.clone();
                            executor.caller = job.principal.clone();
                            workers.spawn_blocking(move || {
                                let _permit = permit;
                                executor.execute_job(&job).map(|_| ())
                            });
                        }
                    }
                }
            }
        });
        Ok(ExecutorHandle { task, pool })
    }

    pub(super) fn permits_job(&self, job: &ReadyJob) -> bool {
        match (&self.authentication, &job.principal) {
            (None, None) => true,
            (Some(policy), Some(principal)) => policy.permits_recovery(principal),
            _ => false,
        }
    }

    pub(super) fn execute_job(&self, job: &ReadyJob) -> Result<bool, ProtocolError> {
        let (lease, versioned) = self
            .transact(&job.session_id, |s| {
                s.acquire_job(
                    self.caller.as_ref(),
                    job.key,
                    now_micros().map_err(storage_error)?,
                    self.execution_lease_micros,
                )
            })
            .map_err(store_error)?;
        let Some(lease) = lease else {
            return Ok(false);
        };
        let input = &versioned.session.jobs[&job.key].input;
        let computation = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| match input {
            JobInput::Process {
                header,
                length,
                digest,
                layers,
            } => {
                let payload = self
                    .entities
                    .load_payload(&job.session_id, job.key.entity, *length, *digest)
                    .map_err(|error| {
                        ProtocolError::new(
                            pipestream_core::ERROR_INTEGRITY,
                            "PIPESTREAM_INTEGRITY_ERROR",
                            format!("retained job input cannot be opened: {error}"),
                        )
                    })?;
                let result = self.processor.process(ProcessContext {
                    execution: &lease,
                    session_id: &job.session_id,
                    header,
                    payload: &payload,
                    now_micros: now_micros().map_err(storage_error)?,
                })?;
                if matches!(result, ProcessingDisposition::Yield { .. })
                    && !layers.layer2_resilience
                {
                    return Err(layer_error("processor yield requires negotiated Layer 2"));
                }
                Ok(Computed::Process(result))
            }
            JobInput::Rehydrate { digest } => Ok(Computed::Rehydrate(
                digest.clone(),
                self.processor.rehydrate(RehydrateContext {
                    execution: &lease,
                    session: &versioned.session,
                    parent: job.key.entity,
                }),
            )),
            JobInput::Resume { claim_id } => {
                let claim = &versioned.session.claims[claim_id];
                Ok(Computed::Resume(self.processor.resume(ResumeContext {
                    execution: &lease,
                    session: &versioned.session,
                    entity: job.key.entity,
                    continuation_token: &claim.token,
                })))
            }
        }))
        .unwrap_or_else(|_| Err(entity_error("application callback panicked")));
        match computation {
            Ok(computed) => {
                let publication = self.transact(&job.session_id, |s| {
                    let now = now_micros().map_err(storage_error)?;
                    s.publish_job(self.caller.as_ref(), &lease, now, |s| match computed {
                        Computed::Process(disposition) => {
                            let outcome =
                                match apply_disposition(s, job.key.entity, disposition, now)? {
                                    AppliedDisposition::Complete => ProcessOutcome::Complete,
                                    AppliedDisposition::Dehydrate => ProcessOutcome::Dehydrate,
                                    AppliedDisposition::Failed => ProcessOutcome::Failed,
                                    AppliedDisposition::Deferred(claim) => {
                                        ProcessOutcome::Deferred {
                                            reason: claim.reason,
                                            claim_id: claim.record.claim_id,
                                        }
                                    }
                                };
                            Ok(JobOutput::Processed(outcome))
                        }
                        Computed::Rehydrate(digest, output) => {
                            s.complete_rehydration(job.key.entity, output)?;
                            Ok(JobOutput::Rehydrated(digest))
                        }
                        Computed::Resume(output) => {
                            s.complete_entity(job.key.entity, output)?;
                            Ok(JobOutput::Resumed)
                        }
                    })
                });
                if let Err(error) = publication {
                    let StoreError::Protocol(error) = error else {
                        return Err(store_error(error));
                    };
                    // Invalid application decisions are terminal refusals. The fence still
                    // prevents this second transaction from publishing an expired result.
                    self.transact(&job.session_id, |s| {
                        s.refuse_job(
                            self.caller.as_ref(),
                            &lease,
                            now_micros().map_err(storage_error)?,
                            &error,
                        )
                    })
                    .map_err(store_error)?;
                }
            }
            Err(error) => {
                self.transact(&job.session_id, |s| {
                    s.refuse_job(
                        self.caller.as_ref(),
                        &lease,
                        now_micros().map_err(storage_error)?,
                        &error,
                    )
                })
                .map_err(store_error)?;
            }
        }
        Ok(true)
    }
}

enum Computed {
    Process(ProcessingDisposition),
    Rehydrate(ScopeDigest, [u8; 32]),
    Resume([u8; 32]),
}

#[cfg(test)]
mod tests;
