//! Synchronous application workers, deliberately separate from transport/control
//! tasks. A host runs these on its bounded worker pool. Every worker owns bounded
//! payload handles; a lease is not an exactly-once external-effect guarantee.

use super::{
    ingress::Applications,
    payload::{ObjectReader, OutputReservation, OutputStaging, PayloadStore},
    *,
};
use std::time::Instant;

mod branch;
mod pool;
mod retry;
pub use branch::{ChildPage, Expansion, ExpansionContext, ExpansionOutcome};
pub use pool::{PoolConfig, PoolSnapshot, WorkerPool};

pub trait Application: Send + Sync {
    fn execute(&self, context: &mut WorkContext) -> Result<ApplicationOutcome>;
    /// Mode 2 requires a real expansion callback. Leaf/caller-expanded contracts
    /// can leave this absent and cannot register authority-expanded mode.
    fn expansion(&self) -> Option<&dyn Expansion> {
        None
    }
}

pub enum ApplicationOutcome {
    Succeeded,
    Retryable(Diagnostic),
    Failed(Diagnostic),
}

/// A small real application useful for storage/worker integration and examples.
/// It streams the admitted bytes unchanged, without buffering the whole input.
pub struct CopyApplication;
impl Application for CopyApplication {
    fn execute(&self, context: &mut WorkContext) -> Result<ApplicationOutcome> {
        let input = context.input_descriptor().clone();
        context.begin_output(input.length, input.content_type)?;
        let mut bytes = [0; 8192];
        let limit = bytes.len().min(context.buffer_limit());
        loop {
            let count = context.read_input(&mut bytes[..limit])?;
            if count == 0 {
                break;
            }
            context.write_output(&bytes[..count])?;
        }
        context.finish_output()?;
        Ok(ApplicationOutcome::Succeeded)
    }
}

/// Trusted authority endpoint configuration, never a callback-supplied URL.
/// It contains only a DNS/IPv6 host and explicit port, not credentials or a path.
#[derive(Clone)]
pub struct ResultEndpoint(String);
impl ResultEndpoint {
    pub fn new(authority: String) -> Result<Self> {
        let value = Self(authority);
        value.locator(
            &SessionIdentity {
                authority: IdentityLabel("test".into()),
                owner: IdentityLabel("test".into()),
                generation: Id(1),
            },
            &WorkKey {
                scope: Number(0),
                producer: Producer(0),
                entity: Id(1),
            },
            Id(1),
            OutputIndex(0),
        )?;
        Ok(value)
    }
    fn locator(
        &self,
        identity: &SessionIdentity,
        work: &WorkKey,
        attempt: Id,
        index: OutputIndex,
    ) -> Result<ResultLocator> {
        let locator = ResultLocator(format!(
            "pipestream://{}/v2/sessions/{}/scopes/{}/producers/{}/entities/{}/attempts/{}/outputs/{}",
            self.0,
            identity.generation.0,
            work.scope.0,
            work.producer.0,
            work.entity.0,
            attempt.0,
            index.0
        ));
        locator.check()?;
        Ok(locator)
    }
}

#[derive(Clone)]
pub struct Executor {
    store: AuthorityStore,
    payloads: PayloadStore,
    applications: Arc<Applications>,
    endpoint: ResultEndpoint,
    caps: Capabilities,
    lease_ms: Duration,
}
impl Executor {
    pub fn new(
        store: AuthorityStore,
        payloads: PayloadStore,
        applications: Arc<Applications>,
        endpoint: ResultEndpoint,
        caps: Capabilities,
        lease_ms: Duration,
    ) -> Result<Self> {
        selected(&caps)?;
        lease_ms.check()?;
        if lease_ms.0 > 300_000 {
            return Err(protocol(
                ErrorCode::LimitExceeded,
                "worker lease exceeds local maximum",
            ));
        }
        Ok(Self {
            store,
            payloads,
            applications,
            endpoint,
            caps,
            lease_ms,
        })
    }

    /// Claim a queued job or recover an expired lease. This commits before an
    /// Execution is returned. A live prior output handle prevents slot recycling.
    pub fn claim(&self, identity: &SessionIdentity, key: &WorkKey) -> Result<Execution> {
        let mut connection = self.store.connect()?;
        let tx = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let binding = self
            .store
            .authorize_session(&tx, identity, Permission::Execute)?;
        sessions::check_connection(&tx, &binding, &self.caps)?;
        let (row, mut job, job_revision, mut view, work_revision) = load(&tx, identity, key)?;
        scopes::unfenced(&tx, identity.generation, key.scope)?;
        eligible(&view)?;
        let now = self.store.check_clock(&tx)?;
        if now
            >= view
                .deadline
                .ok_or(StoreError::Corrupt("job deadline missing"))?
        {
            return Err(protocol(
                ErrorCode::DeadlineExceeded,
                "execution deadline reached",
            ));
        }
        if job.stage == Number(3) {
            return Err(protocol(ErrorCode::NotReady, "job awaits explicit retry"));
        }
        if job.lease_until.is_some_and(|until| now < until) {
            return Err(protocol(
                ErrorCode::NotReady,
                "current worker lease is still live",
            ));
        }
        let expanding = job.parameters.mode == Mode(2) && !job.expansion_complete;
        if !expanding && let Some(child) = &view.child {
            let summary = scopes::closed(&tx, identity.generation, Number(child.scope.0))?
                .ok_or_else(|| protocol(ErrorCode::NotReady, "child scope has not closed"))?;
            if summary.counts.success != summary.declared {
                return Err(protocol(
                    ErrorCode::NotReady,
                    "child scope did not close successfully",
                ));
            }
        }
        if self
            .applications
            .safety(&job.parameters.application, job.parameters.mode)?
            != job.restart_safety
        {
            return Err(protocol(
                ErrorCode::ApplicationUnsupported,
                "retained restart contract changed",
            ));
        }
        let application = self
            .applications
            .implementation(&job.parameters.application, job.parameters.mode)?;
        bound_payloads(&tx, &self.payloads)?;
        let input =
            self.payloads
                .open_object(&job.input_key.0, &identity.owner, &job.parameters.input)?;
        let mut outputs = self.payloads.recover_outputs(
            &job.reservation_key.0,
            &identity.owner,
            &job.parameters.outputs,
        )?;
        outputs.reserve_worker_io()?;
        let child_reader = if !expanding && view.child.is_some() {
            Some(self.payloads.reserve_reader(&identity.owner)?)
        } else {
            None
        };
        job.lease = Number(increment(job.lease.0)?);
        job.lease_until = Some(add_duration(now, self.lease_ms)?.min(view.deadline.unwrap()));
        job.stage = Number(1);
        if view.state == State::WAITING_CHILDREN {
            view.state = State::ACTIVE;
            records::replace(&tx, work_target(row), work_revision, &view, true)?;
        }
        records::replace(&tx, job_target(row), job_revision, &job, false)?;
        self.store.remember_clock(&tx, now)?;
        self.store.authorize(&identity.owner, Permission::Execute)?;
        commit(tx, "worker-claim")?;
        let mut caps = self.caps.clone();
        caps.object_limit = job.object_limit.min(caps.object_limit);
        Ok(Execution {
            application,
            expanding,
            context: WorkContext {
                executor: self.clone(),
                identity: identity.clone(),
                key: key.clone(),
                attempt: job.attempt,
                lease: job.lease,
                input_descriptor: job.parameters.input,
                mode: job.parameters.mode,
                input,
                reservation: outputs,
                caps,
                pending: None,
                produced: Vec::new(),
                failure: None,
                child_reader,
                child_input: None,
            },
        })
    }

    pub fn run(&self, identity: &SessionIdentity, key: &WorkKey) -> Result<WorkView> {
        self.claim(identity, key)?.run()
    }
}

pub struct Execution {
    application: Arc<dyn Application>,
    expanding: bool,
    context: WorkContext,
}
impl Execution {
    pub fn lease(&self) -> Number {
        self.context.lease
    }
    pub fn attempt(&self) -> Id {
        self.context.attempt
    }
    pub fn run(mut self) -> Result<WorkView> {
        // Claims may be held by a host queue; check again before invoking code.
        self.context.check()?;
        if self.expanding {
            return branch::run(self);
        }
        let outcome = match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            self.application.execute(&mut self.context)
        })) {
            Ok(Ok(outcome)) => outcome,
            Ok(Err(error)) => ApplicationOutcome::Failed(diagnostic(&error)),
            Err(_) => ApplicationOutcome::Failed(diag(
                ErrorCode::InternalError,
                "application callback panicked",
            )),
        };
        self.context.publish(outcome)
    }
}

pub struct WorkContext {
    executor: Executor,
    identity: SessionIdentity,
    key: WorkKey,
    attempt: Id,
    lease: Number,
    input_descriptor: Input,
    mode: Mode,
    input: ObjectReader,
    reservation: OutputReservation,
    caps: Capabilities,
    pending: Option<OutputStaging>,
    produced: Vec<Output>,
    failure: Option<Diagnostic>,
    child_reader: Option<Arc<payload::ReadCredit>>,
    child_input: Option<ObjectReader>,
}
impl WorkContext {
    pub fn identity(&self) -> &SessionIdentity {
        &self.identity
    }
    pub fn work(&self) -> &WorkKey {
        &self.key
    }
    pub fn attempt(&self) -> Id {
        self.attempt
    }
    pub fn lease(&self) -> Number {
        self.lease
    }
    pub fn input_descriptor(&self) -> &Input {
        &self.input_descriptor
    }
    pub fn mode(&self) -> Mode {
        self.mode
    }
    pub fn buffer_limit(&self) -> usize {
        self.executor.payloads.chunk_limit()
    }

    fn record<T>(&mut self, result: Result<T>) -> Result<T> {
        if let Err(error) = &result {
            self.failure.get_or_insert_with(|| diagnostic(error));
        }
        result
    }
    fn check(&self) -> Result<()> {
        let mut connection = self.executor.store.connect()?;
        let tx = connection.transaction()?;
        checked(&self.executor.store, &tx, self).map(|_| ())
    }
    pub fn read_input(&mut self, bytes: &mut [u8]) -> Result<usize> {
        let result = self.check().and_then(|()| self.input.read_chunk(bytes));
        self.record(result)
    }
    pub fn begin_output(&mut self, maximum: Number, content_type: ApplicationLabel) -> Result<()> {
        let result = (|| {
            self.check()?;
            if self.pending.is_some() {
                return Err(protocol(ErrorCode::Conflict, "an output is already open"));
            }
            if self.produced.len() as u64 >= self.reservation.budget().count.0 {
                return Err(protocol(
                    ErrorCode::LimitExceeded,
                    "application output count exceeded",
                ));
            }
            self.pending = Some(self.reservation.stage(
                OutputIndex(self.produced.len() as u64),
                maximum,
                content_type,
                &self.caps,
                Instant::now(),
            )?);
            Ok(())
        })();
        self.record(result)
    }
    pub fn write_output(&mut self, bytes: &[u8]) -> Result<()> {
        let result = (|| {
            self.check()?;
            self.pending
                .as_mut()
                .ok_or_else(|| protocol(ErrorCode::Conflict, "no output is open"))?
                .write(bytes, Instant::now())
        })();
        self.record(result)
    }
    pub fn finish_output(&mut self) -> Result<OutputIndex> {
        let result = (|| {
            self.check()?;
            let installed = self
                .pending
                .take()
                .ok_or_else(|| protocol(ErrorCode::Conflict, "no output is open"))?
                .finish(Instant::now())?;
            let index = OutputIndex(self.produced.len() as u64);
            let input = installed.descriptor();
            self.produced.push(Output {
                index,
                length: input.length,
                sha256: input.sha256,
                content_type: input.content_type.clone(),
                locator: self.executor.endpoint.locator(
                    &self.identity,
                    &self.key,
                    self.attempt,
                    index,
                )?,
            });
            // The live reservation pins all installed outputs. Do not retain
            // one open handle per finished object (a manifest may contain 256).
            drop(installed);
            Ok(index)
        })();
        self.record(result)
    }
    pub fn renew(&mut self) -> Result<()> {
        let result = (|| {
            let mut connection = self.executor.store.connect()?;
            let tx = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let (row, mut job, revision, _, view, _, now) =
                checked(&self.executor.store, &tx, self)?;
            job.lease_until =
                Some(add_duration(now, self.executor.lease_ms)?.min(view.deadline.unwrap()));
            records::replace(&tx, job_target(row), revision, &job, false)?;
            self.executor.store.remember_clock(&tx, now)?;
            self.executor
                .store
                .authorize(&self.identity.owner, Permission::Execute)?;
            commit(tx, "worker-renew")
        })();
        self.record(result)
    }

    fn publish(mut self, mut outcome: ApplicationOutcome) -> Result<WorkView> {
        if self
            .child_input
            .take()
            .is_some_and(|reader| !reader.verified())
        {
            self.failure.get_or_insert_with(|| {
                diag(
                    ErrorCode::IntegrityError,
                    "child output was not read through verified EOF",
                )
            });
        }
        if self.pending.take().is_some() {
            self.failure.get_or_insert_with(|| {
                diag(
                    ErrorCode::IntegrityError,
                    "application left an unfinished output",
                )
            });
        }
        if let Some(error) = self.failure.take() {
            outcome = ApplicationOutcome::Failed(error);
        }
        if let ApplicationOutcome::Retryable(error) | ApplicationOutcome::Failed(error) = &outcome
            && error.check().is_err()
        {
            outcome = ApplicationOutcome::Failed(diag(
                ErrorCode::InternalError,
                "invalid application diagnostic",
            ));
        }
        let mut connection = self.executor.store.connect()?;
        let tx = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let (row, mut job, job_revision, binding, mut view, work_revision, now) =
            checked(&self.executor.store, &tx, &self)?;
        self.reservation.usage()?; // fail closed if the payload namespace is quarantined
        for output in &self.produced {
            // The reservation pins these already-fsynced immutable objects.
            // Validate their bindings without borrowing a new read handle just
            // to commit the promised manifest under concurrent handle pressure.
            self.reservation.verify_output(
                output.index,
                &Input {
                    length: output.length,
                    sha256: output.sha256,
                    content_type: output.content_type.clone(),
                },
            )?;
        }
        let retryable = matches!(outcome, ApplicationOutcome::Retryable(_));
        view.state = match &outcome {
            ApplicationOutcome::Succeeded => State::SUCCEEDED,
            ApplicationOutcome::Retryable(_) => State::AWAITING_RETRY,
            ApplicationOutcome::Failed(_) => State::FAILED,
        };
        view.diagnostic = match outcome {
            ApplicationOutcome::Succeeded => None,
            ApplicationOutcome::Retryable(error) | ApplicationOutcome::Failed(error) => Some(error),
        };
        if !retryable {
            view.terminal_at = Some(now);
            view.receipt_until = Some(add_duration(now, binding.policy.receipt_retention_ms)?);
        }
        if view.state == State::SUCCEEDED && binding.results {
            let until = add_duration(now, binding.policy.output_retention_ms)?;
            let total = self
                .produced
                .iter()
                .try_fold(0u64, |sum, output| sum.checked_add(output.length.0))
                .ok_or_else(|| protocol(ErrorCode::LimitExceeded, "output byte sum overflow"))?;
            if self.produced.len() as u64 > job.parameters.outputs.count.0
                || total > job.parameters.outputs.total_bytes.0
                || self
                    .produced
                    .iter()
                    .any(|output| output.length > job.object_limit)
            {
                return Err(StoreError::Corrupt("worker outputs exceed admitted budget"));
            }
            view.output_until = Some(until);
            view.manifest = Some(Manifest {
                version: Literal,
                authority: self.identity.authority.clone(),
                owner: self.identity.owner.clone(),
                generation: self.identity.generation,
                work: self.key.clone(),
                attempt: self.attempt,
                input_sha256: self.input_descriptor.sha256,
                committed_at: now,
                available_until: until,
                outputs: self.produced,
            });
        }
        view.validate_profiles(binding.results)?;
        job.stage = Number(if retryable { 3 } else { 4 });
        job.lease_until = None;
        job.executor_live = retryable;
        // Byte reservations stay charged until reference-safe retention cleanup
        // observes all callback/read/dependency handles drained.
        records::replace(&tx, work_target(row), work_revision, &view, true)?;
        records::replace(&tx, job_target(row), job_revision, &job, true)?;
        self.executor.store.remember_clock(&tx, now)?;
        self.executor
            .store
            .authorize(&self.identity.owner, Permission::Execute)?;
        commit(tx, "worker-publish")?;
        Ok(view)
    }
}

fn diag(code: ErrorCode, detail: &str) -> Diagnostic {
    Diagnostic {
        code: DiagnosticCode(code as u64),
        detail: Detail(detail.into()),
    }
}
fn diagnostic(error: &StoreError) -> Diagnostic {
    match error {
        StoreError::Protocol(error) => diag(error.code, error.detail),
        _ => diag(ErrorCode::InternalError, "application storage failure"),
    }
}
fn work_target(row: i64) -> records::Target {
    records::Target {
        table: records::Table::Work,
        row,
    }
}
fn job_target(row: i64) -> records::Target {
    records::Target {
        table: records::Table::Job,
        row,
    }
}
fn eligible(view: &WorkView) -> Result<()> {
    if view.state.is_terminal() {
        return Err(protocol(
            ErrorCode::AlreadyTerminal,
            "work already terminal",
        ));
    }
    if view.state == State::CANCELLING {
        return Err(protocol(ErrorCode::Cancelled, "work cancellation accepted"));
    }
    Ok(())
}
pub(super) fn bound_payloads(tx: &Transaction<'_>, payloads: &PayloadStore) -> Result<()> {
    let (binding, path): (Vec<u8>, Option<String>) =
        tx.query_row("SELECT store_id,payload_path FROM authority", [], |r| {
            Ok((r.get(0)?, r.get(1)?))
        })?;
    if binding != payloads.binding().as_bytes() || path.as_deref() != payloads.path().to_str() {
        return Err(StoreError::Corrupt(
            "worker payload root is not bound to authority",
        ));
    }
    Ok(())
}
type Loaded = (i64, jobs::JobRecord, Id, WorkView, Id);
pub(super) fn load(
    tx: &Transaction<'_>,
    identity: &SessionIdentity,
    key: &WorkKey,
) -> Result<Loaded> {
    let (work_revision, view) = scopes::work(tx, identity.generation, key)?;
    let row: Option<i64> = tx.query_row("SELECT j.work_row FROM jobs j JOIN work w ON w.row_id=j.work_row WHERE w.generation=?1 AND w.scope=?2 AND w.entity=?3",
        params![sql(identity.generation.0)?, sql(key.scope.0)?, sql(key.entity.0)?], |r| r.get(0)).optional()?;
    let row = row.ok_or_else(|| protocol(ErrorCode::NotReady, "work is not admitted"))?;
    let (record, job) = records::read(tx, job_target(row))?;
    Ok((row, job, record.revision, view, work_revision))
}
type Checked = (i64, jobs::JobRecord, Id, Binding, WorkView, Id, Number);
fn checked(store: &AuthorityStore, tx: &Transaction<'_>, context: &WorkContext) -> Result<Checked> {
    let binding = store.authorize_session(tx, &context.identity, Permission::Execute)?;
    let (row, job, job_revision, view, work_revision) = load(tx, &context.identity, &context.key)?;
    scopes::unfenced(tx, context.identity.generation, context.key.scope)?;
    eligible(&view)?;
    let now = store.check_clock(tx)?;
    if now
        >= view
            .deadline
            .ok_or(StoreError::Corrupt("job deadline missing"))?
    {
        return Err(protocol(
            ErrorCode::DeadlineExceeded,
            "execution deadline reached",
        ));
    }
    if job.attempt != context.attempt
        || job.lease != context.lease
        || job.stage != Number(1)
        || job.lease_until.is_none_or(|until| now >= until)
    {
        return Err(protocol(
            ErrorCode::Conflict,
            "worker lease is stale or expired",
        ));
    }
    Ok((row, job, job_revision, binding, view, work_revision, now))
}

#[cfg(test)]
mod tests;

// Test fixtures that exercise incomplete descendants need a deliberate real
// declaration without an input. Real expanding applications retain their own
// callback; this wrapper is never compiled into the product.
#[cfg(test)]
pub(super) fn fixture_application(inner: Arc<dyn Application>) -> Arc<dyn Application> {
    struct DeclaredChild(Arc<dyn Application>);
    impl Application for DeclaredChild {
        fn execute(&self, context: &mut WorkContext) -> Result<ApplicationOutcome> {
            self.0.execute(context)
        }
        fn expansion(&self) -> Option<&dyn Expansion> {
            self.0.expansion().or(Some(self))
        }
    }
    impl Expansion for DeclaredChild {
        fn expand(&self, context: &mut ExpansionContext<'_>) -> Result<ExpansionOutcome> {
            context.declare(context.operation(Id(1))?, &[Id(1)], true)?;
            Ok(ExpansionOutcome::Complete)
        }
    }
    Arc::new(DeclaredChild(inner))
}
