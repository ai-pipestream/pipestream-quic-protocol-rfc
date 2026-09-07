use super::*;
use crate::v2::authority::ingress::{
    InputPreparation, InputReception, PreparedInput, ValidatedInput,
};
use sha2::{Digest as _, Sha256};

pub trait Expansion: Send + Sync {
    /// Replay stable declaration/admission operations after recovery; seal the
    /// immutable child scope before Complete. Yield releases this worker so
    /// admitted children can run without an extra thread per waiting branch.
    fn expand(&self, context: &mut ExpansionContext<'_>) -> Result<ExpansionOutcome>;
}
pub enum ExpansionOutcome {
    Complete,
    Yield,
    Retryable(Diagnostic),
    Failed(Diagnostic),
}
pub struct ExpansionContext<'a> {
    context: &'a mut WorkContext,
    child: Id,
}

pub struct ChildPage {
    pub members: Vec<WorkKey>,
    pub more: bool,
}
impl WorkContext {
    /// Page only the admitted branch's closed direct child scope. This is an
    /// internal dependency read under the current parent worker, not a caller
    /// result lease or permission to traverse another branch.
    pub fn children(&self, after: Number, limit: PageLimit) -> Result<ChildPage> {
        after.check()?;
        limit.check()?;
        let mut connection = self.executor.store.connect()?;
        let tx = connection.transaction()?;
        let (_, _, _, _, view, _, _) = checked(&self.executor.store, &tx, self)?;
        let child = ready_child(&tx, &self.identity, &view)?;
        let mut statement = tx.prepare("SELECT entity FROM work WHERE generation=?1 AND scope=?2 AND entity>?3 ORDER BY entity LIMIT ?4")?;
        let mut rows = statement.query(params![
            sql(self.identity.generation.0)?,
            sql(child.scope.0)?,
            sql(after.0)?,
            sql(limit.0 + 1)?
        ])?;
        let mut members = Vec::with_capacity(limit.0 as usize);
        let mut more = false;
        while let Some(row) = rows.next()? {
            if members.len() == limit.0 as usize {
                more = true;
                break;
            }
            members.push(WorkKey {
                scope: Number(child.scope.0),
                producer: child.producer,
                entity: Id(number(row, 0)?),
            });
        }
        Ok(ChildPage { members, more })
    }
    pub fn begin_child_output(&mut self, entity: Id, index: OutputIndex) -> Result<Output> {
        let result = (|| {
            entity.check()?;
            index.check()?;
            if self.child_input.is_some() {
                return Err(protocol(
                    ErrorCode::Conflict,
                    "a child output is already open",
                ));
            }
            let mut connection = self.executor.store.connect()?;
            let tx = connection.transaction()?;
            let (_, _, _, _, view, _, _) = checked(&self.executor.store, &tx, self)?;
            let child = ready_child(&tx, &self.identity, &view)?;
            let key = WorkKey {
                scope: Number(child.scope.0),
                producer: child.producer,
                entity,
            };
            let (_, job, _, child_view, _) = load(&tx, &self.identity, &key)?;
            if child_view.state != State::SUCCEEDED || !job.outputs_live {
                return Err(protocol(
                    ErrorCode::OutputUnavailable,
                    "child output is not retained",
                ));
            }
            let output = child_view
                .manifest
                .as_ref()
                .and_then(|manifest| manifest.outputs.get(index.0 as usize))
                .ok_or_else(|| protocol(ErrorCode::NotFound, "child output index absent"))?
                .clone();
            let credit = self.child_reader.as_ref().ok_or_else(|| {
                protocol(ErrorCode::Conflict, "worker has no child reader capacity")
            })?;
            // Internal dependency retention can outlive external output expiry.
            // The retained child job and reader pin keep these exact bytes charged.
            let reader = credit.open_output(
                &job.reservation_key.0,
                index,
                &self.identity.owner,
                &Input {
                    length: output.length,
                    sha256: output.sha256,
                    content_type: output.content_type.clone(),
                },
            )?;
            self.child_input = Some(reader);
            Ok(output)
        })();
        self.record(result)
    }
    pub fn read_child_output(&mut self, bytes: &mut [u8]) -> Result<usize> {
        let result = self.check().and_then(|()| {
            self.child_input
                .as_mut()
                .ok_or_else(|| protocol(ErrorCode::Conflict, "no child output is open"))?
                .read_chunk(bytes)
        });
        self.record(result)
    }
    pub fn finish_child_output(&mut self) -> Result<()> {
        let result = (|| {
            self.check()?;
            let reader = self
                .child_input
                .take()
                .ok_or_else(|| protocol(ErrorCode::Conflict, "no child output is open"))?;
            if !reader.verified() {
                return Err(protocol(
                    ErrorCode::IntegrityError,
                    "child output lacks verified EOF",
                ));
            }
            Ok(())
        })();
        self.record(result)
    }
}

fn ready_child(
    tx: &Transaction<'_>,
    identity: &SessionIdentity,
    view: &WorkView,
) -> Result<ChildScope> {
    let child = view
        .child
        .clone()
        .ok_or_else(|| protocol(ErrorCode::Conflict, "leaf has no child scope"))?;
    let summary = scopes::closed(tx, identity.generation, Number(child.scope.0))?
        .ok_or_else(|| protocol(ErrorCode::NotReady, "child scope is not closed"))?;
    if summary.counts.success != summary.declared {
        return Err(protocol(ErrorCode::NotReady, "child scope did not succeed"));
    }
    Ok(child)
}
impl ExpansionContext<'_> {
    pub fn identity(&self) -> &SessionIdentity {
        &self.context.identity
    }
    pub fn work(&self) -> &WorkKey {
        &self.context.key
    }
    pub fn child_scope(&self) -> Id {
        self.child
    }
    pub fn input_descriptor(&self) -> &Input {
        self.context.input_descriptor()
    }
    pub fn buffer_limit(&self) -> usize {
        self.context.buffer_limit()
    }
    /// Original parent admission duration, not a renewed parent deadline.
    pub fn execution_duration(&self) -> Duration {
        self.context.execution_duration()
    }
    pub fn read_input(&mut self, bytes: &mut [u8]) -> Result<usize> {
        self.context.read_input(bytes)
    }
    pub fn renew(&mut self) -> Result<()> {
        self.context.renew()
    }
    /// Stable local operation identity across worker leases and parent retries.
    /// Applications assign distinct stable sequence numbers to their operations.
    pub fn operation(&self, sequence: Id) -> Result<OperationId> {
        sequence.check()?;
        let mut hash = Sha256::new();
        hash.update(b"pipestream-local-operation-v2");
        hash.update(pack(&self.context.identity.authority)?);
        hash.update(pack(&self.context.identity.owner)?);
        hash.update(pack(&self.context.identity.generation)?);
        hash.update(pack(&self.child)?);
        hash.update(pack(&sequence)?);
        let digest = hash.finalize();
        let mut bytes = [0; 16];
        bytes.copy_from_slice(&digest[..16]);
        Ok(OperationId(bytes))
    }
    fn origin(&self) -> Origin {
        Origin::Worker {
            identity: self.context.identity.clone(),
            parent: self.context.key.clone(),
            child: self.child,
            attempt: self.context.attempt,
            lease: self.context.lease,
        }
    }
    pub fn declare(
        &self,
        operation: OperationId,
        entities: &[Id],
        seal: bool,
    ) -> Result<OperationReceipt> {
        self.context.executor.store.declare_as(
            &self.context.identity,
            operation,
            Number(self.child.0),
            entities,
            seal,
            &self.origin(),
        )
    }
    pub fn receive_input(
        &self,
        operation: OperationId,
        parameters: AdmitParameters,
        now: Instant,
    ) -> Result<InputReception> {
        let header = InputHeader {
            kind: Literal,
            generation: self.context.identity.generation,
            operation,
            parameters,
        };
        self.context.executor.store.receive_input_as(
            (&self.context.identity, self.origin()),
            &header,
            &self.context.caps,
            &self.context.executor.payloads,
            &self.context.executor.applications,
            now,
        )
    }
    pub fn prepare_input(&self, input: ValidatedInput) -> Result<InputPreparation> {
        self.check_origin(&input.origin)?;
        self.context.executor.store.prepare_input(
            input,
            &self.context.caps,
            &self.context.executor.applications,
        )
    }
    pub fn admit_input(&self, prepared: PreparedInput) -> Result<OperationReceipt> {
        self.check_origin(&prepared.input.origin)?;
        self.context.executor.store.admit_input(
            prepared,
            &self.context.caps,
            &self.context.executor.applications,
        )
    }
    fn check_origin(&self, origin: &Origin) -> Result<()> {
        if origin != &self.origin() {
            return Err(protocol(
                ErrorCode::Unauthorized,
                "input is not from this expansion worker",
            ));
        }
        self.context.check()
    }
}

pub(super) fn run(mut execution: Execution) -> Result<WorkView> {
    let child = {
        let mut connection = execution.context.executor.store.connect()?;
        let tx = connection.transaction()?;
        let (_, _, _, _, view, _, _) =
            checked(&execution.context.executor.store, &tx, &execution.context)?;
        view.child
            .ok_or(StoreError::Corrupt("expansion child missing"))?
            .scope
    };
    let expansion = execution.application.expansion().ok_or_else(|| {
        protocol(
            ErrorCode::ApplicationUnsupported,
            "expansion callback missing",
        )
    })?;
    let outcome = match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        expansion.expand(&mut ExpansionContext {
            context: &mut execution.context,
            child,
        })
    })) {
        Ok(Ok(outcome)) => outcome,
        Ok(Err(error)) => ExpansionOutcome::Failed(diagnostic(&error)),
        Err(_) => ExpansionOutcome::Failed(diag(
            ErrorCode::InternalError,
            "expansion callback panicked",
        )),
    };
    finish(execution.context, child, outcome)
}

fn finish(context: WorkContext, child: Id, outcome: ExpansionOutcome) -> Result<WorkView> {
    let outcome = if let Some(error) = &context.failure {
        ExpansionOutcome::Failed(error.clone())
    } else {
        outcome
    };
    let complete = match outcome {
        ExpansionOutcome::Failed(error) => {
            return context.publish(ApplicationOutcome::Failed(error));
        }
        ExpansionOutcome::Retryable(error) => {
            return context.publish(ApplicationOutcome::Retryable(error));
        }
        ExpansionOutcome::Complete => true,
        ExpansionOutcome::Yield => false,
    };
    let mut connection = context.executor.store.connect()?;
    let tx = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let (row, mut job, job_revision, _, mut view, work_revision, now) =
        checked(&context.executor.store, &tx, &context)?;
    let sealed = scopes::load(&tx, context.identity.generation, Number(child.0))?
        .seal
        .is_some();
    if complete && !sealed {
        drop(tx);
        return context.publish(ApplicationOutcome::Failed(diag(
            ErrorCode::Conflict,
            "expansion completed without sealing membership",
        )));
    }
    job.stage = Number(if complete { 2 } else { 0 });
    job.expansion_complete = complete;
    job.lease_until = None;
    if complete {
        view.state = State::WAITING_CHILDREN;
        records::replace(&tx, work_target(row), work_revision, &view, true)?;
    }
    // Repeated voluntary yields are ordinary writes and cannot spend the last
    // credits reserved for terminal/deadline settlement.
    records::replace(&tx, job_target(row), job_revision, &job, complete)?;
    context.executor.store.remember_clock(&tx, now)?;
    context
        .executor
        .store
        .authorize(&context.identity.owner, Permission::Execute)?;
    commit(tx, "worker-expansion")?;
    Ok(view)
}
