//! Durable fences and bounded reconciliation. Stopping a worker is not a wire
//! cancellation: only these committing transitions change authoritative outcomes.

use super::*;

mod reconcile;
pub use reconcile::{ReconcileCursor, ReconcileProgress};

#[cfg(all(test, unix))]
mod tests;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct WorkFence {
    pub outcome: State,
    pub operation: OperationId,
}
impl Wire for WorkFence {
    fn read(d: &mut minicbor::Decoder<'_>) -> std::result::Result<Self, Error> {
        codec::array(d, 2)?;
        let fence = Self {
            outcome: State::read(d)?,
            operation: OperationId::read(d)?,
        };
        fence.check()?;
        Ok(fence)
    }
    fn write(&self, w: &mut codec::Writer) {
        w.array(2);
        self.outcome.write(w);
        self.operation.write(w);
    }
    fn check(&self) -> std::result::Result<(), Error> {
        self.operation.check()?;
        require(
            matches!(self.outcome, State::CANCELLED | State::SKIPPED),
            "invalid work fence outcome",
        )
    }
}

pub(super) fn verify_fence(
    tx: &Transaction<'_>,
    identity: &SessionIdentity,
    view: &WorkView,
    fence: &WorkFence,
    greatest: Number,
) -> Result<()> {
    let receipt = scopes::operation(tx, identity.generation, Producer(0), fence.operation)?
        .ok_or(StoreError::Corrupt("work fence receipt missing"))?;
    let mutation = if fence.outcome == State::SKIPPED {
        Mutation::Skip {
            work: view.work.clone(),
        }
    } else {
        Mutation::Cancel {
            work: view.work.clone(),
        }
    };
    let valid = match &receipt.body {
        Outcome::Cancelled {
            work,
            accepted_at,
            disposition,
            state_at_commit,
        } if fence.outcome == State::CANCELLED => {
            work == &view.work
                && disposition.0 == 0
                && *accepted_at <= greatest
                && view
                    .terminal_at
                    .is_none_or(|terminal| *accepted_at <= terminal)
                && (*state_at_commit == State::CANCELLING || *state_at_commit == view.state)
        }
        Outcome::Skipped {
            work,
            accepted_at,
            disposition,
            state_at_commit,
        } if fence.outcome == State::SKIPPED => {
            work == &view.work
                && disposition.0 == 0
                && *accepted_at <= greatest
                && view
                    .terminal_at
                    .is_none_or(|terminal| *accepted_at <= terminal)
                && (*state_at_commit == State::CANCELLING || *state_at_commit == view.state)
        }
        _ => false,
    };
    if !valid
        || receipt.request_digest != mutation.digest(identity, Producer(0), fence.operation)?
    {
        return Err(StoreError::Corrupt("work fence receipt binding changed"));
    }
    Ok(())
}

fn target(table: records::Table, row: i64) -> records::Target {
    records::Target { table, row }
}

fn find_work(
    tx: &Transaction<'_>,
    identity: &SessionIdentity,
    key: &WorkKey,
) -> Result<(i64, Id, WorkView)> {
    let (revision, view) = scopes::work(tx, identity.generation, key)?;
    let row = tx.query_row(
        "SELECT row_id FROM work WHERE generation=?1 AND scope=?2 AND entity=?3",
        params![
            sql(identity.generation.0)?,
            sql(key.scope.0)?,
            sql(key.entity.0)?
        ],
        |row| row.get(0),
    )?;
    Ok((row, revision, view))
}

fn operation_replay(
    tx: &Transaction<'_>,
    identity: &SessionIdentity,
    operation: OperationId,
    digest: Digest,
) -> Result<Option<OperationReceipt>> {
    let receipt = scopes::operation(tx, identity.generation, Producer(0), operation)?;
    if receipt
        .as_ref()
        .is_some_and(|receipt| receipt.request_digest != digest)
    {
        return Err(protocol(
            ErrorCode::Conflict,
            "cancellation operation changed",
        ));
    }
    Ok(receipt)
}

fn operation_room(tx: &Transaction<'_>, binding: &Binding) -> Result<()> {
    let count = tx.query_row(
        "SELECT operations FROM sessions WHERE generation=?1",
        [sql(binding.identity.generation.0)?],
        |row| number(row, 0),
    )?;
    if count >= binding.limits.operations.0 {
        return Err(protocol(
            ErrorCode::LimitExceeded,
            "session operation capacity exhausted",
        ));
    }
    records::protect(tx, 0, 0)
}

fn retain_operation(
    tx: &Transaction<'_>,
    identity: &SessionIdentity,
    receipt: &OperationReceipt,
) -> Result<()> {
    records::protect(tx, 0, 0)?;
    operations::retain(tx, identity, Producer(0), receipt, None)?;
    tx.execute(
        "UPDATE sessions SET operations=operations+1 WHERE generation=?1",
        [sql(identity.generation.0)?],
    )?;
    Ok(())
}

fn diagnostic(code: ErrorCode, detail: &str) -> Diagnostic {
    Diagnostic {
        code: DiagnosticCode(code as u64),
        detail: Detail(detail.into()),
    }
}

struct TerminalOutcome {
    state: State,
    diagnostic: Option<Diagnostic>,
}

/// Terminal work/job/clock capacity was funded by declaration/admission. Keep
/// input/output liveness charged until the separate retention/pin cleanup.
fn terminal(
    tx: &Transaction<'_>,
    binding: &Binding,
    row: i64,
    revision: Id,
    mut view: WorkView,
    outcome: TerminalOutcome,
    now: Number,
) -> Result<()> {
    if view.state.is_terminal() {
        return Ok(());
    }
    view.state = outcome.state;
    view.terminal_at = Some(now);
    view.receipt_until = Some(add_duration(now, binding.policy.receipt_retention_ms)?);
    view.output_until = None;
    view.manifest = None;
    view.diagnostic = outcome.diagnostic;
    view.validate_profiles(binding.results)?;
    if view.admitted_at.is_some() {
        let job_target = target(records::Table::Job, row);
        let (header, mut job): (_, jobs::JobRecord) = records::read(tx, job_target)?;
        job.stage = Number(4);
        job.lease_until = None;
        job.executor_live = false;
        records::replace(tx, job_target, header.revision, &job, true)?;
    }
    records::replace(tx, target(records::Table::Work, row), revision, &view, true)?;
    Ok(())
}

fn child_closed(tx: &Transaction<'_>, identity: &SessionIdentity, view: &WorkView) -> Result<bool> {
    match &view.child {
        Some(child) => Ok(
            scopes::load(tx, identity.generation, Number(child.scope.0))?
                .summary
                .is_some(),
        ),
        None => Ok(true),
    }
}

fn scope_state(retained: &scopes::RetainedScope) -> scopes::ScopeState {
    scopes::ScopeState {
        last_entity: Number(retained.last_entity),
        declared: retained.declared,
        seal: retained.seal,
        cancelled: retained.cancelled,
        revoked: retained.revoked,
        summary: retained.summary.clone(),
    }
}

/// Freeze membership immediately. Computing its seal and descendant closure may
/// take bounded background batches, but the ancestor fence already excludes writes.
fn scope_fence(
    tx: &Transaction<'_>,
    identity: &SessionIdentity,
    scope: Number,
    revoke: bool,
) -> Result<bool> {
    let retained = scopes::load(tx, identity.generation, scope)?;
    if retained.cancelled && (!revoke || retained.revoked) {
        return Ok(false);
    }
    let mut state = scope_state(&retained);
    state.cancelled = true;
    state.revoked |= revoke;
    records::replace(
        tx,
        retained.summary_record,
        retained.summary_revision,
        &state,
        true,
    )?;
    Ok(true)
}

impl AuthorityStore {
    pub fn cancel_work(
        &self,
        identity: &SessionIdentity,
        operation: OperationId,
        key: &WorkKey,
    ) -> Result<OperationReceipt> {
        self.work_fence(identity, operation, key, false)
    }
    pub fn skip_work(
        &self,
        identity: &SessionIdentity,
        operation: OperationId,
        key: &WorkKey,
    ) -> Result<OperationReceipt> {
        self.work_fence(identity, operation, key, true)
    }
    fn work_fence(
        &self,
        identity: &SessionIdentity,
        operation: OperationId,
        key: &WorkKey,
        skip: bool,
    ) -> Result<OperationReceipt> {
        let mutation = if skip {
            Mutation::Skip { work: key.clone() }
        } else {
            Mutation::Cancel { work: key.clone() }
        };
        let digest = mutation.digest(identity, Producer(0), operation)?;
        let permission = if skip {
            Permission::Skip
        } else {
            Permission::Cancel
        };
        let mut connection = self.connect()?;
        let tx = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let binding = self.authorize_session(&tx, identity, permission)?;
        if let Some(receipt) = operation_replay(&tx, identity, operation, digest)? {
            return Ok(receipt);
        }
        let (row, revision, mut view) = find_work(&tx, identity, key)?;
        operation_room(&tx, &binding)?;
        let now = self.check_clock(&tx)?;
        let outcome = if skip {
            State::SKIPPED
        } else {
            State::CANCELLED
        };
        let disposition = Disposition(u64::from(view.state.is_terminal()));
        if !view.state.is_terminal() {
            scopes::unfenced(&tx, identity.generation, key.scope)?;
            let fence_target = target(records::Table::WorkFence, row);
            let (header, fence): (_, Option<WorkFence>) = records::read(&tx, fence_target)?;
            if let Some(fence) = fence {
                if fence.outcome != outcome {
                    return Err(protocol(
                        ErrorCode::Cancelled,
                        "first work fence has another outcome",
                    ));
                }
            } else {
                add_duration(now, binding.policy.receipt_retention_ms)?;
                records::replace(
                    &tx,
                    fence_target,
                    header.revision,
                    &Some(WorkFence { outcome, operation }),
                    true,
                )?;
                if child_closed(&tx, identity, &view)? {
                    terminal(
                        &tx,
                        &binding,
                        row,
                        revision,
                        view.clone(),
                        TerminalOutcome {
                            state: outcome,
                            diagnostic: None,
                        },
                        now,
                    )?;
                    view.state = outcome;
                } else {
                    view.state = State::CANCELLING;
                    view.diagnostic = None;
                    records::replace(
                        &tx,
                        target(records::Table::Work, row),
                        revision,
                        &view,
                        true,
                    )?;
                }
            }
        }
        let body = if skip {
            Outcome::Skipped {
                work: key.clone(),
                accepted_at: now,
                disposition,
                state_at_commit: view.state,
            }
        } else {
            Outcome::Cancelled {
                work: key.clone(),
                accepted_at: now,
                disposition,
                state_at_commit: view.state,
            }
        };
        let receipt = OperationReceipt {
            operation,
            request_digest: digest,
            body,
        };
        let response = if skip {
            Work::Skipped {
                request: Id(MAX_NUMBER),
                receipt: receipt.clone(),
            }
        } else {
            Work::Cancelled {
                request: Id(MAX_NUMBER),
                receipt: receipt.clone(),
            }
        };
        Control::Work(response).encode(binding.control_limit.0 as usize)?;
        retain_operation(&tx, identity, &receipt)?;
        self.remember_clock(&tx, now)?;
        self.authorize(&identity.owner, permission)?;
        commit(tx, "work-fence")?;
        Ok(receipt)
    }

    pub fn cancel_scope(
        &self,
        identity: &SessionIdentity,
        operation: OperationId,
        scope: Number,
    ) -> Result<OperationReceipt> {
        let digest = Mutation::ScopeCancel { scope }.digest(identity, Producer(0), operation)?;
        let mut connection = self.connect()?;
        let tx = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let binding = self.authorize_session(&tx, identity, Permission::Cancel)?;
        if let Some(receipt) = operation_replay(&tx, identity, operation, digest)? {
            return Ok(receipt);
        }
        operation_room(&tx, &binding)?;
        let now = self.check_clock(&tx)?;
        add_duration(now, binding.policy.receipt_retention_ms)?;
        scope_fence(&tx, identity, scope, false)?;
        let receipt = OperationReceipt {
            operation,
            request_digest: digest,
            body: Outcome::ScopeCancelled {
                scope,
                accepted_at: now,
            },
        };
        Control::Scope(Scope::Cancelled {
            request: Id(MAX_NUMBER),
            receipt: receipt.clone(),
        })
        .encode(binding.control_limit.0 as usize)?;
        retain_operation(&tx, identity, &receipt)?;
        self.remember_clock(&tx, now)?;
        self.authorize(&identity.owner, Permission::Cancel)?;
        commit(tx, "scope-fence")?;
        Ok(receipt)
    }

    /// Local operator action, not an unauthenticated wire RPC. The authorization
    /// provider must explicitly permit Revoke independently of caller access.
    pub fn revoke_session(&self, identity: &SessionIdentity) -> Result<()> {
        self.authorize(&identity.owner, Permission::Revoke)?;
        identity.authority.check()?;
        identity.owner.check()?;
        identity.generation.check()?;
        if identity.authority != self.authority {
            return Err(protocol(ErrorCode::Unauthorized, "authority access denied"));
        }
        let mut connection = self.connect()?;
        let tx = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let owner: Option<String> = tx
            .query_row(
                "SELECT owner FROM sessions WHERE generation=?1",
                [sql(identity.generation.0)?],
                |row| row.get(0),
            )
            .optional()?;
        if owner.as_ref() != Some(&identity.owner.0) {
            return Err(protocol(ErrorCode::Unauthorized, "authority access denied"));
        }
        let (binding, _) = sessions::load(&tx, &self.authority, identity.generation)?
            .ok_or_else(|| protocol(ErrorCode::NotFound, "session not retained"))?;
        let now = self.check_clock(&tx)?;
        add_duration(now, binding.policy.receipt_retention_ms)?;
        if scope_fence(&tx, identity, Number(0), true)? {
            self.remember_clock(&tx, now)?;
        }
        self.authorize(&identity.owner, Permission::Revoke)?;
        commit(tx, "session-revoke")
    }
}
