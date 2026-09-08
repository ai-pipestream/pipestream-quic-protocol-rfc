use super::*;

impl AuthorityStore {
    /// Explicit caller retry. Preserve input/child/deadline, fence the previous
    /// attempt, replenish its record credits and commit exactly one receipt.
    pub fn retry_work(
        &self,
        identity: &SessionIdentity,
        operation: OperationId,
        key: &WorkKey,
        expected_attempt: Id,
    ) -> Result<OperationReceipt> {
        let digest = Mutation::Retry {
            work: key.clone(),
            expected_attempt,
        }
        .digest(identity, Producer(0), operation)?;
        let mut connection = self.connect()?;
        let mut tx = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let binding = self.authorize_session(&tx, identity, Permission::Retry)?;
        if let Some(receipt) = scopes::operation(&tx, identity.generation, Producer(0), operation)?
        {
            if receipt.request_digest != digest {
                return Err(protocol(ErrorCode::Conflict, "retry operation changed"));
            }
            return Ok(receipt);
        }
        let (row, mut job, job_revision, mut view, work_revision) = load(&tx, identity, key)?;
        scopes::unfenced(&tx, identity.generation, key.scope)?;
        eligible(&view)?;
        if job.attempt != expected_attempt {
            return Err(protocol(ErrorCode::Conflict, "retry attempt changed"));
        }
        let now = self.check_clock(&tx)?;
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
        let operations = tx.query_row(
            "SELECT operations FROM sessions WHERE generation=?1",
            [sql(identity.generation.0)?],
            |r| number(r, 0),
        )?;
        if operations >= binding.limits.operations.0 {
            return Err(protocol(
                ErrorCode::LimitExceeded,
                "session operation capacity exhausted",
            ));
        }
        let attempt = Id(increment(expected_attempt.0)?);
        for (target, revision, credits) in [
            (work_target(row), work_revision, jobs::WORK_CREDITS),
            (job_target(row), job_revision, jobs::CREDITS),
        ] {
            let retained = records::header(&tx, target)?;
            records::grow(
                &mut tx,
                target,
                revision,
                retained.capacity,
                retained.credits.max(credits),
            )?;
        }
        job.attempt = attempt;
        job.lease_until = None;
        job.stage = Number(if job.parameters.mode == Mode(1) { 2 } else { 0 });
        view.attempt = Number(attempt.0);
        view.state = if job.stage == Number(2) {
            State::WAITING_CHILDREN
        } else {
            State::ACTIVE
        };
        view.diagnostic = None;
        records::replace(&tx, work_target(row), work_revision, &view, false)?;
        records::replace(&tx, job_target(row), job_revision, &job, false)?;
        let receipt = OperationReceipt {
            operation,
            request_digest: digest,
            body: Outcome::Retried {
                work: key.clone(),
                expected_attempt,
                replacement_attempt: attempt,
                accepted_at: now,
            },
        };
        Control::Work(Work::Retried {
            request: Id(MAX_NUMBER),
            receipt: receipt.clone(),
        })
        .encode(binding.control_limit.0 as usize)?;
        operations::retain(&tx, identity, Producer(0), &receipt, None)?;
        tx.execute(
            "UPDATE sessions SET operations=operations+1 WHERE generation=?1",
            [sql(identity.generation.0)?],
        )?;
        self.remember_clock(&tx, now)?;
        self.authorize(&identity.owner, Permission::Retry)?;
        commit(tx, "worker-retry")?;
        Ok(receipt)
    }
}
