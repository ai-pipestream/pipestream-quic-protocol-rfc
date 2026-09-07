use super::{
    ingress::{Applications, PreparedInput, response_capacity},
    *,
};

impl AuthorityStore {
    /// Commit a validated external input, immutable receipt, restartable job,
    /// child scope and their reservations together. No application is invoked
    /// here. A caller may acknowledge only the returned committed receipt;
    /// an uncertain commit is resolved by replaying the same operation.
    pub fn admit_input(
        &self,
        prepared: PreparedInput,
        caps: &Capabilities,
        applications: &Applications,
    ) -> Result<OperationReceipt> {
        let PreparedInput { input, outputs } = prepared;
        let identity = &input.identity;
        let header = &input.header;
        let parameters = &header.parameters;
        let mut connection = self.connect()?;
        let mut tx = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        if let Some(receipt) = self.check_input(
            &tx,
            (identity, header, &input.origin),
            caps,
            input.payload.store(),
            applications,
        )? {
            return Ok(receipt);
        }
        let permission = input.origin.permission(Permission::Admit);
        let binding = self.authorize_session(&tx, identity, permission)?;
        if input.payload.owner() != &identity.owner
            || input.payload.descriptor() != &parameters.input
            || outputs.owner() != &identity.owner
            || outputs.budget() != &parameters.outputs
            || outputs.binding() != input.payload.binding()
            || outputs.store().path() != input.payload.store().path()
        {
            return Err(StoreError::Corrupt("prepared admission evidence changed"));
        }
        input.payload.check_owned()?;
        input.payload.store().check_execution_capacity(parameters)?;
        outputs.usage()?; // also rejects a quarantined or inherited reservation root
        jobs::capacity(&tx, &self.policy, &binding, parameters)?;
        let (operations, last_scope): (u64, u64) = tx.query_row(
            "SELECT operations,last_scope FROM sessions WHERE generation=?1",
            [sql(identity.generation.0)?],
            |r| Ok((number(r, 0)?, number(r, 1)?)),
        )?;
        if operations >= binding.limits.operations.0 {
            return Err(protocol(
                ErrorCode::LimitExceeded,
                "session operation capacity exhausted",
            ));
        }
        let child = if parameters.mode.0 != 0 {
            let scope = increment(last_scope)?;
            if scope >= binding.limits.scopes.0 {
                return Err(protocol(
                    ErrorCode::LimitExceeded,
                    "session scope capacity exhausted",
                ));
            }
            Some(ChildScope {
                scope: Id(scope),
                producer: Producer(parameters.mode.0 - 1),
            })
        } else {
            None
        };
        let now = self.check_clock(&tx)?;
        let deadline = add_duration(now, parameters.execution_ms)?;
        // Check the largest retention addition reachable within this execution
        // interval before taking on the job's time-based promises.
        add_duration(deadline, binding.policy.receipt_retention_ms)?;
        add_duration(deadline, binding.policy.output_retention_ms)?;
        let key = &parameters.work;
        let target = records::Target {
            table: records::Table::Work,
            row: tx.query_row(
                "SELECT rowid FROM work WHERE generation=?1 AND scope=?2 AND entity=?3",
                params![
                    sql(identity.generation.0)?,
                    sql(key.scope.0)?,
                    sql(key.entity.0)?
                ],
                |r| r.get(0),
            )?,
        };
        let retained = records::header(&tx, target)?;
        let response = response_capacity(parameters.outputs.count)?.max(4096);
        records::grow(
            &mut tx,
            target,
            retained.revision,
            retained.capacity.max(response as usize),
            retained.credits.max(jobs::WORK_CREDITS),
        )?;
        if let Some(child) = &child {
            records::protect(&tx, records::SCOPE_CAPACITY, records::SCOPE_CREDITS)?;
            tx.execute(
                "INSERT INTO scopes(generation,scope,producer,parent,state) VALUES(?1,?2,?3,?4,zeroblob(?5))",
                params![sql(identity.generation.0)?, sql(child.scope.0)?, sql(child.producer.0)?, pack(key)?,
                    (records::HEADER_BYTES + records::SCOPE_CAPACITY) as i64],
            )?;
            records::initialize(
                &tx,
                records::Target {
                    table: records::Table::Scope,
                    row: tx.last_insert_rowid(),
                },
                &scopes::ScopeState::empty(),
                records::SCOPE_CAPACITY,
                records::SCOPE_CREDITS,
            )?;
        }
        let job = jobs::JobRecord {
            parameters: parameters.clone(),
            operation: header.operation,
            originator: input.origin.producer(),
            restart_safety: applications.safety(&parameters.application, parameters.mode)?,
            attempt: Id(1),
            lease: Number(0),
            lease_until: None,
            stage: Number(if parameters.mode == Mode(1) { 2 } else { 0 }),
            input_key: ApplicationLabel(input.payload.key().to_owned()),
            reservation_key: ApplicationLabel(outputs.key().to_owned()),
            object_limit: Number(binding.object_limit.0.min(caps.object_limit.0)),
            input_live: true,
            outputs_live: true,
            executor_live: true,
            expansion_complete: parameters.mode != Mode(2),
            release: None,
        };
        records::protect(&tx, jobs::CAPACITY, jobs::CREDITS)?;
        tx.execute(
            "INSERT INTO jobs(work_row,state) VALUES(?1,zeroblob(?2))",
            params![target.row, (records::HEADER_BYTES + jobs::CAPACITY) as i64],
        )?;
        records::initialize(
            &tx,
            records::Target {
                table: records::Table::Job,
                row: target.row,
            },
            &job,
            jobs::CAPACITY,
            jobs::CREDITS,
        )?;
        for (object_key, purpose) in [(input.payload.key(), 0), (outputs.key(), 2)] {
            tx.execute(
                "INSERT INTO payload_refs VALUES(?1,?2,?3,?4,?5)",
                params![
                    object_key,
                    sql(identity.generation.0)?,
                    sql(key.scope.0)?,
                    sql(key.entity.0)?,
                    purpose
                ],
            )?;
        }
        let view = WorkView {
            work: key.clone(),
            state: if parameters.mode == Mode(1) {
                State::WAITING_CHILDREN
            } else {
                State::ACTIVE
            },
            attempt: Number(1),
            input: Some(parameters.input.clone()),
            admitted_at: Some(now),
            deadline: Some(deadline),
            terminal_at: None,
            receipt_until: None,
            output_until: None,
            child: child.clone(),
            manifest: None,
            diagnostic: None,
        };
        records::replace(&tx, target, retained.revision, &view, false)?;
        let receipt = OperationReceipt {
            operation: header.operation,
            request_digest: Mutation::Admit(parameters.clone()).digest(
                identity,
                input.origin.producer(),
                header.operation,
            )?,
            body: Outcome::Admitted {
                work: key.clone(),
                attempt: Id(1),
                admitted_at: now,
                deadline,
                child: child.clone(),
            },
        };
        Control::Work(Work::Admitted {
            request: RequestTag::Input {
                stream: StreamId((1u64 << 62) - 2),
            },
            receipt: receipt.clone(),
        })
        .encode(binding.control_limit.0.min(caps.control_limit.0) as usize)?;
        tx.execute(
            "INSERT INTO operations VALUES(?1,?5,?2,?3,?4)",
            params![
                sql(identity.generation.0)?,
                header.operation.0.as_slice(),
                receipt.request_digest.0.as_slice(),
                pack(&receipt)?,
                sql(input.origin.producer().0)?
            ],
        )?;
        let required_object = parameters
            .input
            .length
            .0
            .max(parameters.outputs.total_bytes.0.min(job.object_limit.0));
        tx.execute("UPDATE sessions SET operations=operations+1,last_scope=?2,required_control=max(required_control,?3),required_object=max(required_object,?4) WHERE generation=?1",
            params![sql(identity.generation.0)?, sql(child.map_or(last_scope, |c| c.scope.0))?, sql(response)?, sql(required_object)?])?;
        self.remember_clock(&tx, now)?;
        self.authorize(&identity.owner, permission)?;
        commit(tx, "admit-input")?;
        Ok(receipt)
    }
}

#[cfg(test)]
mod tests;
