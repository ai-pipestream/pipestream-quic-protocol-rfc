use super::*;

struct RetainedScope {
    producer: Producer,
    parent: Option<WorkKey>,
    last_entity: u64,
    declared: Number,
    seal: Option<Digest>,
    cancelled: bool,
    summary: Option<ScopeSummary>,
}

fn load(tx: &Transaction<'_>, generation: Id, scope: Number) -> Result<RetainedScope> {
    let mut statement = tx.prepare("SELECT producer,parent,last_entity,declared,seal,cancelled,summary FROM scopes WHERE generation=?1 AND scope=?2")?;
    let mut rows = statement.query(params![sql(generation.0)?, sql(scope.0)?])?;
    let Some(row) = rows.next()? else {
        return Err(protocol(ErrorCode::NotFound, "scope not declared"));
    };
    let parent = row
        .get::<_, Option<Vec<u8>>>(1)?
        .map(|b| unpack(&b))
        .transpose()?;
    let seal = row
        .get::<_, Option<Vec<u8>>>(4)?
        .map(|b| {
            b.try_into()
                .map(Digest)
                .map_err(|_| StoreError::Corrupt("invalid retained seal"))
        })
        .transpose()?;
    let summary = row
        .get::<_, Option<Vec<u8>>>(6)?
        .map(|b| unpack(&b))
        .transpose()?;
    Ok(RetainedScope {
        producer: Producer(number(row, 0)?),
        parent,
        last_entity: number(row, 2)?,
        declared: Number(number(row, 3)?),
        seal,
        cancelled: row.get(5)?,
        summary,
    })
}

/// Inspect one scope at a time. Child scope numbers strictly exceed parents;
/// corrupt ancestry cannot cycle or create an unbounded recursive stack.
pub(super) fn unfenced(tx: &Transaction<'_>, generation: Id, mut scope: Number) -> Result<()> {
    loop {
        let retained = load(tx, generation, scope)?;
        if retained.cancelled {
            return Err(protocol(
                ErrorCode::Cancelled,
                "scope cancellation fence accepted",
            ));
        }
        let Some(parent) = retained.parent else {
            return Ok(());
        };
        if parent.scope >= scope {
            return Err(StoreError::Corrupt("invalid scope ancestry"));
        }
        let (_, view) = work(tx, generation, &parent)?;
        if matches!(
            view.state,
            State::CANCELLING | State::CANCELLED | State::SKIPPED
        ) {
            return Err(protocol(
                ErrorCode::Cancelled,
                "ancestor cancellation fence accepted",
            ));
        }
        scope = parent.scope;
    }
}

pub(super) fn operation(
    tx: &Transaction<'_>,
    generation: Id,
    originator: Producer,
    id: OperationId,
) -> Result<Option<OperationReceipt>> {
    let retained: Option<(Vec<u8>, Vec<u8>)> = tx.query_row(
        "SELECT digest,receipt FROM operations WHERE generation=?1 AND originator=?2 AND operation=?3",
        params![sql(generation.0)?, sql(originator.0)?, id.0.as_slice()], |r| Ok((r.get(0)?, r.get(1)?))).optional()?;
    let Some((digest, receipt)) = retained else {
        return Ok(None);
    };
    let receipt: OperationReceipt = unpack(&receipt)?;
    if receipt.operation != id || receipt.request_digest.0.as_slice() != digest {
        return Err(StoreError::Corrupt("operation index and receipt disagree"));
    }
    Ok(Some(receipt))
}

pub(super) fn work(tx: &Transaction<'_>, generation: Id, key: &WorkKey) -> Result<(Id, WorkView)> {
    let retained: Option<(u64, u64, Vec<u8>)> = tx.query_row(
        "SELECT revision,producer,view FROM work WHERE generation=?1 AND scope=?2 AND entity=?3",
        params![sql(generation.0)?, sql(key.scope.0)?, sql(key.entity.0)?], |r| Ok((number(r, 0)?, number(r, 1)?, r.get(2)?))).optional()?;
    let Some((revision, producer, bytes)) = retained else {
        return Err(protocol(ErrorCode::NotFound, "work not declared"));
    };
    if producer != key.producer.0 {
        return Err(protocol(ErrorCode::Conflict, "work producer mismatch"));
    }
    let view: WorkView = unpack(&bytes)?;
    if view.work != *key {
        return Err(StoreError::Corrupt("work index and view disagree"));
    }
    Ok((Id(revision), view))
}

fn seal(
    tx: &Transaction<'_>,
    identity: &SessionIdentity,
    scope: Number,
    retained: &RetainedScope,
) -> Result<Digest> {
    let mut hash = ScopeSeal::new(
        identity,
        scope,
        retained.producer,
        retained.parent.as_ref(),
        retained.declared,
    )?;
    let mut statement =
        tx.prepare("SELECT entity FROM work WHERE generation=?1 AND scope=?2 ORDER BY entity")?;
    let mut rows = statement.query(params![sql(identity.generation.0)?, sql(scope.0)?])?;
    while let Some(row) = rows.next()? {
        hash.push(Id(number(row, 0)?))?;
    }
    Ok(hash.finish()?)
}

impl AuthorityStore {
    /// Normal caller declaration. Authority-generated membership is not exposed
    /// through this interface; its local executor must authorize producer one.
    pub fn declare(
        &self,
        identity: &SessionIdentity,
        id: OperationId,
        scope: Number,
        entity_ids: &[Id],
        sealed: bool,
    ) -> Result<OperationReceipt> {
        require(entity_ids.len() <= 256, "declaration batch exceeds 256 IDs")?;
        let mutation = Mutation::Declare {
            scope,
            entity_ids: entity_ids.to_vec(),
            seal: sealed,
        };
        let digest = mutation.digest(identity, Producer(0), id)?;
        let mut connection = self.connect()?;
        let tx = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let binding = self.authorize_session(&tx, identity, Permission::Declare)?;
        if let Some(receipt) = operation(&tx, identity.generation, Producer(0), id)? {
            if receipt.request_digest != digest {
                return Err(protocol(
                    ErrorCode::Conflict,
                    "operation parameters changed",
                ));
            }
            return Ok(receipt);
        }
        let mut retained = load(&tx, identity.generation, scope)?;
        if retained.producer != Producer(0) {
            return Err(protocol(
                ErrorCode::Unauthorized,
                "external producer cannot declare authority work",
            ));
        }
        unfenced(&tx, identity.generation, scope)?;
        if retained.seal.is_some()
            || entity_ids
                .first()
                .is_some_and(|id| id.0 <= retained.last_entity)
        {
            return Err(protocol(
                ErrorCode::Conflict,
                "membership is sealed or IDs did not increase",
            ));
        }
        let (entities, operations): (u64, u64) = tx.query_row(
            "SELECT entities,operations FROM sessions WHERE generation=?1",
            [sql(identity.generation.0)?],
            |r| Ok((number(r, 0)?, number(r, 1)?)),
        )?;
        let total = entities
            .checked_add(entity_ids.len() as u64)
            .filter(|n| *n <= binding.limits.entities.0)
            .ok_or_else(|| {
                protocol(
                    ErrorCode::LimitExceeded,
                    "session membership capacity exhausted",
                )
            })?;
        if operations >= binding.limits.operations.0 {
            return Err(protocol(
                ErrorCode::LimitExceeded,
                "session operation capacity exhausted",
            ));
        }
        let now = self.trusted_now(&tx)?;
        for entity in entity_ids {
            let view = WorkView {
                work: WorkKey {
                    scope,
                    producer: retained.producer,
                    entity: *entity,
                },
                state: State::DECLARED,
                attempt: Number(0),
                input: None,
                admitted_at: None,
                deadline: None,
                terminal_at: None,
                receipt_until: None,
                output_until: None,
                child: None,
                manifest: None,
                diagnostic: None,
            };
            tx.execute("INSERT INTO work(generation,scope,producer,entity,revision,view) VALUES(?1,?2,?3,?4,1,?5)",
                params![sql(identity.generation.0)?, sql(scope.0)?, sql(retained.producer.0)?, sql(entity.0)?, pack(&view)?])?;
        }
        retained.declared = Number(
            retained
                .declared
                .0
                .checked_add(entity_ids.len() as u64)
                .filter(|n| *n <= MAX_NUMBER)
                .ok_or_else(|| protocol(ErrorCode::LimitExceeded, "scope count exhausted"))?,
        );
        if let Some(last) = entity_ids.last() {
            retained.last_entity = last.0;
        }
        if sealed {
            retained.seal = Some(seal(&tx, identity, scope, &retained)?);
        }
        if sealed && retained.declared.0 == 0 {
            if scope == Number(0) {
                // Root closure promises its creation receipt for this entire
                // interval, even when no work was admitted. Check before the
                // seal, summary and operation receipt become authoritative.
                add_duration(now, binding.policy.receipt_retention_ms)?;
            }
            retained.summary = Some(ScopeSummary {
                scope,
                producer: retained.producer,
                parent: retained.parent.clone(),
                seal: retained.seal.expect("just sealed"),
                declared: Number(0),
                counts: Counts {
                    success: Number(0),
                    failure: Number(0),
                    cancelled: Number(0),
                    skipped: Number(0),
                },
                status_root: empty_status_root(),
                closed_at: now,
            });
        }
        let receipt = OperationReceipt {
            operation: id,
            request_digest: digest,
            body: Outcome::Declared {
                scope,
                producer: retained.producer,
                accepted_count: BatchCount(entity_ids.len() as u64),
                declared: retained.declared,
                seal: retained.seal,
            },
        };
        Control::Scope(Scope::Declared {
            request: Id(MAX_NUMBER),
            receipt: receipt.clone(),
        })
        .encode(binding.control_limit.0 as usize)?;
        tx.execute("UPDATE scopes SET last_entity=?3,declared=?4,seal=?5,summary=?6 WHERE generation=?1 AND scope=?2",
            params![sql(identity.generation.0)?, sql(scope.0)?, sql(retained.last_entity)?, sql(retained.declared.0)?,
                retained.seal.map(|d| d.0.to_vec()), retained.summary.as_ref().map(pack).transpose()?])?;
        tx.execute(
            "UPDATE sessions SET entities=?2,operations=operations+1 WHERE generation=?1",
            params![sql(identity.generation.0)?, sql(total)?],
        )?;
        tx.execute(
            "INSERT INTO operations VALUES(?1,0,?2,?3,?4)",
            params![
                sql(identity.generation.0)?,
                id.0.as_slice(),
                digest.0.as_slice(),
                pack(&receipt)?
            ],
        )?;
        self.authorize(&identity.owner, Permission::Declare)?;
        commit(tx, "declare")?;
        Ok(receipt)
    }

    pub fn operation(
        &self,
        identity: &SessionIdentity,
        id: OperationId,
    ) -> Result<OperationReceipt> {
        id.check()?;
        let mut connection = self.connect()?;
        let tx = connection.transaction()?;
        self.authorize_session(&tx, identity, Permission::Inspect)?;
        operation(&tx, identity.generation, Producer(0), id)?
            .ok_or_else(|| protocol(ErrorCode::NotFound, "operation not retained"))
    }

    /// Consistent snapshot used by the transport's bounded revision waiter.
    /// Equal revision is not a timeout error; the waiter chooses when to reply.
    pub fn work_view(
        &self,
        identity: &SessionIdentity,
        key: &WorkKey,
        after: Number,
    ) -> Result<(Id, WorkView)> {
        key.check()?;
        after.check()?;
        let mut connection = self.connect()?;
        let tx = connection.transaction()?;
        let binding = self.authorize_session(&tx, identity, Permission::Inspect)?;
        let result = work(&tx, identity.generation, key)?;
        result.1.validate_profiles(binding.results)?;
        if after.0 > result.0.0 {
            return Err(protocol(
                ErrorCode::Conflict,
                "watch revision is ahead of work",
            ));
        }
        Ok(result)
    }

    pub fn scope_page(
        &self,
        identity: &SessionIdentity,
        request: Id,
        scope: Number,
        after: Number,
        limit: PageLimit,
    ) -> Result<Control> {
        request.check()?;
        scope.check()?;
        after.check()?;
        limit.check()?;
        let mut connection = self.connect()?;
        let tx = connection.transaction()?;
        self.authorize_session(&tx, identity, Permission::Inspect)?;
        let retained = load(&tx, identity.generation, scope)?;
        let mut statement = tx.prepare("SELECT entity,view FROM work WHERE generation=?1 AND scope=?2 AND entity>?3 ORDER BY entity LIMIT ?4")?;
        let mut rows = statement.query(params![
            sql(identity.generation.0)?,
            sql(scope.0)?,
            sql(after.0)?,
            sql(limit.0 + 1)?
        ])?;
        let mut entries = Vec::with_capacity(limit.0 as usize);
        let mut more = false;
        while let Some(row) = rows.next()? {
            if entries.len() == limit.0 as usize {
                more = true;
                break;
            }
            let entity = Id(number(row, 0)?);
            let view: WorkView = unpack(&row.get::<_, Vec<u8>>(1)?)?;
            if view.work
                != (WorkKey {
                    scope,
                    producer: retained.producer,
                    entity,
                })
            {
                return Err(StoreError::Corrupt("page index and view disagree"));
            }
            entries.push(ScopeEntry {
                entity,
                state: view.state,
            });
        }
        Ok(Control::Scope(Scope::PageResponse {
            request,
            scope,
            producer: retained.producer,
            parent: retained.parent,
            sealed: retained.seal.is_some(),
            seal: retained.seal,
            declared: retained.declared,
            entries,
            more,
        }))
    }

    /// None means the expected sealed scope still has obligations. Only the
    /// connection-local waiter can turn expiration of its wait into WAIT_TIMEOUT.
    pub fn checkpoint(
        &self,
        identity: &SessionIdentity,
        scope: Number,
        expected: Digest,
    ) -> Result<Option<ScopeSummary>> {
        scope.check()?;
        let mut connection = self.connect()?;
        let tx = connection.transaction()?;
        self.authorize_session(&tx, identity, Permission::Inspect)?;
        let retained = load(&tx, identity.generation, scope)?;
        let Some(seal) = retained.seal else {
            return Err(protocol(ErrorCode::NotReady, "scope is not sealed"));
        };
        if seal != expected {
            return Err(protocol(ErrorCode::IntegrityError, "scope seal mismatch"));
        }
        Ok(retained.summary)
    }
}
