use super::*;

/// Local scan progress only. Durable outcomes never depend on retaining this
/// cursor; restart may recompute an unfinished seal/status fold from stored rows.
#[derive(Default)]
pub struct ReconcileCursor {
    binding: Option<Vec<u8>>,
    work_before: Option<i64>,
    scope_before: Option<i64>,
    scan: Option<ScopeScan>,
}

#[derive(Debug, Default)]
pub struct ReconcileProgress {
    pub inspected_work: usize,
    pub settled_work: usize,
    pub inspected_members: usize,
    pub frozen_scopes: usize,
    pub sealed_scopes: usize,
    pub closed_scopes: usize,
}

struct ScopeScan {
    identity: SessionIdentity,
    scope: Number,
    producer: Producer,
    parent: Option<WorkKey>,
    declared: Number,
    after: Number,
    count: u64,
    seal_hash: Option<ScopeSeal>,
    seal: Option<Digest>,
    status: StatusRoot,
    counts: Counts,
}
impl ScopeScan {
    fn new(
        identity: SessionIdentity,
        scope: Number,
        retained: &scopes::RetainedScope,
    ) -> Result<Self> {
        Ok(Self {
            seal_hash: if retained.seal.is_none() {
                Some(ScopeSeal::new(
                    &identity,
                    scope,
                    retained.producer,
                    retained.parent.as_ref(),
                    retained.declared,
                )?)
            } else {
                None
            },
            identity,
            scope,
            producer: retained.producer,
            parent: retained.parent.clone(),
            declared: retained.declared,
            after: Number(0),
            count: 0,
            seal: retained.seal,
            status: StatusRoot::default(),
            counts: Counts {
                success: Number(0),
                failure: Number(0),
                cancelled: Number(0),
                skipped: Number(0),
            },
        })
    }
}

fn fenced(tx: &Transaction<'_>, identity: &SessionIdentity, scope: Number) -> Result<bool> {
    match scopes::unfenced(tx, identity.generation, scope) {
        Ok(()) => Ok(false),
        Err(StoreError::Protocol(error)) if error.code == ErrorCode::Cancelled => Ok(true),
        Err(error) => Err(error),
    }
}

impl AuthorityStore {
    /// Local reconciliation, independent of caller authorization (including after
    /// revocation). Each call visits at most `limit` work records and `limit`
    /// members of one scope. Existing credit audits remain streaming store scans.
    pub fn reconcile(
        &self,
        cursor: &mut ReconcileCursor,
        limit: usize,
    ) -> Result<ReconcileProgress> {
        if limit == 0 || limit > 256 {
            return Err(protocol(
                ErrorCode::LimitExceeded,
                "invalid reconciliation batch",
            ));
        }
        let mut progress = ReconcileProgress::default();
        let mut connection = self.connect()?;
        let tx = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let binding: Vec<u8> =
            tx.query_row("SELECT store_id FROM authority", [], |row| row.get(0))?;
        if cursor.binding.as_ref().is_some_and(|old| *old != binding) {
            return Err(protocol(
                ErrorCode::Conflict,
                "reconciliation cursor belongs to another authority",
            ));
        }
        cursor.binding = Some(binding);
        let now = self.check_clock(&tx)?;
        let mut statement = tx.prepare("SELECT w.row_id,w.generation,s.owner FROM work w JOIN sessions s ON s.generation=w.generation WHERE NOT EXISTS(SELECT 1 FROM retirements r WHERE r.generation=s.generation) AND (?1 IS NULL OR w.row_id<?1) ORDER BY w.row_id DESC LIMIT ?2")?;
        let mut rows = statement.query(params![cursor.work_before, limit as i64])?;
        let mut before = cursor.work_before;
        while let Some(row) = rows.next()? {
            let id = row.get(0)?;
            before = Some(id);
            progress.inspected_work += 1;
            let identity = SessionIdentity {
                authority: self.authority.clone(),
                owner: IdentityLabel(row.get(2)?),
                generation: Id(number(row, 1)?),
            };
            let (record, view): (_, WorkView) =
                records::read(&tx, target(records::Table::Work, id))?;
            if view.state.is_terminal() {
                continue;
            }
            let (binding, _) = sessions::load(&tx, &self.authority, identity.generation)?
                .ok_or(StoreError::Corrupt("work session missing"))?;
            let (_, own): (_, Option<WorkFence>) =
                records::read(&tx, target(records::Table::WorkFence, id))?;
            let cancelled = fenced(&tx, &identity, view.work.scope)?;
            let outcome = if own.is_some() || cancelled {
                if !child_closed(&tx, &identity, &view)? {
                    continue;
                }
                Some((own.map_or(State::CANCELLED, |fence| fence.outcome), None))
            } else if view.deadline.is_some_and(|deadline| now >= deadline) {
                Some((
                    State::FAILED,
                    Some(diagnostic(
                        ErrorCode::DeadlineExceeded,
                        "execution deadline reached",
                    )),
                ))
            } else if let Some(child) = &view.child {
                scopes::load(&tx, identity.generation, Number(child.scope.0))?
                    .summary
                    .filter(|summary| summary.counts.success != summary.declared)
                    .map(|_| {
                        (
                            State::FAILED,
                            Some(Diagnostic {
                                code: DiagnosticCode(0),
                                detail: Detail(
                                    "STRICT child scope contains nonsuccessful work".into(),
                                ),
                            }),
                        )
                    })
            } else {
                None
            };
            if let Some((outcome, diagnostic)) = outcome {
                terminal(
                    &tx,
                    &binding,
                    id,
                    record.revision,
                    view,
                    TerminalOutcome {
                        state: outcome,
                        diagnostic,
                    },
                    now,
                )?;
                progress.settled_work += 1;
            }
        }
        drop(rows);
        drop(statement);
        if progress.settled_work != 0 {
            self.remember_clock(&tx, now)?;
            commit(tx, "settlement-work")?;
        } else {
            tx.commit()?;
        }
        cursor.work_before = if progress.inspected_work < limit {
            None
        } else {
            before
        };

        // A failed scope pass discards only volatile hashing progress. Its fence,
        // membership and terminal prefix remain durable and may be reread safely.
        if let Err(error) = self.reconcile_scope(cursor, limit, &mut progress) {
            cursor.scan = None;
            cursor.scope_before = None;
            return Err(error);
        }
        Ok(progress)
    }

    fn reconcile_scope(
        &self,
        cursor: &mut ReconcileCursor,
        limit: usize,
        progress: &mut ReconcileProgress,
    ) -> Result<()> {
        let mut connection = self.connect()?;
        let tx = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let now = self.check_clock(&tx)?;
        let mut changed = false;
        let mut scan = if let Some(scan) = cursor.scan.take() {
            scan
        } else {
            let next: Option<(i64, u64, u64, String)> = tx.query_row(
                "SELECT s.rowid,s.generation,s.scope,se.owner FROM scopes s JOIN sessions se ON se.generation=s.generation WHERE NOT EXISTS(SELECT 1 FROM retirements r WHERE r.generation=se.generation) AND (?1 IS NULL OR s.rowid<?1) ORDER BY s.rowid DESC LIMIT 1",
                [cursor.scope_before], |row| Ok((row.get(0)?, number(row,1)?, number(row,2)?, row.get(3)?))).optional()?;
            let Some((row, generation, scope, owner)) = next else {
                cursor.scope_before = None;
                return Ok(());
            };
            cursor.scope_before = Some(row);
            let identity = SessionIdentity {
                authority: self.authority.clone(),
                owner: IdentityLabel(owner),
                generation: Id(generation),
            };
            let mut retained = scopes::load(&tx, identity.generation, Number(scope))?;
            if retained.summary.is_some() {
                return Ok(());
            }
            if !retained.cancelled && fenced(&tx, &identity, Number(scope))? {
                changed = scope_fence(&tx, &identity, Number(scope), false)?;
                progress.frozen_scopes += usize::from(changed);
                retained = scopes::load(&tx, identity.generation, Number(scope))?;
            }
            if retained.seal.is_none() && !retained.cancelled {
                return Ok(());
            }
            ScopeScan::new(identity, Number(scope), &retained)?
        };
        let active: bool = tx.query_row("SELECT EXISTS(SELECT 1 FROM sessions s WHERE s.generation=?1 AND NOT EXISTS(SELECT 1 FROM retirements r WHERE r.generation=s.generation))", [sql(scan.identity.generation.0)?], |r| r.get(0))?;
        if !active {
            cursor.scope_before = None;
            return Ok(());
        }
        let retained = scopes::load(&tx, scan.identity.generation, scan.scope)?;
        if retained.summary.is_some() {
            return Ok(());
        }
        if retained.declared != scan.declared
            || retained.producer != scan.producer
            || retained.parent != scan.parent
        {
            return Err(StoreError::Corrupt("frozen closure membership changed"));
        }
        let mut statement = tx.prepare("SELECT entity,rowid FROM work WHERE generation=?1 AND scope=?2 AND entity>?3 ORDER BY entity LIMIT ?4")?;
        let mut rows = statement.query(params![
            sql(scan.identity.generation.0)?,
            sql(scan.scope.0)?,
            sql(scan.after.0)?,
            limit as i64
        ])?;
        let mut waiting = false;
        while let Some(row) = rows.next()? {
            progress.inspected_members += 1;
            let entity = Id(number(row, 0)?);
            if let Some(hasher) = scan.seal_hash.as_mut() {
                hasher.push(entity)?;
            } else {
                let (_, view): (_, WorkView) =
                    records::read(&tx, target(records::Table::Work, row.get(1)?))?;
                if !view.state.is_terminal() {
                    waiting = true;
                    break;
                }
                let child_root = if let Some(child) = &view.child {
                    let child = scopes::load(&tx, scan.identity.generation, Number(child.scope.0))?;
                    let Some(summary) = child.summary else {
                        waiting = true;
                        break;
                    };
                    Some(summary.status_root)
                } else {
                    None
                };
                scan.status.push(
                    StatusLeaf {
                        work: view.work,
                        state: view.state,
                        attempt: view.attempt,
                        manifest_digest: view
                            .manifest
                            .as_ref()
                            .map(Manifest::digest)
                            .transpose()?,
                        child_status_root: child_root,
                    }
                    .digest()?,
                )?;
                let count = match view.state {
                    State::SUCCEEDED => &mut scan.counts.success,
                    State::FAILED => &mut scan.counts.failure,
                    State::CANCELLED => &mut scan.counts.cancelled,
                    State::SKIPPED => &mut scan.counts.skipped,
                    _ => unreachable!("checked terminal"),
                };
                count.0 = increment(count.0)?;
            }
            scan.after = Number(entity.0);
            scan.count = increment(scan.count)?;
        }
        drop(rows);
        drop(statement);
        if !waiting {
            let more: bool = tx.query_row(
                "SELECT EXISTS(SELECT 1 FROM work WHERE generation=?1 AND scope=?2 AND entity>?3)",
                params![
                    sql(scan.identity.generation.0)?,
                    sql(scan.scope.0)?,
                    sql(scan.after.0)?
                ],
                |row| row.get(0),
            )?;
            if more {
                cursor.scan = Some(scan);
            } else {
                if scan.count != scan.declared.0 {
                    return Err(StoreError::Corrupt(
                        "scope count differs from stored membership",
                    ));
                }
                let mut state = scope_state(&retained);
                if let Some(hash) = scan.seal_hash.take() {
                    let seal = hash.finish()?;
                    if state.seal.is_some_and(|old| old != seal) {
                        return Err(StoreError::Corrupt("scope seal changed"));
                    }
                    if state.seal.is_none() {
                        state.seal = Some(seal);
                        records::replace(
                            &tx,
                            retained.summary_record,
                            retained.summary_revision,
                            &state,
                            true,
                        )?;
                        changed = true;
                        progress.sealed_scopes += 1;
                    }
                    let retained = scopes::load(&tx, scan.identity.generation, scan.scope)?;
                    cursor.scan = Some(ScopeScan::new(scan.identity, scan.scope, &retained)?);
                } else {
                    let (binding, _) =
                        sessions::load(&tx, &self.authority, scan.identity.generation)?
                            .ok_or(StoreError::Corrupt("scope session absent"))?;
                    if scan.scope.0 == 0 {
                        add_duration(now, binding.policy.receipt_retention_ms)?;
                    }
                    state.summary = Some(ScopeSummary {
                        scope: scan.scope,
                        producer: scan.producer,
                        parent: scan.parent,
                        seal: scan
                            .seal
                            .ok_or(StoreError::Corrupt("closure seal absent"))?,
                        declared: scan.declared,
                        counts: scan.counts,
                        status_root: scan.status.finish(),
                        closed_at: now,
                    });
                    records::replace(
                        &tx,
                        retained.summary_record,
                        retained.summary_revision,
                        &state,
                        true,
                    )?;
                    changed = true;
                    progress.closed_scopes += 1;
                }
            }
        }
        if changed {
            self.remember_clock(&tx, now)?;
            commit(tx, "settlement-scope")?;
        } else {
            tx.commit()?;
        }
        Ok(())
    }
}
