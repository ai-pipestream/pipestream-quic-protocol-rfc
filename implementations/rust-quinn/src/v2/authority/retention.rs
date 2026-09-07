//! Three-phase resource reclamation: committed intent, reference/pin-safe file
//! deletion, then logical quota release. Crashes can leave conservative charges,
//! never free capacity while promised bytes or live handles still exist.

use super::{payload::PayloadStore, *};

#[derive(Default)]
pub struct RetentionCursor {
    binding: Option<Vec<u8>>,
    before: Option<i64>,
    payload_after: Option<String>,
    payload_through: Option<String>,
}
#[derive(Debug, Default)]
pub struct RetentionProgress {
    pub inspected_jobs: usize,
    pub committed_intents: usize,
    pub released_inputs: usize,
    pub released_outputs: usize,
    pub inspected_files: usize,
    pub removed_files: usize,
}

fn children_closed(
    tx: &Transaction<'_>,
    identity: &SessionIdentity,
    view: &WorkView,
    at: Number,
) -> Result<bool> {
    match &view.child {
        None => Ok(true),
        Some(child) => Ok(
            scopes::closed(tx, identity.generation, Number(child.scope.0))?
                .is_some_and(|s| s.closed_at <= at),
        ),
    }
}
fn parent_settled(
    tx: &Transaction<'_>,
    identity: &SessionIdentity,
    view: &WorkView,
    at: Number,
) -> Result<bool> {
    let scope = scopes::load(tx, identity.generation, view.work.scope)?;
    let Some(parent) = scope.parent else {
        return Ok(true);
    };
    let (_, parent) = scopes::work(tx, identity.generation, &parent)?;
    Ok(parent.state.is_terminal() && parent.terminal_at.is_some_and(|time| time <= at))
}
pub(super) fn verify_release(
    tx: &Transaction<'_>,
    identity: &SessionIdentity,
    view: &WorkView,
    release: &jobs::Release,
) -> Result<()> {
    let (_, greatest): (_, Number) = records::read(tx, records::CLOCK)?;
    if !view.state.is_terminal()
        || view.terminal_at.is_none_or(|at| at > release.at)
        || release.at > greatest
        || !children_closed(tx, identity, view, release.at)?
        || (release.outputs
            && (!parent_settled(tx, identity, view, release.at)?
                || view.output_until.is_some_and(|until| release.at < until)))
    {
        return Err(StoreError::Corrupt(
            "resource release violates retained promise",
        ));
    }
    Ok(())
}
impl AuthorityStore {
    /// Local maintenance, independent of a caller's current permission. A
    /// trusted non-regressed clock is required, including after revocation.
    /// Jobs and files use separate bounded passes; prior commits survive a
    /// later refusal. Retain this cursor across calls or start a fresh pass.
    pub fn reclaim(
        &self,
        payloads: &PayloadStore,
        cursor: &mut RetentionCursor,
        limit: usize,
    ) -> Result<RetentionProgress> {
        if !(1..=256).contains(&limit) {
            return Err(protocol(
                ErrorCode::LimitExceeded,
                "invalid retention batch",
            ));
        }
        let binding = self.payload_identity()?.as_bytes().to_vec();
        if cursor.binding.as_ref().is_some_and(|old| old != &binding) {
            return Err(protocol(
                ErrorCode::Conflict,
                "retention cursor belongs to another authority",
            ));
        }
        cursor.binding = Some(binding);
        let mut report = RetentionProgress::default();
        let mut connection = self.connect()?;
        for _ in 0..limit {
            let tx = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
            execution::bound_payloads(&tx, payloads)?;
            let now = self.check_clock(&tx)?;
            let candidate: Option<(i64, u64, String)> = tx.query_row(
                "SELECT j.work_row,w.generation,s.owner FROM jobs j JOIN work w ON w.row_id=j.work_row JOIN sessions s ON s.generation=w.generation WHERE (?1 IS NULL OR j.work_row<?1) ORDER BY j.work_row DESC LIMIT 1",
                [cursor.before], |r| Ok((r.get(0)?, number(r, 1)?, r.get(2)?))).optional()?;
            let Some((row, generation, owner)) = candidate else {
                cursor.before = None;
                break;
            };
            let target = records::Target {
                table: records::Table::Job,
                row,
            };
            let (record, mut job): (_, jobs::JobRecord) = records::read(&tx, target)?;
            let identity = SessionIdentity {
                authority: self.authority.clone(),
                owner: IdentityLabel(owner),
                generation: Id(generation),
            };
            let (_, view) = scopes::work(&tx, identity.generation, &job.parameters.work)?;
            if let Some(release) = &job.release {
                verify_release(&tx, &identity, &view, release)?;
            }
            report.inspected_jobs += 1;
            if view.state.is_terminal() && children_closed(&tx, &identity, &view, now)? {
                let previous = job.release.clone().unwrap_or(jobs::Release {
                    input: false,
                    outputs: false,
                    at: now,
                });
                let output_due = view.output_until.is_none_or(|until| now >= until)
                    && parent_settled(&tx, &identity, &view, now)?;
                if !previous.input || (!previous.outputs && output_due) {
                    job.release = Some(jobs::Release {
                        input: true,
                        outputs: previous.outputs || output_due,
                        at: now,
                    });
                    records::replace(&tx, target, record.revision, &job, true)?;
                    self.remember_clock(&tx, now)?;
                    verify_release(&tx, &identity, &view, job.release.as_ref().unwrap())?;
                    commit(tx, "retention-intent")?;
                    report.committed_intents += 1;
                } else {
                    verify_release(&tx, &identity, &view, &previous)?;
                    let (input_gone, outputs_gone) =
                        payloads.reclamation_status(&job.input_key.0, &job.reservation_key.0)?;
                    let input = job.input_live && previous.input && input_gone;
                    let outputs = job.outputs_live && previous.outputs && outputs_gone;
                    if input || outputs {
                        job.input_live &= !input;
                        job.outputs_live &= !outputs;
                        records::replace(&tx, target, record.revision, &job, true)?;
                        self.remember_clock(&tx, now)?;
                        commit(tx, "retention-finish")?;
                        report.released_inputs += usize::from(input);
                        report.released_outputs += usize::from(outputs);
                    }
                }
            }
            cursor.before = Some(row);
        }
        if cursor.payload_through.is_none() {
            cursor.payload_through = payloads.last_retained_key()?;
        }
        let progress = self.collect_payloads_until(
            payloads,
            cursor.payload_after.as_deref(),
            cursor.payload_through.as_deref(),
            limit,
        )?;
        report.inspected_files = progress.inspected;
        report.removed_files = progress.removed;
        if progress.inspected < limit {
            cursor.payload_after = None;
            cursor.payload_through = None;
        } else {
            cursor.payload_after = progress.next;
        }
        Ok(report)
    }
}
