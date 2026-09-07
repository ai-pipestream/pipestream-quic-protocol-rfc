//! Irreversible session retirement, followed by bounded metadata deletion.
//! The immutable eligibility record and closed root survive every partial pass.
//! Generation and owner creation high-water marks are never deleted or reduced.

use super::*;

const CAPACITY: usize = 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Proof {
    identity: SessionIdentity,
    creation: Id,
    root: ScopeSummary,
    pub cutoff: Number,
    at: Number,
}
impl Wire for Proof {
    fn read(d: &mut minicbor::Decoder<'_>) -> std::result::Result<Self, Error> {
        codec::array(d, 5)?;
        codec::array(d, 3)?;
        let value = Self {
            identity: SessionIdentity {
                authority: IdentityLabel::read(d)?,
                owner: IdentityLabel::read(d)?,
                generation: Id::read(d)?,
            },
            creation: Id::read(d)?,
            root: ScopeSummary::read(d)?,
            cutoff: Number::read(d)?,
            at: Number::read(d)?,
        };
        value.check()?;
        Ok(value)
    }
    fn write(&self, w: &mut codec::Writer) {
        w.array(5);
        w.array(3);
        self.identity.authority.write(w);
        self.identity.owner.write(w);
        self.identity.generation.write(w);
        self.creation.write(w);
        self.root.write(w);
        self.cutoff.write(w);
        self.at.write(w);
    }
    fn check(&self) -> std::result::Result<(), Error> {
        self.identity.authority.check()?;
        self.identity.owner.check()?;
        self.identity.generation.check()?;
        self.creation.check()?;
        self.root.check()?;
        self.cutoff.check()?;
        self.at.check()?;
        require(
            self.root.scope == Number(0)
                && self.root.producer == Producer(0)
                && self.root.parent.is_none()
                && self.root.closed_at <= self.cutoff
                && self.cutoff <= self.at,
            "invalid session retirement proof",
        )
    }
}

pub(super) fn load(tx: &Transaction<'_>, generation: Id) -> Result<Option<Proof>> {
    let state: Option<(bool, Option<i64>)> = tx.query_row(
        "SELECT s.retiring,r.generation FROM sessions s LEFT JOIN retirements r ON r.generation=s.generation WHERE s.generation=?1",
        [sql(generation.0)?],
        |r| Ok((r.get(0)?, r.get(1)?)),
    ).optional()?;
    let Some((marked, present)) = state else {
        return Ok(None);
    };
    if marked != present.is_some() {
        return Err(StoreError::Corrupt("retirement flag and proof disagree"));
    }
    if !marked {
        return Ok(None);
    }
    let (header, proof): (_, Proof) = records::read(
        tx,
        records::Target {
            table: records::Table::Retirement,
            row: sql(generation.0)?,
        },
    )?;
    if header.revision != Id(1)
        || header.credits != 0
        || header.capacity != CAPACITY
        || proof.identity.generation != generation
    {
        return Err(StoreError::Corrupt("immutable retirement record changed"));
    }
    verify(tx, &proof)?;
    Ok(Some(proof))
}

pub(super) fn require_live(tx: &Transaction<'_>, generation: Id) -> Result<()> {
    if load(tx, generation)?.is_some() {
        return Err(protocol(ErrorCode::Expired, "session retirement committed"));
    }
    Ok(())
}

pub(super) fn verify_work(proof: &Proof, view: &WorkView) -> Result<()> {
    if !view.state.is_terminal()
        || view.terminal_at.is_none_or(|at| at > proof.cutoff)
        || view.receipt_until.is_none_or(|at| at > proof.cutoff)
        || view.output_until.is_some_and(|at| at > proof.cutoff)
    {
        return Err(StoreError::Corrupt(
            "retiring work has an unresolved promise",
        ));
    }
    Ok(())
}

fn verify(tx: &Transaction<'_>, proof: &Proof) -> Result<()> {
    proof.check()?;
    let (_, greatest): (_, Number) = records::read(tx, records::CLOCK)?;
    let (authority, generation): (String, u64) =
        tx.query_row("SELECT name,last_generation FROM authority", [], |r| {
            Ok((r.get(0)?, number(r, 1)?))
        })?;
    let (binding, _) = sessions::load(tx, &IdentityLabel(authority), proof.identity.generation)?
        .ok_or(StoreError::Corrupt("retirement session absent"))?;
    let creation: u64 = tx.query_row(
        "SELECT last_creation FROM owners WHERE owner=?1",
        [&binding.identity.owner.0],
        |r| number(r, 0),
    )?;
    if proof.identity != binding.identity
        || proof.creation != binding.creation_sequence
        || generation < proof.identity.generation.0
        || creation < proof.creation.0
        || proof.at > greatest
        || proof.cutoff < add_duration(proof.root.closed_at, binding.policy.receipt_retention_ms)?
        || scopes::closed(tx, binding.identity.generation, Number(0))?.as_ref() != Some(&proof.root)
    {
        return Err(StoreError::Corrupt(
            "retirement proof differs from retained authority",
        ));
    }
    Ok(())
}

pub(super) fn verify_all(tx: &Transaction<'_>) -> Result<()> {
    let orphan: bool = tx.query_row("SELECT EXISTS(SELECT 1 FROM retirements r LEFT JOIN sessions s ON s.generation=r.generation WHERE s.generation IS NULL)", [], |r| r.get(0))?;
    if orphan {
        return Err(StoreError::Corrupt("retirement proof has no session"));
    }
    let mut statement = tx.prepare("SELECT s.generation FROM sessions s LEFT JOIN retirements r ON r.generation=s.generation WHERE s.retiring=1 OR r.generation IS NOT NULL ORDER BY s.generation")?;
    let mut rows = statement.query([])?;
    while let Some(row) = rows.next()? {
        load(tx, Id(number(row, 0)?))?
            .ok_or(StoreError::Corrupt("retirement proof disappeared"))?;
    }
    Ok(())
}

#[cfg(unix)]
mod cleanup {
    use super::*;
    use crate::v2::authority::payload::PayloadStore;

    #[derive(Default)]
    pub struct RetirementCursor {
        binding: Option<Vec<u8>>,
        before: Option<u64>,
    }
    #[derive(Debug, Default)]
    pub struct RetirementProgress {
        pub inspected_session: Option<Id>,
        pub started: bool,
        pub deleted_work: usize,
        pub deleted_scopes: usize,
        pub deleted_operations: usize,
        pub completed: bool,
    }

    fn eligible(
        store: &AuthorityStore,
        tx: &Transaction<'_>,
        payloads: &PayloadStore,
        generation: Id,
        now: Number,
    ) -> Result<Option<Proof>> {
        let (binding, _) = sessions::load(tx, &store.authority, generation)?
            .ok_or(StoreError::Corrupt("retirement candidate absent"))?;
        let Some(root) = scopes::closed(tx, generation, Number(0))? else {
            return Ok(None);
        };
        let mut cutoff = add_duration(root.closed_at, binding.policy.receipt_retention_ms)?;
        if now < cutoff {
            return Ok(None);
        }
        let mut statement = tx.prepare("SELECT w.row_id,j.work_row FROM work w LEFT JOIN jobs j ON j.work_row=w.row_id WHERE w.generation=?1 ORDER BY w.row_id")?;
        let mut rows = statement.query([sql(generation.0)?])?;
        while let Some(row) = rows.next()? {
            let (_, view): (_, WorkView) = records::read(
                tx,
                records::Target {
                    table: records::Table::Work,
                    row: row.get(0)?,
                },
            )?;
            if !view.state.is_terminal() {
                return Err(StoreError::Corrupt("closed root contains unresolved work"));
            }
            cutoff = cutoff.max(
                view.receipt_until
                    .ok_or(StoreError::Corrupt("terminal receipt deadline missing"))?,
            );
            if let Some(until) = view.output_until {
                cutoff = cutoff.max(until);
            }
            if let Some(row) = row.get::<_, Option<i64>>(1)? {
                let (_, job): (_, jobs::JobRecord) = records::read(
                    tx,
                    records::Target {
                        table: records::Table::Job,
                        row,
                    },
                )?;
                if job.executor_live || job.input_live || job.outputs_live {
                    return Ok(None);
                }
            }
        }
        if now < cutoff {
            return Ok(None);
        }
        // Audit before committing intentional partial deletion. Subsequently the
        // proof permits missing inter-record relationships, never live resources.
        records::verify(tx)?;
        payloads.audit_references(tx)?;
        let mut scopes = tx.prepare("SELECT rowid FROM scopes WHERE generation=?1")?;
        let mut rows = scopes.query([sql(generation.0)?])?;
        while let Some(row) = rows.next()? {
            let (_, state): (_, scopes::ScopeState) = records::read(
                tx,
                records::Target {
                    table: records::Table::Scope,
                    row: row.get(0)?,
                },
            )?;
            if state.summary.as_ref().is_none_or(|s| s.closed_at > cutoff) {
                return Err(StoreError::Corrupt("closed root contains unclosed scope"));
            }
        }
        Ok(Some(Proof {
            identity: binding.identity,
            creation: binding.creation_sequence,
            root,
            cutoff,
            at: now,
        }))
    }

    impl AuthorityStore {
        /// Inspect one session per call, in descending generation passes. A
        /// committed retirement refuses new access before deleting anything.
        /// Later calls delete at most `limit` work bundles, scopes or operations,
        /// each in its own transaction. The final root/proof/session deletion is
        /// atomic. Ordinary protected SQL may refuse under pinned-WAL pressure;
        /// no partial transaction or released session slot follows that refusal.
        pub fn retire(
            &self,
            payloads: &PayloadStore,
            cursor: &mut RetirementCursor,
            limit: usize,
        ) -> Result<RetirementProgress> {
            if !(1..=256).contains(&limit) {
                return Err(protocol(
                    ErrorCode::LimitExceeded,
                    "invalid retirement batch",
                ));
            }
            let mut connection = self.connect()?;
            let tx = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
            execution::bound_payloads(&tx, payloads)?;
            let binding = self.payload_identity()?.as_bytes().to_vec();
            if cursor.binding.as_ref().is_some_and(|old| old != &binding) {
                return Err(protocol(
                    ErrorCode::Conflict,
                    "retirement cursor belongs to another authority",
                ));
            }
            cursor.binding = Some(binding);
            let now = self.check_clock(&tx)?;
            let generation: Option<u64> = tx.query_row("SELECT generation FROM sessions WHERE (?1 IS NULL OR generation<?1) ORDER BY generation DESC LIMIT 1", [cursor.before.map(sql).transpose()?], |r| number(r, 0)).optional()?;
            let Some(generation) = generation else {
                cursor.before = None;
                return Ok(RetirementProgress::default());
            };
            let generation = Id(generation);
            let mut report = RetirementProgress {
                inspected_session: Some(generation),
                ..RetirementProgress::default()
            };
            if load(&tx, generation)?.is_some() {
                drop(tx);
            } else if let Some(proof) = eligible(self, &tx, payloads, generation, now)? {
                records::protect(&tx, 0, 0)?;
                tx.execute(
                    "INSERT INTO retirements VALUES(?1,zeroblob(?2))",
                    params![
                        sql(generation.0)?,
                        (CAPACITY + records::HEADER_BYTES) as i64
                    ],
                )?;
                records::initialize(
                    &tx,
                    records::Target {
                        table: records::Table::Retirement,
                        row: sql(generation.0)?,
                    },
                    &proof,
                    CAPACITY,
                    0,
                )?;
                tx.execute(
                    "UPDATE sessions SET retiring=1 WHERE generation=?1",
                    [sql(generation.0)?],
                )?;
                self.remember_clock(&tx, now)?;
                load(&tx, generation)?
                    .ok_or(StoreError::Corrupt("retirement intent disappeared"))?;
                commit(tx, "retirement-intent")?;
                report.started = true;
                cursor.before = Some(generation.0);
                return Ok(report);
            } else {
                cursor.before = Some(generation.0);
                return Ok(report);
            }
            for _ in 0..limit {
                let tx = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
                self.check_clock(&tx)?;
                let Some(proof) = load(&tx, generation)? else {
                    report.completed = true;
                    break;
                };
                records::protect(&tx, 0, 0)?;
                let row: Option<i64> = tx
                    .query_row(
                        "SELECT row_id FROM work WHERE generation=?1 ORDER BY row_id DESC LIMIT 1",
                        [sql(generation.0)?],
                        |r| r.get(0),
                    )
                    .optional()?;
                if let Some(row) = row {
                    let (_, view): (_, WorkView) = records::read(
                        &tx,
                        records::Target {
                            table: records::Table::Work,
                            row,
                        },
                    )?;
                    verify_work(&proof, &view)?;
                    if view.admitted_at.is_some() {
                        let (_, job): (_, jobs::JobRecord) = records::read(
                            &tx,
                            records::Target {
                                table: records::Table::Job,
                                row,
                            },
                        )?;
                        if job.executor_live
                            || job.input_live
                            || job.outputs_live
                            || payloads
                                .reclamation_status(&job.input_key.0, &job.reservation_key.0)?
                                != (true, true)
                        {
                            return Err(StoreError::Corrupt("retiring job retains resources"));
                        }
                    }
                    tx.execute(
                        "DELETE FROM payload_refs WHERE generation=?1 AND scope=?2 AND entity=?3",
                        params![
                            sql(generation.0)?,
                            sql(view.work.scope.0)?,
                            sql(view.work.entity.0)?
                        ],
                    )?;
                    tx.execute("DELETE FROM jobs WHERE work_row=?1", [row])?;
                    tx.execute("DELETE FROM work WHERE row_id=?1", [row])?;
                    commit(tx, "retirement-work")?;
                    report.deleted_work += 1;
                    continue;
                }
                let row: Option<i64> = tx.query_row("SELECT rowid FROM scopes WHERE generation=?1 AND scope<>0 ORDER BY scope DESC LIMIT 1", [sql(generation.0)?], |r| r.get(0)).optional()?;
                if let Some(row) = row {
                    tx.execute("DELETE FROM scopes WHERE rowid=?1", [row])?;
                    commit(tx, "retirement-scope")?;
                    report.deleted_scopes += 1;
                    continue;
                }
                let row: Option<i64> = tx.query_row("SELECT rowid FROM operations WHERE generation=?1 ORDER BY rowid DESC LIMIT 1", [sql(generation.0)?], |r| r.get(0)).optional()?;
                if let Some(row) = row {
                    tx.execute("DELETE FROM operations WHERE rowid=?1", [row])?;
                    commit(tx, "retirement-operation")?;
                    report.deleted_operations += 1;
                    continue;
                }
                tx.execute(
                    "DELETE FROM retirements WHERE generation=?1",
                    [sql(generation.0)?],
                )?;
                tx.execute(
                    "DELETE FROM scopes WHERE generation=?1 AND scope=0",
                    [sql(generation.0)?],
                )?;
                tx.execute(
                    "DELETE FROM sessions WHERE generation=?1",
                    [sql(generation.0)?],
                )?;
                commit(tx, "retirement-finish")?;
                report.completed = true;
                break;
            }
            cursor.before = Some(generation.0);
            Ok(report)
        }
    }
}

#[cfg(unix)]
pub use cleanup::{RetirementCursor, RetirementProgress};

#[cfg(all(test, unix))]
mod tests;
