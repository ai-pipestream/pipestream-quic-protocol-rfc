//! Persistent admission records. Execution leases and settlement operate on
//! this fixed record; reading it alone never authorizes a callback.

use super::*;

pub(super) const CAPACITY: usize = 2048;
// Expansion completion and terminal settlement need at most two job writes.
// Input/output reclamation each needs an intent and a completion write. Lease
// acquisition/renewal cannot spend these six autonomous-transition allowances.
pub(super) const CREDITS: u64 = 6;
// A branch's work view can wait, resume, accept a cancellation fence, and settle.
// Explicit retries must replenish their own transition budget before commit.
pub(super) const WORK_CREDITS: u64 = 4;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct JobRecord {
    pub parameters: AdmitParameters,
    pub operation: OperationId,
    pub originator: Producer,
    pub restart_safety: Number,
    pub attempt: Id,
    pub lease: Number,
    pub lease_until: Option<Number>,
    // 0 queued, 1 executing, 2 waiting for children, 3 awaiting explicit retry,
    // 4 settled. These are private scheduler states, not wire work states.
    pub stage: Number,
    pub input_key: ApplicationLabel,
    pub reservation_key: ApplicationLabel,
    pub object_limit: Number,
    pub input_live: bool,
    pub outputs_live: bool,
    pub executor_live: bool,
    // Membership can be sealed before any inputs are admitted. Only a separate
    // completed expansion transition switches a mode-2 job to rehydration.
    pub expansion_complete: bool,
    pub release: Option<Release>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Release {
    pub input: bool,
    pub outputs: bool,
    pub at: Number,
}
impl Wire for Release {
    fn read(d: &mut minicbor::Decoder<'_>) -> std::result::Result<Self, Error> {
        codec::array(d, 3)?;
        let value = Self {
            input: bool::read(d)?,
            outputs: bool::read(d)?,
            at: Number::read(d)?,
        };
        value.check()?;
        Ok(value)
    }
    fn write(&self, w: &mut codec::Writer) {
        w.array(3);
        self.input.write(w);
        self.outputs.write(w);
        self.at.write(w);
    }
    fn check(&self) -> std::result::Result<(), Error> {
        self.at.check()?;
        require(self.input || self.outputs, "empty resource release intent")
    }
}

impl Wire for JobRecord {
    fn read(d: &mut minicbor::Decoder<'_>) -> std::result::Result<Self, Error> {
        codec::array(d, 16)?;
        let value = Self {
            parameters: AdmitParameters::read(d)?,
            operation: OperationId::read(d)?,
            originator: Producer::read(d)?,
            restart_safety: Number::read(d)?,
            attempt: Id::read(d)?,
            lease: Number::read(d)?,
            lease_until: Option::<Number>::read(d)?,
            stage: Number::read(d)?,
            input_key: ApplicationLabel::read(d)?,
            reservation_key: ApplicationLabel::read(d)?,
            object_limit: Number::read(d)?,
            input_live: bool::read(d)?,
            outputs_live: bool::read(d)?,
            executor_live: bool::read(d)?,
            expansion_complete: bool::read(d)?,
            release: Option::<Release>::read(d)?,
        };
        value.check()?;
        Ok(value)
    }
    fn write(&self, w: &mut codec::Writer) {
        w.array(16);
        self.parameters.write(w);
        self.operation.write(w);
        self.originator.write(w);
        self.restart_safety.write(w);
        self.attempt.write(w);
        self.lease.write(w);
        self.lease_until.write(w);
        self.stage.write(w);
        self.input_key.write(w);
        self.reservation_key.write(w);
        self.object_limit.write(w);
        self.input_live.write(w);
        self.outputs_live.write(w);
        self.executor_live.write(w);
        self.expansion_complete.write(w);
        self.release.write(w);
    }
    fn check(&self) -> std::result::Result<(), Error> {
        self.parameters.check()?;
        self.operation.check()?;
        self.originator.check()?;
        self.attempt.check()?;
        self.lease.check()?;
        self.lease_until.check()?;
        self.object_limit.check()?;
        self.release.check()?;
        require(
            self.restart_safety.0 <= 2 && self.stage.0 <= 4,
            "invalid job state",
        )?;
        require(
            [self.input_key.0.as_str(), self.reservation_key.0.as_str()]
                .iter()
                .all(|key| {
                    key.len() == 32
                        && key
                            .bytes()
                            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
                })
                && self.input_key != self.reservation_key,
            "invalid job payload keys",
        )?;
        require(
            (self.stage.0 == 1) == self.lease_until.is_some()
                && (self.stage.0 != 1 || self.lease.0 != 0)
                && self.executor_live == (self.stage.0 != 4)
                && (!self.executor_live || self.input_live && self.outputs_live)
                && (self.parameters.mode == Mode(2) || self.expansion_complete)
                && (self.stage != Number(2) || self.expansion_complete)
                && (self.release.is_none() || !self.executor_live)
                && (self.input_live || self.release.as_ref().is_some_and(|r| r.input))
                && (self.outputs_live || self.release.as_ref().is_some_and(|r| r.outputs)),
            "job lease or resource liveness inconsistent",
        )
    }
}

#[derive(Default)]
struct Usage {
    jobs: u64,
    inputs: u64,
    outputs: u64,
}
fn add(left: u64, right: u64) -> Result<u64> {
    left.checked_add(right)
        .filter(|n| *n <= MAX_NUMBER)
        .ok_or_else(exhausted)
}
fn exhausted() -> StoreError {
    protocol(
        ErrorCode::LimitExceeded,
        "aggregate admission capacity exhausted",
    )
}

pub(super) fn capacity(
    tx: &Transaction<'_>,
    policy: &StorePolicy,
    binding: &Binding,
    proposed: &AdmitParameters,
) -> Result<()> {
    let mut global = 0;
    let mut owner = 0;
    let mut session = Usage::default();
    // Stream bounded records instead of materializing all sessions/jobs. These
    // flags are in the funded job BLOB, so releasing a slot needs no SQL UPDATE.
    let mut statement = tx.prepare(
        "SELECT j.work_row,w.generation,s.owner FROM jobs j JOIN work w ON w.row_id=j.work_row JOIN sessions s ON s.generation=w.generation ORDER BY j.work_row",
    )?;
    let mut rows = statement.query([])?;
    while let Some(row) = rows.next()? {
        let (_, job): (_, JobRecord) = records::read(
            tx,
            records::Target {
                table: records::Table::Job,
                row: row.get(0)?,
            },
        )?;
        let running = u64::from(job.executor_live);
        global = add(global, running)?;
        if row.get::<_, String>(2)? == binding.identity.owner.0 {
            owner = add(owner, running)?;
        }
        if number(row, 1)? == binding.identity.generation.0 {
            session.jobs = add(session.jobs, running)?;
            if job.input_live {
                session.inputs = add(session.inputs, job.parameters.input.length.0)?;
            }
            if job.outputs_live {
                session.outputs = add(session.outputs, job.parameters.outputs.total_bytes.0)?;
            }
        }
    }
    if global >= policy.active_jobs.0
        || owner >= policy.active_jobs_per_owner.0
        || session.jobs >= binding.limits.active_jobs.0
        || add(session.inputs, proposed.input.length.0)? > binding.limits.retained_input_bytes.0
        || add(session.outputs, proposed.outputs.total_bytes.0)?
            > binding.limits.retained_output_bytes.0
    {
        return Err(exhausted());
    }
    Ok(())
}

/// Ending production must not strand an immutable child obligation. Admission
/// is distinct from execution: admitted, unfinished children may still run after
/// this check succeeds. Stream missing-job members without collecting the scope.
pub(super) fn require_expansion_inputs(
    tx: &Transaction<'_>,
    generation: Id,
    parent: &WorkView,
) -> Result<()> {
    let child = parent
        .child
        .as_ref()
        .ok_or(StoreError::Corrupt("expansion child scope missing"))?;
    let scope = scopes::load(tx, generation, Number(child.scope.0))?;
    if child.producer != Producer(1)
        || scope.producer != child.producer
        || scope.parent.as_ref() != Some(&parent.work)
    {
        return Err(StoreError::Corrupt("expansion child scope binding changed"));
    }
    if scope.seal.is_none() {
        return Err(protocol(
            ErrorCode::NotReady,
            "authority expansion membership is not sealed",
        ));
    }
    let mut statement = tx.prepare(
        "SELECT w.row_id,w.entity,w.producer FROM work w LEFT JOIN jobs j ON j.work_row=w.row_id
         WHERE w.generation=?1 AND w.scope=?2 AND j.work_row IS NULL ORDER BY w.entity",
    )?;
    let mut rows = statement.query(params![sql(generation.0)?, sql(child.scope.0)?])?;
    while let Some(row) = rows.next()? {
        let (_, member): (_, WorkView) = records::read(
            tx,
            records::Target {
                table: records::Table::Work,
                row: row.get(0)?,
            },
        )?;
        if member.work.scope != Number(child.scope.0)
            || member.work.entity != Id(number(row, 1)?)
            || member.work.producer != child.producer
            || number(row, 2)? != child.producer.0
            || member.admitted_at.is_some()
        {
            return Err(StoreError::Corrupt("expansion member admission differs"));
        }
        if !member.state.is_terminal() {
            return Err(protocol(
                ErrorCode::NotReady,
                "declared child input is not admitted",
            ));
        }
    }
    Ok(())
}

pub(super) fn verify(tx: &Transaction<'_>) -> Result<()> {
    let mut statement = tx.prepare("SELECT w.row_id,w.generation,s.owner,j.work_row,s.results FROM work w JOIN sessions s ON s.generation=w.generation LEFT JOIN jobs j ON j.work_row=w.row_id ORDER BY w.row_id")?;
    let mut rows = statement.query([])?;
    while let Some(row) = rows.next()? {
        let work_row: i64 = row.get(0)?;
        let (_, view): (_, WorkView) = records::read(
            tx,
            records::Target {
                table: records::Table::Work,
                row: work_row,
            },
        )?;
        let present = row.get::<_, Option<i64>>(3)?.is_some();
        view.validate_profiles(row.get(4)?)
            .map_err(|_| StoreError::Corrupt("work result profile changed"))?;
        if present != view.admitted_at.is_some() {
            return Err(StoreError::Corrupt(
                "admitted work and restartable job disagree",
            ));
        }
        if !present {
            continue;
        }
        let (_, job): (_, JobRecord) = records::read(
            tx,
            records::Target {
                table: records::Table::Job,
                row: work_row,
            },
        )?;
        if job.parameters.work != view.work
            || Some(&job.parameters.input) != view.input.as_ref()
            || job.attempt.0 != view.attempt.0
            || (job.parameters.mode.0 != 0) != view.child.is_some()
            || job.executor_live == view.state.is_terminal()
            || (view.state == State::SUCCEEDED && !job.expansion_complete)
            || view.deadline
                != Some(add_duration(
                    view.admitted_at.unwrap(),
                    job.parameters.execution_ms,
                )?)
        {
            return Err(StoreError::Corrupt("job differs from retained work"));
        }
        let expected_state = match job.stage.0 {
            0 | 1 => State::ACTIVE,
            2 => State::WAITING_CHILDREN,
            3 => State::AWAITING_RETRY,
            _ => view.state,
        };
        if (!view.state.is_terminal()
            && view.state != State::CANCELLING
            && view.state != expected_state)
            || job
                .lease_until
                .is_some_and(|until| Some(until) > view.deadline)
        {
            return Err(StoreError::Corrupt("job stage or lease differs from work"));
        }
        if let Some(child) = &view.child {
            if job.parameters.mode == Mode(2) && job.expansion_complete {
                match require_expansion_inputs(tx, Id(number(row, 1)?), &view) {
                    Err(StoreError::Protocol(error)) if error.code == ErrorCode::NotReady => {
                        return Err(StoreError::Corrupt(
                            "completed expansion has unresolved input obligations",
                        ));
                    }
                    result => result?,
                }
            }
            let retained: (u64, Vec<u8>) = tx.query_row(
                "SELECT producer,parent FROM scopes WHERE generation=?1 AND scope=?2",
                params![sql(number(row, 1)?)?, sql(child.scope.0)?],
                |r| Ok((number(r, 0)?, r.get(1)?)),
            )?;
            if retained.0 != child.producer.0
                || unpack::<WorkKey>(&retained.1)? != view.work
                || child.producer.0 != job.parameters.mode.0 - 1
            {
                return Err(StoreError::Corrupt("job child scope binding changed"));
            }
        }
        for (key, purpose) in [(&job.input_key.0, 0), (&job.reservation_key.0, 2)] {
            let correct: bool = tx.query_row(
                "SELECT EXISTS(SELECT 1 FROM payload_refs WHERE object_key=?1 AND generation=?2 AND scope=?3 AND entity=?4 AND purpose=?5)",
                params![key, sql(number(row, 1)?)?, sql(view.work.scope.0)?, sql(view.work.entity.0)?, purpose],
                |r| r.get(0),
            )?;
            if !correct {
                return Err(StoreError::Corrupt("job payload reference missing"));
            }
        }
        let identity = SessionIdentity {
            authority: IdentityLabel(tx.query_row("SELECT name FROM authority", [], |r| r.get(0))?),
            owner: IdentityLabel(row.get(2)?),
            generation: Id(number(row, 1)?),
        };
        let retiring = retirement::load(tx, identity.generation)?;
        if let Some(proof) = &retiring {
            retirement::verify_work(proof, &view)?;
            if job.executor_live || job.input_live || job.outputs_live {
                return Err(StoreError::Corrupt("retiring job retains resources"));
            }
        }
        #[cfg(unix)]
        if let Some(release) = &job.release
            && retiring.is_none()
        {
            super::retention::verify_release(tx, &identity, &view, release)?;
        }
        if let Some(manifest) = &view.manifest {
            let bytes = manifest
                .outputs
                .iter()
                .try_fold(0u64, |sum, output| sum.checked_add(output.length.0));
            if manifest.authority != identity.authority
                || manifest.owner != identity.owner
                || manifest.generation != identity.generation
                || manifest.outputs.len() as u64 > job.parameters.outputs.count.0
                || bytes.is_none_or(|sum| sum > job.parameters.outputs.total_bytes.0)
                || manifest
                    .outputs
                    .iter()
                    .any(|output| output.length > job.object_limit)
            {
                return Err(StoreError::Corrupt(
                    "manifest binding or output budget changed",
                ));
            }
        }
        let receipt = scopes::operation(tx, identity.generation, job.originator, job.operation)?
            .ok_or(StoreError::Corrupt("job admission receipt missing"))?;
        let expected = Mutation::Admit(job.parameters.clone()).digest(
            &identity,
            job.originator,
            job.operation,
        )?;
        if receipt.request_digest != expected
            || receipt.body
                != (Outcome::Admitted {
                    work: view.work.clone(),
                    attempt: Id(1),
                    admitted_at: view.admitted_at.unwrap(),
                    deadline: view.deadline.unwrap(),
                    child: view.child.clone(),
                })
        {
            return Err(StoreError::Corrupt("job admission receipt changed"));
        }
    }
    Ok(())
}
