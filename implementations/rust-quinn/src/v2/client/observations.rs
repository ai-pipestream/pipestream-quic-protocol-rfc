//! Authenticated observations are supplied by the transport, never inferred
//! from local timeouts. Stored references contain full manifests, not URLs alone.
use super::*;
mod receipts;
mod references;
pub use references::RetainedReference;

codec::record!(
    ObservedWork {
        revision: Id,
        view: WorkView
    } | _s
        | { Ok(()) }
);

fn integrity(detail: &'static str) -> JournalError {
    Error::new(ErrorCode::IntegrityError, detail).into()
}
fn check(condition: bool, detail: &'static str) -> Result<()> {
    if condition {
        Ok(())
    } else {
        Err(integrity(detail))
    }
}

pub(super) fn index(intent: &Intent) -> (i64, i64, i64, Option<i64>) {
    let (kind, work) = match &intent.mutation {
        Mutation::Admit(p) => (0, &p.work),
        Mutation::Declare { scope, .. } => return (1, scope.0 as i64, 0, None),
        Mutation::Retry { work, .. } => (2, work),
        Mutation::Cancel { work } => (3, work),
        Mutation::ScopeCancel { scope } => return (4, scope.0 as i64, 0, None),
        Mutation::Skip { work } => (5, work),
    };
    (
        kind,
        work.scope.0 as i64,
        work.producer.0 as i64,
        Some(work.entity.0 as i64),
    )
}
fn key_bytes(identity: &SessionIdentity, work: &WorkKey, table: &str) -> Result<Vec<u8>> {
    let mut bytes = table.as_bytes().to_vec();
    bytes.extend(codec::encode(&identity.authority, MAX_HEADER)?);
    bytes.extend(codec::encode(&identity.owner, MAX_HEADER)?);
    bytes.extend(codec::encode(&identity.generation, MAX_HEADER)?);
    bytes.extend(codec::encode(work, MAX_HEADER)?);
    Ok(bytes)
}
fn row(connection: &Connection, table: &str, work: &WorkKey) -> Result<Option<i64>> {
    work.check()?;
    Ok(connection
        .query_row(
            &format!("SELECT rowid FROM {table} WHERE scope=?1 AND producer=?2 AND entity=?3"),
            params![
                work.scope.0 as i64,
                work.producer.0 as i64,
                work.entity.0 as i64
            ],
            |r| r.get(0),
        )
        .optional()?)
}

impl Journal {
    /// Call only after authenticating/correlating the WORK view. Returns the
    /// newest durable observation, which can be newer than an out-of-order reply.
    /// The view and any contained manifest commit atomically.
    pub fn observe_work(&self, revision: Id, view: &WorkView) -> Result<ObservedWork> {
        revision.check()?;
        view.validate_profiles(self.creation.results)?;
        let mut connection = self.connect()?;
        let tx = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let incoming = ObservedWork {
            revision,
            view: view.clone(),
        };
        self.validate_view(&tx, &incoming)?;
        let previous = self.read_observed(&tx, &view.work)?;
        if let Some(old) = previous {
            compatible(&old, &incoming)?;
            if old.revision >= revision {
                tx.commit()?;
                return Ok(old);
            }
        }
        if let Some(manifest) = &view.manifest {
            self.save_manifest(&tx, manifest)?;
        }
        self.save_observation(
            &tx,
            "observations",
            &view.work,
            &codec::encode(&incoming, MAX_CONTROL_LIMIT)?,
        )?;
        tx.commit()?;
        Ok(incoming)
    }
    pub fn observed_work(&self, work: &WorkKey) -> Result<Option<ObservedWork>> {
        let connection = self.connect()?;
        let tx = connection.unchecked_transaction()?;
        let value = self.read_observed(&tx, work)?;
        if let Some(value) = &value {
            self.validate_view(&tx, value)?;
        }
        tx.commit()?;
        Ok(value)
    }
    /// Retains immutable evidence even after output expiry. This does not renew
    /// availability or authorize a read; the authority must grant each fresh read.
    pub fn remember_manifest(&self, manifest: &Manifest) -> Result<()> {
        let mut connection = self.connect()?;
        let tx = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        self.save_manifest(&tx, manifest)?;
        tx.commit()?;
        Ok(())
    }
    fn read_observed(
        &self,
        connection: &Connection,
        work: &WorkKey,
    ) -> Result<Option<ObservedWork>> {
        let value: Option<ObservedWork> =
            self.read_observation(connection, "observations", work)?;
        if value.as_ref().is_some_and(|v| v.view.work != *work) {
            return Err(JournalError::Corrupt("work observation index changed"));
        }
        Ok(value)
    }
    fn read_manifest(&self, connection: &Connection, work: &WorkKey) -> Result<Option<Manifest>> {
        let value: Option<Manifest> = self.read_observation(connection, "manifests", work)?;
        if value.as_ref().is_some_and(|v| v.work != *work) {
            return Err(JournalError::Corrupt("manifest index changed"));
        }
        Ok(value)
    }
    fn read_observation<T: Wire>(
        &self,
        connection: &Connection,
        table: &str,
        work: &WorkKey,
    ) -> Result<Option<T>> {
        let Some(row) = row(connection, table, work)? else {
            return Ok(None);
        };
        let image = storage::read(connection, table, "image", row)?
            .ok_or(JournalError::Corrupt("observation missing image"))?;
        let key = key_bytes(&self.read_identity(connection)?, work, table)?;
        Ok(Some(codec::decode(
            storage::unseal(&key, &image)?,
            MAX_CONTROL_LIMIT,
        )?))
    }
    fn save_observation(
        &self,
        connection: &Connection,
        table: &str,
        work: &WorkKey,
        bytes: &[u8],
    ) -> Result<()> {
        let key = key_bytes(&self.read_identity(connection)?, work, table)?;
        let image = storage::seal(&key, bytes);
        if let Some(row) = row(connection, table, work)? {
            storage::write(connection, table, "image", row, &image)?;
        } else {
            let count: i64 =
                connection.query_row(&format!("SELECT count(*) FROM {table}"), [], |r| r.get(0))?;
            if count < 0 || count as u64 >= self.limits.observations.0 {
                return Err(Error::new(
                    ErrorCode::LimitExceeded,
                    "client observation inventory ceiling",
                )
                .into());
            }
            connection.execute(
                &format!("INSERT INTO {table}(scope,producer,entity,image) VALUES(?1,?2,?3,?4)"),
                params![
                    work.scope.0 as i64,
                    work.producer.0 as i64,
                    work.entity.0 as i64,
                    image
                ],
            )?;
        }
        Ok(())
    }
    fn save_manifest(&self, connection: &Connection, manifest: &Manifest) -> Result<()> {
        self.validate_manifest(connection, manifest)?;
        if let Some(old) = self.read_manifest(connection, &manifest.work)? {
            return check(old == *manifest, "immutable retained manifest changed");
        }
        self.save_observation(connection, "manifests", &manifest.work, &manifest.encode()?)
    }
    fn validate_manifest(&self, connection: &Connection, manifest: &Manifest) -> Result<()> {
        manifest.check()?;
        check(
            manifest.work.scope.0 != 0 || manifest.work.producer.0 == 0,
            "root work producer changed",
        )?;
        if !self.creation.results {
            return Err(Error::new(
                ErrorCode::ExtensionUnsupported,
                "journal has no result profile",
            )
            .into());
        }
        let identity = self.read_identity(connection)?;
        check(
            manifest.authority == identity.authority
                && manifest.owner == identity.owner
                && manifest.generation == identity.generation,
            "manifest issuing identity disagrees with session",
        )?;
        check(
            manifest.available_until.0 - manifest.committed_at.0
                == self.creation.policy.output_retention_ms.0,
            "manifest output interval disagrees with retained policy",
        )?;
        self.with_work_receipts(connection, &manifest.work, |intent, receipt| {
            receipts::manifest(intent, &receipt.body, manifest)
        })?;
        if let Some(observed) = self.read_observed(connection, &manifest.work)? {
            manifest_matches_view(manifest, &observed.view)?;
        }
        Ok(())
    }
    fn validate_view(&self, connection: &Connection, observed: &ObservedWork) -> Result<()> {
        observed.check()?;
        observed.view.validate_profiles(self.creation.results)?;
        let view = &observed.view;
        check(
            view.work.scope.0 != 0 || view.work.producer.0 == 0,
            "root work producer changed",
        )?;
        self.read_identity(connection)?;
        if let Some(admitted) = view.admitted_at {
            check(
                view.deadline.expect("validated view").0 - admitted.0
                    <= self.creation.policy.execution_limit_ms.0,
                "view execution interval exceeds retained policy",
            )?;
        }
        self.with_work_receipts(connection, &view.work, |intent, receipt| {
            receipts::view(intent, &receipt.body, view)
        })?;
        if let Some(terminal) = view.terminal_at {
            check(
                view.receipt_until.expect("validated view").0 - terminal.0
                    == self.creation.policy.receipt_retention_ms.0,
                "view receipt interval disagrees with retained policy",
            )?;
            if view.state == State::SUCCEEDED {
                check(
                    terminal < view.deadline.expect("validated success"),
                    "success committed at or after execution deadline",
                )?;
            }
        }
        if let Some(manifest) = &view.manifest {
            self.validate_manifest(connection, manifest)?;
        }
        if let Some(manifest) = self.read_manifest(connection, &view.work)? {
            manifest_matches_view(&manifest, view)?;
        }
        Ok(())
    }
    fn with_work_receipts(
        &self,
        connection: &Connection,
        work: &WorkKey,
        mut run: impl FnMut(&Intent, &OperationReceipt) -> Result<()>,
    ) -> Result<()> {
        let mut statement = connection.prepare("SELECT rowid,operation FROM operations WHERE kind IN (0,2,3,5) AND scope=?1 AND producer=?2 AND entity=?3 AND receipt IS NOT NULL")?;
        let mut rows = statement.query(params![
            work.scope.0 as i64,
            work.producer.0 as i64,
            work.entity.0 as i64
        ])?;
        while let Some(row) = rows.next()? {
            let id: i64 = row.get(0)?;
            let operation = storage::read_operation(row, 1)?;
            let intent = self.read_intent(connection, operation)?;
            let image = storage::read(connection, "operations", "receipt", id)?
                .ok_or(JournalError::Corrupt("work receipt missing"))?;
            let receipt: OperationReceipt =
                codec::decode(storage::unseal(&operation.0, &image)?, MAX_HEADER)?;
            storage::validate_receipt(&intent, &self.read_identity(connection)?, &receipt)?;
            run(&intent, &receipt)?;
        }
        Ok(())
    }
    pub(super) fn validate_known_receipt(
        &self,
        connection: &Connection,
        intent: &Intent,
        receipt: &OperationReceipt,
    ) -> Result<()> {
        if let Mutation::Admit(p) = &intent.mutation {
            check(
                p.work.producer.0 == 0
                    && p.execution_ms.0 <= self.creation.policy.execution_limit_ms.0
                    && (self.creation.results
                        || (p.outputs.count.0 == 0 && p.outputs.total_bytes.0 == 0)),
                "admission receipt violates retained producer, policy or profile",
            )?;
        }
        if let Some(work) = receipts::target(intent) {
            check(
                work.scope.0 != 0 || work.producer.0 == 0,
                "root work producer changed",
            )?;
            self.with_work_receipts(connection, work, |old, prior| {
                receipts::pair(old, prior, intent, receipt)
            })?;
            if let Some(view) = self.read_observed(connection, work)? {
                receipts::view(intent, &receipt.body, &view.view)?;
            }
            if let Some(manifest) = self.read_manifest(connection, work)? {
                receipts::manifest(intent, &receipt.body, &manifest)?;
            }
        }
        Ok(())
    }
    pub(super) fn audit_observations(&self, connection: &Connection) -> Result<()> {
        for table in ["observations", "manifests"] {
            let count: i64 =
                connection.query_row(&format!("SELECT count(*) FROM {table}"), [], |r| r.get(0))?;
            if count < 0 || count as u64 > self.limits.observations.0 {
                return Err(JournalError::Corrupt(
                    "observation inventory exceeds retained limit",
                ));
            }
            let mut statement = connection.prepare(&format!(
                "SELECT scope,producer,entity FROM {table} ORDER BY rowid"
            ))?;
            let mut rows = statement.query([])?;
            while let Some(row) = rows.next()? {
                let work = WorkKey {
                    scope: Number(row.get::<_, i64>(0)? as u64),
                    producer: Producer(row.get::<_, i64>(1)? as u64),
                    entity: Id(row.get::<_, i64>(2)? as u64),
                };
                work.check()?;
                if table == "observations" {
                    let observed = self
                        .read_observed(connection, &work)?
                        .ok_or(JournalError::Corrupt("observation disappeared"))?;
                    self.validate_view(connection, &observed)?;
                    if let Some(manifest) = observed.view.manifest {
                        check(
                            self.read_manifest(connection, &work)?.as_ref() == Some(&manifest),
                            "success observation lacks retained manifest",
                        )?;
                    }
                } else {
                    self.validate_manifest(
                        connection,
                        &self
                            .read_manifest(connection, &work)?
                            .ok_or(JournalError::Corrupt("manifest disappeared"))?,
                    )?;
                }
            }
        }
        self.audit_references(connection)?;
        Ok(())
    }
}

fn admission_matches_view(
    parameters: &AdmitParameters,
    receipt: &Outcome,
    view: &WorkView,
) -> Result<()> {
    let Outcome::Admitted {
        admitted_at,
        deadline,
        child,
        ..
    } = receipt
    else {
        return Err(JournalError::Corrupt("not an admission receipt"));
    };
    check(
        view.input.as_ref() == Some(&parameters.input)
            && view.admitted_at == Some(*admitted_at)
            && view.deadline == Some(*deadline)
            && view.child == *child,
        "view disagrees with known admission",
    )
}
fn manifest_matches_view(manifest: &Manifest, view: &WorkView) -> Result<()> {
    check(
        view.state != State::CANCELLING,
        "manifest contradicts observed cancellation",
    )?;
    if let Some(input) = &view.input {
        check(
            input.sha256 == manifest.input_sha256,
            "manifest disagrees with observed input",
        )?;
    }
    check(
        view.attempt.0 <= manifest.attempt.0,
        "manifest predates an observed execution attempt",
    )?;
    check(
        view.state != State::AWAITING_RETRY || view.attempt.0 < manifest.attempt.0,
        "manifest published by attempt awaiting explicit retry",
    )?;
    if let Some(admitted) = view.admitted_at {
        check(
            manifest.committed_at >= admitted
                && manifest.committed_at < view.deadline.expect("validated view"),
            "manifest commit outside observed execution interval",
        )?;
    }
    if view.state.is_terminal() {
        check(
            view.state == State::SUCCEEDED && view.manifest.as_ref() == Some(manifest),
            "manifest disagrees with immutable terminal observation",
        )?;
    }
    Ok(())
}
fn compatible(old: &ObservedWork, new: &ObservedWork) -> Result<()> {
    if old.revision == new.revision {
        return check(old == new, "same work revision changed contents");
    }
    let (before, after) = if old.revision < new.revision {
        (&old.view, &new.view)
    } else {
        (&new.view, &old.view)
    };
    check(
        before.work == after.work && before.attempt <= after.attempt,
        "work identity or attempt regressed",
    )?;
    if before.state.is_terminal() {
        return check(before == after, "terminal work observation changed");
    }
    if before.state == State::CANCELLING {
        check(
            matches!(
                after.state,
                State::CANCELLING | State::CANCELLED | State::SKIPPED
            ) && before.attempt == after.attempt
                && before.admitted_at == after.admitted_at,
            "observed cancellation fence regressed",
        )?;
    }
    if before.state == State::AWAITING_RETRY && before.attempt == after.attempt {
        check(
            !matches!(
                after.state,
                State::ACTIVE | State::WAITING_CHILDREN | State::SUCCEEDED
            ),
            "failed attempt resumed without replacement",
        )?;
    }
    if before.admitted_at.is_some() {
        check(
            before.input == after.input
                && before.admitted_at == after.admitted_at
                && before.deadline == after.deadline
                && before.child == after.child,
            "immutable admission fields changed between revisions",
        )?;
    }
    Ok(())
}
