use super::*;

codec::record!(
    ReferenceChoice {
        index: OutputIndex,
        manifest: Digest
    } | _s
        | { Ok(()) }
);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetainedReference {
    manifest: Manifest,
    index: OutputIndex,
}
impl RetainedReference {
    pub fn manifest(&self) -> &Manifest {
        &self.manifest
    }
    pub fn index(&self) -> OutputIndex {
        self.index
    }
    /// Uses retained identity; endpoint resolution and owner credentials are
    /// trusted application configuration, never derived from the locator text.
    pub fn attach(&self, request: Id) -> std::result::Result<Control, Error> {
        let control = Control::Session(Session::Attach {
            request,
            authority: self.manifest.authority.clone(),
            owner: self.manifest.owner.clone(),
            generation: self.manifest.generation,
        });
        control.encode(INITIAL_CONTROL_LIMIT)?;
        Ok(control)
    }
    pub fn read(&self, request: Id) -> std::result::Result<Control, Error> {
        let control = Control::Result(ResultMessage::Read {
            request,
            work: self.manifest.work.clone(),
            attempt: self.manifest.attempt,
            index: self.index,
            expected_sha256: self.manifest.outputs[self.index.0 as usize].sha256,
        });
        control.encode(INITIAL_CONTROL_LIMIT)?;
        Ok(control)
    }
}

impl Journal {
    pub fn retained_reference(
        &self,
        work: &WorkKey,
        attempt: Id,
        index: OutputIndex,
    ) -> Result<RetainedReference> {
        attempt.check()?;
        index.check()?;
        let connection = self.connect()?;
        let tx = connection.unchecked_transaction()?;
        let reference = self.read_reference(&tx, work, attempt, index)?;
        tx.commit()?;
        Ok(reference)
    }
    /// Persist the full manifest and selected index atomically. Storing a
    /// manifest alone does not choose an output on the application's behalf.
    pub fn remember_reference(
        &self,
        manifest: &Manifest,
        index: OutputIndex,
    ) -> Result<RetainedReference> {
        index.check()?;
        let mut connection = self.connect()?;
        let tx = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        self.save_manifest(&tx, manifest)?;
        if manifest.outputs.get(index.0 as usize).is_none() {
            return Err(Error::new(ErrorCode::NotFound, "selected manifest output absent").into());
        }
        if reference_row(&tx, &manifest.work, index)?.is_none() {
            let count: i64 =
                tx.query_row("SELECT count(*) FROM result_references", [], |r| r.get(0))?;
            if count < 0 || count as u64 >= self.limits.observations.0 {
                return Err(Error::new(
                    ErrorCode::LimitExceeded,
                    "client result-reference ceiling",
                )
                .into());
            }
            let choice = ReferenceChoice {
                index,
                manifest: manifest.digest()?,
            };
            let key = key_bytes(
                &self.read_identity(&tx)?,
                &manifest.work,
                "result_references",
            )?;
            let image = storage::seal(&key, &codec::encode(&choice, MAX_HEADER)?);
            tx.execute("INSERT INTO result_references(scope,producer,entity,output_index,image) VALUES(?1,?2,?3,?4,?5)",
                params![manifest.work.scope.0 as i64, manifest.work.producer.0 as i64, manifest.work.entity.0 as i64, index.0 as i64, image])?;
        }
        let reference = self.read_reference(&tx, &manifest.work, manifest.attempt, index)?;
        tx.commit()?;
        Ok(reference)
    }
    fn read_reference(
        &self,
        connection: &Connection,
        work: &WorkKey,
        attempt: Id,
        index: OutputIndex,
    ) -> Result<RetainedReference> {
        let row = reference_row(connection, work, index)?
            .ok_or_else(|| Error::new(ErrorCode::NotFound, "no retained output selection"))?;
        let image = storage::read(connection, "result_references", "image", row)?
            .ok_or(JournalError::Corrupt("selected output record missing"))?;
        let key = key_bytes(&self.read_identity(connection)?, work, "result_references")?;
        let choice: ReferenceChoice = codec::decode(storage::unseal(&key, &image)?, MAX_HEADER)?;
        check(
            choice.index == index,
            "retained output selection index changed",
        )?;
        let manifest = self
            .read_manifest(connection, work)?
            .ok_or(JournalError::Corrupt("selected output lacks full manifest"))?;
        self.validate_manifest(connection, &manifest)?;
        check(
            choice.manifest == manifest.digest()?,
            "selected output manifest commitment changed",
        )?;
        if manifest.attempt != attempt || manifest.outputs.get(index.0 as usize).is_none() {
            return Err(Error::new(ErrorCode::NotFound, "retained object identity absent").into());
        }
        Ok(RetainedReference { manifest, index })
    }
    pub(super) fn audit_references(&self, connection: &Connection) -> Result<()> {
        let count: i64 =
            connection.query_row("SELECT count(*) FROM result_references", [], |r| r.get(0))?;
        if count < 0 || count as u64 > self.limits.observations.0 {
            return Err(JournalError::Corrupt(
                "result-reference inventory exceeds retained limit",
            ));
        }
        let mut statement = connection.prepare(
            "SELECT scope,producer,entity,output_index FROM result_references ORDER BY rowid",
        )?;
        let mut rows = statement.query([])?;
        while let Some(row) = rows.next()? {
            let work = WorkKey {
                scope: Number(row.get::<_, i64>(0)? as u64),
                producer: Producer(row.get::<_, i64>(1)? as u64),
                entity: Id(row.get::<_, i64>(2)? as u64),
            };
            let index = OutputIndex(row.get::<_, i64>(3)? as u64);
            work.check()?;
            index.check()?;
            let manifest = self
                .read_manifest(connection, &work)?
                .ok_or(JournalError::Corrupt("reference without retained manifest"))?;
            self.read_reference(connection, &work, manifest.attempt, index)?;
        }
        Ok(())
    }
}
fn reference_row(
    connection: &Connection,
    work: &WorkKey,
    index: OutputIndex,
) -> Result<Option<i64>> {
    work.check()?;
    index.check()?;
    Ok(connection.query_row("SELECT rowid FROM result_references WHERE scope=?1 AND producer=?2 AND entity=?3 AND output_index=?4",
        params![work.scope.0 as i64, work.producer.0 as i64, work.entity.0 as i64, index.0 as i64], |r| r.get(0)).optional()?)
}
