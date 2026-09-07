//! Reconcile the authority's live promises with the exclusively owned root.
//! Stream one job at a time; this is an all-record startup/admission audit,
//! not a constant-time check or a hash of every retained payload body.

use super::*;

fn object(root: &Root, key: &str, entry: &Entry, releasing: bool) -> Result<()> {
    match read_header(&root.path(key, entry.incomplete), false) {
        Ok((header, offset))
            if !entry.incomplete && header == entry.header()? && offset == entry.offset =>
        {
            Ok(())
        }
        Err(StoreError::Io(error)) if releasing && error.kind() == std::io::ErrorKind::NotFound => {
            Ok(())
        }
        _ => Err(StoreError::Corrupt(
            "retained payload file differs from inventory",
        )),
    }
}

impl PayloadStore {
    /// The caller holds the authority writer, before opening any inventory
    /// lock. Release intent is the only authorization for missing live bytes.
    pub(in crate::v2::authority) fn audit_references(&self, tx: &Transaction<'_>) -> Result<()> {
        let entries = self.root.entries()?;
        let authority =
            IdentityLabel(tx.query_row("SELECT name FROM authority", [], |r| r.get(0))?);
        let mut statement = tx.prepare("SELECT j.work_row,w.generation,s.owner FROM jobs j JOIN work w ON w.row_id=j.work_row JOIN sessions s ON s.generation=w.generation ORDER BY j.work_row")?;
        let mut rows = statement.query([])?;
        while let Some(row) = rows.next()? {
            let work_row = row.get(0)?;
            let (_, job): (_, jobs::JobRecord) = records::read(
                tx,
                records::Target {
                    table: records::Table::Job,
                    row: work_row,
                },
            )?;
            let (_, view): (_, WorkView) = records::read(
                tx,
                records::Target {
                    table: records::Table::Work,
                    row: work_row,
                },
            )?;
            let identity = SessionIdentity {
                authority: authority.clone(),
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
            if let Some(release) = &job.release
                && retiring.is_none()
            {
                super::super::retention::verify_release(tx, &identity, &view, release)?;
            }
            let input_releasing = job.release.as_ref().is_some_and(|r| r.input);
            let outputs_releasing = job.release.as_ref().is_some_and(|r| r.outputs);
            match entries.get(&job.input_key.0) {
                Some(entry) => {
                    if !job.input_live
                        || entry.owner != identity.owner
                        || entry.input.as_ref() != Some(&job.parameters.input)
                        || entry.funding.is_some()
                    {
                        return Err(StoreError::Corrupt(
                            "retained input binding differs from job",
                        ));
                    }
                    object(&self.root, &job.input_key.0, entry, input_releasing)?;
                }
                None if !input_releasing => {
                    return Err(StoreError::Corrupt("required job input absent"));
                }
                None => {}
            }
            match entries.reservations.get(&job.reservation_key.0) {
                Some(entry) => {
                    if !job.outputs_live
                        || entry.incomplete
                        || entry.owner != identity.owner
                        || entry.budget != job.parameters.outputs
                    {
                        return Err(StoreError::Corrupt(
                            "retained output reservation differs from job",
                        ));
                    }
                    match reservations::read(&reservations::path(
                        &self.root,
                        &job.reservation_key.0,
                        false,
                    )) {
                        Ok(retained)
                            if retained.owner == entry.owner && retained.budget == entry.budget => {
                        }
                        Err(StoreError::Io(error))
                            if outputs_releasing
                                && error.kind() == std::io::ErrorKind::NotFound => {}
                        _ => {
                            return Err(StoreError::Corrupt(
                                "required output reservation file differs from inventory",
                            ));
                        }
                    }
                }
                None if !outputs_releasing => {
                    return Err(StoreError::Corrupt("required output reservation absent"));
                }
                None => {}
            }
            if let Some(manifest) = &view.manifest {
                for output in &manifest.outputs {
                    let found = entries.iter().find(|(_, entry)| {
                        entry.funding.as_ref().is_some_and(|f| {
                            f.key == job.reservation_key.0 && f.index == output.index
                        })
                    });
                    match found {
                        Some((key, entry)) => {
                            let expected = Input {
                                length: output.length,
                                sha256: output.sha256,
                                content_type: output.content_type.clone(),
                            };
                            if !job.outputs_live
                                || entry.owner != identity.owner
                                || entry.input.as_ref() != Some(&expected)
                            {
                                return Err(StoreError::Corrupt(
                                    "retained output binding differs from manifest",
                                ));
                            }
                            object(&self.root, key, entry, outputs_releasing)?;
                        }
                        None if !outputs_releasing => {
                            return Err(StoreError::Corrupt("required manifest output absent"));
                        }
                        None => {}
                    }
                }
            }
        }
        Ok(())
    }
}

impl AuthorityStore {
    /// Audit the bound root on startup without granting a new UTC promise.
    /// A pending, committed cleanup can legitimately have already deleted files.
    pub fn audit_payloads(&self, payloads: &PayloadStore) -> Result<()> {
        let mut connection = self.connect()?;
        let tx = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        execution::bound_payloads(&tx, payloads)?;
        payloads.audit_references(&tx)
    }
}
