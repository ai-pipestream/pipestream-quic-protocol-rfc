//! Explicit offline reclamation. A membership declaration is not admission.

use super::*;
use pipestream_core::{
    jobs::JobInput,
    persistence::{SqliteSessionStore, StoreError},
};

/// Logical reservations before/after reconciliation and removed file-name lengths.
/// These are not allocated filesystem blocks. Immutable commitments, object and
/// owner identities, partial metadata, and final-lineage allowances remain charged.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Reconciliation {
    pub before: RetainedUsage,
    pub after: RetainedUsage,
    pub spool_files_removed: u64,
    pub staging_files_removed: u64,
    pub orphan_bodies_removed: u64,
    pub commitments_retained: u64,
    pub file_bytes_removed: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    Audited,
    StagingRemoved,
    CommitmentPublished,
    BodyRemoved,
}

impl RetainedRoot {
    pub(crate) fn reconcile(
        root: PathBuf,
        spool: super::super::SpoolLimits,
        sessions: &SqliteSessionStore,
    ) -> Result<Reconciliation, StoreError> {
        reconcile_with(root, spool, sessions, |_| Ok(()))
    }
}

// A private storage fault probe, never an application processor or public hook.
fn reconcile_with(
    root: PathBuf,
    spool: super::super::SpoolLimits,
    sessions: &SqliteSessionStore,
    probe: impl Fn(Phase) -> io::Result<()>,
) -> Result<Reconciliation, StoreError> {
    let root = RetainedRoot::open_mode(root, None, true)?;
    root.require_lineage_reservations()?;
    let expected = root
        .binding
        .lock()
        .map_err(|_| corrupt("retained binding poisoned"))?
        .ok_or_else(|| corrupt("maintenance requires a complete retained pair"))?;
    let mut database = sessions.payload_maintenance(expected)?;
    let spool = super::super::spool::SpoolMaintenance::open(root.path.join(".spool"), spool)?;
    let state = root
        .state
        .lock()
        .map_err(|_| corrupt("retained state poisoned"))?;
    let mut protected = BTreeSet::new();
    while let Some(retained) = database.next_session()? {
        let session = retained.session;
        let owner = session.owner.as_ref().map(|owner| {
            (
                owner.binding.authority.clone(),
                owner.binding.principal.clone(),
            )
        });
        if state
            .owners
            .get(&session.session_id)
            .is_some_and(|stored| stored != &owner)
        {
            return Err(StoreError::Protocol(unauthorized()));
        }
        for (key, job) in &session.jobs {
            if let JobInput::Process { length, digest, .. } = &job.input {
                let identity = (session.session_id.clone(), Some(key.entity));
                let entry = state.entries.get(&identity).ok_or_else(|| {
                    corrupt("admitted payload is missing its retained commitment")
                })?;
                if entry.reclaimed.is_some()
                    || !entry.committed
                    || entry.record.owner != owner
                    || entry.record.length != *length
                    || entry.record.digest != *digest
                {
                    return Err(corrupt(
                        "admitted input has no matching immutable payload receipt",
                    )
                    .into());
                }
                protected.insert(identity);
            }
        }
    }
    // Validate every installed body, not only inputs still in the ready queue.
    // An unpublished stage is not validated input, even when all expected bytes
    // were written: the installation might have rejected their checksum. Its
    // expected digest survives in the immutable record. Published stages are
    // already required to alias the verified body by the inventory audit.
    for entry in state.entries.values() {
        let record = &entry.record;
        let base = record.path(&root.path);
        if regular_length(&base, 2)?.is_some() {
            verify_file(&base, record.length, record.digest)?;
        }
    }
    probe(Phase::Audited)?;
    let mut report = Reconciliation {
        before: state.usage,
        ..Reconciliation::default()
    };
    for (path, length) in &spool.files {
        fs::remove_file(path)?;
        sync_directory(
            path.parent()
                .ok_or_else(|| corrupt("spool lacks directory"))?,
        )?;
        report.spool_files_removed += 1;
        report.file_bytes_removed += length;
    }
    // Removing a redundant admitted stage leaves its receipt/body intact. An
    // unadmitted stage is removed only after publishing its commitment below.
    for key in &protected {
        let entry = &state.entries[key];
        if remove_if_present(
            &suffix(&entry.record.path(&root.path), ".stage"),
            &mut report,
        )? {
            report.staging_files_removed += 1;
        }
    }
    probe(Phase::StagingRemoved)?;
    for (key, entry) in &state.entries {
        if key.1.is_none() || protected.contains(key) {
            continue;
        }
        let base = entry.record.path(&root.path);
        let parent = base
            .parent()
            .ok_or_else(|| corrupt("payload lacks directory"))?;
        if entry.reclaimed.is_none() {
            // This rename needs no new record or file quota, even at the exact
            // retention cap. No body or receipt is removed before directory sync.
            fs::rename(suffix(&base, ".meta"), suffix(&base, ".commit"))?;
        }
        File::open(suffix(&base, ".commit"))?.sync_all()?;
        sync_directory(parent)?;
        probe(Phase::CommitmentPublished)?;
        if remove_if_present(&suffix(&base, ".stage"), &mut report)? {
            report.staging_files_removed += 1;
        }
        // Receipt first: an interrupted cleanup must never expose a normal
        // committed object whose data disappeared. .commit is never executable.
        remove_if_present(&suffix(&base, ".done"), &mut report)?;
        if remove_if_present(&base, &mut report)? {
            report.orphan_bodies_removed += 1;
        }
        probe(Phase::BodyRemoved)?;
        report.commitments_retained += 1;
    }
    // No live handle can observe the stale in-memory charges. Prove reopen's
    // disk-derived inventory while retaining both the root and database locks.
    report.after = scan(&root.path, root.limits)?.usage;
    Ok(report)
}

fn remove_if_present(path: &Path, report: &mut Reconciliation) -> io::Result<bool> {
    let Some(length) = regular_length(path, 2)? else {
        return Ok(false);
    };
    fs::remove_file(path)?;
    sync_directory(
        path.parent()
            .ok_or_else(|| corrupt("retained file lacks directory"))?,
    )?;
    report.file_bytes_removed += length;
    Ok(true)
}

pub(super) fn scan_commitment(
    root: &Path,
    limits: RetainedLimits,
    path: &Path,
    files: &BTreeSet<PathBuf>,
    state: &mut State,
    accounted: &mut BTreeSet<PathBuf>,
) -> io::Result<()> {
    let record = Record::read(path)?;
    let base = record.path(root);
    if record.key.1.is_none()
        || suffix(&base, ".commit") != path
        || files.contains(&suffix(&base, ".meta"))
    {
        return Err(corrupt("invalid or duplicate reclaimed commitment"));
    }
    let stage = suffix(&base, ".stage");
    let receipt = suffix(&base, ".done");
    validate_payload_pair(&base, &stage)?;
    let body_length = regular_length(&base, 2)?;
    let stage_length = regular_length(&stage, 2)?;
    let receipt_length = regular_length(&receipt, 1)?;
    if body_length.is_some_and(|length| length != record.length)
        || stage_length.is_some_and(|length| length > record.length)
        || receipt_length.is_some_and(|length| length > RECEIPT_BYTES)
    {
        return Err(corrupt(
            "interrupted reclamation file exceeds its commitment",
        ));
    }
    if let Some(length) = receipt_length {
        let mut bytes = vec![0; length as usize];
        File::open(&receipt)?.read_exact(&mut bytes)?;
        if !record.encode()[480..].starts_with(&bytes) || body_length.is_none() {
            return Err(corrupt(
                "interrupted reclamation receipt differs or lacks its body",
            ));
        }
    }
    for file in [path.to_owned(), base, stage, receipt] {
        if files.contains(&file) {
            accounted.insert(file);
        }
    }
    state.insert(
        Entry {
            record,
            committed: false,
            staging: stage_length.is_some(),
            reclaimed: Some(Reclaimed {
                bytes: RECORD_BYTES as u64 + body_length.unwrap_or(0) + receipt_length.unwrap_or(0),
                staging_bytes: stage_length.unwrap_or(0),
                files_present: body_length.is_some()
                    || stage_length.is_some()
                    || receipt_length.is_some(),
            }),
        },
        limits,
    )
}

#[cfg(test)]
mod tests;
