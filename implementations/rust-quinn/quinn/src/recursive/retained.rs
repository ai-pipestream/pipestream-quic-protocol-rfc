//! Immutable retained objects with durable byte, object and staging reservations.
//! The root belongs to one cooperating writer process. No reclamation is implicit.

use super::{PrincipalBinding, sync_directory, validate_storage_session_id};
use pipestream_core::{
    MAX_ENTITY_ID, MAX_PAYLOAD, authorization::unauthorized, session::EntityKey,
};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File, OpenOptions},
    io::{self, Read, Write},
    path::{Path, PathBuf},
    sync::{Arc, Mutex, OnceLock, Weak},
};

const RECORD_BYTES: usize = 512;
const RECEIPT_BYTES: u64 = 32;
const POLICY_BYTES: usize = 96;
const MAX_ROOTS: usize = 64;
mod binding;
mod lineage;
mod reconcile;
use pipestream_core::persistence::{PayloadBinding, StoreIdentity};
pub use reconcile::Reconciliation;
type Owner = Option<(String, String)>;
type Key = (String, Option<EntityKey>);

/// Retained charges include the object, its 512-byte metadata and 32-byte receipt.
/// Staging is additional reserved capacity, held through physical cleanup.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetainedLimits {
    pub bytes: u64,
    pub principal_bytes: u64,
    pub objects: u64,
    pub principal_objects: u64,
    pub staging_bytes: u64,
    pub staging_objects: u64,
    pub principals: u64,
}

impl Default for RetainedLimits {
    fn default() -> Self {
        Self {
            bytes: 512 << 20,
            principal_bytes: 128 << 20,
            objects: 8192,
            principal_objects: 2048,
            staging_bytes: 128 << 20,
            staging_objects: 32,
            principals: 1024,
        }
    }
}

impl RetainedLimits {
    fn values(self) -> [u64; 7] {
        [
            self.bytes,
            self.principal_bytes,
            self.objects,
            self.principal_objects,
            self.staging_bytes,
            self.staging_objects,
            self.principals,
        ]
    }

    fn validate(self) -> io::Result<()> {
        if self.values().contains(&0)
            || self.bytes > 16 << 30
            || self.principal_bytes > self.bytes
            || self.objects > 65536
            || self.principal_objects > self.objects
            || self.staging_bytes > self.bytes
            || self.staging_objects > 1024
            || self.staging_objects > self.objects
            || self.principals > 4096
        {
            return Err(limit("invalid retained-payload limits"));
        }
        Ok(())
    }

    fn encode(self) -> [u8; POLICY_BYTES] {
        let mut bytes = [0; POLICY_BYTES];
        bytes[..8].copy_from_slice(b"PSRET004");
        for (slot, value) in bytes[8..64].chunks_exact_mut(8).zip(self.values()) {
            slot.copy_from_slice(&value.to_be_bytes());
        }
        let hash = Sha256::digest(&bytes[..64]);
        bytes[64..].copy_from_slice(&hash);
        bytes
    }

    fn read(path: &Path) -> io::Result<Self> {
        let bytes = read_fixed::<POLICY_BYTES>(path)?;
        if &bytes[..8] != b"PSRET004" || Sha256::digest(&bytes[..64])[..] != bytes[64..] {
            return Err(corrupt("retained policy checksum or version mismatch"));
        }
        let mut values = [0; 7];
        for (value, slot) in values.iter_mut().zip(bytes[8..64].chunks_exact(8)) {
            let mut octets = [0; 8];
            octets.copy_from_slice(slot);
            *value = u64::from_be_bytes(octets);
        }
        let limits = Self {
            bytes: values[0],
            principal_bytes: values[1],
            objects: values[2],
            principal_objects: values[3],
            staging_bytes: values[4],
            staging_objects: values[5],
            principals: values[6],
        };
        limits.validate()?;
        Ok(limits)
    }
}

/// Conservative reservations, including unfinished copies, not allocated disk blocks.
/// Global directory counts include empty directories left by interrupted creation.
/// The root, spool, 96-byte policy and empty lock file are separate fixed overhead.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RetainedUsage {
    pub bytes: u64,
    pub objects: u64,
    pub staging_bytes: u64,
    pub staging_objects: u64,
    /// Unacknowledged payload metadata prefixes with no durable owner yet.
    /// Each reserves one object and 512 bytes in the global totals above.
    pub incomplete_metadata: u64,
    /// Global-only directory reservation, bounded by twice the object limit.
    pub directories: u64,
    /// Fixed final-lineage file allowances, also included in bytes and objects.
    pub lineage_reservations: u64,
    /// Interrupted reservation markers with no durably identified principal yet.
    pub incomplete_lineage_reservations: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Record {
    key: Key,
    owner: Owner,
    length: u64,
    digest: [u8; 32],
}

impl Record {
    fn new(
        principal: Option<&PrincipalBinding>,
        session: &str,
        key: Option<EntityKey>,
        length: u64,
        digest: [u8; 32],
    ) -> io::Result<Self> {
        validate_storage_session_id(session)?;
        if let Some(key) = key {
            if key.entity_id == 0 || key.entity_id > MAX_ENTITY_ID || key.scope_id > MAX_ENTITY_ID {
                return Err(corrupt("reserved retained entity ID"));
            }
        } else if length != 32 {
            return Err(corrupt("lineage must contain one digest"));
        }
        if length > MAX_PAYLOAD as u64 {
            return Err(limit("retained object exceeds entity size limit"));
        }
        let owner = match principal {
            Some(p) => {
                PrincipalBinding::new(&p.authority, &p.principal).map_err(io::Error::other)?;
                Some((p.authority.clone(), p.principal.clone()))
            }
            None => None,
        };
        Ok(Self {
            key: (session.to_owned(), key),
            owner,
            length,
            digest,
        })
    }

    fn encode(&self) -> [u8; RECORD_BYTES] {
        let mut bytes = [0; RECORD_BYTES];
        bytes[..8].copy_from_slice(b"PSOBJ001");
        bytes[8] = u8::from(self.owner.is_some()) | (u8::from(self.key.1.is_none()) << 1);
        let (authority, principal) = self
            .owner
            .as_ref()
            .map(|(a, p)| (a.as_str(), p.as_str()))
            .unwrap_or(("", ""));
        for (slot, text) in
            bytes[9..396]
                .chunks_exact_mut(129)
                .zip([self.key.0.as_str(), authority, principal])
        {
            slot[0] = text.len() as u8;
            slot[1..1 + text.len()].copy_from_slice(text.as_bytes());
        }
        let key = self.key.1.unwrap_or(EntityKey {
            scope_id: 0,
            entity_id: 0,
        });
        bytes[396..400].copy_from_slice(&key.scope_id.to_be_bytes());
        bytes[400..404].copy_from_slice(&key.entity_id.to_be_bytes());
        bytes[404..412].copy_from_slice(&self.length.to_be_bytes());
        bytes[412..444].copy_from_slice(&self.digest);
        let hash = Sha256::digest(&bytes[..480]);
        bytes[480..].copy_from_slice(&hash);
        bytes
    }

    fn read(path: &Path) -> io::Result<Self> {
        let bytes = read_fixed::<RECORD_BYTES>(path)?;
        let mut strings = Vec::with_capacity(3);
        for slot in bytes[9..396].chunks_exact(129) {
            let len = slot[0] as usize;
            if len > 128 {
                return Err(corrupt("retained identity is too long"));
            }
            strings.push(
                std::str::from_utf8(&slot[1..1 + len])
                    .map_err(|_| corrupt("invalid retained identity"))?,
            );
        }
        let principal = if bytes[8] & 1 != 0 {
            Some(PrincipalBinding::new(strings[1], strings[2]).map_err(io::Error::other)?)
        } else {
            None
        };
        let mut scope = [0; 4];
        scope.copy_from_slice(&bytes[396..400]);
        let mut entity = [0; 4];
        entity.copy_from_slice(&bytes[400..404]);
        let key = if bytes[8] & 2 != 0 {
            None
        } else {
            Some(EntityKey {
                scope_id: u32::from_be_bytes(scope),
                entity_id: u32::from_be_bytes(entity),
            })
        };
        let mut length = [0; 8];
        length.copy_from_slice(&bytes[404..412]);
        let mut digest = [0; 32];
        digest.copy_from_slice(&bytes[412..444]);
        let record = Self::new(
            principal.as_ref(),
            strings[0],
            key,
            u64::from_be_bytes(length),
            digest,
        )?;
        if record.encode() != bytes {
            return Err(corrupt(
                "retained record checksum or canonical encoding mismatch",
            ));
        }
        Ok(record)
    }

    fn path(&self, root: &Path) -> PathBuf {
        match self.key.1 {
            Some(key) => root
                .join(&self.key.0)
                .join(format!("scope-{}", key.scope_id))
                .join(format!("entity-{}.bin", key.entity_id)),
            None => root.join(&self.key.0).join("lineage.sha256"),
        }
    }

    fn charge(&self) -> u64 {
        self.length + RECORD_BYTES as u64 + RECEIPT_BYTES
    }
}

#[derive(Debug, Clone)]
struct Entry {
    record: Record,
    committed: bool,
    staging: bool,
    reclaimed: Option<Reclaimed>,
}

// A .commit replaces .meta before any orphan body is removed. During an
// interrupted reconciliation, remaining file lengths stay charged. A restored
// .meta instead reserves the full original installation allowance again.
#[derive(Debug, Clone, Copy)]
struct Reclaimed {
    bytes: u64,
    staging_bytes: u64,
    files_present: bool,
}

impl Entry {
    fn charge(&self) -> u64 {
        self.reclaimed
            .map_or_else(|| self.record.charge(), |r| r.bytes)
    }

    fn staging_charge(&self) -> u64 {
        if !self.staging {
            0
        } else {
            self.reclaimed
                .map_or(self.record.length, |r| r.staging_bytes)
        }
    }
}

#[derive(Debug, Default)]
struct State {
    entries: BTreeMap<Key, Entry>,
    owners: BTreeMap<String, Owner>,
    usage: RetainedUsage,
    principals: BTreeMap<Owner, RetainedUsage>,
    active: BTreeSet<Key>,
    incomplete: BTreeMap<Key, Vec<u8>>,
    directories: BTreeSet<PathBuf>,
    lineages: BTreeMap<String, lineage::Reservation>,
    durable_lineages: BTreeSet<String>,
    incomplete_lineages: BTreeMap<String, Vec<u8>>,
}

impl State {
    fn insert(&mut self, entry: Entry, limits: RetainedLimits) -> io::Result<()> {
        let record = &entry.record;
        if record.key.1.is_none() {
            let reservation = self
                .lineages
                .get(&record.key.0)
                .ok_or_else(|| corrupt("final lineage lacks its admission reservation"))?;
            if reservation.owner != record.owner {
                return Err(io::Error::other(unauthorized()));
            }
            // The permanent reservation already covers this object, its stage,
            // metadata and receipt. It never borrows ordinary staging credit.
            self.entries.insert(record.key.clone(), entry);
            return Ok(());
        }
        if self
            .owners
            .get(&record.key.0)
            .is_some_and(|owner| owner != &record.owner)
        {
            return Err(io::Error::other(unauthorized()));
        }
        let prior = self
            .principals
            .get(&record.owner)
            .copied()
            .unwrap_or_default();
        let charge = entry.charge();
        let staging_charge = entry.staging_charge();
        if self.usage.bytes + charge > limits.bytes
            || self.usage.objects >= limits.objects
            || prior.bytes + charge > limits.principal_bytes
            || prior.objects >= limits.principal_objects
            || (!self.principals.contains_key(&record.owner)
                && self.principals.len() as u64 >= limits.principals)
            || (entry.staging
                && (self.usage.staging_bytes + staging_charge > limits.staging_bytes
                    || self.usage.staging_objects >= limits.staging_objects))
        {
            return Err(limit(
                "retained object or staging reservation budget exhausted",
            ));
        }
        let principal = self.principals.entry(record.owner.clone()).or_default();
        for usage in [&mut self.usage, principal] {
            usage.bytes += charge;
            usage.objects += 1;
            if entry.staging {
                usage.staging_bytes += staging_charge;
                usage.staging_objects += 1;
            }
        }
        self.owners
            .insert(record.key.0.clone(), record.owner.clone());
        self.entries.insert(record.key.clone(), entry);
        Ok(())
    }
}

#[derive(Debug)]
pub(crate) struct RetainedRoot {
    pub(crate) path: PathBuf,
    limits: RetainedLimits,
    state: Mutex<State>,
    identity: StoreIdentity,
    binding: Mutex<Option<PayloadBinding>>,
    exclusive: bool,
    _lock: RootLock,
}

#[derive(Debug)]
struct RootLock {
    file: File,
    owner_process: u32,
}

impl RootLock {
    fn acquire(file: File) -> io::Result<Self> {
        lock_root(&file)?;
        Ok(Self {
            file,
            owner_process: std::process::id(),
        })
    }
}

impl Drop for RootLock {
    fn drop(&mut self) {
        // flock belongs to the open-file description. Close alone can leave
        // ownership with an unrelated child's inherited descriptor until exec.
        // A forked child dropping a copied guard must not unlock its parent.
        #[cfg(unix)]
        if self.owner_process == std::process::id() {
            let _ = rustix::fs::flock(&self.file, rustix::fs::FlockOperation::Unlock);
        }
    }
}

impl RetainedRoot {
    pub(crate) fn open(root: PathBuf, requested: Option<RetainedLimits>) -> io::Result<Arc<Self>> {
        Self::open_mode(root, requested, false)
    }

    fn open_mode(
        root: PathBuf,
        requested: Option<RetainedLimits>,
        exclusive: bool,
    ) -> io::Result<Arc<Self>> {
        if let Some(limits) = requested {
            limits.validate()?;
        }
        if let Ok(meta) = fs::symlink_metadata(&root)
            && (!meta.is_dir() || meta.file_type().is_symlink())
        {
            return Err(corrupt("retained root must be a directory, not a symlink"));
        }
        let root = std::path::absolute(root)?;
        if exclusive {
            // Maintenance only opens a previously initialized, paired root. It
            // must not bootstrap missing policy, identity or ownership files.
            RetainedLimits::read(&root.join(".retained-policy"))?;
            let identity = binding::read_identity(&root)?;
            if binding::read_claim(&root, identity)?.is_none()
                || regular_length(&root.join(".retained-lock"), 1)? != Some(0)
            {
                return Err(corrupt(
                    "maintenance requires an existing paired retained root",
                ));
            }
        }
        let mut durable_parent = root.parent().unwrap_or(&root);
        while !durable_parent.try_exists()? {
            durable_parent = durable_parent
                .parent()
                .ok_or_else(|| corrupt("retained root has no existing ancestor"))?;
        }
        let durable_parent = durable_parent.canonicalize()?;
        fs::create_dir_all(&root)?;
        let root = root.canonicalize()?;
        static ROOTS: OnceLock<Mutex<BTreeMap<PathBuf, Weak<RetainedRoot>>>> = OnceLock::new();
        let mut roots = ROOTS
            .get_or_init(Mutex::default)
            .lock()
            .map_err(|_| corrupt("retained registry poisoned"))?;
        roots.retain(|_, weak| weak.strong_count() != 0);
        if let Some(existing) = roots.get(&root).and_then(Weak::upgrade) {
            if exclusive || existing.exclusive {
                return Err(io::Error::new(
                    io::ErrorKind::WouldBlock,
                    "retained root has a live handle or maintenance owner",
                ));
            }
            existing.verify_policy()?;
            if requested.is_some_and(|limits| limits != existing.limits) {
                return Err(corrupt("retained policy cannot change on reopen"));
            }
            return Ok(existing);
        }
        if roots.len() >= MAX_ROOTS {
            return Err(limit("too many retained stores"));
        }
        let lock_path = root.join(".retained-lock");
        if regular_length(&lock_path, 1)?.is_some_and(|length| length != 0) {
            return Err(corrupt("retained lock file must be empty"));
        }
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&lock_path)?;
        let lock = RootLock::acquire(lock)?;
        let policy = root.join(".retained-policy");
        if regular_length(&policy, 1)?.is_none() {
            for entry in fs::read_dir(&root)? {
                let entry = entry?;
                if ![".retained-lock", ".spool"].contains(&entry.file_name().to_str().unwrap_or(""))
                {
                    return Err(corrupt(
                        "existing retained objects lack quota policy; conversion refused",
                    ));
                }
            }
            binding::create_identity(&root)?;
            write_new(&policy, &requested.unwrap_or_default().encode())?;
            sync_directory(&root)?;
        }
        let limits = RetainedLimits::read(&policy)?;
        let identity = binding::read_identity(&root)?;
        let binding = binding::read_claim(&root, identity)?;
        if requested.is_some_and(|requested| requested != limits) {
            return Err(corrupt("retained policy cannot change on reopen"));
        }
        // Reopening a valid policy also covers a prior full write whose fsync
        // failed. Persist newly created root ancestors before admitting work.
        File::open(&policy)?.sync_all()?;
        sync_ancestors(&root, &durable_parent)?;
        let state = scan(&root, limits)?;
        let store = Arc::new(Self {
            path: root.clone(),
            limits,
            state: Mutex::new(state),
            identity,
            binding: Mutex::new(binding),
            exclusive,
            _lock: lock,
        });
        roots.insert(root, Arc::downgrade(&store));
        Ok(store)
    }

    pub(crate) fn limits(&self) -> RetainedLimits {
        self.limits
    }
    pub(crate) fn usage(
        &self,
        principal: Option<Option<&PrincipalBinding>>,
    ) -> io::Result<RetainedUsage> {
        if let Some(Some(p)) = principal {
            PrincipalBinding::new(&p.authority, &p.principal).map_err(io::Error::other)?;
        }
        let state = self
            .state
            .lock()
            .map_err(|_| corrupt("retained state poisoned"))?;
        Ok(match principal {
            None => state.usage,
            Some(p) => state
                .principals
                .get(&p.map(|p| (p.authority.clone(), p.principal.clone())))
                .copied()
                .unwrap_or_default(),
        })
    }

    fn verify_policy(&self) -> io::Result<()> {
        if RetainedLimits::read(&self.path.join(".retained-policy"))? != self.limits {
            return Err(corrupt("retained policy changed"));
        }
        if binding::read_identity(&self.path)? != self.identity {
            return Err(corrupt("retained-store identity changed"));
        }
        let binding = self
            .binding
            .lock()
            .map_err(|_| corrupt("retained binding lock poisoned"))?;
        if binding::read_claim(&self.path, self.identity)? != *binding {
            return Err(corrupt("retained database claim changed or disappeared"));
        }
        Ok(())
    }

    fn start(self: &Arc<Self>, record: Record) -> io::Result<Operation> {
        self.verify_policy()?;
        let mut state = self
            .state
            .lock()
            .map_err(|_| corrupt("retained state poisoned"))?;
        if state.active.len() as u64 >= self.limits.staging_objects
            || state.active.contains(&record.key)
        {
            return Err(limit("retained object already has an active writer"));
        }
        if let Some(prefix) = state.incomplete.get(&record.key)
            && !record.encode().starts_with(prefix)
        {
            return Err(corrupt(
                "retained metadata retry differs from its durable prefix",
            ));
        }
        let mut directories = BTreeSet::new();
        account_directories(&record.path(&self.path), &self.path, &mut directories);
        if state.directories.len() + directories.difference(&state.directories).count()
            > (2 * self.limits.objects) as usize
        {
            return Err(limit("retained directory reservation budget exhausted"));
        }
        let pending = match state.entries.get(&record.key) {
            Some(existing) if existing.record.owner != record.owner => {
                return Err(io::Error::other(unauthorized()));
            }
            Some(existing) if existing.record != record => {
                return Err(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    super::entity_error("immutable retained object differs"),
                ));
            }
            Some(existing) if existing.reclaimed.is_some() => {
                let reclaimed = existing.reclaimed.expect("checked above");
                if reclaimed.files_present {
                    return Err(corrupt(
                        "interrupted orphan reconciliation must finish before restore",
                    ));
                }
                let extra = record.charge() - reclaimed.bytes;
                let prior = state
                    .principals
                    .get(&record.owner)
                    .ok_or_else(|| corrupt("reclaimed owner charge missing"))?;
                if state.usage.bytes + extra > self.limits.bytes
                    || prior.bytes + extra > self.limits.principal_bytes
                    || state.usage.staging_bytes + record.length > self.limits.staging_bytes
                    || state.usage.staging_objects >= self.limits.staging_objects
                {
                    return Err(limit("retained restoration reservation budget exhausted"));
                }
                state.usage.bytes += extra;
                state.usage.staging_bytes += record.length;
                state.usage.staging_objects += 1;
                let principal = state
                    .principals
                    .get_mut(&record.owner)
                    .expect("checked above");
                principal.bytes += extra;
                principal.staging_bytes += record.length;
                principal.staging_objects += 1;
                let existing = state.entries.get_mut(&record.key).expect("checked above");
                existing.reclaimed = None;
                existing.staging = true;
                true
            }
            Some(existing) => !existing.committed,
            None => {
                let incomplete =
                    record.key.1.is_some() && state.incomplete.contains_key(&record.key);
                if incomplete {
                    state.usage.bytes -= RECORD_BYTES as u64;
                    state.usage.objects -= 1;
                    state.usage.incomplete_metadata -= 1;
                }
                let inserted = state.insert(
                    Entry {
                        record: record.clone(),
                        committed: false,
                        staging: true,
                        reclaimed: None,
                    },
                    self.limits,
                );
                if let Err(error) = inserted {
                    if incomplete {
                        state.usage.bytes += RECORD_BYTES as u64;
                        state.usage.objects += 1;
                        state.usage.incomplete_metadata += 1;
                    }
                    return Err(error);
                }
                state.incomplete.remove(&record.key);
                true
            }
        };
        state.directories.extend(directories);
        state.usage.directories = state.directories.len() as u64;
        state.active.insert(record.key.clone());
        Ok(Operation {
            root: self.clone(),
            record,
            pending,
        })
    }

    pub(crate) fn install(
        self: &Arc<Self>,
        principal: Option<&PrincipalBinding>,
        session: &str,
        key: Option<EntityKey>,
        length: u64,
        digest: [u8; 32],
        reader: impl Read,
    ) -> io::Result<()> {
        self.install_record(
            Record::new(principal, session, key, length, digest)?,
            reader,
        )
    }

    pub(crate) fn install_payload(
        self: &Arc<Self>,
        principal: Option<&PrincipalBinding>,
        session: &str,
        key: EntityKey,
        length: u64,
        digest: [u8; 32],
        reader: impl Read,
    ) -> io::Result<()> {
        // Reject invalid descriptors before retaining a session reservation.
        let record = Record::new(principal, session, Some(key), length, digest)?;
        self.reserve_lineage(principal, session)?;
        self.install_record(record, reader)
    }

    fn install_record(self: &Arc<Self>, record: Record, mut reader: impl Read) -> io::Result<()> {
        let operation = self.start(record)?;
        let record = &operation.record;
        let length = record.length;
        let digest = record.digest;
        let path = record.path(&self.path);
        let parent = path
            .parent()
            .ok_or_else(|| corrupt("retained object has no directory"))?;
        let metadata = suffix(&path, ".meta");
        let stage = suffix(&path, ".stage");
        let receipt = suffix(&path, ".done");
        let commitment = suffix(&path, ".commit");
        if regular_length(&commitment, 1)?.is_some() {
            if Record::read(&commitment)? != *record {
                return Err(corrupt(
                    "orphan commitment is not ready for matching restoration",
                ));
            }
            for file in [&metadata, &path, &stage, &receipt] {
                if regular_length(file, 2)?.is_some() {
                    return Err(corrupt("orphan commitment still has installation files"));
                }
            }
            // Rename the existing immutable metadata, never allocate a second
            // record at full quota. A crash now leaves a fully charged pending
            // installation that reconciliation or identical input can replay.
            fs::rename(&commitment, &metadata)?;
            sync_directory(parent)?;
        }
        let metadata_length = regular_length(&metadata, 1)?;
        if operation.pending && metadata_length.is_none_or(|size| size < RECORD_BYTES as u64) {
            fs::create_dir_all(parent)?;
            for file in [&path, &stage, &receipt] {
                if regular_length(file, 2)?.is_some() {
                    return Err(corrupt("unaccounted retained file"));
                }
            }
            write_prefix(&metadata, &record.encode())?;
            sync_ancestors(parent, &self.path)?;
        }
        if Record::read(&metadata)? != *record {
            return Err(corrupt("retained metadata changed"));
        }
        if operation.pending {
            // A prior attempt may have written all 512 bytes but failed before
            // fsync. A valid encoding alone is not a durable reservation.
            File::open(&metadata)?.sync_all()?;
            sync_ancestors(parent, &self.path)?;
        }
        validate_payload_pair(&path, &stage)?;
        let encoded = record.encode();
        if regular_length(&path, 2)?.is_some() {
            verify_reader(&mut reader, length, digest)?;
            verify_file(&path, length, digest)?;
        } else {
            if regular_length(&receipt, 1)?.is_some() {
                return Err(corrupt("committed retained payload is missing"));
            }
            let prefix_length = regular_length(&stage, 1)?.unwrap_or(0);
            if prefix_length > length {
                return Err(corrupt("retained stage exceeds reservation"));
            }
            let mut staged = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(&stage)?;
            let mut buffer = [0; super::PAYLOAD_IO_CHUNK];
            let mut prior = [0; super::PAYLOAD_IO_CHUNK];
            let mut measured = 0u64;
            let mut hasher = Sha256::new();
            loop {
                let count = reader.read(&mut buffer)?;
                if count == 0 {
                    break;
                }
                if measured + count as u64 > length {
                    return Err(corrupt("retained input exceeds reservation"));
                }
                let existing = (prefix_length.saturating_sub(measured)).min(count as u64) as usize;
                if existing != 0 {
                    staged.read_exact(&mut prior[..existing])?;
                    if prior[..existing] != buffer[..existing] {
                        return Err(corrupt("interrupted retained prefix differs"));
                    }
                }
                staged.write_all(&buffer[existing..count])?;
                measured += count as u64;
                hasher.update(&buffer[..count]);
            }
            if measured != length || <[u8; 32]>::from(hasher.finalize()) != digest {
                return Err(corrupt("retained input differs from immutable descriptor"));
            }
            staged.sync_all()?;
            fs::hard_link(&stage, &path)?;
            sync_directory(parent)?;
        }
        write_prefix(&receipt, &encoded[480..])?;
        if read_fixed::<32>(&receipt)?[..] != encoded[480..] {
            return Err(corrupt("retained receipt differs"));
        }
        sync_directory(parent)?;
        if regular_length(&stage, 2)?.is_some() {
            verify_file(&stage, length, digest)?;
            fs::remove_file(&stage)?;
            sync_directory(parent)?;
        }
        let mut state = self
            .state
            .lock()
            .map_err(|_| corrupt("retained state poisoned"))?;
        let entry = state
            .entries
            .get_mut(&record.key)
            .ok_or_else(|| corrupt("retained reservation disappeared"))?;
        entry.committed = true;
        let release = entry.staging;
        entry.staging = false;
        if release && record.key.1.is_some() {
            state.usage.staging_bytes -= length;
            state.usage.staging_objects -= 1;
            let owner = state
                .principals
                .get_mut(&record.owner)
                .ok_or_else(|| corrupt("retained owner charge missing"))?;
            owner.staging_bytes -= length;
            owner.staging_objects -= 1;
        }
        Ok(())
    }

    pub(crate) fn load(
        self: &Arc<Self>,
        principal: Option<&PrincipalBinding>,
        session: &str,
        key: EntityKey,
        length: u64,
        digest: [u8; 32],
    ) -> io::Result<super::Payload> {
        let record = Record::new(principal, session, Some(key), length, digest)?;
        self.verify_policy()?;
        {
            let state = self
                .state
                .lock()
                .map_err(|_| corrupt("retained state poisoned"))?;
            let entry = state
                .entries
                .get(&record.key)
                .ok_or_else(|| corrupt("retained input has no reservation"))?;
            if entry.record.owner != record.owner {
                return Err(io::Error::other(unauthorized()));
            }
            if entry.record != record || !entry.committed {
                return Err(corrupt("retained input has no matching committed receipt"));
            }
        }
        let path = record.path(&self.path);
        regular_length(&path, 2)?;
        if Record::read(&suffix(&path, ".meta"))? != record
            || read_fixed::<32>(&suffix(&path, ".done"))?[..] != record.encode()[480..]
        {
            return Err(corrupt("retained metadata or receipt changed"));
        }
        super::Payload::open_retained_owned(path, length, digest, self.clone())
    }
}

struct Operation {
    root: Arc<RetainedRoot>,
    record: Record,
    pending: bool,
}
impl Drop for Operation {
    fn drop(&mut self) {
        if let Ok(mut state) = self.root.state.lock() {
            state.active.remove(&self.record.key);
        }
    }
}

fn scan(root: &Path, limits: RetainedLimits) -> io::Result<State> {
    let mut files = BTreeSet::new();
    let mut directories = BTreeSet::new();
    let mut pending = vec![(root.to_path_buf(), 0)];
    let mut count = 0u64;
    while let Some((dir, depth)) = pending.pop() {
        for entry in fs::read_dir(&dir)? {
            let entry = entry?;
            count += 1;
            if count > 6 * limits.objects + 6 {
                return Err(limit("retained directory metadata budget exhausted"));
            }
            let path = entry.path();
            let kind = entry.file_type()?;
            if depth == 0
                && [
                    ".retained-policy",
                    ".retained-lock",
                    ".retained-identity",
                    ".session-store",
                ]
                .contains(&entry.file_name().to_str().unwrap_or(""))
            {
                regular_length(&path, 1)?;
                continue;
            }
            if depth == 0 && entry.file_name() == ".spool" {
                if !kind.is_dir() {
                    return Err(corrupt("invalid spool directory"));
                }
                continue;
            }
            if kind.is_dir() && depth < 2 {
                validate_directory(root, &path)?;
                directories.insert(path.clone());
                if directories.len() as u64 > 2 * limits.objects {
                    return Err(limit("retained directory reservation budget exhausted"));
                }
                pending.push((path, depth + 1));
            } else if kind.is_file() {
                regular_length(&path, 2)?;
                files.insert(path);
            } else {
                return Err(corrupt("unexpected retained path or symlink"));
            }
        }
    }
    let mut state = State::default();
    let mut accounted = BTreeSet::new();
    lineage::scan(root, limits, &files, &mut state, &mut accounted)?;
    for path in files.iter().filter(|path| {
        path.extension()
            .is_some_and(|e| e == "meta" || e == "commit")
    }) {
        if path.extension().is_some_and(|e| e == "commit") {
            reconcile::scan_commitment(root, limits, path, &files, &mut state, &mut accounted)?;
            continue;
        }
        let metadata_length =
            regular_length(path, 1)?.ok_or_else(|| corrupt("retained metadata disappeared"))?;
        if metadata_length < RECORD_BYTES as u64 {
            let key = key_from_metadata_path(root, path)?;
            let base = metadata_base(path)?;
            for candidate in [&base, &suffix(&base, ".stage"), &suffix(&base, ".done")] {
                if files.contains(candidate) {
                    return Err(corrupt("incomplete metadata has associated payload files"));
                }
            }
            let prepaid = key.1.is_none();
            if prepaid && !state.lineages.contains_key(&key.0) {
                return Err(corrupt("incomplete lineage metadata lacks its reservation"));
            }
            if !prepaid
                && (state.usage.bytes + RECORD_BYTES as u64 > limits.bytes
                    || state.usage.objects >= limits.objects)
            {
                return Err(limit("incomplete metadata exceeds retained global budget"));
            }
            let mut prefix = vec![0; metadata_length as usize];
            File::open(path)?.read_exact(&mut prefix)?;
            state.incomplete.insert(key, prefix);
            if !prepaid {
                state.usage.bytes += RECORD_BYTES as u64;
                state.usage.objects += 1;
                state.usage.incomplete_metadata += 1;
            }
            accounted.insert(path.clone());
            continue;
        }
        let record = Record::read(path)?;
        let base = record.path(root);
        if suffix(&base, ".meta") != *path {
            return Err(corrupt("retained metadata path identity mismatch"));
        }
        let stage = suffix(&base, ".stage");
        let receipt = suffix(&base, ".done");
        validate_payload_pair(&base, &stage)?;
        let receipt_length = regular_length(&receipt, 1)?;
        let committed = receipt_length == Some(RECEIPT_BYTES);
        if let Some(length) = receipt_length
            && length < RECEIPT_BYTES
        {
            let mut prefix = vec![0; length as usize];
            File::open(&receipt)?.read_exact(&mut prefix)?;
            if !record.encode()[480..].starts_with(&prefix) {
                return Err(corrupt("retained receipt prefix differs"));
            }
        } else if receipt_length.is_some_and(|length| length > RECEIPT_BYTES) {
            return Err(corrupt("retained receipt exceeds its bound"));
        }
        if (receipt_length.is_some() && !files.contains(&base))
            || (committed && read_fixed::<32>(&receipt)?[..] != record.encode()[480..])
        {
            return Err(corrupt("retained receipt or payload missing"));
        }
        if let Some(length) = regular_length(&base, 2)?
            && length != record.length
        {
            return Err(corrupt("retained length differs"));
        }
        if let Some(length) = regular_length(&stage, 2)?
            && length > record.length
        {
            return Err(corrupt("retained stage exceeds reservation"));
        }
        let staging = !committed || files.contains(&stage);
        for candidate in [path.clone(), base.clone(), stage, receipt] {
            if files.contains(&candidate) {
                accounted.insert(candidate);
            }
        }
        state.insert(
            Entry {
                record,
                committed,
                staging,
                reclaimed: None,
            },
            limits,
        )?;
    }
    if files != accounted {
        return Err(corrupt("unaccounted retained files"));
    }
    state.usage.directories = directories.len() as u64;
    state.directories = directories;
    Ok(state)
}

fn validate_directory(root: &Path, path: &Path) -> io::Result<()> {
    let relative = path
        .strip_prefix(root)
        .map_err(|_| corrupt("directory escaped root"))?;
    let parts: Vec<_> = relative.iter().map(|s| s.to_str()).collect();
    match parts.as_slice() {
        [Some(session)] => validate_storage_session_id(session),
        [Some(session), Some(scope)] => {
            validate_storage_session_id(session)?;
            let id: u32 = scope
                .strip_prefix("scope-")
                .and_then(|s| s.parse().ok())
                .ok_or_else(|| corrupt("invalid retained scope directory"))?;
            if id > MAX_ENTITY_ID || *scope != format!("scope-{id}") {
                return Err(corrupt("noncanonical retained scope directory"));
            }
            Ok(())
        }
        _ => Err(corrupt("invalid retained directory")),
    }
}

#[cfg(unix)]
fn lock_root(lock: &File) -> io::Result<()> {
    rustix::fs::flock(lock, rustix::fs::FlockOperation::NonBlockingLockExclusive)
        .map_err(io::Error::from)
}

#[cfg(not(unix))]
fn lock_root(_lock: &File) -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "retained store requires Unix advisory locking",
    ))
}

// Called with exclusive object-writer ownership, or during quiescent reopen.
// The only permitted alias is our own no-replace stage/publication pair.
fn validate_payload_pair(base: &Path, stage: &Path) -> io::Result<()> {
    regular_length(base, 2)?;
    regular_length(stage, 2)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        let metadata = |path: &Path| match fs::symlink_metadata(path) {
            Ok(value) => Ok(Some(value)),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error),
        };
        match (metadata(base)?, metadata(stage)?) {
            (Some(a), Some(b))
                if a.dev() == b.dev() && a.ino() == b.ino() && a.nlink() == 2 && b.nlink() == 2 => {
            }
            (Some(a), None) | (None, Some(a)) if a.nlink() == 1 => {}
            (None, None) => {}
            _ => return Err(corrupt("unexpected retained payload alias")),
        }
    }
    Ok(())
}

fn suffix(path: &Path, suffix: &str) -> PathBuf {
    let mut name = path.as_os_str().to_owned();
    name.push(suffix);
    PathBuf::from(name)
}

fn account_directories(base: &Path, root: &Path, directories: &mut BTreeSet<PathBuf>) {
    let mut parent = base.parent();
    while let Some(dir) = parent {
        if dir == root {
            break;
        }
        directories.insert(dir.to_owned());
        parent = dir.parent();
    }
}

fn metadata_base(path: &Path) -> io::Result<PathBuf> {
    let name = path
        .file_name()
        .and_then(|s| s.to_str())
        .and_then(|s| s.strip_suffix(".meta"))
        .ok_or_else(|| corrupt("invalid retained metadata filename"))?;
    Ok(path.with_file_name(name))
}

fn key_from_metadata_path(root: &Path, path: &Path) -> io::Result<Key> {
    let base = metadata_base(path)?;
    let relative = base
        .strip_prefix(root)
        .map_err(|_| corrupt("metadata escaped retained root"))?;
    let parts: Vec<_> = relative
        .iter()
        .map(|s| s.to_str().ok_or_else(|| corrupt("invalid retained path")))
        .collect::<io::Result<_>>()?;
    let key = match parts.as_slice() {
        [session, "lineage.sha256"] => ((*session).to_owned(), None),
        [session, scope, file] => {
            let scope_id: u32 = scope
                .strip_prefix("scope-")
                .and_then(|s| s.parse().ok())
                .ok_or_else(|| corrupt("invalid retained scope path"))?;
            let entity_id: u32 = file
                .strip_prefix("entity-")
                .and_then(|s| s.strip_suffix(".bin"))
                .and_then(|s| s.parse().ok())
                .ok_or_else(|| corrupt("invalid retained entity path"))?;
            (
                (*session).to_owned(),
                Some(EntityKey {
                    scope_id,
                    entity_id,
                }),
            )
        }
        _ => return Err(corrupt("invalid retained metadata path")),
    };
    let record = Record::new(
        None,
        &key.0,
        key.1,
        if key.1.is_none() { 32 } else { 0 },
        [0; 32],
    )?;
    if record.path(root) != base {
        return Err(corrupt("noncanonical retained metadata path"));
    }
    Ok(key)
}

// Resume only a matching durable prefix. Never truncate metadata or replace an
// acknowledged receipt; the caller has already reserved the complete length.
fn write_prefix(path: &Path, expected: &[u8]) -> io::Result<()> {
    let length = regular_length(path, 1)?.unwrap_or(0);
    if length > expected.len() as u64 {
        return Err(corrupt("metadata exceeds reserved length"));
    }
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(path)?;
    let mut prefix = vec![0; length as usize];
    file.read_exact(&mut prefix)?;
    if !expected.starts_with(&prefix) {
        return Err(corrupt("metadata differs from retained prefix"));
    }
    file.write_all(&expected[prefix.len()..])?;
    file.sync_all()
}
fn limit(detail: &str) -> io::Error {
    io::Error::other(super::limit_error(detail))
}
fn corrupt(detail: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, detail)
}

fn regular_length(path: &Path, links: u64) -> io::Result<Option<u64>> {
    let meta = match fs::symlink_metadata(path) {
        Ok(m) => m,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e),
    };
    if !meta.is_file() {
        return Err(corrupt("retained file must be regular, not a symlink"));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if meta.nlink() > links {
            return Err(corrupt("unexpected retained hardlink alias"));
        }
    }
    Ok(Some(meta.len()))
}
fn read_fixed<const N: usize>(path: &Path) -> io::Result<[u8; N]> {
    if regular_length(path, 1)? != Some(N as u64) {
        return Err(corrupt("retained metadata length mismatch"));
    }
    let mut bytes = [0; N];
    let mut file = File::open(path)?;
    file.read_exact(&mut bytes)?;
    if file.read(&mut [0])? != 0 {
        return Err(corrupt("retained metadata grew"));
    }
    Ok(bytes)
}
fn write_new(path: &Path, bytes: &[u8]) -> io::Result<()> {
    let mut file = OpenOptions::new().write(true).create_new(true).open(path)?;
    file.write_all(bytes)?;
    file.sync_all()
}
fn sync_ancestors(path: &Path, root: &Path) -> io::Result<()> {
    let mut path = path;
    loop {
        sync_directory(path)?;
        if path == root {
            return Ok(());
        }
        path = path
            .parent()
            .ok_or_else(|| corrupt("retained path escaped root"))?;
    }
}
fn verify_reader(reader: &mut impl Read, length: u64, expected: [u8; 32]) -> io::Result<()> {
    let mut hasher = Sha256::new();
    let mut buffer = [0; super::PAYLOAD_IO_CHUNK];
    let mut measured = 0;
    loop {
        let count = reader.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        measured += count as u64;
        if measured > length {
            return Err(corrupt("retained file exceeds declared length"));
        }
        hasher.update(&buffer[..count]);
    }
    if measured != length || <[u8; 32]>::from(hasher.finalize()) != expected {
        return Err(corrupt("retained file checksum or length mismatch"));
    }
    Ok(())
}
fn verify_file(path: &Path, length: u64, expected: [u8; 32]) -> io::Result<()> {
    if regular_length(path, 2)? != Some(length) {
        return Err(corrupt("retained object length mismatch"));
    }
    verify_reader(&mut File::open(path)?, length, expected)
}

#[cfg(test)]
mod tests;
