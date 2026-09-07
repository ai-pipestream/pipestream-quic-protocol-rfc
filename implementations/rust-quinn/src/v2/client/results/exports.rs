//! Recoverable raw exports inside an explicitly initialized private directory.
//! Immutable intent precedes copying. Files outside this directory are never
//! scanned, adopted, overwritten, or removed. Calls perform blocking storage I/O.
use super::*;
use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt},
    path::PathBuf,
    sync::{
        Mutex,
        atomic::{AtomicBool, Ordering},
    },
};

const CONFIG: &str = "binding";
const LOCK: &str = "export.lock";
const RECORD: usize = 112;
const CHUNK: usize = 8192;
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ExportPolicy {
    pub objects: u64,
    pub bytes: u64,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ExportUsage {
    pub intents: usize,
    pub charged_bytes: u64,
    pub recovered_stages: usize,
}
#[derive(Debug)]
pub struct Exported {
    pub path: PathBuf,
    pub replayed: bool,
}
#[derive(Clone, PartialEq, Eq)]
struct Intent {
    length: u64,
    sha256: Digest,
    selection: Digest,
}
impl Intent {
    fn encode(&self) -> [u8; RECORD] {
        let mut bytes = [0; RECORD];
        bytes[..8].copy_from_slice(b"PSXINT01");
        bytes[8..16].copy_from_slice(&self.length.to_be_bytes());
        bytes[16..48].copy_from_slice(&self.sha256.0);
        bytes[48..80].copy_from_slice(&self.selection.0);
        let checksum = Sha256::digest(&bytes[..80]);
        bytes[80..].copy_from_slice(&checksum);
        bytes
    }
    fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() != RECORD
            || &bytes[..8] != b"PSXINT01"
            || Sha256::digest(&bytes[..80])[..] != bytes[80..]
        {
            return Err(StoreError::Corrupt("invalid export intent"));
        }
        Ok(Self {
            length: u64::from_be_bytes(bytes[8..16].try_into().expect("fixed slice")),
            sha256: Digest(bytes[16..48].try_into().expect("fixed slice")),
            selection: Digest(bytes[48..80].try_into().expect("fixed slice")),
        })
    }
}
fn error(code: ErrorCode, detail: &'static str) -> StoreError {
    StoreError::Protocol(Error { code, detail })
}
fn regular(path: &Path) -> Result<File> {
    let file = OpenOptions::new()
        .read(true)
        .custom_flags((rustix::fs::OFlags::NOFOLLOW | rustix::fs::OFlags::NONBLOCK).bits() as i32)
        .open(path)?;
    let meta = file.metadata()?;
    if !meta.is_file() || meta.nlink() != 1 {
        return Err(StoreError::Corrupt(
            "export entry is not private regular storage",
        ));
    }
    Ok(file)
}
fn read_fixed(path: &Path, size: usize) -> Result<Vec<u8>> {
    let mut file = regular(path)?;
    if file.metadata()?.len() != size as u64 {
        return Err(StoreError::Corrupt("export record length changed"));
    }
    let mut bytes = vec![0; size];
    file.read_exact(&mut bytes)?;
    Ok(bytes)
}
fn create(path: &Path) -> Result<File> {
    Ok(OpenOptions::new()
        .write(true)
        .read(true)
        .create_new(true)
        .mode(0o600)
        .open(path)?)
}
fn key(id: OperationId) -> Result<String> {
    if id.0 == [0; 16] {
        return Err(error(ErrorCode::Conflict, "zero export identity"));
    }
    Ok(id.0.iter().map(|b| format!("{b:02x}")).collect())
}
fn valid_key(key: &str) -> bool {
    key.len() == 32
        && key
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        && key != "0".repeat(32)
}
fn configuration(
    authority: &IdentityLabel,
    owner: &IdentityLabel,
    policy: ExportPolicy,
) -> Result<Vec<u8>> {
    if policy.objects == 0 || policy.objects > 1_000_000 || policy.bytes > MAX_NUMBER {
        return Err(error(ErrorCode::LimitExceeded, "invalid export policy"));
    }
    let binding = super::binding(authority, owner)?;
    let mut bytes = b"PSXROOT1".to_vec();
    bytes.extend(binding.as_bytes());
    bytes.extend(policy.objects.to_be_bytes());
    bytes.extend(policy.bytes.to_be_bytes());
    bytes.extend(Sha256::digest(&bytes));
    Ok(bytes)
}

pub struct ExportStore {
    path: PathBuf,
    authority: IdentityLabel,
    owner: IdentityLabel,
    policy: ExportPolicy,
    intents: Mutex<BTreeMap<String, Intent>>,
    recovered: usize,
    uncertain: AtomicBool,
    lock: File,
    process: u32,
}
impl Drop for ExportStore {
    fn drop(&mut self) {
        if self.process == std::process::id() {
            let _ = rustix::fs::flock(&self.lock, rustix::fs::FlockOperation::Unlock);
        }
    }
}
impl ExportStore {
    pub fn initialize(
        path: &Path,
        authority: IdentityLabel,
        owner: IdentityLabel,
        policy: ExportPolicy,
    ) -> Result<Self> {
        let bytes = configuration(&authority, &owner, policy)?;
        fs::create_dir(path)?;
        fs::set_permissions(path, fs::Permissions::from_mode(0o700))?;
        let mut file = create(&path.join(CONFIG))?;
        file.write_all(&bytes)?;
        file.sync_all()?;
        create(&path.join(LOCK))?.sync_all()?;
        crate::persistence::sync_directory(path)?;
        crate::persistence::sync_directory(
            path.parent()
                .filter(|p| !p.as_os_str().is_empty())
                .unwrap_or(Path::new(".")),
        )?;
        Self::open(path, authority, owner, policy)
    }
    pub fn open(
        path: &Path,
        authority: IdentityLabel,
        owner: IdentityLabel,
        policy: ExportPolicy,
    ) -> Result<Self> {
        let expected = configuration(&authority, &owner, policy)?;
        let meta = fs::symlink_metadata(path)?;
        if !meta.is_dir() || meta.permissions().mode() & 0o077 != 0 {
            return Err(StoreError::Corrupt("export directory is not private"));
        }
        let path = path.canonicalize()?;
        let lock = regular(&path.join(LOCK))?;
        if lock.metadata()?.len() != 0 {
            return Err(StoreError::Corrupt("export lock is not empty"));
        }
        rustix::fs::flock(&lock, rustix::fs::FlockOperation::NonBlockingLockExclusive)
            .map_err(|_| error(ErrorCode::Conflict, "export directory already owned"))?;
        if read_fixed(&path.join(CONFIG), expected.len())? != expected {
            return Err(StoreError::Corrupt("export identity or policy changed"));
        }
        let mut intents = BTreeMap::new();
        let mut files = BTreeMap::new();
        let mut temporaries = vec![];
        let mut scanned = 0;
        for entry in fs::read_dir(&path)? {
            let entry = entry?;
            let name = entry
                .file_name()
                .into_string()
                .map_err(|_| StoreError::Corrupt("non-UTF8 export entry"))?;
            if name == CONFIG || name == LOCK {
                continue;
            }
            scanned += 1;
            if scanned > policy.objects * 3 {
                return Err(StoreError::Corrupt("export entry ceiling exceeded"));
            }
            let (kind, id) = name
                .split_once('-')
                .ok_or(StoreError::Corrupt("unknown export entry"))?;
            if !valid_key(id) {
                return Err(StoreError::Corrupt("invalid export entry identity"));
            }
            match kind {
                "record" => {
                    intents.insert(
                        id.to_owned(),
                        Intent::decode(&read_fixed(&entry.path(), RECORD)?)?,
                    );
                }
                "file" | "stage" => {
                    if files
                        .insert(
                            id.to_owned(),
                            (kind.to_owned(), regular(&entry.path())?.metadata()?.len()),
                        )
                        .is_some()
                    {
                        return Err(StoreError::Corrupt("duplicate export body"));
                    }
                }
                "intent" => {
                    if regular(&entry.path())?.metadata()?.len() > RECORD as u64 {
                        return Err(StoreError::Corrupt("unfinished intent exceeds bound"));
                    }
                    temporaries.push((id.to_owned(), entry.path()));
                }
                _ => return Err(StoreError::Corrupt("unknown export entry")),
            }
        }
        let charged = Self::charge(&intents, policy)?;
        if intents.len() + temporaries.len() > policy.objects as usize {
            return Err(StoreError::Corrupt("export intent count exceeds quota"));
        }
        if charged + temporaries.len() as u64 * RECORD as u64 > policy.bytes {
            return Err(StoreError::Corrupt(
                "unfinished export intents exceed quota",
            ));
        }
        for (id, (kind, length)) in &files {
            let intent = intents
                .get(id)
                .ok_or(StoreError::Corrupt("export body has no committed intent"))?;
            if *length > intent.length || (kind == "file" && *length != intent.length) {
                return Err(StoreError::Corrupt("export body length changed"));
            }
        }
        for (id, _) in &temporaries {
            if intents.contains_key(id) || files.contains_key(id) {
                return Err(StoreError::Corrupt("ambiguous export intent"));
            }
        }
        // No deletion precedes the complete identity, inventory and quota audit.
        let mut recovered = 0;
        for (_, path) in temporaries {
            fs::remove_file(path)?;
            recovered += 1;
        }
        for (id, (kind, _)) in files {
            if kind == "stage" {
                fs::remove_file(path.join(format!("stage-{id}")))?;
                recovered += 1;
            }
        }
        crate::persistence::sync_directory(&path)?;
        Ok(Self {
            path,
            authority,
            owner,
            policy,
            intents: Mutex::new(intents),
            recovered,
            uncertain: AtomicBool::new(false),
            lock,
            process: std::process::id(),
        })
    }
    fn check(&self) -> Result<()> {
        if self.process != std::process::id() || self.uncertain.load(Ordering::Acquire) {
            return Err(StoreError::Corrupt(
                "export owner requires exclusive reopen",
            ));
        }
        Ok(())
    }
    fn charge(intents: &BTreeMap<String, Intent>, policy: ExportPolicy) -> Result<u64> {
        let total = intents.values().try_fold(0u64, |n, i| {
            n.checked_add(i.length)?.checked_add(RECORD as u64)
        });
        let total = total.ok_or(StoreError::Corrupt("export quota overflow"))?;
        if intents.len() as u64 > policy.objects || total > policy.bytes {
            return Err(error(ErrorCode::LimitExceeded, "export quota exhausted"));
        }
        Ok(total)
    }
    fn synchronize(&self) -> Result<()> {
        #[cfg(test)]
        if tests::fail_sync() {
            return Err(std::io::Error::other("injected export directory sync failure").into());
        }
        Ok(crate::persistence::sync_directory(&self.path)?)
    }
    pub fn usage(&self) -> Result<ExportUsage> {
        self.check()?;
        let intents = self
            .intents
            .try_lock()
            .map_err(|_| error(ErrorCode::Conflict, "export operation in progress"))?;
        self.check()?;
        Ok(ExportUsage {
            intents: intents.len(),
            charged_bytes: Self::charge(&intents, self.policy)?,
            recovered_stages: self.recovered,
        })
    }
    fn wanted(&self, reference: &RetainedReference) -> Result<Intent> {
        let manifest = reference.manifest();
        if manifest.authority != self.authority || manifest.owner != self.owner {
            return Err(error(
                ErrorCode::Unauthorized,
                "export owner differs from selection",
            ));
        }
        let output = &manifest.outputs[reference.index().0 as usize];
        let mut hash = Sha256::new();
        hash.update(b"PipeStream export selection v1\0");
        hash.update(manifest.encode()?);
        hash.update(reference.index().0.to_be_bytes());
        Ok(Intent {
            length: output.length.0,
            sha256: output.sha256,
            selection: Digest(hash.finalize().into()),
        })
    }
    /// Copy a selected, pinned local result into this managed directory. An
    /// existing export ID must have the identical manifest/index commitment.
    /// Returned raw files are complete; prefixes stay in staging until verified EOF.
    pub fn export(
        &self,
        id: OperationId,
        reference: &RetainedReference,
        source: &mut LocalResult,
    ) -> Result<Exported> {
        self.check()?;
        let id = key(id)?;
        let wanted = self.wanted(reference)?;
        if source.authority != self.authority
            || source.owner != self.owner
            || source.descriptor.length.0 != wanted.length
            || source.descriptor.sha256 != wanted.sha256
        {
            return Err(error(
                ErrorCode::IntegrityError,
                "export source differs from selection",
            ));
        }
        let mut intents = self
            .intents
            .try_lock()
            .map_err(|_| error(ErrorCode::Conflict, "export operation in progress"))?;
        self.check()?;
        if let Some(old) = intents.get(&id) {
            if *old != wanted {
                return Err(error(
                    ErrorCode::Conflict,
                    "export identity cannot change commitment",
                ));
            }
        } else {
            if intents.len() as u64 >= self.policy.objects
                || Self::charge(&intents, self.policy)?
                    .checked_add(wanted.length)
                    .and_then(|n| n.checked_add(RECORD as u64))
                    .is_none_or(|n| n > self.policy.bytes)
            {
                return Err(error(ErrorCode::LimitExceeded, "export quota exhausted"));
            }
            let prepared = (|| -> Result<()> {
                let temp = self.path.join(format!("intent-{id}"));
                let mut file = create(&temp)?;
                file.write_all(&wanted.encode())?;
                file.sync_all()?;
                fs::rename(temp, self.path.join(format!("record-{id}")))?;
                self.synchronize()
            })();
            if let Err(e) = prepared {
                self.uncertain.store(true, Ordering::Release);
                return Err(e);
            }
            intents.insert(id.clone(), wanted.clone());
        }
        #[cfg(test)]
        tests::crash_point("intent");
        let destination = self.path.join(format!("file-{id}"));
        let transfer = (|| -> Result<Exported> {
            if destination.try_exists()? {
                Self::verify_file(&destination, &wanted)?;
                self.synchronize()?;
                return Ok(Exported {
                    path: destination.clone(),
                    replayed: true,
                });
            }
            let stage = self.path.join(format!("stage-{id}"));
            let mut file = create(&stage)?;
            let mut bytes = [0; CHUNK];
            let limit = CHUNK.min(source.chunk);
            loop {
                let n = source.read_chunk(&mut bytes[..limit])?;
                if n == 0 {
                    break;
                }
                file.write_all(&bytes[..n])?;
                #[cfg(test)]
                tests::crash_point("body");
            }
            if !source.verified() {
                return Err(error(
                    ErrorCode::IntegrityError,
                    "export source EOF is not verified",
                ));
            }
            // Independently bind the copied bytes, even if the supplied reader
            // had already consumed part/all of its stream before this call.
            file.sync_all()?;
            Self::verify_file(&stage, &wanted)?;
            fs::rename(stage, &destination)?;
            #[cfg(test)]
            tests::crash_point("installed");
            self.synchronize()?;
            #[cfg(test)]
            tests::crash_point("synced");
            Ok(Exported {
                path: destination,
                replayed: false,
            })
        })();
        if transfer.is_err() {
            self.uncertain.store(true, Ordering::Release);
        }
        transfer
    }
    fn verify_file(path: &Path, wanted: &Intent) -> Result<()> {
        let mut file = regular(path)?;
        if file.metadata()?.len() != wanted.length {
            return Err(error(ErrorCode::IntegrityError, "export length changed"));
        }
        let mut hash = Sha256::new();
        let mut bytes = [0; CHUNK];
        let mut input = (&mut file).take(wanted.length + 1);
        let mut count = 0u64;
        loop {
            let n = input.read(&mut bytes)?;
            if n == 0 {
                break;
            }
            count += n as u64;
            hash.update(&bytes[..n]);
        }
        if count != wanted.length || Digest(hash.finalize().into()) != wanted.sha256 {
            return Err(error(ErrorCode::IntegrityError, "export bytes changed"));
        }
        Ok(())
    }
    /// Remove only this named local export and intent. Never removes source
    /// copies, journal evidence or server results. Interrupted removal is retryable.
    pub fn remove(&self, id: OperationId) -> Result<bool> {
        self.check()?;
        let id = key(id)?;
        let mut intents = self
            .intents
            .try_lock()
            .map_err(|_| error(ErrorCode::Conflict, "export operation in progress"))?;
        self.check()?;
        if !intents.contains_key(&id) {
            return Ok(false);
        }
        let removed = (|| -> Result<()> {
            for kind in ["file", "stage"] {
                let path = self.path.join(format!("{kind}-{id}"));
                match regular(&path) {
                    Ok(_) => fs::remove_file(path)?,
                    Err(StoreError::Io(e)) if e.kind() == std::io::ErrorKind::NotFound => {}
                    Err(e) => return Err(e),
                }
            }
            self.synchronize()?;
            fs::remove_file(self.path.join(format!("record-{id}")))?;
            #[cfg(test)]
            tests::crash_point("removed");
            self.synchronize()
        })();
        if let Err(e) = removed {
            self.uncertain.store(true, Ordering::Release);
            return Err(e);
        }
        intents.remove(&id);
        Ok(true)
    }
}
#[cfg(test)]
mod tests;
