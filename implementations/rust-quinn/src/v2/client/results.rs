//! Bounded local copies of explicitly selected results. This is not a second
//! authority, an authorization cache, or evidence of remote retention renewal.
//! Blocking calls belong on an owned storage worker, never a control reader.
use super::RetainedReference;
pub use crate::v2::authority::payload::{
    PayloadPolicy as ResultPolicy, PayloadUsage as ResultUsage,
};
use crate::{
    persistence::StoreIdentity,
    v2::{
        authority::{
            StoreError,
            payload::{InstalledPayload, ObjectReader, PayloadStore, StagedPayload},
        },
        *,
    },
};
use sha2::{Digest as _, Sha256};
use std::{path::Path, time::Instant};
type Result<T> = std::result::Result<T, StoreError>;

/// All copies and unfinished downloads in this root share its immutable quota.
/// The trusted authority/owner binding deliberately permits multiple session
/// generations. A cache hit still requires an exact retained manifest selection.
#[derive(Clone)]
pub struct ResultStore {
    payloads: PayloadStore,
    authority: IdentityLabel,
    owner: IdentityLabel,
}

fn binding(authority: &IdentityLabel, owner: &IdentityLabel) -> Result<StoreIdentity> {
    authority.validate()?;
    owner.validate()?;
    let mut hash = Sha256::new();
    hash.update(b"PipeStream local result copies v1\0");
    for label in [authority, owner] {
        hash.update((label.0.len() as u64).to_be_bytes());
        hash.update(label.0.as_bytes());
    }
    let digest = hash.finalize();
    let mut bytes = [0; 16];
    bytes.copy_from_slice(&digest[..16]);
    // This local namespace binding is neither a principal nor a credential.
    Ok(StoreIdentity::from_bytes(bytes)?)
}

impl ResultStore {
    /// New private directory only; never adopt or replace existing files.
    pub fn initialize(
        path: &Path,
        authority: IdentityLabel,
        owner: IdentityLabel,
        policy: ResultPolicy,
    ) -> Result<Self> {
        Self::open_inner(path, authority, owner, policy, true)
    }
    /// Reopen the same owner and immutable quota under exclusive process ownership.
    /// Interrupted stages are unreferenceable and reclaimed only after the full
    /// bounded inventory/configuration audit. Unknown paths fail, not get deleted.
    pub fn open(
        path: &Path,
        authority: IdentityLabel,
        owner: IdentityLabel,
        policy: ResultPolicy,
    ) -> Result<Self> {
        Self::open_inner(path, authority, owner, policy, false)
    }
    fn open_inner(
        path: &Path,
        authority: IdentityLabel,
        owner: IdentityLabel,
        policy: ResultPolicy,
        fresh: bool,
    ) -> Result<Self> {
        let identity = binding(&authority, &owner)?;
        let open = if fresh {
            PayloadStore::initialize
        } else {
            PayloadStore::open
        };
        let payloads = open(path, identity, policy)?;
        payloads.verify_local_owner(&owner)?;
        Ok(Self {
            payloads,
            authority,
            owner,
        })
    }
    pub fn usage(&self) -> Result<ResultUsage> {
        self.payloads.usage(None)
    }
    pub fn recovered_stages(&self) -> usize {
        self.payloads.recovered_stages()
    }
    pub fn chunk_limit(&self) -> usize {
        self.payloads.chunk_limit()
    }

    fn descriptor(&self, reference: &RetainedReference) -> Result<Input> {
        let manifest = reference.manifest();
        if manifest.authority != self.authority || manifest.owner != self.owner {
            return Err(StoreError::Protocol(Error {
                code: ErrorCode::Unauthorized,
                detail: "local result owner does not match selection",
            }));
        }
        let output = &manifest.outputs[reference.index().0 as usize];
        Ok(Input {
            length: output.length,
            sha256: output.sha256,
            content_type: output.content_type.clone(),
        })
    }
    /// Reserve the complete selected length plus header allowance before writing.
    /// Caller must supply journal-validated selection and authenticated transport
    /// evidence; this storage API does not authenticate arbitrary input records.
    pub fn stage(
        &self,
        reference: RetainedReference,
        caps: &Capabilities,
        now: Instant,
    ) -> Result<PendingResult> {
        let input = self.descriptor(&reference)?;
        Ok(PendingResult {
            staged: self.payloads.stage(&self.owner, &input, caps, now)?,
            reference,
        })
    }
    /// Explicit local-only lookup by exact selected content commitment. It does
    /// not contact the authority, grant fresh access, or extend any remote lease.
    /// Before verified EOF, returned bytes are provisional and must not be published.
    pub fn find(&self, reference: &RetainedReference) -> Result<Option<LocalResult>> {
        let input = self.descriptor(reference)?;
        let Some(key) = self.payloads.find_local(&self.owner, &input)? else {
            return Ok(None);
        };
        let reader = self.payloads.open_object(&key, &self.owner, &input)?;
        Ok(Some(LocalResult { key, reader }))
    }
    /// Explicitly discard one local copy, never remote work or journal evidence.
    /// Live download/read handles refuse removal. File capacity is released only
    /// after directory synchronization; an interrupted unlink is safe to retry.
    pub fn remove(&self, key: &str) -> Result<bool> {
        self.payloads.remove_local(&self.owner, key)
    }
}

pub struct PendingResult {
    staged: StagedPayload,
    reference: RetainedReference,
}
impl PendingResult {
    pub fn receive(&mut self, bytes: &[u8], now: Instant) -> Result<()> {
        self.staged.receive(bytes, now)
    }
    /// Call only after authenticated transport has verified the selected result's
    /// length, digest and FIN. A full byte prefix alone is not transport completion.
    pub fn finish(self, verified: &ResultHeader, now: Instant) -> Result<InstalledPayload> {
        verified.encode()?;
        let manifest = self.reference.manifest();
        let output = &manifest.outputs[self.reference.index().0 as usize];
        if verified.generation != manifest.generation
            || verified.work != manifest.work
            || verified.attempt != manifest.attempt
            || verified.index != self.reference.index()
            || verified.length != output.length
            || verified.sha256 != output.sha256
        {
            return Err(StoreError::Protocol(Error {
                code: ErrorCode::IntegrityError,
                detail: "verified result differs from retained selection",
            }));
        }
        #[cfg(test)]
        tests::crash_point("before-finish");
        let installed = self.staged.finish(now)?;
        #[cfg(test)]
        tests::crash_point("after-finish");
        Ok(installed)
    }
}

/// Pins a local object while its bytes are read and verified against the journal.
pub struct LocalResult {
    key: String,
    reader: ObjectReader,
}
impl LocalResult {
    pub fn key(&self) -> &str {
        &self.key
    }
    pub fn read_chunk(&mut self, bytes: &mut [u8]) -> Result<usize> {
        self.reader.read_chunk(bytes)
    }
    pub fn verified(&self) -> bool {
        self.reader.verified()
    }
}

#[cfg(test)]
pub(crate) mod tests;
