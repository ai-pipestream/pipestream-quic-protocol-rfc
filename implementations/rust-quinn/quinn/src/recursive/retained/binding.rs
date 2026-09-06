//! Local database/root pairing, established before service admission or dispatch.

use super::*;
use pipestream_core::persistence::{
    PAYLOAD_BINDING_BYTES, PayloadBinding, SqliteSessionStore, StoreError, StoreIdentity,
};

const IDENTITY_BYTES: usize = 56;
const IDENTITY_MAGIC: &[u8; 8] = b"PSRID001";
const IDENTITY_FILE: &str = ".retained-identity";
const BINDING_FILE: &str = ".session-store";

pub(super) fn create_identity(root: &Path) -> io::Result<()> {
    let identity = StoreIdentity::generate().map_err(io::Error::other)?;
    let mut bytes = [0; IDENTITY_BYTES];
    bytes[..8].copy_from_slice(IDENTITY_MAGIC);
    bytes[8..24].copy_from_slice(identity.as_bytes());
    let checksum = Sha256::digest(&bytes[..24]);
    bytes[24..].copy_from_slice(&checksum);
    write_new(&root.join(IDENTITY_FILE), &bytes)
}

pub(super) fn read_identity(root: &Path) -> io::Result<StoreIdentity> {
    let bytes = read_fixed::<IDENTITY_BYTES>(&root.join(IDENTITY_FILE))?;
    if &bytes[..8] != IDENTITY_MAGIC || Sha256::digest(&bytes[..24])[..] != bytes[24..] {
        return Err(corrupt("invalid retained-store identity"));
    }
    let mut identity = [0; 16];
    identity.copy_from_slice(&bytes[8..24]);
    StoreIdentity::from_bytes(identity).map_err(io::Error::other)
}

pub(super) fn read_claim(
    root: &Path,
    identity: StoreIdentity,
) -> io::Result<Option<PayloadBinding>> {
    let path = root.join(BINDING_FILE);
    if regular_length(&path, 1)?.is_none() {
        return Ok(None);
    }
    let binding = PayloadBinding::decode(&read_fixed::<PAYLOAD_BINDING_BYTES>(&path)?)
        .map_err(io::Error::other)?;
    if binding.payloads() != Some(identity) {
        return Err(corrupt(
            "retained claim belongs to a different payload store",
        ));
    }
    Ok(Some(binding))
}

impl RetainedRoot {
    pub(crate) fn bind_sessions(&self, sessions: &SqliteSessionStore) -> Result<(), StoreError> {
        self.verify_policy()?;
        let mut retained = self
            .binding
            .lock()
            .map_err(|_| corrupt("retained binding lock poisoned"))?;
        let current = sessions.payload_binding()?;
        let expected = PayloadBinding::new(current.database(), self.identity);
        if current.payloads().is_some_and(|id| id != self.identity)
            || retained.is_some_and(|binding| binding != expected)
        {
            return Err(StoreError::Protocol(super::super::entity_error(
                "database and retained root belong to different pairs",
            )));
        }
        if retained.is_none() {
            if current.payloads().is_some() {
                return Err(StoreError::Corrupt(
                    "bound database is missing its retained-root claim".into(),
                ));
            }
            // The immutable file claim precedes the database claim. A complete
            // claim can replay after reopen if the transaction or sync fails.
            write_new(&self.path.join(BINDING_FILE), &expected.encode())?;
            *retained = Some(expected);
        }
        File::open(self.path.join(BINDING_FILE))?.sync_all()?;
        sync_directory(&self.path)?;
        sessions.bind_payload_store(expected)
    }
}

#[cfg(test)]
mod tests;
