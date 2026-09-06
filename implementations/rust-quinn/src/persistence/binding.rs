//! Durable local storage identities. These are not protocol principals or credentials.

use super::*;
use rand::{TryRng, rngs::SysRng};

const MAGIC: &[u8; 8] = b"PSRBND01";
/// Fixed size of the local checksummed database/payload ownership image.
pub const PAYLOAD_BINDING_BYTES: usize = 72;

/// A nonzero local store identity, retained when a matched backup is restored.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StoreIdentity([u8; 16]);

impl StoreIdentity {
    /// Create a store identity using the operating system random source.
    pub fn generate() -> Result<Self, StoreError> {
        loop {
            let mut bytes = [0; 16];
            SysRng
                .try_fill_bytes(&mut bytes)
                .map_err(|error| StoreError::Io(std::io::Error::other(error)))?;
            if bytes != [0; 16] {
                return Ok(Self(bytes));
            }
        }
    }

    /// Decode a retained identity, refusing the reserved unbound value.
    pub fn from_bytes(bytes: [u8; 16]) -> Result<Self, StoreError> {
        if bytes == [0; 16] {
            return Err(StoreError::Corrupt("zero store identity".into()));
        }
        Ok(Self(bytes))
    }

    /// Borrow the exact persistent identity bytes.
    pub fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }
}

/// Immutable database identity and its optional, once-assigned payload-store identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PayloadBinding {
    database: StoreIdentity,
    payloads: Option<StoreIdentity>,
}

impl PayloadBinding {
    /// Construct a proposed complete pair from two validated storage identities.
    pub fn new(database: StoreIdentity, payloads: StoreIdentity) -> Self {
        Self {
            database,
            payloads: Some(payloads),
        }
    }

    /// Return the immutable database identity.
    pub fn database(&self) -> StoreIdentity {
        self.database
    }
    /// Return the bound payload identity, or absence before the first claim.
    pub fn payloads(&self) -> Option<StoreIdentity> {
        self.payloads
    }

    /// Fixed, checksummed local-storage encoding; not a wire message.
    pub fn encode(&self) -> [u8; PAYLOAD_BINDING_BYTES] {
        let mut bytes = [0; PAYLOAD_BINDING_BYTES];
        bytes[..8].copy_from_slice(MAGIC);
        bytes[8..24].copy_from_slice(self.database.as_bytes());
        if let Some(payloads) = self.payloads {
            bytes[24..40].copy_from_slice(payloads.as_bytes());
        }
        let checksum = Sha256::digest(&bytes[..40]);
        bytes[40..].copy_from_slice(&checksum);
        bytes
    }

    /// Refuse truncated, oversized, corrupt or incompatible ownership records.
    pub fn decode(bytes: &[u8]) -> Result<Self, StoreError> {
        if bytes.len() != PAYLOAD_BINDING_BYTES
            || &bytes[..8] != MAGIC
            || Sha256::digest(&bytes[..40])[..] != bytes[40..]
        {
            return Err(StoreError::Corrupt(
                "invalid payload-store binding image".into(),
            ));
        }
        let mut database = [0; 16];
        database.copy_from_slice(&bytes[8..24]);
        let mut payloads = [0; 16];
        payloads.copy_from_slice(&bytes[24..40]);
        Ok(Self {
            database: StoreIdentity::from_bytes(database)?,
            payloads: if payloads == [0; 16] {
                None
            } else {
                Some(StoreIdentity::from_bytes(payloads)?)
            },
        })
    }
}

pub(super) fn initialize(connection: &Connection) -> Result<(), StoreError> {
    let binding = PayloadBinding {
        database: StoreIdentity::generate()?,
        payloads: None,
    };
    connection.execute(
        "INSERT INTO pipestream_payload_binding VALUES (1, ?1)",
        [binding.encode().as_slice()],
    )?;
    Ok(())
}

pub(super) fn read(connection: &Connection) -> Result<PayloadBinding, StoreError> {
    schema::verify(connection, SCHEMA)?;
    let mut query = connection.prepare("SELECT singleton, CASE WHEN length(image)=72 THEN image ELSE NULL END FROM pipestream_payload_binding LIMIT 2")?;
    let mut rows = query.query([])?;
    let row = rows
        .next()?
        .ok_or_else(|| StoreError::Corrupt("missing payload-store binding".into()))?;
    let singleton: i64 = row.get(0)?;
    let image: Option<Vec<u8>> = row.get(1)?;
    let binding = PayloadBinding::decode(
        &image.ok_or_else(|| StoreError::Corrupt("invalid binding image length".into()))?,
    )?;
    if singleton != 1 || rows.next()?.is_some() {
        return Err(StoreError::Corrupt(
            "invalid binding row identity or count".into(),
        ));
    }
    Ok(binding)
}

impl SqliteSessionStore {
    /// Read the checked retained local-store pairing. This does not authenticate a caller.
    pub fn payload_binding(&self) -> Result<PayloadBinding, StoreError> {
        read(&self.connect()?)
    }

    /// Bind once, after the payload root has durably recorded this same pair.
    /// The caller owns the payload root throughout that file claim and this transaction.
    /// No session, admission, job or completion is created by binding.
    pub fn bind_payload_store(&self, expected: PayloadBinding) -> Result<(), StoreError> {
        if expected.payloads.is_none() {
            return Err(StoreError::Protocol(ProtocolError::entity(
                "binding requires a payload-store identity",
            )));
        }
        let mut connection = self.connect()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let current = read(&transaction)?;
        if current.database != expected.database
            || current
                .payloads
                .is_some_and(|id| Some(id) != expected.payloads)
        {
            return Err(StoreError::Protocol(ProtocolError::entity(
                "database belongs to a different payload-store pair",
            )));
        }
        if current != expected {
            // This ordinary metadata write must not consume any admitted job's
            // remaining acquisition, conversion or publication allowance.
            queue::verify_index(&transaction)?;
            storage::verify_index(&transaction)?;
            physical::protect_unchanged(&transaction)?;
            let mut image =
                transaction.blob_open("main", "pipestream_payload_binding", "image", 1, false)?;
            image.write_at(&expected.encode(), 0)?;
            image.close()?;
        }
        transaction.commit()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests;
