//! Durable client intent, separate from server state and connection correlation.
//!
//! Commit creation and mutation parameters before sending them. Transport loss,
//! NOT_FOUND, a refusal or a failed local receipt write leaves the same intent
//! available for replay; none authorizes a new operation or execution attempt.
//! All methods perform blocking local storage work, outside control readers.
use super::{
    codec::{self, Wire},
    *,
};
use crate::persistence::{GUARDED_VFS, PhysicalGuard, PhysicalLimits, PhysicalUsage};
use rusqlite::{Connection, OpenFlags, OptionalExtension, TransactionBehavior, params};
use sha2::{Digest as _, Sha256};
use std::{path::Path, sync::Arc, time::Duration as Elapsed};

mod storage;
#[cfg(test)]
mod tests;

pub use storage::JournalError;
type Result<T, E = JournalError> = std::result::Result<T, E>;
const IMAGE_LIMIT: usize = MAX_HEADER + 5;

codec::record!(
    Creation {
        authority: IdentityLabel,
        owner: IdentityLabel,
        creation_sequence: Id,
        policy: Policy,
        results: bool
    } | _s
        | { Ok(()) }
);

impl Creation {
    pub fn request(&self, request: Id) -> std::result::Result<Control, Error> {
        self.check()?;
        let control = Control::Session(Session::Create {
            request,
            creation_sequence: self.creation_sequence,
            policy: self.policy.clone(),
        });
        control.encode(INITIAL_CONTROL_LIMIT)?;
        Ok(control)
    }

    /// Reconnection may negotiate different limits, but never a different
    /// durable profile combination. The server still checks retained geometry.
    pub fn validate_selection(&self, selected: &Capabilities) -> std::result::Result<(), Error> {
        selected.check()?;
        if selected.response.0 != 1
            || !selected.has(DURABLE_WORK)
            || selected.has(RESULT_DELIVERY) != self.results
        {
            return Err(Error::new(
                ErrorCode::ExtensionUnsupported,
                "journal profile combination changed",
            ));
        }
        Ok(())
    }
}

codec::record!(
    JournalLimits { operations: Id } | s | {
        require(
            s.operations.0 <= 1_000_000,
            "client operation ceiling exceeds one million",
        )
    }
);
impl Default for JournalLimits {
    fn default() -> Self {
        Self {
            operations: Id(4096),
        }
    }
}

/// Immutable parameters loaded from a committed intent. Request/stream numbers
/// are deliberately not persisted: they belong to a particular connection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Intent {
    pub operation: OperationId,
    pub mutation: Mutation,
}
impl Intent {
    pub fn control(&self, request: Id) -> std::result::Result<Control, Error> {
        let operation = self.operation;
        let control = match &self.mutation {
            Mutation::Declare {
                scope,
                entity_ids,
                seal,
            } => Control::Scope(Scope::Declare {
                request,
                operation,
                scope: *scope,
                entity_ids: entity_ids.clone(),
                seal: *seal,
            }),
            Mutation::ScopeCancel { scope } => Control::Scope(Scope::Cancel {
                request,
                operation,
                scope: *scope,
            }),
            Mutation::Retry {
                work,
                expected_attempt,
            } => Control::Work(Work::Retry {
                request,
                operation,
                work: work.clone(),
                expected_attempt: *expected_attempt,
            }),
            Mutation::Cancel { work } => Control::Work(Work::Cancel {
                request,
                operation,
                work: work.clone(),
            }),
            Mutation::Skip { work } => Control::Work(Work::Skip {
                request,
                operation,
                work: work.clone(),
            }),
            Mutation::Admit(_) => {
                return Err(Error::frame("input intent requires an object stream"));
            }
        };
        control.encode(IMAGE_LIMIT)?;
        Ok(control)
    }
    pub fn input(&self, generation: Id) -> std::result::Result<InputHeader, Error> {
        let Mutation::Admit(parameters) = &self.mutation else {
            return Err(Error::frame("control intent is not input admission"));
        };
        let header = InputHeader {
            kind: Literal,
            generation,
            operation: self.operation,
            parameters: parameters.clone(),
        };
        header.encode()?;
        Ok(header)
    }
}

/// One immutable creation/session per journal. Cloneable handles serialize
/// through SQLite, not an in-memory request history. No automatic eviction.
#[derive(Clone)]
pub struct Journal {
    physical: Arc<PhysicalGuard>,
    creation: Creation,
    limits: JournalLimits,
}
impl Journal {
    /// New history only. Failure never deletes or replaces an existing file.
    pub fn initialize(
        path: &Path,
        creation: Creation,
        limits: JournalLimits,
        physical: PhysicalLimits,
    ) -> Result<Self> {
        Self::open_inner(path, creation, limits, physical, true)
    }
    /// Existing history only. The expected authority/owner/creation policy and
    /// quotas come from trusted application configuration, never a locator.
    pub fn open(
        path: &Path,
        creation: Creation,
        limits: JournalLimits,
        physical: PhysicalLimits,
    ) -> Result<Self> {
        Self::open_inner(path, creation, limits, physical, false)
    }
    pub fn creation(&self) -> &Creation {
        &self.creation
    }
    pub fn physical_usage(&self) -> Result<PhysicalUsage> {
        Ok(self.physical.usage()?)
    }

    /// Returns the authenticated binding only after its local durable commit.
    /// Call after wire correlation and TLS authority/owner verification. A CBOR
    /// record supplied directly to this library is not authenticated by it.
    pub fn record_binding(&self, response: &Control, selected: &Capabilities) -> Result<()> {
        self.creation.validate_selection(selected)?;
        let binding = storage::binding(response)?;
        self.validate_binding(&binding)?;
        let mut connection = self.connect()?;
        let tx = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        match self.read_binding(&tx)? {
            Some(old) if old != binding => {
                return Err(JournalError::Protocol(Error::new(
                    ErrorCode::IntegrityError,
                    "immutable session binding changed",
                )));
            }
            Some(_) => {}
            None => {
                storage::write(
                    &tx,
                    "journal",
                    "binding",
                    1,
                    &storage::seal(b"binding", &binding.encode(IMAGE_LIMIT)?),
                )?;
            }
        }
        tx.commit()?;
        Ok(())
    }
    pub fn binding(&self) -> Result<Option<Control>> {
        self.read_binding(&self.connect()?)
    }
    pub fn identity(&self) -> Result<SessionIdentity> {
        self.read_identity(&self.connect()?)
    }

    /// This commit must succeed before any bytes of a new mutation are sent.
    /// Matching preparation is idempotent, including after receipt storage.
    pub fn prepare(&self, intent: &Intent) -> Result<()> {
        let mut connection = self.connect()?;
        let tx = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let identity = self.read_identity(&tx)?;
        let image = storage::intent_image(intent, &identity)?;
        if let Some(row) = storage::operation_row(&tx, intent.operation)? {
            if storage::read(&tx, "operations", "intent", row)? != Some(image) {
                return Err(JournalError::Protocol(Error::new(
                    ErrorCode::Conflict,
                    "operation intent changed",
                )));
            }
        } else {
            let (count, last): (i64, i64) = tx.query_row(
                "SELECT count(*),coalesce(max(row_id),0) FROM operations",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )?;
            if count < 0 || last < 0 {
                return Err(JournalError::Corrupt("invalid operation inventory"));
            }
            if count as u64 >= self.limits.operations.0 || last == i64::MAX {
                return Err(JournalError::Protocol(Error::new(
                    ErrorCode::LimitExceeded,
                    "client journal operation count or cursor ceiling",
                )));
            }
            tx.execute(
                "INSERT INTO operations(row_id,operation,intent,receipt) VALUES(?1,?2,?3,NULL)",
                params![last + 1, intent.operation.0.as_slice(), image],
            )?;
        }
        tx.commit()?;
        Ok(())
    }

    pub fn intent(&self, operation: OperationId) -> Result<Intent> {
        self.read_intent(&self.connect()?, operation)
    }

    /// Validate all immutable request parameters and known outcome constraints,
    /// then save the exact receipt. This is not scope coverage, result validation
    /// or authentication by itself. A declaration seal still needs the full set.
    pub fn record_receipt(&self, receipt: &OperationReceipt) -> Result<()> {
        let mut connection = self.connect()?;
        let tx = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let intent = self.read_intent(&tx, receipt.operation)?;
        storage::validate_receipt(&intent, &self.read_identity(&tx)?, receipt)?;
        let row = storage::operation_row(&tx, receipt.operation)?
            .ok_or(JournalError::Corrupt("intent disappeared"))?;
        let image = storage::seal(&receipt.operation.0, &codec::encode(receipt, MAX_HEADER)?);
        match storage::read(&tx, "operations", "receipt", row)? {
            Some(old) if old != image => {
                return Err(JournalError::Protocol(Error::new(
                    ErrorCode::IntegrityError,
                    "immutable operation receipt changed",
                )));
            }
            Some(_) => {}
            None => {
                storage::write(&tx, "operations", "receipt", row, &image)?;
            }
        }
        tx.commit()?;
        Ok(())
    }
    pub fn receipt(&self, operation: OperationId) -> Result<Option<OperationReceipt>> {
        let connection = self.connect()?;
        let tx = connection.unchecked_transaction()?;
        let intent = self.read_intent(&tx, operation)?;
        let row = storage::operation_row(&tx, operation)?
            .ok_or(JournalError::Corrupt("intent disappeared"))?;
        let receipt = storage::read(&tx, "operations", "receipt", row)?
            .map(|image| {
                let receipt: OperationReceipt =
                    codec::decode(storage::unseal(&operation.0, &image)?, MAX_HEADER)?;
                storage::validate_receipt(&intent, &self.read_identity(&tx)?, &receipt)?;
                Ok::<_, JournalError>(receipt)
            })
            .transpose()?;
        tx.commit()?;
        Ok(receipt)
    }

    /// Page immutable operation IDs in insertion order. NULL receipt means no
    /// durable local receipt, not proof that the authority did not commit.
    pub fn unresolved(&self, after: Number, limit: PageLimit) -> Result<Vec<(Id, Intent)>> {
        after.check()?;
        limit.check()?;
        let connection = self.connect()?;
        let tx = connection.unchecked_transaction()?;
        let mut statement = tx.prepare("SELECT rowid,operation FROM operations WHERE rowid>?1 AND receipt IS NULL ORDER BY rowid LIMIT ?2")?;
        let mut rows = statement.query(params![after.0 as i64, limit.0 as i64])?;
        let mut result = Vec::new();
        while let Some(row) = rows.next()? {
            let id: i64 = row.get(0)?;
            let operation = storage::read_operation(row, 1)?;
            result.push((Id(id as u64), self.read_intent(&tx, operation)?));
        }
        drop(rows);
        drop(statement);
        tx.commit()?;
        Ok(result)
    }

    fn validate_binding(&self, binding: &Control) -> Result<()> {
        let Control::Session(Session::Binding {
            authority,
            owner,
            creation_sequence,
            policy,
            ..
        }) = binding
        else {
            return Err(JournalError::Corrupt("not a session binding"));
        };
        if authority != &self.creation.authority
            || owner != &self.creation.owner
            || creation_sequence != &self.creation.creation_sequence
            || policy != &self.creation.policy
        {
            return Err(JournalError::Protocol(Error::new(
                ErrorCode::IntegrityError,
                "session binding disagrees with persisted creation",
            )));
        }
        Ok(())
    }
    fn read_binding(&self, connection: &Connection) -> Result<Option<Control>> {
        storage::read(connection, "journal", "binding", 1)?
            .map(|image| {
                let control = Control::decode(storage::unseal(b"binding", &image)?, IMAGE_LIMIT)?;
                let normalized = storage::binding(&control)?;
                if normalized != control {
                    return Err(JournalError::Corrupt("noncanonical stored request number"));
                }
                self.validate_binding(&control)?;
                Ok(control)
            })
            .transpose()
    }
    fn read_identity(&self, connection: &Connection) -> Result<SessionIdentity> {
        let Some(Control::Session(Session::Binding {
            authority,
            owner,
            generation,
            ..
        })) = self.read_binding(connection)?
        else {
            return Err(JournalError::Protocol(Error::new(
                ErrorCode::NotReady,
                "session binding not durably observed",
            )));
        };
        Ok(SessionIdentity {
            authority,
            owner,
            generation,
        })
    }
    fn read_intent(&self, connection: &Connection, operation: OperationId) -> Result<Intent> {
        operation.check()?;
        let row = storage::operation_row(connection, operation)?.ok_or(JournalError::Protocol(
            Error::new(ErrorCode::NotFound, "no persisted client intent"),
        ))?;
        let image = storage::read(connection, "operations", "intent", row)?
            .ok_or(JournalError::Corrupt("missing intent"))?;
        storage::decode_intent(&image, operation, &self.read_identity(connection)?)
    }
}
