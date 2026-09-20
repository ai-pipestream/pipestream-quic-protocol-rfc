//! Bridge exposing the durable v2 client journal ([`Journal`]) as the
//! storage SPI's [`SpiJournal`](pipestream_storage::Journal) role.
//!
//! Mapping (SPI → this journal):
//!
//! | SPI | v2 client journal |
//! |---|---|
//! | `append_intent` | [`Journal::prepare`] (idempotent by operation id) |
//! | `commit` | [`Journal::record_receipt`] (validated against the intent) |
//! | `replay(0)` | ordered scan of the operations table (read-only) |
//!
//! Encodings, chosen so a record read back out equals the record written:
//!
//! - Intent id: the 16-byte operation id as 32 lowercase hex characters.
//! - Intent record: the stored intent *body* — `[kind][canonical request
//!   encoding][32B identity-bound digest]` (i.e. the unsealed intent image),
//!   which `prepare` validates and stores verbatim.
//! - Commit record: the canonical `OperationReceipt` encoding, which
//!   `record_receipt` validates (against the intent) and stores.
//!
//! Cursor mapping: the underlying journal numbers intents by rowid and
//! stores receipts in the same row — commits have no independent position.
//! The bridge maps intent at rowid `r` to cursor `2r - 1` and its commit to
//! cursor `2r`: deterministic, stable across reopen, strictly increasing,
//! and each commit sorts immediately after its intent. This is *not* the
//! dense global order the SPI pins for native backends — a commit that
//! lands after later intents is presented next to its own intent instead —
//! so the bridge runs the journal contract suite under
//! [`JournalProfile::SPARSE`](pipestream_storage::contract::JournalProfile),
//! which skips the density check rather than faking positions the journal
//! never recorded.
//!
//! One deliberate deviation from the adoption note's sketch: SPI `replay`
//! cannot be `unresolved` — `unresolved` filters committed operations out,
//! while the SPI replay exists precisely to distinguish committed from
//! admitted-but-uncommitted work, so it must return both.

use super::super::codec;
use super::*;
use pipestream_storage::{Cursor, Journal as SpiJournal, ReplayEntry, StorageError};

/// The SPI view of a bound v2 client journal. See module docs for the
/// encoding and cursor mapping.
pub struct ClientJournalBridge {
    journal: Journal,
    identity: SessionIdentity,
}

impl ClientJournalBridge {
    /// Wraps a journal that already carries its durable session binding
    /// (production callers bind before preparing; tests use the client
    /// fixtures). Fails when the journal is unbound.
    pub fn new(journal: Journal) -> Result<Self> {
        let identity = journal.identity()?;
        Ok(ClientJournalBridge { journal, identity })
    }

    /// The wrapped journal, for the existing typed API (`unresolved`,
    /// `receipt`, …).
    pub fn inner(&self) -> &Journal {
        &self.journal
    }

    /// Encodes an operation id as the SPI intent id (32 lowercase hex).
    pub fn intent_id(operation: OperationId) -> String {
        operation.0.iter().map(|b| format!("{b:02x}")).collect()
    }

    /// Builds the SPI record bytes for a typed intent: the stored intent
    /// body (canonical encoding plus identity-bound digest).
    pub fn intent_record(&self, intent: &Intent) -> Result<Vec<u8>> {
        let image = storage::intent_image(intent, &self.identity)?;
        Ok(storage::unseal(&intent.operation.0, &image)?.to_vec())
    }

    /// Builds the SPI commit-record bytes for a typed receipt.
    pub fn commit_record(receipt: &OperationReceipt) -> Result<Vec<u8>> {
        Ok(codec::encode(receipt, MAX_HEADER)?)
    }
}

fn map_error(error: JournalError) -> StorageError {
    match error {
        JournalError::Corrupt(detail) => StorageError::corrupt(format!("client journal: {detail}")),
        JournalError::Io(error) => StorageError::transient(format!("client journal: {error}")),
        JournalError::Database(error) => {
            let code = error.sqlite_error_code();
            if matches!(
                code,
                Some(rusqlite::ErrorCode::DatabaseBusy | rusqlite::ErrorCode::DatabaseLocked)
            ) {
                StorageError::transient(format!("client journal: {error}"))
            } else {
                StorageError::failed(format!("client journal: {error}"))
            }
        }
        JournalError::Physical(error) => StorageError::failed(format!("client journal: {error}")),
        JournalError::Protocol(error) => match error.code {
            ErrorCode::NotFound => StorageError::not_found(error.to_string()),
            ErrorCode::Conflict | ErrorCode::IntegrityError => {
                StorageError::corrupt(error.to_string())
            }
            _ => StorageError::failed(error.to_string()),
        },
    }
}

/// Parses an SPI intent id back into an operation id.
fn parse_operation(id: &str) -> Result<OperationId, StorageError> {
    if id.len() != 32 || !id.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(StorageError::failed(format!(
            "intent id is not a 16-byte hex operation id: {id:?}"
        )));
    }
    let mut bytes = [0u8; 16];
    for i in 0..16 {
        bytes[i] = u8::from_str_radix(&id[2 * i..2 * i + 2], 16)
            .map_err(|_| StorageError::failed(format!("malformed intent id {id:?}")))?;
    }
    let operation = OperationId(bytes);
    operation
        .check()
        .map_err(|e| StorageError::failed(e.to_string()))?;
    Ok(operation)
}

fn map_sql(error: rusqlite::Error) -> StorageError {
    map_error(JournalError::from(error))
}

impl SpiJournal for ClientJournalBridge {
    fn append_intent(&mut self, intent: &pipestream_storage::Intent) -> Result<Cursor, StorageError> {
        let operation = parse_operation(intent.id())?;
        // Validates the record's canonical encoding and identity-bound
        // digest before anything is written.
        let typed = storage::decode_intent(
            &storage::seal(&operation.0, intent.record()),
            operation,
            &self.identity,
        )
        .map_err(map_error)?;
        self.journal.prepare(&typed).map_err(map_error)?;
        let connection = self.journal.connect().map_err(map_error)?;
        let row = storage::operation_row(&connection, operation)
            .map_err(map_error)?
            .ok_or_else(|| StorageError::failed("intent missing after prepare"))?;
        Ok(Cursor(2 * row as u64 - 1))
    }

    fn commit(&mut self, intent_id: &str, record: &[u8]) -> Result<(), StorageError> {
        let operation = parse_operation(intent_id)?;
        let receipt: OperationReceipt = codec::decode(record, MAX_HEADER)
            .map_err(|_| StorageError::failed("commit record is not a receipt encoding"))?;
        let canonical = codec::encode(&receipt, MAX_HEADER)
            .map_err(|e| StorageError::failed(e.to_string()))?;
        if canonical != record {
            return Err(StorageError::failed("commit record is not canonical"));
        }
        if receipt.operation != operation {
            return Err(StorageError::failed(
                "commit receipt names a different operation than the intent id",
            ));
        }
        self.journal.record_receipt(&receipt).map_err(map_error)
    }

    fn replay(&self, since: Cursor) -> Result<Vec<ReplayEntry>, StorageError> {
        let connection = self.journal.connect().map_err(map_error)?;
        // Intent cursor 2r-1 > since ⟺ rowid r > (since + 1) / 2.
        let after = since.0.div_ceil(2) as i64;
        let mut statement = connection
            .prepare(
                "SELECT rowid, operation, intent, receipt FROM operations \
                 WHERE rowid > ?1 ORDER BY rowid",
            )
            .map_err(map_sql)?;
        let mut rows = statement.query([after]).map_err(map_sql)?;
        let mut entries = Vec::new();
        while let Some(row) = rows.next().map_err(map_sql)? {
            let rowid: i64 = row.get(0).map_err(map_sql)?;
            let operation = storage::read_operation(row, 1).map_err(map_error)?;
            let intent_image: Vec<u8> = row.get(2).map_err(map_sql)?;
            let receipt_image: Option<Vec<u8>> = row.get(3).map_err(map_sql)?;
            let body = storage::unseal(&operation.0, &intent_image)
                .map_err(map_error)?
                .to_vec();
            let intent = pipestream_storage::Intent::new(Self::intent_id(operation), body)
                .map_err(|_| StorageError::corrupt("stored intent exceeds SPI bounds"))?;
            let (commit, commit_cursor) = match receipt_image {
                Some(image) => (
                    Some(storage::unseal(&operation.0, &image).map_err(map_error)?.to_vec()),
                    Some(Cursor(2 * rowid as u64)),
                ),
                None => (None, None),
            };
            entries.push(ReplayEntry {
                cursor: Cursor(2 * rowid as u64 - 1),
                intent,
                commit,
                commit_cursor,
            });
        }
        Ok(entries)
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::persistence::PhysicalLimits;
    use pipestream_storage::contract::{JournalPayloads, JournalProfile, run_journal_with};
    use std::collections::HashMap;
    use std::path::Path;

    fn creation() -> Creation {
        Creation {
            authority: IdentityLabel("issuer-a".into()),
            owner: IdentityLabel("alice".into()),
            creation_sequence: Id(1),
            policy: Policy {
                execution_limit_ms: Duration(60000),
                output_retention_ms: Duration(120000),
                receipt_retention_ms: Duration(180000),
            },
            results: true,
        }
    }

    fn selection() -> Capabilities {
        Capabilities {
            response: ResponseFlag(1),
            supported: vec![
                ProfileId(DURABLE_WORK.into()),
                ProfileId(RESULT_DELIVERY.into()),
            ],
            required: vec![
                ProfileId(DURABLE_WORK.into()),
                ProfileId(RESULT_DELIVERY.into()),
            ],
            control_limit: ControlLimit(65536),
            stream_limit: ConcurrencyLimit(4),
            pending_limit: ConcurrencyLimit(16),
            object_limit: Number(1 << 20),
            stream_idle_ms: IdleMs(5000),
            stream_lifetime_ms: LifetimeMs(30000),
        }
    }

    fn binding() -> Control {
        Control::Session(Session::Binding {
            request: Id(99),
            authority: creation().authority,
            owner: creation().owner,
            generation: Id(7),
            creation_sequence: Id(1),
            policy: creation().policy,
            limits: Limits {
                scopes: Id(100),
                entities: Id(10000),
                operations: Id(10000),
                retained_input_bytes: Number(1 << 20),
                retained_output_bytes: Number(1 << 20),
                active_jobs: Id(100),
            },
        })
    }

    fn bound_journal(path: &Path) -> Journal {
        let journal = Journal::initialize(
            path,
            creation(),
            JournalLimits::default(),
            PhysicalLimits::default(),
        )
        .unwrap();
        journal.record_binding(&binding(), &selection()).unwrap();
        journal
    }

    fn reopen_journal(path: &Path) -> Journal {
        Journal::open(
            path,
            creation(),
            JournalLimits::default(),
            PhysicalLimits::default(),
        )
        .unwrap()
    }

    /// Typed payloads for the bridge: every intent is a valid `ScopeCancel`
    /// mutation, every commit a matching valid `ScopeCancelled` receipt.
    /// The session identity is identical for every fixture journal (same
    /// creation + binding), so one factory serves all stores in a run.
    struct BridgePayloads {
        identity: SessionIdentity,
        counter: u64,
        mutations: HashMap<String, (OperationId, u64)>,
    }

    impl BridgePayloads {
        fn new(identity: SessionIdentity) -> Self {
            BridgePayloads {
                identity,
                counter: 0,
                mutations: HashMap::new(),
            }
        }

        fn build(&mut self, id: Option<&str>) -> pipestream_storage::Intent {
            self.counter += 1;
            let scope = self.counter;
            let (id, operation) = match id {
                Some(id) => (id.to_string(), parse_operation(id).unwrap()),
                None => {
                    let mut bytes = [0u8; 16];
                    bytes[8..].copy_from_slice(&self.counter.to_be_bytes());
                    let operation = OperationId(bytes);
                    (ClientJournalBridge::intent_id(operation), operation)
                }
            };
            let intent = Intent {
                operation,
                mutation: Mutation::ScopeCancel {
                    scope: Number(scope),
                },
            };
            let image = storage::intent_image(&intent, &self.identity).unwrap();
            let body = storage::unseal(&operation.0, &image).unwrap().to_vec();
            self.mutations.insert(id.clone(), (operation, scope));
            pipestream_storage::Intent::new(id, body).unwrap()
        }
    }

    impl JournalPayloads for BridgePayloads {
        fn intent(&mut self, _tag: &str) -> pipestream_storage::Intent {
            self.build(None)
        }

        fn variant_intent(
            &mut self,
            _tag: &str,
            base: &pipestream_storage::Intent,
        ) -> pipestream_storage::Intent {
            // Same operation id, different mutation: the journal must reject
            // the re-append as a conflict.
            let scope = self.counter + 1_000_000;
            let (id, operation) = (base.id().to_string(), parse_operation(base.id()).unwrap());
            let intent = Intent {
                operation,
                mutation: Mutation::ScopeCancel {
                    scope: Number(scope),
                },
            };
            let image = storage::intent_image(&intent, &self.identity).unwrap();
            let body = storage::unseal(&operation.0, &image).unwrap().to_vec();
            pipestream_storage::Intent::new(id, body).unwrap()
        }

        fn commit_record(
            &mut self,
            intent: &pipestream_storage::Intent,
            variant: u32,
        ) -> Vec<u8> {
            let (operation, scope) = self.mutations[intent.id()];
            let typed = Intent {
                operation,
                mutation: Mutation::ScopeCancel {
                    scope: Number(scope),
                },
            };
            let receipt = OperationReceipt {
                operation,
                request_digest: typed
                    .mutation
                    .digest(&self.identity, Producer(0), operation)
                    .unwrap(),
                body: Outcome::ScopeCancelled {
                    scope: Number(scope),
                    accepted_at: Number(u64::from(variant)),
                },
            };
            ClientJournalBridge::commit_record(&receipt).unwrap()
        }
    }

    fn identity() -> SessionIdentity {
        // Derived from the shared fixture binding: generation 7.
        SessionIdentity {
            authority: IdentityLabel("issuer-a".into()),
            owner: IdentityLabel("alice".into()),
            generation: Id(7),
        }
    }

    #[test]
    fn bridge_passes_the_journal_contract() {
        let mut payloads = BridgePayloads::new(identity());
        let dirs = std::sync::Mutex::new(Vec::new());
        let fresh = || {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("journal.sqlite");
            let journal = bound_journal(&path);
            dirs.lock().unwrap().push(dir);
            ClientJournalBridge::new(journal).unwrap()
        };
        let durable_dir = tempfile::tempdir().unwrap();
        let durable_path = durable_dir.path().join("journal.sqlite");
        bound_journal(&durable_path);
        let reopen = || ClientJournalBridge::new(reopen_journal(&durable_path)).unwrap();
        run_journal_with(JournalProfile::SPARSE, &mut payloads, fresh, reopen);
    }

    #[test]
    fn kill_recovery_is_observable_through_spi_replay() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("journal.sqlite");
        let (committed_intent, commit_record, pending_intent) = {
            let journal = bound_journal(&path);
            let mut bridge = ClientJournalBridge::new(journal).unwrap();
            let mut payloads = BridgePayloads::new(identity());
            let committed = payloads.intent("committed");
            let pending = payloads.intent("pending");
            bridge.append_intent(&committed).unwrap();
            bridge.append_intent(&pending).unwrap();
            let commit = payloads.commit_record(&committed, 0);
            bridge.commit(committed.id(), &commit).unwrap();
            (committed, commit, pending)
        };
        // Reopen: the existing kill/recovery path (the journal's open audit).
        let journal = reopen_journal(&path);
        let bridge = ClientJournalBridge::new(journal).unwrap();
        let entries = bridge.replay(Cursor(0)).unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].intent, committed_intent);
        assert_eq!(entries[0].commit.as_deref(), Some(commit_record.as_slice()));
        assert_eq!(entries[0].commit_cursor, Some(Cursor(2)));
        assert_eq!(entries[1].intent, pending_intent);
        assert!(!entries[1].committed());
        // The existing recovery view agrees: only the pending intent is
        // unresolved.
        let unresolved = bridge.inner().unresolved(Number(0), PageLimit(10)).unwrap();
        assert_eq!(unresolved.len(), 1);
        assert_eq!(
            ClientJournalBridge::intent_id(unresolved[0].1.operation),
            pending_intent.id()
        );
    }
}
