//! In-memory backend implementing both SPI roles.
//!
//! Volatile by design: state lives in process memory and does not survive a
//! kill. This is the test and development backbone and the fast lane for
//! conformance fuzzing, not a durability backend. Append order, cursor
//! assignment, idempotent retries, and replay semantics are identical to
//! the durable backends. [`MemoryStore`] is [`Clone`]: clones share the
//! same underlying state, so a "reopen" in the contract suite is modeled as
//! cloning a handle.

use crate::layout;
use crate::{
    Cursor, Digest32, EntityId, Intent, Journal, ObjectStore, ReplayEntry, ScopePath, SealRecord,
    StorageError,
};
use std::collections::{BTreeMap, HashMap};
use std::io::{self, Read};
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};

/// Per-scope state: the objects and, once sealed, the seal record.
#[derive(Debug, Default)]
struct ScopeState {
    objects: BTreeMap<EntityId, StoredObject>,
    seal: Option<SealRecord>,
}

#[derive(Debug)]
struct StoredObject {
    digest: Digest32,
    bytes: Vec<u8>,
}

#[derive(Debug)]
struct IntentState {
    intent: Intent,
    cursor: Cursor,
    commit: Option<Vec<u8>>,
    commit_cursor: Option<Cursor>,
}

#[derive(Debug, Default)]
struct State {
    scopes: BTreeMap<ScopePath, ScopeState>,
    intents: Vec<IntentState>,
    by_intent_id: HashMap<String, usize>,
    last_cursor: u64,
}

/// In-memory backend for both the [`ObjectStore`] and [`Journal`] roles.
///
/// Volatile: nothing survives process exit. Clones share state.
#[derive(Debug, Clone, Default)]
pub struct MemoryStore {
    state: Arc<RwLock<State>>,
}

impl MemoryStore {
    /// Creates an empty store.
    pub fn new() -> Self {
        MemoryStore::default()
    }

    fn read(&self) -> Result<RwLockReadGuard<'_, State>, StorageError> {
        self.state
            .read()
            .map_err(|_| StorageError::failed("memory store lock poisoned"))
    }

    fn write(&self) -> Result<RwLockWriteGuard<'_, State>, StorageError> {
        self.state
            .write()
            .map_err(|_| StorageError::failed("memory store lock poisoned"))
    }
}

/// A snapshot reader over an in-memory object.
///
/// The bytes are copied at open time, so retiring the object afterwards
/// cannot invalidate the read.
#[derive(Debug)]
pub struct MemoryReader {
    bytes: Vec<u8>,
    position: usize,
}

impl Read for MemoryReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let remaining = &self.bytes[self.position..];
        let n = remaining.len().min(buf.len());
        buf[..n].copy_from_slice(&remaining[..n]);
        self.position += n;
        Ok(n)
    }
}

impl ObjectStore for MemoryStore {
    type Reader = MemoryReader;

    fn put(
        &mut self,
        scope: &ScopePath,
        entity: EntityId,
        bytes: &[u8],
        expected_digest: &Digest32,
    ) -> Result<(), StorageError> {
        layout::check_entity_id(entity)?;
        let mut state = self.write()?;
        let scope_state = state.scopes.entry(scope.clone()).or_default();
        // Seal is checked before the digest: a sealed scope rejects every
        // put regardless of bytes.
        if scope_state.seal.is_some() {
            return Err(StorageError::already_sealed(format!(
                "scope {scope} is sealed"
            )));
        }
        let digest = layout::sha256(bytes);
        if &digest != expected_digest {
            return Err(StorageError::corrupt(format!(
                "digest mismatch for entity {entity} in {scope}"
            )));
        }
        match scope_state.objects.get(&entity) {
            Some(existing) if existing.bytes == bytes => Ok(()),
            Some(_) => Err(StorageError::corrupt(format!(
                "conflicting bytes for entity {entity} in {scope}"
            ))),
            None => {
                scope_state.objects.insert(
                    entity,
                    StoredObject {
                        digest,
                        bytes: bytes.to_vec(),
                    },
                );
                Ok(())
            }
        }
    }

    fn open_read(&self, scope: &ScopePath, entity: EntityId) -> Result<MemoryReader, StorageError> {
        let state = self.read()?;
        let scope_state = state
            .scopes
            .get(scope)
            .ok_or_else(|| StorageError::not_found(format!("scope {scope}")))?;
        let object = scope_state
            .objects
            .get(&entity)
            .ok_or_else(|| StorageError::not_found(format!("entity {entity} in {scope}")))?;
        Ok(MemoryReader {
            bytes: object.bytes.clone(),
            position: 0,
        })
    }

    fn seal_scope(
        &mut self,
        scope: &ScopePath,
        digest: &Digest32,
        entity_count: u64,
    ) -> Result<(), StorageError> {
        let mut state = self.write()?;
        let scope_state = state.scopes.entry(scope.clone()).or_default();
        match scope_state.seal {
            Some(existing) if existing.digest == *digest && existing.entity_count == entity_count => {
                Ok(())
            }
            Some(_) => Err(StorageError::corrupt(format!(
                "scope {scope} already carries a different seal"
            ))),
            None => {
                scope_state.seal = Some(SealRecord {
                    digest: *digest,
                    entity_count,
                });
                Ok(())
            }
        }
    }

    fn verify_scope(&self, scope: &ScopePath) -> Result<SealRecord, StorageError> {
        let state = self.read()?;
        let scope_state = state
            .scopes
            .get(scope)
            .ok_or_else(|| StorageError::not_found(format!("scope {scope}")))?;
        let seal = scope_state
            .seal
            .ok_or_else(|| StorageError::not_found(format!("scope {scope} has no seal record")))?;
        let mut live_count = 0u64;
        for (entity, object) in &scope_state.objects {
            if layout::sha256(&object.bytes) != object.digest {
                return Err(StorageError::corrupt(format!(
                    "entity {entity} in {scope} fails its recorded digest"
                )));
            }
            live_count += 1;
        }
        if live_count != seal.entity_count {
            return Err(StorageError::corrupt(format!(
                "scope {scope} entity count differs from its seal"
            )));
        }
        Ok(seal)
    }

    fn list(&self, scope: &ScopePath) -> Result<Vec<EntityId>, StorageError> {
        let state = self.read()?;
        let entities = match state.scopes.get(scope) {
            Some(scope_state) => scope_state.objects.keys().copied().collect(),
            None => Vec::new(),
        };
        Ok(entities)
    }

    fn retire(&mut self, scope: &ScopePath, retention_cursor: u64) -> Result<u64, StorageError> {
        let mut state = self.write()?;
        let Some(scope_state) = state.scopes.get_mut(scope) else {
            return Err(StorageError::not_found(format!(
                "scope {scope} holds nothing to retire"
            )));
        };
        if scope_state.seal.is_some() {
            let removed = scope_state.objects.len() as u64;
            state.scopes.remove(scope);
            return Ok(removed);
        }
        if scope_state.objects.is_empty() {
            return Err(StorageError::not_found(format!(
                "scope {scope} holds nothing to retire"
            )));
        }
        let doomed: Vec<EntityId> = scope_state
            .objects
            .keys()
            .filter(|id| u64::from(**id) <= retention_cursor)
            .copied()
            .collect();
        let removed = doomed.len() as u64;
        for id in doomed {
            scope_state.objects.remove(&id);
        }
        Ok(removed)
    }
}

impl Journal for MemoryStore {
    fn append_intent(&mut self, intent: &Intent) -> Result<Cursor, StorageError> {
        let mut guard = self.write()?;
        let state = &mut *guard;
        if let Some(index) = state.by_intent_id.get(intent.id()) {
            let existing = &state.intents[*index];
            if existing.intent.record() == intent.record() {
                return Ok(existing.cursor);
            }
            return Err(StorageError::corrupt(format!(
                "intent {} already exists with different bytes",
                intent.id()
            )));
        }
        state.last_cursor += 1;
        let cursor = Cursor(state.last_cursor);
        let position = state.intents.len();
        state.by_intent_id.insert(intent.id().to_string(), position);
        state.intents.push(IntentState {
            intent: intent.clone(),
            cursor,
            commit: None,
            commit_cursor: None,
        });
        Ok(cursor)
    }

    fn commit(&mut self, intent_id: &str, record: &[u8]) -> Result<(), StorageError> {
        if record.is_empty() || record.len() > crate::MAX_RECORD_BYTES {
            return Err(StorageError::failed(format!(
                "commit record must be 1 to {} octets",
                crate::MAX_RECORD_BYTES
            )));
        }
        let mut guard = self.write()?;
        let state = &mut *guard;
        let Some(&index) = state.by_intent_id.get(intent_id) else {
            return Err(StorageError::not_found(format!(
                "intent {intent_id} has no durable append"
            )));
        };
        {
            let entry = &state.intents[index];
            match &entry.commit {
                Some(existing) if existing == record => return Ok(()),
                Some(_) => {
                    return Err(StorageError::corrupt(format!(
                        "intent {intent_id} already carries a different commit"
                    )));
                }
                None => {}
            }
        }
        // A commit consumes a cursor in the global append order.
        state.last_cursor += 1;
        state.intents[index].commit = Some(record.to_vec());
        state.intents[index].commit_cursor = Some(Cursor(state.last_cursor));
        Ok(())
    }

    fn replay(&self, since: Cursor) -> Result<Vec<ReplayEntry>, StorageError> {
        let state = self.read()?;
        let entries = state
            .intents
            .iter()
            .filter(|entry| entry.cursor > since)
            .map(|entry| ReplayEntry {
                cursor: entry.cursor,
                intent: entry.intent.clone(),
                commit: entry.commit.clone(),
                commit_cursor: entry.commit_cursor,
            })
            .collect();
        Ok(entries)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::contract;

    #[test]
    fn passes_the_spi_contract() {
        // The in-memory backend is volatile, so "reopen" means a second
        // handle onto the same shared state (clones share the Arc).
        let shared = MemoryStore::new();
        contract::run_all(MemoryStore::new, move || shared.clone());
    }
}
