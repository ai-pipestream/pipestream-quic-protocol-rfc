//! In-memory backend implementing both SPI roles.
//!
//! Volatile by design: state lives in process memory and does not survive a
//! kill. This is the test and development backbone and the fast lane for
//! conformance fuzzing, not a durability backend. [`MemoryStore`] is
//! [`Clone`]: clones share the same underlying state, so a "reopen" in the
//! contract suite is modeled as cloning a handle.

use crate::layout;
use crate::{
    Cursor, Digest32, EntityId, Journal, JournalEntry, JournalKind, ObjectStore, ScopePath,
    SealRecord, StorageError,
};
use std::collections::BTreeMap;
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

#[derive(Debug, Default)]
struct State {
    scopes: BTreeMap<ScopePath, ScopeState>,
    /// Append-only log. A record's cursor is its 1-based position.
    journal: Vec<JournalKind>,
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
            .map_err(|_| StorageError::backend("memory store lock poisoned"))
    }

    fn write(&self) -> Result<RwLockWriteGuard<'_, State>, StorageError> {
        self.state
            .write()
            .map_err(|_| StorageError::backend("memory store lock poisoned"))
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
        let digest = layout::sha256(bytes);
        if &digest != expected_digest {
            return Err(StorageError::corrupt(format!(
                "digest mismatch for entity {entity} in {scope}"
            )));
        }
        let mut state = self.write()?;
        let scope_state = state.scopes.entry(scope.clone()).or_default();
        if scope_state.seal.is_some() {
            return Err(StorageError::already_sealed(format!(
                "scope {scope} is sealed"
            )));
        }
        match scope_state.objects.get(&entity) {
            Some(existing) if existing.digest == digest => Ok(()),
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
                "scope {scope} already sealed with a different digest or entity count"
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
            .ok_or_else(|| StorageError::not_found(format!("seal record for {scope}")))?;
        let live_count = scope_state.objects.len() as u64;
        if live_count != seal.entity_count {
            return Err(StorageError::corrupt(format!(
                "scope {scope} holds {live_count} entities but the seal record says {}",
                seal.entity_count
            )));
        }
        for (entity, object) in &scope_state.objects {
            if layout::sha256(&object.bytes) != object.digest {
                return Err(StorageError::corrupt(format!(
                    "entity {entity} in {scope} no longer matches its recorded digest"
                )));
            }
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
            return Ok(0);
        };
        if scope_state.seal.is_some() {
            let removed = scope_state.objects.len() as u64;
            state.scopes.remove(scope);
            return Ok(removed);
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
    fn append_intent(&mut self, record: &[u8]) -> Result<Cursor, StorageError> {
        let mut state = self.write()?;
        state.journal.push(JournalKind::Intent(record.to_vec()));
        Ok(Cursor(state.journal.len() as u64))
    }

    fn commit(&mut self, intent: Cursor, record: &[u8]) -> Result<(), StorageError> {
        let mut state = self.write()?;
        if intent.0 == 0 || intent.0 as usize > state.journal.len() {
            return Err(StorageError::not_found(format!("intent at {intent:?}")));
        }
        if !matches!(state.journal[intent.0 as usize - 1], JournalKind::Intent(_)) {
            return Err(StorageError::not_found(format!("intent at {intent:?}")));
        }
        let prior = state.journal.iter().find_map(|kind| match kind {
            JournalKind::Commit { intent: settled, record: existing } if *settled == intent => {
                Some(existing)
            }
            _ => None,
        });
        match prior {
            Some(existing) if existing == record => Ok(()),
            Some(_) => Err(StorageError::corrupt(format!(
                "intent at {intent:?} already committed with a different record"
            ))),
            None => {
                state.journal.push(JournalKind::Commit {
                    intent,
                    record: record.to_vec(),
                });
                Ok(())
            }
        }
    }

    fn replay(&self, since: Cursor) -> Result<Vec<JournalEntry>, StorageError> {
        let state = self.read()?;
        let entries = state
            .journal
            .iter()
            .enumerate()
            .skip(since.0 as usize)
            .map(|(index, kind)| JournalEntry {
                cursor: Cursor(index as u64 + 1),
                kind: kind.clone(),
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

    #[test]
    fn reopen_via_clone_shares_state() {
        let mut store = MemoryStore::new();
        let scope = ScopePath::root();
        let bytes = b"shared state";
        store
            .put(&scope, 1, bytes, &crate::sha256(bytes))
            .unwrap();
        let handle = store.clone();
        assert_eq!(handle.list(&scope).unwrap(), vec![1]);
    }
}
