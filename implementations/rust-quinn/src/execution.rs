//! Durable execution attempts. Acquire and publish in separate store transactions;
//! application callbacks run between them, without holding a database transaction.

use crate::{
    ProtocolError,
    authorization::PrincipalBinding,
    session::{EntityKey, EntityState, Session},
};
use rand::{TryRng, rngs::SysRng};
use serde::{Deserialize, Serialize};

pub const MAX_EXECUTION_LEASE_MICROS: u64 = 300_000_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum ExecutionStage {
    Process,
    Rehydrate,
    Resume { claim_id: u64 },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ExecutionKey {
    pub entity: EntityKey,
    pub stage: ExecutionStage,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionRecord {
    pub epoch: u64,
    pub executor: [u8; 16],
    pub acquired_at_micros: u64,
    pub expires_at_micros: u64,
    pub completed_at_micros: Option<u64>,
}

/// A local execution fence, not a bearer credential or a protocol recovery token.
/// External-effect sinks must enforce their own transactional fence or idempotency key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutionLease {
    session_id: String,
    key: ExecutionKey,
    owner: Option<PrincipalBinding>,
    record: ExecutionRecord,
}

impl ExecutionLease {
    #[must_use]
    pub fn session_id(&self) -> &str {
        &self.session_id
    }

    #[must_use]
    pub fn principal_binding(&self) -> Option<&PrincipalBinding> {
        self.owner.as_ref()
    }

    #[must_use]
    pub fn key(&self) -> ExecutionKey {
        self.key
    }

    #[must_use]
    pub fn epoch(&self) -> u64 {
        self.record.epoch
    }

    #[must_use]
    pub fn executor(&self) -> [u8; 16] {
        self.record.executor
    }

    #[must_use]
    pub fn expires_at_micros(&self) -> u64 {
        self.record.expires_at_micros
    }
}

impl Session {
    /// Returns no lease if another unexpired attempt owns the operation, or it finished.
    /// A failed/expired callback is not successful completion. Retrying increments the fence.
    pub fn acquire_execution(
        &mut self,
        caller: Option<&PrincipalBinding>,
        key: ExecutionKey,
        now_micros: u64,
        lease_micros: u64,
    ) -> Result<Option<ExecutionLease>, ProtocolError> {
        self.authorize(caller)?;
        if !(1..=MAX_EXECUTION_LEASE_MICROS).contains(&lease_micros) {
            return Err(ProtocolError::entity("invalid execution lease parameters"));
        }
        let expires_at_micros = now_micros
            .checked_add(lease_micros)
            .ok_or_else(|| ProtocolError::entity("execution lease clock overflow"))?;
        let previous = self.executions.get(&key);
        if previous.is_some_and(|record| {
            record.completed_at_micros.is_some() || now_micros < record.expires_at_micros
        }) {
            return Ok(None);
        }
        self.validate_execution_state(key)?;
        let epoch = previous
            .map_or(0, |record| record.epoch)
            .checked_add(1)
            .ok_or_else(|| ProtocolError::limit("execution epoch exhausted"))?;
        let mut executor = [0; 16];
        SysRng.try_fill_bytes(&mut executor).map_err(|error| {
            ProtocolError::entity(format!("execution identity generation failed: {error}"))
        })?;
        let record = ExecutionRecord {
            epoch,
            executor,
            acquired_at_micros: now_micros,
            expires_at_micros,
            completed_at_micros: None,
        };
        self.executions.insert(key, record.clone());
        Ok(Some(ExecutionLease {
            session_id: self.session_id.clone(),
            key,
            owner: caller.cloned(),
            record,
        }))
    }

    /// Commit the protocol result and attempt completion atomically in a store transaction.
    /// The closure only applies a computed result; it must not perform application I/O.
    pub fn publish_execution<T>(
        &mut self,
        caller: Option<&PrincipalBinding>,
        lease: &ExecutionLease,
        now_micros: u64,
        apply: impl FnOnce(&mut Self) -> Result<T, ProtocolError>,
    ) -> Result<T, ProtocolError> {
        self.authorize(caller)?;
        if lease.session_id != self.session_id
            || lease.owner.as_ref() != caller
            || self.executions.get(&lease.key) != Some(&lease.record)
            || lease.record.completed_at_micros.is_some()
            || now_micros < lease.record.acquired_at_micros
            || now_micros >= lease.record.expires_at_micros
        {
            return Err(ProtocolError::entity("execution lease is stale or expired"));
        }
        self.validate_execution_state(lease.key)?;
        let result = apply(self)?;
        self.executions
            .get_mut(&lease.key)
            .expect("lease was validated")
            .completed_at_micros = Some(now_micros);
        Ok(result)
    }

    fn validate_execution_state(&self, key: ExecutionKey) -> Result<(), ProtocolError> {
        let entity = self
            .entities
            .get(&key.entity)
            .ok_or_else(|| ProtocolError::entity("execution entity is not admitted"))?;
        let valid = match key.stage {
            ExecutionStage::Process => {
                entity.state == EntityState::Processing
                    && !self.claims.values().any(|claim| {
                        claim.entity == key.entity && claim.redeemed_at_micros.is_some()
                    })
            }
            ExecutionStage::Rehydrate => {
                entity.state == EntityState::Rehydrating
                    && self.manifests.get(&key.entity).is_some_and(|manifest| {
                        self.scopes
                            .get(&manifest.child_scope_id)
                            .is_some_and(|scope| scope.digest.is_some())
                    })
            }
            ExecutionStage::Resume { claim_id } => {
                entity.state == EntityState::Processing
                    && self.claims.get(&claim_id).is_some_and(|claim| {
                        claim.entity == key.entity && claim.redeemed_at_micros.is_some()
                    })
            }
        };
        if !valid {
            return Err(ProtocolError::entity(
                "entity is not ready for this execution stage",
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
