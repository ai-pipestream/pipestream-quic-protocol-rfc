//! Transport-independent recursive session state machine.

use crate::{
    Barrier, CHECKPOINT_ACK, Checkpoint, CompletionMode, CompletionPolicy, ERROR_CLAIM_EXPIRED,
    ERROR_CLAIM_NOT_FOUND, ERROR_DEPTH_EXCEEDED, ERROR_ENTITY_INVALID, ERROR_LIMIT_EXCEEDED,
    ERROR_SCOPE_INVALID, FailureAction, MAX_ENTITY_ID, ProtocolError, STATUS_ABANDONED,
    STATUS_CHECKPOINT, STATUS_COMPLETE, STATUS_DEFERRED, STATUS_DEHYDRATING, STATUS_FAILED,
    STATUS_PENDING, STATUS_PROCESSING, STATUS_REHYDRATING, STATUS_RETRYING, STATUS_SKIPPED,
    STATUS_YIELDED, ScopeDigest, StoppingPointValidation,
};
use rand::{TryRng, rngs::SysRng};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};

pub const SESSION_FORMAT_VERSION: u16 = 5;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct EntityKey {
    pub scope_id: u32,
    pub entity_id: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u8)]
pub enum EntityState {
    Pending = STATUS_PENDING,
    Processing = STATUS_PROCESSING,
    Complete = STATUS_COMPLETE,
    Failed = STATUS_FAILED,
    Checkpoint = STATUS_CHECKPOINT,
    Dehydrating = STATUS_DEHYDRATING,
    Rehydrating = STATUS_REHYDRATING,
    Yielded = STATUS_YIELDED,
    Deferred = STATUS_DEFERRED,
    Retrying = STATUS_RETRYING,
    Skipped = STATUS_SKIPPED,
    Abandoned = STATUS_ABANDONED,
}

impl EntityState {
    #[must_use]
    pub fn code(self) -> u8 {
        self as u8
    }

    #[must_use]
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Complete | Self::Failed | Self::Skipped | Self::Abandoned
        )
    }

    fn permits(self, next: Self) -> bool {
        matches!(
            (self, next),
            (
                Self::Pending,
                Self::Processing
                    | Self::Dehydrating
                    | Self::Failed
                    | Self::Skipped
                    | Self::Abandoned
            ) | (
                Self::Processing,
                Self::Complete
                    | Self::Failed
                    | Self::Dehydrating
                    | Self::Checkpoint
                    | Self::Yielded
                    | Self::Deferred
                    | Self::Abandoned
            ) | (
                Self::Dehydrating,
                Self::Rehydrating | Self::Failed | Self::Abandoned
            ) | (
                Self::Rehydrating,
                Self::Complete | Self::Failed | Self::Abandoned
            ) | (Self::Checkpoint, Self::Processing)
                | (
                    Self::Yielded,
                    Self::Processing | Self::Failed | Self::Deferred | Self::Abandoned
                )
                | (
                    Self::Deferred,
                    Self::Processing | Self::Failed | Self::Skipped | Self::Abandoned
                )
                | (Self::Failed, Self::Retrying | Self::Abandoned)
                | (
                    Self::Retrying,
                    Self::Processing | Self::Failed | Self::Abandoned
                )
        )
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NewEntity {
    pub entity_id: u32,
    pub layer: u8,
    pub payload_digest: [u8; 32],
    pub policy: Option<CompletionPolicy>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EntityRecord {
    pub key: EntityKey,
    pub parent: Option<EntityKey>,
    pub depth: u8,
    pub layer: u8,
    pub state: EntityState,
    pub payload_digest: [u8; 32],
    pub output_digest: Option<[u8; 32]>,
    pub policy: Option<CompletionPolicy>,
    pub retry_count: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ResolutionState {
    Active,
    Resolved,
    Partial,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AssemblyManifest {
    pub parent: EntityKey,
    pub child_scope_id: u32,
    pub children: Vec<EntityKey>,
    pub policy: CompletionPolicy,
    pub created_at_micros: u64,
    pub state: ResolutionState,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScopeRecord {
    pub scope_id: u32,
    pub parent: Option<EntityKey>,
    pub depth: u8,
    pub entities: BTreeSet<u32>,
    pub child_scopes: BTreeSet<u32>,
    pub digest: Option<[u8; 32]>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredCheckpoint {
    pub checkpoint_id: String,
    pub sequence_number: u64,
    pub checkpoint_entity_id: u32,
    pub scope_id: u32,
    pub timeout_ms: Option<u64>,
    pub acknowledged: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClaimRecord {
    pub claim_id: u64,
    pub entity: EntityKey,
    pub expiry_timestamp_micros: u64,
    pub token: Vec<u8>,
    pub validation: StoppingPointValidation,
    pub redeemed_at_micros: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Session {
    pub format_version: u16,
    pub session_id: String,
    pub max_scope_depth: u8,
    pub max_entities_per_scope: u32,
    pub scopes: BTreeMap<u32, ScopeRecord>,
    pub entities: BTreeMap<EntityKey, EntityRecord>,
    pub manifests: BTreeMap<EntityKey, AssemblyManifest>,
    pub checkpoints: BTreeMap<(u32, u64), StoredCheckpoint>,
    pub claims: BTreeMap<u64, ClaimRecord>,
    pub work_sets: Option<crate::work_set::WorkSets>,
    pub owner: Option<crate::authorization::SessionOwner>,
    pub executions: BTreeMap<crate::execution::ExecutionKey, crate::execution::ExecutionRecord>,
    pub jobs: BTreeMap<crate::execution::ExecutionKey, crate::jobs::JobRecord>,
}

impl Session {
    pub fn new(
        session_id: impl Into<String>,
        max_scope_depth: u8,
        max_entities_per_scope: u32,
    ) -> Result<Self, ProtocolError> {
        let session_id = session_id.into();
        validate_session_id(&session_id)?;
        if max_scope_depth > 7 {
            return Err(depth_error("max scope depth exceeds 7"));
        }
        if max_entities_per_scope == 0 || max_entities_per_scope > MAX_ENTITY_ID {
            return Err(limit_error("invalid max entities per scope"));
        }
        let mut scopes = BTreeMap::new();
        scopes.insert(
            0,
            ScopeRecord {
                scope_id: 0,
                parent: None,
                depth: 0,
                entities: BTreeSet::new(),
                child_scopes: BTreeSet::new(),
                digest: None,
            },
        );
        Ok(Self {
            format_version: SESSION_FORMAT_VERSION,
            session_id,
            max_scope_depth,
            max_entities_per_scope,
            scopes,
            entities: BTreeMap::new(),
            manifests: BTreeMap::new(),
            checkpoints: BTreeMap::new(),
            claims: BTreeMap::new(),
            work_sets: None,
            owner: None,
            executions: BTreeMap::new(),
            jobs: BTreeMap::new(),
        })
    }

    pub fn add_root(&mut self, entity: NewEntity) -> Result<EntityKey, ProtocolError> {
        let key = EntityKey {
            scope_id: 0,
            entity_id: entity.entity_id,
        };
        self.insert_entity(key, None, 0, entity)?;
        Ok(key)
    }

    pub fn transition(&mut self, key: EntityKey, next: EntityState) -> Result<(), ProtocolError> {
        let record = self
            .entities
            .get_mut(&key)
            .ok_or_else(|| entity_error("entity is not registered"))?;
        if !record.state.permits(next) {
            return Err(entity_error(format!(
                "invalid state transition {:?} -> {:?}",
                record.state, next
            )));
        }
        if record.state == EntityState::Failed && next == EntityState::Retrying {
            let policy = record.policy.as_ref();
            let max_retries = policy.map_or(0, CompletionPolicy::effective_max_retries);
            if policy.map(CompletionPolicy::effective_on_failure) != Some(FailureAction::Retry)
                || max_retries == 0
                || record.retry_count >= max_retries
            {
                return Err(entity_error("retry budget is exhausted"));
            }
            record.retry_count += 1;
        }
        record.state = next;
        Ok(())
    }

    pub fn complete_entity(
        &mut self,
        key: EntityKey,
        output_digest: [u8; 32],
    ) -> Result<(), ProtocolError> {
        self.transition(key, EntityState::Complete)?;
        self.entities
            .get_mut(&key)
            .expect("transition checked entity existence")
            .output_digest = Some(output_digest);
        Ok(())
    }

    pub fn dehydrate(
        &mut self,
        parent: EntityKey,
        child_scope_id: u32,
        children: Vec<NewEntity>,
        created_at_micros: u64,
    ) -> Result<Vec<EntityKey>, ProtocolError> {
        if children.is_empty() {
            return Err(scope_error(
                "zero-child dehydration completes without opening a scope",
            ));
        }
        if children.len() > self.max_entities_per_scope as usize {
            return Err(limit_error("child scope exceeds entity limit"));
        }
        let mut ids = BTreeSet::new();
        for child in &children {
            validate_entity_id(child.entity_id)?;
            if child.layer > 3 {
                return Err(entity_error("entity layer exceeds 3"));
            }
            if let Some(policy) = &child.policy {
                crate::validate_completion_policy(policy)?;
            }
            if !ids.insert(child.entity_id) {
                return Err(entity_error("duplicate entity ID in child scope"));
            }
        }
        self.begin_dehydrating(parent)?;
        self.open_child_scope(parent, child_scope_id, created_at_micros)?;
        let mut keys = Vec::with_capacity(children.len());
        for child in children {
            keys.push(self.add_child(child_scope_id, child)?);
        }
        Ok(keys)
    }

    pub fn begin_dehydrating(&mut self, parent: EntityKey) -> Result<(), ProtocolError> {
        if self.manifests.contains_key(&parent) {
            return Err(entity_error("parent already has an assembly manifest"));
        }
        self.transition(parent, EntityState::Dehydrating)
    }

    pub fn open_child_scope(
        &mut self,
        parent: EntityKey,
        child_scope_id: u32,
        created_at_micros: u64,
    ) -> Result<(), ProtocolError> {
        if child_scope_id == 0 || self.scopes.contains_key(&child_scope_id) {
            return Err(scope_error("child scope ID is zero or already active"));
        }
        let parent_record = self
            .entities
            .get(&parent)
            .ok_or_else(|| entity_error("dehydrating parent is not registered"))?;
        if parent_record.state != EntityState::Dehydrating {
            return Err(entity_error("parent has not entered DEHYDRATING"));
        }
        if self.manifests.contains_key(&parent) {
            return Err(entity_error("parent already has an assembly manifest"));
        }
        let depth = parent_record
            .depth
            .checked_add(1)
            .ok_or_else(|| depth_error("scope depth overflow"))?;
        if depth > self.max_scope_depth {
            return Err(depth_error("dehydration exceeds negotiated depth"));
        }
        self.scopes.insert(
            child_scope_id,
            ScopeRecord {
                scope_id: child_scope_id,
                parent: Some(parent),
                depth,
                entities: BTreeSet::new(),
                child_scopes: BTreeSet::new(),
                digest: None,
            },
        );
        self.scopes
            .get_mut(&parent.scope_id)
            .expect("parent scope exists for a registered entity")
            .child_scopes
            .insert(child_scope_id);
        let policy = parent_record.policy.clone().unwrap_or_default();
        self.manifests.insert(
            parent,
            AssemblyManifest {
                parent,
                child_scope_id,
                children: Vec::new(),
                policy,
                created_at_micros,
                state: ResolutionState::Active,
            },
        );
        Ok(())
    }

    pub fn add_child(
        &mut self,
        child_scope_id: u32,
        child: NewEntity,
    ) -> Result<EntityKey, ProtocolError> {
        let scope = self
            .scopes
            .get(&child_scope_id)
            .ok_or_else(|| scope_error("child scope is not registered"))?;
        if scope.digest.is_some() {
            return Err(scope_error("child scope is already closed"));
        }
        let parent = scope
            .parent
            .ok_or_else(|| scope_error("root scope does not accept scoped children"))?;
        let depth = scope.depth;
        let key = EntityKey {
            scope_id: child_scope_id,
            entity_id: child.entity_id,
        };
        self.insert_entity(key, Some(parent), depth, child)?;
        self.manifests
            .get_mut(&parent)
            .expect("child scope and manifest are created together")
            .children
            .push(key);
        Ok(key)
    }

    pub fn close_scope(&mut self, scope_id: u32) -> Result<ScopeDigest, ProtocolError> {
        let digest = self.scope_digest(scope_id)?;
        self.scopes
            .get_mut(&scope_id)
            .expect("scope was checked while calculating its digest")
            .digest = Some(digest.merkle_root);
        Ok(digest)
    }

    pub fn close_scope_with_expected(
        &mut self,
        expected: &ScopeDigest,
    ) -> Result<ScopeDigest, ProtocolError> {
        let actual = self.scope_digest(expected.scope_id)?;
        if &actual != expected {
            return Err(ProtocolError::new(
                crate::ERROR_INTEGRITY,
                "PIPESTREAM_INTEGRITY_ERROR",
                "peer SCOPE_DIGEST differs from locally observed terminal states",
            ));
        }
        self.scopes
            .get_mut(&expected.scope_id)
            .expect("scope was checked while calculating its digest")
            .digest = Some(actual.merkle_root);
        Ok(actual)
    }

    pub fn scope_digest(&self, scope_id: u32) -> Result<ScopeDigest, ProtocolError> {
        if self.work_sets.is_some() && !self.work_scope_ready(scope_id) {
            return Err(scope_error(
                "work set is unsealed or has unresolved declarations",
            ));
        }
        if scope_id == 0 {
            return Err(scope_error("root scope is not propagated"));
        }
        let scope = self
            .scopes
            .get(&scope_id)
            .ok_or_else(|| scope_error("scope is not registered"))?;
        if scope.child_scopes.iter().any(|child| {
            self.scopes
                .get(child)
                .is_none_or(|value| value.digest.is_none())
        }) {
            return Err(scope_error("nested scope is not closed"));
        }
        let mut statuses = Vec::with_capacity(scope.entities.len());
        for entity_id in &scope.entities {
            let record = self
                .entities
                .get(&EntityKey {
                    scope_id,
                    entity_id: *entity_id,
                })
                .expect("scope entity index and records remain consistent");
            if !self.entity_is_resolved(record) {
                return Err(scope_error("scope still has non-terminal entities"));
            }
            statuses.push((*entity_id, record.state));
        }
        let merkle_root = merkle_root(&statuses)?;
        let digest = ScopeDigest {
            scope_id,
            entities_processed: statuses.len() as u64,
            entities_succeeded: statuses
                .iter()
                .filter(|(_, state)| *state == EntityState::Complete)
                .count() as u64,
            entities_failed: statuses
                .iter()
                .filter(|(_, state)| matches!(state, EntityState::Failed | EntityState::Abandoned))
                .count() as u64,
            entities_deferred: statuses
                .iter()
                .filter(|(_, state)| *state == EntityState::Deferred)
                .count() as u64,
            merkle_root,
        };
        Ok(digest)
    }

    pub fn begin_rehydration(&mut self, parent: EntityKey) -> Result<(), ProtocolError> {
        let manifest = self
            .manifests
            .get(&parent)
            .ok_or_else(|| entity_error("assembly manifest is absent"))?;
        if self
            .scopes
            .get(&manifest.child_scope_id)
            .is_none_or(|scope| scope.digest.is_none())
        {
            return Err(scope_error("child scope has not emitted its digest"));
        }
        let state = self.resolution_state(manifest)?;
        match state {
            ResolutionState::Active => {
                return Err(entity_error("children have not reached a resolution"));
            }
            ResolutionState::Failed => {
                return Err(entity_error("completion policy refuses rehydration"));
            }
            ResolutionState::Resolved | ResolutionState::Partial => {}
        }
        self.transition(parent, EntityState::Rehydrating)?;
        self.manifests
            .get_mut(&parent)
            .expect("manifest was checked above")
            .state = state;
        Ok(())
    }

    pub fn complete_rehydration(
        &mut self,
        parent: EntityKey,
        output_digest: [u8; 32],
    ) -> Result<(), ProtocolError> {
        let manifest = self
            .manifests
            .get(&parent)
            .ok_or_else(|| entity_error("assembly manifest is absent"))?;
        if manifest.state == ResolutionState::Active {
            return Err(entity_error("rehydration began without resolved children"));
        }
        self.transition(parent, EntityState::Complete)?;
        self.entities
            .get_mut(&parent)
            .expect("transition checked entity existence")
            .output_digest = Some(output_digest);
        Ok(())
    }

    pub fn request_checkpoint(&mut self, checkpoint: &Checkpoint) -> Result<(), ProtocolError> {
        if checkpoint.flags != 0 {
            return Err(entity_error("checkpoint request carries ACK"));
        }
        let scope_id = checkpoint.scope_id.unwrap_or(0);
        if !self.scopes.contains_key(&scope_id) {
            return Err(scope_error("checkpoint scope is not registered"));
        }
        validate_entity_id(checkpoint.checkpoint_entity_id)?;
        let key = (scope_id, checkpoint.sequence_number);
        if let Some(existing) = self.checkpoints.get(&key) {
            if existing.checkpoint_id == checkpoint.checkpoint_id
                && existing.checkpoint_entity_id == checkpoint.checkpoint_entity_id
                && existing.timeout_ms == checkpoint.timeout_ms
            {
                return Ok(());
            }
            return Err(entity_error(
                "checkpoint sequence was reused with different fields",
            ));
        }
        self.checkpoints.insert(
            key,
            StoredCheckpoint {
                checkpoint_id: checkpoint.checkpoint_id.clone(),
                sequence_number: checkpoint.sequence_number,
                checkpoint_entity_id: checkpoint.checkpoint_entity_id,
                scope_id,
                timeout_ms: checkpoint.timeout_ms,
                acknowledged: false,
            },
        );
        Ok(())
    }

    pub fn checkpoint_satisfied(
        &self,
        scope_id: u32,
        sequence_number: u64,
    ) -> Result<bool, ProtocolError> {
        let checkpoint = self
            .checkpoints
            .get(&(scope_id, sequence_number))
            .ok_or_else(|| entity_error("checkpoint is not registered"))?;
        let scope = self
            .scopes
            .get(&scope_id)
            .ok_or_else(|| scope_error("checkpoint scope is not registered"))?;
        if let Some(work) = &self.work_sets {
            if !self.work_scope_ready(scope_id) {
                return Ok(false);
            }
            if work.scopes[&scope_id].ids.last() != Some(&checkpoint.checkpoint_entity_id) {
                return Err(entity_error(
                    "sealed checkpoint must name the last declared entity",
                ));
            }
        }
        for entity_id in &scope.entities {
            if is_before(*entity_id, checkpoint.checkpoint_entity_id) {
                let record = self
                    .entities
                    .get(&EntityKey {
                        scope_id,
                        entity_id: *entity_id,
                    })
                    .expect("scope entity index and records remain consistent");
                if !self.entity_is_resolved(record) {
                    return Ok(false);
                }
            }
        }
        if self.manifests.values().any(|manifest| {
            manifest.parent.scope_id == scope_id && manifest.state == ResolutionState::Active
        }) {
            return Ok(false);
        }
        if scope.child_scopes.iter().any(|child| {
            self.scopes
                .get(child)
                .is_none_or(|value| value.digest.is_none())
        }) {
            return Ok(false);
        }
        if self
            .checkpoints
            .iter()
            .any(|((other_scope, other_sequence), other)| {
                *other_sequence < sequence_number
                    && self.is_descendant_scope(*other_scope, scope_id)
                    && !other.acknowledged
            })
        {
            return Ok(false);
        }
        Ok(true)
    }

    pub fn acknowledge_checkpoint(
        &mut self,
        scope_id: u32,
        sequence_number: u64,
    ) -> Result<Checkpoint, ProtocolError> {
        if !self.checkpoint_satisfied(scope_id, sequence_number)? {
            return Err(entity_error("checkpoint barrier is not satisfied"));
        }
        let stored = self
            .checkpoints
            .get_mut(&(scope_id, sequence_number))
            .expect("checkpoint was checked above");
        stored.acknowledged = true;
        Ok(Checkpoint {
            checkpoint_id: stored.checkpoint_id.clone(),
            sequence_number: stored.sequence_number,
            checkpoint_entity_id: stored.checkpoint_entity_id,
            scope_id: (stored.scope_id != 0).then_some(stored.scope_id),
            flags: CHECKPOINT_ACK,
            timeout_ms: stored.timeout_ms,
        })
    }

    pub fn barrier(&self, scope_id: u32) -> Result<Barrier, ProtocolError> {
        let scope = self
            .scopes
            .get(&scope_id)
            .ok_or_else(|| scope_error("barrier scope is not registered"))?;
        let parent = scope
            .parent
            .ok_or_else(|| scope_error("root scope does not use BARRIER"))?;
        Ok(Barrier {
            released: scope.digest.is_some(),
            scope_id,
            parent_entity_id: parent.entity_id,
        })
    }

    pub fn defer_with_random_claim(
        &mut self,
        entity: EntityKey,
        token: Vec<u8>,
        validation: StoppingPointValidation,
        expiry_timestamp_micros: u64,
        now_micros: u64,
    ) -> Result<ClaimRecord, ProtocolError> {
        let mut rng = SysRng;
        for _ in 0..32 {
            let claim_id = rng.try_next_u64().map_err(|error| {
                entity_error(format!("secure claim ID generation failed: {error}"))
            })?;
            if claim_id != 0 && !self.claims.contains_key(&claim_id) {
                return self.defer_with_claim_id(
                    entity,
                    token,
                    validation,
                    claim_id,
                    expiry_timestamp_micros,
                    now_micros,
                );
            }
        }
        Err(limit_error("could not allocate a unique claim ID"))
    }

    pub fn defer_with_claim_id(
        &mut self,
        entity: EntityKey,
        token: Vec<u8>,
        validation: StoppingPointValidation,
        claim_id: u64,
        expiry_timestamp_micros: u64,
        now_micros: u64,
    ) -> Result<ClaimRecord, ProtocolError> {
        if claim_id == 0 || self.claims.contains_key(&claim_id) {
            return Err(claim_not_found("claim ID is zero or already allocated"));
        }
        if expiry_timestamp_micros <= now_micros {
            return Err(claim_expired("claim expiry is not in the future"));
        }
        validate_stopping_point(&validation)?;
        if token.is_empty() || token.len() > 0x00ff_ffff {
            return Err(entity_error("yield token length is invalid"));
        }
        self.transition(entity, EntityState::Yielded)?;
        self.transition(entity, EntityState::Deferred)?;
        let claim = ClaimRecord {
            claim_id,
            entity,
            expiry_timestamp_micros,
            token,
            validation,
            redeemed_at_micros: None,
        };
        self.claims.insert(claim_id, claim.clone());
        Ok(claim)
    }

    pub fn redeem_claim(
        &mut self,
        claim_id: u64,
        state_checksum: [u8; 32],
        now_micros: u64,
    ) -> Result<Vec<u8>, ProtocolError> {
        let claim = self
            .claims
            .get(&claim_id)
            .ok_or_else(|| claim_not_found("claim does not exist"))?;
        if claim.redeemed_at_micros.is_some() {
            return Err(claim_not_found("claim was already redeemed"));
        }
        if claim.expiry_timestamp_micros <= now_micros {
            return Err(claim_expired("claim has expired"));
        }
        if claim.validation.state_checksum != Some(state_checksum) {
            return Err(ProtocolError::new(
                crate::ERROR_INTEGRITY,
                "PIPESTREAM_INTEGRITY_ERROR",
                "stopping-point state checksum differs",
            ));
        }
        let entity = claim.entity;
        let token = claim.token.clone();
        self.transition(entity, EntityState::Processing)?;
        self.claims
            .get_mut(&claim_id)
            .expect("claim was checked above")
            .redeemed_at_micros = Some(now_micros);
        Ok(token)
    }

    pub fn final_lineage_digest(&self) -> Result<[u8; 32], ProtocolError> {
        if self.work_sets.as_ref().is_some_and(|work| {
            work.scopes.is_empty() || work.scopes.keys().any(|id| !self.work_scope_ready(*id))
        }) {
            return Err(entity_error("session has unresolved work-set declarations"));
        }
        if self
            .entities
            .values()
            .any(|entity| !self.entity_is_resolved(entity))
            || self
                .scopes
                .values()
                .filter(|scope| scope.scope_id != 0)
                .any(|scope| scope.digest.is_none())
            || self
                .manifests
                .values()
                .any(|manifest| manifest.state == ResolutionState::Active)
            || self
                .checkpoints
                .values()
                .any(|checkpoint| !checkpoint.acknowledged)
        {
            return Err(entity_error("session lineage is not terminal"));
        }
        let mut hasher = Sha256::new();
        if let Some(work) = &self.work_sets {
            hasher.update(b"pipestream-lineage-sealed-v1");
            hasher.update(work.producer_id);
            hasher.update((work.scopes.len() as u64).to_be_bytes());
            for (id, scope) in &work.scopes {
                hasher.update(id.to_be_bytes());
                hasher.update(scope.seal_digest.expect("work readiness checked above"));
            }
        } else {
            hasher.update(b"pipestream-lineage-v1");
        }
        hasher.update((self.session_id.len() as u64).to_be_bytes());
        hasher.update(self.session_id.as_bytes());
        for (key, entity) in &self.entities {
            hasher.update(key.scope_id.to_be_bytes());
            hasher.update(key.entity_id.to_be_bytes());
            hasher.update([entity.state.code(), entity.depth, entity.layer]);
            match entity.parent {
                Some(parent) => {
                    hasher.update([1]);
                    hasher.update(parent.scope_id.to_be_bytes());
                    hasher.update(parent.entity_id.to_be_bytes());
                }
                None => hasher.update([0]),
            }
            hasher.update(entity.payload_digest);
            match entity.output_digest {
                Some(digest) => {
                    hasher.update([1]);
                    hasher.update(digest);
                }
                None => hasher.update([0]),
            }
        }
        for (scope_id, scope) in self.scopes.iter().filter(|(id, _)| **id != 0) {
            hasher.update(scope_id.to_be_bytes());
            hasher.update(
                scope
                    .digest
                    .expect("terminal check requires every child scope digest"),
            );
        }
        Ok(hasher.finalize().into())
    }

    fn insert_entity(
        &mut self,
        key: EntityKey,
        parent: Option<EntityKey>,
        depth: u8,
        entity: NewEntity,
    ) -> Result<(), ProtocolError> {
        self.work_admission(key, parent)?;
        validate_entity_id(key.entity_id)?;
        if entity.layer > 3 {
            return Err(entity_error("entity layer exceeds 3"));
        }
        let scope = self
            .scopes
            .get_mut(&key.scope_id)
            .ok_or_else(|| scope_error("entity scope is not registered"))?;
        if scope.entities.len() >= self.max_entities_per_scope as usize {
            return Err(limit_error("scope exceeds entity limit"));
        }
        if self.entities.contains_key(&key) || !scope.entities.insert(key.entity_id) {
            return Err(entity_error("entity ID is already active in scope"));
        }
        self.entities.insert(
            key,
            EntityRecord {
                key,
                parent,
                depth,
                layer: entity.layer,
                state: EntityState::Pending,
                payload_digest: entity.payload_digest,
                output_digest: None,
                policy: entity.policy,
                retry_count: 0,
            },
        );
        Ok(())
    }

    pub(crate) fn entity_is_resolved(&self, entity: &EntityRecord) -> bool {
        if entity.state != EntityState::Failed {
            return entity.state.is_terminal();
        }
        let policy = entity.policy.as_ref();
        policy.map(CompletionPolicy::effective_on_failure) != Some(FailureAction::Retry)
            || entity.retry_count >= policy.map_or(0, CompletionPolicy::effective_max_retries)
    }

    fn resolution_state(
        &self,
        manifest: &AssemblyManifest,
    ) -> Result<ResolutionState, ProtocolError> {
        let mut complete = 0usize;
        let mut resolved = 0usize;
        for child in &manifest.children {
            let record = self
                .entities
                .get(child)
                .ok_or_else(|| entity_error("manifest child is absent"))?;
            if self.entity_is_resolved(record) {
                resolved += 1;
            }
            if record.state == EntityState::Complete {
                complete += 1;
            }
        }
        if resolved != manifest.children.len() {
            return Ok(ResolutionState::Active);
        }
        let accepted = match manifest.policy.effective_mode() {
            CompletionMode::Unspecified | CompletionMode::Strict => {
                complete == manifest.children.len()
            }
            CompletionMode::Lenient => complete > 0,
            CompletionMode::BestEffort => true,
            CompletionMode::Quorum => {
                let ratio = manifest
                    .policy
                    .min_success_ratio
                    .ok_or_else(|| entity_error("quorum policy is missing min-success-ratio"))?;
                complete as u64 >= quorum_threshold(ratio, manifest.children.len() as u32)
            }
        };
        Ok(if !accepted {
            ResolutionState::Failed
        } else if complete == manifest.children.len() {
            ResolutionState::Resolved
        } else {
            ResolutionState::Partial
        })
    }

    fn is_descendant_scope(&self, candidate: u32, ancestor: u32) -> bool {
        if candidate == ancestor {
            return true;
        }
        let mut current = candidate;
        while let Some(parent) = self.scopes.get(&current).and_then(|scope| scope.parent) {
            if parent.scope_id == ancestor {
                return true;
            }
            current = parent.scope_id;
        }
        false
    }
}

// Compute ceil(ratio * count) from the exact finite binary32 representation.
// The admitted ratio is in [0, 1], and count is bounded by the uint32 ID space.
fn quorum_threshold(ratio: f32, count: u32) -> u64 {
    let bits = ratio.to_bits() & 0x7fff_ffff;
    let exponent = bits >> 23;
    let fraction = bits & 0x7f_ffff;
    let (significand, shift) = if exponent == 0 {
        (fraction, 149)
    } else {
        (fraction | 0x80_0000, 150 - exponent)
    };
    let product = u64::from(significand) * u64::from(count);
    if shift >= 64 {
        u64::from(product != 0)
    } else {
        product.div_ceil(1u64 << shift)
    }
}

#[must_use]
pub fn is_before(left: u32, right: u32) -> bool {
    const MODULUS: u64 = 0xffff_fffd;
    if left == right || left == 0 || right == 0 || left > MAX_ENTITY_ID || right > MAX_ENTITY_ID {
        return false;
    }
    ((u64::from(right) + MODULUS - u64::from(left)) % MODULUS) < MODULUS / 2
}

pub fn merkle_root(statuses: &[(u32, EntityState)]) -> Result<[u8; 32], ProtocolError> {
    if statuses.is_empty() {
        return Err(scope_error("cannot digest an empty scope"));
    }
    let mut sorted = statuses.to_vec();
    sorted.sort_by_key(|(entity_id, _)| *entity_id);
    if sorted.windows(2).any(|pair| pair[0].0 == pair[1].0) {
        return Err(entity_error("scope digest contains duplicate entity IDs"));
    }
    let mut level: Vec<[u8; 32]> = sorted
        .into_iter()
        .map(|(entity_id, state)| {
            let mut hasher = Sha256::new();
            hasher.update([0]);
            hasher.update(entity_id.to_be_bytes());
            hasher.update([state.code() & 0x0f]);
            hasher.finalize().into()
        })
        .collect();
    while level.len() > 1 {
        let mut next = Vec::with_capacity(level.len().div_ceil(2));
        let mut index = 0;
        while index < level.len() {
            if index + 1 == level.len() {
                next.push(level[index]);
            } else {
                let mut hasher = Sha256::new();
                hasher.update([1]);
                hasher.update(level[index]);
                hasher.update(level[index + 1]);
                next.push(hasher.finalize().into());
            }
            index += 2;
        }
        level = next;
    }
    Ok(level[0])
}

pub fn validate_session_id(value: &str) -> Result<(), ProtocolError> {
    if value.is_empty()
        || value.len() > 128
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_')
    {
        return Err(entity_error("invalid session ID"));
    }
    Ok(())
}

fn validate_entity_id(value: u32) -> Result<(), ProtocolError> {
    if value == 0 || value > MAX_ENTITY_ID {
        return Err(entity_error("entity ID is reserved"));
    }
    Ok(())
}

fn validate_stopping_point(validation: &StoppingPointValidation) -> Result<(), ProtocolError> {
    if validation.state_checksum.is_none() {
        return Err(entity_error("state checksum is required for recovery"));
    }
    if validation.is_resumable != Some(true) {
        return Err(entity_error("stopping point is not resumable"));
    }
    if let (Some(complete), Some(total)) = (validation.children_complete, validation.children_total)
        && complete > total
    {
        return Err(entity_error("children-complete exceeds children-total"));
    }
    if validation
        .checkpoint_ref
        .as_ref()
        .is_some_and(|value| value.is_empty() || value.len() > 256)
    {
        return Err(entity_error("checkpoint reference is invalid"));
    }
    Ok(())
}

fn entity_error(detail: impl Into<String>) -> ProtocolError {
    ProtocolError::new(ERROR_ENTITY_INVALID, "PIPESTREAM_ENTITY_INVALID", detail)
}

fn scope_error(detail: impl Into<String>) -> ProtocolError {
    ProtocolError::new(ERROR_SCOPE_INVALID, "PIPESTREAM_SCOPE_INVALID", detail)
}

fn depth_error(detail: impl Into<String>) -> ProtocolError {
    ProtocolError::new(ERROR_DEPTH_EXCEEDED, "PIPESTREAM_DEPTH_EXCEEDED", detail)
}

fn limit_error(detail: impl Into<String>) -> ProtocolError {
    ProtocolError::new(ERROR_LIMIT_EXCEEDED, "PIPESTREAM_LIMIT_EXCEEDED", detail)
}

fn claim_expired(detail: impl Into<String>) -> ProtocolError {
    ProtocolError::new(ERROR_CLAIM_EXPIRED, "PIPESTREAM_CLAIM_EXPIRED", detail)
}

fn claim_not_found(detail: impl Into<String>) -> ProtocolError {
    ProtocolError::new(ERROR_CLAIM_NOT_FOUND, "PIPESTREAM_CLAIM_NOT_FOUND", detail)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quorum_threshold_uses_exact_integer_rounding() {
        assert_eq!(quorum_threshold(0.75, 3), 3);
        assert_eq!(quorum_threshold(0.75, 4), 3);
        assert_eq!(quorum_threshold(1.0, u32::MAX), u64::from(u32::MAX));
        assert_eq!(quorum_threshold(0.0, u32::MAX), 0);
        assert_eq!(quorum_threshold(f32::from_bits(1), u32::MAX), 1);
        assert_eq!(quorum_threshold(0.5, u32::MAX), 2_147_483_648);
    }

    fn digest(value: &[u8]) -> [u8; 32] {
        Sha256::digest(value).into()
    }

    fn entity(entity_id: u32, value: &[u8]) -> NewEntity {
        NewEntity {
            entity_id,
            layer: 0,
            payload_digest: digest(value),
            policy: None,
        }
    }

    fn resolution_session(
        mode: CompletionMode,
        min_success_ratio: Option<f32>,
        child_states: &[EntityState],
    ) -> (Session, EntityKey) {
        let mut session = Session::new("policy-1", 7, 128).unwrap();
        let root = session
            .add_root(NewEntity {
                entity_id: 1,
                layer: 0,
                payload_digest: digest(b"root"),
                policy: Some(CompletionPolicy {
                    mode: Some(mode),
                    min_success_ratio,
                    ..CompletionPolicy::default()
                }),
            })
            .unwrap();
        session.transition(root, EntityState::Processing).unwrap();
        let children = child_states
            .iter()
            .enumerate()
            .map(|(index, _)| entity(index as u32 + 1, &[index as u8]))
            .collect();
        let keys = session.dehydrate(root, 1, children, 1).unwrap();
        for (key, state) in keys.into_iter().zip(child_states.iter().copied()) {
            session.transition(key, EntityState::Processing).unwrap();
            match state {
                EntityState::Complete => session
                    .complete_entity(key, [key.entity_id as u8; 32])
                    .unwrap(),
                EntityState::Failed => session.transition(key, EntityState::Failed).unwrap(),
                other => panic!("unsupported policy fixture state {other:?}"),
            }
        }
        session.close_scope(1).unwrap();
        (session, root)
    }

    #[test]
    fn recursive_out_of_order_rehydration_produces_stable_lineage() {
        let mut session = Session::new("recursive-1", 7, 128).unwrap();
        let root = session.add_root(entity(1, b"root")).unwrap();
        session.transition(root, EntityState::Processing).unwrap();
        let children = session
            .dehydrate(
                root,
                1,
                vec![entity(1, b"a"), entity(2, b"b"), entity(3, b"c")],
                1,
            )
            .unwrap();
        for child in &children {
            session.transition(*child, EntityState::Processing).unwrap();
        }
        let grandchildren = session
            .dehydrate(children[1], 2, vec![entity(1, b"b1"), entity(2, b"b2")], 2)
            .unwrap();
        for grandchild in grandchildren.iter().rev() {
            session
                .transition(*grandchild, EntityState::Processing)
                .unwrap();
            session
                .transition(*grandchild, EntityState::Complete)
                .unwrap();
        }
        let nested_digest = session.close_scope(2).unwrap();
        assert_eq!(2, nested_digest.entities_succeeded);
        session.begin_rehydration(children[1]).unwrap();
        session
            .complete_rehydration(children[1], digest(b"b-rehydrated"))
            .unwrap();
        for child in [children[2], children[0]] {
            session.transition(child, EntityState::Complete).unwrap();
        }
        let child_digest = session.close_scope(1).unwrap();
        assert_eq!(3, child_digest.entities_succeeded);
        assert!(session.barrier(1).unwrap().released);
        session.begin_rehydration(root).unwrap();
        session
            .complete_rehydration(root, digest(b"root-rehydrated"))
            .unwrap();
        let checkpoint = Checkpoint {
            checkpoint_id: "root-finished".to_owned(),
            sequence_number: 1,
            checkpoint_entity_id: 2,
            scope_id: None,
            flags: 0,
            timeout_ms: Some(30_000),
        };
        session.request_checkpoint(&checkpoint).unwrap();
        assert!(session.checkpoint_satisfied(0, 1).unwrap());
        assert_eq!(
            CHECKPOINT_ACK,
            session.acknowledge_checkpoint(0, 1).unwrap().flags
        );
        let first = session.final_lineage_digest().unwrap();
        let encoded = postcard::to_stdvec(&session).unwrap();
        let restored: Session = postcard::from_bytes(&encoded).unwrap();
        assert_eq!(first, restored.final_lineage_digest().unwrap());
        assert_eq!(session, restored);
    }

    #[test]
    fn completion_modes_control_partial_rehydration() {
        let (mut strict, strict_root) = resolution_session(
            CompletionMode::Strict,
            None,
            &[EntityState::Complete, EntityState::Failed],
        );
        assert_eq!(
            ERROR_ENTITY_INVALID,
            strict.begin_rehydration(strict_root).unwrap_err().code
        );

        let (mut lenient, lenient_root) = resolution_session(
            CompletionMode::Lenient,
            None,
            &[EntityState::Complete, EntityState::Failed],
        );
        lenient.begin_rehydration(lenient_root).unwrap();
        assert_eq!(
            ResolutionState::Partial,
            lenient.manifests[&lenient_root].state
        );

        let (mut best_effort, best_effort_root) = resolution_session(
            CompletionMode::BestEffort,
            None,
            &[EntityState::Failed, EntityState::Failed],
        );
        best_effort.begin_rehydration(best_effort_root).unwrap();
        assert_eq!(
            ResolutionState::Partial,
            best_effort.manifests[&best_effort_root].state
        );

        let (mut quorum, quorum_root) = resolution_session(
            CompletionMode::Quorum,
            Some(0.5),
            &[EntityState::Complete, EntityState::Failed],
        );
        quorum.begin_rehydration(quorum_root).unwrap();

        let (mut missed_quorum, missed_quorum_root) = resolution_session(
            CompletionMode::Quorum,
            Some(0.75),
            &[EntityState::Complete, EntityState::Failed],
        );
        assert_eq!(
            ERROR_ENTITY_INVALID,
            missed_quorum
                .begin_rehydration(missed_quorum_root)
                .unwrap_err()
                .code
        );
    }

    #[test]
    fn failed_entity_is_unresolved_until_retry_budget_is_exhausted() {
        let mut session = Session::new("retry-policy-1", 7, 128).unwrap();
        let root = session.add_root(entity(1, b"root")).unwrap();
        session.transition(root, EntityState::Processing).unwrap();
        let child = session
            .dehydrate(
                root,
                1,
                vec![NewEntity {
                    entity_id: 1,
                    layer: 0,
                    payload_digest: digest(b"child"),
                    policy: Some(CompletionPolicy {
                        max_retries: Some(2),
                        on_failure: Some(crate::FailureAction::Retry),
                        ..CompletionPolicy::default()
                    }),
                }],
                1,
            )
            .unwrap()[0];
        session.transition(child, EntityState::Processing).unwrap();
        session.transition(child, EntityState::Failed).unwrap();
        assert_eq!(
            ERROR_SCOPE_INVALID,
            session.close_scope(1).unwrap_err().code
        );

        for attempt in 1..=2 {
            session.transition(child, EntityState::Retrying).unwrap();
            assert_eq!(attempt, session.entities[&child].retry_count);
            session.transition(child, EntityState::Processing).unwrap();
            session.transition(child, EntityState::Failed).unwrap();
        }
        session.close_scope(1).unwrap();
        assert_eq!(
            ERROR_ENTITY_INVALID,
            session
                .transition(child, EntityState::Retrying)
                .unwrap_err()
                .code
        );
    }

    #[test]
    fn retry_budget_does_not_override_a_fail_action() {
        let mut session = Session::new("no-retry-policy-1", 7, 128).unwrap();
        let entity = session
            .add_root(NewEntity {
                entity_id: 1,
                layer: 0,
                payload_digest: digest(b"root"),
                policy: Some(CompletionPolicy {
                    max_retries: Some(3),
                    on_failure: Some(FailureAction::Fail),
                    ..CompletionPolicy::default()
                }),
            })
            .unwrap();
        session.transition(entity, EntityState::Processing).unwrap();
        session.transition(entity, EntityState::Failed).unwrap();
        assert!(session.entity_is_resolved(&session.entities[&entity]));
        assert_eq!(
            ERROR_ENTITY_INVALID,
            session
                .transition(entity, EntityState::Retrying)
                .unwrap_err()
                .code
        );
    }

    #[test]
    fn claim_is_single_use_and_checksum_bound() {
        let mut session = Session::new("recover-1", 7, 128).unwrap();
        let root = session.add_root(entity(1, b"payload")).unwrap();
        session.transition(root, EntityState::Processing).unwrap();
        let state_checksum = digest(b"state-v1");
        session
            .defer_with_claim_id(
                root,
                b"opaque-continuation".to_vec(),
                StoppingPointValidation {
                    state_checksum: Some(state_checksum),
                    bytes_processed: Some(7),
                    children_complete: Some(0),
                    children_total: Some(0),
                    is_resumable: Some(true),
                    checkpoint_ref: Some("before-external-call".to_owned()),
                },
                42,
                2_000,
                1_000,
            )
            .unwrap();
        assert!(session.redeem_claim(42, digest(b"wrong"), 1_500).is_err());
        assert_eq!(
            b"opaque-continuation".as_slice(),
            session.redeem_claim(42, state_checksum, 1_500).unwrap()
        );
        let replay = session.redeem_claim(42, state_checksum, 1_600).unwrap_err();
        assert_eq!(ERROR_CLAIM_NOT_FOUND, replay.code);
    }

    #[test]
    fn expired_claim_is_refused() {
        let mut session = Session::new("recover-2", 7, 128).unwrap();
        let root = session.add_root(entity(1, b"payload")).unwrap();
        session.transition(root, EntityState::Processing).unwrap();
        let state_checksum = digest(b"state-v1");
        session
            .defer_with_claim_id(
                root,
                vec![1],
                StoppingPointValidation {
                    state_checksum: Some(state_checksum),
                    bytes_processed: None,
                    children_complete: None,
                    children_total: None,
                    is_resumable: Some(true),
                    checkpoint_ref: None,
                },
                43,
                2_000,
                1_000,
            )
            .unwrap();
        let error = session.redeem_claim(43, state_checksum, 2_000).unwrap_err();
        assert_eq!(ERROR_CLAIM_EXPIRED, error.code);
    }

    #[test]
    fn merkle_digest_is_order_independent() {
        let forward = merkle_root(&[
            (1, EntityState::Complete),
            (2, EntityState::Failed),
            (3, EntityState::Skipped),
        ])
        .unwrap();
        let reverse = merkle_root(&[
            (3, EntityState::Skipped),
            (1, EntityState::Complete),
            (2, EntityState::Failed),
        ])
        .unwrap();
        assert_eq!(forward, reverse);
    }
}
