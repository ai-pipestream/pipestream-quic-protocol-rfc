//! Logical growth reserved for the result of each already-admitted job.
//! Processing also protects its possible rehydration job and scope-close digest.
//! New descendants, receipts, payloads and SQLite pages have separate admission.

use super::*;
use crate::{
    ScopeDigest, StoppingPointValidation,
    execution::ExecutionRecord,
    jobs::{JobFailure, JobInput, JobOutput, JobRecord, JobState, ProcessOutcome},
    session::{ClaimRecord, EntityKey},
};

fn size<T: serde::Serialize>(value: &T) -> Result<usize, StoreError> {
    postcard::serialize_with_flavor(value, postcard::ser_flavors::Size::default())
        .map_err(|error| StoreError::Codec(error.to_string()))
}

fn maximum_attempt() -> ExecutionRecord {
    ExecutionRecord {
        epoch: u64::MAX,
        executor: [u8::MAX; 16],
        acquired_at_micros: u64::MAX,
        expires_at_micros: u64::MAX,
        completed_at_micros: Some(u64::MAX),
    }
}

fn maximum_outcome_bytes() -> Result<usize, StoreError> {
    let outcomes = [
        JobState::Refused(JobFailure {
            code: u32::MAX,
            detail: "x".repeat(512),
        }),
        JobState::Finished(JobOutput::Processed(ProcessOutcome::Deferred {
            reason: 5,
            claim_id: u64::MAX,
        })),
        JobState::Finished(JobOutput::Rehydrated(maximum_scope_digest())),
        JobState::Finished(JobOutput::Resumed),
    ];
    outcomes
        .iter()
        .map(size)
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .max()
        .ok_or_else(|| StoreError::Corrupt("no outcome bound".into()))
}

fn maximum_scope_digest() -> ScopeDigest {
    ScopeDigest {
        scope_id: u32::MAX,
        entities_processed: u64::MAX,
        entities_succeeded: u64::MAX,
        entities_failed: u64::MAX,
        entities_deferred: u64::MAX,
        merkle_root: [u8::MAX; 32],
    }
}

// Serialize the real fixed fields, then account for the token's raw u8 elements
// and its Postcard sequence-length prefix. No token-sized allocation is needed.
fn maximum_claim_bytes(entity: EntityKey, token_bytes: usize) -> Result<usize, StoreError> {
    let claim = ClaimRecord {
        claim_id: u64::MAX,
        entity,
        expiry_timestamp_micros: u64::MAX,
        token: Vec::new(),
        validation: StoppingPointValidation {
            state_checksum: Some([u8::MAX; 32]),
            bytes_processed: Some(u64::MAX),
            children_complete: Some(u64::MAX),
            children_total: Some(u64::MAX),
            is_resumable: Some(true),
            checkpoint_ref: Some("x".repeat(256)),
        },
        redeemed_at_micros: None,
    };
    Ok(size(&(u64::MAX, claim))? + token_bytes + size(&token_bytes)? - size(&0usize)?)
}

pub(in crate::persistence) fn reserved_bytes(
    session: &Session,
    limits: StorageLimits,
) -> Result<usize, StoreError> {
    // The policy also covers direct SessionStore callers, not only QUIC callbacks.
    for claim in session.claims.values() {
        if claim.token.len() > limits.yield_token_bytes
            || claim
                .validation
                .checkpoint_ref
                .as_ref()
                .is_some_and(|value| value.len() > 256)
        {
            return Err(limit("retained continuation exceeds publication policy"));
        }
    }
    if !session.jobs.values().any(|job| job.state.is_unfinished())
        && session.future_rehydrations().next().is_none()
    {
        return Ok(0);
    }
    let outcome = maximum_outcome_bytes()?;
    let attempt = maximum_attempt();
    let mut bytes = 0usize;
    let mut new_attempts = 0usize;
    let mut new_claims = 0usize;
    let mut new_jobs = 0usize;
    for (key, job) in &session.jobs {
        if !job.state.is_unfinished() {
            continue;
        }
        let mut growth = outcome - size(&job.state)?;
        growth += size(&(key, &attempt))?;
        if let Some(existing) = session.executions.get(key) {
            growth -= size(&(key, existing))?;
        } else {
            new_attempts += 1;
        }
        let entity = session
            .entities
            .get(&key.entity)
            .ok_or_else(|| StoreError::Corrupt("reserved job entity is missing".into()))?;
        growth += size(&Some([u8::MAX; 32]))? - size(&entity.output_digest)?;
        if matches!(&job.input, JobInput::Process { layers, .. } if layers.layer2_resilience) {
            growth += maximum_claim_bytes(key.entity, limits.yield_token_bytes)?;
            new_claims += 1;
        }
        bytes = bytes
            .checked_add(growth)
            .ok_or_else(|| limit("completion reservation overflow"))?;
    }
    let future = JobRecord {
        input: JobInput::Rehydrate {
            digest: maximum_scope_digest(),
        },
        state: JobState::Queued,
        enqueued_at_micros: u64::MAX,
    };
    for (key, _) in session.future_rehydrations() {
        let entity = session
            .entities
            .get(&key.entity)
            .ok_or_else(|| StoreError::Corrupt("reserved parent is missing".into()))?;
        let closed_digest = session
            .manifests
            .get(&key.entity)
            .and_then(|manifest| session.scopes.get(&manifest.child_scope_id))
            .and_then(|scope| scope.digest);
        let growth = size(&(key, &future))? + outcome - size(&future.state)?
            + size(&(key, &attempt))?
            + size(&Some([u8::MAX; 32]))?
            - size(&entity.output_digest)?
            + size(&Some([u8::MAX; 32]))?
            - size(&closed_digest)?;
        bytes = bytes
            .checked_add(growth)
            .ok_or_else(|| limit("rehydration reservation overflow"))?;
        new_jobs += 1;
        new_attempts += 1;
    }
    // A map's length is encoded once, separately from its key/value entries.
    // Reserve the exact prefix growth for all outstanding insertions together.
    bytes += size(&(session.executions.len() + new_attempts))? - size(&session.executions.len())?;
    bytes += size(&(session.claims.len() + new_claims))? - size(&session.claims.len())?;
    bytes += size(&(session.jobs.len() + new_jobs))? - size(&session.jobs.len())?;
    Ok(bytes)
}

#[cfg(test)]
mod tests;
