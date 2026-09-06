//! Durable inputs and outcomes for bounded application dispatch.

use crate::{
    EntityHeader, LayerSupport, ProtocolError, ScopeDigest,
    authorization::PrincipalBinding,
    execution::{ExecutionKey, ExecutionLease, ExecutionStage},
    session::{EntityState, Session},
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum JobInput {
    Process {
        header: EntityHeader,
        length: u64,
        digest: [u8; 32],
        layers: LayerSupport,
    },
    Rehydrate {
        digest: ScopeDigest,
    },
    Resume {
        claim_id: u64,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProcessOutcome {
    Complete,
    Dehydrate,
    Failed,
    Deferred { reason: u8, claim_id: u64 },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum JobOutput {
    Processed(ProcessOutcome),
    Rehydrated(ScopeDigest),
    Resumed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobFailure {
    pub code: u32,
    pub detail: String,
}

impl JobFailure {
    pub fn new(error: &ProtocolError) -> Self {
        let mut length = error.detail.len().min(512);
        while !error.detail.is_char_boundary(length) {
            length -= 1;
        }
        Self {
            code: error.code,
            detail: error.detail[..length].to_owned(),
        }
    }

    pub fn protocol_error(&self) -> ProtocolError {
        let name = match self.code {
            crate::ERROR_INTEGRITY => "PIPESTREAM_INTEGRITY_ERROR",
            crate::ERROR_ENTITY_INVALID => "PIPESTREAM_ENTITY_INVALID",
            crate::ERROR_LIMIT_EXCEEDED => "PIPESTREAM_LIMIT_EXCEEDED",
            crate::ERROR_WINDOW_EXCEEDED => "PIPESTREAM_WINDOW_EXCEEDED",
            crate::ERROR_LAYER_UNSUPPORTED => "PIPESTREAM_LAYER_UNSUPPORTED",
            crate::ERROR_FRAME => "PIPESTREAM_FRAME_ERROR",
            crate::ERROR_EXTENSION_UNSUPPORTED => "PIPESTREAM_EXTENSION_UNSUPPORTED",
            crate::ERROR_DEPTH_EXCEEDED => "PIPESTREAM_DEPTH_EXCEEDED",
            crate::ERROR_SCOPE_INVALID => "PIPESTREAM_SCOPE_INVALID",
            crate::ERROR_CLAIM_EXPIRED => "PIPESTREAM_CLAIM_EXPIRED",
            crate::ERROR_CLAIM_NOT_FOUND => "PIPESTREAM_CLAIM_NOT_FOUND",
            crate::ERROR_UNAUTHORIZED => "PIPESTREAM_UNAUTHORIZED",
            _ => return ProtocolError::frame("unsupported application refusal code"),
        };
        ProtocolError::new(self.code, name, &self.detail)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum JobState {
    Queued,
    Running,
    Finished(JobOutput),
    Refused(JobFailure),
}

impl JobState {
    pub fn is_unfinished(&self) -> bool {
        matches!(self, Self::Queued | Self::Running)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JobRecord {
    pub input: JobInput,
    pub state: JobState,
    pub enqueued_at_micros: u64,
}

impl Session {
    /// Admitted processing whose possible rehydration has not become an actual job.
    /// Waiting parents retain this credit independently of ordinary queue slots.
    pub(crate) fn future_rehydrations(&self) -> impl Iterator<Item = (ExecutionKey, &JobRecord)> {
        self.jobs.iter().filter_map(|(key, job)| {
            let rehydrate = ExecutionKey {
                stage: ExecutionStage::Rehydrate,
                ..*key
            };
            (matches!(job.input, JobInput::Process { .. })
                && !self.jobs.contains_key(&rehydrate)
                && (job.state.is_unfinished()
                    || (job.state
                        == JobState::Finished(JobOutput::Processed(ProcessOutcome::Dehydrate))
                        && self.entities.get(&key.entity).is_some_and(|entity| {
                            matches!(
                                entity.state,
                                EntityState::Dehydrating | EntityState::Rehydrating
                            )
                        }))))
            .then_some((rehydrate, job))
        })
    }

    /// Invoke in the same transaction as admission, scope closure, or claim redemption.
    pub fn enqueue_job(
        &mut self,
        key: ExecutionKey,
        input: JobInput,
        now: u64,
    ) -> Result<(), ProtocolError> {
        if let Some(existing) = self.jobs.get(&key) {
            return if existing.input == input {
                Ok(())
            } else {
                Err(ProtocolError::entity("durable job input changed"))
            };
        }
        self.validate_execution_state(key)?;
        self.validate_job_input(key, &input)?;
        if self.executions.contains_key(&key) {
            return Err(ProtocolError::entity(
                "cannot attach a job to an existing attempt",
            ));
        }
        self.jobs.insert(
            key,
            JobRecord {
                input,
                state: JobState::Queued,
                enqueued_at_micros: now,
            },
        );
        Ok(())
    }

    fn validate_job_input(&self, key: ExecutionKey, input: &JobInput) -> Result<(), ProtocolError> {
        let entity = self
            .entities
            .get(&key.entity)
            .ok_or_else(|| ProtocolError::entity("job entity is not admitted"))?;
        let valid = match (&input, key.stage) {
            (
                JobInput::Process {
                    header,
                    length,
                    digest,
                    layers,
                },
                ExecutionStage::Process,
            ) => {
                crate::encode_entity_header_for(header, *layers)?;
                crate::validate_entity_payload(header, *length, *digest)?;
                header.entity_id == key.entity.entity_id
                    && header.scope_id.unwrap_or(0) == key.entity.scope_id
                    && entity.payload_digest == *digest
                    && entity.layer == header.layer
                    && entity.policy == header.completion_policy
                    && entity.parent.map(|parent| parent.entity_id) == header.parent_id
                    && entity.parent.map(|parent| parent.scope_id) == header.parent_scope_id
                    && header.chunk_info.is_none()
                    && (!layers.layer2_resilience || self.work_sets.is_none())
            }
            (JobInput::Rehydrate { digest }, ExecutionStage::Rehydrate) => {
                self.scopes.get(&digest.scope_id).is_some_and(|scope| {
                    scope.parent == Some(key.entity) && scope.digest == Some(digest.merkle_root)
                }) && self.scope_digest(digest.scope_id)? == *digest
            }
            (JobInput::Resume { claim_id }, ExecutionStage::Resume { claim_id: expected }) => {
                *claim_id == expected
                    && self.claims.get(claim_id).is_some_and(|claim| {
                        claim.entity == key.entity && claim.redeemed_at_micros.is_some()
                    })
                    && self.work_sets.is_none()
            }
            _ => false,
        };
        if !valid {
            return Err(ProtocolError::entity(
                "job input does not match its execution identity",
            ));
        }
        Ok(())
    }

    /// Acquire a queued job or an expired attempt. Call inside a store transaction.
    pub fn acquire_job(
        &mut self,
        caller: Option<&PrincipalBinding>,
        key: ExecutionKey,
        now: u64,
        lease_micros: u64,
    ) -> Result<Option<ExecutionLease>, ProtocolError> {
        self.authorize(caller)?;
        let job = self
            .jobs
            .get(&key)
            .ok_or_else(|| ProtocolError::entity("job is not queued"))?;
        if !job.state.is_unfinished() || now < job.enqueued_at_micros {
            return Ok(None);
        }
        self.validate_job_input(key, &job.input)?;
        let lease = self.acquire_execution(caller, key, now, lease_micros)?;
        if lease.is_some() {
            self.jobs.get_mut(&key).expect("job was checked").state = JobState::Running;
        }
        Ok(lease)
    }

    /// Apply a computed result and retain its outcome under the same execution fence.
    /// The caller must use a store transaction to roll back a rejected result.
    pub fn publish_job(
        &mut self,
        caller: Option<&PrincipalBinding>,
        lease: &ExecutionLease,
        now: u64,
        apply: impl FnOnce(&mut Self) -> Result<JobOutput, ProtocolError>,
    ) -> Result<JobOutput, ProtocolError> {
        self.publish_execution(caller, lease, now, |session| {
            let key = lease.key();
            if !session
                .jobs
                .get(&key)
                .is_some_and(|job| job.state == JobState::Running)
            {
                return Err(ProtocolError::entity("job is not running"));
            }
            let output = apply(session)?;
            session.validate_job_output(key, &output)?;
            session.jobs.get_mut(&key).expect("job was checked").state =
                JobState::Finished(output.clone());
            Ok(output)
        })
    }

    /// Retain an application refusal without marking the entity complete or removing work.
    pub fn refuse_job(
        &mut self,
        caller: Option<&PrincipalBinding>,
        lease: &ExecutionLease,
        now: u64,
        error: &ProtocolError,
    ) -> Result<(), ProtocolError> {
        self.publish_execution(caller, lease, now, |session| {
            let job = session
                .jobs
                .get_mut(&lease.key())
                .ok_or_else(|| ProtocolError::entity("job is absent"))?;
            if job.state != JobState::Running {
                return Err(ProtocolError::entity("job is not running"));
            }
            job.state = JobState::Refused(JobFailure::new(error));
            Ok(())
        })
    }

    fn validate_job_output(
        &self,
        key: ExecutionKey,
        output: &JobOutput,
    ) -> Result<(), ProtocolError> {
        let entity = &self.entities[&key.entity];
        let valid = match (key.stage, output) {
            (ExecutionStage::Process, JobOutput::Processed(ProcessOutcome::Complete))
            | (ExecutionStage::Resume { .. }, JobOutput::Resumed) => {
                entity.state == EntityState::Complete && entity.output_digest.is_some()
            }
            (ExecutionStage::Process, JobOutput::Processed(ProcessOutcome::Dehydrate)) => {
                entity.state == EntityState::Dehydrating
            }
            (ExecutionStage::Process, JobOutput::Processed(ProcessOutcome::Failed)) => {
                entity.state == EntityState::Failed
            }
            (
                ExecutionStage::Process,
                JobOutput::Processed(ProcessOutcome::Deferred { reason, claim_id }),
            ) => {
                entity.state == EntityState::Deferred
                    && (1..=5).contains(reason)
                    && matches!(&self.jobs[&key].input, JobInput::Process { layers, .. } if layers.layer2_resilience)
                    && self.claims.get(claim_id).is_some_and(|claim| {
                        claim.entity == key.entity && claim.redeemed_at_micros.is_none()
                    })
            }
            (ExecutionStage::Rehydrate, JobOutput::Rehydrated(digest)) => {
                entity.state == EntityState::Complete
                    && entity.output_digest.is_some()
                    && matches!(&self.jobs[&key].input, JobInput::Rehydrate { digest: expected } if digest == expected)
            }
            _ => false,
        };
        if !valid {
            return Err(ProtocolError::entity(
                "job output differs from committed protocol state",
            ));
        }
        Ok(())
    }

    pub(crate) fn validate_jobs(&self) -> Result<(), ProtocolError> {
        for (key, job) in &self.jobs {
            if matches!(&job.state, JobState::Refused(failure) if failure.detail.len() > 512) {
                return Err(ProtocolError::entity(
                    "retained refusal exceeds detail limit",
                ));
            }
            let attempt = self.executions.get(key);
            let valid = match &job.state {
                JobState::Queued => attempt.is_none(),
                JobState::Running => attempt.is_some_and(|a| a.completed_at_micros.is_none()),
                JobState::Finished(_) | JobState::Refused(_) => {
                    attempt.is_some_and(|a| a.completed_at_micros.is_some())
                }
            };
            if !valid {
                return Err(ProtocolError::entity(
                    "job state differs from its execution attempt",
                ));
            }
            if job.state.is_unfinished() {
                self.validate_job_input(*key, &job.input)?;
                self.validate_execution_state(*key)?;
            }
        }
        Ok(())
    }

    pub(crate) fn validate_retained_jobs(&self, previous: &Self) -> Result<(), ProtocolError> {
        for (key, before) in &previous.jobs {
            let after = self
                .jobs
                .get(key)
                .ok_or_else(|| ProtocolError::entity("retained job was removed"))?;
            if before.input != after.input
                || before.enqueued_at_micros != after.enqueued_at_micros
                || (!before.state.is_unfinished() && before.state != after.state)
            {
                return Err(ProtocolError::entity(
                    "retained job input or outcome changed",
                ));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
pub(crate) mod tests;
