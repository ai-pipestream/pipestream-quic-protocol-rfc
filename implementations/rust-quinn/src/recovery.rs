//! Authenticated, retained acceptance of a single claim recovery request.
//! Receipts acknowledge admission, not successful application execution.

use crate::{
    ProtocolError,
    authorization::{PrincipalBinding, unauthorized},
    execution::{ExecutionKey, ExecutionStage},
    jobs::JobInput,
    session::{EntityKey, Session, validate_session_id},
};
use serde::{Deserialize, Serialize};

pub const EXTENSION_AUTHENTICATED_RECOVERY: u16 = 0xff03;
pub const FRAME_RECOVERY: u8 = 0x84;
pub const RECEIPT_RETENTION_MICROS: u64 = 86_400_000_000;
pub const MAX_RECOVERY_RECEIPTS: usize = 1_024;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecoveryRequest {
    pub authority: String,
    pub session_id: String,
    pub request_id: [u8; 16],
    pub claim_id: u64,
    pub state_checksum: [u8; 32],
}

impl RecoveryRequest {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        validate_session_id(&self.session_id)?;
        if self.authority.is_empty()
            || self.authority.len() > 128
            || !self
                .authority
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"-._~".contains(&b))
            || self.request_id == [0; 16]
            || self.claim_id == 0
        {
            return Err(ProtocolError::frame("invalid recovery request identity"));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecoveryAcceptance {
    pub entity: EntityKey,
    pub accepted_at_micros: u64,
    pub retain_until_micros: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecoveryReceipt {
    pub request: RecoveryRequest,
    pub acceptance: RecoveryAcceptance,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecoveryOutcome {
    Complete,
    Refused(crate::jobs::JobFailure),
}

impl RecoveryOutcome {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        if matches!(self, Self::Refused(failure) if failure.detail.len() > 512) {
            return Err(ProtocolError::frame(
                "recovery refusal exceeds detail limit",
            ));
        }
        Ok(())
    }
}

impl RecoveryReceipt {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        self.request.validate()?;
        if !(1..=crate::MAX_ENTITY_ID).contains(&self.acceptance.entity.entity_id)
            || self
                .acceptance
                .accepted_at_micros
                .checked_add(RECEIPT_RETENTION_MICROS)
                != Some(self.acceptance.retain_until_micros)
        {
            return Err(ProtocolError::frame("invalid recovery receipt"));
        }
        Ok(())
    }

    pub fn execution_key(&self) -> ExecutionKey {
        ExecutionKey {
            entity: self.acceptance.entity,
            stage: ExecutionStage::Resume {
                claim_id: self.request.claim_id,
            },
        }
    }
}

impl Session {
    /// Read a retained receipt only after checking current owner, revocation and retention.
    pub fn recovery_receipt(
        &self,
        caller: Option<&PrincipalBinding>,
        request: &RecoveryRequest,
        now: u64,
    ) -> Result<Option<&RecoveryReceipt>, ProtocolError> {
        self.authorize(caller)?;
        let caller = caller.ok_or_else(unauthorized)?;
        if request.authority != caller.authority || request.session_id != self.session_id {
            return Err(unauthorized());
        }
        request.validate()?;
        if self.work_sets.is_some() {
            return Err(ProtocolError::new(
                crate::ERROR_EXTENSION_UNSUPPORTED,
                "PIPESTREAM_EXTENSION_UNSUPPORTED",
                "recovery is outside the sealed-work profile",
            ));
        }
        if let Some(receipt) = self.recovery_receipts.get(&request.request_id) {
            if &receipt.request != request {
                return Err(ProtocolError::entity(
                    "recovery request identity was reused",
                ));
            }
            self.authorize_claim(request.claim_id)?;
            if now < receipt.acceptance.accepted_at_micros {
                return Err(ProtocolError::entity("recovery clock precedes acceptance"));
            }
            if now >= receipt.acceptance.retain_until_micros {
                return Err(ProtocolError::new(
                    crate::ERROR_CLAIM_EXPIRED,
                    "PIPESTREAM_CLAIM_EXPIRED",
                    "recovery receipt retention expired",
                ));
            }
            return Ok(Some(receipt));
        }
        Ok(None)
    }

    /// Invoke through a session-store transaction so receipt, redemption and queue admission
    /// commit together. An identical replay returns the receipt without enqueueing again.
    pub fn accept_recovery(
        &mut self,
        caller: Option<&PrincipalBinding>,
        request: &RecoveryRequest,
        now: u64,
    ) -> Result<RecoveryReceipt, ProtocolError> {
        if let Some(receipt) = self.recovery_receipt(caller, request, now)? {
            return Ok(receipt.clone());
        }
        self.authorize_claim(request.claim_id)?;
        if self.recovery_receipts.len() >= MAX_RECOVERY_RECEIPTS {
            return Err(ProtocolError::limit(
                "session recovery receipt limit exhausted",
            ));
        }
        let retain_until_micros = now
            .checked_add(RECEIPT_RETENTION_MICROS)
            .filter(|time| *time <= i64::MAX as u64)
            .ok_or_else(|| ProtocolError::limit("recovery retention clock overflow"))?;
        let claim = self.claims.get(&request.claim_id).ok_or_else(|| {
            ProtocolError::new(
                crate::ERROR_CLAIM_NOT_FOUND,
                "PIPESTREAM_CLAIM_NOT_FOUND",
                "claim does not exist",
            )
        })?;
        let receipt = RecoveryReceipt {
            request: request.clone(),
            acceptance: RecoveryAcceptance {
                entity: claim.entity,
                accepted_at_micros: now,
                retain_until_micros,
            },
        };
        let key = receipt.execution_key();
        self.redeem_claim(request.claim_id, request.state_checksum, now)?;
        self.enqueue_job(
            key,
            JobInput::Resume {
                claim_id: request.claim_id,
            },
            now,
        )?;
        self.recovery_receipts
            .insert(request.request_id, receipt.clone());
        Ok(receipt)
    }

    pub(crate) fn authorize_claim(&self, claim_id: u64) -> Result<(), ProtocolError> {
        if self.revoked_claims.contains(&claim_id) {
            return Err(unauthorized());
        }
        Ok(())
    }

    /// Operator action. Revocation is durable, irreversible, and does not erase work.
    pub fn revoke_claim(&mut self, claim_id: u64) -> Result<(), ProtocolError> {
        if !self.claims.contains_key(&claim_id) {
            return Err(ProtocolError::new(
                crate::ERROR_CLAIM_NOT_FOUND,
                "PIPESTREAM_CLAIM_NOT_FOUND",
                "claim does not exist",
            ));
        }
        self.revoked_claims.insert(claim_id);
        Ok(())
    }

    pub(crate) fn validate_recovery(&self) -> Result<(), ProtocolError> {
        if self.recovery_receipts.len() > MAX_RECOVERY_RECEIPTS
            || self
                .revoked_claims
                .iter()
                .any(|id| !self.claims.contains_key(id))
        {
            return Err(ProtocolError::entity("invalid retained recovery state"));
        }
        let mut claims = std::collections::BTreeSet::new();
        for (id, receipt) in &self.recovery_receipts {
            receipt.validate()?;
            if id != &receipt.request.request_id
                || receipt.request.session_id != self.session_id
                || self.work_sets.is_some()
                || !claims.insert(receipt.request.claim_id)
                || self
                    .owner
                    .as_ref()
                    .is_none_or(|owner| owner.binding.authority != receipt.request.authority)
                || !self
                    .claims
                    .get(&receipt.request.claim_id)
                    .is_some_and(|claim| {
                        claim.entity == receipt.acceptance.entity
                            && claim.redeemed_at_micros
                                == Some(receipt.acceptance.accepted_at_micros)
                            && claim.expiry_timestamp_micros > receipt.acceptance.accepted_at_micros
                            && claim.validation.state_checksum
                                == Some(receipt.request.state_checksum)
                    })
                || !self.jobs.get(&receipt.execution_key()).is_some_and(|job| {
                    job.input
                        == (JobInput::Resume {
                            claim_id: receipt.request.claim_id,
                        })
                        && job.enqueued_at_micros == receipt.acceptance.accepted_at_micros
                        && match &job.state {
                            crate::jobs::JobState::Finished(output) => {
                                output == &crate::jobs::JobOutput::Resumed
                                    && self.entities.get(&receipt.acceptance.entity).is_some_and(
                                        |entity| {
                                            entity.state == crate::session::EntityState::Complete
                                                && entity.output_digest.is_some()
                                        },
                                    )
                            }
                            _ => true,
                        }
                })
            {
                return Err(ProtocolError::entity(
                    "recovery receipt differs from durable admission",
                ));
            }
        }
        Ok(())
    }

    pub(crate) fn validate_retained_recovery(&self, previous: &Self) -> Result<(), ProtocolError> {
        if previous
            .recovery_receipts
            .iter()
            .any(|(id, receipt)| self.recovery_receipts.get(id) != Some(receipt))
            || !previous.revoked_claims.is_subset(&self.revoked_claims)
            || previous.owner.as_ref().is_some_and(|owner| {
                self.owner.as_ref().is_none_or(|next| {
                    next.binding != owner.binding || (owner.revoked && !next.revoked)
                })
            })
        {
            return Err(ProtocolError::entity(
                "retained recovery identity, outcome or revocation changed",
            ));
        }
        Ok(())
    }
}

mod wire;
pub use wire::{RecoveryFrame, decode, encode};

#[cfg(test)]
mod tests;
