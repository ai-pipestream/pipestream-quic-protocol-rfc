use super::*;
use crate::{cbor_decode, cbor_encode, deterministic, encode_ucf};
use minicbor::{Decoder, Encoder};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecoveryFrame {
    Request(RecoveryRequest),
    Receipt(RecoveryReceipt),
    Outcome {
        receipt: RecoveryReceipt,
        outcome: RecoveryOutcome,
    },
}

pub fn encode(frame: &RecoveryFrame) -> Result<Vec<u8>, ProtocolError> {
    let (request, acceptance, outcome) = match frame {
        RecoveryFrame::Request(request) => {
            request.validate()?;
            (request, None, None)
        }
        RecoveryFrame::Receipt(receipt) => {
            receipt.validate()?;
            (&receipt.request, Some(&receipt.acceptance), None)
        }
        RecoveryFrame::Outcome { receipt, outcome } => {
            receipt.validate()?;
            outcome.validate()?;
            (&receipt.request, Some(&receipt.acceptance), Some(outcome))
        }
    };
    let failure = match outcome {
        Some(RecoveryOutcome::Refused(failure)) => Some(failure),
        _ => None,
    };
    let mut body = Vec::new();
    let mut e = Encoder::new(&mut body);
    e.map(if outcome.is_some() {
        11 + u64::from(failure.is_some()) * 2
    } else if acceptance.is_some() {
        10
    } else {
        6
    })
    .map_err(cbor_encode)?;
    e.str("flags")
        .map_err(cbor_encode)?
        .u8(if outcome.is_some() {
            2
        } else {
            u8::from(acceptance.is_some())
        })
        .map_err(cbor_encode)?;
    if outcome.is_some() {
        e.str("outcome")
            .map_err(cbor_encode)?
            .u8(u8::from(failure.is_some()))
            .map_err(cbor_encode)?;
    }
    e.str("claim-id")
        .map_err(cbor_encode)?
        .u64(request.claim_id)
        .map_err(cbor_encode)?;
    if let Some(acceptance) = acceptance {
        e.str("scope-id")
            .map_err(cbor_encode)?
            .u32(acceptance.entity.scope_id)
            .map_err(cbor_encode)?;
    }
    e.str("authority")
        .map_err(cbor_encode)?
        .str(&request.authority)
        .map_err(cbor_encode)?;
    if let Some(acceptance) = acceptance {
        e.str("entity-id")
            .map_err(cbor_encode)?
            .u32(acceptance.entity.entity_id)
            .map_err(cbor_encode)?;
    }
    e.str("request-id")
        .map_err(cbor_encode)?
        .bytes(&request.request_id)
        .map_err(cbor_encode)?;
    e.str("session-id")
        .map_err(cbor_encode)?
        .str(&request.session_id)
        .map_err(cbor_encode)?;
    if let Some(acceptance) = acceptance {
        e.str("accepted-at")
            .map_err(cbor_encode)?
            .u64(acceptance.accepted_at_micros)
            .map_err(cbor_encode)?;
        if let Some(failure) = failure {
            e.str("failure-code")
                .map_err(cbor_encode)?
                .u32(failure.code)
                .map_err(cbor_encode)?;
        }
        e.str("retain-until")
            .map_err(cbor_encode)?
            .u64(acceptance.retain_until_micros)
            .map_err(cbor_encode)?;
    }
    if let Some(failure) = failure {
        e.str("failure-detail")
            .map_err(cbor_encode)?
            .str(&failure.detail)
            .map_err(cbor_encode)?;
    }
    e.str("state-checksum")
        .map_err(cbor_encode)?
        .bytes(&request.state_checksum)
        .map_err(cbor_encode)?;
    encode_ucf(FRAME_RECOVERY, &body)
}

pub fn decode(bytes: &[u8]) -> Result<RecoveryFrame, ProtocolError> {
    if bytes.len() > 1_024 {
        return Err(ProtocolError::limit("recovery frame exceeds limit"));
    }
    deterministic::validate(bytes)?;
    let mut d = Decoder::new(bytes);
    let count = d
        .map()
        .map_err(cbor_decode)?
        .ok_or_else(|| ProtocolError::frame("indefinite recovery frame"))?;
    let (mut flags, mut claim, mut scope, mut authority, mut entity) =
        (None, None, None, None, None);
    let (mut request, mut session, mut accepted, mut retain, mut checksum) =
        (None, None, None, None, None);
    let (mut outcome, mut failure_code, mut failure_detail) = (None, None, None);
    for _ in 0..count {
        match d.str().map_err(cbor_decode)? {
            "flags" => flags = Some(d.u8().map_err(cbor_decode)?),
            "outcome" => outcome = Some(d.u8().map_err(cbor_decode)?),
            "failure-code" => failure_code = Some(d.u32().map_err(cbor_decode)?),
            "failure-detail" => failure_detail = Some(d.str().map_err(cbor_decode)?.to_owned()),
            "claim-id" => claim = Some(d.u64().map_err(cbor_decode)?),
            "scope-id" => scope = Some(d.u32().map_err(cbor_decode)?),
            "authority" => authority = Some(d.str().map_err(cbor_decode)?.to_owned()),
            "entity-id" => entity = Some(d.u32().map_err(cbor_decode)?),
            "request-id" => {
                request = Some(
                    d.bytes()
                        .map_err(cbor_decode)?
                        .try_into()
                        .map_err(|_| ProtocolError::frame("request-id must be 16 octets"))?,
                )
            }
            "session-id" => session = Some(d.str().map_err(cbor_decode)?.to_owned()),
            "accepted-at" => accepted = Some(d.u64().map_err(cbor_decode)?),
            "retain-until" => retain = Some(d.u64().map_err(cbor_decode)?),
            "state-checksum" => {
                checksum = Some(
                    d.bytes()
                        .map_err(cbor_decode)?
                        .try_into()
                        .map_err(|_| ProtocolError::frame("state-checksum must be 32 octets"))?,
                )
            }
            _ => return Err(ProtocolError::frame("unknown recovery field")),
        }
    }
    if d.position() != bytes.len() {
        return Err(ProtocolError::frame("trailing recovery bytes"));
    }
    let missing = || ProtocolError::frame("missing recovery field");
    let request = RecoveryRequest {
        authority: authority.ok_or_else(missing)?,
        session_id: session.ok_or_else(missing)?,
        request_id: request.ok_or_else(missing)?,
        claim_id: claim.ok_or_else(missing)?,
        state_checksum: checksum.ok_or_else(missing)?,
    };
    request.validate()?;
    let flags = flags.ok_or_else(missing)?;
    if flags != 2 && (outcome.is_some() || failure_code.is_some() || failure_detail.is_some()) {
        return Err(ProtocolError::frame(
            "non-outcome recovery frame contains outcome fields",
        ));
    }
    match flags {
        0 if scope.is_none() && entity.is_none() && accepted.is_none() && retain.is_none() => {
            Ok(RecoveryFrame::Request(request))
        }
        1 | 2 => {
            let receipt = RecoveryReceipt {
                request,
                acceptance: RecoveryAcceptance {
                    entity: EntityKey {
                        scope_id: scope.ok_or_else(missing)?,
                        entity_id: entity.ok_or_else(missing)?,
                    },
                    accepted_at_micros: accepted.ok_or_else(missing)?,
                    retain_until_micros: retain.ok_or_else(missing)?,
                },
            };
            receipt.validate()?;
            if flags == 1 {
                return Ok(RecoveryFrame::Receipt(receipt));
            }
            let outcome = match outcome.ok_or_else(missing)? {
                0 if failure_code.is_none() && failure_detail.is_none() => {
                    RecoveryOutcome::Complete
                }
                1 => RecoveryOutcome::Refused(crate::jobs::JobFailure {
                    code: failure_code.ok_or_else(missing)?,
                    detail: failure_detail.ok_or_else(missing)?,
                }),
                _ => return Err(ProtocolError::frame("invalid recovery outcome")),
            };
            outcome.validate()?;
            Ok(RecoveryFrame::Outcome { receipt, outcome })
        }
        _ => Err(ProtocolError::frame(
            "invalid recovery flags or receipt fields",
        )),
    }
}
