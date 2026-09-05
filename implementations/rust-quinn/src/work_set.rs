//! Client-produced, sealed work sets. Producer identifiers are labels, not credentials.

use crate::{
    MAX_ENTITY_ID, ProtocolError, cbor_decode, cbor_encode, deterministic, encode_ucf,
    session::{EntityKey, Session, validate_session_id},
};
use minicbor::{Decoder, Encoder};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};

#[cfg(test)]
mod tests;

pub const EXTENSION_SEALED_WORK_SETS: u16 = 0xff01;
pub const FRAME_WORK_SET: u8 = 0x83;
pub const ACK: u8 = 1;
pub const SEAL: u8 = 2;
pub const MAX_BATCH: usize = 256;
pub const MAX_SESSION_DECLARATIONS: usize = 1_000_000;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkSetFrame {
    pub session_id: String,
    pub producer_id: [u8; 16],
    pub scope_id: u32,
    pub parent: Option<EntityKey>,
    pub sequence: u64,
    pub entity_ids: Vec<u32>,
    pub flags: u8,
    pub seal_digest: Option<[u8; 32]>,
}

impl WorkSetFrame {
    fn validate(&self) -> Result<(), ProtocolError> {
        validate_session_id(&self.session_id)?;
        if self.producer_id == [0; 16]
            || self.flags > 3
            || (self.flags & SEAL != 0) != self.seal_digest.is_some()
            || self.entity_ids.len() > MAX_BATCH
            || self
                .entity_ids
                .iter()
                .any(|id| *id == 0 || *id > MAX_ENTITY_ID)
            || self.entity_ids.windows(2).any(|pair| pair[0] >= pair[1])
            || (self.entity_ids.is_empty() && self.flags & SEAL == 0)
            || (self.scope_id == 0) != self.parent.is_none()
            || self
                .parent
                .is_some_and(|parent| parent.entity_id == 0 || parent.entity_id > MAX_ENTITY_ID)
        {
            return Err(ProtocolError::frame("invalid WORK_SET fields"));
        }
        Ok(())
    }
}

pub fn encode(frame: &WorkSetFrame) -> Result<Vec<u8>, ProtocolError> {
    frame.validate()?;
    let mut body = Vec::new();
    let mut e = Encoder::new(&mut body);
    e.map(6 + u64::from(frame.parent.is_some()) * 2 + u64::from(frame.seal_digest.is_some()))
        .map_err(cbor_encode)?;
    e.str("flags")
        .map_err(cbor_encode)?
        .u8(frame.flags)
        .map_err(cbor_encode)?;
    e.str("scope-id")
        .map_err(cbor_encode)?
        .u32(frame.scope_id)
        .map_err(cbor_encode)?;
    e.str("sequence")
        .map_err(cbor_encode)?
        .u64(frame.sequence)
        .map_err(cbor_encode)?;
    if let Some(parent) = frame.parent {
        e.str("parent-id")
            .map_err(cbor_encode)?
            .u32(parent.entity_id)
            .map_err(cbor_encode)?;
    }
    e.str("entity-ids")
        .map_err(cbor_encode)?
        .array(frame.entity_ids.len() as u64)
        .map_err(cbor_encode)?;
    for id in &frame.entity_ids {
        e.u32(*id).map_err(cbor_encode)?;
    }
    e.str("session-id")
        .map_err(cbor_encode)?
        .str(&frame.session_id)
        .map_err(cbor_encode)?;
    e.str("producer-id")
        .map_err(cbor_encode)?
        .bytes(&frame.producer_id)
        .map_err(cbor_encode)?;
    if let Some(digest) = frame.seal_digest {
        e.str("seal-digest")
            .map_err(cbor_encode)?
            .bytes(&digest)
            .map_err(cbor_encode)?;
    }
    if let Some(parent) = frame.parent {
        e.str("parent-scope-id")
            .map_err(cbor_encode)?
            .u32(parent.scope_id)
            .map_err(cbor_encode)?;
    }
    encode_ucf(FRAME_WORK_SET, &body)
}

pub fn decode(bytes: &[u8]) -> Result<WorkSetFrame, ProtocolError> {
    deterministic::validate(bytes)?;
    let mut d = Decoder::new(bytes);
    let count = d
        .map()
        .map_err(cbor_decode)?
        .ok_or_else(|| ProtocolError::frame("indefinite WORK_SET"))?;
    let (mut session, mut producer, mut scope, mut sequence, mut ids, mut flags) =
        (None, None, None, None, None, None);
    let (mut parent, mut parent_scope, mut seal_digest) = (None, None, None);
    for _ in 0..count {
        match d.str().map_err(cbor_decode)? {
            "session-id" => session = Some(d.str().map_err(cbor_decode)?.to_owned()),
            "producer-id" => {
                producer = Some(
                    d.bytes()
                        .map_err(cbor_decode)?
                        .try_into()
                        .map_err(|_| ProtocolError::frame("producer-id must be 16 octets"))?,
                )
            }
            "scope-id" => scope = Some(d.u32().map_err(cbor_decode)?),
            "sequence" => sequence = Some(d.u64().map_err(cbor_decode)?),
            "flags" => flags = Some(d.u8().map_err(cbor_decode)?),
            "parent-id" => parent = Some(d.u32().map_err(cbor_decode)?),
            "parent-scope-id" => parent_scope = Some(d.u32().map_err(cbor_decode)?),
            "seal-digest" => {
                seal_digest = Some(
                    d.bytes()
                        .map_err(cbor_decode)?
                        .try_into()
                        .map_err(|_| ProtocolError::frame("seal-digest must be 32 octets"))?,
                )
            }
            "entity-ids" => {
                let count = d
                    .array()
                    .map_err(cbor_decode)?
                    .filter(|count| *count <= MAX_BATCH as u64)
                    .ok_or_else(|| ProtocolError::frame("invalid declaration batch size"))?;
                ids = Some(
                    (0..count)
                        .map(|_| d.u32().map_err(cbor_decode))
                        .collect::<Result<Vec<_>, _>>()?,
                );
            }
            _ => return Err(ProtocolError::frame("unknown WORK_SET member")),
        }
    }
    if parent.is_some() != parent_scope.is_some() {
        return Err(ProtocolError::frame("incomplete WORK_SET parent identity"));
    }
    let missing = || ProtocolError::frame("missing WORK_SET member");
    let frame = WorkSetFrame {
        session_id: session.ok_or_else(missing)?,
        producer_id: producer.ok_or_else(missing)?,
        scope_id: scope.ok_or_else(missing)?,
        sequence: sequence.ok_or_else(missing)?,
        entity_ids: ids.ok_or_else(missing)?,
        flags: flags.ok_or_else(missing)?,
        seal_digest,
        parent: parent
            .zip(parent_scope)
            .map(|(entity_id, scope_id)| EntityKey {
                scope_id,
                entity_id,
            }),
    };
    frame.validate()?;
    Ok(frame)
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkSets {
    pub producer_id: [u8; 16],
    pub scopes: BTreeMap<u32, DeclaredScope>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeclaredScope {
    pub parent: Option<EntityKey>,
    pub ids: BTreeSet<u32>,
    pub requests: BTreeMap<u64, [u8; 32]>,
    pub seal_digest: Option<[u8; 32]>,
}

/// Hash the complete ascending identifier set, independent of batch boundaries.
pub fn seal_digest(
    session_id: &str,
    producer_id: [u8; 16],
    scope_id: u32,
    parent: Option<EntityKey>,
    ids: &BTreeSet<u32>,
) -> [u8; 32] {
    let mut h = Sha256::new();
    h.update(b"pipestream-work-set-v1");
    h.update((session_id.len() as u16).to_be_bytes());
    h.update(session_id.as_bytes());
    h.update(producer_id);
    h.update(scope_id.to_be_bytes());
    h.update([u8::from(parent.is_some())]);
    if let Some(parent) = parent {
        h.update(parent.scope_id.to_be_bytes());
        h.update(parent.entity_id.to_be_bytes());
    }
    h.update((ids.len() as u64).to_be_bytes());
    for id in ids {
        h.update(id.to_be_bytes());
    }
    h.finalize().into()
}

impl Session {
    pub fn new_sealed(
        session_id: impl Into<String>,
        producer_id: [u8; 16],
        depth: u8,
        limit: u32,
    ) -> Result<Self, ProtocolError> {
        if producer_id == [0; 16] {
            return Err(ProtocolError::entity("producer-id is zero"));
        }
        let mut session = Self::new(session_id, depth, limit)?;
        session.work_sets = Some(WorkSets {
            producer_id,
            scopes: BTreeMap::new(),
        });
        Ok(session)
    }

    pub fn declare_work(
        &mut self,
        frame: &WorkSetFrame,
        now: u64,
    ) -> Result<WorkSetFrame, ProtocolError> {
        frame.validate()?;
        let work = self
            .work_sets
            .as_ref()
            .ok_or_else(|| ProtocolError::entity("session is not a sealed-work session"))?;
        if frame.flags & ACK != 0
            || frame.session_id != self.session_id
            || frame.producer_id != work.producer_id
        {
            return Err(ProtocolError::entity(
                "WORK_SET producer, session or direction mismatch",
            ));
        }
        let request_hash: [u8; 32] = Sha256::digest(encode(frame)?).into();
        let existing = work.scopes.get(&frame.scope_id);
        let mut ack = frame.clone();
        ack.flags |= ACK;
        if let Some(previous) = existing.and_then(|s| s.requests.get(&frame.sequence)) {
            return if previous == &request_hash {
                Ok(ack)
            } else {
                Err(ProtocolError::entity("WORK_SET sequence changed"))
            };
        }
        if frame.sequence != existing.map_or(0, |s| s.requests.len() as u64)
            || existing.is_some_and(|s| s.seal_digest.is_some() || s.parent != frame.parent)
            || existing
                .and_then(|s| s.ids.last())
                .zip(frame.entity_ids.first())
                .is_some_and(|(last, first)| first <= last)
        {
            return Err(ProtocolError::entity(
                "late, reordered or conflicting WORK_SET",
            ));
        }
        let count = existing.map_or(0, |s| s.ids.len()) + frame.entity_ids.len();
        if work.scopes.values().map(|s| s.ids.len()).sum::<usize>() + frame.entity_ids.len()
            > MAX_SESSION_DECLARATIONS
        {
            return Err(ProtocolError::limit("session declaration budget exhausted"));
        }
        if count == 0 || count > self.max_entities_per_scope as usize {
            return Err(ProtocolError::limit("work-set entity count is invalid"));
        }
        // Validate the complete seal before mutating a scope or manifest.
        if let Some(expected) = frame.seal_digest {
            let ids = existing
                .into_iter()
                .flat_map(|s| s.ids.iter())
                .chain(frame.entity_ids.iter())
                .copied()
                .collect();
            if seal_digest(
                &self.session_id,
                work.producer_id,
                frame.scope_id,
                frame.parent,
                &ids,
            ) != expected
            {
                return Err(ProtocolError::integrity(
                    "work-set seal differs from declarations",
                ));
            }
        }
        if existing.is_none() {
            if let Some(parent) = frame.parent {
                self.open_child_scope(parent, frame.scope_id, now)?;
            } else if frame.scope_id != 0 {
                return Err(ProtocolError::entity("root work set must use scope zero"));
            }
        }
        let scope = self
            .work_sets
            .as_mut()
            .unwrap()
            .scopes
            .entry(frame.scope_id)
            .or_insert_with(|| DeclaredScope {
                parent: frame.parent,
                ids: BTreeSet::new(),
                requests: BTreeMap::new(),
                seal_digest: None,
            });
        scope.ids.extend(&frame.entity_ids);
        scope.requests.insert(frame.sequence, request_hash);
        scope.seal_digest = frame.seal_digest;
        Ok(ack)
    }

    pub fn work_scope_ready(&self, scope_id: u32) -> bool {
        self.work_sets
            .as_ref()
            .and_then(|w| w.scopes.get(&scope_id))
            .is_some_and(|scope| {
                scope.seal_digest.is_some()
                    && scope.ids.iter().all(|id| {
                        self.entities
                            .get(&EntityKey {
                                scope_id,
                                entity_id: *id,
                            })
                            .is_some_and(|e| self.entity_is_resolved(e))
                    })
            })
    }

    pub fn work_admission(
        &self,
        key: EntityKey,
        parent: Option<EntityKey>,
    ) -> Result<(), ProtocolError> {
        if let Some(work) = &self.work_sets {
            let scope = work
                .scopes
                .get(&key.scope_id)
                .ok_or_else(|| ProtocolError::entity("scope has no declaration"))?;
            if scope.parent != parent || !scope.ids.contains(&key.entity_id) {
                return Err(ProtocolError::entity(
                    "entity was not declared in this work set",
                ));
            }
        }
        Ok(())
    }
}
