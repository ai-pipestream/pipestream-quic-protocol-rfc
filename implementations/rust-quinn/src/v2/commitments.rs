//! Domain-separated commitments over typed, deterministic protocol records.

use super::{codec::*, records::scope_identity, *};
use sha2::{Digest as _, Sha256};

fn hash(domain: &[u8], bytes: &[u8]) -> Digest {
    let mut hasher = Sha256::new();
    hasher.update(domain);
    hasher.update(bytes);
    Digest(hasher.finalize().into())
}

/// Stable issuing identity, not a credential or a discovered URI authority.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionIdentity {
    pub authority: IdentityLabel,
    pub owner: IdentityLabel,
    pub generation: Id,
}

impl SessionIdentity {
    fn write(&self, w: &mut Writer) -> Result<(), Error> {
        self.authority.check()?;
        self.owner.check()?;
        self.generation.check()?;
        self.authority.write(w);
        self.owner.write(w);
        self.generation.write(w);
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Mutation {
    Admit(AdmitParameters),
    Declare {
        scope: Number,
        entity_ids: Vec<Id>,
        seal: bool,
    },
    Retry {
        work: WorkKey,
        expected_attempt: Id,
    },
    Cancel {
        work: WorkKey,
    },
    Skip {
        work: WorkKey,
    },
    ScopeCancel {
        scope: Number,
    },
}

impl Mutation {
    /// Canonical operation preimage excludes connection request numbers and raw
    /// input bytes, but includes the originator's namespace and all parameters.
    pub fn commitment_bytes(
        &self,
        session: &SessionIdentity,
        originator: Producer,
        operation: OperationId,
    ) -> Result<Vec<u8>, Error> {
        originator.check()?;
        operation.check()?;
        let mut w = Writer::new();
        w.array(8);
        session.write(&mut w)?;
        originator.write(&mut w);
        operation.write(&mut w);
        match self {
            Self::Admit(parameters) => {
                parameters.check()?;
                require(
                    parameters.work.producer == originator,
                    "admission originator differs from input producer",
                )?;
                w.uint(4);
                w.uint(0);
                parameters.write(&mut w);
            }
            Self::Declare {
                scope,
                entity_ids,
                seal,
            } => {
                scope.check()?;
                entity_ids.check()?;
                require(
                    (!entity_ids.is_empty() || *seal) && entity_ids.windows(2).all(|p| p[0] < p[1]),
                    "invalid declaration commitment",
                )?;
                w.uint(3);
                w.uint(0);
                w.array(3);
                scope.write(&mut w);
                entity_ids.write(&mut w);
                seal.write(&mut w);
            }
            Self::Retry {
                work,
                expected_attempt,
            } => {
                work.check()?;
                expected_attempt.check()?;
                w.uint(4);
                w.uint(6);
                w.array(2);
                work.write(&mut w);
                expected_attempt.write(&mut w);
            }
            Self::Cancel { work } | Self::Skip { work } => {
                work.check()?;
                w.uint(4);
                w.uint(if matches!(self, Self::Cancel { .. }) {
                    8
                } else {
                    10
                });
                w.array(1);
                work.write(&mut w);
            }
            Self::ScopeCancel { scope } => {
                scope.check()?;
                w.uint(3);
                w.uint(6);
                w.array(1);
                scope.write(&mut w);
            }
        }
        Ok(w.finish())
    }

    pub fn digest(
        &self,
        session: &SessionIdentity,
        originator: Producer,
        operation: OperationId,
    ) -> Result<Digest, Error> {
        Ok(hash(
            b"pipestream-operation-v2",
            &self.commitment_bytes(session, originator, operation)?,
        ))
    }
}

impl Manifest {
    pub fn digest(&self) -> Result<Digest, Error> {
        Ok(hash(b"pipestream-result-manifest-v2", &self.encode()?))
    }
}

/// Constant-memory seal hashing. The caller supplies the retained declared
/// count; finish fails if the iterator is truncated, extended or unsorted.
pub fn scope_seal<I>(
    session: &SessionIdentity,
    scope: Number,
    producer: Producer,
    parent: Option<&WorkKey>,
    declared: Number,
    ids: I,
) -> Result<Digest, Error>
where
    I: IntoIterator<Item = Id>,
{
    scope.check()?;
    producer.check()?;
    declared.check()?;
    if let Some(parent) = parent {
        parent.check()?;
    }
    scope_identity(scope, producer, parent)?;
    let mut w = Writer::new();
    w.array(7);
    session.write(&mut w)?;
    scope.write(&mut w);
    producer.write(&mut w);
    if let Some(parent) = parent {
        parent.write(&mut w);
    } else {
        w.null();
    }
    // The full membership is not a wire batch and can exceed 256 entries.
    // Its CBOR array length is a u64, including on 32-bit mobile platforms.
    w.array_u64(declared.0);
    let mut hasher = Sha256::new();
    hasher.update(b"pipestream-scope-seal-v2");
    hasher.update(w.finish());
    let mut count = 0u64;
    let mut previous = 0;
    for id in ids {
        id.check()?;
        require(
            count < declared.0 && id.0 > previous,
            "seal membership count/order mismatch",
        )?;
        let mut encoded = minicbor::Encoder::new(minicbor::encode::write::Cursor::new([0; 9]));
        encoded
            .u64(id.0)
            .expect("nine bytes encode any unsigned integer");
        let cursor = encoded.into_writer();
        hasher.update(&cursor.get_ref()[..cursor.position()]);
        previous = id.0;
        count += 1;
    }
    require(count == declared.0, "truncated seal membership")?;
    Ok(Digest(hasher.finalize().into()))
}

record!(StatusLeaf { work: WorkKey, state: State, attempt: Number, manifest_digest: Option<Digest>, child_status_root: Option<Digest> } |s| {
    require(s.state.is_terminal(), "status leaf is nonterminal")?;
    require(s.attempt.0 != 0 || matches!(s.state, State::CANCELLED | State::SKIPPED), "invalid inputless status leaf")?;
    require(s.attempt.0 != 0 || s.child_status_root.is_none(), "inputless status leaf has child")?;
    require(s.state == State::SUCCEEDED || s.manifest_digest.is_none(), "nonsuccess status manifest")
});

impl StatusLeaf {
    pub fn commitment_bytes(&self) -> Result<Vec<u8>, Error> {
        encode(self, MAX_HEADER)
    }
    pub fn digest(&self) -> Result<Digest, Error> {
        Ok(hash(
            b"pipestream-status-leaf-v2",
            &self.commitment_bytes()?,
        ))
    }
}

pub fn empty_status_root() -> Digest {
    hash(b"pipestream-status-empty-v2", &[])
}

pub fn status_node(left: Digest, right: Digest) -> Digest {
    let mut hasher = Sha256::new();
    hasher.update(b"pipestream-status-node-v2");
    hasher.update(left.0);
    hasher.update(right.0);
    Digest(hasher.finalize().into())
}

/// Incremental Merkle fold with at most 63 retained hashes. Odd rightmost
/// subtrees are duplicated at each level, never padded with empty leaves.
#[derive(Debug, Default)]
pub struct StatusRoot {
    levels: Vec<Option<Digest>>,
    count: u64,
}

impl StatusRoot {
    pub fn push(&mut self, mut leaf: Digest) -> Result<(), Error> {
        if self.count == MAX_NUMBER {
            return Err(Error::new(
                ErrorCode::LimitExceeded,
                "status leaf count exhausted",
            ));
        }
        self.count += 1;
        let mut level = 0;
        loop {
            if level == self.levels.len() {
                self.levels.push(Some(leaf));
                return Ok(());
            }
            if let Some(left) = self.levels[level].take() {
                leaf = status_node(left, leaf);
                level += 1;
            } else {
                self.levels[level] = Some(leaf);
                return Ok(());
            }
        }
    }

    pub fn finish(self) -> Digest {
        let mut right: Option<(usize, Digest)> = None;
        for (level, left) in self.levels.into_iter().enumerate() {
            if let Some(left) = left {
                right = Some(if let Some((mut height, mut root)) = right {
                    while height < level {
                        root = status_node(root, root);
                        height += 1;
                    }
                    (level + 1, status_node(left, root))
                } else {
                    (level, left)
                });
            }
        }
        right.map_or_else(empty_status_root, |(_, root)| root)
    }
}
