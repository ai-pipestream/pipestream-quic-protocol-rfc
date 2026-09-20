//! Shared logical layout, identical for every backend and every language.
//!
//! This module pins the cross-language layout decisions of
//! `docs/standards/storage-spi-design.md` ("Pinned decisions"):
//!
//! - **Scope tree.** A [`ScopePath`] is 1 to 8 unsigned 32-bit components,
//!   outermost first; the root scope is a single component at depth 0 and
//!   protocol nesting is bounded to depth 7 (Section 6's 3-bit depth
//!   field). The rendered form is one lowercase, zero-padded, 8-hex-digit
//!   component per level, joined by `/`.
//! - **Leaf fanout.** Within a leaf scope, entities shard into 256 buckets:
//!   the first byte of `SHA-256(entity_id as 8-octet big-endian)`, rendered
//!   as 2 lowercase hex characters.
//! - **Canonical scope digest.** SHA-256 over the concatenation, in
//!   ascending entity-id order, of `0x00 || id-be64 || object-sha256` per
//!   entity; the empty scope's digest is SHA-256 of empty input. This is
//!   the value `seal_scope` conventionally records and `verify_scope`
//!   recomputes.
//! - **Entity names.** An entity renders as lowercase, zero-padded,
//!   16-hex-digit unsigned 63-bit id within its bucket.

use crate::StorageError;
use sha2::{Digest, Sha256};
use std::fmt;

/// A 32-byte SHA-256 digest.
pub type Digest32 = [u8; 32];

/// An entity identifier: unsigned 63-bit. (Wider than the protocol's
/// 32-bit wire Entity ID so backend layouts never need re-keying; callers
/// operating at protocol scale simply use the low range.)
pub type EntityId = u64;

/// Largest assignable entity identifier (`0x7fff_ffff_ffff_ffff`).
pub const MAX_ENTITY_ID: EntityId = 0x7fff_ffff_ffff_ffff;

/// Maximum protocol scope nesting depth (3-bit depth field, Section 6).
pub const MAX_SCOPE_DEPTH: usize = 7;

/// Number of fanout buckets within a leaf scope (2 hex chars).
pub const BUCKET_COUNT: u16 = 256;

/// Computes the SHA-256 digest of `bytes`.
pub fn sha256(bytes: &[u8]) -> Digest32 {
    Sha256::digest(bytes).into()
}

/// Validates an entity id for storage: unsigned 63-bit.
pub fn check_entity_id(entity: EntityId) -> Result<(), StorageError> {
    if entity > MAX_ENTITY_ID {
        return Err(StorageError::failed(format!(
            "entity id {entity:#x} is not unsigned 63-bit"
        )));
    }
    Ok(())
}

/// Maps an entity id to its fanout bucket within a leaf scope: the first
/// byte of `SHA-256(entity_id as 8-octet big-endian)`.
pub fn bucket(entity: EntityId) -> u8 {
    sha256(&entity.to_be_bytes())[0]
}

/// Renders an entity id's fanout bucket as two lowercase hex characters.
pub fn bucket_hex(entity: EntityId) -> String {
    format!("{:02x}", bucket(entity))
}

/// Renders a scope path component as lowercase, zero-padded, 8 hex digits.
pub fn scope_component(component: u32) -> String {
    format!("{component:08x}")
}

/// Renders an entity id as lowercase, zero-padded, 16 hex digits — its
/// unique name within a fanout bucket.
pub fn entity_component(entity: EntityId) -> String {
    format!("{entity:016x}")
}

/// Computes the canonical scope content digest: SHA-256 over every direct
/// entity in strictly ascending identifier order, each mixed in as one
/// zero domain-separation octet, the 8-octet big-endian identifier, and the
/// entity's 32-octet content digest. An empty input hashes the empty string.
///
/// `entities` may arrive in any order; it is sorted here. A duplicate
/// entity id is a caller error and fails with [`StorageError::Failed`].
pub fn scope_digest(entities: &[(EntityId, Digest32)]) -> Result<Digest32, StorageError> {
    let mut ordered = entities.to_vec();
    ordered.sort_by_key(|(id, _)| *id);
    let mut hasher = Sha256::new();
    let mut previous: Option<EntityId> = None;
    for (id, digest) in &ordered {
        if previous == Some(*id) {
            return Err(StorageError::failed(format!(
                "duplicate entity id {id} in scope digest input"
            )));
        }
        previous = Some(*id);
        hasher.update([0u8]);
        hasher.update(id.to_be_bytes());
        hasher.update(digest);
    }
    Ok(hasher.finalize().into())
}

/// A scope path: 1 to 8 unsigned 32-bit components, outermost first.
///
/// The root scope of a tree is a single component at depth 0; each child
/// scope adds one level. Depth is bounded to 0-7 by the protocol's 3-bit
/// depth field. Components may be any unsigned 32-bit value.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ScopePath {
    components: Vec<u32>,
}

impl ScopePath {
    /// Returns the single-component root path for `component` (depth 0).
    pub fn root(component: u32) -> Self {
        ScopePath {
            components: vec![component],
        }
    }

    /// Builds a scope path from explicit components, outermost first.
    ///
    /// Fails with [`StorageError::Failed`] when the component count is not
    /// in `1..=8`.
    pub fn from_components(components: Vec<u32>) -> Result<Self, StorageError> {
        if components.is_empty() || components.len() > MAX_SCOPE_DEPTH + 1 {
            return Err(StorageError::failed(format!(
                "scope path needs 1 to {} components, got {}",
                MAX_SCOPE_DEPTH + 1,
                components.len()
            )));
        }
        Ok(ScopePath { components })
    }

    /// Returns the path of a child scope one level below this one.
    ///
    /// Fails with [`StorageError::Failed`] when this path is already at the
    /// maximum protocol depth.
    pub fn child(&self, component: u32) -> Result<Self, StorageError> {
        let mut components = self.components.clone();
        components.push(component);
        ScopePath::from_components(components)
    }

    /// The scope's nesting depth: `components().len() - 1`. A root path is
    /// depth 0.
    pub fn depth(&self) -> usize {
        self.components.len() - 1
    }

    /// The scope's own component (the last one).
    pub fn scope_id(&self) -> u32 {
        // Invariant (enforced by construction): at least one component.
        self.components[self.components.len() - 1]
    }

    /// The path components, outermost first.
    pub fn components(&self) -> &[u32] {
        &self.components
    }

    /// Renders the path as lowercase, zero-padded, 8-hex-digit components
    /// joined by `/` — the canonical cross-backend scope name.
    pub fn to_path_string(&self) -> String {
        self.components
            .iter()
            .map(|c| scope_component(*c))
            .collect::<Vec<_>>()
            .join("/")
    }
}

impl fmt::Display for ScopePath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "/{}", self.to_path_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn root_is_depth_zero() {
        let root = ScopePath::root(0);
        assert_eq!(root.depth(), 0);
        assert_eq!(root.scope_id(), 0);
        assert_eq!(root.to_path_string(), "00000000");
        // Any unsigned 32-bit component may be a root.
        assert_eq!(ScopePath::root(9).to_path_string(), "00000009");
    }

    #[test]
    fn depth_bound_is_enforced() {
        let mut path = ScopePath::root(0);
        for id in 1..=MAX_SCOPE_DEPTH as u32 {
            path = path.child(id).unwrap();
        }
        assert_eq!(path.depth(), MAX_SCOPE_DEPTH);
        assert!(path.child(9).is_err());
    }

    #[test]
    fn component_count_bound_is_enforced() {
        assert!(ScopePath::from_components(vec![]).is_err());
        assert!(ScopePath::from_components(vec![0, 7]).is_ok());
        assert!(ScopePath::from_components(vec![0; 9]).is_err());
    }

    #[test]
    fn bucket_spans_all_256_values() {
        let mut seen = std::collections::BTreeSet::new();
        for entity in 0..4096u64 {
            seen.insert(bucket(entity));
        }
        assert_eq!(seen.len(), BUCKET_COUNT as usize);
    }

    #[test]
    fn bucket_matches_pinned_derivation() {
        // First byte of SHA-256 over the 8-octet big-endian id, 2 hex chars.
        let expected = format!("{:02x}", sha256(&42u64.to_be_bytes())[0]);
        assert_eq!(bucket_hex(42), expected);
    }

    #[test]
    fn components_render_fixed_width_hex() {
        assert_eq!(scope_component(0), "00000000");
        assert_eq!(scope_component(0xdead_beef), "deadbeef");
        assert_eq!(entity_component(1), "0000000000000001");
    }

    #[test]
    fn scope_digest_orders_and_domain_separates() {
        let a = sha256(b"alpha");
        let b = sha256(b"beta");
        let forward = scope_digest(&[(1, a), (2, b)]).unwrap();
        let reversed = scope_digest(&[(2, b), (1, a)]).unwrap();
        assert_eq!(forward, reversed);
        assert_ne!(forward, scope_digest(&[(1, a)]).unwrap());
        // The empty scope hashes the empty input.
        assert_eq!(scope_digest(&[]).unwrap(), sha256(b""));
        // Duplicate ids are rejected.
        assert!(scope_digest(&[(1, a), (1, b)]).is_err());
    }
}
