//! Shared logical layout, identical for every backend and every language.
//!
//! The layout has three parts:
//!
//! - **Scope tree.** A [`ScopePath`] is the chain of 32-bit Scope IDs from
//!   the root scope (ID 0) down to the scope in question. Protocol depth is
//!   0-7 (Section 6's 3-bit depth field), so a path holds 1 to 8 components
//!   inclusive. On the filesystem this is a directory tree; on S3 a key
//!   prefix; in a database a path column.
//! - **Leaf fanout.** Within a scope, entities are sharded into 256 buckets
//!   by [`bucket`]: the first byte of `SHA-256(entity_id as 4-octet
//!   big-endian)`, rendered as two lowercase hex characters. The shard width
//!   is a backend-local constant, but the reference backends all use this
//!   exact derivation so a bucket name computed by the Rust lane matches one
//!   computed by the Java lane.
//! - **Per-scope seal record.** A sealed scope's container holds a
//!   [`crate::SealRecord`] (SCOPE_DIGEST + entity count) alongside the
//!   objects, so verification and audit are scope-local.

use crate::StorageError;
use sha2::{Digest, Sha256};
use std::fmt;

/// A 32-byte SHA-256 digest.
pub type Digest32 = [u8; 32];

/// A protocol Entity ID (32 bits). `0xffff_fffd` through `0xffff_ffff` are
/// reserved by the protocol and are never valid stored entities.
pub type EntityId = u32;

/// Largest Entity ID that may be stored (mirrors `MAX_ENTITY_ID` in the
/// protocol crate; `0xffff_fffc`).
pub const MAX_ENTITY_ID: EntityId = 0xffff_fffc;

/// Scope ID of the root scope (Section 6.2.1).
pub const ROOT_SCOPE_ID: u32 = 0;

/// Maximum protocol scope nesting depth (3-bit depth field, Section 6).
pub const MAX_SCOPE_DEPTH: usize = 7;

/// Number of fanout buckets within a leaf scope (2 hex chars).
pub const BUCKET_COUNT: u16 = 256;

/// Computes the SHA-256 digest of `bytes`.
pub fn sha256(bytes: &[u8]) -> Digest32 {
    Sha256::digest(bytes).into()
}

/// Validates an entity id for storage.
pub(crate) fn check_entity_id(entity: EntityId) -> Result<(), StorageError> {
    if entity > MAX_ENTITY_ID {
        return Err(StorageError::backend(format!(
            "entity id {entity:#010x} exceeds MAX_ENTITY_ID"
        )));
    }
    Ok(())
}

/// Maps an entity id to its fanout bucket within a leaf scope: the first
/// byte of `SHA-256(entity_id.to_be_bytes())`.
pub fn bucket(entity: EntityId) -> u8 {
    sha256(&entity.to_be_bytes())[0]
}

/// Renders an entity id's fanout bucket as two lowercase hex characters
/// (the bucket directory or key-prefix component used by path-based
/// backends).
pub fn bucket_hex(entity: EntityId) -> String {
    format!("{:02x}", bucket(entity))
}

/// Renders a Scope ID as a path component: eight lowercase hex characters,
/// big-endian. Fixed width keeps lexicographic and numeric order aligned.
pub fn scope_component(scope_id: u32) -> String {
    format!("{scope_id:08x}")
}

/// Renders an Entity ID as an object name: eight lowercase hex characters,
/// big-endian.
pub fn entity_component(entity: EntityId) -> String {
    format!("{entity:08x}")
}

/// A scope path: the chain of Scope IDs from the root scope to the scope in
/// question, root first.
///
/// Invariants, enforced at construction: at least one component, at most
/// [`MAX_SCOPE_DEPTH`] + 1 components, and the first component is
/// [`ROOT_SCOPE_ID`]. The root scope itself is the single-component path
/// `[0]` (depth 0).
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ScopePath {
    components: Vec<u32>,
}

impl ScopePath {
    /// Returns the path of the root scope (depth 0).
    pub fn root() -> Self {
        ScopePath {
            components: vec![ROOT_SCOPE_ID],
        }
    }

    /// Builds a scope path from explicit components.
    ///
    /// Fails with [`StorageError::Backend`] when the components are empty,
    /// deeper than the protocol bound, or do not start at the root scope.
    pub fn from_components(components: Vec<u32>) -> Result<Self, StorageError> {
        if components.is_empty() {
            return Err(StorageError::backend("scope path is empty"));
        }
        if components.len() > MAX_SCOPE_DEPTH + 1 {
            return Err(StorageError::backend(format!(
                "scope path depth {} exceeds protocol bound {MAX_SCOPE_DEPTH}",
                components.len() - 1
            )));
        }
        if components[0] != ROOT_SCOPE_ID {
            return Err(StorageError::backend(format!(
                "scope path must start at root scope {ROOT_SCOPE_ID}, got {:#010x}",
                components[0]
            )));
        }
        Ok(ScopePath { components })
    }

    /// Returns the path of a child scope one level below this one.
    ///
    /// Fails with [`StorageError::Backend`] when this path is already at the
    /// maximum protocol depth.
    pub fn child(&self, scope_id: u32) -> Result<Self, StorageError> {
        let mut components = self.components.clone();
        components.push(scope_id);
        ScopePath::from_components(components)
    }

    /// The scope's nesting depth: `components().len() - 1`. The root scope
    /// is depth 0.
    pub fn depth(&self) -> usize {
        self.components.len() - 1
    }

    /// The scope's own Scope ID (the last component).
    pub fn scope_id(&self) -> u32 {
        // Invariant (enforced by construction): at least one component.
        self.components[self.components.len() - 1]
    }

    /// The path components, root first.
    pub fn components(&self) -> &[u32] {
        &self.components
    }

    /// Renders the path as `/`-joined hex components (the directory or key
    /// prefix used by path-based backends), without a leading slash.
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
    fn root_scope_is_depth_zero() {
        let root = ScopePath::root();
        assert_eq!(root.depth(), 0);
        assert_eq!(root.scope_id(), ROOT_SCOPE_ID);
        assert_eq!(root.to_path_string(), "00000000");
    }

    #[test]
    fn depth_bound_is_enforced() {
        let mut path = ScopePath::root();
        for id in 1..=MAX_SCOPE_DEPTH as u32 {
            path = path.child(id).unwrap();
        }
        assert_eq!(path.depth(), MAX_SCOPE_DEPTH);
        assert!(path.child(9).is_err());
    }

    #[test]
    fn paths_must_start_at_root() {
        assert!(ScopePath::from_components(vec![]).is_err());
        assert!(ScopePath::from_components(vec![1]).is_err());
        assert!(ScopePath::from_components(vec![0, 7]).is_ok());
    }

    #[test]
    fn bucket_spans_all_256_values() {
        let mut seen = std::collections::BTreeSet::new();
        for entity in 0..4096u32 {
            seen.insert(bucket(entity));
        }
        assert_eq!(seen.len(), BUCKET_COUNT as usize);
    }

    #[test]
    fn bucket_hex_is_two_lowercase_chars() {
        let rendered = bucket_hex(42);
        assert_eq!(rendered.len(), 2);
        assert!(rendered.chars().all(|c| c.is_ascii_hexdigit()));
        assert_eq!(rendered, rendered.to_lowercase());
    }

    #[test]
    fn components_render_fixed_width_hex() {
        assert_eq!(scope_component(0), "00000000");
        assert_eq!(scope_component(0xdead_beef), "deadbeef");
        assert_eq!(entity_component(1), "00000001");
    }
}
