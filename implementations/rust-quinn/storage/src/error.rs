//! Error taxonomy for the storage SPI.
//!
//! The taxonomy is part of the SPI contract: recovery behavior differs per
//! class, so every backend must map its failures into these categories.

use std::fmt;

/// The one error type returned by every [`crate::ObjectStore`] and
/// [`crate::Journal`] operation.
///
/// The variants distinguish the four classes the contract requires —
/// [`NotFound`](StorageError::NotFound),
/// [`AlreadySealed`](StorageError::AlreadySealed),
/// [`Corrupt`](StorageError::Corrupt), and
/// [`Transient`](StorageError::Transient) — plus a general category,
/// [`Backend`](StorageError::Backend), for everything else (I/O failures,
/// invalid arguments, backend-internal invariant violations).
#[derive(Debug)]
pub enum StorageError {
    /// The named scope, entity, or intent does not exist.
    NotFound(String),
    /// The operation targets a scope that is already sealed. After a scope
    /// seals, `put` into it fails with this class.
    AlreadySealed(String),
    /// A digest or content mismatch: conflicting bytes for an existing
    /// entity, an `expected_digest` that does not match the payload, a
    /// `verify_scope` recompute that disagrees with the seal record, or a
    /// conflicting re-seal or re-commit.
    Corrupt(String),
    /// A retryable failure (lock contention, temporarily unreachable
    /// service). Callers may retry the identical operation.
    Transient(String),
    /// General category for failures outside the four contract classes:
    /// I/O errors, invalid arguments, and backend-internal violations.
    Backend(String),
}

impl StorageError {
    /// Builds a [`StorageError::NotFound`] with the given detail.
    pub fn not_found(detail: impl Into<String>) -> Self {
        StorageError::NotFound(detail.into())
    }

    /// Builds a [`StorageError::AlreadySealed`] with the given detail.
    pub fn already_sealed(detail: impl Into<String>) -> Self {
        StorageError::AlreadySealed(detail.into())
    }

    /// Builds a [`StorageError::Corrupt`] with the given detail.
    pub fn corrupt(detail: impl Into<String>) -> Self {
        StorageError::Corrupt(detail.into())
    }

    /// Builds a [`StorageError::Transient`] with the given detail.
    pub fn transient(detail: impl Into<String>) -> Self {
        StorageError::Transient(detail.into())
    }

    /// Builds a [`StorageError::Backend`] with the given detail.
    pub fn backend(detail: impl Into<String>) -> Self {
        StorageError::Backend(detail.into())
    }

    /// Returns true when the failure is [`StorageError::Transient`] and the
    /// identical operation may be retried.
    pub fn is_retryable(&self) -> bool {
        matches!(self, StorageError::Transient(_))
    }
}

impl fmt::Display for StorageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StorageError::NotFound(detail) => write!(f, "not found: {detail}"),
            StorageError::AlreadySealed(detail) => write!(f, "already sealed: {detail}"),
            StorageError::Corrupt(detail) => write!(f, "corrupt: {detail}"),
            StorageError::Transient(detail) => write!(f, "transient: {detail}"),
            StorageError::Backend(detail) => write!(f, "backend: {detail}"),
        }
    }
}

impl std::error::Error for StorageError {}

impl From<std::io::Error> for StorageError {
    fn from(error: std::io::Error) -> Self {
        StorageError::Backend(format!("i/o: {error}"))
    }
}
