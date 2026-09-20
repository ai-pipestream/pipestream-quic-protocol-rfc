//! Error taxonomy for the storage SPI.
//!
//! The taxonomy is part of the SPI contract: recovery behavior differs per
//! class, so every backend must map its failures into these categories. The
//! five classes match the Java lane's `StorageException.Kind` exactly.

use std::fmt;

/// The one error type returned by every [`crate::ObjectStore`] and
/// [`crate::Journal`] operation.
#[derive(Debug)]
pub enum StorageError {
    /// The addressed scope, entity, intent, or seal record does not exist.
    NotFound(String),
    /// The addressed scope is sealed and rejects mutation. After a scope
    /// seals, `put` into it fails with this class regardless of bytes.
    AlreadySealed(String),
    /// Stored or submitted bytes contradict a recorded or expected digest:
    /// conflicting bytes for an existing entity, an `expected_digest` that
    /// does not match the payload, a `verify_scope` recompute that
    /// disagrees with the seal record, a conflicting re-seal, or a
    /// conflicting intent append or commit.
    Corrupt(String),
    /// Retryable failure: I/O errors, timeouts, lock contention, a
    /// temporarily unreachable service. The identical operation may be
    /// retried.
    Transient(String),
    /// General durable failure that is neither retryable nor corruption:
    /// invalid arguments, double-writer acquisition, use-after-close, and
    /// backend-internal violations.
    Failed(String),
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

    /// Builds a [`StorageError::Failed`] with the given detail.
    pub fn failed(detail: impl Into<String>) -> Self {
        StorageError::Failed(detail.into())
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
            StorageError::Failed(detail) => write!(f, "failed: {detail}"),
        }
    }
}

impl std::error::Error for StorageError {}

impl From<std::io::Error> for StorageError {
    /// Per the pinned error mapping, I/O failures are transient.
    fn from(error: std::io::Error) -> Self {
        StorageError::Transient(format!("i/o: {error}"))
    }
}
