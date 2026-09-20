//! Pluggable storage SPI for the PipeStream reference implementations.
//!
//! This crate implements the storage layer defined by
//! `docs/standards/storage-spi-design.md`. The protocol specification is
//! silent about storage; everything here is invisible on the wire and exists
//! so that any storage mechanism can back the reference implementations by
//! satisfying one contract. The layer splits into two roles:
//!
//! - [`ObjectStore`] — sealed, immutable, content-verified entities and
//!   per-scope seal records holding the scope's SCOPE_DIGEST.
//! - [`Journal`] — append-only, ordered, replayable intent/commit records for
//!   crash recovery.
//!
//! A backend may implement one or both roles. All backends share the logical
//! layout in [`layout`], so a store written by one implementation (Rust or
//! Java) is readable by the other. Slice 1 ships the in-memory
//! ([`memory`]) and filesystem ([`fs`]) backends plus a reusable
//! backend-agnostic contract test suite in [`contract`].
//!
//! The SPI is synchronous: the reference storage model is blocking, and
//! async-service backends are expected to wrap a blocking core internally.

#![deny(missing_docs)]
#![deny(unsafe_code)]

pub mod contract;
mod error;
pub mod fs;
pub mod kafka;
pub mod layout;
mod memory;
pub mod postgres;
mod s3;
mod spi;

pub use error::StorageError;
pub use fs::FsStore;
pub use layout::{Digest32, EntityId, ScopePath, sha256};
pub use kafka::{KafkaConfig, KafkaJournal};
pub use memory::MemoryStore;
pub use postgres::PostgresStore;
pub use s3::{S3Config, S3Store};
pub use spi::{Cursor, Journal, JournalEntry, JournalKind, ObjectStore, SealRecord};
