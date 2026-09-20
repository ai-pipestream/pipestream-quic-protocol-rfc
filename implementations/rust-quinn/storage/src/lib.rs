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
//! Java) is readable by the other. The crate ships backends for in-memory
//! ([`memory`]), filesystem ([`fs`]), PostgreSQL ([`postgres`]), S3
//! ([`s3`], object role only), Kafka ([`kafka`], journal role only), and
//! MongoDB ([`mongo`]), plus the backend-agnostic contract test suite in
//! [`contract`] that certifies every backend against the pinned semantics.
//!
//! The SPI is synchronous: the reference storage model is blocking, and
//! async-service backends are expected to wrap a blocking core internally
//! (the S3, Kafka, and MongoDB backends do exactly that, on private
//! runtimes). Implementations must be safe to share across threads behind
//! caller-side synchronization (e.g. `Mutex`); the contract suite exercises
//! every object backend concurrently.

#![deny(missing_docs)]
#![deny(unsafe_code)]

pub mod contract;
mod error;
pub mod fs;
pub mod kafka;
pub mod layout;
mod memory;
pub mod mongo;
pub mod postgres;
mod s3;
mod spi;

pub use error::StorageError;
pub use fs::FsStore;
pub use layout::{Digest32, EntityId, ScopePath, sha256};
pub use kafka::{KafkaConfig, KafkaJournal};
pub use memory::MemoryStore;
pub use mongo::{MongoConfig, MongoStore};
pub use postgres::PostgresStore;
pub use s3::{S3Config, S3Store};
pub use spi::{
    Cursor, Intent, Journal, MAX_INTENT_ID_BYTES, MAX_RECORD_BYTES, ObjectStore, ReplayEntry,
    SealRecord,
};
