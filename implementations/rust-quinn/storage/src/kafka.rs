//! Kafka backend implementing the [`Journal`] role only.
//!
//! Per the design note's backend matrix, payloads do not belong in the log;
//! pair this journal with an object-store backend (filesystem, Postgres,
//! S3) for the two-system configuration: object write first, journal commit
//! second, reconcile on recovery by replaying.
//!
//! Physical mapping: one Kafka topic with exactly one partition (a single
//! partition is what makes "ordered replay" meaningful in Kafka). Record
//! values carry the same framing as the filesystem journal, minus the
//! length prefix a message boundary already provides: `u8 kind | payload`,
//! kind 0 = intent (payload = record), kind 1 = commit (payload =
//! `u64 BE intent cursor` then record). A record's cursor is
//! `offset + 1`, so `Cursor(0)` stays "the beginning".
//!
//! Durability configuration: produces use `required_acks = -1` (all
//! in-sync replicas) and every append awaits the broker's acknowledgment,
//! so a returned cursor survives a process kill.
//!
//! The journal assumes the reference deployment's single-writer recovery
//! model: [`KafkaJournal::open`] loads the whole partition into an
//! in-memory index, appends go through this instance, and `replay` reads
//! the index. Records produced by another live writer between `open` and
//! `replay` are not the recovery path's concern.
//!
//! The client (`rskafka`, pure Rust) is async; the store owns a private
//! current-thread tokio runtime and blocks on each call, keeping the SPI
//! synchronous.

use crate::{Cursor, Journal, JournalEntry, JournalKind, StorageError};
use rskafka::client::ClientBuilder;
use rskafka::client::partition::{Compression, UnknownTopicHandling};
use rskafka::record::Record;
use std::collections::BTreeMap;

const KIND_INTENT: u8 = 0;
const KIND_COMMIT: u8 = 1;
const PARTITION: i32 = 0;

/// Configuration for [`KafkaJournal`].
#[derive(Debug, Clone)]
pub struct KafkaConfig {
    /// Bootstrap servers, e.g. `127.0.0.1:9092`.
    pub bootstrap_servers: String,
    /// Journal topic. Created with one partition when absent.
    pub topic: String,
}

/// Kafka backend for the [`Journal`] role.
///
/// See the [module documentation](self) for framing, cursor mapping, and
/// the durability configuration.
pub struct KafkaJournal {
    client: rskafka::client::Client,
    runtime: tokio::runtime::Runtime,
    topic: String,
    /// In-memory mirror of the partition, loaded at open. Entry `i` is the
    /// record at offset `i`, cursor `i + 1`.
    index: Vec<JournalKind>,
}

impl std::fmt::Debug for KafkaJournal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KafkaJournal")
            .field("topic", &self.topic)
            .finish_non_exhaustive()
    }
}

impl KafkaJournal {
    /// Connects to the cluster, creates the topic (one partition) when
    /// absent, and loads the partition into memory.
    pub fn open(config: &KafkaConfig) -> Result<Self, StorageError> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| StorageError::backend(format!("tokio runtime: {e}")))?;
        let client = runtime.block_on(ClientBuilder::new(vec![
            config.bootstrap_servers.clone(),
        ]).build());
        let client = client.map_err(map_kafka_error)?;
        let mut journal = KafkaJournal {
            client,
            runtime,
            topic: config.topic.clone(),
            index: Vec::new(),
        };
        journal.ensure_topic()?;
        journal.index = journal.load_index()?;
        Ok(journal)
    }

    fn block_on<F: std::future::Future>(&self, future: F) -> F::Output {
        self.runtime.block_on(future)
    }

    fn ensure_topic(&self) -> Result<(), StorageError> {
        let controller = self.client.controller_client().map_err(map_kafka_error)?;
        let result = self.block_on(controller.create_topic(&self.topic, 1, 1, 30_000));
        match result {
            Ok(()) => Ok(()),
            // TopicAlreadyExists: another store created it first; fine.
            Err(error) if error.to_string().contains("TopicAlreadyExists") => Ok(()),
            Err(error) => Err(map_kafka_error(error)),
        }
    }

    fn partition_client(
        &self,
    ) -> Result<rskafka::client::partition::PartitionClient, StorageError> {
        self.block_on(self.client.partition_client(
            self.topic.as_str(),
            PARTITION,
            UnknownTopicHandling::Error,
        ))
        .map_err(map_kafka_error)
    }

    /// Fetches the whole partition into the in-memory index.
    fn load_index(&self) -> Result<Vec<JournalKind>, StorageError> {
        let partition = self.partition_client()?;
        let mut index = Vec::new();
        loop {
            let (records, high_watermark) = self
                .block_on(partition.fetch_records(
                    index.len() as i64,
                    1..16 * 1024 * 1024,
                    5_000,
                ))
                .map_err(map_kafka_error)?;
            for entry in records {
                let payload = entry.record.value.unwrap_or_default();
                index.push(decode_record(&self.topic, entry.offset, &payload)?);
            }
            if index.len() as i64 >= high_watermark {
                return Ok(index);
            }
        }
    }

    /// Appends one framed record and awaits the broker's acknowledgment.
    fn append_record(&mut self, kind: u8, payload: &[u8]) -> Result<Cursor, StorageError> {
        let partition = self.partition_client()?;
        let mut value = Vec::with_capacity(1 + payload.len());
        value.push(kind);
        value.extend_from_slice(payload);
        let record = Record {
            key: None,
            value: Some(value),
            headers: BTreeMap::new(),
            timestamp: chrono::Utc::now(),
        };
        let offsets = self
            .block_on(partition.produce(vec![record], Compression::NoCompression))
            .map_err(map_kafka_error)?;
        let base_offset = offsets.first().copied().ok_or_else(|| {
            StorageError::backend("kafka produce returned no offset".to_string())
        })?;
        let cursor = u64::try_from(base_offset + 1)
            .map(Cursor)
            .map_err(|_| StorageError::corrupt("negative journal offset".to_string()))?;
        Ok(cursor)
    }
}

/// Decodes one stored record.
fn decode_record(topic: &str, offset: i64, payload: &[u8]) -> Result<JournalKind, StorageError> {
    let corrupt = |detail: &str| {
        StorageError::corrupt(format!("journal {topic} offset {offset}: {detail}"))
    };
    let (kind, body) = match payload.split_first() {
        Some((kind, body)) => (*kind, body),
        None => return Err(corrupt("empty record")),
    };
    match kind {
        KIND_INTENT => Ok(JournalKind::Intent(body.to_vec())),
        KIND_COMMIT => {
            if body.len() < 8 {
                return Err(corrupt("commit record shorter than 8 bytes"));
            }
            let mut cursor = [0u8; 8];
            cursor.copy_from_slice(&body[..8]);
            Ok(JournalKind::Commit {
                intent: Cursor(u64::from_be_bytes(cursor)),
                record: body[8..].to_vec(),
            })
        }
        other => Err(corrupt(&format!("unknown record kind {other}"))),
    }
}

/// Maps a kafka client error into the taxonomy: transport failures and
/// retryable protocol errors are transient, everything else is a general
/// backend error.
fn map_kafka_error(error: rskafka::client::error::Error) -> StorageError {
    use rskafka::client::error::{Error, ProtocolError};
    match &error {
        Error::Connection(_) => StorageError::transient(format!("kafka: {error}")),
        Error::ServerError {
            protocol_error:
                ProtocolError::RequestTimedOut
                | ProtocolError::NotEnoughReplicas
                | ProtocolError::NotEnoughReplicasAfterAppend
                | ProtocolError::UnknownServerError,
            ..
        } => StorageError::transient(format!("kafka: {error}")),
        _ => StorageError::backend(format!("kafka: {error}")),
    }
}

impl Journal for KafkaJournal {
    fn append_intent(&mut self, record: &[u8]) -> Result<Cursor, StorageError> {
        let cursor = self.append_record(KIND_INTENT, record)?;
        self.index.push(JournalKind::Intent(record.to_vec()));
        debug_assert_eq!(cursor.0, self.index.len() as u64);
        Ok(cursor)
    }

    fn commit(&mut self, intent: Cursor, record: &[u8]) -> Result<(), StorageError> {
        if intent.0 == 0 || intent.0 as usize > self.index.len() {
            return Err(StorageError::not_found(format!("intent at {intent:?}")));
        }
        if !matches!(self.index[intent.0 as usize - 1], JournalKind::Intent(_)) {
            return Err(StorageError::not_found(format!("intent at {intent:?}")));
        }
        let prior = self.index.iter().find_map(|kind| match kind {
            JournalKind::Commit { intent: settled, record: existing } if *settled == intent => {
                Some(existing)
            }
            _ => None,
        });
        match prior {
            Some(existing) if existing == record => Ok(()),
            Some(_) => Err(StorageError::corrupt(format!(
                "intent at {intent:?} already committed with a different record"
            ))),
            None => {
                let mut payload = Vec::with_capacity(8 + record.len());
                payload.extend_from_slice(&intent.0.to_be_bytes());
                payload.extend_from_slice(record);
                let cursor = self.append_record(KIND_COMMIT, &payload)?;
                self.index.push(JournalKind::Commit {
                    intent,
                    record: record.to_vec(),
                });
                debug_assert_eq!(cursor.0, self.index.len() as u64);
                Ok(())
            }
        }
    }

    fn replay(&self, since: Cursor) -> Result<Vec<JournalEntry>, StorageError> {
        Ok(self
            .index
            .iter()
            .enumerate()
            .skip(since.0 as usize)
            .map(|(index, kind)| JournalEntry {
                cursor: Cursor(index as u64 + 1),
                kind: kind.clone(),
            })
            .collect())
    }
}
