//! Kafka backend implementing the [`Journal`] role only.
//!
//! Per the design note's backend matrix, payloads do not belong in the log;
//! pair this journal with an object-store backend (filesystem, Postgres,
//! S3) for the two-system configuration: object write first, journal commit
//! second, reconcile on recovery by replaying.
//!
//! Physical mapping: one Kafka topic with exactly one partition (a single
//! partition is what makes "ordered replay" meaningful in Kafka). Record
//! values frame the same fields as the filesystem journal, minus the
//! cursor and checksum a partition log already provides:
//! `u8 type | u32 BE id length | id | payload`, type 0 = intent, type 1 =
//! commit (payload = commit record). A record's cursor is `offset + 1`, so
//! `Cursor(0)` stays "the beginning"; commits consume cursor positions just
//! like on the log backends, and replay folds each commit into its intent's
//! entry.
//!
//! Durability configuration: produces use `required_acks = -1` (all
//! in-sync replicas) and every append awaits the broker's acknowledgment,
//! so a returned cursor survives a process kill.
//!
//! The journal assumes the reference deployment's single-writer recovery
//! model: [`KafkaJournal::open`] loads the whole partition into an
//! in-memory index, appends go through this instance, and `replay` reads
//! the index.
//!
//! The client (`rskafka`, pure Rust) is async; the store owns a private
//! current-thread tokio runtime and blocks on each call, keeping the SPI
//! synchronous.

use crate::{
    Cursor, Intent, Journal, MAX_INTENT_ID_BYTES, MAX_RECORD_BYTES, ReplayEntry, StorageError,
};
use rskafka::client::ClientBuilder;
use rskafka::client::partition::{Compression, UnknownTopicHandling};
use rskafka::record::Record;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

const TYPE_INTENT: u8 = 0;
const TYPE_COMMIT: u8 = 1;
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
    topic: Arc<str>,
    /// In-memory mirror of the partition, loaded at open.
    index: JournalIndex,
}

#[derive(Debug, Default)]
struct JournalIndex {
    intents: Vec<IntentState>,
    by_intent_id: HashMap<String, usize>,
    last_cursor: u64,
}

#[derive(Debug)]
struct IntentState {
    intent: Intent,
    cursor: Cursor,
    commit: Option<Vec<u8>>,
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
            .map_err(|e| StorageError::failed(format!("tokio runtime: {e}")))?;
        let client = runtime
            .block_on(ClientBuilder::new(vec![config.bootstrap_servers.clone()]).build())
            .map_err(map_kafka_error)?;
        let mut journal = KafkaJournal {
            client,
            runtime,
            topic: Arc::from(config.topic.as_str()),
            index: JournalIndex::default(),
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
        let result = self.block_on(controller.create_topic(self.topic.as_ref(), 1, 1, 30_000));
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
            self.topic.as_ref(),
            PARTITION,
            UnknownTopicHandling::Error,
        ))
        .map_err(map_kafka_error)
    }

    /// Fetches the whole partition into the in-memory index.
    fn load_index(&self) -> Result<JournalIndex, StorageError> {
        let partition = self.partition_client()?;
        let mut index = JournalIndex::default();
        let mut offset = 0i64;
        loop {
            let previous_offset = offset;
            let (records, high_watermark) = self
                .block_on(partition.fetch_records(offset, 1..16 * 1024 * 1024, 5_000))
                .map_err(map_kafka_error)?;
            for entry in records {
                let payload = entry.record.value.unwrap_or_default();
                fold_record(&mut index, &self.topic, entry.offset, &payload)?;
            }
            offset = index.last_cursor as i64;
            if offset >= high_watermark {
                return Ok(index);
            }
            if offset == previous_offset {
                return Err(StorageError::transient(format!(
                    "journal {} fetch made no progress at offset {offset}",
                    self.topic
                )));
            }
        }
    }

    /// Appends one framed record and awaits the broker's acknowledgment.
    /// Returns the record's cursor (`offset + 1`).
    fn append_record(&mut self, record_type: u8, id: &[u8], payload: &[u8]) -> Result<Cursor, StorageError> {
        let partition = self.partition_client()?;
        let mut value = Vec::with_capacity(5 + id.len() + payload.len());
        value.push(record_type);
        value.extend_from_slice(&(id.len() as u32).to_be_bytes());
        value.extend_from_slice(id);
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
            StorageError::failed("kafka produce returned no offset".to_string())
        })?;
        let cursor = u64::try_from(base_offset + 1)
            .map(Cursor)
            .map_err(|_| StorageError::corrupt("negative journal offset".to_string()))?;
        Ok(cursor)
    }
}

/// Validates one stored record and folds it into the index.
fn fold_record(
    index: &mut JournalIndex,
    topic: &str,
    offset: i64,
    payload: &[u8],
) -> Result<(), StorageError> {
    let corrupt = |detail: String| {
        StorageError::corrupt(format!("journal {topic} offset {offset}: {detail}"))
    };
    if payload.len() < 5 {
        return Err(corrupt("record shorter than its header".to_string()));
    }
    let record_type = payload[0];
    let mut id_len = [0u8; 4];
    id_len.copy_from_slice(&payload[1..5]);
    let id_len = u32::from_be_bytes(id_len) as usize;
    if id_len == 0 || id_len > MAX_INTENT_ID_BYTES || payload.len() < 5 + id_len {
        return Err(corrupt("record geometry is invalid".to_string()));
    }
    let id = &payload[5..5 + id_len];
    let body = &payload[5 + id_len..];
    if body.is_empty() || body.len() > MAX_RECORD_BYTES {
        return Err(corrupt("record payload is invalid".to_string()));
    }
    let intent_id = std::str::from_utf8(id)
        .map_err(|_| corrupt("intent id is not UTF-8".to_string()))?
        .to_string();
    let cursor = Cursor(u64::try_from(offset + 1).map_err(|_| corrupt("negative offset".to_string()))?);
    index.last_cursor = cursor.0;
    match record_type {
        TYPE_INTENT => {
            if index.by_intent_id.contains_key(&intent_id) {
                return Err(corrupt(format!("journal replays intent identifier {intent_id}")));
            }
            let intent = Intent::new(&intent_id, body)
                .map_err(|_| corrupt("intent fails validation".to_string()))?;
            index.by_intent_id.insert(intent_id, index.intents.len());
            index.intents.push(IntentState {
                intent,
                cursor,
                commit: None,
            });
        }
        TYPE_COMMIT => {
            let Some(intent_index) = index.by_intent_id.get(&intent_id).copied() else {
                return Err(corrupt(format!(
                    "journal commit has no uncommitted intent {intent_id}"
                )));
            };
            let entry = &mut index.intents[intent_index];
            if entry.commit.is_some() {
                return Err(corrupt(format!(
                    "journal commit has no uncommitted intent {intent_id}"
                )));
            }
            entry.commit = Some(body.to_vec());
        }
        other => return Err(corrupt(format!("unknown record kind {other}"))),
    }
    Ok(())
}

/// Maps a kafka client error into the taxonomy: transport failures and
/// retryable protocol errors are transient, everything else is a general
/// failure.
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
        _ => StorageError::failed(format!("kafka: {error}")),
    }
}

impl Journal for KafkaJournal {
    fn append_intent(&mut self, intent: &Intent) -> Result<Cursor, StorageError> {
        if let Some(index) = self.index.by_intent_id.get(intent.id()) {
            let existing = &self.index.intents[*index];
            if existing.intent.record() == intent.record() {
                return Ok(existing.cursor);
            }
            return Err(StorageError::corrupt(format!(
                "intent {} already exists with different bytes",
                intent.id()
            )));
        }
        let cursor = self.append_record(TYPE_INTENT, intent.id().as_bytes(), intent.record())?;
        self.index
            .by_intent_id
            .insert(intent.id().to_string(), self.index.intents.len());
        self.index.intents.push(IntentState {
            intent: intent.clone(),
            cursor,
            commit: None,
        });
        self.index.last_cursor = cursor.0;
        Ok(cursor)
    }

    fn commit(&mut self, intent_id: &str, record: &[u8]) -> Result<(), StorageError> {
        if record.is_empty() || record.len() > MAX_RECORD_BYTES {
            return Err(StorageError::failed(format!(
                "commit record must be 1 to {MAX_RECORD_BYTES} octets"
            )));
        }
        let Some(index) = self.index.by_intent_id.get(intent_id).copied() else {
            return Err(StorageError::not_found(format!(
                "intent {intent_id} has no durable append"
            )));
        };
        match &self.index.intents[index].commit {
            Some(existing) if existing == record => return Ok(()),
            Some(_) => {
                return Err(StorageError::corrupt(format!(
                    "intent {intent_id} already carries a different commit"
                )));
            }
            None => {}
        }
        let cursor = self.append_record(TYPE_COMMIT, intent_id.as_bytes(), record)?;
        self.index.intents[index].commit = Some(record.to_vec());
        self.index.last_cursor = cursor.0;
        Ok(())
    }

    fn replay(&self, since: Cursor) -> Result<Vec<ReplayEntry>, StorageError> {
        Ok(self
            .index
            .intents
            .iter()
            .filter(|entry| entry.cursor > since)
            .map(|entry| ReplayEntry {
                cursor: entry.cursor,
                intent: entry.intent.clone(),
                commit: entry.commit.clone(),
            })
            .collect())
    }
}
