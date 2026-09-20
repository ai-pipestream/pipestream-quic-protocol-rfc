package ai.pipestream.quic.storage;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;

/**
 * Durable {@link Journal} on Apache Kafka: journal role only, because payloads
 * do not belong in the log. One topic per journal with exactly one partition:
 * Kafka offsets are the natural global order within a partition, and a
 * multi-partition topic has no global order, so the single partition is what
 * pins the cross-backend cursor semantics (commits consume offsets in the same
 * global order as intents). The cursor is the record offset plus one, so a
 * fresh topic starts at cursor 1 exactly like the other backends.
 *
 * <p>Record format: the record key is the intent id in UTF-8; the value is one
 * domain octet (0 = intent, 1 = commit) followed by the payload, so every
 * intent and every commit is one record and Kafka's record atomicity applies:
 * a produce either lands whole or not at all, and a produce failure can leave
 * no partial record. The only ambiguity is a timeout after the broker accepted
 * the record; such a retried append can leave a benign identical duplicate in
 * the log, which open tolerates, while a conflicting duplicate is Corrupt.
 *
 * <p>Durability rule: the producer runs with {@code acks=all} and an operation
 * returns success only after the broker acknowledges the record; broker-side
 * flushing ({@code flush.ms}/{@code flush.messages}) and replication factor
 * are deployment configuration. Topics are created with delete retention of
 * {@code retention.ms=-1}: intents and commits are the audit and recovery
 * history, and compaction would destroy replay.
 *
 * <p>Idempotency: an in-memory index of every intent and commit is rebuilt by
 * replaying the topic at open and consulted before every append, so an
 * identical re-append or re-commit is a no-op success and the same key with
 * changed bytes is Corrupt without further log traffic. Replay serves from
 * that index, which is a faithful copy of the durable log.
 *
 * <p>The producer is thread-safe but the journal serializes all operations on
 * itself, matching the other backends' single-writer convention; one live
 * journal per bootstrap servers and topic may exist in a JVM.
 */
public final class KafkaJournal implements Journal {
  /** Kind octet of an intent record value. */
  static final byte KIND_INTENT = 0;
  /** Kind octet of a commit record value. */
  static final byte KIND_COMMIT = 1;
  private static final int PARTITION = 0;
  private static final Duration ADMIN_TIMEOUT = Duration.ofSeconds(30);
  private static final Duration REBUILD_POLL = Duration.ofSeconds(5);
  private static final Duration REBUILD_DEADLINE = Duration.ofSeconds(60);
  private static final Set<String> OPEN_IDENTITIES = ConcurrentHashMap.newKeySet();

  private static final class EntryState {
    final String intentId;
    final long cursor;
    final byte[] payload;
    long commitCursor;
    byte[] commit;

    EntryState(String intentId, long cursor, byte[] payload) {
      this.intentId = intentId;
      this.cursor = cursor;
      this.payload = payload;
    }
  }

  private final String identity;
  private final String bootstrapServers;
  private final String topic;
  private final TopicPartition topicPartition;
  private final KafkaProducer<byte[], byte[]> producer;
  private final Map<String, EntryState> byId = new HashMap<>();
  private final TreeMap<Long, EntryState> byCursor = new TreeMap<>();
  private volatile boolean closed;

  private KafkaJournal(String identity, String bootstrapServers, String topic,
      KafkaProducer<byte[], byte[]> producer) {
    this.identity = identity;
    this.bootstrapServers = bootstrapServers;
    this.topic = topic;
    this.topicPartition = new TopicPartition(topic, PARTITION);
    this.producer = producer;
  }

  /**
   * Opens an exclusively owned journal on one topic. A missing topic is
   * created with one partition, replication factor one, and infinite delete
   * retention; an existing topic must have exactly one partition, otherwise
   * the global cursor order cannot be pinned and open fails. The topic index
   * is rebuilt by replaying the log before the journal accepts operations.
   *
   * @param bootstrapServers broker bootstrap servers, for example {@code 127.0.0.1:9092}
   * @param topic journal topic; this journal owns it exclusively
   * @return exclusive journal handle
   * @throws StorageException {@code TRANSIENT} for broker communication failure, {@code FAILED} for a second live journal on the same identity or a multi-partition topic
   */
  public static KafkaJournal open(String bootstrapServers, String topic) throws StorageException {
    Objects.requireNonNull(bootstrapServers, "bootstrapServers");
    Objects.requireNonNull(topic, "topic");
    if (topic.isEmpty()) throw new IllegalArgumentException("topic must be non-empty");
    String identity = "kafka-journal\n" + bootstrapServers + "\n" + topic;
    if (!OPEN_IDENTITIES.add(identity)) {
      throw StorageException.failed("kafka journal already has a writer for this identity", null);
    }
    try {
      ensureTopic(bootstrapServers, topic);
      var journal = new KafkaJournal(identity, bootstrapServers, topic, createProducer(bootstrapServers));
      try {
        journal.rebuild();
      } catch (StorageException | RuntimeException failure) {
        try {
          journal.producer.close(Duration.ofSeconds(10));
        } catch (RuntimeException cleanup) {
          failure.addSuppressed(cleanup);
        }
        throw failure;
      }
      return journal;
    } catch (StorageException failure) {
      OPEN_IDENTITIES.remove(identity);
      throw failure;
    } catch (RuntimeException failure) {
      OPEN_IDENTITIES.remove(identity);
      throw StorageException.failed("kafka journal open failed", failure);
    }
  }

  @Override public synchronized long appendIntent(Intent intent) throws StorageException {
    Objects.requireNonNull(intent, "intent");
    checkOpen();
    EntryState existing = byId.get(intent.intentId());
    if (existing != null) {
      if (Arrays.equals(existing.payload, intent.record())) return existing.cursor;
      throw StorageException.corrupt("intent " + intent.intentId() + " already exists with different bytes");
    }
    byte[] payload = intent.record();
    long offset = produce(intent.intentId().getBytes(StandardCharsets.UTF_8), value(KIND_INTENT, payload));
    var entry = new EntryState(intent.intentId(), offset + 1, payload);
    byId.put(entry.intentId, entry);
    byCursor.put(entry.cursor, entry);
    return entry.cursor;
  }

  @Override public synchronized void commit(String intentId, byte[] commitRecord) throws StorageException {
    Objects.requireNonNull(intentId, "intentId");
    Objects.requireNonNull(commitRecord, "commitRecord");
    checkOpen();
    if (commitRecord.length < 1 || commitRecord.length > MAX_RECORD_BYTES) {
      throw new IllegalArgumentException("commit record must be 1 to " + MAX_RECORD_BYTES + " octets");
    }
    EntryState entry = byId.get(intentId);
    if (entry == null) {
      throw StorageException.notFound("intent " + intentId + " has no durable append");
    }
    if (entry.commit != null) {
      if (Arrays.equals(entry.commit, commitRecord)) return;
      throw StorageException.corrupt("intent " + intentId + " already carries a different commit");
    }
    long offset = produce(intentId.getBytes(StandardCharsets.UTF_8), value(KIND_COMMIT, commitRecord));
    entry.commitCursor = offset + 1;
    entry.commit = commitRecord.clone();
  }

  @Override public synchronized List<ReplayEntry> replay(long sinceCursor) throws StorageException {
    if (sinceCursor < 0) throw new IllegalArgumentException("replay cursor must be non-negative");
    checkOpen();
    List<ReplayEntry> replayed = new ArrayList<>();
    for (EntryState entry : byCursor.tailMap(sinceCursor, false).values()) {
      replayed.add(new ReplayEntry(entry.cursor,
          new Intent(entry.intentId, entry.payload), entry.commit == null ? null : entry.commit.clone()));
    }
    return List.copyOf(replayed);
  }

  /**
   * Flushes and closes the producer, then releases the single-writer identity.
   * Idempotent; a failed close still releases the identity.
   *
   * @throws StorageException {@code FAILED} if the final flush fails
   */
  @Override public synchronized void close() throws StorageException {
    if (closed) return;
    closed = true;
    try {
      producer.flush();
    } catch (RuntimeException failure) {
      throw StorageException.failed("kafka journal final flush failed", failure);
    } finally {
      try {
        producer.close(Duration.ofSeconds(10));
      } catch (RuntimeException failure) {
        throw StorageException.failed("kafka journal close failed", failure);
      } finally {
        OPEN_IDENTITIES.remove(identity);
      }
    }
  }

  private static void ensureTopic(String bootstrapServers, String topic) throws StorageException {
    Properties config = new Properties();
    config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    try (AdminClient admin = AdminClient.create(config)) {
      TopicDescription description;
      try {
        description = describe(admin, topic);
      } catch (ExecutionException failure) {
        if (!(failure.getCause() instanceof UnknownTopicOrPartitionException)) {
          throw mapAdminFailure(failure.getCause(), "kafka topic inspection");
        }
        try {
          admin.createTopics(List.of(new NewTopic(topic, 1, (short) 1)
              .configs(Map.of("retention.ms", "-1")))).all().get(ADMIN_TIMEOUT.getSeconds(), TimeUnit.SECONDS);
          return;
        } catch (ExecutionException creation) {
          if (!(creation.getCause() instanceof TopicExistsException)) {
            throw mapAdminFailure(creation.getCause(), "kafka topic creation");
          }
          try {
            description = describe(admin, topic);
          } catch (ExecutionException redescribe) {
            throw mapAdminFailure(redescribe.getCause(), "kafka topic inspection");
          }
        }
      } catch (TimeoutException failure) {
        throw StorageException.transientFailure("kafka topic inspection timed out", failure);
      } catch (InterruptedException failure) {
        Thread.currentThread().interrupt();
        throw StorageException.transientFailure("kafka topic inspection interrupted", failure);
      }
      if (description.partitions().size() != 1) {
        throw StorageException.failed(
            "kafka journal topic must have exactly one partition for the global cursor order", null);
      }
    } catch (StorageException failure) {
      throw failure;
    } catch (Exception failure) {
      throw StorageException.transientFailure("kafka topic administration failed", failure);
    }
  }

  private static TopicDescription describe(AdminClient admin, String topic)
      throws InterruptedException, ExecutionException, java.util.concurrent.TimeoutException {
    return admin.describeTopics(List.of(topic)).topicNameValues().get(topic)
        .get(ADMIN_TIMEOUT.getSeconds(), TimeUnit.SECONDS);
  }

  private static KafkaProducer<byte[], byte[]> createProducer(String bootstrapServers) {
    Properties config = new Properties();
    config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    config.put(ProducerConfig.ACKS_CONFIG, "all");
    config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
    config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    config.put(ProducerConfig.CLIENT_ID_CONFIG, "pipestream-storage-journal-" + UUID.randomUUID());
    return new KafkaProducer<>(config);
  }

  /** Rebuilds the intent and commit index by replaying the topic from the beginning. */
  private void rebuild() throws StorageException {
    Properties config = new Properties();
    config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(config)) {
      consumer.assign(List.of(topicPartition));
      consumer.seekToBeginning(List.of(topicPartition));
      long end = consumer.endOffsets(List.of(topicPartition)).get(topicPartition);
      long deadline = System.nanoTime() + REBUILD_DEADLINE.toNanos();
      while (consumer.position(topicPartition) < end) {
        if (System.nanoTime() > deadline) {
          throw StorageException.failed("kafka journal rebuild timed out", null);
        }
        ConsumerRecords<byte[], byte[]> records = consumer.poll(REBUILD_POLL);
        for (ConsumerRecord<byte[], byte[]> record : records.records(topicPartition)) {
          apply(record);
        }
      }
    } catch (StorageException | RuntimeException failure) {
      throw failure;
    } catch (Exception failure) {
      throw StorageException.transientFailure("kafka journal rebuild failed", failure);
    }
  }

  private void apply(ConsumerRecord<byte[], byte[]> record) throws StorageException {
    if (record.key() == null || record.value() == null || record.value().length < 2) {
      throw StorageException.corrupt("kafka journal record has invalid geometry");
    }
    String intentId = new String(record.key(), StandardCharsets.UTF_8);
    byte[] payload = Arrays.copyOfRange(record.value(), 1, record.value().length);
    if (intentId.getBytes(StandardCharsets.UTF_8).length > MAX_INTENT_ID_BYTES
        || payload.length < 1 || payload.length > MAX_RECORD_BYTES) {
      throw StorageException.corrupt("kafka journal record exceeds supported bounds");
    }
    long cursor = record.offset() + 1;
    if (record.value()[0] == KIND_INTENT) {
      EntryState existing = byId.get(intentId);
      if (existing != null) {
        // Benign duplicate from a produce that timed out after the broker
        // accepted it and was retried; a conflicting duplicate is Corrupt.
        if (!Arrays.equals(existing.payload, payload)) {
          throw StorageException.corrupt("kafka journal replays intent identifier " + intentId + " with changed bytes");
        }
        return;
      }
      var entry = new EntryState(intentId, cursor, payload);
      byId.put(intentId, entry);
      byCursor.put(cursor, entry);
    } else if (record.value()[0] == KIND_COMMIT) {
      EntryState entry = byId.get(intentId);
      if (entry == null) {
        throw StorageException.corrupt("kafka journal commit has no intent " + intentId);
      }
      if (entry.commit != null) {
        if (!Arrays.equals(entry.commit, payload)) {
          throw StorageException.corrupt("kafka journal replays conflicting commits for " + intentId);
        }
        return;
      }
      entry.commitCursor = cursor;
      entry.commit = payload;
    } else {
      throw StorageException.corrupt("kafka journal record carries an unknown kind");
    }
  }

  private long produce(byte[] key, byte[] value) throws StorageException {
    try {
      return producer.send(new ProducerRecord<>(topic, PARTITION, key, value)).get().offset();
    } catch (InterruptedException failure) {
      Thread.currentThread().interrupt();
      throw StorageException.failed("kafka append interrupted", failure);
    } catch (ExecutionException failure) {
      Throwable cause = failure.getCause() == null ? failure : failure.getCause();
      if (cause instanceof RetriableException) {
        throw StorageException.transientFailure("kafka append is retryable", cause);
      }
      throw StorageException.failed("kafka append failed", cause);
    }
  }

  private static StorageException mapAdminFailure(Throwable cause, String operation) {
    if (cause instanceof RetriableException) {
      return StorageException.transientFailure(operation + " is retryable", cause);
    }
    return StorageException.failed(operation + " failed", cause);
  }

  private static byte[] value(byte kind, byte[] payload) {
    byte[] value = new byte[payload.length + 1];
    value[0] = kind;
    System.arraycopy(payload, 0, value, 1, payload.length);
    return value;
  }

  private void checkOpen() throws StorageException {
    if (closed) throw StorageException.failed("journal is closed", null);
  }
}
