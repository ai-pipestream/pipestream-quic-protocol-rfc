package ai.pipestream.quic.storage;

import static ai.pipestream.quic.storage.ObjectStoreContractTest.assertStorage;
import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Kafka-backend specifics on a real Apache Kafka broker: restart persistence
 * with replay from cursor 0, cursor continuation across reopen, second-opener
 * behavior, and the single-partition requirement that pins the global cursor
 * order. See {@link KafkaDocker} for the fixture lifecycle and the docker
 * requirement.
 */
final class KafkaBackendTest {
  private static KafkaDocker kafka;

  @BeforeAll static void startKafka() {
    kafka = KafkaDocker.shared();
  }

  @AfterAll static void stopKafka() {
    KafkaDocker.releaseShared();
  }

  private static byte[] bytes(String value) {
    return value.getBytes(java.nio.charset.StandardCharsets.UTF_8);
  }

  @Test void replayPersistsAcrossBrokerRestart() throws Exception {
    String topic = "journal-restart-" + UUID.randomUUID();
    try (Journal journal = KafkaJournal.open(kafka.bootstrapServers(), topic)) {
      assertEquals(1, journal.appendIntent(new Journal.Intent("a", bytes("record-a"))));
      journal.commit("a", bytes("commit-a"));
      assertEquals(3, journal.appendIntent(new Journal.Intent("b", bytes("record-b"))));
      kafka.stopServer();
    }
    kafka.startServer();
    try (Journal reopened = KafkaJournal.open(kafka.bootstrapServers(), topic)) {
      List<Journal.ReplayEntry> replayed = reopened.replay(0);
      assertEquals(List.of("a", "b"),
          replayed.stream().map(entry -> entry.intent().intentId()).toList());
      assertArrayEquals(bytes("commit-a"), replayed.get(0).commitRecord());
      assertNull(replayed.get(1).commitRecord());
      assertEquals(List.of("b"),
          reopened.replay(2).stream().map(entry -> entry.intent().intentId()).toList());
      assertEquals(4, reopened.appendIntent(new Journal.Intent("c", bytes("record-c"))),
          "offsets continue across broker restart");
    }
  }

  @Test void secondOpenerIsRefusedUntilClose() throws Exception {
    String topic = "journal-second-" + UUID.randomUUID();
    Journal first = KafkaJournal.open(kafka.bootstrapServers(), topic);
    assertStorage(StorageException.Kind.FAILED, () -> KafkaJournal.open(kafka.bootstrapServers(), topic));
    first.close();
    try (Journal second = KafkaJournal.open(kafka.bootstrapServers(), topic)) {
      assertEquals(List.of(), second.replay(0));
    }
  }

  @Test void multiPartitionTopicIsRefused() throws Exception {
    String topic = "journal-multipartition-" + UUID.randomUUID();
    kafka.createTopic(topic, 2);
    assertStorage(StorageException.Kind.FAILED, () -> KafkaJournal.open(kafka.bootstrapServers(), topic));
  }

  @Test void missingTopicIsCreatedWithSinglePartition() throws Exception {
    String topic = "journal-created-" + UUID.randomUUID();
    try (Journal journal = KafkaJournal.open(kafka.bootstrapServers(), topic)) {
      assertEquals(1, journal.appendIntent(new Journal.Intent("first", bytes("record"))));
      assertEquals(List.of("first"),
          journal.replay(0).stream().map(entry -> entry.intent().intentId()).toList());
    }
  }
}
