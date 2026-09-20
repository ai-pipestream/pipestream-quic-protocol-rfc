package ai.pipestream.quic.storage;

import java.util.UUID;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

/**
 * Certifies {@link KafkaJournal} against the journal contract on a real
 * Apache Kafka broker (see {@link KafkaDocker} for the fixture lifecycle and
 * the docker requirement). Every test gets a fresh journal on its own topic,
 * so isolation needs no cleanup.
 */
final class KafkaJournalContractTest extends JournalContractTest {
  private static KafkaDocker kafka;

  @BeforeAll static void startKafka() {
    kafka = KafkaDocker.shared();
  }

  @AfterAll static void stopKafka() {
    KafkaDocker.releaseShared();
  }

  @Override protected Journal openJournal() throws StorageException {
    return KafkaJournal.open(kafka.bootstrapServers(), "journal-contract-" + UUID.randomUUID());
  }
}
