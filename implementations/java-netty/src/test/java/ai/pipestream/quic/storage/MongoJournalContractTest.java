package ai.pipestream.quic.storage;

import java.util.UUID;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

/**
 * Certifies {@link MongoJournal} against the journal contract on a real
 * MongoDB server (see {@link MongoDocker} for the fixture lifecycle and the
 * docker requirement). Every test gets a fresh journal on its own database,
 * so cursors start at 1 exactly like the other backends' fresh stores.
 */
final class MongoJournalContractTest extends JournalContractTest {
  private static MongoDocker mongo;

  @BeforeAll static void startMongo() {
    mongo = MongoDocker.shared();
  }

  @AfterAll static void stopMongo() {
    MongoDocker.releaseShared();
  }

  @Override protected Journal openJournal() throws StorageException {
    return MongoJournal.open(mongo.connectionString(),
        "journal-" + UUID.randomUUID().toString().replace("-", ""));
  }
}
