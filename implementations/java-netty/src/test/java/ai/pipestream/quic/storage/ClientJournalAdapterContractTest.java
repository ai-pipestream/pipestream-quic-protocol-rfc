package ai.pipestream.quic.storage;

import java.nio.file.Path;
import java.util.UUID;
import org.junit.jupiter.api.io.TempDir;

/**
 * Certifies {@link ClientJournalAdapter} against the journal contract on a
 * real v2 client journal file per test, no docker required. Every test gets
 * a freshly initialized file so cursors start at 1 exactly like the other
 * backends' fresh stores.
 */
final class ClientJournalAdapterContractTest extends JournalContractTest {
  @TempDir Path directory;

  @Override protected Journal openJournal() throws Exception {
    return ClientJournalAdapter.initialize(directory.resolve(UUID.randomUUID() + ".sqlite"));
  }
}
