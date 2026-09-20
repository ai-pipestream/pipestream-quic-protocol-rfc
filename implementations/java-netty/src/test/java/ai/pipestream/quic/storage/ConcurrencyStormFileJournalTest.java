package ai.pipestream.quic.storage;

import java.nio.file.Path;
import org.junit.jupiter.api.io.TempDir;

/** Runs the {@link ConcurrencyStormJournalTest} storm against the durable filesystem journal. */
final class ConcurrencyStormFileJournalTest extends ConcurrencyStormJournalTest {
  @TempDir Path directory;

  @Override protected Journal openJournal() throws StorageException {
    return FileJournal.open(directory);
  }
}
