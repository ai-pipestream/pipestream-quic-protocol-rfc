package ai.pipestream.quic.storage;

import java.nio.file.Path;
import org.junit.jupiter.api.io.TempDir;

/** Certifies {@link FileJournal} against the journal contract. */
final class FileJournalContractTest extends JournalContractTest {
  @TempDir Path directory;

  @Override protected Journal openJournal() throws StorageException {
    return FileJournal.open(directory);
  }
}
