package ai.pipestream.quic.storage;

import java.nio.file.Path;
import org.junit.jupiter.api.io.TempDir;

/** Runs the {@link ConcurrencyStormObjectStoreTest} storm against the durable filesystem store. */
final class ConcurrencyStormFileObjectStoreTest extends ConcurrencyStormObjectStoreTest {
  @TempDir Path directory;

  @Override protected ObjectStore openStore() throws StorageException {
    return FileObjectStore.open(directory);
  }
}
