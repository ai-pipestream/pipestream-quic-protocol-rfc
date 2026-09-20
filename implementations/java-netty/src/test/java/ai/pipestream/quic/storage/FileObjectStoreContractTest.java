package ai.pipestream.quic.storage;

import java.nio.file.Path;
import org.junit.jupiter.api.io.TempDir;

/** Certifies {@link FileObjectStore} against the object-store contract. */
final class FileObjectStoreContractTest extends ObjectStoreContractTest {
  @TempDir Path directory;

  @Override protected ObjectStore openStore() throws StorageException {
    return FileObjectStore.open(directory);
  }
}
