package ai.pipestream.quic.storage;

import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.io.TempDir;

/** Certifies {@link FileObjectStore} against the object-store contract. */
final class FileObjectStoreContractTest extends ObjectStoreContractTest {
  @TempDir Path directory;

  @Override protected ObjectStore openStore() throws StorageException {
    return FileObjectStore.open(directory);
  }

  @Override protected boolean supportsRecordTamper() {
    return true;
  }

  @Override protected void tamperRecord(ScopePath scope, long entityId) throws Exception {
    Path leaf = directory.resolve("objects");
    for (long component : scope.components()) {
      leaf = leaf.resolve(StoreLayout.scopeComponentName(component));
    }
    Path entity = leaf.resolve(StoreLayout.fanoutBucket(entityId)).resolve(StoreLayout.entityName(entityId));
    byte[] content = Files.readAllBytes(entity);
    content[content.length - 1] ^= 1;
    Files.write(entity, content);
  }
}
