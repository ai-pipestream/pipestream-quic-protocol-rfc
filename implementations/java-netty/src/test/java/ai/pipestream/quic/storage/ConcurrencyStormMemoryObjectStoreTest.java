package ai.pipestream.quic.storage;

/** Runs the {@link ConcurrencyStormObjectStoreTest} storm against the volatile in-memory store. */
final class ConcurrencyStormMemoryObjectStoreTest extends ConcurrencyStormObjectStoreTest {
  @Override protected ObjectStore openStore() {
    return MemoryObjectStore.create();
  }
}
