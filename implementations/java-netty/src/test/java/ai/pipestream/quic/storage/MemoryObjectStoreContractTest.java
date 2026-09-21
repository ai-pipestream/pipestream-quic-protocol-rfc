package ai.pipestream.quic.storage;

/** Certifies {@link MemoryObjectStore} against the object-store contract. */
final class MemoryObjectStoreContractTest extends ObjectStoreContractTest {
  @Override protected ObjectStore openStore() {
    return MemoryObjectStore.create();
  }
}
