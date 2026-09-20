package ai.pipestream.quic.storage;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * Volatile in-memory {@link ObjectStore}: the test and development backbone and
 * the fast lane for conformance fuzzing. Content is content-verified and seals
 * are atomic exactly like the durable backends, but nothing survives process
 * exit. Thread-safe; blocking calls are constant time.
 */
public final class MemoryObjectStore implements ObjectStore {
  private record Entity(byte[] bytes, byte[] digest) {}
  private record Seal(byte[] digest, long entityCount) {}

  private static final class ScopeState {
    final ConcurrentSkipListMap<Long, Entity> entities = new ConcurrentSkipListMap<>();
    volatile Seal seal;
  }

  private final Map<ScopePath, ScopeState> scopes = new ConcurrentHashMap<>();
  private volatile boolean closed;

  private MemoryObjectStore() {}

  /**
   * Creates an empty volatile store.
   * @return in-memory object store
   */
  public static MemoryObjectStore create() {
    return new MemoryObjectStore();
  }

  @Override public void put(ScopePath scope, long entityId, byte[] bytes, byte[] expectedDigest)
      throws StorageException {
    Objects.requireNonNull(scope, "scope");
    Objects.requireNonNull(bytes, "bytes");
    validateEntityId(entityId);
    byte[] expected = validateDigest(expectedDigest);
    checkOpen();
    ScopeState state = scopes.computeIfAbsent(scope, key -> new ScopeState());
    synchronized (state) {
      if (state.seal != null) {
        throw StorageException.alreadySealed("scope " + scope.render() + " is sealed");
      }
      byte[] actual = StoreLayout.sha256().digest(bytes);
      if (!MessageDigest.isEqual(actual, expected)) {
        throw StorageException.corrupt("put digest mismatch for entity " + entityId);
      }
      Entity existing = state.entities.get(entityId);
      if (existing != null) {
        if (Arrays.equals(existing.bytes, bytes)) return;
        throw StorageException.corrupt("conflicting content for entity " + entityId);
      }
      state.entities.put(entityId, new Entity(bytes.clone(), expected.clone()));
    }
  }

  @Override public InputStream openRead(ScopePath scope, long entityId) throws StorageException {
    Objects.requireNonNull(scope, "scope");
    validateEntityId(entityId);
    checkOpen();
    ScopeState state = scopes.get(scope);
    Entity entity = state == null ? null : state.entities.get(entityId);
    if (entity == null) {
      throw StorageException.notFound("entity " + entityId + " is absent from scope " + scope.render());
    }
    return new ByteArrayInputStream(entity.bytes.clone());
  }

  @Override public void sealScope(ScopePath scope, byte[] digest, long entityCount) throws StorageException {
    Objects.requireNonNull(scope, "scope");
    byte[] recorded = validateDigest(digest);
    if (entityCount < 0) throw new IllegalArgumentException("entity count must be non-negative");
    checkOpen();
    ScopeState state = scopes.computeIfAbsent(scope, key -> new ScopeState());
    synchronized (state) {
      Seal existing = state.seal;
      if (existing != null) {
        if (existing.entityCount == entityCount && Arrays.equals(existing.digest, recorded)) return;
        throw StorageException.corrupt("scope " + scope.render() + " already carries a different seal");
      }
      state.seal = new Seal(recorded.clone(), entityCount);
    }
  }

  @Override public SealRecord verifyScope(ScopePath scope) throws StorageException {
    Objects.requireNonNull(scope, "scope");
    checkOpen();
    ScopeState state = scopes.get(scope);
    Seal seal = state == null ? null : state.seal;
    if (seal == null) {
      throw StorageException.notFound("scope " + scope.render() + " has no seal record");
    }
    for (Map.Entry<Long, Entity> entity : state.entities.entrySet()) {
      byte[] actual = StoreLayout.sha256().digest(entity.getValue().bytes);
      if (!MessageDigest.isEqual(actual, entity.getValue().digest)) {
        throw StorageException.corrupt("entity " + entity.getKey() + " fails its recorded digest");
      }
    }
    if (state.entities.size() != seal.entityCount) {
      throw StorageException.corrupt("scope " + scope.render() + " entity count differs from its seal");
    }
    return new SealRecord(seal.digest, seal.entityCount);
  }

  @Override public List<Long> list(ScopePath scope) throws StorageException {
    Objects.requireNonNull(scope, "scope");
    checkOpen();
    ScopeState state = scopes.get(scope);
    return state == null ? List.of() : List.copyOf(state.entities.keySet());
  }

  @Override public List<ScopePath> listScopes() throws StorageException {
    checkOpen();
    List<ScopePath> listed = new ArrayList<>();
    for (Map.Entry<ScopePath, ScopeState> entry : scopes.entrySet()) {
      ScopeState state = entry.getValue();
      if (state.seal != null || !state.entities.isEmpty()) listed.add(entry.getKey());
    }
    listed.sort(java.util.Comparator.comparing(ScopePath::render));
    return List.copyOf(listed);
  }

  @Override public void retire(ScopePath scope, long retentionCursor) throws StorageException {
    Objects.requireNonNull(scope, "scope");
    if (retentionCursor < 0) throw new IllegalArgumentException("retention cursor must be non-negative");
    checkOpen();
    ScopeState state = scopes.get(scope);
    if (state == null || (state.entities.isEmpty() && state.seal == null)) {
      throw StorageException.notFound("scope " + scope.render() + " holds nothing to retire");
    }
    synchronized (state) {
      if (state.seal != null) {
        scopes.remove(scope, state);
      } else {
        state.entities.headMap(retentionCursor, true).clear();
      }
    }
  }

  @Override public void close() {
    closed = true;
    scopes.clear();
  }

  private void checkOpen() throws StorageException {
    if (closed) throw StorageException.failed("object store is closed", null);
  }

  private static void validateEntityId(long entityId) {
    if (entityId < 0 || entityId > StoreLayout.MAX_ENTITY_ID) {
      throw new IllegalArgumentException("entity identifier is not unsigned 32-bit");
    }
  }

  private static byte[] validateDigest(byte[] digest) {
    Objects.requireNonNull(digest, "digest");
    if (digest.length != StoreLayout.DIGEST_BYTES) {
      throw new IllegalArgumentException("digest must be " + StoreLayout.DIGEST_BYTES + " octets");
    }
    return digest;
  }
}
