package ai.pipestream.quic.storage;

import static org.junit.jupiter.api.Assertions.*;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

/**
 * Role contract every {@link ObjectStore} backend must satisfy. Concrete
 * subclasses instantiate one backend each; every semantic of the storage
 * design note is exercised here exactly once.
 */
abstract class ObjectStoreContractTest {
  /** Opens one fresh, empty store for a single test. */
  protected abstract ObjectStore openStore() throws Exception;

  static StorageException assertStorage(StorageException.Kind kind, Executable call) {
    StorageException failure = assertThrows(StorageException.class, call);
    assertEquals(kind, failure.kind(), failure.getMessage());
    return failure;
  }

  static byte[] bytes(String value) {
    return value.getBytes(StandardCharsets.UTF_8);
  }

  static byte[] digest(byte[] bytes) {
    return StoreLayout.sha256().digest(bytes);
  }

  static byte[] readAll(InputStream input) throws Exception {
    try (InputStream stream = input) {
      return stream.readAllBytes();
    }
  }

  @Test void putThenOpenReadReturnsIdenticalBytes() throws Exception {
    try (ObjectStore store = openStore()) {
      var scope = ScopePath.of(0x10, 0x20, 0x30);
      byte[] payload = bytes("entity-payload");
      store.put(scope, 42, payload, digest(payload));
      payload[0] ^= 1;
      assertArrayEquals(bytes("entity-payload"), readAll(store.openRead(scope, 42)));
    }
  }

  @Test void putRejectsDigestMismatchAsCorrupt() throws Exception {
    try (ObjectStore store = openStore()) {
      var scope = ScopePath.root(1);
      byte[] payload = bytes("content");
      assertStorage(StorageException.Kind.CORRUPT,
          () -> store.put(scope, 1, payload, new byte[32]));
      assertStorage(StorageException.Kind.NOT_FOUND, () -> store.openRead(scope, 1));
    }
  }

  @Test void putIsIdempotentForIdenticalBytesAndDigest() throws Exception {
    try (ObjectStore store = openStore()) {
      var scope = ScopePath.root(2);
      byte[] payload = bytes("same-bytes");
      store.put(scope, 7, payload, digest(payload));
      store.put(scope, 7, bytes("same-bytes"), digest(payload));
      assertArrayEquals(bytes("same-bytes"), readAll(store.openRead(scope, 7)));
      assertEquals(List.of(7L), store.list(scope));
    }
  }

  @Test void putConflictingBytesIsCorruptAndKeepsOriginalContent() throws Exception {
    try (ObjectStore store = openStore()) {
      var scope = ScopePath.root(3);
      byte[] original = bytes("original");
      store.put(scope, 9, original, digest(original));
      byte[] conflicting = bytes("conflicting");
      assertStorage(StorageException.Kind.CORRUPT,
          () -> store.put(scope, 9, conflicting, digest(conflicting)));
      assertArrayEquals(bytes("original"), readAll(store.openRead(scope, 9)));
    }
  }

  @Test void putIntoSealedScopeIsAlreadySealedEvenForIdenticalBytes() throws Exception {
    try (ObjectStore store = openStore()) {
      var scope = ScopePath.root(4);
      byte[] payload = bytes("sealed-content");
      store.put(scope, 5, payload, digest(payload));
      store.sealScope(scope, digest(payload), 1);
      assertStorage(StorageException.Kind.ALREADY_SEALED,
          () -> store.put(scope, 5, bytes("sealed-content"), digest(payload)));
      assertStorage(StorageException.Kind.ALREADY_SEALED,
          () -> store.put(scope, 6, bytes("new-content"), digest(bytes("new-content"))));
    }
  }

  @Test void openReadOfMissingEntityIsNotFound() throws Exception {
    try (ObjectStore store = openStore()) {
      assertStorage(StorageException.Kind.NOT_FOUND,
          () -> store.openRead(ScopePath.root(5), 100));
      var scope = ScopePath.root(5);
      byte[] payload = bytes("other");
      store.put(scope, 100, payload, digest(payload));
      assertStorage(StorageException.Kind.NOT_FOUND, () -> store.openRead(scope, 101));
    }
  }

  @Test void openReadSurvivesRetirementOfUnsealedObject() throws Exception {
    try (ObjectStore store = openStore()) {
      var scope = ScopePath.root(6);
      byte[] payload = bytes("outlives-removal");
      store.put(scope, 77, payload, digest(payload));
      InputStream live = store.openRead(scope, 77);
      store.retire(scope, 77);
      assertArrayEquals(bytes("outlives-removal"), readAll(live));
      assertStorage(StorageException.Kind.NOT_FOUND, () -> store.openRead(scope, 77));
    }
  }

  @Test void sealRecordsDigestAndCountAndVerifyScopeMatches() throws Exception {
    try (ObjectStore store = openStore()) {
      var scope = ScopePath.of(7, 8);
      var payloads = List.of(bytes("alpha"), bytes("beta"), bytes("gamma"));
      var digests = new ArrayList<StoreLayout.EntityDigest>();
      long entityId = 1;
      for (byte[] payload : payloads) {
        store.put(scope, entityId, payload, digest(payload));
        digests.add(new StoreLayout.EntityDigest(entityId, digest(payload)));
        entityId++;
      }
      store.sealScope(scope, StoreLayout.scopeDigest(digests), payloads.size());
      store.verifyScope(scope);
      assertArrayEquals(bytes("beta"), readAll(store.openRead(scope, 2)));
      assertEquals(List.of(1L, 2L, 3L), store.list(scope));
    }
  }

  @Test void sealIsIdempotentAndRejectsConflictingSeal() throws Exception {
    try (ObjectStore store = openStore()) {
      var scope = ScopePath.root(9);
      byte[] payload = bytes("sealed");
      store.put(scope, 1, payload, digest(payload));
      byte[] seal = digest(payload);
      store.sealScope(scope, seal, 1);
      store.sealScope(scope, seal.clone(), 1);
      assertStorage(StorageException.Kind.CORRUPT, () -> store.sealScope(scope, new byte[32], 1));
      assertStorage(StorageException.Kind.CORRUPT, () -> store.sealScope(scope, seal, 2));
    }
  }

  @Test void verifyScopeFailsAsCorruptOnRecordedDigestMismatch() throws Exception {
    try (ObjectStore store = openStore()) {
      var scope = ScopePath.root(10);
      byte[] payload = bytes("mismatched");
      store.put(scope, 1, payload, digest(payload));
      store.sealScope(scope, new byte[32], 1);
      assertStorage(StorageException.Kind.CORRUPT, () -> store.verifyScope(scope));
    }
  }

  @Test void verifyScopeFailsAsCorruptOnEntityCountMismatch() throws Exception {
    try (ObjectStore store = openStore()) {
      var scope = ScopePath.root(11);
      byte[] payload = bytes("counted");
      store.put(scope, 1, payload, digest(payload));
      store.sealScope(scope, StoreLayout.scopeDigest(List.of()), 1);
      assertStorage(StorageException.Kind.CORRUPT, () -> store.verifyScope(scope));
    }
  }

  @Test void verifyScopeWithoutSealIsNotFound() throws Exception {
    try (ObjectStore store = openStore()) {
      var scope = ScopePath.root(12);
      byte[] payload = bytes("unsealed");
      store.put(scope, 1, payload, digest(payload));
      assertStorage(StorageException.Kind.NOT_FOUND, () -> store.verifyScope(scope));
    }
  }

  @Test void listReturnsAscendingIdsAndEmptyForUnknownScope() throws Exception {
    try (ObjectStore store = openStore()) {
      assertEquals(List.of(), store.list(ScopePath.root(13)));
      var scope = ScopePath.root(13);
      for (long entityId : new long[] {9, 1, 5}) {
        byte[] payload = bytes("entity-" + entityId);
        store.put(scope, entityId, payload, digest(payload));
      }
      assertEquals(List.of(1L, 5L, 9L), store.list(scope));
    }
  }

  @Test void retireOfUnsealedScopeRemovesOnlyEntitiesUpToCursor() throws Exception {
    try (ObjectStore store = openStore()) {
      var scope = ScopePath.root(14);
      for (long entityId = 1; entityId <= 4; entityId++) {
        byte[] payload = bytes("entity-" + entityId);
        store.put(scope, entityId, payload, digest(payload));
      }
      store.retire(scope, 2);
      assertEquals(List.of(3L, 4L), store.list(scope));
      assertStorage(StorageException.Kind.NOT_FOUND, () -> store.openRead(scope, 2));
      assertArrayEquals(bytes("entity-3"), readAll(store.openRead(scope, 3)));
      store.retire(scope, 3);
      assertEquals(List.of(4L), store.list(scope));
    }
  }

  @Test void retireOfSealedScopeRemovesWholeScope() throws Exception {
    try (ObjectStore store = openStore()) {
      var scope = ScopePath.root(15);
      byte[] payload = bytes("retired-whole");
      store.put(scope, 1, payload, digest(payload));
      store.sealScope(scope, digest(payload), 1);
      store.retire(scope, 0);
      assertEquals(List.of(), store.list(scope));
      assertStorage(StorageException.Kind.NOT_FOUND, () -> store.openRead(scope, 1));
      assertStorage(StorageException.Kind.NOT_FOUND, () -> store.verifyScope(scope));
      assertStorage(StorageException.Kind.NOT_FOUND, () -> store.retire(scope, 0));
    }
  }

  @Test void retireOfUnknownScopeIsNotFound() throws Exception {
    try (ObjectStore store = openStore()) {
      assertStorage(StorageException.Kind.NOT_FOUND, () -> store.retire(ScopePath.root(16), 0));
    }
  }

  @Test void operationsFailAfterClose() throws Exception {
    ObjectStore store = openStore();
    var scope = ScopePath.root(17);
    byte[] payload = bytes("closed");
    store.put(scope, 1, payload, digest(payload));
    store.close();
    store.close();
    assertStorage(StorageException.Kind.FAILED, () -> store.put(scope, 2, payload, digest(payload)));
    assertStorage(StorageException.Kind.FAILED, () -> store.openRead(scope, 1));
    assertStorage(StorageException.Kind.FAILED, () -> store.sealScope(scope, digest(payload), 1));
    assertStorage(StorageException.Kind.FAILED, () -> store.verifyScope(scope));
    assertStorage(StorageException.Kind.FAILED, () -> store.list(scope));
    assertStorage(StorageException.Kind.FAILED, () -> store.retire(scope, 0));
  }
}
