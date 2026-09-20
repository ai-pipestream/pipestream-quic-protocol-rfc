package ai.pipestream.quic.storage;

import static org.junit.jupiter.api.Assertions.*;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import static org.junit.jupiter.api.Assumptions.assumeTrue;

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
      byte[] callerDigest = StoreLayout.scopeDigest(digests);
      store.sealScope(scope, callerDigest, payloads.size());
      ObjectStore.SealRecord seal = store.verifyScope(scope);
      assertArrayEquals(callerDigest, seal.digest(), "the recorded SCOPE_DIGEST comes back verbatim");
      assertEquals(payloads.size(), seal.entityCount());
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

  @Test void verifyScopeReturnsRecordedSealWithoutRecomputingCallerDigest() throws Exception {
    try (ObjectStore store = openStore()) {
      var scope = ScopePath.root(10);
      byte[] payload = bytes("mismatched");
      store.put(scope, 1, payload, digest(payload));
      // Deliberately not the canonical content digest: the store records the
      // caller's SCOPE_DIGEST verbatim and returns it; comparing it against
      // the protocol expectation is the caller's job.
      byte[] callerDigest = new byte[32];
      callerDigest[0] = 0x42;
      store.sealScope(scope, callerDigest, 1);
      ObjectStore.SealRecord seal = store.verifyScope(scope);
      assertArrayEquals(callerDigest, seal.digest());
      assertEquals(1, seal.entityCount());
    }
  }

  @Test void verifyScopeFailsAsCorruptOnEntityCountMismatch() throws Exception {
    try (ObjectStore store = openStore()) {
      var scope = ScopePath.root(11);
      byte[] payload = bytes("counted");
      store.put(scope, 1, payload, digest(payload));
      store.sealScope(scope, StoreLayout.scopeDigest(
          List.of(new StoreLayout.EntityDigest(1, digest(payload)))), 2);
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

  @Test void parentAndChildScopesHoldIndependentEntities() throws Exception {
    try (ObjectStore store = openStore()) {
      var parent = ScopePath.root(0x23);
      var child = ScopePath.of(0x23, 0x29);
      byte[] parentPayload = bytes("parent-entity");
      byte[] childPayload = bytes("child-entity");
      store.put(parent, 1, parentPayload, digest(parentPayload));
      store.put(child, 2, childPayload, digest(childPayload));
      assertEquals(List.of(1L), store.list(parent), "parent lists only its direct entities");
      assertEquals(List.of(2L), store.list(child));
      store.sealScope(parent, StoreLayout.scopeDigest(
          List.of(new StoreLayout.EntityDigest(1, digest(parentPayload)))), 1);
      store.sealScope(child, StoreLayout.scopeDigest(
          List.of(new StoreLayout.EntityDigest(2, digest(childPayload)))), 1);
      store.verifyScope(parent);
      store.verifyScope(child);
      // Retirement is direct-entity only: child scopes survive their parent.
      var parent2 = ScopePath.root(0x24);
      var child2 = ScopePath.of(0x24, 0x30);
      byte[] p2 = bytes("p2"), c2 = bytes("c2");
      store.put(parent2, 1, p2, digest(p2));
      store.put(child2, 2, c2, digest(c2));
      store.retire(parent2, 1);
      assertEquals(List.of(), store.list(parent2));
      assertEquals(List.of(2L), store.list(child2), "child scope survives parent entity retirement");
    }
  }

  @Test void concurrentConflictingPutsYieldExactlyOneSuccess() throws Exception {
    try (ObjectStore store = openStore()) {
      var scope = ScopePath.root(0x21);
      int threads = 8;
      var pool = Executors.newFixedThreadPool(threads);
      var start = new CountDownLatch(1);
      var winners = new ConcurrentHashMap<Integer, byte[]>();
      var corrupt = new AtomicInteger();
      var unexpected = new AtomicInteger();
      List<byte[]> payloads = new ArrayList<>();
      for (int i = 0; i < threads; i++) payloads.add(bytes("variant-" + i));
      List<Future<?>> futures = new ArrayList<>();
      for (int i = 0; i < threads; i++) {
        int variant = i;
        byte[] payload = payloads.get(i);
        futures.add(pool.submit(() -> {
          start.await();
          try {
            store.put(scope, 1, payload, digest(payload));
            winners.put(variant, payload);
          } catch (StorageException failure) {
            if (failure.kind() == StorageException.Kind.CORRUPT) corrupt.incrementAndGet();
            else unexpected.incrementAndGet();
          }
          return null;
        }));
      }
      start.countDown();
      for (Future<?> future : futures) future.get(30, TimeUnit.SECONDS);
      pool.shutdownNow();
      assertEquals(0, unexpected.get(), "no unexpected failure kinds");
      assertEquals(1, winners.size(), "exactly one conflicting put wins");
      assertEquals(threads - 1, corrupt.get(), "every loser is CORRUPT");
      var winner = winners.entrySet().iterator().next();
      try (var stream = store.openRead(scope, 1)) {
        assertArrayEquals(winner.getValue(), stream.readAllBytes(), "stored content is the winner's payload");
      }
      assertArrayEquals(payloads.get(winner.getKey()), winner.getValue());
    }
  }

  @Test void putRacingSealScopeNeverPublishesAfterSeal() throws Exception {
    try (ObjectStore store = openStore()) {
      var scope = ScopePath.root(0x22);
      int threads = 8;
      var pool = Executors.newFixedThreadPool(threads);
      var start = new CountDownLatch(1);
      var succeeded = new ConcurrentHashMap<Long, byte[]>();
      List<Future<?>> futures = new ArrayList<>();
      for (int i = 0; i < threads; i++) {
        long entityId = 100 + i;
        byte[] payload = bytes("racing-" + i);
        futures.add(pool.submit(() -> {
          start.await();
          store.put(scope, entityId, payload, digest(payload));
          succeeded.put(entityId, payload);
          return null;
        }));
      }
      start.countDown();
      for (Future<?> future : futures) future.get(30, TimeUnit.SECONDS);
      pool.shutdownNow();
      // Seal only after every racing put resolved, recording exactly the winners.
      store.sealScope(scope, StoreLayout.scopeDigest(succeeded.entrySet().stream()
          .map(entry -> new StoreLayout.EntityDigest(entry.getKey(), digest(entry.getValue()))).toList()),
          succeeded.size());
      for (var entry : succeeded.entrySet()) {
        try (var stream = store.openRead(scope, entry.getKey())) {
          assertArrayEquals(entry.getValue(), stream.readAllBytes(),
              "every racing put is durably readable");
        }
      }
      ObjectStore.SealRecord seal = store.verifyScope(scope);
      assertEquals(succeeded.size(), seal.entityCount());
      // A put that starts strictly after the seal returned must never publish.
      var gate = new CountDownLatch(1);
      var lateSuccess = new AtomicBoolean();
      var lateKind = new java.util.concurrent.atomic.AtomicReference<StorageException.Kind>();
      var late = new Thread(() -> {
        try {
          gate.await();
          store.put(scope, 999, bytes("late"), digest(bytes("late")));
          lateSuccess.set(true);
        } catch (StorageException failure) {
          lateKind.set(failure.kind());
        } catch (InterruptedException interrupted) {
          Thread.currentThread().interrupt();
        }
      });
      late.start();
      gate.countDown();
      late.join(30_000);
      assertFalse(late.isAlive(), "late put finished");
      assertFalse(lateSuccess.get(), "put after the seal returned must not succeed");
      assertEquals(StorageException.Kind.ALREADY_SEALED, lateKind.get(),
          "put after seal is ALREADY_SEALED");
      assertStorage(StorageException.Kind.NOT_FOUND, () -> store.openRead(scope, 999));
      assertEquals(succeeded.size(), store.list(scope).size(), "late put never published");
    }
  }

  @Test void putIntoSealedScopeWithWrongDigestIsAlreadySealed() throws Exception {
    try (ObjectStore store = openStore()) {
      var scope = ScopePath.root(0x25);
      byte[] payload = bytes("sealed");
      store.put(scope, 1, payload, digest(payload));
      store.sealScope(scope, digest(payload), 1);
      assertStorage(StorageException.Kind.ALREADY_SEALED,
          () -> store.put(scope, 2, bytes("anything"), new byte[32]));
    }
  }

  @Test void retireOfEmptyButExistingUnsealedScopeIsNotFound() throws Exception {
    try (ObjectStore store = openStore()) {
      var scope = ScopePath.root(0x26);
      byte[] payload = bytes("lonely");
      store.put(scope, 1, payload, digest(payload));
      store.retire(scope, 1);
      assertEquals(List.of(), store.list(scope));
      assertStorage(StorageException.Kind.NOT_FOUND, () -> store.retire(scope, 10));
    }
  }

  @Test void verifyScopeDetectsPerObjectRecordedDigestTamper() throws Exception {
    assumeTrue(supportsRecordTamper(), "backend has no physical record to tamper");
    try (ObjectStore store = openStore()) {
      var scope = ScopePath.root(0x27);
      byte[] payload = bytes("tampered");
      store.put(scope, 1, payload, digest(payload));
      store.sealScope(scope, StoreLayout.scopeDigest(
          List.of(new StoreLayout.EntityDigest(1, digest(payload)))), 1);
      tamperRecord(scope, 1);
      assertStorage(StorageException.Kind.CORRUPT, () -> store.verifyScope(scope));
    }
  }

  @Test void verifyScopeDetectsSealCountTamper() throws Exception {
    assumeTrue(supportsRecordTamper(), "backend has no physical record to tamper");
    try (ObjectStore store = openStore()) {
      var scope = ScopePath.root(0x28);
      byte[] payload = bytes("count-tampered");
      store.put(scope, 1, payload, digest(payload));
      store.sealScope(scope, StoreLayout.scopeDigest(
          List.of(new StoreLayout.EntityDigest(1, digest(payload)))), 1);
      tamperSealCount(scope);
      assertStorage(StorageException.Kind.CORRUPT, () -> store.verifyScope(scope));
    }
  }

  /** Whether {@link #tamperRecord} and {@link #tamperSealCount} can corrupt stored records; default false. */
  protected boolean supportsRecordTamper() {
    return false;
  }

  /** Corrupts the stored record of one entity; only invoked when {@link #supportsRecordTamper()}. */
  protected void tamperRecord(ScopePath scope, long entityId) throws Exception {
    throw new UnsupportedOperationException("backend cannot tamper records");
  }

  /** Corrupts the recorded entity count of one seal; only invoked when {@link #supportsRecordTamper()}. */
  protected void tamperSealCount(ScopePath scope) throws Exception {
    throw new UnsupportedOperationException("backend cannot tamper records");
  }
}
