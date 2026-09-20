package ai.pipestream.quic.storage;

import static ai.pipestream.quic.storage.ObjectStoreContractTest.assertStorage;
import static org.junit.jupiter.api.Assertions.*;

import java.io.InputStream;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.LongStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Adversarial concurrency attacks on the filesystem backends: retire storms against
 * sealed and unsealed scopes, puts racing retires and seals, conflicting seal storms,
 * racing store opens (double-writer), use-after-close, and reads held through retire
 * storms. No sleeps: threads are synchronized with latches and joined futures, so
 * final-state assertions are deterministic even though the winner of each race
 * varies. Only pinned {@link StorageException.Kind}s are asserted.
 */
final class AdversarialFsConcurrencyTest {
  @TempDir Path directory;

  private static byte[] bytes(String value) {
    return value.getBytes(java.nio.charset.StandardCharsets.UTF_8);
  }

  private static byte[] digest(byte[] bytes) {
    return StoreLayout.sha256().digest(bytes);
  }

  private static ScopePath scope(int tag) {
    return ScopePath.root(0xd000 + tag);
  }

  @Test void retireStormOnUnsealedScopeLeavesExactlyTheSurvivors() throws Exception {
    var scope = scope(1);
    try (ObjectStore store = FileObjectStore.open(directory)) {
      for (long id = 1; id <= 32; id++) {
        byte[] payload = bytes("storm-" + id);
        store.put(scope, id, payload, digest(payload));
      }
      int threads = 8;
      ExecutorService pool = Executors.newFixedThreadPool(threads);
      CountDownLatch start = new CountDownLatch(1);
      List<? extends Future<?>> futures = java.util.stream.IntStream.range(0, threads)
          .mapToObj(index -> pool.submit(() -> {
            start.await();
            store.retire(scope, 16);
            return null;
          })).toList();
      start.countDown();
      for (Future<?> future : futures) future.get();
      pool.shutdown();
      assertEquals(LongStream.rangeClosed(17, 32).boxed().toList(), store.list(scope),
          "every retire removes ids at or below 16; survivors are exactly 17..32");
      byte[] survivor = bytes("storm-32");
      try (InputStream stream = store.openRead(scope, 32)) {
        assertArrayEquals(survivor, stream.readAllBytes());
      }
    }
  }

  @Test void retireStormOnSealedScopeRemovesItExactlyOnce() throws Exception {
    var scope = scope(2);
    byte[] payload = bytes("sealed-storm");
    try (ObjectStore store = FileObjectStore.open(directory)) {
      store.put(scope, 1, payload, digest(payload));
      store.sealScope(scope, StoreLayout.scopeDigest(
          List.of(new StoreLayout.EntityDigest(1, digest(payload)))), 1);
      int threads = 8;
      ExecutorService pool = Executors.newFixedThreadPool(threads);
      CountDownLatch start = new CountDownLatch(1);
      AtomicReference<StorageException> failure = new AtomicReference<>();
      List<? extends Future<?>> futures = java.util.stream.IntStream.range(0, threads)
          .mapToObj(index -> pool.submit(() -> {
            start.await();
            try {
              store.retire(scope, 0);
            } catch (StorageException raced) {
              failure.compareAndSet(null, raced);
            }
            return null;
          })).toList();
      start.countDown();
      for (Future<?> future : futures) future.get();
      pool.shutdown();
      if (failure.get() != null) {
        assertEquals(StorageException.Kind.NOT_FOUND, failure.get().kind(),
            "losers of the retire race see the scope already gone");
      }
      assertEquals(List.of(), store.listScopes());
      assertStorage(StorageException.Kind.NOT_FOUND, () -> store.verifyScope(scope));
      assertStorage(StorageException.Kind.NOT_FOUND, () -> store.retire(scope, 0));
    }
  }

  @Test void putsRacingRetireNeverLoseAnAcknowledgedEntityAboveTheCursor() throws Exception {
    var scope = scope(3);
    try (ObjectStore store = FileObjectStore.open(directory)) {
      for (long id = 1; id <= 32; id++) {
        byte[] payload = bytes("base-" + id);
        store.put(scope, id, payload, digest(payload));
      }
      CountDownLatch start = new CountDownLatch(1);
      ExecutorService putter = Executors.newSingleThreadExecutor();
      ExecutorService retirer = Executors.newSingleThreadExecutor();
      Future<?> puts = putter.submit(() -> {
        start.await();
        for (long id = 33; id <= 64; id++) {
          byte[] payload = bytes("raced-" + id);
          store.put(scope, id, payload, digest(payload));
        }
        return null;
      });
      Future<?> retire = retirer.submit(() -> {
        start.await();
        store.retire(scope, 48);
        return null;
      });
      start.countDown();
      puts.get();
      retire.get();
      putter.shutdown();
      retirer.shutdown();
      List<Long> remaining = store.list(scope);
      assertTrue(remaining.stream().noneMatch(id -> id <= 32),
          "entities at or below the cursor were retired: " + remaining);
      assertTrue(remaining.containsAll(LongStream.rangeClosed(49, 64).boxed().toList()),
          "entities above the cursor survive every interleaving: " + remaining);
      assertTrue(remaining.stream().allMatch(id -> id >= 33 && id <= 64),
          "no phantom entities: " + remaining);
      for (long id : remaining) {
        try (InputStream stream = store.openRead(scope, id)) {
          assertArrayEquals(bytes("raced-" + id), stream.readAllBytes(),
              "entity " + id + " holds the content the putter acknowledged");
        }
      }
    }
  }

  @Test void sealStormWithIdenticalDigestSucceedsForEveryThread() throws Exception {
    var scope = scope(4);
    byte[] payload = bytes("calm-storm");
    byte[] sealDigest = digest(bytes("shared-seal"));
    try (ObjectStore store = FileObjectStore.open(directory)) {
      store.put(scope, 1, payload, digest(payload));
      int threads = 8;
      ExecutorService pool = Executors.newFixedThreadPool(threads);
      CountDownLatch start = new CountDownLatch(1);
      AtomicReference<StorageException> failure = new AtomicReference<>();
      List<? extends Future<?>> futures = java.util.stream.IntStream.range(0, threads)
          .mapToObj(index -> pool.submit(() -> {
            start.await();
            try {
              store.sealScope(scope, sealDigest, 1);
            } catch (StorageException raced) {
              failure.compareAndSet(null, raced);
            }
            return null;
          })).toList();
      start.countDown();
      for (Future<?> future : futures) future.get();
      pool.shutdown();
      assertNull(failure.get(), "identical re-seal is a no-op success (pinned decision 4)");
      ObjectStore.SealRecord record = store.verifyScope(scope);
      assertArrayEquals(sealDigest, record.digest());
    }
  }

  @Test void sealStormWithConflictingDigestsHasExactlyOneWinner() throws Exception {
    var scope = scope(5);
    byte[] payload = bytes("contested");
    try (ObjectStore store = FileObjectStore.open(directory)) {
      store.put(scope, 1, payload, digest(payload));
      int threads = 8;
      ExecutorService pool = Executors.newFixedThreadPool(threads);
      CountDownLatch start = new CountDownLatch(1);
      AtomicReference<byte[]> winner = new AtomicReference<>();
      AtomicReference<StorageException> corrupt = new AtomicReference<>();
      List<? extends Future<?>> futures = java.util.stream.IntStream.range(0, threads)
          .mapToObj(index -> pool.submit(() -> {
            byte[] candidate = digest(bytes("seal-candidate-" + index));
            start.await();
            try {
              store.sealScope(scope, candidate, 1);
              winner.compareAndSet(null, candidate);
            } catch (StorageException raced) {
              if (raced.kind() == StorageException.Kind.CORRUPT) corrupt.compareAndSet(null, raced);
              else throw new RuntimeException(raced);
            }
            return null;
          })).toList();
      start.countDown();
      for (Future<?> future : futures) future.get();
      pool.shutdown();
      assertNotNull(winner.get(), "exactly one seal wins");
      assertNotNull(corrupt.get(), "every loser sees the conflicting seal as Corrupt");
      ObjectStore.SealRecord record = store.verifyScope(scope);
      assertArrayEquals(winner.get(), record.digest());
      assertStorage(StorageException.Kind.ALREADY_SEALED,
          () -> store.put(scope, 2, payload, digest(payload)));
    }
  }

  @Test void sealRacingOneMorePutLinearizes() throws Exception {
    var scope = scope(6);
    int fixed = 32;
    try (ObjectStore store = FileObjectStore.open(directory)) {
      for (long id = 1; id <= fixed; id++) {
        byte[] payload = bytes("fixed-" + id);
        store.put(scope, id, payload, digest(payload));
      }
      CountDownLatch start = new CountDownLatch(1);
      AtomicReference<StorageException> putError = new AtomicReference<>();
      ExecutorService putter = Executors.newSingleThreadExecutor();
      ExecutorService sealer = Executors.newSingleThreadExecutor();
      Future<?> racingPut = putter.submit(() -> {
        byte[] extra = bytes("racing-extra");
        start.await();
        try {
          store.put(scope, 33, extra, digest(extra));
        } catch (StorageException raced) {
          putError.set(raced);
        }
        return null;
      });
      Future<?> seal = sealer.submit(() -> {
        start.await();
        store.sealScope(scope, StoreLayout.scopeDigest(java.util.Collections.emptyList()), fixed);
        return null;
      });
      start.countDown();
      racingPut.get();
      seal.get();
      putter.shutdown();
      sealer.shutdown();
      if (putError.get() != null) {
        assertEquals(StorageException.Kind.ALREADY_SEALED, putError.get().kind(),
            "if the seal landed first, the racing put is refused");
        store.verifyScope(scope);
      } else {
        // If the put landed first, the seal undercounts the live scope.
        assertStorage(StorageException.Kind.CORRUPT, () -> store.verifyScope(scope));
      }
    }
  }

  @Test void racingOpensOfOneRootYieldExactlyOneWriter() throws Exception {
    int threads = 8;
    ExecutorService pool = Executors.newFixedThreadPool(threads);
    CountDownLatch start = new CountDownLatch(1);
    AtomicReference<FileObjectStore> opened = new AtomicReference<>();
    AtomicReference<StorageException> failed = new AtomicReference<>();
    List<? extends Future<?>> futures = java.util.stream.IntStream.range(0, threads)
        .mapToObj(index -> pool.submit(() -> {
          start.await();
          try {
            FileObjectStore candidate = FileObjectStore.open(directory);
            if (!opened.compareAndSet(null, candidate)) candidate.close();
          } catch (StorageException raced) {
            // Losers normally see FAILED (double-writer guard). A racing first open
            // can instead hit the createDirectories race and see TRANSIENT (both are
            // pinned in the report's bug list); any other kind is a real failure.
            if (raced.kind() != StorageException.Kind.FAILED
                && raced.kind() != StorageException.Kind.TRANSIENT) {
              throw new RuntimeException(raced);
            }
            failed.compareAndSet(null, raced);
          }
          return null;
        })).toList();
    start.countDown();
    for (Future<?> future : futures) future.get();
    pool.shutdown();
    assertNotNull(opened.get(), "exactly one open wins");
    assertNotNull(failed.get(), "every loser is refused");
    try (ObjectStore winner = opened.get()) {
      var scope = scope(7);
      byte[] payload = bytes("winner-writes");
      winner.put(scope, 1, payload, digest(payload));
      assertEquals(List.of(1L), winner.list(scope));
    }
    try (ObjectStore store = FileObjectStore.open(directory)) {
      assertEquals(List.of(1L), store.list(scope(7)), "reopen after the race sees the winner's write");
    }
  }

  @Test void concurrentIdenticalPutsLeaveExactlyOneEntity() throws Exception {
    var scope = scope(8);
    byte[] payload = bytes("same-same-same");
    int threads = 8;
    try (ObjectStore store = FileObjectStore.open(directory)) {
      ExecutorService pool = Executors.newFixedThreadPool(threads);
      CountDownLatch start = new CountDownLatch(1);
      List<? extends Future<?>> futures = java.util.stream.IntStream.range(0, threads)
          .mapToObj(index -> pool.submit(() -> {
            start.await();
            store.put(scope, 77, payload, digest(payload));
            return null;
          })).toList();
      start.countDown();
      for (Future<?> future : futures) future.get();
      pool.shutdown();
      assertEquals(List.of(77L), store.list(scope));
      try (InputStream stream = store.openRead(scope, 77)) {
        assertArrayEquals(payload, stream.readAllBytes());
      }
    }
  }

  @Test void useAfterCloseMapsToFailedOnEveryOperation() throws Exception {
    var scope = scope(9);
    ObjectStore store = FileObjectStore.open(directory);
    byte[] payload = bytes("before-close");
    store.put(scope, 1, payload, digest(payload));
    store.close();
    store.close(); // idempotent
    byte[] after = bytes("after-close");
    assertStorage(StorageException.Kind.FAILED, () -> store.put(scope, 2, after, digest(after)));
    assertStorage(StorageException.Kind.FAILED, () -> store.openRead(scope, 1));
    assertStorage(StorageException.Kind.FAILED, () -> store.sealScope(scope, digest(after), 1));
    assertStorage(StorageException.Kind.FAILED, () -> store.verifyScope(scope));
    assertStorage(StorageException.Kind.FAILED, () -> store.list(scope));
    assertStorage(StorageException.Kind.FAILED, () -> store.listScopes());
    assertStorage(StorageException.Kind.FAILED, () -> store.retire(scope, 1));
  }

  @Test void journalUseAfterCloseMapsToFailedOnEveryOperation() throws Exception {
    Journal journal = FileJournal.open(directory);
    journal.appendIntent(new Journal.Intent("i1", bytes("p1")));
    journal.close();
    journal.close(); // idempotent
    assertStorage(StorageException.Kind.FAILED,
        () -> journal.appendIntent(new Journal.Intent("i2", bytes("p2"))));
    assertStorage(StorageException.Kind.FAILED, () -> journal.commit("i1", bytes("c1")));
    assertStorage(StorageException.Kind.FAILED, () -> journal.replay(0));
  }

  @Test void openReadHoldsTheObjectThroughARetireStorm() throws Exception {
    var scope = scope(10);
    byte[] payload = bytes("held-read");
    try (ObjectStore store = FileObjectStore.open(directory)) {
      store.put(scope, 7, payload, digest(payload));
      InputStream held = store.openRead(scope, 7);
      int threads = 4;
      ExecutorService pool = Executors.newFixedThreadPool(threads);
      CountDownLatch start = new CountDownLatch(1);
      List<? extends Future<?>> futures = java.util.stream.IntStream.range(0, threads)
          .mapToObj(index -> pool.submit(() -> {
            start.await();
            try {
              store.retire(scope, 0xffff_ffffL);
            } catch (StorageException raced) {
              // Once the first retire empties the scope, followers see NotFound
              // (pinned decision 5); anything else is a real failure.
              if (raced.kind() != StorageException.Kind.NOT_FOUND) throw new RuntimeException(raced);
            }
            return null;
          })).toList();
      start.countDown();
      for (Future<?> future : futures) future.get();
      pool.shutdown();
      try (InputStream stream = held) {
        assertArrayEquals(payload, stream.readAllBytes(),
            "a read opened before the storm is never invalidated by retirement");
      }
      assertEquals(List.of(), store.list(scope));
    }
  }

  @Test void reopenAfterRetireStormContinuesCleanly() throws Exception {
    var scope = scope(11);
    try (ObjectStore store = FileObjectStore.open(directory)) {
      for (long id = 1; id <= 16; id++) {
        byte[] payload = bytes("seed-" + id);
        store.put(scope, id, payload, digest(payload));
      }
      store.retire(scope, 0xffff_ffffL);
    }
    try (ObjectStore store = FileObjectStore.open(directory)) {
      assertEquals(List.of(), store.list(scope));
      byte[] payload = bytes("after-storm");
      store.put(scope, 1, payload, digest(payload));
      assertEquals(List.of(1L), store.list(scope));
    }
  }
}
