package ai.pipestream.quic.storage;

import static ai.pipestream.quic.storage.ObjectStoreContractTest.assertStorage;
import static ai.pipestream.quic.storage.ObjectStoreContractTest.bytes;
import static ai.pipestream.quic.storage.ObjectStoreContractTest.digest;
import static ai.pipestream.quic.storage.ObjectStoreContractTest.readAll;
import static org.junit.jupiter.api.Assertions.*;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

/**
 * Concurrency storm for {@link ObjectStore} backends. Where the contract suite
 * proves each semantic once, this suite hammers the semantics that race:
 * N-thread conflicting puts at one entity id (exactly one winner, every loser
 * {@code CORRUPT}), idempotent-versus-conflicting put storms (the winning
 * variant takes all its no-op successes), put-versus-seal publication order
 * (a put that loses the race to the seal never publishes), {@code openRead}
 * streams opened before a {@code retire} racing it (bytes never corrupt, fresh
 * opens only fail {@code NOT_FOUND} or {@code TRANSIENT}), a 100+-stream
 * read fan-out, and executor/thread hygiene (every storm thread dies, and the
 * JVM's active thread count returns to baseline). Orchestration is latch- and
 * barrier-gated so races are scheduled, never slept; loop counts are sized to
 * widen TOCTOU windows while the whole suite stays far under two minutes.
 */
abstract class ConcurrencyStormObjectStoreTest {
  /** Generous but bounded wait for one gated operation or pool shutdown. */
  private static final int OP_TIMEOUT_SECONDS = 60;
  /** Threads simultaneously putting conflicting payloads at one entity id. */
  private static final int CONFLICT_THREADS = 32;
  /** Independent conflicting-put rounds, one fresh scope each. */
  private static final int CONFLICT_ROUNDS = 8;
  /** Putters racing one sealer per round of the seal storm. */
  private static final int SEAL_RACE_PUTTERS = 16;
  /** Put-versus-seal rounds; the outcome-split assertions hold per round. */
  private static final int SEAL_RACE_ROUNDS = 12;
  /** Entities seeded before the openRead-versus-retire storm. */
  private static final int RETIRE_ENTITIES = 24;
  /** Live readers holding streams across the retire. */
  private static final int RETIRE_READERS = 48;
  /** Fresh openRead attempts racing the retire tail. */
  private static final int RETIRE_RACING_OPENERS = 16;
  /** Hard cap per racing opener so the storm stays bounded. */
  private static final int RETIRE_RACING_ATTEMPTS = 400;
  /** Entities seeded for the parallel read fan-out. */
  private static final int FANOUT_ENTITIES = 16;
  /** Simultaneous openRead streams in the fan-out storm. */
  private static final int FANOUT_STREAMS = 128;

  /** Opens one fresh, empty store for a single test. */
  protected abstract ObjectStore openStore() throws Exception;

  @Test void nThreadConflictingPutStormYieldsExactlyOneWinnerPerRound() throws Exception {
    try (ObjectStore store = openStore()) {
      for (int round = 0; round < CONFLICT_ROUNDS; round++) {
        var scope = ScopePath.root(0x3000 + round);
        int threads = CONFLICT_THREADS;
        var factory = new NamedThreadFactory("conflict-" + round + "-");
        var pool = Executors.newFixedThreadPool(threads, factory);
        try {
          var start = new CountDownLatch(1);
          var winners = new ConcurrentHashMap<Integer, byte[]>();
          var corrupt = new AtomicInteger();
          var unexpected = new AtomicReference<StorageException.Kind>();
          List<byte[]> payloads = new ArrayList<>();
          for (int i = 0; i < threads; i++) {
            payloads.add(bytes("storm-" + round + "-variant-" + i));
          }
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
                else unexpected.compareAndSet(null, failure.kind());
              }
              return null;
            }));
          }
          start.countDown();
          for (Future<?> future : futures) future.get(OP_TIMEOUT_SECONDS, TimeUnit.SECONDS);
          assertNull(unexpected.get(), "round " + round + ": no failure kind other than CORRUPT");
          assertEquals(1, winners.size(), "round " + round + ": exactly one conflicting put wins");
          assertEquals(threads - 1, corrupt.get(), "round " + round + ": every loser is CORRUPT");
          byte[] winner = winners.values().iterator().next();
          assertArrayEquals(winner, readAll(store.openRead(scope, 1)),
              "round " + round + ": stored content is the winner payload");
        } finally {
          shutdownAndAssertDead(pool, factory);
        }
      }
    }
  }

  @Test void mixedIdempotentAndConflictingPutStormFollowsTheWinnerVariant() throws Exception {
    try (ObjectStore store = openStore()) {
      var scope = ScopePath.root(0x3800);
      int perVariant = 16;
      int threads = perVariant * 2;
      var factory = new NamedThreadFactory("mixed-");
      var pool = Executors.newFixedThreadPool(threads, factory);
      try {
        var start = new CountDownLatch(1);
        var succeeded = new AtomicInteger();
        var corrupt = new AtomicInteger();
        var unexpected = new AtomicReference<StorageException.Kind>();
        byte[] alpha = bytes("alpha-content");
        byte[] beta = bytes("beta-content");
        List<Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
          byte[] payload = i % 2 == 0 ? alpha : beta;
          futures.add(pool.submit(() -> {
            start.await();
            try {
              store.put(scope, 1, payload, digest(payload));
              succeeded.incrementAndGet();
            } catch (StorageException failure) {
              if (failure.kind() == StorageException.Kind.CORRUPT) corrupt.incrementAndGet();
              else unexpected.compareAndSet(null, failure.kind());
            }
            return null;
          }));
        }
        start.countDown();
        for (Future<?> future : futures) future.get(OP_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        assertNull(unexpected.get(), "no failure kind other than CORRUPT");
        assertEquals(perVariant, succeeded.get(),
            "every put of the winning variant succeeds, first write plus idempotent no-ops");
        assertEquals(perVariant, corrupt.get(), "every put of the losing variant is CORRUPT");
        byte[] stored = readAll(store.openRead(scope, 1));
        assertTrue(Arrays.equals(stored, alpha) || Arrays.equals(stored, beta),
            "stored content is one of the two variants");
      } finally {
        shutdownAndAssertDead(pool, factory);
      }
    }
  }

  @Test void putVersusSealRacesNeverPublishAfterSeal() throws Exception {
    try (ObjectStore store = openStore()) {
      int putters = SEAL_RACE_PUTTERS;
      for (int round = 0; round < SEAL_RACE_ROUNDS; round++) {
        var scope = ScopePath.root(0x4000 + round);
        var factory = new NamedThreadFactory("sealrace-" + round + "-");
        var pool = Executors.newFixedThreadPool(putters + 1, factory);
        try {
          var gate = new CyclicBarrier(putters + 1);
          var winners = new ConcurrentHashMap<Integer, byte[]>();
          var rejected = new AtomicInteger();
          var unexpected = new AtomicReference<StorageException.Kind>();
          byte[] sealDigest = digest(bytes("seal-digest-" + round));
          // Putter digests are client-side prep, computed before the gate so
          // the race under test is the store's own serialization of put
          // against seal, not each caller's hash CPU.
          List<byte[]> payloads = new ArrayList<>();
          List<byte[]> digests = new ArrayList<>();
          for (int i = 0; i < putters; i++) {
            byte[] payload = bytes("seal-race-" + round + "-" + i);
            payloads.add(payload);
            digests.add(digest(payload));
          }
          List<Future<?>> futures = new ArrayList<>();
          for (int i = 0; i < putters; i++) {
            int variant = i;
            byte[] payload = payloads.get(i);
            byte[] expected = digests.get(i);
            futures.add(pool.submit(() -> {
              awaitGate(gate, "seal-race putter");
              try {
                store.put(scope, variant + 1L, payload, expected);
                winners.put(variant, payload);
              } catch (StorageException failure) {
                if (failure.kind() == StorageException.Kind.ALREADY_SEALED) rejected.incrementAndGet();
                else unexpected.compareAndSet(null, failure.kind());
              }
              return null;
            }));
          }
          futures.add(pool.submit(() -> {
            awaitGate(gate, "sealer");
            store.sealScope(scope, sealDigest, 0);
            return null;
          }));
          for (Future<?> future : futures) future.get(OP_TIMEOUT_SECONDS, TimeUnit.SECONDS);
          assertNull(unexpected.get(), "round " + round + ": no failure kind other than ALREADY_SEALED");
          // The outcome split is scheduler-dependent — a filesystem store may
          // serialize the seal before every putter while an in-memory store
          // may admit many. The invariants below must hold for every split.
          assertEquals(putters, winners.size() + rejected.get(), "round " + round + ": every put resolved");
          List<Long> published = winners.keySet().stream().sorted().map(id -> id + 1L).toList();
          assertEquals(published, store.list(scope), "round " + round + ": only pre-seal winners published");
          for (var entry : winners.entrySet()) {
            assertArrayEquals(entry.getValue(), readAll(store.openRead(scope, entry.getKey() + 1L)),
                "round " + round + ": every pre-seal winner is durably readable");
          }
          for (int variant = 0; variant < putters; variant++) {
            if (winners.containsKey(variant)) continue;
            long id = variant + 1L;
            assertStorage(StorageException.Kind.NOT_FOUND, () -> store.openRead(scope, id));
          }
        } finally {
          shutdownAndAssertDead(pool, factory);
        }
      }
    }
  }

  @Test void openReadDuringRetireNeverCorruptsLiveReads() throws Exception {
    try (ObjectStore store = openStore()) {
      var scope = ScopePath.root(0x5000);
      long retained = RETIRE_ENTITIES / 2;
      for (long id = 1; id <= RETIRE_ENTITIES; id++) {
        byte[] payload = bytes("retire-storm-" + id);
        store.put(scope, id, payload, digest(payload));
      }
      var factory = new NamedThreadFactory("retire-");
      var pool = Executors.newFixedThreadPool(RETIRE_READERS + 1 + RETIRE_RACING_OPENERS, factory);
      try {
        var opened = new CyclicBarrier(RETIRE_READERS + 1);
        var retireDone = new CountDownLatch(1);
        var wrongBytes = new AtomicInteger();
        var forbiddenFreshKinds = new AtomicReference<StorageException.Kind>();
        List<Future<?>> futures = new ArrayList<>();

        for (int i = 0; i < RETIRE_READERS; i++) {
          long id = 1 + (i % RETIRE_ENTITIES);
          byte[] expected = bytes("retire-storm-" + id);
          futures.add(pool.submit(() -> {
            InputStream stream;
            try {
              stream = store.openRead(scope, id);
            } finally {
              awaitGate(opened, "live reader open");
            }
            try (InputStream live = stream) {
              byte[] actual = live.readAllBytes();
              if (!Arrays.equals(actual, expected)) wrongBytes.incrementAndGet();
            }
            return null;
          }));
        }

        futures.add(pool.submit(() -> {
          awaitGate(opened, "retirer");
          store.retire(scope, retained);
          retireDone.countDown();
          return null;
        }));

        for (int i = 0; i < RETIRE_RACING_OPENERS; i++) {
          futures.add(pool.submit(() -> {
            for (long id = 1, attempts = 0;
                attempts < RETIRE_RACING_ATTEMPTS && retireDone.getCount() > 0;
                id = id % RETIRE_ENTITIES + 1, attempts++) {
              try (InputStream stream = store.openRead(scope, id)) {
                byte[] actual = stream.readAllBytes();
                if (!Arrays.equals(actual, bytes("retire-storm-" + id))) wrongBytes.incrementAndGet();
              } catch (StorageException failure) {
                if (failure.kind() != StorageException.Kind.NOT_FOUND
                    && failure.kind() != StorageException.Kind.TRANSIENT) {
                  forbiddenFreshKinds.compareAndSet(null, failure.kind());
                }
              }
            }
            return null;
          }));
        }

        for (Future<?> future : futures) future.get(OP_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        assertNull(forbiddenFreshKinds.get(),
            "fresh opens racing retire only fail NOT_FOUND or TRANSIENT, got " + forbiddenFreshKinds.get());
        assertEquals(0, wrongBytes.get(), "no stream, live or racing, ever returned corrupted bytes");
        for (long id = 1; id <= retained; id++) {
          long retired = id;
          assertStorage(StorageException.Kind.NOT_FOUND, () -> store.openRead(scope, retired));
        }
        List<Long> surviving = new ArrayList<>();
        for (long id = retained + 1; id <= RETIRE_ENTITIES; id++) {
          assertArrayEquals(bytes("retire-storm-" + id), readAll(store.openRead(scope, id)));
          surviving.add(id);
        }
        assertEquals(surviving, store.list(scope));
      } finally {
        shutdownAndAssertDead(pool, factory);
      }
    }
  }

  @Test void hundredPlusParallelOpenReadStreamsAllReturnCorrectBytes() throws Exception {
    try (ObjectStore store = openStore()) {
      var scope = ScopePath.root(0x6000);
      for (long id = 1; id <= FANOUT_ENTITIES; id++) {
        byte[] payload = bytes("fanout-" + id);
        store.put(scope, id, payload, digest(payload));
      }
      var factory = new NamedThreadFactory("fanout-");
      var pool = Executors.newFixedThreadPool(FANOUT_STREAMS, factory);
      try {
        var start = new CountDownLatch(1);
        var wrongBytes = new AtomicInteger();
        var opened = new AtomicInteger();
        List<Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < FANOUT_STREAMS; i++) {
          long id = 1 + (i % FANOUT_ENTITIES);
          byte[] expected = bytes("fanout-" + id);
          futures.add(pool.submit(() -> {
            start.await();
            try (InputStream stream = store.openRead(scope, id)) {
              if (!Arrays.equals(stream.readAllBytes(), expected)) wrongBytes.incrementAndGet();
              opened.incrementAndGet();
            }
            return null;
          }));
        }
        start.countDown();
        for (Future<?> future : futures) future.get(OP_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        assertEquals(0, wrongBytes.get(), "all " + FANOUT_STREAMS + " streams returned exact bytes");
        assertEquals(FANOUT_STREAMS, opened.get(), "every stream opened");
      } finally {
        shutdownAndAssertDead(pool, factory);
      }
    }
  }

  @Test void objectStoreStormLeavesNoThreadsBehind() throws Exception {
    int baseline = Thread.activeCount();
    try (ObjectStore store = openStore()) {
      for (int round = 0; round < 3; round++) {
        var scope = ScopePath.root(0x7000 + round);
        int threads = 8;
        var factory = new NamedThreadFactory("leak-" + round + "-");
        var pool = Executors.newFixedThreadPool(threads, factory);
        try {
          var start = new CountDownLatch(1);
          List<Future<?>> futures = new ArrayList<>();
          for (int i = 0; i < threads; i++) {
            int variant = i;
            futures.add(pool.submit(() -> {
              start.await();
              byte[] payload = bytes("leak-" + variant);
              store.put(scope, variant + 1L, payload, digest(payload));
              try (InputStream stream = store.openRead(scope, variant + 1L)) {
                stream.readAllBytes();
              }
              return null;
            }));
          }
          start.countDown();
          for (Future<?> future : futures) future.get(OP_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } finally {
          shutdownAndAssertDead(pool, factory);
        }
      }
    }
    awaitQuiescence(baseline);
    assertTrue(Thread.activeCount() <= baseline,
        "active thread count " + Thread.activeCount() + " returned to baseline " + baseline
            + "; live threads: " + describeLiveThreads());
  }

  /** Shuts a storm pool down and asserts every thread it created is dead. */
  private static void shutdownAndAssertDead(ExecutorService pool, NamedThreadFactory factory)
      throws InterruptedException {
    pool.shutdown();
    assertTrue(pool.awaitTermination(OP_TIMEOUT_SECONDS, TimeUnit.SECONDS), "storm pool terminated");
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
    while (System.nanoTime() < deadline) {
      boolean allDead = true;
      for (Thread thread : factory.createdThreads()) {
        if (thread.isAlive()) {
          allDead = false;
          break;
        }
      }
      if (allDead) break;
      Thread.sleep(5);
    }
    for (Thread thread : factory.createdThreads()) {
      assertFalse(thread.isAlive(), "storm thread " + thread.getName() + " is dead");
    }
    List<String> alive = new ArrayList<>();
    for (Thread thread : Thread.getAllStackTraces().keySet()) {
      if (thread.isAlive() && thread.getName().startsWith(factory.prefix())) {
        alive.add(thread.getName());
      }
    }
    assertEquals(List.of(), alive, "no live thread carries the storm prefix");
  }

  /** Awaits one barrier party, failing loudly instead of hanging a gate. */
  private static void awaitGate(CyclicBarrier gate, String who) {
    try {
      gate.await(OP_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    } catch (Exception failure) {
      throw new IllegalStateException(who + " never reached the gate", failure);
    }
  }

  /** Polls, bounded, until the JVM's active thread count settles back at baseline. */
  private static void awaitQuiescence(int baseline) throws InterruptedException {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(OP_TIMEOUT_SECONDS);
    while (Thread.activeCount() > baseline && System.nanoTime() < deadline) {
      Thread.sleep(10);
    }
  }

  private static String describeLiveThreads() {
    StringBuilder dump = new StringBuilder();
    for (Thread thread : Thread.getAllStackTraces().keySet()) {
      if (thread.isAlive()) dump.append(thread.getName()).append('(').append(thread.getState()).append(") ");
    }
    return dump.toString();
  }

  /** Thread factory that names and records every storm thread for leak checks. */
  private static final class NamedThreadFactory implements ThreadFactory {
    private static final AtomicInteger FACTORY_COUNTER = new AtomicInteger();
    private final String prefix;
    private final AtomicInteger threadCounter = new AtomicInteger();
    private final Set<Thread> created = ConcurrentHashMap.newKeySet();

    NamedThreadFactory(String label) {
      prefix = "storm-" + label + FACTORY_COUNTER.incrementAndGet() + "-";
    }

    @Override public Thread newThread(Runnable task) {
      Thread thread = new Thread(task, prefix + threadCounter.incrementAndGet());
      created.add(thread);
      return thread;
    }

    String prefix() {
      return prefix;
    }

    Set<Thread> createdThreads() {
      return created;
    }
  }
}
