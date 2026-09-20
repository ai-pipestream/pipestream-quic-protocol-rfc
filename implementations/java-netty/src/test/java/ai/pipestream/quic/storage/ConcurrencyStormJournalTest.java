package ai.pipestream.quic.storage;

import static ai.pipestream.quic.storage.JournalContractTest.bytes;
import static ai.pipestream.quic.storage.ObjectStoreContractTest.assertStorage;
import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.SplittableRandom;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

/**
 * Concurrency storm for {@link Journal} backends: what it attacks — append
 * and commit storms from many threads at once (cursors stay contiguous and
 * strictly increasing, every cursor is consumed exactly once in the global
 * order, replay is consistent with the deterministic payload model), replay
 * snapshots taken while the storm is still running (every snapshot is
 * internally consistent, never a torn view), duplicate-append retries racing
 * each other (one cursor, one entry), conflicting-commit storms (the winning
 * variant takes all its no-op successes, the losing side is {@code CORRUPT}),
 * and executor/thread hygiene (every storm thread dies and the JVM's active
 * thread count returns to baseline). Orchestration is latch- and queue-gated
 * so the interleavings are scheduled, never slept; committers consume ids
 * from the same hand-off queue the appenders publish to, which is the
 * two-system-commit ordering the design note pins. Loop counts stay far
 * under two minutes per backend.
 */
abstract class ConcurrencyStormJournalTest {
  /** Generous but bounded wait for one gated operation or pool shutdown. */
  private static final int OP_TIMEOUT_SECONDS = 60;
  /** Threads appending intents in the append/commit storm. */
  private static final int APPEND_THREADS = 4;
  /** Intents each appender contributes. */
  private static final int INTENTS_PER_APPEND_THREAD = 60;
  /** Committers draining the append hand-off queue. */
  private static final int COMMIT_THREADS = 2;
  /** Reader threads taking replay snapshots during the storm. */
  private static final int SNAPSHOT_READERS = 3;
  /** Threads re-appending one identical intent in the retry storm. */
  private static final int DUPLICATE_APPENDERS = 8;
  /** Threads per side of the conflicting-commit storm. */
  private static final int COMMIT_VARIANT_THREADS = 8;
  /** Queue poison pill terminating one committer. */
  private static final String POISON = "<storm-poison>";

  /** Opens one fresh, empty journal for a single test. */
  protected abstract Journal openJournal() throws Exception;

  @Test void appendCommitStormKeepsCursorsStrictlyOrderedAndReplayConsistent() throws Exception {
    try (Journal journal = openJournal()) {
      var pending = new LinkedBlockingQueue<String>();
      var records = new ConcurrentHashMap<String, byte[]>();
      var appendFactory = new NamedThreadFactory("append-");
      var commitFactory = new NamedThreadFactory("commit-");
      var appendPool = Executors.newFixedThreadPool(APPEND_THREADS, appendFactory);
      var commitPool = Executors.newFixedThreadPool(COMMIT_THREADS, commitFactory);
      try {
        var start = new CountDownLatch(1);
        List<Future<?>> appendFutures = new ArrayList<>();
        List<Future<?>> commitFutures = new ArrayList<>();
        for (int a = 0; a < APPEND_THREADS; a++) {
          int appender = a;
          appendFutures.add(appendPool.submit(() -> {
            start.await();
            for (int i = 0; i < INTENTS_PER_APPEND_THREAD; i++) {
              String intentId = "storm-a" + appender + "-i" + i;
              byte[] record = bytes("record-of-" + intentId);
              journal.appendIntent(new Journal.Intent(intentId, record));
              records.put(intentId, record);
              pending.offer(intentId);
            }
            return null;
          }));
        }
        for (int c = 0; c < COMMIT_THREADS; c++) {
          commitFutures.add(commitPool.submit(() -> {
            start.await();
            while (true) {
              String intentId = pending.take();
              if (intentId.equals(POISON)) return null;
              journal.commit(intentId, bytes("commit-of-" + intentId));
            }
          }));
        }
        start.countDown();
        for (Future<?> future : appendFutures) future.get(OP_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        for (int c = 0; c < COMMIT_THREADS; c++) pending.offer(POISON);
        for (Future<?> future : commitFutures) future.get(OP_TIMEOUT_SECONDS, TimeUnit.SECONDS);

        int intents = APPEND_THREADS * INTENTS_PER_APPEND_THREAD;
        List<Journal.ReplayEntry> replayed = journal.replay(0);
        assertEquals(intents, replayed.size(), "every appended intent replays exactly once");
        assertConsistentEntries(replayed, "final replay");
        Set<String> replayedIds = new HashSet<>();
        for (Journal.ReplayEntry entry : replayed) replayedIds.add(entry.intent().intentId());
        assertEquals(records.keySet(), replayedIds, "replayed ids are exactly the appended ids");
        int commits = 0;
        for (Journal.ReplayEntry entry : replayed) {
          assertArrayEquals(records.get(entry.intent().intentId()), entry.intent().record());
          if (entry.committed()) {
            commits++;
            assertArrayEquals(bytes("commit-of-" + entry.intent().intentId()), entry.commitRecord());
          }
        }
        assertEquals(intents, commits, "the queue hand-off committed every appended intent exactly once");
        assertContiguousCursors(replayed, intents + commits, "final replay");
        long since = replayed.get(intents / 2).cursor();
        List<Journal.ReplayEntry> tail = journal.replay(since);
        assertEquals(replayed.size() - intents / 2 - 1, tail.size(), "replay(since) returns exactly the tail");
        assertEquals(replayed.get(intents / 2 + 1).cursor(), tail.get(0).cursor());
      } finally {
        shutdownAndAssertDead(appendPool, appendFactory);
        shutdownAndAssertDead(commitPool, commitFactory);
      }
    }
  }

  @Test void concurrentReplaySnapshotsDuringAppendCommitStormAreConsistent() throws Exception {
    try (Journal journal = openJournal()) {
      var pending = new LinkedBlockingQueue<String>();
      var records = new ConcurrentHashMap<String, byte[]>();
      var maxCursor = new AtomicLong();
      var firstAppend = new CountDownLatch(1);
      var stop = new CountDownLatch(1);
      var violations = new AtomicReference<String>();
      var rng = new SplittableRandom(0xC0FFEE);
      var appendFactory = new NamedThreadFactory("snap-append-");
      var commitFactory = new NamedThreadFactory("snap-commit-");
      var readerFactory = new NamedThreadFactory("snap-read-");
      var appendPool = Executors.newFixedThreadPool(APPEND_THREADS, appendFactory);
      var commitPool = Executors.newFixedThreadPool(COMMIT_THREADS, commitFactory);
      var readerPool = Executors.newFixedThreadPool(SNAPSHOT_READERS, readerFactory);
      try {
        var start = new CountDownLatch(1);
        List<Future<?>> appendFutures = new ArrayList<>();
        List<Future<?>> commitFutures = new ArrayList<>();
        List<Future<?>> readerFutures = new ArrayList<>();
        for (int a = 0; a < APPEND_THREADS; a++) {
          int appender = a;
          appendFutures.add(appendPool.submit(() -> {
            start.await();
            for (int i = 0; i < INTENTS_PER_APPEND_THREAD; i++) {
              String intentId = "snap-a" + appender + "-i" + i;
              byte[] record = bytes("record-of-" + intentId);
              long cursor = journal.appendIntent(new Journal.Intent(intentId, record));
              records.put(intentId, record);
              maxCursor.accumulateAndGet(cursor, Math::max);
              pending.offer(intentId);
              if (i == 0) firstAppend.countDown();
            }
            return null;
          }));
        }
        for (int c = 0; c < COMMIT_THREADS; c++) {
          commitFutures.add(commitPool.submit(() -> {
            start.await();
            while (true) {
              String intentId = pending.take();
              if (intentId.equals(POISON)) return null;
              journal.commit(intentId, bytes("commit-of-" + intentId));
            }
          }));
        }
        for (int r = 0; r < SNAPSHOT_READERS; r++) {
          readerFutures.add(readerPool.submit(() -> {
            start.await();
            firstAppend.await(OP_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            long reads = 0;
            while (stop.getCount() > 0 && reads < 20_000) {
              long ceiling = maxCursor.get();
              if (ceiling == 0) continue;
              long since = rng.nextLong(ceiling + 1);
              List<Journal.ReplayEntry> snapshot = journal.replay(since);
              try {
                assertConsistentEntries(snapshot, "snapshot since=" + since);
                for (Journal.ReplayEntry entry : snapshot) {
                  assertArrayEquals(records.get(entry.intent().intentId()), entry.intent().record());
                  if (entry.committed()) {
                    assertArrayEquals(bytes("commit-of-" + entry.intent().intentId()), entry.commitRecord());
                  }
                }
              } catch (AssertionError violated) {
                violations.compareAndSet(null, violated.getMessage());
              }
              reads++;
            }
            return null;
          }));
        }
        start.countDown();
        for (Future<?> future : appendFutures) future.get(OP_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        for (int c = 0; c < COMMIT_THREADS; c++) pending.offer(POISON);
        for (Future<?> future : commitFutures) future.get(OP_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        stop.countDown();
        for (Future<?> future : readerFutures) future.get(OP_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        assertNull(violations.get(), "no replay snapshot violated ordering or payload invariants: " + violations.get());
        int intents = APPEND_THREADS * INTENTS_PER_APPEND_THREAD;
        List<Journal.ReplayEntry> replayed = journal.replay(0);
        assertEquals(intents, replayed.size());
        assertConsistentEntries(replayed, "post-storm replay");
        assertContiguousCursors(replayed, intents * 2, "post-storm replay");
        for (Journal.ReplayEntry entry : replayed) {
          assertTrue(entry.committed(), "post-storm every intent is committed");
          assertArrayEquals(bytes("commit-of-" + entry.intent().intentId()), entry.commitRecord());
        }
      } finally {
        shutdownAndAssertDead(appendPool, appendFactory);
        shutdownAndAssertDead(commitPool, commitFactory);
        shutdownAndAssertDead(readerPool, readerFactory);
      }
    }
  }

  @Test void duplicateAppendStormReturnsTheOriginalCursorWithOneEntry() throws Exception {
    try (Journal journal = openJournal()) {
      var intent = new Journal.Intent("duplicate-storm", bytes("identical-record"));
      long original = journal.appendIntent(intent);
      var factory = new NamedThreadFactory("dup-append-");
      var pool = Executors.newFixedThreadPool(DUPLICATE_APPENDERS, factory);
      try {
        var start = new CountDownLatch(1);
        var cursors = new ConcurrentHashMap<Integer, Long>();
        List<Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < DUPLICATE_APPENDERS; i++) {
          int appender = i;
          futures.add(pool.submit(() -> {
            start.await();
            cursors.put(appender, journal.appendIntent(intent));
            return null;
          }));
        }
        start.countDown();
        for (Future<?> future : futures) future.get(OP_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        for (var cursor : cursors.values()) {
          assertEquals(original, cursor.longValue(), "duplicate append is a no-op at the original cursor");
        }
        assertEquals(1, journal.replay(0).size(), "the journal holds exactly one entry");
      } finally {
        shutdownAndAssertDead(pool, factory);
      }
    }
  }

  @Test void conflictingCommitStormFollowsTheWinnerVariant() throws Exception {
    try (Journal journal = openJournal()) {
      journal.appendIntent(new Journal.Intent("victim", bytes("victim-record")));
      int perVariant = COMMIT_VARIANT_THREADS;
      byte[] alpha = bytes("commit-alpha");
      byte[] beta = bytes("commit-beta");
      var factory = new NamedThreadFactory("commit-conflict-");
      var pool = Executors.newFixedThreadPool(perVariant * 2, factory);
      try {
        var start = new CountDownLatch(1);
        var succeeded = new AtomicInteger();
        var corrupt = new AtomicInteger();
        var unexpected = new AtomicReference<StorageException.Kind>();
        List<Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < perVariant * 2; i++) {
          byte[] commit = i % 2 == 0 ? alpha : beta;
          futures.add(pool.submit(() -> {
            start.await();
            try {
              journal.commit("victim", commit);
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
            "every commit of the winning variant succeeds, first write plus idempotent no-ops");
        assertEquals(perVariant, corrupt.get(), "every commit of the losing variant is CORRUPT");
        byte[] recorded = journal.replay(0).get(0).commitRecord();
        assertTrue(Arrays.equals(recorded, alpha) || Arrays.equals(recorded, beta),
            "the durable commit is one of the two variants");
      } finally {
        shutdownAndAssertDead(pool, factory);
      }
    }
  }

  @Test void journalStormLeavesNoThreadsBehind() throws Exception {
    int baseline = Thread.activeCount();
    try (Journal journal = openJournal()) {
      for (int round = 0; round < 3; round++) {
        int threads = 6;
        var factory = new NamedThreadFactory("jleak-" + round + "-");
        var pool = Executors.newFixedThreadPool(threads, factory);
        try {
          var start = new CountDownLatch(1);
          int roundNumber = round;
          List<Future<?>> futures = new ArrayList<>();
          for (int t = 0; t < threads; t++) {
            int appender = t;
            futures.add(pool.submit(() -> {
              start.await();
              for (int i = 0; i < 10; i++) {
                String intentId = "leak-r" + roundNumber + "-t" + appender + "-i" + i;
                journal.appendIntent(new Journal.Intent(intentId, bytes("record-" + intentId)));
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

  /**
   * Structural invariants every replay view — final or in-flight — must hold:
   * cursors strictly increasing and consumed exactly once across intents and
   * commits, intent ids unique, and commit state internally coherent.
   */
  private static void assertConsistentEntries(List<Journal.ReplayEntry> entries, String where) {
    long previousCursor = 0;
    Set<Long> consumedCursors = new HashSet<>();
    Set<String> seenIds = new HashSet<>();
    for (Journal.ReplayEntry entry : entries) {
      assertTrue(entry.cursor() > previousCursor, where + ": cursors strictly increasing");
      previousCursor = entry.cursor();
      assertTrue(consumedCursors.add(entry.cursor()), where + ": cursor " + entry.cursor() + " consumed twice");
      assertTrue(seenIds.add(entry.intent().intentId()), where + ": duplicate intent id");
      if (entry.committed()) {
        assertNotNull(entry.commitCursor(), where + ": committed entry carries its commit cursor");
        assertTrue(entry.commitCursor() > entry.cursor(),
            where + ": commit lands after its intent in the global order");
        assertTrue(consumedCursors.add(entry.commitCursor()), where + ": commit cursor consumed twice");
      } else {
        assertNull(entry.commitRecord(), where + ": uncommitted entry has no commit record");
        assertNull(entry.commitCursor(), where + ": uncommitted entry has no commit cursor");
      }
    }
  }

  /**
   * Cursor-contiguity invariant of a complete replay: intent and commit
   * cursors together consume every value in {@code 1..expectedTotal} exactly
   * once, proving commits consumed cursors in the same global order.
   */
  private static void assertContiguousCursors(List<Journal.ReplayEntry> entries, long expectedTotal, String where) {
    Set<Long> consumed = new HashSet<>();
    for (Journal.ReplayEntry entry : entries) {
      consumed.add(entry.cursor());
      if (entry.commitCursor() != null) consumed.add(entry.commitCursor());
    }
    assertEquals(expectedTotal, consumed.size(), where + ": every cursor consumed exactly once");
    long max = 0;
    for (long cursor : consumed) max = Math.max(max, cursor);
    assertEquals(expectedTotal, max, where + ": cursors are contiguous from 1");
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
