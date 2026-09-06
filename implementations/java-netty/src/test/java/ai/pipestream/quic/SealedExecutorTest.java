package ai.pipestream.quic;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.DriverManager;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

final class SealedExecutorTest {
  private static final UUID PRODUCER = new UUID(1, 2);
  private static final SealedWork.EntityKey ROOT = new SealedWork.EntityKey(0, 1);
  @TempDir Path directory;

  @Test void independentSessionProgressesWhileAWorkerStallsAndCallbacksReenterStorage() throws Exception {
    var sessions = sessions(); declare(sessions, "held", List.of(1L, 2L)); declare(sessions, "independent", List.of(1L));
    var entered = new CountDownLatch(1); var release = new CountDownLatch(1);
    var active = new AtomicInteger(); var maximum = new AtomicInteger();
    try (var payloads = payloads()) {
      var executor = SealedExecutor.start(sessions, payloads, (context, input) -> {
        int current = active.incrementAndGet(); maximum.accumulateAndGet(current, Math::max);
        try {
          assertFalse(sessions.declared(context.identity().session(), PRODUCER, 0).isEmpty());
          if (context.identity().session().equals("held") && context.identity().entity().equals(ROOT)) {
            entered.countDown(); assertTrue(release.await(10, TimeUnit.SECONDS));
          }
          return complete(input);
        } finally { active.decrementAndGet(); }
      }, new SealedExecutor.Limits(2, 1, Duration.ofSeconds(30)));
      try {
        executor.admit(input(payloads, "held", ROOT));
        assertTrue(entered.await(5, TimeUnit.SECONDS));
        executor.admit(input(payloads, "held", new SealedWork.EntityKey(0, 2)));
        executor.admit(input(payloads, "independent", ROOT));
        await(() -> sessions.checkpointReady("independent", PRODUCER, 0, 1));
        assertFalse(sessions.checkpointReady("held", PRODUCER, 0, 2));
        assertEquals(SealedJobs.QUEUED, new SealedJobs(sessions).find(key("held", new SealedWork.EntityKey(0, 2))).orElseThrow().state());
        assertTrue(maximum.get() <= 2); assertEquals(1, executor.usage().activeWorkers());
        assertTrue(executor.failure().isEmpty());
        release.countDown(); await(() -> sessions.checkpointReady("held", PRODUCER, 0, 2));
      } finally { release.countDown(); executor.close(); await(executor::isTerminated); }
    }
  }

  @Test void expiredCallbackKeepsItsPhysicalSlotAndShutdownOwnershipUntilItReturns() throws Exception {
    var sessions = sessions(); declare(sessions, "lease", List.of(1L));
    var entered = new CountDownLatch(1); var release = new CountDownLatch(1);
    var calls = new AtomicInteger(); var now = new AtomicLong(100);
    try (var payloads = payloads()) {
      var limits = new SealedExecutor.Limits(1, 1, Duration.ofMillis(10));
      var executor = SealedExecutor.start(sessions, payloads, (context, input) -> {
        calls.incrementAndGet(); entered.countDown(); assertTrue(release.await(10, TimeUnit.SECONDS)); return complete(input);
      }, limits, now::get);
      try {
        executor.admit(input(payloads, "lease", ROOT)); assertTrue(entered.await(5, TimeUnit.SECONDS));
        now.set(200);
        assertEquals(List.of(key("lease", ROOT)), new SealedJobs(sessions).ready(200, 8));
        Thread.sleep(100);
        assertEquals(1, calls.get()); assertEquals(1, executor.usage().activeWorkers());
        executor.close(); assertFalse(executor.isTerminated());
        assertThrows(IOException.class, () -> SealedExecutor.start(sessions(), payloads, (context, input) -> complete(input), limits));
        assertThrows(IOException.class, () -> executor.admit(input(payloads, "lease", ROOT)));
        release.countDown(); await(executor::isTerminated);
        assertEquals(1, executor.usage().rejectedFences());
        assertFalse(sessions.checkpointReady("lease", PRODUCER, 0, 1));
        assertTrue(executor.failure().isEmpty());
        var replacement = SealedExecutor.start(sessions(), payloads, (context, input) -> {
          assertEquals(2, context.epoch()); calls.incrementAndGet(); return complete(input);
        }, limits, now::get);
        try { await(() -> sessions.checkpointReady("lease", PRODUCER, 0, 1)); assertEquals(2, calls.get()); }
        finally { replacement.close(); await(replacement::isTerminated); }
      } finally { release.countDown(); executor.close(); await(executor::isTerminated); }
    }
  }

  @Test void corruptedRetainedPayloadRefusesWithoutCallingApplicationOrCompletingWork() throws Exception {
    var sessions = sessions(); declare(sessions, "corrupt", List.of(1L));
    var jobs = new SealedJobs(sessions); var calls = new AtomicInteger();
    try (var payloads = payloads()) {
      jobs.admit(input(payloads, "corrupt", ROOT));
      try (var files = Files.list(directory.resolve("payloads/objects"))) {
        Path object = files.findFirst().orElseThrow();
        try (var channel = java.nio.channels.FileChannel.open(object, java.nio.file.StandardOpenOption.WRITE)) {
          channel.position(channel.size() - 1); channel.write(java.nio.ByteBuffer.wrap(new byte[] {99}));
        }
      }
      var executor = SealedExecutor.start(sessions, payloads, (context, input) -> { calls.incrementAndGet(); return complete(input); }, SealedExecutor.Limits.defaults());
      try {
        await(() -> jobs.find(key("corrupt", ROOT)).orElseThrow().state() == SealedJobs.REFUSED);
        assertEquals(0, calls.get());
        assertEquals(Wire.ERROR_INTEGRITY, jobs.find(key("corrupt", ROOT)).orElseThrow().outcome().refusal());
        assertFalse(sessions.checkpointReady("corrupt", PRODUCER, 0, 1));
        assertTrue(executor.failure().isEmpty());
      } finally { executor.close(); await(executor::isTerminated); }
    }
  }

  @Test void callbackExceptionIsARetainedRefusalNotAnAutomaticRetry() throws Exception {
    var sessions = sessions(); declare(sessions, "exception", List.of(1L)); var jobs = new SealedJobs(sessions);
    var calls = new AtomicInteger();
    try (var payloads = payloads()) {
      var executor = SealedExecutor.start(sessions, payloads, (context, input) -> {
        calls.incrementAndGet(); throw new IllegalStateException("application failure");
      }, SealedExecutor.Limits.defaults());
      try {
        executor.admit(input(payloads, "exception", ROOT));
        await(() -> jobs.find(key("exception", ROOT)).orElseThrow().state() == SealedJobs.REFUSED);
        assertFalse(sessions.checkpointReady("exception", PRODUCER, 0, 1));
      } finally { executor.close(); await(executor::isTerminated); }
      var replacement = SealedExecutor.start(sessions(), payloads, (context, input) -> { calls.incrementAndGet(); return complete(input); }, SealedExecutor.Limits.defaults());
      try {
        assertTrue(jobs.ready(System.currentTimeMillis(), 8).isEmpty());
        Thread.sleep(50); assertEquals(1, calls.get());
        assertEquals(SealedJobs.REFUSED, jobs.find(key("exception", ROOT)).orElseThrow().state());
      } finally { replacement.close(); await(replacement::isTerminated); }
    }
  }

  @Test void concurrentCloseAndFastDispatchDoNotLeakOwnershipOrRejectHealthyWork() throws Exception {
    var sessions = sessions(); var ids = java.util.stream.LongStream.rangeClosed(1, 32).boxed().toList();
    declare(sessions, "fast", ids);
    try (var payloads = payloads()) {
      var executor = SealedExecutor.start(sessions, payloads, (context, input) -> complete(input), new SealedExecutor.Limits(2, 2, Duration.ofSeconds(30)));
      try {
        for (long id : ids) executor.admit(input(payloads, "fast", new SealedWork.EntityKey(0, id)));
        await(() -> sessions.checkpointReady("fast", PRODUCER, 0, 32));
        assertTrue(executor.failure().isEmpty());
      } finally {
        try (var closers = java.util.concurrent.Executors.newFixedThreadPool(2)) {
          var one = closers.submit(executor::close); var two = closers.submit(executor::close);
          one.get(5, TimeUnit.SECONDS); two.get(5, TimeUnit.SECONDS);
        }
        await(executor::isTerminated);
      }
      var reopened = SealedExecutor.start(sessions(), payloads, (context, input) -> complete(input), SealedExecutor.Limits.defaults());
      reopened.close(); await(reopened::isTerminated);
    }
  }

  @Test void failedStartupDoesNotRetainExecutorOwnershipAndCompletionRejectsCorruptJobs() throws Exception {
    var sessions = sessions(); declare(sessions, "broken", List.of(1L)); var jobs = new SealedJobs(sessions);
    try (var payloads = payloads()) {
      var input = input(payloads, "broken", ROOT); jobs.admit(input);
      jobs.publish(jobs.acquire(key("broken", ROOT), UUID.randomUUID(), 1, 10), 2, SealedJobs.Outcome.complete(input.digest()));
      try (var connection = DriverManager.getConnection("jdbc:sqlite:" + directory.resolve("sessions.sqlite3")); var statement = connection.createStatement()) {
        statement.executeUpdate("UPDATE ps_java_jobs SET image=CAST(substr(image,1,224)||zeroblob(32) AS BLOB)");
      }
      for (int attempt = 0; attempt < 2; attempt++) {
        assertEquals(Wire.ERROR_INTEGRITY, assertThrows(ProtocolException.class, () -> SealedExecutor.start(sessions, payloads, (context, stream) -> complete(stream), SealedExecutor.Limits.defaults())).errorCode());
      }
      assertEquals(Wire.ERROR_INTEGRITY, assertThrows(ProtocolException.class, () -> sessions.checkpointReady("broken", PRODUCER, 0, 1)).errorCode());
    }
  }

  @Test void stalledAdmissionCallsAreBoundedAndRemainOwnedThroughShutdown() throws Exception {
    var sessions = sessions(); declare(sessions, "storage", java.util.stream.LongStream.rangeClosed(1, 9).boxed().toList());
    try (var payloads = payloads(); var callers = java.util.concurrent.Executors.newFixedThreadPool(8)) {
      var inputs = new java.util.ArrayList<SealedPayloadStore.Stored>();
      for (int id = 1; id <= 9; id++) inputs.add(input(payloads, "storage", new SealedWork.EntityKey(0, id)));
      var callbacks = new AtomicInteger();
      var executor = SealedExecutor.start(sessions, payloads, (context, input) -> { callbacks.incrementAndGet(); return complete(input); }, SealedExecutor.Limits.defaults());
      try {
        try (var connection = DriverManager.getConnection("jdbc:sqlite:" + directory.resolve("sessions.sqlite3")); var writer = connection.createStatement()) {
          writer.execute("BEGIN IMMEDIATE");
          boolean transactionActive = true;
          try {
            var pending = new java.util.ArrayList<java.util.concurrent.Future<Void>>();
            for (int i = 0; i < 8; i++) {
              var stored = inputs.get(i);
              pending.add(callers.submit(() -> { executor.admit(stored); return null; }));
            }
            await(() -> executor.usage().activeStorageCalls() == 8);
            assertEquals(Wire.ERROR_LIMIT_EXCEEDED, assertThrows(ProtocolException.class, () -> executor.admit(inputs.get(8))).errorCode());
            executor.close(); assertFalse(executor.isTerminated());
            assertEquals(8, executor.usage().activeStorageCalls());
            assertThrows(IOException.class, () -> SealedExecutor.start(sessions, payloads, (context, input) -> complete(input), SealedExecutor.Limits.defaults()));
            writer.execute("ROLLBACK");
            transactionActive = false;
            for (var request : pending) request.get(8, TimeUnit.SECONDS);
          } finally { if (transactionActive) writer.execute("ROLLBACK"); }
        }
        await(executor::isTerminated);
        assertEquals(0, callbacks.get());
        assertEquals(8, new SealedJobs(sessions).ready(System.currentTimeMillis(), 128).size());
        assertFalse(sessions.checkpointReady("storage", PRODUCER, 0, 9));
      } finally { executor.close(); await(executor::isTerminated); }
    }
  }

  @Test void largeRetainedInputExecutesInAHeapSmallerThanItsPayload() throws Exception {
    Path log = directory.resolve("child.log");
    Process process = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin/java").toString(), "-Xmx24m",
        "--enable-native-access=ALL-UNNAMED", "-cp", System.getProperty("java.class.path"), SealedExecutorTest.class.getName(), directory.toString())
        .redirectErrorStream(true).redirectOutput(log.toFile()).start();
    try {
      assertTrue(process.waitFor(30, TimeUnit.SECONDS));
      assertEquals(0, process.exitValue(), () -> read(log));
    } finally { if (process.isAlive()) process.destroyForcibly().waitFor(5, TimeUnit.SECONDS); }
  }

  public static void main(String[] args) throws Exception {
    var test = new SealedExecutorTest(); test.directory = Path.of(args[0]);
    var sessions = test.sessions(); declare(sessions, "large", List.of(1L));
    try (var payloads = test.payloads()) {
      var identity = new SealedPayloadStore.Identity("large", PRODUCER, ROOT);
      long length = 32L << 20;
      var header = new SealedTransport.Header(ROOT, null, 0, null, BigInteger.valueOf(length), null, Map.of(), null);
      SealedPayloadStore.Stored stored;
      try (var receiver = payloads.begin(identity, header)) {
        byte[] buffer = new byte[8192];
        for (long i = 0; i < length; i += buffer.length) { buffer[0] = (byte) (i / buffer.length); receiver.write(buffer, 0, buffer.length); }
        try (var received = receiver.finish()) { stored = payloads.install(List.of(received)); }
      }
      var executor = SealedExecutor.start(sessions, payloads, (context, input) -> complete(input), SealedExecutor.Limits.defaults());
      try {
        executor.admit(stored); await(() -> sessions.checkpointReady("large", PRODUCER, 0, 1));
        assertArrayEquals(stored.digest(), new SealedJobs(sessions).find(key("large", ROOT)).orElseThrow().outcome().digest());
        assertTrue(executor.failure().isEmpty());
      } finally { executor.close(); await(executor::isTerminated); }
    }
  }

  private static SealedExecutor.Decision complete(java.io.InputStream input) throws IOException {
    var digest = SealedWork.sha256(); byte[] buffer = new byte[8192]; int n;
    while ((n = input.read(buffer)) != -1) digest.update(buffer, 0, n);
    return new SealedExecutor.Decision(3, digest.digest());
  }
  private SealedSessionStore sessions() throws Exception { return SealedSessionStore.open(directory.resolve("sessions.sqlite3")); }
  private SealedPayloadStore payloads() throws Exception { return SealedPayloadStore.open(directory.resolve("payloads"), SealedPayloadStore.Limits.defaults()); }
  private static void declare(SealedSessionStore store, String session, List<Long> ids) throws Exception {
    store.declare(new SealedWork.Declaration(session, PRODUCER, 0, null, BigInteger.ZERO, ids, SealedWork.SEAL,
        SealedWork.sealDigest(session, PRODUCER, 0, null, ids)), 7, 1024);
  }
  private static SealedPayloadStore.Stored input(SealedPayloadStore payloads, String session, SealedWork.EntityKey key) throws Exception {
    var identity = new SealedPayloadStore.Identity(session, PRODUCER, key);
    var header = new SealedTransport.Header(key, null, 0, null, BigInteger.ONE, null, Map.of(), null);
    try (var receiver = payloads.begin(identity, header)) {
      receiver.write(new byte[] {42}, 0, 1);
      try (var receipt = receiver.finish()) { return payloads.install(List.of(receipt)); }
    }
  }
  private static SealedJobs.Key key(String session, SealedWork.EntityKey entity) {
    return new SealedJobs.Key(new SealedPayloadStore.Identity(session, PRODUCER, entity), 0);
  }
  private static void await(Check check) throws Exception {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
    while (!check.test()) { if (System.nanoTime() >= deadline) fail("condition did not become true"); Thread.sleep(5); }
  }
  @FunctionalInterface private interface Check { boolean test() throws Exception; }
  private static String read(Path path) { try { return Files.readString(path); } catch (Exception error) { return error.toString(); } }
}
