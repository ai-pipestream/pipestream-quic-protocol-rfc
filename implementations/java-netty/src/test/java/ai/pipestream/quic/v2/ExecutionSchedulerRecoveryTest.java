package ai.pipestream.quic.v2;

import static ai.pipestream.quic.v2.Messages.DURABLE_WORK;
import static ai.pipestream.quic.v2.Messages.RESULT_DELIVERY;
import static org.junit.jupiter.api.Assertions.*;

import ai.pipestream.quic.BoundedSqlite;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

@Timeout(45)
final class ExecutionSchedulerRecoveryTest {
  private static final Messages.Capabilities SELECTED =
      new Messages.Capabilities(
          true,
          List.of(DURABLE_WORK, RESULT_DELIVERY),
          List.of(),
          1 << 20,
          8,
          16,
          1 << 20,
          1000,
          5000);
  private static final InputStore.Limits INPUT_LIMITS =
      new InputStore.Limits(4L << 20, 32, 1 << 20, 4);
  private static final Records.WorkKey WORK = new Records.WorkKey(0, 0, 1);
  private static final byte[] PAYLOAD = {1, 2, 3, 4, 5, 6};
  private static final byte[] OLD = {9, 9, 9, 9, 9, 9};
  private static final PublicationStore.Endpoint ENDPOINT =
      new PublicationStore.Endpoint("results.example:7443");
  private static final AdmissionStore.Authorization ALLOW = (binding, parameters) -> {};

  @TempDir Path directory;

  @Test
  void schedulerRestartWaitsForExpiredRealLeaseThenReclaimsAndPublishes() throws Exception {
    Path database = directory.resolve("authority.sqlite");
    Path inputsPath = directory.resolve("inputs");
    Path output = directory.resolve("child.out");
    Path error = directory.resolve("child.err");
    Process child =
        new ProcessBuilder(
                Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-cp",
                System.getProperty("java.class.path"),
                ExecutionSchedulerRecoveryTest.class.getName(),
                database.toString(),
                inputsPath.toString())
            .redirectOutput(output.toFile())
            .redirectError(error.toFile())
            .start();
    try {
      assertTrue(child.waitFor(10, TimeUnit.SECONDS), "scheduler crash child did not exit");
      String childError = boundedText(error, 8192);
      assertEquals(125, child.exitValue(), childError);
    } finally {
      if (child.isAlive()) {
        child.destroyForcibly();
        assertTrue(child.waitFor(2, TimeUnit.SECONDS));
      }
    }
    ChildReport report = report(output);
    assertEquals(1, report.attempt());
    assertEquals(1, report.lease());
    assertEquals(2100, report.until());
    assertEquals(1, count(inputsPath.resolve("outputs")));
    assertEquals(1, count(inputsPath.resolve("output-pending")));

    SessionStore sessions = SessionStore.open(database, configuration());
    InputStore.Usage funded;
    ExecutionStore.Lease producing = null;
    try (InputStore inputs = InputStore.open(inputsPath, INPUT_LIMITS)) {
      sessions.verifyInputs(inputs);
      funded = inputs.usage();
      assertEquals(report.bytes(), funded.bytes());
      assertEquals(report.files(), funded.files());
      assertEquals(0, funded.handles());
      assertEquals(1, count(inputsPath.resolve("outputs")));
      assertEquals(0, count(inputsPath.resolve("output-pending")));
      Records.WorkView executing = view(sessions);
      assertEquals(Records.State.ACTIVE, executing.state());
      assertEquals(1, executing.attempt());
      assertNull(executing.manifest());
      ExecutionStore.Candidate retained = assertSingle(sessions.scanExecutions(null, 64).entries());
      assertEquals(JobRecord.Stage.EXECUTING, retained.stage());
      assertEquals(2100L, retained.leaseUntil());

      AtomicInteger earlyCalls = new AtomicInteger();
      CountDownLatch earlySamples = new CountDownLatch(2);
      AdmissionStore.Clock beforeExpiry =
          () -> {
            earlySamples.countDown();
            return new AdmissionStore.Time(2099, true);
          };
      ExecutionScheduler early =
          scheduler(
              sessions,
              inputs,
              context -> {
                earlyCalls.incrementAndGet();
                return ExecutionRuntime.Outcome.succeeded();
              },
              beforeExpiry);
      early.start();
      try {
        assertTrue(earlySamples.await(5, TimeUnit.SECONDS));
      } finally {
        early.close();
        assertTrue(early.awaitStopped(5000));
      }
      assertEquals(0, earlyCalls.get());
      assertEquals(
          JobRecord.Stage.EXECUTING,
          assertSingle(sessions.scanExecutions(null, 64).entries()).stage());
      assertEquals(2100L, assertSingle(sessions.scanExecutions(null, 64).entries()).leaseUntil());

      AtomicReference<ExecutionStore.Lease> replacement = new AtomicReference<>();
      CountDownLatch copied = new CountDownLatch(1);
      ExecutionScheduler recovered =
          scheduler(
              sessions,
              inputs,
              context -> {
                replacement.set(context.lease());
                context.beginOutput(PAYLOAD.length, "application/octet-stream");
                byte[] buffer = new byte[2];
                for (int read; (read = context.readInput(buffer, 0, buffer.length)) != -1; )
                  context.writeOutput(ByteBuffer.wrap(buffer, 0, read));
                context.finishOutput();
                copied.countDown();
                return ExecutionRuntime.Outcome.succeeded();
              },
              clock(2100));
      recovered.start();
      try {
        assertTrue(copied.await(5, TimeUnit.SECONDS));
        await(() -> state(sessions) == Records.State.SUCCEEDED);
      } finally {
        recovered.close();
        assertTrue(recovered.awaitStopped(5000));
      }
      producing = replacement.get();
      assertNotNull(producing);
      assertEquals(1, producing.attempt());
      assertEquals(2, producing.number());
      assertEquals(1, count(inputsPath.resolve("outputs")));
      Records.WorkView succeeded = view(sessions);
      assertEquals(1, succeeded.manifest().outputs().size());
      assertEquals(digest(PAYLOAD), succeeded.manifest().outputs().get(0).sha256());
      assertEquals(PAYLOAD.length, succeeded.manifest().outputs().get(0).length());
      assertEquals(funded, inputs.usage());
      try (InputStream bytes =
          inputs.findOutput(context(), header(), producing, 0).orElseThrow().openStream()) {
        assertArrayEquals(PAYLOAD, bytes.readAllBytes());
      }
      assertEquals(funded, inputs.usage());
    }

    SessionStore reopened = SessionStore.open(database, configuration());
    try (InputStore inputs = InputStore.open(inputsPath, INPUT_LIMITS)) {
      reopened.verifyInputs(inputs);
      Records.WorkView retained = view(reopened);
      assertEquals(Records.State.SUCCEEDED, retained.state());
      assertEquals(1, retained.manifest().outputs().size());
      assertEquals(digest(PAYLOAD), retained.manifest().outputs().get(0).sha256());
      assertEquals(funded, inputs.usage());
      try (InputStream bytes =
          inputs.findOutput(context(), header(), producing, 0).orElseThrow().openStream()) {
        assertArrayEquals(PAYLOAD, bytes.readAllBytes());
      }
    }
  }

  public static void main(String[] args) throws Exception {
    Path database = Path.of(args[0]);
    Path inputsPath = Path.of(args[1]);
    SessionStore sessions = SessionStore.initialize(database, configuration());
    sessions.create(
        sessionAccess(),
        SELECTED,
        new Messages.Create(1, 1, new Records.Policy(10_000, 20_000, 30_000)));
    sessions.declare(
        sessionAccess(), SELECTED, 1, new Messages.Declare(2, operation(1), 0, List.of(1L), false));
    InputStore inputs =
        InputStore.initializeForAuthority(inputsPath, INPUT_LIMITS, sessions.identity());
    sessions.bindInputs(inputs);
    try (InputStore.Receiver receiver = inputs.begin(context(), header(), SELECTED, 1)) {
      receiver.write(ByteBuffer.wrap(PAYLOAD), 2);
      receiver.finish(3);
    }
    sessions.admit(sessionAccess(), SELECTED, 1, inputs, header(), 2, clock(1000), ALLOW);
    InputStore.Usage funded = inputs.usage();
    ExecutionScheduler scheduler =
        scheduler(
            sessions,
            inputs,
            context -> {
              context.beginOutput(PAYLOAD.length, "application/octet-stream");
              context.writeOutput(ByteBuffer.wrap(OLD));
              context.finishOutput();
              context.beginOutput(PAYLOAD.length, "application/octet-stream");
              context.writeOutput(ByteBuffer.wrap(PAYLOAD, 0, 2));
              ExecutionStore.Lease lease = context.lease();
              System.out.printf(
                  "attempt=%d lease=%d until=%d bytes=%d files=%d%n",
                  lease.attempt(), lease.number(), lease.until(), funded.bytes(), funded.files());
              System.out.flush();
              Runtime.getRuntime().halt(125);
              throw new AssertionError("halt returned");
            },
            clock(1100));
    scheduler.start();
    assertTrue(scheduler.awaitStopped(5000), "scheduler stopped without reaching crash callback");
    throw new AssertionError("scheduler crash callback was not reached");
  }

  private static ExecutionScheduler scheduler(
      SessionStore sessions,
      InputStore inputs,
      ExecutionRuntime.Callback callback,
      AdmissionStore.Clock clock) {
    ExecutionRuntime runtime =
        new ExecutionRuntime(
            sessions,
            inputs,
            List.of(new ExecutionRuntime.Registration(application(), callback)),
            ENDPOINT,
            clock,
            ALLOW,
            new ExecutionRuntime.Limits(1, 1, 1000, 128));
    return new ExecutionScheduler(
        sessions, runtime, clock, owner -> execAccess(), new ExecutionScheduler.Limits(1, 1, 4, 1));
  }

  private static SessionStore.Configuration configuration() {
    return new SessionStore.Configuration(
        "issuer-a",
        new Records.Limits(8, 16, 16, 1 << 20, 1 << 20, 4),
        new Records.Policy(60_000, 60_000, 60_000),
        2,
        8,
        4,
        BoundedSqlite.Limits.defaults(),
        new AdmissionStore.ExecutionPolicy(List.of(application()), 4, 4));
  }

  private static AdmissionStore.Application application() {
    return new AdmissionStore.Application(
        "copy", Set.of(0), AdmissionStore.RestartSafety.IDEMPOTENT);
  }

  private static Records.InputHeader header() throws Exception {
    return new Records.InputHeader(
        1,
        operation(2),
        new Records.AdmitParameters(
            WORK,
            new Records.Input(PAYLOAD.length, digest(PAYLOAD), "application/octet-stream"),
            "copy",
            0,
            5000,
            new Records.OutputBudget(2, 2L * PAYLOAD.length)));
  }

  private static Records.WorkView view(SessionStore sessions) throws Exception {
    return sessions
        .snapshot(sessionAccess(), SELECTED, 1, new Messages.Watch(8, WORK, 0, 0))
        .work();
  }

  private static Records.State state(SessionStore sessions) {
    try {
      return view(sessions).state();
    } catch (Exception failure) {
      throw new AssertionError(failure);
    }
  }

  private static void await(BooleanSupplier condition) throws Exception {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    while (!condition.getAsBoolean()) {
      if (System.nanoTime() >= deadline) fail("condition was not reached before deadline");
      java.util.concurrent.locks.LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
    }
  }

  private static ChildReport report(Path output) throws Exception {
    byte[] bytes;
    try (InputStream input = Files.newInputStream(output)) {
      bytes = input.readNBytes(256);
      assertEquals(-1, input.read());
    }
    String text = new String(bytes, StandardCharsets.UTF_8);
    assertTrue(
        text.matches("attempt=[0-9]+ lease=[0-9]+ until=[0-9]+ bytes=[0-9]+ files=[0-9]+\\n"),
        text);
    String[] fields = text.trim().split(" ");
    return new ChildReport(
        value(fields[0]), value(fields[1]), value(fields[2]), value(fields[3]), value(fields[4]));
  }

  private static long value(String field) {
    return Long.parseLong(field.substring(field.indexOf('=') + 1));
  }

  private static String boundedText(Path path, int limit) throws Exception {
    if (!Files.exists(path)) return "";
    try (InputStream input = Files.newInputStream(path)) {
      byte[] bytes = input.readNBytes(limit + 1);
      if (bytes.length > limit) return new String(bytes, 0, limit, StandardCharsets.UTF_8);
      return new String(bytes, StandardCharsets.UTF_8);
    }
  }

  private static long count(Path directory) throws Exception {
    try (var entries = Files.list(directory)) {
      return entries.count();
    }
  }

  private static <T> T assertSingle(List<T> values) {
    assertEquals(1, values.size());
    return values.get(0);
  }

  private static Records.Digest digest(byte[] bytes) throws Exception {
    return new Records.Digest(MessageDigest.getInstance("SHA-256").digest(bytes));
  }

  private static AdmissionStore.Clock clock(long time) {
    return () -> new AdmissionStore.Time(time, true);
  }

  private static Commitments.Context context() {
    return new Commitments.Context("issuer-a", "alice", 1);
  }

  private static Records.OperationId operation(int value) {
    byte[] bytes = new byte[16];
    bytes[15] = (byte) value;
    return new Records.OperationId(bytes);
  }

  private static SessionStore.Access sessionAccess() {
    return new SessionStore.Access("alice", () -> {});
  }

  private static ExecutionStore.Access execAccess() {
    return new ExecutionStore.Access("alice", () -> {});
  }

  private record ChildReport(long attempt, long lease, long until, long bytes, long files) {}
}
