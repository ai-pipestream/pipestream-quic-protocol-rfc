package ai.pipestream.quic.v2;

import static ai.pipestream.quic.v2.Messages.DURABLE_WORK;
import static ai.pipestream.quic.v2.Messages.RESULT_DELIVERY;
import static org.junit.jupiter.api.Assertions.*;

import ai.pipestream.quic.BoundedSqlite;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

@Timeout(20)
final class ExecutionRuntimeCreditTest {
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
  private static final PublicationStore.Endpoint ENDPOINT =
      new PublicationStore.Endpoint("results.example:7443");
  private static final AdmissionStore.Authorization ALLOW = (binding, parameters) -> {};

  @TempDir Path directory;

  @Test
  void runtimeReservesWriterAndInputBeforeCallbackWithoutLeakingEitherHandle() throws Exception {
    InputStore.Limits twoHandles = new InputStore.Limits(4L << 20, 32, 1 << 20, 2);
    Job admitted = job(1, new byte[] {1}, 1, 1, 1);
    Fixture fixture = fixture("preflight", twoHandles, List.of(admitted));
    AtomicBoolean called = new AtomicBoolean();
    ExecutionRuntime runtime =
        runtime(
            fixture,
            context -> {
              called.set(true);
              return ExecutionRuntime.Outcome.succeeded();
            });

    InputStore.Stored stored = fixture.inputs().find(context(), header(admitted)).orElseThrow();
    try (InputStream competitor = stored.openStream()) {
      assertEquals(1, fixture.inputs().usage().handles());
      assertEquals(1, competitor.read());
      ProtocolError refused =
          assertThrows(
              ProtocolError.class,
              () -> runtime.run(execAccess(), 1, new Records.WorkKey(0, 0, 1)));
      assertEquals(ProtocolError.Code.LIMIT_EXCEEDED, refused.code(), refused::getMessage);
      assertFalse(called.get());
      Records.WorkView retained = snapshot(fixture.sessions(), new Records.WorkKey(0, 0, 1));
      assertEquals(Records.State.ACTIVE, retained.state());
      assertNull(retained.manifest());
      assertNull(retained.diagnostic());
      assertEquals(1, fixture.inputs().usage().handles());
    }
    assertEquals(0, fixture.inputs().usage().handles());
    fixture.inputs().close();
  }

  @Test
  void writerCreditProtectsSequentialCallbackWritersFromReadersAndReclaim() throws Exception {
    InputStore.Limits oneHandle = new InputStore.Limits(4L << 20, 32, 1 << 20, 1);
    Path root = directory.resolve("credit");
    Records.InputHeader header = header(job(1, new byte[0], 2, 2, 1));
    Commitments.Context context = context();
    java.util.UUID authority = java.util.UUID.fromString("10000000-0000-0000-0000-000000000001");
    ExecutionStore.Lease first = lease(authority, 1);
    ExecutionStore.Lease replacement = lease(authority, 2);
    try (InputStore store = InputStore.initializeForAuthority(root, oneHandle, authority)) {
      store.reserveOutputs(context, header);
      InputStore.Usage funded = store.usage();
      OutputStore.WriterCredit credit = store.reserveOutputWriter(context, header, first);
      assertEquals(funded.bytes(), store.usage().bytes());
      assertEquals(funded.files(), store.usage().files());
      assertEquals(1, store.usage().handles());

      OutputStore.Writer firstWriter =
          store.beginOutput(context, header, first, 0, 1, "application/octet-stream", 2);
      assertThrows(IOException.class, credit::close);
      assertCode(
          ProtocolError.Code.CONFLICT, () -> store.reclaimOutputs(context, header, replacement));
      firstWriter.write(ByteBuffer.wrap(new byte[] {1}));
      OutputStore.Stored firstStored = firstWriter.finish();
      assertEquals(1, store.usage().handles());
      assertCode(ProtocolError.Code.LIMIT_EXCEEDED, firstStored::openStream);
      assertCode(
          ProtocolError.Code.CONFLICT, () -> store.reclaimOutputs(context, header, replacement));

      try (OutputStore.Writer secondWriter =
          store.beginOutput(context, header, first, 1, 1, "application/octet-stream", 2)) {
        secondWriter.write(ByteBuffer.wrap(new byte[] {2}));
        secondWriter.finish();
      }
      assertEquals(1, store.usage().handles());
      assertEquals(funded.bytes(), store.usage().bytes());
      assertEquals(funded.files(), store.usage().files());
      credit.close();
      credit.close();
      assertEquals(funded, store.usage());
    }
  }

  @Test
  void cleanupFaultAfterDescriptorCloseReleasesHandleAndWorkerForNextAdmittedJob()
      throws Exception {
    InputStore.Limits twoHandles = new InputStore.Limits(4L << 20, 64, 1 << 20, 2);
    Fixture initial =
        fixture(
            "cleanup",
            twoHandles,
            List.of(job(1, new byte[0], 1, 1, 1), job(2, new byte[0], 1, 1, 3)));
    initial.inputs().close();
    AtomicBoolean inject = new AtomicBoolean(true);
    InputStore probed =
        InputStore.open(
            initial.inputsPath(),
            twoHandles,
            phase -> {
              if (phase == InputStore.Phase.OUTPUT_STAGING_REMOVED && inject.getAndSet(false))
                throw new IOException("injected post-close cleanup failure");
            });
    Fixture fixture = new Fixture(initial.sessions(), probed, initial.inputsPath(), initial.app());
    AtomicInteger invocation = new AtomicInteger();
    ExecutionRuntime runtime =
        runtime(
            fixture,
            context -> {
              context.beginOutput(1, "application/octet-stream");
              if (invocation.incrementAndGet() == 1) return ExecutionRuntime.Outcome.succeeded();
              context.writeOutput(ByteBuffer.wrap(new byte[] {7}));
              context.finishOutput();
              return ExecutionRuntime.Outcome.succeeded();
            });
    try {
      IOException cleanup =
          assertThrows(
              IOException.class, () -> runtime.run(execAccess(), 1, new Records.WorkKey(0, 0, 1)));
      assertEquals("injected post-close cleanup failure", cleanup.getMessage());
      Records.WorkView failed = snapshot(fixture.sessions(), new Records.WorkKey(0, 0, 1));
      assertEquals(Records.State.FAILED, failed.state());
      assertEquals(ProtocolError.Code.INTEGRITY_ERROR.value(), failed.diagnostic().code());
      assertNull(failed.manifest());
      assertEquals(0, probed.usage().handles());

      Records.WorkView succeeded = runtime.run(execAccess(), 1, new Records.WorkKey(0, 0, 2));
      assertEquals(Records.State.SUCCEEDED, succeeded.state());
      assertNotNull(succeeded.manifest());
      assertEquals(1, succeeded.manifest().outputs().size());
      assertEquals(0, probed.usage().handles());
      assertEquals(2, invocation.get());
    } finally {
      probed.close();
    }
  }

  private Fixture fixture(String name, InputStore.Limits limits, List<Job> jobs) throws Exception {
    AdmissionStore.Application app =
        new AdmissionStore.Application("copy", Set.of(0), AdmissionStore.RestartSafety.IDEMPOTENT);
    Path database = directory.resolve(name + ".sqlite");
    Path inputsPath = directory.resolve(name + "-inputs");
    SessionStore sessions = SessionStore.initialize(database, configuration(app));
    sessions.create(
        sessionAccess(),
        SELECTED,
        new Messages.Create(1, 1, new Records.Policy(10_000, 20_000, 30_000)));
    InputStore inputs = InputStore.initializeForAuthority(inputsPath, limits, sessions.identity());
    sessions.bindInputs(inputs);
    for (Job job : jobs) {
      sessions.declare(
          sessionAccess(),
          SELECTED,
          1,
          new Messages.Declare(
              (job.operation() + 1L) / 2L + 1L,
              operation(job.operation()),
              0,
              List.of(job.entity()),
              false));
      Records.InputHeader header = header(job);
      try (InputStore.Receiver receiver =
          inputs.begin(context(), header, SELECTED, job.operation())) {
        receiver.write(ByteBuffer.wrap(job.payload()), job.operation() + 1L);
        receiver.finish(job.operation() + 2L);
      }
      sessions.admit(
          sessionAccess(), SELECTED, 1, inputs, header, job.operation() + 3L, clock(1000), ALLOW);
    }
    return new Fixture(sessions, inputs, inputsPath, app);
  }

  private static ExecutionRuntime runtime(Fixture fixture, ExecutionRuntime.Callback callback) {
    return new ExecutionRuntime(
        fixture.sessions(),
        fixture.inputs(),
        List.of(new ExecutionRuntime.Registration(fixture.app(), callback)),
        ENDPOINT,
        clock(1100),
        ALLOW,
        new ExecutionRuntime.Limits(1, 1, 500, 128));
  }

  private static SessionStore.Configuration configuration(AdmissionStore.Application app) {
    return new SessionStore.Configuration(
        "issuer-a",
        new Records.Limits(8, 16, 16, 1 << 20, 1 << 20, 4),
        new Records.Policy(60_000, 60_000, 60_000),
        4,
        16,
        4,
        BoundedSqlite.Limits.defaults(),
        new AdmissionStore.ExecutionPolicy(List.of(app), 8, 8));
  }

  private static Records.WorkView snapshot(SessionStore sessions, Records.WorkKey work)
      throws Exception {
    return sessions
        .snapshot(sessionAccess(), SELECTED, 1, new Messages.Watch(99, work, 0, 0))
        .work();
  }

  private static Records.InputHeader header(Job job) throws Exception {
    return new Records.InputHeader(
        1,
        operation(job.operation() + 1),
        new Records.AdmitParameters(
            new Records.WorkKey(0, 0, job.entity()),
            new Records.Input(
                job.payload().length, digest(job.payload()), "application/octet-stream"),
            "copy",
            0,
            1000,
            new Records.OutputBudget(job.outputs(), job.outputBytes())));
  }

  private static Job job(
      long entity, byte[] payload, int outputs, long outputBytes, int operation) {
    return new Job(entity, payload, outputs, outputBytes, operation);
  }

  private static ExecutionStore.Lease lease(java.util.UUID authority, long number) {
    return new ExecutionStore.Lease(
        authority, "alice", 1, new Records.WorkKey(0, 0, 1), 1, number, 2000);
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

  private static void assertCode(ProtocolError.Code expected, Throwing action) {
    ProtocolError error = assertThrows(ProtocolError.class, action::run);
    assertEquals(expected, error.code(), error::getMessage);
  }

  private record Job(long entity, byte[] payload, int outputs, long outputBytes, int operation) {}

  private record Fixture(
      SessionStore sessions, InputStore inputs, Path inputsPath, AdmissionStore.Application app) {}

  @FunctionalInterface
  private interface Throwing {
    void run() throws Exception;
  }
}
