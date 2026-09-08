package ai.pipestream.quic.v2;

import static ai.pipestream.quic.v2.Messages.DURABLE_WORK;
import static ai.pipestream.quic.v2.Messages.RESULT_DELIVERY;
import static org.junit.jupiter.api.Assertions.*;

import ai.pipestream.quic.BoundedSqlite;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

@Timeout(30)
final class AuthorityExpansionRuntimeTest {
  private static final Messages.Capabilities SELECTED =
      new Messages.Capabilities(
          true,
          List.of(DURABLE_WORK, RESULT_DELIVERY),
          List.of(),
          1 << 20,
          16,
          32,
          1 << 20,
          1000,
          10_000);
  private static final InputStore.Limits INPUT_LIMITS =
      new InputStore.Limits(16L << 20, 128, 1 << 20, 3);
  private static final PublicationStore.Endpoint ENDPOINT =
      new PublicationStore.Endpoint("results.example:7443");
  private static final AdmissionStore.Authorization ALLOW = (binding, parameters) -> {};

  @TempDir Path directory;

  @Test
  void oneWorkerExpandsRealInputsExecutesChildrenAndReassemblesPublishedParent() throws Exception {
    try (Fixture fixture = new Fixture("automatic")) {
      byte[] parentInput = {7};
      fixture.prepare(parentInput);
      AtomicInteger expansionCalls = new AtomicInteger();
      AtomicInteger childCalls = new AtomicInteger();
      AtomicInteger reassemblyCalls = new AtomicInteger();
      AtomicReference<ExecutionStore.Lease> parentPublication = new AtomicReference<>();

      ExecutionRuntime runtime =
          fixture.runtime(
              context -> {
                if (context.lease().work().producer() == 1) {
                  childCalls.incrementAndGet();
                  byte[] buffer = new byte[2];
                  assertEquals(2, context.readInput(buffer, 0, buffer.length));
                  assertEquals(-1, context.readInput(buffer, 0, buffer.length));
                  context.beginOutput(2, "application/octet-stream");
                  context.writeOutput(ByteBuffer.wrap(buffer));
                  assertEquals(0, context.finishOutput());
                  return ExecutionRuntime.Outcome.succeeded();
                }
                reassemblyCalls.incrementAndGet();
                parentPublication.set(context.lease());
                assertEquals(parentInput[0], readOne(context));
                context.beginOutput(4, "application/octet-stream");
                copyChild(context, 10);
                copyChild(context, 20);
                assertEquals(0, context.finishOutput());
                return ExecutionRuntime.Outcome.succeeded();
              },
              context -> {
                expansionCalls.incrementAndGet();
                assertEquals(parentInput[0], readOne(context));
                long scope = context.childScope();
                context.declare(operation(20), List.of(10L, 20L), true);
                produce(context, scope, 10, operation(21), new byte[] {10, 11});
                produce(context, scope, 20, operation(22), new byte[] {20, 21});
                return ExecutionRuntime.ExpansionOutcome.complete();
              });
      ExecutionScheduler scheduler =
          new ExecutionScheduler(
              fixture.sessions,
              runtime,
              clock(),
              owner -> new ExecutionStore.Access(owner, () -> {}),
              new ExecutionScheduler.Limits(1, 1, 4, 1));
      scheduler.start();
      try {
        await(() -> fixture.view(fixture.parent).state() == Records.State.SUCCEEDED);
        await(() -> fixture.summary(0, fixture.rootSeal()) != null);
        assertEquals(1, expansionCalls.get());
        assertEquals(2, childCalls.get());
        assertEquals(1, reassemblyCalls.get());
        assertTrue(scheduler.status().active() <= 1);

        Records.WorkView parent = fixture.view(fixture.parent);
        byte[] expected = {10, 11, 20, 21};
        assertEquals(digest(expected), parent.manifest().outputs().get(0).sha256());
        OutputStore.Stored output =
            fixture
                .inputs
                .findOutput(fixture.context(), fixture.parentHeader, parentPublication.get(), 0)
                .orElseThrow();
        try (InputStream stream = output.openStream()) {
          assertArrayEquals(expected, stream.readAllBytes());
        }
        assertEquals(0, fixture.inputs.usage().handles());

        scheduler.close();
        assertTrue(scheduler.awaitStopped(5000));
        fixture.reopen();
        assertEquals(parent, fixture.view(fixture.parent));
        assertNotNull(fixture.summary(0, fixture.rootSeal()));
        assertEquals(3, scalar(fixture.database, "SELECT count(*) FROM ps_v2_jobs"));
        assertEquals(
            2, scalar(fixture.database, "SELECT count(*) FROM ps_v2_jobs WHERE producer=1"));
      } finally {
        scheduler.close();
        assertTrue(scheduler.awaitStopped(5000));
      }
    }
  }

  @Test
  void modeTwoRequiresExpanderAndExplicitProducerLimitsBeforeAnyCallback() throws Exception {
    for (String missing : List.of("expander", "producer-limits")) {
      try (Fixture fixture = new Fixture(missing)) {
        fixture.prepare(new byte[0]);
        AtomicInteger callbacks = new AtomicInteger();
        ExecutionRuntime.Callback callback =
            context -> {
              callbacks.incrementAndGet();
              return ExecutionRuntime.Outcome.succeeded();
            };
        ProtocolError refusal =
            assertThrows(
                ProtocolError.class,
                () -> {
                  ExecutionRuntime runtime =
                      missing.equals("expander")
                          ? new ExecutionRuntime(
                              fixture.sessions,
                              fixture.inputs,
                              List.of(
                                  new ExecutionRuntime.Registration(fixture.application, callback)),
                              ENDPOINT,
                              clock(),
                              ALLOW,
                              new ExecutionRuntime.Limits(1, 1, 500, 128),
                              SELECTED)
                          : new ExecutionRuntime(
                              fixture.sessions,
                              fixture.inputs,
                              List.of(
                                  new ExecutionRuntime.Registration(
                                      fixture.application,
                                      callback,
                                      context -> ExecutionRuntime.ExpansionOutcome.yielded())),
                              ENDPOINT,
                              clock(),
                              ALLOW,
                              new ExecutionRuntime.Limits(1, 1, 500, 128));
                  runtime.run(execAccess(), fixture.generation, fixture.parent);
                });
        assertEquals(
            ProtocolError.Code.APPLICATION_UNSUPPORTED, refusal.code(), refusal::getMessage);
        assertEquals(0, callbacks.get());
        assertEquals(Records.State.ACTIVE, fixture.view(fixture.parent).state());
      }
    }
  }

  private static void produce(
      ExecutionRuntime.ExpansionContext context,
      long scope,
      long entity,
      Records.OperationId operation,
      byte[] payload)
      throws Exception {
    Records.AdmitParameters parameters =
        new Records.AdmitParameters(
            new Records.WorkKey(scope, 1, entity),
            new Records.Input(payload.length, digest(payload), "application/octet-stream"),
            "expand",
            0,
            5000,
            new Records.OutputBudget(1, payload.length));
    assertTrue(context.beginInput(operation, parameters).isEmpty());
    ByteBuffer bytes = ByteBuffer.wrap(payload);
    context.writeInput(bytes);
    assertFalse(bytes.hasRemaining());
    Records.OperationReceipt receipt = context.finishInput();
    assertEquals(operation, receipt.operation());
    assertEquals(
        new Records.WorkKey(scope, 1, entity), ((Records.Admitted) receipt.outcome()).work());
  }

  private static byte readOne(ExecutionRuntime.Context context) throws Exception {
    byte[] byteBuffer = new byte[1];
    assertEquals(1, context.readInput(byteBuffer, 0, 1));
    assertEquals(-1, context.readInput(byteBuffer, 0, 1));
    return byteBuffer[0];
  }

  private static byte readOne(ExecutionRuntime.ExpansionContext context) throws Exception {
    byte[] byteBuffer = new byte[1];
    assertEquals(1, context.readInput(byteBuffer, 0, 1));
    assertEquals(-1, context.readInput(byteBuffer, 0, 1));
    return byteBuffer[0];
  }

  private static void copyChild(ExecutionRuntime.Context context, long entity) throws Exception {
    Records.Output child = context.beginChildOutput(entity, 0);
    assertEquals(2, child.length());
    byte[] bytes = new byte[2];
    assertEquals(2, context.readChildOutput(bytes, 0, bytes.length));
    context.writeOutput(ByteBuffer.wrap(bytes));
    assertEquals(-1, context.readChildOutput(bytes, 0, bytes.length));
    context.finishChildOutput();
  }

  private final class Fixture implements AutoCloseable {
    final Path database;
    final Path inputPath;
    final AdmissionStore.Application application =
        new AdmissionStore.Application(
            "expand", Set.of(0, 2), AdmissionStore.RestartSafety.IDEMPOTENT);
    final Records.WorkKey parent = new Records.WorkKey(0, 0, 1);
    final Messages.Binding binding;
    final long generation;
    SessionStore sessions;
    InputStore inputs;
    Records.InputHeader parentHeader;
    int request = 10;

    Fixture(String name) throws Exception {
      database = directory.resolve(name + ".sqlite");
      inputPath = directory.resolve(name + "-inputs");
      sessions = SessionStore.initialize(database, configuration(application));
      binding =
          sessions.create(
              access(),
              SELECTED,
              new Messages.Create(1, 1, new Records.Policy(10_000, 20_000, 30_000)));
      generation = binding.generation();
      inputs = InputStore.initializeForAuthority(inputPath, INPUT_LIMITS, sessions.identity());
      sessions.bindInputs(inputs);
    }

    void prepare(byte[] input) throws Exception {
      sessions.declare(
          access(),
          SELECTED,
          generation,
          new Messages.Declare(request++, operation(request++), 0, List.of(1L), true));
      parentHeader =
          new Records.InputHeader(
              generation,
              operation(request++),
              new Records.AdmitParameters(
                  parent,
                  new Records.Input(input.length, digest(input), "application/octet-stream"),
                  "expand",
                  2,
                  5000,
                  new Records.OutputBudget(1, 4)));
      try (InputStore.Receiver receiver = inputs.begin(context(), parentHeader, SELECTED, 1000)) {
        receiver.write(ByteBuffer.wrap(input), 1000);
        receiver.finish(1000);
      }
      sessions.admit(
          access(), SELECTED, generation, inputs, parentHeader, request++, clock(), ALLOW);
    }

    ExecutionRuntime runtime(
        ExecutionRuntime.Callback callback, ExecutionRuntime.Expander expander) {
      return new ExecutionRuntime(
          sessions,
          inputs,
          List.of(new ExecutionRuntime.Registration(application, callback, expander)),
          ENDPOINT,
          clock(),
          ALLOW,
          new ExecutionRuntime.Limits(1, 1, 500, 128),
          SELECTED);
    }

    Records.WorkView view(Records.WorkKey work) throws Exception {
      return sessions
          .snapshot(access(), SELECTED, generation, new Messages.Watch(request++, work, 0, 0))
          .work();
    }

    Records.ScopeSummary summary(long scope, Records.Digest seal) throws Exception {
      try {
        return sessions.scopeSummary(access(), SELECTED, generation, scope, seal);
      } catch (ProtocolError refusal) {
        if (refusal.code() == ProtocolError.Code.NOT_READY) return null;
        throw refusal;
      }
    }

    Records.Digest rootSeal() {
      Commitments.Seal seal = new Commitments.Seal(context(), 0, 0, null, 1);
      seal.add(1);
      return seal.finish();
    }

    Commitments.Context context() {
      return new Commitments.Context("issuer-a", "alice", generation);
    }

    void reopen() throws Exception {
      inputs.close();
      sessions = SessionStore.open(database, configuration(application));
      inputs = InputStore.open(inputPath, INPUT_LIMITS);
      sessions.verifyInputs(inputs);
    }

    @Override
    public void close() throws java.io.IOException {
      inputs.close();
    }
  }

  private static SessionStore.Configuration configuration(AdmissionStore.Application application) {
    return new SessionStore.Configuration(
        "issuer-a",
        new Records.Limits(16, 64, 64, 1 << 20, 1 << 20, 8),
        new Records.Policy(60_000, 60_000, 60_000),
        4,
        16,
        4,
        BoundedSqlite.Limits.defaults(),
        new AdmissionStore.ExecutionPolicy(List.of(application), 8, 8));
  }

  private static void await(CheckedBoolean condition) throws Exception {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(8);
    while (!condition.getAsBoolean()) {
      if (System.nanoTime() >= deadline) fail("condition not reached before bounded deadline");
      java.util.concurrent.locks.LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
    }
  }

  private static long scalar(Path database, String sql) throws Exception {
    try (var connection =
            BoundedSqlite.open(database, configuration(application()).files()).connect();
        var statement = connection.createStatement();
        var rows = statement.executeQuery(sql)) {
      assertTrue(rows.next());
      return rows.getLong(1);
    }
  }

  private static AdmissionStore.Application application() {
    return new AdmissionStore.Application(
        "expand", Set.of(0, 2), AdmissionStore.RestartSafety.IDEMPOTENT);
  }

  private static Records.Digest digest(byte[] bytes) throws Exception {
    return new Records.Digest(MessageDigest.getInstance("SHA-256").digest(bytes));
  }

  private static Records.OperationId operation(int value) {
    byte[] bytes = new byte[16];
    bytes[12] = (byte) (value >>> 24);
    bytes[13] = (byte) (value >>> 16);
    bytes[14] = (byte) (value >>> 8);
    bytes[15] = (byte) value;
    return new Records.OperationId(bytes);
  }

  private static AdmissionStore.Clock clock() {
    return () -> new AdmissionStore.Time(1000, true);
  }

  private static SessionStore.Access access() {
    return new SessionStore.Access("alice", () -> {});
  }

  private static ExecutionStore.Access execAccess() {
    return new ExecutionStore.Access("alice", () -> {});
  }

  @FunctionalInterface
  private interface CheckedBoolean {
    boolean getAsBoolean() throws Exception;
  }
}
