package ai.pipestream.quic.v2;

import static ai.pipestream.quic.v2.Messages.DURABLE_WORK;
import static ai.pipestream.quic.v2.Messages.RESULT_DELIVERY;
import static org.junit.jupiter.api.Assertions.*;

import ai.pipestream.quic.BoundedSqlite;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

@Timeout(30)
final class AuthorityExpansionSchedulingTest {
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
  private static final AdmissionStore.Authorization ALLOW = (binding, parameters) -> {};

  @TempDir Path directory;

  @org.junit.jupiter.params.ParameterizedTest(name = "page size {0}")
  @org.junit.jupiter.params.provider.ValueSource(ints = {1, 4})
  void yieldedParentCannotStarveFirstChildNeededToFundSecondChildAdmission(int pageSize)
      throws Exception {
    Path database = directory.resolve("authority-" + pageSize + ".sqlite");
    Path inputPath = directory.resolve("inputs-" + pageSize);
    AdmissionStore.Application application =
        new AdmissionStore.Application(
            "expand", Set.of(0, 2), AdmissionStore.RestartSafety.IDEMPOTENT);
    SessionStore sessions = SessionStore.initialize(database, configuration(application));
    Messages.Binding binding =
        sessions.create(
            access(),
            SELECTED,
            new Messages.Create(1, 1, new Records.Policy(10_000, 20_000, 30_000)));
    long generation = binding.generation();
    Records.WorkKey parent = new Records.WorkKey(0, 0, 1);
    try (InputStore inputs =
        InputStore.initializeForAuthority(inputPath, INPUT_LIMITS, sessions.identity())) {
      sessions.bindInputs(inputs);
      sessions.declare(
          access(),
          SELECTED,
          generation,
          new Messages.Declare(2, operation(1), 0, List.of(1L), true));
      Records.InputHeader parentHeader =
          header(generation, operation(2), parent, new byte[] {7}, 2);
      install(inputs, generation, parentHeader, new byte[] {7});
      sessions.admit(access(), SELECTED, generation, inputs, parentHeader, 3, clock(), ALLOW);
      Records.ChildScope child = view(sessions, generation, parent, 4).child();
      assertNotNull(child);

      AtomicInteger expansionCalls = new AtomicInteger();
      AtomicInteger firstChildCalls = new AtomicInteger();
      AtomicInteger secondChildCalls = new AtomicInteger();
      AtomicInteger reassemblyCalls = new AtomicInteger();
      ExecutionRuntime runtime =
          new ExecutionRuntime(
              sessions,
              inputs,
              List.of(
                  new ExecutionRuntime.Registration(
                      application,
                      context -> {
                        long entity = context.lease().work().entity();
                        if (context.lease().work().producer() == 1) {
                          if (entity == 10) firstChildCalls.incrementAndGet();
                          else if (entity == 20) secondChildCalls.incrementAndGet();
                          else fail("unexpected produced child " + entity);
                          byte[] byteBuffer = new byte[1];
                          assertEquals(1, context.readInput(byteBuffer, 0, 1));
                          assertEquals((byte) entity, byteBuffer[0]);
                          assertEquals(-1, context.readInput(byteBuffer, 0, 1));
                        } else {
                          assertEquals(parent, context.lease().work());
                          reassemblyCalls.incrementAndGet();
                        }
                        return ExecutionRuntime.Outcome.succeeded();
                      },
                      context -> {
                        expansionCalls.incrementAndGet();
                        assertEquals(child.scope(), context.childScope());
                        context.declare(operation(20), List.of(10L, 20L), true);
                        produce(context, child.scope(), 10, operation(21));
                        produce(context, child.scope(), 20, operation(22));
                        return ExecutionRuntime.ExpansionOutcome.complete();
                      })),
              new PublicationStore.Endpoint("results.example:7443"),
              clock(),
              ALLOW,
              new ExecutionRuntime.Limits(1, 1, 5000, 128),
              SELECTED);
      ExecutionScheduler scheduler =
          new ExecutionScheduler(
              sessions,
              runtime,
              clock(),
              owner -> new ExecutionStore.Access(owner, () -> {}),
              new ExecutionScheduler.Limits(1, 1, pageSize, 1));
      scheduler.start();
      try {
        await(() -> expansionCalls.get() >= 1);
        await(() -> view(sessions, generation, parent, 5).state() == Records.State.SUCCEEDED);
        assertTrue(expansionCalls.get() >= 2, "capacity refusal did not yield and resume parent");
        assertEquals(1, firstChildCalls.get());
        assertEquals(1, secondChildCalls.get());
        assertEquals(1, reassemblyCalls.get());
        Records.WorkView parentView = view(sessions, generation, parent, 6);
        assertEquals(1, parentView.attempt());
        assertEquals(Records.State.SUCCEEDED, parentView.state());
        assertNotNull(parentView.manifest());
        assertTrue(parentView.manifest().outputs().isEmpty());
        assertEquals(
            Records.State.SUCCEEDED,
            view(sessions, generation, new Records.WorkKey(child.scope(), 1, 10), 7).state());
        assertEquals(
            Records.State.SUCCEEDED,
            view(sessions, generation, new Records.WorkKey(child.scope(), 1, 20), 8).state());
        assertEquals(3, scalar(database, "SELECT count(*) FROM ps_v2_jobs"));
        assertEquals(2, scalar(database, "SELECT count(*) FROM ps_v2_jobs WHERE producer=1"));
        assertEquals(0, inputs.usage().handles());
      } finally {
        scheduler.close();
        assertTrue(scheduler.awaitStopped(5000));
      }

      SessionStore reopenedSessions = SessionStore.open(database, configuration(application));
      reopenedSessions.verifyInputs(inputs);
      assertEquals(Records.State.SUCCEEDED, view(reopenedSessions, generation, parent, 9).state());
      assertEquals(1, view(reopenedSessions, generation, parent, 10).attempt());
    }
  }

  private static void produce(
      ExecutionRuntime.ExpansionContext context,
      long scope,
      long entity,
      Records.OperationId operation)
      throws Exception {
    byte[] payload = {(byte) entity};
    Records.AdmitParameters parameters =
        new Records.AdmitParameters(
            new Records.WorkKey(scope, 1, entity),
            new Records.Input(1, digest(payload), "application/octet-stream"),
            "expand",
            0,
            5000,
            new Records.OutputBudget(0, 0));
    if (context.beginInput(operation, parameters).isPresent()) return;
    context.writeInput(ByteBuffer.wrap(payload));
    context.finishInput();
  }

  private static Records.InputHeader header(
      long generation,
      Records.OperationId operation,
      Records.WorkKey work,
      byte[] payload,
      int mode)
      throws Exception {
    return new Records.InputHeader(
        generation,
        operation,
        new Records.AdmitParameters(
            work,
            new Records.Input(payload.length, digest(payload), "application/octet-stream"),
            "expand",
            mode,
            5000,
            new Records.OutputBudget(0, 0)));
  }

  private static void install(
      InputStore inputs, long generation, Records.InputHeader header, byte[] payload)
      throws Exception {
    Commitments.Context context = new Commitments.Context("issuer-a", "alice", generation);
    try (InputStore.Receiver receiver = inputs.begin(context, header, SELECTED, 1000)) {
      receiver.write(ByteBuffer.wrap(payload), 1000);
      receiver.finish(1000);
    }
  }

  private static Records.WorkView view(
      SessionStore sessions, long generation, Records.WorkKey work, long request) throws Exception {
    return sessions
        .snapshot(access(), SELECTED, generation, new Messages.Watch(request, work, 0, 0))
        .work();
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
        new AdmissionStore.ExecutionPolicy(List.of(application), 2, 2));
  }

  private static long scalar(Path database, String sql) throws Exception {
    try (var connection = BoundedSqlite.open(database, BoundedSqlite.Limits.defaults()).connect();
        var statement = connection.createStatement();
        var rows = statement.executeQuery(sql)) {
      assertTrue(rows.next());
      return rows.getLong(1);
    }
  }

  private static void await(CheckedBoolean condition) throws Exception {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(8);
    while (!condition.getAsBoolean()) {
      if (System.nanoTime() >= deadline) fail("condition not reached before bounded deadline");
      java.util.concurrent.locks.LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
    }
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

  @FunctionalInterface
  private interface CheckedBoolean {
    boolean getAsBoolean() throws Exception;
  }
}
