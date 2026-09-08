package ai.pipestream.quic.v2;

import static ai.pipestream.quic.v2.Messages.DURABLE_WORK;
import static ai.pipestream.quic.v2.Messages.RESULT_DELIVERY;
import static org.junit.jupiter.api.Assertions.*;

import ai.pipestream.quic.BoundedSqlite;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

@Timeout(30)
final class ExecutionSchedulerTest {
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
      new InputStore.Limits(16L << 20, 128, 1 << 20, 16);
  private static final PublicationStore.Endpoint ENDPOINT =
      new PublicationStore.Endpoint("results.example:7443");
  private static final AdmissionStore.Authorization ALLOW = (binding, parameters) -> {};

  @TempDir Path directory;

  @Test
  void discoversRealWorkPublishesExactPayloadAndSurvivesPairedReopen() throws Exception {
    byte[] payload = {1, 2, 3, 4, 5, 6};
    try (Fixture fixture = new Fixture(directory.resolve("automatic"))) {
      Records.WorkKey work = fixture.admit("alice", 1, payload, 1, payload.length, 5000);
      AtomicInteger invocations = new AtomicInteger();
      ExecutionScheduler scheduler =
          fixture.scheduler(
              context -> {
                invocations.incrementAndGet();
                byte[] buffer = new byte[2];
                context.beginOutput(payload.length, "application/octet-stream");
                for (int read; (read = context.readInput(buffer, 0, buffer.length)) != -1; )
                  context.writeOutput(ByteBuffer.wrap(buffer, 0, read));
                assertEquals(0, context.finishOutput());
                return ExecutionRuntime.Outcome.succeeded();
              },
              clock(1100),
              owner -> execAccess(owner),
              new ExecutionScheduler.Limits(1, 1, 4, 1));
      scheduler.start();
      try {
        await(
            () ->
                state(fixture.sessions, "alice", fixture.generation("alice"), work)
                    == Records.State.SUCCEEDED);
        assertEquals(1, invocations.get());
        Records.WorkView view = fixture.view("alice", work);
        assertEquals(digest(payload), view.manifest().outputs().get(0).sha256());
        scheduler.close();
        assertTrue(scheduler.awaitStopped(5000));
        assertEquals(1, scheduler.status().completed());
        fixture.reopen();
        assertEquals(view, fixture.view("alice", work));
        fixture.sessions.verifyInputs(fixture.inputs);
      } finally {
        scheduler.close();
        assertTrue(scheduler.awaitStopped(5000));
      }
    }
  }

  @Test
  void busyCallbackDoesNotBlockDeadlineSettlementAndCloseDoesNotInterruptIt() throws Exception {
    try (Fixture fixture = new Fixture(directory.resolve("maintenance"))) {
      Records.WorkKey busy = fixture.admit("alice", 1, new byte[0], 0, 0, 5000);
      Records.WorkKey expired = fixture.admit("alice", 2, new byte[0], 0, 0, 1000);
      CountDownLatch entered = new CountDownLatch(1);
      CountDownLatch release = new CountDownLatch(1);
      AtomicBoolean interrupted = new AtomicBoolean();
      ExecutionScheduler scheduler =
          fixture.scheduler(
              context -> {
                if (context.lease().work().equals(busy)) {
                  entered.countDown();
                  try {
                    assertTrue(release.await(5, TimeUnit.SECONDS));
                  } catch (InterruptedException failure) {
                    interrupted.set(true);
                    throw failure;
                  }
                }
                return ExecutionRuntime.Outcome.succeeded();
              },
              clock(2000),
              owner -> execAccess(owner),
              new ExecutionScheduler.Limits(1, 1, 4, 1));
      scheduler.start();
      try {
        assertTrue(entered.await(5, TimeUnit.SECONDS));
        await(
            () ->
                state(fixture.sessions, "alice", fixture.generation("alice"), expired)
                    == Records.State.FAILED);
        assertEquals(Records.State.ACTIVE, fixture.view("alice", busy).state());
        scheduler.close();
        assertFalse(scheduler.awaitStopped(25));
        assertEquals(Records.State.ACTIVE, fixture.view("alice", busy).state());
        assertFalse(interrupted.get());
        release.countDown();
        assertTrue(scheduler.awaitStopped(5000));
        assertFalse(interrupted.get());
        assertEquals(Records.State.SUCCEEDED, fixture.view("alice", busy).state());
      } finally {
        release.countDown();
        scheduler.close();
        assertTrue(scheduler.awaitStopped(5000));
      }
    }
  }

  @Test
  void perOwnerCapacityStillDispatchesAnotherOwnerWithoutQueueingAlice() throws Exception {
    try (Fixture fixture = new Fixture(directory.resolve("owners"))) {
      Records.WorkKey aliceFirst = fixture.admit("alice", 1, new byte[0], 0, 0, 5000);
      Records.WorkKey aliceSecond = fixture.admit("alice", 2, new byte[0], 0, 0, 5000);
      Records.WorkKey bob = fixture.admit("bob", 1, new byte[0], 0, 0, 5000);
      CountDownLatch aliceEntered = new CountDownLatch(1);
      CountDownLatch bobEntered = new CountDownLatch(1);
      CountDownLatch release = new CountDownLatch(1);
      AtomicInteger aliceSecondCalls = new AtomicInteger();
      ExecutionScheduler scheduler =
          fixture.scheduler(
              context -> {
                if (context.lease().owner().equals("bob")) bobEntered.countDown();
                else if (context.lease().work().equals(aliceFirst)) aliceEntered.countDown();
                else aliceSecondCalls.incrementAndGet();
                assertTrue(release.await(5, TimeUnit.SECONDS));
                return ExecutionRuntime.Outcome.succeeded();
              },
              clock(1100),
              owner -> execAccess(owner),
              new ExecutionScheduler.Limits(2, 1, 4, 1));
      scheduler.start();
      try {
        assertTrue(aliceEntered.await(5, TimeUnit.SECONDS));
        assertTrue(bobEntered.await(5, TimeUnit.SECONDS));
        assertEquals(0, aliceSecondCalls.get());
        assertEquals(Records.State.ACTIVE, fixture.view("alice", aliceSecond).state());
        release.countDown();
        await(
            () ->
                state(fixture.sessions, "alice", fixture.generation("alice"), aliceSecond)
                    == Records.State.SUCCEEDED);
        await(
            () ->
                state(fixture.sessions, "bob", fixture.generation("bob"), bob)
                    == Records.State.SUCCEEDED);
        assertEquals(1, aliceSecondCalls.get());
        assertEquals(Records.State.SUCCEEDED, fixture.view("bob", bob).state());
      } finally {
        release.countDown();
        scheduler.close();
        assertTrue(scheduler.awaitStopped(5000));
      }
    }
  }

  @Test
  void grantDenialIsObservableNeverInvokesCallbackAndTerminalOutcomeIsNotRerun() throws Exception {
    try (Fixture fixture = new Fixture(directory.resolve("denial"))) {
      Records.WorkKey denied = fixture.admit("alice", 1, new byte[0], 0, 0, 5000);
      AtomicInteger calls = new AtomicInteger();
      ExecutionScheduler deniedScheduler =
          fixture.scheduler(
              context -> {
                calls.incrementAndGet();
                return ExecutionRuntime.Outcome.succeeded();
              },
              clock(1100),
              owner ->
                  new ExecutionStore.Access(
                      owner,
                      () -> {
                        throw new ProtocolError(
                            ProtocolError.Code.UNAUTHORIZED, "withdrawn execution grant");
                      }),
              new ExecutionScheduler.Limits(1, 1, 4, 1));
      deniedScheduler.start();
      try {
        await(() -> deniedScheduler.status().refused() > 0);
        assertEquals(0, calls.get());
        assertEquals(Records.State.ACTIVE, fixture.view("alice", denied).state());
        ExecutionScheduler.Failure failure = deniedScheduler.status().lastFailure();
        assertNotNull(failure);
        assertEquals(ProtocolError.Code.UNAUTHORIZED, failure.code());
        assertEquals(
            new ExecutionStore.Position(fixture.generation("alice"), 0, 1), failure.position());
      } finally {
        deniedScheduler.close();
        assertTrue(deniedScheduler.awaitStopped(5000));
      }

      AtomicInteger terminalCalls = new AtomicInteger();
      ExecutionScheduler terminalScheduler =
          fixture.scheduler(
              context -> {
                terminalCalls.incrementAndGet();
                return ExecutionRuntime.Outcome.failed(new Records.Diagnostic(7, "terminal"));
              },
              clock(1100),
              owner -> execAccess(owner),
              new ExecutionScheduler.Limits(1, 1, 4, 1));
      terminalScheduler.start();
      try {
        await(
            () ->
                state(fixture.sessions, "alice", fixture.generation("alice"), denied)
                    == Records.State.FAILED);
        assertEquals(1, terminalCalls.get());
      } finally {
        terminalScheduler.close();
        assertTrue(terminalScheduler.awaitStopped(5000));
      }
      fixture.reopen();
      Records.WorkKey marker = fixture.admit("alice", 2, new byte[0], 0, 0, 5000, 1100);
      AtomicInteger markerCalls = new AtomicInteger();
      ExecutionScheduler revisit =
          fixture.scheduler(
              context -> {
                if (context.lease().work().equals(denied)) terminalCalls.incrementAndGet();
                else if (context.lease().work().equals(marker)) markerCalls.incrementAndGet();
                return ExecutionRuntime.Outcome.succeeded();
              },
              clock(1100),
              owner -> execAccess(owner),
              new ExecutionScheduler.Limits(1, 1, 4, 1));
      revisit.start();
      try {
        await(
            () ->
                state(fixture.sessions, "alice", fixture.generation("alice"), marker)
                    == Records.State.SUCCEEDED);
        assertEquals(1, markerCalls.get());
        assertEquals(1, terminalCalls.get());
      } finally {
        revisit.close();
        assertTrue(revisit.awaitStopped(5000));
      }
    }
  }

  @Test
  void pairedReopenReplacesExpiredLeaseButNeverDispatchesAwaitingRetry() throws Exception {
    try (Fixture fixture = new Fixture(directory.resolve("recovery"))) {
      Records.WorkKey expired = fixture.admit("alice", 1, new byte[0], 0, 0, 5000);
      Records.WorkKey retry = fixture.admit("alice", 2, new byte[0], 0, 0, 5000);
      long generation = fixture.generation("alice");
      fixture.sessions.claimExecution(
          execAccess("alice"), generation, expired, fixture.inputs, 100, clock(1100), ALLOW);
      ExecutionStore.Lease retryLease =
          fixture.sessions.claimExecution(
              execAccess("alice"), generation, retry, fixture.inputs, 100, clock(1100), ALLOW);
      fixture.sessions.failExecution(
          execAccess("alice"),
          retryLease,
          new Records.Diagnostic(6, "explicit retry required"),
          true,
          clock(1150),
          ALLOW);
      fixture.reopen();
      AtomicInteger retryCalls = new AtomicInteger();
      AtomicInteger replacementCalls = new AtomicInteger();
      ExecutionScheduler scheduler =
          fixture.scheduler(
              context -> {
                if (context.lease().work().equals(retry)) retryCalls.incrementAndGet();
                if (context.lease().work().equals(expired)) {
                  replacementCalls.incrementAndGet();
                  assertEquals(1, context.lease().attempt());
                  assertEquals(2, context.lease().number());
                }
                return ExecutionRuntime.Outcome.succeeded();
              },
              clock(1200),
              owner -> execAccess(owner),
              new ExecutionScheduler.Limits(1, 1, 4, 1));
      scheduler.start();
      try {
        await(
            () -> state(fixture.sessions, "alice", generation, expired) == Records.State.SUCCEEDED);
        assertEquals(1, replacementCalls.get());
        assertEquals(0, retryCalls.get());
        assertEquals(Records.State.AWAITING_RETRY, fixture.view("alice", retry).state());
      } finally {
        scheduler.close();
        assertTrue(scheduler.awaitStopped(5000));
      }
    }
  }

  private static void await(BooleanSupplier condition) throws Exception {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    while (!condition.getAsBoolean()) {
      if (System.nanoTime() >= deadline) fail("condition was not reached before deadline");
      java.util.concurrent.locks.LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
    }
  }

  private static Records.State state(
      SessionStore sessions, String owner, long generation, Records.WorkKey work) {
    try {
      return sessions
          .snapshot(sessionAccess(owner), SELECTED, generation, new Messages.Watch(90, work, 0, 0))
          .work()
          .state();
    } catch (Exception failure) {
      throw new AssertionError(failure);
    }
  }

  private final class Fixture implements AutoCloseable {
    private final Path database;
    private final Path inputPath;
    private final AdmissionStore.Application application =
        new AdmissionStore.Application("copy", Set.of(0), AdmissionStore.RestartSafety.IDEMPOTENT);
    private final HashMap<String, Long> owners = new HashMap<>();
    private SessionStore sessions;
    private InputStore inputs;
    private int operation = 1;

    Fixture(Path root) throws Exception {
      database = Path.of(root + ".sqlite");
      inputPath = Path.of(root + "-inputs");
      sessions = SessionStore.initialize(database, configuration(application));
      inputs = InputStore.initializeForAuthority(inputPath, INPUT_LIMITS, sessions.identity());
      sessions.bindInputs(inputs);
    }

    Records.WorkKey admit(
        String owner, long entity, byte[] payload, int outputs, long outputBytes, long duration)
        throws Exception {
      return admit(owner, entity, payload, outputs, outputBytes, duration, 1000);
    }

    Records.WorkKey admit(
        String owner,
        long entity,
        byte[] payload,
        int outputs,
        long outputBytes,
        long duration,
        long admissionUtc)
        throws Exception {
      if (!owners.containsKey(owner)) {
        long sequence =
            sessions
                .nextSequence(
                    sessionAccess(owner), SELECTED, new Messages.NextSequence(operation++))
                .nextCreationSequence();
        Messages.Binding binding =
            sessions.create(
                sessionAccess(owner),
                SELECTED,
                new Messages.Create(
                    operation++, sequence, new Records.Policy(10_000, 20_000, 30_000)));
        owners.put(owner, binding.generation());
      }
      long generation = generation(owner);
      Records.WorkKey work = new Records.WorkKey(0, 0, entity);
      sessions.declare(
          sessionAccess(owner),
          SELECTED,
          generation,
          new Messages.Declare(operation++, operation(operation++), 0, List.of(entity), false));
      Records.InputHeader header =
          new Records.InputHeader(
              generation,
              operation(operation++),
              new Records.AdmitParameters(
                  work,
                  new Records.Input(payload.length, digest(payload), "application/octet-stream"),
                  "copy",
                  0,
                  duration,
                  new Records.OutputBudget(outputs, outputBytes)));
      Commitments.Context context = new Commitments.Context("issuer-a", owner, generation);
      try (InputStore.Receiver receiver = inputs.begin(context, header, SELECTED, 1)) {
        receiver.write(ByteBuffer.wrap(payload), 2);
        receiver.finish(3);
      }
      sessions.admit(
          sessionAccess(owner),
          SELECTED,
          generation,
          inputs,
          header,
          operation++,
          clock(admissionUtc),
          ALLOW);
      return work;
    }

    Records.WorkView view(String owner, Records.WorkKey work) throws Exception {
      return sessions
          .snapshot(
              sessionAccess(owner), SELECTED, generation(owner), new Messages.Watch(91, work, 0, 0))
          .work();
    }

    long generation(String owner) {
      return java.util.Objects.requireNonNull(owners.get(owner));
    }

    ExecutionScheduler scheduler(
        ExecutionRuntime.Callback callback,
        AdmissionStore.Clock clock,
        java.util.function.Function<String, ExecutionStore.Access> grants,
        ExecutionScheduler.Limits limits) {
      ExecutionRuntime runtime =
          new ExecutionRuntime(
              sessions,
              inputs,
              List.of(new ExecutionRuntime.Registration(application, callback)),
              ENDPOINT,
              clock,
              ALLOW,
              new ExecutionRuntime.Limits(limits.workers(), limits.workersPerOwner(), 5000, 128));
      return new ExecutionScheduler(sessions, runtime, clock, grants, limits);
    }

    void reopen() throws Exception {
      inputs.close();
      sessions = SessionStore.open(database, configuration(application));
      inputs = InputStore.open(inputPath, INPUT_LIMITS);
    }

    @Override
    public void close() throws java.io.IOException {
      inputs.close();
    }
  }

  private static SessionStore.Configuration configuration(AdmissionStore.Application app) {
    return new SessionStore.Configuration(
        "issuer-a",
        new Records.Limits(16, 32, 32, 1 << 20, 1 << 20, 4),
        new Records.Policy(60_000, 60_000, 60_000),
        8,
        32,
        16,
        BoundedSqlite.Limits.defaults(),
        new AdmissionStore.ExecutionPolicy(List.of(app), 16, 8));
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

  private static AdmissionStore.Clock clock(long utc) {
    return () -> new AdmissionStore.Time(utc, true);
  }

  private static SessionStore.Access sessionAccess(String owner) {
    return new SessionStore.Access(owner, () -> {});
  }

  private static ExecutionStore.Access execAccess(String owner) {
    return new ExecutionStore.Access(owner, () -> {});
  }
}
