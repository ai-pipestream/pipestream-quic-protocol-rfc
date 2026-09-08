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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

@Timeout(30)
final class ExecutionRuntimeTest {
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
      new InputStore.Limits(8L << 20, 64, 1 << 20, 8);
  private static final PublicationStore.Endpoint ENDPOINT =
      new PublicationStore.Endpoint("results.example:7443");
  private static final AdmissionStore.Authorization ALLOW = (binding, parameters) -> {};

  @TempDir Path directory;

  @Test
  void successfulCopyPublishesExactOutputAndSurvivesPairedReopen() throws Exception {
    byte[] payload = {1, 2, 3, 4, 5, 6};
    Fixture fixture = fixture("copy", payload, 1, payload.length, 1);
    ExecutionRuntime runtime =
        runtime(
            fixture,
            context -> {
              byte[] buffer = new byte[3];
              context.beginOutput(payload.length, "application/octet-stream");
              for (int read; (read = context.readInput(buffer, 0, buffer.length)) != -1; ) {
                context.check();
                context.writeOutput(ByteBuffer.wrap(buffer, 0, read));
              }
              assertEquals(0, context.finishOutput());
              return ExecutionRuntime.Outcome.succeeded();
            },
            2,
            2);
    Records.WorkView view = runtime.run(execAccess("alice"), 1, fixture.work());
    assertEquals(Records.State.SUCCEEDED, view.state());
    assertEquals(digest(payload), view.manifest().outputs().get(0).sha256());
    assertEquals(payload.length, view.manifest().outputs().get(0).length());
    fixture.inputs().close();

    SessionStore reopened = SessionStore.open(fixture.database(), configuration(fixture.app()));
    try (InputStore inputs = InputStore.open(fixture.inputsPath(), INPUT_LIMITS)) {
      reopened.verifyInputs(inputs);
      Records.WorkView retained = snapshot(reopened, fixture.work());
      assertEquals(view, retained);
      assertEquals(payload.length, retained.manifest().outputs().get(0).length());
    }
  }

  @Test
  void callbackExceptionRetryableAndStickyOutputMisuseBecomeDurableFailureStates()
      throws Exception {
    Fixture exception = fixture("exception", new byte[0], 0, 0, 1);
    Records.WorkView failed =
        runtime(
                exception,
                context -> {
                  throw new IllegalStateException("secret callback detail");
                },
                2,
                2)
            .run(execAccess("alice"), 1, exception.work());
    assertEquals(Records.State.FAILED, failed.state());
    assertNotNull(failed.diagnostic());
    assertEquals(15, failed.diagnostic().code());
    assertFalse(failed.diagnostic().detail().contains("secret"));
    exception.inputs().close();

    Fixture retry = fixture("retry", new byte[0], 0, 0, 1);
    Records.Diagnostic diagnostic = new Records.Diagnostic(7, "retry requested");
    Records.WorkView retryable =
        runtime(retry, context -> ExecutionRuntime.Outcome.retryable(diagnostic), 2, 2)
            .run(execAccess("alice"), 1, retry.work());
    assertEquals(Records.State.AWAITING_RETRY, retryable.state());
    assertEquals(diagnostic, retryable.diagnostic());
    retry.inputs().close();

    Fixture sticky = fixture("sticky", new byte[0], 1, 1, 1);
    Records.WorkView stickyFailure =
        runtime(
                sticky,
                context -> {
                  context.beginOutput(1, "application/octet-stream");
                  try {
                    context.writeOutput(ByteBuffer.wrap(new byte[] {1, 2}));
                  } catch (ProtocolError expected) {
                    assertEquals(ProtocolError.Code.LIMIT_EXCEEDED, expected.code());
                  }
                  return ExecutionRuntime.Outcome.succeeded();
                },
                2,
                2)
            .run(execAccess("alice"), 1, sticky.work());
    assertEquals(Records.State.FAILED, stickyFailure.state());
    assertEquals(4, stickyFailure.diagnostic().code());
    assertNull(stickyFailure.manifest());
    sticky.inputs().close();
  }

  @Test
  void zeroOutputSuccessUsesEmptyManifestOnlyWhenResultsWereSelected() throws Exception {
    Fixture durable = fixture("zero-durable", new byte[0], 0, 0, 1, false);
    Records.WorkView durableView =
        runtime(durable, context -> ExecutionRuntime.Outcome.succeeded(), 1, 1)
            .run(execAccess("alice"), 1, durable.work());
    assertEquals(Records.State.SUCCEEDED, durableView.state());
    assertNull(durableView.manifest());
    assertNull(durableView.outputUntil());
    durable.inputs().close();

    Fixture results = fixture("zero-results", new byte[0], 0, 0, 1, true);
    Records.WorkView resultsView =
        runtime(results, context -> ExecutionRuntime.Outcome.succeeded(), 1, 1)
            .run(execAccess("alice"), 1, results.work());
    assertEquals(Records.State.SUCCEEDED, resultsView.state());
    assertNotNull(resultsView.manifest());
    assertTrue(resultsView.manifest().outputs().isEmpty());
    assertNotNull(resultsView.outputUntil());
    results.inputs().close();
  }

  @Test
  void unfinishedOutputIsStickyAndWrongThreadUseIsRejectedWhileCallbackLives() throws Exception {
    Fixture unfinished = fixture("unfinished", new byte[0], 1, 1, 1);
    Records.WorkView failed =
        runtime(
                unfinished,
                context -> {
                  context.beginOutput(1, "application/octet-stream");
                  return ExecutionRuntime.Outcome.succeeded();
                },
                1,
                1)
            .run(execAccess("alice"), 1, unfinished.work());
    assertEquals(Records.State.FAILED, failed.state());
    assertEquals(8, failed.diagnostic().code());
    assertNull(failed.manifest());
    unfinished.inputs().close();

    Fixture threaded = fixture("wrong-thread", new byte[0], 0, 0, 1);
    AtomicReference<Throwable> refusal = new AtomicReference<>();
    Records.WorkView succeeded =
        runtime(
                threaded,
                context -> {
                  Thread worker =
                      Thread.ofVirtual()
                          .start(
                              () -> {
                                try {
                                  context.check();
                                } catch (Throwable error) {
                                  refusal.set(error);
                                }
                              });
                  worker.join();
                  ProtocolError error = assertInstanceOf(ProtocolError.class, refusal.get());
                  assertEquals(ProtocolError.Code.CONFLICT, error.code(), error::getMessage);
                  context.check();
                  return ExecutionRuntime.Outcome.succeeded();
                },
                1,
                1)
            .run(execAccess("alice"), 1, threaded.work());
    assertEquals(Records.State.FAILED, succeeded.state());
    assertEquals(7, succeeded.diagnostic().code());
    assertNull(succeeded.manifest());
    threaded.inputs().close();
  }

  @Test
  void unfinishedOutputCleanupPreservesExplicitRetryableDisposition() throws Exception {
    Fixture fixture = fixture("unfinished-retry", new byte[0], 1, 100, 1);
    Records.Diagnostic diagnostic = new Records.Diagnostic(6, "retry after partial output");
    Records.WorkView view =
        runtime(
                fixture,
                context -> {
                  context.beginOutput(100, "application/octet-stream");
                  context.writeOutput(ByteBuffer.wrap(new byte[10]));
                  return ExecutionRuntime.Outcome.retryable(diagnostic);
                },
                1,
                1)
            .run(execAccess("alice"), 1, fixture.work());
    assertEquals(Records.State.AWAITING_RETRY, view.state());
    assertEquals(diagnostic, view.diagnostic());
    assertEquals(0, fixture.inputs().usage().handles());
    fixture.inputs().close();
  }

  @Test
  void contextIsThreadAndInvocationBoundAndCapacityHasNoWaitingQueue() throws Exception {
    Fixture captured = fixture("captured", new byte[0], 0, 0, 1);
    AtomicReference<ExecutionRuntime.Context> contextRef = new AtomicReference<>();
    runtime(
            captured,
            context -> {
              contextRef.set(context);
              return ExecutionRuntime.Outcome.succeeded();
            },
            2,
            2)
        .run(execAccess("alice"), 1, captured.work());
    assertCode(ProtocolError.Code.CONFLICT, contextRef.get()::check);
    captured.inputs().close();

    Fixture first = fixture("capacity-a", new byte[0], 0, 0, 1);
    Records.WorkKey second = admitSecond(first);
    CountDownLatch entered = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    ExecutionRuntime runtime =
        new ExecutionRuntime(
            first.sessions(),
            first.inputs(),
            List.of(
                new ExecutionRuntime.Registration(
                    first.app(),
                    context -> {
                      entered.countDown();
                      assertTrue(release.await(5, TimeUnit.SECONDS));
                      return ExecutionRuntime.Outcome.succeeded();
                    })),
            ENDPOINT,
            clock(1100),
            ALLOW,
            new ExecutionRuntime.Limits(1, 1, 500, 128));
    try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
      var running = executor.submit(() -> runtime.run(execAccess("alice"), 1, first.work()));
      assertTrue(entered.await(5, TimeUnit.SECONDS));
      assertCode(
          ProtocolError.Code.LIMIT_EXCEEDED, () -> runtime.run(execAccess("alice"), 1, second));
      release.countDown();
      assertEquals(Records.State.SUCCEEDED, running.get(5, TimeUnit.SECONDS).state());
    } finally {
      release.countDown();
      first.inputs().close();
    }
  }

  @Test
  void perOwnerCeilingDoesNotConsumeTheIndependentGlobalWorkerSlot() throws Exception {
    Fixture fixture = fixture("owner-capacity", new byte[0], 0, 0, 1);
    Records.WorkKey aliceSecond = admitSecond(fixture);
    OwnerWork bob = admitForOwner(fixture, "bob", 1, 21);
    CountDownLatch aliceEntered = new CountDownLatch(1);
    CountDownLatch bothEntered = new CountDownLatch(2);
    CountDownLatch release = new CountDownLatch(1);
    ExecutionRuntime runtime =
        new ExecutionRuntime(
            fixture.sessions(),
            fixture.inputs(),
            List.of(
                new ExecutionRuntime.Registration(
                    fixture.app(),
                    context -> {
                      aliceEntered.countDown();
                      bothEntered.countDown();
                      assertTrue(release.await(5, TimeUnit.SECONDS));
                      return ExecutionRuntime.Outcome.succeeded();
                    })),
            ENDPOINT,
            clock(1100),
            ALLOW,
            new ExecutionRuntime.Limits(2, 1, 500, 128));
    try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
      var alice = executor.submit(() -> runtime.run(execAccess("alice"), 1, fixture.work()));
      assertTrue(aliceEntered.await(5, TimeUnit.SECONDS));
      assertCode(
          ProtocolError.Code.LIMIT_EXCEEDED,
          () -> runtime.run(execAccess("alice"), 1, aliceSecond));
      var otherOwner =
          executor.submit(
              () -> runtime.run(execAccess("bob"), bob.binding().generation(), bob.work()));
      assertTrue(bothEntered.await(5, TimeUnit.SECONDS));
      release.countDown();
      assertEquals(Records.State.SUCCEEDED, alice.get(5, TimeUnit.SECONDS).state());
      assertEquals(Records.State.SUCCEEDED, otherOwner.get(5, TimeUnit.SECONDS).state());
    } finally {
      release.countDown();
      fixture.inputs().close();
    }
  }

  @Test
  void committedReplacementFencePreventsStaleCallbackPublication() throws Exception {
    Fixture fixture = fixture("replacement-fence", new byte[0], 0, 0, 1);
    CountDownLatch entered = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    AtomicLong now = new AtomicLong(1100);
    ExecutionRuntime runtime =
        new ExecutionRuntime(
            fixture.sessions(),
            fixture.inputs(),
            List.of(
                new ExecutionRuntime.Registration(
                    fixture.app(),
                    context -> {
                      entered.countDown();
                      assertTrue(release.await(5, TimeUnit.SECONDS));
                      return ExecutionRuntime.Outcome.succeeded();
                    })),
            ENDPOINT,
            () -> new AdmissionStore.Time(now.get(), true),
            ALLOW,
            new ExecutionRuntime.Limits(1, 1, 500, 128));
    try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
      var stale = executor.submit(() -> runtime.run(execAccess("alice"), 1, fixture.work()));
      assertTrue(entered.await(5, TimeUnit.SECONDS));
      now.set(1600);
      ExecutionStore.Lease replacement =
          fixture
              .sessions()
              .claimExecution(
                  execAccess("alice"),
                  1,
                  fixture.work(),
                  fixture.inputs(),
                  400,
                  () -> new AdmissionStore.Time(now.get(), true),
                  ALLOW);
      assertEquals(2, replacement.number());
      release.countDown();
      var failure =
          assertThrows(
              java.util.concurrent.ExecutionException.class, () -> stale.get(5, TimeUnit.SECONDS));
      ProtocolError conflict = assertInstanceOf(ProtocolError.class, failure.getCause());
      assertEquals(ProtocolError.Code.CONFLICT, conflict.code(), conflict::getMessage);
      Records.WorkView retained = snapshot(fixture.sessions(), fixture.work());
      assertEquals(Records.State.ACTIVE, retained.state());
      assertNull(retained.manifest());
    } finally {
      release.countDown();
      fixture.inputs().close();
    }
  }

  @Test
  void caughtUncheckedAuthorizationFailureCannotBeConvertedToSuccess() throws Exception {
    Fixture fixture = fixture("caught-authorization", new byte[] {1}, 0, 0, 1);
    AtomicReference<IllegalStateException> observed = new AtomicReference<>();
    AtomicBoolean armed = new AtomicBoolean();
    AdmissionStore.Authorization authorization =
        (binding, parameters) -> {
          if (armed.getAndSet(false)) throw new IllegalStateException("withdrawn");
        };
    ExecutionRuntime runtime =
        new ExecutionRuntime(
            fixture.sessions(),
            fixture.inputs(),
            List.of(
                new ExecutionRuntime.Registration(
                    fixture.app(),
                    context -> {
                      try {
                        armed.set(true);
                        context.readInput(new byte[1], 0, 1);
                      } catch (IllegalStateException error) {
                        observed.set(error);
                      }
                      return ExecutionRuntime.Outcome.succeeded();
                    })),
            ENDPOINT,
            clock(1100),
            authorization,
            new ExecutionRuntime.Limits(1, 1, 500, 128));
    Records.WorkView view = runtime.run(execAccess("alice"), 1, fixture.work());
    assertNotNull(observed.get());
    assertEquals("withdrawn", observed.get().getMessage());
    assertEquals(Records.State.FAILED, view.state());
    assertEquals(15, view.diagnostic().code());
    assertNull(view.manifest());
    fixture.inputs().close();
  }

  @Test
  void frozenUtcRenewalsCannotRestartTheSameMonotonicLeaseForever() throws Exception {
    Fixture fixture = fixture("frozen-renew", new byte[0], 0, 0, 1);
    java.util.concurrent.atomic.AtomicInteger renewals =
        new java.util.concurrent.atomic.AtomicInteger();
    AtomicReference<ProtocolError> expired = new AtomicReference<>();
    ExecutionRuntime runtime =
        new ExecutionRuntime(
            fixture.sessions(),
            fixture.inputs(),
            List.of(
                new ExecutionRuntime.Registration(
                    fixture.app(),
                    context -> {
                      for (int attempt = 0; attempt < 200; attempt++) {
                        Thread.sleep(5);
                        try {
                          context.renew();
                          renewals.incrementAndGet();
                        } catch (ProtocolError refusal) {
                          expired.set(refusal);
                          break;
                        }
                      }
                      assertNotNull(expired.get(), "frozen UTC kept renewing one durable lease");
                      return ExecutionRuntime.Outcome.succeeded();
                    })),
            ENDPOINT,
            clock(1100),
            ALLOW,
            new ExecutionRuntime.Limits(1, 1, 500, 128));
    assertCode(
        ProtocolError.Code.CONFLICT, () -> runtime.run(execAccess("alice"), 1, fixture.work()));
    assertTrue(renewals.get() >= 1, "fixture never exercised a successful renewal");
    assertEquals(ProtocolError.Code.CONFLICT, expired.get().code(), expired.get()::getMessage);
    Records.WorkView view = snapshot(fixture.sessions(), fixture.work());
    assertEquals(Records.State.ACTIVE, view.state());
    assertNull(view.manifest());
    fixture.inputs().close();
  }

  private static Records.WorkKey admitSecond(Fixture fixture) throws Exception {
    Records.WorkKey work = new Records.WorkKey(0, 0, 2);
    fixture
        .sessions()
        .declare(
            sessionAccess("alice"),
            SELECTED,
            1,
            new Messages.Declare(3, operation(3), 0, List.of(2L), false));
    Records.InputHeader header = header(new byte[0], 0, 0, work, operation(4));
    try (InputStore.Receiver receiver =
        fixture.inputs().begin(context("alice"), header, SELECTED, 1)) {
      receiver.write(ByteBuffer.allocate(0), 2);
      receiver.finish(3);
    }
    fixture
        .sessions()
        .admit(
            sessionAccess("alice"), SELECTED, 1, fixture.inputs(), header, 4, clock(1000), ALLOW);
    return work;
  }

  private static OwnerWork admitForOwner(
      Fixture fixture, String owner, long entity, int operationBase) throws Exception {
    Messages.Binding binding =
        fixture
            .sessions()
            .create(
                sessionAccess(owner),
                SELECTED,
                new Messages.Create(20, 1, new Records.Policy(10_000, 20_000, 30_000)));
    long generation = binding.generation();
    Records.WorkKey work = new Records.WorkKey(0, 0, entity);
    fixture
        .sessions()
        .declare(
            sessionAccess(owner),
            SELECTED,
            generation,
            new Messages.Declare(21, operation(operationBase), 0, List.of(entity), false));
    Records.InputHeader header =
        header(new byte[0], 0, 0, work, operation(operationBase + 1), generation);
    Commitments.Context context = context(owner, generation);
    try (InputStore.Receiver receiver =
        fixture.inputs().begin(context, header, SELECTED, generation)) {
      receiver.write(ByteBuffer.allocate(0), 2);
      receiver.finish(3);
    }
    fixture
        .sessions()
        .admit(
            sessionAccess(owner),
            SELECTED,
            generation,
            fixture.inputs(),
            header,
            22,
            clock(1000),
            ALLOW);
    return new OwnerWork(binding, work);
  }

  private Fixture fixture(String name, byte[] payload, int outputs, long outputBytes, long entity)
      throws Exception {
    return fixture(name, payload, outputs, outputBytes, entity, true);
  }

  private Fixture fixture(
      String name, byte[] payload, int outputs, long outputBytes, long entity, boolean results)
      throws Exception {
    AdmissionStore.Application app =
        new AdmissionStore.Application("copy", Set.of(0), AdmissionStore.RestartSafety.IDEMPOTENT);
    Path database = directory.resolve(name + ".sqlite");
    Path inputsPath = directory.resolve(name + "-inputs");
    SessionStore sessions = SessionStore.initialize(database, configuration(app));
    sessions.create(
        sessionAccess("alice"),
        selected(results),
        new Messages.Create(1, 1, new Records.Policy(10_000, 20_000, 30_000)));
    Records.WorkKey work = new Records.WorkKey(0, 0, entity);
    Messages.Capabilities selected = selected(results);
    sessions.declare(
        sessionAccess("alice"),
        selected,
        1,
        new Messages.Declare(2, operation(1), 0, List.of(entity), false));
    InputStore inputs =
        InputStore.initializeForAuthority(inputsPath, INPUT_LIMITS, sessions.identity());
    sessions.bindInputs(inputs);
    Records.InputHeader header = header(payload, outputs, outputBytes, work);
    try (InputStore.Receiver receiver = inputs.begin(context("alice"), header, selected, 1)) {
      receiver.write(ByteBuffer.wrap(payload), 2);
      receiver.finish(3);
    }
    sessions.admit(sessionAccess("alice"), selected, 1, inputs, header, 2, clock(1000), ALLOW);
    return new Fixture(database, inputsPath, sessions, inputs, app, work, selected);
  }

  private static Messages.Capabilities selected(boolean results) {
    return new Messages.Capabilities(
        true,
        results ? List.of(DURABLE_WORK, RESULT_DELIVERY) : List.of(DURABLE_WORK),
        List.of(),
        1 << 20,
        8,
        16,
        1 << 20,
        1000,
        5000);
  }

  private static ExecutionRuntime runtime(
      Fixture fixture, ExecutionRuntime.Callback callback, int workers, int ownerWorkers) {
    return new ExecutionRuntime(
        fixture.sessions(),
        fixture.inputs(),
        List.of(new ExecutionRuntime.Registration(fixture.app(), callback)),
        ENDPOINT,
        clock(1100),
        ALLOW,
        new ExecutionRuntime.Limits(workers, ownerWorkers, 500, 128));
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
        .snapshot(sessionAccess("alice"), SELECTED, 1, new Messages.Watch(9, work, 0, 0))
        .work();
  }

  private static Records.InputHeader header(
      byte[] payload, int outputs, long outputBytes, Records.WorkKey work) throws Exception {
    return header(payload, outputs, outputBytes, work, operation(2));
  }

  private static Records.InputHeader header(
      byte[] payload,
      int outputs,
      long outputBytes,
      Records.WorkKey work,
      Records.OperationId operation)
      throws Exception {
    return header(payload, outputs, outputBytes, work, operation, 1);
  }

  private static Records.InputHeader header(
      byte[] payload,
      int outputs,
      long outputBytes,
      Records.WorkKey work,
      Records.OperationId operation,
      long generation)
      throws Exception {
    return new Records.InputHeader(
        generation,
        operation,
        new Records.AdmitParameters(
            work,
            new Records.Input(payload.length, digest(payload), "application/octet-stream"),
            "copy",
            0,
            1000,
            new Records.OutputBudget(outputs, outputBytes)));
  }

  private static Records.Digest digest(byte[] bytes) throws Exception {
    return new Records.Digest(MessageDigest.getInstance("SHA-256").digest(bytes));
  }

  private static AdmissionStore.Clock clock(long time) {
    return () -> new AdmissionStore.Time(time, true);
  }

  private static Commitments.Context context(String owner) {
    return context(owner, 1);
  }

  private static Commitments.Context context(String owner, long generation) {
    return new Commitments.Context("issuer-a", owner, generation);
  }

  private static Records.OperationId operation(int value) {
    byte[] bytes = new byte[16];
    bytes[15] = (byte) value;
    return new Records.OperationId(bytes);
  }

  private static SessionStore.Access sessionAccess(String owner) {
    return new SessionStore.Access(owner, () -> {});
  }

  private static ExecutionStore.Access execAccess(String owner) {
    return new ExecutionStore.Access(owner, () -> {});
  }

  private static void assertCode(ProtocolError.Code expected, Throwing action) {
    ProtocolError error = assertThrows(ProtocolError.class, action::run);
    assertEquals(expected, error.code(), error::getMessage);
  }

  private record Fixture(
      Path database,
      Path inputsPath,
      SessionStore sessions,
      InputStore inputs,
      AdmissionStore.Application app,
      Records.WorkKey work,
      Messages.Capabilities selected) {}

  private record OwnerWork(Messages.Binding binding, Records.WorkKey work) {}

  @FunctionalInterface
  private interface Throwing {
    void run() throws Exception;
  }
}
