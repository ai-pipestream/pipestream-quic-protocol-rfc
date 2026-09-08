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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

@Timeout(30)
final class AuthorityExpansionRuntimeFailureTest {
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
  void yieldedExpansionReplaysDeclarationAndAdmissionWithoutChangingChildIdentity()
      throws Exception {
    try (Fixture fixture = new Fixture("replay", 8)) {
      fixture.prepare();
      AtomicInteger calls = new AtomicInteger();
      AtomicReference<Records.OperationReceipt> declaration = new AtomicReference<>();
      AtomicReference<Records.OperationReceipt> admission = new AtomicReference<>();
      ExecutionRuntime runtime =
          fixture.runtime(
              ALLOW,
              context -> ExecutionRuntime.Outcome.succeeded(),
              context -> {
                int call = calls.incrementAndGet();
                Records.OperationReceipt declared =
                    context.declare(operation(20), List.of(10L), true).receipt();
                Records.AdmitParameters child = fixture.child(context.childScope(), 10, 5000);
                java.util.Optional<Records.OperationReceipt> replay =
                    context.beginInput(operation(21), child);
                if (call == 1) {
                  assertTrue(replay.isEmpty());
                  context.writeInput(ByteBuffer.wrap(new byte[] {1, 2, 3}));
                  admission.set(context.finishInput());
                  declaration.set(declared);
                  return ExecutionRuntime.ExpansionOutcome.yielded();
                }
                assertEquals(declaration.get(), declared);
                assertEquals(admission.get(), replay.orElseThrow());
                return ExecutionRuntime.ExpansionOutcome.complete();
              });

      Records.WorkView yielded = runtime.run(execAccess(), fixture.generation, fixture.parent);
      assertEquals(Records.State.ACTIVE, yielded.state());
      assertEquals(1, yielded.attempt());
      assertEquals(0, fixture.inputs.usage().handles());
      Records.WorkKey child = new Records.WorkKey(yielded.child().scope(), 1, 10);
      Records.WorkView admitted = fixture.view(child);
      assertEquals(Records.State.ACTIVE, admitted.state());
      assertEquals(1, admitted.attempt());

      Records.WorkView completed = runtime.run(execAccess(), fixture.generation, fixture.parent);
      assertEquals(2, calls.get());
      assertEquals(Records.State.WAITING_CHILDREN, completed.state());
      assertEquals(1, completed.attempt());
      assertEquals(admitted, fixture.view(child));
      assertEquals(2, fixture.jobCount());
      assertEquals(0, fixture.inputs.usage().handles());
      fixture.reopen();
      assertEquals(completed, fixture.view(fixture.parent));
      assertEquals(admitted, fixture.view(child));
    }
  }

  @Test
  void unfinishedReceiveAndSwallowedMisuseCannotCompleteExpansion() throws Exception {
    try (Fixture unfinished = new Fixture("unfinished", 8)) {
      unfinished.prepare();
      AtomicInteger reached = new AtomicInteger();
      Records.WorkView failed =
          unfinished
              .runtime(
                  ALLOW,
                  context -> ExecutionRuntime.Outcome.succeeded(),
                  context -> {
                    context.declare(operation(30), List.of(10L), true);
                    assertTrue(
                        context
                            .beginInput(
                                operation(31), unfinished.child(context.childScope(), 10, 5000))
                            .isEmpty());
                    context.writeInput(ByteBuffer.wrap(new byte[] {1}));
                    reached.incrementAndGet();
                    return ExecutionRuntime.ExpansionOutcome.complete();
                  })
              .run(execAccess(), unfinished.generation, unfinished.parent);
      assertEquals(1, reached.get());
      assertEquals(Records.State.FAILED, failed.state());
      assertEquals(ProtocolError.Code.INTEGRITY_ERROR.value(), failed.diagnostic().code());
      assertNull(failed.manifest());
      assertEquals(0, unfinished.inputs.usage().handles());
    }

    try (Fixture misuse = new Fixture("misuse", 8)) {
      misuse.prepare();
      AtomicReference<ProtocolError> swallowed = new AtomicReference<>();
      Records.WorkView failed =
          misuse
              .runtime(
                  ALLOW,
                  context -> ExecutionRuntime.Outcome.succeeded(),
                  context -> {
                    try {
                      context.writeInput(ByteBuffer.wrap(new byte[] {1}));
                    } catch (ProtocolError refusal) {
                      swallowed.set(refusal);
                    }
                    return ExecutionRuntime.ExpansionOutcome.complete();
                  })
              .run(execAccess(), misuse.generation, misuse.parent);
      assertEquals(ProtocolError.Code.CONFLICT, swallowed.get().code());
      assertEquals(Records.State.FAILED, failed.state());
      assertEquals(ProtocolError.Code.CONFLICT.value(), failed.diagnostic().code());
      assertEquals(0, misuse.inputs.usage().handles());
    }
  }

  @Test
  void expansionContextIsThreadAndInvocationBound() throws Exception {
    try (Fixture fixture = new Fixture("context", 8)) {
      fixture.prepare();
      AtomicReference<ExecutionRuntime.ExpansionContext> captured = new AtomicReference<>();
      AtomicReference<ProtocolError> wrongThread = new AtomicReference<>();
      Records.WorkView failed =
          fixture
              .runtime(
                  ALLOW,
                  context -> ExecutionRuntime.Outcome.succeeded(),
                  context -> {
                    captured.set(context);
                    Thread thread =
                        Thread.ofVirtual()
                            .start(
                                () -> {
                                  try {
                                    context.check();
                                  } catch (ProtocolError refusal) {
                                    wrongThread.set(refusal);
                                  } catch (Exception unexpected) {
                                    throw new AssertionError(unexpected);
                                  }
                                });
                    thread.join();
                    return ExecutionRuntime.ExpansionOutcome.complete();
                  })
              .run(execAccess(), fixture.generation, fixture.parent);
      assertEquals(ProtocolError.Code.CONFLICT, wrongThread.get().code());
      assertEquals(Records.State.FAILED, failed.state());
      assertEquals(ProtocolError.Code.CONFLICT.value(), failed.diagnostic().code());
      assertCode(ProtocolError.Code.CONFLICT, captured.get()::check);
      assertEquals(0, fixture.inputs.usage().handles());
    }
  }

  @Test
  void revokedCurrentParentFenceStopsLocalInputAndReleasesReceiverCredit() throws Exception {
    try (Fixture fixture = new Fixture("revoked", 8)) {
      fixture.prepare();
      AtomicBoolean revoked = new AtomicBoolean();
      AdmissionStore.Authorization authorization =
          (binding, parameters) -> {
            if (revoked.get()) throw new ProtocolError(ProtocolError.Code.UNAUTHORIZED, "revoked");
          };
      AtomicInteger reached = new AtomicInteger();
      ExecutionRuntime runtime =
          fixture.runtime(
              authorization,
              context -> ExecutionRuntime.Outcome.succeeded(),
              context -> {
                context.declare(operation(40), List.of(10L), true);
                revoked.set(true);
                reached.incrementAndGet();
                context.beginInput(operation(41), fixture.child(context.childScope(), 10, 5000));
                return ExecutionRuntime.ExpansionOutcome.complete();
              });
      assertCode(
          ProtocolError.Code.UNAUTHORIZED,
          () -> runtime.run(execAccess(), fixture.generation, fixture.parent));
      assertEquals(1, reached.get());
      Records.WorkView parent = fixture.view(fixture.parent);
      assertEquals(Records.State.ACTIVE, parent.state());
      assertNull(parent.manifest());
      assertEquals(
          Records.State.DECLARED,
          fixture.view(new Records.WorkKey(parent.child().scope(), 1, 10)).state());
      assertEquals(0, fixture.inputs.usage().handles());
    }
  }

  @Test
  void childJobCapacityLimitYieldsAndLaterReplaysAcceptedWorkWithoutNewAttempt() throws Exception {
    try (Fixture fixture = new Fixture("capacity", 2)) {
      fixture.prepare();
      AtomicInteger expansionCalls = new AtomicInteger();
      ExecutionRuntime runtime =
          fixture.runtime(
              ALLOW,
              context -> ExecutionRuntime.Outcome.succeeded(),
              context -> {
                int call = expansionCalls.incrementAndGet();
                context.declare(operation(50), List.of(10L, 20L), true);
                for (long entity : List.of(10L, 20L)) {
                  java.util.Optional<Records.OperationReceipt> replay =
                      context.beginInput(
                          operation((int) (51 + entity)),
                          fixture.child(context.childScope(), entity, 5000));
                  if (replay.isEmpty()) {
                    context.writeInput(ByteBuffer.wrap(new byte[] {1, 2, 3}));
                    context.finishInput();
                  }
                }
                return call == 1
                    ? ExecutionRuntime.ExpansionOutcome.yielded()
                    : ExecutionRuntime.ExpansionOutcome.complete();
              });

      Records.WorkView yielded = runtime.run(execAccess(), fixture.generation, fixture.parent);
      assertEquals(Records.State.ACTIVE, yielded.state());
      assertEquals(1, yielded.attempt());
      assertEquals(1, expansionCalls.get());
      long scope = yielded.child().scope();
      Records.WorkKey first = new Records.WorkKey(scope, 1, 10);
      Records.WorkKey second = new Records.WorkKey(scope, 1, 20);
      Records.WorkView accepted = fixture.view(first);
      assertEquals(Records.State.ACTIVE, accepted.state());
      assertEquals(Records.State.DECLARED, fixture.view(second).state());
      assertEquals(0, fixture.inputs.usage().handles());

      assertEquals(
          Records.State.SUCCEEDED, runtime.run(execAccess(), fixture.generation, first).state());
      Records.WorkView completed = runtime.run(execAccess(), fixture.generation, fixture.parent);
      assertEquals(2, expansionCalls.get());
      assertEquals(Records.State.WAITING_CHILDREN, completed.state());
      assertEquals(1, completed.attempt());
      assertEquals(accepted.attempt(), fixture.view(first).attempt());
      Records.WorkView recovered = fixture.view(second);
      assertEquals(Records.State.ACTIVE, recovered.state());
      assertEquals(1, recovered.attempt());
      assertEquals(3, fixture.jobCount());
      assertEquals(0, fixture.inputs.usage().handles());
    }
  }

  @Test
  void wrongThreadConflictOverridesEarlierRecoverableCapacityYield() throws Exception {
    try (Fixture fixture = new Fixture("limit-then-thread", 2)) {
      fixture.prepare();
      AtomicReference<ProtocolError> capacity = new AtomicReference<>();
      AtomicReference<ProtocolError> wrongThread = new AtomicReference<>();
      ExecutionRuntime runtime =
          fixture.runtime(
              ALLOW,
              context -> ExecutionRuntime.Outcome.succeeded(),
              context -> {
                context.declare(operation(80), List.of(10L, 20L), true);
                for (long entity : List.of(10L, 20L)) {
                  try {
                    context.beginInput(
                        operation((int) (81 + entity)),
                        fixture.child(context.childScope(), entity, 5000));
                    context.writeInput(ByteBuffer.wrap(new byte[] {1, 2, 3}));
                    context.finishInput();
                  } catch (ProtocolError refusal) {
                    capacity.set(refusal);
                    break;
                  }
                }
                Thread thread =
                    Thread.ofVirtual()
                        .start(
                            () -> {
                              try {
                                context.check();
                              } catch (ProtocolError refusal) {
                                wrongThread.set(refusal);
                              } catch (Exception unexpected) {
                                throw new AssertionError(unexpected);
                              }
                            });
                thread.join();
                return ExecutionRuntime.ExpansionOutcome.yielded();
              });

      Records.WorkView failed = runtime.run(execAccess(), fixture.generation, fixture.parent);
      assertEquals(ProtocolError.Code.LIMIT_EXCEEDED, capacity.get().code());
      assertEquals(ProtocolError.Code.CONFLICT, wrongThread.get().code());
      assertEquals(Records.State.FAILED, failed.state());
      assertEquals(ProtocolError.Code.CONFLICT.value(), failed.diagnostic().code());
      long scope = failed.child().scope();
      Records.WorkView accepted = fixture.view(new Records.WorkKey(scope, 1, 10));
      assertEquals(Records.State.ACTIVE, accepted.state());
      assertEquals(1, accepted.attempt());
      assertEquals(Records.State.DECLARED, fixture.view(new Records.WorkKey(scope, 1, 20)).state());
      assertEquals(0, fixture.inputs.usage().handles());
    }
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
    final int maxJobs;
    SessionStore sessions;
    InputStore inputs;
    int request = 10;

    Fixture(String name, int maxJobs) throws Exception {
      this.maxJobs = maxJobs;
      database = directory.resolve(name + ".sqlite");
      inputPath = directory.resolve(name + "-inputs");
      sessions = SessionStore.initialize(database, configuration(application, maxJobs));
      binding =
          sessions.create(
              access(),
              SELECTED,
              new Messages.Create(1, 1, new Records.Policy(10_000, 20_000, 30_000)));
      generation = binding.generation();
      inputs = InputStore.initializeForAuthority(inputPath, INPUT_LIMITS, sessions.identity());
      sessions.bindInputs(inputs);
    }

    void prepare() throws Exception {
      sessions.declare(
          access(),
          SELECTED,
          generation,
          new Messages.Declare(request++, operation(request++), 0, List.of(1L), true));
      Records.InputHeader header =
          new Records.InputHeader(
              generation,
              operation(request++),
              new Records.AdmitParameters(
                  parent,
                  new Records.Input(0, digest(new byte[0]), "application/octet-stream"),
                  "expand",
                  2,
                  5000,
                  new Records.OutputBudget(0, 0)));
      try (InputStore.Receiver receiver = inputs.begin(context(), header, SELECTED, 1000)) {
        receiver.finish(1000);
      }
      sessions.admit(access(), SELECTED, generation, inputs, header, request++, clock(), ALLOW);
    }

    Records.AdmitParameters child(long scope, long entity, long executionMillis) throws Exception {
      byte[] payload = {1, 2, 3};
      return new Records.AdmitParameters(
          new Records.WorkKey(scope, 1, entity),
          new Records.Input(payload.length, digest(payload), "application/octet-stream"),
          "expand",
          0,
          executionMillis,
          new Records.OutputBudget(0, 0));
    }

    ExecutionRuntime runtime(
        AdmissionStore.Authorization authorization,
        ExecutionRuntime.Callback callback,
        ExecutionRuntime.Expander expander) {
      return new ExecutionRuntime(
          sessions,
          inputs,
          List.of(new ExecutionRuntime.Registration(application, callback, expander)),
          ENDPOINT,
          clock(),
          authorization,
          new ExecutionRuntime.Limits(1, 1, 500, 128),
          SELECTED);
    }

    Records.WorkView view(Records.WorkKey work) throws Exception {
      return sessions
          .snapshot(access(), SELECTED, generation, new Messages.Watch(request++, work, 0, 0))
          .work();
    }

    long jobCount() throws Exception {
      try (var connection =
              BoundedSqlite.open(database, configuration(application, maxJobs).files()).connect();
          var statement = connection.createStatement();
          var rows = statement.executeQuery("SELECT count(*) FROM ps_v2_jobs")) {
        assertTrue(rows.next());
        return rows.getLong(1);
      }
    }

    Commitments.Context context() {
      return new Commitments.Context("issuer-a", "alice", generation);
    }

    void reopen() throws Exception {
      inputs.close();
      sessions = SessionStore.open(database, configuration(application, maxJobs));
      inputs = InputStore.open(inputPath, INPUT_LIMITS);
      sessions.verifyInputs(inputs);
    }

    @Override
    public void close() throws java.io.IOException {
      inputs.close();
    }
  }

  private static SessionStore.Configuration configuration(
      AdmissionStore.Application application, int maxJobs) {
    return new SessionStore.Configuration(
        "issuer-a",
        new Records.Limits(16, 64, 64, 1 << 20, 1 << 20, 8),
        new Records.Policy(60_000, 60_000, 60_000),
        4,
        16,
        4,
        BoundedSqlite.Limits.defaults(),
        new AdmissionStore.ExecutionPolicy(List.of(application), maxJobs, maxJobs));
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

  private static void assertCode(ProtocolError.Code expected, Throwing action) {
    ProtocolError error = assertThrows(ProtocolError.class, action::run);
    assertEquals(expected, error.code(), error::getMessage);
  }

  @FunctionalInterface
  private interface Throwing {
    void run() throws Exception;
  }
}
