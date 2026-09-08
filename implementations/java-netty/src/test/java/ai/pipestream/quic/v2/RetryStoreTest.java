package ai.pipestream.quic.v2;

import static ai.pipestream.quic.v2.Messages.DURABLE_WORK;
import static org.junit.jupiter.api.Assertions.*;

import ai.pipestream.quic.BoundedSqlite;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.sql.Connection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

@Timeout(30)
final class RetryStoreTest {
  private static final Messages.Capabilities SELECTED =
      new Messages.Capabilities(
          true, List.of(DURABLE_WORK), List.of(), 1 << 20, 16, 32, 1 << 20, 1000, 10_000);
  private static final InputStore.Limits INPUT_LIMITS =
      new InputStore.Limits(16L << 20, 128, 1 << 20, 8);
  private static final AdmissionStore.Authorization ALLOW = (binding, parameters) -> {};
  private static final Records.WorkKey WORK = new Records.WorkKey(0, 0, 1);

  @TempDir Path directory;

  @Test
  void activeRetryFencesLeaseAdvancesAttemptOnceAndReplaysAcrossReopen() throws Exception {
    try (Fixture fixture = new Fixture("active", 0)) {
      ExecutionStore.Lease stale = fixture.claim(1100, 500);
      long leaseNumber = stale.number();
      long[] before = fixture.credits();
      Messages.Retry request = new Messages.Retry(20, operation(20), WORK, 1);

      Messages.RetryResponse first = fixture.retry(request, 1200, ALLOW);
      Records.Retried outcome = assertInstanceOf(Records.Retried.class, first.receipt().outcome());
      assertEquals(WORK, outcome.work());
      assertEquals(1, outcome.expectedAttempt());
      assertEquals(2, outcome.replacementAttempt());
      assertEquals(1200, outcome.acceptedAt());
      assertEquals(operation(20), first.receipt().operation());
      assertEquals(20, first.request());
      assertReplacement(fixture, 2, leaseNumber, true);
      assertTrue(fixture.credits()[0] >= 4);
      assertTrue(fixture.credits()[1] >= 6);
      assertTrue(fixture.credits()[0] >= before[0]);
      assertTrue(fixture.credits()[1] >= before[1]);
      assertCode(
          ProtocolError.Code.CONFLICT,
          () -> fixture.sessions.checkExecution(execAccess(), stale, clock(1200), ALLOW));

      Messages.RetryResponse replay =
          fixture.retry(new Messages.Retry(21, operation(20), WORK, 1), 0, ALLOW);
      assertEquals(21, replay.request());
      assertEquals(first.receipt(), replay.receipt());
      assertReplacement(fixture, 2, leaseNumber, true);

      fixture.reopen();
      Messages.RetryResponse reopened =
          fixture.retry(new Messages.Retry(22, operation(20), WORK, 1), 0, ALLOW);
      assertEquals(first.receipt(), reopened.receipt());
      assertReplacement(fixture, 2, leaseNumber, true);
    }
  }

  @Test
  void awaitingRetryCanBeReplacedWhileTerminalAndDeadlineOnlyPermitExactReplay() throws Exception {
    try (Fixture fixture = new Fixture("awaiting", 0)) {
      ExecutionStore.Lease lease = fixture.claim(1100, 500);
      fixture.sessions.failExecution(
          execAccess(), lease, new Records.Diagnostic(7, "again"), true, clock(1200), ALLOW);
      assertEquals(Records.State.AWAITING_RETRY, fixture.view().state());
      Messages.Retry request = new Messages.Retry(30, operation(30), WORK, 1);
      Messages.RetryResponse receipt = fixture.retry(request, 1300, ALLOW);
      assertReplacement(fixture, 2, lease.number(), true);

      ExecutionStore.Lease replacement = fixture.claim(1400, 500);
      fixture.sessions.failExecution(
          execAccess(),
          replacement,
          new Records.Diagnostic(8, "terminal"),
          false,
          clock(1500),
          ALLOW);
      assertCode(
          ProtocolError.Code.ALREADY_TERMINAL,
          () -> fixture.retry(new Messages.Retry(31, operation(31), WORK, 2), 1500, ALLOW));
      assertEquals(
          receipt.receipt(),
          fixture.retry(new Messages.Retry(32, operation(30), WORK, 1), 0, ALLOW).receipt());
    }

    try (Fixture fixture = new Fixture("deadline", 0)) {
      Messages.Retry request = new Messages.Retry(40, operation(40), WORK, 1);
      Messages.RetryResponse receipt = fixture.retry(request, 1500, ALLOW);
      assertCode(
          ProtocolError.Code.DEADLINE_EXCEEDED,
          () -> fixture.retry(new Messages.Retry(41, operation(41), WORK, 2), 11_000, ALLOW));
      assertEquals(
          receipt.receipt(),
          fixture.retry(new Messages.Retry(42, operation(40), WORK, 1), 0, ALLOW).receipt());
      assertEquals(2, fixture.view().attempt());
    }
  }

  @Test
  void callerBranchRetryPreservesChildAndWaitingChildrenState() throws Exception {
    try (Fixture fixture = new Fixture("caller-branch", 1)) {
      Records.WorkView before = fixture.view();
      assertEquals(Records.State.WAITING_CHILDREN, before.state());
      assertNotNull(before.child());

      fixture.retry(new Messages.Retry(45, operation(45), WORK, 1), 1200, ALLOW);
      Records.WorkView after = fixture.view();
      assertEquals(Records.State.WAITING_CHILDREN, after.state());
      assertEquals(2, after.attempt());
      assertEquals(before.child(), after.child());
      JobRecord job = fixture.job();
      assertTrue(job.expansionComplete());
      assertEquals(JobRecord.Stage.WAITING_CHILDREN, job.stage());
      assertNull(job.leaseUntil());

      fixture.reopen();
      assertEquals(after, fixture.view());
    }
  }

  @Test
  void replacementAttemptCanPublishButOldLeaseCannot() throws Exception {
    try (Fixture fixture = new Fixture("replacement-publication", 0)) {
      ExecutionStore.Lease old = fixture.claim(1100, 500);
      fixture.retry(new Messages.Retry(46, operation(46), WORK, 1), 1200, ALLOW);
      assertCode(
          ProtocolError.Code.CONFLICT,
          () ->
              fixture.sessions.succeedExecution(
                  execAccess(),
                  old,
                  fixture.inputs,
                  0,
                  new PublicationStore.Endpoint("localhost:443"),
                  clock(1200),
                  ALLOW));

      ExecutionStore.Lease replacement = fixture.claim(1300, 500);
      Records.WorkView succeeded =
          fixture.sessions.succeedExecution(
              execAccess(),
              replacement,
              fixture.inputs,
              0,
              new PublicationStore.Endpoint("localhost:443"),
              clock(1400),
              ALLOW);
      assertEquals(Records.State.SUCCEEDED, succeeded.state());
      assertEquals(2, succeeded.attempt());
      assertNull(succeeded.manifest());
      fixture.reopen();
      assertEquals(succeeded, fixture.view());
    }
  }

  @Test
  void simultaneousDistinctRetryOperationsAdvanceOnlyOnce() throws Exception {
    try (Fixture fixture = new Fixture("simultaneous", 0)) {
      SessionStore other = SessionStore.open(fixture.database, configuration());
      CountDownLatch ready = new CountDownLatch(2);
      CountDownLatch start = new CountDownLatch(1);
      try (var executor = Executors.newFixedThreadPool(2)) {
        var first =
            executor.submit(
                () -> concurrentRetry(fixture.sessions, fixture.binding, 80, ready, start));
        var second =
            executor.submit(() -> concurrentRetry(other, fixture.binding, 81, ready, start));
        boolean allReady;
        try {
          allReady = ready.await(5, TimeUnit.SECONDS);
        } finally {
          start.countDown();
        }
        assertTrue(allReady);
        Object left = first.get(5, TimeUnit.SECONDS);
        Object right = second.get(5, TimeUnit.SECONDS);
        long successes =
            List.of(left, right).stream().filter(Messages.RetryResponse.class::isInstance).count();
        long conflicts =
            List.of(left, right).stream()
                .filter(ProtocolError.class::isInstance)
                .map(ProtocolError.class::cast)
                .filter(error -> error.code() == ProtocolError.Code.CONFLICT)
                .count();
        assertEquals(1, successes);
        assertEquals(1, conflicts);
      }
      assertEquals(2, fixture.view().attempt());
      fixture.reopen();
      assertEquals(2, fixture.view().attempt());
    }
  }

  @Test
  void completedExpansionPreservesChildAndReturnsToWaitingChildrenAfterRetry() throws Exception {
    try (Fixture fixture = new Fixture("branch", 2)) {
      ExecutionStore.Lease lease = fixture.claim(1100, 500);
      Records.ChildScope child = fixture.view().child();
      assertNotNull(child);
      fixture.sessions.declareProduced(
          execAccess(),
          lease,
          SELECTED,
          fixture.inputs,
          new Messages.Declare(50, operation(50), child.scope(), List.of(), true),
          clock(1200),
          ALLOW);
      Records.WorkView waiting =
          fixture.sessions.finishExpansion(execAccess(), lease, true, clock(1200), ALLOW);
      assertEquals(Records.State.WAITING_CHILDREN, waiting.state());
      assertEquals(child, waiting.child());

      Messages.RetryResponse response =
          fixture.retry(new Messages.Retry(51, operation(51), WORK, 1), 1300, ALLOW);
      assertInstanceOf(Records.Retried.class, response.receipt().outcome());
      Records.WorkView retried = fixture.view();
      assertEquals(Records.State.WAITING_CHILDREN, retried.state());
      assertEquals(2, retried.attempt());
      assertEquals(child, retried.child());
      JobRecord job = fixture.job();
      assertTrue(job.expansionComplete());
      assertEquals(JobRecord.Stage.WAITING_CHILDREN, job.stage());
      assertNull(job.leaseUntil());

      fixture.reopen();
      assertEquals(retried, fixture.view());
      assertTrue(fixture.job().expansionComplete());
    }
  }

  @Test
  void callerRetryOfProducedChildUsesIndependentProducerZeroOperationNamespace() throws Exception {
    try (Fixture fixture = new Fixture("produced-child", 2)) {
      ExecutionStore.Lease parent = fixture.claim(1100, 500);
      Records.ChildScope child = fixture.view().child();
      assertNotNull(child);
      fixture.sessions.declareProduced(
          execAccess(),
          parent,
          SELECTED,
          fixture.inputs,
          new Messages.Declare(90, operation(90), child.scope(), List.of(10L), true),
          clock(1200),
          ALLOW);
      Records.WorkKey childWork = new Records.WorkKey(child.scope(), child.producer(), 10);
      Records.InputHeader childHeader =
          new Records.InputHeader(
              fixture.binding.generation(),
              operation(91),
              new Records.AdmitParameters(
                  childWork,
                  new Records.Input(0, digest(new byte[0]), "application/octet-stream"),
                  "copy",
                  0,
                  10_000,
                  new Records.OutputBudget(0, 0)));
      try (InputStore.Receiver receiver =
          fixture.inputs.begin(fixture.context(), childHeader, SELECTED, 1200)) {
        receiver.write(ByteBuffer.allocate(0), 1200);
        receiver.finish(1200);
      }
      Records.OperationReceipt admission =
          fixture.sessions.admitProduced(
              execAccess(), parent, SELECTED, fixture.inputs, childHeader, clock(1200), ALLOW);

      Messages.RetryResponse retry =
          fixture.retry(new Messages.Retry(92, operation(91), childWork, 1), 1300, ALLOW);
      Records.Retried retried = assertInstanceOf(Records.Retried.class, retry.receipt().outcome());
      assertEquals(childWork, retried.work());
      assertEquals(2, fixture.view(childWork).attempt());
      assertEquals(1, fixture.view().attempt());
      assertEquals(
          retry.receipt(),
          fixture
              .sessions
              .lookupOperation(
                  sessionAccess(),
                  SELECTED,
                  fixture.binding.generation(),
                  new Messages.LookupOperation(93, operation(91)))
              .receipt());
      assertEquals(
          admission,
          fixture.sessions.admitProduced(
              execAccess(), parent, SELECTED, fixture.inputs, childHeader, clock(1300), ALLOW));

      fixture.reopen();
      assertEquals(2, fixture.view(childWork).attempt());
      assertEquals(1, fixture.view().attempt());
      assertEquals(
          retry.receipt(),
          fixture
              .sessions
              .lookupOperation(
                  sessionAccess(),
                  SELECTED,
                  fixture.binding.generation(),
                  new Messages.LookupOperation(94, operation(91)))
              .receipt());
    }
  }

  @Test
  void changedOperationExpectedAttemptAndFinalAuthorizationRefuseAtomically() throws Exception {
    try (Fixture fixture = new Fixture("conflict", 0)) {
      Messages.Retry request = new Messages.Retry(60, operation(60), WORK, 1);
      Messages.RetryResponse first = fixture.retry(request, 1200, ALLOW);
      long[] committed = fixture.credits();
      assertCode(
          ProtocolError.Code.CONFLICT,
          () -> fixture.retry(new Messages.Retry(61, operation(60), WORK, 2), 1200, ALLOW));
      assertCode(
          ProtocolError.Code.CONFLICT,
          () -> fixture.retry(new Messages.Retry(62, operation(61), WORK, 1), 1200, ALLOW));
      assertArrayEquals(committed, fixture.credits());
      assertEquals(
          first.receipt(),
          fixture.retry(new Messages.Retry(63, operation(60), WORK, 1), 0, ALLOW).receipt());
    }

    try (Fixture fixture = new Fixture("cross-type", 0)) {
      fixture.sessions.declare(
          sessionAccess(),
          SELECTED,
          fixture.binding.generation(),
          new Messages.Declare(64, operation(64), 0, List.of(2L), false));
      assertCode(
          ProtocolError.Code.CONFLICT,
          () -> fixture.retry(new Messages.Retry(65, operation(64), WORK, 1), 1200, ALLOW));
      assertEquals(1, fixture.view().attempt());
    }

    try (Fixture fixture = new Fixture("denied", 0)) {
      long[] before = fixture.credits();
      AtomicInteger checks = new AtomicInteger();
      AdmissionStore.Authorization denyFinal =
          (binding, parameters) -> {
            if (checks.incrementAndGet() == 2)
              throw new ProtocolError(
                  ProtocolError.Code.UNAUTHORIZED, "revoked before retry commit");
          };
      assertCode(
          ProtocolError.Code.UNAUTHORIZED,
          () -> fixture.retry(new Messages.Retry(70, operation(70), WORK, 1), 1200, denyFinal));
      assertEquals(2, checks.get());
      assertEquals(1, fixture.view().attempt());
      assertArrayEquals(before, fixture.credits());

      AtomicLong time = new AtomicLong(1300);
      AtomicInteger timedChecks = new AtomicInteger();
      AdmissionStore.Authorization advance =
          (binding, parameters) -> {
            if (timedChecks.incrementAndGet() == 2) time.set(11_000);
          };
      assertCode(
          ProtocolError.Code.DEADLINE_EXCEEDED,
          () ->
              fixture.sessions.retry(
                  sessionAccess(),
                  SELECTED,
                  fixture.binding.generation(),
                  new Messages.Retry(71, operation(71), WORK, 1),
                  () -> new AdmissionStore.Time(time.get(), true),
                  advance));
      assertEquals(2, timedChecks.get());
      assertEquals(1, fixture.view().attempt());
      assertArrayEquals(before, fixture.credits());
    }
  }

  private final class Fixture implements AutoCloseable {
    final Path database;
    final Path inputPath;
    SessionStore sessions;
    InputStore inputs;
    final Messages.Binding binding;
    int request = 100;

    Fixture(String name, int mode) throws Exception {
      database = directory.resolve(name + ".sqlite");
      inputPath = directory.resolve(name + "-inputs");
      sessions = SessionStore.initialize(database, configuration());
      binding =
          sessions.create(
              sessionAccess(),
              SELECTED,
              new Messages.Create(1, 1, new Records.Policy(10_000, 20_000, 30_000)));
      sessions.declare(
          sessionAccess(),
          SELECTED,
          binding.generation(),
          new Messages.Declare(2, operation(1), 0, List.of(1L), false));
      inputs = InputStore.initializeForAuthority(inputPath, INPUT_LIMITS, sessions.identity());
      sessions.bindInputs(inputs);
      Records.InputHeader header = header(binding.generation(), mode);
      try (InputStore.Receiver receiver = inputs.begin(context(), header, SELECTED, 1)) {
        receiver.write(ByteBuffer.allocate(0), 2);
        receiver.finish(3);
      }
      sessions.admit(
          sessionAccess(), SELECTED, binding.generation(), inputs, header, 3, clock(1000), ALLOW);
    }

    Messages.RetryResponse retry(
        Messages.Retry request, long now, AdmissionStore.Authorization authorization)
        throws Exception {
      return sessions.retry(
          sessionAccess(), SELECTED, binding.generation(), request, clock(now), authorization);
    }

    ExecutionStore.Lease claim(long now, long duration) throws Exception {
      return sessions.claimExecution(
          execAccess(), binding.generation(), WORK, inputs, duration, clock(now), ALLOW);
    }

    Records.WorkView view() throws Exception {
      return view(WORK);
    }

    Records.WorkView view(Records.WorkKey work) throws Exception {
      return sessions
          .snapshot(
              sessionAccess(),
              SELECTED,
              binding.generation(),
              new Messages.Watch(request++, work, 0, 0))
          .work();
    }

    JobRecord job() throws Exception {
      try (Connection connection =
          BoundedSqlite.open(database, configuration().files()).connect()) {
        return AdmissionStore.job(connection, binding, WORK).record();
      }
    }

    long[] credits() throws Exception {
      try (Connection connection = BoundedSqlite.open(database, configuration().files()).connect();
          var statement = connection.createStatement();
          var rows =
              statement.executeQuery(
                  "SELECT e.view_slot,j.state_slot FROM ps_v2_entities e JOIN ps_v2_jobs j ON"
                      + " e.generation=j.generation AND e.scope=j.scope AND e.id=j.entity"
                      + " WHERE e.generation="
                      + binding.generation()
                      + " AND e.scope=0 AND e.id=1")) {
        assertTrue(rows.next());
        return new long[] {
          FixedRecords.header(connection, rows.getLong(1), FixedRecords.Kind.WORK).credits(),
          FixedRecords.header(connection, rows.getLong(2), FixedRecords.Kind.JOB).credits()
        };
      }
    }

    Commitments.Context context() {
      return new Commitments.Context("issuer-a", "alice", binding.generation());
    }

    void reopen() throws Exception {
      inputs.close();
      sessions = SessionStore.open(database, configuration());
      inputs = InputStore.open(inputPath, INPUT_LIMITS);
      sessions.verifyInputs(inputs);
    }

    @Override
    public void close() throws java.io.IOException {
      inputs.close();
    }
  }

  private static void assertReplacement(
      Fixture fixture, long attempt, long leaseNumber, boolean expansionComplete) throws Exception {
    Records.WorkView view = fixture.view();
    assertEquals(Records.State.ACTIVE, view.state());
    assertEquals(attempt, view.attempt());
    assertEquals(1000, view.admittedAt());
    assertEquals(11_000, view.deadline());
    JobRecord job = fixture.job();
    assertEquals(attempt, job.attempt());
    assertEquals(leaseNumber, job.lease());
    assertNull(job.leaseUntil());
    assertEquals(JobRecord.Stage.QUEUED, job.stage());
    assertEquals(expansionComplete, job.expansionComplete());
  }

  private static Object concurrentRetry(
      SessionStore sessions,
      Messages.Binding binding,
      int operation,
      CountDownLatch ready,
      CountDownLatch start) {
    ready.countDown();
    try {
      assertTrue(start.await(5, TimeUnit.SECONDS));
      return sessions.retry(
          sessionAccess(),
          SELECTED,
          binding.generation(),
          new Messages.Retry(operation, operation(operation), WORK, 1),
          clock(1200),
          ALLOW);
    } catch (Exception failure) {
      return failure;
    }
  }

  private static SessionStore.Configuration configuration() {
    return new SessionStore.Configuration(
        "issuer-a",
        new Records.Limits(16, 64, 64, 1 << 20, 1 << 20, 8),
        new Records.Policy(60_000, 60_000, 60_000),
        4,
        16,
        4,
        BoundedSqlite.Limits.defaults(),
        new AdmissionStore.ExecutionPolicy(
            List.of(
                new AdmissionStore.Application(
                    "copy", Set.of(0, 1, 2), AdmissionStore.RestartSafety.IDEMPOTENT)),
            8,
            8));
  }

  private static Records.InputHeader header(long generation, int mode) throws Exception {
    return new Records.InputHeader(
        generation,
        operation(2),
        new Records.AdmitParameters(
            WORK,
            new Records.Input(0, digest(new byte[0]), "application/octet-stream"),
            "copy",
            mode,
            10_000,
            new Records.OutputBudget(0, 0)));
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

  @FunctionalInterface
  private interface Throwing {
    void run() throws Exception;
  }
}
