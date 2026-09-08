package ai.pipestream.quic.v2;

import static ai.pipestream.quic.v2.Messages.DURABLE_WORK;
import static org.junit.jupiter.api.Assertions.*;

import ai.pipestream.quic.BoundedSqlite;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

@Timeout(30)
final class FenceStoreTest {
  private static final Messages.Capabilities SELECTED =
      new Messages.Capabilities(
          true, List.of(DURABLE_WORK), List.of(), 1 << 20, 16, 32, 1 << 20, 1000, 10_000);
  private static final InputStore.Limits INPUT_LIMITS =
      new InputStore.Limits(16L << 20, 128, 1 << 20, 8);
  private static final AdmissionStore.Authorization ADMIT = (binding, parameters) -> {};
  private static final FenceStore.Authorization ALLOW = (binding, request) -> {};
  private static final Records.WorkKey WORK = new Records.WorkKey(0, 0, 1);

  @TempDir Path directory;

  @Test
  void declaredWorkCanBeCancelledOrSkippedWithoutInventingAdmission() throws Exception {
    try (Fixture cancelled = new Fixture("declared-cancel", false, 0)) {
      Messages.Cancel request = new Messages.Cancel(10, operation(10), WORK);
      Messages.CancelResponse first = cancelled.cancel(request, 1000, ALLOW);
      Records.Cancelled outcome =
          assertInstanceOf(Records.Cancelled.class, first.receipt().outcome());
      assertEquals(WORK, outcome.work());
      assertEquals(0, outcome.disposition());
      assertEquals(Records.State.CANCELLED, outcome.state());
      assertEquals(0, cancelled.view().attempt());
      assertNull(cancelled.view().input());
      assertEquals(Records.State.CANCELLED, cancelled.view().state());
      Messages.CancelResponse replay =
          cancelled.cancel(new Messages.Cancel(11, operation(10), WORK), 0, ALLOW);
      assertEquals(first.receipt(), replay.receipt());
      cancelled.reopen();
      assertEquals(
          first.receipt(),
          cancelled.cancel(new Messages.Cancel(12, operation(10), WORK), 0, ALLOW).receipt());
    }

    try (Fixture skipped = new Fixture("declared-skip", false, 0)) {
      Messages.SkipResponse response =
          skipped.skip(new Messages.Skip(20, operation(20), WORK), 1000, ALLOW);
      Records.Skipped outcome =
          assertInstanceOf(Records.Skipped.class, response.receipt().outcome());
      assertEquals(0, outcome.disposition());
      assertEquals(Records.State.SKIPPED, outcome.state());
      assertEquals(0, skipped.view().attempt());
      assertNull(skipped.view().admittedAt());
    }
  }

  @Test
  void terminalObservationReturnsDispositionOneWithoutChangingOriginalState() throws Exception {
    try (Fixture fixture = new Fixture("terminal", true, 0)) {
      ExecutionStore.Lease lease = fixture.claim(1100, 500);
      Records.WorkView terminal =
          fixture.sessions.failExecution(
              execAccess(), lease, new Records.Diagnostic(8, "failed"), false, clock(1200), ADMIT);
      Messages.CancelResponse cancel =
          fixture.cancel(new Messages.Cancel(30, operation(30), WORK), 1300, ALLOW);
      Records.Cancelled cancelled = (Records.Cancelled) cancel.receipt().outcome();
      assertEquals(1, cancelled.disposition());
      assertEquals(Records.State.FAILED, cancelled.state());
      assertEquals(terminal, fixture.view());
      Messages.SkipResponse skip =
          fixture.skip(new Messages.Skip(31, operation(31), WORK), 1300, ALLOW);
      Records.Skipped skipped = (Records.Skipped) skip.receipt().outcome();
      assertEquals(1, skipped.disposition());
      assertEquals(Records.State.FAILED, skipped.state());
      assertEquals(terminal, fixture.view());
    }
  }

  @Test
  void acceptedFenceImmediatelyExcludesOldLeasePublicationAndConflictingFence() throws Exception {
    try (Fixture fixture = new Fixture("lease-fence", true, 2)) {
      ExecutionStore.Lease lease = fixture.claim(1100, 500);
      Records.WorkView before = fixture.view();
      Messages.CancelResponse response =
          fixture.cancel(new Messages.Cancel(40, operation(40), WORK), 1200, ALLOW);
      Records.Cancelled outcome = (Records.Cancelled) response.receipt().outcome();
      assertEquals(0, outcome.disposition());
      assertEquals(Records.State.CANCELLING, outcome.state());
      assertEquals(Records.State.CANCELLING, fixture.view().state());
      assertEquals(before.deadline(), fixture.view().deadline());
      assertCode(
          ProtocolError.Code.CANCELLED,
          () ->
              fixture.sessions.succeedExecution(
                  execAccess(),
                  lease,
                  fixture.inputs,
                  0,
                  new PublicationStore.Endpoint("localhost:443"),
                  clock(1200),
                  ADMIT));
      assertCode(
          ProtocolError.Code.CANCELLED,
          () -> fixture.skip(new Messages.Skip(41, operation(41), WORK), 1200, ALLOW));
      Messages.CancelResponse sameDesired =
          fixture.cancel(new Messages.Cancel(42, operation(42), WORK), 1200, ALLOW);
      Records.Cancelled same = (Records.Cancelled) sameDesired.receipt().outcome();
      assertEquals(0, same.disposition());
      assertEquals(Records.State.CANCELLING, same.state());
      assertCode(
          ProtocolError.Code.CANCELLED,
          () ->
              fixture.sessions.expireExecution(fixture.binding.generation(), WORK, clock(11_001)));
      assertEquals(Records.State.CANCELLING, fixture.view().state());
      assertEquals(
          response.receipt(),
          fixture.cancel(new Messages.Cancel(43, operation(40), WORK), 0, ALLOW).receipt());
      FenceStore.Cursor cursor = new FenceStore.Cursor();
      ClosureStore.Cursor closures = new ClosureStore.Cursor();
      for (int step = 0; step < 16 && !fixture.view().state().terminal(); step++) {
        fixture.sessions.reconcileCancellation(cursor, 1, clock(1200));
        fixture.sessions.reconcileClosures(closures, 1, clock(1200));
      }
      assertEquals(Records.State.CANCELLED, fixture.view().state());
      assertTrue(fixture.credits()[1] >= 4);
      fixture.reopen();
      assertEquals(Records.State.CANCELLED, fixture.view().state());
      assertEquals(
          sameDesired.receipt(),
          fixture.cancel(new Messages.Cancel(44, operation(42), WORK), 0, ALLOW).receipt());
      assertTrue(fixture.credits()[1] >= 4);
    }
  }

  @Test
  void skipPolicyAndFinalTimeOrAuthorizationRefusalsRollback() throws Exception {
    try (Fixture fixture = new Fixture("policy", true, 0)) {
      Records.WorkView before = fixture.view();
      long[] credits = fixture.credits();
      FenceStore.Authorization cancelOnly =
          (binding, request) -> {
            if (request instanceof Messages.Skip)
              throw new ProtocolError(ProtocolError.Code.UNAUTHORIZED, "skip denied");
          };
      assertCode(
          ProtocolError.Code.UNAUTHORIZED,
          () -> fixture.skip(new Messages.Skip(50, operation(50), WORK), 1200, cancelOnly));
      assertEquals(before, fixture.view());
      assertArrayEquals(credits, fixture.credits());

      AtomicInteger accessChecks = new AtomicInteger();
      SessionStore.Access revokeAccess =
          new SessionStore.Access(
              "alice",
              () -> {
                if (accessChecks.incrementAndGet() == 3)
                  throw new ProtocolError(ProtocolError.Code.UNAUTHORIZED, "access revoked");
              });
      assertCode(
          ProtocolError.Code.UNAUTHORIZED,
          () ->
              fixture.sessions.cancel(
                  revokeAccess,
                  SELECTED,
                  fixture.binding.generation(),
                  new Messages.Cancel(53, operation(53), WORK),
                  clock(1300),
                  ALLOW));
      assertEquals(3, accessChecks.get());
      assertEquals(before, fixture.view());
      assertArrayEquals(credits, fixture.credits());

      AtomicInteger checks = new AtomicInteger();
      FenceStore.Authorization revoke =
          (binding, request) -> {
            if (checks.incrementAndGet() == 2)
              throw new ProtocolError(ProtocolError.Code.UNAUTHORIZED, "revoked before commit");
          };
      assertCode(
          ProtocolError.Code.UNAUTHORIZED,
          () -> fixture.cancel(new Messages.Cancel(51, operation(51), WORK), 1200, revoke));
      assertEquals(2, checks.get());
      assertEquals(before, fixture.view());
      assertArrayEquals(credits, fixture.credits());

      AtomicLong time = new AtomicLong(1300);
      AtomicInteger timeChecks = new AtomicInteger();
      FenceStore.Authorization regress =
          (binding, request) -> {
            if (timeChecks.incrementAndGet() == 2) time.set(1299);
          };
      assertCode(
          ProtocolError.Code.CLOCK_UNSAFE,
          () ->
              fixture.sessions.cancel(
                  sessionAccess(),
                  SELECTED,
                  fixture.binding.generation(),
                  new Messages.Cancel(52, operation(52), WORK),
                  () -> new AdmissionStore.Time(time.get(), true),
                  regress));
      assertEquals(before, fixture.view());
      assertArrayEquals(credits, fixture.credits());
    }
  }

  @Test
  void exactReplayStillRequiresCurrentAccess() throws Exception {
    try (Fixture fixture = new Fixture("replay-access", false, 0)) {
      fixture.cancel(new Messages.Cancel(54, operation(54), WORK), 1000, ALLOW);
      SessionStore.Access denied =
          new SessionStore.Access(
              "alice",
              () -> {
                throw new ProtocolError(ProtocolError.Code.UNAUTHORIZED, "current access denied");
              });
      assertCode(
          ProtocolError.Code.UNAUTHORIZED,
          () ->
              fixture.sessions.cancel(
                  denied,
                  SELECTED,
                  fixture.binding.generation(),
                  new Messages.Cancel(55, operation(54), WORK),
                  clock(0),
                  ALLOW));
    }
  }

  @Test
  void cancellationAtDeadlineWinsBeforeAnyDeadlineFailureIsCommitted() throws Exception {
    try (Fixture fixture = new Fixture("deadline-cancel", true, 0)) {
      Messages.CancelResponse response =
          fixture.cancel(new Messages.Cancel(55, operation(55), WORK), 11_000, ALLOW);
      Records.Cancelled outcome = (Records.Cancelled) response.receipt().outcome();
      assertEquals(0, outcome.disposition());
      assertEquals(Records.State.CANCELLED, outcome.state());
      assertEquals(Records.State.CANCELLED, fixture.view().state());
      assertEquals(11_000, fixture.view().deadline());
    }
  }

  @Test
  void finalClockCannotCommitExpiredReceiptPromiseAndReplayNeverSamplesClock() throws Exception {
    try (Fixture fixture = new Fixture("receipt-time", true, 0)) {
      Records.WorkView before = fixture.view();
      long[] credits = fixture.credits();
      AtomicLong time = new AtomicLong(1200);
      AtomicInteger checks = new AtomicInteger();
      FenceStore.Authorization overtakeReceipt =
          (binding, request) -> {
            if (checks.incrementAndGet() == 2) time.set(1200 + binding.policy().receiptRetention());
          };
      assertCode(
          ProtocolError.Code.CLOCK_UNSAFE,
          () ->
              fixture.sessions.cancel(
                  sessionAccess(),
                  SELECTED,
                  fixture.binding.generation(),
                  new Messages.Cancel(56, operation(56), WORK),
                  () -> new AdmissionStore.Time(time.get(), true),
                  overtakeReceipt));
      assertEquals(2, checks.get());
      assertEquals(before, fixture.view());
      assertArrayEquals(credits, fixture.credits());

      Messages.Cancel request = new Messages.Cancel(57, operation(57), WORK);
      Messages.CancelResponse accepted = fixture.cancel(request, 1300, ALLOW);
      AtomicInteger clockSamples = new AtomicInteger();
      AtomicInteger replayChecks = new AtomicInteger();
      Messages.CancelResponse replay =
          fixture.sessions.cancel(
              sessionAccess(),
              SELECTED,
              fixture.binding.generation(),
              new Messages.Cancel(58, operation(57), WORK),
              () -> {
                clockSamples.incrementAndGet();
                return new AdmissionStore.Time(0, false);
              },
              (binding, replayRequest) -> replayChecks.incrementAndGet());
      assertEquals(0, clockSamples.get());
      assertEquals(2, replayChecks.get());
      assertEquals(accepted.receipt(), replay.receipt());
    }
  }

  @Test
  void rootScopeCancellationSealsAndSettlesMembersAndOperationKindsCannotCollide()
      throws Exception {
    try (Fixture fixture = new Fixture("scope", false, 0)) {
      fixture.sessions.declare(
          sessionAccess(),
          SELECTED,
          fixture.binding.generation(),
          new Messages.Declare(60, operation(60), 0, List.of(2L), false));
      Messages.CancelScopeResponse response =
          fixture.sessions.cancelScope(
              sessionAccess(),
              SELECTED,
              fixture.binding.generation(),
              new Messages.CancelScope(61, operation(61), 0),
              clock(1000),
              ALLOW);
      Records.ScopeCancelled outcome =
          assertInstanceOf(Records.ScopeCancelled.class, response.receipt().outcome());
      assertEquals(0, outcome.scope());
      Messages.PageResponse frozen =
          fixture.sessions.page(
              sessionAccess(),
              SELECTED,
              fixture.binding.generation(),
              new Messages.Page(62, 0, 0, 10));
      assertFalse(frozen.sealed());
      assertNull(frozen.seal());
      assertCode(
          ProtocolError.Code.CANCELLED,
          () ->
              fixture.sessions.declare(
                  sessionAccess(),
                  SELECTED,
                  fixture.binding.generation(),
                  new Messages.Declare(63, operation(63), 0, List.of(3L), false)));
      assertEquals(
          response.receipt(),
          fixture
              .sessions
              .cancelScope(
                  sessionAccess(),
                  SELECTED,
                  fixture.binding.generation(),
                  new Messages.CancelScope(64, operation(61), 0),
                  clock(0),
                  ALLOW)
              .receipt());
      assertCode(
          ProtocolError.Code.CONFLICT,
          () -> fixture.cancel(new Messages.Cancel(65, operation(60), WORK), 1000, ALLOW));

      FenceStore.Cursor cancellation = new FenceStore.Cursor();
      int settled = 0;
      int sealed = 0;
      for (int step = 0; step < 16 && (settled < 2 || sealed == 0); step++) {
        FenceStore.Progress progress =
            fixture.sessions.reconcileCancellation(cancellation, 1, clock(1000));
        settled += progress.settledWork();
        sealed += progress.sealedScopes();
      }
      assertEquals(2, settled);
      assertTrue(sealed > 0);
      ClosureStore.Cursor closures = new ClosureStore.Cursor();
      for (int step = 0; step < 8; step++)
        fixture.sessions.reconcileClosures(closures, 1, clock(1000));
      assertEquals(Records.State.CANCELLED, fixture.view(WORK).state());
      assertEquals(Records.State.CANCELLED, fixture.view(new Records.WorkKey(0, 0, 2)).state());
      Messages.PageResponse reconciled =
          fixture.sessions.page(
              sessionAccess(),
              SELECTED,
              fixture.binding.generation(),
              new Messages.Page(66, 0, 0, 10));
      assertTrue(reconciled.sealed());
      assertNotNull(reconciled.seal());
      fixture.reopen();
      assertEquals(Records.State.CANCELLED, fixture.view(WORK).state());
    }
  }

  @Test
  void recoveryRejectsFenceWhoseAcceptingOperationWasRemovedWithAdjustedCount() throws Exception {
    try (Fixture fixture = new Fixture("missing-operation", true, 0)) {
      fixture.cancel(new Messages.Cancel(70, operation(70), WORK), 1200, ALLOW);
      try (Connection connection =
          BoundedSqlite.open(fixture.database, configuration().files()).connect()) {
        connection.setAutoCommit(false);
        try (var delete =
                connection.prepareStatement(
                    "DELETE FROM ps_v2_operations WHERE generation=? AND operation=?");
            var count =
                connection.prepareStatement(
                    "UPDATE ps_v2_sessions SET operation_count=operation_count-1 WHERE"
                        + " generation=?")) {
          delete.setLong(1, fixture.binding.generation());
          delete.setBytes(2, operation(70).bytes());
          assertEquals(1, delete.executeUpdate());
          count.setLong(1, fixture.binding.generation());
          assertEquals(1, count.executeUpdate());
        }
        connection.commit();
      }
      assertThrows(SQLException.class, () -> SessionStore.open(fixture.database, configuration()));
    }
  }

  @Test
  void recoveryRejectsScopeCancellationIndexRetargetedToAnotherExistingScope() throws Exception {
    try (Fixture fixture = new Fixture("wrong-scope-index", true, 1)) {
      Records.ChildScope child = fixture.view().child();
      assertNotNull(child);
      fixture.sessions.cancelScope(
          sessionAccess(),
          SELECTED,
          fixture.binding.generation(),
          new Messages.CancelScope(71, operation(71), 0),
          clock(1200),
          ALLOW);
      try (Connection connection =
              BoundedSqlite.open(fixture.database, configuration().files()).connect();
          var update =
              connection.prepareStatement(
                  "UPDATE ps_v2_operations SET cancel_scope=? WHERE generation=? AND"
                      + " operation=?")) {
        update.setLong(1, child.scope());
        update.setLong(2, fixture.binding.generation());
        update.setBytes(3, operation(71).bytes());
        assertEquals(1, update.executeUpdate());
      }
      assertThrows(SQLException.class, () -> SessionStore.open(fixture.database, configuration()));
    }
  }

  private final class Fixture implements AutoCloseable {
    final Path database;
    final Path inputPath;
    SessionStore sessions;
    InputStore inputs;
    final Messages.Binding binding;
    int request = 100;

    Fixture(String name, boolean admit, int mode) throws Exception {
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
      if (admit) {
        Records.InputHeader header = header(binding.generation(), mode);
        try (InputStore.Receiver receiver = inputs.begin(context(), header, SELECTED, 1)) {
          receiver.write(ByteBuffer.allocate(0), 2);
          receiver.finish(3);
        }
        sessions.admit(
            sessionAccess(), SELECTED, binding.generation(), inputs, header, 3, clock(1000), ADMIT);
      }
    }

    Messages.CancelResponse cancel(
        Messages.Cancel request, long now, FenceStore.Authorization authorization)
        throws Exception {
      return sessions.cancel(
          sessionAccess(), SELECTED, binding.generation(), request, clock(now), authorization);
    }

    Messages.SkipResponse skip(
        Messages.Skip request, long now, FenceStore.Authorization authorization) throws Exception {
      return sessions.skip(
          sessionAccess(), SELECTED, binding.generation(), request, clock(now), authorization);
    }

    ExecutionStore.Lease claim(long now, long duration) throws Exception {
      return sessions.claimExecution(
          execAccess(), binding.generation(), WORK, inputs, duration, clock(now), ADMIT);
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

    Commitments.Context context() {
      return new Commitments.Context("issuer-a", "alice", binding.generation());
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
