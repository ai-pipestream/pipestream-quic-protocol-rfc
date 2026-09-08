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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

@Timeout(30)
final class ExpansionTransitionTest {
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
      new InputStore.Limits(16L << 20, 128, 1 << 20, 8);
  private static final AdmissionStore.Authorization ALLOW = (binding, parameters) -> {};
  private static final Records.WorkKey PARENT = new Records.WorkKey(0, 0, 1);

  @TempDir Path directory;

  @Test
  void completionRequiresEverySealedMemberAdmittedThenPersistsWaitingState() throws Exception {
    try (Fixture fixture = new Fixture("complete")) {
      ExecutionStore.Lease lease = fixture.prepareParent(2, 1100, 500);
      long[] beforeDeclaration = fixture.credits();
      assertCode(
          ProtocolError.Code.NOT_READY,
          () -> fixture.sessions.finishExpansion(execAccess(), lease, true, clock(1100), ALLOW));
      assertArrayEquals(beforeDeclaration, fixture.credits());
      assertActiveLease(fixture, lease);
      fixture.declare(lease, operation(20), List.of(10L), true, 1100);
      long[] funded = fixture.credits();

      assertCode(
          ProtocolError.Code.NOT_READY,
          () -> fixture.sessions.finishExpansion(execAccess(), lease, true, clock(1100), ALLOW));
      assertArrayEquals(funded, fixture.credits());
      assertActiveLease(fixture, lease);

      Records.InputHeader child = fixture.childHeader(10, operation(21), new byte[] {1, 2, 3});
      fixture.receive(child, new byte[] {1, 2, 3}, 1100);
      fixture.sessions.admitProduced(
          execAccess(), lease, SELECTED, fixture.inputs, child, clock(1100), ALLOW);
      Records.WorkView completed =
          fixture.sessions.finishExpansion(execAccess(), lease, true, clock(1100), ALLOW);
      assertEquals(Records.State.WAITING_CHILDREN, completed.state());
      assertEquals(1, completed.attempt());
      assertEquals(1000, completed.admittedAt());
      assertEquals(11_000, completed.deadline());
      assertArrayEquals(new long[] {funded[0] - 1, funded[1] - 1}, fixture.credits());
      assertNull(fixture.job().leaseUntil());
      assertTrue(fixture.job().expansionComplete());

      assertCode(
          ProtocolError.Code.CONFLICT,
          () -> fixture.declare(lease, operation(22), List.of(20L), false, 1100));
      fixture.reopen();
      assertEquals(Records.State.WAITING_CHILDREN, fixture.view(PARENT).state());
      assertTrue(fixture.job().expansionComplete());
      assertNull(fixture.job().leaseUntil());
      assertCode(ProtocolError.Code.NOT_READY, () -> fixture.claim(1200, 500));
    }
  }

  @Test
  void completionFinalLeaseFenceRollsBackTentativeFundedWrites() throws Exception {
    try (Fixture fixture = new Fixture("complete-final-fence")) {
      ExecutionStore.Lease lease = fixture.prepareParent(2, 1100, 500);
      fixture.declare(lease, operation(25), List.of(), true, 1100);
      long[] before = fixture.credits();
      AtomicLong time = new AtomicLong(1150);
      AtomicInteger checks = new AtomicInteger();
      AdmissionStore.Authorization expireAtFinalGate =
          (binding, parameters) -> {
            if (checks.incrementAndGet() == 3) time.set(lease.until());
          };

      assertCode(
          ProtocolError.Code.CONFLICT,
          () ->
              fixture.sessions.finishExpansion(
                  execAccess(),
                  lease,
                  true,
                  () -> new AdmissionStore.Time(time.get(), true),
                  expireAtFinalGate));
      assertEquals(3, checks.get());
      assertArrayEquals(before, fixture.credits());
      assertActiveLease(fixture, lease);
      assertFalse(fixture.job().expansionComplete());
      assertEquals(JobRecord.Stage.EXECUTING, fixture.job().stage());
    }
  }

  @Test
  void yieldClearsLeaseWithoutSpendingCreditsAndDeclarationReplaySurvivesReopen() throws Exception {
    try (Fixture fixture = new Fixture("yield")) {
      ExecutionStore.Lease first = fixture.prepareParent(2, 1100, 500);
      Messages.DeclarationResponse declaration =
          fixture.declare(first, operation(30), List.of(10L), false, 1100);
      long[] funded = fixture.credits();
      Records.WorkView yielded =
          fixture.sessions.finishExpansion(execAccess(), first, false, clock(1100), ALLOW);
      assertEquals(Records.State.ACTIVE, yielded.state());
      assertEquals(1, yielded.attempt());
      assertArrayEquals(funded, fixture.credits());
      assertNull(fixture.job().leaseUntil());
      assertFalse(fixture.job().expansionComplete());
      assertCode(
          ProtocolError.Code.CONFLICT,
          () -> fixture.declare(first, operation(31), List.of(20L), false, 1100));

      ExecutionStore.Lease second = fixture.claim(1100, 500);
      assertEquals(first.attempt(), second.attempt());
      assertEquals(first.number() + 1, second.number());
      assertEquals(
          declaration.receipt(),
          fixture.declare(second, operation(30), List.of(10L), false, 1100).receipt());
      fixture.sessions.finishExpansion(execAccess(), second, false, clock(1100), ALLOW);
      assertArrayEquals(funded, fixture.credits());

      fixture.reopen();
      assertEquals(Records.State.ACTIVE, fixture.view(PARENT).state());
      assertFalse(fixture.job().expansionComplete());
      ExecutionStore.Lease third = fixture.claim(1100, 500);
      assertEquals(1, third.attempt());
      assertEquals(3, third.number());
    }
  }

  @Test
  void wrongModeStaleLeaseAndLateFinalGatesRefuseWithoutMutation() throws Exception {
    try (Fixture wrongMode = new Fixture("wrong-mode")) {
      ExecutionStore.Lease leaf = wrongMode.prepareParent(0, 1100, 500);
      long[] before = wrongMode.credits();
      assertCode(
          ProtocolError.Code.CONFLICT,
          () -> wrongMode.sessions.finishExpansion(execAccess(), leaf, false, clock(1100), ALLOW));
      assertArrayEquals(before, wrongMode.credits());
      assertActiveLease(wrongMode, leaf);
    }

    try (Fixture stale = new Fixture("stale")) {
      ExecutionStore.Lease first = stale.prepareParent(2, 1100, 100);
      stale.sessions.finishExpansion(execAccess(), first, false, clock(1100), ALLOW);
      ExecutionStore.Lease second = stale.claim(1100, 500);
      long[] before = stale.credits();
      assertCode(
          ProtocolError.Code.CONFLICT,
          () -> stale.sessions.finishExpansion(execAccess(), first, false, clock(1100), ALLOW));
      assertArrayEquals(before, stale.credits());
      assertActiveLease(stale, second);
    }

    try (Fixture denied = new Fixture("denied")) {
      ExecutionStore.Lease lease = denied.prepareParent(2, 1100, 500);
      long[] before = denied.credits();
      AtomicInteger checks = new AtomicInteger();
      AdmissionStore.Authorization revoke =
          (binding, parameters) -> {
            if (checks.incrementAndGet() == 3)
              throw new ProtocolError(ProtocolError.Code.UNAUTHORIZED, "revoked before commit");
          };
      assertCode(
          ProtocolError.Code.UNAUTHORIZED,
          () -> denied.sessions.finishExpansion(execAccess(), lease, false, clock(1100), revoke));
      assertEquals(3, checks.get());
      assertArrayEquals(before, denied.credits());
      assertActiveLease(denied, lease);
    }

    for (String caseName : List.of("deadline", "regression")) {
      try (Fixture late = new Fixture(caseName)) {
        ExecutionStore.Lease lease = late.prepareParent(2, 1100, 500);
        long[] before = late.credits();
        AtomicLong time = new AtomicLong(1150);
        AtomicInteger checks = new AtomicInteger();
        AdmissionStore.Authorization advance =
            (binding, parameters) -> {
              if (checks.incrementAndGet() == 3)
                time.set(caseName.equals("deadline") ? 11_000 : 1140);
            };
        assertCode(
            caseName.equals("deadline")
                ? ProtocolError.Code.DEADLINE_EXCEEDED
                : ProtocolError.Code.CLOCK_UNSAFE,
            () ->
                late.sessions.finishExpansion(
                    execAccess(),
                    lease,
                    false,
                    () -> new AdmissionStore.Time(time.get(), true),
                    advance));
        assertEquals(3, checks.get());
        assertArrayEquals(before, late.credits());
        assertActiveLease(late, lease);
      }
    }
  }

  private final class Fixture implements AutoCloseable {
    final Path database;
    final Path inputPath;
    SessionStore sessions;
    InputStore inputs;
    final Messages.Binding binding;
    Records.ChildScope child;
    int request = 10;

    Fixture(String name) throws Exception {
      database = directory.resolve(name + ".sqlite");
      inputPath = directory.resolve(name + "-inputs");
      sessions = SessionStore.initialize(database, configuration());
      binding =
          sessions.create(
              sessionAccess(),
              SELECTED,
              new Messages.Create(1, 1, new Records.Policy(20_000, 20_000, 30_000)));
      inputs = InputStore.initializeForAuthority(inputPath, INPUT_LIMITS, sessions.identity());
      sessions.bindInputs(inputs);
    }

    ExecutionStore.Lease prepareParent(int mode, long now, long leaseMillis) throws Exception {
      sessions.declare(
          sessionAccess(),
          SELECTED,
          binding.generation(),
          new Messages.Declare(request++, operation(request++), 0, List.of(1L), false));
      Records.InputHeader header = parentHeader(mode);
      receive(header, new byte[0], 1000);
      sessions.admit(
          sessionAccess(),
          SELECTED,
          binding.generation(),
          inputs,
          header,
          request++,
          clock(1000),
          ALLOW);
      ExecutionStore.Lease lease = claim(now, leaseMillis);
      child = view(PARENT).child();
      if (mode == 2) assertNotNull(child);
      else assertNull(child);
      return lease;
    }

    ExecutionStore.Lease claim(long now, long leaseMillis) throws Exception {
      return sessions.claimExecution(
          execAccess(), binding.generation(), PARENT, inputs, leaseMillis, clock(now), ALLOW);
    }

    Messages.DeclarationResponse declare(
        ExecutionStore.Lease lease,
        Records.OperationId operation,
        List<Long> members,
        boolean seal,
        long now)
        throws Exception {
      return sessions.declareProduced(
          execAccess(),
          lease,
          SELECTED,
          inputs,
          new Messages.Declare(request++, operation, child.scope(), members, seal),
          clock(now),
          ALLOW);
    }

    Records.InputHeader parentHeader(int mode) throws Exception {
      return new Records.InputHeader(
          binding.generation(),
          operation(2),
          new Records.AdmitParameters(
              PARENT,
              new Records.Input(0, digest(new byte[0]), "application/octet-stream"),
              mode == 2 ? "expand" : "leaf",
              mode,
              10_000,
              new Records.OutputBudget(0, 0)));
    }

    Records.InputHeader childHeader(long entity, Records.OperationId operation, byte[] payload)
        throws Exception {
      return new Records.InputHeader(
          binding.generation(),
          operation,
          new Records.AdmitParameters(
              new Records.WorkKey(child.scope(), child.producer(), entity),
              new Records.Input(payload.length, digest(payload), "application/octet-stream"),
              "leaf",
              0,
              10_000,
              new Records.OutputBudget(0, 0)));
    }

    void receive(Records.InputHeader header, byte[] payload, long now) throws Exception {
      try (InputStore.Receiver receiver = inputs.begin(context(), header, SELECTED, now)) {
        receiver.write(ByteBuffer.wrap(payload), now);
        receiver.finish(now);
      }
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
      try (var connection = BoundedSqlite.open(database, configuration().files()).connect()) {
        return AdmissionStore.job(connection, binding, PARENT).record();
      }
    }

    long[] credits() throws Exception {
      try (var connection = BoundedSqlite.open(database, configuration().files()).connect();
          var statement = connection.createStatement();
          var rows =
              statement.executeQuery(
                  "SELECT e.view_slot,j.state_slot FROM ps_v2_entities e JOIN ps_v2_jobs j ON"
                      + " e.generation=j.generation AND e.scope=j.scope AND e.id=j.entity"
                      + " WHERE e.generation=1 AND e.scope=0 AND e.id=1")) {
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

  private static void assertActiveLease(Fixture fixture, ExecutionStore.Lease lease)
      throws Exception {
    JobRecord job = fixture.job();
    assertEquals(Records.State.ACTIVE, fixture.view(PARENT).state());
    assertEquals(lease.number(), job.lease());
    assertEquals(lease.until(), job.leaseUntil());
    assertEquals(job.input().parameters().mode() != 2, job.expansionComplete());
  }

  private static SessionStore.Configuration configuration() {
    AdmissionStore.Application parent =
        new AdmissionStore.Application(
            "expand", Set.of(2), AdmissionStore.RestartSafety.IDEMPOTENT);
    AdmissionStore.Application leaf =
        new AdmissionStore.Application("leaf", Set.of(0), AdmissionStore.RestartSafety.IDEMPOTENT);
    return new SessionStore.Configuration(
        "issuer-a",
        new Records.Limits(16, 64, 64, 1 << 20, 1 << 20, 8),
        new Records.Policy(60_000, 60_000, 60_000),
        4,
        16,
        4,
        BoundedSqlite.Limits.defaults(),
        new AdmissionStore.ExecutionPolicy(List.of(parent, leaf), 8, 8));
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
