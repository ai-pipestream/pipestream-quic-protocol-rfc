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
final class ProducedAdmissionTest {
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
      new InputStore.Limits(16L << 20, 128, 1 << 20, 4);
  private static final AdmissionStore.Authorization ALLOW = (binding, parameters) -> {};
  private static final Records.WorkKey PARENT = new Records.WorkKey(0, 0, 1);

  @TempDir Path directory;

  @Test
  void producedPayloadReceiptAndJobSurviveGracefulReopenAndReplayUnderReplacementLease()
      throws Exception {
    try (Fixture fixture = new Fixture("replay", 8)) {
      ExecutionStore.Lease first = fixture.prepareParent();
      Messages.DeclarationResponse producedDeclaration =
          fixture.declareProduced(first, operation(20), List.of(10L), true);
      Records.OperationReceipt callerOperation =
          fixture
              .sessions
              .lookupOperation(
                  sessionAccess(), SELECTED, 1, new Messages.LookupOperation(92, operation(20)))
              .receipt();
      assertInstanceOf(Records.Admitted.class, callerOperation.outcome());
      assertEquals(PARENT, ((Records.Admitted) callerOperation.outcome()).work());
      assertInstanceOf(Records.Declared.class, producedDeclaration.receipt().outcome());
      assertNotEquals(callerOperation, producedDeclaration.receipt());
      Records.InputHeader header = fixture.header(10, operation(21), new byte[] {1, 2, 3});
      fixture.receive(header, new byte[] {1, 2, 3});
      assertTrue(fixture.check(first, header).isEmpty());
      InputStore.Usage before = fixture.inputs.usage();
      Records.OperationReceipt receipt = fixture.admit(first, header);
      Records.Admitted admitted = (Records.Admitted) receipt.outcome();
      assertEquals(1100, admitted.admittedAt());
      assertEquals(11_100, admitted.deadline());
      InputStore.Usage funded = fixture.inputs.usage();
      assertTrue(funded.bytes() > before.bytes());
      InputStore.Reservation reservation =
          fixture.inputs.findReservation(fixture.context(), header).orElseThrow();
      assertEquals(2, scalar(fixture.database, "SELECT count(*) FROM ps_v2_jobs"));
      assertEquals(4, scalar(fixture.database, "SELECT count(*) FROM ps_v2_operations"));
      assertEquals(Records.State.ACTIVE, fixture.view(header.parameters().work()).state());
      assertCode(
          ProtocolError.Code.UNAUTHORIZED,
          () ->
              fixture.sessions.checkInput(
                  sessionAccess(), SELECTED, 1, fixture.inputs, header, clock(1100), ALLOW));
      assertCode(
          ProtocolError.Code.UNAUTHORIZED,
          () ->
              fixture.sessions.admit(
                  sessionAccess(), SELECTED, 1, fixture.inputs, header, 91, clock(1100), ALLOW));

      fixture.reopen();
      assertEquals(funded, fixture.inputs.usage());
      InputStore.Reservation reopenedReservation =
          fixture.inputs.findReservation(fixture.context(), header).orElseThrow();
      assertEquals(reservation.header(), reopenedReservation.header());
      assertEquals(reservation.context(), reopenedReservation.context());
      assertEquals(reservation.reference(), reopenedReservation.reference());
      assertEquals(
          callerOperation,
          fixture
              .sessions
              .lookupOperation(
                  sessionAccess(), SELECTED, 1, new Messages.LookupOperation(93, operation(20)))
              .receipt());
      assertCode(ProtocolError.Code.CONFLICT, () -> fixture.check(first, header, 1200));
      ExecutionStore.Lease replacement = fixture.claim(1200);
      assertEquals(first.attempt(), replacement.attempt());
      assertEquals(first.number() + 1, replacement.number());
      assertCode(
          ProtocolError.Code.CLOCK_UNSAFE,
          () ->
              fixture.sessions.checkProducedInput(
                  execAccess(),
                  replacement,
                  SELECTED,
                  fixture.inputs,
                  header,
                  () -> new AdmissionStore.Time(0, false),
                  ALLOW));
      assertEquals(receipt, fixture.check(replacement, header, 1200).orElseThrow());
      assertEquals(receipt, fixture.admit(replacement, header, 1200));
      assertEquals(funded, fixture.inputs.usage());
      assertEquals(2, scalar(fixture.database, "SELECT count(*) FROM ps_v2_jobs"));
      assertEquals(4, scalar(fixture.database, "SELECT count(*) FROM ps_v2_operations"));
      assertEquals(Records.State.ACTIVE, fixture.view(header.parameters().work()).state());

      Records.InputHeader changed = fixture.header(10, operation(21), new byte[] {9});
      assertCode(ProtocolError.Code.CONFLICT, () -> fixture.check(replacement, changed, 1200));
      assertEquals(funded, fixture.inputs.usage());
    }
  }

  @Test
  void quotaRefusalPreservesAcceptedSiblingAndAccounting() throws Exception {
    try (Fixture fixture = new Fixture("capacity", 2)) {
      ExecutionStore.Lease parent = fixture.prepareParent();
      fixture.declareProduced(parent, operation(30), List.of(10L, 20L, 30L), true);
      Records.InputHeader accepted = fixture.header(10, operation(31), new byte[] {1});
      fixture.receive(accepted, new byte[] {1});
      Records.OperationReceipt retained = fixture.admit(parent, accepted);
      InputStore.Usage funded = fixture.inputs.usage();

      Records.InputHeader excess = fixture.header(20, operation(32), new byte[] {2});
      fixture.receive(excess, new byte[] {2});
      InputStore.Usage beforeRefusal = fixture.inputs.usage();
      assertCode(ProtocolError.Code.LIMIT_EXCEEDED, () -> fixture.admit(parent, excess));
      assertEquals(beforeRefusal, fixture.inputs.usage());
      assertTrue(fixture.inputs.findReservation(fixture.context(), excess).isEmpty());
      assertEquals(Records.State.DECLARED, fixture.view(excess.parameters().work()).state());
      assertEquals(retained, fixture.check(parent, accepted).orElseThrow());

      assertEquals(Records.State.ACTIVE, fixture.view(accepted.parameters().work()).state());
    }
  }

  @Test
  void preflightAcceptsPartialPayloadButAdmissionRequiresFinishedInput() throws Exception {
    try (Fixture fixture = new Fixture("partial", 8)) {
      ExecutionStore.Lease parent = fixture.prepareParent();
      fixture.declareProduced(parent, operation(34), List.of(10L), true);
      Records.InputHeader partial = fixture.header(10, operation(35), new byte[] {3, 4});
      try (InputStore.Receiver receiver =
          fixture.inputs.begin(fixture.context(), partial, SELECTED, 1100)) {
        receiver.write(ByteBuffer.wrap(new byte[] {3}), 1100);
        assertTrue(fixture.check(parent, partial).isEmpty());
        assertCode(ProtocolError.Code.NOT_READY, () -> fixture.admit(parent, partial));
      }
      assertTrue(fixture.inputs.findReservation(fixture.context(), partial).isEmpty());
      assertEquals(Records.State.DECLARED, fixture.view(partial.parameters().work()).state());
    }
  }

  @Test
  void staleDeadlineAndFinalAuthorizationFencesRollBackFreshProducedAdmission() throws Exception {
    try (Fixture stale = new Fixture("stale", 8)) {
      ExecutionStore.Lease first = stale.prepareParent();
      stale.declareProduced(first, operation(40), List.of(10L), true);
      Records.InputHeader header = stale.header(10, operation(41), new byte[] {1});
      stale.receive(header, new byte[] {1});
      ExecutionStore.Lease replacement = stale.claim(1200);
      assertCode(ProtocolError.Code.CONFLICT, () -> stale.admit(first, header, 1200));
      assertTrue(stale.inputs.findReservation(stale.context(), header).isEmpty());
      assertTrue(stale.check(replacement, header, 1200).isEmpty());
    }

    try (Fixture deadline = new Fixture("deadline", 8)) {
      ExecutionStore.Lease parent = deadline.prepareParent();
      deadline.declareProduced(parent, operation(50), List.of(10L), true);
      Records.InputHeader header = deadline.header(10, operation(51), new byte[] {1});
      deadline.receive(header, new byte[] {1});
      AtomicLong time = new AtomicLong(1100);
      AtomicInteger checks = new AtomicInteger();
      AdmissionStore.Authorization advanceAtFinalInputCheck =
          (binding, parameters) -> {
            if (checks.incrementAndGet() == 3) time.set(11_000);
          };
      assertCode(
          ProtocolError.Code.DEADLINE_EXCEEDED,
          () ->
              deadline.sessions.admitProduced(
                  execAccess(),
                  parent,
                  SELECTED,
                  deadline.inputs,
                  header,
                  () -> new AdmissionStore.Time(time.get(), true),
                  advanceAtFinalInputCheck));
      assertEquals(5, checks.get());
      assertEquals(Records.State.DECLARED, deadline.view(header.parameters().work()).state());
      assertEquals(1, scalar(deadline.database, "SELECT count(*) FROM ps_v2_jobs"));
      assertEquals(3, scalar(deadline.database, "SELECT count(*) FROM ps_v2_operations"));
    }

    try (Fixture revoked = new Fixture("revoked", 8)) {
      ExecutionStore.Lease parent = revoked.prepareParent();
      revoked.declareProduced(parent, operation(60), List.of(10L), true);
      Records.InputHeader header = revoked.header(10, operation(61), new byte[] {1});
      revoked.receive(header, new byte[] {1});
      AtomicInteger checks = new AtomicInteger();
      AdmissionStore.Authorization denyAfterFundingBeforeRepeatChildCheck =
          (binding, parameters) -> {
            if (checks.incrementAndGet() == 3)
              throw new ProtocolError(ProtocolError.Code.UNAUTHORIZED, "revoked");
          };
      assertCode(
          ProtocolError.Code.UNAUTHORIZED,
          () ->
              revoked.sessions.admitProduced(
                  execAccess(),
                  parent,
                  SELECTED,
                  revoked.inputs,
                  header,
                  clock(1100),
                  denyAfterFundingBeforeRepeatChildCheck));
      assertEquals(3, checks.get());
      assertEquals(Records.State.DECLARED, revoked.view(header.parameters().work()).state());
      assertEquals(1, scalar(revoked.database, "SELECT count(*) FROM ps_v2_jobs"));
      assertEquals(3, scalar(revoked.database, "SELECT count(*) FROM ps_v2_operations"));
    }
  }

  @Test
  void finalParentGateCannotCommitAChildWhoseShorterDeadlineWasReached() throws Exception {
    try (Fixture fixture = new Fixture("short-child-deadline", 8)) {
      ExecutionStore.Lease parent = fixture.prepareParent(1000, 200);
      assertEquals(1200, parent.until());
      fixture.declareProduced(parent, operation(70), List.of(10L), true, 1000);
      Records.InputHeader header = fixture.header(10, operation(71), new byte[] {1}, 50);
      fixture.receive(header, new byte[] {1});
      InputStore.Usage before = fixture.inputs.usage();
      AtomicLong time = new AtomicLong(1000);
      AtomicInteger parentChecks = new AtomicInteger();
      AdmissionStore.Authorization advanceAtFinalParentGate =
          (binding, parameters) -> {
            if (parameters.work().equals(PARENT) && parentChecks.incrementAndGet() == 2)
              time.set(1050);
          };

      assertCode(
          ProtocolError.Code.DEADLINE_EXCEEDED,
          () ->
              fixture.sessions.admitProduced(
                  execAccess(),
                  parent,
                  SELECTED,
                  fixture.inputs,
                  header,
                  () -> new AdmissionStore.Time(time.get(), true),
                  advanceAtFinalParentGate));
      assertEquals(2, parentChecks.get());
      assertEquals(Records.State.ACTIVE, fixture.view(PARENT).state());
      assertEquals(Records.State.DECLARED, fixture.view(header.parameters().work()).state());
      assertEquals(1, scalar(fixture.database, "SELECT count(*) FROM ps_v2_jobs"));
      assertEquals(3, scalar(fixture.database, "SELECT count(*) FROM ps_v2_operations"));
      assertTrue(fixture.inputs.usage().bytes() > before.bytes());
      assertTrue(fixture.inputs.findReservation(fixture.context(), header).isPresent());
    }
  }

  private final class Fixture implements AutoCloseable {
    final Path database;
    final Path inputPath;
    final int maxJobs;
    SessionStore sessions;
    InputStore inputs;
    Messages.Binding binding;
    Records.ChildScope child;
    int request = 10;

    Fixture(String name, int maxJobs) throws Exception {
      this.maxJobs = maxJobs;
      database = directory.resolve(name + ".sqlite");
      inputPath = directory.resolve(name + "-inputs");
      sessions = SessionStore.initialize(database, configuration(maxJobs));
      binding =
          sessions.create(
              sessionAccess(),
              SELECTED,
              new Messages.Create(1, 1, new Records.Policy(20_000, 20_000, 30_000)));
      assertEquals(1, binding.generation());
      inputs = InputStore.initializeForAuthority(inputPath, INPUT_LIMITS, sessions.identity());
      sessions.bindInputs(inputs);
    }

    ExecutionStore.Lease prepareParent() throws Exception {
      return prepareParent(1100, 100);
    }

    ExecutionStore.Lease prepareParent(long now, long leaseMillis) throws Exception {
      sessions.declare(
          sessionAccess(),
          SELECTED,
          1,
          new Messages.Declare(request++, operation(request++), 0, List.of(1L), false));
      Records.InputHeader parent =
          new Records.InputHeader(
              1,
              operation(20),
              new Records.AdmitParameters(
                  PARENT,
                  new Records.Input(0, digest(new byte[0]), "application/octet-stream"),
                  "expand",
                  2,
                  10_000,
                  new Records.OutputBudget(0, 0)));
      receive(parent, new byte[0]);
      sessions.admit(sessionAccess(), SELECTED, 1, inputs, parent, request++, clock(1000), ALLOW);
      ExecutionStore.Lease lease =
          sessions.claimExecution(execAccess(), 1, PARENT, inputs, leaseMillis, clock(now), ALLOW);
      child = view(PARENT).child();
      assertNotNull(child);
      return lease;
    }

    Messages.DeclarationResponse declareProduced(
        ExecutionStore.Lease parent,
        Records.OperationId operation,
        List<Long> children,
        boolean seal)
        throws Exception {
      return declareProduced(parent, operation, children, seal, 1100);
    }

    Messages.DeclarationResponse declareProduced(
        ExecutionStore.Lease parent,
        Records.OperationId operation,
        List<Long> children,
        boolean seal,
        long now)
        throws Exception {
      return sessions.declareProduced(
          execAccess(),
          parent,
          SELECTED,
          inputs,
          new Messages.Declare(request++, operation, child.scope(), children, seal),
          clock(now),
          ALLOW);
    }

    Records.InputHeader header(long entity, Records.OperationId operation, byte[] payload)
        throws Exception {
      return header(entity, operation, payload, 10_000);
    }

    Records.InputHeader header(
        long entity, Records.OperationId operation, byte[] payload, long executionMillis)
        throws Exception {
      return new Records.InputHeader(
          1,
          operation,
          new Records.AdmitParameters(
              new Records.WorkKey(child.scope(), child.producer(), entity),
              new Records.Input(payload.length, digest(payload), "application/octet-stream"),
              "leaf",
              0,
              executionMillis,
              new Records.OutputBudget(0, 0)));
    }

    void receive(Records.InputHeader header, byte[] payload) throws Exception {
      try (InputStore.Receiver receiver = inputs.begin(context(), header, SELECTED, 1100)) {
        receiver.write(ByteBuffer.wrap(payload), 1100);
        receiver.finish(1100);
      }
    }

    java.util.Optional<Records.OperationReceipt> check(
        ExecutionStore.Lease parent, Records.InputHeader header) throws Exception {
      return check(parent, header, 1100);
    }

    java.util.Optional<Records.OperationReceipt> check(
        ExecutionStore.Lease parent, Records.InputHeader header, long now) throws Exception {
      return sessions.checkProducedInput(
          execAccess(), parent, SELECTED, inputs, header, clock(now), ALLOW);
    }

    Records.OperationReceipt admit(ExecutionStore.Lease parent, Records.InputHeader header)
        throws Exception {
      return admit(parent, header, 1100);
    }

    Records.OperationReceipt admit(
        ExecutionStore.Lease parent, Records.InputHeader header, long now) throws Exception {
      return sessions.admitProduced(
          execAccess(), parent, SELECTED, inputs, header, clock(now), ALLOW);
    }

    ExecutionStore.Lease claim(long now) throws Exception {
      return sessions.claimExecution(execAccess(), 1, PARENT, inputs, 100, clock(now), ALLOW);
    }

    Records.WorkView view(Records.WorkKey work) throws Exception {
      return sessions
          .snapshot(sessionAccess(), SELECTED, 1, new Messages.Watch(request++, work, 0, 0))
          .work();
    }

    Commitments.Context context() {
      return new Commitments.Context("issuer-a", "alice", 1);
    }

    void reopen() throws Exception {
      inputs.close();
      sessions = SessionStore.open(database, configuration(maxJobs));
      inputs = InputStore.open(inputPath, INPUT_LIMITS);
      sessions.verifyInputs(inputs);
    }

    @Override
    public void close() throws java.io.IOException {
      inputs.close();
    }
  }

  private static SessionStore.Configuration configuration(int maxJobs) {
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
        new AdmissionStore.ExecutionPolicy(List.of(parent, leaf), maxJobs, maxJobs));
  }

  private static Records.Digest digest(byte[] bytes) throws Exception {
    return new Records.Digest(MessageDigest.getInstance("SHA-256").digest(bytes));
  }

  private static long scalar(Path database, String query) throws Exception {
    try (var connection = BoundedSqlite.open(database, configuration(8).files()).connect();
        var statement = connection.createStatement();
        var rows = statement.executeQuery(query)) {
      assertTrue(rows.next());
      return rows.getLong(1);
    }
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
