package ai.pipestream.quic.v2;

import static ai.pipestream.quic.v2.Messages.DURABLE_WORK;
import static org.junit.jupiter.api.Assertions.*;

import ai.pipestream.quic.BoundedSqlite;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

@Timeout(20)
final class ExecutionStorePolicyTest {
  private static final Messages.Capabilities SELECTED =
      new Messages.Capabilities(
          true, List.of(DURABLE_WORK), List.of(), 1 << 20, 8, 16, 1 << 20, 1000, 5000);
  private static final InputStore.Limits INPUT_LIMITS =
      new InputStore.Limits(4L << 20, 32, 1 << 20, 8);
  private static final Records.WorkKey WORK = new Records.WorkKey(0, 0, 1);
  private static final AdmissionStore.Authorization ALLOW = (binding, parameters) -> {};

  @TempDir Path directory;

  @Test
  void expiryForwardJumpPastPromisedReceiptIntervalRollsBackBeforeStableSettlement()
      throws Exception {
    Fixture fixture = fixture("expiry-jump");
    try (InputStore inputs = fixture.inputs()) {
      assertNotNull(inputs.identity());
      JobRecord before = job(fixture);
      long[] credits = credits(fixture);
      AtomicInteger samples = new AtomicInteger();
      AdmissionStore.Clock jumped =
          () ->
              new AdmissionStore.Time(
                  samples.getAndIncrement() == 0
                      ? 2000
                      : 2000 + fixture.binding().policy().receiptRetention(),
                  true);
      assertCode(
          ProtocolError.Code.CLOCK_UNSAFE,
          () -> fixture.sessions().expireExecution(1, WORK, jumped));
      assertEquals(before, job(fixture));
      assertArrayEquals(credits, credits(fixture));
      assertEquals(Records.State.ACTIVE, view(fixture).state());

      Records.WorkView settled = fixture.sessions().expireExecution(1, WORK, clock(2000));
      assertEquals(Records.State.FAILED, settled.state());
      assertEquals(1, settled.attempt());
    }
  }

  @Test
  void renewalFinalGateUsesOldLeaseAndRollsBackTimeAdvanceOrDenial() throws Exception {
    Fixture fixture = fixture("renew-final");
    try (InputStore inputs = fixture.inputs()) {
      assertNotNull(inputs.identity());
      ExecutionStore.Lease lease = claim(fixture, 1100, 200);
      JobRecord before = job(fixture);
      AtomicInteger samples = new AtomicInteger();
      AdmissionStore.Clock crossesOldLease =
          () ->
              new AdmissionStore.Time(
                  switch (samples.getAndIncrement()) {
                    case 0, 1 -> 1200;
                    default -> 1300;
                  },
                  true);
      assertCode(
          ProtocolError.Code.CONFLICT,
          () ->
              fixture.sessions().renewExecution(execAccess(), lease, 500, crossesOldLease, ALLOW));
      assertEquals(before, job(fixture));

      AtomicInteger checks = new AtomicInteger();
      AdmissionStore.Authorization denied =
          (binding, parameters) -> {
            if (checks.incrementAndGet() == 3)
              throw new ProtocolError(ProtocolError.Code.UNAUTHORIZED, "withdrawn");
          };
      assertCode(
          ProtocolError.Code.UNAUTHORIZED,
          () -> fixture.sessions().renewExecution(execAccess(), lease, 500, clock(1200), denied));
      assertEquals(before, job(fixture));
      fixture.sessions().checkExecution(execAccess(), lease, clock(1200), ALLOW);
    }
  }

  private Fixture fixture(String name) throws Exception {
    Path database = directory.resolve(name + ".sqlite");
    Path inputsPath = directory.resolve(name + "-inputs");
    SessionStore sessions = SessionStore.initialize(database, configuration());
    Messages.Binding binding =
        sessions.create(
            sessionAccess(),
            SELECTED,
            new Messages.Create(1, 1, new Records.Policy(10_000, 20_000, 30_000)));
    sessions.declare(
        sessionAccess(), SELECTED, 1, new Messages.Declare(2, operation(1), 0, List.of(1L), false));
    InputStore inputs =
        InputStore.initializeForAuthority(inputsPath, INPUT_LIMITS, sessions.identity());
    sessions.bindInputs(inputs);
    Records.InputHeader header = header();
    try (InputStore.Receiver receiver = inputs.begin(context(), header, SELECTED, 1)) {
      receiver.write(ByteBuffer.allocate(0), 2);
      receiver.finish(3);
    }
    sessions.admit(sessionAccess(), SELECTED, 1, inputs, header, 2, clock(1000), ALLOW);
    return new Fixture(database, sessions, inputs, binding);
  }

  private static ExecutionStore.Lease claim(Fixture fixture, long now, long duration)
      throws Exception {
    return fixture
        .sessions()
        .claimExecution(execAccess(), 1, WORK, fixture.inputs(), duration, clock(now), ALLOW);
  }

  private static JobRecord job(Fixture fixture) throws Exception {
    try (var connection =
        BoundedSqlite.open(fixture.database(), configuration().files()).connect()) {
      return AdmissionStore.job(connection, fixture.binding(), WORK).record();
    }
  }

  private static long[] credits(Fixture fixture) throws Exception {
    try (var connection =
            BoundedSqlite.open(fixture.database(), configuration().files()).connect();
        var statement = connection.createStatement();
        var rows =
            statement.executeQuery(
                "SELECT e.view_slot,j.state_slot FROM ps_v2_entities e JOIN ps_v2_jobs j ON"
                    + " e.generation=j.generation AND e.scope=j.scope AND e.id=j.entity")) {
      assertTrue(rows.next());
      return new long[] {
        FixedRecords.header(connection, rows.getLong(1), FixedRecords.Kind.WORK).credits(),
        FixedRecords.header(connection, rows.getLong(2), FixedRecords.Kind.JOB).credits()
      };
    }
  }

  private static Records.WorkView view(Fixture fixture) throws Exception {
    return fixture
        .sessions()
        .snapshot(sessionAccess(), SELECTED, 1, new Messages.Watch(8, WORK, 0, 0))
        .work();
  }

  private static SessionStore.Configuration configuration() {
    return new SessionStore.Configuration(
        "issuer-a",
        new Records.Limits(8, 16, 16, 1 << 20, 1 << 20, 4),
        new Records.Policy(60_000, 60_000, 60_000),
        2,
        8,
        4,
        BoundedSqlite.Limits.defaults(),
        new AdmissionStore.ExecutionPolicy(
            List.of(
                new AdmissionStore.Application(
                    "copy", Set.of(0), AdmissionStore.RestartSafety.IDEMPOTENT)),
            4,
            4));
  }

  private static Records.InputHeader header() throws Exception {
    return new Records.InputHeader(
        1,
        operation(2),
        new Records.AdmitParameters(
            WORK,
            new Records.Input(
                0,
                new Records.Digest(MessageDigest.getInstance("SHA-256").digest(new byte[0])),
                "application/octet-stream"),
            "copy",
            0,
            1000,
            new Records.OutputBudget(0, 0)));
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

  private record Fixture(
      Path database, SessionStore sessions, InputStore inputs, Messages.Binding binding) {}

  @FunctionalInterface
  private interface Throwing {
    void run() throws Exception;
  }
}
