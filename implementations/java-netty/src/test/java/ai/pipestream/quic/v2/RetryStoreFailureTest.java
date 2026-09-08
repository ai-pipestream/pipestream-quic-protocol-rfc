package ai.pipestream.quic.v2;

import static ai.pipestream.quic.v2.Messages.DURABLE_WORK;
import static org.junit.jupiter.api.Assertions.*;

import ai.pipestream.quic.BoundedSqlite;
import java.io.IOException;
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

@Timeout(20)
final class RetryStoreFailureTest {
  private static final Messages.Capabilities SELECTED =
      new Messages.Capabilities(
          true, List.of(DURABLE_WORK), List.of(), 1 << 20, 8, 16, 1 << 20, 1000, 5000);
  private static final InputStore.Limits INPUT_LIMITS =
      new InputStore.Limits(4L << 20, 32, 1 << 20, 8);
  private static final Records.WorkKey WORK = new Records.WorkKey(0, 0, 1);
  private static final Records.WorkKey DECLARED = new Records.WorkKey(0, 0, 2);
  private static final AdmissionStore.Authorization ALLOW = (binding, parameters) -> {};

  @TempDir Path directory;

  @Test
  void retryAdvancesExactlyOnceAndReplaysItsStableReceiptAfterReopen() throws Exception {
    try (Fixture fixture = fixture("replay", 16)) {
      Messages.Retry first = new Messages.Retry(10, operation(10), WORK, 1);
      Messages.RetryResponse accepted =
          fixture.sessions.retry(access(), SELECTED, 1, first, clock(1100), ALLOW);
      Records.Retried outcome =
          assertInstanceOf(Records.Retried.class, accepted.receipt().outcome());
      assertEquals(new Records.Retried(WORK, 1, 2, 1100), outcome);
      assertEquals(2, view(fixture.sessions, 20, WORK).attempt());
      assertEquals(2000, view(fixture.sessions, 21, WORK).deadline());
      assertEquals(JobRecord.Stage.QUEUED, job(fixture).stage());

      assertCode(
          ProtocolError.Code.CONFLICT,
          () ->
              fixture.sessions.retry(
                  access(),
                  SELECTED,
                  1,
                  new Messages.Retry(11, operation(11), WORK, 1),
                  clock(1101),
                  ALLOW));
      fixture.reopen();
      Messages.RetryResponse replay =
          fixture.sessions.retry(
              access(),
              SELECTED,
              1,
              new Messages.Retry(12, operation(10), WORK, 1),
              () -> new AdmissionStore.Time(0, false),
              ALLOW);
      assertEquals(12, replay.request());
      assertEquals(accepted.receipt(), replay.receipt());
      assertEquals(accepted.receipt(), lookup(fixture.sessions, operation(10)));
      assertEquals(2, view(fixture.sessions, 22, WORK).attempt());
    }
  }

  @Test
  void declaredTerminalOwnerAndApplicationRefusalsHaveExactCodes() throws Exception {
    try (Fixture fixture = fixture("states", 16)) {
      assertCode(
          ProtocolError.Code.NOT_READY,
          () ->
              fixture.sessions.retry(
                  access(), SELECTED, 1, retry(20, DECLARED, 1), clock(1100), ALLOW));
      assertCode(
          ProtocolError.Code.UNAUTHORIZED,
          () ->
              fixture.sessions.retry(
                  new SessionStore.Access("bob", () -> {}),
                  SELECTED,
                  1,
                  retry(21, WORK, 1),
                  clock(1100),
                  ALLOW));
      assertCode(
          ProtocolError.Code.UNAUTHORIZED,
          () ->
              fixture.sessions.retry(
                  access(),
                  SELECTED,
                  1,
                  retry(22, WORK, 1),
                  clock(1100),
                  (binding, parameters) -> {
                    throw new ProtocolError(ProtocolError.Code.UNAUTHORIZED, "application revoked");
                  }));

      ExecutionStore.Lease lease =
          fixture.sessions.claimExecution(
              execAccess(), 1, WORK, fixture.inputs, 500, clock(1100), ALLOW);
      fixture.sessions.failExecution(
          execAccess(), lease, new Records.Diagnostic(8, "terminal"), false, clock(1200), ALLOW);
      assertCode(
          ProtocolError.Code.ALREADY_TERMINAL,
          () ->
              fixture.sessions.retry(
                  access(), SELECTED, 1, retry(23, WORK, 1), clock(1200), ALLOW));
    }
  }

  @Test
  void finalTimeAndAuthorizationRefusalsRollBackEveryAuthoritativeWrite() throws Exception {
    for (String cause :
        List.of("deadline-equal", "deadline-after", "rollback", "access", "application")) {
      try (Fixture fixture = fixture("final-" + cause, 16)) {
        Durable before = durable(fixture);
        AtomicLong now = new AtomicLong(1100);
        AtomicInteger accessChecks = new AtomicInteger();
        AtomicInteger applicationChecks = new AtomicInteger();
        SessionStore.Access gatedAccess =
            new SessionStore.Access(
                "alice",
                () -> {
                  int call = accessChecks.incrementAndGet();
                  if (cause.equals("access") && call == 3) {
                    throw new ProtocolError(ProtocolError.Code.UNAUTHORIZED, "credential revoked");
                  }
                });
        AdmissionStore.Authorization gatedApplication =
            (binding, parameters) -> {
              int call = applicationChecks.incrementAndGet();
              if (call == 2) {
                if (cause.equals("deadline-equal")) now.set(2000);
                if (cause.equals("deadline-after")) now.set(2001);
                if (cause.equals("rollback")) now.set(1099);
                if (cause.equals("application")) {
                  throw new ProtocolError(ProtocolError.Code.UNAUTHORIZED, "application revoked");
                }
              }
            };
        ProtocolError.Code expected =
            switch (cause) {
              case "deadline-equal", "deadline-after" -> ProtocolError.Code.DEADLINE_EXCEEDED;
              case "rollback" -> ProtocolError.Code.CLOCK_UNSAFE;
              default -> ProtocolError.Code.UNAUTHORIZED;
            };
        assertCode(
            expected,
            () ->
                fixture.sessions.retry(
                    gatedAccess,
                    SELECTED,
                    1,
                    retry(30, WORK, 1),
                    () -> new AdmissionStore.Time(now.get(), true),
                    gatedApplication));
        assertEquals(before, durable(fixture), cause);
        assertLookupMissing(fixture.sessions, operation(30));
        assertEquals(1, view(fixture.sessions, 31, WORK).attempt());
        assertEquals(3, accessChecks.get(), cause);
        if (!cause.equals("access")) assertEquals(2, applicationChecks.get(), cause);
      }
    }
  }

  @Test
  void operationCapacityRefusalDoesNotAdvanceAttemptOrRewriteJob() throws Exception {
    try (Fixture fixture = fixture("operation-limit", 2)) {
      Durable before = durable(fixture);
      assertCode(
          ProtocolError.Code.LIMIT_EXCEEDED,
          () ->
              fixture.sessions.retry(
                  access(), SELECTED, 1, retry(40, WORK, 1), clock(1100), ALLOW));
      assertEquals(before, durable(fixture));
      assertLookupMissing(fixture.sessions, operation(40));
      fixture.reopen();
      assertEquals(1, view(fixture.sessions, 41, WORK).attempt());
    }
  }

  @Test
  void recoveryRequiresAnExactContiguousRetryReceiptChain() throws Exception {
    try (Fixture valid = fixture("valid-chain", 16)) {
      valid.sessions.retry(access(), SELECTED, 1, retry(50, WORK, 1), clock(1100), ALLOW);
      valid.sessions.retry(access(), SELECTED, 1, retry(51, WORK, 2), clock(1200), ALLOW);
      assertEquals(3, view(valid.sessions, 52, WORK).attempt());
      valid.reopen();
      assertEquals(3, view(valid.sessions, 53, WORK).attempt());
      assertEquals(
          new Records.Retried(WORK, 2, 3, 1200), lookup(valid.sessions, operation(51)).outcome());
    }

    try (Fixture missing = fixture("missing-chain", 16)) {
      missing.sessions.retry(access(), SELECTED, 1, retry(60, WORK, 1), clock(1100), ALLOW);
      try (Connection connection =
              BoundedSqlite.open(missing.database, missing.configuration.files()).connect();
          var delete =
              connection.prepareStatement(
                  "DELETE FROM ps_v2_operations WHERE generation=1 AND producer=0 AND operation=?");
          var count =
              connection.prepareStatement(
                  "UPDATE ps_v2_sessions SET operation_count=2 WHERE generation=1")) {
        delete.setBytes(1, operation(60).bytes());
        assertEquals(1, delete.executeUpdate());
        assertEquals(1, count.executeUpdate());
      }
      assertThrows(
          SQLException.class, () -> SessionStore.open(missing.database, missing.configuration));
    }

    try (Fixture indexed = fixture("wrong-index", 16)) {
      indexed.sessions.retry(access(), SELECTED, 1, retry(70, WORK, 1), clock(1100), ALLOW);
      try (Connection connection =
              BoundedSqlite.open(indexed.database, indexed.configuration.files()).connect();
          var update =
              connection.prepareStatement(
                  "UPDATE ps_v2_operations SET retry_attempt=2 WHERE generation=1 AND producer=0"
                      + " AND operation=?")) {
        update.setBytes(1, operation(70).bytes());
        assertEquals(1, update.executeUpdate());
      }
      assertThrows(SQLException.class, () -> lookup(indexed.sessions, operation(70)));
      assertThrows(
          SQLException.class, () -> SessionStore.open(indexed.database, indexed.configuration));
    }
  }

  private Fixture fixture(String name, long operations) throws Exception {
    Path database = directory.resolve(name + ".sqlite");
    Path inputsPath = directory.resolve(name + "-inputs");
    SessionStore sessions = SessionStore.initialize(database, configuration(operations));
    Messages.Binding binding =
        sessions.create(
            access(),
            SELECTED,
            new Messages.Create(1, 1, new Records.Policy(10_000, 20_000, 30_000)));
    sessions.declare(
        access(), SELECTED, 1, new Messages.Declare(2, operation(1), 0, List.of(1L, 2L), false));
    InputStore inputs =
        InputStore.initializeForAuthority(inputsPath, INPUT_LIMITS, sessions.identity());
    sessions.bindInputs(inputs);
    Records.InputHeader header = header();
    try (InputStore.Receiver receiver = inputs.begin(context(), header, SELECTED, 1)) {
      receiver.write(ByteBuffer.allocate(0), 2);
      receiver.finish(3);
    }
    sessions.admit(access(), SELECTED, 1, inputs, header, 2, clock(1000), ALLOW);
    return new Fixture(database, inputsPath, configuration(operations), sessions, inputs, binding);
  }

  private static SessionStore.Configuration configuration(long operations) {
    return new SessionStore.Configuration(
        "issuer-a",
        new Records.Limits(8, 16, operations, 1 << 20, 1 << 20, 4),
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

  private static Messages.Retry retry(int operation, Records.WorkKey work, long expected) {
    return new Messages.Retry(9, operation(operation), work, expected);
  }

  private static Records.WorkView view(SessionStore sessions, long request, Records.WorkKey work)
      throws Exception {
    return sessions.snapshot(access(), SELECTED, 1, new Messages.Watch(request, work, 0, 0)).work();
  }

  private static JobRecord job(Fixture fixture) throws Exception {
    try (Connection connection =
        BoundedSqlite.open(fixture.database, fixture.configuration.files()).connect()) {
      return AdmissionStore.job(connection, fixture.binding, WORK).record();
    }
  }

  private static Durable durable(Fixture fixture) throws Exception {
    try (Connection connection =
        BoundedSqlite.open(fixture.database, fixture.configuration.files()).connect()) {
      AdmissionStore.StoredJob stored = AdmissionStore.job(connection, fixture.binding, WORK);
      long workSlot;
      try (var query =
          connection.prepareStatement(
              "SELECT view_slot FROM ps_v2_entities WHERE generation=? AND scope=? AND id=?")) {
        query.setLong(1, fixture.binding.generation());
        query.setLong(2, WORK.scope());
        query.setLong(3, WORK.entity());
        try (var rows = query.executeQuery()) {
          assertTrue(rows.next());
          workSlot = rows.getLong(1);
        }
      }
      return new Durable(
          view(fixture.sessions, 90, WORK),
          stored.record(),
          geometry(FixedRecords.header(connection, workSlot, FixedRecords.Kind.WORK)),
          geometry(stored.geometry()),
          clock(fixture));
    }
  }

  private static Geometry geometry(FixedRecords.Header header) {
    return new Geometry(header.revision(), header.credits(), header.used(), header.capacity());
  }

  private static ClockImage clock(Fixture fixture) throws Exception {
    try (Connection connection =
            BoundedSqlite.open(fixture.database, fixture.configuration.files()).connect();
        var statement = connection.createStatement();
        var rows = statement.executeQuery("SELECT clock_slot FROM ps_v2_meta WHERE singleton=1")) {
      assertTrue(rows.next());
      FixedRecords.Header header =
          FixedRecords.header(connection, rows.getLong(1), FixedRecords.Kind.CLOCK);
      FixedRecords.Snapshot snapshot =
          FixedRecords.read(
              connection,
              FixedRecords.CLOCK,
              FixedRecords.Kind.CLOCK,
              FixedRecords.clockKey("issuer-a"));
      return new ClockImage(
          header.revision(),
          new Cbor.Reader(snapshot.body(), FixedRecords.CLOCK_CAPACITY).number());
    }
  }

  private static Records.OperationReceipt lookup(
      SessionStore sessions, Records.OperationId operation) throws Exception {
    return sessions
        .lookupOperation(access(), SELECTED, 1, new Messages.LookupOperation(91, operation))
        .receipt();
  }

  private static void assertLookupMissing(SessionStore sessions, Records.OperationId operation) {
    assertCode(
        ProtocolError.Code.NOT_FOUND,
        () ->
            sessions.lookupOperation(
                access(), SELECTED, 1, new Messages.LookupOperation(92, operation)));
  }

  private static AdmissionStore.Clock clock(long utc) {
    return () -> new AdmissionStore.Time(utc, true);
  }

  private static Commitments.Context context() {
    return new Commitments.Context("issuer-a", "alice", 1);
  }

  private static Records.OperationId operation(int value) {
    byte[] bytes = new byte[16];
    bytes[15] = (byte) value;
    return new Records.OperationId(bytes);
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

  private record Durable(
      Records.WorkView view,
      JobRecord job,
      Geometry workGeometry,
      Geometry jobGeometry,
      ClockImage clock) {}

  private record Geometry(long revision, long credits, int used, int capacity) {}

  private record ClockImage(long revision, long utc) {}

  private static final class Fixture implements AutoCloseable {
    final Path database;
    final Path inputsPath;
    final SessionStore.Configuration configuration;
    SessionStore sessions;
    InputStore inputs;
    final Messages.Binding binding;

    Fixture(
        Path database,
        Path inputsPath,
        SessionStore.Configuration configuration,
        SessionStore sessions,
        InputStore inputs,
        Messages.Binding binding) {
      this.database = database;
      this.inputsPath = inputsPath;
      this.configuration = configuration;
      this.sessions = sessions;
      this.inputs = inputs;
      this.binding = binding;
    }

    void reopen() throws Exception {
      inputs.close();
      sessions = SessionStore.open(database, configuration);
      inputs = InputStore.open(inputsPath, INPUT_LIMITS);
      sessions.verifyInputs(inputs);
    }

    @Override
    public void close() throws IOException {
      inputs.close();
    }
  }

  @FunctionalInterface
  private interface Throwing {
    void run() throws Exception;
  }
}
