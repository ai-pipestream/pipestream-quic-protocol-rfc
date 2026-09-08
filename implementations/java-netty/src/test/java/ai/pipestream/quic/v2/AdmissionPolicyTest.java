package ai.pipestream.quic.v2;

import static ai.pipestream.quic.v2.Messages.DURABLE_WORK;
import static ai.pipestream.quic.v2.Messages.RESULT_DELIVERY;
import static org.junit.jupiter.api.Assertions.*;

import ai.pipestream.quic.BoundedSqlite;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.sql.Connection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

@Timeout(20)
final class AdmissionPolicyTest {
  private static final InputStore.Limits INPUT_LIMITS =
      new InputStore.Limits(16L << 20, 1024, 1 << 20, 8);
  private static final AdmissionStore.Authorization ALLOW = (binding, parameters) -> {};

  @TempDir Path directory;

  @Test
  void outputProfilesAndResponseBudgetAreCheckedBeforeFundingOrJobs() throws Exception {
    Messages.Capabilities durableOnly = selected(false, 1 << 20);
    Fixture durable = fixture("profiles-durable", execution(8, 8), limits(), durableOnly);
    try (InputStore inputs = durable.inputs()) {
      assertNotNull(inputs.identity());
      Records.InputHeader one = header(2, 1, 17, "copy", 1);
      install(inputs, one, durableOnly);
      assertCode(
          ProtocolError.Code.EXTENSION_UNSUPPORTED,
          () -> admit(durable, one, durableOnly, 1000, ALLOW));
      assertEquals(0, scalar(durable.database(), "SELECT count(*) FROM ps_v2_jobs"));
      assertTrue(inputs.findReservation(context(), one).isEmpty());
    }

    Messages.Capabilities results = selected(true, 65536);
    Fixture result = fixture("profiles-result", execution(8, 8), limits(), results);
    try (InputStore inputs = result.inputs()) {
      Records.InputHeader maximum = header(3, 256, 256, "copy", 1);
      install(inputs, maximum, results);
      assertCode(
          ProtocolError.Code.LIMIT_EXCEEDED, () -> admit(result, maximum, results, 1000, ALLOW));
      assertEquals(0, scalar(result.database(), "SELECT count(*) FROM ps_v2_jobs"));
      assertTrue(inputs.findReservation(context(), maximum).isEmpty());
    }
  }

  @Test
  void validResultFundingPersistsJobAndWorkGeometryAcrossReopen() throws Exception {
    Fixture fixture = fixture("result", execution(8, 8), limits());
    Records.InputHeader header = header(2, 2, 33, "copy", 1);
    try (InputStore inputs = fixture.inputs()) {
      assertNotNull(inputs.identity());
      install(inputs, header);
      Records.OperationReceipt receipt =
          admit(fixture, header, selected(true, 1 << 20), 1000, ALLOW).receipt();
      assertNotNull(inputs.findReservation(context(), header).orElseThrow().reference());
      try (var connection =
              BoundedSqlite.open(
                      fixture.database(), configuration(execution(8, 8), limits()).files())
                  .connect();
          var statement = connection.createStatement();
          var rows =
              statement.executeQuery(
                  """
                  SELECT j.state_slot,e.view_slot FROM ps_v2_jobs j
                  JOIN ps_v2_entities e
                    ON j.generation=e.generation AND j.scope=e.scope AND j.entity=e.id
                  """)) {
        assertTrue(rows.next());
        FixedRecords.Header job =
            FixedRecords.header(connection, rows.getLong(1), FixedRecords.Kind.JOB);
        FixedRecords.Header work =
            FixedRecords.header(connection, rows.getLong(2), FixedRecords.Kind.WORK);
        assertEquals(FixedRecords.JOB_CAPACITY, job.capacity());
        assertEquals(FixedRecords.JOB_CREDITS, job.credits());
        assertTrue(work.capacity() >= 4096);
        assertTrue(work.credits() >= 4);
      }
      assertEquals(receipt, lookup(fixture.sessions(), operation(2)));
    }
    SessionStore reopened =
        SessionStore.open(fixture.database(), configuration(execution(8, 8), limits()));
    try (InputStore inputs = InputStore.open(fixture.inputsPath(), INPUT_LIMITS)) {
      reopened.verifyInputs(inputs);
      assertEquals(1, scalar(fixture.database(), "SELECT count(*) FROM ps_v2_jobs"));
      assertEquals(operation(2), lookup(reopened, operation(2)).operation());
      assertTrue(inputs.findReservation(context(), header).isPresent());
    }
  }

  @Test
  void retainedOperationConflictPrecedesRegistryAndCrossTypeCollision() throws Exception {
    Fixture fixture = fixture("collisions", execution(8, 8), limits());
    try (InputStore inputs = fixture.inputs()) {
      assertNotNull(inputs.identity());
      Records.InputHeader accepted = header(2, 0, 0, "copy", 1);
      install(inputs, accepted);
      admit(fixture, accepted, selected(true, 1 << 20), 1000, ALLOW);
      Records.InputHeader changed = header(2, 0, 0, "not-configured", 1);
      assertCode(
          ProtocolError.Code.CONFLICT,
          () ->
              fixture
                  .sessions()
                  .checkInput(
                      access(), selected(true, 1 << 20), 1, inputs, changed, clock(1000), ALLOW));

      Records.InputHeader declarationCollision = header(1, 0, 0, "copy", 1);
      install(inputs, declarationCollision);
      assertCode(
          ProtocolError.Code.CONFLICT,
          () -> admit(fixture, declarationCollision, selected(true, 1 << 20), 1000, ALLOW));
      assertEquals(1, scalar(fixture.database(), "SELECT count(*) FROM ps_v2_jobs"));
    }
  }

  @Test
  void finalAuthorizationDeadlineAndPersistedClockRefuseOnlyFreshAdmissions() throws Exception {
    Fixture fixture = fixture("clock", execution(8, 8), limits());
    try (InputStore inputs = fixture.inputs()) {
      assertNotNull(inputs.identity());
      Records.InputHeader first = header(2, 0, 0, "copy", 1);
      install(inputs, first);
      Records.OperationReceipt retained =
          admit(fixture, first, selected(true, 1 << 20), 1000, ALLOW).receipt();
      assertEquals(
          retained,
          admit(
                  fixture,
                  first,
                  selected(true, 1 << 20),
                  () -> new AdmissionStore.Time(0, false),
                  ALLOW)
              .receipt());

      declare(fixture.sessions(), 3, 2);
      Records.InputHeader second = header(4, 0, 0, "copy", 2);
      install(inputs, second);
      assertCode(
          ProtocolError.Code.CLOCK_UNSAFE,
          () -> admit(fixture, second, selected(true, 1 << 20), 900, ALLOW));

      AtomicLong time = new AtomicLong(1500);
      AtomicInteger checks = new AtomicInteger();
      AdmissionStore.Authorization advanceAtCommit =
          (binding, parameters) -> {
            if (checks.incrementAndGet() == 3) time.set(2500);
          };
      assertCode(
          ProtocolError.Code.DEADLINE_EXCEEDED,
          () ->
              admit(
                  fixture,
                  second,
                  selected(true, 1 << 20),
                  () -> new AdmissionStore.Time(time.get(), true),
                  advanceAtCommit));
      assertEquals(1, scalar(fixture.database(), "SELECT count(*) FROM ps_v2_jobs"));
      assertCode(ProtocolError.Code.NOT_FOUND, () -> lookup(fixture.sessions(), operation(4)));
    }
  }

  @Test
  void recoveryRejectsValidClockImageBelowRetainedAdmissionTime() throws Exception {
    Fixture fixture = fixture("clock-audit", execution(8, 8), limits());
    try (InputStore inputs = fixture.inputs()) {
      assertNotNull(inputs.identity());
      Records.InputHeader header = header(2, 0, 0, "copy", 1);
      install(inputs, header);
      admit(fixture, header, selected(true, 1 << 20), 1000, ALLOW);
    }
    try (Connection connection =
        BoundedSqlite.open(fixture.database(), configuration(execution(8, 8), limits()).files())
            .connect()) {
      execute(connection, "BEGIN IMMEDIATE");
      long revision =
          FixedRecords.header(connection, FixedRecords.CLOCK, FixedRecords.Kind.CLOCK).revision();
      FixedRecords.replace(
          connection,
          configuration(execution(8, 8), limits()).files(),
          FixedRecords.CLOCK,
          FixedRecords.Kind.CLOCK,
          FixedRecords.clockKey("issuer-a"),
          revision,
          new byte[] {0},
          false);
      execute(connection, "COMMIT");
    }
    assertThrows(
        java.sql.SQLException.class,
        () -> SessionStore.open(fixture.database(), configuration(execution(8, 8), limits())));
  }

  private Fixture fixture(
      String name, AdmissionStore.ExecutionPolicy execution, Records.Limits limits)
      throws Exception {
    return fixture(name, execution, limits, selected(true, 1 << 20));
  }

  private Fixture fixture(
      String name,
      AdmissionStore.ExecutionPolicy execution,
      Records.Limits limits,
      Messages.Capabilities selected)
      throws Exception {
    Path database = directory.resolve(name + ".sqlite");
    Path inputsPath = directory.resolve(name + "-inputs");
    SessionStore sessions = SessionStore.initialize(database, configuration(execution, limits));
    sessions.create(
        access(), selected, new Messages.Create(1, 1, new Records.Policy(10_000, 20_000, 30_000)));
    declare(sessions, selected, 1, 1);
    InputStore inputs =
        InputStore.initializeForAuthority(inputsPath, INPUT_LIMITS, sessions.identity());
    sessions.bindInputs(inputs);
    return new Fixture(database, inputsPath, sessions, inputs);
  }

  private static Messages.AdmissionResponse admit(
      Fixture fixture,
      Records.InputHeader header,
      Messages.Capabilities selected,
      long time,
      AdmissionStore.Authorization authorization)
      throws Exception {
    return admit(fixture, header, selected, clock(time), authorization);
  }

  private static Messages.AdmissionResponse admit(
      Fixture fixture,
      Records.InputHeader header,
      Messages.Capabilities selected,
      AdmissionStore.Clock clock,
      AdmissionStore.Authorization authorization)
      throws Exception {
    return fixture
        .sessions()
        .admit(access(), selected, 1, fixture.inputs(), header, 2, clock, authorization);
  }

  private static void install(InputStore inputs, Records.InputHeader header) throws Exception {
    install(inputs, header, selected(true, 1 << 20));
  }

  private static void install(
      InputStore inputs, Records.InputHeader header, Messages.Capabilities selected)
      throws Exception {
    try (InputStore.Receiver receiver = inputs.begin(context(), header, selected, 1)) {
      receiver.write(ByteBuffer.allocate(0), 2);
      assertEquals(header, receiver.finish(3).header());
    }
  }

  private static void declare(SessionStore sessions, int op, long entity) throws Exception {
    declare(sessions, selected(true, 1 << 20), op, entity);
  }

  private static void declare(
      SessionStore sessions, Messages.Capabilities selected, int op, long entity) throws Exception {
    sessions.declare(
        access(), selected, 1, new Messages.Declare(2, operation(op), 0, List.of(entity), false));
  }

  private static Records.OperationReceipt lookup(
      SessionStore sessions, Records.OperationId operation) throws Exception {
    return sessions
        .lookupOperation(
            access(), selected(true, 1 << 20), 1, new Messages.LookupOperation(9, operation))
        .receipt();
  }

  private static Records.InputHeader header(
      int operation, int outputs, long outputBytes, String application, long entity)
      throws Exception {
    byte[] empty = MessageDigest.getInstance("SHA-256").digest(new byte[0]);
    return new Records.InputHeader(
        1,
        operation(operation),
        new Records.AdmitParameters(
            new Records.WorkKey(0, 0, entity),
            new Records.Input(0, new Records.Digest(empty), "application/octet-stream"),
            application,
            0,
            1000,
            new Records.OutputBudget(outputs, outputBytes)));
  }

  private static Messages.Capabilities selected(boolean results, int control) {
    return new Messages.Capabilities(
        true,
        results ? List.of(DURABLE_WORK, RESULT_DELIVERY) : List.of(DURABLE_WORK),
        List.of(),
        control,
        4,
        256,
        1 << 20,
        1000,
        5000);
  }

  private static AdmissionStore.ExecutionPolicy execution(int jobs, int ownerJobs) {
    return new AdmissionStore.ExecutionPolicy(
        List.of(
            new AdmissionStore.Application(
                "copy", Set.of(0), AdmissionStore.RestartSafety.IDEMPOTENT)),
        jobs,
        ownerJobs);
  }

  private static Records.Limits limits() {
    return new Records.Limits(8, 32, 32, 1 << 20, 1 << 20, 4);
  }

  private static SessionStore.Configuration configuration(
      AdmissionStore.ExecutionPolicy execution, Records.Limits limits) {
    return new SessionStore.Configuration(
        "issuer-a",
        limits,
        new Records.Policy(60_000, 60_000, 60_000),
        4,
        8,
        4,
        BoundedSqlite.Limits.defaults(),
        execution);
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

  private static SessionStore.Access access() {
    return new SessionStore.Access("alice", () -> {});
  }

  private static long scalar(Path database, String sql) throws Exception {
    try (var connection =
            BoundedSqlite.open(database, configuration(execution(8, 8), limits()).files())
                .connect();
        var statement = connection.createStatement();
        var rows = statement.executeQuery(sql)) {
      assertTrue(rows.next());
      return rows.getLong(1);
    }
  }

  private static void execute(Connection connection, String sql) throws Exception {
    try (var statement = connection.createStatement()) {
      statement.execute(sql);
    }
  }

  private static void assertCode(ProtocolError.Code expected, Throwing action) {
    ProtocolError error = assertThrows(ProtocolError.class, action::run);
    assertEquals(expected, error.code(), error::getMessage);
  }

  private record Fixture(
      Path database, Path inputsPath, SessionStore sessions, InputStore inputs) {}

  @FunctionalInterface
  private interface Throwing {
    void run() throws Exception;
  }
}
