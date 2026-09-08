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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

@Timeout(30)
final class AdmissionCapacityTest {
  private static final Messages.Capabilities SELECTED =
      new Messages.Capabilities(
          true,
          List.of(DURABLE_WORK, RESULT_DELIVERY),
          List.of(),
          1 << 20,
          8,
          256,
          1 << 20,
          1000,
          5000);
  private static final AdmissionStore.Clock CLOCK = () -> new AdmissionStore.Time(1000, true);
  private static final AdmissionStore.Authorization ALLOW = (binding, parameters) -> {};

  @TempDir Path directory;

  @Test
  void perOwnerLimitSpansSessionsWithoutConsumingAnotherOwnersGlobalShare() throws Exception {
    Harness h = harness("owner", limits(8, 32, 32, 1 << 20, 1 << 20, 8), 4, 2);
    try (InputStore inputs = h.inputs()) {
      assertSame(inputs, h.inputs());
      Session a1 = session(h, "alice");
      Session a2 = session(h, "alice");
      Session a3 = session(h, "alice");
      Session b1 = session(h, "bob");
      admit(h, a1, 1, new byte[0], 0, 0);
      admit(h, a2, 1, new byte[0], 0, 0);
      assertCode(ProtocolError.Code.LIMIT_EXCEEDED, () -> admit(h, a3, 1, new byte[0], 0, 0));
      admit(h, b1, 1, new byte[0], 0, 0);
      assertEquals(3, scalar(h.database(), h.configuration(), "SELECT count(*) FROM ps_v2_jobs"));
      assertEquals(0, jobs(h, a3));
    }
  }

  @Test
  void globalLimitAcrossOwnersRefusesThirdOwnerIndependentlyOfPerOwnerLimit() throws Exception {
    Harness h = harness("global", limits(8, 32, 32, 1 << 20, 1 << 20, 8), 2, 2);
    try (InputStore inputs = h.inputs()) {
      assertSame(inputs, h.inputs());
      Session alice = session(h, "alice");
      Session bob = session(h, "bob");
      Session carol = session(h, "carol");
      admit(h, alice, 1, new byte[0], 0, 0);
      admit(h, bob, 1, new byte[0], 0, 0);
      assertCode(ProtocolError.Code.LIMIT_EXCEEDED, () -> admit(h, carol, 1, new byte[0], 0, 0));
      assertEquals(2, scalar(h.database(), h.configuration(), "SELECT count(*) FROM ps_v2_jobs"));
      assertEquals(0, jobs(h, carol));
    }
  }

  @Test
  void sessionActiveJobLimitRefusesSecondJobWhileGlobalAndOwnerRemainAvailable() throws Exception {
    Harness h = harness("session", limits(8, 32, 32, 1 << 20, 1 << 20, 1), 8, 8);
    try (InputStore inputs = h.inputs()) {
      assertSame(inputs, h.inputs());
      Session session = session(h, "alice");
      admit(h, session, 1, new byte[0], 0, 0);
      declare(h, session, 2, 2);
      assertCode(ProtocolError.Code.LIMIT_EXCEEDED, () -> admit(h, session, 2, new byte[0], 0, 0));
      assertEquals(1, jobs(h, session));
      assertEquals(Records.State.DECLARED, view(h, session, 2).state());
    }
  }

  @Test
  void sessionInputAndOutputByteLimitsAreIndependent() throws Exception {
    Harness input = harness("input-bytes", limits(8, 32, 32, 3, 100, 8), 8, 8);
    try (InputStore inputs = input.inputs()) {
      assertSame(inputs, input.inputs());
      Session session = session(input, "alice");
      assertCode(
          ProtocolError.Code.LIMIT_EXCEEDED,
          () -> admit(input, session, 1, new byte[] {1, 2, 3, 4}, 0, 0));
      assertEquals(0, jobs(input, session));
    }

    Harness output = harness("output-bytes", limits(8, 32, 32, 100, 3, 8), 8, 8);
    try (InputStore inputs = output.inputs()) {
      assertSame(inputs, output.inputs());
      Session session = session(output, "alice");
      assertCode(
          ProtocolError.Code.LIMIT_EXCEEDED, () -> admit(output, session, 1, new byte[0], 1, 4));
      assertEquals(0, jobs(output, session));
    }
  }

  @Test
  void operationAndScopeLimitsRefuseBeforeJobChildOrReceiptMutation() throws Exception {
    Harness operations = harness("operations", limits(8, 32, 1, 100, 100, 8), 8, 8);
    try (InputStore inputs = operations.inputs()) {
      assertSame(inputs, operations.inputs());
      Session session = session(operations, "alice");
      assertCode(
          ProtocolError.Code.LIMIT_EXCEEDED,
          () -> admit(operations, session, 1, new byte[0], 0, 0));
      assertEquals(0, jobs(operations, session));
      assertEquals(Records.State.DECLARED, view(operations, session, 1).state());
    }

    Harness scopes = harness("scopes", limits(1, 32, 32, 100, 100, 8), 8, 8);
    try (InputStore inputs = scopes.inputs()) {
      assertSame(inputs, scopes.inputs());
      Session session = session(scopes, "alice");
      assertCode(
          ProtocolError.Code.LIMIT_EXCEEDED, () -> admit(scopes, session, 1, new byte[0], 0, 0, 1));
      assertEquals(0, jobs(scopes, session));
      assertEquals(
          1,
          scalar(scopes.database(), scopes.configuration(), "SELECT count(*) FROM ps_v2_scopes"));
      assertEquals(Records.State.DECLARED, view(scopes, session, 1).state());
    }
  }

  @Test
  void maximumOutputCountFundsAndPersistsLargestWorkGeometry() throws Exception {
    Harness h = harness("maximum", limits(8, 32, 32, 1 << 20, 1 << 20, 8), 8, 8);
    try (InputStore inputs = h.inputs()) {
      assertSame(inputs, h.inputs());
      Session session = session(h, "alice");
      admit(h, session, 1, new byte[0], 256, 0);
      try (var connection = BoundedSqlite.open(h.database(), h.configuration().files()).connect();
          var statement = connection.createStatement();
          var rows =
              statement.executeQuery(
                  "SELECT e.view_slot,j.state_slot FROM ps_v2_entities e JOIN ps_v2_jobs j ON"
                      + " e.generation=j.generation AND e.scope=j.scope AND e.id=j.entity")) {
        assertTrue(rows.next());
        assertEquals(
            329728,
            FixedRecords.header(connection, rows.getLong(1), FixedRecords.Kind.WORK).capacity());
        assertEquals(
            FixedRecords.JOB_CREDITS,
            FixedRecords.header(connection, rows.getLong(2), FixedRecords.Kind.JOB).credits());
      }
      assertTrue(h.inputs().usage().bytes() >= 2L * 256 * 8236);
    }
    SessionStore reopened = SessionStore.open(h.database(), h.configuration());
    try (InputStore inputs = InputStore.open(h.inputsPath(), inputLimits())) {
      reopened.verifyInputs(inputs);
      assertEquals(1, scalar(h.database(), h.configuration(), "SELECT count(*) FROM ps_v2_jobs"));
      assertTrue(
          inputs
              .findReservation(context("alice", 1), header(1, 1001, 1, new byte[0], 256, 0, 0))
              .isPresent());
    }
  }

  private Harness harness(String name, Records.Limits limits, int globalJobs, int ownerJobs)
      throws Exception {
    SessionStore.Configuration configuration = configuration(limits, globalJobs, ownerJobs);
    Path database = directory.resolve(name + ".sqlite");
    Path inputsPath = directory.resolve(name + "-inputs");
    SessionStore store = SessionStore.initialize(database, configuration);
    InputStore inputs =
        InputStore.initializeForAuthority(inputsPath, inputLimits(), store.identity());
    store.bindInputs(inputs);
    return new Harness(database, inputsPath, store, inputs, configuration);
  }

  private static Session session(Harness h, String owner) throws Exception {
    long creationSequence =
        h.sessions()
            .nextSequence(access(owner), SELECTED, new Messages.NextSequence(1))
            .nextCreationSequence();
    Messages.Binding binding =
        h.sessions()
            .create(
                access(owner),
                SELECTED,
                new Messages.Create(
                    1, creationSequence, new Records.Policy(10_000, 20_000, 30_000)));
    Session session = new Session(owner, binding.generation(), binding);
    declare(h, session, 101, 1);
    return session;
  }

  private static void declare(Harness h, Session session, int operation, long entity)
      throws Exception {
    h.sessions()
        .declare(
            access(session.owner()),
            SELECTED,
            session.generation(),
            new Messages.Declare(2, operation(operation), 0, List.of(entity), false));
  }

  private static Messages.AdmissionResponse admit(
      Harness h, Session session, int entity, byte[] payload, int outputs, long outputBytes)
      throws Exception {
    return admit(h, session, entity, payload, outputs, outputBytes, 0);
  }

  private static Messages.AdmissionResponse admit(
      Harness h,
      Session session,
      int entity,
      byte[] payload,
      int outputs,
      long outputBytes,
      int mode)
      throws Exception {
    Records.InputHeader header =
        header(session.generation(), 1000 + entity, entity, payload, outputs, outputBytes, mode);
    try (InputStore.Receiver receiver =
        h.inputs().begin(context(session.owner(), session.generation()), header, SELECTED, 1)) {
      receiver.write(ByteBuffer.wrap(payload), 2);
      receiver.finish(3);
    }
    return h.sessions()
        .admit(
            access(session.owner()),
            SELECTED,
            session.generation(),
            h.inputs(),
            header,
            3,
            CLOCK,
            ALLOW);
  }

  private static Records.WorkView view(Harness h, Session session, long entity) throws Exception {
    return h.sessions()
        .snapshot(
            access(session.owner()),
            SELECTED,
            session.generation(),
            new Messages.Watch(5, new Records.WorkKey(0, 0, entity), 0, 0))
        .work();
  }

  private static long jobs(Harness h, Session session) throws Exception {
    return scalar(
        h.database(),
        h.configuration(),
        "SELECT count(*) FROM ps_v2_jobs WHERE generation=" + session.generation());
  }

  private static Records.InputHeader header(
      long generation,
      int operation,
      long entity,
      byte[] payload,
      int outputs,
      long outputBytes,
      int mode)
      throws Exception {
    return new Records.InputHeader(
        generation,
        operation(operation),
        new Records.AdmitParameters(
            new Records.WorkKey(0, 0, entity),
            new Records.Input(
                payload.length,
                new Records.Digest(MessageDigest.getInstance("SHA-256").digest(payload)),
                "application/octet-stream"),
            "copy",
            mode,
            1000,
            new Records.OutputBudget(outputs, outputBytes)));
  }

  private static SessionStore.Configuration configuration(
      Records.Limits limits, int globalJobs, int ownerJobs) {
    return new SessionStore.Configuration(
        "issuer-a",
        limits,
        new Records.Policy(60_000, 60_000, 60_000),
        8,
        32,
        8,
        BoundedSqlite.Limits.defaults(),
        new AdmissionStore.ExecutionPolicy(
            List.of(
                new AdmissionStore.Application(
                    "copy", Set.of(0, 1), AdmissionStore.RestartSafety.IDEMPOTENT)),
            globalJobs,
            ownerJobs));
  }

  private static Records.Limits limits(
      int scopes, int entities, int operations, long input, long output, int activeJobs) {
    return new Records.Limits(scopes, entities, operations, input, output, activeJobs);
  }

  private static InputStore.Limits inputLimits() {
    return new InputStore.Limits(16L << 20, 2048, 1 << 20, 8);
  }

  private static Commitments.Context context(String owner, long generation) {
    return new Commitments.Context("issuer-a", owner, generation);
  }

  private static Records.OperationId operation(int value) {
    byte[] bytes = new byte[16];
    bytes[14] = (byte) (value >>> 8);
    bytes[15] = (byte) value;
    return new Records.OperationId(bytes);
  }

  private static SessionStore.Access access(String owner) {
    return new SessionStore.Access(owner, () -> {});
  }

  private static long scalar(Path database, SessionStore.Configuration configuration, String sql)
      throws Exception {
    try (var connection = BoundedSqlite.open(database, configuration.files()).connect();
        var statement = connection.createStatement();
        var rows = statement.executeQuery(sql)) {
      assertTrue(rows.next());
      return rows.getLong(1);
    }
  }

  private static void assertCode(ProtocolError.Code expected, Throwing action) {
    ProtocolError error = assertThrows(ProtocolError.class, action::run);
    assertEquals(expected, error.code(), error::getMessage);
  }

  private record Harness(
      Path database,
      Path inputsPath,
      SessionStore sessions,
      InputStore inputs,
      SessionStore.Configuration configuration) {}

  private record Session(String owner, long generation, Messages.Binding binding) {}

  @FunctionalInterface
  private interface Throwing {
    void run() throws Exception;
  }
}
