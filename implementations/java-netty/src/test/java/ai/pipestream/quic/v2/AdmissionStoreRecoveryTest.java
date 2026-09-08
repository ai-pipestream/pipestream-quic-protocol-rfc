package ai.pipestream.quic.v2;

import static ai.pipestream.quic.v2.Messages.DURABLE_WORK;
import static ai.pipestream.quic.v2.Messages.RESULT_DELIVERY;
import static org.junit.jupiter.api.Assertions.*;

import ai.pipestream.quic.BoundedSqlite;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

@Timeout(30)
final class AdmissionStoreRecoveryTest {
  private static final Messages.Capabilities SELECTED =
      new Messages.Capabilities(
          true,
          List.of(DURABLE_WORK, RESULT_DELIVERY),
          List.of(),
          65536,
          4,
          16,
          1 << 20,
          1000,
          5000);
  private static final InputStore.Limits INPUT_LIMITS =
      new InputStore.Limits(4L << 20, 32, 1024, 4);
  private static final AdmissionStore.Clock CLOCK = () -> new AdmissionStore.Time(1000, true);
  private static final AdmissionStore.Authorization ALLOW = (binding, parameters) -> {};

  @TempDir Path directory;

  @Test
  void processDeathBeforeCommitOrAfterReturnRecoversAdmissionAtomically() throws Exception {
    for (String phase : List.of("before", "after")) {
      Path database = directory.resolve(phase + ".sqlite");
      Path inputsPath = directory.resolve(phase + "-inputs");
      prepare(database, inputsPath);
      int expectedExit = phase.equals("before") ? 51 : 52;
      assertEquals(
          expectedExit,
          runChild(phase, database, inputsPath),
          Files.readString(directory.resolve(phase + ".err")));

      SessionStore sessions = SessionStore.open(database, configuration());
      try (InputStore inputs = InputStore.open(inputsPath, INPUT_LIMITS)) {
        sessions.verifyInputs(inputs);
        assertTrue(inputs.findReservation(context(), header()).isPresent());
        if (phase.equals("before")) {
          assertEquals(0, scalar(database, "SELECT count(*) FROM ps_v2_jobs"));
          assertEquals(1, scalar(database, "SELECT count(*) FROM ps_v2_scopes"));
          assertEquals(1, scalar(database, "SELECT operation_count FROM ps_v2_sessions"));
          assertEquals(1, clockRevision(database));
          assertEquals(0, clockUtc(database));
          assertLookupMissing(sessions);
          assertEquals(Records.State.DECLARED, view(sessions).state());
        }

        Messages.AdmissionResponse replay = admit(sessions, inputs, 9, ALLOW);
        Records.OperationReceipt retained = lookup(sessions);
        assertEquals(retained, replay.receipt());
        assertEquals(1, scalar(database, "SELECT count(*) FROM ps_v2_jobs"));
        assertEquals(2, scalar(database, "SELECT count(*) FROM ps_v2_scopes"));
        assertEquals(2, scalar(database, "SELECT operation_count FROM ps_v2_sessions"));
        assertEquals(2, clockRevision(database));
        assertEquals(1000, clockUtc(database));
        assertEquals(Records.State.WAITING_CHILDREN, view(sessions).state());
        assertNotNull(view(sessions).child());
        assertEquals(retained, admit(sessions, inputs, 10, ALLOW).receipt());
        assertEquals(1, scalar(database, "SELECT count(*) FROM ps_v2_jobs"));
        assertEquals(2, scalar(database, "SELECT count(*) FROM ps_v2_scopes"));
      }
    }
  }

  public static void main(String[] args) throws Exception {
    String phase = args[0];
    SessionStore sessions = SessionStore.open(Path.of(args[1]), configuration());
    InputStore inputs = InputStore.open(Path.of(args[2]), INPUT_LIMITS);
    AdmissionStore.Authorization authorization = ALLOW;
    if (phase.equals("before")) {
      AtomicInteger checks = new AtomicInteger();
      authorization =
          (binding, parameters) -> {
            if (checks.incrementAndGet() == 3) Runtime.getRuntime().halt(51);
          };
    }
    admit(sessions, inputs, 2, authorization);
    Runtime.getRuntime().halt(52);
  }

  private static void prepare(Path database, Path inputsPath) throws Exception {
    SessionStore sessions = SessionStore.initialize(database, configuration());
    sessions.create(
        access(), SELECTED, new Messages.Create(1, 1, new Records.Policy(10_000, 20_000, 30_000)));
    sessions.declare(
        access(), SELECTED, 1, new Messages.Declare(2, operation(1), 0, List.of(1L), false));
    try (InputStore inputs =
        InputStore.initializeForAuthority(inputsPath, INPUT_LIMITS, sessions.identity())) {
      sessions.bindInputs(inputs);
      try (InputStore.Receiver receiver = inputs.begin(context(), header(), SELECTED, 1)) {
        receiver.write(ByteBuffer.allocate(0), 2);
        assertEquals(header(), receiver.finish(3).header());
      }
    }
  }

  private int runChild(String phase, Path database, Path inputs) throws Exception {
    Process process =
        new ProcessBuilder(
                Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-cp",
                System.getProperty("java.class.path"),
                AdmissionStoreRecoveryTest.class.getName(),
                phase,
                database.toString(),
                inputs.toString())
            .redirectOutput(directory.resolve(phase + ".out").toFile())
            .redirectError(directory.resolve(phase + ".err").toFile())
            .start();
    try {
      assertTrue(process.waitFor(8, TimeUnit.SECONDS), "admission child did not exit");
      return process.exitValue();
    } finally {
      if (process.isAlive()) {
        process.destroyForcibly();
        assertTrue(process.waitFor(2, TimeUnit.SECONDS));
      }
    }
  }

  private static Messages.AdmissionResponse admit(
      SessionStore sessions,
      InputStore inputs,
      long request,
      AdmissionStore.Authorization authorization)
      throws Exception {
    return sessions.admit(access(), SELECTED, 1, inputs, header(), request, CLOCK, authorization);
  }

  private static Records.OperationReceipt lookup(SessionStore sessions) throws Exception {
    return sessions
        .lookupOperation(access(), SELECTED, 1, new Messages.LookupOperation(7, operation(2)))
        .receipt();
  }

  private static void assertLookupMissing(SessionStore sessions) {
    ProtocolError error =
        assertThrows(
            ProtocolError.class,
            () ->
                sessions.lookupOperation(
                    access(), SELECTED, 1, new Messages.LookupOperation(7, operation(2))));
    assertEquals(ProtocolError.Code.NOT_FOUND, error.code(), error::getMessage);
  }

  private static Records.WorkView view(SessionStore sessions) throws Exception {
    return sessions
        .snapshot(access(), SELECTED, 1, new Messages.Watch(8, new Records.WorkKey(0, 0, 1), 0, 0))
        .work();
  }

  private static long clockRevision(Path database) throws Exception {
    try (var connection = BoundedSqlite.open(database, configuration().files()).connect();
        var statement = connection.createStatement();
        var rows = statement.executeQuery("SELECT clock_slot FROM ps_v2_meta WHERE singleton=1")) {
      assertTrue(rows.next());
      return FixedRecords.header(connection, rows.getLong(1), FixedRecords.Kind.CLOCK).revision();
    }
  }

  private static long clockUtc(Path database) throws Exception {
    try (var connection = BoundedSqlite.open(database, configuration().files()).connect()) {
      FixedRecords.Snapshot clock =
          FixedRecords.read(
              connection,
              FixedRecords.CLOCK,
              FixedRecords.Kind.CLOCK,
              FixedRecords.clockKey("issuer-a"));
      return new Cbor.Reader(clock.body(), FixedRecords.CLOCK_CAPACITY).number();
    }
  }

  private static long scalar(Path database, String sql) throws Exception {
    try (var connection = BoundedSqlite.open(database, configuration().files()).connect();
        var statement = connection.createStatement();
        var rows = statement.executeQuery(sql)) {
      assertTrue(rows.next());
      return rows.getLong(1);
    }
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
                    "copy", Set.of(1), AdmissionStore.RestartSafety.IDEMPOTENT)),
            4,
            4));
  }

  private static Commitments.Context context() {
    return new Commitments.Context("issuer-a", "alice", 1);
  }

  private static Records.InputHeader header() throws Exception {
    return new Records.InputHeader(
        1,
        operation(2),
        new Records.AdmitParameters(
            new Records.WorkKey(0, 0, 1),
            new Records.Input(
                0,
                new Records.Digest(MessageDigest.getInstance("SHA-256").digest(new byte[0])),
                "application/octet-stream"),
            "copy",
            1,
            1000,
            new Records.OutputBudget(1, 17)));
  }

  private static Records.OperationId operation(int value) {
    byte[] bytes = new byte[16];
    bytes[15] = (byte) value;
    return new Records.OperationId(bytes);
  }

  private static SessionStore.Access access() {
    return new SessionStore.Access("alice", () -> {});
  }
}
