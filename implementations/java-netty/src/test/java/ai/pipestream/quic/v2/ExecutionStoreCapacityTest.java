package ai.pipestream.quic.v2;

import static ai.pipestream.quic.v2.Messages.DURABLE_WORK;
import static org.junit.jupiter.api.Assertions.*;

import ai.pipestream.quic.BoundedSqlite;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

@Timeout(30)
final class ExecutionStoreCapacityTest {
  private static final Messages.Capabilities SELECTED =
      new Messages.Capabilities(
          true, List.of(DURABLE_WORK), List.of(), 1 << 20, 8, 16, 1 << 20, 1000, 5000);
  private static final InputStore.Limits INPUT_LIMITS =
      new InputStore.Limits(4L << 20, 32, 1 << 20, 8);
  private static final Records.WorkKey WORK = new Records.WorkKey(0, 0, 1);
  private static final AdmissionStore.Authorization ALLOW = (binding, parameters) -> {};
  private static final BoundedSqlite.Limits FILES =
      new BoundedSqlite.Limits(64L << 20, 2L << 20, 64L << 20, 64L << 10);

  @TempDir Path directory;

  @Test
  void fundedFailureCommitsAfterOrdinaryRenewalsSaturatePinnedWal() throws Exception {
    Path database = directory.resolve("execution-wal.sqlite");
    Path inputsPath = directory.resolve("execution-wal-inputs");
    SessionStore sessions = SessionStore.initialize(database, configuration());
    Messages.Binding binding =
        sessions.create(
            sessionAccess(),
            SELECTED,
            new Messages.Create(1, 1, new Records.Policy(10_000, 20_000, 30_000)));
    sessions.declare(
        sessionAccess(), SELECTED, 1, new Messages.Declare(2, operation(1), 0, List.of(1L), false));
    try (InputStore inputs =
            InputStore.initializeForAuthority(inputsPath, INPUT_LIMITS, sessions.identity());
        Connection reader = BoundedSqlite.open(database, FILES).connect()) {
      sessions.bindInputs(inputs);
      Records.InputHeader header = header();
      try (InputStore.Receiver receiver = inputs.begin(context(), header, SELECTED, 1)) {
        receiver.write(ByteBuffer.allocate(0), 2);
        receiver.finish(3);
      }
      sessions.admit(sessionAccess(), SELECTED, 1, inputs, header, 2, clock(1000), ALLOW);
      ExecutionStore.Lease lease =
          sessions.claimExecution(execAccess(), 1, WORK, inputs, 500, clock(1100), ALLOW);
      execute(reader, "BEGIN");
      try (var query = reader.createStatement();
          var rows = query.executeQuery("SELECT image FROM ps_v2_slots WHERE id=1")) {
        assertTrue(rows.next());
        assertTrue(rows.getBytes(1).length > 0);
      }
      long beforeWal = size(database.resolveSibling(database.getFileName() + "-wal"));
      int renewals = 0;
      ProtocolError refusal = null;
      for (int attempt = 0; attempt < 10_000; attempt++) {
        try {
          lease = sessions.renewExecution(execAccess(), lease, 800, clock(1200), ALLOW);
          renewals++;
        } catch (ProtocolError error) {
          refusal = error;
          break;
        }
      }
      assertTrue(renewals > 0);
      assertNotNull(refusal, "ordinary execution renewals did not exhaust bounded WAL");
      assertEquals(ProtocolError.Code.LIMIT_EXCEEDED, refusal.code(), refusal::getMessage);
      SQLException sqlite = assertInstanceOf(SQLException.class, refusal.getCause());
      assertEquals(13, sqlite.getErrorCode() & 255, sqlite::toString);
      long saturatedWal = size(database.resolveSibling(database.getFileName() + "-wal"));
      assertTrue(saturatedWal > beforeWal);
      long[] creditsBeforeFailure = credits(database);

      Records.WorkView failed =
          sessions.failExecution(
              execAccess(),
              lease,
              new Records.Diagnostic(9, "bounded failure"),
              false,
              clock(1200),
              ALLOW);
      assertEquals(Records.State.FAILED, failed.state());
      assertNull(failed.manifest());
      JobRecord retained = job(database, binding);
      assertTrue(retained.inputLive());
      assertTrue(retained.outputsLive());
      assertFalse(retained.executorLive());
      assertTrue(inputs.find(context(), header).isPresent());
      assertTrue(inputs.findReservation(context(), header).isPresent());
      long[] creditsAfterFailure = credits(database);
      assertEquals(creditsBeforeFailure[0] - 1, creditsAfterFailure[0]);
      assertEquals(creditsBeforeFailure[1] - 1, creditsAfterFailure[1]);
      long afterSettlement = size(database.resolveSibling(database.getFileName() + "-wal"));
      assertTrue(afterSettlement > saturatedWal);
      assertTrue(afterSettlement <= FILES.walBytes());
      System.out.printf(
          "execution WAL renewals=%d before=%d saturated=%d afterSettlement=%d%n",
          renewals, beforeWal, saturatedWal, afterSettlement);
      execute(reader, "ROLLBACK");
    }

    SessionStore reopened = SessionStore.open(database, configuration());
    try (InputStore inputs = InputStore.open(inputsPath, INPUT_LIMITS)) {
      reopened.verifyInputs(inputs);
      Records.WorkView view =
          reopened.snapshot(sessionAccess(), SELECTED, 1, new Messages.Watch(8, WORK, 0, 0)).work();
      assertEquals(Records.State.FAILED, view.state());
      assertEquals(new Records.Diagnostic(9, "bounded failure"), view.diagnostic());
      assertTrue(inputs.find(context(), header()).isPresent());
    }
  }

  private static JobRecord job(Path database, Messages.Binding binding) throws Exception {
    try (Connection connection = BoundedSqlite.open(database, FILES).connect()) {
      return AdmissionStore.job(connection, binding, WORK).record();
    }
  }

  private static long[] credits(Path database) throws Exception {
    try (var connection = BoundedSqlite.open(database, FILES).connect();
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

  private static SessionStore.Configuration configuration() {
    return new SessionStore.Configuration(
        "issuer-a",
        new Records.Limits(8, 16, 16, 1 << 20, 1 << 20, 4),
        new Records.Policy(60_000, 60_000, 60_000),
        2,
        8,
        4,
        FILES,
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

  private static void execute(Connection connection, String sql) throws Exception {
    try (var statement = connection.createStatement()) {
      statement.execute(sql);
    }
  }

  private static long size(Path path) throws Exception {
    return Files.exists(path) ? Files.size(path) : 0;
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
}
