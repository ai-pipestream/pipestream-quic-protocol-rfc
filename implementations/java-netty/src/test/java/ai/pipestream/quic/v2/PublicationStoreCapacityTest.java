package ai.pipestream.quic.v2;

import static ai.pipestream.quic.v2.Messages.DURABLE_WORK;
import static ai.pipestream.quic.v2.Messages.RESULT_DELIVERY;
import static org.junit.jupiter.api.Assertions.*;

import ai.pipestream.quic.BoundedSqlite;
import java.io.InputStream;
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
final class PublicationStoreCapacityTest {
  private static final int OUTPUTS = 16;
  private static final Messages.Capabilities SELECTED =
      new Messages.Capabilities(
          true,
          List.of(DURABLE_WORK, RESULT_DELIVERY),
          List.of(),
          1 << 20,
          8,
          32,
          1 << 20,
          1000,
          5000);
  private static final InputStore.Limits INPUT_LIMITS =
      new InputStore.Limits(8L << 20, 128, 1 << 20, 8);
  private static final BoundedSqlite.Limits FILES =
      new BoundedSqlite.Limits(64L << 20, 2L << 20, 64L << 20, 64L << 10);
  private static final Records.WorkKey WORK = new Records.WorkKey(0, 0, 1);
  private static final AdmissionStore.Authorization ALLOW = (binding, parameters) -> {};
  private static final PublicationStore.Endpoint ENDPOINT =
      new PublicationStore.Endpoint("results.example:7443");

  @TempDir Path directory;

  @Test
  void fundedSuccessCommitsLargeManifestAfterOrdinaryRenewalsSaturatePinnedWal() throws Exception {
    Path database = directory.resolve("publication-wal.sqlite");
    Path inputsPath = directory.resolve("publication-wal-inputs");
    SessionStore sessions = SessionStore.initialize(database, configuration());
    sessions.create(
        sessionAccess(),
        SELECTED,
        new Messages.Create(1, 1, new Records.Policy(10_000, 20_000, 30_000)));
    sessions.declare(
        sessionAccess(), SELECTED, 1, new Messages.Declare(2, operation(1), 0, List.of(1L), false));
    InputStore.Usage funded;
    Records.WorkView succeeded;
    try (InputStore inputs =
            InputStore.initializeForAuthority(inputsPath, INPUT_LIMITS, sessions.identity());
        Connection reader = BoundedSqlite.open(database, FILES).connect()) {
      sessions.bindInputs(inputs);
      try (InputStore.Receiver receiver = inputs.begin(context(), header(), SELECTED, 1)) {
        receiver.write(ByteBuffer.allocate(0), 2);
        receiver.finish(3);
      }
      sessions.admit(sessionAccess(), SELECTED, 1, inputs, header(), 2, clock(1000), ALLOW);
      ExecutionStore.Lease lease =
          sessions.claimExecution(execAccess(), 1, WORK, inputs, 900, clock(1100), ALLOW);
      for (int index = 0; index < OUTPUTS; index++) {
        byte[] payload = payload(index);
        try (OutputStore.Writer writer =
            inputs.beginOutput(
                context(),
                header(),
                lease,
                index,
                payload.length,
                "application/octet-stream",
                OUTPUTS)) {
          writer.write(ByteBuffer.wrap(payload));
          assertEquals(digest(payload), writer.finish().sha256());
        }
      }
      funded = inputs.usage();
      long[] creditsBefore = credits(database);

      execute(reader, "BEGIN");
      try (var query = reader.createStatement();
          var rows = query.executeQuery("SELECT image FROM ps_v2_slots WHERE id=1")) {
        assertTrue(rows.next());
        assertTrue(rows.getBytes(1).length > 0);
      }
      long beforeWal = sidecar(database);
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
      assertNotNull(refusal, "ordinary renewals did not exhaust the pinned WAL");
      assertEquals(ProtocolError.Code.LIMIT_EXCEEDED, refusal.code(), refusal::getMessage);
      SQLException sqlite = assertInstanceOf(SQLException.class, refusal.getCause());
      assertEquals(13, sqlite.getErrorCode() & 255, sqlite::toString);
      long saturatedWal = sidecar(database);
      assertTrue(saturatedWal > beforeWal);

      succeeded =
          sessions.succeedExecution(
              execAccess(), lease, inputs, OUTPUTS, ENDPOINT, clock(1200), ALLOW);
      assertEquals(Records.State.SUCCEEDED, succeeded.state());
      Records.Manifest manifest = succeeded.manifest();
      assertNotNull(manifest);
      assertEquals(OUTPUTS, manifest.outputs().size());
      long payloadBytes = 0;
      for (int index = 0; index < OUTPUTS; index++) {
        Records.Output output = manifest.outputs().get(index);
        byte[] payload = payload(index);
        assertEquals(index, output.index());
        assertEquals(payload.length, output.length());
        assertEquals(digest(payload), output.sha256());
        payloadBytes += output.length();
      }
      assertEquals(OUTPUTS, payloadBytes);
      int manifestEncodedBytes = Wire.encodeRecord(manifest, Wire.MAX_CONTROL_LIMIT).length;
      assertEquals(funded, inputs.usage());
      long[] creditsAfter = credits(database);
      assertEquals(creditsBefore[0] - 1, creditsAfter[0]);
      assertEquals(creditsBefore[1] - 1, creditsAfter[1]);
      long afterWal = sidecar(database);
      assertTrue(afterWal > saturatedWal);
      assertTrue(afterWal <= FILES.walBytes());
      System.out.printf(
          "publication WAL renewals=%d before=%d saturated=%d after=%d payloadBytes=%d"
              + " manifestEncodedBytes=%d%n",
          renewals, beforeWal, saturatedWal, afterWal, payloadBytes, manifestEncodedBytes);
      execute(reader, "ROLLBACK");
    }

    SessionStore reopened = SessionStore.open(database, configuration());
    try (InputStore inputs = InputStore.open(inputsPath, INPUT_LIMITS)) {
      reopened.verifyInputs(inputs);
      Records.WorkView retained =
          reopened.snapshot(sessionAccess(), SELECTED, 1, new Messages.Watch(8, WORK, 0, 0)).work();
      assertEquals(succeeded, retained);
      assertEquals(funded, inputs.usage());
      ExecutionStore.Lease locator =
          new ExecutionStore.Lease(reopened.identity(), "alice", 1, WORK, 1, 1, 1);
      for (int index = 0; index < OUTPUTS; index++) {
        OutputStore.Stored output =
            inputs.findOutput(context(), header(), locator, index).orElseThrow();
        try (InputStream stream = output.openStream()) {
          assertArrayEquals(payload(index), stream.readAllBytes());
        }
      }
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
            new Records.Input(0, digest(new byte[0]), "application/octet-stream"),
            "copy",
            0,
            1000,
            new Records.OutputBudget(OUTPUTS, OUTPUTS)));
  }

  private static byte[] payload(int index) {
    return new byte[] {(byte) (index + 1)};
  }

  private static Records.Digest digest(byte[] payload) throws Exception {
    return new Records.Digest(MessageDigest.getInstance("SHA-256").digest(payload));
  }

  private static void execute(Connection connection, String sql) throws Exception {
    try (var statement = connection.createStatement()) {
      statement.execute(sql);
    }
  }

  private static long sidecar(Path database) throws Exception {
    Path wal = database.resolveSibling(database.getFileName() + "-wal");
    return Files.exists(wal) ? Files.size(wal) : 0;
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
