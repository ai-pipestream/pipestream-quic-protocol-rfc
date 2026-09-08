package ai.pipestream.quic.v2;

import static ai.pipestream.quic.v2.Messages.DURABLE_WORK;
import static org.junit.jupiter.api.Assertions.*;

import ai.pipestream.quic.BoundedSqlite;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

@Timeout(15)
final class StoreBindingRecoveryTest {
  private static final InputStore.Limits INPUT_LIMITS =
      new InputStore.Limits(1 << 20, 8, 1 << 16, 2);
  private static final Messages.Capabilities SELECTED =
      new Messages.Capabilities(
          true, List.of(DURABLE_WORK), List.of(), 65536, 4, 16, 1 << 20, 1000, 5000);

  @TempDir Path directory;

  @Test
  void processDeathBeforeAndAfterBindingCommitHasExactRecoveryBoundary() throws Exception {
    for (String phase : List.of("before", "after")) {
      Path database = directory.resolve(phase + ".sqlite");
      Path inputs = directory.resolve(phase + "-inputs");
      int exit = phase.equals("before") ? 41 : 42;
      int actual = runChild(phase, database, inputs);
      assertEquals(exit, actual, Files.readString(directory.resolve(phase + ".err")));

      SessionStore sessions = SessionStore.open(database, configuration());
      try (InputStore store = InputStore.open(inputs, INPUT_LIMITS)) {
        assertEquals(sessions.identity(), store.authorityIdentity().orElseThrow());
        if (phase.equals("before")) {
          assertThrows(SQLException.class, () -> sessions.verifyInputs(store));
          sessions.bindInputs(store);
        }
        sessions.verifyInputs(store);
        sessions.bindInputs(store);
        assertEquals(sessions.identity(), store.authorityIdentity().orElseThrow());
        assertEquals(0, scalar(database, "SELECT count(*) FROM ps_v2_sessions"));
        assertEquals(0, scalar(database, "SELECT count(*) FROM ps_v2_operations"));
        assertEquals(0, scalar(database, "SELECT count(*) FROM ps_v2_entities"));
        assertEquals(new InputStore.Usage(0, 0, 0), store.usage());
      }
    }
  }

  @Test
  void corruptDatabaseInputIdentityIsRejectedWithoutChangingInputBytes() throws Exception {
    Path database = directory.resolve("corrupt-db.sqlite");
    Path inputs = directory.resolve("corrupt-db-inputs");
    SessionStore sessions = SessionStore.initialize(database, configuration());
    byte[] payload = pattern(73, 5);
    Records.InputHeader header = header(payload);
    Commitments.Context context = new Commitments.Context("issuer-a", "alice", 1);
    try (InputStore store =
        InputStore.initializeForAuthority(inputs, INPUT_LIMITS, sessions.identity())) {
      sessions.bindInputs(store);
      install(store, context, header, payload);
      byte[] other = new byte[16];
      other[15] = 9;
      try (var connection = BoundedSqlite.open(database, configuration().files()).connect();
          var update =
              connection.prepareStatement("UPDATE ps_v2_meta SET input_id=? WHERE singleton=1")) {
        update.setBytes(1, other);
        assertEquals(1, update.executeUpdate());
      }
      assertThrows(SQLException.class, () -> sessions.verifyInputs(store));
      assertThrows(SQLException.class, () -> SessionStore.open(database, configuration()));
      assertArrayEquals(payload, read(store.find(context, header).orElseThrow()));
    }
  }

  @Test
  void damagedInputPolicyIsRejectedByCurrentVerificationAndInputRecovery() throws Exception {
    Path database = directory.resolve("corrupt-policy.sqlite");
    Path inputs = directory.resolve("corrupt-policy-inputs");
    SessionStore sessions = SessionStore.initialize(database, configuration());
    try (InputStore store =
        InputStore.initializeForAuthority(inputs, INPUT_LIMITS, sessions.identity())) {
      sessions.bindInputs(store);
      sessions.verifyInputs(store);
      Path policy = inputs.resolve("policy.cbor");
      byte[] damaged = Files.readAllBytes(policy);
      damaged[damaged.length - 1] ^= 1;
      Files.write(policy, damaged);
      assertThrows(IOException.class, () -> sessions.verifyInputs(store));
    }
    assertThrows(IOException.class, () -> InputStore.open(inputs, INPUT_LIMITS));
  }

  public static void main(String[] args) throws Exception {
    String phase = args[0];
    Path database = Path.of(args[1]);
    Path inputs = Path.of(args[2]);
    SessionStore sessions = SessionStore.initialize(database, configuration());
    InputStore store = InputStore.initializeForAuthority(inputs, INPUT_LIMITS, sessions.identity());
    if (store.authorityIdentity().isEmpty()) throw new AssertionError("authority identity missing");
    if (phase.equals("before")) Runtime.getRuntime().halt(41);
    sessions.bindInputs(store);
    Runtime.getRuntime().halt(42);
  }

  private int runChild(String phase, Path database, Path inputs) throws Exception {
    Process process =
        new ProcessBuilder(
                Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-cp",
                System.getProperty("java.class.path"),
                StoreBindingRecoveryTest.class.getName(),
                phase,
                database.toString(),
                inputs.toString())
            .redirectOutput(directory.resolve(phase + ".out").toFile())
            .redirectError(directory.resolve(phase + ".err").toFile())
            .start();
    try {
      assertTrue(process.waitFor(8, TimeUnit.SECONDS), "binding child did not exit");
      return process.exitValue();
    } finally {
      if (process.isAlive()) {
        process.destroyForcibly();
        assertTrue(process.waitFor(2, TimeUnit.SECONDS));
      }
    }
  }

  private static SessionStore.Configuration configuration() {
    return new SessionStore.Configuration(
        "issuer-a",
        new Records.Limits(1, 8, 8, 1 << 20, 1 << 20, 1),
        new Records.Policy(10_000, 10_000, 10_000),
        2,
        8,
        4,
        BoundedSqlite.Limits.defaults());
  }

  private static Records.InputHeader header(byte[] payload) throws Exception {
    byte[] operation = new byte[16];
    operation[15] = 1;
    return new Records.InputHeader(
        1,
        new Records.OperationId(operation),
        new Records.AdmitParameters(
            new Records.WorkKey(0, 0, 1),
            new Records.Input(
                payload.length,
                new Records.Digest(MessageDigest.getInstance("SHA-256").digest(payload)),
                "application/octet-stream"),
            "binding-recovery",
            0,
            1000,
            new Records.OutputBudget(0, 0)));
  }

  private static void install(
      InputStore store, Commitments.Context context, Records.InputHeader header, byte[] payload)
      throws Exception {
    try (InputStore.Receiver receiver = store.begin(context, header, SELECTED, 1)) {
      receiver.write(ByteBuffer.wrap(payload), 2);
      assertArrayEquals(payload, read(receiver.finish(3)));
    }
  }

  private static byte[] read(InputStore.Stored stored) throws IOException {
    try (var input = stored.openStream()) {
      return input.readAllBytes();
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

  private static byte[] pattern(int length, int seed) {
    byte[] bytes = new byte[length];
    for (int index = 0; index < bytes.length; index++) bytes[index] = (byte) (seed + index * 31);
    return bytes;
  }
}
