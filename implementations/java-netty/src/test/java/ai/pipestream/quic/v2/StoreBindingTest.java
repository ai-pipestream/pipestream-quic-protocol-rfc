package ai.pipestream.quic.v2;

import static ai.pipestream.quic.v2.Messages.DURABLE_WORK;
import static org.junit.jupiter.api.Assertions.*;

import ai.pipestream.quic.BoundedSqlite;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.sql.SQLException;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

final class StoreBindingTest {
  private static final InputStore.Limits INPUT_LIMITS =
      new InputStore.Limits(1 << 20, 8, 1 << 16, 2);
  private static final Records.Policy POLICY = new Records.Policy(1000, 1000, 1000);
  private static final Messages.Capabilities SELECTED =
      new Messages.Capabilities(
          true, List.of(DURABLE_WORK), List.of(), 65536, 4, 16, 1 << 20, 1000, 5000);

  @TempDir Path directory;

  @Test
  void matchingPairAndInstalledBytesRemainStableAfterBothStoresReopen() throws Exception {
    Path database = directory.resolve("paired.sqlite");
    Path inputs = directory.resolve("paired-inputs");
    SessionStore sessions = SessionStore.initialize(database, configuration());
    Messages.Binding binding =
        sessions.create(access(), SELECTED, new Messages.Create(1, 1, POLICY));
    byte[] payload = pattern(257, 3);
    Records.InputHeader header = header(payload);
    try (InputStore store =
        InputStore.initializeForAuthority(inputs, INPUT_LIMITS, sessions.identity())) {
      sessions.bindInputs(store);
      sessions.verifyInputs(store);
      install(store, context(binding), header, payload);
    }

    SessionStore reopenedSessions = SessionStore.open(database, configuration());
    try (InputStore reopenedInputs = InputStore.open(inputs, INPUT_LIMITS)) {
      assertEquals(reopenedSessions.identity(), reopenedInputs.authorityIdentity().orElseThrow());
      reopenedSessions.verifyInputs(reopenedInputs);
      assertArrayEquals(payload, read(reopenedInputs.find(context(binding), header).orElseThrow()));
    }
  }

  @Test
  void verificationBeforeBindingRefusesButExactSetupCanFinishAfterReopen() throws Exception {
    Path database = directory.resolve("unbound.sqlite");
    Path inputs = directory.resolve("unbound-inputs");
    SessionStore sessions = SessionStore.initialize(database, configuration());
    UUID identity = sessions.identity();
    try (InputStore store = InputStore.initializeForAuthority(inputs, INPUT_LIMITS, identity)) {
      assertThrows(SQLException.class, () -> sessions.verifyInputs(store));
    }

    SessionStore reopenedSessions = SessionStore.open(database, configuration());
    try (InputStore reopenedInputs = InputStore.open(inputs, INPUT_LIMITS)) {
      assertEquals(identity, reopenedSessions.identity());
      reopenedSessions.bindInputs(reopenedInputs);
      reopenedSessions.verifyInputs(reopenedInputs);
    }
  }

  @Test
  void standaloneForeignAndClosedInputStoresRefuseBeforeDatabaseInspection() throws Exception {
    SessionStore sessions =
        SessionStore.initialize(directory.resolve("authority.sqlite"), configuration());
    try (InputStore standalone =
        InputStore.initialize(directory.resolve("standalone"), INPUT_LIMITS)) {
      assertTrue(standalone.authorityIdentity().isEmpty());
      assertThrows(IOException.class, () -> sessions.bindInputs(standalone));
      assertThrows(IOException.class, () -> sessions.verifyInputs(standalone));
    }

    SessionStore foreignSessions =
        SessionStore.initialize(directory.resolve("foreign.sqlite"), configuration());
    try (InputStore foreign =
        InputStore.initializeForAuthority(
            directory.resolve("foreign-inputs"), INPUT_LIMITS, foreignSessions.identity())) {
      assertThrows(IOException.class, () -> sessions.bindInputs(foreign));
      assertThrows(IOException.class, () -> sessions.verifyInputs(foreign));
    }

    InputStore closed =
        InputStore.initializeForAuthority(
            directory.resolve("closed-inputs"), INPUT_LIMITS, sessions.identity());
    closed.close();
    assertThrows(IOException.class, () -> sessions.bindInputs(closed));
    assertThrows(IOException.class, () -> sessions.verifyInputs(closed));
  }

  @Test
  void committedDatabasePairCannotBeReplacedByAnotherSameAuthorityRoot() throws Exception {
    SessionStore sessions =
        SessionStore.initialize(directory.resolve("fixed-pair.sqlite"), configuration());
    try (InputStore first =
            InputStore.initializeForAuthority(
                directory.resolve("first"), INPUT_LIMITS, sessions.identity());
        InputStore second =
            InputStore.initializeForAuthority(
                directory.resolve("second"), INPUT_LIMITS, sessions.identity())) {
      sessions.bindInputs(first);
      sessions.bindInputs(first);
      sessions.verifyInputs(first);
      assertThrows(SQLException.class, () -> sessions.bindInputs(second));
      assertThrows(SQLException.class, () -> sessions.verifyInputs(second));
      sessions.verifyInputs(first);
      assertEquals(0, second.usage().files());
    }
  }

  @Test
  void independentDatabaseAndRootPairsCannotCrossAndOriginalPairsRemainUsable() throws Exception {
    SessionStore firstSessions =
        SessionStore.initialize(directory.resolve("one.sqlite"), configuration());
    SessionStore secondSessions =
        SessionStore.initialize(directory.resolve("two.sqlite"), configuration());
    try (InputStore firstInputs =
            InputStore.initializeForAuthority(
                directory.resolve("one-inputs"), INPUT_LIMITS, firstSessions.identity());
        InputStore secondInputs =
            InputStore.initializeForAuthority(
                directory.resolve("two-inputs"), INPUT_LIMITS, secondSessions.identity())) {
      firstSessions.bindInputs(firstInputs);
      secondSessions.bindInputs(secondInputs);
      assertThrows(IOException.class, () -> firstSessions.bindInputs(secondInputs));
      assertThrows(IOException.class, () -> firstSessions.verifyInputs(secondInputs));
      assertThrows(IOException.class, () -> secondSessions.bindInputs(firstInputs));
      assertThrows(IOException.class, () -> secondSessions.verifyInputs(firstInputs));
      firstSessions.verifyInputs(firstInputs);
      secondSessions.verifyInputs(secondInputs);
    }
  }

  @Test
  void bindingCreatesNoSessionOperationMembershipOrInputObject() throws Exception {
    Path database = directory.resolve("empty.sqlite");
    SessionStore sessions = SessionStore.initialize(database, configuration());
    try (InputStore inputs =
            InputStore.initializeForAuthority(
                directory.resolve("empty-inputs"), INPUT_LIMITS, sessions.identity());
        var connection = BoundedSqlite.open(database, configuration().files()).connect()) {
      sessions.bindInputs(inputs);
      sessions.verifyInputs(inputs);
      assertEquals(0, scalar(connection, "SELECT count(*) FROM ps_v2_sessions"));
      assertEquals(0, scalar(connection, "SELECT count(*) FROM ps_v2_operations"));
      assertEquals(0, scalar(connection, "SELECT count(*) FROM ps_v2_entities"));
      assertEquals(new InputStore.Usage(0, 0, 0), inputs.usage());
      assertEquals(
          1,
          sessions
              .nextSequence(access(), SELECTED, new Messages.NextSequence(1))
              .nextCreationSequence());
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

  private static SessionStore.Access access() {
    return new SessionStore.Access("alice", () -> {});
  }

  private static Commitments.Context context(Messages.Binding binding) {
    return new Commitments.Context(binding.authority(), binding.owner(), binding.generation());
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
            "binding",
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

  private static long scalar(java.sql.Connection connection, String sql) throws SQLException {
    try (var statement = connection.createStatement();
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
