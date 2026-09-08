package ai.pipestream.quic.v2;

import static org.junit.jupiter.api.Assertions.*;

import ai.pipestream.quic.BoundedSqlite;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

@Timeout(30)
final class RetirementIntegrityTest {
  @TempDir Path directory;

  @Test
  void startedSessionRequiresMatchingRetirementFlagAndProofLink() throws Exception {
    for (String mutation : List.of("missing-link", "missing-flag")) {
      try (Fixture fixture = new Fixture(directory, mutation, true)) {
        fixture.startRetirement();
        fixture.mutate(
            mutation.equals("missing-link")
                ? "UPDATE ps_v2_sessions SET retirement_slot=NULL WHERE generation=1"
                : "UPDATE ps_v2_sessions SET retiring=0 WHERE generation=1");
        assertThrows(
            java.sql.SQLException.class,
            () -> SessionStore.open(fixture.database, ResultFixture.configuration()));
      }
    }
  }

  @Test
  void retirementProofCannotSurviveRemovalOfItsRetainedRoot() throws Exception {
    try (Fixture fixture = new Fixture(directory, "missing-root", true)) {
      fixture.startRetirement();
      try (var connection = fixture.connection();
          var statement = connection.createStatement()) {
        statement.execute("BEGIN IMMEDIATE");
        long slot;
        try (var row =
            statement.executeQuery(
                "SELECT state_slot FROM ps_v2_scopes WHERE generation=1 AND id=0")) {
          assertTrue(row.next());
          slot = row.getLong(1);
        }
        assertEquals(1, statement.executeUpdate("DELETE FROM ps_v2_scopes WHERE generation=1"));
        try (var delete = connection.prepareStatement("DELETE FROM ps_v2_slots WHERE id=?")) {
          delete.setLong(1, slot);
          assertEquals(1, delete.executeUpdate());
        }
        statement.execute("COMMIT");
      }
      assertThrows(
          java.sql.SQLException.class,
          () -> SessionStore.open(fixture.database, ResultFixture.configuration()));
    }
  }

  @Test
  void flagsAloneCannotMarkAnOpenSessionAsRetiring() throws Exception {
    try (Fixture fixture = new Fixture(directory, "flags-only", false)) {
      fixture.mutate("UPDATE ps_v2_sessions SET retiring=1 WHERE generation=1");
      assertThrows(
          java.sql.SQLException.class,
          () -> SessionStore.open(fixture.database, ResultFixture.configuration()));
    }
  }

  @Test
  void wrongOwnerAndRevocationDenyBeforeMalformedRetirementProofDecode() throws Exception {
    try (Fixture fixture = new Fixture(directory, "denial-order", true)) {
      fixture.startRetirement();
      fixture.mutate(
          "UPDATE ps_v2_slots SET image=zeroblob(length(image))"
              + " WHERE id=(SELECT retirement_slot FROM ps_v2_sessions WHERE generation=1)");
      assertCode(
          ProtocolError.Code.UNAUTHORIZED,
          () ->
              fixture.sessions.snapshot(
                  ResultFixture.sessionAccess("bob"),
                  ResultFixture.SELECTED,
                  1,
                  new Messages.Watch(20, ResultFixture.WORK, 0, 0)));

      fixture.mutate("UPDATE ps_v2_sessions SET revoked=1 WHERE generation=1");
      assertCode(
          ProtocolError.Code.UNAUTHORIZED,
          () ->
              fixture.sessions.snapshot(
                  ResultFixture.sessionAccess("alice"),
                  ResultFixture.SELECTED,
                  1,
                  new Messages.Watch(21, ResultFixture.WORK, 0, 0)));
    }
  }

  @Test
  void activeReceiverPinsClosedSessionUntilItsSynchronizedAbort() throws Exception {
    try (Fixture fixture = new Fixture(directory, "receiver", true)) {
      Records.InputHeader header = header();
      InputStore.Receiver receiver =
          fixture.inputs.begin(fixture.context(), header, ResultFixture.SELECTED, 0);
      try {
        receiver.write(ByteBuffer.wrap(new byte[] {1}), 1);
        assertEquals(
            RetirementStore.State.PINNED,
            fixture
                .sessions
                .retireSession(1, fixture.inputs, 1, ResultFixture.clock(31_000))
                .state());
        assertTrue(fixture.inputs.sessionHasResources(fixture.context()));
      } finally {
        receiver.close();
      }
      assertFalse(fixture.inputs.sessionHasResources(fixture.context()));
      assertEquals(new InputStore.Usage(0, 0, 0), fixture.inputs.usage());
      assertEquals(
          RetirementStore.State.STARTED,
          fixture
              .sessions
              .retireSession(1, fixture.inputs, 1, ResultFixture.clock(31_000))
              .state());
    }
  }

  private static Records.InputHeader header() throws Exception {
    byte[] payload = {1};
    return new Records.InputHeader(
        1,
        ResultFixture.operation(40),
        new Records.AdmitParameters(
            new Records.WorkKey(0, 0, 40),
            new Records.Input(
                payload.length, ResultFixture.digest(payload), "application/octet-stream"),
            "copy",
            0,
            1000,
            new Records.OutputBudget(0, 0)));
  }

  private static void closeRoot(SessionStore sessions, Messages.Binding binding) throws Exception {
    ClosureStore.Cursor cursor = new ClosureStore.Cursor();
    Commitments.Seal seal =
        new Commitments.Seal(
            new Commitments.Context(binding.authority(), binding.owner(), binding.generation()),
            0,
            0,
            null,
            0);
    Records.Digest expected = seal.finish();
    for (int calls = 0; calls < 16; calls++) {
      sessions.reconcileClosures(cursor, 1, ResultFixture.clock(1000));
      try {
        sessions.scopeSummary(
            ResultFixture.sessionAccess("alice"),
            ResultFixture.SELECTED,
            binding.generation(),
            0,
            expected);
        return;
      } catch (ProtocolError refusal) {
        if (refusal.code() != ProtocolError.Code.NOT_READY) throw refusal;
      }
    }
    fail("root closure did not commit within bounded reconciliation calls");
  }

  private static void assertCode(ProtocolError.Code expected, Throwing action) {
    ProtocolError failure = assertThrows(ProtocolError.class, action::run);
    assertEquals(expected, failure.code(), failure::getMessage);
  }

  private static final class Fixture implements AutoCloseable {
    final Path database;
    final SessionStore sessions;
    final InputStore inputs;
    final Messages.Binding binding;

    Fixture(Path directory, String name, boolean closeRoot) throws Exception {
      database = directory.resolve(name + ".sqlite");
      sessions = SessionStore.initialize(database, ResultFixture.configuration());
      binding =
          sessions.create(
              ResultFixture.sessionAccess("alice"),
              ResultFixture.SELECTED,
              new Messages.Create(1, 1, new Records.Policy(10_000, 20_000, 30_000)));
      inputs =
          InputStore.initializeForAuthority(
              directory.resolve(name + "-inputs"), ResultFixture.INPUT_LIMITS, sessions.identity());
      sessions.bindInputs(inputs);
      if (closeRoot) {
        sessions.declare(
            ResultFixture.sessionAccess("alice"),
            ResultFixture.SELECTED,
            1,
            new Messages.Declare(2, ResultFixture.operation(1), 0, List.of(), true));
        RetirementIntegrityTest.closeRoot(sessions, binding);
      }
    }

    void startRetirement() throws Exception {
      assertEquals(
          RetirementStore.State.STARTED,
          sessions.retireSession(1, inputs, 1, ResultFixture.clock(31_000)).state());
    }

    java.sql.Connection connection() throws Exception {
      return BoundedSqlite.open(database, ResultFixture.configuration().files()).connect();
    }

    void mutate(String sql) throws Exception {
      try (var connection = connection();
          var statement = connection.createStatement()) {
        assertEquals(1, statement.executeUpdate(sql));
      }
    }

    Commitments.Context context() {
      return new Commitments.Context("issuer-a", "alice", 1);
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
