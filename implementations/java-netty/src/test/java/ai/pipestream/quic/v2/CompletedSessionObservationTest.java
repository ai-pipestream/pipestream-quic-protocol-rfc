package ai.pipestream.quic.v2;

import static org.junit.jupiter.api.Assertions.*;

import ai.pipestream.quic.BoundedSqlite;
import java.nio.file.Path;
import java.sql.Connection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

@Timeout(20)
final class CompletedSessionObservationTest {
  @TempDir Path directory;

  @Test
  void exactCommittedRootIsCorrelatedAndStableAcrossRecovery() throws Exception {
    {
      Fixture fixture = new Fixture("exact");
      Records.ScopeSummary root = fixture.closeRoot();
      Messages.Complete request = new Messages.Complete(10, fixture.generation(), root);
      assertEquals(
          new Messages.Completed(10, fixture.generation(), root), fixture.completed(request));
      fixture.reopen();
      assertEquals(
          new Messages.Completed(11, fixture.generation(), root),
          fixture.completed(new Messages.Complete(11, fixture.generation(), root)));
    }
  }

  @Test
  void unclosedRootIsNotReadyAndChangedGenerationOrSummaryConflicts() throws Exception {
    {
      Fixture fixture = new Fixture("refusals");
      Records.Digest seal = seal(fixture.binding, List.of());
      Records.ScopeSummary claimed = summary(seal, Commitments.emptyStatus(), 1);
      assertCode(
          ProtocolError.Code.NOT_READY,
          () -> fixture.completed(new Messages.Complete(2, fixture.generation(), claimed)));
      assertCode(
          ProtocolError.Code.CONFLICT,
          () -> fixture.completed(new Messages.Complete(3, fixture.generation() + 1, claimed)));

      fixture.sessions.declare(
          access(),
          ResultFixture.SELECTED,
          fixture.generation(),
          new Messages.Declare(4, ResultFixture.operation(2), 0, List.of(), true));
      Records.ScopeSummary root = fixture.close(seal);
      for (Records.ScopeSummary changed :
          List.of(
              summary(ResultFixture.digest(new byte[] {1}), root.statusRoot(), root.closedAt()),
              new Records.ScopeSummary(
                  0,
                  0,
                  null,
                  root.seal(),
                  1,
                  new Records.Counts(1, 0, 0, 0),
                  ResultFixture.digest(new byte[] {3}),
                  root.closedAt()),
              summary(root.seal(), ResultFixture.digest(new byte[] {2}), root.closedAt()),
              summary(root.seal(), root.statusRoot(), root.closedAt() + 1))) {
        assertCode(
            ProtocolError.Code.CONFLICT,
            () -> fixture.completed(new Messages.Complete(5, fixture.generation(), changed)));
      }
      assertEquals(
          root, fixture.completed(new Messages.Complete(6, fixture.generation(), root)).root());
    }
  }

  @Test
  void authorizationPrecedesGenerationAndIsRecheckedAndRevocationRefuses() throws Exception {
    {
      Fixture fixture = new Fixture("authorization");
      Records.ScopeSummary root = fixture.closeRoot();
      SessionStore.Access denied =
          new SessionStore.Access(
              "alice",
              () -> {
                throw new ProtocolError(ProtocolError.Code.UNAUTHORIZED, "denied completion");
              });
      assertCode(
          ProtocolError.Code.UNAUTHORIZED,
          () ->
              fixture.sessions.completed(
                  denied,
                  ResultFixture.SELECTED,
                  Long.MAX_VALUE,
                  new Messages.Complete(20, Long.MAX_VALUE, root)));

      AtomicInteger checks = new AtomicInteger();
      SessionStore.Access changing =
          new SessionStore.Access(
              "alice",
              () -> {
                if (checks.incrementAndGet() == 3)
                  throw new ProtocolError(ProtocolError.Code.UNAUTHORIZED, "credential changed");
              });
      assertCode(
          ProtocolError.Code.UNAUTHORIZED,
          () ->
              fixture.sessions.completed(
                  changing,
                  ResultFixture.SELECTED,
                  fixture.generation(),
                  new Messages.Complete(21, fixture.generation(), root)));
      assertEquals(3, checks.get());
      fixture.sessions.revoke(access(), fixture.generation(), ResultFixture.clock(1100));
      assertCode(
          ProtocolError.Code.UNAUTHORIZED,
          () -> fixture.completed(new Messages.Complete(22, fixture.generation(), root)));
    }
  }

  @Test
  void structurallyValidChildCutRoundTripsButStoreRejectsItAfterAuthorization() throws Exception {
    {
      Fixture fixture = new Fixture("child-cut");
      Records.Digest digest = ResultFixture.digest(new byte[] {3});
      Records.ScopeSummary child =
          new Records.ScopeSummary(
              1,
              0,
              new Records.WorkKey(0, 0, 1),
              digest,
              0,
              new Records.Counts(0, 0, 0, 0),
              Commitments.emptyStatus(),
              1);
      Messages.Complete request = new Messages.Complete(23, fixture.generation(), child);
      Wire.Frame decoded =
          Wire.decode(
              Wire.encode(request, ResultFixture.SELECTED.controlLimit()),
              ResultFixture.SELECTED.controlLimit());
      assertEquals(request, ((Wire.Known) decoded).message());
      assertCode(ProtocolError.Code.CONFLICT, () -> fixture.completed(request));

      SessionStore.Access denied =
          new SessionStore.Access(
              "alice",
              () -> {
                throw new ProtocolError(ProtocolError.Code.UNAUTHORIZED, "denied child cut");
              });
      assertCode(
          ProtocolError.Code.UNAUTHORIZED,
          () ->
              fixture.sessions.completed(
                  denied, ResultFixture.SELECTED, fixture.generation(), request));
    }
  }

  @Test
  void completedObservationDoesNotExpirePublishedOutput() throws Exception {
    try (ResultFixture fixture = new ResultFixture(directory, "output", new byte[] {4, 5, 6})) {
      fixture.sessions.declare(
          access(),
          ResultFixture.SELECTED,
          fixture.binding.generation(),
          new Messages.Declare(30, ResultFixture.operation(30), 0, List.of(), true));
      Records.Digest seal = seal(fixture.binding, List.of(1L));
      Records.ScopeSummary root = close(fixture.sessions, fixture.binding, seal, 1200);
      InputStore.Usage before = fixture.inputs.usage();
      assertEquals(
          root,
          fixture
              .sessions
              .completed(
                  access(),
                  ResultFixture.SELECTED,
                  fixture.binding.generation(),
                  new Messages.Complete(31, fixture.binding.generation(), root))
              .root());
      assertEquals(before, fixture.inputs.usage());
      OutputStore.Stored stored =
          fixture
              .inputs
              .findOutput(fixture.context(), fixture.inputHeader, fixture.lease, 0)
              .orElseThrow();
      try (var reader = stored.openStream()) {
        assertArrayEquals(fixture.payload, reader.readAllBytes());
      }
    }
  }

  @Test
  void checksummedClosureContradictionIsReportedAsStorageCorruption() throws Exception {
    {
      Fixture fixture = new Fixture("corrupt");
      Records.ScopeSummary root = fixture.closeRoot();
      fixture.corruptSummary(root);
      assertThrows(
          java.sql.SQLException.class,
          () -> fixture.completed(new Messages.Complete(40, fixture.generation(), root)));
    }
  }

  private final class Fixture {
    final Path database;
    final Messages.Binding binding;
    SessionStore sessions;

    Fixture(String name) throws Exception {
      database = directory.resolve(name + ".sqlite");
      sessions = SessionStore.initialize(database, ResultFixture.configuration());
      binding =
          sessions.create(
              access(),
              ResultFixture.SELECTED,
              new Messages.Create(1, 1, new Records.Policy(10_000, 20_000, 30_000)));
    }

    long generation() {
      return binding.generation();
    }

    Records.Digest declareSealed() throws Exception {
      sessions.declare(
          access(),
          ResultFixture.SELECTED,
          generation(),
          new Messages.Declare(1, ResultFixture.operation(1), 0, List.of(), true));
      return seal(binding, List.of());
    }

    Records.ScopeSummary closeRoot() throws Exception {
      return close(declareSealed());
    }

    Records.ScopeSummary close(Records.Digest seal) throws Exception {
      return CompletedSessionObservationTest.close(sessions, binding, seal, 1000);
    }

    Messages.Completed completed(Messages.Complete request) throws Exception {
      return sessions.completed(access(), ResultFixture.SELECTED, generation(), request);
    }

    void reopen() throws Exception {
      sessions = SessionStore.open(database, ResultFixture.configuration());
    }

    void corruptSummary(Records.ScopeSummary root) throws Exception {
      try (Connection connection =
          BoundedSqlite.open(database, ResultFixture.configuration().files()).connect()) {
        execute(connection, "BEGIN IMMEDIATE");
        long slot;
        try (var query =
            connection.prepareStatement(
                "SELECT state_slot FROM ps_v2_scopes WHERE generation=? AND id=0 AND producer=0")) {
          query.setLong(1, generation());
          try (var row = query.executeQuery()) {
            assertTrue(row.next());
            slot = row.getLong(1);
          }
        }
        FixedRecords.Snapshot image =
            FixedRecords.read(
                connection,
                slot,
                FixedRecords.Kind.SCOPE,
                FixedRecords.key(binding, FixedRecords.Kind.SCOPE, 0, 0, 0, null));
        ScopeState state = ScopeState.decode(image.body());
        Records.ScopeSummary contradiction =
            summary(root.seal(), ResultFixture.digest(new byte[] {7}), root.closedAt());
        ScopeState corrupted =
            new ScopeState(
                state.id(),
                state.producer(),
                state.parent(),
                state.declared(),
                state.last(),
                state.seal(),
                state.cancelled(),
                state.revoked(),
                contradiction);
        FixedRecords.replace(
            connection,
            ResultFixture.configuration().files(),
            slot,
            FixedRecords.Kind.SCOPE,
            image.header().key(),
            image.header().revision(),
            corrupted.encode(),
            false);
        execute(connection, "COMMIT");
      }
    }
  }

  private static Records.ScopeSummary close(
      SessionStore sessions, Messages.Binding binding, Records.Digest seal, long utc)
      throws Exception {
    ClosureStore.Cursor cursor = new ClosureStore.Cursor();
    for (int calls = 0; calls < 16; calls++) {
      sessions.reconcileClosures(cursor, 1, ResultFixture.clock(utc));
      try {
        return sessions.scopeSummary(
            access(), ResultFixture.SELECTED, binding.generation(), 0, seal);
      } catch (ProtocolError refusal) {
        if (refusal.code() != ProtocolError.Code.NOT_READY) throw refusal;
      }
    }
    return fail("root did not close within bounded reconciliation calls");
  }

  private static Records.ScopeSummary summary(
      Records.Digest seal, Records.Digest status, long closedAt) {
    return new Records.ScopeSummary(
        0, 0, null, seal, 0, new Records.Counts(0, 0, 0, 0), status, closedAt);
  }

  private static Records.Digest seal(Messages.Binding binding, List<Long> members) {
    Commitments.Seal accumulator =
        new Commitments.Seal(
            new Commitments.Context(binding.authority(), binding.owner(), binding.generation()),
            0,
            0,
            null,
            members.size());
    for (long member : members) accumulator.add(member);
    return accumulator.finish();
  }

  private static SessionStore.Access access() {
    return ResultFixture.sessionAccess("alice");
  }

  private static void assertCode(ProtocolError.Code expected, Throwing action) {
    ProtocolError refusal = assertThrows(ProtocolError.class, action::run);
    assertEquals(expected, refusal.code(), refusal::getMessage);
  }

  private static void execute(Connection connection, String sql) throws Exception {
    try (var statement = connection.createStatement()) {
      statement.execute(sql);
    }
  }

  @FunctionalInterface
  private interface Throwing {
    void run() throws Exception;
  }
}
