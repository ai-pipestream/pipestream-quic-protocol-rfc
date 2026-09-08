package ai.pipestream.quic.v2;

import static org.junit.jupiter.api.Assertions.*;

import ai.pipestream.quic.BoundedSqlite;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

@Timeout(30)
final class SessionRetirementTest {
  @TempDir Path directory;

  @Test
  void sealedEmptySessionStartsAtExactCutoffAndFinishesWithoutReusingHistory() throws Exception {
    Path database = directory.resolve("empty.sqlite");
    Path inputPath = directory.resolve("empty-inputs");
    SessionStore sessions = SessionStore.initialize(database, ResultFixture.configuration());
    Messages.Binding binding =
        sessions.create(
            ResultFixture.sessionAccess("alice"),
            ResultFixture.SELECTED,
            new Messages.Create(1, 1, new Records.Policy(10_000, 20_000, 30_000)));
    try (InputStore inputs =
        InputStore.initializeForAuthority(
            inputPath, ResultFixture.INPUT_LIMITS, sessions.identity())) {
      sessions.bindInputs(inputs);
      sessions.declare(
          ResultFixture.sessionAccess("alice"),
          ResultFixture.SELECTED,
          binding.generation(),
          new Messages.Declare(2, ResultFixture.operation(1), 0, List.of(), true));
      closeRoot(sessions, binding, List.of(), 1000);

      for (int invalid : new int[] {0, 257}) {
        assertCode(
            ProtocolError.Code.LIMIT_EXCEEDED,
            () ->
                sessions.retireSession(
                    binding.generation(), inputs, invalid, ResultFixture.clock(31_000)));
      }
      assertCode(
          ProtocolError.Code.CLOCK_UNSAFE,
          () ->
              sessions.retireSession(
                  binding.generation(), inputs, 1, () -> new AdmissionStore.Time(31_000, false)));
      java.util.concurrent.atomic.AtomicInteger samples =
          new java.util.concurrent.atomic.AtomicInteger();
      assertCode(
          ProtocolError.Code.CLOCK_UNSAFE,
          () ->
              sessions.retireSession(
                  binding.generation(),
                  inputs,
                  1,
                  () ->
                      new AdmissionStore.Time(
                          samples.incrementAndGet() == 1 ? 31_000 : 30_999, true)));
      assertEquals(1, sessionCount(database));

      assertEquals(
          RetirementStore.State.NOT_READY,
          sessions
              .retireSession(binding.generation(), inputs, 1, ResultFixture.clock(30_999))
              .state());
      RetirementStore.Progress started =
          sessions.retireSession(binding.generation(), inputs, 1, ResultFixture.clock(31_000));
      assertEquals(
          new RetirementStore.Progress(RetirementStore.State.STARTED, 0, 0, 0, 0), started);
      assertCode(
          ProtocolError.Code.EXPIRED,
          () ->
              sessions.snapshot(
                  ResultFixture.sessionAccess("alice"),
                  ResultFixture.SELECTED,
                  binding.generation(),
                  new Messages.Watch(3, ResultFixture.WORK, 0, 0)));
      assertCode(
          ProtocolError.Code.UNAUTHORIZED,
          () ->
              sessions.snapshot(
                  ResultFixture.sessionAccess("bob"),
                  ResultFixture.SELECTED,
                  binding.generation(),
                  new Messages.Watch(4, ResultFixture.WORK, 0, 0)));
      assertCode(
          ProtocolError.Code.EXPIRED,
          () ->
              sessions.attach(
                  ResultFixture.sessionAccess("alice"),
                  ResultFixture.SELECTED,
                  new Messages.Attach(
                      5, binding.authority(), binding.owner(), binding.generation())));
      assertCode(
          ProtocolError.Code.EXPIRED,
          () ->
              sessions.create(
                  ResultFixture.sessionAccess("alice"),
                  ResultFixture.SELECTED,
                  new Messages.Create(
                      6, binding.creationSequence(), new Records.Policy(10_000, 20_000, 30_000))));

      RetirementStore.Progress complete = finish(sessions, inputs, binding.generation(), 31_000);
      assertEquals(RetirementStore.State.COMPLETE, complete.state());
      assertEquals(
          RetirementStore.State.ABSENT,
          sessions
              .retireSession(binding.generation(), inputs, 1, ResultFixture.clock(31_000))
              .state());
      Messages.Sequence next =
          sessions.nextSequence(
              ResultFixture.sessionAccess("alice"),
              ResultFixture.SELECTED,
              new Messages.NextSequence(7));
      assertEquals(2, next.nextCreationSequence());
      Messages.Binding replacement =
          sessions.create(
              ResultFixture.sessionAccess("alice"),
              ResultFixture.SELECTED,
              new Messages.Create(8, 2, new Records.Policy(10_000, 20_000, 30_000)));
      assertTrue(replacement.generation() > binding.generation());
    }
  }

  @Test
  void publishedSessionReopensDuringBoundedDeletionAndRetainsAllocatorHighWater() throws Exception {
    try (ResultFixture fixture =
        new ResultFixture(directory, "published", new byte[] {9}, new byte[] {1})) {
      fixture.sessions.declare(
          ResultFixture.sessionAccess("alice"),
          ResultFixture.SELECTED,
          fixture.binding.generation(),
          new Messages.Declare(80, ResultFixture.operation(80), 0, List.of(), true));
      closeRoot(fixture.sessions, fixture.binding, List.of(1L), 1200);
      assertEquals(
          RetentionStore.Result.RELEASED,
          fixture.sessions.reclaimOutput(
              fixture.binding.generation(),
              ResultFixture.WORK,
              fixture.inputs,
              ResultFixture.clock(21_200)));
      assertEquals(
          RetentionStore.Result.RELEASED,
          fixture.sessions.reclaimInput(
              fixture.binding.generation(),
              ResultFixture.WORK,
              fixture.inputs,
              ResultFixture.clock(21_200)));
      assertEquals(new InputStore.Usage(0, 0, 0), fixture.inputs.usage());
      assertEquals(
          RetirementStore.State.NOT_READY,
          fixture
              .sessions
              .retireSession(
                  fixture.binding.generation(), fixture.inputs, 1, ResultFixture.clock(31_199))
              .state());
      assertEquals(
          RetirementStore.State.STARTED,
          fixture
              .sessions
              .retireSession(
                  fixture.binding.generation(), fixture.inputs, 1, ResultFixture.clock(31_200))
              .state());
      assertCode(
          ProtocolError.Code.EXPIRED,
          () ->
              fixture.sessions.checkInput(
                  ResultFixture.sessionAccess("alice"),
                  ResultFixture.SELECTED,
                  fixture.binding.generation(),
                  fixture.inputs,
                  fixture.inputHeader,
                  ResultFixture.clock(31_200),
                  ResultFixture.ALLOW_EXECUTION));
      assertCode(
          ProtocolError.Code.EXPIRED,
          () ->
              fixture.inputs.begin(
                  fixture.context(), fixture.inputHeader, ResultFixture.SELECTED, 0));
      RetirementStore.Progress first =
          fixture.sessions.retireSession(
              fixture.binding.generation(), fixture.inputs, 1, ResultFixture.clock(31_200));
      assertEquals(RetirementStore.State.IN_PROGRESS, first.state());
      assertTrue(first.jobs() + first.operations() + first.entities() + first.scopes() <= 1);

      fixture.reopen();
      RetirementStore.Progress complete =
          finish(fixture.sessions, fixture.inputs, fixture.binding.generation(), 31_200);
      assertEquals(RetirementStore.State.COMPLETE, complete.state());
      assertCode(
          ProtocolError.Code.NOT_FOUND,
          () ->
              fixture.sessions.checkInput(
                  ResultFixture.sessionAccess("alice"),
                  ResultFixture.SELECTED,
                  fixture.binding.generation(),
                  fixture.inputs,
                  fixture.inputHeader,
                  ResultFixture.clock(31_200),
                  ResultFixture.ALLOW_EXECUTION));
      assertCode(
          ProtocolError.Code.EXPIRED,
          () ->
              fixture.inputs.begin(
                  fixture.context(), fixture.inputHeader, ResultFixture.SELECTED, 0));
      assertEquals(
          2,
          fixture
              .sessions
              .nextSequence(
                  ResultFixture.sessionAccess("alice"),
                  ResultFixture.SELECTED,
                  new Messages.NextSequence(81))
              .nextCreationSequence());
    }
  }

  @Test
  void revokedSessionRemainsUnauthorizedButOwnerlessRetirementCanStart() throws Exception {
    Path database = directory.resolve("revoked.sqlite");
    SessionStore sessions = SessionStore.initialize(database, ResultFixture.configuration());
    Messages.Binding binding =
        sessions.create(
            ResultFixture.sessionAccess("alice"),
            ResultFixture.SELECTED,
            new Messages.Create(1, 1, new Records.Policy(10_000, 20_000, 30_000)));
    Path inputsPath = directory.resolve("revoked-inputs");
    try (InputStore inputs =
        InputStore.initializeForAuthority(
            inputsPath, ResultFixture.INPUT_LIMITS, sessions.identity())) {
      sessions.bindInputs(inputs);
      sessions.declare(
          ResultFixture.sessionAccess("alice"),
          ResultFixture.SELECTED,
          binding.generation(),
          new Messages.Declare(2, ResultFixture.operation(1), 0, List.of(), true));
      closeRoot(sessions, binding, List.of(), 1000);
      sessions.revoke(
          ResultFixture.sessionAccess("alice"), binding.generation(), ResultFixture.clock(1000));
      assertCode(
          ProtocolError.Code.UNAUTHORIZED,
          () ->
              sessions.attach(
                  ResultFixture.sessionAccess("alice"),
                  ResultFixture.SELECTED,
                  new Messages.Attach(
                      3, binding.authority(), binding.owner(), binding.generation())));
      assertEquals(
          RetirementStore.State.STARTED,
          sessions
              .retireSession(binding.generation(), inputs, 1, ResultFixture.clock(31_000))
              .state());
    }
  }

  private static void closeRoot(
      SessionStore sessions, Messages.Binding binding, List<Long> members, long utc)
      throws Exception {
    ClosureStore.Cursor cursor = new ClosureStore.Cursor();
    Commitments.Seal seal =
        new Commitments.Seal(
            new Commitments.Context(binding.authority(), binding.owner(), binding.generation()),
            0,
            0,
            null,
            members.size());
    for (long member : members) seal.add(member);
    Records.Digest expected = seal.finish();
    for (int calls = 0; calls < 16; calls++) {
      sessions.reconcileClosures(cursor, 1, ResultFixture.clock(utc));
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

  private static RetirementStore.Progress finish(
      SessionStore sessions, InputStore inputs, long generation, long utc) throws Exception {
    RetirementStore.Progress progress = null;
    for (int calls = 0; calls < 64; calls++) {
      progress = sessions.retireSession(generation, inputs, 1, ResultFixture.clock(utc));
      if (progress.state() == RetirementStore.State.COMPLETE) return progress;
      assertEquals(RetirementStore.State.IN_PROGRESS, progress.state());
    }
    return fail("retirement did not complete within bounded metadata units: " + progress);
  }

  private static long sessionCount(Path database) throws Exception {
    try (var connection =
            BoundedSqlite.open(database, ResultFixture.configuration().files()).connect();
        var statement = connection.createStatement();
        var rows = statement.executeQuery("SELECT count(*) FROM ps_v2_sessions")) {
      assertTrue(rows.next());
      return rows.getLong(1);
    }
  }

  private static void assertCode(ProtocolError.Code expected, Throwing action) {
    ProtocolError failure = assertThrows(ProtocolError.class, action::run);
    assertEquals(expected, failure.code(), failure::getMessage);
  }

  @FunctionalInterface
  private interface Throwing {
    void run() throws Exception;
  }
}
