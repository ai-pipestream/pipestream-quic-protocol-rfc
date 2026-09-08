package ai.pipestream.quic.v2;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

@Timeout(20)
final class CheckpointObservationTest {
  @TempDir Path directory;

  @Test
  void sealedEmptyClosureReturnsCorrelatedImmediateCheckpointAndSurvivesRecovery()
      throws Exception {
    Fixture fixture = new Fixture("empty");
    fixture.sessions.declare(
        access(),
        ResultFixture.SELECTED,
        fixture.binding.generation(),
        new Messages.Declare(1, ResultFixture.operation(1), 0, List.of(), true));
    Records.Digest seal = fixture.seal(List.of());
    Messages.Checkpoint pending = new Messages.Checkpoint(2, 0, seal, 30_000);
    assertTrue(
        fixture
            .sessions
            .checkpoint(access(), ResultFixture.SELECTED, fixture.binding.generation(), pending)
            .isEmpty());
    fixture.closeRoot(seal);

    Messages.Checkpoint request = new Messages.Checkpoint(3, 0, seal, 0);
    Messages.CheckpointResponse response =
        fixture
            .sessions
            .checkpoint(access(), ResultFixture.SELECTED, fixture.binding.generation(), request)
            .orElseThrow();
    assertEquals(request.request(), response.request());
    assertEquals(0, response.summary().declared());
    assertEquals(new Records.Counts(0, 0, 0, 0), response.summary().counts());
    assertEquals(Commitments.emptyStatus(), response.summary().statusRoot());

    fixture.reopen();
    Messages.Checkpoint replay = new Messages.Checkpoint(4, 0, seal, 30_000);
    assertEquals(
        new Messages.CheckpointResponse(replay.request(), response.summary()),
        fixture
            .sessions
            .checkpoint(access(), ResultFixture.SELECTED, fixture.binding.generation(), replay)
            .orElseThrow());
  }

  @Test
  void unsealedAndSealedMissingInputMembershipHaveDistinctImmediateObservations() throws Exception {
    Fixture fixture = new Fixture("pending");
    fixture.sessions.declare(
        access(),
        ResultFixture.SELECTED,
        fixture.binding.generation(),
        new Messages.Declare(1, ResultFixture.operation(1), 0, List.of(1L), false));
    Records.Digest expected = fixture.seal(List.of(1L));
    assertCode(
        ProtocolError.Code.NOT_READY,
        () ->
            fixture.sessions.checkpoint(
                access(),
                ResultFixture.SELECTED,
                fixture.binding.generation(),
                new Messages.Checkpoint(2, 0, expected, 0)));

    fixture.sessions.declare(
        access(),
        ResultFixture.SELECTED,
        fixture.binding.generation(),
        new Messages.Declare(3, ResultFixture.operation(2), 0, List.of(), true));
    Optional<Messages.CheckpointResponse> sealed =
        fixture.sessions.checkpoint(
            access(),
            ResultFixture.SELECTED,
            fixture.binding.generation(),
            new Messages.Checkpoint(4, 0, expected, 30_000));
    assertTrue(sealed.isEmpty());
    fixture.sessions.reconcileClosures(new ClosureStore.Cursor(), 1, ResultFixture.clock(1000));
    assertTrue(
        fixture
            .sessions
            .checkpoint(
                access(),
                ResultFixture.SELECTED,
                fixture.binding.generation(),
                new Messages.Checkpoint(5, 0, expected, 0))
            .isEmpty());
  }

  @Test
  void wrongSealAndAuthorizationAreCheckedBeforeClosureOrExistenceInformation() throws Exception {
    Fixture fixture = new Fixture("refusals");
    fixture.sessions.declare(
        access(),
        ResultFixture.SELECTED,
        fixture.binding.generation(),
        new Messages.Declare(1, ResultFixture.operation(1), 0, List.of(), true));
    Records.Digest seal = fixture.seal(List.of());
    assertCode(
        ProtocolError.Code.INTEGRITY_ERROR,
        () ->
            fixture.sessions.checkpoint(
                access(),
                ResultFixture.SELECTED,
                fixture.binding.generation(),
                new Messages.Checkpoint(2, 0, ResultFixture.digest(new byte[] {1}), 0)));
    SessionStore.Access denied =
        new SessionStore.Access(
            "alice",
            () -> {
              throw new ProtocolError(ProtocolError.Code.UNAUTHORIZED, "denied checkpoint");
            });
    assertCode(
        ProtocolError.Code.UNAUTHORIZED,
        () ->
            fixture.sessions.checkpoint(
                denied,
                ResultFixture.SELECTED,
                Long.MAX_VALUE,
                new Messages.Checkpoint(3, 0, seal, 0)));
  }

  @Test
  void revocationAndFinalCredentialRecheckPreventObservation() throws Exception {
    Fixture revoked = new Fixture("revoked");
    revoked.sessions.declare(
        access(),
        ResultFixture.SELECTED,
        revoked.binding.generation(),
        new Messages.Declare(1, ResultFixture.operation(1), 0, List.of(), true));
    Records.Digest revokedSeal = revoked.seal(List.of());
    revoked.closeRoot(revokedSeal);
    revoked.sessions.revoke(access(), revoked.binding.generation(), ResultFixture.clock(1000));
    assertCode(
        ProtocolError.Code.UNAUTHORIZED,
        () ->
            revoked.sessions.checkpoint(
                access(),
                ResultFixture.SELECTED,
                revoked.binding.generation(),
                new Messages.Checkpoint(2, 0, revokedSeal, 0)));

    Fixture current = new Fixture("current");
    current.sessions.declare(
        access(),
        ResultFixture.SELECTED,
        current.binding.generation(),
        new Messages.Declare(1, ResultFixture.operation(1), 0, List.of(), true));
    Records.Digest currentSeal = current.seal(List.of());
    current.closeRoot(currentSeal);
    AtomicInteger checks = new AtomicInteger();
    SessionStore.Access expires =
        new SessionStore.Access(
            "alice",
            () -> {
              if (checks.incrementAndGet() == 3)
                throw new ProtocolError(ProtocolError.Code.UNAUTHORIZED, "credential changed");
            });
    assertCode(
        ProtocolError.Code.UNAUTHORIZED,
        () ->
            current.sessions.checkpoint(
                expires,
                ResultFixture.SELECTED,
                current.binding.generation(),
                new Messages.Checkpoint(2, 0, currentSeal, 0)));
    assertEquals(3, checks.get());
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
              new Messages.Create(90, 1, new Records.Policy(10_000, 20_000, 30_000)));
    }

    Records.Digest seal(List<Long> members) {
      Commitments.Seal seal =
          new Commitments.Seal(
              new Commitments.Context(binding.authority(), binding.owner(), binding.generation()),
              0,
              0,
              null,
              members.size());
      for (long member : members) seal.add(member);
      return seal.finish();
    }

    void closeRoot(Records.Digest seal) throws Exception {
      ClosureStore.Cursor cursor = new ClosureStore.Cursor();
      for (int calls = 0; calls < 8; calls++) {
        sessions.reconcileClosures(cursor, 1, ResultFixture.clock(1000));
        if (sessions
            .checkpoint(
                access(),
                ResultFixture.SELECTED,
                binding.generation(),
                new Messages.Checkpoint(91 + calls, 0, seal, 0))
            .isPresent()) return;
      }
      fail("empty scope did not close within bounded reconciliation calls");
    }

    void reopen() throws Exception {
      sessions = SessionStore.open(database, ResultFixture.configuration());
    }
  }

  private static SessionStore.Access access() {
    return new SessionStore.Access("alice", () -> {});
  }

  private static void assertCode(ProtocolError.Code expected, Throwing action) {
    ProtocolError refusal = assertThrows(ProtocolError.class, action::run);
    assertEquals(expected, refusal.code(), refusal::getMessage);
  }

  @FunctionalInterface
  private interface Throwing {
    void run() throws Exception;
  }
}
