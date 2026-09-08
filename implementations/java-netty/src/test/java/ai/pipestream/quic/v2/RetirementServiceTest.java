package ai.pipestream.quic.v2;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

@Timeout(30)
final class RetirementServiceTest {
  @TempDir Path directory;

  @Test
  void timerRetiresARealClosedEmptySessionAndPreservesOwnerSequence() throws Exception {
    try (Fixture fixture = new Fixture(directory, "timer")) {
      Messages.Binding retired = fixture.create("alice", 1, true);
      RetentionService service =
          new RetentionService(
              fixture.sessions,
              fixture.inputs,
              ResultFixture.clock(31_000),
              new RetentionService.Limits(1, 5));
      try {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (service.status().sessionsRetired() == 0 && System.nanoTime() - deadline < 0)
          Thread.sleep(10);
        assertEquals(1, service.status().sessionsRetired());
        assertTrue(service.status().sessionsExamined() > 0);
      } finally {
        service.close();
        assertTrue(service.awaitStopped(5000));
      }
      assertEquals(
          RetirementStore.State.ABSENT,
          fixture
              .sessions
              .retireSession(retired.generation(), fixture.inputs, 1, ResultFixture.clock(31_000))
              .state());
      assertEquals(
          2,
          fixture
              .sessions
              .nextSequence(
                  ResultFixture.sessionAccess("alice"),
                  ResultFixture.SELECTED,
                  new Messages.NextSequence(9))
              .nextCreationSequence());
      assertEquals(0, fixture.inputs.usage().handles());
    }
  }

  @Test
  void openFirstSessionConsumesPageBudgetButCannotStarveLaterClosedSession() throws Exception {
    try (Fixture fixture = new Fixture(directory, "fair")) {
      Messages.Binding open = fixture.create("alice", 1, false);
      Messages.Binding closed = fixture.create("bob", 1, true);
      try (RetentionService service =
          new RetentionService(
              fixture.sessions,
              fixture.inputs,
              ResultFixture.clock(31_000),
              new RetentionService.Limits(1, 60_000))) {
        for (int call = 0; call < 16 && service.status().sessionsRetired() == 0; call++)
          service.maintain();
        assertEquals(1, service.status().sessionsRetired());
        assertTrue(service.status().sessionsExamined() >= 2);
        Messages.Binding replay =
            fixture.sessions.create(
                ResultFixture.sessionAccess("alice"),
                ResultFixture.SELECTED,
                new Messages.Create(
                    20, open.creationSequence(), new Records.Policy(10_000, 20_000, 30_000)));
        assertEquals(
            new Messages.Binding(
                20,
                open.authority(),
                open.owner(),
                open.generation(),
                open.creationSequence(),
                open.policy(),
                open.limits()),
            replay);
        assertEquals(
            RetirementStore.State.ABSENT,
            fixture
                .sessions
                .retireSession(closed.generation(), fixture.inputs, 1, ResultFixture.clock(31_000))
                .state());
      }
    }
  }

  @Test
  void capturedRetirementSweepExcludesSessionsCreatedBeyondItsHighWater() throws Exception {
    try (Fixture fixture = new Fixture(directory, "ceiling")) {
      fixture.create("alice", 1, false);
      Messages.Binding within = fixture.create("bob", 1, true);
      RetirementStore.Page first = fixture.sessions.scanRetirements(null, 1);
      assertEquals(1, first.examined());
      assertTrue(first.generations().isEmpty());
      assertNotNull(first.next());
      assertEquals(within.generation(), first.next().through());

      Messages.Binding later = fixture.create("alice", 2, true);
      RetirementStore.Page remainder = fixture.sessions.scanRetirements(first.next(), 64);
      assertNull(remainder.next());
      assertEquals(1, remainder.examined());
      assertEquals(List.of(within.generation()), remainder.generations());
      assertFalse(remainder.generations().contains(later.generation()));

      RetirementStore.Page nextSweep = fixture.sessions.scanRetirements(null, 64);
      assertNull(nextSweep.next());
      assertEquals(3, nextSweep.examined());
      assertEquals(List.of(within.generation(), later.generation()), nextSweep.generations());
    }
  }

  private static void closeRoot(SessionStore sessions, Messages.Binding binding, long utc)
      throws Exception {
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
      sessions.reconcileClosures(cursor, 1, ResultFixture.clock(utc));
      try {
        sessions.scopeSummary(
            ResultFixture.sessionAccess(binding.owner()),
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

  private static final class Fixture implements AutoCloseable {
    final SessionStore sessions;
    final InputStore inputs;

    Fixture(Path directory, String name) throws Exception {
      sessions =
          SessionStore.initialize(
              directory.resolve(name + ".sqlite"), ResultFixture.configuration());
      inputs =
          InputStore.initializeForAuthority(
              directory.resolve(name + "-inputs"), ResultFixture.INPUT_LIMITS, sessions.identity());
      sessions.bindInputs(inputs);
    }

    Messages.Binding create(String owner, long sequence, boolean close) throws Exception {
      Messages.Binding binding =
          sessions.create(
              ResultFixture.sessionAccess(owner),
              ResultFixture.SELECTED,
              new Messages.Create(sequence, sequence, new Records.Policy(10_000, 20_000, 30_000)));
      if (close) {
        sessions.declare(
            ResultFixture.sessionAccess(owner),
            ResultFixture.SELECTED,
            binding.generation(),
            new Messages.Declare(
                sequence + 10, ResultFixture.operation((int) sequence), 0, List.of(), true));
        closeRoot(sessions, binding, 1000);
      }
      return binding;
    }

    @Override
    public void close() throws IOException {
      inputs.close();
    }
  }
}
