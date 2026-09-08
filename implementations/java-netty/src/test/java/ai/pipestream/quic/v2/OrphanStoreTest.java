package ai.pipestream.quic.v2;

import static org.junit.jupiter.api.Assertions.*;

import ai.pipestream.quic.BoundedSqlite;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

@Timeout(30)
final class OrphanStoreTest {
  @TempDir Path directory;

  @Test
  void declaredButUnadmittedInputAndFundingAreIndependentlyReleasedAndRemainAbsent()
      throws Exception {
    try (Fixture fixture = new Fixture(directory, "released")) {
      InputStore.Usage retained = fixture.inputs.usage();
      List<InputStore.OrphanCandidate> candidates = fixture.candidates();
      assertEquals(2, candidates.size());
      InputStore.OrphanCandidate object = candidate(candidates, false);
      InputStore.OrphanCandidate funding = candidate(candidates, true);

      assertEquals(
          OrphanStore.Result.RELEASED,
          fixture.sessions.reclaimOrphan(fixture.inputs, object, ResultFixture.clock(1100)));
      assertTrue(fixture.inputs.find(fixture.context, fixture.header).isEmpty());
      assertTrue(fixture.inputs.findReservation(fixture.context, fixture.header).isPresent());
      assertTrue(fixture.inputs.usage().bytes() < retained.bytes());
      assertEquals(
          OrphanStore.Result.ABSENT,
          fixture.sessions.reclaimOrphan(fixture.inputs, object, ResultFixture.clock(1100)));

      assertEquals(
          OrphanStore.Result.RELEASED,
          fixture.sessions.reclaimOrphan(fixture.inputs, funding, ResultFixture.clock(1100)));
      assertEquals(new InputStore.Usage(0, 0, 0), fixture.inputs.usage());
      assertEquals(
          OrphanStore.Result.ABSENT,
          fixture.sessions.reclaimOrphan(fixture.inputs, funding, ResultFixture.clock(1100)));
      fixture.reopen();
      assertEquals(new InputStore.Usage(0, 0, 0), fixture.inputs.usage());
    }
  }

  @Test
  void liveReaderPinsOnlyItsObjectAndUnsafeClockCannotDeleteEitherCandidate() throws Exception {
    try (Fixture fixture = new Fixture(directory, "pin")) {
      List<InputStore.OrphanCandidate> candidates = fixture.candidates();
      InputStore.OrphanCandidate object = candidate(candidates, false);
      InputStore.OrphanCandidate funding = candidate(candidates, true);
      InputStore.Usage retained = fixture.inputs.usage();
      assertCode(
          ProtocolError.Code.CLOCK_UNSAFE,
          () ->
              fixture.sessions.reclaimOrphan(
                  fixture.inputs, object, () -> new AdmissionStore.Time(1100, false)));
      assertEquals(retained, fixture.inputs.usage());

      try (var reader =
          fixture.inputs.find(fixture.context, fixture.header).orElseThrow().openStream()) {
        assertEquals(7, reader.read());
        assertEquals(
            OrphanStore.Result.PINNED,
            fixture.sessions.reclaimOrphan(fixture.inputs, object, ResultFixture.clock(1100)));
        assertEquals(
            OrphanStore.Result.PINNED,
            fixture.sessions.reclaimOrphan(fixture.inputs, funding, ResultFixture.clock(1100)));
      }
      assertEquals(
          OrphanStore.Result.RELEASED,
          fixture.sessions.reclaimOrphan(fixture.inputs, object, ResultFixture.clock(1100)));
      assertEquals(
          OrphanStore.Result.RELEASED,
          fixture.sessions.reclaimOrphan(fixture.inputs, funding, ResultFixture.clock(1100)));
    }
  }

  @Test
  void admittedJobProtectsBothRetainedCandidates() throws Exception {
    try (Fixture fixture = new Fixture(directory, "accepted")) {
      fixture.sessions.admit(
          ResultFixture.sessionAccess("alice"),
          ResultFixture.SELECTED,
          fixture.binding.generation(),
          fixture.inputs,
          fixture.header,
          3,
          ResultFixture.clock(1000),
          ResultFixture.ALLOW_EXECUTION);
      InputStore.Usage retained = fixture.inputs.usage();
      for (InputStore.OrphanCandidate candidate : fixture.candidates()) {
        assertEquals(
            OrphanStore.Result.RETAINED,
            fixture.sessions.reclaimOrphan(fixture.inputs, candidate, ResultFixture.clock(1100)));
      }
      assertEquals(retained, fixture.inputs.usage());
    }
  }

  @Test
  void missingAdmittedJobIsCorruptionAndNeverDeletionAuthority() throws Exception {
    try (Fixture fixture = new Fixture(directory, "lost-job")) {
      fixture.sessions.admit(
          ResultFixture.sessionAccess("alice"),
          ResultFixture.SELECTED,
          fixture.binding.generation(),
          fixture.inputs,
          fixture.header,
          3,
          ResultFixture.clock(1000),
          ResultFixture.ALLOW_EXECUTION);
      List<InputStore.OrphanCandidate> candidates = fixture.candidates();
      InputStore.Usage retained = fixture.inputs.usage();
      try (var connection =
              BoundedSqlite.open(fixture.database, ResultFixture.configuration().files())
                  .connect();
          var statement = connection.createStatement()) {
        statement.execute("BEGIN IMMEDIATE");
        assertEquals(
            1,
            statement.executeUpdate(
                "DELETE FROM ps_v2_jobs WHERE generation="
                    + fixture.binding.generation()
                    + " AND scope=0 AND entity=1"));
        statement.execute("COMMIT");
      }

      for (InputStore.OrphanCandidate candidate : candidates) {
        assertThrows(
            java.sql.SQLException.class,
            () ->
                fixture.sessions.reclaimOrphan(
                    fixture.inputs, candidate, ResultFixture.clock(1100)));
      }
      assertEquals(retained, fixture.inputs.usage());
      assertTrue(fixture.inputs.find(fixture.context, fixture.header).isPresent());
      assertTrue(fixture.inputs.findReservation(fixture.context, fixture.header).isPresent());
    }
  }

  @Test
  void delayedDuplicateReceiverPinsInputAndFundingUntilItsFinCompletes() throws Exception {
    try (Fixture fixture = new Fixture(directory, "duplicate-fin")) {
      List<InputStore.OrphanCandidate> candidates = fixture.candidates();
      byte[] payload = {7, 8, 9};
      try (InputStore.Receiver duplicate =
          fixture.inputs.begin(fixture.context, fixture.header, ResultFixture.SELECTED, 4)) {
        duplicate.write(ByteBuffer.wrap(payload), 5);
        for (InputStore.OrphanCandidate candidate : candidates) {
          assertEquals(
              OrphanStore.Result.PINNED,
              fixture.sessions.reclaimOrphan(fixture.inputs, candidate, ResultFixture.clock(1100)));
        }
        duplicate.finish(6);
      }
      assertTrue(fixture.inputs.find(fixture.context, fixture.header).isPresent());
      for (InputStore.OrphanCandidate candidate : candidates) {
        assertEquals(
            OrphanStore.Result.RELEASED,
            fixture.sessions.reclaimOrphan(fixture.inputs, candidate, ResultFixture.clock(1100)));
      }
      assertEquals(new InputStore.Usage(0, 0, 0), fixture.inputs.usage());
    }
  }

  @Test
  void synchronizedUnlinkFailureKeepsChargeUntilSameProcessRetry() throws Exception {
    try (Fixture fixture = new Fixture(directory, "sync-failure")) {
      InputStore.OrphanCandidate object = candidate(fixture.candidates(), false);
      InputStore.Usage retained = fixture.inputs.usage();
      java.util.concurrent.atomic.AtomicBoolean interrupt =
          new java.util.concurrent.atomic.AtomicBoolean(true);
      fixture.inputs.close();
      fixture.inputs =
          InputStore.open(
              fixture.inputPath,
              ResultFixture.INPUT_LIMITS,
              phase -> {
                if (phase == InputStore.Phase.ORPHAN_SYNCED && interrupt.getAndSet(false)) {
                  throw new java.io.IOException("test orphan synchronization interruption");
                }
              });
      fixture.sessions.verifyInputs(fixture.inputs);

      java.io.IOException failure =
          assertThrows(
              java.io.IOException.class,
              () ->
                  fixture.sessions.reclaimOrphan(
                      fixture.inputs, object, ResultFixture.clock(1100)));
      assertEquals("test orphan synchronization interruption", failure.getMessage());
      assertEquals(retained, fixture.inputs.usage());
      assertTrue(fixture.inputs.find(fixture.context, fixture.header).isEmpty());
      assertCode(
          ProtocolError.Code.CONFLICT,
          () -> fixture.inputs.begin(fixture.context, fixture.header, ResultFixture.SELECTED, 4));
      assertEquals(
          OrphanStore.Result.RELEASED,
          fixture.sessions.reclaimOrphan(fixture.inputs, object, ResultFixture.clock(1100)));
      assertTrue(fixture.inputs.usage().bytes() < retained.bytes());
      assertEquals(retained.files() - 1, fixture.inputs.usage().files());

      InputStore.OrphanCandidate funding = candidate(fixture.candidates(), true);
      interrupt.set(true);
      java.io.IOException fundingFailure =
          assertThrows(
              java.io.IOException.class,
              () ->
                  fixture.sessions.reclaimOrphan(
                      fixture.inputs, funding, ResultFixture.clock(1100)));
      assertEquals("test orphan synchronization interruption", fundingFailure.getMessage());
      assertCode(
          ProtocolError.Code.CONFLICT,
          () -> fixture.inputs.reserveOutputs(fixture.context, fixture.header));
      assertEquals(
          OrphanStore.Result.RELEASED,
          fixture.sessions.reclaimOrphan(fixture.inputs, funding, ResultFixture.clock(1100)));
      assertEquals(new InputStore.Usage(0, 0, 0), fixture.inputs.usage());
    }
  }

  private static InputStore.OrphanCandidate candidate(
      List<InputStore.OrphanCandidate> candidates, boolean funding) {
    return candidates.stream()
        .filter(value -> value.funding() == funding)
        .findFirst()
        .orElseThrow();
  }

  private static void assertCode(ProtocolError.Code expected, Throwing action) {
    ProtocolError error = assertThrows(ProtocolError.class, action::run);
    assertEquals(expected, error.code(), error::getMessage);
  }

  @FunctionalInterface
  private interface Throwing {
    void run() throws Exception;
  }

  static final class Fixture implements AutoCloseable {
    final Path database;
    final Path inputPath;
    final Messages.Binding binding;
    final Commitments.Context context;
    final Records.InputHeader header;
    SessionStore sessions;
    InputStore inputs;

    Fixture(Path directory, String name) throws Exception {
      database = directory.resolve(name + ".sqlite");
      inputPath = directory.resolve(name + "-inputs");
      sessions = SessionStore.initialize(database, ResultFixture.configuration());
      binding =
          sessions.create(
              ResultFixture.sessionAccess("alice"),
              ResultFixture.SELECTED,
              new Messages.Create(1, 1, new Records.Policy(10_000, 20_000, 30_000)));
      sessions.declare(
          ResultFixture.sessionAccess("alice"),
          ResultFixture.SELECTED,
          binding.generation(),
          new Messages.Declare(2, ResultFixture.operation(1), 0, List.of(1L), false));
      inputs =
          InputStore.initializeForAuthority(
              inputPath, ResultFixture.INPUT_LIMITS, sessions.identity());
      sessions.bindInputs(inputs);
      context = new Commitments.Context(binding.authority(), binding.owner(), binding.generation());
      byte[] payload = {7, 8, 9};
      header =
          new Records.InputHeader(
              binding.generation(),
              ResultFixture.operation(2),
              new Records.AdmitParameters(
                  ResultFixture.WORK,
                  new Records.Input(
                      payload.length, ResultFixture.digest(payload), "application/octet-stream"),
                  "copy",
                  0,
                  1000,
                  new Records.OutputBudget(1, 4)));
      try (InputStore.Receiver receiver =
          inputs.begin(context, header, ResultFixture.SELECTED, 1)) {
        receiver.write(ByteBuffer.wrap(payload), 2);
        receiver.finish(3);
      }
      inputs.reserveOutputs(context, header);
    }

    List<InputStore.OrphanCandidate> candidates() throws Exception {
      try (InputStore.OrphanScan scan = inputs.scanOrphans()) {
        InputStore.OrphanPage page = scan.nextPage(64);
        assertTrue(page.done());
        assertEquals(page.examined(), page.candidates().size());
        return page.candidates();
      }
    }

    void reopen() throws Exception {
      inputs.close();
      sessions = SessionStore.open(database, ResultFixture.configuration());
      inputs = InputStore.open(inputPath, ResultFixture.INPUT_LIMITS);
      sessions.verifyInputs(inputs);
    }

    @Override
    public void close() throws java.io.IOException {
      inputs.close();
    }
  }
}
