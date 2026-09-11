package ai.pipestream.quic.v2;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * An installed input that its authority is still admitting is physically owned until the
 * admission transaction releases it: the orphan sweep reports it PINNED, never reclaims it, and
 * an unpinned installation keeps the old behaviour (handoff defect 10).
 */
@Timeout(30)
final class InputStorePinnedInstallTest {
  @TempDir Path directory;

  static final class Fixture implements AutoCloseable {
    final SessionStore sessions;
    final InputStore inputs;
    final Commitments.Context context;
    final Records.InputHeader header;
    final byte[] payload = {7, 8, 9};

    Fixture(Path directory, String name) throws Exception {
      sessions =
          SessionStore.initialize(directory.resolve(name + ".sqlite"), ResultFixture.configuration());
      Messages.Binding binding =
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
              directory.resolve(name + "-inputs"), ResultFixture.INPUT_LIMITS, sessions.identity());
      sessions.bindInputs(inputs);
      context = new Commitments.Context(binding.authority(), binding.owner(), binding.generation());
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
    }

    InputStore.Stored install(boolean pinned) throws Exception {
      try (InputStore.Receiver receiver =
          inputs.begin(context, header, ResultFixture.SELECTED, 1)) {
        receiver.write(ByteBuffer.wrap(payload), 2);
        return receiver.finish(3, pinned);
      }
    }

    InputStore.OrphanCandidate candidate() throws Exception {
      try (InputStore.OrphanScan scan = inputs.scanOrphans()) {
        InputStore.OrphanPage page = scan.nextPage(64);
        assertTrue(page.done());
        assertEquals(1, page.candidates().size(), page.candidates().toString());
        return page.candidates().getFirst();
      }
    }

    OrphanStore.Result reclaim(InputStore.OrphanCandidate candidate) throws Exception {
      return sessions.reclaimOrphan(inputs, candidate, ResultFixture.clock(1100));
    }

    @Override
    public void close() throws java.io.IOException, java.sql.SQLException {
      inputs.close();
      sessions.close();
    }
  }

  @Test
  void aPinnedInstallationIsNotReclaimedUntilItsHandleIsReleased() throws Exception {
    try (Fixture fixture = new Fixture(directory, "pinned")) {
      InputStore.Stored stored = fixture.install(true);
      assertTrue(fixture.inputs.inputInUse(fixture.context, fixture.header));
      InputStore.OrphanCandidate candidate = fixture.candidate();
      assertEquals(OrphanStore.Result.PINNED, fixture.reclaim(candidate));
      assertTrue(fixture.inputs.find(fixture.context, fixture.header).isPresent());
      assertEquals(OrphanStore.Result.PINNED, fixture.reclaim(candidate));
      // The admission transaction ends: ownership ends with it, once.
      stored.release();
      stored.release();
      assertFalse(fixture.inputs.inputInUse(fixture.context, fixture.header));
      assertEquals(OrphanStore.Result.RELEASED, fixture.reclaim(candidate));
      assertTrue(fixture.inputs.find(fixture.context, fixture.header).isEmpty());
    }
  }

  @Test
  void anUnpinnedInstallationKeepsTheOldLifecycle() throws Exception {
    try (Fixture fixture = new Fixture(directory, "unpinned")) {
      InputStore.Stored stored = fixture.install(false);
      assertFalse(fixture.inputs.inputInUse(fixture.context, fixture.header));
      stored.release();
      assertFalse(fixture.inputs.inputInUse(fixture.context, fixture.header));
      assertEquals(OrphanStore.Result.RELEASED, fixture.reclaim(fixture.candidate()));
    }
  }
}
