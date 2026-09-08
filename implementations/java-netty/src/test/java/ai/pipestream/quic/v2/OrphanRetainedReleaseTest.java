package ai.pipestream.quic.v2;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

@Timeout(30)
final class OrphanRetainedReleaseTest {
  private static final byte[] INPUT = {9, 8, 7};
  private static final byte[] OUTPUT = {1, 2, 3, 4};

  @TempDir Path directory;

  @Test
  void cachedCandidatesBecomeAbsentAfterAuthorizedRetentionCleanup() throws Exception {
    try (ResultFixture fixture = new ResultFixture(directory, "released", INPUT, OUTPUT)) {
      List<InputStore.OrphanCandidate> candidates = candidates(fixture.inputs);
      assertEquals(2, candidates.size());
      Records.WorkView manifest = fixture.published;

      releaseBoth(fixture);
      assertEquals(new InputStore.Usage(0, 0, 0), fixture.inputs.usage());
      for (InputStore.OrphanCandidate candidate : candidates) {
        assertEquals(
            OrphanStore.Result.ABSENT,
            fixture.sessions.reclaimOrphan(fixture.inputs, candidate, ResultFixture.clock(21_200)));
      }
      assertEquals(new InputStore.Usage(0, 0, 0), fixture.inputs.usage());
      assertEquals(manifest, current(fixture));
    }
  }

  @Test
  void resurrectedInputUnderReleasedMetadataIsCorruptionAndIsNotDeleted() throws Exception {
    try (ResultFixture fixture =
        new ResultFixture(directory, "input-resurrection", INPUT, OUTPUT)) {
      InputStore.OrphanCandidate cached = candidate(candidates(fixture.inputs), false);
      long inputBytes = Files.size(onlyInput(fixture));
      releaseBoth(fixture);

      try (InputStore.Receiver receiver =
          fixture.inputs.begin(
              fixture.context(), fixture.inputHeader, ResultFixture.SELECTED, 30_000)) {
        receiver.write(ByteBuffer.wrap(INPUT), 30_001);
        receiver.finish(30_002);
      }
      InputStore.Usage resurrected = fixture.inputs.usage();
      assertEquals(inputBytes, resurrected.bytes());
      assertEquals(1, resurrected.files());
      assertThrows(
          java.io.IOException.class,
          () ->
              fixture.sessions.reclaimOrphan(fixture.inputs, cached, ResultFixture.clock(30_100)));
      assertEquals(resurrected, fixture.inputs.usage());
      assertTrue(fixture.inputs.find(fixture.context(), fixture.inputHeader).isPresent());
      assertEquals(fixture.published, current(fixture));
    }
  }

  @Test
  void resurrectedFundingUnderReleasedMetadataIsCorruptionAndIsNotDeleted() throws Exception {
    try (ResultFixture fixture =
        new ResultFixture(directory, "funding-resurrection", INPUT, OUTPUT)) {
      InputStore.OrphanCandidate cached = candidate(candidates(fixture.inputs), true);
      InputStore.Usage retained = fixture.inputs.usage();
      long inputBytes = Files.size(onlyInput(fixture));
      releaseBoth(fixture);

      fixture.inputs.reserveOutputs(fixture.context(), fixture.inputHeader);
      InputStore.Usage resurrected = fixture.inputs.usage();
      assertEquals(retained.bytes() - inputBytes, resurrected.bytes());
      assertEquals(3, resurrected.files());
      assertThrows(
          java.io.IOException.class,
          () ->
              fixture.sessions.reclaimOrphan(fixture.inputs, cached, ResultFixture.clock(30_100)));
      assertEquals(resurrected, fixture.inputs.usage());
      assertTrue(
          fixture.inputs.findReservation(fixture.context(), fixture.inputHeader).isPresent());
      assertEquals(fixture.published, current(fixture));
    }
  }

  private static void releaseBoth(ResultFixture fixture) throws Exception {
    assertEquals(
        RetentionStore.Result.RELEASED,
        fixture.sessions.reclaimInput(
            fixture.binding.generation(),
            ResultFixture.WORK,
            fixture.inputs,
            ResultFixture.clock(21_200)));
    assertEquals(
        RetentionStore.Result.RELEASED,
        fixture.sessions.reclaimOutput(
            fixture.binding.generation(),
            ResultFixture.WORK,
            fixture.inputs,
            ResultFixture.clock(21_200)));
  }

  private static List<InputStore.OrphanCandidate> candidates(InputStore inputs) throws Exception {
    try (InputStore.OrphanScan scan = inputs.scanOrphans()) {
      InputStore.OrphanPage page = scan.nextPage(64);
      assertTrue(page.done());
      assertEquals(2, page.candidates().size());
      return page.candidates();
    }
  }

  private static InputStore.OrphanCandidate candidate(
      List<InputStore.OrphanCandidate> candidates, boolean funding) {
    return candidates.stream()
        .filter(value -> value.funding() == funding)
        .findFirst()
        .orElseThrow();
  }

  private static Records.WorkView current(ResultFixture fixture) throws Exception {
    return fixture
        .sessions
        .snapshot(
            ResultFixture.sessionAccess("alice"),
            ResultFixture.SELECTED,
            fixture.binding.generation(),
            new Messages.Watch(91, ResultFixture.WORK, 0, 0))
        .work();
  }

  private static java.nio.file.Path onlyInput(ResultFixture fixture) throws Exception {
    try (var entries = Files.newDirectoryStream(fixture.inputsPath.resolve("objects"), "*.input")) {
      var iterator = entries.iterator();
      assertTrue(iterator.hasNext());
      java.nio.file.Path input = iterator.next();
      assertFalse(iterator.hasNext());
      return input;
    }
  }
}
