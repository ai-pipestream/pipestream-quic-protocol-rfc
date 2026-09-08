package ai.pipestream.quic.v2;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

@Timeout(30)
final class OrphanScanTest {
  @TempDir Path directory;

  @Test
  void boundedPagesChargeOneHandleAndASecondScannerIsRefused() throws Exception {
    try (OrphanStoreTest.Fixture fixture = new OrphanStoreTest.Fixture(directory, "bounded")) {
      InputStore.Usage before = fixture.inputs.usage();
      InputStore.OrphanScan scan = fixture.inputs.scanOrphans();
      try {
        assertEquals(before.handles() + 1, fixture.inputs.usage().handles());
        assertThrows(ProtocolError.class, fixture.inputs::scanOrphans);
        List<InputStore.OrphanCandidate> found = new ArrayList<>();
        int examined = 0;
        boolean done = false;
        for (int pages = 0; pages < 8 && !done; pages++) {
          InputStore.OrphanPage page = scan.nextPage(1);
          assertTrue(page.examined() <= 1);
          assertTrue(page.candidates().size() <= page.examined());
          examined += page.examined();
          found.addAll(page.candidates());
          done = page.done();
        }
        assertTrue(done);
        assertEquals(2, found.size());
        assertTrue(examined >= found.size());
        assertEquals(2, new HashSet<>(found).size());
      } finally {
        scan.close();
      }
      assertEquals(before, fixture.inputs.usage());
      scan.close();
      assertThrows(java.io.IOException.class, () -> scan.nextPage(1));
    }
  }

  @Test
  void scanRemainsFiniteUnderNewInstallationAndLaterScanObservesIt() throws Exception {
    try (OrphanStoreTest.Fixture fixture = new OrphanStoreTest.Fixture(directory, "snapshot");
        InputStore.OrphanScan first = fixture.inputs.scanOrphans()) {
      Records.InputHeader later = header(fixture, 3, 2, new byte[] {4});
      install(fixture, later, new byte[] {4});
      String laterReference = fixture.inputs.find(fixture.context, later).orElseThrow().reference();

      Set<String> firstReferences = references(drain(first));
      assertTrue(firstReferences.size() <= ResultFixture.INPUT_LIMITS.files());
      try (InputStore.OrphanScan second = fixture.inputs.scanOrphans()) {
        Set<String> secondReferences = references(drain(second));
        assertTrue(secondReferences.contains(laterReference));
        assertEquals(3, secondReferences.size());
      }
    }
  }

  @Test
  void candidatesRetainFullIdentityAndForeignCandidateDoesNotAliasInstallation() throws Exception {
    try (OrphanStoreTest.Fixture first = new OrphanStoreTest.Fixture(directory, "first");
        OrphanStoreTest.Fixture second = new OrphanStoreTest.Fixture(directory, "second")) {
      for (InputStore.OrphanCandidate candidate : first.candidates()) {
        assertEquals(first.inputs.identity(), candidate.installation());
        assertEquals(first.context, candidate.context());
        assertEquals(first.header, candidate.header());
        assertFalse(candidate.reference().isBlank());
        assertThrows(
            java.io.IOException.class,
            () ->
                second.sessions.reclaimOrphan(second.inputs, candidate, ResultFixture.clock(1100)));
        InputStore.OrphanCandidate altered =
            new InputStore.OrphanCandidate(
                candidate.installation(),
                candidate.funding(),
                candidate.context(),
                candidate.header(),
                candidate.reference() + ".changed");
        InputStore.Usage retained = first.inputs.usage();
        assertThrows(
            java.io.IOException.class,
            () -> first.sessions.reclaimOrphan(first.inputs, altered, ResultFixture.clock(1100)));
        assertEquals(retained, first.inputs.usage());
      }
      assertEquals(2, first.candidates().size());
      assertEquals(2, second.candidates().size());
    }
  }

  private static Records.InputHeader header(
      OrphanStoreTest.Fixture fixture, int operation, long entity, byte[] payload)
      throws Exception {
    return new Records.InputHeader(
        fixture.binding.generation(),
        ResultFixture.operation(operation),
        new Records.AdmitParameters(
            new Records.WorkKey(0, 0, entity),
            new Records.Input(
                payload.length, ResultFixture.digest(payload), "application/octet-stream"),
            "copy",
            0,
            1000,
            new Records.OutputBudget(0, 0)));
  }

  private static void install(
      OrphanStoreTest.Fixture fixture, Records.InputHeader header, byte[] payload)
      throws Exception {
    try (InputStore.Receiver receiver =
        fixture.inputs.begin(fixture.context, header, ResultFixture.SELECTED, 4)) {
      receiver.write(ByteBuffer.wrap(payload), 5);
      receiver.finish(6);
    }
  }

  private static List<InputStore.OrphanCandidate> drain(InputStore.OrphanScan scan)
      throws Exception {
    List<InputStore.OrphanCandidate> found = new ArrayList<>();
    for (int pages = 0; pages < 128; pages++) {
      InputStore.OrphanPage page = scan.nextPage(1);
      found.addAll(page.candidates());
      if (page.done()) return found;
    }
    return fail("orphan scan did not finish within retained file-policy bound");
  }

  private static Set<String> references(List<InputStore.OrphanCandidate> candidates) {
    Set<String> references = new HashSet<>();
    for (InputStore.OrphanCandidate candidate : candidates) {
      assertTrue(references.add(candidate.reference()));
    }
    return references;
  }
}
