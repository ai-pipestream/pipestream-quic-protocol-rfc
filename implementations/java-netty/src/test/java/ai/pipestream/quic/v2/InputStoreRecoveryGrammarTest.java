package ai.pipestream.quic.v2;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Characterization tests pinning the v2 input store's recovery name grammar
 * (adoption plan stage 1): ROOT_NAMES, the {@code [0-9a-f]{64}.input} object
 * and {@code [0-9a-f]{64}.funding} reservation rules, the UUID {@code .part}
 * staging rule, and {@code outputFunding}'s reference validation. These tests
 * assert today's behavior unmodified; they are the regression net for every
 * later object-plane change.
 */
final class InputStoreRecoveryGrammarTest {
  private static final InputStore.Limits LIMITS = new InputStore.Limits(16L << 20, 32, 8 << 20, 8);
  private static final String HEX64 = "0123456789abcdef".repeat(4);

  @TempDir Path directory;

  @Test void initializedRootContainsExactlyThePinnedEntries() throws Exception {
    Path root = initialize("exact");
    try (InputStore store = InputStore.open(root, LIMITS)) {
      assertNotNull(store);
      List<String> names;
      try (var entries = Files.newDirectoryStream(root)) {
        names = java.util.stream.StreamSupport.stream(entries.spliterator(), false)
            .map(path -> path.getFileName().toString())
            .sorted()
            .toList();
      }
      assertEquals(
          List.of("objects", "output-pending", "outputs", "pending", "policy.cbor", "reservations", "writer.lock"),
          names);
      for (String name : List.of("pending", "objects", "reservations", "outputs", "output-pending")) {
        assertTrue(Files.isDirectory(root.resolve(name)), name + " is a directory");
      }
      assertTrue(Files.isRegularFile(root.resolve("writer.lock")), "writer.lock is a file");
      assertTrue(Files.isRegularFile(root.resolve("policy.cbor")), "policy.cbor is a file");
    }
  }

  @Test void foreignRootEntryRefusesOpen() throws Exception {
    Path root = initialize("foreign");
    Files.writeString(root.resolve("foreign.bin"), "junk");
    assertOpenRefused(root, "foreign input-store entry");
  }

  @Test void missingRootEntryRefusesOpen() throws Exception {
    Path root = initialize("incomplete");
    deleteRecursively(root.resolve("reservations"));
    assertOpenRefused(root, "incomplete input-store installation");
  }

  @Test void rootEntryWithWrongFileTypeRefusesOpen() throws Exception {
    Path root = initialize("wrong-type");
    deleteRecursively(root.resolve("objects"));
    Files.write(root.resolve("objects"), new byte[] {1});
    assertOpenRefused(root, "wrong file type");
  }

  @Test void malformedObjectNamesRefuseOpen() throws Exception {
    for (String name : new String[] {
        HEX64.toUpperCase() + ".input", // grammar is lowercase-only
        HEX64.substring(1) + ".input", // wrong length
        HEX64 + ".part", // staging suffix outside pending/
        HEX64 + ".input2", // unknown suffix
        "object.input"}) {
      Path root = initialize("object-" + name.length() + name.charAt(0));
      Files.write(root.resolve("objects").resolve(name), new byte[] {1});
      assertOpenRefused(root, "unknown input-store file");
    }
  }

  @Test void malformedReservationNamesRefuseOpen() throws Exception {
    for (String name : new String[] {
        HEX64.toUpperCase() + ".funding", HEX64.substring(2) + ".funding", HEX64 + ".funding2"}) {
      Path root = initialize("reservation-" + name.length());
      Files.write(root.resolve("reservations").resolve(name), new byte[] {1});
      assertOpenRefused(root, "unknown input-store file");
    }
  }

  @Test void malformedStagingNamesRefuseOpen() throws Exception {
    for (String name : new String[] {"staging.tmp", "not-a-uuid.part", UUID.randomUUID() + ".bin"}) {
      Path root = initialize("staging-" + name.length());
      Files.write(root.resolve("pending").resolve(name), new byte[] {1});
      assertOpenRefused(root, "unknown input-store file");
    }
  }

  @Test void wellFormedObjectNameWithGarbageBodyRefusesOpen() throws Exception {
    Path root = initialize("garbage-body");
    Files.write(root.resolve("objects").resolve(HEX64 + ".input"), new byte[] {1, 2, 3});
    // The name passes the grammar; the body must still fail the identity audit.
    assertThrows(IOException.class, () -> InputStore.open(root, LIMITS));
  }

  @Test void wellFormedStagingPartsAreSweptAfterAudit() throws Exception {
    Path root = initialize("swept");
    Files.write(root.resolve("pending").resolve(UUID.randomUUID() + ".part"), new byte[] {1});
    Files.write(root.resolve("pending").resolve(UUID.randomUUID() + ".part"), new byte[0]);
    try (InputStore store = InputStore.open(root, LIMITS)) {
      assertNotNull(store);
      try (var entries = Files.newDirectoryStream(root.resolve("pending"))) {
        assertFalse(entries.iterator().hasNext(), "staging is swept once recovery completes");
      }
    }
  }

  @Test void outputFundingValidatesTheReferenceGrammar() throws Exception {
    Path root = initialize("funding-grammar");
    try (InputStore store = InputStore.open(root, LIMITS)) {
      for (String reference : new String[] {
          null, "", "x", HEX64, HEX64.substring(1) + ".funding", HEX64 + ".funding2",
          HEX64.toUpperCase() + ".funding"}) {
        IOException failure = assertThrows(IOException.class, () -> store.outputFunding(reference));
        assertTrue(failure.getMessage().contains("invalid output funding reference"),
            () -> reference + ": " + failure.getMessage());
      }
      // A grammar-valid reference fails at file lookup instead.
      IOException missing = assertThrows(IOException.class, () -> store.outputFunding(HEX64 + ".funding"));
      assertFalse(missing.getMessage().contains("invalid output funding reference"));
    }
  }

  private Path initialize(String name) throws IOException {
    Path root = directory.resolve(name);
    try (InputStore store = InputStore.initialize(root, LIMITS)) {
      assertNotNull(store);
      return root;
    }
  }

  private static void assertOpenRefused(Path root, String phrase) {
    IOException failure = assertThrows(IOException.class, () -> InputStore.open(root, LIMITS));
    assertTrue(failure.getMessage().contains(phrase), failure.getMessage());
  }

  private static void deleteRecursively(Path directory) throws IOException {
    try (var entries = Files.newDirectoryStream(directory)) {
      for (Path entry : entries) {
        if (Files.isDirectory(entry)) deleteRecursively(entry);
        else Files.delete(entry);
      }
    }
    Files.delete(directory);
  }
}
