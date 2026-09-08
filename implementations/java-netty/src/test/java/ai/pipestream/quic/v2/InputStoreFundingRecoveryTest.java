package ai.pipestream.quic.v2;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

@Timeout(30)
final class InputStoreFundingRecoveryTest {
  private static final InputStore.Limits LIMITS = new InputStore.Limits(1 << 20, 32, 1024, 4);
  private static final Commitments.Context CONTEXT =
      new Commitments.Context("issuer-a", "alice", 1);

  @TempDir Path directory;

  @Test
  void processDeathAtEachFundingBoundaryRecoversOnlyAuthoritativeLinks() throws Exception {
    for (InputStore.Phase phase :
        List.of(
            InputStore.Phase.FUNDING_RECEIVED,
            InputStore.Phase.FUNDING_LINKED,
            InputStore.Phase.FUNDING_SYNCED,
            InputStore.Phase.FUNDING_STAGING_REMOVED)) {
      Path root = directory.resolve(phase.name());
      int exit = 61 + phase.ordinal();
      assertEquals(exit, runChild(root, phase, exit), Files.readString(error(root)));

      Records.InputHeader header = header();
      try (InputStore store = InputStore.open(root, LIMITS)) {
        boolean retained = phase != InputStore.Phase.FUNDING_RECEIVED;
        assertEquals(retained, store.findReservation(CONTEXT, header).isPresent());
        assertEquals(0, count(root.resolve("pending"), ".part"));
        if (retained) {
          Path record = only(root.resolve("reservations"), ".funding");
          long expected = Files.size(record) + 2 * (29 + 2 * 8236L);
          assertEquals(new InputStore.Usage(expected, 5, 0), store.usage());
          InputStore.Usage before = store.usage();
          assertEquals(
              record.getFileName().toString(), store.reserveOutputs(CONTEXT, header).reference());
          assertEquals(before, store.usage());
        } else {
          assertEquals(new InputStore.Usage(0, 0, 0), store.usage());
          assertNotNull(store.reserveOutputs(CONTEXT, header).reference());
        }
      }
    }
  }

  public static void main(String[] args) throws Exception {
    Path root = Path.of(args[0]);
    InputStore.Phase target = InputStore.Phase.valueOf(args[1]);
    int exit = Integer.parseInt(args[2]);
    InputStore store =
        InputStore.initialize(
            root,
            LIMITS,
            phase -> {
              if (phase == target) Runtime.getRuntime().halt(exit);
            });
    store.reserveOutputs(CONTEXT, header());
    throw new AssertionError("funding probe was not reached");
  }

  private int runChild(Path root, InputStore.Phase phase, int exit) throws Exception {
    Process process =
        new ProcessBuilder(
                Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-cp",
                System.getProperty("java.class.path"),
                InputStoreFundingRecoveryTest.class.getName(),
                root.toString(),
                phase.name(),
                Integer.toString(exit))
            .redirectOutput(output(root).toFile())
            .redirectError(error(root).toFile())
            .start();
    try {
      assertTrue(process.waitFor(8, TimeUnit.SECONDS), "funding child did not exit");
      return process.exitValue();
    } finally {
      if (process.isAlive()) {
        process.destroyForcibly();
        assertTrue(process.waitFor(2, TimeUnit.SECONDS));
      }
    }
  }

  private static Records.InputHeader header() throws Exception {
    byte[] operation = new byte[16];
    operation[15] = 1;
    return new Records.InputHeader(
        1,
        new Records.OperationId(operation),
        new Records.AdmitParameters(
            new Records.WorkKey(0, 0, 1),
            new Records.Input(
                0,
                new Records.Digest(MessageDigest.getInstance("SHA-256").digest(new byte[0])),
                "application/octet-stream"),
            "copy",
            0,
            1000,
            new Records.OutputBudget(2, 29)));
  }

  private static int count(Path directory, String suffix) throws Exception {
    int count = 0;
    try (DirectoryStream<Path> entries = Files.newDirectoryStream(directory, "*" + suffix)) {
      for (Path ignored : entries) count++;
    }
    return count;
  }

  private static Path only(Path directory, String suffix) throws Exception {
    try (DirectoryStream<Path> entries = Files.newDirectoryStream(directory, "*" + suffix)) {
      var iterator = entries.iterator();
      assertTrue(iterator.hasNext());
      Path result = iterator.next();
      assertFalse(iterator.hasNext());
      return result;
    }
  }

  private Path output(Path root) {
    return directory.resolve(root.getFileName() + ".out");
  }

  private Path error(Path root) {
    return directory.resolve(root.getFileName() + ".err");
  }
}
