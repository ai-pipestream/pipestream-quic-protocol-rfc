package ai.pipestream.quic.v2;

import static org.junit.jupiter.api.Assertions.*;

import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

@Timeout(30)
final class OutputStoreRecoveryTest {
  private static final InputStore.Limits LIMITS = new InputStore.Limits(4L << 20, 32, 1 << 20, 4);
  private static final UUID AUTHORITY = UUID.fromString("10000000-0000-0000-0000-000000000001");
  private static final Commitments.Context CONTEXT =
      new Commitments.Context("issuer-a", "alice", 1);
  private static final byte[] PAYLOAD = {1, 2, 3, 4, 5};

  @TempDir Path directory;

  @Test
  void processDeathAtEachOutputInstallationBoundaryRecoversOnlyInstalledObjects() throws Exception {
    for (InputStore.Phase phase :
        List.of(
            InputStore.Phase.OUTPUT_RECEIVED,
            InputStore.Phase.OUTPUT_LINKED,
            InputStore.Phase.OUTPUT_SYNCED,
            InputStore.Phase.OUTPUT_STAGING_REMOVED)) {
      Path root = directory.resolve(phase.name());
      InputStore.Usage funded;
      try (InputStore store = InputStore.initializeForAuthority(root, LIMITS, AUTHORITY)) {
        store.reserveOutputs(CONTEXT, header());
        funded = store.usage();
      }
      int exit = 91 + phase.ordinal();
      assertEquals(exit, runChild(root, phase, exit), Files.readString(error(root)));

      try (InputStore store = InputStore.open(root, LIMITS)) {
        boolean installed = phase != InputStore.Phase.OUTPUT_RECEIVED;
        assertEquals(installed, store.findOutput(CONTEXT, header(), lease(), 0).isPresent());
        assertEquals(0, count(root.resolve("output-pending")));
        assertEquals(funded, store.usage());
        assertEquals(0, store.usage().handles());
        if (installed) {
          OutputStore.Stored output = store.findOutput(CONTEXT, header(), lease(), 0).orElseThrow();
          assertEquals(PAYLOAD.length, output.length());
          assertEquals(digest(PAYLOAD), output.sha256());
          try (InputStream stream = output.openStream()) {
            assertArrayEquals(PAYLOAD, stream.readAllBytes());
          }
          assertEquals(funded, store.usage());
        }
      }
    }
  }

  public static void main(String[] args) throws Exception {
    Path root = Path.of(args[0]);
    InputStore.Phase target = InputStore.Phase.valueOf(args[1]);
    int exit = Integer.parseInt(args[2]);
    InputStore store =
        InputStore.open(
            root,
            LIMITS,
            phase -> {
              if (phase == target) Runtime.getRuntime().halt(exit);
            });
    try (OutputStore.Writer writer =
        store.beginOutput(
            CONTEXT,
            header(),
            lease(),
            0,
            PAYLOAD.length,
            "application/octet-stream",
            PAYLOAD.length)) {
      writer.write(ByteBuffer.wrap(PAYLOAD));
      writer.finish();
    }
    throw new AssertionError("output installation probe was not reached");
  }

  private int runChild(Path root, InputStore.Phase phase, int exit) throws Exception {
    Process process =
        new ProcessBuilder(
                Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-cp",
                System.getProperty("java.class.path"),
                OutputStoreRecoveryTest.class.getName(),
                root.toString(),
                phase.name(),
                Integer.toString(exit))
            .redirectOutput(output(root).toFile())
            .redirectError(error(root).toFile())
            .start();
    try {
      assertTrue(process.waitFor(8, TimeUnit.SECONDS), "output child did not exit");
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
            new Records.Input(0, digest(new byte[0]), "application/octet-stream"),
            "copy",
            0,
            1000,
            new Records.OutputBudget(1, PAYLOAD.length)));
  }

  private static ExecutionStore.Lease lease() {
    return new ExecutionStore.Lease(
        AUTHORITY, "alice", 1, new Records.WorkKey(0, 0, 1), 1, 1, 2000);
  }

  private static Records.Digest digest(byte[] payload) throws Exception {
    return new Records.Digest(MessageDigest.getInstance("SHA-256").digest(payload));
  }

  private static int count(Path directory) throws Exception {
    int result = 0;
    try (DirectoryStream<Path> entries = Files.newDirectoryStream(directory)) {
      for (Path ignored : entries) result++;
    }
    return result;
  }

  private Path output(Path root) {
    return directory.resolve(root.getFileName() + ".out");
  }

  private Path error(Path root) {
    return directory.resolve(root.getFileName() + ".err");
  }
}
