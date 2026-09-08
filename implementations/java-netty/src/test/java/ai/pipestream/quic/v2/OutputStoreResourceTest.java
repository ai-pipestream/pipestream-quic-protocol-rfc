package ai.pipestream.quic.v2;

import static org.junit.jupiter.api.Assertions.*;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

@Timeout(50)
final class OutputStoreResourceTest {
  private static final long LENGTH = 64L << 20;
  private static final int CHUNK = 8192;
  private static final UUID AUTHORITY = UUID.fromString("10000000-0000-0000-0000-000000000001");
  private static final InputStore.Limits LIMITS = new InputStore.Limits(160L << 20, 8, LENGTH, 2);

  @TempDir Path directory;

  @Test
  void sixtyFourMiBOutputStreamsThroughThirtyTwoMiBHeapAndReopensExactly() throws Exception {
    Path root = directory.resolve("store");
    Path output = directory.resolve("child.out");
    Process process =
        new ProcessBuilder(
                Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-Xmx32m",
                "-cp",
                System.getProperty("java.class.path"),
                OutputStoreResourceTest.class.getName(),
                root.toString())
            .redirectErrorStream(true)
            .redirectOutput(output.toFile())
            .start();
    try {
      assertTrue(process.waitFor(40, TimeUnit.SECONDS), "output resource child did not finish");
      String report = Files.readString(output);
      assertEquals(0, process.exitValue(), report);
      System.out.print(report);
      assertTrue(report.contains("bytes=67108864"), report);
      assertTrue(report.contains("digest="), report);
      long heapMax = field(report, "heapMax");
      assertTrue(heapMax > 0 && heapMax <= 32L << 20, report);
      assertTrue(field(report, "observedRssKiB") > 0, report);
      assertTrue(field(report, "fundedBytes") >= 2 * LENGTH, report);
      assertEquals(3, field(report, "fundedFiles"), report);
    } finally {
      if (process.isAlive()) {
        process.destroyForcibly();
        assertTrue(process.waitFor(2, TimeUnit.SECONDS));
      }
    }
  }

  public static void main(String[] args) throws Exception {
    Path root = Path.of(args[0]);
    Records.InputHeader header = header();
    Commitments.Context context = new Commitments.Context("issuer-a", "alice", 1);
    ExecutionStore.Lease lease =
        new ExecutionStore.Lease(
            AUTHORITY, "alice", 1, new Records.WorkKey(0, 0, 1), 1, 1, 300_000);
    Records.Digest expected = digest();
    InputStore.Usage funded;
    try (InputStore store = InputStore.initializeForAuthority(root, LIMITS, AUTHORITY)) {
      store.reserveOutputs(context, header);
      funded = store.usage();
      try (OutputStore.Writer writer =
          store.beginOutput(
              context, header, lease, 0, LENGTH, "application/octet-stream", LENGTH)) {
        byte[] chunk = new byte[CHUNK];
        for (long offset = 0; offset < LENGTH; offset += CHUNK) {
          fill(chunk, offset);
          writer.write(ByteBuffer.wrap(chunk));
        }
        assertEquals(expected, writer.finish().sha256());
      }
      assertEquals(funded, store.usage());
      assertEquals(0, store.usage().handles());
      assertManaged(root, funded);
    }

    MessageDigest observed = MessageDigest.getInstance("SHA-256");
    long bytes = 0;
    try (InputStore reopened = InputStore.open(root, LIMITS)) {
      assertEquals(funded, reopened.usage());
      assertManaged(root, funded);
      try (var input = reopened.findOutput(context, header, lease, 0).orElseThrow().openStream()) {
        InputStore.Usage active = reopened.usage();
        assertEquals(funded.bytes(), active.bytes());
        assertEquals(funded.files(), active.files());
        assertEquals(1, active.handles());
        byte[] buffer = new byte[CHUNK];
        for (int read; (read = input.read(buffer)) != -1; ) {
          observed.update(buffer, 0, read);
          bytes += read;
        }
      }
      assertEquals(funded, reopened.usage());
    }
    assertEquals(LENGTH, bytes);
    assertArrayEquals(expected.bytes(), observed.digest());
    System.out.printf(
        "bytes=%d digest=%s buffer=%d heapMax=%d observedRssKiB=%d fundedBytes=%d fundedFiles=%d"
            + " managedBytes=%d managedFiles=%d%n",
        bytes,
        HexFormat.of().formatHex(expected.bytes()),
        CHUNK,
        Runtime.getRuntime().maxMemory(),
        rssKiB(),
        funded.bytes(),
        funded.files(),
        managed(root)[0],
        managed(root)[1]);
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
            "resource",
            0,
            300_000,
            new Records.OutputBudget(1, LENGTH)));
  }

  private static Records.Digest digest() throws Exception {
    MessageDigest digest = MessageDigest.getInstance("SHA-256");
    byte[] chunk = new byte[CHUNK];
    for (long offset = 0; offset < LENGTH; offset += CHUNK) {
      fill(chunk, offset);
      digest.update(chunk);
    }
    return new Records.Digest(digest.digest());
  }

  private static Records.Digest digest(byte[] bytes) throws Exception {
    return new Records.Digest(MessageDigest.getInstance("SHA-256").digest(bytes));
  }

  private static void fill(byte[] bytes, long offset) {
    for (int index = 0; index < bytes.length; index++)
      bytes[index] = (byte) ((offset + index) * 31 + 17);
  }

  private static long field(String report, String name) {
    java.util.regex.Matcher value =
        java.util.regex.Pattern.compile("(?:^|\\s)" + name + "=([0-9]+)").matcher(report);
    assertTrue(value.find(), report);
    return Long.parseLong(value.group(1));
  }

  private static long rssKiB() throws Exception {
    try (BufferedReader status =
        new BufferedReader(
            new InputStreamReader(Files.newInputStream(Path.of("/proc/self/status"))))) {
      for (String line; (line = status.readLine()) != null; ) {
        if (line.startsWith("VmRSS:")) return Long.parseLong(line.replaceAll("[^0-9]", ""));
      }
    }
    throw new AssertionError("VmRSS absent from /proc/self/status");
  }

  private static void assertManaged(Path root, InputStore.Usage funded) throws Exception {
    long[] managed = managed(root);
    assertTrue(managed[0] <= funded.bytes(), "managed bytes exceed funded allowance");
    assertTrue(managed[1] <= funded.files(), "managed files exceed funded allowance");
  }

  private static long[] managed(Path root) throws Exception {
    long bytes = 0, files = 0;
    for (String namespace :
        List.of("reservations", "outputs", "output-pending", "objects", "pending")) {
      Path path = root.resolve(namespace);
      if (!Files.exists(path)) continue;
      try (var paths = Files.walk(path)) {
        for (Path file : paths.filter(Files::isRegularFile).toList()) {
          bytes += Files.size(file);
          files++;
        }
      }
    }
    return new long[] {bytes, files};
  }
}
