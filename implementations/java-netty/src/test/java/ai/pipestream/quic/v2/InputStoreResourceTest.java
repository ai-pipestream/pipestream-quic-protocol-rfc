package ai.pipestream.quic.v2;

import static ai.pipestream.quic.v2.Messages.DURABLE_WORK;
import static org.junit.jupiter.api.Assertions.*;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

@Timeout(45)
final class InputStoreResourceTest {
  private static final long LENGTH = 64L << 20;
  private static final int CHUNK = 8192;
  private static final InputStore.Limits LIMITS = new InputStore.Limits(160L << 20, 4, LENGTH, 2);
  private static final Messages.Capabilities SELECTED =
      new Messages.Capabilities(
          true, List.of(DURABLE_WORK), List.of(), 65536, 4, 16, LENGTH, 1000, 300_000);

  @TempDir Path directory;

  @Test
  void sixtyFourMiBStreamsThroughThirtyTwoMiBHeapAndReopensExactly() throws Exception {
    Path root = directory.resolve("store");
    Path output = directory.resolve("child.out");
    Process process =
        new ProcessBuilder(
                Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-Xmx32m",
                "-cp",
                System.getProperty("java.class.path"),
                InputStoreResourceTest.class.getName(),
                root.toString())
            .redirectErrorStream(true)
            .redirectOutput(output.toFile())
            .start();
    try {
      assertTrue(process.waitFor(35, TimeUnit.SECONDS), "resource child did not finish");
      String report = Files.readString(output);
      assertEquals(0, process.exitValue(), report);
      System.out.print(report);
      assertTrue(report.contains("bytes=67108864"), report);
      assertTrue(report.contains("digest="), report);
      long heapMax = field(report, "heapMax");
      assertTrue(heapMax > 0 && heapMax <= 32L << 20, report);
      assertTrue(field(report, "observedRssKiB") > 0, report);
    } finally {
      if (process.isAlive()) {
        process.destroyForcibly();
        assertTrue(process.waitFor(2, TimeUnit.SECONDS));
      }
    }
  }

  public static void main(String[] args) throws Exception {
    Path root = Path.of(args[0]);
    Records.Digest expected = digest();
    Records.InputHeader header =
        new Records.InputHeader(
            1,
            operation(),
            new Records.AdmitParameters(
                new Records.WorkKey(0, 0, 1),
                new Records.Input(LENGTH, expected, "application/octet-stream"),
                "resource",
                0,
                300_000,
                new Records.OutputBudget(0, 0)));
    Commitments.Context context = new Commitments.Context("issuer-a", "alice", 1);
    try (InputStore store = InputStore.initialize(root, LIMITS);
        InputStore.Receiver receiver = store.begin(context, header, SELECTED, 0)) {
      for (long offset = 0; offset < LENGTH; offset += CHUNK)
        receiver.write(ByteBuffer.wrap(chunk(offset)), offset + 1);
      receiver.finish(LENGTH + 1);
    }

    MessageDigest observed = MessageDigest.getInstance("SHA-256");
    long bytes = 0;
    try (InputStore reopened = InputStore.open(root, LIMITS);
        var input = reopened.find(context, header).orElseThrow().openStream()) {
      byte[] buffer = new byte[CHUNK];
      for (int read; (read = input.read(buffer)) != -1; ) {
        observed.update(buffer, 0, read);
        bytes += read;
      }
    }
    assertEquals(LENGTH, bytes);
    assertArrayEquals(expected.bytes(), observed.digest());
    System.out.printf(
        "bytes=%d digest=%s heapMax=%d observedRssKiB=%d%n",
        bytes,
        HexFormat.of().formatHex(expected.bytes()),
        Runtime.getRuntime().maxMemory(),
        rssKiB());
  }

  private static Records.Digest digest() throws Exception {
    MessageDigest digest = MessageDigest.getInstance("SHA-256");
    for (long offset = 0; offset < LENGTH; offset += CHUNK) digest.update(chunk(offset));
    return new Records.Digest(digest.digest());
  }

  private static byte[] chunk(long offset) {
    byte[] bytes = new byte[CHUNK];
    for (int index = 0; index < bytes.length; index++)
      bytes[index] = (byte) ((offset + index) * 31 + 17);
    return bytes;
  }

  private static Records.OperationId operation() {
    byte[] bytes = new byte[16];
    bytes[15] = 1;
    return new Records.OperationId(bytes);
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
}
