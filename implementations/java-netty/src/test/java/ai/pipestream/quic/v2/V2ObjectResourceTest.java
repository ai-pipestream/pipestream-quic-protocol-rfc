package ai.pipestream.quic.v2;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HexFormat;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

final class V2ObjectResourceTest {
  @TempDir Path directory;

  @Test
  void payloadLargerThanTheJvmHeapNeedsOnlyIncrementalBuffers() throws Exception {
    Path log = directory.resolve("object.log");
    Process child =
        new ProcessBuilder(
                Path.of(System.getProperty("java.home"), "bin/java").toString(),
                "-Xmx24m",
                "-XX:+UseSerialGC",
                "-cp",
                System.getProperty("java.class.path"),
                V2ObjectResourceTest.class.getName())
            .redirectErrorStream(true)
            .redirectOutput(log.toFile())
            .start();
    try {
      assertTrue(child.waitFor(60, TimeUnit.SECONDS), "isolated object verification timed out");
      String output = Files.readString(log);
      assertEquals(0, child.exitValue(), output);
      assertTrue(output.contains("payloadBytes=67108864 chunkBytes=8192 verified=true"), output);
      System.out.print(output);
    } finally {
      if (child.isAlive()) {
        child.destroyForcibly();
        child.waitFor();
      }
    }
  }

  public static void main(String[] args) throws Exception {
    long length = 64L * 1024 * 1024;
    long maximum = Runtime.getRuntime().maxMemory();
    if (maximum >= length) throw new AssertionError("payload fits in the test heap");
    // Independently checked with: head -c 67108864 /dev/zero | sha256sum
    Records.Digest digest =
        new Records.Digest(
            HexFormat.of()
                .parseHex("3b6a07d0d404fab4e23b6d34bc6696a6a312dd92821332385e5af7c01c421351"));
    long start = System.nanoTime();
    ObjectStream.Payload payload =
        new ObjectStream.Payload(
            length, digest, V2ObjectStreamTest.selected(length, 1000, 5000), start);
    ByteBuffer chunk = ByteBuffer.allocateDirect(8192);
    for (long received = 0; received < length; received += chunk.capacity()) {
      chunk.clear();
      payload.feed(chunk, System.nanoTime());
      if (payload.verified()) throw new AssertionError("payload trusted before actual FIN");
    }
    if (payload.consumed() != length
        || !payload.finish(System.nanoTime()).equals(digest)
        || !payload.verified()) throw new AssertionError("unverified payload");
    System.out.println(
        "payloadBytes="
            + length
            + " chunkBytes="
            + chunk.capacity()
            + " verified=true maxHeapBytes="
            + maximum
            + " elapsedMs="
            + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start));
    Files.readAllLines(Path.of("/proc/self/status")).stream()
        .filter(s -> s.startsWith("VmRSS:") || s.startsWith("VmHWM:"))
        .forEach(System.out::println);
  }
}
