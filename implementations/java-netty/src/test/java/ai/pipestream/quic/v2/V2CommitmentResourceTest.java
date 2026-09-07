package ai.pipestream.quic.v2;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

final class V2CommitmentResourceTest {
  @TempDir Path directory;

  @Test
  void wholeScopeExceedsChildHeapButStreamingCommitmentsComplete() throws Exception {
    Path log = directory.resolve("fold.log");
    Process child =
        new ProcessBuilder(
                Path.of(System.getProperty("java.home"), "bin/java").toString(),
                "-Xmx24m",
                "-XX:+UseSerialGC",
                "-cp",
                System.getProperty("java.class.path"),
                V2CommitmentResourceTest.class.getName())
            .redirectErrorStream(true)
            .redirectOutput(log.toFile())
            .start();
    try {
      assertTrue(child.waitFor(60, TimeUnit.SECONDS), "bounded commitment probe timed out");
      String output = Files.readString(log);
      assertEquals(0, child.exitValue(), output);
      assertTrue(output.contains("members=4000003"), output);
      assertTrue(output.contains("largestFrontierBytes=672"), output);
      System.out.print(output);
    } finally {
      if (child.isAlive()) {
        child.destroyForcibly();
        child.waitFor();
      }
    }
  }

  public static void main(String[] args) throws Exception {
    long count = 4_000_003;
    long maxHeap = Runtime.getRuntime().maxMemory();
    if (count * Long.BYTES <= maxHeap) throw new AssertionError("fixture membership fits in heap");
    Commitments.Seal seal = new Commitments.Seal(V2CommitmentsTest.CONTEXT, 0, 0, null, count);
    Commitments.StatusTree status = new Commitments.StatusTree(0, 0, count);
    int maximum = 0;
    long start = System.nanoTime();
    for (long id = 1; id <= count; id++) {
      seal.add(id);
      status.add(V2CommitmentsTest.terminal(id), null);
      maximum = Math.max(maximum, status.retainedHashBytes());
    }
    Records.Digest membership = seal.finish();
    Commitments.Status result = status.finish();
    if (result.counts().total() != count || maximum > 63 * 32)
      throw new AssertionError("invalid fold accounting");
    String processMemory =
        Files.readAllLines(Path.of("/proc/self/status")).stream()
            .filter(s -> s.startsWith("VmRSS:") || s.startsWith("VmHWM:"))
            .reduce("", (left, right) -> left + right + "\n");
    System.out.println(
        "members="
            + count
            + " primitiveMembershipBytes="
            + count * Long.BYTES
            + " maxHeapBytes="
            + maxHeap
            + " largestFrontierBytes="
            + maximum
            + " elapsedMs="
            + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start)
            + " seal="
            + membership
            + " status="
            + result.root());
    System.out.print(processMemory);
  }
}
