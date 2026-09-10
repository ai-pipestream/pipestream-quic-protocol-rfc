package ai.pipestream.quic.v2;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * An open authority with no client connected must not rewrite storage on every scheduler tick.
 * Measured through the kernel's per-process write accounting, so it covers SQLite's WAL-index
 * rewrites, not only bytes the store believes it committed.
 */
@Timeout(60)
final class DurableHostIdleWritesTest {
  @TempDir Path directory;

  @Test
  void idleHostWritesNothingBetweenOperations() throws Exception {
    Path io = Path.of("/proc/self/io");
    assumeTrue(Files.isReadable(io), "Linux per-process I/O accounting required");
    try (DurableHost host =
        DurableHost.initialize(
            directory.resolve("idle"),
            DurableHost.Configuration.defaults("issuer-a", "localhost:7443"),
            ReferenceApplications.all(),
            DurableHost.OwnerPolicy.fromPrincipals(Map::of, false),
            DurableHost.UtcClock.system(true))) {
      assertNotNull(host);
      // Let bootstrap and the first scheduler ticks settle before sampling.
      Thread.sleep(1000);
      long before = writeBytes(io);
      Thread.sleep(3000);
      long after = writeBytes(io);
      // Without the store anchor an idle host rebuilt the 32 KiB WAL index on each of the four
      // store calls per 50 ms scheduler tick: about 7.5 MiB in this window.
      assertTrue(
          after - before < 512 * 1024, "idle host wrote " + (after - before) + " bytes in 3 s");
    }
  }

  private static long writeBytes(Path io) throws IOException {
    for (String line : Files.readAllLines(io)) {
      if (line.startsWith("write_bytes:"))
        return Long.parseLong(line.substring("write_bytes:".length()).trim());
    }
    throw new IOException("write_bytes missing from " + io);
  }
}
