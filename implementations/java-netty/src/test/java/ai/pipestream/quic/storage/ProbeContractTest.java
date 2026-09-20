package ai.pipestream.quic.storage;

import static org.junit.jupiter.api.Assertions.*;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The layout probe is the pre-landing gate of the storage SPI: its report on
 * the fixed script must byte-match {@code expected-report-v1.txt} on every
 * backend. This class pins the two docker-free backends; {@link ProbeDockerTest}
 * pins the docker-backed ones against the same bytes.
 */
final class ProbeContractTest {
  private static final Path SCRIPT = Path.of("../../test-vectors/storage-spi/probe-script-v1.txt");
  private static final Path EXPECTED = Path.of("../../test-vectors/storage-spi/expected-report-v1.txt");

  @TempDir Path root;

  @Test void memoryProbeMatchesExpectedReport() throws Exception {
    assertReport("memory", Probe.config());
  }

  @Test void filesystemProbeMatchesExpectedReport() throws Exception {
    assertReport("fs", Probe.config("PROBE_FS_ROOT", root.toString()));
  }

  static void assertReport(String backend, java.util.Map<String, String> config) throws Exception {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    ByteArrayOutputStream err = new ByteArrayOutputStream();
    int code = Probe.execute(
        new String[] {backend, SCRIPT.toString()}, config, new PrintStream(out, true, StandardCharsets.UTF_8),
        new PrintStream(err, true, StandardCharsets.UTF_8));
    assertEquals(Probe.OK, code, err.toString(StandardCharsets.UTF_8));
    assertArrayEquals(Files.readAllBytes(EXPECTED), out.toByteArray(),
        "probe report must byte-match the canonical report");
  }
}
