package ai.pipestream.quic.storage;

import static org.junit.jupiter.api.Assertions.*;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The layout probe is the pre-landing gate of the storage SPI: its report on
 * the fixed script must byte-match {@code expected-report-v1.txt} on every
 * backend. This class pins the two docker-free backends and both run modes;
 * {@link ProbeDockerTest} pins the docker-backed ones against the same bytes.
 * Report mode is the crossed-store gate: a filesystem store written by one
 * lane must verify clean and render the identical report under the other.
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

  @Test void filesystemReportMatchesExpectedReportOnExistingState() throws Exception {
    Map<String, String> config = Probe.config("PROBE_FS_ROOT", root.toString());
    assertReport("fs", config);
    assertArrayEquals(Files.readAllBytes(EXPECTED),
        runProbe(config, "fs", "--report").toByteArray(),
        "report mode on the same store must byte-match the script-execution report");
  }

  @Test void filesystemReportAcceptsExplicitScopesInAnyOrder() throws Exception {
    Map<String, String> config = Probe.config("PROBE_FS_ROOT", root.toString());
    assertReport("fs", config);
    assertArrayEquals(Files.readAllBytes(EXPECTED),
        runProbe(config, "fs", "--report", "00000000/00000008", "00000000").toByteArray(),
        "explicit scopes render in canonical ascending order");
  }

  @Test void filesystemReportRejectsUninitializedRoot() throws Exception {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    ByteArrayOutputStream err = new ByteArrayOutputStream();
    int code = Probe.execute(
        new String[] {"fs", "--report"},
        Probe.config("PROBE_FS_ROOT", root.resolve("missing").toString()),
        new PrintStream(out, true, StandardCharsets.UTF_8),
        new PrintStream(err, true, StandardCharsets.UTF_8));
    assertEquals(Probe.USAGE, code, err.toString(StandardCharsets.UTF_8));
  }

  @Test void memoryReportDocumentsEmptyStateWithExplicitScopes() throws Exception {
    // The in-memory backend has no directory tree to walk, so report mode
    // takes explicit scopes and reports a fresh (empty) store; the crossed
    // verification gate is filesystem-only.
    String empty = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
    String expected = "scope 00000000\n  content-digest " + empty + "\n"
        + "scope 00000000/00000008\n  content-digest " + empty + "\n"
        + "journal\n";
    assertArrayEquals(expected.getBytes(StandardCharsets.UTF_8),
        runProbe(Probe.config(), "memory", "--report", "00000000", "00000000/00000008").toByteArray());
  }

  static void assertReport(String backend, Map<String, String> config) throws Exception {
    assertArrayEquals(Files.readAllBytes(EXPECTED),
        runProbe(config, backend, SCRIPT.toString()).toByteArray(),
        "probe report must byte-match the canonical report");
  }

  static ByteArrayOutputStream runProbe(Map<String, String> config, String... args) throws Exception {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    ByteArrayOutputStream err = new ByteArrayOutputStream();
    int code = Probe.execute(args, config,
        new PrintStream(out, true, StandardCharsets.UTF_8),
        new PrintStream(err, true, StandardCharsets.UTF_8));
    assertEquals(Probe.OK, code, err.toString(StandardCharsets.UTF_8));
    return out;
  }
}
