package ai.pipestream.quic.v2;

import static org.junit.jupiter.api.Assertions.*;

import ai.pipestream.quic.BoundedSqlite;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

@Timeout(60)
final class OutputRetentionStoreRecoveryTest {
  private static final byte[] INPUT = {9, 8, 7, 6};
  private static final byte[] OUTPUT = {1, 2, 3};
  private static final long RELEASE_AT = 21_200;

  @TempDir Path directory;

  @Test
  void processDeathAcrossOutputNamesFundingAndQuotaReleaseResumesSafely() throws Exception {
    for (CrashPhase phase : CrashPhase.values()) {
      Path root = directory.resolve(phase.name().toLowerCase());
      Path stdout = directory.resolve(phase.name() + ".out");
      Path stderr = directory.resolve(phase.name() + ".err");
      assertEquals(phase.exit, runChild(phase, root, stdout, stderr), boundedText(stderr, 8192));
      UsageBefore before = report(stdout);

      Path database = root.resolve("fixture.sqlite");
      Path inputsPath = root.resolve("fixture-inputs");
      SessionStore sessions = SessionStore.open(database, ResultFixture.configuration());
      Messages.Binding binding = binding(sessions);
      try (InputStore inputs = InputStore.open(inputsPath, ResultFixture.INPUT_LIMITS)) {
        sessions.verifyInputs(inputs);
        assertEquals(1, before.generation);
        assertEquals(0, inputs.usage().handles());
        assertTrue(inputs.find(context(), header()).isPresent());
        assertEquals(before.inputBytes, Files.size(only(inputsPath.resolve("objects"), ".input")));
        long fundingCharge = before.bytes - before.inputBytes;
        int fundingFiles = before.files - 1;
        assertTrue(fundingCharge > before.fundingBytes);
        assertEquals(3, fundingFiles);

        boolean fundingPresent = fundingPresent(phase);
        assertEquals(
            before.inputBytes + (fundingPresent ? fundingCharge : 0), inputs.usage().bytes());
        assertEquals(1 + (fundingPresent ? fundingFiles : 0), inputs.usage().files());
        assertEquals(
            phase == CrashPhase.INTENT_COMMITTED, any(inputsPath.resolve("outputs"), ".output"));
        assertFalse(any(inputsPath.resolve("output-pending"), ".output"));
        assertEquals(fundingPresent, any(inputsPath.resolve("reservations"), ".funding"));
        if (phase == CrashPhase.INTENT_COMMITTED)
          assertEquals(
              before.outputBytes, Files.size(only(inputsPath.resolve("outputs"), ".output")));
        if (fundingPresent)
          assertEquals(
              before.fundingBytes,
              Files.size(only(inputsPath.resolve("reservations"), ".funding")));
        assertEquals(fundingPresent, inputs.findReservation(context(), header()).isPresent());
        assertOutputState(database, binding, phase != CrashPhase.RELEASE_COMMITTED);

        Records.WorkView retained = current(sessions);
        assertEquals(Records.State.SUCCEEDED, retained.state());
        Records.Manifest manifest = retained.manifest();
        assertNotNull(manifest);
        assertEquals(RELEASE_AT, retained.outputUntil());
        assertEquals(1, manifest.outputs().size());
        Records.Output descriptor = manifest.outputs().get(0);
        assertEquals(OUTPUT.length, descriptor.length());
        assertEquals(ResultFixture.digest(OUTPUT), descriptor.sha256());
        if (fundingPresent)
          assertEquals(
              phase == CrashPhase.INTENT_COMMITTED,
              inputs.findOutput(context(), header(), lease(sessions), 0).isPresent());

        RetentionStore.Result resumed =
            sessions.reclaimOutput(1, ResultFixture.WORK, inputs, ResultFixture.clock(RELEASE_AT));
        assertEquals(
            phase == CrashPhase.RELEASE_COMMITTED
                ? RetentionStore.Result.ALREADY_RELEASED
                : RetentionStore.Result.RELEASED,
            resumed);
        assertEquals(before.inputBytes, inputs.usage().bytes());
        assertEquals(1, inputs.usage().files());
        assertEquals(0, inputs.usage().handles());
        assertTrue(inputs.find(context(), header()).isPresent());
        assertTrue(inputs.findReservation(context(), header()).isEmpty());
        assertFalse(any(inputsPath.resolve("outputs"), ".output"));
        assertFalse(any(inputsPath.resolve("output-pending"), ".output"));
        assertFalse(any(inputsPath.resolve("reservations"), ".funding"));
        assertEquals(manifest, current(sessions).manifest());
        assertOutputState(database, binding, false);
        assertEquals(
            RetentionStore.Result.ALREADY_RELEASED,
            sessions.reclaimOutput(1, ResultFixture.WORK, inputs, ResultFixture.clock(RELEASE_AT)));
      }

      sessions = SessionStore.open(database, ResultFixture.configuration());
      binding = binding(sessions);
      try (InputStore inputs = InputStore.open(inputsPath, ResultFixture.INPUT_LIMITS)) {
        sessions.verifyInputs(inputs);
        assertEquals(before.inputBytes, inputs.usage().bytes());
        assertEquals(1, inputs.usage().files());
        assertEquals(0, inputs.usage().handles());
        assertTrue(inputs.find(context(), header()).isPresent());
        Records.WorkView retained = current(sessions);
        assertEquals(Records.State.SUCCEEDED, retained.state());
        assertEquals(ResultFixture.digest(OUTPUT), retained.manifest().outputs().get(0).sha256());
        assertOutputState(database, binding, false);
        assertEquals(
            RetentionStore.Result.ALREADY_RELEASED,
            sessions.reclaimOutput(1, ResultFixture.WORK, inputs, ResultFixture.clock(RELEASE_AT)));
      }
    }
  }

  public static void main(String[] args) throws Exception {
    CrashPhase phase = CrashPhase.valueOf(args[0]);
    Path root = Path.of(args[1]);
    Files.createDirectories(root);
    try (ResultFixture fixture = new ResultFixture(root, "fixture", INPUT, OUTPUT)) {
      InputStore.Usage usage = fixture.inputs.usage();
      System.out.printf(
          "%d %d %d %d %d %d%n",
          fixture.binding.generation(),
          usage.bytes(),
          usage.files(),
          Files.size(only(fixture.inputsPath.resolve("objects"), ".input")),
          Files.size(only(fixture.inputsPath.resolve("outputs"), ".output")),
          Files.size(only(fixture.inputsPath.resolve("reservations"), ".funding")));
      System.out.flush();

      RetentionStore.Probe metadataProbe =
          observed -> {
            if (phase.metadata == observed) Runtime.getRuntime().halt(phase.exit);
          };
      if (phase.physical != null) {
        fixture.inputs.close();
        fixture.inputs =
            InputStore.open(
                fixture.inputsPath,
                ResultFixture.INPUT_LIMITS,
                observed -> {
                  if (phase.physical == observed) Runtime.getRuntime().halt(phase.exit);
                });
        fixture.sessions.verifyInputs(fixture.inputs);
      }
      fixture.sessions.reclaimOutput(
          fixture.binding.generation(),
          ResultFixture.WORK,
          fixture.inputs,
          ResultFixture.clock(RELEASE_AT),
          metadataProbe);
      throw new AssertionError("output reclamation passed crash boundary " + phase);
    }
  }

  private int runChild(CrashPhase phase, Path root, Path stdout, Path stderr) throws Exception {
    Files.createDirectories(root);
    Process process =
        new ProcessBuilder(
                Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-cp",
                System.getProperty("java.class.path"),
                OutputRetentionStoreRecoveryTest.class.getName(),
                phase.name(),
                root.toString())
            .redirectOutput(stdout.toFile())
            .redirectError(stderr.toFile())
            .start();
    try {
      assertTrue(process.waitFor(10, TimeUnit.SECONDS), "output-retention child did not exit");
      return process.exitValue();
    } finally {
      if (process.isAlive()) {
        process.destroyForcibly();
        assertTrue(process.waitFor(2, TimeUnit.SECONDS));
      }
    }
  }

  private static UsageBefore report(Path stdout) throws Exception {
    String text = boundedText(stdout, 512).trim();
    String[] fields = text.split(" ");
    assertEquals(6, fields.length, text);
    return new UsageBefore(
        Long.parseLong(fields[0]),
        Long.parseLong(fields[1]),
        Integer.parseInt(fields[2]),
        Long.parseLong(fields[3]),
        Long.parseLong(fields[4]),
        Long.parseLong(fields[5]));
  }

  private static String boundedText(Path path, int limit) throws Exception {
    try (InputStream stream = Files.newInputStream(path)) {
      byte[] bytes = stream.readNBytes(limit);
      assertEquals(-1, stream.read(), "child diagnostic exceeded bound");
      return new String(bytes, StandardCharsets.UTF_8);
    }
  }

  private static boolean fundingPresent(CrashPhase phase) {
    return phase == CrashPhase.INTENT_COMMITTED
        || phase == CrashPhase.INSTALLED_REMOVED
        || phase == CrashPhase.NAMES_SYNCED;
  }

  private static Records.WorkView current(SessionStore sessions) throws Exception {
    return sessions
        .snapshot(
            ResultFixture.sessionAccess("alice"),
            ResultFixture.SELECTED,
            1,
            new Messages.Watch(90, ResultFixture.WORK, 0, 0))
        .work();
  }

  private static Messages.Binding binding(SessionStore sessions) throws Exception {
    return sessions.create(
        ResultFixture.sessionAccess("alice"),
        ResultFixture.SELECTED,
        new Messages.Create(1, 1, new Records.Policy(10_000, 20_000, 30_000)));
  }

  private static void assertOutputState(
      Path database, Messages.Binding binding, boolean logicallyLive) throws Exception {
    try (var connection =
        BoundedSqlite.open(database, ResultFixture.configuration().files()).connect()) {
      AdmissionStore.StoredJob stored = AdmissionStore.job(connection, binding, ResultFixture.WORK);
      assertNotNull(stored);
      assertEquals(RELEASE_AT, stored.record().outputReleaseAt());
      assertEquals(logicallyLive, stored.record().outputsLive());
      assertTrue(stored.record().inputLive());
      assertNull(stored.record().inputReleaseAt());
    }
  }

  private static ExecutionStore.Lease lease(SessionStore sessions) {
    return new ExecutionStore.Lease(
        sessions.identity(), "alice", 1, ResultFixture.WORK, 1, 1, 1600);
  }

  private static Commitments.Context context() {
    return new Commitments.Context("issuer-a", "alice", 1);
  }

  private static Records.InputHeader header() throws Exception {
    return new Records.InputHeader(
        1,
        ResultFixture.operation(2),
        new Records.AdmitParameters(
            ResultFixture.WORK,
            new Records.Input(
                INPUT.length, ResultFixture.digest(INPUT), "application/octet-stream"),
            "copy",
            0,
            1000,
            new Records.OutputBudget(1, OUTPUT.length)));
  }

  private static Path only(Path directory, String suffix) throws Exception {
    try (var entries = Files.newDirectoryStream(directory, "*" + suffix)) {
      var iterator = entries.iterator();
      assertTrue(iterator.hasNext(), directory.toString());
      Path result = iterator.next();
      assertFalse(iterator.hasNext(), directory.toString());
      return result;
    }
  }

  private static boolean any(Path directory, String suffix) throws Exception {
    try (var entries = Files.newDirectoryStream(directory, "*" + suffix)) {
      return entries.iterator().hasNext();
    }
  }

  private enum CrashPhase {
    INTENT_COMMITTED(141, RetentionStore.Phase.OUTPUT_INTENT_COMMITTED, null),
    INSTALLED_REMOVED(142, null, InputStore.Phase.OUTPUT_RETENTION_INSTALLED_REMOVED),
    NAMES_SYNCED(143, null, InputStore.Phase.OUTPUT_RETENTION_NAMES_SYNCED),
    FUNDING_REMOVED(144, null, InputStore.Phase.OUTPUT_FUNDING_REMOVED),
    FUNDING_SYNCED(145, null, InputStore.Phase.OUTPUT_FUNDING_SYNCED),
    RELEASE_COMMITTED(146, RetentionStore.Phase.OUTPUT_RELEASE_COMMITTED, null);

    final int exit;
    final RetentionStore.Phase metadata;
    final InputStore.Phase physical;

    CrashPhase(int exit, RetentionStore.Phase metadata, InputStore.Phase physical) {
      this.exit = exit;
      this.metadata = metadata;
      this.physical = physical;
    }
  }

  private record UsageBefore(
      long generation,
      long bytes,
      int files,
      long inputBytes,
      long outputBytes,
      long fundingBytes) {}
}
