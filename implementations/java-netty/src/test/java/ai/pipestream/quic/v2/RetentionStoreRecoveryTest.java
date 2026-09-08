package ai.pipestream.quic.v2;

import static org.junit.jupiter.api.Assertions.*;

import ai.pipestream.quic.BoundedSqlite;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

@Timeout(45)
final class RetentionStoreRecoveryTest {
  private static final byte[] INPUT = {9, 8, 7, 6};
  private static final byte[] OUTPUT = {1, 2, 3};

  @TempDir Path directory;

  @Test
  void processDeathAtEveryInputReleaseBoundaryResumesFromDurableEvidence() throws Exception {
    for (CrashPhase phase : CrashPhase.values()) {
      Path root = directory.resolve(phase.name().toLowerCase());
      Path stdout = directory.resolve(phase.name() + ".out");
      Path stderr = directory.resolve(phase.name() + ".err");
      assertEquals(phase.exit, runChild(phase, root, stdout, stderr), boundedText(stderr));
      UsageBefore before = report(stdout);

      Path database = root.resolve("fixture.sqlite");
      Path inputsPath = root.resolve("fixture-inputs");
      SessionStore sessions = SessionStore.open(database, ResultFixture.configuration());
      Messages.Binding binding = binding(sessions);
      try (InputStore inputs = InputStore.open(inputsPath, ResultFixture.INPUT_LIMITS)) {
        sessions.verifyInputs(inputs);
        assertEquals(1, before.generation);
        assertEquals(0, inputs.usage().handles());
        assertEquals(
            phase == CrashPhase.INTENT_COMMITTED ? before.files : before.files - 1,
            inputs.usage().files());
        if (phase == CrashPhase.INTENT_COMMITTED) {
          assertEquals(before.bytes, inputs.usage().bytes());
          assertTrue(inputs.find(context(), header()).isPresent());
        } else {
          assertEquals(before.bytes - before.inputBytes, inputs.usage().bytes());
          assertTrue(inputs.find(context(), header()).isEmpty());
        }
        assertReleaseState(database, binding, phase != CrashPhase.RELEASE_COMMITTED);

        Records.WorkView retained = current(sessions);
        assertEquals(Records.State.SUCCEEDED, retained.state());
        Records.Manifest manifest = retained.manifest();
        assertNotNull(manifest);
        assertEquals(ResultFixture.digest(OUTPUT), manifest.outputs().get(0).sha256());
        ExecutionStore.Lease lease =
            new ExecutionStore.Lease(
                sessions.identity(), "alice", 1, ResultFixture.WORK, 1, 1, 1600);
        OutputStore.Stored output = inputs.findOutput(context(), header(), lease, 0).orElseThrow();
        try (InputStream stream = output.openStream()) {
          assertArrayEquals(OUTPUT, stream.readAllBytes());
        }
        InputStore.Usage physical = inputs.usage();

        RetentionStore.Result resumed =
            sessions.reclaimInput(1, ResultFixture.WORK, inputs, ResultFixture.clock(1200));
        assertEquals(
            phase == CrashPhase.RELEASE_COMMITTED
                ? RetentionStore.Result.ALREADY_RELEASED
                : RetentionStore.Result.RELEASED,
            resumed);
        assertTrue(inputs.find(context(), header()).isEmpty());
        assertEquals(manifest, current(sessions).manifest());
        assertEquals(before.bytes - before.inputBytes, inputs.usage().bytes());
        assertEquals(
            physical.files() - (phase == CrashPhase.INTENT_COMMITTED ? 1 : 0),
            inputs.usage().files());
        assertEquals(0, inputs.usage().handles());
        assertReleaseState(database, binding, false);
        assertEquals(
            RetentionStore.Result.ALREADY_RELEASED,
            sessions.reclaimInput(1, ResultFixture.WORK, inputs, ResultFixture.clock(1200)));
      }

      sessions = SessionStore.open(database, ResultFixture.configuration());
      binding = binding(sessions);
      try (InputStore inputs = InputStore.open(inputsPath, ResultFixture.INPUT_LIMITS)) {
        sessions.verifyInputs(inputs);
        assertTrue(inputs.find(context(), header()).isEmpty());
        assertEquals(before.bytes - before.inputBytes, inputs.usage().bytes());
        assertEquals(before.files - 1, inputs.usage().files());
        assertEquals(Records.State.SUCCEEDED, current(sessions).state());
        assertEquals(0, inputs.usage().handles());
        assertReleaseState(database, binding, false);
        assertEquals(
            RetentionStore.Result.ALREADY_RELEASED,
            sessions.reclaimInput(1, ResultFixture.WORK, inputs, ResultFixture.clock(1200)));
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
          "%d %d %d %d%n",
          fixture.binding.generation(),
          usage.bytes(),
          usage.files(),
          Files.size(onlyInput(fixture.inputsPath)));
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
      fixture.sessions.reclaimInput(
          fixture.binding.generation(),
          ResultFixture.WORK,
          fixture.inputs,
          ResultFixture.clock(1200),
          metadataProbe);
      throw new AssertionError("reclamation passed crash boundary " + phase);
    }
  }

  private int runChild(CrashPhase phase, Path root, Path stdout, Path stderr) throws Exception {
    Files.createDirectories(root);
    Process process =
        new ProcessBuilder(
                Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-cp",
                System.getProperty("java.class.path"),
                RetentionStoreRecoveryTest.class.getName(),
                phase.name(),
                root.toString())
            .redirectOutput(stdout.toFile())
            .redirectError(stderr.toFile())
            .start();
    try {
      assertTrue(process.waitFor(10, TimeUnit.SECONDS), "retention child did not exit");
      return process.exitValue();
    } finally {
      if (process.isAlive()) {
        process.destroyForcibly();
        assertTrue(process.waitFor(2, TimeUnit.SECONDS));
      }
    }
  }

  private static UsageBefore report(Path stdout) throws Exception {
    String[] fields = boundedText(stdout).trim().split(" ");
    assertEquals(4, fields.length);
    return new UsageBefore(
        Long.parseLong(fields[0]),
        Long.parseLong(fields[1]),
        Integer.parseInt(fields[2]),
        Long.parseLong(fields[3]));
  }

  private static String boundedText(Path path) throws Exception {
    try (InputStream stream = Files.newInputStream(path)) {
      return new String(stream.readNBytes(8192), java.nio.charset.StandardCharsets.UTF_8);
    }
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

  private static void assertReleaseState(
      Path database, Messages.Binding binding, boolean logicallyLive) throws Exception {
    try (var connection =
        BoundedSqlite.open(database, ResultFixture.configuration().files()).connect()) {
      AdmissionStore.StoredJob stored = AdmissionStore.job(connection, binding, ResultFixture.WORK);
      assertNotNull(stored);
      assertEquals(1200L, stored.record().inputReleaseAt());
      assertEquals(logicallyLive, stored.record().inputLive());
    }
  }

  private static Path onlyInput(Path root) throws Exception {
    try (var entries = Files.newDirectoryStream(root.resolve("objects"), "*.input")) {
      var iterator = entries.iterator();
      assertTrue(iterator.hasNext());
      Path result = iterator.next();
      assertFalse(iterator.hasNext());
      return result;
    }
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

  private enum CrashPhase {
    INTENT_COMMITTED(131, RetentionStore.Phase.INPUT_INTENT_COMMITTED, null),
    INPUT_REMOVED(132, null, InputStore.Phase.INPUT_RECLAIM_REMOVED),
    INPUT_SYNCED(133, null, InputStore.Phase.INPUT_RECLAIM_SYNCED),
    RELEASE_COMMITTED(134, RetentionStore.Phase.INPUT_RELEASE_COMMITTED, null);

    final int exit;
    final RetentionStore.Phase metadata;
    final InputStore.Phase physical;

    CrashPhase(int exit, RetentionStore.Phase metadata, InputStore.Phase physical) {
      this.exit = exit;
      this.metadata = metadata;
      this.physical = physical;
    }
  }

  private record UsageBefore(long generation, long bytes, int files, long inputBytes) {}
}
