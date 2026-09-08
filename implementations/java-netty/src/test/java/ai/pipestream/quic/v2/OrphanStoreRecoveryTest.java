package ai.pipestream.quic.v2;

import static org.junit.jupiter.api.Assertions.*;

import ai.pipestream.quic.BoundedSqlite;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

@Timeout(45)
final class OrphanStoreRecoveryTest {
  private static final byte[] INPUT = {9, 8, 7};
  private static final Records.WorkKey WORK = new Records.WorkKey(0, 0, 1);

  @TempDir Path directory;

  @Test
  void processDeathDuringInputAndFundingOrphanRemovalRecoversExactPhysicalQuota() throws Exception {
    for (CrashPhase phase : CrashPhase.values()) {
      Path root = directory.resolve(phase.name().toLowerCase());
      Path stdout = directory.resolve(phase.name() + ".out");
      Path stderr = directory.resolve(phase.name() + ".err");
      assertEquals(phase.exit, runChild(phase, root, stdout, stderr), boundedText(stderr, 8192));
      ChildReport before = report(stdout);

      Path database = root.resolve("authority.sqlite");
      Path inputsPath = root.resolve("inputs");
      SessionStore sessions = SessionStore.open(database, ResultFixture.configuration());
      Messages.Binding binding = binding(sessions);
      try (InputStore inputs = InputStore.open(inputsPath, ResultFixture.INPUT_LIMITS)) {
        sessions.verifyInputs(inputs);
        assertEquals(1, before.generation);
        assertEquals(0, inputs.usage().handles());
        if (phase.funding) {
          assertEquals(before.inputBytes, inputs.usage().bytes());
          assertEquals(1, inputs.usage().files());
          assertTrue(inputs.find(context(), header()).isPresent());
          assertTrue(inputs.findReservation(context(), header()).isEmpty());
        } else {
          assertEquals(before.bytes - before.inputBytes, inputs.usage().bytes());
          assertEquals(before.files - 1, inputs.usage().files());
          assertTrue(inputs.find(context(), header()).isEmpty());
          assertTrue(inputs.findReservation(context(), header()).isPresent());
        }
        assertDeclaredWithoutJob(database, binding, sessions);

        InputStore.OrphanCandidate interrupted =
            new InputStore.OrphanCandidate(
                inputs.identity(), phase.funding, context(), header(), before.reference);
        assertEquals(
            OrphanStore.Result.ABSENT,
            sessions.reclaimOrphan(inputs, interrupted, ResultFixture.clock(1000)));

        InputStore.OrphanCandidate remaining;
        try (InputStore.OrphanScan scan = inputs.scanOrphans()) {
          InputStore.OrphanPage page = scan.nextPage(64);
          assertTrue(page.done());
          assertEquals(1, page.candidates().size());
          remaining = page.candidates().get(0);
        }
        assertNotEquals(phase.funding, remaining.funding());
        assertEquals(context(), remaining.context());
        assertEquals(header(), remaining.header());
        assertEquals(
            OrphanStore.Result.RELEASED,
            sessions.reclaimOrphan(inputs, remaining, ResultFixture.clock(1000)));
        assertEquals(new InputStore.Usage(0, 0, 0), inputs.usage());
        assertDeclaredWithoutJob(database, binding, sessions);
        assertEquals(
            OrphanStore.Result.ABSENT,
            sessions.reclaimOrphan(inputs, remaining, ResultFixture.clock(1000)));
      }

      sessions = SessionStore.open(database, ResultFixture.configuration());
      binding = binding(sessions);
      try (InputStore inputs = InputStore.open(inputsPath, ResultFixture.INPUT_LIMITS)) {
        sessions.verifyInputs(inputs);
        try (InputStore.OrphanScan scan = inputs.scanOrphans()) {
          assertEquals(new InputStore.Usage(0, 0, 1), inputs.usage());
          InputStore.OrphanPage page = scan.nextPage(64);
          assertTrue(page.done());
          assertTrue(page.candidates().isEmpty());
        }
        assertEquals(new InputStore.Usage(0, 0, 0), inputs.usage());
        assertDeclaredWithoutJob(database, binding, sessions);
      }
    }
  }

  public static void main(String[] args) throws Exception {
    CrashPhase phase = CrashPhase.valueOf(args[0]);
    Path root = Path.of(args[1]);
    Files.createDirectories(root);
    Path database = root.resolve("authority.sqlite");
    Path inputsPath = root.resolve("inputs");
    SessionStore sessions = SessionStore.initialize(database, ResultFixture.configuration());
    Messages.Binding binding =
        sessions.create(
            ResultFixture.sessionAccess("alice"),
            ResultFixture.SELECTED,
            new Messages.Create(1, 1, new Records.Policy(10_000, 20_000, 30_000)));
    assertEquals(1, binding.generation());
    sessions.declare(
        ResultFixture.sessionAccess("alice"),
        ResultFixture.SELECTED,
        1,
        new Messages.Declare(2, ResultFixture.operation(1), 0, List.of(1L), false));
    InputStore inputs =
        InputStore.initializeForAuthority(
            inputsPath, ResultFixture.INPUT_LIMITS, sessions.identity());
    sessions.bindInputs(inputs);
    try (InputStore.Receiver receiver =
        inputs.begin(context(), header(), ResultFixture.SELECTED, 1)) {
      receiver.write(ByteBuffer.wrap(INPUT), 2);
      receiver.finish(3);
    }
    inputs.reserveOutputs(context(), header());
    InputStore.Usage usage = inputs.usage();
    InputStore.OrphanCandidate target;
    try (InputStore.OrphanScan scan = inputs.scanOrphans()) {
      InputStore.OrphanPage page = scan.nextPage(64);
      assertTrue(page.done());
      assertEquals(2, page.candidates().size());
      target =
          page.candidates().stream()
              .filter(candidate -> candidate.funding() == phase.funding)
              .findFirst()
              .orElseThrow();
    }
    System.out.printf(
        "%d %d %d %d %s%n",
        binding.generation(),
        usage.bytes(),
        usage.files(),
        Files.size(onlyInput(inputsPath)),
        target.reference());
    System.out.flush();

    inputs.close();
    inputs =
        InputStore.open(
            inputsPath,
            ResultFixture.INPUT_LIMITS,
            observed -> {
              if (observed == phase.boundary) Runtime.getRuntime().halt(phase.exit);
            });
    sessions.verifyInputs(inputs);
    sessions.reclaimOrphan(inputs, target, ResultFixture.clock(1000));
    throw new AssertionError("orphan reclamation passed crash boundary " + phase);
  }

  private int runChild(CrashPhase phase, Path root, Path stdout, Path stderr) throws Exception {
    Files.createDirectories(root);
    Process process =
        new ProcessBuilder(
                Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-cp",
                System.getProperty("java.class.path"),
                OrphanStoreRecoveryTest.class.getName(),
                phase.name(),
                root.toString())
            .redirectOutput(stdout.toFile())
            .redirectError(stderr.toFile())
            .start();
    try {
      assertTrue(process.waitFor(10, TimeUnit.SECONDS), "orphan child did not exit");
      return process.exitValue();
    } finally {
      if (process.isAlive()) {
        process.destroyForcibly();
        assertTrue(process.waitFor(2, TimeUnit.SECONDS));
      }
    }
  }

  private static ChildReport report(Path stdout) throws Exception {
    String text = boundedText(stdout, 1024).trim();
    String[] fields = text.split(" ");
    assertEquals(5, fields.length, text);
    return new ChildReport(
        Long.parseLong(fields[0]),
        Long.parseLong(fields[1]),
        Integer.parseInt(fields[2]),
        Long.parseLong(fields[3]),
        fields[4]);
  }

  private static String boundedText(Path path, int limit) throws Exception {
    try (InputStream stream = Files.newInputStream(path)) {
      byte[] bytes = stream.readNBytes(limit);
      assertEquals(-1, stream.read(), "child diagnostic exceeded bound");
      return new String(bytes, StandardCharsets.UTF_8);
    }
  }

  private static void assertDeclaredWithoutJob(
      Path database, Messages.Binding binding, SessionStore sessions) throws Exception {
    Records.WorkView view =
        sessions
            .snapshot(
                ResultFixture.sessionAccess("alice"),
                ResultFixture.SELECTED,
                1,
                new Messages.Watch(90, WORK, 0, 0))
            .work();
    assertEquals(Records.State.DECLARED, view.state());
    assertNull(view.input());
    assertEquals(0, view.attempt());
    try (var connection =
        BoundedSqlite.open(database, ResultFixture.configuration().files()).connect()) {
      assertNull(AdmissionStore.job(connection, binding, WORK));
    }
  }

  private static Messages.Binding binding(SessionStore sessions) throws Exception {
    return sessions.create(
        ResultFixture.sessionAccess("alice"),
        ResultFixture.SELECTED,
        new Messages.Create(1, 1, new Records.Policy(10_000, 20_000, 30_000)));
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
            WORK,
            new Records.Input(
                INPUT.length, ResultFixture.digest(INPUT), "application/octet-stream"),
            "copy",
            0,
            1000,
            new Records.OutputBudget(1, 16)));
  }

  private enum CrashPhase {
    INPUT_REMOVED(151, false, InputStore.Phase.ORPHAN_REMOVED),
    INPUT_SYNCED(152, false, InputStore.Phase.ORPHAN_SYNCED),
    FUNDING_REMOVED(153, true, InputStore.Phase.ORPHAN_REMOVED),
    FUNDING_SYNCED(154, true, InputStore.Phase.ORPHAN_SYNCED);

    final int exit;
    final boolean funding;
    final InputStore.Phase boundary;

    CrashPhase(int exit, boolean funding, InputStore.Phase boundary) {
      this.exit = exit;
      this.funding = funding;
      this.boundary = boundary;
    }
  }

  private record ChildReport(
      long generation, long bytes, int files, long inputBytes, String reference) {}
}
