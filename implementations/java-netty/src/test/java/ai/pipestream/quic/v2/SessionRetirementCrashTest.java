package ai.pipestream.quic.v2;

import static org.junit.jupiter.api.Assertions.*;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

@Timeout(90)
final class SessionRetirementCrashTest {
  private static final long RETIRE_AT = 31_200;

  @TempDir Path directory;

  @Test
  void processDeathAtEachCommittedRetirementBoundaryResumesToExactCompletion() throws Exception {
    for (RetirementStore.Phase phase :
        List.of(
            RetirementStore.Phase.INTENT_COMMITTED,
            RetirementStore.Phase.JOB_REMOVED,
            RetirementStore.Phase.OPERATION_REMOVED,
            RetirementStore.Phase.ENTITY_REMOVED,
            RetirementStore.Phase.FINISHED)) {
      Path root = directory.resolve(phase.name().toLowerCase());
      Path stdout = directory.resolve(phase.name() + ".out");
      Path stderr = directory.resolve(phase.name() + ".err");
      int expectedExit = 100 + phase.ordinal();
      assertEquals(expectedExit, runChild(phase, root, stdout, stderr), boundedText(stderr, 8192));
      assertEquals("ready " + phase, boundedText(stdout, 256).trim());

      Path database = root.resolve("fixture.sqlite");
      Path inputPath = root.resolve("fixture-inputs");
      SessionStore sessions = SessionStore.open(database, ResultFixture.configuration());
      try (InputStore inputs = InputStore.open(inputPath, ResultFixture.INPUT_LIMITS)) {
        sessions.verifyInputs(inputs);
        assertEquals(new InputStore.Usage(0, 0, 0), inputs.usage());
        RetirementStore.Progress terminal = finish(sessions, inputs, 1);
        assertTrue(
            terminal.state() == RetirementStore.State.COMPLETE
                || terminal.state() == RetirementStore.State.ABSENT);
        assertEquals(
            RetirementStore.State.ABSENT,
            sessions.retireSession(1, inputs, 1, ResultFixture.clock(RETIRE_AT)).state());
        Messages.Sequence next =
            sessions.nextSequence(
                ResultFixture.sessionAccess("alice"),
                ResultFixture.SELECTED,
                new Messages.NextSequence(90));
        assertEquals(2, next.nextCreationSequence());
        Messages.Binding replacement =
            sessions.create(
                ResultFixture.sessionAccess("alice"),
                ResultFixture.SELECTED,
                new Messages.Create(91, 2, new Records.Policy(10_000, 20_000, 30_000)));
        assertTrue(replacement.generation() > 1);
      }
    }
  }

  public static void main(String[] args) throws Exception {
    RetirementStore.Phase target = RetirementStore.Phase.valueOf(args[0]);
    Path root = Path.of(args[1]);
    Files.createDirectories(root);
    try (ResultFixture fixture =
        new ResultFixture(root, "fixture", new byte[] {9}, new byte[] {1})) {
      fixture.sessions.declare(
          ResultFixture.sessionAccess("alice"),
          ResultFixture.SELECTED,
          fixture.binding.generation(),
          new Messages.Declare(80, ResultFixture.operation(80), 0, List.of(), true));
      closeRoot(fixture);
      assertEquals(
          RetentionStore.Result.RELEASED,
          fixture.sessions.reclaimOutput(
              fixture.binding.generation(),
              ResultFixture.WORK,
              fixture.inputs,
              ResultFixture.clock(21_200)));
      assertEquals(
          RetentionStore.Result.RELEASED,
          fixture.sessions.reclaimInput(
              fixture.binding.generation(),
              ResultFixture.WORK,
              fixture.inputs,
              ResultFixture.clock(21_200)));
      assertEquals(new InputStore.Usage(0, 0, 0), fixture.inputs.usage());
      System.out.println("ready " + target);
      System.out.flush();
      RetirementStore.Probe probe =
          observed -> {
            if (observed == target) Runtime.getRuntime().halt(100 + target.ordinal());
          };
      for (int calls = 0; calls < 128; calls++) {
        RetirementStore.Progress progress =
            fixture.sessions.retireSession(
                fixture.binding.generation(),
                fixture.inputs,
                1,
                ResultFixture.clock(RETIRE_AT),
                probe);
        if (progress.state() == RetirementStore.State.COMPLETE) break;
      }
      throw new AssertionError("retirement did not reach crash boundary " + target);
    }
  }

  private int runChild(RetirementStore.Phase phase, Path root, Path stdout, Path stderr)
      throws Exception {
    Files.createDirectories(root);
    Process process =
        new ProcessBuilder(
                Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-cp",
                System.getProperty("java.class.path"),
                SessionRetirementCrashTest.class.getName(),
                phase.name(),
                root.toString())
            .redirectOutput(stdout.toFile())
            .redirectError(stderr.toFile())
            .start();
    try {
      assertTrue(process.waitFor(12, TimeUnit.SECONDS), "retirement child did not exit");
      return process.exitValue();
    } finally {
      if (process.isAlive()) {
        process.destroyForcibly();
        assertTrue(process.waitFor(2, TimeUnit.SECONDS));
      }
    }
  }

  private static void closeRoot(ResultFixture fixture) throws Exception {
    Commitments.Seal seal = new Commitments.Seal(fixture.context(), 0, 0, null, 1);
    seal.add(1);
    Records.Digest expected = seal.finish();
    ClosureStore.Cursor cursor = new ClosureStore.Cursor();
    for (int calls = 0; calls < 16; calls++) {
      fixture.sessions.reconcileClosures(cursor, 1, ResultFixture.clock(1200));
      try {
        fixture.sessions.scopeSummary(
            ResultFixture.sessionAccess("alice"),
            ResultFixture.SELECTED,
            fixture.binding.generation(),
            0,
            expected);
        return;
      } catch (ProtocolError refusal) {
        if (refusal.code() != ProtocolError.Code.NOT_READY) throw refusal;
      }
    }
    fail("root closure did not commit before retirement crash");
  }

  private static RetirementStore.Progress finish(
      SessionStore sessions, InputStore inputs, long generation) throws Exception {
    RetirementStore.Progress progress = null;
    for (int calls = 0; calls < 128; calls++) {
      progress = sessions.retireSession(generation, inputs, 1, ResultFixture.clock(RETIRE_AT));
      if (progress.state() == RetirementStore.State.COMPLETE
          || progress.state() == RetirementStore.State.ABSENT) return progress;
      assertEquals(RetirementStore.State.IN_PROGRESS, progress.state());
    }
    return fail("retirement recovery did not finish: " + progress);
  }

  private static String boundedText(Path path, int limit) throws Exception {
    try (InputStream stream = Files.newInputStream(path)) {
      byte[] bytes = stream.readNBytes(limit);
      assertEquals(-1, stream.read(), "child diagnostic exceeded bound");
      return new String(bytes, StandardCharsets.UTF_8);
    }
  }
}
