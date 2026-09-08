package ai.pipestream.quic.v2;

import static ai.pipestream.quic.v2.Messages.DURABLE_WORK;
import static ai.pipestream.quic.v2.Messages.RESULT_DELIVERY;
import static org.junit.jupiter.api.Assertions.*;

import ai.pipestream.quic.BoundedSqlite;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

@Timeout(45)
final class OutputStoreReclaimRecoveryTest {
  private static final Messages.Capabilities SELECTED =
      new Messages.Capabilities(
          true,
          List.of(DURABLE_WORK, RESULT_DELIVERY),
          List.of(),
          1 << 20,
          8,
          16,
          1 << 20,
          1000,
          5000);
  private static final InputStore.Limits INPUT_LIMITS =
      new InputStore.Limits(4L << 20, 32, 1 << 20, 8);
  private static final Records.WorkKey WORK = new Records.WorkKey(0, 0, 1);
  private static final byte[] OLD = {1, 2, 3, 4};
  private static final byte[] FRESH = {9, 8, 7, 6};
  private static final PublicationStore.Endpoint ENDPOINT =
      new PublicationStore.Endpoint("results.example:7443");
  private static final AdmissionStore.Authorization ALLOW = (binding, parameters) -> {};

  @TempDir Path directory;

  @Test
  void committedReplacementReclamationRecoversAtEveryFilesystemBoundary() throws Exception {
    List<InputStore.Phase> phases =
        List.of(
            InputStore.Phase.OUTPUT_RECLAIM_AUDITED,
            InputStore.Phase.OUTPUT_RECLAIM_PENDING_REMOVED,
            InputStore.Phase.OUTPUT_RECLAIM_INSTALLED_REMOVED,
            InputStore.Phase.OUTPUT_RECLAIM_SYNCED);
    for (int index = 0; index < phases.size(); index++) {
      InputStore.Phase phase = phases.get(index);
      Path database = directory.resolve(phase.name() + ".sqlite");
      Path inputsPath = directory.resolve(phase.name() + "-inputs");
      int exit = 121 + index;
      ChildResult child = runChild(database, inputsPath, phase, exit);
      assertEquals(exit, child.exit(), childError(phase));

      SessionStore sessions = SessionStore.open(database, configuration());
      InputStore.Usage funded;
      try (InputStore inputs = InputStore.open(inputsPath, INPUT_LIMITS)) {
        sessions.verifyInputs(inputs);
        funded = inputs.usage();
        assertEquals(child.bytes(), funded.bytes());
        assertEquals(child.files(), funded.files());
        Records.WorkView active = view(sessions);
        assertEquals(Records.State.ACTIVE, active.state());
        assertNull(active.manifest());
        assertEquals(1, active.attempt());
        ExecutionStore.Lease second = lease(sessions, 2, 1300);
        synchronized (inputs) {
          sessions.checkExecution(execAccess(), second, clock(1250), ALLOW);
          boolean installedMustRemain =
              phase == InputStore.Phase.OUTPUT_RECLAIM_AUDITED
                  || phase == InputStore.Phase.OUTPUT_RECLAIM_PENDING_REMOVED;
          assertEquals(
              installedMustRemain,
              inputs.findOutput(context(), header(), lease(sessions, 1, 1200), 0).isPresent());
          inputs.reclaimOutputs(context(), header(), second);
        }
        assertTrue(inputs.findOutput(context(), header(), lease(sessions, 1, 1200), 0).isEmpty());
        assertEquals(0, count(inputsPath.resolve("output-pending")));
        assertEquals(funded, inputs.usage());

        ExecutionRuntime runtime =
            new ExecutionRuntime(
                sessions,
                inputs,
                List.of(
                    new ExecutionRuntime.Registration(
                        application(),
                        callback -> {
                          callback.beginOutput(FRESH.length, "application/octet-stream");
                          callback.writeOutput(ByteBuffer.wrap(FRESH));
                          assertEquals(0, callback.finishOutput());
                          return ExecutionRuntime.Outcome.succeeded();
                        })),
                ENDPOINT,
                clock(1300),
                ALLOW,
                new ExecutionRuntime.Limits(1, 1, 400, 128));
        Records.WorkView succeeded = runtime.run(execAccess(), 1, WORK);
        assertEquals(Records.State.SUCCEEDED, succeeded.state());
        assertEquals(1, succeeded.attempt());
        assertEquals(1, succeeded.manifest().outputs().size());
        assertEquals(digest(FRESH), succeeded.manifest().outputs().get(0).sha256());
        assertEquals(funded, inputs.usage());
      }

      SessionStore reopened = SessionStore.open(database, configuration());
      try (InputStore inputs = InputStore.open(inputsPath, INPUT_LIMITS)) {
        reopened.verifyInputs(inputs);
        Records.WorkView retained = view(reopened);
        assertEquals(Records.State.SUCCEEDED, retained.state());
        assertEquals(digest(FRESH), retained.manifest().outputs().get(0).sha256());
        assertEquals(funded, inputs.usage());
        ExecutionStore.Lease producing = lease(reopened, 3, 1700);
        OutputStore.Stored output =
            inputs.findOutput(context(), header(), producing, 0).orElseThrow();
        try (InputStream stream = output.openStream()) {
          assertArrayEquals(FRESH, stream.readAllBytes());
        }
      }
    }
  }

  public static void main(String[] args) throws Exception {
    Path database = Path.of(args[0]);
    Path inputsPath = Path.of(args[1]);
    InputStore.Phase target = InputStore.Phase.valueOf(args[2]);
    int exit = Integer.parseInt(args[3]);
    SessionStore sessions = SessionStore.initialize(database, configuration());
    sessions.create(
        sessionAccess(),
        SELECTED,
        new Messages.Create(1, 1, new Records.Policy(10_000, 20_000, 30_000)));
    sessions.declare(
        sessionAccess(), SELECTED, 1, new Messages.Declare(2, operation(1), 0, List.of(1L), false));
    InputStore inputs =
        InputStore.initializeForAuthority(inputsPath, INPUT_LIMITS, sessions.identity());
    sessions.bindInputs(inputs);
    try (InputStore.Receiver receiver = inputs.begin(context(), header(), SELECTED, 1)) {
      receiver.write(ByteBuffer.allocate(0), 2);
      receiver.finish(3);
    }
    sessions.admit(sessionAccess(), SELECTED, 1, inputs, header(), 2, clock(1000), ALLOW);
    ExecutionStore.Lease first =
        sessions.claimExecution(execAccess(), 1, WORK, inputs, 100, clock(1100), ALLOW);
    try (OutputStore.Writer writer =
        inputs.beginOutput(
            context(), header(), first, 0, OLD.length, "application/octet-stream", OLD.length)) {
      writer.write(ByteBuffer.wrap(OLD));
      writer.finish();
    }
    ExecutionStore.Lease second =
        sessions.claimExecution(execAccess(), 1, WORK, inputs, 100, clock(1200), ALLOW);
    inputs.close();
    InputStore crashing =
        InputStore.open(
            inputsPath,
            INPUT_LIMITS,
            phase -> {
              if (phase == target) Runtime.getRuntime().halt(exit);
            });
    sessions.verifyInputs(crashing);
    InputStore.Usage funded = crashing.usage();
    System.out.printf("funding bytes=%d files=%d%n", funded.bytes(), funded.files());
    System.out.flush();
    // Reconstruct the valid link-created/staging-not-yet-unlinked image after startup recovery.
    // Both names refer to the exact verified immutable output.
    Path installed = onlyEntry(inputsPath.resolve("outputs"));
    Files.createLink(
        inputsPath.resolve("output-pending").resolve(installed.getFileName()), installed);
    synchronized (crashing) {
      sessions.checkExecution(execAccess(), second, clock(1250), ALLOW);
      crashing.reclaimOutputs(context(), header(), second);
    }
    throw new AssertionError("reclamation probe was not reached");
  }

  private ChildResult runChild(Path database, Path inputs, InputStore.Phase phase, int exit)
      throws Exception {
    Path error = directory.resolve(phase.name() + ".err");
    Path output = directory.resolve(phase.name() + ".out");
    Process process =
        new ProcessBuilder(
                Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-cp",
                System.getProperty("java.class.path"),
                OutputStoreReclaimRecoveryTest.class.getName(),
                database.toString(),
                inputs.toString(),
                phase.name(),
                Integer.toString(exit))
            .redirectOutput(output.toFile())
            .redirectError(error.toFile())
            .start();
    try {
      assertTrue(process.waitFor(10, TimeUnit.SECONDS), "reclamation child did not exit");
      return new ChildResult(process.exitValue(), childFunding(output));
    } finally {
      if (process.isAlive()) {
        process.destroyForcibly();
        assertTrue(process.waitFor(2, TimeUnit.SECONDS));
      }
    }
  }

  private static Funding childFunding(Path output) throws Exception {
    byte[] bytes;
    try (InputStream input = Files.newInputStream(output)) {
      bytes = input.readNBytes(128);
      assertEquals(-1, input.read());
    }
    String text = new String(bytes, StandardCharsets.UTF_8);
    assertTrue(text.matches("funding bytes=[0-9]+ files=[0-9]+\\n"), text);
    String[] fields = text.trim().split(" ");
    return new Funding(
        Long.parseLong(fields[1].substring("bytes=".length())),
        Long.parseLong(fields[2].substring("files=".length())));
  }

  private String childError(InputStore.Phase phase) throws Exception {
    Path error = directory.resolve(phase.name() + ".err");
    return Files.exists(error) ? Files.readString(error) : "child produced no error log";
  }

  private static SessionStore.Configuration configuration() {
    return new SessionStore.Configuration(
        "issuer-a",
        new Records.Limits(8, 16, 16, 1 << 20, 1 << 20, 4),
        new Records.Policy(60_000, 60_000, 60_000),
        2,
        8,
        4,
        BoundedSqlite.Limits.defaults(),
        new AdmissionStore.ExecutionPolicy(List.of(application()), 4, 4));
  }

  private static AdmissionStore.Application application() {
    return new AdmissionStore.Application(
        "copy", Set.of(0), AdmissionStore.RestartSafety.IDEMPOTENT);
  }

  private static Records.InputHeader header() throws Exception {
    return new Records.InputHeader(
        1,
        operation(2),
        new Records.AdmitParameters(
            WORK,
            new Records.Input(0, digest(new byte[0]), "application/octet-stream"),
            "copy",
            0,
            5000,
            new Records.OutputBudget(1, OLD.length)));
  }

  private static Records.WorkView view(SessionStore sessions) throws Exception {
    return sessions
        .snapshot(sessionAccess(), SELECTED, 1, new Messages.Watch(8, WORK, 0, 0))
        .work();
  }

  private static ExecutionStore.Lease lease(SessionStore sessions, long number, long until) {
    return new ExecutionStore.Lease(sessions.identity(), "alice", 1, WORK, 1, number, until);
  }

  private static Records.Digest digest(byte[] bytes) throws Exception {
    return new Records.Digest(MessageDigest.getInstance("SHA-256").digest(bytes));
  }

  private static AdmissionStore.Clock clock(long time) {
    return () -> new AdmissionStore.Time(time, true);
  }

  private static Commitments.Context context() {
    return new Commitments.Context("issuer-a", "alice", 1);
  }

  private static Records.OperationId operation(int value) {
    byte[] bytes = new byte[16];
    bytes[15] = (byte) value;
    return new Records.OperationId(bytes);
  }

  private static SessionStore.Access sessionAccess() {
    return new SessionStore.Access("alice", () -> {});
  }

  private static ExecutionStore.Access execAccess() {
    return new ExecutionStore.Access("alice", () -> {});
  }

  private static Path onlyEntry(Path directory) throws Exception {
    try (DirectoryStream<Path> entries = Files.newDirectoryStream(directory)) {
      var iterator = entries.iterator();
      Path entry = iterator.next();
      assertFalse(iterator.hasNext());
      return entry;
    }
  }

  private static int count(Path directory) throws Exception {
    int count = 0;
    try (DirectoryStream<Path> entries = Files.newDirectoryStream(directory)) {
      for (Path ignored : entries) count++;
    }
    return count;
  }

  private record Funding(long bytes, long files) {}

  private record ChildResult(int exit, Funding funding) {
    long bytes() {
      return funding.bytes();
    }

    long files() {
      return funding.files();
    }
  }
}
