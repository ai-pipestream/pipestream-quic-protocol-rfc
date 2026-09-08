package ai.pipestream.quic.v2;

import static ai.pipestream.quic.v2.Messages.DURABLE_WORK;
import static ai.pipestream.quic.v2.Messages.RESULT_DELIVERY;
import static org.junit.jupiter.api.Assertions.*;

import ai.pipestream.quic.BoundedSqlite;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

@Timeout(30)
final class PublicationStoreRecoveryTest {
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
  private static final byte[] OUTPUT = {1, 2, 3, 4};
  private static final PublicationStore.Endpoint ENDPOINT =
      new PublicationStore.Endpoint("results.example:7443");
  private static final AdmissionStore.Authorization ALLOW = (binding, parameters) -> {};

  @TempDir Path directory;

  @Test
  void processDeathBeforePublicationCommitOrAfterReturnHasExactDurabilityBoundary()
      throws Exception {
    for (String phase : List.of("before", "after")) {
      Path database = directory.resolve(phase + ".sqlite");
      Path inputsPath = directory.resolve(phase + "-inputs");
      int expectedExit = phase.equals("before") ? 81 : 82;
      assertEquals(
          expectedExit,
          runChild(phase, database, inputsPath),
          Files.readString(directory.resolve(phase + ".err")));

      SessionStore sessions = SessionStore.open(database, configuration());
      try (InputStore inputs = InputStore.open(inputsPath, INPUT_LIMITS)) {
        sessions.verifyInputs(inputs);
        ExecutionStore.Lease lease =
            new ExecutionStore.Lease(sessions.identity(), "alice", 1, WORK, 1, 1, 1600);
        InputStore.Usage retained = inputs.usage();
        OutputStore.Stored output = inputs.findOutput(context(), header(), lease, 0).orElseThrow();
        assertEquals(digest(OUTPUT), output.sha256());
        try (InputStream stream = output.openStream()) {
          assertArrayEquals(OUTPUT, stream.readAllBytes());
        }
        assertEquals(retained, inputs.usage());

        Records.WorkView view = view(sessions);
        long[] credits = credits(database);
        if (phase.equals("before")) {
          assertEquals(Records.State.ACTIVE, view.state());
          assertNull(view.manifest());
          assertArrayEquals(
              new long[] {FixedRecords.ADMITTED_WORK_CREDITS, FixedRecords.JOB_CREDITS}, credits);
          Records.WorkView retried =
              sessions.succeedExecution(
                  execAccess(), lease, inputs, 1, ENDPOINT, clock(1200), ALLOW);
          assertEquals(Records.State.SUCCEEDED, retried.state());
          view = retried;
          credits = credits(database);
        }
        assertEquals(Records.State.SUCCEEDED, view.state());
        Records.Manifest manifest = view.manifest();
        assertNotNull(manifest);
        assertEquals(1, manifest.outputs().size());
        assertEquals(digest(OUTPUT), manifest.outputs().get(0).sha256());
        assertEquals(1200, manifest.committedAt());
        assertEquals(21_200, manifest.availableUntil());
        assertArrayEquals(
            new long[] {FixedRecords.ADMITTED_WORK_CREDITS - 1, FixedRecords.JOB_CREDITS - 1},
            credits);
        assertEquals(retained, inputs.usage());
      }
    }
  }

  public static void main(String[] args) throws Exception {
    String phase = args[0];
    Path database = Path.of(args[1]);
    Path inputsPath = Path.of(args[2]);
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
    ExecutionStore.Lease lease =
        sessions.claimExecution(execAccess(), 1, WORK, inputs, 500, clock(1100), ALLOW);
    try (OutputStore.Writer writer =
        inputs.beginOutput(
            context(), header(), lease, 0, OUTPUT.length, "application/octet-stream", 4)) {
      writer.write(ByteBuffer.wrap(OUTPUT));
      writer.finish();
    }
    AtomicInteger checks = new AtomicInteger();
    AdmissionStore.Authorization authorization =
        (binding, parameters) -> {
          if (phase.equals("before") && checks.incrementAndGet() == 3)
            Runtime.getRuntime().halt(81);
        };
    sessions.succeedExecution(execAccess(), lease, inputs, 1, ENDPOINT, clock(1200), authorization);
    Runtime.getRuntime().halt(82);
  }

  private int runChild(String phase, Path database, Path inputs) throws Exception {
    Process process =
        new ProcessBuilder(
                Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-cp",
                System.getProperty("java.class.path"),
                PublicationStoreRecoveryTest.class.getName(),
                phase,
                database.toString(),
                inputs.toString())
            .redirectOutput(directory.resolve(phase + ".out").toFile())
            .redirectError(directory.resolve(phase + ".err").toFile())
            .start();
    try {
      assertTrue(process.waitFor(8, TimeUnit.SECONDS), "publication child did not exit");
      return process.exitValue();
    } finally {
      if (process.isAlive()) {
        process.destroyForcibly();
        assertTrue(process.waitFor(2, TimeUnit.SECONDS));
      }
    }
  }

  private static Records.WorkView view(SessionStore sessions) throws Exception {
    return sessions
        .snapshot(sessionAccess(), SELECTED, 1, new Messages.Watch(8, WORK, 0, 0))
        .work();
  }

  private static long[] credits(Path database) throws Exception {
    try (var connection = BoundedSqlite.open(database, configuration().files()).connect();
        var statement = connection.createStatement();
        var rows =
            statement.executeQuery(
                "SELECT e.view_slot,j.state_slot FROM ps_v2_entities e JOIN ps_v2_jobs j ON"
                    + " e.generation=j.generation AND e.scope=j.scope AND e.id=j.entity")) {
      assertTrue(rows.next());
      return new long[] {
        FixedRecords.header(connection, rows.getLong(1), FixedRecords.Kind.WORK).credits(),
        FixedRecords.header(connection, rows.getLong(2), FixedRecords.Kind.JOB).credits()
      };
    }
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
        new AdmissionStore.ExecutionPolicy(
            List.of(
                new AdmissionStore.Application(
                    "copy", Set.of(0), AdmissionStore.RestartSafety.IDEMPOTENT)),
            4,
            4));
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
            1000,
            new Records.OutputBudget(1, 4)));
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
}
