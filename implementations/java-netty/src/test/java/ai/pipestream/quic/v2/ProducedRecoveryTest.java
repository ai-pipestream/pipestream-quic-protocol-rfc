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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

@Timeout(30)
final class ProducedRecoveryTest {
  private static final Messages.Capabilities SELECTED =
      new Messages.Capabilities(
          true,
          List.of(DURABLE_WORK, RESULT_DELIVERY),
          List.of(),
          1 << 20,
          16,
          32,
          1 << 20,
          1000,
          5000);
  private static final InputStore.Limits INPUT_LIMITS =
      new InputStore.Limits(8L << 20, 64, 1 << 20, 8);
  private static final AdmissionStore.Authorization ALLOW = (binding, parameters) -> {};
  private static final byte[] CHILD_BYTES = {4, 5, 6};
  private static final Records.WorkKey PARENT = new Records.WorkKey(0, 0, 1);

  @TempDir Path directory;

  @Test
  void processDeathBeforeOrAfterProducedAdmissionRecoversExactProducerMetadata() throws Exception {
    for (CrashPhase phase : CrashPhase.values()) {
      Path database = directory.resolve(phase.name().toLowerCase() + ".sqlite");
      Path inputsPath = directory.resolve(phase.name().toLowerCase() + "-inputs");
      Path stdout = directory.resolve(phase.name().toLowerCase() + ".out");
      Path stderr = directory.resolve(phase.name().toLowerCase() + ".err");

      int exit = runChild(phase, database, inputsPath, stdout, stderr);
      assertEquals(phase.exit, exit, () -> childFailure(stdout, stderr));

      SessionStore sessions = SessionStore.open(database, configuration());
      try (InputStore inputs = InputStore.open(inputsPath, INPUT_LIMITS)) {
        sessions.verifyInputs(inputs);
        Messages.Binding binding =
            sessions.attach(access(), SELECTED, new Messages.Attach(90, "issuer-a", "alice", 1));
        assertEquals(1, binding.generation());
        Records.ChildScope child = view(sessions, PARENT, 91).child();
        assertNotNull(child);
        assertEquals(1, child.scope());
        assertEquals(1, child.producer());

        assertCode(
            ProtocolError.Code.NOT_READY,
            () ->
                sessions.claimExecution(execAccess(), 1, PARENT, inputs, 500, clock(1199), ALLOW));
        ExecutionStore.Lease replacement =
            sessions.claimExecution(execAccess(), 1, PARENT, inputs, 500, clock(1200), ALLOW);
        assertEquals(1, replacement.attempt());
        assertEquals(2, replacement.number());
        assertEquals(1700, replacement.until());

        Messages.DeclarationResponse declaration =
            sessions.declareProduced(
                execAccess(),
                replacement,
                SELECTED,
                inputs,
                declaration(child.scope()),
                clock(1200),
                ALLOW);
        Records.Declared declared =
            assertInstanceOf(Records.Declared.class, declaration.receipt().outcome());
        assertEquals(child.scope(), declared.scope());
        assertEquals(1, declared.producer());
        assertEquals(1, declared.acceptedCount());
        assertEquals(1, declared.declared());
        assertNotNull(declared.seal());

        Records.InputHeader header = childHeader(child.scope());
        Commitments.Context context = new Commitments.Context("issuer-a", "alice", 1);
        InputStore.Stored stored = inputs.find(context, header).orElseThrow();
        assertEquals(CHILD_BYTES.length, stored.length());
        assertEquals(digest(CHILD_BYTES), stored.header().parameters().input().sha256());
        try (InputStream stream = stored.openStream()) {
          assertArrayEquals(CHILD_BYTES, stream.readAllBytes());
        }

        Records.OperationReceipt retainedBeforeReplay = null;
        if (phase == CrashPhase.BEFORE_ADMISSION) {
          assertTrue(
              sessions
                  .checkProducedInput(
                      execAccess(), replacement, SELECTED, inputs, header, clock(1200), ALLOW)
                  .isEmpty());
        } else {
          retainedBeforeReplay =
              sessions
                  .checkProducedInput(
                      execAccess(), replacement, SELECTED, inputs, header, clock(1200), ALLOW)
                  .orElseThrow();
          assertAdmission(retainedBeforeReplay, child.scope(), 1100);
        }

        Records.OperationReceipt admitted =
            sessions.admitProduced(
                execAccess(), replacement, SELECTED, inputs, header, clock(1200), ALLOW);
        assertAdmission(
            admitted, child.scope(), phase == CrashPhase.BEFORE_ADMISSION ? 1200 : 1100);
        if (retainedBeforeReplay != null) assertEquals(retainedBeforeReplay, admitted);
        assertEquals(
            admitted,
            sessions.admitProduced(
                execAccess(), replacement, SELECTED, inputs, header, clock(1200), ALLOW));

        Messages.PageResponse page =
            sessions.page(access(), SELECTED, 1, new Messages.Page(92, child.scope(), 0, 8));
        assertTrue(page.sealed());
        assertEquals(declared.seal(), page.seal());
        assertEquals(1, page.producer());
        assertEquals(1, page.declared());
        assertEquals(List.of(10L), page.entries().stream().map(Messages.Entry::entity).toList());
        assertEquals(1, scalar(database, "SELECT count(*) FROM ps_v2_jobs WHERE producer=1"));
        assertEquals(1, scalar(database, "SELECT count(*) FROM ps_v2_entities WHERE producer=1"));
        assertEquals(2, scalar(database, "SELECT count(*) FROM ps_v2_jobs"));

        assertCode(
            ProtocolError.Code.NOT_READY,
            () ->
                sessions.succeedExecution(
                    execAccess(),
                    replacement,
                    inputs,
                    0,
                    new PublicationStore.Endpoint("results.example:7443"),
                    clock(1200),
                    ALLOW));
        Records.WorkView parent = view(sessions, PARENT, 93);
        assertEquals(Records.State.ACTIVE, parent.state());
        assertNull(parent.manifest());
        assertEquals(0, inputs.usage().handles());
      }
    }
  }

  public static void main(String[] args) throws Exception {
    CrashPhase phase = CrashPhase.valueOf(args[0]);
    Path database = Path.of(args[1]);
    Path inputsPath = Path.of(args[2]);
    SessionStore sessions = SessionStore.initialize(database, configuration());
    sessions.create(
        access(), SELECTED, new Messages.Create(1, 1, new Records.Policy(10_000, 20_000, 30_000)));
    sessions.declare(
        access(), SELECTED, 1, new Messages.Declare(2, operation(1), 0, List.of(1L), false));
    InputStore inputs =
        InputStore.initializeForAuthority(inputsPath, INPUT_LIMITS, sessions.identity());
    sessions.bindInputs(inputs);

    install(inputs, parentHeader(), new byte[0], 1000);
    sessions.admit(access(), SELECTED, 1, inputs, parentHeader(), 3, clock(1000), ALLOW);
    ExecutionStore.Lease parent =
        sessions.claimExecution(execAccess(), 1, PARENT, inputs, 100, clock(1100), ALLOW);
    Records.ChildScope child = view(sessions, PARENT, 4).child();
    assertNotNull(child);
    sessions.declareProduced(
        execAccess(), parent, SELECTED, inputs, declaration(child.scope()), clock(1100), ALLOW);
    Records.InputHeader header = childHeader(child.scope());
    install(inputs, header, CHILD_BYTES, 1100);
    if (phase == CrashPhase.BEFORE_ADMISSION) Runtime.getRuntime().halt(61);
    sessions.admitProduced(execAccess(), parent, SELECTED, inputs, header, clock(1100), ALLOW);
    Runtime.getRuntime().halt(62);
  }

  private int runChild(CrashPhase phase, Path database, Path inputs, Path stdout, Path stderr)
      throws Exception {
    Process process =
        new ProcessBuilder(
                Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-Xmx64m",
                "-cp",
                System.getProperty("java.class.path"),
                ProducedRecoveryTest.class.getName(),
                phase.name(),
                database.toString(),
                inputs.toString())
            .redirectOutput(stdout.toFile())
            .redirectError(stderr.toFile())
            .start();
    try {
      assertTrue(process.waitFor(10, TimeUnit.SECONDS), "producer recovery child did not exit");
      return process.exitValue();
    } finally {
      if (process.isAlive()) {
        process.destroyForcibly();
        assertTrue(process.waitFor(2, TimeUnit.SECONDS), "producer recovery child survived kill");
      }
    }
  }

  private static void install(InputStore inputs, Records.InputHeader header, byte[] bytes, long utc)
      throws Exception {
    Commitments.Context context = new Commitments.Context("issuer-a", "alice", 1);
    try (InputStore.Receiver receiver = inputs.begin(context, header, SELECTED, utc)) {
      ByteBuffer source = ByteBuffer.wrap(bytes);
      receiver.write(source, utc);
      assertFalse(source.hasRemaining());
      assertEquals(header, receiver.finish(utc).header());
    }
  }

  private static Records.InputHeader parentHeader() throws Exception {
    return new Records.InputHeader(
        1,
        operation(2),
        new Records.AdmitParameters(
            PARENT,
            new Records.Input(0, digest(new byte[0]), "application/octet-stream"),
            "expand",
            2,
            1000,
            new Records.OutputBudget(0, 0)));
  }

  private static Records.InputHeader childHeader(long scope) throws Exception {
    return new Records.InputHeader(
        1,
        operation(21),
        new Records.AdmitParameters(
            new Records.WorkKey(scope, 1, 10),
            new Records.Input(CHILD_BYTES.length, digest(CHILD_BYTES), "application/octet-stream"),
            "expand",
            0,
            1000,
            new Records.OutputBudget(0, 0)));
  }

  private static Messages.Declare declaration(long scope) {
    return new Messages.Declare(20, operation(20), scope, List.of(10L), true);
  }

  private static void assertAdmission(
      Records.OperationReceipt receipt, long scope, long admittedAt) {
    assertEquals(operation(21), receipt.operation());
    Records.Admitted admitted = assertInstanceOf(Records.Admitted.class, receipt.outcome());
    assertEquals(new Records.WorkKey(scope, 1, 10), admitted.work());
    assertEquals(1, admitted.attempt());
    assertEquals(admittedAt, admitted.admittedAt());
    assertEquals(admittedAt + 1000, admitted.deadline());
    assertNull(admitted.child());
  }

  private static Records.WorkView view(SessionStore sessions, Records.WorkKey work, long request)
      throws Exception {
    return sessions.snapshot(access(), SELECTED, 1, new Messages.Watch(request, work, 0, 0)).work();
  }

  private static long scalar(Path database, String sql) throws Exception {
    try (var connection = BoundedSqlite.open(database, configuration().files()).connect();
        var statement = connection.createStatement();
        var rows = statement.executeQuery(sql)) {
      assertTrue(rows.next());
      return rows.getLong(1);
    }
  }

  private static SessionStore.Configuration configuration() {
    AdmissionStore.Application application =
        new AdmissionStore.Application(
            "expand", Set.of(0, 2), AdmissionStore.RestartSafety.IDEMPOTENT);
    return new SessionStore.Configuration(
        "issuer-a",
        new Records.Limits(32, 64, 64, 1 << 20, 1 << 20, 16),
        new Records.Policy(60_000, 60_000, 60_000),
        4,
        16,
        4,
        BoundedSqlite.Limits.defaults(),
        new AdmissionStore.ExecutionPolicy(List.of(application), 16, 16));
  }

  private static Records.Digest digest(byte[] bytes) throws Exception {
    return new Records.Digest(MessageDigest.getInstance("SHA-256").digest(bytes));
  }

  private static Records.OperationId operation(int value) {
    byte[] bytes = new byte[16];
    bytes[12] = (byte) (value >>> 24);
    bytes[13] = (byte) (value >>> 16);
    bytes[14] = (byte) (value >>> 8);
    bytes[15] = (byte) value;
    return new Records.OperationId(bytes);
  }

  private static AdmissionStore.Clock clock(long utc) {
    return () -> new AdmissionStore.Time(utc, true);
  }

  private static SessionStore.Access access() {
    return new SessionStore.Access("alice", () -> {});
  }

  private static ExecutionStore.Access execAccess() {
    return new ExecutionStore.Access("alice", () -> {});
  }

  private static void assertCode(ProtocolError.Code expected, Throwing action) {
    ProtocolError error = assertThrows(ProtocolError.class, action::run);
    assertEquals(expected, error.code(), error::getMessage);
  }

  private static String childFailure(Path stdout, Path stderr) {
    try {
      return "stdout:\n" + Files.readString(stdout) + "\nstderr:\n" + Files.readString(stderr);
    } catch (Exception error) {
      return "unable to read child output: " + error;
    }
  }

  private enum CrashPhase {
    BEFORE_ADMISSION(61),
    AFTER_ADMISSION(62);

    final int exit;

    CrashPhase(int exit) {
      this.exit = exit;
    }
  }

  @FunctionalInterface
  private interface Throwing {
    void run() throws Exception;
  }
}
