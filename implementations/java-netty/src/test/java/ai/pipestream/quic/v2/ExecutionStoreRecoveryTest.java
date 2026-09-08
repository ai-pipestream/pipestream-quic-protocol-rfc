package ai.pipestream.quic.v2;

import static ai.pipestream.quic.v2.Messages.DURABLE_WORK;
import static org.junit.jupiter.api.Assertions.*;

import ai.pipestream.quic.BoundedSqlite;
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
final class ExecutionStoreRecoveryTest {
  private static final Messages.Capabilities SELECTED =
      new Messages.Capabilities(
          true, List.of(DURABLE_WORK), List.of(), 1 << 20, 8, 16, 1 << 20, 1000, 5000);
  private static final InputStore.Limits INPUT_LIMITS =
      new InputStore.Limits(4L << 20, 32, 1 << 20, 8);
  private static final Records.WorkKey WORK = new Records.WorkKey(0, 0, 1);
  private static final AdmissionStore.Authorization ALLOW = (binding, parameters) -> {};

  @TempDir Path directory;

  @Test
  void processDeathBeforeClaimCommitOrAfterReturnHasExactLeaseBoundary() throws Exception {
    for (String phase : List.of("before", "after")) {
      Path database = directory.resolve(phase + ".sqlite");
      Path inputsPath = directory.resolve(phase + "-inputs");
      prepare(database, inputsPath, AdmissionStore.RestartSafety.IDEMPOTENT);
      int exit = phase.equals("before") ? 71 : 72;
      assertEquals(exit, runChild(phase, database, inputsPath), Files.readString(error(phase)));

      SessionStore sessions =
          SessionStore.open(database, configuration(AdmissionStore.RestartSafety.IDEMPOTENT));
      try (InputStore inputs = InputStore.open(inputsPath, INPUT_LIMITS)) {
        sessions.verifyInputs(inputs);
        JobRecord recovered = job(database, AdmissionStore.RestartSafety.IDEMPOTENT);
        if (phase.equals("before")) {
          assertEquals(0, recovered.lease());
          assertNull(recovered.leaseUntil());
          ExecutionStore.Lease first = claim(sessions, inputs, 1200, 100);
          assertEquals(1, first.number());
        } else {
          assertEquals(1, recovered.lease());
          assertEquals(1500, recovered.leaseUntil());
          assertCode(ProtocolError.Code.NOT_READY, () -> claim(sessions, inputs, 1200, 100));
          ExecutionStore.Lease replacement = claim(sessions, inputs, 1500, 100);
          assertEquals(2, replacement.number());
          assertEquals(1, replacement.attempt());
        }
        Records.WorkView view = view(sessions);
        assertEquals(1, view.attempt());
        assertEquals(1000, view.admittedAt());
        assertEquals(2000, view.deadline());
      }
    }
  }

  @Test
  void allRestartContractsSurviveClaimRenewAndReopen() throws Exception {
    for (AdmissionStore.RestartSafety safety : AdmissionStore.RestartSafety.values()) {
      Path database = directory.resolve(safety + ".sqlite");
      Path inputsPath = directory.resolve(safety + "-inputs");
      prepare(database, inputsPath, safety);
      SessionStore sessions = SessionStore.open(database, configuration(safety));
      try (InputStore inputs = InputStore.open(inputsPath, INPUT_LIMITS)) {
        ExecutionStore.Lease lease = claim(sessions, inputs, 1100, 200);
        ExecutionStore.Lease renewed =
            sessions.renewExecution(execAccess(), lease, 300, clock(1200), ALLOW);
        assertEquals(lease.number(), renewed.number());
        assertEquals(1500, renewed.until());
      }
      SessionStore reopened = SessionStore.open(database, configuration(safety));
      try (InputStore inputs = InputStore.open(inputsPath, INPUT_LIMITS)) {
        reopened.verifyInputs(inputs);
        JobRecord retained = job(database, safety);
        assertEquals(safety, retained.safety());
        assertEquals(1, retained.attempt());
        assertEquals(1, retained.lease());
        assertEquals(1500, retained.leaseUntil());
      }
    }
  }

  @Test
  void retryableFailureThenDeadlineExpirySpendsBothSettlementCreditsAndRecoversTerminal()
      throws Exception {
    Path database = directory.resolve("retry-expire.sqlite");
    Path inputsPath = directory.resolve("retry-expire-inputs");
    prepare(database, inputsPath, AdmissionStore.RestartSafety.IDEMPOTENT);
    SessionStore sessions =
        SessionStore.open(database, configuration(AdmissionStore.RestartSafety.IDEMPOTENT));
    long[] before = credits(database);
    try (InputStore inputs = InputStore.open(inputsPath, INPUT_LIMITS)) {
      ExecutionStore.Lease lease = claim(sessions, inputs, 1100, 200);
      Records.WorkView retry =
          sessions.failExecution(
              execAccess(), lease, new Records.Diagnostic(4, "retry"), true, clock(1200), ALLOW);
      assertEquals(Records.State.AWAITING_RETRY, retry.state());
      Records.WorkView expired = sessions.expireExecution(1, WORK, clock(2000));
      assertEquals(Records.State.FAILED, expired.state());
    }
    long[] after = credits(database);
    assertEquals(before[0] - 2, after[0]);
    assertEquals(before[1] - 2, after[1]);
    SessionStore.open(database, configuration(AdmissionStore.RestartSafety.IDEMPOTENT));
    assertEquals(Records.State.FAILED, view(sessions).state());
  }

  public static void main(String[] args) throws Exception {
    String phase = args[0];
    Path database = Path.of(args[1]);
    Path inputsPath = Path.of(args[2]);
    SessionStore sessions =
        SessionStore.open(database, configuration(AdmissionStore.RestartSafety.IDEMPOTENT));
    InputStore inputs = InputStore.open(inputsPath, INPUT_LIMITS);
    AtomicInteger checks = new AtomicInteger();
    ExecutionStore.Access access =
        new ExecutionStore.Access(
            "alice",
            () -> {
              if (phase.equals("before") && checks.incrementAndGet() == 3)
                Runtime.getRuntime().halt(71);
            });
    sessions.claimExecution(access, 1, WORK, inputs, 400, clock(1100), ALLOW);
    Runtime.getRuntime().halt(72);
  }

  private static void prepare(Path database, Path inputsPath, AdmissionStore.RestartSafety safety)
      throws Exception {
    SessionStore sessions = SessionStore.initialize(database, configuration(safety));
    sessions.create(
        sessionAccess(),
        SELECTED,
        new Messages.Create(1, 1, new Records.Policy(10_000, 20_000, 30_000)));
    sessions.declare(
        sessionAccess(), SELECTED, 1, new Messages.Declare(2, operation(1), 0, List.of(1L), false));
    try (InputStore inputs =
        InputStore.initializeForAuthority(inputsPath, INPUT_LIMITS, sessions.identity())) {
      sessions.bindInputs(inputs);
      Records.InputHeader header = header();
      try (InputStore.Receiver receiver = inputs.begin(context(), header, SELECTED, 1)) {
        receiver.write(ByteBuffer.allocate(0), 2);
        receiver.finish(3);
      }
      sessions.admit(sessionAccess(), SELECTED, 1, inputs, header, 2, clock(1000), ALLOW);
    }
  }

  private int runChild(String phase, Path database, Path inputs) throws Exception {
    Process process =
        new ProcessBuilder(
                Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-cp",
                System.getProperty("java.class.path"),
                ExecutionStoreRecoveryTest.class.getName(),
                phase,
                database.toString(),
                inputs.toString())
            .redirectOutput(directory.resolve(phase + ".out").toFile())
            .redirectError(error(phase).toFile())
            .start();
    try {
      assertTrue(process.waitFor(8, TimeUnit.SECONDS), "execution child did not exit");
      return process.exitValue();
    } finally {
      if (process.isAlive()) {
        process.destroyForcibly();
        assertTrue(process.waitFor(2, TimeUnit.SECONDS));
      }
    }
  }

  private static ExecutionStore.Lease claim(
      SessionStore sessions, InputStore inputs, long now, long duration) throws Exception {
    return sessions.claimExecution(execAccess(), 1, WORK, inputs, duration, clock(now), ALLOW);
  }

  private static Records.WorkView view(SessionStore sessions) throws Exception {
    return sessions
        .snapshot(sessionAccess(), SELECTED, 1, new Messages.Watch(9, WORK, 0, 0))
        .work();
  }

  private static JobRecord job(Path database, AdmissionStore.RestartSafety safety)
      throws Exception {
    try (var connection = BoundedSqlite.open(database, configuration(safety).files()).connect()) {
      Messages.Binding binding =
          new Messages.Binding(
              1,
              "issuer-a",
              "alice",
              1,
              1,
              new Records.Policy(10_000, 20_000, 30_000),
              new Records.Limits(8, 16, 16, 1 << 20, 1 << 20, 4));
      return AdmissionStore.job(connection, binding, WORK).record();
    }
  }

  private static long[] credits(Path database) throws Exception {
    try (var connection =
            BoundedSqlite.open(
                    database, configuration(AdmissionStore.RestartSafety.IDEMPOTENT).files())
                .connect();
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

  private static SessionStore.Configuration configuration(AdmissionStore.RestartSafety safety) {
    return new SessionStore.Configuration(
        "issuer-a",
        new Records.Limits(8, 16, 16, 1 << 20, 1 << 20, 4),
        new Records.Policy(60_000, 60_000, 60_000),
        2,
        8,
        4,
        BoundedSqlite.Limits.defaults(),
        new AdmissionStore.ExecutionPolicy(
            List.of(new AdmissionStore.Application("copy", Set.of(0), safety)), 4, 4));
  }

  private static Records.InputHeader header() throws Exception {
    return new Records.InputHeader(
        1,
        operation(2),
        new Records.AdmitParameters(
            WORK,
            new Records.Input(
                0,
                new Records.Digest(MessageDigest.getInstance("SHA-256").digest(new byte[0])),
                "application/octet-stream"),
            "copy",
            0,
            1000,
            new Records.OutputBudget(0, 0)));
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

  private Path error(String phase) {
    return directory.resolve(phase + ".err");
  }

  private static void assertCode(ProtocolError.Code expected, Throwing action) {
    ProtocolError error = assertThrows(ProtocolError.class, action::run);
    assertEquals(expected, error.code(), error::getMessage);
  }

  @FunctionalInterface
  private interface Throwing {
    void run() throws Exception;
  }
}
