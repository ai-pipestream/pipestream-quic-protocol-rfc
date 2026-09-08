package ai.pipestream.quic.v2;

import static ai.pipestream.quic.v2.Messages.DURABLE_WORK;
import static org.junit.jupiter.api.Assertions.*;

import ai.pipestream.quic.BoundedSqlite;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.sql.Connection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

@Timeout(55)
final class ClosureReconciliationRecoveryTest {
  private static final Messages.Capabilities SELECTED =
      new Messages.Capabilities(
          true, List.of(DURABLE_WORK), List.of(), 1 << 20, 8, 16, 1 << 20, 1000, 5000);
  private static final InputStore.Limits INPUT_LIMITS =
      new InputStore.Limits(4L << 20, 32, 1 << 20, 8);
  private static final PublicationStore.Endpoint ENDPOINT =
      new PublicationStore.Endpoint("results.example:7443");
  private static final AdmissionStore.Authorization ALLOW = (binding, parameters) -> {};

  @TempDir Path directory;

  @Test
  void crashBeforeAndAfterClosureCommitRecoversExactlyOnce() throws Exception {
    for (ClosureStore.Phase phase : ClosureStore.Phase.values()) {
      int exit = phase == ClosureStore.Phase.BEFORE_COMMIT ? 121 : 122;
      Path database = directory.resolve(phase + ".sqlite");
      Path inputsPath = directory.resolve(phase + "-inputs");
      Path output = directory.resolve(phase + ".out");
      Path error = directory.resolve(phase + ".err");
      Process child =
          new ProcessBuilder(
                  Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                  "-cp",
                  System.getProperty("java.class.path"),
                  ClosureReconciliationRecoveryTest.class.getName(),
                  database.toString(),
                  inputsPath.toString(),
                  phase.name(),
                  Integer.toString(exit))
              .redirectOutput(output.toFile())
              .redirectError(error.toFile())
              .start();
      try {
        assertTrue(child.waitFor(10, TimeUnit.SECONDS), "closure child did not exit");
        assertEquals(exit, child.exitValue(), boundedText(error, 8192));
      } finally {
        if (child.isAlive()) {
          child.destroyForcibly();
          assertTrue(child.waitFor(2, TimeUnit.SECONDS));
        }
      }
      long initialCredits = childCredits(output);

      SessionStore sessions = SessionStore.open(database, configuration());
      try (InputStore inputs = InputStore.open(inputsPath, INPUT_LIMITS)) {
        sessions.verifyInputs(inputs);
        Messages.Binding binding =
            sessions.attach(access(), SELECTED, new Messages.Attach(1, "issuer-a", "alice", 1));
        Records.Digest seal = expectedSeal(binding);
        Records.ScopeSummary expected = expectedSummary(sessions, seal);
        long recoveredCredits = scopeCredits(database, binding);

        if (phase == ClosureStore.Phase.BEFORE_COMMIT) {
          assertEquals(initialCredits, recoveredCredits);
          assertCode(
              ProtocolError.Code.NOT_READY,
              () -> sessions.scopeSummary(access(), SELECTED, 1, 0, seal));
          ClosureStore.Progress replay =
              sessions.reconcileClosures(new ClosureStore.Cursor(), 2, clock(1200));
          assertEquals(1, replay.inspectedScopes());
          assertEquals(2, replay.inspectedMembers());
          assertEquals(1, replay.closedScopes());
          assertEquals(0, replay.settledParents());
          assertEquals(expected, sessions.scopeSummary(access(), SELECTED, 1, 0, seal));
          assertEquals(initialCredits - 1, scopeCredits(database, binding));
        } else {
          assertEquals(initialCredits - 1, recoveredCredits);
          assertEquals(expected, sessions.scopeSummary(access(), SELECTED, 1, 0, seal));
          ClosureStore.Progress replay =
              sessions.reconcileClosures(new ClosureStore.Cursor(), 2, clock(1200));
          assertEquals(1, replay.inspectedScopes());
          assertEquals(0, replay.inspectedMembers());
          assertEquals(0, replay.closedScopes());
          assertEquals(0, replay.settledParents());
          assertEquals(initialCredits - 1, scopeCredits(database, binding));
          assertEquals(expected, sessions.scopeSummary(access(), SELECTED, 1, 0, seal));
        }
      }
    }
  }

  @Test
  void strictChildSummaryAndParentFailureAreAtomicAcrossCommitCrashes() throws Exception {
    for (ClosureStore.Phase phase : ClosureStore.Phase.values()) {
      int exit = phase == ClosureStore.Phase.BEFORE_COMMIT ? 123 : 124;
      Path database = directory.resolve("strict-" + phase + ".sqlite");
      Path inputsPath = directory.resolve("strict-" + phase + "-inputs");
      Path output = directory.resolve("strict-" + phase + ".out");
      Path error = directory.resolve("strict-" + phase + ".err");
      Process child =
          new ProcessBuilder(
                  Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                  "-cp",
                  System.getProperty("java.class.path"),
                  ClosureReconciliationRecoveryTest.class.getName(),
                  database.toString(),
                  inputsPath.toString(),
                  phase.name(),
                  Integer.toString(exit),
                  "strict")
              .redirectOutput(output.toFile())
              .redirectError(error.toFile())
              .start();
      try {
        assertTrue(child.waitFor(10, TimeUnit.SECONDS), "strict closure child did not exit");
        assertEquals(exit, child.exitValue(), boundedText(error, 8192));
      } finally {
        if (child.isAlive()) {
          child.destroyForcibly();
          assertTrue(child.waitFor(2, TimeUnit.SECONDS));
        }
      }
      StrictCredits initial = strictCredits(output);

      SessionStore sessions = SessionStore.open(database, configuration());
      try (InputStore inputs = InputStore.open(inputsPath, INPUT_LIMITS)) {
        sessions.verifyInputs(inputs);
        Messages.Binding binding =
            sessions.attach(access(), SELECTED, new Messages.Attach(1, "issuer-a", "alice", 1));
        Records.WorkView parent = view(sessions, new Records.WorkKey(0, 0, 1));
        Records.ChildScope childScope = parent.child();
        assertNotNull(childScope);
        Records.WorkKey childWork =
            new Records.WorkKey(childScope.scope(), childScope.producer(), 2);
        Records.WorkView failedChild = view(sessions, childWork);
        Records.Digest childSeal =
            seal(binding, childScope.scope(), childScope.producer(), parent.work(), List.of(2L));
        Commitments.StatusTree statuses =
            new Commitments.StatusTree(childScope.scope(), childScope.producer(), 1);
        statuses.add(failedChild, null);
        Commitments.Status status = statuses.finish();
        Records.ScopeSummary expected =
            new Records.ScopeSummary(
                childScope.scope(),
                childScope.producer(),
                parent.work(),
                childSeal,
                1,
                status.counts(),
                status.root(),
                1200);
        StrictCredits recovered = strictCredits(database, binding, childScope.scope());

        if (phase == ClosureStore.Phase.BEFORE_COMMIT) {
          assertEquals(initial, recovered);
          assertEquals(Records.State.WAITING_CHILDREN, parent.state());
          assertCode(
              ProtocolError.Code.NOT_READY,
              () -> sessions.scopeSummary(access(), SELECTED, 1, childScope.scope(), childSeal));
          ClosureStore.Progress replay =
              sessions.reconcileClosures(new ClosureStore.Cursor(), 1, clock(1200));
          assertEquals(1, replay.closedScopes());
          assertEquals(1, replay.settledParents());
        } else {
          assertEquals(initial.childScope() - 1, recovered.childScope());
          assertEquals(initial.parentWork() - 1, recovered.parentWork());
          assertEquals(initial.parentJob() - 1, recovered.parentJob());
          assertEquals(Records.State.FAILED, parent.state());
        }
        assertEquals(
            expected, sessions.scopeSummary(access(), SELECTED, 1, childScope.scope(), childSeal));
        Records.WorkView failedParent = view(sessions, parent.work());
        assertEquals(Records.State.FAILED, failedParent.state());
        assertEquals(ProtocolError.Code.CONFLICT.value(), failedParent.diagnostic().code());
        assertNull(failedParent.manifest());
        StrictCredits committed = strictCredits(database, binding, childScope.scope());
        assertEquals(initial.childScope() - 1, committed.childScope());
        assertEquals(initial.parentWork() - 1, committed.parentWork());
        assertEquals(initial.parentJob() - 1, committed.parentJob());

        ClosureStore.Progress replay =
            sessions.reconcileClosures(new ClosureStore.Cursor(), 1, clock(1200));
        assertEquals(0, replay.closedScopes());
        assertEquals(0, replay.settledParents());
        assertEquals(committed, strictCredits(database, binding, childScope.scope()));
      }
    }
  }

  public static void main(String[] args) throws Exception {
    if (args.length == 5 && args[4].equals("strict")) {
      strictMain(args);
      return;
    }
    Path database = Path.of(args[0]);
    Path inputsPath = Path.of(args[1]);
    ClosureStore.Phase target = ClosureStore.Phase.valueOf(args[2]);
    int exit = Integer.parseInt(args[3]);
    SessionStore sessions = SessionStore.initialize(database, configuration());
    Messages.Binding binding =
        sessions.create(
            access(),
            SELECTED,
            new Messages.Create(1, 1, new Records.Policy(10_000, 20_000, 30_000)));
    sessions.declare(
        access(),
        SELECTED,
        binding.generation(),
        new Messages.Declare(2, operation(1), 0, List.of(1L, 2L), true));
    InputStore inputs =
        InputStore.initializeForAuthority(inputsPath, INPUT_LIMITS, sessions.identity());
    sessions.bindInputs(inputs);
    admit(sessions, inputs, binding, new Records.WorkKey(0, 0, 1), new byte[] {1}, 2);
    admit(sessions, inputs, binding, new Records.WorkKey(0, 0, 2), new byte[] {2}, 3);

    ExecutionStore.Lease success =
        sessions.claimExecution(
            execAccess(), 1, new Records.WorkKey(0, 0, 1), inputs, 500, clock(1100), ALLOW);
    sessions.succeedExecution(execAccess(), success, inputs, 0, ENDPOINT, clock(1150), ALLOW);
    ExecutionStore.Lease failure =
        sessions.claimExecution(
            execAccess(), 1, new Records.WorkKey(0, 0, 2), inputs, 500, clock(1150), ALLOW);
    sessions.failExecution(
        execAccess(),
        failure,
        new Records.Diagnostic(9, "member failed"),
        false,
        clock(1160),
        ALLOW);

    System.out.printf("credits=%d%n", scopeCredits(database, binding));
    System.out.flush();
    sessions.reconcileClosures(
        new ClosureStore.Cursor(),
        2,
        clock(1200),
        phase -> {
          if (phase == target) Runtime.getRuntime().halt(exit);
        });
    throw new AssertionError("closure probe was not reached");
  }

  private static void strictMain(String[] args) throws Exception {
    Path database = Path.of(args[0]);
    Path inputsPath = Path.of(args[1]);
    ClosureStore.Phase target = ClosureStore.Phase.valueOf(args[2]);
    int exit = Integer.parseInt(args[3]);
    SessionStore sessions = SessionStore.initialize(database, configuration());
    Messages.Binding binding =
        sessions.create(
            access(),
            SELECTED,
            new Messages.Create(1, 1, new Records.Policy(10_000, 20_000, 30_000)));
    Records.WorkKey parent = new Records.WorkKey(0, 0, 1);
    sessions.declare(
        access(), SELECTED, 1, new Messages.Declare(2, operation(1), 0, List.of(1L), true));
    InputStore inputs =
        InputStore.initializeForAuthority(inputsPath, INPUT_LIMITS, sessions.identity());
    sessions.bindInputs(inputs);
    admit(sessions, inputs, binding, parent, new byte[] {1}, 2, 1);
    Records.ChildScope childScope = view(sessions, parent).child();
    if (childScope == null) throw new AssertionError("strict child scope was not allocated");
    Records.WorkKey child = new Records.WorkKey(childScope.scope(), childScope.producer(), 2);
    sessions.declare(
        access(),
        SELECTED,
        1,
        new Messages.Declare(4, operation(3), childScope.scope(), List.of(2L), true));
    admit(sessions, inputs, binding, child, new byte[] {2}, 4, 0);
    ExecutionStore.Lease lease =
        sessions.claimExecution(execAccess(), 1, child, inputs, 500, clock(1100), ALLOW);
    sessions.failExecution(
        execAccess(), lease, new Records.Diagnostic(9, "child failed"), false, clock(1150), ALLOW);
    StrictCredits credits = strictCredits(database, binding, childScope.scope());
    System.out.printf(
        "child=%d work=%d job=%d%n",
        credits.childScope(), credits.parentWork(), credits.parentJob());
    System.out.flush();
    sessions.reconcileClosures(
        new ClosureStore.Cursor(),
        1,
        clock(1200),
        observed -> {
          if (observed == target) Runtime.getRuntime().halt(exit);
        });
    throw new AssertionError("strict closure probe was not reached");
  }

  private static void admit(
      SessionStore sessions,
      InputStore inputs,
      Messages.Binding binding,
      Records.WorkKey work,
      byte[] payload,
      int operation)
      throws Exception {
    admit(sessions, inputs, binding, work, payload, operation, 0);
  }

  private static void admit(
      SessionStore sessions,
      InputStore inputs,
      Messages.Binding binding,
      Records.WorkKey work,
      byte[] payload,
      int operation,
      int mode)
      throws Exception {
    Records.InputHeader header =
        new Records.InputHeader(
            binding.generation(),
            operation(operation),
            new Records.AdmitParameters(
                work,
                new Records.Input(payload.length, digest(payload), "application/octet-stream"),
                "copy",
                mode,
                1000,
                new Records.OutputBudget(0, 0)));
    Commitments.Context context =
        new Commitments.Context(binding.authority(), binding.owner(), binding.generation());
    try (InputStore.Receiver receiver = inputs.begin(context, header, SELECTED, operation)) {
      receiver.write(ByteBuffer.wrap(payload), operation + 1L);
      receiver.finish(operation + 2L);
    }
    sessions.admit(
        access(),
        SELECTED,
        binding.generation(),
        inputs,
        header,
        operation + 3L,
        clock(1000),
        ALLOW);
  }

  private static Records.ScopeSummary expectedSummary(SessionStore sessions, Records.Digest seal)
      throws Exception {
    Records.WorkView success = view(sessions, new Records.WorkKey(0, 0, 1));
    Records.WorkView failure = view(sessions, new Records.WorkKey(0, 0, 2));
    assertEquals(Records.State.SUCCEEDED, success.state());
    assertEquals(Records.State.FAILED, failure.state());
    Commitments.StatusTree tree = new Commitments.StatusTree(0, 0, 2);
    tree.add(success, null);
    tree.add(failure, null);
    Commitments.Status status = tree.finish();
    assertEquals(new Records.Counts(1, 1, 0, 0), status.counts());
    return new Records.ScopeSummary(0, 0, null, seal, 2, status.counts(), status.root(), 1200);
  }

  private static Records.Digest expectedSeal(Messages.Binding binding) {
    Commitments.Seal seal =
        new Commitments.Seal(
            new Commitments.Context(binding.authority(), binding.owner(), binding.generation()),
            0,
            0,
            null,
            2);
    seal.add(1);
    seal.add(2);
    return seal.finish();
  }

  private static Records.Digest seal(
      Messages.Binding binding,
      long scope,
      int producer,
      Records.WorkKey parent,
      List<Long> members) {
    Commitments.Seal seal =
        new Commitments.Seal(
            new Commitments.Context(binding.authority(), binding.owner(), binding.generation()),
            scope,
            producer,
            parent,
            members.size());
    for (long member : members) seal.add(member);
    return seal.finish();
  }

  private static Records.WorkView view(SessionStore sessions, Records.WorkKey work)
      throws Exception {
    return sessions.snapshot(access(), SELECTED, 1, new Messages.Watch(9, work, 0, 0)).work();
  }

  private static long scopeCredits(Path database, Messages.Binding binding) throws Exception {
    try (Connection connection = BoundedSqlite.open(database, configuration().files()).connect();
        var query =
            connection.prepareStatement(
                "SELECT state_slot FROM ps_v2_scopes WHERE generation=? AND id=0 AND producer=0")) {
      query.setLong(1, binding.generation());
      try (var row = query.executeQuery()) {
        assertTrue(row.next());
        long slot = row.getLong(1);
        assertFalse(row.next());
        return FixedRecords.read(
                connection,
                slot,
                FixedRecords.Kind.SCOPE,
                FixedRecords.key(binding, FixedRecords.Kind.SCOPE, 0, 0, 0, null))
            .header()
            .credits();
      }
    }
  }

  private static long childCredits(Path output) throws Exception {
    String text = boundedText(output, 128);
    assertTrue(text.matches("credits=[0-9]+\\n"), text);
    return Long.parseLong(text.trim().substring("credits=".length()));
  }

  private static StrictCredits strictCredits(Path output) throws Exception {
    String text = boundedText(output, 128);
    assertTrue(text.matches("child=[0-9]+ work=[0-9]+ job=[0-9]+\\n"), text);
    String[] fields = text.trim().split(" ");
    return new StrictCredits(value(fields[0]), value(fields[1]), value(fields[2]));
  }

  private static StrictCredits strictCredits(
      Path database, Messages.Binding binding, long childScope) throws Exception {
    try (Connection connection = BoundedSqlite.open(database, configuration().files()).connect()) {
      long childSlot;
      try (var query =
          connection.prepareStatement(
              "SELECT state_slot FROM ps_v2_scopes WHERE generation=? AND id=?")) {
        query.setLong(1, binding.generation());
        query.setLong(2, childScope);
        try (var row = query.executeQuery()) {
          assertTrue(row.next());
          childSlot = row.getLong(1);
          assertFalse(row.next());
        }
      }
      long workSlot;
      long jobSlot;
      try (var query =
          connection.prepareStatement(
              "SELECT e.view_slot,j.state_slot FROM ps_v2_entities e JOIN ps_v2_jobs j ON"
                  + " e.generation=j.generation AND e.scope=j.scope AND e.id=j.entity"
                  + " WHERE e.generation=? AND e.scope=0 AND e.id=1")) {
        query.setLong(1, binding.generation());
        try (var row = query.executeQuery()) {
          assertTrue(row.next());
          workSlot = row.getLong(1);
          jobSlot = row.getLong(2);
          assertFalse(row.next());
        }
      }
      return new StrictCredits(
          FixedRecords.header(connection, childSlot, FixedRecords.Kind.SCOPE).credits(),
          FixedRecords.header(connection, workSlot, FixedRecords.Kind.WORK).credits(),
          FixedRecords.header(connection, jobSlot, FixedRecords.Kind.JOB).credits());
    }
  }

  private static long value(String field) {
    return Long.parseLong(field.substring(field.indexOf('=') + 1));
  }

  private static String boundedText(Path path, int limit) throws Exception {
    if (!Files.exists(path)) return "";
    try (InputStream input = Files.newInputStream(path)) {
      byte[] bytes = input.readNBytes(limit + 1);
      return new String(bytes, 0, Math.min(bytes.length, limit), StandardCharsets.UTF_8);
    }
  }

  private static SessionStore.Configuration configuration() {
    AdmissionStore.Application application =
        new AdmissionStore.Application(
            "copy", Set.of(0, 1), AdmissionStore.RestartSafety.IDEMPOTENT);
    return new SessionStore.Configuration(
        "issuer-a",
        new Records.Limits(8, 16, 16, 1 << 20, 1 << 20, 4),
        new Records.Policy(60_000, 60_000, 60_000),
        2,
        8,
        4,
        BoundedSqlite.Limits.defaults(),
        new AdmissionStore.ExecutionPolicy(List.of(application), 4, 4));
  }

  private static Records.Digest digest(byte[] bytes) throws Exception {
    return new Records.Digest(MessageDigest.getInstance("SHA-256").digest(bytes));
  }

  private static Records.OperationId operation(int value) {
    byte[] bytes = new byte[16];
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

  private record StrictCredits(long childScope, long parentWork, long parentJob) {}

  @FunctionalInterface
  private interface Throwing {
    void run() throws Exception;
  }
}
