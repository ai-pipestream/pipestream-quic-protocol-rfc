package ai.pipestream.quic.v2;

import static ai.pipestream.quic.v2.Messages.DURABLE_WORK;
import static org.junit.jupiter.api.Assertions.*;

import ai.pipestream.quic.BoundedSqlite;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

@Timeout(20)
final class ExecutionDiscoveryTest {
  private static final Messages.Capabilities SELECTED =
      new Messages.Capabilities(
          true, List.of(DURABLE_WORK), List.of(), 1 << 20, 8, 16, 1 << 20, 1000, 5000);
  private static final InputStore.Limits INPUT_LIMITS =
      new InputStore.Limits(8L << 20, 128, 1 << 20, 16);
  private static final AdmissionStore.Authorization ALLOW = (binding, parameters) -> {};

  @TempDir Path directory;

  @Test
  void boundedPagesAreStableAcrossOwnersGenerationsAndScopes() throws Exception {
    try (Harness harness = harness("ordering")) {
      Messages.Binding alice = harness.create("alice", 1);
      Records.WorkKey rootThree = new Records.WorkKey(0, 0, 3);
      Records.WorkKey rootNine = new Records.WorkKey(0, 0, 9);
      harness.declare(alice, 0, List.of(3L, 9L), false, 1);
      harness.admit(alice, rootNine, 1, 2);
      harness.admit(alice, rootThree, 0, 3);
      Records.ChildScope child = harness.view(alice, rootNine).child();
      assertNotNull(child);
      Records.WorkKey childTwo = new Records.WorkKey(child.scope(), 0, 2);
      harness.declare(alice, child.scope(), List.of(2L), false, 4);
      harness.admit(alice, childTwo, 0, 5);

      Messages.Binding bob = harness.create("bob", 1);
      Records.WorkKey bobOne = new Records.WorkKey(0, 0, 1);
      harness.declare(bob, 0, List.of(1L), false, 6);
      harness.admit(bob, bobOne, 0, 7);

      ExecutionStore.Page first = harness.sessions().scanExecutions(null, 2);
      assertEquals(
          List.of(
              new ExecutionStore.Position(alice.generation(), 0, 3),
              new ExecutionStore.Position(alice.generation(), 0, 9)),
          positions(first.entries()));
      assertNotNull(first.next());
      ExecutionStore.Page second = harness.sessions().scanExecutions(first.next(), 2);
      assertEquals(
          List.of(
              new ExecutionStore.Position(alice.generation(), child.scope(), 2),
              new ExecutionStore.Position(bob.generation(), 0, 1)),
          positions(second.entries()));
      assertNull(second.next());
      assertEquals(List.of("alice", "alice"), owners(first.entries()));
      assertEquals(List.of("alice", "bob"), owners(second.entries()));
      assertEquals(List.of(0, 1, 0, 0), modes(first.entries(), second.entries()));
      assertEquals(
          List.of(JobRecord.Stage.QUEUED, JobRecord.Stage.WAITING_CHILDREN),
          first.entries().stream().map(ExecutionStore.Candidate::stage).toList());
      assertTrue(second.entries().stream().allMatch(candidate -> candidate.deadline() == 2000));
    }
  }

  @Test
  void pageBoundsAndCursorPositionsRefuseInvalidValues() throws Exception {
    assertCode(ProtocolError.Code.FRAME_ERROR, () -> new ExecutionStore.Position(0, 0, 1));
    assertCode(ProtocolError.Code.FRAME_ERROR, () -> new ExecutionStore.Position(1, -1, 1));
    assertCode(ProtocolError.Code.FRAME_ERROR, () -> new ExecutionStore.Position(1, 0, 0));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new ExecutionStore.ScanCursor(
                new ExecutionStore.Position(2, 0, 1), new ExecutionStore.Position(1, 0, 1)));
    try (Harness harness = harness("bounds")) {
      assertCode(
          ProtocolError.Code.LIMIT_EXCEEDED, () -> harness.sessions().scanExecutions(null, 0));
      assertCode(
          ProtocolError.Code.LIMIT_EXCEEDED, () -> harness.sessions().scanExecutions(null, 65));
    }
  }

  @Test
  void oneSweepExcludesLaterAdmissionsBeyondItsFixedCeiling() throws Exception {
    try (Harness harness = harness("ceiling")) {
      Messages.Binding firstBinding = harness.create("alice", 1);
      harness.declare(firstBinding, 0, List.of(1L, 2L), false, 1);
      harness.admit(firstBinding, new Records.WorkKey(0, 0, 1), 0, 2);
      harness.admit(firstBinding, new Records.WorkKey(0, 0, 2), 0, 3);

      ExecutionStore.Page first = harness.sessions().scanExecutions(null, 1);
      assertEquals(
          List.of(new ExecutionStore.Position(firstBinding.generation(), 0, 1)),
          positions(first.entries()));
      assertNotNull(first.next());

      Messages.Binding laterBinding = harness.create("bob", 1);
      harness.declare(laterBinding, 0, List.of(1L), false, 4);
      harness.admit(laterBinding, new Records.WorkKey(0, 0, 1), 0, 5);

      ExecutionStore.Page remainder = harness.sessions().scanExecutions(first.next(), 64);
      assertEquals(
          List.of(new ExecutionStore.Position(firstBinding.generation(), 0, 2)),
          positions(remainder.entries()));
      assertNull(remainder.next());

      ExecutionStore.Page fresh = harness.sessions().scanExecutions(null, 64);
      assertEquals(
          List.of(
              new ExecutionStore.Position(firstBinding.generation(), 0, 1),
              new ExecutionStore.Position(firstBinding.generation(), 0, 2),
              new ExecutionStore.Position(laterBinding.generation(), 0, 1)),
          positions(fresh.entries()));
    }
  }

  @Test
  void scanReportsRetryAndSettledStagesWithoutChangingThem() throws Exception {
    try (Harness harness = harness("stages")) {
      Messages.Binding binding = harness.create("alice", 1);
      Records.WorkKey retry = new Records.WorkKey(0, 0, 1);
      Records.WorkKey settled = new Records.WorkKey(0, 0, 2);
      harness.declare(binding, 0, List.of(1L, 2L), false, 1);
      harness.admit(binding, retry, 0, 2);
      harness.admit(binding, settled, 0, 3);
      ExecutionStore.Lease retryLease = harness.claim(binding, retry, 1100, 200);
      ExecutionStore.Lease settledLease = harness.claim(binding, settled, 1100, 200);
      harness
          .sessions()
          .failExecution(
              execAccess("alice"),
              retryLease,
              new Records.Diagnostic(7, "retry"),
              true,
              clock(1150),
              ALLOW);
      harness
          .sessions()
          .failExecution(
              execAccess("alice"),
              settledLease,
              new Records.Diagnostic(8, "terminal"),
              false,
              clock(1150),
              ALLOW);

      ExecutionStore.Page first = harness.sessions().scanExecutions(null, 64);
      assertEquals(
          List.of(JobRecord.Stage.AWAITING_RETRY, JobRecord.Stage.SETTLED),
          first.entries().stream().map(ExecutionStore.Candidate::stage).toList());
      assertTrue(first.entries().stream().allMatch(candidate -> candidate.leaseUntil() == null));
      assertEquals(Records.State.AWAITING_RETRY, harness.view(binding, retry).state());
      assertEquals(Records.State.FAILED, harness.view(binding, settled).state());
      assertEquals(first, harness.sessions().scanExecutions(null, 64));
    }
  }

  @Test
  void reopenedStoreDiscoversExpiredCommittedLeaseWithoutFabricatingReplacement() throws Exception {
    Path database = directory.resolve("reopen.sqlite");
    Path inputsPath = directory.resolve("reopen-inputs");
    Messages.Binding binding;
    Records.WorkKey work = new Records.WorkKey(0, 0, 1);
    try (InputStore inputs =
        InputStore.initializeForAuthority(
            inputsPath, INPUT_LIMITS, initialize(database).identity())) {
      SessionStore sessions = SessionStore.open(database, configuration());
      sessions.bindInputs(inputs);
      binding = create(sessions, "alice", 1);
      declare(sessions, binding, 0, List.of(1L), false, 1);
      admit(sessions, inputs, binding, work, 0, 2);
      ExecutionStore.Lease lease =
          sessions.claimExecution(execAccess("alice"), 1, work, inputs, 100, clock(1100), ALLOW);
      assertEquals(1200, lease.until());
      assertEquals(1, lease.number());
    }

    SessionStore reopened = SessionStore.open(database, configuration());
    try (InputStore inputs = InputStore.open(inputsPath, INPUT_LIMITS)) {
      reopened.verifyInputs(inputs);
      ExecutionStore.Candidate candidate =
          assertSingle(reopened.scanExecutions(null, 64).entries());
      assertEquals(JobRecord.Stage.EXECUTING, candidate.stage());
      assertEquals(1200L, candidate.leaseUntil());
      assertEquals(1, reopened.scanExecutions(null, 64).entries().size());
      assertEquals(1, view(reopened, binding, work).attempt());

      ExecutionStore.Lease replacement =
          reopened.claimExecution(execAccess("alice"), 1, work, inputs, 100, clock(1201), ALLOW);
      assertEquals(2, replacement.number());
      assertEquals(1, replacement.attempt());
    }
  }

  private Harness harness(String name) throws Exception {
    Path database = directory.resolve(name + ".sqlite");
    SessionStore sessions = initialize(database);
    Path inputsPath = directory.resolve(name + "-inputs");
    InputStore inputs =
        InputStore.initializeForAuthority(inputsPath, INPUT_LIMITS, sessions.identity());
    sessions.bindInputs(inputs);
    return new Harness(sessions, inputs);
  }

  private static SessionStore initialize(Path database) throws Exception {
    return SessionStore.initialize(database, configuration());
  }

  private static SessionStore.Configuration configuration() {
    AdmissionStore.Application application =
        new AdmissionStore.Application(
            "worker", Set.of(0, 1), AdmissionStore.RestartSafety.IDEMPOTENT);
    return new SessionStore.Configuration(
        "issuer-a",
        new Records.Limits(16, 64, 64, 1 << 20, 1 << 20, 8),
        new Records.Policy(60_000, 60_000, 60_000),
        8,
        64,
        8,
        BoundedSqlite.Limits.defaults(),
        new AdmissionStore.ExecutionPolicy(List.of(application), 8, 8));
  }

  private static Messages.Binding create(SessionStore sessions, String owner, long creationSequence)
      throws Exception {
    return sessions.create(
        sessionAccess(owner),
        SELECTED,
        new Messages.Create(1, creationSequence, new Records.Policy(10_000, 20_000, 30_000)));
  }

  private static void declare(
      SessionStore sessions,
      Messages.Binding binding,
      long scope,
      List<Long> entities,
      boolean closed,
      int operation)
      throws Exception {
    sessions.declare(
        sessionAccess(binding.owner()),
        SELECTED,
        binding.generation(),
        new Messages.Declare(2, operation(operation), scope, entities, closed));
  }

  private static void admit(
      SessionStore sessions,
      InputStore inputs,
      Messages.Binding binding,
      Records.WorkKey work,
      int mode,
      int operation)
      throws Exception {
    byte[] payload = new byte[0];
    Records.InputHeader header =
        new Records.InputHeader(
            binding.generation(),
            operation(operation),
            new Records.AdmitParameters(
                work,
                new Records.Input(0, digest(payload), "application/octet-stream"),
                "worker",
                mode,
                1000,
                new Records.OutputBudget(0, 0)));
    Commitments.Context context =
        new Commitments.Context("issuer-a", binding.owner(), binding.generation());
    try (InputStore.Receiver receiver = inputs.begin(context, header, SELECTED, 1)) {
      receiver.write(ByteBuffer.wrap(payload), 2);
      receiver.finish(3);
    }
    sessions.admit(
        sessionAccess(binding.owner()),
        SELECTED,
        binding.generation(),
        inputs,
        header,
        2,
        clock(1000),
        ALLOW);
  }

  private static Records.WorkView view(
      SessionStore sessions, Messages.Binding binding, Records.WorkKey work) throws Exception {
    return sessions
        .snapshot(
            sessionAccess(binding.owner()),
            SELECTED,
            binding.generation(),
            new Messages.Watch(9, work, 0, 0))
        .work();
  }

  private static List<ExecutionStore.Position> positions(
      List<ExecutionStore.Candidate> candidates) {
    return candidates.stream().map(ExecutionStore.Candidate::position).toList();
  }

  private static List<String> owners(List<ExecutionStore.Candidate> candidates) {
    return candidates.stream().map(ExecutionStore.Candidate::owner).toList();
  }

  @SafeVarargs
  private static List<Integer> modes(List<ExecutionStore.Candidate>... pages) {
    List<Integer> result = new ArrayList<>();
    for (List<ExecutionStore.Candidate> page : pages)
      page.stream().map(ExecutionStore.Candidate::mode).forEach(result::add);
    return result;
  }

  private static <T> T assertSingle(List<T> values) {
    assertEquals(1, values.size());
    return values.get(0);
  }

  private static Records.Digest digest(byte[] bytes) throws Exception {
    return new Records.Digest(MessageDigest.getInstance("SHA-256").digest(bytes));
  }

  private static AdmissionStore.Clock clock(long time) {
    return () -> new AdmissionStore.Time(time, true);
  }

  private static Records.OperationId operation(int value) {
    byte[] bytes = new byte[16];
    bytes[15] = (byte) value;
    return new Records.OperationId(bytes);
  }

  private static SessionStore.Access sessionAccess(String owner) {
    return new SessionStore.Access(owner, () -> {});
  }

  private static ExecutionStore.Access execAccess(String owner) {
    return new ExecutionStore.Access(owner, () -> {});
  }

  private static void assertCode(ProtocolError.Code expected, Throwing action) {
    ProtocolError error = assertThrows(ProtocolError.class, action::run);
    assertEquals(expected, error.code(), error::getMessage);
  }

  private record Harness(SessionStore sessions, InputStore inputs) implements AutoCloseable {
    Messages.Binding create(String owner, long sequence) throws Exception {
      return ExecutionDiscoveryTest.create(sessions, owner, sequence);
    }

    void declare(
        Messages.Binding binding, long scope, List<Long> entities, boolean closed, int operation)
        throws Exception {
      ExecutionDiscoveryTest.declare(sessions, binding, scope, entities, closed, operation);
    }

    void admit(Messages.Binding binding, Records.WorkKey work, int mode, int operation)
        throws Exception {
      ExecutionDiscoveryTest.admit(sessions, inputs, binding, work, mode, operation);
    }

    Records.WorkView view(Messages.Binding binding, Records.WorkKey work) throws Exception {
      return ExecutionDiscoveryTest.view(sessions, binding, work);
    }

    ExecutionStore.Lease claim(
        Messages.Binding binding, Records.WorkKey work, long now, long duration) throws Exception {
      return sessions.claimExecution(
          execAccess(binding.owner()),
          binding.generation(),
          work,
          inputs,
          duration,
          clock(now),
          ALLOW);
    }

    @Override
    public void close() throws java.io.IOException {
      inputs.close();
    }
  }

  @FunctionalInterface
  private interface Throwing {
    void run() throws Exception;
  }
}
