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
final class ClosureReconciliationTest {
  private static final Messages.Capabilities SELECTED =
      new Messages.Capabilities(
          true, List.of(DURABLE_WORK), List.of(), 1 << 20, 8, 16, 1 << 20, 1000, 5000);
  private static final InputStore.Limits INPUT_LIMITS =
      new InputStore.Limits(8L << 20, 64, 1 << 20, 8);
  private static final AdmissionStore.Authorization ALLOW = (binding, parameters) -> {};

  @TempDir Path directory;

  @Test
  void sealedEmptyRootClosesWithExactIndependentCommitmentsAndSurvivesRecovery() throws Exception {
    try (Fixture fixture = new Fixture("empty")) {
      fixture.sessions.declare(
          access(),
          SELECTED,
          fixture.generation,
          new Messages.Declare(1, operation(1), 0, List.of(), true));
      Records.Digest seal = seal(fixture.binding, 0, 0, null, List.of());
      assertCode(
          ProtocolError.Code.NOT_READY,
          () -> fixture.sessions.scopeSummary(access(), SELECTED, fixture.generation, 0, seal));
      assertCode(
          ProtocolError.Code.CLOCK_UNSAFE,
          () ->
              fixture.sessions.reconcileClosures(
                  new ClosureStore.Cursor(), 1, () -> new AdmissionStore.Time(1000, false)));
      assertCode(
          ProtocolError.Code.NOT_READY,
          () -> fixture.sessions.scopeSummary(access(), SELECTED, fixture.generation, 0, seal));
      ClosureStore.Progress progress =
          fixture.sessions.reconcileClosures(new ClosureStore.Cursor(), 1, clock(1000));
      assertEquals(1, progress.inspectedScopes());
      assertEquals(0, progress.inspectedMembers());
      assertEquals(1, progress.closedScopes());
      Records.ScopeSummary summary =
          fixture.sessions.scopeSummary(access(), SELECTED, fixture.generation, 0, seal);
      assertEquals(0, summary.declared());
      assertEquals(new Records.Counts(0, 0, 0, 0), summary.counts());
      assertEquals(Commitments.emptyStatus(), summary.statusRoot());
      assertEquals(1000, summary.closedAt());
      fixture.reopen();
      assertEquals(
          summary, fixture.sessions.scopeSummary(access(), SELECTED, fixture.generation, 0, seal));
      assertCode(
          ProtocolError.Code.INTEGRITY_ERROR,
          () ->
              fixture.sessions.scopeSummary(
                  access(), SELECTED, fixture.generation, 0, digest(new byte[] {1})));
      assertCode(
          ProtocolError.Code.UNAUTHORIZED,
          () ->
              fixture.sessions.scopeSummary(
                  new SessionStore.Access("mallory", () -> {}),
                  SELECTED,
                  fixture.generation,
                  0,
                  seal));
    }
  }

  @Test
  void chunkedFoldPublishesNothingUntilAllRealTerminalMembersWereVisited() throws Exception {
    try (Fixture fixture = new Fixture("chunked")) {
      List<Long> ids = List.of(1L, 3L, Long.MAX_VALUE);
      fixture.sessions.declare(
          access(),
          SELECTED,
          fixture.generation,
          new Messages.Declare(1, operation(1), 0, ids, true));
      List<Records.WorkView> terminal = new ArrayList<>();
      int operation = 10;
      long utc = 1000;
      for (long id : ids) {
        terminal.add(fixture.fail(new Records.WorkKey(0, 0, id), operation++, utc));
        utc += 200;
      }
      Records.Digest seal = seal(fixture.binding, 0, 0, null, ids);
      ClosureStore.Cursor cursor = new ClosureStore.Cursor();
      ClosureStore.Progress first = fixture.sessions.reconcileClosures(cursor, 1, clock(1200));
      assertEquals(1, first.inspectedMembers());
      assertEquals(0, first.closedScopes());
      assertTrue(cursor.retainedHashBytes() <= 2016);
      assertCode(
          ProtocolError.Code.NOT_READY,
          () -> fixture.sessions.scopeSummary(access(), SELECTED, fixture.generation, 0, seal));
      fixture.reopen();
      cursor = new ClosureStore.Cursor();
      ClosureStore.Progress restarted = fixture.sessions.reconcileClosures(cursor, 1, clock(1600));
      assertEquals(1, restarted.inspectedMembers());
      assertEquals(0, restarted.closedScopes());
      ClosureStore.Progress second = fixture.sessions.reconcileClosures(cursor, 1, clock(1600));
      assertEquals(1, second.inspectedMembers());
      assertEquals(0, second.closedScopes());
      ClosureStore.Progress third = fixture.sessions.reconcileClosures(cursor, 1, clock(1600));
      assertEquals(1, third.inspectedMembers());
      assertEquals(1, third.closedScopes());

      Commitments.StatusTree statuses = new Commitments.StatusTree(0, 0, ids.size());
      for (Records.WorkView view : terminal) statuses.add(view, null);
      Commitments.Status expected = statuses.finish();
      Records.ScopeSummary summary =
          fixture.sessions.scopeSummary(access(), SELECTED, fixture.generation, 0, seal);
      assertEquals(expected.root(), summary.statusRoot());
      assertEquals(expected.counts(), summary.counts());
      assertEquals(new Records.Counts(0, 3, 0, 0), summary.counts());
    }
  }

  @Test
  void failedStrictChildClosesThenAtomicallyFailsParentAndAllowsRootClosure() throws Exception {
    try (Fixture fixture = new Fixture("strict-child")) {
      Records.WorkKey parent = new Records.WorkKey(0, 0, 1);
      fixture.sessions.declare(
          access(),
          SELECTED,
          fixture.generation,
          new Messages.Declare(1, operation(1), 0, List.of(1L), true));
      fixture.admit(parent, operation(2), 1, 1000);
      Records.WorkView waiting = fixture.view(parent);
      assertEquals(Records.State.WAITING_CHILDREN, waiting.state());
      Records.ChildScope child = waiting.child();
      assertNotNull(child);
      Records.WorkKey childWork = new Records.WorkKey(child.scope(), child.producer(), 2);
      fixture.sessions.declare(
          access(),
          SELECTED,
          fixture.generation,
          new Messages.Declare(3, operation(3), child.scope(), List.of(2L), true));
      fixture.fail(childWork, 4);

      ClosureStore.Cursor cursor = new ClosureStore.Cursor();
      boolean parentSettled = false;
      for (int calls = 0; calls < 8; calls++) {
        ClosureStore.Progress progress = fixture.sessions.reconcileClosures(cursor, 1, clock(1200));
        parentSettled |= progress.settledParents() > 0;
        if (fixture.view(parent).state() == Records.State.FAILED) break;
      }
      assertTrue(parentSettled);
      Records.WorkView failedParent = fixture.view(parent);
      assertEquals(Records.State.FAILED, failedParent.state());
      assertEquals(7, failedParent.diagnostic().code());
      assertNull(failedParent.manifest());
      Records.Digest childSeal =
          seal(fixture.binding, child.scope(), child.producer(), parent, List.of(2L));
      Records.ScopeSummary childSummary =
          fixture.sessions.scopeSummary(
              access(), SELECTED, fixture.generation, child.scope(), childSeal);
      assertEquals(new Records.Counts(0, 1, 0, 0), childSummary.counts());

      reconcileUntilClosed(fixture, 0, clock(1200));
      Records.Digest rootSeal = seal(fixture.binding, 0, 0, null, List.of(1L));
      Records.ScopeSummary root =
          fixture.sessions.scopeSummary(access(), SELECTED, fixture.generation, 0, rootSeal);
      assertEquals(new Records.Counts(0, 1, 0, 0), root.counts());
      assertEquals(Records.State.FAILED, fixture.view(parent).state());
    }
  }

  @Test
  void realEmptyChildClosureMakesStrictParentEligibleForExecution() throws Exception {
    try (Fixture fixture = new Fixture("empty-child")) {
      Records.WorkKey parent = new Records.WorkKey(0, 0, 1);
      fixture.sessions.declare(
          access(),
          SELECTED,
          fixture.generation,
          new Messages.Declare(1, operation(1), 0, List.of(1L), false));
      fixture.admit(parent, operation(2), 1, 1000);
      Records.ChildScope child = fixture.view(parent).child();
      assertNotNull(child);
      fixture.sessions.declare(
          access(),
          SELECTED,
          fixture.generation,
          new Messages.Declare(3, operation(3), child.scope(), List.of(), true));
      Records.Digest childSeal =
          seal(fixture.binding, child.scope(), child.producer(), parent, List.of());
      ClosureStore.Cursor cursor = new ClosureStore.Cursor();
      ClosureStore.Progress progress = fixture.sessions.reconcileClosures(cursor, 1, clock(1100));
      assertEquals(1, progress.closedScopes());
      Records.ScopeSummary summary =
          fixture.sessions.scopeSummary(
              access(), SELECTED, fixture.generation, child.scope(), childSeal);
      assertEquals(new Records.Counts(0, 0, 0, 0), summary.counts());
      ExecutionStore.Lease lease =
          fixture.sessions.claimExecution(
              execAccess(), fixture.generation, parent, fixture.inputs, 500, clock(1100), ALLOW);
      assertEquals(1, lease.attempt());
      assertEquals(1, lease.number());
      fixture.reopen();
      assertEquals(
          summary,
          fixture.sessions.scopeSummary(
              access(), SELECTED, fixture.generation, child.scope(), childSeal));
    }
  }

  @Test
  void sealedUnadmittedMembershipRemainsOpenAndNewCursorRecomputesIt() throws Exception {
    try (Fixture blocked = new Fixture("blocked")) {
      blocked.sessions.declare(
          access(),
          SELECTED,
          blocked.generation,
          new Messages.Declare(1, operation(1), 0, List.of(1L), true));
      ClosureStore.Cursor cursor = new ClosureStore.Cursor();
      ClosureStore.Progress progress = blocked.sessions.reconcileClosures(cursor, 1, clock(1000));
      assertEquals(1, progress.inspectedMembers());
      assertEquals(0, progress.closedScopes());
      assertCode(
          ProtocolError.Code.NOT_READY,
          () ->
              blocked.sessions.scopeSummary(
                  access(),
                  SELECTED,
                  blocked.generation,
                  0,
                  seal(blocked.binding, 0, 0, null, List.of(1L))));
      ClosureStore.Cursor restarted = new ClosureStore.Cursor();
      ClosureStore.Progress retried = blocked.sessions.reconcileClosures(restarted, 1, clock(1000));
      assertEquals(1, retried.inspectedMembers());
      assertEquals(0, retried.closedScopes());
    }
  }

  @Test
  void rootRetentionOverflowAndFinalClockJumpRollbackWithoutPublishingSummary() throws Exception {
    try (Fixture overflow = new Fixture("overflow")) {
      overflow.sessions.declare(
          access(),
          SELECTED,
          overflow.generation,
          new Messages.Declare(1, operation(1), 0, List.of(), true));
      Records.Digest seal = seal(overflow.binding, 0, 0, null, List.of());
      assertCode(
          ProtocolError.Code.LIMIT_EXCEEDED,
          () ->
              overflow.sessions.reconcileClosures(
                  new ClosureStore.Cursor(), 1, clock(Long.MAX_VALUE)));
      assertCode(
          ProtocolError.Code.NOT_READY,
          () -> overflow.sessions.scopeSummary(access(), SELECTED, overflow.generation, 0, seal));
    }

    try (Fixture jump = new Fixture("clock-jump")) {
      jump.sessions.declare(
          access(),
          SELECTED,
          jump.generation,
          new Messages.Declare(1, operation(1), 0, List.of(), true));
      Records.Digest seal = seal(jump.binding, 0, 0, null, List.of());
      java.util.concurrent.atomic.AtomicInteger samples =
          new java.util.concurrent.atomic.AtomicInteger();
      AdmissionStore.Clock jumping =
          () -> new AdmissionStore.Time(samples.getAndIncrement() == 0 ? 1000 : 31_000, true);
      assertCode(
          ProtocolError.Code.CLOCK_UNSAFE,
          () -> jump.sessions.reconcileClosures(new ClosureStore.Cursor(), 1, jumping));
      assertEquals(2, samples.get());
      assertCode(
          ProtocolError.Code.NOT_READY,
          () -> jump.sessions.scopeSummary(access(), SELECTED, jump.generation, 0, seal));
      ClosureStore.Progress retried =
          jump.sessions.reconcileClosures(new ClosureStore.Cursor(), 1, clock(1000));
      assertEquals(1, retried.closedScopes());
    }
  }

  @Test
  void preexistingParentFailureIsPreservedWhileLaterChildClosureStillCommits() throws Exception {
    try (Fixture fixture = new Fixture("prior-parent-failure")) {
      Records.WorkKey parent = new Records.WorkKey(0, 0, 1);
      fixture.sessions.declare(
          access(),
          SELECTED,
          fixture.generation,
          new Messages.Declare(1, operation(1), 0, List.of(1L), false));
      fixture.admit(parent, operation(2), 1, 1000);
      Records.ChildScope child = fixture.view(parent).child();
      assertNotNull(child);
      Records.WorkView deadline =
          fixture.sessions.expireExecution(fixture.generation, parent, clock(2000));
      assertEquals(Records.State.FAILED, deadline.state());
      assertEquals(11, deadline.diagnostic().code());

      Records.WorkKey childWork = new Records.WorkKey(child.scope(), child.producer(), 2);
      fixture.sessions.declare(
          access(),
          SELECTED,
          fixture.generation,
          new Messages.Declare(3, operation(3), child.scope(), List.of(2L), true));
      fixture.fail(childWork, 4, 3000);
      ClosureStore.Cursor cursor = new ClosureStore.Cursor();
      ClosureStore.Progress childProgress =
          fixture.sessions.reconcileClosures(cursor, 1, clock(3200));
      assertEquals(1, childProgress.closedScopes());
      assertEquals(0, childProgress.settledParents());
      Records.WorkView retainedParent = fixture.view(parent);
      assertEquals(deadline, retainedParent);
      Records.Digest childSeal =
          seal(fixture.binding, child.scope(), child.producer(), parent, List.of(2L));
      assertEquals(
          new Records.Counts(0, 1, 0, 0),
          fixture
              .sessions
              .scopeSummary(access(), SELECTED, fixture.generation, child.scope(), childSeal)
              .counts());
    }
  }

  private static void reconcileUntilClosed(Fixture fixture, long scope, AdmissionStore.Clock clock)
      throws Exception {
    Records.Digest seal =
        scope == 0 ? seal(fixture.binding, 0, 0, null, List.of(1L)) : throwUnexpectedScope();
    ClosureStore.Cursor cursor = new ClosureStore.Cursor();
    for (int calls = 0; calls < 8; calls++) {
      fixture.sessions.reconcileClosures(cursor, 1, clock);
      try {
        fixture.sessions.scopeSummary(access(), SELECTED, fixture.generation, scope, seal);
        return;
      } catch (ProtocolError error) {
        if (error.code() != ProtocolError.Code.NOT_READY) throw error;
      }
    }
    fail("scope did not close within a complete bounded sweep");
  }

  private static Records.Digest throwUnexpectedScope() {
    throw new AssertionError("fixture supports root wait only");
  }

  private final class Fixture implements AutoCloseable {
    private final Path database;
    private final Path inputPath;
    private final Messages.Binding binding;
    private final long generation;
    private SessionStore sessions;
    private InputStore inputs;
    private int request = 10;

    Fixture(String name) throws Exception {
      database = directory.resolve(name + ".sqlite");
      inputPath = directory.resolve(name + "-inputs");
      sessions = SessionStore.initialize(database, configuration());
      binding =
          sessions.create(
              access(),
              SELECTED,
              new Messages.Create(1, 1, new Records.Policy(10_000, 20_000, 30_000)));
      generation = binding.generation();
      inputs = InputStore.initializeForAuthority(inputPath, INPUT_LIMITS, sessions.identity());
      sessions.bindInputs(inputs);
    }

    void admit(Records.WorkKey work, Records.OperationId operation, int mode, long utc)
        throws Exception {
      Records.InputHeader header = header(generation, work, operation, mode);
      Commitments.Context context = new Commitments.Context("issuer-a", "alice", generation);
      try (InputStore.Receiver receiver = inputs.begin(context, header, SELECTED, utc)) {
        receiver.write(ByteBuffer.allocate(0), utc);
        receiver.finish(utc);
      }
      sessions.admit(access(), SELECTED, generation, inputs, header, request++, clock(utc), ALLOW);
    }

    Records.WorkView fail(Records.WorkKey work, int operation) throws Exception {
      return fail(work, operation, 1000);
    }

    Records.WorkView fail(Records.WorkKey work, int operation, long admittedAt) throws Exception {
      admit(work, operation(operation), 0, admittedAt);
      ExecutionStore.Lease lease =
          sessions.claimExecution(
              execAccess(), generation, work, inputs, 500, clock(admittedAt + 100), ALLOW);
      return sessions.failExecution(
          execAccess(),
          lease,
          new Records.Diagnostic(9, "member failed"),
          false,
          clock(admittedAt + 150),
          ALLOW);
    }

    Records.WorkView view(Records.WorkKey work) throws Exception {
      return sessions
          .snapshot(access(), SELECTED, generation, new Messages.Watch(request++, work, 0, 0))
          .work();
    }

    void reopen() throws Exception {
      inputs.close();
      sessions = SessionStore.open(database, configuration());
      inputs = InputStore.open(inputPath, INPUT_LIMITS);
      sessions.verifyInputs(inputs);
    }

    @Override
    public void close() throws java.io.IOException {
      inputs.close();
    }
  }

  private static SessionStore.Configuration configuration() {
    AdmissionStore.Application application =
        new AdmissionStore.Application(
            "copy", Set.of(0, 1), AdmissionStore.RestartSafety.IDEMPOTENT);
    return new SessionStore.Configuration(
        "issuer-a",
        new Records.Limits(16, 32, 32, 1 << 20, 1 << 20, 8),
        new Records.Policy(60_000, 60_000, 60_000),
        4,
        16,
        4,
        BoundedSqlite.Limits.defaults(),
        new AdmissionStore.ExecutionPolicy(List.of(application), 16, 16));
  }

  private static Records.InputHeader header(
      long generation, Records.WorkKey work, Records.OperationId operation, int mode)
      throws Exception {
    return new Records.InputHeader(
        generation,
        operation,
        new Records.AdmitParameters(
            work,
            new Records.Input(0, digest(new byte[0]), "application/octet-stream"),
            "copy",
            mode,
            1000,
            new Records.OutputBudget(0, 0)));
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

  @FunctionalInterface
  private interface Throwing {
    void run() throws Exception;
  }
}
