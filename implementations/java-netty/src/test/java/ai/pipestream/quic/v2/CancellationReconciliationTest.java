package ai.pipestream.quic.v2;

import static ai.pipestream.quic.v2.Messages.DURABLE_WORK;
import static org.junit.jupiter.api.Assertions.*;

import ai.pipestream.quic.BoundedSqlite;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

@Timeout(30)
final class CancellationReconciliationTest {
  private static final Messages.Capabilities SELECTED =
      new Messages.Capabilities(
          true, List.of(DURABLE_WORK), List.of(), 1 << 20, 16, 32, 1 << 20, 1000, 10_000);
  private static final InputStore.Limits INPUT_LIMITS =
      new InputStore.Limits(16L << 20, 128, 1 << 20, 8);
  private static final AdmissionStore.Authorization ADMIT = (binding, parameters) -> {};
  private static final FenceStore.Authorization FENCE = (binding, request) -> {};
  private static final Records.WorkKey PARENT = new Records.WorkKey(0, 0, 1);
  private static final PublicationStore.Endpoint ENDPOINT =
      new PublicationStore.Endpoint("localhost:443");

  @TempDir Path directory;

  @Test
  void limitOneCascadePreservesEarlierSkipAndClosesTheRealBranchAcrossReopen() throws Exception {
    try (Fixture fixture = new Fixture("tree")) {
      fixture.declare(0, operation(1), List.of(1L), true);
      fixture.admit(PARENT, operation(2), 1, 1000);
      Records.ChildScope child = fixture.view(PARENT).child();
      assertNotNull(child);
      Records.WorkKey skipped = new Records.WorkKey(child.scope(), child.producer(), 1);
      Records.WorkKey running = new Records.WorkKey(child.scope(), child.producer(), 2);
      Records.WorkKey unadmitted = new Records.WorkKey(child.scope(), child.producer(), 3);
      fixture.declare(child.scope(), operation(3), List.of(1L, 2L, 3L), false);
      fixture.sessions.skip(
          access(),
          SELECTED,
          fixture.generation,
          new Messages.Skip(20, operation(20), skipped),
          clock(1100),
          FENCE);
      fixture.admit(running, operation(4), 0, 1100);
      ExecutionStore.Lease lease =
          fixture.sessions.claimExecution(
              execAccess(), fixture.generation, running, fixture.inputs, 500, clock(1200), ADMIT);

      Messages.CancelResponse response =
          fixture.sessions.cancel(
              access(),
              SELECTED,
              fixture.generation,
              new Messages.Cancel(21, operation(21), PARENT),
              clock(1250),
              FENCE);
      Records.Cancelled cancelled =
          assertInstanceOf(Records.Cancelled.class, response.receipt().outcome());
      assertEquals(Records.State.CANCELLING, cancelled.state());
      assertEquals(Records.State.CANCELLING, fixture.view(PARENT).state());
      assertEquals(Records.State.SKIPPED, fixture.view(skipped).state());

      assertCode(
          ProtocolError.Code.CANCELLED,
          () -> fixture.declare(child.scope(), operation(5), List.of(4L), false));
      assertCode(
          ProtocolError.Code.CANCELLED, () -> fixture.admit(unadmitted, operation(6), 0, 1260));
      assertCode(
          ProtocolError.Code.CANCELLED,
          () ->
              fixture.sessions.retry(
                  access(),
                  SELECTED,
                  fixture.generation,
                  new Messages.Retry(22, operation(22), running, 1),
                  clock(1260),
                  ADMIT));
      assertCode(
          ProtocolError.Code.CANCELLED,
          () ->
              fixture.sessions.succeedExecution(
                  execAccess(), lease, fixture.inputs, 0, ENDPOINT, clock(1260), ADMIT));

      FenceStore.Cursor partial = new FenceStore.Cursor();
      FenceStore.Progress partialProgress =
          fixture.sessions.reconcileCancellation(partial, 1, clock(1300));
      assertEquals(1, partialProgress.inspectedMembers());
      Messages.PageResponse partialPage =
          fixture.sessions.page(
              access(), SELECTED, fixture.generation, new Messages.Page(23, child.scope(), 0, 10));
      assertFalse(partialPage.sealed());
      assertNull(partialPage.seal());
      assertEquals(Records.State.CANCELLING, fixture.view(PARENT).state());
      fixture.reopen();
      Messages.PageResponse restartedPage =
          fixture.sessions.page(
              access(), SELECTED, fixture.generation, new Messages.Page(24, child.scope(), 0, 10));
      assertFalse(restartedPage.sealed());
      assertNull(restartedPage.seal());
      assertEquals(Records.State.CANCELLING, fixture.view(PARENT).state());

      FenceStore.Cursor fences = new FenceStore.Cursor();
      ClosureStore.Cursor closures = new ClosureStore.Cursor();
      int inspectedWork = 0;
      int inspectedMembers = 0;
      int sealedScopes = 0;
      for (int calls = 0; calls < 32; calls++) {
        FenceStore.Progress progress =
            fixture.sessions.reconcileCancellation(fences, 1, clock(1300));
        assertTrue(progress.inspectedWork() <= 1);
        assertTrue(progress.inspectedMembers() <= 1);
        inspectedWork += progress.inspectedWork();
        inspectedMembers += progress.inspectedMembers();
        sealedScopes += progress.sealedScopes();
        fixture.sessions.reconcileClosures(closures, 1, clock(1300));
        if (fixture.view(PARENT).state() == Records.State.CANCELLED) break;
      }
      assertTrue(inspectedWork > 0);
      assertTrue(inspectedMembers >= 3);
      assertTrue(sealedScopes > 0);
      assertEquals(Records.State.SKIPPED, fixture.view(skipped).state());
      assertEquals(Records.State.CANCELLED, fixture.view(running).state());
      assertEquals(Records.State.CANCELLED, fixture.view(unadmitted).state());
      assertEquals(Records.State.CANCELLED, fixture.view(PARENT).state());

      for (int calls = 0; calls < 8; calls++) {
        fixture.sessions.reconcileClosures(closures, 1, clock(1300));
      }

      Records.Digest childSeal =
          seal(fixture.binding, child.scope(), child.producer(), PARENT, List.of(1L, 2L, 3L));
      Records.ScopeSummary childSummary =
          fixture.sessions.scopeSummary(
              access(), SELECTED, fixture.generation, child.scope(), childSeal);
      assertEquals(new Records.Counts(0, 0, 2, 1), childSummary.counts());
      Records.Digest rootSeal = seal(fixture.binding, 0, 0, null, List.of(1L));
      Records.ScopeSummary root =
          fixture.sessions.scopeSummary(access(), SELECTED, fixture.generation, 0, rootSeal);
      assertEquals(new Records.Counts(0, 0, 1, 0), root.counts());

      fixture.reopen();
      assertEquals(Records.State.SKIPPED, fixture.view(skipped).state());
      assertEquals(Records.State.CANCELLED, fixture.view(PARENT).state());
      assertEquals(
          childSummary,
          fixture.sessions.scopeSummary(
              access(), SELECTED, fixture.generation, child.scope(), childSeal));
      assertEquals(
          root, fixture.sessions.scopeSummary(access(), SELECTED, fixture.generation, 0, rootSeal));
    }
  }

  @Test
  void acceptedScopeFenceRestartsPartialClosureAndCancellationCursorIsBoundedAndBound()
      throws Exception {
    try (Fixture first = new Fixture("partial-closure");
        Fixture other = new Fixture("foreign-cursor")) {
      first.declare(0, operation(40), List.of(1L, 2L), true);
      first.sessions.skip(
          access(),
          SELECTED,
          first.generation,
          new Messages.Skip(41, operation(41), PARENT),
          clock(1000),
          FENCE);
      Records.WorkKey second = new Records.WorkKey(0, 0, 2);
      first.sessions.skip(
          access(),
          SELECTED,
          first.generation,
          new Messages.Skip(42, operation(42), second),
          clock(1000),
          FENCE);
      ClosureStore.Cursor closure = new ClosureStore.Cursor();
      ClosureStore.Progress partial = first.sessions.reconcileClosures(closure, 1, clock(1000));
      assertEquals(1, partial.inspectedMembers());
      assertEquals(0, partial.closedScopes());

      first.sessions.cancelScope(
          access(),
          SELECTED,
          first.generation,
          new Messages.CancelScope(43, operation(43), 0),
          clock(1000),
          FENCE);
      FenceStore.Cursor cancellation = new FenceStore.Cursor();
      for (int calls = 0; calls < 8; calls++) {
        first.sessions.reconcileCancellation(cancellation, 1, clock(1000));
        first.sessions.reconcileClosures(closure, 1, clock(1000));
      }
      Records.Digest rootSeal = seal(first.binding, 0, 0, null, List.of(1L, 2L));
      Records.ScopeSummary summary =
          first.sessions.scopeSummary(access(), SELECTED, first.generation, 0, rootSeal);
      assertEquals(new Records.Counts(0, 0, 0, 2), summary.counts());

      FenceStore.Cursor bound = new FenceStore.Cursor();
      first.sessions.reconcileCancellation(bound, 1, clock(1000));
      assertCode(
          ProtocolError.Code.CONFLICT,
          () -> other.sessions.reconcileCancellation(bound, 1, clock(1000)));
      assertCode(
          ProtocolError.Code.LIMIT_EXCEEDED,
          () -> first.sessions.reconcileCancellation(new FenceStore.Cursor(), 0, clock(1000)));
      assertCode(
          ProtocolError.Code.LIMIT_EXCEEDED,
          () -> first.sessions.reconcileCancellation(new FenceStore.Cursor(), 257, clock(1000)));
      first.reopen();
      assertEquals(
          summary, first.sessions.scopeSummary(access(), SELECTED, first.generation, 0, rootSeal));
    }
  }

  @Test
  void revocationDeniesTheOwnerWhileBoundedMaintenanceSealsAndSettlesUnadmittedWork()
      throws Exception {
    try (Fixture fixture = new Fixture("revoke")) {
      fixture.declare(0, operation(30), List.of(1L, 2L), false);
      fixture.sessions.revoke(access(), fixture.generation, clock(1000));
      assertCode(ProtocolError.Code.UNAUTHORIZED, () -> fixture.view(PARENT));
      assertCode(
          ProtocolError.Code.UNAUTHORIZED,
          () ->
              fixture.sessions.cancelScope(
                  access(),
                  SELECTED,
                  fixture.generation,
                  new Messages.CancelScope(31, operation(31), 0),
                  clock(1000),
                  FENCE));

      FenceStore.Cursor fences = new FenceStore.Cursor();
      ClosureStore.Cursor closures = new ClosureStore.Cursor();
      int settled = 0;
      int sealed = 0;
      int closed = 0;
      for (int calls = 0; calls < 16; calls++) {
        FenceStore.Progress cancellation =
            fixture.sessions.reconcileCancellation(fences, 1, clock(1100));
        assertTrue(cancellation.inspectedWork() <= 1);
        assertTrue(cancellation.inspectedMembers() <= 1);
        settled += cancellation.settledWork();
        sealed += cancellation.sealedScopes();
        ClosureStore.Progress closure =
            fixture.sessions.reconcileClosures(closures, 1, clock(1100));
        closed += closure.closedScopes();
        if (settled == 2 && sealed > 0 && closed > 0) break;
      }
      assertEquals(2, settled);
      assertTrue(sealed > 0);
      assertTrue(closed > 0);
      fixture.reopen();
      assertCode(ProtocolError.Code.UNAUTHORIZED, () -> fixture.view(PARENT));
      FenceStore.Progress replay =
          fixture.sessions.reconcileCancellation(new FenceStore.Cursor(), 1, clock(1100));
      assertEquals(0, replay.settledWork());
      assertEquals(0, replay.sealedScopes());
    }
  }

  private final class Fixture implements AutoCloseable {
    final Path database;
    final Path inputPath;
    final Messages.Binding binding;
    final long generation;
    SessionStore sessions;
    InputStore inputs;
    int request = 100;

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

    void declare(long scope, Records.OperationId operation, List<Long> members, boolean seal)
        throws Exception {
      sessions.declare(
          access(),
          SELECTED,
          generation,
          new Messages.Declare(request++, operation, scope, members, seal));
    }

    void admit(Records.WorkKey work, Records.OperationId operation, int mode, long utc)
        throws Exception {
      Records.InputHeader header = header(generation, work, operation, mode);
      try (InputStore.Receiver receiver = inputs.begin(context(), header, SELECTED, utc)) {
        receiver.write(ByteBuffer.allocate(0), utc);
        receiver.finish(utc);
      }
      sessions.admit(access(), SELECTED, generation, inputs, header, request++, clock(utc), ADMIT);
    }

    Records.WorkView view(Records.WorkKey work) throws Exception {
      return sessions
          .snapshot(access(), SELECTED, generation, new Messages.Watch(request++, work, 0, 0))
          .work();
    }

    Commitments.Context context() {
      return new Commitments.Context("issuer-a", "alice", generation);
    }

    void reopen() throws Exception {
      inputs.close();
      sessions = SessionStore.open(database, configuration());
      inputs = InputStore.open(inputPath, INPUT_LIMITS);
      sessions.verifyInputs(inputs);
    }

    @Override
    public void close() throws IOException {
      inputs.close();
    }
  }

  private static SessionStore.Configuration configuration() {
    return new SessionStore.Configuration(
        "issuer-a",
        new Records.Limits(16, 64, 64, 1 << 20, 1 << 20, 8),
        new Records.Policy(60_000, 60_000, 60_000),
        4,
        16,
        4,
        BoundedSqlite.Limits.defaults(),
        new AdmissionStore.ExecutionPolicy(
            List.of(
                new AdmissionStore.Application(
                    "copy", Set.of(0, 1), AdmissionStore.RestartSafety.IDEMPOTENT)),
            8,
            8));
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
            10_000,
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
