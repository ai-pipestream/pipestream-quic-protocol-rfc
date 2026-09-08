package ai.pipestream.quic.v2;

import static ai.pipestream.quic.v2.Messages.DURABLE_WORK;
import static org.junit.jupiter.api.Assertions.*;

import ai.pipestream.quic.BoundedSqlite;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

@Timeout(35)
final class ClosureReconciliationCapacityTest {
  private static final Messages.Capabilities SELECTED =
      new Messages.Capabilities(
          true, List.of(DURABLE_WORK), List.of(), 1 << 20, 8, 16, 1 << 20, 1000, 5000);
  private static final InputStore.Limits INPUT_LIMITS =
      new InputStore.Limits(8L << 20, 64, 1 << 20, 8);
  private static final BoundedSqlite.Limits FILES =
      new BoundedSqlite.Limits(64L << 20, 8L << 20, 64L << 20, 64L << 10);
  private static final AdmissionStore.Authorization ALLOW = (binding, parameters) -> {};
  private static final Records.WorkKey PARENT = new Records.WorkKey(0, 0, 1);

  @TempDir Path directory;

  @Test
  void fundedChildFailureAndRootClosureCommitAfterOrdinaryWalCapacityIsExhausted()
      throws Exception {
    Path database = directory.resolve("closure-wal.sqlite");
    Path inputsPath = directory.resolve("closure-wal-inputs");
    SessionStore sessions = SessionStore.initialize(database, configuration());
    Messages.Binding alice = create(sessions, "alice", 1);
    sessions.declare(
        access("alice"),
        SELECTED,
        alice.generation(),
        new Messages.Declare(2, operation(1), 0, List.of(1L), true));
    InputStore.Usage funded;
    Credits finalCredits;
    try (InputStore inputs =
            InputStore.initializeForAuthority(inputsPath, INPUT_LIMITS, sessions.identity());
        Connection reader = BoundedSqlite.open(database, FILES).connect()) {
      sessions.bindInputs(inputs);
      admit(sessions, inputs, alice, PARENT, 1, 2, 1000);
      Records.WorkView waiting = view(sessions, alice, PARENT);
      assertEquals(Records.State.WAITING_CHILDREN, waiting.state());
      Records.ChildScope child = waiting.child();
      assertNotNull(child);
      Records.WorkKey childWork = new Records.WorkKey(child.scope(), child.producer(), 2);
      sessions.declare(
          access("alice"),
          SELECTED,
          alice.generation(),
          new Messages.Declare(4, operation(3), child.scope(), List.of(2L), true));
      admit(sessions, inputs, alice, childWork, 0, 4, 1000);
      Messages.Binding bob = create(sessions, "bob", 1);
      Records.WorkKey burner = new Records.WorkKey(0, 0, 1);
      sessions.declare(
          access("bob"),
          SELECTED,
          bob.generation(),
          new Messages.Declare(2, operation(5), 0, List.of(1L), false));
      admit(sessions, inputs, bob, burner, 0, 6, 5000);

      ExecutionStore.Lease childLease =
          sessions.claimExecution(
              execAccess("alice"), alice.generation(), childWork, inputs, 500, clock(1100), ALLOW);
      Records.WorkView childFailed =
          sessions.failExecution(
              execAccess("alice"),
              childLease,
              new Records.Diagnostic(9, "child failed"),
              false,
              clock(1150),
              ALLOW);
      assertEquals(Records.State.FAILED, childFailed.state());
      ExecutionStore.Lease renewal =
          sessions.claimExecution(
              execAccess("bob"), bob.generation(), burner, inputs, 500, clock(1150), ALLOW);

      funded = inputs.usage();
      Credits reserved = credits(database, alice, child.scope());
      execute(reader, "BEGIN");
      try (var query = reader.createStatement();
          var rows = query.executeQuery("SELECT image FROM ps_v2_slots WHERE id=1")) {
        assertTrue(rows.next());
        assertTrue(rows.getBytes(1).length > 0);
      }
      long beforeWal = wal(database);
      int renewals = 0;
      ProtocolError refusal = null;
      for (int attempt = 0; attempt < 10_000; attempt++) {
        try {
          renewal = sessions.renewExecution(execAccess("bob"), renewal, 800, clock(1200), ALLOW);
          renewals++;
        } catch (ProtocolError error) {
          refusal = error;
          break;
        }
      }
      assertTrue(renewals > 0);
      assertNotNull(refusal, "ordinary renewals did not exhaust the pinned WAL");
      assertEquals(ProtocolError.Code.LIMIT_EXCEEDED, refusal.code(), refusal::getMessage);
      SQLException sqlite = assertInstanceOf(SQLException.class, refusal.getCause());
      assertEquals(13, sqlite.getErrorCode() & 255, sqlite::toString);
      long saturatedWal = wal(database);
      assertTrue(saturatedWal > beforeWal);
      assertTrue(saturatedWal <= FILES.walBytes());
      assertEquals(reserved, credits(database, alice, child.scope()));

      ClosureStore.Cursor cursor = new ClosureStore.Cursor();
      int inspectedScopes = 0;
      int inspectedMembers = 0;
      int closedScopes = 0;
      int settledParents = 0;
      for (int step = 0; step < 8 && closedScopes < 2; step++) {
        ClosureStore.Progress progress = sessions.reconcileClosures(cursor, 1, clock(1200));
        inspectedScopes += progress.inspectedScopes();
        inspectedMembers += progress.inspectedMembers();
        closedScopes += progress.closedScopes();
        settledParents += progress.settledParents();
      }
      assertEquals(3, inspectedScopes);
      assertEquals(2, inspectedMembers);
      assertEquals(2, closedScopes);
      assertEquals(1, settledParents);

      Records.Digest childSeal = seal(alice, child.scope(), child.producer(), PARENT, List.of(2L));
      Records.ScopeSummary childSummary =
          sessions.scopeSummary(
              access("alice"), SELECTED, alice.generation(), child.scope(), childSeal);
      assertEquals(new Records.Counts(0, 1, 0, 0), childSummary.counts());
      Commitments.StatusTree childStatuses =
          new Commitments.StatusTree(child.scope(), child.producer(), 1);
      childStatuses.add(childFailed, null);
      assertEquals(childStatuses.finish().root(), childSummary.statusRoot());
      Records.WorkView parentFailed = view(sessions, alice, PARENT);
      assertEquals(Records.State.FAILED, parentFailed.state());
      assertEquals(ProtocolError.Code.CONFLICT.value(), parentFailed.diagnostic().code());
      Records.Digest rootSeal = seal(alice, 0, 0, null, List.of(1L));
      Records.ScopeSummary rootSummary =
          sessions.scopeSummary(access("alice"), SELECTED, alice.generation(), 0, rootSeal);
      assertEquals(new Records.Counts(0, 1, 0, 0), rootSummary.counts());
      Commitments.StatusTree rootStatuses = new Commitments.StatusTree(0, 0, 1);
      rootStatuses.add(parentFailed, childSummary.statusRoot());
      assertEquals(rootStatuses.finish().root(), rootSummary.statusRoot());

      Credits spent = credits(database, alice, child.scope());
      finalCredits = spent;
      assertEquals(reserved.rootScope() - 1, spent.rootScope());
      assertEquals(reserved.childScope() - 1, spent.childScope());
      assertEquals(reserved.parentWork() - 1, spent.parentWork());
      assertEquals(reserved.parentJob() - 1, spent.parentJob());
      assertEquals(funded, inputs.usage());
      assertFunding(inputs, alice, PARENT, childWork);
      long afterWal = wal(database);
      assertTrue(afterWal > saturatedWal);
      assertTrue(afterWal <= FILES.walBytes());
      System.out.printf(
          "closure WAL cap=%d renewals=%d before=%d saturated=%d after=%d"
              + " rootCredits=%d->%d childCredits=%d->%d parentWork=%d->%d parentJob=%d->%d%n",
          FILES.walBytes(),
          renewals,
          beforeWal,
          saturatedWal,
          afterWal,
          reserved.rootScope(),
          spent.rootScope(),
          reserved.childScope(),
          spent.childScope(),
          reserved.parentWork(),
          spent.parentWork(),
          reserved.parentJob(),
          spent.parentJob());
      execute(reader, "ROLLBACK");
    }

    SessionStore reopened = SessionStore.open(database, configuration());
    try (InputStore inputs = InputStore.open(inputsPath, INPUT_LIMITS)) {
      reopened.verifyInputs(inputs);
      Messages.Binding recoveredAlice =
          reopened.attach(
              access("alice"), SELECTED, new Messages.Attach(1, "issuer-a", "alice", 1));
      Records.WorkView parent = view(reopened, recoveredAlice, PARENT);
      assertEquals(Records.State.FAILED, parent.state());
      Records.ChildScope child = parent.child();
      assertNotNull(child);
      Records.WorkKey childWork = new Records.WorkKey(child.scope(), child.producer(), 2);
      assertEquals(funded, inputs.usage());
      assertEquals(finalCredits, credits(database, recoveredAlice, child.scope()));
      Records.Digest childSeal =
          seal(recoveredAlice, child.scope(), child.producer(), PARENT, List.of(2L));
      assertEquals(
          new Records.Counts(0, 1, 0, 0),
          reopened
              .scopeSummary(
                  access("alice"), SELECTED, recoveredAlice.generation(), child.scope(), childSeal)
              .counts());
      assertEquals(
          new Records.Counts(0, 1, 0, 0),
          reopened
              .scopeSummary(
                  access("alice"),
                  SELECTED,
                  recoveredAlice.generation(),
                  0,
                  seal(recoveredAlice, 0, 0, null, List.of(1L)))
              .counts());
      assertFunding(inputs, recoveredAlice, PARENT, childWork);
    }
  }

  private static Messages.Binding create(SessionStore sessions, String owner, long sequence)
      throws Exception {
    return sessions.create(
        access(owner),
        SELECTED,
        new Messages.Create(1, sequence, new Records.Policy(10_000, 20_000, 30_000)));
  }

  private static void admit(
      SessionStore sessions,
      InputStore inputs,
      Messages.Binding binding,
      Records.WorkKey work,
      int mode,
      int operation,
      long duration)
      throws Exception {
    byte[] payload = {(byte) work.entity()};
    Records.InputHeader header = header(binding, work, mode, operation, duration, payload);
    Commitments.Context context = context(binding);
    try (InputStore.Receiver receiver = inputs.begin(context, header, SELECTED, operation)) {
      receiver.write(ByteBuffer.wrap(payload), operation + 1L);
      receiver.finish(operation + 2L);
    }
    sessions.admit(
        access(binding.owner()),
        SELECTED,
        binding.generation(),
        inputs,
        header,
        operation + 3L,
        clock(1000),
        ALLOW);
  }

  private static void assertFunding(
      InputStore inputs, Messages.Binding binding, Records.WorkKey parent, Records.WorkKey child)
      throws Exception {
    for (Records.WorkKey work : List.of(parent, child)) {
      Records.InputHeader header =
          header(
              binding,
              work,
              work.equals(parent) ? 1 : 0,
              work.equals(parent) ? 2 : 4,
              1000,
              new byte[] {(byte) work.entity()});
      assertTrue(inputs.find(context(binding), header).isPresent());
      assertTrue(inputs.findReservation(context(binding), header).isPresent());
    }
  }

  private static Records.InputHeader header(
      Messages.Binding binding,
      Records.WorkKey work,
      int mode,
      int operation,
      long duration,
      byte[] payload)
      throws Exception {
    return new Records.InputHeader(
        binding.generation(),
        operation(operation),
        new Records.AdmitParameters(
            work,
            new Records.Input(payload.length, digest(payload), "application/octet-stream"),
            "copy",
            mode,
            duration,
            new Records.OutputBudget(0, 0)));
  }

  private static Credits credits(Path database, Messages.Binding binding, long childScope)
      throws Exception {
    try (Connection connection = BoundedSqlite.open(database, FILES).connect()) {
      long root = scopeSlot(connection, binding.generation(), 0);
      long child = scopeSlot(connection, binding.generation(), childScope);
      long[] parent = jobSlots(connection, binding.generation(), PARENT);
      return new Credits(
          FixedRecords.header(connection, root, FixedRecords.Kind.SCOPE).credits(),
          FixedRecords.header(connection, child, FixedRecords.Kind.SCOPE).credits(),
          FixedRecords.header(connection, parent[0], FixedRecords.Kind.WORK).credits(),
          FixedRecords.header(connection, parent[1], FixedRecords.Kind.JOB).credits());
    }
  }

  private static long scopeSlot(Connection connection, long generation, long scope)
      throws Exception {
    try (var query =
        connection.prepareStatement(
            "SELECT state_slot FROM ps_v2_scopes WHERE generation=? AND id=?")) {
      query.setLong(1, generation);
      query.setLong(2, scope);
      try (var row = query.executeQuery()) {
        assertTrue(row.next());
        long result = row.getLong(1);
        assertFalse(row.next());
        return result;
      }
    }
  }

  private static long[] jobSlots(Connection connection, long generation, Records.WorkKey work)
      throws Exception {
    try (var query =
        connection.prepareStatement(
            "SELECT e.view_slot,j.state_slot FROM ps_v2_entities e JOIN ps_v2_jobs j ON"
                + " e.generation=j.generation AND e.scope=j.scope AND e.id=j.entity"
                + " WHERE e.generation=? AND e.scope=? AND e.id=?")) {
      query.setLong(1, generation);
      query.setLong(2, work.scope());
      query.setLong(3, work.entity());
      try (var row = query.executeQuery()) {
        assertTrue(row.next());
        long[] result = {row.getLong(1), row.getLong(2)};
        assertFalse(row.next());
        return result;
      }
    }
  }

  private static Records.WorkView view(
      SessionStore sessions, Messages.Binding binding, Records.WorkKey work) throws Exception {
    return sessions
        .snapshot(
            access(binding.owner()),
            SELECTED,
            binding.generation(),
            new Messages.Watch(90, work, 0, 0))
        .work();
  }

  private static Records.Digest seal(
      Messages.Binding binding,
      long scope,
      int producer,
      Records.WorkKey parent,
      List<Long> members) {
    Commitments.Seal seal =
        new Commitments.Seal(context(binding), scope, producer, parent, members.size());
    for (long member : members) seal.add(member);
    return seal.finish();
  }

  private static SessionStore.Configuration configuration() {
    AdmissionStore.Application application =
        new AdmissionStore.Application(
            "copy", Set.of(0, 1), AdmissionStore.RestartSafety.IDEMPOTENT);
    return new SessionStore.Configuration(
        "issuer-a",
        new Records.Limits(8, 16, 16, 1 << 20, 1 << 20, 4),
        new Records.Policy(60_000, 60_000, 60_000),
        4,
        16,
        8,
        FILES,
        new AdmissionStore.ExecutionPolicy(List.of(application), 8, 8));
  }

  private static void execute(Connection connection, String sql) throws Exception {
    try (var statement = connection.createStatement()) {
      statement.execute(sql);
    }
  }

  private static long wal(Path database) throws Exception {
    Path path = database.resolveSibling(database.getFileName() + "-wal");
    return Files.exists(path) ? Files.size(path) : 0;
  }

  private static Records.Digest digest(byte[] bytes) throws Exception {
    return new Records.Digest(MessageDigest.getInstance("SHA-256").digest(bytes));
  }

  private static Commitments.Context context(Messages.Binding binding) {
    return new Commitments.Context(binding.authority(), binding.owner(), binding.generation());
  }

  private static Records.OperationId operation(int value) {
    byte[] bytes = new byte[16];
    bytes[15] = (byte) value;
    return new Records.OperationId(bytes);
  }

  private static AdmissionStore.Clock clock(long time) {
    return () -> new AdmissionStore.Time(time, true);
  }

  private static SessionStore.Access access(String owner) {
    return new SessionStore.Access(owner, () -> {});
  }

  private static ExecutionStore.Access execAccess(String owner) {
    return new ExecutionStore.Access(owner, () -> {});
  }

  private record Credits(long rootScope, long childScope, long parentWork, long parentJob) {}
}
