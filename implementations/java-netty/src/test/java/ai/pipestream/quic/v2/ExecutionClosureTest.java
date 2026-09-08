package ai.pipestream.quic.v2;

import static ai.pipestream.quic.v2.Messages.DURABLE_WORK;
import static org.junit.jupiter.api.Assertions.*;

import ai.pipestream.quic.BoundedSqlite;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

@Timeout(20)
final class ExecutionClosureTest {
  private static final Messages.Capabilities SELECTED =
      new Messages.Capabilities(
          true, List.of(DURABLE_WORK), List.of(), 1 << 20, 8, 16, 1 << 20, 1000, 5000);
  private static final InputStore.Limits INPUT_LIMITS =
      new InputStore.Limits(4L << 20, 32, 1 << 20, 8);
  private static final Records.WorkKey PARENT = new Records.WorkKey(0, 0, 1);
  private static final AdmissionStore.Authorization ALLOW = (binding, parameters) -> {};

  @TempDir Path directory;

  @Test
  void checksummedEmptyChildSummaryAllowsClaimAndRecoveryAudit() throws Exception {
    Branch fixture = branch("empty-control");
    try (InputStore inputs = fixture.inputs()) {
      fixture
          .sessions()
          .declare(
              sessionAccess(),
              SELECTED,
              1,
              new Messages.Declare(4, operation(3), fixture.child().scope(), List.of(), true));
      installSummary(
          fixture.database(),
          fixture.binding(),
          fixture.child(),
          new Records.Counts(0, 0, 0, 0),
          Commitments.emptyStatus(),
          1000);
      ExecutionStore.Lease lease =
          fixture
              .sessions()
              .claimExecution(execAccess(), 1, PARENT, inputs, 100, clock(1100), ALLOW);
      assertEquals(1, lease.attempt());
      assertEquals(1, lease.number());
    }
    SessionStore reopened = SessionStore.open(fixture.database(), configuration());
    try (InputStore inputs = InputStore.open(fixture.inputsPath(), INPUT_LIMITS)) {
      reopened.verifyInputs(inputs);
      assertEquals(
          Records.State.ACTIVE,
          reopened
              .snapshot(sessionAccess(), SELECTED, 1, new Messages.Watch(8, PARENT, 0, 0))
              .work()
              .state());
    }
  }

  @Test
  void checksummedSuccessSummaryCannotHideDeclaredChildFromClaimOrRecovery() throws Exception {
    Path database = directory.resolve("false-closure.sqlite");
    Path inputsPath = directory.resolve("false-closure-inputs");
    SessionStore sessions = SessionStore.initialize(database, configuration());
    Messages.Binding binding =
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
      Records.ChildScope child =
          sessions
              .snapshot(sessionAccess(), SELECTED, 1, new Messages.Watch(3, PARENT, 0, 0))
              .work()
              .child();
      assertNotNull(child);
      assertEquals(0, child.producer());
      sessions.declare(
          sessionAccess(),
          SELECTED,
          1,
          new Messages.Declare(4, operation(3), child.scope(), List.of(2L), true));

      installFalseSummary(database, binding, child);
      SQLException current =
          assertThrows(
              SQLException.class,
              () ->
                  sessions.claimExecution(
                      execAccess(), 1, PARENT, inputs, 100, clock(1100), ALLOW));
      assertTrue(current.getMessage().contains("closure precedes a member"), current::getMessage);
    }
    SQLException recovery =
        assertThrows(SQLException.class, () -> SessionStore.open(database, configuration()));
    assertTrue(recovery.getMessage().contains("closure precedes a member"), recovery::getMessage);
  }

  @Test
  void checksummedSuccessSummaryCannotHideActuallyFailedChild() throws Exception {
    Branch fixture = branch("failed-child");
    try (InputStore inputs = fixture.inputs()) {
      Records.WorkKey childWork = new Records.WorkKey(fixture.child().scope(), 0, 2);
      fixture
          .sessions()
          .declare(
              sessionAccess(),
              SELECTED,
              1,
              new Messages.Declare(4, operation(3), fixture.child().scope(), List.of(2L), true));
      Records.InputHeader childHeader = header(operation(4), childWork, 0);
      try (InputStore.Receiver receiver = inputs.begin(context(), childHeader, SELECTED, 1)) {
        receiver.write(ByteBuffer.allocate(0), 2);
        receiver.finish(3);
      }
      fixture
          .sessions()
          .admit(sessionAccess(), SELECTED, 1, inputs, childHeader, 5, clock(1000), ALLOW);
      ExecutionStore.Lease childLease =
          fixture
              .sessions()
              .claimExecution(execAccess(), 1, childWork, inputs, 100, clock(1100), ALLOW);
      Records.WorkView failed =
          fixture
              .sessions()
              .failExecution(
                  execAccess(),
                  childLease,
                  new Records.Diagnostic(9, "child failed"),
                  false,
                  clock(1150),
                  ALLOW);
      assertEquals(Records.State.FAILED, failed.state());
      installSummary(
          fixture.database(),
          fixture.binding(),
          fixture.child(),
          new Records.Counts(1, 0, 0, 0),
          Commitments.statusLeaf(failed, null),
          failed.terminalAt());
      SQLException current =
          assertThrows(
              SQLException.class,
              () ->
                  fixture
                      .sessions()
                      .claimExecution(execAccess(), 1, PARENT, inputs, 100, clock(1200), ALLOW));
      assertTrue(
          current.getMessage().contains("closure differs from complete retained membership"),
          current::getMessage);
    }
    SQLException recovery =
        assertThrows(
            SQLException.class, () -> SessionStore.open(fixture.database(), configuration()));
    assertTrue(
        recovery.getMessage().contains("closure differs from complete retained membership"),
        recovery::getMessage);
  }

  @Test
  void authoritativeParentDeadlineFailureDoesNotCancelCallerChildObligations() throws Exception {
    Branch fixture = branch("parent-expired");
    try (InputStore inputs = fixture.inputs()) {
      Records.WorkView parent = fixture.sessions().expireExecution(1, PARENT, clock(2000));
      assertEquals(Records.State.FAILED, parent.state());
      Records.WorkKey childWork = new Records.WorkKey(fixture.child().scope(), 0, 2);
      fixture
          .sessions()
          .declare(
              sessionAccess(),
              SELECTED,
              1,
              new Messages.Declare(4, operation(3), fixture.child().scope(), List.of(2L), false));
      Records.InputHeader childHeader = header(operation(4), childWork, 0);
      try (InputStore.Receiver receiver = inputs.begin(context(), childHeader, SELECTED, 1)) {
        receiver.write(ByteBuffer.allocate(0), 2);
        receiver.finish(3);
      }
      Records.Admitted child =
          assertInstanceOf(
              Records.Admitted.class,
              fixture
                  .sessions()
                  .admit(sessionAccess(), SELECTED, 1, inputs, childHeader, 5, clock(3000), ALLOW)
                  .receipt()
                  .outcome());
      assertEquals(3000, child.admittedAt());
      assertEquals(4000, child.deadline());
      assertEquals(
          Records.State.FAILED,
          fixture
              .sessions()
              .snapshot(sessionAccess(), SELECTED, 1, new Messages.Watch(6, PARENT, 0, 0))
              .work()
              .state());
    }
  }

  private Branch branch(String name) throws Exception {
    Path database = directory.resolve(name + ".sqlite");
    Path inputsPath = directory.resolve(name + "-inputs");
    SessionStore sessions = SessionStore.initialize(database, configuration());
    Messages.Binding binding =
        sessions.create(
            sessionAccess(),
            SELECTED,
            new Messages.Create(1, 1, new Records.Policy(10_000, 20_000, 30_000)));
    sessions.declare(
        sessionAccess(), SELECTED, 1, new Messages.Declare(2, operation(1), 0, List.of(1L), false));
    InputStore inputs =
        InputStore.initializeForAuthority(inputsPath, INPUT_LIMITS, sessions.identity());
    sessions.bindInputs(inputs);
    Records.InputHeader header = header();
    try (InputStore.Receiver receiver = inputs.begin(context(), header, SELECTED, 1)) {
      receiver.write(ByteBuffer.allocate(0), 2);
      receiver.finish(3);
    }
    sessions.admit(sessionAccess(), SELECTED, 1, inputs, header, 2, clock(1000), ALLOW);
    Records.ChildScope child =
        sessions
            .snapshot(sessionAccess(), SELECTED, 1, new Messages.Watch(3, PARENT, 0, 0))
            .work()
            .child();
    assertNotNull(child);
    return new Branch(database, inputsPath, sessions, inputs, binding, child);
  }

  private static void installFalseSummary(
      Path database, Messages.Binding binding, Records.ChildScope child) throws Exception {
    try (Connection connection = BoundedSqlite.open(database, configuration().files()).connect()) {
      execute(connection, "BEGIN IMMEDIATE");
      long slot;
      try (var query =
          connection.prepareStatement(
              "SELECT state_slot FROM ps_v2_scopes WHERE generation=1 AND id=? AND producer=0")) {
        query.setLong(1, child.scope());
        try (var row = query.executeQuery()) {
          assertTrue(row.next());
          slot = row.getLong(1);
        }
      }
      FixedRecords.Snapshot image =
          FixedRecords.read(
              connection,
              slot,
              FixedRecords.Kind.SCOPE,
              FixedRecords.key(
                  binding, FixedRecords.Kind.SCOPE, child.scope(), child.producer(), 0, null));
      ScopeState state = ScopeState.decode(image.body());
      assertNotNull(state.seal());
      assertEquals(1, state.declared());
      byte[] status = new byte[32];
      status[31] = 1;
      Records.ScopeSummary falseSuccess =
          new Records.ScopeSummary(
              child.scope(),
              child.producer(),
              PARENT,
              state.seal(),
              1,
              new Records.Counts(1, 0, 0, 0),
              new Records.Digest(status),
              1000);
      ScopeState corrupted =
          new ScopeState(
              state.id(),
              state.producer(),
              state.parent(),
              state.declared(),
              state.last(),
              state.seal(),
              state.cancelled(),
              state.revoked(),
              falseSuccess);
      FixedRecords.replace(
          connection,
          configuration().files(),
          slot,
          FixedRecords.Kind.SCOPE,
          image.header().key(),
          image.header().revision(),
          corrupted.encode(),
          false);
      execute(connection, "COMMIT");
    }
  }

  private static void installSummary(
      Path database,
      Messages.Binding binding,
      Records.ChildScope child,
      Records.Counts counts,
      Records.Digest statusRoot,
      long closedAt)
      throws Exception {
    try (Connection connection = BoundedSqlite.open(database, configuration().files()).connect()) {
      execute(connection, "BEGIN IMMEDIATE");
      long slot;
      try (var query =
          connection.prepareStatement(
              "SELECT state_slot FROM ps_v2_scopes WHERE generation=1 AND id=? AND producer=0")) {
        query.setLong(1, child.scope());
        try (var row = query.executeQuery()) {
          assertTrue(row.next());
          slot = row.getLong(1);
        }
      }
      FixedRecords.Snapshot image =
          FixedRecords.read(
              connection,
              slot,
              FixedRecords.Kind.SCOPE,
              FixedRecords.key(
                  binding, FixedRecords.Kind.SCOPE, child.scope(), child.producer(), 0, null));
      ScopeState state = ScopeState.decode(image.body());
      assertNotNull(state.seal());
      Records.ScopeSummary summary =
          new Records.ScopeSummary(
              child.scope(),
              child.producer(),
              PARENT,
              state.seal(),
              state.declared(),
              counts,
              statusRoot,
              closedAt);
      ScopeState replacement =
          new ScopeState(
              state.id(),
              state.producer(),
              state.parent(),
              state.declared(),
              state.last(),
              state.seal(),
              state.cancelled(),
              state.revoked(),
              summary);
      FixedRecords.replace(
          connection,
          configuration().files(),
          slot,
          FixedRecords.Kind.SCOPE,
          image.header().key(),
          image.header().revision(),
          replacement.encode(),
          false);
      execute(connection, "COMMIT");
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
                    "copy", Set.of(0, 1), AdmissionStore.RestartSafety.IDEMPOTENT)),
            4,
            4));
  }

  private static Records.InputHeader header() throws Exception {
    return header(operation(2), PARENT, 1);
  }

  private static Records.InputHeader header(
      Records.OperationId operation, Records.WorkKey work, int mode) throws Exception {
    return new Records.InputHeader(
        1,
        operation,
        new Records.AdmitParameters(
            work,
            new Records.Input(
                0,
                new Records.Digest(MessageDigest.getInstance("SHA-256").digest(new byte[0])),
                "application/octet-stream"),
            "copy",
            mode,
            1000,
            new Records.OutputBudget(0, 0)));
  }

  private static void execute(Connection connection, String sql) throws Exception {
    try (var statement = connection.createStatement()) {
      statement.execute(sql);
    }
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

  private record Branch(
      Path database,
      Path inputsPath,
      SessionStore sessions,
      InputStore inputs,
      Messages.Binding binding,
      Records.ChildScope child) {}
}
