package ai.pipestream.quic.v2;

import static ai.pipestream.quic.v2.Messages.DURABLE_WORK;
import static org.junit.jupiter.api.Assertions.*;

import ai.pipestream.quic.BoundedSqlite;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

@Timeout(15)
final class AdmissionStoreTest {
  private static final Messages.Capabilities SELECTED =
      new Messages.Capabilities(
          true, List.of(DURABLE_WORK), List.of(), 65536, 4, 16, 1 << 20, 1000, 5000);
  private static final Records.Policy POLICY = new Records.Policy(10_000, 20_000, 30_000);
  private static final InputStore.Limits INPUT_LIMITS =
      new InputStore.Limits(8L << 20, 32, 1 << 20, 8);
  private static final AdmissionStore.Clock CLOCK = () -> new AdmissionStore.Time(1000, true);
  private static final AdmissionStore.Authorization ALLOW = (binding, parameters) -> {};

  @TempDir Path directory;

  @Test
  void exactLeafAdmissionCommitsTypedViewJobFundingAndReplayAcrossReopen() throws Exception {
    Fixture fixture = fixture("leaf", 0, 0, 0, execution(4, 4));
    try (InputStore inputs = fixture.inputs()) {
      assertNotNull(inputs.identity());
      assertEquals(Optional.empty(), check(fixture));
      assertCode(
          ProtocolError.Code.NOT_READY,
          () ->
              fixture
                  .sessions()
                  .admit(
                      access("alice"),
                      SELECTED,
                      1,
                      fixture.inputs(),
                      fixture.header(),
                      2,
                      CLOCK,
                      ALLOW));
      install(fixture);
      Messages.AdmissionResponse admitted = admit(fixture, 2);
      assertTrue(admitted.request().input());
      assertEquals(2, admitted.request().id());
      Records.Admitted outcome =
          assertInstanceOf(Records.Admitted.class, admitted.receipt().outcome());
      assertEquals(new Records.WorkKey(0, 0, 1), outcome.work());
      assertEquals(1, outcome.attempt());
      assertEquals(1000, outcome.admittedAt());
      assertEquals(2000, outcome.deadline());
      assertNull(outcome.child());
      assertEquals(admitted.receipt(), check(fixture).orElseThrow());
      assertEquals(admitted.receipt(), admit(fixture, 6).receipt());
      assertView(fixture.sessions(), Records.State.ACTIVE, null);
      assertEquals(1, scalar(fixture.database(), "SELECT count(*) FROM ps_v2_jobs"));
      assertEquals(2, scalar(fixture.database(), "SELECT operation_count FROM ps_v2_sessions"));

      long jobSlot = scalar(fixture.database(), "SELECT state_slot FROM ps_v2_jobs");
      try (var connection =
          BoundedSqlite.open(fixture.database(), configuration(execution(4, 4)).files())
              .connect()) {
        FixedRecords.Header job = FixedRecords.header(connection, jobSlot, FixedRecords.Kind.JOB);
        assertEquals(FixedRecords.JOB_CAPACITY, job.capacity());
        assertEquals(FixedRecords.JOB_CREDITS, job.credits());
      }
    }
    SessionStore reopened = SessionStore.open(fixture.database(), configuration(execution(4, 4)));
    try (InputStore inputs = InputStore.open(fixture.inputsPath(), INPUT_LIMITS)) {
      reopened.verifyInputs(inputs);
      assertView(reopened, Records.State.ACTIVE, null);
      assertEquals(
          fixture.admissionOperation(),
          reopened
              .lookupOperation(
                  access("alice"),
                  SELECTED,
                  1,
                  new Messages.LookupOperation(9, fixture.admissionOperation()))
              .receipt()
              .operation());
    }
  }

  @Test
  void callerAndAuthorityBranchesAllocateOneStableChildAndReplay() throws Exception {
    for (int mode : List.of(1, 2)) {
      Fixture fixture = fixture("branch-" + mode, mode, 0, 0, execution(4, 4));
      try (InputStore inputs = fixture.inputs()) {
        assertNotNull(inputs.identity());
        install(fixture);
        Records.Admitted first =
            assertInstanceOf(Records.Admitted.class, admit(fixture, 2).receipt().outcome());
        assertNotNull(first.child());
        assertEquals(mode == 1 ? 0 : 1, first.child().producer());
        assertTrue(first.child().scope() > 0);
        assertEquals(first, admit(fixture, 3).receipt().outcome());
        assertView(
            fixture.sessions(),
            mode == 1 ? Records.State.WAITING_CHILDREN : Records.State.ACTIVE,
            first.child());
        assertEquals(2, scalar(fixture.database(), "SELECT count(*) FROM ps_v2_scopes"));
      }
      SessionStore reopened = SessionStore.open(fixture.database(), configuration(execution(4, 4)));
      try (InputStore inputs = InputStore.open(fixture.inputsPath(), INPUT_LIMITS)) {
        reopened.verifyInputs(inputs);
        Records.WorkView view = view(reopened);
        assertNotNull(view.child());
        assertEquals(mode == 1 ? 0 : 1, view.child().producer());
      }
    }
  }

  @Test
  void callerChildDeclarationAndAdmissionUseTheirOwnDeadlineAfterParentDeadline() throws Exception {
    Fixture fixture = fixture("child-deadline", 1, 0, 0, execution(4, 4));
    try (InputStore inputs = fixture.inputs()) {
      assertNotNull(inputs.identity());
      install(fixture);
      Records.Admitted parent =
          assertInstanceOf(Records.Admitted.class, admit(fixture, 2).receipt().outcome());
      Records.ChildScope child = parent.child();
      assertNotNull(child);
      assertEquals(0, child.producer());
      assertEquals(2000, parent.deadline());

      fixture
          .sessions()
          .declare(
              access("alice"),
              SELECTED,
              1,
              new Messages.Declare(3, operation(3), child.scope(), List.of(2L), false));
      byte[] payload = new byte[0];
      Records.InputHeader childHeader =
          header(4, payload, "copy", 0, 0, 2, 0, child.scope(), child.producer());
      install(inputs, context(fixture.binding()), childHeader, payload);
      Records.Admitted admitted =
          assertInstanceOf(
              Records.Admitted.class,
              admit(fixture, childHeader, 4, () -> new AdmissionStore.Time(3000, true), ALLOW)
                  .receipt()
                  .outcome());
      assertEquals(3000, admitted.admittedAt());
      assertEquals(4000, admitted.deadline());
      assertEquals(Records.State.WAITING_CHILDREN, view(fixture.sessions()).state());
    }
  }

  @Test
  void identityDeclarationApplicationAndModeRefusalsDoNotCreateJobs() throws Exception {
    Fixture fixture = fixture("refusals", 0, 0, 0, execution(4, 4));
    try (InputStore inputs = fixture.inputs()) {
      assertNotNull(inputs.identity());
      install(fixture);
      assertCode(
          ProtocolError.Code.CONFLICT,
          () ->
              fixture
                  .sessions()
                  .checkInput(
                      access("alice"),
                      SELECTED,
                      1,
                      fixture.inputs(),
                      new Records.InputHeader(
                          2, fixture.header().operation(), fixture.header().parameters()),
                      CLOCK,
                      ALLOW));
      Records.InputHeader producerOne =
          changed(fixture.header(), new Records.WorkKey(0, 1, 1), "copy", 0);
      assertCode(ProtocolError.Code.UNAUTHORIZED, () -> check(fixture, producerOne, CLOCK, ALLOW));
      assertCode(
          ProtocolError.Code.CONFLICT,
          () ->
              check(
                  fixture,
                  changed(fixture.header(), new Records.WorkKey(0, 0, 99), "copy", 0),
                  CLOCK,
                  ALLOW));
      assertCode(
          ProtocolError.Code.APPLICATION_UNSUPPORTED,
          () ->
              check(
                  fixture,
                  changed(fixture.header(), new Records.WorkKey(0, 0, 1), "unknown", 0),
                  CLOCK,
                  ALLOW));
      assertCode(
          ProtocolError.Code.APPLICATION_UNSUPPORTED,
          () ->
              check(
                  fixture,
                  changed(fixture.header(), new Records.WorkKey(0, 0, 1), "leaf-only", 1),
                  CLOCK,
                  ALLOW));
      SessionStore.Access denied =
          new SessionStore.Access(
              "alice",
              () -> {
                throw new ProtocolError(ProtocolError.Code.UNAUTHORIZED, "revoked");
              });
      assertCode(
          ProtocolError.Code.UNAUTHORIZED,
          () ->
              fixture
                  .sessions()
                  .checkInput(
                      denied, SELECTED, 999, fixture.inputs(), fixture.header(), CLOCK, ALLOW));
      assertEquals(0, scalar(fixture.database(), "SELECT count(*) FROM ps_v2_jobs"));
    }
  }

  @Test
  void clockSafetyOverflowAndFinalAuthorizationRefuseAtomically() throws Exception {
    Fixture fixture = fixture("clock", 0, 0, 0, execution(4, 4));
    try (InputStore inputs = fixture.inputs()) {
      assertNotNull(inputs.identity());
      install(fixture);
      assertCode(
          ProtocolError.Code.CLOCK_UNSAFE,
          () ->
              check(fixture, fixture.header(), () -> new AdmissionStore.Time(1000, false), ALLOW));
      assertCode(
          ProtocolError.Code.LIMIT_EXCEEDED,
          () ->
              check(
                  fixture,
                  fixture.header(),
                  () -> new AdmissionStore.Time(Long.MAX_VALUE, true),
                  ALLOW));
      AtomicInteger checks = new AtomicInteger();
      AdmissionStore.Authorization revoked =
          (binding, parameters) -> {
            if (checks.incrementAndGet() == 3)
              throw new ProtocolError(ProtocolError.Code.UNAUTHORIZED, "policy changed");
          };
      assertCode(
          ProtocolError.Code.UNAUTHORIZED,
          () -> admit(fixture, fixture.header(), 7, CLOCK, revoked));
      assertEquals(0, scalar(fixture.database(), "SELECT count(*) FROM ps_v2_jobs"));
      assertEquals(1, scalar(fixture.database(), "SELECT operation_count FROM ps_v2_sessions"));
      assertView(fixture.sessions(), Records.State.DECLARED, null);
      assertTrue(
          fixture.inputs().find(context(fixture.binding()), fixture.header()).isPresent(),
          "failed metadata commit may retain funded immutable input orphan");
    }
  }

  @Test
  void sessionAndExecutionJobCapsRefuseWithoutPartialAdmission() throws Exception {
    AdmissionStore.ExecutionPolicy one = execution(1, 1);
    Fixture first = fixture("quota-one", 0, 0, 0, one);
    try (InputStore inputs = first.inputs()) {
      assertNotNull(inputs.identity());
      install(first);
      admit(first, 2);
      declare(first.sessions(), 3, 2);
      byte[] payload = new byte[0];
      Records.InputHeader second = header(4, payload, "copy", 0, 0, 2, 0);
      install(first.inputs(), context(first.binding()), second, payload);
      assertCode(ProtocolError.Code.LIMIT_EXCEEDED, () -> admit(first, second, 3, CLOCK, ALLOW));
      assertEquals(1, scalar(first.database(), "SELECT count(*) FROM ps_v2_jobs"));
      assertView(first.sessions(), Records.State.ACTIVE, null);
    }
  }

  private Fixture fixture(
      String name,
      int mode,
      int outputs,
      long outputBytes,
      AdmissionStore.ExecutionPolicy execution)
      throws Exception {
    Path database = directory.resolve(name + ".sqlite");
    Path inputsPath = directory.resolve(name + "-inputs");
    SessionStore sessions = SessionStore.initialize(database, configuration(execution));
    Messages.Binding binding =
        sessions.create(access("alice"), SELECTED, new Messages.Create(1, 1, POLICY));
    declare(sessions, 1, 1);
    byte[] payload = new byte[0];
    Records.InputHeader header = header(2, payload, "copy", mode, outputs, 1, outputBytes);
    InputStore inputs =
        InputStore.initializeForAuthority(inputsPath, INPUT_LIMITS, sessions.identity());
    sessions.bindInputs(inputs);
    return new Fixture(
        database, inputsPath, sessions, inputs, binding, header, operation(2), payload);
  }

  private static AdmissionStore.ExecutionPolicy execution(int jobs, int ownerJobs) {
    return new AdmissionStore.ExecutionPolicy(
        List.of(
            new AdmissionStore.Application(
                "copy", Set.of(0, 1, 2), AdmissionStore.RestartSafety.IDEMPOTENT),
            new AdmissionStore.Application(
                "leaf-only", Set.of(0), AdmissionStore.RestartSafety.TRANSACTIONAL)),
        jobs,
        ownerJobs);
  }

  private static SessionStore.Configuration configuration(
      AdmissionStore.ExecutionPolicy execution) {
    return new SessionStore.Configuration(
        "issuer-a",
        new Records.Limits(8, 32, 32, 1 << 20, 1 << 20, 4),
        new Records.Policy(60_000, 60_000, 60_000),
        4,
        8,
        4,
        BoundedSqlite.Limits.defaults(),
        execution);
  }

  private static void declare(SessionStore store, int operation, long entity) throws Exception {
    store.declare(
        access("alice"),
        SELECTED,
        1,
        new Messages.Declare(2, operation(operation), 0, List.of(entity), false));
  }

  private static Optional<Records.OperationReceipt> check(Fixture fixture) throws Exception {
    return check(fixture, fixture.header(), CLOCK, ALLOW);
  }

  private static Optional<Records.OperationReceipt> check(
      Fixture fixture,
      Records.InputHeader header,
      AdmissionStore.Clock clock,
      AdmissionStore.Authorization authorization)
      throws Exception {
    return fixture
        .sessions()
        .checkInput(access("alice"), SELECTED, 1, fixture.inputs(), header, clock, authorization);
  }

  private static Messages.AdmissionResponse admit(Fixture fixture, long stream) throws Exception {
    return admit(fixture, fixture.header(), stream, CLOCK, ALLOW);
  }

  private static Messages.AdmissionResponse admit(
      Fixture fixture,
      Records.InputHeader header,
      long stream,
      AdmissionStore.Clock clock,
      AdmissionStore.Authorization authorization)
      throws Exception {
    return fixture
        .sessions()
        .admit(
            access("alice"), SELECTED, 1, fixture.inputs(), header, stream, clock, authorization);
  }

  private static void install(Fixture fixture) throws Exception {
    install(fixture.inputs(), context(fixture.binding()), fixture.header(), fixture.payload());
  }

  private static void install(
      InputStore inputs, Commitments.Context context, Records.InputHeader header, byte[] payload)
      throws Exception {
    try (InputStore.Receiver receiver = inputs.begin(context, header, SELECTED, 1)) {
      receiver.write(ByteBuffer.wrap(payload), 2);
      receiver.finish(3);
    }
  }

  private static Records.WorkView view(SessionStore sessions) throws Exception {
    return sessions
        .snapshot(
            access("alice"),
            SELECTED,
            1,
            new Messages.Watch(20, new Records.WorkKey(0, 0, 1), 0, 0))
        .work();
  }

  private static void assertView(
      SessionStore sessions, Records.State state, Records.ChildScope child) throws Exception {
    Records.WorkView view = view(sessions);
    assertEquals(state, view.state());
    assertEquals(state == Records.State.DECLARED ? 0 : 1, view.attempt());
    assertEquals(child, view.child());
  }

  private static Records.InputHeader changed(
      Records.InputHeader original, Records.WorkKey work, String application, int mode) {
    Records.AdmitParameters parameters = original.parameters();
    return new Records.InputHeader(
        original.generation(),
        original.operation(),
        new Records.AdmitParameters(
            work,
            parameters.input(),
            application,
            mode,
            parameters.executionMs(),
            parameters.outputs()));
  }

  private static Records.InputHeader header(
      int operation,
      byte[] payload,
      String application,
      int mode,
      int outputs,
      long entity,
      long outputBytes)
      throws Exception {
    return header(operation, payload, application, mode, outputs, entity, outputBytes, 0, 0);
  }

  private static Records.InputHeader header(
      int operation,
      byte[] payload,
      String application,
      int mode,
      int outputs,
      long entity,
      long outputBytes,
      long scope,
      int producer)
      throws Exception {
    return new Records.InputHeader(
        1,
        operation(operation),
        new Records.AdmitParameters(
            new Records.WorkKey(scope, producer, entity),
            new Records.Input(
                payload.length,
                new Records.Digest(MessageDigest.getInstance("SHA-256").digest(payload)),
                "application/octet-stream"),
            application,
            mode,
            1000,
            new Records.OutputBudget(outputs, outputBytes)));
  }

  private static Records.OperationId operation(int value) {
    byte[] bytes = new byte[16];
    bytes[15] = (byte) value;
    return new Records.OperationId(bytes);
  }

  private static SessionStore.Access access(String owner) {
    return new SessionStore.Access(owner, () -> {});
  }

  private static Commitments.Context context(Messages.Binding binding) {
    return new Commitments.Context(binding.authority(), binding.owner(), binding.generation());
  }

  private static long scalar(Path database, String sql) throws Exception {
    try (var connection =
            BoundedSqlite.open(database, configuration(execution(4, 4)).files()).connect();
        var statement = connection.createStatement();
        var rows = statement.executeQuery(sql)) {
      assertTrue(rows.next());
      return rows.getLong(1);
    }
  }

  private static void assertCode(ProtocolError.Code code, Throwing action) {
    ProtocolError error = assertThrows(ProtocolError.class, action::run);
    assertEquals(code, error.code(), error::getMessage);
  }

  private record Fixture(
      Path database,
      Path inputsPath,
      SessionStore sessions,
      InputStore inputs,
      Messages.Binding binding,
      Records.InputHeader header,
      Records.OperationId admissionOperation,
      byte[] payload) {}

  @FunctionalInterface
  private interface Throwing {
    void run() throws Exception;
  }
}
