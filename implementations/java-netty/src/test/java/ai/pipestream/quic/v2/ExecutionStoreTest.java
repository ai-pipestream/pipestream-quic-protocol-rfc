package ai.pipestream.quic.v2;

import static ai.pipestream.quic.v2.Messages.DURABLE_WORK;
import static org.junit.jupiter.api.Assertions.*;

import ai.pipestream.quic.BoundedSqlite;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.sql.Connection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

@Timeout(20)
final class ExecutionStoreTest {
  private static final Messages.Capabilities SELECTED =
      new Messages.Capabilities(
          true, List.of(DURABLE_WORK), List.of(), 1 << 20, 8, 16, 1 << 20, 1000, 5000);
  private static final InputStore.Limits INPUT_LIMITS =
      new InputStore.Limits(4L << 20, 32, 1 << 20, 8);
  private static final AdmissionStore.Authorization ALLOW = (binding, parameters) -> {};

  @TempDir Path directory;

  @Test
  void claimRenewAndCheckPreserveWireAttemptDeadlineAndFundedCredits() throws Exception {
    Fixture fixture = fixture("renew", 0);
    try (InputStore inputs = fixture.inputs()) {
      assertNotNull(inputs.identity());
      long[] credits = credits(fixture);
      ExecutionStore.Lease first = claim(fixture, 1100, 500);
      assertEquals(fixture.sessions().identity(), first.installation());
      assertEquals("alice", first.owner());
      assertEquals(1, first.generation());
      assertEquals(WORK, first.work());
      assertEquals(1, first.attempt());
      assertEquals(1, first.number());
      assertEquals(1600, first.until());
      fixture.sessions().checkExecution(execAccess(), first, clock(1200), ALLOW);
      assertCode(ProtocolError.Code.NOT_READY, () -> claim(fixture, 1200, 100));

      ExecutionStore.Lease renewed =
          fixture.sessions().renewExecution(execAccess(), first, 500, clock(1300), ALLOW);
      assertEquals(first.number(), renewed.number());
      assertEquals(1800, renewed.until());
      assertEquals(first.attempt(), renewed.attempt());
      assertEquals(credits[0], credits(fixture)[0]);
      assertEquals(credits[1], credits(fixture)[1]);
      Records.WorkView view = view(fixture);
      assertEquals(1, view.attempt());
      assertEquals(1000, view.admittedAt());
      assertEquals(2000, view.deadline());
    }
  }

  @Test
  void expiredLeaseReplacementFencesEveryStaleHandleWithoutChangingAttempt() throws Exception {
    Fixture fixture = fixture("replace", 0);
    try (InputStore inputs = fixture.inputs()) {
      assertNotNull(inputs.identity());
      ExecutionStore.Lease stale = claim(fixture, 1100, 100);
      ExecutionStore.Lease replacement = claim(fixture, 1201, 300);
      assertTrue(replacement.number() > stale.number());
      assertEquals(1, replacement.attempt());
      assertEquals(1501, replacement.until());
      assertCode(
          ProtocolError.Code.CONFLICT,
          () -> fixture.sessions().checkExecution(execAccess(), stale, clock(1201), ALLOW));
      assertCode(
          ProtocolError.Code.CONFLICT,
          () -> fixture.sessions().renewExecution(execAccess(), stale, 100, clock(1201), ALLOW));
      assertCode(ProtocolError.Code.DEADLINE_EXCEEDED, () -> claim(fixture, 2000, 1));
      fixture.sessions().checkExecution(execAccess(), replacement, clock(1300), ALLOW);
      assertEquals(1, view(fixture).attempt());
      assertEquals(2000, view(fixture).deadline());
    }
  }

  @Test
  void retryableAndTerminalFailureHaveDistinctDurableResourceState() throws Exception {
    Fixture retryable = fixture("retryable", 0);
    try (InputStore inputs = retryable.inputs()) {
      assertNotNull(inputs.identity());
      ExecutionStore.Lease lease = claim(retryable, 1100, 500);
      Records.WorkView failed =
          retryable
              .sessions()
              .failExecution(
                  execAccess(),
                  lease,
                  new Records.Diagnostic(7, "retry"),
                  true,
                  clock(1200),
                  ALLOW);
      assertEquals(Records.State.AWAITING_RETRY, failed.state());
      assertEquals(new Records.Diagnostic(7, "retry"), failed.diagnostic());
      JobRecord retry = job(retryable).record();
      assertEquals(JobRecord.Stage.AWAITING_RETRY, retry.stage());
      assertTrue(retry.inputLive());
      assertTrue(retry.outputsLive());
      assertTrue(retry.executorLive());
    }

    Fixture terminal = fixture("terminal", 0);
    try (InputStore inputs = terminal.inputs()) {
      assertNotNull(inputs.identity());
      long[] before = credits(terminal);
      ExecutionStore.Lease lease = claim(terminal, 1100, 500);
      Records.WorkView failed =
          terminal
              .sessions()
              .failExecution(
                  execAccess(),
                  lease,
                  new Records.Diagnostic(8, "fatal"),
                  false,
                  clock(1200),
                  ALLOW);
      assertEquals(Records.State.FAILED, failed.state());
      assertNull(failed.manifest());
      JobRecord record = job(terminal).record();
      assertEquals(JobRecord.Stage.SETTLED, record.stage());
      assertTrue(record.inputLive());
      assertTrue(record.outputsLive());
      assertFalse(record.executorLive());
      long[] after = credits(terminal);
      assertEquals(before[0] - 1, after[0]);
      assertEquals(before[1] - 1, after[1]);
      assertCode(
          ProtocolError.Code.ALREADY_TERMINAL,
          () -> terminal.sessions().checkExecution(execAccess(), lease, clock(1200), ALLOW));
    }
  }

  @Test
  void branchReadinessAndFinalAuthorizationTimeAreCheckedAtCommit() throws Exception {
    Fixture callerBranch = fixture("caller-branch", 1);
    try (InputStore inputs = callerBranch.inputs()) {
      assertNotNull(inputs.identity());
      assertCode(ProtocolError.Code.NOT_READY, () -> claim(callerBranch, 1100, 100));
      assertEquals(0, job(callerBranch).record().lease());
    }

    Fixture authorityBranch = fixture("authority-branch", 2);
    try (InputStore inputs = authorityBranch.inputs()) {
      assertNotNull(inputs.identity());
      ExecutionStore.Lease lease = claim(authorityBranch, 1100, 100);
      assertNotNull(view(authorityBranch).child());
      assertFalse(job(authorityBranch).record().expansionComplete());
      assertEquals(1, lease.attempt());
    }

    Fixture gated = fixture("gated", 0);
    try (InputStore inputs = gated.inputs()) {
      assertNotNull(inputs.identity());
      AtomicLong now = new AtomicLong(1100);
      AtomicInteger checks = new AtomicInteger();
      AdmissionStore.Authorization advance =
          (binding, parameters) -> {
            if (checks.incrementAndGet() == 2) now.set(2000);
          };
      assertCode(
          ProtocolError.Code.DEADLINE_EXCEEDED,
          () ->
              gated
                  .sessions()
                  .claimExecution(
                      execAccess(),
                      1,
                      WORK,
                      gated.inputs(),
                      100,
                      () -> new AdmissionStore.Time(now.get(), true),
                      advance));
      assertEquals(0, job(gated).record().lease());
      ExecutionStore.Access denied =
          new ExecutionStore.Access(
              "alice",
              () -> {
                throw new ProtocolError(ProtocolError.Code.UNAUTHORIZED, "revoked");
              });
      assertCode(
          ProtocolError.Code.UNAUTHORIZED,
          () ->
              gated
                  .sessions()
                  .claimExecution(denied, 999, WORK, gated.inputs(), 100, clock(0), ALLOW));
    }
  }

  @Test
  void deadlineExpirySettlesLocallyAndTerminalReobservationNeedsNoFreshClock() throws Exception {
    Fixture fixture = fixture("expiry", 0);
    try (InputStore inputs = fixture.inputs()) {
      assertNotNull(inputs.identity());
      assertCode(
          ProtocolError.Code.NOT_READY,
          () -> fixture.sessions().expireExecution(1, WORK, clock(1999)));
      Records.WorkView expired = fixture.sessions().expireExecution(1, WORK, clock(2000));
      assertEquals(Records.State.FAILED, expired.state());
      assertEquals(1, expired.attempt());
      assertNull(expired.manifest());
      Records.WorkView replay =
          fixture.sessions().expireExecution(1, WORK, () -> new AdmissionStore.Time(0, false));
      assertEquals(expired, replay);
      assertFalse(job(fixture).record().executorLive());
    }
  }

  private Fixture fixture(String name, int mode) throws Exception {
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
    Records.InputHeader header = header(mode);
    try (InputStore.Receiver receiver = inputs.begin(context(), header, SELECTED, 1)) {
      receiver.write(ByteBuffer.allocate(0), 2);
      receiver.finish(3);
    }
    sessions.admit(sessionAccess(), SELECTED, 1, inputs, header, 2, clock(1000), ALLOW);
    return new Fixture(database, inputsPath, sessions, inputs, binding);
  }

  private static ExecutionStore.Lease claim(Fixture fixture, long now, long duration)
      throws Exception {
    return fixture
        .sessions()
        .claimExecution(execAccess(), 1, WORK, fixture.inputs(), duration, clock(now), ALLOW);
  }

  private static Records.WorkView view(Fixture fixture) throws Exception {
    return fixture
        .sessions()
        .snapshot(sessionAccess(), SELECTED, 1, new Messages.Watch(9, WORK, 0, 0))
        .work();
  }

  private static AdmissionStore.StoredJob job(Fixture fixture) throws Exception {
    try (Connection connection =
        BoundedSqlite.open(fixture.database(), configuration().files()).connect()) {
      return AdmissionStore.job(connection, fixture.binding(), WORK);
    }
  }

  private static long[] credits(Fixture fixture) throws Exception {
    try (Connection connection =
        BoundedSqlite.open(fixture.database(), configuration().files()).connect()) {
      AdmissionStore.StoredJob job = AdmissionStore.job(connection, fixture.binding(), WORK);
      long viewSlot;
      try (var query =
          connection.prepareStatement(
              "SELECT view_slot FROM ps_v2_entities WHERE generation=? AND scope=? AND id=?")) {
        query.setLong(1, fixture.binding().generation());
        query.setLong(2, WORK.scope());
        query.setLong(3, WORK.entity());
        try (var row = query.executeQuery()) {
          assertTrue(row.next());
          viewSlot = row.getLong(1);
        }
      }
      FixedRecords.Header work = FixedRecords.header(connection, viewSlot, FixedRecords.Kind.WORK);
      FixedRecords.Header state =
          FixedRecords.header(connection, job.slot(), FixedRecords.Kind.JOB);
      return new long[] {work.credits(), state.credits()};
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
                    "copy", Set.of(0, 1, 2), AdmissionStore.RestartSafety.IDEMPOTENT)),
            4,
            4));
  }

  private static Records.InputHeader header(int mode) throws Exception {
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
            mode,
            1000,
            new Records.OutputBudget(0, 0)));
  }

  private static AdmissionStore.Clock clock(long utc) {
    return () -> new AdmissionStore.Time(utc, true);
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

  private static void assertCode(ProtocolError.Code expected, Throwing action) {
    ProtocolError error = assertThrows(ProtocolError.class, action::run);
    assertEquals(expected, error.code(), error::getMessage);
  }

  static final Records.WorkKey WORK = new Records.WorkKey(0, 0, 1);

  record Fixture(
      Path database,
      Path inputsPath,
      SessionStore sessions,
      InputStore inputs,
      Messages.Binding binding) {}

  @FunctionalInterface
  private interface Throwing {
    void run() throws Exception;
  }
}
