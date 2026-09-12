package ai.pipestream.quic.v2;

import static ai.pipestream.quic.v2.Messages.DURABLE_WORK;
import static ai.pipestream.quic.v2.Messages.RESULT_DELIVERY;
import static org.junit.jupiter.api.Assertions.*;

import ai.pipestream.quic.BoundedSqlite;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

@Timeout(30)
final class OutputRetentionDependencyTest {
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

  @TempDir Path directory;

  @Test
  void childOutputExpiryWaitsForParentSettlementAndThenForItsPhysicalReader() throws Exception {
    AtomicLong now = new AtomicLong(1000);
    try (BranchExecutionTest.Fixture fixture =
        new BranchExecutionTest.Fixture(directory, "dependent-output", now)) {
      fixture.createParent(new byte[0], true);
      fixture.declareChildren(List.of(10L), true);
      Records.WorkKey child =
          new Records.WorkKey(fixture.child.scope(), fixture.child.producer(), 10);
      Records.InputHeader childHeader = fixture.admit(child, new byte[0], 0, 1, 3, operation(90));
      AtomicReference<ExecutionStore.Lease> childLease = new AtomicReference<>();
      byte[] childBytes = {1, 2, 3};
      Records.WorkView childView =
          fixture
              .runtime(
                  context -> {
                    childLease.set(context.lease());
                    context.beginOutput(childBytes.length, "application/octet-stream");
                    context.writeOutput(ByteBuffer.wrap(childBytes));
                    assertEquals(0, context.finishOutput());
                    return ExecutionRuntime.Outcome.succeeded();
                  })
              .run(executionAccess(), fixture.generation, child);
      assertEquals(Records.State.SUCCEEDED, childView.state());
      fixture.closeChild();
      Records.ScopeSummary childSummary =
          fixture.sessions.scopeSummary(
              access(), SELECTED, fixture.generation, fixture.child.scope(), fixture.childSeal());
      assertEquals(1100, childView.outputUntil());

      now.set(1100);
      assertEquals(
          RetentionStore.Result.NOT_READY,
          fixture.sessions.reclaimOutput(
              fixture.generation, child, fixture.inputs, clock(now.get())));
      assertNull(job(fixture, child).record().outputReleaseAt());

      OutputStore.Stored stored =
          fixture
              .inputs
              .findOutput(fixture.context(), childHeader, childLease.get(), 0)
              .orElseThrow();
      try (InputStream reader = stored.openStream()) {
        assertEquals(childBytes[0], reader.read());
        Records.WorkView parent =
            fixture
                .runtime(
                    context -> {
                      context.beginOutput(5, "application/octet-stream");
                      context.writeOutput(ByteBuffer.wrap(new byte[] {4, 5, 6, 7, 8}));
                      assertEquals(0, context.finishOutput());
                      return ExecutionRuntime.Outcome.succeeded();
                    })
                .run(executionAccess(), fixture.generation, fixture.parent);
        assertEquals(Records.State.SUCCEEDED, parent.state());
        assertEquals(
            RetentionStore.Result.PINNED,
            fixture.sessions.reclaimOutput(
                fixture.generation, child, fixture.inputs, clock(now.get())));
        assertNull(job(fixture, child).record().outputReleaseAt());
        assertTrue(job(fixture, child).record().outputsLive());
      }

      assertEquals(
          RetentionStore.Result.RELEASED,
          fixture.sessions.reclaimOutput(
              fixture.generation, child, fixture.inputs, clock(now.get())));
      assertEquals(1100L, job(fixture, child).record().outputReleaseAt());
      assertFalse(job(fixture, child).record().outputsLive());
      assertEquals(
          childSummary,
          fixture.sessions.scopeSummary(
              access(), SELECTED, fixture.generation, fixture.child.scope(), fixture.childSeal()));

      fixture.reopen();
      assertEquals(1100L, job(fixture, child).record().outputReleaseAt());
      assertEquals(childView, fixture.view(child));
      assertEquals(0, fixture.inputs.usage().handles());
    }
  }

  @Test
  void branchAdmissionRefusesWhenItsChildDependencyPinCannotBeFunded() throws Exception {
    AdmissionStore.Application application =
        new AdmissionStore.Application(
            "copy", Set.of(0, 1), AdmissionStore.RestartSafety.IDEMPOTENT);
    Path database = directory.resolve("unfunded-pin.sqlite");
    Path inputPath = directory.resolve("unfunded-pin-inputs");
    SessionStore sessions = SessionStore.initialize(database, configuration(application));
    Messages.Binding binding =
        sessions.create(
            access(), SELECTED, new Messages.Create(1, 1, new Records.Policy(10_000, 100, 30_000)));
    long generation = binding.generation();
    Records.WorkKey branch = new Records.WorkKey(0, 0, 1);
    Records.WorkKey leaf = new Records.WorkKey(0, 0, 2);
    AdmissionStore.Authorization allow = (retained, parameters) -> {};
    try (InputStore inputs =
        InputStore.initializeForAuthority(
            inputPath, new InputStore.Limits(16L << 20, 128, 1 << 20, 2), sessions.identity())) {
      sessions.bindInputs(inputs);
      sessions.declare(
          access(),
          SELECTED,
          generation,
          new Messages.Declare(2, operation(2), 0, List.of(1L, 2L), false));
      Records.InputHeader branchHeader = header(generation, branch, operation(3), 1);
      receive(inputs, generation, branchHeader);
      InputStore.Usage before = inputs.usage();

      ProtocolError refused =
          assertThrows(
              ProtocolError.class,
              () ->
                  sessions.admit(
                      access(), SELECTED, generation, inputs, branchHeader, 4, clock(1000), allow));
      assertEquals(ProtocolError.Code.LIMIT_EXCEEDED, refused.code(), refused::getMessage);
      assertTrue(refused.getMessage().contains("dependency"), refused.getMessage());
      assertEquals(before, inputs.usage());
      Records.WorkView declared =
          sessions
              .snapshot(access(), SELECTED, generation, new Messages.Watch(5, branch, 0, 0))
              .work();
      assertEquals(Records.State.DECLARED, declared.state());
      assertNull(declared.child());
      try (var connection =
          BoundedSqlite.open(database, configuration(application).files()).connect()) {
        assertNull(AdmissionStore.job(connection, binding, branch));
      }

      Records.InputHeader leafHeader = header(generation, leaf, operation(6), 0);
      receive(inputs, generation, leafHeader);
      Records.Admitted admitted =
          assertInstanceOf(
              Records.Admitted.class,
              sessions
                  .admit(access(), SELECTED, generation, inputs, leafHeader, 7, clock(1000), allow)
                  .receipt()
                  .outcome());
      assertEquals(leaf, admitted.work());
      assertNull(admitted.child());
    }
  }

  private static Records.InputHeader header(
      long generation, Records.WorkKey work, Records.OperationId operation, int mode)
      throws Exception {
    return new Records.InputHeader(
        generation,
        operation,
        new Records.AdmitParameters(
            work,
            new Records.Input(0, ResultFixture.digest(new byte[0]), "application/octet-stream"),
            "copy",
            mode,
            5000,
            new Records.OutputBudget(1, 5)));
  }

  private static void receive(InputStore inputs, long generation, Records.InputHeader header)
      throws Exception {
    Commitments.Context context = new Commitments.Context("issuer-a", "alice", generation);
    try (InputStore.Receiver receiver = inputs.begin(context, header, SELECTED, 1000)) {
      receiver.write(ByteBuffer.allocate(0), 1000);
      receiver.finish(1000);
    }
  }

  private static AdmissionStore.StoredJob job(
      BranchExecutionTest.Fixture fixture, Records.WorkKey work) throws Exception {
    try (var connection =
        BoundedSqlite.open(fixture.database, configuration(fixture.application).files())
            .connect()) {
      return AdmissionStore.job(connection, fixture.binding, work);
    }
  }

  private static SessionStore.Configuration configuration(AdmissionStore.Application application) {
    return new SessionStore.Configuration(
        "issuer-a",
        new Records.Limits(32, 64, 64, 1 << 20, 1 << 20, 8),
        new Records.Policy(60_000, 60_000, 60_000),
        4,
        16,
        4,
        BoundedSqlite.Limits.defaults(),
        new AdmissionStore.ExecutionPolicy(List.of(application), 16, 16));
  }

  private static Records.OperationId operation(int value) {
    byte[] bytes = new byte[16];
    bytes[15] = (byte) value;
    return new Records.OperationId(bytes);
  }

  private static AdmissionStore.Clock clock(long now) {
    return () -> new AdmissionStore.Time(now, true);
  }

  private static SessionStore.Access access() {
    return new SessionStore.Access("alice", () -> {});
  }

  private static ExecutionStore.Access executionAccess() {
    return new ExecutionStore.Access("alice", () -> {});
  }
}
