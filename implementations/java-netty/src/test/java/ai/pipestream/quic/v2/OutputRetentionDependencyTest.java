package ai.pipestream.quic.v2;

import static ai.pipestream.quic.v2.Messages.DURABLE_WORK;
import static ai.pipestream.quic.v2.Messages.RESULT_DELIVERY;
import static org.junit.jupiter.api.Assertions.*;

import ai.pipestream.quic.BoundedSqlite;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.List;
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
