package ai.pipestream.quic.v2;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

@Timeout(45)
final class RetirementChildScopeTest {
  private static final long RETIRE_AT = 31_200;

  @TempDir Path directory;

  @Test
  void nonRootScopeIsRemovedOnlyAfterItsWorkAndPostCommitInterruptionReopens() throws Exception {
    AtomicLong now = new AtomicLong(1000);
    try (BranchExecutionTest.Fixture fixture =
        new BranchExecutionTest.Fixture(directory, "child-retirement", now)) {
      fixture.createParent(new byte[0], true);
      fixture.declareChildren(List.of(10L), true);
      assertEquals(
          RetirementStore.State.NOT_READY,
          fixture
              .sessions
              .retireSession(fixture.generation, fixture.inputs, 1, ResultFixture.clock(1000))
              .state());
      Records.WorkKey child =
          new Records.WorkKey(fixture.child.scope(), fixture.child.producer(), 10);
      fixture.admit(child, new byte[0], 0, 1, 1, operation(90));
      Records.WorkView childView =
          fixture
              .runtime(
                  context -> {
                    context.beginOutput(1, "application/octet-stream");
                    context.writeOutput(ByteBuffer.wrap(new byte[] {1}));
                    assertEquals(0, context.finishOutput());
                    return ExecutionRuntime.Outcome.succeeded();
                  })
              .run(executionAccess(), fixture.generation, child);
      assertEquals(Records.State.SUCCEEDED, childView.state());
      fixture.closeChild();

      now.set(1200);
      Records.WorkView parentView =
          fixture
              .runtime(
                  context -> {
                    context.beginOutput(5, "application/octet-stream");
                    context.writeOutput(ByteBuffer.wrap(new byte[] {2, 3, 4, 5, 6}));
                    assertEquals(0, context.finishOutput());
                    return ExecutionRuntime.Outcome.succeeded();
                  })
              .run(executionAccess(), fixture.generation, fixture.parent);
      assertEquals(Records.State.SUCCEEDED, parentView.state());
      fixture.closeRoot();
      Records.ScopeSummary childSummary =
          fixture.sessions.scopeSummary(
              access(), selected(), fixture.generation, fixture.child.scope(), fixture.childSeal());
      Records.ScopeSummary rootSummary =
          fixture.sessions.scopeSummary(
              access(), selected(), fixture.generation, 0, fixture.rootSeal());

      release(fixture, child);
      release(fixture, fixture.parent);
      assertEquals(new InputStore.Usage(0, 0, 0), fixture.inputs.usage());
      assertEquals(
          RetirementStore.State.NOT_READY,
          fixture
              .sessions
              .retireSession(
                  fixture.generation, fixture.inputs, 1, ResultFixture.clock(RETIRE_AT - 1))
              .state());
      assertEquals(
          RetirementStore.State.STARTED,
          fixture
              .sessions
              .retireSession(fixture.generation, fixture.inputs, 1, ResultFixture.clock(RETIRE_AT))
              .state());

      List<RetirementStore.Phase> phases = new ArrayList<>();
      java.sql.SQLException interruption = null;
      for (int calls = 0; calls < 64 && interruption == null; calls++) {
        try {
          fixture.sessions.retireSession(
              fixture.generation,
              fixture.inputs,
              1,
              ResultFixture.clock(RETIRE_AT),
              phase -> {
                phases.add(phase);
                if (phase == RetirementStore.Phase.SCOPE_REMOVED)
                  throw new java.sql.SQLException("test scope removal interruption");
              });
        } catch (java.sql.SQLException failure) {
          interruption = failure;
        }
      }
      assertNotNull(interruption);
      assertEquals("test scope removal interruption", interruption.getMessage());
      assertTrue(phases.contains(RetirementStore.Phase.ENTITY_REMOVED));
      assertTrue(phases.contains(RetirementStore.Phase.OPERATION_REMOVED));
      assertEquals(RetirementStore.Phase.SCOPE_REMOVED, phases.get(phases.size() - 1));

      fixture.reopen();
      assertEquals(new InputStore.Usage(0, 0, 0), fixture.inputs.usage());
      RetirementStore.Progress complete = finish(fixture);
      assertEquals(RetirementStore.State.COMPLETE, complete.state());
      assertEquals(
          RetirementStore.State.ABSENT,
          fixture
              .sessions
              .retireSession(fixture.generation, fixture.inputs, 1, ResultFixture.clock(RETIRE_AT))
              .state());
      assertNotNull(childSummary);
      assertNotNull(rootSummary);
    }
  }

  private static void release(BranchExecutionTest.Fixture fixture, Records.WorkKey work)
      throws Exception {
    assertEquals(
        RetentionStore.Result.RELEASED,
        fixture.sessions.reclaimOutput(
            fixture.generation, work, fixture.inputs, ResultFixture.clock(21_200)));
    assertEquals(
        RetentionStore.Result.RELEASED,
        fixture.sessions.reclaimInput(
            fixture.generation, work, fixture.inputs, ResultFixture.clock(21_200)));
  }

  private static RetirementStore.Progress finish(BranchExecutionTest.Fixture fixture)
      throws Exception {
    RetirementStore.Progress progress = null;
    for (int calls = 0; calls < 64; calls++) {
      progress =
          fixture.sessions.retireSession(
              fixture.generation, fixture.inputs, 1, ResultFixture.clock(RETIRE_AT));
      if (progress.state() == RetirementStore.State.COMPLETE) return progress;
      assertEquals(RetirementStore.State.IN_PROGRESS, progress.state());
    }
    return fail("child-scope retirement did not finish: " + progress);
  }

  private static Messages.Capabilities selected() {
    return new Messages.Capabilities(
        true,
        List.of(Messages.DURABLE_WORK, Messages.RESULT_DELIVERY),
        List.of(),
        1 << 20,
        16,
        32,
        1 << 20,
        1000,
        5000);
  }

  private static Records.OperationId operation(int value) {
    byte[] bytes = new byte[16];
    bytes[15] = (byte) value;
    return new Records.OperationId(bytes);
  }

  private static SessionStore.Access access() {
    return new SessionStore.Access("alice", () -> {});
  }

  private static ExecutionStore.Access executionAccess() {
    return new ExecutionStore.Access("alice", () -> {});
  }
}
