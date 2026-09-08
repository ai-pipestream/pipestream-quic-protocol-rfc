package ai.pipestream.quic.v2;

import static org.junit.jupiter.api.Assertions.*;

import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

@Timeout(30)
final class BranchSchedulerTest {
  @TempDir Path directory;

  @Test
  void oneWorkerAutomaticallyExecutesChildrenClosesScopesAndThenReassemblesParent()
      throws Exception {
    AtomicLong now = new AtomicLong(1000);
    try (BranchExecutionTest.Fixture fixture =
        new BranchExecutionTest.Fixture(directory, "scheduler", now)) {
      fixture.createParent(new byte[] {42}, true);
      fixture.declareChildren(List.of(10L, 20L), true);
      Records.WorkKey first = fixture.admitChild(10, 2);
      Records.WorkKey second = fixture.admitChild(20, 2);
      ExecutionStore.Candidate parentBefore = candidate(fixture, fixture.parent);
      assertEquals(1, parentBefore.mode());
      assertFalse(parentBefore.dependenciesReady());

      AtomicInteger parentCalls = new AtomicInteger();
      AtomicInteger childCalls = new AtomicInteger();
      AtomicReference<ExecutionStore.Lease> parentLease = new AtomicReference<>();
      ExecutionRuntime runtime =
          fixture.runtime(
              context -> {
                Records.WorkKey work = context.lease().work();
                if (work.equals(first) || work.equals(second)) {
                  childCalls.incrementAndGet();
                  byte value = (byte) work.entity();
                  context.beginOutput(2, "application/octet-stream");
                  context.writeOutput(ByteBuffer.wrap(new byte[] {value, (byte) (value + 1)}));
                  assertEquals(0, context.finishOutput());
                  return ExecutionRuntime.Outcome.succeeded();
                }
                assertEquals(fixture.parent, work);
                parentLease.set(context.lease());
                parentCalls.incrementAndGet();
                assertEquals(Records.State.SUCCEEDED, fixture.view(first).state());
                assertEquals(Records.State.SUCCEEDED, fixture.view(second).state());
                context.beginOutput(4, "application/octet-stream");
                copy(context, 10);
                copy(context, 20);
                assertEquals(0, context.finishOutput());
                return ExecutionRuntime.Outcome.succeeded();
              });
      ExecutionScheduler scheduler =
          new ExecutionScheduler(
              fixture.sessions,
              runtime,
              () -> new AdmissionStore.Time(now.get(), true),
              owner -> new ExecutionStore.Access(owner, () -> {}),
              new ExecutionScheduler.Limits(1, 1, 4, 1));
      scheduler.start();
      AtomicInteger peak = new AtomicInteger();
      try {
        await(
            () -> {
              peak.accumulateAndGet(scheduler.status().active(), Math::max);
              assertTrue(scheduler.status().active() <= 1);
              return fixture.view(fixture.parent).state() == Records.State.SUCCEEDED;
            });
        await(() -> summary(fixture, 0, fixture.rootSeal()) != null);
        assertEquals(2, childCalls.get());
        assertEquals(1, parentCalls.get());
        assertTrue(peak.get() <= 1);
        ExecutionStore.Candidate parentAfter = candidate(fixture, fixture.parent);
        assertTrue(parentAfter.dependenciesReady());
        Records.WorkView parent = fixture.view(fixture.parent);
        byte[] expected = {10, 11, 20, 21};
        assertEquals(digest(expected), parent.manifest().outputs().get(0).sha256());
        OutputStore.Stored stored =
            fixture
                .inputs
                .findOutput(fixture.context(), fixture.parentHeader, parentLease.get(), 0)
                .orElseThrow();
        try (InputStream stream = stored.openStream()) {
          assertArrayEquals(expected, stream.readAllBytes());
        }
        Records.ScopeSummary child =
            fixture.sessions.scopeSummary(
                new SessionStore.Access("alice", () -> {}),
                capabilities(),
                fixture.generation,
                fixture.child.scope(),
                fixture.childSeal());
        Records.ScopeSummary root = summary(fixture, 0, fixture.rootSeal());
        Commitments.StatusTree status = new Commitments.StatusTree(0, 0, 1);
        status.add(parent, child.statusRoot());
        assertEquals(status.finish().root(), root.statusRoot());
        scheduler.close();
        assertTrue(scheduler.awaitStopped(5000));
        fixture.reopen();
        assertEquals(parent, fixture.view(fixture.parent));
        assertEquals(root, summary(fixture, 0, fixture.rootSeal()));
      } finally {
        scheduler.close();
        assertTrue(scheduler.awaitStopped(5000));
      }
    }
  }

  private static void copy(ExecutionRuntime.Context context, long entity) throws Exception {
    Records.Output output = context.beginChildOutput(entity, 0);
    byte[] buffer = new byte[2];
    int read = context.readChildOutput(buffer, 0, buffer.length);
    assertEquals(output.length(), read);
    context.writeOutput(ByteBuffer.wrap(buffer, 0, read));
    assertEquals(-1, context.readChildOutput(buffer, 0, buffer.length));
    context.finishChildOutput();
  }

  private static ExecutionStore.Candidate candidate(
      BranchExecutionTest.Fixture fixture, Records.WorkKey work) throws Exception {
    ExecutionStore.Page page = fixture.sessions.scanExecutions(null, 16);
    return page.entries().stream()
        .filter(entry -> entry.work().equals(work))
        .findFirst()
        .orElseThrow();
  }

  private static Records.ScopeSummary summary(
      BranchExecutionTest.Fixture fixture, long scope, Records.Digest seal) {
    try {
      return fixture.sessions.scopeSummary(
          new SessionStore.Access("alice", () -> {}),
          capabilities(),
          fixture.generation,
          scope,
          seal);
    } catch (ProtocolError refusal) {
      if (refusal.code() == ProtocolError.Code.NOT_READY) return null;
      throw refusal;
    } catch (Exception failure) {
      throw new AssertionError(failure);
    }
  }

  private static void await(CheckedBoolean condition) throws Exception {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(8);
    while (!condition.getAsBoolean()) {
      if (System.nanoTime() >= deadline) fail("condition not reached before bounded deadline");
      java.util.concurrent.locks.LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
    }
  }

  private static Messages.Capabilities capabilities() {
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

  private static Records.Digest digest(byte[] bytes) throws Exception {
    return new Records.Digest(MessageDigest.getInstance("SHA-256").digest(bytes));
  }

  @FunctionalInterface
  private interface CheckedBoolean {
    boolean getAsBoolean() throws Exception;
  }
}
