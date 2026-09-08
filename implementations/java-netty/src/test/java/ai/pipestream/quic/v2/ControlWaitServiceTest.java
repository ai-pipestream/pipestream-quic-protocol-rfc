package ai.pipestream.quic.v2;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

@Timeout(20)
final class ControlWaitServiceTest {
  @TempDir Path directory;

  @Test
  void watchImmediateTimeoutChangeFutureAndSignedWrapUseFreshAuthorizedViews() throws Exception {
    AtomicLong nanos = new AtomicLong(100);
    try (ResultFixture fixture = new ResultFixture(directory, "watch", new byte[0], false);
        ControlWaitService service = service(fixture.sessions, nanos)) {
      Messages.WatchResponse current = snapshot(fixture, 1);
      try (var immediate =
          service.watch(
              access("alice"),
              ResultFixture.SELECTED,
              fixture.binding.generation(),
              new Messages.Watch(2, ResultFixture.WORK, 0, 1000))) {
        Messages.WatchResponse response = get(immediate);
        assertEquals(2, response.request());
        assertEquals(current.revision(), response.revision());
      }

      CountDownLatch unchangedObserved = new CountDownLatch(1);
      try (var unchanged =
          service.watch(
              observedAccess("alice", unchangedObserved),
              ResultFixture.SELECTED,
              fixture.binding.generation(),
              new Messages.Watch(3, ResultFixture.WORK, current.revision(), 1000))) {
        assertTrue(unchangedObserved.await(5, TimeUnit.SECONDS));
        assertFalse(unchanged.response().toCompletableFuture().isDone());
        nanos.addAndGet(TimeUnit.MILLISECONDS.toNanos(1000));
        Messages.WatchResponse response = get(unchanged);
        assertEquals(3, response.request());
        assertEquals(current.revision(), response.revision());
        assertEquals(current.work(), response.work());
      }

      Messages.WatchResponse beforeFailure = snapshot(fixture, 4);
      CountDownLatch changeObserved = new CountDownLatch(1);
      try (var changed =
          service.watch(
              observedAccess("alice", changeObserved),
              ResultFixture.SELECTED,
              fixture.binding.generation(),
              new Messages.Watch(5, ResultFixture.WORK, beforeFailure.revision(), 5000))) {
        assertTrue(changeObserved.await(5, TimeUnit.SECONDS));
        assertFalse(changed.response().toCompletableFuture().isDone());
        fixture.sessions.failExecution(
            ResultFixture.executionAccess("alice"),
            fixture.lease,
            new Records.Diagnostic(8, "terminal"),
            false,
            ResultFixture.clock(1200),
            ResultFixture.ALLOW_EXECUTION);
        Messages.WatchResponse response = get(changed);
        assertTrue(response.revision() > beforeFailure.revision());
        assertEquals(Records.State.FAILED, response.work().state());
      }

      assertFailure(
          ProtocolError.Code.CONFLICT,
          service.watch(
              access("alice"),
              ResultFixture.SELECTED,
              fixture.binding.generation(),
              new Messages.Watch(6, ResultFixture.WORK, Long.MAX_VALUE, 1000)));
    }

    AtomicLong wrapped = new AtomicLong(Long.MAX_VALUE - TimeUnit.MILLISECONDS.toNanos(400));
    try (ResultFixture fixture = new ResultFixture(directory, "wrap", new byte[] {1});
        ControlWaitService service = service(fixture.sessions, wrapped)) {
      Messages.WatchResponse current = snapshot(fixture, 7);
      try (var wait =
          service.watch(
              access("alice"),
              ResultFixture.SELECTED,
              fixture.binding.generation(),
              new Messages.Watch(8, ResultFixture.WORK, current.revision(), 1000))) {
        wrapped.addAndGet(TimeUnit.MILLISECONDS.toNanos(1000));
        assertEquals(current.revision(), get(wait).revision());
      }
    }
  }

  @Test
  void checkpointImmediateRefusalsTimeoutAndClosureArrivalAreDistinct() throws Exception {
    try (ScopeFixture fixture = new ScopeFixture("checkpoint")) {
      Records.Digest empty = fixture.seal(List.of());
      assertFailure(
          ProtocolError.Code.NOT_READY,
          fixture.service.checkpoint(
              access("alice"),
              ResultFixture.SELECTED,
              fixture.binding.generation(),
              new Messages.Checkpoint(1, 0, empty, 0)));
      fixture.sessions.declare(
          access("alice"),
          ResultFixture.SELECTED,
          fixture.binding.generation(),
          new Messages.Declare(2, ResultFixture.operation(1), 0, List.of(), true));
      assertFailure(
          ProtocolError.Code.INTEGRITY_ERROR,
          fixture.service.checkpoint(
              access("alice"),
              ResultFixture.SELECTED,
              fixture.binding.generation(),
              new Messages.Checkpoint(3, 0, ResultFixture.digest(new byte[] {9}), 1000)));

      try (var timeout =
          fixture.service.checkpoint(
              access("alice"),
              ResultFixture.SELECTED,
              fixture.binding.generation(),
              new Messages.Checkpoint(4, 0, empty, 1000))) {
        fixture.nanos.addAndGet(TimeUnit.MILLISECONDS.toNanos(1000));
        assertFailure(ProtocolError.Code.WAIT_TIMEOUT, timeout);
      }

      try (var closure =
          fixture.service.checkpoint(
              access("alice"),
              ResultFixture.SELECTED,
              fixture.binding.generation(),
              new Messages.Checkpoint(5, 0, empty, 5000))) {
        fixture.closeRoot();
        Messages.CheckpointResponse response = get(closure);
        assertEquals(5, response.request());
        assertEquals(empty, response.summary().seal());
      }
      try (var ready =
          fixture.service.checkpoint(
              access("alice"),
              ResultFixture.SELECTED,
              fixture.binding.generation(),
              new Messages.Checkpoint(6, 0, empty, 0))) {
        assertEquals(empty, get(ready).summary().seal());
      }
    }
  }

  @Test
  void elapsedCheckpointCannotPublishLateReadyClosure() throws Exception {
    try (ScopeFixture fixture = new ScopeFixture("late-ready")) {
      Records.Digest empty = fixture.seal(List.of());
      fixture.sessions.declare(
          access("alice"),
          ResultFixture.SELECTED,
          fixture.binding.generation(),
          new Messages.Declare(1, ResultFixture.operation(1), 0, List.of(), true));
      fixture.closeRoot();
      CountDownLatch entered = new CountDownLatch(1);
      CountDownLatch release = new CountDownLatch(1);
      SessionStore.Access gated = gatedAccess("alice", entered, release, new AtomicBoolean());
      ControlWaitService.Wait<Messages.CheckpointResponse> wait =
          fixture.service.checkpoint(
              gated,
              ResultFixture.SELECTED,
              fixture.binding.generation(),
              new Messages.Checkpoint(2, 0, empty, 1000));
      try (wait) {
        assertTrue(entered.await(5, TimeUnit.SECONDS));
        fixture.nanos.addAndGet(TimeUnit.MILLISECONDS.toNanos(1000));
      } finally {
        release.countDown();
      }
      assertFailure(ProtocolError.Code.CANCELLED, wait);

      CountDownLatch secondEntered = new CountDownLatch(1);
      CountDownLatch secondRelease = new CountDownLatch(1);
      var late =
          fixture.service.checkpoint(
              gatedAccess("alice", 3, secondEntered, secondRelease, new AtomicBoolean()),
              ResultFixture.SELECTED,
              fixture.binding.generation(),
              new Messages.Checkpoint(3, 0, empty, 1000));
      try (late) {
        assertTrue(secondEntered.await(5, TimeUnit.SECONDS));
        fixture.nanos.addAndGet(TimeUnit.MILLISECONDS.toNanos(1000));
        secondRelease.countDown();
        assertFailure(ProtocolError.Code.WAIT_TIMEOUT, late);
      } finally {
        secondRelease.countDown();
      }
    }
  }

  @Test
  void waitingRechecksCurrentAuthorizationInsteadOfReturningCachedTimeout() throws Exception {
    AtomicLong nanos = new AtomicLong();
    AtomicBoolean revoked = new AtomicBoolean();
    CountDownLatch observed = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    try (ResultFixture fixture = new ResultFixture(directory, "revoked", new byte[] {1});
        ControlWaitService service = service(fixture.sessions, nanos)) {
      Messages.WatchResponse current = snapshot(fixture, 1);
      SessionStore.Access access = gatedAccess("alice", 4, observed, release, revoked);
      try (var wait =
          service.watch(
              access,
              ResultFixture.SELECTED,
              fixture.binding.generation(),
              new Messages.Watch(2, ResultFixture.WORK, current.revision(), 1000))) {
        assertTrue(observed.await(5, TimeUnit.SECONDS));
        assertFalse(wait.response().toCompletableFuture().isDone());
        revoked.set(true);
        nanos.addAndGet(TimeUnit.MILLISECONDS.toNanos(1000));
        release.countDown();
        assertFailure(ProtocolError.Code.UNAUTHORIZED, wait);
      } finally {
        release.countDown();
      }
    }
  }

  @Test
  void capacityIsGlobalAndPerOwnerAndCloseOfRunningReadRemainsCharged() throws Exception {
    AtomicLong nanos = new AtomicLong();
    CountDownLatch entered = new CountDownLatch(2);
    CountDownLatch release = new CountDownLatch(1);
    try (ResultFixture fixture = new ResultFixture(directory, "capacity", new byte[] {1});
        ControlWaitService service =
            new ControlWaitService(
                fixture.sessions, new ControlWaitService.Limits(2, 1, 2, 1000), nanos::get)) {
      Messages.Binding bobBinding =
          fixture.sessions.create(
              access("bob"),
              ResultFixture.SELECTED,
              new Messages.Create(20, 1, new Records.Policy(10_000, 20_000, 30_000)));
      fixture.sessions.declare(
          access("bob"),
          ResultFixture.SELECTED,
          bobBinding.generation(),
          new Messages.Declare(21, ResultFixture.operation(20), 0, List.of(1L), false));
      var alice =
          service.watch(
              gatedAccess("alice", entered, release, new AtomicBoolean()),
              ResultFixture.SELECTED,
              fixture.binding.generation(),
              new Messages.Watch(1, ResultFixture.WORK, 1, 5000));
      assertCode(
          ProtocolError.Code.LIMIT_EXCEEDED,
          () ->
              service.watch(
                  access("alice"),
                  ResultFixture.SELECTED,
                  fixture.binding.generation(),
                  new Messages.Watch(3, ResultFixture.WORK, 1, 5000)));
      var bob =
          service.watch(
              gatedAccess("bob", entered, release, new AtomicBoolean()),
              ResultFixture.SELECTED,
              bobBinding.generation(),
              new Messages.Watch(2, ResultFixture.WORK, 1, 5000));
      try {
        assertTrue(entered.await(5, TimeUnit.SECONDS));
        assertEquals(new ControlWaitService.Usage(2, 2), service.usage());
        assertCode(
            ProtocolError.Code.LIMIT_EXCEEDED,
            () ->
                service.watch(
                    access("carol"),
                    ResultFixture.SELECTED,
                    fixture.binding.generation(),
                    new Messages.Watch(4, ResultFixture.WORK, 1, 5000)));
        alice.close();
        assertFailure(ProtocolError.Code.CANCELLED, alice);
        assertEquals(new ControlWaitService.Usage(2, 2), service.usage());
      } finally {
        bob.close();
        release.countDown();
      }
      awaitUsage(service, new ControlWaitService.Usage(0, 0));
    }
  }

  private ControlWaitService service(SessionStore sessions, AtomicLong nanos) {
    return new ControlWaitService(sessions, new ControlWaitService.Limits(8, 4, 2, 5), nanos::get);
  }

  private static Messages.WatchResponse snapshot(ResultFixture fixture, long request)
      throws Exception {
    return fixture.sessions.snapshot(
        access("alice"),
        ResultFixture.SELECTED,
        fixture.binding.generation(),
        new Messages.Watch(request, ResultFixture.WORK, 0, 0));
  }

  private static SessionStore.Access access(String owner) {
    return new SessionStore.Access(owner, () -> {});
  }

  private static SessionStore.Access gatedAccess(
      String owner, CountDownLatch entered, CountDownLatch release, AtomicBoolean denied) {
    return gatedAccess(owner, 1, entered, release, denied);
  }

  private static SessionStore.Access gatedAccess(
      String owner,
      int targetCheck,
      CountDownLatch entered,
      CountDownLatch release,
      AtomicBoolean denied) {
    AtomicInteger checks = new AtomicInteger();
    return new SessionStore.Access(
        owner,
        () -> {
          if (!Thread.currentThread().getName().startsWith("pipestream-v2-observe-")) return;
          if (checks.incrementAndGet() != targetCheck) return;
          entered.countDown();
          try {
            if (!release.await(5, TimeUnit.SECONDS)) throw new AssertionError("gate timed out");
          } catch (InterruptedException failure) {
            Thread.currentThread().interrupt();
            throw new AssertionError(failure);
          }
          if (denied.get())
            throw new ProtocolError(ProtocolError.Code.UNAUTHORIZED, "worker access revoked");
        });
  }

  private static SessionStore.Access observedAccess(String owner, CountDownLatch observed) {
    AtomicInteger checks = new AtomicInteger();
    return new SessionStore.Access(
        owner,
        () -> {
          if (Thread.currentThread().getName().startsWith("pipestream-v2-observe-")
              && checks.incrementAndGet() == 4) observed.countDown();
        });
  }

  private static <T extends Messages.Message> T get(ControlWaitService.Wait<T> wait)
      throws Exception {
    return wait.response().toCompletableFuture().get(5, TimeUnit.SECONDS);
  }

  private static void assertFailure(ProtocolError.Code expected, ControlWaitService.Wait<?> wait) {
    ExecutionException wrapper =
        assertThrows(
            ExecutionException.class,
            () -> wait.response().toCompletableFuture().get(5, TimeUnit.SECONDS));
    ProtocolError failure = assertInstanceOf(ProtocolError.class, wrapper.getCause());
    assertEquals(expected, failure.code(), failure::getMessage);
  }

  private static void assertCode(ProtocolError.Code expected, Throwing action) {
    ProtocolError failure = assertThrows(ProtocolError.class, action::run);
    assertEquals(expected, failure.code(), failure::getMessage);
  }

  private static void awaitUsage(ControlWaitService service, ControlWaitService.Usage expected)
      throws Exception {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    while (!expected.equals(service.usage()) && System.nanoTime() - deadline < 0) {
      LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
    }
    assertEquals(expected, service.usage());
  }

  private final class ScopeFixture implements AutoCloseable {
    final SessionStore sessions;
    final Messages.Binding binding;
    final AtomicLong nanos = new AtomicLong();
    final ControlWaitService service;

    ScopeFixture(String name) throws Exception {
      sessions =
          SessionStore.initialize(
              directory.resolve(name + ".sqlite"), ResultFixture.configuration());
      binding =
          sessions.create(
              access("alice"),
              ResultFixture.SELECTED,
              new Messages.Create(90, 1, new Records.Policy(10_000, 20_000, 30_000)));
      service = service(sessions, nanos);
    }

    Records.Digest seal(List<Long> members) {
      Commitments.Seal seal =
          new Commitments.Seal(
              new Commitments.Context(binding.authority(), binding.owner(), binding.generation()),
              0,
              0,
              null,
              members.size());
      for (long member : members) seal.add(member);
      return seal.finish();
    }

    void closeRoot() throws Exception {
      ClosureStore.Cursor cursor = new ClosureStore.Cursor();
      for (int count = 0; count < 8; count++) {
        sessions.reconcileClosures(cursor, 1, ResultFixture.clock(1000));
        if (sessions
            .checkpoint(
                access("alice"),
                ResultFixture.SELECTED,
                binding.generation(),
                new Messages.Checkpoint(100 + count, 0, seal(List.of()), 0))
            .isPresent()) return;
      }
      fail("root did not close within bounded reconciliation");
    }

    @Override
    public void close() {
      service.close();
    }
  }

  @FunctionalInterface
  private interface Throwing {
    void run() throws Exception;
  }
}
