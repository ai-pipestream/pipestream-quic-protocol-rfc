package ai.pipestream.quic.v2;

import static ai.pipestream.quic.v2.Messages.DURABLE_WORK;
import static ai.pipestream.quic.v2.Messages.RESULT_DELIVERY;
import static org.junit.jupiter.api.Assertions.*;

import ai.pipestream.quic.BoundedSqlite;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

@Timeout(30)
final class ProducedInputOrphanRaceTest {
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
          10_000);
  private static final InputStore.Limits INPUT_LIMITS =
      new InputStore.Limits(16L << 20, 128, 1 << 20, 3);
  private static final AdmissionStore.Authorization ALLOW = (binding, parameters) -> {};
  private static final PublicationStore.Endpoint ENDPOINT =
      new PublicationStore.Endpoint("results.example:7443");

  @TempDir Path directory;

  @Test
  void completedProducedInputCannotBeReapedBetweenFinAndAdmission() throws Exception {
    CountDownLatch admissionGate = new CountDownLatch(1);
    CountDownLatch releaseAdmission = new CountDownLatch(1);
    AtomicBoolean afterObjectSync = new AtomicBoolean();
    AtomicReference<Throwable> runtimeFailure = new AtomicReference<>();
    try (Fixture fixture = new Fixture(directory)) {
      fixture.prepare();
      Set<String> originalObjects = names(fixture.inputPath.resolve("objects"));
      fixture.inputs.close();
      fixture.inputs =
          InputStore.open(
              fixture.inputPath,
              INPUT_LIMITS,
              phase -> {
                if (phase == InputStore.Phase.OBJECT_SYNCED) afterObjectSync.set(true);
              });
      fixture.sessions.verifyInputs(fixture.inputs);
      fixture.declareChild();
      Records.InputHeader childHeader = fixture.childHeader();
      ExecutionStore.Access gatedAccess =
          new ExecutionStore.Access(
              "alice",
              () -> {
                if (afterObjectSync.compareAndSet(true, false)) {
                  admissionGate.countDown();
                  try {
                    if (!releaseAdmission.await(5, TimeUnit.SECONDS))
                      throw new IllegalStateException("test admission handoff was not released");
                  } catch (InterruptedException failure) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("test admission handoff interrupted", failure);
                  }
                }
              });

      Thread runtimeThread =
          new Thread(
              () -> {
                try {
                  Records.WorkView view =
                      fixture
                          .runtime(
                              context -> ExecutionRuntime.Outcome.succeeded(),
                              context -> {
                                assertTrue(
                                    context
                                        .beginInput(
                                            childHeader.operation(), childHeader.parameters())
                                        .isEmpty());
                                context.writeInput(ByteBuffer.wrap(new byte[] {4, 5, 6}));
                                Records.OperationReceipt receipt = context.finishInput();
                                assertEquals(childHeader.operation(), receipt.operation());
                                return ExecutionRuntime.ExpansionOutcome.complete();
                              })
                          .run(gatedAccess, fixture.binding.generation(), fixture.parent);
                  assertEquals(Records.State.WAITING_CHILDREN, view.state());
                } catch (Throwable failure) {
                  runtimeFailure.set(failure);
                }
              },
              "produced-input-runtime");
      runtimeThread.start();
      ExecutorService executor = null;
      try {
        assertTrue(admissionGate.await(5, TimeUnit.SECONDS));
        Set<String> linked = names(fixture.inputPath.resolve("objects"));
        linked.removeAll(originalObjects);
        assertEquals(1, linked.size());
        InputStore.OrphanCandidate candidate =
            new InputStore.OrphanCandidate(
                fixture.inputs.identity(),
                false,
                fixture.context(),
                childHeader,
                linked.iterator().next());
        CountDownLatch reaperEntered = new CountDownLatch(1);
        AtomicReference<Thread> reaperThread = new AtomicReference<>();
        executor = Executors.newSingleThreadExecutor();
        Future<OrphanStore.Result> reaped =
            executor.submit(
                () -> {
                  reaperThread.set(Thread.currentThread());
                  reaperEntered.countDown();
                  return fixture.sessions.reclaimOrphan(fixture.inputs, candidate, clock(1000));
                });
        assertTrue(reaperEntered.await(5, TimeUnit.SECONDS));
        awaitBlocked(reaperThread.get());
        assertFalse(reaped.isDone());
        releaseAdmission.countDown();
        runtimeThread.join(TimeUnit.SECONDS.toMillis(5));
        assertFalse(runtimeThread.isAlive());
        if (runtimeFailure.get() != null) throw new AssertionError(runtimeFailure.get());
        assertEquals(OrphanStore.Result.RETAINED, reaped.get(5, TimeUnit.SECONDS));
      } finally {
        releaseAdmission.countDown();
        runtimeThread.join(TimeUnit.SECONDS.toMillis(5));
        if (executor != null) {
          executor.shutdownNow();
          assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        }
      }

      Records.WorkKey child = childHeader.parameters().work();
      Records.WorkView childView = fixture.view(child);
      assertEquals(Records.State.ACTIVE, childView.state());
      assertEquals(childHeader.parameters().input(), childView.input());
      InputStore.Stored retained =
          fixture.inputs.find(fixture.context(), childHeader).orElseThrow();
      try (var stream = retained.openStream()) {
        assertArrayEquals(new byte[] {4, 5, 6}, stream.readAllBytes());
      }
      assertEquals(0, fixture.inputs.usage().handles());
    }
  }

  private static void awaitBlocked(Thread thread) {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    while (thread.getState() != Thread.State.BLOCKED) {
      if (System.nanoTime() >= deadline)
        fail("orphan reaper did not block on the input-store handoff monitor");
      java.util.concurrent.locks.LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
    }
  }

  private static Set<String> names(Path path) throws Exception {
    Set<String> names = new HashSet<>();
    try (var entries = Files.newDirectoryStream(path)) {
      for (Path entry : entries) assertTrue(names.add(entry.getFileName().toString()));
    }
    return names;
  }

  private static final class Fixture implements AutoCloseable {
    final Path inputPath;
    final AdmissionStore.Application application =
        new AdmissionStore.Application(
            "expand", Set.of(0, 2), AdmissionStore.RestartSafety.IDEMPOTENT);
    final Records.WorkKey parent = new Records.WorkKey(0, 0, 1);
    final SessionStore sessions;
    final Messages.Binding binding;
    InputStore inputs;
    Records.ChildScope child;
    int request = 10;

    Fixture(Path directory) throws Exception {
      Path database = directory.resolve("race.sqlite");
      inputPath = directory.resolve("race-inputs");
      sessions = SessionStore.initialize(database, configuration(application));
      binding =
          sessions.create(
              access(),
              SELECTED,
              new Messages.Create(1, 1, new Records.Policy(10_000, 20_000, 30_000)));
      inputs = InputStore.initializeForAuthority(inputPath, INPUT_LIMITS, sessions.identity());
      sessions.bindInputs(inputs);
    }

    void prepare() throws Exception {
      sessions.declare(
          access(),
          SELECTED,
          binding.generation(),
          new Messages.Declare(request++, operation(request++), 0, List.of(1L), true));
      Records.InputHeader header =
          new Records.InputHeader(
              binding.generation(),
              operation(request++),
              new Records.AdmitParameters(
                  parent,
                  new Records.Input(0, digest(new byte[0]), "application/octet-stream"),
                  "expand",
                  2,
                  5000,
                  new Records.OutputBudget(0, 0)));
      try (InputStore.Receiver receiver = inputs.begin(context(), header, SELECTED, 1000)) {
        receiver.finish(1000);
      }
      sessions.admit(
          access(), SELECTED, binding.generation(), inputs, header, request++, clock(1000), ALLOW);
      child = view(parent).child();
      assertNotNull(child);
    }

    void declareChild() throws Exception {
      ExecutionStore.Lease lease =
          sessions.claimExecution(
              executionAccess(), binding.generation(), parent, inputs, 5000, clock(1000), ALLOW);
      sessions.declareProduced(
          executionAccess(),
          lease,
          SELECTED,
          inputs,
          new Messages.Declare(request++, operation(30), child.scope(), List.of(10L), true),
          clock(1000),
          ALLOW);
      sessions.finishExpansion(executionAccess(), lease, false, clock(1000), ALLOW);
    }

    Records.InputHeader childHeader() throws Exception {
      byte[] payload = {4, 5, 6};
      return new Records.InputHeader(
          binding.generation(),
          operation(31),
          new Records.AdmitParameters(
              new Records.WorkKey(child.scope(), child.producer(), 10),
              new Records.Input(payload.length, digest(payload), "application/octet-stream"),
              "expand",
              0,
              5000,
              new Records.OutputBudget(0, 0)));
    }

    ExecutionRuntime runtime(
        ExecutionRuntime.Callback callback, ExecutionRuntime.Expander expander) {
      return new ExecutionRuntime(
          sessions,
          inputs,
          List.of(new ExecutionRuntime.Registration(application, callback, expander)),
          ENDPOINT,
          clock(1000),
          ALLOW,
          new ExecutionRuntime.Limits(1, 1, 5000, 128),
          SELECTED);
    }

    Commitments.Context context() {
      return new Commitments.Context(binding.authority(), binding.owner(), binding.generation());
    }

    Records.WorkView view(Records.WorkKey work) throws Exception {
      return sessions
          .snapshot(
              access(), SELECTED, binding.generation(), new Messages.Watch(request++, work, 0, 0))
          .work();
    }

    @Override
    public void close() throws java.io.IOException {
      inputs.close();
    }
  }

  private static SessionStore.Configuration configuration(AdmissionStore.Application application) {
    return new SessionStore.Configuration(
        "issuer-a",
        new Records.Limits(16, 64, 64, 1 << 20, 1 << 20, 8),
        new Records.Policy(60_000, 60_000, 60_000),
        4,
        16,
        4,
        BoundedSqlite.Limits.defaults(),
        new AdmissionStore.ExecutionPolicy(List.of(application), 8, 8));
  }

  private static Records.Digest digest(byte[] bytes) throws Exception {
    return new Records.Digest(MessageDigest.getInstance("SHA-256").digest(bytes));
  }

  private static Records.OperationId operation(int value) {
    byte[] bytes = new byte[16];
    bytes[15] = (byte) value;
    return new Records.OperationId(bytes);
  }

  private static AdmissionStore.Clock clock(long utc) {
    return () -> new AdmissionStore.Time(utc, true);
  }

  private static SessionStore.Access access() {
    return new SessionStore.Access("alice", () -> {});
  }

  private static ExecutionStore.Access executionAccess() {
    return new ExecutionStore.Access("alice", () -> {});
  }
}
