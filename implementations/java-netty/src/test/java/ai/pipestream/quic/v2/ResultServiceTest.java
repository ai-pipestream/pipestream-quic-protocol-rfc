package ai.pipestream.quic.v2;

import static ai.pipestream.quic.v2.Messages.DURABLE_WORK;
import static ai.pipestream.quic.v2.Messages.RESULT_DELIVERY;
import static org.junit.jupiter.api.Assertions.*;

import ai.pipestream.quic.BoundedSqlite;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

@Timeout(30)
final class ResultServiceTest {
  private static final byte[] OUTPUT = {1, 2, 3, 4};
  private static final Messages.Capabilities SELECTED =
      new Messages.Capabilities(
          true,
          List.of(DURABLE_WORK, RESULT_DELIVERY),
          List.of(),
          1 << 20,
          8,
          16,
          1 << 20,
          1000,
          3000);
  private static final InputStore.Limits INPUT_LIMITS =
      new InputStore.Limits(16L << 20, 128, 1 << 20, 16);
  private static final AdmissionStore.Authorization EXECUTE = (binding, parameters) -> {};
  private static final ResultStore.Authorization READ = (binding, work) -> {};
  private static final PublicationStore.Endpoint ENDPOINT =
      new PublicationStore.Endpoint("results.example:7443");
  private static final Records.WorkKey WORK = new Records.WorkKey(0, 0, 1);

  @TempDir Path directory;

  @Test
  void acceptedTransportProgressControlsIdleButCannotExtendAbsoluteLifetime() throws Exception {
    try (Fixture fixture = new Fixture("progress", List.of("alice"))) {
      AtomicLong nanos = new AtomicLong();
      try (ResultService service =
          new ResultService(
              fixture.sessions,
              fixture.inputs,
              new ResultService.Limits(2, 2, 2, 1000),
              nanos::get)) {
        Published alice = fixture.published("alice");
        try (ResultService.Read read = fixture.begin(service, alice, 10, READ)) {
          assertEquals(new ResultService.Usage(1, 1), service.usage());
          Records.ResultHeader header = read.start();
          assertEquals(OUTPUT.length, header.length());
          assertEquals(alice.digest, header.sha256());
          byte[] bytes = new byte[2];
          assertEquals(2, read.read(bytes, 0, 2));
          nanos.set(tick(5));
          read.check();
          read.sent(1);
          nanos.set(tick(9));
          read.sent(1);
          assertEquals(2, read.read(bytes, 0, 2));
          nanos.set(tick(18));
          read.sent(2);
          assertEquals(-1, read.read(bytes, 0, 2));
          read.check();
          read.finish();
        }
        assertEquals(new ResultService.Usage(0, 0), service.usage());

        nanos.set(0);
        ResultService.Read lifetime = fixture.begin(service, alice, 11, READ);
        lifetime.start();
        byte[] one = new byte[1];
        for (long time : List.of(5L, 14L, 23L, 29L)) {
          nanos.set(tick(time));
          assertEquals(1, lifetime.read(one, 0, 1));
          lifetime.sent(1);
        }
        nanos.set(tick(30));
        assertCode(ProtocolError.Code.LIMIT_EXCEEDED, lifetime::check);
        assertEquals(new ResultService.Usage(0, 0), service.usage());
      }
    }
  }

  @Test
  void noProgressMisuseAndExactIdleBoundaryCloseWithoutLeakingQuota() throws Exception {
    try (Fixture fixture = new Fixture("idle", List.of("alice"))) {
      AtomicLong nanos = new AtomicLong();
      try (ResultService service =
          new ResultService(
              fixture.sessions,
              fixture.inputs,
              new ResultService.Limits(1, 1, 2, 1000),
              nanos::get)) {
        Published alice = fixture.published("alice");
        ResultService.Read idle = fixture.begin(service, alice, 20, READ);
        idle.start();
        assertEquals(2, idle.read(new byte[2], 0, 2));
        nanos.set(tick(9));
        idle.sent(0);
        nanos.set(tick(10));
        assertCode(ProtocolError.Code.LIMIT_EXCEEDED, idle::check);
        assertEquals(new ResultService.Usage(0, 0), service.usage());

        ResultService.Read oversized = fixture.begin(service, alice, 21, READ);
        oversized.start();
        assertCode(ProtocolError.Code.LIMIT_EXCEEDED, () -> oversized.read(new byte[3], 0, 3));
        assertEquals(new ResultService.Usage(0, 0), service.usage());

        ResultService.Read zero = fixture.begin(service, alice, 22, READ);
        zero.start();
        assertCode(ProtocolError.Code.LIMIT_EXCEEDED, () -> zero.read(new byte[1], 0, 0));

        ResultService.Read duplicateStart = fixture.begin(service, alice, 23, READ);
        duplicateStart.start();
        assertCode(ProtocolError.Code.CONFLICT, duplicateStart::start);

        ResultService.Read outstanding = fixture.begin(service, alice, 24, READ);
        outstanding.start();
        assertEquals(2, outstanding.read(new byte[2], 0, 2));
        assertCode(ProtocolError.Code.CONFLICT, () -> outstanding.read(new byte[2], 0, 2));

        ResultService.Read invalid = fixture.begin(service, alice, 25, READ);
        invalid.start();
        assertEquals(2, invalid.read(new byte[2], 0, 2));
        assertCode(ProtocolError.Code.INTEGRITY_ERROR, () -> invalid.sent(3));
        invalid.close();
        assertEquals(new ResultService.Usage(0, 0), service.usage());

        ResultService.Read unfinished = fixture.begin(service, alice, 26, READ);
        unfinished.start();
        assertCode(ProtocolError.Code.INTEGRITY_ERROR, unfinished::finish);
        assertEquals(new ResultService.Usage(0, 0), service.usage());
      }
    }
  }

  @Test
  void globalPerOwnerAndExclusiveStoreLimitsReleaseOnlyAfterPhysicalClose() throws Exception {
    try (Fixture fixture = new Fixture("quotas", List.of("alice", "bob", "carol"))) {
      long baseline = fixture.inputs.usage().handles();
      try (ResultService service =
          new ResultService(
              fixture.sessions,
              fixture.inputs,
              new ResultService.Limits(2, 1, 4, 1000),
              new AtomicLong()::get)) {
        assertThrows(
            IllegalStateException.class,
            () ->
                new ResultService(
                    fixture.sessions, fixture.inputs, new ResultService.Limits(1, 1, 4, 1000)));
        ResultService.Read alice = fixture.begin(service, fixture.published("alice"), 30, READ);
        assertCode(
            ProtocolError.Code.LIMIT_EXCEEDED,
            () -> fixture.begin(service, fixture.published("alice"), 31, READ));
        ResultService.Read bob = fixture.begin(service, fixture.published("bob"), 32, READ);
        assertEquals(new ResultService.Usage(2, 2), service.usage());
        assertEquals(baseline + 2, fixture.inputs.usage().handles());
        assertCode(
            ProtocolError.Code.LIMIT_EXCEEDED,
            () -> fixture.begin(service, fixture.published("carol"), 33, READ));
        alice.close();
        assertEquals(new ResultService.Usage(1, 1), service.usage());
        try (ResultService.Read carol =
            fixture.begin(service, fixture.published("carol"), 34, READ)) {
          carol.check();
          assertEquals(new ResultService.Usage(2, 2), service.usage());
        }
        bob.close();
        assertEquals(baseline, fixture.inputs.usage().handles());
      }

      ResultService replacement =
          new ResultService(
              fixture.sessions, fixture.inputs, new ResultService.Limits(1, 1, 4, 1000));
      boolean replacementClosed = false;
      try {
        for (int request = 40; request < 43; request++) {
          try (ResultService.Read replay =
              fixture.begin(replacement, fixture.published("alice"), request, READ)) {
            replay.start();
            byte[] bytes = new byte[4];
            assertEquals(4, replay.read(bytes, 0, 4));
            assertArrayEquals(OUTPUT, bytes);
            replay.sent(4);
            assertEquals(-1, replay.read(bytes, 0, 4));
            replay.finish();
          }
        }
        try (ResultService.Read shutdown =
            fixture.begin(replacement, fixture.published("alice"), 43, READ)) {
          replacement.close();
          replacementClosed = true;
          assertEquals(new ResultService.Usage(0, 0), replacement.usage());
          assertCode(ProtocolError.Code.CANCELLED, shutdown::start);
        }
      } finally {
        if (!replacementClosed) replacement.close();
      }
    }
  }

  @Test
  void acquisitionTimeStoragePressureAndUtcExpiryNeverLeakDeliveryCapacity() throws Exception {
    try (Fixture fixture = new Fixture("acquisition", List.of("alice"))) {
      AtomicLong nanos = new AtomicLong();
      try (ResultService service =
          new ResultService(
              fixture.sessions,
              fixture.inputs,
              new ResultService.Limits(3, 3, 4, 1000),
              nanos::get)) {
        Published alice = fixture.published("alice");
        java.util.concurrent.atomic.AtomicInteger checks =
            new java.util.concurrent.atomic.AtomicInteger();
        ResultStore.Authorization slowFinal =
            (binding, work) -> {
              if (checks.incrementAndGet() == 2) nanos.set(tick(30));
            };
        assertCode(
            ProtocolError.Code.LIMIT_EXCEEDED,
            () -> fixture.begin(service, alice, 60, clock(1600), slowFinal));
        assertEquals(2, checks.get());
        assertEquals(new ResultService.Usage(0, 0), service.usage());

        nanos.set(0);
        ResultService.Read retained = fixture.begin(service, alice, 61, READ);
        assertCode(
            ProtocolError.Code.EXPIRED,
            () -> fixture.begin(service, alice, 62, clock(21_500), READ));
        assertCode(
            ProtocolError.Code.CLOCK_UNSAFE,
            () -> fixture.begin(service, alice, 63, () -> new AdmissionStore.Time(0, false), READ));
        retained.check();
        retained.close();

        List<InputStream> held = new ArrayList<>();
        try {
          InputStore.Stored input =
              fixture
                  .inputs
                  .find(context("alice", alice.generation), header(alice.generation))
                  .orElseThrow();
          for (int count = 0; count < INPUT_LIMITS.handles(); count++) {
            held.add(input.openStream());
          }
          assertCode(ProtocolError.Code.LIMIT_EXCEEDED, input::openStream);
          assertCode(
              ProtocolError.Code.LIMIT_EXCEEDED, () -> fixture.begin(service, alice, 64, READ));
          assertEquals(new ResultService.Usage(0, 0), service.usage());
        } finally {
          for (InputStream stream : held) stream.close();
        }
        try (ResultService.Read recovered = fixture.begin(service, alice, 65, READ)) {
          assertEquals(OUTPUT.length, recovered.start().length());
        }
      }
    }
  }

  @Test
  void maintenanceExpiresRevokedAndBusyReadsAndNanoTimeWrapIsSafe() throws Exception {
    try (Fixture fixture = new Fixture("maintenance", List.of("alice", "bob"))) {
      AtomicLong nanos = new AtomicLong(Long.MAX_VALUE - tick(5));
      try (ResultService service =
          new ResultService(
              fixture.sessions,
              fixture.inputs,
              new ResultService.Limits(2, 2, 4, 1000),
              nanos::get)) {
        ResultService.Read wrapped = fixture.begin(service, fixture.published("alice"), 50, READ);
        wrapped.start();
        nanos.set(nanos.get() + tick(6));
        wrapped.check();
        nanos.decrementAndGet();
        assertCode(ProtocolError.Code.CLOCK_UNSAFE, wrapped::check);
      }

      nanos.set(0);
      try (ResultService service =
          new ResultService(
              fixture.sessions, fixture.inputs, new ResultService.Limits(2, 2, 4, 1), nanos::get)) {
        AtomicBoolean armed = new AtomicBoolean();
        AtomicReference<Thread> designated = new AtomicReference<>();
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        ResultStore.Authorization gated =
            (binding, work) -> {
              if (armed.get() && Thread.currentThread() == designated.get()) {
                entered.countDown();
                try {
                  assertTrue(release.await(5, TimeUnit.SECONDS));
                } catch (InterruptedException interrupted) {
                  Thread.currentThread().interrupt();
                  throw new AssertionError(interrupted);
                }
              }
            };
        ResultService.Read busy = fixture.begin(service, fixture.published("bob"), 51, gated);
        armed.set(true);
        AtomicReference<Throwable> workerFailure = new AtomicReference<>();
        Thread worker =
            Thread.ofVirtual()
                .start(
                    () -> {
                      try {
                        designated.set(Thread.currentThread());
                        busy.check();
                      } catch (Throwable failure) {
                        workerFailure.set(failure);
                      }
                    });
        try {
          assertTrue(entered.await(5, TimeUnit.SECONDS));
          ResultService.Maintenance maintenance = service.maintain();
          assertEquals(1, maintenance.busy());
          assertEquals(0, maintenance.closed());
          assertEquals(new ResultService.Usage(1, 1), service.usage());
          assertEquals(1, fixture.inputs.usage().handles());
        } finally {
          armed.set(false);
          release.countDown();
          worker.join(5000);
        }
        assertFalse(worker.isAlive());
        assertNull(workerFailure.get());
        busy.close();

        ResultService.Read revoked = fixture.begin(service, fixture.published("alice"), 52, READ);
        fixture.sessions.revoke(
            access("alice"), fixture.published("alice").generation, clock(1700));
        assertCode(ProtocolError.Code.UNAUTHORIZED, revoked::check);
        assertEquals(new ResultService.Usage(0, 0), service.usage());

        ResultService.Read timed =
            fixture.begin(service, fixture.published("bob"), 53, clock(1700), READ);
        nanos.set(tick(10));
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (service.usage().reads() != 0 && System.nanoTime() < deadline) {
          Thread.sleep(2);
        }
        assertEquals(new ResultService.Usage(0, 0), service.usage());
        timed.close();
      }
    }
  }

  private final class Fixture implements AutoCloseable {
    final Path database;
    final Path inputPath;
    final SessionStore sessions;
    final InputStore inputs;
    final java.util.Map<String, Published> published = new java.util.HashMap<>();

    Fixture(String name, List<String> owners) throws Exception {
      database = directory.resolve(name + ".sqlite");
      inputPath = directory.resolve(name + "-inputs");
      sessions = SessionStore.initialize(database, configuration());
      Messages.Binding first = create(owners.get(0));
      inputs = InputStore.initializeForAuthority(inputPath, INPUT_LIMITS, sessions.identity());
      sessions.bindInputs(inputs);
      publish(first);
      for (int index = 1; index < owners.size(); index++) publish(create(owners.get(index)));
    }

    private Messages.Binding create(String owner) throws Exception {
      return sessions.create(
          access(owner),
          SELECTED,
          new Messages.Create(1, 1, new Records.Policy(10_000, 20_000, 30_000)));
    }

    private void publish(Messages.Binding binding) throws Exception {
      String owner = binding.owner();
      long generation = binding.generation();
      sessions.declare(
          access(owner),
          SELECTED,
          generation,
          new Messages.Declare(2, operation(1), 0, List.of(1L), false));
      Records.InputHeader header = header(generation);
      Commitments.Context context = context(owner, generation);
      try (InputStore.Receiver receiver = inputs.begin(context, header, SELECTED, 1)) {
        receiver.write(ByteBuffer.allocate(0), 2);
        receiver.finish(3);
      }
      sessions.admit(access(owner), SELECTED, generation, inputs, header, 2, clock(1500), EXECUTE);
      ExecutionStore.Lease lease =
          sessions.claimExecution(
              execAccess(owner), generation, WORK, inputs, 500, clock(1500), EXECUTE);
      try (OutputStore.Writer writer =
          inputs.beginOutput(
              context,
              header,
              lease,
              0,
              OUTPUT.length,
              "application/octet-stream",
              OUTPUT.length)) {
        writer.write(ByteBuffer.wrap(OUTPUT));
        writer.finish();
      }
      Records.WorkView view =
          sessions.succeedExecution(
              execAccess(owner), lease, inputs, 1, ENDPOINT, clock(1500), EXECUTE);
      Records.Output output = view.manifest().outputs().get(0);
      published.put(owner, new Published(owner, generation, output.sha256()));
    }

    Published published(String owner) {
      return published.get(owner);
    }

    ResultService.Read begin(
        ResultService service,
        Published result,
        int request,
        ResultStore.Authorization authorization)
        throws Exception {
      return begin(service, result, request, clock(1600), authorization);
    }

    ResultService.Read begin(
        ResultService service,
        Published result,
        int request,
        AdmissionStore.Clock utc,
        ResultStore.Authorization authorization)
        throws Exception {
      return service.begin(
          access(result.owner),
          SELECTED,
          result.generation,
          new Messages.Read(request, WORK, 1, 0, result.digest),
          utc,
          authorization);
    }

    @Override
    public void close() throws IOException {
      inputs.close();
    }
  }

  private static SessionStore.Configuration configuration() {
    return new SessionStore.Configuration(
        "issuer-a",
        new Records.Limits(8, 16, 16, 1 << 20, 1 << 20, 4),
        new Records.Policy(60_000, 60_000, 60_000),
        4,
        8,
        4,
        BoundedSqlite.Limits.defaults(),
        new AdmissionStore.ExecutionPolicy(
            List.of(
                new AdmissionStore.Application(
                    "copy", Set.of(0), AdmissionStore.RestartSafety.IDEMPOTENT)),
            8,
            8));
  }

  private static Records.InputHeader header(long generation) throws Exception {
    return new Records.InputHeader(
        generation,
        operation(2),
        new Records.AdmitParameters(
            WORK,
            new Records.Input(0, digest(new byte[0]), "application/octet-stream"),
            "copy",
            0,
            1000,
            new Records.OutputBudget(1, OUTPUT.length)));
  }

  private static Records.Digest digest(byte[] bytes) throws Exception {
    return new Records.Digest(MessageDigest.getInstance("SHA-256").digest(bytes));
  }

  private static Records.OperationId operation(int value) {
    byte[] bytes = new byte[16];
    bytes[15] = (byte) value;
    return new Records.OperationId(bytes);
  }

  private static Commitments.Context context(String owner, long generation) {
    return new Commitments.Context("issuer-a", owner, generation);
  }

  private static SessionStore.Access access(String owner) {
    return new SessionStore.Access(owner, () -> {});
  }

  private static ExecutionStore.Access execAccess(String owner) {
    return new ExecutionStore.Access(owner, () -> {});
  }

  private static AdmissionStore.Clock clock(long utc) {
    return () -> new AdmissionStore.Time(utc, true);
  }

  private static long ms(long value) {
    return TimeUnit.MILLISECONDS.toNanos(value);
  }

  private static long tick(long value) {
    return ms(Math.multiplyExact(value, 100));
  }

  private static void assertCode(ProtocolError.Code expected, Throwing action) {
    ProtocolError error = assertThrows(ProtocolError.class, action::run);
    assertEquals(expected, error.code(), error::getMessage);
  }

  private record Published(String owner, long generation, Records.Digest digest) {}

  @FunctionalInterface
  private interface Throwing {
    void run() throws Exception;
  }
}
