package ai.pipestream.quic.v2;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

@Timeout(30)
final class RetentionServiceTest {
  private static final byte[] INPUT = {9, 8, 7};
  private static final byte[] OUTPUT = {1, 2, 3, 4};

  @TempDir Path directory;

  @Test
  void manualMaintenanceReleasesPublishedInputAndOutputAndDetachesCleanly() throws Exception {
    try (ResultFixture fixture = new ResultFixture(directory, "manual", INPUT, OUTPUT)) {
      RetentionService service =
          new RetentionService(
              fixture.sessions,
              fixture.inputs,
              ResultFixture.clock(21_200),
              new RetentionService.Limits(64, 60_000));
      try {
        assertThrows(IOException.class, fixture.inputs::close);
        assertThrows(
            IllegalStateException.class,
            () ->
                new RetentionService(
                    fixture.sessions,
                    fixture.inputs,
                    ResultFixture.clock(21_200),
                    new RetentionService.Limits(1, 60_000)));
        RetentionService.Status status = service.maintain();
        assertEquals(1, status.jobsExamined());
        assertEquals(0, status.orphansExamined());
        assertEquals(2, status.released());
        assertEquals(0, status.refused());
        assertNull(status.lastFailure());
        assertEquals(new InputStore.Usage(0, 0, 0), fixture.inputs.usage());
        assertEquals(Records.State.SUCCEEDED, current(fixture).state());
      } finally {
        service.close();
        assertTrue(service.awaitStopped(5000));
      }
      assertTrue(service.status().stopping());
      assertTrue(service.status().stopped());
      assertEquals(0, fixture.inputs.usage().handles());
    }
  }

  @Test
  void daemonTimerEventuallyReclaimsWithoutForegroundMaintenance() throws Exception {
    try (ResultFixture fixture = new ResultFixture(directory, "timer", INPUT, OUTPUT)) {
      RetentionService service =
          new RetentionService(
              fixture.sessions,
              fixture.inputs,
              ResultFixture.clock(21_200),
              new RetentionService.Limits(1, 5));
      boolean stopped = false;
      try {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (service.status().released() < 2 && System.nanoTime() - deadline < 0)
          Thread.sleep(10);
        assertEquals(2, service.status().released());
        assertEquals(0, fixture.inputs.usage().bytes());
        assertEquals(0, fixture.inputs.usage().files());
        assertEquals(Records.State.SUCCEEDED, current(fixture).state());
        service.close();
        assertTrue(service.awaitStopped(5000));
        stopped = true;
        assertEquals(0, fixture.inputs.usage().handles());
      } finally {
        if (!stopped) {
          service.close();
          assertTrue(service.awaitStopped(5000));
        }
      }
    }
  }

  @Test
  void unsafeClockRecordsNamedFailuresAndRetainsEveryResource() throws Exception {
    try (ResultFixture fixture = new ResultFixture(directory, "unsafe", INPUT, OUTPUT)) {
      InputStore.Usage retained = fixture.inputs.usage();
      try (RetentionService service =
          new RetentionService(
              fixture.sessions,
              fixture.inputs,
              () -> new AdmissionStore.Time(21_200, false),
              new RetentionService.Limits(64, 60_000))) {
        RetentionService.Status status = service.maintain();
        assertEquals(1, status.jobsExamined());
        assertEquals(2, status.refused());
        assertNotNull(status.lastFailure());
        assertEquals(ProtocolError.Code.CLOCK_UNSAFE, status.lastFailure().code());
        assertEquals("output retention unavailable", status.lastFailure().detail());
        assertEquals(retained, fixture.inputs.usage());
        assertTrue(fixture.inputs.find(fixture.context(), fixture.inputHeader).isPresent());
        assertTrue(
            fixture.inputs.findReservation(fixture.context(), fixture.inputHeader).isPresent());
      }
    }
  }

  @Test
  void pinnedFirstJobDoesNotStarveLaterSettledJobAcrossSingleEntryPages() throws Exception {
    try (Fixture fixture = new Fixture(directory, "fair", List.of(1L, 2L))) {
      Records.InputHeader first = fixture.publish(1, new byte[] {1});
      Records.InputHeader second = fixture.publish(2, new byte[] {2});
      OutputStore.Stored firstOutput =
          fixture.inputs.findOutput(fixture.context(), first, fixture.lease(1), 0).orElseThrow();
      try (InputStream pin = firstOutput.openStream();
          RetentionService service =
              new RetentionService(
                  fixture.sessions,
                  fixture.inputs,
                  ResultFixture.clock(21_200),
                  new RetentionService.Limits(1, 60_000))) {
        assertEquals(1, pin.read());
        service.maintain();
        assertTrue(fixture.inputs.findReservation(fixture.context(), first).isPresent());
        service.maintain();
        assertTrue(fixture.inputs.findReservation(fixture.context(), second).isEmpty());
        assertTrue(fixture.inputs.findReservation(fixture.context(), first).isPresent());
        assertEquals(Records.State.SUCCEEDED, fixture.view(1).state());
        assertEquals(Records.State.SUCCEEDED, fixture.view(2).state());
        assertTrue(service.status().jobsExamined() >= 2);
      }
    }
  }

  @Test
  void replacementServiceRetriesAbsentSynchronizedOrphanChargesWithoutStoreReopen()
      throws Exception {
    for (boolean funding : List.of(false, true)) {
      AtomicInteger syncs = new AtomicInteger();
      int targetSync = funding ? 2 : 1;
      InputStore.Probe probe =
          phase -> {
            if (phase == InputStore.Phase.ORPHAN_SYNCED && syncs.incrementAndGet() == targetSync)
              throw new IOException("stop after orphan sync");
          };
      try (Fixture fixture = new Fixture(directory, "replacement-" + funding, List.of(1L), probe)) {
        Records.InputHeader orphan = fixture.install(1, new byte[] {1});
        fixture.inputs.reserveOutputs(fixture.context(), orphan);
        RetentionService first =
            new RetentionService(
                fixture.sessions,
                fixture.inputs,
                ResultFixture.clock(1000),
                new RetentionService.Limits(64, 60_000));
        InputStore.Usage before = fixture.inputs.usage();
        long inputBytes = Files.size(onlyInput(fixture.inputsPath));
        try {
          RetentionService.Status failed = first.maintain();
          assertEquals(1, failed.refused());
          assertEquals(funding ? 1 : 0, failed.released());
          assertNotNull(failed.lastFailure());
          assertEquals(ProtocolError.Code.INTERNAL_ERROR, failed.lastFailure().code());
          assertEquals("orphan reconciliation unavailable", failed.lastFailure().detail());
          assertTrue(fixture.inputs.usage().bytes() > 0);
          assertEquals(before.bytes() - (funding ? inputBytes : 0), fixture.inputs.usage().bytes());
          assertEquals(before.files() - (funding ? 1 : 0), fixture.inputs.usage().files());
          if (funding) {
            assertTrue(fixture.inputs.find(fixture.context(), orphan).isEmpty());
            assertTrue(fixture.inputs.findReservation(fixture.context(), orphan).isEmpty());
          } else {
            assertTrue(fixture.inputs.find(fixture.context(), orphan).isEmpty());
            assertTrue(fixture.inputs.findReservation(fixture.context(), orphan).isPresent());
          }
        } finally {
          first.close();
          assertTrue(first.awaitStopped(5000));
        }

        try (RetentionService replacement =
            new RetentionService(
                fixture.sessions,
                fixture.inputs,
                ResultFixture.clock(1000),
                new RetentionService.Limits(64, 60_000))) {
          RetentionService.Status recovered = replacement.maintain();
          assertEquals(funding ? 1 : 2, recovered.released());
          assertEquals(0, recovered.refused());
          assertEquals(0, fixture.inputs.usage().bytes());
          assertEquals(0, fixture.inputs.usage().files());
        }
        assertEquals(0, fixture.inputs.usage().handles());
        assertEquals(Records.State.DECLARED, fixture.view(1).state());
      }
    }
  }

  @Test
  void closeDuringActiveMaintenanceWaitsForPhysicalCompletionBeforeDetaching() throws Exception {
    CountDownLatch entered = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    InputStore.Probe probe =
        phase -> {
          if (phase == InputStore.Phase.ORPHAN_SYNCED) {
            entered.countDown();
            try {
              if (!release.await(5, TimeUnit.SECONDS))
                throw new IOException("maintenance release latch timed out");
            } catch (InterruptedException interrupted) {
              Thread.currentThread().interrupt();
              throw new IOException("maintenance probe interrupted", interrupted);
            }
          }
        };
    try (Fixture fixture = new Fixture(directory, "close-active", List.of(1L), probe)) {
      fixture.install(1, new byte[] {1}, 0, 0);
      RetentionService service =
          new RetentionService(
              fixture.sessions,
              fixture.inputs,
              ResultFixture.clock(1000),
              new RetentionService.Limits(64, 60_000));
      FutureTask<RetentionService.Status> maintenance = new FutureTask<>(service::maintain);
      Thread worker = Thread.ofPlatform().start(maintenance);
      try {
        assertTrue(entered.await(5, TimeUnit.SECONDS));
        service.close();
        assertFalse(service.awaitStopped(0));
        assertFalse(service.status().stopped());
      } finally {
        release.countDown();
        service.close();
        worker.join(5000);
      }
      assertFalse(worker.isAlive());
      assertEquals(1, maintenance.get(1, TimeUnit.SECONDS).released());
      assertTrue(service.awaitStopped(5000));
      assertEquals(new InputStore.Usage(0, 0, 0), fixture.inputs.usage());
      assertEquals(Records.State.DECLARED, fixture.view(1).state());
    }
  }

  @Test
  void orphanCleanupPreservesIndependentlyAcceptedWorkAndItsState() throws Exception {
    try (Fixture fixture = new Fixture(directory, "orphan", List.of(1L, 2L))) {
      Records.InputHeader accepted = fixture.admit(1, new byte[] {1});
      Records.InputHeader orphan = fixture.install(2, new byte[] {2});
      fixture.inputs.reserveOutputs(fixture.context(), orphan);
      try (RetentionService service =
          new RetentionService(
              fixture.sessions,
              fixture.inputs,
              ResultFixture.clock(1000),
              new RetentionService.Limits(64, 60_000))) {
        RetentionService.Status status = service.maintain();
        assertEquals(Records.State.ACTIVE, fixture.view(1).state());
        assertEquals(Records.State.DECLARED, fixture.view(2).state());
        assertTrue(fixture.inputs.find(fixture.context(), accepted).isPresent());
        assertTrue(fixture.inputs.findReservation(fixture.context(), accepted).isPresent());
        assertTrue(fixture.inputs.find(fixture.context(), orphan).isEmpty());
        assertTrue(fixture.inputs.findReservation(fixture.context(), orphan).isEmpty());
        assertEquals(2, status.released());
        assertEquals(4, status.orphansExamined());
        assertEquals(0, status.refused());
      }
    }
  }

  @Test
  void staleUnsafeOrphanCandidateBecomesAbsentAfterTheSameInputIsLegitimatelySettled()
      throws Exception {
    AtomicBoolean safe = new AtomicBoolean(false);
    try (Fixture fixture = new Fixture(directory, "stale-candidate", List.of(1L, 2L))) {
      Records.InputHeader retained = fixture.install(1, new byte[] {1});
      fixture.inputs.reserveOutputs(fixture.context(), retained);
      try (RetentionService service =
          new RetentionService(
              fixture.sessions,
              fixture.inputs,
              () -> new AdmissionStore.Time(21_200, safe.get()),
              new RetentionService.Limits(64, 60_000))) {
        RetentionService.Status unsafe = service.maintain();
        assertEquals(1, unsafe.refused());
        assertEquals(ProtocolError.Code.CLOCK_UNSAFE, unsafe.lastFailure().code());
        assertEquals("orphan reconciliation unavailable", unsafe.lastFailure().detail());

        assertEquals(retained, fixture.publish(1, new byte[] {1}));
        Records.InputHeader later = fixture.install(2, new byte[] {2});
        fixture.inputs.reserveOutputs(fixture.context(), later);
        safe.set(true);
        for (int pass = 0; pass < 4 && fixture.inputs.usage().bytes() > 0; pass++)
          service.maintain();

        RetentionService.Status recovered = service.status();
        assertEquals(1, recovered.refused());
        assertEquals(4, recovered.released());
        assertEquals(0, fixture.inputs.usage().bytes());
        assertEquals(0, fixture.inputs.usage().files());
        assertEquals(Records.State.SUCCEEDED, fixture.view(1).state());
        assertEquals(
            ResultFixture.digest(new byte[] {1}),
            fixture.view(1).manifest().outputs().get(0).sha256());
        assertEquals(Records.State.DECLARED, fixture.view(2).state());
        assertTrue(fixture.inputs.find(fixture.context(), later).isEmpty());
        assertTrue(fixture.inputs.findReservation(fixture.context(), later).isEmpty());
      }
    }
  }

  private static Records.WorkView current(ResultFixture fixture) throws Exception {
    return fixture
        .sessions
        .snapshot(
            ResultFixture.sessionAccess("alice"),
            ResultFixture.SELECTED,
            fixture.binding.generation(),
            new Messages.Watch(90, ResultFixture.WORK, 0, 0))
        .work();
  }

  private static Path onlyInput(Path root) throws Exception {
    try (var entries = Files.newDirectoryStream(root.resolve("objects"), "*.input")) {
      var iterator = entries.iterator();
      assertTrue(iterator.hasNext());
      Path result = iterator.next();
      assertFalse(iterator.hasNext());
      return result;
    }
  }

  private static final class Fixture implements AutoCloseable {
    final SessionStore sessions;
    InputStore inputs;
    final Messages.Binding binding;
    final Path inputsPath;
    final java.util.Map<Long, Records.InputHeader> headers = new java.util.HashMap<>();
    final java.util.Map<Long, ExecutionStore.Lease> leases = new java.util.HashMap<>();

    Fixture(Path directory, String name, List<Long> members) throws Exception {
      this(directory, name, members, null);
    }

    Fixture(Path directory, String name, List<Long> members, InputStore.Probe probe)
        throws Exception {
      sessions =
          SessionStore.initialize(
              directory.resolve(name + ".sqlite"), ResultFixture.configuration());
      binding =
          sessions.create(
              ResultFixture.sessionAccess("alice"),
              ResultFixture.SELECTED,
              new Messages.Create(1, 1, new Records.Policy(10_000, 20_000, 30_000)));
      sessions.declare(
          ResultFixture.sessionAccess("alice"),
          ResultFixture.SELECTED,
          1,
          new Messages.Declare(2, ResultFixture.operation(1), 0, members, false));
      inputsPath = directory.resolve(name + "-inputs");
      inputs =
          InputStore.initializeForAuthority(
              inputsPath, ResultFixture.INPUT_LIMITS, sessions.identity());
      sessions.bindInputs(inputs);
      if (probe != null) {
        inputs.close();
        inputs = InputStore.open(inputsPath, ResultFixture.INPUT_LIMITS, probe);
        sessions.verifyInputs(inputs);
      }
    }

    Records.InputHeader install(long entity, byte[] payload) throws Exception {
      return install(entity, payload, 1, 16);
    }

    Records.InputHeader install(long entity, byte[] payload, int outputCount, long outputBytes)
        throws Exception {
      Records.InputHeader header = header(entity, payload, outputCount, outputBytes);
      try (InputStore.Receiver receiver =
          inputs.begin(context(), header, ResultFixture.SELECTED, 1)) {
        receiver.write(ByteBuffer.wrap(payload), 2);
        receiver.finish(3);
      }
      headers.put(entity, header);
      return header;
    }

    Records.InputHeader admit(long entity, byte[] payload) throws Exception {
      Records.InputHeader header = install(entity, payload);
      sessions.admit(
          ResultFixture.sessionAccess("alice"),
          ResultFixture.SELECTED,
          1,
          inputs,
          header,
          entity + 10,
          ResultFixture.clock(1000),
          ResultFixture.ALLOW_EXECUTION);
      return header;
    }

    Records.InputHeader publish(long entity, byte[] payload) throws Exception {
      Records.InputHeader header = admit(entity, payload);
      ExecutionStore.Lease lease =
          sessions.claimExecution(
              ResultFixture.executionAccess("alice"),
              1,
              new Records.WorkKey(0, 0, entity),
              inputs,
              500,
              ResultFixture.clock(1000),
              ResultFixture.ALLOW_EXECUTION);
      leases.put(entity, lease);
      try (OutputStore.Writer writer =
          inputs.beginOutput(
              context(), header, lease, 0, payload.length, "application/octet-stream", 16)) {
        writer.write(ByteBuffer.wrap(payload));
        writer.finish();
      }
      sessions.succeedExecution(
          ResultFixture.executionAccess("alice"),
          lease,
          inputs,
          1,
          ResultFixture.ENDPOINT,
          ResultFixture.clock(1000),
          ResultFixture.ALLOW_EXECUTION);
      return header;
    }

    ExecutionStore.Lease lease(long entity) {
      return leases.get(entity);
    }

    Records.WorkView view(long entity) throws Exception {
      return sessions
          .snapshot(
              ResultFixture.sessionAccess("alice"),
              ResultFixture.SELECTED,
              1,
              new Messages.Watch(90 + entity, new Records.WorkKey(0, 0, entity), 0, 0))
          .work();
    }

    Commitments.Context context() {
      return new Commitments.Context("issuer-a", "alice", 1);
    }

    private static Records.InputHeader header(
        long entity, byte[] payload, int outputCount, long outputBytes) throws Exception {
      return new Records.InputHeader(
          1,
          ResultFixture.operation(Math.toIntExact(entity + 20)),
          new Records.AdmitParameters(
              new Records.WorkKey(0, 0, entity),
              new Records.Input(
                  payload.length, ResultFixture.digest(payload), "application/octet-stream"),
              "copy",
              0,
              1000,
              new Records.OutputBudget(outputCount, outputBytes)));
    }

    @Override
    public void close() throws IOException {
      inputs.close();
    }
  }
}
