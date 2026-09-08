package ai.pipestream.quic.v2;

import static org.junit.jupiter.api.Assertions.*;

import ai.pipestream.quic.BoundedSqlite;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

@Timeout(30)
final class RetentionStoreTest {
  private static final byte[] INPUT = {9, 8, 7, 6};
  private static final byte[] OUTPUT = {1, 2, 3};

  @TempDir Path directory;

  @Test
  void terminalInputReclaimsExactlyOnceWithoutChangingOutputFundingOrReceipt() throws Exception {
    try (ResultFixture fixture = new ResultFixture(directory, "release", INPUT, OUTPUT)) {
      InputStore.Usage retained = fixture.inputs.usage();
      Records.Manifest manifest = fixture.published.manifest();
      long inputBytes = Files.size(onlyInput(fixture.inputsPath));
      long credits = job(fixture).geometry().credits();
      int capacity = job(fixture).geometry().capacity();
      assertTrue(fixture.inputs.find(fixture.context(), fixture.inputHeader).isPresent());
      assertCode(
          ProtocolError.Code.CLOCK_UNSAFE,
          () ->
              fixture.sessions.reclaimInput(
                  fixture.binding.generation(),
                  ResultFixture.WORK,
                  fixture.inputs,
                  ResultFixture.clock(1199)));
      assertNull(job(fixture).record().inputReleaseAt());
      assertTrue(job(fixture).record().inputLive());
      assertEquals(retained, fixture.inputs.usage());

      assertEquals(
          RetentionStore.Result.RELEASED,
          fixture.sessions.reclaimInput(
              fixture.binding.generation(),
              ResultFixture.WORK,
              fixture.inputs,
              ResultFixture.clock(1200)));
      InputStore.Usage released = fixture.inputs.usage();
      assertEquals(retained.bytes() - inputBytes, released.bytes());
      assertEquals(retained.files() - 1, released.files());
      assertEquals(0, released.handles());
      assertTrue(fixture.inputs.find(fixture.context(), fixture.inputHeader).isEmpty());
      Long releaseAt = job(fixture).record().inputReleaseAt();
      assertEquals(1200L, releaseAt);
      assertFalse(job(fixture).record().inputLive());
      assertEquals(credits - 2, job(fixture).geometry().credits());
      assertEquals(capacity, job(fixture).geometry().capacity());
      assertTrue(
          fixture
              .inputs
              .findOutput(fixture.context(), fixture.inputHeader, fixture.lease, 0)
              .isPresent());
      assertEquals(manifest, current(fixture).manifest());
      assertEquals(
          RetentionStore.Result.ALREADY_RELEASED,
          fixture.sessions.reclaimInput(
              fixture.binding.generation(),
              ResultFixture.WORK,
              fixture.inputs,
              ResultFixture.clock(1200)));

      fixture.reopen();
      assertEquals(released, fixture.inputs.usage());
      assertEquals(releaseAt, job(fixture).record().inputReleaseAt());
      assertFalse(job(fixture).record().inputLive());
      assertEquals(credits - 2, job(fixture).geometry().credits());
      assertEquals(manifest, current(fixture).manifest());
      assertEquals(
          RetentionStore.Result.ALREADY_RELEASED,
          fixture.sessions.reclaimInput(
              fixture.binding.generation(),
              ResultFixture.WORK,
              fixture.inputs,
              ResultFixture.clock(1200)));
    }
  }

  @Test
  void liveReaderAndDuplicateReceiverPersistIntentButDelayPhysicalRemoval() throws Exception {
    try (ResultFixture readerFixture = new ResultFixture(directory, "reader-pin", INPUT, OUTPUT)) {
      InputStore.Stored stored =
          readerFixture
              .inputs
              .find(readerFixture.context(), readerFixture.inputHeader)
              .orElseThrow();
      try (InputStream reader = stored.openStream()) {
        assertEquals(
            RetentionStore.Result.PINNED,
            readerFixture.sessions.reclaimInput(
                readerFixture.binding.generation(),
                ResultFixture.WORK,
                readerFixture.inputs,
                ResultFixture.clock(1200)));
        assertTrue(
            readerFixture.inputs.inputPinned(readerFixture.context(), readerFixture.inputHeader));
        assertArrayEquals(INPUT, reader.readAllBytes());
      }
      assertEquals(
          RetentionStore.Result.RELEASED,
          readerFixture.sessions.reclaimInput(
              readerFixture.binding.generation(),
              ResultFixture.WORK,
              readerFixture.inputs,
              ResultFixture.clock(1200)));
    }

    try (ResultFixture receiverFixture =
        new ResultFixture(directory, "receiver-pin", INPUT, OUTPUT)) {
      try (InputStore.Receiver duplicate =
          receiverFixture.inputs.begin(
              receiverFixture.context(),
              receiverFixture.inputHeader,
              ResultFixture.SELECTED,
              1200)) {
        duplicate.write(ByteBuffer.wrap(INPUT), 1200);
        assertEquals(
            RetentionStore.Result.PINNED,
            receiverFixture.sessions.reclaimInput(
                receiverFixture.binding.generation(),
                ResultFixture.WORK,
                receiverFixture.inputs,
                ResultFixture.clock(1200)));
        assertFalse(
            receiverFixture.inputs.inputPinned(
                receiverFixture.context(), receiverFixture.inputHeader));
        duplicate.finish(1200);
        assertTrue(
            receiverFixture
                .inputs
                .find(receiverFixture.context(), receiverFixture.inputHeader)
                .isPresent());
        assertTrue(job(receiverFixture).record().inputLive());
      }
      assertEquals(
          RetentionStore.Result.RELEASED,
          receiverFixture.sessions.reclaimInput(
              receiverFixture.binding.generation(),
              ResultFixture.WORK,
              receiverFixture.inputs,
              ResultFixture.clock(1200)));
    }
  }

  @Test
  void zeroByteInputUsesTheSameDurableReleaseProtocol() throws Exception {
    try (ResultFixture fixture = new ResultFixture(directory, "zero", new byte[0], OUTPUT)) {
      InputStore.Usage retained = fixture.inputs.usage();
      assertEquals(
          RetentionStore.Result.RELEASED,
          fixture.sessions.reclaimInput(
              fixture.binding.generation(),
              ResultFixture.WORK,
              fixture.inputs,
              ResultFixture.clock(1200)));
      assertEquals(retained.files() - 1, fixture.inputs.usage().files());
      assertTrue(fixture.inputs.usage().bytes() < retained.bytes());
      assertTrue(fixture.inputs.find(fixture.context(), fixture.inputHeader).isEmpty());
    }
  }

  @Test
  void nonterminalAndUnsafeClockRefusalsDoNotInstallIntentOrDeleteInput() throws Exception {
    try (ResultFixture active = new ResultFixture(directory, "active", INPUT, OUTPUT, false)) {
      InputStore.Usage retained = active.inputs.usage();
      assertEquals(
          RetentionStore.Result.NOT_READY,
          active.sessions.reclaimInput(
              active.binding.generation(),
              ResultFixture.WORK,
              active.inputs,
              ResultFixture.clock(1200)));
      assertEquals(retained, active.inputs.usage());
      assertTrue(active.inputs.find(active.context(), active.inputHeader).isPresent());
    }

    try (ResultFixture unsafe = new ResultFixture(directory, "unsafe", INPUT, OUTPUT)) {
      InputStore.Usage retained = unsafe.inputs.usage();
      assertCode(
          ProtocolError.Code.CLOCK_UNSAFE,
          () ->
              unsafe.sessions.reclaimInput(
                  unsafe.binding.generation(),
                  ResultFixture.WORK,
                  unsafe.inputs,
                  () -> new AdmissionStore.Time(1200, false)));
      assertEquals(retained, unsafe.inputs.usage());
      assertTrue(unsafe.inputs.find(unsafe.context(), unsafe.inputHeader).isPresent());
    }
  }

  @Test
  void terminalBranchInputWaitsForActualChildClosureBeforeRelease() throws Exception {
    Path database = directory.resolve("branch.sqlite");
    Path inputPath = directory.resolve("branch-inputs");
    SessionStore sessions = SessionStore.initialize(database, ResultFixture.configuration());
    Messages.Binding binding =
        sessions.create(
            ResultFixture.sessionAccess("alice"),
            ResultFixture.SELECTED,
            new Messages.Create(1, 1, new Records.Policy(10_000, 20_000, 30_000)));
    sessions.declare(
        ResultFixture.sessionAccess("alice"),
        ResultFixture.SELECTED,
        binding.generation(),
        new Messages.Declare(2, ResultFixture.operation(1), 0, List.of(1L), false));
    try (InputStore inputs =
        InputStore.initializeForAuthority(
            inputPath, ResultFixture.INPUT_LIMITS, sessions.identity())) {
      sessions.bindInputs(inputs);
      Records.InputHeader header =
          new Records.InputHeader(
              binding.generation(),
              ResultFixture.operation(2),
              new Records.AdmitParameters(
                  ResultFixture.WORK,
                  new Records.Input(
                      INPUT.length, ResultFixture.digest(INPUT), "application/octet-stream"),
                  "copy",
                  1,
                  10_000,
                  new Records.OutputBudget(0, 0)));
      Commitments.Context context =
          new Commitments.Context(binding.authority(), binding.owner(), binding.generation());
      try (InputStore.Receiver receiver =
          inputs.begin(context, header, ResultFixture.SELECTED, 1)) {
        receiver.write(ByteBuffer.wrap(INPUT), 2);
        receiver.finish(3);
      }
      sessions.admit(
          ResultFixture.sessionAccess("alice"),
          ResultFixture.SELECTED,
          binding.generation(),
          inputs,
          header,
          3,
          ResultFixture.clock(1000),
          ResultFixture.ALLOW_EXECUTION);
      Records.WorkView failed =
          sessions.expireExecution(
              binding.generation(), ResultFixture.WORK, ResultFixture.clock(11_000));
      assertEquals(Records.State.FAILED, failed.state());
      assertNotNull(failed.child());
      assertEquals(
          RetentionStore.Result.NOT_READY,
          sessions.reclaimInput(
              binding.generation(), ResultFixture.WORK, inputs, ResultFixture.clock(11_000)));
      try (var connection =
          BoundedSqlite.open(database, ResultFixture.configuration().files()).connect()) {
        assertNull(
            AdmissionStore.job(connection, binding, ResultFixture.WORK).record().inputReleaseAt());
      }

      sessions.declare(
          ResultFixture.sessionAccess("alice"),
          ResultFixture.SELECTED,
          binding.generation(),
          new Messages.Declare(
              4, ResultFixture.operation(4), failed.child().scope(), List.of(), true));
      ClosureStore.Cursor cursor = new ClosureStore.Cursor();
      for (int step = 0; step < 8; step++)
        sessions.reconcileClosures(cursor, 1, ResultFixture.clock(11_000));
      Messages.PageResponse childPage =
          sessions.page(
              ResultFixture.sessionAccess("alice"),
              ResultFixture.SELECTED,
              binding.generation(),
              new Messages.Page(5, failed.child().scope(), 0, 1));
      assertTrue(childPage.sealed());
      Records.ScopeSummary childSummary =
          sessions.scopeSummary(
              ResultFixture.sessionAccess("alice"),
              ResultFixture.SELECTED,
              binding.generation(),
              failed.child().scope(),
              childPage.seal());
      Records.WorkView beforeRelease =
          sessions
              .snapshot(
                  ResultFixture.sessionAccess("alice"),
                  ResultFixture.SELECTED,
                  binding.generation(),
                  new Messages.Watch(6, ResultFixture.WORK, 0, 0))
              .work();
      assertEquals(
          RetentionStore.Result.RELEASED,
          sessions.reclaimInput(
              binding.generation(), ResultFixture.WORK, inputs, ResultFixture.clock(11_000)));
      assertEquals(
          beforeRelease,
          sessions
              .snapshot(
                  ResultFixture.sessionAccess("alice"),
                  ResultFixture.SELECTED,
                  binding.generation(),
                  new Messages.Watch(7, ResultFixture.WORK, 0, 0))
              .work());
      assertEquals(
          childSummary,
          sessions.scopeSummary(
              ResultFixture.sessionAccess("alice"),
              ResultFixture.SELECTED,
              binding.generation(),
              failed.child().scope(),
              childPage.seal()));
    }
  }

  @Test
  void revokedTerminalSessionStillAllowsOwnerlessLocalInputMaintenance() throws Exception {
    try (ResultFixture fixture = new ResultFixture(directory, "revoked", INPUT, OUTPUT)) {
      fixture.sessions.revoke(
          new SessionStore.Access("local-operator", () -> {}),
          fixture.binding.generation(),
          ResultFixture.clock(1200));
      assertEquals(
          RetentionStore.Result.RELEASED,
          fixture.sessions.reclaimInput(
              fixture.binding.generation(),
              ResultFixture.WORK,
              fixture.inputs,
              ResultFixture.clock(1200)));
      assertFalse(job(fixture).record().inputLive());
      fixture.reopen();
      assertFalse(job(fixture).record().inputLive());
    }
  }

  @Test
  void intraOperationRegressionAndUnsafeClockAfterIntentNeverDeleteOrRefund() throws Exception {
    try (ResultFixture regressed = new ResultFixture(directory, "regressed", INPUT, OUTPUT)) {
      InputStore.Usage retained = regressed.inputs.usage();
      AtomicInteger samples = new AtomicInteger();
      assertCode(
          ProtocolError.Code.CLOCK_UNSAFE,
          () ->
              regressed.sessions.reclaimInput(
                  regressed.binding.generation(),
                  ResultFixture.WORK,
                  regressed.inputs,
                  () ->
                      new AdmissionStore.Time(samples.incrementAndGet() == 1 ? 1300 : 1299, true)));
      assertNull(job(regressed).record().inputReleaseAt());
      assertEquals(retained, regressed.inputs.usage());
      assertTrue(regressed.inputs.find(regressed.context(), regressed.inputHeader).isPresent());
    }

    try (ResultFixture unsafe =
        new ResultFixture(directory, "unsafe-after-intent", INPUT, OUTPUT)) {
      InputStore.Usage retained = unsafe.inputs.usage();
      AtomicInteger samples = new AtomicInteger();
      assertCode(
          ProtocolError.Code.CLOCK_UNSAFE,
          () ->
              unsafe.sessions.reclaimInput(
                  unsafe.binding.generation(),
                  ResultFixture.WORK,
                  unsafe.inputs,
                  () -> new AdmissionStore.Time(1300, samples.incrementAndGet() < 3)));
      assertEquals(1300L, job(unsafe).record().inputReleaseAt());
      assertTrue(job(unsafe).record().inputLive());
      assertEquals(retained, unsafe.inputs.usage());
      assertTrue(unsafe.inputs.find(unsafe.context(), unsafe.inputHeader).isPresent());
      assertEquals(
          RetentionStore.Result.RELEASED,
          unsafe.sessions.reclaimInput(
              unsafe.binding.generation(),
              ResultFixture.WORK,
              unsafe.inputs,
              ResultFixture.clock(1300)));
    }
  }

  @Test
  void committedIntentAndReleaseProbeFailuresRecoverWithoutLosingEvidence() throws Exception {
    for (RetentionStore.Phase phase :
        List.of(
            RetentionStore.Phase.INPUT_INTENT_COMMITTED,
            RetentionStore.Phase.INPUT_RELEASE_COMMITTED)) {
      try (ResultFixture fixture =
          new ResultFixture(directory, "probe-" + phase.name(), INPUT, OUTPUT)) {
        AtomicInteger reached = new AtomicInteger();
        IOException interrupted =
            assertThrows(
                IOException.class,
                () ->
                    fixture.sessions.reclaimInput(
                        fixture.binding.generation(),
                        ResultFixture.WORK,
                        fixture.inputs,
                        ResultFixture.clock(1200),
                        observed -> {
                          if (observed == phase) {
                            reached.incrementAndGet();
                            throw new IOException("stop at " + phase);
                          }
                        }));
        assertEquals("stop at " + phase, interrupted.getMessage());
        assertEquals(1, reached.get());
        fixture.reopen();
        RetentionStore.Result recovered =
            fixture.sessions.reclaimInput(
                fixture.binding.generation(),
                ResultFixture.WORK,
                fixture.inputs,
                ResultFixture.clock(1200));
        assertEquals(
            phase == RetentionStore.Phase.INPUT_RELEASE_COMMITTED
                ? RetentionStore.Result.ALREADY_RELEASED
                : RetentionStore.Result.RELEASED,
            recovered);
        assertTrue(fixture.inputs.find(fixture.context(), fixture.inputHeader).isEmpty());
        assertEquals(fixture.published.manifest(), current(fixture).manifest());
      }
    }
  }

  @Test
  void physicalRemovalInterruptionsResumeFromDurableIntent() throws Exception {
    for (InputStore.Phase phase :
        List.of(InputStore.Phase.INPUT_RECLAIM_REMOVED, InputStore.Phase.INPUT_RECLAIM_SYNCED)) {
      try (ResultFixture fixture =
          new ResultFixture(directory, "physical-" + phase.name(), INPUT, OUTPUT)) {
        fixture.inputs.close();
        AtomicInteger reached = new AtomicInteger();
        fixture.inputs =
            InputStore.open(
                fixture.inputsPath,
                ResultFixture.INPUT_LIMITS,
                observed -> {
                  if (observed == phase) {
                    reached.incrementAndGet();
                    throw new IOException("stop at " + phase);
                  }
                });
        assertThrows(
            IOException.class,
            () ->
                fixture.sessions.reclaimInput(
                    fixture.binding.generation(),
                    ResultFixture.WORK,
                    fixture.inputs,
                    ResultFixture.clock(1200)));
        assertEquals(1, reached.get());
        fixture.reopen();
        RetentionStore.Result recovered =
            fixture.sessions.reclaimInput(
                fixture.binding.generation(),
                ResultFixture.WORK,
                fixture.inputs,
                ResultFixture.clock(1200));
        assertEquals(RetentionStore.Result.RELEASED, recovered);
        assertTrue(fixture.inputs.find(fixture.context(), fixture.inputHeader).isEmpty());
      }
    }
  }

  @Test
  void sameProcessPhysicalSyncFailureRetainsChargesUntilRetryCompletes() throws Exception {
    try (ResultFixture fixture = new ResultFixture(directory, "same-process-sync", INPUT, OUTPUT)) {
      InputStore.Usage retained = fixture.inputs.usage();
      fixture.inputs.close();
      AtomicInteger syncs = new AtomicInteger();
      fixture.inputs =
          InputStore.open(
              fixture.inputsPath,
              ResultFixture.INPUT_LIMITS,
              phase -> {
                if (phase == InputStore.Phase.INPUT_RECLAIM_SYNCED && syncs.incrementAndGet() == 1)
                  throw new IOException("sync interrupted");
              });
      IOException failure =
          assertThrows(
              IOException.class,
              () ->
                  fixture.sessions.reclaimInput(
                      fixture.binding.generation(),
                      ResultFixture.WORK,
                      fixture.inputs,
                      ResultFixture.clock(1200)));
      assertEquals("sync interrupted", failure.getMessage());
      assertEquals(retained, fixture.inputs.usage());
      assertTrue(job(fixture).record().inputLive());

      assertEquals(
          RetentionStore.Result.RELEASED,
          fixture.sessions.reclaimInput(
              fixture.binding.generation(),
              ResultFixture.WORK,
              fixture.inputs,
              ResultFixture.clock(1200)));
      assertFalse(job(fixture).record().inputLive());
    }
  }

  @Test
  void missingLiveInputWithoutCommittedIntentRefusesInsteadOfRefunding() throws Exception {
    try (ResultFixture fixture = new ResultFixture(directory, "missing", INPUT, OUTPUT)) {
      InputStore.Usage retained = fixture.inputs.usage();
      Files.delete(onlyInput(fixture.inputsPath));
      assertThrows(
          IOException.class,
          () ->
              fixture.sessions.reclaimInput(
                  fixture.binding.generation(),
                  ResultFixture.WORK,
                  fixture.inputs,
                  ResultFixture.clock(1200)));
      assertEquals(retained, fixture.inputs.usage());
      assertEquals(fixture.published.manifest(), current(fixture).manifest());
    }
  }

  @Test
  void recoveryRejectsChecksummedReleaseTimestampBeforeTerminalEligibility() throws Exception {
    try (ResultFixture fixture =
            new ResultFixture(directory, "invalid-release-time", INPUT, OUTPUT);
        var connection =
            BoundedSqlite.open(fixture.database, ResultFixture.configuration().files()).connect();
        var statement = connection.createStatement()) {
      statement.execute("BEGIN IMMEDIATE");
      AdmissionStore.StoredJob stored =
          AdmissionStore.job(connection, fixture.binding, ResultFixture.WORK);
      JobRecord source = stored.record();
      assertThrows(
          ProtocolError.class,
          () ->
              new JobRecord(
                  source.input(),
                  source.safety(),
                  source.attempt(),
                  source.lease(),
                  source.leaseUntil(),
                  source.stage(),
                  source.inputReference(),
                  source.outputReference(),
                  source.objectLimit(),
                  false,
                  source.outputsLive(),
                  source.executorLive(),
                  source.expansionComplete(),
                  null,
                  source.outputReleaseAt()));
      JobRecord invalid =
          new JobRecord(
              source.input(),
              source.safety(),
              source.attempt(),
              source.lease(),
              source.leaseUntil(),
              source.stage(),
              source.inputReference(),
              source.outputReference(),
              source.objectLimit(),
              source.inputLive(),
              source.outputsLive(),
              source.executorLive(),
              source.expansionComplete(),
              0L,
              source.outputReleaseAt());
      FixedRecords.replace(
          connection,
          ResultFixture.configuration().files(),
          stored.slot(),
          FixedRecords.Kind.JOB,
          FixedRecords.key(
              fixture.context(),
              FixedRecords.Kind.JOB,
              ResultFixture.WORK.scope(),
              ResultFixture.WORK.producer(),
              ResultFixture.WORK.entity(),
              source.input().operation().bytes()),
          stored.geometry().revision(),
          invalid.encode(),
          false);
      statement.execute("COMMIT");
    }
    assertThrows(
        java.sql.SQLException.class,
        () ->
            SessionStore.open(
                directory.resolve("invalid-release-time.sqlite"), ResultFixture.configuration()));
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

  private static AdmissionStore.StoredJob job(ResultFixture fixture) throws Exception {
    try (var connection =
        BoundedSqlite.open(fixture.database, ResultFixture.configuration().files()).connect()) {
      return AdmissionStore.job(connection, fixture.binding, ResultFixture.WORK);
    }
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

  private static void assertCode(ProtocolError.Code expected, Throwing action) {
    ProtocolError error = assertThrows(ProtocolError.class, action::run);
    assertEquals(expected, error.code(), error::getMessage);
  }

  @FunctionalInterface
  private interface Throwing {
    void run() throws Exception;
  }
}
