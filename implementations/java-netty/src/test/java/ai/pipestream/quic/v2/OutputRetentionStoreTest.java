package ai.pipestream.quic.v2;

import static org.junit.jupiter.api.Assertions.*;

import ai.pipestream.quic.BoundedSqlite;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

@Timeout(30)
final class OutputRetentionStoreTest {
  private static final byte[] INPUT = {9, 8, 7};
  private static final byte[] OUTPUT = {1, 2, 3, 4};

  @TempDir Path directory;

  @Test
  void outputFundingReclaimsExactlyAtExpiryAndPreservesInputManifestAndReceipt() throws Exception {
    try (ResultFixture fixture = new ResultFixture(directory, "release", INPUT, OUTPUT)) {
      InputStore.Usage retained = fixture.inputs.usage();
      Records.WorkView published = fixture.published;
      long inputBytes = Files.size(onlyInput(fixture.inputsPath));
      long credits = job(fixture).geometry().credits();
      assertEquals(
          RetentionStore.Result.NOT_READY,
          fixture.sessions.reclaimOutput(
              fixture.binding.generation(),
              ResultFixture.WORK,
              fixture.inputs,
              ResultFixture.clock(21_199)));
      assertNull(job(fixture).record().outputReleaseAt());
      assertEquals(retained, fixture.inputs.usage());

      assertEquals(
          RetentionStore.Result.RELEASED,
          fixture.sessions.reclaimOutput(
              fixture.binding.generation(),
              ResultFixture.WORK,
              fixture.inputs,
              ResultFixture.clock(21_200)));
      assertEquals(new InputStore.Usage(inputBytes, 1, 0), fixture.inputs.usage());
      assertEquals(3, retained.files() - fixture.inputs.usage().files());
      assertTrue(fixture.inputs.find(fixture.context(), fixture.inputHeader).isPresent());
      assertTrue(fixture.inputs.findReservation(fixture.context(), fixture.inputHeader).isEmpty());
      assertThrows(
          IOException.class,
          () ->
              fixture.inputs.findOutput(fixture.context(), fixture.inputHeader, fixture.lease, 0));
      assertEquals(published, current(fixture));
      assertEquals(21_200L, job(fixture).record().outputReleaseAt());
      assertFalse(job(fixture).record().outputsLive());
      assertEquals(credits - 2, job(fixture).geometry().credits());
      assertEquals(
          RetentionStore.Result.ALREADY_RELEASED,
          fixture.sessions.reclaimOutput(
              fixture.binding.generation(),
              ResultFixture.WORK,
              fixture.inputs,
              ResultFixture.clock(21_200)));

      fixture.reopen();
      assertEquals(new InputStore.Usage(inputBytes, 1, 0), fixture.inputs.usage());
      assertEquals(published, current(fixture));
      assertEquals(21_200L, job(fixture).record().outputReleaseAt());
      assertFalse(job(fixture).record().outputsLive());
    }
  }

  @Test
  void outputReaderAtEofBlocksBeforeIntentUntilDescriptorCloses() throws Exception {
    try (ResultFixture fixture = new ResultFixture(directory, "reader", INPUT, OUTPUT)) {
      InputStore.Usage retained = fixture.inputs.usage();
      OutputStore.Stored output =
          fixture
              .inputs
              .findOutput(fixture.context(), fixture.inputHeader, fixture.lease, 0)
              .orElseThrow();
      try (InputStream reader = output.openStream()) {
        assertArrayEquals(OUTPUT, reader.readAllBytes());
        assertEquals(-1, reader.read());
        assertEquals(
            RetentionStore.Result.PINNED,
            fixture.sessions.reclaimOutput(
                fixture.binding.generation(),
                ResultFixture.WORK,
                fixture.inputs,
                ResultFixture.clock(21_200)));
        assertNull(job(fixture).record().outputReleaseAt());
        assertTrue(job(fixture).record().outputsLive());
        assertEquals(retained.bytes(), fixture.inputs.usage().bytes());
        assertEquals(retained.files(), fixture.inputs.usage().files());
      }
      assertEquals(
          RetentionStore.Result.RELEASED,
          fixture.sessions.reclaimOutput(
              fixture.binding.generation(),
              ResultFixture.WORK,
              fixture.inputs,
              ResultFixture.clock(21_200)));
    }
  }

  @Test
  void reservedWriterAndReaderCreditsBlockWithoutCreatingReleaseIntent() throws Exception {
    try (ResultFixture writerFixture =
            new ResultFixture(directory, "writer-credit", INPUT, OUTPUT);
        OutputStore.WriterCredit credit =
            writerFixture.inputs.reserveOutputWriter(
                writerFixture.context(), writerFixture.inputHeader, writerFixture.lease)) {
      assertNotNull(credit);
      assertEquals(
          RetentionStore.Result.PINNED,
          writerFixture.sessions.reclaimOutput(
              writerFixture.binding.generation(),
              ResultFixture.WORK,
              writerFixture.inputs,
              ResultFixture.clock(21_200)));
      assertNull(job(writerFixture).record().outputReleaseAt());
      assertTrue(job(writerFixture).record().outputsLive());
    }

    try (ResultFixture readerFixture =
            new ResultFixture(directory, "reader-credit", INPUT, OUTPUT);
        OutputStore.ReaderCredit credit = readerFixture.inputs.reserveOutputReader();
        InputStream reader =
            readerFixture
                .inputs
                .findOutput(
                    readerFixture.context(), readerFixture.inputHeader, readerFixture.lease, 0)
                .orElseThrow()
                .openStream(credit)) {
      assertNotNull(credit);
      assertEquals(OUTPUT[0], reader.read());
      assertEquals(
          RetentionStore.Result.PINNED,
          readerFixture.sessions.reclaimOutput(
              readerFixture.binding.generation(),
              ResultFixture.WORK,
              readerFixture.inputs,
              ResultFixture.clock(21_200)));
      assertNull(job(readerFixture).record().outputReleaseAt());
      assertTrue(job(readerFixture).record().outputsLive());
    }

    try (ResultFixture unborrowed = new ResultFixture(directory, "unborrowed", INPUT, OUTPUT);
        OutputStore.ReaderCredit credit = unborrowed.inputs.reserveOutputReader()) {
      assertNotNull(credit);
      assertEquals(
          RetentionStore.Result.RELEASED,
          unborrowed.sessions.reclaimOutput(
              unborrowed.binding.generation(),
              ResultFixture.WORK,
              unborrowed.inputs,
              ResultFixture.clock(21_200)));
    }
  }

  @Test
  void zeroOutputManifestReleasesItsExactPrepaidFundingRecord() throws Exception {
    try (ResultFixture fixture =
        new ResultFixture(directory, "zero-output", INPUT, new byte[0], 0)) {
      InputStore.Usage retained = fixture.inputs.usage();
      long inputBytes = Files.size(onlyInput(fixture.inputsPath));
      long fundingBytes = Files.size(onlyFunding(fixture.inputsPath));
      assertTrue(fixture.published.manifest().outputs().isEmpty());
      assertEquals(2, retained.files());

      assertEquals(
          RetentionStore.Result.RELEASED,
          fixture.sessions.reclaimOutput(
              fixture.binding.generation(),
              ResultFixture.WORK,
              fixture.inputs,
              ResultFixture.clock(21_200)));
      assertEquals(new InputStore.Usage(inputBytes, 1, 0), fixture.inputs.usage());
      assertEquals(fundingBytes, retained.bytes() - inputBytes);
      assertTrue(fixture.inputs.findReservation(fixture.context(), fixture.inputHeader).isEmpty());
      assertTrue(Files.isDirectory(fixture.inputsPath.resolve("outputs")));
      assertDirectoryEmpty(fixture.inputsPath.resolve("outputs"));
      assertEquals(fixture.published, current(fixture));
    }
  }

  @Test
  void synchronizedFundingRemovalFailureRetriesWithoutEarlyRefund() throws Exception {
    try (ResultFixture fixture = new ResultFixture(directory, "sync-retry", INPUT, OUTPUT)) {
      InputStore.Usage retained = fixture.inputs.usage();
      long inputBytes = Files.size(onlyInput(fixture.inputsPath));
      java.util.concurrent.atomic.AtomicBoolean interrupt =
          new java.util.concurrent.atomic.AtomicBoolean(true);
      fixture.inputs.close();
      fixture.inputs =
          InputStore.open(
              fixture.inputsPath,
              ResultFixture.INPUT_LIMITS,
              phase -> {
                if (phase == InputStore.Phase.OUTPUT_FUNDING_SYNCED && interrupt.getAndSet(false)) {
                  throw new IOException("test output funding sync interruption");
                }
              });
      fixture.sessions.verifyInputs(fixture.inputs);

      IOException failure =
          assertThrows(
              IOException.class,
              () ->
                  fixture.sessions.reclaimOutput(
                      fixture.binding.generation(),
                      ResultFixture.WORK,
                      fixture.inputs,
                      ResultFixture.clock(21_200)));
      assertEquals("test output funding sync interruption", failure.getMessage());
      assertEquals(retained, fixture.inputs.usage());
      assertEquals(21_200L, job(fixture).record().outputReleaseAt());
      assertTrue(job(fixture).record().outputsLive());
      assertTrue(fixture.inputs.findReservation(fixture.context(), fixture.inputHeader).isEmpty());

      assertEquals(
          RetentionStore.Result.RELEASED,
          fixture.sessions.reclaimOutput(
              fixture.binding.generation(),
              ResultFixture.WORK,
              fixture.inputs,
              ResultFixture.clock(21_200)));
      assertEquals(new InputStore.Usage(inputBytes, 1, 0), fixture.inputs.usage());
    }
  }

  @Test
  void inputAndOutputReleaseInEitherOrderPreservesEvidenceAndSpendsFourCredits() throws Exception {
    assertCombinedRelease("input-first", true);
    assertCombinedRelease("output-first", false);
  }

  @Test
  void missingPromisedOutputBeforeIntentRefusesWithoutRefundOrEvidence() throws Exception {
    try (ResultFixture fixture = new ResultFixture(directory, "missing", INPUT, OUTPUT)) {
      InputStore.Usage retained = fixture.inputs.usage();
      Files.delete(onlyOutput(fixture.inputsPath));
      assertThrows(
          IOException.class,
          () ->
              fixture.sessions.reclaimOutput(
                  fixture.binding.generation(),
                  ResultFixture.WORK,
                  fixture.inputs,
                  ResultFixture.clock(21_200)));
      assertEquals(retained, fixture.inputs.usage());
      assertNull(job(fixture).record().outputReleaseAt());
      assertTrue(job(fixture).record().outputsLive());
      assertEquals(fixture.published, current(fixture));
    }
  }

  @Test
  void unsafeOrRegressingClockCannotCreateIntentOrRemoveOutputs() throws Exception {
    try (ResultFixture fixture = new ResultFixture(directory, "clock", INPUT, OUTPUT)) {
      InputStore.Usage retained = fixture.inputs.usage();
      assertCode(
          ProtocolError.Code.CLOCK_UNSAFE,
          () ->
              fixture.sessions.reclaimOutput(
                  fixture.binding.generation(),
                  ResultFixture.WORK,
                  fixture.inputs,
                  () -> new AdmissionStore.Time(21_200, false)));
      assertEquals(retained, fixture.inputs.usage());
      assertNull(job(fixture).record().outputReleaseAt());

      java.util.concurrent.atomic.AtomicInteger samples =
          new java.util.concurrent.atomic.AtomicInteger();
      assertCode(
          ProtocolError.Code.CLOCK_UNSAFE,
          () ->
              fixture.sessions.reclaimOutput(
                  fixture.binding.generation(),
                  ResultFixture.WORK,
                  fixture.inputs,
                  () ->
                      new AdmissionStore.Time(
                          samples.incrementAndGet() == 1 ? 21_300 : 21_299, true)));
      assertEquals(retained, fixture.inputs.usage());
      assertNull(job(fixture).record().outputReleaseAt());
      assertTrue(
          fixture
              .inputs
              .findOutput(fixture.context(), fixture.inputHeader, fixture.lease, 0)
              .isPresent());
    }
  }

  @Test
  void recoveryRejectsChecksummedOutputReleaseBeforeExternalExpiry() throws Exception {
    try (ResultFixture fixture = new ResultFixture(directory, "bad-time", INPUT, OUTPUT);
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
                  source.inputLive(),
                  false,
                  source.executorLive(),
                  source.expansionComplete(),
                  source.inputReleaseAt(),
                  null));
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
              source.inputReleaseAt(),
              1200L);
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
            SessionStore.open(directory.resolve("bad-time.sqlite"), ResultFixture.configuration()));
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

  private void assertCombinedRelease(String name, boolean inputFirst) throws Exception {
    try (ResultFixture fixture = new ResultFixture(directory, name, INPUT, OUTPUT)) {
      long credits = job(fixture).geometry().credits();
      if (inputFirst) {
        assertEquals(
            RetentionStore.Result.RELEASED,
            fixture.sessions.reclaimInput(
                fixture.binding.generation(),
                ResultFixture.WORK,
                fixture.inputs,
                ResultFixture.clock(31_200)));
        assertEquals(
            RetentionStore.Result.RELEASED,
            fixture.sessions.reclaimOutput(
                fixture.binding.generation(),
                ResultFixture.WORK,
                fixture.inputs,
                ResultFixture.clock(31_200)));
      } else {
        assertEquals(
            RetentionStore.Result.RELEASED,
            fixture.sessions.reclaimOutput(
                fixture.binding.generation(),
                ResultFixture.WORK,
                fixture.inputs,
                ResultFixture.clock(31_200)));
        assertEquals(
            RetentionStore.Result.RELEASED,
            fixture.sessions.reclaimInput(
                fixture.binding.generation(),
                ResultFixture.WORK,
                fixture.inputs,
                ResultFixture.clock(31_200)));
      }
      assertEquals(new InputStore.Usage(0, 0, 0), fixture.inputs.usage());
      JobRecord released = job(fixture).record();
      assertEquals(31_200L, released.inputReleaseAt());
      assertEquals(31_200L, released.outputReleaseAt());
      assertFalse(released.inputLive());
      assertFalse(released.outputsLive());
      assertEquals(credits - 4, job(fixture).geometry().credits());
      assertEquals(fixture.published, current(fixture));

      fixture.reopen();
      assertEquals(new InputStore.Usage(0, 0, 0), fixture.inputs.usage());
      assertEquals(31_200L, job(fixture).record().inputReleaseAt());
      assertEquals(31_200L, job(fixture).record().outputReleaseAt());
      assertEquals(fixture.published, current(fixture));
    }
  }

  private static AdmissionStore.StoredJob job(ResultFixture fixture) throws Exception {
    try (var connection =
        BoundedSqlite.open(fixture.database, ResultFixture.configuration().files()).connect()) {
      return AdmissionStore.job(connection, fixture.binding, ResultFixture.WORK);
    }
  }

  private static Path onlyOutput(Path root) throws Exception {
    try (var entries = Files.newDirectoryStream(root.resolve("outputs"), "*.output")) {
      var iterator = entries.iterator();
      assertTrue(iterator.hasNext());
      Path result = iterator.next();
      assertFalse(iterator.hasNext());
      return result;
    }
  }

  private static Path onlyInput(Path root) throws Exception {
    return only(root.resolve("objects"), "*.input");
  }

  private static Path onlyFunding(Path root) throws Exception {
    return only(root.resolve("reservations"), "*.funding");
  }

  private static Path only(Path directory, String glob) throws Exception {
    try (var entries = Files.newDirectoryStream(directory, glob)) {
      var iterator = entries.iterator();
      assertTrue(iterator.hasNext());
      Path result = iterator.next();
      assertFalse(iterator.hasNext());
      return result;
    }
  }

  private static void assertDirectoryEmpty(Path directory) throws Exception {
    try (var entries = Files.newDirectoryStream(directory)) {
      assertFalse(entries.iterator().hasNext());
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
