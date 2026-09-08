package ai.pipestream.quic.v2;

import static ai.pipestream.quic.v2.Messages.DURABLE_WORK;
import static ai.pipestream.quic.v2.Messages.RESULT_DELIVERY;
import static org.junit.jupiter.api.Assertions.*;

import ai.pipestream.quic.BoundedSqlite;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

@Timeout(30)
final class ResultStoreTest {
  private static final ResultStore.Authorization ALLOW = (binding, work) -> {};
  private static final byte[] PAYLOAD = {1, 2, 3, 4, 5};

  @TempDir Path directory;

  @Test
  void manifestReturnsExactImmutablePublicationWithoutConsultingTime() throws Exception {
    try (ResultFixture fixture = new ResultFixture(directory, "manifest", PAYLOAD)) {
      AtomicInteger checks = new AtomicInteger();
      Messages.ManifestResponse response =
          fixture.sessions.manifest(
              ResultFixture.sessionAccess("alice"),
              ResultFixture.SELECTED,
              fixture.binding.generation(),
              new Messages.GetManifest(10, ResultFixture.WORK, 1),
              (binding, work) -> checks.incrementAndGet());
      assertEquals(2, checks.get());
      assertEquals(10, response.request());
      assertEquals(fixture.published.manifest(), response.manifest());
      assertEquals(fixture.payloadDigest, response.manifest().outputs().get(0).sha256());
      assertEquals(21_200, response.manifest().availableUntil());

      fixture.reopen();
      assertEquals(
          response.manifest(),
          fixture
              .sessions
              .manifest(
                  ResultFixture.sessionAccess("alice"),
                  ResultFixture.SELECTED,
                  fixture.binding.generation(),
                  new Messages.GetManifest(11, ResultFixture.WORK, 1),
                  ALLOW)
              .manifest());
    }
  }

  @Test
  void exactReadPinsBytesReturnsCommittedHeaderAndReleasesHandle() throws Exception {
    try (ResultFixture fixture = new ResultFixture(directory, "read", PAYLOAD)) {
      assertEquals(0, fixture.inputs.usage().handles());
      Messages.Read request =
          new Messages.Read(20, ResultFixture.WORK, 1, 0, fixture.payloadDigest);
      try (ResultStore.Opened opened =
          fixture.sessions.openResult(
              ResultFixture.sessionAccess("alice"),
              ResultFixture.SELECTED,
              fixture.binding.generation(),
              fixture.inputs,
              request,
              ResultFixture.clock(1300),
              ALLOW,
              () -> {})) {
        assertEquals(
            new Records.ResultHeader(
                20,
                fixture.binding.generation(),
                ResultFixture.WORK,
                1,
                0,
                PAYLOAD.length,
                fixture.payloadDigest),
            opened.header());
        assertEquals(1, fixture.inputs.usage().handles());
        assertArrayEquals(PAYLOAD, opened.reader().readAllBytes());
      }
      assertEquals(0, fixture.inputs.usage().handles());
      assertEquals(Records.State.SUCCEEDED, fixture.published.state());

      fixture.reopen();
      try (ResultStore.Opened opened =
          fixture.sessions.openResult(
              ResultFixture.sessionAccess("alice"),
              ResultFixture.SELECTED,
              fixture.binding.generation(),
              fixture.inputs,
              new Messages.Read(21, ResultFixture.WORK, 1, 0, fixture.payloadDigest),
              ResultFixture.clock(1300),
              ALLOW,
              () -> {})) {
        assertArrayEquals(PAYLOAD, opened.reader().readAllBytes());
      }
    }
  }

  @Test
  void emptyPublishedObjectReturnsImmediateVerifiedEof() throws Exception {
    try (ResultFixture fixture = new ResultFixture(directory, "empty", new byte[0]);
        ResultStore.Opened opened =
            fixture.sessions.openResult(
                ResultFixture.sessionAccess("alice"),
                ResultFixture.SELECTED,
                fixture.binding.generation(),
                fixture.inputs,
                new Messages.Read(25, ResultFixture.WORK, 1, 0, fixture.payloadDigest),
                ResultFixture.clock(1300),
                ALLOW,
                () -> {})) {
      assertEquals(0, opened.header().length());
      assertEquals(-1, opened.reader().read(new byte[1]));
    }
  }

  @Test
  void ownerAndResultAuthorizationPrecedeRetainedResultDisclosure() throws Exception {
    try (ResultFixture fixture = new ResultFixture(directory, "authorization", PAYLOAD)) {
      AtomicInteger resultChecks = new AtomicInteger();
      assertCode(
          ProtocolError.Code.UNAUTHORIZED,
          () ->
              fixture.sessions.manifest(
                  ResultFixture.sessionAccess("mallory"),
                  ResultFixture.SELECTED,
                  fixture.binding.generation(),
                  new Messages.GetManifest(30, ResultFixture.WORK, 99),
                  (binding, work) -> resultChecks.incrementAndGet()));
      assertEquals(0, resultChecks.get());

      ResultStore.Authorization denied =
          (binding, work) -> {
            throw new ProtocolError(ProtocolError.Code.UNAUTHORIZED, "result permission denied");
          };
      assertCode(
          ProtocolError.Code.UNAUTHORIZED,
          () ->
              fixture.sessions.openResult(
                  ResultFixture.sessionAccess("alice"),
                  ResultFixture.SELECTED,
                  fixture.binding.generation(),
                  fixture.inputs,
                  new Messages.Read(31, ResultFixture.WORK, 99, 0, fixture.payloadDigest),
                  ResultFixture.clock(0),
                  denied,
                  () -> {}));
      assertEquals(0, fixture.inputs.usage().handles());
    }
  }

  @Test
  void wrongAttemptIndexAndDigestReturnExactNamedRefusalsWithoutPins() throws Exception {
    try (ResultFixture fixture = new ResultFixture(directory, "identity", PAYLOAD)) {
      InputStore.Usage retained = fixture.inputs.usage();
      assertCode(
          ProtocolError.Code.NOT_FOUND,
          () ->
              fixture.sessions.manifest(
                  ResultFixture.sessionAccess("alice"),
                  ResultFixture.SELECTED,
                  1,
                  new Messages.GetManifest(40, ResultFixture.WORK, 2),
                  ALLOW));
      assertReadCode(
          fixture,
          ProtocolError.Code.NOT_FOUND,
          new Messages.Read(41, ResultFixture.WORK, 1, 1, fixture.payloadDigest));
      assertReadCode(
          fixture,
          ProtocolError.Code.INTEGRITY_ERROR,
          new Messages.Read(42, ResultFixture.WORK, 1, 0, ResultFixture.digest(new byte[] {9})));
      assertReadCode(
          fixture,
          ProtocolError.Code.NOT_FOUND,
          new Messages.Read(43, ResultFixture.WORK, 2, 0, fixture.payloadDigest));
      assertEquals(0, fixture.inputs.usage().handles());
      assertEquals(retained, fixture.inputs.usage());
    }
  }

  @Test
  void realAdmittedButUnpublishedWorkIsNotReadyAndProfilesBoundRepresentation() throws Exception {
    try (ResultFixture fixture = new ResultFixture(directory, "unpublished", PAYLOAD, false)) {
      InputStore.Usage retained = fixture.inputs.usage();
      assertCode(
          ProtocolError.Code.NOT_READY,
          () ->
              fixture.sessions.manifest(
                  ResultFixture.sessionAccess("alice"),
                  ResultFixture.SELECTED,
                  1,
                  new Messages.GetManifest(45, ResultFixture.WORK, 1),
                  ALLOW));
      assertEquals(retained, fixture.inputs.usage());
    }

    try (ResultFixture fixture = new ResultFixture(directory, "profile", PAYLOAD)) {
      Messages.Read request =
          new Messages.Read(46, ResultFixture.WORK, 1, 0, fixture.payloadDigest);
      InputStore.Usage retained = fixture.inputs.usage();
      assertCode(
          ProtocolError.Code.EXTENSION_UNSUPPORTED,
          () ->
              fixture.sessions.openResult(
                  ResultFixture.sessionAccess("alice"),
                  capabilities(false, 1 << 20),
                  1,
                  fixture.inputs,
                  request,
                  ResultFixture.clock(1300),
                  ALLOW,
                  () -> {}));
      assertCode(
          ProtocolError.Code.LIMIT_EXCEEDED,
          () ->
              fixture.sessions.openResult(
                  ResultFixture.sessionAccess("alice"),
                  capabilities(true, PAYLOAD.length - 1),
                  1,
                  fixture.inputs,
                  request,
                  ResultFixture.clock(1300),
                  ALLOW,
                  () -> {}));
      assertEquals(retained, fixture.inputs.usage());
    }
  }

  @Test
  void expiryAndFinalClockChecksClosePinsAndLeavePublishedOutcomeUnchanged() throws Exception {
    try (ResultFixture fixture = new ResultFixture(directory, "time", PAYLOAD)) {
      Records.WorkView retained = fixture.published;
      InputStore.Usage usage = fixture.inputs.usage();
      Messages.Read request =
          new Messages.Read(50, ResultFixture.WORK, 1, 0, fixture.payloadDigest);
      assertCode(
          ProtocolError.Code.EXPIRED,
          () ->
              fixture.sessions.openResult(
                  ResultFixture.sessionAccess("alice"),
                  ResultFixture.SELECTED,
                  1,
                  fixture.inputs,
                  request,
                  ResultFixture.clock(21_200),
                  ALLOW,
                  () -> {}));

      AtomicLong now = new AtomicLong(1300);
      AtomicInteger checks = new AtomicInteger();
      ResultStore.Authorization expireAtFinal =
          (binding, work) -> {
            if (checks.incrementAndGet() == 2) now.set(21_200);
          };
      assertCode(
          ProtocolError.Code.EXPIRED,
          () ->
              fixture.sessions.openResult(
                  ResultFixture.sessionAccess("alice"),
                  ResultFixture.SELECTED,
                  1,
                  fixture.inputs,
                  request,
                  () -> new AdmissionStore.Time(now.get(), true),
                  expireAtFinal,
                  () -> {}));
      assertEquals(0, fixture.inputs.usage().handles());

      AtomicBoolean trusted = new AtomicBoolean(true);
      AtomicInteger trustChecks = new AtomicInteger();
      ResultStore.Authorization loseTrust =
          (binding, work) -> {
            if (trustChecks.incrementAndGet() == 2) trusted.set(false);
          };
      assertCode(
          ProtocolError.Code.CLOCK_UNSAFE,
          () ->
              fixture.sessions.openResult(
                  ResultFixture.sessionAccess("alice"),
                  ResultFixture.SELECTED,
                  1,
                  fixture.inputs,
                  request,
                  () -> new AdmissionStore.Time(1500, trusted.get()),
                  loseTrust,
                  () -> {}));

      AtomicLong regressed = new AtomicLong(1400);
      AtomicInteger regressionChecks = new AtomicInteger();
      ResultStore.Authorization regressAtFinal =
          (binding, work) -> {
            if (regressionChecks.incrementAndGet() == 2) regressed.set(1399);
          };
      assertCode(
          ProtocolError.Code.CLOCK_UNSAFE,
          () ->
              fixture.sessions.openResult(
                  ResultFixture.sessionAccess("alice"),
                  ResultFixture.SELECTED,
                  1,
                  fixture.inputs,
                  request,
                  () -> new AdmissionStore.Time(regressed.get(), true),
                  regressAtFinal,
                  () -> {}));
      assertEquals(
          retained,
          fixture
              .sessions
              .snapshot(
                  ResultFixture.sessionAccess("alice"),
                  ResultFixture.SELECTED,
                  1,
                  new Messages.Watch(51, ResultFixture.WORK, 0, 0))
              .work());
      assertEquals(usage, fixture.inputs.usage());
      assertEquals(
          retained.manifest(),
          fixture
              .sessions
              .manifest(
                  ResultFixture.sessionAccess("alice"),
                  ResultFixture.SELECTED,
                  1,
                  new Messages.GetManifest(52, ResultFixture.WORK, 1),
                  ALLOW)
              .manifest());
    }
  }

  @Test
  void finalCheckFailureRollsBackWatermarkAndSuccessfulReadAdvancesIt() throws Exception {
    try (ResultFixture fixture = new ResultFixture(directory, "final-check", PAYLOAD)) {
      Records.WorkView before = fixture.published;
      InputStore.Usage usage = fixture.inputs.usage();
      assertEquals(1200, watermark(fixture));
      Messages.Read request =
          new Messages.Read(60, ResultFixture.WORK, 1, 0, fixture.payloadDigest);
      IllegalStateException denied =
          assertThrows(
              IllegalStateException.class,
              () ->
                  fixture.sessions.openResult(
                      ResultFixture.sessionAccess("alice"),
                      ResultFixture.SELECTED,
                      1,
                      fixture.inputs,
                      request,
                      ResultFixture.clock(1500),
                      ALLOW,
                      () -> {
                        throw new IllegalStateException("delivery lifetime exhausted");
                      }));
      assertEquals("delivery lifetime exhausted", denied.getMessage());
      assertEquals(1200, watermark(fixture));
      assertEquals(before, current(fixture));
      assertEquals(usage, fixture.inputs.usage());

      AtomicLong now = new AtomicLong(1500);
      AtomicInteger checks = new AtomicInteger();
      try (ResultStore.Opened opened =
          fixture.sessions.openResult(
              ResultFixture.sessionAccess("alice"),
              ResultFixture.SELECTED,
              1,
              fixture.inputs,
              request,
              () -> new AdmissionStore.Time(now.get(), true),
              (binding, work) -> {
                if (checks.incrementAndGet() == 2) now.set(1600);
              },
              () -> {})) {
        assertArrayEquals(PAYLOAD, opened.reader().readAllBytes());
      }
      assertEquals(1600, watermark(fixture));
      assertEquals(before, current(fixture));
      assertCode(
          ProtocolError.Code.CLOCK_UNSAFE,
          () ->
              fixture.sessions.openResult(
                  ResultFixture.sessionAccess("alice"),
                  ResultFixture.SELECTED,
                  1,
                  fixture.inputs,
                  request,
                  ResultFixture.clock(1499),
                  ALLOW,
                  () -> {}));
      assertEquals(usage, fixture.inputs.usage());
    }
  }

  @Test
  void missingOrCorruptPublishedObjectIsUnavailableWithoutChangingManifest() throws Exception {
    for (String mode : new String[] {"missing", "corrupt"}) {
      try (ResultFixture fixture = new ResultFixture(directory, mode, PAYLOAD)) {
        InputStore.Usage retained = fixture.inputs.usage();
        Path object;
        try (var entries =
            Files.newDirectoryStream(fixture.inputsPath.resolve("outputs"), "*.output")) {
          var iterator = entries.iterator();
          assertTrue(iterator.hasNext());
          object = iterator.next();
          assertFalse(iterator.hasNext());
        }
        if (mode.equals("missing")) Files.delete(object);
        else Files.write(object, new byte[] {9}, StandardOpenOption.TRUNCATE_EXISTING);
        assertReadCode(
            fixture,
            ProtocolError.Code.OUTPUT_UNAVAILABLE,
            new Messages.Read(70, ResultFixture.WORK, 1, 0, fixture.payloadDigest));
        assertEquals(
            fixture.published.manifest(),
            fixture
                .sessions
                .manifest(
                    ResultFixture.sessionAccess("alice"),
                    ResultFixture.SELECTED,
                    1,
                    new Messages.GetManifest(71, ResultFixture.WORK, 1),
                    ALLOW)
                .manifest());
        assertEquals(retained, fixture.inputs.usage());
      }
    }
  }

  @Test
  void liveReadRecheckUsesCurrentAuthorizationWithoutResamplingUtc() throws Exception {
    try (ResultFixture fixture = new ResultFixture(directory, "recheck", PAYLOAD)) {
      AtomicInteger checks = new AtomicInteger();
      fixture.sessions.checkResultRead(
          ResultFixture.sessionAccess("alice"),
          ResultFixture.SELECTED,
          1,
          ResultFixture.WORK,
          (binding, work) -> checks.incrementAndGet());
      assertEquals(1, checks.get());
      assertCode(
          ProtocolError.Code.UNAUTHORIZED,
          () ->
              fixture.sessions.checkResultRead(
                  ResultFixture.sessionAccess("alice"),
                  ResultFixture.SELECTED,
                  1,
                  ResultFixture.WORK,
                  (binding, work) -> {
                    throw new ProtocolError(ProtocolError.Code.UNAUTHORIZED, "withdrawn");
                  }));
    }
  }

  private static void assertReadCode(
      ResultFixture fixture, ProtocolError.Code code, Messages.Read request) {
    assertCode(
        code,
        () ->
            fixture.sessions.openResult(
                ResultFixture.sessionAccess("alice"),
                ResultFixture.SELECTED,
                fixture.binding.generation(),
                fixture.inputs,
                request,
                ResultFixture.clock(1300),
                ALLOW,
                () -> {}));
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

  private static long watermark(ResultFixture fixture) throws Exception {
    try (var connection =
        BoundedSqlite.open(fixture.database, ResultFixture.configuration().files()).connect()) {
      return AdmissionStore.watermark(connection, fixture.binding.authority());
    }
  }

  private static Messages.Capabilities capabilities(boolean results, int objectLimit) {
    return new Messages.Capabilities(
        true,
        results ? List.of(DURABLE_WORK, RESULT_DELIVERY) : List.of(DURABLE_WORK),
        List.of(),
        1 << 20,
        8,
        16,
        objectLimit,
        1000,
        5000);
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
