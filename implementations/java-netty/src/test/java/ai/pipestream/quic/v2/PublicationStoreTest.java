package ai.pipestream.quic.v2;

import static ai.pipestream.quic.v2.Messages.DURABLE_WORK;
import static ai.pipestream.quic.v2.Messages.RESULT_DELIVERY;
import static org.junit.jupiter.api.Assertions.*;

import ai.pipestream.quic.BoundedSqlite;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

@Timeout(20)
final class PublicationStoreTest {
  private static final InputStore.Limits INPUT_LIMITS =
      new InputStore.Limits(16L << 20, 128, 1 << 20, 8);
  private static final Records.WorkKey WORK = new Records.WorkKey(0, 0, 1);
  private static final AdmissionStore.Authorization ALLOW = (binding, parameters) -> {};
  private static final PublicationStore.Endpoint ENDPOINT =
      new PublicationStore.Endpoint("results.example:7443");

  @TempDir Path directory;

  @Test
  void nonemptyOutputsCommitExactManifestAndSurviveBothStoreReopens() throws Exception {
    Fixture fixture = fixture("nonempty", true, 2, 9);
    byte[][] payloads = {new byte[] {1, 2, 3, 4}, new byte[] {5, 6, 7, 8, 9}};
    Records.WorkView succeeded;
    ExecutionStore.Lease lease;
    InputStore.Usage funded;
    try (InputStore inputs = fixture.inputs()) {
      assertNotNull(inputs.identity());
      lease = claim(fixture, 1100);
      for (int index = 0; index < payloads.length; index++)
        writeOutput(fixture, lease, index, payloads[index], "application/octet-stream");
      funded = inputs.usage();
      succeeded =
          fixture
              .sessions()
              .succeedExecution(execAccess(), lease, inputs, 2, ENDPOINT, clock(1200), ALLOW);
      assertEquals(Records.State.SUCCEEDED, succeeded.state());
      Records.Manifest manifest = succeeded.manifest();
      assertNotNull(manifest);
      assertEquals("issuer-a", manifest.authority());
      assertEquals("alice", manifest.owner());
      assertEquals(1, manifest.generation());
      assertEquals(WORK, manifest.work());
      assertEquals(1, manifest.attempt());
      assertEquals(fixture.header().parameters().input().sha256(), manifest.inputSha256());
      assertEquals(1200, manifest.committedAt());
      assertEquals(21_200, manifest.availableUntil());
      assertEquals(2, manifest.outputs().size());
      for (int index = 0; index < payloads.length; index++) {
        Records.Output output = manifest.outputs().get(index);
        assertEquals(index, output.index());
        assertEquals(payloads[index].length, output.length());
        assertEquals(digest(payloads[index]), output.sha256());
        assertEquals("application/octet-stream", output.contentType());
        assertTrue(output.locator().value().contains("results.example:7443"));
      }
      assertEquals(funded, inputs.usage(), "publication consumes retained funding, not new quota");
    }

    SessionStore reopened = SessionStore.open(fixture.database(), configuration(true));
    try (InputStore inputs = InputStore.open(fixture.inputsPath(), INPUT_LIMITS)) {
      reopened.verifyInputs(inputs);
      Records.WorkView retained = view(reopened, selected(true));
      assertEquals(succeeded, retained);
      assertEquals(funded, inputs.usage());
      for (int index = 0; index < payloads.length; index++) {
        OutputStore.Stored output =
            inputs.findOutput(context(), fixture.header(), lease, index).orElseThrow();
        assertEquals(index, output.index());
        assertEquals(payloads[index].length, output.length());
        assertEquals(digest(payloads[index]), output.sha256());
        try (InputStream stream = output.openStream()) {
          assertArrayEquals(payloads[index], stream.readAllBytes());
        }
      }
    }
  }

  @Test
  void zeroOutputSuccessDistinguishesResultsManifestFromDurableOnlyState() throws Exception {
    Fixture results = fixture("zero-results", true, 0, 0);
    try (InputStore inputs = results.inputs()) {
      assertNotNull(inputs.identity());
      Records.WorkView view =
          results
              .sessions()
              .succeedExecution(
                  execAccess(), claim(results, 1100), inputs, 0, ENDPOINT, clock(1200), ALLOW);
      Records.Manifest manifest = view.manifest();
      assertNotNull(manifest);
      assertTrue(manifest.outputs().isEmpty());
      assertEquals(21_200, view.outputUntil());
    }

    Fixture durable = fixture("zero-durable", false, 0, 0);
    try (InputStore inputs = durable.inputs()) {
      assertNotNull(inputs.identity());
      long[] before = credits(durable);
      Records.WorkView view =
          durable
              .sessions()
              .succeedExecution(
                  execAccess(), claim(durable, 1100), inputs, 0, ENDPOINT, clock(1200), ALLOW);
      assertEquals(Records.State.SUCCEEDED, view.state());
      assertNull(view.manifest());
      assertNull(view.outputUntil());
      long[] after = credits(durable);
      assertEquals(before[0] - 1, after[0]);
      assertEquals(before[1] - 1, after[1]);
    }
  }

  @Test
  void staleMissingAndFinalGateRefusalsLeaveNoSuccessManifest() throws Exception {
    Fixture fixture = fixture("refusals", true, 1, 4);
    try (InputStore inputs = fixture.inputs()) {
      assertNotNull(inputs.identity());
      ExecutionStore.Lease stale = claim(fixture, 1100);
      ExecutionStore.Lease current = claim(fixture, stale.until(), 400);
      assertEquals(2000, current.until());
      assertCode(
          ProtocolError.Code.CONFLICT,
          () ->
              fixture
                  .sessions()
                  .succeedExecution(execAccess(), stale, inputs, 0, ENDPOINT, clock(1700), ALLOW));
      assertCode(
          ProtocolError.Code.NOT_READY,
          () ->
              fixture
                  .sessions()
                  .succeedExecution(
                      execAccess(), current, inputs, 1, ENDPOINT, clock(1700), ALLOW));
      assertEquals(Records.State.ACTIVE, view(fixture.sessions(), selected(true)).state());

      writeOutput(fixture, current, 0, new byte[] {1, 2, 3, 4}, "application/octet-stream");
      AtomicLong now = new AtomicLong(1700);
      AtomicInteger checks = new AtomicInteger();
      AdmissionStore.Authorization advances =
          (binding, parameters) -> {
            if (checks.incrementAndGet() == 3) now.set(2000);
          };
      assertCode(
          ProtocolError.Code.DEADLINE_EXCEEDED,
          () ->
              fixture
                  .sessions()
                  .succeedExecution(
                      execAccess(),
                      current,
                      inputs,
                      1,
                      ENDPOINT,
                      () -> new AdmissionStore.Time(now.get(), true),
                      advances));
      assertEquals(Records.State.ACTIVE, view(fixture.sessions(), selected(true)).state());
    }
  }

  @Test
  void installedOutputSetCannotBeSilentlyPublishedAsAContiguousPrefix() throws Exception {
    Fixture fixture = fixture("prefix", true, 2, 8);
    try (InputStore inputs = fixture.inputs()) {
      assertNotNull(inputs.identity());
      ExecutionStore.Lease lease = claim(fixture, 1100);
      writeOutput(fixture, lease, 0, new byte[] {1, 2, 3, 4}, "application/octet-stream");
      writeOutput(fixture, lease, 1, new byte[] {5, 6, 7, 8}, "application/octet-stream");
      assertCode(
          ProtocolError.Code.CONFLICT,
          () ->
              fixture
                  .sessions()
                  .succeedExecution(execAccess(), lease, inputs, 1, ENDPOINT, clock(1200), ALLOW));
      assertEquals(Records.State.ACTIVE, view(fixture.sessions(), selected(true)).state());
    }
  }

  @Test
  void publicationForwardJumpPastOutputPromiseRollsBackBeforeReceiptOrExecutionDeadline()
      throws Exception {
    Fixture fixture = fixture("output-jump", true, 0, 0, 50_000);
    try (InputStore inputs = fixture.inputs()) {
      assertNotNull(inputs.identity());
      ExecutionStore.Lease lease = claim(fixture, 1100, 49_000);
      long[] before = credits(fixture);
      AtomicInteger samples = new AtomicInteger();
      AdmissionStore.Clock jumped =
          () -> new AdmissionStore.Time(samples.getAndIncrement() < 2 ? 1200 : 21_200, true);
      assertCode(
          ProtocolError.Code.CLOCK_UNSAFE,
          () ->
              fixture
                  .sessions()
                  .succeedExecution(execAccess(), lease, inputs, 0, ENDPOINT, jumped, ALLOW));
      assertEquals(Records.State.ACTIVE, view(fixture.sessions(), selected(true)).state());
      assertArrayEquals(before, credits(fixture));
    }
  }

  private Fixture fixture(String name, boolean results, int outputs, long outputBytes)
      throws Exception {
    return fixture(name, results, outputs, outputBytes, 1000);
  }

  private Fixture fixture(
      String name, boolean results, int outputs, long outputBytes, long executionMs)
      throws Exception {
    Path database = directory.resolve(name + ".sqlite");
    Path inputsPath = directory.resolve(name + "-inputs");
    SessionStore sessions = SessionStore.initialize(database, configuration(results));
    Messages.Capabilities selected = selected(results);
    Messages.Binding binding =
        sessions.create(
            sessionAccess(),
            selected,
            new Messages.Create(1, 1, new Records.Policy(60_000, 20_000, 30_000)));
    sessions.declare(
        sessionAccess(), selected, 1, new Messages.Declare(2, operation(1), 0, List.of(1L), false));
    InputStore inputs =
        InputStore.initializeForAuthority(inputsPath, INPUT_LIMITS, sessions.identity());
    sessions.bindInputs(inputs);
    Records.InputHeader header = header(outputs, outputBytes, executionMs);
    try (InputStore.Receiver receiver = inputs.begin(context(), header, selected, 1)) {
      receiver.write(ByteBuffer.allocate(0), 2);
      receiver.finish(3);
    }
    sessions.admit(sessionAccess(), selected, 1, inputs, header, 2, clock(1000), ALLOW);
    return new Fixture(database, inputsPath, sessions, inputs, binding, header, selected);
  }

  private static ExecutionStore.Lease claim(Fixture fixture, long now) throws Exception {
    return claim(fixture, now, 500);
  }

  private static ExecutionStore.Lease claim(Fixture fixture, long now, long duration)
      throws Exception {
    return fixture
        .sessions()
        .claimExecution(execAccess(), 1, WORK, fixture.inputs(), duration, clock(now), ALLOW);
  }

  private static void writeOutput(
      Fixture fixture, ExecutionStore.Lease lease, int index, byte[] payload, String contentType)
      throws Exception {
    try (OutputStore.Writer writer =
        fixture
            .inputs()
            .beginOutput(
                context(),
                fixture.header(),
                lease,
                index,
                payload.length,
                contentType,
                Math.min(
                    fixture.selected().objectLimit(),
                    fixture.header().parameters().outputs().totalBytes()))) {
      writer.write(ByteBuffer.wrap(payload));
      OutputStore.Stored stored = writer.finish();
      assertEquals(index, stored.index());
      assertEquals(payload.length, stored.length());
      assertEquals(digest(payload), stored.sha256());
    }
  }

  private static Records.WorkView view(SessionStore sessions, Messages.Capabilities selected)
      throws Exception {
    return sessions
        .snapshot(sessionAccess(), selected, 1, new Messages.Watch(8, WORK, 0, 0))
        .work();
  }

  private static long[] credits(Fixture fixture) throws Exception {
    try (var connection =
            BoundedSqlite.open(fixture.database(), configuration(fixture.results()).files())
                .connect();
        var statement = connection.createStatement();
        var rows =
            statement.executeQuery(
                "SELECT e.view_slot,j.state_slot FROM ps_v2_entities e JOIN ps_v2_jobs j ON"
                    + " e.generation=j.generation AND e.scope=j.scope AND e.id=j.entity")) {
      assertTrue(rows.next());
      return new long[] {
        FixedRecords.header(connection, rows.getLong(1), FixedRecords.Kind.WORK).credits(),
        FixedRecords.header(connection, rows.getLong(2), FixedRecords.Kind.JOB).credits()
      };
    }
  }

  private static SessionStore.Configuration configuration(boolean results) {
    return new SessionStore.Configuration(
        "issuer-a",
        new Records.Limits(8, 16, 16, 1 << 20, 1 << 20, 4),
        new Records.Policy(60_000, 60_000, 60_000),
        2,
        8,
        4,
        BoundedSqlite.Limits.defaults(),
        new AdmissionStore.ExecutionPolicy(
            List.of(
                new AdmissionStore.Application(
                    "copy", Set.of(0), AdmissionStore.RestartSafety.IDEMPOTENT)),
            4,
            4));
  }

  private static Messages.Capabilities selected(boolean results) {
    return new Messages.Capabilities(
        true,
        results ? List.of(DURABLE_WORK, RESULT_DELIVERY) : List.of(DURABLE_WORK),
        List.of(),
        1 << 20,
        8,
        16,
        1 << 20,
        1000,
        5000);
  }

  private static Records.InputHeader header(int outputs, long outputBytes) throws Exception {
    return header(outputs, outputBytes, 1000);
  }

  private static Records.InputHeader header(int outputs, long outputBytes, long executionMs)
      throws Exception {
    return new Records.InputHeader(
        1,
        operation(2),
        new Records.AdmitParameters(
            WORK,
            new Records.Input(0, digest(new byte[0]), "application/octet-stream"),
            "copy",
            0,
            executionMs,
            new Records.OutputBudget(outputs, outputBytes)));
  }

  private static Records.Digest digest(byte[] payload) throws Exception {
    return new Records.Digest(MessageDigest.getInstance("SHA-256").digest(payload));
  }

  private static AdmissionStore.Clock clock(long time) {
    return () -> new AdmissionStore.Time(time, true);
  }

  private static Commitments.Context context() {
    return new Commitments.Context("issuer-a", "alice", 1);
  }

  private static Records.OperationId operation(int value) {
    byte[] bytes = new byte[16];
    bytes[15] = (byte) value;
    return new Records.OperationId(bytes);
  }

  private static SessionStore.Access sessionAccess() {
    return new SessionStore.Access("alice", () -> {});
  }

  private static ExecutionStore.Access execAccess() {
    return new ExecutionStore.Access("alice", () -> {});
  }

  private static void assertCode(ProtocolError.Code expected, Throwing action) {
    ProtocolError error = assertThrows(ProtocolError.class, action::run);
    assertEquals(expected, error.code(), error::getMessage);
  }

  private record Fixture(
      Path database,
      Path inputsPath,
      SessionStore sessions,
      InputStore inputs,
      Messages.Binding binding,
      Records.InputHeader header,
      Messages.Capabilities selected) {
    boolean results() {
      return selected.supported().contains(RESULT_DELIVERY);
    }
  }

  @FunctionalInterface
  private interface Throwing {
    void run() throws Exception;
  }
}
