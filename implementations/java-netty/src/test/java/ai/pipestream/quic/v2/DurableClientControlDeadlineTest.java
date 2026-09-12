package ai.pipestream.quic.v2;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * The client's control deadline (Section 12.1 "bound waiting", 12.6 waits up to 30000 ms): a
 * durable connection with nothing outstanding is never failed for silence; a pending WATCH is
 * allowed its whole wait plus the control deadline before the client gives up; a withheld
 * response is still bounded, and the bound is named. Driven against the raw authority with the
 * control deadline shortened to 2 s so each case takes seconds, not minutes.
 */
class DurableClientControlDeadlineTest {
  static final Records.WorkKey WORK = new Records.WorkKey(0, 0, 1);
  static final Records.Policy POLICY = new Records.Policy(30_000, 60_000, 120_000);
  static final long CONTROL_TIMEOUT_MS = 2_000;

  @TempDir static Path directory;
  static DurableTestPki pki;
  static Map<Records.Digest, String> principals;
  static Records.Manifest manifest;

  @BeforeAll
  static void setup() throws Exception {
    pki = DurableTestPki.generate(directory, List.of("alice"));
    principals = pki.principals(List.of("alice"));
    manifest = manifestFor(new byte[1000]);
  }

  static Records.Manifest manifestFor(byte[] payload) throws Exception {
    return new Records.Manifest(
            "issuer-a",
            "alice",
            1,
            WORK,
            1,
            DurableServerTest.digest(new byte[] {1}),
            1_000,
            2_000_000_000_000L,
            List.of(
                new Records.Output(
                    0,
                    payload.length,
                    DurableServerTest.digest(payload),
                    "application/octet-stream",
                    new Locator(
                        "pipestream://localhost:7443/v2/sessions/1/scopes/0/producers/0/entities/1/attempts/1/outputs/0"))));
  }

  static ClientOptions shortControlDeadline() {
    ClientOptions defaults = ClientOptions.defaults();
    CoreOptions core = defaults.core();
    return new ClientOptions(
        new CoreOptions(
            core.controlLimit(),
            core.pendingLimit(),
            core.streamIdleMs(),
            core.streamLifetimeMs(),
            core.connections(),
            core.connectionsPerOwner(),
            core.queuedControlBytes(),
            core.controlWindowBytes(),
            core.readChunkBytes(),
            core.handshakeTimeoutMs(),
            CONTROL_TIMEOUT_MS),
        defaults.dataStreams(),
        defaults.maxDataStreams(),
        defaults.dataSendBytes(),
        defaults.streamWindowBytes(),
        defaults.chunkBytes(),
        defaults.objectLimit(),
        defaults.headerTimeoutMs());
  }

  static final class Session implements AutoCloseable {
    final RawDurableAuthority authority;
    final ClientJournal journal;
    final DurableClient client;

    Session(String name) throws Exception {
      authority = new RawDurableAuthority(pki.server(principals), manifest, POLICY);
      journal =
          ClientJournal.initialize(
              directory.resolve(name + ".sqlite"),
              new ClientJournal.Intent("issuer-a", "alice", 1, POLICY, true),
              ClientJournal.Limits.defaults());
      client =
          DurableClient.connect(
              authority.address(), pki.client("alice"), journal, shortControlDeadline());
      DurableClientTest.get(client.ready());
      DurableClientTest.get(client.binding());
    }

    @Override
    public void close() throws java.io.IOException, java.sql.SQLException {
      client.close();
      journal.close();
      authority.close();
    }
  }

  @Test
  @Timeout(60)
  void silenceWithNothingOutstandingNeverFailsTheConnection() throws Exception {
    try (Session session = new Session("idle")) {
      // Three control deadlines of silence in both directions, nothing pending.
      Thread.sleep(3 * CONTROL_TIMEOUT_MS + 500);
      assertFalse(session.client.closed().toCompletableFuture().isDone(), "closed for silence");
      assertEquals(manifest, DurableClientTest.get(session.client.manifest(WORK, 1)));
    }
  }

  @Test
  @Timeout(60)
  void aPendingWaitIsAllowedItsWholeWaitBeforeTheControlDeadline() throws Exception {
    try (Session session = new Session("wait")) {
      // The authority answers 2.5 control deadlines after the request, inside a 10 s wait: the
      // client must not judge the silence against the bare control deadline (defect D1).
      session.authority.controlDelayMs = 5 * CONTROL_TIMEOUT_MS / 2;
      long started = System.nanoTime();
      ProtocolError answer = DurableClientTest.refusal(session.client.watch(WORK, 0, 10_000));
      long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);
      assertEquals(ProtocolError.Code.NOT_FOUND, answer.code(), answer.toString());
      assertTrue(answer.getMessage().endsWith("raw authority"), answer.toString());
      assertTrue(elapsedMs >= 2 * CONTROL_TIMEOUT_MS, "answered early: " + elapsedMs + " ms");
      assertFalse(session.client.closed().toCompletableFuture().isDone(), "failed while waiting");
      assertEquals(manifest, DurableClientTest.get(session.client.manifest(WORK, 1)));
    }
  }

  @Test
  @Timeout(60)
  void aWithheldResponseIsBoundedByTheWaitPlusTheControlDeadline() throws Exception {
    try (Session session = new Session("withheld")) {
      session.authority.withholdControls = true;
      long wait = 1_000;
      long started = System.nanoTime();
      ProtocolError failure = DurableClientTest.refusal(session.client.watch(WORK, 0, wait));
      long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);
      assertEquals(ProtocolError.Code.LIMIT_EXCEEDED, failure.code(), failure.toString());
      assertTrue(failure.getMessage().endsWith("control response deadline"), failure.toString());
      assertTrue(
          elapsedMs >= wait + CONTROL_TIMEOUT_MS - 200 && elapsedMs <= wait + CONTROL_TIMEOUT_MS + 1_500,
          "bound missed: " + elapsedMs + " ms");
      // The bound is the connection's: it is failed with the same named reason.
      ProtocolError closed = DurableClientTest.refusal(session.client.closed());
      assertEquals(ProtocolError.Code.LIMIT_EXCEEDED, closed.code(), closed.toString());
    }
  }
  static Records.OperationReceipt admission(Records.InputHeader header, long admittedAt) {
    Records.AdmitParameters parameters = header.parameters();
    return new Records.OperationReceipt(
        header.operation(),
        Commitments.operation(new Commitments.Context("issuer-a", "alice", 1), 0, header),
        new Records.Admitted(
            parameters.work(), 1, admittedAt, admittedAt + parameters.executionMs(), null));
  }

  static Records.AdmitParameters parameters(InputSource source) {
    return new Records.AdmitParameters(
        WORK,
        source.input(),
        "copy/v2",
        0,
        10_000,
        new Records.OutputBudget(1, source.input().length()));
  }

  /**
   * Section 12.4: "The client still needs the correlated receipt or operation lookup; STOP_SENDING
   * alone is not admission evidence." The authority stops the input stream as soon as it has read
   * the header and answers on control three quarters of a control deadline later (an input's
   * receipt carries no wait, so it is due within the control deadline): the admission stays
   * pending across the stop, nothing is journaled until the receipt arrives, and the receipt is
   * then validated and journaled exactly as for an unstopped transfer.
   */
  @Test
  @Timeout(60)
  void stopSendingAloneIsNotAdmissionEvidence() throws Exception {
    Path input = directory.resolve("stopped-input.bin");
    byte[] bytes = new byte[300_000];
    new java.util.Random(7).nextBytes(bytes);
    java.nio.file.Files.write(input, bytes);
    try (Session session = new Session("stopped")) {
      long delay = 3 * CONTROL_TIMEOUT_MS / 4;
      session.authority.inputs =
          (header, stream, reply) -> {
            stream.close();
            stream
                .eventLoop()
                .schedule(
                    () ->
                        reply.accept(
                            new Messages.AdmissionResponse(
                                new Records.RequestTag(true, stream.streamId()),
                                admission(header, 1_000))),
                    delay,
                    TimeUnit.MILLISECONDS);
          };
      DurableClientTest.get(
          session.client.declare(DurableServerTest.operation(1), 0, List.of(1L), true));
      InputSource source = InputSource.file(input, "application/octet-stream", 16L << 20);
      Records.OperationId operation = DurableServerTest.operation(2);
      var admitted =
          session
              .client
              .admit(operation, parameters(source), DurableServerTest.operation(1), source)
              .toCompletableFuture();
      Thread.sleep(delay / 2);
      assertFalse(admitted.isDone(), "admission resolved by the stop alone: " + admitted);
      assertTrue(session.journal.receipt(operation).isEmpty(), "receipt journaled before it arrived");
      Records.OperationReceipt receipt = DurableClientTest.get(admitted);
      assertEquals(operation, receipt.operation());
      assertEquals(1, ((Records.Admitted) receipt.outcome()).attempt());
      assertEquals(receipt, session.journal.receipt(operation).orElseThrow());
      assertFalse(session.client.closed().toCompletableFuture().isDone(), "connection failed");
    }
  }

  /** A stopped input whose receipt never comes is bounded by the control deadline and named. */
  @Test
  @Timeout(60)
  void aStoppedInputWithoutAReceiptIsBoundedByTheControlDeadline() throws Exception {
    Path input = directory.resolve("stopped-unanswered.bin");
    java.nio.file.Files.write(input, new byte[10_000]);
    try (Session session = new Session("unanswered")) {
      DurableClientTest.get(
          session.client.declare(DurableServerTest.operation(1), 0, List.of(1L), true));
      InputSource source = InputSource.file(input, "application/octet-stream", 16L << 20);
      Records.OperationId operation = DurableServerTest.operation(2);
      long started = System.nanoTime();
      ProtocolError failure =
          DurableClientTest.refusal(
              session.client.admit(
                  operation, parameters(source), DurableServerTest.operation(1), source));
      long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);
      assertEquals(ProtocolError.Code.LIMIT_EXCEEDED, failure.code(), failure.toString());
      assertTrue(failure.getMessage().endsWith("control response deadline"), failure.toString());
      assertTrue(
          elapsedMs >= CONTROL_TIMEOUT_MS - 200 && elapsedMs <= CONTROL_TIMEOUT_MS + 1_500,
          "bound missed: " + elapsedMs + " ms");
      assertTrue(session.journal.receipt(operation).isEmpty(), "receipt journaled without one");
    }
  }
}
