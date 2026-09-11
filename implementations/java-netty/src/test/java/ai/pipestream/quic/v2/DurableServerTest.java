package ai.pipestream.quic.v2;

import static ai.pipestream.quic.v2.Messages.*;
import static org.junit.jupiter.api.Assertions.*;

import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/** Real mTLS loopback exercise of the composed Java durable listener with a raw Java peer. */
@Timeout(120)
final class DurableServerTest {
  @TempDir static Path directory;
  static DurableTestPki pki;
  static Map<Records.Digest, String> principals;
  static final Records.Policy POLICY = new Records.Policy(20_000, 60_000, 120_000);

  @BeforeAll
  static void certificates() throws Exception {
    pki = DurableTestPki.generate(directory, List.of("alice", "bob"));
    principals = pki.principals(List.of("alice", "bob"));
  }

  static DurableHost.Configuration configuration() {
    return DurableHost.Configuration.defaults("issuer-a", "localhost:7443");
  }

  static DurableHost host(Path root, boolean initialize) throws Exception {
    DurableHost.OwnerPolicy owners = DurableHost.OwnerPolicy.fromPrincipals(() -> principals, true);
    return initialize
        ? DurableHost.initialize(
            root,
            configuration(),
            ReferenceApplications.all(),
            owners,
            DurableHost.UtcClock.system(true))
        : DurableHost.open(
            root,
            configuration(),
            ReferenceApplications.all(),
            owners,
            DurableHost.UtcClock.system(true));
  }

  static DurableServer server(DurableHost host) throws Exception {
    return DurableServer.start(
        new InetSocketAddress("127.0.0.1", 0),
        pki.server(principals),
        host,
        DurableOptions.defaults());
  }

  static Records.Digest digest(byte[] bytes) throws Exception {
    return new Records.Digest(MessageDigest.getInstance("SHA-256").digest(bytes));
  }

  static Records.OperationId operation(int value) {
    byte[] bytes = new byte[16];
    bytes[15] = (byte) value;
    bytes[14] = (byte) (value >>> 8);
    return new Records.OperationId(bytes);
  }

  static byte[] payload(int length, long seed) {
    byte[] bytes = new byte[length];
    new Random(seed).nextBytes(bytes);
    return bytes;
  }

  static Records.InputHeader header(
      long generation, int op, Records.WorkKey work, byte[] payload, String application, int mode)
      throws Exception {
    return new Records.InputHeader(
        generation,
        operation(op),
        new Records.AdmitParameters(
            work,
            new Records.Input(payload.length, digest(payload), "application/octet-stream"),
            application,
            mode,
            10_000,
            new Records.OutputBudget(1, payload.length)));
  }

  static Records.WorkView awaitTerminal(RawDurablePeer peer, Records.WorkKey work)
      throws Exception {
    long revision = 0;
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
    while (true) {
      WatchResponse view =
          assertInstanceOf(
              WatchResponse.class, peer.call(new Watch(peer.request(), work, revision, 5000)));
      if (view.work().state().terminal()) return view.work();
      revision = view.revision();
      assertTrue(System.nanoTime() < deadline, "work did not reach a terminal state");
    }
  }

  @Test
  void createsAdmitsExecutesDeliversCheckpointsAndCompletesOverRealQuic() throws Exception {
    Path root = directory.resolve("slice");
    byte[] input = payload(200_000, 7);
    Records.WorkKey work = new Records.WorkKey(0, 0, 1);
    Binding binding;
    Records.WorkView succeeded;
    Records.ScopeSummary summary;
    try (DurableHost host = host(root, true);
        DurableServer server = server(host);
        RawDurablePeer peer = new RawDurablePeer(server.address(), pki.client("alice"), 65_536)) {
      Capabilities selected =
          peer.negotiate(RawDurablePeer.offer(List.of(DURABLE_WORK, RESULT_DELIVERY), 1 << 20));
      assertEquals(List.of(DURABLE_WORK, RESULT_DELIVERY), selected.supported());
      Sequence sequence =
          assertInstanceOf(Sequence.class, peer.call(new NextSequence(peer.request())));
      assertEquals(1, sequence.nextCreationSequence());
      binding = assertInstanceOf(Binding.class, peer.call(new Create(peer.request(), 1, POLICY)));
      assertEquals("issuer-a", binding.authority());
      assertEquals("alice", binding.owner());
      assertEquals(1, binding.generation());
      assertEquals(POLICY, binding.policy());
      DeclarationResponse declared =
          assertInstanceOf(
              DeclarationResponse.class,
              peer.call(new Declare(peer.request(), operation(1), 0, List.of(1L), true)));
      Records.Declared outcome =
          assertInstanceOf(Records.Declared.class, declared.receipt().outcome());
      assertEquals(1, outcome.acceptedCount());
      assertNotNull(outcome.seal());

      Records.InputHeader header = header(1, 2, work, input, "copy/v2", 0);
      var stream = peer.sendInput(header, input, true);
      AdmissionResponse admission = assertInstanceOf(AdmissionResponse.class, peer.next());
      assertEquals(new Records.RequestTag(true, stream.streamId()), admission.request());
      Records.Admitted admitted =
          assertInstanceOf(Records.Admitted.class, admission.receipt().outcome());
      assertEquals(work, admitted.work());
      assertEquals(1, admitted.attempt());
      assertNull(admitted.child());
      assertEquals(operation(2), admission.receipt().operation());
      assertEquals(
          Commitments.operation(new Commitments.Context("issuer-a", "alice", 1), 0, header),
          admission.receipt().requestDigest());

      succeeded = awaitTerminal(peer, work);
      assertEquals(Records.State.SUCCEEDED, succeeded.state());
      assertNotNull(succeeded.manifest());
      assertEquals(1, succeeded.manifest().outputs().size());
      Records.Output output = succeeded.manifest().outputs().get(0);
      assertEquals(input.length, output.length());
      assertEquals(digest(input), output.sha256());

      ManifestResponse manifest =
          assertInstanceOf(
              ManifestResponse.class, peer.call(new GetManifest(peer.request(), work, 1)));
      assertEquals(succeeded.manifest(), manifest.manifest());

      long readRequest = peer.request();
      peer.send(new Read(readRequest, work, 1, 0, output.sha256()));
      byte[] object = peer.nextObject();
      Records.ResultHeader result = decodeResultHeader(object);
      assertEquals(readRequest, result.request());
      assertEquals(1, result.generation());
      assertEquals(work, result.work());
      assertEquals(1, result.attempt());
      assertEquals(0, result.index());
      assertEquals(input.length, result.length());
      byte[] body = Arrays.copyOfRange(object, headerLength(object), object.length);
      assertArrayEquals(input, body);
      assertTrue(peer.messages.isEmpty(), "result stream is the response, not a second control");

      PageResponse page =
          assertInstanceOf(PageResponse.class, peer.call(new Page(peer.request(), 0, 0, 256)));
      assertTrue(page.sealed());
      assertEquals(outcome.seal(), page.seal());
      assertEquals(1, page.declared());
      CheckpointResponse checkpoint =
          assertInstanceOf(
              CheckpointResponse.class,
              peer.call(new Checkpoint(peer.request(), 0, page.seal(), 5000)));
      summary = checkpoint.summary();
      assertEquals(new Records.Counts(1, 0, 0, 0), summary.counts());
      assertEquals(0, summary.scope());
      Completed completed =
          assertInstanceOf(Completed.class, peer.call(new Complete(peer.request(), 1, summary)));
      assertEquals(summary, completed.root());

      // A structurally valid but altered summary is CONFLICT, not FRAME_ERROR.
      Records.ScopeSummary altered =
          new Records.ScopeSummary(
              0,
              0,
              null,
              summary.seal(),
              1,
              new Records.Counts(0, 1, 0, 0),
              summary.statusRoot(),
              summary.closedAt());
      Refusal conflict =
          assertInstanceOf(Refusal.class, peer.call(new Complete(peer.request(), 1, altered)));
      assertEquals(ProtocolError.Code.CONFLICT, conflict.code());

      Detached detached = assertInstanceOf(Detached.class, peer.call(new Detach(peer.request())));
      assertNotNull(detached);
      Refusal afterDetach =
          assertInstanceOf(Refusal.class, peer.call(new Watch(peer.request(), work, 0, 0)));
      assertEquals(ProtocolError.Code.NOT_READY, afterDetach.code());
      peer.control.shutdownOutput().sync();
      peer.fin.get(10, TimeUnit.SECONDS);
      DurableServer.Snapshot snapshot =
          server.snapshot().toCompletableFuture().get(5, TimeUnit.SECONDS);
      assertEquals(0, snapshot.inputs());
      assertEquals(0, snapshot.results());
    }

    // Reopen the same roots: the session, outcome and object survive; replay is byte-identical.
    try (DurableHost host = host(root, false);
        DurableServer server = server(host);
        RawDurablePeer peer = new RawDurablePeer(server.address(), pki.client("alice"), 65_536)) {
      peer.negotiate(RawDurablePeer.offer(List.of(DURABLE_WORK, RESULT_DELIVERY), 1 << 20));
      Binding attached =
          assertInstanceOf(
              Binding.class, peer.call(new Attach(peer.request(), "issuer-a", "alice", 1)));
      assertEquals(binding.generation(), attached.generation());
      assertEquals(binding.policy(), attached.policy());
      WatchResponse view =
          assertInstanceOf(WatchResponse.class, peer.call(new Watch(peer.request(), work, 0, 0)));
      assertEquals(succeeded, view.work());
      Records.InputHeader header = header(1, 2, work, input, "copy/v2", 0);
      var stream = peer.sendInput(header, new byte[0], false);
      AdmissionResponse replay = assertInstanceOf(AdmissionResponse.class, peer.next());
      assertEquals(new Records.RequestTag(true, stream.streamId()), replay.request());
      assertEquals(operation(2), replay.receipt().operation());
      assertEquals(
          1, assertInstanceOf(Records.Admitted.class, replay.receipt().outcome()).attempt());
      // The redundant stream is stopped (STOP_SENDING, application error 0; Section 12.4) and
      // released without opening an input: the transport exposes no error code to the peer, so
      // the stop is observed as a payload write that no longer makes progress, and the release as
      // an input count of zero while the replayed receipt is already in hand.
      io.netty.channel.ChannelFuture late =
          stream.writeAndFlush(io.netty.buffer.Unpooled.wrappedBuffer(new byte[] {1}));
      assertFalse(late.await(1000) && late.isSuccess(), "replayed stream still accepts payload");
      assertEquals(
          0, server.snapshot().toCompletableFuture().get(5, TimeUnit.SECONDS).inputs());
      // Identical creation replays the identical binding without a new generation.
      Refusal second =
          assertInstanceOf(Refusal.class, peer.call(new Create(peer.request(), 1, POLICY)));
      assertEquals(ProtocolError.Code.CONFLICT, second.code());
      Records.ScopeSummary rootAgain =
          assertInstanceOf(
                  CheckpointResponse.class,
                  peer.call(new Checkpoint(peer.request(), 0, summary.seal(), 0)))
              .summary();
      assertEquals(summary, rootAgain);
    }
  }

  @Test
  void anonymousCallerGetsCoreOnlyAndCannotOpenInputStreams() throws Exception {
    Path root = directory.resolve("anonymous");
    try (DurableHost host = host(root, true);
        DurableServer server = server(host);
        RawDurablePeer peer = new RawDurablePeer(server.address(), pki.client(null), 65_536)) {
      Capabilities selected =
          peer.negotiate(RawDurablePeer.offer(List.of(DURABLE_WORK, RESULT_DELIVERY), 1 << 20));
      assertTrue(selected.supported().isEmpty());
      Refusal refusal =
          assertInstanceOf(Refusal.class, peer.call(new NextSequence(peer.request())));
      assertEquals(ProtocolError.Code.EXTENSION_UNSUPPORTED, refusal.code());
      peer.sendInput(
          header(1, 9, new Records.WorkKey(0, 0, 1), new byte[0], "copy/v2", 0), new byte[0], true);
      peer.error(ProtocolError.Code.EXTENSION_UNSUPPORTED);
    }
  }

  @Test
  void requiredDurableWithoutIdentityClosesUnauthorizedBeforeCapabilities() throws Exception {
    Path root = directory.resolve("required");
    try (DurableHost host = host(root, true);
        DurableServer server = server(host);
        RawDurablePeer peer = new RawDurablePeer(server.address(), pki.client(null), 65_536)) {
      peer.open();
      peer.send(
          new Capabilities(
              false,
              List.of(DURABLE_WORK, RESULT_DELIVERY),
              List.of(DURABLE_WORK, RESULT_DELIVERY),
              1 << 20,
              16,
              64,
              1 << 20,
              5000,
              30_000));
      peer.error(ProtocolError.Code.UNAUTHORIZED);
      assertTrue(peer.messages.isEmpty());
    }
  }

  @Test
  void inputRefusalsAreCorrelatedByStreamAndLeaveDeclarationsIntact() throws Exception {
    Path root = directory.resolve("refusals");
    byte[] input = payload(1000, 3);
    Records.WorkKey work = new Records.WorkKey(0, 0, 1);
    try (DurableHost host = host(root, true);
        DurableServer server = server(host);
        RawDurablePeer peer = new RawDurablePeer(server.address(), pki.client("bob"), 65_536)) {
      peer.negotiate(RawDurablePeer.offer(List.of(DURABLE_WORK, RESULT_DELIVERY), 1 << 20));
      // Input before any session: NOT_READY tagged by the actual stream.
      var early = peer.sendInput(header(1, 1, work, input, "copy/v2", 0), input, true);
      Refusal notReady = assertInstanceOf(Refusal.class, peer.next());
      assertEquals(new Records.RequestTag(true, early.streamId()), notReady.request());
      assertEquals(ProtocolError.Code.NOT_READY, notReady.code());
      assertInstanceOf(Binding.class, peer.call(new Create(peer.request(), 1, POLICY)));
      // Undeclared work: CONFLICT.
      var undeclared = peer.sendInput(header(1, 2, work, input, "copy/v2", 0), input, true);
      Refusal conflict = assertInstanceOf(Refusal.class, peer.next());
      assertEquals(new Records.RequestTag(true, undeclared.streamId()), conflict.request());
      assertEquals(ProtocolError.Code.CONFLICT, conflict.code());
      assertInstanceOf(
          DeclarationResponse.class,
          peer.call(new Declare(peer.request(), operation(3), 0, List.of(1L), true)));
      // Unknown application: APPLICATION_UNSUPPORTED before any payload is retained.
      var unknown = peer.sendInput(header(1, 4, work, input, "nope/v9", 0), input, true);
      Refusal unsupported = assertInstanceOf(Refusal.class, peer.next());
      assertEquals(new Records.RequestTag(true, unknown.streamId()), unsupported.request());
      assertEquals(ProtocolError.Code.APPLICATION_UNSUPPORTED, unsupported.code());
      // Wrong digest: INTEGRITY_ERROR after reception; declaration survives.
      Records.InputHeader lying =
          new Records.InputHeader(
              1,
              operation(5),
              new Records.AdmitParameters(
                  work,
                  new Records.Input(
                      input.length, digest(new byte[] {1}), "application/octet-stream"),
                  "copy/v2",
                  0,
                  10_000,
                  new Records.OutputBudget(1, input.length)));
      var corrupt = peer.sendInput(lying, input, true);
      Refusal integrity = assertInstanceOf(Refusal.class, peer.next());
      assertEquals(new Records.RequestTag(true, corrupt.streamId()), integrity.request());
      assertEquals(ProtocolError.Code.INTEGRITY_ERROR, integrity.code());
      // Truncated FIN: INTEGRITY_ERROR as well.
      var truncated =
          peer.sendInput(header(1, 6, work, input, "copy/v2", 0), Arrays.copyOf(input, 500), true);
      Refusal short_ = assertInstanceOf(Refusal.class, peer.next());
      assertEquals(new Records.RequestTag(true, truncated.streamId()), short_.request());
      assertEquals(ProtocolError.Code.INTEGRITY_ERROR, short_.code());
      // Producer-1 input from an external caller: UNAUTHORIZED.
      var foreign =
          peer.sendInput(
              header(1, 7, new Records.WorkKey(0, 1, 1), input, "copy/v2", 0), input, true);
      Refusal unauthorized = assertInstanceOf(Refusal.class, peer.next());
      assertEquals(new Records.RequestTag(true, foreign.streamId()), unauthorized.request());
      assertEquals(ProtocolError.Code.UNAUTHORIZED, unauthorized.code());
      // The declared entity is still admissible with the right bytes.
      var good = peer.sendInput(header(1, 8, work, input, "copy/v2", 0), input, true);
      AdmissionResponse admission = assertInstanceOf(AdmissionResponse.class, peer.next());
      assertEquals(new Records.RequestTag(true, good.streamId()), admission.request());
      assertEquals(Records.State.SUCCEEDED, awaitTerminal(peer, work).state());
      WatchResponse view =
          assertInstanceOf(WatchResponse.class, peer.call(new Watch(peer.request(), work, 0, 0)));
      assertEquals(1, view.work().attempt());
      DurableServer.Snapshot snapshot =
          server.snapshot().toCompletableFuture().get(5, TimeUnit.SECONDS);
      assertEquals(0, snapshot.inputs());
      assertEquals(0, host.status().queuedStorageTasks());
    }
  }

  static int headerLength(byte[] object) {
    return 4 + java.nio.ByteBuffer.wrap(object).getInt();
  }

  static Records.ResultHeader decodeResultHeader(byte[] object) {
    return (Records.ResultHeader)
        Wire.decodeHeader(false, Arrays.copyOfRange(object, 0, headerLength(object)));
  }
}
