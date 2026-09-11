package ai.pipestream.quic.v2;

import static ai.pipestream.quic.v2.Messages.*;
import static org.junit.jupiter.api.Assertions.*;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.quic.QuicConnectionCloseEvent;
import io.netty.handler.codec.quic.QuicStreamChannel;
import io.netty.handler.codec.quic.QuicStreamType;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * Wire-level attacks and resource boundaries against the listener with a raw peer: correlation and
 * framing violations, result-read refusals, COMPLETE competing with transfers, stalled inputs,
 * stream exhaustion, and independent progress of a healthy connection beside a stalled one.
 */
@Timeout(240)
final class DurableWireNegativeTest {
  @TempDir static Path directory;
  static DurableTestPki pki;
  static Map<Records.Digest, String> principals;
  static final Records.Policy POLICY = new Records.Policy(30_000, 60_000, 120_000);
  static final Records.WorkKey WORK = new Records.WorkKey(0, 0, 1);

  @BeforeAll
  static void certificates() throws Exception {
    pki = DurableTestPki.generate(directory, List.of("alice", "bob"));
    principals = pki.principals(List.of("alice", "bob"));
  }

  static DurableOptions options(
      long streamIdleMs, long streamLifetimeMs, int dataStreams, long headerTimeoutMs) {
    return options(
        streamIdleMs,
        streamLifetimeMs,
        dataStreams,
        headerTimeoutMs,
        DurableOptions.defaults().core().controlTimeoutMs());
  }

  static DurableOptions options(
      long streamIdleMs,
      long streamLifetimeMs,
      int dataStreams,
      long headerTimeoutMs,
      long controlTimeoutMs) {
    DurableOptions defaults = DurableOptions.defaults();
    CoreOptions core = defaults.core();
    return new DurableOptions(
        new CoreOptions(
            core.controlLimit(),
            core.pendingLimit(),
            streamIdleMs,
            streamLifetimeMs,
            core.connections(),
            core.connectionsPerOwner(),
            core.queuedControlBytes(),
            core.controlWindowBytes(),
            core.readChunkBytes(),
            core.handshakeTimeoutMs(),
            controlTimeoutMs),
        dataStreams,
        defaults.maxDataStreams(),
        defaults.dataSendBytes(),
        defaults.streamWindowBytes(),
        defaults.chunkBytes(),
        defaults.objectLimit(),
        headerTimeoutMs,
        false,
        defaults.shutdownTimeoutMs());
  }

  private static final class Authority implements AutoCloseable {
    final DurableHost host;
    final DurableServer server;

    Authority(String name, DurableOptions options) throws Exception {
      DurableHost.OwnerPolicy owners =
          DurableHost.OwnerPolicy.fromPrincipals(() -> principals, true);
      host =
          DurableHost.initialize(
              directory.resolve(name),
              DurableHost.Configuration.defaults("issuer-a", "localhost:7443"),
              ReferenceApplications.all(),
              owners,
              DurableHost.UtcClock.system(true));
      server =
          DurableServer.start(
              new InetSocketAddress("127.0.0.1", 0), pki.server(principals), host, options);
    }

    RawDurablePeer peer(String owner) throws Exception {
      RawDurablePeer peer = new RawDurablePeer(server.address(), pki.client(owner), 65_536);
      peer.negotiate(RawDurablePeer.offer(List.of(DURABLE_WORK, RESULT_DELIVERY), 1 << 20));
      return peer;
    }

    @Override
    public void close() throws java.io.IOException {
      server.close();
      host.close();
    }
  }

  @FunctionalInterface
  interface StreamOpen {
    QuicStreamChannel open() throws Exception;
  }

  /** Peer stream credit returns only after the server acknowledges a reset; retry briefly. */
  static QuicStreamChannel withStreamCredit(StreamOpen open) throws Exception {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
    while (true) {
      try {
        return open.open();
      } catch (java.util.concurrent.ExecutionException limit) {
        if (!String.valueOf(limit.getCause()).contains("STREAM_LIMIT")
            || System.nanoTime() > deadline) throw limit;
        Thread.sleep(50);
      }
    }
  }

  static Records.WorkView admitAndSucceed(RawDurablePeer peer, byte[] input) throws Exception {
    assertInstanceOf(Binding.class, peer.call(new Create(peer.request(), 1, POLICY)));
    assertInstanceOf(
        DeclarationResponse.class,
        peer.call(
            new Declare(peer.request(), DurableServerTest.operation(1), 0, List.of(1L, 2L), true)));
    peer.sendInput(DurableServerTest.header(1, 2, WORK, input, "copy/v2", 0), input, true);
    assertInstanceOf(AdmissionResponse.class, peer.next());
    Records.WorkView view = DurableServerTest.awaitTerminal(peer, WORK);
    assertEquals(Records.State.SUCCEEDED, view.state());
    return view;
  }

  @Test
  void resultReadRefusalsAndCompleteExclusionAreExact() throws Exception {
    byte[] input = DurableServerTest.payload(120_000, 8);
    try (Authority authority = new Authority("reads", DurableOptions.defaults());
        RawDurablePeer peer = authority.peer("alice")) {
      Records.WorkView view = admitAndSucceed(peer, input);
      Records.Output output = view.manifest().outputs().get(0);
      // Wrong digest: INTEGRITY_ERROR; wrong attempt/index: NOT_FOUND; unadmitted work: NOT_READY.
      Refusal wrongHash =
          assertInstanceOf(
              Refusal.class,
              peer.call(
                  new Read(peer.request(), WORK, 1, 0, DurableServerTest.digest(new byte[] {9}))));
      assertEquals(ProtocolError.Code.INTEGRITY_ERROR, wrongHash.code());
      Refusal wrongAttempt =
          assertInstanceOf(
              Refusal.class, peer.call(new Read(peer.request(), WORK, 2, 0, output.sha256())));
      assertEquals(ProtocolError.Code.NOT_FOUND, wrongAttempt.code());
      Refusal wrongIndex =
          assertInstanceOf(
              Refusal.class, peer.call(new Read(peer.request(), WORK, 1, 1, output.sha256())));
      assertEquals(ProtocolError.Code.NOT_FOUND, wrongIndex.code());
      // Declared-only work has no attempt 1 yet: NOT_FOUND (wrong attempt) or NOT_READY are both
      // consistent with 12.7; admitted-but-unpublished work must be NOT_READY.
      Records.WorkKey second = new Records.WorkKey(0, 0, 2);
      Refusal declaredOnly =
          assertInstanceOf(
              Refusal.class, peer.call(new Read(peer.request(), second, 1, 0, output.sha256())));
      assertTrue(
          declaredOnly.code() == ProtocolError.Code.NOT_FOUND
              || declaredOnly.code() == ProtocolError.Code.NOT_READY,
          declaredOnly.toString());
      peer.sendInput(
          DurableServerTest.header(1, 3, second, input, "retry-copy/v2", 0), input, true);
      assertInstanceOf(AdmissionResponse.class, peer.next());
      long revision = 0;
      while (true) {
        WatchResponse pending =
            assertInstanceOf(
                WatchResponse.class, peer.call(new Watch(peer.request(), second, revision, 5000)));
        if (pending.work().state() == Records.State.AWAITING_RETRY) break;
        assertFalse(pending.work().state().terminal(), pending.work().toString());
        revision = pending.revision();
      }
      Refusal unpublished =
          assertInstanceOf(
              Refusal.class, peer.call(new Read(peer.request(), second, 1, 0, output.sha256())));
      assertEquals(ProtocolError.Code.NOT_READY, unpublished.code());
      Refusal manifestMissing =
          assertInstanceOf(Refusal.class, peer.call(new GetManifest(peer.request(), second, 1)));
      assertEquals(ProtocolError.Code.NOT_READY, manifestMissing.code());
      // A COMPLETE while a result transfer is outstanding is NOT_READY and the transfer finishes.
      long readRequest = peer.request();
      peer.send(new Read(readRequest, WORK, 1, 0, output.sha256()));
      PageResponse page =
          assertInstanceOf(PageResponse.class, peer.call(new Page(peer.request(), 0, 0, 256)));
      Refusal early =
          assertInstanceOf(
              Refusal.class,
              peer.call(
                  new Complete(
                      peer.request(),
                      1,
                      new Records.ScopeSummary(
                          0,
                          0,
                          null,
                          page.seal(),
                          2,
                          new Records.Counts(1, 1, 0, 0),
                          page.seal(),
                          1))));
      assertTrue(
          early.code() == ProtocolError.Code.NOT_READY
              || early.code() == ProtocolError.Code.CONFLICT,
          early.toString());
      byte[] object = peer.nextObject();
      assertEquals(readRequest, DurableServerTest.decodeResultHeader(object).request());
      assertArrayEquals(
          input, Arrays.copyOfRange(object, DurableServerTest.headerLength(object), object.length));
      // Another identical read reopens the same immutable output without re-execution.
      long again = peer.request();
      peer.send(new Read(again, WORK, 1, 0, output.sha256()));
      byte[] reopened = peer.nextObject();
      assertEquals(again, DurableServerTest.decodeResultHeader(reopened).request());
      assertEquals(
          1,
          assertInstanceOf(WatchResponse.class, peer.call(new Watch(peer.request(), WORK, 0, 0)))
              .work()
              .attempt());
      // Checkpoint refusals: unsealed child cut is impossible here, but a wrong seal is
      // INTEGRITY_ERROR and a zero wait on an unresolved sealed scope is WAIT_TIMEOUT.
      Refusal wrongSeal =
          assertInstanceOf(
              Refusal.class,
              peer.call(
                  new Checkpoint(peer.request(), 0, DurableServerTest.digest(new byte[] {1}), 0)));
      assertEquals(ProtocolError.Code.INTEGRITY_ERROR, wrongSeal.code());
      Refusal timeout =
          assertInstanceOf(
              Refusal.class, peer.call(new Checkpoint(peer.request(), 0, page.seal(), 0)));
      assertEquals(
          ProtocolError.Code.WAIT_TIMEOUT,
          timeout.code(),
          "member 2 is still declared, not terminal");
      Refusal positive =
          assertInstanceOf(
              Refusal.class, peer.call(new Checkpoint(peer.request(), 0, page.seal(), 1000)));
      assertEquals(ProtocolError.Code.WAIT_TIMEOUT, positive.code());
      Refusal missingScope =
          assertInstanceOf(
              Refusal.class, peer.call(new Checkpoint(peer.request(), 9, page.seal(), 0)));
      assertEquals(ProtocolError.Code.NOT_FOUND, missingScope.code());
      peer.call(new Detach(peer.request()));
    }
  }

  @Test
  void correlationAndFramingViolationsAreFatalWhileRefusedRequestsConsumeIds() throws Exception {
    try (Authority authority = new Authority("framing", DurableOptions.defaults())) {
      // Repeated request identifier: FRAME_ERROR closes the connection.
      try (RawDurablePeer peer = authority.peer("alice")) {
        assertInstanceOf(Binding.class, peer.call(new Create(1, 1, POLICY)));
        peer.send(new NextSequence(1));
        peer.error(ProtocolError.Code.FRAME_ERROR);
      }
      // A refused valid request consumes its identifier: reusing it afterwards is FRAME_ERROR.
      try (RawDurablePeer peer = authority.peer("alice")) {
        Refusal refused = assertInstanceOf(Refusal.class, peer.call(new Watch(1, WORK, 0, 0)));
        assertEquals(ProtocolError.Code.NOT_READY, refused.code());
        peer.send(new Watch(1, WORK, 0, 0));
        peer.error(ProtocolError.Code.FRAME_ERROR);
      }
      // A second capability exchange is FRAME_ERROR; control FIN before detach is FRAME_ERROR.
      try (RawDurablePeer peer = authority.peer("alice")) {
        peer.send(RawDurablePeer.offer(List.of(DURABLE_WORK), 1 << 20));
        peer.error(ProtocolError.Code.FRAME_ERROR);
      }
      try (RawDurablePeer peer = authority.peer("alice")) {
        peer.control.shutdownOutput().sync();
        peer.error(ProtocolError.Code.FRAME_ERROR);
      }
      // A server response type sent by the client is a direction violation.
      try (RawDurablePeer peer = authority.peer("alice")) {
        peer.send(new Detached(1));
        peer.error(ProtocolError.Code.FRAME_ERROR);
      }
      // A second client bidirectional stream is forbidden by the server's transport parameters
      // (one bidirectional stream), so the client's own transport refuses to open it.
      try (RawDurablePeer peer = authority.peer("alice")) {
        Throwable limit =
            assertThrows(
                Throwable.class,
                () ->
                    peer.connection
                        .createStream(
                            QuicStreamType.BIDIRECTIONAL, new ChannelInboundHandlerAdapter())
                        .get(5, TimeUnit.SECONDS));
        assertTrue(
            String.valueOf(limit.getCause()).contains("STREAM_LIMIT"), String.valueOf(limit));
        assertInstanceOf(Sequence.class, peer.call(new NextSequence(peer.request())));
        peer.call(new Detach(peer.request()));
      }
      // Oversized object header length: the input is refused before any payload is retained.
      try (RawDurablePeer peer = authority.peer("alice")) {
        assertInstanceOf(Binding.class, peer.call(new Create(peer.request(), 1, POLICY)));
        QuicStreamChannel stream =
            peer.connection
                .createStream(QuicStreamType.UNIDIRECTIONAL, new ChannelInboundHandlerAdapter())
                .get(5, TimeUnit.SECONDS);
        stream
            .writeAndFlush(
                Unpooled.wrappedBuffer(ByteBuffer.allocate(8).putInt(5000).putInt(0).array()))
            .sync();
        Refusal refused = assertInstanceOf(Refusal.class, peer.next());
        assertEquals(new Records.RequestTag(true, stream.streamId()), refused.request());
        assertEquals(ProtocolError.Code.FRAME_ERROR, refused.code());
        // The connection survives an input-local framing failure; control continues.
        assertInstanceOf(Sequence.class, peer.call(new NextSequence(peer.request())));
        peer.call(new Detach(peer.request()));
      }
    }
  }

  @Test
  void sequentialInputsBeyondTheConcurrentLimitReplenishStreamCredit() throws Exception {
    byte[] input = DurableServerTest.payload(20_000, 4);
    try (Authority authority = new Authority("replenish", options(5000, 30_000, 2, 5000));
        RawDurablePeer peer = authority.peer("alice")) {
      assertInstanceOf(Binding.class, peer.call(new Create(peer.request(), 1, POLICY)));
      assertInstanceOf(
          DeclarationResponse.class,
          peer.call(
              new Declare(
                  peer.request(),
                  DurableServerTest.operation(1),
                  0,
                  List.of(1L, 2L, 3L, 4L, 5L, 6L, 7L),
                  true)));
      for (int entity = 1; entity <= 7; entity++) {
        Records.WorkKey work = new Records.WorkKey(0, 0, entity);
        int op = 10 + entity;
        // Every third input is deliberately corrupt and refused; the rest admit and succeed.
        byte[] bytes = entity % 3 == 0 ? Arrays.copyOf(input, input.length - 1) : input;
        QuicStreamChannel stream =
            withStreamCredit(
                () ->
                    peer.sendInput(
                        DurableServerTest.header(1, op, work, input, "copy/v2", 0), bytes, true));
        Message response = peer.next();
        if (entity % 3 == 0) {
          Refusal refused = assertInstanceOf(Refusal.class, response);
          assertEquals(new Records.RequestTag(true, stream.streamId()), refused.request());
          assertEquals(ProtocolError.Code.INTEGRITY_ERROR, refused.code());
          // The refused stream's slot returns as credit before anything else happens: the
          // Section 12.1 refused-stream rule, measured the same way against the Rust authority.
          peer.awaitStreamCredit(2);
        } else {
          assertEquals(
              new Records.RequestTag(true, stream.streamId()),
              assertInstanceOf(AdmissionResponse.class, response).request());
          assertEquals(
              Records.State.SUCCEEDED, DurableServerTest.awaitTerminal(peer, work).state());
        }
      }
      DurableServer.Snapshot snapshot =
          authority.server.snapshot().toCompletableFuture().get(5, TimeUnit.SECONDS);
      assertEquals(0, snapshot.inputs());
      peer.call(new Detach(peer.request()));
    }
  }

  /**
   * Control silence is idleness only while nothing is outstanding. A granted watch and one slowly
   * progressing input each outlive the control deadline without a single control frame; a stalled
   * input whose idle bound equals the control deadline is refused per stream, with a named reason,
   * on a connection that survives; and only a connection with nothing outstanding is closed for
   * idle control, carrying the bound's name as the close reason.
   */
  @Test
  void controlSilenceIsIdleOnlyWithNothingOutstanding() throws Exception {
    byte[] input = DurableServerTest.payload(24_000, 7);
    try (Authority authority = new Authority("silent", options(2000, 20_000, 2, 1500, 2000));
        RawDurablePeer peer = authority.peer("alice")) {
      assertInstanceOf(Binding.class, peer.call(new Create(peer.request(), 1, POLICY)));
      assertInstanceOf(
          DeclarationResponse.class,
          peer.call(
              new Declare(
                  peer.request(), DurableServerTest.operation(1), 0, List.of(1L, 2L, 3L), true)));
      Records.WorkKey declared = new Records.WorkKey(0, 0, 3);
      // A granted wait twice the control deadline: no control frame for 4 s, the response arrives.
      WatchResponse current =
          assertInstanceOf(
              WatchResponse.class, peer.call(new Watch(peer.request(), declared, 0, 0)));
      long start = System.nanoTime();
      WatchResponse waited =
          assertInstanceOf(
              WatchResponse.class,
              peer.call(new Watch(peer.request(), declared, current.revision(), 4000)));
      assertEquals(current.revision(), waited.revision());
      assertTrue(
          System.nanoTime() - start >= TimeUnit.MILLISECONDS.toNanos(3500),
          "the wait was granted for its full duration");
      assertFalse(peer.closed.isDone(), "connection survived a granted wait");
      // One input progressing below its idle bound for longer than twice the control deadline.
      QuicStreamChannel slow =
          peer.sendInput(
              DurableServerTest.header(1, 2, WORK, input, "copy/v2", 0),
              Arrays.copyOf(input, 1000),
              false);
      int offset = 1000;
      for (int i = 0; i < 12; i++) {
        Thread.sleep(400);
        slow.writeAndFlush(Unpooled.wrappedBuffer(input, offset, 1000)).sync();
        offset += 1000;
      }
      slow.writeAndFlush(Unpooled.wrappedBuffer(input, offset, input.length - offset)).sync();
      slow.shutdownOutput().sync();
      assertInstanceOf(AdmissionResponse.class, peer.next());
      assertEquals(Records.State.SUCCEEDED, DurableServerTest.awaitTerminal(peer, WORK).state());
      assertFalse(peer.closed.isDone(), "connection survived a long silent upload");
      // Idle bound equal to the control deadline: the stalled stream alone is refused, by name.
      QuicStreamChannel stalled =
          withStreamCredit(
              () ->
                  peer.sendInput(
                      DurableServerTest.header(
                          1, 3, new Records.WorkKey(0, 0, 2), input, "copy/v2", 0),
                      Arrays.copyOf(input, 1000),
                      false));
      Refusal idle = assertInstanceOf(Refusal.class, peer.next());
      assertEquals(new Records.RequestTag(true, stalled.streamId()), idle.request());
      assertEquals(ProtocolError.Code.LIMIT_EXCEEDED, idle.code());
      assertEquals("input receive deadline", idle.detail());
      stalled.shutdownOutput(0x204).sync();
      assertInstanceOf(WatchResponse.class, peer.call(new Watch(peer.request(), declared, 0, 0)));
      assertFalse(peer.closed.isDone(), "connection survived a per-stream refusal");
      // A durable connection with nothing outstanding is never closed for control silence: twice
      // the control deadline of complete silence, then it still answers.
      Thread.sleep(4500);
      assertFalse(peer.closed.isDone(), "durable connection closed for control silence");
      assertInstanceOf(WatchResponse.class, peer.call(new Watch(peer.request(), declared, 0, 0)));
      // A core-only connection with nothing outstanding is closed at the control deadline, with
      // the bound's name as the close reason.
      try (RawDurablePeer core =
          new RawDurablePeer(authority.server.address(), pki.client("bob"), 65_536)) {
        core.negotiate(RawDurablePeer.offer(List.of(), 1 << 20));
        QuicConnectionCloseEvent close = core.closed.get(10, TimeUnit.SECONDS);
        assertTrue(close.isApplicationClose(), close.toString());
        assertEquals(ProtocolError.Code.LIMIT_EXCEEDED.applicationError(), close.error());
        assertEquals("idle control deadline", new String(close.reason(), StandardCharsets.UTF_8));
      }
    }
  }

  /**
   * The neutral driver's stalled-principal shape, against a peer that does read control: three
   * partial inputs with no FIN, a granted watch as long as the idle bound on
   * declared-never-admitted work, and a result stream requested and never read. Every stalled input
   * is refused per stream, by name, and the connection is still open when the last refusal has been
   * read.
   */
  @Test
  void stalledPrincipalIsRefusedPerStreamOnASurvivingConnection() throws Exception {
    byte[] input = DurableServerTest.payload(24_000, 5);
    byte[] stall = DurableServerTest.payload(256 * 1024, 11);
    try (Authority authority = new Authority("principal", options(3000, 12_000, 4, 1500, 3000));
        RawDurablePeer peer = authority.peer("alice")) {
      assertInstanceOf(Binding.class, peer.call(new Create(peer.request(), 1, POLICY)));
      assertInstanceOf(
          DeclarationResponse.class,
          peer.call(
              new Declare(
                  peer.request(),
                  DurableServerTest.operation(1),
                  0,
                  List.of(1L, 2L, 3L, 4L, 5L, 6L),
                  true)));
      peer.sendInput(DurableServerTest.header(1, 2, WORK, input, "copy/v2", 0), input, true);
      assertInstanceOf(AdmissionResponse.class, peer.next());
      Records.WorkView view = DurableServerTest.awaitTerminal(peer, WORK);
      assertEquals(Records.State.SUCCEEDED, view.state());
      Records.Output output = view.manifest().outputs().get(0);
      // Three stalled inputs: a declared 256 KiB payload, 128 KiB sent, no FIN.
      long[] stalled = new long[3];
      QuicStreamChannel[] stalledStreams = new QuicStreamChannel[3];
      for (int i = 0; i < 3; i++) {
        int index = i;
        QuicStreamChannel stream =
            withStreamCredit(
                () ->
                    peer.sendInput(
                        DurableServerTest.header(
                            1,
                            10 + index,
                            new Records.WorkKey(0, 0, 2 + index),
                            stall,
                            "copy/v2",
                            0),
                        Arrays.copyOf(stall, 128 * 1024),
                        false));
        stalled[i] = stream.streamId();
        stalledStreams[i] = stream;
      }
      // Control for the post-refusal probe below: before any bound expires, a one-byte write on
      // each stalled stream completes at once, so a write that hangs after the refusal is a
      // state change caused by the refusal and not a peer-side buffer limit.
      for (QuicStreamChannel stream : stalledStreams) {
        ChannelFuture control = stream.writeAndFlush(Unpooled.wrappedBuffer(new byte[] {1}));
        assertTrue(
            control.await(1000) && control.isSuccess(),
            "pre-refusal write on stream " + stream.streamId() + " did not complete: " + control.cause());
      }
      // A watch as long as the idle bound, held on declared-never-admitted work, and a result read
      // whose stream is never read.
      peer.send(new Watch(peer.request(), new Records.WorkKey(0, 0, 6), 0, 3000));
      peer.holdIncoming = true;
      peer.send(new Read(peer.request(), WORK, 1, 0, output.sha256()));
      // Read control until every stalled stream has been refused, or the bound is missed. Each
      // refusal is timed from the last stalled byte: the idle bound is measured from that byte, and
      // the tick interval is at most a tenth of the bound, so a refusal later than idle + 1 s means
      // something else delayed it.
      long established = System.nanoTime();
      long deadline = established + TimeUnit.SECONDS.toNanos(8);
      List<Long> refusalMillis = new ArrayList<>();
      int refused = 0;
      while (refused < 3) {
        long remaining = deadline - System.nanoTime();
        assertTrue(remaining > 0, "stalled inputs were not all refused by idle+5s");
        Message message =
            peer.messages.poll(Math.max(1, remaining / 1_000_000), TimeUnit.MILLISECONDS);
        assertNotNull(
            message,
            "no control frame before the bound; connection "
                + (peer.closed.isDone() ? peer.closed.get() : "still open"));
        if (message instanceof Refusal refusal && refusal.request().input()) {
          assertEquals(ProtocolError.Code.LIMIT_EXCEEDED, refusal.code(), refusal.toString());
          assertEquals("input receive deadline", refusal.detail());
          refusalMillis.add(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - established));
          refused++;
        }
      }
      assertFalse(
          peer.closed.isDone(),
          "connection closed before the refusals were read: "
              + (peer.closed.isDone() ? peer.closed.get() : ""));
      assertTrue(
          refusalMillis.get(2) <= 4000,
          "stalled inputs refused later than idle + 1 s; refusal times ms " + refusalMillis);
      // Each refusal aborts its stream (Section 12.1): after the refusal has been read, a one-byte
      // write on the stalled stream no longer completes, where the identical write before the
      // bound completed at once. The transport queues writes on an aborted stream rather than
      // failing them, so the assertion is on progress, not on a failure cause.
      for (QuicStreamChannel stream : stalledStreams) {
        ChannelFuture probe = stream.writeAndFlush(Unpooled.wrappedBuffer(new byte[] {1}));
        long probeStart = System.nanoTime();
        boolean settled = probe.await(1000);
        String outcome =
            "stream "
                + stream.streamId()
                + " settled="
                + settled
                + " success="
                + probe.isSuccess()
                + " cause="
                + probe.cause()
                + " afterMs="
                + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - probeStart)
                + " open="
                + stream.isOpen()
                + " active="
                + stream.isActive()
                + " outputShutdown="
                + stream.isOutputShutdown();
        assertFalse(
            settled && probe.isSuccess(), "write on refused stream still made progress: " + outcome);
      }
      for (long id : stalled) assertTrue(id >= 0);
      // The neutral driver reads control only at the end of its window, long after the idle bound:
      // the refused streams and the expired watch leave nothing outstanding, and the connection
      // must still be there, silent for twice the control deadline, when it does.
      Thread.sleep(6500);
      assertFalse(peer.closed.isDone(), "durable connection closed for control silence");
      assertInstanceOf(
          WatchResponse.class,
          peer.call(new Watch(peer.request(), new Records.WorkKey(0, 0, 6), 0, 0)));
      peer.resumeIncoming();
    }
  }

  @Test
  void stalledInputsExpireWithoutBlockingAHealthyConnection() throws Exception {
    byte[] input = DurableServerTest.payload(300_000, 5);
    try (Authority authority = new Authority("stall", options(1000, 5000, 2, 1500));
        RawDurablePeer stalled = authority.peer("alice");
        RawDurablePeer healthy = authority.peer("bob")) {
      assertInstanceOf(Binding.class, stalled.call(new Create(stalled.request(), 1, POLICY)));
      assertInstanceOf(
          DeclarationResponse.class,
          stalled.call(
              new Declare(
                  stalled.request(),
                  DurableServerTest.operation(1),
                  0,
                  List.of(1L, 2L, 3L),
                  true)));
      // Header never completes: header deadline expires with a correlated LIMIT_EXCEEDED.
      QuicStreamChannel headless =
          stalled
              .connection
              .createStream(QuicStreamType.UNIDIRECTIONAL, new ChannelInboundHandlerAdapter())
              .get(5, TimeUnit.SECONDS);
      headless.writeAndFlush(Unpooled.wrappedBuffer(new byte[] {0, 0})).sync();
      Refusal header = assertInstanceOf(Refusal.class, stalled.next());
      assertEquals(new Records.RequestTag(true, headless.streamId()), header.request());
      assertEquals(ProtocolError.Code.LIMIT_EXCEEDED, header.code());
      headless.shutdownOutput(0x204).sync();
      // Payload stalls after the header: idle deadline expires, declaration survives, then a
      // complete retransmission of the same operation admits normally.
      Records.InputHeader partial = DurableServerTest.header(1, 2, WORK, input, "copy/v2", 0);
      QuicStreamChannel stalledStream =
          stalled.sendInput(partial, Arrays.copyOf(input, 1000), false);
      long start = System.nanoTime();
      Refusal idle = assertInstanceOf(Refusal.class, stalled.next());
      assertEquals(new Records.RequestTag(true, stalledStream.streamId()), idle.request());
      assertEquals(ProtocolError.Code.LIMIT_EXCEEDED, idle.code());
      assertTrue(System.nanoTime() - start < TimeUnit.SECONDS.toNanos(10));
      stalledStream.shutdownOutput(0x204).sync();
      // Meanwhile the healthy owner creates a session and works normally.
      assertInstanceOf(Binding.class, healthy.call(new Create(healthy.request(), 1, POLICY)));
      assertInstanceOf(
          DeclarationResponse.class,
          healthy.call(
              new Declare(
                  healthy.request(), DurableServerTest.operation(1), 0, List.of(1L), true)));
      healthy.sendInput(DurableServerTest.header(2, 2, WORK, input, "copy/v2", 0), input, true);
      assertInstanceOf(AdmissionResponse.class, healthy.next());
      assertEquals(Records.State.SUCCEEDED, DurableServerTest.awaitTerminal(healthy, WORK).state());
      // Stream exhaustion: the negotiated stream limit is enforced by the transport parameters
      // (MAX_STREAMS), so a third concurrent input cannot even be opened while two are live;
      // earlier ones proceed, and resetting them releases the slots.
      QuicStreamChannel one =
          withStreamCredit(
              () ->
                  stalled.sendInput(
                      DurableServerTest.header(
                          1, 3, new Records.WorkKey(0, 0, 2), input, "copy/v2", 0),
                      Arrays.copyOf(input, 10),
                      false));
      QuicStreamChannel two =
          withStreamCredit(
              () ->
                  stalled.sendInput(
                      DurableServerTest.header(
                          1, 4, new Records.WorkKey(0, 0, 3), input, "copy/v2", 0),
                      Arrays.copyOf(input, 10),
                      false));
      Throwable limit =
          assertThrows(
              Throwable.class,
              () ->
                  stalled
                      .connection
                      .createStream(
                          QuicStreamType.UNIDIRECTIONAL, new ChannelInboundHandlerAdapter())
                      .get(5, TimeUnit.SECONDS));
      assertTrue(String.valueOf(limit.getCause()).contains("STREAM_LIMIT"), String.valueOf(limit));
      one.shutdownOutput(0x204).sync();
      two.shutdownOutput(0x204).sync();
      for (int i = 0; i < 2; i++) {
        Refusal reset = assertInstanceOf(Refusal.class, stalled.next());
        assertTrue(reset.request().input(), reset.toString());
        assertEquals(
            ProtocolError.Code.INTEGRITY_ERROR, reset.code(), "interrupted input, as in Rust");
      }
      // Declarations survive every refused input; the retransmission admits and succeeds.
      try {
        withStreamCredit(() -> stalled.sendInput(partial, input, true));
      } catch (Exception failure) {
        java.util.List<Message> pending = new java.util.ArrayList<>();
        Message drained;
        while ((drained = stalled.messages.poll(1, TimeUnit.SECONDS)) != null) pending.add(drained);
        throw new AssertionError(
            "retransmission failed; connection close event: "
                + (stalled.closed.isDone() ? stalled.closed.get() : "connection still open")
                + "; control messages: "
                + pending
                + "; server inputs: "
                + authority.server.snapshot().toCompletableFuture().get(5, TimeUnit.SECONDS),
            failure);
      }
      assertInstanceOf(AdmissionResponse.class, stalled.next());
      assertEquals(Records.State.SUCCEEDED, DurableServerTest.awaitTerminal(stalled, WORK).state());
      DurableServer.Snapshot snapshot =
          authority.server.snapshot().toCompletableFuture().get(5, TimeUnit.SECONDS);
      assertEquals(0, snapshot.inputs());
      assertEquals(0, authority.host.inputs().usage().handles());
      stalled.call(new Detach(stalled.request()));
      healthy.call(new Detach(healthy.request()));
    }
  }

  @Test
  void controlFinBeforeDetachFailsTheConnectionAndDropsOnlyThePendingResponse() throws Exception {
    try (Authority authority = new Authority("fin", DurableOptions.defaults())) {
      long pending;
      try (RawDurablePeer peer = authority.peer("alice")) {
        assertInstanceOf(Binding.class, peer.call(new Create(peer.request(), 1, POLICY)));
        assertInstanceOf(
            DeclarationResponse.class,
            peer.call(
                new Declare(peer.request(), DurableServerTest.operation(1), 0, List.of(1L), true)));
        WatchResponse current =
            assertInstanceOf(WatchResponse.class, peer.call(new Watch(peer.request(), WORK, 0, 0)));
        pending = peer.request();
        peer.send(new Watch(pending, WORK, current.revision(), 5_000));
        // A client FIN without DETACH is a framing failure: the server fails the connection and
        // the parked watch never gets a response; nothing durable changes.
        peer.control.shutdownOutput().sync();
        QuicConnectionCloseEvent close = peer.closed.get(10, TimeUnit.SECONDS);
        assertEquals(
            ProtocolError.Code.FRAME_ERROR.applicationError(), close.error(), close.toString());
        assertNull(peer.messages.poll(), "no response may follow the failure");
      }
      try (RawDurablePeer again = authority.peer("alice")) {
        assertInstanceOf(
            Binding.class, again.call(new Attach(again.request(), "issuer-a", "alice", 1)));
        WatchResponse view =
            assertInstanceOf(
                WatchResponse.class, again.call(new Watch(again.request(), WORK, 0, 0)));
        assertEquals(Records.State.DECLARED, view.work().state());
        assertInstanceOf(Detached.class, again.call(new Detach(again.request())));
      }
    }
  }

  @Test
  void outputRetentionExpiryWhilePinnedByAReadCompletesTheTransferThenExpires() throws Exception {
    byte[] input = DurableServerTest.payload(200_000, 8);
    Records.Policy brief = new Records.Policy(30_000, 2_000, 120_000);
    try (Authority authority = new Authority("pinned", DurableOptions.defaults());
        RawDurablePeer peer = authority.peer("alice")) {
      assertInstanceOf(Binding.class, peer.call(new Create(peer.request(), 1, brief)));
      assertInstanceOf(
          DeclarationResponse.class,
          peer.call(
              new Declare(peer.request(), DurableServerTest.operation(1), 0, List.of(1L), true)));
      peer.sendInput(DurableServerTest.header(1, 2, WORK, input, "copy/v2", 0), input, true);
      assertInstanceOf(AdmissionResponse.class, peer.next());
      Records.WorkView view = DurableServerTest.awaitTerminal(peer, WORK);
      assertEquals(Records.State.SUCCEEDED, view.state());
      Records.Output output = view.manifest().outputs().get(0);
      InputStore.Usage retained = authority.host.inputs().usage();
      // Open the read but do not consume it: the transfer pins the output across its retention.
      peer.holdIncoming = true;
      long readRequest = peer.request();
      peer.send(new Read(readRequest, WORK, 1, 0, output.sha256()));
      Thread.sleep(3_500);
      InputStore.Usage whilePinned = authority.host.inputs().usage();
      peer.resumeIncoming();
      byte[] object = peer.nextObject();
      assertEquals(readRequest, DurableServerTest.decodeResultHeader(object).request());
      assertArrayEquals(
          input,
          Arrays.copyOfRange(object, DurableServerTest.headerLength(object), object.length),
          () -> "pinned output must stay readable; usage " + retained + " -> " + whilePinned);
      // Once released, the expired output is retired: later reads are refused and storage shrinks.
      Refusal expired = null;
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
      while (expired == null && System.nanoTime() < deadline) {
        long request = peer.request();
        Message reply = peer.call(new Read(request, WORK, 1, 0, output.sha256()));
        if (reply instanceof Refusal refusal) expired = refusal;
        else {
          peer.nextObject();
          Thread.sleep(250);
        }
      }
      assertNotNull(expired, "output never expired after release");
      assertTrue(
          expired.code() == ProtocolError.Code.EXPIRED
              || expired.code() == ProtocolError.Code.NOT_FOUND,
          expired.toString());
      deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
      while (authority.host.inputs().usage().files() >= retained.files()
          && System.nanoTime() < deadline) Thread.sleep(100);
      assertTrue(authority.host.inputs().usage().files() < retained.files(), "output file retired");
      assertEquals(Records.State.SUCCEEDED, DurableServerTest.awaitTerminal(peer, WORK).state());
      peer.call(new Detach(peer.request()));
    }
  }
}
