package ai.pipestream.quic.v2;

import static ai.pipestream.quic.v2.Messages.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.netty.buffer.Unpooled;
import io.netty.handler.codec.quic.QuicConnectionCloseEvent;
import io.netty.handler.codec.quic.QuicStreamChannel;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * Section 12.1, 12.2 and 12.8 peer rules observed from a raw peer or a raw authority: an ignorable
 * frame activates nothing (S12-024), stream ids are never recycled across a long connection
 * (S12-068), an input whose admission response was already sent gets no second refusal (S12-077),
 * an unrecognised connection close code ends the transport without implying success (S12-082),
 * and the client's close after a completed drain carries application error 0 (S12-321).
 */
@Timeout(120)
class PeerRuleWireTest {
  @TempDir static Path directory;
  static DurableTestPki pki;
  static Map<Records.Digest, String> principals;
  static final Records.Policy POLICY = new Records.Policy(20_000, 60_000, 120_000);

  @BeforeAll
  static void certificates() throws Exception {
    pki = DurableTestPki.generate(directory, List.of("alice"));
    principals = pki.principals(List.of("alice"));
  }

  static DurableHost host(String name) throws Exception {
    return DurableHost.initialize(
        directory.resolve(name),
        // Funded for a 100-member scope: the default file policy reserves the retained
        // promises' share of the log per declared member and refuses admissions past about 28.
        V2Main.configuration(
            Map.of(
                "authority",
                "issuer-a",
                "result-authority",
                "localhost:7443",
                "db-mib",
                "1024",
                "wal-mib",
                "256")),
        ReferenceApplications.all(),
        DurableHost.OwnerPolicy.fromPrincipals(() -> principals, true),
        DurableHost.UtcClock.system(true));
  }

  static DurableServer server(DurableHost host) throws Exception {
    return DurableServer.start(
        new InetSocketAddress("127.0.0.1", 0),
        pki.server(principals),
        host,
        DurableOptions.defaults());
  }

  static RawDurablePeer peer(DurableServer server, List<Integer> profiles) throws Exception {
    RawDurablePeer peer = new RawDurablePeer(server.address(), pki.client("alice"), 65_536);
    peer.negotiate(RawDurablePeer.offer(profiles, 1 << 20));
    return peer;
  }

  static DurableServer.Snapshot snapshot(DurableServer server) throws Exception {
    return server.snapshot().toCompletableFuture().get(5, TimeUnit.SECONDS);
  }

  /** Open a stream once the peer's allowance permits it; the listener returns slots in batches. */
  static QuicStreamChannel open(RawDurablePeer peer, Records.InputHeader header, byte[] bytes)
      throws Exception {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
    while (true) {
      try {
        return peer.sendInput(header, bytes, true);
      } catch (ExecutionException limit) {
        if (!String.valueOf(limit.getCause()).contains("STREAM_LIMIT")
            || System.nanoTime() > deadline) throw limit;
        Thread.sleep(20);
      }
    }
  }

  @Test
  void anIgnorableFrameActivatesNoProfile() throws Exception {
    try (DurableHost host = host("ignorable");
        DurableServer server = server(host);
        RawDurablePeer peer = peer(server, List.of(DURABLE_WORK))) {
      assertInstanceOf(Binding.class, peer.call(new Create(peer.request(), 1, POLICY)));
      // An ignorable frame with a body, then a result-delivery request the selection lacks.
      byte[] ignorable = new byte[5 + 64];
      ByteBuffer.wrap(ignorable).put((byte) 0x80).putInt(64);
      peer.sendBytes(ignorable);
      Refusal refused =
          assertInstanceOf(
              Refusal.class,
              peer.call(new GetManifest(peer.request(), new Records.WorkKey(0, 0, 1), 1)));
      assertEquals(ProtocolError.Code.EXTENSION_UNSUPPORTED, refused.code(), refused.toString());
      // The connection is intact and the frame consumed no request identity.
      assertInstanceOf(Sequence.class, peer.call(new NextSequence(peer.request())));
      assertInstanceOf(Detached.class, peer.call(new Detach(peer.request())));
    }
  }

  @Test
  void streamIdsAreNeverRecycledAcrossALongConnection() throws Exception {
    byte[] input = DurableServerTest.payload(2_000, 3);
    int transfers = 100;
    try (DurableHost host = host("ids");
        DurableServer server = server(host);
        RawDurablePeer peer = peer(server, List.of(DURABLE_WORK, RESULT_DELIVERY))) {
      Binding binding =
          assertInstanceOf(Binding.class, peer.call(new Create(peer.request(), 1, POLICY)));
      List<Long> members = new ArrayList<>();
      for (long id = 1; id <= transfers; id++) members.add(id);
      assertInstanceOf(
          DeclarationResponse.class,
          peer.call(
              new Declare(peer.request(), DurableServerTest.operation(1), 0, members, true)));
      long previous = -1;
      java.util.Set<Long> seen = new java.util.HashSet<>();
      for (int entity = 1; entity <= transfers; entity++) {
        Records.WorkKey work = new Records.WorkKey(0, 0, entity);
        // Every fifth input is truncated and refused; the rest admit. Refused, finished and
        // admitted streams all consume fresh ids.
        boolean refuse = entity % 5 == 0;
        byte[] bytes = refuse ? Arrays.copyOf(input, input.length - 1) : input;
        QuicStreamChannel stream =
            open(
                peer,
                DurableServerTest.header(
                    binding.generation(), 10 + entity, work, input, "copy/v2", 0),
                bytes);
        assertTrue(stream.streamId() > previous, "stream id regressed at entity " + entity);
        assertTrue(seen.add(stream.streamId()), "stream id reused at entity " + entity);
        previous = stream.streamId();
        Message response = peer.next();
        if (refuse) {
          assertEquals(
              ProtocolError.Code.INTEGRITY_ERROR,
              assertInstanceOf(Refusal.class, response).code(),
              response.toString());
        } else {
          assertEquals(
              new Records.RequestTag(true, stream.streamId()),
              assertInstanceOf(
                      AdmissionResponse.class, response, "entity " + entity + ": " + response)
                  .request());
        }
      }
      assertEquals(transfers, seen.size());
      assertInstanceOf(Detached.class, peer.call(new Detach(peer.request())));
    }
  }

  @Test
  void anInputWhoseAdmissionResponseWasSentGetsNoSecondRefusal() throws Exception {
    byte[] input = DurableServerTest.payload(30_000, 4);
    Records.WorkKey work = new Records.WorkKey(0, 0, 1);
    try (DurableHost host = host("replayed");
        DurableServer server = server(host);
        RawDurablePeer peer = peer(server, List.of(DURABLE_WORK, RESULT_DELIVERY))) {
      Binding binding =
          assertInstanceOf(Binding.class, peer.call(new Create(peer.request(), 1, POLICY)));
      assertInstanceOf(
          DeclarationResponse.class,
          peer.call(
              new Declare(peer.request(), DurableServerTest.operation(1), 0, List.of(1L), true)));
      Records.InputHeader header =
          DurableServerTest.header(binding.generation(), 2, work, input, "copy/v2", 0);
      peer.sendInput(header, input, true);
      AdmissionResponse admitted = assertInstanceOf(AdmissionResponse.class, peer.next());
      assertEquals(Records.State.SUCCEEDED, DurableServerTest.awaitTerminal(peer, work).state());
      // The same admission again, with a payload that would be refused as an invalid input if it
      // were read: the retained response is replayed from the header and no REFUSAL follows.
      QuicStreamChannel replay =
          peer.sendInput(header, Arrays.copyOf(input, input.length / 2), true);
      AdmissionResponse replayed = assertInstanceOf(AdmissionResponse.class, peer.next());
      assertEquals(new Records.RequestTag(true, replay.streamId()), replayed.request());
      assertEquals(admitted.receipt(), replayed.receipt());
      assertNull(peer.messages.poll(1500, TimeUnit.MILLISECONDS), "a second control response");
      assertEquals(
          Records.State.SUCCEEDED,
          assertInstanceOf(WatchResponse.class, peer.call(new Watch(peer.request(), work, 0, 0)))
              .work()
              .state());
      assertInstanceOf(Detached.class, peer.call(new Detach(peer.request())));
    }
  }

  @Test
  void anUnrecognisedConnectionCloseCodeEndsTheTransportWithoutImplyingSuccess()
      throws Exception {
    byte[] input = DurableServerTest.payload(10_000, 5);
    Records.WorkKey work = new Records.WorkKey(0, 0, 1);
    try (DurableHost host = host("unknown-close");
        DurableServer server = server(host)) {
      try (RawDurablePeer peer = peer(server, List.of(DURABLE_WORK, RESULT_DELIVERY))) {
        Binding binding =
            assertInstanceOf(Binding.class, peer.call(new Create(peer.request(), 1, POLICY)));
        assertInstanceOf(
            DeclarationResponse.class,
            peer.call(
                new Declare(
                    peer.request(), DurableServerTest.operation(1), 0, List.of(1L, 2L), true)));
        peer.sendInput(
            DurableServerTest.header(binding.generation(), 2, work, input, "copy/v2", 0),
            input,
            true);
        assertInstanceOf(AdmissionResponse.class, peer.next());
        assertEquals(Records.State.SUCCEEDED, DurableServerTest.awaitTerminal(peer, work).state());
        assertEquals(1, snapshot(server).active());
        // Close with an application error the registry does not name.
        peer.connection.close(true, 0x1234, Unpooled.EMPTY_BUFFER).sync();
      }
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
      while (snapshot(server).active() != 0) {
        assertTrue(
            System.nanoTime() < deadline, "connection slot not released: " + snapshot(server));
        Thread.sleep(20);
      }
      assertEquals(0, snapshot(server).owners());
      // Nothing was implied by the unknown code: the session is intact, the declared member is
      // still DECLARED and the succeeded one still SUCCEEDED, and the owner can attach again.
      try (RawDurablePeer again = peer(server, List.of(DURABLE_WORK, RESULT_DELIVERY))) {
        assertInstanceOf(
            Binding.class, again.call(new Attach(again.request(), "issuer-a", "alice", 1)));
        assertEquals(
            Records.State.SUCCEEDED,
            assertInstanceOf(
                    WatchResponse.class, again.call(new Watch(again.request(), work, 0, 0)))
                .work()
                .state());
        assertEquals(
            Records.State.DECLARED,
            assertInstanceOf(
                    WatchResponse.class,
                    again.call(new Watch(again.request(), new Records.WorkKey(0, 0, 2), 0, 0)))
                .work()
                .state());
        assertInstanceOf(Detached.class, again.call(new Detach(again.request())));
      }
    }
  }

  @Test
  void aReceiverAbortOfAResultReadIsAnsweredByASenderReset() throws Exception {
    byte[] input = DurableServerTest.payload(200_000, 8);
    Records.WorkKey work = new Records.WorkKey(0, 0, 1);
    try (DurableHost host = host("receiver-abort");
        DurableServer server = server(host);
        RawDurablePeer peer = peer(server, List.of(DURABLE_WORK, RESULT_DELIVERY))) {
      Binding binding =
          assertInstanceOf(Binding.class, peer.call(new Create(peer.request(), 1, POLICY)));
      assertInstanceOf(
          DeclarationResponse.class,
          peer.call(
              new Declare(peer.request(), DurableServerTest.operation(1), 0, List.of(1L), true)));
      peer.sendInput(
          DurableServerTest.header(binding.generation(), 2, work, input, "copy/v2", 0),
          input,
          true);
      assertInstanceOf(AdmissionResponse.class, peer.next());
      Records.WorkView done = DurableServerTest.awaitTerminal(peer, work);
      Records.Output output = done.manifest().outputs().get(0);
      // Once the result stream has started, abort it from the receiving side with STOP_SENDING,
      // as the client does for a rejected delivery. The sender's side then ends: the listener
      // releases the read (its snapshot counts none) with most of the object unsent, and no
      // control response follows for the request. The pinned codec does not expose the
      // RESET_STREAM code to the receiver; that is the observable half.
      // The peer holds its reads, so the sender fills the 64 KiB stream window and stalls on flow
      // control with most of the object unsent; one read batch proves the stream started.
      peer.holdIncoming = true;
      peer.send(new Read(peer.request(), work, 1, 0, output.sha256()));
      RawDurablePeer.Incoming incoming = peer.nextIncoming();
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
      while (peer.held.isEmpty()) {
        assertTrue(System.nanoTime() < deadline, "result stream held");
        Thread.sleep(10);
      }
      incoming.stream.read();
      while (incoming.bytes.size() == 0) {
        assertTrue(System.nanoTime() < deadline, "result stream started");
        Thread.sleep(10);
      }
      incoming.stream.shutdownInput(0x204).sync();
      deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
      while (snapshot(server).results() != 0) {
        assertTrue(
            System.nanoTime() < deadline, "result read not released: " + snapshot(server));
        Thread.sleep(20);
      }
      // The peer's held channel does not report the sender's reset while its reads stay off;
      // the release at the listener and the partial object are the evidence.
      byte[] received = incoming.bytes.toByteArray();
      assertTrue(
          received.length < DurableServerTest.headerLength(received) + input.length,
          "the object was not delivered in full");
      assertNull(peer.messages.poll(1500, TimeUnit.MILLISECONDS), "a control response");
      assertInstanceOf(Sequence.class, peer.call(new NextSequence(peer.request())));
      assertInstanceOf(Detached.class, peer.call(new Detach(peer.request())));
    }
  }

  @Test
  void objectsLargerThanTheStreamWindowFlowBothWaysWithBoundedCredit() throws Exception {
    // S12-041: the peer's stream window is 64 KiB and the object is sixteen times that. Both the
    // input admission and the result read complete exactly, consumed incrementally under credit
    // replenishment, and the peer's stream credit is whole again afterwards.
    byte[] input = DurableServerTest.payload(1 << 20, 11);
    Records.WorkKey work = new Records.WorkKey(0, 0, 1);
    try (DurableHost host = host("large-object");
        DurableServer server = server(host);
        RawDurablePeer peer = peer(server, List.of(DURABLE_WORK, RESULT_DELIVERY))) {
      int allowance = peer.selected.streamLimit();
      Binding binding =
          assertInstanceOf(Binding.class, peer.call(new Create(peer.request(), 1, POLICY)));
      assertInstanceOf(
          DeclarationResponse.class,
          peer.call(
              new Declare(peer.request(), DurableServerTest.operation(1), 0, List.of(1L), true)));
      peer.sendInput(
          DurableServerTest.header(binding.generation(), 2, work, input, "copy/v2", 0),
          input,
          true);
      assertInstanceOf(AdmissionResponse.class, peer.next());
      Records.WorkView done = DurableServerTest.awaitTerminal(peer, work);
      assertEquals(Records.State.SUCCEEDED, done.state());
      Records.Output output = done.manifest().outputs().get(0);
      assertEquals(input.length, output.length());
      peer.send(new Read(peer.request(), work, 1, 0, output.sha256()));
      byte[] object = peer.nextObject();
      assertEquals(
          input.length, object.length - DurableServerTest.headerLength(object), "object bytes");
      assertTrue(
          Arrays.equals(
              input, 0, input.length, object, DurableServerTest.headerLength(object), object.length),
          "object content");
      // The single input slot comes back in the listener's next batched MAX_STREAMS update; the
      // credit is never below the allowance minus that one stream.
      assertTrue(peer.streamCredit() >= allowance - 1, "credit " + peer.streamCredit());
      assertInstanceOf(Detached.class, peer.call(new Detach(peer.request())));
    }
  }

  @Test
  void controlKeepsFlowingWhileEveryDataStreamIsSaturated() throws Exception {
    // S12-043, S12-044, S12-045: the control reservation is observed peer-side. Every input
    // stream of the allowance is opened with a partial payload and no FIN, so the listener holds
    // them all open and the data side is saturated; control requests on the same connection are
    // still answered promptly, and finishing the streams later returns every slot.
    byte[] input = DurableServerTest.payload(20_000, 12);
    try (DurableHost host = host("saturated");
        DurableServer server = server(host);
        RawDurablePeer peer = peer(server, List.of(DURABLE_WORK, RESULT_DELIVERY))) {
      int allowance = peer.selected.streamLimit();
      Binding binding =
          assertInstanceOf(Binding.class, peer.call(new Create(peer.request(), 1, POLICY)));
      List<Long> members = new ArrayList<>();
      for (long id = 1; id <= allowance; id++) members.add(id);
      assertInstanceOf(
          DeclarationResponse.class,
          peer.call(
              new Declare(peer.request(), DurableServerTest.operation(1), 0, members, true)));
      List<QuicStreamChannel> open = new ArrayList<>();
      for (int entity = 1; entity <= allowance; entity++) {
        open.add(
            peer.sendInput(
                DurableServerTest.header(
                    binding.generation(),
                    10 + entity,
                    new Records.WorkKey(0, 0, entity),
                    input,
                    "copy/v2",
                    0),
                Arrays.copyOf(input, input.length / 2),
                false));
      }
      assertEquals(0, peer.streamCredit(), "every data stream slot is in use");
      // Control on the saturated connection: ten requests, each answered within a second.
      for (int round = 0; round < 10; round++) {
        long started = System.nanoTime();
        assertInstanceOf(Sequence.class, peer.call(new NextSequence(peer.request())));
        WatchResponse watched =
            assertInstanceOf(
                WatchResponse.class,
                peer.call(new Watch(peer.request(), new Records.WorkKey(0, 0, 1), 0, 0)));
        assertEquals(Records.State.DECLARED, watched.work().state());
        long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);
        assertTrue(
            elapsedMs < 1000, "control stalled behind saturated data: " + elapsedMs + " ms");
      }
      // Finish every input: each admits, and the credit is whole again.
      for (int entity = 1; entity <= allowance; entity++) {
        QuicStreamChannel stream = open.get(entity - 1);
        stream
            .writeAndFlush(
                io.netty.buffer.Unpooled.wrappedBuffer(
                    input, input.length / 2, input.length - input.length / 2))
            .sync();
        stream.shutdownOutput().sync();
      }
      for (int entity = 1; entity <= allowance; entity++)
        assertInstanceOf(AdmissionResponse.class, peer.next());
      peer.awaitStreamCredit(allowance);
      assertInstanceOf(Detached.class, peer.call(new Detach(peer.request())));
    }
  }

  @Test
  void theClientClosesACompletedSessionWithApplicationErrorZero() throws Exception {
    try (RawDurableAuthority authority =
            new RawDurableAuthority(
                pki.server(principals),
                DurableClientControlDeadlineTest.manifestFor(new byte[1000]),
                DurableClientControlDeadlineTest.POLICY);
        ClientJournal journal =
            ClientJournal.initialize(
                directory.resolve("close-zero.sqlite"),
                new ClientJournal.Intent(
                    "issuer-a", "alice", 1, DurableClientControlDeadlineTest.POLICY, true),
                ClientJournal.Limits.defaults())) {
      DurableClient client =
          DurableClient.connect(
              authority.address(), pki.client("alice"), journal, ClientOptions.defaults());
      DurableClientTest.get(client.ready());
      DurableClientTest.get(client.binding());
      DurableClientTest.get(client.detach());
      client.close();
      QuicConnectionCloseEvent close = authority.clientClosed.get(10, TimeUnit.SECONDS);
      assertTrue(close.isApplicationClose(), close.toString());
      assertEquals(0, close.error(), close.toString());
    }
  }
}
