package ai.pipestream.quic.v2;

import static ai.pipestream.quic.v2.Messages.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * Section 12.7 (S12-079): once a result header has started the response, a later sender error
 * aborts the result stream and never produces a second control response for that request. The
 * published object is truncated on disk by a hook the moment the header has been accepted by the
 * transport, so the failure is real and lands mid-payload.
 */
@Timeout(60)
class ResultAbortWireTest {
  @TempDir static Path directory;
  static DurableTestPki pki;
  static Map<Records.Digest, String> principals;
  static final Records.Policy POLICY = new Records.Policy(20_000, 60_000, 120_000);
  static final Records.WorkKey WORK = new Records.WorkKey(0, 0, 1);

  @BeforeAll
  static void certificates() throws Exception {
    pki = DurableTestPki.generate(directory, List.of("alice"));
    principals = pki.principals(List.of("alice"));
  }

  static Path publishedObject(Path root) throws IOException {
    try (Stream<Path> files = Files.list(root.resolve(DurableHost.OBJECTS).resolve("outputs"))) {
      List<Path> objects = files.filter(p -> p.getFileName().toString().endsWith(".output")).toList();
      assertEquals(1, objects.size(), "one published object: " + objects);
      return objects.get(0);
    }
  }

  @Test
  void senderFailureAfterTheHeaderAbortsTheStreamWithoutASecondControlResponse()
      throws Exception {
    Path root = directory.resolve("abort");
    AtomicReference<Throwable> truncation = new AtomicReference<>();
    AtomicReference<Long> truncatedTo = new AtomicReference<>();
    Boundaries truncate =
        new Boundaries() {
          @Override
          public void committed(Boundary boundary, Details details) {}

          @Override
          public void sent(Boundary boundary, Details details) {
            if (boundary != Boundary.RESULT_HEADER_SENT || truncatedTo.get() != null) return;
            try (FileChannel channel =
                FileChannel.open(publishedObject(root), StandardOpenOption.WRITE)) {
              long half = channel.size() / 2;
              channel.truncate(half);
              truncatedTo.set(half);
            } catch (Throwable failure) {
              truncation.set(failure);
            }
          }

          @Override
          public boolean withhold(Boundary boundary) {
            return false;
          }
        };
    byte[] input = DurableServerTest.payload(900_000, 23);
    try (DurableHost host =
            DurableHost.initialize(
                root,
                DurableHost.Configuration.defaults("issuer-a", "localhost:7443"),
                ReferenceApplications.all(),
                DurableHost.OwnerPolicy.fromPrincipals(() -> principals, true),
                DurableHost.UtcClock.system(true));
        DurableServer server =
            DurableServer.start(
                new InetSocketAddress("127.0.0.1", 0),
                pki.server(principals),
                host,
                DurableOptions.defaults(),
                truncate);
        RawDurablePeer peer = new RawDurablePeer(server.address(), pki.client("alice"), 65_536)) {
      host.boundaries(truncate);
      peer.negotiate(RawDurablePeer.offer(List.of(DURABLE_WORK, RESULT_DELIVERY), 1 << 20));
      Binding binding =
          assertInstanceOf(Binding.class, peer.call(new Create(peer.request(), 1, POLICY)));
      assertInstanceOf(
          DeclarationResponse.class,
          peer.call(
              new Declare(peer.request(), DurableServerTest.operation(1), 0, List.of(1L), true)));
      peer.sendInput(
          DurableServerTest.header(binding.generation(), 2, WORK, input, "copy/v2", 0), input, true);
      assertInstanceOf(AdmissionResponse.class, peer.next());
      Records.WorkView succeeded = DurableServerTest.awaitTerminal(peer, WORK);
      assertEquals(Records.State.SUCCEEDED, succeeded.state());
      Records.Output output = succeeded.manifest().outputs().get(0);
      // The stored object carries its own framing around the payload bytes.
      assertTrue(Files.size(publishedObject(root)) >= input.length);

      long readRequest = peer.request();
      peer.send(new Read(readRequest, WORK, 1, 0, output.sha256()));
      RawDurablePeer.Incoming incoming = peer.nextIncoming();
      // The header started the response; the object then shrank under the sender, which must
      // abort the stream (no FIN) rather than answer the request again on the control stream.
      ExecutionException aborted =
          assertThrows(ExecutionException.class, () -> incoming.complete.get(20, TimeUnit.SECONDS));
      assertNotNull(aborted.getCause(), String.valueOf(aborted));
      assertNull(truncation.get(), "the hook truncated the object: " + truncation.get());
      assertNotNull(truncatedTo.get(), "the header boundary was reached");
      byte[] received = incoming.bytes.toByteArray();
      assertTrue(received.length >= DurableServerTest.headerLength(received), "header received");
      Records.ResultHeader header = DurableServerTest.decodeResultHeader(received);
      assertEquals(readRequest, header.request());
      assertEquals(input.length, header.length());
      assertTrue(received.length < DurableServerTest.headerLength(received) + input.length);
      assertNull(peer.messages.poll(2, TimeUnit.SECONDS), "no second control response");
      // The connection and the published outcome survive: the failure was one delivery.
      WatchResponse view =
          assertInstanceOf(WatchResponse.class, peer.call(new Watch(peer.request(), WORK, 0, 0)));
      assertEquals(Records.State.SUCCEEDED, view.work().state());
      assertInstanceOf(Detached.class, peer.call(new Detach(peer.request())));
    }
  }
}
