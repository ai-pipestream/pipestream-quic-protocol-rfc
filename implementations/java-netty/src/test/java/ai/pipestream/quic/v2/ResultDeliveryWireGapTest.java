package ai.pipestream.quic.v2;

import static ai.pipestream.quic.v2.Messages.*;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * Section 12.7 result delivery over real QUIC: unexpectedly missing retained storage is
 * OUTPUT_UNAVAILABLE without changing the authoritative outcome (S12-265), and a revoked session
 * cannot begin a new read (S12-277). Both clauses were proven only at the store layer before.
 */
@Timeout(120)
class ResultDeliveryWireGapTest {
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

  static final class Session implements AutoCloseable {
    final Path root;
    final DurableHost host;
    final DurableServer server;
    final RawDurablePeer peer;
    final Binding binding;

    Session(String name) throws Exception {
      root = directory.resolve(name);
      host =
          DurableHost.initialize(
              root,
              DurableHost.Configuration.defaults("issuer-a", "localhost:7443"),
              ReferenceApplications.all(),
              DurableHost.OwnerPolicy.fromPrincipals(() -> principals, true),
              DurableHost.UtcClock.system(true));
      server =
          DurableServer.start(
              new InetSocketAddress("127.0.0.1", 0),
              pki.server(principals),
              host,
              DurableOptions.defaults());
      peer = new RawDurablePeer(server.address(), pki.client("alice"), 65_536);
      peer.negotiate(RawDurablePeer.offer(List.of(DURABLE_WORK, RESULT_DELIVERY), 1 << 20));
      binding = assertInstanceOf(Binding.class, peer.call(new Create(peer.request(), 1, POLICY)));
    }

    /** Declare, admit and run one copy/v2 unit; return its terminal view. */
    Records.WorkView succeed(byte[] input) throws Exception {
      assertInstanceOf(
          DeclarationResponse.class,
          peer.call(
              new Declare(
                  peer.request(), DurableServerTest.operation(1), 0, List.of(1L, 2L), true)));
      peer.sendInput(
          DurableServerTest.header(binding.generation(), 2, WORK, input, "copy/v2", 0),
          input,
          true);
      assertInstanceOf(AdmissionResponse.class, peer.next());
      Records.WorkView view = DurableServerTest.awaitTerminal(peer, WORK);
      assertEquals(Records.State.SUCCEEDED, view.state());
      return view;
    }

    /** Read the published output over the wire and return the delivered payload bytes. */
    byte[] read(Records.Output output) throws Exception {
      long request = peer.request();
      peer.send(new Read(request, WORK, 1, 0, output.sha256()));
      byte[] object = peer.nextObject();
      assertEquals(request, DurableServerTest.decodeResultHeader(object).request());
      return Arrays.copyOfRange(object, DurableServerTest.headerLength(object), object.length);
    }

    /** The delivered read releases its slot shortly after the last byte; wait for that. */
    void awaitNoPendingReads() throws Exception {
      long deadline = System.nanoTime() + 5_000_000_000L;
      while (host.status().pendingReads() != 0 && System.nanoTime() < deadline) Thread.sleep(20);
      assertEquals(0, host.status().pendingReads(), "a read is still pending");
    }

    /** Delete every installed output object under the authority root; return how many. */
    int deleteInstalledOutputs() throws Exception {
      int deleted = 0;
      try (Stream<Path> files = Files.walk(root)) {
        for (Path file : files.filter(f -> f.toString().endsWith(".output")).toList()) {
          Files.delete(file);
          deleted++;
        }
      }
      return deleted;
    }

    @Override
    public void close() throws java.io.IOException {
      peer.close();
      server.close();
      host.close();
    }
  }

  @Test
  void missingRetainedOutputIsUnavailableOverTheWireAndTheOutcomeStands() throws Exception {
    byte[] input = DurableServerTest.payload(4096, 11);
    try (Session session = new Session("unavailable")) {
      Records.WorkView view = session.succeed(input);
      Records.Output output = view.manifest().outputs().get(0);
      assertArrayEquals(input, session.read(output));

      assertTrue(session.deleteInstalledOutputs() > 0, "no installed output object found");

      // Retained storage unexpectedly missing: OUTPUT_UNAVAILABLE, correlated to the request.
      long request = session.peer.request();
      Refusal unavailable =
          assertInstanceOf(
              Refusal.class,
              session.peer.call(new Read(request, WORK, 1, 0, output.sha256())));
      assertEquals(ProtocolError.Code.OUTPUT_UNAVAILABLE, unavailable.code());
      assertEquals(request, unavailable.request().id());

      // Never an automatic rerun or a new successful result: the outcome and manifest stand.
      WatchResponse watch =
          assertInstanceOf(
              WatchResponse.class,
              session.peer.call(new Watch(session.peer.request(), WORK, 0, 0)));
      assertEquals(Records.State.SUCCEEDED, watch.work().state());
      assertEquals(1, watch.work().attempt());
      ManifestResponse manifest =
          assertInstanceOf(
              ManifestResponse.class,
              session.peer.call(new GetManifest(session.peer.request(), WORK, 1)));
      assertEquals(view.manifest(), manifest.manifest());
      // A second read is refused the same way, and nothing was left pinned.
      assertEquals(
          ProtocolError.Code.OUTPUT_UNAVAILABLE,
          assertInstanceOf(
                  Refusal.class,
                  session.peer.call(
                      new Read(session.peer.request(), WORK, 1, 0, output.sha256())))
              .code());
      session.awaitNoPendingReads();
    }
  }

  @Test
  void revocationStopsNewReadsWithoutGrowingUsage() throws Exception {
    byte[] input = DurableServerTest.payload(4096, 12);
    try (Session session = new Session("revoked")) {
      Records.WorkView view = session.succeed(input);
      Records.Output output = view.manifest().outputs().get(0);
      assertArrayEquals(input, session.read(output));
      session.awaitNoPendingReads();

      session.host.revoke(session.binding.generation());

      // A fresh read after revocation is denied and never becomes a pending read.
      Message answer =
          session.peer.call(new Read(session.peer.request(), WORK, 1, 0, output.sha256()));
      Refusal denied = assertInstanceOf(Refusal.class, answer, "read after revocation: " + answer);
      assertEquals(ProtocolError.Code.UNAUTHORIZED, denied.code());
      session.awaitNoPendingReads();
    }
  }
}
