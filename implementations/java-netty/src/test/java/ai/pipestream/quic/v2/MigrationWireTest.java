package ai.pipestream.quic.v2;

import static ai.pipestream.quic.v2.Messages.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * Section 12.1 (S12-010): connection migration does not change authenticated identity. The peer's
 * datagrams reach the listener from a new address mid-session (the relay rebinds its server-facing
 * socket, the passive migration a NAT rebinding causes); the same session continues under the same
 * owner without any re-authentication, and replies to the old address are simply lost.
 */
@Timeout(60)
class MigrationWireTest {
  @TempDir static Path directory;
  static DurableTestPki pki;
  static Map<Records.Digest, String> principals;
  static final Records.Policy POLICY = new Records.Policy(20_000, 60_000, 120_000);

  @BeforeAll
  static void certificates() throws Exception {
    pki = DurableTestPki.generate(directory, List.of("alice"));
    principals = pki.principals(List.of("alice"));
  }

  @Test
  void aRebindingPeerKeepsItsSessionAndOwnerWithoutReauthentication() throws Exception {
    byte[] first = DurableServerTest.payload(20_000, 5);
    byte[] second = DurableServerTest.payload(30_000, 6);
    try (DurableHost host =
            DurableHost.initialize(
                directory.resolve("migration"),
                DurableHost.Configuration.defaults("issuer-a", "localhost:7443"),
                ReferenceApplications.all(),
                DurableHost.OwnerPolicy.fromPrincipals(() -> principals, true),
                DurableHost.UtcClock.system(true));
        DurableServer server =
            DurableServer.start(
                new InetSocketAddress("127.0.0.1", 0),
                pki.server(principals),
                host,
                DurableOptions.defaults());
        DatagramRelay relay = new DatagramRelay(server.address(), 7L, 0, 0);
        RawDurablePeer peer = new RawDurablePeer(relay.address(), pki.client("alice"), 65_536)) {
      peer.negotiate(RawDurablePeer.offer(List.of(DURABLE_WORK, RESULT_DELIVERY), 1 << 20));
      Binding binding =
          assertInstanceOf(Binding.class, peer.call(new Create(peer.request(), 1, POLICY)));
      assertEquals("alice", binding.owner());
      assertInstanceOf(
          DeclarationResponse.class,
          peer.call(
              new Declare(
                  peer.request(), DurableServerTest.operation(1), 0, List.of(1L, 2L), true)));
      Records.WorkKey one = new Records.WorkKey(0, 0, 1);
      Records.WorkKey two = new Records.WorkKey(0, 0, 2);
      peer.sendInput(
          DurableServerTest.header(binding.generation(), 2, one, first, "copy/v2", 0), first, true);
      assertInstanceOf(AdmissionResponse.class, peer.next());
      assertEquals(Records.State.SUCCEEDED, DurableServerTest.awaitTerminal(peer, one).state());
      InetSocketAddress before = relay.serverFacingAddress();

      // The path changes under the established connection: new source port, old one closed.
      InetSocketAddress after = relay.rebind();
      assertNotEquals(before.getPort(), after.getPort());

      // Same connection, same session, same owner: control, input and result all continue, and
      // nothing was re-authenticated or re-attached on the way.
      Sequence sequence =
          assertInstanceOf(Sequence.class, peer.call(new NextSequence(peer.request())));
      assertEquals(2, sequence.nextCreationSequence());
      WatchResponse watched =
          assertInstanceOf(WatchResponse.class, peer.call(new Watch(peer.request(), one, 0, 0)));
      assertEquals(Records.State.SUCCEEDED, watched.work().state());
      peer.sendInput(
          DurableServerTest.header(binding.generation(), 3, two, second, "copy/v2", 0),
          second,
          true);
      assertInstanceOf(AdmissionResponse.class, peer.next());
      assertEquals(1, binding.generation());
      Records.WorkView done = DurableServerTest.awaitTerminal(peer, two);
      assertEquals(Records.State.SUCCEEDED, done.state());
      assertEquals("alice", done.manifest().owner());
      assertTrue(
          relay.forwardedSinceRebind.get() > 0, "traffic reached the listener on the new path");
      assertEquals(1, relay.rebinds.get());
      assertFalse(peer.closed.isDone(), "the migration never closed the connection");
      assertInstanceOf(Detached.class, peer.call(new Detach(peer.request())));
      assertTrue(host.status().completedJobs() >= 2);
    }
  }
}
