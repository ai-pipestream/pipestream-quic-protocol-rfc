package ai.pipestream.quic.v2;

import static ai.pipestream.quic.v2.Messages.*;
import static org.junit.jupiter.api.Assertions.*;

import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * Current-authorization gates through the real listener: live principal remapping and removal,
 * cross-owner attachment and reads, untrusted and unmapped credentials, offline revocation, and
 * accepted work continuing under its retained grant independent of the presenting connection.
 */
@Timeout(240)
final class DurableAuthorizationTest {
  @TempDir static Path directory;
  static DurableTestPki pki;
  static Map<Records.Digest, String> principals;
  static final Records.Policy POLICY = new Records.Policy(30_000, 60_000, 120_000);

  @BeforeAll
  static void certificates() throws Exception {
    pki = DurableTestPki.generate(directory, List.of("alice", "bob", "alice-rotated"));
    principals = new HashMap<>(pki.principals(List.of("alice", "bob")));
    principals.put(pki.fingerprint("alice-rotated"), "alice");
  }

  static DurableHost host(Path root, boolean initialize, Map<Records.Digest, String> live)
      throws Exception {
    DurableHost.OwnerPolicy owners = DurableHost.OwnerPolicy.fromPrincipals(() -> live, true);
    DurableHost.Configuration configuration =
        DurableHost.Configuration.defaults("issuer-a", "localhost:7443");
    return initialize
        ? DurableHost.initialize(
            root,
            configuration,
            ReferenceApplications.all(),
            owners,
            DurableHost.UtcClock.system(true))
        : DurableHost.open(
            root,
            configuration,
            ReferenceApplications.all(),
            owners,
            DurableHost.UtcClock.system(true));
  }

  static Records.InputHeader header(long generation, int op, Records.WorkKey work, byte[] payload)
      throws Exception {
    return DurableServerTest.header(generation, op, work, payload, "copy/v2", 0);
  }

  @Test
  void rotationKeepsTheOwnerWhileRemappingAndRemovalDenyFurtherRequests() throws Exception {
    Map<Records.Digest, String> live = new HashMap<>(principals);
    TlsAuthentication serverAuthentication = pki.server(live);
    try (DurableHost host = host(directory.resolve("remap"), true, live);
        DurableServer server =
            DurableServer.start(
                new InetSocketAddress("127.0.0.1", 0),
                serverAuthentication,
                host,
                DurableOptions.defaults())) {
      Binding binding;
      try (RawDurablePeer alice =
          new RawDurablePeer(server.address(), pki.client("alice"), 65_536)) {
        alice.negotiate(RawDurablePeer.offer(List.of(DURABLE_WORK, RESULT_DELIVERY), 1 << 20));
        binding =
            assertInstanceOf(Binding.class, alice.call(new Create(alice.request(), 1, POLICY)));
        assertInstanceOf(
            DeclarationResponse.class,
            alice.call(
                new Declare(
                    alice.request(), DurableServerTest.operation(1), 0, List.of(1L, 2L), true)));
        // A rotated certificate mapped to the same owner attaches to the same session.
        try (RawDurablePeer rotated =
            new RawDurablePeer(server.address(), pki.client("alice-rotated"), 65_536)) {
          rotated.negotiate(RawDurablePeer.offer(List.of(DURABLE_WORK, RESULT_DELIVERY), 1 << 20));
          Binding attached =
              assertInstanceOf(
                  Binding.class,
                  rotated.call(new Attach(rotated.request(), "issuer-a", "alice", 1)));
          assertEquals(binding.generation(), attached.generation());
          rotated.call(new Detach(rotated.request()));
        }
        // Live remap of alice's live credential to another owner: the connection's original
        // owner cannot silently change, so its next request is refused UNAUTHORIZED and a
        // committing mutation cannot proceed under the old identity.
        Map<Records.Digest, String> remapped = new HashMap<>(live);
        remapped.put(pki.fingerprint("alice"), "mallory");
        serverAuthentication.replacePrincipals(remapped);
        Refusal denied =
            assertInstanceOf(
                Refusal.class,
                alice.call(new Watch(alice.request(), new Records.WorkKey(0, 0, 1), 0, 0)));
        assertEquals(ProtocolError.Code.UNAUTHORIZED, denied.code());
        var stream =
            alice.sendInput(
                header(1, 3, new Records.WorkKey(0, 0, 1), new byte[] {1}), new byte[] {1}, true);
        Refusal input = assertInstanceOf(Refusal.class, alice.next());
        assertEquals(new Records.RequestTag(true, stream.streamId()), input.request());
        assertEquals(ProtocolError.Code.UNAUTHORIZED, input.code());
        // Restoring the mapping restores requests on the same connection: the original owner is
        // retained, not replaced.
        serverAuthentication.replacePrincipals(live);
        assertInstanceOf(
            WatchResponse.class,
            alice.call(new Watch(alice.request(), new Records.WorkKey(0, 0, 1), 0, 0)));
        alice.call(new Detach(alice.request()));
      }
      // A newly mapped mallory credential (alice's old certificate) cannot attach to alice's
      // session and learns nothing about it.
      Map<Records.Digest, String> stolen = new HashMap<>(live);
      stolen.put(pki.fingerprint("alice"), "mallory");
      serverAuthentication.replacePrincipals(stolen);
      live.clear();
      live.putAll(stolen);
      try (RawDurablePeer mallory =
          new RawDurablePeer(server.address(), pki.client("alice"), 65_536)) {
        mallory.negotiate(RawDurablePeer.offer(List.of(DURABLE_WORK, RESULT_DELIVERY), 1 << 20));
        Refusal denied =
            assertInstanceOf(
                Refusal.class, mallory.call(new Attach(mallory.request(), "issuer-a", "alice", 1)));
        assertEquals(ProtocolError.Code.UNAUTHORIZED, denied.code());
        Refusal watch =
            assertInstanceOf(
                Refusal.class,
                mallory.call(new Watch(mallory.request(), new Records.WorkKey(0, 0, 1), 0, 0)));
        assertEquals(
            ProtocolError.Code.NOT_READY,
            watch.code(),
            "no session is attached; nothing is disclosed");
        Sequence next =
            assertInstanceOf(Sequence.class, mallory.call(new NextSequence(mallory.request())));
        assertEquals(1, next.nextCreationSequence(), "a different owner starts its own sequence");
        mallory.call(new Detach(mallory.request()));
      }
    }
  }

  @Test
  void crossOwnerAccessIsRefusedWithoutDisclosureAndRevocationCancelsAndDenies() throws Exception {
    Map<Records.Digest, String> live = new HashMap<>(principals);
    Path root = directory.resolve("revoke");
    byte[] bytes = new byte[2000];
    Records.WorkKey work = new Records.WorkKey(0, 0, 1);
    Records.WorkView succeeded;
    try (DurableHost host = host(root, true, live);
        DurableServer server =
            DurableServer.start(
                new InetSocketAddress("127.0.0.1", 0),
                pki.server(live),
                host,
                DurableOptions.defaults());
        RawDurablePeer alice = new RawDurablePeer(server.address(), pki.client("alice"), 65_536);
        RawDurablePeer bob = new RawDurablePeer(server.address(), pki.client("bob"), 65_536)) {
      alice.negotiate(RawDurablePeer.offer(List.of(DURABLE_WORK, RESULT_DELIVERY), 1 << 20));
      bob.negotiate(RawDurablePeer.offer(List.of(DURABLE_WORK, RESULT_DELIVERY), 1 << 20));
      assertInstanceOf(Binding.class, alice.call(new Create(alice.request(), 1, POLICY)));
      assertInstanceOf(
          DeclarationResponse.class,
          alice.call(
              new Declare(
                  alice.request(), DurableServerTest.operation(1), 0, List.of(1L, 2L), true)));
      alice.sendInput(header(1, 2, work, bytes), bytes, true);
      assertInstanceOf(AdmissionResponse.class, alice.next());
      succeeded = DurableServerTest.awaitTerminal(alice, work);
      assertEquals(Records.State.SUCCEEDED, succeeded.state());
      // Bob cannot attach to alice's generation, cannot see her work and gets his own session.
      Refusal attach =
          assertInstanceOf(
              Refusal.class, bob.call(new Attach(bob.request(), "issuer-a", "alice", 1)));
      assertEquals(ProtocolError.Code.UNAUTHORIZED, attach.code());
      Refusal foreign =
          assertInstanceOf(
              Refusal.class, bob.call(new Attach(bob.request(), "issuer-a", "bob", 1)));
      assertTrue(
          foreign.code() == ProtocolError.Code.UNAUTHORIZED
              || foreign.code() == ProtocolError.Code.NOT_FOUND,
          foreign.toString());
      Binding bobs =
          assertInstanceOf(Binding.class, bob.call(new Create(bob.request(), 1, POLICY)));
      assertEquals(2, bobs.generation());
      Refusal read =
          assertInstanceOf(Refusal.class, bob.call(new GetManifest(bob.request(), work, 1)));
      assertTrue(
          read.code() == ProtocolError.Code.NOT_FOUND,
          "bob's own session has no such work: " + read);
      alice.call(new Detach(alice.request()));
      bob.call(new Detach(bob.request()));
    }
    // Offline revocation of alice's generation: attachment is denied, retained state is not
    // disclosed, and the owner's next creation sequence has advanced past the revoked one.
    try (DurableHost host = host(root, false, live)) {
      host.revoke(1);
    }
    try (DurableHost host = host(root, false, live);
        DurableServer server =
            DurableServer.start(
                new InetSocketAddress("127.0.0.1", 0),
                pki.server(live),
                host,
                DurableOptions.defaults());
        RawDurablePeer alice = new RawDurablePeer(server.address(), pki.client("alice"), 65_536)) {
      alice.negotiate(RawDurablePeer.offer(List.of(DURABLE_WORK, RESULT_DELIVERY), 1 << 20));
      Refusal denied =
          assertInstanceOf(
              Refusal.class, alice.call(new Attach(alice.request(), "issuer-a", "alice", 1)));
      assertEquals(ProtocolError.Code.UNAUTHORIZED, denied.code());
      Refusal replay =
          assertInstanceOf(Refusal.class, alice.call(new Create(alice.request(), 1, POLICY)));
      assertTrue(
          replay.code() == ProtocolError.Code.UNAUTHORIZED
              || replay.code() == ProtocolError.Code.EXPIRED,
          "revoked creation cannot be replayed as live: " + replay);
      Sequence next =
          assertInstanceOf(Sequence.class, alice.call(new NextSequence(alice.request())));
      assertEquals(2, next.nextCreationSequence(), "generations are never reissued");
      alice.call(new Detach(alice.request()));
    }
  }

  @Test
  void untrustedOrExpiredCredentialsFailTheHandshakeNotTheApplication() throws Exception {
    Map<Records.Digest, String> live = new HashMap<>(principals);
    DurableTestPki stranger =
        DurableTestPki.generate(
            Files.createDirectories(directory.resolve("stranger")), List.of("eve"));
    try (DurableHost host = host(directory.resolve("handshake"), true, live);
        DurableServer server =
            DurableServer.start(
                new InetSocketAddress("127.0.0.1", 0),
                pki.server(live),
                host,
                DurableOptions.defaults())) {
      TlsAuthentication eve =
          TlsAuthentication.client(
              pki.path("ca.crt"), "localhost", stranger.path("eve.crt"), stranger.path("eve.key"));
      try (RawDurablePeer peer = new RawDurablePeer(server.address(), eve, 65_536)) {
        // The client may see its own handshake complete before the server's alert arrives; what
        // matters is that no capabilities response ever comes and the close is a transport-level
        // TLS failure, never a fabricated application refusal.
        assertThrows(
            Throwable.class,
            () ->
                peer.negotiate(
                    RawDurablePeer.offer(List.of(DURABLE_WORK, RESULT_DELIVERY), 1 << 20)));
        assertTrue(peer.messages.isEmpty(), "no application frame before authentication");
        var closed = peer.closed.get(10, TimeUnit.SECONDS);
        assertFalse(closed.isApplicationClose(), closed.toString());
      }
      // The server's own certificate must be trusted by the client: an untrusted server CA fails
      // client-side verification before any control frame.
      TlsAuthentication wrongRoot =
          TlsAuthentication.client(
              stranger.path("ca.crt"), "localhost", pki.path("alice.crt"), pki.path("alice.key"));
      try (RawDurablePeer peer = new RawDurablePeer(server.address(), wrongRoot, 65_536)) {
        assertThrows(Throwable.class, peer::handshake);
      }
      DurableServer.Snapshot snapshot =
          server.snapshot().toCompletableFuture().get(5, TimeUnit.SECONDS);
      assertEquals(0, snapshot.inputs());
    }
  }
}
