package ai.pipestream.quic.v2;

import static ai.pipestream.quic.v2.Messages.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * Section 12.3: credential expiry prevents new requests on that connection; a fresh credential
 * may reconnect to the same retained principal (S12-098). The server's authentication clock is
 * moved to the live credential's expiry; the connection is closed UNAUTHORIZED, the expired
 * credential cannot connect again, and a longer-lived certificate mapped to the same principal
 * attaches to the same retained session generation and sees its declarations.
 */
@Timeout(120)
class CredentialExpiryReconnectTest {
  @TempDir static Path directory;
  static DurableTestPki pki;
  static Map<Records.Digest, String> principals;
  static final Records.Policy POLICY = new Records.Policy(20_000, 60_000, 120_000);

  static final class MovingClock extends Clock {
    final AtomicReference<Instant> now = new AtomicReference<>(Instant.now());

    @Override
    public ZoneId getZone() {
      return ZoneOffset.UTC;
    }

    @Override
    public Clock withZone(ZoneId zone) {
      return this;
    }

    @Override
    public Instant instant() {
      return now.get();
    }
  }

  @BeforeAll
  static void certificates() throws Exception {
    pki = DurableTestPki.generate(directory, List.of("alice"));
    // A fresh credential for the same principal that outlives the first one by a day.
    pki.clientLeaf("alice-fresh", 3);
    principals = new HashMap<>(pki.principals(List.of("alice")));
    principals.put(pki.fingerprint("alice-fresh"), "alice");
  }

  static RawDurablePeer connect(DurableServer server, String credential) throws Exception {
    RawDurablePeer peer = new RawDurablePeer(server.address(), pki.client(credential), 65_536);
    peer.negotiate(RawDurablePeer.offer(List.of(DURABLE_WORK, RESULT_DELIVERY), 1 << 20));
    return peer;
  }

  @Test
  void expiryClosesTheConnectionAndAFreshCredentialAttachesToTheSameGeneration()
      throws Exception {
    MovingClock clock = new MovingClock();
    TlsAuthentication authentication =
        TlsAuthentication.server(
            pki.path("ca.crt"), pki.path("server.crt"), pki.path("server.key"), principals, clock);
    Instant aliceExpiry = pki.certificate("alice").getNotAfter().toInstant();
    Instant freshExpiry = pki.certificate("alice-fresh").getNotAfter().toInstant();
    assertTrue(freshExpiry.isAfter(aliceExpiry), "fresh credential must outlive the first");
    try (DurableHost host =
            DurableHost.initialize(
                directory.resolve("expiry"),
                DurableHost.Configuration.defaults("issuer-a", "localhost:7443"),
                ReferenceApplications.all(),
                DurableHost.OwnerPolicy.fromPrincipals(() -> principals, true),
                DurableHost.UtcClock.system(true));
        DurableServer server =
            DurableServer.start(
                new InetSocketAddress("127.0.0.1", 0),
                authentication,
                host,
                DurableOptions.defaults())) {
      Binding binding;
      try (RawDurablePeer alice = connect(server, "alice")) {
        binding = assertInstanceOf(Binding.class, alice.call(new Create(alice.request(), 1, POLICY)));
        assertInstanceOf(
            DeclarationResponse.class,
            alice.call(
                new Declare(
                    alice.request(), DurableServerTest.operation(1), 0, List.of(1L, 2L), true)));

        // The live credential expires: no further request is served on that connection.
        clock.now.set(aliceExpiry);
        alice.error(ProtocolError.Code.UNAUTHORIZED);
      }

      // The expired credential cannot come back.
      try (RawDurablePeer expired =
          new RawDurablePeer(server.address(), pki.client("alice"), 65_536)) {
        // The server verifies the chain against its authentication clock, so the expired
        // certificate is refused at the handshake: QUIC CRYPTO_ERROR carrying TLS alert
        // certificate_expired (45), never an application close and never an authenticated
        // REFUSAL.
        var close = expired.closed.get(10, java.util.concurrent.TimeUnit.SECONDS);
        assertFalse(close.isApplicationClose(), close.toString());
        assertEquals(0x100 + 45, close.error(), close.toString());
      }

      // A fresh credential mapped to the same principal reaches the same retained session.
      try (RawDurablePeer fresh = connect(server, "alice-fresh")) {
        Binding attached =
            assertInstanceOf(
                Binding.class,
                fresh.call(new Attach(fresh.request(), "issuer-a", "alice", binding.generation())));
        assertEquals(binding.generation(), attached.generation());
        assertEquals(binding.owner(), attached.owner());
        PageResponse page =
            assertInstanceOf(PageResponse.class, fresh.call(new Page(fresh.request(), 0, 0, 256)));
        assertEquals(2, page.declared());
        assertTrue(page.sealed());
        assertEquals(List.of(1L, 2L), page.entries().stream().map(Entry::entity).toList());
      }
    }
  }
}
