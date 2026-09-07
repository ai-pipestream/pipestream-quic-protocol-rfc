package ai.pipestream.quic.v2;

import static ai.pipestream.quic.v2.Messages.*;
import static org.junit.jupiter.api.Assertions.*;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.handler.codec.quic.InsecureQuicTokenHandler;
import io.netty.handler.codec.quic.QuicChannel;
import io.netty.handler.codec.quic.QuicClientCodecBuilder;
import io.netty.handler.codec.quic.QuicConnectionCloseEvent;
import io.netty.handler.codec.quic.QuicServerCodecBuilder;
import io.netty.handler.codec.quic.QuicSslContext;
import io.netty.handler.codec.quic.QuicSslContextBuilder;
import io.netty.handler.codec.quic.QuicStreamChannel;
import io.netty.handler.codec.quic.QuicStreamType;
import io.netty.util.AttributeKey;
import io.netty.util.concurrent.Future;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyFactory;
import java.security.Principal;
import java.security.PrivateKey;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.security.spec.PKCS8EncodedKeySpec;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.X509ExtendedKeyManager;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

@Timeout(60)
final class V2TlsTest {
  @TempDir static Path directory;
  private static final AttributeKey<TlsAuthentication.Guard> GUARD =
      AttributeKey.valueOf("v2-tls-test-guard");
  private static final AttributeKey<SSLEngine> ENGINE = AttributeKey.valueOf("v2-tls-test-engine");

  @BeforeAll
  static void certificates() throws Exception {
    for (String ca : List.of("ca", "other-ca")) {
      command(
          "openssl",
          "req",
          "-x509",
          "-newkey",
          "ec",
          "-pkeyopt",
          "ec_paramgen_curve:prime256v1",
          "-noenc",
          "-keyout",
          ca + ".key",
          "-out",
          ca + ".crt",
          "-days",
          "2",
          "-subj",
          "/CN=" + ca,
          "-addext",
          "basicConstraints=critical,CA:TRUE",
          "-addext",
          "keyUsage=critical,keyCertSign,cRLSign");
    }
    certificate(
        "server", "ca", "serverAuth", "DNS:localhost,IP:127.0.0.1,DNS:*.example.test", null);
    certificate("cn-only", "ca", "serverAuth", null, null);
    certificate("server-wrong-usage", "ca", "clientAuth", "DNS:localhost", null);
    for (String name : List.of("alice", "rotated", "unmapped"))
      certificate(name, "ca", "clientAuth", null, null);
    certificate("untrusted", "other-ca", "clientAuth", null, null);
    certificate("client-wrong-usage", "ca", "serverAuth", null, null);
    certificate("expired", "ca", "clientAuth", null, "20000101000000Z");
    certificate("future", "ca", "clientAuth", null, "20990101000000Z");
    command(
        "openssl",
        "x509",
        "-req",
        "-in",
        "alice.csr",
        "-CA",
        "ca.crt",
        "-CAkey",
        "ca.key",
        "-CAcreateserial",
        "-out",
        "reissued.crt",
        "-days",
        "2",
        "-extfile",
        "alice.ext");
    Files.copy(path("alice.key"), path("reissued.key"));
  }

  @Test
  void actualMutualTlsBindsRotatedCertificatesToTheSameOwner() throws Exception {
    TlsAuthentication server = server("server", Clock.systemUTC(), mapping());
    try (Network net = new Network(server)) {
      for (String credential : List.of("alice", "rotated")) {
        Pair pair = net.connect(client("localhost", credential), null);
        assertEquals("alice", pair.server.guard.requireOwner());
        assertEquals(1, pair.server.active.get());
        assertEquals(
            TlsAuthentication.ALPN, pair.client.channel.sslEngine().getApplicationProtocol());
        assertEquals("TLSv1.3", pair.client.channel.sslEngine().getSession().getProtocol());
        assertEquals(required(true), net.exchange(pair, required(false)));
      }
      assertEquals(2, net.responses.get());
    }
    assertNotEquals(
        TlsAuthentication.fingerprint(cert("alice")),
        TlsAuthentication.fingerprint(cert("rotated")));
    assertArrayEquals(
        cert("alice").getPublicKey().getEncoded(), cert("reissued").getPublicKey().getEncoded());
    assertNotEquals(
        TlsAuthentication.fingerprint(cert("alice")),
        TlsAuthentication.fingerprint(cert("reissued")));
  }

  @Test
  void absentOrUnmappedIdentityNeverActivatesRequiredDurability() throws Exception {
    for (String credential : new String[] {null, "unmapped", "reissued"}) {
      try (Network net = new Network(server("server", Clock.systemUTC(), mapping()))) {
        Pair optional = net.connect(client("localhost", credential), null);
        var selected = net.exchange(optional, optional(false));
        assertEquals(List.of(), selected.supported());
        assertEquals(List.of(), selected.required());
        assertThrows(ProtocolError.class, optional.server.guard::requireOwner);
        var resultRequired =
            new Capabilities(
                false,
                List.of(DURABLE_WORK, RESULT_DELIVERY),
                List.of(RESULT_DELIVERY),
                4096,
                4,
                8,
                1024,
                1000,
                5000);
        assertUnauthorized(() -> optional.server.guard.negotiate(resultRequired, optional(false)));
        assertUnauthorized(() -> optional.server.guard.negotiate(optional(false), resultRequired));
        Pair required = net.connect(client("localhost", credential), null);
        net.send(required, required(false));
        var close = required.client.closed.get(5, TimeUnit.SECONDS);
        assertTrue(close.isApplicationClose());
        assertEquals(ProtocolError.Code.UNAUTHORIZED.applicationError(), close.error());
        assertEquals(1, net.responses.get(), "required denial must precede capabilities response");
      }
    }
  }

  @Test
  void badClientCertificatesFailTlsWithoutApplicationActivation() throws Exception {
    try (Network net = new Network(server("server", Clock.systemUTC(), mapping()))) {
      for (String credential :
          List.of("untrusted", "client-wrong-usage", "expired", "future", "wrong-key")) {
        var attempt =
            net.attempt(client("localhost", null), external(credential, "pipestream/2", 0));
        Probe server = net.accepted.poll(5, TimeUnit.SECONDS);
        assertNotNull(server, credential);
        assertThrows(
            ExecutionException.class,
            () -> server.guard.ready().toCompletableFuture().get(5, TimeUnit.SECONDS),
            credential);
        var close =
            (credential.equals("wrong-key") ? server.closed : attempt.probe.closed)
                .get(5, TimeUnit.SECONDS);
        if (credential.equals("wrong-key")) {
          assertThrows(ExecutionException.class, () -> attempt.connection.get(5, TimeUnit.SECONDS));
          assertEquals(0, attempt.probe.active.get());
        }
        assertTlsClose(close);
        assertEquals(0, server.active.get(), credential);
        assertEquals(0, net.responses.get());
      }
    }
  }

  @Test
  void serverTrustUsageAndSanAreHandshakeChecksNotCommonNameFallback() throws Exception {
    for (String identity :
        List.of("other.test", "two.labels.example.test", "localhost.", "localhost%1")) {
      try (Network net = new Network(server("server", Clock.systemUTC(), mapping()))) {
        var attempt = net.attempt(client(identity, "alice"), null);
        Probe peer = net.accepted.poll(5, TimeUnit.SECONDS);
        assertNotNull(peer);
        assertThrows(
            ExecutionException.class,
            () -> attempt.probe.guard.ready().toCompletableFuture().get(5, TimeUnit.SECONDS));
        assertTlsClose(peer.closed.get(5, TimeUnit.SECONDS));
        assertEquals(0, attempt.probe.active.get());
      }
    }
    for (String certificate : List.of("cn-only", "server-wrong-usage")) {
      try (Network net = new Network(server(certificate, Clock.systemUTC(), mapping()))) {
        var attempt = net.attempt(client("localhost", "alice"), null);
        Probe peer = net.accepted.poll(5, TimeUnit.SECONDS);
        assertNotNull(peer);
        assertThrows(
            ExecutionException.class,
            () -> attempt.probe.guard.ready().toCompletableFuture().get(5, TimeUnit.SECONDS));
        assertTlsClose(peer.closed.get(5, TimeUnit.SECONDS));
        assertEquals(0, attempt.probe.active.get());
      }
    }
    try (Network net = new Network(server("server", Clock.systemUTC(), mapping()))) {
      for (String identity : List.of("LOCALHOST", "127.0.0.1", "worker.example.test")) {
        Pair pair = net.connect(client(identity, "alice"), null);
        assertEquals(required(true), net.exchange(pair, required(false)));
      }
      TlsAuthentication otherRoots =
          TlsAuthentication.client(
              path("other-ca.crt"), "localhost", path("alice.crt"), path("alice.key"));
      var attempt = net.attempt(otherRoots, null);
      Probe peer = net.accepted.poll(5, TimeUnit.SECONDS);
      assertNotNull(peer);
      assertThrows(
          ExecutionException.class,
          () -> attempt.probe.guard.ready().toCompletableFuture().get(5, TimeUnit.SECONDS));
      assertTlsClose(peer.closed.get(5, TimeUnit.SECONDS));
    }
  }

  @Test
  void currentExpiryAndMappingApplyToExistingConnectionsWithoutChangingOwners() throws Exception {
    MovingClock clock = new MovingClock();
    TlsAuthentication auth = server("server", clock, mapping());
    try (Network net = new Network(auth)) {
      Pair pair = net.connect(client("localhost", "alice"), null);
      assertEquals("alice", pair.server.guard.requireOwner());
      auth.replacePrincipals(Map.of(TlsAuthentication.fingerprint(cert("alice")), "bob"));
      assertUnauthorized(pair.server.guard::requireOwner);
      auth.replacePrincipals(Map.of());
      assertUnauthorized(pair.server.guard::requireOwner);
      auth.replacePrincipals(mapping());
      assertEquals("alice", pair.server.guard.requireOwner());
      clock.now.set(cert("alice").getNotAfter().toInstant());
      assertUnauthorized(pair.server.guard::requireOwner);
      assertUnauthorized(pair.server.guard::requireAuthenticated);
      net.send(pair, required(false));
      var close = pair.client.closed.get(5, TimeUnit.SECONDS);
      assertEquals(ProtocolError.Code.UNAUTHORIZED.applicationError(), close.error());
      assertEquals(0, net.responses.get());
    }
  }

  @Test
  void resumedExternalClientsRevalidateCurrentMappingAndCredentialExpiry() throws Exception {
    MovingClock clock = new MovingClock();
    TlsAuthentication auth = server("server", clock, mapping());
    QuicSslContext cached = external("alice", "pipestream/2", 8);
    TlsAuthentication verifier = client("localhost", null);
    try (Network net = new Network(auth)) {
      Pair first = net.connect(verifier, cached);
      assertFalse(resumed(first.server.channel));
      net.exchange(first, required(false));
      Pair second = net.connect(verifier, cached);
      assertTrue(
          resumed(second.server.channel),
          "must exercise actual TLS resumption, not a second full handshake");
      assertEquals("alice", second.server.guard.requireOwner());
      net.exchange(second, required(false));
      auth.replacePrincipals(Map.of());
      Pair removed = net.connect(verifier, cached);
      assertTrue(resumed(removed.server.channel));
      net.send(removed, required(false));
      assertEquals(
          ProtocolError.Code.UNAUTHORIZED.applicationError(),
          removed.client.closed.get(5, TimeUnit.SECONDS).error());
      auth.replacePrincipals(mapping());
      // Process a fresh ticket with an authenticated request before the expiry attempt.
      Pair refresh = net.connect(verifier, cached);
      net.exchange(refresh, required(false));
      clock.now.set(cert("alice").getNotAfter().toInstant());
      var expired = net.attempt(verifier, cached);
      Probe peer = net.accepted.poll(5, TimeUnit.SECONDS);
      assertNotNull(peer);
      assertThrows(
          ExecutionException.class,
          () -> peer.guard.ready().toCompletableFuture().get(5, TimeUnit.SECONDS));
      assertTrue(resumed(peer.channel), "expiry must be checked on an actual resumed handshake");
      assertTlsClose(expired.probe.closed.get(5, TimeUnit.SECONDS));
      assertEquals(0, peer.active.get());
      assertEquals(3, net.responses.get());
    }
  }

  @Test
  void builtInClientUsesFullHandshakesOnReconnect() throws Exception {
    TlsAuthentication caller = client("localhost", "alice");
    try (Network net = new Network(server("server", Clock.systemUTC(), mapping()))) {
      for (int connection = 0; connection < 3; connection++) {
        Pair pair = net.connect(caller, null);
        assertFalse(resumed(pair.server.channel));
        net.exchange(pair, required(false));
      }
    }
  }

  @Test
  void alpnMismatchNeverBecomesAnApplicationConnection() throws Exception {
    try (Network net = new Network(server("server", Clock.systemUTC(), mapping()))) {
      var attempt = net.attempt(client("localhost", null), external("alice", "pipestream/1", 0));
      Probe peer = net.accepted.poll(5, TimeUnit.SECONDS);
      assertNotNull(peer);
      assertThrows(
          ExecutionException.class,
          () -> peer.guard.ready().toCompletableFuture().get(5, TimeUnit.SECONDS));
      assertTlsClose(attempt.probe.closed.get(5, TimeUnit.SECONDS));
      assertEquals(0, peer.active.get());
      assertEquals(0, net.responses.get());
    }
  }

  @Test
  void configurationAndUnstartedGuardsFailClosed() throws Exception {
    TlsAuthentication auth = server("server", Clock.systemUTC(), mapping());
    assertUnauthorized(auth.guard()::requireAuthenticated);
    assertUnauthorized(auth.guard()::requireOwner);
    assertThrows(
        ProtocolError.class,
        () ->
            auth.replacePrincipals(
                Map.of(TlsAuthentication.fingerprint(cert("alice")), "wrong owner")));
    assertThrows(
        IllegalArgumentException.class,
        () -> TlsAuthentication.client(path("ca.crt"), "localhost", path("alice.crt"), null));
    Path empty = directory.resolve("empty-roots");
    Files.write(empty, new byte[0]);
    assertThrows(
        java.security.cert.CertificateException.class,
        () -> TlsAuthentication.client(empty, "localhost", null, null));
    Path excessive = directory.resolve("large-roots");
    Files.write(excessive, new byte[1048577]);
    assertThrows(
        java.security.cert.CertificateException.class,
        () -> TlsAuthentication.client(excessive, "localhost", null, null));
    Path manyRoots = directory.resolve("many-roots");
    Files.writeString(manyRoots, Files.readString(path("ca.crt")).repeat(257));
    assertTrue(Files.size(manyRoots) < 1048576);
    assertThrows(
        java.security.cert.CertificateException.class,
        () -> TlsAuthentication.client(manyRoots, "localhost", null, null));
    var tooMany = new java.util.HashMap<Records.Digest, String>();
    for (int n = 0; n <= 16384; n++) {
      byte[] digest = new byte[32];
      java.nio.ByteBuffer.wrap(digest).putInt(n);
      tooMany.put(new Records.Digest(digest), "alice");
    }
    assertThrows(IllegalArgumentException.class, () -> auth.replacePrincipals(tooMany));
  }

  private static void assertUnauthorized(org.junit.jupiter.api.function.Executable action) {
    assertEquals(ProtocolError.Code.UNAUTHORIZED, assertThrows(ProtocolError.class, action).code());
  }

  private static void assertTlsClose(QuicConnectionCloseEvent event) {
    assertFalse(event.isApplicationClose());
    assertTrue(event.error() >= 0x100 && event.error() < 0x200, event.toString());
  }

  private static Capabilities optional(boolean response) {
    return new Capabilities(
        response, List.of(DURABLE_WORK, RESULT_DELIVERY), List.of(), 4096, 4, 8, 1024, 1000, 5000);
  }

  private static Capabilities required(boolean response) {
    return new Capabilities(
        response,
        List.of(DURABLE_WORK, RESULT_DELIVERY),
        List.of(DURABLE_WORK, RESULT_DELIVERY),
        4096,
        4,
        8,
        1024,
        1000,
        5000);
  }

  private static Path path(String name) {
    return directory.resolve(name);
  }

  private static X509Certificate cert(String name) throws Exception {
    try (var input = Files.newInputStream(path(name + ".crt"))) {
      return (X509Certificate) CertificateFactory.getInstance("X.509").generateCertificate(input);
    }
  }

  private static Map<Records.Digest, String> mapping() throws Exception {
    return Map.of(
        TlsAuthentication.fingerprint(cert("alice")),
        "alice",
        TlsAuthentication.fingerprint(cert("rotated")),
        "alice");
  }

  private static TlsAuthentication server(String name, Clock clock, Map<Records.Digest, String> map)
      throws Exception {
    return TlsAuthentication.server(
        path("ca.crt"), path(name + ".crt"), path(name + ".key"), map, clock);
  }

  private static TlsAuthentication client(String reference, String credential) throws Exception {
    return TlsAuthentication.client(
        path("ca.crt"),
        reference,
        credential == null ? null : path(credential + ".crt"),
        credential == null ? null : path(credential + ".key"));
  }

  private static boolean resumed(QuicChannel channel) throws Exception {
    // Pinned Netty test observation only: no production reflection or session substitution.
    SSLEngine engine = channel.attr(ENGINE).get();
    var method = engine.getClass().getDeclaredMethod("isSessionReused");
    method.setAccessible(true);
    return (boolean) method.invoke(engine);
  }

  private static QuicSslContext external(String credential, String alpn, int cache)
      throws Exception {
    X509Certificate leaf = cert(credential.equals("wrong-key") ? "alice" : credential);
    String pem =
        Files.readString(path((credential.equals("wrong-key") ? "rotated" : credential) + ".key"))
            .replace("-----BEGIN PRIVATE KEY-----", "")
            .replace("-----END PRIVATE KEY-----", "")
            .replaceAll("\\s", "");
    PrivateKey key =
        KeyFactory.getInstance("EC")
            .generatePrivate(new PKCS8EncodedKeySpec(Base64.getDecoder().decode(pem)));
    // Deliberately send invalid test certificates even when the server's issuer hints differ.
    var keys =
        new X509ExtendedKeyManager() {
          @Override
          public String[] getClientAliases(String type, Principal[] issuers) {
            return new String[] {"test"};
          }

          @Override
          public String chooseClientAlias(String[] types, Principal[] issuers, Socket socket) {
            return "test";
          }

          @Override
          public String chooseEngineClientAlias(
              String[] types, Principal[] issuers, SSLEngine engine) {
            return "test";
          }

          @Override
          public String[] getServerAliases(String type, Principal[] issuers) {
            return new String[0];
          }

          @Override
          public String chooseServerAlias(String type, Principal[] issuers, Socket socket) {
            return null;
          }

          @Override
          public X509Certificate[] getCertificateChain(String alias) {
            return new X509Certificate[] {leaf};
          }

          @Override
          public PrivateKey getPrivateKey(String alias) {
            return key;
          }
        };
    return QuicSslContextBuilder.forClient()
        .keyManager(keys, null)
        .trustManager(path("ca.crt").toFile())
        .applicationProtocols(alpn)
        .sessionCacheSize(cache)
        .earlyData(false)
        .build();
  }

  private static void certificate(String name, String ca, String usage, String san, String start)
      throws Exception {
    command(
        "openssl",
        "req",
        "-new",
        "-newkey",
        "ec",
        "-pkeyopt",
        "ec_paramgen_curve:prime256v1",
        "-noenc",
        "-keyout",
        name + ".key",
        "-out",
        name + ".csr",
        "-subj",
        "/CN=localhost");
    Files.writeString(
        path(name + ".ext"),
        "basicConstraints=critical,CA:FALSE\nkeyUsage=critical,digitalSignature\nextendedKeyUsage="
            + usage
            + "\n"
            + (san == null ? "" : "subjectAltName=" + san + "\n"));
    var args =
        new ArrayList<>(
            List.of(
                "openssl",
                "x509",
                "-req",
                "-in",
                name + ".csr",
                "-CA",
                ca + ".crt",
                "-CAkey",
                ca + ".key",
                "-CAcreateserial",
                "-out",
                name + ".crt",
                "-days",
                "2",
                "-extfile",
                name + ".ext"));
    if (start != null) {
      Files.writeString(path(name + ".db"), "");
      Files.writeString(path(name + ".serial"), "01\n");
      Files.writeString(
          path(name + ".conf"),
          "[ca]\ndefault_ca=CA\n[CA]\ndatabase="
              + name
              + ".db\nserial="
              + name
              + ".serial\nnew_certs_dir=.\ncertificate="
              + ca
              + ".crt\nprivate_key="
              + ca
              + ".key\ndefault_md=sha256\npolicy=policy\n[policy]\ncommonName=supplied\n");
      args =
          new ArrayList<>(
              List.of(
                  "openssl",
                  "ca",
                  "-batch",
                  "-notext",
                  "-config",
                  name + ".conf",
                  "-in",
                  name + ".csr",
                  "-out",
                  name + ".crt",
                  "-extfile",
                  name + ".ext",
                  "-startdate",
                  start,
                  "-enddate",
                  start.startsWith("2000") ? "20000102000000Z" : "20990102000000Z"));
    }
    command(args.toArray(String[]::new));
  }

  private static void command(String... args) throws Exception {
    Path log = Files.createTempFile(directory, "openssl-", ".log");
    Process process =
        new ProcessBuilder(args)
            .directory(directory.toFile())
            .redirectErrorStream(true)
            .redirectOutput(log.toFile())
            .start();
    try {
      assertTrue(process.waitFor(10, TimeUnit.SECONDS));
      assertEquals(0, process.exitValue(), () -> log.toString());
    } finally {
      if (process.isAlive()) process.destroyForcibly().waitFor(5, TimeUnit.SECONDS);
    }
  }

  private static final class MovingClock extends Clock {
    final AtomicReference<Instant> now = new AtomicReference<>(Instant.now());

    @Override
    public ZoneId getZone() {
      return ZoneOffset.UTC;
    }

    @Override
    public Clock withZone(ZoneId zone) {
      if (!zone.equals(ZoneOffset.UTC)) throw new IllegalArgumentException();
      return this;
    }

    @Override
    public Instant instant() {
      return now.get();
    }
  }

  private record Attempt(Probe probe, Future<QuicChannel> connection) {}

  private record Pair(Probe client, Probe server) {}

  private static final class Probe extends ChannelInboundHandlerAdapter {
    final TlsAuthentication.Guard guard;
    final AtomicInteger active = new AtomicInteger();
    final CompletableFuture<QuicConnectionCloseEvent> closed = new CompletableFuture<>();
    volatile QuicChannel channel;

    Probe(TlsAuthentication.Guard guard) {
      this.guard = guard;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
      guard.requireAuthenticated();
      active.incrementAndGet();
      ctx.fireChannelActive();
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object event) {
      if (event instanceof QuicConnectionCloseEvent close) closed.complete(close);
      ctx.fireUserEventTriggered(event);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
      /* Guard and close events retain failure evidence. */
    }
  }

  /** Transport test fixture, not a durable endpoint or conformance implementation. */
  private static final class Network implements AutoCloseable {
    final MultiThreadIoEventLoopGroup group =
        new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
    final List<Channel> datagrams = new ArrayList<>();
    final LinkedBlockingQueue<Probe> accepted = new LinkedBlockingQueue<>();
    final AtomicInteger responses = new AtomicInteger();
    final Channel listener;

    Network(TlsAuthentication authentication) throws Exception {
      listener =
          new Bootstrap()
              .group(group)
              .channel(NioDatagramChannel.class)
              .handler(
                  new QuicServerCodecBuilder()
                      .sslEngineProvider(
                          c -> {
                            var engine = authentication.engine(c.alloc(), 0);
                            // Keep the actual engine observation available after native connection
                            // cleanup.
                            c.attr(ENGINE).set(engine);
                            return engine;
                          })
                      .maxIdleTimeout(10, TimeUnit.SECONDS)
                      .initialMaxData(65536)
                      .initialMaxStreamDataBidirectionalLocal(4096)
                      .initialMaxStreamDataBidirectionalRemote(4096)
                      .initialMaxStreamDataUnidirectional(4096)
                      .initialMaxStreamsBidirectional(1)
                      .initialMaxStreamsUnidirectional(4)
                      .tokenHandler(InsecureQuicTokenHandler.INSTANCE)
                      .handler(
                          new ChannelInitializer<QuicChannel>() {
                            @Override
                            protected void initChannel(QuicChannel channel) {
                              var guard = authentication.guard();
                              var probe = new Probe(guard);
                              probe.channel = channel;
                              channel.attr(GUARD).set(guard);
                              channel.pipeline().addLast(guard, probe);
                              accepted.add(probe);
                            }
                          })
                      .streamHandler(
                          new ChannelInitializer<QuicStreamChannel>() {
                            @Override
                            protected void initChannel(QuicStreamChannel channel) {
                              channel
                                  .pipeline()
                                  .addLast(
                                      new SimpleChannelInboundHandler<ByteBuf>() {
                                        final Wire.Decoder decoder = new Wire.Decoder(4096);

                                        @Override
                                        protected void channelRead0(
                                            ChannelHandlerContext ctx, ByteBuf bytes) {
                                          try {
                                            var guard = channel.parent().attr(GUARD).get();
                                            guard.requireAuthenticated();
                                            Wire.Frame frame = decoder.feed(bytes.nioBuffer());
                                            if (frame == null) return;
                                            var selected =
                                                guard.negotiate(
                                                    (Capabilities) ((Wire.Known) frame).message(),
                                                    optional(false));
                                            responses.incrementAndGet();
                                            ctx.writeAndFlush(
                                                Unpooled.wrappedBuffer(
                                                    Wire.encode(selected, 4096)));
                                          } catch (ProtocolError denied) {
                                            channel
                                                .parent()
                                                .close(
                                                    true,
                                                    (int) denied.code().applicationError(),
                                                    Unpooled.EMPTY_BUFFER);
                                          }
                                        }
                                      });
                            }
                          })
                      .build())
              .bind(new InetSocketAddress("127.0.0.1", 0))
              .sync()
              .channel();
      datagrams.add(listener);
    }

    Attempt attempt(TlsAuthentication authentication, QuicSslContext external) throws Exception {
      int port = ((InetSocketAddress) listener.localAddress()).getPort();
      Channel socket =
          new Bootstrap()
              .group(group)
              .channel(NioDatagramChannel.class)
              .handler(
                  new QuicClientCodecBuilder()
                      .sslEngineProvider(
                          c ->
                              external == null
                                  ? authentication.engine(c.alloc(), port)
                                  : external.newEngine(c.alloc(), "localhost", port))
                      .maxIdleTimeout(10, TimeUnit.SECONDS)
                      .initialMaxData(65536)
                      .initialMaxStreamDataBidirectionalLocal(4096)
                      .initialMaxStreamDataBidirectionalRemote(4096)
                      .initialMaxStreamDataUnidirectional(4096)
                      .initialMaxStreamsBidirectional(1)
                      .initialMaxStreamsUnidirectional(4)
                      .build())
              .bind(new InetSocketAddress("127.0.0.1", 0))
              .sync()
              .channel();
      datagrams.add(socket);
      Probe probe = new Probe(authentication.guard());
      Future<QuicChannel> connection =
          QuicChannel.newBootstrap(socket)
              .handler(
                  new ChannelInitializer<QuicChannel>() {
                    @Override
                    protected void initChannel(QuicChannel channel) {
                      probe.channel = channel;
                      channel.pipeline().addLast(probe.guard, probe);
                    }
                  })
              .streamHandler(new ChannelInboundHandlerAdapter())
              .remoteAddress(listener.localAddress())
              .connect();
      return new Attempt(probe, connection);
    }

    Pair connect(TlsAuthentication authentication, QuicSslContext external) throws Exception {
      Attempt attempt = attempt(authentication, external);
      attempt.connection.get(5, TimeUnit.SECONDS);
      attempt.probe.guard.ready().toCompletableFuture().get(5, TimeUnit.SECONDS);
      Probe server = accepted.poll(5, TimeUnit.SECONDS);
      assertNotNull(server);
      server.guard.ready().toCompletableFuture().get(5, TimeUnit.SECONDS);
      return new Pair(attempt.probe, server);
    }

    CompletableFuture<Capabilities> send(Pair pair, Capabilities capabilities) throws Exception {
      var result = new CompletableFuture<Capabilities>();
      var channel =
          pair.client
              .channel
              .createStream(
                  QuicStreamType.BIDIRECTIONAL,
                  new SimpleChannelInboundHandler<ByteBuf>() {
                    final Wire.Decoder decoder = new Wire.Decoder(4096);

                    @Override
                    protected void channelRead0(ChannelHandlerContext ctx, ByteBuf bytes) {
                      var frame = decoder.feed(bytes.nioBuffer());
                      if (frame != null)
                        result.complete((Capabilities) ((Wire.Known) frame).message());
                    }
                  })
              .get(5, TimeUnit.SECONDS);
      assertEquals(0, channel.streamId());
      channel.writeAndFlush(Unpooled.wrappedBuffer(Wire.encode(capabilities, 4096))).sync();
      return result;
    }

    Capabilities exchange(Pair pair, Capabilities capabilities) throws Exception {
      return send(pair, capabilities).get(5, TimeUnit.SECONDS);
    }

    @Override
    public void close() {
      for (var channel : datagrams) channel.close().awaitUninterruptibly();
      group.shutdownGracefully(0, 2, TimeUnit.SECONDS).awaitUninterruptibly();
    }
  }
}
