package ai.pipestream.quic.v2;

import static org.junit.jupiter.api.Assertions.*;

import ai.pipestream.quic.AddressValidationTokenHandler;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.ChannelInputShutdownEvent;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.handler.codec.quic.QLogConfiguration;
import io.netty.handler.codec.quic.QuicChannelOption;
import io.netty.handler.codec.quic.QuicServerCodecBuilder;
import io.netty.handler.codec.quic.QuicStreamChannel;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.CleanupMode;
import org.junit.jupiter.api.io.TempDir;

@Timeout(30)
final class V2CoreClientTest {
  @TempDir(cleanup = CleanupMode.ON_SUCCESS)
  static Path directory;

  @BeforeAll
  static void certificates() throws Exception {
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
        "ca.key",
        "-out",
        "ca.crt",
        "-days",
        "2",
        "-subj",
        "/CN=Core-Client-Test-CA",
        "-addext",
        "basicConstraints=critical,CA:TRUE",
        "-addext",
        "keyUsage=critical,keyCertSign,cRLSign");
    for (String name : List.of("server", "client")) {
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
          "/CN=" + name);
      Files.writeString(
          path(name + ".ext"),
          "basicConstraints=critical,CA:FALSE\n"
              + "keyUsage=critical,digitalSignature\n"
              + "extendedKeyUsage="
              + (name.equals("server")
                  ? "serverAuth\nsubjectAltName=DNS:localhost\n"
                  : "clientAuth\n"));
      command(
          "openssl",
          "x509",
          "-req",
          "-in",
          name + ".csr",
          "-CA",
          "ca.crt",
          "-CAkey",
          "ca.key",
          "-CAcreateserial",
          "-out",
          name + ".crt",
          "-days",
          "2",
          "-extfile",
          name + ".ext");
    }
  }

  @Test
  void realCoreServerNegotiatesAndDetachWaitsForBothHalfCloses() throws Exception {
    CoreOptions options = options(1000, 2000);
    try (CoreServer server = realServer(options);
        CoreClient client = CoreClient.connect(server.address(), client("localhost"), options)) {
      assertEquals(selected(options), get(client.ready()));
      get(client.detach());
      get(client.closed());
    }
  }

  @Test
  void detachDuringHandshakeIsOneCancellationProofLogicalRequest() throws Exception {
    CoreOptions options = options(1000, 2000);
    try (MaliciousServer server = new MaliciousServer(options, false, (peer, stream) -> {});
        CoreClient client = CoreClient.connect(server.address(), client("localhost"), options)) {
      CompletionStage<Void> first = client.detach();
      CompletionStage<Void> second = client.detach();
      assertSame(first, second);
      for (int attempt = 0; attempt < 256; attempt++) assertSame(first, client.detach());
      assertTrue(first.toCompletableFuture().cancel(true));
      assertFalse(second.toCompletableFuture().isCancelled());
      QuicStreamChannel stream = get(server.offerReceived);
      server.send(stream, selected(options));
      assertEquals(new Messages.Detach(1), get(server.detachReceived));
      get(server.clientFin);
      server.send(stream, new Messages.Detached(1));
      stream.shutdownOutput();
      get(second);
      assertFalse(server.scriptFailure.isDone());
      get(client.detach());
      get(client.closed());
    }
  }

  @Test
  void wrongTlsRoleAndBadServiceIdentityFailWithoutForgingReadiness() throws Exception {
    CoreOptions options = options(500, 1000);
    TlsAuthentication wrongRole = serverAuthentication();
    assertThrows(
        IllegalArgumentException.class,
        () -> CoreClient.connect(new InetSocketAddress("127.0.0.1", 9), wrongRole, options));
    try (CoreServer server = realServer(options);
        CoreClient client =
            CoreClient.connect(server.address(), client("not-localhost"), options)) {
      assertNonTimeoutFailure(client.ready());
      assertNonTimeoutFailure(client.closed());
    }
  }

  @Test
  void invalidOrMissingCapabilitiesNeverBecomeReady() throws Exception {
    CoreOptions options = options(250, 1000);
    record Attack(ProtocolError.Code code, BiConsumer<MaliciousServer, QuicStreamChannel> action) {}
    List<Attack> attacks =
        List.of(
            new Attack(
                ProtocolError.Code.FRAME_ERROR,
                (server, stream) -> server.send(stream, options.offer())),
            new Attack(
                ProtocolError.Code.FRAME_ERROR,
                (server, stream) -> server.send(stream, increased(options))),
            new Attack(ProtocolError.Code.LIMIT_EXCEEDED, (server, stream) -> {}),
            new Attack(
                ProtocolError.Code.LIMIT_EXCEEDED,
                (server, stream) ->
                    stream.writeAndFlush(Unpooled.wrappedBuffer(new byte[] {1, 0}))));
    for (Attack attack : attacks) {
      try (MaliciousServer server = new MaliciousServer(options, false, attack.action());
          CoreClient client = CoreClient.connect(server.address(), client("localhost"), options)) {
        assertEquals(attack.code(), assertProtocolFailure(client.ready()).code());
        assertEquals(attack.code(), assertProtocolFailure(client.closed()).code());
      }
    }
  }

  @Test
  void detachRequiresCorrelatedAcknowledgmentBeforePeerFin() throws Exception {
    CoreOptions options = options(300, 1000);
    record Attack(
        String name,
        ProtocolError.Code code,
        BiConsumer<MaliciousServer, QuicStreamChannel> action) {}
    List<Attack> attacks =
        List.of(
            new Attack(
                "peer FIN before Detached",
                ProtocolError.Code.FRAME_ERROR,
                (server, stream) -> server.retainFin(stream, stream.shutdownOutput())),
            new Attack(
                "wrong Detached correlation",
                ProtocolError.Code.FRAME_ERROR,
                (server, stream) -> server.send(stream, new Messages.Detached(2))),
            new Attack(
                "duplicate Detached",
                ProtocolError.Code.FRAME_ERROR,
                (server, stream) -> {
                  server.send(stream, new Messages.Detached(1));
                  server.send(stream, new Messages.Detached(1));
                  stream.shutdownOutput();
                }),
            new Attack(
                "Detached without peer FIN",
                ProtocolError.Code.LIMIT_EXCEEDED,
                (server, stream) -> server.send(stream, new Messages.Detached(1))));
    for (Attack attack : attacks) {
      try (MaliciousServer server = new MaliciousServer(options, true, attack.action());
          CoreClient client = CoreClient.connect(server.address(), client("localhost"), options)) {
        get(client.ready());
        ProtocolError detach = assertProtocolFailure(client.detach());
        String diagnostic =
            attack.name()
                + ": "
                + detach.getMessage()
                + ", detachReceived="
                + server.detachReceived.isDone()
                + ", clientFin="
                + server.clientFin.isDone()
                + ", scriptFailure="
                + server.scriptFailure.isDone()
                + server.finDiagnostic();
        assertEquals(attack.code(), detach.code(), diagnostic);
        ProtocolError closed = assertProtocolFailure(client.closed());
        assertEquals(attack.code(), closed.code(), diagnostic + ", closed=" + closed.getMessage());
      }
    }
  }

  @Test
  void unsolicitedDetachedResponseIsRejectedEvenAfterAValidSelection() throws Exception {
    CoreOptions options = options(500, 1000);
    try (MaliciousServer server =
            new MaliciousServer(
                options,
                false,
                (peer, stream) -> {
                  peer.send(stream, selected(options));
                  peer.send(stream, new Messages.Detached(1));
                });
        CoreClient client = CoreClient.connect(server.address(), client("localhost"), options)) {
      assertEquals(ProtocolError.Code.FRAME_ERROR, assertProtocolFailure(client.closed()).code());
      assertEquals(ProtocolError.Code.FRAME_ERROR, assertProtocolFailure(client.detach()).code());
    }
  }

  @Test
  void resetAndStopOfControlRemainNamedProtocolFailures() throws Exception {
    CoreOptions options = options(500, 1000);
    for (boolean stop : List.of(false, true)) {
      BiConsumer<MaliciousServer, QuicStreamChannel> attack =
          (server, stream) -> {
            if (stop) stream.shutdownInput(42);
            else stream.shutdownOutput(42);
            server.send(stream, selected(options));
          };
      try (MaliciousServer server = new MaliciousServer(options, false, attack);
          CoreClient client = CoreClient.connect(server.address(), client("localhost"), options)) {
        ProtocolError failure = assertProtocolFailure(client.detach());
        assertEquals(ProtocolError.Code.CONTROL_RESET, failure.code());
      }
    }
  }

  @Test
  void correlatedDetachRefusalRetainsItsNamedProtocolError() throws Exception {
    CoreOptions options = options(500, 1000);
    try (MaliciousServer server =
            new MaliciousServer(
                options,
                true,
                (peer, stream) ->
                    peer.send(
                        stream,
                        new Messages.Refusal(
                            new Records.RequestTag(false, 1),
                            ProtocolError.Code.NOT_READY,
                            "deliberate refusal")));
        CoreClient client = CoreClient.connect(server.address(), client("localhost"), options)) {
      get(client.ready());
      assertEquals(ProtocolError.Code.NOT_READY, assertProtocolFailure(client.detach()).code());
      assertEquals(ProtocolError.Code.NOT_READY, assertProtocolFailure(client.closed()).code());
    }
  }

  @Test
  void largeIgnoredFrameCrossesTinyWindowWithoutBlockingDetach() throws Exception {
    CoreOptions options = new CoreOptions(16384, 8, 1000, 2000, 1, 1, 32768, 128, 128, 1000, 1000);
    try (MaliciousServer server =
            new MaliciousServer(
                options,
                true,
                (peer, stream) -> {
                  byte[] ignored = ignored(12000);
                  byte[] detached = Wire.encode(new Messages.Detached(1), options.controlLimit());
                  ByteBuf response =
                      Unpooled.buffer(ignored.length + detached.length)
                          .writeBytes(ignored)
                          .writeBytes(detached);
                  stream.writeAndFlush(response).addListener(done -> stream.shutdownOutput());
                });
        CoreClient client = CoreClient.connect(server.address(), client("localhost"), options)) {
      get(client.ready());
      get(client.detach());
      get(client.closed());
    }
  }

  @Test
  void ignoredTrafficCannotRenewAbsoluteDetachLifetime() throws Exception {
    CoreOptions options = options(5000, 1000);
    try (MaliciousServer server =
            new MaliciousServer(
                options,
                true,
                (peer, stream) -> {
                  peer.send(stream, new Messages.Detached(1));
                  stream
                      .eventLoop()
                      .scheduleAtFixedRate(
                          () -> {
                            peer.ignoredWrites.incrementAndGet();
                            stream.writeAndFlush(Unpooled.wrappedBuffer(ignored(1)));
                          },
                          0,
                          50,
                          TimeUnit.MILLISECONDS);
                });
        CoreClient client = CoreClient.connect(server.address(), client("localhost"), options)) {
      get(client.ready());
      long started = System.nanoTime();
      assertEquals(
          ProtocolError.Code.LIMIT_EXCEEDED, assertProtocolFailure(client.detach()).code());
      assertTrue(System.nanoTime() - started < TimeUnit.SECONDS.toNanos(3));
      assertTrue(server.ignoredWrites.get() >= 3);
    }
  }

  @Test
  void processClientCountAdmissionReleasesOnlyAfterOwnedTermination() throws Exception {
    CoreOptions options = new CoreOptions(4096, 1, 1000, 1000, 1, 1, 4101, 128, 128, 30000, 30000);
    List<CoreClient> clients = new ArrayList<>();
    try (DatagramSocket silent = new DatagramSocket(new InetSocketAddress("127.0.0.1", 0))) {
      InetSocketAddress remote = new InetSocketAddress("127.0.0.1", silent.getLocalPort());
      try {
        for (int count = 0; count < 64; count++)
          clients.add(CoreClient.connect(remote, client("localhost"), options));
        assertEquals(
            ProtocolError.Code.LIMIT_EXCEEDED,
            assertThrows(
                    ProtocolError.class,
                    () -> CoreClient.connect(remote, client("localhost"), options))
                .code());
        CoreClient released = clients.remove(0);
        released.close();
        assertEquals(ProtocolError.Code.CANCELLED, assertProtocolFailure(released.closed()).code());
        clients.add(CoreClient.connect(remote, client("localhost"), options));
      } finally {
        for (CoreClient client : clients) client.close();
      }
    }
  }

  @Test
  void configuredBufferAdmissionCanBindBeforeTheCountCeiling() throws Exception {
    CoreOptions options =
        new CoreOptions(1048576, 1, 1000, 1000, 1, 1, 16777216, 128, 128, 30000, 30000);
    int accepted =
        (int)
            ((128L * 1024 * 1024)
                / (options.queuedControlBytes()
                    + options.controlLimit()
                    + options.readChunkBytes()));
    List<CoreClient> clients = new ArrayList<>();
    try (DatagramSocket silent = new DatagramSocket(new InetSocketAddress("127.0.0.1", 0))) {
      InetSocketAddress remote = new InetSocketAddress("127.0.0.1", silent.getLocalPort());
      try {
        for (int count = 0; count < accepted; count++)
          clients.add(CoreClient.connect(remote, client("localhost"), options));
        assertTrue(accepted < 64);
        assertEquals(
            ProtocolError.Code.LIMIT_EXCEEDED,
            assertThrows(
                    ProtocolError.class,
                    () -> CoreClient.connect(remote, client("localhost"), options))
                .code());
      } finally {
        for (CoreClient client : clients) client.close();
      }
    }
  }

  @Test
  @Tag("sealed-interop")
  @Timeout(60)
  void javaCoreClientNegotiatesAndDetachesFromRustAuthorityWithBothIdentityModes()
      throws Exception {
    Path executable =
        Path.of("../rust-quinn/target/release/pipestream-quinn").toAbsolutePath().normalize();
    assertTrue(Files.isExecutable(executable), "sealed interop requires the release Rust CLI");
    Path state = path("rust-authority.sqlite");
    Path objects = path("rust-objects");
    Path principals = path("rust-principals.tsv");
    String fingerprint =
        java.util.HexFormat.of()
            .formatHex(MessageDigest.getInstance("SHA-256").digest(cert("client").getEncoded()));
    Files.writeString(principals, "sha256\tprincipal\n" + fingerprint + "\tmapped-client\n");
    String[] storage = {
      "--state-db", state.toString(),
      "--object-dir", objects.toString(),
      "--authority", "issuer-a",
      "--principal-map", principals.toString(),
      "--trust-system-clock"
    };
    List<String> initialize =
        new ArrayList<>(List.of(executable.toString(), "v2", "init-authority"));
    initialize.addAll(List.of(storage));
    command(initialize.toArray(String[]::new));

    Path ready = path("rust-ready");
    Path log = path("rust-server.log");
    List<String> serve = new ArrayList<>(List.of(executable.toString(), "v2", "serve"));
    serve.addAll(List.of(storage));
    serve.addAll(
        List.of(
            "--cert", path("server.crt").toString(),
            "--key", path("server.key").toString(),
            "--client-ca", path("ca.crt").toString(),
            "--result-authority", "localhost:7443",
            "--ready-file", ready.toString()));
    Process process =
        new ProcessBuilder(serve)
            .directory(directory.toFile())
            .redirectErrorStream(true)
            .redirectOutput(log.toFile())
            .start();
    try {
      InetSocketAddress address = awaitReady(process, ready, log);
      CoreOptions options = options(2000, 3000);
      for (boolean mapped : List.of(false, true)) {
        TlsAuthentication authentication =
            TlsAuthentication.client(
                path("ca.crt"),
                "localhost",
                mapped ? path("client.crt") : null,
                mapped ? path("client.key") : null);
        try (CoreClient client = CoreClient.connect(address, authentication, options)) {
          Messages.Capabilities capabilities = get(client.ready());
          assertTrue(capabilities.supported().isEmpty());
          assertTrue(capabilities.required().isEmpty());
          assertEquals(0, capabilities.objectLimit());
          get(client.detach());
          get(client.closed());
        }
      }
      process.destroy();
      assertTrue(process.waitFor(10, TimeUnit.SECONDS), "Rust server did not drain after SIGTERM");
      assertEquals(0, process.exitValue(), () -> boundedLog(log));
      assertTrue(Files.readString(log).contains("DRAINED"), () -> boundedLog(log));
    } finally {
      if (process.isAlive()) {
        process.destroy();
        if (!process.waitFor(5, TimeUnit.SECONDS)) {
          process.destroyForcibly();
          process.waitFor(5, TimeUnit.SECONDS);
        }
      }
    }
  }

  @Test
  void closeBeforeSuccessfulDetachAbortsEveryOutstandingOutcome() throws Exception {
    CoreOptions options = options(1000, 2000);
    try (MaliciousServer server = new MaliciousServer(options, true, (ignored, stream) -> {})) {
      CoreClient client = CoreClient.connect(server.address(), client("localhost"), options);
      get(client.ready());
      CompletionStage<Void> detach = client.detach();
      client.close();
      assertEquals(ProtocolError.Code.CANCELLED, assertProtocolFailure(detach).code());
      assertEquals(ProtocolError.Code.CANCELLED, assertProtocolFailure(client.closed()).code());
    }
  }

  private static CoreOptions options(long controlMs, long lifetimeMs) {
    return new CoreOptions(4096, 8, 1000, lifetimeMs, 4, 4, 8192, 32768, 4096, 1000, controlMs);
  }

  private static byte[] ignored(int bodyLength) {
    return ByteBuffer.allocate(bodyLength + 5).put((byte) 128).putInt(bodyLength).array();
  }

  private static InetSocketAddress awaitReady(Process process, Path ready, Path log)
      throws Exception {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
    while (System.nanoTime() < deadline) {
      if (!process.isAlive()) fail("Rust server exited before readiness: " + boundedLog(log));
      if (Files.isRegularFile(ready)) {
        String[] address = Files.readString(ready).trim().split(":");
        if (address.length == 2)
          return new InetSocketAddress(address[0], Integer.parseInt(address[1]));
      }
      Thread.sleep(10);
    }
    throw new AssertionError("Rust server readiness timeout: " + boundedLog(log));
  }

  private static String boundedLog(Path log) {
    try {
      String text = Files.exists(log) ? Files.readString(log) : "";
      return text.substring(Math.max(0, text.length() - 8192));
    } catch (Exception failure) {
      return failure.toString();
    }
  }

  private static Messages.Capabilities selected(CoreOptions options) {
    Messages.Capabilities offer = options.offer();
    return new Messages.Capabilities(
        true,
        offer.supported(),
        offer.required(),
        offer.controlLimit(),
        offer.streamLimit(),
        offer.pendingLimit(),
        offer.objectLimit(),
        offer.streamIdleMs(),
        offer.streamLifetimeMs());
  }

  private static Messages.Capabilities increased(CoreOptions options) {
    Messages.Capabilities offer = options.offer();
    return new Messages.Capabilities(
        true,
        List.of(),
        List.of(),
        offer.controlLimit() + 1,
        offer.streamLimit(),
        offer.pendingLimit(),
        offer.objectLimit(),
        offer.streamIdleMs(),
        offer.streamLifetimeMs());
  }

  private static <T> T get(CompletionStage<T> stage) throws Exception {
    return stage.toCompletableFuture().get(5, TimeUnit.SECONDS);
  }

  private static ProtocolError assertProtocolFailure(CompletionStage<?> stage) {
    return assertInstanceOf(ProtocolError.class, failure(stage));
  }

  private static Throwable failure(CompletionStage<?> stage) {
    Throwable failure = assertThrows(Throwable.class, () -> get(stage));
    while ((failure instanceof java.util.concurrent.ExecutionException
            || failure instanceof CompletionException)
        && failure.getCause() != null) failure = failure.getCause();
    assertFalse(failure instanceof java.util.concurrent.TimeoutException);
    return failure;
  }

  private static void assertNonTimeoutFailure(CompletionStage<?> stage) {
    assertNotNull(failure(stage));
  }

  private static Path path(String name) {
    return directory.resolve(name);
  }

  private static X509Certificate cert(String name) throws Exception {
    try (var input = Files.newInputStream(path(name + ".crt"))) {
      return (X509Certificate) CertificateFactory.getInstance("X.509").generateCertificate(input);
    }
  }

  private static TlsAuthentication client(String reference) throws Exception {
    return TlsAuthentication.client(path("ca.crt"), reference, null, null);
  }

  private static TlsAuthentication serverAuthentication() throws Exception {
    return TlsAuthentication.server(
        path("ca.crt"),
        path("server.crt"),
        path("server.key"),
        Map.of(TlsAuthentication.fingerprint(cert("client")), "client"),
        Clock.systemUTC());
  }

  private static CoreServer realServer(CoreOptions options) throws Exception {
    return CoreServer.start(new InetSocketAddress("127.0.0.1", 0), serverAuthentication(), options);
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
      assertEquals(0, process.exitValue(), log::toString);
    } finally {
      if (process.isAlive()) process.destroyForcibly().waitFor(5, TimeUnit.SECONDS);
    }
  }

  /** Independent network peer: it decodes client requests but never invokes client internals. */
  private static final class MaliciousServer implements AutoCloseable {
    final MultiThreadIoEventLoopGroup group =
        new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
    final CoreOptions options;
    final boolean negotiate;
    final BiConsumer<MaliciousServer, QuicStreamChannel> attack;
    final CompletableFuture<QuicStreamChannel> offerReceived = new CompletableFuture<>();
    final CompletableFuture<Messages.Detach> detachReceived = new CompletableFuture<>();
    final CompletableFuture<Void> clientFin = new CompletableFuture<>();
    final CompletableFuture<Throwable> scriptFailure = new CompletableFuture<>();
    final AtomicInteger ignoredWrites = new AtomicInteger();
    final AtomicLong offerAt = new AtomicLong();
    final AtomicLong detachAt = new AtomicLong();
    final AtomicLong attackAt = new AtomicLong();
    final AtomicReference<QuicStreamChannel> attackStream = new AtomicReference<>();
    final AtomicReference<ChannelFuture> attackFin = new AtomicReference<>();
    final Path qlogDirectory;
    final Channel listener;

    MaliciousServer(
        CoreOptions options,
        boolean negotiate,
        BiConsumer<MaliciousServer, QuicStreamChannel> attack)
        throws Exception {
      this.options = options;
      this.negotiate = negotiate;
      this.attack = attack;
      qlogDirectory = Files.createTempDirectory(directory, "core-client-qlog-");
      TlsAuthentication authentication = serverAuthentication();
      var codec =
          new QuicServerCodecBuilder()
              .version(1)
              .sslEngineProvider(c -> authentication.engine(c.alloc(), 0))
              .maxIdleTimeout(3, TimeUnit.SECONDS)
              .initialMaxData(32768)
              .initialMaxStreamDataBidirectionalRemote(32768)
              .initialMaxStreamDataBidirectionalLocal(0)
              .initialMaxStreamDataUnidirectional(0)
              .initialMaxStreamsBidirectional(1)
              .initialMaxStreamsUnidirectional(0)
              .option(
                  QuicChannelOption.QLOG,
                  new QLogConfiguration(
                      qlogDirectory.toString(), "Core client malicious peer", "FIN diagnostic"))
              .tokenHandler(new AddressValidationTokenHandler())
              .handler(new ChannelInboundHandlerAdapter())
              .streamHandler(
                  new ChannelInitializer<QuicStreamChannel>() {
                    @Override
                    protected void initChannel(QuicStreamChannel stream) {
                      stream.config().setAllowHalfClosure(true);
                      stream.pipeline().addLast(new Script(stream));
                    }
                  })
              .build();
      listener =
          new Bootstrap()
              .group(group)
              .channel(NioDatagramChannel.class)
              .handler(codec)
              .bind(new InetSocketAddress("127.0.0.1", 0))
              .sync()
              .channel();
    }

    InetSocketAddress address() {
      return (InetSocketAddress) listener.localAddress();
    }

    void send(QuicStreamChannel stream, Messages.Message message) {
      stream.writeAndFlush(Unpooled.wrappedBuffer(Wire.encode(message, 8192)));
    }

    void retainFin(QuicStreamChannel stream, ChannelFuture fin) {
      attackStream.set(stream);
      attackFin.set(fin);
    }

    String finDiagnostic() {
      ChannelFuture fin = attackFin.get();
      QuicStreamChannel stream = attackStream.get();
      if (fin == null || stream == null) return "";
      long assertionAt = System.nanoTime();
      return ", serverFinDone="
          + fin.isDone()
          + ", serverFinSuccess="
          + fin.isSuccess()
          + ", serverFinCause="
          + fin.cause()
          + ", serverStreamActive="
          + stream.isActive()
          + ", serverStreamInputShutdown="
          + stream.isInputShutdown()
          + ", serverStreamOutputShutdown="
          + stream.isOutputShutdown()
          + ", offerToDetachMs="
          + elapsed(offerAt.get(), detachAt.get())
          + ", offerToAttackMs="
          + elapsed(offerAt.get(), attackAt.get())
          + ", offerToAssertionMs="
          + elapsed(offerAt.get(), assertionAt)
          + ", qlog="
          + qlogDirectory;
    }

    private static long elapsed(long start, long end) {
      return start == 0 || end == 0 ? -1 : TimeUnit.NANOSECONDS.toMillis(end - start);
    }

    @Override
    public void close() {
      listener.close().awaitUninterruptibly();
      group.shutdownGracefully(0, 2, TimeUnit.SECONDS).awaitUninterruptibly();
      assertFalse(
          scriptFailure.isDone(),
          () -> "malicious peer script failed: " + scriptFailure.getNow(null));
    }

    private final class Script extends SimpleChannelInboundHandler<ByteBuf> {
      final QuicStreamChannel stream;
      final Wire.Decoder decoder = new Wire.Decoder(4096);
      boolean negotiated;
      boolean attacked;

      Script(QuicStreamChannel stream) {
        this.stream = stream;
      }

      @Override
      protected void channelRead0(ChannelHandlerContext context, ByteBuf bytes) {
        try {
          ByteBuffer source = bytes.nioBuffer();
          while (source.hasRemaining()) {
            Wire.Frame frame = decoder.feed(source);
            if (frame == null) return;
            Messages.Message message = ((Wire.Known) frame).message();
            if (!negotiated) {
              assertInstanceOf(Messages.Capabilities.class, message);
              negotiated = true;
              offerAt.compareAndSet(0, System.nanoTime());
              offerReceived.complete(stream);
              if (negotiate) {
                decoder.limit(options.controlLimit());
                send(stream, selected(options));
              } else if (attack != null) {
                attacked = true;
                attack(stream);
              }
            } else if (message instanceof Messages.Detach detach) {
              detachAt.compareAndSet(0, System.nanoTime());
              if (!detachReceived.complete(detach))
                throw new AssertionError("client sent more than one detach");
              if (!attacked) {
                attacked = true;
                attack(stream);
              }
            }
          }
        } catch (Throwable failure) {
          scriptFailure.complete(failure);
          context.fireExceptionCaught(failure);
        }
      }

      @Override
      public void userEventTriggered(ChannelHandlerContext context, Object event) {
        if (event instanceof ChannelInputShutdownEvent && !attacked) {
          clientFin.complete(null);
          attacked = true;
          attack(stream);
        } else {
          if (event instanceof ChannelInputShutdownEvent) clientFin.complete(null);
          context.fireUserEventTriggered(event);
        }
      }

      private void attack(QuicStreamChannel stream) {
        attackAt.compareAndSet(0, System.nanoTime());
        attack.accept(MaliciousServer.this, stream);
      }
    }
  }
}
