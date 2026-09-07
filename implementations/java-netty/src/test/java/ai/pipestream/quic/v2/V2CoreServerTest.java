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
import io.netty.channel.socket.ChannelInputShutdownEvent;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.handler.codec.quic.QuicChannel;
import io.netty.handler.codec.quic.QuicClientCodecBuilder;
import io.netty.handler.codec.quic.QuicConnectionCloseEvent;
import io.netty.handler.codec.quic.QuicStreamChannel;
import io.netty.handler.codec.quic.QuicStreamType;
import io.netty.util.concurrent.Future;
import java.io.ByteArrayOutputStream;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

@Timeout(30)
final class V2CoreServerTest {
  @TempDir static Path directory;

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
        "/CN=Core-Test-CA",
        "-addext",
        "basicConstraints=critical,CA:TRUE",
        "-addext",
        "keyUsage=critical,keyCertSign,cRLSign");
    for (String name : List.of("server", "alice", "bob")) {
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
  void actualCoreNegotiationRefusalAndDetachPreserveEveryHalfClosedResponse() throws Exception {
    CoreOptions options = options(16, 65536, 5000, 10000, 8, 4);
    try (CoreServer server = start(options);
        Peer peer = new Peer(server.address(), "alice", 128, false, null)) {
      peer.open();
      var frames = new ArrayList<Message>();
      frames.add(options.offer());
      frames.add(new NextSequence(1));
      frames.add(new Detach(2));
      for (long request = 3; request <= 8; request++) frames.add(new Detach(request));
      peer.send(frames.toArray(Message[]::new));
      peer.control.shutdownOutput().sync();
      await(() -> snapshot(server).peakControlFrames() >= 2);
      assertTrue(peer.messages.isEmpty(), "application has not consumed the blocked replies");
      assertTrue(peer.connection.isActive());
      peer.control.eventLoop().submit(() -> peer.control.config().setAutoRead(true)).sync();
      Capabilities selected = assertInstanceOf(Capabilities.class, peer.next());
      options.offer().validateResponse(selected);
      assertTrue(selected.supported().isEmpty());
      assertEquals(0, selected.objectLimit());
      refusal(peer.next(), 1, ProtocolError.Code.EXTENSION_UNSUPPORTED);
      assertEquals(new Detached(2), peer.next());
      for (long request = 3; request <= 8; request++)
        refusal(peer.next(), request, ProtocolError.Code.NOT_READY);
      peer.fin.get(5, TimeUnit.SECONDS);
      assertTrue(peer.connection.isActive(), "server must not close on local write/FIN acceptance");
      assertFalse(peer.closed.isDone());
      assertTrue(snapshot(server).peakControlFrames() <= options.pendingLimit());
      assertTrue(snapshot(server).peakControlBytes() <= options.queuedControlBytes());
      peer.connection.close(true, 0, Unpooled.EMPTY_BUFFER).sync();
      await(() -> snapshot(server).active() == 0);
    }
  }

  @Test
  void allProfileDependentCoreRequestsReceiveCorrelatedRefusalsWithoutClosing() throws Exception {
    CoreOptions options = options(32, 65536, 5000, 10000, 8, 4);
    var operation =
        new Records.OperationId(new byte[] {1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0});
    var work = new Records.WorkKey(0, 0, 1);
    var digest = new Records.Digest(new byte[32]);
    try (CoreServer server = start(options);
        Peer peer = new Peer(server.address(), null, 65536, true, null)) {
      peer.negotiate(options.offer());
      List<Message> requests =
          List.of(
              new Create(1, 1, new Records.Policy(1000, 1000, 1000)),
              new Attach(2, "authority", "alice", 1),
              new NextSequence(3),
              new Declare(4, operation, 0, List.of(1L), true),
              new Page(5, 0, 0, 1),
              new Checkpoint(6, 0, digest, 0),
              new CancelScope(7, operation, 0),
              new LookupOperation(8, operation),
              new Watch(9, work, 0, 0),
              new Retry(10, operation, work, 1),
              new Cancel(11, operation, work),
              new Skip(12, operation, work),
              new Read(13, work, 1, 0, digest),
              new GetManifest(14, work, 1),
              new Complete(
                  15,
                  1,
                  new Records.ScopeSummary(
                      0, 0, null, digest, 0, new Records.Counts(0, 0, 0, 0), digest, 0)));
      for (Message request : requests) {
        peer.send(request);
        refusal(
            peer.next(),
            ClientCorrelation.requestId(request),
            ProtocolError.Code.EXTENSION_UNSUPPORTED);
        assertTrue(peer.connection.isActive());
      }
      peer.send(new Detach(Long.MAX_VALUE));
      assertEquals(new Detached(Long.MAX_VALUE), peer.next());
      peer.send(new Detach(Long.MAX_VALUE));
      peer.error(ProtocolError.Code.FRAME_ERROR);
    }
  }

  @Test
  void malformedNegotiationDirectionIdsAndControlFinUseNamedConnectionErrors() throws Exception {
    CoreOptions options = options(16, 65536, 5000, 10000, 16, 16);
    try (CoreServer server = start(options)) {
      List<byte[]> first =
          List.of(
              new byte[] {0, 0, 0, 0, 0},
              new byte[] {(byte) 128, 0, 0, 0, 0},
              Wire.encode(new NextSequence(1), 4096),
              Wire.encode(
                  new Capabilities(true, List.of(), List.of(), 4096, 1, 1, 0, 1000, 1000), 4096));
      for (byte[] frame : first) {
        try (Peer peer = new Peer(server.address(), null, 65536, true, null)) {
          peer.open();
          peer.sendBytes(frame);
          peer.error(ProtocolError.Code.FRAME_ERROR);
        }
      }
      try (Peer peer = new Peer(server.address(), null, 65536, true, null)) {
        peer.open();
        peer.sendBytes(new byte[] {1, 0, 0, 16, 1});
        peer.error(ProtocolError.Code.LIMIT_EXCEEDED);
      }
      for (Message message : List.of(options.offer(), new Sequence(1, 1), new NextSequence(2))) {
        try (Peer peer = new Peer(server.address(), null, 65536, true, null)) {
          peer.negotiate(options.offer());
          peer.send(message);
          peer.error(ProtocolError.Code.FRAME_ERROR);
        }
      }
      try (Peer peer = new Peer(server.address(), null, 65536, true, null)) {
        peer.negotiate(options.offer());
        peer.send(new NextSequence(1));
        peer.next();
        peer.send(new NextSequence(1));
        peer.error(ProtocolError.Code.FRAME_ERROR);
      }
      try (Peer peer = new Peer(server.address(), null, 65536, true, null)) {
        peer.negotiate(options.offer());
        peer.send(new NextSequence(1));
        refusal(peer.next(), 1, ProtocolError.Code.EXTENSION_UNSUPPORTED);
        peer.send(new NextSequence(4));
        refusal(peer.next(), 4, ProtocolError.Code.EXTENSION_UNSUPPORTED);
        peer.send(new NextSequence(3));
        peer.error(ProtocolError.Code.FRAME_ERROR);
      }
      try (Peer peer = new Peer(server.address(), null, 65536, true, null)) {
        peer.negotiate(options.offer());
        peer.control.shutdownOutput().sync();
        peer.error(ProtocolError.Code.FRAME_ERROR);
      }
      try (Peer peer = new Peer(server.address(), null, 65536, true, null)) {
        peer.open();
        peer.sendBytes(new byte[] {1, 0});
        peer.control.shutdownOutput().sync();
        peer.error(ProtocolError.Code.FRAME_ERROR);
      }
      try (Peer peer = new Peer(server.address(), null, 65536, true, null)) {
        peer.negotiate(options.offer());
        peer.sendBytes(new byte[] {(byte) 192, 0, 0, 0, 0});
        peer.error(ProtocolError.Code.EXTENSION_UNSUPPORTED);
      }
    }
  }

  @Test
  void requiredUnsupportedAndRequiredUnauthenticatedProfilesNeverGetCapabilityResponses()
      throws Exception {
    CoreOptions options = options(16, 65536, 5000, 10000, 8, 4);
    try (CoreServer server = start(options)) {
      for (String caller : new String[] {null, "alice"}) {
        try (Peer peer = new Peer(server.address(), caller, 65536, true, null)) {
          peer.open();
          peer.send(
              new Capabilities(
                  false, List.of(DURABLE_WORK), List.of(DURABLE_WORK), 4096, 1, 1, 0, 1000, 1000));
          peer.error(
              caller == null
                  ? ProtocolError.Code.UNAUTHORIZED
                  : ProtocolError.Code.EXTENSION_UNSUPPORTED);
          assertTrue(peer.messages.isEmpty());
        }
      }
      for (int required : List.of(123, 65281)) {
        try (Peer peer = new Peer(server.address(), null, 65536, true, null)) {
          peer.open();
          peer.send(
              new Capabilities(
                  false, List.of(required), List.of(required), 4096, 1, 1, 0, 1000, 1000));
          peer.error(ProtocolError.Code.EXTENSION_UNSUPPORTED);
          assertTrue(peer.messages.isEmpty());
        }
      }
      try (Peer peer = new Peer(server.address(), "alice", 65536, true, null)) {
        peer.negotiate(
            new Capabilities(
                false,
                List.of(123, DURABLE_WORK, RESULT_DELIVERY),
                List.of(),
                4096,
                1,
                1,
                0,
                1000,
                1000));
        peer.send(new Detach(1));
        assertEquals(new Detached(1), peer.next());
      }
    }
  }

  @Test
  void ignoredFramesCrossTinyReceiveWindowsWithoutConsumingRequestIdentity() throws Exception {
    CoreOptions options =
        new CoreOptions(16384, 16, 1000, 10000, 8, 4, 65536, 128, 128, 5000, 5000);
    try (CoreServer server = start(options);
        Peer peer = new Peer(server.address(), null, 128, true, null)) {
      peer.negotiate(options.offer());
      byte[] ignored = new byte[12005];
      ByteBuffer.wrap(ignored).put((byte) 128).putInt(12000);
      peer.sendBytes(ignored);
      peer.send(new Detach(1));
      assertEquals(new Detached(1), peer.next());
      peer.control.shutdownOutput().sync();
      peer.fin.get(5, TimeUnit.SECONDS);
      assertTrue(peer.connection.isActive());
    }
  }

  @Test
  void resetAndStopOfControlAreFatalWithoutBecomingWorkOutcomes() throws Exception {
    CoreOptions options = options(16, 65536, 5000, 10000, 8, 4);
    try (CoreServer server = start(options)) {
      try (Peer peer = new Peer(server.address(), null, 65536, true, null)) {
        peer.negotiate(options.offer());
        peer.control.shutdownOutput(42).sync();
        peer.error(ProtocolError.Code.CONTROL_RESET);
      }
      try (Peer peer = new Peer(server.address(), null, 65536, true, null)) {
        peer.negotiate(options.offer());
        peer.control.shutdownInput(42).sync();
        peer.send(new Detach(1));
        peer.error(ProtocolError.Code.CONTROL_RESET);
      }
    }
  }

  @Test
  void independentTimersExpireMissingStreamsPartialHeadersAndTrickledFrames() throws Exception {
    CoreOptions options = options(16, 65536, 300, 1000, 8, 4);
    try (CoreServer server = start(options)) {
      try (Peer peer = new Peer(server.address(), null, 65536, true, null)) {
        peer.handshake();
        peer.error(ProtocolError.Code.LIMIT_EXCEEDED);
      }
      try (Peer peer = new Peer(server.address(), null, 65536, true, null)) {
        peer.open();
        peer.sendBytes(new byte[] {1, 0});
        peer.error(ProtocolError.Code.LIMIT_EXCEEDED);
      }
      try (Peer peer = new Peer(server.address(), null, 65536, true, null)) {
        peer.open();
        byte[] offer = Wire.encode(options.offer(), 4096);
        var sent = new AtomicInteger();
        var ticker =
            peer.control
                .eventLoop()
                .scheduleAtFixedRate(
                    () -> {
                      int index = sent.getAndIncrement();
                      if (index < offer.length)
                        peer.control.writeAndFlush(
                            Unpooled.wrappedBuffer(new byte[] {offer[index]}));
                    },
                    0,
                    60,
                    TimeUnit.MILLISECONDS);
        try {
          peer.error(ProtocolError.Code.LIMIT_EXCEEDED);
          assertTrue(sent.get() >= 2);
        } finally {
          ticker.cancel(false);
        }
      }
      await(() -> snapshot(server).active() == 0);
    }
  }

  @Test
  void stalledHandshakeReleasesItsGlobalSlotWithoutApplicationActivation() throws Exception {
    CoreOptions options = new CoreOptions(4096, 4, 1000, 1000, 1, 1, 8192, 8192, 4096, 300, 1000);
    var tasks = new ArrayBlockingQueue<Runnable>(8);
    try (CoreServer server = start(options);
        Peer peer = new Peer(server.address(), null, 65536, true, tasks::add)) {
      assertNotNull(
          tasks.poll(5, TimeUnit.SECONDS), "actual client TLS verification must be paused");
      assertNotNull(peer.connection);
      assertEquals(0, snapshot(server).owners());
      await(() -> snapshot(server).refused() == 0 && snapshot(server).active() == 0);
      assertTrue(snapshot(server).peakConnections() == 1);
      try (Peer replacement = new Peer(server.address(), null, 65536, true, null)) {
        replacement.negotiate(options.offer());
        replacement.send(new Detach(1));
        assertEquals(new Detached(1), replacement.next());
      }
    }
  }

  @Test
  void globalAndOwnerConnectionCeilingsPreserveExistingConnectionsAndReleaseExactlyOnce()
      throws Exception {
    CoreOptions options = options(16, 65536, 5000, 10000, 2, 1);
    try (CoreServer server = start(options);
        Peer alice = new Peer(server.address(), "alice", 65536, true, null)) {
      alice.negotiate(options.offer());
      try (Peer duplicate = new Peer(server.address(), "alice", 65536, true, null)) {
        duplicate.handshake();
        duplicate.error(ProtocolError.Code.LIMIT_EXCEEDED);
      }
      try (Peer bob = new Peer(server.address(), "bob", 65536, true, null)) {
        bob.negotiate(options.offer());
        assertEquals(2, snapshot(server).active());
        for (int attempt = 0; attempt < 16; attempt++) {
          long before = snapshot(server).refused();
          try (Peer extra = new Peer(server.address(), null, 65536, true, null)) {
            try {
              extra.handshake();
            } catch (ExecutionException rejected) {
              assertNotNull(rejected.getCause());
            }
            // Native TLS can complete before the later close is observed. Neither a successful
            // handshake nor a local exception proves admission/refusal: require the actual close.
            QuicConnectionCloseEvent close = extra.closed.get(5, TimeUnit.SECONDS);
            assertFalse(close.isApplicationClose());
            assertEquals(0x02, close.error());
            assertTrue(extra.messages.isEmpty());
            assertEquals(2, snapshot(server).active());
            assertEquals(3, snapshot(server).peakTransports());
            assertTrue(snapshot(server).refused() > before);
          }
        }
        alice.connection.close(true, 0, Unpooled.EMPTY_BUFFER).sync();
        await(() -> snapshot(server).active() == 1);
        try (Peer replacement = new Peer(server.address(), "alice", 65536, true, null)) {
          replacement.negotiate(options.offer());
          bob.send(new Detach(1));
          assertEquals(new Detached(1), bob.next());
          replacement.send(new Detach(1));
          assertEquals(new Detached(1), replacement.next());
          assertEquals(2, snapshot(server).peakConnections());
          assertEquals(3, snapshot(server).peakTransports());
          assertEquals(2, snapshot(server).owners());
          // Retried Initial packets can create another refused transport for the same remote peer.
          assertTrue(snapshot(server).refused() >= 17);
        }
      }
    }
    try (CoreServer server = start(options);
        Peer anonymous = new Peer(server.address(), null, 65536, true, null)) {
      anonymous.negotiate(options.offer());
      try (Peer duplicate = new Peer(server.address(), null, 65536, true, null)) {
        duplicate.handshake();
        duplicate.error(ProtocolError.Code.LIMIT_EXCEEDED);
      }
      try (Peer mapped = new Peer(server.address(), "bob", 65536, true, null)) {
        mapped.negotiate(options.offer());
        assertEquals(2, snapshot(server).owners());
        assertEquals(2, snapshot(server).active());
        anonymous.send(new Detach(1));
        assertEquals(new Detached(1), anonymous.next());
      }
    }
  }

  @Test
  void nonReadingControlPeerHitsBoundedCountAndByteQueuesWithoutBlockingAnotherConnection()
      throws Exception {
    for (boolean byteBudget : List.of(false, true)) {
      CoreOptions options =
          options(byteBudget ? 1024 : 2, byteBudget ? 4101 : 65536, 5000, 10000, 4, 4);
      try (CoreServer server = start(options);
          Peer stalled = new Peer(server.address(), null, 128, true, null)) {
        stalled.negotiate(options.offer());
        stalled
            .control
            .eventLoop()
            .submit(() -> stalled.control.config().setAutoRead(false))
            .sync();
        List<Message> requests = new ArrayList<>();
        for (long request = 1; request <= 300; request++) requests.add(new NextSequence(request));
        stalled.send(requests.toArray(Message[]::new));
        stalled.error(ProtocolError.Code.LIMIT_EXCEEDED);
        assertTrue(snapshot(server).peakControlBytes() <= options.queuedControlBytes());
        assertTrue(snapshot(server).peakControlFrames() <= options.pendingLimit());
        if (byteBudget) assertTrue(snapshot(server).peakControlBytes() > 4000);
        else assertEquals(2, snapshot(server).peakControlFrames());
        try (Peer other = new Peer(server.address(), null, 65536, true, null)) {
          other.negotiate(options.offer());
          other.send(new Detach(1));
          assertEquals(new Detached(1), other.next());
        }
      }
    }
  }

  @Test
  void detachDeadlineCannotBeRenewedByLaterValidRequests() throws Exception {
    CoreOptions options = options(16, 65536, 5000, 1000, 8, 4);
    try (CoreServer server = start(options);
        Peer peer = new Peer(server.address(), null, 65536, true, null)) {
      peer.negotiate(options.offer());
      peer.send(new Detach(1));
      assertEquals(new Detached(1), peer.next());
      var id = new AtomicInteger(2);
      var ticker =
          peer.control
              .eventLoop()
              .scheduleAtFixedRate(
                  () ->
                      peer.control.writeAndFlush(
                          Unpooled.wrappedBuffer(
                              Wire.encode(new Detach(id.getAndIncrement()), 4096))),
                  0,
                  100,
                  TimeUnit.MILLISECONDS);
      try {
        peer.error(ProtocolError.Code.LIMIT_EXCEEDED);
        assertTrue(id.get() >= 4);
        Message response;
        while ((response = peer.messages.poll()) != null)
          assertEquals(
              ProtocolError.Code.NOT_READY, assertInstanceOf(Refusal.class, response).code());
      } finally {
        ticker.cancel(false);
      }
    }
  }

  @Test
  void incomingRequestsCannotRenewTheOldestBlockedResponseDeadline() throws Exception {
    CoreOptions options = options(1024, 65536, 500, 1000, 4, 4);
    try (CoreServer server = start(options);
        Peer peer = new Peer(server.address(), null, 128, true, null)) {
      peer.negotiate(options.offer());
      peer.control.eventLoop().submit(() -> peer.control.config().setAutoRead(false)).sync();
      var request = new AtomicInteger(1);
      var ticker =
          peer.control
              .eventLoop()
              .scheduleAtFixedRate(
                  () ->
                      peer.control.writeAndFlush(
                          Unpooled.wrappedBuffer(
                              Wire.encode(new NextSequence(request.getAndIncrement()), 4096))),
                  0,
                  40,
                  TimeUnit.MILLISECONDS);
      try {
        peer.error(ProtocolError.Code.LIMIT_EXCEEDED);
        assertTrue(request.get() >= 8, "valid requests kept arriving before the write deadline");
        assertTrue(snapshot(server).peakControlFrames() >= 2, "responses really were queued");
        assertTrue(snapshot(server).peakControlFrames() < options.pendingLimit());
        assertTrue(snapshot(server).peakControlBytes() < options.queuedControlBytes());
      } finally {
        ticker.cancel(false);
      }
    }
  }

  @Test
  void invalidBudgetsAndWrongTlsRoleRefuseBeforeOpeningAListener() throws Exception {
    int boundary = 128 * 1024 * 1024 / (4096 + 4101 + 128);
    assertDoesNotThrow(
        () -> new CoreOptions(4096, 1, 1000, 1000, boundary, 1, 4101, 128, 128, 1000, 1000));
    assertThrows(
        IllegalArgumentException.class,
        () -> new CoreOptions(4096, 1, 1000, 1000, boundary + 1, 1, 4101, 128, 128, 1000, 1000));
    assertThrows(
        IllegalArgumentException.class,
        () -> new CoreOptions(4096, 0, 1000, 1000, 1, 1, 8192, 128, 128, 1000, 1000));
    assertThrows(
        IllegalArgumentException.class,
        () -> new CoreOptions(4096, 1, 1000, 1000, 1, 1, 4096, 128, 128, 1000, 1000));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            CoreServer.start(
                new InetSocketAddress("127.0.0.1", 0), client(null), CoreOptions.defaults()));
    try (CoreServer server = start(CoreOptions.defaults())) {
      assertTrue(server.address().getPort() > 0);
      assertEquals(0, snapshot(server).active());
    }
  }

  private static CoreOptions options(
      int pending, int bytes, long controlMs, long lifetimeMs, int connections, int owner) {
    return new CoreOptions(
        4096, pending, 1000, lifetimeMs, connections, owner, bytes, 32768, 4096, 5000, controlMs);
  }

  private static Path path(String name) {
    return directory.resolve(name);
  }

  private static X509Certificate cert(String name) throws Exception {
    try (var input = Files.newInputStream(path(name + ".crt"))) {
      return (X509Certificate) CertificateFactory.getInstance("X.509").generateCertificate(input);
    }
  }

  private static TlsAuthentication client(String owner) throws Exception {
    return TlsAuthentication.client(
        path("ca.crt"),
        "localhost",
        owner == null ? null : path(owner + ".crt"),
        owner == null ? null : path(owner + ".key"));
  }

  private static CoreServer start(CoreOptions options) throws Exception {
    TlsAuthentication server =
        TlsAuthentication.server(
            path("ca.crt"),
            path("server.crt"),
            path("server.key"),
            Map.of(
                TlsAuthentication.fingerprint(cert("alice")),
                "alice",
                TlsAuthentication.fingerprint(cert("bob")),
                "bob"),
            Clock.systemUTC());
    return CoreServer.start(new InetSocketAddress("127.0.0.1", 0), server, options);
  }

  private static CoreServer.Snapshot snapshot(CoreServer server) {
    try {
      return server.snapshot().toCompletableFuture().get(5, TimeUnit.SECONDS);
    } catch (Exception failure) {
      throw new AssertionError(failure);
    }
  }

  private static void await(BooleanSupplier condition) throws Exception {
    long start = System.nanoTime();
    while (!condition.getAsBoolean()) {
      assertTrue(System.nanoTime() - start < TimeUnit.SECONDS.toNanos(5), "condition deadline");
      Thread.sleep(5);
    }
  }

  private static void refusal(Message message, long request, ProtocolError.Code code) {
    assertEquals(
        new Records.RequestTag(false, request), assertInstanceOf(Refusal.class, message).request());
    assertEquals(code, ((Refusal) message).code());
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

  /** Raw bounded test peer deliberately capable of illegal application messages. */
  private static final class Peer implements AutoCloseable {
    final MultiThreadIoEventLoopGroup group =
        new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
    final ArrayBlockingQueue<Message> messages = new ArrayBlockingQueue<>(1024);
    final CompletableFuture<QuicConnectionCloseEvent> closed = new CompletableFuture<>();
    final CompletableFuture<Void> fin = new CompletableFuture<>();
    final TlsAuthentication.Guard guard;
    final boolean autoRead;
    final Channel socket;
    final Future<QuicChannel> connecting;
    volatile QuicChannel connection;
    QuicStreamChannel control;

    Peer(InetSocketAddress remote, String owner, int window, boolean autoRead, Executor tlsExecutor)
        throws Exception {
      this.autoRead = autoRead;
      TlsAuthentication authentication = client(owner);
      guard = authentication.guard();
      var codec =
          new QuicClientCodecBuilder()
              .version(1)
              .sslEngineProvider(c -> authentication.engine(c.alloc(), remote.getPort()))
              .maxIdleTimeout(10, TimeUnit.SECONDS)
              .initialMaxData(window)
              .initialMaxStreamDataBidirectionalLocal(window)
              .initialMaxStreamDataBidirectionalRemote(0)
              .initialMaxStreamDataUnidirectional(0)
              .initialMaxStreamsBidirectional(0)
              .initialMaxStreamsUnidirectional(0);
      if (tlsExecutor != null) codec.sslTaskExecutor(tlsExecutor);
      socket =
          new Bootstrap()
              .group(group)
              .channel(NioDatagramChannel.class)
              .handler(codec.build())
              .bind(new InetSocketAddress("127.0.0.1", 0))
              .sync()
              .channel();
      connecting =
          QuicChannel.newBootstrap(socket)
              .handler(
                  new ChannelInitializer<QuicChannel>() {
                    @Override
                    protected void initChannel(QuicChannel channel) {
                      connection = channel;
                      channel
                          .pipeline()
                          .addLast(
                              guard,
                              new ChannelInboundHandlerAdapter() {
                                @Override
                                public void userEventTriggered(
                                    ChannelHandlerContext ctx, Object event) {
                                  if (event instanceof QuicConnectionCloseEvent close)
                                    closed.complete(close);
                                  ctx.fireUserEventTriggered(event);
                                }

                                @Override
                                public void exceptionCaught(
                                    ChannelHandlerContext ctx, Throwable cause) {
                                  /* Guard and connect future retain failures. */
                                }
                              });
                    }
                  })
              .streamHandler(new ChannelInboundHandlerAdapter())
              .remoteAddress(remote)
              .connect();
    }

    void handshake() throws Exception {
      connecting.get(5, TimeUnit.SECONDS);
      guard.ready().toCompletableFuture().get(5, TimeUnit.SECONDS);
    }

    void open() throws Exception {
      handshake();
      control =
          connection
              .createStream(
                  QuicStreamType.BIDIRECTIONAL,
                  new ChannelInitializer<QuicStreamChannel>() {
                    @Override
                    protected void initChannel(QuicStreamChannel stream) {
                      stream.config().setAllowHalfClosure(true).setAutoRead(autoRead);
                      stream
                          .pipeline()
                          .addLast(
                              new SimpleChannelInboundHandler<ByteBuf>() {
                                final Wire.Decoder decoder = new Wire.Decoder(16384);

                                @Override
                                protected void channelRead0(
                                    ChannelHandlerContext ctx, ByteBuf bytes) {
                                  ByteBuffer source = bytes.nioBuffer();
                                  while (source.hasRemaining()) {
                                    Wire.Frame frame = decoder.feed(source);
                                    if (frame == null) break;
                                    assertTrue(messages.offer(((Wire.Known) frame).message()));
                                  }
                                }

                                @Override
                                public void userEventTriggered(
                                    ChannelHandlerContext ctx, Object event) {
                                  if (event instanceof ChannelInputShutdownEvent) {
                                    decoder.finish();
                                    fin.complete(null);
                                  }
                                  ctx.fireUserEventTriggered(event);
                                }

                                @Override
                                public void exceptionCaught(
                                    ChannelHandlerContext ctx, Throwable cause) {
                                  fin.completeExceptionally(cause);
                                }
                              });
                    }
                  })
              .get(5, TimeUnit.SECONDS);
      assertEquals(0, control.streamId());
    }

    void negotiate(Capabilities offer) throws Exception {
      open();
      send(offer);
      offer.validateResponse(assertInstanceOf(Capabilities.class, next()));
    }

    void send(Message... frames) throws Exception {
      var bytes = new ByteArrayOutputStream();
      for (Message message : frames) bytes.writeBytes(Wire.encode(message, 16384));
      sendBytes(bytes.toByteArray());
    }

    void sendBytes(byte[] bytes) throws Exception {
      control.writeAndFlush(Unpooled.wrappedBuffer(bytes)).sync();
    }

    Message next() throws Exception {
      Message message = messages.poll(5, TimeUnit.SECONDS);
      assertNotNull(message);
      return message;
    }

    void error(ProtocolError.Code code) throws Exception {
      QuicConnectionCloseEvent event = closed.get(5, TimeUnit.SECONDS);
      assertTrue(event.isApplicationClose(), event.toString());
      assertEquals(code.applicationError(), event.error(), event.toString());
    }

    @Override
    public void close() {
      if (connection != null) connection.close().awaitUninterruptibly();
      socket.close().awaitUninterruptibly();
      group.shutdownGracefully(0, 2, TimeUnit.SECONDS).awaitUninterruptibly();
    }
  }
}
