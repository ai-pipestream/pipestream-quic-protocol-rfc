package ai.pipestream.quic.v2;

import static org.junit.jupiter.api.Assertions.*;

import ai.pipestream.quic.AddressValidationTokenHandler;
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
import io.netty.handler.codec.quic.QuicChannel;
import io.netty.handler.codec.quic.QuicChannelOption;
import io.netty.handler.codec.quic.QuicClientCodecBuilder;
import io.netty.handler.codec.quic.QuicServerCodecBuilder;
import io.netty.handler.codec.quic.QuicStreamChannel;
import io.netty.handler.codec.quic.QuicStreamFrame;
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
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

@Timeout(30)
final class StreamTransportTest {
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
        "/CN=Stream-Transport-Test-CA",
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
  void limitsValidateGeometryAndNativeReservation() {
    var limits = limits();
    assertEquals(512, limits.receiveWindowBytes());
    assertEquals(4224, limits.nativeSendLimits().total());
    assertEquals(4096, limits.nativeSendLimits().reserved());
    assertThrows(
        IllegalArgumentException.class, () -> new StreamTransport.Limits(-1, 1, 1, 1, 128, 128, 1));
    assertThrows(
        IllegalArgumentException.class, () -> new StreamTransport.Limits(1, 0, 1, 1, 128, 128, 1));
    assertThrows(
        IllegalArgumentException.class, () -> new StreamTransport.Limits(0, 1, 0, 1, 128, 128, 1));
    assertThrows(
        IllegalArgumentException.class, () -> new StreamTransport.Limits(1, 1, 0, 1, 128, 128, 1));
    assertThrows(
        IllegalArgumentException.class, () -> new StreamTransport.Limits(1, 1, 1, 0, 128, 128, 1));
    assertThrows(
        IllegalArgumentException.class, () -> new StreamTransport.Limits(1, 1, 1, 1, 127, 128, 1));
    assertThrows(
        IllegalArgumentException.class, () -> new StreamTransport.Limits(1, 1, 1, 1, 128, 127, 1));
    assertThrows(
        IllegalArgumentException.class, () -> new StreamTransport.Limits(1, 1, 1, 1, 128, 128, 0));
  }

  @Test
  void stalledDataDoesNotBlockTypedControlAndFinFollowsExactPayload() throws Exception {
    try (Network network = new Network()) {
      ProtocolError duplicateControl =
          assertInstanceOf(
              ProtocolError.class,
              assertThrows(
                      ExecutionException.class,
                      () ->
                          on(
                              network.client,
                              () -> {
                                network.clientTransport.bindControl(network.clientControl);
                                return null;
                              }))
                  .getCause());
      assertEquals(ProtocolError.Code.FRAME_ERROR, duplicateControl.code());
      byte[] first = bytes(0, 128);
      byte[] second = bytes(128, 128);
      StreamTransport.Data outgoing =
          on(
                  network.client,
                  () -> network.clientTransport.openData(new ChannelInboundHandlerAdapter()))
              .toCompletableFuture()
              .get(5, TimeUnit.SECONDS);

      on(network.client, () -> outgoing.write(first))
          .toCompletableFuture()
          .get(5, TimeUnit.SECONDS);
      StreamTransport.Data incoming = network.serverData.get(5, TimeUnit.SECONDS);
      var stalled = on(network.client, () -> outgoing.write(second));
      assertFalse(stalled.toCompletableFuture().isDone());
      assertFalse(network.serverReceived.isDone());
      ExecutionException earlyRelease =
          assertThrows(
              ExecutionException.class,
              () ->
                  on(
                      network.client,
                      () -> {
                        outgoing.release();
                        return null;
                      }));
      assertInstanceOf(IllegalStateException.class, earlyRelease.getCause());
      ProtocolError inFlight =
          assertInstanceOf(
              ProtocolError.class,
              assertThrows(
                      ExecutionException.class,
                      () -> on(network.client, () -> outgoing.write(new byte[] {1})))
                  .getCause());
      assertEquals(ProtocolError.Code.LIMIT_EXCEEDED, inFlight.code());

      network.send(network.clientControl, new Messages.Detach(1));
      assertEquals(new Messages.Detach(1), network.serverMessages.poll(5, TimeUnit.SECONDS));
      network.send(network.serverControl, new Messages.Detached(1));
      assertEquals(new Messages.Detached(1), network.clientMessages.poll(5, TimeUnit.SECONDS));
      assertFalse(stalled.toCompletableFuture().isDone());
      var fin = on(network.client, outgoing::finish);
      assertFalse(fin.toCompletableFuture().isDone());
      assertFalse(network.serverReceived.isDone());

      on(network.server, () -> incoming.stream().config().setAutoRead(true));
      stalled.toCompletableFuture().get(5, TimeUnit.SECONDS);
      fin.toCompletableFuture().get(5, TimeUnit.SECONDS);
      assertArrayEquals(join(first, second), network.serverReceived.get(5, TimeUnit.SECONDS));
      assertEquals(1, on(network.client, () -> network.clientTransport.snapshot()).outgoing());
      on(
          network.client,
          () -> {
            outgoing.release();
            return null;
          });
      on(
          network.server,
          () -> {
            incoming.release();
            return null;
          });
      assertEquals(0, on(network.client, () -> network.clientTransport.snapshot()).outgoing());
    }
  }

  @Test
  void serverToClientDataUsesIndependentControlAndExplicitSettlement() throws Exception {
    try (Network network = new Network()) {
      byte[] first = bytes(3, 128);
      byte[] second = bytes(131, 128);
      StreamTransport.Data outgoing =
          on(
                  network.server,
                  () -> network.serverTransport.openData(new ChannelInboundHandlerAdapter()))
              .toCompletableFuture()
              .get(5, TimeUnit.SECONDS);
      on(network.server, () -> outgoing.write(first))
          .toCompletableFuture()
          .get(5, TimeUnit.SECONDS);
      StreamTransport.Data incoming = network.clientData.get(5, TimeUnit.SECONDS);
      var stalled = on(network.server, () -> outgoing.write(second));
      assertFalse(stalled.toCompletableFuture().isDone());
      assertFalse(network.clientReceived.isDone());

      network.send(network.clientControl, new Messages.Detach(1));
      assertEquals(new Messages.Detach(1), network.serverMessages.poll(5, TimeUnit.SECONDS));
      network.send(network.serverControl, new Messages.Detached(1));
      assertEquals(new Messages.Detached(1), network.clientMessages.poll(5, TimeUnit.SECONDS));
      on(network.client, () -> incoming.stream().config().setAutoRead(true));
      stalled.toCompletableFuture().get(5, TimeUnit.SECONDS);
      on(network.server, outgoing::finish).toCompletableFuture().get(5, TimeUnit.SECONDS);
      assertArrayEquals(join(first, second), network.clientReceived.get(5, TimeUnit.SECONDS));
      assertEquals(1, on(network.server, () -> network.serverTransport.snapshot()).outgoing());
      QuicStreamChannel ended = on(network.client, incoming::stream);
      on(
          network.server,
          () -> {
            outgoing.release();
            return null;
          });
      on(
          network.client,
          () -> {
            incoming.release();
            return null;
          });
      ProtocolError duplicate =
          assertInstanceOf(
              ProtocolError.class,
              assertThrows(
                      ExecutionException.class,
                      () -> on(network.client, () -> network.clientTransport.claimIncoming(ended)))
                  .getCause());
      assertEquals(ProtocolError.Code.FRAME_ERROR, duplicate.code());
    }
  }

  @Test
  void localResetSettlesQueuedWriteBeforeExplicitSlotRelease() throws Exception {
    try (Network network = new Network()) {
      StreamTransport.Data outgoing =
          on(
                  network.client,
                  () -> network.clientTransport.openData(new ChannelInboundHandlerAdapter()))
              .toCompletableFuture()
              .get(5, TimeUnit.SECONDS);
      on(network.client, () -> outgoing.write(bytes(0, 128)))
          .toCompletableFuture()
          .get(5, TimeUnit.SECONDS);
      network.serverData.get(5, TimeUnit.SECONDS);
      var queued = on(network.client, () -> outgoing.write(bytes(128, 128)));
      assertFalse(queued.toCompletableFuture().isDone());
      ProtocolError reset = new ProtocolError(ProtocolError.Code.CANCELLED, "test reset");
      on(
          network.client,
          () -> {
            outgoing.abort(reset);
            return null;
          });
      assertSame(
          reset,
          assertThrows(
                  ExecutionException.class,
                  () -> queued.toCompletableFuture().get(5, TimeUnit.SECONDS))
              .getCause());
      assertEquals(1, on(network.client, () -> network.clientTransport.snapshot()).outgoing());
      on(
          network.client,
          () -> {
            outgoing.release();
            return null;
          });
      assertEquals(0, on(network.client, () -> network.clientTransport.snapshot()).outgoing());
    }
  }

  @Test
  void stalledWriteDeadlineDoesNotPreventControlProgress() throws Exception {
    try (Network network = new Network(200)) {
      StreamTransport.Data outgoing =
          on(
                  network.client,
                  () -> network.clientTransport.openData(new ChannelInboundHandlerAdapter()))
              .toCompletableFuture()
              .get(5, TimeUnit.SECONDS);
      on(network.client, () -> outgoing.write(bytes(0, 128)))
          .toCompletableFuture()
          .get(5, TimeUnit.SECONDS);
      network.serverData.get(5, TimeUnit.SECONDS);
      var stalled = on(network.client, () -> outgoing.write(bytes(128, 128)));
      network.send(network.clientControl, new Messages.Detach(1));
      assertEquals(new Messages.Detach(1), network.serverMessages.poll(5, TimeUnit.SECONDS));
      ExecutionException timeout =
          assertThrows(
              ExecutionException.class,
              () -> stalled.toCompletableFuture().get(5, TimeUnit.SECONDS));
      ProtocolError failure = assertInstanceOf(ProtocolError.class, timeout.getCause());
      assertEquals(ProtocolError.Code.LIMIT_EXCEEDED, failure.code());
    }
  }

  private static StreamTransport.Limits limits() {
    return new StreamTransport.Limits(1, 4, 128, 4096, 128, 128, 5000);
  }

  private static byte[] bytes(int start, int length) {
    byte[] bytes = new byte[length];
    for (int i = 0; i < length; i++) bytes[i] = (byte) (start + i);
    return bytes;
  }

  private static byte[] join(byte[] first, byte[] second) {
    byte[] joined = Arrays.copyOf(first, first.length + second.length);
    System.arraycopy(second, 0, joined, first.length, second.length);
    return joined;
  }

  private static <T> T on(QuicChannel channel, Callable<T> action) throws Exception {
    return channel.eventLoop().submit(action).get(5, TimeUnit.SECONDS);
  }

  private static Path path(String name) {
    return directory.resolve(name);
  }

  private static X509Certificate cert(String name) throws Exception {
    try (var input = Files.newInputStream(path(name + ".crt"))) {
      return (X509Certificate) CertificateFactory.getInstance("X.509").generateCertificate(input);
    }
  }

  private static TlsAuthentication serverAuth() throws Exception {
    return TlsAuthentication.server(
        path("ca.crt"),
        path("server.crt"),
        path("server.key"),
        Map.of(TlsAuthentication.fingerprint(cert("client")), "client"),
        Clock.systemUTC());
  }

  private static TlsAuthentication clientAuth() throws Exception {
    return TlsAuthentication.client(
        path("ca.crt"), "localhost", path("client.crt"), path("client.key"));
  }

  private static void command(String... command) throws Exception {
    Process process =
        new ProcessBuilder(command).directory(directory.toFile()).redirectErrorStream(true).start();
    try {
      assertTrue(process.waitFor(10, TimeUnit.SECONDS));
      String output = new String(process.getInputStream().readAllBytes());
      assertEquals(0, process.exitValue(), output);
    } finally {
      if (process.isAlive()) process.destroyForcibly().waitFor(5, TimeUnit.SECONDS);
    }
  }

  /** Authenticated transport fixture only; it advertises no durable application profile. */
  private static final class Network implements AutoCloseable {
    final StreamTransport.Limits limits;
    final MultiThreadIoEventLoopGroup group =
        new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
    final CompletableFuture<StreamTransport.Data> serverData = new CompletableFuture<>();
    final CompletableFuture<byte[]> serverReceived = new CompletableFuture<>();
    final CompletableFuture<byte[]> clientReceived = new CompletableFuture<>();
    final CompletableFuture<StreamTransport.Data> clientData = new CompletableFuture<>();
    final LinkedBlockingQueue<Messages.Message> serverMessages = new LinkedBlockingQueue<>(16);
    final LinkedBlockingQueue<Messages.Message> clientMessages = new LinkedBlockingQueue<>(16);
    Channel listener;
    Channel socket;
    QuicChannel server;
    QuicChannel client;
    StreamTransport serverTransport;
    StreamTransport clientTransport;
    QuicStreamChannel serverControl;
    QuicStreamChannel clientControl;

    Network() throws Exception {
      this(5000);
    }

    Network(long writeTimeoutMs) throws Exception {
      limits = new StreamTransport.Limits(1, 4, 128, 4096, 128, 128, writeTimeoutMs);
      try {
        TlsAuthentication serverAuth = serverAuth();
        CompletableFuture<TlsAuthentication.Guard> serverGuard = new CompletableFuture<>();
        CompletableFuture<QuicChannel> accepted = new CompletableFuture<>();
        CompletableFuture<StreamTransport> serverOwner = new CompletableFuture<>();
        CompletableFuture<QuicStreamChannel> serverControlFuture = new CompletableFuture<>();
        var serverBuilder =
            limits
                .configure(new QuicServerCodecBuilder(), true)
                .version(1)
                .sslEngineProvider(c -> serverAuth.engine(c.alloc(), 0))
                .maxIdleTimeout(10, TimeUnit.SECONDS)
                .tokenHandler(new AddressValidationTokenHandler())
                .option(QuicChannelOption.STREAM_SEND_BUFFER_LIMITS, limits.nativeSendLimits())
                .handler(
                    new ChannelInitializer<QuicChannel>() {
                      @Override
                      protected void initChannel(QuicChannel channel) {
                        var guard = serverAuth.guard();
                        channel.pipeline().addLast(guard);
                        serverGuard.complete(guard);
                        accepted.complete(channel);
                        serverOwner.complete(new StreamTransport(channel, limits, true));
                      }
                    })
                .streamHandler(
                    new ChannelInitializer<QuicStreamChannel>() {
                      @Override
                      protected void initChannel(QuicStreamChannel stream) {
                        StreamTransport owner = serverOwner.join();
                        if (stream.type() == QuicStreamType.BIDIRECTIONAL) {
                          owner.bindControl(stream);
                          stream.pipeline().addLast(new ControlReader(serverMessages));
                          serverControlFuture.complete(stream);
                        } else {
                          StreamTransport.Data data = owner.claimIncoming(stream);
                          stream.pipeline().addLast(new DataReader(serverReceived));
                          serverData.complete(data);
                        }
                      }
                    });
        listener =
            new Bootstrap()
                .group(group)
                .channel(NioDatagramChannel.class)
                .handler(serverBuilder.build())
                .bind(new InetSocketAddress("127.0.0.1", 0))
                .sync()
                .channel();

        TlsAuthentication clientAuth = clientAuth();
        CompletableFuture<TlsAuthentication.Guard> clientGuard = new CompletableFuture<>();
        var clientBuilder =
            limits
                .configure(new QuicClientCodecBuilder(), false)
                .version(1)
                .sslEngineProvider(
                    c ->
                        clientAuth.engine(
                            c.alloc(), ((InetSocketAddress) listener.localAddress()).getPort()))
                .maxIdleTimeout(10, TimeUnit.SECONDS);
        socket =
            new Bootstrap()
                .group(group)
                .channel(NioDatagramChannel.class)
                .handler(clientBuilder.build())
                .bind(new InetSocketAddress("127.0.0.1", 0))
                .sync()
                .channel();
        CompletableFuture<StreamTransport> clientOwner = new CompletableFuture<>();
        Future<QuicChannel> connecting =
            QuicChannel.newBootstrap(socket)
                .option(QuicChannelOption.STREAM_SEND_BUFFER_LIMITS, limits.nativeSendLimits())
                .handler(
                    new ChannelInitializer<QuicChannel>() {
                      @Override
                      protected void initChannel(QuicChannel channel) {
                        var guard = clientAuth.guard();
                        channel.pipeline().addLast(guard);
                        clientGuard.complete(guard);
                        clientOwner.complete(new StreamTransport(channel, limits, false));
                      }
                    })
                .streamHandler(
                    new ChannelInitializer<QuicStreamChannel>() {
                      @Override
                      protected void initChannel(QuicStreamChannel stream) {
                        StreamTransport.Data data = clientOwner.join().claimIncoming(stream);
                        stream.pipeline().addLast(new DataReader(clientReceived));
                        clientData.complete(data);
                      }
                    })
                .remoteAddress(listener.localAddress())
                .connect();
        client = connecting.get(5, TimeUnit.SECONDS);
        server = accepted.get(5, TimeUnit.SECONDS);
        clientGuard.get(5, TimeUnit.SECONDS).ready().toCompletableFuture().get(5, TimeUnit.SECONDS);
        serverGuard.get(5, TimeUnit.SECONDS).ready().toCompletableFuture().get(5, TimeUnit.SECONDS);
        clientTransport = clientOwner.get(5, TimeUnit.SECONDS);
        serverTransport = serverOwner.get(5, TimeUnit.SECONDS);
        clientControl =
            client
                .createStream(QuicStreamType.BIDIRECTIONAL, new ControlReader(clientMessages))
                .get(5, TimeUnit.SECONDS);
        on(
            client,
            () -> {
              clientTransport.bindControl(clientControl);
              return null;
            });
        Messages.Capabilities offer = coreCapabilities(false);
        Messages.Capabilities selected = coreCapabilities(true);
        send(clientControl, offer);
        serverControl = serverControlFuture.get(5, TimeUnit.SECONDS);
        assertEquals(offer, serverMessages.poll(5, TimeUnit.SECONDS));
        send(serverControl, selected);
        assertEquals(selected, clientMessages.poll(5, TimeUnit.SECONDS));
      } catch (Exception | Error failure) {
        close();
        throw failure;
      }
    }

    void send(QuicStreamChannel stream, Messages.Message message) throws Exception {
      stream
          .writeAndFlush(Unpooled.wrappedBuffer(Wire.encode(message, 4096)))
          .get(5, TimeUnit.SECONDS);
    }

    @Override
    public void close() {
      if (client != null) client.close().syncUninterruptibly();
      if (server != null) server.close().syncUninterruptibly();
      if (socket != null) socket.close().syncUninterruptibly();
      if (listener != null) listener.close().syncUninterruptibly();
      group.shutdownGracefully(0, 2, TimeUnit.SECONDS).syncUninterruptibly();
    }
  }

  private static final class ControlReader extends SimpleChannelInboundHandler<ByteBuf> {
    final Wire.Decoder decoder = new Wire.Decoder(4096);
    final LinkedBlockingQueue<Messages.Message> messages;

    ControlReader(LinkedBlockingQueue<Messages.Message> messages) {
      this.messages = messages;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, ByteBuf bytes) {
      ByteBuffer source = bytes.nioBuffer();
      while (source.hasRemaining()) {
        Wire.Frame frame = decoder.feed(source);
        if (frame == null) return;
        assertTrue(messages.offer(((Wire.Known) frame).message()));
      }
    }
  }

  private static final class DataReader extends SimpleChannelInboundHandler<QuicStreamFrame> {
    final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    final CompletableFuture<byte[]> received;

    DataReader(CompletableFuture<byte[]> received) {
      this.received = received;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, QuicStreamFrame frame) {
      ByteBuf input = frame.content();
      assertTrue(bytes.size() + input.readableBytes() <= 256, "test payload bound");
      byte[] part = new byte[input.readableBytes()];
      input.readBytes(part);
      bytes.writeBytes(part);
      if (frame.hasFin()) received.complete(bytes.toByteArray());
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
      if (!received.isDone())
        received.completeExceptionally(new AssertionError("data stream closed without FIN frame"));
      ctx.fireChannelInactive();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
      received.completeExceptionally(cause);
      ctx.close();
    }
  }

  private static Messages.Capabilities coreCapabilities(boolean response) {
    return new Messages.Capabilities(response, List.of(), List.of(), 4096, 1, 1, 128, 1000, 5000);
  }
}
