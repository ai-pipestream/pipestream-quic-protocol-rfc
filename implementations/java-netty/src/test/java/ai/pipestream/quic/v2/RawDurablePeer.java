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
import io.netty.handler.codec.quic.QuicChannelOption;
import io.netty.handler.codec.quic.QuicClientCodecBuilder;
import io.netty.handler.codec.quic.QuicConnectionCloseEvent;
import io.netty.handler.codec.quic.QuicStreamChannel;
import io.netty.handler.codec.quic.QuicStreamFrame;
import io.netty.handler.codec.quic.QuicStreamType;
import io.netty.util.concurrent.Future;
import java.io.ByteArrayOutputStream;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Raw bounded QUIC peer for attacking the durable listener with exact frames, including illegal
 * ones. It is deliberately not a durable client: it journals nothing and validates nothing beyond
 * what each test asserts.
 */
final class RawDurablePeer implements AutoCloseable {
  /** One received server unidirectional stream: raw bytes and the terminal event. */
  static final class Incoming {
    final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    final CompletableFuture<byte[]> complete = new CompletableFuture<>();
    volatile long streamId;
  }

  final MultiThreadIoEventLoopGroup group =
      new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
  final ArrayBlockingQueue<Message> messages = new ArrayBlockingQueue<>(1024);
  final ArrayBlockingQueue<Incoming> incoming = new ArrayBlockingQueue<>(64);
  final CompletableFuture<QuicConnectionCloseEvent> closed = new CompletableFuture<>();
  final CompletableFuture<Void> fin = new CompletableFuture<>();
  final TlsAuthentication.Guard guard;
  final Channel socket;
  final Future<QuicChannel> connecting;
  volatile QuicChannel connection;
  QuicStreamChannel control;
  Capabilities selected;
  long nextRequest = 1;

  RawDurablePeer(InetSocketAddress remote, TlsAuthentication authentication, int window)
      throws Exception {
    guard = authentication.guard();
    var codec =
        new QuicClientCodecBuilder()
            .version(1)
            .sslEngineProvider(c -> authentication.engine(c.alloc(), remote.getPort()))
            .maxIdleTimeout(10, TimeUnit.SECONDS)
            .initialMaxData(8L * window)
            .initialMaxStreamDataBidirectionalLocal(window)
            .initialMaxStreamDataBidirectionalRemote(0)
            .initialMaxStreamDataUnidirectional(window)
            .initialMaxStreamsBidirectional(0)
            .initialMaxStreamsUnidirectional(16);
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
            .streamHandler(
                new ChannelInitializer<QuicStreamChannel>() {
                  @Override
                  protected void initChannel(QuicStreamChannel stream) {
                    Incoming record = new Incoming();
                    record.streamId = stream.streamId();
                    stream.config().setOption(QuicChannelOption.READ_FRAMES, true);
                    stream
                        .pipeline()
                        .addLast(
                            new SimpleChannelInboundHandler<QuicStreamFrame>() {
                              @Override
                              protected void channelRead0(
                                  ChannelHandlerContext ctx, QuicStreamFrame frame) {
                                byte[] part = new byte[frame.content().readableBytes()];
                                frame.content().readBytes(part);
                                record.bytes.writeBytes(part);
                                if (frame.hasFin())
                                  record.complete.complete(record.bytes.toByteArray());
                              }

                              @Override
                              public void channelInactive(ChannelHandlerContext ctx) {
                                record.complete.completeExceptionally(
                                    new IllegalStateException("result stream ended without FIN"));
                                ctx.fireChannelInactive();
                              }

                              @Override
                              public void exceptionCaught(
                                  ChannelHandlerContext ctx, Throwable cause) {
                                record.complete.completeExceptionally(cause);
                                ctx.close();
                              }
                            });
                    assertTrue(incoming.offer(record));
                  }
                })
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
                    stream.config().setAllowHalfClosure(true);
                    stream
                        .pipeline()
                        .addLast(
                            new SimpleChannelInboundHandler<ByteBuf>() {
                              final Wire.Decoder decoder = new Wire.Decoder(1 << 20);

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

  static Capabilities offer(List<Integer> profiles, long objectLimit) {
    return new Capabilities(false, profiles, List.of(), 1 << 20, 16, 64, objectLimit, 5000, 30_000);
  }

  Capabilities negotiate(Capabilities offer) throws Exception {
    open();
    send(offer);
    selected = assertInstanceOf(Capabilities.class, next());
    offer.validateResponse(selected);
    return selected;
  }

  long request() {
    return nextRequest++;
  }

  void send(Message... frames) throws Exception {
    var bytes = new ByteArrayOutputStream();
    for (Message message : frames) bytes.writeBytes(Wire.encode(message, 1 << 20));
    sendBytes(bytes.toByteArray());
  }

  void sendBytes(byte[] bytes) throws Exception {
    control.writeAndFlush(Unpooled.wrappedBuffer(bytes)).sync();
  }

  /**
   * Send one request and await its correlated control response.
   *
   * @param request client request
   * @return response or refusal
   * @throws Exception timeout or transport failure
   */
  Message call(Message request) throws Exception {
    send(request);
    Message response = next();
    long id = ClientCorrelation.requestId(request);
    Records.RequestTag tag = response instanceof Refusal r ? r.request() : null;
    if (tag != null) assertEquals(new Records.RequestTag(false, id), tag);
    return response;
  }

  Message next() throws Exception {
    Message message = messages.poll(10, TimeUnit.SECONDS);
    assertNotNull(message, "no control response within deadline");
    return message;
  }

  /**
   * Open one unidirectional input stream and write the exact header, payload and FIN.
   *
   * @param header input header
   * @param payload complete payload
   * @param finish whether to send FIN
   * @return stream, for identity and later abort
   * @throws Exception transport failure
   */
  QuicStreamChannel sendInput(Records.InputHeader header, byte[] payload, boolean finish)
      throws Exception {
    QuicStreamChannel stream =
        connection
            .createStream(QuicStreamType.UNIDIRECTIONAL, new ChannelInboundHandlerAdapter())
            .get(5, TimeUnit.SECONDS);
    stream.writeAndFlush(Unpooled.wrappedBuffer(Wire.encodeHeader(header))).sync();
    int offset = 0;
    while (offset < payload.length) {
      int count = Math.min(16384, payload.length - offset);
      stream.writeAndFlush(Unpooled.wrappedBuffer(payload, offset, count)).sync();
      offset += count;
    }
    if (finish) stream.shutdownOutput().sync();
    return stream;
  }

  /**
   * Await the next server unidirectional stream to complete with FIN.
   *
   * @return raw header-plus-payload bytes
   * @throws Exception timeout or stream failure
   */
  byte[] nextObject() throws Exception {
    Incoming record = incoming.poll(10, TimeUnit.SECONDS);
    assertNotNull(record, "no result stream within deadline");
    return record.complete.get(10, TimeUnit.SECONDS);
  }

  Incoming nextIncoming() throws Exception {
    Incoming record = incoming.poll(10, TimeUnit.SECONDS);
    assertNotNull(record, "no result stream within deadline");
    return record;
  }

  void error(ProtocolError.Code code) throws Exception {
    QuicConnectionCloseEvent event = closed.get(10, TimeUnit.SECONDS);
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
