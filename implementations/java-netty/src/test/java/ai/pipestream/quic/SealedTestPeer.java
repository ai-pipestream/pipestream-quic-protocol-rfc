package ai.pipestream.quic;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.incubator.codec.quic.QuicChannel;
import io.netty.incubator.codec.quic.QuicClientCodecBuilder;
import io.netty.incubator.codec.quic.QuicConnectionCloseEvent;
import io.netty.incubator.codec.quic.QuicServerCodecBuilder;
import io.netty.incubator.codec.quic.QuicSslContextBuilder;
import io.netty.incubator.codec.quic.QuicStreamChannel;
import io.netty.incubator.codec.quic.QuicStreamType;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/** Fault-injection transports, not reference implementations or protocol oracles. */
final class SealedTestPeer {
  private SealedTestPeer() {}

  static final class RawClient implements AutoCloseable {
    final NioEventLoopGroup group = new NioEventLoopGroup(1);
    Channel datagram; QuicChannel connection; QuicStreamChannel control;
    final ArrayBlockingQueue<byte[]> replies = new ArrayBlockingQueue<>(16);
    final CompletableFuture<Long> closeCode = new CompletableFuture<>();
    volatile Throwable receiveFailure;

    RawClient(InetSocketAddress remote, Path certs) throws Exception {
      try {
      var tls = QuicSslContextBuilder.forClient().trustManager(certs.resolve("ca.crt").toFile()).applicationProtocols(Wire.ALPN).build();
      datagram = new Bootstrap().group(group).channel(NioDatagramChannel.class).handler(new QuicClientCodecBuilder()
          .sslEngineProvider(channel -> tls.newEngine(channel.alloc(), "localhost", remote.getPort()))
          .maxIdleTimeout(10, TimeUnit.SECONDS).initialMaxData(4L << 20)
          .initialMaxStreamDataBidirectionalLocal(2L << 20).initialMaxStreamDataBidirectionalRemote(2L << 20)
          .initialMaxStreamsBidirectional(0).initialMaxStreamsUnidirectional(0).build())
          .bind(new InetSocketAddress(0)).sync().channel();
      connection = QuicChannel.newBootstrap(datagram).handler(new ChannelInboundHandlerAdapter() {
        @Override public void userEventTriggered(ChannelHandlerContext context, Object event) {
          if (event instanceof QuicConnectionCloseEvent close && close.isApplicationClose()) closeCode.complete(Integer.toUnsignedLong(close.error()));
          context.fireUserEventTriggered(event);
        }
      })
          .streamHandler(new ChannelInboundHandlerAdapter()).remoteAddress(remote).connect().get(5, TimeUnit.SECONDS);
      control = connection.createStream(QuicStreamType.BIDIRECTIONAL, new ChannelInitializer<QuicStreamChannel>() {
        @Override protected void initChannel(QuicStreamChannel channel) {
          channel.pipeline().addLast(new LengthFieldBasedFrameDecoder(Wire.MAX_CONTROL_FRAME + 5, 1, 4, 0, 0))
              .addLast(new SimpleChannelInboundHandler<ByteBuf>() {
                @Override protected void channelRead0(ChannelHandlerContext context, ByteBuf bytes) {
                  byte[] frame = new byte[bytes.readableBytes()]; bytes.readBytes(frame);
                  if (!replies.offer(frame)) {
                    receiveFailure = new AssertionError("fault peer's local reply backlog overflowed");
                    context.channel().parent().close();
                  }
                }
              });
        }
      }).get(5, TimeUnit.SECONDS);
      send(SealedTransport.capabilities(SealedTransport.Limits.defaults()));
      Wire.ControlFrame response = response();
      if (response.type() != Wire.FRAME_CAPABILITIES) throw new AssertionError("expected capabilities");
      SealedTransport.response(response.payload(), SealedTransport.Limits.defaults());
      } catch (Exception | AssertionError failure) { close(); throw failure; }
    }

    void send(byte[] frame) throws Exception { control.writeAndFlush(Unpooled.wrappedBuffer(frame)).get(5, TimeUnit.SECONDS); }
    Wire.ControlFrame response() throws Exception {
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
      while (true) {
        long remaining = deadline - System.nanoTime();
        if (remaining <= 0) throw new AssertionError("fault-injection peer response timed out");
        byte[] encoded = replies.poll(remaining, TimeUnit.NANOSECONDS);
        if (encoded == null) throw new AssertionError("fault-injection peer did not receive a response; active="
            + connection.isActive() + ", close=" + closeCode.getNow(null), receiveFailure);
        var frame = Wire.decodeControl(encoded);
        if (frame.type() == Wire.FRAME_STATUS && SealedTransport.status(frame.payload()).state() == 0) continue;
        return frame;
      }
    }
    @Override public void close() {
      if (connection != null) connection.close().syncUninterruptibly();
      if (datagram != null) datagram.close().syncUninterruptibly();
      group.shutdownGracefully(0, 1, TimeUnit.SECONDS).syncUninterruptibly();
    }
  }

  @FunctionalInterface interface Responder { List<byte[]> respond(Wire.ControlFrame request) throws Exception; }

  /** Sends scripted malformed replies. It deliberately has no storage or processing behavior. */
  static final class ScriptServer implements AutoCloseable {
    final NioEventLoopGroup group = new NioEventLoopGroup(1);
    Channel datagram;
    ScriptServer(Path certs, Responder responder) throws Exception {
      try {
      var tls = QuicSslContextBuilder.forServer(certs.resolve("server.key").toFile(), null, certs.resolve("server.crt").toFile())
          .applicationProtocols(Wire.ALPN).build();
      datagram = new Bootstrap().group(group).channel(NioDatagramChannel.class).handler(new QuicServerCodecBuilder()
          .sslContext(tls).maxIdleTimeout(10, TimeUnit.SECONDS).initialMaxData(4L << 20)
          .initialMaxStreamDataBidirectionalLocal(2L << 20).initialMaxStreamDataBidirectionalRemote(2L << 20)
          .initialMaxStreamsBidirectional(1).initialMaxStreamsUnidirectional(0)
          .tokenHandler(new AddressValidationTokenHandler()).handler(new ChannelInboundHandlerAdapter() {
            @Override public boolean isSharable() { return true; }
          }).streamHandler(new ChannelInitializer<QuicStreamChannel>() {
            @Override protected void initChannel(QuicStreamChannel channel) {
              channel.pipeline().addLast(new LengthFieldBasedFrameDecoder(Wire.MAX_CONTROL_FRAME + 5, 1, 4, 0, 0))
                  .addLast(new SimpleChannelInboundHandler<ByteBuf>() {
                    @Override protected void channelRead0(ChannelHandlerContext context, ByteBuf bytes) throws Exception {
                      byte[] encoded = new byte[bytes.readableBytes()]; bytes.readBytes(encoded);
                      var frame = Wire.decodeControl(encoded);
                      if (frame.type() == Wire.FRAME_STATUS) return;
                      for (byte[] response : responder.respond(frame)) context.writeAndFlush(Unpooled.wrappedBuffer(response));
                    }
                    @Override public void exceptionCaught(ChannelHandlerContext context, Throwable failure) { context.channel().parent().close(); }
                  });
            }
          }).build()).bind(new InetSocketAddress("127.0.0.1", 0)).sync().channel();
      } catch (Exception | AssertionError failure) { close(); throw failure; }
    }
    InetSocketAddress address() { return (InetSocketAddress) datagram.localAddress(); }
    @Override public void close() {
      if (datagram != null) datagram.close().syncUninterruptibly();
      group.shutdownGracefully(0, 1, TimeUnit.SECONDS).syncUninterruptibly();
    }
  }
}
