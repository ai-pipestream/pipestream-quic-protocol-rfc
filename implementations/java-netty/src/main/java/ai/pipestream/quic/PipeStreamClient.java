package ai.pipestream.quic;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.incubator.codec.quic.QuicChannel;
import io.netty.incubator.codec.quic.QuicClientCodecBuilder;
import io.netty.incubator.codec.quic.QuicSslContext;
import io.netty.incubator.codec.quic.QuicSslContextBuilder;
import io.netty.incubator.codec.quic.QuicStreamChannel;
import io.netty.incubator.codec.quic.QuicStreamType;
import io.netty.util.NetUtil;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/** Standalone Netty client for the PipeStream Layer 0 reference contract. */
public final class PipeStreamClient {
  private PipeStreamClient() {}

  /**
   * Sends one entity and waits for terminal status and GOAWAY acknowledgement.
   *
   * @param remote server UDP address
   * @param caCertificate trusted CA PEM
   * @param serverName certificate DNS name
   * @param entityId Layer 0 entity identifier
   * @param input payload path
   * @param contentType MIME type
   * @throws Exception on transport or protocol failure
   */
  public static void send(
      InetSocketAddress remote,
      Path caCertificate,
      String serverName,
      long entityId,
      Path input,
      String contentType)
      throws Exception {
    send(remote, caCertificate, serverName, entityId, input, contentType, null);
  }

  /** Sends one child entity and waits for terminal status and graceful completion. */
  public static void send(
      InetSocketAddress remote,
      Path caCertificate,
      String serverName,
      long entityId,
      Path input,
      String contentType,
      Long parentId)
      throws Exception {
    byte[] payload = Files.readAllBytes(input);
    QuicSslContext tls = QuicSslContextBuilder.forClient()
        .trustManager(caCertificate.toFile())
        .applicationProtocols(Wire.ALPN)
        .build();
    NioEventLoopGroup group = new NioEventLoopGroup(1);
    Channel datagram = null;
    QuicChannel connection = null;
    try {
      datagram = new Bootstrap()
          .group(group)
          .channel(NioDatagramChannel.class)
          .handler(new QuicClientCodecBuilder()
              .sslEngineProvider(channel -> tls.newEngine(channel.alloc(), serverName, remote.getPort()))
              .maxIdleTimeout(30_000, TimeUnit.MILLISECONDS)
              .initialMaxData(128L << 20)
              .initialMaxStreamDataBidirectionalLocal(1L << 20)
              .initialMaxStreamDataBidirectionalRemote(1L << 20)
              .initialMaxStreamDataUnidirectional((long) Wire.MAX_PAYLOAD + Wire.MAX_ENTITY_HEADER + 4)
              .initialMaxStreamsBidirectional(1)
              .initialMaxStreamsUnidirectional(128)
              .build())
          .bind(new InetSocketAddress(NetUtil.LOCALHOST4, 0))
          .sync()
          .channel();
      ClientControlHandler handler = new ClientControlHandler(entityId);
      connection = QuicChannel.newBootstrap(datagram)
          .handler(new ChannelInboundHandlerAdapter())
          .streamHandler(new ChannelInboundHandlerAdapter() {
            @Override
            public void channelActive(ChannelHandlerContext context) {
              context.close();
            }
          })
          .remoteAddress(remote)
          .connect()
          .get(10, TimeUnit.SECONDS);
      QuicStreamChannel control = connection.createStream(
          QuicStreamType.BIDIRECTIONAL,
          new io.netty.channel.ChannelInitializer<QuicStreamChannel>() {
            @Override
            protected void initChannel(QuicStreamChannel channel) {
              channel.pipeline()
                  .addLast(new LengthFieldBasedFrameDecoder(
                      Wire.MAX_CONTROL_FRAME + 5, 1, 4, 0, 0))
                  .addLast(handler);
            }
          }).get(10, TimeUnit.SECONDS);
      if (control.streamId() != 0) {
        throw Wire.frame("first bidirectional stream is not stream 0");
      }
      control.writeAndFlush(Unpooled.wrappedBuffer(
          Wire.encodeCapabilities(Wire.Capabilities.defaults()))).sync();
      handler.capabilities.get(10, TimeUnit.SECONDS);
      control.writeAndFlush(Unpooled.wrappedBuffer(Wire.encodeStatus(
          new Wire.Status(Wire.STATUS_UNSPECIFIED, Wire.CONNECTION_LEVEL, 0, null, 0)))).sync();
      control.writeAndFlush(Unpooled.wrappedBuffer(Wire.encodeStatus(
          new Wire.Status(Wire.STATUS_PENDING, entityId, 0, null, 0)))).sync();
      QuicStreamChannel entityStream = connection.createStream(
          QuicStreamType.UNIDIRECTIONAL, new ChannelInboundHandlerAdapter()).get(10, TimeUnit.SECONDS);
      entityStream.writeAndFlush(Unpooled.wrappedBuffer(
          Wire.encodeEntity(entityId, parentId, payload, contentType)))
          .addListener(QuicStreamChannel.SHUTDOWN_OUTPUT)
          .sync();
      handler.complete.get(30, TimeUnit.SECONDS);
      long nextEntityId = Wire.nextEntityId(entityId);
      control.writeAndFlush(Unpooled.wrappedBuffer(Wire.encodeCheckpoint(
          new Wire.Checkpoint(
              "entity-" + Long.toUnsignedString(entityId),
              1,
              nextEntityId,
              null,
              0,
              null)))).sync();
      handler.checkpoint.get(10, TimeUnit.SECONDS);
      control.writeAndFlush(Unpooled.wrappedBuffer(Wire.encodeStatus(
          new Wire.Status(
              Wire.STATUS_UNSPECIFIED, Wire.CONNECTION_LEVEL, 0, nextEntityId, 0)))).sync();
      control.writeAndFlush(Unpooled.wrappedBuffer(Wire.encodeGoaway(entityId))).sync();
      handler.goaway.get(10, TimeUnit.SECONDS);
      control.writeAndFlush(Unpooled.EMPTY_BUFFER).addListener(QuicStreamChannel.SHUTDOWN_OUTPUT).sync();
      connection.close(true, 0, Unpooled.copiedBuffer("complete", StandardCharsets.UTF_8)).sync();
      System.out.printf("SENT %s %d%n", Long.toUnsignedString(entityId), payload.length);
    } finally {
      if (connection != null && connection.isOpen()) {
        connection.close().sync();
      }
      if (datagram != null) {
        datagram.close().sync();
      }
      group.shutdownGracefully().sync();
    }
  }

  private static final class ClientControlHandler extends SimpleChannelInboundHandler<ByteBuf> {
    private final long entityId;
    private final CompletableFuture<Void> capabilities = new CompletableFuture<>();
    private final CompletableFuture<Void> complete = new CompletableFuture<>();
    private final CompletableFuture<Void> checkpoint = new CompletableFuture<>();
    private final CompletableFuture<Void> goaway = new CompletableFuture<>();
    private boolean processing;

    private ClientControlHandler(long entityId) {
      this.entityId = entityId;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext context, ByteBuf input) {
      byte[] bytes = new byte[input.readableBytes()];
      input.readBytes(bytes);
      try {
        Wire.ControlFrame frame = Wire.decodeControl(bytes);
        if (!capabilities.isDone()) {
          if (frame.type() != Wire.FRAME_CAPABILITIES) {
            throw Wire.frame("server did not answer capabilities");
          }
          Wire.decodeCapabilities(frame.payload());
          capabilities.complete(null);
          return;
        }
        if (frame.type() == Wire.FRAME_STATUS) {
          Wire.Status status = Wire.decodeStatus(frame.payload());
          if (status.state() == Wire.STATUS_UNSPECIFIED
              && status.entityId() == Wire.CONNECTION_LEVEL
              && status.cursor() == null) {
            return;
          }
          if (status.entityId() != entityId) {
            throw Wire.entity("status references another entity");
          }
          if (!processing && status.state() == Wire.STATUS_PROCESSING) {
            processing = true;
            return;
          }
          if (processing && status.state() == Wire.STATUS_COMPLETE) {
            complete.complete(null);
            return;
          }
          throw Wire.entity("unexpected status progression");
        }
        if (frame.type() == Wire.FRAME_GOAWAY) {
          if (Wire.decodeGoaway(frame.payload()) != entityId) {
            throw Wire.frame("GOAWAY acknowledgement changed last entity ID");
          }
          goaway.complete(null);
          return;
        }
        if (frame.type() == Wire.FRAME_CHECKPOINT) {
          Wire.Checkpoint observed = Wire.decodeCheckpoint(frame.payload());
          if (!complete.isDone()
              || observed.flags() != Wire.CHECKPOINT_ACK
              || observed.checkpointEntityId() != Wire.nextEntityId(entityId)) {
            throw Wire.entity("invalid checkpoint acknowledgement");
          }
          checkpoint.complete(null);
          return;
        }
        throw Wire.frame("unexpected control frame type " + frame.type());
      } catch (Exception failure) {
        fail(failure);
        ((QuicChannel) context.channel().parent()).close(
            true,
            failure instanceof ProtocolException protocol
                ? Math.toIntExact(protocol.errorCode()) : Math.toIntExact(Wire.ERROR_FRAME),
            Unpooled.copiedBuffer(failure.getMessage(), StandardCharsets.UTF_8));
      }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext context, Throwable cause) {
      fail(cause);
      context.close();
    }

    private void fail(Throwable failure) {
      capabilities.completeExceptionally(failure);
      complete.completeExceptionally(failure);
      checkpoint.completeExceptionally(failure);
      goaway.completeExceptionally(failure);
    }
  }
}
