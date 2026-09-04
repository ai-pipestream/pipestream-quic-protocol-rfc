package ai.pipestream.quic;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.ChannelInputShutdownReadComplete;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.incubator.codec.quic.QuicChannel;
import io.netty.incubator.codec.quic.QuicServerCodecBuilder;
import io.netty.incubator.codec.quic.QuicSslContext;
import io.netty.incubator.codec.quic.QuicSslContextBuilder;
import io.netty.incubator.codec.quic.QuicStreamChannel;
import io.netty.incubator.codec.quic.QuicStreamType;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/** Standalone Netty server for the PipeStream Layer 0 reference contract. */
public final class PipeStreamServer implements AutoCloseable {
  private final NioEventLoopGroup group;
  private final Channel datagram;
  private final Path outputDirectory;
  private final ConcurrentHashMap<QuicChannel, Session> sessions = new ConcurrentHashMap<>();
  private final CompletableFuture<Wire.Entity> firstEntity = new CompletableFuture<>();
  private final CompletableFuture<Void> firstSessionComplete = new CompletableFuture<>();

  private PipeStreamServer(
      NioEventLoopGroup group, Channel datagram, Path outputDirectory) {
    this.group = group;
    this.datagram = datagram;
    this.outputDirectory = outputDirectory;
  }

  /**
   * Starts a server.
   *
   * @param bind UDP address
   * @param certificate PEM certificate chain
   * @param privateKey PEM private key
   * @param outputDirectory received payload directory
   * @return running server
   * @throws Exception on TLS or bind failure
   */
  public static PipeStreamServer start(
      InetSocketAddress bind, Path certificate, Path privateKey, Path outputDirectory)
      throws Exception {
    Files.createDirectories(outputDirectory);
    QuicSslContext tls = QuicSslContextBuilder
        .forServer(privateKey.toFile(), null, certificate.toFile())
        .applicationProtocols(Wire.ALPN)
        .build();
    NioEventLoopGroup group = new NioEventLoopGroup(1);
    PipeStreamServer[] holder = new PipeStreamServer[1];
    ChannelHandler codec = new QuicServerCodecBuilder()
        .sslContext(tls)
        .maxIdleTimeout(30_000, TimeUnit.MILLISECONDS)
        .initialMaxData(128L << 20)
        .initialMaxStreamDataBidirectionalLocal(1L << 20)
        .initialMaxStreamDataBidirectionalRemote(1L << 20)
        .initialMaxStreamDataUnidirectional((long) Wire.MAX_PAYLOAD + Wire.MAX_ENTITY_HEADER + 4)
        .initialMaxStreamsBidirectional(1)
        .initialMaxStreamsUnidirectional(128)
        .tokenHandler(new AddressValidationTokenHandler())
        .handler(new ChannelInboundHandlerAdapter() {
          @Override
          public void channelActive(ChannelHandlerContext context) {
            PipeStreamServer server = Objects.requireNonNull(holder[0]);
            server.sessions.put((QuicChannel) context.channel(), new Session((QuicChannel) context.channel(), server));
          }

          @Override
          public void channelInactive(ChannelHandlerContext context) {
            PipeStreamServer server = Objects.requireNonNull(holder[0]);
            server.sessions.remove((QuicChannel) context.channel());
          }

          @Override
          public boolean isSharable() {
            return true;
          }
        })
        .streamHandler(new ChannelInitializer<QuicStreamChannel>() {
          @Override
          protected void initChannel(QuicStreamChannel channel) {
            PipeStreamServer server = Objects.requireNonNull(holder[0]);
            Session session = server.sessions.computeIfAbsent(
                channel.parent(), parent -> new Session(parent, server));
            if (channel.type() == QuicStreamType.BIDIRECTIONAL) {
              if (channel.streamId() != 0) {
                session.fail(Wire.frame("bidirectional stream other than stream 0"));
                channel.close();
                return;
              }
              session.control = channel;
              channel.pipeline()
                  .addLast(new LengthFieldBasedFrameDecoder(
                      Wire.MAX_CONTROL_FRAME + 5, 1, 4, 0, 0))
                  .addLast(new ServerControlHandler(session));
            } else {
              channel.pipeline().addLast(new ServerEntityHandler(session));
            }
          }
        })
        .build();
    Channel datagram;
    try {
      datagram = new Bootstrap()
          .group(group)
          .channel(NioDatagramChannel.class)
          .handler(codec)
          .bind(bind)
          .sync()
          .channel();
    } catch (Exception exception) {
      group.shutdownGracefully().sync();
      throw exception;
    }
    PipeStreamServer server = new PipeStreamServer(group, datagram, outputDirectory);
    holder[0] = server;
    return server;
  }

  /** @return bound UDP address */
  public InetSocketAddress address() {
    return (InetSocketAddress) datagram.localAddress();
  }

  /**
   * Waits for the first valid entity.
   *
   * @param timeout timeout value
   * @param unit timeout unit
   * @return received entity
   * @throws Exception on timeout or protocol failure
   */
  public Wire.Entity awaitFirstEntity(long timeout, TimeUnit unit) throws Exception {
    return firstEntity.get(timeout, unit);
  }

  /**
   * Waits until the first client has completed its cursor and GOAWAY exchange.
   *
   * @param timeout timeout value
   * @param unit timeout unit
   * @throws Exception on timeout or protocol failure
   */
  public void awaitFirstSession(long timeout, TimeUnit unit) throws Exception {
    firstSessionComplete.get(timeout, unit);
  }

  @Override
  public void close() {
    datagram.close().syncUninterruptibly();
    group.shutdownGracefully().syncUninterruptibly();
  }

  private static final class Session {
    private final QuicChannel connection;
    private final PipeStreamServer server;
    private final CompletableFuture<Long> pendingEntity = new CompletableFuture<>();
    private volatile QuicStreamChannel control;
    private volatile boolean entityComplete;

    private Session(QuicChannel connection, PipeStreamServer server) {
      this.connection = connection;
      this.server = server;
    }

    private void fail(Throwable failure) {
      server.firstEntity.completeExceptionally(failure);
      long code = failure instanceof ProtocolException protocol ? protocol.errorCode() : Wire.ERROR_FRAME;
      connection.close(
          true,
          Math.toIntExact(code),
          Unpooled.copiedBuffer(failure.getMessage(), StandardCharsets.UTF_8));
    }

    private void writeControl(byte[] frame) {
      QuicStreamChannel channel = control;
      if (channel == null) {
        fail(Wire.frame("control stream is not initialized"));
        return;
      }
      channel.writeAndFlush(Unpooled.wrappedBuffer(frame));
    }
  }

  private static final class ServerControlHandler extends SimpleChannelInboundHandler<ByteBuf> {
    private final Session session;
    private boolean capabilitiesComplete;
    private boolean cursorReceived;

    private ServerControlHandler(Session session) {
      this.session = session;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext context, ByteBuf input) {
      byte[] bytes = new byte[input.readableBytes()];
      input.readBytes(bytes);
      try {
        Wire.ControlFrame frame = Wire.decodeControl(bytes);
        if (!capabilitiesComplete) {
          if (frame.type() != Wire.FRAME_CAPABILITIES) {
            throw Wire.frame("first frame must be CAPABILITIES");
          }
          Wire.Capabilities peer = Wire.decodeCapabilities(frame.payload());
          Wire.Capabilities negotiated = Wire.Capabilities.defaults().negotiate(peer);
          session.writeControl(Wire.encodeCapabilities(negotiated));
          session.writeControl(Wire.encodeStatus(new Wire.Status(
              Wire.STATUS_UNSPECIFIED, Wire.CONNECTION_LEVEL, 0, null, 0)));
          capabilitiesComplete = true;
          return;
        }
        if (frame.type() == Wire.FRAME_STATUS) {
          Wire.Status status = Wire.decodeStatus(frame.payload());
          if (status.state() == Wire.STATUS_UNSPECIFIED
              && status.entityId() == Wire.CONNECTION_LEVEL
              && status.cursor() == null) {
            return;
          }
          if (!session.pendingEntity.isDone()) {
            if (status.state() != Wire.STATUS_PENDING) {
              throw Wire.entity("first entity status must be PENDING");
            }
            session.pendingEntity.complete(status.entityId());
          } else {
            if (status.state() != Wire.STATUS_UNSPECIFIED
                || status.entityId() != Wire.CONNECTION_LEVEL
                || status.cursor() == null) {
              throw Wire.entity("invalid connection-level cursor update");
            }
            cursorReceived = true;
          }
          return;
        }
        if (frame.type() == Wire.FRAME_CHECKPOINT) {
          Wire.Checkpoint checkpoint = Wire.decodeCheckpoint(frame.payload());
          if (!session.entityComplete
              || checkpoint.flags() != 0
              || checkpoint.checkpointEntityId()
                  != Wire.nextEntityId(session.pendingEntity.join())) {
            throw Wire.entity("checkpoint barrier is not satisfied");
          }
          session.writeControl(Wire.encodeCheckpoint(new Wire.Checkpoint(
              checkpoint.checkpointId(),
              checkpoint.sequenceNumber(),
              checkpoint.checkpointEntityId(),
              checkpoint.scopeId(),
              Wire.CHECKPOINT_ACK,
              checkpoint.timeoutMs())));
          return;
        }
        if (frame.type() == Wire.FRAME_GOAWAY) {
          if (!cursorReceived) {
            throw Wire.frame("GOAWAY received before cursor update");
          }
          long lastEntity = Wire.decodeGoaway(frame.payload());
          context.writeAndFlush(Unpooled.wrappedBuffer(Wire.encodeGoaway(lastEntity)))
              .addListener(write -> {
                if (write.isSuccess()) {
                  session.server.firstSessionComplete.complete(null);
                } else {
                  session.server.firstSessionComplete.completeExceptionally(write.cause());
                }
              });
          context.writeAndFlush(Unpooled.EMPTY_BUFFER)
              .addListener(QuicStreamChannel.SHUTDOWN_OUTPUT);
          return;
        }
        throw Wire.frame("unexpected control frame type " + frame.type());
      } catch (Exception failure) {
        session.fail(failure);
      }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext context, Throwable cause) {
      session.fail(cause);
      context.close();
    }
  }

  private static final class ServerEntityHandler extends ChannelInboundHandlerAdapter {
    private final Session session;
    private final ByteArrayOutputStream input = new ByteArrayOutputStream();
    private boolean processed;

    private ServerEntityHandler(Session session) {
      this.session = session;
    }

    @Override
    public void channelRead(ChannelHandlerContext context, Object message) {
      ByteBuf bytes = (ByteBuf) message;
      try {
        if ((long) input.size() + bytes.readableBytes()
            > (long) Wire.MAX_PAYLOAD + Wire.MAX_ENTITY_HEADER + 4) {
          session.fail(Wire.limit("entity stream exceeds local limit"));
          context.close();
          return;
        }
        byte[] chunk = new byte[bytes.readableBytes()];
        bytes.readBytes(chunk);
        input.writeBytes(chunk);
      } finally {
        bytes.release();
      }
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext context, Object event) {
      if (event == ChannelInputShutdownReadComplete.INSTANCE) {
        process(context);
      } else {
        context.fireUserEventTriggered(event);
      }
    }

    @Override
    public void channelInactive(ChannelHandlerContext context) {
      process(context);
    }

    private void process(ChannelHandlerContext context) {
      if (processed) {
        return;
      }
      processed = true;
      try {
        Wire.Entity entity = Wire.decodeEntity(input.toByteArray());
        session.pendingEntity.whenCompleteAsync((pending, failure) -> {
          if (failure != null) {
            session.fail(failure);
            return;
          }
          if (pending != entity.header().entityId()) {
            session.fail(Wire.entity("PENDING and EntityHeader IDs differ"));
            return;
          }
          try {
            session.writeControl(Wire.encodeStatus(new Wire.Status(
                Wire.STATUS_PROCESSING, pending, 0, null, 0)));
            Files.write(
                session.server.outputDirectory.resolve(Long.toUnsignedString(pending) + ".bin"),
                entity.payload());
            if (entity.header().parentId() != null) {
              Files.writeString(
                  session.server.outputDirectory.resolve(
                      Long.toUnsignedString(pending) + ".parent"),
                  Long.toUnsignedString(entity.header().parentId()) + System.lineSeparator());
            }
            session.writeControl(Wire.encodeStatus(new Wire.Status(
                Wire.STATUS_COMPLETE, pending, 0, null, 0)));
            session.entityComplete = true;
            session.server.firstEntity.complete(entity);
            System.out.printf("RECEIVED %s %d%n", Long.toUnsignedString(pending), entity.payload().length);
          } catch (IOException ioFailure) {
            session.fail(ioFailure);
          }
        }, context.executor());
      } catch (Exception failure) {
        session.fail(failure);
      }
    }
  }
}
