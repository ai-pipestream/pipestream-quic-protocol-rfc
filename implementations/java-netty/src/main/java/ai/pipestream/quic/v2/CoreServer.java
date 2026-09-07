package ai.pipestream.quic.v2;

import static ai.pipestream.quic.v2.Messages.*;
import static ai.pipestream.quic.v2.ProtocolError.Code.*;

import ai.pipestream.quic.AddressValidationTokenHandler;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.FixedRecvByteBufAllocator;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.ChannelInputShutdownEvent;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.incubator.codec.quic.QuicChannel;
import io.netty.incubator.codec.quic.QuicServerCodecBuilder;
import io.netty.incubator.codec.quic.QuicStreamChannel;
import io.netty.incubator.codec.quic.QuicStreamType;
import io.netty.util.AttributeKey;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Real V2 Core-only QUIC listener. It implements bounded negotiation, refusals and detach; it
 * neither advertises nor simulates durable work/results. No data stream can consume its control
 * receive credit. Durable integration must add a proven shared data/control credit budget.
 */
public final class CoreServer implements AutoCloseable {
  private static final AttributeKey<Connection> CONNECTION =
      AttributeKey.valueOf(CoreServer.class, "connection");
  private final TlsAuthentication authentication;
  private final CoreOptions options;
  private final NioEventLoopGroup group;
  private final Set<Connection> connections = new HashSet<>();
  private final Map<Optional<String>, Integer> owners = new HashMap<>();
  private final AtomicBoolean stopping = new AtomicBoolean();
  private Channel listener;
  private long refusedAdmissions;
  private int peakConnections;
  private int peakTransports;
  private QuicChannel rejected;
  private int peakControlBytes;
  private int peakControlFrames;

  private CoreServer(TlsAuthentication authentication, CoreOptions options) {
    this.authentication = Objects.requireNonNull(authentication);
    this.options = Objects.requireNonNull(options);
    if (!authentication.isServer())
      throw new IllegalArgumentException("server TLS configuration required");
    group = new NioEventLoopGroup(1);
  }

  /**
   * Bind an actual listener. The caller owns its lifetime and must close it.
   *
   * @param address explicit local UDP bind address
   * @param authentication server trust/key/principal configuration
   * @param options explicit bounded Core policy
   * @return running listener
   * @throws InterruptedException if binding is interrupted
   */
  public static CoreServer start(
      InetSocketAddress address, TlsAuthentication authentication, CoreOptions options)
      throws InterruptedException {
    var server = new CoreServer(authentication, options);
    boolean success = false;
    try {
      var codec =
          new QuicServerCodecBuilder()
              .version(1)
              .sslEngineProvider(c -> authentication.engine(c.alloc(), 0))
              .maxIdleTimeout(
                  Math.max(options.controlTimeoutMs(), options.streamLifetimeMs()),
                  TimeUnit.MILLISECONDS)
              .initialMaxData(options.controlWindowBytes())
              .initialMaxStreamDataBidirectionalLocal(0)
              .initialMaxStreamDataBidirectionalRemote(options.controlWindowBytes())
              .initialMaxStreamDataUnidirectional(0)
              .initialMaxStreamsBidirectional(1)
              .initialMaxStreamsUnidirectional(0)
              .tokenHandler(new AddressValidationTokenHandler())
              .handler(
                  new ChannelInitializer<QuicChannel>() {
                    @Override
                    protected void initChannel(QuicChannel channel) {
                      server.accept(channel);
                    }
                  })
              .streamHandler(
                  new ChannelInitializer<QuicStreamChannel>() {
                    @Override
                    protected void initChannel(QuicStreamChannel channel) {
                      Connection connection = channel.parent().attr(CONNECTION).get();
                      if (connection == null)
                        channel
                            .parent()
                            .close(
                                true,
                                (int) LIMIT_EXCEEDED.applicationError(),
                                Unpooled.EMPTY_BUFFER);
                      else connection.stream(channel);
                    }
                  })
              .build();
      server.listener =
          new Bootstrap()
              .group(server.group)
              .channel(NioDatagramChannel.class)
              .handler(
                  new ChannelInitializer<Channel>() {
                    @Override
                    protected void initChannel(Channel channel) {
                      channel
                          .pipeline()
                          .addLast(
                              new ChannelInboundHandlerAdapter() {
                                @Override
                                public void channelRead(ChannelHandlerContext ctx, Object packet) {
                                  try {
                                    ctx.fireChannelRead(packet);
                                  } finally {
                                    server.finishAdmission();
                                  }
                                }
                              },
                              codec);
                    }
                  })
              .bind(address)
              .sync()
              .channel();
      success = true;
      return server;
    } finally {
      if (!success) server.close();
    }
  }

  /**
   * Get the actual bound socket address.
   *
   * @return resolved UDP address, including an allocated ephemeral port
   */
  public InetSocketAddress address() {
    return (InetSocketAddress) listener.localAddress();
  }

  /**
   * Listener-owned counts, not durable work statistics or total process-memory measurements.
   *
   * @param active active admitted connections, including handshakes
   * @param owners active mapped-owner/anonymous buckets
   * @param refused global/per-owner admission attempts refused, including repeated Initial packets;
   *     not a count of distinct remote peers
   * @param peakConnections high-water global admitted connections
   * @param peakTransports high-water admitted plus the one packet-local refusal transport
   * @param peakControlBytes high-water per-connection bytes awaiting Netty write completion
   * @param peakControlFrames high-water per-connection frames awaiting Netty write completion
   */
  public record Snapshot(
      int active,
      int owners,
      long refused,
      int peakConnections,
      int peakTransports,
      int peakControlBytes,
      int peakControlFrames) {}

  /**
   * Read an event-loop-consistent snapshot. Callers should bound their own polling.
   *
   * @return counts, or failed completion after shutdown
   */
  public CompletionStage<Snapshot> snapshot() {
    var result = new CompletableFuture<Snapshot>();
    if (stopping.get()) {
      result.completeExceptionally(new IllegalStateException("listener closed"));
    } else {
      try {
        group
            .next()
            .execute(
                () ->
                    result.complete(
                        new Snapshot(
                            connections.size(),
                            owners.size(),
                            refusedAdmissions,
                            peakConnections,
                            peakTransports,
                            peakControlBytes,
                            peakControlFrames)));
      } catch (RuntimeException closed) {
        result.completeExceptionally(closed);
      }
    }
    return result.minimalCompletionStage();
  }

  private void accept(QuicChannel channel) {
    peakTransports = Math.max(peakTransports, connections.size() + 1);
    if (stopping.get() || connections.size() >= options.connections()) {
      refusedAdmissions++;
      // The pinned codec constructs one connection per received UDP datagram before recv().
      // Close after that recv, when Initial keys exist, not from its channel initializer.
      // The enclosing datagram handler clears this slot synchronously, without a handshake queue.
      if (rejected != null) throw new IllegalStateException("nested QUIC packet admission");
      rejected = channel;
      return;
    }
    Connection connection = new Connection(channel);
    channel.attr(CONNECTION).set(connection);
    connections.add(connection);
    peakConnections = Math.max(peakConnections, connections.size());
    channel.closeFuture().addListener(ignored -> connection.end());
    channel.pipeline().addLast(connection.guard, connection);
    long interval =
        Math.max(
            1,
            Math.min(100, Math.min(options.handshakeTimeoutMs(), options.controlTimeoutMs()) / 4));
    connection.timer =
        channel
            .eventLoop()
            .scheduleAtFixedRate(connection::check, interval, interval, TimeUnit.MILLISECONDS);
  }

  private void finishAdmission() {
    QuicChannel channel = rejected;
    rejected = null;
    if (channel != null) channel.close(false, 0x02, Unpooled.EMPTY_BUFFER);
  }

  private void observe(ControlWrites writes) {
    peakControlBytes = Math.max(peakControlBytes, writes.peakBytes());
    peakControlFrames = Math.max(peakControlFrames, writes.peakFrames());
  }

  /** Close only this listener and its owned connections. No durable work outcome is asserted. */
  @Override
  public void close() {
    if (!stopping.compareAndSet(false, true)) return;
    if (group.next().inEventLoop()) {
      stopChannels();
      group.shutdownGracefully(0, 2, TimeUnit.SECONDS);
    } else {
      group.next().submit(this::stopChannels).awaitUninterruptibly();
      group.shutdownGracefully(0, 2, TimeUnit.SECONDS).awaitUninterruptibly();
    }
  }

  private void stopChannels() {
    for (Connection connection : Set.copyOf(connections)) connection.channel.close();
    if (listener != null) listener.close();
  }

  private final class Connection extends ChannelInboundHandlerAdapter {
    final QuicChannel channel;
    final TlsAuthentication.Guard guard = authentication.guard();
    final long started = System.nanoTime();
    long lastFrame = started;
    long detachStart;
    boolean authenticated;
    boolean detached;
    boolean closing;
    boolean ended;
    boolean ownerCounted;
    Optional<String> owner;
    Capabilities selected;
    Control control;
    io.netty.util.concurrent.ScheduledFuture<?> timer;

    Connection(QuicChannel channel) {
      this.channel = channel;
    }

    @Override
    public void channelActive(ChannelHandlerContext context) {
      try {
        ObjectStream.before(System.nanoTime(), started, options.handshakeTimeoutMs() * 1000000L);
        guard.requireAuthenticated();
        authenticated = true;
        try {
          owner = Optional.of(guard.requireOwner());
        } catch (ProtocolError absent) {
          if (absent.code() != UNAUTHORIZED) throw absent;
          owner = Optional.empty();
        }
        int count = owners.getOrDefault(owner, 0);
        if (count >= options.connectionsPerOwner()) {
          refusedAdmissions++;
          fail(ProtocolError.limit("owner connection ceiling"));
          return;
        }
        owners.put(owner, count + 1);
        ownerCounted = true;
        lastFrame = System.nanoTime();
        context.fireChannelActive();
      } catch (ProtocolError failure) {
        fail(failure);
      }
    }

    void stream(QuicStreamChannel stream) {
      if (closing || ended) {
        stream.close();
        return;
      }
      try {
        check();
        if (closing || ended) return;
        guard.requireAuthenticated();
        if (!authenticated
            || stream.type() != QuicStreamType.BIDIRECTIONAL
            || stream.streamId() != 0
            || control != null)
          throw ProtocolError.frame("only client bidirectional stream zero is control");
        stream
            .config()
            .setAllowHalfClosure(true)
            .setRecvByteBufAllocator(new FixedRecvByteBufAllocator(options.readChunkBytes()));
        control = new Control(stream);
        stream.pipeline().addLast(control);
      } catch (ProtocolError failure) {
        fail(failure);
      }
    }

    void check() {
      if (closing || ended) return;
      try {
        long now = System.nanoTime();
        if (!authenticated)
          ObjectStream.before(now, started, options.handshakeTimeoutMs() * 1000000L);
        else {
          guard.requireAuthenticated();
          ObjectStream.before(now, lastFrame, options.controlTimeoutMs() * 1000000L);
          if (detached)
            ObjectStream.before(now, detachStart, selected.streamLifetimeMs() * 1000000L);
          if (control != null) control.writes.check(now);
        }
      } catch (ProtocolError failure) {
        fail(failure);
      }
    }

    void fail(ProtocolError failure) {
      if (closing || ended) return;
      closing = true;
      stopActivity();
      // Release admission only after native close has finished, via closeFuture/end().
      channel.close(
          authenticated,
          authenticated ? (int) failure.code().applicationError() : 0x02,
          Unpooled.EMPTY_BUFFER);
    }

    void stopActivity() {
      if (timer != null) timer.cancel(false);
      if (control != null) {
        observe(control.writes);
        control.writes.end();
      }
    }

    void end() {
      if (ended) return;
      ended = true;
      stopActivity();
      connections.remove(this);
      if (ownerCounted) {
        owners.compute(owner, (key, count) -> count == null || count == 1 ? null : count - 1);
        ownerCounted = false;
      }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
      end();
      ctx.fireChannelInactive();
    }

    @Override
    public void channelUnregistered(ChannelHandlerContext ctx) {
      end();
      ctx.fireChannelUnregistered();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
      if (cause instanceof javax.net.ssl.SSLException)
        return; // Native QUIC must flush its TLS alert.
      fail(new ProtocolError(INTERNAL_ERROR, "connection transport failed"));
    }

    private final class Control extends SimpleChannelInboundHandler<ByteBuf> {
      final QuicStreamChannel stream;
      final Wire.Decoder decoder = new Wire.Decoder(4096);
      final ControlWrites writes;
      long highest;
      boolean inputFin;
      boolean outputFin;

      Control(QuicStreamChannel stream) {
        this.stream = stream;
        writes = new ControlWrites(stream, options, Connection.this::fail, this::finishOutput);
      }

      @Override
      protected void channelRead0(ChannelHandlerContext ctx, ByteBuf bytes) {
        if (closing || ended) return;
        try {
          check();
          if (closing || ended) return;
          ByteBuffer source = bytes.nioBuffer();
          while (source.hasRemaining() && !closing && !ended) {
            Wire.Frame frame = decoder.feed(source);
            if (frame == null) break;
            receive(frame);
            lastFrame = System.nanoTime();
          }
        } catch (ProtocolError failure) {
          fail(failure);
        }
      }

      void receive(Wire.Frame frame) {
        check();
        if (closing || ended) return;
        guard.requireAuthenticated();
        if (selected == null) {
          if (!(frame instanceof Wire.Known known)
              || !(known.message() instanceof Capabilities offer)
              || offer.response())
            throw ProtocolError.frame("first control must be a client capability offer");
          selected = guard.negotiate(offer, options.offer());
          decoder.limit(selected.controlLimit());
          writes.selected(selected);
          writes.send(selected);
        } else if (frame instanceof Wire.Known known) {
          Message message = known.message();
          long request = ClientCorrelation.requestId(message);
          if (highest == 0 && request != 1 || request <= highest)
            throw ProtocolError.frame("control request does not increase from one");
          highest = request;
          Message response;
          if (detached)
            response =
                new Refusal(
                    new Records.RequestTag(false, request), NOT_READY, "connection detached");
          else if (message instanceof Detach) {
            detached = true;
            detachStart = System.nanoTime();
            response = new Detached(request);
          } else
            response =
                new Refusal(
                    new Records.RequestTag(false, request),
                    EXTENSION_UNSUPPORTED,
                    "profile not activated");
          writes.send(response);
        }
        observe(writes);
      }

      @Override
      public void userEventTriggered(ChannelHandlerContext ctx, Object event) {
        if (event instanceof ChannelInputShutdownEvent) {
          if (closing || ended) return;
          try {
            check();
            if (closing || ended) return;
            decoder.finish();
            if (!detached) throw ProtocolError.frame("control FIN before detach");
            inputFin = true;
            finishOutput();
          } catch (ProtocolError failure) {
            fail(failure);
          }
        } else ctx.fireUserEventTriggered(event);
      }

      void finishOutput() {
        if (!closing && !ended && inputFin && !outputFin && writes.empty()) {
          outputFin = true;
          stream
              .shutdownOutput()
              .addListener(
                  result -> {
                    if (!result.isSuccess())
                      fail(new ProtocolError(CONTROL_RESET, "control FIN failed"));
                  });
          // Do not close the connection on local write/FIN completion: that is not a peer ACK.
          // The client owns graceful connection close; the absolute detach timer stays armed.
        }
      }

      @Override
      public void channelInactive(ChannelHandlerContext ctx) {
        if (!closing && !ended && !(inputFin && outputFin))
          fail(new ProtocolError(CONTROL_RESET, "control stream lost"));
        ctx.fireChannelInactive();
      }

      @Override
      public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        fail(new ProtocolError(CONTROL_RESET, "control stream reset or stopped"));
      }
    }
  }
}
