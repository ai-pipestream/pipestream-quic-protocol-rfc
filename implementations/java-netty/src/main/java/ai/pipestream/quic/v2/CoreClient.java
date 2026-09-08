package ai.pipestream.quic.v2;

import static ai.pipestream.quic.v2.Messages.*;
import static ai.pipestream.quic.v2.ProtocolError.Code.*;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoop;
import io.netty.channel.FixedRecvByteBufAllocator;
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
import io.netty.handler.codec.quic.QuicStreamType;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * An authenticated V2 Core-only client with bounded negotiation and graceful detach. It owns one
 * UDP socket and event loop, and never advertises durable profiles or opens object streams.
 * Completion callbacks may run on networking or termination threads: applications must use async
 * continuations for blocking work. No transport result is evidence of durable work completion.
 */
public final class CoreClient implements AutoCloseable {
  private static final Object ADMISSION = new Object();
  private static int admitted;
  private static long reservedBytes;
  private final long bufferBudget;
  private final CoreOptions options;
  private final StreamTransport.Limits transportLimits;
  private final TlsAuthentication authentication;
  private final TlsAuthentication.Guard guard;
  private final MultiThreadIoEventLoopGroup group;
  private final EventLoop loop;
  private final ClientCorrelation correlation;
  private final CompletableFuture<Capabilities> readiness = new CompletableFuture<>();
  private final CompletableFuture<Void> detached = new CompletableFuture<>();
  private final CompletableFuture<Void> termination = new CompletableFuture<>();
  private final CompletionStage<Capabilities> readyView = readiness.minimalCompletionStage();
  private final CompletionStage<Void> detachView = detached.minimalCompletionStage();
  private final CompletionStage<Void> closedView = termination.minimalCompletionStage();
  private final AtomicBoolean detachRequested = new AtomicBoolean();
  private final AtomicBoolean closeRequested = new AtomicBoolean();
  private final long started = System.nanoTime();
  private long lastFrame = started;
  private long detachStart;
  private Channel socket;
  private QuicChannel connection;
  private StreamTransport transport;
  private Control control;
  private Capabilities selected;
  private boolean authenticated;
  private boolean detachSent;
  private boolean detachAcknowledged;
  private boolean stopping;
  private Throwable terminalFailure;
  private io.netty.util.concurrent.ScheduledFuture<?> timer;

  private CoreClient(TlsAuthentication authentication, CoreOptions options) {
    this.authentication = Objects.requireNonNull(authentication);
    this.options = Objects.requireNonNull(options);
    transportLimits = StreamTransport.Limits.core(options);
    if (authentication.isServer())
      throw new IllegalArgumentException("client TLS configuration required");
    guard = authentication.guard();
    correlation = new ClientCorrelation(options.offer());
    bufferBudget =
        (long) options.queuedControlBytes() + options.controlLimit() + options.readChunkBytes();
    synchronized (ADMISSION) {
      if (admitted >= 64 || bufferBudget > 128L * 1024 * 1024 - reservedBytes)
        throw ProtocolError.limit("Core client process admission exhausted");
      admitted++;
      reservedBytes += bufferBudget;
    }
    try {
      group = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
      loop = group.next();
    } catch (RuntimeException | Error failure) {
      release();
      throw failure;
    }
  }

  /**
   * Bind a local socket and initiate authenticated negotiation asynchronously. There is no DNS
   * lookup or insecure identity fallback here; the remote must already be resolved and the TLS
   * configuration supplies its independent reference identity. Failed setup releases owned state.
   *
   * @param remote resolved remote UDP address with a nonzero port
   * @param authentication client trust, service identity and optional caller credentials
   * @param options Core policy; connection/per-owner ceilings apply to listeners, not this client
   * @return owned client; await {@link #ready()} before assuming negotiation succeeded
   * @throws InterruptedException if interrupted while binding the local UDP socket
   */
  public static CoreClient connect(
      InetSocketAddress remote, TlsAuthentication authentication, CoreOptions options)
      throws InterruptedException {
    Objects.requireNonNull(remote);
    if (remote.isUnresolved() || remote.getPort() == 0)
      throw new IllegalArgumentException("resolved remote with nonzero port required");
    CoreClient client = new CoreClient(authentication, options);
    boolean success = false;
    try {
      var codec =
          client
              .transportLimits
              .configure(new QuicClientCodecBuilder(), false)
              .version(1)
              .sslEngineProvider(c -> authentication.engine(c.alloc(), remote.getPort()))
              .maxIdleTimeout(
                  Math.max(
                      options.handshakeTimeoutMs(),
                      Math.max(options.controlTimeoutMs(), options.streamLifetimeMs())),
                  TimeUnit.MILLISECONDS)
              .build();
      // Retain the channel before waiting so interrupted bind cannot orphan a socket.
      var binding =
          new Bootstrap()
              .group(client.group)
              .channel(NioDatagramChannel.class)
              .handler(codec)
              .bind(
                  new InetSocketAddress(
                      remote.getAddress().isLoopbackAddress() ? remote.getAddress() : null, 0));
      client.socket = binding.channel();
      binding.sync();
      client.loop.execute(() -> client.start(remote));
      success = true;
      return client;
    } finally {
      if (!success) client.close();
    }
  }

  /**
   * Observe verified server capability selection, not just a successful TLS handshake.
   *
   * @return read-only completion stage; modifying a derived future cannot alter this client
   */
  public CompletionStage<Capabilities> ready() {
    return readyView;
  }

  /**
   * Request one connection-only detach, optionally before negotiation completes. All calls share
   * that operation; cancelling a derived waiter does not cancel it or create another request.
   * Success requires its correlated acknowledgment, actual server control FIN and local send FIN
   * completion. Local write completion is not a peer ACK, and detach is not a work checkpoint.
   *
   * @return read-only drain result; refusal, premature loss or deadline fails it
   */
  public CompletionStage<Void> detach() {
    if (detachRequested.compareAndSet(false, true)) {
      try {
        loop.execute(this::sendDetach);
      } catch (RuntimeException stopped) {
        // Shutdown already owns the underlying completion; do not invent another outcome.
      }
    }
    return detachView;
  }

  /**
   * Observe owned socket/event-loop termination, separately from negotiation and draining.
   *
   * @return successful completion after graceful detach, exceptional after abort or failure
   */
  public CompletionStage<Void> closed() {
    return closedView;
  }

  private void start(InetSocketAddress remote) {
    if (stopping) return;
    long interval =
        Math.max(
            1,
            Math.min(100, Math.min(options.handshakeTimeoutMs(), options.controlTimeoutMs()) / 4));
    timer = loop.scheduleAtFixedRate(this::check, interval, interval, TimeUnit.MILLISECONDS);
    try {
      QuicChannel.newBootstrap(socket)
          .option(QuicChannelOption.STREAM_SEND_BUFFER_LIMITS, transportLimits.nativeSendLimits())
          .handler(
              new ChannelInitializer<QuicChannel>() {
                @Override
                protected void initChannel(QuicChannel channel) {
                  connection = channel;
                  transport = new StreamTransport(channel, transportLimits, false);
                  channel.pipeline().addLast(guard, new Connection());
                  channel
                      .closeFuture()
                      .addListener(
                          ignored -> {
                            if (!stopping)
                              fail(
                                  new ProtocolError(
                                      CONTROL_RESET, "connection ended before drain"));
                          });
                }
              })
          .streamHandler(
              new ChannelInitializer<QuicStreamChannel>() {
                @Override
                protected void initChannel(QuicStreamChannel stream) {
                  fail(ProtocolError.frame("Core server cannot open streams"));
                }
              })
          .remoteAddress(remote)
          .connect()
          .addListener(
              result -> {
                if (!result.isSuccess()) deferFailure(result.cause());
              });
      guard
          .ready()
          .whenComplete(
              (ignored, failure) -> {
                if (failure != null) deferFailure(failure);
              });
    } catch (RuntimeException failure) {
      fail(failure);
    }
  }

  private void deferFailure(Throwable failure) {
    // Allow the native receive/handshake callback to flush its TLS alert before cleanup.
    if (!stopping) loop.execute(() -> fail(failure));
  }

  private void check() {
    if (stopping) return;
    try {
      long now = System.nanoTime();
      if (!authenticated)
        ObjectStream.before(now, started, options.handshakeTimeoutMs() * 1000000L);
      else {
        guard.requireAuthenticated();
        ObjectStream.before(now, lastFrame, options.controlTimeoutMs() * 1000000L);
        if (detachSent)
          ObjectStream.before(now, detachStart, selected.streamLifetimeMs() * 1000000L);
        if (control != null) control.writes.check(now);
      }
    } catch (ProtocolError failure) {
      fail(failure);
    }
  }

  private void sendDetach() {
    if (stopping || selected == null || !detachRequested.get() || detachSent) return;
    try {
      check();
      if (stopping) return;
      byte[] frame = correlation.register(new Detach(1), null);
      detachSent = true;
      detachStart = System.nanoTime();
      control.writes.sendEncoded(frame);
      control.finishOutput();
    } catch (ProtocolError failure) {
      fail(failure);
    }
  }

  private void fail(Throwable failure) {
    if (!stopping) stop(Objects.requireNonNull(failure));
  }

  private void stop(Throwable failure) {
    if (stopping) return;
    stopping = true;
    terminalFailure = failure;
    if (timer != null) timer.cancel(false);
    if (control != null) control.writes.end();
    correlation.close();
    if (failure != null) {
      readiness.completeExceptionally(failure);
      detached.completeExceptionally(failure);
    }
    if (connection != null) {
      int code =
          failure instanceof ProtocolError protocol
              ? (int) protocol.code().applicationError()
              : (int) CANCELLED.applicationError();
      connection.close(
          authenticated, failure == null ? 0 : authenticated ? code : 0x0c, Unpooled.EMPTY_BUFFER);
    }
    if (socket != null) socket.close();
    group
        .shutdownGracefully(0, 2, TimeUnit.SECONDS)
        .addListener(
            ignored -> {
              release();
              if (terminalFailure == null) termination.complete(null);
              else termination.completeExceptionally(terminalFailure);
            });
    if (failure == null) detached.complete(null);
  }

  private void release() {
    synchronized (ADMISSION) {
      admitted--;
      reservedBytes -= bufferBudget;
    }
  }

  /**
   * Abort an unfinished connection and release its owned resources. This is idempotent and blocks
   * outside the owner event loop until termination; inside a completion callback it never waits on
   * itself. Calling close without a successful detach does not report graceful drain.
   */
  @Override
  public void close() {
    if (closeRequested.compareAndSet(false, true)) {
      if (loop.inEventLoop()) fail(new ProtocolError(CANCELLED, "client closed by caller"));
      else {
        try {
          loop.execute(() -> fail(new ProtocolError(CANCELLED, "client closed by caller")));
        } catch (RuntimeException stopped) {
          // A terminal event already started shutdown; await its owner below.
        }
      }
    }
    if (!loop.inEventLoop()) group.terminationFuture().awaitUninterruptibly();
  }

  private final class Connection extends ChannelInboundHandlerAdapter {
    @Override
    public void channelActive(ChannelHandlerContext ctx) {
      if (stopping) return;
      try {
        check();
        if (stopping) return;
        guard.requireAuthenticated();
        authenticated = true;
        lastFrame = System.nanoTime();
        connection
            .createStream(
                QuicStreamType.BIDIRECTIONAL,
                new ChannelInitializer<QuicStreamChannel>() {
                  @Override
                  protected void initChannel(QuicStreamChannel stream) {
                    transport.bindControl(stream);
                    stream
                        .config()
                        .setAllowHalfClosure(true)
                        .setRecvByteBufAllocator(
                            new FixedRecvByteBufAllocator(options.readChunkBytes()));
                    control = new Control(stream);
                    stream.pipeline().addLast(control);
                  }
                })
            .addListener(
                opened -> {
                  if (stopping) return;
                  if (!opened.isSuccess())
                    fail(new ProtocolError(CONTROL_RESET, "control open failed"));
                  else if (control.stream.streamId() != 0)
                    fail(ProtocolError.frame("control must use stream zero"));
                  else control.writes.send(options.offer());
                });
      } catch (ProtocolError failure) {
        fail(failure);
      }
      ctx.fireChannelActive();
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object event) {
      if (!stopping && event instanceof QuicConnectionCloseEvent closed) {
        if (closed.isApplicationClose() && closed.error() > 0x200 && closed.error() <= 0x212)
          fail(
              new ProtocolError(
                  ProtocolError.Code.from(closed.error() - 0x200L), "peer closed connection"));
        else
          fail(
              new IOException(
                  "peer closed before verified drain: application="
                      + closed.isApplicationClose()
                      + ", code="
                      + Integer.toUnsignedString(closed.error())));
      }
      ctx.fireUserEventTriggered(event);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
      deferFailure(cause);
    }
  }

  private final class Control extends SimpleChannelInboundHandler<ByteBuf> {
    final QuicStreamChannel stream;
    final Wire.Decoder decoder = new Wire.Decoder(4096);
    final ControlWrites writes;
    boolean inputFin;
    boolean outputFinStarted;
    boolean outputFin;

    Control(QuicStreamChannel stream) {
      this.stream = stream;
      writes = new ControlWrites(stream, options, CoreClient.this::fail, this::finishOutput);
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, ByteBuf bytes) {
      if (stopping) return;
      try {
        check();
        if (stopping) return;
        ByteBuffer source = bytes.nioBuffer();
        while (source.hasRemaining() && !stopping) {
          Wire.Frame frame = decoder.feed(source);
          if (frame == null) break;
          check();
          if (stopping) return;
          guard.requireAuthenticated();
          ClientCorrelation.Completion completion = correlation.receive(frame);
          if (selected == null) {
            selected = (Capabilities) ((Wire.Known) frame).message();
            decoder.limit(selected.controlLimit());
            writes.selected(selected);
            lastFrame = System.nanoTime();
            readiness.complete(selected);
            sendDetach();
          } else if (completion != null) {
            if (completion.response() instanceof Refusal refusal)
              throw new ProtocolError(refusal.code(), "detach refused by server");
            detachAcknowledged = true;
          }
          lastFrame = System.nanoTime();
        }
      } catch (ProtocolError failure) {
        fail(failure);
      }
    }

    void finishOutput() {
      check();
      if (!stopping && detachSent && !outputFinStarted && writes.empty()) {
        outputFinStarted = true;
        stream
            .shutdownOutput()
            .addListener(
                result -> {
                  if (!result.isSuccess())
                    fail(new ProtocolError(CONTROL_RESET, "control FIN failed"));
                  else {
                    outputFin = true;
                    finish();
                  }
                });
      }
    }

    void finish() {
      check();
      if (!stopping && detachAcknowledged && inputFin && outputFin && writes.empty()) stop(null);
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object event) {
      if (!stopping && event instanceof ChannelInputShutdownEvent) {
        try {
          check();
          if (stopping) return;
          decoder.finish();
          if (!detachAcknowledged)
            throw ProtocolError.frame("server FIN before detach acknowledgment");
          inputFin = true;
          finish();
        } catch (ProtocolError failure) {
          fail(failure);
        }
      } else ctx.fireUserEventTriggered(event);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
      if (!stopping && !(inputFin && outputFin))
        fail(new ProtocolError(CONTROL_RESET, "control ended before verified drain"));
      ctx.fireChannelInactive();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
      fail(new ProtocolError(CONTROL_RESET, "control stream reset or stopped"));
    }
  }
}
