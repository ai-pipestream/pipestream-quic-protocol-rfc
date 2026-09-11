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
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.ChannelInputShutdownEvent;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.handler.codec.quic.QuicChannel;
import io.netty.handler.codec.quic.QuicChannelOption;
import io.netty.handler.codec.quic.QuicServerCodecBuilder;
import io.netty.handler.codec.quic.QuicStreamChannel;
import io.netty.handler.codec.quic.QuicStreamFrame;
import io.netty.handler.codec.quic.QuicStreamType;
import io.netty.util.AttributeKey;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
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
 * The authenticated V2 durable listener. It composes the TLS guard, per-connection request
 * ownership, stream transport, control writes and control waits over one {@link DurableHost}, and
 * implements every Section 12 Core, durable-work and result-delivery operation on the wire.
 *
 * <p>Parsing, correlation and every Netty operation run on the connection event loop. SQLite,
 * hashing, blocking file I/O and application callbacks run on host worker threads; each such task
 * retains its own physical request ticket until it actually returns. Response ownership is released
 * only after Netty settles the write, which is native acceptance, never peer receipt.
 */
public final class DurableServer implements AutoCloseable {
  private static final AttributeKey<Connection> CONNECTION =
      AttributeKey.valueOf(DurableServer.class, "connection");

  /**
   * Listener-owned counts, not durable work statistics.
   *
   * @param active admitted connections, including handshakes
   * @param owners active mapped-owner/anonymous buckets
   * @param refused global/per-owner admission attempts refused
   * @param peakConnections high-water admitted connections
   * @param inputs input transfers currently owned by connections
   * @param results result transfers currently owned by connections
   * @param waits control observations currently owned by connections
   */
  public record Snapshot(
      int active,
      int owners,
      long refused,
      int peakConnections,
      int inputs,
      int results,
      int waits) {}

  private final TlsAuthentication authentication;
  private final DurableHost host;
  private final DurableOptions options;
  private final CoreOptions core;
  private final StreamTransport.Limits transportLimits;
  private final Boundaries boundaries;
  private final MultiThreadIoEventLoopGroup group;
  private final Set<Connection> connections = new HashSet<>();
  private final Map<Optional<String>, Integer> owners = new HashMap<>();
  private final AtomicBoolean stopping = new AtomicBoolean();
  private Channel listener;
  private long refusedAdmissions;
  private int peakConnections;
  private QuicChannel rejected;

  private DurableServer(
      TlsAuthentication authentication,
      DurableHost host,
      DurableOptions options,
      Boundaries boundaries) {
    this.authentication = Objects.requireNonNull(authentication);
    this.host = Objects.requireNonNull(host);
    this.options = Objects.requireNonNull(options);
    this.boundaries = Objects.requireNonNull(boundaries);
    core = options.core();
    transportLimits = options.transportLimits();
    if (!authentication.isServer())
      throw new IllegalArgumentException("server TLS configuration required");
    if (options.objectLimit() > host.configuration().objects().objectBytes())
      throw new IllegalArgumentException("advertised object limit exceeds stored object policy");
    group = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
  }

  /**
   * Bind an actual durable listener over a live host. The caller owns and closes it.
   *
   * @param address explicit local UDP bind address; port zero allocates one
   * @param authentication server trust, key and principal mapping
   * @param host live authority composition
   * @param options bounded listener policy
   * @return running listener
   * @throws InterruptedException if binding is interrupted
   */
  public static DurableServer start(
      InetSocketAddress address,
      TlsAuthentication authentication,
      DurableHost host,
      DurableOptions options)
      throws InterruptedException {
    return start(address, authentication, host, options, Boundaries.NONE);
  }

  /**
   * Bind a listener with test-only boundary hooks. Not for shipped launchers.
   *
   * @param address explicit local UDP bind address
   * @param authentication server trust, key and principal mapping
   * @param host live authority composition
   * @param options bounded listener policy
   * @param boundaries reached-boundary observer
   * @return running listener
   * @throws InterruptedException if binding is interrupted
   */
  static DurableServer start(
      InetSocketAddress address,
      TlsAuthentication authentication,
      DurableHost host,
      DurableOptions options,
      Boundaries boundaries)
      throws InterruptedException {
    var server = new DurableServer(authentication, host, options, boundaries);
    boolean success = false;
    try {
      var codec =
          server
              .transportLimits
              .configure(new QuicServerCodecBuilder(), true)
              .version(1)
              .sslEngineProvider(c -> authentication.engine(c.alloc(), 0))
              .maxIdleTimeout(
                  Math.max(server.core.controlTimeoutMs(), server.core.streamLifetimeMs()),
                  TimeUnit.MILLISECONDS)
              .option(
                  QuicChannelOption.STREAM_SEND_BUFFER_LIMITS,
                  server.transportLimits.nativeSendLimits())
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
      server.boundaries.sent(Boundaries.Boundary.LISTENING, Boundaries.Details.NONE);
      return server;
    } finally {
      if (!success) server.close();
    }
  }

  /**
   * Get the actual bound socket address.
   *
   * @return resolved UDP address
   */
  public InetSocketAddress address() {
    return (InetSocketAddress) listener.localAddress();
  }

  /**
   * Read an event-loop-consistent snapshot.
   *
   * @return counts, or failed completion after shutdown
   */
  public CompletionStage<Snapshot> snapshot() {
    var result = new CompletableFuture<Snapshot>();
    if (stopping.get()) result.completeExceptionally(new IllegalStateException("listener closed"));
    else {
      try {
        group
            .next()
            .execute(
                () -> {
                  int inputs = 0;
                  int results = 0;
                  int waits = 0;
                  for (Connection connection : connections) {
                    inputs += connection.inputs.size();
                    results += connection.results.size();
                    waits += connection.waits.size();
                  }
                  result.complete(
                      new Snapshot(
                          connections.size(),
                          owners.size(),
                          refusedAdmissions,
                          peakConnections,
                          inputs,
                          results,
                          waits));
                });
      } catch (RuntimeException closed) {
        result.completeExceptionally(closed);
      }
    }
    return result.minimalCompletionStage();
  }

  private void accept(QuicChannel channel) {
    if (stopping.get() || connections.size() >= core.connections()) {
      refusedAdmissions++;
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
            1, Math.min(100, Math.min(core.handshakeTimeoutMs(), core.controlTimeoutMs()) / 4));
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

  /**
   * Stop accepting connections, refuse new requests on existing ones, wait up to the configured
   * shutdown budget for connection-local owners to drain, then close the transport. No durable
   * outcome is asserted; the host must be closed separately after this returns.
   */
  @Override
  public void close() {
    if (!stopping.compareAndSet(false, true)) return;
    if (group.next().inEventLoop()) {
      stopChannels(true);
      group.shutdownGracefully(0, 2, TimeUnit.SECONDS);
      return;
    }
    if (listener != null) group.next().submit(() -> listener.close()).awaitUninterruptibly();
    group.next().submit(() -> stopChannels(false)).awaitUninterruptibly();
    long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(options.shutdownTimeoutMs());
    while (System.nanoTime() - deadline < 0) {
      boolean drained =
          Boolean.TRUE.equals(
              group
                  .next()
                  .submit(() -> connections.stream().allMatch(Connection::quiet))
                  .awaitUninterruptibly()
                  .getNow());
      if (drained) break;
      try {
        Thread.sleep(10);
      } catch (InterruptedException interrupted) {
        Thread.currentThread().interrupt();
        break;
      }
    }
    group.next().submit(() -> stopChannels(true)).awaitUninterruptibly();
    group.shutdownGracefully(0, 2, TimeUnit.SECONDS).awaitUninterruptibly();
    boundaries.sent(Boundaries.Boundary.SHUTDOWN_DRAINED, Boundaries.Details.NONE);
  }

  private void stopChannels(boolean force) {
    for (Connection connection : Set.copyOf(connections)) {
      if (force) connection.channel.close();
      else connection.drain();
    }
    if (force && listener != null) listener.close();
  }

  /**
   * The bounded diagnostic carried in a REFUSAL: the local label that named the bound or check (for
   * example the header, idle or lifetime deadline), never state the peer may act on.
   */
  private static String diagnostic(ProtocolError failure) {
    String detail = failure.detail();
    return detail.length() <= 120 ? detail : detail.substring(0, 120);
  }

  /**
   * Enforce one local deadline under its own label, so the REFUSAL or close reason names the bound
   * that fired rather than the shared clock check.
   *
   * @param now monotonic nanoseconds
   * @param then the instant the bound started from
   * @param millis the bound
   * @param label the bound's name
   */
  private static void deadline(long now, long then, long millis, String label) {
    try {
      ObjectStream.before(now, then, millis * 1000000L);
    } catch (ProtocolError expired) {
      throw ProtocolError.limit(label);
    }
  }

  private static ProtocolError named(Throwable failure) {
    Throwable cause = failure;
    while (cause != null) {
      if (cause instanceof ProtocolError error) return error;
      cause = cause.getCause();
    }
    if (failure instanceof IOException || failure instanceof SQLException)
      return new ProtocolError(INTERNAL_ERROR, "authority storage failed");
    return new ProtocolError(INTERNAL_ERROR, "authority operation failed");
  }

  /** Blocking work executed on a host worker thread. */
  @FunctionalInterface
  private interface Blocking<T> {
    T run() throws Exception;
  }

  private final class Connection extends ChannelInboundHandlerAdapter {
    final QuicChannel channel;
    final StreamTransport transport;
    final TlsAuthentication.Guard guard = authentication.guard();
    final Set<InputTransfer> inputs = new HashSet<>();
    final Set<ResultTransfer> results = new HashSet<>();
    final Set<ControlWaitService.Wait<?>> waits = new HashSet<>();
    final long started = System.nanoTime();
    long lastFrame = started;
    long detachStart;
    boolean authenticated;
    boolean detachRequested;
    boolean detachAcknowledged;
    boolean draining;
    boolean closing;
    boolean ended;
    boolean ownerCounted;
    Optional<String> owner;
    Capabilities selected;
    DurableRequests requests;
    SessionStore.Access access;
    Control control;
    io.netty.util.concurrent.ScheduledFuture<?> timer;

    Connection(QuicChannel channel) {
      this.channel = channel;
      transport = new StreamTransport(channel, transportLimits, true);
    }

    boolean durable() {
      return requests != null;
    }

    boolean quiet() {
      return ended || requests == null || requests.usage().pending() == 0;
    }

    @Override
    public void channelActive(ChannelHandlerContext context) {
      try {
        ObjectStream.before(System.nanoTime(), started, core.handshakeTimeoutMs() * 1000000L);
        guard.requireAuthenticated();
        authenticated = true;
        try {
          owner = Optional.of(guard.requireOwner());
        } catch (ProtocolError absent) {
          if (absent.code() != UNAUTHORIZED) throw absent;
          owner = Optional.empty();
        }
        int count = owners.getOrDefault(owner, 0);
        if (count >= core.connectionsPerOwner()) {
          refusedAdmissions++;
          fail(ProtocolError.limit("owner connection ceiling"));
          return;
        }
        owners.put(owner, count + 1);
        ownerCounted = true;
        lastFrame = System.nanoTime();
        boundaries.sent(
            Boundaries.Boundary.CONNECTION_AUTHENTICATED,
            Boundaries.Details.NONE.owner(owner.orElse("")));
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
        if (!authenticated) throw ProtocolError.frame("stream before authentication");
        if (stream.type() == QuicStreamType.BIDIRECTIONAL) {
          if (stream.streamId() != 0 || control != null)
            throw ProtocolError.frame("only client bidirectional stream zero is control");
          transport.bindControl(stream);
          stream
              .config()
              .setAllowHalfClosure(true)
              .setRecvByteBufAllocator(new FixedRecvByteBufAllocator(core.readChunkBytes()));
          control = new Control(stream);
          stream.pipeline().addLast(control);
          return;
        }
        if (selected == null) throw ProtocolError.frame("object stream before negotiation");
        if (!durable())
          throw new ProtocolError(EXTENSION_UNSUPPORTED, "input stream without durable profile");
        InputTransfer transfer = new InputTransfer(stream);
        inputs.add(transfer);
        transfer.begin();
      } catch (ProtocolError failure) {
        fail(failure);
      }
    }

    void check() {
      if (closing || ended) return;
      try {
        long now = System.nanoTime();
        if (!authenticated) deadline(now, started, core.handshakeTimeoutMs(), "handshake deadline");
        else {
          guard.requireAuthenticated();
          // Stream bounds are judged first: a stalled input is refused per stream (Section 12.1)
          // before any connection-level judgement, so its REFUSAL is readable on the surviving
          // control stream even when the idle bound equals the control deadline.
          for (InputTransfer transfer : Set.copyOf(inputs)) transfer.check(now);
          if (control != null) control.writes.check(now);
          if (detachRequested)
            deadline(now, detachStart, selected.streamLifetimeMs(), "detach lifetime");
          // Control silence closes only a core-only connection with nothing outstanding. A durable
          // connection may stay quiet for as long as its owner keeps it: per-owner ceilings and the
          // transport idle timeout bound it, Section 12 requires no more, and closing it would
          // discard queued REFUSALs that a slow reader has not consumed yet. A live input, a
          // result read, a granted wait and a pending request each carry their own bound. The
          // clock is read again because a stream refused above renews the activity clock after
          // `now`.
          if (!durable() && !outstanding())
            deadline(
                System.nanoTime(), lastFrame, core.controlTimeoutMs(), "idle control deadline");
        }
      } catch (ProtocolError failure) {
        fail(failure);
      }
    }

    /** Whether any input, result read, granted wait or pending request is still open. */
    boolean outstanding() {
      return !inputs.isEmpty()
          || !results.isEmpty()
          || !waits.isEmpty()
          || (requests != null && requests.usage().pending() > 0);
    }

    /** Record activity in either direction; it renews only the idle-control clock. */
    void touch() {
      lastFrame = System.nanoTime();
    }

    void drain() {
      if (closing || ended) return;
      draining = true;
      if (requests != null) requests.refuseNew();
    }

    void fail(ProtocolError failure) {
      if (closing || ended) return;
      closing = true;
      stopActivity();
      // After authentication the close carries the named bound as its reason, so a client can
      // tell a LIMIT_EXCEEDED close from a crash without any other channel.
      channel.close(
          authenticated,
          authenticated ? (int) failure.code().applicationError() : 0x02,
          authenticated
              ? Unpooled.copiedBuffer(diagnostic(failure), StandardCharsets.UTF_8)
              : Unpooled.EMPTY_BUFFER);
    }

    void stopActivity() {
      if (timer != null) timer.cancel(false);
      if (control != null) control.writes.end();
      if (requests != null) requests.close();
      for (ControlWaitService.Wait<?> wait : Set.copyOf(waits)) wait.close();
      waits.clear();
      for (InputTransfer transfer : Set.copyOf(inputs)) transfer.connectionLost();
      for (ResultTransfer transfer : Set.copyOf(results)) transfer.connectionLost();
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
      if (cause instanceof javax.net.ssl.SSLException) return;
      fail(new ProtocolError(INTERNAL_ERROR, "connection transport failed"));
    }

    /**
     * Run blocking storage work on a host worker with its own retained ticket, then continue on the
     * event loop. The worker copy is released only after the work physically returns.
     */
    <T> void blocking(
        DurableRequests.Ticket ticket,
        Blocking<T> work,
        java.util.function.Consumer<T> success,
        java.util.function.Consumer<ProtocolError> failure) {
      DurableRequests.Ticket retained;
      try {
        retained = ticket.retain();
      } catch (ProtocolError released) {
        failure.accept(released);
        return;
      }
      try {
        host.workers()
            .submit(
                owner.orElse(""),
                () -> {
                  T value;
                  try {
                    value = work.run();
                  } catch (Throwable thrown) {
                    ProtocolError error = named(thrown);
                    retained.close();
                    loop(() -> failure.accept(error));
                    if (thrown instanceof Error fatal) throw fatal;
                    return;
                  }
                  retained.close();
                  loop(() -> success.accept(value));
                });
      } catch (ProtocolError rejected) {
        retained.close();
        failure.accept(rejected);
      }
    }

    void loop(Runnable action) {
      try {
        channel.eventLoop().execute(action);
      } catch (RuntimeException stopped) {
        // Event loop already terminated; the owning ticket was released by the worker.
      }
    }

    /** Send one correlated response, releasing the request owner after the write settles. */
    void respond(DurableRequests.Ticket ticket, Message response) {
      if (closing || ended) {
        ticket.close();
        return;
      }
      touch();
      byte[] frame;
      try {
        frame = Wire.encode(response, selected.controlLimit());
      } catch (ProtocolError oversized) {
        respondRefusal(ticket, oversized);
        return;
      }
      if (!control.writes.sendEncoded(frame, success -> ticket.close())) ticket.close();
    }

    void respondRefusal(DurableRequests.Ticket ticket, ProtocolError failure) {
      if (closing || ended) {
        ticket.close();
        return;
      }
      touch();
      long id = ticket.request().map(ClientCorrelation::requestId).orElse(0L);
      if (id == 0) {
        ticket.close();
        return;
      }
      Refusal refusal =
          new Refusal(new Records.RequestTag(false, id), failure.code(), diagnostic(failure));
      if (!control.writes.sendEncoded(
          Wire.encode(refusal, selected.controlLimit()),
          success -> {
            ticket.close();
            if (success)
              boundaries.sent(
                  Boundaries.Boundary.REFUSAL_SENT,
                  Boundaries.Details.NONE.refusal(failure.code()));
          })) ticket.close();
    }

    private void dispatch(DurableRequests.Ticket ticket) {
      Message request = ticket.request().orElseThrow();
      long generation = ticket.binding().map(Binding::generation).orElse(0L);
      AdmissionStore.Clock clock = host.storageClock();
      SessionStore sessions = host.sessions();
      switch (request) {
        case NextSequence r ->
            blocking(
                ticket,
                () -> sessions.nextSequence(access, selected, r),
                v -> respond(ticket, v),
                f -> respondRefusal(ticket, f));
        case Create r ->
            blocking(
                ticket,
                () -> {
                  Binding binding = sessions.create(access, selected, r);
                  boundaries.committed(
                      Boundaries.Boundary.SESSION_COMMITTED,
                      Boundaries.Details.NONE
                          .owner(binding.owner())
                          .generation(binding.generation()));
                  return binding;
                },
                binding -> bindAndRespond(ticket, binding),
                f -> respondRefusal(ticket, f));
        case Attach r ->
            blocking(
                ticket,
                () -> sessions.attach(access, selected, r),
                binding -> bindAndRespond(ticket, binding),
                f -> respondRefusal(ticket, f));
        case Declare r ->
            blocking(
                ticket,
                () -> {
                  DeclarationResponse response = sessions.declare(access, selected, generation, r);
                  boundaries.committed(
                      Boundaries.Boundary.DECLARATION_COMMITTED,
                      Boundaries.Details.NONE.operation(r.operation()).generation(generation));
                  return response;
                },
                v -> respondSent(ticket, v, Boundaries.Boundary.DECLARATION_RESPONSE_SENT),
                f -> respondRefusal(ticket, f));
        case Page r ->
            blocking(
                ticket,
                () -> sessions.page(access, selected, generation, r),
                v -> respond(ticket, v),
                f -> respondRefusal(ticket, f));
        case Checkpoint r ->
            observe(ticket, host.waits().checkpoint(access, selected, generation, r));
        case Watch r -> observe(ticket, host.waits().watch(access, selected, generation, r));
        case CancelScope r ->
            blocking(
                ticket,
                () -> {
                  var response =
                      sessions.cancelScope(
                          access, selected, generation, r, clock, host.fenceAuthorization());
                  boundaries.committed(
                      Boundaries.Boundary.FENCE_COMMITTED,
                      Boundaries.Details.NONE.operation(r.operation()).generation(generation));
                  return response;
                },
                v -> respond(ticket, v),
                f -> respondRefusal(ticket, f));
        case LookupOperation r ->
            blocking(
                ticket,
                () -> sessions.lookupOperation(access, selected, generation, r),
                v -> respond(ticket, v),
                f -> respondRefusal(ticket, f));
        case Retry r ->
            blocking(
                ticket,
                () -> {
                  var response =
                      sessions.retry(
                          access, selected, generation, r, clock, host.applicationAuthorization());
                  boundaries.committed(
                      Boundaries.Boundary.RETRY_COMMITTED,
                      Boundaries.Details.NONE
                          .operation(r.operation())
                          .work(r.work())
                          .generation(generation));
                  return response;
                },
                v -> respond(ticket, v),
                f -> respondRefusal(ticket, f));
        case Cancel r ->
            blocking(
                ticket,
                () -> {
                  var response =
                      sessions.cancel(
                          access, selected, generation, r, clock, host.fenceAuthorization());
                  boundaries.committed(
                      Boundaries.Boundary.FENCE_COMMITTED,
                      Boundaries.Details.NONE
                          .operation(r.operation())
                          .work(r.work())
                          .generation(generation));
                  return response;
                },
                v -> respond(ticket, v),
                f -> respondRefusal(ticket, f));
        case Skip r ->
            blocking(
                ticket,
                () -> {
                  var response =
                      sessions.skip(
                          access, selected, generation, r, clock, host.fenceAuthorization());
                  boundaries.committed(
                      Boundaries.Boundary.FENCE_COMMITTED,
                      Boundaries.Details.NONE
                          .operation(r.operation())
                          .work(r.work())
                          .generation(generation));
                  return response;
                },
                v -> respond(ticket, v),
                f -> respondRefusal(ticket, f));
        case GetManifest r ->
            blocking(
                ticket,
                () ->
                    sessions.manifest(access, selected, generation, r, host.resultAuthorization()),
                v -> respond(ticket, v),
                f -> respondRefusal(ticket, f));
        case Read r -> {
          ResultTransfer transfer = new ResultTransfer(ticket, r, generation);
          results.add(transfer);
          transfer.begin();
        }
        case Complete r ->
            blocking(
                ticket,
                () -> sessions.completed(access, selected, generation, r),
                v -> respondSent(ticket, v, Boundaries.Boundary.COMPLETE_RESPONSE_SENT),
                f -> respondRefusal(ticket, f));
        case Detach r -> {
          detachRequested = true;
          detachStart = System.nanoTime();
          ticket
              .drained()
              .whenComplete(
                  (ignored, failure) ->
                      loop(
                          () -> {
                            if (failure != null || closing || ended) {
                              ticket.close();
                              return;
                            }
                            byte[] frame =
                                Wire.encode(new Detached(r.request()), selected.controlLimit());
                            if (!control.writes.sendEncoded(
                                frame,
                                success -> {
                                  detachAcknowledged = true;
                                  ticket.close();
                                  if (success)
                                    boundaries.sent(
                                        Boundaries.Boundary.DETACH_ACKNOWLEDGED,
                                        Boundaries.Details.NONE);
                                  control.finishOutput();
                                })) ticket.close();
                          }));
        }
        default -> respondRefusal(ticket, ProtocolError.frame("unexpected client request"));
      }
    }

    private void respondSent(
        DurableRequests.Ticket ticket, Message response, Boundaries.Boundary boundary) {
      if (closing || ended) {
        ticket.close();
        return;
      }
      byte[] frame;
      try {
        frame = Wire.encode(response, selected.controlLimit());
      } catch (ProtocolError oversized) {
        respondRefusal(ticket, oversized);
        return;
      }
      if (boundaries.withhold(boundary)) {
        // Lost-ACK fixture: the durable commit happened; the reply is dropped with the connection.
        ticket.close();
        fail(new ProtocolError(CONTROL_RESET, "fixture withheld reply"));
        return;
      }
      if (!control.writes.sendEncoded(
          frame,
          success -> {
            ticket.close();
            if (success) boundaries.sent(boundary, Boundaries.Details.NONE);
          })) ticket.close();
    }

    private void bindAndRespond(DurableRequests.Ticket ticket, Binding binding) {
      try {
        ticket.bind(binding);
      } catch (ProtocolError failure) {
        respondRefusal(ticket, failure);
        return;
      }
      respondSent(ticket, binding, Boundaries.Boundary.SESSION_RESPONSE_SENT);
    }

    private <T extends Message> void observe(
        DurableRequests.Ticket ticket, ControlWaitService.Wait<T> wait) {
      waits.add(wait);
      wait.response()
          .whenComplete(
              (response, failure) ->
                  loop(
                      () -> {
                        waits.remove(wait);
                        touch();
                        if (failure == null) respond(ticket, response);
                        else respondRefusal(ticket, named(failure));
                      }));
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
        writes = new ControlWrites(stream, core, Connection.this::fail, this::finishOutput);
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
          if (selected.supported().contains(DURABLE_WORK)) {
            String verified = guard.requireOwner();
            access = host.access(verified, guard::requireOwner);
            requests = new DurableRequests(access, selected);
            if (draining) requests.refuseNew();
          }
          decoder.limit(selected.controlLimit());
          writes.selected(selected);
          writes.send(selected);
          return;
        }
        if (!(frame instanceof Wire.Known known)) return;
        Message message = known.message();
        if (!durable()) {
          long request = ClientCorrelation.requestId(message);
          if (highest == 0 && request != 1 || request <= highest)
            throw ProtocolError.frame("control request does not increase from one");
          highest = request;
          Message response;
          if (detachRequested)
            response =
                new Refusal(
                    new Records.RequestTag(false, request), NOT_READY, "connection detached");
          else if (message instanceof Detach) {
            detachRequested = true;
            detachAcknowledged = true;
            detachStart = System.nanoTime();
            response = new Detached(request);
          } else
            response =
                new Refusal(
                    new Records.RequestTag(false, request),
                    EXTENSION_UNSUPPORTED,
                    "profile not activated");
          writes.send(response);
          return;
        }
        DurableRequests.Acceptance acceptance = requests.accept(message);
        if (acceptance.refusal() != null) {
          ProtocolError.Code code = acceptance.refusal().code();
          writes.sendEncoded(
              Wire.encode(acceptance.refusal(), selected.controlLimit()),
              success -> {
                if (success)
                  boundaries.sent(
                      Boundaries.Boundary.REFUSAL_SENT, Boundaries.Details.NONE.refusal(code));
              });
          return;
        }
        dispatch(acceptance.ticket());
      }

      @Override
      public void userEventTriggered(ChannelHandlerContext ctx, Object event) {
        if (event instanceof ChannelInputShutdownEvent) {
          if (closing || ended) return;
          try {
            check();
            if (closing || ended) return;
            decoder.finish();
            if (!detachRequested) throw ProtocolError.frame("control FIN before detach");
            inputFin = true;
            finishOutput();
          } catch (ProtocolError failure) {
            fail(failure);
          }
        } else ctx.fireUserEventTriggered(event);
      }

      void finishOutput() {
        if (!closing && !ended && inputFin && detachAcknowledged && !outputFin && writes.empty()) {
          outputFin = true;
          stream
              .shutdownOutput()
              .addListener(
                  result -> {
                    if (!result.isSuccess())
                      fail(new ProtocolError(CONTROL_RESET, "control FIN failed"));
                  });
          // Local FIN completion is not a peer ACK; the client owns graceful connection close.
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

    /** One caller input stream: header, payload reception, admission and correlated response. */
    private final class InputTransfer extends SimpleChannelInboundHandler<QuicStreamFrame> {
      final QuicStreamChannel stream;
      final long streamId;
      final long accepted = System.nanoTime();
      StreamTransport.Data data;
      DurableRequests.Ticket ticket;
      ObjectStream.HeaderReader headerReader;
      Records.InputHeader header;
      InputStore.Receiver receiver;
      long headerAccepted;
      long lastProgress;
      boolean busy;
      boolean finished;
      boolean responded;
      boolean done;

      InputTransfer(QuicStreamChannel stream) {
        this.stream = stream;
        streamId = stream.streamId();
      }

      void begin() {
        try {
          data = transport.claimIncoming(stream);
        } catch (ProtocolError failure) {
          inputs.remove(this);
          touch();
          throw failure;
        }
        try {
          ticket = requests.input();
        } catch (ProtocolError failure) {
          refuse(failure);
          return;
        }
        headerReader = new ObjectStream.HeaderReader(true, accepted, options.headerTimeoutMs());
        stream.pipeline().addLast(this);
        stream.read();
      }

      void check(long now) {
        if (done) return;
        try {
          if (header == null) {
            if (headerReader != null) headerReader.checkDeadline(now);
          } else {
            deadline(now, headerAccepted, selected.streamLifetimeMs(), "input stream lifetime");
            deadline(now, lastProgress, selected.streamIdleMs(), "input receive deadline");
          }
        } catch (ProtocolError expired) {
          refuse(expired);
        }
      }

      @Override
      protected void channelRead0(ChannelHandlerContext ctx, QuicStreamFrame frame) {
        if (done || closing || ended) return;
        ByteBuf content = frame.content();
        boolean fin = frame.hasFin();
        try {
          if (header == null) {
            ByteBuffer source = content.nioBuffer();
            Records.Value decoded = headerReader.feed(source, System.nanoTime());
            if (decoded == null) {
              if (fin) headerReader.finish(System.nanoTime());
              stream.read();
              return;
            }
            header = (Records.InputHeader) decoded;
            headerAccepted = System.nanoTime();
            lastProgress = headerAccepted;
            validateHeader();
            byte[] rest = new byte[source.remaining()];
            source.get(rest);
            startAdmission(rest, fin);
            return;
          }
          if (receiver == null) throw ProtocolError.frame("payload before admission check");
          byte[] chunk = new byte[content.readableBytes()];
          content.readBytes(chunk);
          if (chunk.length > 0) {
            lastProgress = System.nanoTime();
            touch();
          }
          write(chunk, fin);
        } catch (ProtocolError failure) {
          refuse(failure);
        }
      }

      private void validateHeader() {
        Binding binding = ticket.binding().orElseThrow();
        if (header.parameters().work().producer() != 0)
          throw new ProtocolError(UNAUTHORIZED, "external input for authority producer");
        if (header.generation() != binding.generation())
          throw new ProtocolError(CONFLICT, "input names another session");
        header.parameters().validateProfiles(selected.supported().contains(RESULT_DELIVERY));
        if (header.parameters().input().length() > selected.objectLimit())
          throw ProtocolError.limit("input exceeds negotiated object limit");
      }

      private void startAdmission(byte[] rest, boolean fin) {
        busy = true;
        long generation = header.generation();
        blocking(
            ticket,
            () ->
                host.sessions()
                    .checkInput(
                        access,
                        selected,
                        generation,
                        host.inputs(),
                        header,
                        host.storageClock(),
                        host.applicationAuthorization()),
            retained -> {
              busy = false;
              if (done) return;
              if (retained.isPresent()) {
                replay(retained.get());
                return;
              }
              busy = true;
              blocking(
                  ticket,
                  () ->
                      host.inputs()
                          .begin(
                              new Commitments.Context(
                                  ticket.binding().orElseThrow().authority(),
                                  ticket.binding().orElseThrow().owner(),
                                  generation),
                              header,
                              selected,
                              headerAccepted),
                  opened -> {
                    busy = false;
                    if (done) {
                      closeReceiver(opened);
                      return;
                    }
                    receiver = opened;
                    if (rest.length > 0) {
                      lastProgress = System.nanoTime();
                      touch();
                    }
                    write(rest, fin);
                  },
                  this::refuse);
            },
            this::refuse);
      }

      private void replay(Records.OperationReceipt receipt) {
        responded = true;
        AdmissionResponse response =
            new AdmissionResponse(new Records.RequestTag(true, streamId), receipt);
        data.stopReplayed();
        data.release();
        stream.close();
        done = true;
        inputs.remove(this);
        touch();
        if (boundaries.withhold(Boundaries.Boundary.ADMISSION_RESPONSE_SENT)) {
          ticket.close();
          fail(new ProtocolError(CONTROL_RESET, "fixture withheld reply"));
          return;
        }
        if (!control.writes.sendEncoded(
            Wire.encode(response, selected.controlLimit()),
            success -> {
              ticket.close();
              if (success)
                boundaries.sent(
                    Boundaries.Boundary.ADMISSION_RESPONSE_SENT,
                    Boundaries.Details.NONE
                        .operation(header.operation())
                        .work(header.parameters().work()));
            })) ticket.close();
      }

      private void write(byte[] chunk, boolean fin) {
        busy = true;
        InputStore.Receiver target = receiver;
        long now = System.nanoTime();
        if (fin) finished = true;
        blocking(
            ticket,
            () -> {
              if (chunk.length > 0) target.write(ByteBuffer.wrap(chunk), now);
              if (!fin) return null;
              // The installed object stays pinned against orphan reclamation until the admission
              // transaction has decided its durable fate (handoff defect 10).
              InputStore.Stored stored = target.finish(now, true);
              try {
                boundaries.committed(
                    Boundaries.Boundary.INPUT_INSTALLED,
                    Boundaries.Details.NONE
                        .operation(header.operation())
                        .work(header.parameters().work()));
                AdmissionResponse response =
                    host.sessions()
                        .admit(
                            access,
                            selected,
                            header.generation(),
                            host.inputs(),
                            header,
                            streamId,
                            host.storageClock(),
                            host.applicationAuthorization());
                boundaries.committed(
                    Boundaries.Boundary.ADMISSION_COMMITTED,
                    Boundaries.Details.NONE
                        .operation(header.operation())
                        .work(header.parameters().work())
                        .attempt(1));
                return response;
              } finally {
                stored.release();
              }
            },
            response -> {
              busy = false;
              if (done) return;
              if (response == null) {
                stream.read();
                return;
              }
              admitted(response);
            },
            this::refuse);
      }

      private void admitted(AdmissionResponse response) {
        responded = true;
        done = true;
        inputs.remove(this);
        touch();
        receiver = null;
        data.release();
        stream.close();
        if (boundaries.withhold(Boundaries.Boundary.ADMISSION_RESPONSE_SENT)) {
          ticket.close();
          fail(new ProtocolError(CONTROL_RESET, "fixture withheld reply"));
          return;
        }
        byte[] frame = Wire.encode(response, selected.controlLimit());
        if (!control.writes.sendEncoded(
            frame,
            success -> {
              ticket.close();
              if (success)
                boundaries.sent(
                    Boundaries.Boundary.ADMISSION_RESPONSE_SENT,
                    Boundaries.Details.NONE
                        .operation(header.operation())
                        .work(header.parameters().work())
                        .attempt(1));
            })) ticket.close();
      }

      private void refuse(ProtocolError failure) {
        if (done) return;
        done = true;
        inputs.remove(this);
        touch();
        InputStore.Receiver open = receiver;
        receiver = null;
        if (data != null) {
          data.abort(failure);
          data.release();
          stream.close();
        }
        if (open != null) closeReceiver(open);
        if (ticket == null) {
          sendInputRefusal(failure, null);
          return;
        }
        if (responded) {
          ticket.close();
          return;
        }
        sendInputRefusal(failure, ticket);
      }

      private void sendInputRefusal(ProtocolError failure, DurableRequests.Ticket owner) {
        if (closing || ended || control == null) {
          if (owner != null) owner.close();
          return;
        }
        Refusal refusal =
            new Refusal(
                new Records.RequestTag(true, streamId), failure.code(), diagnostic(failure));
        if (!control.writes.sendEncoded(
                Wire.encode(refusal, selected.controlLimit()),
                success -> {
                  if (owner != null) owner.close();
                  if (success)
                    boundaries.sent(
                        Boundaries.Boundary.REFUSAL_SENT,
                        Boundaries.Details.NONE.refusal(failure.code()));
                })
            && owner != null) owner.close();
      }

      private void closeReceiver(InputStore.Receiver open) {
        DurableRequests.Ticket retained = null;
        if (ticket != null) {
          try {
            retained = ticket.retain();
          } catch (ProtocolError released) {
            retained = null;
          }
        }
        DurableRequests.Ticket owner = retained;
        try {
          host.workers()
              .submit(
                  Connection.this.owner.orElse(""),
                  () -> {
                    try {
                      open.close();
                    } catch (IOException ignored) {
                      // Uncertain cleanup stays charged in the store; recovery reconciles it.
                    } finally {
                      if (owner != null) owner.close();
                    }
                  });
        } catch (ProtocolError rejected) {
          // No worker capacity: close inline on this thread is forbidden on the loop; the store
          // retains the receiver charge until host maintenance or shutdown closes storage.
          if (owner != null) owner.close();
        }
      }

      void connectionLost() {
        if (done) return;
        done = true;
        inputs.remove(this);
        touch();
        InputStore.Receiver open = receiver;
        receiver = null;
        if (open != null) closeReceiver(open);
        if (ticket != null && !busy) ticket.close();
        else if (ticket != null) {
          // The worker copy releases its own reference; release the response owner now.
          ticket.close();
        }
      }

      @Override
      public void channelInactive(ChannelHandlerContext ctx) {
        // An interrupted input has invalid FIN geometry (Section 12.5): INTEGRITY_ERROR for that
        // input only, discarding partial reception and never its declaration.
        if (!done && !finished)
          refuse(new ProtocolError(INTEGRITY_ERROR, "input stream interrupted before FIN"));
        ctx.fireChannelInactive();
      }

      @Override
      public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        refuse(new ProtocolError(INTEGRITY_ERROR, "input stream interrupted"));
      }
    }

    /** One retained result transfer: pin, server stream, header, chunks, FIN. */
    private final class ResultTransfer extends ChannelInboundHandlerAdapter {
      final DurableRequests.Ticket ticket;
      final Read request;
      final long generation;
      ResultService.Read read;
      StreamTransport.Data data;
      boolean headerSent;
      boolean done;

      ResultTransfer(DurableRequests.Ticket ticket, Read request, long generation) {
        this.ticket = ticket;
        this.request = request;
        this.generation = generation;
      }

      void begin() {
        blocking(
            ticket,
            () ->
                host.results()
                    .begin(
                        access,
                        selected,
                        generation,
                        request,
                        host.storageClock(),
                        host.resultAuthorization()),
            opened -> {
              if (done) {
                closeRead(opened);
                return;
              }
              read = opened;
              open();
            },
            failure -> {
              results.remove(this);
              touch();
              done = true;
              respondRefusal(ticket, failure);
            });
      }

      private void open() {
        CompletionStage<StreamTransport.Data> opening;
        try {
          opening = transport.openData(this);
        } catch (ProtocolError failure) {
          abort(failure, true);
          return;
        }
        opening.whenComplete(
            (opened, failure) ->
                loop(
                    () -> {
                      if (failure != null) {
                        abort(named(failure), true);
                        return;
                      }
                      if (done) {
                        opened.abort(new ProtocolError(CANCELLED, "transfer ended"));
                        opened.release();
                        return;
                      }
                      data = opened;
                      blocking(
                          ticket,
                          () -> read.start(),
                          header -> sendHeader(Wire.encodeHeader(header)),
                          f -> abort(f, false));
                    }));
      }

      private void sendHeader(byte[] bytes) {
        if (done) return;
        int chunk = Math.min(bytes.length, options.chunkBytes());
        byte[] part = java.util.Arrays.copyOfRange(bytes, 0, chunk);
        byte[] remaining = java.util.Arrays.copyOfRange(bytes, chunk, bytes.length);
        data.write(part)
            .whenComplete(
                (ignored, failure) -> {
                  if (failure != null) {
                    abort(named(failure), false);
                    return;
                  }
                  if (remaining.length > 0) sendHeader(remaining);
                  else {
                    headerSent = true;
                    boundaries.sent(
                        Boundaries.Boundary.RESULT_HEADER_SENT,
                        Boundaries.Details.NONE.work(request.work()).attempt(request.attempt()));
                    pump();
                  }
                });
      }

      private void pump() {
        if (done) return;
        byte[] buffer = new byte[options.chunkBytes()];
        blocking(
            ticket,
            () -> {
              int count = read.read(buffer, 0, buffer.length);
              if (count < 0) read.check();
              return count;
            },
            count -> {
              if (done) return;
              if (count < 0) {
                data.finish()
                    .whenComplete(
                        (ignored, failure) -> {
                          if (failure != null) {
                            abort(named(failure), false);
                            return;
                          }
                          blocking(
                              ticket,
                              () -> {
                                read.finish();
                                return null;
                              },
                              v -> complete(),
                              f -> abort(f, false));
                        });
                return;
              }
              byte[] chunk = java.util.Arrays.copyOfRange(buffer, 0, count);
              data.write(chunk)
                  .whenComplete(
                      (ignored, failure) -> {
                        if (failure != null) {
                          abort(named(failure), false);
                          return;
                        }
                        blocking(
                            ticket,
                            () -> {
                              read.sent(count);
                              return null;
                            },
                            v -> pump(),
                            f -> abort(f, false));
                      });
            },
            f -> abort(f, false));
      }

      private void complete() {
        if (done) return;
        done = true;
        results.remove(this);
        touch();
        data.release();
        boundaries.sent(
            Boundaries.Boundary.RESULT_FIN_SENT,
            Boundaries.Details.NONE.work(request.work()).attempt(request.attempt()));
        ticket.close();
      }

      private void abort(ProtocolError failure, boolean refusable) {
        if (done) return;
        done = true;
        results.remove(this);
        touch();
        ResultService.Read open = read;
        read = null;
        if (data != null) {
          data.abort(failure);
          data.release();
        }
        if (open != null) closeRead(open);
        if (refusable && !headerSent && data == null) respondRefusal(ticket, failure);
        else ticket.close();
      }

      private void closeRead(ResultService.Read open) {
        DurableRequests.Ticket retained;
        try {
          retained = ticket.retain();
        } catch (ProtocolError released) {
          retained = null;
        }
        DurableRequests.Ticket owner = retained;
        try {
          host.workers()
              .submit(
                  Connection.this.owner.orElse(""),
                  () -> {
                    try {
                      open.close();
                    } catch (IOException ignored) {
                      // Retained by the result service; its sweep retries physical cleanup.
                    } finally {
                      if (owner != null) owner.close();
                    }
                  });
        } catch (ProtocolError rejected) {
          if (owner != null) owner.close();
        }
      }

      void connectionLost() {
        if (done) return;
        done = true;
        results.remove(this);
        touch();
        ResultService.Read open = read;
        read = null;
        if (open != null) closeRead(open);
        ticket.close();
      }

      @Override
      public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        abort(new ProtocolError(CONTROL_RESET, "result stream stopped"), false);
      }
    }
  }
}
