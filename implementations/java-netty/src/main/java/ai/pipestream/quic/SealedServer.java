package ai.pipestream.quic;

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
import io.netty.channel.socket.ChannelInputShutdownReadComplete;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.TooLongFrameException;
import io.netty.incubator.codec.quic.QuicChannel;
import io.netty.incubator.codec.quic.QuicServerCodecBuilder;
import io.netty.incubator.codec.quic.QuicSslContextBuilder;
import io.netty.incubator.codec.quic.QuicStreamChannel;
import io.netty.incubator.codec.quic.QuicStreamType;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Independent Netty listener for the client-originated sealed-work profile.
 * Storage, file reception, and application work run in separate bounded pools.
 * Producer labels are not credentials; use only with application-authorized peers.
 */
public final class SealedServer implements AutoCloseable {
  private static final int MAX_CONNECTIONS = 32, MAX_STREAMS = 8, MAX_OBSERVERS = 128;
  private final NioEventLoopGroup group;
  private final ThreadPoolExecutor metadata;
  private final ThreadPoolExecutor ingress;
  // A connection slot is retained until its cleanup returns, so this queue cannot overflow.
  private final ThreadPoolExecutor cleanup;
  private final Map<QuicChannel, Peer> peers = new HashMap<>();
  private final AtomicInteger connections = new AtomicInteger();
  private final AtomicBoolean closing = new AtomicBoolean();
  private final AtomicReference<Throwable> cleanupFailure = new AtomicReference<>();
  private final SealedSessionStore sessions;
  private final SealedPayloadStore payloads;
  private final SealedExecutor executor;
  private final SealedJobs jobs;
  private final SealedTransport.Limits limits;
  private volatile Channel datagram;

  private SealedServer(SealedSessionStore sessions, SealedPayloadStore payloads,
      SealedExecutor.Processor processor, SealedTransport.Limits limits, SealedExecutor.Limits execution) throws Exception {
    this.sessions = Objects.requireNonNull(sessions); this.payloads = Objects.requireNonNull(payloads);
    this.limits = Objects.requireNonNull(limits); this.jobs = new SealedJobs(sessions);
    SealedTransport.capabilities(limits);
    executor = SealedExecutor.start(sessions, payloads, processor, execution);
    group = new NioEventLoopGroup(1);
    metadata = pool("sealed-metadata", 4, 64); ingress = pool("sealed-ingress", 4, 32);
    cleanup = pool("sealed-cleanup", 1, MAX_CONNECTIONS);
  }

  /**
   * Binds a sealed-only listener and starts its durable executor. No Layer 0 fallback is offered.
   * The caller owns the stores and must keep them alive until {@link #isTerminated()}.
   * @param bind UDP listening address, including port zero for an ephemeral port
   * @param certificate server PEM certificate chain
   * @param privateKey server PEM private key
   * @param sessions dedicated Java state store
   * @param payloads exclusively owned Java payload store
   * @param processor application processing and rehydration callback
   * @param limits maximum negotiated connection capabilities
   * @param execution physical application worker and lease limits
   * @return running listener
   * @throws Exception for invalid configuration, store, TLS, or bind failure
   */
  public static SealedServer start(InetSocketAddress bind, Path certificate, Path privateKey,
      SealedSessionStore sessions, SealedPayloadStore payloads, SealedExecutor.Processor processor,
      SealedTransport.Limits limits, SealedExecutor.Limits execution) throws Exception {
    var tls = QuicSslContextBuilder.forServer(privateKey.toFile(), null, certificate.toFile())
        .applicationProtocols(Wire.ALPN).build();
    SealedServer server = new SealedServer(sessions, payloads, processor, limits, execution);
    try {
      var codec = new QuicServerCodecBuilder().sslContext(tls).maxIdleTimeout(30, TimeUnit.SECONDS)
          .initialMaxData(1L << 20).initialMaxStreamDataBidirectionalLocal(1L << 20)
          .initialMaxStreamDataBidirectionalRemote(1L << 20).initialMaxStreamDataUnidirectional(65536)
          .initialMaxStreamsBidirectional(1).initialMaxStreamsUnidirectional(MAX_STREAMS)
          .tokenHandler(new AddressValidationTokenHandler())
          .handler(new ChannelInboundHandlerAdapter() {
            @Override public boolean isSharable() { return true; }
            @Override public void channelActive(ChannelHandlerContext context) { server.attach((QuicChannel) context.channel()); }
            @Override public void channelInactive(ChannelHandlerContext context) {
              Peer peer = server.peers.remove((QuicChannel) context.channel());
              if (peer != null) peer.dispose();
            }
          }).streamHandler(new ChannelInitializer<QuicStreamChannel>() {
            @Override protected void initChannel(QuicStreamChannel channel) {
              Peer peer = server.attach(channel.parent());
              if (peer == null) { channel.close(); return; }
              if (channel.type() == QuicStreamType.BIDIRECTIONAL) {
                if (channel.streamId() != 0 || peer.control != null) { peer.fail(Wire.frame("only control stream zero is permitted")); return; }
                peer.control = channel;
                channel.pipeline().addLast(new LengthFieldBasedFrameDecoder(Wire.MAX_CONTROL_FRAME + 5, 1, 4, 0, 0))
                    .addLast(server.new Control(peer));
              } else {
                if (!peer.attached() || peer.goaway) { peer.fail(Wire.entity("entity stream without attached session")); return; }
                if (peer.receivers.size() >= MAX_STREAMS) { peer.fail(Wire.limit("entity stream capacity exhausted")); return; }
                channel.config().setAutoRead(false);
                channel.config().setRecvByteBufAllocator(new FixedRecvByteBufAllocator(8192).maxMessagesPerRead(1));
                Receiver receiver = server.new Receiver(peer, channel);
                peer.receivers.add(receiver); channel.pipeline().addLast(receiver);
                try { server.ingress.execute(receiver::receive); }
                catch (RejectedExecutionException full) { peer.fail(Wire.limit("payload worker capacity exhausted")); }
              }
            }
          }).build();
      server.datagram = new Bootstrap().group(server.group).channel(NioDatagramChannel.class)
          .handler(codec).bind(bind).sync().channel();
      server.group.next().scheduleAtFixedRate(server::tick, 10, 10, TimeUnit.MILLISECONDS);
      return server;
    } catch (Exception failure) { server.close(); throw failure; }
  }

  /** Returns the bound UDP address.
   * @return actual address, including the assigned ephemeral port
   */
  public InetSocketAddress address() { return (InetSocketAddress) datagram.localAddress(); }

  /**
   * Reports physical shutdown, not merely cancellation of network waiters.
   * @return true only after networking, file/storage tasks, and callbacks have terminated
   */
  public boolean isTerminated() {
    return group.isTerminated() && metadata.isTerminated() && ingress.isTerminated()
        && cleanup.isTerminated() && executor.isTerminated();
  }

  /** Returns a local cleanup or executor failure without exposing it to a remote peer.
   * @return first cleanup failure, otherwise the executor's fatal failure if present
   */
  public Optional<Throwable> failure() {
    Throwable failure = cleanupFailure.get();
    return failure == null ? executor.failure() : Optional.of(failure);
  }

  /** Stops ingress without erasing admitted work or interrupting application callbacks. */
  @Override public void close() {
    if (!closing.compareAndSet(false, true)) return;
    group.next().execute(() -> {
      for (Peer peer : List.copyOf(peers.values())) peer.fail(new IOException("sealed listener is closing"));
      if (datagram != null) datagram.close();
      metadata.shutdown(); ingress.shutdown(); cleanup.shutdown(); executor.close();
      group.next().scheduleAtFixedRate(this::finishShutdown, 0, 10, TimeUnit.MILLISECONDS);
    });
  }

  private void finishShutdown() {
    if (metadata.isTerminated() && ingress.isTerminated() && cleanup.isTerminated() && executor.isTerminated()) {
      group.shutdownGracefully(0, 1, TimeUnit.SECONDS);
    }
  }

  private static ThreadPoolExecutor pool(String name, int threads, int queue) {
    AtomicInteger sequence = new AtomicInteger();
    return new ThreadPoolExecutor(threads, threads, 0, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(queue),
        task -> new Thread(task, name + "-" + sequence.incrementAndGet()), new ThreadPoolExecutor.AbortPolicy());
  }

  private Peer attach(QuicChannel channel) {
    Peer existing = peers.get(channel);
    if (existing != null) return existing;
    if (closing.get() || connections.get() >= MAX_CONNECTIONS) {
      channel.close(true, (int) Wire.ERROR_LIMIT_EXCEEDED, Unpooled.EMPTY_BUFFER); return null;
    }
    connections.incrementAndGet(); Peer peer = new Peer(channel); peers.put(channel, peer); return peer;
  }

  private void tick() {
    for (Peer peer : List.copyOf(peers.values())) {
      try { peer.tick(); } catch (Exception failure) { peer.fail(failure); }
    }
  }

  @FunctionalInterface private interface Work<T> { T run() throws Exception; }
  @FunctionalInterface private interface Reply<T> { void accept(T value) throws Exception; }
  private record Action<T>(int bytes, Work<T> work, Reply<T> reply) {}
  private record Cut(long scope, BigInteger sequence) {}
  private static final class Pending {
    final SealedTransport.Checkpoint request;
    final long started = System.nanoTime();
    final BigInteger nanos;
    int copies = 1;
    boolean registered;
    Pending(SealedTransport.Checkpoint request) {
      this.request = request;
      nanos = (request.timeoutMs() == null ? BigInteger.valueOf(30_000) : request.timeoutMs()).multiply(BigInteger.valueOf(1_000_000));
    }
    boolean expired() { return BigInteger.valueOf(System.nanoTime() - started).compareTo(nanos) >= 0; }
  }

  private final class Peer {
    final QuicChannel connection;
    final Set<Receiver> receivers = new HashSet<>();
    final Map<SealedWork.EntityKey, Assembly> assemblies = new HashMap<>(); // guarded by assemblies
    final Set<SealedWork.EntityKey> announced = new HashSet<>(), pending = new HashSet<>(), admittedHere = new HashSet<>();
    final Map<Long, SealedSessionStore.ScopeInfo> scopes = new HashMap<>();
    final Map<SealedJobs.Key, Integer> observers = new LinkedHashMap<>();
    final Map<Cut, Pending> checkpoints = new LinkedHashMap<>();
    final ArrayDeque<Action<?>> actions = new ArrayDeque<>();
    volatile boolean dead;
    volatile String session;
    volatile UUID producer;
    QuicStreamChannel control;
    SealedTransport.Limits selected;
    boolean actionRunning, polling, goaway;
    int queuedBytes, outputBytes;
    Long rootAck;
    Peer(QuicChannel connection) { this.connection = connection; }
    boolean attached() { return selected != null && session != null && !dead; }

    void fail(Throwable failure) {
      if (dead) return;
      dispose();
      long code = failure instanceof ProtocolException protocol ? protocol.errorCode() : Wire.ERROR_FRAME;
      // Exceptions can contain paths or SQL; only the protocol code crosses the network.
      String name = switch ((int) code) {
        case 4 -> "PIPESTREAM_INTEGRITY_ERROR"; case 5 -> "PIPESTREAM_ENTITY_INVALID";
        case 6 -> "PIPESTREAM_LIMIT_EXCEEDED"; case 7 -> "PIPESTREAM_DEPTH_EXCEEDED";
        case 9 -> "PIPESTREAM_SCOPE_INVALID"; case 12 -> "PIPESTREAM_LAYER_UNSUPPORTED";
        case 14 -> "PIPESTREAM_CHECKPOINT_TIMEOUT"; case 15 -> "PIPESTREAM_EXTENSION_UNSUPPORTED";
        default -> "PIPESTREAM_FRAME_ERROR";
      };
      connection.close(true, (int) code, Unpooled.copiedBuffer(name, StandardCharsets.US_ASCII));
    }

    void dispose() {
      if (dead) return;
      dead = true; actions.clear(); checkpoints.clear(); observers.clear();
      cleanup.execute(() -> {
        List<SealedPayloadStore.Received> abandoned = new ArrayList<>();
        synchronized (assemblies) {
          for (Assembly assembly : assemblies.values()) if (!assembly.installing) abandoned.addAll(assembly.receipts.values());
          assemblies.clear();
        }
        try { closeReceipts(abandoned); }
        catch (IOException failure) { cleanupFailure.compareAndSet(null, failure); SealedServer.this.close(); }
        finally { connections.decrementAndGet(); }
      });
    }

    void write(byte[] frame) throws ProtocolException {
      if (dead) return;
      if (control == null) throw Wire.frame("control stream is absent");
      if (frame.length > (1 << 20) - outputBytes) throw Wire.limit("control output capacity exhausted");
      outputBytes += frame.length;
      control.writeAndFlush(Unpooled.wrappedBuffer(frame)).addListener(sent -> {
        outputBytes -= frame.length;
        if (!sent.isSuccess()) fail(sent.cause());
      });
    }

    void status(SealedWork.EntityKey key, int state, int depth) throws ProtocolException {
      write(Wire.encodeStatus(new Wire.Status(state, key.entityId(), key.scopeId(), null, depth)));
    }

    <T> void enqueue(int bytes, Work<T> work, Reply<T> reply) throws ProtocolException {
      if (actions.size() >= 32 || bytes > (4 << 20) - queuedBytes) throw Wire.limit("connection storage backlog exhausted");
      actions.add(new Action<>(bytes, work, reply)); queuedBytes += bytes; dispatch();
    }

    void dispatch() {
      if (dead || actionRunning || actions.isEmpty()) return;
      actionRunning = true; execute(actions.remove());
    }

    <T> void execute(Action<T> action) {
      try {
        metadata.execute(() -> {
          T result = null; Throwable failure = null;
          try { if (!dead) result = action.work.run(); } catch (Exception error) { failure = error; }
          T value = result; Throwable error = failure;
          connection.eventLoop().execute(() -> {
            actionRunning = false; queuedBytes -= action.bytes;
            if (dead) return;
            try {
              checkDeadlines();
              if (error != null) { fail(error); return; }
              action.reply.accept(value); dispatch();
            } catch (Exception problem) { fail(problem); }
          });
        });
      } catch (RejectedExecutionException full) { fail(Wire.limit("metadata worker capacity exhausted")); }
    }

    void cache(List<SealedSessionStore.ScopeInfo> ancestry) throws ProtocolException {
      if (scopes.size() + ancestry.size() > 16392) throw Wire.limit("scope observation capacity exhausted");
      for (var scope : ancestry) scopes.put(scope.id(), scope);
    }

    boolean covered(long ancestor, long descendant) {
      if (ancestor == 0 || ancestor == descendant) return true;
      for (int depth = 0; depth <= 7; depth++) {
        var scope = scopes.get(descendant);
        if (scope == null) return true; // Unknown lineage cannot justify an early ACK.
        if (scope.parent() == null) return false;
        descendant = scope.parent().scopeId();
        if (ancestor == descendant) return true;
      }
      return true;
    }

    boolean blocked(long scope) {
      for (var cut : checkpoints.keySet()) if (cut.scope() != scope && covered(scope, cut.scope())) return true;
      for (var key : pending) if (covered(scope, key.scopeId())) return true;
      for (var key : observers.keySet()) if (covered(scope, key.identity().entity().scopeId())) return true;
      for (var receiver : receivers) if (receiver.header == null || covered(scope, receiver.header.key().scopeId())) return true;
      synchronized (assemblies) {
        for (var key : assemblies.keySet()) if (covered(scope, key.scopeId())) return true;
      }
      return false;
    }

    void observe(SealedWork.EntityKey key, int kind, int depth) throws ProtocolException {
      if (observers.size() >= MAX_OBSERVERS) throw Wire.limit("result observation capacity exhausted");
      observers.put(new SealedJobs.Key(new SealedPayloadStore.Identity(session, producer, key), kind), depth);
    }

    void checkDeadlines() throws ProtocolException {
      for (Pending checkpoint : checkpoints.values()) if (checkpoint.expired()) {
        throw new ProtocolException(0x0e, "PIPESTREAM_CHECKPOINT_TIMEOUT", "checkpoint deadline expired");
      }
    }

    void tick() throws Exception {
      if (dead || goaway) return;
      checkDeadlines();
      if (executor.failure().isPresent()) { fail(executor.failure().orElseThrow()); return; }
      if (!attached() || polling || actionRunning || !actions.isEmpty()) return;
      var watching = new LinkedHashMap<>(observers);
      List<Map.Entry<Cut, Pending>> cuts = checkpoints.entrySet().stream()
          .filter(entry -> entry.getValue().registered && !blocked(entry.getKey().scope())).toList();
      if (watching.isEmpty() && cuts.isEmpty()) return;
      polling = true;
      try {
        metadata.execute(() -> {
          List<SealedJobs.Job> outcomes = new ArrayList<>(); List<Map.Entry<Cut, Pending>> ready = new ArrayList<>();
          Throwable error = null;
          try {
            if (!dead) {
              for (var job : jobs.findAll(List.copyOf(watching.keySet()))) {
                if (job.state() == SealedJobs.FINISHED || job.state() == SealedJobs.REFUSED) outcomes.add(job);
              }
              for (var cut : cuts) if (!dead && sessions.acknowledgeCheckpoint(session, producer, cut.getValue().request)) ready.add(cut);
            }
          } catch (Exception failure) { error = failure; }
          Throwable failure = error;
          connection.eventLoop().execute(() -> {
            polling = false;
            if (dead) return;
            try {
              checkDeadlines();
              if (failure != null) { fail(failure); return; }
              for (var job : outcomes) {
                Integer depth = observers.remove(job.key());
                if (depth == null) continue;
                if (job.state() == SealedJobs.REFUSED) throw new ProtocolException(job.outcome().refusal(), "PIPESTREAM_EXECUTION_REFUSED", "retained execution refusal");
                status(job.key().identity().entity(), job.outcome().state(), depth);
              }
              for (var cut : ready) {
                if (blocked(cut.getKey().scope()) || checkpoints.get(cut.getKey()) != cut.getValue()) continue;
                var checkpoint = cut.getValue();
                for (int copy = 0; copy < checkpoint.copies; copy++) {
                  checkDeadlines(); write(SealedTransport.checkpoint(checkpoint.request.acknowledgement()));
                }
                checkpoints.remove(cut.getKey());
                if (cut.getKey().scope() == 0) rootAck = checkpoint.request.lastId();
              }
            } catch (Exception problem) { fail(problem); }
          });
        });
      } catch (RejectedExecutionException full) { polling = false; fail(Wire.limit("metadata worker capacity exhausted")); }
    }
  }

  private final class Control extends SimpleChannelInboundHandler<ByteBuf> {
    final Peer peer;
    Control(Peer peer) { this.peer = peer; }
    @Override protected void channelRead0(ChannelHandlerContext context, ByteBuf bytes) {
      if (peer.dead) return;
      byte[] encoded = new byte[bytes.readableBytes()]; bytes.readBytes(encoded);
      try {
        var frame = Wire.decodeControl(encoded);
        if (peer.selected == null) {
          if (frame.type() != Wire.FRAME_CAPABILITIES) throw Wire.frame("first frame must be CAPABILITIES");
          peer.selected = SealedTransport.negotiate(frame.payload(), limits);
          peer.write(SealedTransport.capabilities(peer.selected));
          peer.write(Wire.encodeStatus(new Wire.Status(0, Wire.CONNECTION_LEVEL, 0, null, 0))); return;
        }
        if (peer.goaway) throw Wire.entity("control after GOAWAY");
        if (frame.type() == Wire.FRAME_CAPABILITIES) throw Wire.frame("duplicate capabilities");
        if (frame.type() == Wire.FRAME_STATUS) {
          var status = SealedTransport.status(frame.payload());
          if (status.state() == 0) return;
          if (status.state() != 1) throw Wire.entity("producer may only announce PENDING");
          var key = new SealedWork.EntityKey(status.scopeId(), status.entityId());
          if (!peer.announced.add(key)) throw Wire.entity("repeated PENDING announcement");
          if (peer.announced.size() > 16384 || peer.pending.size() >= peer.selected.window()) throw Wire.limit("PENDING window exhausted");
          peer.pending.add(key);
          peer.enqueue(encoded.length, () -> sessions.describe(requireSession(), peer.producer, key), info -> {
            peer.cache(info.ancestry());
            if (info.ancestry().getFirst().depth() != status.depth()) throw Wire.entity("PENDING scope depth mismatch");
            boolean arriving = peer.receivers.stream().anyMatch(receiver -> receiver.header != null && receiver.header.key().equals(key));
            if (info.state() != 0 && !peer.admittedHere.contains(key) && !arriving) throw Wire.entity("PENDING recycles admitted work");
            if (peer.admittedHere.contains(key)) peer.pending.remove(key);
          }); return;
        }
        if (frame.type() == SealedWork.FRAME) {
          var declaration = SealedWork.decodePayload(frame.payload());
          if ((declaration.flags() & SealedWork.ACK) != 0) throw Wire.entity("producer declaration carries ACK");
          peer.enqueue(encoded.length, () -> {
            if (peer.session == null) {
              if (declaration.scopeId() != 0 || declaration.sequence().signum() != 0) throw Wire.entity("attachment requires root sequence zero");
            } else if (!peer.session.equals(declaration.sessionId()) || !peer.producer.equals(declaration.producerId())) throw Wire.entity("declaration changes session binding");
            var ack = sessions.declare(declaration, peer.selected.depth(), peer.selected.entities());
            return new DeclarationReply(ack, sessions.ancestry(declaration.sessionId(), declaration.producerId(), declaration.scopeId()));
          }, reply -> {
            peer.producer = declaration.producerId(); peer.session = declaration.sessionId();
            peer.cache(reply.ancestry()); peer.write(SealedWork.encode(reply.ack()));
          }); return;
        }
        if (frame.type() == Wire.FRAME_CHECKPOINT) {
          var request = SealedTransport.checkpoint(frame.payload());
          if (request.flags() != 0) throw Wire.entity("producer checkpoint carries ACK");
          Cut cut = new Cut(request.scopeId() == null ? 0 : request.scopeId(), request.sequence());
          Pending previous = peer.checkpoints.get(cut);
          if (previous != null) {
            if (!previous.request.equals(request)) throw Wire.entity("checkpoint sequence changed while pending");
            if (++previous.copies > 1024) throw Wire.limit("checkpoint duplicate capacity exhausted");
            peer.checkDeadlines(); return;
          }
          if (peer.checkpoints.size() >= 1024) throw Wire.limit("pending checkpoint capacity exhausted");
          Pending pending = new Pending(request); peer.checkpoints.put(cut, pending); peer.checkDeadlines();
          peer.enqueue(encoded.length, () -> {
            sessions.registerCheckpoint(requireSession(), peer.producer, request);
            return sessions.ancestry(peer.session, peer.producer, cut.scope());
          }, ancestry -> { peer.cache(ancestry); pending.registered = true; }); return;
        }
        if (frame.type() == SealedScope.FRAME) {
          var digest = SealedScope.decode(encoded);
          peer.enqueue(encoded.length, () -> {
            String session = requireSession();
            var ancestry = sessions.ancestry(session, peer.producer, digest.scopeId());
            return new ClosureReply(executor.confirmScope(session, peer.producer, digest), ancestry);
          }, reply -> {
            peer.cache(reply.ancestry()); var closure = reply.closure();
            peer.write(SealedScope.encode(closure.digest()));
            var info = peer.scopes.get(digest.scopeId());
            if (info == null || !Objects.equals(info.parent(), closure.parent())) throw Wire.integrity("scope observation is absent");
            peer.scopes.put(info.id(), new SealedSessionStore.ScopeInfo(info.id(), info.parent(), info.depth(), true));
            if (closure.digest().failed().signum() != 0) peer.status(closure.parent(), 4, info.depth() - 1);
            else {
              peer.status(closure.parent(), 7, info.depth() - 1);
              peer.observe(closure.parent(), SealedJobs.REHYDRATE, info.depth() - 1);
            }
          }); return;
        }
        if (frame.type() == 0x55) {
          var barrier = SealedTransport.barrier(frame.payload());
          if (barrier.released()) throw Wire.entity("producer barrier carries release");
          peer.enqueue(encoded.length, () -> sessions.ancestry(requireSession(), peer.producer, barrier.scopeId()), ancestry -> {
            peer.cache(ancestry); var info = ancestry.getFirst();
            if (info.parent() == null || info.parent().entityId() != barrier.parentId()) throw SealedScope.invalid("barrier parent mismatch");
            peer.write(SealedTransport.barrier(new SealedTransport.Barrier(barrier.scopeId(), barrier.parentId(), info.closed())));
          }); return;
        }
        if (frame.type() == Wire.FRAME_GOAWAY) {
          long last = Wire.decodeGoaway(frame.payload());
          if (peer.rootAck == null || peer.rootAck != last || !peer.checkpoints.isEmpty() || peer.blocked(0)
              || peer.actionRunning || !peer.actions.isEmpty()) throw Wire.entity("GOAWAY lacks completed root checkpoint");
          peer.goaway = true; peer.write(Wire.encodeGoaway(last));
          peer.control.writeAndFlush(Unpooled.EMPTY_BUFFER).addListener(QuicStreamChannel.SHUTDOWN_OUTPUT); return;
        }
        if (frame.type() == 0x82 || frame.type() == 0x84) throw Wire.layerUnsupported("sealed profile excludes Layer 2 controls");
        // Section 6.1 permits unknown controls to be skipped after validating their length.
      } catch (Exception failure) { peer.fail(failure); }
    }
    String requireSession() throws ProtocolException {
      if (!peer.attached()) throw Wire.entity("control requires attached sealed session"); return peer.session;
    }
    @Override public void exceptionCaught(ChannelHandlerContext context, Throwable failure) {
      peer.fail(failure instanceof TooLongFrameException ? Wire.limit("control frame exceeds receive bound") : failure);
    }
    @Override public void channelInactive(ChannelHandlerContext context) {
      if (!peer.goaway) peer.fail(Wire.entity("control stream closed before GOAWAY"));
    }
  }

  private record DeclarationReply(SealedWork.Declaration ack, List<SealedSessionStore.ScopeInfo> ancestry) {}
  private record ClosureReply(SealedJobs.Closure closure, List<SealedSessionStore.ScopeInfo> ancestry) {}
  private static final class Assembly {
    final int total;
    final Map<Integer, SealedPayloadStore.Received> receipts = new HashMap<>();
    final Set<Integer> claimed = new HashSet<>();
    boolean installing;
    Assembly(int total) { this.total = total; }
  }

  private final class Receiver extends ChannelInboundHandlerAdapter {
    final Peer peer;
    final QuicStreamChannel channel;
    final ArrayBlockingQueue<byte[]> bytes = new ArrayBlockingQueue<>(8);
    volatile SealedTransport.Header header;
    volatile boolean fin, reset;
    boolean reading;
    Receiver(Peer peer, QuicStreamChannel channel) { this.peer = peer; this.channel = channel; }
    @Override public void channelActive(ChannelHandlerContext context) { requestRead(); }
    void requestRead() {
      if (!reading && !fin && !reset && !peer.dead && bytes.remainingCapacity() >= 4) { reading = true; channel.read(); }
    }
    @Override public void channelRead(ChannelHandlerContext context, Object message) {
      ByteBuf input = (ByteBuf) message;
      try {
        if (peer.dead || reset) return;
        while (input.isReadable()) {
          byte[] chunk = new byte[Math.min(8192, input.readableBytes())]; input.readBytes(chunk);
          if (!bytes.offer(chunk)) { peer.fail(Wire.limit("entity read backlog exhausted")); break; }
        }
      } finally { input.release(); }
    }
    @Override public void channelReadComplete(ChannelHandlerContext context) { reading = false; }
    @Override public void userEventTriggered(ChannelHandlerContext context, Object event) {
      if (event == ChannelInputShutdownReadComplete.INSTANCE) fin = true;
      else context.fireUserEventTriggered(event);
    }
    @Override public void channelInactive(ChannelHandlerContext context) { if (!fin) reset = true; }
    @Override public void exceptionCaught(ChannelHandlerContext context, Throwable cause) { reset = true; peer.fail(cause); }

    void receive() {
      try (InputStream source = new InputStream() {
        byte[] current; int offset;
        @Override public int read() throws IOException {
          byte[] one = new byte[1]; return read(one, 0, 1) < 0 ? -1 : Byte.toUnsignedInt(one[0]);
        }
        @Override public int read(byte[] target, int start, int count) throws IOException {
          Objects.checkFromIndexSize(start, count, target.length);
          if (count == 0) return 0;
          while (current == null || offset == current.length) {
            if (peer.dead || reset) throw new IOException("entity reception was cancelled");
            current = bytes.poll(); offset = 0;
            if (current != null) break;
            if (fin) return -1;
            channel.eventLoop().execute(Receiver.this::requestRead);
            try { current = bytes.poll(100, TimeUnit.MILLISECONDS); }
            catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); throw new IOException("entity reader interrupted", interrupted); }
          }
          int copied = Math.min(count, current.length - offset);
          System.arraycopy(current, offset, target, start, copied); offset += copied; return copied;
        }
      }) {
        DataInputStream input = new DataInputStream(source);
        long length = Integer.toUnsignedLong(input.readInt());
        if (length == 0 || length > Wire.MAX_ENTITY_HEADER) throw Wire.limit("entity header exceeds bound");
        byte[] encoded = new byte[(int) length]; input.readFully(encoded);
        header = SealedTransport.header(encoded);
        var info = sessions.describe(peer.session, peer.producer, header.key());
        if (info.state() != 0) throw Wire.entity("entity payload was already admitted");
        if (!Objects.equals(info.ancestry().getFirst().parent(), header.parent())) throw SealedScope.invalid("entity parent differs from declaration");
        if (header.metadata().containsKey("pipestream.session-id") && !peer.session.equals(header.metadata().get("pipestream.session-id"))) throw Wire.entity("entity metadata changes session binding");
        int total = header.chunk() == null ? 1 : header.chunk().total().min(BigInteger.valueOf(1025)).intValueExact();
        if (total > 1024) throw Wire.limit("chunk count exceeds receive bound");
        int index = header.chunk() == null ? 0 : header.chunk().index().intValueExact();
        Assembly assembly;
        synchronized (peer.assemblies) {
          if (peer.dead) throw new IOException("connection closed");
          assembly = peer.assemblies.get(header.key());
          if (assembly == null) {
            if (peer.assemblies.size() >= 32) throw Wire.limit("partial assembly capacity exhausted");
            assembly = new Assembly(total); peer.assemblies.put(header.key(), assembly);
          }
          if (assembly.total != total || assembly.installing || !assembly.claimed.add(index)) throw Wire.entity("duplicate or inconsistent entity chunk");
        }
        var identity = new SealedPayloadStore.Identity(peer.session, peer.producer, header.key());
        SealedPayloadStore.Received receipt;
        try (var receiver = payloads.begin(identity, header)) {
          byte[] buffer = new byte[8192]; int count;
          while ((count = input.read(buffer)) >= 0) receiver.write(buffer, 0, count);
          if (peer.dead || reset) throw new IOException("entity stream cancelled before admission");
          receipt = receiver.finish();
        }
        List<SealedPayloadStore.Received> complete = null;
        synchronized (peer.assemblies) {
          if (!peer.dead) {
            assembly.receipts.put(index, receipt);
            if (assembly.receipts.size() == total) { assembly.installing = true; complete = List.copyOf(assembly.receipts.values()); }
          }
        }
        if (peer.dead && complete == null) { receipt.close(); return; }
        if (complete != null) {
          try {
            var stored = payloads.install(complete);
            if (peer.dead) return; // An installed orphan is not admission evidence.
            executor.admit(stored);
            channel.eventLoop().execute(() -> {
              if (peer.dead) return;
              try {
                peer.cache(info.ancestry()); peer.admittedHere.add(header.key()); peer.pending.remove(header.key());
                peer.status(header.key(), 2, info.ancestry().getFirst().depth());
                peer.observe(header.key(), SealedJobs.PROCESS, info.ancestry().getFirst().depth());
              } catch (Exception failure) { peer.fail(failure); }
            });
          } finally {
            try { closeReceipts(complete); }
            finally { synchronized (peer.assemblies) { peer.assemblies.remove(header.key()); } }
          }
        }
      } catch (Exception failure) { channel.eventLoop().execute(() -> peer.fail(failure)); }
      finally { channel.eventLoop().execute(() -> { peer.receivers.remove(this); channel.close(); }); }
    }
  }

  private static void closeReceipts(List<SealedPayloadStore.Received> receipts) throws IOException {
    IOException failure = null;
    for (var receipt : receipts) {
      try { receipt.close(); }
      catch (IOException error) { if (failure == null) failure = error; else failure.addSuppressed(error); }
    }
    if (failure != null) throw failure;
  }
}
