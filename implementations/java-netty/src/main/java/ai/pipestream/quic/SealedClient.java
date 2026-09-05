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
import io.netty.handler.codec.TooLongFrameException;
import io.netty.incubator.codec.quic.QuicChannel;
import io.netty.incubator.codec.quic.QuicClientCodecBuilder;
import io.netty.incubator.codec.quic.QuicConnectionCloseEvent;
import io.netty.incubator.codec.quic.QuicSslContextBuilder;
import io.netty.incubator.codec.quic.QuicStreamChannel;
import io.netty.incubator.codec.quic.QuicStreamType;
import java.io.IOException;
import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Blocking, file-streaming Java producer for sealed work over Netty QUIC.
 * Operations are serialized per client and must not run on a Netty event loop.
 * Reconnection is explicit: replay retained declarations on a new client. This
 * API does not retry payloads, authenticate a producer label, or resume effects.
 */
public final class SealedClient implements AutoCloseable {
  private static final int MAX_TRACKED_ENTITIES = 65_536;
  private static final int MAX_CHUNKS = 65_536;
  private final NioEventLoopGroup group;
  private final Channel datagram;
  private final QuicChannel connection;
  private final QuicStreamChannel control;
  private final Inbox inbox;
  private final long operationNanos;
  private final SealedTransport.Limits limits;
  private final Map<Long, Scope> scopes = new HashMap<>();
  private final Map<SealedWork.EntityKey, Integer> states = new HashMap<>();
  private final Map<CheckpointKey, SealedTransport.Checkpoint> checkpoints = new HashMap<>();
  private String session;
  private UUID producer;
  private int declared;
  private Long rootCheckpoint;
  private boolean closed;

  private SealedClient(NioEventLoopGroup group, Channel datagram, QuicChannel connection,
      QuicStreamChannel control, Inbox inbox, long operationNanos, SealedTransport.Limits limits) {
    this.group = group; this.datagram = datagram; this.connection = connection;
    this.control = control; this.inbox = inbox; this.operationNanos = operationNanos; this.limits = limits;
  }

  /**
   * Connects with certificate validation and requires sealed work without a downgrade retry.
   * No 0-RTT data or server-originated work streams are accepted.
   * @param remote server address
   * @param caCertificate trusted CA PEM
   * @param serverName expected certificate DNS name
   * @param offered client resource limits
   * @param operationTimeout monotonic network-wait budget; does not interrupt blocking file I/O
   * @return connected client after exact capability validation
   * @throws Exception for invalid arguments, TLS, transport, timeout, or protocol refusal
   */
  public static SealedClient connect(InetSocketAddress remote, Path caCertificate, String serverName,
      SealedTransport.Limits offered, Duration operationTimeout) throws Exception {
    long timeout = operationTimeout.toNanos();
    if (timeout <= 0 || timeout > TimeUnit.HOURS.toNanos(1)) throw new IllegalArgumentException("operation timeout must be positive and at most one hour");
    byte[] offer = SealedTransport.capabilities(offered);
    var tls = QuicSslContextBuilder.forClient().trustManager(caCertificate.toFile()).applicationProtocols(Wire.ALPN).build();
    NioEventLoopGroup group = new NioEventLoopGroup(1);
    Channel datagram = null; QuicChannel connection = null;
    Inbox inbox = new Inbox();
    try {
      long deadline = System.nanoTime() + timeout;
      var bind = new Bootstrap().group(group).channel(NioDatagramChannel.class)
          .handler(new QuicClientCodecBuilder()
              .sslEngineProvider(channel -> tls.newEngine(channel.alloc(), serverName, remote.getPort()))
              .maxIdleTimeout(30_000, TimeUnit.MILLISECONDS).initialMaxData(8L << 20)
              .initialMaxStreamDataBidirectionalLocal(2L << 20).initialMaxStreamDataBidirectionalRemote(2L << 20)
              .initialMaxStreamsBidirectional(0).initialMaxStreamsUnidirectional(0).build())
          .bind(new InetSocketAddress(0));
      datagram = bind.channel();
      bind.get(remaining(deadline), TimeUnit.NANOSECONDS);
      connection = QuicChannel.newBootstrap(datagram)
          .handler(new ChannelInboundHandlerAdapter() {
            @Override public void userEventTriggered(ChannelHandlerContext context, Object event) {
              if (event instanceof QuicConnectionCloseEvent close && close.error() != 0) {
                inbox.failure.compareAndSet(null, close.isApplicationClose()
                    ? peerError(Integer.toUnsignedLong(close.error())) : new IOException("QUIC transport closed with error " + close.error()));
              }
              context.fireUserEventTriggered(event);
            }
            @Override public void channelInactive(ChannelHandlerContext context) { inbox.disconnected = true; }
            @Override public void exceptionCaught(ChannelHandlerContext context, Throwable failure) { inbox.failure.compareAndSet(null, failure); context.close(); }
          })
          .streamHandler(new ChannelInboundHandlerAdapter() {
            @Override public void channelActive(ChannelHandlerContext context) {
              inbox.failure.compareAndSet(null, Wire.entity("sealed server originated an unexpected stream"));
              context.channel().parent().close();
            }
          }).remoteAddress(remote).connect().get(remaining(deadline), TimeUnit.NANOSECONDS);
      QuicStreamChannel control = connection.createStream(QuicStreamType.BIDIRECTIONAL,
          new ChannelInitializer<QuicStreamChannel>() {
            @Override protected void initChannel(QuicStreamChannel channel) {
              channel.pipeline().addLast(new LengthFieldBasedFrameDecoder(Wire.MAX_CONTROL_FRAME + 5, 1, 4, 0, 0)).addLast(inbox);
            }
          }).get(remaining(deadline), TimeUnit.NANOSECONDS);
      if (control.streamId() != 0) throw Wire.frame("control stream must be stream zero");
      control.writeAndFlush(Unpooled.wrappedBuffer(offer)).get(remaining(deadline), TimeUnit.NANOSECONDS);
      Wire.ControlFrame response = inbox.next(deadline);
      if (response.type() != Wire.FRAME_CAPABILITIES) throw Wire.frame("server did not answer CAPABILITIES");
      SealedTransport.Limits selected = SealedTransport.response(response.payload(), offered);
      SealedClient result = new SealedClient(group, datagram, connection, control, inbox, timeout, selected);
      result.write(Wire.encodeStatus(new Wire.Status(0, Wire.CONNECTION_LEVEL, 0, null, 0)), deadline);
      return result;
    } catch (Exception failure) {
      if (connection != null) connection.close().syncUninterruptibly();
      if (datagram != null) datagram.close().syncUninterruptibly();
      group.shutdownGracefully(0, 1, TimeUnit.SECONDS).syncUninterruptibly();
      throw failure;
    }
  }

  /** Returns negotiated limits.
   * @return immutable selected limits
   */
  public SealedTransport.Limits limits() { return limits; }

  /**
   * Declares or replays a batch and verifies every ACK field before remembering membership.
   * @param request original declaration, with ACK clear
   * @return exactly correlated durable declaration ACK
   * @throws Exception for identity, sequence, capacity, transport, or ACK failure
   */
  public synchronized SealedWork.Declaration declare(SealedWork.Declaration request) throws Exception {
    return operation(deadline -> {
      byte[] encoded = SealedWork.encode(request);
      if ((request.flags() & SealedWork.ACK) != 0) throw Wire.entity("declaration request carries ACK");
      if (session == null) {
        if (request.scopeId() != 0 || request.sequence().signum() != 0) throw Wire.entity("attach with original root sequence zero");
      } else if (!session.equals(request.sessionId()) || !producer.equals(request.producerId())) throw Wire.entity("declaration changes session identity");
      Scope scope = scopes.get(request.scopeId());
      boolean fresh = scope == null;
      if (fresh) {
        int depth = 0;
        if (request.parent() != null) {
          Scope parent = scopes.get(request.parent().scopeId());
          if (parent == null || !parent.ids.contains(request.parent().entityId())) throw Wire.entity("parent declaration must be replayed first");
          depth = parent.depth + 1;
        }
        if (depth > limits.depth()) throw new ProtocolException(7, "PIPESTREAM_DEPTH_EXCEEDED", "scope exceeds selected depth");
        scope = new Scope(request.parent(), depth);
      }
      SealedWork.Declaration previous = scope.batches.get(request.sequence());
      if (previous != null) {
        if (!previous.equals(request)) throw Wire.entity("changed declaration replay");
      } else {
        if (!Objects.equals(scope.parent, request.parent()) || scope.sealed || !request.sequence().equals(BigInteger.valueOf(scope.batches.size()))) throw Wire.entity("invalid declaration binding or sequence");
        if (!scope.ids.isEmpty() && !request.entityIds().isEmpty() && request.entityIds().getFirst() <= scope.ids.last()) throw Wire.entity("declaration IDs must increase");
        if (declared + request.entityIds().size() > MAX_TRACKED_ENTITIES || scope.ids.size() + request.entityIds().size() > limits.entities()) throw Wire.limit("client declaration tracking budget exhausted");
        if ((request.flags() & SealedWork.SEAL) != 0) {
          List<Long> all = new ArrayList<>(scope.ids); all.addAll(request.entityIds());
          if (!MessageDigest.isEqual(request.sealDigest(), SealedWork.sealDigest(request.sessionId(), request.producerId(), request.scopeId(), request.parent(), all))) throw Wire.integrity("seal differs from declared membership");
        }
      }
      write(encoded, deadline);
      Wire.ControlFrame frame = receive(deadline);
      if (frame.type() != SealedWork.FRAME) throw Wire.entity("expected WORK_SET acknowledgement");
      SealedWork.Declaration acknowledgement = SealedWork.decodePayload(frame.payload());
      SealedWork.requireAcknowledgement(request, acknowledgement);
      if (previous == null) {
        scope.batches.put(request.sequence(), request);
        scope.ids.addAll(request.entityIds());
        scope.sealed = (request.flags() & SealedWork.SEAL) != 0;
        declared += request.entityIds().size();
      }
      if (fresh) scopes.put(request.scopeId(), scope);
      session = request.sessionId(); producer = request.producerId();
      return acknowledgement;
    });
  }

  /**
   * Streams a declared file without whole-payload buffering and waits for its processing result.
   * @param header validated identity and optional payload commitments; no chunk-info
   * @param payload regular file to stream
   * @return observed lifecycle statuses through COMPLETE, FAILED, or DEHYDRATING
   * @throws Exception for admission, payload mutation, transport, or response mismatch
   */
  public synchronized List<Wire.Status> send(SealedTransport.Header header, Path payload) throws Exception {
    return operation(deadline -> {
      if (header.chunk() != null) throw Wire.entity("use sendChunks for a chunked entity");
      Scope scope = sending(header);
      announce(header.key(), scope.depth, deadline);
      stream(header, payload, deadline);
      return processing(header.key(), scope.depth, deadline);
    });
  }

  /**
   * One file-backed chunk. The file contains just this chunk's payload bytes.
   * @param header header including chunk-info
   * @param payload file containing the chunk
   */
  public record FileChunk(SealedTransport.Header header, Path payload) {}

  /**
   * Streams all chunks in caller order on independent QUIC streams, with one entity lifecycle.
   * @param chunks complete chunk set, in any arrival order
   * @return processing statuses for the reassembled entity
   * @throws Exception for invalid chunk geometry, changed files, transport, or response mismatch
   */
  public synchronized List<Wire.Status> sendChunks(List<FileChunk> chunks) throws Exception {
    return operation(deadline -> {
      if (chunks.isEmpty() || chunks.size() > MAX_CHUNKS) throw Wire.limit("invalid local chunk count");
      List<FileChunk> files = List.copyOf(chunks);
      SealedTransport.Header first = files.getFirst().header();
      Scope scope = sending(first);
      TreeMap<BigInteger, BigInteger> ranges = new TreeMap<>();
      var indexes = new java.util.HashSet<BigInteger>();
      for (FileChunk chunk : files) {
        SealedTransport.Header header = chunk.header(); SealedTransport.header(header);
        if (header.chunk() == null || !header.key().equals(first.key()) || !Objects.equals(header.parent(), first.parent())
            || header.layer() != first.layer() || !Objects.equals(header.contentType(), first.contentType()) || !header.metadata().equals(first.metadata())
            || !header.chunk().total().equals(BigInteger.valueOf(files.size())) || !indexes.add(header.chunk().index())) throw Wire.entity("inconsistent chunk identity or count");
        BigInteger size = BigInteger.valueOf(Files.size(chunk.payload()));
        if (ranges.putIfAbsent(header.chunk().offset(), size) != null) throw Wire.entity("duplicate chunk range");
      }
      BigInteger next = BigInteger.ZERO;
      for (var range : ranges.entrySet()) {
        if (!range.getKey().equals(next)) throw Wire.entity("chunk ranges overlap or have gaps");
        next = next.add(range.getValue());
        if (next.compareTo(SealedCbor.MAX_UINT) > 0) throw Wire.entity("chunk end exceeds uint64");
      }
      announce(first.key(), scope.depth, deadline);
      for (FileChunk chunk : files) stream(chunk.header(), chunk.payload(), deadline);
      return processing(first.key(), scope.depth, deadline);
    });
  }

  /**
   * Confirms a sealed child scope and validates its parent's rehydration lifecycle.
   * @param scopeId child scope whose complete statuses this client has observed
   * @return the exact server-confirmed status digest
   * @throws Exception for incomplete membership, changed digest, wrong parent, or transport failure
   */
  public synchronized SealedScope.Digest closeScope(long scopeId) throws Exception {
    return operation(deadline -> {
      Scope scope = scopes.get(scopeId);
      if (scope == null || scope.parent == null || !scope.sealed || scope.closed) throw SealedScope.invalid("child scope is absent, unsealed, or already closed");
      List<SealedScope.Terminal> leaves = new ArrayList<>();
      for (long id : scope.ids) {
        Integer state = states.get(new SealedWork.EntityKey(scopeId, id));
        if (state == null || (state != 3 && state != 4)) throw Wire.entity("scope has outstanding declared work");
        leaves.add(new SealedScope.Terminal(id, state));
      }
      for (Scope child : scopes.values()) if (child.parent != null && child.parent.scopeId() == scopeId && !child.closed) throw Wire.entity("descendant scope is not closed");
      SealedScope.Digest digest = SealedScope.summarize(scopeId, leaves);
      write(SealedScope.encode(digest), deadline);
      Wire.ControlFrame response = receive(deadline);
      if (response.type() != SealedScope.FRAME || !SealedScope.decode(Wire.encodeControl(response.type(), response.payload())).equals(digest)) throw Wire.entity("scope digest acknowledgement differs");
      int parentDepth = scope.depth - 1;
      Wire.Status status = expectStatus(scope.parent, parentDepth, deadline);
      if (digest.failed().signum() == 0) {
        if (status.state() != 7) throw Wire.entity("expected REHYDRATING parent");
        states.put(scope.parent, 7);
        status = expectStatus(scope.parent, parentDepth, deadline);
      }
      if (status.state() != 3 && status.state() != 4) throw Wire.entity("expected terminal parent result");
      if (digest.failed().signum() != 0 && status.state() != 4) throw Wire.entity("STRICT parent cannot succeed with failed children");
      states.put(scope.parent, status.state()); scope.closed = true;
      return digest;
    });
  }

  /**
   * Queries a scoped barrier and correlates both scope and parent identity.
   * @param scopeId declared child scope
   * @return current server barrier status
   * @throws Exception for a missing scope, changed response, or transport failure
   */
  public synchronized SealedTransport.Barrier barrier(long scopeId) throws Exception {
    return operation(deadline -> {
      Scope scope = scopes.get(scopeId);
      if (scope == null || scope.parent == null) throw SealedScope.invalid("unknown child scope");
      write(SealedTransport.barrier(new SealedTransport.Barrier(scopeId, scope.parent.entityId(), false)), deadline);
      Wire.ControlFrame response = receive(deadline);
      if (response.type() != 0x55) throw Wire.frame("expected BARRIER");
      SealedTransport.Barrier barrier = SealedTransport.barrier(response.payload());
      if (barrier.scopeId() != scopeId || barrier.parentId() != scope.parent.entityId()) throw Wire.entity("barrier response identity differs");
      return barrier;
    });
  }

  /**
   * Requests a whole-scope checkpoint and checks all ACK fields, including optional fields.
   * All currently declared entities in the scope must have an observed admission status.
   * @param request checkpoint with ACK clear
   * @return exactly correlated ACK, never a timeout converted into completion
   * @throws Exception for invalid admission, changed replay, timeout, or response mismatch
   */
  public synchronized SealedTransport.Checkpoint checkpoint(SealedTransport.Checkpoint request) throws Exception {
    return operation(deadline -> {
      if (request.flags() != 0) throw Wire.entity("checkpoint request carries ACK");
      long scopeId = request.scopeId() == null ? 0 : request.scopeId();
      Scope scope = scopes.get(scopeId);
      if (scope == null) throw SealedScope.invalid("checkpoint scope is absent");
      for (long id : scope.ids) if (!states.containsKey(new SealedWork.EntityKey(scopeId, id))) throw Wire.entity("checkpoint requires observed admission for every declared entity");
      CheckpointKey key = new CheckpointKey(scopeId, request.sequence());
      SealedTransport.Checkpoint previous = checkpoints.get(key);
      if (previous != null && !previous.equals(request)) throw Wire.entity("checkpoint sequence was reused with changed fields");
      if (previous == null && checkpoints.size() >= 1024) throw Wire.limit("checkpoint history exhausted");
      write(SealedTransport.checkpoint(request), deadline);
      Wire.ControlFrame response = receive(deadline);
      if (response.type() != Wire.FRAME_CHECKPOINT || !SealedTransport.checkpoint(response.payload()).equals(request.acknowledgement())) throw Wire.entity("checkpoint acknowledgement differs");
      if (!scope.sealed || scope.ids.last() != request.lastId()) throw Wire.entity("checkpoint acknowledged an unsealed or incorrect cut");
      requireResolved(scopeId);
      checkpoints.put(key, request);
      if (scopeId == 0) rootCheckpoint = request.lastId();
      return request.acknowledgement();
    });
  }

  /**
   * Completes GOAWAY only against an acknowledged sealed root checkpoint.
   * @param lastId inclusive largest root entity identifier
   * @throws Exception for incomplete work or a changed GOAWAY response
   */
  public synchronized void goaway(long lastId) throws Exception {
    operation(deadline -> {
      Scope root = scopes.get(0L);
      if (root == null || !root.sealed || root.ids.last() != lastId || rootCheckpoint == null || rootCheckpoint != lastId) throw Wire.entity("GOAWAY requires the acknowledged sealed root cut");
      for (Scope scope : scopes.values()) if (scope.parent != null && !scope.closed) throw Wire.entity("GOAWAY has unclosed descendants");
      requireResolved(0);
      write(Wire.encodeGoaway(lastId), deadline);
      Wire.ControlFrame response = receive(deadline);
      if (response.type() != Wire.FRAME_GOAWAY || Wire.decodeGoaway(response.payload()) != lastId) throw Wire.entity("GOAWAY acknowledgement differs");
      control.writeAndFlush(Unpooled.EMPTY_BUFFER).addListener(QuicStreamChannel.SHUTDOWN_OUTPUT).get(remaining(deadline), TimeUnit.NANOSECONDS);
      return null;
    });
    close();
  }

  /** Disconnects without claiming completion or retrying any application work. */
  @Override public synchronized void close() {
    if (connection.eventLoop().inEventLoop()) throw new IllegalStateException("blocking client operation on its Netty event loop");
    if (closed) return;
    closed = true;
    connection.close().syncUninterruptibly(); datagram.close().syncUninterruptibly();
    group.shutdownGracefully(0, 1, TimeUnit.SECONDS).syncUninterruptibly();
    inbox.frames.clear(); inbox.bytes.set(0);
  }

  private Scope sending(SealedTransport.Header header) throws ProtocolException {
    SealedTransport.header(header);
    Scope scope = scopes.get(header.key().scopeId());
    if (scope == null || !scope.ids.contains(header.key().entityId()) || !Objects.equals(scope.parent, header.parent())
        || states.containsKey(header.key())) throw Wire.entity("payload is undeclared, repeated, or changes its parent");
    String explicit = header.metadata().get("pipestream.session-id");
    if (explicit != null && !explicit.equals(session)) throw Wire.entity("payload changes session identity");
    return scope;
  }

  private void requireResolved(long scopeId) throws ProtocolException {
    for (long id : scopes.get(scopeId).ids) {
      Integer state = states.get(new SealedWork.EntityKey(scopeId, id));
      if (state == null || (state != 3 && state != 4)) throw Wire.entity("checkpoint acknowledged unresolved declared work");
    }
    for (Scope child : scopes.values()) {
      if (child.parent != null && child.parent.scopeId() == scopeId && !child.closed) throw Wire.entity("checkpoint acknowledged an unclosed descendant");
    }
  }

  private void announce(SealedWork.EntityKey key, int depth, long deadline) throws Exception {
    write(Wire.encodeStatus(new Wire.Status(1, key.entityId(), key.scopeId(), null, depth)), deadline);
  }

  private void stream(SealedTransport.Header header, Path payload, long deadline) throws Exception {
    if (!Files.isRegularFile(payload)) throw new IOException("payload must be a regular file");
    long expected = Files.size(payload);
    if (header.payloadLength() != null && !header.payloadLength().equals(BigInteger.valueOf(expected))) throw Wire.entity("payload file length differs from header");
    QuicStreamChannel stream = connection.createStream(QuicStreamType.UNIDIRECTIONAL, new ChannelInboundHandlerAdapter()).get(remaining(deadline), TimeUnit.NANOSECONDS);
    try (var input = Files.newInputStream(payload)) {
      stream.writeAndFlush(Unpooled.wrappedBuffer(SealedTransport.header(header))).get(remaining(deadline), TimeUnit.NANOSECONDS);
      MessageDigest hash = SealedWork.sha256(); long count = 0; byte[] buffer = new byte[8192]; int n;
      while ((n = input.read(buffer)) != -1) {
        count = Math.addExact(count, n);
        if (count > expected) throw Wire.entity("payload grew during streaming");
        hash.update(buffer, 0, n);
        stream.writeAndFlush(Unpooled.copiedBuffer(buffer, 0, n)).get(remaining(deadline), TimeUnit.NANOSECONDS);
      }
      if (count != expected || (header.checksum() != null && !MessageDigest.isEqual(header.checksum(), hash.digest()))) throw Wire.integrity("payload length or checksum changed during streaming");
      stream.writeAndFlush(Unpooled.EMPTY_BUFFER).addListener(QuicStreamChannel.SHUTDOWN_OUTPUT).get(remaining(deadline), TimeUnit.NANOSECONDS);
    } catch (Exception failure) { stream.close(); throw failure; }
  }

  private List<Wire.Status> processing(SealedWork.EntityKey key, int depth, long deadline) throws Exception {
    Wire.Status admitted = expectStatus(key, depth, deadline);
    if (admitted.state() == 4 || admitted.state() == 6) {
      states.put(key, admitted.state()); return List.of(admitted);
    }
    if (admitted.state() != 2) throw Wire.entity("first processing status must be PROCESSING");
    states.put(key, 2);
    List<Wire.Status> observed = new ArrayList<>(); observed.add(admitted);
    int previous = admitted.state();
    while (true) {
      if (observed.size() >= 128) throw Wire.limit("per-operation status history exhausted");
      Wire.Status status = expectStatus(key, depth, deadline);
      boolean terminal = status.state() == 3 || status.state() == 4 || status.state() == 6;
      if ((previous == 5 && status.state() != 2) || (previous == 2 && !terminal && status.state() != 5)) throw Wire.entity("invalid processing result progression");
      observed.add(status); states.put(key, status.state());
      if (terminal) return List.copyOf(observed);
      previous = status.state();
    }
  }

  private Wire.Status expectStatus(SealedWork.EntityKey key, int depth, long deadline) throws Exception {
    Wire.ControlFrame frame = receive(deadline);
    if (frame.type() != Wire.FRAME_STATUS) throw Wire.frame("expected STATUS");
    Wire.Status status = SealedTransport.status(frame.payload());
    if (status.entityId() != key.entityId() || status.scopeId() != key.scopeId() || status.depth() != depth) throw Wire.entity("STATUS changes entity, scope, or depth");
    return status;
  }

  private Wire.ControlFrame receive(long deadline) throws Exception {
    while (true) {
      Wire.ControlFrame frame = inbox.next(deadline);
      if (frame.type() == 0x82 || frame.type() == 0x84) throw Wire.layerUnsupported("Layer 2 frame in sealed session");
      if (frame.type() == Wire.FRAME_CAPABILITIES) throw Wire.frame("duplicate CAPABILITIES");
      if (List.of(0x50, 0x54, 0x55, 0x56, 0x81, 0x83).contains(frame.type())) return frame;
    }
  }

  private void write(byte[] frame, long deadline) throws Exception {
    control.writeAndFlush(Unpooled.wrappedBuffer(frame)).get(remaining(deadline), TimeUnit.NANOSECONDS);
  }

  private <T> T operation(Operation<T> operation) throws Exception {
    if (connection.eventLoop().inEventLoop()) throw new IllegalStateException("blocking client operation on its Netty event loop");
    if (closed) throw new IOException("sealed client is closed");
    try { return operation.run(System.nanoTime() + operationNanos); }
    catch (Exception failure) {
      int error = failure instanceof ProtocolException protocol ? (int) protocol.errorCode() : (int) Wire.ERROR_ENTITY_INVALID;
      connection.close(true, error, Unpooled.copiedBuffer("sealed client operation refused", StandardCharsets.US_ASCII));
      close(); throw failure;
    }
  }

  private static long remaining(long deadline) throws TimeoutException {
    long left = deadline - System.nanoTime(); if (left <= 0) throw new TimeoutException("sealed client operation deadline expired"); return left;
  }

  private static ProtocolException peerError(long code) {
    String name = switch ((int) code) {
      case 4 -> "PIPESTREAM_INTEGRITY_ERROR"; case 5 -> "PIPESTREAM_ENTITY_INVALID";
      case 6 -> "PIPESTREAM_LIMIT_EXCEEDED"; case 7 -> "PIPESTREAM_DEPTH_EXCEEDED";
      case 9 -> "PIPESTREAM_SCOPE_INVALID"; case 12 -> "PIPESTREAM_LAYER_UNSUPPORTED";
      case 13 -> "PIPESTREAM_FRAME_ERROR"; case 14 -> "PIPESTREAM_CHECKPOINT_TIMEOUT";
      case 15 -> "PIPESTREAM_EXTENSION_UNSUPPORTED"; default -> "PIPESTREAM_PEER_ERROR";
    };
    return new ProtocolException(code, name, "peer closed the connection");
  }

  private static final class Scope {
    final SealedWork.EntityKey parent; final int depth;
    final TreeSet<Long> ids = new TreeSet<>();
    final Map<BigInteger, SealedWork.Declaration> batches = new HashMap<>();
    boolean sealed, closed;
    Scope(SealedWork.EntityKey parent, int depth) { this.parent = parent; this.depth = depth; }
  }

  private record CheckpointKey(long scope, BigInteger sequence) {}
  @FunctionalInterface private interface Operation<T> { T run(long deadline) throws Exception; }

  private static final class Inbox extends SimpleChannelInboundHandler<ByteBuf> {
    final ArrayBlockingQueue<byte[]> frames = new ArrayBlockingQueue<>(128);
    final AtomicLong bytes = new AtomicLong();
    final AtomicReference<Throwable> failure = new AtomicReference<>();
    volatile boolean disconnected;
    boolean capabilities;

    @Override protected void channelRead0(ChannelHandlerContext context, ByteBuf input) {
      if (failure.get() != null) return;
      try {
        if (input.readableBytes() > Wire.MAX_CONTROL_FRAME + 5) throw Wire.limit("control frame exceeds local limit");
        byte[] encoded = new byte[input.readableBytes()]; input.readBytes(encoded);
        Wire.ControlFrame frame = Wire.decodeControl(encoded);
        if (!capabilities) {
          if (frame.type() != Wire.FRAME_CAPABILITIES) throw Wire.frame("first response must be CAPABILITIES");
          capabilities = true;
        } else if (frame.type() == Wire.FRAME_CAPABILITIES) throw Wire.frame("duplicate CAPABILITIES");
        if (frame.type() == Wire.FRAME_STATUS && SealedTransport.status(frame.payload()).state() == 0) return;
        long charged = bytes.addAndGet(encoded.length);
        if (charged > 4L << 20 || !frames.offer(encoded)) {
          bytes.addAndGet(-encoded.length); throw Wire.limit("control response backlog exhausted");
        }
      } catch (Exception error) { fail(context, error); }
    }

    @Override public void channelInactive(ChannelHandlerContext context) {
      // A control-stream FIN can precede the connection's named application close.
      context.channel().parent().closeFuture().addListener(ignored -> disconnected = true);
    }
    @Override public void exceptionCaught(ChannelHandlerContext context, Throwable error) {
      fail(context, error instanceof TooLongFrameException ? Wire.limit("control frame exceeds local limit") : error);
    }
    private void fail(ChannelHandlerContext context, Throwable error) {
      failure.compareAndSet(null, error);
      ((QuicChannel) context.channel().parent()).close(true,
          error instanceof ProtocolException protocol ? (int) protocol.errorCode() : (int) Wire.ERROR_FRAME,
          Unpooled.copiedBuffer("invalid sealed response", StandardCharsets.US_ASCII));
    }

    Wire.ControlFrame next(long deadline) throws Exception {
      while (true) {
        Throwable error = failure.get();
        if (error != null) {
          if (error instanceof Exception exception) throw exception;
          throw new IOException("QUIC input failed", error);
        }
        byte[] encoded = frames.poll(Math.min(remaining(deadline), TimeUnit.MILLISECONDS.toNanos(25)), TimeUnit.NANOSECONDS);
        if (encoded != null) { bytes.addAndGet(-encoded.length); return Wire.decodeControl(encoded); }
        if (failure.get() != null) continue;
        if (disconnected) throw new IOException("peer disconnected before the expected response");
      }
    }
  }
}
