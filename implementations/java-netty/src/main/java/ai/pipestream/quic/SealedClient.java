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
import io.netty.handler.ssl.SslHandshakeCompletionEvent;
import io.netty.incubator.codec.quic.QuicChannel;
import io.netty.incubator.codec.quic.QuicClientCodecBuilder;
import io.netty.incubator.codec.quic.QuicConnectionCloseEvent;
import io.netty.incubator.codec.quic.QuicSslContextBuilder;
import io.netty.incubator.codec.quic.QuicStreamChannel;
import io.netty.incubator.codec.quic.QuicStreamType;
import java.io.IOException;
import java.io.ByteArrayInputStream;
import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Blocking, file-streaming Java producer for sealed work over Netty QUIC.
 * Operations are serialized per client and must not run on a Netty event loop.
 * Ephemeral clients replay declarations explicitly; durable clients retain their
 * verified observations and attach using the original root declaration. Neither
 * mode retries payloads, authenticates a producer label, or resumes external effects.
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
  private final SealedProducerJournal journal;
  private final Map<Long, Scope> scopes = new HashMap<>();
  private final Map<SealedWork.EntityKey, Integer> states = new HashMap<>();
  private final Map<CheckpointKey, SealedTransport.Checkpoint> checkpoints = new HashMap<>();
  private final Map<CheckpointKey, SealedWork.Declaration> unacknowledged = new LinkedHashMap<>();
  private final Map<CheckpointKey, SealedTransport.Checkpoint> unacknowledgedCheckpoints = new LinkedHashMap<>();
  private final Set<SealedWork.EntityKey> attemptedInputs = new HashSet<>();
  private final Set<SealedWork.EntityKey> unresolvedInputs = new HashSet<>();
  private final Set<Long> unresolvedScopes = new HashSet<>();
  private String session;
  private UUID producer;
  private int declared;
  private Long rootCheckpoint;
  private boolean closed;

  private SealedClient(NioEventLoopGroup group, Channel datagram, QuicChannel connection,
      QuicStreamChannel control, Inbox inbox, long operationNanos, SealedTransport.Limits limits, SealedProducerJournal journal) {
    this.group = group; this.datagram = datagram; this.connection = connection;
    this.control = control; this.inbox = inbox; this.operationNanos = operationNanos; this.limits = limits;
    this.journal = journal;
  }

  /**
   * Producer-owned local journal configuration, separate from server storage.
   * @param database local database file, never shared by concurrent producers
   * @param maxOperations maximum retained operation identities
   * @param maxBytes logical request and reserved observation bytes
   * @param fileLimits physical SQLite file-length limits, not filesystem-block preallocation
   */
  public record Durability(Path database, int maxOperations, long maxBytes, SealedSessionStore.FileLimits fileLimits) {
    /** Checks mandatory fields and supported quota ranges. */
    public Durability {
      Objects.requireNonNull(database, "database"); Objects.requireNonNull(fileLimits, "fileLimits");
      new SealedProducerJournal.Limits(maxOperations, maxBytes);
    }
    /**
     * Uses bounded defaults for a new or identically configured journal.
     * @param database producer database path
     * @return immutable journal configuration
     */
    public static Durability at(Path database) {
      return new Durability(database, 131_072, 256L << 20,
          new SealedSessionStore.FileLimits(512L << 20, 64L << 20, 64L << 20, 512L << 10));
    }
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
    return connectConfigured(remote, caCertificate, serverName, offered, operationTimeout, null);
  }

  /**
   * Connects using a durable producer journal bound to the DNS name and exact CA PEM bytes.
   * Restores verified observations and replays the original root declaration to attach.
   * It never resends payloads or treats an earlier checkpoint ACK as a current-connection cut.
   * Missing file length/checksum fields are filled before journaling and streaming a payload.
   * @param remote server address; address/port changes do not change the retained trust binding
   * @param caCertificate trusted CA PEM, at most one MiB; changed trust bytes require another journal
   * @param serverName expected certificate DNS name, also part of the immutable binding
   * @param offered client resource limits
   * @param operationTimeout network-wait budget; does not interrupt blocking local I/O
   * @param durability immutable local journal policy
   * @return attached client, or a fresh client if the journal has no root declaration yet
   * @throws Exception for binding, storage, TLS, protocol, timeout or already-acknowledged shutdown
   */
  public static SealedClient connectDurable(InetSocketAddress remote, Path caCertificate, String serverName,
      SealedTransport.Limits offered, Duration operationTimeout, Durability durability) throws Exception {
    return connectConfigured(remote, caCertificate, serverName, offered, operationTimeout, Objects.requireNonNull(durability));
  }

  private static SealedClient connectConfigured(InetSocketAddress remote, Path caCertificate, String serverName,
      SealedTransport.Limits offered, Duration operationTimeout, Durability durability) throws Exception {
    long timeout = operationTimeout.toNanos();
    if (timeout <= 0 || timeout > TimeUnit.HOURS.toNanos(1)) throw new IllegalArgumentException("operation timeout must be positive and at most one hour");
    byte[] offer = SealedTransport.capabilities(offered);
    byte[] trusted = null;
    if (durability != null) {
      if (serverName == null || serverName.isBlank() || serverName.length() > 253) throw new IllegalArgumentException("invalid durable peer name");
      try (var input = Files.newInputStream(caCertificate)) { trusted = input.readNBytes((1 << 20) + 1); }
      if (trusted.length == 0 || trusted.length > 1 << 20) throw new IOException("durable CA file exceeds local bound or is empty");
    }
    var builder = QuicSslContextBuilder.forClient().applicationProtocols(Wire.ALPN);
    if (trusted == null) builder.trustManager(caCertificate.toFile());
    else {
      var certificates = java.security.cert.CertificateFactory.getInstance("X.509")
          .generateCertificates(new ByteArrayInputStream(trusted)).toArray(java.security.cert.X509Certificate[]::new);
      if (certificates.length == 0) throw new IOException("durable CA file contains no certificates");
      builder.trustManager(certificates);
    }
    var tls = builder.build();
    NioEventLoopGroup group = new NioEventLoopGroup(1);
    Channel datagram = null; QuicChannel connection = null;
    SealedProducerJournal journal = null;
    Inbox inbox = new Inbox();
    var authenticated = new CompletableFuture<Void>();
    try {
      if (durability != null) {
        var hash = SealedWork.sha256();
        hash.update("pipestream-java-producer-peer-v1".getBytes(StandardCharsets.US_ASCII));
        hash.update(Wire.ALPN.getBytes(StandardCharsets.US_ASCII));
        hash.update(ByteBuffer.allocate(4).putInt(SealedWork.EXTENSION).array());
        byte[] name = SealedCbor.utf8(serverName);
        hash.update(ByteBuffer.allocate(4).putInt(name.length).array()); hash.update(name); hash.update(trusted);
        journal = SealedProducerJournal.open(durability.database(), hash.digest(),
            new SealedProducerJournal.Limits(durability.maxOperations(), durability.maxBytes()), durability.fileLimits());
      }
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
              if (event instanceof SslHandshakeCompletionEvent handshake) {
                try {
                  if (!handshake.isSuccess()) throw new IOException("QUIC TLS handshake failed", handshake.cause());
                  TlsPeerIdentity.verify(((QuicChannel) context.channel()).sslEngine().getSession(), serverName);
                  authenticated.complete(null);
                } catch (Exception failure) {
                  authenticated.completeExceptionally(failure); inbox.failure.compareAndSet(null, failure); context.close();
                }
              }
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
      authenticated.get(remaining(deadline), TimeUnit.NANOSECONDS);
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
      SealedClient result = new SealedClient(group, datagram, connection, control, inbox, timeout, selected, journal);
      result.write(Wire.encodeStatus(new Wire.Status(0, Wire.CONNECTION_LEVEL, 0, null, 0)), deadline);
      SealedWork.Declaration root = result.restore();
      if (root != null) result.declare(root);
      return result;
    } catch (Exception failure) {
      if (connection != null) connection.close().syncUninterruptibly();
      if (datagram != null) datagram.close().syncUninterruptibly();
      group.shutdownGracefully(0, 1, TimeUnit.SECONDS).syncUninterruptibly();
      if (journal != null) try { journal.close(); } catch (Exception close) { failure.addSuppressed(close); }
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
      Scope scope = prepareDeclaration(request);
      var entry = intent(SealedProducerJournal.Kind.DECLARATION, sequenceKey(request.scopeId(), request.sequence()), encoded, 4096);
      if (entry != null && !entry.resolved()) unacknowledged.put(new CheckpointKey(request.scopeId(), request.sequence()), request);
      write(encoded, deadline);
      Wire.ControlFrame frame = receive(deadline);
      if (frame.type() != SealedWork.FRAME) throw Wire.entity("expected WORK_SET acknowledgement");
      SealedWork.Declaration acknowledgement = SealedWork.decodePayload(frame.payload());
      SealedWork.requireAcknowledgement(request, acknowledgement);
      observe(entry, SealedWork.encode(acknowledgement), true);
      rememberDeclaration(request, scope);
      return acknowledgement;
    });
  }

  private Scope prepareDeclaration(SealedWork.Declaration request) throws ProtocolException {
    SealedWork.encode(request);
    if ((request.flags() & SealedWork.ACK) != 0) throw Wire.entity("declaration request carries ACK");
    if (session == null) {
      if (request.scopeId() != 0 || request.sequence().signum() != 0) throw Wire.entity("attach with original root sequence zero");
    } else if (!session.equals(request.sessionId()) || !producer.equals(request.producerId())) throw Wire.entity("declaration changes session identity");
    Scope scope = scopes.get(request.scopeId());
    if (scope == null) {
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
    return scope;
  }

  private void rememberDeclaration(SealedWork.Declaration request, Scope scope) {
    if (!scope.batches.containsKey(request.sequence())) {
      scope.batches.put(request.sequence(), request);
      scope.ids.addAll(request.entityIds());
      scope.sealed = (request.flags() & SealedWork.SEAL) != 0;
      declared += request.entityIds().size();
    }
    scopes.put(request.scopeId(), scope);
    session = request.sessionId(); producer = request.producerId();
    unacknowledged.remove(new CheckpointKey(request.scopeId(), request.sequence()));
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
      var files = prepareInputs(List.of(new FileChunk(header, payload)));
      var entry = beginInput(files);
      announce(header.key(), scope.depth, deadline);
      stream(files.files().getFirst().header(), payload, deadline);
      return processing(header.key(), scope.depth, deadline, entry);
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
      var prepared = prepareInputs(files);
      var entry = beginInput(prepared);
      announce(first.key(), scope.depth, deadline);
      for (FileChunk chunk : prepared.files()) stream(chunk.header(), chunk.payload(), deadline);
      return processing(first.key(), scope.depth, deadline, entry);
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
      var entry = intent(SealedProducerJournal.Kind.SCOPE, scopeKey(scopeId), SealedScope.encode(digest), 128);
      if (entry != null) unresolvedScopes.add(scopeId);
      write(SealedScope.encode(digest), deadline);
      Wire.ControlFrame response = receive(deadline);
      if (response.type() != SealedScope.FRAME || !SealedScope.decode(Wire.encodeControl(response.type(), response.payload())).equals(digest)) throw Wire.entity("scope digest acknowledgement differs");
      // Replaying the digest must not erase a parent status already observed on
      // an earlier connection if this connection dies before returning a status.
      if (entry == null || entry.observation().length <= 1) entry = observe(entry, new byte[]{1}, false);
      int parentDepth = scope.depth - 1;
      Wire.Status status = expectStatus(scope.parent, parentDepth, deadline);
      if (digest.failed().signum() == 0) {
        if (status.state() != 7) throw Wire.entity("expected REHYDRATING parent");
        entry = observe(entry, scopeObservation(status), false);
        states.put(scope.parent, 7);
        status = expectStatus(scope.parent, parentDepth, deadline);
      }
      if (status.state() != 3 && status.state() != 4) throw Wire.entity("expected terminal parent result");
      if (digest.failed().signum() != 0 && status.state() != 4) throw Wire.entity("STRICT parent cannot succeed with failed children");
      observe(entry, scopeObservation(status), true);
      states.put(scope.parent, status.state()); scope.closed = true;
      unresolvedScopes.remove(scopeId);
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
      var entry = intent(SealedProducerJournal.Kind.CHECKPOINT, sequenceKey(scopeId, request.sequence()), SealedTransport.checkpoint(request), 4096);
      if (entry != null) checkpoints.put(key, request);
      if (entry != null && !entry.resolved()) unacknowledgedCheckpoints.put(key, request);
      write(SealedTransport.checkpoint(request), deadline);
      Wire.ControlFrame response = receive(deadline);
      if (response.type() != Wire.FRAME_CHECKPOINT || !SealedTransport.checkpoint(response.payload()).equals(request.acknowledgement())) throw Wire.entity("checkpoint acknowledgement differs");
      if (!scope.sealed || scope.ids.last() != request.lastId()) throw Wire.entity("checkpoint acknowledged an unsealed or incorrect cut");
      requireResolved(scopeId);
      observe(entry, SealedTransport.checkpoint(request.acknowledgement()), true);
      unacknowledgedCheckpoints.remove(key);
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
      var entry = intent(SealedProducerJournal.Kind.SHUTDOWN, scopeKey(0), Wire.encodeGoaway(lastId), 32);
      write(Wire.encodeGoaway(lastId), deadline);
      Wire.ControlFrame response = receive(deadline);
      if (response.type() != Wire.FRAME_GOAWAY || Wire.decodeGoaway(response.payload()) != lastId) throw Wire.entity("GOAWAY acknowledgement differs");
      observe(entry, Wire.encodeGoaway(lastId), true);
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
    if (journal != null) try { journal.close(); }
    catch (Exception failure) { throw new IllegalStateException("producer journal could not close", failure); }
  }

  private Scope sending(SealedTransport.Header header) throws ProtocolException {
    SealedTransport.header(header);
    Scope scope = scopes.get(header.key().scopeId());
    if (scope == null || !scope.ids.contains(header.key().entityId()) || !Objects.equals(scope.parent, header.parent())
        || states.containsKey(header.key()) || attemptedInputs.contains(header.key())) throw Wire.entity("payload is undeclared, repeated, unresolved from an earlier attempt, or changes its parent");
    String explicit = header.metadata().get("pipestream.session-id");
    if (explicit != null && !explicit.equals(session)) throw Wire.entity("payload changes session identity");
    return scope;
  }

  /**
   * Returns inputs whose final processing outcome was not durably observed.
   * These may already be admitted at the server; the list does not authorize retry.
   * Ephemeral clients do not retain this durable-intent inventory.
   * @return immutable scope/id-ordered identities, also available after disconnect
   */
  public synchronized List<SealedWork.EntityKey> unresolvedInputs() {
    return unresolvedInputs.stream().sorted(java.util.Comparator.comparingLong(SealedWork.EntityKey::scopeId)
        .thenComparingLong(SealedWork.EntityKey::entityId)).toList();
  }

  /**
   * Returns original declaration requests whose ACKs remain unobserved.
   * This inventory is populated only in durable mode.
   * @return immutable requests in original journal order, suitable for explicit exact replay
   */
  public synchronized List<SealedWork.Declaration> declarationsAwaitingAcknowledgement() {
    return List.copyOf(unacknowledged.values());
  }

  /**
   * Returns checkpoint requests whose exact ACKs remain unobserved.
   * Replaying one starts a new connection-local wait; it does not undo an earlier timeout.
   * This inventory is populated only in durable mode.
   * @return immutable original requests, including optional-field presence
   */
  public synchronized List<SealedTransport.Checkpoint> checkpointsAwaitingAcknowledgement() {
    return List.copyOf(unacknowledgedCheckpoints.values());
  }

  /**
   * Returns child closures whose final parent outcomes remain unobserved.
   * This inventory is populated only in durable mode.
   * @return immutable sorted child scope IDs, suitable for exact closure replay
   */
  public synchronized List<Long> scopesAwaitingClosure() { return unresolvedScopes.stream().sorted().toList(); }

  /**
   * Returns the last verified lifecycle observation for a declared entity.
   * An absent value does not mean the receiver has not admitted the payload.
   * @param key scope-qualified entity identity
   * @return the observed status, including nonterminal states, or empty
   */
  public synchronized Optional<Wire.Status> observedStatus(SealedWork.EntityKey key) {
    Integer state = states.get(key); Scope scope = scopes.get(key.scopeId());
    return state == null || scope == null ? Optional.empty()
        : Optional.of(new Wire.Status(state, key.entityId(), key.scopeId(), null, scope.depth));
  }

  private SealedProducerInputs.Prepared prepareInputs(List<FileChunk> files) throws Exception {
    return journal == null ? new SealedProducerInputs.Prepared(files, new byte[0]) : SealedProducerInputs.prepare(files);
  }

  private SealedProducerJournal.Entry beginInput(SealedProducerInputs.Prepared prepared) throws Exception {
    if (journal == null) return null;
    var key = prepared.files().getFirst().header().key();
    var entry = intent(SealedProducerJournal.Kind.INPUT, entityKey(key), prepared.descriptor(), 32);
    if (entry.revision() != 0 || attemptedInputs.contains(key)) throw Wire.entity("durable input intent already exists; no implicit retry");
    attemptedInputs.add(key); unresolvedInputs.add(key);
    return entry;
  }

  private SealedProducerJournal.Entry intent(SealedProducerJournal.Kind kind, byte[] identity, byte[] request, int capacity) throws Exception {
    return journal == null ? null : journal.begin(kind, identity, request, capacity);
  }

  private SealedProducerJournal.Entry observe(SealedProducerJournal.Entry entry, byte[] evidence, boolean resolved) throws Exception {
    return entry == null ? null : journal.observe(entry.id(), entry.revision(), evidence, resolved);
  }

  private static byte[] entityKey(SealedWork.EntityKey key) {
    return ByteBuffer.allocate(8).putInt((int) key.scopeId()).putInt((int) key.entityId()).array();
  }

  private static byte[] scopeKey(long scope) { return ByteBuffer.allocate(4).putInt((int) scope).array(); }

  private static byte[] sequenceKey(long scope, BigInteger sequence) {
    return ByteBuffer.allocate(12).putInt((int) scope).putLong(sequence.longValue()).array();
  }

  private static byte[] scopeObservation(Wire.Status status) {
    byte[] frame = Wire.encodeStatus(status);
    return ByteBuffer.allocate(frame.length + 1).put((byte) 2).put(frame).array();
  }

  private static Wire.Status restoredStatus(byte[] encoded, SealedWork.EntityKey key, int depth) throws ProtocolException {
    var frame = Wire.decodeControl(encoded);
    if (frame.type() != Wire.FRAME_STATUS) throw Wire.integrity("producer observation is not STATUS");
    var status = SealedTransport.status(frame.payload());
    if (status.entityId() != key.entityId() || status.scopeId() != key.scopeId() || status.depth() != depth) throw Wire.integrity("producer status identity differs");
    return status;
  }

  private static void requireIdentity(SealedProducerJournal.Entry entry, byte[] identity) throws ProtocolException {
    if (!MessageDigest.isEqual(entry.identity(), identity)) throw Wire.integrity("producer intent key differs from request");
  }

  private SealedWork.Declaration restore() throws Exception {
    if (journal == null) return null;
    SealedWork.Declaration root = null;
    List<SealedTransport.Checkpoint> acknowledged = new ArrayList<>();
    long cursor = 0;
    for (SealedProducerJournal.Entry entry; (entry = journal.next(cursor)) != null; cursor = entry.id()) {
      if (root == null && entry.kind() != SealedProducerJournal.Kind.DECLARATION) throw Wire.integrity("producer journal starts without a declaration");
      switch (entry.kind()) {
        case DECLARATION -> {
          var request = SealedWork.decode(entry.request());
          requireIdentity(entry, sequenceKey(request.scopeId(), request.sequence()));
          if (root == null) {
            if (request.scopeId() != 0 || request.sequence().signum() != 0 || entry.id() != 1) throw Wire.integrity("producer journal lacks original root attachment");
            root = request;
          }
          if (!request.sessionId().equals(root.sessionId()) || !request.producerId().equals(root.producerId())
              || (request.flags() & SealedWork.ACK) != 0) throw Wire.integrity("producer declaration changes ownership or carries ACK");
          if (entry.resolved()) {
            SealedWork.requireAcknowledgement(request, SealedWork.decode(entry.observation()));
            rememberDeclaration(request, prepareDeclaration(request));
          } else {
            if (entry.revision() != 0) throw Wire.integrity("unresolved declaration contains an ACK observation");
            unacknowledged.put(new CheckpointKey(request.scopeId(), request.sequence()), request);
          }
        }
        case INPUT -> {
          var header = SealedProducerInputs.first(entry.request());
          requireIdentity(entry, entityKey(header.key()));
          Scope scope = sending(header);
          attemptedInputs.add(header.key());
          if (!entry.resolved()) unresolvedInputs.add(header.key());
          if (entry.revision() != 0) {
            var status = restoredStatus(entry.observation(), header.key(), scope.depth);
            boolean terminal = status.state() == 3 || status.state() == 4 || status.state() == 6;
            if (entry.resolved() != terminal || (!terminal && status.state() != 2 && status.state() != 5)) throw Wire.integrity("producer input observation has invalid lifecycle state");
            states.put(header.key(), status.state());
          }
        }
        case SCOPE -> {
          var digest = SealedScope.decode(entry.request());
          requireIdentity(entry, scopeKey(digest.scopeId()));
          Scope scope = scopes.get(digest.scopeId());
          if (scope == null || scope.parent == null || !scope.sealed) throw Wire.integrity("retained closure has no sealed child scope");
          requireResolved(digest.scopeId());
          List<SealedScope.Terminal> leaves = new ArrayList<>();
          for (long id : scope.ids) leaves.add(new SealedScope.Terminal(id, states.get(new SealedWork.EntityKey(digest.scopeId(), id))));
          if (!SealedScope.summarize(digest.scopeId(), leaves).equals(digest)) throw Wire.integrity("retained closure differs from observed descendants");
          if (!entry.resolved()) unresolvedScopes.add(digest.scopeId());
          if (entry.revision() != 0) {
            byte[] evidence = entry.observation();
            if (evidence.length == 1 && evidence[0] == 1 && !entry.resolved()) continue;
            if (evidence.length < 2 || evidence[0] != 2) throw Wire.integrity("invalid retained scope observation");
            var status = restoredStatus(java.util.Arrays.copyOfRange(evidence, 1, evidence.length), scope.parent, scope.depth - 1);
            boolean terminal = status.state() == 3 || status.state() == 4;
            if (entry.resolved() != terminal || (!terminal && status.state() != 7)
                || (digest.failed().signum() != 0 && status.state() != 4)) throw Wire.integrity("invalid retained parent outcome");
            states.put(scope.parent, status.state()); scope.closed = entry.resolved();
          }
        }
        case CHECKPOINT -> {
          var frame = Wire.decodeControl(entry.request());
          if (frame.type() != Wire.FRAME_CHECKPOINT) throw Wire.integrity("invalid retained checkpoint frame");
          var request = SealedTransport.checkpoint(frame.payload());
          long scope = request.scopeId() == null ? 0 : request.scopeId();
          requireIdentity(entry, sequenceKey(scope, request.sequence()));
          if (request.flags() != 0 || checkpoints.size() >= 1024) throw Wire.integrity("invalid retained checkpoint history");
          checkpoints.put(new CheckpointKey(scope, request.sequence()), request);
          if (entry.resolved()) {
            var ack = Wire.decodeControl(entry.observation());
            if (ack.type() != Wire.FRAME_CHECKPOINT || !SealedTransport.checkpoint(ack.payload()).equals(request.acknowledgement())) throw Wire.integrity("retained checkpoint ACK differs");
            acknowledged.add(request);
          } else {
            if (entry.revision() != 0) throw Wire.integrity("unresolved checkpoint contains ACK evidence");
            unacknowledgedCheckpoints.put(new CheckpointKey(scope, request.sequence()), request);
          }
        }
        case SHUTDOWN -> {
          requireIdentity(entry, scopeKey(0));
          var request = Wire.decodeControl(entry.request());
          if (request.type() != Wire.FRAME_GOAWAY) throw Wire.integrity("invalid retained shutdown request");
          long last = Wire.decodeGoaway(request.payload());
          if (entry.resolved()) {
            var ack = Wire.decodeControl(entry.observation());
            if (ack.type() != Wire.FRAME_GOAWAY || Wire.decodeGoaway(ack.payload()) != last) throw Wire.integrity("retained shutdown ACK differs");
            throw new IOException("producer journal already contains an acknowledged shutdown");
          }
          if (entry.revision() != 0) throw Wire.integrity("unresolved shutdown contains ACK evidence");
        }
      }
    }
    // A pending checkpoint may have been acknowledged after a later seal was
    // appended. Validate its evidence only after reconstructing all membership.
    for (var request : acknowledged) {
      long scopeId = request.scopeId() == null ? 0 : request.scopeId();
      Scope scope = scopes.get(scopeId);
      if (scope == null || !scope.sealed || scope.ids.last() != request.lastId()) throw Wire.integrity("retained checkpoint acknowledges an incorrect cut");
      requireResolved(scopeId);
    }
    rootCheckpoint = null;
    return root;
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

  private List<Wire.Status> processing(SealedWork.EntityKey key, int depth, long deadline, SealedProducerJournal.Entry entry) throws Exception {
    Wire.Status admitted = expectStatus(key, depth, deadline);
    if (admitted.state() == 4 || admitted.state() == 6) {
      observe(entry, Wire.encodeStatus(admitted), true); unresolvedInputs.remove(key);
      states.put(key, admitted.state()); return List.of(admitted);
    }
    if (admitted.state() != 2) throw Wire.entity("first processing status must be PROCESSING");
    entry = observe(entry, Wire.encodeStatus(admitted), false);
    states.put(key, 2);
    List<Wire.Status> observed = new ArrayList<>(); observed.add(admitted);
    int previous = admitted.state();
    while (true) {
      if (observed.size() >= 128) throw Wire.limit("per-operation status history exhausted");
      Wire.Status status = expectStatus(key, depth, deadline);
      boolean terminal = status.state() == 3 || status.state() == 4 || status.state() == 6;
      if ((previous == 5 && status.state() != 2) || (previous == 2 && !terminal && status.state() != 5)) throw Wire.entity("invalid processing result progression");
      entry = observe(entry, Wire.encodeStatus(status), terminal);
      observed.add(status); states.put(key, status.state());
      if (terminal) { unresolvedInputs.remove(key); return List.copyOf(observed); }
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
      try { close(); } catch (RuntimeException close) { failure.addSuppressed(close); }
      throw failure;
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
