package ai.pipestream.quic.v2;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.FixedRecvByteBufAllocator;
import io.netty.handler.codec.quic.QuicChannel;
import io.netty.handler.codec.quic.QuicChannelOption;
import io.netty.handler.codec.quic.QuicCodecBuilder;
import io.netty.handler.codec.quic.QuicStreamChannel;
import io.netty.handler.codec.quic.QuicStreamSendBufferLimits;
import io.netty.handler.codec.quic.QuicStreamType;
import io.netty.util.AttributeKey;
import java.io.IOException;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

/**
 * Connection-confined stream admission. This is transport ownership, not profile negotiation,
 * authorization, object verification or durable request settlement. Callers must do those before
 * transmitting objects, retain receipt correlation after FIN/reset, and drive control deadlines
 * independently. All methods except immutable limit access run on the connection event loop.
 */
final class StreamTransport {
  private static final AttributeKey<StreamTransport> OWNER =
      AttributeKey.valueOf(StreamTransport.class, "owner");
  private static final AttributeKey<Boolean> CLAIMED =
      AttributeKey.valueOf(StreamTransport.class, "claimed");

  /** Explicit local ceilings, independent of negotiated durable admission promises. */
  record Limits(
      int dataStreams,
      int maxDataStreams,
      long dataSendBytes,
      long controlSendBytes,
      int streamWindowBytes,
      int chunkBytes,
      long writeTimeoutMs) {
    Limits {
      if (dataStreams < 0
          || dataStreams > 128
          || maxDataStreams < dataStreams
          || maxDataStreams > 8192
          || dataSendBytes < 0
          || dataSendBytes > 8L * 1024 * 1024
          || (dataStreams == 0) != (dataSendBytes == 0)
          || (dataStreams == 0) != (maxDataStreams == 0)
          || controlSendBytes < 1
          || controlSendBytes > 16L * 1024 * 1024
          || streamWindowBytes < 128
          || streamWindowBytes > 1024 * 1024
          || chunkBytes < 128
          || chunkBytes > 65536
          || writeTimeoutMs < 1
          || writeTimeoutMs > 300000) {
        throw new IllegalArgumentException("invalid stream transport limits");
      }
    }

    static Limits core(CoreOptions options) {
      return new Limits(
          0,
          0,
          0,
          options.queuedControlBytes(),
          options.controlWindowBytes(),
          options.readChunkBytes(),
          options.controlTimeoutMs());
    }

    QuicStreamSendBufferLimits nativeSendLimits() {
      return new QuicStreamSendBufferLimits(dataSendBytes + controlSendBytes, controlSendBytes);
    }

    long receiveWindowBytes() {
      return 2L * (dataStreams + 1) * streamWindowBytes;
    }

    <B extends QuicCodecBuilder<B>> B configure(B builder, boolean server) {
      // The explicit replenishment window avoids quiche's unrelated default 48 KiB cap.
      // Fixing both maxima prevents autotuning from invalidating these configured ceilings.
      // End-to-end reservation under independent credit delivery still needs transport tests.
      return builder
          .initialMaxData(receiveWindowBytes())
          .initialConnectionWindow(receiveWindowBytes())
          .pairReceiveCredit(true)
          .maxConnectionWindow(receiveWindowBytes())
          .maxStreamWindow(streamWindowBytes)
          .initialMaxStreamDataBidirectionalLocal(server ? 0 : streamWindowBytes)
          .initialMaxStreamDataBidirectionalRemote(server ? streamWindowBytes : 0)
          .initialMaxStreamDataUnidirectional(dataStreams == 0 ? 0 : streamWindowBytes)
          .initialMaxStreamsBidirectional(server ? 1 : 0)
          .initialMaxStreamsUnidirectional(dataStreams);
    }
  }

  record Snapshot(
      int incoming,
      int outgoing,
      int opening,
      int queuedBytes,
      int peakQueuedBytes,
      long localStreams,
      long peerStreams) {}

  private final QuicChannel channel;
  private final Limits limits;
  private final boolean server;
  private final Set<Data> data = new HashSet<>();
  private final Set<Opening> opening = new HashSet<>();
  private final io.netty.util.concurrent.ScheduledFuture<?> timer;
  private QuicStreamChannel control;
  private int incoming;
  private int outgoing;
  private int queuedBytes;
  private int peakQueuedBytes;
  private long localStreams;
  private long peerStreams;
  private boolean closed;

  StreamTransport(QuicChannel channel, Limits limits, boolean server) {
    this.channel = Objects.requireNonNull(channel);
    this.limits = Objects.requireNonNull(limits);
    this.server = server;
    var installed = channel.config().getOption(QuicChannelOption.STREAM_SEND_BUFFER_LIMITS);
    if (installed == null
        || installed.total() != limits.dataSendBytes() + limits.controlSendBytes()
        || installed.reserved() != limits.controlSendBytes()) {
      throw new IllegalArgumentException(
          "native send limits must be installed before registration");
    }
    if (!channel.attr(OWNER).compareAndSet(null, this))
      throw new IllegalStateException("connection already has a stream owner");
    long interval = Math.max(1, Math.min(100, limits.writeTimeoutMs() / 4));
    timer =
        limits.dataStreams() == 0
            ? null
            : channel
                .eventLoop()
                .scheduleAtFixedRate(this::check, interval, interval, TimeUnit.MILLISECONDS);
    channel.closeFuture().addListener(ignored -> end());
  }

  private void confined() {
    if (!channel.eventLoop().inEventLoop())
      throw new IllegalStateException("stream transport requires its connection event loop");
  }

  private void live() {
    confined();
    if (closed) throw new IllegalStateException("stream transport closed");
  }

  void bindControl(QuicStreamChannel stream) {
    live();
    if (stream.parent() != channel
        || stream.type() != QuicStreamType.BIDIRECTIONAL
        || stream.streamId() != 0
        || stream.isLocalCreated() == server
        || control != null)
      throw ProtocolError.frame("control must be the one client bidirectional stream zero");
    if (!stream.attr(CLAIMED).compareAndSet(null, true))
      throw ProtocolError.frame("stream already claimed");
    stream.config().setOption(QuicChannelOption.USE_RESERVED_SEND_BUFFER, true);
    control = stream;
  }

  CompletionStage<Data> openData(ChannelHandler handler) {
    live();
    Objects.requireNonNull(handler);
    if (outgoing >= limits.dataStreams() || localStreams >= limits.maxDataStreams())
      throw ProtocolError.limit(
          "outgoing stream admission exhausted; reconnect for lifetime limit");
    Opening request = new Opening();
    opening.add(request);
    outgoing++;
    localStreams++;
    try {
      channel
          .createStream(
              QuicStreamType.UNIDIRECTIONAL,
              new ChannelInitializer<QuicStreamChannel>() {
                @Override
                protected void initChannel(QuicStreamChannel stream) {
                  configureData(stream);
                  stream.pipeline().addLast(handler);
                }
              })
          .addListener(
              created -> {
                opening.remove(request);
                if (!created.isSuccess()) {
                  outgoing--;
                  request.result.completeExceptionally(
                      new IOException("object stream open failed", created.cause()));
                  return;
                }
                QuicStreamChannel stream = (QuicStreamChannel) created.getNow();
                if (closed || request.result.isDone()) {
                  stream.shutdownOutput((int) ProtocolError.Code.LIMIT_EXCEEDED.applicationError());
                  outgoing--;
                  return;
                }
                if ((stream.streamId() >>> 2) >= limits.maxDataStreams()
                    || !stream.isLocalCreated()
                    || (stream.streamId() & 3) != (server ? 3 : 2)) {
                  stream.shutdownOutput((int) ProtocolError.Code.LIMIT_EXCEEDED.applicationError());
                  outgoing--;
                  request.result.completeExceptionally(
                      ProtocolError.limit("stream ordinal ceiling"));
                  return;
                }
                Data owned = new Data(stream, true);
                data.add(owned);
                request.result.complete(owned);
              });
    } catch (RuntimeException failure) {
      opening.remove(request);
      outgoing--;
      request.result.completeExceptionally(failure);
    }
    return request.result.minimalCompletionStage();
  }

  Data claimIncoming(QuicStreamChannel stream) {
    live();
    if (stream.parent() != channel
        || stream.type() != QuicStreamType.UNIDIRECTIONAL
        || stream.isLocalCreated()
        || (stream.streamId() & 3) != (server ? 2 : 3))
      throw ProtocolError.frame("object must use a peer unidirectional stream");
    if (incoming >= limits.dataStreams() || (stream.streamId() >>> 2) >= limits.maxDataStreams())
      throw ProtocolError.limit("incoming stream admission or lifetime ordinal exhausted");
    configureData(stream);
    incoming++;
    peerStreams++;
    Data owned = new Data(stream, false);
    data.add(owned);
    return owned;
  }

  private void configureData(QuicStreamChannel stream) {
    if (!stream.attr(CLAIMED).compareAndSet(null, true))
      throw ProtocolError.frame("stream already claimed");
    stream.config().setOption(QuicChannelOption.USE_RESERVED_SEND_BUFFER, false);
    // Receive-only QUIC streams do not support half closure. Frame mode exposes the actual FIN;
    // channelInactive alone cannot distinguish verified end-of-object from reset/connection loss.
    stream.config().setOption(QuicChannelOption.READ_FRAMES, true);
    stream.config().setAutoRead(false);
    stream
        .config()
        .setRecvByteBufAllocator(
            new FixedRecvByteBufAllocator(limits.chunkBytes()).maxMessagesPerRead(1));
  }

  Snapshot snapshot() {
    confined();
    return new Snapshot(
        incoming,
        outgoing,
        opening.size(),
        queuedBytes,
        peakQueuedBytes,
        localStreams,
        peerStreams);
  }

  private void check() {
    if (closed) return;
    long now = System.nanoTime();
    for (Opening request : Set.copyOf(opening)) {
      try {
        ObjectStream.before(now, request.start, limits.writeTimeoutMs() * 1000000L);
      } catch (ProtocolError failure) {
        // Keep admission charged until the actual create operation settles, even if its waiter
        // ends.
        request.result.completeExceptionally(failure);
      }
    }
    for (Data owned : Set.copyOf(data)) {
      if (owned.failure != null) continue;
      try {
        if (owned.pending != null)
          ObjectStream.before(now, owned.pending.start, limits.writeTimeoutMs() * 1000000L);
        if (owned.finish != null && !owned.finish.isDone())
          ObjectStream.before(now, owned.finishStart, limits.writeTimeoutMs() * 1000000L);
      } catch (ProtocolError failure) {
        owned.abort(failure);
      }
    }
  }

  private void end() {
    if (closed) return;
    closed = true;
    if (timer != null) timer.cancel(false);
    for (Opening request : opening)
      request.result.completeExceptionally(new IOException("connection ended during stream open"));
    for (Data owned : Set.copyOf(data)) {
      owned.failure = new IOException("connection ended without a durable transport outcome");
      if (owned.finish != null) owned.finish.completeExceptionally(owned.failure);
      owned.releaseRequested = true;
      owned.releaseIfReady();
    }
  }

  private static final class Opening {
    final long start = System.nanoTime();
    final CompletableFuture<Data> result = new CompletableFuture<>();
  }

  private static final class Write {
    final long start = System.nanoTime();
    final int size;
    final CompletableFuture<Void> result = new CompletableFuture<>();

    Write(int size) {
      this.size = size;
    }
  }

  /** A bounded transport lease, retained separately from native ACK and durable receipt state. */
  final class Data {
    private final QuicStreamChannel stream;
    private final boolean local;
    private Write pending;
    private CompletableFuture<Void> finish;
    private long finishStart;
    private Throwable failure;
    private boolean outputFin;
    private boolean releaseRequested;
    private boolean released;

    private Data(QuicStreamChannel stream, boolean local) {
      this.stream = stream;
      this.local = local;
    }

    QuicStreamChannel stream() {
      confined();
      return stream;
    }

    private void writable() {
      live();
      if (!local || released || releaseRequested || failure != null || finish != null)
        throw new IllegalStateException("object output is not writable");
    }

    CompletionStage<Void> write(byte[] bytes) {
      writable();
      Objects.requireNonNull(bytes);
      if (bytes.length == 0 || bytes.length > limits.chunkBytes() || pending != null)
        throw ProtocolError.limit("object writes require one nonempty bounded in-flight chunk");
      ByteBuf buffer = stream.alloc().directBuffer(bytes.length, bytes.length);
      buffer.writeBytes(bytes);
      Write write = new Write(bytes.length);
      pending = write;
      queuedBytes += write.size;
      peakQueuedBytes = Math.max(peakQueuedBytes, queuedBytes);
      stream
          .writeAndFlush(buffer)
          .addListener(
              sent -> {
                pending = null;
                queuedBytes -= write.size;
                if (!sent.isSuccess() && failure == null)
                  failure =
                      new IOException("object send failed without a durable outcome", sent.cause());
                if (failure != null) {
                  write.result.completeExceptionally(failure);
                  if (finish != null) finish.completeExceptionally(failure);
                } else {
                  write.result.complete(null);
                  sendFin();
                }
                releaseIfReady();
              });
      return write.result.minimalCompletionStage();
    }

    CompletionStage<Void> finish() {
      confined();
      if (finish != null) return finish.minimalCompletionStage();
      writable();
      finishStart = System.nanoTime();
      finish = new CompletableFuture<>();
      sendFin();
      return finish.minimalCompletionStage();
    }

    private void sendFin() {
      if (finish == null || pending != null || outputFin || failure != null) return;
      outputFin = true;
      stream
          .shutdownOutput()
          .addListener(
              sent -> {
                if (!sent.isSuccess() && failure == null)
                  failure =
                      new IOException("object FIN failed without a durable outcome", sent.cause());
                if (failure != null) finish.completeExceptionally(failure);
                else finish.complete(null);
                releaseIfReady();
              });
    }

    void abort(ProtocolError cause) {
      confined();
      Objects.requireNonNull(cause);
      if (released || failure != null) return;
      failure = cause;
      if (finish != null) finish.completeExceptionally(cause);
      if (local) stream.shutdownOutput((int) cause.code().applicationError());
      else stream.shutdownInput((int) cause.code().applicationError());
    }

    void release() {
      confined();
      if (released) return;
      if (failure == null
          && !(local ? finish != null && finish.isDone() : stream.isInputShutdown()))
        throw new IllegalStateException("cannot settle a live object transport");
      releaseRequested = true;
      releaseIfReady();
    }

    private void releaseIfReady() {
      if (!releaseRequested || pending != null || released) return;
      released = true;
      data.remove(this);
      if (local) outgoing--;
      else incoming--;
    }
  }
}
