package ai.pipestream.quic.v2;

import io.netty.buffer.Unpooled;
import io.netty.handler.codec.quic.QuicChannelOption;
import io.netty.handler.codec.quic.QuicStreamChannel;
import java.util.ArrayDeque;
import java.util.function.Consumer;

/**
 * Event-loop-confined admission to Netty writes; completion means native acceptance, never peer
 * ACK.
 */
final class ControlWrites {
  private record Write(int bytes, long start) {}

  private final QuicStreamChannel channel;
  private final CoreOptions options;
  private final Consumer<ProtocolError> failure;
  private final Runnable drained;
  private final ArrayDeque<Write> pending = new ArrayDeque<>();
  private int limit = 4096;
  private int countLimit;
  private int bytes;
  private int peakBytes;
  private int peakFrames;
  private boolean ended;

  /**
   * Own the control stream's writes.
   *
   * @param channel control stream
   * @param options core ceilings
   * @param failure fatal write failure sink
   * @param drained callback when no write is pending
   */
  ControlWrites(
      QuicStreamChannel channel,
      CoreOptions options,
      Consumer<ProtocolError> failure,
      Runnable drained) {
    if (!Boolean.TRUE.equals(
        channel.config().getOption(QuicChannelOption.USE_RESERVED_SEND_BUFFER)))
      throw new IllegalArgumentException("control must own the reserved native allowance");
    this.channel = channel;
    this.options = options;
    this.failure = failure;
    this.drained = drained;
    countLimit = options.pendingLimit();
  }

  /**
   * Apply negotiated control ceilings.
   *
   * @param selected negotiated capabilities
   */
  void selected(Messages.Capabilities selected) {
    limit = selected.controlLimit();
    countLimit = selected.pendingLimit();
  }

  /**
   * Encode and queue one message.
   *
   * @param message control message
   * @return false when the connection ended or the write was refused
   */
  boolean send(Messages.Message message) {
    if (ended) return false;
    if (pending.size() >= countLimit) {
      failure.accept(ProtocolError.limit("control write count exhausted"));
      return false;
    }
    return sendEncoded(Wire.encode(message, limit));
  }

  /**
   * Accept a frame already encoded and registered by the connection's correlation owner.
   *
   * @param frame encoded control frame
   * @return false when the connection already ended or the write was refused
   */
  boolean sendEncoded(byte[] frame) {
    return sendEncoded(frame, null);
  }

  /** Native write settlement: success means Netty accepted the write, never peer receipt. */
  @FunctionalInterface
  interface Settlement {
    /**
     * Observe one settled frame.
     *
     * @param success whether the local transport accepted the write
     */
    void settled(boolean success);
  }

  /**
   * Queue one encoded frame and observe its native write settlement. Settlement means Netty
   * accepted or failed the write, never peer receipt; the callback runs on the event loop exactly
   * once per accepted frame, including after end(). A refused frame never invokes it.
   *
   * @param frame encoded control frame
   * @param settled callback after the write future completes, or null
   * @return false when the connection already ended or the write was refused
   */
  boolean sendEncoded(byte[] frame, Settlement settled) {
    if (ended) return false;
    if (pending.size() >= countLimit || frame.length > limit + 5) {
      failure.accept(ProtocolError.limit("control write count or frame ceiling exhausted"));
      return false;
    }
    if (frame.length > options.queuedControlBytes() - bytes) {
      failure.accept(ProtocolError.limit("control write bytes exhausted"));
      return false;
    }
    Write write = new Write(frame.length, System.nanoTime());
    pending.addLast(write);
    bytes += frame.length;
    peakBytes = Math.max(peakBytes, bytes);
    peakFrames = Math.max(peakFrames, pending.size());
    channel
        .writeAndFlush(Unpooled.wrappedBuffer(frame))
        .addListener(
            result -> {
              if (pending.remove(write)) bytes -= write.bytes();
              try {
                if (settled != null) settled.settled(result.isSuccess());
              } finally {
                if (!ended) {
                  if (!result.isSuccess())
                    failure.accept(
                        new ProtocolError(ProtocolError.Code.CONTROL_RESET, "control send failed"));
                  else if (pending.isEmpty()) drained.run();
                }
              }
            });
    return !ended;
  }

  /**
   * Enforce the control write deadline.
   *
   * @param now monotonic nanoseconds
   */
  void check(long now) {
    if (!ended && !pending.isEmpty()) {
      ObjectStream.before(now, pending.getFirst().start(), options.controlTimeoutMs() * 1000000L);
      // The extension schedules an event-loop native writable retry even when classification is
      // unchanged. Keep the control retry independent of data consumers and writable callbacks.
      channel.config().setOption(QuicChannelOption.USE_RESERVED_SEND_BUFFER, true);
    }
  }

  /**
   * Whether no write is pending.
   *
   * @return true when idle
   */
  boolean empty() {
    return pending.isEmpty();
  }

  /**
   * Peak queued bytes.
   *
   * @return bytes
   */
  int peakBytes() {
    return peakBytes;
  }

  /**
   * Peak queued frames.
   *
   * @return frames
   */
  int peakFrames() {
    return peakFrames;
  }

  /** Stop accepting writes. */
  void end() {
    ended = true;
    // Netty owns and releases the actual ByteBufs when their futures terminate.
    pending.clear();
    bytes = 0;
  }
}
