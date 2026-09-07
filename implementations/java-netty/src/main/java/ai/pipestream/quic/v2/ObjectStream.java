package ai.pipestream.quic.v2;

import static ai.pipestream.quic.v2.Checks.*;
import static ai.pipestream.quic.v2.ProtocolError.*;
import static ai.pipestream.quic.v2.Records.*;

import java.nio.ByteBuffer;
import java.security.MessageDigest;

/** Incremental object validation, separate from authorization, storage and durable admission. */
public final class ObjectStream {
  private ObjectStream() {}

  private static long nanos(long milliseconds) {
    range(milliseconds, 1, 86400000);
    return milliseconds * 1_000_000;
  }

  static void before(long now, long then, long interval) {
    // nanoTime subtraction deliberately permits wrap at the signed-long boundary.
    long elapsed = now - then;
    if (elapsed < 0 || elapsed >= interval)
      throw limit("deadline or monotonic clock violation");
  }

  /**
   * A single-use header reader with a fixed byte ceiling and an absolute local deadline. Payload
   * bytes remain in the caller's buffer for authorization and reservation before acceptance.
   * Instances are caller-confined. The transport must drive checkDeadline even when no bytes
   * arrive.
   */
  public static final class HeaderReader {
    private final boolean input;
    private final long start;
    private final long timeout;
    private final byte[] prefix = new byte[4];
    private int prefixBytes;
    private byte[] body;
    private int bodyBytes;
    private boolean complete;
    private boolean failed;

    /**
     * Start receiving one object header.
     *
     * @param input true for a caller input; false for a result
     * @param nowNanos monotonic time at stream acceptance, not a UTC timestamp
     * @param timeoutMs absolute local header deadline, 1..86400000 milliseconds
     */
    public HeaderReader(boolean input, long nowNanos, long timeoutMs) {
      this.input = input;
      start = nowNanos;
      timeout = nanos(timeoutMs);
    }

    /**
     * Consume no more than one header and leave any payload untouched.
     *
     * @param bytes available transport bytes; position advances only through the header
     * @param nowNanos current monotonic time
     * @return a complete typed header, or null while incomplete
     */
    public Value feed(ByteBuffer bytes, long nowNanos) {
      require(!complete && !failed, "object header reader is complete or invalid");
      try {
        checkDeadline(nowNanos);
        while (prefixBytes < 4 && bytes.hasRemaining()) prefix[prefixBytes++] = bytes.get();
        if (prefixBytes < 4) return null;
        if (body == null) {
          long length = Integer.toUnsignedLong(ByteBuffer.wrap(prefix).getInt());
          require(length >= 1 && length <= Wire.HEADER_LIMIT, "invalid object header length");
          body = new byte[(int) length];
        }
        int count = Math.min(bytes.remaining(), body.length - bodyBytes);
        bytes.get(body, bodyBytes, count);
        bodyBytes += count;
        if (bodyBytes != body.length) return null;
        Value value =
            Wire.decodeRecord(
                input ? Wire.RecordKind.INPUT_HEADER : Wire.RecordKind.RESULT_HEADER,
                body,
                Wire.HEADER_LIMIT);
        body = null;
        complete = true;
        return value;
      } catch (ProtocolError error) {
        failed = true;
        body = null;
        throw error;
      }
    }

    /**
     * Enforce the absolute header deadline independently of network progress.
     *
     * @param nowNanos current monotonic time
     */
    public void checkDeadline(long nowNanos) {
      require(!failed, "object header reader is invalid");
      if (complete) return;
      try {
        before(nowNanos, start, timeout);
      } catch (ProtocolError error) {
        failed = true;
        body = null;
        throw error;
      }
    }

    /**
     * Check header geometry at transport FIN; payload geometry is checked separately.
     *
     * @param nowNanos current monotonic time
     */
    public void finish(long nowNanos) {
      checkDeadline(nowNanos);
      if (!complete) {
        failed = true;
        body = null;
        throw frame("FIN before complete object header");
      }
    }

    /**
     * Inspect current allocation without including the fixed four-octet prefix.
     *
     * @return allocated header body bytes, never more than 4096
     */
    public int bufferedCapacity() {
      return body == null ? 0 : body.length;
    }
  }

  /**
   * A constant-buffer payload verifier. Acceptance here is reversible byte consumption, not durable
   * work admission or publication. Only successful finish proves length, digest and FIN geometry.
   * The transport must drive checkDeadline independently of payload callbacks. This receiver-side
   * progress clock must not be used to treat disk reads or queued sender bytes as transport
   * progress.
   */
  public static final class Payload {
    private final long length;
    private final Digest expected;
    private final MessageDigest hash = Commitments.sha256();
    private final long start;
    private final long idle;
    private final long lifetime;
    private long lastProgress;
    private long consumed;
    private boolean ended;
    private boolean verified;

    /**
     * Start verification after header authorization and capacity reservation.
     *
     * @param length exact committed payload length
     * @param expected expected SHA-256, including for an empty payload
     * @param selected negotiated response, not the original offer
     * @param nowNanos monotonic time when the validated header is accepted
     */
    public Payload(long length, Digest expected, Messages.Capabilities selected, long nowNanos) {
      number(length);
      present(expected);
      present(selected);
      require(selected.response(), "payload verification requires negotiated limits");
      if (length > selected.objectLimit()) throw limit("object exceeds negotiated payload ceiling");
      this.length = length;
      this.expected = expected;
      start = nowNanos;
      lastProgress = nowNanos;
      idle = nanos(selected.streamIdleMs());
      lifetime = nanos(selected.streamLifetimeMs());
    }

    /**
     * Hash all supplied payload bytes without retaining them.
     *
     * @param bytes payload only; the caller must not mutate it during this call
     * @param nowNanos monotonic receive-progress time
     */
    public void feed(ByteBuffer bytes, long nowNanos) {
      require(!ended, "payload verification is complete or invalid");
      try {
        checkDeadline(nowNanos);
        int count = bytes.remaining();
        if (count > length - consumed)
          throw new ProtocolError(Code.INTEGRITY_ERROR, "trailing object bytes");
        hash.update(bytes);
        consumed += count;
        if (count != 0) lastProgress = nowNanos;
      } catch (ProtocolError error) {
        ended = true;
        throw error;
      }
    }

    /**
     * Enforce both deadlines without requiring another read callback.
     *
     * @param nowNanos current monotonic time
     */
    public void checkDeadline(long nowNanos) {
      require(!ended, "payload verification is complete or invalid");
      try {
        before(nowNanos, start, lifetime);
        before(nowNanos, lastProgress, idle);
      } catch (ProtocolError error) {
        ended = true;
        throw error;
      }
    }

    /**
     * Validate actual transport FIN; never call this merely because the expected count was
     * received.
     *
     * @param nowNanos monotonic FIN observation time
     * @return verified digest
     */
    public Digest finish(long nowNanos) {
      checkDeadline(nowNanos);
      ended = true;
      if (consumed != length)
        throw new ProtocolError(Code.INTEGRITY_ERROR, "truncated object payload");
      Digest actual = new Digest(hash.digest());
      if (!MessageDigest.isEqual(expected.bytes(), actual.bytes()))
        throw new ProtocolError(Code.INTEGRITY_ERROR, "object digest differs from commitment");
      verified = true;
      return actual;
    }

    /** Abandon an incomplete delivery after reset or connection loss. No work state is changed. */
    public void abort() {
      ended = true;
    }

    /**
     * Inspect byte progress; equality to length alone does not prove FIN or digest validity.
     *
     * @return received payload byte count
     */
    public long consumed() {
      return consumed;
    }

    /**
     * Inspect complete validation, not merely payload progress.
     *
     * @return true only after a successful finish
     */
    public boolean verified() {
      return verified;
    }
  }
}
