package ai.pipestream.quic.v2;

import static ai.pipestream.quic.v2.ProtocolError.*;
import static ai.pipestream.quic.v2.Records.*;

import java.nio.ByteBuffer;
import java.util.Arrays;

/** Independent V2 binary framing and exact typed record encoding. */
public final class Wire {
  private Wire() {}

  /** Initial and CAPABILITIES body ceiling in octets. */
  public static final int INITIAL_CONTROL_LIMIT = 4096;

  /** Largest negotiable control body ceiling in octets. */
  public static final int MAX_CONTROL_LIMIT = 1048576;

  /** Object header body ceiling in octets. */
  public static final int HEADER_LIMIT = 4096;

  /** A parsed control frame, or bounded discarded extension metadata. */
  public sealed interface Frame permits Known, Ignored {}

  /**
   * A parsed known control message.
   *
   * @param message validated known control body
   */
  public record Known(Messages.Message message) implements Frame {}

  /**
   * Metadata for an incrementally discarded ignorable frame.
   *
   * @param type ignorable extension type
   * @param length discarded body length
   */
  public record Ignored(int type, int length) implements Frame {}

  /**
   * Encode one control frame.
   *
   * @param message typed control
   * @param limit negotiated body ceiling
   * @return complete control frame
   */
  public static byte[] encode(Messages.Message message, int limit) {
    validateLimit(limit);
    Cbor.Writer w =
        new Cbor.Writer(
            message instanceof Messages.Capabilities
                ? Math.min(limit, INITIAL_CONTROL_LIMIT)
                : limit);
    MessageCodec.write(w, message);
    byte[] body = w.finish();
    return ByteBuffer.allocate(body.length + 5)
        .put((byte) Messages.type(message))
        .putInt(body.length)
        .put(body)
        .array();
  }

  /**
   * Decode exactly one complete control frame.
   *
   * @param frame exactly one complete frame
   * @param limit body ceiling
   * @return known body or ignored metadata
   */
  public static Frame decode(byte[] frame, int limit) {
    Decoder decoder = new Decoder(limit);
    ByteBuffer bytes = ByteBuffer.wrap(frame);
    Frame result = decoder.feed(bytes);
    require(result != null && !bytes.hasRemaining(), "missing frame or trailing control bytes");
    decoder.finish();
    return result;
  }

  static void validateLimit(int limit) {
    require(limit >= INITIAL_CONTROL_LIMIT && limit <= MAX_CONTROL_LIMIT, "invalid control limit");
  }

  /**
   * Incremental one-frame-at-a-time control reader. Ignorable bodies are never buffered. A failure
   * poisons this decoder; it cannot resume at an invented frame boundary.
   */
  public static final class Decoder {
    private int limit;
    private final byte[] prefix = new byte[5];
    private int prefixBytes;
    private byte[] body;
    private int received;
    private int length;
    private int type;
    private boolean failed;

    /**
     * Start an incremental control decoder.
     *
     * @param limit current body limit; start with INITIAL_CONTROL_LIMIT
     */
    public Decoder(int limit) {
      validateLimit(limit);
      this.limit = limit;
    }

    /**
     * Update the negotiated body ceiling.
     *
     * @param selected newly negotiated limit; change only at a frame boundary
     */
    public void limit(int selected) {
      require(!failed && prefixBytes == 0, "cannot change a partial frame limit");
      validateLimit(selected);
      limit = selected;
    }

    /**
     * Inspect the current decoder allocation.
     *
     * @return currently allocated body bytes; ignored frames consume zero
     */
    public int bufferedCapacity() {
      return body == null ? 0 : body.length;
    }

    /**
     * Consume bytes through at most one frame.
     *
     * @param bytes caller's available bytes, consumed only through one frame
     * @return one completed frame or null when more bytes are needed
     */
    public Frame feed(ByteBuffer bytes) {
      require(!failed, "decoder already failed");
      try {
        while (prefixBytes < 5 && bytes.hasRemaining()) prefix[prefixBytes++] = bytes.get();
        if (prefixBytes < 5) return null;
        if (body == null && received == 0) {
          type = Byte.toUnsignedInt(prefix[0]);
          long declared = Integer.toUnsignedLong(ByteBuffer.wrap(prefix, 1, 4).getInt());
          if (declared > limit || type == 1 && declared > INITIAL_CONTROL_LIMIT)
            throw ProtocolError.limit("control body exceeds negotiated limit");
          length = (int) declared;
          if (type == 0 || type >= 8 && type < 128) throw frame("unknown required control type");
          if (type >= 192)
            throw new ProtocolError(
                Code.EXTENSION_UNSUPPORTED, "private type has no implemented defining profile");
          if (type < 128) body = new byte[length];
        }
        int count = Math.min(bytes.remaining(), length - received);
        if (body == null) bytes.position(bytes.position() + count);
        else bytes.get(body, received, count);
        received += count;
        if (received != length) return null;
        Frame frame;
        if (body == null) frame = new Ignored(type, length);
        else {
          Cbor.Reader reader = new Cbor.Reader(body, limit);
          frame = new Known(MessageCodec.read(type, reader));
          reader.end();
        }
        prefixBytes = 0;
        received = 0;
        body = null;
        return frame;
      } catch (ProtocolError error) {
        failed = true;
        body = null;
        throw error;
      }
    }

    /** Validate control FIN geometry. This does not assert connection or work drain. */
    public void finish() {
      if (failed || prefixBytes != 0) {
        failed = true;
        body = null;
        throw frame("truncated or failed control stream");
      }
    }
  }

  /** Named record roots for standalone durable records and object headers. */
  public enum RecordKind {
    /** Logical work identity. */
    WORK_KEY,
    /** Control or input-stream correlation tag. */
    REQUEST_TAG,
    /** Independent retained lifetimes. */
    POLICY,
    /** Session capacity ceilings. */
    LIMITS,
    /** Input descriptor. */
    INPUT,
    /** Reserved output count and byte budget. */
    OUTPUT_BUDGET,
    /** Application diagnostic. */
    DIAGNOSTIC,
    /** Immutable child allocation. */
    CHILD_SCOPE,
    /** Disjoint terminal partition. */
    COUNTS,
    /** Immutable scope closure. */
    SCOPE_SUMMARY,
    /** Complete immutable admission parameters. */
    ADMIT_PARAMETERS,
    /** Input stream header body. */
    INPUT_HEADER,
    /** Result stream header body. */
    RESULT_HEADER,
    /** Published object descriptor. */
    OUTPUT,
    /** Immutable publication manifest. */
    MANIFEST,
    /** Consistent work snapshot. */
    WORK_VIEW,
    /** Retained operation identity and outcome. */
    OPERATION_RECEIPT,
    /** Typed operation outcome. */
    OUTCOME
  }

  /**
   * Encode a standalone typed record.
   *
   * @param value typed immutable record
   * @param limit body ceiling
   * @return unframed deterministic CBOR
   */
  public static byte[] encodeRecord(Value value, int limit) {
    Cbor.Writer writer = new Cbor.Writer(limit);
    RecordCodec.write(writer, value);
    return writer.finish();
  }

  /**
   * Decode exactly one typed record.
   *
   * @param kind exact expected root
   * @param bytes unframed bytes
   * @param limit body ceiling
   * @return typed record
   */
  public static Value decodeRecord(RecordKind kind, byte[] bytes, int limit) {
    Cbor.Reader r = new Cbor.Reader(bytes, limit);
    Value value =
        switch (kind) {
          case WORK_KEY -> RecordCodec.work(r);
          case REQUEST_TAG -> RecordCodec.tag(r);
          case POLICY -> RecordCodec.policy(r);
          case LIMITS -> RecordCodec.limits(r);
          case INPUT -> RecordCodec.input(r);
          case OUTPUT_BUDGET -> RecordCodec.budget(r);
          case DIAGNOSTIC -> RecordCodec.diagnostic(r);
          case CHILD_SCOPE -> RecordCodec.child(r);
          case COUNTS -> RecordCodec.counts(r);
          case SCOPE_SUMMARY -> RecordCodec.summary(r);
          case ADMIT_PARAMETERS -> RecordCodec.admit(r);
          case INPUT_HEADER -> RecordCodec.inputHeader(r);
          case RESULT_HEADER -> RecordCodec.resultHeader(r);
          case OUTPUT -> RecordCodec.output(r);
          case MANIFEST -> RecordCodec.manifest(r);
          case WORK_VIEW -> RecordCodec.view(r);
          case OPERATION_RECEIPT -> RecordCodec.receipt(r);
          case OUTCOME -> RecordCodec.outcome(r);
        };
    r.end();
    return value;
  }

  /**
   * Encode an object header without its payload.
   *
   * @param header input or result header
   * @return u32 length followed by exact CBOR, without payload
   */
  public static byte[] encodeHeader(Value header) {
    require(
        header instanceof InputHeader || header instanceof ResultHeader, "not an object header");
    byte[] body = encodeRecord(header, HEADER_LIMIT);
    return ByteBuffer.allocate(body.length + 4).putInt(body.length).put(body).array();
  }

  /**
   * Decode an exact object header without its payload.
   *
   * @param input true for an input header
   * @param bytes exact prefix/body, without payload
   * @return typed header
   */
  public static Value decodeHeader(boolean input, byte[] bytes) {
    require(bytes.length >= 4, "truncated object prefix");
    long length = Integer.toUnsignedLong(ByteBuffer.wrap(bytes).getInt());
    require(
        length >= 1 && length <= HEADER_LIMIT && length == bytes.length - 4L,
        "invalid object header length");
    return decodeRecord(
        input ? RecordKind.INPUT_HEADER : RecordKind.RESULT_HEADER,
        Arrays.copyOfRange(bytes, 4, bytes.length),
        HEADER_LIMIT);
  }
}
