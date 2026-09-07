package ai.pipestream.quic.v2;

/** A version-2 named protocol failure. It does not assert a durable work outcome. */
public final class ProtocolError extends RuntimeException {
  private static final long serialVersionUID = 1L;

  /** Section 12.2 codes, distinct from the version-1 application errors. */
  public enum Code {
    /** Malformed framing, schema, direction or correlation. */
    FRAME_ERROR(1),
    /** A required or used profile cannot be activated. */
    EXTENSION_UNSUPPORTED(2),
    /** The authenticated principal lacks the requested authority. */
    UNAUTHORIZED(3),
    /** A bounded capacity or stream deadline was reached. */
    LIMIT_EXCEEDED(4),
    /** No matching retained identity is currently found; not proof of noncommit. */
    NOT_FOUND(5),
    /** The applicable retention or authorization promise has expired. */
    EXPIRED(6),
    /** The request conflicts with an immutable identity or current state. */
    CONFLICT(7),
    /** Observed bytes or commitments contradict verified evidence. */
    INTEGRITY_ERROR(8),
    /** Required durable state or connection drain is not ready. */
    NOT_READY(9),
    /** A connection-local checkpoint wait ended before closure. */
    WAIT_TIMEOUT(10),
    /** The immutable execution deadline has been reached. */
    DEADLINE_EXCEEDED(11),
    /** The request is excluded by an accepted cancellation fence. */
    CANCELLED(12),
    /** The configured application contract or mode is unavailable. */
    APPLICATION_UNSUPPORTED(13),
    /** The mandatory control stream was reset. */
    CONTROL_RESET(14),
    /** An internal processing or persistence failure prevents the operation. */
    INTERNAL_ERROR(15),
    /** A promised result cannot currently be served. */
    OUTPUT_UNAVAILABLE(16),
    /** Trusted time is unavailable or violates retained clock state. */
    CLOCK_UNSAFE(17),
    /** The requested transition cannot replace a terminal logical-work outcome. */
    ALREADY_TERMINAL(18);
    private final int value;

    Code(int value) {
      this.value = value;
    }

    /**
     * Get the correlated refusal code.
     *
     * @return REFUSAL's integer code
     */
    public int value() {
      return value;
    }

    /**
     * Get the connection/stream application error.
     *
     * @return QUIC application error code, not a TLS CRYPTO_ERROR
     */
    public long applicationError() {
      return 0x200L + value;
    }

    /**
     * Decode a defined refusal code.
     *
     * @param value wire code
     * @return known code; reserved codes fail
     */
    public static Code from(long value) {
      if (value < 1 || value > 18) throw frame("unknown refusal code");
      return values()[(int) value - 1];
    }
  }

  /** Named failure retained by exception serialization. */
  private final Code code;

  /**
   * Construct a named local protocol failure.
   *
   * @param code named failure
   * @param detail local diagnostic, not authoritative state
   */
  public ProtocolError(Code code, String detail) {
    super(code.name() + ": " + detail);
    this.code = java.util.Objects.requireNonNull(code);
  }

  /**
   * Get the named error.
   *
   * @return named failure
   */
  public Code code() {
    return code;
  }

  static ProtocolError frame(String detail) {
    return new ProtocolError(Code.FRAME_ERROR, detail);
  }

  static ProtocolError limit(String detail) {
    return new ProtocolError(Code.LIMIT_EXCEEDED, detail);
  }

  static void require(boolean condition, String detail) {
    if (!condition) throw frame(detail);
  }
}
