package ai.pipestream.quic;

/** A named PipeStream application-protocol refusal. */
public final class ProtocolException extends Exception {
  private static final long serialVersionUID = 1L;

  private final long errorCode;
  private final String errorName;

  /**
   * Creates a refusal.
   *
   * @param errorCode QUIC application error code
   * @param errorName registered PipeStream error name
   * @param detail diagnostic detail
   */
  public ProtocolException(long errorCode, String errorName, String detail) {
    super(errorName + ": " + detail);
    this.errorCode = errorCode;
    this.errorName = errorName;
  }

  /** @return QUIC application error code */
  public long errorCode() {
    return errorCode;
  }

  /** @return registered PipeStream error name */
  public String errorName() {
    return errorName;
  }
}
