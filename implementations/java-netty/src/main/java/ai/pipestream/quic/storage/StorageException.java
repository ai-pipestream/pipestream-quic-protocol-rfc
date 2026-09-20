package ai.pipestream.quic.storage;

/**
 * The single failure type of the storage SPI. Every backend reports every failure
 * through this class, distinguishing the {@link Kind} that governs recovery behavior:
 * retry, recompute, or refuse. Kinds are part of the cross-language contract; the
 * names are native to each implementation, the semantics are shared.
 */
public final class StorageException extends Exception {
  private static final long serialVersionUID = 1L;

  /** Failure classes, part of the storage contract. */
  public enum Kind {
    /** The addressed scope, entity, intent, or seal record does not exist. */
    NOT_FOUND,
    /** The addressed scope is sealed and rejects mutation. */
    ALREADY_SEALED,
    /** Stored or submitted bytes contradict a recorded or expected digest. */
    CORRUPT,
    /** Retryable backend failure; the operation may succeed if retried. */
    TRANSIENT,
    /** Any other durable failure that is not retryable and not corruption. */
    FAILED
  }

  private final Kind kind;

  private StorageException(Kind kind, String message) {
    super(message);
    this.kind = kind;
  }

  private StorageException(Kind kind, String message, Throwable cause) {
    super(message, cause);
    this.kind = kind;
  }

  /** @return failure class governing recovery behavior */
  public Kind kind() {
    return kind;
  }

  /**
   * Creates a {@link Kind#NOT_FOUND} failure.
   * @param message diagnostic detail
   * @return not-found failure
   */
  public static StorageException notFound(String message) {
    return new StorageException(Kind.NOT_FOUND, message);
  }

  /**
   * Creates an {@link Kind#ALREADY_SEALED} failure.
   * @param message diagnostic detail
   * @return already-sealed failure
   */
  public static StorageException alreadySealed(String message) {
    return new StorageException(Kind.ALREADY_SEALED, message);
  }

  /**
   * Creates a {@link Kind#CORRUPT} failure.
   * @param message diagnostic detail
   * @return corruption failure
   */
  public static StorageException corrupt(String message) {
    return new StorageException(Kind.CORRUPT, message);
  }

  /**
   * Creates a {@link Kind#TRANSIENT} failure.
   * @param message diagnostic detail
   * @param cause retryable backend failure
   * @return transient failure
   */
  public static StorageException transientFailure(String message, Throwable cause) {
    return new StorageException(Kind.TRANSIENT, message, cause);
  }

  /**
   * Creates a {@link Kind#FAILED} failure.
   * @param message diagnostic detail
   * @param cause backend failure, or null
   * @return general failure
   */
  public static StorageException failed(String message, Throwable cause) {
    return cause == null
        ? new StorageException(Kind.FAILED, message)
        : new StorageException(Kind.FAILED, message, cause);
  }
}
