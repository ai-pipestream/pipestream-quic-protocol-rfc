package ai.pipestream.quic.storage;

import java.util.List;
import java.util.Objects;

/**
 * Durable intent and commit log used for crash recovery. Appends are ordered
 * and replayable from a cursor; the recovery path replays the journal to
 * distinguish committed work from admitted-but-uncommitted work. Blocking
 * calls; implementations are thread-safe. All failures are reported as
 * {@link StorageException} with the kind that governs recovery.
 */
public interface Journal extends AutoCloseable {
  /** Largest retained intent identifier, in UTF-8 octets. */
  int MAX_INTENT_ID_BYTES = 256;
  /** Largest retained intent or commit record payload. */
  int MAX_RECORD_BYTES = 16 * 1024 * 1024;

  /**
   * Durably records admitted-but-uncommitted work and returns its assigned
   * cursor, a strictly increasing value starting at 1. Appending an intent
   * whose identifier is already recorded with identical bytes succeeds as a
   * no-op and returns the original cursor; the same identifier with different
   * bytes fails with {@code CORRUPT}. This makes post-crash retries safe.
   *
   * @param intent immutable intent record
   * @return assigned append cursor, strictly increasing
   * @throws StorageException {@code CORRUPT} for a conflicting record under an existing intent identifier
   */
  long appendIntent(Intent intent) throws StorageException;

  /**
   * Marks one intent committed, durably recording the commit bytes after the
   * intent (and, in a two-system deployment, after the object write).
   * Committing an already-committed intent with identical bytes succeeds as a
   * no-op; different bytes fail with {@code CORRUPT}.
   *
   * @param intentId identifier of a previously appended intent
   * @param commitRecord immutable commit record, not retained by this call
   * @throws StorageException {@code NOT_FOUND} if no intent carries the identifier, {@code CORRUPT} for conflicting commit bytes
   */
  void commit(String intentId, byte[] commitRecord) throws StorageException;

  /**
   * Replays every intent and commit appended after {@code sinceCursor}, in
   * append order. Each returned entry carries the intent's append cursor and,
   * when committed, the commit bytes and the cursor the commit consumed.
   * Passing the last seen cursor replays exactly the new tail; passing 0
   * replays the whole journal.
   *
   * @param sinceCursor exclusive lower cursor bound; 0 starts at the beginning
   * @return ordered replay entries with a cursor greater than {@code sinceCursor}
   * @throws StorageException {@code CORRUPT} if retained records fail their integrity checks
   */
  List<ReplayEntry> replay(long sinceCursor) throws StorageException;

  /**
   * Releases backend resources. Idempotent.
   *
   * @throws StorageException {@code FAILED} if cleanup itself fails
   */
  @Override void close() throws StorageException;

  /**
   * One immutable admitted-but-uncommitted work record.
   * @param intentId non-empty caller-assigned identifier, unique per journal
   * @param record application record bytes, not interpreted by the journal
   */
  record Intent(String intentId, byte[] record) {
    /**
     * Validates and copies the record.
     * @param intentId non-empty caller-assigned identifier
     * @param record application record bytes
     */
    public Intent {
      Objects.requireNonNull(intentId, "intentId");
      if (intentId.isEmpty() || intentId.getBytes(java.nio.charset.StandardCharsets.UTF_8).length > MAX_INTENT_ID_BYTES) {
        throw new IllegalArgumentException("intent identifier must be 1 to " + MAX_INTENT_ID_BYTES + " UTF-8 octets");
      }
      Objects.requireNonNull(record, "record");
      if (record.length < 1 || record.length > MAX_RECORD_BYTES) {
        throw new IllegalArgumentException("intent record must be 1 to " + MAX_RECORD_BYTES + " octets");
      }
      record = record.clone();
    }

    /** @return defensive record copy */
    @Override public byte[] record() {
      return record.clone();
    }
  }

  /**
   * One replayed intent with its commit state at the cursor it was read from.
   * @param cursor strictly increasing append cursor of the intent
   * @param intent immutable intent record
   * @param commitRecord commit bytes when committed, null when still uncommitted
   * @param commitCursor cursor the commit record consumed, null when uncommitted
   */
  record ReplayEntry(long cursor, Intent intent, byte[] commitRecord, Long commitCursor) {
    /**
     * Validates the entry.
     * @param cursor positive append cursor of the intent
     * @param intent immutable intent record
     * @param commitRecord commit bytes, or null when uncommitted
     * @param commitCursor cursor the commit consumed, or null when uncommitted
     */
    public ReplayEntry {
      if (cursor < 1) throw new IllegalArgumentException("replay cursor must be positive");
      Objects.requireNonNull(intent, "intent");
      if ((commitRecord == null) != (commitCursor == null)) {
        throw new IllegalArgumentException("commit record and commit cursor must both be present or both be absent");
      }
      if (commitRecord != null && (commitRecord.length < 1 || commitRecord.length > MAX_RECORD_BYTES)) {
        throw new IllegalArgumentException("commit record must be 1 to " + MAX_RECORD_BYTES + " octets");
      }
      if (commitCursor != null && commitCursor < 1) {
        throw new IllegalArgumentException("commit cursor must be positive");
      }
      commitRecord = commitRecord == null ? null : commitRecord.clone();
    }

    /**
     * Creates an uncommitted entry, or an entry whose commit cursor is not needed.
     * @param cursor positive append cursor of the intent
     * @param intent immutable intent record
     * @param commitRecord commit bytes, or null when uncommitted
     */
    public ReplayEntry(long cursor, Intent intent, byte[] commitRecord) {
      this(cursor, intent, commitRecord, null);
    }

    /** @return defensive commit copy, or null when uncommitted */
    @Override public byte[] commitRecord() {
      return commitRecord == null ? null : commitRecord.clone();
    }

    /** @return cursor the commit record consumed in the global order, or null when uncommitted */
    @Override public Long commitCursor() {
      return commitCursor;
    }

    /** @return whether the intent has a durable commit record */
    public boolean committed() {
      return commitRecord != null;
    }
  }
}
