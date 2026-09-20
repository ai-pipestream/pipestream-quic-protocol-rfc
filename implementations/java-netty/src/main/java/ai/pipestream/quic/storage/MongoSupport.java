package ai.pipestream.quic.storage;

import com.mongodb.MongoException;
import com.mongodb.MongoSocketException;
import com.mongodb.MongoTimeoutException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/** Shared collections, indexes, identity registry, and error mapping for the MongoDB backends. */
final class MongoSupport {
  /** Entity collection; bucket mirrors the shared 2-hex fanout layout. */
  static final String OBJECTS_COLLECTION = "objects";
  /** Per-scope seal collection, one document per sealed scope. */
  static final String SEALS_COLLECTION = "seals";
  /** Append-only journal collection; one document per intent or commit in global cursor order. */
  static final String JOURNAL_COLLECTION = "journal";
  /** Counters collection backing the journal's monotonic global sequence. */
  static final String COUNTERS_COLLECTION = "counters";
  /** Counters document id of the journal sequence. */
  static final String JOURNAL_COUNTER_ID = "journal";
  /** Journal document kind discriminant for intents. */
  static final int KIND_INTENT = 0;
  /** Journal document kind discriminant for commits. */
  static final int KIND_COMMIT = 1;
  /** MongoDB duplicate key error code. */
  static final int DUPLICATE_KEY = 11000;

  private MongoSupport() {}

  /** Same-JVM open registry: one live backend per role, server, and database. */
  private static final Set<String> OPEN_IDENTITIES = ConcurrentHashMap.newKeySet();

  /** Claims one live backend per role and identity, mirroring the other backends' single-writer convention. */
  static void acquire(String identity) throws StorageException {
    if (!OPEN_IDENTITIES.add(identity)) {
      throw StorageException.failed("mongo backend already has a writer for this role and identity", null);
    }
  }

  /** Releases a claim from {@link #acquire}; safe to call once. */
  static void release(String identity) {
    OPEN_IDENTITIES.remove(identity);
  }

  /**
   * Maps driver failures to the storage taxonomy. Retryable write errors,
   * network failures, timeouts, and lost primaries are retryable; everything
   * else is a general failure.
   */
  static StorageException mapMongoException(MongoException failure, String operation) {
    if (failure.hasErrorLabel("RetryableWriteError")
        || failure instanceof MongoSocketException
        || failure instanceof MongoTimeoutException
        || failure instanceof com.mongodb.MongoNotPrimaryException) {
      return StorageException.transientFailure(operation + " failed", failure);
    }
    return StorageException.failed(operation + " failed", failure);
  }
}
