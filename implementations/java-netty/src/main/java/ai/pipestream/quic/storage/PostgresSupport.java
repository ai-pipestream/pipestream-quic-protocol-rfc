package ai.pipestream.quic.storage;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/** Shared schema, identity registry, and error mapping for the Postgres backends. */
final class PostgresSupport {
  /** Entity table; bucket mirrors the shared 2-hex fanout layout. */
  static final String ENTITIES_TABLE = "ps_object_entities";
  /** Per-scope seal table, one row per sealed scope. */
  static final String SEALS_TABLE = "ps_object_seals";
  /** Append-only journal table; one row per intent or commit in global cursor order. */
  static final String JOURNAL_TABLE = "ps_journal_records";
  /** Journal row kind discriminant mirroring the filesystem log record type byte. */
  static final int KIND_INTENT = 0;
  /** Journal row kind discriminant for commit records. */
  static final int KIND_COMMIT = 1;

  private static final String ENTITIES_DDL = """
      CREATE TABLE IF NOT EXISTS ps_object_entities (
        scope_path TEXT NOT NULL,
        bucket TEXT NOT NULL,
        entity_id BIGINT NOT NULL CHECK (entity_id >= 0),
        payload BYTEA NOT NULL,
        digest BYTEA NOT NULL CHECK (octet_length(digest) = 32),
        PRIMARY KEY (scope_path, bucket, entity_id)
      )""";
  private static final String SEALS_DDL = """
      CREATE TABLE IF NOT EXISTS ps_object_seals (
        scope_path TEXT PRIMARY KEY,
        digest BYTEA NOT NULL CHECK (octet_length(digest) = 32),
        entity_count BIGINT NOT NULL CHECK (entity_count >= 0)
      )""";
  private static final String JOURNAL_DDL = """
      CREATE TABLE IF NOT EXISTS ps_journal_records (
        cursor BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        kind SMALLINT NOT NULL CHECK (kind IN (0, 1)),
        intent_id TEXT NOT NULL CHECK (octet_length(intent_id) BETWEEN 1 AND 256),
        payload BYTEA NOT NULL CHECK (octet_length(payload) BETWEEN 1 AND 16777216)
      )""";
  private static final String JOURNAL_INTENT_INDEX =
      "CREATE UNIQUE INDEX IF NOT EXISTS ps_journal_intent_ids ON ps_journal_records(intent_id) WHERE kind = 0";

  /** Same-JVM open registry: one live store or journal per role and JDBC URL. */
  private static final Set<String> OPEN_IDENTITIES = ConcurrentHashMap.newKeySet();

  private PostgresSupport() {}

  /** Claims one live backend per role and URL, mirroring the filesystem writer lock. */
  static void acquire(String identity) throws StorageException {
    if (!OPEN_IDENTITIES.add(identity)) {
      throw StorageException.failed("postgres backend already has a writer for this role and URL", null);
    }
  }

  /** Releases a claim from {@link #acquire}; safe to call once. */
  static void release(String identity) {
    OPEN_IDENTITIES.remove(identity);
  }

  /** Opens a connection and creates the shared tables when absent; idempotent. */
  static Connection connectAndEnsureSchema(String jdbcUrl, String user, String password)
      throws SQLException {
    Connection connection = DriverManager.getConnection(jdbcUrl, user, password);
    try (Statement statement = connection.createStatement()) {
      statement.execute(ENTITIES_DDL);
      statement.execute(SEALS_DDL);
      statement.execute(JOURNAL_DDL);
      statement.execute(JOURNAL_INTENT_INDEX);
    }
    return connection;
  }

  /** Maps SQL failures to the storage taxonomy: connection, serialization, deadlock, and lock-timeout states are retryable. */
  static StorageException mapSqlException(SQLException failure, String operation) {
    for (SQLException cursor = failure; cursor != null; cursor = cursor.getNextException()) {
      String state = cursor.getSQLState();
      if (state != null && (state.startsWith("08") || state.equals("40001")
          || state.equals("40P01") || state.equals("55P03"))) {
        return StorageException.transientFailure(operation + " failed", failure);
      }
    }
    return StorageException.failed(operation + " failed", failure);
  }

  /** Closes a connection during cleanup; the first failure stays primary. */
  static void closeAfterFailure(Connection connection, Throwable failure) {
    if (connection != null) try { connection.close(); } catch (SQLException cleanup) { failure.addSuppressed(cleanup); }
  }
}
