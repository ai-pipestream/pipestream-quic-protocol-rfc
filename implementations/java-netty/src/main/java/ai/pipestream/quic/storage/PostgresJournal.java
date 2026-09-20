package ai.pipestream.quic.storage;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Durable Postgres {@link Journal}: one append-only table where every intent
 * and every commit is a row carrying its own cursor from an identity column,
 * so commits consume cursors in the global order exactly as the filesystem
 * log does. Statements commit with {@code synchronous_commit=on} (the
 * postgres default), so success implies the record survives a server kill.
 * Thread safety comes from one JDBC connection guarded by synchronized
 * methods; dead connections are re-established lazily so retryable failures
 * can actually be retried. One live journal per JDBC URL may exist in a JVM,
 * mirroring the filesystem writer lock; both roles may share one database.
 */
public final class PostgresJournal implements Journal {
  private final String identity;
  private final String jdbcUrl;
  private final String user;
  private final String password;
  private Connection connection;
  private volatile boolean closed;

  private PostgresJournal(String jdbcUrl, String user, String password) {
    this.identity = "journal\n" + jdbcUrl;
    this.jdbcUrl = jdbcUrl;
    this.user = user;
    this.password = password;
  }

  /**
   * Opens an exclusively owned journal. The schema is created when absent;
   * an existing schema is used as-is (schema management is administrative for
   * this backend).
   *
   * @param jdbcUrl PostgreSQL JDBC URL, for example {@code jdbc:postgresql://127.0.0.1:5432/pipestream}
   * @param user database role
   * @param password database password
   * @return exclusive journal handle
   * @throws StorageException {@code TRANSIENT} for connection failure, {@code FAILED} for a second live journal on the same URL
   */
  public static PostgresJournal open(String jdbcUrl, String user, String password) throws StorageException {
    Objects.requireNonNull(jdbcUrl, "jdbcUrl");
    Objects.requireNonNull(user, "user");
    Objects.requireNonNull(password, "password");
    PostgresSupport.acquire("journal\n" + jdbcUrl);
    Connection connection = null;
    try {
      connection = PostgresSupport.connectAndEnsureSchema(jdbcUrl, user, password);
    } catch (SQLException failure) {
      PostgresSupport.release("journal\n" + jdbcUrl);
      throw PostgresSupport.mapSqlException(failure, "postgres journal open");
    } catch (RuntimeException failure) {
      PostgresSupport.release("journal\n" + jdbcUrl);
      throw failure;
    }
    var journal = new PostgresJournal(jdbcUrl, user, password);
    journal.connection = connection;
    return journal;
  }

  @Override public synchronized long appendIntent(Intent intent) throws StorageException {
    Objects.requireNonNull(intent, "intent");
    checkOpen();
    byte[] record = intent.record();
    Connection connection = connection();
    try {
      Long existing = findRow(connection, PostgresSupport.KIND_INTENT, intent.intentId());
      if (existing != null) {
        long cursor = existing;
        byte[] stored = readPayload(connection, cursor);
        if (Arrays.equals(stored, record)) return cursor;
        throw StorageException.corrupt("intent " + intent.intentId() + " already exists with different bytes");
      }
      try (PreparedStatement insert = connection.prepareStatement(
          "INSERT INTO ps_journal_records(kind, intent_id, payload) VALUES (?, ?, ?) RETURNING cursor")) {
        insert.setInt(1, PostgresSupport.KIND_INTENT);
        insert.setString(2, intent.intentId());
        insert.setBytes(3, record);
        try (ResultSet row = insert.executeQuery()) {
          if (!row.next()) throw StorageException.failed("append returned no cursor", null);
          return row.getLong(1);
        }
      }
    } catch (SQLException failure) {
      throw PostgresSupport.mapSqlException(failure, "appendIntent");
    }
  }

  @Override public synchronized void commit(String intentId, byte[] commitRecord) throws StorageException {
    Objects.requireNonNull(intentId, "intentId");
    Objects.requireNonNull(commitRecord, "commitRecord");
    checkOpen();
    if (commitRecord.length < 1 || commitRecord.length > MAX_RECORD_BYTES) {
      throw new IllegalArgumentException("commit record must be 1 to " + MAX_RECORD_BYTES + " octets");
    }
    Connection connection = connection();
    try {
      Long intentCursor = findRow(connection, PostgresSupport.KIND_INTENT, intentId);
      if (intentCursor == null) {
        throw StorageException.notFound("intent " + intentId + " has no durable append");
      }
      Long commitCursor = findRow(connection, PostgresSupport.KIND_COMMIT, intentId);
      if (commitCursor != null) {
        byte[] stored = readPayload(connection, commitCursor);
        if (Arrays.equals(stored, commitRecord)) return;
        throw StorageException.corrupt("intent " + intentId + " already carries a different commit");
      }
      try (PreparedStatement insert = connection.prepareStatement(
          "INSERT INTO ps_journal_records(kind, intent_id, payload) VALUES (?, ?, ?)")) {
        insert.setInt(1, PostgresSupport.KIND_COMMIT);
        insert.setString(2, intentId);
        insert.setBytes(3, commitRecord);
        insert.executeUpdate();
      }
    } catch (SQLException failure) {
      throw PostgresSupport.mapSqlException(failure, "commit");
    }
  }

  @Override public synchronized List<ReplayEntry> replay(long sinceCursor) throws StorageException {
    if (sinceCursor < 0) throw new IllegalArgumentException("replay cursor must be non-negative");
    checkOpen();
    List<ReplayEntry> replayed = new ArrayList<>();
    Map<String, Integer> positionByIntent = new HashMap<>();
    Map<String, byte[]> commitPayloadByIntent = new HashMap<>();
    Map<String, Long> commitCursorByIntent = new HashMap<>();
    try (PreparedStatement select = connection().prepareStatement(
        "SELECT cursor, kind, intent_id, payload FROM ps_journal_records WHERE cursor > ? ORDER BY cursor ASC")) {
      select.setLong(1, sinceCursor);
      try (ResultSet rows = select.executeQuery()) {
        while (rows.next()) {
          long cursor = rows.getLong(1);
          int kind = rows.getInt(2);
          String intentId = rows.getString(3);
          byte[] payload = rows.getBytes(4);
          if (kind == PostgresSupport.KIND_INTENT) {
            positionByIntent.put(intentId, replayed.size());
            replayed.add(new ReplayEntry(cursor, new Intent(intentId, payload), null));
          } else {
            // The intent sits at or below sinceCursor, outside this replay
            // window; like the filesystem journal, the commit is invisible.
            if (!positionByIntent.containsKey(intentId)) continue;
            commitPayloadByIntent.put(intentId, payload);
            commitCursorByIntent.put(intentId, cursor);
          }
        }
      }
      for (var entry : positionByIntent.entrySet()) {
        Long commitCursor = commitCursorByIntent.get(entry.getKey());
        if (commitCursor == null) continue;
        ReplayEntry original = replayed.get(entry.getValue());
        replayed.set(entry.getValue(), new ReplayEntry(original.cursor(), original.intent(),
            commitPayloadByIntent.get(entry.getKey()), commitCursor));
      }
      return List.copyOf(replayed);
    } catch (SQLException failure) {
      throw PostgresSupport.mapSqlException(failure, "replay");
    }
  }

  /**
   * Closes the connection and releases the role claim; idempotent. A failed
   * close still releases the claim so a later open can succeed.
   *
   * @throws StorageException {@code FAILED} if the connection close fails
   */
  @Override public synchronized void close() throws StorageException {
    if (closed) return;
    closed = true;
    StorageException cleanup = null;
    if (connection != null) {
      try {
        connection.close();
      } catch (SQLException failure) {
        cleanup = StorageException.failed("postgres journal close failed", failure);
      }
    }
    PostgresSupport.release(identity);
    if (cleanup != null) throw cleanup;
  }

  /** Returns the cursor of the newest row with the kind and intent id, or null. */
  private Long findRow(Connection connection, int kind, String intentId) throws SQLException {
    try (PreparedStatement select = connection.prepareStatement(
        "SELECT cursor FROM ps_journal_records WHERE kind = ? AND intent_id = ? ORDER BY cursor DESC LIMIT 1")) {
      select.setInt(1, kind);
      select.setString(2, intentId);
      try (ResultSet row = select.executeQuery()) {
        return row.next() ? row.getLong(1) : null;
      }
    }
  }

  private byte[] readPayload(Connection connection, long cursor) throws SQLException, StorageException {
    try (PreparedStatement select = connection.prepareStatement(
        "SELECT payload FROM ps_journal_records WHERE cursor = ?")) {
      select.setLong(1, cursor);
      try (ResultSet row = select.executeQuery()) {
        if (!row.next()) throw StorageException.failed("journal row " + cursor + " vanished", null);
        return row.getBytes(1);
      }
    }
  }

  private Connection connection() throws StorageException {
    try {
      if (connection == null || !connection.isValid(2)) {
        if (connection != null) try { connection.close(); } catch (SQLException ignored) { /* replaced */ }
        connection = DriverManager.getConnection(jdbcUrl, user, password);
      }
      return connection;
    } catch (SQLException failure) {
      throw PostgresSupport.mapSqlException(failure, "postgres journal connection");
    }
  }

  private void checkOpen() throws StorageException {
    if (closed) throw StorageException.failed("journal is closed", null);
  }
}
