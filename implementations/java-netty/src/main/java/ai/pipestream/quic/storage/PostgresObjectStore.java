package ai.pipestream.quic.storage;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.security.MessageDigest;

/**
 * Durable Postgres {@link ObjectStore}: scope tree, fanout, and seal records in
 * ordinary tables with payloads as {@code bytea}. Each statement commits with
 * {@code synchronous_commit=on} (the postgres default), so an operation returns
 * success only after the server acknowledges the write; single-statement
 * atomicity plus the contract's idempotent retries provide the same guarantees
 * as the filesystem backend's fsync-then-rename sequence. Thread safety comes
 * from one JDBC connection guarded by synchronized methods, the same shape as
 * the single-writer filesystem store; dead connections are re-established
 * lazily, so retryable failures can actually be retried. One live store per
 * role and JDBC URL may exist in a JVM, mirroring the filesystem writer lock.
 * Both roles may share one database; table names are fixed and created when
 * absent.
 */
public final class PostgresObjectStore implements ObjectStore {
  private final String identity;
  private final String jdbcUrl;
  private final String user;
  private final String password;
  private Connection connection;
  private volatile boolean closed;

  private PostgresObjectStore(String jdbcUrl, String user, String password) {
    this.identity = "objects\n" + jdbcUrl;
    this.jdbcUrl = jdbcUrl;
    this.user = user;
    this.password = password;
  }

  /**
   * Opens an exclusively owned object store. The schema is created when absent;
   * an existing schema is used as-is (schema management is administrative for
   * this backend).
   *
   * @param jdbcUrl PostgreSQL JDBC URL, for example {@code jdbc:postgresql://127.0.0.1:5432/pipestream}
   * @param user database role
   * @param password database password
   * @return exclusive store handle
   * @throws StorageException {@code TRANSIENT} for connection failure, {@code FAILED} for a second live store on the same URL
   */
  public static PostgresObjectStore open(String jdbcUrl, String user, String password)
      throws StorageException {
    Objects.requireNonNull(jdbcUrl, "jdbcUrl");
    Objects.requireNonNull(user, "user");
    Objects.requireNonNull(password, "password");
    PostgresSupport.acquire("objects\n" + jdbcUrl);
    Connection connection = null;
    try {
      connection = PostgresSupport.connectAndEnsureSchema(jdbcUrl, user, password);
    } catch (SQLException failure) {
      PostgresSupport.release("objects\n" + jdbcUrl);
      throw PostgresSupport.mapSqlException(failure, "postgres store open");
    } catch (RuntimeException failure) {
      PostgresSupport.release("objects\n" + jdbcUrl);
      throw failure;
    }
    var store = new PostgresObjectStore(jdbcUrl, user, password);
    store.connection = connection;
    return store;
  }

  @Override public synchronized void put(ScopePath scope, long entityId, byte[] bytes, byte[] expectedDigest)
      throws StorageException {
    Objects.requireNonNull(scope, "scope");
    Objects.requireNonNull(bytes, "bytes");
    validateEntityId(entityId);
    byte[] expected = validateDigest(expectedDigest);
    checkOpen();
    Connection connection = connection();
    String scopePath = scope.render();
    String bucket = StoreLayout.fanoutBucket(entityId);
    try {
      if (sealed(connection, scopePath)) {
        throw StorageException.alreadySealed("scope " + scopePath + " is sealed");
      }
      byte[] actual = StoreLayout.sha256().digest(bytes);
      if (!MessageDigest.isEqual(actual, expected)) {
        throw StorageException.corrupt("put digest mismatch for entity " + entityId);
      }
      try (PreparedStatement insert = connection.prepareStatement(
          "INSERT INTO ps_object_entities(scope_path, bucket, entity_id, payload, digest) "
              + "VALUES (?, ?, ?, ?, ?) ON CONFLICT (scope_path, bucket, entity_id) DO NOTHING")) {
        insert.setString(1, scopePath);
        insert.setString(2, bucket);
        insert.setLong(3, entityId);
        insert.setBytes(4, bytes);
        insert.setBytes(5, expected);
        if (insert.executeUpdate() == 1) return;
      }
      try (PreparedStatement select = connection.prepareStatement(
          "SELECT payload FROM ps_object_entities WHERE scope_path = ? AND bucket = ? AND entity_id = ?")) {
        select.setString(1, scopePath);
        select.setString(2, bucket);
        select.setLong(3, entityId);
        try (ResultSet row = select.executeQuery()) {
          if (!row.next()) throw StorageException.failed("put vanished before verification", null);
          if (Arrays.equals(row.getBytes(1), bytes)) return;
          throw StorageException.corrupt("conflicting content for entity " + entityId);
        }
      }
    } catch (SQLException failure) {
      throw PostgresSupport.mapSqlException(failure, "put");
    }
  }

  @Override public synchronized InputStream openRead(ScopePath scope, long entityId) throws StorageException {
    Objects.requireNonNull(scope, "scope");
    validateEntityId(entityId);
    checkOpen();
    Connection connection = connection();
    try (PreparedStatement select = connection.prepareStatement(
        "SELECT payload FROM ps_object_entities WHERE scope_path = ? AND bucket = ? AND entity_id = ?")) {
      select.setString(1, scope.render());
      select.setString(2, StoreLayout.fanoutBucket(entityId));
      select.setLong(3, entityId);
      try (ResultSet row = select.executeQuery()) {
        if (!row.next()) {
          throw StorageException.notFound("entity " + entityId + " is absent from scope " + scope.render());
        }
        // bytea materializes per read; the returned stream is an independent
        // snapshot, so removal after the read never invalidates it.
        return new ByteArrayInputStream(row.getBytes(1));
      }
    } catch (SQLException failure) {
      throw PostgresSupport.mapSqlException(failure, "openRead");
    }
  }

  @Override public synchronized void sealScope(ScopePath scope, byte[] digest, long entityCount)
      throws StorageException {
    Objects.requireNonNull(scope, "scope");
    byte[] recorded = validateDigest(digest);
    if (entityCount < 0) throw new IllegalArgumentException("entity count must be non-negative");
    checkOpen();
    Connection connection = connection();
    String scopePath = scope.render();
    try {
      byte[] existingDigest = null;
      long existingCount = -1;
      try (PreparedStatement select = connection.prepareStatement(
          "SELECT digest, entity_count FROM ps_object_seals WHERE scope_path = ?")) {
        select.setString(1, scopePath);
        try (ResultSet row = select.executeQuery()) {
          if (row.next()) {
            existingDigest = row.getBytes(1);
            existingCount = row.getLong(2);
          }
        }
      }
      if (existingDigest != null) {
        if (existingCount == entityCount && Arrays.equals(existingDigest, recorded)) return;
        throw StorageException.corrupt("scope " + scopePath + " already carries a different seal");
      }
      try (PreparedStatement insert = connection.prepareStatement(
          "INSERT INTO ps_object_seals(scope_path, digest, entity_count) VALUES (?, ?, ?)")) {
        insert.setString(1, scopePath);
        insert.setBytes(2, recorded);
        insert.setLong(3, entityCount);
        insert.executeUpdate();
      }
    } catch (SQLException failure) {
      throw PostgresSupport.mapSqlException(failure, "sealScope");
    }
  }

  @Override public synchronized SealRecord verifyScope(ScopePath scope) throws StorageException {
    Objects.requireNonNull(scope, "scope");
    checkOpen();
    Connection connection = connection();
    String scopePath = scope.render();
    try {
      long count = -1;
      byte[] recorded = null;
      try (PreparedStatement select = connection.prepareStatement(
          "SELECT digest, entity_count FROM ps_object_seals WHERE scope_path = ?")) {
        select.setString(1, scopePath);
        try (ResultSet row = select.executeQuery()) {
          if (row.next()) {
            recorded = row.getBytes(1);
            count = row.getLong(2);
          }
        }
      }
      if (recorded == null) {
        throw StorageException.notFound("scope " + scopePath + " has no seal record");
      }
      long live = 0;
      try (PreparedStatement select = connection.prepareStatement(
          "SELECT entity_id, payload, digest FROM ps_object_entities WHERE scope_path = ? ORDER BY entity_id ASC")) {
        select.setString(1, scopePath);
        try (ResultSet rows = select.executeQuery()) {
          while (rows.next()) {
            long entityId = rows.getLong(1);
            byte[] payload = rows.getBytes(2);
            byte[] storedDigest = rows.getBytes(3);
            if (!MessageDigest.isEqual(StoreLayout.sha256().digest(payload), storedDigest)) {
              throw StorageException.corrupt("entity " + entityId + " fails its recorded digest");
            }
            live++;
          }
        }
      }
      if (live != count) {
        throw StorageException.corrupt("scope " + scopePath + " entity count differs from its seal");
      }
      return new SealRecord(recorded, count);
    } catch (SQLException failure) {
      throw PostgresSupport.mapSqlException(failure, "verifyScope");
    }
  }

  @Override public synchronized List<Long> list(ScopePath scope) throws StorageException {
    Objects.requireNonNull(scope, "scope");
    checkOpen();
    List<Long> ids = new ArrayList<>();
    try (PreparedStatement select = connection().prepareStatement(
        "SELECT entity_id FROM ps_object_entities WHERE scope_path = ? ORDER BY entity_id ASC")) {
      select.setString(1, scope.render());
      try (ResultSet rows = select.executeQuery()) {
        while (rows.next()) ids.add(rows.getLong(1));
      }
      return List.copyOf(ids);
    } catch (SQLException failure) {
      throw PostgresSupport.mapSqlException(failure, "list");
    }
  }

  @Override public synchronized void retire(ScopePath scope, long retentionCursor) throws StorageException {
    Objects.requireNonNull(scope, "scope");
    if (retentionCursor < 0) throw new IllegalArgumentException("retention cursor must be non-negative");
    checkOpen();
    Connection connection = connection();
    String scopePath = scope.render();
    try {
      boolean sealed = sealed(connection, scopePath);
      long remaining;
      if (sealed) {
        try (PreparedStatement delete = connection.prepareStatement(
            "DELETE FROM ps_object_entities WHERE scope_path = ?")) {
          delete.setString(1, scopePath);
          delete.executeUpdate();
        }
        try (PreparedStatement delete = connection.prepareStatement(
            "DELETE FROM ps_object_seals WHERE scope_path = ?")) {
          delete.setString(1, scopePath);
          delete.executeUpdate();
        }
        return;
      }
      try (PreparedStatement count = connection.prepareStatement(
          "SELECT count(*) FROM ps_object_entities WHERE scope_path = ?")) {
        count.setString(1, scopePath);
        try (ResultSet row = count.executeQuery()) {
          row.next();
          remaining = row.getLong(1);
        }
      }
      if (remaining == 0) {
        throw StorageException.notFound("scope " + scopePath + " holds nothing to retire");
      }
      try (PreparedStatement delete = connection.prepareStatement(
          "DELETE FROM ps_object_entities WHERE scope_path = ? AND entity_id <= ?")) {
        delete.setString(1, scopePath);
        delete.setLong(2, retentionCursor);
        delete.executeUpdate();
      }
    } catch (SQLException failure) {
      throw PostgresSupport.mapSqlException(failure, "retire");
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
        cleanup = StorageException.failed("postgres store close failed", failure);
      }
    }
    PostgresSupport.release(identity);
    if (cleanup != null) throw cleanup;
  }

  private boolean sealed(Connection connection, String scopePath) throws SQLException {
    try (PreparedStatement select = connection.prepareStatement(
        "SELECT 1 FROM ps_object_seals WHERE scope_path = ?")) {
      select.setString(1, scopePath);
      try (ResultSet row = select.executeQuery()) {
        return row.next();
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
      throw PostgresSupport.mapSqlException(failure, "postgres store connection");
    }
  }

  private void checkOpen() throws StorageException {
    if (closed) throw StorageException.failed("object store is closed", null);
  }

  private static void validateEntityId(long entityId) {
    if (entityId < 0 || entityId > StoreLayout.MAX_ENTITY_ID) {
      throw new IllegalArgumentException("entity identifier is not unsigned 32-bit");
    }
  }

  private static byte[] validateDigest(byte[] digest) {
    Objects.requireNonNull(digest, "digest");
    if (digest.length != StoreLayout.DIGEST_BYTES) {
      throw new IllegalArgumentException("digest must be " + StoreLayout.DIGEST_BYTES + " octets");
    }
    return digest;
  }
}
