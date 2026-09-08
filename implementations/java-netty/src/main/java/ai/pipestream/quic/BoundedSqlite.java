package ai.pipestream.quic;

import java.io.IOException;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Objects;

/**
 * Schema-independent access to the Java reference's bounded Linux SQLite VFS. Sharing this file
 * policy does not open, convert or authorize any protocol session. The caller owns each returned
 * connection and must perform blocking I/O off transport loops.
 */
public final class BoundedSqlite {
  /**
   * Immutable byte-length bounds, distinct from logical quotas and allocated disk blocks.
   *
   * @param databaseBytes main database bound
   * @param walBytes write-ahead log bound
   * @param journalBytes rollback journal bound
   * @param sharedMemoryBytes shared-memory sidecar bound
   */
  public record Limits(
      long databaseBytes, long walBytes, long journalBytes, long sharedMemoryBytes) {
    /** Validate positive 64 KiB multiples and the native VFS ceilings. */
    public Limits {
      new SealedSessionStore.FileLimits(databaseBytes, walBytes, journalBytes, sharedMemoryBytes);
    }

    /**
     * Return the reference file policy.
     *
     * @return 256 MiB main database, 64 MiB WAL/journal, 512 KiB shared memory
     */
    public static Limits defaults() {
      return new Limits(256L << 20, 64L << 20, 64L << 20, 512L << 10);
    }
  }

  private final SealedSqliteFiles files;

  private BoundedSqlite(SealedSqliteFiles files) {
    this.files = files;
  }

  /**
   * Open a file-policy handle without interpreting the database schema.
   *
   * @param path database path, never a SQLite URI or connection string
   * @param limits exact policy required on both creation and reopening
   * @return handle using the existing native file guard, with no unbounded fallback
   * @throws IOException for unsupported platform, changed policy or invalid file layout
   * @throws SQLException if the native guard cannot initialize
   */
  public static BoundedSqlite open(Path path, Limits limits) throws IOException, SQLException {
    Objects.requireNonNull(path);
    Objects.requireNonNull(limits);
    return new BoundedSqlite(
        SealedSqliteFiles.open(
            path,
            new SealedSessionStore.FileLimits(
                limits.databaseBytes(),
                limits.walBytes(),
                limits.journalBytes(),
                limits.sharedMemoryBytes())));
  }

  /**
   * Open an owned connection with full synchronization, foreign keys and bounded busy waiting.
   * Journal-mode selection belongs to schema bootstrap, after refusing foreign formats.
   *
   * @return connection; the caller must close it on every path
   * @throws SQLException for file-policy, native guard or connection configuration failure
   */
  public Connection connect() throws SQLException {
    Connection connection = files.connect();
    try (var statement = connection.createStatement()) {
      statement.execute("PRAGMA busy_timeout=5000");
      statement.execute("PRAGMA foreign_keys=ON");
      statement.execute("PRAGMA synchronous=FULL");
      statement.execute("PRAGMA temp_store=MEMORY");
      statement.execute("PRAGMA mmap_size=0");
      statement.execute("PRAGMA cache_size=-2048");
      for (String pragma : new String[] {"foreign_keys", "synchronous", "mmap_size"}) {
        try (var row = statement.executeQuery("PRAGMA " + pragma)) {
          long expected =
              switch (pragma) {
                case "foreign_keys" -> 1;
                case "synchronous" -> 2;
                default -> 0;
              };
          if (!row.next() || row.getLong(1) != expected)
            throw new SQLException("SQLite setting not applied: " + pragma);
        }
      }
      long pages;
      try (var row = statement.executeQuery("PRAGMA page_size")) {
        if (!row.next() || row.getLong(1) <= 0) throw new SQLException("missing SQLite page size");
        pages = files.limits().databaseBytes() / row.getLong(1);
      }
      try (var row = statement.executeQuery("PRAGMA max_page_count=" + pages)) {
        if (!row.next() || row.getLong(1) > pages)
          throw new SQLException("SQLite page capacity exceeded");
      }
      return connection;
    } catch (SQLException | RuntimeException failure) {
      try {
        connection.close();
      } catch (SQLException close) {
        failure.addSuppressed(close);
      }
      throw failure;
    }
  }
}
