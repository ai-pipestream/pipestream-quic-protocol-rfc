package ai.pipestream.quic;

import java.sql.Connection;
import java.sql.SQLException;

/** Fixed-capacity writes through the guarded connection's SQLite BLOB API. */
final class SealedSqliteImages {
  static final int MAX_BYTES = 16 * 1024 * 1024;

  private SealedSqliteImages() {}

  /** Changes only this connection's ceiling; it remains in force through rollback. */
  static void walCeiling(Connection connection, long bytes) throws SQLException {
    try (var query = connection.prepareStatement("SELECT pipestream_wal_ceiling(?)")) {
      query.setLong(1, bytes);
      try (var result = query.executeQuery()) {
        if (!result.next() || result.getLong(1) != bytes || result.next()) {
          throw new SQLException("WAL ceiling did not acknowledge exact bound");
        }
      }
    }
  }

  /** The caller must own an explicit main-database writer transaction. */
  static void replace(Connection connection, String table, String column, long rowid, byte[] image)
      throws SQLException {
    if (image == null || image.length < 1 || image.length > MAX_BYTES) {
      throw new IllegalArgumentException("invalid fixed image length");
    }
    try (var query = connection.prepareStatement("SELECT pipestream_blob_replace(?,?,?,?)")) {
      query.setString(1, table); query.setString(2, column);
      query.setLong(3, rowid); query.setBytes(4, image);
      try (var result = query.executeQuery()) {
        if (!result.next() || result.getInt(1) != image.length || result.next()) {
          throw new SQLException("fixed image write did not acknowledge exact length");
        }
      }
    }
  }
}
