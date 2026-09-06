package ai.pipestream.quic;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

final class SealedSqliteImagesTest {
  @TempDir Path directory;

  private SealedSqliteFiles files(String name) throws Exception {
    return SealedSqliteFiles.open(directory.resolve(name + ".db"),
        new SealedSessionStore.FileLimits(32L << 20, 32L << 20, 32L << 20, 1L << 20));
  }

  @Test void onlyGuardedMainConnectionsReceiveTheDirectOnlyPrimitive() throws Exception {
    try (var guarded = files("guarded").connect()) {
      assertEquals(1, scalar(guarded, "SELECT count(*) FROM pragma_function_list WHERE name='pipestream_blob_replace'"));
      execute(guarded, "CREATE TABLE images(image BLOB NOT NULL) STRICT");
      execute(guarded, "INSERT INTO images VALUES(zeroblob(1))");
      assertThrows(SQLException.class, () -> SealedSqliteImages.replace(guarded, "images", "image", 1, new byte[]{1}));
      assertThrows(SQLException.class, () -> SealedSqliteImages.walCeiling(guarded, 1));
      execute(guarded, "BEGIN");
      assertEquals(1, scalar(guarded, "SELECT count(*) FROM images"));
      assertThrows(SQLException.class, () -> SealedSqliteImages.replace(guarded, "images", "image", 1, new byte[]{1}));
      assertThrows(SQLException.class, () -> SealedSqliteImages.walCeiling(guarded, 1));
      execute(guarded, "ROLLBACK");
      execute(guarded, "CREATE VIEW forbidden AS SELECT pipestream_blob_replace('images','image',1,x'01')");
      execute(guarded, "CREATE VIEW forbidden_ceiling AS SELECT pipestream_wal_ceiling(1)");
      execute(guarded, "BEGIN IMMEDIATE");
      assertThrows(SQLException.class, () -> scalar(guarded, "SELECT * FROM forbidden"));
      assertThrows(SQLException.class, () -> scalar(guarded, "SELECT * FROM forbidden_ceiling"));
      for (String argument : List.of("-1", "33554433", "'1'", "1.0", "NULL", "x'01'")) {
        assertThrows(SQLException.class, () -> scalar(guarded, "SELECT pipestream_wal_ceiling(" + argument + ")"));
      }
      assertThrows(SQLException.class, () -> execute(guarded, "SELECT pipestream_guard_unregister('anything')"));
      assertThrows(SQLException.class, () -> execute(guarded, "SELECT load_extension('anything')"));
      SealedSqliteImages.replace(guarded, "images", "image", 1, new byte[]{1});
      execute(guarded, "COMMIT");
      assertArrayEquals(new byte[]{1}, image(guarded));
    }
    for (String url : List.of("jdbc:sqlite::memory:", "jdbc:sqlite:" + directory.resolve("plain.db"))) {
      try (var plain = DriverManager.getConnection(url)) {
        assertEquals(0, scalar(plain, "SELECT count(*) FROM pragma_function_list WHERE name='pipestream_blob_replace'"));
        assertEquals(0, scalar(plain, "SELECT count(*) FROM pragma_function_list WHERE name='pipestream_wal_ceiling'"));
      }
    }
  }

  @Test void walCeilingsStayWithTheirWriterAcrossRollbackAndOtherConnections() throws Exception {
    for (int page : List.of(512, 4096, 65536)) {
      var files = files("ceiling-" + page);
      try (var first = files.connect(); var second = files.connect(); var reader = files.connect()) {
        execute(first, "PRAGMA page_size=" + page);
        execute(first, "PRAGMA journal_mode=WAL");
        execute(first, "PRAGMA wal_autocheckpoint=0");
        execute(first, "PRAGMA cache_size=2");
        execute(first, "CREATE TABLE images(image BLOB NOT NULL) STRICT");
        execute(first, "INSERT INTO images VALUES(zeroblob(" + (page * 3) + "))");
        assertEquals(0, scalar(first, "PRAGMA wal_checkpoint(TRUNCATE)"));
        assertEquals(0, files.usage().walBytes());
        execute(reader, "BEGIN");
        byte[] zero = image(reader);
        byte[] one = zero.clone(); Arrays.fill(one, (byte) 1);
        byte[] two = zero.clone(); Arrays.fill(two, (byte) 2);
        execute(first, "BEGIN IMMEDIATE");
        SealedSqliteImages.walCeiling(first, 32);
        assertThrows(SQLException.class, () -> SealedSqliteImages.walCeiling(second, files.limits().walBytes()));
        fullReplacementRollsBack(first, one);
        assertTrue(files.usage().walBytes() <= 32);
        assertArrayEquals(zero, image(first)); assertArrayEquals(zero, image(reader));

        // A different connection may use its own ceiling, not this writer's 32 bytes.
        execute(second, "BEGIN IMMEDIATE");
        SealedSqliteImages.walCeiling(second, 2L << 20);
        SealedSqliteImages.replace(second, "images", "image", 1, one);
        execute(second, "COMMIT");
        assertTrue(files.usage().walBytes() > 32);
        assertArrayEquals(one, image(first)); assertArrayEquals(zero, image(reader));
        execute(first, "BEGIN IMMEDIATE");
        // Rollback did not silently relax the first connection's own ceiling.
        fullReplacementRollsBack(first, two);
        assertArrayEquals(one, image(first));
        execute(first, "BEGIN IMMEDIATE");
        SealedSqliteImages.walCeiling(first, 2L << 20);
        SealedSqliteImages.replace(first, "images", "image", 1, two);
        execute(first, "COMMIT");
        assertArrayEquals(two, image(second)); assertArrayEquals(zero, image(reader));
        assertTrue(files.usage().walBytes() <= 2L << 20);
        integrity(first);
        execute(reader, "ROLLBACK");
      }
    }
  }

  @Test void aWalHandleOpenedLaterInheritsItsMainConnectionsCeiling() throws Exception {
    try (var connection = files("late-wal").connect()) {
      execute(connection, "PRAGMA journal_mode=DELETE");
      execute(connection, "CREATE TABLE images(image BLOB NOT NULL) STRICT");
      execute(connection, "INSERT INTO images VALUES(zeroblob(1024))");
      execute(connection, "BEGIN IMMEDIATE");
      SealedSqliteImages.walCeiling(connection, 0);
      execute(connection, "ROLLBACK");
      execute(connection, "PRAGMA journal_mode=WAL");
      execute(connection, "BEGIN IMMEDIATE");
      byte[] bytes = new byte[1024]; Arrays.fill(bytes, (byte) 1);
      fullReplacementRollsBack(connection, bytes);
      assertArrayEquals(new byte[1024], image(connection)); integrity(connection);
    }
  }

  private static void fullReplacementRollsBack(Connection connection, byte[] bytes) throws SQLException {
    SQLException full = assertThrows(SQLException.class, () -> {
      SealedSqliteImages.replace(connection, "images", "image", 1, bytes);
      execute(connection, "COMMIT");
    });
    assertTrue(SealedSqliteFiles.isFull(full), full.toString());
    try { execute(connection, "ROLLBACK"); }
    catch (SQLException automaticRollback) { assertTrue(automaticRollback.getMessage().contains("no transaction")); }
  }

  @Test void exactCapacityWritesPreserveRowsPagesAndRollbackWithoutSqlUpdate() throws Exception {
    for (int page : List.of(512, 4096, 65536)) {
      var files = files("pages-" + page);
      try (var connection = files.connect()) {
        execute(connection, "PRAGMA page_size=" + page);
        execute(connection, "PRAGMA journal_mode=WAL");
        execute(connection, "PRAGMA cache_size=2");
        execute(connection, "CREATE TABLE images(id INTEGER PRIMARY KEY, image BLOB NOT NULL) STRICT");
        execute(connection, "CREATE TRIGGER no_update BEFORE UPDATE ON images BEGIN SELECT RAISE(ABORT,'SQL UPDATE forbidden'); END");
        execute(connection, "CREATE TRIGGER no_delete BEFORE DELETE ON images BEGIN SELECT RAISE(ABORT,'SQL DELETE forbidden'); END");
        for (int length : List.of(1, 127, page - 1, page, page + 1, page * 3 + 19)) {
          long rowid = scalar(connection, "SELECT count(*)+1 FROM images");
          execute(connection, "INSERT INTO images VALUES(" + rowid + ",zeroblob(" + length + "))");
          long pages = scalar(connection, "PRAGMA page_count");
          assertEquals(pages, scalar(connection, "PRAGMA max_page_count=" + pages));
          byte[] bytes = new byte[length]; Arrays.fill(bytes, (byte) 0x59);
          execute(connection, "BEGIN IMMEDIATE");
          SealedSqliteImages.replace(connection, "images", "image", rowid, bytes);
          assertThrows(SQLException.class, () -> execute(connection, "UPDATE images SET image=x'00' WHERE id=" + rowid));
          assertArrayEquals(bytes, image(connection, rowid));
          execute(connection, "ROLLBACK");
          assertArrayEquals(new byte[length], image(connection, rowid));
          assertEquals(pages, scalar(connection, "PRAGMA page_count"));
          execute(connection, "BEGIN IMMEDIATE");
          SealedSqliteImages.replace(connection, "images", "image", rowid, bytes);
          execute(connection, "COMMIT");
          assertEquals(rowid, scalar(connection, "SELECT count(*) FROM images"));
          assertEquals(pages, scalar(connection, "PRAGMA page_count"));
          assertArrayEquals(bytes, image(connection, rowid));
          assertEquals(rowid, scalar(connection, "SELECT rowid FROM images WHERE id=" + rowid));
          assertTrue(scalar(connection, "PRAGMA max_page_count=65536") > pages);
        }
        integrity(connection);
      }
      try (var reopened = files.connect()) { integrity(reopened); assertEquals(6, scalar(reopened, "SELECT count(*) FROM images")); }
    }
  }

  @Test void nativeValidationRefusesResizeMissingRowsWrongTypesAndIndexedImages() throws Exception {
    try (var connection = files("invalid").connect()) {
      execute(connection, "CREATE TABLE images(image BLOB NOT NULL) STRICT");
      execute(connection, "INSERT INTO images VALUES(zeroblob(4))");
      execute(connection, "BEGIN IMMEDIATE");
      for (String arguments : List.of("'images','image',1,zeroblob(3)", "'images','image',1,zeroblob(5)",
          "'images','image',1,zeroblob(0)", "'images','image',1,zeroblob(16777217)",
          "'images','image',2,zeroblob(4)", "'images','image',0,zeroblob(4)",
          "'images','image',-1,zeroblob(4)", "'images','image','1',zeroblob(4)",
          "'images','image',1.0,zeroblob(4)", "'images','image',1,'text'",
          "'images','image',1,NULL", "'images','absent',1,zeroblob(4)",
          "'absent','image',1,zeroblob(4)", "'images; DROP TABLE images','image',1,zeroblob(4)",
          "'images'||char(0),'image',1,zeroblob(4)", "x'696d61676573','image',1,zeroblob(4)")) {
        assertThrows(SQLException.class, () -> scalar(connection, "SELECT pipestream_blob_replace(" + arguments + ")"), arguments);
        assertArrayEquals(new byte[4], image(connection));
      }
      execute(connection, "CREATE INDEX indexed_image ON images(image)");
      assertThrows(SQLException.class, () -> SealedSqliteImages.replace(connection, "images", "image", 1, new byte[4]));
      execute(connection, "ROLLBACK");
      assertArrayEquals(new byte[4], image(connection)); integrity(connection);
    }
  }

  @Test void generatedColumnsAreRefusedWithoutChangingStoredImages() throws Exception {
    try (var connection = files("virtual").connect()) {
      execute(connection, "CREATE TABLE images(image BLOB NOT NULL CHECK(length(image)=4), state INTEGER GENERATED ALWAYS AS (CASE substr(image,1,1) WHEN x'00' THEN 0 WHEN x'01' THEN 1 ELSE NULL END) VIRTUAL) STRICT");
      execute(connection, "INSERT INTO images(image) VALUES(zeroblob(4))");
      assertEquals(0, scalar(connection, "SELECT state FROM images"));
      execute(connection, "BEGIN IMMEDIATE");
      SQLException refusal = assertThrows(SQLException.class,
          () -> SealedSqliteImages.replace(connection, "images", "image", 1, new byte[]{1, 0, 0, 0}));
      assertTrue(refusal.getMessage().contains("generated columns"));
      assertEquals(0, scalar(connection, "SELECT state FROM images"));
      execute(connection, "COMMIT");
      assertEquals(0, scalar(connection, "SELECT state FROM images"));
      assertArrayEquals(new byte[4], image(connection)); integrity(connection);
    }
  }

  private static void execute(Connection connection, String sql) throws SQLException {
    try (var statement = connection.createStatement()) { statement.execute(sql); }
  }
  private static long scalar(Connection connection, String sql) throws SQLException {
    try (var statement = connection.createStatement(); var rows = statement.executeQuery(sql)) {
      assertTrue(rows.next()); return rows.getLong(1);
    }
  }
  private static byte[] image(Connection connection) throws SQLException { return image(connection, 1); }
  private static byte[] image(Connection connection, long rowid) throws SQLException {
    try (var query = connection.prepareStatement("SELECT image FROM images WHERE rowid=?")) {
      query.setLong(1, rowid);
      try (var rows = query.executeQuery()) { assertTrue(rows.next()); return rows.getBytes(1); }
    }
  }
  private static void integrity(Connection connection) throws SQLException {
    try (var statement = connection.createStatement(); var rows = statement.executeQuery("PRAGMA integrity_check")) {
      assertTrue(rows.next()); assertEquals("ok", rows.getString(1)); assertFalse(rows.next());
    }
  }
}
