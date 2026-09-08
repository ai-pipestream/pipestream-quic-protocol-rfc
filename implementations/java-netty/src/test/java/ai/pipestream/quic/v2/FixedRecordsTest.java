package ai.pipestream.quic.v2;

import static org.junit.jupiter.api.Assertions.*;

import ai.pipestream.quic.BoundedSqlite;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

final class FixedRecordsTest {
  @TempDir Path directory;

  @Test
  void ordinaryRewritePreservesCreditsAndSpendConsumesExactlyOne() throws Exception {
    try (Connection connection = initialized("credits")) {
      byte[] key = key(1);
      begin(connection);
      long slot =
          FixedRecords.allocate(
              connection, limits(), FixedRecords.Kind.WORK, key, new byte[] {1}, 128, 2);
      commit(connection);

      begin(connection);
      assertEquals(
          2, FixedRecords.read(connection, slot, FixedRecords.Kind.WORK, key).header().credits());
      long revision =
          FixedRecords.replace(
              connection, limits(), slot, FixedRecords.Kind.WORK, key, 1, new byte[] {2, 3}, false);
      assertEquals(2, revision);
      assertEquals(
          2, FixedRecords.read(connection, slot, FixedRecords.Kind.WORK, key).header().credits());
      revision =
          FixedRecords.replace(
              connection,
              limits(),
              slot,
              FixedRecords.Kind.WORK,
              key,
              revision,
              new byte[] {4},
              true);
      assertEquals(1, FixedRecords.header(connection, slot, FixedRecords.Kind.WORK).credits());
      commit(connection);

      begin(connection);
      assertEquals(
          4,
          FixedRecords.replace(
              connection,
              limits(),
              slot,
              FixedRecords.Kind.WORK,
              key,
              revision,
              new byte[] {5},
              true));
      assertEquals(0, FixedRecords.header(connection, slot, FixedRecords.Kind.WORK).credits());
      assertThrows(
          ProtocolError.class,
          () ->
              FixedRecords.replace(
                  connection,
                  limits(),
                  slot,
                  FixedRecords.Kind.WORK,
                  key,
                  4,
                  new byte[] {6},
                  true));
      rollback(connection);
    }
  }

  @Test
  void staleRevisionCrossIdentityAndDamagedPaddingAreRefused() throws Exception {
    Path path = directory.resolve("corrupt.sqlite");
    long slot;
    byte[] key = key(1);
    try (Connection connection = initialized(path)) {
      begin(connection);
      slot =
          FixedRecords.allocate(
              connection, limits(), FixedRecords.Kind.FENCE, key, new byte[] {(byte) 0xf6}, 128, 1);
      commit(connection);
      assertThrows(
          SQLException.class,
          () -> FixedRecords.read(connection, slot, FixedRecords.Kind.FENCE, key(2)));
      begin(connection);
      assertThrows(
          ProtocolError.class,
          () ->
              FixedRecords.replace(
                  connection,
                  limits(),
                  slot,
                  FixedRecords.Kind.FENCE,
                  key,
                  2,
                  new byte[] {1},
                  false));
      rollback(connection);

      byte[] image = image(connection, slot);
      image[image.length - 1] = 1;
      begin(connection);
      BoundedSqlite.replaceImage(connection, "ps_v2_slots", "image", slot, image);
      commit(connection);
      assertThrows(
          SQLException.class,
          () -> FixedRecords.read(connection, slot, FixedRecords.Kind.FENCE, key));
    }
  }

  @Test
  void growPreservesBodyRevisionAndCredits() throws Exception {
    try (Connection connection = initialized("grow")) {
      byte[] key = key(3);
      begin(connection);
      long slot =
          FixedRecords.allocate(
              connection, limits(), FixedRecords.Kind.WORK, key, new byte[] {1, 2, 3}, 128, 1);
      commit(connection);
      begin(connection);
      FixedRecords.grow(connection, limits(), slot, FixedRecords.Kind.WORK, key, 1, 4096, 3);
      FixedRecords.Snapshot grown =
          FixedRecords.read(connection, slot, FixedRecords.Kind.WORK, key);
      assertArrayEquals(new byte[] {1, 2, 3}, grown.body());
      assertEquals(1, grown.header().revision());
      assertEquals(3, grown.header().credits());
      assertEquals(4096, grown.header().capacity());
      commit(connection);
    }
  }

  @Test
  void ordinaryProtectedWritesSaturatePinnedWalButPromisedRewriteStillCommits() throws Exception {
    Path path = directory.resolve("pinned.sqlite");
    BoundedSqlite.Limits limits =
        new BoundedSqlite.Limits(64L << 20, 1L << 20, 64L << 20, 64L << 10);
    try (Connection writer = initialized(path, limits, 4096);
        Connection reader = BoundedSqlite.open(path, limits).connect()) {
      byte[] key = key(4);
      begin(writer);
      long slot =
          FixedRecords.allocate(
              writer,
              limits,
              FixedRecords.Kind.WORK,
              key,
              new byte[] {1},
              FixedRecords.WORK_CAPACITY,
              1);
      execute(writer, "CREATE TABLE ordinary(id INTEGER PRIMARY KEY,image BLOB NOT NULL) STRICT");
      execute(writer, "INSERT INTO ordinary VALUES(1,zeroblob(4096))");
      commit(writer);
      execute(writer, "PRAGMA wal_checkpoint(TRUNCATE)");
      execute(reader, "BEGIN");
      image(reader, slot);
      long beforeWal = sidecar(path, "-wal");
      int writes = 0;
      SQLException full = null;
      for (int value = 1; value <= 10_000; value++) {
        begin(writer);
        try {
          FixedRecords.protect(writer, limits);
          execute(writer, "UPDATE ordinary SET image=randomblob(4096) WHERE id=1");
          commit(writer);
          writes++;
        } catch (SQLException failure) {
          full = failure;
          try {
            rollback(writer);
          } catch (SQLException ignoredAutomaticRollback) {
            // SQLITE_FULL may roll the transaction back itself.
          }
          break;
        }
      }
      assertTrue(writes > 0);
      assertNotNull(full, "ordinary protected writes did not saturate the bounded WAL");
      assertEquals(13, full.getErrorCode() & 255, full::toString);
      long saturatedWal = sidecar(path, "-wal");

      begin(writer);
      long revision = FixedRecords.header(writer, slot, FixedRecords.Kind.WORK).revision();
      FixedRecords.replace(
          writer, limits, slot, FixedRecords.Kind.WORK, key, revision, new byte[] {3}, true);
      long clockRevision =
          FixedRecords.header(writer, FixedRecords.CLOCK, FixedRecords.Kind.CLOCK).revision();
      FixedRecords.replace(
          writer,
          limits,
          FixedRecords.CLOCK,
          FixedRecords.Kind.CLOCK,
          FixedRecords.clockKey("issuer-a"),
          clockRevision,
          new byte[] {3},
          false);
      commit(writer);
      FixedRecords.Snapshot target = FixedRecords.read(writer, slot, FixedRecords.Kind.WORK, key);
      FixedRecords.Snapshot clock =
          FixedRecords.read(
              writer,
              FixedRecords.CLOCK,
              FixedRecords.Kind.CLOCK,
              FixedRecords.clockKey("issuer-a"));
      assertArrayEquals(new byte[] {3}, target.body());
      assertEquals(2, target.header().revision());
      assertEquals(0, target.header().credits());
      assertArrayEquals(new byte[] {3}, clock.body());
      assertEquals(2, clock.header().revision());
      assertEquals(0, clock.header().credits());
      System.out.printf(
          "fixed-record saturation writes=%d beforeWal=%d saturatedWal=%d afterWal=%d"
              + " remainingCredit=%d%n",
          writes, beforeWal, saturatedWal, sidecar(path, "-wal"), target.header().credits());
      execute(reader, "ROLLBACK");
    }
  }

  @Test
  void costFormulaSanityCoversEighteenGeometries() {
    int cases = 0;
    for (long page : List.of(512L, 4096L, 65536L)) {
      for (int capacity : List.of(1, 127, 128, 2048, 65536, FixedRecords.MAX_CAPACITY)) {
        long frame = page + 24;
        long dirty =
            Math.ceilDiv((long) FixedRecords.HEADER + capacity, page - 4)
                + 1
                + Math.ceilDiv((long) FixedRecords.HEADER + FixedRecords.CLOCK_CAPACITY, page - 4)
                + 1;
        long expected = 32 + (dirty + 1 + Math.ceilDiv(65536, frame)) * frame;
        assertEquals(expected, FixedRecords.cost(capacity, page));
        cases++;
      }
    }
    assertEquals(18, cases);
  }

  @Test
  void eighteenNativeRewriteCasesFitCostWithoutPageGrowth() throws Exception {
    int cases = 0;
    for (int page : List.of(512, 4096, 65536)) {
      for (int capacity : List.of(1, 127, 128, 2048, 65536, FixedRecords.MAX_CAPACITY)) {
        Path path = directory.resolve("native-" + page + "-" + capacity + ".sqlite");
        try (Connection writer = initialized(path, limits(), page);
            Connection reader = BoundedSqlite.open(path, limits()).connect()) {
          assertEquals(page, scalar(writer, "PRAGMA page_size"));
          byte[] key = key(++cases);
          byte[] initial = pattern(capacity, 0x35);
          byte[] replacement = pattern(capacity, 0x6a);
          begin(writer);
          long slot =
              FixedRecords.allocate(
                  writer, limits(), FixedRecords.Kind.WORK, key, initial, capacity, 1);
          commit(writer);
          execute(
              writer,
              "CREATE TRIGGER no_slot_update BEFORE UPDATE ON ps_v2_slots BEGIN SELECT"
                  + " RAISE(ABORT,'SQL UPDATE forbidden'); END");
          execute(writer, "PRAGMA wal_checkpoint(TRUNCATE)");
          long pages = scalar(writer, "PRAGMA page_count");
          assertEquals(pages, scalar(writer, "PRAGMA max_page_count=" + pages));
          execute(reader, "BEGIN");
          image(reader, slot);
          long beforeWal = sidecar(path, "-wal");
          begin(writer);
          FixedRecords.replace(
              writer, limits(), slot, FixedRecords.Kind.WORK, key, 1, replacement, true);
          FixedRecords.replace(
              writer,
              limits(),
              FixedRecords.CLOCK,
              FixedRecords.Kind.CLOCK,
              FixedRecords.clockKey("issuer-a"),
              1,
              new byte[] {1},
              false);
          commit(writer);
          long delta = sidecar(path, "-wal") - beforeWal;
          assertTrue(delta > 0 && delta <= FixedRecords.cost(capacity, page));
          assertEquals(pages, scalar(writer, "PRAGMA page_count"));
          FixedRecords.Snapshot target =
              FixedRecords.read(writer, slot, FixedRecords.Kind.WORK, key);
          FixedRecords.Snapshot clock =
              FixedRecords.read(
                  writer,
                  FixedRecords.CLOCK,
                  FixedRecords.Kind.CLOCK,
                  FixedRecords.clockKey("issuer-a"));
          assertArrayEquals(replacement, target.body());
          assertEquals(2, target.header().revision());
          assertEquals(0, target.header().credits());
          assertArrayEquals(new byte[] {1}, clock.body());
          assertEquals(2, clock.header().revision());
          assertEquals(0, clock.header().credits());
          System.out.printf(
              "fixed-record page=%d capacity=%d pages=%d walDelta=%d cost=%d%n",
              page, capacity, pages, delta, FixedRecords.cost(capacity, page));
          execute(reader, "ROLLBACK");
        }
      }
    }
    assertEquals(18, cases);
  }

  @Test
  void failedPhysicalGrowRollsBackExactImageAcrossReopen() throws Exception {
    Path path = directory.resolve("grow-full.sqlite");
    byte[] key = key(9);
    byte[] body = pattern(128, 0x27);
    long slot;
    try (Connection connection = initialized(path)) {
      begin(connection);
      slot = FixedRecords.allocate(connection, limits(), FixedRecords.Kind.WORK, key, body, 128, 1);
      commit(connection);
      execute(connection, "PRAGMA wal_checkpoint(TRUNCATE)");
      long pages = scalar(connection, "PRAGMA page_count");
      assertEquals(pages, scalar(connection, "PRAGMA max_page_count=" + pages));
      begin(connection);
      SQLException full =
          assertThrows(
              SQLException.class,
              () ->
                  FixedRecords.grow(
                      connection, limits(), slot, FixedRecords.Kind.WORK, key, 1, 65536, 2));
      assertEquals(13, full.getErrorCode() & 255, full::toString);
      try {
        rollback(connection);
      } catch (SQLException ignoredAutomaticRollback) {
        // SQLITE_FULL may roll the transaction back itself.
      }
    }
    try (Connection reopened = BoundedSqlite.open(path, limits()).connect()) {
      FixedRecords.Snapshot original =
          FixedRecords.read(reopened, slot, FixedRecords.Kind.WORK, key);
      assertArrayEquals(body, original.body());
      assertEquals(1, original.header().revision());
      assertEquals(1, original.header().credits());
      assertEquals(128, original.header().capacity());
    }
  }

  @Test
  void validChecksumNullOrStringClockBodyIsRejectedOnRecovery() throws Exception {
    int fixture = 0;
    for (byte[] body : List.of(new byte[] {(byte) 0xf6}, new byte[] {0x61, 0x78})) {
      Path path = directory.resolve("clock-" + fixture++ + ".sqlite");
      SessionStore.Configuration configuration =
          new SessionStore.Configuration(
              "issuer-a",
              new Records.Limits(1, 1, 1, 1 << 20, 1 << 20, 1),
              new Records.Policy(1000, 1000, 1000),
              1,
              1,
              1,
              limits());
      SessionStore.initialize(path, configuration);
      try (Connection connection = BoundedSqlite.open(path, limits()).connect()) {
        begin(connection);
        long revision =
            FixedRecords.header(connection, FixedRecords.CLOCK, FixedRecords.Kind.CLOCK).revision();
        FixedRecords.replace(
            connection,
            limits(),
            FixedRecords.CLOCK,
            FixedRecords.Kind.CLOCK,
            FixedRecords.clockKey("issuer-a"),
            revision,
            body,
            false);
        commit(connection);
      }
      assertThrows(SQLException.class, () -> SessionStore.open(path, configuration));
    }
  }

  private Connection initialized(String name) throws Exception {
    return initialized(directory.resolve(name + ".sqlite"));
  }

  private Connection initialized(Path path) throws Exception {
    return initialized(path, limits(), 4096);
  }

  private Connection initialized(Path path, BoundedSqlite.Limits limits, int page)
      throws Exception {
    Connection connection = BoundedSqlite.open(path, limits).connect();
    boolean success = false;
    try {
      execute(connection, "PRAGMA page_size=" + page);
      execute(connection, "PRAGMA journal_mode=WAL");
      begin(connection);
      FixedRecords.createSchema(connection, "issuer-a");
      commit(connection);
      success = true;
      return connection;
    } finally {
      if (!success) connection.close();
    }
  }

  private static BoundedSqlite.Limits limits() {
    return BoundedSqlite.Limits.defaults();
  }

  private static byte[] key(int value) {
    byte[] key = new byte[32];
    Arrays.fill(key, (byte) value);
    return key;
  }

  private static byte[] pattern(int length, int seed) {
    byte[] bytes = new byte[length];
    for (int index = 0; index < bytes.length; index++) bytes[index] = (byte) (seed + index * 31);
    return bytes;
  }

  private static byte[] image(Connection connection, long slot) throws SQLException {
    try (var query = connection.prepareStatement("SELECT image FROM ps_v2_slots WHERE id=?")) {
      query.setLong(1, slot);
      try (var row = query.executeQuery()) {
        assertTrue(row.next());
        return row.getBytes(1);
      }
    }
  }

  private static long scalar(Connection connection, String sql) throws SQLException {
    try (var statement = connection.createStatement();
        var row = statement.executeQuery(sql)) {
      assertTrue(row.next());
      return row.getLong(1);
    }
  }

  private static long sidecar(Path path, String suffix) throws Exception {
    Path sidecar = Path.of(path + suffix);
    return java.nio.file.Files.exists(sidecar) ? java.nio.file.Files.size(sidecar) : 0;
  }

  private static void begin(Connection connection) throws SQLException {
    execute(connection, "BEGIN IMMEDIATE");
  }

  private static void commit(Connection connection) throws SQLException {
    execute(connection, "COMMIT");
  }

  private static void rollback(Connection connection) throws SQLException {
    execute(connection, "ROLLBACK");
  }

  private static void execute(Connection connection, String sql) throws SQLException {
    try (var statement = connection.createStatement()) {
      statement.execute(sql);
    }
  }
}
