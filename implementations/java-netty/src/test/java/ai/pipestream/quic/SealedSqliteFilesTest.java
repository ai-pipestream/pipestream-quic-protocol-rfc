package ai.pipestream.quic;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

final class SealedSqliteFilesTest {
  private static final UUID PRODUCER = new UUID(1, 2);
  private static final long UNIT = 65536;
  @TempDir Path directory;

  private static SealedSessionStore.FileLimits limits(long main, long wal, long journal) {
    return new SealedSessionStore.FileLimits(main, wal, journal, UNIT);
  }
  private static SealedWork.Declaration declaration(long sequence, List<Long> ids) {
    return new SealedWork.Declaration("bounded", PRODUCER, 0, null, BigInteger.valueOf(sequence), ids, 0, null);
  }
  private static long scalar(Connection connection, String sql) throws SQLException {
    try (var statement = connection.createStatement(); var rows = statement.executeQuery(sql)) {
      assertTrue(rows.next()); return rows.getLong(1);
    }
  }
  private static void execute(Connection connection, String sql) throws SQLException {
    try (var statement = connection.createStatement()) { statement.execute(sql); }
  }
  private static void integrity(Connection connection) throws SQLException {
    try (var statement = connection.createStatement(); var rows = statement.executeQuery("PRAGMA integrity_check")) {
      assertTrue(rows.next()); assertEquals("ok", rows.getString(1)); assertFalse(rows.next());
    }
  }

  @Test void immutablePolicyAndGuardSelectionSurviveReopenWithoutChangingTheDefaultVfs() throws Exception {
    Path path = directory.resolve("spaces and ?query.db");
    var policy = limits(4L << 20, 2L << 20, 2L << 20);
    var store = SealedSessionStore.open(path, policy);
    var request = declaration(0, List.of(1L, 2L));
    store.declare(request, 7, 100);
    assertEquals(policy, SealedSessionStore.open(path).fileLimits());
    assertEquals(request.acknowledgement(), SealedSessionStore.open(path, policy).declare(request, 7, 100));
    assertThrows(IOException.class, () -> SealedSessionStore.open(path, SealedSessionStore.FileLimits.defaults()));
    var files = SealedSqliteFiles.open(path, policy);
    try (var connection = files.connect()) {
      assertEquals(0, scalar(connection, "PRAGMA mmap_size=1048576"));
      assertThrows(SQLException.class, () -> execute(connection, "SELECT load_extension('must-not-load')"));
      assertThrows(SQLException.class, () -> execute(connection, "SELECT pipestream_guard_unregister('anything')"));
      integrity(connection);
    }
    // The named VFS refuses an unregistered path instead of using unbounded I/O.
    Path unregistered = directory.resolve("unregistered.db");
    assertThrows(SQLException.class, () -> java.sql.DriverManager.getConnection(
        "jdbc:sqlite:" + unregistered.toUri() + "?vfs=pipestream-java-bounded-unix-v1"));
    assertFalse(Files.exists(unregistered));
    try (var plain = java.sql.DriverManager.getConnection("jdbc:sqlite:" + directory.resolve("ordinary.db"))) {
      execute(plain, "CREATE TABLE ordinary(value)");
    }
    assertFalse(Files.exists(directory.resolve("ordinary.db.psjlimits")));
  }

  @Test void pageExhaustionIsNamedAndDoesNotPartiallyDeclareOrEraseReplay() throws Exception {
    Path path = directory.resolve("pages.db");
    var policy = limits(2 * UNIT, 4L << 20, 4L << 20);
    var store = SealedSessionStore.open(path, policy);
    SealedWork.Declaration last = null;
    long committed = 0;
    boolean refused = false;
    for (int batch = 0; batch < 32; batch++) {
      long start = batch * 256L + 1;
      var request = declaration(batch, java.util.stream.LongStream.range(start, start + 256).boxed().toList());
      try { store.declare(request, 7, 16384); last = request; committed += 256; }
      catch (ProtocolException full) { assertEquals(Wire.ERROR_LIMIT_EXCEEDED, full.errorCode()); refused = true; break; }
    }
    assertTrue(refused); assertNotNull(last); assertTrue(committed > 0);
    assertEquals(committed, store.declared("bounded", PRODUCER, 0).size());
    assertEquals(last.acknowledgement(), store.declare(last, 7, 16384));
    assertTrue(store.fileUsage().databaseBytes() <= policy.databaseBytes());
    assertEquals(last.acknowledgement(), SealedSessionStore.open(path).declare(last, 7, 16384));
    try (var connection = SealedSqliteFiles.open(path, policy).connect()) { integrity(connection); }
  }

  @Test void heldWalReaderExhaustsTheActualFileAndRollbackPreservesCommittedRows() throws Exception {
    Path path = directory.resolve("wal.db");
    var files = SealedSqliteFiles.open(path, limits(4L << 20, UNIT, 2L << 20));
    try (var writer = files.connect()) {
      execute(writer, "PRAGMA journal_mode=WAL"); execute(writer, "PRAGMA wal_autocheckpoint=0");
      execute(writer, "PRAGMA synchronous=FULL"); execute(writer, "PRAGMA busy_timeout=0");
      execute(writer, "CREATE TABLE retained(value BLOB)");
      try (var reader = files.connect()) {
        execute(reader, "BEGIN"); assertEquals(0, scalar(reader, "SELECT count(*) FROM retained"));
        int committed = 0;
        SQLException refused = null;
        for (int i = 0; i < 128; i++) {
          try { execute(writer, "INSERT INTO retained VALUES(zeroblob(4000))"); committed++; }
          catch (SQLException full) { refused = full; break; }
        }
        assertNotNull(refused); assertTrue(SealedSqliteFiles.isFull(refused), refused.toString());
        assertTrue(committed > 0); assertEquals(committed, scalar(writer, "SELECT count(*) FROM retained"));
        assertTrue(files.usage().walBytes() <= UNIT);
        try (var statement = writer.createStatement(); var checkpoint = statement.executeQuery("PRAGMA wal_checkpoint(TRUNCATE)")) {
          assertTrue(checkpoint.next()); assertEquals(1, checkpoint.getInt(1));
        }
        assertEquals(0, scalar(reader, "SELECT count(*) FROM retained"));
        execute(reader, "ROLLBACK");
      }
      try (var statement = writer.createStatement(); var checkpoint = statement.executeQuery("PRAGMA wal_checkpoint(TRUNCATE)")) {
        assertTrue(checkpoint.next()); assertEquals(0, checkpoint.getInt(1));
      }
      assertEquals(0, files.usage().walBytes());
      execute(writer, "INSERT INTO retained VALUES(zeroblob(4000))"); integrity(writer);
    }
  }

  @Test void rollbackJournalExhaustionCannotCommitPartialUpdates() throws Exception {
    var files = SealedSqliteFiles.open(directory.resolve("journal.db"), limits(4L << 20, 2L << 20, UNIT));
    try (var connection = files.connect()) {
      execute(connection, "PRAGMA journal_mode=DELETE"); execute(connection, "PRAGMA cache_size=8");
      execute(connection, "CREATE TABLE retained(id INTEGER PRIMARY KEY, value BLOB)");
      for (int i = 0; i < 40; i++) execute(connection, "INSERT INTO retained VALUES(" + i + ",zeroblob(4000))");
      execute(connection, "BEGIN IMMEDIATE");
      SQLException failure = assertThrows(SQLException.class, () -> {
        execute(connection, "UPDATE retained SET value=randomblob(4000)"); execute(connection, "COMMIT");
      });
      assertTrue(SealedSqliteFiles.isFull(failure), failure.toString());
      try { execute(connection, "ROLLBACK"); } catch (SQLException alreadyRolledBack) { assertTrue(alreadyRolledBack.getMessage().contains("no transaction")); }
      assertEquals(40, scalar(connection, "SELECT count(*) FROM retained WHERE value=zeroblob(4000)"));
      assertTrue(files.usage().journalBytes() <= UNIT); integrity(connection);
    }
  }

  @Test void changedMissingCorruptAndAliasedPoliciesDoNotCreateFreeCapacity() throws Exception {
    for (String mutation : List.of("corrupt", "missing", "symlink", "hardlink")) {
      Path path = directory.resolve(mutation + ".db");
      var store = SealedSessionStore.open(path);
      var request = declaration(0, List.of(1L)); store.declare(request, 7, 100);
      Path policy = path.resolveSibling(path.getFileName() + ".psjlimits");
      byte[] before = Files.readAllBytes(path);
      if (mutation.equals("corrupt")) {
        byte[] bytes = Files.readAllBytes(policy); bytes[8] ^= 1; Files.write(policy, bytes);
      } else if (mutation.equals("missing")) Files.delete(policy);
      else if (mutation.equals("hardlink")) Files.createLink(directory.resolve("alias"), path);
      else {
        Path target = directory.resolve("saved-policy"); Files.move(policy, target); Files.createSymbolicLink(policy, target);
      }
      assertThrows(IOException.class, () -> SealedSessionStore.open(path));
      assertThrows(SQLException.class, () -> store.declare(request, 7, 100));
      assertArrayEquals(before, Files.readAllBytes(path));
    }
  }

  @Test void nativeRegistrationIsReclaimedAcrossManyIndependentStoresAndConcurrentHandles() throws Exception {
    for (int index = 0; index < 80; index++) {
      var store = SealedSessionStore.open(directory.resolve("store-" + index + ".db"));
      assertEquals(declaration(0, List.of(1L)).acknowledgement(), store.declare(declaration(0, List.of(1L)), 7, 100));
    }
    Path path = directory.resolve("shared.db");
    var store = SealedSessionStore.open(path);
    var other = SealedSessionStore.open(path);
    var request = declaration(0, List.of(1L, 2L));
    try (var workers = Executors.newFixedThreadPool(8)) {
      var tasks = java.util.stream.IntStream.range(0, 64).mapToObj(index -> workers.submit(
          () -> (index % 2 == 0 ? store : other).declare(request, 7, 100))).toList();
      for (var task : tasks) assertEquals(request.acknowledgement(), task.get(10, TimeUnit.SECONDS));
    }
    assertEquals(2, store.declared("bounded", PRODUCER, 0).size());
  }

  @Test void concurrentDatabaseRegistryHasANamedBoundAndReleasesCapacityOnClose() throws Exception {
    var store = SealedSessionStore.open(directory.resolve("refused.db"));
    var connections = new java.util.ArrayList<Connection>();
    try {
      for (int index = 0; index < 64; index++) {
        connections.add(SealedSqliteFiles.open(directory.resolve("held-" + index + ".db"), null).connect());
      }
      var request = declaration(0, List.of(1L));
      ProtocolException refusal = assertThrows(ProtocolException.class, () -> store.declare(request, 7, 100));
      assertEquals(Wire.ERROR_LIMIT_EXCEEDED, refusal.errorCode());
      connections.removeLast().close();
      assertEquals(request.acknowledgement(), store.declare(request, 7, 100));
    } finally {
      for (var connection : connections) connection.close();
    }
  }

  @Test void preexistingOversizeFilesRefuseBeforeSqliteCanCheckpointOrRepairThem() throws Exception {
    for (String suffix : List.of("", "-wal", "-journal", "-shm")) {
      Path path = directory.resolve("oversize" + (suffix.isEmpty() ? "-main" : suffix) + ".db");
      var policy = limits(4L << 20, UNIT, UNIT);
      var store = SealedSessionStore.open(path, policy);
      var request = declaration(0, List.of(1L)); store.declare(request, 7, 100);
      Path oversized = path.resolveSibling(path.getFileName() + suffix);
      long cap = suffix.isEmpty() ? policy.databaseBytes() : UNIT;
      try (var file = java.nio.channels.FileChannel.open(oversized,
          java.nio.file.StandardOpenOption.CREATE, java.nio.file.StandardOpenOption.WRITE)) {
        file.position(cap); file.write(java.nio.ByteBuffer.wrap(new byte[]{1})); file.force(true);
      }
      assertThrows(IOException.class, () -> SealedSessionStore.open(path));
      assertThrows(SQLException.class, () -> store.declare(request, 7, 100));
      assertEquals(cap + 1, Files.size(oversized));
    }
    assertThrows(IllegalArgumentException.class, () -> limits(0, UNIT, UNIT));
    assertThrows(IllegalArgumentException.class, () -> limits(UNIT + 1, UNIT, UNIT));
    assertThrows(IllegalArgumentException.class, () -> limits(Long.MAX_VALUE, UNIT, UNIT));
    assertThrows(IllegalArgumentException.class, () -> new SealedSessionStore.FileLimits(UNIT, UNIT, UNIT, 32L << 20));
  }

  @Test void abruptExitWithUncheckpointedWalRetainsAcknowledgedMembershipAndPolicy() throws Exception {
    Path database = directory.resolve("crash.db"), log = directory.resolve("child.log");
    Process child = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin/java").toString(),
        "--enable-native-access=ALL-UNNAMED", "-cp", System.getProperty("java.class.path"),
        CrashWriter.class.getName(), database.toString()).redirectErrorStream(true).redirectOutput(log.toFile()).start();
    try {
      assertTrue(child.waitFor(30, TimeUnit.SECONDS)); assertEquals(37, child.exitValue(), () -> read(log));
    } finally { if (child.isAlive()) child.destroyForcibly().waitFor(5, TimeUnit.SECONDS); }
    assertTrue(Files.size(database.resolveSibling("crash.db-wal")) > 0);
    var reopened = SealedSessionStore.open(database);
    var request = declaration(0, List.of(1L, 2L));
    assertEquals(request.acknowledgement(), reopened.declare(request, 7, 100));
    assertEquals(limits(4L << 20, 2L << 20, 2L << 20), reopened.fileLimits());
    assertFalse(reopened.checkpointReady("bounded", PRODUCER, 0, 2));
  }

  public static final class CrashWriter {
    private CrashWriter() {}
    public static void main(String[] args) throws Exception {
      Path path = Path.of(args[0]); var policy = limits(4L << 20, 2L << 20, 2L << 20);
      var store = SealedSessionStore.open(path, policy);
      try (var held = SealedSqliteFiles.open(path, policy).connect()) {
        execute(held, "BEGIN"); scalar(held, "SELECT count(*) FROM ps_java_sessions");
        store.declare(declaration(0, List.of(1L, 2L)), 7, 100);
        Runtime.getRuntime().halt(37);
      }
    }
  }
  private static String read(Path path) { try { return Files.readString(path); } catch (IOException e) { return e.toString(); } }
}
