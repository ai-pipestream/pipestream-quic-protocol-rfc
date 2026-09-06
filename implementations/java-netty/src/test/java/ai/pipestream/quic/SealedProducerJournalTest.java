package ai.pipestream.quic;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

final class SealedProducerJournalTest {
  private static final byte[] PEER = SealedWork.sha256().digest(new byte[]{1});
  private static final SealedProducerJournal.Limits LIMITS = new SealedProducerJournal.Limits(100, 1L << 20);
  private static final SealedSessionStore.FileLimits FILES = new SealedSessionStore.FileLimits(1L << 20, 1L << 20, 1L << 20, 65536);
  private static final SealedProducerJournal.Kind DECLARE = SealedProducerJournal.Kind.DECLARATION;
  @TempDir Path directory;

  private SealedProducerJournal open(Path path) throws Exception {
    return SealedProducerJournal.open(path, PEER, LIMITS, FILES);
  }

  @Test void intentAndVerifiedObservationRemainDistinctAcrossReopen() throws Exception {
    Path path = directory.resolve("producer.db");
    try (var journal = open(path)) {
      var entry = journal.begin(DECLARE, new byte[]{1}, new byte[]{2, 3}, 8);
      assertEquals(1, entry.id()); assertEquals(0, entry.revision());
      assertFalse(entry.resolved()); assertArrayEquals(new byte[0], entry.observation());
      byte[] request = entry.request(); request[0] = 99;
      assertArrayEquals(new byte[]{2, 3}, journal.next(0).request());
    }
    try (var journal = open(path)) {
      var pending = journal.next(0);
      assertFalse(pending.resolved()); assertEquals(0, pending.revision());
      var processing = journal.observe(1, 0, new byte[]{2}, false);
      assertEquals(1, processing.revision()); assertFalse(processing.resolved());
      assertNull(journal.next(1));
    }
    try (var journal = open(path)) {
      var processing = journal.next(0);
      assertFalse(processing.resolved()); assertArrayEquals(new byte[]{2}, processing.observation());
      assertEquals(2, journal.observe(1, 1, new byte[]{3}, true).revision());
      assertEquals(2, journal.observe(1, 1, new byte[]{3}, true).revision());
      assertEntity(() -> journal.observe(1, 2, new byte[]{4}, true));
      assertEntity(() -> journal.observe(1, 2, new byte[]{3}, false));
    }
    try (var journal = open(path)) {
      var completed = journal.next(0);
      assertTrue(completed.resolved()); assertEquals(2, completed.revision());
      assertArrayEquals(new byte[]{3}, completed.observation());
    }
  }

  @Test void identicalReplayAtFullLogicalQuotaKeepsTheOriginalReservation() throws Exception {
    Path path = directory.resolve("exact.db");
    var quota = new SealedProducerJournal.Limits(1, 1 + 2 + 8 + 56);
    try (var journal = SealedProducerJournal.open(path, PEER, quota, FILES)) {
      journal.begin(DECLARE, new byte[]{1}, new byte[]{2, 3}, 8);
      var usage = journal.usage();
      assertEquals(1, journal.begin(DECLARE, new byte[]{1}, new byte[]{2, 3}, 8).id());
      assertEntity(() -> journal.begin(DECLARE, new byte[]{1}, new byte[]{4, 5}, 8));
      assertEntity(() -> journal.begin(DECLARE, new byte[]{1}, new byte[]{2, 3}, 7));
      assertLimit(() -> journal.begin(DECLARE, new byte[]{2}, new byte[]{2, 3}, 8));
      journal.observe(1, 0, new byte[8], true);
      assertEquals(usage, journal.usage());
    }
    try (var journal = SealedProducerJournal.open(path, PEER, quota, FILES)) {
      assertTrue(journal.begin(DECLARE, new byte[]{1}, new byte[]{2, 3}, 8).resolved());
    }
  }

  @Test void requestAndObservationLimitsRefuseWithoutPartialState() throws Exception {
    try (var journal = open(directory.resolve("limits.db"))) {
      assertLimit(() -> journal.begin(DECLARE, new byte[257], new byte[]{1}, 1));
      assertLimit(() -> journal.begin(DECLARE, new byte[]{1}, new byte[0], 1));
      assertLimit(() -> journal.begin(DECLARE, new byte[]{1}, new byte[]{1}, 4097));
      assertEquals(0, journal.usage().operations());
      journal.begin(DECLARE, new byte[]{1}, new byte[]{1}, 1);
      assertLimit(() -> journal.observe(1, 0, new byte[]{1, 2}, true));
      assertEntity(() -> journal.observe(2, 0, new byte[]{1}, true));
      assertEntity(() -> journal.observe(1, 1, new byte[]{1}, true));
      assertEquals(0, journal.next(0).revision()); assertFalse(journal.next(0).resolved());
    }
  }

  @Test void actualDeclarationAndCheckpointImagesPreserveUnsignedValuesAndOptionalPresence() throws Exception {
    Path path = directory.resolve("wire.db"); UUID producer = new UUID(1, 2);
    var parent = new SealedWork.EntityKey(0, 1);
    var declaration = new SealedWork.Declaration("producer-wire", producer, 7, parent, SealedCbor.MAX_UINT,
        List.of(1L, Wire.MAX_ENTITY_ID), SealedWork.SEAL,
        SealedWork.sealDigest("producer-wire", producer, 7, parent, List.of(1L, Wire.MAX_ENTITY_ID)));
    var absent = new SealedTransport.Checkpoint("cut", BigInteger.ONE.shiftLeft(63), Wire.MAX_ENTITY_ID, null, 0, null);
    var present = new SealedTransport.Checkpoint("cut", SealedCbor.MAX_UINT, Wire.MAX_ENTITY_ID, 0L, 0, BigInteger.ZERO);
    try (var journal = open(path)) {
      var entry = journal.begin(DECLARE, ByteBuffer.allocate(12).putInt(7).putLong(-1L).array(), SealedWork.encode(declaration), 4096);
      journal.observe(entry.id(), 0, SealedWork.encode(declaration.acknowledgement()), true);
      for (var cut : List.of(absent, present)) {
        byte[] key = ByteBuffer.allocate(12).putInt(0).putLong(cut.sequence().longValue()).array();
        entry = journal.begin(SealedProducerJournal.Kind.CHECKPOINT, key, SealedTransport.checkpoint(cut), 4096);
        journal.observe(entry.id(), 0, SealedTransport.checkpoint(cut.acknowledgement()), true);
      }
    }
    try (var journal = open(path)) {
      var entry = journal.next(0);
      assertEquals(declaration, SealedWork.decode(entry.request()));
      SealedWork.requireAcknowledgement(declaration, SealedWork.decode(entry.observation()));
      long cursor = 1;
      for (var cut : List.of(absent, present)) {
        entry = journal.next(cursor++);
        assertEquals(cut, SealedTransport.checkpoint(Wire.decodeControl(entry.request()).payload()));
        assertEquals(cut.acknowledgement(), SealedTransport.checkpoint(Wire.decodeControl(entry.observation()).payload()));
      }
      assertEquals(3, journal.usage().operations());
    }
  }

  @Test void peerAndPoliciesAreImmutableAndForeignServerStoreIsNotConverted() throws Exception {
    Path path = directory.resolve("binding.db");
    try (var journal = open(path)) { journal.begin(DECLARE, new byte[]{1}, new byte[]{1}, 8); }
    assertThrows(SQLException.class, () -> SealedProducerJournal.open(path, new byte[32], LIMITS, FILES));
    assertThrows(SQLException.class, () -> SealedProducerJournal.open(path, PEER, new SealedProducerJournal.Limits(101, 1L << 20), FILES));
    assertThrows(IOException.class, () -> SealedProducerJournal.open(path, PEER, LIMITS,
        new SealedSessionStore.FileLimits(2L << 20, 1L << 20, 1L << 20, 65536)));
    try (var journal = open(path)) { assertEquals(1, journal.usage().operations()); }
    Path serverPath = directory.resolve("server.db");
    SealedSessionStore.open(serverPath);
    try (var connection = DriverManager.getConnection("jdbc:sqlite:" + serverPath)) {
      assertEquals("wal", scalar(connection, "PRAGMA journal_mode"));
    }
    assertThrows(SQLException.class, () -> SealedProducerJournal.open(serverPath, PEER, LIMITS, SealedSessionStore.FileLimits.defaults()));
    try (var connection = DriverManager.getConnection("jdbc:sqlite:" + serverPath)) {
      assertEquals("wal", scalar(connection, "PRAGMA journal_mode"));
    }
    SealedSessionStore.open(serverPath);
    assertThrows(SQLException.class, () -> SealedSessionStore.open(path));
  }

  @Test void corruptionOfRequestsObservationsOrAppendFrontierFailsClosed() throws Exception {
    List<String> corruptions = List.of(
        "UPDATE ps_producer_operations SET request=x'09' WHERE id=1",
        "UPDATE ps_producer_operations SET image=zeroblob(length(image)) WHERE id=1",
        "DELETE FROM ps_producer_operations WHERE id=2",
        "DELETE FROM ps_producer_operations WHERE id=1",
        "UPDATE ps_producer_meta SET head=zeroblob(length(head))",
        "CREATE VIEW unexpected AS SELECT 1");
    int index = 0;
    for (String mutation : corruptions) {
      Path path = directory.resolve("corrupt-" + index++ + ".db");
      try (var journal = open(path)) {
        journal.begin(DECLARE, new byte[]{1}, new byte[]{1}, 8);
        journal.begin(DECLARE, new byte[]{2}, new byte[]{2}, 8);
      }
      try (var connection = DriverManager.getConnection("jdbc:sqlite:" + path); var statement = connection.createStatement()) {
        statement.execute(mutation);
      }
      Exception failure = assertThrows(Exception.class, () -> open(path), mutation);
      assertTrue(failure instanceof SQLException || failure instanceof ProtocolException, failure.toString());
    }
  }

  @Test void observationsCannotBeTransplantedBetweenRowsOrJournals() throws Exception {
    for (boolean differentJournal : List.of(false, true)) {
      Path first = directory.resolve("first-" + differentJournal + ".db");
      Path second = differentJournal ? directory.resolve("second.db") : first;
      try (var journal = open(first)) {
        journal.begin(DECLARE, new byte[]{1}, new byte[]{1}, 8);
        journal.begin(DECLARE, new byte[]{2}, new byte[]{2}, 8);
        journal.observe(1, 0, new byte[]{3}, true);
      }
      if (differentJournal) try (var journal = open(second)) { journal.begin(DECLARE, new byte[]{1}, new byte[]{1}, 8); }
      byte[] image;
      try (var connection = DriverManager.getConnection("jdbc:sqlite:" + first);
          var query = connection.createStatement(); var rows = query.executeQuery("SELECT image FROM ps_producer_operations WHERE id=1")) {
        assertTrue(rows.next()); image = rows.getBytes(1);
      }
      try (var connection = DriverManager.getConnection("jdbc:sqlite:" + second);
          var update = connection.prepareStatement("UPDATE ps_producer_operations SET image=? WHERE id=?")) {
        update.setBytes(1, image); update.setInt(2, differentJournal ? 1 : 2); update.executeUpdate();
      }
      assertEquals(4, assertThrows(ProtocolException.class, () -> open(second)).errorCode());
    }
  }

  @Test void oversizedRetainedPolicyRefusesBeforeReadingItsBlob() throws Exception {
    Path path = directory.resolve("oversized-policy.db");
    try (var journal = open(path)) { journal.begin(DECLARE, new byte[]{1}, new byte[]{1}, 8); }
    try (var connection = DriverManager.getConnection("jdbc:sqlite:" + path); var statement = connection.createStatement()) {
      statement.execute("PRAGMA ignore_check_constraints=ON");
      statement.execute("UPDATE ps_producer_meta SET policy=zeroblob(65536)");
    }
    assertEquals("invalid producer policy length", assertThrows(SQLException.class, () -> open(path)).getMessage());
  }

  @Test void writerOwnershipAndClosedHandlesRefuse() throws Exception {
    Path path = directory.resolve("ownership.db");
    var journal = open(path);
    try {
      journal.begin(DECLARE, new byte[]{1}, new byte[]{1}, 8);
      assertThrows(IOException.class, () -> open(path));
      Path alias = directory.resolve("parent-alias"); Files.createSymbolicLink(alias, directory);
      assertThrows(IOException.class, () -> open(alias.resolve("ownership.db")));
      try (var external = DriverManager.getConnection("jdbc:sqlite:" + path); var statement = external.createStatement()) {
        statement.execute("PRAGMA busy_timeout=1");
        assertThrows(SQLException.class, () -> statement.execute("SELECT * FROM ps_producer_operations"));
      }
      runProbe(path, "locked", 0);
    } finally { journal.close(); }
    journal.close();
    assertThrows(IOException.class, () -> journal.next(0));
    assertThrows(IOException.class, () -> journal.begin(DECLARE, new byte[]{1}, new byte[]{1}, 8));
    try (var reopened = open(path)) { assertEquals(1, reopened.usage().operations()); }
  }

  @Test void symlinkAndHardLinkLockFilesAreRejected() throws Exception {
    for (boolean symlink : List.of(false, true)) {
      Path path = directory.resolve("alias-" + symlink + ".db");
      Path target = directory.resolve("target-" + symlink); Files.createFile(target);
      Path lock = path.resolveSibling(path.getFileName() + ".producerlock");
      if (symlink) Files.createSymbolicLink(lock, target); else Files.createLink(lock, target);
      assertThrows(IOException.class, () -> open(path)); assertEquals(0, Files.size(target));
    }
  }

  @Test void reservedObservationUpdatesSurviveDatabaseExhaustion() throws Exception {
    Path path = directory.resolve("full.db");
    var files = new SealedSessionStore.FileLimits(65536, 65536, 65536, 65536);
    var limits = new SealedProducerJournal.Limits(100, 1L << 20);
    long count;
    try (var journal = SealedProducerJournal.open(path, PEER, limits, files)) {
      boolean full = false;
      for (int i = 1; i <= 100; i++) {
        try { journal.begin(SealedProducerJournal.Kind.INPUT, new byte[]{(byte) i}, new byte[2048], 4096); }
        catch (ProtocolException failure) { assertEquals(Wire.ERROR_LIMIT_EXCEEDED, failure.errorCode()); full = true; break; }
      }
      assertTrue(full); count = journal.usage().operations(); assertTrue(count > 1 && count < 100);
      var usage = journal.usage(); byte[] observation = new byte[4096]; Arrays.fill(observation, (byte) 7);
      for (long id = 1; id <= count; id++) assertTrue(journal.observe(id, 0, observation, true).resolved());
      assertEquals(usage, journal.usage());
      assertTrue(journal.fileUsage().databaseBytes() <= 65536);
      assertTrue(journal.fileUsage().journalBytes() <= 65536);
      assertEquals(0, journal.fileUsage().walBytes());
    }
    try (var journal = SealedProducerJournal.open(path, PEER, limits, files)) {
      assertEquals(count, journal.usage().operations());
      for (long id = 0; id < count; id++) assertTrue(journal.next(id).resolved());
    }
  }

  @Test void abruptProcessExitDoesNotInventOrLoseCommittedEvidence() throws Exception {
    for (String phase : List.of("intent", "processing", "resolved", "uncommitted")) {
      Path path = directory.resolve("crash-" + phase + ".db");
      runProbe(path, phase, 73);
      try (var journal = open(path)) {
        var entry = journal.next(0); assertNotNull(entry); assertEquals(1, entry.id());
        assertEquals(phase.equals("resolved"), entry.resolved());
        assertEquals(phase.equals("intent") ? 0 : phase.equals("resolved") ? 2 : 1, entry.revision());
        assertArrayEquals(phase.equals("intent") ? new byte[0] : phase.equals("resolved") ? new byte[]{3} : new byte[]{2}, entry.observation());
        assertEquals(1, journal.usage().operations());
      }
    }
  }

  private void runProbe(Path path, String mode, int expected) throws Exception {
    Path log = directory.resolve("probe-" + mode + ".log");
    Process process = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
        "--enable-native-access=ALL-UNNAMED", "-cp", System.getProperty("java.class.path"),
        Probe.class.getName(), path.toString(), mode).redirectErrorStream(true).redirectOutput(log.toFile()).start();
    try {
      assertTrue(process.waitFor(20, TimeUnit.SECONDS), () -> "producer probe timed out: " + log);
      assertEquals(expected, process.exitValue(), () -> {
        try { return Files.readString(log); } catch (IOException failure) { return failure.toString(); }
      });
    } finally { if (process.isAlive()) process.destroyForcibly().waitFor(5, TimeUnit.SECONDS); }
  }

  public static final class Probe {
    private Probe() {}
    public static void main(String[] args) throws Exception {
      Path path = Path.of(args[0]); String mode = args[1];
      if (mode.equals("locked")) {
        try (var journal = SealedProducerJournal.open(path, PEER, LIMITS, FILES)) {
          throw new AssertionError("another process acquired live journal: " + journal.usage());
        } catch (IOException expected) {
          if (!expected.getMessage().contains("already has a writer")) throw expected;
          return;
        }
      }
      try (var journal = SealedProducerJournal.open(path, PEER, LIMITS, FILES)) {
        journal.begin(DECLARE, new byte[]{1}, new byte[]{1}, 8);
        if (!mode.equals("intent")) journal.observe(1, 0, new byte[]{2}, false);
        if (mode.equals("resolved")) journal.observe(1, 1, new byte[]{3}, true);
        if (mode.equals("uncommitted")) {
          var field = SealedProducerJournal.class.getDeclaredField("connection"); field.setAccessible(true);
          Connection connection = (Connection) field.get(journal);
          try (var statement = connection.createStatement()) {
            statement.execute("PRAGMA cache_size=1"); statement.execute("BEGIN EXCLUSIVE");
            SealedSqliteImages.replace(connection, "ps_producer_operations", "image", 1, new byte[64]);
            // Force dirty-page spill before abrupt process termination, leaving a hot journal.
            statement.execute("INSERT INTO ps_producer_operations VALUES(2,0,x'02',zeroblob(65536),zeroblob(64))");
          }
        }
        Runtime.getRuntime().halt(73);
      }
    }
  }

  private static String scalar(Connection connection, String sql) throws SQLException {
    try (var statement = connection.createStatement(); var rows = statement.executeQuery(sql)) {
      assertTrue(rows.next()); return rows.getString(1);
    }
  }

  private static void assertEntity(org.junit.jupiter.api.function.Executable operation) {
    assertEquals(Wire.ERROR_ENTITY_INVALID, assertThrows(ProtocolException.class, operation).errorCode());
  }

  private static void assertLimit(org.junit.jupiter.api.function.Executable operation) {
    assertEquals(Wire.ERROR_LIMIT_EXCEEDED, assertThrows(ProtocolException.class, operation).errorCode());
  }
}
