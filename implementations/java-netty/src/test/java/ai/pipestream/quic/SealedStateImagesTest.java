package ai.pipestream.quic;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

final class SealedStateImagesTest {
  private static final String SESSION = "state-images";
  private static final UUID PRODUCER = new UUID(1, 2);
  private static final byte[] OWNER = SealedWork.producerBytes(PRODUCER);
  private static final SealedWork.EntityKey ROOT = new SealedWork.EntityKey(0, 1);
  @TempDir Path directory;

  @Test void entityImagesHaveExactCapacityExplicitAbsenceAndIdentityBoundDigests() throws Exception {
    var empty = new SealedStateImages.Entity(null, false, null, null);
    byte[] unadmitted = SealedStateImages.entity(SESSION, OWNER, ROOT, empty);
    assertEquals(112, unadmitted.length);
    assertArrayEquals(new byte[72], Arrays.copyOfRange(unadmitted, 8, 80));
    assertArrayEquals(independentHash("pipestream-java-entity-image-v1", new byte[]{0,0,0,0,0,0,0,1}, unadmitted), Arrays.copyOfRange(unadmitted, 80, 112));
    assertNull(SealedStateImages.entity(SESSION, OWNER, ROOT, unadmitted).state());
    for (int state : List.of(2, 3, 4, 6, 7)) {
      for (boolean managed : List.of(false, true)) {
        byte[] payload = new byte[32], output = state == 3 ? new byte[32] : null;
        Arrays.fill(payload, (byte) 0x12);
        if (output != null) Arrays.fill(output, (byte) 0x34);
        var input = new SealedStateImages.Entity(state, managed, payload, output);
        byte[] encoded = SealedStateImages.entity(SESSION, OWNER, ROOT, input);
        assertEquals(112, encoded.length);
        var decoded = SealedStateImages.entity(SESSION, OWNER, ROOT, encoded);
        assertEquals(state, decoded.state()); assertEquals(managed, decoded.managed());
        assertArrayEquals(payload, decoded.payloadDigest()); assertArrayEquals(output, decoded.outputDigest());
        assertArrayEquals(encoded, SealedStateImages.entity(SESSION, OWNER, ROOT, decoded));
        payload[0] ^= 1; assertNotEquals(payload[0], input.payloadDigest()[0]);
        byte[] returned = decoded.payloadDigest(); returned[0] ^= 1;
        assertNotEquals(returned[0], decoded.payloadDigest()[0]);
      }
    }
    for (int offset : List.of(0, 8, 16, 48, 80, 111)) {
      byte[] corrupt = unadmitted.clone(); corrupt[offset] ^= 1;
      assertEquals(Wire.ERROR_INTEGRITY, assertThrows(ProtocolException.class,
          () -> SealedStateImages.entity(SESSION, OWNER, ROOT, corrupt)).errorCode());
    }
    assertThrows(ProtocolException.class, () -> SealedStateImages.entity("other", OWNER, ROOT, unadmitted));
    assertThrows(ProtocolException.class, () -> SealedStateImages.entity(SESSION, SealedWork.producerBytes(new UUID(2, 1)), ROOT, unadmitted));
    assertThrows(ProtocolException.class, () -> SealedStateImages.entity(SESSION, OWNER, new SealedWork.EntityKey(1, 1), unadmitted));
    assertThrows(ProtocolException.class, () -> SealedStateImages.entity(SESSION, OWNER, new SealedWork.EntityKey(0, 2), unadmitted));
  }

  @Test void invalidLifecycleAndNoncanonicalUnusedBytesAreRejectedEvenWithMatchingChecksums() throws Exception {
    for (var invalid : List.of(new SealedStateImages.Entity(null, true, null, null),
        new SealedStateImages.Entity(null, false, new byte[32], null),
        new SealedStateImages.Entity(2, false, null, null),
        new SealedStateImages.Entity(2, false, new byte[31], null),
        new SealedStateImages.Entity(3, false, new byte[32], null),
        new SealedStateImages.Entity(3, false, new byte[32], new byte[31]),
        new SealedStateImages.Entity(4, false, new byte[32], new byte[32]),
        new SealedStateImages.Entity(5, false, new byte[32], null))) {
      assertThrows(ProtocolException.class, () -> SealedStateImages.entity(SESSION, OWNER, ROOT, invalid));
    }
    byte[] original = SealedStateImages.entity(SESSION, OWNER, ROOT, new SealedStateImages.Entity(null, false, null, null));
    for (int offset : List.of(11, 15, 16, 48)) {
      byte[] invalid = original.clone(); invalid[offset] = 5;
      byte[] hash = independentHash("pipestream-java-entity-image-v1", new byte[]{0,0,0,0,0,0,0,1}, invalid);
      System.arraycopy(hash, 0, invalid, 80, 32);
      assertThrows(ProtocolException.class, () -> SealedStateImages.entity(SESSION, OWNER, ROOT, invalid));
    }
  }

  @Test void closureImagesDistinguishReservedCapacityFromActualClosedWork() throws Exception {
    byte[] open = SealedStateImages.closure(SESSION, OWNER, 7, ROOT, null);
    assertEquals(128, open.length);
    assertNull(SealedStateImages.readClosure(SESSION, OWNER, 7, ROOT, open));
    byte[] frame = SealedScope.encode(SealedScope.summarize(7, List.of(new SealedScope.Terminal(10, 3))));
    byte[] closed = SealedStateImages.closure(SESSION, OWNER, 7, ROOT, frame);
    assertEquals(128, closed.length);
    assertArrayEquals(frame, SealedStateImages.readClosure(SESSION, OWNER, 7, ROOT, closed));
    assertThrows(ProtocolException.class, () -> SealedStateImages.readClosure(SESSION, OWNER, 8, ROOT, closed));
    assertThrows(ProtocolException.class, () -> SealedStateImages.readClosure(SESSION, OWNER, 7, new SealedWork.EntityKey(0, 2), closed));
    assertThrows(ProtocolException.class, () -> SealedStateImages.closure(SESSION, OWNER, 0, null, frame));
    assertThrows(ProtocolException.class, () -> SealedStateImages.closure(SESSION, OWNER, 8, ROOT, frame));
    assertThrows(ProtocolException.class, () -> SealedStateImages.closure(SESSION, OWNER, 7, null, frame));
    assertNull(SealedStateImages.readClosure(SESSION, OWNER, 0, null, SealedStateImages.closure(SESSION, OWNER, 0, null, null)));
    byte[] key = ByteBuffer.allocate(13).putInt(7).put((byte) 1).putInt(0).putInt(1).array();
    for (int offset : List.of(8, 9, 86, 95)) {
      byte[] invalid = open.clone(); invalid[offset] = 5;
      System.arraycopy(independentHash("pipestream-java-closure-image-v1", key, invalid), 0, invalid, 96, 32);
      assertThrows(ProtocolException.class, () -> SealedStateImages.readClosure(SESSION, OWNER, 7, ROOT, invalid));
    }
  }

  @Test void recursiveEntityAndClosureTransitionsFitTheAlreadyAllocatedMainPages() throws Exception {
    for (int page : List.of(512, 4096, 65536)) {
      Path path = directory.resolve("pages-" + page + ".db");
      var files = SealedSqliteFiles.open(path, null);
      // Initialize an empty, policy-guarded database at the actual test geometry.
      try (var connection = files.connect()) {
        execute(connection, "PRAGMA page_size=" + page);
        execute(connection, "CREATE TABLE geometry(value INTEGER)"); execute(connection, "DROP TABLE geometry");
      }
      var sessions = SealedSessionStore.open(path);
      var root = new SealedWork.Declaration(SESSION, PRODUCER, 0, null, java.math.BigInteger.ZERO,
          List.of(1L), SealedWork.SEAL, SealedWork.sealDigest(SESSION, PRODUCER, 0, null, List.of(1L)));
      sessions.declare(root, 7, 100);
      atPageCap(sessions, connection -> {
        SealedSessionStore.admit(connection, SESSION, PRODUCER, ROOT, null, new byte[32]);
        SealedSessionStore.processed(connection, SESSION, PRODUCER, ROOT, 6, null); return null;
      });
      sessions.declare(new SealedWork.Declaration(SESSION, PRODUCER, 7, ROOT, java.math.BigInteger.ZERO,
          List.of(10L), SealedWork.SEAL, SealedWork.sealDigest(SESSION, PRODUCER, 7, ROOT, List.of(10L))), 7, 100);
      try (var connection = files.connect()) {
        for (String table : List.of("ps_java_entities", "ps_java_scopes")) {
          for (String operation : List.of("INSERT", "UPDATE", "DELETE")) {
            execute(connection, "CREATE TRIGGER no_" + table + operation + " BEFORE " + operation + " ON " + table
                + " BEGIN SELECT RAISE(ABORT,'unexpected SQL row mutation'); END");
          }
        }
        assertEquals(page, scalar(connection, "PRAGMA page_size"));
        assertEquals(224, scalar(connection, "SELECT sum(length(image)) FROM ps_java_entities"));
        assertEquals(256, scalar(connection, "SELECT sum(length(closure_image)) FROM ps_java_scopes"));
      }
      atPageCap(sessions, connection -> {
        var child = new SealedWork.EntityKey(7, 10);
        SealedSessionStore.admit(connection, SESSION, PRODUCER, child, ROOT, new byte[32]);
        SealedSessionStore.processed(connection, SESSION, PRODUCER, child, 3, new byte[32]); return null;
      });
      atPageCap(sessions, connection -> {
        assertTrue(SealedSessionStore.closeScope(connection, SESSION, PRODUCER, 7).isPresent());
        assertEquals(SealedSessionStore.ChildResolution.REHYDRATING, SealedSessionStore.resolveChildren(connection, SESSION, PRODUCER, ROOT));
        return null;
      });
      atPageCap(sessions, connection -> {
        SealedSessionStore.rehydrated(connection, SESSION, PRODUCER, ROOT, true, new byte[32]); return null;
      });
      assertTrue(SealedSessionStore.open(path).checkpointReady(SESSION, PRODUCER, 0, 1));
      try (var connection = files.connect()) {
        assertEquals(3, scalar(connection, "SELECT sum(rowid) FROM ps_java_entities"));
        assertEquals(3, scalar(connection, "SELECT sum(rowid) FROM ps_java_scopes"));
      }
    }
  }

  private static byte[] independentHash(String domain, byte[] key, byte[] image) throws Exception {
    var hash = java.security.MessageDigest.getInstance("SHA-256");
    hash.update(domain.getBytes(StandardCharsets.US_ASCII));
    hash.update(new byte[]{0, 12}); hash.update(SESSION.getBytes(StandardCharsets.US_ASCII));
    hash.update(OWNER); hash.update(key); hash.update(image, 0, image.length - 32);
    return hash.digest();
  }

  private static void atPageCap(SealedSessionStore store, SealedSessionStore.Operation<Void> operation) throws Exception {
    store.transaction(connection -> {
      long pages = scalar(connection, "PRAGMA page_count");
      assertEquals(pages, scalar(connection, "PRAGMA max_page_count=" + pages));
      operation.apply(connection);
      assertEquals(pages, scalar(connection, "PRAGMA page_count"));
      return null;
    });
  }
  private static long scalar(Connection connection, String sql) throws SQLException {
    try (var statement = connection.createStatement(); var rows = statement.executeQuery(sql)) {
      assertTrue(rows.next()); return rows.getLong(1);
    }
  }
  private static void execute(Connection connection, String sql) throws SQLException {
    try (var statement = connection.createStatement()) { statement.execute(sql); }
  }
}
