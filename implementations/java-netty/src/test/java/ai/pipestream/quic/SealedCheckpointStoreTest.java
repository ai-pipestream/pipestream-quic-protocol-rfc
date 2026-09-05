package ai.pipestream.quic;

import static org.junit.jupiter.api.Assertions.*;

import java.math.BigInteger;
import java.nio.file.Path;
import java.sql.DriverManager;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

final class SealedCheckpointStoreTest {
  private static final String SESSION = "sealed-1";
  private static final UUID PRODUCER = UUID.fromString("01010101-0101-0101-0101-010101010101");
  private static final SealedWork.EntityKey ROOT = new SealedWork.EntityKey(0, 1);
  @TempDir Path directory;
  private Path database() { return directory.resolve("sessions.sqlite3"); }
  private SealedSessionStore ready() throws Exception {
    var store = SealedSessionStore.open(database());
    store.declare(SealedWorkTest.declaration(0, null, 0, List.of(1L), List.of(1L)), 7, 10);
    finish(store, ROOT, null, 3); return store;
  }
  private static void finish(SealedSessionStore store, SealedWork.EntityKey key, SealedWork.EntityKey parent, int state) throws Exception {
    store.admit(SESSION, PRODUCER, key, parent, new byte[32]); store.processed(SESSION, PRODUCER, key, state, state == 3 ? new byte[32] : null);
  }
  private static SealedTransport.Checkpoint cut(long scope, BigInteger sequence) {
    return new SealedTransport.Checkpoint("checkpoint-" + scope, sequence, 1, scope, 0, SealedCbor.MAX_UINT);
  }
  private void sql(String query) throws Exception {
    try (var connection = DriverManager.getConnection("jdbc:sqlite:" + database()); var statement = connection.createStatement()) { statement.executeUpdate(query); }
  }

  @Test void exactIdentityIncludesOptionalRootPresenceAndUnsignedCountersAcrossReopen() throws Exception {
    var store = ready(); var request = cut(0, SealedCbor.MAX_UINT);
    store.registerCheckpoint(SESSION, PRODUCER, request);
    assertTrue(store.acknowledgeCheckpoint(SESSION, PRODUCER, request));
    var reopened = SealedSessionStore.open(database()); reopened.registerCheckpoint(SESSION, PRODUCER, request);
    assertTrue(reopened.acknowledgeCheckpoint(SESSION, PRODUCER, request));
    var omitted = new SealedTransport.Checkpoint(request.id(), request.sequence(), 1, null, 0, request.timeoutMs());
    assertEquals(5, assertThrows(ProtocolException.class, () -> reopened.registerCheckpoint(SESSION, PRODUCER, omitted)).errorCode());
    assertEquals(5, assertThrows(ProtocolException.class, () -> reopened.registerCheckpoint(SESSION, new UUID(1, 2), request)).errorCode());
  }

  @Test void pendingNestedCheckpointSurvivesReopenAndPreventsRootAck() throws Exception {
    var store = SealedSessionStore.open(database());
    store.declare(SealedWorkTest.declaration(0, null, 0, List.of(1L), List.of(1L)), 7, 10);
    finish(store, ROOT, null, 6);
    store.declare(SealedWorkTest.declaration(7, ROOT, 0, List.of(1L), List.of(1L)), 7, 10);
    finish(store, new SealedWork.EntityKey(7, 1), ROOT, 3);
    store.closeScope(SESSION, PRODUCER, 7).orElseThrow(); store.resolveChildren(SESSION, PRODUCER, ROOT);
    store.rehydrated(SESSION, PRODUCER, ROOT, true, new byte[32]);
    var root = cut(0, BigInteger.ZERO); var child = cut(7, BigInteger.ZERO);
    store.registerCheckpoint(SESSION, PRODUCER, root); store.registerCheckpoint(SESSION, PRODUCER, child);
    var reopened = SealedSessionStore.open(database());
    assertFalse(reopened.acknowledgeCheckpoint(SESSION, PRODUCER, root));
    assertTrue(reopened.acknowledgeCheckpoint(SESSION, PRODUCER, child));
    assertTrue(reopened.acknowledgeCheckpoint(SESSION, PRODUCER, root));
  }

  @Test void changedAckBitAndMissingCheckpointRowsCannotEraseOutstandingObligations() throws Exception {
    var store = ready(); var request = cut(0, BigInteger.ZERO); store.registerCheckpoint(SESSION, PRODUCER, request);
    sql("UPDATE ps_java_checkpoints SET acknowledged=1");
    assertEquals(4, assertThrows(ProtocolException.class, () -> store.acknowledgeCheckpoint(SESSION, PRODUCER, request)).errorCode());
    sql("UPDATE ps_java_checkpoints SET acknowledged=0");
    sql("DELETE FROM ps_java_checkpoints");
    assertEquals(4, assertThrows(ProtocolException.class, () -> store.registerCheckpoint(SESSION, PRODUCER, request)).errorCode());
  }

  @Test void failedHistoryWriteRollsBackRegistrationAndAcknowledgement() throws Exception {
    var store = ready(); var request = cut(0, BigInteger.ZERO);
    sql("CREATE TRIGGER fail_history BEFORE UPDATE ON ps_java_checkpoint_history BEGIN SELECT RAISE(ABORT,'injected history failure'); END");
    assertThrows(java.sql.SQLException.class, () -> store.registerCheckpoint(SESSION, PRODUCER, request));
    sql("DROP TRIGGER fail_history"); store.registerCheckpoint(SESSION, PRODUCER, request);
    sql("CREATE TRIGGER fail_history BEFORE UPDATE ON ps_java_checkpoint_history BEGIN SELECT RAISE(ABORT,'injected history failure'); END");
    assertThrows(java.sql.SQLException.class, () -> store.acknowledgeCheckpoint(SESSION, PRODUCER, request));
    sql("DROP TRIGGER fail_history"); assertTrue(store.acknowledgeCheckpoint(SESSION, PRODUCER, request));
  }

  @Test void retainedCheckpointCapacityRemainsChargedAfterAcknowledgementAndReopen() throws Exception {
    var store = ready();
    for (int index = 0; index < 1024; index++) store.registerCheckpoint(SESSION, PRODUCER, cut(0, BigInteger.valueOf(index)));
    var first = cut(0, BigInteger.ZERO); assertTrue(store.acknowledgeCheckpoint(SESSION, PRODUCER, first));
    var reopened = SealedSessionStore.open(database()); reopened.registerCheckpoint(SESSION, PRODUCER, first);
    assertEquals(6, assertThrows(ProtocolException.class, () -> reopened.registerCheckpoint(SESSION, PRODUCER, cut(0, BigInteger.valueOf(1024)))).errorCode());
  }

  @Test void versionTwoIsRefusedWithoutConversion() throws Exception {
    ready(); sql("UPDATE ps_java_meta SET version=2");
    assertThrows(java.sql.SQLException.class, () -> SealedSessionStore.open(database()));
    try (var connection = DriverManager.getConnection("jdbc:sqlite:" + database()); var statement = connection.createStatement();
        var rows = statement.executeQuery("SELECT version FROM ps_java_meta")) { assertTrue(rows.next()); assertEquals(2, rows.getInt(1)); }
  }
}
