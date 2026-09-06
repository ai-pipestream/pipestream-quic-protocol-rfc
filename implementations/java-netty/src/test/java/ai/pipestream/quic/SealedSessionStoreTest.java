package ai.pipestream.quic;

import static org.junit.jupiter.api.Assertions.*;

import java.math.BigInteger;
import java.nio.file.Path;
import java.nio.file.Files;
import java.sql.DriverManager;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

final class SealedSessionStoreTest {
  private static final UUID PRODUCER = UUID.fromString("01010101-0101-0101-0101-010101010101");
  private static final SealedWork.EntityKey ROOT = new SealedWork.EntityKey(0, 1);
  @TempDir Path directory;

  private Path database() { return directory.resolve("sealed.sqlite3"); }
  private static SealedWork.Declaration root() throws ProtocolException {
    return SealedWorkTest.declaration(0, null, 0, List.of(1L, 2L), List.of(1L, 2L));
  }

  @Test void durableAcknowledgementReplayDoesNotEraseMissingPayloadsOrReuseIds() throws Exception {
    var first = SealedSessionStore.open(database());
    var ack = first.declare(root(), 7, 100);
    first.admit("sealed-1", PRODUCER, ROOT, null, new byte[32]);
    first.processed("sealed-1", PRODUCER, ROOT, Wire.STATUS_COMPLETE, new byte[32]);
    var reopened = SealedSessionStore.open(database());
    assertEquals(ack, reopened.declare(root(), 7, 100));
    assertEquals(List.of(1L, 2L), reopened.declared("sealed-1", PRODUCER, 0));
    assertEquals(Wire.ERROR_ENTITY_INVALID, assertThrows(ProtocolException.class,
        () -> reopened.admit("sealed-1", PRODUCER, ROOT, null, new byte[32])).errorCode());
    assertEquals(Wire.ERROR_ENTITY_INVALID, assertThrows(ProtocolException.class,
        () -> reopened.declare(SealedWorkTest.declaration(0, null, 1, List.of(3L), null), 7, 100)).errorCode());
    assertEquals(1, scalar("SELECT count(*) FROM ps_java_batches"));
    assertEquals(1, scalar("SELECT count(*) FROM ps_java_entities WHERE substr(image,9,4)=zeroblob(4)"));
    assertEquals(1, scalar("SELECT count(*) FROM ps_java_entities WHERE substr(image,9,4)=x'00000003'"));
  }

  @Test void childDeclarationsRequireAdmittedDehydratingParentAndImmutableBinding() throws Exception {
    var store = SealedSessionStore.open(database());
    store.declare(root(), 7, 100);
    var child = SealedWorkTest.declaration(7, ROOT, 0, List.of(10L, 20L), List.of(10L, 20L));
    assertEquals(Wire.ERROR_ENTITY_INVALID, assertThrows(ProtocolException.class, () -> store.declare(child, 7, 100)).errorCode());
    store.admit("sealed-1", PRODUCER, ROOT, null, new byte[32]);
    assertThrows(ProtocolException.class, () -> store.declare(child, 7, 100));
    store.processed("sealed-1", PRODUCER, ROOT, 6, null);
    var ack = store.declare(child, 7, 100);
    assertEquals(ack, SealedSessionStore.open(database()).declare(child, 7, 100));
    assertThrows(ProtocolException.class, () -> store.declare(SealedWorkTest.declaration(8, ROOT, 0, List.of(1L), null), 7, 100));
    assertEquals(List.of(10L, 20L), store.declared("sealed-1", PRODUCER, 7));
    assertThrows(ProtocolException.class, () -> store.admit("sealed-1", PRODUCER, new SealedWork.EntityKey(7, 10), new SealedWork.EntityKey(0, 2), new byte[32]));
    store.admit("sealed-1", PRODUCER, new SealedWork.EntityKey(7, 20), ROOT, new byte[32]);
    store.admit("sealed-1", PRODUCER, new SealedWork.EntityKey(7, 10), ROOT, new byte[32]);
    assertEquals(2, scalar("SELECT count(*) FROM ps_java_scopes"));
  }

  @Test void failedSealAndCapacityRefusalsRollBackTheEntireBatch() throws Exception {
    var store = SealedSessionStore.open(database());
    var start = SealedWorkTest.declaration(0, null, 0, List.of(1L), null);
    store.declare(start, 7, 2);
    var bad = new SealedWork.Declaration("sealed-1", PRODUCER, 0, null, BigInteger.ONE, List.of(2L), SealedWork.SEAL, new byte[32]);
    assertEquals(Wire.ERROR_INTEGRITY, assertThrows(ProtocolException.class, () -> store.declare(bad, 7, 2)).errorCode());
    assertEquals(List.of(1L), store.declared("sealed-1", PRODUCER, 0));
    assertEquals(Wire.ERROR_LIMIT_EXCEEDED, assertThrows(ProtocolException.class,
        () -> store.declare(SealedWorkTest.declaration(0, null, 1, List.of(2L, 3L), null), 7, 2)).errorCode());
    var finalBatch = SealedWorkTest.declaration(0, null, 1, List.of(2L), List.of(1L, 2L));
    assertEquals(finalBatch.acknowledgement(), store.declare(finalBatch, 7, 2));
    assertEquals(start.acknowledgement(), store.declare(start, 7, 2));
    assertEquals(2, scalar("SELECT count(*) FROM ps_java_batches"));
    assertEquals(Wire.ERROR_EXTENSION_UNSUPPORTED, assertThrows(ProtocolException.class,
        () -> store.declare(start, 6, 2)).errorCode());
  }

  @Test void wrongIdentitySequenceAndChangedReplayCannotMutateRetainedState() throws Exception {
    var store = SealedSessionStore.open(database());
    store.declare(root(), 7, 100);
    var wrong = new SealedWork.Declaration("sealed-1", new UUID(1, 2), 0, null, BigInteger.ZERO, root().entityIds(), root().flags(), root().sealDigest());
    assertEquals(Wire.ERROR_ENTITY_INVALID, assertThrows(ProtocolException.class, () -> store.declare(wrong, 7, 100)).errorCode());
    assertThrows(ProtocolException.class, () -> store.declared("sealed-1", new UUID(1, 2), 0));
    var changed = SealedWorkTest.declaration(0, null, 0, List.of(1L), List.of(1L));
    assertThrows(ProtocolException.class, () -> store.declare(changed, 7, 100));
    assertEquals(List.of(1L, 2L), store.declared("sealed-1", PRODUCER, 0));
    assertEquals(1, scalar("SELECT count(*) FROM ps_java_batches"));
  }

  @Test void concurrentHandlesReplayOneCommittedDeclaration() throws Exception {
    var store = SealedSessionStore.open(database());
    var other = SealedSessionStore.open(database());
    var declaration = root();
    try (var executor = Executors.newFixedThreadPool(4)) {
      var futures = java.util.stream.IntStream.range(0, 8).mapToObj(i -> executor.submit(
          () -> (i % 2 == 0 ? store : other).declare(declaration, 7, 100))).toList();
      for (var result : futures) assertEquals(declaration.acknowledgement(), result.get(5, TimeUnit.SECONDS));
    }
    assertEquals(1, scalar("SELECT count(*) FROM ps_java_sessions"));
    assertEquals(1, scalar("SELECT count(*) FROM ps_java_batches"));
    assertEquals(2, scalar("SELECT count(*) FROM ps_java_entities"));
  }

  @Test void unknownSchemaAndChangedChecksummedBatchAreRefusedWithoutConversion() throws Exception {
    var store = SealedSessionStore.open(database());
    store.declare(root(), 7, 100);
    try (var connection = DriverManager.getConnection("jdbc:sqlite:" + database()); var update = connection.createStatement()) {
      update.executeUpdate("UPDATE ps_java_batches SET checksum=zeroblob(32)");
    }
    assertEquals(Wire.ERROR_INTEGRITY, assertThrows(ProtocolException.class, () -> store.declare(root(), 7, 100)).errorCode());
    try (var connection = DriverManager.getConnection("jdbc:sqlite:" + database()); var update = connection.createStatement()) {
      update.executeUpdate("UPDATE ps_java_meta SET version=99");
    }
    assertThrows(java.sql.SQLException.class, () -> SealedSessionStore.open(database()));
    assertEquals(99, scalar("SELECT version FROM ps_java_meta"));
  }

  @Test void abruptProcessExitAfterCommitRetainsTheExactAcknowledgement() throws Exception {
    var command = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
        "--enable-native-access=ALL-UNNAMED",
        "-cp", System.getProperty("java.class.path"), CrashWriter.class.getName(), database().toString())
        .redirectErrorStream(true).start();
    try {
      assertTrue(command.waitFor(10, TimeUnit.SECONDS));
      assertEquals(23, command.exitValue(), new String(command.getInputStream().readAllBytes(), java.nio.charset.StandardCharsets.UTF_8));
    } finally {
      if (command.isAlive()) command.destroyForcibly().waitFor(5, TimeUnit.SECONDS);
    }
    var reopened = SealedSessionStore.open(database());
    assertEquals(root().acknowledgement(), reopened.declare(root(), 7, 100));
    assertEquals(List.of(1L, 2L), reopened.declared("sealed-1", PRODUCER, 0));
  }

  @Test void recursiveClosureWaitsForMissingChildrenSealsAndRehydrationAcrossReopen() throws Exception {
    var store = SealedSessionStore.open(database());
    store.declare(root(), 7, 100);
    finish(store, new SealedWork.EntityKey(0, 2), null, 3);
    finish(store, ROOT, null, 6);
    assertEquals(SealedSessionStore.ChildResolution.PENDING, store.resolveChildren("sealed-1", PRODUCER, ROOT));
    store.declare(SealedWorkTest.declaration(7, ROOT, 0, List.of(10L, 20L), List.of(10L, 20L)), 7, 100);
    var nestedParent = new SealedWork.EntityKey(7, 10);
    finish(store, nestedParent, ROOT, 6);
    finish(store, new SealedWork.EntityKey(7, 20), ROOT, 3);
    store.declare(SealedWorkTest.declaration(9, nestedParent, 0, List.of(1L, 2L, 3L), null), 7, 100);
    finish(store, new SealedWork.EntityKey(9, 3), nestedParent, 3);
    finish(store, new SealedWork.EntityKey(9, 1), nestedParent, 3);
    assertTrue(store.closeScope("sealed-1", PRODUCER, 9).isEmpty());
    store = SealedSessionStore.open(database());
    assertFalse(store.checkpointReady("sealed-1", PRODUCER, 0, 2));
    finish(store, new SealedWork.EntityKey(9, 2), nestedParent, 3);
    assertTrue(store.closeScope("sealed-1", PRODUCER, 9).isEmpty(), "all received children is not a sealed set");
    store.declare(SealedWorkTest.declaration(9, nestedParent, 1, List.of(), List.of(1L, 2L, 3L)), 7, 100);
    assertEquals(SealedSessionStore.ChildResolution.PENDING, store.resolveChildren("sealed-1", PRODUCER, nestedParent));
    var grandchild = store.closeScope("sealed-1", PRODUCER, 9).orElseThrow();
    assertEquals(BigInteger.valueOf(3), grandchild.succeeded());
    assertTrue(store.closeScope("sealed-1", PRODUCER, 7).isEmpty());
    assertEquals(SealedSessionStore.ChildResolution.REHYDRATING, store.resolveChildren("sealed-1", PRODUCER, nestedParent));
    assertTrue(store.closeScope("sealed-1", PRODUCER, 7).isEmpty());
    store = SealedSessionStore.open(database());
    store.rehydrated("sealed-1", PRODUCER, nestedParent, true, new byte[32]);
    var child = store.closeScope("sealed-1", PRODUCER, 7).orElseThrow();
    assertEquals(BigInteger.TWO, child.succeeded());
    assertFalse(store.checkpointReady("sealed-1", PRODUCER, 0, 2));
    assertEquals(SealedSessionStore.ChildResolution.REHYDRATING, store.resolveChildren("sealed-1", PRODUCER, ROOT));
    store.rehydrated("sealed-1", PRODUCER, ROOT, true, new byte[32]);
    assertTrue(store.checkpointReady("sealed-1", PRODUCER, 0, 2));
    var reopened = SealedSessionStore.open(database());
    assertEquals(child, reopened.closeScope("sealed-1", PRODUCER, 7).orElseThrow());
    assertEquals(grandchild, reopened.closeScope("sealed-1", PRODUCER, 9).orElseThrow());
    assertEquals(Wire.ERROR_ENTITY_INVALID, assertThrows(ProtocolException.class,
        () -> reopened.checkpointReady("sealed-1", PRODUCER, 0, 1)).errorCode());
    assertThrows(ProtocolException.class, () -> reopened.resolveChildren("sealed-1", PRODUCER, ROOT));
    assertThrows(ProtocolException.class, () -> reopened.rehydrated("sealed-1", PRODUCER, ROOT, true, new byte[32]));
    assertEquals("PIPESTREAM_SCOPE_INVALID", assertThrows(ProtocolException.class,
        () -> reopened.closeScope("sealed-1", PRODUCER, 0)).errorName());
  }

  @Test void failedChildClosesItsScopeButCannotAuthorizeStrictRehydration() throws Exception {
    var store = SealedSessionStore.open(database());
    store.declare(SealedWorkTest.declaration(0, null, 0, List.of(1L), List.of(1L)), 7, 100);
    finish(store, ROOT, null, 6);
    store.declare(SealedWorkTest.declaration(7, ROOT, 0, List.of(10L, 20L), List.of(10L, 20L)), 7, 100);
    finish(store, new SealedWork.EntityKey(7, 20), ROOT, 4);
    assertTrue(store.closeScope("sealed-1", PRODUCER, 7).isEmpty());
    finish(store, new SealedWork.EntityKey(7, 10), ROOT, 3);
    assertEquals(SealedSessionStore.ChildResolution.PENDING, store.resolveChildren("sealed-1", PRODUCER, ROOT));
    var summary = store.closeScope("sealed-1", PRODUCER, 7).orElseThrow();
    assertEquals(BigInteger.ONE, summary.failed());
    assertEquals(SealedSessionStore.ChildResolution.FAILED, store.resolveChildren("sealed-1", PRODUCER, ROOT));
    assertThrows(ProtocolException.class, () -> store.rehydrated("sealed-1", PRODUCER, ROOT, true, new byte[32]));
    assertTrue(store.checkpointReady("sealed-1", PRODUCER, 0, 1), "resolved does not mean every entity succeeded");
    assertEquals(2, scalar("SELECT count(*) FROM ps_java_entities WHERE substr(image,9,4)=x'00000004'"));
  }

  @Test void declarationAndClosureWriteFailuresRollBackWithoutAcknowledgements() throws Exception {
    var store = SealedSessionStore.open(database());
    sql("CREATE TRIGGER fail_batch BEFORE INSERT ON ps_java_batches BEGIN SELECT RAISE(ABORT,'injected batch failure'); END");
    assertThrows(java.sql.SQLException.class, () -> store.declare(root(), 7, 100));
    assertEquals(0, scalar("SELECT count(*) FROM ps_java_sessions"));
    assertEquals(0, scalar("SELECT count(*) FROM ps_java_entities"));
    sql("DROP TRIGGER fail_batch");
    store.declare(root(), 7, 100);
    finish(store, ROOT, null, 6);
    store.declare(SealedWorkTest.declaration(7, ROOT, 0, List.of(10L), List.of(10L)), 7, 100);
    finish(store, new SealedWork.EntityKey(7, 10), ROOT, 3);
    sql("CREATE INDEX fail_closure ON ps_java_scopes(closure_image)");
    assertThrows(java.sql.SQLException.class, () -> store.closeScope("sealed-1", PRODUCER, 7));
    assertEquals(0, scalar("SELECT count(*) FROM ps_java_scopes WHERE substr(closure_image,9,1)=x'01'"));
    assertEquals(SealedSessionStore.ChildResolution.PENDING, store.resolveChildren("sealed-1", PRODUCER, ROOT));
    sql("DROP INDEX fail_closure");
    assertTrue(store.closeScope("sealed-1", PRODUCER, 7).isPresent());
    sql("UPDATE ps_java_scopes SET closure_image=zeroblob(128) WHERE id=7");
    assertEquals(Wire.ERROR_INTEGRITY, assertThrows(ProtocolException.class,
        () -> store.resolveChildren("sealed-1", PRODUCER, ROOT)).errorCode());
  }

  @Test void missingDeclaredRowsAndChangedScopeMetadataCannotProduceReplayOrCompletion() throws Exception {
    var store = SealedSessionStore.open(database());
    store.declare(root(), 7, 100);
    finish(store, ROOT, null, 3);
    sql("DELETE FROM ps_java_entities WHERE id=2");
    assertEquals(Wire.ERROR_INTEGRITY, assertThrows(ProtocolException.class, () -> store.declare(root(), 7, 100)).errorCode());
    assertEquals(Wire.ERROR_INTEGRITY, assertThrows(ProtocolException.class,
        () -> store.checkpointReady("sealed-1", PRODUCER, 0, 1)).errorCode());
    byte[] missing = SealedStateImages.entity("sealed-1", SealedWork.producerBytes(PRODUCER), new SealedWork.EntityKey(0, 2),
        new SealedStateImages.Entity(null, false, null, null));
    try (var connection = DriverManager.getConnection("jdbc:sqlite:" + database());
        var insert = connection.prepareStatement("INSERT INTO ps_java_entities(session,scope,id,image) VALUES ('sealed-1',0,2,?)")) {
      insert.setBytes(1, missing); assertEquals(1, insert.executeUpdate());
    }
    assertEquals(root().acknowledgement(), store.declare(root(), 7, 100));
    sql("UPDATE ps_java_scopes SET sealed=0,digest=NULL");
    assertEquals(Wire.ERROR_INTEGRITY, assertThrows(ProtocolException.class,
        () -> store.declared("sealed-1", PRODUCER, 0)).errorCode());
  }

  @Test void schemaRefusesInvalidImageGeometryAndForeignFormats() throws Exception {
    var store = SealedSessionStore.open(database());
    store.declare(root(), 7, 100);
    for (String statement : List.of("UPDATE ps_java_entities SET image=NULL",
        "UPDATE ps_java_entities SET image=zeroblob(111)", "UPDATE ps_java_entities SET image=zeroblob(113)",
        "UPDATE ps_java_entities SET image=printf('%0112d',0)",
        "UPDATE ps_java_scopes SET closure_image=NULL", "UPDATE ps_java_scopes SET closure_image=zeroblob(127)",
        "UPDATE ps_java_scopes SET digest=NULL", "UPDATE ps_java_scopes SET next_sequence='12345678'")) {
      assertThrows(java.sql.SQLException.class, () -> sql(statement), statement);
    }
    Path foreign = directory.resolve("foreign.sqlite3");
    try (var connection = DriverManager.getConnection("jdbc:sqlite:" + foreign); var statement = connection.createStatement()) {
      statement.execute("CREATE TABLE unrelated (value INTEGER)");
      assertThrows(java.io.IOException.class, () -> SealedSessionStore.open(foreign));
      assertFalse(Files.exists(foreign.resolveSibling("foreign.sqlite3.psjlimits")));
      try (var rows = statement.executeQuery("PRAGMA journal_mode")) { assertTrue(rows.next()); assertEquals("delete", rows.getString(1)); }
    }
  }

  @Test void retainedDepthLimitRefusesChildAllocationWithoutPartialScope() throws Exception {
    var store = SealedSessionStore.open(database());
    store.declare(root(), 0, 100);
    finish(store, ROOT, null, 6);
    var child = SealedWorkTest.declaration(7, ROOT, 0, List.of(1L), List.of(1L));
    assertEquals("PIPESTREAM_DEPTH_EXCEEDED", assertThrows(ProtocolException.class,
        () -> store.declare(child, 7, 100)).errorName(), "a reconnect cannot raise retained limits");
    assertEquals(1, scalar("SELECT count(*) FROM ps_java_scopes"));
    assertEquals(1, scalar("SELECT count(*) FROM ps_java_batches"));
  }

  @Test void retainedGlobalAndSessionDeclarationBudgetsSurviveReopen() throws Exception {
    var store = SealedSessionStore.open(database());
    for (int session = 0; session < 4; session++) {
      for (int batch = 0; batch < 64; batch++) {
        List<Long> ids = java.util.stream.LongStream.rangeClosed(batch * 256L + 1, (batch + 1) * 256L).boxed().toList();
        var request = new SealedWork.Declaration("budget-" + session, PRODUCER, 0, null,
            BigInteger.valueOf(batch), ids, 0, null);
        assertEquals(request.acknowledgement(), store.declare(request, 7, Wire.MAX_ENTITY_ID));
      }
      var reopened = SealedSessionStore.open(database());
      var overSession = new SealedWork.Declaration("budget-" + session, PRODUCER, 0, null,
          BigInteger.valueOf(64), List.of(16385L), 0, null);
      assertEquals(Wire.ERROR_LIMIT_EXCEEDED, assertThrows(ProtocolException.class,
          () -> reopened.declare(overSession, 7, Wire.MAX_ENTITY_ID)).errorCode());
      assertEquals(16384, reopened.declared("budget-" + session, PRODUCER, 0).size());
    }
    var reopened = SealedSessionStore.open(database());
    assertEquals(Wire.ERROR_LIMIT_EXCEEDED, assertThrows(ProtocolException.class,
        () -> reopened.declare(root(), 7, 100)).errorCode());
    assertEquals(4, scalar("SELECT count(*) FROM ps_java_sessions"), "new session rolls back when global budget is full");
    assertEquals(65536, scalar("SELECT count(*) FROM ps_java_entities"));
    var original = new SealedWork.Declaration("budget-0", PRODUCER, 0, null, BigInteger.ZERO,
        java.util.stream.LongStream.rangeClosed(1, 256).boxed().toList(), 0, null);
    assertEquals(original.acknowledgement(), reopened.declare(original, 7, Wire.MAX_ENTITY_ID), "replay needs no additional capacity");
  }

  @Test void malformedRetainedBatchIsLengthGatedBeforeDecoding() throws Exception {
    var store = SealedSessionStore.open(database());
    store.declare(root(), 7, 100);
    try (var connection = DriverManager.getConnection("jdbc:sqlite:" + database()); var statement = connection.createStatement()) {
      statement.execute("PRAGMA ignore_check_constraints=ON");
      statement.execute("UPDATE ps_java_batches SET request=zeroblob(100000)");
    }
    assertEquals(Wire.ERROR_INTEGRITY, assertThrows(ProtocolException.class,
        () -> store.declare(root(), 7, 100)).errorCode());
  }

  @Test void abruptExitAfterChildClosureRetainsSummaryAndParentReadiness() throws Exception {
    var command = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
        "--enable-native-access=ALL-UNNAMED", "-cp", System.getProperty("java.class.path"),
        CrashClosureWriter.class.getName(), database().toString()).redirectErrorStream(true).start();
    try {
      assertTrue(command.waitFor(10, TimeUnit.SECONDS));
      assertEquals(24, command.exitValue(), new String(command.getInputStream().readAllBytes(), java.nio.charset.StandardCharsets.UTF_8));
    } finally {
      if (command.isAlive()) command.destroyForcibly().waitFor(5, TimeUnit.SECONDS);
    }
    var store = SealedSessionStore.open(database());
    var child = SealedWorkTest.declaration(7, ROOT, 0, List.of(10L), List.of(10L));
    assertEquals(root().acknowledgement(), store.declare(root(), 7, 100));
    assertEquals(child.acknowledgement(), store.declare(child, 7, 100));
    assertEquals(SealedScope.summarize(7, List.of(new SealedScope.Terminal(10, 3))),
        store.closeScope("sealed-1", PRODUCER, 7).orElseThrow());
    assertFalse(store.checkpointReady("sealed-1", PRODUCER, 0, 2));
    assertEquals(SealedSessionStore.ChildResolution.REHYDRATING, store.resolveChildren("sealed-1", PRODUCER, ROOT));
    store.rehydrated("sealed-1", PRODUCER, ROOT, true, new byte[32]);
    assertTrue(store.checkpointReady("sealed-1", PRODUCER, 0, 2));
  }

  public static final class CrashClosureWriter {
    private CrashClosureWriter() {}
    public static void main(String[] args) throws Exception {
      var store = SealedSessionStore.open(Path.of(args[0]));
      store.declare(root(), 7, 100);
      finish(store, ROOT, null, 6);
      finish(store, new SealedWork.EntityKey(0, 2), null, 3);
      store.declare(SealedWorkTest.declaration(7, ROOT, 0, List.of(10L), List.of(10L)), 7, 100);
      finish(store, new SealedWork.EntityKey(7, 10), ROOT, 3);
      store.closeScope("sealed-1", PRODUCER, 7).orElseThrow();
      Runtime.getRuntime().halt(24);
    }
  }

  private static void finish(SealedSessionStore store, SealedWork.EntityKey key,
      SealedWork.EntityKey parent, int state) throws Exception {
    store.admit("sealed-1", PRODUCER, key, parent, new byte[32]);
    store.processed("sealed-1", PRODUCER, key, state, state == 3 ? new byte[32] : null);
  }

  private void sql(String command) throws Exception {
    try (var connection = DriverManager.getConnection("jdbc:sqlite:" + database()); var statement = connection.createStatement()) {
      statement.execute(command);
    }
  }

  public static final class CrashWriter {
    private CrashWriter() {}
    public static void main(String[] args) throws Exception {
      SealedSessionStore.open(Path.of(args[0])).declare(root(), 7, 100);
      Runtime.getRuntime().halt(23);
    }
  }

  private long scalar(String query) throws Exception {
    try (var connection = DriverManager.getConnection("jdbc:sqlite:" + database());
        var statement = connection.createStatement(); var rows = statement.executeQuery(query)) {
      assertTrue(rows.next()); return rows.getLong(1);
    }
  }
}
