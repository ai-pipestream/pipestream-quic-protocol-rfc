package ai.pipestream.quic;

import static org.junit.jupiter.api.Assertions.*;

import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

final class SealedCompletionReservationsTest {
  private static final UUID PRODUCER = new UUID(1, 2), WORKER = new UUID(3, 4);
  private static final String SESSION = "completion-cost";
  private static final SealedWork.EntityKey ROOT = new SealedWork.EntityKey(0, 1), CHILD = new SealedWork.EntityKey(7, 1);
  @TempDir Path directory;

  @Test void indexCapacityAndStageAccountingAreExplicit() {
    var limits = new SealedSessionStore.FileLimits(1L << 30, 1L << 30, 1L << 20, 65536);
    for (int page : List.of(512, 4096, 65536)) {
      var model = new SealedCompletionReservations.Model(page);
      assertEquals(32L + (4062 + 4096) * (page + 24L), model.usableWal(limits));
      assertEquals(model.acquisition() + model.publication(0) + model.conversion(65536)
          + model.acquisition() + model.publication(1), model.admission(65536));
      assertTrue(model.publication(0) > model.publication(1));
      assertTrue(model.conversion(65536) > model.conversion(128));
      assertThrows(IllegalArgumentException.class, () -> model.stage(0));
    }
    assertThrows(IllegalArgumentException.class, () -> new SealedCompletionReservations.Model(513));
  }

  @Test void observationBatchPreservesOrderBoundsOwnershipAndMissingJobRefusal() throws Exception {
    var sessions = SealedSessionStore.open(directory.resolve("observation.db"));
    var jobs = new SealedJobs(sessions);
    List<Long> ids = List.of(1L, 2L, 3L);
    sessions.declare(new SealedWork.Declaration(SESSION, PRODUCER, 0, null, BigInteger.ZERO, ids,
        SealedWork.SEAL, SealedWork.sealDigest(SESSION, PRODUCER, 0, null, ids)), 7, 1024);
    var second = new SealedWork.EntityKey(0, 2);
    try (var payloads = SealedPayloadStore.open(directory.resolve("payloads"), SealedPayloadStore.Limits.defaults())) {
      jobs.admit(input(payloads, ROOT, null, 127)); jobs.admit(input(payloads, second, null, 127));
      var order = List.of(key(second, 0), key(ROOT, 0));
      assertEquals(order, jobs.findAll(order).stream().map(SealedJobs.Job::key).toList());
      assertEquals(List.of(), jobs.findAll(List.of()));
      assertEquals(Wire.ERROR_ENTITY_INVALID, assertThrows(ProtocolException.class,
          () -> jobs.findAll(List.of(key(ROOT, 0), key(ROOT, 0)))).errorCode());
      assertEquals(Wire.ERROR_LIMIT_EXCEEDED, assertThrows(ProtocolException.class,
          () -> jobs.findAll(java.util.Collections.nCopies(129, key(ROOT, 0)))).errorCode());
      var foreign = new SealedJobs.Key(new SealedPayloadStore.Identity(SESSION, WORKER, ROOT), 0);
      assertEquals(Wire.ERROR_ENTITY_INVALID, assertThrows(ProtocolException.class,
          () -> jobs.findAll(List.of(key(second, 0), foreign))).errorCode());
      for (var absent : List.of(key(ROOT, 1), key(new SealedWork.EntityKey(0, 3), 0))) {
        assertEquals(Wire.ERROR_INTEGRITY, assertThrows(ProtocolException.class,
            () -> jobs.findAll(List.of(key(second, 0), absent))).errorCode());
      }
      assertEquals(order, jobs.findAll(order).stream().map(SealedJobs.Job::key).toList());
    }
  }

  @Test void observationsRemainReadOnlyAndDoNotCompeteForAnExecutionWriterLease() throws Exception {
    Path path = directory.resolve("read-only.db");
    var sessions = SealedSessionStore.open(path); var jobs = new SealedJobs(sessions);
    declare(sessions, 0, null);
    try (var payloads = SealedPayloadStore.open(directory.resolve("payloads"), SealedPayloadStore.Limits.defaults())) {
      jobs.admit(input(payloads, ROOT, null, 127));
      try (var writer = SealedSqliteFiles.open(path, null).connect();
          var pool = java.util.concurrent.Executors.newSingleThreadExecutor()) {
        execute(writer, "BEGIN IMMEDIATE");
        try {
          var observed = pool.submit(() -> {
            assertEquals(List.of(key(ROOT, 0)), jobs.ready(0, 128));
            assertEquals(SealedJobs.QUEUED, jobs.find(key(ROOT, 0)).orElseThrow().state());
            assertEquals(1, jobs.findAll(List.of(key(ROOT, 0))).size());
            assertEquals(1, sessions.jobUsage().processingJobs());
            assertEquals(List.of(1L), sessions.declared(SESSION, PRODUCER, 0));
            assertFalse(sessions.checkpointReady(SESSION, PRODUCER, 0, 1));
            return true;
          });
          assertTrue(observed.get(2, java.util.concurrent.TimeUnit.SECONDS));
        } finally { execute(writer, "ROLLBACK"); }
      }
      SQLException refused = assertThrows(SQLException.class, () -> sessions.readTransaction(connection -> {
        execute(connection, "UPDATE ps_java_meta SET version=version"); return null;
      }));
      assertEquals(8, refused.getErrorCode() & 255, "SQLite query-only snapshot must refuse SQL writes");
      assertThrows(SQLException.class, () -> sessions.readTransaction(connection -> {
        SealedSqliteImages.walCeiling(connection, 0); return null;
      }));
      assertEquals(1, sessions.jobUsage().processingJobs());
    }
  }

  @Test void admissionRefusesBeforeChangingEntityWhenFutureWritesCannotFit() throws Exception {
    var limits = new SealedSessionStore.FileLimits(8L << 20, 65536, 1L << 20, 65536);
    var sessions = SealedSessionStore.open(directory.resolve("small.db"), limits);
    declare(sessions, 0, null);
    try (var payloads = SealedPayloadStore.open(directory.resolve("payloads"), SealedPayloadStore.Limits.defaults())) {
      var input = input(payloads, ROOT, null, 127);
      assertEquals(Wire.ERROR_LIMIT_EXCEEDED,
          assertThrows(ProtocolException.class, () -> new SealedJobs(sessions).admit(input)).errorCode());
      assertEquals(0, sessions.jobUsage().processingJobs());
      assertEquals(0, sessions.transaction(c -> SealedSessionStore.status(c, SESSION, PRODUCER, ROOT)).intValue());
      assertFalse(sessions.checkpointReady(SESSION, PRODUCER, 0, 1));
      assertTrue(payloads.find(input.identity()).isPresent(), "refused admission must not delete retained input");
    }
  }

  @Test void wholeExecutionTransactionsFitTheirOwnStageBudgetWithSpilling() throws Exception {
    int scenarios = 0, stages = 0;
    for (int page : List.of(512, 4096, 65536)) {
      for (int cache : List.of(2, 2000)) {
        for (int descriptor : List.of(127, 4096, 65000)) {
          for (int outcome : List.of(0, 1, 2)) {
            Path root = directory.resolve("case-" + scenarios++);
            var files = SealedSqliteFiles.open(root.resolve("sessions.db"), null);
            try (var connection = files.connect()) {
              execute(connection, "PRAGMA page_size=" + page);
              // Persist a SQLite cache fixture for the public store's separately
              // opened connections. This is not a production test hook.
              execute(connection, "CREATE TABLE geometry(value INTEGER)");
              execute(connection, "DROP TABLE geometry");
              execute(connection, "PRAGMA default_cache_size=" + cache);
            }
            var sessions = SealedSessionStore.open(files.path());
            var jobs = new SealedJobs(sessions);
            var model = new SealedCompletionReservations.Model(page);
            assertEquals(cache, sessions.transaction(c -> scalar(c, "PRAGMA cache_size")).longValue());
            declare(sessions, 0, null);
            try (var payloads = SealedPayloadStore.open(root.resolve("payloads"), SealedPayloadStore.Limits.defaults())) {
              var parent = input(payloads, ROOT, null, descriptor);
              jobs.admit(parent);
              long capacity = sessions.transaction(c -> scalar(c, "SELECT length(input) FROM ps_java_jobs WHERE kind=1"));
              assertEquals(model.admission((int) capacity), credit(sessions, model));
              var lease = measure(files, sessions, model.acquisition(), () -> jobs.acquire(key(ROOT, 0), WORKER, 1, 100)); stages++;
              assertEquals(model.publication(0) + model.future((int) capacity), credit(sessions, model));
              measure(files, sessions, model.publication(0), () -> { jobs.publish(lease, 2, SealedJobs.Outcome.dehydrate()); return null; }); stages++;
              assertEquals(model.future((int) capacity), credit(sessions, model));
              declare(sessions, 7, ROOT);
              var child = input(payloads, CHILD, ROOT, descriptor);
              jobs.admit(child);
              var childLease = measure(files, sessions, model.acquisition(), () -> jobs.acquire(key(CHILD, 0), WORKER, 1, 100)); stages++;
              measure(files, sessions, model.publication(0), () -> { jobs.publish(childLease, 2, SealedJobs.Outcome.complete(child.digest())); return null; }); stages++;
              assertEquals(model.future((int) capacity), credit(sessions, model));
              measure(files, sessions, model.conversion((int) capacity), () -> jobs.closeScope(SESSION, PRODUCER, 7).orElseThrow()); stages++;
              assertEquals(model.acquisition() + model.publication(1), credit(sessions, model));
              var rehydrate = measure(files, sessions, model.acquisition(), () -> jobs.acquire(key(ROOT, 1), WORKER, 1, 100)); stages++;
              assertEquals(model.publication(1), credit(sessions, model));
              SealedJobs.Outcome result = switch (outcome) {
                case 0 -> SealedJobs.Outcome.complete(parent.digest());
                case 1 -> SealedJobs.Outcome.failed();
                default -> SealedJobs.Outcome.refused(Wire.ERROR_INTEGRITY);
              };
              measure(files, sessions, model.publication(1), () -> { jobs.publish(rehydrate, 2, result); return null; }); stages++;
              assertEquals(0, credit(sessions, model));
              assertEquals(outcome != 2, sessions.checkpointReady(SESSION, PRODUCER, 0, 1));
              assertEquals(0, credit(SealedSessionStore.open(files.path()), model));
            }
          }
        }
      }
    }
    assertEquals(54, scenarios); assertEquals(378, stages);
    System.err.printf("Java completion cost: scenarios=%d whole transactions=%d%n", scenarios, stages);
  }

  @Test void renewalPreservesPublicationCreditAndWrongAdjustmentsRollback() throws Exception {
    var sessions = SealedSessionStore.open(directory.resolve("renewal.db"));
    var model = new SealedCompletionReservations.Model(4096);
    declare(sessions, 0, null);
    var jobs = new SealedJobs(sessions);
    try (var payloads = SealedPayloadStore.open(directory.resolve("payloads"), SealedPayloadStore.Limits.defaults())) {
      var input = input(payloads, ROOT, null, 127); jobs.admit(input);
      var first = jobs.acquire(key(ROOT, 0), WORKER, 10, 100);
      long before = credit(sessions, model);
      var renewed = jobs.acquire(key(ROOT, 0), WORKER, 110, 100);
      assertEquals(before, credit(sessions, model)); assertEquals(first.epoch() + 1, renewed.epoch());
      assertEquals(Wire.ERROR_ENTITY_INVALID, assertThrows(ProtocolException.class,
          () -> jobs.publish(first, 20, SealedJobs.Outcome.complete(input.digest()))).errorCode());
      assertEquals(Wire.ERROR_INTEGRITY, assertThrows(ProtocolException.class,
          () -> sessions.fundedTransaction((connection, funding) -> { funding.adjust(-1); return null; })).errorCode());
      assertEquals(before, credit(sessions, model));
      jobs.publish(renewed, 120, SealedJobs.Outcome.complete(input.digest()));
      assertEquals(0, credit(sessions, model));
    }
  }

  @Test void failedConversionLeavesItsCreditAndCanRetryWithTheSameReaderPinned() throws Exception {
    for (int page : List.of(512, 4096)) {
      Path root = directory.resolve("retry-" + page);
      var limits = new SealedSessionStore.FileLimits(8L << 20, 1L << 20, 1L << 20, 65536);
      var files = geometry(root.resolve("sessions.db"), limits, page);
      var sessions = SealedSessionStore.open(files.path()); var jobs = new SealedJobs(sessions);
      var model = new SealedCompletionReservations.Model(page);
      declare(sessions, 0, null); filler(sessions, 0);
      try (var payloads = SealedPayloadStore.open(root.resolve("payloads"), SealedPayloadStore.Limits.defaults())) {
        var input = input(payloads, ROOT, null, 65000); jobs.admit(input);
        jobs.publish(jobs.acquire(key(ROOT, 0), WORKER, 1, 100), 2, SealedJobs.Outcome.dehydrate());
        declare(sessions, 7, ROOT);
        var child = input(payloads, CHILD, ROOT, 127); jobs.admit(child);
        jobs.publish(jobs.acquire(key(CHILD, 0), WORKER, 1, 100), 2, SealedJobs.Outcome.complete(child.digest()));
        long originalCredit = credit(sessions, model);
        try (var reader = files.connect()) {
          pin(reader);
          saturate(sessions);
          long beforeFailure = files.usage().walBytes();
          SQLException aborted = assertThrows(SQLException.class, () -> sessions.fundedTransaction((connection, funding) -> {
            assertEquals(7, SealedJobs.closeScope(connection, funding, SESSION, PRODUCER, 7, null).orElseThrow().state());
            assertEquals(model.acquisition() + model.publication(1), SealedJobs.completionBytes(connection, model));
            // The real conversion has run, but its transaction has not committed.
            throw new SQLException("injected abort after conversion writes");
          }));
          assertEquals("injected abort after conversion writes", aborted.getMessage());
          long abortedTail = files.usage().walBytes();
          assertTrue(abortedTail > beforeFailure, "tiny cache must exercise a physical uncommitted tail");
          assertEquals(originalCredit, credit(sessions, model));
          assertTrue(jobs.find(key(ROOT, 1)).isEmpty());
          assertEquals(1, sessions.jobUsage().waitingParents());
          assertFalse(sessions.checkpointReady(SESSION, PRODUCER, 0, 1));
          jobs.closeScope(SESSION, PRODUCER, 7).orElseThrow();
          var lease = jobs.acquire(key(ROOT, 1), WORKER, 1, 100);
          jobs.publish(lease, 2, SealedJobs.Outcome.complete(input.digest()));
          assertTrue(sessions.checkpointReady(SESSION, PRODUCER, 0, 1));
          assertEquals(0, credit(sessions, model));
          assertTrue(files.usage().walBytes() <= limits.walBytes());
          long complete = files.usage().walBytes();
          jobs.publish(lease, 2, SealedJobs.Outcome.complete(input.digest()));
          assertEquals(complete, files.usage().walBytes(), "immutable replay must not buy another stage");
        }
      }
    }
  }

  @Test void recursiveConversionAndStrictRetirementSurviveWalOrIndexSaturationAndReopen() throws Exception {
    for (int page : List.of(512, 4096, 65536)) {
      for (boolean strictFailure : List.of(false, true)) {
        Path root = directory.resolve("nested-" + page + "-" + strictFailure);
        var limits = new SealedSessionStore.FileLimits(64L << 20, 8L << 20, 8L << 20, 65536);
        var files = geometry(root.resolve("sessions.db"), limits, page);
        var sessions = SealedSessionStore.open(files.path()); var jobs = new SealedJobs(sessions);
        var model = new SealedCompletionReservations.Model(page);
        declare(sessions, 0, null); filler(sessions, 0);
        try (var payloads = SealedPayloadStore.open(root.resolve("payloads"), SealedPayloadStore.Limits.defaults())) {
          var parent = input(payloads, ROOT, null, 65000); jobs.admit(parent);
          jobs.publish(jobs.acquire(key(ROOT, 0), WORKER, 1, 100), 2, SealedJobs.Outcome.dehydrate());
          declare(sessions, 7, ROOT);
          var child = input(payloads, CHILD, ROOT, 127); jobs.admit(child);
          jobs.publish(jobs.acquire(key(CHILD, 0), WORKER, 1, 100), 2,
              strictFailure ? SealedJobs.Outcome.failed() : SealedJobs.Outcome.complete(child.digest()));
          long before = credit(sessions, model);
          try (var reader = files.connect()) {
            pin(reader); saturate(sessions);
            var reopened = SealedSessionStore.open(files.path()); var continued = new SealedJobs(reopened);
            assertEquals(before, credit(reopened, model));
            assertEquals(strictFailure ? 4 : 7, continued.closeScope(SESSION, PRODUCER, 7).orElseThrow().state());
            if (!strictFailure) {
              var lease = continued.acquire(key(ROOT, 1), WORKER, 1, 100);
              continued.publish(lease, 2, SealedJobs.Outcome.complete(parent.digest()));
            }
            assertEquals(0, credit(reopened, model));
            assertTrue(reopened.checkpointReady(SESSION, PRODUCER, 0, 1));
            assertTrue(files.usage().walBytes() <= model.usableWal(limits));
            assertTrue(files.usage().sharedMemoryBytes() <= 65536);
            if (page == 512) assertTrue(model.usableWal(limits) < limits.walBytes(), "this case must fund the smaller WAL-index cap");
            long complete = files.usage().walBytes();
            continued.closeScope(SESSION, PRODUCER, 7).orElseThrow();
            assertEquals(complete, files.usage().walBytes());
          }
        }
      }
    }
  }

  @Test void priorFilePolicyIsRefusedEvenWithACorrectChecksum() throws Exception {
    Path path = directory.resolve("previous.db");
    SealedSessionStore.open(path);
    Path policy = path.resolveSibling(path.getFileName() + ".psjlimits");
    byte[] bytes = Files.readAllBytes(policy), database = Files.readAllBytes(path);
    bytes[7] = '1';
    var hash = java.security.MessageDigest.getInstance("SHA-256"); hash.update(bytes, 0, 40);
    System.arraycopy(hash.digest(), 0, bytes, 40, 32);
    Files.write(policy, bytes);
    assertThrows(java.io.IOException.class, () -> SealedSessionStore.open(path));
    assertArrayEquals(bytes, Files.readAllBytes(policy));
    assertArrayEquals(database, Files.readAllBytes(path));
  }

  private static SealedSqliteFiles geometry(Path path, SealedSessionStore.FileLimits limits, int page) throws Exception {
    var files = SealedSqliteFiles.open(path, limits);
    try (var connection = files.connect()) {
      execute(connection, "PRAGMA page_size=" + page);
      execute(connection, "CREATE TABLE geometry(value INTEGER)"); execute(connection, "DROP TABLE geometry");
      execute(connection, "PRAGMA default_cache_size=2");
    }
    return files;
  }

  private static void pin(Connection reader) throws SQLException {
    try (var statement = reader.createStatement(); var rows = statement.executeQuery("PRAGMA wal_checkpoint(TRUNCATE)")) {
      assertTrue(rows.next()); assertEquals(0, rows.getInt(1));
    }
    execute(reader, "BEGIN");
    assertTrue(scalar(reader, "SELECT count(*) FROM ps_java_jobs") > 0);
  }

  private static void filler(SealedSessionStore sessions, int sequence) throws Exception {
    sessions.declare(new SealedWork.Declaration("filler", PRODUCER, 0, null, BigInteger.valueOf(sequence),
        List.of(sequence + 1L), 0, null), 7, 16384);
  }

  private static void saturate(SealedSessionStore sessions) throws Exception {
    for (int sequence = 1; sequence < 16000; sequence++) {
      try { filler(sessions, sequence); }
      catch (ProtocolException full) {
        assertEquals(Wire.ERROR_LIMIT_EXCEEDED, full.errorCode());
        assertInstanceOf(SQLException.class, full.getCause(), "exhaust physical writes, not a logical declaration limit");
        assertTrue(SealedSqliteFiles.isFull((SQLException) full.getCause()));
        assertEquals(sequence, sessions.declared("filler", PRODUCER, 0).size());
        return;
      }
    }
    fail("public declarations did not reach their protected WAL ceiling");
  }

  private static long credit(SealedSessionStore sessions, SealedCompletionReservations.Model model) throws Exception {
    return sessions.transaction(c -> SealedJobs.completionBytes(c, model));
  }

  private static <T> T measure(SealedSqliteFiles files, SealedSessionStore sessions, long bound, Action<T> action) throws Exception {
    try (var reader = files.connect(); var statement = reader.createStatement()) {
      try (var checkpoint = statement.executeQuery("PRAGMA wal_checkpoint(TRUNCATE)")) {
        assertTrue(checkpoint.next()); assertEquals(0, checkpoint.getInt(1));
      }
      assertEquals(0, files.usage().walBytes());
      long pages = scalar(reader, "PRAGMA page_count");
      statement.execute("BEGIN");
      assertTrue(scalar(reader, "SELECT count(*) FROM ps_java_jobs") > 0);
      T value = action.run();
      long written = files.usage().walBytes();
      assertTrue(written > 0 && written <= bound, () -> "whole transaction WAL=" + written + " budget=" + bound);
      assertEquals(pages, sessions.transaction(c -> scalar(c, "PRAGMA page_count")).longValue(), "completion must not allocate main pages");
      assertTrue(files.usage().sharedMemoryBytes() <= sessions.fileLimits().sharedMemoryBytes());
      return value;
    }
  }

  private static void declare(SealedSessionStore store, long scope, SealedWork.EntityKey parent) throws Exception {
    store.declare(new SealedWork.Declaration(SESSION, PRODUCER, scope, parent, BigInteger.ZERO, List.of(1L),
        SealedWork.SEAL, SealedWork.sealDigest(SESSION, PRODUCER, scope, parent, List.of(1L))), 7, 1024);
  }
  private static SealedPayloadStore.Stored input(SealedPayloadStore payloads, SealedWork.EntityKey key,
      SealedWork.EntityKey parent, int metadata) throws Exception {
    var identity = new SealedPayloadStore.Identity(SESSION, PRODUCER, key);
    var header = new SealedTransport.Header(key, parent, 0, null, BigInteger.ONE, null, Map.of("data", "x".repeat(metadata)), null);
    try (var receiver = payloads.begin(identity, header)) {
      receiver.write(new byte[]{42}, 0, 1);
      try (var received = receiver.finish()) { return payloads.install(List.of(received)); }
    }
  }
  private static SealedJobs.Key key(SealedWork.EntityKey entity, int kind) {
    return new SealedJobs.Key(new SealedPayloadStore.Identity(SESSION, PRODUCER, entity), kind);
  }
  private static void execute(Connection connection, String sql) throws SQLException {
    try (var statement = connection.createStatement()) { statement.execute(sql); }
  }
  private static long scalar(Connection connection, String sql) throws SQLException {
    try (var statement = connection.createStatement(); var rows = statement.executeQuery(sql)) {
      assertTrue(rows.next()); return rows.getLong(1);
    }
  }
  @FunctionalInterface private interface Action<T> { T run() throws Exception; }
}
