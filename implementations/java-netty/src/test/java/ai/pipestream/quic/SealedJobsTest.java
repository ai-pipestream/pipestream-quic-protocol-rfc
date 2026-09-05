package ai.pipestream.quic;

import static org.junit.jupiter.api.Assertions.*;

import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.LongStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

final class SealedJobsTest {
  private static final UUID PRODUCER = new UUID(1, 2), WORKER = new UUID(3, 4);
  private static final String SESSION = "queued-work";
  private static final SealedWork.EntityKey ROOT = new SealedWork.EntityKey(0, 1);
  @TempDir Path directory;

  @Test void admissionAndJobCommitTogetherAndSurviveReopenWithoutManualPublication() throws Exception {
    var sessions = sessions(); declare(sessions, SESSION, 0, null, List.of(1L));
    var jobs = new SealedJobs(sessions);
    try (var payloads = payloads()) {
      var input = input(payloads, SESSION, ROOT, null);
      sql("CREATE TRIGGER fail_job BEFORE INSERT ON ps_java_jobs BEGIN SELECT RAISE(ABORT,'injected job write failure'); END");
      assertThrows(SQLException.class, () -> jobs.admit(input));
      assertEquals(1, scalar("SELECT count(*) FROM ps_java_entities WHERE state IS NULL AND managed=0"));
      assertEquals(0, scalar("SELECT count(*) FROM ps_java_jobs"));
      sql("DROP TRIGGER fail_job");
      jobs.admit(input);
      assertEquals(List.of(key(SESSION, ROOT, SealedJobs.PROCESS)), jobs.ready(0, 8));
      assertThrows(ProtocolException.class, () -> sessions.processed(SESSION, PRODUCER, ROOT, 3, input.digest()));
      var reopened = new SealedJobs(sessions());
      assertEquals(jobs.ready(0, 8), reopened.ready(0, 8));
      assertThrows(ProtocolException.class, () -> reopened.admit(input));
      var otherOwner = new SealedJobs.Key(new SealedPayloadStore.Identity(SESSION, UUID.randomUUID(), ROOT), 0);
      assertThrows(ProtocolException.class, () -> reopened.find(otherOwner));
      var lease = reopened.acquire(key(SESSION, ROOT, 0), WORKER, 10, 100);
      reopened.publish(lease, 20, SealedJobs.Outcome.complete(input.digest()));
      assertTrue(sessions.checkpointReady(SESSION, PRODUCER, 0, 1));
      assertTrue(reopened.ready(30, 8).isEmpty()); reopened.audit();
    }
  }

  @Test void concurrentAcquisitionAndExpiredReopenNeverAllowStalePublication() throws Exception {
    var sessions = sessions(); declare(sessions, SESSION, 0, null, List.of(1L));
    var jobs = new SealedJobs(sessions);
    try (var payloads = payloads(); var workers = Executors.newFixedThreadPool(2)) {
      var input = input(payloads, SESSION, ROOT, null); jobs.admit(input);
      var start = new CountDownLatch(1);
      var key = key(SESSION, ROOT, 0);
      var one = workers.submit(() -> acquireAfter(start, new SealedJobs(sessions()), key));
      var two = workers.submit(() -> acquireAfter(start, new SealedJobs(sessions()), key));
      start.countDown();
      var first = one.get(5, TimeUnit.SECONDS); var second = two.get(5, TimeUnit.SECONDS);
      assertNotEquals(first == null, second == null);
      var lease = first == null ? second : first;
      assertTrue(jobs.ready(109, 8).isEmpty());
      assertEquals(List.of(key), jobs.ready(110, 8));
      assertThrows(ProtocolException.class, () -> jobs.publish(lease, 110, SealedJobs.Outcome.complete(input.digest())));
      var reopened = new SealedJobs(sessions());
      var replacement = reopened.acquire(key, WORKER, 110, 100);
      assertEquals(lease.epoch() + 1, replacement.epoch());
      assertThrows(ProtocolException.class, () -> jobs.publish(lease, 20, SealedJobs.Outcome.complete(input.digest())));
      reopened.publish(replacement, 120, SealedJobs.Outcome.complete(input.digest()));
      reopened.publish(replacement, 1000, SealedJobs.Outcome.complete(input.digest()));
      assertThrows(ProtocolException.class, () -> reopened.publish(replacement, 120, SealedJobs.Outcome.failed()));
      assertEquals(SealedJobs.FINISHED, reopened.find(key).orElseThrow().state());
      assertTrue(sessions.checkpointReady(SESSION, PRODUCER, 0, 1));
    }
  }

  @Test void refusalIsRetainedAndNotEntityCompletionEvenWithDiagnosticZero() throws Exception {
    var sessions = sessions(); declare(sessions, SESSION, 0, null, List.of(1L));
    var jobs = new SealedJobs(sessions);
    try (var payloads = payloads()) {
      jobs.admit(input(payloads, SESSION, ROOT, null));
      var lease = jobs.acquire(key(SESSION, ROOT, 0), WORKER, 1, 100);
      jobs.publish(lease, 2, SealedJobs.Outcome.refused(0));
      var retained = new SealedJobs(sessions()).find(lease.key()).orElseThrow();
      assertEquals(SealedJobs.REFUSED, retained.state()); assertEquals(0L, retained.outcome().refusal());
      assertFalse(sessions.checkpointReady(SESSION, PRODUCER, 0, 1));
      assertTrue(jobs.ready(1000, 8).isEmpty());
      assertThrows(ProtocolException.class, () -> jobs.acquire(lease.key(), UUID.randomUUID(), 1000, 100));
      jobs.publish(lease, 1000, SealedJobs.Outcome.refused(0));
    }
  }

  @Test void nestedClosureAtomicallyQueuesRehydrationAndPinsWholeScopeCompletion() throws Exception {
    var sessions = sessions(); declare(sessions, SESSION, 0, null, List.of(1L));
    var jobs = new SealedJobs(sessions);
    var middle = new SealedWork.EntityKey(7, 10); var leaf = new SealedWork.EntityKey(9, 20);
    try (var payloads = payloads()) {
      process(jobs, input(payloads, SESSION, ROOT, null), SealedJobs.Outcome.dehydrate());
      declare(sessions, SESSION, 7, ROOT, List.of(10L));
      process(jobs, input(payloads, SESSION, middle, ROOT), SealedJobs.Outcome.dehydrate());
      declare(sessions, SESSION, 9, middle, List.of(20L));
      assertTrue(jobs.closeScope(SESSION, PRODUCER, 9).isEmpty());
      var leafInput = input(payloads, SESSION, leaf, middle);
      process(jobs, leafInput, SealedJobs.Outcome.complete(leafInput.digest()));
      assertThrows(ProtocolException.class, () -> sessions.closeScope(SESSION, PRODUCER, 9));
      var child = jobs.closeScope(SESSION, PRODUCER, 9).orElseThrow();
      assertEquals(middle, child.parent()); assertEquals(7, child.state());
      assertFalse(sessions.checkpointReady(SESSION, PRODUCER, 0, 1));
      assertTrue(jobs.closeScope(SESSION, PRODUCER, 7).isEmpty());
      var job = jobs.find(key(SESSION, middle, 1)).orElseThrow();
      assertArrayEquals(SealedScope.encode(child.digest()), SealedScope.encode(job.input().child()));
      assertThrows(ProtocolException.class, () -> sessions.rehydrated(SESSION, PRODUCER, middle, true, new byte[32]));
      var lease = jobs.acquire(job.key(), WORKER, 1, 100);
      assertThrows(ProtocolException.class, () -> jobs.publish(lease, 2, SealedJobs.Outcome.dehydrate()));
      jobs.publish(lease, 2, SealedJobs.Outcome.complete(new byte[32]));
      assertEquals(3, jobs.closeScope(SESSION, PRODUCER, 9).orElseThrow().state());
      jobs.closeScope(SESSION, PRODUCER, 7).orElseThrow();
      var rootJob = key(SESSION, ROOT, 1);
      jobs.publish(jobs.acquire(rootJob, WORKER, 1, 100), 2, SealedJobs.Outcome.complete(new byte[32]));
      assertTrue(sessions.checkpointReady(SESSION, PRODUCER, 0, 1));
      new SealedJobs(sessions()).audit();
      sql("DELETE FROM ps_java_jobs WHERE kind=1 AND scope=7 AND entity=10");
      assertEquals(Wire.ERROR_INTEGRITY, assertThrows(ProtocolException.class, jobs::audit).errorCode());
      assertEquals(Wire.ERROR_INTEGRITY, assertThrows(ProtocolException.class, () -> sessions.checkpointReady(SESSION, PRODUCER, 0, 1)).errorCode());
    }
  }

  @Test void failedChildrenPropagateFailureWithoutARehydrationCallback() throws Exception {
    var sessions = sessions(); declare(sessions, SESSION, 0, null, List.of(1L));
    var jobs = new SealedJobs(sessions); var child = new SealedWork.EntityKey(7, 1);
    try (var payloads = payloads()) {
      process(jobs, input(payloads, SESSION, ROOT, null), SealedJobs.Outcome.dehydrate());
      declare(sessions, SESSION, 7, ROOT, List.of(1L));
      process(jobs, input(payloads, SESSION, child, ROOT), SealedJobs.Outcome.failed());
      var closure = jobs.closeScope(SESSION, PRODUCER, 7).orElseThrow();
      assertEquals(4, closure.state()); assertEquals(BigInteger.ONE, closure.digest().failed());
      assertTrue(jobs.find(key(SESSION, ROOT, 1)).isEmpty());
      assertEquals(4, jobs.closeScope(SESSION, PRODUCER, 7).orElseThrow().state());
      assertTrue(sessions.checkpointReady(SESSION, PRODUCER, 0, 1));
    }
  }

  @Test void fullQueueRollsBackBothScopeClosureAndRehydrationTransition() throws Exception {
    var sessions = sessions(); declare(sessions, SESSION, 0, null, LongStream.rangeClosed(1, 33).boxed().toList());
    var jobs = new SealedJobs(sessions); var child = new SealedWork.EntityKey(7, 1);
    try (var payloads = payloads()) {
      process(jobs, input(payloads, SESSION, ROOT, null), SealedJobs.Outcome.dehydrate());
      declare(sessions, SESSION, 7, ROOT, List.of(1L));
      var input = input(payloads, SESSION, child, ROOT); process(jobs, input, SealedJobs.Outcome.complete(input.digest()));
      for (int id = 2; id <= 33; id++) jobs.admit(input(payloads, SESSION, new SealedWork.EntityKey(0, id), null));
      assertEquals(Wire.ERROR_LIMIT_EXCEEDED, assertThrows(ProtocolException.class, () -> jobs.closeScope(SESSION, PRODUCER, 7)).errorCode());
      assertEquals(1, scalar("SELECT count(*) FROM ps_java_scopes WHERE id=7 AND closure IS NULL"));
      assertEquals(6, scalar("SELECT state FROM ps_java_entities WHERE scope=0 AND id=1"));
      assertEquals(0, scalar("SELECT count(*) FROM ps_java_jobs WHERE kind=1"));
      var key = key(SESSION, new SealedWork.EntityKey(0, 2), 0);
      jobs.publish(jobs.acquire(key, WORKER, 1, 100), 2, SealedJobs.Outcome.failed());
      assertEquals(7, jobs.closeScope(SESSION, PRODUCER, 7).orElseThrow().state());
      assertEquals(32, jobs.ready(3, 128).size());
    }
  }

  @Test void globalQueueLimitAppliesAcrossIndependentHandlesAndSessions() throws Exception {
    var sessions = sessions(); var jobs = new SealedJobs(sessions);
    try (var payloads = payloads()) {
      for (int session = 0; session < 4; session++) {
        String name = SESSION + session;
        declare(sessions, name, 0, null, LongStream.rangeClosed(1, 32).boxed().toList());
        for (int id = 1; id <= 32; id++) jobs.admit(input(payloads, name, new SealedWork.EntityKey(0, id), null));
      }
      declare(sessions, SESSION, 0, null, List.of(1L));
      var stored = input(payloads, SESSION, ROOT, null);
      assertEquals(Wire.ERROR_LIMIT_EXCEEDED, assertThrows(ProtocolException.class, () -> new SealedJobs(sessions()).admit(stored)).errorCode());
      assertEquals(1, scalar("SELECT count(*) FROM ps_java_entities WHERE session='queued-work' AND state IS NULL AND managed=0"));
      assertEquals(128, new SealedJobs(sessions()).ready(0, 128).size());
    }
  }

  @Test void missingChangedAndOversizedJobRecordsFailClosed() throws Exception {
    var sessions = sessions(); declare(sessions, SESSION, 0, null, List.of(1L));
    var jobs = new SealedJobs(sessions);
    try (var payloads = payloads()) {
      jobs.admit(input(payloads, SESSION, ROOT, null));
      sql("UPDATE ps_java_jobs SET checksum=zeroblob(32)");
      assertEquals(Wire.ERROR_INTEGRITY, assertThrows(ProtocolException.class, () -> jobs.ready(0, 8)).errorCode());
      sql("UPDATE ps_java_jobs SET input=zeroblob(5000000)");
      assertEquals(Wire.ERROR_INTEGRITY, assertThrows(ProtocolException.class, () -> jobs.find(key(SESSION, ROOT, 0))).errorCode());
      sql("DELETE FROM ps_java_jobs");
      assertEquals(Wire.ERROR_INTEGRITY, assertThrows(ProtocolException.class, jobs::audit).errorCode());
      assertThrows(ProtocolException.class, () -> sessions.processed(SESSION, PRODUCER, ROOT, 3, new byte[32]));
      assertEquals(Wire.ERROR_INTEGRITY, assertThrows(ProtocolException.class, () -> sessions.checkpointReady(SESSION, PRODUCER, 0, 1)).errorCode());
    }
  }

  @Test void completedJobsKeepTheirRetainedMetadataChargeAcrossReopen() throws Exception {
    var sessions = sessions();
    var all = LongStream.rangeClosed(1, 270).boxed().toList();
    sessions.declare(new SealedWork.Declaration(SESSION, PRODUCER, 0, null, BigInteger.ZERO,
        all.subList(0, 256), 0, null), 7, 1024);
    sessions.declare(new SealedWork.Declaration(SESSION, PRODUCER, 0, null, BigInteger.ONE,
        all.subList(256, all.size()), SealedWork.SEAL, SealedWork.sealDigest(SESSION, PRODUCER, 0, null, all)), 7, 1024);
    var jobs = new SealedJobs(sessions); int completed = 0; boolean refused = false;
    try (var payloads = payloads()) {
      for (long id : all) {
        var entity = new SealedWork.EntityKey(0, id);
        var identity = new SealedPayloadStore.Identity(SESSION, PRODUCER, entity);
        var header = new SealedTransport.Header(entity, null, 0, null, BigInteger.ONE, null, Map.of("application-data", "x".repeat(65_000)), null);
        SealedPayloadStore.Stored input;
        try (var receiver = payloads.begin(identity, header)) {
          receiver.write(new byte[] {42}, 0, 1);
          try (var receipt = receiver.finish()) { input = payloads.install(List.of(receipt)); }
        }
        try { jobs.admit(input); }
        catch (ProtocolException error) {
          assertEquals(Wire.ERROR_LIMIT_EXCEEDED, error.errorCode());
          assertTrue(completed >= 250, "quota must permit the expected metadata before refusing");
          assertEquals(completed, scalar("SELECT count(*) FROM ps_java_jobs"));
          assertEquals(0, scalar("SELECT count(*) FROM ps_java_jobs WHERE state<2"));
          assertEquals(1, scalar("SELECT count(*) FROM ps_java_entities WHERE id=" + id + " AND state IS NULL AND managed=0"));
          assertEquals(Wire.ERROR_LIMIT_EXCEEDED, assertThrows(ProtocolException.class, () -> new SealedJobs(sessions()).admit(input)).errorCode());
          refused = true; break;
        }
        jobs.publish(jobs.acquire(new SealedJobs.Key(identity, 0), WORKER, 1, 100), 2, SealedJobs.Outcome.complete(input.digest()));
        completed++;
      }
      assertTrue(refused, "retained terminal metadata must have a finite budget");
      assertFalse(sessions.checkpointReady(SESSION, PRODUCER, 0, all.getLast()));
    }
  }

  @Test void oldSchemaIsRefusedWithoutAddingDispatchTablesOrChangingPolicy() throws Exception {
    sessions();
    sql("DROP TABLE ps_java_jobs"); sql("DROP TABLE ps_java_job_policy"); sql("UPDATE ps_java_meta SET version=1");
    assertThrows(SQLException.class, this::sessions);
    assertEquals(1, scalar("SELECT version FROM ps_java_meta"));
    assertEquals(0, scalar("SELECT count(*) FROM sqlite_master WHERE name LIKE 'ps_java_job%'"));
  }

  @Test void abruptExitAfterAcquisitionRetainsDispatchAndRequiresANewerFence() throws Exception {
    Process process = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin/java").toString(),
        "--enable-native-access=ALL-UNNAMED", "-cp", System.getProperty("java.class.path"), SealedJobsTest.class.getName(), directory.toString())
        .redirectErrorStream(true).redirectOutput(directory.resolve("child.log").toFile()).start();
    try {
      assertTrue(process.waitFor(30, TimeUnit.SECONDS));
      assertEquals(0, process.exitValue(), () -> read(directory.resolve("child.log")));
    } finally { if (process.isAlive()) process.destroyForcibly().waitFor(5, TimeUnit.SECONDS); }
    var jobs = new SealedJobs(sessions()); var key = key(SESSION, ROOT, 0);
    assertTrue(jobs.ready(109, 8).isEmpty()); assertEquals(List.of(key), jobs.ready(110, 8));
    var replacement = jobs.acquire(key, UUID.randomUUID(), 110, 100);
    assertEquals(2, replacement.epoch());
    assertThrows(ProtocolException.class, () -> jobs.publish(new SealedJobs.Lease(key, 1, WORKER, 110), 20, SealedJobs.Outcome.complete(new byte[32])));
    try (var payloads = payloads()) {
      var input = payloads.find(key.identity()).orElseThrow();
      jobs.publish(replacement, 120, SealedJobs.Outcome.complete(input.digest()));
    }
    assertTrue(sessions().checkpointReady(SESSION, PRODUCER, 0, 1));
  }

  public static void main(String[] args) throws Exception {
    var test = new SealedJobsTest(); test.directory = Path.of(args[0]);
    var sessions = test.sessions(); declare(sessions, SESSION, 0, null, List.of(1L));
    var jobs = new SealedJobs(sessions);
    try (var payloads = test.payloads()) { jobs.admit(input(payloads, SESSION, ROOT, null)); }
    jobs.acquire(key(SESSION, ROOT, 0), WORKER, 10, 100);
    Runtime.getRuntime().halt(0);
  }

  private static SealedJobs.Lease acquireAfter(CountDownLatch start, SealedJobs jobs, SealedJobs.Key key) throws Exception {
    if (!start.await(5, TimeUnit.SECONDS)) throw new AssertionError("acquisition was not released");
    try { return jobs.acquire(key, UUID.randomUUID(), 10, 100); }
    catch (ProtocolException expected) { assertEquals(Wire.ERROR_ENTITY_INVALID, expected.errorCode()); return null; }
  }
  private SealedSessionStore sessions() throws Exception { return SealedSessionStore.open(directory.resolve("sessions.sqlite3")); }
  private SealedPayloadStore payloads() throws Exception { return SealedPayloadStore.open(directory.resolve("payloads"), SealedPayloadStore.Limits.defaults()); }
  private static void declare(SealedSessionStore store, String session, long scope, SealedWork.EntityKey parent, List<Long> ids) throws Exception {
    store.declare(new SealedWork.Declaration(session, PRODUCER, scope, parent, BigInteger.ZERO, ids, SealedWork.SEAL,
        SealedWork.sealDigest(session, PRODUCER, scope, parent, ids)), 7, 1024);
  }
  private static SealedPayloadStore.Stored input(SealedPayloadStore payloads, String session, SealedWork.EntityKey key, SealedWork.EntityKey parent) throws Exception {
    var identity = new SealedPayloadStore.Identity(session, PRODUCER, key);
    var header = new SealedTransport.Header(key, parent, 0, null, BigInteger.ONE, null, Map.of(), null);
    try (var receiver = payloads.begin(identity, header)) {
      receiver.write(new byte[] {42}, 0, 1);
      try (var received = receiver.finish()) { return payloads.install(List.of(received)); }
    }
  }
  private static SealedJobs.Key key(String session, SealedWork.EntityKey entity, int kind) {
    return new SealedJobs.Key(new SealedPayloadStore.Identity(session, PRODUCER, entity), kind);
  }
  private static void process(SealedJobs jobs, SealedPayloadStore.Stored input, SealedJobs.Outcome outcome) throws Exception {
    jobs.admit(input); jobs.publish(jobs.acquire(new SealedJobs.Key(input.identity(), 0), WORKER, 1, 100), 2, outcome);
  }
  private void sql(String sql) throws Exception {
    try (var connection = DriverManager.getConnection("jdbc:sqlite:" + directory.resolve("sessions.sqlite3")); var statement = connection.createStatement()) { statement.execute(sql); }
  }
  private long scalar(String sql) throws Exception {
    try (var connection = DriverManager.getConnection("jdbc:sqlite:" + directory.resolve("sessions.sqlite3")); var query = connection.createStatement(); var rows = query.executeQuery(sql)) {
      assertTrue(rows.next()); return rows.getLong(1);
    }
  }
  private static String read(Path path) { try { return Files.readString(path); } catch (Exception error) { return error.toString(); } }
}
