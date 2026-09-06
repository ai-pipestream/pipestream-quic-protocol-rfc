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

  @Test void admittedPublicationSurvivesUnrelatedWalExhaustionWithReaderPinned() throws Exception {
    Path path = directory.resolve("sessions.sqlite3");
    var limits = new SealedSessionStore.FileLimits(8L << 20, 512L << 10, 1L << 20, 65536);
    var sessions = SealedSessionStore.open(path, limits);
    declare(sessions, SESSION, 0, null, List.of(1L));
    var jobs = new SealedJobs(sessions);
    try (var payloads = payloads()) {
      var input = input(payloads, SESSION, ROOT, null);
      jobs.admit(input);
      var lease = jobs.acquire(key(SESSION, ROOT, SealedJobs.PROCESS), WORKER, 10, 100);
      var before = jobs.find(lease.key()).orElseThrow();
      sessions.declare(new SealedWork.Declaration("filler", PRODUCER, 0, null,
          BigInteger.ZERO, List.of(1L), 0, null), 7, 16384);
      try (var reader = SealedSqliteFiles.open(path, limits).connect(); var statement = reader.createStatement()) {
        try (var checkpoint = statement.executeQuery("PRAGMA wal_checkpoint(TRUNCATE)")) {
          assertTrue(checkpoint.next()); assertEquals(0, checkpoint.getInt(1));
        }
        statement.execute("BEGIN");
        try (var snapshot = statement.executeQuery("SELECT count(*) FROM ps_java_jobs")) {
          assertTrue(snapshot.next()); assertTrue(snapshot.getInt(1) > 0);
        }
        boolean saturated = false;
        int writes = 0;
        for (; writes < 10000; writes++) {
          try {
            // Real admissions use the public store and its guarded connection.
            // No raw writer, corrupt policy or artificial side table fills the WAL.
            sessions.declare(new SealedWork.Declaration("filler", PRODUCER, 0, null,
                BigInteger.valueOf(writes + 1L), List.of(writes + 2L), 0, null), 7, 16384);
          } catch (ProtocolException full) {
            assertEquals(Wire.ERROR_LIMIT_EXCEEDED, full.errorCode());
            saturated = true; break;
          }
        }
        assertTrue(saturated); assertTrue(writes > 0);
        assertEquals(writes + 1, sessions.declared("filler", PRODUCER, 0).size());
        System.err.printf("Pinned Java WAL: declarations=%d bytes=%d cap=%d%n",
            writes, sessions.fileUsage().walBytes(), limits.walBytes());
        var retained = jobs.find(lease.key()).orElseThrow();
        assertEquals(before.state(), retained.state()); assertEquals(before.epoch(), retained.epoch());
        assertEquals(before.worker(), retained.worker()); assertEquals(before.expires(), retained.expires());
        assertArrayEquals(before.input().digest(), retained.input().digest());
        assertFalse(sessions.checkpointReady(SESSION, PRODUCER, 0, 1));
        // The same reader stays pinned throughout publication and verification.
        jobs.publish(lease, 20, SealedJobs.Outcome.complete(input.digest()));
        assertEquals(SealedJobs.FINISHED, jobs.find(lease.key()).orElseThrow().state());
        assertTrue(sessions.checkpointReady(SESSION, PRODUCER, 0, 1));
        assertTrue(sessions.fileUsage().walBytes() <= limits.walBytes());
        jobs.audit();
      }
    }
  }

  @Test void admissionAndJobCommitTogetherAndSurviveReopenWithoutManualPublication() throws Exception {
    var sessions = sessions(); declare(sessions, SESSION, 0, null, List.of(1L));
    var jobs = new SealedJobs(sessions);
    try (var payloads = payloads()) {
      var input = input(payloads, SESSION, ROOT, null);
      sql("CREATE TRIGGER fail_job BEFORE INSERT ON ps_java_jobs BEGIN SELECT RAISE(ABORT,'injected job write failure'); END");
      assertThrows(SQLException.class, () -> jobs.admit(input));
      assertEquals(1, scalar("SELECT count(*) FROM ps_java_entities WHERE substr(image,9,8)=zeroblob(8)"));
      assertEquals(0, scalar("SELECT count(*) FROM ps_java_jobs"));
      sql("DROP TRIGGER fail_job");
      jobs.admit(input);
      var reserved = sessions.jobUsage();
      assertEquals(1, reserved.processingJobs()); assertEquals(1, reserved.reservedRehydrationSlots());
      assertEquals(0, reserved.waitingParents());
      assertTrue(reserved.rehydrationReservedBytes() > reserved.retainedJobBytes());
      assertEquals(List.of(key(SESSION, ROOT, SealedJobs.PROCESS)), jobs.ready(0, 8));
      assertThrows(ProtocolException.class, () -> sessions.processed(SESSION, PRODUCER, ROOT, 3, input.digest()));
      var reopened = new SealedJobs(sessions());
      assertEquals(jobs.ready(0, 8), reopened.ready(0, 8));
      assertThrows(ProtocolException.class, () -> reopened.admit(input));
      var otherOwner = new SealedJobs.Key(new SealedPayloadStore.Identity(SESSION, UUID.randomUUID(), ROOT), 0);
      assertThrows(ProtocolException.class, () -> reopened.find(otherOwner));
      var lease = reopened.acquire(key(SESSION, ROOT, 0), WORKER, 10, 100);
      assertEquals(reserved, sessions.jobUsage(), "acquisition must retain completion credit");
      reopened.publish(lease, 20, SealedJobs.Outcome.complete(input.digest()));
      assertEquals(reserved.retainedJobBytes() + reserved.rehydrationReservedBytes(), sessions.jobUsage().retainedJobBytes());
      assertEquals(0, sessions.jobUsage().rehydrationReservedBytes());
      assertEquals(0, sessions.jobUsage().processingJobs());
      assertEquals(0, sessions.jobUsage().reservedRehydrationSlots());
      assertTrue(sessions.checkpointReady(SESSION, PRODUCER, 0, 1));
      assertTrue(reopened.ready(30, 8).isEmpty()); reopened.audit();
    }
  }

  @Test void executionChangesOnlyThePreallocatedJobImageAndKeepsItsRowIdentity() throws Exception {
    var sessions = sessions(); declare(sessions, SESSION, 0, null, List.of(1L));
    var jobs = new SealedJobs(sessions);
    try (var payloads = payloads()) {
      var input = input(payloads, SESSION, ROOT, null); jobs.admit(input);
      byte[] queued = jobImage();
      long rowid = scalar("SELECT rowid FROM ps_java_jobs WHERE kind=0");
      assertEquals(256, queued.length);
      sql("CREATE TRIGGER no_job_update BEFORE UPDATE ON ps_java_jobs BEGIN SELECT RAISE(ABORT,'SQL job update forbidden'); END");
      sql("CREATE TRIGGER no_job_insert BEFORE INSERT ON ps_java_jobs BEGIN SELECT RAISE(ABORT,'SQL job insert forbidden'); END");
      sql("CREATE TRIGGER no_job_delete BEFORE DELETE ON ps_java_jobs BEGIN SELECT RAISE(ABORT,'SQL job delete forbidden'); END");
      var lease = jobs.acquire(key(SESSION, ROOT, SealedJobs.PROCESS), WORKER, 10, 100);
      byte[] running = jobImage();
      assertFalse(java.util.Arrays.equals(queued, running)); assertEquals(256, running.length);
      assertEquals(rowid, scalar("SELECT rowid FROM ps_java_jobs WHERE kind=0"));
      jobs.publish(lease, 20, SealedJobs.Outcome.complete(input.digest()));
      assertFalse(java.util.Arrays.equals(running, jobImage()));
      assertEquals(256, jobImage().length); assertEquals(rowid, scalar("SELECT rowid FROM ps_java_jobs WHERE kind=0"));
      assertEquals(2, scalar("SELECT count(*) FROM ps_java_jobs"));
      assertEquals(1, scalar("SELECT count(*) FROM ps_java_jobs WHERE kind=1 AND substr(image,9,4)=x'00000005'"));
      assertEquals(0, scalar("SELECT count(*) FROM sqlite_schema WHERE type='index' AND name='ps_java_jobs_ready'"));
      new SealedJobs(sessions()).audit();
      assertTrue(sessions.checkpointReady(SESSION, PRODUCER, 0, 1));
    }
  }

  @Test void imageWriteFailureRollsBackTheAlreadyChangedEntityOutcome() throws Exception {
    var sessions = sessions(); declare(sessions, SESSION, 0, null, List.of(1L));
    var jobs = new SealedJobs(sessions);
    try (var payloads = payloads()) {
      var input = input(payloads, SESSION, ROOT, null); jobs.admit(input);
      var lease = jobs.acquire(key(SESSION, ROOT, SealedJobs.PROCESS), WORKER, 10, 100);
      byte[] running = jobImage();
      // SQLite refuses writable BLOB handles on indexed images. Unlike a SQL
      // UPDATE trigger, this injects a failure in the actual publication path.
      sql("CREATE INDEX refuse_image_write ON ps_java_jobs(image)");
      assertThrows(SQLException.class, () -> jobs.publish(lease, 20, SealedJobs.Outcome.complete(input.digest())));
      assertArrayEquals(running, jobImage());
      var entity = sessions.transaction(connection -> SealedSessionStore.entity(connection, SESSION, ROOT));
      assertEquals(2, entity.state()); assertNull(entity.value().outputDigest());
      assertFalse(sessions.checkpointReady(SESSION, PRODUCER, 0, 1));
      jobs.audit();
      sql("DROP INDEX refuse_image_write");
      jobs.publish(lease, 20, SealedJobs.Outcome.complete(input.digest()));
      assertTrue(sessions.checkpointReady(SESSION, PRODUCER, 0, 1));
    }
  }

  @Test void corruptImagesCannotHideFromDiscoveryOrEraseCompletionObligations() throws Exception {
    var sessions = sessions(); declare(sessions, SESSION, 0, null, List.of(1L));
    var jobs = new SealedJobs(sessions);
    try (var payloads = payloads()) {
      jobs.admit(input(payloads, SESSION, ROOT, null));
      byte[] original = jobImage();
      for (int offset : List.of(0, 8, 11, 12, 20, 36, 44, 100, 224, 255)) {
        byte[] corrupt = original.clone(); corrupt[offset] ^= 0x7f;
        replaceJobImage(corrupt);
        assertEquals(Wire.ERROR_INTEGRITY, assertThrows(ProtocolException.class,
            () -> jobs.find(key(SESSION, ROOT, SealedJobs.PROCESS))).errorCode());
        assertEquals(Wire.ERROR_INTEGRITY, assertThrows(ProtocolException.class,
            () -> jobs.ready(Long.MAX_VALUE, 8)).errorCode());
        assertEquals(Wire.ERROR_INTEGRITY, assertThrows(ProtocolException.class,
            () -> sessions.checkpointReady(SESSION, PRODUCER, 0, 1)).errorCode());
        replaceJobImage(original);
      }
      assertEquals(List.of(key(SESSION, ROOT, SealedJobs.PROCESS)), jobs.ready(0, 8));
      jobs.audit();
    }
  }

  @Test void expiryProjectionKeepsUnsignedByteBoundariesAndWideJavaCountersInOrder() throws Exception {
    var sessions = sessions();
    List<Long> expiries = List.of(127L, 128L, 255L, 256L, 65535L, 65536L, 1L << 32, Long.MAX_VALUE);
    List<Long> ids = LongStream.rangeClosed(1, expiries.size()).boxed().toList();
    declare(sessions, SESSION, 0, null, ids);
    var jobs = new SealedJobs(sessions);
    try (var payloads = payloads()) {
      for (int index = 0; index < ids.size(); index++) {
        var entity = new SealedWork.EntityKey(0, ids.get(index));
        jobs.admit(input(payloads, SESSION, entity, null));
        jobs.acquire(key(SESSION, entity, SealedJobs.PROCESS), WORKER, expiries.get(index) - 1, 1);
      }
      for (int index = 0; index < expiries.size(); index++) {
        long expiry = expiries.get(index);
        assertEquals(ids.subList(0, index), jobs.ready(expiry - 1, 32).stream()
            .map(job -> job.identity().entity().entityId()).toList());
        assertEquals(ids.subList(0, index + 1), jobs.ready(expiry, 32).stream()
            .map(job -> job.identity().entity().entityId()).toList());
      }
    }
  }

  @Test void preImageSchemaPolicyIsRefusedWithoutConversionOrJobChanges() throws Exception {
    var sessions = sessions(); declare(sessions, SESSION, 0, null, List.of(1L));
    var jobs = new SealedJobs(sessions);
    try (var payloads = payloads()) {
      jobs.admit(input(payloads, SESSION, ROOT, null));
      byte[] image = jobImage();
      sql("UPDATE ps_java_meta SET version=4");
      assertThrows(SQLException.class, this::sessions);
      assertArrayEquals(image, jobImage());
      assertEquals(4, scalar("SELECT version FROM ps_java_meta"));
      assertEquals(2, scalar("SELECT count(*) FROM ps_java_jobs"));
    }
  }

  private byte[] jobImage() throws SQLException {
    try (var connection = DriverManager.getConnection("jdbc:sqlite:" + directory.resolve("sessions.sqlite3"));
        var statement = connection.createStatement(); var rows = statement.executeQuery("SELECT image FROM ps_java_jobs WHERE kind=0")) {
      assertTrue(rows.next()); byte[] bytes = rows.getBytes(1); assertFalse(rows.next()); return bytes;
    }
  }

  private void replaceJobImage(byte[] bytes) throws SQLException {
    try (var connection = DriverManager.getConnection("jdbc:sqlite:" + directory.resolve("sessions.sqlite3"));
        var update = connection.prepareStatement("UPDATE ps_java_jobs SET image=? WHERE kind=0")) {
      update.setBytes(1, bytes); assertEquals(1, update.executeUpdate());
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
      assertEquals(0, sessions.jobUsage().rehydrationReservedBytes());
      assertEquals(0, sessions.jobUsage().processingJobs());
      assertEquals(0, sessions.jobUsage().reservedRehydrationSlots());
      assertThrows(ProtocolException.class, () -> jobs.acquire(lease.key(), UUID.randomUUID(), 1000, 100));
      jobs.publish(lease, 1000, SealedJobs.Outcome.refused(0));
    }
  }

  @Test void recursiveConversionAndRetirementNeverInsertReplaceOrDeleteAdmittedRows() throws Exception {
    var sessions = sessions(); declare(sessions, SESSION, 0, null, List.of(1L));
    var jobs = new SealedJobs(sessions);
    try (var payloads = payloads()) {
      process(jobs, input(payloads, SESSION, ROOT, null), SealedJobs.Outcome.dehydrate());
      long reservedRowid = scalar("SELECT rowid FROM ps_java_jobs WHERE kind=1 AND scope=0");
      long reservedLength = scalar("SELECT length(input) FROM ps_java_jobs WHERE kind=1 AND scope=0");
      assertTrue(jobs.find(key(SESSION, ROOT, SealedJobs.REHYDRATE)).isEmpty());
      assertThrows(ProtocolException.class, () -> jobs.acquire(key(SESSION, ROOT, SealedJobs.REHYDRATE), WORKER, 10, 100));
      declare(sessions, SESSION, 7, ROOT, List.of(10L));
      var child = new SealedWork.EntityKey(7, 10);
      var input = input(payloads, SESSION, child, ROOT);
      process(jobs, input, SealedJobs.Outcome.complete(input.digest()));
      assertEquals(4, scalar("SELECT count(*) FROM ps_java_jobs"));
      for (String table : List.of("ps_java_jobs", "ps_java_entities", "ps_java_scopes")) {
        for (String operation : List.of("INSERT", "UPDATE", "DELETE")) {
          sql("CREATE TRIGGER forbid_" + table + operation + " BEFORE " + operation + " ON " + table
              + " BEGIN SELECT RAISE(ABORT,'unexpected SQL row mutation'); END");
        }
      }
      var before = sessions.jobUsage();
      jobs.closeScope(SESSION, PRODUCER, 7).orElseThrow();
      assertEquals(reservedRowid, scalar("SELECT rowid FROM ps_java_jobs WHERE kind=1 AND scope=0"));
      assertEquals(reservedLength, scalar("SELECT length(input) FROM ps_java_jobs WHERE kind=1 AND scope=0"));
      var lease = jobs.acquire(key(SESSION, ROOT, SealedJobs.REHYDRATE), WORKER, 10, 100);
      jobs.publish(lease, 20, SealedJobs.Outcome.complete(new byte[32]));
      assertEquals(4, scalar("SELECT count(*) FROM ps_java_jobs"));
      assertEquals(before.retainedJobBytes() + before.rehydrationReservedBytes(), sessions.jobUsage().retainedJobBytes());
      assertTrue(SealedSessionStore.open(directory.resolve("sessions.sqlite3")).checkpointReady(SESSION, PRODUCER, 0, 1));
      new SealedJobs(sessions()).audit();
    }
  }

  @Test void reservedRowCorruptionAndLossCannotBecomeAnAbsentOptionalJob() throws Exception {
    var sessions = sessions(); declare(sessions, SESSION, 0, null, List.of(1L));
    var jobs = new SealedJobs(sessions);
    try (var payloads = payloads()) {
      jobs.admit(input(payloads, SESSION, ROOT, null));
      var future = key(SESSION, ROOT, SealedJobs.REHYDRATE);
      assertTrue(jobs.find(future).isEmpty());
      byte[] descriptor;
      try (var connection = DriverManager.getConnection("jdbc:sqlite:" + directory.resolve("sessions.sqlite3"));
          var query = connection.createStatement(); var rows = query.executeQuery("SELECT input FROM ps_java_jobs WHERE kind=1")) {
        assertTrue(rows.next()); descriptor = rows.getBytes(1);
      }
      byte[] corrupt = descriptor.clone(); corrupt[corrupt.length - 1] = 1;
      try (var connection = DriverManager.getConnection("jdbc:sqlite:" + directory.resolve("sessions.sqlite3"));
          var update = connection.prepareStatement("UPDATE ps_java_jobs SET input=? WHERE kind=1")) {
        update.setBytes(1, corrupt); assertEquals(1, update.executeUpdate());
        assertEquals(Wire.ERROR_INTEGRITY, assertThrows(ProtocolException.class, jobs::audit).errorCode());
        assertEquals(Wire.ERROR_INTEGRITY, assertThrows(ProtocolException.class, () -> jobs.find(future)).errorCode());
        update.setBytes(1, descriptor); assertEquals(1, update.executeUpdate());
      }
      jobs.audit();
      sql("DELETE FROM ps_java_jobs WHERE kind=1");
      assertEquals(Wire.ERROR_INTEGRITY, assertThrows(ProtocolException.class, jobs::audit).errorCode());
      assertEquals(Wire.ERROR_INTEGRITY, assertThrows(ProtocolException.class, () -> jobs.find(future)).errorCode());
      assertEquals(Wire.ERROR_INTEGRITY, assertThrows(ProtocolException.class,
          () -> jobs.acquire(key(SESSION, ROOT, SealedJobs.PROCESS), WORKER, 10, 100)).errorCode());
      assertEquals(Wire.ERROR_INTEGRITY, assertThrows(ProtocolException.class,
          () -> sessions.checkpointReady(SESSION, PRODUCER, 0, 1)).errorCode());
      assertEquals(1, scalar("SELECT count(*) FROM ps_java_jobs WHERE kind=0 AND substr(image,9,4)=x'00000000'"));
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
      assertEquals(0, sessions.jobUsage().rehydrationReservedBytes());
      assertEquals(0, sessions.jobUsage().processingJobs());
      assertEquals(0, sessions.jobUsage().reservedRehydrationSlots());
      assertEquals(0, sessions.jobUsage().waitingParents());
    }
  }

  @Test void fullQueueCannotConsumeWaitingParentReservationAndStorageFailureRollsBackClosure() throws Exception {
    var sessions = sessions(); declare(sessions, SESSION, 0, null, LongStream.rangeClosed(1, 34).boxed().toList());
    var jobs = new SealedJobs(sessions); var child = new SealedWork.EntityKey(7, 1);
    try (var payloads = payloads()) {
      process(jobs, input(payloads, SESSION, ROOT, null), SealedJobs.Outcome.dehydrate());
      declare(sessions, SESSION, 7, ROOT, List.of(1L));
      var input = input(payloads, SESSION, child, ROOT); process(jobs, input, SealedJobs.Outcome.complete(input.digest()));
      for (int id = 2; id <= 33; id++) jobs.admit(input(payloads, SESSION, new SealedWork.EntityKey(0, id), null));
      var before = sessions.jobUsage();
      assertEquals(32, before.processingJobs()); assertEquals(1, before.waitingParents());
      assertEquals(33, before.reservedRehydrationSlots()); assertEquals(0, before.rehydrationJobs());
      assertEquals(32, jobs.ready(3, 128).size());
      var excess = input(payloads, SESSION, new SealedWork.EntityKey(0, 34), null);
      assertEquals(Wire.ERROR_LIMIT_EXCEEDED, assertThrows(ProtocolException.class, () -> jobs.admit(excess)).errorCode());
      assertEquals(1, scalar("SELECT count(*) FROM ps_java_entities WHERE scope=0 AND id=34 AND substr(image,9,8)=zeroblob(8)"));
      assertEquals(before, sessions().jobUsage(), "reopen must retain the parent reservation");
      long futureRows = scalar("SELECT count(*) FROM ps_java_jobs WHERE kind=1");
      long futureRowid = scalar("SELECT rowid FROM ps_java_jobs WHERE kind=1 AND scope=0 AND entity=1");
      sql("CREATE INDEX fail_rehydrate ON ps_java_jobs(input)");
      assertThrows(SQLException.class, () -> jobs.closeScope(SESSION, PRODUCER, 7));
      assertEquals(1, scalar("SELECT count(*) FROM ps_java_scopes WHERE id=7 AND substr(closure_image,9,1)=x'00'"));
      assertEquals(6, sessions.transaction(connection -> SealedSessionStore.entity(connection, SESSION, ROOT)).state());
      assertEquals(futureRows, scalar("SELECT count(*) FROM ps_java_jobs WHERE kind=1"));
      assertTrue(jobs.find(key(SESSION, ROOT, SealedJobs.REHYDRATE)).isEmpty());
      assertEquals(before, sessions.jobUsage());
      sql("DROP INDEX fail_rehydrate");
      assertEquals(7, new SealedJobs(sessions()).closeScope(SESSION, PRODUCER, 7).orElseThrow().state());
      assertEquals(futureRows, scalar("SELECT count(*) FROM ps_java_jobs WHERE kind=1"));
      assertEquals(futureRowid, scalar("SELECT rowid FROM ps_java_jobs WHERE kind=1 AND scope=0 AND entity=1"));
      assertEquals(33, jobs.ready(3, 128).size());
      var after = sessions.jobUsage();
      assertEquals(32, after.processingJobs()); assertEquals(0, after.waitingParents());
      assertEquals(32, after.reservedRehydrationSlots()); assertEquals(1, after.rehydrationJobs());
      assertEquals(before.retainedJobBytes() + before.rehydrationReservedBytes(), after.retainedJobBytes() + after.rehydrationReservedBytes());
      assertEquals(scalar("SELECT length(input)+256 FROM ps_java_jobs WHERE kind=1 AND scope=0 AND entity=1"), before.rehydrationReservedBytes() - after.rehydrationReservedBytes());
      jobs.publish(jobs.acquire(key(SESSION, ROOT, 1), WORKER, 1, 100), 2, SealedJobs.Outcome.complete(new byte[32]));
      assertEquals(32, sessions.jobUsage().processingJobs());
      assertEquals(0, sessions.jobUsage().rehydrationJobs());
      var completed = sessions.jobUsage();
      jobs.closeScope(SESSION, PRODUCER, 7).orElseThrow();
      assertEquals(completed, sessions.jobUsage(), "closure replay must not allocate another job or reservation");
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
      assertEquals(1, scalar("SELECT count(*) FROM ps_java_entities WHERE session='queued-work' AND substr(image,9,8)=zeroblob(8)"));
      assertEquals(128, new SealedJobs(sessions()).ready(0, 128).size());
    }
  }

  @Test void waitingParentsDoNotBlockTheirChildrenAndReservedQueuesCannotHideOtherSessions() throws Exception {
    var sessions = sessions(); var jobs = new SealedJobs(sessions);
    declare(sessions, SESSION, 0, null, LongStream.rangeClosed(1, 32).boxed().toList());
    try (var payloads = payloads()) {
      for (int id = 1; id <= 32; id++) {
        process(jobs, input(payloads, SESSION, new SealedWork.EntityKey(0, id), null), SealedJobs.Outcome.dehydrate());
      }
      assertEquals(32, sessions.jobUsage().waitingParents());
      assertEquals(0, sessions.jobUsage().processingJobs());
      for (int id = 1; id <= 9; id++) {
        var parent = new SealedWork.EntityKey(0, id); var child = new SealedWork.EntityKey(id, 1);
        declare(sessions, SESSION, id, parent, List.of(1L));
        var stored = input(payloads, SESSION, child, parent);
        process(jobs, stored, SealedJobs.Outcome.complete(stored.digest()));
        jobs.closeScope(SESSION, PRODUCER, id).orElseThrow();
      }
      assertEquals(23, sessions.jobUsage().waitingParents());
      assertEquals(9, sessions.jobUsage().rehydrationJobs());
      String independent = "zzz-independent";
      declare(sessions, independent, 0, null, List.of(1L));
      jobs.admit(input(payloads, independent, ROOT, null));
      var page = jobs.ready(3, 4);
      assertEquals(4, page.size());
      assertTrue(page.contains(key(independent, ROOT, 0)), "one session cannot monopolize bounded discovery");
      assertEquals(3, page.stream().filter(item -> item.kind() == SealedJobs.REHYDRATE).count());
      new SealedJobs(sessions()).audit();
    }
  }

  @Test void largeMetadataAndMaximumIdentifiersConsumeExactlyTheirReservedDescriptor() throws Exception {
    var sessions = sessions(); var jobs = new SealedJobs(sessions);
    String session = "s".repeat(128);
    var root = new SealedWork.EntityKey(0, Wire.MAX_ENTITY_ID);
    var child = new SealedWork.EntityKey(0xffff_ffffL, Wire.MAX_ENTITY_ID);
    declare(sessions, session, 0, null, List.of(root.entityId()));
    try (var payloads = payloads()) {
      var stored = input(payloads, session, root, null, Map.of("data", "x".repeat(65_000)));
      process(jobs, stored, SealedJobs.Outcome.dehydrate());
      declare(sessions, session, child.scopeId(), root, List.of(child.entityId()));
      var leaf = input(payloads, session, child, root);
      process(jobs, leaf, SealedJobs.Outcome.complete(leaf.digest()));
      var before = sessions.jobUsage();
      assertTrue(before.rehydrationReservedBytes() > 65_000);
      new SealedJobs(sessions()).closeScope(session, PRODUCER, child.scopeId()).orElseThrow();
      var after = sessions.jobUsage();
      assertEquals(0, after.rehydrationReservedBytes());
      assertEquals(before.retainedJobBytes() + before.rehydrationReservedBytes(), after.retainedJobBytes());
      assertEquals(before.rehydrationReservedBytes(), scalar("SELECT length(input)+256 FROM ps_java_jobs WHERE kind=1 AND scope=0"));
      jobs.publish(jobs.acquire(key(session, root, 1), WORKER, 1, 100), 2, SealedJobs.Outcome.complete(stored.digest()));
      assertTrue(sessions.checkpointReady(session, PRODUCER, 0, root.entityId()));
    }
  }

  @Test void missingChangedAndOversizedJobRecordsFailClosed() throws Exception {
    var sessions = sessions(); declare(sessions, SESSION, 0, null, List.of(1L));
    var jobs = new SealedJobs(sessions);
    try (var payloads = payloads()) {
      jobs.admit(input(payloads, SESSION, ROOT, null));
      sql("UPDATE ps_java_jobs SET image=CAST(substr(image,1,224)||zeroblob(32) AS BLOB)");
      assertEquals(Wire.ERROR_INTEGRITY, assertThrows(ProtocolException.class, () -> jobs.ready(0, 8)).errorCode());
      sql("UPDATE ps_java_jobs SET input=zeroblob(5000000)");
      assertEquals(Wire.ERROR_INTEGRITY, assertThrows(ProtocolException.class, () -> jobs.find(key(SESSION, ROOT, 0))).errorCode());
      sql("DELETE FROM ps_java_jobs");
      assertEquals(Wire.ERROR_INTEGRITY, assertThrows(ProtocolException.class, jobs::audit).errorCode());
      assertThrows(ProtocolException.class, () -> sessions.processed(SESSION, PRODUCER, ROOT, 3, new byte[32]));
      assertEquals(Wire.ERROR_INTEGRITY, assertThrows(ProtocolException.class, () -> sessions.checkpointReady(SESSION, PRODUCER, 0, 1)).errorCode());
    }
  }

  @Test void terminalMetadataCannotSpendTheWaitingParentsReservedBytesAcrossReopen() throws Exception {
    var sessions = sessions();
    var all = LongStream.rangeClosed(1, 270).boxed().toList();
    sessions.declare(new SealedWork.Declaration(SESSION, PRODUCER, 0, null, BigInteger.ZERO,
        all.subList(0, 256), 0, null), 7, 1024);
    sessions.declare(new SealedWork.Declaration(SESSION, PRODUCER, 0, null, BigInteger.ONE,
        all.subList(256, all.size()), SealedWork.SEAL, SealedWork.sealDigest(SESSION, PRODUCER, 0, null, all)), 7, 1024);
    var jobs = new SealedJobs(sessions); int completed = 0; boolean refused = false;
    try (var payloads = payloads()) {
      process(jobs, input(payloads, SESSION, ROOT, null, Map.of("application-data", "x".repeat(65_000))), SealedJobs.Outcome.dehydrate());
      declare(sessions, SESSION, 7, ROOT, List.of(1L));
      var child = input(payloads, SESSION, new SealedWork.EntityKey(7, 1), ROOT);
      process(jobs, child, SealedJobs.Outcome.complete(child.digest()));
      var initial = sessions.jobUsage();
      long allocated = initial.retainedJobBytes() + initial.rehydrationReservedBytes();
      long smallestPair = Long.MAX_VALUE;
      for (long id : all.subList(1, all.size())) {
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
          assertTrue(completed > 0);
          assertTrue(allocated <= 16L << 20);
          assertTrue(allocated + smallestPair > 16L << 20, "refusal must occur only when even the smallest tested pair no longer fits");
          assertEquals(allocated, scalar("SELECT sum(length(input)+length(image)) FROM ps_java_jobs"));
          assertEquals(2L * (completed + 2), scalar("SELECT count(*) FROM ps_java_jobs"));
          assertTrue(jobs.ready(Long.MAX_VALUE, SealedJobs.MAX_QUEUED).isEmpty());
          assertEquals(1, scalar("SELECT count(*) FROM ps_java_entities WHERE id=" + id + " AND substr(image,9,8)=zeroblob(8)"));
          assertEquals(Wire.ERROR_LIMIT_EXCEEDED, assertThrows(ProtocolException.class, () -> new SealedJobs(sessions()).admit(input)).errorCode());
          refused = true; break;
        }
        long charge = scalar("SELECT sum(length(input)+length(image)) FROM ps_java_jobs WHERE scope=0 AND entity=" + id);
        assertTrue(charge > 130_000); smallestPair = Math.min(smallestPair, charge); allocated += charge;
        jobs.publish(jobs.acquire(new SealedJobs.Key(identity, 0), WORKER, 1, 100), 2, SealedJobs.Outcome.complete(input.digest()));
        completed++;
      }
      assertTrue(refused, "retained terminal metadata must have a finite budget");
      var before = sessions.jobUsage();
      assertEquals(1, before.waitingParents()); assertEquals(1, before.reservedRehydrationSlots());
      assertTrue(before.rehydrationReservedBytes() > 65_000);
      assertEquals(before, sessions().jobUsage());
      var reopened = new SealedJobs(sessions());
      reopened.closeScope(SESSION, PRODUCER, 7).orElseThrow();
      var after = sessions.jobUsage();
      assertEquals(0, after.rehydrationReservedBytes());
      assertEquals(before.retainedJobBytes() + before.rehydrationReservedBytes(), after.retainedJobBytes());
      reopened.publish(reopened.acquire(key(SESSION, ROOT, 1), WORKER, 1, 100), 2, SealedJobs.Outcome.complete(new byte[32]));
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

  @Test void versionThreeCannotBeReopenedWithStrongerReservationSemantics() throws Exception {
    sessions(); sql("UPDATE ps_java_meta SET version=3");
    byte[] before = Files.readAllBytes(directory.resolve("sessions.sqlite3"));
    assertThrows(SQLException.class, this::sessions);
    assertEquals(3, scalar("SELECT version FROM ps_java_meta"));
    assertArrayEquals(before, Files.readAllBytes(directory.resolve("sessions.sqlite3")));
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
    assertEquals(1, sessions().jobUsage().reservedRehydrationSlots());
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

  @Test void abruptExitWhileWaitingRetainsTheParentsBytesAndCompletionSlot() throws Exception {
    Path log = directory.resolve("waiting-child.log");
    Process process = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin/java").toString(),
        "--enable-native-access=ALL-UNNAMED", "-cp", System.getProperty("java.class.path"),
        SealedJobsTest.class.getName(), directory.toString(), "waiting")
        .redirectErrorStream(true).redirectOutput(log.toFile()).start();
    try {
      assertTrue(process.waitFor(30, TimeUnit.SECONDS)); assertEquals(37, process.exitValue(), () -> read(log));
    } finally { if (process.isAlive()) process.destroyForcibly().waitFor(5, TimeUnit.SECONDS); }
    var sessions = sessions(); var jobs = new SealedJobs(sessions);
    var before = sessions.jobUsage();
    assertEquals(1, before.waitingParents()); assertEquals(1, before.reservedRehydrationSlots());
    assertEquals(0, before.processingJobs()); assertEquals(0, before.rehydrationJobs());
    assertTrue(before.rehydrationReservedBytes() > 0);
    jobs.closeScope(SESSION, PRODUCER, 7).orElseThrow();
    assertEquals(before.retainedJobBytes() + before.rehydrationReservedBytes(), sessions.jobUsage().retainedJobBytes());
    jobs.publish(jobs.acquire(key(SESSION, ROOT, 1), WORKER, 1, 100), 2, SealedJobs.Outcome.complete(new byte[32]));
    assertTrue(sessions.checkpointReady(SESSION, PRODUCER, 0, 1));
    assertEquals(0, sessions.jobUsage().reservedRehydrationSlots());
  }

  public static void main(String[] args) throws Exception {
    var test = new SealedJobsTest(); test.directory = Path.of(args[0]);
    var sessions = test.sessions(); declare(sessions, SESSION, 0, null, List.of(1L));
    var jobs = new SealedJobs(sessions);
    if (args.length == 2 && args[1].equals("waiting")) {
      try (var payloads = test.payloads()) {
        process(jobs, input(payloads, SESSION, ROOT, null), SealedJobs.Outcome.dehydrate());
        declare(sessions, SESSION, 7, ROOT, List.of(1L));
        var child = input(payloads, SESSION, new SealedWork.EntityKey(7, 1), ROOT);
        process(jobs, child, SealedJobs.Outcome.complete(child.digest()));
        Runtime.getRuntime().halt(37);
      }
    }
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
    return input(payloads, session, key, parent, Map.of());
  }
  private static SealedPayloadStore.Stored input(SealedPayloadStore payloads, String session, SealedWork.EntityKey key, SealedWork.EntityKey parent, Map<String, String> metadata) throws Exception {
    var identity = new SealedPayloadStore.Identity(session, PRODUCER, key);
    var header = new SealedTransport.Header(key, parent, 0, null, BigInteger.ONE, null, metadata, null);
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
