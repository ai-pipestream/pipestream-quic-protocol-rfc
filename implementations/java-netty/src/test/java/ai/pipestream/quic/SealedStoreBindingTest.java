package ai.pipestream.quic;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

final class SealedStoreBindingTest {
  private static final UUID PRODUCER = new UUID(1, 2);
  private static final SealedWork.EntityKey ENTITY = new SealedWork.EntityKey(0, 1);
  private static final SealedPayloadStore.Identity IDENTITY = new SealedPayloadStore.Identity("binding", PRODUCER, ENTITY);
  @TempDir Path directory;

  @Test void managedAdmissionPersistsBothIdentitiesAcrossReopen() throws Exception {
    var sessions = sessions("sessions"); declare(sessions);
    var initial = sessions.binding();
    assertEquals(SealedStoreBinding.UNBOUND, initial.payloads());
    SealedStoreBinding paired;
    try (var payloads = payloads("payloads")) {
      new SealedJobs(sessions).admit(input(payloads));
      paired = sessions.binding();
      assertEquals(initial.database(), paired.database());
      assertNotEquals(SealedStoreBinding.UNBOUND, paired.payloads());
      assertArrayEquals(paired.encode(), Files.readAllBytes(directory.resolve("payloads/session-store.bin")));
    }
    var reopened = sessions("sessions");
    try (var payloads = payloads("payloads")) {
      payloads.bind(reopened);
      assertEquals(paired, reopened.binding());
      var job = new SealedJobs(reopened).find(new SealedJobs.Key(IDENTITY, SealedJobs.PROCESS)).orElseThrow();
      assertEquals(SealedJobs.QUEUED, job.state());
      assertArrayEquals(payloads.find(IDENTITY).orElseThrow().digest(), job.input().digest());
      assertFalse(reopened.checkpointReady("binding", PRODUCER, 0, 1));
    }
  }

  @Test void pairingRefusesWrongDatabaseAndWrongPayloadRoot() throws Exception {
    var first = sessions("first"); var second = sessions("second");
    try (var payloads = payloads("payloads"); var other = payloads("other")) {
      payloads.bind(first);
      var before = first.binding();
      assertIntegrity(() -> payloads.bind(second));
      assertIntegrity(() -> other.bind(first));
      assertEquals(before, first.binding());
      assertEquals(SealedStoreBinding.UNBOUND, second.binding().payloads());
      assertFalse(Files.exists(directory.resolve("other/session-store.bin")));
    }
  }

  @Test void executorRefusesWrongPairBeforeDispatchAndReleasesStartupOwnership() throws Exception {
    var sessions = sessions("sessions"); var calls = new AtomicInteger();
    try (var payloads = payloads("payloads"); var other = payloads("other")) {
      payloads.bind(sessions);
      SealedExecutor.Processor processor = (context, input) -> { calls.incrementAndGet(); return new SealedExecutor.Decision(Wire.STATUS_FAILED, null); };
      assertIntegrity(() -> SealedExecutor.start(sessions, other, processor, SealedExecutor.Limits.defaults()));
      var executor = SealedExecutor.start(sessions, payloads, processor, SealedExecutor.Limits.defaults());
      try { assertEquals(0, calls.get()); }
      finally { stop(executor); }
    }
  }

  @Test void executorRejectsForeignInputEvenWithIdenticalEntityIdentity() throws Exception {
    var sessions = sessions("sessions"); declare(sessions); var calls = new AtomicInteger();
    try (var payloads = payloads("payloads"); var other = payloads("other")) {
      var foreign = input(other);
      var executor = SealedExecutor.start(sessions, payloads, (context, input) -> {
        calls.incrementAndGet(); return new SealedExecutor.Decision(Wire.STATUS_FAILED, null);
      }, SealedExecutor.Limits.defaults());
      try {
        assertEquals(Wire.ERROR_ENTITY_INVALID, assertThrows(ProtocolException.class, () -> executor.admit(foreign)).errorCode());
        assertEquals(0, sessions.jobUsage().processingJobs());
        assertEquals(0, calls.get());
        assertFalse(Files.exists(directory.resolve("other/session-store.bin")));
        assertFalse(sessions.checkpointReady("binding", PRODUCER, 0, 1));
      } finally { stop(executor); }
    }
  }

  @Test void oldMetadataHandleCannotAdmitAfterItsStoreClosesOrReopens() throws Exception {
    var sessions = sessions("sessions"); declare(sessions); SealedPayloadStore.Stored stale;
    try (var payloads = payloads("payloads")) { stale = input(payloads); }
    var jobs = new SealedJobs(sessions);
    assertThrows(IOException.class, () -> jobs.admit(stale));
    try (var reopened = payloads("payloads")) {
      assertThrows(IOException.class, () -> jobs.admit(stale));
      assertEquals(0, sessions.jobUsage().processingJobs());
      assertEquals(SealedStoreBinding.UNBOUND, sessions.binding().payloads());
      jobs.admit(reopened.find(IDENTITY).orElseThrow());
      assertEquals(1, sessions.jobUsage().processingJobs());
    }
  }

  @Test void missingOrCorruptPayloadCannotBeAdmittedFromCachedMetadata() throws Exception {
    for (boolean missing : List.of(false, true)) {
      String suffix = missing ? "missing" : "corrupt";
      var sessions = sessions(suffix); declare(sessions);
      try (var payloads = payloads(suffix + "-payloads")) {
        var stored = input(payloads);
        Path object;
        try (var paths = Files.list(directory.resolve(suffix + "-payloads/objects"))) { object = paths.findFirst().orElseThrow(); }
        if (missing) Files.delete(object);
        else try (var channel = FileChannel.open(object, StandardOpenOption.WRITE)) {
          channel.position(channel.size() - 1); channel.write(ByteBuffer.wrap(new byte[] {99}));
        }
        assertIntegrity(() -> new SealedJobs(sessions).admit(stored));
        assertEquals(0, sessions.jobUsage().processingJobs());
        assertEquals(SealedStoreBinding.UNBOUND, sessions.binding().payloads());
        assertFalse(sessions.checkpointReady("binding", PRODUCER, 0, 1));
      }
    }
  }

  @Test void failedDatabaseClaimLeavesAReplayableFileClaimWithoutAdmission() throws Exception {
    var sessions = sessions("sessions"); declare(sessions);
    try (var payloads = payloads("payloads")) {
      var stored = input(payloads);
      sql("sessions", "CREATE INDEX block_binding_write ON ps_java_meta(length(binding))");
      assertThrows(SQLException.class, () -> new SealedJobs(sessions).admit(stored));
      assertEquals(SealedStoreBinding.UNBOUND, sessions.binding().payloads());
      assertEquals(0, sessions.jobUsage().processingJobs());
      var claim = SealedStoreBinding.decode(Files.readAllBytes(directory.resolve("payloads/session-store.bin")));
      assertEquals(sessions.binding().database(), claim.database());
      assertIntegrity(() -> payloads.bind(sessions("other")));
      sql("sessions", "DROP INDEX block_binding_write");
    }
    try (var payloads = payloads("payloads")) {
      new SealedJobs(sessions("sessions")).admit(payloads.find(IDENTITY).orElseThrow());
      assertEquals(1, sessions.jobUsage().processingJobs());
    }
  }

  @Test void twoRootsRacingForOneDatabaseCannotBothBind() throws Exception {
    var sessions = sessions("sessions"); var start = new CountDownLatch(1);
    try (var left = payloads("left"); var right = payloads("right"); var threads = Executors.newFixedThreadPool(2)) {
      var first = threads.submit(() -> compete(left, sessions, start));
      var second = threads.submit(() -> compete(right, sessions, start));
      start.countDown();
      boolean won = first.get(10, TimeUnit.SECONDS);
      assertNotEquals(won, second.get(10, TimeUnit.SECONDS));
      var winner = won ? left : right; var loser = won ? right : left;
      winner.bind(sessions);
      assertIntegrity(() -> loser.bind(sessions));
      assertEquals(0, sessions.jobUsage().processingJobs());
    }
  }

  @Test void admissionKeepsTheStoreOpenUntilItsDatabaseTransactionReturns() throws Exception {
    var sessions = sessions("sessions"); declare(sessions);
    try (var payloads = payloads("payloads"); var threads = Executors.newSingleThreadExecutor()) {
      payloads.bind(sessions); var stored = input(payloads);
      try (var connection = DriverManager.getConnection("jdbc:sqlite:" + directory.resolve("sessions.sqlite"));
          var statement = connection.createStatement()) {
        statement.execute("BEGIN IMMEDIATE");
        var admission = threads.submit(() -> { new SealedJobs(sessions).admit(stored); return null; });
        try {
          await(() -> payloads.usage().activeHandles() > 0);
          assertThrows(IOException.class, payloads::close);
          assertEquals(0, sessions.jobUsage().processingJobs());
        } finally { statement.execute("ROLLBACK"); }
        admission.get(10, TimeUnit.SECONDS);
        assertEquals(1, sessions.jobUsage().processingJobs());
        assertEquals(0, payloads.usage().activeHandles());
      }
    }
  }

  @Test void corruptFileBindingRefusesReopenAndReleasesTheFailedOpenLock() throws Exception {
    var sessions = sessions("sessions");
    try (var payloads = payloads("payloads")) { payloads.bind(sessions); }
    Path marker = directory.resolve("payloads/session-store.bin"); byte[] original = Files.readAllBytes(marker);
    byte[] corrupt = original.clone(); corrupt[20] ^= 1; Files.write(marker, corrupt);
    assertIntegrity(() -> payloads("payloads"));
    assertArrayEquals(corrupt, Files.readAllBytes(marker));
    Files.write(marker, original);
    try (var reopened = payloads("payloads")) { reopened.bind(sessions); }
  }

  @Test void missingClaimFromABoundPairIsNotSilentlyRecreated() throws Exception {
    var sessions = sessions("sessions");
    try (var payloads = payloads("payloads")) {
      payloads.bind(sessions); var before = sessions.binding();
      Path marker = directory.resolve("payloads/session-store.bin"); Files.delete(marker);
      assertIntegrity(() -> payloads.bind(sessions));
      assertEquals(before, sessions.binding());
      assertFalse(Files.exists(marker));
    }
  }

  @Test void invalidBindingImagesAreRejectedBeforeAnyJobMutation() throws Exception {
    var sessions = sessions("sessions"); declare(sessions);
    try (var payloads = payloads("payloads")) {
      var stored = input(payloads); payloads.bind(sessions);
      byte[] original = sessions.binding().encode();
      for (int length : List.of(0, 1, 40, 71, 73)) {
        assertIntegrity(() -> SealedStoreBinding.decode(new byte[length]));
      }
      byte[] corrupt = original.clone(); corrupt[40] ^= 1;
      try (var connection = DriverManager.getConnection("jdbc:sqlite:" + directory.resolve("sessions.sqlite"));
          var update = connection.prepareStatement("UPDATE ps_java_meta SET binding=?")) {
        update.setBytes(1, corrupt); update.executeUpdate();
      }
      assertThrows(SQLException.class, () -> new SealedJobs(sessions).admit(stored));
      try (var connection = DriverManager.getConnection("jdbc:sqlite:" + directory.resolve("sessions.sqlite"));
          var query = connection.createStatement(); var rows = query.executeQuery("SELECT count(*) FROM ps_java_jobs")) {
        assertTrue(rows.next()); assertEquals(0, rows.getInt(1));
      }
      byte[] zeroDatabase = original.clone(); java.util.Arrays.fill(zeroDatabase, 8, 24, (byte) 0);
      byte[] checksum = SealedWork.sha256().digest(java.util.Arrays.copyOf(zeroDatabase, 40));
      System.arraycopy(checksum, 0, zeroDatabase, 40, checksum.length);
      assertIntegrity(() -> SealedStoreBinding.decode(zeroDatabase));
    }
  }

  @Test void olderDatabaseAndPayloadPoliciesAreRefusedWithoutConversion() throws Exception {
    sessions("sessions"); sql("sessions", "UPDATE ps_java_meta SET version=5");
    assertThrows(SQLException.class, () -> sessions("sessions"));
    try (var payloads = payloads("payloads")) { assertEquals(0, payloads.usage().retainedFiles()); }
    Path policy = directory.resolve("payloads/policy.cbor");
    var fields = new java.util.HashMap<>(SealedCbor.decode(Files.readAllBytes(policy), 4096));
    fields.put("format", "pipestream-java-payload-v1"); fields.remove("store-id");
    byte[] old = SealedCbor.encode(fields, 4096); Files.write(policy, old);
    assertIntegrity(() -> payloads("payloads"));
    assertArrayEquals(old, Files.readAllBytes(policy));
  }

  @Test void abruptExitBetweenBindingClaimsCanResumeOnlyTheSamePair() throws Exception {
    Path log = directory.resolve("binding-child.log");
    var child = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin/java").toString(),
        "--enable-native-access=ALL-UNNAMED", "-cp", System.getProperty("java.class.path"),
        SealedStoreBindingTest.class.getName(), directory.toString())
        .redirectErrorStream(true).redirectOutput(log.toFile()).start();
    try {
      assertTrue(child.waitFor(30, TimeUnit.SECONDS));
      assertEquals(73, child.exitValue(), () -> {
        try { return Files.readString(log); } catch (IOException failure) { return failure.toString(); }
      });
    } finally { if (child.isAlive()) child.destroyForcibly().waitFor(5, TimeUnit.SECONDS); }
    var sessions = sessions("sessions");
    assertEquals(SealedStoreBinding.UNBOUND, sessions.binding().payloads());
    assertEquals(0, sessions.jobUsage().processingJobs());
    try (var payloads = payloads("payloads")) {
      assertIntegrity(() -> payloads.bind(sessions("other")));
      sql("sessions", "DROP INDEX block_binding_write");
      new SealedJobs(sessions).admit(payloads.find(IDENTITY).orElseThrow());
      assertEquals(1, sessions.jobUsage().processingJobs());
      assertFalse(sessions.checkpointReady("binding", PRODUCER, 0, 1));
    }
  }

  public static void main(String[] arguments) throws Exception {
    var test = new SealedStoreBindingTest(); test.directory = Path.of(arguments[0]);
    var sessions = test.sessions("sessions"); declare(sessions);
    var payloads = test.payloads("payloads"); var stored = input(payloads);
    test.sql("sessions", "CREATE INDEX block_binding_write ON ps_java_meta(length(binding))");
    try { new SealedJobs(sessions).admit(stored); }
    catch (SQLException expected) {
      if (Files.size(test.directory.resolve("payloads/session-store.bin")) != SealedStoreBinding.BYTES) {
        throw new AssertionError("file claim was not installed", expected);
      }
      Runtime.getRuntime().halt(73);
    }
    throw new AssertionError("binding fault did not refuse");
  }

  private static boolean compete(SealedPayloadStore payloads, SealedSessionStore sessions, CountDownLatch start) throws Exception {
    assertTrue(start.await(5, TimeUnit.SECONDS));
    try { payloads.bind(sessions); return true; }
    catch (ProtocolException failure) { assertEquals(Wire.ERROR_INTEGRITY, failure.errorCode()); return false; }
  }

  private SealedSessionStore sessions(String name) throws Exception { return SealedSessionStore.open(directory.resolve(name + ".sqlite")); }
  private SealedPayloadStore payloads(String name) throws Exception {
    return SealedPayloadStore.open(directory.resolve(name), SealedPayloadStore.Limits.defaults());
  }
  private void sql(String name, String command) throws Exception {
    try (var connection = DriverManager.getConnection("jdbc:sqlite:" + directory.resolve(name + ".sqlite")); var statement = connection.createStatement()) {
      statement.execute(command);
    }
  }
  private static void declare(SealedSessionStore sessions) throws Exception {
    sessions.declare(new SealedWork.Declaration("binding", PRODUCER, 0, null, BigInteger.ZERO, List.of(1L),
        SealedWork.SEAL, SealedWork.sealDigest("binding", PRODUCER, 0, null, List.of(1L))), 7, 1024);
  }
  private static SealedPayloadStore.Stored input(SealedPayloadStore payloads) throws Exception {
    var header = new SealedTransport.Header(ENTITY, null, 0, null, BigInteger.ONE, null, Map.of(), null);
    try (var receiver = payloads.begin(IDENTITY, header)) {
      receiver.write(new byte[] {42}, 0, 1);
      try (var receipt = receiver.finish()) { return payloads.install(List.of(receipt)); }
    }
  }
  private static void stop(SealedExecutor executor) throws Exception { executor.close(); await(executor::isTerminated); }
  private static void await(Check condition) throws Exception {
    long until = System.nanoTime() + Duration.ofSeconds(5).toNanos();
    while (!condition.get()) { assertTrue(System.nanoTime() < until, "operation did not progress"); Thread.sleep(5); }
  }
  private static void assertIntegrity(org.junit.jupiter.api.function.Executable action) {
    assertEquals(Wire.ERROR_INTEGRITY, assertThrows(ProtocolException.class, action).errorCode());
  }
  @FunctionalInterface private interface Check { boolean get() throws Exception; }
}
