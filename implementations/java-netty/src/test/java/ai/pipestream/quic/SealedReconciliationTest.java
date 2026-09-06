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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

final class SealedReconciliationTest {
  private static final UUID PRODUCER = new UUID(1, 2), WORKER = new UUID(3, 4);
  private static final SealedPayloadStore.Limits LIMITS = SealedPayloadStore.Limits.defaults();
  @TempDir Path directory;

  @Test void reclaimsAbandonedNamesAndBodiesButRetainsAllAdmittedStates() throws Exception {
    var sessions = sessions(); declare(sessions, List.of(1L, 2L, 3L, 4L));
    var jobs = new SealedJobs(sessions); Map<Path, byte[]> admitted = new HashMap<>();
    Path orphan; byte[] original;
    try (var payloads = payloads()) {
      payloads.bind(sessions);
      for (long id : List.of(1L, 2L, 3L)) {
        var stored = install(payloads, id, body(id), null); jobs.admit(stored);
        if (id != 2) jobs.publish(jobs.acquire(key(id), WORKER, 1, 100), 2,
            id == 1 ? SealedJobs.Outcome.complete(stored.digest()) : SealedJobs.Outcome.refused(Wire.ERROR_INTEGRITY));
        Path path = object(id); admitted.put(path, Files.readAllBytes(path));
      }
      install(payloads, 4, body(4), null); orphan = object(4); original = Files.readAllBytes(orphan);
    }
    var state = sessions.jobUsage();
    Path spool = root().resolve("spool/" + UUID.randomUUID() + ".tmp"); Files.write(spool, new byte[117]);
    Path partial = root().resolve("objects/install-" + UUID.randomUUID() + ".tmp"); Files.write(partial, new byte[73]);
    Path linked = root().resolve("objects/install-" + UUID.randomUUID() + ".tmp"); Files.createLink(linked, object(2));
    SealedPayloadStore.Usage before;
    try (var payloads = payloads()) { before = payloads.usage(); }
    var result = SealedPayloadStore.reconcile(root(), LIMITS, sessions);
    assertEquals(3, result.admittedPayloads()); assertEquals(1, result.temporaryFilesRemoved());
    assertEquals(2, result.stagingFilesRemoved()); assertEquals(1, result.payloadsReclaimed());
    assertEquals(1, result.commitmentsRetained()); assertEquals(117, result.temporaryBytesReleased());
    assertFalse(Files.exists(orphan)); assertFalse(Files.exists(spool)); assertFalse(Files.exists(partial)); assertFalse(Files.exists(linked));
    assertArrayEquals(Arrays.copyOf(original, prefix(original)), Files.readAllBytes(reference(4)));
    for (var entry : admitted.entrySet()) assertArrayEquals(entry.getValue(), Files.readAllBytes(entry.getKey()));
    assertEquals(state, sessions.jobUsage());
    assertEquals(SealedJobs.FINISHED, jobs.find(key(1)).orElseThrow().state());
    assertEquals(SealedJobs.QUEUED, jobs.find(key(2)).orElseThrow().state());
    assertEquals(SealedJobs.REFUSED, jobs.find(key(3)).orElseThrow().state());
    assertTrue(jobs.find(key(4)).isEmpty()); assertFalse(sessions.checkpointReady("reclaim", PRODUCER, 0, 4));
    try (var payloads = payloads()) {
      assertTrue(payloads.find(identity(4)).isEmpty());
      assertEquals(before.retainedBytes() - payloads.usage().retainedBytes(), result.retainedBytesReleased());
    }
    var again = SealedPayloadStore.reconcile(root(), LIMITS, sessions());
    assertEquals(0, again.payloadsReclaimed()); assertEquals(0, again.retainedBytesReleased());
    assertEquals(1, again.commitmentsRetained()); assertEquals(state, sessions().jobUsage());
  }

  @Test void fullQuotaReclamationPreservesIdentityAndPermitsMatchingRestoration() throws Exception {
    var sessions = sessions(); declare(sessions, List.of(1L));
    long bytes;
    try (var sizing = SealedPayloadStore.open(directory.resolve("sizing"), LIMITS)) {
      install(sizing, 1, body(1), null); bytes = sizing.usage().retainedBytes();
    }
    var limits = new SealedPayloadStore.Limits(8192, 2, bytes * 2, 2, 4096, 2);
    try (var payloads = SealedPayloadStore.open(root(), limits)) { payloads.bind(sessions); install(payloads, 1, body(1), null); }
    Files.createLink(root().resolve("objects/install-" + UUID.randomUUID() + ".tmp"), object(1));
    try (var payloads = SealedPayloadStore.open(root(), limits)) {
      assertEquals(limits.retainedBytes(), payloads.usage().retainedBytes()); assertEquals(2, payloads.usage().retainedFiles());
    }
    var reclaimed = SealedPayloadStore.reconcile(root(), limits, sessions);
    assertEquals(1, reclaimed.payloadsReclaimed()); assertEquals(bytes + 4096, reclaimed.retainedBytesReleased());
    byte[] commitment = Files.readAllBytes(reference(1));
    try (var payloads = SealedPayloadStore.open(root(), limits)) {
      long retained = payloads.usage().retainedBytes(); byte[] changed = body(1); changed[0] ^= 1;
      assertEquals(Wire.ERROR_ENTITY_INVALID, assertThrows(ProtocolException.class, () -> install(payloads, 1, changed, null)).errorCode());
      assertEquals(Wire.ERROR_ENTITY_INVALID, assertThrows(ProtocolException.class, () -> install(payloads, 1, body(1), "changed/type")).errorCode());
      assertArrayEquals(commitment, Files.readAllBytes(reference(1))); assertFalse(Files.exists(object(1)));
      assertEquals(retained, payloads.usage().retainedBytes());
      var restored = install(payloads, 1, body(1), null);
      assertEquals(2, payloads.usage().retainedFiles());
      assertTrue(payloads.usage().retainedBytes() <= limits.retainedBytes());
      new SealedJobs(sessions).admit(restored);
    }
    var result = SealedPayloadStore.reconcile(root(), limits, sessions());
    assertEquals(1, result.admittedPayloads()); assertEquals(0, result.payloadsReclaimed());
    assertEquals(0, result.commitmentsRetained()); assertEquals(commitment.length, result.retainedBytesReleased());
    try (var payloads = SealedPayloadStore.open(root(), limits)) {
      try (var input = payloads.find(identity(1)).orElseThrow().openStream()) { assertArrayEquals(body(1), input.readAllBytes()); }
    }
  }

  @Test void corruptionOrMissingAdmittedInputRefusesBeforeAnyCleanup() throws Exception {
    for (String fault : List.of("missing", "body", "orphan-body", "unknown", "symlink", "commitment", "job")) {
      directory = Files.createDirectory(directory.resolve(fault));
      var sessions = sessions(); declare(sessions, List.of(1L, 2L));
      try (var payloads = payloads()) {
        new SealedJobs(sessions).admit(install(payloads, 1, body(1), null));
        install(payloads, 2, body(2), null);
      }
      Path spool = root().resolve("spool/" + UUID.randomUUID() + ".tmp"); Files.write(spool, new byte[] {7});
      switch (fault) {
        case "missing" -> Files.delete(object(1));
        case "body", "orphan-body" -> {
          try (var channel = FileChannel.open(object(fault.equals("body") ? 1 : 2), StandardOpenOption.WRITE)) {
            channel.position(channel.size() - 1); channel.write(ByteBuffer.wrap(new byte[] {99}));
          }
        }
        case "unknown" -> Files.write(root().resolve("objects/foreign"), new byte[] {3});
        case "symlink" -> { Files.delete(object(1)); Files.createSymbolicLink(object(1), object(2)); }
        case "commitment" -> {
          byte[] original = Files.readAllBytes(object(2)); byte[] reference = Arrays.copyOf(original, prefix(original));
          reference[20] ^= 1; Files.write(reference(2), reference);
        }
        case "job" -> {
          try (var connection = DriverManager.getConnection("jdbc:sqlite:" + directory.resolve("sessions.sqlite"));
              var statement = connection.createStatement()) {
            assertEquals(1, statement.executeUpdate("UPDATE ps_java_jobs SET image=zeroblob(256) WHERE kind=0"));
          }
        }
        default -> throw new AssertionError(fault);
      }
      Map<String, byte[]> before = files();
      assertEquals(Wire.ERROR_INTEGRITY, assertThrows(ProtocolException.class, () -> SealedPayloadStore.reconcile(root(), LIMITS, sessions)).errorCode());
      assertFiles(before, files()); assertArrayEquals(new byte[] {7}, Files.readAllBytes(spool));
      if (fault.equals("symlink")) assertTrue(Files.isSymbolicLink(object(1)));
      if (fault.equals("job")) assertThrows(ProtocolException.class, () -> sessions.checkpointReady("reclaim", PRODUCER, 0, 2));
      else assertFalse(sessions.checkpointReady("reclaim", PRODUCER, 0, 2));
      directory = directory.getParent();
    }
  }

  @Test void chunkedRestorationBindsTheAssembledPayloadAcrossChunkBoundaries() throws Exception {
    var sessions = sessions(); declare(sessions, List.of(1L));
    byte[] original;
    try (var payloads = payloads()) {
      payloads.bind(sessions); installChunks(payloads, body(1), 17);
      original = Files.readAllBytes(object(1));
    }
    SealedPayloadStore.reconcile(root(), LIMITS, sessions);
    try (var payloads = payloads()) {
      byte[] changed = body(1); changed[2000] ^= 1;
      assertEquals(Wire.ERROR_ENTITY_INVALID, assertThrows(ProtocolException.class,
          () -> installChunks(payloads, changed, 2048)).errorCode());
      assertFalse(Files.exists(object(1)));
      var restored = installChunks(payloads, body(1), 2048);
      assertArrayEquals(original, Files.readAllBytes(object(1)));
      new SealedJobs(sessions).admit(restored);
    }
    assertEquals(1, SealedPayloadStore.reconcile(root(), LIMITS, sessions).admittedPayloads());
  }

  @Test void concurrentRestorationPublishesOneObjectWithoutLeakingCredits() throws Exception {
    var sessions = sessions(); declare(sessions, List.of(1L));
    long objectBytes;
    try (var sizing = SealedPayloadStore.open(directory.resolve("sizing"), LIMITS)) {
      install(sizing, 1, body(1), null); objectBytes = sizing.usage().retainedBytes();
    }
    var limits = new SealedPayloadStore.Limits(8192, 2, objectBytes * 5, 5, 4096, 2);
    try (var payloads = SealedPayloadStore.open(root(), limits)) {
      payloads.bind(sessions); install(payloads, 1, body(1), null);
    }
    SealedPayloadStore.reconcile(root(), limits, sessions);
    long commitmentBytes = Files.size(reference(1));
    try (var payloads = SealedPayloadStore.open(root(), limits); var threads = Executors.newFixedThreadPool(4)) {
      var header = new SealedTransport.Header(identity(1).entity(), null, 0, null, BigInteger.valueOf(4096), null, Map.of(), null);
      try (var receiver = payloads.begin(identity(1), header)) {
        receiver.write(body(1), 0, 4096);
        try (var receipt = receiver.finish()) {
          var ready = new CountDownLatch(4); var start = new CountDownLatch(1);
          var attempts = new ArrayList<java.util.concurrent.Future<SealedPayloadStore.Stored>>();
          for (int i = 0; i < 4; i++) attempts.add(threads.submit(() -> {
            ready.countDown();
            if (!start.await(5, TimeUnit.SECONDS)) throw new IOException("restoration start timeout");
            return payloads.install(List.of(receipt));
          }));
          try { assertTrue(ready.await(5, TimeUnit.SECONDS)); }
          finally { start.countDown(); }
          for (var attempt : attempts) assertArrayEquals(SealedWork.sha256().digest(body(1)), attempt.get(5, TimeUnit.SECONDS).digest());
        }
      }
      assertEquals(2, payloads.usage().retainedFiles());
      assertEquals(objectBytes + commitmentBytes, payloads.usage().retainedBytes());
      assertEquals(0, payloads.usage().temporaryBytes()); assertEquals(0, payloads.usage().activeHandles());
    }
    try (var payloads = SealedPayloadStore.open(root(), limits)) {
      assertEquals(objectBytes + commitmentBytes, payloads.usage().retainedBytes());
      assertEquals(2, payloads.usage().retainedFiles());
      new SealedJobs(sessions).admit(payloads.find(identity(1)).orElseThrow());
    }
  }

  @Test void olderPayloadPolicyRefusesWithoutRewritingOrRemovingFiles() throws Exception {
    var sessions = sessions(); declare(sessions, List.of(1L));
    try (var payloads = payloads()) { payloads.bind(sessions); install(payloads, 1, body(1), null); }
    Path policy = root().resolve("policy.cbor");
    var fields = new HashMap<>(SealedCbor.decode(Files.readAllBytes(policy), 4096));
    fields.put("format", "pipestream-java-payload-v2");
    Files.write(policy, SealedCbor.encode(fields, 4096));
    var before = files();
    assertEquals(Wire.ERROR_INTEGRITY, assertThrows(ProtocolException.class,
        () -> SealedPayloadStore.reconcile(root(), LIMITS, sessions)).errorCode());
    assertEquals(Wire.ERROR_INTEGRITY, assertThrows(ProtocolException.class, this::payloads).errorCode());
    assertFiles(before, files());
  }

  @Test void filesystemFailureLeavesResumableCommitmentsAndReleasesOwnership() throws Exception {
    var sessions = sessions(); declare(sessions, List.of(1L));
    try (var payloads = payloads()) { payloads.bind(sessions); install(payloads, 1, body(1), null); }
    var expected = new IOException("injected after durable commitment publication");
    assertSame(expected, assertThrows(IOException.class, () -> SealedPayloadStore.reconcile(root(), LIMITS, sessions, phase -> {
      if (phase == SealedPayloadStore.ReconcilePhase.COMMITMENTS_PUBLISHED) throw expected;
    })));
    assertFalse(Files.exists(object(1))); assertTrue(Files.size(reference(1)) > 4096);
    assertEquals(0, sessions.jobUsage().processingJobs());
    assertFalse(sessions.checkpointReady("reclaim", PRODUCER, 0, 1));
    var resumed = SealedPayloadStore.reconcile(root(), LIMITS, sessions());
    assertEquals(4096, resumed.retainedBytesReleased()); assertEquals(1, resumed.commitmentsRetained());
    try (var payloads = payloads()) { new SealedJobs(sessions).admit(install(payloads, 1, body(1), null)); }
  }

  @Test void wrongUnboundAndCallerManagedStoresAreNotGuessed() throws Exception {
    var sessions = sessions(); declare(sessions, List.of(1L));
    Path absent = directory.resolve("absent");
    assertThrows(ProtocolException.class, () -> SealedPayloadStore.reconcile(absent, LIMITS, sessions));
    assertFalse(Files.exists(absent));
    try (var payloads = payloads()) { install(payloads, 1, body(1), null); }
    assertEquals(Wire.ERROR_ENTITY_INVALID, assertThrows(ProtocolException.class,
        () -> SealedPayloadStore.reconcile(root(), LIMITS, sessions)).errorCode());
    try (var payloads = payloads()) { payloads.bind(sessions); }
    var other = SealedSessionStore.open(directory.resolve("other.sqlite"));
    var before = files();
    assertThrows(ProtocolException.class, () -> SealedPayloadStore.reconcile(root(), LIMITS, other));
    assertFiles(before, files());
    sessions.admit("reclaim", PRODUCER, identity(1).entity(), null, SealedWork.sha256().digest(body(1)));
    assertEquals(Wire.ERROR_ENTITY_INVALID, assertThrows(ProtocolException.class,
        () -> SealedPayloadStore.reconcile(root(), LIMITS, sessions)).errorCode());
    assertFiles(before, files());
  }

  @Test void openHandlesExcludeMaintenanceAndMaintenanceHoldsTheDatabaseWriter() throws Exception {
    var sessions = sessions(); declare(sessions, List.of(1L));
    try (var payloads = payloads()) {
      payloads.bind(sessions); install(payloads, 1, body(1), null);
      assertThrows(IOException.class, () -> SealedPayloadStore.reconcile(root(), LIMITS, sessions));
    }
    var audited = new CountDownLatch(1); var release = new CountDownLatch(1);
    try (var threads = Executors.newFixedThreadPool(2)) {
      var maintenance = threads.submit(() -> SealedPayloadStore.reconcile(root(), LIMITS, sessions, phase -> {
        if (phase == SealedPayloadStore.ReconcilePhase.AUDITED) {
          audited.countDown();
          try { if (!release.await(5, TimeUnit.SECONDS)) throw new IOException("test release timeout"); }
          catch (InterruptedException failure) { Thread.currentThread().interrupt(); throw new IOException(failure); }
        }
      }));
      try {
        assertTrue(audited.await(5, TimeUnit.SECONDS)); assertThrows(IOException.class, this::payloads);
        assertTrue(Files.exists(object(1))); assertFalse(Files.exists(reference(1)));
        var entered = new CountDownLatch(1);
        var declaration = threads.submit(() -> {
          entered.countDown();
          return sessions.declare(new SealedWork.Declaration("independent", PRODUCER, 0, null, BigInteger.ZERO,
              List.of(1L), 0, null), 7, 1024);
        });
        assertTrue(entered.await(5, TimeUnit.SECONDS));
        assertThrows(java.util.concurrent.TimeoutException.class, () -> declaration.get(100, TimeUnit.MILLISECONDS));
        release.countDown(); assertEquals(1, maintenance.get(5, TimeUnit.SECONDS).payloadsReclaimed());
        assertNotNull(declaration.get(5, TimeUnit.SECONDS));
      } finally { release.countDown(); }
    }
  }

  @Test void abruptExitAtEveryFilesystemBoundaryRetainsReplayAndPendingWork() throws Exception {
    Path parent = directory;
    for (var phase : SealedPayloadStore.ReconcilePhase.values()) {
      directory = Files.createDirectory(parent.resolve(phase.name()));
      runChild(phase.name(), "48m", 73);
      var sessions = sessions(); assertEquals(0, sessions.jobUsage().processingJobs());
      assertFalse(sessions.checkpointReady("reclaim", PRODUCER, 0, 1));
      var result = SealedPayloadStore.reconcile(root(), LIMITS, sessions);
      assertEquals(1, result.commitmentsRetained()); assertFalse(Files.exists(object(1)));
      try (var payloads = payloads()) {
        assertTrue(payloads.find(identity(1)).isEmpty());
        byte[] changed = body(1); changed[0] ^= 1;
        assertEquals(Wire.ERROR_ENTITY_INVALID, assertThrows(ProtocolException.class, () -> install(payloads, 1, changed, null)).errorCode());
        new SealedJobs(sessions).admit(install(payloads, 1, body(1), null));
      }
      assertEquals(1, sessions().jobUsage().processingJobs());
    }
    directory = parent;
  }

  @Test void largeBodyReclaimsAndRestoresWithAHeapSmallerThanThePayload() throws Exception {
    runChild("large", "24m", 0);
  }

  private void runChild(String mode, String heap, int expectedExit) throws Exception {
    Path log = directory.resolve("child.log");
    var child = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin/java").toString(), "-Xmx" + heap,
        "--enable-native-access=ALL-UNNAMED", "-cp", System.getProperty("java.class.path"),
        SealedReconciliationTest.class.getName(), directory.toString(), mode)
        .redirectErrorStream(true).redirectOutput(log.toFile()).start();
    try {
      assertTrue(child.waitFor(30, TimeUnit.SECONDS));
      assertEquals(expectedExit, child.exitValue(), () -> { try { return Files.readString(log); } catch (IOException failure) { return failure.toString(); } });
    } finally { if (child.isAlive()) child.destroyForcibly().waitFor(5, TimeUnit.SECONDS); }
  }

  public static void main(String[] arguments) throws Exception {
    var test = new SealedReconciliationTest(); test.directory = Path.of(arguments[0]);
    var sessions = test.sessions(); declare(sessions, List.of(1L));
    if (arguments[1].equals("large")) {
      int length = 32 << 20;
      try (var payloads = test.payloads()) { payloads.bind(sessions); installLarge(payloads, length); }
      var result = SealedPayloadStore.reconcile(test.root(), LIMITS, sessions);
      if (result.retainedBytesReleased() != length || result.commitmentsRetained() != 1) throw new AssertionError(result);
      try (var payloads = test.payloads()) {
        var restored = installLarge(payloads, length);
        new SealedJobs(sessions).admit(restored);
        byte[] block = new byte[8192]; long count = 0;
        try (var input = restored.openStream()) { int read; while ((read = input.read(block)) != -1) count += read; }
        if (count != length) throw new AssertionError("restored length differs");
      }
      return;
    }
    try (var payloads = test.payloads()) { payloads.bind(sessions); install(payloads, 1, body(1), null); }
    Files.write(test.root().resolve("spool/" + UUID.randomUUID() + ".tmp"), new byte[31]);
    var target = SealedPayloadStore.ReconcilePhase.valueOf(arguments[1]);
    SealedPayloadStore.reconcile(test.root(), LIMITS, sessions, phase -> {
      if (phase == target) Runtime.getRuntime().halt(73);
    });
    throw new AssertionError("crash boundary did not run");
  }

  private static SealedPayloadStore.Stored installLarge(SealedPayloadStore payloads, int length) throws Exception {
    var header = new SealedTransport.Header(identity(1).entity(), null, 0, null, BigInteger.valueOf(length), null, Map.of(), null);
    try (var receiver = payloads.begin(identity(1), header)) {
      byte[] block = new byte[8192]; Arrays.fill(block, (byte) 42);
      for (int offset = 0; offset < length; offset += block.length) receiver.write(block, 0, block.length);
      try (var receipt = receiver.finish()) { return payloads.install(List.of(receipt)); }
    }
  }
  private static SealedPayloadStore.Stored install(SealedPayloadStore payloads, long id, byte[] bytes, String contentType) throws Exception {
    var header = new SealedTransport.Header(identity(id).entity(), null, 0, contentType, BigInteger.valueOf(bytes.length), null, Map.of(), null);
    try (var receiver = payloads.begin(identity(id), header)) {
      receiver.write(bytes, 0, bytes.length);
      try (var receipt = receiver.finish()) { return payloads.install(List.of(receipt)); }
    }
  }
  private static SealedPayloadStore.Stored installChunks(SealedPayloadStore payloads, byte[] bytes, int split) throws Exception {
    try (var first = receiveChunk(payloads, Arrays.copyOfRange(bytes, 0, split), 0, 0);
        var last = receiveChunk(payloads, Arrays.copyOfRange(bytes, split, bytes.length), 1, split)) {
      return payloads.install(List.of(last, first));
    }
  }
  private static SealedPayloadStore.Received receiveChunk(SealedPayloadStore payloads, byte[] bytes, int index, int offset) throws Exception {
    var chunk = new SealedTransport.Chunk(BigInteger.TWO, BigInteger.valueOf(index), BigInteger.valueOf(offset));
    var header = new SealedTransport.Header(identity(1).entity(), null, 0, null, BigInteger.valueOf(bytes.length),
        SealedWork.sha256().digest(bytes), Map.of(), chunk);
    try (var receiver = payloads.begin(identity(1), header)) {
      receiver.write(bytes, 0, bytes.length); return receiver.finish();
    }
  }
  private static byte[] body(long id) { byte[] bytes = new byte[4096]; Arrays.fill(bytes, (byte) id); return bytes; }
  private static int prefix(byte[] object) { return 44 + ByteBuffer.wrap(object, 8, 4).getInt(); }
  private SealedSessionStore sessions() throws Exception { return SealedSessionStore.open(directory.resolve("sessions.sqlite")); }
  private SealedPayloadStore payloads() throws Exception { return SealedPayloadStore.open(root(), LIMITS); }
  private Path root() { return directory.resolve("payloads"); }
  private static SealedPayloadStore.Identity identity(long id) { return new SealedPayloadStore.Identity("reclaim", PRODUCER, new SealedWork.EntityKey(0, id)); }
  private static SealedJobs.Key key(long id) { return new SealedJobs.Key(identity(id), SealedJobs.PROCESS); }
  private Path object(long id) throws Exception {
    byte[] bytes = SealedCbor.encode(Map.of("session", "reclaim", "producer", SealedWork.producerBytes(PRODUCER), "scope", 0L, "entity", id), 1024);
    return root().resolve("objects/" + java.util.HexFormat.of().formatHex(SealedWork.sha256().digest(bytes)) + ".pay");
  }
  private Path reference(long id) throws Exception { String name = object(id).getFileName().toString(); return root().resolve("objects/" + name.substring(0, name.length() - 4) + ".commit"); }
  private static void declare(SealedSessionStore sessions, List<Long> ids) throws Exception {
    sessions.declare(new SealedWork.Declaration("reclaim", PRODUCER, 0, null, BigInteger.ZERO, ids, SealedWork.SEAL,
        SealedWork.sealDigest("reclaim", PRODUCER, 0, null, ids)), 7, 1024);
  }
  private Map<String, byte[]> files() throws Exception {
    Map<String, byte[]> result = new HashMap<>();
    try (var paths = Files.walk(root())) {
      for (Path path : paths.filter(Files::isRegularFile).toList()) result.put(root().relativize(path).toString(), Files.readAllBytes(path));
    }
    return result;
  }
  private static void assertFiles(Map<String, byte[]> expected, Map<String, byte[]> actual) {
    assertEquals(expected.keySet(), actual.keySet());
    expected.forEach((name, bytes) -> assertArrayEquals(bytes, actual.get(name), name));
  }
}
