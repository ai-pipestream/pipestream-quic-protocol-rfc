package ai.pipestream.quic;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

final class SealedPayloadStoreTest {
  private static final UUID PRODUCER = new UUID(1, 2);
  private static final SealedWork.EntityKey ENTITY = new SealedWork.EntityKey(0, 1);
  private static final SealedPayloadStore.Identity IDENTITY = new SealedPayloadStore.Identity("payload-test", PRODUCER, ENTITY);
  private static final SealedPayloadStore.Limits LIMITS = new SealedPayloadStore.Limits(1 << 20, 256, 4 << 20, 256, 1 << 20, 16);
  @TempDir Path directory;

  @Test void fileAndChunkInputsAreImmutableReopenableAndInstalledBeforeAdmission() throws Exception {
    Path root = directory.resolve("payloads"), database = directory.resolve("session.sqlite3");
    var sessions = declared(database);
    try (var store = SealedPayloadStore.open(root, LIMITS)) {
      try (var last = receive(store, IDENTITY, chunk(1, 3, "def"), "def");
          var first = receive(store, IDENTITY, chunk(0, 0, "abc"), "abc")) {
        var stored = store.install(List.of(last, first));
        assertArrayEquals(hash("abcdef"), stored.digest());
        assertEquals(6, stored.length()); assertNull(stored.header().chunk());
        assertEquals(BigInteger.valueOf(6), stored.header().payloadLength());
        assertFalse(sessions.checkpointReady(IDENTITY.session(), PRODUCER, 0, 1));
        try (var connection = DriverManager.getConnection("jdbc:sqlite:" + database);
            var query = connection.createStatement(); var rows = query.executeQuery("SELECT state FROM ps_java_entities")) {
          assertTrue(rows.next()); assertNull(rows.getObject(1));
        }
        sessions.admit(IDENTITY.session(), PRODUCER, ENTITY, null, stored.digest());
        assertFalse(sessions.checkpointReady(IDENTITY.session(), PRODUCER, 0, 1));
        try (var input = stored.openStream()) {
          assertArrayEquals(bytes("abcdef"), input.readAllBytes());
          assertThrows(IOException.class, store::close);
        }
      }
      assertEquals(0, store.usage().temporaryBytes()); assertEquals(0, store.usage().temporaryFiles());
      assertEquals(1, store.usage().retainedFiles()); assertTrue(store.usage().retainedBytes() > 6);
    }
    try (var reopened = SealedPayloadStore.open(root, LIMITS)) {
      var stored = reopened.find(IDENTITY).orElseThrow();
      try (var input = stored.openStream()) { assertArrayEquals(bytes("abcdef"), input.readAllBytes()); }
      assertEquals(1, reopened.usage().retainedFiles());
      assertEquals(0, reopened.usage().activeHandles());
    }
  }

  @Test void replayDoesNotConsumeCapacityOrOverwriteChangedBytesAndHeaders() throws Exception {
    Path root = directory.resolve("replay");
    try (var store = SealedPayloadStore.open(root, LIMITS)) {
      var header = header("one", null);
      try (var receipt = receive(store, IDENTITY, header, "one")) { store.install(List.of(receipt)); }
      var original = store.usage();
      try (var receipt = receive(store, IDENTITY, header, "one")) {
        assertArrayEquals(hash("one"), store.install(List.of(receipt)).digest());
      }
      assertEquals(original, store.usage());
      try (var receipt = receive(store, IDENTITY, header("two", null), "two")) {
        assertEquals(Wire.ERROR_ENTITY_INVALID, assertThrows(ProtocolException.class, () -> store.install(List.of(receipt))).errorCode());
      }
      var changed = new SealedTransport.Header(ENTITY, null, 1, "text/plain", BigInteger.valueOf(3), hash("one"), Map.of(), null);
      try (var receipt = receive(store, IDENTITY, changed, "one")) {
        assertEquals(Wire.ERROR_ENTITY_INVALID, assertThrows(ProtocolException.class, () -> store.install(List.of(receipt))).errorCode());
      }
      assertEquals(original, store.usage());
      try (var input = store.find(IDENTITY).orElseThrow().openStream()) { assertArrayEquals(bytes("one"), input.readAllBytes()); }
    }
  }

  @Test void completeChunkGeometryAndIdentityAreRequiredBeforeInstallation() throws Exception {
    try (var store = SealedPayloadStore.open(directory.resolve("chunks"), LIMITS)) {
      try (var first = receive(store, IDENTITY, chunk(0, 0, "abc"), "abc")) {
        assertEquals(Wire.ERROR_ENTITY_INVALID, assertThrows(ProtocolException.class, () -> store.install(List.of(first))).errorCode());
        for (var invalid : List.of(chunk(0, 3, "def"), chunk(1, 2, "def"), chunk(1, 4, "def"), chunk(1, 0, "def"))) {
          try (var second = receive(store, IDENTITY, invalid, "def")) {
            assertEquals(Wire.ERROR_ENTITY_INVALID, assertThrows(ProtocolException.class, () -> store.install(List.of(first, second))).errorCode());
          }
        }
        var changed = chunk(1, 3, "def");
        changed = new SealedTransport.Header(ENTITY, null, 3, changed.contentType(), changed.payloadLength(), changed.checksum(), changed.metadata(), changed.chunk());
        try (var second = receive(store, IDENTITY, changed, "def")) {
          assertEquals(Wire.ERROR_ENTITY_INVALID, assertThrows(ProtocolException.class, () -> store.install(List.of(first, second))).errorCode());
        }
      }
      assertTrue(store.find(IDENTITY).isEmpty()); assertEquals(0, store.usage().retainedFiles());
      assertEquals(0, store.usage().temporaryFiles());
    }
  }

  @Test void replayNeedsNoNewRetainedPublicationCapacity() throws Exception {
    long objectBytes;
    try (var probe = SealedPayloadStore.open(directory.resolve("measure"), LIMITS)) {
      try (var receipt = receive(probe, IDENTITY, header("abc", null), "abc")) { probe.install(List.of(receipt)); }
      objectBytes = probe.usage().retainedBytes();
    }
    var limits = new SealedPayloadStore.Limits(1024, 8, objectBytes * 2, 2, 1024, 2);
    Path root = directory.resolve("capacity-replay");
    try (var store = SealedPayloadStore.open(root, limits)) {
      try (var receipt = receive(store, IDENTITY, header("abc", null), "abc")) { store.install(List.of(receipt)); }
      assertEquals(objectBytes, store.usage().retainedBytes());
      var nextKey = new SealedWork.EntityKey(0, 2);
      var nextIdentity = new SealedPayloadStore.Identity(IDENTITY.session(), PRODUCER, nextKey);
      var nextHeader = new SealedTransport.Header(nextKey, null, 0, "text/plain", BigInteger.valueOf(3), hash("abc"), Map.of(), null);
      try (var receipt = receive(store, nextIdentity, nextHeader, "abc")) {
        assertEquals(Wire.ERROR_LIMIT_EXCEEDED, assertThrows(ProtocolException.class, () -> store.install(List.of(receipt))).errorCode());
      }
      try (var receipt = receive(store, IDENTITY, header("abc", null), "abc")) {
        assertArrayEquals(hash("abc"), store.install(List.of(receipt)).digest());
      }
      assertEquals(objectBytes, store.usage().retainedBytes()); assertEquals(1, store.usage().retainedFiles());
      assertEquals(0, store.usage().temporaryFiles());
      assertTrue(store.find(nextIdentity).isEmpty());
    }
    try (var reopened = SealedPayloadStore.open(root, limits)) {
      try (var receipt = receive(reopened, IDENTITY, header("abc", null), "abc")) {
        assertArrayEquals(hash("abc"), reopened.install(List.of(receipt)).digest());
      }
      assertEquals(objectBytes, reopened.usage().retainedBytes()); assertEquals(1, reopened.usage().retainedFiles());
    }
  }

  @Test void lengthsChecksumsAndUnsignedLimitsFailBeforeAdmissionAndReleaseSpools() throws Exception {
    try (var store = SealedPayloadStore.open(directory.resolve("invalid"), LIMITS)) {
      try (var receiver = store.begin(IDENTITY, header("abcd", null))) {
        receiver.write(bytes("abc"), 0, 3);
        assertEquals(Wire.ERROR_INTEGRITY, assertThrows(ProtocolException.class, receiver::finish).errorCode());
      }
      try (var receiver = store.begin(IDENTITY, header("abc", null))) {
        receiver.write(bytes("abd"), 0, 3);
        assertEquals(Wire.ERROR_INTEGRITY, assertThrows(ProtocolException.class, receiver::finish).errorCode());
      }
      try (var receiver = store.begin(IDENTITY, header("ab", null))) {
        assertEquals(Wire.ERROR_ENTITY_INVALID, assertThrows(ProtocolException.class, () -> receiver.write(bytes("abc"), 0, 3)).errorCode());
      }
      var huge = new SealedTransport.Header(ENTITY, null, 0, null, SealedCbor.MAX_UINT, null, Map.of(), null);
      assertEquals(Wire.ERROR_LIMIT_EXCEEDED, assertThrows(ProtocolException.class, () -> store.begin(IDENTITY, huge)).errorCode());
      var chunks = new SealedTransport.Header(ENTITY, null, 0, null, null, null, Map.of(), new SealedTransport.Chunk(SealedCbor.MAX_UINT, BigInteger.ZERO, BigInteger.ZERO));
      assertEquals(Wire.ERROR_LIMIT_EXCEEDED, assertThrows(ProtocolException.class, () -> store.begin(IDENTITY, chunks)).errorCode());
      assertEquals(new SealedPayloadStore.Usage(0, 0, 0, 0, 0), store.usage());
    }
  }

  @Test void quotasIncludeEmptyFilesPublicationHeadroomAndConcurrentWriters() throws Exception {
    var limits = new SealedPayloadStore.Limits(8, 1, 64, 2, 32, 2);
    try (var store = SealedPayloadStore.open(directory.resolve("limits"), limits)) {
      try (var receiver = store.begin(IDENTITY, header("", null))) {
        assertEquals(Wire.ERROR_LIMIT_EXCEEDED, assertThrows(ProtocolException.class, () -> store.begin(IDENTITY, header("", null))).errorCode());
        try (var receipt = receiver.finish()) {
          assertEquals(Wire.ERROR_LIMIT_EXCEEDED, assertThrows(ProtocolException.class, () -> store.install(List.of(receipt))).errorCode());
          assertEquals(0, store.usage().retainedFiles());
        }
      }
      var unbounded = new SealedTransport.Header(ENTITY, null, 0, null, null, null, Map.of(), null);
      try (var receiver = store.begin(IDENTITY, unbounded)) {
        assertEquals(Wire.ERROR_LIMIT_EXCEEDED, assertThrows(ProtocolException.class, () -> receiver.write(new byte[9], 0, 9)).errorCode());
      }
      assertEquals(new SealedPayloadStore.Usage(0, 0, 0, 0, 0), store.usage());
      assertThrows(IOException.class, () -> SealedPayloadStore.open(directory.resolve("limits"), limits));
    }
    assertEquals(Wire.ERROR_INTEGRITY, assertThrows(ProtocolException.class, () -> SealedPayloadStore.open(directory.resolve("limits"), LIMITS)).errorCode());
  }

  @Test void concurrentInstallationsHaveOneImmutableObjectAndExactCharges() throws Exception {
    try (var store = SealedPayloadStore.open(directory.resolve("concurrent"), LIMITS);
        var first = receive(store, IDENTITY, header("same", null), "same");
        var second = receive(store, IDENTITY, header("same", null), "same");
        var workers = Executors.newFixedThreadPool(2)) {
      var one = workers.submit(() -> store.install(List.of(first)));
      var two = workers.submit(() -> store.install(List.of(second)));
      assertArrayEquals(one.get(5, TimeUnit.SECONDS).digest(), two.get(5, TimeUnit.SECONDS).digest());
      assertEquals(1, store.usage().retainedFiles());
      try (var files = Files.list(directory.resolve("concurrent/objects"))) {
        Path object = files.findFirst().orElseThrow(); assertEquals(Files.size(object), store.usage().retainedBytes());
      }
    }
  }

  @Test void cancellingReceiptOwnershipCannotReleaseAnInstallationsSpoolCredit() throws Exception {
    Path root = directory.resolve("cancel-install");
    try (var store = SealedPayloadStore.open(root, LIMITS)) {
      var receipt = receive(store, IDENTITY, header("abc", null), "abc");
      try {
        // Hold the real publication gate after installation acquires its input pin.
        var field = SealedPayloadStore.class.getDeclaredField("publication"); field.setAccessible(true);
        Object gate = field.get(store);
        var result = new FutureTask<>(() -> store.install(List.of(receipt)));
        Thread worker = Thread.ofPlatform().unstarted(result);
        try {
          synchronized (gate) {
            worker.start();
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (worker.getState() != Thread.State.BLOCKED && !result.isDone() && System.nanoTime() < deadline) Thread.sleep(1);
            assertEquals(Thread.State.BLOCKED, worker.getState());
            receipt.close();
            assertEquals(3, store.usage().temporaryBytes()); assertEquals(1, store.usage().temporaryFiles());
            assertTrue(Files.isRegularFile(only(root.resolve("spool"))));
            assertThrows(IOException.class, store::close);
          }
          assertArrayEquals(hash("abc"), result.get(5, TimeUnit.SECONDS).digest());
          assertEquals(0, store.usage().temporaryBytes()); assertEquals(0, store.usage().temporaryFiles());
          assertEquals(0, store.usage().activeHandles());
        } finally { worker.join(5000); if (worker.isAlive()) worker.interrupt(); }
      } finally { receipt.close(); }
    }
  }

  @Test void concurrentReaderCloseReleasesExactlyOneHandle() throws Exception {
    try (var store = SealedPayloadStore.open(directory.resolve("reader-close"), LIMITS);
        var workers = Executors.newFixedThreadPool(8)) {
      SealedPayloadStore.Stored stored;
      try (var receipt = receive(store, IDENTITY, header("abc", null), "abc")) {
        stored = store.install(List.of(receipt));
      }
      for (int attempt = 0; attempt < 64; attempt++) {
        var input = stored.openStream();
        try {
          assertEquals(1, store.usage().activeHandles());
          var start = new CountDownLatch(1);
          List<java.util.concurrent.Future<Void>> closures = new ArrayList<>();
          for (int i = 0; i < 8; i++) closures.add(workers.submit(() -> {
            if (!start.await(5, TimeUnit.SECONDS)) throw new AssertionError("reader close was not released");
            input.close(); return null;
          }));
          start.countDown();
          for (var closure : closures) closure.get(5, TimeUnit.SECONDS);
          assertEquals(0, store.usage().activeHandles());
        } finally { input.close(); }
      }
    }
  }

  @Test void corruptSpoolsAndRetainedObjectsNeverBecomeNewSuccessfulCommitments() throws Exception {
    Path root = directory.resolve("corrupt");
    try (var store = SealedPayloadStore.open(root, LIMITS)) {
      try (var receipt = receive(store, IDENTITY, header("abc", null), "abc")) {
        Files.write(only(root.resolve("spool")), bytes("abd"));
        assertEquals(Wire.ERROR_INTEGRITY, assertThrows(ProtocolException.class, () -> store.install(List.of(receipt))).errorCode());
        assertEquals(0, store.usage().retainedFiles());
        assertTrue(store.find(IDENTITY).isEmpty());
      }
      SealedPayloadStore.Stored stored;
      try (var receipt = receive(store, IDENTITY, header("abc", null), "abc")) { stored = store.install(List.of(receipt)); }
      try (var file = FileChannel.open(only(root.resolve("objects")), StandardOpenOption.WRITE)) {
        file.position(file.size() - 1); file.write(ByteBuffer.wrap(new byte[] {'x'}));
      }
      assertEquals(Wire.ERROR_INTEGRITY, assertThrows(ProtocolException.class, stored::openStream).errorCode());
      assertEquals(Wire.ERROR_INTEGRITY, assertThrows(ProtocolException.class, () -> store.find(IDENTITY)).errorCode());
      assertEquals(0, store.usage().activeHandles());
    }
  }

  @Test void handleBudgetAndFailedCleanupCannotReleaseCapacityEarly() throws Exception {
    Path root = directory.resolve("handles");
    try (var store = SealedPayloadStore.open(root, LIMITS)) {
      List<SealedPayloadStore.Receiver> receivers = new ArrayList<>();
      try {
        for (int i = 0; i < 128; i++) receivers.add(store.begin(IDENTITY, header("", null)));
        assertEquals(Wire.ERROR_LIMIT_EXCEEDED, assertThrows(ProtocolException.class, () -> store.begin(IDENTITY, header("", null))).errorCode());
        assertThrows(IOException.class, store::close);
      } finally { for (var receiver : receivers) receiver.close(); }
      var receipt = receive(store, IDENTITY, header("abc", null), "abc");
      Files.delete(only(root.resolve("spool")));
      assertThrows(IOException.class, receipt::close);
      assertEquals(3, store.usage().temporaryBytes()); assertEquals(1, store.usage().temporaryFiles());
      assertEquals(0, store.usage().activeHandles());
    }
    try (var reopened = SealedPayloadStore.open(root, LIMITS)) { assertEquals(0, reopened.usage().temporaryBytes()); }
  }

  @Test void foreignLayoutsAndSymlinksAreRefusedWithoutConversion() throws Exception {
    Path foreign = Files.createDirectory(directory.resolve("foreign"));
    Path document = foreign.resolve("document.txt"); Files.writeString(document, "retain this");
    assertEquals(Wire.ERROR_INTEGRITY, assertThrows(ProtocolException.class, () -> SealedPayloadStore.open(foreign, LIMITS)).errorCode());
    assertEquals("retain this", Files.readString(document)); assertFalse(Files.exists(foreign.resolve("policy.cbor")));
    Path root = directory.resolve("symlink");
    try (var store = SealedPayloadStore.open(root, LIMITS)) { assertEquals(0, store.usage().retainedFiles()); }
    Files.createSymbolicLink(root.resolve("spool").resolve(UUID.randomUUID() + ".tmp"), document);
    assertEquals(Wire.ERROR_INTEGRITY, assertThrows(ProtocolException.class, () -> SealedPayloadStore.open(root, LIMITS)).errorCode());
    assertEquals("retain this", Files.readString(document));
  }

  @Test void abruptExitRetainsUnadmittedObjectsAndAccountsForAbandonedSpools() throws Exception {
    Path root = directory.resolve("crash");
    runChild("crash", root, "64m");
    try (var reopened = SealedPayloadStore.open(root, LIMITS)) {
      assertEquals(1, reopened.usage().temporaryFiles()); assertEquals(3, reopened.usage().temporaryBytes());
      assertEquals(1, reopened.usage().retainedFiles());
      assertArrayEquals(hash("abc"), reopened.find(IDENTITY).orElseThrow().digest());
    }
    var sessions = SealedSessionStore.open(root.resolveSibling("crash.sqlite3"));
    assertFalse(sessions.checkpointReady(IDENTITY.session(), PRODUCER, 0, 1));
    try (var connection = DriverManager.getConnection("jdbc:sqlite:" + root.resolveSibling("crash.sqlite3"));
        var query = connection.createStatement(); var rows = query.executeQuery("SELECT state FROM ps_java_entities")) {
      assertTrue(rows.next()); assertNull(rows.getObject(1));
    }
  }

  @Test void rejectedLocalOpenDoesNotReleaseTheCrossProcessWriterLock() throws Exception {
    Path root = directory.resolve("exclusive");
    try (var store = SealedPayloadStore.open(root, LIMITS)) {
      assertEquals(0, store.usage().activeHandles());
      runChild("locked", root, "64m");
      assertThrows(IOException.class, () -> SealedPayloadStore.open(root, LIMITS));
      runChild("locked", root, "64m");
    }
    try (var reopened = SealedPayloadStore.open(root, LIMITS)) { assertEquals(0, reopened.usage().activeHandles()); }
  }

  @Test void largePayloadStreamsWithAHeapSmallerThanThePayload() throws Exception {
    runChild("large", directory.resolve("large"), "24m");
  }

  private void runChild(String mode, Path root, String heap) throws Exception {
    Path log = Files.createTempFile(directory, "payload-child-", ".log");
    Process process = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin/java").toString(), "-Xmx" + heap,
        "--enable-native-access=ALL-UNNAMED", "-cp", System.getProperty("java.class.path"), SealedPayloadStoreTest.class.getName(), mode, root.toString())
        .redirectErrorStream(true).redirectOutput(log.toFile()).start();
    try {
      assertTrue(process.waitFor(30, TimeUnit.SECONDS), () -> read(log));
      assertEquals(0, process.exitValue(), () -> read(log));
    } finally { if (process.isAlive()) process.destroyForcibly().waitFor(5, TimeUnit.SECONDS); }
  }

  public static void main(String[] arguments) throws Exception {
    Path root = Path.of(arguments[1]);
    if (arguments[0].equals("crash")) {
      declared(root.resolveSibling("crash.sqlite3"));
      var store = SealedPayloadStore.open(root, LIMITS);
      var receipt = receive(store, IDENTITY, header("abc", null), "abc");
      store.install(List.of(receipt));
      Runtime.getRuntime().halt(0);
    } else if (arguments[0].equals("locked")) {
      try (var store = SealedPayloadStore.open(root, LIMITS)) {
        throw new AssertionError("second process acquired an owned payload store: " + store.usage());
      } catch (IOException expected) {
        if (!expected.getMessage().equals("payload store already has a writer")) throw expected;
      }
    } else if (arguments[0].equals("large")) {
      long length = 32L << 20;
      var limits = new SealedPayloadStore.Limits(40L << 20, 4, 80L << 20, 4, length, 2);
      try (var store = SealedPayloadStore.open(root, limits)) {
        var header = new SealedTransport.Header(ENTITY, null, 0, null, BigInteger.valueOf(length), null, Map.of(), null);
        try (var receiver = store.begin(IDENTITY, header)) {
          byte[] buffer = new byte[8192]; var digest = SealedWork.sha256();
          for (long offset = 0; offset < length; offset += buffer.length) {
            buffer[0] = (byte) (offset / buffer.length); digest.update(buffer); receiver.write(buffer, 0, buffer.length);
          }
          try (var receipt = receiver.finish()) {
            var stored = store.install(List.of(receipt));
            if (!java.security.MessageDigest.isEqual(digest.digest(), stored.digest())) throw new AssertionError("streamed digest differs");
            long read = 0;
            try (var input = stored.openStream()) { int n; while ((n = input.read(buffer)) != -1) read += n; }
            if (read != length) throw new AssertionError("streamed length differs");
          }
        }
        if (store.usage().temporaryBytes() != 0) throw new AssertionError("spool capacity was not reclaimed");
      }
    } else throw new IllegalArgumentException("unknown child mode");
  }

  private static SealedSessionStore declared(Path path) throws Exception {
    var sessions = SealedSessionStore.open(path);
    var declaration = new SealedWork.Declaration(IDENTITY.session(), PRODUCER, 0, null, BigInteger.ZERO, List.of(1L), SealedWork.SEAL,
        SealedWork.sealDigest(IDENTITY.session(), PRODUCER, 0, null, List.of(1L)));
    sessions.declare(declaration, 7, 16); return sessions;
  }
  private static SealedPayloadStore.Received receive(SealedPayloadStore store, SealedPayloadStore.Identity identity, SealedTransport.Header header, String text) throws Exception {
    try (var receiver = store.begin(identity, header)) { byte[] bytes = bytes(text); receiver.write(bytes, 0, bytes.length); return receiver.finish(); }
  }
  private static SealedTransport.Header header(String text, SealedTransport.Chunk chunk) {
    return new SealedTransport.Header(ENTITY, null, 0, "text/plain", BigInteger.valueOf(bytes(text).length), hash(text), Map.of(), chunk);
  }
  private static SealedTransport.Header chunk(int index, int offset, String text) {
    return header(text, new SealedTransport.Chunk(BigInteger.TWO, BigInteger.valueOf(index), BigInteger.valueOf(offset)));
  }
  private static byte[] bytes(String text) { return text.getBytes(StandardCharsets.UTF_8); }
  private static byte[] hash(String text) { return SealedWork.sha256().digest(bytes(text)); }
  private static Path only(Path directory) throws IOException {
    try (var files = Files.list(directory)) { var paths = files.toList(); assertEquals(1, paths.size()); return paths.getFirst(); }
  }
  private static String read(Path path) { try { return Files.readString(path); } catch (IOException failure) { return failure.toString(); } }
}
