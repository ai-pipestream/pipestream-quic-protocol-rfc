package ai.pipestream.quic.v2;

import static ai.pipestream.quic.v2.Messages.DURABLE_WORK;
import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

@Timeout(10)
final class InputStoreTest {
  private static final Messages.Capabilities SELECTED =
      new Messages.Capabilities(
          true, List.of(DURABLE_WORK), List.of(), 65536, 4, 16, 1 << 20, 1000, 5000);
  private static final InputStore.Limits AMPLE = new InputStore.Limits(16L << 20, 32, 8 << 20, 8);

  @TempDir Path directory;

  @Test
  void emptyAndExactInputsInstallOnlyAfterFinishAndReplayWithoutOverwrite() throws Exception {
    Path root = directory.resolve("exact");
    try (InputStore store = InputStore.initialize(root, AMPLE)) {
      Commitments.Context context = context("alice", 1);
      Records.InputHeader empty = header(1, new byte[0], "empty");
      try (InputStore.Receiver receiver = store.begin(context, empty, SELECTED, 1)) {
        assertTrue(store.find(context, empty).isEmpty());
        InputStore.Stored stored = receiver.finish(2);
        assertEquals(empty, stored.header());
        assertEquals(context, stored.context());
        assertEquals(0, stored.length());
        assertArrayEquals(new byte[0], read(stored));
      }

      byte[] payload = pattern(257, 7);
      Records.InputHeader exact = header(2, payload, "exact");
      InputStore.Stored first;
      try (InputStore.Receiver receiver = store.begin(context, exact, SELECTED, 3)) {
        receiver.write(ByteBuffer.wrap(payload), 4);
        first = receiver.finish(5);
      }
      assertArrayEquals(payload, read(first));
      InputStore.Stored replay = store.find(context, exact).orElseThrow();
      assertArrayEquals(payload, read(replay));
      InputStore.Usage beforeDuplicate = store.usage();
      try (InputStore.Receiver duplicate = store.begin(context, exact, SELECTED, 6)) {
        duplicate.write(ByteBuffer.wrap(payload), 7);
        assertArrayEquals(payload, read(duplicate.finish(8)));
      }
      assertEquals(beforeDuplicate, store.usage());
      assertEquals(2, store.usage().files());
    }
    try (InputStore reopened = InputStore.open(root, AMPLE)) {
      assertArrayEquals(
          pattern(257, 7),
          read(
              reopened
                  .find(context("alice", 1), header(2, pattern(257, 7), "exact"))
                  .orElseThrow()));
    }
  }

  @Test
  void truncatedTrailingAndDigestMismatchNeverInstall() throws Exception {
    try (InputStore store = InputStore.initialize(directory.resolve("invalid"), AMPLE)) {
      Commitments.Context context = context("alice", 1);
      byte[] expected = pattern(4, 1);
      Records.InputHeader header = header(1, expected, "invalid");
      try (InputStore.Receiver receiver = store.begin(context, header, SELECTED, 1)) {
        receiver.write(ByteBuffer.wrap(Arrays.copyOf(expected, 3)), 2);
        assertCode(ProtocolError.Code.INTEGRITY_ERROR, () -> receiver.finish(3));
      }
      try (InputStore.Receiver receiver = store.begin(context, header, SELECTED, 4)) {
        assertCode(
            ProtocolError.Code.INTEGRITY_ERROR,
            () -> receiver.write(ByteBuffer.wrap(new byte[] {1, 2, 3, 4, 5}), 5));
      }
      Records.InputHeader wrong =
          new Records.InputHeader(
              1,
              operation(2),
              new Records.AdmitParameters(
                  new Records.WorkKey(0, 0, 2),
                  new Records.Input(4, digest(new byte[] {9}), "application/octet-stream"),
                  "invalid",
                  0,
                  1000,
                  new Records.OutputBudget(0, 0)));
      try (InputStore.Receiver receiver = store.begin(context, wrong, SELECTED, 6)) {
        receiver.write(ByteBuffer.wrap(expected), 7);
        assertCode(ProtocolError.Code.INTEGRITY_ERROR, () -> receiver.finish(8));
      }
      assertTrue(store.find(context, header).isEmpty());
      assertTrue(store.find(context, wrong).isEmpty());
    }
  }

  @Test
  void quotasAreReservedBeforeReceptionAndReleasedByAbort() throws Exception {
    InputStore.Limits one = new InputStore.Limits(65_536, 8, 1024, 1);
    try (InputStore store = InputStore.initialize(directory.resolve("quota"), one)) {
      byte[] payload = pattern(1024, 3);
      Commitments.Context context = context("alice", 1);
      Records.InputHeader empty = header(2, new byte[0], "two");
      assertEquals(new InputStore.Usage(0, 0, 0), store.usage());
      InputStore.Usage emptyReservation;
      try (InputStore.Receiver receiver = store.begin(context, empty, SELECTED, 1)) {
        receiver.checkDeadline(1);
        emptyReservation = store.usage();
      }
      assertEquals(new InputStore.Usage(0, 0, 0), store.usage());
      try (InputStore.Receiver active =
          store.begin(context, header(1, payload, "one"), SELECTED, 2)) {
        active.checkDeadline(2);
        InputStore.Usage activeReservation = store.usage();
        assertEquals(2, activeReservation.files());
        assertTrue(activeReservation.bytes() > 0);
        assertEquals(1, activeReservation.handles());
        assertTrue(one.files() - activeReservation.files() >= emptyReservation.files());
        assertTrue(one.bytes() - activeReservation.bytes() >= emptyReservation.bytes());
        assertCode(
            ProtocolError.Code.LIMIT_EXCEEDED, () -> store.begin(context, empty, SELECTED, 3));
      }
      assertEquals(new InputStore.Usage(0, 0, 0), store.usage());
      InputStore.Stored stored;
      try (InputStore.Receiver retry = store.begin(context, empty, SELECTED, 4)) {
        stored = retry.finish(5);
      }
      try (var first = stored.openStream()) {
        assertEquals(1, store.usage().handles());
        assertCode(ProtocolError.Code.LIMIT_EXCEEDED, stored::openStream);
        assertEquals(-1, first.read());
      }
      assertEquals(0, store.usage().handles());
      try (var reopened = stored.openStream()) {
        assertEquals(-1, reopened.read());
      }
    }

    for (InputStore.Limits limit :
        List.of(
            new InputStore.Limits(4096, 1, 1024, 2),
            new InputStore.Limits(1, 8, 1024, 2),
            new InputStore.Limits(4096, 8, 3, 2))) {
      Path root =
          directory.resolve(
              "bound-" + limit.files() + "-" + limit.bytes() + "-" + limit.objectBytes());
      try (InputStore store = InputStore.initialize(root, limit)) {
        assertCode(
            ProtocolError.Code.LIMIT_EXCEEDED,
            () -> store.begin(context("alice", 1), header(1, new byte[4], "bounded"), SELECTED, 1));
        assertEquals(new InputStore.Usage(0, 0, 0), store.usage());
      }
    }
  }

  @Test
  void incrementalLargeInputPreservesExactBytesAcrossFixedChunks() throws Exception {
    byte[] payload = pattern(256 * 1024 + 31, 11);
    try (InputStore store = InputStore.initialize(directory.resolve("chunks"), AMPLE)) {
      Records.InputHeader header = header(1, payload, "chunks");
      try (InputStore.Receiver receiver = store.begin(context("alice", 1), header, SELECTED, 1)) {
        for (int offset = 0; offset < payload.length; offset += 4096) {
          int length = Math.min(4096, payload.length - offset);
          receiver.write(ByteBuffer.wrap(payload, offset, length), offset + 2L);
        }
        assertArrayEquals(payload, read(receiver.finish(payload.length + 3L)));
      }
    }
  }

  @Test
  void writeConsumesOnlyTheSelectedByteBufferRange() throws Exception {
    byte[] payload = pattern(31, 19);
    byte[] framed = new byte[payload.length + 9];
    System.arraycopy(payload, 0, framed, 4, payload.length);
    ByteBuffer selected = ByteBuffer.wrap(framed);
    selected.position(4).limit(4 + payload.length);
    try (InputStore store = InputStore.initialize(directory.resolve("position"), AMPLE);
        InputStore.Receiver receiver =
            store.begin(context("alice", 1), header(1, payload, "position"), SELECTED, 1)) {
      receiver.write(selected, 2);
      assertEquals(selected.limit(), selected.position());
      assertArrayEquals(payload, read(receiver.finish(3)));
    }
  }

  @Test
  void idleAndAbsoluteDeadlinesAbortAndReleaseReception() throws Exception {
    byte[] payload = new byte[] {1, 2, 3, 4, 5, 6};
    try (InputStore store = InputStore.initialize(directory.resolve("deadlines"), AMPLE)) {
      InputStore.Receiver idle =
          store.begin(context("alice", 1), header(1, payload, "idle"), SELECTED, 0);
      assertCode(
          ProtocolError.Code.LIMIT_EXCEEDED,
          () -> idle.checkDeadline(TimeUnit.MILLISECONDS.toNanos(1000) + 1));
      idle.close();
      assertEquals(0, store.usage().handles());

      InputStore.Receiver lifetime =
          store.begin(context("alice", 1), header(2, payload, "lifetime"), SELECTED, 0);
      for (int index = 0; index < 5; index++)
        lifetime.write(
            ByteBuffer.wrap(new byte[] {payload[index]}),
            TimeUnit.MILLISECONDS.toNanos(900L * (index + 1)));
      assertCode(
          ProtocolError.Code.LIMIT_EXCEEDED,
          () -> lifetime.checkDeadline(TimeUnit.MILLISECONDS.toNanos(5000) + 1));
      lifetime.close();
      assertEquals(0, store.usage().handles());
    }
  }

  @Test
  void identityAndHeaderArePartOfImmutableObjectIdentity() throws Exception {
    byte[] payload = pattern(32, 5);
    try (InputStore store = InputStore.initialize(directory.resolve("identity"), AMPLE)) {
      Records.InputHeader first = header(1, payload, "a");
      install(store, context("alice", 1), first, payload, 1);
      assertTrue(store.find(context("bob", 1), first).isEmpty());
      Records.InputHeader changed = header(2, payload, "b");
      install(store, context("alice", 1), changed, payload, 10);
      assertArrayEquals(payload, read(store.find(context("alice", 1), first).orElseThrow()));
      assertArrayEquals(payload, read(store.find(context("alice", 1), changed).orElseThrow()));
    }
  }

  @Test
  void abortAndReaderHandlesAreOwnedAcrossCloseAndReopen() throws Exception {
    Path root = directory.resolve("ownership");
    InputStore store = InputStore.initialize(root, AMPLE);
    Records.InputHeader header = header(1, pattern(8, 2), "owned");
    InputStore.Receiver receiver = store.begin(context("alice", 1), header, SELECTED, 1);
    assertThrows(IOException.class, store::close);
    receiver.close();
    receiver.close();
    assertTrue(store.find(context("alice", 1), header).isEmpty());
    install(store, context("alice", 1), header, pattern(8, 2), 2);
    var stream = store.find(context("alice", 1), header).orElseThrow().openStream();
    assertEquals(1, store.usage().handles());
    assertThrows(IOException.class, store::close);
    stream.close();
    assertEquals(0, store.usage().handles());
    store.close();
    try (InputStore reopened = InputStore.open(root, AMPLE)) {
      assertArrayEquals(
          pattern(8, 2), read(reopened.find(context("alice", 1), header).orElseThrow()));
    }
  }

  @Test
  void liveDuplicateChangedPolicyAndForeignLayoutsAreRefused() throws Exception {
    Path root = directory.resolve("layout");
    InputStore live = InputStore.initialize(root, AMPLE);
    assertThrows(IOException.class, () -> InputStore.open(root, AMPLE));
    assertThrows(IOException.class, () -> InputStore.initialize(root, AMPLE));
    live.close();
    assertThrows(
        IOException.class,
        () -> {
          InputStore unexpected =
              InputStore.open(root, new InputStore.Limits(15L << 20, 32, 8 << 20, 8));
          try {
            fail("changed input-store policy was accepted: " + unexpected.identity());
          } finally {
            unexpected.close();
          }
        });
    try (InputStore reopened = InputStore.open(root, AMPLE)) {
      assertNotNull(reopened.identity());
    }

    Path empty = directory.resolve("empty");
    Files.createDirectory(empty);
    assertThrows(IOException.class, () -> InputStore.open(empty, AMPLE));
    Path dirty = directory.resolve("dirty");
    Files.createDirectory(dirty);
    Files.writeString(dirty.resolve("foreign"), "not a store");
    assertThrows(IOException.class, () -> InputStore.open(dirty, AMPLE));
    assertThrows(IOException.class, () -> InputStore.initialize(dirty, AMPLE));
  }

  @Test
  void corruptedOrTruncatedRetainedObjectIsRejectedByFindAndRecovery() throws Exception {
    for (String damage : List.of("corrupt", "truncate")) {
      Path root = directory.resolve("damage-" + damage);
      Commitments.Context context = context("alice", 1);
      byte[] payload = pattern(64, 13);
      Records.InputHeader header = header(1, payload, damage);
      InputStore store = InputStore.initialize(root, AMPLE);
      install(store, context, header, payload, 1);
      Path object = onlyObject(root);
      byte[] bytes = Files.readAllBytes(object);
      if (damage.equals("truncate")) {
        Files.write(object, Arrays.copyOf(bytes, bytes.length - 1));
      } else {
        bytes[bytes.length - 1] ^= 1;
        Files.write(object, bytes);
      }
      assertThrows(IOException.class, () -> store.find(context, header));
      store.close();
      assertThrows(IOException.class, () -> InputStore.open(root, AMPLE));
    }
  }

  @Test
  void lookupMustCompleteDurableInstallationBeforeReturningObject() throws Exception {
    Path root = directory.resolve("durable-lookup");
    Commitments.Context context = context("alice", 1);
    byte[] payload = pattern(64, 29);
    Records.InputHeader header = header(1, payload, "durable-lookup");
    try (InputStore store = InputStore.initialize(root, AMPLE)) {
      install(store, context, header, payload, 1);
      Path object = onlyObject(root);
      Files.setPosixFilePermissions(object, PosixFilePermissions.fromString("r--------"));
      try {
        assertThrows(IOException.class, () -> store.find(context, header));
      } finally {
        Files.setPosixFilePermissions(object, PosixFilePermissions.fromString("rw-------"));
      }
      assertArrayEquals(payload, read(store.find(context, header).orElseThrow()));
    }
  }

  @Test
  void processDeathAtEachFilesystemBoundaryHasExactRecoveryState() throws Exception {
    for (InputStore.Phase phase :
        List.of(
            InputStore.Phase.RECEIVED,
            InputStore.Phase.LINKED,
            InputStore.Phase.OBJECT_SYNCED,
            InputStore.Phase.STAGING_REMOVED)) {
      Path root = directory.resolve("crash-" + phase);
      int expectedExit = 40 + phase.ordinal();
      assertEquals(expectedExit, runChild("crash", root, phase.name(), expectedExit));
      try (InputStore store = InputStore.open(root, AMPLE)) {
        boolean installed = phase != InputStore.Phase.RECEIVED;
        assertEquals(
            installed,
            store.find(context("alice", 1), header(1, pattern(64, 23), "crash")).isPresent());
        assertEquals(installed ? 1 : 0, store.usage().files());
        assertEquals(0, store.usage().handles());
        assertEquals(0, entries(root.resolve("pending"), "*.part"));
      }
    }
  }

  @Test
  void processLockExcludesDuplicateUntilOwnedProcessCloses() throws Exception {
    Path root = directory.resolve("process-lock");
    Path ready = directory.resolve("process.ready");
    Path release = directory.resolve("process.release");
    Process child = child("hold", root, ready.toString(), release.toString());
    try {
      awaitFile(ready, child);
      assertThrows(IOException.class, () -> InputStore.open(root, AMPLE));
      Files.createFile(release);
      assertTrue(child.waitFor(5, TimeUnit.SECONDS), "lock child did not exit");
      assertEquals(0, child.exitValue());
      try (InputStore reopened = InputStore.open(root, AMPLE)) {
        assertNotNull(reopened.identity());
      }
    } finally {
      if (child.isAlive()) child.destroyForcibly();
      assertTrue(child.waitFor(2, TimeUnit.SECONDS), "lock child did not terminate");
    }
  }

  public static void main(String[] args) throws Exception {
    Path root = Path.of(args[1]);
    if (args[0].equals("crash")) {
      InputStore.Phase target = InputStore.Phase.valueOf(args[2]);
      int exit = Integer.parseInt(args[3]);
      try (InputStore store =
              InputStore.initialize(
                  root,
                  AMPLE,
                  phase -> {
                    if (phase == target) Runtime.getRuntime().halt(exit);
                  });
          InputStore.Receiver receiver =
              store.begin(context("alice", 1), header(1, pattern(64, 23), "crash"), SELECTED, 1)) {
        if (store.identity() == null) throw new AssertionError("input store identity missing");
        receiver.write(ByteBuffer.wrap(pattern(64, 23)), 2);
        receiver.finish(3);
      }
      throw new AssertionError("crash probe did not halt at " + target);
    }
    try (InputStore store = InputStore.initialize(root, AMPLE)) {
      if (store.identity() == null) throw new AssertionError("input store identity missing");
      Files.createFile(Path.of(args[2]));
      Path release = Path.of(args[3]);
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
      while (!Files.exists(release) && System.nanoTime() < deadline) Thread.sleep(10);
      if (!Files.exists(release)) throw new AssertionError("lock child release was not signaled");
    }
  }

  private int runChild(String mode, Path root, String phase, int exit) throws Exception {
    Process child = child(mode, root, phase, Integer.toString(exit));
    try {
      assertTrue(child.waitFor(5, TimeUnit.SECONDS), "crash child did not exit");
      return child.exitValue();
    } finally {
      if (child.isAlive()) {
        child.destroyForcibly();
        assertTrue(child.waitFor(2, TimeUnit.SECONDS));
      }
    }
  }

  private Process child(String mode, Path root, String third, String fourth) throws IOException {
    return new ProcessBuilder(
            Path.of(System.getProperty("java.home"), "bin", "java").toString(),
            "-cp",
            System.getProperty("java.class.path"),
            InputStoreTest.class.getName(),
            mode,
            root.toString(),
            third,
            fourth)
        .redirectOutput(directory.resolve(mode + "-" + root.getFileName() + ".out").toFile())
        .redirectError(directory.resolve(mode + "-" + root.getFileName() + ".err").toFile())
        .start();
  }

  private static void awaitFile(Path ready, Process child) throws Exception {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    while (!Files.exists(ready) && child.isAlive() && System.nanoTime() < deadline)
      Thread.sleep(10);
    assertTrue(
        Files.exists(ready),
        () ->
            "lock child did not become ready; "
                + (child.isAlive() ? "still alive" : "exit=" + child.exitValue()));
  }

  private static int entries(Path directory, String glob) throws IOException {
    int count = 0;
    try (var entries = Files.newDirectoryStream(directory, glob)) {
      for (Path ignored : entries) count++;
    }
    return count;
  }

  private static void install(
      InputStore store,
      Commitments.Context context,
      Records.InputHeader header,
      byte[] payload,
      long now)
      throws Exception {
    try (InputStore.Receiver receiver = store.begin(context, header, SELECTED, now)) {
      receiver.write(ByteBuffer.wrap(payload), now + 1);
      receiver.finish(now + 2);
    }
  }

  private static byte[] read(InputStore.Stored stored) throws IOException {
    try (var input = stored.openStream()) {
      return input.readAllBytes();
    }
  }

  private static Path onlyObject(Path root) throws IOException {
    Path found = null;
    int count = 0;
    try (var objects = Files.newDirectoryStream(root.resolve("objects"), "*.input")) {
      for (Path object : objects) {
        assertTrue(++count <= AMPLE.files(), "object directory exceeded bounded fixture policy");
        found = object;
      }
    }
    assertEquals(1, count);
    return found;
  }

  private static Commitments.Context context(String owner, long generation) {
    return new Commitments.Context("issuer-a", owner, generation);
  }

  private static Records.InputHeader header(int id, byte[] payload, String application) {
    return new Records.InputHeader(
        1,
        operation(id),
        new Records.AdmitParameters(
            new Records.WorkKey(0, 0, id),
            new Records.Input(payload.length, digest(payload), "application/octet-stream"),
            application,
            0,
            1000,
            new Records.OutputBudget(0, 0)));
  }

  private static Records.OperationId operation(int value) {
    byte[] bytes = new byte[16];
    bytes[15] = (byte) value;
    return new Records.OperationId(bytes);
  }

  private static Records.Digest digest(byte[] bytes) {
    try {
      return new Records.Digest(MessageDigest.getInstance("SHA-256").digest(bytes));
    } catch (java.security.NoSuchAlgorithmException impossible) {
      throw new AssertionError(impossible);
    }
  }

  private static byte[] pattern(int length, int seed) {
    byte[] bytes = new byte[length];
    for (int index = 0; index < length; index++) bytes[index] = (byte) (seed + index * 31);
    return bytes;
  }

  private static void assertCode(ProtocolError.Code code, Throwing action) {
    ProtocolError error = assertThrows(ProtocolError.class, action::run);
    assertEquals(code, error.code(), error::getMessage);
  }

  @FunctionalInterface
  private interface Throwing {
    void run() throws Exception;
  }
}
