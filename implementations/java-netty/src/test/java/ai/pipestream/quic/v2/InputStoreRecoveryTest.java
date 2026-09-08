package ai.pipestream.quic.v2;

import static ai.pipestream.quic.v2.Messages.DURABLE_WORK;
import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

final class InputStoreRecoveryTest {
  private static final InputStore.Limits LIMITS = new InputStore.Limits(1 << 20, 8, 1 << 16, 2);
  private static final Messages.Capabilities SELECTED =
      new Messages.Capabilities(
          true, List.of(DURABLE_WORK), List.of(), 65536, 4, 16, 1 << 20, 1000, 5000);

  @TempDir Path directory;

  @Test
  void recoveryValidatesInstalledObjectsBeforeDeletingAbandonedPendingFiles() throws Exception {
    Path root = directory.resolve("validation-order");
    install(root, "alice", pattern(64, 3));
    Path object = only(root.resolve("objects"), "*.input");
    byte[] damaged = Files.readAllBytes(object);
    damaged[damaged.length - 1] ^= 1;
    Files.write(object, damaged);
    Path pending = root.resolve("pending").resolve(UUID.randomUUID() + ".part");
    byte[] abandoned = pattern(17, 9);
    Files.write(pending, abandoned);

    assertThrows(IOException.class, () -> InputStore.open(root, LIMITS));
    assertTrue(Files.isRegularFile(pending, LinkOption.NOFOLLOW_LINKS));
    assertArrayEquals(abandoned, Files.readAllBytes(pending));
    assertEquals(1, count(root.resolve("pending"), "*.part"));
  }

  @Test
  void objectCopiedAcrossInstallationsIsRejectedWithoutChangingSource() throws Exception {
    Path sourceRoot = directory.resolve("source");
    Path targetRoot = directory.resolve("target");
    byte[] payload = pattern(91, 5);
    Fixture source = install(sourceRoot, "alice", payload);
    try (InputStore target = InputStore.initialize(targetRoot, LIMITS)) {
      assertNotNull(target.identity());
    }
    Path sourceObject = only(sourceRoot.resolve("objects"), "*.input");
    Files.copy(sourceObject, targetRoot.resolve("objects").resolve(sourceObject.getFileName()));

    assertThrows(IOException.class, () -> InputStore.open(targetRoot, LIMITS));
    try (InputStore reopenedSource = InputStore.open(sourceRoot, LIMITS)) {
      assertArrayEquals(
          payload, read(reopenedSource.find(source.context(), source.header()).orElseThrow()));
    }
  }

  @Test
  void canonicalPendingSymlinkIsRefusedWithoutDeletingItsOutsideTarget() throws Exception {
    Path root = directory.resolve("symlink");
    try (InputStore initialized = InputStore.initialize(root, LIMITS)) {
      assertNotNull(initialized.identity());
    }
    Path outside = directory.resolve("outside.part");
    byte[] retained = pattern(23, 7);
    Files.write(outside, retained);
    Path pending = root.resolve("pending").resolve(UUID.randomUUID() + ".part");
    Files.createSymbolicLink(pending, outside);

    assertThrows(IOException.class, () -> InputStore.open(root, LIMITS));
    assertTrue(Files.isSymbolicLink(pending));
    assertTrue(Files.isRegularFile(outside, LinkOption.NOFOLLOW_LINKS));
    assertArrayEquals(retained, Files.readAllBytes(outside));
  }

  private Fixture install(Path root, String owner, byte[] payload) throws Exception {
    Commitments.Context context = new Commitments.Context("issuer-a", owner, 1);
    Records.InputHeader header = header(payload);
    try (InputStore store = InputStore.initialize(root, LIMITS);
        InputStore.Receiver receiver = store.begin(context, header, SELECTED, 1)) {
      assertNotNull(store.identity());
      receiver.write(ByteBuffer.wrap(payload), 2);
      assertArrayEquals(payload, read(receiver.finish(3)));
    }
    return new Fixture(context, header);
  }

  private static Records.InputHeader header(byte[] payload) throws Exception {
    byte[] operation = new byte[16];
    operation[15] = 1;
    return new Records.InputHeader(
        1,
        new Records.OperationId(operation),
        new Records.AdmitParameters(
            new Records.WorkKey(0, 0, 1),
            new Records.Input(
                payload.length,
                new Records.Digest(MessageDigest.getInstance("SHA-256").digest(payload)),
                "application/octet-stream"),
            "recovery",
            0,
            1000,
            new Records.OutputBudget(0, 0)));
  }

  private static byte[] read(InputStore.Stored stored) throws IOException {
    try (var input = stored.openStream()) {
      return input.readAllBytes();
    }
  }

  private static Path only(Path directory, String glob) throws IOException {
    Path found = null;
    int count = 0;
    try (var entries = Files.newDirectoryStream(directory, glob)) {
      for (Path entry : entries) {
        assertTrue(++count <= LIMITS.files());
        found = entry;
      }
    }
    assertEquals(1, count);
    return found;
  }

  private static int count(Path directory, String glob) throws IOException {
    int count = 0;
    try (var entries = Files.newDirectoryStream(directory, glob)) {
      for (Path ignored : entries) count++;
    }
    return count;
  }

  private static byte[] pattern(int length, int seed) {
    byte[] bytes = new byte[length];
    for (int index = 0; index < bytes.length; index++) bytes[index] = (byte) (seed + index * 31);
    return bytes;
  }

  private record Fixture(Commitments.Context context, Records.InputHeader header) {}
}
