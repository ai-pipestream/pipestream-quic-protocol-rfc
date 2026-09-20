package ai.pipestream.quic.storage;

import static ai.pipestream.quic.storage.ObjectStoreContractTest.assertStorage;
import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Filesystem-backend specifics: physical layout, reopen durability, tamper detection, hygiene. */
final class FileObjectStoreBackendTest {
  @TempDir Path directory;

  private static byte[] bytes(String value) {
    return value.getBytes(java.nio.charset.StandardCharsets.UTF_8);
  }

  private static byte[] digest(byte[] bytes) {
    return StoreLayout.sha256().digest(bytes);
  }

  private static Path entityFile(Path root, ScopePath scope, long entityId) {
    Path leaf = root.resolve("objects");
    for (long component : scope.components()) leaf = leaf.resolve(StoreLayout.scopeComponentName(component));
    return leaf.resolve(StoreLayout.fanoutBucket(entityId)).resolve(StoreLayout.entityName(entityId));
  }

  private static List<Path> walk(Path root) throws IOException {
    try (Stream<Path> paths = Files.walk(root)) {
      return paths.filter(Files::isRegularFile).toList();
    }
  }

  @Test void physicalLayoutFollowsScopeTreeAndFanout() throws Exception {
    var scope = ScopePath.of(0x01020304, 0x05060708);
    byte[] payload = bytes("physical-layout");
    try (ObjectStore store = FileObjectStore.open(directory)) {
      store.put(scope, 0x01020304, payload, digest(payload));
      store.sealScope(scope, digest(payload), 1);
      Path entity = entityFile(directory, scope, 0x01020304);
      assertTrue(Files.isRegularFile(entity));
      assertEquals("01020304", entity.getFileName().toString());
      Path leaf = entity.getParent().getParent();
      assertEquals("objects/01020304/05060708", leaf.subpath(directory.getNameCount(), leaf.getNameCount()).toString());
      ByteBuffer file = ByteBuffer.wrap(Files.readAllBytes(entity));
      assertEquals(payload.length, file.getLong());
      byte[] recorded = new byte[32];
      file.get(recorded);
      assertArrayEquals(digest(payload), recorded);
      assertArrayEquals(payload, java.util.Arrays.copyOfRange(file.array(), 40, file.array().length));
      ByteBuffer seal = ByteBuffer.wrap(Files.readAllBytes(leaf.resolve(FileObjectStore.SEAL_FILE_NAME)));
      assertArrayEquals(digest(payload), java.util.Arrays.copyOf(seal.array(), 32));
      assertEquals(1, seal.getLong(32));
    }
  }

  @Test void contentSurvivesCloseAndReopen() throws Exception {
    var scope = ScopePath.root(0x1111);
    byte[] first = bytes("first"), second = bytes("second");
    try (ObjectStore store = FileObjectStore.open(directory)) {
      store.put(scope, 1, first, digest(first));
      store.put(scope, 2, second, digest(second));
      store.sealScope(scope, StoreLayout.scopeDigest(List.of(
          new StoreLayout.EntityDigest(1, digest(first)), new StoreLayout.EntityDigest(2, digest(second)))), 2);
    }
    try (ObjectStore reopened = FileObjectStore.open(directory)) {
      reopened.verifyScope(scope);
      assertEquals(List.of(1L, 2L), reopened.list(scope));
      try (var stream = reopened.openRead(scope, 1)) {
        assertArrayEquals(first, stream.readAllBytes());
      }
    }
  }

  @Test void tamperedEntityBytesFailVerificationAsCorrupt() throws Exception {
    var scope = ScopePath.root(0x2222);
    byte[] payload = bytes("tamper-me");
    try (ObjectStore store = FileObjectStore.open(directory)) {
      store.put(scope, 5, payload, digest(payload));
      store.sealScope(scope, StoreLayout.scopeDigest(
          List.of(new StoreLayout.EntityDigest(5, digest(payload)))), 1);
    }
    Path entity = entityFile(directory, scope, 5);
    byte[] tampered = Files.readAllBytes(entity);
    tampered[tampered.length - 1] ^= 1;
    Files.write(entity, tampered);
    try (ObjectStore reopened = FileObjectStore.open(directory)) {
      assertStorage(StorageException.Kind.CORRUPT, () -> reopened.verifyScope(scope));
    }
  }

  @Test void tamperedSealDigestIsReturnedVerbatim() throws Exception {
    var scope = ScopePath.root(0x3333);
    byte[] payload = bytes("sealed-tamper");
    byte[] callerDigest = StoreLayout.scopeDigest(
        List.of(new StoreLayout.EntityDigest(1, digest(payload))));
    try (ObjectStore store = FileObjectStore.open(directory)) {
      store.put(scope, 1, payload, digest(payload));
      store.sealScope(scope, callerDigest, 1);
    }
    // The store verifies only store-owned state; the caller's SCOPE_DIGEST is
    // opaque, so a tampered digest byte comes back verbatim, not Corrupt.
    Path seal = entityFile(directory, scope, 1).getParent().getParent().resolve(FileObjectStore.SEAL_FILE_NAME);
    byte[] raw = Files.readAllBytes(seal);
    raw[0] ^= 1;
    Files.write(seal, raw);
    byte[] tampered = java.util.Arrays.copyOf(raw, StoreLayout.DIGEST_BYTES);
    try (ObjectStore reopened = FileObjectStore.open(directory)) {
      ObjectStore.SealRecord record = reopened.verifyScope(scope);
      assertArrayEquals(tampered, record.digest(), "tampered SCOPE_DIGEST returned verbatim");
      assertEquals(1, record.entityCount());
    }
  }

  @Test void abandonedScratchIsSweptAndNeverLeavesTheRoot() throws Exception {
    var scope = ScopePath.root(0x4444);
    byte[] payload = bytes("clean-store");
    Path scratch;
    try (ObjectStore store = FileObjectStore.open(directory)) {
      store.put(scope, 1, payload, digest(payload));
      Path bucket = entityFile(directory, scope, 1).getParent();
      scratch = bucket.resolve("write-00000000-0000-0000-0000-000000000000.tmp");
      Files.write(scratch, new byte[] {1, 2, 3});
    }
    assertTrue(Files.exists(scratch));
    try (ObjectStore reopened = FileObjectStore.open(directory)) {
      assertFalse(Files.exists(scratch), "stray scratch swept at open");
      assertEquals(List.of(1L), reopened.list(scope));
    }
    for (Path file : walk(directory)) {
      assertFalse(file.getFileName().toString().endsWith(".tmp"), "no scratch left: " + file);
      assertTrue(file.startsWith(directory), "every file stays under the configured root");
    }
  }

  @Test void secondWriterIsRefusedAndReopenAfterCloseSucceeds() throws Exception {
    ObjectStore first = FileObjectStore.open(directory);
    assertStorage(StorageException.Kind.FAILED, () -> FileObjectStore.open(directory));
    first.close();
    try (ObjectStore second = FileObjectStore.open(directory)) {
      assertEquals(List.of(), second.list(ScopePath.root(0x5555)));
    }
  }

  @Test void journalSharesTheSameRoot() throws Exception {
    try (ObjectStore store = FileObjectStore.open(directory);
         Journal journal = FileJournal.open(directory)) {
      store.put(ScopePath.root(0x6666), 1, bytes("coexists"), digest(bytes("coexists")));
      journal.appendIntent(new Journal.Intent("intent", bytes("record")));
    }
  }

  @Test void rootThatIsARegularFileIsFailedNotTransient() throws Exception {
    Path file = directory.resolve("a-regular-file");
    Files.write(file, new byte[] {1, 2, 3});
    assertStorage(StorageException.Kind.FAILED, () -> FileObjectStore.open(file));
  }

  @Test void failedPutLeavesNoScratchAndStoreRemainsUsable() throws Exception {
    var scope = ScopePath.root(0x7778);
    Path bucket = entityFile(directory, scope, 3).getParent();
    Files.createDirectories(bucket);
    Set<PosixFilePermission> writable = Files.getPosixFilePermissions(bucket);
    Files.setPosixFilePermissions(bucket, Set.of(
        PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_EXECUTE));
    try (ObjectStore store = FileObjectStore.open(directory)) {
      assertStorage(StorageException.Kind.TRANSIENT,
          () -> store.put(scope, 3, bytes("blocked"), digest(bytes("blocked"))));
    } finally {
      Files.setPosixFilePermissions(bucket, writable);
    }
    try (Stream<Path> paths = Files.walk(directory)) {
      List<Path> scratch = paths
          .filter(path -> path.getFileName().toString().endsWith(".tmp"))
          .toList();
      assertEquals(List.of(), scratch, "failed put leaves no scratch behind");
    }
    try (ObjectStore store = FileObjectStore.open(directory)) {
      byte[] payload = bytes("after-failure");
      store.put(scope, 3, payload, digest(payload));
      try (var stream = store.openRead(scope, 3)) {
        assertArrayEquals(payload, stream.readAllBytes());
      }
    }
  }
}
