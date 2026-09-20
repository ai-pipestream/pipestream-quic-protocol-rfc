package ai.pipestream.quic.storage;

import static ai.pipestream.quic.storage.ObjectStoreContractTest.assertStorage;
import static org.junit.jupiter.api.Assertions.*;

import java.io.InputStream;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Adversarial attacks on the logical layout and on stores opened on foreign or
 * hostile roots: u32-max entity identifiers, the depth-7 scope boundary, garbage
 * dropped into scope directories, roots whose expected directories have been replaced
 * by files, a writer lock already held by a foreign channel, and symlinked roots.
 * Every refusal asserts the pinned {@link StorageException.Kind}; lenient acceptances
 * that the pinned format mandates (scratch sweep, the backend-root lock file) are
 * pinned just as precisely.
 */
final class AdversarialFsLayoutTest {
  @TempDir Path directory;

  private static final long U32_MAX = 0xffff_ffffL;

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

  private static ScopePath deepScope() {
    return ScopePath.of(1, 2, 3, 4, 5, 6, 7, 8);
  }

  @Test void u32MaxEntityIdRoundTripsAcrossReopen() throws Exception {
    var scope = ScopePath.root(0xc001);
    byte[] payload = bytes("max-id");
    try (ObjectStore store = FileObjectStore.open(directory)) {
      store.put(scope, U32_MAX, payload, digest(payload));
      store.sealScope(scope, StoreLayout.scopeDigest(
          List.of(new StoreLayout.EntityDigest(U32_MAX, digest(payload)))), 1);
    }
    assertEquals("ffffffff", entityFile(directory, scope, U32_MAX).getFileName().toString());
    try (ObjectStore store = FileObjectStore.open(directory)) {
      try (InputStream stream = store.openRead(scope, U32_MAX)) {
        assertArrayEquals(payload, stream.readAllBytes());
      }
      assertEquals(List.of(U32_MAX), store.list(scope));
      store.verifyScope(scope);
    }
  }

  @Test void entityIdAboveU32IsRejectedBeforeAnyIO() throws Exception {
    var scope = ScopePath.root(0xc002);
    try (ObjectStore store = FileObjectStore.open(directory)) {
      byte[] payload = bytes("too-big");
      assertThrows(IllegalArgumentException.class,
          () -> store.put(scope, 1L << 32, payload, digest(payload)));
      assertThrows(IllegalArgumentException.class, () -> store.openRead(scope, 1L << 32));
      assertEquals(List.of(), store.list(scope), "rejected ids leave no trace");
    }
  }

  @Test void retireCursorBoundaryRemovesExactlyUpToTheCursor() throws Exception {
    var scope = ScopePath.root(0xc003);
    try (ObjectStore store = FileObjectStore.open(directory)) {
      for (long id = 0; id <= 4; id++) {
        byte[] payload = bytes("e" + id);
        store.put(scope, id, payload, digest(payload));
      }
      store.retire(scope, 2);
      assertEquals(List.of(3L, 4L), store.list(scope), "ids at or below the cursor are gone");
      store.retire(scope, U32_MAX);
      assertEquals(List.of(), store.list(scope));
      // Pinned decision 5: an emptied unsealed scope retires as NotFound.
      assertStorage(StorageException.Kind.NOT_FOUND, () -> store.retire(scope, U32_MAX));
    }
  }

  @Test void depthSevenScopeTreeFullyWorks() throws Exception {
    var scope = deepScope();
    byte[] payload = bytes("deep-payload");
    try (ObjectStore store = FileObjectStore.open(directory)) {
      store.put(scope, 0x01020304, payload, digest(payload));
      store.sealScope(scope, StoreLayout.scopeDigest(
          List.of(new StoreLayout.EntityDigest(0x01020304, digest(payload)))), 1);
    }
    Path leaf = directory.resolve("objects");
    for (long component : scope.components()) {
      leaf = leaf.resolve(StoreLayout.scopeComponentName(component));
    }
    assertTrue(Files.isRegularFile(leaf.resolve(FileObjectStore.SEAL_FILE_NAME)));
    try (ObjectStore store = FileObjectStore.open(directory)) {
      try (InputStream stream = store.openRead(scope, 0x01020304)) {
        assertArrayEquals(payload, stream.readAllBytes());
      }
      store.verifyScope(scope);
      assertEquals(List.of(scope), store.listScopes(), "intermediate dirs hold no state and are not scopes");
    }
  }

  @Test void depthEightIsRejectedAtTheScopePathBoundary() {
    assertThrows(IllegalArgumentException.class,
        () -> ScopePath.of(1, 2, 3, 4, 5, 6, 7, 8, 9));
    assertEquals(7, deepScope().depth(), "eight components is depth 7, the protocol maximum");
  }

  @Test void maxComponentValuesNestAtFullDepth() throws Exception {
    var scope = ScopePath.of(U32_MAX, U32_MAX, U32_MAX, U32_MAX, U32_MAX, U32_MAX, U32_MAX, U32_MAX);
    byte[] payload = bytes("max-depth-max-component");
    try (ObjectStore store = FileObjectStore.open(directory)) {
      store.put(scope, U32_MAX, payload, digest(payload));
      store.sealScope(scope, StoreLayout.scopeDigest(
          List.of(new StoreLayout.EntityDigest(U32_MAX, digest(payload)))), 1);
      store.verifyScope(scope);
      assertEquals(List.of(scope), store.listScopes());
    }
  }

  @Test void foreignChannelHoldingTheWriterLockIsRefusedAsFailed() throws Exception {
    Path objects = directory.resolve("objects");
    Files.createDirectories(objects);
    try (FileChannel foreign = FileChannel.open(objects.resolve("writer.lock"),
        StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
      // A foreign fcntl(F_SETLK) locker (the cross-language double-writer guard) holds
      // the lock; the store must refuse with FAILED, not hang or interlock silently.
      foreign.lock();
      assertStorage(StorageException.Kind.FAILED, () -> FileObjectStore.open(directory));
    }
    try (ObjectStore store = FileObjectStore.open(directory)) {
      assertEquals(List.of(), store.listScopes());
    }
  }

  @Test void foreignChannelHoldingTheJournalWriterLockIsRefusedAsFailed() throws Exception {
    Path journal = directory.resolve("journal");
    Files.createDirectories(journal);
    try (FileChannel foreign = FileChannel.open(journal.resolve("writer.lock"),
        StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
      foreign.lock();
      assertStorage(StorageException.Kind.FAILED, () -> FileJournal.open(directory));
    }
    try (Journal reopened = FileJournal.open(directory)) {
      assertEquals(List.of(), reopened.replay(0));
    }
  }

  @Test void objectsDirectoryReplacedByAFileMapsToTransient() throws Exception {
    Files.createDirectories(directory);
    Files.write(directory.resolve("objects"), bytes("not a directory"));
    StorageException failure = assertThrows(StorageException.class, () -> FileObjectStore.open(directory));
    assertEquals(StorageException.Kind.TRANSIENT, failure.kind(), failure.getMessage());
  }

  @Test void journalDirectoryReplacedByAFileMapsToTransient() throws Exception {
    Files.createDirectories(directory);
    Files.write(directory.resolve("journal"), bytes("not a directory"));
    StorageException failure = assertThrows(StorageException.class, () -> FileJournal.open(directory));
    assertEquals(StorageException.Kind.TRANSIENT, failure.kind(), failure.getMessage());
  }

  @Test void journalLogReplacedByADirectoryMapsToTransient() throws Exception {
    Files.createDirectories(directory.resolve("journal").resolve("journal.log"));
    StorageException failure = assertThrows(StorageException.class, () -> FileJournal.open(directory));
    assertEquals(StorageException.Kind.TRANSIENT, failure.kind(), failure.getMessage());
  }

  @Test void garbageRegularFileInsideAScopeLeafIsCorrupt() throws Exception {
    var scope = ScopePath.root(0xc004);
    byte[] payload = bytes("clean");
    try (ObjectStore store = FileObjectStore.open(directory)) {
      store.put(scope, 1, payload, digest(payload));
    }
    Path leaf = entityFile(directory, scope, 1).getParent().getParent();
    Files.write(leaf.resolve("junk.txt"), bytes("garbage"));
    try (ObjectStore store = FileObjectStore.open(directory)) {
      assertStorage(StorageException.Kind.CORRUPT, () -> store.list(scope));
      assertStorage(StorageException.Kind.CORRUPT, () -> store.listScopes());
    }
  }

  @Test void garbageDirectoryInsideAScopeLeafIsCorrupt() throws Exception {
    var scope = ScopePath.root(0xc005);
    byte[] payload = bytes("clean");
    try (ObjectStore store = FileObjectStore.open(directory)) {
      store.put(scope, 1, payload, digest(payload));
    }
    Path leaf = entityFile(directory, scope, 1).getParent().getParent();
    Files.createDirectory(leaf.resolve("zz"));
    try (ObjectStore store = FileObjectStore.open(directory)) {
      assertStorage(StorageException.Kind.CORRUPT, () -> store.list(scope));
    }
    // A three-hex-digit directory matches neither bucket nor scope naming.
    Files.delete(leaf.resolve("zz"));
    Files.createDirectory(leaf.resolve("abc"));
    try (ObjectStore store = FileObjectStore.open(directory)) {
      assertStorage(StorageException.Kind.CORRUPT, () -> store.list(scope));
    }
  }

  @Test void foreignFilePlantedInsideABucketIsCorrupt() throws Exception {
    var scope = ScopePath.root(0xc006);
    byte[] payload = bytes("clean");
    try (ObjectStore store = FileObjectStore.open(directory)) {
      store.put(scope, 1, payload, digest(payload));
      store.sealScope(scope, StoreLayout.scopeDigest(
          List.of(new StoreLayout.EntityDigest(1, digest(payload)))), 1);
    }
    Path bucket = entityFile(directory, scope, 1).getParent();
    Files.write(bucket.resolve("intruder"), bytes("garbage"));
    try (ObjectStore store = FileObjectStore.open(directory)) {
      assertStorage(StorageException.Kind.CORRUPT, () -> store.list(scope));
      assertStorage(StorageException.Kind.CORRUPT, () -> store.verifyScope(scope));
    }
  }

  @Test void forgedEightHexFileAtScopeLevelIsListedButVerificationMisclassifiesIt() throws Exception {
    var scope = ScopePath.root(0xc007);
    byte[] payload = bytes("clean");
    try (ObjectStore store = FileObjectStore.open(directory)) {
      store.put(scope, 1, payload, digest(payload));
      store.sealScope(scope, StoreLayout.scopeDigest(
          List.of(new StoreLayout.EntityDigest(1, digest(payload)))), 1);
    }
    // list() names files by their 8-hex object name without rehashing, so the forged
    // id appears in the audit listing; verifyScope then looks for that id under its
    // fanout bucket, finds nothing, and maps the miss to retryable TRANSIENT instead
    // of CORRUPT — pinned here and listed as a bug (the file is on-disk corruption,
    // not an I/O failure, and a retry loop would spin forever).
    Path leaf = entityFile(directory, scope, 1).getParent().getParent();
    Files.write(leaf.resolve("deadbeef"), bytes("not an entity file"));
    try (ObjectStore store = FileObjectStore.open(directory)) {
      assertEquals(List.of(1L, 0xdeadbeefL), store.list(scope));
      StorageException failure = assertThrows(StorageException.class, () -> store.verifyScope(scope));
      assertEquals(StorageException.Kind.TRANSIENT, failure.kind(), failure.getMessage());
    }
  }

  @Test void regularFileAtTheBackendRootIsIgnoredByScopeListing() throws Exception {
    var scope = ScopePath.root(0xc008);
    byte[] payload = bytes("clean");
    try (ObjectStore store = FileObjectStore.open(directory)) {
      store.put(scope, 1, payload, digest(payload));
      Files.write(directory.resolve("objects").resolve("README"), bytes("foreign file at the root"));
      assertEquals(List.of(scope), store.listScopes());
      assertEquals(List.of(1L), store.list(scope));
    }
  }

  @Test void symlinkedRootIsRefusedBecauseRootsMustBeRealDirectories() throws Exception {
    Path realRoot = directory.resolve("real-store");
    Files.createDirectories(realRoot);
    Path link = directory.resolve("linked-store");
    Files.createSymbolicLink(link, realRoot);
    // Roots are validated with NOFOLLOW_LINKS: a symlink, even to a real directory,
    // is refused as FAILED rather than silently resolved.
    assertStorage(StorageException.Kind.FAILED, () -> FileObjectStore.open(link));
    var scope = ScopePath.root(0xc009);
    byte[] payload = bytes("through-real-path");
    try (ObjectStore store = FileObjectStore.open(realRoot)) {
      store.put(scope, 1, payload, digest(payload));
    }
    try (ObjectStore store = FileObjectStore.open(realRoot)) {
      assertArrayEquals(payload, store.openRead(scope, 1).readAllBytes());
    }
    Path fileLink = directory.resolve("file-link");
    Files.createSymbolicLink(fileLink, directory.resolve("some-file"));
    Files.write(directory.resolve("some-file"), bytes("x"));
    assertStorage(StorageException.Kind.FAILED, () -> FileObjectStore.open(fileLink));
  }

  @Test void fanoutSpreadsEntityIdsAcrossBuckets() throws Exception {
    var scope = ScopePath.root(0xc00a);
    Set<String> buckets = new TreeSet<>();
    try (ObjectStore store = FileObjectStore.open(directory)) {
      for (long id = 1; id <= 64; id++) {
        byte[] payload = bytes("spread-" + id);
        store.put(scope, id, payload, digest(payload));
        buckets.add(StoreLayout.fanoutBucket(id));
      }
      assertTrue(buckets.size() >= 32,
          "64 ids hash into many of the 256 buckets, never one flat container: " + buckets.size());
      try (Stream<Path> walk = Files.walk(entityFile(directory, scope, 1).getParent().getParent())) {
        long entityFiles = walk.filter(path -> path.getFileName().toString().matches("[0-9a-f]{8}"))
            .filter(Files::isRegularFile).count();
        assertEquals(64, entityFiles);
      }
    }
  }
}
