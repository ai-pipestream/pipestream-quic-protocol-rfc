package ai.pipestream.quic.storage;

import static ai.pipestream.quic.storage.ObjectStoreContractTest.assertStorage;
import static org.junit.jupiter.api.Assertions.*;

import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Leak audit for {@link FileObjectStore}: file-descriptor growth across
 * open/close and operation cycles (counted through {@code /proc/self/fd}),
 * scratch-file hygiene on every error path (failed put, failed seal,
 * conflicting put, sealed put, truncated read target), the double-writer
 * refusal path, and the guarantee — the original sin of this layer — that no
 * write ever escapes the configured root. Content is derived from loop
 * indices; there is no randomness and no wall-clock waiting.
 */
final class LeakAuditFileObjectStoreTest {
  @TempDir Path directory;

  private static byte[] payload(int salt) {
    return ("leak-audit-payload-" + salt).getBytes(java.nio.charset.StandardCharsets.UTF_8);
  }

  private static byte[] digest(byte[] bytes) {
    return StoreLayout.sha256().digest(bytes);
  }

  private static long fdCount() throws Exception {
    try (Stream<Path> fds = Files.list(Path.of("/proc/self/fd"))) {
      return fds.count();
    }
  }

  private static List<Path> regularFiles(Path root) throws Exception {
    try (Stream<Path> paths = Files.walk(root)) {
      return paths.filter(Files::isRegularFile).toList();
    }
  }

  @Test void openCloseCyclesDoNotGrowFileDescriptors() throws Exception {
    long before = fdCount();
    for (int cycle = 0; cycle < 500; cycle++) {
      FileObjectStore store = FileObjectStore.open(directory);
      store.close();
    }
    assertTrue(fdCount() - before <= 2,
        "500 open/close cycles leak no file descriptors");
  }

  @Test void openPutReadCyclesDoNotGrowFileDescriptors() throws Exception {
    var scope = ScopePath.root(0x0ace01);
    long before = fdCount();
    for (int cycle = 0; cycle < 60; cycle++) {
      try (ObjectStore store = FileObjectStore.open(directory)) {
        byte[] payload = payload(cycle);
        store.put(scope, cycle, payload, digest(payload));
        try (InputStream stream = store.openRead(scope, cycle)) {
          assertArrayEquals(payload, stream.readAllBytes());
        }
      }
    }
    assertTrue(fdCount() - before <= 2, "put/read cycles leak no file descriptors");
  }

  @Test void failedMutationPathsLeaveNoScratchAnywhereUnderTheRoot() throws Exception {
    var sealed = ScopePath.root(0x0ace02);
    var conflict = ScopePath.root(0x0ace06);
    var blocked = ScopePath.root(0x0ace07);
    byte[] first = payload(1);
    byte[] conflicting = payload(2);
    Path blockedBucket = directory
        .resolve("objects")
        .resolve(StoreLayout.scopeComponentName(0x0ace07))
        .resolve(StoreLayout.fanoutBucket(3));
    Files.createDirectories(blockedBucket);
    Set<PosixFilePermission> writable = Files.getPosixFilePermissions(blockedBucket);
    long before = fdCount();
    try (ObjectStore store = FileObjectStore.open(directory)) {
      store.put(sealed, 7, first, digest(first));
      store.sealScope(sealed, digest(first), 1);
      store.put(conflict, 7, first, digest(first));
      for (int round = 0; round < 30; round++) {
        final int sealedId = 100 + round;
        // Pinned decision 4: a sealed scope rejects put before the digest is
        // even considered, regardless of bytes.
        assertStorage(StorageException.Kind.ALREADY_SEALED,
            () -> store.put(sealed, sealedId, first, digest(first)));
        assertStorage(StorageException.Kind.CORRUPT,
            () -> store.put(conflict, 7, conflicting, digest(conflicting)));
        assertStorage(StorageException.Kind.CORRUPT,
            () -> store.put(conflict, 8, conflicting, digest(first)));
        assertStorage(StorageException.Kind.CORRUPT,
            () -> store.sealScope(sealed, digest(conflicting), 1));
        assertStorage(StorageException.Kind.NOT_FOUND,
            () -> store.retire(ScopePath.root(0x0ace99), 0));
      }
      Files.setPosixFilePermissions(blockedBucket,
          Set.of(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_EXECUTE));
      try {
        // Only entity 3 maps to the blocked bucket; every attempt fails in
        // the scratch CREATE_NEW with EACCES, mapped to TRANSIENT.
        for (int round = 0; round < 30; round++) {
          assertStorage(StorageException.Kind.TRANSIENT,
              () -> store.put(blocked, 3, first, digest(first)));
        }
      } finally {
        Files.setPosixFilePermissions(blockedBucket, writable);
      }
    }
    assertTrue(fdCount() - before <= 2, "failed mutations leak no file descriptors");
    for (Path file : regularFiles(directory)) {
      assertTrue(file.startsWith(directory), "write escaped the configured root: " + file);
      String name = file.getFileName().toString();
      assertFalse(name.endsWith(".tmp"), "scratch left behind: " + file);
    }
    try (ObjectStore reopened = FileObjectStore.open(directory)) {
      assertEquals(List.of(7L), reopened.list(sealed), "no phantom entities from failed puts");
      assertEquals(List.of(7L), reopened.list(conflict), "conflicting puts left only the original");
    }
  }

  @Test void openReadOnTruncatedEntityReleasesItsChannel() throws Exception {
    var scope = ScopePath.root(0x0ace03);
    Path target = directory
        .resolve("objects")
        .resolve(StoreLayout.scopeComponentName(0x0ace03))
        .resolve(StoreLayout.fanoutBucket(9))
        .resolve(StoreLayout.entityName(9));
    Files.createDirectories(target.getParent());
    Files.write(target, new byte[] {1, 2, 3});
    long before = fdCount();
    try (ObjectStore store = FileObjectStore.open(directory)) {
      for (int round = 0; round < 200; round++) {
        assertStorage(StorageException.Kind.CORRUPT, () -> store.openRead(scope, 9));
      }
    }
    assertTrue(fdCount() - before <= 2,
        "truncated-entity openRead leaks no channel (fd leak in the size check path)");
  }

  @Test void doubleWriterRefusalIsDescriptorNeutralAndDoesNotPoisonTheRoot() throws Exception {
    long before = fdCount();
    ObjectStore first = FileObjectStore.open(directory);
    for (int round = 0; round < 100; round++) {
      assertStorage(StorageException.Kind.FAILED, () -> FileObjectStore.open(directory));
    }
    assertTrue(fdCount() - before <= 2, "refused opens leak no file descriptors");
    first.close();
    try (ObjectStore second = FileObjectStore.open(directory)) {
      assertEquals(List.of(), second.listScopes());
    }
  }

  @Test void concurrentPutsNeverEscapeTheRootAndLeaveNoScratch() throws Exception {
    var scope = ScopePath.root(0x0ace04);
    int threads = 8;
    int putsPerThread = 24;
    ExecutorService pool = Executors.newFixedThreadPool(threads);
    CountDownLatch start = new CountDownLatch(1);
    CountDownLatch done = new CountDownLatch(threads);
    try (ObjectStore store = FileObjectStore.open(directory)) {
      for (int worker = 0; worker < threads; worker++) {
        int base = worker * putsPerThread;
        pool.submit(() -> {
          try {
            start.await();
            for (int offset = 0; offset < putsPerThread; offset++) {
              int id = base + offset;
              byte[] payload = payload(id);
              store.put(scope, id, payload, digest(payload));
            }
          } catch (Exception failure) {
            fail("concurrent put failed: " + failure);
          } finally {
            done.countDown();
          }
        });
      }
      start.countDown();
      assertTrue(done.await(60, TimeUnit.SECONDS), "all concurrent puts finished");
    } finally {
      pool.shutdownNow();
    }
    try (ObjectStore counted = FileObjectStore.open(directory)) {
      assertEquals(threads * putsPerThread, counted.list(scope).size(),
          "every concurrent put landed exactly once");
    }
    for (Path file : regularFiles(directory)) {
      assertTrue(file.startsWith(directory), "write escaped the configured root: " + file);
      assertFalse(file.getFileName().toString().endsWith(".tmp"), "scratch left behind: " + file);
    }
  }

  @Test void closeWithUnclosedReadStreamStillReleasesTheWriterLock() throws Exception {
    var scope = ScopePath.root(0x0ace05);
    byte[] payload = payload(5);
    long before = fdCount();
    InputStream unclosed;
    ObjectStore store = FileObjectStore.open(directory);
    store.put(scope, 5, payload, digest(payload));
    unclosed = store.openRead(scope, 5);
    store.close();
    try (ObjectStore reopened = FileObjectStore.open(directory)) {
      assertEquals(List.of(5L), reopened.list(scope), "store lock released despite open read");
    }
    unclosed.close();
    assertTrue(fdCount() - before <= 2,
        "closing the reader leaks nothing; the store close leaked no lock descriptor");
  }

  @Test void retireSealCyclesLeaveNoScratchAndNoDescriptors() throws Exception {
    long before = fdCount();
    for (int cycle = 0; cycle < 20; cycle++) {
      var scope = ScopePath.root(0x0ace10 + cycle);
      byte[] payload = payload(cycle);
      try (ObjectStore store = FileObjectStore.open(directory)) {
        store.put(scope, 1, payload, digest(payload));
        store.sealScope(scope, StoreLayout.scopeDigest(
            List.of(new StoreLayout.EntityDigest(1, digest(payload)))), 1);
        store.retire(scope, 0);
        byte[] second = payload(1000 + cycle);
        byte[] third = payload(2000 + cycle);
        store.put(scope, 2, second, digest(second));
        store.put(scope, 3, third, digest(third));
        store.retire(scope, 2);
        assertEquals(List.of(3L), store.list(scope), "partial retire keeps the newer entity");
        store.retire(scope, Long.MAX_VALUE);
      }
    }
    assertTrue(fdCount() - before <= 2, "retire cycles leak no file descriptors");
    for (Path file : regularFiles(directory)) {
      assertFalse(file.getFileName().toString().endsWith(".tmp"), "scratch left behind: " + file);
    }
    try (ObjectStore reopened = FileObjectStore.open(directory)) {
      assertEquals(List.of(), reopened.listScopes(), "retired scopes leave nothing behind");
    }
  }
}
