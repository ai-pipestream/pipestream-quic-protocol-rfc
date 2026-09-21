package ai.pipestream.quic.v2;

import static ai.pipestream.quic.v2.Messages.DURABLE_WORK;
import static org.junit.jupiter.api.Assertions.*;

import ai.pipestream.quic.storage.FileObjectStore;
import ai.pipestream.quic.storage.ObjectStore;
import ai.pipestream.quic.storage.ScopePath;
import ai.pipestream.quic.storage.StoreLayout;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Adoption plan stage 3 proofs for the SPI input payload plane: the full
 * reception, identity, quota, orphan, and recovery semantics of {@link
 * InputStore} with payload bytes on an {@link ObjectStore} backend and
 * envelope sidecars preserving the grammar. The existing suites exercise the
 * v2 plane; this suite exercises the SPI plane, including the kill/resume row
 * for the window between the backend put and the sidecar write.
 */
final class InputStoreSpiPlaneTest {
  private static final InputStore.Limits LIMITS = new InputStore.Limits(16L << 20, 32, 8 << 20, 8);
  private static final Messages.Capabilities SELECTED =
      new Messages.Capabilities(
          true, List.of(DURABLE_WORK), List.of(), 65536, 4, 16, 1 << 20, 1000, 5000);

  @TempDir Path directory;

  @Test
  void installFindReadAndReopenWithPayloadInTheBackend() throws Exception {
    Path root = directory.resolve("store");
    Path spiRoot = directory.resolve("spi");
    ObjectStore backend = FileObjectStore.open(spiRoot);
    byte[] payload = pattern(1021, 7);
    Commitments.Context context = context("alice", 1);
    Records.InputHeader header = header(1, payload, "spi-plane");
    try (InputStore store =
        InputStore.initializeForAuthority(root, LIMITS, java.util.UUID.randomUUID(), backend)) {
      try (InputStore.Receiver receiver = store.begin(context, header, SELECTED, 1)) {
        receiver.write(ByteBuffer.wrap(payload), 2);
        assertArrayEquals(payload, read(receiver.finish(3)));
      }
      Path sidecar = only(root.resolve("objects"), "*.input");
      byte[] retained = Files.readAllBytes(sidecar);
      assertArrayEquals(
          new byte[] {'P', 'S', 'J', 'V', '2', 'I', 'S', 'P'}, java.util.Arrays.copyOf(retained, 8),
          "identity sidecar carries the SPI magic");
      assertTrue(retained.length < 512, "sidecar holds metadata only, payload bytes live in the backend");
      assertArrayEquals(payload, readBackendEntity(spiRoot, store.spiScopeFor(sidecar)));
    }
    try (ObjectStore reopenedBackend = FileObjectStore.open(spiRoot);
        InputStore reopened =
            InputStore.open(root, LIMITS, reopenedBackend, null)) {
      InputStore.Stored found = reopened.find(context, header).orElseThrow();
      assertArrayEquals(payload, read(found));
    }
  }

  @Test
  void duplicateFinishKeepsQuotaStable() throws Exception {
    Path root = directory.resolve("duplicate");
    Path spiRoot = directory.resolve("duplicate-spi");
    ObjectStore backend = FileObjectStore.open(spiRoot);
    byte[] payload = pattern(257, 3);
    Commitments.Context context = context("alice", 1);
    Records.InputHeader header = header(2, payload, "spi-plane");
    try (InputStore store =
        InputStore.initializeForAuthority(root, LIMITS, java.util.UUID.randomUUID(), backend)) {
      try (InputStore.Receiver receiver = store.begin(context, header, SELECTED, 1)) {
        receiver.write(ByteBuffer.wrap(payload), 2);
        read(receiver.finish(3));
      }
      InputStore.Usage before = store.usage();
      try (InputStore.Receiver duplicate = store.begin(context, header, SELECTED, 4)) {
        duplicate.write(ByteBuffer.wrap(payload), 5);
        assertArrayEquals(payload, read(duplicate.finish(6)));
      }
      assertEquals(before, store.usage(), "identical re-install is a no-op for quota");
      assertEquals(1, count(root.resolve("objects"), "*.input"));
    }
  }

  @Test
  void reclaimInputRemovesSidecarAndPayloadTogether() throws Exception {
    Path root = directory.resolve("reclaim");
    Path spiRoot = directory.resolve("reclaim-spi");
    ObjectStore backend = FileObjectStore.open(spiRoot);
    byte[] payload = pattern(91, 5);
    Commitments.Context context = context("alice", 1);
    Records.InputHeader header = header(3, payload, "spi-plane");
    try (InputStore store =
        InputStore.initializeForAuthority(root, LIMITS, java.util.UUID.randomUUID(), backend)) {
      try (InputStore.Receiver receiver = store.begin(context, header, SELECTED, 1)) {
        receiver.write(ByteBuffer.wrap(payload), 2);
        read(receiver.finish(3));
      }
      Path sidecar = only(root.resolve("objects"), "*.input");
      InputStore.Usage before = store.usage();
      assertTrue(store.reclaimInput(context, header));
      assertEquals(0, count(root.resolve("objects"), "*.input"));
      assertThrows(IOException.class, () -> readBackendEntity(spiRoot, store.spiScopeFor(sidecar)));
      assertTrue(store.usage().bytes() < before.bytes(), "reclaim refunds the logical charge");
    }
  }

  @Test
  void orphanScanAndReclaimRemoveBothHalves() throws Exception {
    Path root = directory.resolve("orphan");
    Path spiRoot = directory.resolve("orphan-spi");
    ObjectStore backend = FileObjectStore.open(spiRoot);
    byte[] payload = pattern(63, 11);
    Commitments.Context context = context("alice", 1);
    Records.InputHeader header = header(4, payload, "spi-plane");
    try (InputStore store =
        InputStore.initializeForAuthority(root, LIMITS, java.util.UUID.randomUUID(), backend)) {
      try (InputStore.Receiver receiver = store.begin(context, header, SELECTED, 1)) {
        receiver.write(ByteBuffer.wrap(payload), 2);
        read(receiver.finish(3));
      }
      Path sidecar = only(root.resolve("objects"), "*.input");
      try (InputStore.OrphanScan scan = store.scanOrphans()) {
        InputStore.OrphanPage page = scan.nextPage(8);
        assertEquals(1, page.candidates().size());
        InputStore.OrphanCandidate candidate = page.candidates().get(0);
        assertFalse(candidate.funding());
        assertTrue(store.reclaimOrphan(candidate));
      }
      assertEquals(0, count(root.resolve("objects"), "*.input"));
      assertThrows(IOException.class, () -> readBackendEntity(spiRoot, store.spiScopeFor(sidecar)));
    }
  }

  @Test
  void recoverySweepsPayloadOrphanedWithoutItsSidecar() throws Exception {
    Path root = directory.resolve("sweep");
    Path spiRoot = directory.resolve("sweep-spi");
    ObjectStore backend = FileObjectStore.open(spiRoot);
    byte[] payload = pattern(77, 13);
    Commitments.Context context = context("alice", 1);
    Records.InputHeader header = header(5, payload, "spi-plane");
    Path sidecar;
    try (InputStore store =
        InputStore.initializeForAuthority(root, LIMITS, java.util.UUID.randomUUID(), backend)) {
      try (InputStore.Receiver receiver = store.begin(context, header, SELECTED, 1)) {
        receiver.write(ByteBuffer.wrap(payload), 2);
        read(receiver.finish(3));
      }
      sidecar = only(root.resolve("objects"), "*.input");
    }
    // A kill between the backend put and the sidecar write leaves exactly this state.
    Files.delete(sidecar);
    try (ObjectStore reopenedBackend = FileObjectStore.open(spiRoot);
        InputStore reopened =
            InputStore.open(root, LIMITS, reopenedBackend, null)) {
      assertTrue(reopened.find(context, header).isEmpty());
      assertThrows(IOException.class, () -> readBackendEntity(spiRoot, reopened.spiScopeFor(sidecar)),
          "orphan payload is swept during recovery");
    }
  }

  @Test
  void openRefusesSidecarWhosePayloadIsMissing() throws Exception {
    Path root = directory.resolve("missing-payload");
    Path spiRoot = directory.resolve("missing-payload-spi");
    ObjectStore backend = FileObjectStore.open(spiRoot);
    byte[] payload = pattern(55, 17);
    Commitments.Context context = context("alice", 1);
    Records.InputHeader header = header(6, payload, "spi-plane");
    Path sidecar;
    ScopePath scope;
    try (InputStore store =
        InputStore.initializeForAuthority(root, LIMITS, java.util.UUID.randomUUID(), backend)) {
      try (InputStore.Receiver receiver = store.begin(context, header, SELECTED, 1)) {
        receiver.write(ByteBuffer.wrap(payload), 2);
        read(receiver.finish(3));
      }
      sidecar = only(root.resolve("objects"), "*.input");
      scope = store.spiScopeFor(sidecar);
    }
    deleteBackendEntity(spiRoot, scope);
    try (ObjectStore reopenedBackend = FileObjectStore.open(spiRoot)) {
      assertThrows(IOException.class, () -> InputStore.open(root, LIMITS, reopenedBackend, null));
    }
  }

  @Test
  void killBetweenBackendPutAndSidecarLeavesSweptOrphan() throws Exception {
    Path root = directory.resolve("kill");
    Path spiRoot = directory.resolve("kill-spi");
    runKillChild(root, spiRoot, "LINKED");
    try (ObjectStore backend = FileObjectStore.open(spiRoot);
        InputStore reopened = InputStore.open(root, LIMITS, backend, null)) {
      assertNotNull(reopened);
      assertEquals(0, count(root.resolve("objects"), "*.input"), "no sidecar survived the kill");
      assertEquals(0, backendPayloadEntities(spiRoot), "orphan payload swept during recovery");
    }
  }

  @Test
  void killAfterObjectSyncResumesInstalled() throws Exception {
    Path root = directory.resolve("kill-late");
    Path spiRoot = directory.resolve("kill-late-spi");
    runKillChild(root, spiRoot, "OBJECT_SYNCED");
    try (ObjectStore backend = FileObjectStore.open(spiRoot);
        InputStore reopened = InputStore.open(root, LIMITS, backend, null)) {
      assertNotNull(reopened);
      assertEquals(1, count(root.resolve("objects"), "*.input"));
      assertEquals(1, backendPayloadEntities(spiRoot), "payload retained with its sidecar");
    }
  }

  private static void runKillChild(Path root, Path spiRoot, String phase) throws Exception {
    ProcessBuilder builder =
        new ProcessBuilder(
            "java",
            "-cp",
            System.getProperty("java.class.path"),
            SpiPlaneKillChild.class.getName(),
            root.toString(),
            spiRoot.toString(),
            phase);
    builder.redirectErrorStream(true);
    Process process = builder.start();
    process.getInputStream().readAllBytes();
    assertNotEquals(0, process.waitFor(), "child halted at " + phase);
  }

  private static int backendPayloadEntities(Path spiRoot) throws IOException {
    int count = 0;
    Path base = spiRoot.resolve("objects");
    if (!Files.isDirectory(base, LinkOption.NOFOLLOW_LINKS)) return 0;
    try (var walk = Files.walk(base)) {
      for (Path path : walk.filter(Files::isRegularFile).toList()) {
        if (path.getFileName().toString().matches("[0-9a-f]{8}")) count++;
      }
    }
    return count;
  }

  private static byte[] read(InputStore.Stored stored) throws IOException {
    try (InputStream input = stored.openStream()) {
      return input.readAllBytes();
    }
  }

  private static Commitments.Context context(String owner, long generation) {
    return new Commitments.Context("issuer-a", owner, generation);
  }

  private static Records.InputHeader header(int id, byte[] payload, String application)
      throws Exception {
    byte[] operation = new byte[16];
    operation[15] = (byte) id;
    return new Records.InputHeader(
        1,
        new Records.OperationId(operation),
        new Records.AdmitParameters(
            new Records.WorkKey(0, 0, id),
            new Records.Input(
                payload.length,
                new Records.Digest(MessageDigest.getInstance("SHA-256").digest(payload)),
                "application/octet-stream"),
            application,
            0,
            1000,
            new Records.OutputBudget(0, 0)));
  }

  private static byte[] pattern(int length, int seed) {
    byte[] bytes = new byte[length];
    for (int index = 0; index < length; index++) bytes[index] = (byte) (seed + index * 31);
    return bytes;
  }

  /** Reads the payload straight out of the filesystem backend at the derived scope. */
  private static byte[] readBackendEntity(Path spiRoot, ScopePath scope)
      throws IOException {
    byte[] file = Files.readAllBytes(entityPath(spiRoot, scope));
    ByteBuffer image = ByteBuffer.wrap(file);
    long length = image.getLong();
    byte[] payload = new byte[(int) length];
    image.position(8 + 32);
    image.get(payload);
    return payload;
  }

  private static void deleteBackendEntity(Path spiRoot, ScopePath scope)
      throws IOException {
    Files.delete(entityPath(spiRoot, scope));
  }

  private static Path entityPath(Path spiRoot, ScopePath scope)
      throws IOException {
    Path entity = spiRoot.resolve("objects");
    for (long component : scope.components()) {
      entity = entity.resolve(StoreLayout.scopeComponentName(component));
    }
    entity = entity.resolve(StoreLayout.fanoutBucket(0)).resolve(StoreLayout.entityName(0));
    if (!Files.isRegularFile(entity, LinkOption.NOFOLLOW_LINKS)) {
      throw new IOException("backend entity is absent for scope " + scope.render());
    }
    return entity;
  }

  private static Path only(Path directory, String glob) throws IOException {
    List<Path> matches;
    try (var entries = Files.newDirectoryStream(directory, glob)) {
      matches = java.util.stream.StreamSupport.stream(entries.spliterator(), false).toList();
    }
    assertEquals(1, matches.size(), "expected exactly one " + glob + " in " + directory);
    return matches.get(0);
  }

  private static int count(Path directory, String glob) throws IOException {
    try (var entries = Files.newDirectoryStream(directory, glob)) {
      return java.util.stream.StreamSupport.stream(entries.spliterator(), false)
          .mapToInt(entry -> 1)
          .sum();
    }
  }
}
