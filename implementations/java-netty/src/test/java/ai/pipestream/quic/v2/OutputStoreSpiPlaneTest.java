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
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Adoption plan stage 4 proofs for the SPI output payload plane: publication,
 * identity, funding parity, replacement reclaim, and recovery semantics of
 * {@link OutputStore} with payload bytes on an {@link ObjectStore} backend
 * and metadata sidecars preserving the name grammar. Funding stays entirely
 * v2, as the plan requires.
 */
final class OutputStoreSpiPlaneTest {
  private static final InputStore.Limits LIMITS = new InputStore.Limits(16L << 20, 64, 8 << 20, 16);
  private static final Messages.Capabilities SELECTED =
      new Messages.Capabilities(
          true, List.of(DURABLE_WORK), List.of(), 65536, 4, 16, 1 << 20, 1000, 5000);
  private static final Commitments.Context CONTEXT = new Commitments.Context("issuer-a", "alice", 1);

  @TempDir Path directory;

  @Test
  void publishFindReadVerifyAndReopenWithPayloadInTheBackend() throws Exception {
    Path root = directory.resolve("store");
    Path spiRoot = directory.resolve("spi");
    UUID authority = UUID.randomUUID();
    ObjectStore backend = FileObjectStore.open(spiRoot);
    byte[] payload = pattern(1021, 7);
    Records.InputHeader header = header(1, 2, 8192);
    try (InputStore store =
        InputStore.initializeForAuthority(root, LIMITS, authority, backend)) {
      store.reserveOutputs(CONTEXT, header);
      OutputStore.Stored published;
      try (OutputStore.Writer writer =
          store.beginOutput(
              CONTEXT, header, lease(authority, header, 1, 1), 0, payload.length,
              "application/octet-stream", 8192)) {
        writer.write(ByteBuffer.wrap(payload));
        published = writer.finish();
      }
      Path sidecar = root.resolve("outputs").resolve(published.reference());
      byte[] retained = Files.readAllBytes(sidecar);
      assertArrayEquals(
          new byte[] {'P', 'S', 'J', 'V', '2', 'O', 'S', 'P'}, java.util.Arrays.copyOf(retained, 8),
          "installed output is a metadata sidecar on the SPI plane");
      assertTrue(retained.length < 512, "payload bytes live in the backend");
      assertArrayEquals(payload, readBackendEntity(spiRoot, store.spiScopeFor(sidecar)));
      assertArrayEquals(payload, read(published));

      OutputStore.Stored found =
          store.findOutput(CONTEXT, header, lease(authority, header, 1, 1), 0).orElseThrow();
      assertArrayEquals(payload, read(found));
      store.verifyOutputCount(CONTEXT, header, lease(authority, header, 1, 1), 1);
    }
    try (ObjectStore reopenedBackend = FileObjectStore.open(spiRoot);
        InputStore reopened =
            InputStore.open(root, LIMITS, reopenedBackend, null)) {
      OutputStore.Stored found =
          reopened.findOutput(CONTEXT, header, lease(authority, header, 1, 1), 0).orElseThrow();
      assertArrayEquals(payload, read(found));
      reopened.verifyOutputCount(CONTEXT, header, lease(authority, header, 1, 1), 1);
    }
  }

  @Test
  void replacementReclaimRemovesSidecarAndPayloadTogether() throws Exception {
    Path root = directory.resolve("reclaim");
    Path spiRoot = directory.resolve("reclaim-spi");
    UUID authority = UUID.randomUUID();
    ObjectStore backend = FileObjectStore.open(spiRoot);
    byte[] payload = pattern(511, 3);
    Records.InputHeader header = header(2, 2, 8192);
    try (InputStore store =
        InputStore.initializeForAuthority(root, LIMITS, authority, backend)) {
      store.reserveOutputs(CONTEXT, header);
      String reference;
      try (OutputStore.Writer writer =
          store.beginOutput(
              CONTEXT, header, lease(authority, header, 2, 1), 0, payload.length,
              "application/octet-stream", 8192)) {
        writer.write(ByteBuffer.wrap(payload));
        reference = writer.finish().reference();
      }
      Path sidecar = root.resolve("outputs").resolve(reference);
      store.reclaimOutputs(CONTEXT, header, lease(authority, header, 2, 2));
      assertFalse(Files.exists(sidecar, LinkOption.NOFOLLOW_LINKS));
      assertThrows(IOException.class, () -> readBackendEntity(spiRoot, store.spiScopeFor(sidecar)));
      assertTrue(
          store.findOutput(CONTEXT, header, lease(authority, header, 2, 1), 0).isEmpty());
    }
  }

  @Test
  void recoverySweepsOutputPayloadOrphanedWithoutItsSidecar() throws Exception {
    Path root = directory.resolve("sweep");
    Path spiRoot = directory.resolve("sweep-spi");
    UUID authority = UUID.randomUUID();
    ObjectStore backend = FileObjectStore.open(spiRoot);
    byte[] payload = pattern(77, 11);
    Records.InputHeader header = header(3, 1, 4096);
    Path sidecar;
    try (InputStore store =
        InputStore.initializeForAuthority(root, LIMITS, authority, backend)) {
      store.reserveOutputs(CONTEXT, header);
      try (OutputStore.Writer writer =
          store.beginOutput(
              CONTEXT, header, lease(authority, header, 3, 1), 0, payload.length,
              "application/octet-stream", 4096)) {
        writer.write(ByteBuffer.wrap(payload));
        sidecar = root.resolve("outputs").resolve(writer.finish().reference());
      }
    }
    // A kill between the backend put and the sidecar write leaves exactly this state.
    Files.delete(sidecar);
    try (ObjectStore reopenedBackend = FileObjectStore.open(spiRoot);
        InputStore reopened =
            InputStore.open(root, LIMITS, reopenedBackend, null)) {
      assertNotNull(reopened);
      assertThrows(
          IOException.class, () -> readBackendEntity(spiRoot, reopened.spiScopeFor(sidecar)),
          "orphan output payload is swept during recovery");
    }
  }

  @Test
  void openRefusesOutputSidecarWhosePayloadIsMissing() throws Exception {
    Path root = directory.resolve("missing");
    Path spiRoot = directory.resolve("missing-spi");
    UUID authority = UUID.randomUUID();
    ObjectStore backend = FileObjectStore.open(spiRoot);
    byte[] payload = pattern(63, 13);
    Records.InputHeader header = header(4, 1, 4096);
    Path sidecar;
    ScopePath scope;
    try (InputStore store =
        InputStore.initializeForAuthority(root, LIMITS, authority, backend)) {
      store.reserveOutputs(CONTEXT, header);
      try (OutputStore.Writer writer =
          store.beginOutput(
              CONTEXT, header, lease(authority, header, 4, 1), 0, payload.length,
              "application/octet-stream", 4096)) {
        writer.write(ByteBuffer.wrap(payload));
        sidecar = root.resolve("outputs").resolve(writer.finish().reference());
        scope = store.spiScopeFor(sidecar);
      }
    }
    Files.delete(entityPath(spiRoot, scope));
    try (ObjectStore reopenedBackend = FileObjectStore.open(spiRoot)) {
      assertThrows(
          IOException.class, () -> InputStore.open(root, LIMITS, reopenedBackend, null));
    }
  }

  @Test
  void killBetweenOutputPutAndSidecarLeavesSweptOrphan() throws Exception {
    Path root = directory.resolve("kill");
    Path spiRoot = directory.resolve("kill-spi");
    runKillChild(root, spiRoot, "OUTPUT_LINKED");
    try (ObjectStore backend = FileObjectStore.open(spiRoot);
        InputStore reopened = InputStore.open(root, LIMITS, backend, null)) {
      assertNotNull(reopened);
      assertEquals(0, count(root.resolve("outputs"), "*.output"), "no sidecar survived the kill");
      assertEquals(0, backendPayloadEntities(spiRoot), "orphan output payload swept during recovery");
    }
  }

  @Test
  void killAfterOutputSyncResumesPublished() throws Exception {
    Path root = directory.resolve("kill-late");
    Path spiRoot = directory.resolve("kill-late-spi");
    runKillChild(root, spiRoot, "OUTPUT_SYNCED");
    try (ObjectStore backend = FileObjectStore.open(spiRoot);
        InputStore reopened = InputStore.open(root, LIMITS, backend, null)) {
      assertNotNull(reopened);
      assertEquals(1, count(root.resolve("outputs"), "*.output"));
      assertEquals(1, backendPayloadEntities(spiRoot), "payload retained with its sidecar");
    }
  }

  private static void runKillChild(Path root, Path spiRoot, String phase) throws Exception {
    ProcessBuilder builder =
        new ProcessBuilder(
            "java",
            "-cp",
            System.getProperty("java.class.path"),
            SpiOutputKillChild.class.getName(),
            root.toString(),
            spiRoot.toString(),
            phase);
    builder.redirectErrorStream(true);
    Process process = builder.start();
    process.getInputStream().readAllBytes();
    assertNotEquals(0, process.waitFor(), "child halted at " + phase);
  }

  private static ExecutionStore.Lease lease(
      UUID authority, Records.InputHeader header, long attempt, long number) {
    return new ExecutionStore.Lease(
        authority, "alice", 1, header.parameters().work(), attempt, number, 2000);
  }

  private static Records.InputHeader header(int id, int outputs, long outputBytes) throws Exception {
    byte[] operation = new byte[16];
    operation[15] = (byte) id;
    return new Records.InputHeader(
        1,
        new Records.OperationId(operation),
        new Records.AdmitParameters(
            new Records.WorkKey(0, 0, id),
            new Records.Input(0, new Records.Digest(new byte[32]), "application/octet-stream"),
            "spi-output-plane",
            0,
            1000,
            new Records.OutputBudget(outputs, outputBytes)));
  }

  private static byte[] pattern(int length, int seed) {
    byte[] bytes = new byte[length];
    for (int index = 0; index < length; index++) bytes[index] = (byte) (seed + index * 31);
    return bytes;
  }

  private static byte[] read(OutputStore.Stored stored) throws IOException {
    try (InputStream input = stored.openStream()) {
      return input.readAllBytes();
    }
  }

  private static byte[] readBackendEntity(Path spiRoot, ScopePath scope) throws IOException {
    byte[] file = Files.readAllBytes(entityPath(spiRoot, scope));
    ByteBuffer image = ByteBuffer.wrap(file);
    long length = image.getLong();
    byte[] payload = new byte[(int) length];
    image.position(8 + 32);
    image.get(payload);
    return payload;
  }

  private static Path entityPath(Path spiRoot, ScopePath scope) throws IOException {
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

  private static int count(Path directory, String glob) throws IOException {
    try (var entries = Files.newDirectoryStream(directory, glob)) {
      return java.util.stream.StreamSupport.stream(entries.spliterator(), false)
          .mapToInt(entry -> 1)
          .sum();
    }
  }
}
