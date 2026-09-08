package ai.pipestream.quic.v2;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

@Timeout(20)
final class OutputStoreReclaimTest {
  // This low-level fixture supplies synthetic leases to exercise the trusted-caller storage
  // contract. Authority-committed replacement leases are covered by the recovery fixture.
  private static final UUID AUTHORITY = UUID.fromString("10000000-0000-0000-0000-000000000001");
  private static final InputStore.Limits LIMITS = new InputStore.Limits(4L << 20, 32, 1 << 20, 4);
  private static final Commitments.Context CONTEXT =
      new Commitments.Context("issuer-a", "alice", 1);

  @TempDir Path directory;

  @Test
  void strictlyNewerLeaseReclaimsAndReusesSlotsWithoutRefundingFunding() throws Exception {
    Path root = directory.resolve("reclaim");
    Records.InputHeader header = header();
    ExecutionStore.Lease first = lease(AUTHORITY, "alice", 1);
    ExecutionStore.Lease second = lease(AUTHORITY, "alice", 2);
    try (InputStore store = InputStore.initializeForAuthority(root, LIMITS, AUTHORITY)) {
      store.reserveOutputs(CONTEXT, header);
      InputStore.Usage funded = store.usage();
      write(store, header, first, new byte[] {1, 2, 3, 4});
      assertCode(ProtocolError.Code.CONFLICT, () -> store.reclaimOutputs(CONTEXT, header, first));
      IOException foreign =
          assertThrows(
              IOException.class,
              () ->
                  store.reclaimOutputs(
                      CONTEXT,
                      header,
                      lease(UUID.fromString("20000000-0000-0000-0000-000000000002"), "alice", 2)));
      assertTrue(foreign.getMessage().contains("different or no authority"), foreign::getMessage);
      assertTrue(store.findOutput(CONTEXT, header, first, 0).isPresent());

      store.reclaimOutputs(CONTEXT, header, second);
      assertTrue(store.findOutput(CONTEXT, header, first, 0).isEmpty());
      assertEquals(funded, store.usage());
      write(store, header, second, new byte[] {5, 6, 7, 8});
      try (var input = store.findOutput(CONTEXT, header, second, 0).orElseThrow().openStream()) {
        assertArrayEquals(new byte[] {5, 6, 7, 8}, input.readAllBytes());
      }
      assertEquals(funded, store.usage());
    }
  }

  @Test
  void liveReaderBlocksItsFundingWhileUnrelatedFundingCanReclaim() throws Exception {
    Path root = directory.resolve("pins");
    Records.InputHeader firstHeader = header();
    Records.InputHeader otherHeader = header(2);
    ExecutionStore.Lease first = lease(AUTHORITY, "alice", 1);
    ExecutionStore.Lease second = lease(AUTHORITY, "alice", 2);
    ExecutionStore.Lease otherFirst = lease(AUTHORITY, "alice", 1, new Records.WorkKey(0, 0, 2));
    ExecutionStore.Lease otherSecond = lease(AUTHORITY, "alice", 2, new Records.WorkKey(0, 0, 2));
    try (InputStore store = InputStore.initializeForAuthority(root, LIMITS, AUTHORITY)) {
      store.reserveOutputs(CONTEXT, firstHeader);
      store.reserveOutputs(CONTEXT, otherHeader);
      write(store, firstHeader, first, new byte[] {1, 2, 3, 4});
      write(store, otherHeader, otherFirst, new byte[] {5, 6, 7, 8});
      try (var reader =
          store.findOutput(CONTEXT, firstHeader, first, 0).orElseThrow().openStream()) {
        assertCode(
            ProtocolError.Code.CONFLICT, () -> store.reclaimOutputs(CONTEXT, firstHeader, second));
        store.reclaimOutputs(CONTEXT, otherHeader, otherSecond);
        assertTrue(store.findOutput(CONTEXT, otherHeader, otherFirst, 0).isEmpty());
        assertEquals(1, reader.read());
      }
      store.reclaimOutputs(CONTEXT, firstHeader, second);
      assertTrue(store.findOutput(CONTEXT, firstHeader, first, 0).isEmpty());
    }
  }

  @Test
  void futureAndCorruptTargetsAreRefusedBeforeAnyUnlink() throws Exception {
    Path futureRoot = directory.resolve("future");
    Records.InputHeader header = header();
    ExecutionStore.Lease first = lease(AUTHORITY, "alice", 1);
    ExecutionStore.Lease second = lease(AUTHORITY, "alice", 2);
    try (InputStore store = InputStore.initializeForAuthority(futureRoot, LIMITS, AUTHORITY)) {
      store.reserveOutputs(CONTEXT, header);
      write(store, header, second, new byte[] {1, 2, 3, 4});
      Path installed = onlyEntry(futureRoot.resolve("outputs"));
      assertCode(ProtocolError.Code.CONFLICT, () -> store.reclaimOutputs(CONTEXT, header, first));
      assertTrue(Files.exists(installed));
      assertTrue(store.findOutput(CONTEXT, header, second, 0).isPresent());
    }

    Path corruptRoot = directory.resolve("corrupt");
    try (InputStore store = InputStore.initializeForAuthority(corruptRoot, LIMITS, AUTHORITY)) {
      store.reserveOutputs(CONTEXT, header);
      write(store, header, first, new byte[] {5, 6, 7, 8});
      Path installed = onlyEntry(corruptRoot.resolve("outputs"));
      byte[] bytes = Files.readAllBytes(installed);
      bytes[bytes.length - 1] ^= 1;
      Files.write(installed, bytes);
      assertThrows(IOException.class, () -> store.reclaimOutputs(CONTEXT, header, second));
      assertTrue(Files.exists(installed));
      assertEquals(1, count(corruptRoot.resolve("outputs")));
    }
  }

  private static Path onlyEntry(Path directory) throws Exception {
    try (DirectoryStream<Path> entries = Files.newDirectoryStream(directory)) {
      var iterator = entries.iterator();
      Path result = iterator.next();
      assertFalse(iterator.hasNext());
      return result;
    }
  }

  private static int count(Path directory) throws Exception {
    int count = 0;
    try (DirectoryStream<Path> entries = Files.newDirectoryStream(directory)) {
      for (Path ignored : entries) count++;
    }
    return count;
  }

  private static void write(
      InputStore store, Records.InputHeader header, ExecutionStore.Lease lease, byte[] payload)
      throws Exception {
    try (OutputStore.Writer writer =
        store.beginOutput(
            CONTEXT, header, lease, 0, payload.length, "application/octet-stream", 4)) {
      writer.write(ByteBuffer.wrap(payload));
      writer.finish();
    }
  }

  private static Records.InputHeader header() throws Exception {
    return header(1);
  }

  private static Records.InputHeader header(int operation) throws Exception {
    byte[] id = new byte[16];
    id[15] = (byte) operation;
    return new Records.InputHeader(
        1,
        new Records.OperationId(id),
        new Records.AdmitParameters(
            new Records.WorkKey(0, 0, operation),
            new Records.Input(
                0,
                new Records.Digest(MessageDigest.getInstance("SHA-256").digest(new byte[0])),
                "application/octet-stream"),
            "copy",
            0,
            1000,
            new Records.OutputBudget(1, 4)));
  }

  private static ExecutionStore.Lease lease(UUID installation, String owner, long number) {
    return lease(installation, owner, number, new Records.WorkKey(0, 0, 1));
  }

  private static ExecutionStore.Lease lease(
      UUID installation, String owner, long number, Records.WorkKey work) {
    return new ExecutionStore.Lease(installation, owner, 1, work, 1, number, 2000);
  }

  private static void assertCode(ProtocolError.Code expected, Throwing action) {
    ProtocolError error = assertThrows(ProtocolError.class, action::run);
    assertEquals(expected, error.code(), error::getMessage);
  }

  @FunctionalInterface
  private interface Throwing {
    void run() throws Exception;
  }
}
