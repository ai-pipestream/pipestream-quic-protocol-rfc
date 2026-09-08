package ai.pipestream.quic.v2;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

@Timeout(20)
final class OutputStoreTest {
  private static final InputStore.Limits LIMITS = new InputStore.Limits(4L << 20, 32, 1 << 20, 2);
  private static final UUID AUTHORITY = UUID.fromString("10000000-0000-0000-0000-000000000001");
  private static final Commitments.Context CONTEXT =
      new Commitments.Context("issuer-a", "alice", 1);

  @TempDir Path directory;

  @Test
  void zeroAndChunkedNonemptyOutputsRetainExactIdentityWithoutExtraQuotaAcrossReopen()
      throws Exception {
    Path root = directory.resolve("streamed");
    Records.InputHeader header = header(2, 9);
    ExecutionStore.Lease lease = lease(AUTHORITY, "alice", 1);
    InputStore.Usage funded;
    try (InputStore store = InputStore.initializeForAuthority(root, LIMITS, AUTHORITY)) {
      store.reserveOutputs(CONTEXT, header);
      funded = store.usage();
      try (OutputStore.Writer writer =
          store.beginOutput(CONTEXT, header, lease, 0, 0, "application/empty", 9)) {
        assertEquals(0, writer.finish().length());
      }
      byte[] payload = new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9};
      try (OutputStore.Writer writer =
          store.beginOutput(
              CONTEXT, header, lease, 1, payload.length, "application/octet-stream", 9)) {
        writer.write(ByteBuffer.wrap(payload, 0, 3));
        writer.write(ByteBuffer.wrap(payload, 3, 6));
        OutputStore.Stored stored = writer.finish();
        assertEquals(1, stored.index());
        assertEquals(payload.length, stored.length());
        assertEquals(digest(payload), stored.sha256());
        assertEquals("application/octet-stream", stored.contentType());
        assertEquals(CONTEXT, stored.context());
        assertEquals(header, stored.header());
        assertEquals(1, stored.attempt());
        assertEquals(1, stored.leaseNumber());
      }
      assertEquals(funded, store.usage());
    }
    try (InputStore reopened = InputStore.open(root, LIMITS)) {
      assertEquals(funded, reopened.usage());
      OutputStore.Stored stored = reopened.findOutput(CONTEXT, header, lease, 1).orElseThrow();
      try (InputStream input = stored.openStream()) {
        assertArrayEquals(new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9}, input.readAllBytes());
      }
      assertEquals(funded, reopened.usage());
    }
  }

  @Test
  void exactLengthObjectAggregateAndCountBoundsRefuseWithoutInstallingSlots() throws Exception {
    Path root = directory.resolve("bounds");
    Records.InputHeader header = header(2, 5);
    ExecutionStore.Lease lease = lease(AUTHORITY, "alice", 1);
    try (InputStore store = InputStore.initializeForAuthority(root, LIMITS, AUTHORITY)) {
      store.reserveOutputs(CONTEXT, header);
      assertThrows(
          ProtocolError.class,
          () -> store.beginOutput(CONTEXT, header, lease, 2, 0, "application/x", 5));
      assertThrows(
          ProtocolError.class,
          () -> store.beginOutput(CONTEXT, header, lease, 0, 6, "application/x", 5));

      try (OutputStore.Writer truncated =
          store.beginOutput(CONTEXT, header, lease, 0, 3, "application/x", 5)) {
        truncated.write(ByteBuffer.wrap(new byte[] {1, 2}));
        assertCode(ProtocolError.Code.INTEGRITY_ERROR, truncated::finish);
      }
      assertTrue(store.findOutput(CONTEXT, header, lease, 0).isEmpty());

      try (OutputStore.Writer over =
          store.beginOutput(CONTEXT, header, lease, 0, 3, "application/x", 5)) {
        assertCode(
            ProtocolError.Code.LIMIT_EXCEEDED,
            () -> over.write(ByteBuffer.wrap(new byte[] {1, 2, 3, 4})));
      }
      assertTrue(store.findOutput(CONTEXT, header, lease, 0).isEmpty());
      try (OutputStore.Writer exact =
          store.beginOutput(CONTEXT, header, lease, 0, 3, "application/x", 5)) {
        exact.write(ByteBuffer.wrap(new byte[] {1, 2, 3}));
        exact.finish();
      }
      assertThrows(
          ProtocolError.class,
          () -> store.beginOutput(CONTEXT, header, lease, 1, 3, "application/x", 5));
      assertTrue(store.findOutput(CONTEXT, header, lease, 1).isEmpty());
    }
  }

  @Test
  void handlesAndWorkerIdentityAreCheckedAndReleasedWithoutSlotReuse() throws Exception {
    Path root = directory.resolve("identity");
    InputStore.Limits oneHandle = new InputStore.Limits(4L << 20, 32, 1 << 20, 1);
    Records.InputHeader header = header(2, 8);
    ExecutionStore.Lease lease = lease(AUTHORITY, "alice", 1);
    try (InputStore store = InputStore.initializeForAuthority(root, oneHandle, AUTHORITY)) {
      store.reserveOutputs(CONTEXT, header);
      OutputStore.Writer active =
          store.beginOutput(CONTEXT, header, lease, 0, 4, "application/x", 4);
      try {
        assertThrows(
            ProtocolError.class,
            () -> store.beginOutput(CONTEXT, header, lease, 1, 4, "application/x", 4));
        assertThrows(IOException.class, store::close);
      } finally {
        active.close();
      }
      assertCode(
          ProtocolError.Code.CONFLICT,
          () ->
              store.beginOutput(
                  CONTEXT, header, lease(AUTHORITY, "bob", 1), 0, 4, "application/x", 4));
      assertThrows(
          IOException.class,
          () ->
              store.beginOutput(
                  CONTEXT,
                  header,
                  lease(UUID.fromString("20000000-0000-0000-0000-000000000002"), "alice", 1),
                  0,
                  4,
                  "application/x",
                  4));
      try (OutputStore.Writer completed =
          store.beginOutput(CONTEXT, header, lease, 0, 4, "application/x", 4)) {
        completed.write(ByteBuffer.wrap(new byte[] {1, 2, 3, 4}));
        OutputStore.Stored stored = completed.finish();
        try (InputStream reader = stored.openStream()) {
          assertThrows(IOException.class, store::close);
          assertEquals(1, reader.read());
          assertCode(ProtocolError.Code.LIMIT_EXCEEDED, stored::openStream);
        }
      }
      assertCode(
          ProtocolError.Code.CONFLICT,
          () ->
              store.beginOutput(
                  CONTEXT, header, lease(AUTHORITY, "alice", 2), 0, 4, "application/x", 4));
    }
  }

  private static Records.InputHeader header(int outputs, long total) throws Exception {
    byte[] operation = new byte[16];
    operation[15] = 1;
    return new Records.InputHeader(
        1,
        new Records.OperationId(operation),
        new Records.AdmitParameters(
            new Records.WorkKey(0, 0, 1),
            new Records.Input(0, digest(new byte[0]), "application/octet-stream"),
            "copy",
            0,
            1000,
            new Records.OutputBudget(outputs, total)));
  }

  private static ExecutionStore.Lease lease(UUID installation, String owner, long number) {
    return new ExecutionStore.Lease(
        installation, owner, 1, new Records.WorkKey(0, 0, 1), 1, number, 2000);
  }

  private static void assertCode(ProtocolError.Code expected, ThrowingRunnable action) {
    ProtocolError error = assertThrows(ProtocolError.class, action::run);
    assertEquals(expected, error.code(), error::getMessage);
  }

  @FunctionalInterface
  private interface ThrowingRunnable {
    void run() throws Exception;
  }

  private static Records.Digest digest(byte[] payload) throws Exception {
    return new Records.Digest(MessageDigest.getInstance("SHA-256").digest(payload));
  }
}
