package ai.pipestream.quic.v2;

import static ai.pipestream.quic.v2.Messages.DURABLE_WORK;
import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

@Timeout(15)
final class InputStoreFundingTest {
  private static final Messages.Capabilities SELECTED =
      new Messages.Capabilities(
          true, List.of(DURABLE_WORK), List.of(), 65536, 4, 16, 1 << 20, 1000, 5000);
  private static final Commitments.Context CONTEXT =
      new Commitments.Context("issuer-a", "alice", 1);

  @TempDir Path directory;

  @Test
  void outputFundingChargesExactBytesAndNamesAndReplayChangesNothing() throws Exception {
    Path root = directory.resolve("funding");
    InputStore.Limits limits = new InputStore.Limits(1 << 20, 32, 1024, 4);
    Records.InputHeader header = header(1, 2, 17);
    try (InputStore store = InputStore.initialize(root, limits)) {
      InputStore.Usage before = store.usage();
      InputStore.Reservation first = store.reserveOutputs(CONTEXT, header);
      Path record = only(root.resolve("reservations"), ".funding");
      long recordBytes = Files.size(record);
      long expectedBytes = recordBytes + 2 * (17 + 2 * 8236L);
      assertEquals(new InputStore.Usage(before.bytes() + expectedBytes, 5, 0), store.usage());
      assertEquals(header, first.header());
      assertEquals(CONTEXT, first.context());
      assertEquals(record.getFileName().toString(), first.reference());

      InputStore.Usage funded = store.usage();
      InputStore.Reservation replay = store.reserveOutputs(CONTEXT, header);
      assertEquals(first.reference(), replay.reference());
      assertEquals(funded, store.usage());
      assertEquals(
          first.reference(), store.findReservation(CONTEXT, header).orElseThrow().reference());
      assertEquals(funded, store.usage());
    }
    try (InputStore reopened = InputStore.open(root, limits)) {
      assertEquals(header, reopened.findReservation(CONTEXT, header).orElseThrow().header());
      assertEquals(5, reopened.usage().files());
    }
  }

  @Test
  void fundedCapacityCannotBeSpentByOrdinaryInputAndConflictsAreAtomic() throws Exception {
    Records.InputHeader header = header(2, 1, 31);
    Path measureRoot = directory.resolve("measure");
    long charge;
    long recordBytes;
    try (InputStore measure =
        InputStore.initialize(measureRoot, new InputStore.Limits(1 << 20, 16, 1024, 4))) {
      measure.reserveOutputs(CONTEXT, header);
      charge = measure.usage().bytes();
      recordBytes = Files.size(only(measureRoot.resolve("reservations"), ".funding"));
    }

    Path root = directory.resolve("tight");
    InputStore.Limits tight = new InputStore.Limits(charge + recordBytes, 4, 1024, 4);
    try (InputStore store = InputStore.initialize(root, tight)) {
      store.reserveOutputs(CONTEXT, header);
      InputStore.Usage funded = store.usage();
      assertEquals(charge, funded.bytes());
      assertEquals(3, funded.files());
      assertEquals(recordBytes, tight.bytes() - funded.bytes());
      assertEquals(1, tight.files() - funded.files());
      assertCode(
          ProtocolError.Code.LIMIT_EXCEEDED,
          () -> store.begin(CONTEXT, header(3, 0, 0), SELECTED, 1));
      assertEquals(funded, store.usage());

      Records.InputHeader changed = header(2, 1, 32);
      assertCode(ProtocolError.Code.CONFLICT, () -> store.reserveOutputs(CONTEXT, changed));
      assertCode(ProtocolError.Code.CONFLICT, () -> store.findReservation(CONTEXT, changed));
      assertEquals(funded, store.usage());
      assertEquals(header, store.findReservation(CONTEXT, header).orElseThrow().header());
    }
  }

  @Test
  void zeroOutputsAreFundedAndOverflowLeavesUsageUnchanged() throws Exception {
    Path root = directory.resolve("bounds");
    InputStore.Limits limits = new InputStore.Limits(1 << 20, 16, 1024, 4);
    try (InputStore store = InputStore.initialize(root, limits)) {
      Records.InputHeader empty = header(4, 0, 0);
      store.reserveOutputs(CONTEXT, empty);
      InputStore.Usage retained = store.usage();
      assertEquals(1, retained.files());
      assertTrue(retained.bytes() > 0);
      assertEquals(empty, store.findReservation(CONTEXT, empty).orElseThrow().header());

      Records.InputHeader overflow = header(5, 256, Long.MAX_VALUE);
      assertCode(ProtocolError.Code.LIMIT_EXCEEDED, () -> store.reserveOutputs(CONTEXT, overflow));
      assertEquals(retained, store.usage());
      assertTrue(store.findReservation(CONTEXT, overflow).isEmpty());
    }
  }

  @Test
  void corruptFundingRefusesRecoveryBeforeAbandonedPendingCleanup() throws Exception {
    Path root = directory.resolve("corrupt");
    InputStore.Limits limits = new InputStore.Limits(1 << 20, 16, 1024, 4);
    Path pending = root.resolve("pending").resolve("00000000-0000-0000-0000-000000000001.part");
    try (InputStore store = InputStore.initialize(root, limits)) {
      store.reserveOutputs(CONTEXT, header(6, 1, 8));
      Path funding = only(root.resolve("reservations"), ".funding");
      byte[] damaged = Files.readAllBytes(funding);
      damaged[damaged.length - 1] ^= 1;
      Files.write(funding, damaged);
      Files.write(pending, new byte[] {1});
    }

    assertThrows(java.io.IOException.class, () -> InputStore.open(root, limits));
    assertTrue(Files.exists(pending), "recovery must validate retained funding before cleanup");
  }

  private static Records.InputHeader header(int operation, int outputs, long outputBytes)
      throws Exception {
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
            new Records.OutputBudget(outputs, outputBytes)));
  }

  private static Path only(Path directory, String suffix) throws Exception {
    try (DirectoryStream<Path> entries = Files.newDirectoryStream(directory, "*" + suffix)) {
      var iterator = entries.iterator();
      assertTrue(iterator.hasNext());
      Path result = iterator.next();
      assertFalse(iterator.hasNext());
      return result;
    }
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
