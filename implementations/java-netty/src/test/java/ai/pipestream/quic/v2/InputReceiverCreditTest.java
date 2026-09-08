package ai.pipestream.quic.v2;

import static ai.pipestream.quic.v2.Messages.DURABLE_WORK;
import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

@Timeout(15)
final class InputReceiverCreditTest {
  private static final Messages.Capabilities SELECTED =
      new Messages.Capabilities(
          true, List.of(DURABLE_WORK), List.of(), 65536, 4, 16, 1 << 20, 1000, 5000);
  private static final InputStore.Limits ONE_HANDLE = new InputStore.Limits(1L << 20, 32, 1024, 1);

  @TempDir Path directory;

  @Test
  void reservedHandleCannotBeStolenAndFundsSequentialExactReceivers() throws Exception {
    try (InputStore store = InputStore.initialize(directory.resolve("sequential"), ONE_HANDLE)) {
      Commitments.Context context = context("alice");
      byte[] firstBytes = {1, 2, 3};
      byte[] secondBytes = {4, 5};
      Records.InputHeader first = header(1, firstBytes);
      Records.InputHeader second = header(2, secondBytes);
      InputStore.ReceiverCredit credit = store.reserveInputReceiver();
      assertEquals(new InputStore.Usage(0, 0, 1), store.usage());
      assertCode(ProtocolError.Code.LIMIT_EXCEEDED, () -> store.begin(context, first, SELECTED, 1));

      try (InputStore.Receiver receiver = store.begin(context, first, SELECTED, 2, credit)) {
        assertEquals(1, store.usage().handles());
        assertThrows(IOException.class, credit::close);
        assertEquals(1, store.usage().handles());
        assertCode(
            ProtocolError.Code.CONFLICT, () -> store.begin(context, second, SELECTED, 3, credit));
        receiver.write(ByteBuffer.wrap(firstBytes), 4);
        assertEquals(first, receiver.finish(5).header());
      }
      assertEquals(1, store.usage().handles());
      InputStore.Usage afterFirst = store.usage();

      try (InputStore.Receiver receiver =
          store.begin(context("bob"), second, SELECTED, 6, credit)) {
        receiver.write(ByteBuffer.wrap(secondBytes), 7);
        InputStore.Stored stored = receiver.finish(8);
        assertEquals(context("bob"), stored.context());
        assertEquals(second, stored.header());
      }
      assertEquals(1, store.usage().handles());
      assertTrue(store.usage().bytes() > afterFirst.bytes());
      assertTrue(store.usage().files() > afterFirst.files());
      credit.close();
      assertEquals(0, store.usage().handles());
      assertArrayEquals(firstBytes, read(store.find(context, first).orElseThrow()));
      assertArrayEquals(secondBytes, read(store.find(context("bob"), second).orElseThrow()));
      assertEquals(0, store.usage().handles());
    }
  }

  @Test
  void abortAndProtocolFailureReturnBorrowedCreditOnlyAfterReceiverCleanup() throws Exception {
    try (InputStore store = InputStore.initialize(directory.resolve("cleanup"), ONE_HANDLE)) {
      Commitments.Context context = context("alice");
      InputStore.ReceiverCredit credit = store.reserveInputReceiver();
      InputStore.Usage reserved = store.usage();
      InputStore.Receiver aborted =
          store.begin(context, header(1, new byte[] {1, 2}), SELECTED, 1, credit);
      aborted.write(ByteBuffer.wrap(new byte[] {1}), 2);
      assertThrows(IOException.class, credit::close);
      aborted.close();
      aborted.close();
      assertEquals(reserved, store.usage());

      Records.InputHeader exact = header(2, new byte[] {3});
      InputStore.Receiver invalid = store.begin(context, exact, SELECTED, 3, credit);
      assertCode(
          ProtocolError.Code.INTEGRITY_ERROR,
          () -> invalid.write(ByteBuffer.wrap(new byte[] {3, 4}), 4));
      assertEquals(reserved, store.usage());
      invalid.close();
      invalid.close();
      assertEquals(reserved, store.usage());

      try (InputStore.Receiver recovered = store.begin(context, exact, SELECTED, 5, credit)) {
        recovered.write(ByteBuffer.wrap(new byte[] {3}), 6);
        recovered.finish(7);
      }
      assertEquals(1, store.usage().handles());
      credit.close();
      assertEquals(0, store.usage().handles());
      assertArrayEquals(new byte[] {3}, read(store.find(context, exact).orElseThrow()));
    }
  }

  @Test
  void aggregateFileQuotaFailureLeavesReservedCreditReusableAndUnborrowed() throws Exception {
    InputStore.Limits twoFiles = new InputStore.Limits(1L << 20, 2, 1024, 1);
    try (InputStore store = InputStore.initialize(directory.resolve("file-quota"), twoFiles)) {
      InputStore.ReceiverCredit credit = store.reserveInputReceiver();
      Records.InputHeader first = header(1, new byte[0]);
      try (InputStore.Receiver receiver =
          store.begin(context("alice"), first, SELECTED, 1, credit)) {
        receiver.finish(2);
      }
      assertEquals(1, store.usage().files());
      assertEquals(1, store.usage().handles());
      assertCode(
          ProtocolError.Code.LIMIT_EXCEEDED,
          () -> store.begin(context("alice"), header(2, new byte[0]), SELECTED, 3, credit));
      assertEquals(1, store.usage().files());
      assertEquals(1, store.usage().handles());
      credit.close();
      assertEquals(0, store.usage().handles());
      assertArrayEquals(new byte[0], read(store.find(context("alice"), first).orElseThrow()));
    }
  }

  @Test
  void uncertainStagingRemovalRetainsChargeUntilIdempotentCleanupRetry() throws Exception {
    AtomicBoolean failRemoval = new AtomicBoolean(true);
    InputStore.Probe probe =
        phase -> {
          if (phase == InputStore.Phase.STAGING_REMOVED && failRemoval.getAndSet(false))
            throw new IOException("injected staging removal observation failure");
        };
    try (InputStore store =
        InputStore.initialize(directory.resolve("cleanup-retry"), ONE_HANDLE, probe)) {
      InputStore.ReceiverCredit credit = store.reserveInputReceiver();
      InputStore.Receiver receiver =
          store.begin(context("alice"), header(1, new byte[] {1}), SELECTED, 1, credit);
      receiver.write(ByteBuffer.wrap(new byte[] {1}), 2);
      InputStore.Usage charged = store.usage();
      assertThrows(IOException.class, receiver::close);
      assertFalse(failRemoval.get());
      assertThrows(IOException.class, credit::close);
      assertEquals(charged, store.usage());

      receiver.close();
      receiver.close();
      assertEquals(new InputStore.Usage(0, 0, 1), store.usage());
      Records.InputHeader recovered = header(2, new byte[0]);
      try (InputStore.Receiver retry =
          store.begin(context("alice"), recovered, SELECTED, 3, credit)) {
        retry.finish(4);
      }
      assertEquals(1, store.usage().handles());
      credit.close();
      assertEquals(0, store.usage().handles());
      assertArrayEquals(new byte[0], read(store.find(context("alice"), recovered).orElseThrow()));
    }
  }

  @Test
  void foreignClosedAndFailedBeginNeverConsumeOrCorruptCredit() throws Exception {
    try (InputStore owner = InputStore.initialize(directory.resolve("owner"), ONE_HANDLE);
        InputStore foreign = InputStore.initialize(directory.resolve("foreign"), ONE_HANDLE)) {
      InputStore.ReceiverCredit credit = owner.reserveInputReceiver();
      Records.InputHeader valid = header(1, new byte[] {1});
      assertCode(
          ProtocolError.Code.CONFLICT,
          () -> foreign.begin(context("alice"), valid, SELECTED, 1, credit));
      assertEquals(new InputStore.Usage(0, 0, 1), owner.usage());
      assertEquals(new InputStore.Usage(0, 0, 0), foreign.usage());

      Records.InputHeader overQuota = header(2, new byte[1025]);
      assertCode(
          ProtocolError.Code.LIMIT_EXCEEDED,
          () -> owner.begin(context("alice"), overQuota, SELECTED, 2, credit));
      assertEquals(new InputStore.Usage(0, 0, 1), owner.usage());
      try (InputStore.Receiver receiver =
          owner.begin(context("alice"), valid, SELECTED, 3, credit)) {
        receiver.write(ByteBuffer.wrap(new byte[] {1}), 4);
        receiver.finish(5);
      }
      credit.close();
      credit.close();
      assertEquals(0, owner.usage().handles());
      assertCode(
          ProtocolError.Code.CONFLICT,
          () -> owner.begin(context("alice"), header(3, new byte[0]), SELECTED, 6, credit));
      assertEquals(0, owner.usage().handles());
    }
  }

  private static byte[] read(InputStore.Stored stored) throws IOException {
    try (var input = stored.openStream()) {
      return input.readAllBytes();
    }
  }

  private static Commitments.Context context(String owner) {
    return new Commitments.Context("issuer-a", owner, 1);
  }

  private static Records.InputHeader header(int id, byte[] payload) {
    return new Records.InputHeader(
        1,
        operation(id),
        new Records.AdmitParameters(
            new Records.WorkKey(0, 0, id),
            new Records.Input(payload.length, digest(payload), "application/octet-stream"),
            "test/v1",
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

  private static void assertCode(ProtocolError.Code expected, Throwing action) {
    ProtocolError error = assertThrows(ProtocolError.class, action::run);
    assertEquals(expected, error.code(), error::getMessage);
  }

  @FunctionalInterface
  private interface Throwing {
    void run() throws Exception;
  }
}
