package ai.pipestream.quic.v2;

import static ai.pipestream.quic.v2.Messages.DURABLE_WORK;
import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

@Timeout(15)
final class InputStoreReceiverPinsTest {
  private static final Messages.Capabilities SELECTED =
      new Messages.Capabilities(
          true, List.of(DURABLE_WORK), List.of(), 65_536, 4, 16, 1 << 20, 1000, 5000);
  private static final InputStore.Limits LIMITS = new InputStore.Limits(16L << 20, 64, 1 << 20, 4);

  @TempDir Path directory;

  @Test
  void duplicateReceptionAndReaderIndependentlyPinOnlyTheExactInstalledInput() throws Exception {
    Commitments.Context context = context("alice");
    byte[] payload = {1, 2, 3};
    Records.InputHeader exact = header(1, payload);
    Records.InputHeader unrelated = header(2, new byte[] {9});
    try (InputStore store = InputStore.initialize(directory.resolve("duplicate"), LIMITS)) {
      InputStore.Stored installed = install(store, context, exact, payload);
      assertFalse(store.inputInUse(context, exact));

      InputStore.Receiver duplicate = store.begin(context, exact, SELECTED, 10);
      duplicate.write(ByteBuffer.wrap(payload), 11);
      assertTrue(store.inputInUse(context, exact));
      assertFalse(store.inputPinned(context, exact));
      assertFalse(store.inputInUse(context, unrelated));

      InputStore.Receiver other = store.begin(context, unrelated, SELECTED, 12);
      assertTrue(store.inputInUse(context, unrelated));
      assertTrue(store.inputInUse(context, exact));
      try (InputStream reader = installed.openStream()) {
        assertArrayEquals(payload, reader.readAllBytes());
        assertEquals(-1, reader.read());
        assertTrue(store.inputPinned(context, exact));
        assertTrue(store.inputInUse(context, exact));
        assertEquals(exact, duplicate.finish(13).header());
        assertTrue(store.inputPinned(context, exact));
        assertTrue(store.inputInUse(context, exact));
      } finally {
        duplicate.close();
        other.close();
      }
      assertFalse(store.inputPinned(context, exact));
      assertFalse(store.inputInUse(context, exact));
      assertFalse(store.inputInUse(context, unrelated));
      assertEquals(0, store.usage().handles());
    }
  }

  @Test
  void receiverCreditHasNoIdentityUntilBorrowedAndSequentialCleanupReleasesIt() throws Exception {
    Commitments.Context context = context("alice");
    Records.InputHeader first = header(1, new byte[] {1});
    Records.InputHeader zero = header(2, new byte[0]);
    try (InputStore store = InputStore.initialize(directory.resolve("credit"), LIMITS);
        InputStore.ReceiverCredit credit = store.reserveInputReceiver()) {
      assertFalse(store.inputInUse(context, first));
      assertFalse(store.inputInUse(context, zero));
      InputStore.Receiver receiver = store.begin(context, first, SELECTED, 1, credit);
      assertTrue(store.inputInUse(context, first));
      assertFalse(store.inputInUse(context, zero));
      receiver.write(ByteBuffer.wrap(new byte[] {1}), 2);
      receiver.finish(3);
      assertFalse(store.inputInUse(context, first));
      assertEquals(1, store.usage().handles());

      try (InputStore.Receiver empty = store.begin(context, zero, SELECTED, 4, credit)) {
        assertTrue(store.inputInUse(context, zero));
        empty.finish(5);
      }
      assertFalse(store.inputInUse(context, zero));
      assertEquals(1, store.usage().handles());
    }
  }

  @Test
  void abortProtocolFailureAndTimeoutReleaseIdentityOnlyAfterSafeCleanup() throws Exception {
    Commitments.Context context = context("alice");
    try (InputStore store = InputStore.initialize(directory.resolve("failures"), LIMITS)) {
      Records.InputHeader aborted = header(1, new byte[] {1, 2});
      InputStore.Receiver abort = store.begin(context, aborted, SELECTED, 0);
      assertTrue(store.inputInUse(context, aborted));
      abort.close();
      abort.close();
      assertFalse(store.inputInUse(context, aborted));

      Records.InputHeader invalid = header(2, new byte[] {3});
      InputStore.Receiver overrun = store.begin(context, invalid, SELECTED, 0);
      assertTrue(store.inputInUse(context, invalid));
      assertCode(
          ProtocolError.Code.INTEGRITY_ERROR,
          () -> overrun.write(ByteBuffer.wrap(new byte[] {3, 4}), 1));
      assertFalse(store.inputInUse(context, invalid));
      overrun.close();

      Records.InputHeader timed = header(3, new byte[] {4});
      InputStore.Receiver timeout = store.begin(context, timed, SELECTED, 0);
      assertTrue(store.inputInUse(context, timed));
      assertCode(
          ProtocolError.Code.LIMIT_EXCEEDED,
          () -> timeout.checkDeadline(TimeUnit.MILLISECONDS.toNanos(1000) + 1));
      assertFalse(store.inputInUse(context, timed));
      timeout.close();
      assertEquals(0, store.usage().handles());
    }
  }

  @Test
  void uncertainStagingRemovalKeepsExactIdentityAndHandleUntilRetryClose() throws Exception {
    AtomicBoolean failRemoval = new AtomicBoolean(true);
    InputStore.Probe probe =
        phase -> {
          if (phase == InputStore.Phase.STAGING_REMOVED && failRemoval.get()) {
            throw new IOException("injected staging removal failure");
          }
        };
    Commitments.Context context = context("alice");
    Records.InputHeader header = header(1, new byte[] {7});
    try (InputStore store =
        InputStore.initialize(directory.resolve("uncertain-cleanup"), LIMITS, probe)) {
      InputStore.Receiver receiver = store.begin(context, header, SELECTED, 0);
      receiver.write(ByteBuffer.wrap(new byte[] {7}), 1);
      InputStore.Usage charged = store.usage();
      assertTrue(store.inputInUse(context, header));
      assertThrows(IOException.class, receiver::close);
      assertTrue(store.inputInUse(context, header));
      assertEquals(charged, store.usage());

      failRemoval.set(false);
      receiver.close();
      receiver.close();
      assertFalse(store.inputInUse(context, header));
      assertEquals(0, store.usage().handles());
    }
  }

  private static InputStore.Stored install(
      InputStore store, Commitments.Context context, Records.InputHeader header, byte[] payload)
      throws Exception {
    try (InputStore.Receiver receiver = store.begin(context, header, SELECTED, 1)) {
      receiver.write(ByteBuffer.wrap(payload), 2);
      return receiver.finish(3);
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
