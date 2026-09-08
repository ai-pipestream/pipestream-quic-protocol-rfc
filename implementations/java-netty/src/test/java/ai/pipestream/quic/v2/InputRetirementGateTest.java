package ai.pipestream.quic.v2;

import static ai.pipestream.quic.v2.Messages.DURABLE_WORK;
import static org.junit.jupiter.api.Assertions.*;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

@Timeout(15)
final class InputRetirementGateTest {
  private static final Messages.Capabilities SELECTED =
      new Messages.Capabilities(
          true, List.of(DURABLE_WORK), List.of(), 65_536, 4, 16, 1 << 20, 1000, 5000);
  private static final InputStore.Limits LIMITS = new InputStore.Limits(4L << 20, 32, 1 << 20, 4);

  @TempDir Path directory;

  @Test
  void activeReceiverCountsAsItsSessionResourceBeforeInstallationOnly() throws Exception {
    AtomicBoolean expired = new AtomicBoolean();
    Commitments.Context first = context(1);
    Commitments.Context other = context(2);
    try (InputStore store = store("active", expired)) {
      InputStore.Receiver receiver =
          store.begin(first, header(1, 1, new byte[] {1}, 0), SELECTED, 0);
      try {
        assertTrue(store.sessionHasResources(first));
        assertFalse(store.sessionHasResources(other));
        assertEquals(1, store.usage().handles());
        receiver.close();
        assertFalse(store.sessionHasResources(first));
        assertEquals(new InputStore.Usage(0, 0, 0), store.usage());
      } finally {
        receiver.close();
      }
    }
    try (InputStore store = store("unrelated", expired)) {
      InputStore.Receiver receiver =
          store.begin(other, header(2, 2, new byte[] {2}, 0), SELECTED, 0);
      try {
        assertFalse(store.sessionHasResources(first));
        assertTrue(store.sessionHasResources(other));
        receiver.close();
        assertFalse(store.sessionHasResources(other));
        assertEquals(new InputStore.Usage(0, 0, 0), store.usage());
      } finally {
        receiver.close();
      }
    }
  }

  @Test
  void expiredGenerationRefusesNewReceptionAndOutputFundingWithoutCharging() throws Exception {
    AtomicBoolean expired = new AtomicBoolean(true);
    Commitments.Context context = context(1);
    Records.InputHeader input = header(1, 1, new byte[] {1}, 0);
    Records.InputHeader funded = header(1, 2, new byte[] {2}, 1);
    try (InputStore store = store("refused", expired)) {
      assertCode(ProtocolError.Code.EXPIRED, () -> store.begin(context, input, SELECTED, 0));
      assertCode(ProtocolError.Code.EXPIRED, () -> store.reserveOutputs(context, funded));
      assertEquals(new InputStore.Usage(0, 0, 0), store.usage());
      assertFalse(store.sessionHasResources(context));
    }
  }

  @Test
  void generationExpiryAtFinishAbortsAndSynchronouslyRefundsTheReceiver() throws Exception {
    AtomicBoolean expired = new AtomicBoolean();
    Commitments.Context context = context(1);
    byte[] payload = {1, 2, 3};
    Records.InputHeader header = header(1, 1, payload, 0);
    try (InputStore store = store("finish", expired)) {
      InputStore.Receiver receiver = store.begin(context, header, SELECTED, 0);
      receiver.write(ByteBuffer.wrap(payload), 1);
      InputStore.Usage charged = store.usage();
      assertTrue(charged.bytes() > 0);
      assertEquals(2, charged.files());
      assertEquals(1, charged.handles());
      assertTrue(store.sessionHasResources(context));

      expired.set(true);
      assertCode(ProtocolError.Code.EXPIRED, () -> receiver.finish(2));
      assertEquals(new InputStore.Usage(0, 0, 0), store.usage());
      assertFalse(store.sessionHasResources(context));
      receiver.close();
      assertEquals(new InputStore.Usage(0, 0, 0), store.usage());
    }
  }

  private InputStore store(String name, AtomicBoolean expired) throws Exception {
    UUID authority = UUID.randomUUID();
    InputStore store =
        InputStore.initializeForAuthority(directory.resolve(name), LIMITS, authority);
    store.bindGenerationGate(
        authority,
        context -> {
          if (context.generation() == 1 && expired.get())
            throw new ProtocolError(ProtocolError.Code.EXPIRED, "session retired");
        });
    return store;
  }

  private static Commitments.Context context(long generation) {
    return new Commitments.Context("issuer-a", "alice", generation);
  }

  private static Records.InputHeader header(
      long generation, int operation, byte[] payload, int outputs) throws Exception {
    return new Records.InputHeader(
        generation,
        operation(operation),
        new Records.AdmitParameters(
            new Records.WorkKey(0, 0, operation),
            new Records.Input(payload.length, digest(payload), "application/octet-stream"),
            "copy",
            0,
            1000,
            new Records.OutputBudget(outputs, outputs == 0 ? 0 : 16)));
  }

  private static Records.OperationId operation(int value) {
    byte[] bytes = new byte[16];
    bytes[15] = (byte) value;
    return new Records.OperationId(bytes);
  }

  private static Records.Digest digest(byte[] payload) throws Exception {
    return new Records.Digest(MessageDigest.getInstance("SHA-256").digest(payload));
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
