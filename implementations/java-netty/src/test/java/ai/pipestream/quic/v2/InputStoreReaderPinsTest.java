package ai.pipestream.quic.v2;

import static ai.pipestream.quic.v2.Messages.DURABLE_WORK;
import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

@Timeout(20)
final class InputStoreReaderPinsTest {
  private static final Messages.Capabilities SELECTED =
      new Messages.Capabilities(
          true, List.of(DURABLE_WORK), List.of(), 65_536, 4, 16, 1 << 20, 1000, 5000);
  private static final InputStore.Limits LIMITS = new InputStore.Limits(16L << 20, 64, 1 << 20, 4);

  @TempDir Path directory;

  @Test
  void multipleReadersPinOnlyTheirExactObjectUntilEachReaderCloses() throws Exception {
    Commitments.Context context = context("alice", 1);
    byte[] one = {1, 2, 3};
    byte[] two = {4, 5, 6, 7};
    Records.InputHeader firstHeader = header(1, 1, one);
    Records.InputHeader secondHeader = header(1, 2, two);
    try (InputStore store = InputStore.initialize(directory.resolve("multiple"), LIMITS)) {
      InputStore.Stored first = install(store, context, firstHeader, one);
      InputStore.Stored second = install(store, context, secondHeader, two);
      InputStore.Usage retained = store.usage();
      assertFalse(store.inputPinned(context, firstHeader));
      assertFalse(store.inputPinned(context, secondHeader));

      InputStream left = first.openStream();
      InputStream right = first.openStream();
      InputStream other = second.openStream();
      try {
        assertEquals(3, store.usage().handles());
        assertTrue(store.inputPinned(context, firstHeader));
        assertTrue(store.inputPinned(context, secondHeader));
        assertArrayEquals(one, left.readAllBytes());
        assertEquals(-1, left.read());
        assertTrue(store.inputPinned(context, firstHeader), "EOF does not release a physical pin");

        left.close();
        left.close();
        assertTrue(store.inputPinned(context, firstHeader));
        right.close();
        assertFalse(store.inputPinned(context, firstHeader));
        assertTrue(store.inputPinned(context, secondHeader));
        assertArrayEquals(two, other.readAllBytes());
      } finally {
        left.close();
        right.close();
        other.close();
      }
      assertFalse(store.inputPinned(context, firstHeader));
      assertFalse(store.inputPinned(context, secondHeader));
      assertEquals(retained.bytes(), store.usage().bytes());
      assertEquals(retained.files(), store.usage().files());
      assertEquals(0, store.usage().handles());
    }
  }

  @Test
  void exhaustedGlobalHandleLimitDoesNotCreateAnExtraObjectPin() throws Exception {
    InputStore.Limits twoHandles = new InputStore.Limits(16L << 20, 64, 1 << 20, 2);
    Commitments.Context context = context("alice", 1);
    byte[] one = {1};
    byte[] two = {2};
    Records.InputHeader firstHeader = header(1, 1, one);
    Records.InputHeader secondHeader = header(1, 2, two);
    try (InputStore store = InputStore.initialize(directory.resolve("handles"), twoHandles)) {
      InputStore.Stored first = install(store, context, firstHeader, one);
      InputStore.Stored second = install(store, context, secondHeader, two);
      try (InputStream firstReader = first.openStream();
          InputStream duplicate = first.openStream()) {
        assertTrue(store.inputPinned(context, firstHeader));
        assertFalse(store.inputPinned(context, secondHeader));
        assertCode(ProtocolError.Code.LIMIT_EXCEEDED, second::openStream);
        assertEquals(2, store.usage().handles());
        assertFalse(store.inputPinned(context, secondHeader));
        assertEquals(1, firstReader.read());
        assertEquals(1, duplicate.read());
      }
      assertEquals(0, store.usage().handles());
    }
  }

  @Test
  void failedMissingOrCorruptOpenLeavesNoPhantomPinOrHandle() throws Exception {
    for (String damage : List.of("missing", "corrupt")) {
      Path root = directory.resolve(damage);
      Commitments.Context context = context("alice", 1);
      byte[] payload = {1, 2, 3, 4};
      Records.InputHeader header = header(1, 1, payload);
      try (InputStore store = InputStore.initialize(root, LIMITS)) {
        InputStore.Stored stored = install(store, context, header, payload);
        InputStore.Usage retained = store.usage();
        Path object = onlyInput(root);
        if (damage.equals("missing")) Files.delete(object);
        else Files.write(object, new byte[] {9}, StandardOpenOption.TRUNCATE_EXISTING);

        assertThrows(IOException.class, stored::openStream);
        assertFalse(store.inputPinned(context, header));
        assertEquals(0, store.usage().handles());
        assertEquals(retained.bytes(), store.usage().bytes());
        assertEquals(retained.files(), store.usage().files());
      }
    }
  }

  @Test
  void pinIdentityIncludesOwnerGenerationAndCompleteHeader() throws Exception {
    Commitments.Context alice = context("alice", 1);
    byte[] payload = {7, 8};
    Records.InputHeader installed = header(1, 1, payload);
    try (InputStore store = InputStore.initialize(directory.resolve("identity"), LIMITS)) {
      InputStore.Stored stored = install(store, alice, installed, payload);
      Records.InputHeader changedOperation =
          new Records.InputHeader(1, operation(2), installed.parameters());
      Records.InputHeader changedApplication =
          new Records.InputHeader(
              1,
              installed.operation(),
              new Records.AdmitParameters(
                  installed.parameters().work(),
                  installed.parameters().input(),
                  "other",
                  installed.parameters().mode(),
                  installed.parameters().executionMs(),
                  installed.parameters().outputs()));
      Records.InputHeader changedGeneration = header(2, 1, payload);
      try (InputStream reader = stored.openStream()) {
        assertTrue(store.inputPinned(alice, installed));
        assertFalse(store.inputPinned(new Commitments.Context("issuer-b", "alice", 1), installed));
        assertFalse(store.inputPinned(context("bob", 1), installed));
        assertFalse(store.inputPinned(context("alice", 2), changedGeneration));
        assertFalse(store.inputPinned(alice, changedOperation));
        assertFalse(store.inputPinned(alice, changedApplication));
        assertTrue(store.find(context("bob", 1), installed).isEmpty());
        assertTrue(store.find(context("alice", 2), changedGeneration).isEmpty());
        assertTrue(store.find(alice, changedOperation).isEmpty());
        assertTrue(store.find(alice, changedApplication).isEmpty());
        assertEquals(7, reader.read());
      }
    }
  }

  @Test
  void receiverCreditUsesAHandleButNeverLooksLikeAnInstalledReaderPin() throws Exception {
    Commitments.Context context = context("alice", 1);
    Records.InputHeader installedHeader = header(1, 1, new byte[0]);
    byte[] incoming = {5, 6};
    Records.InputHeader receivingHeader = header(1, 2, incoming);
    try (InputStore store = InputStore.initialize(directory.resolve("receiver"), LIMITS)) {
      InputStore.Stored installed = install(store, context, installedHeader, new byte[0]);
      try (InputStore.ReceiverCredit credit = store.reserveInputReceiver()) {
        assertNotNull(credit);
        assertEquals(1, store.usage().handles());
        assertFalse(store.inputPinned(context, installedHeader));
        assertFalse(store.inputPinned(context, receivingHeader));
      }
      assertEquals(0, store.usage().handles());

      try (InputStore.Receiver receiver = store.begin(context, receivingHeader, SELECTED, 4)) {
        assertEquals(1, store.usage().handles());
        assertFalse(store.inputPinned(context, installedHeader));
        assertFalse(store.inputPinned(context, receivingHeader));
        receiver.write(ByteBuffer.wrap(new byte[] {5}), 5);
        assertFalse(store.inputPinned(context, receivingHeader));
      }
      assertEquals(0, store.usage().handles());

      try (InputStream reader = installed.openStream()) {
        assertEquals(-1, reader.read(new byte[1]));
        assertTrue(store.inputPinned(context, installedHeader));
      }
      assertFalse(store.inputPinned(context, installedHeader));
      assertEquals(0, store.usage().handles());
    }
  }

  @Test
  void cleanCloseAndReopenNeverRetainsVolatilePinsOrChangesCharges() throws Exception {
    Path root = directory.resolve("reopen");
    Commitments.Context context = context("alice", 1);
    byte[] payload = {3, 1, 4};
    Records.InputHeader header = header(1, 1, payload);
    InputStore.Usage retained;
    try (InputStore store = InputStore.initialize(root, LIMITS)) {
      InputStore.Stored stored = install(store, context, header, payload);
      retained = store.usage();
      try (InputStream reader = stored.openStream()) {
        assertTrue(store.inputPinned(context, header));
        assertArrayEquals(payload, reader.readAllBytes());
      }
      assertFalse(store.inputPinned(context, header));
    }

    try (InputStore reopened = InputStore.open(root, LIMITS)) {
      assertFalse(reopened.inputPinned(context, header));
      assertEquals(retained.bytes(), reopened.usage().bytes());
      assertEquals(retained.files(), reopened.usage().files());
      assertEquals(0, reopened.usage().handles());
      try (InputStream reader = reopened.find(context, header).orElseThrow().openStream()) {
        assertTrue(reopened.inputPinned(context, header));
        assertArrayEquals(payload, reader.readAllBytes());
      }
      assertFalse(reopened.inputPinned(context, header));
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

  private static Path onlyInput(Path root) throws Exception {
    try (var entries = Files.newDirectoryStream(root.resolve("objects"), "*.input")) {
      var iterator = entries.iterator();
      assertTrue(iterator.hasNext());
      Path result = iterator.next();
      assertFalse(iterator.hasNext());
      return result;
    }
  }

  private static Commitments.Context context(String owner, long generation) {
    return new Commitments.Context("issuer-a", owner, generation);
  }

  private static Records.InputHeader header(long generation, int id, byte[] payload)
      throws Exception {
    return new Records.InputHeader(
        generation,
        operation(id),
        new Records.AdmitParameters(
            new Records.WorkKey(0, 0, id),
            new Records.Input(payload.length, digest(payload), "application/octet-stream"),
            "copy",
            0,
            1000,
            new Records.OutputBudget(0, 0)));
  }

  private static Records.Digest digest(byte[] bytes) throws Exception {
    return new Records.Digest(MessageDigest.getInstance("SHA-256").digest(bytes));
  }

  private static Records.OperationId operation(int value) {
    byte[] bytes = new byte[16];
    bytes[12] = (byte) (value >>> 24);
    bytes[13] = (byte) (value >>> 16);
    bytes[14] = (byte) (value >>> 8);
    bytes[15] = (byte) value;
    return new Records.OperationId(bytes);
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
