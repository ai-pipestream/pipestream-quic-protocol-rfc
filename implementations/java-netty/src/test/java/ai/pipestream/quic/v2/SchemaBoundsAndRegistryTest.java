package ai.pipestream.quic.v2;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Section 12 schema bounds and registries that the frozen corpus exercises only with valid
 * vectors: the nullable positions of the work view (S12-017), the 256-id declaration bound
 * (S12-145), the nine work states against their integer registry (S12-193) and the WORK operation
 * numbers of cancel and skip and their responses (S12-215).
 */
class SchemaBoundsAndRegistryTest {
  static final Records.WorkKey WORK = new Records.WorkKey(0, 0, 1);
  static final Records.OperationId OP = DurableServerTest.operation(7);

  static Records.WorkView declared() {
    return new Records.WorkView(
        WORK, Records.State.DECLARED, 0, null, null, null, null, null, null, null, null, null);
  }

  static Records.Input input() throws Exception {
    return new Records.Input(3, DurableServerTest.digest(new byte[] {1, 2, 3}), "text/plain");
  }

  static Records.WorkView roundTrip(Records.WorkView view) {
    byte[] bytes = Wire.encodeRecord(view, Wire.MAX_CONTROL_LIMIT);
    Records.WorkView decoded =
        (Records.WorkView)
            Wire.decodeRecord(Wire.RecordKind.WORK_VIEW, bytes, Wire.MAX_CONTROL_LIMIT);
    assertEquals(view, decoded);
    assertArrayEquals(bytes, Wire.encodeRecord(decoded, Wire.MAX_CONTROL_LIMIT));
    return decoded;
  }

  static ProtocolError refused(Runnable action) {
    return assertThrows(ProtocolError.class, action::run);
  }

  /** S12-017: every nullable position of the view round-trips null and non-null; no other may. */
  @Test
  void workViewNullablePositionsAreExactlyTheSchemaNullables() throws Exception {
    // All nine nullable positions null at once (a declared entity), then each one populated in
    // the states the invariants permit: input and the two admission times on an ACTIVE view,
    // terminal and receipt times with a diagnostic on a FAILED view, a child scope on a branch,
    // an output time and a manifest on a SUCCEEDED view.
    roundTrip(declared());
    Records.WorkView active =
        new Records.WorkView(
            WORK, Records.State.ACTIVE, 1, input(), 1_000L, 11_000L, null, null, null, null,
            null, null);
    roundTrip(active);
    roundTrip(
        new Records.WorkView(
            WORK, Records.State.WAITING_CHILDREN, 1, input(), 1_000L, 11_000L, null, null, null,
            new Records.ChildScope(3, 0), null, null));
    roundTrip(
        new Records.WorkView(
            WORK, Records.State.FAILED, 1, input(), 1_000L, 11_000L, 2_000L, 32_000L, null, null,
            null, new Records.Diagnostic(7, "failed")));
    Records.Manifest manifest =
        new Records.Manifest(
            "issuer-a",
            "alice",
            1,
            WORK,
            1,
            input().sha256(),
            2_000L,
            62_000L,
            List.of(
                new Records.Output(
                    0,
                    3,
                    input().sha256(),
                    "text/plain",
                    new Locator(
                        "pipestream://localhost:7443/v2/sessions/1/scopes/0/producers/0"
                            + "/entities/1/attempts/1/outputs/0"))));
    roundTrip(
        new Records.WorkView(
            WORK, Records.State.SUCCEEDED, 1, input(), 1_000L, 11_000L, 2_000L, 32_000L, 62_000L,
            null, manifest, null));
    // A null where the schema forbids it is FRAME_ERROR: the state and attempt positions of the
    // encoded declared view are single bytes after the 12-array header and the 3-array work key.
    byte[] encoded = Wire.encodeRecord(declared(), Wire.MAX_CONTROL_LIMIT);
    assertEquals((byte) 0x8C, encoded[0], "twelve-element view");
    assertEquals((byte) 0x83, encoded[1], "three-element work key");
    assertEquals((byte) 0xF6, encoded[7], "first nullable position is null");
    for (int position : new int[] {2, 3, 4, 5, 6}) {
      byte[] patched = Arrays.copyOf(encoded, encoded.length);
      patched[position] = (byte) 0xF6;
      ProtocolError refused =
          refused(
              () ->
                  Wire.decodeRecord(Wire.RecordKind.WORK_VIEW, patched, Wire.MAX_CONTROL_LIMIT));
      assertEquals(ProtocolError.Code.FRAME_ERROR, refused.code(), "null at byte " + position);
    }
    // A bare null is not any record.
    for (Wire.RecordKind kind : Wire.RecordKind.values()) {
      ProtocolError refused =
          refused(
              () -> Wire.decodeRecord(kind, new byte[] {(byte) 0xF6}, Wire.MAX_CONTROL_LIMIT));
      assertEquals(ProtocolError.Code.FRAME_ERROR, refused.code(), kind.name());
    }
    // A boolean where a number is required is refused the same way.
    byte[] bool = Arrays.copyOf(encoded, encoded.length);
    bool[5] = (byte) 0xF5;
    assertEquals(
        ProtocolError.Code.FRAME_ERROR,
        refused(() -> Wire.decodeRecord(Wire.RecordKind.WORK_VIEW, bool, Wire.MAX_CONTROL_LIMIT))
            .code());
  }

  /** S12-145: 256 ids declare and round-trip; 257 are refused before and after encoding. */
  @Test
  void declarationsCarryAtMostTwoHundredFiftySixIds() {
    List<Long> full = new ArrayList<>();
    for (long id = 1; id <= 256; id++) full.add(id);
    Messages.Declare declare = new Messages.Declare(3, OP, 0, full, true);
    byte[] frame = Wire.encode(declare, Wire.MAX_CONTROL_LIMIT);
    Wire.Known known = assertInstanceOf(Wire.Known.class, Wire.decode(frame, Wire.MAX_CONTROL_LIMIT));
    assertEquals(declare, known.message());
    List<Long> over = new ArrayList<>(full);
    over.add(257L);
    ProtocolError refused = refused(() -> new Messages.Declare(4, OP, 0, over, true));
    assertEquals(ProtocolError.Code.FRAME_ERROR, refused.code(), refused.toString());
  }

  /** S12-193: the nine states carry the registry integers 0 to 8 and nothing else decodes. */
  @Test
  void workStatesMatchTheIntegerRegistry() {
    List<String> registry =
        List.of(
            "DECLARED",
            "ACTIVE",
            "AWAITING_RETRY",
            "WAITING_CHILDREN",
            "CANCELLING",
            "SUCCEEDED",
            "FAILED",
            "CANCELLED",
            "SKIPPED");
    assertEquals(9, Records.State.values().length);
    for (int value = 0; value <= 8; value++) {
      Records.State state = Records.State.from(value);
      assertEquals(registry.get(value), state.name(), "state " + value);
      assertEquals(value, state.value(), state.name());
      assertEquals(value >= 5, state.terminal(), state.name());
    }
    for (long value : new long[] {9, 10, -1, 255, Long.MAX_VALUE}) {
      assertEquals(
          ProtocolError.Code.FRAME_ERROR,
          refused(() -> Records.State.from(value)).code(),
          "state " + value);
    }
  }

  /** S12-215: cancel, skip and their responses are WORK operations 8, 9, 10 and 11 on the wire. */
  @Test
  void cancelAndSkipUseTheirRegisteredOperationNumbers() throws Exception {
    Records.OperationReceipt receipt =
        new Records.OperationReceipt(
            OP,
            DurableServerTest.digest(new byte[] {9}),
            new Records.Cancelled(WORK, 1_000, 0, Records.State.CANCELLED));
    Records.OperationReceipt skipped =
        new Records.OperationReceipt(
            OP,
            DurableServerTest.digest(new byte[] {9}),
            new Records.Skipped(WORK, 1_000, 0, Records.State.SKIPPED));
    record Expected(Messages.Message message, int operation, int fields) {}
    for (Expected expected :
        List.of(
            new Expected(new Messages.Cancel(5, OP, WORK), 8, 4),
            new Expected(new Messages.CancelResponse(5, receipt), 9, 3),
            new Expected(new Messages.Skip(6, OP, WORK), 10, 4),
            new Expected(new Messages.SkipResponse(6, skipped), 11, 3))) {
      byte[] frame = Wire.encode(expected.message(), Wire.MAX_CONTROL_LIMIT);
      assertEquals(4, frame[0], "WORK frame type");
      assertEquals((byte) (0x80 | expected.fields()), frame[5], "field count");
      assertEquals((byte) expected.operation(), frame[6], expected.message().toString());
      Wire.Known known =
          assertInstanceOf(Wire.Known.class, Wire.decode(frame, Wire.MAX_CONTROL_LIMIT));
      assertEquals(expected.message(), known.message());
    }
  }
}
