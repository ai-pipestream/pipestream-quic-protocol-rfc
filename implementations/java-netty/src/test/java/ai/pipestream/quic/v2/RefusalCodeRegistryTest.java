package ai.pipestream.quic.v2;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Section 12.2 / 11.10: the eighteen named refusal codes and their numeric values, the QUIC
 * application-error mapping {@code 0x200 + code}, and the rule that a reserved or unknown
 * REFUSAL code is FRAME_ERROR. Table-driven against the registry text, not against the enum's
 * own order.
 */
class RefusalCodeRegistryTest {
  static final List<String> REGISTRY =
      List.of(
          "FRAME_ERROR",
          "EXTENSION_UNSUPPORTED",
          "UNAUTHORIZED",
          "LIMIT_EXCEEDED",
          "NOT_FOUND",
          "EXPIRED",
          "CONFLICT",
          "INTEGRITY_ERROR",
          "NOT_READY",
          "WAIT_TIMEOUT",
          "DEADLINE_EXCEEDED",
          "CANCELLED",
          "APPLICATION_UNSUPPORTED",
          "CONTROL_RESET",
          "INTERNAL_ERROR",
          "OUTPUT_UNAVAILABLE",
          "CLOCK_UNSAFE",
          "ALREADY_TERMINAL");

  @Test
  void everyNamedCodeHasItsRegistryValueAndApplicationError() {
    assertEquals(18, REGISTRY.size());
    assertEquals(18, ProtocolError.Code.values().length, "codes outside the registry");
    for (int value = 1; value <= 18; value++) {
      ProtocolError.Code code = ProtocolError.Code.from(value);
      assertEquals(REGISTRY.get(value - 1), code.name(), "code " + value);
      assertEquals(value, code.value(), code.name());
      assertEquals(0x200L + value, code.applicationError(), code.name());
      assertEquals(code, ProtocolError.Code.valueOf(REGISTRY.get(value - 1)));
    }
    assertEquals(0x204L, ProtocolError.Code.LIMIT_EXCEEDED.applicationError());
    assertEquals(0x212L, ProtocolError.Code.ALREADY_TERMINAL.applicationError());
  }

  @Test
  void reservedAndUnknownCodesAreFrameErrors() {
    for (long value : new long[] {0, 19, 20, 31, 32, 0x200, 0x204, 255, -1, Long.MAX_VALUE}) {
      ProtocolError refused =
          assertThrows(ProtocolError.class, () -> ProtocolError.Code.from(value), "code " + value);
      assertEquals(ProtocolError.Code.FRAME_ERROR, refused.code(), "code " + value);
    }
  }
}
