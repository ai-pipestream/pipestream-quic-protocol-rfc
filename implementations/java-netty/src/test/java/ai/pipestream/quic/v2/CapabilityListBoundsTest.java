package ai.pipestream.quic.v2;

import static ai.pipestream.quic.v2.Messages.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Section 12.1: capability profile lists are strictly increasing, contain at most 32 entries and
 * have no duplicates (S12-028). The typed decoder constructs {@link Capabilities}, so the record's
 * own validation is the bound every wire cut reaches.
 */
class CapabilityListBoundsTest {
  static Capabilities offer(List<Integer> supported) {
    return new Capabilities(false, supported, List.of(), 1 << 20, 16, 64, 1 << 20, 5000, 30_000);
  }

  static List<Integer> profiles(int count) {
    List<Integer> ids = new ArrayList<>();
    for (int i = 1; i <= count; i++) ids.add(1000 + i);
    return ids;
  }

  @Test
  void thirtyTwoEntriesAreTheBoundAndThirtyThreeAreAFrameError() {
    assertEquals(32, offer(profiles(32)).supported().size());
    ProtocolError error = assertThrows(ProtocolError.class, () -> offer(profiles(33)));
    assertEquals(ProtocolError.Code.FRAME_ERROR, error.code());
    assertTrue(error.getMessage().endsWith("collection exceeds schema bound"), error.getMessage());
  }

  @Test
  void decreasingDuplicateAndOutOfRangeEntriesAreFrameErrors() {
    for (List<Integer> bad :
        List.of(
            List.of(DURABLE_WORK, DURABLE_WORK - 1),
            List.of(RESULT_DELIVERY, DURABLE_WORK),
            List.of(DURABLE_WORK, DURABLE_WORK),
            List.of(0, DURABLE_WORK),
            List.of(DURABLE_WORK, 65535))) {
      ProtocolError error = assertThrows(ProtocolError.class, () -> offer(bad), bad.toString());
      assertEquals(ProtocolError.Code.FRAME_ERROR, error.code(), bad.toString());
    }
    // The same bound applies to the required list.
    ProtocolError required =
        assertThrows(
            ProtocolError.class,
            () ->
                new Capabilities(
                    false,
                    List.of(DURABLE_WORK, RESULT_DELIVERY),
                    List.of(RESULT_DELIVERY, DURABLE_WORK),
                    1 << 20,
                    16,
                    64,
                    1 << 20,
                    5000,
                    30_000));
    assertEquals(ProtocolError.Code.FRAME_ERROR, required.code());
  }
}
