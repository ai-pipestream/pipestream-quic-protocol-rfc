package ai.pipestream.quic;

import static org.junit.jupiter.api.Assertions.*;

import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;

final class SealedWorkTest {
  private static final UUID PRODUCER = UUID.fromString("01010101-0101-0101-0101-010101010101");
  private static final HexFormat HEX = HexFormat.of();

  static SealedWork.Declaration declaration(long scope, SealedWork.EntityKey parent,
      long sequence, List<Long> ids, List<Long> entireSet) throws ProtocolException {
    return new SealedWork.Declaration("sealed-1", PRODUCER, scope, parent, BigInteger.valueOf(sequence), ids,
        entireSet == null ? 0 : SealedWork.SEAL,
        entireSet == null ? null : SealedWork.sealDigest("sealed-1", PRODUCER, scope, parent, entireSet));
  }

  @Test void allFrozenDeclarationsHaveExactBytesOrNamedRefusals() throws Exception {
    for (String row : Files.readAllLines(Path.of("../../test-vectors/work-sets.tsv")).stream().skip(1).toList()) {
      String[] fields = row.split("\t");
      byte[] frame = HEX.parseHex(fields[2]);
      if (fields[1].equals("valid")) {
        assertArrayEquals(frame, SealedWork.encode(SealedWork.decode(frame)), fields[0]);
      } else {
        assertEquals("PIPESTREAM_FRAME_ERROR", assertThrows(ProtocolException.class,
            () -> SealedWork.decode(frame), fields[0]).errorName(), fields[0]);
      }
    }
  }

  @Test void fullUnsignedCounterRangeHasMinimalMajorTypeZeroEncoding() throws Exception {
    for (String[] pair : new String[][] {
        {"23", "a1616e17"}, {"24", "a1616e1818"}, {"255", "a1616e18ff"},
        {"256", "a1616e190100"}, {"65535", "a1616e19ffff"}, {"65536", "a1616e1a00010000"},
        {"4294967295", "a1616e1affffffff"}, {"4294967296", "a1616e1b0000000100000000"},
        {"9223372036854775808", "a1616e1b8000000000000000"},
        {"18446744073709551615", "a1616e1bffffffffffffffff"}}) {
      BigInteger value = new BigInteger(pair[0]);
      assertEquals(pair[1], HEX.formatHex(SealedCbor.encode(Map.of("n", value), 1024)));
      assertEquals(value, SealedCbor.decode(HEX.parseHex(pair[1]), 1024).get("n"));
      var request = new SealedWork.Declaration("sealed-1", PRODUCER, 0, null, value, List.of(1L), 0, null);
      assertEquals(request, SealedWork.decode(SealedWork.encode(request)));
    }
  }

  @Test void malformedCborCannotCoerceNumbersOrDropDuplicateFields() {
    for (String input : List.of("a1616e1817", "a1616e1900ff", "a1616e1a0000ffff",
        "a1616e1b00000000ffffffff", "a1616e20", "a1616ef93c00", "a1616ef6", "a1616ec24101",
        "bf616e01ff", "a2616e01616e02", "a2616201616102", "a1616e0100",
        "a1616e62c080", "a1616e63eda080", "a1616e81", "a1616e9affffffff")) {
      assertEquals(Wire.ERROR_FRAME, assertThrows(ProtocolException.class,
          () -> SealedCbor.decode(HEX.parseHex(input), 1024), input).errorCode(), input);
    }
  }

  @Test void depthByteLimitsAndUtf8AreExplicit() throws Exception {
    Object nested = BigInteger.ONE;
    for (int i = 0; i < 10; i++) nested = List.of(nested);
    Map<String, Object> deep = Map.of("n", nested);
    assertEquals(Wire.ERROR_LIMIT_EXCEEDED,
        assertThrows(ProtocolException.class, () -> SealedCbor.encode(deep, 1024)).errorCode());
    assertEquals(Wire.ERROR_LIMIT_EXCEEDED,
        assertThrows(ProtocolException.class, () -> SealedCbor.encode(Map.of("n", new byte[32]), 16)).errorCode());
    assertEquals(Wire.ERROR_FRAME,
        assertThrows(ProtocolException.class, () -> SealedCbor.encode(Map.of("n", "\uD800"), 1024)).errorCode());
    String text = "emoji \uD83D\uDE00";
    assertEquals(text, SealedCbor.decode(SealedCbor.encode(Map.of("n", text), 1024), 1024).get("n"));
    assertThrows(ProtocolException.class, () -> SealedCbor.encode(Map.of("n", SealedCbor.MAX_UINT.add(BigInteger.ONE)), 1024));
  }

  @Test void sealBindsTheWholeSetAndAcknowledgementComparesEveryField() throws Exception {
    var request = declaration(0, null, 0, List.of(1L, 2L), List.of(1L, 2L));
    assertEquals("f50a638f29d19d57fa224adf5b61cc7d2b5c3f03d0a4eaffe225d683ad4d2c04", HEX.formatHex(request.sealDigest()));
    var ack = request.acknowledgement();
    SealedWork.requireAcknowledgement(request, ack);
    byte[] returnedDigest = request.sealDigest();
    returnedDigest[0] ^= 1;
    assertEquals(ack, request.acknowledgement(), "caller cannot mutate retained digest");
    var changed = new SealedWork.Declaration(request.sessionId(), new UUID(1, 2), 0, null,
        request.sequence(), request.entityIds(), ack.flags(), request.sealDigest());
    assertEquals(Wire.ERROR_ENTITY_INVALID,
        assertThrows(ProtocolException.class, () -> SealedWork.requireAcknowledgement(request, changed)).errorCode());
    assertFalse(Arrays.equals(request.sealDigest(), SealedWork.sealDigest("another", PRODUCER, 0, null, List.of(1L, 2L))));
    assertFalse(Arrays.equals(request.sealDigest(), SealedWork.sealDigest("sealed-1", PRODUCER, 7,
        new SealedWork.EntityKey(0, 1), List.of(1L, 2L))));
  }

  @Test void identityAndWidthsMatchTheNormativeSyntax() {
    for (String session : List.of("", ".", "a.b", "a~b", "a/b", "x".repeat(129), "\u00E9")) {
      var declaration = new SealedWork.Declaration(session, PRODUCER, 0, null, BigInteger.ZERO, List.of(1L), 0, null);
      assertThrows(ProtocolException.class, () -> SealedWork.encode(declaration), session);
    }
    assertThrows(ProtocolException.class, () -> SealedWork.encode(declaration(0, new SealedWork.EntityKey(0, 1), 0, List.of(1L), null)));
    var wide = new SealedWork.Declaration("valid", PRODUCER, 0x1_0000_0000L, new SealedWork.EntityKey(0, 1), BigInteger.ZERO, List.of(1L), 0, null);
    assertThrows(ProtocolException.class, () -> SealedWork.encode(wide));
  }
}
