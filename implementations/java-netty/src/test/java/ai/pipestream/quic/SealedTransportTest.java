package ai.pipestream.quic;

import static org.junit.jupiter.api.Assertions.*;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

final class SealedTransportTest {
  @Test void frozenScopedMessagesUseExactBytesAndNamedRefusals() throws Exception {
    int checked = 0;
    for (String line : Files.readAllLines(Path.of("../../test-vectors/recursive/index.tsv")).stream().skip(1).toList()) {
      String[] parts = line.split("\t");
      if (!List.of("scoped-status", "barrier-released", "scoped-checkpoint", "root-scope-barrier").contains(parts[0])) continue;
      byte[] encoded = HexFormat.of().parseHex(parts[5]);
      var frame = Wire.decodeControl(encoded); checked++;
      if (parts[0].equals("root-scope-barrier")) {
        assertEquals(parts[4], assertThrows(ProtocolException.class, () -> SealedTransport.barrier(frame.payload())).errorName());
      } else {
        byte[] roundtrip = switch (frame.type()) {
          case 0x50 -> Wire.encodeStatus(SealedTransport.status(frame.payload()));
          case 0x55 -> SealedTransport.barrier(SealedTransport.barrier(frame.payload()));
          case 0x81 -> SealedTransport.checkpoint(SealedTransport.checkpoint(frame.payload()));
          default -> throw new AssertionError("unexpected fixture");
        };
        assertArrayEquals(encoded, roundtrip, parts[0]);
      }
    }
    assertEquals(4, checked);
  }

  @Test void optionalCheckpointMembersAndUint64CountersAreNotNarrowed() throws Exception {
    for (Long scope : Arrays.asList(null, 0L, 0xffff_ffffL)) {
      var request = new SealedTransport.Checkpoint("scope", SealedCbor.MAX_UINT, Wire.MAX_ENTITY_ID, scope, 0, SealedCbor.MAX_UINT);
      assertEquals(request, SealedTransport.checkpoint(Wire.decodeControl(SealedTransport.checkpoint(request)).payload()));
    }
    var omitted = SealedCbor.encode(Map.of("checkpoint-id", "x", "sequence-number", BigInteger.ONE,
        "checkpoint-entity-id", 1), Wire.MAX_CONTROL_FRAME);
    var decoded = SealedTransport.checkpoint(omitted);
    assertNull(decoded.scopeId()); assertNull(decoded.timeoutMs()); assertEquals(0, decoded.flags());
    assertNotEquals(decoded.acknowledgement(), new SealedTransport.Checkpoint("x", BigInteger.ONE, 1, 0L, 1, null));
  }

  @Test void scopedHeaderPreservesFullChunkOffsetsAndRejectsInvalidBindings() throws Exception {
    var header = new SealedTransport.Header(new SealedWork.EntityKey(7, 42), new SealedWork.EntityKey(0, 1), 3,
        "application/octet-stream", SealedCbor.MAX_UINT, new byte[32], Map.of("test", "value"),
        new SealedTransport.Chunk(SealedCbor.MAX_UINT, SealedCbor.MAX_UINT.subtract(BigInteger.ONE), SealedCbor.MAX_UINT));
    byte[] encoded = SealedTransport.header(header);
    assertEquals(encoded.length - 4, ByteBuffer.wrap(encoded).getInt());
    var decoded = SealedTransport.header(Arrays.copyOfRange(encoded, 4, encoded.length));
    assertArrayEquals(encoded, SealedTransport.header(decoded));
    assertEquals(header.chunk(), decoded.chunk()); assertEquals(header.payloadLength(), decoded.payloadLength());
    var empty = SealedCbor.encode(Map.of("entity-id", 1, "layer", 0), 100);
    assertEquals(new SealedWork.EntityKey(0, 1), SealedTransport.header(empty).key());
    assertThrows(ProtocolException.class, () -> SealedTransport.header(new SealedTransport.Header(
        new SealedWork.EntityKey(7, 1), new SealedWork.EntityKey(7, 2), 0, null, null, null, Map.of(), null)));
    assertEquals(Wire.ERROR_LAYER_UNSUPPORTED, assertThrows(ProtocolException.class, () -> SealedTransport.header(
        SealedCbor.encode(Map.of("entity-id", 1, "layer", 0, "completion-policy", Map.of("mode", 1)), 100))).errorCode());
  }

  @Test void sealedNegotiationCannotDowngradeOrEscalateLimits() throws Exception {
    var limits = SealedTransport.Limits.defaults();
    byte[] payload = Wire.decodeControl(SealedTransport.capabilities(limits)).payload();
    assertEquals(limits, SealedTransport.response(payload, limits));
    assertEquals(limits, SealedTransport.negotiate(payload, limits));
    Map<String, Object> fields = SealedCbor.decode(payload, Wire.MAX_CONTROL_FRAME);
    for (String key : List.of("layer1-recursive", "layer2-resilience", "max-window-size", "max-scope-depth", "max-entities-per-scope", "keepalive-timeout-ms")) {
      var changed = new LinkedHashMap<>(fields);
      changed.put(key, switch (key) {
        case "layer1-recursive" -> false; case "layer2-resilience" -> true;
        default -> ((BigInteger) changed.get(key)).add(BigInteger.ONE);
      });
      assertThrows(ProtocolException.class, () -> SealedTransport.response(SealedCbor.encode(changed, 4096), limits), key);
    }
    var omitted = new LinkedHashMap<>(fields); omitted.remove("required-extensions");
    assertThrows(ProtocolException.class, () -> SealedTransport.negotiate(SealedCbor.encode(omitted, 4096), limits));
    var unknown = new LinkedHashMap<>(fields); unknown.put("supported-extensions", List.of(65000, SealedWork.EXTENSION));
    assertEquals(limits, SealedTransport.negotiate(SealedCbor.encode(unknown, 4096), limits));
    unknown.put("required-extensions", List.of(65000, SealedWork.EXTENSION));
    assertEquals(Wire.ERROR_EXTENSION_UNSUPPORTED, assertThrows(ProtocolException.class,
        () -> SealedTransport.negotiate(SealedCbor.encode(unknown, 4096), limits)).errorCode());
  }

  @Test void statusRejectsLayerTwoCursorAndInvalidExtensionButIgnoresReservedBits() throws Exception {
    byte[] payload = Wire.decodeControl(Wire.encodeStatus(new Wire.Status(2, 9, 7, null, 2))).payload();
    payload[3] = (byte) 0xff; payload[15] = 10;
    assertEquals(new Wire.Status(2, 9, 7, null, 2), SealedTransport.status(payload));
    byte[] extended = Arrays.copyOf(payload, 21); extended[1] |= (byte) 0x80; extended[19] = 1;
    assertEquals(new Wire.Status(2, 9, 7, null, 2), SealedTransport.status(extended));
    extended[19] = 2;
    assertEquals(Wire.ERROR_FRAME, assertThrows(ProtocolException.class, () -> SealedTransport.status(extended)).errorCode());
    assertEquals(Wire.ERROR_ENTITY_INVALID, assertThrows(ProtocolException.class, () -> SealedTransport.status(
        Wire.decodeControl(Wire.encodeStatus(new Wire.Status(0, Wire.CONNECTION_LEVEL, 0, 1L, 0))).payload())).errorCode());
    assertEquals(Wire.ERROR_LAYER_UNSUPPORTED, assertThrows(ProtocolException.class, () -> SealedTransport.status(
        Wire.decodeControl(Wire.encodeStatus(new Wire.Status(9, 1, 0, null, 0))).payload())).errorCode());
  }
}
