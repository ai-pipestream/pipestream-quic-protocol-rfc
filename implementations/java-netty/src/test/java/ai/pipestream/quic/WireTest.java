package ai.pipestream.quic;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;

final class WireTest {
  private static final Path VECTORS = Path.of("..", "..", "test-vectors");

  @Test
  void capabilitiesMatchGoldenVector() throws Exception {
    assertArrayEquals(
        Files.readAllBytes(VECTORS.resolve("valid/capabilities-default.bin")),
        Wire.encodeCapabilities(Wire.Capabilities.defaults()));
  }

  @Test
  void entityMatchesGoldenVector() throws Exception {
    assertArrayEquals(
        Files.readAllBytes(VECTORS.resolve("valid/entity-text.bin")),
        Wire.encodeEntity(7, "PipeStream Layer 0\n".getBytes(), "text/plain; charset=utf-8"));
  }

  @Test
  void entireCorpusHasExpectedAcceptanceAndNamedRefusals() throws Exception {
    List<String> rows = Files.readAllLines(VECTORS.resolve("index.tsv"));
    for (String row : rows.subList(1, rows.size())) {
      String[] fields = row.split("\t", -1);
      String name = fields[0];
      String expectation = fields[2];
      byte[] input = Files.readAllBytes(
          VECTORS.resolve(expectation).resolve(name + ".bin"));
      if ("valid".equals(expectation)) {
        decodeNamed(name, input);
      } else {
        ProtocolException exception = assertThrows(
            ProtocolException.class, () -> decodeNamed(name, input), name);
        assertEquals(fields[3], exception.errorName(), name);
      }
    }
  }

  private static void decodeNamed(String name, byte[] input) throws ProtocolException {
    if (name.startsWith("entity-")) {
      Wire.decodeEntity(input);
      return;
    }
    Wire.ControlFrame frame = Wire.decodeControl(input);
    if (name.startsWith("capabilities-") || name.startsWith("cbor-")) {
      Wire.decodeCapabilities(frame.payload());
    } else if (name.startsWith("status-")) {
      Wire.decodeStatus(frame.payload());
    } else if (name.startsWith("goaway")) {
      Wire.decodeGoaway(frame.payload());
    } else if (name.startsWith("checkpoint-")) {
      Wire.decodeCheckpoint(frame.payload());
    }
  }
}
