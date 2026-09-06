package ai.pipestream.quic;

import static org.junit.jupiter.api.Assertions.*;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

final class SealedProducerInputsTest {
  @TempDir Path directory;
  private static final SealedWork.EntityKey KEY = new SealedWork.EntityKey(7, 1), PARENT = new SealedWork.EntityKey(0, 1);

  private SealedClient.FileChunk file(String text, SealedTransport.Chunk chunk, BigInteger length, byte[] checksum) throws Exception {
    Path path = Files.createTempFile(directory, "input", ".bin"); Files.writeString(path, text);
    return new SealedClient.FileChunk(new SealedTransport.Header(KEY, PARENT, 0, "text/plain", length, checksum, Map.of("tag", "value"), chunk), path);
  }

  @Test void absentCommitmentsAreFilledWithoutChangingTheCallersHeader() throws Exception {
    var original = file("input", null, null, null); var prepared = SealedProducerInputs.prepare(List.of(original));
    assertNull(original.header().checksum()); assertNull(original.header().payloadLength());
    var actual = SealedProducerInputs.first(prepared.descriptor());
    assertEquals(BigInteger.valueOf(5), actual.payloadLength());
    assertArrayEquals(SealedWork.sha256().digest(Files.readAllBytes(original.payload())), actual.checksum());
    assertEquals(original.header().metadata(), actual.metadata()); assertEquals(PARENT, actual.parent());
    byte[] modified = prepared.descriptor(); modified[0] ^= 1;
    assertNotEquals(modified[0], prepared.descriptor()[0]);
    Files.delete(original.payload());
    assertEquals(KEY, SealedProducerInputs.first(prepared.descriptor()).key(), "restore does not reopen original producer files");
  }

  @Test void changedLengthOrChecksumRefusesBeforeAnIntentCanBeStored() throws Exception {
    assertEquals(5, assertThrows(ProtocolException.class, () -> SealedProducerInputs.prepare(List.of(file("input", null, BigInteger.ONE, null)))).errorCode());
    assertEquals(4, assertThrows(ProtocolException.class, () -> SealedProducerInputs.prepare(List.of(file("input", null, null, new byte[32])))).errorCode());
    assertEquals(6, assertThrows(ProtocolException.class, () -> SealedProducerInputs.prepare(List.of())).errorCode());
  }

  @Test void completeOutOfOrderChunkCommitmentsRoundTripAndGapsRefuse() throws Exception {
    var first = file("abc", new SealedTransport.Chunk(BigInteger.TWO, BigInteger.ZERO, BigInteger.ZERO), null, null);
    var second = file("def", new SealedTransport.Chunk(BigInteger.TWO, BigInteger.ONE, BigInteger.valueOf(3)), null, null);
    var prepared = SealedProducerInputs.prepare(List.of(second, first));
    assertEquals(BigInteger.ONE, SealedProducerInputs.first(prepared.descriptor()).chunk().index());
    assertEquals(2, prepared.files().size());
    var gap = file("def", new SealedTransport.Chunk(BigInteger.TWO, BigInteger.ONE, BigInteger.valueOf(4)), null, null);
    assertEquals(4, assertThrows(ProtocolException.class, () -> SealedProducerInputs.prepare(List.of(first, gap))).errorCode());
    assertEquals(4, assertThrows(ProtocolException.class, () -> SealedProducerInputs.prepare(List.of(first, first))).errorCode());
  }

  @Test void corruptedDescriptorLengthsAndTrailingBytesRefuse() throws Exception {
    byte[] original = SealedProducerInputs.prepare(List.of(file("input", null, null, null))).descriptor();
    for (int length : List.of(0, 7, 11, 12, 16, original.length - 1)) {
      assertThrows(ProtocolException.class, () -> SealedProducerInputs.first(Arrays.copyOf(original, length)));
    }
    byte[] oversized = original.clone(); ByteBuffer.wrap(oversized).putInt(12, Integer.MAX_VALUE);
    assertEquals(4, assertThrows(ProtocolException.class, () -> SealedProducerInputs.first(oversized)).errorCode());
    byte[] framing = original.clone(); ByteBuffer.wrap(framing).putInt(16, 1);
    assertEquals(4, assertThrows(ProtocolException.class, () -> SealedProducerInputs.first(framing)).errorCode());
    assertEquals(4, assertThrows(ProtocolException.class, () -> SealedProducerInputs.first(Arrays.copyOf(original, original.length + 1))).errorCode());
  }
}
