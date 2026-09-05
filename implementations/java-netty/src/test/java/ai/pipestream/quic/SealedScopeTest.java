package ai.pipestream.quic;

import static org.junit.jupiter.api.Assertions.*;

import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HexFormat;
import java.util.List;
import org.junit.jupiter.api.Test;

final class SealedScopeTest {
  private static final HexFormat HEX = HexFormat.of();

  @Test void frozenScopeDigestHasExactIndependentEncoding() throws Exception {
    for (String line : Files.readAllLines(Path.of("../../test-vectors/recursive/index.tsv")).stream().skip(1).toList()) {
      String[] fields = line.split("\t");
      if (!fields[1].equals("scope-digest")) continue;
      byte[] frame = HEX.parseHex(fields[5]);
      if (fields[3].equals("valid")) assertArrayEquals(frame, SealedScope.encode(SealedScope.decode(frame)));
      else assertEquals(fields[4], assertThrows(ProtocolException.class, () -> SealedScope.decode(frame)).errorName());
    }
  }

  @Test void oddNodePromotionAndStatusDomainSeparationMatchLiteralHashes() throws Exception {
    var leaves = List.of(new SealedScope.Terminal(10, 3), new SealedScope.Terminal(20, 4), new SealedScope.Terminal(30, 3));
    var digest = SealedScope.summarize(7, leaves);
    assertEquals("fbce897c6546788c036b3cf7ade586b398f33cc248d4d68b0b8c8d1b94726e41", HEX.formatHex(digest.merkleRoot()));
    assertEquals(BigInteger.valueOf(3), digest.processed());
    assertEquals(BigInteger.TWO, digest.succeeded());
    assertEquals(BigInteger.ONE, digest.failed());
    assertEquals("f9677b5b014e488d85efb0489d02ba45102fa2836f5f0ad15731f41c19d0a976",
        HEX.formatHex(SealedScope.summarize(7, leaves.subList(0, 1)).merkleRoot()));
    assertEquals(digest, SealedScope.decode(SealedScope.encode(digest)));
    byte[] exposed = digest.merkleRoot(); exposed[0] ^= 1;
    assertEquals(digest, SealedScope.decode(SealedScope.encode(digest)));
  }

  @Test void unsignedCountersReservedBitsAndImpossibleCountsAreChecked() throws Exception {
    var maximum = new SealedScope.Digest(0xffff_ffffL, SealedCbor.MAX_UINT,
        SealedCbor.MAX_UINT.subtract(BigInteger.ONE), BigInteger.ONE, new byte[32]);
    byte[] encoded = SealedScope.encode(maximum);
    assertEquals(maximum, SealedScope.decode(encoded));
    encoded[5] = (byte) 0xff; encoded[8] = 1;
    assertEquals(maximum, SealedScope.decode(encoded), "receiver ignores reserved fields");
    encoded[44] = 1;
    assertEquals("PIPESTREAM_SCOPE_INVALID", assertThrows(ProtocolException.class, () -> SealedScope.decode(encoded)).errorName());
    assertThrows(ProtocolException.class, () -> SealedScope.encode(new SealedScope.Digest(1,
        BigInteger.ONE, BigInteger.ONE, BigInteger.ONE, new byte[32])));
    assertThrows(ProtocolException.class, () -> SealedScope.summarize(1, List.of()));
    assertThrows(ProtocolException.class, () -> SealedScope.summarize(0, List.of(new SealedScope.Terminal(1, 3))));
    for (List<SealedScope.Terminal> invalid : List.of(
        List.of(new SealedScope.Terminal(2, 3), new SealedScope.Terminal(1, 3)),
        List.of(new SealedScope.Terminal(1, 3), new SealedScope.Terminal(1, 4)),
        List.of(new SealedScope.Terminal(1, 2)), List.of(new SealedScope.Terminal(1, 9)))) {
      assertThrows(ProtocolException.class, () -> SealedScope.summarize(1, invalid));
    }
  }
}
