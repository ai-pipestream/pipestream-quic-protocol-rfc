package ai.pipestream.quic.storage;

import static org.junit.jupiter.api.Assertions.*;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.junit.jupiter.api.Test;

/**
 * Property tests attacking the canonical scope content digest from pinned
 * decision 3: SHA-256 over the concatenation, in strictly ascending entity-id
 * order, of {@code (0x00 || id-be32 || object-sha256-digest)} per entity, with
 * the empty scope hashing the empty input. Each random case is checked against
 * two independent recomputations written directly from the spec wording — one
 * incremental (field-by-field {@link MessageDigest#update} calls), one
 * one-shot over a freshly concatenated buffer — and per-entity object digests
 * are recomputed from the raw payloads rather than reused. Named vectors are
 * pinned from python3 hashlib. Seeded throughout; failure messages carry the
 * seed and iteration.
 */
final class PropertyModelScopeDigestTest {
  private static final HexFormat HEX = HexFormat.of();
  private static final long SEED = 0xd16e57_6aL;
  private static final int ITERATIONS = 500;

  private static MessageDigest sha256() {
    try {
      return MessageDigest.getInstance("SHA-256");
    } catch (NoSuchAlgorithmException impossible) {
      throw new IllegalStateException(impossible);
    }
  }

  private static byte[] bigEndian32(long value) {
    return new byte[] {(byte) (value >>> 24), (byte) (value >>> 16),
        (byte) (value >>> 8), (byte) value};
  }

  /** Spec-literal incremental recomputation: per-entity framed updates, ascending order. */
  private static byte[] independentIncrementalDigest(List<StoreLayout.EntityDigest> ascending) {
    MessageDigest hash = sha256();
    for (StoreLayout.EntityDigest entity : ascending) {
      hash.update((byte) 0);
      hash.update(bigEndian32(entity.entityId()));
      hash.update(entity.digest());
    }
    return hash.digest();
  }

  /** Spec-literal one-shot recomputation over one concatenated buffer. */
  private static byte[] independentOneShotDigest(List<StoreLayout.EntityDigest> ascending) {
    byte[] framed = new byte[ascending.size() * 37];
    int offset = 0;
    for (StoreLayout.EntityDigest entity : ascending) {
      framed[offset++] = 0;
      byte[] id = bigEndian32(entity.entityId());
      System.arraycopy(id, 0, framed, offset, 4);
      offset += 4;
      System.arraycopy(entity.digest(), 0, framed, offset, 32);
      offset += 32;
    }
    return sha256().digest(framed);
  }

  private static long randomU32(Random random) {
    return random.nextLong() & 0xffff_ffffL;
  }

  @Test void emptyScopeHashesTheEmptyInput() {
    assertEquals("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
        HEX.formatHex(StoreLayout.scopeDigest(List.of())));
  }

  @Test void scopeDigestMatchesIndependentRecomputationsForRandomEntitySets() {
    Random random = new Random(SEED);
    for (int i = 0; i < ITERATIONS; i++) {
      int size = random.nextInt(13);
      // Raw payloads feed independent per-entity digest recomputation.
      Map<Long, byte[]> payloads = new LinkedHashMap<>();
      while (payloads.size() < size) {
        long entityId = randomU32(random);
        byte[] payload = new byte[random.nextInt(129)];
        random.nextBytes(payload);
        payloads.putIfAbsent(entityId, payload);
      }
      // Build the digest input in a deliberately scrambled insertion order.
      List<Long> scrambled = new ArrayList<>(payloads.keySet());
      Collections.shuffle(scrambled, random);
      List<StoreLayout.EntityDigest> entities = new ArrayList<>();
      for (long entityId : scrambled) {
        byte[] objectDigest = sha256().digest(payloads.get(entityId));
        entities.add(new StoreLayout.EntityDigest(entityId, objectDigest));
      }
      List<StoreLayout.EntityDigest> ascending = new ArrayList<>(entities);
      ascending.sort(java.util.Comparator.comparingLong(StoreLayout.EntityDigest::entityId));

      byte[] actual = StoreLayout.scopeDigest(entities);
      String context = "seed " + SEED + " iteration " + i + " size " + size;
      assertArrayEquals(independentIncrementalDigest(ascending), actual,
          "incremental spec recomputation must agree: " + context);
      assertArrayEquals(independentOneShotDigest(ascending), actual,
          "one-shot spec recomputation must agree: " + context);
      // Input order must be irrelevant to the result.
      assertArrayEquals(StoreLayout.scopeDigest(ascending), actual,
          "ascending input must agree: " + context);
    }
  }

  @Test void scopeDigestMatchesPinnedPythonHashlibVectors() {
    byte[] first = new byte[32], middle = new byte[32], last = new byte[32];
    java.util.Arrays.fill(first, (byte) 0x11);
    java.util.Arrays.fill(middle, (byte) 0x22);
    java.util.Arrays.fill(last, (byte) 0x33);
    // python3: hashlib.sha256(b"\x00" + struct.pack(">I", id) + digest).digest()
    assertEquals("120c4e5a29b8f49b3401db6e5b5f065ac35a068c62efa3431553e3cd409136c9",
        HEX.formatHex(StoreLayout.scopeDigest(List.of(new StoreLayout.EntityDigest(1, new byte[32])))));
    var three = List.of(new StoreLayout.EntityDigest(1, first),
        new StoreLayout.EntityDigest(0x100, middle), new StoreLayout.EntityDigest(0x89abcdefL, last));
    assertEquals("9f738d058efa80d9e0ae6e57c6f450f93fc5ca0028b4833ce0a59356642dd05f",
        HEX.formatHex(StoreLayout.scopeDigest(three)));
    assertEquals("500fa6f323655dc31d0e2256d52d591e08c476cda4050615241dc09d081d8013",
        HEX.formatHex(StoreLayout.scopeDigest(List.of(new StoreLayout.EntityDigest(1, first)))));
    // A seven-entity random set, recomputed with python3 hashlib.
    var seven = List.of(entity(0xb5b62be6L, "41e7d1b17be0b10a4095f0c93f236080db5ec1fcf470a5ffb6ef74d66f114bb6"),
        entity(0x4f166f1cL, "5c637096607e2b70891225ff04f68adee52be2c49568f2209b0c56f55efe9ee1"),
        entity(0x4d4d5404L, "178c4d9a2b35dcbb9fd82a2b6d8e49e985bc5b07de7aa8f61ff99e06a41d27bf"),
        entity(0xb61de20bL, "2a082b9273a9adc72a6d8337d3e00a331b959c63fb6365fd21a881311369951f"),
        entity(0xf602f2f8L, "b8b9b37b80139f68fc382274dc8a59d19eb3b8aff1e751aa9ab894977ac069e0"),
        entity(0x8d5bf71bL, "9b711ae712e8b0d3ea4fc15d4bae623388c8b73c78cffa8eb0f14f57d8e4310f"),
        entity(0x99861143L, "f65c38f656231eb91ce1267c03ea393b433d2d71225926629f45a16baa6540ac"));
    assertEquals("9335c14fc46bab4ad9dcd4b60ed3f03b1b6a8dafbc016566564e9e3836001d6a",
        HEX.formatHex(StoreLayout.scopeDigest(seven)));
  }

  private static StoreLayout.EntityDigest entity(long entityId, String digestHex) {
    return new StoreLayout.EntityDigest(entityId, HEX.parseHex(digestHex));
  }

  @Test void duplicateEntityIdsAreRejected() {
    Random random = new Random(SEED ^ 0x5eed);
    for (int i = 0; i < 100; i++) {
      long entityId = randomU32(random);
      var list = List.of(new StoreLayout.EntityDigest(entityId, new byte[32]),
          new StoreLayout.EntityDigest(entityId, new byte[32]));
      assertThrows(IllegalArgumentException.class, () -> StoreLayout.scopeDigest(list),
          "duplicate id 0x" + Long.toHexString(entityId));
    }
  }
}
