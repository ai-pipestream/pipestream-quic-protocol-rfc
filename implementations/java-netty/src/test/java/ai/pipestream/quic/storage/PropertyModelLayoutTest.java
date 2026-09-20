package ai.pipestream.quic.storage;

import static org.junit.jupiter.api.Assertions.*;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Random;
import org.junit.jupiter.api.Test;

/**
 * Property tests attacking the shared logical layout in {@link StoreLayout} and
 * {@link ScopePath}: the fanout bucket function, entity and scope naming, and
 * the protocol depth bound. The fanout property re-derives the bucket from the
 * pinned decision 2 wording — first byte of SHA-256 over the 4-octet
 * big-endian entity id — with an independent {@link MessageDigest} computation
 * written directly from the spec, over random entity ids spanning the full
 * unsigned 32-bit range. Named vectors are pinned from python3 hashlib so a
 * JCE regression cannot cancel itself out. All randomness is seeded; every
 * failure message carries the seed and iteration.
 */
final class PropertyModelLayoutTest {
  private static final HexFormat HEX = HexFormat.of();
  /** Fixed seed so every run is reproducible; change only together with the seed tag in messages. */
  private static final long SEED = 0x1a7007f7L;
  private static final int FANOUT_ITERATIONS = 2_000;
  private static final int ROUND_TRIP_ITERATIONS = 1_000;

  /** Independent spec-literal computation of pinned decision 2. */
  private static String independentFanoutBucket(long entityId) {
    byte[] id = bigEndian32(entityId);
    MessageDigest hash;
    try {
      hash = MessageDigest.getInstance("SHA-256");
    } catch (NoSuchAlgorithmException impossible) {
      throw new IllegalStateException(impossible);
    }
    byte[] digest = hash.digest(id);
    return HEX.formatHex(digest, 0, 1);
  }

  private static byte[] bigEndian32(long value) {
    return new byte[] {(byte) (value >>> 24), (byte) (value >>> 16),
        (byte) (value >>> 8), (byte) value};
  }

  private static long randomU32(Random random) {
    return random.nextLong() & 0xffff_ffffL;
  }

  @Test void fanoutBucketMatchesIndependentDigestOverFullU32Range() {
    Random random = new Random(SEED);
    // Edge ids first, then a uniform sweep of the whole unsigned 32-bit space.
    long[] edges = {0, 1, 255, 256, 0x7fff_ffffL, 0x8000_0000L, 0xffff_fffeL,
        StoreLayout.MAX_ENTITY_ID};
    for (long edge : edges) {
      assertEquals(independentFanoutBucket(edge), StoreLayout.fanoutBucket(edge),
          "edge id " + Long.toHexString(edge));
    }
    for (int i = 0; i < FANOUT_ITERATIONS; i++) {
      long entityId = randomU32(random);
      assertEquals(independentFanoutBucket(entityId), StoreLayout.fanoutBucket(entityId),
          "seed " + SEED + " iteration " + i + " id 0x" + Long.toHexString(entityId));
      String bucket = StoreLayout.fanoutBucket(entityId);
      assertTrue(bucket.matches("[0-9a-f]{2}"),
          "bucket must be 2 lowercase hex digits, got " + bucket);
      assertEquals(bucket, bucket.toLowerCase(), "bucket must be lowercase");
      assertTrue(Integer.parseInt(bucket, 16) < StoreLayout.FANOUT_BUCKETS,
          "bucket must address one of the " + StoreLayout.FANOUT_BUCKETS + " buckets");
    }
  }

  @Test void fanoutBucketMatchesPinnedPythonHashlibVectors() {
    // Computed independently with: hashlib.sha256(struct.pack(">I", i)).digest()[:1].hex()
    assertEquals("df", StoreLayout.fanoutBucket(0));
    assertEquals("b4", StoreLayout.fanoutBucket(1));
    assertEquals("e3", StoreLayout.fanoutBucket(255));
    assertEquals("9f", StoreLayout.fanoutBucket(0x01020304));
    assertEquals("b4", StoreLayout.fanoutBucket(0x89abcdefL));
    assertEquals("ad", StoreLayout.fanoutBucket(0xffffffffL));
    assertEquals("5f", StoreLayout.fanoutBucket(0xdeadbeefL));
    assertEquals("50", StoreLayout.fanoutBucket(0x80000000L));
    assertEquals("b8", StoreLayout.fanoutBucket(123456789));
    assertEquals("49", StoreLayout.fanoutBucket(0xfffffff0L));
  }

  @Test void entityNameRoundTripsAcrossFullU32Range() {
    Random random = new Random(SEED ^ 0x5d1f);
    long[] edges = {0, 1, 0xffff_fffeL, StoreLayout.MAX_ENTITY_ID};
    for (long edge : edges) {
      assertEntityNameRoundTrip(edge);
    }
    for (int i = 0; i < ROUND_TRIP_ITERATIONS; i++) {
      assertEntityNameRoundTrip(randomU32(random));
    }
  }

  private static void assertEntityNameRoundTrip(long entityId) {
    String name = StoreLayout.entityName(entityId);
    assertEquals(8, name.length(), "entity name for " + Long.toHexString(entityId));
    assertTrue(name.matches("[0-9a-f]{8}"), "entity name must be 8 lowercase hex digits: " + name);
    assertEquals(entityId, Long.parseUnsignedLong(name, 16), "entity name round-trip");
    // Pinned decision 1 renders scope components with the same 8-hex convention.
    assertEquals(StoreLayout.scopeComponentName(entityId), name,
        "entity and scope component naming must agree");
  }

  @Test void scopePathRenderParseRoundTrips() {
    Random random = new Random(SEED ^ 0xacce55);
    for (int i = 0; i < ROUND_TRIP_ITERATIONS; i++) {
      ScopePath scope = randomScope(random);
      String rendered = scope.render();
      assertEquals(scope, ScopePath.parse(rendered), "render/parse round-trip of " + rendered);
      assertEquals(rendered, ScopePath.parse(rendered).render(), "parse/render round-trip");
      assertEquals(independentRender(scope.components()), rendered,
          "render must equal the spec-literal join of 8-hex components");
      String[] levels = rendered.split("/", -1);
      assertEquals(scope.components().size(), levels.length, "one rendered level per component");
      for (String level : levels) {
        assertTrue(level.matches("[0-9a-f]{8}"), "level must be 8 lowercase hex digits: " + level);
      }
      assertEquals(scope.depth(), scope.components().size() - 1, "depth is one less than levels");
    }
  }

  /** Renders one component per pinned decision 1 without touching production code. */
  private static String independentRender(List<Long> components) {
    StringBuilder name = new StringBuilder();
    for (int i = 0; i < components.size(); i++) {
      if (i > 0) name.append('/');
      name.append(String.format(java.util.Locale.ROOT, "%08x", components.get(i)));
    }
    return name.toString();
  }

  private static ScopePath randomScope(Random random) {
    int levels = 1 + random.nextInt(ScopePath.MAX_DEPTH + 1);
    long[] components = new long[levels];
    for (int i = 0; i < levels; i++) {
      components[i] = randomU32(random);
    }
    return ScopePath.of(components);
  }

  @Test void depthIsEnforcedToProtocolRangeZeroThroughSeven() {
    Random random = new Random(SEED ^ 0xd3077L);
    assertEquals(7, ScopePath.MAX_DEPTH, "protocol bound is the 3-bit depth field, 0-7");
    // Depth 0 through 7 (1 to 8 components) is constructible and renders/parses.
    for (int levels = 1; levels <= ScopePath.MAX_DEPTH + 1; levels++) {
      long[] components = new long[levels];
      for (int i = 0; i < levels; i++) components[i] = randomU32(random);
      ScopePath scope = ScopePath.of(components);
      assertEquals(levels - 1, scope.depth());
      assertEquals(scope, ScopePath.parse(scope.render()));
      // The canonical parse must accept exactly the same maximum depth.
      assertEquals(scope, ScopePath.parse(independentRender(scope.components())));
    }
    // One level below depth 0 (empty) and one above depth 7 (9 components) is rejected,
    // both through the constructor and through the canonical parse.
    assertThrows(IllegalArgumentException.class, () -> new ScopePath(List.of()));
    assertThrows(IllegalArgumentException.class,
        () -> ScopePath.of(1, 2, 3, 4, 5, 6, 7, 8, 9));
    assertThrows(IllegalArgumentException.class,
        () -> ScopePath.parse("00000001/00000002/00000003/00000004/00000005/"
            + "00000006/00000007/00000008/00000009"));
    assertThrows(IllegalArgumentException.class, () -> ScopePath.parse(""));
  }

  @Test void parseRejectsNonCanonicalRenderings() {
    Random random = new Random(SEED ^ 0xbada55);
    for (int i = 0; i < 200; i++) {
      ScopePath scope = randomScope(random);
      String rendered = scope.render();
      List<String> levels = new ArrayList<>(List.of(rendered.split("/", -1)));
      int mutated = random.nextInt(levels.size());
      String original = levels.get(mutated);
      String broken = switch (random.nextInt(4)) {
        case 0 -> original.toUpperCase(java.util.Locale.ROOT);
        case 1 -> original.substring(1);
        case 2 -> original + "0";
        default -> original.replace('0', 'g');
      };
      // All-digit levels survive uppercasing and all-nonzero levels survive the
      // '0'->'g' rewrite; fall back to a length violation so every mutation
      // genuinely leaves the canonical rendering.
      if (broken.equals(original)) {
        broken = original + "0";
      }
      levels.set(mutated, broken);
      String corrupted = String.join("/", levels);
      assertThrows(IllegalArgumentException.class, () -> ScopePath.parse(corrupted),
          "non-canonical rendering must not parse: " + corrupted);
    }
    assertThrows(IllegalArgumentException.class, () -> ScopePath.parse("00000001/"),
        "trailing empty level");
    assertThrows(IllegalArgumentException.class, () -> ScopePath.parse("/00000001"),
        "leading empty level");
    assertThrows(NullPointerException.class, () -> ScopePath.parse(null));
  }

  @Test void componentsAreUnsigned32BitValidated() {
    assertThrows(IllegalArgumentException.class, () -> ScopePath.root(-1));
    assertThrows(IllegalArgumentException.class, () -> ScopePath.root(ScopePath.MAX_COMPONENT + 1));
    assertThrows(IllegalArgumentException.class, () -> StoreLayout.entityName(-1));
    assertThrows(IllegalArgumentException.class, () -> StoreLayout.entityName(StoreLayout.MAX_ENTITY_ID + 1));
    assertThrows(IllegalArgumentException.class, () -> StoreLayout.fanoutBucket(-1));
    assertThrows(IllegalArgumentException.class,
        () -> StoreLayout.fanoutBucket(StoreLayout.MAX_ENTITY_ID + 1));
    assertEquals(0xffff_ffffL, ScopePath.MAX_COMPONENT);
    assertEquals(0xffff_ffffL, StoreLayout.MAX_ENTITY_ID);
  }
}
