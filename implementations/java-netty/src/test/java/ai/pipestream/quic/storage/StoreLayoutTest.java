package ai.pipestream.quic.storage;

import static org.junit.jupiter.api.Assertions.*;

import java.util.HexFormat;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Locks the shared logical layout, including literal digest vectors computed
 * independently with python3 hashlib, so the independent Rust implementation
 * can certify byte-identical naming and scope digests against the same values.
 */
final class StoreLayoutTest {
  private static final HexFormat HEX = HexFormat.of();

  @Test void fanoutBucketIsTwoLowercaseHexDigits() {
    assertEquals(StoreLayout.FANOUT_BUCKETS, 256);
    assertEquals("df", StoreLayout.fanoutBucket(0));
    assertEquals("b4", StoreLayout.fanoutBucket(1));
    assertEquals("e3", StoreLayout.fanoutBucket(255));
    assertEquals("9f", StoreLayout.fanoutBucket(0x01020304));
    assertEquals("ad", StoreLayout.fanoutBucket(StoreLayout.MAX_ENTITY_ID));
    assertTrue(StoreLayout.fanoutBucket(0x89abcdefL).matches("[0-9a-f]{2}"));
    assertThrows(IllegalArgumentException.class, () -> StoreLayout.fanoutBucket(-1));
    assertThrows(IllegalArgumentException.class, () -> StoreLayout.fanoutBucket(StoreLayout.MAX_ENTITY_ID + 1));
  }

  @Test void namesAreFixedWidthLowercaseHex() {
    assertEquals("00000000", StoreLayout.scopeComponentName(0));
    assertEquals("deadbeef", StoreLayout.scopeComponentName(0xdeadbeefL));
    assertEquals("00000000", StoreLayout.entityName(0));
    assertEquals("001a2b3c", StoreLayout.entityName(0x1a2b3c));
    assertEquals("ffffffff", StoreLayout.entityName(StoreLayout.MAX_ENTITY_ID));
    assertThrows(IllegalArgumentException.class, () -> StoreLayout.scopeComponentName(-1));
    assertThrows(IllegalArgumentException.class, () -> StoreLayout.scopeComponentName(0x1_0000_0000L));
    assertThrows(IllegalArgumentException.class, () -> StoreLayout.entityName(-1));
    assertThrows(IllegalArgumentException.class, () -> StoreLayout.entityName(StoreLayout.MAX_ENTITY_ID + 1));
  }

  @Test void scopeDigestMatchesIndependentVectors() {
    assertEquals("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
        HEX.formatHex(StoreLayout.scopeDigest(List.of())));
    assertEquals("120c4e5a29b8f49b3401db6e5b5f065ac35a068c62efa3431553e3cd409136c9",
        HEX.formatHex(StoreLayout.scopeDigest(List.of(new StoreLayout.EntityDigest(1, new byte[32])))));
    byte[] first = new byte[32], middle = new byte[32], last = new byte[32];
    java.util.Arrays.fill(first, (byte) 0x11);
    java.util.Arrays.fill(middle, (byte) 0x22);
    java.util.Arrays.fill(last, (byte) 0x33);
    var ascending = List.of(new StoreLayout.EntityDigest(1, first),
        new StoreLayout.EntityDigest(0x100, middle), new StoreLayout.EntityDigest(0x89abcdefL, last));
    assertEquals("9f738d058efa80d9e0ae6e57c6f450f93fc5ca0028b4833ce0a59356642dd05f",
        HEX.formatHex(StoreLayout.scopeDigest(ascending)));
    var shuffled = List.of(ascending.get(2), ascending.get(0), ascending.get(1));
    assertArrayEquals(StoreLayout.scopeDigest(ascending), StoreLayout.scopeDigest(shuffled), "input order is irrelevant");
  }

  @Test void entityDigestDefendsItsCopyAndValidatesLength() {
    byte[] digest = new byte[32];
    var entity = new StoreLayout.EntityDigest(9, digest);
    digest[0] = 1;
    assertArrayEquals(new byte[32], entity.digest());
    byte[] exposed = entity.digest();
    exposed[1] = 1;
    assertArrayEquals(new byte[32], entity.digest());
    assertThrows(IllegalArgumentException.class, () -> new StoreLayout.EntityDigest(-1, new byte[32]));
    assertThrows(IllegalArgumentException.class, () -> new StoreLayout.EntityDigest(StoreLayout.MAX_ENTITY_ID + 1, new byte[32]));
    assertThrows(IllegalArgumentException.class, () -> new StoreLayout.EntityDigest(1, new byte[31]));
    assertThrows(IllegalArgumentException.class,
        () -> StoreLayout.scopeDigest(List.of(new StoreLayout.EntityDigest(1, new byte[32]),
            new StoreLayout.EntityDigest(1, new byte[32]))));
  }
}
