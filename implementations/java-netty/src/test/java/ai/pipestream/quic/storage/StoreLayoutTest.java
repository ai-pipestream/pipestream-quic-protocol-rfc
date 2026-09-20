package ai.pipestream.quic.storage;

import static org.junit.jupiter.api.Assertions.*;

import java.util.HexFormat;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Locks the shared logical layout, including literal digest vectors, so the
 * independent Rust implementation can certify byte-identical naming and scope
 * digests against the same values.
 */
final class StoreLayoutTest {
  private static final HexFormat HEX = HexFormat.of();

  @Test void fanoutBucketIsTwoLowercaseHexDigits() {
    assertEquals(StoreLayout.FANOUT_BUCKETS, 256);
    assertEquals("af", StoreLayout.fanoutBucket(0));
    assertEquals("cd", StoreLayout.fanoutBucket(1));
    assertEquals("b8", StoreLayout.fanoutBucket(255));
    assertEquals("66", StoreLayout.fanoutBucket(0x0102030405060708L));
    assertTrue(StoreLayout.fanoutBucket(Long.MAX_VALUE).matches("[0-9a-f]{2}"));
    assertThrows(IllegalArgumentException.class, () -> StoreLayout.fanoutBucket(-1));
  }

  @Test void namesAreFixedWidthLowercaseHex() {
    assertEquals("00000000", StoreLayout.scopeComponentName(0));
    assertEquals("deadbeef", StoreLayout.scopeComponentName(0xdeadbeefL));
    assertEquals("0000000000000000", StoreLayout.entityName(0));
    assertEquals("00000000001a2b3c", StoreLayout.entityName(0x1a2b3c));
    assertEquals("7fffffffffffffff", StoreLayout.entityName(StoreLayout.MAX_ENTITY_ID));
    assertThrows(IllegalArgumentException.class, () -> StoreLayout.scopeComponentName(-1));
    assertThrows(IllegalArgumentException.class, () -> StoreLayout.scopeComponentName(0x1_0000_0000L));
    assertThrows(IllegalArgumentException.class, () -> StoreLayout.entityName(-1));
    assertThrows(IllegalArgumentException.class, () -> StoreLayout.entityName(StoreLayout.MAX_ENTITY_ID + 1));
  }

  @Test void scopeDigestMatchesIndependentVectors() {
    assertEquals("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
        HEX.formatHex(StoreLayout.scopeDigest(List.of())));
    assertEquals("c6f95ebdeb1dc8dcf5bb0518d73446c98d17c60867993647318c5034aac8ff7e",
        HEX.formatHex(StoreLayout.scopeDigest(List.of(new StoreLayout.EntityDigest(1, new byte[32])))));
    byte[] first = new byte[32], middle = new byte[32], last = new byte[32];
    java.util.Arrays.fill(first, (byte) 0x11);
    java.util.Arrays.fill(middle, (byte) 0x22);
    java.util.Arrays.fill(last, (byte) 0x33);
    var ascending = List.of(new StoreLayout.EntityDigest(1, first),
        new StoreLayout.EntityDigest(0x100, middle), new StoreLayout.EntityDigest(0x0102030405060708L, last));
    assertEquals("6b682fbb65dab84e15ab0a50cb82e0c82f00eda5d1b19f065b94921ea85ee091",
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
    assertThrows(IllegalArgumentException.class, () -> new StoreLayout.EntityDigest(1, new byte[31]));
    assertThrows(IllegalArgumentException.class,
        () -> StoreLayout.scopeDigest(List.of(new StoreLayout.EntityDigest(1, new byte[32]),
            new StoreLayout.EntityDigest(1, new byte[32]))));
  }
}
