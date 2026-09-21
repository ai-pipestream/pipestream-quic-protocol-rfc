package ai.pipestream.quic.storage;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import org.junit.jupiter.api.Test;

final class ScopePathTest {
  @Test void depthBoundAcceptsEightComponentsAndRejectsDeeper() {
    assertEquals(0, ScopePath.root(7).depth());
    long[] deepest = {1, 2, 3, 4, 5, 6, 7, 8};
    assertEquals(ScopePath.MAX_DEPTH, ScopePath.of(deepest).depth());
    assertThrows(IllegalArgumentException.class,
        () -> ScopePath.of(1, 2, 3, 4, 5, 6, 7, 8, 9));
    assertThrows(IllegalArgumentException.class, () -> new ScopePath(List.of()));
  }

  @Test void componentsAreUnsignedThirtyTwoBit() {
    ScopePath.root(0);
    ScopePath.root(ScopePath.MAX_COMPONENT);
    assertThrows(IllegalArgumentException.class, () -> ScopePath.root(-1));
    assertThrows(IllegalArgumentException.class, () -> ScopePath.root(ScopePath.MAX_COMPONENT + 1));
    assertThrows(IllegalArgumentException.class, () -> ScopePath.of(1, -1));
    assertThrows(IllegalArgumentException.class, () -> ScopePath.of(ScopePath.MAX_COMPONENT + 1, 1));
  }

  @Test void listIsCopiedAndImmutable() {
    var components = new java.util.ArrayList<>(List.of(1L, 2L));
    var path = new ScopePath(components);
    components.add(3L);
    assertEquals(List.of(1L, 2L), path.components());
    assertThrows(UnsupportedOperationException.class, () -> path.components().add(3L));
  }

  @Test void renderIsCanonicalEightHexDigitsPerLevel() {
    assertEquals("00000007", ScopePath.root(7).render());
    assertEquals("00000001/00000002/ffffffff", ScopePath.of(1, 2, ScopePath.MAX_COMPONENT).render());
    assertEquals(ScopePath.of(0xdeadbeefL, 0x01020304L).render(), ScopePath.of(0xdeadbeefL, 0x01020304L).toString());
  }

  @Test void parseRoundTripsTheCanonicalRenderAndRejectsForeignForms() {
    assertEquals(ScopePath.root(0), ScopePath.parse("00000000"));
    var nested = ScopePath.parse("00000001/00000002/ffffffff");
    assertEquals(ScopePath.of(1, 2, ScopePath.MAX_COMPONENT), nested);
    assertEquals(nested, ScopePath.parse(nested.render()));
    assertThrows(IllegalArgumentException.class, () -> ScopePath.parse(""));
    assertThrows(IllegalArgumentException.class, () -> ScopePath.parse("1/2"));
    assertThrows(IllegalArgumentException.class, () -> ScopePath.parse("000000001"));
    assertThrows(IllegalArgumentException.class, () -> ScopePath.parse("0000000g"));
    assertThrows(IllegalArgumentException.class, () -> ScopePath.parse("00000001/"));
  }
}
