package ai.pipestream.quic;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import org.junit.jupiter.api.Test;

final class TlsPeerIdentityTest {
  @Test void exactNamesAndAsciiAlabelsMatchCaseInsensitively() {
    assertTrue(TlsPeerIdentity.matchesDns("localhost", "LOCALHOST"));
    assertTrue(TlsPeerIdentity.matchesDns("WWW.Example.COM", "www.example.com"));
    assertTrue(TlsPeerIdentity.matchesDns("XN--BCHER-KVA.example", "xn--bcher-kva.example"));
    assertFalse(TlsPeerIdentity.matchesDns("www.example.com", "other.example.com"));
    assertFalse(TlsPeerIdentity.matchesDns("key.example", "\u212aey.example"), "Unicode case folding must not create an ASCII certificate match");
  }

  @Test void wildcardMatchesExactlyOneCompleteLeftMostLabel() {
    assertTrue(TlsPeerIdentity.matchesDns("www.example.com", "*.example.com"));
    assertFalse(TlsPeerIdentity.matchesDns("example.com", "*.example.com"));
    assertFalse(TlsPeerIdentity.matchesDns("two.levels.example.com", "*.example.com"));
    for (String presented : List.of("w*.example.com", "www.*.com", "*.*.com", "*.example.*", "*", "*example.com")) {
      assertFalse(TlsPeerIdentity.matchesDns("www.example.com", presented), presented);
    }
  }

  @Test void malformedReferenceNamesCannotBecomeCertificateMatches() {
    for (String invalid : List.of("", ".example", "example.", "two..labels", "*.example.com", "_service.example", "bad name.example",
        "name:443", "name\u0000.example", "b\u00fccher.example", "-label.example", "label-.example", "a".repeat(64) + ".example")) {
      assertFalse(TlsPeerIdentity.matchesDns(invalid, invalid), invalid);
    }
    assertFalse(TlsPeerIdentity.matchesDns(null, "localhost"));
    assertFalse(TlsPeerIdentity.matchesDns("localhost", null));
  }
}
