package ai.pipestream.quic;

import io.netty.util.NetUtil;
import java.security.cert.CertificateParsingException;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.Locale;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;

/**
 * DNS-ID/IP-ID checks after PKIX validation, before application data is sent.
 * Implements RFC 9525 Sections 6.3 and 6.4 for ASCII reference names and literal
 * IP addresses. Callers supply IDNA A-labels, not Unicode U-labels. Common Names
 * and unrelated SAN types are never used as a fallback.
 */
final class TlsPeerIdentity {
  private TlsPeerIdentity() {}

  /**
   * Checks the leaf SAN against the caller's independent reference identity.
   * This does not replace the transport's certificate-chain validation.
   * @param session successfully authenticated TLS session
   * @param reference ASCII DNS name or IP literal, without a port or zone identifier
   * @throws SSLPeerUnverifiedException for missing or mismatched service identity
   */
  static void verify(SSLSession session, String reference) throws SSLPeerUnverifiedException {
    if (reference == null || reference.isEmpty() || reference.length() > 253 || reference.indexOf('%') >= 0) throw refusal();
    byte[] ip = NetUtil.createByteArrayFromIpAddressString(reference);
    if (ip == null && !dns(reference)) throw refusal();
    var chain = session.getPeerCertificates();
    if (chain.length == 0 || !(chain[0] instanceof X509Certificate leaf)) throw refusal();
    try {
      var names = leaf.getSubjectAlternativeNames();
      if (names == null) throw refusal();
      for (var name : names) {
        if (name.size() != 2 || !(name.get(0) instanceof Integer type) || !(name.get(1) instanceof String value)) continue;
        if (ip != null && type == 7 && Arrays.equals(ip, NetUtil.createByteArrayFromIpAddressString(value))) return;
        if (ip == null && type == 2 && matchesDns(reference, value)) return;
      }
    } catch (CertificateParsingException invalid) {
      var failure = refusal(); failure.initCause(invalid); throw failure;
    }
    throw refusal();
  }

  /**
   * Matches DNS labels, permitting only a whole left-most wildcard label.
   * @param reference ASCII reference DNS name, never a wildcard
   * @param presented certificate dNSName SAN
   * @return true only for an exact label match or one matching wildcard label
   */
  static boolean matchesDns(String reference, String presented) {
    if (!dns(reference) || presented == null) return false;
    if (!dns(presented.startsWith("*.") ? presented.substring(2) : presented)) return false;
    String expected = reference.toLowerCase(Locale.ROOT), actual = presented.toLowerCase(Locale.ROOT);
    if (actual.startsWith("*.")) {
      String suffix = actual.substring(2);
      int dot = expected.indexOf('.');
      return dns(suffix) && dot > 0 && expected.substring(dot + 1).equals(suffix);
    }
    return dns(actual) && expected.equals(actual);
  }

  private static boolean dns(String name) {
    if (name == null || name.isEmpty() || name.length() > 253) return false;
    int labelStart = 0;
    for (int i = 0; i <= name.length(); i++) {
      if (i == name.length() || name.charAt(i) == '.') {
        int length = i - labelStart;
        if (length < 1 || length > 63 || name.charAt(labelStart) == '-' || name.charAt(i - 1) == '-') return false;
        labelStart = i + 1;
      } else {
        char ch = name.charAt(i);
        if (!((ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9') || ch == '-')) return false;
      }
    }
    return true;
  }

  private static SSLPeerUnverifiedException refusal() {
    return new SSLPeerUnverifiedException("server certificate SAN does not match the configured service identity");
  }
}
