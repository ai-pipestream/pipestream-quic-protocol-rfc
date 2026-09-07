package ai.pipestream.quic.v2;

import static ai.pipestream.quic.v2.ProtocolError.*;

import io.netty.util.NetUtil;

/**
 * A validated Section 11.6 V2 output name, never a credential or redirect.
 *
 * @param value original ASCII URI, retained verbatim for commitments; scheme syntax is
 *     case-insensitive
 */
public record Locator(String value) {
  /** Validates syntax without resolving a host or accessing the network. */
  public Locator {
    parse(value);
  }

  record Target(long generation, Records.WorkKey work, long attempt, int index) {}

  Target target() {
    return parse(value);
  }

  private static long decimal(String value, long minimum, long maximum) {
    require(
        !value.isEmpty() && (value.length() == 1 || value.charAt(0) != '0'),
        "noncanonical URI decimal");
    long n = 0;
    for (int i = 0; i < value.length(); i++) {
      char c = value.charAt(i);
      require(Checks.digit(c) && n <= (maximum - (c - '0')) / 10, "URI decimal exceeds range");
      n = n * 10 + c - '0';
    }
    return Checks.range(n, minimum, maximum);
  }

  private static Target parse(String value) {
    require(
        value != null
            && value.length() <= 1024
            && value.regionMatches(true, 0, "pipestream://", 0, 13),
        "invalid result URI scheme or length");
    for (int n = 0; n < value.length(); n++)
      require(value.charAt(n) >= 33 && value.charAt(n) <= 126, "non-ASCII URI");
    int slash = value.indexOf('/', 13);
    require(slash > 13, "missing result URI authority/path");
    String authority = value.substring(13, slash);
    int colon = authority.lastIndexOf(':');
    require(colon > 0, "explicit URI port required");
    decimal(authority.substring(colon + 1), 1, 65535);
    String host = authority.substring(0, colon);
    if (host.startsWith("[")) {
      require(
          host.endsWith("]") && host.indexOf('%') < 0 && NetUtil.isValidIpV6Address(host),
          "invalid IPv6 literal");
    } else {
      require(host.length() <= 253, "DNS name too long");
      for (String label : host.split("\\.", -1)) {
        require(!label.isEmpty() && label.length() <= 63, "invalid DNS label length");
        for (int n = 0; n < label.length(); n++) {
          char c = label.charAt(n);
          require(
              Checks.alpha(c) || Checks.digit(c) || c == '-' && n > 0 && n < label.length() - 1,
              "invalid DNS label");
        }
      }
    }
    String[] parts = value.substring(slash).split("/", -1);
    require(
        parts.length == 14 && parts[0].isEmpty() && parts[1].equals("v2"), "invalid V2 URI path");
    String[] tags = {"sessions", "scopes", "producers", "entities", "attempts", "outputs"};
    for (int n = 0; n < tags.length; n++)
      require(parts[2 + n * 2].equals(tags[n]), "invalid V2 URI path tag");
    return new Target(
        decimal(parts[3], 1, Long.MAX_VALUE),
        new Records.WorkKey(
            decimal(parts[5], 0, Long.MAX_VALUE),
            (int) decimal(parts[7], 0, 1),
            decimal(parts[9], 1, Long.MAX_VALUE)),
        decimal(parts[11], 1, Long.MAX_VALUE),
        (int) decimal(parts[13], 0, 255));
  }
}
