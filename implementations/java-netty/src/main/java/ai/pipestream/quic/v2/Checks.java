package ai.pipestream.quic.v2;

import static ai.pipestream.quic.v2.ProtocolError.*;

import java.util.List;

final class Checks {
  private Checks() {}

  static long range(long value, long minimum, long maximum) {
    require(value >= minimum && value <= maximum, "integer outside schema range");
    return value;
  }

  static long number(long value) {
    return range(value, 0, Long.MAX_VALUE);
  }

  static long id(long value) {
    return range(value, 1, Long.MAX_VALUE);
  }

  static long duration(long value) {
    return range(value, 1, 31536000000L);
  }

  static int producer(int value) {
    range(value, 0, 1);
    return value;
  }

  static String identity(String value) {
    require(value != null && !value.isEmpty() && value.length() <= 128, "invalid identity length");
    for (int n = 0; n < value.length(); n++) {
      char c = value.charAt(n);
      require(alpha(c) || digit(c) || "-._~".indexOf(c) >= 0, "invalid identity character");
    }
    return value;
  }

  static String label(String value) {
    require(value != null && !value.isEmpty() && value.length() <= 128, "invalid label length");
    for (int n = 0; n < value.length(); n++)
      require(value.charAt(n) >= 32 && value.charAt(n) <= 126, "nonprintable application label");
    return value;
  }

  static boolean alpha(char c) {
    return c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z';
  }

  static boolean digit(char c) {
    return c >= '0' && c <= '9';
  }

  static <T> T present(T value) {
    require(value != null, "missing required field");
    return value;
  }

  static <T> List<T> list(List<T> value, int maximum) {
    require(value != null && value.size() <= maximum, "collection exceeds schema bound");
    for (T item : value) present(item);
    return List.copyOf(value);
  }

  static long sum(long left, long right) {
    number(left);
    number(right);
    require(right <= Long.MAX_VALUE - left, "aggregate overflows schema range");
    return left + right;
  }

  static void scope(long scope, int producer, Records.WorkKey parent) {
    number(scope);
    producer(producer);
    require(
        scope == 0 ? parent == null && producer == 0 : parent != null && parent.scope() < scope,
        "invalid scope parent/producer relationship");
  }
}
