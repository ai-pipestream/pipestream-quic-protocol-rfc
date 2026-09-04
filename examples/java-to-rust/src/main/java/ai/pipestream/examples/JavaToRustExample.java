package ai.pipestream.examples;

import ai.pipestream.quic.PipeStreamClient;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;

/** Java client example that transfers one entity to a PipeStream Layer 0 server. */
public final class JavaToRustExample {
  private JavaToRustExample() {}

  /**
   * Runs the Java-to-Rust transfer.
   *
   * @param arguments named command-line options
   */
  public static void main(String[] arguments) {
    try {
      Map<String, String> options = parseOptions(arguments);
      long entityId = Long.parseUnsignedLong(options.getOrDefault("entity-id", "101"));
      Long parentId = options.containsKey("parent-id")
          ? Long.parseUnsignedLong(options.get("parent-id"))
          : null;
      PipeStreamClient.send(
          address(required(options, "connect")),
          Path.of(required(options, "ca")),
          options.getOrDefault("server-name", "localhost"),
          entityId,
          Path.of(required(options, "input")),
          options.getOrDefault("content-type", "application/octet-stream"),
          parentId);
      System.out.printf(
          "JAVA EXAMPLE COMPLETE entity=%s%n", Long.toUnsignedString(entityId));
    } catch (Exception failure) {
      System.err.println(failure.getMessage());
      System.exit(1);
    }
  }

  private static Map<String, String> parseOptions(String[] arguments) {
    LinkedHashMap<String, String> options = new LinkedHashMap<>();
    for (int index = 0; index < arguments.length; index++) {
      String argument = arguments[index];
      if (!argument.startsWith("--") || argument.length() == 2) {
        throw new IllegalArgumentException("expected a named option, found: " + argument);
      }
      if (++index == arguments.length) {
        throw new IllegalArgumentException("missing value for " + argument);
      }
      String previous = options.put(argument.substring(2), arguments[index]);
      if (previous != null) {
        throw new IllegalArgumentException("duplicate option: " + argument);
      }
    }
    return options;
  }

  private static String required(Map<String, String> options, String name) {
    String value = options.get(name);
    if (value == null || value.isBlank()) {
      throw new IllegalArgumentException("missing --" + name);
    }
    return value;
  }

  private static InetSocketAddress address(String value) {
    int separator = value.lastIndexOf(':');
    if (separator < 1 || separator == value.length() - 1) {
      throw new IllegalArgumentException("address must be host:port: " + value);
    }
    String host = value.substring(0, separator);
    int port = Integer.parseUnsignedInt(value.substring(separator + 1));
    if (port > 65_535) {
      throw new IllegalArgumentException("port exceeds 65535: " + port);
    }
    return new InetSocketAddress(host, port);
  }
}
