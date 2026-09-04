package ai.pipestream.quic;

import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/** Command-line entry point for the Netty reference implementation. */
public final class Main {
  private Main() {}

  /**
   * Runs the standalone server or client.
   *
   * @param arguments command and named options
   */
  public static void main(String[] arguments) {
    try {
      run(arguments);
    } catch (Exception failure) {
      System.err.println(failure.getMessage());
      System.exit(1);
    }
  }

  private static void run(String[] arguments) throws Exception {
    if (arguments.length == 0 || "--help".equals(arguments[0])) {
      usage();
      return;
    }
    String command = arguments[0];
    Map<String, String> options = options(arguments);
    if ("serve".equals(command)) {
      InetSocketAddress bind = address(options.getOrDefault("bind", "127.0.0.1:0"));
      Path certificate = requiredPath(options, "cert");
      Path key = requiredPath(options, "key");
      Path output = requiredPath(options, "output-dir");
      try (PipeStreamServer server = PipeStreamServer.start(bind, certificate, key, output)) {
        String ready = server.address().getHostString() + ":" + server.address().getPort() + System.lineSeparator();
        if (options.containsKey("ready-file")) {
          Files.writeString(Path.of(options.get("ready-file")), ready);
        }
        System.out.print("READY " + ready);
        System.out.flush();
        if (options.containsKey("once")) {
          server.awaitFirstSession(60, TimeUnit.SECONDS);
        } else {
          new java.util.concurrent.CountDownLatch(1).await();
        }
      }
      return;
    }
    if ("send".equals(command)) {
      PipeStreamClient.send(
          address(required(options, "connect")),
          requiredPath(options, "ca"),
          options.getOrDefault("server-name", "localhost"),
          Long.parseUnsignedLong(required(options, "entity-id")),
          requiredPath(options, "input"),
          options.getOrDefault("content-type", "application/octet-stream"),
          options.containsKey("parent-id")
              ? Long.parseUnsignedLong(options.get("parent-id"))
              : null);
      return;
    }
    throw new IllegalArgumentException("unknown command: " + command);
  }

  private static Map<String, String> options(String[] arguments) {
    LinkedHashMap<String, String> result = new LinkedHashMap<>();
    for (int index = 1; index < arguments.length; index++) {
      String argument = arguments[index];
      if (!argument.startsWith("--")) {
        throw new IllegalArgumentException("expected named option, found: " + argument);
      }
      String key = argument.substring(2);
      if ("once".equals(key)) {
        result.put(key, "true");
      } else {
        if (++index == arguments.length) {
          throw new IllegalArgumentException("missing value for --" + key);
        }
        result.put(key, arguments[index]);
      }
    }
    return result;
  }

  private static String required(Map<String, String> options, String name) {
    String value = options.get(name);
    if (value == null || value.isBlank()) {
      throw new IllegalArgumentException("missing --" + name);
    }
    return value;
  }

  private static Path requiredPath(Map<String, String> options, String name) {
    return Path.of(required(options, name));
  }

  private static InetSocketAddress address(String value) {
    int separator = value.lastIndexOf(':');
    if (separator < 1 || separator == value.length() - 1) {
      throw new IllegalArgumentException("address must be host:port: " + value);
    }
    return new InetSocketAddress(value.substring(0, separator), Integer.parseInt(value.substring(separator + 1)));
  }

  private static void usage() {
    System.out.println("""
        pipestream-netty serve --cert FILE --key FILE --output-dir DIR [--bind HOST:PORT] [--ready-file FILE] [--once]
        pipestream-netty send --connect HOST:PORT --ca FILE --entity-id UINT32 --input FILE [--server-name NAME] [--content-type MIME] [--parent-id UINT32]
        """);
  }
}
