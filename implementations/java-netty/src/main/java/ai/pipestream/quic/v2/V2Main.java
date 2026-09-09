package ai.pipestream.quic.v2;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Clock;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

/**
 * Separate V2 entry point. It is not the legacy {@link ai.pipestream.quic.Main} and shares no
 * command with it. Authority commands explicitly initialize or reopen roots; nothing is created
 * implicitly. {@code serve} prints {@code READY host:port} only after recovery and binding; on
 * SIGTERM/SIGINT it drains local owners and prints {@code DRAINED}, which is not a claim that all
 * durable work completed.
 */
public final class V2Main {
  private V2Main() {}

  /**
   * Run one V2 command.
   *
   * @param arguments command and named options
   */
  public static void main(String[] arguments) {
    try {
      run(arguments);
    } catch (Exception failure) {
      // A driver reads stdout: an authority refusal is named there with its code and the peer's
      // bounded diagnostic; local failures only reach stderr. Every failure exits 1.
      if (failure instanceof ProtocolError error && error.fromAuthority())
        System.out.println("REFUSED code=" + error.code() + " detail=" + error.detail());
      System.err.println(
          failure instanceof ProtocolError error
              ? error.code() + ": " + error.getMessage()
              : failure.getMessage() == null ? failure.toString() : failure.getMessage());
      System.exit(1);
    }
  }

  /**
   * Run one command.
   *
   * @param arguments full argument vector
   * @throws Exception command failure
   */
  static void run(String[] arguments) throws Exception {
    if (arguments.length == 0 || "--help".equals(arguments[0])) {
      usage();
      return;
    }
    String command = arguments[0];
    Map<String, String> options = options(arguments, 1);
    switch (command) {
      case "init-authority" -> initAuthority(options);
      case "serve" -> serve(options, null);
      default -> {
        if (!ClientCommands.run(command, arguments)) {
          usage();
          throw new IllegalArgumentException("unknown V2 command: " + command);
        }
      }
    }
  }

  /**
   * Host configuration from options.
   *
   * @param options parsed options
   * @return configuration
   */
  static DurableHost.Configuration configuration(Map<String, String> options) {
    DurableHost.Configuration defaults =
        DurableHost.Configuration.defaults(
            required(options, "authority"), required(options, "result-authority"));
    return defaults;
  }

  /**
   * Initialize a new authority root.
   *
   * @param options parsed options
   * @throws Exception initialization failure
   */
  static void initAuthority(Map<String, String> options) throws Exception {
    Path root = requiredPath(options, "root");
    DurableHost host =
        DurableHost.initialize(
            root,
            configuration(options),
            ReferenceApplications.all(),
            DurableHost.OwnerPolicy.fromPrincipals(Map::of, false),
            DurableHost.UtcClock.system(options.containsKey("trust-system-clock")));
    host.close();
    System.out.println("INITIALIZED " + root.toAbsolutePath());
  }

  /**
   * Run the durable listener until interrupted.
   *
   * @param options named options
   * @param boundaries test-only observer, or null for production
   * @throws Exception configuration, recovery or bind failure
   */
  static void serve(Map<String, String> options, Boundaries boundaries) throws Exception {
    if (!options.containsKey("trust-system-clock"))
      throw new IllegalArgumentException(
          "serve requires --trust-system-clock: an explicit operator assertion that system UTC is"
              + " trustworthy across restart");
    Path root = requiredPath(options, "root");
    Map<Records.Digest, String> principals =
        PrincipalMap.read(requiredPath(options, "principal-map"));
    TlsAuthentication authentication =
        TlsAuthentication.server(
            requiredPath(options, "client-ca"),
            requiredPath(options, "cert"),
            requiredPath(options, "key"),
            principals,
            Clock.systemUTC());
    DurableOptions durable = DurableOptions.defaults();
    if (options.containsKey("object-limit")) {
      long limit = Long.parseUnsignedLong(options.get("object-limit"));
      durable =
          new DurableOptions(
              durable.core(),
              durable.dataStreams(),
              durable.maxDataStreams(),
              durable.dataSendBytes(),
              durable.streamWindowBytes(),
              durable.chunkBytes(),
              limit,
              durable.headerTimeoutMs(),
              durable.requireDurable(),
              durable.shutdownTimeoutMs());
    }
    Path ready = options.containsKey("ready-file") ? Path.of(options.get("ready-file")) : null;
    if (ready != null && Files.exists(ready))
      throw new IOException("ready file already exists: " + ready);
    DurableHost host =
        DurableHost.open(
            root,
            configuration(options),
            ReferenceApplications.all(),
            DurableHost.OwnerPolicy.fromPrincipals(
                () -> principals, options.containsKey("allow-skip")),
            DurableHost.UtcClock.system(true));
    if (boundaries != null) host.boundaries(boundaries);
    DurableServer server;
    try {
      server =
          boundaries == null
              ? DurableServer.start(
                  address(options.getOrDefault("bind", "127.0.0.1:0")),
                  authentication,
                  host,
                  durable)
              : DurableServer.start(
                  address(options.getOrDefault("bind", "127.0.0.1:0")),
                  authentication,
                  host,
                  durable,
                  boundaries);
    } catch (Exception | Error failure) {
      host.close();
      throw failure;
    }
    String bound = server.address().getHostString() + ":" + server.address().getPort();
    if (ready != null) {
      Files.writeString(
          ready,
          bound + System.lineSeparator(),
          StandardOpenOption.CREATE_NEW,
          StandardOpenOption.WRITE);
      try {
        Files.setPosixFilePermissions(ready, PosixFilePermissions.fromString("rw-------"));
      } catch (UnsupportedOperationException ignored) {
        // Non-POSIX filesystems keep their default permissions.
      }
    }
    System.out.println("READY " + bound);
    System.out.flush();
    CountDownLatch stopped = new CountDownLatch(1);
    java.util.concurrent.atomic.AtomicBoolean draining =
        new java.util.concurrent.atomic.AtomicBoolean();
    Runnable drain =
        () -> {
          if (!draining.compareAndSet(false, true)) return;
          int status;
          try {
            server.close();
            host.close();
            System.out.println("DRAINED");
            status = 0;
          } catch (IOException incomplete) {
            System.out.println("STOPPED " + incomplete.getMessage());
            status = 2;
          } finally {
            System.out.flush();
            stopped.countDown();
          }
          Runtime.getRuntime().halt(status);
        };
    // SIGTERM/SIGINT request drain; exit zero means local owners drained, not work completion.
    boolean handled = true;
    for (String name : new String[] {"TERM", "INT"}) {
      try {
        onSignal(name, () -> new Thread(drain, "pipestream-v2-shutdown").start());
      } catch (ReflectiveOperationException | RuntimeException unsupported) {
        handled = false;
      }
    }
    if (!handled) {
      // Without signal handling the JVM exits 143 after the hook; DRAINED still reports drain.
      Runtime.getRuntime().addShutdownHook(new Thread(drain, "pipestream-v2-shutdown"));
    }
    stopped.await();
  }

  private static void onSignal(String name, Runnable action) throws ReflectiveOperationException {
    // Reflection avoids a compile-time dependency on the internal signal API.
    Class<?> signal = Class.forName("sun.misc.Signal");
    Class<?> handler = Class.forName("sun.misc.SignalHandler");
    Object instance = signal.getConstructor(String.class).newInstance(name);
    Object proxy =
        java.lang.reflect.Proxy.newProxyInstance(
            handler.getClassLoader(),
            new Class<?>[] {handler},
            (p, method, arguments) -> {
              if ("handle".equals(method.getName())) {
                action.run();
                return null;
              }
              if ("toString".equals(method.getName())) return "pipestream-v2-signal";
              if ("hashCode".equals(method.getName())) return System.identityHashCode(p);
              if ("equals".equals(method.getName())) return p == arguments[0];
              return null;
            });
    signal.getMethod("handle", signal, handler).invoke(null, instance, proxy);
  }

  /**
   * Parse {@code --name value} options.
   *
   * @param arguments argument vector
   * @param from first index to scan
   * @return options in order
   */
  static Map<String, String> options(String[] arguments, int from) {
    Map<String, String> options = new LinkedHashMap<>();
    for (int i = from; i < arguments.length; i++) {
      String argument = arguments[i];
      if (!argument.startsWith("--")) continue;
      String name = argument.substring(2);
      if (i + 1 < arguments.length && !arguments[i + 1].startsWith("--")) {
        options.put(name, arguments[++i]);
      } else options.put(name, "");
    }
    return options;
  }

  /**
   * Required option value.
   *
   * @param options parsed options
   * @param name option name
   * @return value
   */
  static String required(Map<String, String> options, String name) {
    String value = options.get(name);
    if (value == null || value.isEmpty()) throw new IllegalArgumentException("missing --" + name);
    return value;
  }

  /**
   * Required path option.
   *
   * @param options parsed options
   * @param name option name
   * @return path
   */
  static Path requiredPath(Map<String, String> options, String name) {
    return Path.of(required(options, name));
  }

  /**
   * Parse {@code host:port}.
   *
   * @param value text
   * @return address
   */
  static InetSocketAddress address(String value) {
    int colon = value.lastIndexOf(':');
    if (colon <= 0) throw new IllegalArgumentException("expected host:port, got " + value);
    return new InetSocketAddress(
        value.substring(0, colon), Integer.parseInt(value.substring(colon + 1)));
  }

  /** Print usage. */
  static void usage() {
    List<String> lines = new ArrayList<>();
    lines.add("PipeStream V2 durable endpoints (Java/Netty)");
    lines.add("  init-authority --root DIR --authority LABEL --result-authority HOST:PORT");
    lines.add("  serve --root DIR --authority LABEL --result-authority HOST:PORT --bind HOST:PORT");
    lines.add(
        "        --cert PEM --key PEM --client-ca PEM --principal-map TSV --trust-system-clock");
    lines.add("        [--allow-skip] [--ready-file PATH] [--object-limit BYTES]");
    lines.addAll(ClientCommands.usage());
    lines.add("Legacy version-1 commands remain in ai.pipestream.quic.Main.");
    for (String line : lines) System.out.println(line);
  }
}
