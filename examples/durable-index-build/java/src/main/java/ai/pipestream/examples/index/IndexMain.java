package ai.pipestream.examples.index;

import ai.pipestream.quic.v2.DurableHost;
import ai.pipestream.quic.v2.TlsAuthentication;
import ai.pipestream.quic.v2.DurableOptions;
import ai.pipestream.quic.v2.DurableServer;
import ai.pipestream.quic.v2.PrincipalMap;
import ai.pipestream.quic.v2.Records;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Clock;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Example launcher for the index-build authority. Mirrors the shipped
 * launcher's serve flow but registers the example's own application
 * contracts ({@code index-file/v1}, {@code tf/v1}, {@code index-merge/v1})
 * through the public {@link DurableHost} application list instead of the
 * reference list. No test hooks: the shipped launcher's boundaries stay
 * untouched.
 */
public final class IndexMain {
  private IndexMain() {}

  public static void main(String[] args) throws Exception {
    if (args.length == 0) throw new IllegalArgumentException("need a command");
    Map<String, String> options = options(args, 1);
    switch (args[0]) {
      case "init-authority" -> initAuthority(options);
      case "serve" -> serve(options);
      default -> throw new IllegalArgumentException("unknown command: " + args[0]);
    }
  }

  static Map<String, String> options(String[] arguments, int from) {
    Map<String, String> options = new HashMap<>();
    for (int i = from; i < arguments.length; i++) {
      String flag = arguments[i];
      if (!flag.startsWith("--")) throw new IllegalArgumentException("expected --flag: " + flag);
      String name = flag.substring(2);
      if (i + 1 < arguments.length && !arguments[i + 1].startsWith("--")) {
        options.put(name, arguments[++i]);
      } else {
        options.put(name, "true");
      }
    }
    return options;
  }

  static String required(Map<String, String> options, String name) {
    String value = options.get(name);
    if (value == null) throw new IllegalArgumentException("missing --" + name);
    return value;
  }

  static Path requiredPath(Map<String, String> options, String name) {
    return Path.of(required(options, name));
  }

  static List<DurableHost.Application> applications(IndexContracts.ReaderConfig reader) {
    return List.of(
        new DurableHost.Application(
            IndexContracts.FILE_LABEL,
            Set.of(2),
            DurableHost.RestartSafety.IDEMPOTENT,
            IndexContracts::runFile,
            IndexContracts::expandFile),
        new DurableHost.Application(
            IndexContracts.TF_LABEL,
            Set.of(0),
            DurableHost.RestartSafety.IDEMPOTENT,
            IndexContracts::runTf,
            null),
        new DurableHost.Application(
            IndexContracts.MERGE_LABEL,
            Set.of(0),
            DurableHost.RestartSafety.IDEMPOTENT,
            IndexContracts.runMerge(reader),
            null));
  }

  static DurableHost.OwnerPolicy policy(Map<Records.Digest, String> principals) {
    return new DurableHost.OwnerPolicy() {
      @Override
      public boolean authorized(String owner) {
        return principals.containsValue(owner);
      }

      @Override
      public boolean skipPermitted(String owner) {
        return false;
      }
    };
  }

  static DurableHost.UtcClock clock() {
    return () -> new DurableHost.UtcClock.Sample(System.currentTimeMillis(), true);
  }

  static void initAuthority(Map<String, String> options) throws Exception {
    Path root = requiredPath(options, "root");
    DurableHost host =
        DurableHost.initialize(
            root,
            DurableHost.Configuration.defaults(
                required(options, "authority"), required(options, "result-authority")),
            applications(null),
            policy(Map.of()),
            clock());
    host.close();
    System.out.println("INITIALIZED " + root.toAbsolutePath());
  }

  static void serve(Map<String, String> options) throws Exception {
    if (!options.containsKey("trust-system-clock"))
      throw new IllegalArgumentException("serve requires --trust-system-clock");
    Path root = requiredPath(options, "root");
    Map<Records.Digest, String> principals = PrincipalMap.read(requiredPath(options, "principal-map"));
    TlsAuthentication authentication =
        TlsAuthentication.server(
            requiredPath(options, "client-ca"),
            requiredPath(options, "cert"),
            requiredPath(options, "key"),
            principals,
            Clock.systemUTC());
    IndexContracts.ReaderConfig reader = readerConfig(options);
    DurableHost host =
        DurableHost.open(
            root,
            DurableHost.Configuration.defaults(
                required(options, "authority"), required(options, "result-authority")),
            applications(reader),
            policy(principals),
            clock());
    DurableServer server;
    try {
      server =
          DurableServer.start(
              address(options.getOrDefault("bind", "127.0.0.1:0")),
              authentication,
              host,
              DurableOptions.defaults());
    } catch (Exception | Error failure) {
      host.close();
      throw failure;
    }
    String bound = server.address().getHostString() + ":" + server.address().getPort();
    if (options.containsKey("ready-file")) {
      Path ready = Path.of(options.get("ready-file"));
      Files.writeString(ready, bound + System.lineSeparator(), StandardOpenOption.CREATE_NEW);
      try {
        Files.setPosixFilePermissions(ready, PosixFilePermissions.fromString("rw-------"));
      } catch (UnsupportedOperationException ignored) {
        // Non-POSIX filesystems keep their default permissions.
      }
    }
    System.out.println("READY " + bound);
    System.out.flush();
    java.util.concurrent.CountDownLatch stopped = new java.util.concurrent.CountDownLatch(1);
    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  try {
                    server.close();
                    host.close();
                    System.out.println("DRAINED");
                  } catch (Exception incomplete) {
                    System.out.println("STOPPED " + incomplete.getMessage());
                  } finally {
                    System.out.flush();
                    stopped.countDown();
                  }
                }));
    stopped.await();
  }

  /** Reader flags are optional: only merge authorities need them. */
  static IndexContracts.ReaderConfig readerConfig(Map<String, String> options) {
    if (!options.containsKey("reader-endpoint")) return null;
    return new IndexContracts.ReaderConfig(
        address(required(options, "reader-endpoint")),
        options.getOrDefault("reader-server-name", "localhost"),
        requiredPath(options, "reader-ca"),
        requiredPath(options, "reader-cert"),
        requiredPath(options, "reader-key"),
        options.getOrDefault("reader-owner", "workload"));
  }

  static InetSocketAddress address(String value) {
    int colon = value.lastIndexOf(':');
    return new InetSocketAddress(
        value.substring(0, colon), Integer.parseInt(value.substring(colon + 1)));
  }
}
