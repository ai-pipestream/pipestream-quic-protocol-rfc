package ai.pipestream.quic.v2;

import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Client subcommands of {@link V2Main}. Arguments mirror the Rust command guide where the concept
 * is shared: journal arguments, connection arguments, then one operation. Every invocation reopens
 * the journal; original intent is replayed, never reinvented. Output lines are diagnostics for
 * humans and fixtures, not a wire format.
 */
final class ClientCommands {
  private static final long TIMEOUT_SECONDS = 120;

  private ClientCommands() {}

  static List<String> usage() {
    return List.of(
        "  next-sequence <connection>",
        "  init-client --journal FILE --authority LABEL --owner LABEL --creation-sequence N",
        "              [--no-results] [--execution-ms N] [--output-retention-ms N]"
            + " [--receipt-retention-ms N]",
        "  client --journal FILE --authority LABEL --owner LABEL --creation-sequence N <connection>"
            + " <operation>",
        "    connection: --connect HOST:PORT --server-name NAME --ca PEM --cert PEM --key PEM"
            + " [--object-limit BYTES]",
        "    operations: binding | declare --operation HEX [--scope N] --entities A,B,.. [--seal]",
        "      admit --operation HEX --declaration HEX --work S:P:E --input FILE --application"
            + " LABEL [--mode N] [--execution-ms N] [--content-type T] [--output-count N]"
            + " [--output-bytes N]",
        "      replay --operation HEX [--input FILE] | lookup --operation HEX"
            + " | unresolved [--after N] [--limit N]",
        "      watch --work S:P:E [--after REV] [--wait-ms N] | page [--scope N] [--after E]"
            + " [--limit N]",
        "      checkpoint [--scope N] --seal HEX [--wait-ms N] | manifest --work S:P:E --attempt N",
        "      select --work S:P:E --attempt N --index N | read --work S:P:E --attempt N --index N"
            + " --output FILE",
        "      retry --operation HEX --work S:P:E --expected-attempt N | cancel --operation HEX"
            + " --work S:P:E",
        "      skip --operation HEX --work S:P:E | cancel-scope --operation HEX [--scope N]",
        "      complete | detach");
  }

  static boolean run(String command, String[] arguments) throws Exception {
    Map<String, String> options = V2Main.options(arguments, 1);
    switch (command) {
      case "next-sequence" -> nextSequence(options);
      case "init-client" -> initClient(options);
      case "client" -> client(arguments, options);
      default -> {
        return false;
      }
    }
    return true;
  }

  private static <T> T get(CompletionStage<T> stage) throws Exception {
    try {
      return stage.toCompletableFuture().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    } catch (ExecutionException | CompletionException wrapped) {
      Throwable cause = wrapped.getCause();
      if (cause instanceof Exception exception) throw exception;
      throw wrapped;
    }
  }

  private static TlsAuthentication authentication(Map<String, String> options) throws Exception {
    return TlsAuthentication.client(
        V2Main.requiredPath(options, "ca"),
        options.getOrDefault("server-name", "localhost"),
        V2Main.requiredPath(options, "cert"),
        V2Main.requiredPath(options, "key"));
  }

  private static ClientOptions clientOptions(Map<String, String> options) {
    ClientOptions defaults = ClientOptions.defaults();
    if (!options.containsKey("object-limit")) return defaults;
    return new ClientOptions(
        defaults.core(),
        defaults.dataStreams(),
        defaults.maxDataStreams(),
        defaults.dataSendBytes(),
        defaults.streamWindowBytes(),
        defaults.chunkBytes(),
        Long.parseUnsignedLong(options.get("object-limit")),
        defaults.headerTimeoutMs());
  }

  private static InetSocketAddress connect(Map<String, String> options) {
    return V2Main.address(V2Main.required(options, "connect"));
  }

  private static void nextSequence(Map<String, String> options) throws Exception {
    // A sequence query needs the durable profile but no journal: use a throwaway intent whose
    // profile selection is durable-only, and never bind.
    Path scratch = java.nio.file.Files.createTempFile("pipestream-v2-sequence-", ".sqlite");
    java.nio.file.Files.delete(scratch);
    try (ClientJournal journal =
            ClientJournal.initialize(
                scratch,
                new ClientJournal.Intent(
                    "sequence-query",
                    "sequence-query",
                    1,
                    new Records.Policy(1000, 1000, 1000),
                    false),
                ClientJournal.Limits.defaults());
        DurableClient client =
            DurableClient.connect(
                connect(options), authentication(options), journal, clientOptions(options))) {
      get(client.ready());
      System.out.println("NEXT_SEQUENCE " + get(client.nextSequence()));
      get(client.detach());
    } finally {
      for (String suffix : new String[] {"", "-journal", ".psjlimits", ".psjlock"})
        java.nio.file.Files.deleteIfExists(scratch.resolveSibling(scratch.getFileName() + suffix));
    }
  }

  private static ClientJournal.Intent intent(Map<String, String> options) {
    return new ClientJournal.Intent(
        V2Main.required(options, "authority"),
        V2Main.required(options, "owner"),
        Long.parseUnsignedLong(V2Main.required(options, "creation-sequence")),
        new Records.Policy(
            Long.parseUnsignedLong(options.getOrDefault("execution-ms", "60000")),
            Long.parseUnsignedLong(options.getOrDefault("output-retention-ms", "3600000")),
            Long.parseUnsignedLong(options.getOrDefault("receipt-retention-ms", "86400000"))),
        !options.containsKey("no-results"));
  }

  private static void initClient(Map<String, String> options) throws Exception {
    ClientJournal.Intent intent = intent(options);
    try (ClientJournal journal =
        ClientJournal.initialize(
            V2Main.requiredPath(options, "journal"), intent, ClientJournal.Limits.defaults())) {
      System.out.println("INITIALIZED " + journal.intent());
    }
  }

  private static String operationName(String[] arguments) {
    for (int i = 1; i < arguments.length; i++) {
      if (arguments[i].startsWith("--")) {
        if (i + 1 < arguments.length && !arguments[i + 1].startsWith("--")) i++;
        continue;
      }
      return arguments[i];
    }
    throw new IllegalArgumentException("client requires an operation");
  }

  private static Records.OperationId operation(Map<String, String> options, String name) {
    byte[] bytes = HexFormat.of().parseHex(V2Main.required(options, name));
    return new Records.OperationId(bytes);
  }

  private static Records.WorkKey work(String value) {
    String[] parts = value.split(":");
    if (parts.length != 3)
      throw new IllegalArgumentException("work key must be scope:producer:entity");
    return new Records.WorkKey(
        Long.parseUnsignedLong(parts[0]),
        Integer.parseInt(parts[1]),
        Long.parseUnsignedLong(parts[2]));
  }

  private static String hex(byte[] bytes) {
    return HexFormat.of().formatHex(bytes);
  }

  private static void client(String[] arguments, Map<String, String> options) throws Exception {
    String operation = operationName(arguments);
    ClientJournal.Intent intent = intent(options);
    Path journalFile = V2Main.requiredPath(options, "journal");
    try (ClientJournal journal = ClientJournal.open(journalFile, ClientJournal.Limits.defaults())) {
      if (!journal.intent().equals(intent))
        throw new ProtocolError(
            ProtocolError.Code.CONFLICT, "journal intent differs from the supplied arguments");
      try (DurableClient client =
          DurableClient.connect(
              connect(options), authentication(options), journal, clientOptions(options))) {
        get(client.ready());
        Messages.Binding binding = get(client.binding());
        switch (operation) {
          case "binding" -> System.out.println("BINDING " + binding);
          case "declare" -> {
            List<Long> entities = new ArrayList<>();
            for (String entity : V2Main.required(options, "entities").split(","))
              entities.add(Long.parseUnsignedLong(entity.trim()));
            print(
                "RECEIPT",
                get(
                    client.declare(
                        operation(options, "operation"),
                        Long.parseUnsignedLong(options.getOrDefault("scope", "0")),
                        entities,
                        options.containsKey("seal"))));
          }
          case "admit" -> admit(client, options, operation(options, "operation"), null);
          case "replay" -> {
            Records.OperationId id = operation(options, "operation");
            ClientJournal.PendingOperation pending =
                journal
                    .operation(id)
                    .orElseThrow(
                        () ->
                            new ProtocolError(
                                ProtocolError.Code.NOT_FOUND, "operation not journaled"));
            if (pending.input() != null) admit(client, options, id, pending);
            else print("RECEIPT", get(replay(client, pending)));
          }
          case "lookup" -> print("RECEIPT", get(client.lookup(operation(options, "operation"))));
          case "unresolved" -> {
            for (ClientJournal.PendingOperation pending :
                journal.unresolved(
                    Long.parseUnsignedLong(options.getOrDefault("after", "0")),
                    Integer.parseInt(options.getOrDefault("limit", "256"))))
              System.out.println(
                  "UNRESOLVED sequence="
                      + pending.sequence()
                      + " operation="
                      + hex(pending.operation().bytes())
                      + " kind="
                      + (pending.input() != null
                          ? "admit"
                          : pending.mutation().getClass().getSimpleName()));
          }
          case "watch" -> {
            ClientJournal.Observed observed =
                get(
                    client.watch(
                        work(V2Main.required(options, "work")),
                        Long.parseUnsignedLong(options.getOrDefault("after", "0")),
                        Long.parseUnsignedLong(options.getOrDefault("wait-ms", "0"))));
            Records.WorkView view = observed.view();
            System.out.println(
                "WORK revision="
                    + observed.revision()
                    + " state="
                    + view.state().value()
                    + " attempt="
                    + view.attempt()
                    + " child="
                    + (view.child() == null
                        ? "none"
                        : view.child().scope() + ":" + view.child().producer()));
            System.out.println("VIEW " + view);
          }
          case "page" -> {
            DurableClient.ScopePage page =
                get(
                    client.page(
                        Long.parseUnsignedLong(options.getOrDefault("scope", "0")),
                        Long.parseUnsignedLong(options.getOrDefault("after", "0")),
                        Integer.parseInt(options.getOrDefault("limit", "256"))));
            System.out.println(
                "SCOPE scope="
                    + page.scope()
                    + " producer="
                    + page.producer()
                    + " declared="
                    + page.declared()
                    + " membership_verified="
                    + page.membershipVerified()
                    + " seal="
                    + (page.seal() == null ? "none" : hex(page.seal().bytes())));
            System.out.println("MEMBERS " + page.entries() + " more=" + page.more());
          }
          case "checkpoint" -> {
            Records.ScopeSummary summary =
                get(
                    client.checkpoint(
                        Long.parseUnsignedLong(options.getOrDefault("scope", "0")),
                        new Records.Digest(
                            HexFormat.of().parseHex(V2Main.required(options, "seal"))),
                        Long.parseUnsignedLong(options.getOrDefault("wait-ms", "0"))));
            System.out.println(
                "COVERAGE " + summary + " status_root=" + hex(summary.statusRoot().bytes()));
          }
          case "manifest" ->
              System.out.println(
                  "MANIFEST "
                      + get(
                          client.manifest(
                              work(V2Main.required(options, "work")),
                              Long.parseUnsignedLong(V2Main.required(options, "attempt")))));
          case "select" -> {
            Records.WorkKey key = work(V2Main.required(options, "work"));
            long attempt = Long.parseUnsignedLong(V2Main.required(options, "attempt"));
            int index = Integer.parseInt(V2Main.required(options, "index"));
            get(client.manifest(key, attempt));
            System.out.println("REFERENCE " + get(client.select(key, attempt, index)).output());
          }
          case "read" -> {
            ResultFiles.Delivered delivered =
                get(
                    client.read(
                        work(V2Main.required(options, "work")),
                        Long.parseUnsignedLong(V2Main.required(options, "attempt")),
                        Integer.parseInt(V2Main.required(options, "index")),
                        new ResultFiles.Destination(V2Main.requiredPath(options, "output"))));
            System.out.println(
                "VERIFIED length="
                    + delivered.length()
                    + " sha256="
                    + hex(delivered.sha256().bytes()));
          }
          case "retry" ->
              print(
                  "RECEIPT",
                  get(
                      client.retry(
                          operation(options, "operation"),
                          work(V2Main.required(options, "work")),
                          Long.parseUnsignedLong(V2Main.required(options, "expected-attempt")))));
          case "cancel" ->
              print(
                  "RECEIPT",
                  get(
                      client.cancel(
                          operation(options, "operation"),
                          work(V2Main.required(options, "work")))));
          case "skip" ->
              print(
                  "RECEIPT",
                  get(
                      client.skip(
                          operation(options, "operation"),
                          work(V2Main.required(options, "work")))));
          case "cancel-scope" ->
              print(
                  "RECEIPT",
                  get(
                      client.cancelScope(
                          operation(options, "operation"),
                          Long.parseUnsignedLong(options.getOrDefault("scope", "0")))));
          case "complete" -> System.out.println("COMPLETED " + get(client.complete()));
          case "detach" -> {
            get(client.detach());
            System.out.println("DETACHED");
            return;
          }
          default -> throw new IllegalArgumentException("unknown client operation: " + operation);
        }
        get(client.detach());
      }
    }
  }

  private static CompletionStage<Records.OperationReceipt> replay(
      DurableClient client, ClientJournal.PendingOperation pending) {
    return switch (pending.mutation()) {
      case Messages.Declare m -> client.declare(m.operation(), m.scope(), m.entityIds(), m.seal());
      case Messages.CancelScope m -> client.cancelScope(m.operation(), m.scope());
      case Messages.Retry m -> client.retry(m.operation(), m.work(), m.expectedAttempt());
      case Messages.Cancel m -> client.cancel(m.operation(), m.work());
      case Messages.Skip m -> client.skip(m.operation(), m.work());
      default -> throw new IllegalArgumentException("unknown journaled mutation");
    };
  }

  private static void admit(
      DurableClient client,
      Map<String, String> options,
      Records.OperationId operation,
      ClientJournal.PendingOperation pending)
      throws Exception {
    Path input = V2Main.requiredPath(options, "input");
    Records.OperationId declaration =
        pending != null ? pending.declaration() : operation(options, "declaration");
    String contentType =
        pending != null
            ? pending.input().parameters().input().contentType()
            : options.getOrDefault("content-type", "application/octet-stream");
    try (InputSource source = InputSource.file(input, contentType, 16L << 20)) {
      Records.AdmitParameters parameters;
      if (pending != null) {
        parameters = pending.input().parameters();
        if (!parameters.input().equals(source.input()))
          throw new ProtocolError(
              ProtocolError.Code.INTEGRITY_ERROR,
              "file does not match the journaled admission intent");
      } else {
        int count = Integer.parseInt(options.getOrDefault("output-count", "1"));
        long bytes =
            options.containsKey("output-bytes")
                ? Long.parseUnsignedLong(options.get("output-bytes"))
                : count == 0 ? 0 : source.input().length();
        parameters =
            new Records.AdmitParameters(
                work(V2Main.required(options, "work")),
                source.input(),
                V2Main.required(options, "application"),
                Integer.parseInt(options.getOrDefault("mode", "0")),
                Long.parseUnsignedLong(options.getOrDefault("execution-ms", "60000")),
                new Records.OutputBudget(count, bytes));
      }
      print("RECEIPT", get(client.admit(operation, parameters, declaration, source)));
    }
  }

  private static void print(String name, Records.OperationReceipt receipt) {
    System.out.println(
        name
            + " operation="
            + hex(receipt.operation().bytes())
            + " digest="
            + hex(receipt.requestDigest().bytes())
            + " outcome="
            + receipt.outcome());
  }

  static String join(String[] arguments) {
    return String.join(" ", Arrays.asList(arguments));
  }
}
