package ai.pipestream.quic.storage;

import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

/**
 * Cross-language storage layout probe: a test-utility CLI, never part of the
 * shipped API surface. It executes the language-neutral script format v1 from
 * {@code test-vectors/storage-spi/README.md} against one configured backend
 * and prints the canonical report, which must be byte-identical to
 * {@code expected-report-v1.txt} and to the Rust lane's probe on the same
 * backend kind. The class lives in test sources on purpose.
 *
 * <p>Usage: {@code Probe <backend> <script>} with backend one of
 * {@code memory}, {@code fs}, {@code postgres}, {@code s3}, {@code mongo}
 * (object store plus journal where the backend has both roles), or
 * {@code kafka} (journal only; an object-only backend omits the journal
 * section and a journal-only backend omits the scope sections, per the
 * README). Backend configuration comes from the environment; every variable
 * can also be supplied to {@link #execute(String[], Map, PrintStream, PrintStream)}
 * directly, which is how the tests inject configuration without mutating the
 * process environment:
 *
 * <ul>
 *   <li>{@code fs}: {@code PROBE_FS_ROOT} (required) — store root directory
 *   <li>{@code postgres}: {@code PROBE_PG_URL}, {@code PROBE_PG_USER}, {@code PROBE_PG_PASSWORD}
 *   <li>{@code s3}: {@code PROBE_S3_ENDPOINT}, {@code PROBE_S3_BUCKET}, {@code PROBE_S3_ACCESS_KEY},
 *       {@code PROBE_S3_SECRET_KEY} (required); {@code PROBE_S3_REGION} (default {@code us-east-1}),
 *       {@code PROBE_S3_PREFIX} (default {@code probe/}), {@code PROBE_S3_PATH_STYLE} (default {@code true})
 *   <li>{@code mongo}: {@code PROBE_MONGO_URI}, {@code PROBE_MONGO_DATABASE} (required)
 *   <li>{@code kafka}: {@code PROBE_KAFKA_BOOTSTRAP}, {@code PROBE_KAFKA_TOPIC} (required)
 * </ul>
 *
 * <p>Report rendering is exact: LF line endings, a trailing newline, scope
 * sections in ascending rendered-path order with entities in ascending id
 * order, and the journal in ascending cursor order. Nothing
 * platform-specific leaks into the output.
 */
public final class Probe {
  /** Exit code for successful execution. */
  static final int OK = 0;
  /** Exit code for script or storage failures. */
  static final int FAILURE = 1;
  /** Exit code for usage and configuration errors. */
  static final int USAGE = 2;

  private Probe() {}

  /**
   * CLI entry point; exits nonzero on failure.
   * @param args {@code <backend> <script>}
   */
  public static void main(String[] args) {
    int code = execute(args, System.getenv(), System.out, System.err);
    if (code != OK) System.exit(code);
  }

  /**
   * Runs the probe, writing the canonical report to {@code out}.
   *
   * @param args {@code <backend> <script>}
   * @param config backend configuration; normally {@code System.getenv()}
   * @param out report destination
   * @param err diagnostic destination
   * @return {@link #OK}, {@link #FAILURE}, or {@link #USAGE}
   */
  static int execute(String[] args, Map<String, String> config, PrintStream out, PrintStream err) {
    if (args == null || args.length != 2) {
      usage(err);
      return USAGE;
    }
    try {
      Backend backend = Backend.open(args[0], config);
      try (backend) {
        List<String> lines = Files.readAllLines(Path.of(args[1]));
        Report report = runScript(backend, lines);
        out.print(render(backend, report));
      }
      return OK;
    } catch (IllegalArgumentException failure) {
      err.println("probe: " + failure.getMessage());
      usage(err);
      return USAGE;
    } catch (StorageException failure) {
      err.println("probe: storage failure [" + failure.kind() + "]: " + failure.getMessage());
      return FAILURE;
    } catch (Exception failure) {
      err.println("probe: " + failure);
      return FAILURE;
    }
  }

  private static void usage(PrintStream err) {
    err.println("usage: Probe <backend> <script>");
    err.println("backends: memory fs postgres s3 mongo (object store + journal), kafka (journal only)");
    err.println("configuration: PROBE_FS_ROOT | PROBE_PG_URL/USER/PASSWORD | PROBE_S3_ENDPOINT/BUCKET/"
        + "ACCESS_KEY/SECRET_KEY[/REGION/PREFIX/PATH_STYLE] | PROBE_MONGO_URI/DATABASE | "
        + "PROBE_KAFKA_BOOTSTRAP/TOPIC");
  }

  /** One opened backend, holding whichever roles it implements. */
  private static final class Backend implements AutoCloseable {
    final ObjectStore store;
    final Journal journal;

    private Backend(ObjectStore store, Journal journal) {
      this.store = store;
      this.journal = journal;
    }

    static Backend open(String name, Map<String, String> config) throws StorageException {
      return switch (name) {
        case "memory" -> new Backend(MemoryObjectStore.create(), MemoryJournal.create());
        case "fs" -> {
          Path root = Path.of(required(config, "PROBE_FS_ROOT"));
          yield new Backend(FileObjectStore.open(root), FileJournal.open(root));
        }
        case "postgres" -> {
          String url = required(config, "PROBE_PG_URL");
          String user = required(config, "PROBE_PG_USER");
          String password = required(config, "PROBE_PG_PASSWORD");
          yield new Backend(PostgresObjectStore.open(url, user, password), PostgresJournal.open(url, user, password));
        }
        case "s3" -> new Backend(S3ObjectStore.open(
            java.net.URI.create(required(config, "PROBE_S3_ENDPOINT")),
            config.getOrDefault("PROBE_S3_REGION", "us-east-1"),
            required(config, "PROBE_S3_ACCESS_KEY"), required(config, "PROBE_S3_SECRET_KEY"),
            Boolean.parseBoolean(config.getOrDefault("PROBE_S3_PATH_STYLE", "true")),
            required(config, "PROBE_S3_BUCKET"),
            config.getOrDefault("PROBE_S3_PREFIX", "probe/")), null);
        case "mongo" -> {
          String uri = required(config, "PROBE_MONGO_URI");
          String database = required(config, "PROBE_MONGO_DATABASE");
          yield new Backend(MongoObjectStore.open(uri, database), MongoJournal.open(uri, database));
        }
        case "kafka" -> new Backend(null, KafkaJournal.open(
            required(config, "PROBE_KAFKA_BOOTSTRAP"), required(config, "PROBE_KAFKA_TOPIC")));
        default -> throw new IllegalArgumentException("unknown backend: " + name);
      };
    }

    private static String required(Map<String, String> config, String key) {
      String value = config.get(key);
      if (value == null || value.isEmpty()) throw new IllegalArgumentException("missing configuration " + key);
      return value;
    }

    @Override public void close() throws StorageException {
      StorageException failure = null;
      if (store != null) try {
        store.close();
      } catch (StorageException close) {
        failure = close;
      }
      if (journal != null) try {
        journal.close();
      } catch (StorageException close) {
        if (failure == null) failure = close;
        else failure.addSuppressed(close);
      }
      if (failure != null) throw failure;
    }
  }

  /** Script execution outcome: scopes touched, in report order. */
  private record Report(TreeSet<ScopePath> scopes) {}

  private static Report runScript(Backend backend, List<String> lines) throws StorageException {
    TreeSet<ScopePath> touched = new TreeSet<>(Comparator.comparing(ScopePath::render));
    for (int number = 0; number < lines.size(); number++) {
      String line = lines.get(number);
      int comment = line.indexOf('#');
      if (comment >= 0) line = line.substring(0, comment);
      line = line.trim();
      if (line.isEmpty()) continue;
      String[] tokens = line.split("\\s+");
      try {
        apply(backend, touched, tokens);
      } catch (IllegalArgumentException failure) {
        throw new IllegalArgumentException("script line " + (number + 1) + ": " + failure.getMessage());
      }
    }
    return new Report(touched);
  }

  private static void apply(Backend backend, TreeSet<ScopePath> touched, String[] tokens)
      throws StorageException {
    // Backends without a role silently skip that role's operations: an
    // object-only backend omits the journal section and a journal-only
    // backend omits the scope sections, per the README.
    switch (tokens[0]) {
      case "put" -> {
        require(tokens, 4, "put <scope> <entity-id-dec> <payload-hex>");
        if (backend.store == null) return;
        ScopePath scope = ScopePath.parse(tokens[1]);
        long entityId = Long.parseLong(tokens[2]);
        byte[] payload = payload(tokens[3]);
        backend.store.put(scope, entityId, payload, StoreLayout.sha256().digest(payload));
        touched.add(scope);
      }
      case "seal" -> {
        require(tokens, 3, "seal <scope> <entity-count-dec>");
        if (backend.store == null) return;
        ScopePath scope = ScopePath.parse(tokens[1]);
        long count = Long.parseLong(tokens[2]);
        backend.store.sealScope(scope, contentDigest(backend.store, scope), count);
        touched.add(scope);
      }
      case "append" -> {
        require(tokens, 3, "append <intent-id> <payload-hex>");
        if (backend.journal != null) {
          backend.journal.appendIntent(new Journal.Intent(tokens[1], payload(tokens[2])));
        }
      }
      case "commit" -> {
        require(tokens, 3, "commit <intent-id> <payload-hex>");
        if (backend.journal != null) backend.journal.commit(tokens[1], payload(tokens[2]));
      }
      case "retire" -> {
        require(tokens, 3, "retire <scope> <cursor-dec>");
        if (backend.store == null) return;
        ScopePath scope = ScopePath.parse(tokens[1]);
        backend.store.retire(scope, Long.parseLong(tokens[2]));
        touched.add(scope);
      }
      default -> throw new IllegalArgumentException("unknown operation: " + tokens[0]);
    }
  }

  private static void require(String[] tokens, int count, String syntax) {
    if (tokens.length != count) {
      throw new IllegalArgumentException("expected '" + syntax + "' but found " + tokens.length + " fields");
    }
  }

  private static byte[] payload(String hex) {
    if (hex.isEmpty()) throw new IllegalArgumentException("payload hex must be non-empty");
    return HexFormat.of().parseHex(hex);
  }

  /** Canonical content digest of one scope's live entities, per pinned decision 3. */
  private static byte[] contentDigest(ObjectStore store, ScopePath scope) throws StorageException {
    List<StoreLayout.EntityDigest> digests = new ArrayList<>();
    for (long entityId : store.list(scope)) {
      byte[] bytes;
      try {
        bytes = store.openRead(scope, entityId).readAllBytes();
      } catch (java.io.IOException failure) {
        throw StorageException.transientFailure("probe read failed", failure);
      }
      digests.add(new StoreLayout.EntityDigest(entityId, StoreLayout.sha256().digest(bytes)));
    }
    return StoreLayout.scopeDigest(digests);
  }

  private static String render(Backend backend, Report report) throws StorageException {
    StringBuilder out = new StringBuilder();
    HexFormat hex = HexFormat.of();
    if (backend.store != null) {
      for (ScopePath scope : report.scopes()) {
        out.append("scope ").append(scope.render()).append('\n');
        List<StoreLayout.EntityDigest> digests = new ArrayList<>();
        for (long entityId : backend.store.list(scope)) {
          byte[] bytes;
          try {
            bytes = backend.store.openRead(scope, entityId).readAllBytes();
          } catch (java.io.IOException failure) {
            throw StorageException.transientFailure("probe read failed", failure);
          }
          byte[] digest = StoreLayout.sha256().digest(bytes);
          digests.add(new StoreLayout.EntityDigest(entityId, digest));
          out.append("  entity ").append(StoreLayout.entityName(entityId))
              .append(" sha256=").append(hex.formatHex(digest)).append('\n');
        }
        out.append("  content-digest ").append(hex.formatHex(StoreLayout.scopeDigest(digests))).append('\n');
        try {
          ObjectStore.SealRecord seal = backend.store.verifyScope(scope);
          out.append("  sealed digest=").append(hex.formatHex(seal.digest()))
              .append(" count=").append(seal.entityCount()).append('\n');
        } catch (StorageException failure) {
          if (failure.kind() != StorageException.Kind.NOT_FOUND) throw failure;
        }
      }
    }
    if (backend.journal != null) {
      out.append("journal").append('\n');
      // The journal section lists every record flat, ascending cursor:
      // intent lines at their intent cursor, commit lines at the cursor the
      // commit consumed.
      List<Record> records = new ArrayList<>();
      for (Journal.ReplayEntry entry : backend.journal.replay(0)) {
        records.add(new Record(entry.cursor(), "intent", entry.intent().intentId(),
            StoreLayout.sha256().digest(entry.intent().record())));
        if (entry.committed()) {
          records.add(new Record(entry.commitCursor(), "commit", entry.intent().intentId(),
              StoreLayout.sha256().digest(entry.commitRecord())));
        }
      }
      records.sort(Comparator.comparingLong(Record::cursor));
      for (Record record : records) {
        out.append("  ").append(record.cursor()).append(' ').append(record.kind()).append(' ')
            .append(record.intentId()).append(" sha256=").append(hex.formatHex(record.digest()))
            .append('\n');
      }
    }
    return out.toString();
  }

  /** One flat journal-section record, intent or commit. */
  private record Record(long cursor, String kind, String intentId, byte[] digest) {}

  /** Test seam: mutable configuration without touching the process environment. */
  static Map<String, String> config(String... entries) {
    Map<String, String> config = new HashMap<>();
    for (int i = 0; i + 1 < entries.length; i += 2) config.put(entries[i], entries[i + 1]);
    return config;
  }
}
