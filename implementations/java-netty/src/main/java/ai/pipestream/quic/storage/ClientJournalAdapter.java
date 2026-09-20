package ai.pipestream.quic.storage;

import ai.pipestream.quic.v2.ClientJournal;
import ai.pipestream.quic.v2.Records;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Adapts the v2 client's durable SQLite journal ({@link ClientJournal}, format 1)
 * to the storage {@link Journal} contract, proving the SPI against the real
 * client stack. The v2 journal owns the file: its exclusive writer lock, its
 * seven-table schema audit, its format and limits checks, and its proven
 * rollback-journal kill recovery all gate every adapter open, so an SPI
 * replay after a process kill runs only after ClientJournal's own recovery
 * has accepted the file.
 *
 * <p><b>Cursor bridge.</b> The SPI pins a single global cursor in which
 * commits consume cursors in append order; {@code ClientJournal} sequences
 * are intent-operation rowids and a receipt is attached to the intent's own
 * row, so receipt recording consumes no sequence. The adapter therefore
 * keeps the explicit cursor mapping the SPI requires in its own table
 * {@code ps_spi_journal} in the same SQLite file: one row per intent or
 * commit, {@code cursor} an AUTOINCREMENT primary key (monotonic even across
 * deletion, starting at 1), {@code kind} distinguishing intent from commit,
 * and a unique {@code (intent_id, kind)} pair enforcing idempotency. The
 * table name deliberately does not match {@code ps_v2c_%}, so ClientJournal's
 * schema-count audit is unaffected; ClientJournal's behavior, schema, and
 * format version are not changed.
 *
 * <p><b>What is and is not bridged.</b> {@link #appendIntent} and
 * {@link #commit} durably insert mapping rows (identical re-append or
 * re-commit is a no-op success, changed bytes are Corrupt, resolved by
 * re-reading after a constraint race so genuine write failures surface as
 * retryable instead). {@link #replay} selects the mapping table in cursor
 * order and attaches commits to in-window intents. ClientJournal's
 * {@code journalMutation}/{@code journalInput} genuinely cannot carry opaque
 * SPI payloads: they journal typed protocol messages and input headers,
 * require a validated session binding, and bind every row to a session
 * digest; and {@code journalReceipt} requires a protocol-validated
 * {@code OperationReceipt} whose digest matches the journaled operation, so
 * an opaque SPI commit record cannot be recorded as a receipt without
 * fabricating protocol evidence. Those mappings are therefore documented
 * here as unbridgeable rather than faked; the SPI payloads live in the
 * mapping table while ClientJournal provides the file's durability,
 * ownership, and recovery. No contract-suite hook is needed: every pinned
 * journal semantic is expressible over the bridge.
 *
 * <p>Thread safety: operations serialize on the adapter, and the unique
 * indexes protect the file across processes; the underlying ClientJournal
 * permits one writer per file, which a second adapter open inherits as a
 * general failure, mirroring the other backends' single-writer convention.
 * Blocking calls belong off transport event loops.
 */
public final class ClientJournalAdapter implements Journal {
  /** Adapter mapping table in the same SQLite file; not matched by ClientJournal's ps_v2c_% audit. */
  public static final String MAPPING_TABLE = "ps_spi_journal";
  private static final String KIND_INTENT = "intent";
  private static final String KIND_COMMIT = "commit";
  private static final int KIND_INTENT_VALUE = 0;
  private static final int KIND_COMMIT_VALUE = 1;
  private static final int SQLITE_BUSY = 5;
  private static final int SQLITE_LOCKED = 6;
  /** Same-JVM open registry: one adapter per file, mirroring the other backends' single-writer convention. */
  private static final java.util.Set<String> OPEN_FILES = java.util.concurrent.ConcurrentHashMap.newKeySet();

  private static String acquire(Path file) throws StorageException {
    String identity = file.toAbsolutePath().normalize().toString();
    if (!OPEN_FILES.add(identity)) {
      throw StorageException.failed("client journal adapter already has a writer for " + identity, null);
    }
    return identity;
  }

  private static void release(String identity) {
    OPEN_FILES.remove(identity);
  }

  private final ClientJournal client;
  private final Connection connection;
  private final String identity;
  private volatile boolean closed;

  private ClientJournalAdapter(String identity, ClientJournal client, Connection connection) {
    this.identity = identity;
    this.client = client;
    this.connection = connection;
  }

  /**
   * Creates a new SPI journal as a new v2 client journal file with the
   * caller's creation intent, then creates the mapping table. The file must
   * not exist in any of its SQLite sidecar forms.
   *
   * @param file new journal path
   * @param intent immutable v2 creation intent
   * @param limits exact retained v2 policy
   * @return open adapter
   * @throws IOException file exists or the native guard refuses the layout
   * @throws SQLException v2 initialization failure
   * @throws StorageException a second live adapter holds this file
   */
  public static ClientJournalAdapter initialize(Path file, ClientJournal.Intent intent,
      ClientJournal.Limits limits) throws IOException, SQLException, StorageException {
    String identity = acquire(file);
    try {
      ClientJournal client = ClientJournal.initialize(file, intent, limits);
      try {
        return new ClientJournalAdapter(identity, client, openMapping(file));
      } catch (SQLException | RuntimeException failure) {
        try {
          client.close();
        } catch (SQLException cleanup) {
          failure.addSuppressed(cleanup);
        }
        throw failure;
      }
    } catch (IOException | SQLException | RuntimeException failure) {
      release(identity);
      throw failure;
    }
  }

  /**
   * Creates a new SPI journal with the adapter's documented synthetic session
   * intent: authority {@code pipestream-spi}, owner {@code storage-adapter},
   * creation sequence 1, an hour for every retention duration, and no result
   * delivery. The same deterministic intent is used by {@link #open(Path)},
   * so the two factories pair.
   *
   * @param file new journal path
   * @return open adapter
   * @throws IOException file exists or the native guard refuses the layout
   * @throws SQLException v2 initialization failure
   * @throws StorageException a second live adapter holds this file
   */
  public static ClientJournalAdapter initialize(Path file) throws IOException, SQLException, StorageException {
    return initialize(file, syntheticIntent(), ClientJournal.Limits.defaults());
  }

  /**
   * Reopens an existing v2 client journal file through ClientJournal's own
   * recovery (format, limits, and schema audit) and attaches the mapping
   * table, creating it when absent so a plain v2 journal reports as an empty
   * SPI journal.
   *
   * @param file existing journal path
   * @param limits exact retained v2 policy
   * @return open adapter
   * @throws IOException missing file, a second writer, or changed policy
   * @throws SQLException corruption or storage failure
   * @throws StorageException a second live adapter holds this file
   */
  public static ClientJournalAdapter open(Path file, ClientJournal.Limits limits)
      throws IOException, SQLException, StorageException {
    String identity = acquire(file);
    try {
      ClientJournal client = ClientJournal.open(file, limits);
      try {
        return new ClientJournalAdapter(identity, client, openMapping(file));
      } catch (SQLException | RuntimeException failure) {
        try {
          client.close();
        } catch (SQLException cleanup) {
          failure.addSuppressed(cleanup);
        }
        throw failure;
      }
    } catch (IOException | SQLException | RuntimeException failure) {
      release(identity);
      throw failure;
    }
  }

  /**
   * Reopens an existing journal created by {@link #initialize(Path)} with
   * the adapter's synthetic session intent and default limits.
   *
   * @param file existing journal path
   * @return open adapter
   * @throws IOException missing file, a second writer, or changed policy
   * @throws SQLException corruption or storage failure
   * @throws StorageException a second live adapter holds this file
   */
  public static ClientJournalAdapter open(Path file) throws IOException, SQLException, StorageException {
    return open(file, ClientJournal.Limits.defaults());
  }

  /** The underlying v2 client journal, for callers that also need the native API. */
  public ClientJournal clientJournal() {
    return client;
  }

  @Override public synchronized long appendIntent(Intent intent) throws StorageException {
    Objects.requireNonNull(intent, "intent");
    checkOpen();
    return append(KIND_INTENT_VALUE, KIND_INTENT, intent.intentId(), intent.record());
  }

  @Override public synchronized void commit(String intentId, byte[] commitRecord) throws StorageException {
    Objects.requireNonNull(intentId, "intentId");
    Objects.requireNonNull(commitRecord, "commitRecord");
    checkOpen();
    if (commitRecord.length < 1 || commitRecord.length > MAX_RECORD_BYTES) {
      throw new IllegalArgumentException("commit record must be 1 to " + MAX_RECORD_BYTES + " octets");
    }
    if (find(KIND_INTENT, intentId) == null) {
      throw StorageException.notFound("intent " + intentId + " has no durable append");
    }
    append(KIND_COMMIT_VALUE, KIND_COMMIT, intentId, commitRecord);
  }

  @Override public synchronized List<ReplayEntry> replay(long sinceCursor) throws StorageException {
    if (sinceCursor < 0) throw new IllegalArgumentException("replay cursor must be non-negative");
    checkOpen();
    List<ReplayEntry> replayed = new ArrayList<>();
    Map<String, Integer> positionByIntent = new HashMap<>();
    Map<String, byte[]> commitPayloadByIntent = new HashMap<>();
    Map<String, Long> commitCursorByIntent = new HashMap<>();
    try (PreparedStatement select = connection.prepareStatement(
        "SELECT cursor, kind, intent_id, payload FROM " + MAPPING_TABLE + " WHERE cursor > ? ORDER BY cursor ASC")) {
      select.setLong(1, sinceCursor);
      try (ResultSet rows = select.executeQuery()) {
        while (rows.next()) {
          long cursor = rows.getLong(1);
          int kind = rows.getInt(2);
          String intentId = rows.getString(3);
          byte[] payload = rows.getBytes(4);
          if (kind == KIND_INTENT_VALUE) {
            positionByIntent.put(intentId, replayed.size());
            replayed.add(new ReplayEntry(cursor, new Intent(intentId, payload), null));
          } else {
            // The intent sits at or below sinceCursor, outside this replay
            // window; like every other backend, the commit is invisible.
            if (!positionByIntent.containsKey(intentId)) continue;
            commitPayloadByIntent.put(intentId, payload);
            commitCursorByIntent.put(intentId, cursor);
          }
        }
      }
    } catch (SQLException failure) {
      throw mapSqlException(failure, "replay");
    }
    for (var entry : positionByIntent.entrySet()) {
      Long commitCursor = commitCursorByIntent.get(entry.getKey());
      if (commitCursor == null) continue;
      ReplayEntry original = replayed.get(entry.getValue());
      replayed.set(entry.getValue(), new ReplayEntry(original.cursor(), original.intent(),
          commitPayloadByIntent.get(entry.getKey()), commitCursor));
    }
    return List.copyOf(replayed);
  }

  /**
   * Closes the mapping connection and the underlying v2 client journal;
   * idempotent. A failed close still releases what it can.
   *
   * @throws StorageException {@code FAILED} if closing fails
   */
  @Override public synchronized void close() throws StorageException {
    if (closed) return;
    closed = true;
    StorageException cleanup = null;
    try {
      connection.close();
    } catch (SQLException failure) {
      cleanup = StorageException.failed("client journal adapter close failed", failure);
    } finally {
      try {
        client.close();
      } catch (SQLException failure) {
        if (cleanup == null) cleanup = StorageException.failed("client journal close failed", failure);
        else cleanup.addSuppressed(failure);
      } finally {
        release(identity);
      }
    }
    if (cleanup != null) throw cleanup;
  }

  /**
   * Inserts one mapping row, returning its AUTOINCREMENT cursor. An existing
   * identical row is a no-op success (the intent case returns the original
   * cursor); changed bytes are Corrupt. A raced or ambiguous insert failure
   * is resolved by re-reading, so genuine transient write failures surface
   * as retryable rather than as false corruption.
   */
  private long append(int kindValue, String kind, String intentId, byte[] payload) throws StorageException {
    Row existing = find(kind, intentId);
    if (existing != null) {
      if (Arrays.equals(existing.payload(), payload)) return existing.cursor();
      throw StorageException.corrupt(
          (kindValue == KIND_INTENT_VALUE ? "intent " : "commit for intent ") + intentId
              + " already exists with different bytes");
    }
    try (PreparedStatement insert = connection.prepareStatement(
        "INSERT INTO " + MAPPING_TABLE + " (kind, intent_id, payload) VALUES (?, ?, ?)",
        Statement.RETURN_GENERATED_KEYS)) {
      insert.setInt(1, kindValue);
      insert.setString(2, intentId);
      insert.setBytes(3, payload);
      insert.executeUpdate();
      try (ResultSet keys = insert.getGeneratedKeys()) {
        if (!keys.next()) throw StorageException.failed("cursor allocation returned no key", null);
        return keys.getLong(1);
      }
    } catch (SQLException failure) {
      Row raced = find(kind, intentId);
      if (raced != null) {
        if (Arrays.equals(raced.payload(), payload)) return raced.cursor();
        throw StorageException.corrupt(
            (kindValue == KIND_INTENT_VALUE ? "intent " : "commit for intent ") + intentId
                + " already exists with different bytes");
      }
      throw mapSqlException(failure, "append " + kind);
    }
  }

  private Row find(String kind, String intentId) throws StorageException {
    int kindValue = kind.equals(KIND_INTENT) ? KIND_INTENT_VALUE : KIND_COMMIT_VALUE;
    try (PreparedStatement select = connection.prepareStatement(
        "SELECT cursor, payload FROM " + MAPPING_TABLE + " WHERE kind = ? AND intent_id = ?")) {
      select.setInt(1, kindValue);
      select.setString(2, intentId);
      try (ResultSet row = select.executeQuery()) {
        return row.next() ? new Row(row.getLong(1), row.getBytes(2)) : null;
      }
    } catch (SQLException failure) {
      throw mapSqlException(failure, "lookup");
    }
  }

  private record Row(long cursor, byte[] payload) {}

  private static Connection openMapping(Path file) throws SQLException {
    Connection connection =
        DriverManager.getConnection("jdbc:sqlite:" + file.toAbsolutePath().normalize());
    try (Statement statement = connection.createStatement()) {
      statement.execute("PRAGMA busy_timeout=5000");
      statement.execute("CREATE TABLE IF NOT EXISTS " + MAPPING_TABLE + " ("
          + "cursor INTEGER PRIMARY KEY AUTOINCREMENT,"
          + "kind INTEGER NOT NULL,"
          + "intent_id TEXT NOT NULL,"
          + "payload BLOB NOT NULL,"
          + "UNIQUE (intent_id, kind))");
    } catch (SQLException | RuntimeException failure) {
      try {
        connection.close();
      } catch (SQLException cleanup) {
        failure.addSuppressed(cleanup);
      }
      throw failure;
    }
    return connection;
  }

  private static StorageException mapSqlException(SQLException failure, String operation) {
    int code = failure.getErrorCode();
    if (code == SQLITE_BUSY || code == SQLITE_LOCKED) {
      return StorageException.transientFailure(operation + " failed", failure);
    }
    return StorageException.failed(operation + " failed", failure);
  }

  private static ClientJournal.Intent syntheticIntent() {
    return new ClientJournal.Intent(
        "pipestream-spi", "storage-adapter", 1, new Records.Policy(3_600_000, 3_600_000, 3_600_000), false);
  }

  private void checkOpen() throws StorageException {
    if (closed) throw StorageException.failed("journal is closed", null);
  }
}
