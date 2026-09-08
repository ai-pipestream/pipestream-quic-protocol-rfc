package ai.pipestream.quic.v2;

import ai.pipestream.quic.BoundedSqlite;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * The durable client's own SQLite journal, independent of any authority store. It commits original
 * session intent before the first offer, every mutation's operation identity and immutable
 * parameters before the frame is sent, and validated receipts/observations before the API reports
 * them. An operation without a journaled receipt is unresolved: it is neither success nor
 * permission to invent new work. All methods block and must run off network threads; one exclusive
 * owner per file.
 */
public final class ClientJournal implements AutoCloseable {
  private static final int FORMAT = 1;
  private static final int MESSAGE_LIMIT = 1 << 20;

  /**
   * Immutable original creation intent.
   *
   * @param authority expected issuer label
   * @param owner expected mapped owner
   * @param creationSequence owner creation sequence
   * @param policy exact requested policy
   * @param results whether result delivery is selected with durable work
   */
  public record Intent(
      String authority,
      String owner,
      long creationSequence,
      Records.Policy policy,
      boolean results) {
    /** Validate labels and identifiers. */
    public Intent {
      Checks.identity(authority);
      Checks.identity(owner);
      Checks.id(creationSequence);
      Objects.requireNonNull(policy);
    }

    /**
     * Profiles this intent selects, in wire order.
     *
     * @return supported profile list
     */
    public List<Integer> profiles() {
      return results
          ? List.of(Messages.DURABLE_WORK, Messages.RESULT_DELIVERY)
          : List.of(Messages.DURABLE_WORK);
    }
  }

  /**
   * Bounded journal policy.
   *
   * @param maxOperations retained operations, resolved or not
   * @param maxObservations retained work views, members, manifests and summaries
   * @param maxObservationBytes aggregate encoded observation bytes
   * @param files SQLite file limits
   */
  public record Limits(
      int maxOperations,
      int maxObservations,
      long maxObservationBytes,
      BoundedSqlite.Limits files) {
    /** Validate bounds. */
    public Limits {
      Checks.range(maxOperations, 1, 1_000_000);
      Checks.range(maxObservations, 1, 10_000_000);
      Checks.range(maxObservationBytes, 1, 1L << 40);
      Objects.requireNonNull(files);
    }

    /**
     * Defaults matching the Rust client: 4,096 operations and observations, 64 MiB.
     *
     * @return default limits
     */
    public static Limits defaults() {
      return new Limits(4096, 4096, 64L << 20, BoundedSqlite.Limits.defaults());
    }
  }

  /**
   * A journaled operation whose receipt has not been validated and stored.
   *
   * @param sequence journal order
   * @param operation immutable identity
   * @param mutation original control mutation with request one, or null for an admission
   * @param input original input header, or null for a control mutation
   * @param declaration covering declaration operation for an admission, or null
   */
  public record PendingOperation(
      long sequence,
      Records.OperationId operation,
      Messages.Message mutation,
      Records.InputHeader input,
      Records.OperationId declaration) {
    /** Require exactly one namespace. */
    public PendingOperation {
      Checks.id(sequence);
      Objects.requireNonNull(operation);
      if ((mutation == null) == (input == null))
        throw new IllegalArgumentException("pending operation needs a mutation or an input");
    }
  }

  /**
   * Newest validated work observation.
   *
   * @param revision authority revision
   * @param view consistent view
   */
  public record Observed(long revision, Records.WorkView view) {}

  /**
   * Retained scope evidence.
   *
   * @param scope scope identity
   * @param producer scope producer
   * @param parent parent work or null for root
   * @param declared declared count as last observed
   * @param sealed whether a committed seal was observed
   * @param seal committed seal or null
   * @param membershipVerified whether the complete sealed membership was received and verified
   * @param summary immutable closure summary or null
   */
  public record ScopeEvidence(
      long scope,
      int producer,
      Records.WorkKey parent,
      long declared,
      boolean sealed,
      Records.Digest seal,
      boolean membershipVerified,
      Records.ScopeSummary summary) {}

  /**
   * A saved output selection: the manifest and one index.
   *
   * @param manifest authenticated manifest
   * @param index selected output index
   */
  public record Selection(Records.Manifest manifest, int index) {
    /** Validate the index. */
    public Selection {
      Objects.requireNonNull(manifest);
      Checks.range(index, 0, manifest.outputs().size() - 1L);
    }

    /**
     * The selected output.
     *
     * @return descriptor
     */
    public Records.Output output() {
      return manifest.outputs().get(index);
    }
  }

  private final BoundedSqlite database;
  private final Connection connection;
  private final Limits limits;
  private final Intent intent;
  private Messages.Binding binding;
  private boolean closed;

  private ClientJournal(
      BoundedSqlite database, Connection connection, Limits limits, Intent intent) {
    this.database = database;
    this.connection = connection;
    this.limits = limits;
    this.intent = intent;
  }

  /**
   * Create a new journal and commit the original creation intent before anything is sent.
   *
   * @param file new journal path
   * @param intent immutable creation intent
   * @param limits bounded policy
   * @return open journal
   * @throws IOException existing file or unsafe layout
   * @throws SQLException initialization failure
   */
  public static ClientJournal initialize(Path file, Intent intent, Limits limits)
      throws IOException, SQLException {
    Objects.requireNonNull(intent);
    Objects.requireNonNull(limits);
    Path absolute = file.toAbsolutePath().normalize();
    for (String suffix : new String[] {"", "-wal", "-shm", "-journal", ".psjlimits", ".psjlock"})
      if (Files.exists(
          absolute.resolveSibling(absolute.getFileName() + suffix), LinkOption.NOFOLLOW_LINKS))
        throw new IOException("client journal initialization requires a new file");
    Files.createDirectories(absolute.getParent());
    BoundedSqlite database = BoundedSqlite.open(absolute, limits.files());
    Connection connection = database.connect();
    try {
      try (Statement sql = connection.createStatement()) {
        try (ResultSet mode = sql.executeQuery("PRAGMA journal_mode=DELETE")) {
          if (!mode.next() || !"delete".equalsIgnoreCase(mode.getString(1)))
            throw new SQLException("journal mode not applied");
        }
        sql.execute("BEGIN IMMEDIATE");
        sql.execute(
            "CREATE TABLE ps_v2c_meta (key TEXT PRIMARY KEY NOT NULL, value BLOB NOT NULL)");
        sql.execute(
            "CREATE TABLE ps_v2c_operations (sequence INTEGER PRIMARY KEY AUTOINCREMENT,"
                + " operation BLOB NOT NULL UNIQUE, kind INTEGER NOT NULL, request BLOB NOT NULL,"
                + " declaration BLOB, digest BLOB NOT NULL, receipt BLOB)");
        sql.execute(
            "CREATE TABLE ps_v2c_work (scope INTEGER NOT NULL, producer INTEGER NOT NULL,"
                + " entity INTEGER NOT NULL, revision INTEGER NOT NULL, view BLOB NOT NULL,"
                + " PRIMARY KEY (scope, producer, entity))");
        sql.execute(
            "CREATE TABLE ps_v2c_manifests (scope INTEGER NOT NULL, producer INTEGER NOT NULL,"
                + " entity INTEGER NOT NULL, attempt INTEGER NOT NULL, manifest BLOB NOT NULL,"
                + " PRIMARY KEY (scope, producer, entity, attempt))");
        sql.execute(
            "CREATE TABLE ps_v2c_selections (scope INTEGER NOT NULL, producer INTEGER NOT NULL,"
                + " entity INTEGER NOT NULL, attempt INTEGER NOT NULL, idx INTEGER NOT NULL,"
                + " PRIMARY KEY (scope, producer, entity, attempt, idx))");
        sql.execute(
            "CREATE TABLE ps_v2c_scopes (scope INTEGER PRIMARY KEY NOT NULL, producer INTEGER NOT"
                + " NULL, parent BLOB, declared INTEGER NOT NULL, sealed INTEGER NOT NULL, seal"
                + " BLOB, verified INTEGER NOT NULL, summary BLOB)");
        sql.execute(
            "CREATE TABLE ps_v2c_members (scope INTEGER NOT NULL, entity INTEGER NOT NULL,"
                + " PRIMARY KEY (scope, entity))");
        try (PreparedStatement insert =
            connection.prepareStatement("INSERT INTO ps_v2c_meta (key, value) VALUES (?, ?)")) {
          insert.setString(1, "format");
          insert.setBytes(2, new byte[] {FORMAT});
          insert.executeUpdate();
          insert.setString(1, "intent");
          insert.setBytes(2, encodeIntent(intent));
          insert.executeUpdate();
          insert.setString(1, "limits");
          insert.setBytes(2, encodeLimits(limits));
          insert.executeUpdate();
        }
        sql.execute("COMMIT");
      }
      return new ClientJournal(database, connection, limits, intent);
    } catch (SQLException | RuntimeException | Error failure) {
      try {
        connection.close();
      } catch (SQLException close) {
        failure.addSuppressed(close);
      }
      throw failure;
    }
  }

  /**
   * Reopen an existing journal. The retained intent and limits must match exactly; foreign or newer
   * formats are refused without conversion.
   *
   * @param file existing journal
   * @param limits exact retained policy
   * @return open journal
   * @throws IOException missing file or changed file policy
   * @throws SQLException unsupported format, corruption or storage failure
   */
  public static ClientJournal open(Path file, Limits limits) throws IOException, SQLException {
    Objects.requireNonNull(limits);
    if (!Files.isRegularFile(file, LinkOption.NOFOLLOW_LINKS) || Files.size(file) == 0)
      throw new IOException("client journal recovery requires an existing journal");
    BoundedSqlite database = BoundedSqlite.open(file.toAbsolutePath().normalize(), limits.files());
    Connection connection = database.connect();
    try {
      Intent intent;
      Messages.Binding binding = null;
      try (Statement sql = connection.createStatement()) {
        try (ResultSet mode = sql.executeQuery("PRAGMA journal_mode")) {
          if (!mode.next() || !"delete".equalsIgnoreCase(mode.getString(1)))
            throw new SQLException("foreign client journal mode");
        }
        try (ResultSet tables =
            sql.executeQuery(
                "SELECT count(*) FROM sqlite_master WHERE type='table' AND name LIKE 'ps_v2c_%'")) {
          if (!tables.next() || tables.getLong(1) != 7)
            throw new SQLException("foreign or incomplete client journal schema");
        }
      }
      byte[] format = meta(connection, "format");
      if (format == null || format.length != 1 || format[0] != FORMAT)
        throw new SQLException("unsupported client journal format");
      byte[] retainedLimits = meta(connection, "limits");
      if (retainedLimits == null || !java.util.Arrays.equals(retainedLimits, encodeLimits(limits)))
        throw new SQLException("client journal limits differ from retained policy");
      byte[] retainedIntent = meta(connection, "intent");
      if (retainedIntent == null) throw new SQLException("client journal lacks creation intent");
      intent = decodeIntent(retainedIntent);
      byte[] retainedBinding = meta(connection, "binding");
      if (retainedBinding != null) {
        binding =
            (Messages.Binding) ((Wire.Known) Wire.decode(retainedBinding, MESSAGE_LIMIT)).message();
        checkBinding(intent, binding);
      }
      ClientJournal journal = new ClientJournal(database, connection, limits, intent);
      journal.binding = binding;
      journal.audit();
      return journal;
    } catch (SQLException | RuntimeException | Error failure) {
      try {
        connection.close();
      } catch (SQLException close) {
        failure.addSuppressed(close);
      }
      throw failure;
    }
  }

  private static byte[] meta(Connection connection, String key) throws SQLException {
    try (PreparedStatement select =
        connection.prepareStatement("SELECT value FROM ps_v2c_meta WHERE key=?")) {
      select.setString(1, key);
      try (ResultSet row = select.executeQuery()) {
        return row.next() ? row.getBytes(1) : null;
      }
    }
  }

  private static byte[] encodeIntent(Intent intent) {
    Cbor.Writer out = new Cbor.Writer(1024);
    out.array(5);
    out.text(intent.authority(), 128);
    out.text(intent.owner(), 128);
    out.number(intent.creationSequence());
    RecordCodec.write(out, intent.policy());
    out.bool(intent.results());
    return out.finish();
  }

  private static Intent decodeIntent(byte[] bytes) {
    Cbor.Reader in = new Cbor.Reader(bytes, 1024);
    in.exact(5);
    Intent intent =
        new Intent(in.text(128), in.text(128), in.number(), RecordCodec.policy(in), in.bool());
    in.end();
    return intent;
  }

  private static byte[] encodeLimits(Limits limits) {
    Cbor.Writer out = new Cbor.Writer(256);
    out.array(7);
    out.number(limits.maxOperations());
    out.number(limits.maxObservations());
    out.number(limits.maxObservationBytes());
    out.number(limits.files().databaseBytes());
    out.number(limits.files().walBytes());
    out.number(limits.files().journalBytes());
    out.number(limits.files().sharedMemoryBytes());
    return out.finish();
  }

  private static void checkBinding(Intent intent, Messages.Binding binding) {
    if (!binding.authority().equals(intent.authority())
        || !binding.owner().equals(intent.owner())
        || binding.creationSequence() != intent.creationSequence()
        || !binding.policy().equals(intent.policy()))
      throw new ProtocolError(
          ProtocolError.Code.INTEGRITY_ERROR, "binding contradicts journaled creation intent");
  }

  private void audit() throws SQLException {
    // Every retained receipt must still carry the digest journaled for its intent.
    try (Statement sql = connection.createStatement();
        ResultSet rows =
            sql.executeQuery(
                "SELECT operation, digest, receipt FROM ps_v2c_operations WHERE receipt IS NOT"
                    + " NULL")) {
      while (rows.next()) {
        Records.OperationReceipt receipt =
            (Records.OperationReceipt)
                Wire.decodeRecord(
                    Wire.RecordKind.OPERATION_RECEIPT, rows.getBytes(3), MESSAGE_LIMIT);
        if (!java.util.Arrays.equals(receipt.operation().bytes(), rows.getBytes(1))
            || !java.util.Arrays.equals(receipt.requestDigest().bytes(), rows.getBytes(2)))
          throw new SQLException("client journal receipt contradicts its intent");
      }
    }
  }

  private synchronized void live() {
    if (closed) throw new IllegalStateException("client journal closed");
  }

  /**
   * The immutable creation intent.
   *
   * @return intent
   */
  public Intent intent() {
    return intent;
  }

  /**
   * The journaled session binding, if creation or attachment has been validated.
   *
   * @return binding
   */
  public synchronized Optional<Messages.Binding> binding() {
    return Optional.ofNullable(binding);
  }

  /**
   * Commit the validated session binding. A later contradictory binding is INTEGRITY_ERROR.
   *
   * @param committed exact validated creation/attachment response
   * @throws SQLException storage failure
   */
  public synchronized void bind(Messages.Binding committed) throws SQLException {
    live();
    Objects.requireNonNull(committed);
    checkBinding(intent, committed);
    if (binding != null) {
      if (binding.generation() != committed.generation()
          || !binding.limits().equals(committed.limits()))
        throw new ProtocolError(
            ProtocolError.Code.INTEGRITY_ERROR, "attachment contradicts journaled binding");
      return;
    }
    Messages.Binding normalized =
        new Messages.Binding(
            1,
            committed.authority(),
            committed.owner(),
            committed.generation(),
            committed.creationSequence(),
            committed.policy(),
            committed.limits());
    transaction(
        () -> {
          try (PreparedStatement insert =
              connection.prepareStatement(
                  "INSERT INTO ps_v2c_meta (key, value) VALUES ('binding', ?)")) {
            insert.setBytes(1, Wire.encode(normalized, MESSAGE_LIMIT));
            insert.executeUpdate();
          }
        });
    binding = normalized;
  }

  private Commitments.Context context() {
    if (binding == null)
      throw new ProtocolError(ProtocolError.Code.NOT_READY, "session not yet bound");
    return new Commitments.Context(binding.authority(), binding.owner(), binding.generation());
  }

  /**
   * Journal a control mutation's immutable identity and parameters before sending it. Replaying the
   * same identity with the same parameters is idempotent; different parameters are CONFLICT.
   *
   * @param mutation declaration, scope cancellation, retry, cancellation or skip (request ignored)
   * @return journal sequence
   * @throws SQLException storage failure
   */
  public synchronized long journalMutation(Messages.Message mutation) throws SQLException {
    live();
    Records.OperationId id = operationOf(mutation);
    Messages.Message normalized = withRequest(mutation, 1);
    Records.Digest digest = Commitments.operation(context(), 0, normalized);
    return journal(id, kind(normalized), Wire.encode(normalized, MESSAGE_LIMIT), null, digest);
  }

  /**
   * Journal an input admission's immutable header before opening its stream.
   *
   * @param header immutable input header
   * @param declaration covering declaration operation
   * @return journal sequence
   * @throws SQLException storage failure
   */
  public synchronized long journalInput(Records.InputHeader header, Records.OperationId declaration)
      throws SQLException {
    live();
    Objects.requireNonNull(declaration);
    if (header.generation() != context().generation())
      throw new ProtocolError(ProtocolError.Code.CONFLICT, "input names another session");
    Records.Digest digest = Commitments.operation(context(), 0, header);
    return journal(
        header.operation(),
        0,
        Wire.encodeRecord(header, Wire.HEADER_LIMIT),
        declaration.bytes(),
        digest);
  }

  private long journal(
      Records.OperationId id, int kind, byte[] request, byte[] declaration, Records.Digest digest)
      throws SQLException {
    try (PreparedStatement select =
        connection.prepareStatement(
            "SELECT sequence, kind, request, digest FROM ps_v2c_operations WHERE operation=?")) {
      select.setBytes(1, id.bytes());
      try (ResultSet row = select.executeQuery()) {
        if (row.next()) {
          if (row.getInt(2) != kind
              || !java.util.Arrays.equals(row.getBytes(3), request)
              || !java.util.Arrays.equals(row.getBytes(4), digest.bytes()))
            throw new ProtocolError(
                ProtocolError.Code.CONFLICT, "operation identity reused with different parameters");
          return row.getLong(1);
        }
      }
    }
    long[] sequence = new long[1];
    transaction(
        () -> {
          try (Statement count = connection.createStatement();
              ResultSet rows = count.executeQuery("SELECT count(*) FROM ps_v2c_operations")) {
            if (rows.next() && rows.getLong(1) >= limits.maxOperations())
              throw ProtocolError.limit("client journal operation capacity exhausted");
          }
          try (PreparedStatement insert =
              connection.prepareStatement(
                  "INSERT INTO ps_v2c_operations (operation, kind, request, declaration, digest)"
                      + " VALUES (?, ?, ?, ?, ?)",
                  Statement.RETURN_GENERATED_KEYS)) {
            insert.setBytes(1, id.bytes());
            insert.setInt(2, kind);
            insert.setBytes(3, request);
            insert.setBytes(4, declaration);
            insert.setBytes(5, digest.bytes());
            insert.executeUpdate();
            try (ResultSet keys = insert.getGeneratedKeys()) {
              if (!keys.next()) throw new SQLException("journal sequence unavailable");
              sequence[0] = keys.getLong(1);
            }
          }
        });
    return sequence[0];
  }

  /**
   * Look up a journaled operation.
   *
   * @param id operation identity
   * @return pending operation, whether or not resolved
   * @throws SQLException storage failure
   */
  public synchronized Optional<PendingOperation> operation(Records.OperationId id)
      throws SQLException {
    live();
    try (PreparedStatement select =
        connection.prepareStatement(
            "SELECT sequence, kind, request, declaration FROM ps_v2c_operations WHERE"
                + " operation=?")) {
      select.setBytes(1, id.bytes());
      try (ResultSet row = select.executeQuery()) {
        if (!row.next()) return Optional.empty();
        return Optional.of(
            pending(row.getLong(1), id, row.getInt(2), row.getBytes(3), row.getBytes(4)));
      }
    }
  }

  private static PendingOperation pending(
      long sequence, Records.OperationId id, int kind, byte[] request, byte[] declaration) {
    if (kind == 0)
      return new PendingOperation(
          sequence,
          id,
          null,
          (Records.InputHeader)
              Wire.decodeRecord(Wire.RecordKind.INPUT_HEADER, request, Wire.HEADER_LIMIT),
          new Records.OperationId(declaration));
    return new PendingOperation(
        sequence, id, ((Wire.Known) Wire.decode(request, MESSAGE_LIMIT)).message(), null, null);
  }

  /**
   * Commit a receipt the client has already validated against the journaled intent, identity and
   * typed outcome. The journal independently requires the digest to equal the journaled digest.
   *
   * @param receipt validated receipt
   * @throws SQLException storage failure
   */
  public synchronized void journalReceipt(Records.OperationReceipt receipt) throws SQLException {
    live();
    Objects.requireNonNull(receipt);
    byte[] encoded = Wire.encodeRecord(receipt, MESSAGE_LIMIT);
    transaction(
        () -> {
          try (PreparedStatement select =
              connection.prepareStatement(
                  "SELECT digest, receipt FROM ps_v2c_operations WHERE operation=?")) {
            select.setBytes(1, receipt.operation().bytes());
            try (ResultSet row = select.executeQuery()) {
              if (!row.next())
                throw new ProtocolError(
                    ProtocolError.Code.CONFLICT, "receipt for an unjournaled operation");
              if (!java.util.Arrays.equals(row.getBytes(1), receipt.requestDigest().bytes()))
                throw new ProtocolError(
                    ProtocolError.Code.INTEGRITY_ERROR,
                    "receipt digest contradicts journaled intent");
              byte[] existing = row.getBytes(2);
              if (existing != null && !java.util.Arrays.equals(existing, encoded))
                throw new ProtocolError(
                    ProtocolError.Code.INTEGRITY_ERROR, "receipt contradicts the retained receipt");
              if (existing != null) return;
            }
          }
          try (PreparedStatement update =
              connection.prepareStatement(
                  "UPDATE ps_v2c_operations SET receipt=? WHERE operation=?")) {
            update.setBytes(1, encoded);
            update.setBytes(2, receipt.operation().bytes());
            update.executeUpdate();
          }
        });
  }

  /**
   * The retained receipt for an operation.
   *
   * @param id operation identity
   * @return receipt when resolved
   * @throws SQLException storage failure
   */
  public synchronized Optional<Records.OperationReceipt> receipt(Records.OperationId id)
      throws SQLException {
    live();
    try (PreparedStatement select =
        connection.prepareStatement("SELECT receipt FROM ps_v2c_operations WHERE operation=?")) {
      select.setBytes(1, id.bytes());
      try (ResultSet row = select.executeQuery()) {
        if (!row.next() || row.getBytes(1) == null) return Optional.empty();
        return Optional.of(
            (Records.OperationReceipt)
                Wire.decodeRecord(
                    Wire.RecordKind.OPERATION_RECEIPT, row.getBytes(1), MESSAGE_LIMIT));
      }
    }
  }

  /**
   * Journaled operations without a validated receipt, in journal order.
   *
   * @param after exclusive lower sequence bound
   * @param limit maximum entries, 1..256
   * @return unresolved operations
   * @throws SQLException storage failure
   */
  public synchronized List<PendingOperation> unresolved(long after, int limit) throws SQLException {
    live();
    Checks.range(limit, 1, 256);
    List<PendingOperation> result = new ArrayList<>();
    try (PreparedStatement select =
        connection.prepareStatement(
            "SELECT sequence, operation, kind, request, declaration FROM ps_v2c_operations"
                + " WHERE receipt IS NULL AND sequence > ? ORDER BY sequence LIMIT ?")) {
      select.setLong(1, after);
      select.setInt(2, limit);
      try (ResultSet rows = select.executeQuery()) {
        while (rows.next())
          result.add(
              pending(
                  rows.getLong(1),
                  new Records.OperationId(rows.getBytes(2)),
                  rows.getInt(3),
                  rows.getBytes(4),
                  rows.getBytes(5)));
      }
    }
    return result;
  }

  /**
   * Commit a validated work observation. Revisions must not decrease and immutable fields already
   * known must agree; a contradiction is INTEGRITY_ERROR and the prior evidence is kept.
   *
   * @param revision authority revision
   * @param view validated view
   * @throws SQLException storage failure
   */
  public synchronized void observeWork(long revision, Records.WorkView view) throws SQLException {
    live();
    Checks.id(revision);
    Objects.requireNonNull(view);
    Optional<Observed> prior = observedWork(view.work());
    if (prior.isPresent()) {
      if (revision < prior.get().revision())
        throw new ProtocolError(ProtocolError.Code.INTEGRITY_ERROR, "work revision regressed");
      ClientValidation.consistent(prior.get().view(), view);
      if (revision == prior.get().revision() && prior.get().view().equals(view)) return;
    }
    byte[] encoded = Wire.encodeRecord(view, MESSAGE_LIMIT);
    transaction(
        () -> {
          if (prior.isEmpty()) reserveObservation(encoded.length);
          try (PreparedStatement upsert =
              connection.prepareStatement(
                  "INSERT INTO ps_v2c_work (scope, producer, entity, revision, view) VALUES"
                      + " (?,?,?,?,?) ON CONFLICT(scope, producer, entity) DO UPDATE SET"
                      + " revision=excluded.revision, view=excluded.view")) {
            upsert.setLong(1, view.work().scope());
            upsert.setInt(2, view.work().producer());
            upsert.setLong(3, view.work().entity());
            upsert.setLong(4, revision);
            upsert.setBytes(5, encoded);
            upsert.executeUpdate();
          }
        });
  }

  /**
   * Newest validated observation for one work identity.
   *
   * @param work logical work
   * @return observation when retained
   * @throws SQLException storage failure
   */
  public synchronized Optional<Observed> observedWork(Records.WorkKey work) throws SQLException {
    live();
    try (PreparedStatement select =
        connection.prepareStatement(
            "SELECT revision, view FROM ps_v2c_work WHERE scope=? AND producer=? AND entity=?")) {
      select.setLong(1, work.scope());
      select.setInt(2, work.producer());
      select.setLong(3, work.entity());
      try (ResultSet row = select.executeQuery()) {
        if (!row.next()) return Optional.empty();
        return Optional.of(
            new Observed(
                row.getLong(1),
                (Records.WorkView)
                    Wire.decodeRecord(Wire.RecordKind.WORK_VIEW, row.getBytes(2), MESSAGE_LIMIT)));
      }
    }
  }

  /**
   * Commit a validated manifest. A different manifest for the same work/attempt is INTEGRITY_ERROR.
   *
   * @param manifest validated manifest
   * @throws SQLException storage failure
   */
  public synchronized void observeManifest(Records.Manifest manifest) throws SQLException {
    live();
    Objects.requireNonNull(manifest);
    Optional<Records.Manifest> prior = manifest(manifest.work(), manifest.attempt());
    if (prior.isPresent()) {
      if (!prior.get().equals(manifest))
        throw new ProtocolError(
            ProtocolError.Code.INTEGRITY_ERROR, "manifest contradicts retained manifest");
      return;
    }
    byte[] encoded = Wire.encodeRecord(manifest, MESSAGE_LIMIT);
    transaction(
        () -> {
          reserveObservation(encoded.length);
          try (PreparedStatement insert =
              connection.prepareStatement(
                  "INSERT INTO ps_v2c_manifests (scope, producer, entity, attempt, manifest) VALUES"
                      + " (?,?,?,?,?)")) {
            insert.setLong(1, manifest.work().scope());
            insert.setInt(2, manifest.work().producer());
            insert.setLong(3, manifest.work().entity());
            insert.setLong(4, manifest.attempt());
            insert.setBytes(5, encoded);
            insert.executeUpdate();
          }
        });
  }

  /**
   * Retained manifest for a work attempt.
   *
   * @param work logical work
   * @param attempt producing attempt
   * @return manifest when retained
   * @throws SQLException storage failure
   */
  public synchronized Optional<Records.Manifest> manifest(Records.WorkKey work, long attempt)
      throws SQLException {
    live();
    try (PreparedStatement select =
        connection.prepareStatement(
            "SELECT manifest FROM ps_v2c_manifests WHERE scope=? AND producer=? AND entity=? AND"
                + " attempt=?")) {
      select.setLong(1, work.scope());
      select.setInt(2, work.producer());
      select.setLong(3, work.entity());
      select.setLong(4, attempt);
      try (ResultSet row = select.executeQuery()) {
        if (!row.next()) return Optional.empty();
        return Optional.of(
            (Records.Manifest)
                Wire.decodeRecord(Wire.RecordKind.MANIFEST, row.getBytes(1), MESSAGE_LIMIT));
      }
    }
  }

  /**
   * Save an explicit output selection from a retained manifest.
   *
   * @param work logical work
   * @param attempt producing attempt
   * @param index output index
   * @return selection
   * @throws SQLException storage failure
   */
  public synchronized Selection select(Records.WorkKey work, long attempt, int index)
      throws SQLException {
    live();
    Records.Manifest manifest =
        manifest(work, attempt)
            .orElseThrow(
                () -> new ProtocolError(ProtocolError.Code.NOT_FOUND, "no retained manifest"));
    Selection selection = new Selection(manifest, index);
    transaction(
        () -> {
          try (PreparedStatement insert =
              connection.prepareStatement(
                  "INSERT OR IGNORE INTO ps_v2c_selections (scope, producer, entity, attempt, idx)"
                      + " VALUES (?,?,?,?,?)")) {
            insert.setLong(1, work.scope());
            insert.setInt(2, work.producer());
            insert.setLong(3, work.entity());
            insert.setLong(4, attempt);
            insert.setInt(5, index);
            insert.executeUpdate();
          }
        });
    return selection;
  }

  /**
   * A saved selection.
   *
   * @param work logical work
   * @param attempt producing attempt
   * @param index output index
   * @return selection when saved
   * @throws SQLException storage failure
   */
  public synchronized Optional<Selection> selection(Records.WorkKey work, long attempt, int index)
      throws SQLException {
    live();
    try (PreparedStatement select =
        connection.prepareStatement(
            "SELECT 1 FROM ps_v2c_selections WHERE scope=? AND producer=? AND entity=? AND"
                + " attempt=? AND idx=?")) {
      select.setLong(1, work.scope());
      select.setInt(2, work.producer());
      select.setLong(3, work.entity());
      select.setLong(4, attempt);
      select.setInt(5, index);
      try (ResultSet row = select.executeQuery()) {
        if (!row.next()) return Optional.empty();
      }
    }
    Optional<Records.Manifest> manifest = manifest(work, attempt);
    return manifest.map(m -> new Selection(m, index));
  }

  /**
   * Commit a validated membership page. Producer and parent are immutable once known; members are
   * only ever added; a committed seal is immutable; membership is marked verified only when every
   * declared member has been observed and the recomputed seal equals the committed seal.
   *
   * @param page validated page
   * @throws SQLException storage failure
   */
  public synchronized void observePage(Messages.PageResponse page) throws SQLException {
    live();
    Objects.requireNonNull(page);
    Optional<ScopeEvidence> prior = scope(page.scope());
    if (prior.isPresent()) ClientValidation.consistent(prior.get(), page);
    transaction(
        () -> {
          if (prior.isEmpty()) {
            reserveObservation(64);
            try (PreparedStatement insert =
                connection.prepareStatement(
                    "INSERT INTO ps_v2c_scopes (scope, producer, parent, declared, sealed, seal,"
                        + " verified) VALUES (?,?,?,?,?,?,0)")) {
              insert.setLong(1, page.scope());
              insert.setInt(2, page.producer());
              insert.setBytes(
                  3, page.parent() == null ? null : Wire.encodeRecord(page.parent(), 256));
              insert.setLong(4, page.declared());
              insert.setInt(5, page.sealed() ? 1 : 0);
              insert.setBytes(6, page.seal() == null ? null : page.seal().bytes());
              insert.executeUpdate();
            }
          } else {
            try (PreparedStatement update =
                connection.prepareStatement(
                    "UPDATE ps_v2c_scopes SET declared=?, sealed=?, seal=COALESCE(seal, ?) WHERE"
                        + " scope=?")) {
              update.setLong(1, page.declared());
              update.setInt(2, page.sealed() || prior.get().sealed() ? 1 : 0);
              update.setBytes(3, page.seal() == null ? null : page.seal().bytes());
              update.setLong(4, page.scope());
              update.executeUpdate();
            }
          }
          try (PreparedStatement insert =
              connection.prepareStatement(
                  "INSERT OR IGNORE INTO ps_v2c_members (scope, entity) VALUES (?, ?)")) {
            for (Messages.Entry entry : page.entries()) {
              reserveObservation(16);
              insert.setLong(1, page.scope());
              insert.setLong(2, entry.entity());
              insert.executeUpdate();
            }
          }
          if (page.sealed() && page.seal() != null) verifyMembership(page);
        });
  }

  private void verifyMembership(Messages.PageResponse page) throws SQLException {
    long count;
    try (PreparedStatement select =
        connection.prepareStatement("SELECT count(*) FROM ps_v2c_members WHERE scope=?")) {
      select.setLong(1, page.scope());
      try (ResultSet row = select.executeQuery()) {
        count = row.next() ? row.getLong(1) : 0;
      }
    }
    if (count != page.declared()) return;
    Commitments.Seal seal =
        new Commitments.Seal(
            context(), page.scope(), page.producer(), page.parent(), page.declared());
    try (PreparedStatement select =
        connection.prepareStatement(
            "SELECT entity FROM ps_v2c_members WHERE scope=? ORDER BY entity")) {
      select.setLong(1, page.scope());
      try (ResultSet rows = select.executeQuery()) {
        while (rows.next()) seal.add(rows.getLong(1));
      }
    }
    if (!seal.finish().equals(page.seal()))
      throw new ProtocolError(
          ProtocolError.Code.INTEGRITY_ERROR, "observed membership contradicts seal");
    try (PreparedStatement update =
        connection.prepareStatement("UPDATE ps_v2c_scopes SET verified=1 WHERE scope=?")) {
      update.setLong(1, page.scope());
      update.executeUpdate();
    }
  }

  /**
   * Retained evidence for a scope.
   *
   * @param scope scope identity
   * @return evidence when retained
   * @throws SQLException storage failure
   */
  public synchronized Optional<ScopeEvidence> scope(long scope) throws SQLException {
    live();
    try (PreparedStatement select =
        connection.prepareStatement(
            "SELECT producer, parent, declared, sealed, seal, verified, summary FROM ps_v2c_scopes"
                + " WHERE scope=?")) {
      select.setLong(1, scope);
      try (ResultSet row = select.executeQuery()) {
        if (!row.next()) return Optional.empty();
        byte[] parent = row.getBytes(2);
        byte[] seal = row.getBytes(5);
        byte[] summary = row.getBytes(7);
        return Optional.of(
            new ScopeEvidence(
                scope,
                row.getInt(1),
                parent == null
                    ? null
                    : (Records.WorkKey) Wire.decodeRecord(Wire.RecordKind.WORK_KEY, parent, 256),
                row.getLong(3),
                row.getInt(4) != 0,
                seal == null ? null : new Records.Digest(seal),
                row.getInt(6) != 0,
                summary == null
                    ? null
                    : (Records.ScopeSummary)
                        Wire.decodeRecord(Wire.RecordKind.SCOPE_SUMMARY, summary, MESSAGE_LIMIT)));
      }
    }
  }

  /**
   * Observed members of a scope in increasing order.
   *
   * @param scope scope identity
   * @param after exclusive lower bound
   * @param limit maximum entries, 1..256
   * @return member identities
   * @throws SQLException storage failure
   */
  public synchronized List<Long> members(long scope, long after, int limit) throws SQLException {
    live();
    Checks.range(limit, 1, 256);
    List<Long> result = new ArrayList<>();
    try (PreparedStatement select =
        connection.prepareStatement(
            "SELECT entity FROM ps_v2c_members WHERE scope=? AND entity>? ORDER BY entity LIMIT"
                + " ?")) {
      select.setLong(1, scope);
      select.setLong(2, after);
      select.setInt(3, limit);
      try (ResultSet rows = select.executeQuery()) {
        while (rows.next()) result.add(rows.getLong(1));
      }
    }
    return result;
  }

  /**
   * Commit a validated immutable closure summary for a scope whose seal is already retained.
   *
   * @param summary validated summary
   * @throws SQLException storage failure
   */
  public synchronized void observeSummary(Records.ScopeSummary summary) throws SQLException {
    live();
    Objects.requireNonNull(summary);
    ScopeEvidence evidence =
        scope(summary.scope())
            .orElseThrow(
                () ->
                    new ProtocolError(
                        ProtocolError.Code.CONFLICT, "summary for an unobserved scope"));
    if (evidence.seal() == null || !evidence.seal().equals(summary.seal()))
      throw new ProtocolError(
          ProtocolError.Code.INTEGRITY_ERROR, "summary seal contradicts retained seal");
    if (evidence.producer() != summary.producer()
        || !Objects.equals(evidence.parent(), summary.parent()))
      throw new ProtocolError(
          ProtocolError.Code.INTEGRITY_ERROR, "summary identity contradicts scope");
    if (evidence.summary() != null) {
      if (!evidence.summary().equals(summary))
        throw new ProtocolError(
            ProtocolError.Code.INTEGRITY_ERROR, "summary contradicts retained summary");
      return;
    }
    byte[] encoded = Wire.encodeRecord(summary, MESSAGE_LIMIT);
    transaction(
        () -> {
          reserveObservation(encoded.length);
          try (PreparedStatement update =
              connection.prepareStatement("UPDATE ps_v2c_scopes SET summary=? WHERE scope=?")) {
            update.setBytes(1, encoded);
            update.setLong(2, summary.scope());
            update.executeUpdate();
          }
        });
  }

  /**
   * Record that the authority acknowledged completed-session shutdown for the exact root summary.
   *
   * @param root acknowledged root summary
   * @throws SQLException storage failure
   */
  public synchronized void completed(Records.ScopeSummary root) throws SQLException {
    live();
    Objects.requireNonNull(root);
    transaction(
        () -> {
          try (PreparedStatement insert =
              connection.prepareStatement(
                  "INSERT OR REPLACE INTO ps_v2c_meta (key, value) VALUES ('completed', ?)")) {
            insert.setBytes(1, Wire.encodeRecord(root, MESSAGE_LIMIT));
            insert.executeUpdate();
          }
        });
  }

  /**
   * The acknowledged completed-session root, if any.
   *
   * @return root summary
   * @throws SQLException storage failure
   */
  public synchronized Optional<Records.ScopeSummary> completedRoot() throws SQLException {
    live();
    byte[] bytes = meta(connection, "completed");
    return bytes == null
        ? Optional.empty()
        : Optional.of(
            (Records.ScopeSummary)
                Wire.decodeRecord(Wire.RecordKind.SCOPE_SUMMARY, bytes, MESSAGE_LIMIT));
  }

  private void reserveObservation(long bytes) throws SQLException {
    long count;
    try (Statement sql = connection.createStatement();
        ResultSet rows =
            sql.executeQuery(
                "SELECT (SELECT count(*) FROM ps_v2c_work) + (SELECT count(*) FROM ps_v2c_members)"
                    + " + (SELECT count(*) FROM ps_v2c_manifests) + (SELECT count(*) FROM"
                    + " ps_v2c_scopes)")) {
      count = rows.next() ? rows.getLong(1) : 0;
    }
    if (count >= limits.maxObservations())
      throw ProtocolError.limit("client journal observation capacity exhausted");
    long retained;
    try (Statement sql = connection.createStatement();
        ResultSet rows =
            sql.executeQuery(
                "SELECT COALESCE((SELECT sum(length(view)) FROM ps_v2c_work),0)"
                    + " + COALESCE((SELECT sum(length(manifest)) FROM ps_v2c_manifests),0)"
                    + " + COALESCE((SELECT sum(length(summary)) FROM ps_v2c_scopes),0)")) {
      retained = rows.next() ? rows.getLong(1) : 0;
    }
    if (retained + bytes > limits.maxObservationBytes())
      throw ProtocolError.limit("client journal observation bytes exhausted");
  }

  @FunctionalInterface
  private interface Work {
    void run() throws SQLException;
  }

  private void transaction(Work work) throws SQLException {
    try (Statement sql = connection.createStatement()) {
      sql.execute("BEGIN IMMEDIATE");
      try {
        work.run();
        sql.execute("COMMIT");
      } catch (SQLException | RuntimeException | Error failure) {
        try {
          sql.execute("ROLLBACK");
        } catch (SQLException rollback) {
          failure.addSuppressed(rollback);
        }
        if (failure instanceof SQLException sqlFailure && (sqlFailure.getErrorCode() & 255) == 13) {
          ProtocolError refusal = ProtocolError.limit("client journal file capacity exhausted");
          refusal.initCause(failure);
          throw refusal;
        }
        throw failure;
      }
    }
  }

  static Messages.Message withRequest(Messages.Message message, long request) {
    return switch (message) {
      case Messages.Declare m ->
          new Messages.Declare(request, m.operation(), m.scope(), m.entityIds(), m.seal());
      case Messages.CancelScope m -> new Messages.CancelScope(request, m.operation(), m.scope());
      case Messages.Retry m ->
          new Messages.Retry(request, m.operation(), m.work(), m.expectedAttempt());
      case Messages.Cancel m -> new Messages.Cancel(request, m.operation(), m.work());
      case Messages.Skip m -> new Messages.Skip(request, m.operation(), m.work());
      default -> throw ProtocolError.frame("not a journaled control mutation");
    };
  }

  static Records.OperationId operationOf(Messages.Message message) {
    return switch (message) {
      case Messages.Declare m -> m.operation();
      case Messages.CancelScope m -> m.operation();
      case Messages.Retry m -> m.operation();
      case Messages.Cancel m -> m.operation();
      case Messages.Skip m -> m.operation();
      default -> throw ProtocolError.frame("not a journaled control mutation");
    };
  }

  private static int kind(Messages.Message message) {
    return switch (message) {
      case Messages.Declare m -> 1;
      case Messages.CancelScope m -> 2;
      case Messages.Retry m -> 3;
      case Messages.Cancel m -> 4;
      case Messages.Skip m -> 5;
      default -> throw ProtocolError.frame("not a journaled control mutation");
    };
  }

  /**
   * Close the exclusive connection. Retained evidence stays on disk.
   *
   * @throws SQLException close failure
   */
  @Override
  public synchronized void close() throws SQLException {
    if (closed) return;
    closed = true;
    connection.close();
  }
}
