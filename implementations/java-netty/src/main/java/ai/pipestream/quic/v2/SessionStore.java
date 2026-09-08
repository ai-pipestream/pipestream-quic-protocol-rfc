package ai.pipestream.quic.v2;

import static ai.pipestream.quic.v2.Messages.*;
import static ai.pipestream.quic.v2.Records.*;

import ai.pipestream.quic.BoundedSqlite;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Semaphore;

/**
 * Blocking V2 authority identity/creation transactions. This is not a durable-profile endpoint.
 * Session membership, admission, execution and retirement extend this same database; neither V1 nor
 * another implementation's database is accepted. Never call it on a transport event loop.
 */
final class SessionStore {
  private static final int VERSION = 2;
  private static final int MAX_BINDING_BYTES = 1024;
  private static final Set<String> TABLES =
      Set.of(
          "ps_v2_meta",
          "ps_v2_owners",
          "ps_v2_sessions",
          "ps_v2_scopes",
          "ps_v2_entities",
          "ps_v2_operations");
  // Bound simultaneous V2 database connections even when callers open multiple store handles.
  private static final Semaphore DATABASE_OPERATIONS = new Semaphore(16);

  /**
   * Immutable deployment policy, checked byte for byte on recovery.
   *
   * @param authority configured issuer, never a client-selected label
   * @param sessionLimits admission ceilings returned by each new session
   * @param maximumPolicy largest policy accepted exactly, without clamping
   * @param maxOwners bound on permanently retained owner high-water records
   * @param maxSessions bound on retained sessions, including retirement in progress
   * @param maxSessionsPerOwner bound on one owner's retained sessions
   * @param files separate hard SQLite file-length limits
   */
  record Configuration(
      String authority,
      Limits sessionLimits,
      Policy maximumPolicy,
      int maxOwners,
      int maxSessions,
      int maxSessionsPerOwner,
      BoundedSqlite.Limits files) {
    /** Check local configuration bounds and required values. */
    Configuration {
      Checks.identity(authority);
      Objects.requireNonNull(sessionLimits);
      Objects.requireNonNull(maximumPolicy);
      Objects.requireNonNull(files);
      Checks.range(maxOwners, 1, 65536);
      Checks.range(maxSessions, 1, 65536);
      Checks.range(maxSessionsPerOwner, 1, maxSessions);
    }

    /**
     * Encode the exact local storage policy; this is not a wire message.
     *
     * @return bounded deterministic CBOR policy image
     */
    byte[] encode() {
      Cbor.Writer out = new Cbor.Writer(1024);
      out.array(10);
      out.text(authority, 128);
      RecordCodec.write(out, sessionLimits);
      RecordCodec.write(out, maximumPolicy);
      out.number(maxOwners);
      out.number(maxSessions);
      out.number(maxSessionsPerOwner);
      out.number(files.databaseBytes());
      out.number(files.walBytes());
      out.number(files.journalBytes());
      out.number(files.sharedMemoryBytes());
      return out.finish();
    }
  }

  /**
   * A local, nonblocking current-credential/policy gate, never deserialized from a peer.
   *
   * @param owner original verified principal; the callback must reject remapping to another owner
   * @param checkCurrent credential and owner-policy check, throwing UNAUTHORIZED on denial
   */
  record Access(String owner, Runnable checkCurrent) {
    /** Validate the immutable local binding. */
    Access {
      Checks.identity(owner);
      Objects.requireNonNull(checkCurrent);
    }

    /** Recheck current authorization; a prior successful invocation is not cached permission. */
    void check() {
      checkCurrent.run();
    }
  }

  private record Retained(
      Binding binding, int profiles, int controlLimit, boolean revoked, boolean retiring) {}

  private final BoundedSqlite database;
  private final Configuration config;
  private final byte[] configBytes;

  private SessionStore(BoundedSqlite database, Configuration config) {
    this.database = database;
    this.config = config;
    configBytes = config.encode();
  }

  /**
   * Perform explicit first installation. Existing files or policy history are never overwritten.
   * Interrupted initialization fails closed on recovery instead of reissuing an empty authority.
   *
   * @param path new database path
   * @param config immutable deployment configuration
   * @return initialized stateless transaction handle
   * @throws IOException for existing history or unsafe filesystem state
   * @throws SQLException if durable initialization fails
   */
  static SessionStore initialize(Path path, Configuration config) throws IOException, SQLException {
    Objects.requireNonNull(config);
    Path absolute = path.toAbsolutePath().normalize();
    Path parent = absolute.getParent();
    if (parent == null) throw new IOException("V2 database requires a parent directory");
    Path ancestor = parent;
    while (!Files.exists(ancestor)) ancestor = ancestor.getParent();
    ancestor = ancestor.toRealPath();
    Files.createDirectories(parent);
    absolute = parent.toRealPath().resolve(absolute.getFileName());
    for (String suffix : new String[] {"", "-wal", "-shm", "-journal", ".psjlimits", ".psjlock"}) {
      if (Files.exists(
          absolute.resolveSibling(absolute.getFileName() + suffix), LinkOption.NOFOLLOW_LINKS))
        throw new IOException("V2 initialization requires a new database and no prior sidecars");
    }
    Files.createFile(absolute);
    try (var file = FileChannel.open(absolute, StandardOpenOption.WRITE)) {
      file.force(true);
    }
    for (Path directory = absolute.getParent(); ; directory = directory.getParent()) {
      try (var file = FileChannel.open(directory, StandardOpenOption.READ)) {
        file.force(true);
      }
      if (directory.equals(ancestor)) break;
      if (directory.getParent() == null)
        throw new IOException("V2 directory escaped existing ancestor");
    }
    return openConfigured(absolute, config, true);
  }

  /**
   * Recover a nonempty existing database; absence is not an empty authority.
   *
   * @param path existing V2 database
   * @param config exact retained deployment configuration
   * @return audited stateless transaction handle
   * @throws IOException for missing files, changed file policy or unsafe layout
   * @throws SQLException for unsupported format, corruption or storage failure
   */
  static SessionStore open(Path path, Configuration config) throws IOException, SQLException {
    if (!Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS) || Files.size(path) == 0)
      throw new IOException("V2 recovery requires an existing initialized database");
    return openConfigured(path, config, false);
  }

  private static SessionStore openConfigured(Path path, Configuration config, boolean initialize)
      throws IOException, SQLException {
    Objects.requireNonNull(config);
    if (!DATABASE_OPERATIONS.tryAcquire())
      throw ProtocolError.limit("V2 database operation capacity");
    try {
      SessionStore store = new SessionStore(BoundedSqlite.open(path, config.files()), config);
      store.bootstrap(initialize);
      return store;
    } finally {
      DATABASE_OPERATIONS.release();
    }
  }

  /**
   * Read an owner's next sequence without reserving storage or creating that owner's history.
   *
   * @param access current verified owner gate
   * @param selected completed capability selection
   * @param request decoded sequence query
   * @return correlated sequence, not a promise that creation capacity is available
   * @throws SQLException for storage failure
   */
  Sequence nextSequence(Access access, Capabilities selected, NextSequence request)
      throws SQLException {
    return transaction(
        access,
        selected,
        false,
        connection ->
            new Sequence(request.request(), increment(ownerHighWater(connection, access.owner()))));
  }

  /**
   * Commit allocators, immutable creation receipt and root scope together, or replay that receipt.
   * The returned binding is evidence of creation, not work admission or execution.
   *
   * @param access current verified owner gate
   * @param selected completed capability selection whose durable combination is retained
   * @param request immutable creation sequence and exact policy
   * @return correlated receipt only after the transaction commits
   * @throws SQLException for storage failure, with no successful receipt returned
   */
  Binding create(Access access, Capabilities selected, Create request) throws SQLException {
    return transaction(
        access,
        selected,
        true,
        connection -> {
          long highWater = ownerHighWater(connection, access.owner());
          if (request.creationSequence() <= highWater) {
            Retained retained;
            try (var query =
                connection.prepareStatement(
                    "SELECT generation FROM ps_v2_sessions WHERE owner=? AND sequence=?")) {
              query.setString(1, access.owner());
              query.setLong(2, request.creationSequence());
              try (var rows = query.executeQuery()) {
                if (!rows.next())
                  throw error(ProtocolError.Code.EXPIRED, "creation receipt retired");
                retained = visible(connection, rows.getLong(1), access.owner());
              }
            }
            compatible(retained, selected);
            if (!retained.binding().policy().equals(request.policy()))
              throw error(ProtocolError.Code.CONFLICT, "creation policy differs");
            return correlate(retained.binding(), request.request());
          }
          if (request.creationSequence() != increment(highWater))
            throw error(ProtocolError.Code.CONFLICT, "creation sequence is not next");
          acceptsPolicy(request.policy());
          if (count(connection, "SELECT count(*) FROM ps_v2_sessions") >= config.maxSessions())
            throw ProtocolError.limit("retained session capacity");
          try (var query =
              connection.prepareStatement("SELECT count(*) FROM ps_v2_sessions WHERE owner=?")) {
            query.setString(1, access.owner());
            try (var row = query.executeQuery()) {
              if (!row.next()) throw corrupt("missing session count");
              if (row.getLong(1) >= config.maxSessionsPerOwner())
                throw ProtocolError.limit("owner session capacity");
            }
          }
          if (highWater == 0
              && count(connection, "SELECT count(*) FROM ps_v2_owners") >= config.maxOwners())
            throw ProtocolError.limit("non-reusable owner history capacity");
          long generation = increment(meta(connection));
          Binding binding =
              new Binding(
                  1,
                  config.authority(),
                  access.owner(),
                  generation,
                  request.creationSequence(),
                  request.policy(),
                  config.sessionLimits());
          byte[] encoded = Wire.encode(binding, Wire.INITIAL_CONTROL_LIMIT);
          if (encoded.length > MAX_BINDING_BYTES)
            throw ProtocolError.limit("creation receipt representation");

          try (var update =
              connection.prepareStatement("UPDATE ps_v2_meta SET high_water=? WHERE singleton=1")) {
            update.setLong(1, generation);
            if (update.executeUpdate() != 1) throw corrupt("missing authority allocator");
          }
          try (var update =
              connection.prepareStatement(
                  """
                  INSERT INTO ps_v2_owners(owner,high_water) VALUES (?,?)
                  ON CONFLICT(owner) DO UPDATE SET high_water=excluded.high_water
                  """)) {
            update.setString(1, access.owner());
            update.setLong(2, request.creationSequence());
            update.executeUpdate();
          }
          int profiles = profiles(selected);
          try (var insert =
              connection.prepareStatement(
                  """
                  INSERT INTO ps_v2_sessions(generation,owner,sequence,receipt,receipt_hash,profiles,
                      control_limit,revoked,retiring) VALUES (?,?,?,?,?,?,?,0,0)
                  """)) {
            insert.setLong(1, generation);
            insert.setString(2, access.owner());
            insert.setLong(3, request.creationSequence());
            insert.setBytes(4, encoded);
            insert.setBytes(5, receiptHash(encoded, profiles, selected.controlLimit()));
            insert.setInt(6, profiles);
            insert.setInt(7, selected.controlLimit());
            insert.executeUpdate();
          }
          try (var insert =
              connection.prepareStatement(
                  """
                  INSERT INTO ps_v2_scopes(generation,id,producer,parent_scope,parent_producer,parent_entity,
                      sealed,seal,declared,last_entity) VALUES (?,0,0,NULL,NULL,NULL,0,NULL,0,0)
                  """)) {
            insert.setLong(1, generation);
            insert.executeUpdate();
          }
          return correlate(binding, request.request());
        });
  }

  /**
   * Read the exact retained binding under current authorization and compatible control limits. The
   * connection owner must separately enforce at most one session binding per connection.
   *
   * @param access current verified owner gate
   * @param selected completed capability selection
   * @param request expected authority, owner and generation
   * @return correlated immutable binding
   * @throws SQLException for missing live metadata, corruption or storage failure
   */
  Binding attach(Access access, Capabilities selected, Attach request) throws SQLException {
    access.check();
    if (!access.owner().equals(request.owner())) throw denied();
    return transaction(
        access,
        selected,
        false,
        connection -> {
          if (!config.authority().equals(request.authority()))
            throw error(ProtocolError.Code.CONFLICT, "authority differs");
          Retained retained = visible(connection, request.generation(), access.owner());
          compatible(retained, selected);
          return correlate(retained.binding(), request.request());
        });
  }

  /**
   * Atomically declare caller-owned membership and its immutable replay receipt.
   *
   * @param access current owner authorization
   * @param selected completed capability selection
   * @param generation retained session generation
   * @param request exact declaration intent, not input admission
   * @return correlated committed receipt
   * @throws SQLException for storage failure
   */
  DeclarationResponse declare(
      Access access, Capabilities selected, long generation, Declare request) throws SQLException {
    return sessionTransaction(
        access,
        selected,
        generation,
        true,
        (connection, binding) -> DeclarationStore.declare(connection, binding, selected, request));
  }

  /**
   * Look up a caller-originated operation without scheduling work or inventing an outcome.
   *
   * @param access current owner authorization
   * @param selected completed capability selection
   * @param generation retained session generation
   * @param request operation identity in producer namespace zero
   * @return correlated retained receipt
   * @throws SQLException for storage failure or corrupt retained evidence
   */
  OperationResponse lookupOperation(
      Access access, Capabilities selected, long generation, LookupOperation request)
      throws SQLException {
    return sessionTransaction(
        access,
        selected,
        generation,
        false,
        (connection, binding) -> DeclarationStore.lookup(connection, binding, request));
  }

  /**
   * Observe a bounded membership page in one read snapshot.
   *
   * @param access current owner authorization
   * @param selected completed capability selection
   * @param generation retained session generation
   * @param request scope, exclusive lower bound and page ceiling
   * @return ordered members and immutable seal when committed
   * @throws SQLException for storage failure or corrupt retained evidence
   */
  PageResponse page(Access access, Capabilities selected, long generation, Page request)
      throws SQLException {
    return sessionTransaction(
        access,
        selected,
        generation,
        false,
        (connection, binding) -> DeclarationStore.page(connection, binding, request));
  }

  /**
   * Read one current work snapshot, without performing the request's optional wait. The connection
   * dispatcher must schedule and bound any wait outside this transaction, using this observation to
   * decide whether the revision changed. This method alone is not a WORK-wait RPC implementation.
   *
   * @param access current owner authorization
   * @param selected completed capability selection
   * @param generation retained session generation
   * @param request work identity, revision comparison and eventual response correlation
   * @return current revision/view, not proof that a requested wait elapsed
   * @throws SQLException for storage failure or corrupt retained evidence
   */
  WatchResponse snapshot(Access access, Capabilities selected, long generation, Watch request)
      throws SQLException {
    return sessionTransaction(
        access,
        selected,
        generation,
        false,
        (connection, binding) -> DeclarationStore.snapshot(connection, binding, request));
  }

  private interface SessionTransaction<T> {
    T run(Connection connection, Binding binding) throws SQLException;
  }

  private <T> T sessionTransaction(
      Access access,
      Capabilities selected,
      long generation,
      boolean write,
      SessionTransaction<T> action)
      throws SQLException {
    return transaction(
        access,
        selected,
        write,
        connection -> {
          Checks.id(generation);
          Retained retained = visible(connection, generation, access.owner());
          compatible(retained, selected);
          return action.run(connection, retained.binding());
        });
  }

  private void bootstrap(boolean initialize) throws SQLException {
    try (Connection connection = database.connect();
        var statement = connection.createStatement()) {
      statement.execute("BEGIN IMMEDIATE");
      try {
        Set<String> present = new TreeSet<>();
        try (var rows =
            statement.executeQuery(
                """
                SELECT type,name FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%'
                """)) {
          while (rows.next()) {
            if (!"table".equals(rows.getString(1))) throw corrupt("unknown schema object");
            present.add(rows.getString(2));
          }
        }
        if (present.isEmpty() && initialize) createSchema(connection);
        else if (!present.equals(TABLES))
          throw corrupt("unknown or incomplete schema; conversion refused");
        meta(connection);
        audit(connection);
        statement.execute("COMMIT");
      } catch (SQLException | RuntimeException failure) {
        rollback(connection, failure);
        throw failure;
      }
      try (var mode = statement.executeQuery("PRAGMA journal_mode=WAL")) {
        if (!mode.next() || !"wal".equalsIgnoreCase(mode.getString(1)))
          throw corrupt("WAL mode required");
      }
    }
  }

  private void createSchema(Connection connection) throws SQLException {
    try (var sql = connection.createStatement()) {
      sql.execute(
          """
          CREATE TABLE ps_v2_meta (
            singleton INTEGER PRIMARY KEY CHECK(singleton=1),
            version INTEGER NOT NULL CHECK(version=2),
            config BLOB NOT NULL CHECK(length(config) BETWEEN 1 AND 1024),
            high_water INTEGER NOT NULL CHECK(high_water>=0)
          ) STRICT
          """);
      sql.execute(
          """
          CREATE TABLE ps_v2_owners (
            owner TEXT PRIMARY KEY CHECK(length(owner) BETWEEN 1 AND 128),
            high_water INTEGER NOT NULL CHECK(high_water>0)
          ) STRICT
          """);
      sql.execute(
          """
          CREATE TABLE ps_v2_sessions (
            generation INTEGER PRIMARY KEY CHECK(generation>0),
            owner TEXT NOT NULL REFERENCES ps_v2_owners(owner),
            sequence INTEGER NOT NULL CHECK(sequence>0),
            receipt BLOB NOT NULL CHECK(length(receipt) BETWEEN 1 AND 1024),
            receipt_hash BLOB NOT NULL CHECK(length(receipt_hash)=32),
            profiles INTEGER NOT NULL CHECK(profiles IN (1,3)),
            control_limit INTEGER NOT NULL CHECK(control_limit BETWEEN 4096 AND 1048576),
            revoked INTEGER NOT NULL CHECK(revoked IN (0,1)),
            retiring INTEGER NOT NULL CHECK(retiring IN (0,1)),
            entity_count INTEGER NOT NULL DEFAULT 0 CHECK(entity_count>=0),
            operation_count INTEGER NOT NULL DEFAULT 0 CHECK(operation_count>=0),
            UNIQUE(owner,sequence)
          ) STRICT
          """);
      sql.execute(
          """
          CREATE TABLE ps_v2_scopes (
            generation INTEGER NOT NULL REFERENCES ps_v2_sessions(generation),
            id INTEGER NOT NULL CHECK(id>=0), producer INTEGER NOT NULL CHECK(producer IN (0,1)),
            parent_scope INTEGER, parent_producer INTEGER, parent_entity INTEGER,
            sealed INTEGER NOT NULL CHECK(sealed IN (0,1)), seal BLOB,
            declared INTEGER NOT NULL CHECK(declared>=0), last_entity INTEGER NOT NULL CHECK(last_entity>=0),
            PRIMARY KEY(generation,id), UNIQUE(generation,id,producer),
            UNIQUE(generation,parent_scope,parent_producer,parent_entity),
            CHECK((id=0 AND producer=0 AND parent_scope IS NULL AND parent_producer IS NULL AND parent_entity IS NULL)
              OR (id>0 AND parent_scope IS NOT NULL AND parent_scope>=0 AND parent_scope<id
                AND parent_producer IS NOT NULL AND parent_producer IN (0,1)
                AND parent_entity IS NOT NULL AND parent_entity>0)),
            CHECK((sealed=0 AND seal IS NULL) OR (sealed=1 AND seal IS NOT NULL AND length(seal)=32))
          ) STRICT
          """);
    }
    DeclarationStore.createSchema(connection);
    try (var insert = connection.prepareStatement("INSERT INTO ps_v2_meta VALUES(1,?,?,0)")) {
      insert.setInt(1, VERSION);
      insert.setBytes(2, configBytes);
      insert.executeUpdate();
    }
  }

  private long meta(Connection connection) throws SQLException {
    try (var statement = connection.createStatement();
        var row =
            statement.executeQuery(
                """
                SELECT version,CASE WHEN length(config)<=1024 THEN config END,high_water
                FROM ps_v2_meta WHERE singleton=1
                """)) {
      if (!row.next() || row.getInt(1) != VERSION || !Arrays.equals(configBytes, row.getBytes(2)))
        throw corrupt("authority, configuration or version differs");
      long highWater = row.getLong(3);
      if (row.wasNull() || highWater < 0 || row.next())
        throw corrupt("invalid authority high-water mark");
      return highWater;
    }
  }

  private void audit(Connection connection) throws SQLException {
    try (var statement = connection.createStatement()) {
      try (var row = statement.executeQuery("PRAGMA quick_check(1)")) {
        if (!row.next() || !"ok".equals(row.getString(1)) || row.next())
          throw corrupt("SQLite integrity check failed");
      }
      try (var row = statement.executeQuery("PRAGMA foreign_key_check")) {
        if (row.next()) throw corrupt("SQLite foreign-key check failed");
      }
      if (count(connection, "SELECT count(*) FROM ps_v2_sessions") > config.maxSessions()
          || count(connection, "SELECT count(*) FROM ps_v2_owners") > config.maxOwners())
        throw corrupt("retained accounting exceeds configuration");
      try (var row =
          statement.executeQuery(
              """
              SELECT 1 FROM ps_v2_sessions s JOIN ps_v2_owners o ON o.owner=s.owner
              WHERE s.generation>(SELECT high_water FROM ps_v2_meta WHERE singleton=1)
                OR s.sequence>o.high_water
                OR (s.retiring=0 AND NOT EXISTS
                  (SELECT 1 FROM ps_v2_scopes r WHERE r.generation=s.generation AND r.id=0)) LIMIT 1
              """)) {
        if (row.next()) throw corrupt("allocator or live root scope is inconsistent");
      }
      try (var query =
          connection.prepareStatement(
              """
              SELECT 1 FROM ps_v2_sessions GROUP BY owner HAVING count(*)>? LIMIT 1
              """)) {
        query.setInt(1, config.maxSessionsPerOwner());
        try (var row = query.executeQuery()) {
          if (row.next()) throw corrupt("owner accounting exceeds configuration");
        }
      }
      try (var rows = statement.executeQuery("SELECT owner,high_water FROM ps_v2_owners")) {
        while (rows.next()) {
          try {
            Checks.identity(rows.getString(1));
          } catch (ProtocolError failure) {
            throw corrupt("invalid retained owner", failure);
          }
          if (rows.getLong(2) <= 0) throw corrupt("invalid owner high-water mark");
        }
      }
      // Stream bounded receipts from SQLite; never materialize all owners/sessions in a map.
      try (var rows =
          statement.executeQuery("SELECT generation FROM ps_v2_sessions ORDER BY generation")) {
        while (rows.next()) {
          Retained session = retained(connection, rows.getLong(1));
          if (!session.retiring()) DeclarationStore.audit(connection, session.binding());
        }
      }
    }
  }

  private long ownerHighWater(Connection connection, String owner) throws SQLException {
    try (var query =
        connection.prepareStatement("SELECT high_water FROM ps_v2_owners WHERE owner=?")) {
      query.setString(1, owner);
      try (var row = query.executeQuery()) {
        if (!row.next()) return 0;
        long value = row.getLong(1);
        if (row.wasNull() || value <= 0) throw corrupt("invalid owner high-water mark");
        return value;
      }
    }
  }

  private Retained retained(Connection connection, long generation) throws SQLException {
    try (var query =
        connection.prepareStatement(
            """
            SELECT owner,sequence,CASE WHEN length(receipt)<=1024 THEN receipt END,
              CASE WHEN length(receipt_hash)=32 THEN receipt_hash END,profiles,control_limit,revoked,retiring
            FROM ps_v2_sessions WHERE generation=?
            """)) {
      query.setLong(1, generation);
      try (var row = query.executeQuery()) {
        if (!row.next()) return null;
        byte[] bytes = row.getBytes(3);
        byte[] hash = row.getBytes(4);
        int profiles = row.getInt(5);
        int control = row.getInt(6);
        if (bytes == null
            || hash == null
            || (profiles != 1 && profiles != 3)
            || control < 4096
            || control > Wire.MAX_CONTROL_LIMIT
            || !MessageDigest.isEqual(hash, receiptHash(bytes, profiles, control)))
          throw corrupt("creation receipt integrity failure");
        Binding binding;
        try {
          Wire.Frame decoded = Wire.decode(bytes, Wire.INITIAL_CONTROL_LIMIT);
          if (!(decoded instanceof Wire.Known known) || !(known.message() instanceof Binding value))
            throw corrupt("invalid creation receipt type");
          binding = value;
        } catch (ProtocolError invalid) {
          throw corrupt("invalid creation receipt encoding", invalid);
        }
        if (binding.request() != 1
            || binding.generation() != generation
            || !binding.authority().equals(config.authority())
            || !binding.owner().equals(row.getString(1))
            || binding.creationSequence() != row.getLong(2)
            || !binding.limits().equals(config.sessionLimits()))
          throw corrupt("creation binding differs");
        return new Retained(binding, profiles, control, row.getInt(7) != 0, row.getInt(8) != 0);
      }
    }
  }

  private Retained visible(Connection connection, long generation, String owner)
      throws SQLException {
    // Do not decode another owner's retained contents, including malformed receipts, before denial.
    try (var query =
        connection.prepareStatement(
            "SELECT owner,revoked,retiring FROM ps_v2_sessions WHERE generation=?")) {
      query.setLong(1, generation);
      try (var row = query.executeQuery()) {
        if (!row.next()) throw error(ProtocolError.Code.NOT_FOUND, "session unavailable");
        if (!owner.equals(row.getString(1)) || row.getInt(2) != 0) throw denied();
        if (row.getInt(3) != 0)
          throw error(ProtocolError.Code.EXPIRED, "session retirement committed");
      }
    }
    try (var query =
        connection.prepareStatement(
            "SELECT producer,parent_scope,parent_producer,parent_entity FROM ps_v2_scopes WHERE"
                + " generation=? AND id=0")) {
      query.setLong(1, generation);
      try (var row = query.executeQuery()) {
        if (!row.next()
            || row.getInt(1) != 0
            || row.getObject(2) != null
            || row.getObject(3) != null
            || row.getObject(4) != null) throw corrupt("live root scope missing or invalid");
      }
    }
    return retained(connection, generation);
  }

  private static void compatible(Retained retained, Capabilities selected) {
    if (retained.profiles() != profiles(selected))
      throw error(
          ProtocolError.Code.EXTENSION_UNSUPPORTED,
          "immutable session profile combination differs");
    if (selected.controlLimit() < retained.controlLimit())
      throw ProtocolError.limit("connection cannot represent retained session responses");
  }

  private void acceptsPolicy(Policy policy) {
    Policy maximum = config.maximumPolicy();
    if (policy.executionLimit() > maximum.executionLimit()
        || policy.outputRetention() > maximum.outputRetention()
        || policy.receiptRetention() > maximum.receiptRetention())
      throw ProtocolError.limit("requested policy exceeds configured support");
  }

  private static Binding correlate(Binding binding, long request) {
    return new Binding(
        request,
        binding.authority(),
        binding.owner(),
        binding.generation(),
        binding.creationSequence(),
        binding.policy(),
        binding.limits());
  }

  private static int profiles(Capabilities selected) {
    ProtocolError.require(selected.response(), "store requires completed capability selection");
    if (!selected.supported().contains(DURABLE_WORK))
      throw error(ProtocolError.Code.EXTENSION_UNSUPPORTED, "session requires durable work");
    ProtocolError.require(
        selected.supported().stream().allMatch(p -> p == DURABLE_WORK || p == RESULT_DELIVERY),
        "unsupported profile in completed selection");
    return selected.supported().contains(RESULT_DELIVERY) ? 3 : 1;
  }

  @FunctionalInterface
  private interface Transaction<T> {
    T run(Connection connection) throws SQLException;
  }

  private <T> T transaction(
      Access access, Capabilities selected, boolean write, Transaction<T> action)
      throws SQLException {
    Objects.requireNonNull(access).check();
    profiles(Objects.requireNonNull(selected));
    if (!DATABASE_OPERATIONS.tryAcquire())
      throw ProtocolError.limit("V2 database operation capacity");
    try (Connection connection = database.connect();
        var statement = connection.createStatement()) {
      if (!write) statement.execute("PRAGMA query_only=ON");
      statement.execute(write ? "BEGIN IMMEDIATE" : "BEGIN");
      try {
        access.check();
        meta(connection);
        T result = action.run(connection);
        access.check();
        statement.execute("COMMIT");
        return result;
      } catch (SQLException | RuntimeException failure) {
        rollback(connection, failure);
        throw failure;
      }
    } catch (SQLException failure) {
      if ((failure.getErrorCode() & 255) == 13) {
        ProtocolError refusal = ProtocolError.limit("SQLite file capacity exhausted");
        refusal.initCause(failure);
        throw refusal;
      }
      throw failure;
    } finally {
      DATABASE_OPERATIONS.release();
    }
  }

  private static long count(Connection connection, String sql) throws SQLException {
    try (var query = connection.createStatement();
        var row = query.executeQuery(sql)) {
      if (!row.next()) throw corrupt("missing accounting row");
      return row.getLong(1);
    }
  }

  private static long increment(long value) {
    if (value == Long.MAX_VALUE) throw ProtocolError.limit("non-reusable identity exhausted");
    return value + 1;
  }

  private static byte[] receiptHash(byte[] receipt, int profiles, int control) {
    try {
      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      Cbor.Writer out = new Cbor.Writer(digest);
      out.array(4);
      out.text("pipestream-java-v2-creation", 128);
      out.bytes(receipt);
      out.number(profiles);
      out.number(control);
      return digest.digest();
    } catch (NoSuchAlgorithmException impossible) {
      throw new AssertionError(impossible);
    }
  }

  private static void rollback(Connection connection, Exception failure) {
    try (var statement = connection.createStatement()) {
      statement.execute("ROLLBACK");
    } catch (SQLException rollback) {
      failure.addSuppressed(rollback);
    }
  }

  private static ProtocolError error(ProtocolError.Code code, String detail) {
    return new ProtocolError(code, detail);
  }

  private static ProtocolError denied() {
    return error(ProtocolError.Code.UNAUTHORIZED, "session access denied");
  }

  private static SQLException corrupt(String detail) {
    return new SQLException("V2 store: " + detail);
  }

  private static SQLException corrupt(String detail, Throwable cause) {
    return new SQLException("V2 store: " + detail, cause);
  }
}
