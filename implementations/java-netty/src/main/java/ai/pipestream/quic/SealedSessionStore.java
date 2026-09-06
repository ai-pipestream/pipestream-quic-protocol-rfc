package ai.pipestream.quic;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;

/**
 * Blocking SQLite storage for sealed declarations, admission, and recursive closure.
 * Calls must run outside Netty event loops. Producer labels do not authorize a caller.
 * This API does not execute application callbacks or promise exactly-once effects.
 */
public final class SealedSessionStore {
  /**
   * Immutable SQLite file-length caps, separate from logical record and payload quotas.
   * All values must be positive multiples of 64 KiB; main/WAL/journal are capped
   * at 16 GiB each and shared memory at 16 MiB. These are not filesystem-block caps.
   * @param databaseBytes main database length cap
   * @param walBytes WAL length cap
   * @param journalBytes rollback journal length cap
   * @param sharedMemoryBytes WAL shared-memory length cap
   */
  public record FileLimits(long databaseBytes, long walBytes, long journalBytes, long sharedMemoryBytes) {
    /** Validates each file bound. */
    public FileLimits {
      long[] values = {databaseBytes, walBytes, journalBytes, sharedMemoryBytes};
      for (int i = 0; i < values.length; i++) {
        if (values[i] <= 0 || values[i] % 65536 != 0 || values[i] > (i == 3 ? 16L << 20 : 16L << 30)) {
          throw new IllegalArgumentException("invalid SQLite file-length limit");
        }
      }
    }
    /**
     * Returns the reference implementation's default file bounds.
     * @return 256 MiB database, 64 MiB WAL/journal, and 512 KiB shared memory
     */
    public static FileLimits defaults() { return new FileLimits(256L << 20, 64L << 20, 64L << 20, 512L << 10); }
  }

  /**
   * Sampled lengths, not a transactional snapshot or allocated filesystem blocks.
   * @param databaseBytes main database bytes
   * @param walBytes WAL bytes
   * @param journalBytes rollback journal bytes
   * @param sharedMemoryBytes shared-memory bytes
   */
  public record FileUsage(long databaseBytes, long walBytes, long journalBytes, long sharedMemoryBytes) {}

  /**
   * Logical durable-job charges, separate from database file lengths.
   * @param retainedJobBytes allocated descriptors and state images outside reserved futures, including retired rows
   * @param rehydrationReservedBytes allocated input and state-image capacity held by reserved futures
   * @param processingJobs queued or running ordinary processing jobs
   * @param rehydrationJobs queued or running rehydration jobs consuming reserved slots
   * @param reservedRehydrationSlots slots held for possible future rehydration
   * @param waitingParents dehydrated parents retaining a future execution slot
   */
  public record JobUsage(long retainedJobBytes, long rehydrationReservedBytes, int processingJobs,
      int rehydrationJobs, int reservedRehydrationSlots, int waitingParents) {}
  /** Parent resolution when its child scope is examined under the Layer 1 STRICT policy. */
  public enum ChildResolution {
    /** The child scope is missing or has not closed. */
    PENDING,
    /** Every child succeeded; the parent durably entered REHYDRATING. */
    REHYDRATING,
    /** At least one child failed; the parent durably entered FAILED. */
    FAILED
  }
  private static final int VERSION = 6;
  private static final long MAX_SESSIONS = 512;
  private static final long MAX_DECLARATIONS = 65_536;
  private static final long MAX_SESSION_DECLARATIONS = 16_384;
  private static final Set<String> TABLES = Set.of("ps_java_meta", "ps_java_sessions",
      "ps_java_scopes", "ps_java_batches", "ps_java_entities", "ps_java_jobs", "ps_java_job_policy", "ps_java_checkpoints", "ps_java_checkpoint_history");
  private final Path database;
  private final SealedSqliteFiles files;

  private SealedSessionStore(SealedSqliteFiles files) { this.files = files; this.database = files.path(); }

  /**
   * Opens or creates this implementation's database without converting another format.
   * @param database database filename, separate from the Rust store
   * @return stateless handle; each operation owns its connection and transaction
   * @throws IOException when its parent directory cannot be created
   * @throws SQLException for unsupported schema, corruption, or SQLite failure
   */
  public static SealedSessionStore open(Path database) throws IOException, SQLException {
    return openConfigured(database, null);
  }

  /**
   * Opens with explicit immutable file limits; existing policies must match.
   * @param database database filename
   * @param limits immutable file-length limits
   * @return bounded store handle
   * @throws IOException for invalid policy or filesystem layout
   * @throws SQLException for database or native guard failure
   */
  public static SealedSessionStore open(Path database, FileLimits limits) throws IOException, SQLException {
    return openConfigured(database, Objects.requireNonNull(limits, "limits"));
  }

  /**
   * Returns the policy retained with this database.
   * @return immutable file-length policy
   */
  public FileLimits fileLimits() { return files.limits(); }

  /**
   * Samples current file lengths and verifies policy/layout.
   * @return file lengths; absent sidecars count as zero
   * @throws IOException for corruption, aliasing, or over-budget files
   */
  public FileUsage fileUsage() throws IOException { return files.usage(); }

  /**
   * Audits retained jobs and reports their completion reservations in one snapshot.
   * @return logical job and reservation charges
   * @throws SQLException for storage failure
   * @throws ProtocolException for corrupt job state or capacity failure
   */
  public JobUsage jobUsage() throws SQLException, ProtocolException { return readTransaction(SealedJobs::audit); }

  private static SealedSessionStore openConfigured(Path database, FileLimits limits) throws IOException, SQLException {
    SealedSessionStore store = new SealedSessionStore(SealedSqliteFiles.open(database, limits));
    try (Connection connection = store.connection(); var statement = connection.createStatement()) {
      statement.execute("BEGIN IMMEDIATE");
      try {
        Set<String> present = new TreeSet<>();
        try (ResultSet rows = statement.executeQuery("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")) {
          while (rows.next()) present.add(rows.getString(1));
        }
        if (present.isEmpty()) {
          statement.execute("CREATE TABLE ps_java_meta (singleton INTEGER PRIMARY KEY CHECK(singleton=1), version INTEGER NOT NULL, sessions INTEGER NOT NULL, declarations INTEGER NOT NULL, per_session INTEGER NOT NULL, binding BLOB NOT NULL CHECK(length(binding)=72)) STRICT");
          try (var insert = connection.prepareStatement("INSERT INTO ps_java_meta VALUES (1,?,?,?,?,?)")) {
            insert.setInt(1, VERSION); insert.setLong(2, MAX_SESSIONS); insert.setLong(3, MAX_DECLARATIONS);
            insert.setLong(4, MAX_SESSION_DECLARATIONS);
            insert.setBytes(5, new SealedStoreBinding(UUID.randomUUID(), SealedStoreBinding.UNBOUND).encode());
            insert.executeUpdate();
          }
          statement.execute("CREATE TABLE ps_java_sessions (id TEXT PRIMARY KEY, producer BLOB NOT NULL CHECK(length(producer)=16), depth INTEGER NOT NULL CHECK(depth BETWEEN 0 AND 7), scope_limit INTEGER NOT NULL CHECK(scope_limit BETWEEN 1 AND 4294967292)) STRICT");
          statement.execute("""
              CREATE TABLE ps_java_scopes (
                session TEXT NOT NULL REFERENCES ps_java_sessions(id),
                id INTEGER NOT NULL CHECK(id BETWEEN 0 AND 4294967295),
                parent_scope INTEGER, parent_id INTEGER,
                depth INTEGER NOT NULL CHECK(depth BETWEEN 0 AND 7),
                next_sequence BLOB NOT NULL CHECK(length(next_sequence)=8),
                sealed INTEGER NOT NULL CHECK(sealed IN (0,1)), digest BLOB,
                closure_image BLOB NOT NULL CHECK(length(closure_image)=128),
                PRIMARY KEY(session,id), UNIQUE(session,parent_scope,parent_id),
                FOREIGN KEY(session,parent_scope,parent_id) REFERENCES ps_java_entities(session,scope,id),
                CHECK((id=0 AND parent_scope IS NULL AND parent_id IS NULL AND depth=0)
                   OR (id>0 AND parent_scope IS NOT NULL AND parent_id IS NOT NULL AND depth>0)),
                CHECK((sealed=0 AND digest IS NULL) OR (sealed=1 AND digest IS NOT NULL AND length(digest)=32))
              ) STRICT
              """);
          statement.execute("CREATE TABLE ps_java_batches (session TEXT NOT NULL, scope INTEGER NOT NULL, sequence BLOB NOT NULL CHECK(length(sequence)=8), request BLOB NOT NULL CHECK(length(request) BETWEEN 1 AND 8192), checksum BLOB NOT NULL CHECK(length(checksum)=32), PRIMARY KEY(session,scope,sequence), FOREIGN KEY(session,scope) REFERENCES ps_java_scopes(session,id)) STRICT");
          statement.execute("""
              CREATE TABLE ps_java_entities (
                session TEXT NOT NULL, scope INTEGER NOT NULL,
                id INTEGER NOT NULL CHECK(id BETWEEN 1 AND 4294967292),
                image BLOB NOT NULL CHECK(length(image)=112),
                PRIMARY KEY(session,scope,id),
                FOREIGN KEY(session,scope) REFERENCES ps_java_scopes(session,id)
              ) STRICT
              """);
          SealedJobs.createSchema(connection);
          statement.execute("CREATE TABLE ps_java_checkpoints (session TEXT NOT NULL, scope INTEGER NOT NULL, sequence BLOB NOT NULL CHECK(length(sequence)=8), request BLOB NOT NULL CHECK(length(request) BETWEEN 1 AND 4096), checksum BLOB NOT NULL CHECK(length(checksum)=32), acknowledged INTEGER NOT NULL CHECK(acknowledged IN (0,1)), PRIMARY KEY(session,scope,sequence), FOREIGN KEY(session,scope) REFERENCES ps_java_scopes(session,id)) STRICT");
          statement.execute("CREATE TABLE ps_java_checkpoint_history (session TEXT PRIMARY KEY REFERENCES ps_java_sessions(id), records INTEGER NOT NULL CHECK(records BETWEEN 0 AND 1024), checksum BLOB NOT NULL CHECK(length(checksum)=32)) STRICT");
        } else if (!present.equals(TABLES)) {
          throw new SQLException("unknown or incomplete Java sealed-work schema; no conversion performed");
        }
        policy(connection);
        statement.execute("COMMIT");
      } catch (SQLException failure) {
        rollback(connection, failure);
        throw failure;
      }
      try (ResultSet mode = statement.executeQuery("PRAGMA journal_mode=WAL")) {
        if (!mode.next() || !"wal".equalsIgnoreCase(mode.getString(1))) throw new SQLException("SQLite WAL mode is required");
      }
    }
    return store;
  }

  /**
   * Declares a batch, validates its seal, and returns only after a durable commit.
   * Identical replay returns the same ACK without adding rows or changing limits.
   * @param request declaration with ACK clear
   * @param maxDepth negotiated scope-depth limit
   * @param maxEntitiesPerScope negotiated per-scope entity limit
   * @return exact acknowledgement
   * @throws ProtocolException for identity, sequence, integrity, or capacity refusal
   * @throws SQLException for persistence failure
   */
  public SealedWork.Declaration declare(SealedWork.Declaration request, int maxDepth,
      long maxEntitiesPerScope) throws ProtocolException, SQLException {
    byte[] encoded = SealedWork.encode(request);
    if ((request.flags() & SealedWork.ACK) != 0) throw Wire.entity("declaration request carries ACK");
    if (maxDepth < 0 || maxDepth > 7 || maxEntitiesPerScope < 1 || maxEntitiesPerScope > Wire.MAX_ENTITY_ID) {
      throw Wire.limit("invalid negotiated recursive limits");
    }
    return transaction(connection -> {
      boolean existing;
      try (PreparedStatement query = connection.prepareStatement("SELECT producer,depth,scope_limit FROM ps_java_sessions WHERE id=?")) {
        query.setString(1, request.sessionId());
        try (ResultSet rows = query.executeQuery()) {
          existing = rows.next();
          if (existing) {
            producer(request.producerId(), rows.getBytes(1));
            if (rows.getInt(2) > maxDepth || rows.getLong(3) > maxEntitiesPerScope) {
              throw new ProtocolException(Wire.ERROR_EXTENSION_UNSUPPORTED, "PIPESTREAM_EXTENSION_UNSUPPORTED", "connection limits cannot attach to retained session");
            }
          }
        }
      }
      if (!existing) {
        if (request.scopeId() != 0 || request.sequence().signum() != 0) throw Wire.entity("first declaration must be root sequence zero");
        if (count(connection, "SELECT count(*) FROM ps_java_sessions", null) >= MAX_SESSIONS) throw Wire.limit("retained session count exhausted");
        try (PreparedStatement insert = connection.prepareStatement("INSERT INTO ps_java_sessions VALUES (?,?,?,?)")) {
          insert.setString(1, request.sessionId()); insert.setBytes(2, SealedWork.producerBytes(request.producerId()));
          insert.setInt(3, maxDepth); insert.setLong(4, maxEntitiesPerScope); insert.executeUpdate();
        }
        try (var insert = connection.prepareStatement("INSERT INTO ps_java_checkpoint_history VALUES (?,0,?)")) {
          insert.setString(1, request.sessionId()); insert.setBytes(2, SealedWork.sha256().digest()); insert.executeUpdate();
        }
      }
      Scope scope = scope(connection, request.sessionId(), request.scopeId());
      if (scope != null) verifyDeclaration(connection, request.sessionId(), request.producerId(), scope);
      try (PreparedStatement query = connection.prepareStatement("SELECT CASE WHEN length(request) BETWEEN 1 AND 8192 THEN request END,checksum FROM ps_java_batches WHERE session=? AND scope=? AND sequence=?")) {
        query.setString(1, request.sessionId()); query.setLong(2, request.scopeId()); query.setBytes(3, sequence(request.sequence()));
        try (ResultSet rows = query.executeQuery()) {
          if (rows.next()) {
            if (scope == null) throw Wire.integrity("retained batch has no scope");
            byte[] retained = rows.getBytes(1);
            if (retained == null) throw Wire.integrity("stored declaration exceeds format bound");
            if (!MessageDigest.isEqual(SealedWork.sha256().digest(retained), rows.getBytes(2))) throw Wire.integrity("stored declaration checksum differs");
            if (!Arrays.equals(encoded, retained)) throw Wire.entity("declaration sequence was reused with changed fields");
            return request.acknowledgement();
          }
        }
      }
      if (scope == null) {
        if (request.sequence().signum() != 0) throw Wire.entity("new scope must start at sequence zero");
        int depth = 0;
        if (request.parent() != null) {
          EntityState parent = entity(connection, request.sessionId(), request.parent());
          if (parent == null || parent.state() == null || parent.state() != 6) throw Wire.entity("parent is not admitted and DEHYDRATING");
          Scope parentScope = scope(connection, request.sessionId(), request.parent().scopeId());
          if (parentScope == null) throw Wire.integrity("parent scope is absent");
          verifyDeclaration(connection, request.sessionId(), request.producerId(), parentScope);
          depth = parentScope.depth() + 1;
          if (depth > sessionDepth(connection, request.sessionId())) throw new ProtocolException(7, "PIPESTREAM_DEPTH_EXCEEDED", "child scope exceeds retained depth limit");
          try (PreparedStatement query = connection.prepareStatement("SELECT id FROM ps_java_scopes WHERE session=? AND parent_scope=? AND parent_id=?")) {
            query.setString(1, request.sessionId()); query.setLong(2, request.parent().scopeId()); query.setLong(3, request.parent().entityId());
            try (ResultSet rows = query.executeQuery()) { if (rows.next()) throw Wire.entity("parent already owns a child scope"); }
          }
        }
        try (PreparedStatement insert = connection.prepareStatement("INSERT INTO ps_java_scopes VALUES (?,?,?,?,?,?,0,NULL,?)")) {
          insert.setString(1, request.sessionId()); insert.setLong(2, request.scopeId());
          insert.setObject(3, request.parent() == null ? null : request.parent().scopeId());
          insert.setObject(4, request.parent() == null ? null : request.parent().entityId());
          insert.setInt(5, depth); insert.setBytes(6, sequence(BigInteger.ZERO));
          insert.setBytes(7, SealedStateImages.closure(request.sessionId(), SealedWork.producerBytes(request.producerId()),
              request.scopeId(), request.parent(), null));
          insert.executeUpdate();
        }
        scope = scope(connection, request.sessionId(), request.scopeId());
        if (scope == null) throw Wire.integrity("new scope disappeared");
      }
      if (!Objects.equals(scope.parent(), request.parent()) || scope.sealed() || !scope.nextSequence().equals(request.sequence())) {
        throw Wire.entity("scope binding, seal, or next sequence differs");
      }
      List<Long> all = identifiers(connection, request.sessionId(), request.scopeId());
      if (!all.isEmpty() && !request.entityIds().isEmpty() && request.entityIds().getFirst() <= all.getLast()) throw Wire.entity("declaration IDs do not increase across batches");
      all.addAll(request.entityIds());
      if (all.isEmpty()) throw Wire.entity("cannot seal an empty work set");
      if (all.size() > sessionEntityLimit(connection, request.sessionId())
          || count(connection, "SELECT count(*) FROM ps_java_entities", null) + request.entityIds().size() > MAX_DECLARATIONS
          || count(connection, "SELECT count(*) FROM ps_java_entities WHERE session=?", request.sessionId()) + request.entityIds().size() > MAX_SESSION_DECLARATIONS) {
        throw Wire.limit("retained declaration budget exhausted");
      }
      if ((request.flags() & SealedWork.SEAL) != 0 && !MessageDigest.isEqual(request.sealDigest(),
          SealedWork.sealDigest(request.sessionId(), request.producerId(), request.scopeId(), request.parent(), all))) {
        throw Wire.integrity("work-set seal does not match the entire declaration");
      }
      BigInteger next = request.sequence().add(BigInteger.ONE);
      if (next.compareTo(SealedCbor.MAX_UINT) > 0) throw Wire.entity("declaration sequence exhausted");
      try (PreparedStatement insert = connection.prepareStatement("INSERT INTO ps_java_entities(session,scope,id,image) VALUES (?,?,?,?)")) {
        for (long id : request.entityIds()) {
          insert.setString(1, request.sessionId()); insert.setLong(2, request.scopeId()); insert.setLong(3, id);
          insert.setBytes(4, SealedStateImages.entity(request.sessionId(), SealedWork.producerBytes(request.producerId()),
              new SealedWork.EntityKey(request.scopeId(), id), new SealedStateImages.Entity(null, false, null, null)));
          insert.addBatch();
        }
        insert.executeBatch();
      }
      try (PreparedStatement update = connection.prepareStatement("UPDATE ps_java_scopes SET next_sequence=?,sealed=?,digest=? WHERE session=? AND id=?")) {
        update.setBytes(1, sequence(next)); update.setInt(2, (request.flags() & SealedWork.SEAL) == 0 ? 0 : 1);
        update.setBytes(3, request.sealDigest()); update.setString(4, request.sessionId()); update.setLong(5, request.scopeId()); update.executeUpdate();
      }
      try (PreparedStatement insert = connection.prepareStatement("INSERT INTO ps_java_batches VALUES (?,?,?,?,?)")) {
        insert.setString(1, request.sessionId()); insert.setLong(2, request.scopeId()); insert.setBytes(3, sequence(request.sequence()));
        insert.setBytes(4, encoded); insert.setBytes(5, SealedWork.sha256().digest(encoded)); insert.executeUpdate();
      }
      return request.acknowledgement();
    });
  }

  /**
   * Admits a fully validated and retained payload under a declared identity.
   * The caller must durably install the payload before invoking this operation.
   * @param sessionId session identity
   * @param producerId retained producer label
   * @param key entity identity
   * @param parent immutable parent binding from the EntityHeader
   * @param payloadDigest verified SHA-256 payload digest
   * @throws ProtocolException for undeclared, repeated, or mismatched admission
   * @throws SQLException for persistence failure
   */
  public void admit(String sessionId, UUID producerId, SealedWork.EntityKey key,
      SealedWork.EntityKey parent, byte[] payloadDigest) throws ProtocolException, SQLException {
    transaction(connection -> {
      admit(connection, sessionId, producerId, key, parent, payloadDigest);
      return null;
    });
  }

  static void admit(Connection connection, String sessionId, UUID producerId, SealedWork.EntityKey key,
      SealedWork.EntityKey parent, byte[] payloadDigest) throws ProtocolException, SQLException {
    admit(connection, sessionId, producerId, key, parent, payloadDigest, false);
  }

  static void admit(Connection connection, String sessionId, UUID producerId, SealedWork.EntityKey key,
      SealedWork.EntityKey parent, byte[] payloadDigest, boolean managed) throws ProtocolException, SQLException {
    if (payloadDigest == null || payloadDigest.length != 32) throw Wire.integrity("payload digest must be SHA-256");
    byte[] digest = payloadDigest.clone();
    owner(connection, sessionId, producerId);
    Scope scope = scope(connection, sessionId, key.scopeId());
    EntityState entity = entity(connection, sessionId, key);
    if (scope == null || !Objects.equals(scope.parent(), parent) || entity == null || entity.state() != null) throw Wire.entity("payload is undeclared, repeated, or has a different parent");
    verifyDeclaration(connection, sessionId, producerId, scope);
    SealedSqliteImages.replace(connection, "ps_java_entities", "image", entity.rowid(),
        SealedStateImages.entity(sessionId, entity.producer(), key, new SealedStateImages.Entity(2, managed, digest, null)));
  }

  Path database() { return database; }

  record ScopeInfo(long id, SealedWork.EntityKey parent, int depth, boolean closed) {}
  record EntityInfo(int state, List<ScopeInfo> ancestry) {}

  EntityInfo describe(String session, UUID producer, SealedWork.EntityKey key) throws SQLException, ProtocolException {
    return readTransaction(connection -> new EntityInfo(status(connection, session, producer, key), ancestry(connection, session, producer, key.scopeId())));
  }

  List<ScopeInfo> ancestry(String session, UUID producer, long scopeId) throws SQLException, ProtocolException {
    return readTransaction(connection -> { owner(connection, session, producer); return ancestry(connection, session, producer, scopeId); });
  }

  private static List<ScopeInfo> ancestry(Connection connection, String session, UUID producer, long scopeId) throws SQLException, ProtocolException {
    List<ScopeInfo> result = new ArrayList<>();
    for (int depth = 0; depth <= 7; depth++) {
      Scope current = scope(connection, session, scopeId);
      if (current == null) throw SealedScope.invalid("scope is absent");
      verifyDeclaration(connection, session, producer, current);
      result.add(new ScopeInfo(current.id(), current.parent(), current.depth(), current.closure() != null));
      if (current.parent() == null) return List.copyOf(result);
      scopeId = current.parent().scopeId();
    }
    throw Wire.integrity("scope ancestry exceeds depth bound");
  }

  void registerCheckpoint(String session, UUID producer, SealedTransport.Checkpoint request) throws SQLException, ProtocolException {
    byte[] encoded = checkpointRequest(request);
    long scopeId = request.scopeId() == null ? 0 : request.scopeId();
    transaction(connection -> {
      owner(connection, session, producer);
      auditCheckpoints(connection, session, producer);
      Scope scope = scope(connection, session, scopeId);
      if (scope == null) throw SealedScope.invalid("checkpoint scope is absent");
      verifyDeclaration(connection, session, producer, scope);
      if (checkpointRecord(connection, session, request, encoded) != null) return null;
      if (count(connection, "SELECT count(*) FROM ps_java_checkpoints", null) >= 4096
          || count(connection, "SELECT count(*) FROM ps_java_checkpoints WHERE session=?", session) >= 1024) throw Wire.limit("retained checkpoint capacity exhausted");
      try (var insert = connection.prepareStatement("INSERT INTO ps_java_checkpoints VALUES (?,?,?,?,?,0)")) {
        insert.setString(1, session); insert.setLong(2, scopeId); insert.setBytes(3, sequence(request.sequence()));
        insert.setBytes(4, encoded); insert.setBytes(5, checkpointChecksum(session, producer, encoded, false)); insert.executeUpdate();
      }
      saveCheckpointHistory(connection, session);
      return null;
    });
  }

  boolean acknowledgeCheckpoint(String session, UUID producer, SealedTransport.Checkpoint request) throws SQLException, ProtocolException {
    byte[] encoded = checkpointRequest(request);
    long scopeId = request.scopeId() == null ? 0 : request.scopeId();
    return transaction(connection -> {
      owner(connection, session, producer);
      auditCheckpoints(connection, session, producer);
      if (checkpointRecord(connection, session, request, encoded) == null) throw Wire.entity("checkpoint was not registered");
      if (!checkpointReady(connection, session, producer, scopeId, request.lastId())) return false;
      try (var query = connection.prepareStatement("SELECT scope FROM ps_java_checkpoints WHERE session=? AND acknowledged=0 AND scope<>?")) {
        query.setString(1, session); query.setLong(2, scopeId);
        try (var rows = query.executeQuery()) {
          int checked = 0;
          while (rows.next()) {
            if (++checked > 1024) throw Wire.integrity("retained checkpoint count exceeds policy");
            for (ScopeInfo ancestor : ancestry(connection, session, producer, rows.getLong(1))) if (ancestor.id() == scopeId) return false;
          }
        }
      }
      try (var update = connection.prepareStatement("UPDATE ps_java_checkpoints SET acknowledged=1,checksum=? WHERE session=? AND scope=? AND sequence=?")) {
        update.setBytes(1, checkpointChecksum(session, producer, encoded, true));
        update.setString(2, session); update.setLong(3, scopeId); update.setBytes(4, sequence(request.sequence())); update.executeUpdate();
      }
      saveCheckpointHistory(connection, session);
      return true;
    });
  }

  private static byte[] checkpointRequest(SealedTransport.Checkpoint request) throws ProtocolException {
    if (request.flags() != 0) throw Wire.entity("checkpoint request carries ACK");
    byte[] encoded = SealedTransport.checkpoint(request);
    if (encoded.length > 4096) throw Wire.limit("checkpoint identity exceeds local bound");
    return encoded;
  }

  private static Boolean checkpointRecord(Connection connection, String session, SealedTransport.Checkpoint request, byte[] encoded) throws SQLException, ProtocolException {
    try (var query = connection.prepareStatement("SELECT CASE WHEN length(request) BETWEEN 1 AND 4096 THEN request END,checksum,acknowledged FROM ps_java_checkpoints WHERE session=? AND scope=? AND sequence=?")) {
      query.setString(1, session); query.setLong(2, request.scopeId() == null ? 0 : request.scopeId()); query.setBytes(3, sequence(request.sequence()));
      try (var rows = query.executeQuery()) {
        if (!rows.next()) return null;
        byte[] stored = rows.getBytes(1);
        // auditCheckpoints validates identity, row-key correlation, and mutable ACK state first.
        if (stored == null) throw Wire.integrity("retained checkpoint identity is corrupt");
        if (!Arrays.equals(stored, encoded)) throw Wire.entity("checkpoint sequence reused with changed fields");
        return rows.getInt(3) == 1;
      }
    }
  }

  private static byte[] checkpointChecksum(String session, UUID producer, byte[] request, boolean acknowledged) {
    var hash = SealedWork.sha256();
    hash.update("pipestream-java-checkpoint-v1".getBytes(StandardCharsets.US_ASCII));
    byte[] identity = session.getBytes(StandardCharsets.US_ASCII);
    hash.update(ByteBuffer.allocate(4).putInt(identity.length).array()); hash.update(identity);
    hash.update(SealedWork.producerBytes(producer)); hash.update(request); hash.update((byte) (acknowledged ? 1 : 0));
    return hash.digest();
  }

  private static void auditCheckpoints(Connection connection, String session, UUID producer) throws SQLException, ProtocolException {
    long globalCount = count(connection, "SELECT count(*) FROM ps_java_checkpoints", null);
    if (globalCount > 4096 || globalCount != count(connection, "SELECT coalesce(sum(records),0) FROM ps_java_checkpoint_history", null)
        || count(connection, "SELECT count(*) FROM ps_java_checkpoint_history", null) != count(connection, "SELECT count(*) FROM ps_java_sessions", null)) throw Wire.integrity("checkpoint history violates global accounting");
    var history = SealedWork.sha256(); int count = 0;
    try (var query = connection.prepareStatement("SELECT scope,sequence,CASE WHEN length(request) BETWEEN 1 AND 4096 THEN request END,checksum,acknowledged FROM ps_java_checkpoints WHERE session=? ORDER BY scope,sequence")) {
      query.setString(1, session);
      try (var rows = query.executeQuery()) {
        while (rows.next()) {
          if (++count > 1024) throw Wire.integrity("checkpoint history exceeds session policy");
          byte[] encoded = rows.getBytes(3); int acknowledged = rows.getInt(5);
          if (encoded == null || (acknowledged != 0 && acknowledged != 1)
              || !MessageDigest.isEqual(checkpointChecksum(session, producer, encoded, acknowledged == 1), rows.getBytes(4))) throw Wire.integrity("retained checkpoint state is corrupt");
          var frame = Wire.decodeControl(encoded);
          if (frame.type() != Wire.FRAME_CHECKPOINT) throw Wire.integrity("retained checkpoint has wrong frame type");
          var request = SealedTransport.checkpoint(frame.payload());
          if (request.flags() != 0 || rows.getLong(1) != (request.scopeId() == null ? 0 : request.scopeId())
              || !Arrays.equals(rows.getBytes(2), sequence(request.sequence()))) throw Wire.integrity("retained checkpoint key changed");
          history.update(rows.getBytes(4));
        }
      }
    }
    try (var query = connection.prepareStatement("SELECT records,checksum FROM ps_java_checkpoint_history WHERE session=?")) {
      query.setString(1, session);
      try (var rows = query.executeQuery()) {
        if (!rows.next() || rows.getInt(1) != count || !MessageDigest.isEqual(history.digest(), rows.getBytes(2))) throw Wire.integrity("retained checkpoint history is incomplete or corrupt");
      }
    }
  }

  private static void saveCheckpointHistory(Connection connection, String session) throws SQLException, ProtocolException {
    var hash = SealedWork.sha256(); int count = 0;
    try (var query = connection.prepareStatement("SELECT checksum FROM ps_java_checkpoints WHERE session=? ORDER BY scope,sequence")) {
      query.setString(1, session);
      try (var rows = query.executeQuery()) {
        while (rows.next()) {
          if (++count > 1024) throw Wire.limit("checkpoint history exceeds session policy");
          hash.update(rows.getBytes(1));
        }
      }
    }
    try (var update = connection.prepareStatement("UPDATE ps_java_checkpoint_history SET records=?,checksum=? WHERE session=?")) {
      update.setInt(1, count); update.setBytes(2, hash.digest()); update.setString(3, session);
      if (update.executeUpdate() != 1) throw Wire.integrity("retained checkpoint history is absent");
    }
  }

  static int status(Connection connection, String session, UUID producer, SealedWork.EntityKey key)
      throws SQLException, ProtocolException {
    owner(connection, session, producer);
    EntityState entity = entity(connection, session, key);
    if (entity == null) throw Wire.entity("entity is undeclared");
    verifyDeclaration(connection, session, producer, scope(connection, session, key.scopeId()));
    return entity.state() == null ? 0 : entity.state();
  }

  /**
   * Records a processing result without executing a callback inside a transaction.
   * @param sessionId session identity
   * @param producerId retained label
   * @param key admitted entity
   * @param state COMPLETE, FAILED, or DEHYDRATING
   * @param outputDigest SHA-256 result commitment for COMPLETE, otherwise null
   * @throws ProtocolException for invalid lifecycle or result shape
   * @throws SQLException for persistence failure
   */
  public void processed(String sessionId, UUID producerId, SealedWork.EntityKey key,
      int state, byte[] outputDigest) throws ProtocolException, SQLException {
    transaction(connection -> {
      SealedJobs.requireUnmanaged(connection, sessionId, key);
      processed(connection, sessionId, producerId, key, state, outputDigest);
      return null;
    });
  }

  static void processed(Connection connection, String sessionId, UUID producerId, SealedWork.EntityKey key,
      int state, byte[] outputDigest) throws ProtocolException, SQLException {
    if ((state != 3 && state != 4 && state != 6) || (state == 3) != (outputDigest != null)
        || (outputDigest != null && outputDigest.length != 32)) throw Wire.entity("invalid processing outcome");
    byte[] digest = outputDigest == null ? null : outputDigest.clone();
    owner(connection, sessionId, producerId);
    EntityState entity = entity(connection, sessionId, key);
    if (entity == null || entity.state() == null || entity.state() != 2) throw Wire.entity("processing result requires PROCESSING state");
    verifyDeclaration(connection, sessionId, producerId, scope(connection, sessionId, key.scopeId()));
    setOutcome(connection, sessionId, key, state, digest);
  }

  /**
   * Reads all declared identities in a scope, including payloads that never arrived.
   * @param sessionId session identity
   * @param producerId retained label
   * @param scopeId scope to inspect
   * @return immutable ascending identifiers
   * @throws ProtocolException for identity mismatch or absent scope
   * @throws SQLException for persistence failure
   */
  public List<Long> declared(String sessionId, UUID producerId, long scopeId) throws ProtocolException, SQLException {
    return readTransaction(connection -> {
      owner(connection, sessionId, producerId);
      Scope scope = scope(connection, sessionId, scopeId);
      if (scope == null) throw Wire.entity("scope is absent");
      verifyDeclaration(connection, sessionId, producerId, scope);
      return List.copyOf(identifiers(connection, sessionId, scopeId));
    });
  }

  /**
   * Closes a child scope only after its sealed membership and descendants resolve.
   * Repeating the operation verifies and returns the same durable summary.
   * @param sessionId session identity
   * @param producerId retained label, not caller authentication
   * @param scopeId nonzero child scope
   * @return committed summary, or empty while any declared work is outstanding
   * @throws ProtocolException for unknown scope, identity, or inconsistent retained state
   * @throws SQLException for persistence failure
   */
  public Optional<SealedScope.Digest> closeScope(String sessionId, UUID producerId, long scopeId)
      throws ProtocolException, SQLException {
    SealedScope.childScope(scopeId);
    return transaction(connection -> {
      Scope scope = scope(connection, sessionId, scopeId);
      if (scope != null) SealedJobs.requireUnmanaged(connection, sessionId, scope.parent());
      return closeScope(connection, sessionId, producerId, scopeId);
    });
  }

  static Optional<SealedScope.Digest> closeScope(Connection connection, String sessionId, UUID producerId, long scopeId)
      throws ProtocolException, SQLException {
    var digest = previewScope(connection, sessionId, producerId, scopeId);
    if (digest.isPresent()) commitClosure(connection, sessionId, producerId, digest.get());
    return digest;
  }

  static Optional<SealedScope.Digest> previewScope(Connection connection, String sessionId, UUID producerId, long scopeId)
      throws ProtocolException, SQLException {
    SealedScope.childScope(scopeId);
    SealedJobs.audit(connection);
    owner(connection, sessionId, producerId);
    Scope scope = scope(connection, sessionId, scopeId);
    if (scope == null) throw SealedScope.invalid("scope is absent");
    List<SealedScope.Terminal> leaves = terminalScope(connection, sessionId, producerId, scope, 0);
    if (leaves == null) return Optional.empty();
    SealedScope.Digest digest = SealedScope.summarize(scopeId, leaves);
    return Optional.of(digest);
  }

  static void commitClosure(Connection connection, String sessionId, UUID producerId, SealedScope.Digest digest)
      throws SQLException, ProtocolException {
    long scopeId = digest.scopeId();
    Scope scope = scope(connection, sessionId, scopeId);
    if (scope == null) throw Wire.integrity("closure scope disappeared");
    if (scope.closure() == null) {
      SealedSqliteImages.replace(connection, "ps_java_scopes", "closure_image", scope.rowid(),
          SealedStateImages.closure(sessionId, SealedWork.producerBytes(producerId), scopeId, scope.parent(), SealedScope.encode(digest)));
    }
  }

  /**
   * Resolves a DEHYDRATING parent's closed child set under the Layer 1 STRICT policy.
   * All-success starts REHYDRATING; a terminal failure propagates FAILED without a callback.
   * @param sessionId session identity
   * @param producerId retained label
   * @param parent admitted DEHYDRATING entity
   * @return pending, newly REHYDRATING, or newly FAILED
   * @throws ProtocolException for invalid lifecycle, identity, or storage integrity
   * @throws SQLException for persistence failure
   */
  public ChildResolution resolveChildren(String sessionId, UUID producerId, SealedWork.EntityKey parent)
      throws ProtocolException, SQLException {
    return transaction(connection -> {
      SealedJobs.requireUnmanaged(connection, sessionId, parent);
      return resolveChildren(connection, sessionId, producerId, parent);
    });
  }

  static ChildResolution resolveChildren(Connection connection, String sessionId, UUID producerId, SealedWork.EntityKey parent)
      throws ProtocolException, SQLException {
    owner(connection, sessionId, producerId);
    EntityState entity = entity(connection, sessionId, parent);
    if (entity == null || entity.state() == null || entity.state() != 6) throw Wire.entity("child resolution requires DEHYDRATING parent");
    verifyDeclaration(connection, sessionId, producerId, scope(connection, sessionId, parent.scopeId()));
    Scope child = child(connection, sessionId, parent);
    if (child == null || child.closure() == null) return ChildResolution.PENDING;
    List<SealedScope.Terminal> leaves = terminalScope(connection, sessionId, producerId, child, 0);
    if (leaves == null) throw Wire.integrity("closed child has outstanding work");
    boolean success = leaves.stream().allMatch(leaf -> leaf.state() == Wire.STATUS_COMPLETE);
    setOutcome(connection, sessionId, parent, success ? 7 : Wire.STATUS_FAILED, null);
    return success ? ChildResolution.REHYDRATING : ChildResolution.FAILED;
  }

  /**
   * Publishes a rehydration result after child closure and the REHYDRATING transition.
   * Application execution takes place outside this transaction; this API is not an executor fence.
   * @param sessionId session identity
   * @param producerId retained label
   * @param parent REHYDRATING parent
   * @param success whether the application completed successfully
   * @param outputDigest SHA-256 output commitment on success, otherwise null
   * @throws ProtocolException for invalid lifecycle, result, or child state
   * @throws SQLException for persistence failure
   */
  public void rehydrated(String sessionId, UUID producerId, SealedWork.EntityKey parent,
      boolean success, byte[] outputDigest) throws ProtocolException, SQLException {
    transaction(connection -> {
      SealedJobs.requireUnmanaged(connection, sessionId, parent);
      rehydrated(connection, sessionId, producerId, parent, success, outputDigest);
      return null;
    });
  }

  static void rehydrated(Connection connection, String sessionId, UUID producerId, SealedWork.EntityKey parent,
      boolean success, byte[] outputDigest) throws ProtocolException, SQLException {
    if (success != (outputDigest != null) || (outputDigest != null && outputDigest.length != 32)) throw Wire.entity("invalid rehydration outcome");
    byte[] digest = outputDigest == null ? null : outputDigest.clone();
    owner(connection, sessionId, producerId);
    EntityState entity = entity(connection, sessionId, parent);
    if (entity == null || entity.state() == null || entity.state() != 7) throw Wire.entity("rehydration result requires REHYDRATING parent");
    verifyDeclaration(connection, sessionId, producerId, scope(connection, sessionId, parent.scopeId()));
    Scope child = child(connection, sessionId, parent);
    if (child == null || child.closure() == null) throw Wire.integrity("rehydrating parent has no closed child scope");
    List<SealedScope.Terminal> leaves = terminalScope(connection, sessionId, producerId, child, 0);
    if (leaves == null || leaves.stream().anyMatch(leaf -> leaf.state() != Wire.STATUS_COMPLETE)) throw Wire.integrity("STRICT rehydration requires every child COMPLETE");
    setOutcome(connection, sessionId, parent, success ? Wire.STATUS_COMPLETE : Wire.STATUS_FAILED, digest);
  }

  /**
   * Examines durable readiness for an inclusive, whole-sealed-scope checkpoint cut.
   * This is not a checkpoint ACK: the connection must also enforce request correlation,
   * monotonic deadlines, outstanding ingress, and nested checkpoint acknowledgements.
   * @param sessionId session identity
   * @param producerId retained label
   * @param scopeId root or child scope
   * @param inclusiveLastId largest identifier declared in that scope
   * @return false while the scope is unsealed or any declared work is outstanding
   * @throws ProtocolException for identity, corruption, or a wrong bound on a ready scope
   * @throws SQLException for persistence failure
   */
  public boolean checkpointReady(String sessionId, UUID producerId, long scopeId, long inclusiveLastId)
      throws ProtocolException, SQLException {
    if (scopeId < 0 || scopeId > 0xffff_ffffL || inclusiveLastId < 1 || inclusiveLastId > Wire.MAX_ENTITY_ID) throw Wire.entity("invalid checkpoint identity");
    return readTransaction(connection -> checkpointReady(connection, sessionId, producerId, scopeId, inclusiveLastId));
  }

  private static boolean checkpointReady(Connection connection, String sessionId, UUID producerId, long scopeId, long inclusiveLastId)
      throws SQLException, ProtocolException {
      owner(connection, sessionId, producerId);
      SealedJobs.audit(connection);
      Scope scope = scope(connection, sessionId, scopeId);
      if (scope == null) throw SealedScope.invalid("checkpoint scope is absent");
      List<SealedScope.Terminal> leaves = terminalScope(connection, sessionId, producerId, scope, 0);
      if (leaves == null) return false;
      if (leaves.getLast().entityId() != inclusiveLastId) throw Wire.entity("checkpoint does not name the entire sealed scope");
      return true;
  }

  private static void setOutcome(Connection connection, String session, SealedWork.EntityKey key,
      int state, byte[] digest) throws SQLException, ProtocolException {
    EntityState entity = entity(connection, session, key);
    if (entity == null) throw Wire.integrity("entity disappeared during publication");
    SealedSqliteImages.replace(connection, "ps_java_entities", "image", entity.rowid(),
        SealedStateImages.entity(session, entity.producer(), key,
            new SealedStateImages.Entity(state, entity.value().managed(), entity.value().payloadDigest(), digest)));
  }

  static byte[] childClosure(Connection connection, String session, SealedWork.EntityKey parent) throws SQLException, ProtocolException {
    Scope child = child(connection, session, parent);
    return child == null ? null : child.closure();
  }

  private static Scope child(Connection connection, String session, SealedWork.EntityKey parent) throws SQLException, ProtocolException {
    try (PreparedStatement query = connection.prepareStatement("SELECT id FROM ps_java_scopes WHERE session=? AND parent_scope=? AND parent_id=?")) {
      query.setString(1, session); query.setLong(2, parent.scopeId()); query.setLong(3, parent.entityId());
      try (ResultSet rows = query.executeQuery()) {
        if (!rows.next()) return null;
        return scope(connection, session, rows.getLong(1));
      }
    }
  }

  private static List<SealedScope.Terminal> terminalScope(Connection connection, String session,
      UUID producer, Scope scope, int recursion) throws SQLException, ProtocolException {
    if (recursion > 7) throw Wire.integrity("retained scope graph exceeds protocol depth");
    verifyDeclaration(connection, session, producer, scope);
    boolean ready = scope.sealed();
    List<SealedScope.Terminal> leaves = new ArrayList<>();
    try (PreparedStatement query = connection.prepareStatement("SELECT id,CASE WHEN length(image)=112 THEN image END FROM ps_java_entities WHERE session=? AND scope=? ORDER BY id")) {
      query.setString(1, session); query.setLong(2, scope.id());
      try (ResultSet rows = query.executeQuery()) {
        while (rows.next()) {
          if (leaves.size() >= MAX_SESSION_DECLARATIONS) throw Wire.integrity("retained scope exceeds declaration budget");
          long id = rows.getLong(1);
          var entity = SealedStateImages.entity(session, SealedWork.producerBytes(producer), new SealedWork.EntityKey(scope.id(), id), rows.getBytes(2));
          Integer state = entity.state();
          if (state == null || (state != Wire.STATUS_COMPLETE && state != Wire.STATUS_FAILED)) ready = false;
          leaves.add(new SealedScope.Terminal(id, state == null ? 0 : state));
        }
      }
    }
    try (PreparedStatement query = connection.prepareStatement("SELECT id FROM ps_java_scopes WHERE session=? AND parent_scope=?")) {
      query.setString(1, session); query.setLong(2, scope.id());
      try (ResultSet rows = query.executeQuery()) {
        int children = 0;
        while (rows.next()) {
          if (++children > leaves.size()) throw Wire.integrity("retained child scopes exceed declared parents");
          Scope child = scope(connection, session, rows.getLong(1));
          if (child.closure() == null || terminalScope(connection, session, producer, child, recursion + 1) == null) ready = false;
        }
      }
    }
    if (scope.closure() != null && (!ready || !Arrays.equals(scope.closure(),
        SealedScope.encode(SealedScope.summarize(scope.id(), leaves))))) throw Wire.integrity("retained scope closure disagrees with declarations or statuses");
    return ready ? leaves : null;
  }

  private static void verifyDeclaration(Connection connection, String session, UUID producer, Scope scope)
      throws SQLException, ProtocolException {
    if (scope == null) throw Wire.integrity("retained entity has no scope");
    List<Long> declared = new ArrayList<>();
    BigInteger next = BigInteger.ZERO;
    byte[] seal = null;
    try (PreparedStatement query = connection.prepareStatement("SELECT sequence,CASE WHEN length(request) BETWEEN 1 AND 8192 THEN request END,checksum FROM ps_java_batches WHERE session=? AND scope=? ORDER BY sequence")) {
      query.setString(1, session); query.setLong(2, scope.id());
      try (ResultSet rows = query.executeQuery()) {
        while (rows.next()) {
          if (next.compareTo(BigInteger.valueOf(MAX_SESSION_DECLARATIONS)) > 0) throw Wire.integrity("retained batch count exceeds format bound");
          byte[] encoded = rows.getBytes(2);
          if (encoded == null || !MessageDigest.isEqual(SealedWork.sha256().digest(encoded), rows.getBytes(3))) throw Wire.integrity("retained declaration is oversized or corrupt");
          SealedWork.Declaration batch;
          try { batch = SealedWork.decode(encoded); }
          catch (ProtocolException invalid) { throw Wire.integrity("retained declaration is malformed"); }
          if (!Arrays.equals(sequence(next), rows.getBytes(1)) || !batch.sequence().equals(next)
              || !batch.sessionId().equals(session) || !batch.producerId().equals(producer)
              || batch.scopeId() != scope.id() || !Objects.equals(batch.parent(), scope.parent())
              || (batch.flags() & SealedWork.ACK) != 0 || seal != null
              || (!declared.isEmpty() && !batch.entityIds().isEmpty() && batch.entityIds().getFirst() <= declared.getLast())
              || declared.size() + batch.entityIds().size() > MAX_SESSION_DECLARATIONS) throw Wire.integrity("retained declaration history or binding differs");
          declared.addAll(batch.entityIds()); seal = batch.sealDigest(); next = next.add(BigInteger.ONE);
        }
      }
    }
    if (declared.isEmpty() || !next.equals(scope.nextSequence()) || scope.sealed() != (seal != null)
        || !Arrays.equals(seal, scope.digest()) || !declared.equals(identifiers(connection, session, scope.id()))
        || declared.size() > sessionEntityLimit(connection, session)
        || (seal != null && !MessageDigest.isEqual(seal, SealedWork.sealDigest(session, producer, scope.id(), scope.parent(), declared)))) {
      throw Wire.integrity("retained scope no longer matches acknowledged membership");
    }
    if (scope.parent() == null) {
      if (scope.id() != 0 || scope.depth() != 0) throw Wire.integrity("retained root binding is invalid");
    } else {
      Scope parent = scope(connection, session, scope.parent().scopeId());
      EntityState entity = entity(connection, session, scope.parent());
      if (parent == null || entity == null || entity.state() == null || scope.depth() != parent.depth() + 1
          || scope.depth() > sessionDepth(connection, session)) throw Wire.integrity("retained parent binding or depth is invalid");
    }
  }

  private Connection connection() throws SQLException {
    Connection connection = files.connect();
    try (var statement = connection.createStatement()) {
      statement.execute("PRAGMA busy_timeout=5000");
      statement.execute("PRAGMA foreign_keys=ON");
      statement.execute("PRAGMA synchronous=FULL");
      statement.execute("PRAGMA temp_store=MEMORY");
      statement.execute("PRAGMA mmap_size=0");
      long pages;
      try (var size = statement.executeQuery("PRAGMA page_size")) {
        if (!size.next()) throw new SQLException("missing SQLite page size");
        pages = files.limits().databaseBytes() / size.getLong(1);
      }
      try (var cap = statement.executeQuery("PRAGMA max_page_count=" + pages)) {
        if (!cap.next() || cap.getLong(1) > pages) throw new SQLException("database exceeds page limit");
      }
    } catch (SQLException failure) { connection.close(); throw failure; }
    return connection;
  }

  private static void policy(Connection connection) throws SQLException {
    try (var query = connection.createStatement(); ResultSet rows = query.executeQuery("SELECT version,sessions,declarations,per_session FROM ps_java_meta WHERE singleton=1")) {
      if (!rows.next() || rows.getInt(1) != VERSION || rows.getLong(2) != MAX_SESSIONS
          || rows.getLong(3) != MAX_DECLARATIONS || rows.getLong(4) != MAX_SESSION_DECLARATIONS || rows.next()) {
        throw new SQLException("unsupported Java sealed-work policy or schema version");
      }
    }
    SealedJobs.checkPolicy(connection);
    try {
      var binding = binding(connection);
      if (binding.payloads().equals(SealedStoreBinding.UNBOUND)) {
        try (var query = connection.createStatement(); var rows = query.executeQuery("SELECT 1 FROM ps_java_jobs LIMIT 1")) {
          if (rows.next()) throw new SQLException("managed jobs have no payload-store binding");
        }
      }
    } catch (ProtocolException failure) { throw new SQLException("corrupt Java store binding", failure); }
  }

  SealedStoreBinding binding() throws SQLException, ProtocolException { return readTransaction(SealedSessionStore::binding); }

  private static SealedStoreBinding binding(Connection connection) throws SQLException, ProtocolException {
    try (var query = connection.createStatement(); var rows = query.executeQuery(
        "SELECT CASE WHEN length(binding)=72 THEN binding END FROM ps_java_meta WHERE singleton=1")) {
      if (!rows.next()) throw Wire.integrity("missing Java store binding");
      return SealedStoreBinding.decode(rows.getBytes(1));
    }
  }

  void bindPayloads(SealedStoreBinding expected) throws SQLException, ProtocolException {
    if (expected.payloads().equals(SealedStoreBinding.UNBOUND)) throw Wire.integrity("payload identity is unbound");
    transaction(connection -> {
      var current = binding(connection);
      if (!current.database().equals(expected.database())
          || (!current.payloads().equals(SealedStoreBinding.UNBOUND) && !current.equals(expected))) {
        throw Wire.integrity("Java database belongs to a different payload store");
      }
      if (!current.equals(expected)) SealedSqliteImages.replace(connection, "ps_java_meta", "binding", 1, expected.encode());
      return null;
    });
  }

  <T> T withPayloadMaintenance(SealedStoreBinding expected, Maintenance<T> operation)
      throws IOException, SQLException, ProtocolException {
    try {
      return transaction(connection -> {
        if (!binding(connection).equals(expected)) throw Wire.integrity("payload maintenance requires the retained database/store pair");
        try { return operation.apply(connection); }
        catch (IOException failure) { throw new UncheckedIOException(failure); }
      });
    } catch (UncheckedIOException failure) {
      IOException cause = failure.getCause();
      for (Throwable suppressed : failure.getSuppressed()) cause.addSuppressed(suppressed);
      throw cause;
    }
  }

  @FunctionalInterface interface Maintenance<T> {
    T apply(Connection connection) throws IOException, SQLException, ProtocolException;
  }

  <T> T transaction(Operation<T> operation) throws SQLException, ProtocolException {
    return fundedTransaction((connection, funding) -> operation.apply(connection));
  }

  /** Observations hold a checked read snapshot, never SQLite's writer lock. */
  <T> T readTransaction(Operation<T> operation) throws SQLException, ProtocolException {
    try (Connection connection = connection(); var statement = connection.createStatement()) {
      statement.execute("PRAGMA query_only=ON");
      statement.execute("BEGIN");
      try {
        policy(connection);
        SealedJobs.audit(connection);
        T result = operation.apply(connection);
        statement.execute("COMMIT");
        return result;
      } catch (SQLException | ProtocolException | RuntimeException failure) {
        rollback(connection, failure);
        throw failure;
      }
    } catch (SQLException failure) {
      if (SealedSqliteFiles.isFull(failure)) {
        ProtocolException refusal = Wire.limit("SQLite file capacity exhausted");
        refusal.initCause(failure);
        throw refusal;
      }
      throw failure;
    }
  }

  <T> T fundedTransaction(FundedOperation<T> operation) throws SQLException, ProtocolException {
    try (Connection connection = connection(); var statement = connection.createStatement()) {
      statement.execute("BEGIN IMMEDIATE");
      try {
        policy(connection);
        var funding = SealedCompletionReservations.protect(connection, files.limits());
        T result = operation.apply(connection, funding);
        funding.verify();
        statement.execute("COMMIT");
        return result;
      } catch (SQLException | ProtocolException | RuntimeException failure) {
        rollback(connection, failure);
        throw failure;
      }
    } catch (SQLException failure) {
      if (SealedSqliteFiles.isFull(failure)) {
        ProtocolException refusal = Wire.limit("SQLite file capacity exhausted");
        refusal.initCause(failure);
        throw refusal;
      }
      throw failure;
    }
  }

  private static void rollback(Connection connection, Exception failure) {
    try (var statement = connection.createStatement()) { statement.execute("ROLLBACK"); }
    catch (SQLException rollback) { failure.addSuppressed(rollback); }
  }

  private static void owner(Connection connection, String id, UUID producer) throws SQLException, ProtocolException {
    if (!SealedWork.validSessionId(id)) throw Wire.entity("invalid session identity");
    try (PreparedStatement query = connection.prepareStatement("SELECT producer FROM ps_java_sessions WHERE id=?")) {
      query.setString(1, id);
      try (ResultSet rows = query.executeQuery()) {
        if (!rows.next()) throw Wire.entity("session is absent");
        producer(producer, rows.getBytes(1));
      }
    }
  }

  private static void producer(UUID producer, byte[] retained) throws ProtocolException {
    if (!MessageDigest.isEqual(SealedWork.producerBytes(producer), retained)) throw Wire.entity("producer binding differs");
  }

  private static int sessionDepth(Connection connection, String id) throws SQLException {
    return (int) count(connection, "SELECT depth FROM ps_java_sessions WHERE id=?", id);
  }

  private static long sessionEntityLimit(Connection connection, String id) throws SQLException {
    return count(connection, "SELECT scope_limit FROM ps_java_sessions WHERE id=?", id);
  }

  private static long count(Connection connection, String sql, String id) throws SQLException {
    try (PreparedStatement query = connection.prepareStatement(sql)) {
      if (id != null) query.setString(1, id);
      try (ResultSet rows = query.executeQuery()) {
        if (!rows.next()) throw new SQLException("expected retained row");
        return rows.getLong(1);
      }
    }
  }

  private static Scope scope(Connection connection, String session, long id) throws SQLException, ProtocolException {
    try (PreparedStatement query = connection.prepareStatement("SELECT parent_scope,parent_id,s.depth,next_sequence,sealed,digest,CASE WHEN length(closure_image)=128 THEN closure_image END,s.rowid,p.producer FROM ps_java_scopes s JOIN ps_java_sessions p ON p.id=s.session WHERE session=? AND s.id=?")) {
      query.setString(1, session); query.setLong(2, id);
      try (ResultSet rows = query.executeQuery()) {
        if (!rows.next()) return null;
        long parentScope = rows.getLong(1);
        boolean root = rows.wasNull();
        var parent = root ? null : new SealedWork.EntityKey(parentScope, rows.getLong(2));
        byte[] closure = SealedStateImages.readClosure(session, rows.getBytes(9), id, parent, rows.getBytes(7));
        boolean sealed = rows.getInt(5) == 1;
        if (closure != null && !sealed) throw Wire.integrity("unsealed scope contains a closure");
        return new Scope(id, parent, rows.getInt(3), new BigInteger(1, rows.getBytes(4)), sealed, rows.getBytes(6), closure, rows.getLong(8));
      }
    }
  }

  static EntityState entity(Connection connection, String session, SealedWork.EntityKey key) throws SQLException, ProtocolException {
    try (PreparedStatement query = connection.prepareStatement("SELECT e.rowid,CASE WHEN length(image)=112 THEN image END,s.producer FROM ps_java_entities e JOIN ps_java_sessions s ON s.id=e.session WHERE session=? AND scope=? AND e.id=?")) {
      query.setString(1, session); query.setLong(2, key.scopeId()); query.setLong(3, key.entityId());
      try (ResultSet rows = query.executeQuery()) {
        if (!rows.next()) return null;
        return new EntityState(rows.getLong(1), rows.getBytes(3), SealedStateImages.entity(session, rows.getBytes(3), key, rows.getBytes(2)));
      }
    }
  }

  private static List<Long> identifiers(Connection connection, String session, long scope) throws SQLException {
    List<Long> ids = new ArrayList<>();
    try (PreparedStatement query = connection.prepareStatement("SELECT id FROM ps_java_entities WHERE session=? AND scope=? ORDER BY id")) {
      query.setString(1, session); query.setLong(2, scope);
      try (ResultSet rows = query.executeQuery()) {
        while (rows.next()) {
          if (ids.size() >= MAX_SESSION_DECLARATIONS) throw new SQLException("stored identifiers exceed local budget");
          ids.add(rows.getLong(1));
        }
      }
    }
    return ids;
  }

  private static byte[] sequence(BigInteger sequence) { return ByteBuffer.allocate(8).putLong(sequence.longValue()).array(); }
  private record Scope(long id, SealedWork.EntityKey parent, int depth, BigInteger nextSequence, boolean sealed, byte[] digest, byte[] closure, long rowid) {}
  record EntityState(long rowid, byte[] producer, SealedStateImages.Entity value) {
    EntityState { producer = producer.clone(); }
    @Override public byte[] producer() { return producer.clone(); }
    Integer state() { return value.state(); }
  }
  @FunctionalInterface interface Operation<T> { T apply(Connection connection) throws SQLException, ProtocolException; }
  @FunctionalInterface interface FundedOperation<T> {
    T apply(Connection connection, SealedCompletionReservations funding) throws SQLException, ProtocolException;
  }
}
