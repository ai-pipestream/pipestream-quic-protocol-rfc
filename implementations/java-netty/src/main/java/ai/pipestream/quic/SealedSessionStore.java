package ai.pipestream.quic;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.sql.Connection;
import java.sql.DriverManager;
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
  /** Parent resolution when its child scope is examined under the Layer 1 STRICT policy. */
  public enum ChildResolution {
    /** The child scope is missing or has not closed. */
    PENDING,
    /** Every child succeeded; the parent durably entered REHYDRATING. */
    REHYDRATING,
    /** At least one child failed; the parent durably entered FAILED. */
    FAILED
  }
  private static final int VERSION = 2;
  private static final long MAX_SESSIONS = 512;
  private static final long MAX_DECLARATIONS = 65_536;
  private static final long MAX_SESSION_DECLARATIONS = 16_384;
  private static final Set<String> TABLES = Set.of("ps_java_meta", "ps_java_sessions",
      "ps_java_scopes", "ps_java_batches", "ps_java_entities", "ps_java_jobs", "ps_java_job_policy");
  private final Path database;

  private SealedSessionStore(Path database) { this.database = database; }

  /**
   * Opens or creates this implementation's database without converting another format.
   * @param database database filename, separate from the Rust store
   * @return stateless handle; each operation owns its connection and transaction
   * @throws IOException when its parent directory cannot be created
   * @throws SQLException for unsupported schema, corruption, or SQLite failure
   */
  public static SealedSessionStore open(Path database) throws IOException, SQLException {
    Path path = database.toAbsolutePath().normalize();
    Files.createDirectories(path.getParent());
    SealedSessionStore store = new SealedSessionStore(path);
    try (Connection connection = store.connection(); var statement = connection.createStatement()) {
      statement.execute("BEGIN IMMEDIATE");
      try {
        Set<String> present = new TreeSet<>();
        try (ResultSet rows = statement.executeQuery("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")) {
          while (rows.next()) present.add(rows.getString(1));
        }
        if (present.isEmpty()) {
          statement.execute("CREATE TABLE ps_java_meta (singleton INTEGER PRIMARY KEY CHECK(singleton=1), version INTEGER NOT NULL, sessions INTEGER NOT NULL, declarations INTEGER NOT NULL, per_session INTEGER NOT NULL) STRICT");
          statement.execute("INSERT INTO ps_java_meta VALUES (1," + VERSION + "," + MAX_SESSIONS + "," + MAX_DECLARATIONS + "," + MAX_SESSION_DECLARATIONS + ")");
          statement.execute("CREATE TABLE ps_java_sessions (id TEXT PRIMARY KEY, producer BLOB NOT NULL CHECK(length(producer)=16), depth INTEGER NOT NULL CHECK(depth BETWEEN 0 AND 7), scope_limit INTEGER NOT NULL CHECK(scope_limit BETWEEN 1 AND 4294967292)) STRICT");
          statement.execute("""
              CREATE TABLE ps_java_scopes (
                session TEXT NOT NULL REFERENCES ps_java_sessions(id),
                id INTEGER NOT NULL CHECK(id BETWEEN 0 AND 4294967295),
                parent_scope INTEGER, parent_id INTEGER,
                depth INTEGER NOT NULL CHECK(depth BETWEEN 0 AND 7),
                next_sequence BLOB NOT NULL CHECK(length(next_sequence)=8),
                sealed INTEGER NOT NULL CHECK(sealed IN (0,1)), digest BLOB, closure BLOB,
                PRIMARY KEY(session,id), UNIQUE(session,parent_scope,parent_id),
                FOREIGN KEY(session,parent_scope,parent_id) REFERENCES ps_java_entities(session,scope,id),
                CHECK((id=0 AND parent_scope IS NULL AND parent_id IS NULL AND depth=0)
                   OR (id>0 AND parent_scope IS NOT NULL AND parent_id IS NOT NULL AND depth>0)),
                CHECK((sealed=0 AND digest IS NULL) OR (sealed=1 AND digest IS NOT NULL AND length(digest)=32)),
                CHECK(closure IS NULL OR (id>0 AND sealed=1 AND length(closure)=77))
              ) STRICT
              """);
          statement.execute("CREATE TABLE ps_java_batches (session TEXT NOT NULL, scope INTEGER NOT NULL, sequence BLOB NOT NULL CHECK(length(sequence)=8), request BLOB NOT NULL CHECK(length(request) BETWEEN 1 AND 8192), checksum BLOB NOT NULL CHECK(length(checksum)=32), PRIMARY KEY(session,scope,sequence), FOREIGN KEY(session,scope) REFERENCES ps_java_scopes(session,id)) STRICT");
          statement.execute("""
              CREATE TABLE ps_java_entities (
                session TEXT NOT NULL, scope INTEGER NOT NULL,
                id INTEGER NOT NULL CHECK(id BETWEEN 1 AND 4294967292),
                state INTEGER, payload_digest BLOB, output_digest BLOB,
                managed INTEGER NOT NULL DEFAULT 0 CHECK(managed IN (0,1)),
                PRIMARY KEY(session,scope,id),
                FOREIGN KEY(session,scope) REFERENCES ps_java_scopes(session,id),
                CHECK((state IS NULL AND payload_digest IS NULL AND output_digest IS NULL)
                   OR (state IS NOT NULL AND state IN (2,3,4,6,7)
                     AND payload_digest IS NOT NULL AND length(payload_digest)=32)),
                CHECK(managed=0 OR state IS NOT NULL),
                CHECK((state=3 AND output_digest IS NOT NULL AND length(output_digest)=32)
                   OR ((state IS NULL OR state<>3) AND output_digest IS NULL))
              ) STRICT
              """);
          SealedJobs.createSchema(connection);
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
        try (PreparedStatement insert = connection.prepareStatement("INSERT INTO ps_java_scopes VALUES (?,?,?,?,?,?,0,NULL,NULL)")) {
          insert.setString(1, request.sessionId()); insert.setLong(2, request.scopeId());
          insert.setObject(3, request.parent() == null ? null : request.parent().scopeId());
          insert.setObject(4, request.parent() == null ? null : request.parent().entityId());
          insert.setInt(5, depth); insert.setBytes(6, sequence(BigInteger.ZERO)); insert.executeUpdate();
        }
        scope = new Scope(request.scopeId(), request.parent(), depth, BigInteger.ZERO, false, null, null);
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
      try (PreparedStatement insert = connection.prepareStatement("INSERT INTO ps_java_entities(session,scope,id) VALUES (?,?,?)")) {
        for (long id : request.entityIds()) {
          insert.setString(1, request.sessionId()); insert.setLong(2, request.scopeId()); insert.setLong(3, id); insert.addBatch();
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
    if (payloadDigest == null || payloadDigest.length != 32) throw Wire.integrity("payload digest must be SHA-256");
    byte[] digest = payloadDigest.clone();
    owner(connection, sessionId, producerId);
    Scope scope = scope(connection, sessionId, key.scopeId());
    EntityState entity = entity(connection, sessionId, key);
    if (scope == null || !Objects.equals(scope.parent(), parent) || entity == null || entity.state() != null) throw Wire.entity("payload is undeclared, repeated, or has a different parent");
    verifyDeclaration(connection, sessionId, producerId, scope);
    try (PreparedStatement update = connection.prepareStatement("UPDATE ps_java_entities SET state=2,payload_digest=? WHERE session=? AND scope=? AND id=?")) {
      update.setBytes(1, digest); update.setString(2, sessionId); update.setLong(3, key.scopeId()); update.setLong(4, key.entityId()); update.executeUpdate();
    }
  }

  Path database() { return database; }

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
    return transaction(connection -> {
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
    SealedScope.childScope(scopeId);
    SealedJobs.audit(connection);
    owner(connection, sessionId, producerId);
    Scope scope = scope(connection, sessionId, scopeId);
    if (scope == null) throw SealedScope.invalid("scope is absent");
    List<SealedScope.Terminal> leaves = terminalScope(connection, sessionId, producerId, scope, 0);
    if (leaves == null) return Optional.empty();
    SealedScope.Digest digest = SealedScope.summarize(scopeId, leaves);
    if (scope.closure() == null) {
      try (PreparedStatement update = connection.prepareStatement("UPDATE ps_java_scopes SET closure=? WHERE session=? AND id=?")) {
        update.setBytes(1, SealedScope.encode(digest)); update.setString(2, sessionId); update.setLong(3, scopeId);
        update.executeUpdate();
      }
    }
    return Optional.of(digest);
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
    return transaction(connection -> {
      owner(connection, sessionId, producerId);
      SealedJobs.audit(connection);
      Scope scope = scope(connection, sessionId, scopeId);
      if (scope == null) throw SealedScope.invalid("checkpoint scope is absent");
      List<SealedScope.Terminal> leaves = terminalScope(connection, sessionId, producerId, scope, 0);
      if (leaves == null) return false;
      if (leaves.getLast().entityId() != inclusiveLastId) throw Wire.entity("checkpoint does not name the entire sealed scope");
      return true;
    });
  }

  private static void setOutcome(Connection connection, String session, SealedWork.EntityKey key,
      int state, byte[] digest) throws SQLException {
    try (PreparedStatement update = connection.prepareStatement("UPDATE ps_java_entities SET state=?,output_digest=? WHERE session=? AND scope=? AND id=?")) {
      update.setInt(1, state); update.setBytes(2, digest); update.setString(3, session);
      update.setLong(4, key.scopeId()); update.setLong(5, key.entityId()); update.executeUpdate();
    }
  }

  private static Scope child(Connection connection, String session, SealedWork.EntityKey parent) throws SQLException {
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
    try (PreparedStatement query = connection.prepareStatement("SELECT id,state FROM ps_java_entities WHERE session=? AND scope=? ORDER BY id")) {
      query.setString(1, session); query.setLong(2, scope.id());
      try (ResultSet rows = query.executeQuery()) {
        while (rows.next()) {
          if (leaves.size() >= MAX_SESSION_DECLARATIONS) throw Wire.integrity("retained scope exceeds declaration budget");
          int state = rows.getInt(2);
          if (rows.wasNull() || (state != Wire.STATUS_COMPLETE && state != Wire.STATUS_FAILED)) ready = false;
          leaves.add(new SealedScope.Terminal(rows.getLong(1), state));
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
    Connection connection = DriverManager.getConnection("jdbc:sqlite:" + database);
    try (var statement = connection.createStatement()) {
      statement.execute("PRAGMA busy_timeout=5000");
      statement.execute("PRAGMA foreign_keys=ON");
      statement.execute("PRAGMA synchronous=FULL");
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
  }

  <T> T transaction(Operation<T> operation) throws SQLException, ProtocolException {
    try (Connection connection = connection(); var statement = connection.createStatement()) {
      statement.execute("BEGIN IMMEDIATE");
      try {
        policy(connection);
        T result = operation.apply(connection);
        statement.execute("COMMIT");
        return result;
      } catch (SQLException | ProtocolException | RuntimeException failure) {
        rollback(connection, failure);
        throw failure;
      }
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

  private static Scope scope(Connection connection, String session, long id) throws SQLException {
    try (PreparedStatement query = connection.prepareStatement("SELECT parent_scope,parent_id,depth,next_sequence,sealed,digest,CASE WHEN length(closure)=77 THEN closure END,length(closure) FROM ps_java_scopes WHERE session=? AND id=?")) {
      query.setString(1, session); query.setLong(2, id);
      try (ResultSet rows = query.executeQuery()) {
        if (!rows.next()) return null;
        long parentScope = rows.getLong(1);
        boolean root = rows.wasNull();
        if (rows.getObject(8) != null && rows.getLong(8) != 77) throw new SQLException("stored closure exceeds format bound");
        return new Scope(id, root ? null : new SealedWork.EntityKey(parentScope, rows.getLong(2)), rows.getInt(3),
            new BigInteger(1, rows.getBytes(4)), rows.getInt(5) == 1, rows.getBytes(6), rows.getBytes(7));
      }
    }
  }

  private static EntityState entity(Connection connection, String session, SealedWork.EntityKey key) throws SQLException {
    try (PreparedStatement query = connection.prepareStatement("SELECT state FROM ps_java_entities WHERE session=? AND scope=? AND id=?")) {
      query.setString(1, session); query.setLong(2, key.scopeId()); query.setLong(3, key.entityId());
      try (ResultSet rows = query.executeQuery()) {
        if (!rows.next()) return null;
        int state = rows.getInt(1);
        return new EntityState(rows.wasNull() ? null : state);
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
  private record Scope(long id, SealedWork.EntityKey parent, int depth, BigInteger nextSequence, boolean sealed, byte[] digest, byte[] closure) {}
  private record EntityState(Integer state) {}
  @FunctionalInterface interface Operation<T> { T apply(Connection connection) throws SQLException, ProtocolException; }
}
