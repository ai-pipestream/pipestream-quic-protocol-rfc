package ai.pipestream.quic;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Local producer write-ahead intents and verified observations, not server state.
 * The client must validate response semantics before recording an observation.
 * One cooperating process owns a journal; no network operation runs in its transactions.
 */
final class SealedProducerJournal implements AutoCloseable {
  /** Largest retained request or file-commitment descriptor. */
  static final int MAX_REQUEST_BYTES = 16 * 1024 * 1024;
  /** Largest normalized observation, excluding its internal integrity envelope. */
  static final int MAX_OBSERVATION_BYTES = 4096;
  private static final int IMAGE_OVERHEAD = 56;
  private static final byte[] POLICY_MAGIC = {'P', 'S', 'J', 'P', 'O', 'L', '0', '1'};
  private static final byte[] IMAGE_MAGIC = {'P', 'S', 'J', 'P', 'O', 'B', '0', '1'};
  private static final byte[] HEAD_MAGIC = {'P', 'S', 'J', 'P', 'H', 'D', '0', '1'};
  private static final String META = "CREATE TABLE ps_producer_meta (singleton INTEGER PRIMARY KEY CHECK(singleton=1), policy BLOB NOT NULL CHECK(length(policy)=100), head BLOB NOT NULL CHECK(length(head)=56)) STRICT";
  private static final String OPERATIONS = """
      CREATE TABLE ps_producer_operations (
        id INTEGER PRIMARY KEY CHECK(id>0),
        kind INTEGER NOT NULL CHECK(kind BETWEEN 0 AND 4),
        identity BLOB NOT NULL CHECK(length(identity) BETWEEN 1 AND 256),
        request BLOB NOT NULL CHECK(length(request) BETWEEN 1 AND 16777216),
        image BLOB NOT NULL CHECK(length(image) BETWEEN 57 AND 4152),
        UNIQUE(kind,identity)
      ) STRICT""";
  private static final Map<String, String> SCHEMA = Map.of("ps_producer_meta", META, "ps_producer_operations", OPERATIONS);
  private static final Set<Path> OWNERS = ConcurrentHashMap.newKeySet();
  private static final String RECORD_COLUMNS = "id,kind,length(identity),length(request),length(image),identity,request,image";

  /** Stable on-disk operation discriminants; existing ordinals must not change. */
  enum Kind {
    /** One declaration batch and its exact ACK. */
    DECLARATION,
    /** One immutable file or chunk-set commitment and its observed lifecycle. */
    INPUT,
    /** Child scope confirmation and parent rehydration observations. */
    SCOPE,
    /** One scope-qualified checkpoint identity and its exact ACK. */
    CHECKPOINT,
    /** Final root shutdown exchange. */
    SHUTDOWN
  }

  /**
   * Immutable logical reservations, separate from SQLite file lengths.
   * @param operations maximum retained identities
   * @param bytes total key, request and reserved observation image bytes
   */
  record Limits(int operations, long bytes) {
    /**
     * Checks the supported local policy range.
     * @param operations maximum retained identities
     * @param bytes maximum charged bytes
     */
    Limits {
      if (operations < 1 || operations > 131_072 || bytes < 1 || bytes > (1L << 30)) {
        throw new IllegalArgumentException("invalid producer journal limits");
      }
    }
  }

  /**
   * One immutable intent with its latest durably verified evidence.
   * @param id non-recycled local append position
   * @param kind operation discriminator
   * @param identity operation-specific identity bytes
   * @param request original request or immutable input descriptor
   * @param capacity reserved observation bytes
   * @param revision zero for no observation, otherwise increasing
   * @param observation caller-validated evidence, empty for an unobserved intent
   * @param resolved whether this operation's final response was verified
   */
  record Entry(long id, Kind kind, byte[] identity, byte[] request, int capacity,
      long revision, byte[] observation, boolean resolved) {
    /**
     * Copies mutable byte arrays; decoding and transition validation occur in the journal.
     * @param id append position
     * @param kind operation discriminator
     * @param identity operation identity
     * @param request immutable request
     * @param capacity observation reservation
     * @param revision observation revision
     * @param observation verified evidence
     * @param resolved final-response flag
     */
    Entry {
      identity = identity.clone(); request = request.clone(); observation = observation.clone();
    }
    /** Returns an identity copy.
     * @return detached identity bytes
     */
    @Override public byte[] identity() { return identity.clone(); }
    /** Returns a request copy.
     * @return detached original request bytes
     */
    @Override public byte[] request() { return request.clone(); }
    /** Returns an observation copy.
     * @return detached evidence bytes
     */
    @Override public byte[] observation() { return observation.clone(); }
  }

  /**
   * Durable logical charges, including unused observation capacity.
   * @param operations number of retained immutable intents
   * @param bytes total charged bytes
   */
  record Usage(long operations, long bytes) {}

  private final SealedSqliteFiles files;
  private final Connection connection;
  private final FileChannel lockChannel;
  private final FileLock lock;
  private final byte[] policy;
  private final Limits limits;
  private boolean closed;

  private SealedProducerJournal(SealedSqliteFiles files, Connection connection,
      FileChannel lockChannel, FileLock lock, byte[] policy, Limits limits) {
    this.files = files; this.connection = connection; this.lockChannel = lockChannel;
    this.lock = lock; this.policy = policy; this.limits = limits;
  }

  /**
   * Opens only an empty database or this exact immutable peer/policy binding.
   * @param path local database path
   * @param peerBinding 32-byte digest of the caller's peer trust context
   * @param limits immutable logical reservations
   * @param fileLimits immutable SQLite file-length bounds
   * @return audited journal with exclusive cooperating-writer ownership
   * @throws IOException for file policy or writer ownership failure
   * @throws SQLException for a foreign schema, changed binding or database failure
   * @throws ProtocolException for corrupt retained intent or observation evidence
   */
  static SealedProducerJournal open(Path path, byte[] peerBinding, Limits limits,
      SealedSessionStore.FileLimits fileLimits) throws IOException, SQLException, ProtocolException {
    if (peerBinding == null || peerBinding.length != 32 || limits == null || fileLimits == null) {
      throw new IllegalArgumentException("producer journal requires peer and storage limits");
    }
    byte[] peer = peerBinding.clone();
    SealedSqliteFiles files = SealedSqliteFiles.open(path, fileLimits);
    Path lockPath = files.path().resolveSibling(files.path().getFileName() + ".producerlock");
    // Closing another descriptor for the same inode drops this process's POSIX
    // record lock. Reject same-JVM opens before opening any second descriptor.
    if (!OWNERS.add(lockPath)) throw new IOException("producer journal already has a writer");
    FileChannel channel = null;
    FileLock lock = null; Connection connection = null;
    try {
      if (Files.exists(lockPath, LinkOption.NOFOLLOW_LINKS)) verifyLock(lockPath);
      channel = FileChannel.open(lockPath, StandardOpenOption.CREATE,
          StandardOpenOption.WRITE, LinkOption.NOFOLLOW_LINKS);
      verifyLock(lockPath);
      try { lock = channel.tryLock(); }
      catch (OverlappingFileLockException held) { throw new IOException("producer journal already has a writer", held); }
      if (lock == null) throw new IOException("producer journal already has a writer");
      connection = files.connect();
      byte[] policy;
      try (var statement = connection.createStatement()) {
        statement.execute("PRAGMA busy_timeout=1000");
        statement.execute("PRAGMA synchronous=FULL");
        statement.execute("PRAGMA temp_store=MEMORY");
        statement.execute("PRAGMA mmap_size=0");
        statement.execute("PRAGMA cache_size=-1024");
        Map<String, String> schema = schema(connection);
        if (!schema.isEmpty() && !schema.equals(SCHEMA)) throw new SQLException("foreign producer database schema; conversion refused");
        if (!schema.isEmpty()) {
          policy = readPolicy(connection);
          verifyPolicy(policy, peer, limits);
          try (var rows = statement.executeQuery("PRAGMA journal_mode")) {
            if (!rows.next() || !"delete".equals(rows.getString(1))) throw new SQLException("producer journal mode changed");
          }
        } else {
          for (String pragma : new String[]{"user_version", "application_id"}) {
            try (var rows = statement.executeQuery("PRAGMA " + pragma)) {
              if (!rows.next() || rows.getLong(1) != 0) throw new SQLException("foreign empty database; conversion refused");
            }
          }
          policy = policy(peer, limits);
        }
        // No WAL reader can retain completion pages. The cooperating writer keeps this
        // SQLite connection and exclusive database lock until close, using DELETE journals.
        try (var rows = statement.executeQuery("PRAGMA locking_mode=EXCLUSIVE")) {
          if (!rows.next() || !"exclusive".equals(rows.getString(1))) throw new SQLException("producer database lock mode refused");
        }
        if (schema.isEmpty()) {
          try (var rows = statement.executeQuery("PRAGMA journal_mode=DELETE")) {
            if (!rows.next() || !"delete".equals(rows.getString(1))) throw new SQLException("producer journal mode refused");
          }
        }
        statement.execute("BEGIN EXCLUSIVE");
        try {
          // Recheck after taking the SQLite lock; never initialize a concurrently created store.
          if (!schema(connection).equals(schema)) throw new SQLException("producer database schema changed during open");
          if (schema.isEmpty()) {
            statement.execute(META); statement.execute(OPERATIONS);
            try (var insert = connection.prepareStatement("INSERT INTO ps_producer_meta VALUES(1,?,?)")) {
              insert.setBytes(1, policy); insert.setBytes(2, head(policy, new Usage(0, 0))); insert.executeUpdate();
            }
          } else if (!Arrays.equals(readPolicy(connection), policy)) throw new SQLException("producer binding changed during open");
          statement.execute("COMMIT");
        } catch (SQLException failure) { rollback(connection, failure); throw failure; }
      }
      var result = new SealedProducerJournal(files, connection, channel, lock, policy, limits);
      result.audit();
      return result;
    } catch (IOException | SQLException | ProtocolException | RuntimeException failure) {
      if (connection != null) try { connection.close(); } catch (SQLException close) { failure.addSuppressed(close); }
      if (lock != null) try { lock.release(); } catch (IOException close) { failure.addSuppressed(close); }
      if (channel != null) try { channel.close(); } catch (IOException close) { failure.addSuppressed(close); }
      OWNERS.remove(lockPath);
      throw failure;
    }
  }

  /**
   * Allocates the entire observation slot before the caller can send this intent.
   * @param kind operation discriminator
   * @param identity bounded operation identity
   * @param request original immutable request bytes
   * @param capacity bytes reserved for normalized observation evidence
   * @return new unobserved intent, or the identical retained request
   * @throws SQLException for database failure
   * @throws ProtocolException for changed identity, capacity exhaustion or corrupt evidence
   * @throws IOException for closed ownership or invalid files
   */
  synchronized Entry begin(Kind kind, byte[] identity, byte[] request, int capacity)
      throws SQLException, ProtocolException, IOException {
    checkOpen();
    if (kind == null || identity == null || identity.length < 1 || identity.length > 256
        || request == null || request.length < 1 || request.length > MAX_REQUEST_BYTES
        || capacity < 1 || capacity > MAX_OBSERVATION_BYTES) throw Wire.limit("producer request or observation exceeds local bound");
    byte[] key = identity.clone(), original = request.clone();
    return transaction(() -> {
      Entry previous;
      try (var query = connection.prepareStatement("SELECT " + RECORD_COLUMNS + " FROM ps_producer_operations WHERE kind=? AND identity=?")) {
        query.setInt(1, kind.ordinal()); query.setBytes(2, key);
        try (var rows = query.executeQuery()) { previous = rows.next() ? entry(rows) : null; }
      }
      if (previous != null) {
        if (!Arrays.equals(previous.request(), original) || previous.capacity() != capacity) throw Wire.entity("producer intent identity reused with changed fields");
        return previous;
      }
      Usage usage = usageInside();
      long charge = key.length + (long) original.length + capacity + IMAGE_OVERHEAD;
      if (usage.operations() >= limits.operations() || charge > limits.bytes() - usage.bytes()) throw Wire.limit("producer journal reservation exhausted");
      long id = usage.operations() + 1;
      byte[] image = image(id, kind, key, original, capacity, 0, new byte[0], false);
      try (var insert = connection.prepareStatement("INSERT INTO ps_producer_operations VALUES(?,?,?,?,?)")) {
        insert.setLong(1, id); insert.setInt(2, kind.ordinal()); insert.setBytes(3, key);
        insert.setBytes(4, original); insert.setBytes(5, image); insert.executeUpdate();
      }
      SealedSqliteImages.replace(connection, "ps_producer_meta", "head", 1,
          head(policy, new Usage(id, usage.bytes() + charge)));
      return new Entry(id, kind, key, original, capacity, 0, new byte[0], false);
    });
  }

  /**
   * Persists only caller-validated evidence; an intent alone remains unresolved.
   * @param id existing intent position
   * @param expectedRevision previously observed revision, checked on mutable records
   * @param observation normalized and semantically validated response evidence
   * @param resolved whether the final response for this operation was verified
   * @return committed observation; an identical final observation is idempotent
   * @throws SQLException for database failure
   * @throws ProtocolException for missing intent, stale or changed evidence, or exhausted capacity
   * @throws IOException for closed ownership or invalid files
   */
  synchronized Entry observe(long id, long expectedRevision, byte[] observation, boolean resolved)
      throws SQLException, ProtocolException, IOException {
    checkOpen();
    if (observation == null || observation.length < 1 || observation.length > MAX_OBSERVATION_BYTES) throw Wire.limit("invalid producer observation length");
    byte[] value = observation.clone();
    return transaction(() -> {
      Entry previous = getInside(id);
      if (previous == null) throw Wire.entity("observation has no durable producer intent");
      if (value.length > previous.capacity()) throw Wire.limit("producer observation exceeds its reservation");
      if (previous.resolved()) {
        if (!resolved || !Arrays.equals(value, previous.observation())) throw Wire.entity("resolved producer observation cannot change");
        return previous;
      }
      if (previous.revision() != expectedRevision || expectedRevision == Long.MAX_VALUE) throw Wire.entity("stale producer observation revision");
      long revision = expectedRevision + 1;
      byte[] image = image(id, previous.kind(), previous.identity(), previous.request(), previous.capacity(), revision, value, resolved);
      SealedSqliteImages.replace(connection, "ps_producer_operations", "image", id, image);
      return new Entry(id, previous.kind(), previous.identity(), previous.request(), previous.capacity(), revision, value, resolved);
    });
  }

  /**
   * Reads a single bounded record without loading the journal as one image.
   * @param afterId exclusive local append position, zero to start
   * @return next audited record, or null at the current end
   * @throws SQLException for database failure
   * @throws ProtocolException for corrupt retained evidence
   * @throws IOException for closed ownership or invalid files
   */
  synchronized Entry next(long afterId) throws SQLException, ProtocolException, IOException {
    checkOpen();
    if (afterId < 0) throw new IllegalArgumentException("negative producer cursor");
    try (var query = connection.prepareStatement("SELECT " + RECORD_COLUMNS + " FROM ps_producer_operations WHERE id>? ORDER BY id LIMIT 1")) {
      query.setLong(1, afterId);
      try (var rows = query.executeQuery()) { return rows.next() ? entry(rows) : null; }
    }
  }

  /**
   * Reads the protected append frontier and charged byte count.
   * @return logical reservations, not filesystem blocks
   * @throws SQLException for database or frontier-integrity failure
   * @throws IOException for closed ownership or invalid files
   */
  synchronized Usage usage() throws SQLException, IOException {
    checkOpen(); return usageInside();
  }

  /**
   * Samples the guarded database and sidecar lengths.
   * @return current file lengths
   * @throws IOException for closed ownership or invalid files
   * @throws SQLException for changed journal binding
   */
  synchronized SealedSessionStore.FileUsage fileUsage() throws IOException, SQLException {
    checkOpen(); return files.usage();
  }

  private Entry getInside(long id) throws SQLException, ProtocolException {
    try (var query = connection.prepareStatement("SELECT " + RECORD_COLUMNS + " FROM ps_producer_operations WHERE id=?")) {
      query.setLong(1, id);
      try (var rows = query.executeQuery()) { return rows.next() ? entry(rows) : null; }
    }
  }

  private Usage usageInside() throws SQLException {
    try (var query = connection.createStatement(); var rows = query.executeQuery("SELECT length(head),head FROM ps_producer_meta WHERE singleton=1")) {
      if (!rows.next()) throw new SQLException("missing producer append frontier");
      if (rows.getLong(1) != 56) throw new SQLException("invalid producer append frontier length");
      byte[] image = rows.getBytes(2);
      if (image == null || image.length != 56 || !Arrays.equals(Arrays.copyOf(image, 8), HEAD_MAGIC)) throw new SQLException("invalid producer append frontier");
      ByteBuffer fields = ByteBuffer.wrap(image); fields.position(8);
      Usage usage = new Usage(fields.getLong(), fields.getLong());
      if (usage.operations() < 0 || usage.operations() > limits.operations() || usage.bytes() < 0 || usage.bytes() > limits.bytes()
          || !MessageDigest.isEqual(image, head(policy, usage))) throw new SQLException("producer append frontier checksum or policy mismatch");
      return usage;
    }
  }

  private Usage actualUsage() throws SQLException {
    try (var query = connection.createStatement(); var rows = query.executeQuery(
        "SELECT count(*),coalesce(sum(length(identity)+length(request)+length(image)),0) FROM ps_producer_operations")) {
      if (!rows.next()) throw new SQLException("missing producer accounting");
      return new Usage(rows.getLong(1), rows.getLong(2));
    }
  }

  private void audit() throws SQLException, ProtocolException, IOException {
    try (var query = connection.createStatement(); var rows = query.executeQuery("PRAGMA quick_check")) {
      if (!rows.next() || !"ok".equals(rows.getString(1)) || rows.next()) throw new SQLException("producer SQLite integrity check failed");
    }
    Usage usage = usageInside();
    if (!usage.equals(actualUsage())) throw Wire.integrity("producer journal differs from retained append frontier");
    long cursor = 0;
    for (Entry entry; (entry = next(cursor)) != null; cursor = entry.id()) {
      if (entry.id() != cursor + 1) throw Wire.integrity("producer journal intent sequence has a gap");
    }
    if (cursor != usage.operations()) throw Wire.integrity("producer journal count differs");
  }

  private Entry entry(java.sql.ResultSet rows) throws SQLException, ProtocolException {
    long id = rows.getLong(1); int ordinal = rows.getInt(2);
    // Check retained geometry before JDBC allocates Java arrays, including when
    // an external tool has bypassed the on-disk CHECK constraints.
    long keyBytes = rows.getLong(3), requestBytes = rows.getLong(4), imageBytes = rows.getLong(5);
    if (keyBytes < 1 || keyBytes > 256 || requestBytes < 1 || requestBytes > MAX_REQUEST_BYTES
        || imageBytes <= IMAGE_OVERHEAD || imageBytes > IMAGE_OVERHEAD + MAX_OBSERVATION_BYTES) throw Wire.integrity("invalid producer record lengths");
    byte[] key = rows.getBytes(6), request = rows.getBytes(7), image = rows.getBytes(8);
    if (id < 1 || id > limits.operations() || ordinal < 0 || ordinal >= Kind.values().length
        || key == null || key.length < 1 || key.length > 256 || request == null || request.length < 1
        || request.length > MAX_REQUEST_BYTES || image == null || image.length <= IMAGE_OVERHEAD
        || image.length > IMAGE_OVERHEAD + MAX_OBSERVATION_BYTES) throw Wire.integrity("invalid producer record geometry");
    Kind kind = Kind.values()[ordinal];
    if (!Arrays.equals(Arrays.copyOf(image, 8), IMAGE_MAGIC)
        || !MessageDigest.isEqual(checksum(id, kind, key, request, image), Arrays.copyOfRange(image, image.length - 32, image.length))) {
      throw Wire.integrity("producer observation binding or checksum mismatch");
    }
    ByteBuffer fields = ByteBuffer.wrap(image); fields.position(8);
    long revision = fields.getLong(); int length = fields.getInt(), done = fields.getInt();
    int capacity = image.length - IMAGE_OVERHEAD;
    if (revision < 0 || length < 0 || length > capacity || done < 0 || done > 1
        || (revision == 0) != (length == 0) || (done == 1 && revision == 0)) throw Wire.integrity("invalid producer observation state");
    for (int i = 24 + length; i < image.length - 32; i++) if (image[i] != 0) throw Wire.integrity("nonzero producer observation padding");
    return new Entry(id, kind, key, request, capacity, revision, Arrays.copyOfRange(image, 24, 24 + length), done == 1);
  }

  private byte[] image(long id, Kind kind, byte[] key, byte[] request, int capacity,
      long revision, byte[] observation, boolean resolved) {
    byte[] image = new byte[IMAGE_OVERHEAD + capacity];
    ByteBuffer.wrap(image).put(IMAGE_MAGIC).putLong(revision).putInt(observation.length)
        .putInt(resolved ? 1 : 0).put(observation);
    System.arraycopy(checksum(id, kind, key, request, image), 0, image, image.length - 32, 32);
    return image;
  }

  private byte[] checksum(long id, Kind kind, byte[] key, byte[] request, byte[] image) {
    var hash = SealedWork.sha256(); hash.update(policy);
    hash.update(ByteBuffer.allocate(20).putLong(id).putInt(kind.ordinal()).putInt(key.length).putInt(request.length).array());
    hash.update(key); hash.update(request); hash.update(image, 0, image.length - 32);
    return hash.digest();
  }

  private <T> T transaction(Operation<T> operation) throws SQLException, ProtocolException {
    try (var statement = connection.createStatement()) {
      statement.execute("BEGIN EXCLUSIVE");
      try {
        T result = operation.run(); statement.execute("COMMIT"); return result;
      } catch (SQLException | ProtocolException | RuntimeException failure) {
        rollback(connection, failure);
        if (failure instanceof SQLException sql && SealedSqliteFiles.isFull(sql)) throw Wire.limit("producer database file reservation exhausted");
        throw failure;
      }
    }
  }

  private void checkOpen() throws IOException, SQLException {
    if (closed || !lock.isValid()) throw new IOException("producer journal is closed or has lost ownership");
    files.usage();
    if (!Arrays.equals(policy, readPolicy(connection))) throw new SQLException("producer journal binding changed");
  }

  @Override public synchronized void close() throws SQLException, IOException {
    if (closed) return;
    closed = true;
    try { connection.close(); }
    finally {
      try { lock.release(); }
      finally {
        try { lockChannel.close(); }
        finally { OWNERS.remove(files.path().resolveSibling(files.path().getFileName() + ".producerlock")); }
      }
    }
  }

  private static Map<String, String> schema(Connection connection) throws SQLException {
    Map<String, String> schema = new TreeMap<>();
    try (var query = connection.createStatement(); var rows = query.executeQuery("SELECT length(name),length(sql),name,sql FROM sqlite_schema WHERE sql IS NOT NULL")) {
      while (rows.next()) {
        if (schema.size() >= SCHEMA.size()) throw new SQLException("foreign producer database objects; conversion refused");
        if (rows.getLong(1) > 64 || rows.getLong(2) > 2048) throw new SQLException("producer schema object exceeds supported bounds");
        schema.put(rows.getString(3), rows.getString(4));
      }
    }
    return schema;
  }

  private static byte[] readPolicy(Connection connection) throws SQLException {
    try (var query = connection.createStatement(); var rows = query.executeQuery("SELECT length(policy),policy FROM ps_producer_meta WHERE singleton=1")) {
      if (!rows.next()) throw new SQLException("missing producer journal policy");
      if (rows.getLong(1) != 100) throw new SQLException("invalid producer policy length");
      byte[] result = rows.getBytes(2);
      if (rows.next()) throw new SQLException("duplicate producer journal policy");
      return result;
    }
  }

  private static byte[] policy(byte[] peer, Limits limits) {
    byte[] result = new byte[100]; UUID identity = UUID.randomUUID();
    ByteBuffer.wrap(result).put(POLICY_MAGIC).putLong(identity.getMostSignificantBits()).putLong(identity.getLeastSignificantBits())
        .put(peer).putInt(limits.operations()).putLong(limits.bytes());
    System.arraycopy(SealedWork.sha256().digest(Arrays.copyOf(result, 68)), 0, result, 68, 32);
    return result;
  }

  private static byte[] head(byte[] policy, Usage usage) {
    byte[] image = new byte[56];
    ByteBuffer.wrap(image).put(HEAD_MAGIC).putLong(usage.operations()).putLong(usage.bytes());
    var hash = SealedWork.sha256(); hash.update(policy); hash.update(image, 0, 24);
    System.arraycopy(hash.digest(), 0, image, 24, 32);
    return image;
  }

  private static void verifyPolicy(byte[] policy, byte[] peer, Limits limits) throws SQLException {
    if (policy == null || policy.length != 100 || !Arrays.equals(Arrays.copyOf(policy, 8), POLICY_MAGIC)
        || !MessageDigest.isEqual(SealedWork.sha256().digest(Arrays.copyOf(policy, 68)), Arrays.copyOfRange(policy, 68, 100))) {
      throw new SQLException("unsupported or corrupt producer policy; conversion refused");
    }
    ByteBuffer fields = ByteBuffer.wrap(policy); fields.position(56);
    if (!Arrays.equals(Arrays.copyOfRange(policy, 24, 56), peer)
        || fields.getInt() != limits.operations() || fields.getLong() != limits.bytes()) {
      throw new SQLException("producer peer binding or retained limits differ");
    }
  }

  private static void verifyLock(Path path) throws IOException {
    var attributes = Files.readAttributes(path, "unix:isRegularFile,nlink,size", LinkOption.NOFOLLOW_LINKS);
    if (!Boolean.TRUE.equals(attributes.get("isRegularFile")) || ((Number) attributes.get("nlink")).longValue() != 1
        || ((Number) attributes.get("size")).longValue() != 0) throw new IOException("producer lock is not an empty private regular file");
  }

  private static void rollback(Connection connection, Exception failure) {
    try (var statement = connection.createStatement()) { statement.execute("ROLLBACK"); }
    catch (SQLException rollback) { failure.addSuppressed(rollback); }
  }

  @FunctionalInterface private interface Operation<T> { T run() throws SQLException, ProtocolException; }
}
