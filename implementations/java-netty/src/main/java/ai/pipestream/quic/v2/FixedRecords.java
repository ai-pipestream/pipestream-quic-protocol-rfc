package ai.pipestream.quic.v2;

import ai.pipestream.quic.BoundedSqlite;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;

/**
 * Java V2 fixed-capacity records and persistent rewrite credits. One credit funds this image and
 * one shared-clock image in the same transaction, not arbitrary SQL or a complete job. All
 * mutations require the caller's writer transaction.
 */
final class FixedRecords {
  /** Bytes in each fixed-record header. */
  static final int HEADER = 128;

  /** Largest permitted fixed-record body capacity. */
  static final int MAX_CAPACITY = Wire.MAX_CONTROL_LIMIT;

  /** Capacity of the shared-clock record body. */
  static final int CLOCK_CAPACITY = 64;

  /** Capacity of a retained scope-state body. */
  static final int SCOPE_CAPACITY = 1024;

  /** Capacity of a retained work-view body. */
  static final int WORK_CAPACITY = 2048;

  /** Capacity of a retained fence body. */
  static final int FENCE_CAPACITY = 256;

  /** Capacity of one restartable job, including lease and reclamation progress. */
  static final int JOB_CAPACITY = 2048;

  /** Expansion/settlement plus input and output reclamation intent/completion writes. */
  static final long JOB_CREDITS = 6;

  /** Rewrite credits reserved for a scope image. */
  static final long SCOPE_CREDITS = 4;

  /** Rewrite credits reserved for a work image. */
  static final long WORK_CREDITS = 2;

  /** Work-image settlement credits funded when admitting or explicitly replacing an attempt. */
  static final long ADMITTED_WORK_CREDITS = 4;

  /** Rewrite credits reserved for a fence image. */
  static final long FENCE_CREDITS = 1;

  /** Slot identifier of the shared clock record. */
  static final long CLOCK = 1;

  private static final byte[] MAGIC = "PSJV2R03".getBytes(StandardCharsets.US_ASCII);

  /** Local record roles, never wire profile identifiers. */
  enum Kind {
    /** Shared clock record. */
    CLOCK,
    /** Scope-state record. */
    SCOPE,
    /** Work-view record. */
    WORK,
    /** Entity fence record. */
    FENCE,
    /** Restartable execution and storage-retention record. */
    JOB
  }

  /**
   * Parsed, checksummed local geometry; arrays remain internal to the store.
   *
   * @param revision local image revision
   * @param credits remaining rewrite credits
   * @param used encoded body bytes in use
   * @param capacity allocated body capacity
   * @param key immutable ownership key
   * @param digest body checksum
   */
  record Header(long revision, long credits, int used, int capacity, byte[] key, byte[] digest) {}

  /**
   * One bounded record, not a whole session snapshot.
   *
   * @param header verified image geometry
   * @param body checksummed encoded body
   */
  record Snapshot(Header header, byte[] body) {}

  /**
   * Retained write promises and the separately protected clock counter.
   *
   * @param bytes WAL bytes reserved for retained promises
   * @param clockCredits credits held by non-clock records
   * @param clockRevision shared-clock revision
   */
  record Forecast(long bytes, long clockCredits, long clockRevision) {
    /**
     * Reject an additional credit reservation that would overflow the protected clock sequence.
     *
     * @param revision proposed clock revision
     * @param additional additional non-clock credits
     */
    void room(long revision, long additional) {
      if (additional < 0
          || clockCredits > Long.MAX_VALUE - additional
          || revision < 1
          || revision > Long.MAX_VALUE - clockCredits - additional) throw exhausted();
    }
  }

  private FixedRecords() {}

  /**
   * Create the private slot table; initialization has not yet promised any work.
   *
   * @param connection writer connection
   * @param authority authority name used in the shared-clock key
   * @throws SQLException if SQLite rejects the schema or clock initialization
   */
  static void createSchema(Connection connection, String authority) throws SQLException {
    try (var statement = connection.createStatement()) {
      statement.execute(
          """
          CREATE TABLE ps_v2_slots (
            id INTEGER PRIMARY KEY CHECK(id>0),
            kind INTEGER NOT NULL CHECK(kind BETWEEN 0 AND 4),
            image BLOB NOT NULL CHECK(length(image) BETWEEN 129 AND 1048704)
          ) STRICT
          """);
      statement.execute("INSERT INTO ps_v2_slots(id,kind,image) VALUES(1,0,zeroblob(192))");
    }
    write(connection, CLOCK, Kind.CLOCK, clockKey(authority), new byte[] {0}, 1, CLOCK_CAPACITY, 0);
  }

  /**
   * Hash a local record's immutable protocol identity without a generic object tree.
   *
   * @param binding session binding
   * @param kind local record role
   * @param scope scope identifier
   * @param producer producer identifier
   * @param entity entity identifier
   * @param declaration original declaration operation ID, or {@code null}
   * @return immutable 32-byte ownership key
   */
  static byte[] key(
      Messages.Binding binding,
      Kind kind,
      long scope,
      int producer,
      long entity,
      byte[] declaration) {
    return key(
        new Commitments.Context(binding.authority(), binding.owner(), binding.generation()),
        kind,
        scope,
        producer,
        entity,
        declaration);
  }

  /**
   * Hash a checked local accounting row's immutable identity without inventing a session receipt.
   *
   * @param context owner-qualified context from retained metadata
   * @param kind record role
   * @param scope scope identity
   * @param producer producer identity
   * @param entity entity identity
   * @param declaration originating operation, or null
   * @return ownership commitment
   */
  static byte[] key(
      Commitments.Context context,
      Kind kind,
      long scope,
      int producer,
      long entity,
      byte[] declaration) {
    var digest = Commitments.sha256();
    Cbor.Writer out = new Cbor.Writer(digest);
    out.array(9);
    out.text("pipestream-java-v2-slot-key", 128);
    out.text(context.authority(), 128);
    out.text(context.owner(), 128);
    out.number(context.generation());
    out.number(kind.ordinal());
    out.number(scope);
    out.number(producer);
    out.number(entity);
    if (declaration == null) out.nil();
    else out.bytes(declaration);
    return digest.digest();
  }

  /**
   * Local shared-clock identity, separate from any owner's session.
   *
   * @param authority authority name
   * @return immutable 32-byte shared-clock key
   */
  static byte[] clockKey(String authority) {
    var digest = Commitments.sha256();
    Cbor.Writer out = new Cbor.Writer(digest);
    out.array(2);
    out.text("pipestream-java-v2-clock", 128);
    out.text(authority, 128);
    return digest.digest();
  }

  private static byte[] headerHash(long id, Kind kind, byte[] prefix) {
    var digest = Commitments.sha256();
    digest.update("pipestream-java-v2-fixed-record".getBytes(StandardCharsets.US_ASCII));
    digest.update(ByteBuffer.allocate(12).putLong(id).putInt(kind.ordinal()).array());
    digest.update(prefix, 0, 96);
    return digest.digest();
  }

  private static Header parse(long id, int kind, long length, byte[] prefix) throws SQLException {
    if (id < 1
        || kind < 0
        || kind >= Kind.values().length
        || prefix == null
        || prefix.length != HEADER
        || !Arrays.equals(Arrays.copyOf(prefix, 8), MAGIC))
      throw corrupt("invalid fixed-record header");
    ByteBuffer in = ByteBuffer.wrap(prefix);
    long revision = in.getLong(8), credits = in.getLong(16);
    int used = in.getInt(24), capacity = in.getInt(28);
    if (revision < 1
        || credits < 0
        || revision > Long.MAX_VALUE - credits
        || used < 1
        || capacity < used
        || capacity > MAX_CAPACITY
        || length != (long) HEADER + capacity
        || !Arrays.equals(
            Arrays.copyOfRange(prefix, 96, HEADER), headerHash(id, Kind.values()[kind], prefix)))
      throw corrupt("fixed-record geometry or header checksum differs");
    return new Header(
        revision,
        credits,
        used,
        capacity,
        Arrays.copyOfRange(prefix, 32, 64),
        Arrays.copyOfRange(prefix, 64, 96));
  }

  /**
   * Read bounded geometry before allocating the body.
   *
   * @param connection reader connection
   * @param id slot identifier
   * @param kind required local role
   * @return validated fixed-record header
   * @throws SQLException if the slot is absent, malformed, or has the wrong role
   */
  static Header header(Connection connection, long id, Kind kind) throws SQLException {
    try (var query =
        connection.prepareStatement(
            "SELECT kind,length(image),substr(image,1,128) FROM ps_v2_slots WHERE id=?")) {
      query.setLong(1, id);
      try (var row = query.executeQuery()) {
        if (!row.next() || row.getInt(1) != kind.ordinal())
          throw corrupt("missing or wrong-role fixed record");
        return parse(id, row.getInt(1), row.getLong(2), row.getBytes(3));
      }
    }
  }

  /**
   * Read and verify one image; the expected immutable key is supplied by its owning row.
   *
   * @param connection reader connection
   * @param id slot identifier
   * @param kind required local role
   * @param key expected immutable ownership key
   * @return verified image snapshot
   * @throws SQLException if the image or its ownership/checksum validation fails
   */
  static Snapshot read(Connection connection, long id, Kind kind, byte[] key) throws SQLException {
    Header header = header(connection, id, kind);
    if (key == null || key.length != 32 || !Arrays.equals(header.key(), key))
      throw corrupt("fixed record belongs to another identity");
    try (var query =
        connection.prepareStatement(
            "SELECT CASE WHEN length(image)=? THEN image END FROM ps_v2_slots WHERE id=?")) {
      query.setInt(1, HEADER + header.capacity());
      query.setLong(2, id);
      try (var row = query.executeQuery()) {
        if (!row.next()) throw corrupt("fixed record disappeared");
        byte[] image = row.getBytes(1);
        if (image == null || image.length != HEADER + header.capacity())
          throw corrupt("fixed record resized");
        Header current = parse(id, kind.ordinal(), image.length, Arrays.copyOf(image, HEADER));
        if (current.revision() != header.revision() || !Arrays.equals(current.key(), key))
          throw corrupt("fixed record changed outside its transaction");
        byte[] body = Arrays.copyOfRange(image, HEADER, HEADER + current.used());
        if (!Arrays.equals(Commitments.sha256().digest(body), current.digest()))
          throw corrupt("fixed-record body checksum differs");
        for (int index = HEADER + current.used(); index < image.length; index++)
          if (image[index] != 0) throw corrupt("fixed-record padding is not zero");
        return new Snapshot(current, body);
      }
    }
  }

  private static void validate(byte[] key, byte[] body, long revision, int capacity, long credits) {
    if (key == null
        || key.length != 32
        || body == null
        || body.length < 1
        || capacity < body.length
        || capacity > MAX_CAPACITY
        || credits < 0
        || revision < 1
        || revision > Long.MAX_VALUE - credits) throw exhausted();
  }

  private static void write(
      Connection connection,
      long id,
      Kind kind,
      byte[] key,
      byte[] body,
      long revision,
      int capacity,
      long credits)
      throws SQLException {
    validate(key, body, revision, capacity, credits);
    byte[] image = new byte[HEADER + capacity];
    ByteBuffer.wrap(image)
        .put(MAGIC)
        .putLong(revision)
        .putLong(credits)
        .putInt(body.length)
        .putInt(capacity)
        .put(key)
        .put(Commitments.sha256().digest(body));
    System.arraycopy(headerHash(id, kind, image), 0, image, 96, 32);
    System.arraycopy(body, 0, image, HEADER, body.length);
    BoundedSqlite.replaceImage(connection, "ps_v2_slots", "image", id, image);
  }

  /**
   * Allocate capacity and its future writes before returning a newly owned slot.
   *
   * @param connection writer connection
   * @param limits bounded SQLite file policy
   * @param kind local record role
   * @param key immutable ownership key
   * @param body initial encoded body
   * @param capacity allocated body capacity
   * @param credits reserved future rewrites
   * @return newly allocated slot identifier
   * @throws SQLException if allocation or its bounded-write reservation fails
   */
  static long allocate(
      Connection connection,
      BoundedSqlite.Limits limits,
      Kind kind,
      byte[] key,
      byte[] body,
      int capacity,
      long credits)
      throws SQLException {
    if (kind == Kind.CLOCK) throw corrupt("shared clock cannot be allocated twice");
    validate(key, body, 1, capacity, credits);
    long page = geometry(connection);
    Forecast retained = forecast(connection, page, 0, 0, 0);
    retained.room(retained.clockRevision(), credits);
    install(
        connection, limits, page, add(retained.bytes(), multiply(credits, cost(capacity, page))));
    try (var insert =
        connection.prepareStatement("INSERT INTO ps_v2_slots(kind,image) VALUES(?,zeroblob(?))")) {
      insert.setInt(1, kind.ordinal());
      insert.setInt(2, HEADER + capacity);
      insert.executeUpdate();
    }
    long id;
    try (var query = connection.createStatement();
        var row = query.executeQuery("SELECT last_insert_rowid()")) {
      if (!row.next() || (id = row.getLong(1)) < 1) throw corrupt("fixed-record allocation failed");
    }
    write(connection, id, kind, key, body, 1, capacity, credits);
    return id;
  }

  /**
   * Preserve all retained promises before an ordinary SQL mutation.
   *
   * @param connection writer connection
   * @param limits bounded SQLite file policy
   * @throws SQLException if retained promises cannot be protected
   */
  static void protect(Connection connection, BoundedSqlite.Limits limits) throws SQLException {
    long page = geometry(connection);
    Forecast retained = forecast(connection, page, 0, 0, 0);
    retained.room(retained.clockRevision(), 0);
    install(connection, limits, page, retained.bytes());
  }

  /**
   * Rewrite one image atomically with its revision and remaining credits.
   *
   * @param connection writer connection
   * @param limits bounded SQLite file policy
   * @param id slot identifier
   * @param kind local record role
   * @param key immutable ownership key
   * @param expected required current revision
   * @param body replacement encoded body
   * @param spend whether to consume one image rewrite credit
   * @return new image revision
   * @throws SQLException if validation, conflict detection, or write reservation fails
   */
  static long replace(
      Connection connection,
      BoundedSqlite.Limits limits,
      long id,
      Kind kind,
      byte[] key,
      long expected,
      byte[] body,
      boolean spend)
      throws SQLException {
    Snapshot original = read(connection, id, kind, key);
    Header header = original.header();
    if (header.revision() != expected)
      throw new ProtocolError(ProtocolError.Code.CONFLICT, "fixed-record revision changed");
    long credits = header.credits();
    if (spend) {
      if (credits == 0 || kind == Kind.CLOCK) throw exhausted();
      credits--;
    }
    if (expected == Long.MAX_VALUE) throw exhausted();
    long revision = expected + 1;
    validate(key, body, revision, header.capacity(), credits);
    long page = geometry(connection);
    Forecast next = forecast(connection, page, id, header.capacity(), credits);
    next.room(kind == Kind.CLOCK ? revision : next.clockRevision(), 0);
    install(connection, limits, page, next.bytes());
    write(connection, id, kind, key, body, revision, header.capacity(), credits);
    return revision;
  }

  /**
   * Grow a promised image without changing its observable body or revision.
   *
   * @param connection writer connection
   * @param limits bounded SQLite file policy
   * @param id slot identifier
   * @param kind local record role
   * @param key immutable ownership key
   * @param expected required current revision
   * @param capacity replacement body capacity
   * @param credits replacement rewrite-credit count
   * @throws SQLException if validation, conflict detection, or write reservation fails
   */
  static void grow(
      Connection connection,
      BoundedSqlite.Limits limits,
      long id,
      Kind kind,
      byte[] key,
      long expected,
      int capacity,
      long credits)
      throws SQLException {
    Snapshot original = read(connection, id, kind, key);
    Header header = original.header();
    if (header.revision() != expected)
      throw new ProtocolError(ProtocolError.Code.CONFLICT, "fixed-record revision changed");
    if (kind == Kind.CLOCK || capacity < header.capacity() || credits < header.credits())
      throw exhausted();
    validate(key, original.body(), expected, capacity, credits);
    long page = geometry(connection);
    Forecast next = forecast(connection, page, id, capacity, credits);
    next.room(next.clockRevision(), 0);
    install(connection, limits, page, next.bytes());
    if (capacity == header.capacity() && credits == header.credits()) return;
    try (var query =
        connection.prepareStatement("UPDATE ps_v2_slots SET image=zeroblob(?) WHERE id=?")) {
      query.setInt(1, HEADER + capacity);
      query.setLong(2, id);
      if (query.executeUpdate() != 1) throw corrupt("fixed record disappeared during growth");
    }
    write(connection, id, kind, key, original.body(), expected, capacity, credits);
  }

  /**
   * Stream checksummed headers to price retained promises, with one prospective replacement.
   *
   * @param connection reader connection
   * @param page SQLite page size
   * @param replacement replacement slot, or zero for none
   * @param capacity prospective replacement capacity
   * @param credits prospective replacement credits
   * @return retained WAL forecast
   * @throws SQLException if a retained header is malformed or missing
   */
  static Forecast forecast(
      Connection connection, long page, long replacement, int capacity, long credits)
      throws SQLException {
    if (replacement < 0
        || credits < 0
        || (replacement != 0 && (capacity < 1 || capacity > MAX_CAPACITY))) throw exhausted();
    long total = 0, clockCredits = 0, clockRevision = 0;
    boolean found = replacement == 0;
    try (var query = connection.createStatement();
        var rows =
            query.executeQuery(
                "SELECT id,kind,length(image),substr(image,1,128) FROM ps_v2_slots ORDER BY id")) {
      while (rows.next()) {
        long id = rows.getLong(1);
        int kind = rows.getInt(2);
        Header header = parse(id, kind, rows.getLong(3), rows.getBytes(4));
        if (kind == Kind.CLOCK.ordinal()) {
          if (id != CLOCK || header.capacity() != CLOCK_CAPACITY || header.credits() != 0)
            throw corrupt("shared clock geometry differs");
          clockRevision = header.revision();
        }
        int pricedCapacity = header.capacity();
        long pricedCredits = header.credits();
        if (id == replacement) {
          found = true;
          pricedCapacity = capacity;
          pricedCredits = credits;
        }
        clockCredits = add(clockCredits, pricedCredits);
        total = add(total, multiply(pricedCredits, cost(pricedCapacity, page)));
      }
    }
    if (!found || clockRevision == 0) throw corrupt("missing funded record or shared clock");
    return new Forecast(total, clockCredits, clockRevision);
  }

  /**
   * Audit full images and exact owner references during recovery, without building a map.
   *
   * @param connection reader connection
   * @param limits bounded SQLite file policy
   * @param authority authority name used to validate the shared clock
   * @throws SQLException if image, ownership, or retained-write validation fails
   */
  static void audit(Connection connection, BoundedSqlite.Limits limits, String authority)
      throws SQLException {
    try (var query = connection.createStatement();
        var row = query.executeQuery("PRAGMA auto_vacuum")) {
      if (!row.next() || row.getInt(1) != 0)
        throw corrupt("retained write promises require no auto-vacuum");
    }
    try (var query = connection.createStatement();
        var rows = query.executeQuery("SELECT id,kind FROM ps_v2_slots ORDER BY id")) {
      while (rows.next()) {
        long id = rows.getLong(1);
        int code = rows.getInt(2);
        if (code < 0 || code >= Kind.values().length) throw corrupt("unknown fixed-record role");
        Kind kind = Kind.values()[code];
        Header header = header(connection, id, kind);
        Snapshot image =
            read(connection, id, kind, kind == Kind.CLOCK ? clockKey(authority) : header.key());
        if (kind == Kind.CLOCK) {
          try {
            Cbor.Reader value = new Cbor.Reader(image.body(), CLOCK_CAPACITY);
            value.number();
            value.end();
          } catch (ProtocolError invalid) {
            throw new SQLException("V2 fixed records: invalid shared clock value", invalid);
          }
        }
        String reference =
            switch (kind) {
              case CLOCK -> "SELECT count(*) FROM ps_v2_meta WHERE clock_slot=?";
              case SCOPE -> "SELECT count(*) FROM ps_v2_scopes WHERE state_slot=?";
              case WORK -> "SELECT count(*) FROM ps_v2_entities WHERE view_slot=?";
              case FENCE -> "SELECT count(*) FROM ps_v2_entities WHERE fence_slot=?";
              case JOB -> "SELECT count(*) FROM ps_v2_jobs WHERE state_slot=?";
            };
        try (var owner = connection.prepareStatement(reference)) {
          owner.setLong(1, id);
          try (var count = owner.executeQuery()) {
            if (!count.next() || count.getLong(1) != 1)
              throw corrupt("orphaned or multiply owned fixed record");
          }
        }
      }
    }
    long page = pageSize(connection);
    Forecast retained = forecast(connection, page, 0, 0, 0);
    retained.room(retained.clockRevision(), 0);
    if (retained.bytes() > usable(limits, page) - 32)
      throw corrupt("retained write promises exceed file policy");
  }

  /**
   * SQLite geometry required by the pinned native WAL cost model.
   *
   * @param connection database connection
   * @return validated SQLite page size
   * @throws SQLException if the journal or page geometry is unsupported
   */
  static long geometry(Connection connection) throws SQLException {
    try (var query = connection.createStatement()) {
      try (var row = query.executeQuery("PRAGMA journal_mode")) {
        if (!row.next() || !"wal".equalsIgnoreCase(row.getString(1)))
          throw corrupt("funded writes require WAL");
      }
      try (var row = query.executeQuery("PRAGMA auto_vacuum")) {
        if (!row.next() || row.getInt(1) != 0)
          throw corrupt("funded writes require no auto-vacuum");
      }
    }
    return pageSize(connection);
  }

  private static long pageSize(Connection connection) throws SQLException {
    try (var query = connection.createStatement();
        var row = query.executeQuery("PRAGMA page_size")) {
      if (!row.next()) throw corrupt("missing SQLite page geometry");
      long page = row.getLong(1);
      if (page < 512 || page > 65536 || Long.bitCount(page) != 1)
        throw corrupt("unsupported SQLite page geometry");
      return page;
    }
  }

  /**
   * Conservative WAL bytes for this full image and the shared clock, including commit padding.
   *
   * @param capacity image body capacity
   * @param page SQLite page size
   * @return reserved WAL bytes
   */
  static long cost(int capacity, long page) {
    if (capacity < 1
        || capacity > MAX_CAPACITY
        || page < 512
        || page > 65536
        || Long.bitCount(page) != 1) throw exhausted();
    long frame = page + 24;
    long dirty =
        Math.ceilDiv((long) HEADER + capacity, page - 4)
            + 1
            + Math.ceilDiv((long) HEADER + CLOCK_CAPACITY, page - 4)
            + 1;
    return add(32, multiply(dirty + 1 + Math.ceilDiv(65536, frame), frame));
  }

  private static long usable(BoundedSqlite.Limits limits, long page) {
    long regions = limits.sharedMemoryBytes() / 32768;
    long frames = 4062 + (regions - 1) * 4096;
    return Math.min(limits.walBytes(), add(32, multiply(frames, page + 24)));
  }

  private static void install(
      Connection connection, BoundedSqlite.Limits limits, long page, long retained)
      throws SQLException {
    long usable = usable(limits, page);
    if (retained > usable - 32) throw exhausted();
    BoundedSqlite.walCeiling(connection, usable - retained);
  }

  private static long add(long left, long right) {
    try {
      return Math.addExact(left, right);
    } catch (ArithmeticException overflow) {
      throw exhausted();
    }
  }

  private static long multiply(long left, long right) {
    try {
      return Math.multiplyExact(left, right);
    } catch (ArithmeticException overflow) {
      throw exhausted();
    }
  }

  private static ProtocolError exhausted() {
    return ProtocolError.limit("fixed-record completion capacity exhausted");
  }

  private static SQLException corrupt(String detail) {
    return new SQLException("V2 fixed records: " + detail);
  }
}
