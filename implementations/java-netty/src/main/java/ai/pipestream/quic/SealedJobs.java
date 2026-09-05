package ai.pipestream.quic;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/** Durable dispatch for the Java sealed server; all methods are blocking storage operations. */
final class SealedJobs {
  static final int PROCESS = 0, REHYDRATE = 1;
  static final int QUEUED = 0, RUNNING = 1, FINISHED = 2, REFUSED = 3;
  static final int MAX_QUEUED = 128, MAX_SESSION_QUEUED = 32;
  private static final int MAX_DESCRIPTOR = Wire.MAX_ENTITY_HEADER + 2048;
  private static final long MAX_RETAINED = 64L << 20, MAX_SESSION_RETAINED = 16L << 20;
  private static final int RESERVED_OUTCOME = 256;
  private static final String COLUMNS = "session,scope,entity,kind,CASE WHEN length(input) BETWEEN 1 AND "
      + MAX_DESCRIPTOR + " THEN input END,input_hash,state,epoch,worker,expires,"
      + "CASE WHEN length(outcome) BETWEEN 1 AND 128 THEN outcome END,length(outcome),checksum";
  private final SealedSessionStore sessions;

  SealedJobs(SealedSessionStore sessions) { this.sessions = sessions; }

  record Key(SealedPayloadStore.Identity identity, int kind) {}
  record Input(SealedPayloadStore.Identity identity, SealedTransport.Header header,
      long length, byte[] digest, SealedScope.Digest child) {
    Input { digest = digest.clone(); }
    @Override public byte[] digest() { return digest.clone(); }
  }
  record Outcome(int state, byte[] digest, Long refusal) {
    Outcome { digest = digest == null ? null : digest.clone(); }
    @Override public byte[] digest() { return digest == null ? null : digest.clone(); }
    static Outcome complete(byte[] digest) { return new Outcome(Wire.STATUS_COMPLETE, digest, null); }
    static Outcome failed() { return new Outcome(Wire.STATUS_FAILED, null, null); }
    static Outcome dehydrate() { return new Outcome(6, null, null); }
    static Outcome refused(long code) { return new Outcome(0, null, code); }
  }
  record Lease(Key key, long epoch, UUID worker, long expires) {}
  record Job(Key key, Input input, int state, long epoch, UUID worker, long expires, Outcome outcome) {}
  record Closure(SealedScope.Digest digest, SealedWork.EntityKey parent, int state) {}

  static void createSchema(Connection connection) throws SQLException {
    try (var statement = connection.createStatement()) {
      statement.execute("CREATE TABLE ps_java_job_policy (singleton INTEGER PRIMARY KEY CHECK(singleton=1), queued INTEGER NOT NULL, session_queued INTEGER NOT NULL, retained INTEGER NOT NULL, session_retained INTEGER NOT NULL, descriptor INTEGER NOT NULL, outcome_reservation INTEGER NOT NULL) STRICT");
      statement.execute("INSERT INTO ps_java_job_policy VALUES (1," + MAX_QUEUED + "," + MAX_SESSION_QUEUED + ","
          + MAX_RETAINED + "," + MAX_SESSION_RETAINED + "," + MAX_DESCRIPTOR + "," + RESERVED_OUTCOME + ")");
      statement.execute("""
          CREATE TABLE ps_java_jobs (
            session TEXT NOT NULL, scope INTEGER NOT NULL, entity INTEGER NOT NULL,
            kind INTEGER NOT NULL CHECK(kind IN (0,1)), input BLOB NOT NULL,
            input_hash BLOB NOT NULL CHECK(length(input_hash)=32),
            state INTEGER NOT NULL CHECK(state BETWEEN 0 AND 3),
            epoch INTEGER NOT NULL CHECK(epoch>=0), worker BLOB, expires INTEGER NOT NULL CHECK(expires>=0),
            outcome BLOB, checksum BLOB NOT NULL CHECK(length(checksum)=32),
            PRIMARY KEY(session,scope,entity,kind),
            FOREIGN KEY(session,scope,entity) REFERENCES ps_java_entities(session,scope,id),
            CHECK((state=0 AND epoch=0 AND worker IS NULL AND expires=0 AND outcome IS NULL)
              OR (state>0 AND epoch>0 AND worker IS NOT NULL AND length(worker)=16 AND expires>0
                AND ((state=1 AND outcome IS NULL) OR (state IN (2,3) AND outcome IS NOT NULL))))
          ) STRICT
          """);
      statement.execute("CREATE INDEX ps_java_jobs_ready ON ps_java_jobs(state,expires,session,scope,entity,kind)");
    }
  }

  static void checkPolicy(Connection connection) throws SQLException {
    try (var statement = connection.createStatement(); var rows = statement.executeQuery("SELECT queued,session_queued,retained,session_retained,descriptor,outcome_reservation FROM ps_java_job_policy WHERE singleton=1")) {
      if (!rows.next() || rows.getInt(1) != MAX_QUEUED || rows.getInt(2) != MAX_SESSION_QUEUED
          || rows.getLong(3) != MAX_RETAINED || rows.getLong(4) != MAX_SESSION_RETAINED
          || rows.getInt(5) != MAX_DESCRIPTOR || rows.getInt(6) != RESERVED_OUTCOME || rows.next()) {
        throw new SQLException("unsupported Java durable-job policy");
      }
    }
  }

  static void requireUnmanaged(Connection connection, String session, SealedWork.EntityKey key) throws SQLException, ProtocolException {
    if (key == null) return;
    try (var query = connection.prepareStatement("SELECT managed FROM ps_java_entities WHERE session=? AND scope=? AND id=?")) {
      bindEntity(query, session, key);
      try (var rows = query.executeQuery()) { if (rows.next() && rows.getInt(1) != 0) throw Wire.entity("managed work requires durable dispatch and an executor fence"); }
    }
  }

  void admit(SealedPayloadStore.Stored stored) throws SQLException, ProtocolException {
    Input input = new Input(stored.identity(), stored.header(), stored.length(), stored.digest(), null);
    byte[] descriptor = encode(input);
    sessions.transaction(connection -> {
      audit(connection);
      var identity = input.identity();
      SealedSessionStore.admit(connection, identity.session(), identity.producer(), identity.entity(), input.header().parent(), input.digest());
      try (var update = connection.prepareStatement("UPDATE ps_java_entities SET managed=1 WHERE session=? AND scope=? AND id=?")) {
        bindEntity(update, identity.session(), identity.entity()); update.executeUpdate();
      }
      enqueue(connection, new Key(identity, PROCESS), descriptor);
      return null;
    });
  }

  Optional<Closure> closeScope(String session, UUID producer, long scope) throws SQLException, ProtocolException {
    return closeScope(session, producer, scope, null);
  }

  Closure confirmScope(String session, UUID producer, SealedScope.Digest expected) throws SQLException, ProtocolException {
    return closeScope(session, producer, expected.scopeId(), expected).orElseThrow(() -> SealedScope.invalid("scope digest has unresolved declared work"));
  }

  private Optional<Closure> closeScope(String session, UUID producer, long scope, SealedScope.Digest expected) throws SQLException, ProtocolException {
    return sessions.transaction(connection -> {
      audit(connection);
      var summary = SealedSessionStore.closeScope(connection, session, producer, scope);
      if (summary.isEmpty()) return Optional.empty();
      if (expected != null && !expected.equals(summary.get())) throw Wire.integrity("scope digest differs from retained results");
      SealedWork.EntityKey parent;
      try (var query = connection.prepareStatement("SELECT parent_scope,parent_id FROM ps_java_scopes WHERE session=? AND id=?")) {
        query.setString(1, session); query.setLong(2, scope);
        try (var rows = query.executeQuery()) {
          if (!rows.next() || rows.getObject(1) == null || rows.getObject(2) == null) throw Wire.integrity("closed child has no parent");
          parent = new SealedWork.EntityKey(rows.getLong(1), rows.getLong(2));
        }
      }
      var identity = new SealedPayloadStore.Identity(session, producer, parent);
      Job process = read(connection, new Key(identity, PROCESS));
      if (process == null || process.state() != FINISHED || process.outcome().state() != 6) throw Wire.entity("child parent is not managed dehydrated work");
      int state = SealedSessionStore.status(connection, session, producer, parent);
      if (state == 6) {
        var resolution = SealedSessionStore.resolveChildren(connection, session, producer, parent);
        if (resolution == SealedSessionStore.ChildResolution.PENDING) throw Wire.integrity("newly closed child remained pending");
        state = resolution == SealedSessionStore.ChildResolution.REHYDRATING ? 7 : Wire.STATUS_FAILED;
        if (state == 7) {
          Input original = process.input();
          enqueue(connection, new Key(identity, REHYDRATE), encode(new Input(identity, original.header(), original.length(), original.digest(), summary.get())));
        }
      }
      return Optional.of(new Closure(summary.get(), parent, state));
    });
  }

  List<Key> ready(long now, int limit) throws SQLException, ProtocolException {
    if (now < 0 || limit < 1 || limit > MAX_QUEUED) throw new IllegalArgumentException("invalid ready-job bounds");
    return sessions.transaction(connection -> {
      audit(connection);
      List<Key> result = new ArrayList<>();
      try (var query = connection.prepareStatement("SELECT " + COLUMNS + " FROM ps_java_jobs WHERE state=0 OR (state=1 AND expires<=?) ORDER BY state,expires,session,scope,entity,kind LIMIT ?")) {
        query.setLong(1, now); query.setInt(2, limit);
        try (var rows = query.executeQuery()) { while (rows.next()) result.add(decode(rows).key()); }
      }
      return List.copyOf(result);
    });
  }

  Optional<Job> find(Key key) throws SQLException, ProtocolException {
    return sessions.transaction(connection -> {
      SealedSessionStore.status(connection, key.identity().session(), key.identity().producer(), key.identity().entity());
      return Optional.ofNullable(read(connection, key));
    });
  }

  Lease acquire(Key key, UUID worker, long now, long durationMillis) throws SQLException, ProtocolException {
    if (worker == null || worker.equals(new UUID(0, 0)) || now < 0 || durationMillis < 1
        || durationMillis > 300_000 || now > Long.MAX_VALUE - durationMillis) throw new IllegalArgumentException("invalid executor lease");
    return sessions.transaction(connection -> {
      audit(connection);
      Job job = required(connection, key);
      if (job.state() >= FINISHED || (job.state() == RUNNING && job.expires() > now)) throw Wire.entity("job is terminal or has an active executor");
      if (job.epoch() == Long.MAX_VALUE) throw Wire.limit("executor epoch exhausted");
      Lease lease = new Lease(key, job.epoch() + 1, worker, now + durationMillis);
      writeState(connection, job, RUNNING, lease, null);
      return lease;
    });
  }

  void publish(Lease lease, long now, Outcome outcome) throws SQLException, ProtocolException {
    if (now < 0) throw new IllegalArgumentException("invalid publication time");
    byte[] encodedOutcome = encode(outcome);
    sessions.transaction(connection -> {
      audit(connection);
      Job job = required(connection, lease.key());
      if (job.epoch() != lease.epoch() || !java.util.Objects.equals(job.worker(), lease.worker()) || job.expires() != lease.expires()) throw Wire.entity("stale executor fence");
      if (job.state() >= FINISHED) {
        if (!Arrays.equals(encode(job.outcome()), encodedOutcome)) throw Wire.entity("terminal job outcome is immutable");
        return null;
      }
      if (job.state() != RUNNING || now >= lease.expires()) throw Wire.entity("executor lease expired or is not running");
      var identity = job.key().identity();
      if (outcome.refusal() == null) {
        if (job.key().kind() == PROCESS) SealedSessionStore.processed(connection, identity.session(), identity.producer(), identity.entity(), outcome.state(), outcome.digest());
        else {
          if (outcome.state() == 6) throw Wire.entity("rehydration cannot dehydrate again");
          SealedSessionStore.rehydrated(connection, identity.session(), identity.producer(), identity.entity(), outcome.state() == Wire.STATUS_COMPLETE, outcome.digest());
        }
      }
      writeState(connection, job, outcome.refusal() == null ? FINISHED : REFUSED, lease, outcome);
      return null;
    });
  }

  void audit() throws SQLException, ProtocolException { sessions.transaction(connection -> { audit(connection); return null; }); }

  private static void enqueue(Connection connection, Key key, byte[] descriptor) throws SQLException, ProtocolException {
    try (var query = connection.prepareStatement("SELECT count(*) FILTER (WHERE state<2),count(*) FILTER (WHERE state<2 AND session=?),coalesce(sum(length(input)+?),0),coalesce(sum(CASE WHEN session=? THEN length(input)+? ELSE 0 END),0) FROM ps_java_jobs")) {
      query.setString(1, key.identity().session()); query.setInt(2, RESERVED_OUTCOME);
      query.setString(3, key.identity().session()); query.setInt(4, RESERVED_OUTCOME);
      try (var rows = query.executeQuery()) {
        if (!rows.next()) throw Wire.integrity("job accounting is absent");
        long charge = descriptor.length + RESERVED_OUTCOME;
        if (rows.getLong(1) >= MAX_QUEUED || rows.getLong(2) >= MAX_SESSION_QUEUED
            || charge > MAX_RETAINED - rows.getLong(3) || charge > MAX_SESSION_RETAINED - rows.getLong(4)) throw Wire.limit("durable job capacity exhausted");
      }
    }
    byte[] hash = SealedWork.sha256().digest(descriptor);
    try (var insert = connection.prepareStatement("INSERT INTO ps_java_jobs VALUES (?,?,?,?,?,?,0,0,NULL,0,NULL,?)")) {
      bindKey(insert, key); insert.setBytes(5, descriptor); insert.setBytes(6, hash);
      insert.setBytes(7, checksum(key, hash, QUEUED, 0, null, 0, null)); insert.executeUpdate();
    }
  }

  private static void writeState(Connection connection, Job job, int state, Lease lease, Outcome outcome) throws SQLException, ProtocolException {
    byte[] encoded = outcome == null ? null : encode(outcome);
    byte[] descriptorHash = SealedWork.sha256().digest(encode(job.input()));
    try (var update = connection.prepareStatement("UPDATE ps_java_jobs SET state=?,epoch=?,worker=?,expires=?,outcome=?,checksum=? WHERE session=? AND scope=? AND entity=? AND kind=?")) {
      update.setInt(1, state); update.setLong(2, lease.epoch()); update.setBytes(3, SealedWork.producerBytes(lease.worker()));
      update.setLong(4, lease.expires()); update.setBytes(5, encoded);
      update.setBytes(6, checksum(job.key(), descriptorHash, state, lease.epoch(), lease.worker(), lease.expires(), encoded));
      update.setString(7, job.key().identity().session()); update.setLong(8, job.key().identity().entity().scopeId());
      update.setLong(9, job.key().identity().entity().entityId()); update.setInt(10, job.key().kind());
      if (update.executeUpdate() != 1) throw Wire.integrity("job disappeared during publication");
    }
  }

  private static Job required(Connection connection, Key key) throws SQLException, ProtocolException {
    SealedSessionStore.status(connection, key.identity().session(), key.identity().producer(), key.identity().entity());
    Job job = read(connection, key);
    if (job == null) throw Wire.entity("job is absent");
    return job;
  }

  private static Job read(Connection connection, Key key) throws SQLException, ProtocolException {
    try (var query = connection.prepareStatement("SELECT " + COLUMNS + " FROM ps_java_jobs WHERE session=? AND scope=? AND entity=? AND kind=?")) {
      bindKey(query, key);
      try (var rows = query.executeQuery()) {
        if (!rows.next()) return null;
        Job job = decode(rows);
        if (!job.key().equals(key)) throw Wire.entity("job ownership differs");
        return job;
      }
    }
  }

  static void audit(Connection connection) throws SQLException, ProtocolException {
    try (var statement = connection.createStatement(); var rows = statement.executeQuery("SELECT 1 FROM ps_java_entities e WHERE managed=1 AND (NOT EXISTS(SELECT 1 FROM ps_java_jobs j WHERE j.session=e.session AND j.scope=e.scope AND j.entity=e.id AND j.kind=0) OR (e.state=7 AND NOT EXISTS(SELECT 1 FROM ps_java_jobs j WHERE j.session=e.session AND j.scope=e.scope AND j.entity=e.id AND j.kind=1))) LIMIT 1")) {
      if (rows.next()) throw Wire.integrity("managed entity has no durable job");
    }
    long retained = 0; int queued = 0, count = 0;
    Map<String, Long> sessionBytes = new java.util.HashMap<>();
    Map<String, Integer> sessionQueued = new java.util.HashMap<>();
    try (var statement = connection.createStatement(); var rows = statement.executeQuery("SELECT " + COLUMNS + " FROM ps_java_jobs ORDER BY session,scope,entity,kind")) {
      while (rows.next()) {
        if (++count > 131_072) throw Wire.integrity("retained job count exceeds entity policy");
        Job job = decode(rows); var identity = job.key().identity();
        long charge = rows.getBytes(5).length + RESERVED_OUTCOME;
        retained += charge;
        long scopeBytes = sessionBytes.merge(identity.session(), charge, Long::sum);
        if (retained > MAX_RETAINED || scopeBytes > MAX_SESSION_RETAINED || sessionBytes.size() > 512) throw Wire.integrity("retained job bytes exceed policy");
        if (job.state() < FINISHED && (++queued > MAX_QUEUED
            || sessionQueued.merge(identity.session(), 1, Integer::sum) > MAX_SESSION_QUEUED)) throw Wire.integrity("unfinished jobs exceed policy");
        try (var entity = connection.prepareStatement("SELECT managed,state,payload_digest,output_digest,producer FROM ps_java_entities e JOIN ps_java_sessions s ON e.session=s.id WHERE e.session=? AND e.scope=? AND e.id=?")) {
          bindEntity(entity, identity.session(), identity.entity());
          try (var record = entity.executeQuery()) {
            if (!record.next() || record.getInt(1) != 1 || !Arrays.equals(record.getBytes(3), job.input().digest())
                || !Arrays.equals(record.getBytes(5), SealedWork.producerBytes(identity.producer()))) throw Wire.integrity("job no longer binds its admitted input");
            int state = record.getInt(2);
            if (job.state() != FINISHED) {
              if (state != (job.key().kind() == PROCESS ? 2 : 7)) throw Wire.integrity("unfinished job lifecycle differs");
            } else if (job.outcome().state() != 6) {
              if (state != job.outcome().state() || !Arrays.equals(record.getBytes(4), job.outcome().digest())) throw Wire.integrity("terminal job lifecycle differs");
            } else {
              if (state != 6 && state != 7 && state != 3 && state != 4) throw Wire.integrity("dehydrated job lifecycle differs");
              Job rehydration = read(connection, new Key(identity, REHYDRATE));
              if ((state == 3 || state == 7) && rehydration == null) throw Wire.integrity("resolved parent lost its rehydration job");
              if (state == 4 && rehydration == null) {
                try (var child = connection.prepareStatement("SELECT CASE WHEN length(closure)=77 THEN closure END FROM ps_java_scopes WHERE session=? AND parent_scope=? AND parent_id=?")) {
                  bindEntity(child, identity.session(), identity.entity());
                  try (var children = child.executeQuery()) {
                    if (!children.next() || children.getBytes(1) == null || SealedScope.decode(children.getBytes(1)).failed().signum() == 0) throw Wire.integrity("failed parent has neither rehydration nor failed children");
                  }
                }
              }
            }
          }
        }
      }
    }
  }

  private static Job decode(ResultSet rows) throws SQLException, ProtocolException {
    byte[] descriptor = rows.getBytes(5), inputHash = rows.getBytes(6), outcomeBytes = rows.getBytes(11);
    if (descriptor == null || !MessageDigest.isEqual(SealedWork.sha256().digest(descriptor), inputHash)
        || (rows.getObject(12) != null && outcomeBytes == null)) throw Wire.integrity("job descriptor or outcome is oversized or corrupt");
    Input input = decodeInput(descriptor);
    Key key = new Key(input.identity(), rows.getInt(4));
    if (!key.identity().session().equals(rows.getString(1)) || key.identity().entity().scopeId() != rows.getLong(2)
        || key.identity().entity().entityId() != rows.getLong(3) || (key.kind() != PROCESS && key.kind() != REHYDRATE)
        || (key.kind() == REHYDRATE) != (input.child() != null)) throw Wire.integrity("job descriptor binding differs");
    int state = rows.getInt(7); long epoch = rows.getLong(8), expires = rows.getLong(10);
    byte[] workerBytes = rows.getBytes(9); UUID worker = workerBytes == null ? null : uuid(workerBytes);
    if (!MessageDigest.isEqual(checksum(key, inputHash, state, epoch, worker, expires, outcomeBytes), rows.getBytes(13))) throw Wire.integrity("job state checksum differs");
    Outcome outcome = outcomeBytes == null ? null : decodeOutcome(outcomeBytes);
    if ((state == FINISHED && (outcome == null || outcome.refusal() != null))
        || (state == REFUSED && (outcome == null || outcome.refusal() == null))
        || (state < FINISHED && outcome != null)) throw Wire.integrity("job outcome state differs");
    if (key.kind() == REHYDRATE && outcome != null && outcome.state() == 6) throw Wire.integrity("rehydration outcome cannot dehydrate");
    return new Job(key, input, state, epoch, worker, expires, outcome);
  }

  private static byte[] encode(Input input) throws ProtocolException {
    if (input.length() < 0 || input.digest().length != 32 || input.header().chunk() != null
        || !input.header().key().equals(input.identity().entity()) || !SealedWork.validSessionId(input.identity().session())
        || input.identity().producer().equals(new UUID(0, 0))) throw Wire.entity("invalid durable job input");
    Map<String, Object> fields = new LinkedHashMap<>();
    fields.put("session", input.identity().session()); fields.put("producer", SealedWork.producerBytes(input.identity().producer()));
    fields.put("header", SealedTransport.header(input.header())); fields.put("length", input.length()); fields.put("digest", input.digest());
    if (input.child() != null) fields.put("child", SealedScope.encode(input.child()));
    return SealedCbor.encode(fields, MAX_DESCRIPTOR);
  }
  private static Input decodeInput(byte[] bytes) throws ProtocolException {
    var fields = SealedCbor.decode(bytes, MAX_DESCRIPTOR);
    SealedWork.only(fields, "session", "producer", "header", "length", "digest", "child");
    if (!(fields.get("header") instanceof byte[] header) || header.length < 4
        || ByteBuffer.wrap(header).getInt() != header.length - 4) throw Wire.integrity("job input header geometry differs");
    var parsed = SealedTransport.header(Arrays.copyOfRange(header, 4, header.length));
    var identity = new SealedPayloadStore.Identity(SealedWork.text(fields, "session"), uuid(SealedWork.bytes(fields, "producer", 16)), parsed.key());
    Input result = new Input(identity, parsed, SealedWork.bounded(fields, "length", Long.MAX_VALUE), SealedWork.bytes(fields, "digest", 32),
        fields.containsKey("child") ? SealedScope.decode(SealedWork.bytes(fields, "child", 77)) : null);
    if (!Arrays.equals(bytes, encode(result))) throw Wire.integrity("job input is not canonical");
    return result;
  }
  private static byte[] encode(Outcome outcome) throws ProtocolException {
    if (outcome.refusal() != null) {
      if (outcome.refusal() < 0 || outcome.refusal() > 0xffff_ffffL || outcome.state() != 0 || outcome.digest() != null) throw Wire.entity("invalid job refusal");
      return SealedCbor.encode(Map.of("refusal", outcome.refusal()), 128);
    }
    if ((outcome.state() != 3 && outcome.state() != 4 && outcome.state() != 6)
        || (outcome.state() == 3) != (outcome.digest() != null) || (outcome.digest() != null && outcome.digest().length != 32)) throw Wire.entity("invalid job result");
    return SealedCbor.encode(outcome.digest() == null ? Map.of("state", outcome.state()) : Map.of("state", outcome.state(), "digest", outcome.digest()), 128);
  }
  private static Outcome decodeOutcome(byte[] bytes) throws ProtocolException {
    var fields = SealedCbor.decode(bytes, 128); SealedWork.only(fields, "state", "digest", "refusal");
    Outcome outcome = fields.containsKey("refusal") ? Outcome.refused(SealedWork.bounded(fields, "refusal", 0xffff_ffffL))
        : new Outcome((int) SealedWork.bounded(fields, "state", 6), fields.containsKey("digest") ? SealedWork.bytes(fields, "digest", 32) : null, null);
    if (!Arrays.equals(bytes, encode(outcome))) throw Wire.integrity("job outcome is not canonical");
    return outcome;
  }
  private static byte[] checksum(Key key, byte[] inputHash, int state, long epoch, UUID worker, long expires, byte[] outcome) throws ProtocolException {
    Map<String, Object> fields = new LinkedHashMap<>();
    fields.put("format", "pipestream-java-job-v1"); fields.put("session", key.identity().session());
    fields.put("producer", SealedWork.producerBytes(key.identity().producer())); fields.put("scope", key.identity().entity().scopeId());
    fields.put("entity", key.identity().entity().entityId()); fields.put("kind", key.kind()); fields.put("input", inputHash);
    fields.put("state", state); fields.put("epoch", epoch); fields.put("expires", expires);
    if (worker != null) fields.put("worker", SealedWork.producerBytes(worker));
    if (outcome != null) fields.put("outcome", outcome);
    return SealedWork.sha256().digest(SealedCbor.encode(fields, 2048));
  }
  private static UUID uuid(byte[] bytes) throws ProtocolException {
    if (bytes.length != 16) throw Wire.integrity("job UUID is not 16 octets");
    var input = ByteBuffer.wrap(bytes); return new UUID(input.getLong(), input.getLong());
  }
  private static void bindKey(PreparedStatement statement, Key key) throws SQLException {
    bindEntity(statement, key.identity().session(), key.identity().entity()); statement.setInt(4, key.kind());
  }
  private static void bindEntity(PreparedStatement statement, String session, SealedWork.EntityKey entity) throws SQLException {
    statement.setString(1, session); statement.setLong(2, entity.scopeId()); statement.setLong(3, entity.entityId());
  }
}
