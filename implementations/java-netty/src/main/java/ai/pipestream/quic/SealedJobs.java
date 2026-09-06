package ai.pipestream.quic;

import java.io.IOException;
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
  static final int RESERVED = 4, RETIRED = 5;
  static final int MAX_QUEUED = 128, MAX_SESSION_QUEUED = 32;
  // One completion slot per admitted entity; waiting parents must not occupy
  // ordinary processing slots needed by their own children.
  private static final int MAX_COMPLETIONS = 65_536, MAX_SESSION_COMPLETIONS = 16_384;
  private static final int MAX_DESCRIPTOR = Wire.MAX_ENTITY_HEADER + 2048;
  private static final long MAX_RETAINED = 64L << 20, MAX_SESSION_RETAINED = 16L << 20;
  private static final int RESERVED_OUTCOME = 256;
  // Five- and six-member descriptor maps both have a one-byte CBOR header.
  // Adding "child" costs six key bytes, two byte-string header bytes and a
  // fixed 77-byte SCOPE_DIGEST frame. Tests pin conversion to actual descriptors.
  private static final int CHILD_DESCRIPTOR_BYTES = 6 + 2 + 77;
  private static final byte[] IMAGE_MAGIC = {'P', 'S', 'J', 'J', 'O', 'B', '0', '1'};
  private static final int STATE_OFFSET = 8, EXPIRY_OFFSET = 36, OUTCOME_OFFSET = 48;
  private static final int CHECKSUM_OFFSET = RESERVED_OUTCOME - 32;
  // Positive signed Java counters are stored big-endian, so fixed-width BLOB
  // ordering agrees with numeric ordering. These are read projections only;
  // the table has no generated columns or indexes over mutable image bytes.
  private static final String STATE_SQL = "substr(image," + (STATE_OFFSET + 1) + ",4)";
  private static final String EXPIRY_SQL = "substr(image," + (EXPIRY_OFFSET + 1) + ",8)";
  private static final String COLUMNS = "session,scope,entity,kind,CASE WHEN length(input) BETWEEN 1 AND "
      + MAX_DESCRIPTOR + " THEN input END,input_hash,CASE WHEN length(image)=" + RESERVED_OUTCOME + " THEN image END";
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
            image BLOB NOT NULL CHECK(length(image)=256),
            PRIMARY KEY(session,scope,entity,kind),
            FOREIGN KEY(session,scope,entity) REFERENCES ps_java_entities(session,scope,id)
          ) STRICT
          """);
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
    var entity = SealedSessionStore.entity(connection, session, key);
    if (entity != null && entity.value().managed()) throw Wire.entity("managed work requires durable dispatch and an executor fence");
  }

  void admit(SealedPayloadStore.Stored stored) throws IOException, SQLException, ProtocolException {
    Input input = new Input(stored.identity(), stored.header(), stored.length(), stored.digest(), null);
    byte[] descriptor = encode(input);
    stored.withAdmission(sessions, (connection, funding) -> {
      audit(connection);
      if (descriptor.length + CHILD_DESCRIPTOR_BYTES > MAX_DESCRIPTOR) throw Wire.limit("future rehydration descriptor exceeds its bound");
      funding.adjust(funding.model().admission(descriptor.length + CHILD_DESCRIPTOR_BYTES));
      var identity = input.identity();
      SealedSessionStore.admit(connection, identity.session(), identity.producer(), identity.entity(), input.header().parent(), input.digest(), true);
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
    return sessions.fundedTransaction((connection, funding) -> closeScope(connection, funding, session, producer, scope, expected));
  }

  static Optional<Closure> closeScope(Connection connection, SealedCompletionReservations funding, String session,
      UUID producer, long scope, SealedScope.Digest expected) throws SQLException, ProtocolException {
    audit(connection);
    var summary = SealedSessionStore.previewScope(connection, session, producer, scope);
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
      int capacity = encode(process.input()).length + CHILD_DESCRIPTOR_BYTES;
      long released = summary.get().failed().signum() == 0
          ? funding.model().conversion(capacity) : funding.model().future(capacity);
      funding.adjust(-released);
      SealedSessionStore.commitClosure(connection, session, producer, summary.get());
      var resolution = SealedSessionStore.resolveChildren(connection, session, producer, parent);
      if (resolution == SealedSessionStore.ChildResolution.PENDING) throw Wire.integrity("newly closed child remained pending");
      state = resolution == SealedSessionStore.ChildResolution.REHYDRATING ? 7 : Wire.STATUS_FAILED;
      if (state == 7) {
        Input original = process.input();
        activateFuture(connection, process, new Input(identity, original.header(), original.length(), original.digest(), summary.get()));
      } else retireFuture(connection, process);
      audit(connection);
    }
    return Optional.of(new Closure(summary.get(), parent, state));
  }

  List<Key> ready(long now, int limit) throws SQLException, ProtocolException {
    if (now < 0 || limit < 1 || limit > MAX_QUEUED) throw new IllegalArgumentException("invalid ready-job bounds");
    return sessions.readTransaction(connection -> {
      audit(connection);
      List<Key> result = new ArrayList<>();
      // Round-robin the bounded page across sessions. A large reserved queue in
      // one session must not hide other sessions behind its occupied workers.
      try (var query = connection.prepareStatement("SELECT " + COLUMNS + " FROM ps_java_jobs WHERE " + STATE_SQL
          + "=x'00000000' OR (" + STATE_SQL + "=x'00000001' AND " + EXPIRY_SQL + "<=?) ORDER BY row_number() OVER (PARTITION BY session ORDER BY kind DESC,"
          + STATE_SQL + "," + EXPIRY_SQL + ",scope,entity),session LIMIT ?")) {
        query.setBytes(1, ByteBuffer.allocate(8).putLong(now).array()); query.setInt(2, limit);
        try (var rows = query.executeQuery()) { while (rows.next()) result.add(decode(rows).key()); }
      }
      return List.copyOf(result);
    });
  }

  Optional<Job> find(Key key) throws SQLException, ProtocolException {
    return sessions.readTransaction(connection -> find(connection, key));
  }

  /** A bounded observer batch shares one audited read snapshot. */
  List<Job> findAll(List<Key> requested) throws SQLException, ProtocolException {
    if (requested.size() > 128) throw Wire.limit("job observation batch exceeds its bound");
    List<Key> keys = List.copyOf(requested);
    if (keys.isEmpty()) return List.of();
    if (new java.util.HashSet<>(keys).size() != keys.size()) throw Wire.entity("duplicate job observation key");
    return sessions.readTransaction(connection -> {
      List<Job> result = new ArrayList<>(keys.size());
      for (Key key : keys) result.add(find(connection, key).orElseThrow(() -> Wire.integrity("observed durable job is absent")));
      return List.copyOf(result);
    });
  }

  private static Optional<Job> find(Connection connection, Key key) throws SQLException, ProtocolException {
    if (key.kind() != PROCESS && key.kind() != REHYDRATE) throw Wire.entity("invalid job kind");
    var identity = key.identity();
    int state = SealedSessionStore.status(connection, identity.session(), identity.producer(), identity.entity());
    Job process = read(connection, new Key(identity, PROCESS)), future = read(connection, new Key(identity, REHYDRATE));
    if (process != null) auditPair(connection, process, future, state);
    else if (future != null || SealedSessionStore.entity(connection, identity.session(), identity.entity()).value().managed()) {
      throw Wire.integrity("managed work lost its processing job");
    }
    return Optional.ofNullable(key.kind() == PROCESS ? process : future).filter(job -> job.state() < RESERVED);
  }

  Lease acquire(Key key, UUID worker, long now, long durationMillis) throws SQLException, ProtocolException {
    if (worker == null || worker.equals(new UUID(0, 0)) || now < 0 || durationMillis < 1
        || durationMillis > 300_000 || now > Long.MAX_VALUE - durationMillis) throw new IllegalArgumentException("invalid executor lease");
    return sessions.fundedTransaction((connection, funding) -> {
      audit(connection);
      Job job = required(connection, key);
      if (job.state() >= FINISHED || (job.state() == RUNNING && job.expires() > now)) throw Wire.entity("job is terminal or has an active executor");
      if (job.epoch() == Long.MAX_VALUE) throw Wire.limit("executor epoch exhausted");
      Lease lease = new Lease(key, job.epoch() + 1, worker, now + durationMillis);
      if (job.state() == QUEUED) funding.adjust(-funding.model().acquisition());
      writeState(connection, job, RUNNING, lease, null);
      audit(connection);
      return lease;
    });
  }

  void publish(Lease lease, long now, Outcome outcome) throws SQLException, ProtocolException {
    sessions.fundedTransaction((connection, funding) -> {
      publish(connection, funding, lease, now, outcome);
      return null;
    });
  }

  static void publish(Connection connection, SealedCompletionReservations funding, Lease lease, long now, Outcome outcome)
      throws SQLException, ProtocolException {
    if (now < 0) throw new IllegalArgumentException("invalid publication time");
    byte[] encodedOutcome = encode(outcome);
    audit(connection);
    Job job = required(connection, lease.key());
    if (job.epoch() != lease.epoch() || !java.util.Objects.equals(job.worker(), lease.worker()) || job.expires() != lease.expires()) throw Wire.entity("stale executor fence");
    if (job.state() >= FINISHED) {
      if (!Arrays.equals(encode(job.outcome()), encodedOutcome)) throw Wire.entity("terminal job outcome is immutable");
      return;
    }
    if (job.state() != RUNNING || now >= lease.expires()) throw Wire.entity("executor lease expired or is not running");
    long released = funding.model().publication(job.key().kind());
    if (job.key().kind() == PROCESS && (outcome.refusal() != null || outcome.state() != 6)) {
      released += funding.model().future(encode(job.input()).length + CHILD_DESCRIPTOR_BYTES);
    }
    funding.adjust(-released);
    var identity = job.key().identity();
    if (outcome.refusal() == null) {
      if (job.key().kind() == PROCESS) SealedSessionStore.processed(connection, identity.session(), identity.producer(), identity.entity(), outcome.state(), outcome.digest());
      else {
        if (outcome.state() == 6) throw Wire.entity("rehydration cannot dehydrate again");
        SealedSessionStore.rehydrated(connection, identity.session(), identity.producer(), identity.entity(), outcome.state() == Wire.STATUS_COMPLETE, outcome.digest());
      }
    }
    writeState(connection, job, outcome.refusal() == null ? FINISHED : REFUSED, lease, outcome);
    if (job.key().kind() == PROCESS && (outcome.refusal() != null || outcome.state() != 6)) retireFuture(connection, job);
    audit(connection);
  }

  void audit() throws SQLException, ProtocolException { sessions.readTransaction(connection -> null); }

  private static void enqueue(Connection connection, Key key, byte[] descriptor) throws SQLException, ProtocolException {
    if (key.kind() != PROCESS) throw Wire.integrity("rehydration requires its preallocated row");
    if (descriptor.length + CHILD_DESCRIPTOR_BYTES > MAX_DESCRIPTOR) throw Wire.limit("future rehydration descriptor exceeds its bound");
    byte[] hash = SealedWork.sha256().digest(descriptor);
    try (var insert = connection.prepareStatement("INSERT INTO ps_java_jobs VALUES (?,?,?,?,?,?,?)")) {
      bindKey(insert, key); insert.setBytes(5, descriptor); insert.setBytes(6, hash);
      insert.setBytes(7, stateImage(key, hash, QUEUED, 0, null, 0, null)); insert.executeUpdate();
      Key future = new Key(key.identity(), REHYDRATE);
      bindKey(insert, future);
      insert.setBytes(5, Arrays.copyOf(descriptor, descriptor.length + CHILD_DESCRIPTOR_BYTES));
      insert.setBytes(6, hash); insert.setBytes(7, stateImage(future, hash, RESERVED, 0, null, 0, null));
      insert.executeUpdate();
    }
    audit(connection, true);
  }

  private static void activateFuture(Connection connection, Job process, Input input) throws SQLException, ProtocolException {
    Key key = new Key(process.key().identity(), REHYDRATE);
    Job future = read(connection, key);
    if (future == null || future.state() != RESERVED) throw Wire.integrity("rehydration has no reserved row");
    byte[] descriptor = encode(input), original = encode(future.input());
    if (!Arrays.equals(original, encode(process.input())) || descriptor.length != original.length + CHILD_DESCRIPTOR_BYTES) {
      throw Wire.integrity("rehydration conversion differs from its reserved input");
    }
    long rowid = rowid(connection, key);
    byte[] hash = SealedWork.sha256().digest(descriptor);
    SealedSqliteImages.replace(connection, "ps_java_jobs", "input", rowid, descriptor);
    SealedSqliteImages.replace(connection, "ps_java_jobs", "input_hash", rowid, hash);
    SealedSqliteImages.replace(connection, "ps_java_jobs", "image", rowid, stateImage(key, hash, QUEUED, 0, null, 0, null));
  }

  private static void retireFuture(Connection connection, Job process) throws SQLException, ProtocolException {
    Key key = new Key(process.key().identity(), REHYDRATE);
    Job future = read(connection, key);
    if (future == null || future.state() != RESERVED || !Arrays.equals(encode(future.input()), encode(process.input()))) {
      throw Wire.integrity("unused rehydration row is missing or changed");
    }
    SealedSqliteImages.replace(connection, "ps_java_jobs", "image", rowid(connection, key),
        stateImage(key, SealedWork.sha256().digest(encode(future.input())), RETIRED, 0, null, 0, null));
  }

  private static void writeState(Connection connection, Job job, int state, Lease lease, Outcome outcome) throws SQLException, ProtocolException {
    byte[] encoded = outcome == null ? null : encode(outcome);
    byte[] descriptorHash = SealedWork.sha256().digest(encode(job.input()));
    SealedSqliteImages.replace(connection, "ps_java_jobs", "image", rowid(connection, job.key()),
        stateImage(job.key(), descriptorHash, state, lease.epoch(), lease.worker(), lease.expires(), encoded));
  }

  private static long rowid(Connection connection, Key key) throws SQLException, ProtocolException {
    try (var query = connection.prepareStatement("SELECT rowid FROM ps_java_jobs WHERE session=? AND scope=? AND entity=? AND kind=?")) {
      bindKey(query, key);
      try (var rows = query.executeQuery()) {
        if (!rows.next()) throw Wire.integrity("job disappeared during publication");
        return rows.getLong(1);
      }
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

  static SealedSessionStore.JobUsage audit(Connection connection) throws SQLException, ProtocolException {
    return audit(connection, false);
  }

  static long completionBytes(Connection connection, SealedCompletionReservations.Model model)
      throws SQLException, ProtocolException {
    audit(connection);
    long bytes = 0;
    try (var statement = connection.createStatement(); var rows = statement.executeQuery("SELECT " + COLUMNS + " FROM ps_java_jobs")) {
      int count = 0;
      while (rows.next()) {
        if (++count > 131_072) throw Wire.integrity("completion job count exceeds policy");
        Job job = decode(rows);
        long credit = switch (job.state()) {
          case QUEUED -> model.acquisition() + model.publication(job.key().kind());
          case RUNNING -> model.publication(job.key().kind());
          case RESERVED -> model.future(rows.getBytes(5).length);
          default -> 0;
        };
        bytes = Math.addExact(bytes, credit);
      }
    }
    return bytes;
  }

  private static SealedSessionStore.JobUsage audit(Connection connection, boolean admission) throws SQLException, ProtocolException {
    try (var statement = connection.createStatement(); var rows = statement.executeQuery("SELECT e.session,e.scope,e.id,CASE WHEN length(image)=112 THEN image END,s.producer,EXISTS(SELECT 1 FROM ps_java_jobs j WHERE j.session=e.session AND j.scope=e.scope AND j.entity=e.id AND j.kind=0),EXISTS(SELECT 1 FROM ps_java_jobs j WHERE j.session=e.session AND j.scope=e.scope AND j.entity=e.id AND j.kind=1) FROM ps_java_entities e JOIN ps_java_sessions s ON s.id=e.session")) {
      int entities = 0;
      while (rows.next()) {
        if (++entities > 65_536) throw Wire.integrity("entity images exceed declaration policy");
        var image = SealedStateImages.entity(rows.getString(1), rows.getBytes(5),
            new SealedWork.EntityKey(rows.getLong(2), rows.getLong(3)), rows.getBytes(4));
        if (image.managed() && (!rows.getBoolean(6) || (image.state() == 7 && !rows.getBoolean(7)))) {
          throw Wire.integrity("managed entity has no durable job");
        }
      }
    }
    long retained = 0, reserved = 0;
    int processing = 0, rehydrating = 0, futureSlots = 0, waiting = 0, count = 0;
    Map<String, Long> sessionBytes = new java.util.HashMap<>();
    Map<String, Integer> sessionQueued = new java.util.HashMap<>();
    Map<String, Integer> sessionCompletions = new java.util.HashMap<>();
    try (var statement = connection.createStatement(); var rows = statement.executeQuery("SELECT " + COLUMNS + " FROM ps_java_jobs ORDER BY session,scope,entity,kind")) {
      while (rows.next()) {
        if (++count > 131_072) throw Wire.integrity("retained job count exceeds entity policy");
        Job job = decode(rows); var identity = job.key().identity();
        long charge = rows.getBytes(5).length + RESERVED_OUTCOME;
        var record = SealedSessionStore.entity(connection, identity.session(), identity.entity());
        if (record == null || !record.value().managed() || !Arrays.equals(record.value().payloadDigest(), job.input().digest())
            || !Arrays.equals(record.producer(), SealedWork.producerBytes(identity.producer()))) throw Wire.integrity("job no longer binds its admitted input");
        int state = record.state();
        if (job.state() < RESERVED) {
          if (job.state() != FINISHED) {
            if (state != (job.key().kind() == PROCESS ? 2 : 7)) throw Wire.integrity("unfinished job lifecycle differs");
          } else if (job.outcome().state() != 6) {
            if (state != job.outcome().state() || !Arrays.equals(record.value().outputDigest(), job.outcome().digest())) throw Wire.integrity("terminal job lifecycle differs");
          }
        }
        if (job.key().kind() == PROCESS) {
          auditPair(connection, job, read(connection, new Key(identity, REHYDRATE)), state);
          if (job.state() == FINISHED && job.outcome().state() == 6 && state == 6) waiting++;
        } else if (read(connection, new Key(identity, PROCESS)) == null) {
          throw Wire.integrity("rehydration row has no processing owner");
        }
        boolean futureRehydration = job.state() == RESERVED;
        if (futureRehydration) reserved += charge;
        else retained += charge;
        long scopeBytes = sessionBytes.merge(identity.session(), charge, Long::sum);
        if (retained + reserved > MAX_RETAINED || scopeBytes > MAX_SESSION_RETAINED || sessionBytes.size() > 512) {
          throw admission ? Wire.limit("durable job completion-byte capacity exhausted")
              : Wire.integrity("retained jobs and completion reservations exceed policy");
        }
        if (job.state() < FINISHED && job.key().kind() == PROCESS && (++processing > MAX_QUEUED
            || sessionQueued.merge(identity.session(), 1, Integer::sum) > MAX_SESSION_QUEUED)) {
          throw admission ? Wire.limit("durable processing queue is full")
              : Wire.integrity("unfinished processing jobs exceed policy");
        }
        boolean activeRehydration = job.state() < FINISHED && job.key().kind() == REHYDRATE;
        if (activeRehydration) rehydrating++;
        if (futureRehydration) futureSlots++;
        if ((futureRehydration || activeRehydration) && (futureSlots + rehydrating > MAX_COMPLETIONS
            || sessionCompletions.merge(identity.session(), 1, Integer::sum) > MAX_SESSION_COMPLETIONS)) {
          throw admission ? Wire.limit("durable completion reservations are full")
              : Wire.integrity("durable completion reservations exceed entity policy");
        }
      }
    }
    return new SealedSessionStore.JobUsage(retained, reserved, processing, rehydrating, futureSlots, waiting);
  }

  private static void auditPair(Connection connection, Job process, Job future, int entityState)
      throws SQLException, ProtocolException {
    if (future == null) throw Wire.integrity("processing job lost its preallocated rehydration row");
    Input original = process.input(), bound = future.input();
    if (!Arrays.equals(encode(original), encode(new Input(bound.identity(), bound.header(), bound.length(), bound.digest(), null)))) {
      throw Wire.integrity("preallocated rehydration input differs from processing input");
    }
    if (process.state() < FINISHED) {
      if (future.state() != RESERVED) throw Wire.integrity("unfinished processing has no future reservation");
    } else if (process.state() == REFUSED || process.outcome().state() != 6) {
      if (future.state() != RETIRED) throw Wire.integrity("terminal processing retained a runnable future");
    } else if (entityState == 6) {
      if (future.state() != RESERVED) throw Wire.integrity("waiting parent has no future reservation");
    } else if (entityState == 4 && future.state() == RETIRED) {
      byte[] closure = SealedSessionStore.childClosure(connection, original.identity().session(), original.identity().entity());
      if (closure == null || SealedScope.decode(closure).failed().signum() == 0) throw Wire.integrity("retired parent has no failed child closure");
    } else {
      if ((entityState != 7 && entityState != 3 && entityState != 4) || future.state() >= RESERVED) {
        throw Wire.integrity("resolved parent has no active rehydration record");
      }
      byte[] closure = SealedSessionStore.childClosure(connection, original.identity().session(), original.identity().entity());
      if (closure == null || bound.child() == null || !Arrays.equals(closure, SealedScope.encode(bound.child()))) {
        throw Wire.integrity("rehydration input differs from closed children");
      }
    }
  }

  private static Job decode(ResultSet rows) throws SQLException, ProtocolException {
    byte[] descriptor = rows.getBytes(5), inputHash = rows.getBytes(6), image = rows.getBytes(7);
    if (image == null || image.length != RESERVED_OUTCOME || !Arrays.equals(Arrays.copyOf(image, 8), IMAGE_MAGIC)
        || descriptor == null) throw Wire.integrity("job descriptor or image version/length differs");
    int storedState = ByteBuffer.wrap(image).getInt(STATE_OFFSET);
    if (storedState == RESERVED || storedState == RETIRED) {
      if (descriptor.length <= CHILD_DESCRIPTOR_BYTES) throw Wire.integrity("reserved descriptor capacity is invalid");
      int originalLength = descriptor.length - CHILD_DESCRIPTOR_BYTES;
      for (int offset = originalLength; offset < descriptor.length; offset++) {
        if (descriptor[offset] != 0) throw Wire.integrity("reserved descriptor padding is not zero");
      }
      descriptor = Arrays.copyOf(descriptor, originalLength);
    }
    if (!MessageDigest.isEqual(SealedWork.sha256().digest(descriptor), inputHash)) throw Wire.integrity("job descriptor checksum differs");
    Input input = decodeInput(descriptor);
    Key key = new Key(input.identity(), rows.getInt(4));
    if (!key.identity().session().equals(rows.getString(1)) || key.identity().entity().scopeId() != rows.getLong(2)
        || key.identity().entity().entityId() != rows.getLong(3) || (key.kind() != PROCESS && key.kind() != REHYDRATE)
        || (key.kind() == REHYDRATE && storedState < RESERVED) != (input.child() != null)
        || (storedState >= RESERVED && key.kind() != REHYDRATE)) throw Wire.integrity("job descriptor binding differs");
    if (image.length != RESERVED_OUTCOME || !Arrays.equals(Arrays.copyOf(image, 8), IMAGE_MAGIC)) throw Wire.integrity("job state image version or length differs");
    ByteBuffer fields = ByteBuffer.wrap(image); fields.position(STATE_OFFSET);
    int state = fields.getInt(); long epoch = fields.getLong();
    byte[] workerBytes = new byte[16]; fields.get(workerBytes);
    UUID worker = Arrays.equals(workerBytes, new byte[16]) ? null : uuid(workerBytes);
    long expires = fields.getLong(); int outcomeLength = fields.getInt();
    if (outcomeLength < 0 || outcomeLength > 128) throw Wire.integrity("job outcome exceeds image capacity");
    byte[] outcomeBytes = outcomeLength == 0 ? null : Arrays.copyOfRange(image, OUTCOME_OFFSET, OUTCOME_OFFSET + outcomeLength);
    for (int offset = OUTCOME_OFFSET + outcomeLength; offset < CHECKSUM_OFFSET; offset++) {
      if (image[offset] != 0) throw Wire.integrity("job image padding is not zero");
    }
    validateState(state, epoch, worker, expires, outcomeBytes);
    if (!MessageDigest.isEqual(checksum(key, inputHash, state, epoch, worker, expires, outcomeBytes),
        Arrays.copyOfRange(image, CHECKSUM_OFFSET, RESERVED_OUTCOME))) throw Wire.integrity("job state checksum differs");
    Outcome outcome = outcomeBytes == null ? null : decodeOutcome(outcomeBytes);
    if ((state == FINISHED && (outcome == null || outcome.refusal() != null))
        || (state == REFUSED && (outcome == null || outcome.refusal() == null))
        || (state < FINISHED && outcome != null)) throw Wire.integrity("job outcome state differs");
    if (key.kind() == REHYDRATE && outcome != null && outcome.state() == 6) throw Wire.integrity("rehydration outcome cannot dehydrate");
    return new Job(key, input, state, epoch, worker, expires, outcome);
  }

  private static byte[] stateImage(Key key, byte[] hash, int state, long epoch, UUID worker,
      long expires, byte[] outcome) throws ProtocolException {
    validateState(state, epoch, worker, expires, outcome);
    byte[] image = new byte[RESERVED_OUTCOME];
    ByteBuffer fields = ByteBuffer.wrap(image);
    fields.put(IMAGE_MAGIC).putInt(state).putLong(epoch)
        .put(worker == null ? new byte[16] : SealedWork.producerBytes(worker))
        .putLong(expires).putInt(outcome == null ? 0 : outcome.length);
    if (outcome != null) fields.put(outcome);
    fields.position(CHECKSUM_OFFSET);
    fields.put(checksum(key, hash, state, epoch, worker, expires, outcome));
    return image;
  }

  private static void validateState(int state, long epoch, UUID worker, long expires, byte[] outcome)
      throws ProtocolException {
    if (state < QUEUED || state > RETIRED || (outcome != null && (outcome.length < 1 || outcome.length > 128))
        || ((state == QUEUED || state >= RESERVED) && (epoch != 0 || worker != null || expires != 0 || outcome != null))
        || (state > QUEUED && state < RESERVED && (epoch <= 0 || worker == null || worker.equals(new UUID(0, 0)) || expires <= 0
          || (state == RUNNING) != (outcome == null)))) throw Wire.integrity("invalid durable job state image");
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
