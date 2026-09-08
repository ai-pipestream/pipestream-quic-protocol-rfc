package ai.pipestream.quic.v2;

import static ai.pipestream.quic.v2.Messages.*;
import static ai.pipestream.quic.v2.Records.*;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Admission into the Java authority's authenticated writer transaction. Immutable payload and
 * output funding precede the atomic metadata reference; an unreferenced file is not an admitted
 * job. Execution callbacks, output publication and collection are separate lifecycle operations.
 */
final class AdmissionStore {
  /** Safe restart mechanisms that an explicitly configured application must implement. */
  enum RestartSafety {
    /** Repeated invocation has application-defined idempotent effects. */
    IDEMPOTENT,
    /** The application's external effects enforce execution fences. */
    EXTERNALLY_FENCED,
    /** Effects participate in an application-defined transactional protocol. */
    TRANSACTIONAL
  }

  /**
   * An explicit versioned processing contract, never selected through a fallback.
   *
   * @param label configured wire label, including the application's version convention
   * @param modes supported leaf and expansion modes
   * @param safety application guarantee that permits safe restart
   */
  record Application(String label, Set<Integer> modes, RestartSafety safety) {
    /** Validate and freeze a bounded contract. */
    Application {
      Checks.label(label);
      modes = Set.copyOf(modes);
      Objects.requireNonNull(safety);
      ProtocolError.require(!modes.isEmpty() && modes.size() <= 3, "application mode set is empty");
      for (int mode : modes) Checks.range(mode, 0, 2);
    }
  }

  /**
   * Immutable deployment-level executor capacity and configured processing contracts.
   *
   * @param applications explicitly enabled, unique application labels
   * @param maxJobs total simultaneously funded jobs
   * @param maxJobsPerOwner one owner's simultaneously funded jobs across sessions
   */
  record ExecutionPolicy(List<Application> applications, int maxJobs, int maxJobsPerOwner) {
    /** Canonicalize the deployment policy and reject ambiguous contracts. */
    ExecutionPolicy {
      ProtocolError.require(applications.size() <= 16, "too many configured applications");
      applications =
          applications.stream().sorted(Comparator.comparing(Application::label)).toList();
      for (int i = 1; i < applications.size(); i++)
        ProtocolError.require(
            !applications.get(i - 1).label().equals(applications.get(i).label()),
            "duplicate application contract");
      Checks.range(maxJobs, 1, 65536);
      Checks.range(maxJobsPerOwner, 1, maxJobs);
    }

    /**
     * Construct a deployment that permits declarations but cannot admit applications.
     *
     * @return explicit empty processing registry
     */
    static ExecutionPolicy disabled() {
      return new ExecutionPolicy(List.of(), 1, 1);
    }

    /**
     * Append this exact policy to the authority's installation commitment.
     *
     * @param out bounded canonical encoder
     */
    void write(Cbor.Writer out) {
      out.array(3);
      out.number(maxJobs);
      out.number(maxJobsPerOwner);
      out.array(applications.size());
      for (Application application : applications) {
        out.array(3);
        out.text(application.label(), 128);
        int modes = 0;
        for (int mode : application.modes()) modes |= 1 << mode;
        out.number(modes);
        out.number(application.safety().ordinal());
      }
    }

    /**
     * Resolve one explicitly configured processing contract.
     *
     * @param parameters admitted application intent
     * @return exact supported contract
     */
    Application resolve(AdmitParameters parameters) {
      for (Application application : applications)
        if (application.label().equals(parameters.application())
            && application.modes().contains(parameters.mode())) return application;
      throw error(
          ProtocolError.Code.APPLICATION_UNSUPPORTED, "application contract or mode unsupported");
    }
  }

  /**
   * Explicit trust accompanies a UTC sample; a monotonic duration clock is not a UTC substitute.
   *
   * @param utcMillis UTC milliseconds since the Unix epoch
   * @param trusted whether the deployment can currently trust that UTC value
   */
  record Time(long utcMillis, boolean trusted) {}

  /** Local trusted UTC source, never a peer-supplied timestamp. */
  @FunctionalInterface
  interface Clock {
    /**
     * Obtain a fresh reading without blocking on a remote clock service.
     *
     * @return UTC value with explicit current trust
     */
    Time sample();
  }

  /** Local current application policy; checking permission must not perform application effects. */
  @FunctionalInterface
  interface Authorization {
    /**
     * Recheck the current owner's application permission.
     *
     * @param binding authenticated retained session
     * @param parameters exact application request
     */
    void check(Binding binding, AdmitParameters parameters);
  }

  /**
   * Internal commit result; freshness controls clock/deadline checks, not wire receipt contents.
   *
   * @param receipt exact admission evidence
   * @param fresh true only for a new mutation in this transaction
   */
  record Admission(OperationReceipt receipt, boolean fresh) {}

  private record Usage(
      long jobs, long ownerJobs, long sessionJobs, long inputBytes, long outputBytes) {}

  /**
   * Checked job ownership and fixed-image state within one transaction.
   *
   * @param slot owned fixed record
   * @param geometry retained revision and funding
   * @param record decoded job
   */
  record StoredJob(long slot, FixedRecords.Header geometry, JobRecord record) {}

  private AdmissionStore() {}

  /**
   * Create job ownership links in the authority's initialization transaction.
   *
   * @param connection initialization writer
   * @throws SQLException schema failure
   */
  static void createSchema(Connection connection) throws SQLException {
    try (var statement = connection.createStatement()) {
      statement.execute(
          """
          CREATE TABLE ps_v2_jobs (
            generation INTEGER NOT NULL, scope INTEGER NOT NULL, entity INTEGER NOT NULL,
            producer INTEGER NOT NULL CHECK(producer IN (0,1)),
            operation BLOB NOT NULL CHECK(length(operation)=16 AND operation!=zeroblob(16)),
            state_slot INTEGER NOT NULL UNIQUE REFERENCES ps_v2_slots(id),
            PRIMARY KEY(generation,scope,entity), UNIQUE(generation,producer,operation),
            FOREIGN KEY(generation,scope,entity) REFERENCES ps_v2_entities(generation,scope,id),
            FOREIGN KEY(generation,producer,operation) REFERENCES ps_v2_operations(generation,producer,operation)
              DEFERRABLE INITIALLY DEFERRED
          ) STRICT
          """);
    }
  }

  /**
   * Check the header and current state, returning an exact replay without reading its body.
   *
   * @param connection authenticated metadata snapshot
   * @param config immutable deployment policy
   * @param binding retained session
   * @param selected selected transport limits
   * @param header immutable input header
   * @param clock fresh trusted UTC source
   * @param authorization current application policy
   * @return retained receipt or null for new eligible input
   * @throws SQLException corrupted metadata or storage failure
   */
  static OperationReceipt check(
      Connection connection,
      SessionStore.Configuration config,
      Binding binding,
      Capabilities selected,
      InputHeader header,
      Clock clock,
      Authorization authorization)
      throws SQLException {
    return check(connection, config, binding, selected, header, clock, authorization, 0);
  }

  /**
   * Apply common admission checks for an already authorized producer. A producer-one caller must
   * hold the current parent fence in its enclosing transaction, including on receipt replay.
   *
   * @param connection checked metadata transaction
   * @param config immutable deployment policy
   * @param binding authorized retained session
   * @param selected selected transport limits
   * @param header exact input intent
   * @param clock trusted UTC source
   * @param authorization current application policy
   * @param producer authorized operation originator
   * @return retained receipt or null for eligible new input
   * @throws SQLException corrupt retained metadata or storage failure
   */
  static OperationReceipt check(
      Connection connection,
      SessionStore.Configuration config,
      Binding binding,
      Capabilities selected,
      InputHeader header,
      Clock clock,
      Authorization authorization,
      int producer)
      throws SQLException {
    Checks.producer(producer);
    Objects.requireNonNull(header);
    Objects.requireNonNull(clock);
    Objects.requireNonNull(authorization);
    if (header.generation() != binding.generation())
      throw error(ProtocolError.Code.CONFLICT, "input generation differs from attached session");
    AdmitParameters parameters = header.parameters();
    if (parameters.work().producer() != producer)
      throw error(ProtocolError.Code.UNAUTHORIZED, "cannot supply another producer's input");
    authorization.check(binding, parameters);
    DeclarationStore.Operation prior =
        DeclarationStore.operation(connection, binding, producer, header.operation());
    if (prior != null) {
      if (!header.equals(prior.input()))
        throw error(
            ProtocolError.Code.CONFLICT, "operation identity has different immutable parameters");
      Wire.encode(
          new AdmissionResponse(new RequestTag(true, 4611686018427387903L), prior.receipt()),
          selected.controlLimit());
      return prior.receipt();
    }
    config.execution().resolve(parameters);
    Wire.encodeRecord(header, Wire.HEADER_LIMIT);
    if (parameters.outputs().count() != 0 && !selected.supported().contains(RESULT_DELIVERY))
      throw error(
          ProtocolError.Code.EXTENSION_UNSUPPORTED, "output budget requires result delivery");
    if (parameters.executionMs() > binding.policy().executionLimit()
        || parameters.input().length() > selected.objectLimit()
        || responseCapacity(parameters.outputs().count()) > selected.controlLimit())
      throw ProtocolError.limit("input duration, object size or promised response exceeds policy");
    long now = now(connection, binding.authority(), clock);
    long deadline = add(now, parameters.executionMs());
    add(deadline, binding.policy().receiptRetention());
    add(deadline, binding.policy().outputRetention());
    DeclarationStore.Entity entity;
    try {
      entity = DeclarationStore.member(connection, binding, parameters.work());
    } catch (ProtocolError refusal) {
      if (refusal.code() == ProtocolError.Code.NOT_FOUND)
        throw error(ProtocolError.Code.CONFLICT, "input work is undeclared");
      throw refusal;
    }
    if (entity.view().state() == State.CANCELLING)
      throw error(ProtocolError.Code.CANCELLED, "work cancellation fenced input");
    if (entity.view().state().terminal())
      throw error(ProtocolError.Code.ALREADY_TERMINAL, "work already terminal");
    if (entity.view().state() != State.DECLARED)
      throw error(ProtocolError.Code.CONFLICT, "work already admitted under another operation");
    ancestors(connection, binding, parameters.work().scope());
    Usage usage = usage(connection, binding);
    if (usage.jobs() >= config.execution().maxJobs()
        || usage.ownerJobs() >= config.execution().maxJobsPerOwner()
        || usage.sessionJobs() >= binding.limits().activeJobs()
        || parameters.input().length() > binding.limits().inputBytes() - usage.inputBytes()
        || parameters.outputs().totalBytes() > binding.limits().outputBytes() - usage.outputBytes())
      throw ProtocolError.limit("retained input, output or executor capacity");
    try (var query =
        connection.prepareStatement(
            """
            SELECT operation_count,last_scope,(SELECT count(*) FROM ps_v2_scopes WHERE generation=?)
              FROM ps_v2_sessions WHERE generation=?
            """)) {
      query.setLong(1, binding.generation());
      query.setLong(2, binding.generation());
      try (var row = query.executeQuery()) {
        if (!row.next()) throw corrupt("session accounting missing");
        if (row.getLong(1) >= binding.limits().operations())
          throw ProtocolError.limit("session operation capacity");
        if (parameters.mode() != 0
            && (row.getLong(3) >= binding.limits().scopes() || row.getLong(2) == Long.MAX_VALUE))
          throw ProtocolError.limit("child scope capacity or allocator exhausted");
      }
    }
    return null;
  }

  /**
   * Install all local admission commitments within an already checked writer transaction.
   *
   * @param connection authenticated writer transaction
   * @param config immutable deployment policy
   * @param binding retained session
   * @param selected selected transport limits
   * @param inputs exclusively held paired storage
   * @param header exact input intent
   * @param clock trusted UTC source
   * @param authorization current application policy
   * @return receipt and whether this transaction newly admitted work
   * @throws IOException missing or corrupt input, funding or synchronization failure
   * @throws SQLException metadata corruption or storage failure
   */
  static Admission admit(
      Connection connection,
      SessionStore.Configuration config,
      Binding binding,
      Capabilities selected,
      InputStore inputs,
      InputHeader header,
      Clock clock,
      Authorization authorization)
      throws IOException, SQLException {
    return admit(connection, config, binding, selected, inputs, header, clock, authorization, 0);
  }

  /**
   * Commit common input and resource promises for an explicitly authorized producer. The enclosing
   * transaction must retain the local parent fence for producer one through commitment.
   *
   * @param connection checked writer transaction
   * @param config immutable deployment policy
   * @param binding authorized retained session
   * @param selected selected transport limits
   * @param inputs exclusively held paired storage
   * @param header immutable input intent
   * @param clock trusted UTC source
   * @param authorization current application policy
   * @param producer authorized operation originator
   * @return retained receipt and whether admission is new
   * @throws IOException missing input, funding or synchronization failure
   * @throws SQLException corrupt metadata or storage failure
   */
  static Admission admit(
      Connection connection,
      SessionStore.Configuration config,
      Binding binding,
      Capabilities selected,
      InputStore inputs,
      InputHeader header,
      Clock clock,
      Authorization authorization,
      int producer)
      throws IOException, SQLException {
    OperationReceipt prior =
        check(connection, config, binding, selected, header, clock, authorization, producer);
    if (prior != null) return new Admission(prior, false);
    inputs.requireExecutionHandles(header.parameters());
    Commitments.Context context = context(binding);
    InputStore.Stored input =
        inputs
            .find(context, header)
            .orElseThrow(
                () ->
                    error(ProtocolError.Code.NOT_READY, "complete validated input is unavailable"));
    InputStore.Reservation funding = inputs.reserveOutputs(context, header);
    // Filesystem installation can take time; preflight was not an execution interval promise.
    check(connection, config, binding, selected, header, clock, authorization, producer);
    long admittedAt = now(connection, binding.authority(), clock);
    AdmitParameters parameters = header.parameters();
    long deadline = add(admittedAt, parameters.executionMs());
    add(deadline, binding.policy().receiptRetention());
    add(deadline, binding.policy().outputRetention());
    DeclarationStore.Entity entity =
        DeclarationStore.member(connection, binding, parameters.work());
    FixedRecords.protect(connection, config.files());
    byte[] workKey =
        FixedRecords.key(
            binding,
            FixedRecords.Kind.WORK,
            parameters.work().scope(),
            parameters.work().producer(),
            parameters.work().entity(),
            entity.declaration().bytes());
    FixedRecords.grow(
        connection,
        config.files(),
        entity.slot(),
        FixedRecords.Kind.WORK,
        workKey,
        entity.revision(),
        Math.max(4096, responseCapacity(parameters.outputs().count())),
        Math.max(FixedRecords.ADMITTED_WORK_CREDITS, entity.geometry().credits()));
    ChildScope child =
        parameters.mode() == 0 ? null : allocateChild(connection, config, binding, parameters);
    WorkView view =
        new WorkView(
            parameters.work(),
            parameters.mode() == 1 ? State.WAITING_CHILDREN : State.ACTIVE,
            1,
            parameters.input(),
            admittedAt,
            deadline,
            null,
            null,
            null,
            child,
            null,
            null);
    JobRecord job =
        new JobRecord(
            header,
            config.execution().resolve(parameters).safety(),
            1,
            0,
            null,
            parameters.mode() == 1 ? JobRecord.Stage.WAITING_CHILDREN : JobRecord.Stage.QUEUED,
            input.reference(),
            funding.reference(),
            Math.min(selected.objectLimit(), parameters.outputs().totalBytes()),
            true,
            true,
            true,
            parameters.mode() != 2,
            null,
            null);
    long slot =
        FixedRecords.allocate(
            connection,
            config.files(),
            FixedRecords.Kind.JOB,
            jobKey(context, header),
            job.encode(),
            FixedRecords.JOB_CAPACITY,
            FixedRecords.JOB_CREDITS);
    try (var insert =
        connection.prepareStatement(
            """
            INSERT INTO ps_v2_jobs(generation,scope,entity,producer,operation,state_slot) VALUES (?,?,?,?,?,?)
            """)) {
      insert.setLong(1, binding.generation());
      insert.setLong(2, parameters.work().scope());
      insert.setLong(3, parameters.work().entity());
      insert.setInt(4, parameters.work().producer());
      insert.setBytes(5, header.operation().bytes());
      insert.setLong(6, slot);
      insert.executeUpdate();
    }
    FixedRecords.replace(
        connection,
        config.files(),
        entity.slot(),
        FixedRecords.Kind.WORK,
        workKey,
        entity.revision(),
        Wire.encodeRecord(view, Wire.MAX_CONTROL_LIMIT),
        false);
    OperationReceipt receipt =
        new OperationReceipt(
            header.operation(),
            Commitments.operation(context, producer, header),
            new Admitted(parameters.work(), 1, admittedAt, deadline, child));
    Wire.encode(
        new AdmissionResponse(new RequestTag(true, 4611686018427387903L), receipt),
        selected.controlLimit());
    DeclarationStore.retainAdmission(connection, binding, header, receipt);
    try (var update =
        connection.prepareStatement(
            """
            UPDATE ps_v2_sessions SET operation_count=operation_count+1,
              required_control=max(required_control,?),required_object=max(required_object,?) WHERE generation=?
            """)) {
      update.setInt(1, Math.max(4096, responseCapacity(parameters.outputs().count())));
      update.setLong(2, Math.max(parameters.input().length(), job.objectLimit()));
      update.setLong(3, binding.generation());
      if (update.executeUpdate() != 1) throw corrupt("admission session disappeared");
    }
    // Exact file lookup is owner-qualified and re-establishes durability, never a raw path open.
    if (!inputs
        .findReservation(context, header)
        .orElseThrow(() -> new IOException("output funding missing"))
        .reference()
        .equals(job.outputReference())) throw corrupt("output funding reference differs");
    return new Admission(receipt, true);
  }

  /**
   * Last time/application checks after filesystem synchronization and immediately before commit.
   * Replays are observations and do not extend intervals or require a currently executable job.
   *
   * @param connection admission transaction
   * @param config immutable deployment policy
   * @param binding authenticated retained session
   * @param header exact intent
   * @param clock fresh trusted UTC source
   * @param authorization current application permission
   * @param fresh whether this transaction promises a new admission
   * @throws SQLException clock image corruption or failed durable clock update
   */
  static void beforeCommit(
      Connection connection,
      SessionStore.Configuration config,
      Binding binding,
      InputHeader header,
      Clock clock,
      Authorization authorization,
      boolean fresh)
      throws SQLException {
    authorization.check(binding, header.parameters());
    if (fresh) {
      long utc = now(connection, binding.authority(), clock);
      checkAdmissionInterval(connection, binding, header, utc);
      remember(connection, config, binding.authority(), utc);
    }
  }

  /**
   * Check a new admission's interval and ancestry at the enclosing transaction's final clock
   * sample. A local producer must validate both this child interval and its parent ownership.
   *
   * @param connection checked admission transaction
   * @param binding retained session
   * @param header exact newly admitted input
   * @param utc final trusted UTC sample
   * @throws SQLException corrupt admission or ancestor evidence
   */
  static void checkAdmissionInterval(
      Connection connection, Binding binding, InputHeader header, long utc) throws SQLException {
    WorkView view = DeclarationStore.member(connection, binding, header.parameters().work()).view();
    if (view.admittedAt() == null || view.deadline() == null)
      throw corrupt("new admission lacks its interval");
    if (utc < view.admittedAt())
      throw error(ProtocolError.Code.CLOCK_UNSAFE, "UTC moved behind admission");
    if (utc >= view.deadline())
      throw error(ProtocolError.Code.DEADLINE_EXCEEDED, "input commit passed execution deadline");
    ancestors(connection, binding, header.parameters().work().scope());
  }

  /**
   * Fence caller membership against its parent admission without requiring execution ownership.
   * Producer-one declarations require their separate local worker interface, never this caller API.
   *
   * @param connection checked metadata transaction
   * @param binding authenticated session
   * @param scope scope to receive new caller membership
   * @throws SQLException corrupt metadata
   */
  static void checkDeclaration(Connection connection, Binding binding, long scope)
      throws SQLException {
    DeclarationStore.Scope target = DeclarationStore.scope(connection, binding, scope);
    if (target.producer() != 0)
      throw error(ProtocolError.Code.UNAUTHORIZED, "caller cannot declare producer-one work");
    ancestors(connection, binding, scope);
  }

  /**
   * Persist a checked UTC watermark without consuming another record's credits.
   *
   * @param connection writer transaction
   * @param config immutable file policy
   * @param authority clock owner
   * @param utc trusted nondecreasing time
   * @throws SQLException corrupt clock or failed write
   */
  static void remember(
      Connection connection, SessionStore.Configuration config, String authority, long utc)
      throws SQLException {
    FixedRecords.Snapshot clockImage = clockImage(connection, authority);
    Cbor.Writer value = new Cbor.Writer(FixedRecords.CLOCK_CAPACITY);
    value.number(utc);
    FixedRecords.replace(
        connection,
        config.files(),
        FixedRecords.CLOCK,
        FixedRecords.Kind.CLOCK,
        FixedRecords.clockKey(authority),
        clockImage.header().revision(),
        value.finish(),
        false);
  }

  private static ChildScope allocateChild(
      Connection connection,
      SessionStore.Configuration config,
      Binding binding,
      AdmitParameters parameters)
      throws SQLException {
    long id;
    try (var query =
        connection.prepareStatement("SELECT last_scope FROM ps_v2_sessions WHERE generation=?")) {
      query.setLong(1, binding.generation());
      try (var row = query.executeQuery()) {
        if (!row.next()) throw corrupt("scope allocator missing");
        id = add(row.getLong(1), 1);
      }
    }
    int producer = parameters.mode() == 1 ? 0 : 1;
    ScopeState state =
        new ScopeState(id, producer, parameters.work(), 0, 0, null, false, false, null);
    long slot =
        FixedRecords.allocate(
            connection,
            config.files(),
            FixedRecords.Kind.SCOPE,
            FixedRecords.key(binding, FixedRecords.Kind.SCOPE, id, producer, 0, null),
            state.encode(),
            FixedRecords.SCOPE_CAPACITY,
            FixedRecords.SCOPE_CREDITS);
    try (var insert =
        connection.prepareStatement(
            """
            INSERT INTO ps_v2_scopes(generation,id,producer,parent_scope,parent_producer,parent_entity,state_slot)
              VALUES (?,?,?,?,?,?,?)
            """)) {
      insert.setLong(1, binding.generation());
      insert.setLong(2, id);
      insert.setInt(3, producer);
      insert.setLong(4, parameters.work().scope());
      insert.setInt(5, parameters.work().producer());
      insert.setLong(6, parameters.work().entity());
      insert.setLong(7, slot);
      insert.executeUpdate();
    }
    try (var update =
        connection.prepareStatement("UPDATE ps_v2_sessions SET last_scope=? WHERE generation=?")) {
      update.setLong(1, id);
      update.setLong(2, binding.generation());
      if (update.executeUpdate() != 1) throw corrupt("scope allocator disappeared");
    }
    return new ChildScope(id, producer);
  }

  /**
   * Check actual ancestry and all accepted exclusion fences, not parent deadlines.
   *
   * @param connection metadata snapshot
   * @param binding retained session
   * @param scopeId scope to check
   * @throws SQLException contradictory parent metadata
   */
  static void ancestors(Connection connection, Binding binding, long scopeId) throws SQLException {
    long remaining = binding.limits().scopes();
    while (true) {
      if (remaining-- == 0) throw corrupt("ancestor chain exceeds scope bound");
      DeclarationStore.Scope scope = DeclarationStore.scope(connection, binding, scopeId);
      if (scope.state().revoked())
        throw error(ProtocolError.Code.UNAUTHORIZED, "session scope revoked");
      if (scope.state().cancelled())
        throw error(ProtocolError.Code.CANCELLED, "ancestor scope cancelled");
      if (scope.parent() == null) return;
      WorkView parent = DeclarationStore.member(connection, binding, scope.parent()).view();
      if (parent.child() == null
          || parent.child().scope() != scope.id()
          || parent.child().producer() != scope.producer())
        throw corrupt("child scope contradicts parent admission");
      if (parent.state() == State.CANCELLING
          || parent.state() == State.CANCELLED
          || parent.state() == State.SKIPPED)
        throw error(ProtocolError.Code.CANCELLED, "parent excludes new descendants");
      if (parent.deadline() == null) throw corrupt("child parent has no admitted interval");
      scopeId = scope.parent().scope();
    }
  }

  private static Usage usage(Connection connection, Binding binding) throws SQLException {
    long jobs = 0, ownerJobs = 0, sessionJobs = 0, inputs = 0, outputs = 0;
    try (var query = connection.createStatement();
        var rows =
            query.executeQuery(
                """
                SELECT j.generation,CASE WHEN length(CAST(s.owner AS BLOB)) BETWEEN 1 AND 128 THEN s.owner END,
                  j.scope,j.entity,j.producer,
                  CASE WHEN length(j.operation)=16 THEN j.operation END,j.state_slot
                  FROM ps_v2_jobs j JOIN ps_v2_sessions s ON s.generation=j.generation
                """)) {
      while (rows.next()) {
        Commitments.Context owner;
        try {
          owner = new Commitments.Context(binding.authority(), rows.getString(2), rows.getLong(1));
        } catch (ProtocolError invalid) {
          throw new SQLException("V2 admission: invalid retained job owner", invalid);
        }
        JobRecord job = readRow(connection, owner, rows, 3).record();
        if (job.executorLive()) {
          jobs = add(jobs, 1);
          if (owner.owner().equals(binding.owner())) ownerJobs = add(ownerJobs, 1);
          if (owner.generation() == binding.generation()) sessionJobs = add(sessionJobs, 1);
        }
        if (owner.generation() == binding.generation()) {
          if (job.inputLive()) inputs = add(inputs, job.input().parameters().input().length());
          if (job.outputsLive())
            outputs = add(outputs, job.input().parameters().outputs().totalBytes());
        }
      }
    }
    return new Usage(jobs, ownerJobs, sessionJobs, inputs, outputs);
  }

  private static StoredJob readRow(
      Connection connection, Commitments.Context context, ResultSet row, int first)
      throws SQLException {
    long scope = row.getLong(first), entity = row.getLong(first + 1);
    int producer = row.getInt(first + 2);
    byte[] operation = row.getBytes(first + 3);
    long slot = row.getLong(first + 4);
    if (operation == null || operation.length != 16) throw corrupt("job operation identity length");
    FixedRecords.Snapshot image =
        FixedRecords.read(
            connection,
            slot,
            FixedRecords.Kind.JOB,
            FixedRecords.key(context, FixedRecords.Kind.JOB, scope, producer, entity, operation));
    JobRecord job = JobRecord.decode(image.body());
    if (job.input().generation() != context.generation()
        || job.input().parameters().work().scope() != scope
        || job.input().parameters().work().entity() != entity
        || job.input().parameters().work().producer() != producer
        || !Arrays.equals(job.input().operation().bytes(), operation))
      throw corrupt("job identity differs from owner row");
    return new StoredJob(slot, image.header(), job);
  }

  /**
   * Read one owned job without scheduling it or changing its lease.
   *
   * @param connection metadata snapshot
   * @param binding retained owner
   * @param work logical work key
   * @return checked job, or null before admission
   * @throws SQLException corrupt retained job
   */
  static StoredJob job(Connection connection, Binding binding, WorkKey work) throws SQLException {
    try (var query =
        connection.prepareStatement(
            """
            SELECT scope,entity,producer,CASE WHEN length(operation)=16 THEN operation END,state_slot
              FROM ps_v2_jobs WHERE generation=? AND scope=? AND entity=?
            """)) {
      query.setLong(1, binding.generation());
      query.setLong(2, work.scope());
      query.setLong(3, work.entity());
      try (var row = query.executeQuery()) {
        if (!row.next()) return null;
        return readRow(connection, context(binding), row, 1);
      }
    }
  }

  /**
   * Cross-check replay evidence against both its durable job and admitted work identity.
   *
   * @param connection retained metadata snapshot
   * @param binding exact session binding
   * @param input original input intent
   * @param admitted immutable admission outcome
   * @throws SQLException missing or contradictory committed state
   */
  static void validateReceipt(
      Connection connection, Binding binding, InputHeader input, Admitted admitted)
      throws SQLException {
    StoredJob stored = job(connection, binding, input.parameters().work());
    if (stored == null || !stored.record().input().equals(input))
      throw corrupt("admission receipt lacks matching job");
    WorkView view = DeclarationStore.member(connection, binding, input.parameters().work()).view();
    if (!admitted.work().equals(input.parameters().work())
        || admitted.attempt() != 1
        || !Objects.equals(view.input(), input.parameters().input())
        || !Objects.equals(view.admittedAt(), admitted.admittedAt())
        || !Objects.equals(view.deadline(), admitted.deadline())
        || !Objects.equals(view.child(), admitted.child())
        || admitted.deadline() - admitted.admittedAt() != input.parameters().executionMs())
      throw corrupt("admission outcome contradicts work view or interval");
  }

  /**
   * Audit actual funded jobs and all membership-to-job links before accepting recovered capacity.
   *
   * @param connection recovery snapshot
   * @param config exact deployment policy
   * @param binding retained session
   * @throws SQLException corruption, unsupported state or broken funding commitments
   */
  static void audit(Connection connection, SessionStore.Configuration config, Binding binding)
      throws SQLException {
    Usage usage = usage(connection, binding);
    if (usage.jobs() > config.execution().maxJobs()
        || usage.ownerJobs() > config.execution().maxJobsPerOwner()
        || usage.sessionJobs() > binding.limits().activeJobs()
        || usage.inputBytes() > binding.limits().inputBytes()
        || usage.outputBytes() > binding.limits().outputBytes())
      throw corrupt("retained job accounting exceeds policy");
    long lastScope, requiredControl, requiredObject;
    int profiles;
    long watermark = watermark(connection, binding.authority());
    try (var query =
        connection.prepareStatement(
            """
            SELECT last_scope,required_control,required_object,
              (SELECT count(*) FROM ps_v2_scopes WHERE generation=?),
              (SELECT max(id) FROM ps_v2_scopes WHERE generation=?),profiles FROM ps_v2_sessions WHERE generation=?
            """)) {
      query.setLong(1, binding.generation());
      query.setLong(2, binding.generation());
      query.setLong(3, binding.generation());
      try (var row = query.executeQuery()) {
        if (!row.next()) throw corrupt("admission accounting missing");
        lastScope = row.getLong(1);
        requiredControl = row.getLong(2);
        requiredObject = row.getLong(3);
        profiles = row.getInt(6);
        if (row.getLong(4) > binding.limits().scopes() || lastScope != row.getLong(5))
          throw corrupt("child scope allocator or capacity differs");
      }
    }
    try (var query =
        connection.prepareStatement("SELECT id FROM ps_v2_scopes WHERE generation=? AND id>0")) {
      query.setLong(1, binding.generation());
      try (var rows = query.executeQuery()) {
        while (rows.next()) {
          DeclarationStore.Scope child =
              DeclarationStore.scope(connection, binding, rows.getLong(1));
          if (child.parent() == null) throw corrupt("child scope lacks parent identity");
          WorkView parent;
          try {
            parent = DeclarationStore.member(connection, binding, child.parent()).view();
          } catch (ProtocolError invalid) {
            throw new SQLException("V2 admission: child parent is missing", invalid);
          }
          if (parent.child() == null
              || parent.child().scope() != child.id()
              || parent.child().producer() != child.producer())
            throw corrupt("child scope lacks its parent's matching admission");
        }
      }
    }
    try (var query =
        connection.prepareStatement(
            "SELECT scope,producer,id FROM ps_v2_entities WHERE generation=?")) {
      query.setLong(1, binding.generation());
      try (var rows = query.executeQuery()) {
        while (rows.next()) {
          WorkKey work = new WorkKey(rows.getLong(1), rows.getInt(2), rows.getLong(3));
          DeclarationStore.Entity entity = DeclarationStore.member(connection, binding, work);
          StoredJob stored = job(connection, binding, work);
          if ((stored == null) != (entity.view().input() == null))
            throw corrupt("admitted work/job coverage differs");
          FenceStore.auditWork(connection, binding, entity);
          if (stored == null) continue;
          JobRecord record = stored.record();
          AdmitParameters parameters = record.input().parameters();
          try {
            parameters.validateProfiles((profiles & 2) != 0);
            entity.view().validateProfiles((profiles & 2) != 0);
          } catch (ProtocolError invalid) {
            throw new SQLException("V2 admission: retained profile/record contradiction", invalid);
          }
          Application application;
          try {
            application = config.execution().resolve(parameters);
          } catch (ProtocolError invalid) {
            throw new SQLException("V2 admission: retained application unsupported", invalid);
          }
          if (record.safety() != application.safety()
              || parameters.executionMs() > binding.policy().executionLimit()
              || record.objectLimit() > requiredObject
              || parameters.input().length() > requiredObject
              || responseCapacity(parameters.outputs().count()) > requiredControl
              || entity.geometry().capacity()
                  < Math.max(4096, responseCapacity(parameters.outputs().count()))
              || stored.geometry().capacity() < FixedRecords.JOB_CAPACITY)
            throw corrupt("admitted job has unfunded metadata or changed execution contract");
          WorkView view = entity.view();
          if (view.admittedAt() == null || view.admittedAt() > watermark)
            throw corrupt("admitted time exceeds durable UTC watermark");
          try {
            add(view.deadline(), binding.policy().receiptRetention());
            add(view.deadline(), binding.policy().outputRetention());
          } catch (ProtocolError invalid) {
            throw new SQLException("V2 admission: retained retention promise overflows", invalid);
          }
          ExecutionStore.audit(connection, binding, entity, stored, watermark);
          if (parameters.mode() == 0
              ? view.child() != null
              : view.child() == null || view.child().producer() != (parameters.mode() == 1 ? 0 : 1))
            throw corrupt("child allocation differs from mode");
          if (view.child() != null) {
            DeclarationStore.Scope child =
                DeclarationStore.scope(connection, binding, view.child().scope());
            if (!work.equals(child.parent()) || child.producer() != view.child().producer())
              throw corrupt("child metadata contradicts parent membership");
          }
          DeclarationStore.Operation receipt =
              DeclarationStore.operation(
                  connection, binding, work.producer(), record.input().operation());
          if (receipt == null || !record.input().equals(receipt.input()))
            throw corrupt("job admission receipt missing");
        }
      }
    }
  }

  /**
   * Verify every retained file link for one session during paired-store recovery.
   *
   * @param connection checked metadata snapshot
   * @param binding retained session
   * @param inputs exclusively held paired input/output-funding store
   * @throws IOException missing or corrupted authoritative files
   * @throws SQLException corrupt metadata
   */
  static void verifyStorage(Connection connection, Binding binding, InputStore inputs)
      throws IOException, SQLException {
    try (var query =
        connection.prepareStatement(
            """
            SELECT scope,entity,producer,CASE WHEN length(operation)=16 THEN operation END,state_slot
              FROM ps_v2_jobs WHERE generation=?
            """)) {
      query.setLong(1, binding.generation());
      try (var rows = query.executeQuery()) {
        while (rows.next()) {
          JobRecord record = readRow(connection, context(binding), rows, 1).record();
          WorkView view =
              DeclarationStore.member(connection, binding, record.input().parameters().work())
                  .view();
          RetentionStore.audit(
              connection, binding, view, record, watermark(connection, binding.authority()));
          var input = inputs.find(context(binding), record.input());
          if (!inputs
              .inputReference(context(binding), record.input())
              .equals(record.inputReference()))
            throw corrupt("admitted input reference contradicts retained identity");
          if (input.isPresent() && !input.get().reference().equals(record.inputReference()))
            throw corrupt("admitted input reference differs");
          if (input.isEmpty() && record.inputReleaseAt() == null)
            throw new IOException("admitted input is missing without release evidence");
          if (input.isPresent() && !record.inputLive())
            throw corrupt("refunded input still has an installed name");
          if (!inputs
              .outputReference(context(binding), record.input())
              .equals(record.outputReference()))
            throw corrupt("admitted output reference contradicts retained identity");
          if (record.outputReleaseAt() != null) {
            inputs.verifyRetainedOutputs(context(binding), record, view);
          } else if (record.outputsLive()
              && !inputs
                  .findReservation(context(binding), record.input())
                  .orElseThrow(() -> new IOException("admitted output funding is missing"))
                  .reference()
                  .equals(record.outputReference()))
            throw corrupt("admitted output funding reference differs");
          if (view.state() == State.SUCCEEDED && record.outputReleaseAt() == null)
            PublicationStore.verifyStorage(binding, inputs, view, record);
        }
      }
    }
  }

  /**
   * Conservative bound on the largest future work/control encoding, including maximum labels,
   * locators, digests, counters, times, diagnostic and correlation fields. Bounded by output count,
   * never payload byte length. Each output is below 1280 bytes and the remaining fields below 2048.
   *
   * @param outputs maximum output count promised at admission
   * @return required control body and work-image capacity
   */
  static int responseCapacity(int outputs) {
    Checks.range(outputs, 0, 256);
    return 2048 + outputs * 1280;
  }

  /**
   * Retain the greatest sample within one operation as well as the durable database watermark.
   *
   * @param clock deployment UTC source
   * @return operation-local trust and regression gate
   */
  static Clock checkedClock(Clock clock) {
    Objects.requireNonNull(clock);
    return new Clock() {
      private long greatest = -1;

      @Override
      public Time sample() {
        Time reading = clock.sample();
        if (reading == null
            || !reading.trusted()
            || reading.utcMillis() < 0
            || reading.utcMillis() < greatest)
          throw error(ProtocolError.Code.CLOCK_UNSAFE, "UTC became unsafe during the operation");
        greatest = reading.utcMillis();
        return reading;
      }
    };
  }

  /**
   * Check a fresh UTC sample against the retained watermark.
   *
   * @param connection metadata snapshot
   * @param authority clock owner
   * @param clock trusted local source
   * @return safe current UTC
   * @throws SQLException corrupt retained clock
   */
  static long now(Connection connection, String authority, Clock clock) throws SQLException {
    Time sample = clock.sample();
    if (sample == null || !sample.trusted() || sample.utcMillis() < 0)
      throw error(ProtocolError.Code.CLOCK_UNSAFE, "trusted UTC unavailable");
    long remembered = watermark(connection, authority);
    if (sample.utcMillis() < remembered)
      throw error(ProtocolError.Code.CLOCK_UNSAFE, "UTC precedes durable watermark");
    return sample.utcMillis();
  }

  /**
   * Read the checked durable UTC watermark without obtaining a new clock sample.
   *
   * @param connection consistent metadata snapshot
   * @param authority retained issuer
   * @return greatest committed safe observation
   * @throws SQLException corrupt clock image or failed read
   */
  static long watermark(Connection connection, String authority) throws SQLException {
    Cbor.Reader in =
        new Cbor.Reader(clockImage(connection, authority).body(), FixedRecords.CLOCK_CAPACITY);
    long remembered;
    try {
      remembered = in.number();
      in.end();
    } catch (ProtocolError invalid) {
      throw new SQLException("V2 admission: corrupt UTC watermark", invalid);
    }
    return remembered;
  }

  private static FixedRecords.Snapshot clockImage(Connection connection, String authority)
      throws SQLException {
    return FixedRecords.read(
        connection, FixedRecords.CLOCK, FixedRecords.Kind.CLOCK, FixedRecords.clockKey(authority));
  }

  private static byte[] jobKey(Commitments.Context context, InputHeader input) {
    WorkKey work = input.parameters().work();
    return FixedRecords.key(
        context,
        FixedRecords.Kind.JOB,
        work.scope(),
        work.producer(),
        work.entity(),
        input.operation().bytes());
  }

  private static Commitments.Context context(Binding binding) {
    return new Commitments.Context(binding.authority(), binding.owner(), binding.generation());
  }

  private static long add(long left, long right) {
    if (left < 0 || right < 0 || right > Long.MAX_VALUE - left)
      throw ProtocolError.limit("admission capacity or time exhausted");
    return left + right;
  }

  private static ProtocolError error(ProtocolError.Code code, String detail) {
    return new ProtocolError(code, detail);
  }

  private static SQLException corrupt(String detail) {
    return new SQLException("V2 admission: " + detail);
  }
}
