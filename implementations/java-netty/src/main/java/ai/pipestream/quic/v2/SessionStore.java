package ai.pipestream.quic.v2;

import static ai.pipestream.quic.v2.Messages.*;
import static ai.pipestream.quic.v2.Records.*;

import ai.pipestream.quic.BoundedSqlite;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.Semaphore;

/**
 * Blocking V2 authority identity/creation transactions. This is not a durable-profile endpoint.
 * Session membership, admission, execution and retirement extend this same database; neither V1 nor
 * another implementation's database is accepted. Never call it on a transport event loop.
 */
final class SessionStore {
  private static final int VERSION = 5;
  private static final int MAX_BINDING_BYTES = 1024;
  private static final Set<String> TABLES =
      Set.of(
          "ps_v2_meta",
          "ps_v2_owners",
          "ps_v2_sessions",
          "ps_v2_scopes",
          "ps_v2_entities",
          "ps_v2_operations",
          "ps_v2_jobs",
          "ps_v2_slots");
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
   * @param execution immutable enabled application contracts and executor capacity
   */
  record Configuration(
      String authority,
      Limits sessionLimits,
      Policy maximumPolicy,
      int maxOwners,
      int maxSessions,
      int maxSessionsPerOwner,
      BoundedSqlite.Limits files,
      AdmissionStore.ExecutionPolicy execution) {
    /** Check local configuration bounds and required values. */
    Configuration {
      Checks.identity(authority);
      Objects.requireNonNull(sessionLimits);
      Objects.requireNonNull(maximumPolicy);
      Objects.requireNonNull(files);
      Objects.requireNonNull(execution);
      Checks.range(maxOwners, 1, 65536);
      Checks.range(maxSessions, 1, 65536);
      Checks.range(maxSessionsPerOwner, 1, maxSessions);
    }

    /**
     * Configure a session/declaration store with no enabled processing contracts.
     *
     * @param authority configured issuer
     * @param sessionLimits per-session ceilings
     * @param maximumPolicy maximum accepted lifetimes
     * @param maxOwners retained owner bound
     * @param maxSessions retained session bound
     * @param maxSessionsPerOwner per-owner session bound
     * @param files SQLite file ceilings
     */
    Configuration(
        String authority,
        Limits sessionLimits,
        Policy maximumPolicy,
        int maxOwners,
        int maxSessions,
        int maxSessionsPerOwner,
        BoundedSqlite.Limits files) {
      this(
          authority,
          sessionLimits,
          maximumPolicy,
          maxOwners,
          maxSessions,
          maxSessionsPerOwner,
          files,
          AdmissionStore.ExecutionPolicy.disabled());
    }

    /**
     * Encode the exact local storage policy; this is not a wire message.
     *
     * @return bounded deterministic CBOR policy image
     */
    byte[] encode() {
      Cbor.Writer out = new Cbor.Writer(8192);
      out.array(11);
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
      execution.write(out);
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
      Binding binding,
      int profiles,
      int controlLimit,
      boolean revoked,
      boolean retiring,
      int requiredControl,
      long requiredObject) {}

  private record Metadata(long highWater, UUID identity, UUID inputs) {}

  private final BoundedSqlite database;
  private final Configuration config;
  private final byte[] configBytes;
  private final UUID identity;

  private SessionStore(BoundedSqlite database, Configuration config, UUID identity) {
    this.database = database;
    this.config = config;
    configBytes = config.encode();
    this.identity = identity;
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
      BoundedSqlite database = BoundedSqlite.open(path, config.files());
      UUID identity;
      if (initialize) identity = UUID.randomUUID();
      else {
        try (Connection connection = database.connect()) {
          identity = readMetadata(connection, config.encode()).identity();
        }
      }
      SessionStore store = new SessionStore(database, config, identity);
      store.bootstrap(initialize);
      return store;
    } finally {
      DATABASE_OPERATIONS.release();
    }
  }

  /**
   * Return this database installation's persistent identity, not a protocol authority name.
   *
   * @return immutable local database identity
   */
  UUID identity() {
    return identity;
  }

  /**
   * Commit an immutable two-way binding to input storage created for this database. This local
   * setup operation neither authenticates a caller nor admits work. Repeating the exact binding is
   * safe after an interrupted setup; another input installation cannot replace it.
   *
   * @param inputs exclusively held input store created for this database identity
   * @throws IOException closed input storage, foreign ownership, changed policy or failed sync
   * @throws SQLException conflicting retained binding or database failure
   */
  void bindInputs(InputStore inputs) throws IOException, SQLException {
    inputBinding(inputs, true);
  }

  /**
   * Require an already committed exact input-store binding without adopting an unbound database.
   * The future admission transaction must repeat this check inside its own writer transaction.
   *
   * @param inputs expected live input-store owner
   * @throws IOException closed input storage, foreign ownership, changed policy or failed sync
   * @throws SQLException missing or conflicting binding, corrupt metadata or database failure
   */
  void verifyInputs(InputStore inputs) throws IOException, SQLException {
    inputBinding(inputs, false);
  }

  private void inputBinding(InputStore inputs, boolean bind) throws IOException, SQLException {
    Objects.requireNonNull(inputs);
    synchronized (inputs) {
      inputs.verifyAuthority(identity);
      if (!DATABASE_OPERATIONS.tryAcquire())
        throw ProtocolError.limit("V2 database operation capacity");
      try (Connection connection = database.connect();
          var statement = connection.createStatement()) {
        if (!bind) statement.execute("PRAGMA query_only=ON");
        statement.execute(bind ? "BEGIN IMMEDIATE" : "BEGIN");
        try {
          Metadata metadata = metadata(connection);
          if (metadata.inputs() == null) {
            if (!bind) throw corrupt("input storage has not been bound");
            FixedRecords.protect(connection, config.files());
            try (var update =
                connection.prepareStatement(
                    "UPDATE ps_v2_meta SET input_id=?,identity_hash=? WHERE singleton=1 AND"
                        + " input_id IS NULL")) {
              update.setBytes(1, uuid(inputs.identity()));
              update.setBytes(2, identityHash(configBytes, identity, inputs.identity()));
              if (update.executeUpdate() != 1) throw corrupt("input binding changed during setup");
            }
          } else if (!metadata.inputs().equals(inputs.identity())) {
            throw corrupt("database belongs to a different input installation");
          }
          try (var query = connection.createStatement();
              var rows = query.executeQuery("SELECT generation FROM ps_v2_sessions")) {
            while (rows.next()) {
              Retained session = retained(connection, rows.getLong(1));
              AdmissionStore.verifyStorage(connection, session.binding(), inputs);
            }
          }
          // Keep the input-owner monitor until commit so close cannot invalidate the checked pair.
          inputs.verifyAuthority(identity);
          statement.execute("COMMIT");
        } catch (IOException | SQLException | RuntimeException failure) {
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

          FixedRecords.protect(connection, config.files());
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
          long rootSlot =
              FixedRecords.allocate(
                  connection,
                  config.files(),
                  FixedRecords.Kind.SCOPE,
                  FixedRecords.key(binding, FixedRecords.Kind.SCOPE, 0, 0, 0, null),
                  ScopeState.root().encode(),
                  FixedRecords.SCOPE_CAPACITY,
                  FixedRecords.SCOPE_CREDITS);
          try (var insert =
              connection.prepareStatement(
                  """
                  INSERT INTO ps_v2_scopes(generation,id,producer,parent_scope,parent_producer,parent_entity,
                      state_slot) VALUES (?,0,0,NULL,NULL,NULL,?)
                  """)) {
            insert.setLong(1, generation);
            insert.setLong(2, rootSlot);
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
        (connection, binding) -> {
          boolean replay =
              DeclarationStore.operation(connection, binding, request.operation()) != null;
          if (!replay) AdmissionStore.checkDeclaration(connection, binding, request.scope());
          return DeclarationStore.declare(connection, config.files(), binding, selected, request);
        });
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

  /**
   * Observe an exact committed closure under current caller authorization. This is a snapshot, not
   * a checkpoint wait implementation; the eventual dispatcher must bound waiting separately.
   * Retained immutable evidence is observable without issuing a new clock-based promise.
   *
   * @param access current authenticated owner gate
   * @param selected retained compatible profile selection
   * @param generation attached session
   * @param scope requested scope
   * @param expectedSeal caller's verified membership commitment
   * @return complete immutable summary, never partial progress
   * @throws SQLException inconsistent retained evidence or database failure
   */
  ScopeSummary scopeSummary(
      Access access, Capabilities selected, long generation, long scope, Digest expectedSeal)
      throws SQLException {
    Objects.requireNonNull(expectedSeal);
    Checks.number(scope);
    return sessionTransaction(
        access,
        selected,
        generation,
        false,
        (connection, binding) -> {
          DeclarationStore.Scope retained = DeclarationStore.scope(connection, binding, scope);
          if (retained.seal() == null)
            throw error(ProtocolError.Code.NOT_READY, "scope membership is not sealed");
          if (!retained.seal().equals(expectedSeal))
            throw error(ProtocolError.Code.INTEGRITY_ERROR, "checkpoint membership seal differs");
          if (retained.state().summary() == null)
            throw error(ProtocolError.Code.NOT_READY, "scope has no committed closure");
          ClosureStore.verify(connection, binding, scope);
          return retained.state().summary();
        });
  }

  /**
   * Perform one bounded background closure step, independently of a caller or execution grant.
   * Partial folds are volatile; immutable terminal rows and the sealed membership remain durable.
   * Existing descendant-summary audits retain their documented session-wide streaming cost.
   *
   * @param cursor local discovery/fold progress, not a durable receipt
   * @param limit maximum directly examined members, between one and 256
   * @param clock trusted UTC for a newly committed closure or parent failure
   * @return directly examined records and newly committed outcomes
   * @throws SQLException corrupt evidence or failed storage operation
   */
  ClosureStore.Progress reconcileClosures(
      ClosureStore.Cursor cursor, int limit, AdmissionStore.Clock clock) throws SQLException {
    return reconcileClosures(cursor, limit, clock, phase -> {});
  }

  /**
   * Reconcile closure with trusted local commit-boundary instrumentation.
   *
   * @param cursor local discovery/fold progress
   * @param limit maximum directly examined members
   * @param clock trusted deployment UTC source
   * @param probe bounded local durability instrumentation, not a peer or application callback
   * @return committed progress; an after-commit observation failure can lose this return value
   * @throws SQLException storage or instrumentation failure
   */
  ClosureStore.Progress reconcileClosures(
      ClosureStore.Cursor cursor, int limit, AdmissionStore.Clock clock, ClosureStore.Probe probe)
      throws SQLException {
    Objects.requireNonNull(cursor);
    Objects.requireNonNull(clock);
    Objects.requireNonNull(probe);
    if (limit < 1 || limit > 256)
      throw ProtocolError.limit("closure reconciliation batch capacity");
    synchronized (cursor) {
      if (cursor.installation != null && !cursor.installation.equals(identity))
        throw error(ProtocolError.Code.CONFLICT, "closure cursor belongs to another authority");
      cursor.installation = identity;
      if (!DATABASE_OPERATIONS.tryAcquire())
        throw ProtocolError.limit("V2 database operation capacity");
      try (Connection connection = database.connect();
          var statement = connection.createStatement()) {
        statement.execute("BEGIN IMMEDIATE");
        boolean committed = false;
        try {
          metadata(connection);
          ClosureStore.Position position = ClosureStore.next(connection, cursor);
          if (position == null) {
            statement.execute("COMMIT");
            committed = true;
            ClosureStore.advance(cursor, null);
            return new ClosureStore.Progress(0, 0, 0, 0);
          }
          Retained retained = retained(connection, position.generation());
          if (retained == null) throw corrupt("scope has no retained session");
          if (retained.retiring()) {
            statement.execute("COMMIT");
            committed = true;
            ClosureStore.advance(cursor, position);
            return new ClosureStore.Progress(1, 0, 0, 0);
          }
          Binding binding = retained.binding();
          DeclarationStore.Scope scope =
              DeclarationStore.scope(connection, binding, position.scope());
          ClosureStore.Batch batch = ClosureStore.fold(connection, binding, scope, cursor, limit);
          if (batch.status() == null) {
            statement.execute("COMMIT");
            committed = true;
            return new ClosureStore.Progress(1, batch.inspected(), 0, 0);
          }
          AdmissionStore.Clock checkedClock = AdmissionStore.checkedClock(clock);
          long now = AdmissionStore.now(connection, binding.authority(), checkedClock);
          ScopeSummary summary =
              ClosureStore.publish(
                  connection, config, binding, scope, cursor.scan, batch.status(), now);
          WorkView failed = strictChildFailure(connection, retained, scope, summary, now);
          long committedAt = AdmissionStore.now(connection, binding.authority(), checkedClock);
          if (scope.id() == 0
              && committedAt >= ClosureStore.rootReceiptUntil(binding, summary.closedAt()))
            throw error(
                ProtocolError.Code.CLOCK_UNSAFE, "UTC jump overtook root closure retention");
          if (failed != null) checkTerminalInterval(failed, committedAt);
          AdmissionStore.remember(connection, config, binding.authority(), committedAt);
          probe.at(ClosureStore.Phase.BEFORE_COMMIT);
          statement.execute("COMMIT");
          committed = true;
          ClosureStore.advance(cursor, position);
          probe.at(ClosureStore.Phase.AFTER_COMMIT);
          return new ClosureStore.Progress(1, batch.inspected(), 1, failed == null ? 0 : 1);
        } catch (SQLException | RuntimeException | Error failure) {
          cursor.scan = null;
          if (!committed) rollback(connection, failure);
          throw failure;
        }
      } catch (SQLException failure) {
        cursor.scan = null;
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
  }

  private WorkView strictChildFailure(
      Connection connection,
      Retained retained,
      DeclarationStore.Scope child,
      ScopeSummary summary,
      long now)
      throws SQLException {
    if (child.parent() == null
        || summary.counts().success() == summary.declared()
        || retained.revoked()) return null;
    Binding binding = retained.binding();
    DeclarationStore.Entity parent = DeclarationStore.member(connection, binding, child.parent());
    if (parent.view().state().terminal()) return null;
    try {
      ExecutionStore.eligible(connection, binding, parent.view());
    } catch (ProtocolError refusal) {
      if (refusal.code() == ProtocolError.Code.CANCELLED) return null;
      throw refusal;
    }
    ExecutionStore.Loaded loaded =
        ExecutionStore.load(connection, config, binding, child.parent(), (owner, parameters) -> {});
    return ExecutionStore.fail(
        connection,
        config,
        binding,
        loaded,
        new Diagnostic(
            ProtocolError.Code.CONFLICT.value(), "STRICT child scope contains non-successful work"),
        false,
        now);
  }

  /**
   * Validate a new input header before accepting payload, or replay its retained admission. This
   * does not reserve capacity or authorize a later commit; admission repeats all current checks.
   *
   * @param access current authenticated owner
   * @param selected selected connection capabilities
   * @param generation attached session
   * @param inputs live, already paired input store
   * @param header exact input intent
   * @param clock trusted UTC source, with explicit unsafe readings
   * @param authorization current application authorization, without application effects
   * @return matching retained admission, or empty for a currently admissible new header
   * @throws IOException invalid input-store pairing or storage failure
   * @throws SQLException database corruption or failure
   */
  Optional<OperationReceipt> checkInput(
      Access access,
      Capabilities selected,
      long generation,
      InputStore inputs,
      InputHeader header,
      AdmissionStore.Clock clock,
      AdmissionStore.Authorization authorization)
      throws IOException, SQLException {
    AdmissionStore.Clock checkedClock = AdmissionStore.checkedClock(clock);
    return inputTransaction(
        access,
        selected,
        generation,
        inputs,
        header,
        checkedClock,
        authorization,
        false,
        (connection, binding) ->
            new InputResult<>(
                Optional.ofNullable(
                    AdmissionStore.check(
                        connection,
                        config,
                        binding,
                        selected,
                        header,
                        checkedClock,
                        authorization)),
                false));
  }

  /**
   * Commit validated immutable input, funded completion records, job and admission receipt
   * together. Files installed before a failed database commit remain charged, unauthoritative
   * orphans. No application callback runs here and a returned receipt does not assert successful
   * processing.
   *
   * @param access current authenticated owner
   * @param selected selected connection capabilities
   * @param generation attached session
   * @param inputs live, already paired input store
   * @param header immutable input intent
   * @param streamId actual client input-stream correlation supplied by the transport
   * @param clock trusted UTC source
   * @param authorization current application authorization without side effects
   * @return correlated admission only after durable commit
   * @throws IOException missing/corrupt input, unsafe pairing or file failure
   * @throws SQLException database corruption or failure
   */
  AdmissionResponse admit(
      Access access,
      Capabilities selected,
      long generation,
      InputStore inputs,
      InputHeader header,
      long streamId,
      AdmissionStore.Clock clock,
      AdmissionStore.Authorization authorization)
      throws IOException, SQLException {
    RequestTag tag = new RequestTag(true, streamId);
    AdmissionStore.Clock checkedClock = AdmissionStore.checkedClock(clock);
    return inputTransaction(
        access,
        selected,
        generation,
        inputs,
        header,
        checkedClock,
        authorization,
        true,
        (connection, binding) -> {
          AdmissionStore.Admission result =
              AdmissionStore.admit(
                  connection,
                  config,
                  binding,
                  selected,
                  inputs,
                  header,
                  checkedClock,
                  authorization);
          return new InputResult<>(new AdmissionResponse(tag, result.receipt()), result.fresh());
        });
  }

  /**
   * Commit a fresh durable worker lease after revalidating the exact paired input and funding. An
   * expired lease may be replaced without creating a new wire attempt. This method schedules no
   * callback and exposes no durable-profile endpoint.
   *
   * @param access current retained execution grant, not connection credentials
   * @param generation retained session
   * @param work logical work
   * @param inputs exclusively held paired input store
   * @param leaseMillis positive bounded local ownership duration
   * @param clock trusted UTC source
   * @param authorization current application execution policy
   * @return committed local lease
   * @throws IOException invalid pairing or missing/corrupt input/funding
   * @throws SQLException corrupt metadata or failed commit
   */
  ExecutionStore.Lease claimExecution(
      ExecutionStore.Access access,
      long generation,
      WorkKey work,
      InputStore inputs,
      long leaseMillis,
      AdmissionStore.Clock clock,
      AdmissionStore.Authorization authorization)
      throws IOException, SQLException {
    synchronized (Objects.requireNonNull(inputs)) {
      return executionTransaction(
              access,
              generation,
              work,
              null,
              inputs,
              leaseMillis,
              clock,
              authorization,
              ExecutionStore.Change.CLAIM,
              null,
              null)
          .lease();
    }
  }

  /**
   * Renew still-live ownership without changing its number or the original work deadline.
   *
   * @param access current retained execution grant
   * @param lease original worker fence
   * @param leaseMillis positive desired duration
   * @param clock trusted UTC source
   * @param authorization current application policy
   * @return committed renewed observation of the same local ownership
   * @throws SQLException corrupt metadata or failed commit
   */
  ExecutionStore.Lease renewExecution(
      ExecutionStore.Access access,
      ExecutionStore.Lease lease,
      long leaseMillis,
      AdmissionStore.Clock clock,
      AdmissionStore.Authorization authorization)
      throws SQLException {
    return executeOwned(
            access, lease, leaseMillis, clock, authorization, ExecutionStore.Change.RENEW, null)
        .lease();
  }

  /**
   * Revalidate a worker before scheduling another effect, without manufacturing a new lease.
   *
   * @param access current retained execution grant
   * @param lease worker fence
   * @param clock trusted UTC source
   * @param authorization current application policy
   * @throws SQLException corrupt metadata or failed snapshot
   */
  void checkExecution(
      ExecutionStore.Access access,
      ExecutionStore.Lease lease,
      AdmissionStore.Clock clock,
      AdmissionStore.Authorization authorization)
      throws SQLException {
    executeOwned(access, lease, 0, clock, authorization, ExecutionStore.Change.CHECK, null);
  }

  /**
   * Atomically publish a fenced failure or retryable attempt outcome from prepaid image credits.
   * Byte reservations remain charged until separate reference-safe reclamation; failure is not
   * permission to delete an input or release a still-running physical callback's handles.
   *
   * @param access current retained execution grant
   * @param lease worker fence
   * @param diagnostic bounded application explanation
   * @param retryable whether explicit retry is required instead of terminal failure
   * @param clock trusted UTC source
   * @param authorization current application policy
   * @return committed work view
   * @throws SQLException corrupt metadata or failed atomic settlement
   */
  WorkView failExecution(
      ExecutionStore.Access access,
      ExecutionStore.Lease lease,
      Diagnostic diagnostic,
      boolean retryable,
      AdmissionStore.Clock clock,
      AdmissionStore.Authorization authorization)
      throws SQLException {
    return executeOwned(
            access,
            lease,
            0,
            clock,
            authorization,
            retryable ? ExecutionStore.Change.RETRYABLE : ExecutionStore.Change.FAIL,
            Objects.requireNonNull(diagnostic))
        .work();
  }

  /**
   * Verify already installed outputs and atomically publish a fenced successful outcome. The
   * application must have completed outside this transaction; this method never runs it or infers
   * successful processing from a file's existence. A rejected commit leaves bounded orphan files
   * charged, not a visible success or permission to reclaim their storage.
   *
   * @param access current retained execution grant
   * @param lease current worker fence
   * @param inputs exclusively held paired input/output store
   * @param outputCount exact completed output count, not a prefix selection
   * @param endpoint trusted deployment endpoint for result locators
   * @param clock trusted publication UTC source
   * @param authorization current application execution policy
   * @return committed terminal success and its manifest when results are selected
   * @throws IOException missing, unfinished or corrupt output storage
   * @throws SQLException contradictory metadata or failed atomic commit
   */
  WorkView succeedExecution(
      ExecutionStore.Access access,
      ExecutionStore.Lease lease,
      InputStore inputs,
      int outputCount,
      PublicationStore.Endpoint endpoint,
      AdmissionStore.Clock clock,
      AdmissionStore.Authorization authorization)
      throws IOException, SQLException {
    Objects.requireNonNull(lease);
    synchronized (Objects.requireNonNull(inputs)) {
      return executionTransaction(
              access,
              lease.generation(),
              lease.work(),
              lease,
              inputs,
              0,
              clock,
              authorization,
              ExecutionStore.Change.SUCCEED,
              null,
              new PublicationStore.Request(outputCount, endpoint))
          .work();
    }
  }

  /**
   * Read immutable callback inputs after checking current local ownership.
   *
   * @param access current retained owner grant
   * @param lease committed local worker
   * @param clock trusted UTC source
   * @param authorization current application permission
   * @return admitted parameters, never authority inferred from callback input
   * @throws SQLException unavailable or inconsistent metadata
   */
  ExecutionStore.Details describeExecution(
      ExecutionStore.Access access,
      ExecutionStore.Lease lease,
      AdmissionStore.Clock clock,
      AdmissionStore.Authorization authorization)
      throws SQLException {
    return executeOwned(access, lease, 0, clock, authorization, ExecutionStore.Change.CHECK, null)
        .details();
  }

  /**
   * Page the current parent's exact successful child scope without granting a caller result lease.
   *
   * @param access current retained execution grant
   * @param lease live parent worker
   * @param after exclusive child entity bound
   * @param limit maximum returned members
   * @param clock trusted UTC source
   * @param authorization current parent application permission
   * @return bounded immutable child identities
   * @throws SQLException contradictory metadata or failed observation
   */
  BranchStore.Page children(
      ExecutionStore.Access access,
      ExecutionStore.Lease lease,
      long after,
      int limit,
      AdmissionStore.Clock clock,
      AdmissionStore.Authorization authorization)
      throws SQLException {
    return branchRead(
        access,
        lease,
        clock,
        authorization,
        (connection, binding, parent) ->
            BranchStore.page(connection, binding, parent, after, limit));
  }

  /**
   * Observe a committed direct-child output under the current parent's dependency authorization.
   *
   * @param access current retained execution grant
   * @param lease live parent worker
   * @param entity exact direct child
   * @param index published output index
   * @param clock trusted UTC source
   * @param authorization current parent application permission
   * @return immutable output and producing file identity, not independent read authorization
   * @throws SQLException contradictory metadata or failed observation
   */
  BranchStore.Source childOutput(
      ExecutionStore.Access access,
      ExecutionStore.Lease lease,
      long entity,
      int index,
      AdmissionStore.Clock clock,
      AdmissionStore.Authorization authorization)
      throws SQLException {
    return branchRead(
        access,
        lease,
        clock,
        authorization,
        (connection, binding, parent) ->
            BranchStore.output(connection, config, identity, binding, parent, entity, index));
  }

  private <T> T branchRead(
      ExecutionStore.Access access,
      ExecutionStore.Lease lease,
      AdmissionStore.Clock clock,
      AdmissionStore.Authorization authorization,
      BranchRead<T> read)
      throws SQLException {
    Objects.requireNonNull(access).check();
    Objects.requireNonNull(lease);
    Objects.requireNonNull(authorization);
    if (!access.owner().equals(lease.owner()) || !identity.equals(lease.installation()))
      throw error(
          ProtocolError.Code.UNAUTHORIZED, "parent belongs to another owner or installation");
    AdmissionStore.Clock checkedClock = AdmissionStore.checkedClock(clock);
    if (!DATABASE_OPERATIONS.tryAcquire())
      throw ProtocolError.limit("V2 database operation capacity");
    try (Connection connection = database.connect();
        var statement = connection.createStatement()) {
      statement.execute("PRAGMA query_only=ON");
      statement.execute("BEGIN");
      try {
        access.check();
        metadata(connection);
        Binding binding = visible(connection, lease.generation(), access.owner()).binding();
        ExecutionStore.Loaded parent =
            ExecutionStore.load(connection, config, binding, lease.work(), authorization);
        ExecutionStore.check(
            connection,
            binding,
            parent,
            lease,
            ExecutionStore.Change.CHECK,
            AdmissionStore.now(connection, binding.authority(), checkedClock));
        T result = read.apply(connection, binding, parent);
        access.check();
        authorization.check(binding, parent.stored().record().input().parameters());
        ExecutionStore.check(
            connection,
            binding,
            parent,
            lease,
            ExecutionStore.Change.CHECK,
            AdmissionStore.now(connection, binding.authority(), checkedClock));
        statement.execute("COMMIT");
        return result;
      } catch (SQLException | RuntimeException | Error failure) {
        rollback(connection, failure);
        throw failure;
      }
    } finally {
      DATABASE_OPERATIONS.release();
    }
  }

  @FunctionalInterface
  private interface BranchRead<T> {
    T apply(Connection connection, Binding binding, ExecutionStore.Loaded parent)
        throws SQLException;
  }

  /**
   * Inspect the immutable deployment registry without granting execution.
   *
   * @return retained application and executor policy
   */
  AdmissionStore.ExecutionPolicy executionPolicy() {
    return config.execution();
  }

  /**
   * Discover a bounded page of retained jobs for authority-owned background maintenance. This is
   * not a caller API: it crosses owners without supplying credentials, changing leases, sampling
   * time or authorizing execution. Each sweep fixes an inclusive endpoint so concurrent admissions
   * cannot extend it indefinitely. Jobs admitted behind the cursor appear in the next sweep.
   *
   * @param cursor prior page continuation, null to start a fresh sweep
   * @param limit maximum examined jobs, between one and 64
   * @return checked observations and optional continuation; neither is execution authority
   * @throws SQLException missing or contradictory live records, or database failure
   */
  ExecutionStore.Page scanExecutions(ExecutionStore.ScanCursor cursor, int limit)
      throws SQLException {
    if (limit < 1 || limit > 64) throw ProtocolError.limit("job discovery page capacity");
    if (!DATABASE_OPERATIONS.tryAcquire())
      throw ProtocolError.limit("V2 database operation capacity");
    try (Connection connection = database.connect();
        var statement = connection.createStatement()) {
      statement.execute("PRAGMA query_only=ON");
      statement.execute("BEGIN");
      try {
        metadata(connection);
        ExecutionStore.Position through = cursor == null ? null : cursor.through();
        if (cursor == null) {
          try (var rows =
              statement.executeQuery(
                  "SELECT generation,scope,entity FROM ps_v2_jobs ORDER BY generation DESC,scope"
                      + " DESC,entity DESC LIMIT 1")) {
            if (rows.next()) through = jobPosition(rows);
          }
        }
        if (through == null) {
          statement.execute("COMMIT");
          return new ExecutionStore.Page(List.of(), null);
        }
        String lower = cursor == null ? "" : " AND (generation,scope,entity)>(?,?,?)";
        List<ExecutionStore.Candidate> entries = new ArrayList<>(limit);
        ExecutionStore.Position last = null;
        boolean more = false;
        try (var query =
            connection.prepareStatement(
                "SELECT generation,scope,entity,producer FROM ps_v2_jobs"
                    + " WHERE (generation,scope,entity)<=(?,?,?)"
                    + lower
                    + " ORDER BY generation,scope,entity LIMIT ?")) {
          query.setLong(1, through.generation());
          query.setLong(2, through.scope());
          query.setLong(3, through.entity());
          int parameter = 4;
          if (cursor != null) {
            query.setLong(parameter++, cursor.after().generation());
            query.setLong(parameter++, cursor.after().scope());
            query.setLong(parameter++, cursor.after().entity());
          }
          query.setInt(parameter, limit + 1);
          try (var rows = query.executeQuery()) {
            int examined = 0;
            while (rows.next()) {
              if (examined++ == limit) {
                more = true;
                break;
              }
              last = jobPosition(rows);
              Retained retained = retained(connection, last.generation());
              if (retained == null) throw corrupt("job lacks retained session");
              // Intentional retirement may already have removed this job's linked membership.
              if (retained.retiring()) continue;
              Binding binding = retained.binding();
              WorkKey work = new WorkKey(last.scope(), rows.getInt(4), last.entity());
              ExecutionStore.Loaded loaded =
                  ExecutionStore.load(connection, config, binding, work, (owner, input) -> {});
              JobRecord job = loaded.stored().record();
              WorkView view = loaded.entity().view();
              boolean consistent =
                  switch (job.stage()) {
                    case QUEUED, EXECUTING -> view.state() == State.ACTIVE;
                    case WAITING_CHILDREN -> view.state() == State.WAITING_CHILDREN;
                    case AWAITING_RETRY -> view.state() == State.AWAITING_RETRY;
                    case SETTLED -> view.state().terminal();
                  };
              if (!consistent || view.deadline() == null)
                throw corrupt("job discovery contradicts admitted work state");
              entries.add(
                  new ExecutionStore.Candidate(
                      last,
                      binding.owner(),
                      work,
                      job.stage(),
                      job.input().parameters().mode(),
                      view.deadline(),
                      job.leaseUntil(),
                      !job.expansionComplete() || BranchStore.ready(connection, binding, view)));
            }
          }
        }
        statement.execute("COMMIT");
        return new ExecutionStore.Page(
            entries, more ? new ExecutionStore.ScanCursor(last, through) : null);
      } catch (SQLException | RuntimeException failure) {
        rollback(connection, failure);
        throw failure;
      }
    } finally {
      DATABASE_OPERATIONS.release();
    }
  }

  private static ExecutionStore.Position jobPosition(java.sql.ResultSet row) throws SQLException {
    try {
      return new ExecutionStore.Position(row.getLong(1), row.getLong(2), row.getLong(3));
    } catch (ProtocolError invalid) {
      throw new SQLException("V2 job discovery identity is invalid", invalid);
    }
  }

  private record ExecutionResult(
      ExecutionStore.Lease lease, WorkView work, ExecutionStore.Details details) {
    ExecutionResult(ExecutionStore.Lease lease, WorkView work) {
      this(lease, work, null);
    }
  }

  private ExecutionResult executeOwned(
      ExecutionStore.Access access,
      ExecutionStore.Lease lease,
      long duration,
      AdmissionStore.Clock clock,
      AdmissionStore.Authorization authorization,
      ExecutionStore.Change change,
      Diagnostic diagnostic)
      throws SQLException {
    Objects.requireNonNull(lease);
    try {
      return executionTransaction(
          access,
          lease.generation(),
          lease.work(),
          lease,
          null,
          duration,
          clock,
          authorization,
          change,
          diagnostic,
          null);
    } catch (IOException impossible) {
      throw new SQLException(
          "unexpected filesystem access in metadata-only worker transition", impossible);
    }
  }

  private ExecutionResult executionTransaction(
      ExecutionStore.Access access,
      long generation,
      WorkKey work,
      ExecutionStore.Lease lease,
      InputStore inputs,
      long duration,
      AdmissionStore.Clock clock,
      AdmissionStore.Authorization authorization,
      ExecutionStore.Change change,
      Diagnostic diagnostic,
      PublicationStore.Request publication)
      throws IOException, SQLException {
    Objects.requireNonNull(access).check();
    Objects.requireNonNull(authorization);
    Objects.requireNonNull(work);
    Checks.id(generation);
    if (change == ExecutionStore.Change.CLAIM || change == ExecutionStore.Change.RENEW)
      Checks.id(duration);
    if (lease != null
        && (!access.owner().equals(lease.owner()) || !identity.equals(lease.installation())))
      throw error(
          ProtocolError.Code.UNAUTHORIZED, "worker belongs to another owner or installation");
    AdmissionStore.Clock checkedClock = AdmissionStore.checkedClock(clock);
    boolean write = change != ExecutionStore.Change.CHECK;
    if (!DATABASE_OPERATIONS.tryAcquire())
      throw ProtocolError.limit("V2 database operation capacity");
    try (Connection connection = database.connect();
        var statement = connection.createStatement()) {
      if (!write) statement.execute("PRAGMA query_only=ON");
      statement.execute(write ? "BEGIN IMMEDIATE" : "BEGIN");
      try {
        access.check();
        Metadata metadata = metadata(connection);
        Retained retained = visible(connection, generation, access.owner());
        Binding binding = retained.binding();
        ExecutionStore.Loaded loaded =
            ExecutionStore.load(connection, config, binding, work, authorization);
        long now = AdmissionStore.now(connection, binding.authority(), checkedClock);
        ExecutionStore.check(connection, binding, loaded, lease, change, now);
        if (inputs != null) {
          inputs.verifyAuthority(identity);
          if (!inputs.identity().equals(metadata.inputs()))
            throw corrupt("worker input storage pairing differs");
          ExecutionStore.verifyInput(inputs, binding, loaded);
          inputs.verifyAuthority(identity);
        }
        List<Output> outputs =
            change == ExecutionStore.Change.SUCCEED
                ? PublicationStore.prepare(
                    inputs,
                    binding,
                    loaded,
                    lease,
                    retained.profiles() == 3,
                    Objects.requireNonNull(publication))
                : List.of();
        // Processing never runs in this transaction. Refresh policy and time after input I/O.
        authorization.check(binding, loaded.stored().record().input().parameters());
        now = AdmissionStore.now(connection, binding.authority(), checkedClock);
        ExecutionStore.check(connection, binding, loaded, lease, change, now);
        ExecutionResult result;
        if (change == ExecutionStore.Change.CLAIM || change == ExecutionStore.Change.RENEW) {
          result =
              new ExecutionResult(
                  ExecutionStore.lease(
                      connection, config, identity, binding, loaded, change, duration, now),
                  null);
        } else if (change == ExecutionStore.Change.CHECK) {
          result = new ExecutionResult(null, loaded.entity().view());
        } else if (change == ExecutionStore.Change.SUCCEED) {
          result =
              new ExecutionResult(
                  null,
                  ExecutionStore.succeed(
                      connection, config, binding, loaded, retained.profiles() == 3, outputs, now));
        } else {
          result =
              new ExecutionResult(
                  null,
                  ExecutionStore.fail(
                      connection,
                      config,
                      binding,
                      loaded,
                      diagnostic,
                      change == ExecutionStore.Change.RETRYABLE,
                      now));
        }
        // Recheck elapsed storage work against the pre-transition fence, never against a renewal
        // which could otherwise hide expiry during its own transaction.
        access.check();
        authorization.check(binding, loaded.stored().record().input().parameters());
        long committedAt = AdmissionStore.now(connection, binding.authority(), checkedClock);
        ExecutionStore.check(connection, binding, loaded, lease, change, committedAt);
        if (result.lease() != null && committedAt >= result.lease().until())
          throw error(ProtocolError.Code.CONFLICT, "new worker lease expired before commit");
        if (write && result.work() != null) checkTerminalInterval(result.work(), committedAt);
        if (write) AdmissionStore.remember(connection, config, binding.authority(), committedAt);
        statement.execute("COMMIT");
        return new ExecutionResult(
            result.lease(),
            result.work(),
            new ExecutionStore.Details(binding, loaded.stored().record()));
      } catch (IOException | SQLException | RuntimeException failure) {
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

  /**
   * Settle a reached execution deadline as local authority maintenance. This must not depend on the
   * former caller's certificate or execution grant, and must never be exposed as an anonymous
   * caller API. Accepted cancellation/revocation fences take precedence over deadline failure.
   *
   * @param generation retained session
   * @param work logical work
   * @param clock trusted UTC source for a new settlement
   * @return committed failure, or an unchanged already terminal observation
   * @throws SQLException corrupt retained state or failed settlement
   */
  WorkView expireExecution(long generation, WorkKey work, AdmissionStore.Clock clock)
      throws SQLException {
    Checks.id(generation);
    Objects.requireNonNull(work);
    AdmissionStore.Clock checkedClock = AdmissionStore.checkedClock(clock);
    if (!DATABASE_OPERATIONS.tryAcquire())
      throw ProtocolError.limit("V2 database operation capacity");
    try (Connection connection = database.connect();
        var statement = connection.createStatement()) {
      statement.execute("BEGIN IMMEDIATE");
      try {
        metadata(connection);
        Retained retained = retained(connection, generation);
        if (retained == null) throw error(ProtocolError.Code.NOT_FOUND, "session unavailable");
        if (retained.retiring())
          throw error(ProtocolError.Code.EXPIRED, "session retirement committed");
        Binding binding = retained.binding();
        DeclarationStore.Entity entity = DeclarationStore.member(connection, binding, work);
        if (entity.view().state().terminal()) {
          statement.execute("COMMIT");
          return entity.view();
        }
        if (retained.revoked())
          throw error(ProtocolError.Code.CANCELLED, "revocation requires cancellation settlement");
        ExecutionStore.eligible(connection, binding, entity.view());
        AdmissionStore.StoredJob stored = AdmissionStore.job(connection, binding, work);
        if (stored == null) throw error(ProtocolError.Code.NOT_READY, "work is not admitted");
        long now = AdmissionStore.now(connection, binding.authority(), checkedClock);
        if (now < entity.view().deadline())
          throw error(ProtocolError.Code.NOT_READY, "original execution deadline has not arrived");
        WorkView result =
            ExecutionStore.fail(
                connection,
                config,
                binding,
                new ExecutionStore.Loaded(entity, stored),
                new Diagnostic(
                    ProtocolError.Code.DEADLINE_EXCEEDED.value(), "execution deadline reached"),
                false,
                now);
        long committedAt = AdmissionStore.now(connection, binding.authority(), checkedClock);
        checkTerminalInterval(result, committedAt);
        AdmissionStore.remember(connection, config, binding.authority(), committedAt);
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

  private static void checkTerminalInterval(WorkView view, long now) {
    if (view.state().terminal()
        && (now >= view.receiptUntil() || view.outputUntil() != null && now >= view.outputUntil()))
      throw error(
          ProtocolError.Code.CLOCK_UNSAFE,
          "UTC jump overtook the proposed terminal retention interval");
  }

  private record InputResult<T>(T value, boolean fresh) {}

  @FunctionalInterface
  private interface InputTransaction<T> {
    InputResult<T> run(Connection connection, Binding binding) throws IOException, SQLException;
  }

  private <T> T inputTransaction(
      Access access,
      Capabilities selected,
      long generation,
      InputStore inputs,
      InputHeader header,
      AdmissionStore.Clock clock,
      AdmissionStore.Authorization authorization,
      boolean write,
      InputTransaction<T> action)
      throws IOException, SQLException {
    Objects.requireNonNull(access).check();
    profiles(Objects.requireNonNull(selected));
    Checks.id(generation);
    synchronized (Objects.requireNonNull(inputs)) {
      if (!DATABASE_OPERATIONS.tryAcquire())
        throw ProtocolError.limit("V2 database operation capacity");
      try (Connection connection = database.connect();
          var statement = connection.createStatement()) {
        if (!write) statement.execute("PRAGMA query_only=ON");
        statement.execute(write ? "BEGIN IMMEDIATE" : "BEGIN");
        try {
          access.check();
          Metadata metadata = metadata(connection);
          Retained retained = visible(connection, generation, access.owner());
          compatible(retained, selected);
          // Do not inspect another principal's payload namespace before owner authorization.
          inputs.verifyAuthority(identity);
          if (!inputs.identity().equals(metadata.inputs()))
            throw corrupt("input storage pairing differs");
          InputResult<T> result = action.run(connection, retained.binding());
          inputs.verifyAuthority(identity);
          access.check();
          AdmissionStore.beforeCommit(
              connection, config, retained.binding(), header, clock, authorization, result.fresh());
          statement.execute("COMMIT");
          return result.value();
        } catch (IOException | SQLException | RuntimeException failure) {
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
          if (write) FixedRecords.protect(connection, config.files());
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
    FixedRecords.createSchema(connection, config.authority());
    try (var sql = connection.createStatement()) {
      sql.execute(
          """
          CREATE TABLE ps_v2_meta (
            singleton INTEGER PRIMARY KEY CHECK(singleton=1),
            version INTEGER NOT NULL CHECK(version=5),
            config BLOB NOT NULL CHECK(length(config) BETWEEN 1 AND 8192),
            high_water INTEGER NOT NULL CHECK(high_water>=0),
            clock_slot INTEGER NOT NULL UNIQUE REFERENCES ps_v2_slots(id) CHECK(clock_slot=1),
            store_id BLOB NOT NULL CHECK(length(store_id)=16 AND store_id!=zeroblob(16)),
            input_id BLOB CHECK(input_id IS NULL OR (length(input_id)=16 AND input_id!=zeroblob(16))),
            identity_hash BLOB NOT NULL CHECK(length(identity_hash)=32)
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
            last_scope INTEGER NOT NULL DEFAULT 0 CHECK(last_scope>=0),
            required_control INTEGER NOT NULL DEFAULT 4096 CHECK(required_control BETWEEN 4096 AND 1048576),
            required_object INTEGER NOT NULL DEFAULT 0 CHECK(required_object>=0),
            UNIQUE(owner,sequence)
          ) STRICT
          """);
      sql.execute(
          """
          CREATE TABLE ps_v2_scopes (
            generation INTEGER NOT NULL REFERENCES ps_v2_sessions(generation),
            id INTEGER NOT NULL CHECK(id>=0), producer INTEGER NOT NULL CHECK(producer IN (0,1)),
            parent_scope INTEGER, parent_producer INTEGER, parent_entity INTEGER,
            state_slot INTEGER NOT NULL UNIQUE REFERENCES ps_v2_slots(id),
            PRIMARY KEY(generation,id), UNIQUE(generation,id,producer),
            UNIQUE(generation,parent_scope,parent_producer,parent_entity),
            CHECK((id=0 AND producer=0 AND parent_scope IS NULL AND parent_producer IS NULL AND parent_entity IS NULL)
              OR (id>0 AND parent_scope IS NOT NULL AND parent_scope>=0 AND parent_scope<id
                AND parent_producer IS NOT NULL AND parent_producer IN (0,1)
                AND parent_entity IS NOT NULL AND parent_entity>0))
          ) STRICT
          """);
    }
    DeclarationStore.createSchema(connection);
    AdmissionStore.createSchema(connection);
    try (var insert =
        connection.prepareStatement("INSERT INTO ps_v2_meta VALUES(1,?,?,0,1,?,NULL,?)")) {
      insert.setInt(1, VERSION);
      insert.setBytes(2, configBytes);
      insert.setBytes(3, uuid(identity));
      insert.setBytes(4, identityHash(configBytes, identity, null));
      insert.executeUpdate();
    }
  }

  private long meta(Connection connection) throws SQLException {
    return metadata(connection).highWater();
  }

  private Metadata metadata(Connection connection) throws SQLException {
    Metadata metadata = readMetadata(connection, configBytes);
    if (!identity.equals(metadata.identity()))
      throw corrupt("database installation identity changed");
    return metadata;
  }

  private static Metadata readMetadata(Connection connection, byte[] configBytes)
      throws SQLException {
    try (var statement = connection.createStatement();
        var row =
            statement.executeQuery(
                """
                SELECT version,CASE WHEN length(config)<=8192 THEN config END,high_water,
                  CASE WHEN length(store_id)=16 THEN store_id END,
                  CASE WHEN length(input_id)=16 THEN input_id END,input_id IS NOT NULL,
                  CASE WHEN length(identity_hash)=32 THEN identity_hash END
                FROM ps_v2_meta WHERE singleton=1
                """)) {
      if (!row.next() || row.getInt(1) != VERSION || !Arrays.equals(configBytes, row.getBytes(2)))
        throw corrupt("authority, configuration or version differs");
      long highWater = row.getLong(3);
      if (row.wasNull() || highWater < 0) throw corrupt("invalid authority high-water mark");
      UUID identity = uuid(row.getBytes(4));
      UUID inputs = row.getBoolean(6) ? uuid(row.getBytes(5)) : null;
      byte[] hash = row.getBytes(7);
      if (hash == null || !MessageDigest.isEqual(hash, identityHash(configBytes, identity, inputs)))
        throw corrupt("database installation binding integrity failure");
      if (row.next()) throw corrupt("duplicate authority metadata");
      return new Metadata(highWater, identity, inputs);
    }
  }

  private static byte[] identityHash(byte[] configBytes, UUID identity, UUID inputs) {
    MessageDigest digest = Commitments.sha256();
    Cbor.Writer out = new Cbor.Writer(digest);
    out.array(4);
    out.text("pipestream-java-v2-installation", 128);
    out.bytes(configBytes);
    out.bytes(uuid(identity));
    if (inputs == null) out.nil();
    else out.bytes(uuid(inputs));
    return digest.digest();
  }

  private static byte[] uuid(UUID value) {
    return ByteBuffer.allocate(16)
        .putLong(value.getMostSignificantBits())
        .putLong(value.getLeastSignificantBits())
        .array();
  }

  private static UUID uuid(byte[] bytes) throws SQLException {
    if (bytes == null || bytes.length != 16) throw corrupt("invalid installation identity length");
    ByteBuffer value = ByteBuffer.wrap(bytes);
    UUID identity = new UUID(value.getLong(), value.getLong());
    if (identity.equals(new UUID(0, 0))) throw corrupt("zero installation identity");
    return identity;
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
      FixedRecords.audit(connection, config.files(), config.authority());
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
      try (var row =
          statement.executeQuery(
              """
              SELECT 1 FROM ps_v2_jobs WHERE (SELECT input_id FROM ps_v2_meta WHERE singleton=1) IS NULL LIMIT 1
              """)) {
        if (row.next()) throw corrupt("admitted jobs without paired storage");
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
      try (var rows =
          statement.executeQuery(
              "SELECT CASE WHEN length(CAST(owner AS BLOB)) BETWEEN 1 AND 128 THEN owner"
                  + " END,high_water FROM ps_v2_owners")) {
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
          if (!session.retiring()) AdmissionStore.audit(connection, config, session.binding());
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
            SELECT CASE WHEN length(CAST(owner AS BLOB)) BETWEEN 1 AND 128 THEN owner END,
              sequence,CASE WHEN length(receipt)<=1024 THEN receipt END,
              CASE WHEN length(receipt_hash)=32 THEN receipt_hash END,profiles,control_limit,revoked,retiring,
              required_control,required_object
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
        int requiredControl = row.getInt(9);
        long requiredObject = row.getLong(10);
        if (requiredControl < 4096
            || requiredControl > Wire.MAX_CONTROL_LIMIT
            || requiredObject < 0) throw corrupt("invalid retained response requirements");
        return new Retained(
            binding,
            profiles,
            control,
            row.getInt(7) != 0,
            row.getInt(8) != 0,
            requiredControl,
            requiredObject);
      }
    }
  }

  private Retained visible(Connection connection, long generation, String owner)
      throws SQLException {
    // Do not decode another owner's retained contents, including malformed receipts, before denial.
    try (var query =
        connection.prepareStatement(
            "SELECT CASE WHEN length(CAST(owner AS BLOB)) BETWEEN 1 AND 128 THEN owner"
                + " END,revoked,retiring FROM ps_v2_sessions WHERE generation=?")) {
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
    if (selected.controlLimit() < Math.max(retained.controlLimit(), retained.requiredControl())
        || selected.objectLimit() < retained.requiredObject())
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

  private static void rollback(Connection connection, Throwable failure) {
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
