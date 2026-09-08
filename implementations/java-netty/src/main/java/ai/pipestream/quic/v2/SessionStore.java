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
  private static final int VERSION = 9;
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
              if (session.retiring()) {
                RetirementStore.auditRemaining(
                    connection, session.binding(), retirementProof(connection, session.binding()));
                if (inputs.sessionHasResources(RetirementStore.context(session.binding())))
                  throw corrupt("retiring session still owns physical storage");
              } else AdmissionStore.verifyStorage(connection, session.binding(), inputs);
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
      inputs.bindGenerationGate(identity, context -> checkInputGeneration(inputs, context));
    }
  }

  private void checkInputGeneration(InputStore inputs, Commitments.Context context)
      throws IOException {
    // The caller holds the input-store monitor, also held across every retirement commit.
    inputs.verifyAuthority(identity);
    if (!DATABASE_OPERATIONS.tryAcquire())
      throw ProtocolError.limit("V2 database operation capacity");
    try (Connection connection = database.connect();
        var statement = connection.createStatement()) {
      statement.execute("PRAGMA query_only=ON");
      statement.execute("BEGIN");
      try {
        Metadata metadata = metadata(connection);
        if (!inputs.identity().equals(metadata.inputs()))
          throw corrupt("input generation gate storage pairing differs");
        if (!config.authority().equals(context.authority())) throw denied();
        Retained session = retained(connection, context.generation());
        if (session == null)
          throw error(
              context.generation() <= metadata.highWater()
                  ? ProtocolError.Code.EXPIRED
                  : ProtocolError.Code.NOT_FOUND,
              "input generation unavailable");
        if (!session.binding().owner().equals(context.owner()) || session.revoked()) throw denied();
        if (session.retiring())
          throw error(ProtocolError.Code.EXPIRED, "session retirement committed");
        statement.execute("COMMIT");
      } catch (SQLException | RuntimeException failure) {
        rollback(connection, failure);
        throw failure;
      }
    } catch (SQLException failure) {
      throw new IOException("V2 input generation check failed", failure);
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
   * Replace an admitted attempt and retain its immutable receipt in one owner-authorized commit.
   * Replay returns the original receipt without advancing the attempt or extending any lifetime.
   * The access and application gates must authorize retry, not merely permission to read work.
   *
   * @param access current owner authorization for retry
   * @param selected completed compatible capability selection
   * @param generation retained session generation
   * @param request immutable retry intent
   * @param clock trusted UTC source for a new mutation
   * @param authorization current application retry policy
   * @return correlated committed or replayed receipt
   * @throws SQLException failed storage transaction or corrupt retained evidence
   */
  RetryResponse retry(
      Access access,
      Capabilities selected,
      long generation,
      Retry request,
      AdmissionStore.Clock clock,
      AdmissionStore.Authorization authorization)
      throws SQLException {
    Objects.requireNonNull(access).check();
    profiles(Objects.requireNonNull(selected));
    Checks.id(generation);
    Objects.requireNonNull(request);
    Objects.requireNonNull(authorization);
    AdmissionStore.Clock checkedClock = AdmissionStore.checkedClock(clock);
    if (!DATABASE_OPERATIONS.tryAcquire())
      throw ProtocolError.limit("V2 database operation capacity");
    try (Connection connection = database.connect();
        var statement = connection.createStatement()) {
      statement.execute("BEGIN IMMEDIATE");
      try {
        access.check();
        meta(connection);
        Retained retained = visible(connection, generation, access.owner());
        compatible(retained, selected);
        Binding binding = retained.binding();
        Digest digest =
            Commitments.operation(
                new Commitments.Context(binding.authority(), binding.owner(), generation),
                0,
                request);
        DeclarationStore.Operation prior =
            DeclarationStore.operation(connection, binding, request.operation());
        if (prior != null
            && (!(prior.receipt().outcome() instanceof Retried)
                || !prior.receipt().requestDigest().equals(digest)))
          throw error(ProtocolError.Code.CONFLICT, "retry operation parameters differ");
        if (prior == null)
          ExecutionStore.eligible(
              connection,
              binding,
              DeclarationStore.member(connection, binding, request.work()).view());
        ExecutionStore.Loaded loaded =
            ExecutionStore.load(connection, config, binding, request.work(), authorization);
        OperationReceipt receipt;
        if (prior == null) {
          long now = AdmissionStore.now(connection, binding.authority(), checkedClock);
          RetryStore.eligible(connection, binding, loaded, request, now);
          receipt = RetryStore.replace(connection, config, binding, loaded, request, digest, now);
        } else {
          RetryStore.audit(connection, binding, loaded.entity().view());
          receipt = prior.receipt();
        }
        RetryResponse response = new RetryResponse(request.request(), receipt);
        Wire.encode(response, selected.controlLimit());
        access.check();
        authorization.check(binding, loaded.stored().record().input().parameters());
        if (prior == null) {
          long committedAt = AdmissionStore.now(connection, binding.authority(), checkedClock);
          RetryStore.eligible(connection, binding, loaded, request, committedAt);
          AdmissionStore.remember(connection, config, binding.authority(), committedAt);
        }
        statement.execute("COMMIT");
        return response;
      } catch (SQLException | RuntimeException | Error failure) {
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
   * Accept or replay an owner-authorized work cancellation.
   *
   * @param access current owner authorization
   * @param selected compatible negotiated profiles
   * @param generation retained session
   * @param request immutable cancellation intent
   * @param clock trusted UTC source
   * @param authorization explicit application cancellation policy
   * @return correlated committed receipt
   * @throws SQLException failed transaction or corrupt state
   */
  CancelResponse cancel(
      Access access,
      Capabilities selected,
      long generation,
      Cancel request,
      AdmissionStore.Clock clock,
      FenceStore.Authorization authorization)
      throws SQLException {
    return (CancelResponse)
        fenceTransaction(access, selected, generation, request, clock, authorization);
  }

  /**
   * Accept or replay a skip under a policy explicitly authorizing skip.
   *
   * @param access current owner authorization
   * @param selected compatible profiles
   * @param generation retained session
   * @param request immutable skip intent
   * @param clock trusted UTC source
   * @param authorization explicit skip policy
   * @return correlated committed receipt
   * @throws SQLException failed transaction or corrupt state
   */
  SkipResponse skip(
      Access access,
      Capabilities selected,
      long generation,
      Skip request,
      AdmissionStore.Clock clock,
      FenceStore.Authorization authorization)
      throws SQLException {
    return (SkipResponse)
        fenceTransaction(access, selected, generation, request, clock, authorization);
  }

  /**
   * Freeze membership now; bounded maintenance computes the seal and terminal outcomes.
   *
   * @param access current owner authorization
   * @param selected compatible profiles
   * @param generation retained session
   * @param request immutable scope-cancellation intent
   * @param clock trusted UTC source
   * @param authorization explicit scope policy
   * @return correlated fence receipt, not a closure acknowledgment
   * @throws SQLException failed transaction or corrupt state
   */
  CancelScopeResponse cancelScope(
      Access access,
      Capabilities selected,
      long generation,
      CancelScope request,
      AdmissionStore.Clock clock,
      FenceStore.Authorization authorization)
      throws SQLException {
    return (CancelScopeResponse)
        fenceTransaction(access, selected, generation, request, clock, authorization);
  }

  private Message fenceTransaction(
      Access access,
      Capabilities selected,
      long generation,
      Message request,
      AdmissionStore.Clock clock,
      FenceStore.Authorization authorization)
      throws SQLException {
    Objects.requireNonNull(access).check();
    profiles(Objects.requireNonNull(selected));
    Checks.id(generation);
    Objects.requireNonNull(request);
    Objects.requireNonNull(authorization);
    AdmissionStore.Clock checkedClock = AdmissionStore.checkedClock(clock);
    return maintenanceTransaction(
        connection -> {
          access.check();
          Retained retained = visible(connection, generation, access.owner());
          compatible(retained, selected);
          Binding binding = retained.binding();
          authorization.check(binding, request);
          Digest digest =
              Commitments.operation(
                  new Commitments.Context(binding.authority(), binding.owner(), generation),
                  0,
                  request);
          DeclarationStore.Operation prior =
              DeclarationStore.operation(connection, binding, FenceStore.operation(request));
          if (prior != null && !prior.receipt().requestDigest().equals(digest))
            throw error(ProtocolError.Code.CONFLICT, "fence operation parameters differ");
          FenceStore.Accepted accepted =
              prior == null
                  ? FenceStore.accept(
                      connection,
                      config,
                      binding,
                      request,
                      digest,
                      AdmissionStore.now(connection, binding.authority(), checkedClock))
                  : new FenceStore.Accepted(prior.receipt(), null);
          Message response =
              switch (request) {
                case Cancel m -> new CancelResponse(m.request(), accepted.receipt());
                case Skip m -> new SkipResponse(m.request(), accepted.receipt());
                case CancelScope m -> new CancelScopeResponse(m.request(), accepted.receipt());
                default -> throw new IllegalArgumentException("not a fence request");
              };
          Wire.encode(response, selected.controlLimit());
          access.check();
          authorization.check(binding, request);
          if (prior == null) {
            long now = AdmissionStore.now(connection, binding.authority(), checkedClock);
            if (accepted.terminal() != null) checkTerminalInterval(accepted.terminal(), now);
            AdmissionStore.remember(connection, config, binding.authority(), now);
          }
          return response;
        });
  }

  /**
   * Revoke a retained session under a trusted local administrative gate, not a peer RPC. The gate
   * must authorize this target generation independently of its now-revoked owner's grant.
   *
   * @param operatorAuthorization current administrative authorization
   * @param generation retained target session
   * @param clock trusted UTC source
   * @throws SQLException failed transaction or contradictory root
   */
  void revoke(Access operatorAuthorization, long generation, AdmissionStore.Clock clock)
      throws SQLException {
    Objects.requireNonNull(operatorAuthorization).check();
    Checks.id(generation);
    AdmissionStore.Clock checkedClock = AdmissionStore.checkedClock(clock);
    maintenanceTransaction(
        connection -> {
          operatorAuthorization.check();
          Retained retained = retained(connection, generation);
          if (retained == null) throw error(ProtocolError.Code.NOT_FOUND, "session unavailable");
          if (retained.retiring())
            throw error(ProtocolError.Code.EXPIRED, "session retirement committed");
          Binding binding = retained.binding();
          DeclarationStore.Scope root = DeclarationStore.scope(connection, binding, 0);
          if (root.state().revoked() != retained.revoked())
            throw corrupt("root revocation differs from session");
          if (!retained.revoked()) {
            AdmissionStore.now(connection, binding.authority(), checkedClock);
            FenceStore.freeze(connection, config, binding, root, true);
            try (var update =
                connection.prepareStatement(
                    "UPDATE ps_v2_sessions SET revoked=1 WHERE generation=?")) {
              update.setLong(1, generation);
              if (update.executeUpdate() != 1)
                throw corrupt("session disappeared during revocation");
            }
          }
          operatorAuthorization.check();
          if (!retained.revoked())
            AdmissionStore.remember(
                connection,
                config,
                binding.authority(),
                AdmissionStore.now(connection, binding.authority(), checkedClock));
          return null;
        });
  }

  /**
   * Materialize accepted fences without a live caller. Closure folds run separately.
   *
   * @param cursor installation-bound volatile discovery state
   * @param limit per-category direct work/member budget, one to 256
   * @param clock trusted UTC source
   * @return committed direct record accounting
   * @throws SQLException inconsistent evidence or failed funded commit
   */
  FenceStore.Progress reconcileCancellation(
      FenceStore.Cursor cursor, int limit, AdmissionStore.Clock clock) throws SQLException {
    Objects.requireNonNull(cursor);
    if (limit < 1 || limit > 256)
      throw ProtocolError.limit("cancellation reconciliation batch capacity");
    synchronized (cursor) {
      if (cursor.installation != null && !cursor.installation.equals(identity))
        throw error(
            ProtocolError.Code.CONFLICT, "cancellation cursor belongs to another authority");
      cursor.installation = identity;
      AdmissionStore.Clock checkedClock = AdmissionStore.checkedClock(clock);
      try {
        FenceReconciliation.Batch batch =
            maintenanceTransaction(
                connection -> {
                  long now = AdmissionStore.now(connection, config.authority(), checkedClock);
                  FenceReconciliation.Batch result =
                      FenceReconciliation.step(
                          connection,
                          config,
                          cursor,
                          limit,
                          generation -> {
                            Retained retained = retained(connection, generation);
                            if (retained == null)
                              throw corrupt("cancellation target lacks session");
                            return retained.retiring() ? null : retained.binding();
                          },
                          now);
                  if (result.wrote()) {
                    long committedAt =
                        AdmissionStore.now(connection, config.authority(), checkedClock);
                    if (result.terminal() != null && committedAt >= result.terminal())
                      throw error(
                          ProtocolError.Code.CLOCK_UNSAFE,
                          "UTC jump overtook cancellation receipt retention");
                    AdmissionStore.remember(connection, config, config.authority(), committedAt);
                  }
                  return result;
                });
        cursor.beforeWork = batch.beforeWork();
        cursor.beforeScope = batch.beforeScope();
        cursor.scan = batch.scan();
        return batch.progress();
      } catch (SQLException | RuntimeException | Error failure) {
        cursor.scan = null;
        throw failure;
      }
    }
  }

  private <T> T maintenanceTransaction(Transaction<T> action) throws SQLException {
    if (!DATABASE_OPERATIONS.tryAcquire())
      throw ProtocolError.limit("V2 database operation capacity");
    try (Connection connection = database.connect();
        var statement = connection.createStatement()) {
      statement.execute("BEGIN IMMEDIATE");
      try {
        meta(connection);
        T result = action.run(connection);
        statement.execute("COMMIT");
        return result;
      } catch (SQLException | RuntimeException | Error failure) {
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
   * Declare or replay authority-produced children under their current parent's execution fence. The
   * child scope is fixed by parent admission; sealing membership never completes expansion. This
   * local API is not a caller RPC and does not run application code.
   *
   * @param access current retained execution grant
   * @param parent current parent lease
   * @param selected retained compatible profile selection
   * @param inputs live paired storage held through commitment
   * @param request immutable declaration in the producer-one operation namespace
   * @param clock trusted UTC source
   * @param authorization current parent application policy
   * @return correlated committed declaration receipt
   * @throws IOException invalid storage pairing or closed storage
   * @throws SQLException corrupt metadata or failed commit
   */
  DeclarationResponse declareProduced(
      ExecutionStore.Access access,
      ExecutionStore.Lease parent,
      Capabilities selected,
      InputStore inputs,
      Declare request,
      AdmissionStore.Clock clock,
      AdmissionStore.Authorization authorization)
      throws IOException, SQLException {
    Objects.requireNonNull(request);
    return producerTransaction(
        access,
        parent,
        selected,
        inputs,
        request.scope(),
        null,
        clock,
        authorization,
        true,
        (connection, binding) -> {
          boolean fresh =
              DeclarationStore.operation(connection, binding, 1, request.operation()) == null;
          return new InputResult<>(
              DeclarationStore.declare(connection, config.files(), binding, selected, 1, request),
              fresh);
        });
  }

  /**
   * Preflight a locally produced child's immutable header without reserving admission capacity.
   * Receipt replay still requires current parent authority; a previous check grants no later
   * commit.
   *
   * @param access current retained execution grant
   * @param parent current parent lease
   * @param selected retained compatible profile selection
   * @param inputs live paired storage
   * @param header immutable producer-one input intent
   * @param clock trusted UTC source
   * @param authorization current parent and child application policy
   * @return retained admission or empty for currently eligible new input
   * @throws IOException invalid storage pairing or closed storage
   * @throws SQLException corrupt metadata or storage failure
   */
  Optional<OperationReceipt> checkProducedInput(
      ExecutionStore.Access access,
      ExecutionStore.Lease parent,
      Capabilities selected,
      InputStore inputs,
      InputHeader header,
      AdmissionStore.Clock clock,
      AdmissionStore.Authorization authorization)
      throws IOException, SQLException {
    Objects.requireNonNull(header);
    AdmissionStore.Clock checkedClock = AdmissionStore.checkedClock(clock);
    return producerTransaction(
        access,
        parent,
        selected,
        inputs,
        header.parameters().work().scope(),
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
                        authorization,
                        1)),
                false));
  }

  /**
   * Admit validated locally produced input with the same immutable receipt and resource promises as
   * caller input, additionally fenced by the current parent through commitment. Installed files
   * after a refused commit remain charged orphans; accepted siblings and declarations are
   * preserved.
   *
   * @param access current retained execution grant
   * @param parent current parent lease
   * @param selected retained compatible profile selection
   * @param inputs live paired storage
   * @param header exact producer-one input intent
   * @param clock trusted UTC source
   * @param authorization current parent and child application policy
   * @return committed admission receipt, with no fabricated transport stream correlation
   * @throws IOException missing/corrupt input, invalid pairing or funding failure
   * @throws SQLException corrupt metadata or failed commit
   */
  OperationReceipt admitProduced(
      ExecutionStore.Access access,
      ExecutionStore.Lease parent,
      Capabilities selected,
      InputStore inputs,
      InputHeader header,
      AdmissionStore.Clock clock,
      AdmissionStore.Authorization authorization)
      throws IOException, SQLException {
    Objects.requireNonNull(header);
    AdmissionStore.Clock checkedClock = AdmissionStore.checkedClock(clock);
    return producerTransaction(
        access,
        parent,
        selected,
        inputs,
        header.parameters().work().scope(),
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
                  authorization,
                  1);
          return new InputResult<>(result.receipt(), result.fresh());
        });
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
   * Yield a current expanding worker or finish producing its complete admitted child set. A seal
   * alone is insufficient for completion, and completion does not assert child or parent success.
   *
   * @param access current retained execution grant
   * @param lease current mode-two worker ownership
   * @param complete finish expansion, or yield for a later lease of the same attempt
   * @param clock trusted UTC source
   * @param authorization current application policy
   * @return parent view after the atomic progress transition
   * @throws SQLException contradictory metadata or failed atomic write
   */
  WorkView finishExpansion(
      ExecutionStore.Access access,
      ExecutionStore.Lease lease,
      boolean complete,
      AdmissionStore.Clock clock,
      AdmissionStore.Authorization authorization)
      throws SQLException {
    return executeOwned(
            access,
            lease,
            0,
            clock,
            authorization,
            complete
                ? ExecutionStore.Change.EXPANSION_COMPLETE
                : ExecutionStore.Change.EXPANSION_YIELD,
            null)
        .work();
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
   * Return authenticated immutable publication evidence, without consulting UTC or object storage.
   * Expired availability does not make the retained manifest mutable or grant a new read lease.
   *
   * @param access current verified owner
   * @param selected negotiated result-delivery capabilities
   * @param generation attached session
   * @param request exact manifest query
   * @param authorization current result permission
   * @return exactly correlated publication evidence
   * @throws SQLException missing or contradictory metadata
   */
  ManifestResponse manifest(
      Access access,
      Capabilities selected,
      long generation,
      GetManifest request,
      ResultStore.Authorization authorization)
      throws SQLException {
    Objects.requireNonNull(request);
    Objects.requireNonNull(authorization);
    return sessionTransaction(
        access,
        selected,
        generation,
        false,
        (connection, binding) -> {
          resultProfile(selected);
          authorization.check(binding, request.work());
          ManifestResponse response =
              new ManifestResponse(
                  request.request(),
                  ResultStore.retained(connection, binding, request.work(), request.attempt()));
          Wire.encode(response, selected.controlLimit());
          authorization.check(binding, request.work());
          return response;
        });
  }

  /**
   * Pin one exact output inside the same writer transaction as fresh authorization and safe-UTC
   * availability. Final checks occur after potentially slow storage verification. A refused
   * acquisition closes its pin and rolls back the UTC watermark, never changing work outcomes.
   *
   * @param access current verified owner
   * @param selected negotiated result-delivery capabilities
   * @param generation attached session
   * @param inputs paired exclusive object store
   * @param request exact object request
   * @param clock trusted UTC source
   * @param authorization current result permission
   * @param finalCheck local delivery-lifetime gate, not a peer-supplied callback
   * @return pinned object to transfer under independently enforced elapsed deadlines
   * @throws IOException failed storage pairing or physical cleanup
   * @throws SQLException missing or contradictory metadata, or failed transaction
   */
  ResultStore.Opened openResult(
      Access access,
      Capabilities selected,
      long generation,
      InputStore inputs,
      Read request,
      AdmissionStore.Clock clock,
      ResultStore.Authorization authorization,
      Runnable finalCheck)
      throws IOException, SQLException {
    Objects.requireNonNull(access).check();
    resultProfile(Objects.requireNonNull(selected));
    Checks.id(generation);
    Objects.requireNonNull(request);
    Objects.requireNonNull(authorization);
    Objects.requireNonNull(finalCheck);
    AdmissionStore.Clock checkedClock = AdmissionStore.checkedClock(clock);
    synchronized (Objects.requireNonNull(inputs)) {
      if (!DATABASE_OPERATIONS.tryAcquire())
        throw ProtocolError.limit("V2 database operation capacity");
      ResultStore.Opened opened = null;
      try (Connection connection = database.connect();
          var statement = connection.createStatement()) {
        statement.execute("BEGIN IMMEDIATE");
        try {
          access.check();
          Metadata metadata = metadata(connection);
          Retained retained = visible(connection, generation, access.owner());
          compatible(retained, selected);
          Binding binding = retained.binding();
          authorization.check(binding, request.work());
          Manifest manifest =
              ResultStore.retained(connection, binding, request.work(), request.attempt());
          Output output = ResultStore.requested(manifest, request);
          ResultStore.available(
              manifest, AdmissionStore.now(connection, binding.authority(), checkedClock));
          if (output.length() > selected.objectLimit())
            throw ProtocolError.limit("connection cannot represent requested output");
          inputs.verifyAuthority(identity);
          if (!inputs.identity().equals(metadata.inputs()))
            throw corrupt("result storage pairing differs");
          opened = ResultStore.open(connection, identity, binding, inputs, request, output);
          inputs.verifyAuthority(identity);
          access.check();
          authorization.check(binding, request.work());
          finalCheck.run();
          long committedAt = AdmissionStore.now(connection, binding.authority(), checkedClock);
          ResultStore.available(manifest, committedAt);
          AdmissionStore.remember(connection, config, binding.authority(), committedAt);
          statement.execute("COMMIT");
          return opened;
        } catch (IOException | SQLException | RuntimeException | Error failure) {
          rollback(connection, failure);
          throw failure;
        }
      } catch (IOException | SQLException | RuntimeException | Error failure) {
        // Include failures closing JDBC resources after COMMIT: no caller received this pin yet.
        if (opened != null) {
          try {
            opened.close();
          } catch (IOException cleanup) {
            failure.addSuppressed(cleanup);
          }
        }
        if (failure instanceof SQLException sql && (sql.getErrorCode() & 255) == 13) {
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
   * Recheck a live read's owner and result permission. Acquisition already pinned immutable bytes;
   * UTC availability expiry does not revoke that bounded delivery lease.
   *
   * @param access current verified owner
   * @param selected immutable connection selection
   * @param generation retained session
   * @param work pinned work identity
   * @param authorization current result permission
   * @throws SQLException failed metadata observation
   */
  void checkResultRead(
      Access access,
      Capabilities selected,
      long generation,
      WorkKey work,
      ResultStore.Authorization authorization)
      throws SQLException {
    Objects.requireNonNull(work);
    Objects.requireNonNull(authorization);
    sessionTransaction(
        access,
        selected,
        generation,
        false,
        (connection, binding) -> {
          resultProfile(selected);
          authorization.check(binding, work);
          return null;
        });
  }

  private static void resultProfile(Capabilities selected) {
    if (profiles(selected) != 3)
      throw error(ProtocolError.Code.EXTENSION_UNSUPPORTED, "result delivery not selected");
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
    return scanExecutions(cursor, null, limit);
  }

  /**
   * Discover a finite ordered suffix for round-robin dispatch. Maintenance uses its independent
   * full sweep so occupied workers cannot postpone deadline settlement.
   *
   * @param cursor prior finite-sweep continuation, taking precedence over after
   * @param after exclusive initial position, null to begin at the first job
   * @param limit maximum examined jobs, between one and 64
   * @return bounded advisory observations and continuation
   * @throws SQLException contradictory records or database failure
   */
  ExecutionStore.Page scanExecutions(
      ExecutionStore.ScanCursor cursor, ExecutionStore.Position after, int limit)
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
        ExecutionStore.Position lowerBound = cursor == null ? after : cursor.after();
        String lower = lowerBound == null ? "" : " AND (generation,scope,entity)>(?,?,?)";
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
          if (lowerBound != null) {
            query.setLong(parameter++, lowerBound.generation());
            query.setLong(parameter++, lowerBound.scope());
            query.setLong(parameter++, lowerBound.entity());
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
                    case CANCELLING -> view.state() == State.CANCELLING;
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
        } else if (change == ExecutionStore.Change.EXPANSION_COMPLETE
            || change == ExecutionStore.Change.EXPANSION_YIELD) {
          result =
              new ExecutionResult(
                  null,
                  ExecutionStore.finishExpansion(
                      connection,
                      config,
                      binding,
                      loaded,
                      change == ExecutionStore.Change.EXPANSION_COMPLETE));
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
            new ExecutionStore.Details(
                binding,
                loaded.stored().record(),
                loaded.entity().view().child(),
                retained.profiles() == 3));
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

  private <T> T producerTransaction(
      ExecutionStore.Access access,
      ExecutionStore.Lease lease,
      Capabilities selected,
      InputStore inputs,
      long scope,
      InputHeader header,
      AdmissionStore.Clock clock,
      AdmissionStore.Authorization authorization,
      boolean write,
      InputTransaction<T> action)
      throws IOException, SQLException {
    Objects.requireNonNull(access).check();
    Objects.requireNonNull(lease);
    Objects.requireNonNull(authorization);
    profiles(Objects.requireNonNull(selected));
    if (!access.owner().equals(lease.owner()) || !identity.equals(lease.installation()))
      throw error(
          ProtocolError.Code.UNAUTHORIZED, "parent belongs to another owner or installation");
    AdmissionStore.Clock checkedClock = AdmissionStore.checkedClock(clock);
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
          Retained retained = visible(connection, lease.generation(), access.owner());
          compatible(retained, selected);
          Binding binding = retained.binding();
          inputs.verifyAuthority(identity);
          if (!inputs.identity().equals(metadata.inputs()))
            throw corrupt("local producer input storage pairing differs");
          ExecutionStore.Loaded parent =
              ExecutionStore.load(connection, config, binding, lease.work(), authorization);
          checkProducer(
              connection,
              binding,
              parent,
              lease,
              scope,
              AdmissionStore.now(connection, binding.authority(), checkedClock));
          if (write) FixedRecords.protect(connection, config.files());
          InputResult<T> result = action.run(connection, binding);
          inputs.verifyAuthority(identity);
          access.check();
          if (header != null)
            AdmissionStore.beforeCommit(
                connection, config, binding, header, checkedClock, authorization, result.fresh());
          authorization.check(binding, parent.stored().record().input().parameters());
          long committedAt = AdmissionStore.now(connection, binding.authority(), checkedClock);
          checkProducer(connection, binding, parent, lease, scope, committedAt);
          if (result.fresh() && header != null)
            AdmissionStore.checkAdmissionInterval(connection, binding, header, committedAt);
          if (result.fresh())
            AdmissionStore.remember(connection, config, binding.authority(), committedAt);
          statement.execute("COMMIT");
          return result.value();
        } catch (IOException | SQLException | RuntimeException | Error failure) {
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

  private static void checkProducer(
      Connection connection,
      Binding binding,
      ExecutionStore.Loaded parent,
      ExecutionStore.Lease lease,
      long scope,
      long now)
      throws SQLException {
    ExecutionStore.check(connection, binding, parent, lease, ExecutionStore.Change.CHECK, now);
    JobRecord job = parent.stored().record();
    if (job.input().parameters().mode() != 2 || job.expansionComplete())
      throw error(ProtocolError.Code.CONFLICT, "parent has no pending authority expansion");
    ChildScope child = parent.entity().view().child();
    if (child == null || child.producer() != 1)
      throw corrupt("authority-expanded parent lacks producer-one child scope");
    if (scope != child.scope())
      throw error(ProtocolError.Code.CONFLICT, "local mutation targets another parent's scope");
    DeclarationStore.Scope target = DeclarationStore.scope(connection, binding, scope);
    if (target.producer() != 1 || !lease.work().equals(target.parent()))
      throw corrupt("producer-one child scope contradicts parent admission");
    AdmissionStore.ancestors(connection, binding, scope);
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

  /**
   * Reclaim one terminal job's input independently of the caller's current permission. The target
   * count is one; existing child-closure verification retains its session-wide streaming audit
   * cost. Output retention and session retirement are separate operations.
   *
   * @param generation retained session identity
   * @param work exact admitted work
   * @param inputs exclusively paired payload installation
   * @param clock trusted UTC source
   * @return committed outcome or a live physical dependency
   * @throws IOException file or synchronization failure
   * @throws SQLException contradictory evidence or database failure
   */
  RetentionStore.Result reclaimInput(
      long generation, WorkKey work, InputStore inputs, AdmissionStore.Clock clock)
      throws IOException, SQLException {
    return reclaimInput(generation, work, inputs, clock, phase -> {});
  }

  /**
   * Reclaim one input with trusted local commit-boundary instrumentation. Eligibility commits
   * before physical deletion; logical quota commits only after synchronized absence. A failure
   * after either commit can lose this method's return value, never its evidence.
   *
   * @param generation retained session identity
   * @param work exact admitted work
   * @param inputs exclusively paired payload installation
   * @param clock trusted UTC source
   * @param probe bounded local durability instrumentation
   * @return committed outcome or live physical dependency
   * @throws IOException file, synchronization or instrumentation failure
   * @throws SQLException contradictory evidence or database failure
   */
  RetentionStore.Result reclaimInput(
      long generation,
      WorkKey work,
      InputStore inputs,
      AdmissionStore.Clock clock,
      RetentionStore.Probe probe)
      throws IOException, SQLException {
    return reclaimResource(generation, work, inputs, clock, probe, true);
  }

  /**
   * Reclaim a terminal job's outputs after external expiry and dependent parent settlement.
   * Receipt, input and manifest retention are independent. Physical result readers and callback
   * credits must close before output funding can be removed or its capacity reused.
   *
   * @param generation retained session
   * @param work exact admitted work
   * @param inputs exclusively paired payload installation
   * @param clock trusted UTC source
   * @return committed outcome or outstanding logical/physical dependency
   * @throws IOException file or synchronization failure
   * @throws SQLException contradictory evidence or database failure
   */
  RetentionStore.Result reclaimOutput(
      long generation, WorkKey work, InputStore inputs, AdmissionStore.Clock clock)
      throws IOException, SQLException {
    return reclaimOutput(generation, work, inputs, clock, phase -> {});
  }

  /**
   * Reclaim terminal outputs with trusted local durability instrumentation. Commits eligibility
   * before deletion, then removes and synchronizes output names before funding and quota release.
   *
   * @param generation retained session
   * @param work exact admitted work
   * @param inputs exclusively paired payload installation
   * @param clock trusted UTC source
   * @param probe bounded local commit observer
   * @return committed outcome or outstanding dependency
   * @throws IOException file, synchronization or instrumentation failure
   * @throws SQLException contradictory evidence or database failure
   */
  RetentionStore.Result reclaimOutput(
      long generation,
      WorkKey work,
      InputStore inputs,
      AdmissionStore.Clock clock,
      RetentionStore.Probe probe)
      throws IOException, SQLException {
    return reclaimResource(generation, work, inputs, clock, probe, false);
  }

  private RetentionStore.Result reclaimResource(
      long generation,
      WorkKey work,
      InputStore inputs,
      AdmissionStore.Clock clock,
      RetentionStore.Probe probe,
      boolean inputResource)
      throws IOException, SQLException {
    Checks.id(generation);
    Objects.requireNonNull(work);
    Objects.requireNonNull(inputs);
    Objects.requireNonNull(probe);
    AdmissionStore.Clock checkedClock = AdmissionStore.checkedClock(clock);
    synchronized (inputs) {
      inputs.verifyAuthority(identity);
      if (!DATABASE_OPERATIONS.tryAcquire())
        throw ProtocolError.limit("V2 database operation capacity");
      try {
        // At most an eligibility transaction followed by one removal/completion transaction.
        for (int phase = 0; phase < 2; phase++) {
          try (Connection connection = database.connect();
              var statement = connection.createStatement()) {
            statement.execute("BEGIN IMMEDIATE");
            boolean committed = false;
            try {
              Metadata metadata = metadata(connection);
              if (!inputs.identity().equals(metadata.inputs()))
                throw corrupt("retention input storage pairing differs");
              Retained retained = retained(connection, generation);
              if (retained == null)
                throw error(ProtocolError.Code.NOT_FOUND, "session unavailable");
              if (retained.retiring())
                throw error(ProtocolError.Code.EXPIRED, "session retirement committed");
              Binding binding = retained.binding();
              ExecutionStore.Loaded loaded =
                  ExecutionStore.load(connection, config, binding, work, (owner, input) -> {});
              JobRecord job = loaded.stored().record();
              WorkView view = loaded.entity().view();
              Commitments.Context context =
                  new Commitments.Context(binding.authority(), binding.owner(), generation);
              if (!inputs.inputReference(context, job.input()).equals(job.inputReference()))
                throw corrupt("input release reference contradicts retained identity");
              long watermark = AdmissionStore.watermark(connection, binding.authority());
              ExecutionStore.audit(
                  connection, binding, loaded.entity(), loaded.stored(), watermark);
              if (!inputs.outputReference(context, job.input()).equals(job.outputReference()))
                throw corrupt("output release reference contradicts retained identity");
              if (!(inputResource ? job.inputLive() : job.outputsLive())) {
                statement.execute("COMMIT");
                committed = true;
                return RetentionStore.Result.ALREADY_RELEASED;
              }
              long now = AdmissionStore.now(connection, binding.authority(), checkedClock);
              if (!(inputResource
                  ? RetentionStore.inputEligible(connection, binding, view, now)
                  : RetentionStore.outputEligible(connection, binding, view, now))) {
                statement.execute("COMMIT");
                committed = true;
                return RetentionStore.Result.NOT_READY;
              }
              // A failed/cancelled callback can still hold a physical writer after settlement.
              // Do not audit its mutating staging header or commit release while it remains live.
              if (!inputResource && inputs.outputInUse(context, job.input())) {
                statement.execute("COMMIT");
                committed = true;
                return RetentionStore.Result.PINNED;
              }
              if ((inputResource ? job.inputReleaseAt() : job.outputReleaseAt()) == null) {
                if (inputResource) {
                  InputStore.Stored original =
                      inputs
                          .find(context, job.input())
                          .orElseThrow(
                              () -> new IOException("live input missing before release intent"));
                  if (!original.reference().equals(job.inputReference()))
                    throw corrupt("input release reference differs");
                } else inputs.verifyRetainedOutputs(context, job, view);
                inputs.verifyAuthority(identity);
                long at = AdmissionStore.now(connection, binding.authority(), checkedClock);
                JobRecord intent =
                    inputResource
                        ? RetentionStore.input(job, at, true)
                        : RetentionStore.output(job, at, true);
                ExecutionStore.replaceJob(
                    connection, config, binding, loaded.stored(), intent, true);
                AdmissionStore.remember(connection, config, binding.authority(), at);
                RetentionStore.audit(connection, binding, view, intent, at);
                statement.execute("COMMIT");
                committed = true;
                probe.at(
                    inputResource
                        ? RetentionStore.Phase.INPUT_INTENT_COMMITTED
                        : RetentionStore.Phase.OUTPUT_INTENT_COMMITTED);
              } else {
                inputs.verifyAuthority(identity);
                AdmissionStore.now(connection, binding.authority(), checkedClock);
                if (!(inputResource
                    ? inputs.reclaimInput(context, job.input())
                    : inputs.reclaimOutput(context, job, view))) {
                  statement.execute("COMMIT");
                  committed = true;
                  return RetentionStore.Result.PINNED;
                }
                inputs.verifyAuthority(identity);
                long at = AdmissionStore.now(connection, binding.authority(), checkedClock);
                JobRecord completed =
                    inputResource
                        ? RetentionStore.input(job, at, false)
                        : RetentionStore.output(job, at, false);
                ExecutionStore.replaceJob(
                    connection, config, binding, loaded.stored(), completed, true);
                AdmissionStore.remember(connection, config, binding.authority(), at);
                statement.execute("COMMIT");
                committed = true;
                probe.at(
                    inputResource
                        ? RetentionStore.Phase.INPUT_RELEASE_COMMITTED
                        : RetentionStore.Phase.OUTPUT_RELEASE_COMMITTED);
                return RetentionStore.Result.RELEASED;
              }
            } catch (IOException | SQLException | RuntimeException | Error failure) {
              if (!committed) rollback(connection, failure);
              throw failure;
            }
          }
        }
        throw corrupt("resource release failed to advance its committed intent");
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
   * Reconcile one discovered resource using current metadata, never file age or an inferred missing
   * job. Known declared membership, admission receipts and any competing admitted job must agree.
   * Unknown or partially retiring sessions are refused, not treated as orphans. The caller must
   * keep upload completion and admission under the input-store monitor when performing them as one
   * live handoff; otherwise a never-admitted upload is only staging.
   *
   * @param inputs exclusively paired installation
   * @param candidate checked physical discovery hint
   * @param clock trusted UTC source checked before destructive work
   * @return retained, physically pinned, released or already absent
   * @throws IOException contradictory files or synchronization failure
   * @throws SQLException missing/contradictory admitted evidence or database failure
   */
  OrphanStore.Result reclaimOrphan(
      InputStore inputs, InputStore.OrphanCandidate candidate, AdmissionStore.Clock clock)
      throws IOException, SQLException {
    Objects.requireNonNull(inputs);
    Objects.requireNonNull(candidate);
    AdmissionStore.Clock checkedClock = AdmissionStore.checkedClock(clock);
    synchronized (inputs) {
      inputs.verifyAuthority(identity);
      inputs.verifyOrphanCandidate(candidate);
      if (!DATABASE_OPERATIONS.tryAcquire())
        throw ProtocolError.limit("V2 database operation capacity");
      try (Connection connection = database.connect();
          var statement = connection.createStatement()) {
        statement.execute("BEGIN IMMEDIATE");
        boolean committed = false;
        try {
          Metadata metadata = metadata(connection);
          if (!inputs.identity().equals(metadata.inputs()))
            throw corrupt("orphan input storage pairing differs");
          Retained retained = retained(connection, candidate.context().generation());
          if (retained == null)
            throw error(ProtocolError.Code.NOT_FOUND, "orphan candidate lacks a retained session");
          if (retained.retiring())
            throw error(
                ProtocolError.Code.EXPIRED, "orphan candidate belongs to a retiring session");
          Binding binding = retained.binding();
          Commitments.Context context =
              new Commitments.Context(binding.authority(), binding.owner(), binding.generation());
          if (!context.equals(candidate.context()))
            throw corrupt("orphan candidate contradicts retained owner or authority");
          OrphanStore.Result result;
          OrphanStore.Reference reference =
              OrphanStore.reference(connection, config, binding, candidate);
          if (reference == OrphanStore.Reference.LIVE) {
            result = OrphanStore.Result.RETAINED;
          } else if (reference == OrphanStore.Reference.RELEASED) {
            inputs.verifyReleasedCandidate(candidate);
            result = OrphanStore.Result.ABSENT;
          } else if (inputs.inputInUse(context, candidate.header())
              || inputs.outputInUse(context, candidate.header())) {
            result = OrphanStore.Result.PINNED;
          } else {
            inputs.verifyAuthority(identity);
            long at = AdmissionStore.now(connection, binding.authority(), checkedClock);
            boolean removed = inputs.reclaimOrphan(candidate);
            inputs.verifyAuthority(identity);
            AdmissionStore.remember(connection, config, binding.authority(), at);
            result = removed ? OrphanStore.Result.RELEASED : OrphanStore.Result.ABSENT;
          }
          statement.execute("COMMIT");
          committed = true;
          return result;
        } catch (IOException | SQLException | RuntimeException | Error failure) {
          if (!committed) rollback(connection, failure);
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
   * Retire one closed session after all retention promises and physical resources have ended. The
   * initial call commits only immutable eligibility. Later calls remove at most {@code limit}
   * metadata bundles, keeping the root and proof until their final atomic deletion.
   *
   * @param generation exact retained generation
   * @param inputs exclusively paired physical installation
   * @param limit maximum cleanup bundles, from one through 256
   * @param clock trusted nondecreasing UTC source
   * @return committed progress; absence never resets identity allocators
   * @throws IOException physical storage failure or contradictory resources
   * @throws SQLException corrupt retained evidence or transaction failure
   */
  RetirementStore.Progress retireSession(
      long generation, InputStore inputs, int limit, AdmissionStore.Clock clock)
      throws IOException, SQLException {
    return retireSession(generation, inputs, limit, clock, phase -> {});
  }

  /**
   * Retire one session with trusted local post-commit instrumentation for restart tests.
   *
   * @param generation exact retained generation
   * @param inputs exclusively paired physical installation
   * @param limit maximum cleanup bundles, from one through 256
   * @param clock trusted nondecreasing UTC source
   * @param probe observer invoked only after the named transaction commits
   * @return directly committed cleanup counts
   * @throws IOException physical storage failure or contradictory resources
   * @throws SQLException corrupt evidence, transaction failure or observer failure
   */
  RetirementStore.Progress retireSession(
      long generation,
      InputStore inputs,
      int limit,
      AdmissionStore.Clock clock,
      RetirementStore.Probe probe)
      throws IOException, SQLException {
    Checks.id(generation);
    if (limit < 1 || limit > 256) throw ProtocolError.limit("session retirement batch limit");
    Objects.requireNonNull(inputs);
    Objects.requireNonNull(probe);
    AdmissionStore.Clock checkedClock = AdmissionStore.checkedClock(clock);
    synchronized (inputs) {
      inputs.verifyAuthority(identity);
      if (!DATABASE_OPERATIONS.tryAcquire())
        throw ProtocolError.limit("V2 database operation capacity");
      int jobs = 0, operations = 0, entities = 0, scopes = 0;
      try {
        for (int unit = 0; unit < limit; unit++) {
          try (Connection connection = database.connect();
              var statement = connection.createStatement()) {
            statement.execute("BEGIN IMMEDIATE");
            boolean committed = false;
            try {
              Metadata metadata = metadata(connection);
              if (!inputs.identity().equals(metadata.inputs()))
                throw corrupt("retirement input storage pairing differs");
              Retained session = retained(connection, generation);
              if (session == null) {
                statement.execute("COMMIT");
                committed = true;
                return new RetirementStore.Progress(
                    RetirementStore.State.ABSENT, jobs, operations, entities, scopes);
              }
              Binding binding = session.binding();
              long now = AdmissionStore.now(connection, binding.authority(), checkedClock);
              RetirementRecord proof;
              if (!session.retiring()) {
                FixedRecords.audit(connection, config.files(), binding.authority());
                proof =
                    RetirementStore.eligible(
                        connection, config, binding, session.revoked(), inputs, now);
                if (proof == null) {
                  statement.execute("COMMIT");
                  committed = true;
                  return new RetirementStore.Progress(RetirementStore.State.NOT_READY, 0, 0, 0, 0);
                }
                if (inputs.sessionHasResources(RetirementStore.context(binding))) {
                  statement.execute("COMMIT");
                  committed = true;
                  return new RetirementStore.Progress(RetirementStore.State.PINNED, 0, 0, 0, 0);
                }
                inputs.verifyAuthority(identity);
                long at = AdmissionStore.now(connection, binding.authority(), checkedClock);
                proof =
                    new RetirementRecord(
                        proof.context(),
                        proof.creationSequence(),
                        proof.root(),
                        proof.cutoff(),
                        at);
                // No live-state audit exemption exists before this entire transaction commits.
                long slot =
                    FixedRecords.allocate(
                        connection,
                        config.files(),
                        FixedRecords.Kind.RETIREMENT,
                        RetirementStore.key(binding),
                        proof.encode(),
                        RetirementRecord.CAPACITY,
                        0);
                try (var update =
                    connection.prepareStatement(
                        "UPDATE ps_v2_sessions SET retiring=1,retirement_slot=?"
                            + " WHERE generation=? AND retiring=0 AND retirement_slot IS NULL")) {
                  update.setLong(1, slot);
                  update.setLong(2, generation);
                  if (update.executeUpdate() != 1) throw corrupt("retirement intent changed");
                }
                AdmissionStore.remember(connection, config, binding.authority(), at);
                RetirementStore.auditRemaining(connection, binding, proof);
                statement.execute("COMMIT");
                committed = true;
                probe.at(RetirementStore.Phase.INTENT_COMMITTED);
                return new RetirementStore.Progress(RetirementStore.State.STARTED, 0, 0, 0, 0);
              }
              proof = retirementProof(connection, binding);
              if (unit == 0) {
                FixedRecords.audit(connection, config.files(), binding.authority());
                RetirementStore.auditRemaining(connection, binding, proof);
                if (inputs.sessionHasResources(RetirementStore.context(binding)))
                  throw corrupt("retirement intent contradicted by physical resources");
              }
              FixedRecords.protect(connection, config.files());
              RetirementStore.Phase phase = RetirementStore.removeOne(connection, binding, proof);
              inputs.verifyAuthority(identity);
              long at = AdmissionStore.now(connection, binding.authority(), checkedClock);
              AdmissionStore.remember(connection, config, binding.authority(), at);
              statement.execute("COMMIT");
              committed = true;
              switch (phase) {
                case JOB_REMOVED -> jobs++;
                case OPERATION_REMOVED -> operations++;
                case ENTITY_REMOVED -> entities++;
                case SCOPE_REMOVED -> scopes++;
                case FINISHED -> {}
                case INTENT_COMMITTED -> throw new AssertionError("cleanup returned intent");
              }
              probe.at(phase);
              if (phase == RetirementStore.Phase.FINISHED)
                return new RetirementStore.Progress(
                    RetirementStore.State.COMPLETE, jobs, operations, entities, scopes);
            } catch (IOException | SQLException | RuntimeException | Error failure) {
              if (!committed) rollback(connection, failure);
              throw failure;
            }
          }
        }
        return new RetirementStore.Progress(
            RetirementStore.State.IN_PROGRESS, jobs, operations, entities, scopes);
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
   * Discover closed or retiring sessions in a finite keyset sweep. Open sessions consume the
   * examined-row budget too; a closed-root hint neither checks time nor authorizes deletion.
   *
   * @param cursor prior continuation, null to capture a new generation high-water mark
   * @param limit maximum examined sessions, from one through 64
   * @return bounded current hints and optional continuation
   * @throws SQLException corrupt session/root evidence or database failure
   */
  RetirementStore.Page scanRetirements(RetirementStore.ScanCursor cursor, int limit)
      throws SQLException {
    if (limit < 1 || limit > 64) throw ProtocolError.limit("retirement discovery page capacity");
    if (!DATABASE_OPERATIONS.tryAcquire())
      throw ProtocolError.limit("V2 database operation capacity");
    try (Connection connection = database.connect();
        var statement = connection.createStatement()) {
      statement.execute("PRAGMA query_only=ON");
      statement.execute("BEGIN");
      try {
        long highWater = metadata(connection).highWater();
        long through = cursor == null ? highWater : cursor.through();
        long after = cursor == null ? 0 : cursor.after();
        if (through > highWater) throw corrupt("retirement cursor exceeds authority history");
        List<Long> generations = new ArrayList<>(limit);
        int examined = 0;
        long last = after;
        boolean more = false;
        try (var query =
            connection.prepareStatement(
                "SELECT generation FROM ps_v2_sessions WHERE generation>? AND generation<=?"
                    + " ORDER BY generation LIMIT ?")) {
          query.setLong(1, after);
          query.setLong(2, through);
          query.setInt(3, limit + 1);
          try (var rows = query.executeQuery()) {
            while (rows.next()) {
              if (examined == limit) {
                more = true;
                break;
              }
              last = rows.getLong(1);
              examined++;
              Retained session = retained(connection, last);
              if (session.retiring()
                  || DeclarationStore.scope(connection, session.binding(), 0).state().summary()
                      != null) generations.add(last);
            }
          }
        }
        statement.execute("COMMIT");
        return new RetirementStore.Page(
            generations, examined, more ? new RetirementStore.ScanCursor(last, through) : null);
      } catch (SQLException | RuntimeException failure) {
        rollback(connection, failure);
        throw failure;
      }
    } finally {
      DATABASE_OPERATIONS.release();
    }
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
            version INTEGER NOT NULL CHECK(version=9),
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
            retirement_slot INTEGER UNIQUE REFERENCES ps_v2_slots(id),
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
          if (session.retiring()) {
            RetirementStore.auditRemaining(
                connection, session.binding(), retirementProof(connection, session.binding()));
          }
          if (!session.retiring()) DeclarationStore.audit(connection, session.binding());
          if (!session.retiring()) AdmissionStore.audit(connection, config, session.binding());
          if (!session.retiring())
            FenceStore.auditScopes(connection, session.binding(), session.revoked());
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
              required_control,required_object,retirement_slot
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
        Long retirementSlot = row.getObject(11) == null ? null : row.getLong(11);
        RetirementStore.load(connection, binding, row.getInt(8) != 0, retirementSlot);
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
      }
    }
    Retained retained = retained(connection, generation);
    if (retained.retiring())
      throw error(ProtocolError.Code.EXPIRED, "session retirement committed");
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
    return retained;
  }

  private RetirementRecord retirementProof(Connection connection, Binding binding)
      throws SQLException {
    try (var query =
        connection.prepareStatement(
            "SELECT retiring,retirement_slot FROM ps_v2_sessions WHERE generation=?")) {
      query.setLong(1, binding.generation());
      try (var row = query.executeQuery()) {
        if (!row.next()) throw corrupt("retirement session disappeared");
        return RetirementStore.load(
            connection,
            binding,
            row.getInt(1) != 0,
            row.getObject(2) == null ? null : row.getLong(2));
      }
    }
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
