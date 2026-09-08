package ai.pipestream.quic.v2;

import static ai.pipestream.quic.v2.Messages.*;
import static ai.pipestream.quic.v2.Records.*;

import ai.pipestream.quic.BoundedSqlite;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;

/**
 * Durable membership and producer-operation records inside SessionStore's authenticated
 * transactions. All helpers require the caller's checked session binding. None allocates a child,
 * admits input, executes work, acknowledges closure or bypasses a local worker fence.
 */
final class DeclarationStore {
  private static final int REQUEST_BYTES = 4101;
  private static final int RECEIPT_BYTES = 1024;
  private static final int VIEW_BYTES = 1048576;

  /**
   * Retained scope image and its slot revision.
   *
   * @param slot fixed-record slot identifier
   * @param revision fixed-record revision
   * @param state decoded scope state
   */
  record Scope(long slot, long revision, ScopeState state) {
    /**
     * Returns the immutable scope identifier.
     *
     * @return immutable scope identifier
     */
    long id() {
      return state.id();
    }

    /**
     * Returns the immutable producer identifier.
     *
     * @return immutable producer identifier
     */
    int producer() {
      return state.producer();
    }

    /**
     * Returns the immutable parent work key, or {@code null} for the root.
     *
     * @return immutable parent work key, or {@code null}
     */
    WorkKey parent() {
      return state.parent();
    }

    /**
     * Returns the retained membership seal, or {@code null} before sealing.
     *
     * @return retained membership seal, or {@code null}
     */
    Digest seal() {
      return state.seal();
    }

    /**
     * Returns the accepted member count.
     *
     * @return accepted member count
     */
    long declared() {
      return state.declared();
    }

    /**
     * Returns the highest accepted member identifier.
     *
     * @return highest accepted member identifier
     */
    long last() {
      return state.last();
    }

    /**
     * Returns this scope with replacement membership counters and digest.
     *
     * @param count replacement accepted-member count
     * @param highWater replacement highest member identifier
     * @param digest replacement membership digest
     * @return scope with replacement membership state
     */
    Scope members(long count, long highWater, Digest digest) {
      return new Scope(slot, revision, state.members(count, highWater, digest));
    }
  }

  /**
   * Retained declaration-backed work view and its fixed-record geometry.
   *
   * @param slot fixed-record slot identifier
   * @param revision fixed-record revision
   * @param view decoded work view
   * @param declaration declaration operation identifier
   * @param geometry fixed-record geometry
   */
  record Entity(
      long slot,
      long revision,
      WorkView view,
      OperationId declaration,
      FixedRecords.Header geometry) {}

  /**
   * Decoded durable declaration request, admitted input header, and replay receipt.
   *
   * @param request decoded declaration request
   * @param input admitted input header
   * @param receipt replay receipt
   */
  record Operation(Declare request, InputHeader input, OperationReceipt receipt) {}

  private record Usage(long entities, long operations) {}

  private DeclarationStore() {}

  /**
   * Create this version's tables in the same initialization transaction as the session schema.
   *
   * @param connection owned initialization transaction
   * @throws SQLException for schema or storage failure
   */
  static void createSchema(Connection connection) throws SQLException {
    try (var sql = connection.createStatement()) {
      sql.execute(
          """
          CREATE TABLE ps_v2_entities (
            generation INTEGER NOT NULL, scope INTEGER NOT NULL, id INTEGER NOT NULL CHECK(id>0),
            producer INTEGER NOT NULL CHECK(producer IN (0,1)),
            declaration BLOB NOT NULL CHECK(length(declaration)=16),
            view_slot INTEGER NOT NULL UNIQUE REFERENCES ps_v2_slots(id),
            fence_slot INTEGER NOT NULL UNIQUE REFERENCES ps_v2_slots(id),
            PRIMARY KEY(generation,scope,id),
            FOREIGN KEY(generation,scope,producer) REFERENCES ps_v2_scopes(generation,id,producer),
            FOREIGN KEY(generation,producer,declaration) REFERENCES ps_v2_operations(generation,producer,operation)
              DEFERRABLE INITIALLY DEFERRED
          ) STRICT
          """);
      sql.execute(
          """
          CREATE TABLE ps_v2_operations (
            generation INTEGER NOT NULL REFERENCES ps_v2_sessions(generation),
            producer INTEGER NOT NULL CHECK(producer IN (0,1)),
            operation BLOB NOT NULL CHECK(length(operation)=16 AND operation!=zeroblob(16)),
            request_kind INTEGER NOT NULL CHECK(request_kind IN (0,1)),
            request BLOB NOT NULL CHECK(length(request) BETWEEN 1 AND 4101),
            request_digest BLOB NOT NULL CHECK(length(request_digest)=32),
            receipt BLOB NOT NULL CHECK(length(receipt) BETWEEN 1 AND 1024),
            record_hash BLOB NOT NULL CHECK(length(record_hash)=32),
            PRIMARY KEY(generation,producer,operation)
          ) STRICT
          """);
    }
  }

  /**
   * Commit or replay a caller declaration. All writes use the enclosing writer transaction.
   *
   * @param connection checked writer transaction
   * @param files immutable guarded file policy used to preserve completion writes
   * @param binding authorized immutable session
   * @param selected current selected limits
   * @param request immutable declaration
   * @return correlated declaration receipt
   * @throws SQLException for storage failure or corruption
   */
  static DeclarationResponse declare(
      Connection connection,
      BoundedSqlite.Limits files,
      Binding binding,
      Capabilities selected,
      Declare request)
      throws SQLException {
    return declare(connection, files, binding, selected, 0, request);
  }

  /**
   * Commit membership in an explicitly authorized producer namespace. The enclosing transaction
   * must enforce the local parent fence for producer one, including on replay.
   *
   * @param connection checked writer transaction
   * @param files guarded file policy
   * @param binding authorized immutable session
   * @param selected selected limits
   * @param producer authorized operation originator
   * @param request immutable declaration
   * @return correlated retained receipt
   * @throws SQLException storage failure or corrupt retained evidence
   */
  static DeclarationResponse declare(
      Connection connection,
      BoundedSqlite.Limits files,
      Binding binding,
      Capabilities selected,
      int producer,
      Declare request)
      throws SQLException {
    Checks.producer(producer);
    Commitments.Context context = context(binding);
    Digest digest = Commitments.operation(context, producer, request);
    Operation prior = operation(connection, binding, producer, request.operation());
    if (prior != null) {
      if (!digest.equals(prior.receipt().requestDigest())
          || !normalized(request).equals(prior.request()))
        throw error(
            ProtocolError.Code.CONFLICT, "operation identity has different immutable parameters");
      return new DeclarationResponse(request.request(), prior.receipt());
    }
    Scope scope = scope(connection, binding, request.scope());
    if (scope.producer() != producer)
      throw error(ProtocolError.Code.UNAUTHORIZED, "cannot declare another producer's scope");
    if (scope.seal() != null)
      throw error(ProtocolError.Code.CONFLICT, "scope membership is sealed");
    if (!request.entityIds().isEmpty() && request.entityIds().getFirst() <= scope.last())
      throw error(
          ProtocolError.Code.CONFLICT, "entity identity is not above the scope high-water mark");

    Usage usage = usage(connection, binding);
    int added = request.entityIds().size();
    if (added > binding.limits().entities() - usage.entities()
        || usage.operations() >= binding.limits().operations())
      throw ProtocolError.limit("session declaration or operation capacity");
    long declared = add(scope.declared(), added);
    long last = request.entityIds().isEmpty() ? scope.last() : request.entityIds().getLast();
    try (var insert =
        connection.prepareStatement(
            """
            INSERT INTO ps_v2_entities(generation,scope,id,producer,declaration,view_slot,fence_slot)
              VALUES (?,?,?,?,?,?,?)
            """)) {
      for (long entity : request.entityIds()) {
        WorkView view =
            new WorkView(
                new WorkKey(scope.id(), scope.producer(), entity),
                State.DECLARED,
                0,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null);
        byte[] bytes = Wire.encodeRecord(view, VIEW_BYTES);
        // Test the largest future connection correlation representation of this observed record.
        Wire.encode(new WatchResponse(Long.MAX_VALUE, 1, view), selected.controlLimit());
        insert.setLong(1, binding.generation());
        insert.setLong(2, scope.id());
        insert.setLong(3, entity);
        insert.setInt(4, scope.producer());
        insert.setBytes(5, request.operation().bytes());
        long viewSlot =
            FixedRecords.allocate(
                connection,
                files,
                FixedRecords.Kind.WORK,
                FixedRecords.key(
                    binding,
                    FixedRecords.Kind.WORK,
                    scope.id(),
                    scope.producer(),
                    entity,
                    request.operation().bytes()),
                bytes,
                FixedRecords.WORK_CAPACITY,
                FixedRecords.WORK_CREDITS);
        long fenceSlot =
            FixedRecords.allocate(
                connection,
                files,
                FixedRecords.Kind.FENCE,
                FixedRecords.key(
                    binding,
                    FixedRecords.Kind.FENCE,
                    scope.id(),
                    scope.producer(),
                    entity,
                    request.operation().bytes()),
                new byte[] {(byte) 0xf6},
                FixedRecords.FENCE_CAPACITY,
                FixedRecords.FENCE_CREDITS);
        insert.setLong(6, viewSlot);
        insert.setLong(7, fenceSlot);
        insert.executeUpdate();
      }
    }
    Digest seal =
        request.seal() ? seal(connection, binding, scope.members(declared, last, null)) : null;
    OperationReceipt receipt =
        new OperationReceipt(
            request.operation(),
            digest,
            new Declared(scope.id(), scope.producer(), added, declared, seal));
    Wire.encode(new DeclarationResponse(Long.MAX_VALUE, receipt), selected.controlLimit());
    byte[] requestBytes = Wire.encode(normalized(request), Wire.INITIAL_CONTROL_LIMIT);
    byte[] receiptBytes = Wire.encodeRecord(receipt, RECEIPT_BYTES);
    FixedRecords.replace(
        connection,
        files,
        scope.slot(),
        FixedRecords.Kind.SCOPE,
        FixedRecords.key(binding, FixedRecords.Kind.SCOPE, scope.id(), scope.producer(), 0, null),
        scope.revision(),
        scope.members(declared, last, seal).state().encode(),
        false);
    try (var update =
        connection.prepareStatement(
            """
            UPDATE ps_v2_sessions SET entity_count=?,operation_count=? WHERE generation=?
            """)) {
      update.setLong(1, add(usage.entities(), added));
      update.setLong(2, add(usage.operations(), 1));
      update.setLong(3, binding.generation());
      if (update.executeUpdate() != 1)
        throw corrupt("session disappeared inside declaration transaction");
    }
    try (var insert =
        connection.prepareStatement(
            """
            INSERT INTO ps_v2_operations(generation,producer,operation,request_kind,request,request_digest,receipt,record_hash)
              VALUES (?,?,?,0,?,?,?,?)
            """)) {
      insert.setLong(1, binding.generation());
      insert.setInt(2, producer);
      insert.setBytes(3, request.operation().bytes());
      insert.setBytes(4, requestBytes);
      insert.setBytes(5, digest.bytes());
      insert.setBytes(6, receiptBytes);
      insert.setBytes(7, operationHash(binding, producer, 0, requestBytes, receiptBytes));
      insert.executeUpdate();
    }
    return new DeclarationResponse(request.request(), receipt);
  }

  /**
   * Retrieve retained caller operation evidence, not an assertion of noncommit on absence.
   *
   * @param connection checked read transaction
   * @param binding authorized session
   * @param request operation lookup
   * @return correlated retained receipt
   * @throws SQLException for corruption or storage failure
   */
  static OperationResponse lookup(Connection connection, Binding binding, LookupOperation request)
      throws SQLException {
    Operation operation = operation(connection, binding, request.operation());
    if (operation == null)
      throw error(ProtocolError.Code.NOT_FOUND, "operation receipt unavailable");
    return new OperationResponse(request.request(), operation.receipt());
  }

  /**
   * Read a single bounded page; full sealed membership is not inferred from a partial page.
   *
   * @param connection checked read transaction
   * @param binding authorized session
   * @param request page bounds
   * @return snapshot with exact continuation flag
   * @throws SQLException for corruption or storage failure
   */
  static PageResponse page(Connection connection, Binding binding, Page request)
      throws SQLException {
    Scope scope = scope(connection, binding, request.scope());
    var entries = new ArrayList<Entry>(request.limit());
    boolean more = false;
    try (var query =
        connection.prepareStatement(
            """
              SELECT id,view_slot,fence_slot,
            CASE WHEN length(declaration)=16 THEN declaration END,producer
              FROM ps_v2_entities WHERE generation=? AND scope=? AND id>? ORDER BY id LIMIT ?
            """)) {
      query.setLong(1, binding.generation());
      query.setLong(2, scope.id());
      query.setLong(3, request.afterEntity());
      query.setInt(4, request.limit() + 1);
      try (var rows = query.executeQuery()) {
        while (rows.next()) {
          Entity value = entity(connection, binding, scope, rows);
          if (entries.size() == request.limit()) {
            more = true;
            break;
          }
          entries.add(new Entry(value.view().work().entity(), value.view().state()));
        }
      }
    }
    return new PageResponse(
        request.request(),
        scope.id(),
        scope.producer(),
        scope.parent(),
        scope.seal() != null,
        scope.seal(),
        scope.declared(),
        entries,
        more);
  }

  /**
   * Take a current view without holding a database transaction for a connection-local wait.
   *
   * @param connection checked read transaction
   * @param binding authorized session
   * @param request identity and revision comparison
   * @return current revision and exact view
   * @throws SQLException for corruption or storage failure
   */
  static WatchResponse snapshot(Connection connection, Binding binding, Watch request)
      throws SQLException {
    Scope scope = scope(connection, binding, request.work().scope());
    if (scope.producer() != request.work().producer())
      throw error(ProtocolError.Code.CONFLICT, "work producer differs from its scope");
    try (var query =
        connection.prepareStatement(
            """
              SELECT id,view_slot,fence_slot,
            CASE WHEN length(declaration)=16 THEN declaration END,producer
              FROM ps_v2_entities WHERE generation=? AND scope=? AND id=?
            """)) {
      query.setLong(1, binding.generation());
      query.setLong(2, scope.id());
      query.setLong(3, request.work().entity());
      try (var rows = query.executeQuery()) {
        if (!rows.next()) throw error(ProtocolError.Code.NOT_FOUND, "work identity is undeclared");
        Entity entity = entity(connection, binding, scope, rows);
        if (request.afterRevision() > entity.revision())
          throw error(ProtocolError.Code.CONFLICT, "observed revision is ahead of retained work");
        return new WatchResponse(request.request(), entity.revision(), entity.view());
      }
    }
  }

  /**
   * Audit one live session's actual records and charges before admitting recovered capacity.
   *
   * @param connection initialization snapshot
   * @param binding decoded immutable live session
   * @throws SQLException for missing, contradictory or corrupt records
   */
  static void audit(Connection connection, Binding binding) throws SQLException {
    Usage usage = usage(connection, binding);
    if (count(connection, "ps_v2_entities", binding.generation()) != usage.entities()
        || count(connection, "ps_v2_operations", binding.generation()) != usage.operations())
      throw corrupt("declaration accounting differs from retained rows");
    long summarized = -1;
    try (var query =
        connection.prepareStatement("SELECT id FROM ps_v2_scopes WHERE generation=? ORDER BY id")) {
      query.setLong(1, binding.generation());
      try (var scopes = query.executeQuery()) {
        while (scopes.next()) {
          Scope scope = scope(connection, binding, scopes.getLong(1));
          if (scope.state().summary() != null) summarized = scope.id();
          long observed = 0, last = 0;
          try (var members =
              connection.prepareStatement(
                  """
                    SELECT id,view_slot,fence_slot,
                  CASE WHEN length(declaration)=16 THEN declaration END,producer
                    FROM ps_v2_entities WHERE generation=? AND scope=? ORDER BY id
                  """)) {
            members.setLong(1, binding.generation());
            members.setLong(2, scope.id());
            try (var rows = members.executeQuery()) {
              while (rows.next()) {
                last = entity(connection, binding, scope, rows).view().work().entity();
                observed++;
              }
            }
          }
          if (observed != scope.declared() || last != scope.last())
            throw corrupt("scope membership accounting differs");
          if (scope.seal() != null && !scope.seal().equals(seal(connection, binding, scope)))
            throw corrupt("scope seal does not commit retained membership");
        }
      }
    }
    if (summarized >= 0) ClosureStore.verify(connection, binding, summarized);
    long covered = 0;
    try (var query =
        connection.prepareStatement(
            "SELECT producer,CASE WHEN length(operation)=16 THEN operation END FROM"
                + " ps_v2_operations WHERE generation=?")) {
      query.setLong(1, binding.generation());
      try (var rows = query.executeQuery()) {
        while (rows.next()) {
          int producer = rows.getInt(1);
          byte[] bytes = rows.getBytes(2);
          if (bytes == null || bytes.length != 16)
            throw corrupt("invalid retained operation identity");
          try {
            Operation value = operation(connection, binding, producer, new OperationId(bytes));
            if (value == null) throw corrupt("retained operation disappeared inside audit");
            int accepted = value.request() == null ? 0 : value.request().entityIds().size();
            if (accepted > usage.entities() - covered)
              throw corrupt("declaration receipts exceed retained membership");
            covered += accepted;
          } catch (ProtocolError invalid) {
            throw corrupt("invalid retained operation identity", invalid);
          }
        }
      }
    }
    if (covered != usage.entities())
      throw corrupt("declaration receipts do not cover retained membership");
  }

  /**
   * Load and validate one retained scope under its session binding.
   *
   * @param connection open database connection
   * @param binding immutable session binding
   * @param id scope identifier
   * @return validated retained scope
   * @throws SQLException for storage or retained-state validation failure
   */
  static Scope scope(Connection connection, Binding binding, long id) throws SQLException {
    try (var query =
        connection.prepareStatement(
            """
            SELECT producer,parent_scope,parent_producer,parent_entity,state_slot
            FROM ps_v2_scopes WHERE generation=? AND id=?
            """)) {
      query.setLong(1, binding.generation());
      query.setLong(2, id);
      try (var row = query.executeQuery()) {
        if (!row.next()) throw error(ProtocolError.Code.NOT_FOUND, "scope unavailable");
        try {
          int producer = row.getInt(1);
          WorkKey parent =
              row.getObject(2) == null
                  ? null
                  : new WorkKey(row.getLong(2), row.getInt(3), row.getLong(4));
          Checks.scope(id, producer, parent);
          long slot = row.getLong(5);
          FixedRecords.Snapshot image =
              FixedRecords.read(
                  connection,
                  slot,
                  FixedRecords.Kind.SCOPE,
                  FixedRecords.key(binding, FixedRecords.Kind.SCOPE, id, producer, 0, null));
          ScopeState state = ScopeState.decode(image.body());
          if (state.id() != id
              || state.producer() != producer
              || !java.util.Objects.equals(state.parent(), parent))
            throw corrupt("scope image differs from immutable identity");
          return new Scope(slot, image.header().revision(), state);
        } catch (ProtocolError invalid) {
          throw corrupt("invalid retained scope metadata", invalid);
        }
      }
    }
  }

  private static Entity entity(Connection connection, Binding binding, Scope scope, ResultSet row)
      throws SQLException {
    long id = row.getLong(1), viewSlot = row.getLong(2), fenceSlot = row.getLong(3);
    byte[] declaration = row.getBytes(4);
    if (id <= 0 || declaration == null || row.getInt(5) != scope.producer())
      throw corrupt("retained work view integrity failure");
    try {
      FixedRecords.Snapshot image =
          FixedRecords.read(
              connection,
              viewSlot,
              FixedRecords.Kind.WORK,
              FixedRecords.key(
                  binding, FixedRecords.Kind.WORK, scope.id(), scope.producer(), id, declaration));
      FixedRecords.Snapshot fence =
          FixedRecords.read(
              connection,
              fenceSlot,
              FixedRecords.Kind.FENCE,
              FixedRecords.key(
                  binding, FixedRecords.Kind.FENCE, scope.id(), scope.producer(), id, declaration));
      if (!Arrays.equals(fence.body(), new byte[] {(byte) 0xf6}))
        throw corrupt("declaration-only store contains a lifecycle fence");
      WorkView view =
          (WorkView) Wire.decodeRecord(Wire.RecordKind.WORK_VIEW, image.body(), VIEW_BYTES);
      if (!view.work().equals(new WorkKey(scope.id(), scope.producer(), id)))
        throw corrupt("work identity differs from retained scope");
      new OperationId(declaration);
      return new Entity(
          viewSlot, image.header().revision(), view, new OperationId(declaration), image.header());
    } catch (ProtocolError invalid) {
      throw corrupt("invalid retained work view", invalid);
    }
  }

  /**
   * Load and validate one producer-zero declaration operation, or return {@code null} when absent.
   *
   * @param connection open database connection
   * @param binding immutable session binding
   * @param id declaration operation identifier
   * @return validated operation, or {@code null} when absent
   * @throws SQLException for storage or retained-state validation failure
   */
  static Operation operation(Connection connection, Binding binding, OperationId id)
      throws SQLException {
    return operation(connection, binding, 0, id);
  }

  /**
   * Load one operation from its explicit producer namespace, validating its complete retained
   * commitment and target. This helper grants no producer authority.
   *
   * @param connection checked metadata snapshot
   * @param binding immutable session binding
   * @param producer operation originator
   * @param id operation identity within that namespace
   * @return validated operation, or null when absent
   * @throws SQLException storage failure or corrupt retained evidence
   */
  static Operation operation(Connection connection, Binding binding, int producer, OperationId id)
      throws SQLException {
    Checks.producer(producer);
    try (var query =
        connection.prepareStatement(
            """
            SELECT CASE WHEN length(request)<=4101 THEN request END,
              CASE WHEN length(request_digest)=32 THEN request_digest END,
              CASE WHEN length(receipt)<=1024 THEN receipt END,
              CASE WHEN length(record_hash)=32 THEN record_hash END,request_kind
            FROM ps_v2_operations WHERE generation=? AND producer=? AND operation=?
            """)) {
      query.setLong(1, binding.generation());
      query.setInt(2, producer);
      query.setBytes(3, id.bytes());
      try (var row = query.executeQuery()) {
        if (!row.next()) return null;
        byte[] requestBytes = row.getBytes(1),
            digest = row.getBytes(2),
            receiptBytes = row.getBytes(3),
            hash = row.getBytes(4);
        int kind = row.getInt(5);
        if (requestBytes == null
            || digest == null
            || receiptBytes == null
            || hash == null
            || requestBytes.length > REQUEST_BYTES
            || kind < 0
            || kind > 1
            || !Arrays.equals(
                hash, operationHash(binding, producer, kind, requestBytes, receiptBytes)))
          throw corrupt("operation record integrity failure");
        try {
          if (kind == 1) {
            InputHeader input =
                (InputHeader) Wire.decodeRecord(Wire.RecordKind.INPUT_HEADER, requestBytes, 4096);
            OperationReceipt receipt =
                (OperationReceipt)
                    Wire.decodeRecord(
                        Wire.RecordKind.OPERATION_RECEIPT, receiptBytes, RECEIPT_BYTES);
            Digest expected = Commitments.operation(context(binding), producer, input);
            if (!input.operation().equals(id)
                || input.generation() != binding.generation()
                || input.parameters().work().producer() != producer
                || !receipt.operation().equals(id)
                || !receipt.requestDigest().equals(expected)
                || !Arrays.equals(digest, expected.bytes())
                || !(receipt.outcome() instanceof Admitted admitted))
              throw corrupt("admission receipt differs from immutable intent");
            AdmissionStore.validateReceipt(connection, binding, input, admitted);
            return new Operation(null, input, receipt);
          }
          Wire.Frame frame = Wire.decode(requestBytes, Wire.INITIAL_CONTROL_LIMIT);
          if (!(frame instanceof Wire.Known known)
              || !(known.message() instanceof Declare request)
              || request.request() != 1
              || !request.operation().equals(id))
            throw corrupt("invalid retained declaration request");
          OperationReceipt receipt =
              (OperationReceipt)
                  Wire.decodeRecord(Wire.RecordKind.OPERATION_RECEIPT, receiptBytes, RECEIPT_BYTES);
          Digest expected = Commitments.operation(context(binding), producer, request);
          if (!id.equals(receipt.operation())
              || !expected.equals(receipt.requestDigest())
              || !Arrays.equals(digest, expected.bytes())
              || !(receipt.outcome() instanceof Declared declared)
              || declared.scope() != request.scope()
              || declared.producer() != producer
              || declared.acceptedCount() != request.entityIds().size()
              || request.seal() != (declared.seal() != null))
            throw corrupt("declaration receipt differs from immutable intent");
          Scope scope = scope(connection, binding, request.scope());
          if (scope.producer() != declared.producer()
              || declared.declared() > scope.declared()
              || (declared.seal() != null
                  && (!declared.seal().equals(scope.seal())
                      || declared.declared() != scope.declared())))
            throw corrupt("declaration receipt contradicts retained scope");
          try (var member =
              connection.prepareStatement(
                  """
                  SELECT producer,CASE WHEN length(declaration)=16 THEN declaration END
                    FROM ps_v2_entities WHERE generation=? AND scope=? AND id=?
                  """)) {
            member.setLong(1, binding.generation());
            member.setLong(2, request.scope());
            for (long entity : request.entityIds()) {
              member.setLong(3, entity);
              try (var retained = member.executeQuery()) {
                if (!retained.next()
                    || retained.getInt(1) != declared.producer()
                    || !Arrays.equals(id.bytes(), retained.getBytes(2)))
                  throw corrupt("declaration receipt has missing or contradictory membership");
              }
            }
          }
          return new Operation(request, null, receipt);
        } catch (ProtocolError invalid) {
          throw corrupt("invalid retained operation encoding", invalid);
        }
      }
    }
  }

  /**
   * Read the checked identity and funded work image for one member.
   *
   * @param connection enclosing snapshot
   * @param binding authenticated session
   * @param work expected work identity
   * @return current work and immutable declaration linkage
   * @throws SQLException corrupt metadata or storage failure
   */
  static Entity member(Connection connection, Binding binding, WorkKey work) throws SQLException {
    Scope scope = scope(connection, binding, work.scope());
    if (scope.producer() != work.producer())
      throw error(ProtocolError.Code.CONFLICT, "work producer differs from scope");
    try (var query =
        connection.prepareStatement(
            """
            SELECT id,view_slot,fence_slot,CASE WHEN length(declaration)=16 THEN declaration END,producer
              FROM ps_v2_entities WHERE generation=? AND scope=? AND id=?
            """)) {
      query.setLong(1, binding.generation());
      query.setLong(2, work.scope());
      query.setLong(3, work.entity());
      try (var row = query.executeQuery()) {
        if (!row.next()) throw error(ProtocolError.Code.NOT_FOUND, "work identity is undeclared");
        return entity(connection, binding, scope, row);
      }
    }
  }

  /**
   * Retain an admission in the input producer's operation namespace, shared with declarations.
   *
   * @param connection admission writer transaction
   * @param binding authenticated immutable session
   * @param input exact input header
   * @param receipt committed typed outcome
   * @throws SQLException duplicate identity or storage failure
   */
  static void retainAdmission(
      Connection connection, Binding binding, InputHeader input, OperationReceipt receipt)
      throws SQLException {
    byte[] requestBytes = Wire.encodeRecord(input, 4096);
    byte[] receiptBytes = Wire.encodeRecord(receipt, RECEIPT_BYTES);
    int producer = input.parameters().work().producer();
    try (var insert =
        connection.prepareStatement(
            """
            INSERT INTO ps_v2_operations(generation,producer,operation,request_kind,request,request_digest,receipt,record_hash)
              VALUES (?,?,?,1,?,?,?,?)
            """)) {
      insert.setLong(1, binding.generation());
      insert.setInt(2, producer);
      insert.setBytes(3, input.operation().bytes());
      insert.setBytes(4, requestBytes);
      insert.setBytes(5, receipt.requestDigest().bytes());
      insert.setBytes(6, receiptBytes);
      insert.setBytes(7, operationHash(binding, producer, 1, requestBytes, receiptBytes));
      insert.executeUpdate();
    }
  }

  private static Usage usage(Connection connection, Binding binding) throws SQLException {
    try (var query =
        connection.prepareStatement(
            "SELECT entity_count,operation_count FROM ps_v2_sessions WHERE generation=?")) {
      query.setLong(1, binding.generation());
      try (var row = query.executeQuery()) {
        if (!row.next()) throw corrupt("session accounting missing");
        long entities = row.getLong(1), operations = row.getLong(2);
        if (entities < 0
            || entities > binding.limits().entities()
            || operations < 0
            || operations > binding.limits().operations())
          throw corrupt("session accounting exceeds retained limits");
        return new Usage(entities, operations);
      }
    }
  }

  private static Digest seal(Connection connection, Binding binding, Scope scope)
      throws SQLException {
    Commitments.Seal seal =
        new Commitments.Seal(
            context(binding), scope.id(), scope.producer(), scope.parent(), scope.declared());
    try (var query =
        connection.prepareStatement(
            "SELECT id FROM ps_v2_entities WHERE generation=? AND scope=? ORDER BY id")) {
      query.setLong(1, binding.generation());
      query.setLong(2, scope.id());
      try (var row = query.executeQuery()) {
        while (row.next()) seal.add(row.getLong(1));
      }
      return seal.finish();
    } catch (ProtocolError invalid) {
      throw corrupt("scope seal membership differs", invalid);
    }
  }

  private static long count(Connection connection, String table, long generation)
      throws SQLException {
    try (var query =
        connection.prepareStatement("SELECT count(*) FROM " + table + " WHERE generation=?")) {
      query.setLong(1, generation);
      try (var row = query.executeQuery()) {
        if (!row.next()) throw corrupt("missing aggregate count");
        return row.getLong(1);
      }
    }
  }

  private static Declare normalized(Declare request) {
    return new Declare(
        1, request.operation(), request.scope(), request.entityIds(), request.seal());
  }

  private static Commitments.Context context(Binding binding) {
    return new Commitments.Context(binding.authority(), binding.owner(), binding.generation());
  }

  private static Cbor.Writer hashWriter(
      java.security.MessageDigest digest, String domain, Binding binding) {
    Cbor.Writer out = new Cbor.Writer(digest);
    out.array(5);
    out.text(domain, 128);
    out.text(binding.authority(), 128);
    out.text(binding.owner(), 128);
    out.number(binding.generation());
    return out;
  }

  private static byte[] operationHash(
      Binding binding, int producer, int kind, byte[] request, byte[] receipt) {
    var digest = Commitments.sha256();
    Cbor.Writer out = hashWriter(digest, "pipestream-java-v2-operation", binding);
    out.array(4);
    out.number(producer);
    out.number(kind);
    out.bytes(request);
    out.bytes(receipt);
    return digest.digest();
  }

  private static long add(long left, long right) {
    if (left < 0 || right < 0 || right > Long.MAX_VALUE - left)
      throw ProtocolError.limit("declaration counter exhausted");
    return left + right;
  }

  private static ProtocolError error(ProtocolError.Code code, String detail) {
    return new ProtocolError(code, detail);
  }

  private static SQLException corrupt(String detail) {
    return new SQLException("V2 declarations: " + detail);
  }

  private static SQLException corrupt(String detail, Throwable cause) {
    return new SQLException("V2 declarations: " + detail, cause);
  }
}
