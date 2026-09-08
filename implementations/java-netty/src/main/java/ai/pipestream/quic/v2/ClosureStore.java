package ai.pipestream.quic.v2;

import static ai.pipestream.quic.v2.Records.*;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Objects;
import java.util.UUID;

/**
 * Produce and verify closure from authoritative ordered members and terminal outcomes. Partial
 * folds are volatile, not completion evidence. A membership seal alone never proves closure.
 */
final class ClosureStore {
  private ClosureStore() {}

  /**
   * Volatile descending sweep and incremental fold. Durable closure never depends on retaining this
   * object. The store synchronizes on it; callers must not use a cursor for another authority.
   */
  static final class Cursor {
    /** Authority installation that owns this scheduler cursor. */
    UUID installation;

    private Position before;

    /** Partially folded scope state passed to the transactional publisher. */
    Scan scan;

    /** Start without a retained sweep or partial commitment. */
    Cursor() {}

    /**
     * Inspect the partial status-tree payload, excluding fixed object and seal-hasher overhead.
     *
     * @return at most 63 retained 32-byte hashes
     */
    synchronized int retainedHashBytes() {
      return scan == null ? 0 : scan.status.retainedHashBytes();
    }
  }

  /**
   * Local work performed by one bounded closure step, not protocol completion counters.
   *
   * @param inspectedScopes zero or one directly selected scope
   * @param inspectedMembers directly folded members, excluding existing descendant audits
   * @param closedScopes newly committed scope summaries
   * @param settledParents newly committed STRICT parent failures
   */
  record Progress(
      int inspectedScopes, int inspectedMembers, int closedScopes, int settledParents) {}

  /**
   * Stable point in a descending local scope sweep.
   *
   * @param generation retained session generation
   * @param scope retained scope identifier
   */
  record Position(long generation, long scope) {}

  /** Commit boundaries available to local durability instrumentation, never peer-controlled. */
  enum Phase {
    /** Funded images and clock written inside the still-uncommitted transaction. */
    BEFORE_COMMIT,
    /** SQLite has successfully committed the new summary and any parent failure. */
    AFTER_COMMIT
  }

  /** Trusted local durability instrumentation; not an application processing callback. */
  @FunctionalInterface
  interface Probe {
    /**
     * Observe a closure commit boundary.
     *
     * @param phase exact transaction phase
     * @throws SQLException injected or observed local failure
     */
    void at(Phase phase) throws SQLException;
  }

  /** One bounded in-memory fold over immutable membership and terminal outcomes. */
  static final class Scan {
    private final Messages.Binding binding;
    private final ScopeState source;
    private final Commitments.Seal seal;
    private final Commitments.StatusTree status;
    private long after;
    private long latest;

    private Scan(Messages.Binding binding, DeclarationStore.Scope scope, long earliest) {
      this.binding = binding;
      source = scope.state();
      seal =
          new Commitments.Seal(
              new Commitments.Context(binding.authority(), binding.owner(), binding.generation()),
              scope.id(),
              scope.producer(),
              scope.parent(),
              scope.declared());
      status = new Commitments.StatusTree(scope.id(), scope.producer(), scope.declared());
      latest = earliest;
    }
  }

  /**
   * One direct member batch; null status means that no full closure is ready.
   *
   * @param inspected number of directly folded members
   * @param status completed status tree, or null while closure remains incomplete
   */
  record Batch(int inspected, Commitments.Status status) {}

  /**
   * Select the next scope position for a descending sweep.
   *
   * @param connection authoritative metadata snapshot
   * @param cursor local sweep state
   * @return next position, or null after the last scope
   * @throws SQLException for storage failure or an invalid retained identity
   */
  static Position next(Connection connection, Cursor cursor) throws SQLException {
    if (cursor.scan != null)
      return new Position(cursor.scan.binding.generation(), cursor.scan.source.id());
    try (var query =
        connection.prepareStatement(
            "SELECT generation,id FROM ps_v2_scopes"
                + (cursor.before == null ? "" : " WHERE (generation,id)<(?,?)")
                + " ORDER BY generation DESC,id DESC LIMIT 1")) {
      if (cursor.before != null) {
        query.setLong(1, cursor.before.generation());
        query.setLong(2, cursor.before.scope());
      }
      try (var rows = query.executeQuery()) {
        if (!rows.next()) return null;
        long generation = rows.getLong(1), scope = rows.getLong(2);
        if (generation < 1 || scope < 0) throw corrupt("invalid scope discovery identity");
        return new Position(generation, scope);
      }
    }
  }

  /**
   * Advance the local sweep beyond one fully handled position.
   *
   * @param cursor local sweep state
   * @param position last handled position, or null to restart the sweep
   */
  static void advance(Cursor cursor, Position position) {
    cursor.scan = null;
    cursor.before = position;
  }

  /**
   * Fold at most one bounded direct-member page into the cursor's in-progress closure.
   *
   * @param connection authoritative metadata snapshot
   * @param binding immutable session binding
   * @param scope selected retained scope
   * @param cursor local sweep and fold state
   * @param limit maximum direct members to inspect
   * @return inspection result and completed status when all members are terminal
   * @throws SQLException for contradictory retained evidence or storage failure
   */
  static Batch fold(
      Connection connection,
      Messages.Binding binding,
      DeclarationStore.Scope scope,
      Cursor cursor,
      int limit)
      throws SQLException {
    Position position = new Position(binding.generation(), scope.id());
    if (scope.state().summary() != null || scope.seal() == null) {
      advance(cursor, position);
      return new Batch(0, null);
    }
    Long parentTime = parentAdmission(connection, binding, scope);
    if (scope.parent() != null
        && DeclarationStore.member(connection, binding, scope.parent()).view().state()
            == State.SUCCEEDED)
      throw corrupt("successful parent lacks its prior committed child closure");
    if (cursor.scan == null)
      cursor.scan = new Scan(binding, scope, parentTime == null ? 0 : parentTime);
    Scan scan = cursor.scan;
    if (!scan.binding.equals(binding) || !scan.source.equals(scope.state()))
      throw corrupt("partial closure membership or scope state changed");
    boolean profiles = results(connection, binding);
    boolean descendantsAudited = false;
    int inspected = 0;
    boolean more = false;
    try (var query =
        connection.prepareStatement(
            "SELECT id FROM ps_v2_entities WHERE generation=? AND scope=? AND id>? ORDER BY id"
                + " LIMIT ?")) {
      query.setLong(1, binding.generation());
      query.setLong(2, scope.id());
      query.setLong(3, scan.after);
      query.setInt(4, limit + 1);
      try (var rows = query.executeQuery()) {
        while (rows.next()) {
          if (inspected == limit) {
            more = true;
            break;
          }
          inspected++;
          long id = rows.getLong(1);
          WorkView view =
              DeclarationStore.member(
                      connection, binding, new WorkKey(scope.id(), scope.producer(), id))
                  .view();
          if (!view.state().terminal()) {
            advance(cursor, position);
            return new Batch(inspected, null);
          }
          view.validateProfiles(profiles);
          if (view.manifest() != null
              && (!view.manifest().authority().equals(binding.authority())
                  || !view.manifest().owner().equals(binding.owner())
                  || view.manifest().generation() != binding.generation()))
            throw corrupt("terminal manifest belongs to another closure session");
          Digest child = null;
          if (view.child() != null) {
            DeclarationStore.Scope descendant =
                DeclarationStore.scope(connection, binding, view.child().scope());
            if (descendant.state().summary() == null) {
              advance(cursor, position);
              return new Batch(inspected, null);
            }
            // One audit covers every existing summary in this stable transaction. Do not trust
            // counters or a root merely because their bounded storage image has a checksum.
            if (!descendantsAudited) {
              verify(connection, binding, descendant.id());
              descendantsAudited = true;
            }
            child = childRoot(connection, binding, view, Long.MAX_VALUE);
            scan.latest = Math.max(scan.latest, descendant.state().summary().closedAt());
          }
          scan.seal.add(id);
          scan.status.add(view, child);
          scan.latest = Math.max(scan.latest, view.terminalAt());
          scan.after = id;
        }
      }
    }
    if (more) return new Batch(inspected, null);
    if (scan.after != scope.last() || !scan.seal.finish().equals(scope.seal()))
      throw corrupt("closure fold differs from complete sealed membership");
    return new Batch(inspected, scan.status.finish());
  }

  /**
   * Persist a fully verified summary for one scope.
   *
   * @param connection authoritative metadata transaction
   * @param config bounded storage configuration
   * @param binding immutable session binding
   * @param scope fully folded retained scope
   * @param scan completed in-memory fold
   * @param status completed status tree
   * @param now checked closure time
   * @return persisted scope summary
   * @throws SQLException for storage failure
   */
  static ScopeSummary publish(
      Connection connection,
      SessionStore.Configuration config,
      Messages.Binding binding,
      DeclarationStore.Scope scope,
      Scan scan,
      Commitments.Status status,
      long now)
      throws SQLException {
    if (now < scan.latest)
      throw new ProtocolError(
          ProtocolError.Code.CLOCK_UNSAFE, "closure time precedes retained terminal evidence");
    if (scope.id() == 0) rootReceiptUntil(binding, now);
    ScopeSummary summary =
        new ScopeSummary(
            scope.id(),
            scope.producer(),
            scope.parent(),
            scope.seal(),
            scope.declared(),
            status.counts(),
            status.root(),
            now);
    ScopeState source = scope.state();
    ScopeState replacement =
        new ScopeState(
            source.id(),
            source.producer(),
            source.parent(),
            source.declared(),
            source.last(),
            source.seal(),
            source.cancelled(),
            source.revoked(),
            summary);
    FixedRecords.replace(
        connection,
        config.files(),
        scope.slot(),
        FixedRecords.Kind.SCOPE,
        FixedRecords.key(binding, FixedRecords.Kind.SCOPE, scope.id(), scope.producer(), 0, null),
        scope.revision(),
        replacement.encode(),
        true);
    return summary;
  }

  /**
   * Calculate the retention boundary for a root closure receipt.
   *
   * @param binding immutable session binding
   * @param closedAt committed root closure time
   * @return exclusive receipt-retention boundary
   */
  static long rootReceiptUntil(Messages.Binding binding, long closedAt) {
    try {
      return Math.addExact(closedAt, binding.policy().receiptRetention());
    } catch (ArithmeticException overflow) {
      throw ProtocolError.limit("root closure retention overflows");
    }
  }

  /**
   * Verify the requested committed summary and every other retained summary in its session. The
   * caller must hold one stable metadata transaction throughout this call. Children have greater
   * scope identifiers, so descending traversal verifies their summaries before a parent's fold
   * consumes them. Open scopes are not falsely required to have summaries, but their actual parent
   * links are still checked; a summarized ancestor requires all its descendant summaries.
   *
   * <p>The conservative session-wide audit visits each scope and each member of a summarized scope
   * once, with indexed identity lookups. It retains one bounded work image and at most 63 status
   * hashes, not a recursive stack, whole-scope collection or subtree map. Repeated invocations
   * repeat this work; callers should not mistake the result for an authorization or cached proof
   * valid outside the current snapshot.
   *
   * @param connection stable authoritative metadata snapshot
   * @param binding checked immutable session binding
   * @param scopeId scope whose existing summary is required as evidence
   * @throws SQLException missing, contradictory or unsupported retained closure evidence
   */
  static void verify(Connection connection, Messages.Binding binding, long scopeId)
      throws SQLException {
    Objects.requireNonNull(connection);
    Objects.requireNonNull(binding);
    try {
      Checks.number(scopeId);
      boolean results = results(connection, binding);
      long watermark = watermark(connection, binding);
      DeclarationStore.Scope requested = DeclarationStore.scope(connection, binding, scopeId);
      if (requested.state().summary() == null) throw corrupt("required scope summary is absent");
      long scopes = 0;
      long members = 0;
      boolean found = false;
      try (var query =
          connection.prepareStatement(
              "SELECT id FROM ps_v2_scopes WHERE generation=? ORDER BY id DESC")) {
        query.setLong(1, binding.generation());
        try (var rows = query.executeQuery()) {
          while (rows.next()) {
            if (scopes >= binding.limits().scopes())
              throw corrupt("scope coverage exceeds retained session limit");
            scopes++;
            long id = rows.getLong(1);
            DeclarationStore.Scope scope = DeclarationStore.scope(connection, binding, id);
            Long parentAdmission = parentAdmission(connection, binding, scope);
            ScopeSummary summary = scope.state().summary();
            if (id == scopeId) found = true;
            if (summary == null) continue;
            if (summary.closedAt() > watermark
                || parentAdmission != null && summary.closedAt() < parentAdmission)
              throw corrupt("closure time contradicts creation or durable UTC");
            if (scope.declared() > binding.limits().entities() - members)
              throw corrupt("closed membership exceeds retained session limit");
            members += scope.declared();
            verifyScope(connection, binding, scope, results);
          }
        }
      }
      if (!found) throw corrupt("requested scope disappeared from closure coverage");
    } catch (ProtocolError invalid) {
      throw new SQLException("V2 closure: invalid retained evidence", invalid);
    }
  }

  private static Long parentAdmission(
      Connection connection, Messages.Binding binding, DeclarationStore.Scope scope)
      throws SQLException {
    if (scope.parent() == null) {
      if (scope.id() != 0 || scope.producer() != 0) throw corrupt("non-root scope lacks parent");
      return null;
    }
    WorkView parent = DeclarationStore.member(connection, binding, scope.parent()).view();
    if (scope.parent().scope() >= scope.id()
        || parent.input() == null
        || parent.admittedAt() == null
        || parent.child() == null
        || parent.child().scope() != scope.id()
        || parent.child().producer() != scope.producer())
      throw corrupt("child scope contradicts its parent's admitted membership");
    return parent.admittedAt();
  }

  private static void verifyScope(
      Connection connection,
      Messages.Binding binding,
      DeclarationStore.Scope scope,
      boolean results)
      throws SQLException {
    ScopeSummary summary = scope.state().summary();
    if (summary == null || scope.seal() == null)
      throw corrupt("closure lacks a complete membership seal");
    Commitments.Context context =
        new Commitments.Context(binding.authority(), binding.owner(), binding.generation());
    Commitments.Seal seal =
        new Commitments.Seal(
            context, scope.id(), scope.producer(), scope.parent(), scope.declared());
    Commitments.StatusTree status =
        new Commitments.StatusTree(scope.id(), scope.producer(), scope.declared());
    long last = 0;
    try (var query =
        connection.prepareStatement(
            "SELECT id FROM ps_v2_entities WHERE generation=? AND scope=? ORDER BY id")) {
      query.setLong(1, binding.generation());
      query.setLong(2, scope.id());
      try (var rows = query.executeQuery()) {
        while (rows.next()) {
          long id = rows.getLong(1);
          seal.add(id);
          WorkKey work = new WorkKey(scope.id(), scope.producer(), id);
          WorkView view = DeclarationStore.member(connection, binding, work).view();
          if (!view.state().terminal() || view.terminalAt() > summary.closedAt())
            throw corrupt("closure precedes a member's authoritative terminal outcome");
          view.validateProfiles(results);
          if (view.manifest() != null
              && (!view.manifest().authority().equals(binding.authority())
                  || !view.manifest().owner().equals(binding.owner())
                  || view.manifest().generation() != binding.generation()))
            throw corrupt("terminal manifest belongs to a different session identity");
          Digest childRoot = childRoot(connection, binding, view, summary.closedAt());
          status.add(view, childRoot);
          last = id;
        }
      }
    }
    Digest actualSeal = seal.finish();
    Commitments.Status actual = status.finish();
    if (last != scope.last()
        || !actualSeal.equals(scope.seal())
        || !actualSeal.equals(summary.seal())
        || !actual.counts().equals(summary.counts())
        || !actual.root().equals(summary.statusRoot()))
      throw corrupt("closure differs from complete retained membership or terminal status");
  }

  private static Digest childRoot(
      Connection connection, Messages.Binding binding, WorkView view, long closedAt)
      throws SQLException {
    if (view.child() == null) return null;
    DeclarationStore.Scope child =
        DeclarationStore.scope(connection, binding, view.child().scope());
    ScopeSummary summary = child.state().summary();
    if (child.id() <= view.work().scope()
        || !view.work().equals(child.parent())
        || child.producer() != view.child().producer()
        || summary == null) throw corrupt("terminal branch lacks its exact descendant closure");
    if (summary.closedAt() > closedAt) throw corrupt("ancestor scope closed before its descendant");
    if (view.state() == State.SUCCEEDED
        && (summary.counts().success() != summary.declared()
            || summary.closedAt() > view.terminalAt()))
      throw corrupt("successful branch lacks prior STRICT child success");
    return summary.statusRoot();
  }

  private static boolean results(Connection connection, Messages.Binding binding)
      throws SQLException {
    try (var query =
        connection.prepareStatement("SELECT profiles FROM ps_v2_sessions WHERE generation=?")) {
      query.setLong(1, binding.generation());
      try (var row = query.executeQuery()) {
        if (!row.next()) throw corrupt("session profile binding missing");
        long profiles = row.getLong(1);
        if (profiles != 1 && profiles != 3) throw corrupt("invalid retained profile combination");
        return profiles == 3;
      }
    }
  }

  private static long watermark(Connection connection, Messages.Binding binding)
      throws SQLException {
    FixedRecords.Snapshot clock =
        FixedRecords.read(
            connection,
            FixedRecords.CLOCK,
            FixedRecords.Kind.CLOCK,
            FixedRecords.clockKey(binding.authority()));
    Cbor.Reader in = new Cbor.Reader(clock.body(), FixedRecords.CLOCK_CAPACITY);
    long value = in.number();
    in.end();
    return value;
  }

  private static SQLException corrupt(String detail) {
    return new SQLException("V2 closure: " + detail);
  }
}
