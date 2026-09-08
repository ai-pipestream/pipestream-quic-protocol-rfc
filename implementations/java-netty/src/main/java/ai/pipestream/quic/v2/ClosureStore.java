package ai.pipestream.quic.v2;

import static ai.pipestream.quic.v2.Records.*;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Objects;

/**
 * Verify retained closure against authoritative ordered members before treating it as execution
 * evidence. This reader never creates a closure, substitutes a partial page, or infers completion
 * from a membership seal.
 */
final class ClosureStore {
  private ClosureStore() {}

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
