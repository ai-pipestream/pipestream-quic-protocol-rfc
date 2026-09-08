package ai.pipestream.quic.v2;

import static ai.pipestream.quic.v2.Messages.*;
import static ai.pipestream.quic.v2.Records.*;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/** Bounded restartable materialization behind already authoritative ancestor fences. */
final class FenceReconciliation {
  private FenceReconciliation() {}

  /** Resolve one live retained session without depending on caller authorization. */
  @FunctionalInterface
  interface Sessions {
    /**
     * Obtain checked session state, including revoked sessions.
     *
     * @param generation selected generation
     * @return binding, or null during intentional retirement
     * @throws SQLException missing or corrupt session
     */
    Binding binding(long generation) throws SQLException;
  }

  /** One volatile streaming membership digest, bounded independently of the member count. */
  static final class Scan {
    private final Binding binding;
    private final ScopeState source;
    private final Commitments.Seal seal;
    private long after;
    private long count;

    private Scan(Binding binding, ScopeState source) {
      this.binding = binding;
      this.source = source;
      seal =
          new Commitments.Seal(
              new Commitments.Context(binding.authority(), binding.owner(), binding.generation()),
              source.id(),
              source.producer(),
              source.parent(),
              source.declared());
    }
  }

  /**
   * Proposed progress plus commit-time checks and volatile cursor positions.
   *
   * @param progress direct batch accounting
   * @param beforeWork next committed descending work position, null to start a new sweep
   * @param beforeScope next completed scope position
   * @param scan incomplete membership digest
   * @param terminal earliest newly issued terminal retention deadline, null if none
   * @param wrote whether prepaid metadata changed
   */
  record Batch(
      FenceStore.Progress progress,
      ExecutionStore.Position beforeWork,
      ClosureStore.Position beforeScope,
      Scan scan,
      Long terminal,
      boolean wrote) {}

  private record Member(ExecutionStore.Position position, int producer) {}

  /**
   * Settle at most limit work and fold at most limit membership IDs from one selected scope.
   * Ancestor checks walk their bounded depth; no whole-tree list or fake seal is constructed.
   *
   * @param connection enclosing writer transaction
   * @param config storage policy
   * @param cursor local progress, advanced only by the enclosing successful commit
   * @param limit direct per-category record budget
   * @param sessions owner-independent retained-session resolver
   * @param now safe batch timestamp
   * @return proposed progress
   * @throws SQLException corrupt state or failed funded writes
   */
  static Batch step(
      Connection connection,
      SessionStore.Configuration config,
      FenceStore.Cursor cursor,
      int limit,
      Sessions sessions,
      long now)
      throws SQLException {
    List<Member> members = new ArrayList<>(limit);
    try (var query =
        connection.prepareStatement(
            "SELECT generation,scope,id,producer FROM ps_v2_entities"
                + (cursor.beforeWork == null ? "" : " WHERE (generation,scope,id)<(?,?,?)")
                + " ORDER BY generation DESC,scope DESC,id DESC LIMIT ?")) {
      int parameter = 1;
      if (cursor.beforeWork != null) {
        query.setLong(parameter++, cursor.beforeWork.generation());
        query.setLong(parameter++, cursor.beforeWork.scope());
        query.setLong(parameter++, cursor.beforeWork.entity());
      }
      query.setInt(parameter, limit);
      try (var rows = query.executeQuery()) {
        while (rows.next())
          members.add(
              new Member(
                  new ExecutionStore.Position(rows.getLong(1), rows.getLong(2), rows.getLong(3)),
                  rows.getInt(4)));
      }
    }
    int settled = 0;
    Long terminal = null;
    ExecutionStore.Position beforeWork = null;
    for (Member member : members) {
      beforeWork = member.position();
      Binding binding = sessions.binding(beforeWork.generation());
      if (binding == null) continue;
      WorkKey work = new WorkKey(beforeWork.scope(), member.producer(), beforeWork.entity());
      DeclarationStore.Entity entity = DeclarationStore.member(connection, binding, work);
      WorkView view = entity.view();
      if (view.state().terminal()) continue;
      if (entity.fence() == null && !FenceStore.inherited(connection, binding, work.scope()))
        continue;
      if (!FenceStore.childrenClosed(connection, binding, view)) continue;
      State desired = entity.fence() == null ? State.CANCELLED : entity.fence().outcome();
      WorkView result = FenceStore.transition(connection, config, binding, entity, desired, now);
      terminal =
          terminal == null ? result.receiptUntil() : Math.min(terminal, result.receiptUntil());
      settled++;
    }

    ClosureStore.Position position = nextScope(connection, cursor);
    ClosureStore.Position beforeScope = cursor.beforeScope;
    Scan scan = cursor.scan;
    int inspected = 0, sealed = 0;
    boolean wrote = settled > 0;
    if (position == null) {
      beforeScope = null;
      scan = null;
    } else {
      Binding binding = sessions.binding(position.generation());
      if (binding == null) {
        beforeScope = position;
        scan = null;
      } else {
        DeclarationStore.Scope scope =
            DeclarationStore.scope(connection, binding, position.scope());
        if (!FenceStore.inherited(connection, binding, scope.id())) {
          beforeScope = position;
          scan = null;
        } else {
          if (!scope.state().cancelled()) {
            FenceStore.freeze(connection, config, binding, scope, false);
            scope = DeclarationStore.scope(connection, binding, scope.id());
            wrote = true;
          }
          if (scope.seal() != null) {
            beforeScope = position;
            scan = null;
          } else {
            // A revocation can legitimately change flags while a cancelled scope is folded.
            // Restart the volatile fold rather than rejecting that committed fence transition.
            if (scan != null
                && (!scan.binding.equals(binding)
                    || scan.source.cancelled() && !scope.state().cancelled()
                    || scan.source.revoked() && !scope.state().revoked()
                    || !new ScopeState(
                            scan.source.id(),
                            scan.source.producer(),
                            scan.source.parent(),
                            scan.source.declared(),
                            scan.source.last(),
                            scan.source.seal(),
                            scope.state().cancelled(),
                            scope.state().revoked(),
                            scan.source.summary())
                        .equals(scope.state())))
              throw new SQLException("V2 cancellation: partial seal membership changed");
            if (scan == null || !scan.source.equals(scope.state()))
              scan = new Scan(binding, scope.state());
            boolean more = false;
            try (var query =
                connection.prepareStatement(
                    "SELECT id FROM ps_v2_entities WHERE generation=? AND scope=? AND id>? ORDER BY"
                        + " id LIMIT ?")) {
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
                  long id = rows.getLong(1);
                  if (id <= scan.after || scan.count >= scope.declared())
                    throw new SQLException("V2 cancellation: frozen membership accounting differs");
                  scan.seal.add(id);
                  scan.after = id;
                  scan.count++;
                  inspected++;
                }
              }
            }
            if (!more) {
              if (scan.count != scope.declared() || scan.after != scope.last())
                throw new SQLException("V2 cancellation: incomplete frozen membership");
              Digest digest = scan.seal.finish();
              FixedRecords.replace(
                  connection,
                  config.files(),
                  scope.slot(),
                  FixedRecords.Kind.SCOPE,
                  FixedRecords.key(
                      binding, FixedRecords.Kind.SCOPE, scope.id(), scope.producer(), 0, null),
                  scope.revision(),
                  scope.state().members(scope.declared(), scope.last(), digest).encode(),
                  true);
              sealed = 1;
              wrote = true;
              beforeScope = position;
              scan = null;
            }
          }
        }
      }
    }
    return new Batch(
        new FenceStore.Progress(members.size(), settled, inspected, sealed),
        beforeWork,
        beforeScope,
        scan,
        terminal,
        wrote);
  }

  private static ClosureStore.Position nextScope(Connection connection, FenceStore.Cursor cursor)
      throws SQLException {
    if (cursor.scan != null)
      return new ClosureStore.Position(cursor.scan.binding.generation(), cursor.scan.source.id());
    try (var query =
        connection.prepareStatement(
            "SELECT generation,id FROM ps_v2_scopes"
                + (cursor.beforeScope == null ? "" : " WHERE (generation,id)<(?,?)")
                + " ORDER BY generation DESC,id DESC LIMIT 1")) {
      if (cursor.beforeScope != null) {
        query.setLong(1, cursor.beforeScope.generation());
        query.setLong(2, cursor.beforeScope.scope());
      }
      try (var rows = query.executeQuery()) {
        return rows.next() ? new ClosureStore.Position(rows.getLong(1), rows.getLong(2)) : null;
      }
    }
  }
}
