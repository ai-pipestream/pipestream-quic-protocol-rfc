package ai.pipestream.quic.v2;

import static ai.pipestream.quic.v2.Messages.*;
import static ai.pipestream.quic.v2.Records.*;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Objects;
import java.util.UUID;

/** Atomic exclusion fences; descendant settlement never substitutes for accepting the fence. */
final class FenceStore {
  private FenceStore() {}

  /** Current policy for the exact operation, including explicit permission for skip. */
  @FunctionalInterface
  interface Authorization {
    /**
     * Authorize without application effects.
     *
     * @param binding retained owner/session
     * @param request exact cancellation, skip or scope-cancellation request
     */
    void check(Binding binding, Message request);
  }

  /**
   * Immutable first own fence. Inherited cancellation does not invent an owner operation.
   *
   * @param outcome promised terminal outcome
   * @param operation first accepting caller operation
   */
  record Fence(State outcome, OperationId operation) {
    /** Validate a concrete accepted terminal fence. */
    Fence {
      Objects.requireNonNull(operation);
      ProtocolError.require(
          outcome == State.CANCELLED || outcome == State.SKIPPED, "invalid fence outcome");
    }
  }

  /** Installation-bound volatile scheduler state; restart reconstructs it from durable fences. */
  static final class Cursor {
    /** Installation owning this cursor. */
    UUID installation;

    /** Last committed descending work position. */
    ExecutionStore.Position beforeWork;

    /** Last completed descending scope position. */
    ClosureStore.Position beforeScope;

    /** Incomplete membership digest, never advertised as a seal. */
    FenceReconciliation.Scan scan;

    /** Construct an empty reconciliation sweep. */
    Cursor() {}
  }

  /**
   * Directly visited work and seal members; ancestor validation has additional depth cost.
   *
   * @param inspectedWork directly selected work records, at most the requested limit
   * @param settledWork newly terminal work
   * @param inspectedMembers directly folded membership records, at most the requested limit
   * @param sealedScopes newly materialized full membership seals, zero or one
   */
  record Progress(int inspectedWork, int settledWork, int inspectedMembers, int sealedScopes) {}

  /**
   * Proposed immutable receipt and any newly issued terminal retention promise.
   *
   * @param receipt cancellation acceptance or terminal observation
   * @param terminal newly terminal work, null otherwise
   */
  record Accepted(OperationReceipt receipt, WorkView terminal) {}

  /**
   * Decode the checked fixed fence image without recursively reading its operation journal.
   *
   * @param bytes image body
   * @return typed own fence, null for a prepaid unused image
   * @throws SQLException corrupt typed image
   */
  static Fence decode(byte[] bytes) throws SQLException {
    try {
      Cbor.Reader in = new Cbor.Reader(bytes, FixedRecords.FENCE_CAPACITY);
      if (in.nullable()) {
        in.end();
        return null;
      }
      in.exact(2);
      Fence fence = new Fence(State.from(in.number()), new OperationId(in.bytes(16)));
      in.end();
      return fence;
    } catch (ProtocolError invalid) {
      throw new SQLException("V2 fence: invalid image", invalid);
    }
  }

  /**
   * Normalize only correlation, which is excluded from the immutable intent commitment.
   *
   * @param request caller fence mutation
   * @return canonical journal request
   */
  static Message normalize(Message request) {
    return switch (request) {
      case Cancel m -> new Cancel(1, m.operation(), m.work());
      case Skip m -> new Skip(1, m.operation(), m.work());
      case CancelScope m -> new CancelScope(1, m.operation(), m.scope());
      default -> throw new IllegalArgumentException("not a fence request");
    };
  }

  /**
   * Select the private journal kind, not a new wire code.
   *
   * @param request typed fence request
   * @return checked private kind
   */
  static int kind(Message request) {
    return switch (request) {
      case Cancel ignored -> 3;
      case Skip ignored -> 4;
      case CancelScope ignored -> 5;
      default -> throw new IllegalArgumentException("not a fence request");
    };
  }

  /**
   * Extract the immutable operation identity.
   *
   * @param request fence request
   * @return caller operation identity
   */
  static OperationId operation(Message request) {
    return switch (request) {
      case Cancel m -> m.operation();
      case Skip m -> m.operation();
      case CancelScope m -> m.operation();
      default -> throw new IllegalArgumentException("not a fence request");
    };
  }

  /**
   * Validate the bounded typed journal request.
   *
   * @param kind private journal kind
   * @param id retained operation key
   * @param request decoded request
   * @throws SQLException request, key or kind mismatch
   */
  static void validateRequest(int kind, OperationId id, Message request) throws SQLException {
    if (!(request instanceof Cancel || request instanceof Skip || request instanceof CancelScope)
        || kind(request) != kind
        || !operation(request).equals(id)
        || !normalize(request).equals(request)) throw corrupt("invalid retained fence request");
  }

  /**
   * Accept a fresh fence or preserve a previously terminal outcome. The enclosing transaction
   * rechecks authorization and final safe UTC before committing the receipt and fixed images.
   *
   * @param connection writer transaction
   * @param config immutable storage configuration
   * @param binding retained owner
   * @param request fresh caller intent
   * @param digest exact owner-qualified commitment
   * @param now initial safe UTC sample
   * @return proposed receipt and newly terminal promise
   * @throws SQLException inconsistent records or failed atomic writes
   */
  static Accepted accept(
      Connection connection,
      SessionStore.Configuration config,
      Binding binding,
      Message request,
      Digest digest,
      long now)
      throws SQLException {
    OperationId id = operation(request);
    Outcome outcome;
    WorkView terminal = null;
    if (request instanceof CancelScope scopeRequest) {
      DeclarationStore.Scope scope =
          DeclarationStore.scope(connection, binding, scopeRequest.scope());
      freeze(connection, config, binding, scope, false);
      outcome = new ScopeCancelled(scope.id(), now);
    } else {
      WorkKey key = request instanceof Cancel cancel ? cancel.work() : ((Skip) request).work();
      State desired = request instanceof Skip ? State.SKIPPED : State.CANCELLED;
      DeclarationStore.Entity entity = DeclarationStore.member(connection, binding, key);
      WorkView view = entity.view();
      int disposition = view.state().terminal() ? 1 : 0;
      if (disposition == 0) {
        if (entity.fence() != null) {
          auditWork(connection, binding, entity);
          if (entity.fence().outcome() != desired)
            throw new ProtocolError(ProtocolError.Code.CANCELLED, "first own fence fixed outcome");
        } else {
          // A previously accepted ancestor fence cannot be converted to a later own skip.
          AdmissionStore.ancestors(connection, binding, key.scope());
          Cbor.Writer body = new Cbor.Writer(FixedRecords.FENCE_CAPACITY);
          body.array(2);
          body.number(desired.value());
          body.bytes(id.bytes());
          FixedRecords.replace(
              connection,
              config.files(),
              entity.fenceSlot(),
              FixedRecords.Kind.FENCE,
              FixedRecords.key(
                  binding,
                  FixedRecords.Kind.FENCE,
                  key.scope(),
                  key.producer(),
                  key.entity(),
                  entity.declaration().bytes()),
              entity.fenceGeometry().revision(),
              body.finish(),
              true);
          view =
              transition(
                  connection,
                  config,
                  binding,
                  entity,
                  childrenClosed(connection, binding, view) ? desired : State.CANCELLING,
                  now);
          if (view.state().terminal()) terminal = view;
        }
      }
      outcome =
          request instanceof Skip
              ? new Skipped(key, now, disposition, view.state())
              : new Cancelled(key, now, disposition, view.state());
    }
    OperationReceipt receipt = new OperationReceipt(id, digest, outcome);
    DeclarationStore.retainFence(connection, binding, request, receipt);
    return new Accepted(receipt, terminal);
  }

  /**
   * Freeze one scope without calculating or fabricating its membership seal.
   *
   * @param connection writer snapshot
   * @param config storage policy
   * @param binding retained session
   * @param scope checked current scope
   * @param revoke whether the root is being locally revoked
   * @throws SQLException failed funded image update
   */
  static void freeze(
      Connection connection,
      SessionStore.Configuration config,
      Binding binding,
      DeclarationStore.Scope scope,
      boolean revoke)
      throws SQLException {
    ScopeState state = scope.state();
    if (state.cancelled() && (!revoke || state.revoked())) return;
    ScopeState frozen =
        new ScopeState(
            state.id(),
            state.producer(),
            state.parent(),
            state.declared(),
            state.last(),
            state.seal(),
            true,
            state.revoked() || revoke,
            state.summary());
    FixedRecords.replace(
        connection,
        config.files(),
        scope.slot(),
        FixedRecords.Kind.SCOPE,
        FixedRecords.key(binding, FixedRecords.Kind.SCOPE, scope.id(), scope.producer(), 0, null),
        scope.revision(),
        frozen.encode(),
        true);
  }

  /**
   * Determine whether any retained ancestor already excludes new work.
   *
   * @param connection metadata snapshot
   * @param binding retained session
   * @param scope containing scope
   * @return true only for an accepted cancellation or revocation
   * @throws SQLException corrupt ancestry
   */
  static boolean inherited(Connection connection, Binding binding, long scope) throws SQLException {
    try {
      AdmissionStore.ancestors(connection, binding, scope);
      return false;
    } catch (ProtocolError refusal) {
      if (refusal.code() == ProtocolError.Code.CANCELLED
          || refusal.code() == ProtocolError.Code.UNAUTHORIZED) return true;
      throw refusal;
    }
  }

  /**
   * Require exact child binding and actual closure before a terminal cancellation.
   *
   * @param connection metadata snapshot
   * @param binding retained session
   * @param view target work
   * @return true for a leaf or an already closed child scope
   * @throws SQLException contradictory parent/child metadata
   */
  static boolean childrenClosed(Connection connection, Binding binding, WorkView view)
      throws SQLException {
    if (view.child() == null) return true;
    DeclarationStore.Scope child =
        DeclarationStore.scope(connection, binding, view.child().scope());
    if (!view.work().equals(child.parent()) || child.producer() != view.child().producer())
      throw corrupt("cancellation child contradicts parent");
    return child.state().summary() != null;
  }

  /**
   * Spend only prepaid metadata credits, retaining all payload/funding charges for later cleanup.
   *
   * @param connection writer snapshot
   * @param config storage policy
   * @param binding retained session
   * @param entity original work
   * @param state pending or terminal fence outcome
   * @param now safe settlement timestamp
   * @return replacement view
   * @throws SQLException failed funded update or contradictory job
   */
  static WorkView transition(
      Connection connection,
      SessionStore.Configuration config,
      Binding binding,
      DeclarationStore.Entity entity,
      State state,
      long now)
      throws SQLException {
    if (state != State.CANCELLING && state != State.CANCELLED && state != State.SKIPPED)
      throw new IllegalArgumentException("not a cancellation transition");
    WorkView view = entity.view();
    boolean terminal = state.terminal();
    Long until = null;
    if (terminal) {
      if (binding.policy().receiptRetention() > Long.MAX_VALUE - now)
        throw ProtocolError.limit("cancellation receipt timestamp exhausted");
      until = now + binding.policy().receiptRetention();
    }
    WorkView replacement =
        new WorkView(
            view.work(),
            state,
            view.attempt(),
            view.input(),
            view.admittedAt(),
            view.deadline(),
            terminal ? now : null,
            until,
            null,
            view.child(),
            null,
            null);
    AdmissionStore.StoredJob stored = AdmissionStore.job(connection, binding, view.work());
    if ((stored == null) != (view.input() == null))
      throw corrupt("cancellation job coverage differs");
    if (stored != null) {
      JobRecord job = stored.record();
      if (job.attempt() != view.attempt() || job.stage() == JobRecord.Stage.SETTLED)
        throw corrupt("cancellation job contradicts current work");
      JobRecord replacementJob =
          new JobRecord(
              job.input(),
              job.safety(),
              job.attempt(),
              job.lease(),
              null,
              terminal ? JobRecord.Stage.SETTLED : JobRecord.Stage.CANCELLING,
              job.inputReference(),
              job.outputReference(),
              job.objectLimit(),
              job.inputLive(),
              job.outputsLive(),
              !terminal,
              job.expansionComplete(),
              0);
      ExecutionStore.replaceJob(connection, config, binding, stored, replacementJob, true);
    }
    ExecutionStore.replaceWork(connection, config, binding, entity, replacement, true);
    return replacement;
  }

  /**
   * Validate journal outcome against the actual retained scope/work and trusted clock watermark.
   *
   * @param connection consistent snapshot
   * @param binding retained owner
   * @param request typed immutable intent
   * @param receipt decoded receipt
   * @throws SQLException contradictory committed evidence
   */
  static void validateReceipt(
      Connection connection, Binding binding, Message request, OperationReceipt receipt)
      throws SQLException {
    long accepted;
    if (request instanceof CancelScope scope) {
      if (!(receipt.outcome() instanceof ScopeCancelled outcome)
          || outcome.scope() != scope.scope()
          || !DeclarationStore.scope(connection, binding, scope.scope()).state().cancelled())
        throw corrupt("scope cancellation receipt contradicts frozen scope");
      accepted = outcome.acceptedAt();
    } else {
      WorkKey work;
      State observed;
      State desired;
      int disposition;
      if (request instanceof Cancel cancel && receipt.outcome() instanceof Cancelled outcome) {
        work = cancel.work();
        desired = State.CANCELLED;
        observed = outcome.state();
        disposition = outcome.disposition();
        accepted = outcome.acceptedAt();
        if (!work.equals(outcome.work())) throw corrupt("cancel receipt target differs");
      } else if (request instanceof Skip skip && receipt.outcome() instanceof Skipped outcome) {
        work = skip.work();
        desired = State.SKIPPED;
        observed = outcome.state();
        disposition = outcome.disposition();
        accepted = outcome.acceptedAt();
        if (!work.equals(outcome.work())) throw corrupt("skip receipt target differs");
      } else throw corrupt("fence receipt outcome kind differs");
      DeclarationStore.Entity entity = DeclarationStore.member(connection, binding, work);
      WorkView view = entity.view();
      if (disposition == 1) {
        if (!view.state().terminal() || observed != view.state() || accepted < view.terminalAt())
          throw corrupt("fence terminal observation differs");
      } else if (entity.fence() == null
          || entity.fence().outcome() != desired
          || view.state() != State.CANCELLING && view.state() != desired
          || observed != State.CANCELLING && observed != desired
          || view.terminalAt() != null && accepted > view.terminalAt())
        throw corrupt("accepted fence contradicts retained work");
      if (view.admittedAt() != null && accepted < view.admittedAt())
        throw corrupt("fence acceptance precedes admission");
    }
    if (accepted > AdmissionStore.watermark(connection, binding.authority()))
      throw corrupt("fence receipt exceeds committed clock");
  }

  /**
   * Audit fence provenance separately from image decoding, avoiding recursive journal reads.
   *
   * @param connection consistent recovery snapshot
   * @param binding retained session
   * @param entity checked work and fence image
   * @throws SQLException missing receipt, unfounded terminal state or contradictory closure
   */
  static void auditWork(Connection connection, Binding binding, DeclarationStore.Entity entity)
      throws SQLException {
    WorkView view = entity.view();
    Fence fence = entity.fence();
    if (fence != null) {
      DeclarationStore.Operation operation =
          DeclarationStore.operation(connection, binding, fence.operation());
      if (operation == null) throw corrupt("own fence lacks accepting receipt");
      Outcome outcome = operation.receipt().outcome();
      boolean valid =
          fence.outcome() == State.SKIPPED
              ? outcome instanceof Skipped skipped
                  && skipped.disposition() == 0
                  && skipped.work().equals(view.work())
              : outcome instanceof Cancelled cancelled
                  && cancelled.disposition() == 0
                  && cancelled.work().equals(view.work());
      if (!valid || view.state() != State.CANCELLING && view.state() != fence.outcome())
        throw corrupt("own fence differs from first accepting operation");
    } else if (view.state() == State.CANCELLING
        || view.state() == State.SKIPPED
        || view.state() == State.CANCELLED && !inherited(connection, binding, view.work().scope()))
      throw corrupt("work state lacks cancellation provenance");
    if (view.state() == State.CANCELLED || view.state() == State.SKIPPED) {
      long watermark = AdmissionStore.watermark(connection, binding.authority());
      if (!childrenClosed(connection, binding, view)
          || view.terminalAt() > watermark
          || binding.policy().receiptRetention() > Long.MAX_VALUE - view.terminalAt()
          || view.receiptUntil() != view.terminalAt() + binding.policy().receiptRetention())
        throw corrupt("terminal cancellation lacks closure or a valid receipt interval");
      if (view.child() != null
          && DeclarationStore.scope(connection, binding, view.child().scope())
                  .state()
                  .summary()
                  .closedAt()
              > view.terminalAt()) throw corrupt("terminal cancellation precedes child closure");
    }
  }

  /**
   * Require every materialized scope fence to have direct or inherited durable provenance. The
   * indexed scope-cancellation journal is independently checked against each typed request.
   *
   * @param connection consistent recovery snapshot
   * @param binding retained session
   * @param revoked retained session access-denial flag
   * @throws SQLException contradictory root, flags or missing accepting evidence
   */
  static void auditScopes(Connection connection, Binding binding, boolean revoked)
      throws SQLException {
    try (var query =
        connection.prepareStatement("SELECT id FROM ps_v2_scopes WHERE generation=?")) {
      query.setLong(1, binding.generation());
      try (var rows = query.executeQuery()) {
        while (rows.next()) {
          DeclarationStore.Scope scope =
              DeclarationStore.scope(connection, binding, rows.getLong(1));
          if (scope.id() == 0 && scope.state().revoked() != revoked
              || scope.state().revoked() && !scope.state().cancelled())
            throw corrupt("scope and session revocation disagree");
          if (!scope.state().cancelled() || scope.state().revoked()) continue;
          boolean proven = false;
          if (scope.parent() != null) {
            DeclarationStore.Entity parent =
                DeclarationStore.member(connection, binding, scope.parent());
            proven =
                parent.fence() != null || inherited(connection, binding, scope.parent().scope());
          }
          if (!proven) {
            try (var receipt =
                connection.prepareStatement(
                    "SELECT 1 FROM ps_v2_operations WHERE generation=? AND cancel_scope=? LIMIT"
                        + " 1")) {
              receipt.setLong(1, binding.generation());
              receipt.setLong(2, scope.id());
              try (var accepted = receipt.executeQuery()) {
                proven = accepted.next();
              }
            }
          }
          if (!proven) throw corrupt("scope cancellation lacks accepting or inherited evidence");
        }
      }
    }
  }

  private static SQLException corrupt(String detail) {
    return new SQLException("V2 fence: " + detail);
  }
}
