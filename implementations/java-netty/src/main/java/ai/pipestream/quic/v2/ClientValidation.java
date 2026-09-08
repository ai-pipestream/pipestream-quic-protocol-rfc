package ai.pipestream.quic.v2;

import static ai.pipestream.quic.v2.ProtocolError.Code.*;

import java.util.Objects;

/**
 * Pure client-side consistency rules from Section 12: immutable identity and commitment fields
 * never change once observed, terminal outcomes never change, scope identity is immutable, and
 * parent/ child relationships must agree in whichever order they arrive. A contradiction is
 * INTEGRITY_ERROR; validated prior evidence is never replaced by the contradiction.
 */
final class ClientValidation {
  private ClientValidation() {}

  private static ProtocolError contradiction(String detail) {
    return new ProtocolError(INTEGRITY_ERROR, detail);
  }

  /**
   * Check a newer view against retained evidence for the same work.
   *
   * @param prior retained view
   * @param view newly received view
   */
  static void consistent(Records.WorkView prior, Records.WorkView view) {
    if (!prior.work().equals(view.work())) throw contradiction("view names different work");
    if (view.attempt() < prior.attempt()) throw contradiction("attempt regressed");
    if (prior.input() != null && !prior.input().equals(view.input()))
      throw contradiction("admitted input changed");
    if (prior.admittedAt() != null && !prior.admittedAt().equals(view.admittedAt()))
      throw contradiction("admission time changed");
    if (prior.deadline() != null && !prior.deadline().equals(view.deadline()))
      throw contradiction("execution deadline changed");
    if (prior.child() != null && !prior.child().equals(view.child()))
      throw contradiction("child scope changed");
    if (prior.state().terminal()) {
      if (prior.state() != view.state()
          || prior.attempt() != view.attempt()
          || !Objects.equals(prior.terminalAt(), view.terminalAt())
          || !Objects.equals(prior.receiptUntil(), view.receiptUntil())
          || !Objects.equals(prior.outputUntil(), view.outputUntil())
          || !Objects.equals(prior.manifest(), view.manifest())
          || !Objects.equals(prior.diagnostic(), view.diagnostic()))
        throw contradiction("terminal outcome changed");
    }
    if (prior.manifest() != null && !prior.manifest().equals(view.manifest()))
      throw contradiction("manifest changed");
  }

  /**
   * Check a newer page against retained scope evidence.
   *
   * @param prior retained evidence
   * @param page newly received page
   */
  static void consistent(ClientJournal.ScopeEvidence prior, Messages.PageResponse page) {
    if (prior.producer() != page.producer()) throw contradiction("scope producer changed");
    if (!Objects.equals(prior.parent(), page.parent())) throw contradiction("scope parent changed");
    if (prior.sealed()) {
      if (!page.sealed()) throw contradiction("sealed scope reported unsealed");
      if (prior.declared() != page.declared())
        throw contradiction("sealed membership count changed");
    }
    if (prior.seal() != null && page.seal() != null && !prior.seal().equals(page.seal()))
      throw contradiction("membership seal changed");
    if (page.declared() < prior.declared()) throw contradiction("declared count regressed");
  }

  /**
   * Check a parent's admitted child allocation against the child scope's observed identity.
   *
   * @param parent retained or new parent view
   * @param childScope retained or new child scope evidence
   */
  static void relationship(Records.WorkView parent, ClientJournal.ScopeEvidence childScope) {
    if (childScope.parent() == null || !childScope.parent().equals(parent.work())) return;
    if (parent.attempt() == 0 && parent.child() == null) return; // not yet admitted: pending
    if (parent.child() == null) throw contradiction("leaf work cannot own a child scope");
    if (parent.child().scope() != childScope.scope()
        || parent.child().producer() != childScope.producer())
      throw contradiction("child scope identity contradicts parent admission");
  }

  /**
   * Check a member view against its scope's observed producer and verified membership.
   *
   * @param scope retained scope evidence
   * @param view member view
   * @param member whether the entity is among the observed members
   */
  static void membership(ClientJournal.ScopeEvidence scope, Records.WorkView view, boolean member) {
    if (view.work().scope() != scope.scope()) return;
    if (view.work().producer() != scope.producer())
      throw contradiction("member producer contradicts scope producer");
    if (scope.membershipVerified() && !member)
      throw contradiction("work is not a member of its verified sealed scope");
  }

  /**
   * Validate a receipt against its journaled intent and typed request.
   *
   * @param pending journaled operation
   * @param expectedDigest journaled request digest
   * @param receipt received receipt
   */
  static void receipt(
      ClientJournal.PendingOperation pending,
      Records.Digest expectedDigest,
      Records.OperationReceipt receipt) {
    if (!receipt.operation().equals(pending.operation()))
      throw contradiction("receipt names another operation");
    if (!receipt.requestDigest().equals(expectedDigest))
      throw contradiction("receipt digest differs from journaled request");
    Records.Outcome outcome = receipt.outcome();
    if (pending.input() != null) {
      if (!(outcome instanceof Records.Admitted admitted))
        throw contradiction("admission receipt lacks an admission outcome");
      Records.AdmitParameters parameters = pending.input().parameters();
      if (!admitted.work().equals(parameters.work())) throw contradiction("admitted work differs");
      if (admitted.attempt() < 1) throw contradiction("admitted attempt not positive");
      if (parameters.mode() == 0 && admitted.child() != null)
        throw contradiction("leaf admission allocated a child scope");
      if (parameters.mode() != 0
          && (admitted.child() == null || admitted.child().producer() != parameters.mode() - 1))
        throw contradiction("branch admission child scope missing or mismatched");
      if (admitted.deadline() != admitted.admittedAt() + parameters.executionMs())
        throw contradiction("deadline differs from admission plus execution duration");
      return;
    }
    switch (pending.mutation()) {
      case Messages.Declare m -> {
        if (!(outcome instanceof Records.Declared d)
            || d.scope() != m.scope()
            || d.acceptedCount() != m.entityIds().size()
            || d.declared() < m.entityIds().size()
            || (m.seal() && d.seal() == null))
          throw contradiction("declaration outcome contradicts request");
      }
      case Messages.CancelScope m -> {
        if (!(outcome instanceof Records.ScopeCancelled c) || c.scope() != m.scope())
          throw contradiction("scope cancellation outcome contradicts request");
      }
      case Messages.Retry m -> {
        if (!(outcome instanceof Records.Retried r)
            || !r.work().equals(m.work())
            || r.expectedAttempt() != m.expectedAttempt()
            || r.replacementAttempt() != m.expectedAttempt() + 1)
          throw contradiction("retry outcome contradicts request");
      }
      case Messages.Cancel m -> {
        if (!(outcome instanceof Records.Cancelled c) || !c.work().equals(m.work()))
          throw contradiction("cancellation outcome contradicts request");
      }
      case Messages.Skip m -> {
        if (!(outcome instanceof Records.Skipped s) || !s.work().equals(m.work()))
          throw contradiction("skip outcome contradicts request");
      }
      default -> throw contradiction("unknown journaled mutation");
    }
  }

  /**
   * Validate a checkpoint summary against the retained seal and identity.
   *
   * @param scope retained scope evidence with a committed seal
   * @param summary received summary
   */
  static void summary(ClientJournal.ScopeEvidence scope, Records.ScopeSummary summary) {
    if (scope.seal() == null || !scope.seal().equals(summary.seal()))
      throw contradiction("summary seal contradicts retained seal");
    if (summary.scope() != scope.scope()
        || summary.producer() != scope.producer()
        || !Objects.equals(summary.parent(), scope.parent()))
      throw contradiction("summary identity contradicts retained scope");
    if (scope.sealed() && summary.declared() != scope.declared())
      throw contradiction("summary count contradicts sealed membership");
  }

  /**
   * Validate a manifest against the work view that publishes it and the session identity.
   *
   * @param context authenticated session
   * @param view succeeded view whose manifest should match
   * @param manifest received manifest
   */
  static void manifest(
      Commitments.Context context, Records.WorkView view, Records.Manifest manifest) {
    if (!manifest.authority().equals(context.authority())
        || !manifest.owner().equals(context.owner())
        || manifest.generation() != context.generation())
      throw contradiction("manifest names another session");
    if (view != null) {
      if (!manifest.work().equals(view.work())) throw contradiction("manifest names other work");
      if (view.input() != null && !manifest.inputSha256().equals(view.input().sha256()))
        throw contradiction("manifest input digest contradicts admitted input");
      if (view.manifest() != null && !view.manifest().equals(manifest))
        throw contradiction("manifest contradicts the published view");
    }
  }
}
