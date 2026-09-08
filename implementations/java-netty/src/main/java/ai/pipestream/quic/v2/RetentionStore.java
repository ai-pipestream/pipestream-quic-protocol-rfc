package ai.pipestream.quic.v2;

import static ai.pipestream.quic.v2.Records.*;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

/** Checked durable release evidence, separate from physical deletion and quota completion. */
final class RetentionStore {
  /** Outcome of one bounded local input-reclamation operation. */
  enum Result {
    /** Work or its descendants still require input retention. */
    NOT_READY,
    /** Eligibility is durable but an actual reader or receiver is still live. */
    PINNED,
    /** Physical removal is synchronized and its logical input charge was released. */
    RELEASED,
    /** A prior invocation already completed this input release. */
    ALREADY_RELEASED
  }

  /** Durable metadata boundaries, never peer-controlled application callbacks. */
  enum Phase {
    /** Input eligibility and its trusted-clock sample are committed before deletion. */
    INPUT_INTENT_COMMITTED,
    /** Input quota completion is committed after synchronized deletion. */
    INPUT_RELEASE_COMMITTED
  }

  /** Trusted local failure instrumentation. */
  @FunctionalInterface
  interface Probe {
    /**
     * Observe a completed durability boundary.
     *
     * @param phase committed boundary
     * @throws IOException injected observation failure
     */
    void at(Phase phase) throws IOException;
  }

  private RetentionStore() {}

  /**
   * Check whether a terminal work's descendants no longer require its input.
   *
   * @param connection retained snapshot
   * @param binding exact session
   * @param view exact work
   * @param at proposed eligibility time
   * @return whether logical input dependencies are settled by that time
   * @throws SQLException contradictory child identity
   */
  static boolean inputEligible(
      Connection connection, Messages.Binding binding, WorkView view, long at) throws SQLException {
    if (!view.state().terminal() || view.terminalAt() == null || view.terminalAt() > at)
      return false;
    if (view.child() == null) return true;
    DeclarationStore.Scope scope =
        DeclarationStore.scope(connection, binding, view.child().scope());
    if (!view.work().equals(scope.parent()) || scope.producer() != view.child().producer())
      throw corrupt("release child scope contradicts parent admission");
    ScopeSummary summary = scope.state().summary();
    if (summary != null) ClosureStore.verify(connection, binding, scope.id());
    return summary != null && summary.closedAt() <= at;
  }

  /**
   * Verify retained release evidence even after a logical charge has been refunded.
   *
   * @param connection retained snapshot
   * @param binding exact session
   * @param view corresponding work
   * @param job checked job encoding
   * @param watermark greatest durably trusted UTC
   * @throws SQLException impossible release time, dependency or identity
   */
  static void audit(
      Connection connection, Messages.Binding binding, WorkView view, JobRecord job, long watermark)
      throws SQLException {
    if (!job.input().parameters().work().equals(view.work())
        || job.attempt() != view.attempt()
        || !job.input().parameters().input().equals(view.input()))
      throw corrupt("release job contradicts work identity");
    if (job.inputReleaseAt() != null)
      verifyTime(connection, binding, view, job.inputReleaseAt(), watermark);
    if (job.outputReleaseAt() != null) {
      long at = job.outputReleaseAt();
      verifyTime(connection, binding, view, at, watermark);
      if (view.outputUntil() != null && at < view.outputUntil())
        throw corrupt("output release precedes external retention");
      DeclarationStore.Scope scope =
          DeclarationStore.scope(connection, binding, view.work().scope());
      if (scope.parent() != null) {
        WorkView parent = DeclarationStore.member(connection, binding, scope.parent()).view();
        if (parent.child() == null
            || parent.child().scope() != scope.id()
            || parent.child().producer() != scope.producer()
            || !parent.state().terminal()
            || parent.terminalAt() == null
            || parent.terminalAt() > at)
          throw corrupt("output release precedes dependent parent settlement");
      }
    }
  }

  private static void verifyTime(
      Connection connection, Messages.Binding binding, WorkView view, long at, long watermark)
      throws SQLException {
    if (at > watermark || !inputEligible(connection, binding, view, at))
      throw corrupt("release evidence contradicts terminal dependencies or clock");
  }

  /**
   * Preserve immutable eligibility evidence while changing only the input retention phase.
   *
   * @param job original settled job
   * @param at first eligibility time, ignored when already retained
   * @param live whether the logical input charge remains live
   * @return replacement job image
   */
  static JobRecord input(JobRecord job, long at, boolean live) {
    return new JobRecord(
        job.input(),
        job.safety(),
        job.attempt(),
        job.lease(),
        job.leaseUntil(),
        job.stage(),
        job.inputReference(),
        job.outputReference(),
        job.objectLimit(),
        live,
        job.outputsLive(),
        job.executorLive(),
        job.expansionComplete(),
        job.inputReleaseAt() == null ? at : job.inputReleaseAt(),
        job.outputReleaseAt());
  }

  private static SQLException corrupt(String detail) {
    return new SQLException("V2 retention: " + detail);
  }
}
