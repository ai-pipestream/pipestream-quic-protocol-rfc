package ai.pipestream.quic.v2;

import static ai.pipestream.quic.v2.Records.*;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

/** Checked durable release evidence, separate from physical deletion and quota completion. */
final class RetentionStore {
  /** Outcome of one local resource-reclamation operation. */
  enum Result {
    /** External availability or accepted work still requires retention. */
    NOT_READY,
    /** A physical reader, receiver, writer or callback credit is still live. */
    PINNED,
    /** Physical removal is synchronized and its logical resource charge was released. */
    RELEASED,
    /** A prior invocation already completed this resource release. */
    ALREADY_RELEASED
  }

  /** Durable metadata boundaries, never peer-controlled application callbacks. */
  enum Phase {
    /** Input eligibility and its trusted-clock sample are committed before deletion. */
    INPUT_INTENT_COMMITTED,
    /** Input quota completion is committed after synchronized deletion. */
    INPUT_RELEASE_COMMITTED,
    /** Output eligibility is committed before any physical removal. */
    OUTPUT_INTENT_COMMITTED,
    /** Output funding and logical quota have both been released. */
    OUTPUT_RELEASE_COMMITTED
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
   * Check external availability and accepted parent dependencies in addition to child closure.
   *
   * @param connection retained snapshot
   * @param binding exact session
   * @param view exact terminal work
   * @param at proposed eligibility time
   * @return whether all logical output dependencies ended by that time
   * @throws SQLException contradictory scope or parent identity
   */
  static boolean outputEligible(
      Connection connection, Messages.Binding binding, WorkView view, long at) throws SQLException {
    if (!inputEligible(connection, binding, view, at)
        || view.outputUntil() != null && at < view.outputUntil()) return false;
    DeclarationStore.Scope scope = DeclarationStore.scope(connection, binding, view.work().scope());
    if (scope.parent() == null) return true;
    WorkView parent = DeclarationStore.member(connection, binding, scope.parent()).view();
    if (parent.child() == null
        || parent.child().scope() != scope.id()
        || parent.child().producer() != scope.producer())
      throw corrupt("output dependency contradicts parent admission");
    return parent.state().terminal() && parent.terminalAt() != null && parent.terminalAt() <= at;
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
      if (at > watermark || !outputEligible(connection, binding, view, at))
        throw corrupt("output release precedes external retention or dependent parent settlement");
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

  /**
   * Preserve release evidence while changing only the output retention phase.
   *
   * @param job original settled job
   * @param at first eligibility time
   * @param live whether output funding remains logically charged
   * @return replacement job image
   */
  static JobRecord output(JobRecord job, long at, boolean live) {
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
        job.inputLive(),
        live,
        job.executorLive(),
        job.expansionComplete(),
        job.inputReleaseAt(),
        job.outputReleaseAt() == null ? at : job.outputReleaseAt());
  }

  private static SQLException corrupt(String detail) {
    return new SQLException("V2 retention: " + detail);
  }
}
