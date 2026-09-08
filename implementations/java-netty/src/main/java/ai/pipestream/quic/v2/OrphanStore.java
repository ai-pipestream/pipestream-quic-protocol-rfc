package ai.pipestream.quic.v2;

import static ai.pipestream.quic.v2.Records.*;

import java.sql.Connection;
import java.sql.SQLException;

/** Checked absence of admission references, separate from expiry of an accepted promise. */
final class OrphanStore {
  /** Outcome for one explicitly identified physical resource. */
  enum Result {
    /** An admitted job still references this resource. */
    RETAINED,
    /** A physical reader, reception or output credit still owns this identity. */
    PINNED,
    /** Synchronized removal completed and its physical reservation was refunded. */
    RELEASED,
    /** The resource was already absent with no pending same-process refund. */
    ABSENT
  }

  /** Admission ownership after checking retained work and release evidence. */
  enum Reference {
    /** No admitted job promised this exact resource. */
    UNREFERENCED,
    /** An admitted job still reserves the resource. */
    LIVE,
    /** An admitted job completed its authorized release; physical absence must still be checked. */
    RELEASED
  }

  private OrphanStore() {}

  /**
   * Classify a known declared work's exact resource as unadmitted, live or already released.
   * Missing admitted metadata is corruption, not an orphan. This helper neither authorizes file
   * deletion by itself nor supports partially retired sessions.
   *
   * @param connection exclusive metadata writer snapshot
   * @param config retained application policy
   * @param binding exact checked session
   * @param candidate installation-derived identity
   * @return current admission ownership, including a checked completed release
   * @throws SQLException contradictory job, work, receipt or release evidence
   */
  static Reference reference(
      Connection connection,
      SessionStore.Configuration config,
      Messages.Binding binding,
      InputStore.OrphanCandidate candidate)
      throws SQLException {
    WorkKey work = candidate.header().parameters().work();
    DeclarationStore.Entity member = DeclarationStore.member(connection, binding, work);
    AdmissionStore.StoredJob stored = AdmissionStore.job(connection, binding, work);
    DeclarationStore.Operation operation =
        DeclarationStore.operation(
            connection, binding, work.producer(), candidate.header().operation());
    if (stored == null) {
      if (member.view().input() != null
          || operation != null
              && operation.input() != null
              && operation.input().equals(candidate.header()))
        throw new SQLException("V2 orphan: admitted work lost its job");
      return Reference.UNREFERENCED;
    }
    ExecutionStore.Loaded loaded =
        ExecutionStore.load(connection, config, binding, work, (owner, input) -> {});
    ExecutionStore.audit(
        connection,
        binding,
        loaded.entity(),
        loaded.stored(),
        AdmissionStore.watermark(connection, binding.authority()));
    JobRecord job = stored.record();
    String reference = candidate.funding() ? job.outputReference() : job.inputReference();
    if (!reference.equals(candidate.reference())) {
      if (job.input().equals(candidate.header()))
        throw new SQLException("V2 orphan: job resource name contradicts input identity");
      return Reference.UNREFERENCED;
    }
    if (!job.input().equals(candidate.header()))
      throw new SQLException("V2 orphan: referenced bytes contradict retained job");
    return (candidate.funding() ? job.outputsLive() : job.inputLive())
        ? Reference.LIVE
        : Reference.RELEASED;
  }
}
