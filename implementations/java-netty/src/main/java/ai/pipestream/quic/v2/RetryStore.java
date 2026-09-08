package ai.pipestream.quic.v2;

import static ai.pipestream.quic.v2.Messages.*;
import static ai.pipestream.quic.v2.Records.*;

import java.sql.Connection;
import java.sql.SQLException;

/** Atomic caller retry and indexed evidence for the complete, non-reusable attempt sequence. */
final class RetryStore {
  private RetryStore() {}

  /**
   * Validate the pre-transition work without requiring the worker being replaced to remain live.
   *
   * @param connection writer snapshot
   * @param binding retained owner and policy
   * @param loaded original work and job
   * @param request immutable caller retry
   * @param now safe current UTC
   * @throws SQLException corrupt ancestry or stored state
   */
  static void eligible(
      Connection connection, Binding binding, ExecutionStore.Loaded loaded, Retry request, long now)
      throws SQLException {
    WorkView view = loaded.entity().view();
    ExecutionStore.eligible(connection, binding, view);
    if (view.attempt() != request.expectedAttempt())
      throw new ProtocolError(ProtocolError.Code.CONFLICT, "retry attempt changed");
    if (now >= view.deadline())
      throw new ProtocolError(
          ProtocolError.Code.DEADLINE_EXCEEDED, "original execution deadline reached");
  }

  /**
   * Fund replacement settlement, fence the old worker, and retain the immutable operation.
   *
   * @param connection enclosing writer transaction
   * @param config exact storage policy
   * @param binding current session
   * @param loaded original work and job
   * @param request caller intent
   * @param digest owner-qualified request commitment
   * @param now initial safe acceptance time
   * @return proposed receipt, authoritative only after the enclosing commit
   * @throws SQLException inconsistent records or failed atomic writes
   */
  static OperationReceipt replace(
      Connection connection,
      SessionStore.Configuration config,
      Binding binding,
      ExecutionStore.Loaded loaded,
      Retry request,
      Digest digest,
      long now)
      throws SQLException {
    if (request.expectedAttempt() == Long.MAX_VALUE)
      throw ProtocolError.limit("wire attempt identity exhausted");
    audit(connection, binding, loaded.entity().view());
    DeclarationStore.Entity entity = loaded.entity();
    AdmissionStore.StoredJob stored = loaded.stored();
    WorkView view = entity.view();
    JobRecord job = stored.record();
    WorkKey work = view.work();
    byte[] workKey =
        FixedRecords.key(
            binding,
            FixedRecords.Kind.WORK,
            work.scope(),
            work.producer(),
            work.entity(),
            entity.declaration().bytes());
    byte[] jobKey =
        FixedRecords.key(
            binding,
            FixedRecords.Kind.JOB,
            work.scope(),
            work.producer(),
            work.entity(),
            job.input().operation().bytes());
    // A retry is new work funding, not a free use of another admitted job's WAL headroom.
    FixedRecords.grow(
        connection,
        config.files(),
        entity.slot(),
        FixedRecords.Kind.WORK,
        workKey,
        entity.revision(),
        entity.geometry().capacity(),
        Math.max(FixedRecords.ADMITTED_WORK_CREDITS, entity.geometry().credits()));
    FixedRecords.grow(
        connection,
        config.files(),
        stored.slot(),
        FixedRecords.Kind.JOB,
        jobKey,
        stored.geometry().revision(),
        stored.geometry().capacity(),
        Math.max(FixedRecords.JOB_CREDITS, stored.geometry().credits()));
    long attempt = request.expectedAttempt() + 1;
    boolean waiting = job.input().parameters().mode() != 0 && job.expansionComplete();
    WorkView replacement =
        new WorkView(
            work,
            waiting ? State.WAITING_CHILDREN : State.ACTIVE,
            attempt,
            view.input(),
            view.admittedAt(),
            view.deadline(),
            null,
            null,
            null,
            view.child(),
            null,
            null);
    JobRecord next =
        new JobRecord(
            job.input(),
            job.safety(),
            attempt,
            job.lease(),
            null,
            waiting ? JobRecord.Stage.WAITING_CHILDREN : JobRecord.Stage.QUEUED,
            job.inputReference(),
            job.outputReference(),
            job.objectLimit(),
            job.inputLive(),
            job.outputsLive(),
            job.executorLive(),
            job.expansionComplete(),
            job.inputReleaseAt(),
            job.outputReleaseAt());
    FixedRecords.replace(
        connection,
        config.files(),
        entity.slot(),
        FixedRecords.Kind.WORK,
        workKey,
        entity.revision(),
        Wire.encodeRecord(replacement, Wire.MAX_CONTROL_LIMIT),
        false);
    FixedRecords.replace(
        connection,
        config.files(),
        stored.slot(),
        FixedRecords.Kind.JOB,
        jobKey,
        stored.geometry().revision(),
        next.encode(),
        false);
    OperationReceipt receipt =
        new OperationReceipt(
            request.operation(),
            digest,
            new Retried(work, request.expectedAttempt(), attempt, now));
    DeclarationStore.retainRetry(connection, binding, request, receipt);
    return receipt;
  }

  /**
   * Verify each increment from admission attempt one has exactly one retained caller receipt.
   * Receipt decoding separately checks every indexed key against the committed typed request.
   *
   * @param connection consistent snapshot
   * @param binding retained session
   * @param view current admitted work
   * @throws SQLException missing, duplicate or contradictory attempt evidence
   */
  static void audit(Connection connection, Binding binding, WorkView view) throws SQLException {
    try (var query =
        connection.prepareStatement(
            """
            SELECT count(*),coalesce(max(retry_attempt),0) FROM ps_v2_operations
              WHERE generation=? AND retry_scope=? AND retry_producer=? AND retry_entity=?
            """)) {
      query.setLong(1, binding.generation());
      query.setLong(2, view.work().scope());
      query.setInt(3, view.work().producer());
      query.setLong(4, view.work().entity());
      try (var rows = query.executeQuery()) {
        if (!rows.next()
            || rows.getLong(1) != view.attempt() - 1
            || rows.getLong(2) != rows.getLong(1))
          throw new SQLException("V2 retry: attempt lacks a complete receipt sequence");
      }
    }
  }
}
