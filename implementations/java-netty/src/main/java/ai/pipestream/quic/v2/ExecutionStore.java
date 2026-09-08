package ai.pipestream.quic.v2;

import static ai.pipestream.quic.v2.Messages.*;
import static ai.pipestream.quic.v2.Records.*;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

/**
 * Durable local worker ownership and failure settlement. These are authority-internal operations,
 * not peer messages or an execution scheduler. Application callbacks run only after a committed
 * claim and outside metadata transactions; result publication uses a separate funded file path.
 */
final class ExecutionStore {
  /**
   * Current execution authorization, independent of a presenting TLS certificate's lifetime.
   *
   * @param owner retained principal
   * @param checkCurrent current execution-policy gate, without application effects
   */
  record Access(String owner, Runnable checkCurrent) {
    /** Validate the local retained-grant gate. */
    Access {
      Checks.identity(owner);
      Objects.requireNonNull(checkCurrent);
    }

    /** Recheck permission; a prior successful check is not cached authorization. */
    void check() {
      checkCurrent.run();
    }
  }

  /**
   * Immutable local worker fence. This is neither wire attempt identity nor a bearer credential.
   *
   * @param installation exact authority database installation
   * @param owner retained owner
   * @param generation session generation
   * @param work logical work
   * @param attempt current wire attempt
   * @param number durable, strictly increasing local acquisition counter
   * @param until lease expiry observed when this handle was issued
   */
  record Lease(
      UUID installation,
      String owner,
      long generation,
      WorkKey work,
      long attempt,
      long number,
      long until) {
    /** Validate bounded local identity; the database still checks every field on use. */
    Lease {
      Objects.requireNonNull(installation);
      Checks.identity(owner);
      Checks.id(generation);
      Objects.requireNonNull(work);
      Checks.id(attempt);
      Checks.id(number);
      Checks.id(until);
    }
  }

  /** Internal transition selector, never decoded from a peer. */
  enum Change {
    /** Acquire a new internal execution generation. */
    CLAIM,
    /** Extend current ownership without changing its generation. */
    RENEW,
    /** Check ownership before scheduling an application effect. */
    CHECK,
    /** Commit a non-retryable application failure. */
    FAIL,
    /** Commit an attempt failure requiring explicit caller retry. */
    RETRYABLE,
    /** Publish verified immutable outputs and authoritative success together. */
    SUCCEED,
    /** Release execution ownership while retaining an incomplete expansion obligation. */
    EXPANSION_YIELD,
    /** Finish producing admitted children, independently from their eventual closure. */
    EXPANSION_COMPLETE
  }

  /**
   * One checked work/job pair, held only within its database transaction.
   *
   * @param entity current work state
   * @param stored current durable job
   */
  record Loaded(DeclarationStore.Entity entity, AdmissionStore.StoredJob stored) {}

  /**
   * Immutable application inputs from a checked local ownership observation.
   *
   * @param binding retained owner/session policy
   * @param job admitted application, payload and output ceilings
   * @param child exact admitted child scope, or null for a leaf
   * @param results whether the retained session selected result delivery
   */
  record Details(Binding binding, JobRecord job, ChildScope child, boolean results) {}

  /**
   * Indexed local discovery position; scopes have exactly one producer.
   *
   * @param generation retained session
   * @param scope allocated scope
   * @param entity declared entity
   */
  record Position(long generation, long scope, long entity) implements Comparable<Position> {
    /** Validate a real job key, not a sentinel identity. */
    Position {
      Checks.id(generation);
      Checks.number(scope);
      Checks.id(entity);
    }

    @Override
    public int compareTo(Position other) {
      int order = Long.compare(generation, other.generation);
      if (order == 0) order = Long.compare(scope, other.scope);
      return order == 0 ? Long.compare(entity, other.entity) : order;
    }
  }

  /**
   * Volatile progress through one finite discovery sweep, not a durable receipt or lease.
   *
   * @param after exclusive last examined job
   * @param through inclusive upper bound fixed by the sweep's first snapshot
   */
  record ScanCursor(Position after, Position through) {
    /** Reject reversed cursor bounds. */
    ScanCursor {
      Objects.requireNonNull(after);
      Objects.requireNonNull(through);
      if (after.compareTo(through) > 0) throw new IllegalArgumentException("reversed job cursor");
    }
  }

  /**
   * Advisory scheduling observation. Every action must recheck authoritative state and permission.
   *
   * @param position local ordered job key
   * @param owner retained principal, never a connection credential
   * @param work exact logical work
   * @param stage observed durable job state
   * @param mode admitted application mode
   * @param deadline original execution deadline
   * @param leaseUntil observed lease expiry, null without an executing worker
   * @param dependenciesReady advisory child-closure availability, not verified execution authority
   */
  record Candidate(
      Position position,
      String owner,
      WorkKey work,
      JobRecord.Stage stage,
      int mode,
      long deadline,
      Long leaseUntil,
      boolean dependenciesReady) {}

  /**
   * At most one bounded page of local discovery observations.
   *
   * @param entries checked jobs in ascending key order
   * @param next continuation for this sweep, null after its fixed endpoint
   */
  record Page(List<Candidate> entries, ScanCursor next) {
    /** Freeze the bounded observation, without conferring execution authority. */
    Page {
      entries = List.copyOf(entries);
      if (entries.size() > 64) throw ProtocolError.limit("job discovery page capacity");
    }
  }

  private ExecutionStore() {}

  /**
   * Authorize before inspecting executable state or exposing its time/lease commitments.
   *
   * @param connection checked owner transaction
   * @param config immutable application registry
   * @param binding retained session
   * @param work requested work
   * @param authorization current application permission
   * @return checked job and work
   * @throws SQLException inconsistent retained state
   */
  static Loaded load(
      Connection connection,
      SessionStore.Configuration config,
      Binding binding,
      WorkKey work,
      AdmissionStore.Authorization authorization)
      throws SQLException {
    DeclarationStore.Entity entity = DeclarationStore.member(connection, binding, work);
    AdmissionStore.StoredJob stored = AdmissionStore.job(connection, binding, work);
    if (stored == null) throw error(ProtocolError.Code.NOT_READY, "work is not admitted");
    JobRecord job = stored.record();
    authorization.check(binding, job.input().parameters());
    if (config.execution().resolve(job.input().parameters()).safety() != job.safety())
      throw error(ProtocolError.Code.APPLICATION_UNSUPPORTED, "retained restart contract differs");
    WorkView view = entity.view();
    if (view.input() == null
        || view.attempt() != job.attempt()
        || !view.input().equals(job.input().parameters().input()))
      throw corrupt("job contradicts admitted work");
    return new Loaded(entity, stored);
  }

  /**
   * Verify admitted input and output funding before a new worker can be scheduled.
   *
   * @param inputs already bound, exclusively held input store
   * @param binding retained owner
   * @param loaded checked job
   * @throws IOException missing or corrupt immutable bytes/funding
   */
  static void verifyInput(InputStore inputs, Binding binding, Loaded loaded) throws IOException {
    JobRecord job = loaded.stored().record();
    Commitments.Context context =
        new Commitments.Context(binding.authority(), binding.owner(), binding.generation());
    if (!inputs
        .find(context, job.input())
        .orElseThrow(() -> new IOException("execution input missing"))
        .reference()
        .equals(job.inputReference())) throw new IOException("execution input differs");
    if (!inputs
        .findReservation(context, job.input())
        .orElseThrow(() -> new IOException("execution funding missing"))
        .reference()
        .equals(job.outputReference())) throw new IOException("execution funding differs");
  }

  /**
   * Validate execution eligibility without changing stored state.
   *
   * @param connection metadata transaction
   * @param binding retained session
   * @param loaded checked job
   * @param lease prior ownership, or null for acquisition
   * @param change requested transition
   * @param now safe UTC sample
   * @throws SQLException corrupt child relationships
   */
  static void check(
      Connection connection, Binding binding, Loaded loaded, Lease lease, Change change, long now)
      throws SQLException {
    WorkView view = loaded.entity().view();
    JobRecord job = loaded.stored().record();
    eligible(connection, binding, view);
    if (now >= view.deadline())
      throw error(ProtocolError.Code.DEADLINE_EXCEEDED, "original execution deadline reached");
    if (change == Change.CLAIM) {
      if (job.stage() == JobRecord.Stage.AWAITING_RETRY)
        throw error(ProtocolError.Code.NOT_READY, "job awaits explicit retry");
      if (job.leaseUntil() != null && now < job.leaseUntil())
        throw error(ProtocolError.Code.NOT_READY, "current worker lease remains live");
      if (job.input().parameters().mode() != 2 || job.expansionComplete())
        requireChildren(connection, binding, view);
    } else if (lease == null
        || job.stage() != JobRecord.Stage.EXECUTING
        || job.attempt() != lease.attempt()
        || job.lease() != lease.number()
        || job.leaseUntil() == null
        || now >= job.leaseUntil()) {
      throw error(ProtocolError.Code.CONFLICT, "worker lease is stale or expired");
    }
    if (change == Change.SUCCEED) {
      if (!job.expansionComplete())
        throw error(ProtocolError.Code.NOT_READY, "authority expansion is not complete");
      requireChildren(connection, binding, view);
    }
    if (change == Change.EXPANSION_YIELD || change == Change.EXPANSION_COMPLETE) {
      if (job.input().parameters().mode() != 2 || job.expansionComplete())
        throw error(ProtocolError.Code.CONFLICT, "job has no pending authority expansion");
      DeclarationStore.Scope child = expansionScope(connection, binding, view);
      AdmissionStore.ancestors(connection, binding, child.id());
    }
  }

  /**
   * Commit expansion progress without treating a membership seal as admitted child work. Yield uses
   * ordinary write capacity; completion spends one prepaid work/job update and keeps all accepted
   * input, output and executor promises live while waiting for child closure.
   *
   * @param connection checked writer transaction
   * @param config retained storage policy
   * @param binding retained owner/session
   * @param loaded current worker before the transition
   * @param complete whether the callback finished producing its child inputs
   * @return retained parent work view, not a successful computation outcome
   * @throws SQLException corrupt membership or failed atomic image writes
   */
  static WorkView finishExpansion(
      Connection connection,
      SessionStore.Configuration config,
      Binding binding,
      Loaded loaded,
      boolean complete)
      throws SQLException {
    JobRecord job = loaded.stored().record();
    WorkView view = loaded.entity().view();
    if (complete)
      requireProducedInputs(connection, binding, expansionScope(connection, binding, view));
    WorkView result = view;
    if (complete) {
      result =
          new WorkView(
              view.work(),
              State.WAITING_CHILDREN,
              view.attempt(),
              view.input(),
              view.admittedAt(),
              view.deadline(),
              null,
              null,
              null,
              view.child(),
              null,
              null);
      replaceWork(connection, config, binding, loaded.entity(), result, true);
    }
    JobRecord replacement =
        new JobRecord(
            job.input(),
            job.safety(),
            job.attempt(),
            job.lease(),
            null,
            complete ? JobRecord.Stage.WAITING_CHILDREN : JobRecord.Stage.QUEUED,
            job.inputReference(),
            job.outputReference(),
            job.objectLimit(),
            job.inputLive(),
            job.outputsLive(),
            job.executorLive(),
            complete,
            0);
    replaceJob(connection, config, binding, loaded.stored(), replacement, complete);
    return result;
  }

  private static DeclarationStore.Scope expansionScope(
      Connection connection, Binding binding, WorkView view) throws SQLException {
    ChildScope child = view.child();
    if (child == null || child.producer() != 1)
      throw corrupt("authority expansion lacks its producer-one child scope");
    DeclarationStore.Scope scope = DeclarationStore.scope(connection, binding, child.scope());
    if (scope.producer() != 1 || !view.work().equals(scope.parent()))
      throw corrupt("authority expansion child scope contradicts its parent");
    return scope;
  }

  private static void requireProducedInputs(
      Connection connection, Binding binding, DeclarationStore.Scope scope) throws SQLException {
    if (scope.seal() == null)
      throw error(ProtocolError.Code.NOT_READY, "authority expansion membership is not sealed");
    // Jobs are admission's durable index. Inspect only members without one; an unresolved
    // declaration cannot receive producer-one input after expansion ownership is relinquished.
    // This scan uses constant application memory, not a payload/member-sized collection.
    try (var query =
        connection.prepareStatement(
            """
            SELECT e.id FROM ps_v2_entities e
            WHERE e.generation=? AND e.scope=? AND NOT EXISTS (
              SELECT 1 FROM ps_v2_jobs j
              WHERE j.generation=e.generation AND j.scope=e.scope AND j.entity=e.id)
            ORDER BY e.id
            """)) {
      query.setLong(1, binding.generation());
      query.setLong(2, scope.id());
      try (var rows = query.executeQuery()) {
        while (rows.next()) {
          WorkView member =
              DeclarationStore.member(
                      connection, binding, new WorkKey(scope.id(), 1, rows.getLong(1)))
                  .view();
          if (member.input() != null) throw corrupt("admitted expansion member has no job");
          if (!member.state().terminal())
            throw error(ProtocolError.Code.NOT_READY, "declared child input is not admitted");
        }
      }
    }
  }

  /**
   * Apply a checked lease transition using ordinary write capacity, preserving settlement credits.
   *
   * @param connection writer transaction
   * @param config retained storage policy
   * @param installation database installation identity
   * @param binding retained session
   * @param loaded checked current job
   * @param change acquisition or renewal
   * @param duration requested local lease duration
   * @param now safe commit-time sample
   * @return new observation of durable ownership
   * @throws SQLException failed atomic image write
   */
  static Lease lease(
      Connection connection,
      SessionStore.Configuration config,
      UUID installation,
      Binding binding,
      Loaded loaded,
      Change change,
      long duration,
      long now)
      throws SQLException {
    JobRecord job = loaded.stored().record();
    WorkView view = loaded.entity().view();
    long number = change == Change.CLAIM ? add(job.lease(), 1) : job.lease();
    // Check overflow before applying the deadline ceiling, never saturate an invalid addition.
    long until = Math.min(add(now, duration), view.deadline());
    if (change == Change.RENEW) until = Math.max(until, job.leaseUntil());
    JobRecord replacement =
        new JobRecord(
            job.input(),
            job.safety(),
            job.attempt(),
            number,
            until,
            JobRecord.Stage.EXECUTING,
            job.inputReference(),
            job.outputReference(),
            job.objectLimit(),
            job.inputLive(),
            job.outputsLive(),
            job.executorLive(),
            job.expansionComplete(),
            0);
    if (view.state() == State.WAITING_CHILDREN) {
      WorkView active =
          new WorkView(
              view.work(),
              State.ACTIVE,
              view.attempt(),
              view.input(),
              view.admittedAt(),
              view.deadline(),
              null,
              null,
              null,
              view.child(),
              null,
              null);
      replaceWork(connection, config, binding, loaded.entity(), active, false);
    }
    replaceJob(connection, config, binding, loaded.stored(), replacement, false);
    return new Lease(
        installation,
        binding.owner(),
        binding.generation(),
        view.work(),
        job.attempt(),
        number,
        until);
  }

  /**
   * Commit an application attempt failure or authoritative terminal failure from funded images.
   *
   * @param connection writer transaction
   * @param config exact file policy
   * @param binding retained session
   * @param loaded checked job/work
   * @param diagnostic validated bounded explanation
   * @param retryable whether explicit retry remains possible
   * @param now safe commit-time sample
   * @return replacement work view, returned only after outer commit
   * @throws SQLException atomic image write failure
   */
  static WorkView fail(
      Connection connection,
      SessionStore.Configuration config,
      Binding binding,
      Loaded loaded,
      Diagnostic diagnostic,
      boolean retryable,
      long now)
      throws SQLException {
    WorkView view = loaded.entity().view();
    JobRecord job = loaded.stored().record();
    WorkView failed =
        new WorkView(
            view.work(),
            retryable ? State.AWAITING_RETRY : State.FAILED,
            view.attempt(),
            view.input(),
            view.admittedAt(),
            view.deadline(),
            retryable ? null : now,
            retryable ? null : add(now, binding.policy().receiptRetention()),
            null,
            view.child(),
            null,
            diagnostic);
    JobRecord settled =
        new JobRecord(
            job.input(),
            job.safety(),
            job.attempt(),
            job.lease(),
            null,
            retryable ? JobRecord.Stage.AWAITING_RETRY : JobRecord.Stage.SETTLED,
            job.inputReference(),
            job.outputReference(),
            job.objectLimit(),
            job.inputLive(),
            job.outputsLive(),
            retryable,
            job.expansionComplete(),
            0);
    replaceWork(connection, config, binding, loaded.entity(), failed, true);
    replaceJob(connection, config, binding, loaded.stored(), settled, true);
    return failed;
  }

  /**
   * Publish verified descriptors and success from one prepaid work/job image pair. The enclosing
   * transaction checks current ownership again after these writes and returns only after commit.
   *
   * @param connection writer transaction
   * @param config exact file policy
   * @param binding retained session
   * @param loaded current fenced work/job pair
   * @param results retained result-delivery profile selection
   * @param outputs exact verified output descriptors
   * @param now trusted publication timestamp
   * @return proposed terminal view, not authoritative until the enclosing commit
   * @throws SQLException failed atomic image writes
   */
  static WorkView succeed(
      Connection connection,
      SessionStore.Configuration config,
      Binding binding,
      Loaded loaded,
      boolean results,
      List<Output> outputs,
      long now)
      throws SQLException {
    WorkView view = loaded.entity().view();
    JobRecord job = loaded.stored().record();
    Manifest manifest =
        results
            ? new Manifest(
                binding.authority(),
                binding.owner(),
                binding.generation(),
                view.work(),
                job.attempt(),
                view.input().sha256(),
                now,
                add(now, binding.policy().outputRetention()),
                outputs)
            : null;
    WorkView succeeded =
        new WorkView(
            view.work(),
            State.SUCCEEDED,
            view.attempt(),
            view.input(),
            view.admittedAt(),
            view.deadline(),
            now,
            add(now, binding.policy().receiptRetention()),
            manifest == null ? null : manifest.availableUntil(),
            view.child(),
            manifest,
            null);
    succeeded.validateProfiles(results);
    JobRecord settled =
        new JobRecord(
            job.input(),
            job.safety(),
            job.attempt(),
            job.lease(),
            null,
            JobRecord.Stage.SETTLED,
            job.inputReference(),
            job.outputReference(),
            job.objectLimit(),
            job.inputLive(),
            job.outputsLive(),
            false,
            job.expansionComplete(),
            0);
    replaceWork(connection, config, binding, loaded.entity(), succeeded, true);
    replaceJob(connection, config, binding, loaded.stored(), settled, true);
    return succeeded;
  }

  /**
   * Check terminal and cancellation precedence without treating parent failure as cancellation.
   *
   * @param connection metadata transaction
   * @param binding retained session
   * @param view requested work
   * @throws SQLException inconsistent ancestry
   */
  static void eligible(Connection connection, Binding binding, WorkView view) throws SQLException {
    if (view.state().terminal())
      throw error(ProtocolError.Code.ALREADY_TERMINAL, "logical work is already terminal");
    if (view.state() == State.CANCELLING)
      throw error(ProtocolError.Code.CANCELLED, "work cancellation already accepted");
    AdmissionStore.ancestors(connection, binding, view.work().scope());
  }

  private static void requireChildren(Connection connection, Binding binding, WorkView view)
      throws SQLException {
    if (view.child() == null) return;
    DeclarationStore.Scope child =
        DeclarationStore.scope(connection, binding, view.child().scope());
    if (!view.work().equals(child.parent()) || child.producer() != view.child().producer())
      throw corrupt("child scope contradicts executing parent");
    ScopeSummary summary = child.state().summary();
    if (summary == null)
      throw error(ProtocolError.Code.NOT_READY, "child scope is not closed successfully");
    ClosureStore.verify(connection, binding, child.id());
    if (view.state() == State.SUCCEEDED && summary.closedAt() > view.terminalAt())
      throw corrupt("parent success precedes its child's authoritative closure");
    if (summary.counts().success() != summary.declared())
      throw error(ProtocolError.Code.NOT_READY, "child scope is not closed successfully");
  }

  private static void replaceJob(
      Connection connection,
      SessionStore.Configuration config,
      Binding binding,
      AdmissionStore.StoredJob stored,
      JobRecord replacement,
      boolean spend)
      throws SQLException {
    WorkKey work = replacement.input().parameters().work();
    FixedRecords.replace(
        connection,
        config.files(),
        stored.slot(),
        FixedRecords.Kind.JOB,
        FixedRecords.key(
            binding,
            FixedRecords.Kind.JOB,
            work.scope(),
            work.producer(),
            work.entity(),
            replacement.input().operation().bytes()),
        stored.geometry().revision(),
        replacement.encode(),
        spend);
  }

  private static void replaceWork(
      Connection connection,
      SessionStore.Configuration config,
      Binding binding,
      DeclarationStore.Entity stored,
      WorkView replacement,
      boolean spend)
      throws SQLException {
    WorkKey work = replacement.work();
    FixedRecords.replace(
        connection,
        config.files(),
        stored.slot(),
        FixedRecords.Kind.WORK,
        FixedRecords.key(
            binding,
            FixedRecords.Kind.WORK,
            work.scope(),
            work.producer(),
            work.entity(),
            stored.declaration().bytes()),
        stored.revision(),
        Wire.encodeRecord(replacement, Wire.MAX_CONTROL_LIMIT),
        spend);
  }

  /**
   * Audit only implemented lifecycle states and the remaining promises they actually fund.
   *
   * @param connection recovery snapshot
   * @param binding retained session
   * @param entity checked work
   * @param stored checked job
   * @param watermark persisted greatest UTC sample
   * @throws SQLException unsupported state or unfunded/mismatched retained state
   */
  static void audit(
      Connection connection,
      Binding binding,
      DeclarationStore.Entity entity,
      AdmissionStore.StoredJob stored,
      long watermark)
      throws SQLException {
    WorkView view = entity.view();
    JobRecord job = stored.record();
    boolean settled = job.stage() == JobRecord.Stage.SETTLED;
    boolean failed = view.state() == State.FAILED;
    boolean retry = job.stage() == JobRecord.Stage.AWAITING_RETRY;
    // A terminal deadline failure can follow AWAITING_RETRY without a new attempt. That path
    // spends two settlement writes; terminal history does not retain the intermediate diagnostic.
    boolean expanded = job.input().parameters().mode() == 2 && job.expansionComplete();
    int spent = (failed ? 2 : settled || retry ? 1 : 0) + (expanded ? 1 : 0);
    RetryStore.audit(connection, binding, view);
    if (view.attempt() != job.attempt()
        || !job.inputLive()
        || !job.outputsLive()
        || job.executorLive() == settled
        || job.releaseIntent() != 0
        || !view.input().equals(job.input().parameters().input())
        || job.input().parameters().mode() != 2 && !job.expansionComplete()
        || entity.geometry().credits() < FixedRecords.ADMITTED_WORK_CREDITS - spent
        || stored.geometry().credits() < FixedRecords.JOB_CREDITS - spent
        || job.leaseUntil() != null
            && (job.leaseUntil() > view.deadline() || job.leaseUntil() <= view.admittedAt()))
      throw corrupt("job lifecycle identity, interval or funding differs");
    boolean valid =
        switch (job.stage()) {
          case QUEUED ->
              (job.lease() == 0
                      || job.attempt() > 1
                      || job.input().parameters().mode() == 2 && !job.expansionComplete())
                  && view.state() == State.ACTIVE
                  && job.input().parameters().mode() != 1;
          case WAITING_CHILDREN ->
              view.state() == State.WAITING_CHILDREN
                  && (job.input().parameters().mode() == 1 || job.lease() > 0 && expanded);
          case EXECUTING -> job.lease() > 0 && view.state() == State.ACTIVE;
          case AWAITING_RETRY -> job.lease() > 0 && view.state() == State.AWAITING_RETRY;
          case SETTLED ->
              view.state() == State.FAILED || view.state() == State.SUCCEEDED && job.lease() > 0;
        };
    if (!valid) throw corrupt("unsupported execution lifecycle state");
    if (expanded) {
      try {
        requireProducedInputs(connection, binding, expansionScope(connection, binding, view));
      } catch (ProtocolError invalid) {
        throw new SQLException("V2 execution: completed expansion left unresolved input", invalid);
      }
    }
    if (job.stage() == JobRecord.Stage.EXECUTING
        && (job.input().parameters().mode() != 2 || job.expansionComplete())) {
      try {
        requireChildren(connection, binding, view);
      } catch (ProtocolError invalid) {
        throw new SQLException("V2 execution: worker lacks closed successful children", invalid);
      }
    }
    if (view.state() == State.SUCCEEDED) {
      if (!job.expansionComplete()) throw corrupt("success precedes completed authority expansion");
      PublicationStore.audit(connection, binding, view, job);
      try {
        requireChildren(connection, binding, view);
      } catch (ProtocolError invalid) {
        throw new SQLException("V2 execution: success lacks closed successful children", invalid);
      }
    }
    if (settled
        && (view.terminalAt() > watermark
            || view.receiptUntil() != add(view.terminalAt(), binding.policy().receiptRetention())))
      throw corrupt("terminal receipt interval contradicts clock or policy");
  }

  private static long add(long left, long right) {
    if (left < 0 || right < 0 || right > Long.MAX_VALUE - left)
      throw ProtocolError.limit("execution counter or timestamp exhausted");
    return left + right;
  }

  private static ProtocolError error(ProtocolError.Code code, String detail) {
    return new ProtocolError(code, detail);
  }

  private static SQLException corrupt(String detail) {
    return new SQLException("V2 execution: " + detail);
  }
}
