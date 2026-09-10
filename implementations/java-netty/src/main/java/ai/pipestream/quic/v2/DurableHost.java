package ai.pipestream.quic.v2;

import ai.pipestream.quic.BoundedSqlite;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;
import java.time.Clock;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * One authority's composed durable services: a matched metadata/object store pair, bounded callback
 * execution, background discovery and deadline maintenance, retention/orphan cleanup, result-read
 * leases, control waits and the off-loop storage worker pool used by the listener. It owns no
 * network socket; {@link DurableServer} binds one or more listeners to a host. Initialization and
 * recovery are explicit and distinct: missing history is never an empty authority.
 *
 * <p>All blocking work (SQLite, file I/O, hashing, application callbacks) runs on host-owned worker
 * threads. Nothing here runs on a Netty event loop.
 */
public final class DurableHost implements AutoCloseable {
  /** Name of the authority metadata database inside the root directory. */
  public static final String DATABASE = "authority.sqlite";

  /** Name of the paired immutable object directory inside the root directory. */
  public static final String OBJECTS = "objects";

  /**
   * Immutable object-storage policy for the paired input/output store.
   *
   * @param bytes total retained logical bytes
   * @param files total retained file names
   * @param objectBytes maximum single input or output object
   * @param handles simultaneously open physical handles
   */
  public record ObjectLimits(long bytes, int files, long objectBytes, int handles) {
    /** Validate through the store's own policy checks. */
    public ObjectLimits {
      new InputStore.Limits(bytes, files, objectBytes, handles);
    }

    /**
     * Store-level limits.
     *
     * @return limits
     */
    InputStore.Limits toStore() {
      return new InputStore.Limits(bytes, files, objectBytes, handles);
    }
  }

  /**
   * Physical callback execution ceilings.
   *
   * @param workers simultaneously running callbacks
   * @param workersPerOwner one owner's share of workers
   * @param leaseMillis local worker lease duration; never extends an execution deadline
   * @param bufferBytes maximum incremental application I/O chunk
   */
  public record ExecutionLimits(
      int workers, int workersPerOwner, long leaseMillis, int bufferBytes) {
    /** Validate through the runtime's own policy checks. */
    public ExecutionLimits {
      new ExecutionRuntime.Limits(workers, workersPerOwner, leaseMillis, bufferBytes);
    }
  }

  /**
   * Bounded background discovery and deadline maintenance.
   *
   * @param pageSize maximum records examined per sweep
   * @param pollMillis delay between sweeps
   */
  public record SchedulerLimits(int pageSize, long pollMillis) {
    /** Validate bounded sweeps. */
    public SchedulerLimits {
      new ExecutionScheduler.Limits(1, 1, pageSize, pollMillis);
    }
  }

  /**
   * Bounded retention, orphan and retirement maintenance.
   *
   * @param pageSize maximum records examined per sweep
   * @param pollMillis delay between sweeps
   */
  public record RetentionLimits(int pageSize, long pollMillis) {
    /** Validate through the retention service's own checks. */
    public RetentionLimits {
      new RetentionService.Limits(pageSize, pollMillis);
    }
  }

  /**
   * Bounded result-read leases.
   *
   * @param reads simultaneously pinned reads
   * @param readsPerOwner one owner's share
   * @param bufferBytes maximum result chunk
   * @param pollMillis independent deadline sweep interval
   */
  public record ResultLimits(int reads, int readsPerOwner, int bufferBytes, long pollMillis) {
    /** Validate through the result service's own checks. */
    public ResultLimits {
      new ResultService.Limits(reads, readsPerOwner, bufferBytes, pollMillis);
    }
  }

  /**
   * Bounded asynchronous WORK/checkpoint observations.
   *
   * @param pending maximum waiting or running observations
   * @param perOwner one owner's share
   * @param workers simultaneous blocking observations
   * @param pollMillis scheduling sweep interval
   */
  public record WaitLimits(int pending, int perOwner, int workers, long pollMillis) {
    /** Validate through the wait service's own checks. */
    public WaitLimits {
      new ControlWaitService.Limits(pending, perOwner, workers, pollMillis);
    }
  }

  /**
   * Off-loop storage worker pool used for every connection-originated SQLite, hashing and file
   * operation.
   *
   * @param threads worker threads
   * @param queued maximum queued tasks across all connections
   * @param queuedPerOwner one owner's share of queued plus running tasks
   */
  public record WorkerLimits(int threads, int queued, int queuedPerOwner) {
    /** Validate bounded queues. */
    public WorkerLimits {
      Checks.range(threads, 1, 64);
      Checks.range(queued, 1, 65_536);
      Checks.range(queuedPerOwner, 1, queued);
    }
  }

  /**
   * Local producer-1 limits applied to authority-expanded declarations and admissions. These mirror
   * a negotiated capability response; no network stream is involved.
   *
   * @param controlLimit maximum local control body
   * @param streamLimit maximum concurrent local receivers
   * @param pendingLimit maximum pending local operations
   * @param objectLimit maximum produced child input
   * @param streamIdleMs local receiver idle ceiling
   * @param streamLifetimeMs local receiver lifetime ceiling
   */
  public record ProducerLimits(
      int controlLimit,
      int streamLimit,
      int pendingLimit,
      long objectLimit,
      long streamIdleMs,
      long streamLifetimeMs) {
    /** Validate through the capability record. */
    public ProducerLimits {
      toCapabilities(
          controlLimit, streamLimit, pendingLimit, objectLimit, streamIdleMs, streamLifetimeMs);
    }

    private static Messages.Capabilities toCapabilities(
        int controlLimit,
        int streamLimit,
        int pendingLimit,
        long objectLimit,
        long streamIdleMs,
        long streamLifetimeMs) {
      return new Messages.Capabilities(
          true,
          List.of(Messages.DURABLE_WORK, Messages.RESULT_DELIVERY),
          List.of(),
          controlLimit,
          streamLimit,
          pendingLimit,
          objectLimit,
          streamIdleMs,
          streamLifetimeMs);
    }

    /**
     * Producer ceilings as negotiated capabilities.
     *
     * @return capabilities
     */
    Messages.Capabilities capabilities() {
      return toCapabilities(
          controlLimit, streamLimit, pendingLimit, objectLimit, streamIdleMs, streamLifetimeMs);
    }
  }

  /**
   * Immutable host configuration. Fields persisted in the installation commitment (authority,
   * session limits, maximum policy, history bounds, file limits, jobs and the application registry)
   * are compared byte for byte on reopen.
   *
   * @param authority configured issuer label
   * @param resultAuthority trusted {@code host:port} written into result locators
   * @param sessionLimits admission ceilings returned by every created session
   * @param maximumPolicy largest session policy accepted exactly
   * @param maxOwners retained owner high-water records
   * @param maxSessions retained sessions, including retirement in progress
   * @param maxSessionsPerOwner one owner's retained sessions
   * @param files SQLite file-length limits for the authority database
   * @param objects paired object-store policy
   * @param maxJobs simultaneously funded jobs
   * @param maxJobsPerOwner one owner's simultaneously funded jobs
   * @param execution physical callback limits
   * @param scheduler background discovery limits
   * @param retention cleanup limits
   * @param results result-read limits
   * @param waits control observation limits
   * @param storageWorkers off-loop storage pool limits
   * @param producer local producer-1 limits for authority expansion
   */
  public record Configuration(
      String authority,
      String resultAuthority,
      Records.Limits sessionLimits,
      Records.Policy maximumPolicy,
      int maxOwners,
      int maxSessions,
      int maxSessionsPerOwner,
      BoundedSqlite.Limits files,
      ObjectLimits objects,
      int maxJobs,
      int maxJobsPerOwner,
      ExecutionLimits execution,
      SchedulerLimits scheduler,
      RetentionLimits retention,
      ResultLimits results,
      WaitLimits waits,
      WorkerLimits storageWorkers,
      ProducerLimits producer) {
    /** Validate every bound and the locator endpoint syntax. */
    public Configuration {
      Checks.identity(authority);
      new PublicationStore.Endpoint(resultAuthority);
      Objects.requireNonNull(sessionLimits);
      Objects.requireNonNull(maximumPolicy);
      Checks.range(maxOwners, 1, 65536);
      Checks.range(maxSessions, 1, 65536);
      Checks.range(maxSessionsPerOwner, 1, maxSessions);
      Objects.requireNonNull(files);
      Objects.requireNonNull(objects);
      Checks.range(maxJobs, 1, 65536);
      Checks.range(maxJobsPerOwner, 1, maxJobs);
      Objects.requireNonNull(execution);
      Objects.requireNonNull(scheduler);
      Objects.requireNonNull(retention);
      Objects.requireNonNull(results);
      Objects.requireNonNull(waits);
      Objects.requireNonNull(storageWorkers);
      Objects.requireNonNull(producer);
      if (execution.workers() > maxJobs || execution.workersPerOwner() > maxJobsPerOwner)
        throw new IllegalArgumentException("execution workers exceed funded job ceilings");
    }

    /**
     * Conservative defaults matching the Rust reference CLI where the concept is shared.
     *
     * @param authority configured issuer label
     * @param resultAuthority trusted locator endpoint
     * @return explicit default configuration
     */
    public static Configuration defaults(String authority, String resultAuthority) {
      return new Configuration(
          authority,
          resultAuthority,
          new Records.Limits(4096, 1_000_000, 1_000_000, 1L << 30, 1L << 30, 16),
          new Records.Policy(60_000, 3_600_000, 86_400_000),
          1024,
          1024,
          64,
          BoundedSqlite.Limits.defaults(),
          new ObjectLimits(8L << 30, 10_000, 16L << 20, 128),
          64,
          16,
          new ExecutionLimits(4, 2, 30_000, 65_536),
          new SchedulerLimits(64, 50),
          new RetentionLimits(64, 1000),
          new ResultLimits(128, 32, 65_536, 250),
          new WaitLimits(1024, 64, 4, 50),
          new WorkerLimits(8, 512, 64),
          new ProducerLimits(1 << 20, 64, 64, 16L << 20, 30_000, 300_000));
    }
  }

  /** Safe restart mechanisms that an explicitly configured application must implement. */
  public enum RestartSafety {
    /** Repeated invocation has application-defined idempotent effects. */
    IDEMPOTENT,
    /** The application's external effects enforce execution fences. */
    EXTERNALLY_FENCED,
    /** Effects participate in an application-defined transactional protocol. */
    TRANSACTIONAL;

    /**
     * Store representation.
     *
     * @return internal safety
     */
    AdmissionStore.RestartSafety internal() {
      return AdmissionStore.RestartSafety.values()[ordinal()];
    }
  }

  /**
   * Explicit application disposition after processing one admitted input.
   *
   * @param success publish the completed exact output set
   * @param retryable retain a nonterminal attempt failure for explicit caller retry
   * @param diagnostic bounded explanation, required for either failure disposition
   */
  public record Result(boolean success, boolean retryable, Records.Diagnostic diagnostic) {
    /** Reject contradictory dispositions. */
    public Result {
      new ExecutionRuntime.Outcome(success, retryable, diagnostic);
    }

    /**
     * Publish the exact completed output set.
     *
     * @return success request
     */
    public static Result succeeded() {
      return new Result(true, false, null);
    }

    /**
     * Terminal application failure.
     *
     * @param code application code
     * @param detail bounded explanation
     * @return terminal failure request
     */
    public static Result failed(long code, String detail) {
      return new Result(false, false, new Records.Diagnostic(code, detail));
    }

    /**
     * Retain a nonterminal attempt failure awaiting explicit caller retry.
     *
     * @param code application code
     * @param detail bounded explanation
     * @return retryable failure request
     */
    public static Result retryable(long code, String detail) {
      return new Result(false, true, new Records.Diagnostic(code, detail));
    }

    /**
     * Runtime representation.
     *
     * @return internal outcome
     */
    ExecutionRuntime.Outcome internal() {
      return new ExecutionRuntime.Outcome(success, retryable, diagnostic);
    }
  }

  /** Local producer disposition for authority expansion. */
  public enum Disposition {
    /** All declared obligations have admitted inputs or are already terminal. */
    COMPLETE,
    /** Release the worker and resume under a later lease without losing admitted children. */
    YIELD,
    /** Settle the parent as failed. */
    FAILED,
    /** Await explicit caller retry authorization. */
    RETRYABLE
  }

  /**
   * Explicit expansion result, not a membership seal.
   *
   * @param disposition requested local transition
   * @param diagnostic bounded explanation required only for failure dispositions
   */
  public record Expansion(Disposition disposition, Records.Diagnostic diagnostic) {
    /** Reject missing or contradictory failure details. */
    public Expansion {
      internal(disposition, diagnostic);
    }

    private static ExecutionRuntime.ExpansionOutcome internal(
        Disposition disposition, Records.Diagnostic diagnostic) {
      return new ExecutionRuntime.ExpansionOutcome(
          ExecutionRuntime.ExpansionDisposition.values()[
              Objects.requireNonNull(disposition).ordinal()],
          diagnostic);
    }

    /**
     * Durably finish production and await children.
     *
     * @return completion request
     */
    public static Expansion complete() {
      return new Expansion(Disposition.COMPLETE, null);
    }

    /**
     * Release the lease cooperatively.
     *
     * @return yield request
     */
    public static Expansion yielded() {
      return new Expansion(Disposition.YIELD, null);
    }

    /**
     * Terminal parent failure.
     *
     * @param code application code
     * @param detail bounded explanation
     * @return failure request
     */
    public static Expansion failed(long code, String detail) {
      return new Expansion(Disposition.FAILED, new Records.Diagnostic(code, detail));
    }

    /**
     * Nonterminal failed attempt awaiting explicit retry.
     *
     * @param code application code
     * @param detail bounded explanation
     * @return retryable request
     */
    public static Expansion retryable(long code, String detail) {
      return new Expansion(Disposition.RETRYABLE, new Records.Diagnostic(code, detail));
    }

    /**
     * Runtime representation.
     *
     * @return internal expansion outcome
     */
    ExecutionRuntime.ExpansionOutcome internal() {
      return internal(disposition, diagnostic);
    }
  }

  /**
   * One ordered page of a parent's own closed successful child scope.
   *
   * @param members child identities in increasing entity order
   * @param more whether more members exist beyond this page
   */
  public record ChildPage(List<Records.WorkKey> members, boolean more) {
    /** Freeze the page. */
    public ChildPage {
      members = List.copyOf(members);
    }
  }

  /** Thread-confined, revocable processing interface for one admitted input. */
  public interface Work {
    /**
     * Admitted input commitment.
     *
     * @return immutable input descriptor
     */
    Records.Input input();

    /**
     * Logical work identity.
     *
     * @return work key
     */
    Records.WorkKey key();

    /**
     * Current wire attempt.
     *
     * @return attempt number
     */
    long attempt();

    /**
     * Session generation.
     *
     * @return generation
     */
    long generation();

    /**
     * Retained owner.
     *
     * @return owner label
     */
    String owner();

    /**
     * Maximum incremental I/O chunk.
     *
     * @return bytes
     */
    int bufferLimit();

    /**
     * Check current owner policy, ancestor fences, attempt, lease and original deadline.
     *
     * @throws Exception when the current fence denies further work
     */
    void check() throws Exception;

    /**
     * Cooperatively renew the local lease; never a new wire attempt.
     *
     * @throws Exception when renewal is denied
     */
    void renew() throws Exception;

    /**
     * Read a bounded input chunk.
     *
     * @param bytes destination
     * @param offset destination offset
     * @param length requested bytes, at most bufferLimit
     * @return bytes read, or -1 at verified EOF
     * @throws Exception storage or fence failure
     */
    int readInput(byte[] bytes, int offset, int length) throws Exception;

    /**
     * Start one exact-length output funded by the admission budget.
     *
     * @param length exact payload length
     * @param contentType printable ASCII content type
     * @throws Exception storage or fence failure
     */
    void beginOutput(long length, String contentType) throws Exception;

    /**
     * Write a bounded output chunk.
     *
     * @param bytes chunk consumed by this call
     * @throws Exception storage, budget or fence failure
     */
    void writeOutput(ByteBuffer bytes) throws Exception;

    /**
     * Verify and install one complete output; installation is not publication.
     *
     * @return zero-based output index
     * @throws Exception storage or fence failure
     */
    int finishOutput() throws Exception;

    /**
     * Page this parent's own closed successful child scope (mode 1).
     *
     * @param afterEntity exclusive lower bound
     * @param limit maximum identities, 1..256
     * @return ordered page
     * @throws Exception unavailable or contradictory evidence
     */
    ChildPage children(long afterEntity, int limit) throws Exception;

    /**
     * Open one committed direct-child output under the current parent dependency.
     *
     * @param entity member of the child scope
     * @param index committed output index
     * @return immutable output descriptor
     * @throws Exception missing or unavailable output
     */
    Records.Output beginChildOutput(long entity, int index) throws Exception;

    /**
     * Read one bounded child-output chunk.
     *
     * @param bytes destination
     * @param offset destination offset
     * @param length requested bytes, at most bufferLimit
     * @return bytes read, or -1 at verified EOF
     * @throws Exception storage or fence failure
     */
    int readChildOutput(byte[] bytes, int offset, int length) throws Exception;

    /**
     * Finish an exactly consumed child output.
     *
     * @throws Exception physical close failure
     */
    void finishChildOutput() throws Exception;
  }

  /** Thread-confined, parent-fenced authority producer interface for mode 2. */
  public interface Production {
    /**
     * Parent input commitment.
     *
     * @return immutable input descriptor
     */
    Records.Input input();

    /**
     * Child scope allocated at parent admission.
     *
     * @return scope identity
     */
    long childScope();

    /**
     * Maximum incremental I/O chunk.
     *
     * @return bytes
     */
    int bufferLimit();

    /**
     * Check current parent ownership, deadline and authorization.
     *
     * @throws Exception when the current fence denies production
     */
    void check() throws Exception;

    /**
     * Cooperatively renew the parent lease.
     *
     * @throws Exception when renewal is denied
     */
    void renew() throws Exception;

    /**
     * Read a bounded parent-input chunk.
     *
     * @param bytes destination
     * @param offset destination offset
     * @param length requested bytes, at most bufferLimit
     * @return bytes read, or -1 at EOF
     * @throws Exception storage or fence failure
     */
    int readInput(byte[] bytes, int offset, int length) throws Exception;

    /**
     * Declare or replay exact child membership under a stable operation identity.
     *
     * @param operation original producer operation identity
     * @param members ordered child entities
     * @param seal make membership immutable
     * @return retained declaration receipt
     * @throws Exception storage or fence failure
     */
    Records.OperationReceipt declare(
        Records.OperationId operation, List<Long> members, boolean seal) throws Exception;

    /**
     * Replay or start one child admission.
     *
     * @param operation stable child-admission operation identity
     * @param parameters exact child input and application intent
     * @return receipt when already admitted; empty means write the payload and finish
     * @throws Exception storage or fence failure
     */
    Optional<Records.OperationReceipt> beginInput(
        Records.OperationId operation, Records.AdmitParameters parameters) throws Exception;

    /**
     * Stream one bounded chunk into the current child receiver.
     *
     * @param bytes consumed incremental input
     * @throws Exception staging or fence failure
     */
    void writeInput(ByteBuffer bytes) throws Exception;

    /**
     * Verify, install and admit the child input under the current parent fence.
     *
     * @return immutable admission receipt
     * @throws Exception verification, installation or fence failure
     */
    Records.OperationReceipt finishInput() throws Exception;
  }

  /** Application processing for leaf and caller-expanded work. */
  @FunctionalInterface
  public interface Processor {
    /**
     * Process one admitted input outside the authority transaction.
     *
     * @param work revocable input/output interface
     * @return explicit disposition
     * @throws Exception application failure; not automatic retry authorization
     */
    Result process(Work work) throws Exception;
  }

  /** Restart-safe production of a parent's child obligations. */
  @FunctionalInterface
  public interface Producer {
    /**
     * Produce or replay children outside the authority transaction.
     *
     * @param production parent-fenced producer interface
     * @return explicit disposition
     * @throws Exception application failure; not automatic retry authorization
     */
    Expansion produce(Production production) throws Exception;
  }

  /**
   * One explicitly configured, versioned application contract bound to real code.
   *
   * @param label wire application label
   * @param modes supported admission modes (0 leaf, 1 caller-expanded, 2 authority-expanded)
   * @param safety restart guarantee promised by the application
   * @param processor processing implementation
   * @param producer authority producer, required exactly when modes contains 2
   */
  public record Application(
      String label,
      Set<Integer> modes,
      RestartSafety safety,
      Processor processor,
      Producer producer) {
    /** Validate the contract and require real code for every declared phase. */
    public Application {
      modes = Set.copyOf(modes);
      contract(label, modes, safety);
      Objects.requireNonNull(processor);
      if (modes.contains(2) != (producer != null))
        throw new IllegalArgumentException("producer required exactly for mode 2");
    }

    private static AdmissionStore.Application contract(
        String label, Set<Integer> modes, RestartSafety safety) {
      return new AdmissionStore.Application(
          label, modes, Objects.requireNonNull(safety).internal());
    }

    /**
     * Store contract for this application.
     *
     * @return contract
     */
    AdmissionStore.Application contract() {
      return contract(label, modes, safety);
    }

    /**
     * Runtime registration for this application.
     *
     * @return registration
     */
    ExecutionRuntime.Registration registration() {
      ExecutionRuntime.Callback callback =
          context -> processor.process(new WorkAdapter(context)).internal();
      ExecutionRuntime.Expander expander =
          producer == null
              ? null
              : context -> producer.produce(new ProductionAdapter(context)).internal();
      return new ExecutionRuntime.Registration(contract(), callback, expander);
    }
  }

  /** Current owner policy consulted by workers, publication, reads and fences. */
  public interface OwnerPolicy {
    /**
     * Whether an owner's retained grants are still current.
     *
     * @param owner retained principal
     * @return true while the owner may commit, publish or read
     */
    boolean authorized(String owner);

    /**
     * Whether an owner may use one configured application.
     *
     * @param owner retained principal
     * @param application configured label
     * @return true when permitted
     */
    default boolean applicationPermitted(String owner, String application) {
      return authorized(owner);
    }

    /**
     * Whether an owner may request an explicit skip.
     *
     * @param owner retained principal
     * @return true only under explicit policy
     */
    boolean skipPermitted(String owner);

    /**
     * Policy derived from a live principal mapping: an owner stays authorized while at least one
     * configured credential maps to it.
     *
     * @param mapping current mapping supplier, normally the server's principal table
     * @param allowSkip global explicit skip permission
     * @return live policy
     */
    static OwnerPolicy fromPrincipals(
        Supplier<Map<Records.Digest, String>> mapping, boolean allowSkip) {
      Objects.requireNonNull(mapping);
      return new OwnerPolicy() {
        @Override
        public boolean authorized(String owner) {
          return mapping.get().containsValue(owner);
        }

        @Override
        public boolean skipPermitted(String owner) {
          return allowSkip && authorized(owner);
        }
      };
    }
  }

  /** Trusted deployment UTC source with explicit current trust. */
  @FunctionalInterface
  public interface UtcClock {
    /**
     * One UTC observation.
     *
     * @param utcMillis milliseconds since the Unix epoch
     * @param trusted whether the deployment currently trusts this value
     */
    record Sample(long utcMillis, boolean trusted) {}

    /**
     * Obtain a fresh nonblocking reading.
     *
     * @return current sample
     */
    Sample sample();

    /**
     * The system UTC clock with an explicit operator trust assertion. Forward jumps count as
     * elapsed time; the persisted regression guard still refuses backwards samples.
     *
     * @param trusted operator assertion that system UTC is trustworthy across restart
     * @return clock
     */
    static UtcClock system(boolean trusted) {
      Clock clock = Clock.systemUTC();
      return () -> new Sample(clock.millis(), trusted);
    }

    /**
     * Adapt to the storage clock contract.
     *
     * @return storage clock
     */
    default AdmissionStore.Clock internal() {
      return () -> {
        Sample sample = sample();
        return new AdmissionStore.Time(sample.utcMillis(), sample.trusted());
      };
    }
  }

  /**
   * Host-wide diagnostics; counts, not durable outcomes.
   *
   * @param schedulerRunning discovery is live
   * @param activeJobs callbacks in flight
   * @param completedJobs callbacks returned since start
   * @param refusedJobs callbacks refused or failed since start
   * @param retentionReleased retention releases since start
   * @param retentionRefused retention refusals since start
   * @param sessionsRetired sessions whose metadata deletion committed
   * @param pendingReads pinned result reads
   * @param pendingWaits charged control observations
   * @param queuedStorageTasks storage tasks queued or running
   */
  public record Status(
      boolean schedulerRunning,
      int activeJobs,
      long completedJobs,
      long refusedJobs,
      long retentionReleased,
      long retentionRefused,
      long sessionsRetired,
      int pendingReads,
      int pendingWaits,
      int queuedStorageTasks) {}

  /** Bounded off-loop storage worker pool with per-owner queue shares. */
  static final class Workers {
    private final ThreadPoolExecutor executor;
    private final WorkerLimits limits;
    private final Map<String, Integer> owners = new HashMap<>();
    private int queued;
    private boolean stopped;

    /**
     * Create the bounded pool.
     *
     * @param limits pool ceilings
     */
    Workers(WorkerLimits limits) {
      this.limits = limits;
      executor =
          new ThreadPoolExecutor(
              limits.threads(),
              limits.threads(),
              0,
              TimeUnit.MILLISECONDS,
              new ArrayBlockingQueue<>(limits.queued()),
              Thread.ofPlatform().daemon().name("pipestream-v2-storage-", 0).factory(),
              new ThreadPoolExecutor.AbortPolicy());
    }

    /**
     * Queue one bounded task charged to an owner. The charge is released when the task returns.
     *
     * @param owner charged owner label
     * @param task blocking work
     */
    void submit(String owner, Runnable task) {
      synchronized (this) {
        if (stopped) throw new ProtocolError(ProtocolError.Code.CANCELLED, "host stopping");
        int count = owners.getOrDefault(owner, 0);
        if (queued >= limits.queued() || count >= limits.queuedPerOwner())
          throw ProtocolError.limit("storage worker capacity exhausted");
        owners.put(owner, count + 1);
        queued++;
      }
      try {
        executor.execute(
            () -> {
              try {
                task.run();
              } finally {
                release(owner);
              }
            });
      } catch (RejectedExecutionException rejected) {
        release(owner);
        throw ProtocolError.limit("storage worker queue rejected task");
      }
    }

    private synchronized void release(String owner) {
      queued--;
      owners.compute(owner, (key, count) -> count == null || count == 1 ? null : count - 1);
    }

    /**
     * Queued task count.
     *
     * @return tasks waiting for a worker
     */
    synchronized int queued() {
      return queued;
    }

    /** Stop accepting tasks. */
    void stop() {
      synchronized (this) {
        stopped = true;
      }
      executor.shutdown();
    }

    /**
     * Await termination.
     *
     * @param millis bound in milliseconds
     * @return whether the pool terminated
     * @throws InterruptedException if interrupted while waiting
     */
    boolean awaitStopped(long millis) throws InterruptedException {
      return executor.awaitTermination(millis, TimeUnit.MILLISECONDS);
    }
  }

  private final Path root;
  private final Configuration configuration;
  private final OwnerPolicy owners;
  private final UtcClock clock;
  private final AdmissionStore.Clock storageClock;
  private final SessionStore sessions;
  private final InputStore inputs;
  private final ExecutionRuntime runtime;
  private final ExecutionScheduler scheduler;

  /**
   * Install test-only durability hooks on the execution runtime and scheduler. Shipped launchers
   * never call this; hooks observe or hold committed boundaries and cannot forge them.
   *
   * @param hooks boundary hooks
   */
  void boundaries(Boundaries hooks) {
    Objects.requireNonNull(hooks);
    runtime.boundaries(hooks);
    scheduler.boundaries(hooks);
  }

  private final RetentionService retention;
  private final ResultService results;
  private final ControlWaitService waits;
  private final Workers workers;
  private final PublicationStore.Endpoint endpoint;
  private volatile boolean stopping;
  private boolean closed;

  private DurableHost(
      Path root,
      Configuration configuration,
      List<Application> applications,
      OwnerPolicy owners,
      UtcClock clock,
      SessionStore sessions,
      InputStore inputs)
      throws IOException, SQLException {
    this.root = root;
    this.configuration = configuration;
    this.owners = owners;
    this.clock = clock;
    this.storageClock = clock.internal();
    this.sessions = sessions;
    this.inputs = inputs;
    endpoint = new PublicationStore.Endpoint(configuration.resultAuthority());
    List<ExecutionRuntime.Registration> registrations = new ArrayList<>();
    for (Application application : applications) registrations.add(application.registration());
    ExecutionRuntime createdRuntime = null;
    ExecutionScheduler createdScheduler = null;
    RetentionService createdRetention = null;
    ResultService createdResults = null;
    ControlWaitService createdWaits = null;
    Workers createdWorkers = null;
    try {
      createdRuntime =
          new ExecutionRuntime(
              sessions,
              inputs,
              registrations,
              endpoint,
              storageClock,
              applicationAuthorization(),
              new ExecutionRuntime.Limits(
                  configuration.execution().workers(),
                  configuration.execution().workersPerOwner(),
                  configuration.execution().leaseMillis(),
                  configuration.execution().bufferBytes()),
              configuration.producer().capabilities());
      createdScheduler =
          new ExecutionScheduler(
              sessions,
              createdRuntime,
              storageClock,
              this::executionGrant,
              new ExecutionScheduler.Limits(
                  configuration.execution().workers(),
                  configuration.execution().workersPerOwner(),
                  configuration.scheduler().pageSize(),
                  configuration.scheduler().pollMillis()));
      createdRetention =
          new RetentionService(
              sessions,
              inputs,
              storageClock,
              new RetentionService.Limits(
                  configuration.retention().pageSize(), configuration.retention().pollMillis()));
      createdResults =
          new ResultService(
              sessions,
              inputs,
              new ResultService.Limits(
                  configuration.results().reads(),
                  configuration.results().readsPerOwner(),
                  configuration.results().bufferBytes(),
                  configuration.results().pollMillis()));
      createdWaits =
          new ControlWaitService(
              sessions,
              new ControlWaitService.Limits(
                  configuration.waits().pending(),
                  configuration.waits().perOwner(),
                  configuration.waits().workers(),
                  configuration.waits().pollMillis()));
      createdWorkers = new Workers(configuration.storageWorkers());
      createdScheduler.start();
    } catch (IOException | SQLException | RuntimeException | Error failure) {
      if (createdWorkers != null) createdWorkers.stop();
      if (createdWaits != null) createdWaits.close();
      if (createdResults != null) closeQuietly(createdResults, failure);
      if (createdRetention != null) closeQuietly(createdRetention, failure);
      if (createdScheduler != null) createdScheduler.close();
      throw failure;
    }
    runtime = createdRuntime;
    scheduler = createdScheduler;
    retention = createdRetention;
    results = createdResults;
    waits = createdWaits;
    workers = createdWorkers;
  }

  private static void closeQuietly(AutoCloseable closeable, Throwable primary) {
    try {
      closeable.close();
    } catch (Exception suppressed) {
      primary.addSuppressed(suppressed);
    }
  }

  private static void releaseQuietly(SessionStore sessions, Throwable primary) {
    try {
      sessions.close();
    } catch (SQLException suppressed) {
      primary.addSuppressed(suppressed);
    }
  }

  /**
   * Perform explicit first installation of new authority roots. Existing files are never adopted or
   * overwritten. The host is live on return: discovery, maintenance and storage workers run.
   *
   * @param root new directory receiving the database and object store
   * @param configuration immutable deployment configuration
   * @param applications exact enabled application contracts
   * @param owners current owner policy
   * @param clock trusted deployment UTC source
   * @return live host
   * @throws IOException existing history, unsafe filesystem state or object-store failure
   * @throws SQLException failed durable initialization
   */
  public static DurableHost initialize(
      Path root,
      Configuration configuration,
      List<Application> applications,
      OwnerPolicy owners,
      UtcClock clock)
      throws IOException, SQLException {
    Objects.requireNonNull(root);
    Objects.requireNonNull(configuration);
    Objects.requireNonNull(owners);
    Objects.requireNonNull(clock);
    Files.createDirectories(root);
    SessionStore sessions =
        SessionStore.initialize(
            root.resolve(DATABASE), storeConfiguration(configuration, applications));
    InputStore inputs =
        InputStore.initializeForAuthority(
            root.resolve(OBJECTS), configuration.objects().toStore(), sessions.identity());
    try {
      sessions.bindInputs(inputs);
      sessions.anchor();
      return new DurableHost(root, configuration, applications, owners, clock, sessions, inputs);
    } catch (IOException | SQLException | RuntimeException | Error failure) {
      closeQuietly(inputs, failure);
      releaseQuietly(sessions, failure);
      throw failure;
    }
  }

  /**
   * Recover existing authority roots. Absent or empty history is an error, never a new empty
   * authority. Recovery audits, orphan/funding reconciliation and store pairing checks finish
   * before the host is returned; no capacity is admitted earlier.
   *
   * @param root existing directory containing the database and object store
   * @param configuration exact retained deployment configuration
   * @param applications exact retained application contracts
   * @param owners current owner policy
   * @param clock trusted deployment UTC source
   * @return live host
   * @throws IOException missing files, changed policy or object-store recovery failure
   * @throws SQLException unsupported format, corruption or storage failure
   */
  public static DurableHost open(
      Path root,
      Configuration configuration,
      List<Application> applications,
      OwnerPolicy owners,
      UtcClock clock)
      throws IOException, SQLException {
    Objects.requireNonNull(root);
    Objects.requireNonNull(configuration);
    Objects.requireNonNull(owners);
    Objects.requireNonNull(clock);
    SessionStore sessions =
        SessionStore.open(root.resolve(DATABASE), storeConfiguration(configuration, applications));
    InputStore inputs = InputStore.open(root.resolve(OBJECTS), configuration.objects().toStore());
    try {
      sessions.verifyInputs(inputs);
      sessions.anchor();
      return new DurableHost(root, configuration, applications, owners, clock, sessions, inputs);
    } catch (IOException | SQLException | RuntimeException | Error failure) {
      closeQuietly(inputs, failure);
      releaseQuietly(sessions, failure);
      throw failure;
    }
  }

  private static SessionStore.Configuration storeConfiguration(
      Configuration configuration, List<Application> applications) {
    List<AdmissionStore.Application> contracts = new ArrayList<>();
    for (Application application : applications) contracts.add(application.contract());
    return new SessionStore.Configuration(
        configuration.authority(),
        configuration.sessionLimits(),
        configuration.maximumPolicy(),
        configuration.maxOwners(),
        configuration.maxSessions(),
        configuration.maxSessionsPerOwner(),
        configuration.files(),
        new AdmissionStore.ExecutionPolicy(
            contracts, configuration.maxJobs(), configuration.maxJobsPerOwner()));
  }

  /**
   * Root directory of this installation.
   *
   * @return absolute root
   */
  public Path root() {
    return root;
  }

  /**
   * Immutable configuration in force.
   *
   * @return configuration
   */
  public Configuration configuration() {
    return configuration;
  }

  /**
   * Current host diagnostics.
   *
   * @return counts
   */
  public Status status() {
    ExecutionScheduler.Status execution = scheduler.status();
    RetentionService.Status cleanup = retention.status();
    return new Status(
        execution.running(),
        execution.active(),
        execution.completed(),
        execution.refused(),
        cleanup.released(),
        cleanup.refused(),
        cleanup.sessionsRetired(),
        results.usage().reads(),
        waits.usage().pending(),
        workers.queued());
  }

  /**
   * Revoke one retained session under local operator authority. Revocation is durable and
   * irreversible for that generation; existing and future connections are denied and unresolved
   * obligations settle as CANCELLED through maintenance.
   *
   * @param generation retained session generation
   * @throws SQLException failed transaction or contradictory retained root
   */
  public void revoke(long generation) throws SQLException {
    requireLive();
    sessions.revoke(
        new SessionStore.Access(configuration.authority(), this::requireLive),
        generation,
        storageClock);
  }

  private void requireLive() {
    if (stopping) throw new ProtocolError(ProtocolError.Code.CANCELLED, "host stopping");
  }

  /**
   * Authority metadata store.
   *
   * @return store
   */
  SessionStore sessions() {
    return sessions;
  }

  /**
   * Input and output payload store.
   *
   * @return store
   */
  InputStore inputs() {
    return inputs;
  }

  /**
   * Result delivery service.
   *
   * @return service
   */
  ResultService results() {
    return results;
  }

  /**
   * Control wait service.
   *
   * @return service
   */
  ControlWaitService waits() {
    return waits;
  }

  /**
   * Bounded storage worker pool.
   *
   * @return pool
   */
  Workers workers() {
    return workers;
  }

  /**
   * Trusted storage clock.
   *
   * @return clock
   */
  AdmissionStore.Clock storageClock() {
    return storageClock;
  }

  /**
   * Owner policy.
   *
   * @return policy
   */
  OwnerPolicy owners() {
    return owners;
  }

  /**
   * Result endpoint.
   *
   * @return endpoint
   */
  PublicationStore.Endpoint endpoint() {
    return endpoint;
  }

  /**
   * Configured clock.
   *
   * @return clock
   */
  UtcClock clock() {
    return clock;
  }

  /**
   * Whether close has begun.
   *
   * @return true once closing
   */
  boolean stopping() {
    return stopping;
  }

  /**
   * Build the current-authorization gate for one authenticated connection. The returned access
   * rechecks the connection's live credential, original owner mapping and current owner policy on
   * every use from any thread.
   *
   * @param owner original verified owner captured at authentication
   * @param connectionCheck nonblocking live-credential check for the connection, must return the
   *     current mapped owner or throw UNAUTHORIZED
   * @return thread-safe access gate
   */
  SessionStore.Access access(String owner, Supplier<String> connectionCheck) {
    Objects.requireNonNull(connectionCheck);
    return new SessionStore.Access(
        owner,
        () -> {
          requireLive();
          String current = connectionCheck.get();
          if (!owner.equals(current) || !owners.authorized(owner))
            throw new ProtocolError(ProtocolError.Code.UNAUTHORIZED, "owner no longer authorized");
        });
  }

  private ExecutionStore.Access executionGrant(String owner) {
    return new ExecutionStore.Access(
        owner,
        () -> {
          if (!owners.authorized(owner))
            throw new ProtocolError(
                ProtocolError.Code.UNAUTHORIZED, "retained execution grant revoked");
        });
  }

  /**
   * Live application grant check.
   *
   * @return authorization
   */
  AdmissionStore.Authorization applicationAuthorization() {
    return (binding, parameters) -> {
      if (!owners.applicationPermitted(binding.owner(), parameters.application()))
        throw new ProtocolError(ProtocolError.Code.UNAUTHORIZED, "application not permitted");
    };
  }

  /**
   * Live fence grant check.
   *
   * @return authorization
   */
  FenceStore.Authorization fenceAuthorization() {
    return (binding, request) -> {
      if (!owners.authorized(binding.owner()))
        throw new ProtocolError(ProtocolError.Code.UNAUTHORIZED, "owner no longer authorized");
      if (request instanceof Messages.Skip && !owners.skipPermitted(binding.owner()))
        throw new ProtocolError(ProtocolError.Code.UNAUTHORIZED, "skip not permitted");
    };
  }

  /**
   * Live result grant check.
   *
   * @return authorization
   */
  ResultStore.Authorization resultAuthorization() {
    return (binding, work) -> {
      if (!owners.authorized(binding.owner()))
        throw new ProtocolError(ProtocolError.Code.UNAUTHORIZED, "owner no longer authorized");
    };
  }

  /**
   * Stop the host in dependency order: no new scheduling or discovery, no new maintenance page,
   * abandon control waits, wait for busy workers, then release services and store handles.
   * Listeners must already be closed so no connection-local owner can start new work. A busy result
   * read or callback that does not return within the bounded wait leaves storage open and the close
   * fails; accepted work remains recoverable on the next open.
   *
   * @throws IOException an owner remained busy or physical cleanup is still pending
   */
  @Override
  public void close() throws IOException {
    synchronized (this) {
      if (closed) return;
      closed = true;
    }
    stopping = true;
    IOException failure = null;
    scheduler.close();
    workers.stop();
    waits.close();
    try {
      if (!scheduler.awaitStopped(60_000)) failure = new IOException("callbacks still running");
      if (!workers.awaitStopped(60_000)) failure = new IOException("storage workers still busy");
    } catch (InterruptedException interrupted) {
      Thread.currentThread().interrupt();
      failure = new IOException("interrupted while draining workers", interrupted);
    }
    try {
      results.close();
    } catch (IOException pending) {
      failure = pending;
    }
    try {
      retention.close();
      if (!retention.awaitStopped(60_000)) failure = new IOException("retention still busy");
    } catch (IOException | InterruptedException pending) {
      if (pending instanceof InterruptedException) Thread.currentThread().interrupt();
      failure = new IOException("retention shutdown incomplete", pending);
    }
    try {
      sessions.close();
    } catch (SQLException pending) {
      failure = new IOException("session store anchor did not close", pending);
    }
    if (failure != null) throw failure;
    inputs.close();
  }

  /** Thread-confined adapter from the internal runtime context to the public processing API. */
  private static final class WorkAdapter implements Work {
    private final ExecutionRuntime.Context context;

    WorkAdapter(ExecutionRuntime.Context context) {
      this.context = context;
    }

    @Override
    public Records.Input input() {
      return context.input();
    }

    @Override
    public Records.WorkKey key() {
      return context.lease().work();
    }

    @Override
    public long attempt() {
      return context.lease().attempt();
    }

    @Override
    public long generation() {
      return context.lease().generation();
    }

    @Override
    public String owner() {
      return context.lease().owner();
    }

    @Override
    public int bufferLimit() {
      return context.bufferLimit();
    }

    @Override
    public void check() throws Exception {
      context.check();
    }

    @Override
    public void renew() throws Exception {
      context.renew();
    }

    @Override
    public int readInput(byte[] bytes, int offset, int length) throws Exception {
      return context.readInput(bytes, offset, length);
    }

    @Override
    public void beginOutput(long length, String contentType) throws Exception {
      context.beginOutput(length, contentType);
    }

    @Override
    public void writeOutput(ByteBuffer bytes) throws Exception {
      context.writeOutput(bytes);
    }

    @Override
    public int finishOutput() throws Exception {
      return context.finishOutput();
    }

    @Override
    public ChildPage children(long afterEntity, int limit) throws Exception {
      BranchStore.Page page = context.children(afterEntity, limit);
      return new ChildPage(page.members(), page.more());
    }

    @Override
    public Records.Output beginChildOutput(long entity, int index) throws Exception {
      return context.beginChildOutput(entity, index);
    }

    @Override
    public int readChildOutput(byte[] bytes, int offset, int length) throws Exception {
      return context.readChildOutput(bytes, offset, length);
    }

    @Override
    public void finishChildOutput() throws Exception {
      context.finishChildOutput();
    }
  }

  /** Thread-confined adapter from the internal expansion context to the public producer API. */
  private static final class ProductionAdapter implements Production {
    private final ExecutionRuntime.ExpansionContext context;

    ProductionAdapter(ExecutionRuntime.ExpansionContext context) {
      this.context = context;
    }

    @Override
    public Records.Input input() {
      return context.input();
    }

    @Override
    public long childScope() {
      return context.childScope();
    }

    @Override
    public int bufferLimit() {
      return context.bufferLimit();
    }

    @Override
    public void check() throws Exception {
      context.check();
    }

    @Override
    public void renew() throws Exception {
      context.renew();
    }

    @Override
    public int readInput(byte[] bytes, int offset, int length) throws Exception {
      return context.readInput(bytes, offset, length);
    }

    @Override
    public Records.OperationReceipt declare(
        Records.OperationId operation, List<Long> members, boolean seal) throws Exception {
      return context.declare(operation, members, seal).receipt();
    }

    @Override
    public Optional<Records.OperationReceipt> beginInput(
        Records.OperationId operation, Records.AdmitParameters parameters) throws Exception {
      return context.beginInput(operation, parameters);
    }

    @Override
    public void writeInput(ByteBuffer bytes) throws Exception {
      context.writeInput(bytes);
    }

    @Override
    public Records.OperationReceipt finishInput() throws Exception {
      return context.finishInput();
    }
  }
}
