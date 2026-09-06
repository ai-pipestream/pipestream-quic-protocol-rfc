package ai.pipestream.quic;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;

/**
 * Bounded, restartable execution of Java sealed work, separate from connection event loops.
 * One executor owns a canonical database in this process. Closing stops new dispatch;
 * physical capacity stays owned until existing callbacks and storage calls return.
 * Callbacks must fence or deduplicate their own external effects.
 */
public final class SealedExecutor implements AutoCloseable {
  private static final Set<Path> OPEN = new HashSet<>();
  private static final int MAX_STORAGE_CALLS = 8;
  private final Path database;
  private final SealedJobs jobs;
  private final SealedPayloadStore payloads;
  private final Processor processor;
  private final Limits limits;
  private final LongSupplier clock;
  private final UUID workerId = UUID.randomUUID();
  private final Set<SealedJobs.Key> running = new HashSet<>();
  private final Map<String, Integer> sessionWorkers = new HashMap<>();
  private final AtomicBoolean closed = new AtomicBoolean();
  private final AtomicBoolean workersStopped = new AtomicBoolean(), dispatcherStopped = new AtomicBoolean();
  private final AtomicReference<Throwable> failure = new AtomicReference<>();
  private final AtomicLong rejectedFences = new AtomicLong();
  private final ScheduledThreadPoolExecutor dispatcher;
  private final ThreadPoolExecutor workers;
  private int storageCalls;
  private boolean ownershipReleased;

  /** Application operation; rehydration consumes the same retained parent input. */
  public enum Operation {
    /** Process newly admitted input. */
    PROCESS,
    /** Rehydrate after a closed, successful child scope. */
    REHYDRATE
  }

  /**
   * Physical execution policy. Session labels are not authenticated tenant identities.
   * @param workers maximum simultaneous callbacks across sessions
   * @param workersPerSession maximum simultaneous callbacks for one session
   * @param leaseDuration wall-clock lease duration, one millisecond through five minutes
   */
  public record Limits(int workers, int workersPerSession, Duration leaseDuration) {
    /** Validates a finite worker and lease policy. */
    public Limits {
      Objects.requireNonNull(leaseDuration);
      if (workers < 1 || workers > 32 || workersPerSession < 1 || workersPerSession > workers
          || leaseDuration.compareTo(Duration.ofMillis(1)) < 0 || leaseDuration.compareTo(Duration.ofMinutes(5)) > 0) {
        throw new IllegalArgumentException("invalid sealed executor limits");
      }
    }
    /** Returns four workers, at most two per session, with five-minute leases.
     * @return default execution policy
     */
    public static Limits defaults() { return new Limits(4, 2, Duration.ofMinutes(5)); }
  }

  /**
   * Immutable execution identity and fence, not an authorization credential.
   * @param identity session and scoped input identity
   * @param header retained application header
   * @param operation processing or rehydration
   * @param child closed child summary for rehydration, otherwise null
   * @param epoch increasing durable attempt number
   * @param worker executor identifier
   * @param expiresEpochMillis issuer wall-clock expiry in milliseconds
   */
  public record Context(SealedPayloadStore.Identity identity, SealedTransport.Header header,
      Operation operation, SealedScope.Digest child, long epoch, UUID worker, long expiresEpochMillis) {}

  /**
   * Application result; COMPLETE requires an actual SHA-256 output commitment.
   * @param state COMPLETE (3), FAILED (4), or DEHYDRATING (6)
   * @param outputDigest SHA-256 for COMPLETE, otherwise null
   */
  public record Decision(int state, byte[] outputDigest) {
    /** Copies the commitment and rejects invalid outcome shapes. */
    public Decision {
      outputDigest = outputDigest == null ? null : outputDigest.clone();
      if ((state != 3 && state != 4 && state != 6) || (state == 3) != (outputDigest != null)
          || (outputDigest != null && outputDigest.length != 32)) throw new IllegalArgumentException("invalid application result");
    }
    /** Returns a defensive copy of the result commitment.
     * @return SHA-256 or null
     */
    @Override public byte[] outputDigest() { return outputDigest == null ? null : outputDigest.clone(); }
  }

  /** Application callback, invoked outside persistence transactions and network event loops. */
  @FunctionalInterface public interface Processor {
    /**
     * Executes one fenced attempt using previously verified, file-backed input.
     * The executor closes the reader after the callback returns.
     * @param context immutable operation and fence
     * @param input payload reader, not an in-memory whole-entity copy
     * @return actual application outcome
     * @throws Exception for a retained execution refusal; exceptions never imply completion
     */
    Decision execute(Context context, InputStream input) throws Exception;
  }

  /**
   * Current physical activity, including callbacks continuing after close.
   * @param activeWorkers occupied worker permits
   * @param activeSessions sessions occupying permits
   * @param activeStorageCalls started admission or scope-closure calls
   * @param closing whether new dispatch has stopped
   * @param rejectedFences stale or expired publication attempts refused
   */
  public record Usage(int activeWorkers, int activeSessions, int activeStorageCalls, boolean closing, long rejectedFences) {}

  private SealedExecutor(Path database, SealedSessionStore sessions, SealedPayloadStore payloads,
      Processor processor, Limits limits, LongSupplier clock) {
    this.database = database; this.jobs = new SealedJobs(sessions); this.payloads = payloads;
    this.processor = processor; this.limits = limits; this.clock = clock;
    workers = new ThreadPoolExecutor(limits.workers(), limits.workers(), 0, TimeUnit.MILLISECONDS,
        new ArrayBlockingQueue<>(limits.workers()), Thread.ofPlatform().daemon().name("sealed-worker-", 0).factory(), new ThreadPoolExecutor.AbortPolicy()) {
      @Override protected void terminated() { workersStopped.set(true); releaseDirectory(); }
    };
    dispatcher = new ScheduledThreadPoolExecutor(1, Thread.ofPlatform().daemon().name("sealed-dispatch-", 0).factory()) {
      @Override protected void terminated() { dispatcherStopped.set(true); releaseDirectory(); }
    };
    dispatcher.setRemoveOnCancelPolicy(true);
    dispatcher.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
    dispatcher.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
  }

  /**
   * Starts bounded periodic dispatch, including retained queued or expired attempts.
   * Startup checks the persistent database/payload pairing and audits durable jobs
   * before executing any application code.
   * @param sessions Java session database with durable completion reservations
   * @param payloads open retained-payload store, owned and closed by the caller
   * @param processor application callback
   * @param limits worker and lease policy
   * @return running executor
   * @throws IOException for duplicate ownership or an inaccessible database
   * @throws SQLException for persistence failure
   * @throws ProtocolException for corrupt retained work
   */
  public static SealedExecutor start(SealedSessionStore sessions, SealedPayloadStore payloads,
      Processor processor, Limits limits) throws IOException, SQLException, ProtocolException {
    return start(sessions, payloads, processor, limits, System::currentTimeMillis);
  }

  static SealedExecutor start(SealedSessionStore sessions, SealedPayloadStore payloads,
      Processor processor, Limits limits, LongSupplier clock) throws IOException, SQLException, ProtocolException {
    Objects.requireNonNull(payloads); Objects.requireNonNull(processor); Objects.requireNonNull(limits); Objects.requireNonNull(clock);
    Path database = sessions.database().toRealPath();
    synchronized (OPEN) { if (!OPEN.add(database)) throw new IOException("Java sealed executor already owns this database"); }
    SealedExecutor executor = null;
    try {
      payloads.bind(sessions);
      new SealedJobs(sessions).audit();
      executor = new SealedExecutor(database, sessions, payloads, processor, limits, clock);
      executor.dispatcher.scheduleWithFixedDelay(executor::dispatch, 0, 10, TimeUnit.MILLISECONDS);
      return executor;
    } catch (IOException | SQLException | ProtocolException | RuntimeException error) {
      if (executor != null) executor.close();
      else synchronized (OPEN) { OPEN.remove(database); }
      throw error;
    }
  }

  /**
   * Revalidates an installed input, then atomically admits it and queues its processing job.
   * This blocking call belongs on a storage worker, not a Netty event loop.
   * @param input verified, immutable payload installed before admission
   * @throws IOException when dispatch or the payload store has closed, or input I/O fails
   * @throws SQLException for persistence failure
   * @throws ProtocolException for identity, capacity, or integrity refusal
   */
  public void admit(SealedPayloadStore.Stored input) throws IOException, SQLException, ProtocolException {
    beginStorage();
    try {
      if (!input.belongsTo(payloads)) throw Wire.entity("input belongs to a different payload-store handle");
      jobs.admit(input);
    }
    finally { endStorage(); }
  }

  /**
   * Commits child closure and required rehydration dispatch in one transaction.
   * A failed child propagates failure without calling the rehydrator.
   * @param session session identity
   * @param producer declared producer label
   * @param scope nonzero child scope
   * @return closed summary, or empty while declared work remains unresolved
   * @throws IOException when dispatch has closed
   * @throws SQLException for persistence failure
   * @throws ProtocolException for scope, lifecycle, or capacity refusal
   */
  public Optional<SealedScope.Digest> closeScope(String session, UUID producer, long scope) throws IOException, SQLException, ProtocolException {
    beginStorage();
    try { return jobs.closeScope(session, producer, scope).map(SealedJobs.Closure::digest); }
    finally { endStorage(); }
  }

  SealedJobs.Closure confirmScope(String session, UUID producer, SealedScope.Digest expected)
      throws IOException, SQLException, ProtocolException {
    beginStorage();
    try { return jobs.confirmScope(session, producer, expected); }
    finally { endStorage(); }
  }

  /** Returns physical activity without waiting for storage.
   * @return current activity snapshot
   */
  public synchronized Usage usage() { return new Usage(running.size(), sessionWorkers.size(), storageCalls, closed.get(), rejectedFences.get()); }

  /** Returns a fatal dispatch/storage failure, which stops new dispatch.
   * @return failure if encountered, not an application refusal or ordinary lease expiry
   */
  public Optional<Throwable> failure() { return Optional.ofNullable(failure.get()); }

  /** Reports when callbacks and started storage calls have actually returned after close.
   * @return true when the caller may close its payload store or start a replacement executor
   */
  public synchronized boolean isTerminated() { return ownershipReleased; }

  /**
   * Stops new dispatch without interrupting callbacks or claiming their work complete.
   * Database ownership remains reserved until all physical tasks actually return.
   * The caller must keep the payload store alive through that interval.
   */
  @Override public void close() {
    synchronized (this) { if (!closed.compareAndSet(false, true)) return; }
    dispatcher.shutdown(); workers.shutdown(); releaseDirectory();
  }

  private void dispatch() {
    if (closed.get()) return;
    try {
      for (var key : jobs.ready(clock.getAsLong(), SealedJobs.MAX_QUEUED)) {
        if (!reserve(key)) continue;
        boolean submitted = false;
        try {
          var lease = jobs.acquire(key, workerId, clock.getAsLong(), limits.leaseDuration().toMillis());
          workers.execute(() -> execute(lease)); submitted = true;
        } catch (RejectedExecutionException error) {
          if (!closed.get()) throw error;
        } catch (ProtocolException error) {
          if (error.errorCode() != Wire.ERROR_ENTITY_INVALID) throw error;
        } finally { if (!submitted) release(key); }
      }
    } catch (Throwable error) { fail(error); }
  }

  private void execute(SealedJobs.Lease lease) {
    try {
      var job = jobs.find(lease.key()).orElseThrow(() -> Wire.integrity("acquired job disappeared"));
      SealedJobs.Outcome outcome;
      try {
        var stored = payloads.find(lease.key().identity()).orElseThrow(() -> Wire.integrity("admitted payload is missing"));
        if (stored.length() != job.input().length() || !MessageDigest.isEqual(stored.digest(), job.input().digest())
            || !Arrays.equals(SealedTransport.header(stored.header()), SealedTransport.header(job.input().header()))) throw Wire.integrity("retained execution input differs");
        Operation operation = lease.key().kind() == SealedJobs.PROCESS ? Operation.PROCESS : Operation.REHYDRATE;
        Context context = new Context(stored.identity(), stored.header(), operation, job.input().child(), lease.epoch(), lease.worker(), lease.expires());
        try (InputStream input = stored.openStream()) {
          Decision decision = Objects.requireNonNull(processor.execute(context, input), "application returned no decision");
          if (operation == Operation.REHYDRATE && decision.state() == 6) throw Wire.entity("rehydration cannot dehydrate again");
          outcome = new SealedJobs.Outcome(decision.state(), decision.outputDigest(), null);
        }
      } catch (ProtocolException error) { outcome = SealedJobs.Outcome.refused(error.errorCode()); }
      catch (Exception error) { outcome = SealedJobs.Outcome.refused(Wire.ERROR_FRAME); }
      try { jobs.publish(lease, clock.getAsLong(), outcome); }
      catch (ProtocolException error) {
        if (error.errorCode() != Wire.ERROR_ENTITY_INVALID) throw error;
        rejectedFences.incrementAndGet();
      }
    } catch (Throwable error) { fail(error); }
    finally { release(lease.key()); }
  }

  private synchronized boolean reserve(SealedJobs.Key key) {
    if (closed.get() || running.size() >= limits.workers() || running.contains(key)
        || sessionWorkers.getOrDefault(key.identity().session(), 0) >= limits.workersPerSession()) return false;
    running.add(key); sessionWorkers.merge(key.identity().session(), 1, Integer::sum); return true;
  }
  private synchronized void release(SealedJobs.Key key) {
    if (!running.remove(key)) throw new IllegalStateException("worker permit released twice");
    String session = key.identity().session(); int remaining = sessionWorkers.get(session) - 1;
    if (remaining == 0) sessionWorkers.remove(session); else sessionWorkers.put(session, remaining);
  }
  private void fail(Throwable error) { failure.compareAndSet(null, error); close(); }
  private synchronized void beginStorage() throws IOException, ProtocolException {
    if (closed.get()) throw new IOException("sealed executor is closed");
    if (storageCalls >= MAX_STORAGE_CALLS) throw Wire.limit("sealed executor storage-call capacity exhausted");
    storageCalls++;
  }
  private synchronized void endStorage() { storageCalls--; releaseDirectory(); }
  private synchronized void releaseDirectory() {
    if (!ownershipReleased && closed.get() && workersStopped.get() && dispatcherStopped.get() && storageCalls == 0) {
      synchronized (OPEN) { OPEN.remove(database); ownershipReleased = true; }
    }
  }
}
