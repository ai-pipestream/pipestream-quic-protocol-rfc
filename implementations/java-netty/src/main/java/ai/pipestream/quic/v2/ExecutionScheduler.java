package ai.pipestream.quic.v2;

import java.io.IOException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * Authority-owned durable job discovery and bounded leaf dispatch, independent of connections. The
 * database is the job source; a volatile cursor and bounded in-flight map are only scheduling
 * hints. Restart begins a new sweep and every callback still requires a committed current lease.
 * This does not activate a durable-profile endpoint or implement branch/cancellation lifecycle.
 */
final class ExecutionScheduler implements AutoCloseable {
  /**
   * Physical worker and discovery ceilings for one shared authority runner.
   *
   * @param workers maximum submitted or executing invocations, with no waiting job queue
   * @param workersPerOwner one retained owner's physical share across sessions
   * @param pageSize maximum job records examined in each discovery snapshot
   * @param pollMillis positive delay between pages, also bounding idle sweep frequency
   */
  record Limits(int workers, int workersPerOwner, int pageSize, long pollMillis) {
    /** Validate finite deployment ceilings. */
    Limits {
      Checks.range(workers, 1, 128);
      Checks.range(workersPerOwner, 1, workers);
      Checks.range(pageSize, 1, 64);
      Checks.range(pollMillis, 1, 60_000);
    }
  }

  /**
   * One bounded diagnostic observation, not a durable computation outcome.
   *
   * @param position affected job, null for discovery/clock failures
   * @param code named refusal or INTERNAL_ERROR for local infrastructure failure
   * @param detail fixed local phase description, never application exception text
   */
  record Failure(ExecutionStore.Position position, ProtocolError.Code code, String detail) {}

  /**
   * Bounded process diagnostics; counters saturate and are not protocol completion counts.
   *
   * @param running discovery has started and has not been stopped
   * @param active submitted/running invocations, including cleanup
   * @param completed callback runner calls that returned an authoritative view
   * @param refused failed discovery, dispatch or maintenance calls
   * @param lastFailure last refusal, retained across subsequent successful calls
   */
  record Status(boolean running, int active, long completed, long refused, Failure lastFailure) {}

  private final SessionStore sessions;
  private final ExecutionRuntime runtime;
  private final AdmissionStore.Clock clock;
  private final Function<String, ExecutionStore.Access> grants;
  private final Limits limits;
  private final ThreadPoolExecutor workers;
  private final Thread dispatcher;
  private final Map<ExecutionStore.Position, String> inFlight = new HashMap<>();
  private final Map<String, Integer> owners = new HashMap<>();
  private ExecutionStore.ScanCursor cursor;
  private final ClosureStore.Cursor closures = new ClosureStore.Cursor();
  private boolean started;
  private boolean stopping;
  private long completed;
  private long refused;
  private Failure lastFailure;

  /**
   * Configure an inactive scheduler for an already initialized and verified paired authority.
   * Grants are resolved on worker threads from retained principal identity, never a captured
   * connection certificate. The returned gate must recheck current permission on every use.
   *
   * @param sessions same authority handle used by the callback runtime
   * @param runtime shared bounded callback runner
   * @param clock trusted nonblocking deployment UTC source
   * @param grants current local execution-grant resolver, without application effects
   * @param limits physical worker and bounded discovery policy
   */
  ExecutionScheduler(
      SessionStore sessions,
      ExecutionRuntime runtime,
      AdmissionStore.Clock clock,
      Function<String, ExecutionStore.Access> grants,
      Limits limits) {
    this.sessions = Objects.requireNonNull(sessions);
    this.runtime = Objects.requireNonNull(runtime);
    this.clock = Objects.requireNonNull(clock);
    this.grants = Objects.requireNonNull(grants);
    this.limits = Objects.requireNonNull(limits);
    runtime.checkScheduler(sessions, limits.workers(), limits.workersPerOwner());
    workers =
        new ThreadPoolExecutor(
            limits.workers(),
            limits.workers(),
            0,
            TimeUnit.MILLISECONDS,
            new SynchronousQueue<>(),
            Thread.ofPlatform().daemon().name("pipestream-v2-worker-", 0).factory(),
            new ThreadPoolExecutor.AbortPolicy());
    dispatcher = Thread.ofPlatform().daemon().name("pipestream-v2-discovery").unstarted(this::loop);
  }

  /** Start one background discovery thread. A stopped scheduler cannot be restarted. */
  synchronized void start() {
    if (started || stopping) throw new IllegalStateException("scheduler already started or closed");
    started = true;
    try {
      dispatcher.start();
    } catch (RuntimeException | Error failure) {
      close();
      throw failure;
    }
  }

  /**
   * Observe bounded process diagnostics without waiting for a callback or database transaction.
   *
   * @return immutable local observation, not a work receipt or root checkpoint
   */
  synchronized Status status() {
    return new Status(started && !stopping, inFlight.size(), completed, refused, lastFailure);
  }

  /**
   * Stop further dispatch without cancelling logical work, interrupting accepted callbacks or
   * closing the paired stores. An already running maintenance transaction may finish. The host must
   * await physical stop before closing storage; uncooperative callbacks can prevent that.
   */
  @Override
  public synchronized void close() {
    stopping = true;
    workers.shutdown();
    notifyAll();
  }

  /**
   * Wait within one monotonic budget for discovery and physical workers to stop.
   *
   * @param timeoutMillis maximum total wait, zero for an immediate observation
   * @return true only when neither discovery nor a physical worker remains live
   * @throws InterruptedException the waiting host thread was interrupted
   */
  boolean awaitStopped(long timeoutMillis) throws InterruptedException {
    Checks.range(timeoutMillis, 0, 300_000);
    long budget = TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
    long start = System.nanoTime();
    if (budget > 0 && dispatcher.isAlive()) TimeUnit.NANOSECONDS.timedJoin(dispatcher, budget);
    long left = budget - (System.nanoTime() - start);
    if (left > 0) workers.awaitTermination(left, TimeUnit.NANOSECONDS);
    return !dispatcher.isAlive() && workers.isTerminated();
  }

  private void loop() {
    try {
      while (!stopped()) {
        try {
          page();
        } catch (SQLException | RuntimeException failure) {
          record(null, failure, "job discovery unavailable");
        }
        if (!stopped()) {
          try {
            sessions.reconcileClosures(closures, limits.pageSize(), clock);
          } catch (SQLException | RuntimeException failure) {
            record(null, failure, "closure reconciliation unavailable");
          }
        }
        synchronized (this) {
          if (!stopping) wait(limits.pollMillis());
        }
      }
    } catch (InterruptedException interruption) {
      Thread.currentThread().interrupt();
      record(
          null,
          new ProtocolError(ProtocolError.Code.INTERNAL_ERROR, "interrupted"),
          "discovery thread interrupted");
    } catch (Error fatal) {
      record(null, fatal, "fatal discovery failure");
      throw fatal;
    } finally {
      close();
    }
  }

  private synchronized boolean stopped() {
    return stopping;
  }

  private void page() throws SQLException {
    ExecutionStore.Page page = sessions.scanExecutions(cursor, limits.pageSize());
    // Discovery grants no time promise. The actual claim/expiry rechecks the durable watermark.
    long now = AdmissionStore.checkedClock(clock).sample().utcMillis();
    for (ExecutionStore.Candidate candidate : page.entries()) {
      if (stopped()) return;
      if (candidate.stage() == JobRecord.Stage.SETTLED) continue;
      if (now >= candidate.deadline()) {
        try {
          sessions.expireExecution(candidate.position().generation(), candidate.work(), clock);
        } catch (SQLException | RuntimeException failure) {
          record(candidate.position(), failure, "deadline settlement refused");
        }
        continue;
      }
      if (candidate.stage() == JobRecord.Stage.AWAITING_RETRY) continue;
      if (candidate.leaseUntil() != null && now < candidate.leaseUntil()) continue;
      if (candidate.mode() != 0) {
        record(
            candidate.position(),
            new ProtocolError(ProtocolError.Code.APPLICATION_UNSUPPORTED, "branch runtime"),
            "branch execution is not implemented");
        continue;
      }
      dispatch(candidate);
    }
    // Advance only after a complete page. A stopped or failed page can be safely rediscovered.
    cursor = page.next();
  }

  private synchronized void dispatch(ExecutionStore.Candidate candidate) {
    String owner = candidate.owner();
    if (stopping
        || inFlight.containsKey(candidate.position())
        || inFlight.size() >= limits.workers()
        || owners.getOrDefault(owner, 0) >= limits.workersPerOwner()) return;
    inFlight.put(candidate.position(), owner);
    owners.merge(owner, 1, Integer::sum);
    try {
      workers.execute(() -> execute(candidate));
    } catch (RejectedExecutionException failure) {
      // A worker may have removed its map entry just before returning to the no-queue executor.
      // A later sweep retries the still-durable job; rejection cannot lose accepted work.
      release(candidate);
      record(
          candidate.position(),
          new ProtocolError(ProtocolError.Code.LIMIT_EXCEEDED, "physical worker unavailable"),
          "physical dispatch capacity unavailable");
    } catch (RuntimeException | Error failure) {
      release(candidate);
      throw failure;
    }
  }

  private void execute(ExecutionStore.Candidate candidate) {
    try {
      ExecutionStore.Access access = grants.apply(candidate.owner());
      if (access == null || !candidate.owner().equals(access.owner()))
        throw new ProtocolError(
            ProtocolError.Code.UNAUTHORIZED, "retained execution owner differs");
      runtime.run(access, candidate.position().generation(), candidate.work());
      synchronized (this) {
        completed = increment(completed);
      }
    } catch (IOException | SQLException | RuntimeException failure) {
      record(candidate.position(), failure, "callback dispatch or settlement refused");
    } catch (Error fatal) {
      record(candidate.position(), fatal, "fatal worker failure");
      close();
      throw fatal;
    } finally {
      release(candidate);
    }
  }

  private synchronized void release(ExecutionStore.Candidate candidate) {
    String owner = inFlight.remove(candidate.position());
    if (owner != null) {
      int remaining = owners.get(owner) - 1;
      if (remaining == 0) owners.remove(owner);
      else owners.put(owner, remaining);
    }
  }

  private synchronized void record(
      ExecutionStore.Position position, Throwable failure, String phase) {
    refused = increment(refused);
    lastFailure =
        new Failure(
            position,
            failure instanceof ProtocolError error
                ? error.code()
                : ProtocolError.Code.INTERNAL_ERROR,
            phase);
  }

  private static long increment(long count) {
    return count == Long.MAX_VALUE ? count : count + 1;
  }
}
