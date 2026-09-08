package ai.pipestream.quic.v2;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

/**
 * Authority-local asynchronous WORK and checkpoint observations. SQLite never runs on the calling
 * transport event loop or timer thread. Waiting releases the database worker between observations;
 * each new observation rechecks the original owner's current authorization. This is not an endpoint
 * and does not advertise a profile. The transport still owns correlation, per-connection pending
 * limits, response delivery deadlines and drain accounting. An authority host must share one
 * instance across its connections; these limits do not coordinate separate instances or processes.
 */
final class ControlWaitService implements AutoCloseable {
  /**
   * Finite authority-wide observation bounds, independent of payload workers.
   *
   * @param pending maximum waiting, queued or physically running observations
   * @param perOwner one owner's share across all connections and sessions
   * @param workers maximum simultaneous blocking observations
   * @param pollMillis delay between bounded scheduling sweeps
   */
  record Limits(int pending, int perOwner, int workers, long pollMillis) {
    /** Validate bounded queues, threads and scheduling intervals. */
    Limits {
      Checks.range(pending, 1, 4096);
      Checks.range(perOwner, 1, pending);
      Checks.range(workers, 1, Math.min(pending, 16));
      Checks.range(pollMillis, 1, 1000);
    }
  }

  /**
   * Current charged observations, including cancelled reads that have not returned from SQLite.
   *
   * @param pending charged observations
   * @param owners distinct charged original owners
   */
  record Usage(int pending, int owners) {}

  /**
   * One connection-local observation. Closing abandons this wait, never durable execution. Callers
   * must marshal completion onto their event loop with an asynchronous continuation; synchronous
   * continuations must not block an authority worker. Cancelling a derived future does not close
   * this handle. The connection must close the handle explicitly on loss or deadline.
   *
   * @param <T> typed authoritative response
   */
  final class Wait<T extends Messages.Message> implements AutoCloseable {
    private final SessionStore.Access access;
    private final Observation<T> observation;
    private final CompletableFuture<T> result = new CompletableFuture<>();
    private final long started;
    private final long duration;
    // All lifecycle state is guarded by registry; no lock is held over a database read.
    private boolean busy;
    private boolean ended;

    private Wait(SessionStore.Access access, long waitMs, Observation<T> observation) {
      this.access = access;
      this.observation = observation;
      started = nanoClock.getAsLong();
      duration = TimeUnit.MILLISECONDS.toNanos(waitMs);
    }

    /**
     * Observe this wait without giving callers authority to forge its result.
     *
     * @return read-only completion stage; persistence failures remain exceptional
     */
    CompletionStage<T> response() {
      return result.minimalCompletionStage();
    }

    private void run() {
      try {
        synchronized (registry) {
          if (ended) return;
        }
        T value = observation.read(() -> nanoClock.getAsLong() - started >= duration);
        if (value != null) finish(value, null);
      } catch (Exception failure) {
        finish(null, failure);
      } catch (Error failure) {
        finish(null, failure);
        throw failure;
      } finally {
        synchronized (registry) {
          busy = false;
          if (ended) release(this);
        }
      }
    }

    private void finish(T value, Throwable failure) {
      synchronized (registry) {
        if (ended) return;
        ended = true;
        if (!busy) release(this);
      }
      if (failure == null) result.complete(value);
      else result.completeExceptionally(failure);
    }

    /** Abandon only this connection's wait; a running read stays charged until it returns. */
    @Override
    public void close() {
      finish(null, new ProtocolError(ProtocolError.Code.CANCELLED, "connection wait abandoned"));
    }
  }

  @FunctionalInterface
  private interface Observation<T> {
    T read(java.util.function.BooleanSupplier expired) throws SQLException;
  }

  private final SessionStore sessions;
  private final Limits limits;
  private final LongSupplier nanoClock;
  private final Object registry = new Object();
  private final Set<Wait<?>> pending = new LinkedHashSet<>();
  private final Map<String, Integer> owners = new HashMap<>();
  private final ThreadPoolExecutor workers;
  private final ScheduledExecutorService timer;
  private boolean stopped;

  /**
   * Start bounded observation workers for the authority.
   *
   * @param sessions opened authority metadata
   * @param limits finite queue, owner and worker bounds
   */
  ControlWaitService(SessionStore sessions, Limits limits) {
    this(sessions, limits, System::nanoTime);
  }

  /**
   * Start with a trusted local monotonic source. Signed nanoTime values and one wrap are supported
   * by subtraction; elapsed intervals must be less than 2^63 nanoseconds. This clock does not issue
   * UTC leases or extend any durable deadline.
   *
   * @param sessions opened authority metadata
   * @param limits finite queue, owner and worker bounds
   * @param nanoClock nonblocking elapsed-time source, never a peer timestamp
   */
  ControlWaitService(SessionStore sessions, Limits limits, LongSupplier nanoClock) {
    this.sessions = Objects.requireNonNull(sessions);
    this.limits = Objects.requireNonNull(limits);
    this.nanoClock = Objects.requireNonNull(nanoClock);
    workers =
        new ThreadPoolExecutor(
            limits.workers(),
            limits.workers(),
            0,
            TimeUnit.MILLISECONDS,
            new ArrayBlockingQueue<>(limits.pending()),
            Thread.ofPlatform().daemon().name("pipestream-v2-observe-", 0).factory(),
            new ThreadPoolExecutor.AbortPolicy());
    ScheduledExecutorService created = null;
    try {
      created =
          Executors.newSingleThreadScheduledExecutor(
              Thread.ofPlatform().daemon().name("pipestream-v2-observe-timer").factory());
      timer = created;
      timer.scheduleWithFixedDelay(
          this::poll, limits.pollMillis(), limits.pollMillis(), TimeUnit.MILLISECONDS);
    } catch (RuntimeException | Error failure) {
      if (created != null) created.shutdownNow();
      workers.shutdown();
      throw failure;
    }
  }

  /**
   * Observe a work revision, waiting only when the caller already knows the current revision.
   * Expiry returns a newly authorized unchanged view, not WAIT_TIMEOUT or a cached prior view.
   *
   * @param access current original-owner gate
   * @param selected negotiated capabilities
   * @param generation attached session
   * @param request exact work, revision, correlation and maximum wait
   * @return connection-local observation handle
   */
  Wait<Messages.WatchResponse> watch(
      SessionStore.Access access,
      Messages.Capabilities selected,
      long generation,
      Messages.Watch request) {
    Objects.requireNonNull(request);
    Objects.requireNonNull(selected);
    return begin(
        access,
        request.waitMs(),
        expired -> {
          Messages.WatchResponse view = sessions.snapshot(access, selected, generation, request);
          if (request.afterRevision() == 0
              || view.revision() != request.afterRevision()
              || expired.getAsBoolean()) {
            Wire.encode(view, selected.controlLimit());
            return view;
          }
          return null;
        });
  }

  /**
   * Wait for a matching sealed scope's immutable closure. Missing, unsealed, unauthorized and
   * mismatching scopes refuse immediately; only a valid unresolved scope consumes the wait.
   *
   * @param access current original-owner gate
   * @param selected negotiated capabilities
   * @param generation attached session
   * @param request exact scope, seal, correlation and maximum wait
   * @return connection-local observation handle
   */
  Wait<Messages.CheckpointResponse> checkpoint(
      SessionStore.Access access,
      Messages.Capabilities selected,
      long generation,
      Messages.Checkpoint request) {
    Objects.requireNonNull(request);
    Objects.requireNonNull(selected);
    return begin(
        access,
        request.waitMs(),
        expired -> {
          var view = sessions.checkpoint(access, selected, generation, request);
          // A queued or slow read cannot turn an elapsed positive wait into a late success. Zero
          // requests are immediate observations and may return an already committed closure.
          if (request.waitMs() != 0 && expired.getAsBoolean())
            throw new ProtocolError(ProtocolError.Code.WAIT_TIMEOUT, "checkpoint wait elapsed");
          if (view.isPresent()) return view.get();
          if (expired.getAsBoolean())
            throw new ProtocolError(ProtocolError.Code.WAIT_TIMEOUT, "checkpoint wait elapsed");
          return null;
        });
  }

  private <T extends Messages.Message> Wait<T> begin(
      SessionStore.Access access, long waitMs, Observation<T> observation) {
    Objects.requireNonNull(access).check();
    Wait<T> wait = new Wait<>(access, waitMs, observation);
    synchronized (registry) {
      if (stopped)
        throw new ProtocolError(ProtocolError.Code.CANCELLED, "observation service stopped");
      int count = owners.getOrDefault(access.owner(), 0);
      if (pending.size() >= limits.pending() || count >= limits.perOwner())
        throw ProtocolError.limit("control observation capacity exhausted");
      pending.add(wait);
      owners.put(access.owner(), count + 1);
    }
    schedule(wait);
    return wait;
  }

  private void poll() {
    List<Wait<?>> snapshot;
    synchronized (registry) {
      if (stopped) return;
      snapshot = List.copyOf(pending);
    }
    for (Wait<?> wait : snapshot) schedule(wait);
  }

  private void schedule(Wait<?> wait) {
    synchronized (registry) {
      if (stopped || wait.ended || wait.busy) return;
      wait.busy = true;
    }
    try {
      workers.execute(wait::run);
    } catch (RuntimeException failure) {
      synchronized (registry) {
        wait.busy = false;
        if (wait.ended) release(wait);
      }
      wait.finish(null, failure);
    }
  }

  // Called only under registry; set removal makes concurrent finish/worker cleanup idempotent.
  private void release(Wait<?> wait) {
    if (!pending.remove(wait)) return;
    owners.compute(wait.access.owner(), (owner, count) -> count == 1 ? null : count - 1);
  }

  /**
   * Inspect physical observation charges without waiting for database work.
   *
   * @return current authority-wide wait accounting
   */
  Usage usage() {
    synchronized (registry) {
      return new Usage(pending.size(), owners.size());
    }
  }

  /**
   * Reject new waits and abandon current ones, without interrupting storage or durable jobs. This
   * method does not claim physical quiescence; usage reaches zero after running reads return.
   */
  @Override
  public void close() {
    List<Wait<?>> snapshot;
    synchronized (registry) {
      if (stopped) return;
      stopped = true;
      snapshot = List.copyOf(pending);
    }
    timer.shutdownNow();
    for (Wait<?> wait : snapshot) wait.close();
    workers.shutdown();
  }
}
