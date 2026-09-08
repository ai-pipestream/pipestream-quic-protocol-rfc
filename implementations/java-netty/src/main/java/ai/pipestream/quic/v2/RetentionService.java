package ai.pipestream.quic.v2;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * One authority-owned cleanup worker, independent of caller connections and execution capacity. Job
 * discovery uses finite keyset sweeps; orphan discovery is a separately bounded, weakly consistent
 * directory pass. Hints are revalidated under paired ownership before each deletion. Blocking file
 * work is never performed on a transport event loop or the execution dispatcher.
 */
final class RetentionService implements AutoCloseable {
  /**
   * Finite scheduling ceilings, not wall-clock or whole-process memory guarantees.
   *
   * @param pageSize maximum job and physical-candidate visits per maintenance call
   * @param pollMillis delay between background calls
   */
  record Limits(int pageSize, long pollMillis) {
    /** Validate finite limits. */
    Limits {
      Checks.range(pageSize, 1, 64);
      Checks.range(pollMillis, 1, 60_000);
    }
  }

  /**
   * Fixed-size local failure diagnostic, not an authoritative work outcome.
   *
   * @param code protocol refusal or INTERNAL_ERROR for infrastructure failure
   * @param detail fixed local phase, never untrusted exception text
   */
  record Failure(ProtocolError.Code code, String detail) {}

  /**
   * Bounded counters and lifecycle observations, not wire completion counts.
   *
   * @param jobsExamined job candidates examined over all calls
   * @param orphansExamined physical candidates revalidated over all calls
   * @param released resource releases completed by this service
   * @param refused operations refused or failed
   * @param lastFailure most recent fixed-size diagnostic
   * @param stopping no new maintenance page may begin
   * @param stopped active work and physical scan have stopped and storage ownership is released
   */
  record Status(
      long jobsExamined,
      long orphansExamined,
      long released,
      long refused,
      Failure lastFailure,
      boolean stopping,
      boolean stopped) {}

  private final SessionStore sessions;
  private final InputStore inputs;
  private final AdmissionStore.Clock clock;
  private final Limits limits;
  private final ScheduledExecutorService timer;
  private final ReentrantLock operation = new ReentrantLock();
  private ExecutionStore.ScanCursor jobs;
  private InputStore.OrphanScan orphans;
  private InputStore.OrphanCandidate pendingOrphan;
  private volatile boolean stopping;
  private boolean stopped;
  private long jobsExamined;
  private long orphansExamined;
  private long released;
  private long refused;
  private Failure lastFailure;

  /**
   * Verify and attach one automatically running cleanup service to a paired authority.
   *
   * @param sessions retained authority
   * @param inputs exclusive payload installation
   * @param clock trusted UTC source
   * @param limits finite page and polling policy
   * @throws IOException invalid payload binding or storage failure
   * @throws SQLException contradictory retained metadata
   */
  RetentionService(
      SessionStore sessions, InputStore inputs, AdmissionStore.Clock clock, Limits limits)
      throws IOException, SQLException {
    this.sessions = Objects.requireNonNull(sessions);
    this.inputs = Objects.requireNonNull(inputs);
    this.clock = Objects.requireNonNull(clock);
    this.limits = Objects.requireNonNull(limits);
    sessions.verifyInputs(inputs);
    inputs.claimRetention(this);
    ScheduledExecutorService created = null;
    try {
      created =
          Executors.newSingleThreadScheduledExecutor(
              Thread.ofPlatform().daemon().name("pipestream-v2-retention").factory());
      timer = created;
      timer.scheduleWithFixedDelay(
          this::maintain, limits.pollMillis(), limits.pollMillis(), TimeUnit.MILLISECONDS);
    } catch (RuntimeException | Error failure) {
      if (created != null) created.shutdown();
      inputs.releaseRetention(this);
      throw failure;
    }
  }

  /**
   * Perform one bounded scheduling page if no other maintenance call owns the worker. Refused or
   * pinned jobs do not prevent later jobs from being visited. A failed physical orphan deletion
   * retains its exact candidate for retry before advancing that scan; job cleanup remains live.
   *
   * @return current bounded diagnostics
   */
  Status maintain() {
    if (operation.isHeldByCurrentThread() || !operation.tryLock()) return status();
    try {
      if (stopping) return status();
      try {
        ExecutionStore.Page page = sessions.scanExecutions(jobs, limits.pageSize());
        for (ExecutionStore.Candidate candidate : page.entries()) {
          if (stopping) break;
          examinedJob();
          if (candidate.stage() != JobRecord.Stage.SETTLED) continue;
          try {
            if (sessions.reclaimInput(
                    candidate.position().generation(), candidate.work(), inputs, clock)
                == RetentionStore.Result.RELEASED) released();
          } catch (IOException | SQLException | RuntimeException failure) {
            failed(failure, "input retention unavailable");
          }
          if (stopping) break;
          try {
            if (sessions.reclaimOutput(
                    candidate.position().generation(), candidate.work(), inputs, clock)
                == RetentionStore.Result.RELEASED) released();
          } catch (IOException | SQLException | RuntimeException failure) {
            failed(failure, "output retention unavailable");
          }
        }
        jobs = page.next();
      } catch (SQLException | RuntimeException failure) {
        failed(failure, "retention job discovery unavailable");
      }
      if (!stopping) orphanPage();
    } catch (Error failure) {
      failed(failure, "fatal retention failure");
      stopping = true;
      timer.shutdown();
      throw failure;
    } finally {
      if (stopping) {
        try {
          detach();
        } catch (IOException failure) {
          failed(failure, "retention scanner close incomplete");
        }
      }
      operation.unlock();
    }
    return status();
  }

  private void orphanPage() {
    try {
      boolean scanStarted = orphans != null;
      for (int visited = 0; visited < limits.pageSize() && !stopping; visited++) {
        boolean done = false;
        if (pendingOrphan == null) pendingOrphan = inputs.pendingOrphan();
        if (pendingOrphan == null) {
          if (orphans == null) {
            if (scanStarted) break;
            orphans = inputs.scanOrphans();
            scanStarted = true;
          }
          InputStore.OrphanPage page = orphans.nextPage(1);
          done = page.done();
          if (done) orphans = null;
          if (!page.candidates().isEmpty()) pendingOrphan = page.candidates().getFirst();
          if (pendingOrphan == null) {
            if (done) break;
            continue;
          }
        }
        examinedOrphan();
        OrphanStore.Result result = sessions.reclaimOrphan(inputs, pendingOrphan, clock);
        if (result == OrphanStore.Result.RELEASED) released();
        pendingOrphan = null;
        if (done) break;
      }
    } catch (IOException | SQLException | RuntimeException failure) {
      failed(failure, "orphan reconciliation unavailable");
    }
  }

  /**
   * Observe counters without waiting for filesystem or database work.
   *
   * @return fixed-size local status
   */
  synchronized Status status() {
    return new Status(
        jobsExamined, orphansExamined, released, refused, lastFailure, stopping, stopped);
  }

  /**
   * Stop scheduling without interrupting an active deletion or cancelling accepted work. A busy
   * call finishes its current operation and releases ownership on exit. Retry close if a physical
   * directory close failed; storage remains attached and charged until it succeeds.
   *
   * @throws IOException scanner close failed while no operation was active
   */
  @Override
  public void close() throws IOException {
    stopping = true;
    timer.shutdown();
    if (operation.isHeldByCurrentThread() || !operation.tryLock()) return;
    try {
      detach();
    } finally {
      operation.unlock();
    }
  }

  /**
   * Await physical maintenance stop within one monotonic waiting budget. This never interrupts
   * accepted callbacks or a filesystem operation whose completion remains uncertain.
   *
   * @param timeoutMillis maximum wait, zero for an immediate observation
   * @return whether this service released its storage attachment
   * @throws IOException uncertain scanner close
   * @throws InterruptedException waiting thread interrupted
   */
  boolean awaitStopped(long timeoutMillis) throws IOException, InterruptedException {
    Checks.range(timeoutMillis, 0, 300_000);
    if (!stopping) return false;
    if (timeoutMillis == 0 || operation.isHeldByCurrentThread())
      return timer.isTerminated() && status().stopped();
    long budget = TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
    long start = System.nanoTime();
    timer.awaitTermination(budget, TimeUnit.NANOSECONDS);
    long remaining = Math.max(0, budget - (System.nanoTime() - start));
    if (operation.tryLock(remaining, TimeUnit.NANOSECONDS)) {
      try {
        detach();
      } finally {
        operation.unlock();
      }
    }
    return timer.isTerminated() && status().stopped();
  }

  private void detach() throws IOException {
    synchronized (this) {
      if (stopped) return;
    }
    if (orphans != null) orphans.close();
    orphans = null;
    inputs.releaseRetention(this);
    synchronized (this) {
      stopped = true;
    }
  }

  private synchronized void examinedJob() {
    jobsExamined = increment(jobsExamined);
  }

  private synchronized void examinedOrphan() {
    orphansExamined = increment(orphansExamined);
  }

  private synchronized void released() {
    released = increment(released);
  }

  private synchronized void failed(Throwable failure, String detail) {
    refused = increment(refused);
    lastFailure =
        new Failure(
            failure instanceof ProtocolError protocol
                ? protocol.code()
                : ProtocolError.Code.INTERNAL_ERROR,
            detail);
  }

  private static long increment(long value) {
    return value == Long.MAX_VALUE ? value : value + 1;
  }
}
