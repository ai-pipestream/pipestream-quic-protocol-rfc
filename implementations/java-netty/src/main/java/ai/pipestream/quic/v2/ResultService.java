package ai.pipestream.quic.v2;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.LongSupplier;

/**
 * Bounded authority-local result delivery leases, including time waiting for a transport stream.
 * The transport must check each lease before scheduling bytes or FIN and report actual accepted
 * payload progress. Disk reads and application enqueue do not renew idle time. Blocking operations
 * run outside transport event loops. This library does not itself activate a V2 endpoint.
 */
final class ResultService implements AutoCloseable {
  /**
   * Deployment bounds shared across all connections of the one exclusive payload installation.
   *
   * @param reads global admitted or acquiring reads
   * @param readsPerOwner one owner's share across sessions and connections
   * @param bufferBytes maximum one outstanding payload chunk per read
   * @param pollMillis maximum scheduled delay between independent maintenance sweeps
   */
  record Limits(int reads, int readsPerOwner, int bufferBytes, long pollMillis) {
    /** Validate finite resource and timer ceilings. */
    Limits {
      Checks.range(reads, 1, 128);
      Checks.range(readsPerOwner, 1, reads);
      Checks.range(bufferBytes, 1, 1 << 20);
      Checks.range(pollMillis, 1, 1000);
    }
  }

  /**
   * Current volatile delivery capacity; durable object bytes remain separately charged.
   *
   * @param reads acquiring, pending, transferring or not yet physically closed reads
   * @param owners owners with at least one charged read
   */
  record Usage(int reads, int owners) {}

  /**
   * One finite sweep, with no lock held while waiting for a busy read.
   *
   * @param inspected entries selected from a bounded registry snapshot
   * @param closed entries actually closed and removed
   * @param busy entries whose I/O or authorization operation was already running
   */
  record Maintenance(int inspected, int closed, int busy) {}

  private final SessionStore sessions;
  private final InputStore inputs;
  private final Limits limits;
  private final LongSupplier nanoClock;
  private final Object registry = new Object();
  private final Set<Read> reads = new LinkedHashSet<>();
  private final Map<String, Integer> owners = new HashMap<>();
  private final ScheduledExecutorService timer;
  private volatile boolean stopping;
  // Guarded by registry. Release the installation claim only once, after every physical close.
  private boolean detached;

  /**
   * Start delivery maintenance for one already paired, exclusive authority installation.
   *
   * @param sessions retained authority metadata
   * @param inputs exclusive paired payload store
   * @param limits finite delivery bounds
   * @throws IOException storage pairing or exclusive service ownership failure
   * @throws SQLException metadata validation failure
   */
  ResultService(SessionStore sessions, InputStore inputs, Limits limits)
      throws IOException, SQLException {
    this(sessions, inputs, limits, System::nanoTime);
  }

  /**
   * Start maintenance with an explicit trusted local elapsed clock. Signed nanoTime values and one
   * wrap are supported by subtraction; elapsed intervals must remain less than 2^63 nanos.
   *
   * @param sessions retained authority metadata
   * @param inputs exclusive paired payload store
   * @param limits finite delivery bounds
   * @param nanoClock nonblocking local monotonic source, never a peer timestamp
   * @throws IOException storage pairing or exclusive service ownership failure
   * @throws SQLException metadata validation failure
   */
  ResultService(SessionStore sessions, InputStore inputs, Limits limits, LongSupplier nanoClock)
      throws IOException, SQLException {
    this.sessions = Objects.requireNonNull(sessions);
    this.inputs = Objects.requireNonNull(inputs);
    this.limits = Objects.requireNonNull(limits);
    this.nanoClock = Objects.requireNonNull(nanoClock);
    sessions.verifyInputs(inputs);
    inputs.claimResults(this);
    ScheduledExecutorService created = null;
    try {
      created =
          Executors.newSingleThreadScheduledExecutor(
              Thread.ofPlatform().daemon().name("pipestream-v2-results").factory());
      timer = created;
      timer.scheduleWithFixedDelay(
          this::maintain, limits.pollMillis(), limits.pollMillis(), TimeUnit.MILLISECONDS);
    } catch (RuntimeException | Error failure) {
      if (created != null) created.shutdownNow();
      inputs.releaseResults(this);
      throw failure;
    }
  }

  /**
   * Acquire a current authenticated output, charging pending time before storage verification.
   * Repeating a request is an independent delivery lease, never a retry of computation.
   *
   * @param access current original-owner credential gate
   * @param selected selected connection capabilities
   * @param generation attached session
   * @param request exact committed object request
   * @param utc trusted UTC source used only for fresh acquisition
   * @param authorization current result permission, rechecked throughout delivery
   * @return one pending transfer to start once, then finish or abort
   * @throws IOException physical storage or cleanup failure
   * @throws SQLException failed metadata transaction
   */
  Read begin(
      SessionStore.Access access,
      Messages.Capabilities selected,
      long generation,
      Messages.Read request,
      AdmissionStore.Clock utc,
      ResultStore.Authorization authorization)
      throws IOException, SQLException {
    Objects.requireNonNull(access).check();
    Read read =
        new Read(
            access,
            Objects.requireNonNull(selected),
            generation,
            Objects.requireNonNull(request),
            Objects.requireNonNull(authorization));
    // Acquire this entry before publishing it to the timer, without holding the registry over I/O.
    read.lock.lock();
    try {
      synchronized (registry) {
        if (stopping) throw error(ProtocolError.Code.CANCELLED, "result service stopped");
        int count = owners.getOrDefault(access.owner(), 0);
        if (reads.size() >= limits.reads() || count >= limits.readsPerOwner())
          throw ProtocolError.limit("result delivery capacity exhausted");
        reads.add(read);
        owners.put(access.owner(), count + 1);
      }
      try {
        read.opened =
            sessions.openResult(
                access, selected, generation, inputs, request, utc, authorization, read::elapsed);
        // Commit and resource release may themselves have taken time. Never return an expired
        // lease.
        read.validate();
        return read;
      } catch (IOException | SQLException | RuntimeException | Error failure) {
        read.fail(failure);
        throw failure;
      }
    } finally {
      read.lock.unlock();
    }
  }

  /**
   * Observe bounded registry usage without waiting for disk or authorization.
   *
   * @return current charged reads and owner count
   */
  Usage usage() {
    synchronized (registry) {
      return new Usage(reads.size(), owners.size());
    }
  }

  /**
   * Check every entry in one finite, at most 128-entry snapshot. Busy entries remain charged and
   * are checked by their foreground operation and a subsequent sweep. Neither the registry lock nor
   * another read lock is held across disk or authorization. The timer calls this without peer
   * activity. Close failures retain their entry so later maintenance can retry physical cleanup.
   *
   * @return bounded progress counters
   */
  Maintenance maintain() {
    List<Read> snapshot;
    synchronized (registry) {
      snapshot = new ArrayList<>(reads);
    }
    int closed = 0;
    int busy = 0;
    for (Read read : snapshot) {
      if (!read.lock.tryLock()) {
        busy++;
        continue;
      }
      try {
        if (read.released) continue; // A foreground close may have won after the snapshot.
        if (read.reason == null) {
          try {
            read.validate();
          } catch (SQLException | RuntimeException failure) {
            read.fail(failure);
          }
        } else {
          try {
            read.dispose();
          } catch (IOException ignored) {
            /* Still charged; retry next sweep. */
          }
        }
        if (read.released) closed++;
      } finally {
        read.lock.unlock();
      }
    }
    return new Maintenance(snapshot.size(), closed, busy);
  }

  /**
   * Stop new acquisition and cancel every idle delivery. A busy I/O or failed physical close stays
   * charged; retry close after it settles. No storage owner is released while reads can still use
   * it.
   *
   * @throws IOException physical cleanup remains pending
   */
  @Override
  public void close() throws IOException {
    stopping = true;
    maintain();
    synchronized (registry) {
      if (!reads.isEmpty()) throw new IOException("result delivery cleanup still pending");
      if (detached) return;
      // Claim release does not wait for a read: all registered reads have physically closed.
      inputs.releaseResults(this);
      detached = true;
      timer.shutdown();
    }
  }

  private void release(Read read) {
    synchronized (registry) {
      if (!reads.remove(read)) return;
      int count = owners.get(read.access.owner());
      if (count == 1) owners.remove(read.access.owner());
      else owners.put(read.access.owner(), count - 1);
    }
  }

  /** A single pending or transferring delivery, with one bounded outstanding payload chunk. */
  final class Read implements AutoCloseable {
    private final SessionStore.Access access;
    private final Messages.Capabilities selected;
    private final long generation;
    private final Messages.Read request;
    private final ResultStore.Authorization authorization;
    private final ReentrantLock lock = new ReentrantLock();
    private final long created;
    private long observed;
    private long progress;
    private ResultStore.Opened opened;
    private ProtocolError.Code reason;
    private boolean started;
    private boolean eof;
    private int pending;
    private boolean released;

    private Read(
        SessionStore.Access access,
        Messages.Capabilities selected,
        long generation,
        Messages.Read request,
        ResultStore.Authorization authorization) {
      this.access = access;
      this.selected = selected;
      this.generation = generation;
      this.request = request;
      this.authorization = authorization;
      created = nanoClock.getAsLong();
      observed = created;
      progress = created;
    }

    /**
     * Start the sole response stream. Stream-slot waiting does not reset either deadline.
     *
     * @return exact committed response header, not yet a claim of successful delivery
     * @throws IOException physical cleanup failure
     * @throws SQLException failed authorization observation
     */
    Records.ResultHeader start() throws IOException, SQLException {
      return operation(
          () -> {
            if (started) throw error(ProtocolError.Code.CONFLICT, "result stream already started");
            started = true;
            return opened.header();
          });
    }

    /**
     * Read one bounded chunk. The caller must not request another until accepted progress consumes
     * the previous one. This does not renew idle time or grant permission for a later transport
     * write.
     *
     * @param buffer destination owned by the bounded transport adapter
     * @param offset first writable position
     * @param length positive capacity, no greater than the configured chunk ceiling
     * @return payload count, or -1 after the complete object
     * @throws IOException physical cleanup failure
     * @throws SQLException failed authorization observation
     */
    int read(byte[] buffer, int offset, int length) throws IOException, SQLException {
      return operation(
          () -> {
            Objects.checkFromIndexSize(offset, length, Objects.requireNonNull(buffer).length);
            if (length < 1 || length > limits.bufferBytes())
              throw ProtocolError.limit("result chunk exceeds configured bounds");
            if (!started || pending != 0)
              throw error(ProtocolError.Code.CONFLICT, "result stream not ready for another chunk");
            int count;
            try {
              count = opened.reader().read(buffer, offset, length);
            } catch (IOException failure) {
              ProtocolError refusal =
                  error(ProtocolError.Code.OUTPUT_UNAVAILABLE, "retained output read failed");
              refusal.initCause(failure);
              throw refusal;
            }
            validate(); // Disk work can cross deadlines or permission changes.
            pending = Math.max(0, count);
            eof = count == -1;
            return count;
          });
    }

    /**
     * Record payload bytes actually accepted by the bounded transport writer. Call check
     * immediately before that write, including after waiting for flow-control capacity. Enqueue is
     * not acceptance.
     *
     * @param count actual accepted payload count; zero grants no renewal
     * @throws IOException physical cleanup failure
     * @throws SQLException failed authorization observation
     */
    void sent(int count) throws IOException, SQLException {
      operation(
          () -> {
            if (!started || count < 0 || count > pending)
              throw error(ProtocolError.Code.INTEGRITY_ERROR, "result progress exceeds read bytes");
            pending -= count;
            if (count > 0) progress = observed;
            return null;
          });
    }

    /**
     * Recheck immediately before scheduling payload or FIN; checking is not transport progress.
     *
     * @throws IOException physical cleanup failure
     * @throws SQLException failed authorization observation
     */
    void check() throws IOException, SQLException {
      operation(() -> null);
    }

    /**
     * Release resources after the complete payload and a successfully scheduled transport FIN,
     * never a reset. The transport must check immediately before scheduling that FIN.
     *
     * @throws IOException physical close failure, leaving capacity charged
     * @throws SQLException failed authorization observation
     */
    void finish() throws IOException, SQLException {
      operation(
          () -> {
            if (!started || !eof || pending != 0)
              throw error(
                  ProtocolError.Code.INTEGRITY_ERROR, "result stream has not completed its object");
            reason = ProtocolError.Code.ALREADY_TERMINAL;
            dispose();
            return null;
          });
    }

    /**
     * Abort only this delivery, leaving all computation state and durable byte charges unchanged.
     * Repeated close is safe, and retries a prior failed physical close.
     *
     * @throws IOException physical descriptor could not be closed
     */
    @Override
    public void close() throws IOException {
      lock.lock();
      try {
        if (reason == null) reason = ProtocolError.Code.CANCELLED;
        dispose();
      } finally {
        lock.unlock();
      }
    }

    private long elapsed() {
      if (reason != null) throw error(reason, "result transfer closed");
      if (stopping) throw error(ProtocolError.Code.CANCELLED, "result service stopped");
      long now = nanoClock.getAsLong();
      if (now - observed < 0)
        throw error(ProtocolError.Code.CLOCK_UNSAFE, "result elapsed clock regressed");
      if (now - created >= TimeUnit.MILLISECONDS.toNanos(selected.streamLifetimeMs())
          || now - progress >= TimeUnit.MILLISECONDS.toNanos(selected.streamIdleMs()))
        throw ProtocolError.limit("result stream deadline reached");
      observed = now;
      return now;
    }

    private void validate() throws SQLException {
      // Denial precedes elapsed-time disclosure, including for an expired/revoked live lease.
      if (reason != null) throw error(reason, "result transfer closed");
      sessions.checkResultRead(access, selected, generation, request.work(), authorization);
      elapsed();
    }

    private <T> T operation(Action<T> action) throws IOException, SQLException {
      lock.lock();
      try {
        try {
          validate();
          return action.run();
        } catch (IOException | SQLException | RuntimeException | Error failure) {
          fail(failure);
          throw failure;
        }
      } finally {
        lock.unlock();
      }
    }

    private void fail(Throwable failure) {
      if (reason == null)
        reason =
            failure instanceof ProtocolError protocol
                ? protocol.code()
                : ProtocolError.Code.INTERNAL_ERROR;
      try {
        dispose();
      } catch (IOException cleanup) {
        failure.addSuppressed(cleanup);
      }
    }

    private void dispose() throws IOException {
      if (released) return;
      if (opened != null) {
        opened.close();
        opened = null;
      }
      pending = 0;
      released = true;
      release(this);
    }
  }

  @FunctionalInterface
  private interface Action<T> {
    T run() throws IOException, SQLException;
  }

  private static ProtocolError error(ProtocolError.Code code, String detail) {
    return new ProtocolError(code, detail);
  }
}
