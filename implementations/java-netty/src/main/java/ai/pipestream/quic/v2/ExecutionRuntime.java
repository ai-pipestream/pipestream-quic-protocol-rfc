package ai.pipestream.quic.v2;

import static ai.pipestream.quic.v2.Records.*;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Bounded synchronous application execution, separate from transport readers and metadata
 * transactions. The host dispatches calls on its worker threads; there is no volatile job queue.
 * Leaf and caller-expanded branch callbacks use the same fenced I/O boundary. Authority-produced
 * expansion requires its separate durable producer interface before registering mode 2 here.
 */
final class ExecutionRuntime {
  /**
   * Physical invocation and incremental I/O ceilings for one host-owned runner.
   *
   * @param workers maximum simultaneously running callbacks
   * @param workersPerOwner one retained owner's share across sessions
   * @param leaseMillis local lease duration, without extending the execution deadline
   * @param bufferBytes maximum application I/O chunk
   */
  record Limits(int workers, int workersPerOwner, long leaseMillis, int bufferBytes) {
    /** Validate bounded deployment configuration. */
    Limits {
      Checks.range(workers, 1, 128);
      Checks.range(workersPerOwner, 1, workers);
      Checks.range(leaseMillis, 1, 300_000);
      Checks.range(bufferBytes, 1, 65536);
    }
  }

  /** Application code must implement its declared restart-safety contract. */
  @FunctionalInterface
  interface Callback {
    /**
     * Process admitted input outside the authority transaction.
     *
     * @param context thread-confined, revocable input/output interface
     * @return explicit processing outcome
     * @throws Exception application failure, not automatic retry authorization
     */
    Outcome execute(Context context) throws Exception;
  }

  /**
   * Bind real code to an exact versioned contract; no label or mode fallback exists.
   *
   * @param contract immutable configured restart guarantee and supported modes
   * @param callback actual processing implementation
   */
  record Registration(AdmissionStore.Application contract, Callback callback) {
    /** Require real code and only the implemented leaf or caller-expanded branch modes. */
    Registration {
      Objects.requireNonNull(contract);
      Objects.requireNonNull(callback);
      if (!Set.of(0, 1).containsAll(contract.modes()))
        throw error(
            ProtocolError.Code.APPLICATION_UNSUPPORTED,
            "authority expansion runtime is not implemented");
    }
  }

  /**
   * Explicit callback disposition, not a transport response.
   *
   * @param success publish the completed exact output set
   * @param retryable retain a nonterminal attempt failure for explicit caller retry
   * @param diagnostic bounded explanation, required for either failure disposition
   */
  record Outcome(boolean success, boolean retryable, Diagnostic diagnostic) {
    /** Reject contradictory application dispositions. */
    Outcome {
      if (success && (retryable || diagnostic != null) || !success && diagnostic == null)
        throw new IllegalArgumentException("invalid application outcome");
    }

    /**
     * Returns a successful application outcome.
     *
     * @return a request to publish the exact completed object set
     */
    static Outcome succeeded() {
      return new Outcome(true, false, null);
    }

    /**
     * Return a terminal application failure.
     *
     * @param diagnostic bounded application explanation
     * @return terminal failure request
     */
    static Outcome failed(Diagnostic diagnostic) {
      return new Outcome(false, false, diagnostic);
    }

    /**
     * Request an explicit retryable attempt outcome, never an automatic rerun.
     *
     * @param diagnostic bounded application explanation
     * @return nonterminal failure request
     */
    static Outcome retryable(Diagnostic diagnostic) {
      return new Outcome(false, true, diagnostic);
    }
  }

  private final SessionStore sessions;
  private final InputStore inputs;
  private final List<Registration> applications;
  private final PublicationStore.Endpoint endpoint;
  private final AdmissionStore.Clock clock;
  private final AdmissionStore.Authorization authorization;
  private final Limits limits;
  private final Map<String, Integer> owners = new HashMap<>();
  private int active;

  /**
   * Construct a runner for an already initialized paired authority.
   *
   * @param sessions authority metadata
   * @param inputs exclusively opened input/output storage
   * @param applications exact supported callbacks
   * @param endpoint trusted deployment result endpoint
   * @param clock trusted UTC source, independent of connection credentials
   * @param authorization current retained application grants
   * @param limits bounded invocation and chunk configuration
   */
  ExecutionRuntime(
      SessionStore sessions,
      InputStore inputs,
      List<Registration> applications,
      PublicationStore.Endpoint endpoint,
      AdmissionStore.Clock clock,
      AdmissionStore.Authorization authorization,
      Limits limits) {
    this.sessions = Objects.requireNonNull(sessions);
    this.inputs = Objects.requireNonNull(inputs);
    this.applications = List.copyOf(applications);
    this.endpoint = Objects.requireNonNull(endpoint);
    this.clock = Objects.requireNonNull(clock);
    this.authorization = Objects.requireNonNull(authorization);
    this.limits = Objects.requireNonNull(limits);
    AdmissionStore.ExecutionPolicy policy = sessions.executionPolicy();
    if (applications.size() > 16
        || limits.workers() > policy.maxJobs()
        || limits.workersPerOwner() > policy.maxJobsPerOwner())
      throw ProtocolError.limit("runtime exceeds retained executor policy");
    for (int i = 0; i < this.applications.size(); i++) {
      Registration registration = this.applications.get(i);
      if (!policy.applications().contains(registration.contract()))
        throw error(
            ProtocolError.Code.APPLICATION_UNSUPPORTED, "runtime contract differs from admission");
      for (int j = 0; j < i; j++)
        if (this.applications.get(j).contract().label().equals(registration.contract().label()))
          throw new IllegalArgumentException("duplicate runtime application");
    }
  }

  /**
   * Claim and execute an admitted job on the calling worker thread. No callback runs before the
   * claim commits, and no callback or blocking payload operation belongs on a control reader.
   * Invocation slots are retained until code returns and physical handles close.
   *
   * @param access current retained execution grant
   * @param generation admitted session
   * @param work admitted logical work
   * @return authoritative success, failure or awaiting-retry view
   * @throws IOException retained payload or physical resource failure
   * @throws SQLException metadata failure
   */
  WorkView run(ExecutionStore.Access access, long generation, WorkKey work)
      throws IOException, SQLException {
    Objects.requireNonNull(access).check();
    acquire(access.owner());
    Context context;
    try {
      AdmissionStore.Clock checkedClock = AdmissionStore.checkedClock(clock);
      AdmissionStore.Authorization gate =
          (binding, parameters) -> {
            authorization.check(binding, parameters);
            resolve(parameters);
          };
      // Claim, orphan eligibility and opening the input are serialized with all output writers.
      // The callback itself is deliberately outside this monitor and every metadata transaction.
      synchronized (inputs) {
        long started = System.nanoTime();
        AdmissionStore.Time observed = checkedClock.sample();
        ExecutionStore.Lease lease =
            sessions.claimExecution(
                access, generation, work, inputs, limits.leaseMillis(), checkedClock, gate);
        ExecutionStore.Details details =
            sessions.describeExecution(access, lease, checkedClock, gate);
        Commitments.Context identity =
            new Commitments.Context(details.binding().authority(), access.owner(), generation);
        inputs.reclaimOutputs(identity, details.job().input(), lease);
        sessions.checkExecution(access, lease, checkedClock, gate);
        OutputStore.WriterCredit writerCredit =
            details.job().input().parameters().outputs().count() == 0
                ? null
                : inputs.reserveOutputWriter(identity, details.job().input(), lease);
        InputStream reader = null;
        OutputStore.ReaderCredit childCredit = null;
        try {
          if (details.job().input().parameters().mode() != 0)
            childCredit = inputs.reserveOutputReader();
          reader =
              inputs
                  .find(identity, details.job().input())
                  .orElseThrow(() -> new IOException("admitted input missing"))
                  .openStream();
          context =
              new Context(
                  access,
                  lease,
                  details,
                  identity,
                  checkedClock,
                  gate,
                  reader,
                  writerCredit,
                  childCredit,
                  started,
                  interval(lease, observed));
        } catch (IOException | RuntimeException | Error failure) {
          if (reader != null)
            try {
              reader.close();
            } catch (IOException cleanup) {
              failure.addSuppressed(cleanup);
            }
          if (writerCredit != null)
            try {
              writerCredit.close();
            } catch (IOException cleanup) {
              failure.addSuppressed(cleanup);
            }
          if (childCredit != null)
            try {
              childCredit.close();
            } catch (IOException cleanup) {
              failure.addSuppressed(cleanup);
            }
          throw failure;
        }
      }
    } catch (IOException | SQLException | RuntimeException | Error failure) {
      release(access.owner());
      throw failure;
    }
    try (Invocation invocation = new Invocation(context)) {
      return invocation.context.execute();
    }
  }

  private Registration resolve(AdmitParameters parameters) {
    for (Registration registration : applications)
      if (registration.contract().label().equals(parameters.application())
          && registration.contract().modes().contains(parameters.mode())) return registration;
    throw error(ProtocolError.Code.APPLICATION_UNSUPPORTED, "no callback for admitted application");
  }

  /**
   * Check the owning discovery source and physical pool geometry before background dispatch.
   *
   * @param source same authority handle used by this runner
   * @param workers scheduler worker ceiling
   * @param perOwner scheduler per-owner ceiling
   */
  void checkScheduler(SessionStore source, int workers, int perOwner) {
    if (source != sessions) throw new IllegalArgumentException("scheduler authority differs");
    if (workers > limits.workers() || perOwner > limits.workersPerOwner())
      throw ProtocolError.limit("scheduler exceeds callback runtime capacity");
  }

  private synchronized void acquire(String owner) {
    int count = owners.getOrDefault(owner, 0);
    if (active >= limits.workers() || count >= limits.workersPerOwner())
      throw ProtocolError.limit("physical callback capacity exhausted");
    active++;
    owners.put(owner, count + 1);
  }

  private synchronized void release(String owner) {
    int count = owners.get(owner);
    if (count == 1) owners.remove(owner);
    else owners.put(owner, count - 1);
    active--;
  }

  private long interval(ExecutionStore.Lease lease, AdmissionStore.Time observed) {
    return Math.min(limits.leaseMillis(), Math.max(0, lease.until() - observed.utcMillis()))
        * 1_000_000;
  }

  private final class Invocation implements AutoCloseable {
    private final Context context;

    Invocation(Context context) {
      this.context = context;
    }

    @Override
    public void close() throws IOException {
      try {
        context.close();
      } finally {
        // Namespace cleanup may fail after descriptors closed. Release only with positive
        // physical-close evidence, but do not leak a worker slot for a directory-sync error.
        if (context.physicalClosed) release(context.access.owner());
      }
    }
  }

  /** Revocable, bounded and thread-confined application I/O; it exposes no raw file handle. */
  final class Context {
    private final Thread thread = Thread.currentThread();
    private final ExecutionStore.Access access;
    private ExecutionStore.Lease lease;
    private final ExecutionStore.Details details;
    private final Commitments.Context identity;
    private final AdmissionStore.Clock checkedClock;
    private final AdmissionStore.Authorization gate;
    private final InputStream reader;
    private final OutputStore.WriterCredit writerCredit;
    private final OutputStore.ReaderCredit childCredit;
    private final AtomicReference<Diagnostic> failure = new AtomicReference<>();
    private boolean acceptFailures = true;
    private Exception storageFailure;
    private OutputStore.Writer pending;
    private InputStream childReader;
    private boolean childEof;
    private int produced;
    private volatile boolean open = true;
    private boolean physicalClosed;
    private long leaseStart;
    private long leaseNanos;

    private Context(
        ExecutionStore.Access access,
        ExecutionStore.Lease lease,
        ExecutionStore.Details details,
        Commitments.Context identity,
        AdmissionStore.Clock checkedClock,
        AdmissionStore.Authorization gate,
        InputStream reader,
        OutputStore.WriterCredit writerCredit,
        OutputStore.ReaderCredit childCredit,
        long started,
        long interval) {
      this.access = access;
      this.lease = lease;
      this.details = details;
      this.identity = identity;
      this.checkedClock =
          () -> {
            threadCheck();
            monotonicCheck();
            AdmissionStore.Time sample = checkedClock.sample();
            monotonicCheck();
            return sample;
          };
      this.gate =
          (binding, parameters) -> {
            threadCheck();
            monotonicCheck();
            gate.check(binding, parameters);
            monotonicCheck();
          };
      this.reader = reader;
      this.writerCredit = writerCredit;
      this.childCredit = childCredit;
      leaseStart = started;
      leaseNanos = interval;
    }

    /**
     * Returns the admitted input commitment.
     *
     * @return immutable admitted input commitment, not a payload copy
     */
    Input input() {
      threadCheck();
      return details.job().input().parameters().input();
    }

    /**
     * Returns the current local execution lease.
     *
     * @return current local fence, not a bearer credential or wire retry
     */
    ExecutionStore.Lease lease() {
      threadCheck();
      return lease;
    }

    /**
     * Returns the maximum incremental I/O size.
     *
     * @return maximum incremental I/O size
     */
    int bufferLimit() {
      threadCheck();
      return limits.bufferBytes();
    }

    /**
     * Check current authorization, ancestor fences, attempt, lease and original deadline.
     *
     * @throws SQLException metadata unavailable
     */
    void check() throws SQLException {
      threadCheck();
      monotonicCheck();
      try {
        sessions.checkExecution(access, lease, checkedClock, gate);
      } catch (SQLException failure) {
        storageFailure = failure;
        throw failure;
      } catch (ProtocolError failure) {
        throw record(failure);
      } catch (RuntimeException gateFailure) {
        recordDiagnostic(
            new Diagnostic(
                ProtocolError.Code.INTERNAL_ERROR.value(), "execution policy check failed"));
        throw gateFailure;
      }
    }

    /**
     * Cooperatively renew before the existing lease expires; this never changes a wire attempt.
     *
     * @throws SQLException metadata unavailable
     */
    void renew() throws SQLException {
      check();
      long started = System.nanoTime();
      try {
        AdmissionStore.Time observed = checkedClock.sample();
        ExecutionStore.Lease renewed =
            sessions.renewExecution(access, lease, limits.leaseMillis(), checkedClock, gate);
        long extension =
            Math.min(limits.leaseMillis(), renewed.until() - lease.until()) * 1_000_000;
        long remaining = leaseNanos - (started - leaseStart);
        lease = renewed;
        leaseStart = started;
        // A frozen UTC value cannot create new process time through repeated renewals which
        // leave the durable expiry unchanged. Only a positive retained-expiry delta adds time.
        leaseNanos = Math.min(interval(lease, observed), remaining + extension);
      } catch (SQLException failure) {
        storageFailure = failure;
        throw failure;
      } catch (ProtocolError failure) {
        throw record(failure);
      } catch (RuntimeException gateFailure) {
        recordDiagnostic(
            new Diagnostic(
                ProtocolError.Code.INTERNAL_ERROR.value(), "execution policy check failed"));
        throw gateFailure;
      }
    }

    /**
     * Read a bounded input chunk after checking the current fence.
     *
     * @param bytes caller-owned incremental buffer
     * @param offset destination offset
     * @param length requested bytes, at most bufferLimit
     * @return bytes read, or -1 at EOF
     * @throws IOException retained input failure
     * @throws SQLException metadata failure
     */
    int readInput(byte[] bytes, int offset, int length) throws IOException, SQLException {
      return io(
          () -> {
            Objects.checkFromIndexSize(offset, length, bytes.length);
            chunk(length);
            return reader.read(bytes, offset, length);
          });
    }

    /**
     * Page only this parent's exact closed and successful direct child scope.
     *
     * @param after exclusive lower child entity bound
     * @param limit maximum returned identities, from one through 256
     * @return ordered child identities and continuation flag, not payloads
     * @throws IOException earlier physical I/O failure
     * @throws SQLException unavailable or contradictory retained evidence
     */
    BranchStore.Page children(long after, int limit) throws IOException, SQLException {
      return io(() -> sessions.children(access, lease, after, limit, checkedClock, gate));
    }

    /**
     * Open one committed direct-child output under the current parent dependency. External result
     * expiry does not revoke this internal dependency; a locator is never dereferenced.
     *
     * @param entity member of this parent's own child scope
     * @param index exact committed output index
     * @return immutable descriptor for the opened payload
     * @throws IOException missing, corrupt or unavailable physical output
     * @throws SQLException unavailable or contradictory retained evidence
     */
    Output beginChildOutput(long entity, int index) throws IOException, SQLException {
      return io(
          () -> {
            if (childCredit == null)
              throw error(ProtocolError.Code.CONFLICT, "leaf has no child reader");
            if (childReader != null)
              throw error(ProtocolError.Code.CONFLICT, "a child output is already open");
            BranchStore.Source source =
                sessions.childOutput(access, lease, entity, index, checkedClock, gate);
            childReader = BranchStore.open(inputs, source, childCredit);
            childEof = false;
            return source.output();
          });
    }

    /**
     * Read one bounded child-output chunk after checking current parent ownership and policy.
     *
     * @param bytes caller-owned incremental destination
     * @param offset destination offset
     * @param length requested bytes, at most bufferLimit
     * @return bytes read, zero for an empty request, or -1 at verified EOF
     * @throws IOException retained payload failure
     * @throws SQLException unavailable execution metadata
     */
    int readChildOutput(byte[] bytes, int offset, int length) throws IOException, SQLException {
      return io(
          () -> {
            Objects.checkFromIndexSize(offset, length, bytes.length);
            chunk(length);
            if (childReader == null)
              throw error(ProtocolError.Code.CONFLICT, "no child output is open");
            int read = childReader.read(bytes, offset, length);
            if (read < 0) childEof = true;
            return read;
          });
    }

    /**
     * Finish an exactly consumed child output and return its sequential reader capacity.
     *
     * @throws IOException failed physical close
     * @throws SQLException unavailable execution metadata
     */
    void finishChildOutput() throws IOException, SQLException {
      io(
          () -> {
            if (childReader == null)
              throw error(ProtocolError.Code.CONFLICT, "no child output is open");
            if (!childEof)
              throw error(ProtocolError.Code.INTEGRITY_ERROR, "child output lacks verified EOF");
            childReader.close();
            childReader = null;
            childEof = false;
            return null;
          });
    }

    /**
     * Start one exact-length output under admission-funded count and byte allowances.
     *
     * @param length exact output payload length
     * @param contentType printable ASCII content type
     * @throws IOException physical storage failure
     * @throws SQLException metadata failure
     */
    void beginOutput(long length, String contentType) throws IOException, SQLException {
      io(
          () -> {
            if (pending != null) throw error(ProtocolError.Code.CONFLICT, "output already open");
            if (produced >= details.job().input().parameters().outputs().count())
              throw ProtocolError.limit("application output count exceeded");
            pending =
                inputs.beginOutput(
                    identity,
                    details.job().input(),
                    lease,
                    produced,
                    length,
                    contentType,
                    details.job().objectLimit());
            return null;
          });
    }

    /**
     * Write a bounded chunk; a swallowed over-budget error still prevents success.
     *
     * @param bytes incremental buffer consumed by this call
     * @throws IOException physical storage failure
     * @throws SQLException metadata failure
     */
    void writeOutput(ByteBuffer bytes) throws IOException, SQLException {
      io(
          () -> {
            chunk(bytes.remaining());
            if (pending == null) throw error(ProtocolError.Code.CONFLICT, "no output open");
            pending.write(bytes);
            return null;
          });
    }

    /**
     * Verify and install one complete output; installation is not publication.
     *
     * @return completed zero-based output index
     * @throws IOException physical storage failure
     * @throws SQLException metadata failure
     */
    int finishOutput() throws IOException, SQLException {
      return io(
          () -> {
            if (pending == null) throw error(ProtocolError.Code.CONFLICT, "no output open");
            pending.finish();
            pending = null;
            return produced++;
          });
    }

    private WorkView execute() throws IOException, SQLException {
      check();
      Outcome outcome;
      try {
        outcome =
            Objects.requireNonNull(
                resolve(details.job().input().parameters()).callback().execute(this));
      } catch (InterruptedException interrupted) {
        Thread.currentThread().interrupt();
        throw new IOException("application worker interrupted", interrupted);
      } catch (Exception | AssertionError applicationFailure) {
        Diagnostic diagnostic =
            applicationFailure instanceof ProtocolError protocol
                ? new Diagnostic(protocol.code().value(), "application interface refused")
                : new Diagnostic(
                    ProtocolError.Code.INTERNAL_ERROR.value(), "application callback failed");
        outcome = Outcome.failed(diagnostic);
      }
      // Low-level persistence failures are not fabricated computation outcomes. Preserve the job
      // for recovery/operator inspection; exact authority fences still decide any later mutation.
      if (storageFailure instanceof IOException failure) throw failure;
      if (storageFailure instanceof SQLException failure) throw failure;
      if (outcome.success() && (pending != null || childReader != null))
        failure.compareAndSet(
            null,
            new Diagnostic(
                ProtocolError.Code.INTEGRITY_ERROR.value(),
                "application left unfinished output I/O"));
      Diagnostic sticky;
      synchronized (failure) {
        // Freeze the callback's errors before choosing its outcome. Later use of a captured
        // context still refuses, but cannot race a new error into an already chosen success.
        acceptFailures = false;
        sticky = failure.get();
      }
      if (sticky != null) outcome = Outcome.failed(sticky);
      check();
      if (outcome.success())
        return sessions.succeedExecution(
            access, lease, inputs, produced, endpoint, checkedClock, gate);
      return sessions.failExecution(
          access, lease, outcome.diagnostic(), outcome.retryable(), checkedClock, gate);
    }

    private <T> T io(IoAction<T> action) throws IOException, SQLException {
      // Handles are private to this context/thread. Keep the current-fence check and local I/O
      // under the same store monitor as replacement claims, so an old callback cannot recreate
      // an old-lease slot after the replacement reclaimed it. No application callback runs here.
      synchronized (inputs) {
        check();
        Diagnostic prior = failure.get();
        if (prior != null)
          throw new ProtocolError(
              ProtocolError.Code.from(prior.code()), "prior application I/O failure");
        try {
          return action.run();
        } catch (IOException failure) {
          storageFailure = failure;
          throw failure;
        } catch (SQLException failure) {
          storageFailure = failure;
          throw failure;
        } catch (ProtocolError failure) {
          throw record(failure);
        } catch (RuntimeException invalidCall) {
          recordDiagnostic(
              new Diagnostic(
                  ProtocolError.Code.INTERNAL_ERROR.value(), "invalid application I/O call"));
          throw invalidCall;
        }
      }
    }

    private void chunk(int length) {
      if (length > limits.bufferBytes())
        throw ProtocolError.limit("application chunk limit exceeded");
    }

    private void threadCheck() {
      if (!open || Thread.currentThread() != thread)
        throw record(
            error(
                ProtocolError.Code.CONFLICT,
                "callback context is closed or belongs to another thread"));
    }

    private void monotonicCheck() {
      if (System.nanoTime() - leaseStart >= leaseNanos)
        throw record(error(ProtocolError.Code.CONFLICT, "local monotonic lease interval ended"));
    }

    private ProtocolError record(ProtocolError error) {
      recordDiagnostic(new Diagnostic(error.code().value(), "application interface refused"));
      return error;
    }

    private void recordDiagnostic(Diagnostic diagnostic) {
      synchronized (failure) {
        if (acceptFailures) failure.compareAndSet(null, diagnostic);
      }
    }

    private void close() throws IOException {
      open = false;
      synchronized (failure) {
        acceptFailures = false;
      }
      IOException failure = null;
      boolean writerClosed = pending == null;
      boolean readerClosed = false;
      boolean creditClosed = writerCredit == null;
      boolean childClosed = childReader == null;
      boolean childCreditClosed = childCredit == null;
      // A bounded idempotent retry distinguishes namespace-cleanup failure from a descriptor
      // that remains open. The first error is still reported; persistent uncertainty stays charged.
      for (int attempt = 0; attempt < 2; attempt++) {
        try {
          if (!childClosed) childReader.close();
          childClosed = true;
        } catch (IOException error) {
          if (failure == null) failure = error;
          else if (failure != error) failure.addSuppressed(error);
        }
        try {
          if (!writerClosed) pending.close();
          writerClosed = true;
        } catch (IOException error) {
          if (failure == null) failure = error;
          else if (failure != error) failure.addSuppressed(error);
        }
        try {
          if (!readerClosed) reader.close();
          readerClosed = true;
        } catch (IOException error) {
          if (failure == null) failure = error;
          else if (failure != error) failure.addSuppressed(error);
        }
        try {
          if (!creditClosed) writerCredit.close();
          creditClosed = true;
        } catch (IOException error) {
          if (failure == null) failure = error;
          else if (failure != error) failure.addSuppressed(error);
        }
        try {
          if (!childCreditClosed) childCredit.close();
          childCreditClosed = true;
        } catch (IOException error) {
          if (failure == null) failure = error;
          else if (failure != error) failure.addSuppressed(error);
        }
      }
      physicalClosed =
          writerClosed && readerClosed && creditClosed && childClosed && childCreditClosed;
      if (failure != null) throw failure;
    }
  }

  @FunctionalInterface
  private interface IoAction<T> {
    T run() throws IOException, SQLException;
  }

  private static ProtocolError error(ProtocolError.Code code, String detail) {
    return new ProtocolError(code, detail);
  }
}
