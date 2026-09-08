package ai.pipestream.quic.v2;

import static ai.pipestream.quic.v2.Messages.*;
import static ai.pipestream.quic.v2.ProtocolError.Code.*;

import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.LongSupplier;

/**
 * One authenticated durable connection's request and transfer ownership. Decode-order acceptance is
 * synchronous and nonblocking; storage workers and transport writes retain separate tickets.
 * Completing a database future alone never releases connection capacity. This class does not run
 * storage, activate a listener, validate input-stream IDs or assert peer receipt of response bytes.
 */
final class DurableRequests implements AutoCloseable {
  private enum Kind {
    ORDINARY,
    BINDING,
    INPUT,
    OUTPUT,
    COMPLETE,
    DETACH
  }

  /**
   * A valid control either has a correlated refusal or an owned accepted ticket, never both.
   *
   * @param refusal immediate named refusal, null on acceptance
   * @param ticket accepted request, null on refusal
   */
  record Acceptance(Refusal refusal, Ticket ticket) {
    /** Require one unambiguous outcome. */
    Acceptance {
      if ((refusal == null) == (ticket == null))
        throw new IllegalArgumentException("acceptance needs exactly one outcome");
    }
  }

  /**
   * Physical connection charges, counting each request once regardless of retained worker copies.
   *
   * @param pending accepted requests and transfers with any live owner
   * @param inputs input transfers including their storage/admission/response work
   * @param outputs result requests including time waiting for a unidirectional stream
   * @param bindingPending creation or attachment still owned by a task or response
   * @param draining detach or completed-session exclusion is active
   */
  record Usage(int pending, int inputs, int outputs, boolean bindingPending, boolean draining) {}

  private final class Flight {
    final Message request;
    final Kind kind;
    final Binding attached;
    final long accepted;
    final CompletableFuture<Void> drained = new CompletableFuture<>();
    int references = 1;

    Flight(Message request, Kind kind) {
      this.request = request;
      this.kind = kind;
      attached = binding;
      accepted = nanoClock.getAsLong();
    }
  }

  /**
   * One physical owner of a request. Retain a copy before dispatching blocking work or transferring
   * ownership to a write/stream callback. Close only after that owner's operation actually ends.
   * Cancelling a caller future is not evidence that its worker or queued write has stopped.
   */
  final class Ticket implements AutoCloseable {
    private final Flight flight;
    private boolean released;

    private Ticket(Flight flight) {
      this.flight = flight;
    }

    /**
     * Retain an independent physical owner, even after the connection itself has closed.
     *
     * @return independently closeable ticket
     */
    Ticket retain() {
      synchronized (lock) {
        requireLive();
        if (flight.references == Integer.MAX_VALUE)
          throw ProtocolError.limit("request owner count exhausted");
        flight.references++;
        return new Ticket(flight);
      }
    }

    /**
     * Inspect the original control request; an input transfer has no control request.
     *
     * @return typed request, or empty for an input
     */
    Optional<Message> request() {
      return Optional.ofNullable(flight.request);
    }

    /**
     * Get the immutable session captured at request acceptance.
     *
     * @return attached binding, empty for pre-binding session commands
     */
    Optional<Binding> binding() {
      return Optional.ofNullable(flight.attached);
    }

    /**
     * Read the original elapsed-time origin, including subsequent worker queue time.
     *
     * @return local monotonic timestamp, not a wire or UTC value
     */
    long acceptedNanos() {
      return flight.accepted;
    }

    /**
     * Install an actually committed creation/attachment response before exposing it to the peer.
     * The caller must obtain it from the authorized SessionStore operation, not synthesize it.
     *
     * @param committed exact correlated storage response
     */
    void bind(Binding committed) {
      Objects.requireNonNull(committed);
      synchronized (lock) {
        requireLive();
        access.check();
        if (closed) throw error(NOT_READY, "connection closed");
        if (flight.kind != Kind.BINDING)
          throw error(CONFLICT, "request does not own session binding");
        if (committed.request() != ClientCorrelation.requestId(flight.request)
            || !committed.owner().equals(access.owner()))
          throw error(INTEGRITY_ERROR, "binding response identity differs");
        if (flight.request instanceof Create request
            && (committed.creationSequence() != request.creationSequence()
                || !committed.policy().equals(request.policy())))
          throw error(INTEGRITY_ERROR, "creation response differs from request");
        if (flight.request instanceof Attach request
            && (!committed.authority().equals(request.authority())
                || !committed.owner().equals(request.owner())
                || committed.generation() != request.generation()))
          throw error(INTEGRITY_ERROR, "attachment response differs from request");
        if (binding != null && !binding.equals(committed))
          throw error(CONFLICT, "connection cannot replace its session");
        binding = committed;
      }
    }

    /**
     * Wait for every other accepted request/transfer owner to finish after detach acceptance.
     * Transport must separately enforce detach lifetime, response writes and control FIN/ACK rules.
     *
     * @return read-only drain stage; not evidence of durable session closure
     */
    CompletionStage<Void> drained() {
      synchronized (lock) {
        requireLive();
        if (flight.kind != Kind.DETACH) throw error(CONFLICT, "request is not detach");
        return flight.drained.minimalCompletionStage();
      }
    }

    private void requireLive() {
      if (released) throw error(CONFLICT, "request owner already released");
    }

    /** Release this owner once; only the final physical owner releases connection capacity. */
    @Override
    public void close() {
      Flight ready;
      boolean abandonedDetach;
      synchronized (lock) {
        if (released) return;
        released = true;
        abandonedDetach = false;
        if (--flight.references == 0) {
          pending.remove(flight);
          switch (flight.kind) {
            case BINDING -> bindingPending = false;
            case INPUT -> inputs--;
            case OUTPUT -> outputs--;
            case COMPLETE -> completing = false;
            case DETACH -> abandonedDetach = true;
            default -> {}
          }
        }
        ready = readyDetach();
      }
      if (abandonedDetach)
        flight.drained.completeExceptionally(error(CANCELLED, "detach owner abandoned"));
      if (ready != null) ready.drained.complete(null);
    }
  }

  private final Object lock = new Object();
  private final SessionStore.Access access;
  private final Capabilities selected;
  private final LongSupplier nanoClock;
  private final Set<Flight> pending = new HashSet<>();
  private long highest;
  private Binding binding;
  private boolean bindingPending;
  private boolean completing;
  private boolean detached;
  private boolean closed;
  private int inputs;
  private int outputs;
  private Flight detach;

  /**
   * Start request ownership after authenticated durable-profile negotiation.
   *
   * @param access original verified owner and nonblocking current-credential gate
   * @param selected completed profile/limit selection
   */
  DurableRequests(SessionStore.Access access, Capabilities selected) {
    this(access, selected, System::nanoTime);
  }

  /**
   * Start with an explicit local monotonic source for deadline/capacity testing.
   *
   * @param access original verified owner and current nonblocking gate
   * @param selected completed durable capability selection
   * @param nanoClock nonblocking local elapsed-time source
   */
  DurableRequests(SessionStore.Access access, Capabilities selected, LongSupplier nanoClock) {
    this.access = Objects.requireNonNull(access);
    this.selected = Objects.requireNonNull(selected);
    this.nanoClock = Objects.requireNonNull(nanoClock);
    if (!selected.response() || !selected.supported().contains(DURABLE_WORK))
      throw error(EXTENSION_UNSUPPORTED, "durable selection required");
    access.check();
  }

  /**
   * Accept a fully decoded control synchronously in wire order. Framing/direction/correlation
   * errors throw; a well-formed refusal consumes its request ID and returns its correlation.
   *
   * @param message fully decoded request, not CAPABILITIES or a server response
   * @return immediate refusal or owned request for asynchronous dispatch
   */
  Acceptance accept(Message message) {
    Objects.requireNonNull(message);
    long id = ClientCorrelation.requestId(message);
    Wire.encode(message, selected.controlLimit());
    Flight ready = null;
    Acceptance result;
    synchronized (lock) {
      if (highest == 0 && id != 1 || id <= highest)
        throw ProtocolError.frame("control request does not increase from one");
      highest = id;
      try {
        access.check();
        if (closed || detached || completing) throw error(NOT_READY, "connection draining");
        if ((message instanceof Read || message instanceof GetManifest)
            && !selected.supported().contains(RESULT_DELIVERY))
          throw error(EXTENSION_UNSUPPORTED, "result profile not selected");
        Kind kind = kind(message);
        if (kind == Kind.BINDING && (bindingPending || binding != null))
          throw error(CONFLICT, "connection already binding or bound");
        if (!(message instanceof SessionMessage || message instanceof Detach) && binding == null)
          throw error(NOT_READY, "session not attached");
        if (kind == Kind.COMPLETE && !pending.isEmpty())
          throw error(NOT_READY, "connection has outstanding requests or transfers");
        if (pending.size() >= selected.pendingLimit()
            || kind == Kind.OUTPUT && outputs >= selected.streamLimit())
          throw ProtocolError.limit("connection request or result capacity exhausted");
        Flight flight = new Flight(message, kind);
        pending.add(flight);
        switch (kind) {
          case BINDING -> bindingPending = true;
          case OUTPUT -> outputs++;
          case COMPLETE -> completing = true;
          case DETACH -> {
            detached = true;
            detach = flight;
          }
          default -> {}
        }
        result = new Acceptance(null, new Ticket(flight));
        ready = readyDetach();
      } catch (ProtocolError failure) {
        result =
            new Acceptance(
                new Refusal(new Records.RequestTag(false, id), failure.code(), "request refused"),
                null);
      }
    }
    if (ready != null) ready.drained.complete(null);
    return result;
  }

  /**
   * Reserve an input before any header/payload I/O. The transport separately validates the actual
   * peer unidirectional stream ID and keeps this ticket through admission-response completion.
   *
   * @return input owner with the captured session binding
   */
  Ticket input() {
    synchronized (lock) {
      access.check();
      if (closed || detached || completing) throw error(NOT_READY, "connection draining");
      if (binding == null) throw error(NOT_READY, "session not attached");
      if (pending.size() >= selected.pendingLimit() || inputs >= selected.streamLimit())
        throw ProtocolError.limit("connection input or request capacity exhausted");
      Flight flight = new Flight(null, Kind.INPUT);
      pending.add(flight);
      inputs++;
      return new Ticket(flight);
    }
  }

  private static Kind kind(Message request) {
    if (request instanceof Create || request instanceof Attach) return Kind.BINDING;
    if (request instanceof Read) return Kind.OUTPUT;
    if (request instanceof Complete) return Kind.COMPLETE;
    if (request instanceof Detach) return Kind.DETACH;
    return Kind.ORDINARY;
  }

  private Flight readyDetach() {
    return !closed && detach != null && pending.size() == 1 && pending.contains(detach)
        ? detach
        : null;
  }

  /**
   * Read current connection-owned capacity, not durable job or whole-process resource statistics.
   *
   * @return current request/transfer charges
   */
  Usage usage() {
    synchronized (lock) {
      return new Usage(pending.size(), inputs, outputs, bindingPending, detached || completing);
    }
  }

  /** Stop acceptance without inventing completion or releasing still-running physical owners. */
  @Override
  public void close() {
    Flight waiting;
    synchronized (lock) {
      if (closed) return;
      closed = true;
      waiting = detach;
    }
    if (waiting != null)
      waiting.drained.completeExceptionally(error(CANCELLED, "connection closed before drain"));
  }

  private static ProtocolError error(ProtocolError.Code code, String detail) {
    return new ProtocolError(code, detail);
  }
}
