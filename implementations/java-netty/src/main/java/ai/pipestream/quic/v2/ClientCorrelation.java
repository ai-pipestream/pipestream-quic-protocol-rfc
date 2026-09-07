package ai.pipestream.quic.v2;

import static ai.pipestream.quic.v2.Checks.*;
import static ai.pipestream.quic.v2.Messages.*;
import static ai.pipestream.quic.v2.ProtocolError.*;
import static ai.pipestream.quic.v2.Records.*;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Caller-confined V2 client correlation and delivery verification. This does not authenticate a
 * peer, persist intent, validate durable receipts or acknowledge coverage. A completed control
 * response must still pass the durable client's identity/commitment checks before success is
 * reported.
 */
public final class ClientCorrelation {
  /**
   * An immutable original request retained until its response or connection loss.
   *
   * @param tag actual control or input-stream identity
   * @param control original control request, null for an input
   * @param input original input header, null for a control
   */
  public record Pending(RequestTag tag, Message control, InputHeader input) {
    /** Validate the two disjoint request namespaces. */
    public Pending {
      present(tag);
      require(
          tag.input() ? input != null && control == null : control != null && input == null,
          "inconsistent pending request namespace");
    }
  }

  /**
   * A correlated control response, not yet durable evidence.
   *
   * @param request original immutable request
   * @param response correctly correlated response or refusal
   */
  public record Completion(Pending request, Message response) {
    /** Require the original request and actual response. */
    public Completion {
      present(request);
      present(response);
    }
  }

  /**
   * The selected object from an authenticated retained manifest, without retaining all its
   * siblings.
   *
   * @param generation session generation
   * @param work logical work identity
   * @param attempt producing attempt
   * @param index output index
   * @param length exact full-object length
   * @param sha256 expected full-object digest
   */
  public record ResultCommitment(
      long generation, WorkKey work, long attempt, int index, long length, Digest sha256) {
    /** Validate immutable object identity and geometry. */
    public ResultCommitment {
      id(generation);
      present(work);
      id(attempt);
      range(index, 0, 255);
      number(length);
      present(sha256);
    }

    /**
     * Select a descriptor without implying fresh read authorization.
     *
     * @param manifest independently authenticated and retained manifest
     * @param index existing object index
     * @return exact immutable object commitment
     */
    public static ResultCommitment from(Manifest manifest, int index) {
      present(manifest);
      range(index, 0, manifest.outputs().size() - 1L);
      Output output = manifest.outputs().get(index);
      return new ResultCommitment(
          manifest.generation(),
          manifest.work(),
          manifest.attempt(),
          index,
          output.length(),
          output.sha256());
    }

    private boolean matches(Read request) {
      return work.equals(request.work())
          && attempt == request.attempt()
          && index == request.index()
          && sha256.equals(request.expectedSha256());
    }

    private boolean matches(ResultHeader header) {
      return generation == header.generation()
          && work.equals(header.work())
          && attempt == header.attempt()
          && index == header.index()
          && length == header.length()
          && sha256.equals(header.sha256());
    }
  }

  private static final class Flight {
    private final Pending request;
    private final ResultCommitment result;
    private boolean started;
    private boolean streamReserved;
    private ObjectStream.Payload payload;

    private Flight(Pending request, ResultCommitment result) {
      this.request = request;
      this.result = result;
    }
  }

  private final Capabilities offer;
  private Capabilities selected;
  private final Map<RequestTag, Flight> pending = new HashMap<>();
  private long lastRequest;
  private int inputCount;
  private int resultStreams;
  private boolean failed;

  /**
   * Start connection-local state with the original offer; this does not send it.
   *
   * @param offer exact client capabilities to be sent on control Stream 0
   */
  public ClientCorrelation(Capabilities offer) {
    present(offer);
    require(!offer.response(), "client correlation requires an offer");
    this.offer = offer;
  }

  private void live() {
    require(!failed, "connection correlation is unusable");
  }

  private void negotiated() {
    live();
    require(selected != null, "only capabilities allowed before negotiation");
  }

  private void profile(int profile) {
    if (!selected.supported().contains(profile))
      throw new ProtocolError(Code.EXTENSION_UNSUPPORTED, "message requires an inactive profile");
  }

  private void room() {
    if (pending.size() >= selected.pendingLimit())
      throw limit("pending request capacity exhausted");
  }

  /**
   * Register an immutable control before transmission. Resource refusal leaves its number reusable.
   *
   * @param request client request; IDs start at one and strictly increase across all types
   * @param result exact retained object for a Read, null for every other request
   * @return encoded frame checked against the negotiated control ceiling, ready for transmission
   */
  public byte[] register(Message request, ResultCommitment result) {
    negotiated();
    present(request);
    long id = requestId(request);
    require(
        lastRequest == 0 ? id == 1 : id > lastRequest,
        "control request ID repeated or not increasing");
    if (!(request instanceof Detach)) profile(DURABLE_WORK);
    if (request instanceof ResultMessage) profile(RESULT_DELIVERY);
    if (request instanceof Read read) {
      require(
          result != null && result.matches(read),
          "result read lacks its exact retained commitment");
      if (result.length() > selected.objectLimit())
        throw limit("selected result exceeds object limit");
    } else require(result == null, "non-result request carries object commitment");
    room();
    byte[] frame = Wire.encode(request, selected.controlLimit());
    RequestTag tag = new RequestTag(false, id);
    pending.put(tag, new Flight(new Pending(tag, request, null), result));
    lastRequest = id;
    return frame;
  }

  /**
   * Register an input using its actual newly opened QUIC stream; no application history is
   * invented. The transport owns QUIC stream non-reuse. STOP_SENDING alone must not remove this
   * pending receipt.
   *
   * @param streamId actual caller-initiated unidirectional stream ID
   * @param header immutable original input header
   */
  public void registerInput(long streamId, InputHeader header) {
    negotiated();
    profile(DURABLE_WORK);
    present(header);
    RequestTag tag = new RequestTag(true, streamId);
    require(
        (streamId & 3) == 2 && !pending.containsKey(tag),
        "invalid or duplicate input stream identity");
    require(header.parameters().work().producer() == 0, "external input uses authority producer");
    header.parameters().validateProfiles(selected.supported().contains(RESULT_DELIVERY));
    if (header.parameters().input().length() > selected.objectLimit()
        || inputCount >= selected.streamLimit())
      throw limit("input object or stream ceiling exceeded");
    room();
    pending.put(tag, new Flight(new Pending(tag, null, header), null));
    inputCount++;
  }

  /**
   * Accept one parsed server control. Fatal framing/correlation errors poison this connection
   * state.
   *
   * @param frame one known response or bounded ignored frame from the wire decoder
   * @return original request and response, or null for capabilities/ignored frames
   */
  public Completion receive(Wire.Frame frame) {
    live();
    present(frame);
    try {
      if (frame instanceof Wire.Ignored) {
        negotiated();
        return null;
      }
      Message response = ((Wire.Known) frame).message();
      if (response instanceof Capabilities capabilities) {
        require(selected == null, "repeated capability exchange");
        offer.validateResponse(capabilities);
        selected = capabilities;
        return null;
      }
      negotiated();
      require(Messages.response(response), "server sent a client request");
      RequestTag tag = responseTag(response);
      Flight flight = pending.get(tag);
      require(
          flight != null && !flight.started, "unsolicited, duplicate or already-started response");
      if (!(response instanceof Refusal)) {
        if (tag.input())
          require(response instanceof AdmissionResponse, "input received wrong response kind");
        else
          require(
              matches(flight.request.control(), response),
              "control response kind does not match request");
      }
      pending.remove(tag);
      if (tag.input()) inputCount--;
      return new Completion(flight.request, response);
    } catch (ProtocolError error) {
      failed = true;
      throw error;
    }
  }

  /**
   * Start the one response stream for a pending result read. Commitment mismatch is delivery-local;
   * its slot remains occupied until abortResult. Unknown/wrong-kind/duplicate correlation is fatal.
   *
   * @param header structurally valid header from an actual server unidirectional stream
   * @param nowNanos monotonic header acceptance time
   */
  public void beginResult(ResultHeader header, long nowNanos) {
    live();
    present(header);
    Flight flight;
    try {
      negotiated();
      profile(RESULT_DELIVERY);
      flight = pending.get(new RequestTag(false, header.request()));
      require(
          flight != null && flight.result != null && !flight.started,
          "unsolicited or duplicate result response");
    } catch (ProtocolError error) {
      failed = true;
      throw error;
    }
    flight.started = true;
    if (!flight.result.matches(header))
      throw new ProtocolError(Code.INTEGRITY_ERROR, "result header contradicts retained object");
    if (resultStreams >= selected.streamLimit())
      throw limit("active result stream ceiling exceeded");
    flight.streamReserved = true;
    resultStreams++;
    flight.payload = new ObjectStream.Payload(header.length(), header.sha256(), selected, nowNanos);
  }

  private Flight result(long request) {
    negotiated();
    Flight flight = pending.get(new RequestTag(false, request));
    require(
        flight != null && flight.started && flight.payload != null,
        "no accepted result header for payload");
    return flight;
  }

  /**
   * Verify reversible payload consumption for the stream already bound to this request.
   *
   * @param request bound result request number, not a new claim from payload bytes
   * @param bytes actual payload bytes
   * @param nowNanos monotonic receive-progress time
   */
  public void resultBytes(long request, ByteBuffer bytes, long nowNanos) {
    result(request).payload.feed(bytes, nowNanos);
  }

  /**
   * Drive an active result deadline independently of read callbacks.
   *
   * @param request bound result request number
   * @param nowNanos current monotonic time
   */
  public void checkResultDeadline(long request, long nowNanos) {
    result(request).payload.checkDeadline(nowNanos);
  }

  /**
   * Validate actual FIN and complete only that delivery, including when FIN reveals bad bytes.
   *
   * @param request bound result request number
   * @param nowNanos monotonic FIN observation time
   * @return original request after complete payload verification
   */
  public Pending finishResult(long request, long nowNanos) {
    Flight flight = result(request);
    try {
      flight.payload.finish(nowNanos);
      return flight.request;
    } finally {
      releaseResult(new RequestTag(false, request), flight);
    }
  }

  /**
   * Release a started delivery after its transport abort. This never cancels or retries work.
   *
   * @param request the request bound by the received header, including a mismatching header
   * @return original request for delivery-failure reporting
   */
  public Pending abortResult(long request) {
    negotiated();
    RequestTag tag = new RequestTag(false, request);
    Flight flight = pending.get(tag);
    require(flight != null && flight.started, "no started result to abort");
    if (flight.payload != null) flight.payload.abort();
    releaseResult(tag, flight);
    return flight.request;
  }

  private void releaseResult(RequestTag tag, Flight flight) {
    if (flight.streamReserved) resultStreams--;
    pending.remove(tag);
  }

  /**
   * Close correlation and return unresolved requests for uncertainty recovery, never success.
   *
   * @return bounded immutable inventory; repeated close returns an empty inventory
   */
  public List<Pending> close() {
    failed = true;
    List<Pending> unresolved = pending.values().stream().map(f -> f.request).toList();
    for (Flight flight : pending.values()) if (flight.payload != null) flight.payload.abort();
    pending.clear();
    inputCount = 0;
    resultStreams = 0;
    return unresolved;
  }

  /**
   * Inspect bounded outstanding state, including responses whose streams have started.
   *
   * @return unresolved control and input request count
   */
  public int pendingCount() {
    return pending.size();
  }

  private static long requestId(Message message) {
    return switch (message) {
      case Create r -> r.request();
      case Attach r -> r.request();
      case NextSequence r -> r.request();
      case Declare r -> r.request();
      case Page r -> r.request();
      case Checkpoint r -> r.request();
      case CancelScope r -> r.request();
      case LookupOperation r -> r.request();
      case Watch r -> r.request();
      case Retry r -> r.request();
      case Cancel r -> r.request();
      case Skip r -> r.request();
      case Read r -> r.request();
      case GetManifest r -> r.request();
      case Complete r -> r.request();
      case Detach r -> r.request();
      default -> throw frame("not a client control request");
    };
  }

  private static RequestTag responseTag(Message response) {
    if (response instanceof Refusal r) return r.request();
    if (response instanceof AdmissionResponse r) return r.request();
    long id =
        switch (response) {
          case Binding r -> r.request();
          case Sequence r -> r.request();
          case DeclarationResponse r -> r.request();
          case PageResponse r -> r.request();
          case CheckpointResponse r -> r.request();
          case CancelScopeResponse r -> r.request();
          case OperationResponse r -> r.request();
          case WatchResponse r -> r.request();
          case RetryResponse r -> r.request();
          case CancelResponse r -> r.request();
          case SkipResponse r -> r.request();
          case ManifestResponse r -> r.request();
          case Completed r -> r.request();
          case Detached r -> r.request();
          default -> throw frame("not a server control response");
        };
    return new RequestTag(false, id);
  }

  private static boolean matches(Message request, Message response) {
    return switch (request) {
      case Create ignored -> response instanceof Binding;
      case Attach ignored -> response instanceof Binding;
      case NextSequence ignored -> response instanceof Sequence;
      case Declare ignored -> response instanceof DeclarationResponse;
      case Page ignored -> response instanceof PageResponse;
      case Checkpoint ignored -> response instanceof CheckpointResponse;
      case CancelScope ignored -> response instanceof CancelScopeResponse;
      case LookupOperation ignored -> response instanceof OperationResponse;
      case Watch ignored -> response instanceof WatchResponse;
      case Retry ignored -> response instanceof RetryResponse;
      case Cancel ignored -> response instanceof CancelResponse;
      case Skip ignored -> response instanceof SkipResponse;
      case GetManifest ignored -> response instanceof ManifestResponse;
      case Complete ignored -> response instanceof Completed;
      case Detach ignored -> response instanceof Detached;
      default -> false;
    };
  }
}
