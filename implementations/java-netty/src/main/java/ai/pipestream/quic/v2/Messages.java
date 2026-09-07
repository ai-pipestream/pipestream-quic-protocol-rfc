package ai.pipestream.quic.v2;

import static ai.pipestream.quic.v2.Checks.*;
import static ai.pipestream.quic.v2.ProtocolError.*;
import static ai.pipestream.quic.v2.Records.*;

import java.util.List;
import java.util.TreeSet;

/** Typed control messages for all seven Section 12 control types. */
public final class Messages {
  private Messages() {}

  /** Private-use durable-work profile identifier; not an IANA assignment. */
  public static final int DURABLE_WORK = 65284;

  /** Private-use result-delivery profile identifier; requires durable work. */
  public static final int RESULT_DELIVERY = 65285;

  /** Wire message families are closed and have distinct request/response types. */
  public sealed interface Message
      permits Capabilities,
          SessionMessage,
          ScopeMessage,
          WorkMessage,
          ResultMessage,
          DrainMessage,
          Refusal {}

  /** Session control-family requests and responses. */
  public sealed interface SessionMessage extends Message
      permits Create, Binding, Attach, NextSequence, Sequence {}

  /** Scope control-family requests and responses. */
  public sealed interface ScopeMessage extends Message
      permits Declare,
          DeclarationResponse,
          Page,
          PageResponse,
          Checkpoint,
          CheckpointResponse,
          CancelScope,
          CancelScopeResponse {}

  /** Work control-family requests and responses. */
  public sealed interface WorkMessage extends Message
      permits AdmissionResponse,
          LookupOperation,
          OperationResponse,
          Watch,
          WatchResponse,
          Retry,
          RetryResponse,
          Cancel,
          CancelResponse,
          Skip,
          SkipResponse {}

  /** Result control-family requests and responses. */
  public sealed interface ResultMessage extends Message
      permits Read, GetManifest, ManifestResponse {}

  /** Drain control-family requests and responses. */
  public sealed interface DrainMessage extends Message
      permits Complete, Completed, Detach, Detached {}

  /**
   * Core capability offer or selection; constructing it does not activate any profile.
   *
   * @param response true for a server selection
   * @param supported strictly increasing supported or selected identifiers
   * @param required strictly increasing required identifiers
   * @param controlLimit maximum control body octets
   * @param streamLimit maximum concurrent incoming object streams
   * @param pendingLimit maximum unresolved requests
   * @param objectLimit maximum object payload octets
   * @param streamIdleMs maximum idle interval in milliseconds
   * @param streamLifetimeMs absolute stream lifetime in milliseconds
   */
  public record Capabilities(
      boolean response,
      List<Integer> supported,
      List<Integer> required,
      int controlLimit,
      int streamLimit,
      int pendingLimit,
      long objectLimit,
      long streamIdleMs,
      long streamLifetimeMs)
      implements Message {
    /** Validate the record's structural constraints. */
    public Capabilities {
      supported = extensions(supported);
      required = extensions(required);
      require(supported.containsAll(required), "required profile not supported");
      range(controlLimit, 4096, 1048576);
      range(streamLimit, 1, 1024);
      range(pendingLimit, 1, 1024);
      number(objectLimit);
      range(streamIdleMs, 1000, 300000);
      range(streamLifetimeMs, 1000, 86400000);
      require(streamIdleMs <= streamLifetimeMs, "idle deadline exceeds lifetime");
      dependencies(supported);
      if (response)
        for (int profile : supported)
          require(profile < 65281 || profile > 65283, "legacy profile selected on V2");
    }

    private static List<Integer> extensions(List<Integer> ids) {
      ids = list(ids, 32);
      int previous = 0;
      for (int value : ids) {
        range(value, 1, 65534);
        require(value > previous, "profile list not strictly increasing");
        previous = value;
      }
      return ids;
    }

    private static void dependencies(List<Integer> ids) {
      if (ids.contains(RESULT_DELIVERY) && !ids.contains(DURABLE_WORK))
        throw new ProtocolError(
            Code.EXTENSION_UNSUPPORTED, "result delivery requires durable work");
    }

    /**
     * Select only implemented, enabled profiles after authentication policy has run.
     *
     * @param offer client offer
     * @param local server offer with unauthorized profiles removed
     * @return selected intersection and required union
     */
    public static Capabilities negotiate(Capabilities offer, Capabilities local) {
      require(!offer.response && !local.response, "negotiation requires offers");
      List<Integer> selected =
          offer.supported.stream()
              .filter(local.supported::contains)
              .filter(p -> p == DURABLE_WORK || p == RESULT_DELIVERY)
              .toList();
      TreeSet<Integer> required = new TreeSet<>(offer.required);
      required.addAll(local.required);
      if (!selected.containsAll(required))
        throw new ProtocolError(Code.EXTENSION_UNSUPPORTED, "required profile unavailable");
      return new Capabilities(
          true,
          selected,
          List.copyOf(required),
          Math.min(offer.controlLimit, local.controlLimit),
          Math.min(offer.streamLimit, local.streamLimit),
          Math.min(offer.pendingLimit, local.pendingLimit),
          Math.min(offer.objectLimit, local.objectLimit),
          Math.min(offer.streamIdleMs, local.streamIdleMs),
          Math.min(offer.streamLifetimeMs, local.streamLifetimeMs));
    }

    /**
     * Validate a server selection against this client offer.
     *
     * @param selected server response; this instance must be the original offer
     */
    public void validateResponse(Capabilities selected) {
      require(
          !response
              && selected.response
              && supported.containsAll(selected.supported)
              && selected.required.containsAll(required)
              && selected.supported.containsAll(selected.required),
          "unsolicited or incomplete profile selection");
      require(
          selected.supported.stream().allMatch(p -> p == DURABLE_WORK || p == RESULT_DELIVERY),
          "unknown selected profile");
      require(
          selected.controlLimit <= controlLimit
              && selected.streamLimit <= streamLimit
              && selected.pendingLimit <= pendingLimit
              && selected.objectLimit <= objectLimit
              && selected.streamIdleMs <= streamIdleMs
              && selected.streamLifetimeMs <= streamLifetimeMs,
          "increased negotiated limit");
    }
  }

  /**
   * Request a new session using the owner's durable creation sequence.
   *
   * @param request connection-local request identity
   * @param creationSequence durable owner creation sequence
   * @param policy independent promised lifetimes
   */
  public record Create(long request, long creationSequence, Policy policy)
      implements SessionMessage {
    /** Validate the record's structural constraints. */
    public Create {
      id(request);
      id(creationSequence);
      present(policy);
    }
  }

  /**
   * Return the authenticated session identity and immutable accepted policy.
   *
   * @param request connection-local request identity
   * @param authority configured issuer identity
   * @param owner authenticated owner identity
   * @param generation immutable session generation
   * @param creationSequence durable owner creation sequence
   * @param policy independent promised lifetimes
   * @param limits accepted session capacity limits
   */
  public record Binding(
      long request,
      String authority,
      String owner,
      long generation,
      long creationSequence,
      Policy policy,
      Limits limits)
      implements SessionMessage {
    /** Validate the record's structural constraints. */
    public Binding {
      id(request);
      identity(authority);
      identity(owner);
      id(generation);
      id(creationSequence);
      present(policy);
      present(limits);
    }
  }

  /**
   * Request attachment to an existing owner-qualified session.
   *
   * @param request connection-local request identity
   * @param authority configured issuer identity
   * @param owner authenticated owner identity
   * @param generation immutable session generation
   */
  public record Attach(long request, String authority, String owner, long generation)
      implements SessionMessage {
    /** Validate the record's structural constraints. */
    public Attach {
      id(request);
      identity(authority);
      identity(owner);
      id(generation);
    }
  }

  /**
   * Request the next available owner creation sequence.
   *
   * @param request connection-local request identity
   */
  public record NextSequence(long request) implements SessionMessage {
    /** Validate the record's structural constraints. */
    public NextSequence {
      id(request);
    }
  }

  /**
   * Return the next creation sequence without creating a session.
   *
   * @param request connection-local request identity
   * @param nextCreationSequence next available owner creation sequence
   */
  public record Sequence(long request, long nextCreationSequence) implements SessionMessage {
    /** Validate the record's structural constraints. */
    public Sequence {
      id(request);
      id(nextCreationSequence);
    }
  }

  /**
   * Declare an ordered batch and optionally seal its scope.
   *
   * @param request connection-local request identity
   * @param operation stable mutation identity
   * @param scope scope ID
   * @param entityIds strictly increasing member IDs
   * @param seal whether this batch seals the scope
   */
  public record Declare(
      long request, OperationId operation, long scope, List<Long> entityIds, boolean seal)
      implements ScopeMessage {
    /** Validate the record's structural constraints. */
    public Declare {
      id(request);
      present(operation);
      number(scope);
      entityIds = list(entityIds, 256);
      require(seal || !entityIds.isEmpty(), "empty unsealed declaration");
      long previous = 0;
      for (long entity : entityIds) {
        id(entity);
        require(entity > previous, "declaration IDs not increasing");
        previous = entity;
      }
    }
  }

  /**
   * Return the committed declaration receipt.
   *
   * @param request connection-local request identity
   * @param receipt typed committed operation receipt
   */
  public record DeclarationResponse(long request, OperationReceipt receipt)
      implements ScopeMessage {
    /** Validate the record's structural constraints. */
    public DeclarationResponse {
      id(request);
      present(receipt);
      require(receipt.outcome() instanceof Declared, "wrong declaration outcome");
    }
  }

  /**
   * Request a bounded page of declared members.
   *
   * @param request connection-local request identity
   * @param scope scope ID
   * @param afterEntity exclusive lower entity ID
   * @param limit maximum number of entries
   */
  public record Page(long request, long scope, long afterEntity, int limit)
      implements ScopeMessage {
    /** Validate the record's structural constraints. */
    public Page {
      id(request);
      number(scope);
      number(afterEntity);
      range(limit, 1, 256);
    }
  }

  /**
   * One declared member and its observed work-state hint.
   *
   * @param entity positive member ID
   * @param state state at the observation or commit
   */
  public record Entry(long entity, State state) {
    /** Validate the record's structural constraints. */
    public Entry {
      id(entity);
      present(state);
    }
  }

  /**
   * Return a scope snapshot; an empty page alone is not completeness evidence.
   *
   * @param request connection-local request identity
   * @param scope scope ID
   * @param producer scope producer
   * @param parent parent work key, null only for root
   * @param sealed whether membership is final
   * @param seal membership seal, or null before sealing
   * @param declared total declared membership count
   * @param entries bounded ordered member entries
   * @param more whether later IDs exist in this snapshot
   */
  public record PageResponse(
      long request,
      long scope,
      int producer,
      WorkKey parent,
      boolean sealed,
      Digest seal,
      long declared,
      List<Entry> entries,
      boolean more)
      implements ScopeMessage {
    /** Validate the record's structural constraints. */
    public PageResponse {
      id(request);
      Checks.scope(scope, producer, parent);
      require(sealed == (seal != null), "seal presence disagrees with sealed flag");
      number(declared);
      entries = list(entries, 256);
      require(entries.size() <= declared, "page exceeds membership count");
      long previous = 0;
      for (Entry entry : entries) {
        require(entry.entity > previous, "page IDs not increasing");
        previous = entry.entity;
      }
      require(
          !more || !entries.isEmpty() && entries.size() < declared, "impossible page continuation");
    }
  }

  /**
   * Wait for a scope to close under an exact expected membership seal.
   *
   * @param request connection-local request identity
   * @param scope scope ID
   * @param seal membership seal, or null before sealing
   * @param waitMs bounded connection-local wait in milliseconds
   */
  public record Checkpoint(long request, long scope, Digest seal, long waitMs)
      implements ScopeMessage {
    /** Validate the record's structural constraints. */
    public Checkpoint {
      id(request);
      number(scope);
      present(seal);
      range(waitMs, 0, 30000);
    }
  }

  /**
   * Return the immutable scope closure summary.
   *
   * @param request connection-local request identity
   * @param summary immutable closure summary
   */
  public record CheckpointResponse(long request, ScopeSummary summary) implements ScopeMessage {
    /** Validate the record's structural constraints. */
    public CheckpointResponse {
      id(request);
      present(summary);
    }
  }

  /**
   * Request a cancellation fence over the target scope.
   *
   * @param request connection-local request identity
   * @param operation stable mutation identity
   * @param scope scope ID
   */
  public record CancelScope(long request, OperationId operation, long scope)
      implements ScopeMessage {
    /** Validate the record's structural constraints. */
    public CancelScope {
      id(request);
      present(operation);
      number(scope);
    }
  }

  /**
   * Return the committed scope cancellation receipt.
   *
   * @param request connection-local request identity
   * @param receipt typed committed operation receipt
   */
  public record CancelScopeResponse(long request, OperationReceipt receipt)
      implements ScopeMessage {
    /** Validate the record's structural constraints. */
    public CancelScopeResponse {
      id(request);
      present(receipt);
      require(receipt.outcome() instanceof ScopeCancelled, "wrong scope cancellation outcome");
    }
  }

  /**
   * Correlate a durable admission receipt with its actual input stream.
   *
   * @param request actual QUIC input-stream correlation tag
   * @param receipt typed committed operation receipt
   */
  public record AdmissionResponse(RequestTag request, OperationReceipt receipt)
      implements WorkMessage {
    /** Validate the record's structural constraints. */
    public AdmissionResponse {
      present(request);
      present(receipt);
      require(
          request.input() && receipt.outcome() instanceof Admitted,
          "wrong input admission response");
    }
  }

  /**
   * Look up an operation in the authenticated originator namespace.
   *
   * @param request connection-local request identity
   * @param operation stable mutation identity
   */
  public record LookupOperation(long request, OperationId operation) implements WorkMessage {
    /** Validate the record's structural constraints. */
    public LookupOperation {
      id(request);
      present(operation);
    }
  }

  /**
   * Return the retained receipt; absence requires a named refusal.
   *
   * @param request connection-local request identity
   * @param receipt typed committed operation receipt
   */
  public record OperationResponse(long request, OperationReceipt receipt) implements WorkMessage {
    /** Validate the record's structural constraints. */
    public OperationResponse {
      id(request);
      present(receipt);
    }
  }

  /**
   * Read a work revision or wait for it to change.
   *
   * @param request connection-local request identity
   * @param work logical work identity
   * @param afterRevision last observed revision, or zero for an immediate read
   * @param waitMs bounded connection-local wait in milliseconds
   */
  public record Watch(long request, WorkKey work, long afterRevision, long waitMs)
      implements WorkMessage {
    /** Validate the record's structural constraints. */
    public Watch {
      id(request);
      present(work);
      number(afterRevision);
      range(waitMs, 0, 30000);
    }
  }

  /**
   * Return the current durable revision and consistent work view.
   *
   * @param request connection-local request identity
   * @param revision current positive durable revision
   * @param work consistent durable work snapshot
   */
  public record WatchResponse(long request, long revision, WorkView work) implements WorkMessage {
    /** Validate the record's structural constraints. */
    public WatchResponse {
      id(request);
      id(revision);
      present(work);
    }
  }

  /**
   * Authorize exactly one replacement attempt using the expected current attempt.
   *
   * @param request connection-local request identity
   * @param operation stable mutation identity
   * @param work logical work identity
   * @param expectedAttempt exact attempt being replaced
   */
  public record Retry(long request, OperationId operation, WorkKey work, long expectedAttempt)
      implements WorkMessage {
    /** Validate the record's structural constraints. */
    public Retry {
      id(request);
      present(operation);
      present(work);
      id(expectedAttempt);
    }
  }

  /**
   * Return the committed attempt-replacement receipt.
   *
   * @param request connection-local request identity
   * @param receipt typed committed operation receipt
   */
  public record RetryResponse(long request, OperationReceipt receipt) implements WorkMessage {
    /** Validate the record's structural constraints. */
    public RetryResponse {
      id(request);
      present(receipt);
      require(receipt.outcome() instanceof Retried, "wrong retry outcome");
    }
  }

  /**
   * Request a work cancellation fence without rewriting a terminal outcome.
   *
   * @param request connection-local request identity
   * @param operation stable mutation identity
   * @param work logical work identity
   */
  public record Cancel(long request, OperationId operation, WorkKey work) implements WorkMessage {
    /** Validate the record's structural constraints. */
    public Cancel {
      id(request);
      present(operation);
      present(work);
    }
  }

  /**
   * Return the committed cancellation disposition.
   *
   * @param request connection-local request identity
   * @param receipt typed committed operation receipt
   */
  public record CancelResponse(long request, OperationReceipt receipt) implements WorkMessage {
    /** Validate the record's structural constraints. */
    public CancelResponse {
      id(request);
      present(receipt);
      require(receipt.outcome() instanceof Cancelled, "wrong cancellation outcome");
    }
  }

  /**
   * Request a skip fence over declared or admitted work.
   *
   * @param request connection-local request identity
   * @param operation stable mutation identity
   * @param work logical work identity
   */
  public record Skip(long request, OperationId operation, WorkKey work) implements WorkMessage {
    /** Validate the record's structural constraints. */
    public Skip {
      id(request);
      present(operation);
      present(work);
    }
  }

  /**
   * Return the committed skip disposition.
   *
   * @param request connection-local request identity
   * @param receipt typed committed operation receipt
   */
  public record SkipResponse(long request, OperationReceipt receipt) implements WorkMessage {
    /** Validate the record's structural constraints. */
    public SkipResponse {
      id(request);
      present(receipt);
      require(receipt.outcome() instanceof Skipped, "wrong skip outcome");
    }
  }

  /**
   * Request an authenticated object transfer bound to its expected digest.
   *
   * @param request connection-local request identity
   * @param work logical work identity
   * @param attempt producing execution attempt
   * @param index zero-based output index
   * @param expectedSha256 expected object commitment
   */
  public record Read(long request, WorkKey work, long attempt, int index, Digest expectedSha256)
      implements ResultMessage {
    /** Validate the record's structural constraints. */
    public Read {
      id(request);
      present(work);
      id(attempt);
      range(index, 0, 255);
      present(expectedSha256);
    }
  }

  /**
   * Request the manifest for an exact producing attempt.
   *
   * @param request connection-local request identity
   * @param work logical work identity
   * @param attempt producing execution attempt
   */
  public record GetManifest(long request, WorkKey work, long attempt) implements ResultMessage {
    /** Validate the record's structural constraints. */
    public GetManifest {
      id(request);
      present(work);
      id(attempt);
    }
  }

  /**
   * Return an immutable publication manifest, not a transferable credential.
   *
   * @param request connection-local request identity
   * @param manifest immutable result publication
   */
  public record ManifestResponse(long request, Manifest manifest) implements ResultMessage {
    /** Validate the record's structural constraints. */
    public ManifestResponse {
      id(request);
      present(manifest);
    }
  }

  /**
   * Request completed-session drain under the exact root closure summary.
   *
   * @param request connection-local request identity
   * @param generation immutable session generation
   * @param root exact root scope closure summary
   */
  public record Complete(long request, long generation, ScopeSummary root) implements DrainMessage {
    /** Validate the record's structural constraints. */
    public Complete {
      id(request);
      id(generation);
      present(root);
      require(root.scope() == 0, "completed drain needs root summary");
    }
  }

  /**
   * Acknowledge the root cut and connection drain without expiring outputs.
   *
   * @param request connection-local request identity
   * @param generation immutable session generation
   * @param root exact root scope closure summary
   */
  public record Completed(long request, long generation, ScopeSummary root)
      implements DrainMessage {
    /** Validate the record's structural constraints. */
    public Completed {
      id(request);
      id(generation);
      present(root);
      require(root.scope() == 0, "completed drain needs root summary");
    }
  }

  /**
   * Request connection-only drain without asserting work completion.
   *
   * @param request connection-local request identity
   */
  public record Detach(long request) implements DrainMessage {
    /** Validate the record's structural constraints. */
    public Detach {
      id(request);
    }
  }

  /**
   * Acknowledge connection-only drain without cancelling durable work.
   *
   * @param request connection-local request identity
   */
  public record Detached(long request) implements DrainMessage {
    /** Validate the record's structural constraints. */
    public Detached {
      id(request);
    }
  }

  /**
   * A correlated named failure, not a successful durable mutation.
   *
   * @param request connection-local request identity
   * @param code named protocol error
   * @param detail non-authoritative bounded UTF-8 diagnostic
   */
  public record Refusal(RequestTag request, Code code, String detail) implements Message {
    /** Validate the record's structural constraints. */
    public Refusal {
      present(request);
      present(code);
      Cbor.utf8(detail, 512);
    }
  }

  /**
   * Get the control-family type.
   *
   * @param message typed control
   * @return control type octet
   */
  public static int type(Message message) {
    return switch (message) {
      case Capabilities ignored -> 1;
      case SessionMessage ignored -> 2;
      case ScopeMessage ignored -> 3;
      case WorkMessage ignored -> 4;
      case ResultMessage ignored -> 5;
      case DrainMessage ignored -> 6;
      case Refusal ignored -> 7;
    };
  }

  /**
   * Determine the permitted message direction.
   *
   * @param message typed control
   * @return true for a server-originated response
   */
  public static boolean response(Message message) {
    return switch (message) {
      case Capabilities c -> c.response;
      case Binding ignored -> true;
      case Sequence ignored -> true;
      case DeclarationResponse ignored -> true;
      case PageResponse ignored -> true;
      case CheckpointResponse ignored -> true;
      case CancelScopeResponse ignored -> true;
      case AdmissionResponse ignored -> true;
      case OperationResponse ignored -> true;
      case WatchResponse ignored -> true;
      case RetryResponse ignored -> true;
      case CancelResponse ignored -> true;
      case SkipResponse ignored -> true;
      case ManifestResponse ignored -> true;
      case Completed ignored -> true;
      case Detached ignored -> true;
      case Refusal ignored -> true;
      default -> false;
    };
  }
}
