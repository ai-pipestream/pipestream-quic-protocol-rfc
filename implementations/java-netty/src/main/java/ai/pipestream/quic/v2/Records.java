package ai.pipestream.quic.v2;

import static ai.pipestream.quic.v2.Checks.*;
import static ai.pipestream.quic.v2.ProtocolError.*;

import java.util.Arrays;
import java.util.List;

/** Immutable Section 12/Appendix F records. Constructors reject structural contradictions. */
public final class Records {
  private Records() {}

  /** Explicitly typed record values; no untyped CBOR maps are exposed. */
  public sealed interface Value
      permits WorkKey,
          RequestTag,
          Policy,
          Limits,
          Input,
          OutputBudget,
          Diagnostic,
          ChildScope,
          Counts,
          ScopeSummary,
          AdmitParameters,
          InputHeader,
          ResultHeader,
          Output,
          Manifest,
          WorkView,
          OperationReceipt,
          Outcome {}

  /**
   * SHA-256 octets with value equality and defensive ownership.
   *
   * @param bytes exactly 32 octets
   */
  public record Digest(byte[] bytes) {
    /** Validate the record's structural constraints. */
    public Digest {
      require(bytes != null && bytes.length == 32, "digest must have 32 bytes");
      bytes = bytes.clone();
    }

    /**
     * Return an owned copy of the immutable octets.
     *
     * @return defensive byte array
     */
    @Override
    public byte[] bytes() {
      return bytes.clone();
    }

    @Override
    public boolean equals(Object other) {
      return other instanceof Digest d && Arrays.equals(bytes, d.bytes);
    }

    @Override
    public int hashCode() {
      return Arrays.hashCode(bytes);
    }

    @Override
    public String toString() {
      return java.util.HexFormat.of().formatHex(bytes);
    }
  }

  /**
   * Immutable mutation/export identity.
   *
   * @param bytes exactly 16 octets, not all zero
   */
  public record OperationId(byte[] bytes) {
    /** Validate the record's structural constraints. */
    public OperationId {
      require(bytes != null && bytes.length == 16, "operation ID must have 16 bytes");
      int any = 0;
      for (byte b : bytes) any |= b;
      require(any != 0, "zero operation identity");
      bytes = bytes.clone();
    }

    /**
     * Return an owned copy of the immutable octets.
     *
     * @return defensive byte array
     */
    @Override
    public byte[] bytes() {
      return bytes.clone();
    }

    @Override
    public boolean equals(Object other) {
      return other instanceof OperationId id && Arrays.equals(bytes, id.bytes);
    }

    @Override
    public int hashCode() {
      return Arrays.hashCode(bytes);
    }

    @Override
    public String toString() {
      return java.util.HexFormat.of().formatHex(bytes);
    }
  }

  /**
   * Stable logical work within an authenticated session.
   *
   * @param scope scope ID
   * @param producer 0 caller or 1 authority
   * @param entity positive member ID
   */
  public record WorkKey(long scope, int producer, long entity) implements Value {
    /** Validate the record's structural constraints. */
    public WorkKey {
      number(scope);
      Checks.producer(producer);
      id(entity);
    }
  }

  /**
   * Correlation namespace, independent of operation identity.
   *
   * @param input true for actual QUIC input stream ID
   * @param id request or stream number
   */
  public record RequestTag(boolean input, long id) implements Value {
    /** Validate the record's structural constraints. */
    public RequestTag {
      range(id, input ? 0 : 1, input ? 4611686018427387903L : Long.MAX_VALUE);
    }
  }

  /**
   * Independent promised lifetimes, in milliseconds.
   *
   * @param executionLimit maximum execution duration
   * @param outputRetention result lifetime
   * @param receiptRetention terminal receipt lifetime
   */
  public record Policy(long executionLimit, long outputRetention, long receiptRetention)
      implements Value {
    /** Validate the record's structural constraints. */
    public Policy {
      duration(executionLimit);
      duration(outputRetention);
      duration(receiptRetention);
    }
  }

  /**
   * Immutable session admission ceilings.
   *
   * @param scopes scope count
   * @param entities member count
   * @param operations operation count
   * @param inputBytes retained input bytes
   * @param outputBytes retained output bytes
   * @param activeJobs executor slots
   */
  public record Limits(
      long scopes,
      long entities,
      long operations,
      long inputBytes,
      long outputBytes,
      long activeJobs)
      implements Value {
    /** Validate the record's structural constraints. */
    public Limits {
      id(scopes);
      id(entities);
      id(operations);
      number(inputBytes);
      number(outputBytes);
      id(activeJobs);
    }
  }

  /**
   * Input content commitment.
   *
   * @param length payload length
   * @param sha256 digest
   * @param contentType printable ASCII label
   */
  public record Input(long length, Digest sha256, String contentType) implements Value {
    /** Validate the record's structural constraints. */
    public Input {
      number(length);
      present(sha256);
      label(contentType);
    }
  }

  /**
   * Reserved output capacity.
   *
   * @param count object count
   * @param totalBytes aggregate payload bytes
   */
  public record OutputBudget(int count, long totalBytes) implements Value {
    /** Validate the record's structural constraints. */
    public OutputBudget {
      range(count, 0, 256);
      number(totalBytes);
      require(count != 0 || totalBytes == 0, "zero outputs have nonzero byte budget");
    }
  }

  /** Known work states; only the final four are terminal. */
  public enum State {
    /** Membership exists; input has not been admitted. */
    DECLARED,
    /** Admitted work is eligible for execution. */
    ACTIVE,
    /** The current attempt failed and requires explicit retry authorization. */
    AWAITING_RETRY,
    /** An admitted branch awaits descendant closure. */
    WAITING_CHILDREN,
    /** An accepted cancellation or skip fence awaits settlement. */
    CANCELLING,
    /** Terminal success, with a manifest when result delivery is selected. */
    SUCCEEDED,
    /** Terminal failure; this work identity cannot be retried. */
    FAILED,
    /** Terminal cancellation. */
    CANCELLED,
    /** Terminal skip. */
    SKIPPED;

    /**
     * Get the state's wire representation.
     *
     * @return Appendix F integer state
     */
    public int value() {
      return ordinal();
    }

    /**
     * Check whether the state is terminal.
     *
     * @return whether this state is an immutable logical-work outcome
     */
    public boolean terminal() {
      return ordinal() >= SUCCEEDED.ordinal();
    }

    /**
     * Decode a defined work state.
     *
     * @param value wire state
     * @return defined state
     */
    public static State from(long value) {
      range(value, 0, 8);
      return values()[(int) value];
    }
  }

  /**
   * Non-authoritative detail attached to a view.
   *
   * @param code application code
   * @param detail at most 512 UTF-8 octets
   */
  public record Diagnostic(long code, String detail) implements Value {
    /** Validate the record's structural constraints. */
    public Diagnostic {
      range(code, 0, 4294967295L);
      Cbor.utf8(detail, 512);
    }
  }

  /**
   * Immutable child allocation.
   *
   * @param scope positive scope ID
   * @param producer child producer
   */
  public record ChildScope(long scope, int producer) implements Value {
    /** Validate the record's structural constraints. */
    public ChildScope {
      id(scope);
      Checks.producer(producer);
    }
  }

  /**
   * Disjoint terminal counts.
   *
   * @param success successes
   * @param failure failures
   * @param cancelled cancellations
   * @param skipped skips
   */
  public record Counts(long success, long failure, long cancelled, long skipped) implements Value {
    /** Validate the record's structural constraints. */
    public Counts {
      sum(sum(success, failure), sum(cancelled, skipped));
    }

    /**
     * Get the checked terminal-count total.
     *
     * @return checked sum in the protocol integer range
     */
    public long total() {
      return sum(sum(success, failure), sum(cancelled, skipped));
    }
  }

  /**
   * Immutable closure commitment.
   *
   * @param scope scope ID
   * @param producer scope producer
   * @param parent null only for root
   * @param seal membership seal
   * @param declared membership count
   * @param counts terminal partition
   * @param statusRoot status commitment
   * @param closedAt UTC milliseconds
   */
  public record ScopeSummary(
      long scope,
      int producer,
      WorkKey parent,
      Digest seal,
      long declared,
      Counts counts,
      Digest statusRoot,
      long closedAt)
      implements Value {
    /** Validate the record's structural constraints. */
    public ScopeSummary {
      Checks.scope(scope, producer, parent);
      present(seal);
      number(declared);
      present(counts);
      present(statusRoot);
      number(closedAt);
      require(counts.total() == declared, "closure counts differ from declared membership");
    }
  }

  /**
   * Immutable admission request parameters.
   *
   * @param work logical work
   * @param input input commitment
   * @param application configured contract
   * @param mode leaf/caller-expanded/authority-expanded
   * @param executionMs duration
   * @param outputs reserved maximum outputs
   */
  public record AdmitParameters(
      WorkKey work,
      Input input,
      String application,
      int mode,
      long executionMs,
      OutputBudget outputs)
      implements Value {
    /** Validate the record's structural constraints. */
    public AdmitParameters {
      present(work);
      present(input);
      label(application);
      range(mode, 0, 2);
      duration(executionMs);
      present(outputs);
    }

    /**
     * Validate admission against the retained profiles.
     *
     * @param results whether result delivery is activated
     */
    public void validateProfiles(boolean results) {
      require(results || outputs.count == 0, "outputs without result delivery");
    }
  }

  /**
   * Input stream header; the stream ID is supplied by QUIC, not this record.
   *
   * @param generation attached session
   * @param operation stable mutation ID
   * @param parameters immutable request
   */
  public record InputHeader(long generation, OperationId operation, AdmitParameters parameters)
      implements Value {
    /** Validate the record's structural constraints. */
    public InputHeader {
      id(generation);
      present(operation);
      present(parameters);
    }
  }

  /**
   * Result response stream header.
   *
   * @param request outstanding control request
   * @param generation session
   * @param work logical work
   * @param attempt producing attempt
   * @param index output index
   * @param length payload length
   * @param sha256 object digest
   */
  public record ResultHeader(
      long request,
      long generation,
      WorkKey work,
      long attempt,
      int index,
      long length,
      Digest sha256)
      implements Value {
    /** Validate the record's structural constraints. */
    public ResultHeader {
      id(request);
      id(generation);
      present(work);
      id(attempt);
      range(index, 0, 255);
      number(length);
      present(sha256);
    }
  }

  /**
   * Published object descriptor.
   *
   * @param index contiguous output index
   * @param length payload bytes
   * @param sha256 object digest
   * @param contentType printable label
   * @param locator output name without credentials
   */
  public record Output(int index, long length, Digest sha256, String contentType, Locator locator)
      implements Value {
    /** Validate the record's structural constraints. */
    public Output {
      range(index, 0, 255);
      number(length);
      present(sha256);
      label(contentType);
      present(locator);
    }
  }

  /**
   * Authenticated immutable output manifest, not a portable signature.
   *
   * @param authority issuer label
   * @param owner authenticated owner
   * @param generation session
   * @param work logical work
   * @param attempt producing attempt
   * @param inputSha256 admitted input digest
   * @param committedAt publication UTC
   * @param availableUntil output deadline
   * @param outputs contiguous descriptors
   */
  public record Manifest(
      String authority,
      String owner,
      long generation,
      WorkKey work,
      long attempt,
      Digest inputSha256,
      long committedAt,
      long availableUntil,
      List<Output> outputs)
      implements Value {
    /** Validate the record's structural constraints. */
    public Manifest {
      identity(authority);
      identity(owner);
      id(generation);
      present(work);
      id(attempt);
      present(inputSha256);
      number(committedAt);
      number(availableUntil);
      require(availableUntil > committedAt, "invalid output availability interval");
      outputs = list(outputs, 256);
      long total = 0;
      for (int n = 0; n < outputs.size(); n++) {
        Output output = outputs.get(n);
        require(output.index == n, "noncontiguous output indexes");
        total = sum(total, output.length);
        Locator.Target target = output.locator.target();
        require(
            target.generation() == generation
                && target.work().equals(work)
                && target.attempt() == attempt
                && target.index() == n,
            "locator contradicts manifest identity");
      }
    }
  }

  /**
   * Consistent work snapshot; nullable fields are explicit on the wire.
   *
   * @param work logical work
   * @param state current state
   * @param attempt zero only before admission
   * @param input retained input descriptor
   * @param admittedAt admission UTC
   * @param deadline execution deadline
   * @param terminalAt terminal UTC
   * @param receiptUntil terminal receipt deadline
   * @param outputUntil output deadline
   * @param child immutable child allocation
   * @param manifest success manifest
   * @param diagnostic current diagnostic
   */
  public record WorkView(
      WorkKey work,
      State state,
      long attempt,
      Input input,
      Long admittedAt,
      Long deadline,
      Long terminalAt,
      Long receiptUntil,
      Long outputUntil,
      ChildScope child,
      Manifest manifest,
      Diagnostic diagnostic)
      implements Value {
    /** Validate the record's structural constraints. */
    public WorkView {
      present(work);
      present(state);
      number(attempt);
      for (Long time : new Long[] {admittedAt, deadline, terminalAt, receiptUntil, outputUntil})
        if (time != null) number(time);
      if (input == null) {
        require(
            attempt == 0 && admittedAt == null && deadline == null && child == null,
            "inputless work has admission fields");
        require(
            state == State.DECLARED
                || state == State.CANCELLING
                || state == State.CANCELLED
                || state == State.SKIPPED,
            "state requires admitted input");
      } else {
        require(
            attempt > 0
                && admittedAt != null
                && deadline != null
                && deadline > admittedAt
                && state != State.DECLARED,
            "incomplete admission fields");
        if (child != null) require(child.scope > work.scope, "invalid child allocation");
      }
      require(
          state != State.WAITING_CHILDREN || child != null,
          "waiting for children without child allocation");
      if (state.terminal()) {
        require(
            terminalAt != null && receiptUntil != null && receiptUntil > terminalAt,
            "terminal receipt interval missing or invalid");
        require(admittedAt == null || terminalAt >= admittedAt, "terminal time precedes admission");
      } else
        require(
            terminalAt == null && receiptUntil == null && outputUntil == null && manifest == null,
            "nonterminal work has terminal fields");
      require(
          state != State.FAILED && state != State.AWAITING_RETRY || diagnostic != null,
          "failure/retry state requires diagnostic");
      if (state == State.SUCCEEDED) {
        require(terminalAt < deadline, "success at or beyond execution deadline");
        if (manifest == null) require(outputUntil == null, "output deadline without manifest");
        else
          require(
              outputUntil != null
                  && manifest.work.equals(work)
                  && manifest.attempt == attempt
                  && manifest.inputSha256.equals(input.sha256)
                  && manifest.committedAt == terminalAt
                  && manifest.availableUntil == outputUntil,
              "manifest contradicts work view");
      } else require(manifest == null && outputUntil == null, "non-success has published output");
    }

    /**
     * Validate a work view against the retained profiles.
     *
     * @param results retained session profile combination
     */
    public void validateProfiles(boolean results) {
      require(manifest == null || results, "manifest without result delivery");
      require(
          state != State.SUCCEEDED || !results || manifest != null,
          "result-enabled success lacks manifest");
    }
  }

  /** Typed immutable mutation outcomes. */
  public sealed interface Outcome extends Value
      permits Admitted, Declared, Retried, Cancelled, ScopeCancelled, Skipped {}

  /**
   * Committed first admission and its immutable execution interval.
   *
   * @param work logical work identity
   * @param attempt producing execution attempt
   * @param admittedAt admission UTC milliseconds
   * @param deadline immutable execution deadline
   * @param child immutable child allocation, null for a leaf
   */
  public record Admitted(
      WorkKey work, long attempt, long admittedAt, long deadline, ChildScope child)
      implements Outcome {
    /** Validate the record's structural constraints. */
    public Admitted {
      present(work);
      require(attempt == 1, "admission must begin at attempt one");
      number(admittedAt);
      number(deadline);
      require(deadline > admittedAt, "invalid execution interval");
      if (child != null) require(child.scope > work.scope, "invalid child scope");
    }
  }

  /**
   * Committed declaration count and optional final membership seal.
   *
   * @param scope scope ID
   * @param producer scope producer
   * @param acceptedCount members accepted by this operation
   * @param declared total declared membership count
   * @param seal membership seal, or null before sealing
   */
  public record Declared(long scope, int producer, int acceptedCount, long declared, Digest seal)
      implements Outcome {
    /** Validate the record's structural constraints. */
    public Declared {
      number(scope);
      Checks.producer(producer);
      range(acceptedCount, 0, 256);
      number(declared);
      require(scope != 0 || producer == 0, "invalid root producer");
      require(
          acceptedCount <= declared && (acceptedCount > 0 || seal != null),
          "invalid declaration receipt");
    }
  }

  /**
   * Committed replacement of exactly the expected attempt.
   *
   * @param work logical work identity
   * @param expectedAttempt exact attempt being replaced
   * @param replacementAttempt expected attempt plus one
   * @param acceptedAt durable commit UTC milliseconds
   */
  public record Retried(
      WorkKey work, long expectedAttempt, long replacementAttempt, long acceptedAt)
      implements Outcome {
    /** Validate the record's structural constraints. */
    public Retried {
      present(work);
      id(expectedAttempt);
      id(replacementAttempt);
      number(acceptedAt);
      require(
          expectedAttempt < Long.MAX_VALUE && replacementAttempt == expectedAttempt + 1,
          "retry must advance by exactly one");
    }
  }

  private static void fence(
      WorkKey work, long acceptedAt, int disposition, State state, State desired) {
    present(work);
    number(acceptedAt);
    range(disposition, 0, 1);
    present(state);
    require(
        disposition == 1 ? state.terminal() : state == State.CANCELLING || state == desired,
        "fence disposition contradicts state");
  }

  /**
   * Committed work cancellation disposition and state at that commit.
   *
   * @param work logical work identity
   * @param acceptedAt durable commit UTC milliseconds
   * @param disposition zero for an accepted fence, one for an already-terminal outcome
   * @param state state at the observation or commit
   */
  public record Cancelled(WorkKey work, long acceptedAt, int disposition, State state)
      implements Outcome {
    /** Validate the record's structural constraints. */
    public Cancelled {
      fence(work, acceptedAt, disposition, state, State.CANCELLED);
    }
  }

  /**
   * Committed scope cancellation fence.
   *
   * @param scope scope ID
   * @param acceptedAt durable commit UTC milliseconds
   */
  public record ScopeCancelled(long scope, long acceptedAt) implements Outcome {
    /** Validate the record's structural constraints. */
    public ScopeCancelled {
      number(scope);
      number(acceptedAt);
    }
  }

  /**
   * Committed work skip disposition and state at that commit.
   *
   * @param work logical work identity
   * @param acceptedAt durable commit UTC milliseconds
   * @param disposition zero for an accepted fence, one for an already-terminal outcome
   * @param state state at the observation or commit
   */
  public record Skipped(WorkKey work, long acceptedAt, int disposition, State state)
      implements Outcome {
    /** Validate the record's structural constraints. */
    public Skipped {
      fence(work, acceptedAt, disposition, state, State.SKIPPED);
    }
  }

  /**
   * Retained identity, request commitment and typed outcome.
   *
   * @param operation original mutation ID
   * @param requestDigest domain-separated request hash
   * @param outcome committed outcome
   */
  public record OperationReceipt(OperationId operation, Digest requestDigest, Outcome outcome)
      implements Value {
    /** Validate the record's structural constraints. */
    public OperationReceipt {
      present(operation);
      present(requestDigest);
      present(outcome);
    }
  }
}
