package ai.pipestream.quic.v2;

import static ai.pipestream.quic.v2.Checks.*;
import static ai.pipestream.quic.v2.ProtocolError.*;
import static ai.pipestream.quic.v2.Records.*;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/** Section 12 commitments computed from typed values, independently of the Rust implementation. */
public final class Commitments {
  private Commitments() {}

  /**
   * Authenticated context, not a bearer credential or a proof of authorization.
   *
   * @param authority configured issuer identity
   * @param owner authenticated owner identity
   * @param generation immutable session generation
   */
  public record Context(String authority, String owner, long generation) {
    /** Validate the record's structural constraints. */
    public Context {
      identity(authority);
      identity(owner);
      id(generation);
    }
  }

  static MessageDigest sha256() {
    try {
      return MessageDigest.getInstance("SHA-256");
    } catch (NoSuchAlgorithmException impossible) {
      throw new IllegalStateException("SHA-256 unavailable", impossible);
    }
  }

  private static MessageDigest domain(String value) {
    MessageDigest digest = sha256();
    digest.update(value.getBytes(StandardCharsets.US_ASCII));
    return digest;
  }

  private static void context(Cbor.Writer writer, Context context) {
    present(context);
    writer.text(context.authority(), 128);
    writer.text(context.owner(), 128);
    writer.number(context.generation());
  }

  private static MessageDigest operation(
      Context context, int producer, OperationId operation, int type, int code) {
    Checks.producer(producer);
    present(operation);
    MessageDigest digest = domain("pipestream-operation-v2");
    Cbor.Writer writer = new Cbor.Writer(digest);
    writer.array(8);
    context(writer, context);
    writer.number(producer);
    writer.bytes(operation.bytes());
    writer.number(type);
    writer.number(code);
    return digest;
  }

  /**
   * Commit an input admission, excluding transport stream identity and payload bytes.
   *
   * @param context authenticated session
   * @param producer operation originator, not inferred from the target
   * @param input immutable input header
   * @return domain-separated request digest
   */
  public static Digest operation(Context context, int producer, InputHeader input) {
    present(input);
    present(context);
    require(
        input.generation() == context.generation(),
        "admission generation differs from hash context");
    MessageDigest digest = operation(context, producer, input.operation(), 4, 0);
    RecordCodec.write(new Cbor.Writer(digest), input.parameters());
    return new Digest(digest.digest());
  }

  /**
   * Commit a control mutation, excluding its connection-local request number.
   *
   * @param context authenticated session
   * @param producer operation originator; may differ from the target producer
   * @param mutation declaration, scope cancellation, retry, cancellation or skip
   * @return domain-separated request digest
   */
  public static Digest operation(Context context, int producer, Messages.Message mutation) {
    present(mutation);
    OperationId operation;
    int type;
    int code;
    switch (mutation) {
      case Messages.Declare m -> {
        operation = m.operation();
        type = 3;
        code = 0;
      }
      case Messages.CancelScope m -> {
        operation = m.operation();
        type = 3;
        code = 6;
      }
      case Messages.Retry m -> {
        operation = m.operation();
        type = 4;
        code = 6;
      }
      case Messages.Cancel m -> {
        operation = m.operation();
        type = 4;
        code = 8;
      }
      case Messages.Skip m -> {
        operation = m.operation();
        type = 4;
        code = 10;
      }
      default -> throw frame("message is not a retained mutation");
    }
    MessageDigest digest = operation(context, producer, operation, type, code);
    Cbor.Writer w = new Cbor.Writer(digest);
    switch (mutation) {
      case Messages.Declare m -> {
        w.array(3);
        w.number(m.scope());
        w.array(m.entityIds().size());
        for (long entity : m.entityIds()) w.number(entity);
        w.bool(m.seal());
      }
      case Messages.CancelScope m -> {
        w.array(1);
        w.number(m.scope());
      }
      case Messages.Retry m -> {
        w.array(2);
        RecordCodec.write(w, m.work());
        w.number(m.expectedAttempt());
      }
      case Messages.Cancel m -> {
        w.array(1);
        RecordCodec.write(w, m.work());
      }
      case Messages.Skip m -> {
        w.array(1);
        RecordCodec.write(w, m.work());
      }
      default -> throw new AssertionError("validated mutation changed type");
    }
    return new Digest(digest.digest());
  }

  /**
   * Hash an immutable manifest without materializing another encoded copy.
   *
   * @param manifest complete validated publication record
   * @return manifest commitment
   */
  public static Digest manifest(Manifest manifest) {
    present(manifest);
    MessageDigest digest = domain("pipestream-result-manifest-v2");
    RecordCodec.write(new Cbor.Writer(digest), manifest);
    return new Digest(digest.digest());
  }

  /**
   * Compute one terminal leaf. This does not establish membership or authenticate execution.
   *
   * @param view consistent terminal work view
   * @param childRoot separately verified child root, present exactly when the view has a child
   * @return status leaf
   */
  public static Digest statusLeaf(WorkView view, Digest childRoot) {
    present(view);
    require(view.state().terminal(), "status leaf requires terminal work");
    require(
        (view.child() == null) == (childRoot == null), "child root presence contradicts work view");
    MessageDigest digest = domain("pipestream-status-leaf-v2");
    Cbor.Writer w = new Cbor.Writer(digest);
    w.array(5);
    RecordCodec.write(w, view.work());
    w.number(view.state().value());
    w.number(view.attempt());
    if (view.manifest() == null) w.nil();
    else w.bytes(manifest(view.manifest()).bytes());
    if (childRoot == null) w.nil();
    else w.bytes(childRoot.bytes());
    return new Digest(digest.digest());
  }

  /**
   * Combine two adjacent roots; callers duplicate an odd last root at each level.
   *
   * @param left earlier root
   * @param right later root
   * @return domain-separated parent hash
   */
  public static Digest statusNode(Digest left, Digest right) {
    present(left);
    present(right);
    MessageDigest digest = domain("pipestream-status-node-v2");
    digest.update(left.bytes());
    digest.update(right.bytes());
    return new Digest(digest.digest());
  }

  /**
   * Compute the empty status root.
   *
   * @return the status commitment for a scope with no declared members
   */
  public static Digest emptyStatus() {
    return new Digest(domain("pipestream-status-empty-v2").digest());
  }

  /**
   * A single-use constant-memory membership hasher. Errors invalidate the partial hash. Instances
   * are confined to one caller; declaration batches need not be retained.
   */
  public static final class Seal {
    private final MessageDigest digest;
    private final Cbor.Writer writer;
    private final long declared;
    private long count;
    private long previous;
    private boolean ended;

    /**
     * Start a seal over an exact, known membership count.
     *
     * @param context authenticated session
     * @param scope scope ID
     * @param producer scope producer
     * @param parent parent work key, null only for root
     * @param declared exact final membership count
     */
    public Seal(Context context, long scope, int producer, WorkKey parent, long declared) {
      Checks.scope(scope, producer, parent);
      number(declared);
      digest = domain("pipestream-scope-seal-v2");
      writer = new Cbor.Writer(digest);
      this.declared = declared;
      writer.array(7);
      context(writer, context);
      writer.number(scope);
      writer.number(producer);
      if (parent == null) writer.nil();
      else RecordCodec.write(writer, parent);
      writer.array(declared);
    }

    /**
     * Add the next member without retaining it.
     *
     * @param entity positive ID, strictly greater than every prior ID
     */
    public void add(long entity) {
      require(!ended, "seal is finished or invalid");
      try {
        id(entity);
        require(count < declared && entity > previous, "seal membership count/order mismatch");
        writer.number(entity);
        count++;
        previous = entity;
      } catch (ProtocolError error) {
        ended = true;
        throw error;
      }
    }

    /**
     * Finish only after every declared member has been added.
     *
     * @return immutable membership seal
     */
    public Digest finish() {
      require(!ended, "seal is finished or invalid");
      ended = true;
      require(count == declared, "incomplete sealed membership");
      return new Digest(digest.digest());
    }
  }

  /**
   * Computed status commitment and disjoint terminal counts.
   *
   * @param root status root, not a membership seal
   * @param counts observed terminal partition
   */
  public record Status(Digest root, Counts counts) {
    /** Validate the record's structural constraints. */
    public Status {
      present(root);
      present(counts);
    }
  }

  /**
   * A single-use status fold with at most 63 retained 32-octet subtree hashes. Scope identity,
   * strict member ordering, exact count and terminal state are checked here. Callers must
   * separately verify the membership seal, session profile requirements and descendant coverage.
   * Instances are not thread-safe.
   */
  public static final class StatusTree {
    private final Digest[] frontier = new Digest[63];
    private final long scope;
    private final int producer;
    private final long declared;
    private final long[] counts = new long[4];
    private long count;
    private long previous;
    private boolean ended;

    /**
     * Start an exact-count status fold.
     *
     * @param scope scope ID
     * @param producer scope producer
     * @param declared exact membership count
     */
    public StatusTree(long scope, int producer, long declared) {
      number(scope);
      Checks.producer(producer);
      number(declared);
      require(scope != 0 || producer == 0, "invalid root producer");
      this.scope = scope;
      this.producer = producer;
      this.declared = declared;
    }

    /**
     * Add the next terminal member and its separately checked descendant root.
     *
     * @param view terminal member from this scope in strictly increasing ID order
     * @param childRoot child commitment, null exactly for a leaf
     */
    public void add(WorkView view, Digest childRoot) {
      require(!ended, "status fold is finished or invalid");
      try {
        present(view);
        require(
            count < declared
                && view.work().scope() == scope
                && view.work().producer() == producer
                && view.work().entity() > previous,
            "status membership count/identity/order mismatch");
        Digest carry = statusLeaf(view, childRoot);
        int level = 0;
        while (frontier[level] != null) {
          carry = statusNode(frontier[level], carry);
          frontier[level] = null;
          level++;
        }
        frontier[level] = carry;
        counts[view.state().value() - State.SUCCEEDED.value()]++;
        count++;
        previous = view.work().entity();
      } catch (ProtocolError error) {
        ended = true;
        throw error;
      }
    }

    /**
     * Inspect the retained subtree payload.
     *
     * @return currently retained hash bytes, excluding fixed object/array overhead
     */
    public int retainedHashBytes() {
      int bytes = 0;
      for (Digest value : frontier) if (value != null) bytes += 32;
      return bytes;
    }

    /**
     * Finish the tree, duplicating an odd final hash at each level.
     *
     * @return status root and exact terminal partition
     */
    public Status finish() {
      require(!ended, "status fold is finished or invalid");
      ended = true;
      require(count == declared, "incomplete status membership");
      Digest right = null;
      int height = 0;
      for (int level = 0; level < frontier.length; level++) {
        if (frontier[level] == null) continue;
        if (right == null) {
          right = frontier[level];
          height = level;
        } else {
          while (height < level) {
            right = statusNode(right, right);
            height++;
          }
          right = statusNode(frontier[level], right);
          height = level + 1;
        }
      }
      return new Status(
          right == null ? emptyStatus() : right,
          new Counts(counts[0], counts[1], counts[2], counts[3]));
    }
  }
}
