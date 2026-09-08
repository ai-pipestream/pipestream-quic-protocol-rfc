package ai.pipestream.quic.v2;

import static ai.pipestream.quic.v2.Records.*;

import java.sql.SQLException;
import java.util.Objects;

/**
 * Immutable local evidence preceding partial session deletion, never a wire receipt. Relational
 * verification must additionally bind this image to the retained session, closed root, trusted
 * clock and non-reuse allocators before it can authorize skipping live-state relationships.
 *
 * @param context exact authority, owner and generation
 * @param creationSequence owner's immutable creation identity
 * @param root verified closed caller-owned root, retained until final session deletion
 * @param cutoff latest creation, work, output or dependency promise checked at retirement
 * @param at trusted UTC sample when retirement was authorized
 */
record RetirementRecord(
    Commitments.Context context, long creationSequence, ScopeSummary root, long cutoff, long at) {
  /** Fixed allocation, including worst-case bounded identity labels and a root summary. */
  static final int CAPACITY = 1024;

  /** Reject malformed identity, non-root closure and inconsistent retirement timestamps. */
  RetirementRecord {
    Objects.requireNonNull(context);
    Checks.id(creationSequence);
    Objects.requireNonNull(root);
    Checks.number(cutoff);
    Checks.number(at);
    ProtocolError.require(
        root.scope() == 0 && root.producer() == 0 && root.parent() == null,
        "retirement requires a caller-owned root closure");
    ProtocolError.require(
        root.closedAt() <= cutoff && cutoff <= at, "retirement timestamps contradict root closure");
  }

  /**
   * Encode the bounded private image without changing the protocol grammar.
   *
   * @return deterministic local CBOR
   */
  byte[] encode() {
    Cbor.Writer out = new Cbor.Writer(CAPACITY);
    out.array(5);
    out.array(3);
    out.text(context.authority(), 128);
    out.text(context.owner(), 128);
    out.number(context.generation());
    out.number(creationSequence);
    RecordCodec.write(out, root);
    out.number(cutoff);
    out.number(at);
    return out.finish();
  }

  /**
   * Decode one checked-capacity image and reject trailing data or malformed retained evidence.
   *
   * @param bytes bounded storage body
   * @return structurally valid proof, still requiring relational validation
   * @throws SQLException invalid local representation
   */
  static RetirementRecord decode(byte[] bytes) throws SQLException {
    try {
      Cbor.Reader in = new Cbor.Reader(bytes, CAPACITY);
      in.exact(5);
      in.exact(3);
      Commitments.Context context =
          new Commitments.Context(in.text(128), in.text(128), in.number());
      RetirementRecord result =
          new RetirementRecord(
              context, in.number(), RecordCodec.summary(in), in.number(), in.number());
      in.end();
      return result;
    } catch (ProtocolError invalid) {
      throw new SQLException("V2 retirement: invalid retained eligibility image", invalid);
    }
  }

  /**
   * Verify a surviving work view after an earlier retirement transaction removed other members.
   * This cannot establish original eligibility by itself or waive checks of remaining job
   * resources.
   *
   * @param view independently decoded surviving work
   * @throws SQLException unresolved work or an unexpired promise contradicts this proof
   */
  void verifyWork(WorkView view) throws SQLException {
    if (!view.state().terminal()
        || view.terminalAt() == null
        || view.terminalAt() > cutoff
        || view.receiptUntil() == null
        || view.receiptUntil() > cutoff
        || view.outputUntil() != null && view.outputUntil() > cutoff)
      throw new SQLException("V2 retirement: surviving work has an unresolved promise");
  }
}
