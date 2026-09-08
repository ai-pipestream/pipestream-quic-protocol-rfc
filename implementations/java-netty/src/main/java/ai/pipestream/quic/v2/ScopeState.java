package ai.pipestream.quic.v2;

import static ai.pipestream.quic.v2.Records.*;

import java.sql.SQLException;
import java.util.Objects;

/**
 * Local fixed-capacity scope image; its membership seal is not a closure acknowledgment.
 *
 * @param id scope identifier
 * @param producer owning producer identifier
 * @param parent immutable parent work key, or {@code null} for the root
 * @param declared accepted member count
 * @param last highest accepted member identifier
 * @param seal membership digest, or {@code null} before a seal
 * @param cancelled whether cancellation was retained
 * @param revoked whether the root was revoked
 * @param summary immutable sealed summary, or {@code null}
 */
record ScopeState(
    long id,
    int producer,
    WorkKey parent,
    long declared,
    long last,
    Digest seal,
    boolean cancelled,
    boolean revoked,
    ScopeSummary summary) {
  /** Validate retained identity, counters and any immutable closure evidence. */
  ScopeState {
    Checks.scope(id, producer, parent);
    Checks.range(declared, 0, Long.MAX_VALUE);
    Checks.range(last, 0, Long.MAX_VALUE);
    ProtocolError.require((declared == 0) == (last == 0), "scope count and high-water disagree");
    ProtocolError.require(!revoked || id == 0, "revocation is root-qualified");
    if (summary != null)
      ProtocolError.require(
          summary.scope() == id
              && summary.producer() == producer
              && Objects.equals(summary.parent(), parent)
              && Objects.equals(summary.seal(), seal)
              && summary.declared() == declared,
          "scope summary differs from retained membership");
  }

  /**
   * Construct the one empty caller-owned root at session creation.
   *
   * @return the initial root scope state
   */
  static ScopeState root() {
    return new ScopeState(0, 0, null, 0, 0, null, false, false, null);
  }

  /**
   * Preserve all other scope state while applying an ordinary membership transaction.
   *
   * @param count accepted member count
   * @param highWater highest accepted member identifier
   * @param digest membership digest
   * @return state with the replacement membership fields
   */
  ScopeState members(long count, long highWater, Digest digest) {
    return new ScopeState(
        id, producer, parent, count, highWater, digest, cancelled, revoked, summary);
  }

  /**
   * Encode this private storage image, without changing the Appendix F wire grammar.
   *
   * @return bounded retained-state encoding
   */
  byte[] encode() {
    Cbor.Writer out = new Cbor.Writer(FixedRecords.SCOPE_CAPACITY);
    out.array(9);
    out.number(id);
    out.number(producer);
    RecordCodec.nullable(out, parent);
    out.number(declared);
    out.number(last);
    RecordCodec.nullable(out, seal);
    out.bool(cancelled);
    out.bool(revoked);
    RecordCodec.nullable(out, summary);
    return out.finish();
  }

  /**
   * Decode bounded retained state, mapping storage damage to a storage failure.
   *
   * @param bytes bounded retained-state encoding
   * @return decoded scope state
   * @throws SQLException if the retained encoding is invalid
   */
  static ScopeState decode(byte[] bytes) throws SQLException {
    try {
      Cbor.Reader in = new Cbor.Reader(bytes, FixedRecords.SCOPE_CAPACITY);
      in.exact(9);
      ScopeState result =
          new ScopeState(
              in.number(),
              RecordCodec.small(in, 1),
              RecordCodec.parent(in),
              in.number(),
              in.number(),
              RecordCodec.nullableDigest(in),
              in.bool(),
              in.bool(),
              in.nullable() ? null : RecordCodec.summary(in));
      in.end();
      return result;
    } catch (ProtocolError invalid) {
      throw new SQLException("V2 store: invalid retained scope image", invalid);
    }
  }
}
