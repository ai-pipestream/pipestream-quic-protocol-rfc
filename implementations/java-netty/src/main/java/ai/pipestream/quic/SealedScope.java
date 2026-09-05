package ai.pipestream.quic;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Independent Section 9.5 status commitments for sealed Layer 1 child scopes. */
public final class SealedScope {
  /** SCOPE_DIGEST unified control frame type. */
  public static final int FRAME = 0x54;
  private SealedScope() {}

  /**
   * One direct entity's final status; payload and output contents are not committed here.
   * @param entityId assignable identifier
   * @param state COMPLETE or FAILED; Layer 2 is not active in this profile
   */
  public record Terminal(long entityId, int state) {}

  /**
   * An immutable child-scope summary. Counters preserve the full wire uint64 range.
   * @param scopeId nonzero unsigned 32-bit child scope
   * @param processed total direct entities
   * @param succeeded COMPLETE entities
   * @param failed FAILED entities
   * @param merkleRoot SHA-256 commitment to ordered identifiers and final statuses
   */
  public record Digest(long scopeId, BigInteger processed, BigInteger succeeded,
      BigInteger failed, byte[] merkleRoot) {
    /** Defensively copies the root and requires non-null counters. */
    public Digest {
      Objects.requireNonNull(processed, "processed");
      Objects.requireNonNull(succeeded, "succeeded");
      Objects.requireNonNull(failed, "failed");
      merkleRoot = merkleRoot.clone();
    }

    /** Returns the status commitment.
     * @return defensive copy of the root
     */
    @Override public byte[] merkleRoot() { return merkleRoot.clone(); }

    @Override public boolean equals(Object other) {
      return other instanceof Digest that && scopeId == that.scopeId
          && processed.equals(that.processed) && succeeded.equals(that.succeeded)
          && failed.equals(that.failed) && Arrays.equals(merkleRoot, that.merkleRoot);
    }

    @Override public int hashCode() {
      return 31 * Objects.hash(scopeId, processed, succeeded, failed) + Arrays.hashCode(merkleRoot);
    }
  }

  /**
   * Computes the ordered status Merkle tree, promoting an odd node unchanged.
   * The caller must separately establish sealed membership and descendant closure.
   * @param scopeId nonzero child scope
   * @param entities all direct entities in strictly ascending identifier order
   * @return summary, not proof of payload integrity or correct computation
   * @throws ProtocolException for invalid scope, order, or nonterminal statuses
   */
  public static Digest summarize(long scopeId, List<Terminal> entities) throws ProtocolException {
    childScope(scopeId);
    if (entities.isEmpty()) throw invalid("cannot summarize an empty scope");
    List<byte[]> level = new ArrayList<>(entities.size());
    MessageDigest hash = SealedWork.sha256();
    ByteBuffer leaf = ByteBuffer.allocate(6);
    long previous = 0;
    long succeeded = 0;
    for (Terminal entity : entities) {
      if (entity.entityId <= previous || entity.entityId > Wire.MAX_ENTITY_ID
          || (entity.state != Wire.STATUS_COMPLETE && entity.state != Wire.STATUS_FAILED)) {
        throw Wire.entity("scope status leaves must be ordered and terminal at Layer 1");
      }
      previous = entity.entityId;
      if (entity.state == Wire.STATUS_COMPLETE) succeeded++;
      leaf.clear().put((byte) 0).putInt((int) entity.entityId).put((byte) entity.state);
      level.add(hash.digest(leaf.array()));
    }
    while (level.size() > 1) {
      List<byte[]> next = new ArrayList<>((level.size() + 1) / 2);
      for (int i = 0; i < level.size(); i += 2) {
        if (i + 1 == level.size()) next.add(level.get(i));
        else {
          hash.update((byte) 1); hash.update(level.get(i)); hash.update(level.get(i + 1));
          next.add(hash.digest());
        }
      }
      level = next;
    }
    return new Digest(scopeId, BigInteger.valueOf(entities.size()), BigInteger.valueOf(succeeded),
        BigInteger.valueOf(entities.size() - succeeded), level.getFirst());
  }

  /**
   * Encodes the fixed 72-octet payload, with reserved fields and deferred count zero.
   * @param digest child-scope summary
   * @return complete SCOPE_DIGEST frame
   * @throws ProtocolException for invalid summary fields
   */
  public static byte[] encode(Digest digest) throws ProtocolException {
    validate(digest);
    return Wire.encodeControl(FRAME, ByteBuffer.allocate(72).putInt(0).putInt((int) digest.scopeId)
        .putLong(digest.processed.longValue()).putLong(digest.succeeded.longValue())
        .putLong(digest.failed.longValue()).putLong(0).put(digest.merkleRoot).array());
  }

  /**
   * Decodes a sealed-profile summary; reserved bits are ignored as Section 6.3 requires.
   * @param frame complete frame
   * @return immutable child-scope summary
   * @throws ProtocolException for wrong layout, counters, or scope
   */
  public static Digest decode(byte[] frame) throws ProtocolException {
    Wire.ControlFrame control = Wire.decodeControl(frame);
    if (control.type() != FRAME || control.payload().length != 72) throw Wire.frame("expected 72-octet SCOPE_DIGEST");
    ByteBuffer payload = ByteBuffer.wrap(control.payload());
    payload.getInt();
    long scopeId = Integer.toUnsignedLong(payload.getInt());
    BigInteger processed = counter(payload), succeeded = counter(payload), failed = counter(payload);
    if (counter(payload).signum() != 0) throw invalid("sealed Layer 1 scope cannot contain deferred entities");
    byte[] root = new byte[32]; payload.get(root);
    Digest result = new Digest(scopeId, processed, succeeded, failed, root);
    validate(result);
    return result;
  }

  private static BigInteger counter(ByteBuffer input) {
    byte[] bytes = new byte[8]; input.get(bytes); return new BigInteger(1, bytes);
  }

  private static void validate(Digest digest) throws ProtocolException {
    childScope(digest.scopeId);
    if (digest.processed.signum() <= 0 || digest.processed.compareTo(SealedCbor.MAX_UINT) > 0
        || digest.succeeded.signum() < 0 || digest.failed.signum() < 0
        || !digest.succeeded.add(digest.failed).equals(digest.processed) || digest.merkleRoot.length != 32) {
      throw invalid("scope counts or status commitment are invalid");
    }
  }

  static void childScope(long scopeId) throws ProtocolException {
    if (scopeId < 1 || scopeId > 0xffff_ffffL) throw invalid("expected a nonzero child scope");
  }

  static ProtocolException invalid(String message) {
    return new ProtocolException(0x09, "PIPESTREAM_SCOPE_INVALID", message);
  }
}
