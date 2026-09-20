package ai.pipestream.quic.storage;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.List;
import java.util.Objects;

/**
 * Shared logical layout for every storage backend, identical across languages so a
 * store written by one implementation is readable by the other. This class pins:
 * the scope tree naming derived from {@link ScopePath}, the 2-hex-character hash
 * fanout sharding entities within one leaf scope, and the canonical per-scope
 * content digest that seal records and verification compare against. Physical
 * record encodings remain backend-local.
 */
public final class StoreLayout {
  /** Hex digits of one fanout bucket name; 2 digits address 256 buckets. */
  public static final int FANOUT_HEX_DIGITS = 2;
  /** Number of entity buckets within one leaf scope. */
  public static final int FANOUT_BUCKETS = 1 << (4 * FANOUT_HEX_DIGITS);
  /** One content or scope digest, in octets. */
  public static final int DIGEST_BYTES = 32;
  /** Largest assignable entity identifier; identifiers are unsigned 63-bit. */
  public static final long MAX_ENTITY_ID = 0x7fff_ffff_ffff_ffffL;

  /** One entity's recorded content digest, in ascending-identifier order input.
   * @param entityId unsigned 63-bit entity identifier
   * @param digest exactly {@link #DIGEST_BYTES} content octets
   */
  public record EntityDigest(long entityId, byte[] digest) {
    /**
     * Copies the digest.
     * @param entityId unsigned 63-bit entity identifier
     * @param digest exactly {@link #DIGEST_BYTES} content octets
     */
    public EntityDigest {
      if (entityId < 0 || entityId > MAX_ENTITY_ID) {
        throw new IllegalArgumentException("entity identifier is not unsigned 63-bit");
      }
      Objects.requireNonNull(digest, "digest");
      if (digest.length != DIGEST_BYTES) {
        throw new IllegalArgumentException("content digest must be " + DIGEST_BYTES + " octets");
      }
      digest = digest.clone();
    }

    /** @return defensive digest copy */
    @Override public byte[] digest() {
      return digest.clone();
    }
  }

  private StoreLayout() {}

  /**
   * Renders one scope path component as lowercase zero-padded 8 hex digits.
   * @param component unsigned 32-bit scope component
   * @return one path level of the scope tree
   */
  public static String scopeComponentName(long component) {
    if (component < 0 || component > ScopePath.MAX_COMPONENT) {
      throw new IllegalArgumentException("scope component is not an unsigned 32-bit value");
    }
    return HexFormat.of().toHexDigits(component, 8);
  }

  /**
   * Renders one entity identifier as lowercase zero-padded 16 hex digits, its
   * unique name within a fanout bucket.
   * @param entityId unsigned 63-bit entity identifier
   * @return entity file name
   */
  public static String entityName(long entityId) {
    if (entityId < 0 || entityId > MAX_ENTITY_ID) {
      throw new IllegalArgumentException("entity identifier is not unsigned 63-bit");
    }
    return HexFormat.of().toHexDigits(entityId, 16);
  }

  /**
   * Selects the fanout bucket of one entity: the first octet of SHA-256 over the
   * 8-octet big-endian entity identifier, rendered as 2 lowercase hex digits.
   * @param entityId unsigned 63-bit entity identifier
   * @return 2-hex-character bucket name within the leaf scope
   */
  public static String fanoutBucket(long entityId) {
    if (entityId < 0 || entityId > MAX_ENTITY_ID) {
      throw new IllegalArgumentException("entity identifier is not unsigned 63-bit");
    }
    byte[] hash = sha256().digest(bigEndian(entityId));
    return HexFormat.of().formatHex(hash, 0, FANOUT_HEX_DIGITS / 2);
  }

  /**
   * Computes the canonical scope content digest: SHA-256 over every direct
   * entity in strictly ascending identifier order, each mixed in as one
   * zero-valued domain-separation octet, the 8-octet big-endian identifier, and
   * its {@value #DIGEST_BYTES}-octet content digest. An empty scope hashes the
   * empty input. This is the value {@code sealScope} records and
   * {@code verifyScope} recomputes.
   * @param entities direct entity content digests in any order
   * @return canonical scope digest
   */
  public static byte[] scopeDigest(List<EntityDigest> entities) {
    Objects.requireNonNull(entities, "entities");
    List<EntityDigest> ordered = new ArrayList<>(entities);
    ordered.sort(Comparator.comparingLong(EntityDigest::entityId));
    MessageDigest hash = sha256();
    long previous = -1;
    for (EntityDigest entity : ordered) {
      if (entity.entityId() == previous) {
        throw new IllegalArgumentException("duplicate entity identifier in scope digest input");
      }
      previous = entity.entityId();
      hash.update((byte) 0);
      hash.update(bigEndian(entity.entityId()));
      hash.update(entity.digest());
    }
    return hash.digest();
  }

  static MessageDigest sha256() {
    try {
      return MessageDigest.getInstance("SHA-256");
    } catch (NoSuchAlgorithmException impossible) {
      throw new IllegalStateException("SHA-256 is unavailable", impossible);
    }
  }

  /** Returns the big-endian encoding shared by fanout and digest domain separation. */
  private static byte[] bigEndian(long value) {
    return java.nio.ByteBuffer.allocate(8).putLong(value).array();
  }
}
