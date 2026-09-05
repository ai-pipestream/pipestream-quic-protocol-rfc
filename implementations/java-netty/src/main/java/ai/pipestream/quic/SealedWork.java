package ai.pipestream.quic;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

/** Typed declaration contract for Section 9.8; producer labels are not credentials. */
public final class SealedWork {
  /** Negotiated private-use identifier, not an IANA assignment. */
  public static final int EXTENSION = 0xff01;
  /** WORK_SET unified control frame type. */
  public static final int FRAME = 0x83;
  /** Exact durable declaration acknowledgement. */
  public static final int ACK = 1;
  /** Final declaration batch for a scope. */
  public static final int SEAL = 2;
  /** Maximum identifiers in one declaration. */
  public static final int MAX_BATCH = 256;
  private static final long MAX_SCOPE = 0xffff_ffffL;

  private SealedWork() {}

  /**
   * A scope-qualified entity identifier. Values are validated at protocol entry points.
   * @param scopeId unsigned 32-bit scope identifier
   * @param entityId assignable entity identifier
   */
  public record EntityKey(long scopeId, long entityId) {}

  /**
   * An immutable declaration or its exact acknowledgement.
   *
   * @param sessionId stable bounded ASCII session identity
   * @param producerId nonzero 128-bit label, not authentication material
   * @param scopeId unsigned 32-bit scope; zero denotes the root
   * @param parent scope-qualified parent, absent only for the root
   * @param sequence unsigned 64-bit batch sequence
   * @param entityIds strictly increasing assignable identifiers
   * @param flags ACK and SEAL bits
   * @param sealDigest SHA-256 commitment, present exactly when SEAL is set
   */
  public record Declaration(String sessionId, UUID producerId, long scopeId, EntityKey parent,
      BigInteger sequence, List<Long> entityIds, int flags, byte[] sealDigest) {
    /** Defensively copies collection and digest inputs. */
    public Declaration {
      Objects.requireNonNull(sessionId, "sessionId");
      Objects.requireNonNull(producerId, "producerId");
      Objects.requireNonNull(sequence, "sequence");
      entityIds = List.copyOf(entityIds);
      sealDigest = sealDigest == null ? null : sealDigest.clone();
    }

    /** Returns a defensive digest copy.
     * @return 32-octet seal, or null for an unsealed batch
     */
    @Override public byte[] sealDigest() { return sealDigest == null ? null : sealDigest.clone(); }

    @Override public boolean equals(Object other) {
      return other instanceof Declaration that && sessionId.equals(that.sessionId)
          && producerId.equals(that.producerId) && scopeId == that.scopeId
          && Objects.equals(parent, that.parent) && sequence.equals(that.sequence)
          && entityIds.equals(that.entityIds) && flags == that.flags
          && Arrays.equals(sealDigest, that.sealDigest);
    }

    @Override public int hashCode() {
      return 31 * Objects.hash(sessionId, producerId, scopeId, parent, sequence, entityIds, flags)
          + Arrays.hashCode(sealDigest);
    }

    /** Creates the corresponding declaration acknowledgement.
     * @return the same declaration with ACK set, without implying payload admission
     */
    public Declaration acknowledgement() {
      return new Declaration(sessionId, producerId, scopeId, parent, sequence, entityIds, flags | ACK, sealDigest);
    }
  }

  /**
   * Encodes a declaration as a complete WORK_SET UCF.
   * @param declaration message to encode
   * @return deterministic binary frame
   * @throws ProtocolException for invalid fields or resource limits
   */
  public static byte[] encode(Declaration declaration) throws ProtocolException {
    validate(declaration);
    Map<String, Object> fields = new LinkedHashMap<>();
    fields.put("flags", declaration.flags);
    fields.put("scope-id", declaration.scopeId);
    fields.put("sequence", declaration.sequence);
    fields.put("session-id", declaration.sessionId);
    fields.put("producer-id", producerBytes(declaration.producerId));
    fields.put("entity-ids", declaration.entityIds);
    if (declaration.parent != null) {
      fields.put("parent-id", declaration.parent.entityId);
      fields.put("parent-scope-id", declaration.parent.scopeId);
    }
    if (declaration.sealDigest != null) fields.put("seal-digest", declaration.sealDigest);
    return Wire.encodeControl(FRAME, SealedCbor.encode(fields, Wire.MAX_CONTROL_FRAME));
  }

  /**
   * Decodes a complete WORK_SET UCF without changing durable state.
   * @param frame complete frame bytes
   * @return validated immutable declaration
   * @throws ProtocolException for malformed or non-deterministic input
   */
  public static Declaration decode(byte[] frame) throws ProtocolException {
    Wire.ControlFrame control = Wire.decodeControl(frame);
    if (control.type() != FRAME) throw Wire.frame("expected WORK_SET");
    return decodePayload(control.payload());
  }

  /**
   * Decodes the payload following a WORK_SET UCF header.
   * @param payload serialized message
   * @return validated immutable declaration
   * @throws ProtocolException for invalid fields or encoding
   */
  public static Declaration decodePayload(byte[] payload) throws ProtocolException {
    Map<String, Object> fields = SealedCbor.decode(payload, Wire.MAX_CONTROL_FRAME);
    only(fields, "flags", "scope-id", "sequence", "session-id", "producer-id", "entity-ids",
        "parent-id", "parent-scope-id", "seal-digest");
    byte[] producer = bytes(fields, "producer-id", 16);
    ByteBuffer label = ByteBuffer.wrap(producer);
    EntityKey parent = null;
    if (fields.containsKey("parent-id") != fields.containsKey("parent-scope-id")) {
      throw Wire.frame("parent fields must occur together");
    }
    if (fields.containsKey("parent-id")) parent = new EntityKey(
        bounded(fields, "parent-scope-id", MAX_SCOPE), bounded(fields, "parent-id", Wire.MAX_ENTITY_ID));
    if (!(fields.get("entity-ids") instanceof List<?> values) || values.size() > MAX_BATCH) {
      throw Wire.frame("invalid WORK_SET identifier array");
    }
    var ids = new java.util.ArrayList<Long>(values.size());
    for (Object value : values) {
      if (!(value instanceof BigInteger id) || id.signum() <= 0
          || id.compareTo(BigInteger.valueOf(Wire.MAX_ENTITY_ID)) > 0) throw Wire.frame("invalid declared identifier");
      ids.add(id.longValueExact());
    }
    Declaration declaration = new Declaration(text(fields, "session-id"), new UUID(label.getLong(), label.getLong()),
        bounded(fields, "scope-id", MAX_SCOPE), parent, uint(fields, "sequence"), ids,
        (int) bounded(fields, "flags", 3), fields.containsKey("seal-digest") ? bytes(fields, "seal-digest", 32) : null);
    validate(declaration);
    return declaration;
  }

  /**
   * Checks exact ACK correlation; accepting a changed response would lose the work contract.
   * @param request original request
   * @param response received response
   * @throws ProtocolException when either message is invalid or the response differs
   */
  public static void requireAcknowledgement(Declaration request, Declaration response) throws ProtocolException {
    validate(request);
    validate(response);
    if ((request.flags & ACK) != 0 || !request.acknowledgement().equals(response)) {
      throw Wire.entity("WORK_SET acknowledgement differs from request");
    }
  }

  /**
   * Computes the Section 9.8.3 seal independently of declaration batch boundaries.
   * @param sessionId stable session identity
   * @param producerId producer label
   * @param scopeId scope being sealed
   * @param parent parent binding, absent for root
   * @param identifiers entire strictly increasing set, not just the final batch
   * @return 32-octet SHA-256 commitment
   * @throws ProtocolException for invalid identity or ordering
   */
  public static byte[] sealDigest(String sessionId, UUID producerId, long scopeId, EntityKey parent,
      List<Long> identifiers) throws ProtocolException {
    validateIdentity(sessionId, producerId, scopeId, parent);
    ordered(identifiers);
    if (identifiers.isEmpty()) throw Wire.frame("cannot seal an empty work set");
    MessageDigest hash = sha256();
    hash.update("pipestream-work-set-v1".getBytes(StandardCharsets.US_ASCII));
    byte[] session = sessionId.getBytes(StandardCharsets.US_ASCII);
    hash.update(ByteBuffer.allocate(2).putShort((short) session.length).array());
    hash.update(session);
    hash.update(producerBytes(producerId));
    hash.update(ByteBuffer.allocate(4).putInt((int) scopeId).array());
    hash.update((byte) (parent == null ? 0 : 1));
    if (parent != null) hash.update(ByteBuffer.allocate(8).putInt((int) parent.scopeId).putInt((int) parent.entityId).array());
    hash.update(ByteBuffer.allocate(8).putLong(identifiers.size()).array());
    ByteBuffer id = ByteBuffer.allocate(4);
    for (long value : identifiers) { id.clear().putInt((int) value); hash.update(id.array()); }
    return hash.digest();
  }

  static void validate(Declaration declaration) throws ProtocolException {
    validateIdentity(declaration.sessionId, declaration.producerId, declaration.scopeId, declaration.parent);
    if (declaration.flags < 0 || declaration.flags > 3 || declaration.sequence.signum() < 0
        || declaration.sequence.compareTo(SealedCbor.MAX_UINT) > 0
        || declaration.entityIds.size() > MAX_BATCH
        || (declaration.entityIds.isEmpty() && (declaration.flags & SEAL) == 0)
        || ((declaration.flags & SEAL) != 0) != (declaration.sealDigest != null)
        || (declaration.sealDigest != null && declaration.sealDigest.length != 32)) {
      throw Wire.frame("invalid WORK_SET fields");
    }
    ordered(declaration.entityIds);
  }

  private static void validateIdentity(String session, UUID producer, long scope, EntityKey parent) throws ProtocolException {
    if (!validSessionId(session) || (producer.getMostSignificantBits() == 0 && producer.getLeastSignificantBits() == 0)
        || scope < 0 || scope > MAX_SCOPE || (scope == 0) != (parent == null)
        || (parent != null && (parent.scopeId < 0 || parent.scopeId > MAX_SCOPE
        || parent.entityId < 1 || parent.entityId > Wire.MAX_ENTITY_ID))) throw Wire.frame("invalid WORK_SET identity");
  }

  static boolean validSessionId(String id) {
    if (id == null || id.isEmpty() || id.length() > 128) return false;
    for (int i = 0; i < id.length(); i++) {
      char c = id.charAt(i);
      if (!(c >= 'A' && c <= 'Z') && !(c >= 'a' && c <= 'z') && !(c >= '0' && c <= '9') && "-_".indexOf(c) < 0) return false;
    }
    return true;
  }

  private static void ordered(List<Long> identifiers) throws ProtocolException {
    long previous = 0;
    for (long id : identifiers) {
      if (id <= previous || id > Wire.MAX_ENTITY_ID) throw Wire.frame("declaration IDs must be assignable and strictly increasing");
      previous = id;
    }
  }

  static byte[] producerBytes(UUID producer) {
    return ByteBuffer.allocate(16).putLong(producer.getMostSignificantBits()).putLong(producer.getLeastSignificantBits()).array();
  }

  static MessageDigest sha256() {
    try { return MessageDigest.getInstance("SHA-256"); }
    catch (NoSuchAlgorithmException exception) { throw new IllegalStateException("JDK lacks SHA-256", exception); }
  }

  static BigInteger uint(Map<String, Object> fields, String key) throws ProtocolException {
    if (!(fields.get(key) instanceof BigInteger value) || value.signum() < 0 || value.compareTo(SealedCbor.MAX_UINT) > 0) {
      throw Wire.frame("missing or invalid uint " + key);
    }
    return value;
  }

  static long bounded(Map<String, Object> fields, String key, long max) throws ProtocolException {
    BigInteger value = uint(fields, key);
    if (value.compareTo(BigInteger.valueOf(max)) > 0) throw Wire.frame("out-of-range " + key);
    return value.longValueExact();
  }

  static String text(Map<String, Object> fields, String key) throws ProtocolException {
    if (!(fields.get(key) instanceof String value)) throw Wire.frame("missing or invalid text " + key);
    return value;
  }

  static byte[] bytes(Map<String, Object> fields, String key, int size) throws ProtocolException {
    if (!(fields.get(key) instanceof byte[] value) || value.length != size) throw Wire.frame("missing or invalid bytes " + key);
    return value;
  }

  static void only(Map<String, Object> fields, String... keys) throws ProtocolException {
    for (String key : fields.keySet()) if (Arrays.stream(keys).noneMatch(key::equals)) throw Wire.frame("unknown field " + key);
  }
}
