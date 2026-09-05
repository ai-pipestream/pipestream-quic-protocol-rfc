package ai.pipestream.quic;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Independent transport messages for the negotiated sealed Layer 1 profile. */
public final class SealedTransport {
  private static final long MAX_SCOPE = 0xffff_ffffL;
  private SealedTransport() {}

  /**
   * Limits selected for a sealed session. Layer 1 and extension 65281 are required.
   * @param depth maximum nesting depth, zero through seven
   * @param entities maximum entities per scope
   * @param window maximum outstanding announcements
   * @param keepaliveMs idle heartbeat timeout in milliseconds
   */
  public record Limits(int depth, long entities, long window, BigInteger keepaliveMs) {
    /** Requires a non-null timeout. */
    public Limits { Objects.requireNonNull(keepaliveMs, "keepaliveMs"); }
    /** Returns conservative offer limits.
     * @return a fresh sealed-session offer
     */
    public static Limits defaults() { return new Limits(7, 16_384, 256, BigInteger.valueOf(30_000)); }
  }

  /**
   * A scope-qualified checkpoint, preserving optional members for exact ACK comparison.
   * @param id opaque request identifier
   * @param sequence unsigned 64-bit request sequence
   * @param lastId inclusive maximum declared identifier in the sealed scope
   * @param scopeId scope, or null for the root default
   * @param flags zero for a request, one for its ACK
   * @param timeoutMs unsigned timeout, or null for the 30-second default
   */
  public record Checkpoint(String id, BigInteger sequence, long lastId, Long scopeId,
      int flags, BigInteger timeoutMs) {
    /** Requires non-null request identity. */
    public Checkpoint { Objects.requireNonNull(id, "id"); Objects.requireNonNull(sequence, "sequence"); }
    /** Creates the exactly correlated ACK.
     * @return the same request fields with ACK set
     */
    public Checkpoint acknowledgement() { return new Checkpoint(id, sequence, lastId, scopeId, 1, timeoutMs); }
  }

  /**
   * One chunk's placement in an entity; all fields preserve wire uint64 values.
   * @param total total number of chunks, at least one
   * @param index zero-based chunk number
   * @param offset byte offset in the reassembled entity
   */
  public record Chunk(BigInteger total, BigInteger index, BigInteger offset) {}

  /**
   * A sealed entity header; its content layer is separate from the protocol layer.
   * @param key scope-qualified entity identity
   * @param parent parent binding, absent for root entities
   * @param layer application data layer, zero through three
   * @param contentType optional MIME type
   * @param payloadLength optional unsigned length of this stream's payload
   * @param checksum optional SHA-256 of this stream's payload
   * @param metadata application metadata, possibly empty
   * @param chunk optional chunk placement
   */
  public record Header(SealedWork.EntityKey key, SealedWork.EntityKey parent, int layer,
      String contentType, BigInteger payloadLength, byte[] checksum, Map<String, String> metadata, Chunk chunk) {
    /** Defensively copies metadata and checksum. */
    public Header {
      Objects.requireNonNull(key, "key"); metadata = Map.copyOf(metadata);
      checksum = checksum == null ? null : checksum.clone();
    }
    /** Returns the optional payload commitment.
     * @return a defensive copy, or null
     */
    @Override public byte[] checksum() { return checksum == null ? null : checksum.clone(); }
  }

  /**
   * A child-scope barrier request or response.
   * @param scopeId child scope
   * @param parentId parent identifier in the scope's declared parent scope
   * @param released whether the subtree barrier was crossed
   */
  public record Barrier(long scopeId, long parentId, boolean released) {}

  /**
   * Encodes capabilities requiring sealed work on every attachment, without fallback.
   * @param limits offered or selected resource limits
   * @return complete CAPABILITIES frame
   * @throws ProtocolException for invalid limits
   */
  public static byte[] capabilities(Limits limits) throws ProtocolException {
    validateLimits(limits);
    return Wire.encodeControl(Wire.FRAME_CAPABILITIES, SealedCbor.encode(Map.of(
        "layer0-core", true, "layer1-recursive", true, "layer2-resilience", false,
        "max-scope-depth", limits.depth, "max-entities-per-scope", limits.entities,
        "max-window-size", limits.window, "keepalive-timeout-ms", limits.keepaliveMs,
        "serialization-format", 0, "supported-extensions", List.of(SealedWork.EXTENSION),
        "required-extensions", List.of(SealedWork.EXTENSION)), Wire.MAX_CONTROL_FRAME));
  }

  /**
   * Validates a response against the exact sealed capability offer.
   * @param payload CAPABILITIES payload
   * @param offered client resource limits
   * @return selected limits
   * @throws ProtocolException for downgrade, unsolicited selection, or limit escalation
   */
  public static Limits response(byte[] payload, Limits offered) throws ProtocolException {
    validateLimits(offered);
    CapabilityFields fields = capabilityFields(payload);
    if (!fields.supported.contains(SealedWork.EXTENSION)) throw unsupported();
    if (!fields.supported.equals(List.of(SealedWork.EXTENSION)) || !fields.required.equals(List.of(SealedWork.EXTENSION))
        || !fields.layer1 || fields.layer2 || fields.serialization != 0
        || fields.limits.depth > offered.depth || fields.limits.entities > offered.entities
        || fields.limits.window > offered.window || fields.limits.keepaliveMs.compareTo(offered.keepaliveMs) > 0) {
      throw Wire.frame("server selected invalid sealed capabilities or increased offered limits");
    }
    return fields.limits;
  }

  /**
   * Selects a sealed client's offer using local limits; unknown requirements are refused.
   * @param payload CAPABILITIES payload
   * @param local server limits
   * @return selected sealed limits
   * @throws ProtocolException for unsupported profile or invalid offer
   */
  public static Limits negotiate(byte[] payload, Limits local) throws ProtocolException {
    validateLimits(local);
    CapabilityFields peer = capabilityFields(payload);
    if (!peer.layer1 || !peer.supported.contains(SealedWork.EXTENSION) || !peer.required.contains(SealedWork.EXTENSION)
        || peer.required.stream().anyMatch(id -> id != SealedWork.EXTENSION)) throw unsupported();
    return new Limits(Math.min(local.depth, peer.limits.depth), Math.min(local.entities, peer.limits.entities),
        Math.min(local.window, peer.limits.window), local.keepaliveMs.min(peer.limits.keepaliveMs));
  }

  /**
   * Encodes a scoped checkpoint without narrowing unsigned counters.
   * @param checkpoint request or response
   * @return complete CHECKPOINT frame
   * @throws ProtocolException for invalid fields
   */
  public static byte[] checkpoint(Checkpoint checkpoint) throws ProtocolException {
    validateCheckpoint(checkpoint);
    Map<String, Object> fields = new LinkedHashMap<>();
    fields.put("checkpoint-id", checkpoint.id); fields.put("sequence-number", checkpoint.sequence);
    fields.put("checkpoint-entity-id", checkpoint.lastId); fields.put("flags", checkpoint.flags);
    if (checkpoint.scopeId != null) fields.put("scope-id", checkpoint.scopeId);
    if (checkpoint.timeoutMs != null) fields.put("timeout-ms", checkpoint.timeoutMs);
    return Wire.encodeControl(Wire.FRAME_CHECKPOINT, SealedCbor.encode(fields, Wire.MAX_CONTROL_FRAME));
  }

  /**
   * Decodes and validates a scoped checkpoint payload.
   * @param payload CHECKPOINT payload
   * @return typed request or ACK
   * @throws ProtocolException for malformed fields or encoding
   */
  public static Checkpoint checkpoint(byte[] payload) throws ProtocolException {
    Map<String, Object> fields = SealedCbor.decode(payload, Wire.MAX_CONTROL_FRAME);
    SealedWork.only(fields, "checkpoint-id", "sequence-number", "checkpoint-entity-id", "flags", "scope-id", "timeout-ms");
    Checkpoint result = new Checkpoint(SealedWork.text(fields, "checkpoint-id"), SealedWork.uint(fields, "sequence-number"),
        SealedWork.bounded(fields, "checkpoint-entity-id", Wire.MAX_ENTITY_ID),
        fields.containsKey("scope-id") ? SealedWork.bounded(fields, "scope-id", MAX_SCOPE) : null,
        fields.containsKey("flags") ? (int) SealedWork.bounded(fields, "flags", 1) : 0,
        fields.containsKey("timeout-ms") ? SealedWork.uint(fields, "timeout-ms") : null);
    validateCheckpoint(result); return result;
  }

  /**
   * Encodes an EntityHeader preceded by its four-octet length, but no payload bytes.
   * @param header identity and payload metadata
   * @return header prefix suitable for incremental streaming
   * @throws ProtocolException for invalid header fields
   */
  public static byte[] header(Header header) throws ProtocolException {
    validateHeader(header);
    Map<String, Object> fields = new LinkedHashMap<>();
    fields.put("entity-id", header.key.entityId()); fields.put("scope-id", header.key.scopeId()); fields.put("layer", header.layer);
    if (header.parent != null) { fields.put("parent-id", header.parent.entityId()); fields.put("parent-scope-id", header.parent.scopeId()); }
    if (header.contentType != null) fields.put("content-type", header.contentType);
    if (header.payloadLength != null) fields.put("payload-length", header.payloadLength);
    if (header.checksum != null) fields.put("checksum", header.checksum);
    if (!header.metadata.isEmpty()) fields.put("metadata", header.metadata);
    if (header.chunk != null) fields.put("chunk-info", Map.of("total-chunks", header.chunk.total, "chunk-index", header.chunk.index, "chunk-offset", header.chunk.offset));
    byte[] encoded = SealedCbor.encode(fields, Wire.MAX_ENTITY_HEADER);
    return ByteBuffer.allocate(4 + encoded.length).putInt(encoded.length).put(encoded).array();
  }

  /**
   * Decodes header bytes without allocating or reading the entity payload.
   * @param encoded serialized header, excluding its length prefix
   * @return validated sealed header
   * @throws ProtocolException for wrong shape, identity, or Layer 2 policy
   */
  public static Header header(byte[] encoded) throws ProtocolException {
    Map<String, Object> fields = SealedCbor.decode(encoded, Wire.MAX_ENTITY_HEADER);
    SealedWork.only(fields, "entity-id", "scope-id", "parent-id", "parent-scope-id", "layer", "content-type", "payload-length", "checksum", "metadata", "chunk-info", "completion-policy");
    if (fields.containsKey("completion-policy")) throw Wire.layerUnsupported("sealed profile excludes Layer 2 policies");
    long scope = fields.containsKey("scope-id") ? SealedWork.bounded(fields, "scope-id", MAX_SCOPE) : 0;
    if (fields.containsKey("parent-id") != fields.containsKey("parent-scope-id")) throw SealedScope.invalid("sealed parent fields must occur together");
    SealedWork.EntityKey parent = fields.containsKey("parent-id") ? new SealedWork.EntityKey(
        SealedWork.bounded(fields, "parent-scope-id", MAX_SCOPE), SealedWork.bounded(fields, "parent-id", Wire.MAX_ENTITY_ID)) : null;
    Map<String, String> metadata = new LinkedHashMap<>();
    if (fields.containsKey("metadata")) {
      Map<String, Object> map = map(fields.get("metadata"));
      for (String name : map.keySet()) metadata.put(name, SealedWork.text(map, name));
    }
    Chunk chunk = null;
    if (fields.containsKey("chunk-info")) {
      Map<String, Object> map = map(fields.get("chunk-info"));
      SealedWork.only(map, "total-chunks", "chunk-index", "chunk-offset");
      chunk = new Chunk(SealedWork.uint(map, "total-chunks"), SealedWork.uint(map, "chunk-index"), SealedWork.uint(map, "chunk-offset"));
    }
    Header result = new Header(new SealedWork.EntityKey(scope, SealedWork.bounded(fields, "entity-id", Wire.MAX_ENTITY_ID)), parent,
        (int) SealedWork.bounded(fields, "layer", 3), fields.containsKey("content-type") ? SealedWork.text(fields, "content-type") : null,
        fields.containsKey("payload-length") ? SealedWork.uint(fields, "payload-length") : null,
        fields.containsKey("checksum") ? SealedWork.bytes(fields, "checksum", 32) : null, metadata, chunk);
    validateHeader(result); return result;
  }

  /**
   * Decodes scoped statuses, rejecting cursor recycling and Layer 2 states.
   * Unknown status extensions are skipped using their validated length.
   * @param payload STATUS payload
   * @return validated status
   * @throws ProtocolException for wrong version, layout, or identity
   */
  public static Wire.Status status(byte[] payload) throws ProtocolException {
    if (payload.length < 16) throw Wire.frame("short STATUS");
    ByteBuffer bytes = ByteBuffer.wrap(payload); int word = bytes.getInt();
    if (word >>> 28 != 1) throw Wire.layerUnsupported("unsupported STATUS version");
    boolean extension = (word & (1 << 23)) != 0, cursor = (word & (1 << 22)) != 0;
    int base = cursor ? 20 : 16;
    if (extension) {
      if (payload.length < base + 4 || Integer.toUnsignedLong(bytes.getInt(base)) != payload.length - base - 4L
          || payload.length == base + 4) throw Wire.frame("STATUS extension length mismatch");
    } else if (payload.length != base) throw Wire.frame("STATUS length mismatch");
    if (cursor) throw Wire.entity("sealed profile forbids cursor recycling");
    int state = (word >>> 24) & 15, depth = (word >>> 19) & 7;
    long entity = Integer.toUnsignedLong(bytes.getInt()), scope = Integer.toUnsignedLong(bytes.getInt());
    if (state >= 8) throw Wire.layerUnsupported("sealed profile excludes Layer 2 status");
    if (state == 0) {
      if (entity != Wire.CONNECTION_LEVEL || scope != 0 || depth != 0) throw Wire.entity("invalid heartbeat identity");
    } else if (entity < 1 || entity > Wire.MAX_ENTITY_ID || (scope == 0) != (depth == 0)) throw Wire.entity("invalid status identity or depth");
    return new Wire.Status(state, entity, scope, null, depth);
  }

  /**
   * Encodes a child-scope barrier.
   * @param barrier request or response
   * @return complete BARRIER frame
   * @throws ProtocolException for invalid identifiers
   */
  public static byte[] barrier(Barrier barrier) throws ProtocolException {
    SealedScope.childScope(barrier.scopeId); entityId(barrier.parentId);
    return Wire.encodeControl(0x55, ByteBuffer.allocate(12).putInt(barrier.released ? 0x8000_0000 : 0)
        .putInt((int) barrier.scopeId).putInt((int) barrier.parentId).array());
  }

  /**
   * Decodes a barrier, ignoring reserved bits as specified.
   * @param payload BARRIER payload
   * @return validated request or response
   * @throws ProtocolException for invalid layout or scope
   */
  public static Barrier barrier(byte[] payload) throws ProtocolException {
    if (payload.length != 12) throw Wire.frame("BARRIER must contain 12 octets");
    ByteBuffer bytes = ByteBuffer.wrap(payload);
    boolean released = bytes.getInt() < 0;
    Barrier result = new Barrier(Integer.toUnsignedLong(bytes.getInt()), Integer.toUnsignedLong(bytes.getInt()), released);
    SealedScope.childScope(result.scopeId); entityId(result.parentId); return result;
  }

  private static void validateHeader(Header header) throws ProtocolException {
    entityId(header.key.entityId());
    if (header.key.scopeId() < 0 || header.key.scopeId() > MAX_SCOPE || (header.key.scopeId() == 0) != (header.parent == null)) throw SealedScope.invalid("invalid sealed scope binding");
    if (header.parent != null) {
      entityId(header.parent.entityId());
      if (header.parent.scopeId() < 0 || header.parent.scopeId() > MAX_SCOPE || header.parent.scopeId() == header.key.scopeId()) throw SealedScope.invalid("invalid parent scope");
    }
    if (header.layer < 0 || header.layer > 3) throw Wire.entity("invalid data layer");
    if (header.payloadLength != null) unsigned(header.payloadLength);
    if (header.checksum != null && header.checksum.length != 32) throw Wire.integrity("checksum must be SHA-256");
    if (header.chunk != null) {
      unsigned(header.chunk.total); unsigned(header.chunk.index); unsigned(header.chunk.offset);
      if (header.chunk.total.signum() == 0 || header.chunk.index.compareTo(header.chunk.total) >= 0) throw Wire.entity("invalid chunk index or count");
    }
  }

  private static void validateCheckpoint(Checkpoint checkpoint) throws ProtocolException {
    entityId(checkpoint.lastId); unsigned(checkpoint.sequence);
    if (checkpoint.timeoutMs != null) unsigned(checkpoint.timeoutMs);
    if (checkpoint.flags < 0 || checkpoint.flags > 1 || (checkpoint.scopeId != null && (checkpoint.scopeId < 0 || checkpoint.scopeId > MAX_SCOPE))) throw Wire.frame("invalid checkpoint flags or scope");
  }

  private static void validateLimits(Limits limits) throws ProtocolException {
    unsigned(limits.keepaliveMs);
    if (limits.depth < 0 || limits.depth > 7 || limits.entities < 1 || limits.entities > Wire.MAX_ENTITY_ID
        || limits.window < 1 || limits.window > Wire.MAX_WINDOW) throw Wire.limit("invalid negotiated limits");
  }

  private static CapabilityFields capabilityFields(byte[] payload) throws ProtocolException {
    Map<String, Object> fields = SealedCbor.decode(payload, Wire.MAX_CONTROL_FRAME);
    SealedWork.only(fields, "layer0-core", "layer1-recursive", "layer2-resilience", "max-scope-depth", "max-entities-per-scope", "max-window-size", "keepalive-timeout-ms", "serialization-format", "supported-extensions", "required-extensions");
    if (!bool(fields, "layer0-core")) throw Wire.layerUnsupported("Layer 0 is mandatory");
    Limits limits = new Limits((int) defaultBound(fields, "max-scope-depth", 7, 7),
        defaultBound(fields, "max-entities-per-scope", Wire.MAX_ENTITY_ID, Wire.MAX_ENTITY_ID),
        defaultBound(fields, "max-window-size", Wire.MAX_WINDOW, Wire.MAX_WINDOW),
        fields.containsKey("keepalive-timeout-ms") ? SealedWork.uint(fields, "keepalive-timeout-ms") : BigInteger.valueOf(30_000));
    validateLimits(limits);
    List<Integer> supported = extensions(fields, "supported-extensions"), required = extensions(fields, "required-extensions");
    if (!supported.containsAll(required)) throw Wire.frame("required extension is not supported");
    return new CapabilityFields(limits, bool(fields, "layer1-recursive"), bool(fields, "layer2-resilience"),
        (int) defaultBound(fields, "serialization-format", 255, 0), supported, required);
  }

  private record CapabilityFields(Limits limits, boolean layer1, boolean layer2, int serialization, List<Integer> supported, List<Integer> required) {}

  private static long defaultBound(Map<String, Object> fields, String key, long max, long defaultValue) throws ProtocolException {
    return fields.containsKey(key) ? SealedWork.bounded(fields, key, max) : defaultValue;
  }

  private static boolean bool(Map<String, Object> fields, String key) throws ProtocolException {
    if (!(fields.get(key) instanceof Boolean value)) throw Wire.frame("missing or invalid " + key);
    return value;
  }

  private static List<Integer> extensions(Map<String, Object> fields, String key) throws ProtocolException {
    if (!fields.containsKey(key)) return List.of();
    if (!(fields.get(key) instanceof List<?> values) || values.size() > 32) throw Wire.frame("invalid extension list");
    List<Integer> result = new ArrayList<>(); int previous = 0;
    for (Object item : values) {
      if (!(item instanceof BigInteger value) || value.compareTo(BigInteger.valueOf(previous)) <= 0 || value.compareTo(BigInteger.valueOf(65535)) >= 0) throw Wire.frame("invalid extension ordering or identifier");
      previous = value.intValueExact(); result.add(previous);
    }
    return List.copyOf(result);
  }

  private static Map<String, Object> map(Object value) throws ProtocolException {
    if (!(value instanceof Map<?, ?> input)) throw Wire.frame("expected nested map");
    Map<String, Object> result = new LinkedHashMap<>();
    for (var entry : input.entrySet()) {
      if (!(entry.getKey() instanceof String key)) throw Wire.frame("expected text key");
      result.put(key, entry.getValue());
    }
    return result;
  }

  private static void unsigned(BigInteger value) throws ProtocolException {
    if (value == null || value.signum() < 0 || value.compareTo(SealedCbor.MAX_UINT) > 0) throw Wire.frame("expected uint64");
  }

  private static void entityId(long id) throws ProtocolException {
    if (id < 1 || id > Wire.MAX_ENTITY_ID) throw Wire.entity("reserved entity identifier");
  }

  private static ProtocolException unsupported() {
    return new ProtocolException(Wire.ERROR_EXTENSION_UNSUPPORTED, "PIPESTREAM_EXTENSION_UNSUPPORTED", "sealed profile is required");
  }
}
