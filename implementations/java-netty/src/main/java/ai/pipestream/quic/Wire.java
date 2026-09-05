package ai.pipestream.quic;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.StreamReadFeature;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.cbor.CBORFactory;
import com.fasterxml.jackson.dataformat.cbor.CBORGenerator;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

/** Independent Java codec for the PipeStream Layer 0 wire contract. */
public final class Wire {
  public static final String ALPN = "pipestream/1";
  public static final int FRAME_STATUS = 0x50;
  public static final int FRAME_GOAWAY = 0x56;
  public static final int FRAME_CAPABILITIES = 0x80;
  public static final int FRAME_CHECKPOINT = 0x81;

  public static final int STATUS_UNSPECIFIED = 0;
  public static final int STATUS_PENDING = 1;
  public static final int STATUS_PROCESSING = 2;
  public static final int STATUS_COMPLETE = 3;
  public static final int STATUS_FAILED = 4;
  public static final int CHECKPOINT_ACK = 1;

  public static final long CONNECTION_LEVEL = 0xffff_ffffL;
  public static final long MAX_ENTITY_ID = 0xffff_fffcL;
  public static final long MAX_WINDOW = 0x7fff_fffeL;
  public static final int MAX_CONTROL_FRAME = 1 << 20;
  public static final int MAX_ENTITY_HEADER = 1 << 16;
  public static final int MAX_PAYLOAD = 64 << 20;

  public static final long ERROR_NO_ERROR = 0x00;
  public static final long ERROR_INTEGRITY = 0x04;
  public static final long ERROR_ENTITY_INVALID = 0x05;
  public static final long ERROR_LIMIT_EXCEEDED = 0x06;
  public static final long ERROR_LAYER_UNSUPPORTED = 0x0c;
  public static final long ERROR_FRAME = 0x0d;
  public static final long ERROR_EXTENSION_UNSUPPORTED = 0x0f;

  private static final TypeReference<LinkedHashMap<String, Object>> MAP_TYPE = new TypeReference<>() {};
  private static final ObjectMapper CBOR = new ObjectMapper(
      CBORFactory.builder()
          .enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION)
          .enable(CBORGenerator.Feature.WRITE_MINIMAL_INTS)
          .build());

  static {
    CBOR.getFactory().configure(JsonGenerator.Feature.AUTO_CLOSE_TARGET, false);
  }

  private Wire() {}

  /** Negotiated Layer 0 capabilities. */
  public record Capabilities(
      boolean layer0Core,
      boolean layer1Recursive,
      boolean layer2Resilience,
      long maxWindowSize,
      int serializationFormat,
      long keepaliveTimeoutMs,
      List<Integer> supportedExtensions,
      List<Integer> requiredExtensions) {
    public Capabilities {
      supportedExtensions = List.copyOf(supportedExtensions);
      requiredExtensions = List.copyOf(requiredExtensions);
    }

    public Capabilities(boolean layer0Core, boolean layer1Recursive, boolean layer2Resilience,
        long maxWindowSize, int serializationFormat, long keepaliveTimeoutMs) {
      this(layer0Core, layer1Recursive, layer2Resilience, maxWindowSize, serializationFormat,
          keepaliveTimeoutMs, List.of(), List.of());
    }

    private void validateExtensions() throws ProtocolException {
      for (List<Integer> ids : List.of(supportedExtensions, requiredExtensions)) {
        int previous = 0;
        if (ids.size() > 32) throw frame("too many extension identifiers");
        for (int id : ids) {
          if (id <= previous || id >= 65535) throw frame("invalid extension identifier list");
          previous = id;
        }
      }
      if (!supportedExtensions.containsAll(requiredExtensions)) {
        throw frame("required extension not advertised as supported");
      }
    }

    public void validateResponse(Capabilities response) throws ProtocolException {
      validateExtensions();
      response.validateExtensions();
      if (!supportedExtensions.containsAll(response.supportedExtensions)) {
        throw frame("server selected an unoffered extension");
      }
      if (!response.supportedExtensions.containsAll(requiredExtensions)) throw extensionUnsupported();
      if (!response.requiredExtensions.containsAll(requiredExtensions)) {
        throw frame("server omitted a client requirement");
      }
      if (!response.layer0Core || (response.layer1Recursive && !layer1Recursive)
          || (response.layer2Resilience && (!layer2Resilience || !response.layer1Recursive))
          || response.maxWindowSize < 1 || response.maxWindowSize > maxWindowSize
          || response.keepaliveTimeoutMs > keepaliveTimeoutMs || response.serializationFormat != 0) {
        throw frame("server exceeded offered capabilities");
      }
    }
    /** @return conservative reference capabilities */
    public static Capabilities defaults() {
      return new Capabilities(true, false, false, 1024, 0, 30_000);
    }

    /**
     * Negotiates local and peer limits.
     *
     * @param peer peer capabilities
     * @return negotiated capabilities
     * @throws ProtocolException when Layer 0 or resource constraints are invalid
     */
    public Capabilities negotiate(Capabilities peer) throws ProtocolException {
      validateExtensions();
      peer.validateExtensions();
      if (!supportedExtensions.containsAll(peer.requiredExtensions)
          || !peer.supportedExtensions.containsAll(requiredExtensions)) throw extensionUnsupported();
      List<Integer> selected = supportedExtensions.stream().filter(peer.supportedExtensions::contains).toList();
      TreeSet<Integer> required = new TreeSet<>(requiredExtensions);
      required.addAll(peer.requiredExtensions);
      if (!peer.layer0Core) {
        throw layerUnsupported("Layer 0 is mandatory");
      }
      if (peer.maxWindowSize < 1 || peer.maxWindowSize > MAX_WINDOW) {
        throw limit("invalid max-window-size");
      }
      boolean layer1 = layer1Recursive && peer.layer1Recursive;
      return new Capabilities(
          true,
          layer1,
          layer1 && layer2Resilience && peer.layer2Resilience,
          Math.min(maxWindowSize, peer.maxWindowSize),
          0,
          Math.min(keepaliveTimeoutMs, peer.keepaliveTimeoutMs), selected, List.copyOf(required));
    }
  }

  /** Layer 0 entity header fields implemented by this reference. */
  public record EntityHeader(
      long entityId,
      Long parentId,
      int layer,
      String contentType,
      Long payloadLength,
      byte[] checksum) {
    public EntityHeader {
      checksum = checksum == null ? null : checksum.clone();
    }

    @Override
    public byte[] checksum() {
      return checksum == null ? null : checksum.clone();
    }
  }

  /** Decoded status frame. */
  public record Status(int state, long entityId, long scopeId, Long cursor, int depth) {}

  /** Decoded Layer 0 checkpoint request or acknowledgement. */
  public record Checkpoint(
      String checkpointId,
      long sequenceNumber,
      long checkpointEntityId,
      Long scopeId,
      int flags,
      Long timeoutMs) {}

  /** Decoded UCF. */
  public record ControlFrame(int type, byte[] payload) {
    public ControlFrame {
      payload = payload.clone();
    }

    @Override
    public byte[] payload() {
      return payload.clone();
    }
  }

  /** Decoded entity frame. */
  public record Entity(EntityHeader header, byte[] payload) {
    public Entity {
      payload = payload.clone();
    }

    @Override
    public byte[] payload() {
      return payload.clone();
    }
  }

  /**
   * Encodes a complete UCF.
   *
   * @param type one-octet frame type
   * @param payload frame payload
   * @return encoded UCF
   */
  public static byte[] encodeControl(int type, byte[] payload) {
    if (type < 0 || type > 0xff) {
      throw new IllegalArgumentException("frame type must fit one octet");
    }
    ByteBuffer output = ByteBuffer.allocate(5 + payload.length).order(ByteOrder.BIG_ENDIAN);
    output.put((byte) type).putInt(payload.length).put(payload);
    return output.array();
  }

  /**
   * Decodes exactly one UCF.
   *
   * @param data encoded frame
   * @return decoded control frame
   * @throws ProtocolException when length or limits are invalid
   */
  public static ControlFrame decodeControl(byte[] data) throws ProtocolException {
    if (data.length < 5) {
      throw frame("truncated UCF header");
    }
    ByteBuffer input = ByteBuffer.wrap(data).order(ByteOrder.BIG_ENDIAN);
    int type = Byte.toUnsignedInt(input.get());
    long length = Integer.toUnsignedLong(input.getInt());
    if (length > MAX_CONTROL_FRAME) {
      throw limit("control frame exceeds local limit");
    }
    if (length != input.remaining()) {
      throw frame("UCF length does not match payload");
    }
    byte[] payload = new byte[(int) length];
    input.get(payload);
    return new ControlFrame(type, payload);
  }

  /** @return encoded default capabilities UCF */
  public static byte[] encodeCapabilities(Capabilities capabilities) throws ProtocolException {
    capabilities.validateExtensions();
    LinkedHashMap<String, Object> map = new LinkedHashMap<>();
    map.put("layer0-core", capabilities.layer0Core);
    map.put("max-window-size", capabilities.maxWindowSize);
    map.put("layer1-recursive", capabilities.layer1Recursive);
    map.put("layer2-resilience", capabilities.layer2Resilience);
    map.put("keepalive-timeout-ms", capabilities.keepaliveTimeoutMs);
    map.put("serialization-format", capabilities.serializationFormat);
    if (!capabilities.supportedExtensions.isEmpty()) map.put("supported-extensions", capabilities.supportedExtensions);
    if (!capabilities.requiredExtensions.isEmpty()) map.put("required-extensions", capabilities.requiredExtensions);
    return encodeControl(FRAME_CAPABILITIES, encodeMap(map));
  }

  /**
   * Decodes and validates capability CBOR.
   *
   * @param payload capability UCF payload
   * @return capabilities
   * @throws ProtocolException on invalid CBOR or constraints
   */
  public static Capabilities decodeCapabilities(byte[] payload) throws ProtocolException {
    LinkedHashMap<String, Object> map = decodeMap(payload);
    boolean layer0 = requiredBoolean(map, "layer0-core");
    boolean layer1 = requiredBoolean(map, "layer1-recursive");
    boolean layer2 = requiredBoolean(map, "layer2-resilience");
    long maxWindow = number(map, "max-window-size", MAX_WINDOW);
    long serializationValue = number(map, "serialization-format", 0);
    if (serializationValue < 0 || serializationValue > 255) {
      throw frame("serialization-format exceeds uint8");
    }
    int serialization = (int) serializationValue;
    long keepalive = number(map, "keepalive-timeout-ms", 30_000);
    rejectUnknown(map,
        "layer0-core", "max-window-size", "layer1-recursive", "layer2-resilience",
        "keepalive-timeout-ms", "serialization-format", "max-scope-depth", "max-entities-per-scope",
        "supported-extensions", "required-extensions");
    Capabilities result = new Capabilities(layer0, layer1, layer2, maxWindow, serialization, keepalive,
        extensionList(map, "supported-extensions"), extensionList(map, "required-extensions"));
    result.validateExtensions();
    if (!layer0) {
      throw layerUnsupported("Layer 0 is mandatory");
    }
    if (maxWindow < 1 || maxWindow > MAX_WINDOW) {
      throw limit("invalid max-window-size");
    }
    long depth = number(map, "max-scope-depth", 7);
    long entities = number(map, "max-entities-per-scope", MAX_ENTITY_ID);
    if (depth < 0 || depth > 7 || entities < 1 || entities > MAX_ENTITY_ID) {
      throw limit("invalid recursive capability limit");
    }
    if (serialization < 0 || serialization > 255 || keepalive < 0) {
      throw frame("invalid serialization format or keepalive");
    }
    if (!Arrays.equals(encodeMap(map), payload)) {
      throw frame("capabilities CBOR is not deterministic");
    }
    return result;
  }

  /** @return encoded checkpoint request or acknowledgement UCF */
  public static byte[] encodeCheckpoint(Checkpoint checkpoint) throws ProtocolException {
    LinkedHashMap<String, Object> map = new LinkedHashMap<>();
    map.put("flags", checkpoint.flags);
    if (checkpoint.scopeId != null) {
      map.put("scope-id", checkpoint.scopeId);
    }
    if (checkpoint.timeoutMs != null) {
      map.put("timeout-ms", checkpoint.timeoutMs);
    }
    map.put("checkpoint-id", checkpoint.checkpointId);
    map.put("sequence-number", checkpoint.sequenceNumber);
    map.put("checkpoint-entity-id", checkpoint.checkpointEntityId);
    return encodeControl(FRAME_CHECKPOINT, encodeMap(map));
  }

  /** @return decoded and validated Layer 0 checkpoint */
  public static Checkpoint decodeCheckpoint(byte[] payload) throws ProtocolException {
    LinkedHashMap<String, Object> map = decodeMap(payload);
    Object checkpointIdValue = map.get("checkpoint-id");
    if (!(checkpointIdValue instanceof String checkpointId)
        || checkpointId.isEmpty()
        || checkpointId.getBytes(java.nio.charset.StandardCharsets.UTF_8).length > 256) {
      throw frame("invalid checkpoint-id");
    }
    long sequence = checkpointNumber(map, "sequence-number");
    long checkpointEntity = checkpointNumber(map, "checkpoint-entity-id");
    Long scope = optionalCheckpointNumber(map, "scope-id");
    long flagsValue = number(map, "flags", 0);
    Long timeout = optionalCheckpointNumber(map, "timeout-ms");
    rejectUnknown(
        map,
        "flags",
        "scope-id",
        "timeout-ms",
        "checkpoint-id",
        "sequence-number",
        "checkpoint-entity-id");
    if (checkpointEntity < 1 || checkpointEntity > MAX_ENTITY_ID) {
      throw entity("invalid checkpoint-entity-id");
    }
    if (scope != null && scope != 0) {
      throw layerUnsupported("checkpoint scope requires Layer 1");
    }
    if (flagsValue < 0 || flagsValue > CHECKPOINT_ACK) {
      throw frame("unknown checkpoint flags");
    }
    Checkpoint result = new Checkpoint(
        checkpointId, sequence, checkpointEntity, scope, Math.toIntExact(flagsValue), timeout);
    if (!Arrays.equals(encodeMap(map), payload)) {
      throw frame("checkpoint CBOR is not deterministic");
    }
    return result;
  }

  /**
   * Encodes a status UCF.
   *
   * @param status status fields
   * @return encoded status frame
   */
  public static byte[] encodeStatus(Status status) {
    if (status.depth < 0 || status.depth > 7) {
      throw new IllegalArgumentException("depth must be 0 through 7");
    }
    int word = (1 << 28) | ((status.state & 0xf) << 24) | (status.depth << 19);
    if (status.cursor != null) {
      word |= 1 << 22;
    }
    ByteBuffer payload = ByteBuffer.allocate(status.cursor == null ? 16 : 20).order(ByteOrder.BIG_ENDIAN);
    payload.putInt(word).putInt((int) status.entityId).putInt((int) status.scopeId).putInt(0);
    if (status.cursor != null) {
      payload.putInt(status.cursor.intValue());
    }
    return encodeControl(FRAME_STATUS, payload.array());
  }

  /** @return decoded Layer 0 status */
  public static Status decodeStatus(byte[] payload) throws ProtocolException {
    if (payload.length != 16 && payload.length != 20) {
      throw frame("invalid STATUS payload length");
    }
    ByteBuffer input = ByteBuffer.wrap(payload).order(ByteOrder.BIG_ENDIAN);
    int word = input.getInt();
    int version = word >>> 28;
    if (version != 1) {
      throw layerUnsupported("unsupported STATUS version");
    }
    if ((word & (1 << 23)) != 0) {
      throw frame("Layer 0 STATUS cannot carry extensions");
    }
    boolean hasCursor = (word & (1 << 22)) != 0;
    if (hasCursor != (payload.length == 20)) {
      throw frame("STATUS cursor flag and length disagree");
    }
    int state = (word >>> 24) & 0xf;
    int depth = (word >>> 19) & 0x7;
    long entityId = Integer.toUnsignedLong(input.getInt());
    long scopeId = Integer.toUnsignedLong(input.getInt());
    input.getInt();
    if (depth != 0 || scopeId != 0) {
      throw layerUnsupported("scope fields require Layer 1");
    }
    if (state >= 8) {
      throw layerUnsupported("status requires Layer 2");
    }
    if (state == STATUS_UNSPECIFIED && entityId != CONNECTION_LEVEL) {
      throw entity("UNSPECIFIED is connection-level only");
    }
    Long cursor = hasCursor ? Integer.toUnsignedLong(input.getInt()) : null;
    if (cursor != null
        && (state != STATUS_UNSPECIFIED
            || entityId != CONNECTION_LEVEL
            || scopeId != 0
            || depth != 0)) {
      throw entity("cursor update must be connection-level");
    }
    return new Status(state, entityId, scopeId, cursor, depth);
  }

  /** @return encoded GOAWAY UCF */
  public static byte[] encodeGoaway(long lastEntityId) {
    ByteBuffer payload = ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN);
    payload.putInt(0).putInt((int) lastEntityId);
    return encodeControl(FRAME_GOAWAY, payload.array());
  }

  /** @return last entity identifier from a GOAWAY payload */
  public static long decodeGoaway(byte[] payload) throws ProtocolException {
    if (payload.length != 8) {
      throw frame("invalid GOAWAY payload length");
    }
    return Integer.toUnsignedLong(ByteBuffer.wrap(payload, 4, 4).order(ByteOrder.BIG_ENDIAN).getInt());
  }

  /** @return the next assignable Entity ID in the Layer 0 circular space */
  public static long nextEntityId(long current) throws ProtocolException {
    if (current < 1 || current > MAX_ENTITY_ID) {
      throw entity("entity-id is reserved");
    }
    return current == MAX_ENTITY_ID ? 1 : current + 1;
  }

  /** @return encoded entity frame with SHA-256 and exact length */
  public static byte[] encodeEntity(long entityId, byte[] payload, String contentType)
      throws ProtocolException {
    return encodeEntity(entityId, null, payload, contentType);
  }

  /** @return encoded child entity frame with SHA-256 and exact length */
  public static byte[] encodeEntity(
      long entityId, Long parentId, byte[] payload, String contentType)
      throws ProtocolException {
    if (entityId < 1 || entityId > MAX_ENTITY_ID) {
      throw entity("entity-id is reserved");
    }
    if (parentId != null && (parentId < 1 || parentId > MAX_ENTITY_ID)) {
      throw entity("parent-id is reserved or invalid");
    }
    byte[] checksum = sha256(payload);
    LinkedHashMap<String, Object> map = new LinkedHashMap<>();
    map.put("layer", 0);
    map.put("checksum", checksum);
    map.put("entity-id", entityId);
    if (parentId != null) {
      map.put("parent-id", parentId);
    }
    map.put("content-type", contentType);
    map.put("payload-length", payload.length);
    byte[] header = encodeMap(map);
    ByteBuffer output = ByteBuffer.allocate(4 + header.length + payload.length).order(ByteOrder.BIG_ENDIAN);
    output.putInt(header.length).put(header).put(payload);
    return output.array();
  }

  /** @return validated entity frame */
  public static Entity decodeEntity(byte[] data) throws ProtocolException {
    if (data.length < 4) {
      throw frame("truncated entity header length");
    }
    ByteBuffer input = ByteBuffer.wrap(data).order(ByteOrder.BIG_ENDIAN);
    long headerLength = Integer.toUnsignedLong(input.getInt());
    if (headerLength > MAX_ENTITY_HEADER) {
      throw limit("entity header exceeds local limit");
    }
    if (headerLength > input.remaining()) {
      throw frame("truncated entity header");
    }
    byte[] encodedHeader = new byte[(int) headerLength];
    input.get(encodedHeader);
    byte[] payload = new byte[input.remaining()];
    input.get(payload);
    if (payload.length > MAX_PAYLOAD) {
      throw limit("entity payload exceeds local limit");
    }
    LinkedHashMap<String, Object> map = decodeMap(encodedHeader);
    long entityId = requiredNumber(map, "entity-id");
    Long parentId = optionalNumber(map, "parent-id");
    int layer = Math.toIntExact(requiredNumber(map, "layer"));
    String contentType = optionalString(map, "content-type");
    Long payloadLength = optionalNumber(map, "payload-length");
    byte[] checksum = optionalBytes(map, "checksum");
    rejectUnknown(
        map,
        "layer",
        "checksum",
        "entity-id",
        "parent-id",
        "content-type",
        "payload-length");
    if (entityId < 1 || entityId > MAX_ENTITY_ID) {
      throw entity("entity-id is reserved");
    }
    if (parentId != null && (parentId < 1 || parentId > MAX_ENTITY_ID)) {
      throw entity("parent-id is reserved or invalid");
    }
    if (layer < 0 || layer > 3) {
      throw entity("layer must be 0 through 3");
    }
    if (payloadLength != null && payloadLength != payload.length) {
      throw entity("payload-length mismatch");
    }
    if (checksum != null) {
      if (checksum.length != 32) {
        throw integrity("checksum must contain 32 octets");
      }
      if (!MessageDigest.isEqual(checksum, sha256(payload))) {
        throw integrity("checksum mismatch");
      }
    }
    EntityHeader result = new EntityHeader(
        entityId, parentId, layer, contentType, payloadLength, checksum);
    byte[] canonical = encodeEntityMap(result);
    if (!Arrays.equals(canonical, encodedHeader)) {
      throw frame("entity header CBOR is not deterministic");
    }
    return new Entity(result, payload);
  }

  private static byte[] encodeEntityMap(EntityHeader header) throws ProtocolException {
    LinkedHashMap<String, Object> map = new LinkedHashMap<>();
    map.put("layer", header.layer);
    if (header.checksum != null) {
      map.put("checksum", header.checksum);
    }
    map.put("entity-id", header.entityId);
    if (header.parentId != null) {
      map.put("parent-id", header.parentId);
    }
    if (header.contentType != null) {
      map.put("content-type", header.contentType);
    }
    if (header.payloadLength != null) {
      map.put("payload-length", header.payloadLength);
    }
    return encodeMap(map);
  }

  private static byte[] encodeMap(Map<String, Object> map) throws ProtocolException {
    try (ByteArrayOutputStream output = new ByteArrayOutputStream();
         JsonGenerator generator = CBOR.getFactory().createGenerator(output)) {
      generator.writeStartObject(map, map.size());
      for (Map.Entry<String, Object> entry : map.entrySet().stream().sorted((left, right) -> {
        byte[] a = left.getKey().getBytes(java.nio.charset.StandardCharsets.UTF_8);
        byte[] b = right.getKey().getBytes(java.nio.charset.StandardCharsets.UTF_8);
        int length = Integer.compare(a.length, b.length);
        return length != 0 ? length : Arrays.compareUnsigned(a, b);
      }).toList()) {
        generator.writeFieldName(entry.getKey());
        Object value = entry.getValue();
        if (value instanceof Boolean bool) {
          generator.writeBoolean(bool);
        } else if (value instanceof byte[] bytes) {
          generator.writeBinary(bytes);
        } else if (value instanceof String text) {
          generator.writeString(text);
        } else if (value instanceof Integer integer) {
          generator.writeNumber(integer);
        } else if (value instanceof Long number) {
          generator.writeNumber(number);
        } else if (value instanceof List<?> ids) {
          generator.writeStartArray(ids, ids.size());
          for (Object id : ids) {
            if (!(id instanceof Integer integer)) throw frame("extension identifier must be uint16");
            generator.writeNumber(integer);
          }
          generator.writeEndArray();
        } else {
          throw frame("unsupported CBOR value");
        }
      }
      generator.writeEndObject();
      generator.flush();
      return output.toByteArray();
    } catch (IOException exception) {
      throw frame("CBOR encode failed: " + exception.getMessage());
    }
  }

  private static LinkedHashMap<String, Object> decodeMap(byte[] data) throws ProtocolException {
    try {
      return CBOR.readValue(data, MAP_TYPE);
    } catch (IOException exception) {
      throw frame("CBOR decode failed: " + exception.getMessage());
    }
  }

  private static List<Integer> extensionList(Map<String, Object> map, String key) throws ProtocolException {
    if (!map.containsKey(key)) return List.of();
    if (!(map.get(key) instanceof List<?> ids) || ids.size() > 32) {
      throw frame("invalid extension array");
    }
    var result = new java.util.ArrayList<Integer>(ids.size());
    for (Object id : ids) {
      if (!(id instanceof Integer integer) || integer < 1 || integer >= 65535) {
        throw frame("extension identifier must be uint16 in 1..65534");
      }
      result.add(integer);
    }
    return result;
  }

  private static ProtocolException extensionUnsupported() {
    return new ProtocolException(ERROR_EXTENSION_UNSUPPORTED, "PIPESTREAM_EXTENSION_UNSUPPORTED",
        "a required extension is not supported by both peers");
  }

  private static boolean requiredBoolean(Map<String, Object> map, String key) throws ProtocolException {
    Object value = map.get(key);
    if (!(value instanceof Boolean result)) {
      throw frame("missing boolean " + key);
    }
    return result;
  }

  private static long requiredNumber(Map<String, Object> map, String key) throws ProtocolException {
    Object value = map.get(key);
    if (!(value instanceof Number number)) {
      throw entity(key + " is absent or not an unsigned integer");
    }
    return number.longValue();
  }

  private static long checkpointNumber(Map<String, Object> map, String key)
      throws ProtocolException {
    Object value = map.get(key);
    if (!(value instanceof Number number) || number.longValue() < 0) {
      throw frame("missing uint " + key);
    }
    return number.longValue();
  }

  private static Long optionalCheckpointNumber(Map<String, Object> map, String key)
      throws ProtocolException {
    Object value = map.get(key);
    if (value == null) {
      return null;
    }
    if (!(value instanceof Number number) || number.longValue() < 0) {
      throw frame(key + " must be uint");
    }
    return number.longValue();
  }

  private static long number(Map<String, Object> map, String key, long defaultValue)
      throws ProtocolException {
    Object value = map.get(key);
    if (value == null) {
      return defaultValue;
    }
    if (!(value instanceof Number number)) {
      throw frame(key + " must be an unsigned integer");
    }
    return number.longValue();
  }

  private static Long optionalNumber(Map<String, Object> map, String key) throws ProtocolException {
    Object value = map.get(key);
    if (value == null) {
      return null;
    }
    if (!(value instanceof Number number)) {
      throw entity(key + " must be an unsigned integer");
    }
    return number.longValue();
  }

  private static String optionalString(Map<String, Object> map, String key) throws ProtocolException {
    Object value = map.get(key);
    if (value == null) {
      return null;
    }
    if (!(value instanceof String text)) {
      throw entity(key + " must be text");
    }
    return text;
  }

  private static byte[] optionalBytes(Map<String, Object> map, String key) throws ProtocolException {
    Object value = map.get(key);
    if (value == null) {
      return null;
    }
    if (!(value instanceof byte[] bytes)) {
      throw integrity(key + " must be a byte string");
    }
    return bytes;
  }

  private static void rejectUnknown(Map<String, Object> map, String... known) throws ProtocolException {
    for (String key : map.keySet()) {
      if (Arrays.stream(known).noneMatch(key::equals)) {
        throw frame("unknown field " + key);
      }
    }
  }

  private static byte[] sha256(byte[] payload) throws ProtocolException {
    try {
      return MessageDigest.getInstance("SHA-256").digest(payload);
    } catch (NoSuchAlgorithmException exception) {
      throw new IllegalStateException("JDK lacks SHA-256", exception);
    }
  }

  static ProtocolException frame(String detail) {
    return new ProtocolException(ERROR_FRAME, "PIPESTREAM_FRAME_ERROR", detail);
  }

  static ProtocolException entity(String detail) {
    return new ProtocolException(ERROR_ENTITY_INVALID, "PIPESTREAM_ENTITY_INVALID", detail);
  }

  static ProtocolException integrity(String detail) {
    return new ProtocolException(ERROR_INTEGRITY, "PIPESTREAM_INTEGRITY_ERROR", detail);
  }

  static ProtocolException limit(String detail) {
    return new ProtocolException(ERROR_LIMIT_EXCEEDED, "PIPESTREAM_LIMIT_EXCEEDED", detail);
  }

  static ProtocolException layerUnsupported(String detail) {
    return new ProtocolException(ERROR_LAYER_UNSUPPORTED, "PIPESTREAM_LAYER_UNSUPPORTED", detail);
  }
}
