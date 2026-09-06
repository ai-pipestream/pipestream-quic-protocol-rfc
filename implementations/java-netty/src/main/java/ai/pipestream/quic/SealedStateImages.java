package ai.pipestream.quic;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Arrays;

/** Java-owned fixed-capacity entity and closure records, distinct from wire frames. */
final class SealedStateImages {
  static final int ENTITY_BYTES = 112, CLOSURE_BYTES = 128;
  private static final byte[] ENTITY_MAGIC = {'P', 'S', 'J', 'E', 'N', 'T', '0', '1'};
  private static final byte[] CLOSURE_MAGIC = {'P', 'S', 'J', 'C', 'L', 'O', '0', '1'};
  private static final String ENTITY_DOMAIN = "pipestream-java-entity-image-v1";
  private static final String CLOSURE_DOMAIN = "pipestream-java-closure-image-v1";

  private SealedStateImages() {}

  record Entity(Integer state, boolean managed, byte[] payloadDigest, byte[] outputDigest) {
    Entity {
      payloadDigest = payloadDigest == null ? null : payloadDigest.clone();
      outputDigest = outputDigest == null ? null : outputDigest.clone();
    }
    @Override public byte[] payloadDigest() { return payloadDigest == null ? null : payloadDigest.clone(); }
    @Override public byte[] outputDigest() { return outputDigest == null ? null : outputDigest.clone(); }
  }

  static byte[] entity(String session, byte[] producer, SealedWork.EntityKey key, Entity entity)
      throws ProtocolException {
    validate(entity);
    byte[] image = new byte[ENTITY_BYTES];
    ByteBuffer fields = ByteBuffer.wrap(image);
    fields.put(ENTITY_MAGIC).putInt(entity.state() == null ? 0 : entity.state()).putInt(entity.managed() ? 1 : 0);
    if (entity.payloadDigest() != null) fields.put(entity.payloadDigest());
    fields.position(48);
    if (entity.outputDigest() != null) fields.put(entity.outputDigest());
    fields.position(ENTITY_BYTES - 32);
    fields.put(checksum(ENTITY_DOMAIN, session, producer, entityKey(key), image));
    return image;
  }

  static Entity entity(String session, byte[] producer, SealedWork.EntityKey key, byte[] image)
      throws ProtocolException {
    verify(ENTITY_DOMAIN, session, producer, entityKey(key), image, ENTITY_MAGIC, ENTITY_BYTES);
    ByteBuffer fields = ByteBuffer.wrap(image); fields.position(8);
    int state = fields.getInt(), managed = fields.getInt();
    if (managed < 0 || managed > 1) throw Wire.integrity("invalid entity management flag");
    if (state == 0) zero(image, 16, 48);
    if (state != Wire.STATUS_COMPLETE) zero(image, 48, 80);
    Entity entity = new Entity(state == 0 ? null : state, managed == 1,
        state == 0 ? null : Arrays.copyOfRange(image, 16, 48),
        state == Wire.STATUS_COMPLETE ? Arrays.copyOfRange(image, 48, 80) : null);
    validate(entity);
    return entity;
  }

  static byte[] closure(String session, byte[] producer, long scope, SealedWork.EntityKey parent, byte[] frame)
      throws ProtocolException {
    if (frame != null && (scope == 0 || frame.length != 77 || SealedScope.decode(frame).scopeId() != scope)) {
      throw Wire.integrity("closure image has a different scope or frame geometry");
    }
    byte[] image = new byte[CLOSURE_BYTES];
    ByteBuffer fields = ByteBuffer.wrap(image);
    fields.put(CLOSURE_MAGIC).put((byte) (frame == null ? 0 : 1));
    if (frame != null) fields.put(frame);
    fields.position(CLOSURE_BYTES - 32);
    fields.put(checksum(CLOSURE_DOMAIN, session, producer, scopeKey(scope, parent), image));
    return image;
  }

  static byte[] readClosure(String session, byte[] producer, long scope, SealedWork.EntityKey parent, byte[] image)
      throws ProtocolException {
    verify(CLOSURE_DOMAIN, session, producer, scopeKey(scope, parent), image, CLOSURE_MAGIC, CLOSURE_BYTES);
    if (image[8] == 0) { zero(image, 9, CLOSURE_BYTES - 32); return null; }
    if (image[8] != 1 || scope == 0) throw Wire.integrity("invalid scope closure flag");
    zero(image, 86, CLOSURE_BYTES - 32);
    byte[] frame = Arrays.copyOfRange(image, 9, 86);
    if (SealedScope.decode(frame).scopeId() != scope) throw Wire.integrity("closure image scope differs");
    return frame;
  }

  private static void validate(Entity entity) throws ProtocolException {
    Integer state = entity.state();
    if (state == null) {
      if (entity.managed() || entity.payloadDigest() != null || entity.outputDigest() != null) throw Wire.integrity("unadmitted entity contains execution state");
    } else if ((state != 2 && state != 3 && state != 4 && state != 6 && state != 7)
        || entity.payloadDigest() == null || entity.payloadDigest().length != 32
        || (state == Wire.STATUS_COMPLETE) != (entity.outputDigest() != null)
        || (entity.outputDigest() != null && entity.outputDigest().length != 32)) {
      throw Wire.integrity("invalid entity state image");
    }
  }

  private static byte[] entityKey(SealedWork.EntityKey key) throws ProtocolException {
    if (key == null || key.scopeId() < 0 || key.scopeId() > 0xffff_ffffL || key.entityId() < 1 || key.entityId() > Wire.MAX_ENTITY_ID) {
      throw Wire.integrity("invalid entity image identity");
    }
    return ByteBuffer.allocate(8).putInt((int) key.scopeId()).putInt((int) key.entityId()).array();
  }

  private static byte[] scopeKey(long scope, SealedWork.EntityKey parent) throws ProtocolException {
    if (scope < 0 || scope > 0xffff_ffffL || (scope == 0) != (parent == null)) throw Wire.integrity("invalid closure image identity");
    ByteBuffer key = ByteBuffer.allocate(13).putInt((int) scope).put((byte) (parent == null ? 0 : 1));
    if (parent != null) key.put(entityKey(parent));
    return key.array();
  }

  private static byte[] checksum(String domain, String session, byte[] producer, byte[] key, byte[] image)
      throws ProtocolException {
    if (!SealedWork.validSessionId(session) || producer == null || producer.length != 16
        || Arrays.equals(producer, new byte[16])) throw Wire.integrity("invalid image session or producer");
    byte[] name = session.getBytes(StandardCharsets.US_ASCII);
    var digest = SealedWork.sha256();
    digest.update(domain.getBytes(StandardCharsets.US_ASCII));
    digest.update(ByteBuffer.allocate(2).putShort((short) name.length).array());
    digest.update(name); digest.update(producer); digest.update(key);
    digest.update(image, 0, image.length - 32);
    return digest.digest();
  }

  private static void verify(String domain, String session, byte[] producer, byte[] key,
      byte[] image, byte[] magic, int length) throws ProtocolException {
    if (image == null || image.length != length || !Arrays.equals(Arrays.copyOf(image, 8), magic)) {
      throw Wire.integrity("state image version or length differs");
    }
    if (!MessageDigest.isEqual(checksum(domain, session, producer, key, image), Arrays.copyOfRange(image, length - 32, length))) {
      throw Wire.integrity("state image checksum or identity differs");
    }
  }

  private static void zero(byte[] image, int start, int end) throws ProtocolException {
    for (int offset = start; offset < end; offset++) if (image[offset] != 0) throw Wire.integrity("unused state image bytes are not zero");
  }
}
