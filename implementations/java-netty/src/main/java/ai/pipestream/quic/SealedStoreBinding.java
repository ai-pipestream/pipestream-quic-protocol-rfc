package ai.pipestream.quic;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.UUID;

/** Fixed, checksummed identities for the Java database and its optional payload store. */
record SealedStoreBinding(UUID database, UUID payloads) {
  static final int BYTES = 72;
  static final UUID UNBOUND = new UUID(0, 0);
  private static final byte[] MAGIC = "PSJBND01".getBytes(StandardCharsets.US_ASCII);

  SealedStoreBinding {
    if (database == null || database.equals(UNBOUND) || payloads == null) {
      throw new IllegalArgumentException("invalid Java store identity");
    }
  }

  byte[] encode() {
    ByteBuffer image = ByteBuffer.allocate(BYTES);
    image.put(MAGIC).putLong(database.getMostSignificantBits()).putLong(database.getLeastSignificantBits())
        .putLong(payloads.getMostSignificantBits()).putLong(payloads.getLeastSignificantBits());
    image.put(SealedWork.sha256().digest(Arrays.copyOf(image.array(), BYTES - 32)));
    return image.array();
  }

  static SealedStoreBinding decode(byte[] image) throws ProtocolException {
    if (image == null || image.length != BYTES
        || !Arrays.equals(MAGIC, Arrays.copyOf(image, MAGIC.length))
        || !MessageDigest.isEqual(SealedWork.sha256().digest(Arrays.copyOf(image, BYTES - 32)),
            Arrays.copyOfRange(image, BYTES - 32, BYTES))) throw Wire.integrity("invalid Java store binding image");
    ByteBuffer fields = ByteBuffer.wrap(image); fields.position(MAGIC.length);
    UUID database = new UUID(fields.getLong(), fields.getLong());
    UUID payloads = new UUID(fields.getLong(), fields.getLong());
    if (database.equals(UNBOUND)) throw Wire.integrity("missing Java database identity");
    return new SealedStoreBinding(database, payloads);
  }
}
