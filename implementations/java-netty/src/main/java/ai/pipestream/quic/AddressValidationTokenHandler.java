package ai.pipestream.quic;

import io.netty.buffer.ByteBuf;
import io.netty.incubator.codec.quic.QuicTokenHandler;
import java.net.InetSocketAddress;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.security.SecureRandom;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

/** Process-scoped HMAC address-validation tokens for the Netty QUIC listener. */
final class AddressValidationTokenHandler implements QuicTokenHandler {
  private static final int TAG_LENGTH = 32;
  private static final int MAX_CONNECTION_ID_LENGTH = 20;
  private static final int MAX_ADDRESS_LENGTH = 16;
  private static final int MAX_TOKEN_LENGTH = 1 + MAX_ADDRESS_LENGTH + TAG_LENGTH + MAX_CONNECTION_ID_LENGTH;
  private final SecretKeySpec key;

  AddressValidationTokenHandler() {
    byte[] secret = new byte[32];
    new SecureRandom().nextBytes(secret);
    key = new SecretKeySpec(secret, "HmacSHA256");
  }

  @Override
  public boolean writeToken(ByteBuf output, ByteBuf destinationConnectionId, InetSocketAddress address) {
    byte[] addressBytes = address.getAddress().getAddress();
    byte[] connectionId = new byte[destinationConnectionId.readableBytes()];
    destinationConnectionId.getBytes(destinationConnectionId.readerIndex(), connectionId);
    output.writeByte(addressBytes.length);
    output.writeBytes(addressBytes);
    output.writeBytes(sign(addressBytes, connectionId));
    output.writeBytes(connectionId);
    return true;
  }

  @Override
  public int validateToken(ByteBuf token, InetSocketAddress address) {
    if (token.readableBytes() < 1 + 4 + TAG_LENGTH + 1) {
      return -1;
    }
    int base = token.readerIndex();
    int addressLength = token.getUnsignedByte(base);
    int connectionIdOffset = 1 + addressLength + TAG_LENGTH;
    if ((addressLength != 4 && addressLength != 16)
        || token.readableBytes() <= connectionIdOffset
        || token.readableBytes() > MAX_TOKEN_LENGTH) {
      return -1;
    }
    byte[] expectedAddress = address.getAddress().getAddress();
    if (expectedAddress.length != addressLength) {
      return -1;
    }
    byte[] encodedAddress = new byte[addressLength];
    token.getBytes(base + 1, encodedAddress);
    if (!MessageDigest.isEqual(expectedAddress, encodedAddress)) {
      return -1;
    }
    byte[] observedTag = new byte[TAG_LENGTH];
    token.getBytes(base + 1 + addressLength, observedTag);
    byte[] connectionId = new byte[token.readableBytes() - connectionIdOffset];
    token.getBytes(base + connectionIdOffset, connectionId);
    if (!MessageDigest.isEqual(observedTag, sign(encodedAddress, connectionId))) {
      return -1;
    }
    return connectionIdOffset;
  }

  @Override
  public int maxTokenLength() {
    return MAX_TOKEN_LENGTH;
  }

  private byte[] sign(byte[] address, byte[] connectionId) {
    try {
      Mac mac = Mac.getInstance("HmacSHA256");
      mac.init(key);
      mac.update(address);
      return mac.doFinal(connectionId);
    } catch (GeneralSecurityException exception) {
      throw new IllegalStateException("JDK lacks HmacSHA256", exception);
    }
  }
}
