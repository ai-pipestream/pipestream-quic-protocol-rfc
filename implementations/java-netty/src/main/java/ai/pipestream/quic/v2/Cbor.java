package ai.pipestream.quic.v2;

import static ai.pipestream.quic.v2.ProtocolError.*;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;

/** Schema-driven deterministic CBOR. No generic object tree or JSON conversion. */
final class Cbor {
  private Cbor() {}

  static byte[] utf8(String value, int maximum) {
    require(value != null && value.length() <= maximum, "text exceeds byte bound");
    try {
      ByteBuffer encoded =
          StandardCharsets.UTF_8
              .newEncoder()
              .onMalformedInput(CodingErrorAction.REPORT)
              .onUnmappableCharacter(CodingErrorAction.REPORT)
              .encode(CharBuffer.wrap(value));
      require(encoded.remaining() <= maximum, "text exceeds UTF-8 byte bound");
      byte[] bytes = new byte[encoded.remaining()];
      encoded.get(bytes);
      return bytes;
    } catch (CharacterCodingException invalid) {
      throw frame("unpaired surrogate in text");
    }
  }

  static final class Reader {
    private final ByteBuffer input;

    Reader(byte[] bytes, int maximum) {
      require(maximum >= 0 && maximum <= 1048576, "invalid decoder limit");
      if (bytes.length > maximum) throw limit("encoded record exceeds byte limit");
      input = ByteBuffer.wrap(bytes).asReadOnlyBuffer();
    }

    int octet() {
      require(input.hasRemaining(), "truncated CBOR");
      return Byte.toUnsignedInt(input.get());
    }

    private long argument(int major) {
      int initial = octet();
      require(initial >>> 5 == major, "unexpected CBOR type");
      int info = initial & 31;
      if (info < 24) return info;
      int count =
          switch (info) {
            case 24 -> 1;
            case 25 -> 2;
            case 26 -> 4;
            case 27 -> 8;
            default -> throw frame("indefinite or reserved CBOR argument");
          };
      require(count <= input.remaining(), "truncated CBOR argument");
      long number = 0;
      for (int n = 0; n < count; n++) number = (number << 8) | octet();
      long minimum =
          switch (count) {
            case 1 -> 24;
            case 2 -> 256;
            case 4 -> 65536;
            default -> 4294967296L;
          };
      require(number >= minimum, "nonminimal or out-of-range CBOR argument");
      return number;
    }

    long number() {
      return argument(0);
    }

    int array(int maximum) {
      long count = argument(4);
      require(
          count <= maximum && count <= input.remaining(),
          "array cardinality exceeds schema or remaining bytes");
      return (int) count;
    }

    void exact(int count) {
      require(array(count) == count, "wrong array cardinality");
    }

    boolean nullable() {
      require(input.hasRemaining(), "truncated nullable field");
      if (Byte.toUnsignedInt(input.get(input.position())) != 0xf6) return false;
      input.get();
      return true;
    }

    boolean bool() {
      int value = octet();
      require(value == 0xf4 || value == 0xf5, "expected boolean");
      return value == 0xf5;
    }

    byte[] bytes(int length) {
      require(argument(2) == length && length <= input.remaining(), "wrong byte-string length");
      byte[] bytes = new byte[length];
      input.get(bytes);
      return bytes;
    }

    String text(int maximum) {
      long length = argument(3);
      require(
          length <= maximum && length <= input.remaining(),
          "text exceeds schema or remaining bytes");
      ByteBuffer bytes = input.slice();
      bytes.limit((int) length);
      try {
        String value =
            StandardCharsets.UTF_8
                .newDecoder()
                .onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT)
                .decode(bytes)
                .toString();
        input.position(input.position() + (int) length);
        return value;
      } catch (CharacterCodingException invalid) {
        throw frame("invalid UTF-8");
      }
    }

    void end() {
      require(!input.hasRemaining(), "trailing CBOR item");
    }
  }

  static final class Writer {
    private final ByteArrayOutputStream output;
    private final MessageDigest digest;
    private final long maximum;
    private long count;

    Writer(int maximum) {
      require(maximum >= 0 && maximum <= 1048576, "invalid encoder limit");
      this.maximum = maximum;
      output = new ByteArrayOutputStream(Math.min(128, maximum));
      digest = null;
    }

    Writer(MessageDigest digest) {
      this.digest = digest;
      output = null;
      maximum = Long.MAX_VALUE;
    }

    private void room(long length) {
      if (length < 0 || length > maximum - count) throw limit("encoded record exceeds byte limit");
      count += length;
    }

    void octet(int value) {
      room(1);
      if (output != null) output.write(value);
      else digest.update((byte) value);
    }

    void raw(byte[] bytes) {
      room(bytes.length);
      if (output != null) output.writeBytes(bytes);
      else digest.update(bytes);
    }

    private void argument(int major, long number) {
      require(number >= 0, "negative or overflowing CBOR value");
      if (number < 24) {
        octet((major << 5) | (int) number);
        return;
      }
      int width = number <= 255 ? 1 : number <= 65535 ? 2 : number <= 4294967295L ? 4 : 8;
      octet((major << 5) | (width == 1 ? 24 : width == 2 ? 25 : width == 4 ? 26 : 27));
      for (int shift = (width - 1) * 8; shift >= 0; shift -= 8)
        octet((int) (number >>> shift) & 255);
    }

    void number(long value) {
      argument(0, value);
    }

    void array(long count) {
      argument(4, count);
    }

    void bytes(byte[] value) {
      argument(2, value.length);
      raw(value);
    }

    void text(String value, int maximum) {
      byte[] bytes = utf8(value, maximum);
      argument(3, bytes.length);
      raw(bytes);
    }

    void bool(boolean value) {
      octet(value ? 0xf5 : 0xf4);
    }

    void nil() {
      octet(0xf6);
    }

    byte[] finish() {
      if (output == null) throw new IllegalStateException("hash-only writer");
      return output.toByteArray();
    }
  }
}
