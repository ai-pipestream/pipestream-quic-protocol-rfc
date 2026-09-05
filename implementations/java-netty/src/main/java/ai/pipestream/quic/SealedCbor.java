package ai.pipestream.quic;

import java.io.ByteArrayOutputStream;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** The definite-length, text-keyed CBOR types used by the sealed Layer 1 contract. */
final class SealedCbor {
  static final BigInteger MAX_UINT = BigInteger.ONE.shiftLeft(64).subtract(BigInteger.ONE);
  private static final int MAX_DEPTH = 8;
  private static final int MAX_ITEMS = 65_536;

  private SealedCbor() {}

  static byte[] encode(Map<String, Object> map, int limit) throws ProtocolException {
    Writer writer = new Writer(limit);
    writer.item(map, 0);
    return writer.output.toByteArray();
  }

  static Map<String, Object> decode(byte[] input, int limit) throws ProtocolException {
    if (input.length > limit) throw Wire.limit("serialized message exceeds local limit");
    Reader reader = new Reader(input);
    Object value = reader.item(0);
    if (reader.position != input.length) throw Wire.frame("trailing CBOR data");
    if (!(value instanceof Map<?, ?> map)) throw Wire.frame("CBOR message must be a map");
    Map<String, Object> result = new LinkedHashMap<>();
    for (var entry : map.entrySet()) {
      if (!(entry.getKey() instanceof String key)) throw Wire.frame("map key must be text");
      result.put(key, entry.getValue());
    }
    return result;
  }

  static byte[] utf8(String text) throws ProtocolException {
    try {
      ByteBuffer encoded = StandardCharsets.UTF_8.newEncoder()
          .onMalformedInput(CodingErrorAction.REPORT)
          .onUnmappableCharacter(CodingErrorAction.REPORT).encode(CharBuffer.wrap(text));
      byte[] bytes = new byte[encoded.remaining()];
      encoded.get(bytes);
      return bytes;
    } catch (CharacterCodingException exception) {
      throw Wire.frame("text contains an unpaired surrogate");
    }
  }

  private static String text(byte[] bytes) throws ProtocolException {
    try {
      return StandardCharsets.UTF_8.newDecoder()
          .onMalformedInput(CodingErrorAction.REPORT)
          .onUnmappableCharacter(CodingErrorAction.REPORT).decode(ByteBuffer.wrap(bytes)).toString();
    } catch (CharacterCodingException exception) {
      throw Wire.frame("text is not valid UTF-8");
    }
  }

  private static final class Reader {
    private final byte[] input;
    private int position;
    private int items;

    Reader(byte[] input) { this.input = input; }

    private int octet() throws ProtocolException {
      if (position == input.length) throw Wire.frame("truncated CBOR");
      return Byte.toUnsignedInt(input[position++]);
    }

    private BigInteger argument(int info) throws ProtocolException {
      if (info < 24) return BigInteger.valueOf(info);
      int octets = switch (info) {
        case 24 -> 1;
        case 25 -> 2;
        case 26 -> 4;
        case 27 -> 8;
        default -> throw Wire.frame("indefinite length or reserved CBOR argument");
      };
      BigInteger value = new BigInteger(1, bytes(octets));
      int minimumBits = switch (octets) { case 1 -> 5; case 2 -> 9; case 4 -> 17; default -> 33; };
      if (value.bitLength() < minimumBits || (octets == 1 && value.intValue() < 24)) {
        throw Wire.frame("non-minimal CBOR argument");
      }
      return value;
    }

    private byte[] bytes(int length) throws ProtocolException {
      if (length > input.length - position) throw Wire.frame("truncated CBOR item");
      byte[] result = Arrays.copyOfRange(input, position, position + length);
      position += length;
      return result;
    }

    private int length(BigInteger value) throws ProtocolException {
      if (value.compareTo(BigInteger.valueOf(input.length - position)) > 0) {
        throw Wire.frame("CBOR length exceeds remaining input");
      }
      return value.intValueExact();
    }

    private Object item(int depth) throws ProtocolException {
      if (depth > MAX_DEPTH || ++items > MAX_ITEMS) throw Wire.limit("CBOR structure exceeds local limit");
      int initial = octet();
      int major = initial >>> 5;
      int info = initial & 31;
      if (major == 7) {
        if (info == 20) return false;
        if (info == 21) return true;
        throw Wire.frame("unexpected CBOR simple or floating-point value");
      }
      BigInteger argument = argument(info);
      if (major == 0) return argument;
      if (major == 2) return bytes(length(argument));
      if (major == 3) return text(bytes(length(argument)));
      if (major == 4) {
        int count = length(argument);
        if (count > MAX_ITEMS - items) throw Wire.limit("CBOR array exceeds local item budget");
        List<Object> values = new ArrayList<>(Math.min(count, 256));
        for (int i = 0; i < count; i++) values.add(item(depth + 1));
        return values;
      }
      if (major == 5) {
        int count = length(argument);
        if (count > (input.length - position) / 2) throw Wire.frame("truncated CBOR map");
        if (count > (MAX_ITEMS - items) / 2) throw Wire.limit("CBOR map exceeds local item budget");
        Map<String, Object> values = new LinkedHashMap<>();
        byte[] previous = null;
        for (int i = 0; i < count; i++) {
          int start = position;
          Object key = item(depth + 1);
          if (!(key instanceof String name)) throw Wire.frame("CBOR map key must be text");
          byte[] encoded = Arrays.copyOfRange(input, start, position);
          if (previous != null && Arrays.compareUnsigned(previous, encoded) >= 0) {
            throw Wire.frame("duplicate or non-deterministically ordered map key");
          }
          previous = encoded;
          values.put(name, item(depth + 1));
        }
        return values;
      }
      throw Wire.frame("CBOR type is not part of the sealed-work schema");
    }
  }

  private static final class Writer {
    private final ByteArrayOutputStream output = new ByteArrayOutputStream(256);
    private final int limit;
    private int items;

    Writer(int limit) { this.limit = limit; }

    private void octet(int value) throws ProtocolException {
      if (output.size() >= limit) throw Wire.limit("serialized message exceeds local limit");
      output.write(value);
    }

    private void bytes(byte[] bytes) throws ProtocolException {
      if (bytes.length > limit - output.size()) throw Wire.limit("serialized message exceeds local limit");
      output.writeBytes(bytes);
    }

    private void argument(int major, BigInteger value) throws ProtocolException {
      if (value.signum() < 0 || value.compareTo(MAX_UINT) > 0) throw Wire.frame("integer exceeds CBOR uint64");
      int bits = value.bitLength();
      if (value.compareTo(BigInteger.valueOf(24)) < 0) { octet((major << 5) | value.intValue()); return; }
      int octets = bits <= 8 ? 1 : bits <= 16 ? 2 : bits <= 32 ? 4 : 8;
      octet((major << 5) | (octets == 1 ? 24 : octets == 2 ? 25 : octets == 4 ? 26 : 27));
      for (int shift = (octets - 1) * 8; shift >= 0; shift -= 8) octet(value.shiftRight(shift).intValue() & 255);
    }

    private void item(Object value, int depth) throws ProtocolException {
      if (depth > MAX_DEPTH || ++items > MAX_ITEMS) throw Wire.limit("CBOR structure exceeds local limit");
      if (value instanceof BigInteger number) { argument(0, number); }
      else if (value instanceof Integer number) { argument(0, BigInteger.valueOf(number)); }
      else if (value instanceof Long number) { argument(0, BigInteger.valueOf(number)); }
      else if (value instanceof Boolean bool) { octet(bool ? 0xf5 : 0xf4); }
      else if (value instanceof byte[] data) { argument(2, BigInteger.valueOf(data.length)); bytes(data); }
      else if (value instanceof String string) {
        if (string.length() > limit - output.size()) throw Wire.limit("text exceeds local byte limit");
        byte[] data = utf8(string); argument(3, BigInteger.valueOf(data.length)); bytes(data);
      }
      else if (value instanceof List<?> list) {
        argument(4, BigInteger.valueOf(list.size()));
        for (Object entry : list) item(entry, depth + 1);
      } else if (value instanceof Map<?, ?> map) {
        if (map.size() > (MAX_ITEMS - items) / 2) throw Wire.limit("CBOR map exceeds local item budget");
        List<Key> entries = new ArrayList<>();
        for (var entry : map.entrySet()) {
          if (!(entry.getKey() instanceof String name)) throw Wire.frame("CBOR map key must be text");
          Writer key = new Writer(limit);
          key.item(name, depth + 1);
          entries.add(new Key(key.output.toByteArray(), entry.getValue()));
        }
        entries.sort((a, b) -> Arrays.compareUnsigned(a.encoded, b.encoded));
        argument(5, BigInteger.valueOf(entries.size()));
        for (Key entry : entries) {
          if (++items > MAX_ITEMS) throw Wire.limit("CBOR structure exceeds local limit");
          bytes(entry.encoded);
          item(entry.value, depth + 1);
        }
      } else { throw Wire.frame("unsupported CBOR value"); }
    }
  }

  private record Key(byte[] encoded, Object value) {}
}
