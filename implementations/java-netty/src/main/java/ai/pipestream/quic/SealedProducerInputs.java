package ai.pipestream.quic;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.TreeMap;

/** File commitments retained before a durable producer opens any Entity Stream. */
final class SealedProducerInputs {
  private static final int MAX_CHUNKS = 65_536;
  private static final long MAGIC = 0x50534a50494e3031L;

  private SealedProducerInputs() {}

  /**
   * Effective headers with mandatory length/checksum and their retained descriptor.
   * @param files original paths paired with verified, committed headers
   * @param descriptor bounded local image, not a new wire message
   */
  record Prepared(List<SealedClient.FileChunk> files, byte[] descriptor) {
    /** Copies mutable inputs. */
    Prepared { files = List.copyOf(files); descriptor = descriptor.clone(); }
    /** Returns a descriptor copy.
     * @return detached descriptor bytes
     */
    @Override public byte[] descriptor() { return descriptor.clone(); }
  }

  /**
   * Hashes files incrementally and fills omitted wire length/checksum commitments.
   * @param source complete unchunked file or chunk set
   * @return verified inputs and immutable request descriptor
   * @throws IOException for missing or changing files
   * @throws ProtocolException for invalid geometry, commitments or metadata bounds
   */
  static Prepared prepare(List<SealedClient.FileChunk> source) throws IOException, ProtocolException {
    if (source.isEmpty() || source.size() > MAX_CHUNKS) throw Wire.limit("invalid durable input count");
    List<SealedClient.FileChunk> files = new ArrayList<>(); List<byte[]> headers = new ArrayList<>();
    int size = 12; byte[] buffer = new byte[8192];
    for (var file : source) {
      var header = file.header(); SealedTransport.header(header);
      if (!Files.isRegularFile(file.payload())) throw new IOException("durable input must be a regular file");
      long expected = Files.size(file.payload()), count = 0;
      if (header.payloadLength() != null && !header.payloadLength().equals(BigInteger.valueOf(expected))) throw Wire.entity("durable input length differs from header");
      var hash = SealedWork.sha256();
      try (var input = Files.newInputStream(file.payload())) {
        int n;
        while ((n = input.read(buffer)) != -1) {
          count = Math.addExact(count, n);
          if (count > expected) throw Wire.integrity("durable input grew during hashing");
          hash.update(buffer, 0, n);
        }
      }
      byte[] digest = hash.digest();
      if (count != expected || (header.checksum() != null && !MessageDigest.isEqual(digest, header.checksum()))) throw Wire.integrity("durable input checksum or length differs");
      var committed = new SealedTransport.Header(header.key(), header.parent(), header.layer(), header.contentType(),
          BigInteger.valueOf(expected), digest, header.metadata(), header.chunk());
      byte[] encoded = SealedTransport.header(committed);
      if (encoded.length + 4 > SealedProducerJournal.MAX_REQUEST_BYTES - size) throw Wire.limit("durable input descriptor exceeds local bound");
      size += encoded.length + 4; headers.add(encoded); files.add(new SealedClient.FileChunk(committed, file.payload()));
    }
    ByteBuffer image = ByteBuffer.allocate(size).putLong(MAGIC).putInt(headers.size());
    for (byte[] header : headers) image.putInt(header.length).put(header);
    first(image.array());
    return new Prepared(files, image.array());
  }

  /**
   * Audits retained commitments and returns their shared entity identity/header.
   * @param descriptor local input image
   * @return first committed header; its chunk may be in any arrival position
   * @throws ProtocolException for corrupt or inconsistent retained input
   */
  static SealedTransport.Header first(byte[] descriptor) throws ProtocolException {
    if (descriptor == null || descriptor.length < 12 || descriptor.length > SealedProducerJournal.MAX_REQUEST_BYTES) throw Wire.integrity("invalid producer input descriptor length");
    ByteBuffer image = ByteBuffer.wrap(descriptor);
    if (image.getLong() != MAGIC) throw Wire.integrity("unsupported producer input descriptor");
    int count = image.getInt();
    if (count < 1 || count > MAX_CHUNKS) throw Wire.integrity("invalid retained input count");
    SealedTransport.Header first = null;
    var indexes = new HashSet<BigInteger>(); var ranges = new TreeMap<BigInteger, BigInteger>();
    for (int i = 0; i < count; i++) {
      if (image.remaining() < 4) throw Wire.integrity("truncated producer input descriptor");
      int length = image.getInt();
      if (length < 5 || length > Wire.MAX_ENTITY_HEADER + 4 || length > image.remaining()) throw Wire.integrity("invalid retained header length");
      int end = image.position() + length;
      if (image.getInt() != length - 4) throw Wire.integrity("retained header framing differs");
      byte[] encoded = Arrays.copyOfRange(descriptor, image.position(), end); image.position(end);
      var header = SealedTransport.header(encoded);
      if (header.payloadLength() == null || header.payloadLength().bitLength() > 63 || header.checksum() == null) throw Wire.integrity("producer input lacks a file commitment");
      if (first == null) first = header;
      if (!header.key().equals(first.key()) || !Objects.equals(header.parent(), first.parent())
          || header.layer() != first.layer() || !Objects.equals(header.contentType(), first.contentType())
          || !header.metadata().equals(first.metadata()) || (header.chunk() == null) != (first.chunk() == null)) throw Wire.integrity("retained chunk identities differ");
      if (header.chunk() == null) {
        if (count != 1) throw Wire.integrity("multiple unchunked inputs in one intent");
      } else {
        if (!header.chunk().total().equals(BigInteger.valueOf(count)) || !indexes.add(header.chunk().index())
            || ranges.putIfAbsent(header.chunk().offset(), header.payloadLength()) != null) throw Wire.integrity("retained chunk geometry differs");
      }
    }
    if (image.hasRemaining()) throw Wire.integrity("trailing producer input descriptor data");
    BigInteger end = BigInteger.ZERO;
    for (var range : ranges.entrySet()) {
      if (!range.getKey().equals(end)) throw Wire.integrity("retained chunks overlap or leave a gap");
      end = end.add(range.getValue());
      if (end.compareTo(SealedCbor.MAX_UINT) > 0) throw Wire.integrity("retained chunk end exceeds uint64");
    }
    return first;
  }
}
