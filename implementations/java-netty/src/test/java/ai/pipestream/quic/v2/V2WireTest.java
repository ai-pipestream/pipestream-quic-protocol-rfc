package ai.pipestream.quic.v2;

import static ai.pipestream.quic.v2.Messages.*;
import static ai.pipestream.quic.v2.Records.*;
import static org.junit.jupiter.api.Assertions.*;

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;

final class V2WireTest {
  static final Path VECTORS = Path.of("..", "..", "test-vectors", "v2");
  static final HexFormat HEX = HexFormat.of();

  record Vector(
      String name,
      String root,
      String framing,
      boolean accepted,
      String error,
      String hash,
      byte[] bytes) {}

  static List<Vector> vectors() throws Exception {
    List<Vector> vectors = new ArrayList<>();
    for (String row : Files.readAllLines(VECTORS.resolve("wire.tsv")).subList(1, 71)) {
      String[] f = row.split("\t", -1);
      vectors.add(
          new Vector(f[0], f[1], f[2], f[4].equals("accept"), f[5], f[6], HEX.parseHex(f[7])));
    }
    assertEquals(
        71,
        Files.readAllLines(VECTORS.resolve("wire.tsv")).size(),
        "unexpected frozen corpus size");
    return vectors;
  }

  static byte[] named(String name) throws Exception {
    return vectors().stream().filter(v -> v.name.equals(name)).findFirst().orElseThrow().bytes;
  }

  static Object decode(Vector v) {
    if (v.framing.startsWith("control:")) return Wire.decode(v.bytes, Wire.MAX_CONTROL_LIMIT);
    if (v.framing.equals("input-header")) return Wire.decodeHeader(true, v.bytes);
    if (v.framing.equals("result-header")) return Wire.decodeHeader(false, v.bytes);
    assertEquals("record", v.framing);
    return Wire.decodeRecord(
        v.root.equals("v2-result-manifest")
            ? Wire.RecordKind.MANIFEST
            : Wire.RecordKind.SCOPE_SUMMARY,
        v.bytes,
        Wire.MAX_CONTROL_LIMIT);
  }

  @TestFactory
  Stream<DynamicTest> allFrozenExpectationsAndExactRoundTrips() throws Exception {
    return vectors().stream()
        .map(
            v ->
                DynamicTest.dynamicTest(
                    v.name,
                    () -> {
                      assertEquals(
                          v.hash,
                          HEX.formatHex(MessageDigest.getInstance("SHA-256").digest(v.bytes)));
                      if (!v.accepted) {
                        assertEquals(
                            v.error,
                            assertThrows(ProtocolError.class, () -> decode(v)).code().name());
                        return;
                      }
                      Object decoded = decode(v);
                      byte[] encoded;
                      if (decoded instanceof Wire.Known known)
                        encoded = Wire.encode(known.message(), Wire.MAX_CONTROL_LIMIT);
                      else if (decoded instanceof InputHeader || decoded instanceof ResultHeader)
                        encoded = Wire.encodeHeader((Value) decoded);
                      else encoded = Wire.encodeRecord((Value) decoded, Wire.MAX_CONTROL_LIMIT);
                      assertArrayEquals(v.bytes, encoded);
                    }));
  }

  @Test
  void everyControlCutAndBytewiseDeliveryUsesTheSameTypedDecoder() throws Exception {
    for (Vector v : vectors())
      if (v.accepted && v.framing.startsWith("control:")) {
        Object expected = decode(v);
        for (int cut = 0; cut < v.bytes.length; cut++) {
          Wire.Decoder decoder = new Wire.Decoder(Wire.MAX_CONTROL_LIMIT);
          assertNull(decoder.feed(ByteBuffer.wrap(v.bytes, 0, cut)), v.name + "/" + cut);
          assertEquals(expected, decoder.feed(ByteBuffer.wrap(v.bytes, cut, v.bytes.length - cut)));
          decoder.finish();
        }
        Wire.Decoder decoder = new Wire.Decoder(Wire.MAX_CONTROL_LIMIT);
        for (int n = 0; n < v.bytes.length; n++) {
          Wire.Frame frame = decoder.feed(ByteBuffer.wrap(v.bytes, n, 1));
          if (n == v.bytes.length - 1) assertEquals(expected, frame);
          else assertNull(frame);
        }
      }
  }

  @Test
  void framingRejectsBeforeAllocationAndNeverResynchronizesAfterError() {
    for (long length : new long[] {4097, 1048577, 4294967295L}) {
      Wire.Decoder decoder = new Wire.Decoder(4096);
      ByteBuffer prefix = ByteBuffer.allocate(5).put((byte) 1).putInt((int) length).flip();
      assertEquals(
          ProtocolError.Code.LIMIT_EXCEEDED,
          assertThrows(ProtocolError.class, () -> decoder.feed(prefix)).code());
      assertEquals(0, decoder.bufferedCapacity());
      assertThrows(ProtocolError.class, () -> decoder.feed(ByteBuffer.wrap(new byte[0])));
    }
    Wire.Decoder decoder = new Wire.Decoder(4096);
    decoder.feed(ByteBuffer.wrap(new byte[] {1, 0}));
    assertThrows(ProtocolError.class, decoder::finish);
    assertThrows(ProtocolError.class, () -> decoder.limit(8192));
  }

  @Test
  void ignorableBodiesAreDiscardedIncrementallyAndDoNotConsumeFollowingFrames() {
    Wire.Decoder decoder = new Wire.Decoder(Wire.MAX_CONTROL_LIMIT);
    assertNull(
        decoder.feed(
            ByteBuffer.allocate(5).put((byte) 0xbf).putInt(Wire.MAX_CONTROL_LIMIT).flip()));
    byte[] block = new byte[1024];
    for (int n = 0; n < 1024; n++) {
      Wire.Frame frame = decoder.feed(ByteBuffer.wrap(block));
      assertEquals(0, decoder.bufferedCapacity());
      if (n == 1023) assertEquals(new Wire.Ignored(0xbf, Wire.MAX_CONTROL_LIMIT), frame);
      else assertNull(frame);
    }
    byte[] next = Wire.encode(new Detach(1), 4096);
    ByteBuffer two = ByteBuffer.allocate(next.length * 2).put(next).put(next).flip();
    assertEquals(new Wire.Known(new Detach(1)), decoder.feed(two));
    assertEquals(next.length, two.remaining());
    assertEquals(new Wire.Known(new Detach(1)), decoder.feed(two));
    assertEquals(
        new Wire.Ignored(0x80, 0),
        decoder.feed(ByteBuffer.wrap(new byte[] {(byte) 0x80, 0, 0, 0, 0})));
    for (int type : new int[] {0, 8, 127, 192, 255}) {
      ProtocolError e =
          assertThrows(
              ProtocolError.class, () -> Wire.decode(new byte[] {(byte) type, 0, 0, 0, 0}, 4096));
      assertEquals(
          type < 128 ? ProtocolError.Code.FRAME_ERROR : ProtocolError.Code.EXTENSION_UNSUPPORTED,
          e.code());
    }
  }

  @Test
  void schemaDirectedCborRejectsNonminimalOverlongTruncatedAndInvalidUnicode() {
    for (String bytes :
        List.of(
            "83000000",
            "9f000001ff",
            "9803000001",
            "8300001801",
            "8300001b8000000000000000",
            "83000020",
            "830000f5",
            "830000c001",
            "830000fa3f800000",
            "83000001f6",
            "9b7fffffffffffffff")) {
      assertThrows(
          ProtocolError.class,
          () -> Wire.decodeRecord(Wire.RecordKind.WORK_KEY, HEX.parseHex(bytes), 4096),
          bytes);
    }
    for (String bytes :
        List.of("820062c080", "820063eda080", "82006180", "82007f6161ff", "8200780161")) {
      assertThrows(
          ProtocolError.class,
          () -> Wire.decodeRecord(Wire.RecordKind.DIAGNOSTIC, HEX.parseHex(bytes), 4096),
          bytes);
    }
    Diagnostic exact = new Diagnostic(4294967295L, "\uD83D\uDE80".repeat(128));
    assertEquals(
        exact, Wire.decodeRecord(Wire.RecordKind.DIAGNOSTIC, Wire.encodeRecord(exact, 1024), 1024));
    assertThrows(ProtocolError.class, () -> new Diagnostic(0, "\uD83D\uDE80".repeat(129)));
    assertThrows(ProtocolError.class, () -> new Diagnostic(0, "\uD800"));
    assertThrows(ProtocolError.class, () -> new Diagnostic(0, "\uDC00"));
    for (long n :
        new long[] {1, 23, 24, 255, 256, 65535, 65536, 4294967295L, 4294967296L, Long.MAX_VALUE}) {
      WorkKey key = new WorkKey(n, 1, n);
      assertEquals(
          key, Wire.decodeRecord(Wire.RecordKind.WORK_KEY, Wire.encodeRecord(key, 128), 128));
    }
  }

  @Test
  void typedValuesOwnCollectionsAndDigestBytesAndRejectContradictions() {
    byte[] bytes = new byte[32];
    Digest digest = new Digest(bytes);
    bytes[0] = 1;
    assertEquals(new Digest(new byte[32]), digest);
    digest.bytes()[1] = 1;
    assertEquals(0, digest.bytes()[1]);
    byte[] id = new byte[16];
    id[0] = 1;
    OperationId operation = new OperationId(id);
    id[0] = 2;
    assertEquals(1, operation.bytes()[0]);
    List<Long> ids = new ArrayList<>(List.of(1L));
    Declare declare = new Declare(1, operation, 0, ids, true);
    ids.add(2L);
    assertEquals(List.of(1L), declare.entityIds());
    assertThrows(UnsupportedOperationException.class, () -> declare.entityIds().add(2L));
    assertThrows(ProtocolError.class, () -> new Counts(Long.MAX_VALUE, 1, 0, 0));
    assertThrows(ProtocolError.class, () -> new WorkKey(-1, 0, 1));
    assertThrows(
        ProtocolError.class, () -> new Retried(new WorkKey(0, 0, 1), Long.MAX_VALUE, 1, 0));
    assertThrows(ProtocolError.class, () -> new Retried(new WorkKey(0, 0, 1), 1, 3, 0));
    assertThrows(ProtocolError.class, () -> new OutputBudget(0, 1));
    assertThrows(ProtocolError.class, () -> new ChildScope(0, 1));
    assertThrows(ProtocolError.class, () -> new OperationId(new byte[16]));
    assertThrows(ProtocolError.class, () -> ProtocolError.Code.from(19));
  }

  @Test
  void capabilitySelectionNeverActivatesUnknownProfilesOrIncreasesOffers() {
    Capabilities offer =
        new Capabilities(
            false,
            List.of(77, DURABLE_WORK, RESULT_DELIVERY),
            List.of(RESULT_DELIVERY),
            65536,
            8,
            16,
            9999,
            5000,
            60000);
    Capabilities local =
        new Capabilities(
            false,
            List.of(77, DURABLE_WORK, RESULT_DELIVERY),
            List.of(DURABLE_WORK),
            8192,
            4,
            32,
            8888,
            3000,
            50000);
    Capabilities selected = Capabilities.negotiate(offer, local);
    offer.validateResponse(selected);
    assertEquals(List.of(DURABLE_WORK, RESULT_DELIVERY), selected.supported());
    assertEquals(selected.supported(), selected.required());
    assertEquals(8192, selected.controlLimit());
    assertEquals(4, selected.streamLimit());
    assertEquals(16, selected.pendingLimit());
    assertEquals(8888, selected.objectLimit());
    assertEquals(3000, selected.streamIdleMs());
    assertEquals(50000, selected.streamLifetimeMs());
    assertThrows(
        ProtocolError.class,
        () ->
            offer.validateResponse(
                new Capabilities(
                    true,
                    selected.supported(),
                    List.of(DURABLE_WORK),
                    8192,
                    4,
                    16,
                    8888,
                    3000,
                    50000)));
    assertThrows(
        ProtocolError.class,
        () ->
            offer.validateResponse(
                new Capabilities(
                    true,
                    selected.supported(),
                    selected.required(),
                    1048576,
                    4,
                    16,
                    8888,
                    3000,
                    50000)));
    Capabilities core = new Capabilities(false, List.of(), List.of(), 4096, 1, 1, 0, 1000, 1000);
    assertEquals(
        ProtocolError.Code.EXTENSION_UNSUPPORTED,
        assertThrows(ProtocolError.class, () -> Capabilities.negotiate(offer, core)).code());
  }

  @Test
  void outputLocatorsRequireExactNumericIdentityWithoutCredentialsOrResolution() throws Exception {
    Manifest manifest =
        (Manifest) Wire.decodeRecord(Wire.RecordKind.MANIFEST, named("result-manifest"), 4096);
    String valid = manifest.outputs().getFirst().locator().value();
    for (String invalid :
        List.of(
            valid.replace(":9443", ":09443"),
            valid.replace(":9443", ":65536"),
            valid + "?token=x",
            valid + "#part",
            valid.replace("processor.example", "alice@processor.example"),
            valid.replace("processor.example", "a..b"),
            valid.replace("/sessions/1", "/sessions/01"),
            valid.replace("/outputs/0", "/outputs/256"),
            valid.replace("/attempts/1", "/attempts/9223372036854775808"))) {
      assertThrows(ProtocolError.class, () -> new Locator(invalid), invalid);
    }
    assertDoesNotThrow(
        () ->
            new Locator(
                valid.replace("pipestream://processor.example", "PIPESTREAM://[2001:db8::1]")));
    Output old = manifest.outputs().getFirst();
    Output other =
        new Output(
            0,
            old.length(),
            old.sha256(),
            old.contentType(),
            new Locator(valid.replace("/entities/1", "/entities/2")));
    assertThrows(
        ProtocolError.class,
        () ->
            new Manifest(
                manifest.authority(),
                manifest.owner(),
                manifest.generation(),
                manifest.work(),
                manifest.attempt(),
                manifest.inputSha256(),
                manifest.committedAt(),
                manifest.availableUntil(),
                List.of(other)));
  }
}
