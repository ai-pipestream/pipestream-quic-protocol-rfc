package ai.pipestream.quic.v2;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * The independent client against a raw authority that misbehaves on the result stream: headers that
 * contradict the retained selection, unsolicited or duplicate deliveries, oversized or undecodable
 * headers, truncated and over-long payloads. A contradiction or bad payload fails only that
 * delivery and leaves the connection usable; a correlation or framing violation fails the
 * connection. Nothing is ever installed at the destination and no staging file survives.
 */
@Timeout(180)
final class DurableClientResultNegativeTest {
  static final Records.WorkKey WORK = new Records.WorkKey(0, 0, 1);
  static final Records.Policy POLICY = new Records.Policy(30_000, 60_000, 120_000);

  @TempDir static Path directory;
  static DurableTestPki pki;
  static Map<Records.Digest, String> principals;
  static byte[] payload;
  static Records.Digest digest;
  static Records.Manifest manifest;

  @BeforeAll
  static void setup() throws Exception {
    pki = DurableTestPki.generate(directory, List.of("alice"));
    principals = pki.principals(List.of("alice"));
    payload = new byte[50_000];
    new Random(21).nextBytes(payload);
    digest = DurableServerTest.digest(payload);
    manifest =
        new Records.Manifest(
            "issuer-a",
            "alice",
            1,
            WORK,
            1,
            DurableServerTest.digest(new byte[] {1}),
            1_000,
            2_000_000_000_000L,
            List.of(
                new Records.Output(
                    0,
                    payload.length,
                    digest,
                    "application/octet-stream",
                    new Locator(
                        "pipestream://localhost:7443/v2/sessions/1/scopes/0/producers/0/entities/1/attempts/1/outputs/0"))));
  }

  static Records.ResultHeader header(Messages.Read read, long length, Records.Digest sha256) {
    return new Records.ResultHeader(read.request(), 1, WORK, 1, 0, length, sha256);
  }

  /** One client bound, manifest observed and output selected against a fresh raw authority. */
  static final class Session implements AutoCloseable {
    final RawDurableAuthority authority;
    final ClientJournal journal;
    final DurableClient client;
    final Path outputs;

    Session(String name) throws Exception {
      authority = new RawDurableAuthority(pki.server(principals), manifest, POLICY);
      journal =
          ClientJournal.initialize(
              directory.resolve(name + ".sqlite"),
              new ClientJournal.Intent("issuer-a", "alice", 1, POLICY, true),
              ClientJournal.Limits.defaults());
      client =
          DurableClient.connect(
              authority.address(), pki.client("alice"), journal, ClientOptions.defaults());
      DurableClientTest.get(client.ready());
      DurableClientTest.get(client.binding());
      DurableClientTest.get(client.manifest(WORK, 1));
      DurableClientTest.get(client.select(WORK, 1, 0));
      outputs = Files.createDirectories(directory.resolve(name + "-outputs"));
    }

    ResultFiles.Destination destination(String file) {
      return new ResultFiles.Destination(outputs.resolve(file));
    }

    /** No installed file other than the named survivors and no staging leftovers. */
    void assertOnlyInstalled(String... survivors) throws Exception {
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
      List<String> names;
      do {
        try (Stream<Path> listing = Files.list(outputs)) {
          names = listing.map(p -> p.getFileName().toString()).sorted().toList();
        }
        if (names.equals(Arrays.stream(survivors).sorted().toList())) return;
        Thread.sleep(25);
      } while (System.nanoTime() < deadline);
      assertEquals(Arrays.stream(survivors).sorted().toList(), names, "output directory contents");
    }

    @Override
    public void close() throws java.io.IOException, java.sql.SQLException {
      client.close();
      journal.close();
      authority.close();
    }
  }

  @Test
  void aConformantDeliveryInstallsTheVerifiedBytes() throws Exception {
    try (Session session = new Session("valid")) {
      session.authority.script =
          (read, stream) -> {
            RawDurableAuthority.write(
                stream, RawDurableAuthority.header(header(read, payload.length, digest)));
            RawDurableAuthority.write(stream, payload);
            RawDurableAuthority.fin(stream);
          };
      ResultFiles.Delivered delivered =
          DurableClientTest.get(session.client.read(WORK, 1, 0, session.destination("out.bin")));
      assertEquals(payload.length, delivered.length());
      assertArrayEquals(payload, Files.readAllBytes(delivered.path()));
      session.assertOnlyInstalled("out.bin");
      DurableClientTest.get(session.client.detach());
    }
  }

  @Test
  void headersContradictingTheSelectionFailOnlyThatDelivery() throws Exception {
    try (Session session = new Session("contradiction")) {
      // Wrong length.
      session.authority.script =
          (read, stream) -> {
            RawDurableAuthority.write(
                stream, RawDurableAuthority.header(header(read, payload.length + 1, digest)));
            RawDurableAuthority.write(stream, payload);
            RawDurableAuthority.fin(stream);
          };
      ProtocolError length =
          DurableClientTest.refusal(session.client.read(WORK, 1, 0, session.destination("a.bin")));
      assertEquals(ProtocolError.Code.INTEGRITY_ERROR, length.code(), length.toString());
      // Wrong digest.
      Records.Digest other = DurableServerTest.digest(new byte[] {7});
      session.authority.script =
          (read, stream) -> {
            RawDurableAuthority.write(
                stream, RawDurableAuthority.header(header(read, payload.length, other)));
            RawDurableAuthority.write(stream, payload);
            RawDurableAuthority.fin(stream);
          };
      ProtocolError sha =
          DurableClientTest.refusal(session.client.read(WORK, 1, 0, session.destination("b.bin")));
      assertEquals(ProtocolError.Code.INTEGRITY_ERROR, sha.code(), sha.toString());
      // Wrong attempt in the header.
      session.authority.script =
          (read, stream) -> {
            RawDurableAuthority.write(
                stream,
                RawDurableAuthority.header(
                    new Records.ResultHeader(
                        read.request(), 1, WORK, 2, 0, payload.length, digest)));
            RawDurableAuthority.write(stream, payload);
            RawDurableAuthority.fin(stream);
          };
      ProtocolError attempt =
          DurableClientTest.refusal(session.client.read(WORK, 1, 0, session.destination("c.bin")));
      assertEquals(ProtocolError.Code.INTEGRITY_ERROR, attempt.code(), attempt.toString());
      session.assertOnlyInstalled();
      // The connection is still usable: a conformant delivery follows on the same session.
      session.authority.script =
          (read, stream) -> {
            RawDurableAuthority.write(
                stream, RawDurableAuthority.header(header(read, payload.length, digest)));
            RawDurableAuthority.write(stream, payload);
            RawDurableAuthority.fin(stream);
          };
      assertArrayEquals(
          payload,
          Files.readAllBytes(
              DurableClientTest.get(session.client.read(WORK, 1, 0, session.destination("d.bin")))
                  .path()));
      // S12-078: every rejected delivery was aborted locally; the client never answered the
      // authority with a REFUSAL in the wrong direction. The raw authority records every frame.
      assertTrue(
          session.authority.received.stream().noneMatch(Messages.Refusal.class::isInstance),
          "client sent a refusal: " + session.authority.received);
      assertTrue(
          session.authority.received.stream().filter(Messages.Read.class::isInstance).count() >= 4,
          "the reads were recorded: " + session.authority.received);
      DurableClientTest.get(session.client.detach());
    }
  }

  @Test
  void payloadsThatDoNotMatchTheHeaderFailOnlyThatDelivery() throws Exception {
    try (Session session = new Session("payload")) {
      // Truncated: FIN before the declared length.
      session.authority.script =
          (read, stream) -> {
            RawDurableAuthority.write(
                stream, RawDurableAuthority.header(header(read, payload.length, digest)));
            RawDurableAuthority.write(stream, Arrays.copyOf(payload, payload.length / 2));
            RawDurableAuthority.fin(stream);
          };
      ProtocolError truncated =
          DurableClientTest.refusal(session.client.read(WORK, 1, 0, session.destination("a.bin")));
      assertEquals(ProtocolError.Code.INTEGRITY_ERROR, truncated.code(), truncated.toString());
      // Trailing bytes beyond the declared length.
      session.authority.script =
          (read, stream) -> {
            RawDurableAuthority.write(
                stream, RawDurableAuthority.header(header(read, payload.length, digest)));
            RawDurableAuthority.write(stream, payload);
            RawDurableAuthority.write(stream, new byte[] {0});
            RawDurableAuthority.fin(stream);
          };
      ProtocolError trailing =
          DurableClientTest.refusal(session.client.read(WORK, 1, 0, session.destination("b.bin")));
      assertEquals(ProtocolError.Code.INTEGRITY_ERROR, trailing.code(), trailing.toString());
      // A duplicate header where payload belongs: right length, wrong bytes.
      session.authority.script =
          (read, stream) -> {
            byte[] encoded = RawDurableAuthority.header(header(read, payload.length, digest));
            RawDurableAuthority.write(stream, encoded);
            byte[] corrupt = Arrays.copyOf(payload, payload.length);
            System.arraycopy(encoded, 0, corrupt, 0, Math.min(encoded.length, corrupt.length));
            RawDurableAuthority.write(stream, corrupt);
            RawDurableAuthority.fin(stream);
          };
      ProtocolError corrupt =
          DurableClientTest.refusal(session.client.read(WORK, 1, 0, session.destination("c.bin")));
      assertEquals(ProtocolError.Code.INTEGRITY_ERROR, corrupt.code(), corrupt.toString());
      // Reset before FIN.
      session.authority.script =
          (read, stream) -> {
            RawDurableAuthority.write(
                stream, RawDurableAuthority.header(header(read, payload.length, digest)));
            RawDurableAuthority.write(stream, Arrays.copyOf(payload, 1000));
            stream.shutdownOutput(0x20d).sync();
          };
      ProtocolError reset =
          DurableClientTest.refusal(session.client.read(WORK, 1, 0, session.destination("e.bin")));
      assertTrue(
          reset.code() == ProtocolError.Code.INTEGRITY_ERROR
              || reset.code() == ProtocolError.Code.CONTROL_RESET,
          reset.toString());
      session.assertOnlyInstalled();
      session.authority.script =
          (read, stream) -> {
            RawDurableAuthority.write(
                stream, RawDurableAuthority.header(header(read, payload.length, digest)));
            RawDurableAuthority.write(stream, payload);
            RawDurableAuthority.fin(stream);
          };
      assertArrayEquals(
          payload,
          Files.readAllBytes(
              DurableClientTest.get(session.client.read(WORK, 1, 0, session.destination("d.bin")))
                  .path()));
      session.assertOnlyInstalled("d.bin");
      // S12-078 again for payload failures: aborted locally, never answered with a REFUSAL.
      assertTrue(
          session.authority.received.stream().noneMatch(Messages.Refusal.class::isInstance),
          "client sent a refusal: " + session.authority.received);
      DurableClientTest.get(session.client.detach());
    }
  }

  @Test
  void unsolicitedResultStreamsFailTheConnection() throws Exception {
    try (Session session = new Session("unsolicited")) {
      session.authority.script =
          (read, stream) -> {
            RawDurableAuthority.write(
                stream,
                RawDurableAuthority.header(
                    new Records.ResultHeader(
                        read.request() + 1000, 1, WORK, 1, 0, payload.length, digest)));
            RawDurableAuthority.write(stream, payload);
            RawDurableAuthority.fin(stream);
          };
      ProtocolError failure =
          DurableClientTest.refusal(session.client.read(WORK, 1, 0, session.destination("a.bin")));
      assertEquals(ProtocolError.Code.FRAME_ERROR, failure.code(), failure.toString());
      DurableClientTest.refusal(session.client.closed());
      session.assertOnlyInstalled();
    }
  }

  @Test
  void duplicateResultStreamsForOneReadFailTheConnection() throws Exception {
    try (Session session = new Session("duplicate")) {
      session.authority.script =
          (read, stream) -> {
            byte[] encoded = RawDurableAuthority.header(header(read, payload.length, digest));
            RawDurableAuthority.write(stream, encoded);
            QuicStreamChannelHolder second = new QuicStreamChannelHolder();
            stream
                .parent()
                .createStream(
                    io.netty.handler.codec.quic.QuicStreamType.UNIDIRECTIONAL,
                    new io.netty.channel.ChannelInboundHandlerAdapter())
                .addListener(opened -> second.complete(opened));
            RawDurableAuthority.write(second.await(), encoded);
          };
      ProtocolError failure =
          DurableClientTest.refusal(session.client.read(WORK, 1, 0, session.destination("a.bin")));
      assertEquals(ProtocolError.Code.FRAME_ERROR, failure.code(), failure.toString());
      DurableClientTest.refusal(session.client.closed());
      session.assertOnlyInstalled();
    }
  }

  @Test
  void oversizedOrUndecodableHeadersFailTheConnection() throws Exception {
    try (Session session = new Session("oversize")) {
      // Length prefix beyond the header ceiling.
      session.authority.script =
          (read, stream) -> {
            RawDurableAuthority.write(
                stream, java.nio.ByteBuffer.allocate(4).putInt(Wire.HEADER_LIMIT + 1).array());
            RawDurableAuthority.write(stream, new byte[64]);
          };
      ProtocolError oversize =
          DurableClientTest.refusal(session.client.read(WORK, 1, 0, session.destination("a.bin")));
      assertEquals(ProtocolError.Code.FRAME_ERROR, oversize.code(), oversize.toString());
      DurableClientTest.refusal(session.client.closed());
      session.assertOnlyInstalled();
    }
    try (Session session = new Session("garbage")) {
      // Well-formed length, undecodable body.
      session.authority.script =
          (read, stream) -> {
            byte[] junk = new byte[36];
            new Random(5).nextBytes(junk);
            java.nio.ByteBuffer.wrap(junk).putInt(32);
            RawDurableAuthority.write(stream, junk);
            RawDurableAuthority.fin(stream);
          };
      ProtocolError garbage =
          DurableClientTest.refusal(session.client.read(WORK, 1, 0, session.destination("a.bin")));
      assertEquals(ProtocolError.Code.FRAME_ERROR, garbage.code(), garbage.toString());
      DurableClientTest.refusal(session.client.closed());
      session.assertOnlyInstalled();
      assertFalse(Files.exists(session.outputs.resolve("a.bin")));
    }
  }

  /** Bridges a Netty stream-creation future to the script thread. */
  static final class QuicStreamChannelHolder {
    private final java.util.concurrent.CompletableFuture<
            io.netty.handler.codec.quic.QuicStreamChannel>
        future = new java.util.concurrent.CompletableFuture<>();

    void complete(io.netty.util.concurrent.Future<?> opened) {
      if (opened.isSuccess())
        future.complete((io.netty.handler.codec.quic.QuicStreamChannel) opened.getNow());
      else future.completeExceptionally(opened.cause());
    }

    io.netty.handler.codec.quic.QuicStreamChannel await() throws Exception {
      return future.get(5, TimeUnit.SECONDS);
    }
  }
}
