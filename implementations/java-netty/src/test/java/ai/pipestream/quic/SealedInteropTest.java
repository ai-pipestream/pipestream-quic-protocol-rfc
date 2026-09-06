package ai.pipestream.quic;

import static org.junit.jupiter.api.Assertions.*;

import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Real cross-implementation QUIC tests, enabled by the sealed-interop Maven profile. */
@Tag("sealed-interop")
final class SealedInteropTest {
  private static final UUID PRODUCER = UUID.fromString("01010101-0101-0101-0101-010101010101");
  private static final String SESSION = "java-sealed";
  @TempDir Path directory;

  @Test void javaProducerCompletesNestedChunkedWorkAgainstRust() throws Exception {
    Path certs = certificates();
    try (RustServer server = new RustServer(directory.resolve("rust"), certs);
        SealedClient client = connect(server, certs, SealedTransport.Limits.defaults())) {
      var rootBatch = declaration(0, null, 0, List.of(1L, 2L), null);
      client.declare(rootBatch);
      var root = new SealedWork.EntityKey(0, 1);
      send(client, root, null, "dehydrate", "root");
      send(client, new SealedWork.EntityKey(0, 2), null, "complete", "second-root");
      client.declare(declaration(7, root, 0, List.of(10L, 20L, 30L), List.of(10L, 20L, 30L)));
      var branch = new SealedWork.EntityKey(7, 10);
      send(client, new SealedWork.EntityKey(7, 30), root, "complete", "last-child-first");
      send(client, branch, root, "dehydrate", "branch");
      send(client, new SealedWork.EntityKey(7, 20), root, "complete", "middle-child");
      assertFalse(client.barrier(7).released());
      client.declare(declaration(9, branch, 0, List.of(1L, 2L), List.of(1L, 2L)));
      send(client, new SealedWork.EntityKey(9, 1), branch, "complete", "leaf");
      var first = chunk(new SealedWork.EntityKey(9, 2), branch, 0, 0, "abc");
      var second = chunk(new SealedWork.EntityKey(9, 2), branch, 1, 3, "def");
      assertEquals(3, client.sendChunks(List.of(second, first)).getLast().state());
      assertEquals(BigInteger.TWO, client.closeScope(9).succeeded());
      assertTrue(client.barrier(9).released());
      assertEquals(BigInteger.valueOf(3), client.closeScope(7).succeeded());
      assertTrue(client.barrier(7).released());
      var childCut = new SealedTransport.Checkpoint("child", SealedCbor.MAX_UINT, 30, 7L, 0, BigInteger.valueOf(2000));
      assertEquals(childCut.acknowledgement(), client.checkpoint(childCut));
      client.declare(declaration(0, null, 1, List.of(), List.of(1L, 2L)));
      assertEquals(rootBatch.acknowledgement(), client.declare(rootBatch));
      var cut = new SealedTransport.Checkpoint("root", BigInteger.ONE.shiftLeft(63), 2, null, 0, BigInteger.valueOf(2000));
      assertEquals(cut.acknowledgement(), client.checkpoint(cut));
      client.goaway(2);
    }
  }

  @Test void durableJavaProducerRestoresRecursiveWorkAcrossRustRestarts() throws Exception {
    Path certs = certificates(), state = directory.resolve("durable-rust");
    var durable = SealedClient.Durability.at(directory.resolve("producer.db"));
    var root = new SealedWork.EntityKey(0, 1);
    var cut = new SealedTransport.Checkpoint("durable-root", SealedCbor.MAX_UINT, 2, null, 0, BigInteger.valueOf(5000));
    try (var server = new RustServer(state, certs);
        var client = SealedClient.connectDurable(server.address, certs.resolve("ca.crt"), "localhost", SealedTransport.Limits.defaults(), Duration.ofSeconds(10), durable)) {
      client.declare(declaration(0, null, 0, List.of(1L, 2L), List.of(1L, 2L)));
      send(client, root, null, "dehydrate", "root");
      send(client, new SealedWork.EntityKey(0, 2), null, "complete", "other-root");
      client.declare(declaration(7, root, 0, List.of(10L, 20L), List.of(10L, 20L)));
      send(client, new SealedWork.EntityKey(7, 10), root, "complete", "child");
      var chunked = new SealedWork.EntityKey(7, 20);
      client.sendChunks(List.of(chunk(chunked, root, 1, 3, "def"), chunk(chunked, root, 0, 0, "abc")));
    }
    try (var server = new RustServer(state, certs);
        var client = SealedClient.connectDurable(server.address, certs.resolve("ca.crt"), "localhost", SealedTransport.Limits.defaults(), Duration.ofSeconds(10), durable)) {
      assertEquals(6, client.observedStatus(root).orElseThrow().state());
      assertEquals(3, client.observedStatus(new SealedWork.EntityKey(7, 20)).orElseThrow().state());
      assertTrue(client.unresolvedInputs().isEmpty());
      assertEquals(BigInteger.TWO, client.closeScope(7).succeeded());
      assertEquals(cut.acknowledgement(), client.checkpoint(cut));
    }
    try (var server = new RustServer(state, certs);
        var client = SealedClient.connectDurable(server.address, certs.resolve("ca.crt"), "localhost", SealedTransport.Limits.defaults(), Duration.ofSeconds(10), durable)) {
      assertEquals(3, client.observedStatus(root).orElseThrow().state());
      assertTrue(client.scopesAwaitingClosure().isEmpty());
      assertEquals(cut.acknowledgement(), client.checkpoint(cut)); client.goaway(2);
    }
  }

  @Test void durableDeclarationIntentSurvivesADroppedOrChangedAck() throws Exception {
    Path certs = certificates();
    var request = declaration(0, null, 0, List.of(1L, 2L), List.of(1L, 2L));
    for (boolean changed : List.of(false, true)) {
      var durable = SealedClient.Durability.at(directory.resolve("producer-" + changed + ".db"));
      var calls = new java.util.concurrent.atomic.AtomicInteger();
      var retained = SealedSessionStore.open(directory.resolve("receiver-" + changed + ".db"));
      try (var server = new SealedTestPeer.ScriptServer(certs, frame -> {
        if (frame.type() == Wire.FRAME_CAPABILITIES) return List.of(SealedTransport.capabilities(SealedTransport.Limits.defaults()));
        assertEquals(SealedWork.FRAME, frame.type());
        var declaration = SealedWork.decodePayload(frame.payload());
        var ack = retained.declare(declaration, 7, 16384);
        if (calls.incrementAndGet() == 1) {
          if (!changed) return List.of();
          return List.of(SealedWork.encode(new SealedWork.Declaration(ack.sessionId(), ack.producerId(), ack.scopeId(), ack.parent(),
              BigInteger.ONE, ack.entityIds(), ack.flags(), ack.sealDigest())));
        }
        return List.of(SealedWork.encode(ack));
      })) {
        try (var client = SealedClient.connectDurable(server.address(), certs.resolve("ca.crt"), "localhost", SealedTransport.Limits.defaults(), Duration.ofSeconds(1), durable)) {
          if (changed) assertEquals(5, assertThrows(ProtocolException.class, () -> client.declare(request)).errorCode());
          else assertThrows(java.util.concurrent.TimeoutException.class, () -> client.declare(request));
          assertEquals(List.of(request), client.declarationsAwaitingAcknowledgement());
          assertTrue(client.observedStatus(new SealedWork.EntityKey(0, 1)).isEmpty());
        }
        try (var client = SealedClient.connectDurable(server.address(), certs.resolve("ca.crt"), "localhost", SealedTransport.Limits.defaults(), Duration.ofSeconds(5), durable)) {
          assertTrue(client.declarationsAwaitingAcknowledgement().isEmpty());
          assertTrue(client.observedStatus(new SealedWork.EntityKey(0, 1)).isEmpty());
          assertEquals(request.acknowledgement(), client.declare(request));
        }
        assertEquals(3, calls.get());
      }
    }
  }

  @Test void javaReconnectReplaysDeclaredMembershipAfterRustRestartAndRejectsOwnerChanges() throws Exception {
    Path certs = certificates(), state = directory.resolve("restart");
    var root = declaration(0, null, 0, List.of(1L, 2L), List.of(1L, 2L));
    try (RustServer server = new RustServer(state, certs);
        SealedClient client = connect(server, certs, SealedTransport.Limits.defaults())) {
      assertEquals(root.acknowledgement(), client.declare(root));
    }
    try (RustServer server = new RustServer(state, certs)) {
      try (SealedClient wrongOwner = connect(server, certs, SealedTransport.Limits.defaults())) {
        var wrong = new SealedWork.Declaration(SESSION, new UUID(2, 3), 0, null, BigInteger.ZERO, root.entityIds(), root.flags(),
            SealedWork.sealDigest(SESSION, new UUID(2, 3), 0, null, root.entityIds()));
        assertEquals(Wire.ERROR_ENTITY_INVALID, assertThrows(ProtocolException.class, () -> wrongOwner.declare(wrong)).errorCode());
      }
      try (SealedClient tooSmall = connect(server, certs, new SealedTransport.Limits(1, 2, 2, BigInteger.valueOf(30_000)))) {
        assertEquals(Wire.ERROR_EXTENSION_UNSUPPORTED, assertThrows(ProtocolException.class, () -> tooSmall.declare(root)).errorCode());
      }
      assertDoesNotThrow(() -> { try (SealedClient client = connect(server, certs, SealedTransport.Limits.defaults())) {
        assertEquals(root.acknowledgement(), client.declare(root));
        send(client, new SealedWork.EntityKey(0, 2), null, "complete", "later-id-first");
        send(client, new SealedWork.EntityKey(0, 1), null, "complete", "first-id-last");
        client.checkpoint(new SealedTransport.Checkpoint("reopened", BigInteger.ONE, 2, 0L, 0, BigInteger.valueOf(2000)));
        client.goaway(2);
      } }, () -> read(server.log));
    }
    try (RustServer server = new RustServer(state, certs);
        var replayingPeer = new SealedTestPeer.RawClient(server.address, certs)) {
      replayingPeer.send(SealedWork.encode(root));
      SealedWork.requireAcknowledgement(root, SealedWork.decodePayload(replayingPeer.response().payload()));
      var request = new SealedTransport.Checkpoint("reopened", BigInteger.ONE, 2, 0L, 0, BigInteger.valueOf(2000));
      replayingPeer.send(SealedTransport.checkpoint(request));
      var response = replayingPeer.response();
      assertEquals(Wire.FRAME_CHECKPOINT, response.type());
      assertEquals(request.acknowledgement(), SealedTransport.checkpoint(response.payload()));
    }
  }

  @Test void discardedDeclarationAckReplaysThroughThePublicClientAfterRustRestart() throws Exception {
    Path certs = certificates(), state = directory.resolve("discarded-ack");
    var request = declaration(0, null, 0, List.of(1L, 2L), List.of(1L, 2L));
    try (RustServer server = new RustServer(state, certs);
        var droppingPeer = new SealedTestPeer.RawClient(server.address, certs)) {
      droppingPeer.send(SealedWork.encode(request));
      // Discard the reply at the transport boundary without giving it to a producer ledger.
      assertEquals(SealedWork.FRAME, droppingPeer.response().type());
    }
    try (RustServer server = new RustServer(state, certs);
        SealedClient client = connect(server, certs, SealedTransport.Limits.defaults())) {
      assertEquals(request.acknowledgement(), client.declare(request));
      send(client, new SealedWork.EntityKey(0, 1), null, "complete", "one");
      send(client, new SealedWork.EntityKey(0, 2), null, "complete", "two");
      client.checkpoint(new SealedTransport.Checkpoint("after-discard", BigInteger.ONE, 2, null, 0, BigInteger.valueOf(2000)));
      client.goaway(2);
    }
  }

  @Test void javaClientRejectsChangedDeclarationAcksAndCapabilityDowngradeOverQuic() throws Exception {
    Path certs = certificates();
    var request = declaration(0, null, 0, List.of(1L, 2L), List.of(1L, 2L));
    for (String mutation : List.of("session", "producer", "scope", "sequence", "flags", "ids", "seal", "downgrade", "oversized", "layer2")) {
      try (var server = new SealedTestPeer.ScriptServer(certs, frame -> {
        if (frame.type() == Wire.FRAME_CAPABILITIES) return List.of(mutation.equals("downgrade")
            ? Wire.encodeCapabilities(Wire.Capabilities.defaults()) : SealedTransport.capabilities(SealedTransport.Limits.defaults()));
        assertEquals(SealedWork.FRAME, frame.type());
        if (mutation.equals("oversized")) return List.of(java.nio.ByteBuffer.allocate(5).put((byte) SealedWork.FRAME).putInt(Wire.MAX_CONTROL_FRAME + 1).array());
        if (mutation.equals("layer2")) return List.of(Wire.encodeControl(0x84, new byte[0]));
        var expected = SealedWork.decodePayload(frame.payload()).acknowledgement();
        var changed = new SealedWork.Declaration(mutation.equals("session") ? "other" : expected.sessionId(),
            mutation.equals("producer") ? new UUID(2, 3) : expected.producerId(),
            mutation.equals("scope") ? 7 : expected.scopeId(),
            mutation.equals("scope") ? new SealedWork.EntityKey(0, 1) : expected.parent(),
            mutation.equals("sequence") ? expected.sequence().add(BigInteger.ONE) : expected.sequence(),
            mutation.equals("ids") ? List.of(1L) : expected.entityIds(),
            mutation.equals("flags") ? SealedWork.SEAL : expected.flags(), mutation.equals("seal") ? new byte[32] : expected.sealDigest());
        return List.of(SealedWork.encode(changed));
      })) {
        if (mutation.equals("downgrade")) {
          assertEquals(Wire.ERROR_EXTENSION_UNSUPPORTED, assertThrows(ProtocolException.class, () ->
              SealedClient.connect(server.address(), certs.resolve("ca.crt"), "localhost", SealedTransport.Limits.defaults(), Duration.ofSeconds(5))).errorCode());
        } else {
          try (SealedClient client = SealedClient.connect(server.address(), certs.resolve("ca.crt"), "localhost", SealedTransport.Limits.defaults(), Duration.ofSeconds(5))) {
            long expectedError = switch (mutation) {
              case "oversized" -> Wire.ERROR_LIMIT_EXCEEDED; case "layer2" -> Wire.ERROR_LAYER_UNSUPPORTED;
              default -> Wire.ERROR_ENTITY_INVALID;
            };
            assertEquals(expectedError, assertThrows(ProtocolException.class, () -> client.declare(request), mutation).errorCode());
          }
        }
      }
    }
  }

  @Test void javaClientReportsCheckpointTimeoutAndWrongSealedBoundWithoutCompletion() throws Exception {
    Path certs = certificates();
    for (boolean sealed : List.of(false, true)) {
      try (RustServer server = new RustServer(directory.resolve(sealed ? "wrong-bound" : "unsealed-timeout"), certs);
          SealedClient client = connect(server, certs, SealedTransport.Limits.defaults())) {
        var ids = List.of(1L, 2L);
        client.declare(declaration(0, null, 0, ids, sealed ? ids : null));
        send(client, new SealedWork.EntityKey(0, 1), null, "complete", "one");
        send(client, new SealedWork.EntityKey(0, 2), null, "complete", "two");
        var request = new SealedTransport.Checkpoint("not-ready", BigInteger.ONE, sealed ? 1 : 2, null, 0, BigInteger.valueOf(100));
        assertEquals(sealed ? Wire.ERROR_ENTITY_INVALID : 14, assertThrows(ProtocolException.class, () -> client.checkpoint(request)).errorCode());
      }
    }
  }

  private SealedClient.FileChunk chunk(SealedWork.EntityKey key, SealedWork.EntityKey parent, int index, int offset, String text) throws Exception {
    byte[] bytes = text.getBytes(StandardCharsets.UTF_8);
    Path path = Files.createTempFile(directory, "chunk-", ".bin"); Files.write(path, bytes);
    var base = header(key, parent, "complete", bytes);
    return new SealedClient.FileChunk(new SealedTransport.Header(key, parent, 0, base.contentType(), base.payloadLength(),
        base.checksum(), base.metadata(), new SealedTransport.Chunk(BigInteger.TWO, BigInteger.valueOf(index), BigInteger.valueOf(offset))), path);
  }

  private void send(SealedClient client, SealedWork.EntityKey key, SealedWork.EntityKey parent, String action, String text) throws Exception {
    byte[] bytes = text.getBytes(StandardCharsets.UTF_8);
    Path payload = Files.createTempFile(directory, "payload-", ".bin"); Files.write(payload, bytes);
    var statuses = client.send(header(key, parent, action, bytes), payload);
    assertEquals(2, statuses.getFirst().state());
    assertEquals(action.equals("dehydrate") ? 6 : 3, statuses.getLast().state());
  }

  private static SealedTransport.Header header(SealedWork.EntityKey key, SealedWork.EntityKey parent, String action, byte[] bytes) {
    return new SealedTransport.Header(key, parent, 0, "application/octet-stream", BigInteger.valueOf(bytes.length),
        SealedWork.sha256().digest(bytes), Map.of("pipestream.action", action, "pipestream.session-id", SESSION), null);
  }

  private static SealedWork.Declaration declaration(long scope, SealedWork.EntityKey parent, int sequence,
      List<Long> ids, List<Long> entire) throws Exception {
    return new SealedWork.Declaration(SESSION, PRODUCER, scope, parent, BigInteger.valueOf(sequence), ids,
        entire == null ? 0 : SealedWork.SEAL, entire == null ? null : SealedWork.sealDigest(SESSION, PRODUCER, scope, parent, entire));
  }

  private static SealedClient connect(RustServer server, Path certs, SealedTransport.Limits limits) throws Exception {
    return SealedClient.connect(server.address, certs.resolve("ca.crt"), "localhost", limits, Duration.ofSeconds(10));
  }

  private Path certificates() throws Exception {
    Path certs = Files.createDirectory(directory.resolve("certs"));
    command(certs, "openssl", "req", "-x509", "-newkey", "rsa:2048", "-noenc", "-keyout", "ca.key", "-out", "ca.crt",
        "-days", "2", "-subj", "/CN=Java-Sealed-Test-CA", "-addext", "basicConstraints=critical,CA:TRUE", "-addext", "keyUsage=critical,keyCertSign,cRLSign");
    command(certs, "openssl", "req", "-new", "-newkey", "rsa:2048", "-noenc", "-keyout", "server.key", "-out", "server.csr", "-subj", "/CN=localhost");
    Files.writeString(certs.resolve("extensions"), "subjectAltName=DNS:localhost\nbasicConstraints=critical,CA:FALSE\nkeyUsage=critical,digitalSignature\nextendedKeyUsage=serverAuth\n");
    command(certs, "openssl", "x509", "-req", "-in", "server.csr", "-CA", "ca.crt", "-CAkey", "ca.key", "-CAcreateserial",
        "-out", "server.crt", "-days", "2", "-extfile", "extensions");
    return certs;
  }

  private static void command(Path directory, String... args) throws Exception {
    Path log = Files.createTempFile(directory, "command-", ".log");
    Process process = new ProcessBuilder(args).directory(directory.toFile()).redirectErrorStream(true).redirectOutput(log.toFile()).start();
    try { assertTrue(process.waitFor(10, TimeUnit.SECONDS)); assertEquals(0, process.exitValue(), () -> read(log)); }
    finally { if (process.isAlive()) process.destroyForcibly().waitFor(5, TimeUnit.SECONDS); }
  }

  private static String read(Path file) {
    try { return Files.readString(file); } catch (Exception error) { return error.toString(); }
  }

  private static final class RustServer implements AutoCloseable {
    final Process process; final InetSocketAddress address; final Path log;
    RustServer(Path state, Path certs) throws Exception {
      Path binary = Path.of("../rust-quinn/target/release/pipestream-quinn").toAbsolutePath().normalize();
      assertTrue(Files.isExecutable(binary), "Build Rust first: cargo build --release --locked --workspace --manifest-path implementations/rust-quinn/Cargo.toml");
      Files.createDirectories(state);
      Path ready = state.resolve("ready-" + UUID.randomUUID()); log = state.resolve("server-" + UUID.randomUUID() + ".log");
      List<String> args = new ArrayList<>(List.of(binary.toString(), "serve-recursive", "--bind", "127.0.0.1:0",
          "--cert", certs.resolve("server.crt").toString(), "--key", certs.resolve("server.key").toString(),
          "--state-db", state.resolve("sessions.sqlite3").toString(), "--entity-dir", state.resolve("entities").toString(), "--ready-file", ready.toString()));
      process = new ProcessBuilder(args).redirectErrorStream(true).redirectOutput(log.toFile()).start();
      try {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (!Files.exists(ready)) {
          assertTrue(process.isAlive(), () -> read(log));
          assertTrue(System.nanoTime() < deadline, () -> "Rust server readiness timeout: " + read(log));
          Thread.sleep(10);
        }
        String[] parts = Files.readString(ready).trim().split(":");
        address = new InetSocketAddress(parts[0], Integer.parseInt(parts[1]));
      } catch (Throwable failure) { process.destroyForcibly().waitFor(5, TimeUnit.SECONDS); throw failure; }
    }
    @Override public void close() {
      process.destroy();
      try { if (!process.waitFor(5, TimeUnit.SECONDS)) process.destroyForcibly().waitFor(5, TimeUnit.SECONDS); }
      catch (InterruptedException failure) { Thread.currentThread().interrupt(); process.destroyForcibly(); }
    }
  }
}
