package ai.pipestream.quic;

import static org.junit.jupiter.api.Assertions.*;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.incubator.codec.quic.QuicStreamChannel;
import io.netty.incubator.codec.quic.QuicStreamType;
import java.io.InputStream;
import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.DriverManager;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.io.TempDir;

final class SealedServerTest {
  private static final UUID PRODUCER = new UUID(1, 2);
  private static final String SESSION = "java-listener";
  private static final SealedWork.EntityKey ROOT = new SealedWork.EntityKey(0, 1);
  @TempDir Path directory;

  @Test @Tag("sealed-interop") void rustPublicProducerRunsRecursiveReplayAndRefusalScenariosAgainstJava() throws Exception {
    Path certs = certificates(); var calls = new AtomicInteger();
    try (var fixture = new Fixture(certs, (context, input) -> {
      calls.incrementAndGet();
      if (context.operation() == SealedExecutor.Operation.PROCESS && "dehydrate".equals(context.header().metadata().get("pipestream.action"))) {
        input.transferTo(java.io.OutputStream.nullOutputStream()); return new SealedExecutor.Decision(6, null);
      }
      return complete(input);
    })) {
      Path binary = Path.of("../rust-quinn/target/release/pipestream-quinn").toAbsolutePath().normalize();
      assertTrue(Files.isExecutable(binary), "Build the Rust public scenario client before running interop tests");
      Path log = directory.resolve("rust-producer.log");
      Process process = new ProcessBuilder(binary.toString(), "sealed-scenario", "--connect", "127.0.0.1:" + fixture.server.address().getPort(),
          "--ca", certs.resolve("ca.crt").toString(), "--session-id", "rust-to-java")
          .redirectErrorStream(true).redirectOutput(log.toFile()).start();
      try {
        assertTrue(process.waitFor(40, TimeUnit.SECONDS), () -> "Rust scenario timed out: " + log);
        assertEquals(0, process.exitValue(), () -> { try { return Files.readString(log); } catch (Exception failure) { return failure.toString(); } });
        assertTrue(Files.readString(log).contains("SEALED_OK rust-to-java")); assertEquals(9, calls.get());
      } finally { if (process.isAlive()) process.destroyForcibly().waitFor(5, TimeUnit.SECONDS); }
    }
  }

  @Test void publicProducerCompletesMultipleRootsNestedScopesAndOutOfOrderChunks() throws Exception {
    Path certs = certificates(); var calls = new AtomicInteger();
    try (var fixture = new Fixture(certs, (context, input) -> {
      calls.incrementAndGet();
      if (context.operation() == SealedExecutor.Operation.PROCESS && "dehydrate".equals(context.header().metadata().get("action"))) {
        input.transferTo(java.io.OutputStream.nullOutputStream()); return new SealedExecutor.Decision(6, null);
      }
      return complete(input);
    }); var client = connect(fixture.server, certs)) {
      client.declare(declaration(0, null, 0, List.of(1L, 2L), null));
      assertEquals(6, send(client, ROOT, null, "dehydrate", "root").getLast().state());
      send(client, new SealedWork.EntityKey(0, 2), null, "complete", "other-root");
      var branch = new SealedWork.EntityKey(7, 10);
      client.declare(declaration(7, ROOT, 0, List.of(10L, 20L), List.of(10L, 20L)));
      send(client, branch, ROOT, "dehydrate", "branch");
      send(client, new SealedWork.EntityKey(7, 20), ROOT, "complete", "sibling");
      assertFalse(client.barrier(7).released());
      client.declare(declaration(9, branch, 0, List.of(1L, 2L), List.of(1L, 2L)));
      send(client, new SealedWork.EntityKey(9, 1), branch, "complete", "leaf");
      var chunked = new SealedWork.EntityKey(9, 2);
      var first = chunk(chunked, branch, 0, 0, "abc");
      var second = chunk(chunked, branch, 1, 3, "def");
      assertEquals(3, client.sendChunks(List.of(second, first)).getLast().state());
      assertEquals(BigInteger.TWO, client.closeScope(9).succeeded());
      assertTrue(client.barrier(9).released());
      assertEquals(BigInteger.TWO, client.closeScope(7).succeeded());
      client.declare(declaration(0, null, 1, List.of(), List.of(1L, 2L)));
      var cut = checkpoint("root", SealedCbor.MAX_UINT, 2, null, 5000);
      assertEquals(cut.acknowledgement(), client.checkpoint(cut));
      client.goaway(2); assertEquals(8, calls.get());
    }
  }

  @Test void pendingCheckpointDoesNotBlockPayloadsOrIndependentCompletionAndCannotOvertakeStatuses() throws Exception {
    Path certs = certificates(); var entered = new CountDownLatch(1); var release = new CountDownLatch(1);
    try (var fixture = new Fixture(certs, (context, input) -> {
      if (context.identity().entity().equals(ROOT)) { entered.countDown(); if (!release.await(10, TimeUnit.SECONDS)) throw new IllegalStateException("test callback not released"); }
      return complete(input);
    }); var raw = new SealedTestPeer.RawClient(fixture.server.address(), certs)) {
      try {
        declare(raw, declaration(0, null, 0, List.of(1L, 2L), List.of(1L, 2L)));
        var cut = checkpoint("pending", BigInteger.ONE, 2, 0L, 5000);
        raw.send(SealedTransport.checkpoint(cut));
        payload(raw, ROOT, null, "first", true);
        assertTrue(entered.await(5, TimeUnit.SECONDS));
        payload(raw, new SealedWork.EntityKey(0, 2), null, "second", true);
        int processing = 0;
        while (true) {
          var frame = raw.response(); assertEquals(Wire.FRAME_STATUS, frame.type());
          var status = SealedTransport.status(frame.payload());
          if (status.state() == 2) processing++;
          if (status.state() == 3) { assertEquals(2, status.entityId()); break; }
        }
        assertEquals(2, processing);
        release.countDown();
        assertEquals(3, SealedTransport.status(raw.response().payload()).state());
        var response = raw.response(); assertEquals(Wire.FRAME_CHECKPOINT, response.type());
        assertEquals(cut.acknowledgement(), SealedTransport.checkpoint(response.payload()));
      } finally { release.countDown(); }
    }
  }

  @Test void heldSqliteCannotStopCheckpointDeadlineOrImmediateProtocolRefusal() throws Exception {
    Path certs = certificates();
    try (var fixture = new Fixture(certs, (context, input) -> complete(input));
        var raw = new SealedTestPeer.RawClient(fixture.server.address(), certs);
        var second = new SealedTestPeer.RawClient(fixture.server.address(), certs)) {
      var root = declaration(0, null, 0, List.of(1L), List.of(1L)); declare(raw, root); declare(second, root);
      try (var sql = DriverManager.getConnection("jdbc:sqlite:" + fixture.database); var statement = sql.createStatement()) {
        statement.execute("BEGIN IMMEDIATE");
        try {
          long start = System.nanoTime();
          raw.send(SealedTransport.checkpoint(checkpoint("deadline", BigInteger.ZERO, 1, null, 150)));
          assertEquals(14L, raw.closeCode.get(2, TimeUnit.SECONDS));
          assertTrue(System.nanoTime() - start < TimeUnit.SECONDS.toNanos(2));
          second.send(SealedTransport.checkpoint(checkpoint("queued", BigInteger.ZERO, 1, null, 5000)));
          second.send(SealedTransport.capabilities(SealedTransport.Limits.defaults()));
          assertEquals(Wire.ERROR_FRAME, second.closeCode.get(2, TimeUnit.SECONDS));
        } finally { statement.execute("ROLLBACK"); }
      }
      assertFalse(fixture.sessions.checkpointReady(SESSION, PRODUCER, 0, 1));
    }
  }

  @Test void walCapacityRefusesOverQuicWithoutLosingDeclaredWorkAndReplayResumesAfterCheckpoint() throws Exception {
    Path certs = certificates(), database = directory.resolve("sessions.sqlite3");
    var policy = new SealedSessionStore.FileLimits(4L << 20, 65536, 2L << 20, 65536);
    SealedSessionStore.open(database, policy);
    var accepted = new java.util.ArrayList<SealedWork.Declaration>();
    var root = declaration(0, null, 0, List.of(1L), null);
    accepted.add(root);
    SealedWork.Declaration refused = null;
    try (var fixture = new Fixture(certs, (context, input) -> complete(input))) {
      var files = SealedSqliteFiles.open(database, policy);
      try (var client = connect(fixture.server, certs); var reader = files.connect(); var statement = reader.createStatement()) {
        client.declare(root);
        statement.execute("BEGIN");
        try {
          try (var rows = statement.executeQuery("SELECT count(*) FROM ps_java_entities")) {
            assertTrue(rows.next()); assertEquals(1, rows.getLong(1));
          }
          for (int batch = 1; batch < 128; batch++) {
            long first = (batch - 1) * 8L + 2;
            var request = declaration(0, null, batch, java.util.stream.LongStream.range(first, first + 8).boxed().toList(), null);
            try { assertEquals(request.acknowledgement(), client.declare(request)); accepted.add(request); }
            catch (ProtocolException full) {
              assertEquals(Wire.ERROR_LIMIT_EXCEEDED, full.errorCode()); refused = request; break;
            }
          }
          assertNotNull(refused, "real WAL exhaustion must reach the public QUIC client");
          long committed = accepted.stream().mapToLong(batch -> batch.entityIds().size()).sum();
          assertEquals(committed, fixture.sessions.declared(SESSION, PRODUCER, 0).size());
          assertFalse(fixture.sessions.checkpointReady(SESSION, PRODUCER, 0, committed));
          assertEquals(0, fixture.payloads.usage().retainedFiles());
          assertTrue(fixture.sessions.fileUsage().walBytes() <= policy.walBytes());
          try (var rows = statement.executeQuery("SELECT count(*) FROM ps_java_entities")) {
            assertTrue(rows.next()); assertEquals(1, rows.getLong(1), "held snapshot must not advance");
          }
        } finally { statement.execute("ROLLBACK"); }
        try (var checkpoint = statement.executeQuery("PRAGMA wal_checkpoint(TRUNCATE)")) {
          assertTrue(checkpoint.next()); assertEquals(0, checkpoint.getInt(1));
        }
        try (var rows = statement.executeQuery("SELECT count(*) FROM ps_java_jobs")) {
          assertTrue(rows.next()); assertEquals(0, rows.getLong(1));
        }
      }
      try (var replay = connect(fixture.server, certs)) {
        for (var request : accepted) assertEquals(request.acknowledgement(), replay.declare(request));
        assertEquals(refused.acknowledgement(), replay.declare(refused));
      }
      assertTrue(fixture.server.failure().isEmpty());
    }
    assertEquals(policy, SealedSessionStore.open(database).fileLimits());
    assertEquals(refused.acknowledgement(), SealedSessionStore.open(database).declare(refused, 7, 16384));
  }

  @Test void resetStreamNeverCompletesDeclaredWorkAndCheckpointAckReplaysAfterRestart() throws Exception {
    Path certs = certificates(); var calls = new AtomicInteger();
    SealedExecutor.Processor processor = (context, input) -> { calls.incrementAndGet(); return complete(input); };
    var root = declaration(0, null, 0, List.of(1L), List.of(1L));
    var cut = checkpoint("retained", BigInteger.ONE.shiftLeft(63), 1, 0L, 5000);
    try (var fixture = new Fixture(certs, processor); var raw = new SealedTestPeer.RawClient(fixture.server.address(), certs)) {
      declare(raw, root);
      var stream = payload(raw, ROOT, null, "unfinished", false);
      stream.close().sync();
      assertFalse(fixture.sessions.checkpointReady(SESSION, PRODUCER, 0, 1));
    }
    assertEquals(0, calls.get());
    try (var fixture = new Fixture(certs, processor); var client = connect(fixture.server, certs)) {
      client.declare(root); send(client, ROOT, null, "complete", "finished");
      assertEquals(cut.acknowledgement(), client.checkpoint(cut));
    }
    try (var fixture = new Fixture(certs, processor); var raw = new SealedTestPeer.RawClient(fixture.server.address(), certs)) {
      declare(raw, root); raw.send(SealedTransport.checkpoint(cut));
      assertEquals(cut.acknowledgement(), SealedTransport.checkpoint(raw.response().payload()));
      var changed = checkpoint("changed", cut.sequence(), 1, 0L, 5000);
      raw.send(SealedTransport.checkpoint(changed));
      assertEquals(Wire.ERROR_ENTITY_INVALID, raw.closeCode.get(5, TimeUnit.SECONDS));
    }
    assertEquals(1, calls.get());
  }

  @Test void forgedScopeSummaryRollsBackClosureAndCorrectReplayPropagatesStrictFailure() throws Exception {
    Path certs = certificates(); var rehydrations = new AtomicInteger();
    try (var fixture = new Fixture(certs, (context, input) -> {
      if (context.operation() == SealedExecutor.Operation.REHYDRATE) rehydrations.incrementAndGet();
      input.transferTo(java.io.OutputStream.nullOutputStream());
      return new SealedExecutor.Decision(context.identity().entity().scopeId() == 0 ? 6 : 4, null);
    })) {
      var root = declaration(0, null, 0, List.of(1L), List.of(1L));
      var child = declaration(7, ROOT, 0, List.of(1L), List.of(1L));
      try (var client = connect(fixture.server, certs)) {
        client.declare(root); send(client, ROOT, null, "dehydrate", "root");
        client.declare(child); assertEquals(4, send(client, new SealedWork.EntityKey(7, 1), ROOT, "failed", "child").getLast().state());
      }
      try (var raw = new SealedTestPeer.RawClient(fixture.server.address(), certs)) {
        declare(raw, root);
        var forged = SealedScope.summarize(7, List.of(new SealedScope.Terminal(1, 3)));
        raw.send(SealedScope.encode(forged)); assertEquals(4L, raw.closeCode.get(5, TimeUnit.SECONDS));
        assertFalse(fixture.sessions.ancestry(SESSION, PRODUCER, 7).getFirst().closed());
        assertEquals(6, fixture.sessions.describe(SESSION, PRODUCER, ROOT).state());
      }
      try (var raw = new SealedTestPeer.RawClient(fixture.server.address(), certs)) {
        declare(raw, root);
        var correct = SealedScope.summarize(7, List.of(new SealedScope.Terminal(1, 4)));
        raw.send(SealedScope.encode(correct)); assertEquals(SealedScope.FRAME, raw.response().type());
        assertEquals(4, SealedTransport.status(raw.response().payload()).state());
        var cut = checkpoint("strict-failure", BigInteger.ONE, 1, null, 5000);
        raw.send(SealedTransport.checkpoint(cut)); assertEquals(cut.acknowledgement(), SealedTransport.checkpoint(raw.response().payload()));
      }
      assertEquals(0, rehydrations.get());
    }
  }

  @Test void duplicateCheckpointDoesNotExtendDeadlineAndStorageBacklogHasNamedBound() throws Exception {
    Path certs = certificates();
    try (var fixture = new Fixture(certs, (context, input) -> complete(input));
        var raw = new SealedTestPeer.RawClient(fixture.server.address(), certs);
        var flooded = new SealedTestPeer.RawClient(fixture.server.address(), certs)) {
      var root = declaration(0, null, 0, List.of(1L), List.of(1L)); declare(raw, root); declare(flooded, root);
      try (var sql = DriverManager.getConnection("jdbc:sqlite:" + fixture.database); var statement = sql.createStatement()) {
        statement.execute("BEGIN IMMEDIATE");
        try {
          var request = SealedTransport.checkpoint(checkpoint("duplicate", BigInteger.ONE, 1, null, 500));
          raw.send(request); Thread.sleep(300); raw.send(request);
          // Extending on the duplicate would leave another 500 ms, not this 300 ms budget.
          assertEquals(14L, raw.closeCode.get(300, TimeUnit.MILLISECONDS));
          for (int index = 0; index < 34; index++) flooded.send(SealedWork.encode(root));
          assertEquals(6L, flooded.closeCode.get(2, TimeUnit.SECONDS));
          fixture.server.close(); assertFalse(fixture.server.isTerminated(), "started SQLite operations retain physical ownership");
        } finally { statement.execute("ROLLBACK"); }
      }
    }
  }

  @Test void unobservedDeclarationAndCheckpointAcksReplayAfterServerRestart() throws Exception {
    Path certs = certificates(); var root = declaration(0, null, 0, List.of(1L), List.of(1L));
    var cut = checkpoint("dropped-ack", BigInteger.ONE, 1, null, 5000); var calls = new AtomicInteger();
    SealedExecutor.Processor processor = (context, input) -> { calls.incrementAndGet(); return complete(input); };
    try (var fixture = new Fixture(certs, processor); var raw = new SealedTestPeer.RawClient(fixture.server.address(), certs)) {
      raw.send(SealedWork.encode(root)); awaitRows(fixture.database, "SELECT count(*) FROM ps_java_batches", 1);
      // No producer ledger observes the response before this connection and listener stop.
    }
    try (var fixture = new Fixture(certs, processor); var raw = new SealedTestPeer.RawClient(fixture.server.address(), certs)) {
      declare(raw, root); payload(raw, ROOT, null, "retained", true);
      assertEquals(2, SealedTransport.status(raw.response().payload()).state());
      assertEquals(3, SealedTransport.status(raw.response().payload()).state());
      raw.send(SealedTransport.checkpoint(cut)); awaitRows(fixture.database, "SELECT count(*) FROM ps_java_checkpoints WHERE acknowledged=1", 1);
    }
    try (var fixture = new Fixture(certs, processor); var raw = new SealedTestPeer.RawClient(fixture.server.address(), certs)) {
      declare(raw, root); raw.send(SealedTransport.checkpoint(cut));
      assertEquals(cut.acknowledgement(), SealedTransport.checkpoint(raw.response().payload()));
    }
    assertEquals(1, calls.get());
  }

  private static void awaitRows(Path database, String query, int expected) throws Exception {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    while (true) {
      try (var sql = DriverManager.getConnection("jdbc:sqlite:" + database); var statement = sql.createStatement(); var rows = statement.executeQuery(query)) {
        assertTrue(rows.next()); if (rows.getInt(1) == expected) return;
      }
      assertTrue(System.nanoTime() < deadline, query); Thread.sleep(5);
    }
  }

  @Test void realQuicTransfersAndProcesses32MiBUnderA24MiBJavaHeap() throws Exception {
    Path certs = certificates(), log = directory.resolve("heap-gate.log");
    Process process = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin/java").toString(), "-Xmx24m",
        "--enable-native-access=ALL-UNNAMED", "-cp", System.getProperty("java.class.path"), SealedServerTest.class.getName(),
        directory.toString(), certs.toString()).redirectErrorStream(true).redirectOutput(log.toFile()).start();
    try {
      assertTrue(process.waitFor(45, TimeUnit.SECONDS));
      assertEquals(0, process.exitValue(), () -> { try { return Files.readString(log); } catch (Exception failure) { return failure.toString(); } });
    } finally { if (process.isAlive()) process.destroyForcibly().waitFor(5, TimeUnit.SECONDS); }
  }

  public static void main(String[] args) throws Exception {
    var test = new SealedServerTest(); test.directory = Path.of(args[0]); Path certs = Path.of(args[1]);
    Path input = test.directory.resolve("large.bin"); long length = 32L << 20; var hash = SealedWork.sha256();
    try (var output = Files.newOutputStream(input)) {
      byte[] buffer = new byte[8192];
      for (long offset = 0; offset < length; offset += buffer.length) { buffer[0] = (byte) (offset / buffer.length); output.write(buffer); hash.update(buffer); }
    }
    byte[] digest = hash.digest();
    try (var fixture = test.new Fixture(certs, (context, source) -> complete(source));
        var client = SealedClient.connect(fixture.server.address(), certs.resolve("ca.crt"), "localhost", SealedTransport.Limits.defaults(), Duration.ofSeconds(30))) {
      client.declare(declaration(0, null, 0, List.of(1L), List.of(1L)));
      var header = new SealedTransport.Header(ROOT, null, 0, null, BigInteger.valueOf(length), digest, Map.of(), null);
      assertEquals(3, client.send(header, input).getLast().state());
      client.checkpoint(checkpoint("heap", BigInteger.ONE, 1, null, 5000)); client.goaway(1);
      var stored = fixture.payloads.find(new SealedPayloadStore.Identity(SESSION, PRODUCER, ROOT)).orElseThrow();
      assertEquals(length, stored.length()); assertArrayEquals(digest, stored.digest());
      assertEquals(0, fixture.payloads.usage().temporaryBytes()); assertEquals(0, fixture.payloads.usage().activeHandles());
    }
  }

  private final class Fixture implements AutoCloseable {
    final Path database = directory.resolve("sessions.sqlite3");
    final SealedSessionStore sessions;
    final SealedPayloadStore payloads;
    final SealedServer server;
    Fixture(Path certs, SealedExecutor.Processor processor) throws Exception {
      sessions = SealedSessionStore.open(database);
      payloads = SealedPayloadStore.open(directory.resolve("payloads"), SealedPayloadStore.Limits.defaults());
      try {
        server = SealedServer.start(new InetSocketAddress("127.0.0.1", 0), certs.resolve("server.crt"), certs.resolve("server.key"),
            sessions, payloads, processor, SealedTransport.Limits.defaults(), new SealedExecutor.Limits(4, 2, Duration.ofSeconds(30)));
      } catch (Exception failure) { payloads.close(); throw failure; }
    }
    @Override public void close() throws java.io.IOException {
      server.close(); long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
      while (!server.isTerminated() && System.nanoTime() < deadline) {
        try { Thread.sleep(10); }
        catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); throw new java.io.IOException("interrupted during test shutdown", interrupted); }
      }
      assertTrue(server.isTerminated(), "physical server work must stop before closing payload storage"); payloads.close();
    }
  }

  private static SealedExecutor.Decision complete(InputStream input) throws Exception {
    var hash = SealedWork.sha256(); byte[] buffer = new byte[8192]; int count;
    while ((count = input.read(buffer)) >= 0) hash.update(buffer, 0, count);
    return new SealedExecutor.Decision(3, hash.digest());
  }
  private static SealedClient connect(SealedServer server, Path certs) throws Exception {
    return SealedClient.connect(server.address(), certs.resolve("ca.crt"), "localhost", SealedTransport.Limits.defaults(), Duration.ofSeconds(10));
  }
  private static SealedWork.Declaration declaration(long scope, SealedWork.EntityKey parent, long sequence, List<Long> ids, List<Long> whole) throws Exception {
    return new SealedWork.Declaration(SESSION, PRODUCER, scope, parent, BigInteger.valueOf(sequence), ids,
        whole == null ? 0 : SealedWork.SEAL, whole == null ? null : SealedWork.sealDigest(SESSION, PRODUCER, scope, parent, whole));
  }
  private static SealedTransport.Checkpoint checkpoint(String id, BigInteger sequence, long last, Long scope, long timeout) {
    return new SealedTransport.Checkpoint(id, sequence, last, scope, 0, BigInteger.valueOf(timeout));
  }
  private static void declare(SealedTestPeer.RawClient raw, SealedWork.Declaration request) throws Exception {
    raw.send(SealedWork.encode(request)); SealedWork.requireAcknowledgement(request, SealedWork.decodePayload(raw.response().payload()));
  }
  private List<Wire.Status> send(SealedClient client, SealedWork.EntityKey key, SealedWork.EntityKey parent, String action, String text) throws Exception {
    byte[] bytes = text.getBytes(StandardCharsets.UTF_8); Path file = Files.createTempFile(directory, "entity", ".bin"); Files.write(file, bytes);
    return client.send(new SealedTransport.Header(key, parent, 0, "text/plain", BigInteger.valueOf(bytes.length), SealedWork.sha256().digest(bytes), Map.of("action", action), null), file);
  }
  private SealedClient.FileChunk chunk(SealedWork.EntityKey key, SealedWork.EntityKey parent, int index, int offset, String text) throws Exception {
    byte[] bytes = text.getBytes(StandardCharsets.UTF_8); Path file = Files.createTempFile(directory, "chunk", ".bin"); Files.write(file, bytes);
    return new SealedClient.FileChunk(new SealedTransport.Header(key, parent, 0, "text/plain", BigInteger.valueOf(bytes.length), SealedWork.sha256().digest(bytes), Map.of(),
        new SealedTransport.Chunk(BigInteger.TWO, BigInteger.valueOf(index), BigInteger.valueOf(offset))), file);
  }
  private static QuicStreamChannel payload(SealedTestPeer.RawClient raw, SealedWork.EntityKey key, SealedWork.EntityKey parent, String text, boolean fin) throws Exception {
    byte[] bytes = text.getBytes(StandardCharsets.UTF_8);
    var stream = raw.connection.createStream(QuicStreamType.UNIDIRECTIONAL, new ChannelInboundHandlerAdapter()).get(5, TimeUnit.SECONDS);
    var header = new SealedTransport.Header(key, parent, 0, "text/plain", BigInteger.valueOf(bytes.length), SealedWork.sha256().digest(bytes), Map.of(), null);
    stream.writeAndFlush(Unpooled.wrappedBuffer(SealedTransport.header(header))).get(5, TimeUnit.SECONDS);
    stream.writeAndFlush(Unpooled.wrappedBuffer(bytes)).get(5, TimeUnit.SECONDS);
    if (fin) stream.writeAndFlush(Unpooled.EMPTY_BUFFER).addListener(QuicStreamChannel.SHUTDOWN_OUTPUT).get(5, TimeUnit.SECONDS);
    return stream;
  }
  private Path certificates() throws Exception {
    Path certs = Files.createDirectory(directory.resolve("certs"));
    command(certs, "openssl", "req", "-x509", "-newkey", "rsa:2048", "-noenc", "-keyout", "ca.key", "-out", "ca.crt",
        "-days", "2", "-subj", "/CN=Java-Listener-Test-CA", "-addext", "basicConstraints=critical,CA:TRUE", "-addext", "keyUsage=critical,keyCertSign,cRLSign");
    command(certs, "openssl", "req", "-new", "-newkey", "rsa:2048", "-noenc", "-keyout", "server.key", "-out", "server.csr", "-subj", "/CN=localhost");
    Files.writeString(certs.resolve("extensions"), "subjectAltName=DNS:localhost\nbasicConstraints=critical,CA:FALSE\nkeyUsage=critical,digitalSignature\nextendedKeyUsage=serverAuth\n");
    command(certs, "openssl", "x509", "-req", "-in", "server.csr", "-CA", "ca.crt", "-CAkey", "ca.key", "-CAcreateserial",
        "-out", "server.crt", "-days", "2", "-extfile", "extensions"); return certs;
  }
  private static void command(Path directory, String... args) throws Exception {
    Path log = Files.createTempFile(directory, "openssl", ".log");
    Process process = new ProcessBuilder(args).directory(directory.toFile()).redirectErrorStream(true).redirectOutput(log.toFile()).start();
    try { assertTrue(process.waitFor(10, TimeUnit.SECONDS)); assertEquals(0, process.exitValue(), () -> log.toString()); }
    finally { if (process.isAlive()) process.destroyForcibly().waitFor(5, TimeUnit.SECONDS); }
  }
}
